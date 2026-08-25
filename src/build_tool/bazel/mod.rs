//! Bazel build tool integration.
//!
//! Queries Bazel for the source packages that feed into a given target,
//! using `bazel query` with `--output=package` for directory-level granularity.
//! External dependencies and generated files are filtered out.

mod graph;

use super::{AbortOnDrop, BuildToolError, ResolvedBuildInfo};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Walk up from `start` looking for a Bazel workspace marker file
/// (`MODULE.bazel`, `WORKSPACE.bazel`, `WORKSPACE`, or `REPO.bazel`) and
/// return the first directory that contains one.
///
/// Returns `None` if no marker is found before hitting the filesystem root.
/// Used to populate `BUILD_WORKSPACE_DIRECTORY` when don launches a built
/// bazel artifact directly (bypassing `bazel run`, which would set the var
/// itself). Launcher scripts emitted by `rules_*` commonly read this var
/// under `set -u`, so missing it fails the service immediately.
pub(crate) fn find_workspace_root(start: &Path) -> Option<PathBuf> {
    let mut current: &Path = start;
    loop {
        for marker in ["MODULE.bazel", "WORKSPACE.bazel", "WORKSPACE", "REPO.bazel"] {
            if current.join(marker).exists() {
                return Some(current.to_path_buf());
            }
        }
        current = current.parent()?;
    }
}

/// Bazel build graph resolver.
///
/// Shells out to `bazel query` to determine which first-party source packages
/// contribute to a given target. The resolved packages become watch directories.
pub(crate) struct BazelResolver {
    /// Optional emitter for streaming bazel client stderr live (lock-wait
    /// notices, INFO/WARN/ERROR lines). When `None`, stderr is still
    /// captured for error reporting but isn't surfaced until the process
    /// exits — fine for tests, but in production the runner attaches one
    /// so messages like "Another command (pid=X) is running. Waiting for
    /// it to complete on the server (server_pid=Y)..." appear immediately.
    emitter: Option<crate::output::LifecycleEmitter>,
}

/// The full `bazel` argument list for building `targets`.
///
/// `--curses=no` forces line-buffered progress output. Without it, bazel
/// detects the piped stderr and *may* still emit progress with \r-only
/// updates (or buffer for seconds), so our line-reader sees nothing for long
/// stretches of analysis/loading. With curses off, each progress tick is a
/// separate \n-terminated line we can stream.
///
/// `--color=auto` suppresses ANSI when stderr isn't a TTY (our case — we pipe
/// it). Forcing colors on keeps INFO/WARN/ERROR visually distinct in the
/// bazel-prefixed stream; the sanitize pass keeps SGR and strips only
/// cursor/screen codes.
///
/// A configured `--config` goes *after* both, so a workspace that deliberately
/// sets either of them in its `.bazelrc` wins. That includes winning in ways
/// that stop don reading this output as lines, which is the workspace's call
/// to make.
fn build_args(targets: &[String], config: Option<&str>) -> Vec<String> {
    let mut args: Vec<String> = vec!["build".into(), "--curses=no".into(), "--color=yes".into()];
    if let Some(config) = config {
        args.push(format!("--config={config}"));
    }
    args.extend(targets.iter().cloned());
    args
}

impl BazelResolver {
    /// Create a new resolver.
    ///
    /// All bazel operations rely on user cancellation (Ctrl+C → `kill_on_drop`
    /// → server-side build cancellation) rather than client-side timeouts.
    /// A fixed timeout produced misleading errors on slow cold-start queries
    /// and on legitimately long builds.
    pub(crate) fn new() -> Self {
        Self { emitter: None }
    }

    /// Attach an emitter so query/cquery stderr is streamed live (not just
    /// captured-then-replayed-on-error). Use this whenever you have one;
    /// it makes the lock-wait notice visible while bazel is blocked behind
    /// another client.
    pub(crate) fn with_emitter(mut self, emitter: crate::output::LifecycleEmitter) -> Self {
        self.emitter = Some(emitter);
        self
    }

    /// Check that the `bazel` binary is available on PATH.
    async fn check_installed(&self) -> Result<(), BuildToolError> {
        let result = tokio::process::Command::new("bazel")
            .arg("version")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await;

        match result {
            Ok(status) if status.success() => Ok(()),
            Ok(_) => Ok(()), // bazel version may return non-zero in some setups, but binary exists
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(BuildToolError::NotInstalled {
                    tool: "bazel".to_string(),
                })
            }
            Err(e) => Err(BuildToolError::Io {
                tool: "bazel".to_string(),
                source: e,
            }),
        }
    }

    /// Run a bazel query and return stdout as a string.
    async fn run_query(
        &self,
        query: &str,
        output_format: &str,
        working_dir: &Path,
    ) -> Result<String, BuildToolError> {
        let mut child = tokio::process::Command::new("bazel")
            .args(["query", query, &format!("--output={output_format}")])
            .current_dir(working_dir)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| BuildToolError::Io {
                tool: "bazel".to_string(),
                source: e,
            })?;

        // Keep query progress quiet. The only live signal worth surfacing
        // here is Bazel waiting behind another command; failures still
        // come back with captured stderr in the returned error.
        let stderr_handle = spawn_stderr_stream(child.stderr.take(), self.emitter.clone(), true);

        // `wait_with_output` reads whatever pipes are still attached; we
        // already took stderr, so it just collects stdout and exit status.
        let output = child
            .wait_with_output()
            .await
            .map_err(|e| BuildToolError::Io {
                tool: "bazel".to_string(),
                source: e,
            })?;

        let stderr_collected = match stderr_handle.into_inner() {
            Some(h) => h.await.unwrap_or_default(),
            None => String::new(),
        };

        if !output.status.success() {
            // Truncate long error messages
            let truncated = if stderr_collected.len() > 500 {
                format!("{}...", &stderr_collected[..500])
            } else {
                stderr_collected
            };
            return Err(BuildToolError::QueryFailed {
                tool: "bazel".to_string(),
                message: truncated.trim().to_string(),
            });
        }

        String::from_utf8(output.stdout).map_err(|e| BuildToolError::ParseError {
            tool: "bazel".to_string(),
            message: format!("non-UTF-8 output: {e}"),
        })
    }

    /// Run `bazel build` for multiple targets in a single invocation.
    ///
    /// Bazel parallelizes the build internally, so this is more efficient than
    /// running separate builds per target. Returns which targets succeeded/failed.
    ///
    /// Build output is streamed line-by-line through the provided callback.
    pub(crate) async fn build_targets<F>(
        &self,
        targets: &[String],
        working_dir: &Path,
        config: Option<&str>,
        mut on_line: F,
        emitter: Option<&crate::output::LifecycleEmitter>,
    ) -> Result<super::BatchBuildResult, BuildToolError>
    where
        F: FnMut(&str) + Send + 'static,
    {
        if targets.is_empty() {
            return Ok(super::BatchBuildResult {
                succeeded: Vec::new(),
                failed: Vec::new(),
            });
        }

        self.check_installed().await?;

        // Built once and used for both the spawn and the debug line, so the
        // two cannot describe different commands.
        let args = build_args(targets, config);
        let mut cmd = tokio::process::Command::new("bazel");
        cmd.args(&args);
        cmd.current_dir(working_dir)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            // If the spawning future is dropped (e.g. shutdown mid-startup),
            // SIGKILL the bazel client. The bazel server detects the client
            // disconnect and cancels the in-flight build.
            .kill_on_drop(true);

        if let Some(em) = emitter {
            em.debug_spawn("bazel", "bazel", &args);
        }

        let mut child = cmd.spawn().map_err(|e| BuildToolError::Io {
            tool: "bazel".to_string(),
            source: e,
        })?;

        // Stream stderr (Bazel writes build progress to stderr).
        // Wrap the spawn handle in an AbortOnDrop guard so cancellation of
        // `build_targets` (e.g. shutdown mid-build) tears the reader down
        // immediately. Without this, the reader stays alive holding a clone
        // of the `on_line` callback's senders (often a `LifecycleEmitter`
        // bound to the OutputManager). Bazel's child *build action*
        // processes inherit fds 1/2, so the pipe doesn't close just because
        // the bazel client gets SIGKILL'd — the action processes can keep
        // the writer end open for minutes. That kept stdout_sink_task's
        // channel from closing and made `OutputManager::shutdown` hang.
        let stderr = child.stderr.take();
        let targets_for_parse = targets.to_vec();
        let stream_handle = AbortOnDrop::new(tokio::spawn(async move {
            let mut failed_targets: Vec<String> = Vec::new();
            if let Some(stderr) = stderr {
                let mut reader = tokio::io::BufReader::new(stderr);
                let mut line_buf = Vec::new();
                loop {
                    line_buf.clear();
                    match tokio::io::AsyncBufReadExt::read_until(&mut reader, b'\n', &mut line_buf)
                        .await
                    {
                        Ok(0) => break,
                        Ok(_) => {
                            if line_buf.last() == Some(&b'\n') {
                                line_buf.pop();
                            }
                            if line_buf.last() == Some(&b'\r') {
                                line_buf.pop();
                            }
                            let text = String::from_utf8_lossy(&line_buf);
                            // Parse ERROR lines to identify failed targets.
                            if text.contains("ERROR:") {
                                for target in &targets_for_parse {
                                    if text.contains(target.as_str()) {
                                        failed_targets.push(target.clone());
                                    }
                                }
                            }
                            on_line(&text);
                        }
                        Err(_) => break,
                    }
                }
            }
            failed_targets
        }));

        // Also drain stdout. Same drop-on-cancel rationale as stderr above.
        let stdout = child.stdout.take();
        let stdout_handle = AbortOnDrop::new(tokio::spawn(async move {
            if let Some(mut stdout) = stdout {
                let mut buf = vec![0u8; 4096];
                loop {
                    match tokio::io::AsyncReadExt::read(&mut stdout, &mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
            }
        }));

        let status = child.wait().await.map_err(|e| BuildToolError::Io {
            tool: "bazel".to_string(),
            source: e,
        })?;

        if let Some(h) = stdout_handle.into_inner() {
            let _ = h.await;
        }
        let failed_from_output = match stream_handle.into_inner() {
            Some(h) => h.await.unwrap_or_default(),
            None => Vec::new(),
        };

        if status.success() {
            Ok(super::BatchBuildResult {
                succeeded: targets.to_vec(),
                failed: Vec::new(),
            })
        } else {
            // Determine which targets failed. If we identified specific targets
            // from ERROR lines, mark those as failed and the rest as succeeded.
            // Otherwise, conservatively mark all as failed.
            if failed_from_output.is_empty() {
                let code = status.code().unwrap_or(-1);
                Ok(super::BatchBuildResult {
                    succeeded: Vec::new(),
                    failed: targets
                        .iter()
                        .map(|t| (t.clone(), format!("bazel build failed (exit code {code})")))
                        .collect(),
                })
            } else {
                let failed_set: std::collections::HashSet<&str> =
                    failed_from_output.iter().map(|s| s.as_str()).collect();
                let succeeded: Vec<String> = targets
                    .iter()
                    .filter(|t| !failed_set.contains(t.as_str()))
                    .cloned()
                    .collect();
                let failed: Vec<(String, String)> = failed_from_output
                    .iter()
                    .map(|t| (t.clone(), "bazel build failed".to_string()))
                    .collect();
                Ok(super::BatchBuildResult { succeeded, failed })
            }
        }
    }

    /// Check if targets are already up to date without building.
    ///
    /// Uses `bazel build --check_up_to_date` which exits 0 if all targets
    /// are up to date and non-zero if any need rebuilding. This avoids
    /// unnecessary service restarts when a watched file changed but the
    /// build output would be identical.
    ///
    /// Bazel reports "needs rebuilding" as a non-zero exit and may print
    /// `ERROR:` lines on stderr even though this is an expected control-flow
    /// outcome, not an actual failure. Suppress live stderr emission for this
    /// probe so the logs stay quiet unless a real rebuild runs.
    pub(crate) async fn check_up_to_date(
        &self,
        targets: &[String],
        working_dir: &Path,
        config: Option<&str>,
    ) -> Result<bool, BuildToolError> {
        if targets.is_empty() {
            return Ok(true);
        }

        let mut cmd = tokio::process::Command::new("bazel");
        cmd.arg("build");
        cmd.arg("--check_up_to_date");
        // The same configuration the build would use. Asking about a
        // different one answers about artifacts the build will not produce.
        if let Some(config) = config {
            cmd.arg(format!("--config={config}"));
        }
        cmd.args(targets);
        cmd.current_dir(working_dir)
            .stdout(std::process::Stdio::null())
            // Pipe (not null) so the lock-wait notice can be streamed to
            // the user. Without this, `--check_up_to_date` blocked behind
            // another bazel client looks like don is hung.
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);

        let mut child = cmd.spawn().map_err(|e| BuildToolError::Io {
            tool: "bazel".to_string(),
            source: e,
        })?;

        let stderr_handle = spawn_stderr_stream(child.stderr.take(), self.emitter.clone(), true);

        let status = child.wait().await.map_err(|e| BuildToolError::Io {
            tool: "bazel".to_string(),
            source: e,
        })?;

        if let Some(h) = stderr_handle.into_inner() {
            let _ = h.await;
        }

        Ok(status.success())
    }

    /// Resolve per-target source packages in ONE `bazel query` call.
    ///
    /// Runs `deps(T1 + T2 + ... + Tn) --output=xml`, stream-parses the
    /// resulting graph, and DFS-walks from each input target to attribute
    /// its own set of first-party source packages. Returns a map keyed by
    /// input target label; every input appears in the map, even if its
    /// package set is empty.
    ///
    /// This is strictly better than running N separate queries: Bazel's
    /// analysis phase loads the workspace graph once, the client starts up
    /// once, and we get accurate per-target attribution rather than a union.
    pub(crate) async fn resolve_per_target(
        &self,
        targets: &[String],
        working_dir: &Path,
    ) -> Result<HashMap<String, ResolvedBuildInfo>, BuildToolError> {
        let mut out: HashMap<String, ResolvedBuildInfo> = HashMap::new();
        if targets.is_empty() {
            return Ok(out);
        }

        self.check_installed().await?;

        // `+` is bazel's set-union operator. Targets are parenthesised so
        // operator precedence of `//` / `:` / flags can't bite us.
        let union_expr: String = targets
            .iter()
            .map(|t| format!("({t})"))
            .collect::<Vec<_>>()
            .join(" + ");
        let query = format!("deps({union_expr})");
        let xml = self.run_query(&query, "xml", working_dir).await?;

        let graph = graph::BazelDepGraph::parse_xml(xml.as_bytes())?;

        for target in targets {
            let packages = graph.packages_for(target);
            let watch_paths: Vec<String> = packages.iter().map(|p| format!("{p}/**")).collect();
            let graph_definition_globs: Vec<String> = packages
                .iter()
                .flat_map(|p| [format!("{p}/BUILD"), format!("{p}/BUILD.bazel")])
                .collect();
            out.insert(
                target.clone(),
                ResolvedBuildInfo {
                    watch_paths,
                    graph_definition_globs,
                },
            );
        }
        Ok(out)
    }

    /// Resolve the output binary paths for a batch of Bazel targets in ONE
    /// `bazel cquery` invocation.
    ///
    /// Bazel's `--output=starlark` lets us emit per-target attribution
    /// (`<label>\t<executable-path>`) so the analysis pass runs once instead
    /// of N times. We read `target.files_to_run.executable.path` — the exact
    /// binary `bazel run` would launch — which is more precise than picking
    /// the first entry out of `target.files`. Targets without an executable
    /// (non-`*_binary` rules, aliases that don't forward `files_to_run`) are
    /// absent from the returned map; the caller should fall back to
    /// `bazel run` for those.
    ///
    /// Keys are inserted in both canonical (`@@//pkg:name`) and stripped
    /// (`//pkg:name`) forms so callers can look up using whichever form
    /// the user wrote in `don.toml`.
    pub(crate) async fn resolve_binary_paths(
        &self,
        targets: &[String],
        working_dir: &Path,
    ) -> Result<HashMap<String, String>, BuildToolError> {
        let mut out: HashMap<String, String> = HashMap::new();
        if targets.is_empty() {
            return Ok(out);
        }

        // `set(T1 T2 ... Tn)` is bazel's space-separated set literal.
        // `set` takes bare labels — no parenthesisation (unlike `+`,
        // where we wrap targets to fence operator precedence).
        let query = format!("set({})", targets.join(" "));

        // Per-target line: `<label>\t<executable-path-or-empty>`. The
        // no-executable case (non-runnable targets) is handled inline so
        // the expression always evaluates — the parser drops lines whose
        // path doesn't start with `bazel-out/`.
        let starlark = r#"str(target.label) + "\t" + (target.files_to_run.executable.path if target.files_to_run.executable else "")"#;
        let starlark_arg = format!("--starlark:expr={starlark}");

        let mut child = tokio::process::Command::new("bazel")
            .args(["cquery", &query, "--output=starlark", &starlark_arg])
            .current_dir(working_dir)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| BuildToolError::Io {
                tool: "bazel".to_string(),
                source: e,
            })?;

        // `cquery` is a metadata lookup, not user-interesting build output.
        // Surface only lock-wait notices live; keep the rest for errors.
        let stderr_handle = spawn_stderr_stream(child.stderr.take(), self.emitter.clone(), true);

        let output = child
            .wait_with_output()
            .await
            .map_err(|e| BuildToolError::Io {
                tool: "bazel".to_string(),
                source: e,
            })?;

        let stderr_collected = match stderr_handle.into_inner() {
            Some(h) => h.await.unwrap_or_default(),
            None => String::new(),
        };

        if !output.status.success() {
            return Err(BuildToolError::QueryFailed {
                tool: "bazel".to_string(),
                message: format!("cquery failed: {}", stderr_collected.trim()),
            });
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        for line in stdout.lines() {
            let line = line.trim();
            let Some((label, path)) = line.split_once('\t') else {
                continue;
            };
            let path = path.trim();
            if !path.starts_with("bazel-out/") {
                continue;
            }
            let canonical = label.to_string();
            let stripped = label.strip_prefix("@@").unwrap_or(label).to_string();
            out.insert(canonical, path.to_string());
            out.insert(stripped, path.to_string());
        }

        Ok(out)
    }
}

/// Stream a bazel client's stderr to the given emitter line-by-line, while
/// accumulating it for error reporting. Spawn this BEFORE `child.wait()`
/// so messages like the lock-wait notice surface immediately instead of
/// being held until exit.
///
/// The returned [`AbortOnDrop`] yields the full collected stderr text when
/// the child closes its stderr pipe.
fn spawn_stderr_stream(
    stderr: Option<tokio::process::ChildStderr>,
    emitter: Option<crate::output::LifecycleEmitter>,
    lock_wait_only: bool,
) -> AbortOnDrop<String> {
    AbortOnDrop::new(tokio::spawn(async move {
        let mut collected = String::new();
        let Some(stderr) = stderr else {
            return collected;
        };
        let mut reader = tokio::io::BufReader::new(stderr);
        let mut line_buf = Vec::new();
        loop {
            line_buf.clear();
            match tokio::io::AsyncBufReadExt::read_until(&mut reader, b'\n', &mut line_buf).await {
                Ok(0) => break,
                Ok(_) => {
                    collected.push_str(&String::from_utf8_lossy(&line_buf));
                    if let Some(ref em) = emitter {
                        let mut text = line_buf.as_slice();
                        if text.last() == Some(&b'\n') {
                            text = &text[..text.len() - 1];
                        }
                        if text.last() == Some(&b'\r') {
                            text = &text[..text.len() - 1];
                        }
                        let text = String::from_utf8_lossy(text);
                        if should_emit_stderr_line(lock_wait_only, &text) {
                            em.bazel_event(&text);
                        }
                    }
                }
                Err(_) => break,
            }
        }
        collected
    }))
}

fn should_emit_stderr_line(lock_wait_only: bool, line: &str) -> bool {
    !lock_wait_only || is_lock_wait_notice(line)
}

fn is_lock_wait_notice(line: &str) -> bool {
    line.contains("Another command")
        && (line.contains("Waiting for it to complete on the server")
            || line.contains("Waiting for it to complete"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    /// The whole command line, in order. `--config` has to come after don's
    /// own flags — that ordering is what lets a workspace override them from
    /// its `.bazelrc` — and before the targets, because bazel reads
    /// everything after the first target as a target.
    #[test]
    fn build_args_places_the_configuration_after_dons_own_flags() {
        struct Case {
            name: &'static str,
            targets: &'static [&'static str],
            config: Option<&'static str>,
            want: &'static [&'static str],
        }

        let cases = vec![
            Case {
                name: "no configuration named",
                targets: &["//a", "//b"],
                config: None,
                want: &["build", "--curses=no", "--color=yes", "//a", "//b"],
            },
            Case {
                name: "a configuration is the last flag before the targets",
                targets: &["//a"],
                config: Some("don"),
                want: &["build", "--curses=no", "--color=yes", "--config=don", "//a"],
            },
            Case {
                name: "no targets is still a well-formed command",
                targets: &[],
                config: Some("don"),
                want: &["build", "--curses=no", "--color=yes", "--config=don"],
            },
        ];

        for case in cases {
            let targets: Vec<String> = case.targets.iter().map(|t| (*t).to_string()).collect();
            let got = build_args(&targets, case.config);
            assert_eq!(got, case.want, "{}", case.name);
        }
    }

    #[test]
    fn test_should_emit_stderr_line() {
        struct Case {
            name: &'static str,
            lock_wait_only: bool,
            line: &'static str,
            expected: bool,
        }

        let cases = vec![
            Case {
                name: "full mode emits progress",
                lock_wait_only: false,
                line: "INFO: Invocation ID: 123",
                expected: true,
            },
            Case {
                name: "lock-wait mode suppresses invocation id",
                lock_wait_only: true,
                line: "INFO: Invocation ID: 123",
                expected: false,
            },
            Case {
                name: "lock-wait mode keeps server wait notice",
                lock_wait_only: true,
                line: "Another command (pid=1) is running. Waiting for it to complete on the server (server_pid=2)...",
                expected: true,
            },
            Case {
                name: "lock-wait mode suppresses unrelated info",
                lock_wait_only: true,
                line: "Loading: 2328 packages loaded",
                expected: false,
            },
        ];

        for case in cases {
            assert_eq!(
                should_emit_stderr_line(case.lock_wait_only, case.line),
                case.expected,
                "{}",
                case.name
            );
        }
    }
}
