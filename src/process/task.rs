//! Task execution — run one-shot commands with skip detection and timeout.
//!
//! Tasks are short-lived: no PID files, no ready checks. They check
//! `TaskStateStore::needs_run()`, spawn the command, and report success/failure.

use nix::sys::signal::Signal;
use tokio::time;

use crate::config::template::{self, TemplateError};
use crate::config::{Platform, Task};
use crate::duration::parse_duration;
use crate::sys::{ChildOutput, SpawnConfig, spawn_process};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::time::Duration;

/// Errors from task execution.
#[derive(Debug, thiserror::Error)]
pub enum TaskError {
    #[error("process error: {0}")]
    Process(#[from] crate::sys::ProcessError),
    #[error("timed out after {timeout}")]
    Timeout { timeout: String },
    #[error("invalid duration: {0}")]
    Duration(#[from] crate::duration::DurationError),
    #[error("template error in {field}: {source}")]
    Template {
        field: String,
        #[source]
        source: TemplateError,
    },
}

/// Result of spawning a task process: the handle for waiting and the
/// child's output stream for processing.
pub(crate) struct TaskSpawn {
    pub handle: crate::sys::ProcessHandle,
    pub child_output: ChildOutput,
    pub rendered_cmdline: String,
}

#[derive(Debug)]
struct PreparedTaskCommand {
    cmd: String,
    args: Vec<String>,
    work_dir: PathBuf,
    env: HashMap<String, String>,
    rendered_cmdline: String,
}

/// Spawn a task process. Does not wait for completion.
///
/// Resolves the task's command path using its download config (if any) so
/// that tasks with downloads run the cached binary. The caller is
/// responsible for wiring up output processing and calling `wait_for_task`
/// to get the exit status.
pub(crate) async fn spawn_task(
    task: &Task,
    task_name: &str,
    base_dir: &Path,
    platform: Platform,
    params: &HashMap<String, String>,
    bazel_binary: Option<&str>,
) -> Result<TaskSpawn, TaskError> {
    let prepared = prepare_task_command(task, task_name, base_dir, platform, params, bazel_binary)?;

    let (handle, child_output) = spawn_process(SpawnConfig {
        cmd: &prepared.cmd,
        args: &prepared.args,
        dir: Some(prepared.work_dir.as_path()),
        env: prepared.env,
        pgid_file_path: None,
        force_pipe: false,
        listen_fds: vec![],
    })
    .await?;

    Ok(TaskSpawn {
        handle,
        child_output,
        rendered_cmdline: prepared.rendered_cmdline,
    })
}

fn prepare_task_command(
    task: &Task,
    task_name: &str,
    base_dir: &Path,
    platform: Platform,
    params: &HashMap<String, String>,
    // The built artifact for this task's `bazel.target`, once the build has
    // resolved one. Owned by the supervisor, which is the only thing that
    // sees the build outcome.
    bazel_binary: Option<&str>,
) -> Result<PreparedTaskCommand, TaskError> {
    let render = |field: &str, s: &str| -> Result<String, TaskError> {
        template::render(s, params).map_err(|source| TaskError::Template {
            field: field.to_string(),
            source,
        })
    };

    // Render templates before resolving paths / env so a `{{name}}` in `dir`
    // flows through the same substitution the command sees.
    let rendered_dir: Option<std::path::PathBuf> = match task.dir.as_deref() {
        Some(d) => {
            let s = d.to_string_lossy();
            Some(render("dir", &s)?.into())
        }
        None => None,
    };
    let work_dir = match rendered_dir.as_deref() {
        Some(d) => base_dir.join(d),
        None => base_dir.to_path_buf(),
    };
    let work_dir = work_dir.as_path();

    let mut env: HashMap<String, String> = std::env::vars().collect();
    for (k, v) in &task.env {
        env.insert(k.clone(), render(&format!("env['{k}']"), v)?);
    }
    // Expose downloaded binaries on PATH.
    crate::sys::env::prepend_to_path(&mut env, &base_dir.join(".don").join("bin"));
    // Expose each param to the child as DON_PARAM_<NAME> so tasks can read
    // their own inputs without re-parsing placeholders. Intentionally
    // separate from the `{{name}}` substitution so the task author can
    // pick whichever interface fits the command better.
    for (k, v) in params {
        env.insert(format!("DON_PARAM_{}", k.to_ascii_uppercase()), v.clone());
    }

    // Resolve the command path, using the download binary if configured.
    let cache_base = base_dir.join(".don").join("cache");
    let resolved_cmd = task
        .resolved_cmd(platform, task_name, Some(&cache_base))
        .map_err(|msg| {
            TaskError::Process(crate::sys::ProcessError::Spawn {
                cmd: task_name.to_string(),
                source: std::io::Error::new(std::io::ErrorKind::InvalidInput, msg),
            })
        })?;

    // What this task runs, and anything that has to precede its own `args`.
    //
    // A download replaces `cmd` outright, so templates in the config's `cmd`
    // are irrelevant there — the binary path is fixed. A task with no `cmd`
    // at all is defined by `bazel.target`: it runs the artifact the build
    // resolved, and falls back to `bazel run` when that path isn't known
    // (the build hasn't happened yet, or the cquery couldn't answer).
    let (cmd_str, leading_args): (String, Vec<String>) = match (&task.cmd, &resolved_cmd) {
        (_, Some(path)) if task.download.is_some() => {
            (path.to_string_lossy().into_owned(), Vec::new())
        }
        (Some(cmd), _) => (render("cmd", cmd)?, Vec::new()),
        (None, _) => match (bazel_binary, &task.bazel) {
            (Some(binary), _) => (binary.to_string(), Vec::new()),
            (None, Some(bazel)) => (
                "bazel".to_string(),
                vec!["run".to_string(), bazel.target.clone()],
            ),
            // Validation rejects a task with neither, so this is a config
            // that got here without passing through it.
            (None, None) => {
                return Err(TaskError::Process(crate::sys::ProcessError::Spawn {
                    cmd: task_name.to_string(),
                    source: std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "task has no `cmd` and no `bazel.target`",
                    ),
                }));
            }
        },
    };

    // Bazel launcher scripts (`rules_*`-generated wrappers) commonly read
    // `BUILD_WORKSPACE_DIRECTORY` under `set -u`. Running the built artifact
    // directly bypasses `bazel run`, which is what would normally set it, so
    // fill it in from the workspace root. On the `bazel run` fallback bazel
    // overwrites whatever we set, so this is safe either way.
    if task.bazel.is_some()
        && let Some(workspace) = crate::build_tool::bazel::find_workspace_root(work_dir)
    {
        env.insert(
            "BUILD_WORKSPACE_DIRECTORY".to_string(),
            workspace.to_string_lossy().into_owned(),
        );
    }

    // Render each arg through the template engine.
    let args: Vec<String> = leading_args
        .into_iter()
        .map(Ok)
        .chain(
            task.args
                .iter()
                .enumerate()
                .map(|(i, a)| render(&format!("args[{i}]"), a)),
        )
        .collect::<Result<_, _>>()?;

    let rendered_cmdline = crate::output::format_cmdline(&cmd_str, &args);
    Ok(PreparedTaskCommand {
        cmd: cmd_str,
        args,
        work_dir: work_dir.to_path_buf(),
        env,
        rendered_cmdline,
    })
}

/// Wait for a task to complete, with an optional timeout.
///
/// On timeout, the process group is killed and `TaskError::Timeout` is returned.
pub(crate) async fn wait_for_task(
    handle: &mut crate::sys::ProcessHandle,
    timeout_str: Option<&str>,
) -> Result<ExitStatus, TaskError> {
    if let Some(timeout_str) = timeout_str {
        let timeout = parse_duration(timeout_str)?;
        match time::timeout(timeout, handle.wait()).await {
            Ok(result) => Ok(result?),
            Err(_elapsed) => {
                let _ = handle
                    .terminate(Signal::SIGKILL, Duration::from_millis(500))
                    .await;
                Err(TaskError::Timeout {
                    timeout: timeout_str.to_string(),
                })
            }
        }
    } else {
        Ok(handle.wait().await?)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn task_from(toml: &str) -> Task {
        let config: crate::config::Config = toml.parse().unwrap();
        config.tasks.get("t").unwrap().clone()
    }

    /// What a task actually spawns: its own `cmd` when it has one, the
    /// artifact the build resolved when it doesn't, and `bazel run` when
    /// neither is available yet.
    #[test]
    fn a_task_runs_its_command_or_its_built_artifact() {
        struct Case {
            name: &'static str,
            toml: &'static str,
            bazel_binary: Option<&'static str>,
            want_cmd: &'static str,
            want_args: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                name: "a plain command task",
                toml: "[tasks.t]\ncmd = \"echo\"\nargs = [\"hi\"]\n",
                bazel_binary: None,
                want_cmd: "echo",
                want_args: vec!["hi"],
            },
            Case {
                name: "no cmd: the resolved artifact is the command",
                toml: "[tasks.t]\nbazel.target = \"//a:b\"\n",
                bazel_binary: Some("/ws/bazel-bin/a/b"),
                want_cmd: "/ws/bazel-bin/a/b",
                want_args: vec![],
            },
            Case {
                name: "the task's own args follow the artifact",
                toml: "[tasks.t]\nbazel.target = \"//a:b\"\nargs = [\"--one=2\"]\n",
                bazel_binary: Some("/ws/bazel-bin/a/b"),
                want_cmd: "/ws/bazel-bin/a/b",
                want_args: vec!["--one=2"],
            },
            Case {
                name: "no artifact yet: fall back to bazel run",
                toml: "[tasks.t]\nbazel.target = \"//a:b\"\n",
                bazel_binary: None,
                want_cmd: "bazel",
                want_args: vec!["run", "//a:b"],
            },
            Case {
                name: "the fallback still passes the task's args through",
                toml: "[tasks.t]\nbazel.target = \"//a:b\"\nargs = [\"--\", \"x\"]\n",
                bazel_binary: None,
                want_cmd: "bazel",
                want_args: vec!["run", "//a:b", "--", "x"],
            },
            Case {
                name: "an explicit cmd wins — bazel.target is then watch-only",
                toml: "[tasks.t]\ncmd = \"make\"\nbazel.target = \"//a:b\"\n",
                bazel_binary: Some("/ws/bazel-bin/a/b"),
                want_cmd: "make",
                want_args: vec![],
            },
        ];

        for case in cases {
            let task = task_from(case.toml);
            let prepared = prepare_task_command(
                &task,
                "t",
                std::path::Path::new("/tmp"),
                Platform::LinuxX86_64,
                &HashMap::new(),
                case.bazel_binary,
            )
            .unwrap();
            assert_eq!(prepared.cmd, case.want_cmd, "{}: cmd", case.name);
            assert_eq!(prepared.args, case.want_args, "{}: args", case.name);
        }
    }

    /// A task with neither is a config that never passed validation.
    #[test]
    fn a_task_with_nothing_to_run_is_an_error() {
        let mut task = task_from("[tasks.t]\ncmd = \"echo\"\n");
        task.cmd = None;
        let err = prepare_task_command(
            &task,
            "t",
            std::path::Path::new("/tmp"),
            Platform::LinuxX86_64,
            &HashMap::new(),
            None,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("no `cmd`"),
            "unexpected error: {err}"
        );
    }
}
