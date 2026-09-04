//! Service lifecycle management — start, stop, restart.
//!
//! Services are long-running processes with PID files, output capture,
//! and optional ready checks.

use crate::config::service::{GoConfig, RustConfig, ServiceKind};
use crate::config::{Platform, ReadyCheck, ResolvedService, ShutdownConfig};
use crate::duration::parse_duration;
use crate::output::LifecycleEmitter;
use crate::sys::env::merge_env;
use crate::sys::{ProcessHandle, SpawnConfig, signal_name, spawn_process};
use nix::sys::signal::Signal;
use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::watch;

/// A running service handle — either a local process or a Docker container.
pub enum ServiceHandle {
    /// A locally spawned process with its own process group.
    Process(ProcessHandle),
    /// A Docker container managed via the bollard API.
    Docker(crate::docker::DockerHandle),
}

/// Result of starting a service: the handle for lifecycle management
/// and the child's output stream for processing.
pub(crate) struct StartResult {
    pub handle: ServiceHandle,
    pub child_output: crate::sys::ChildOutput,
}

#[derive(Clone)]
pub(crate) struct StopDebug {
    service: String,
    emitter: LifecycleEmitter,
}

impl StopDebug {
    pub(crate) fn new(service: impl Into<String>, emitter: LifecycleEmitter) -> Self {
        Self {
            service: service.into(),
            emitter,
        }
    }

    fn signal(&self, sig: Signal, pgid: i32) {
        self.emitter.service_event(
            &self.service,
            &format!("send {} to pgid {pgid}", signal_name(sig)),
        );
    }

    fn docker_stop(&self, signal: &str, timeout: Duration) {
        self.emitter.service_event(
            &self.service,
            &format!("docker stop signal={signal} timeout={timeout:?}"),
        );
    }
}

/// Errors from service operations.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("process error: {0}")]
    Process(#[from] crate::sys::ProcessError),
    #[error("env error: {0}")]
    Env(#[from] crate::sys::env::EnvError),
    #[error("ready check failed after {retries} retries")]
    ReadyCheckExhausted { retries: u32 },
    #[error("process exited during ready check")]
    ProcessExitedDuringReadyCheck,
    #[error("ready check error: {0}")]
    ReadyCheckError(String),
    #[error("invalid duration: {0}")]
    Duration(#[from] crate::duration::DurationError),
    #[error("docker error: {0}")]
    Docker(String),
    #[error("config error: {0}")]
    Config(String),
}

/// Start a service: merge env, spawn process.
///
/// Returns a `StartResult` containing the process handle and the child's
/// output stream. The caller is responsible for wiring up output processing,
/// the ready check, and state updates.
/// Start a service.
///
/// `listen_fds` are raw fds of listenfd-mode proxy listeners to pass to the
/// child at fd 3, 4, …. The parent keeps the owning `TcpListener`s alive;
/// these are just borrowed fds. Empty if the service has no listenfd
/// entries. `listen_fds_env` is the matching `LISTEN_FDS` / `LISTEN_FDNAMES`
/// env var map (empty when `listen_fds` is empty).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn start_service(
    name: &str,
    resolved: &ResolvedService,
    base_dir: &Path,
    pid_dir: &Path,
    listen_fds: &[RawFd],
    listen_fds_env: &HashMap<String, String>,
    docker_client: Option<&bollard::Docker>,
    service_writer: Option<&crate::output::ServiceWriter>,
    platform: Platform,
    emitter: Option<&crate::output::LifecycleEmitter>,
    fallback_ports: bool,
    prior_docker_port_bindings: &[crate::docker::DockerPortBinding],
    secrets: &crate::secrets::SecretStore,
) -> Result<StartResult, ServiceError> {
    // Dispatch based on the service kind.
    if let Some(ServiceKind::Docker(docker_config)) = &resolved.kind {
        // Docker kind: start a container via the Docker API.
        let client = docker_client
            .ok_or_else(|| ServiceError::Docker("docker client not available".to_string()))?;
        let (handle, child_output) = crate::docker::start_docker_service(
            client,
            name,
            docker_config,
            fallback_ports,
            prior_docker_port_bindings,
            &resolved.env,
            &resolved.env_file,
            base_dir,
            service_writer,
            secrets,
            &resolved.secrets,
        )
        .await
        .map_err(|e| ServiceError::Docker(e.to_string()))?;
        return Ok(StartResult {
            handle: ServiceHandle::Docker(handle),
            child_output,
        });
    }

    // Resolve working directory: join service's `dir` with base_dir so
    // relative paths like `./app` resolve correctly regardless of cwd.
    let service_dir_buf = match resolved.dir.as_deref() {
        Some(d) => base_dir.join(d),
        None => base_dir.to_path_buf(),
    };
    let service_dir = service_dir_buf.as_path();

    // Determine the run command and args based on kind.
    // For rust/go kinds, the binary path is relative to the service's
    // working directory (where cargo/go build runs), not base_dir.
    let (cmd, args): (String, Vec<String>) = match &resolved.kind {
        Some(ServiceKind::Rust(rust_config)) => {
            let binary_path = rust_binary_path(rust_config, service_dir);
            (binary_path.to_string_lossy().into_owned(), vec![])
        }
        Some(ServiceKind::Go(go_config)) => {
            let binary_path = go_binary_path(go_config, name, service_dir);
            (binary_path.to_string_lossy().into_owned(), vec![])
        }
        Some(ServiceKind::Custom { .. }) => {
            let cache_base = base_dir.join(".don").join("cache");
            let (executable, run_args) = resolved
                .resolved_run_cmd(platform, name, Some(&cache_base))
                .map_err(ServiceError::Config)?;
            (executable.to_string_lossy().into_owned(), run_args.to_vec())
        }
        Some(ServiceKind::Bazel(bazel)) => {
            // Prefer the binary path resolved by the startup `bazel cquery`
            // (stored on `resolved.resolved_binary_path`) — launching the
            // built artifact directly skips bazel's per-launch analysis.
            // Fall back to `bazel run <target>` when the path isn't known
            // yet (e.g. lazy service pre-first-connection).
            if let Some(ref bin_path) = resolved.resolved_binary_path {
                (bin_path.clone(), vec![])
            } else {
                (
                    "bazel".to_string(),
                    vec!["run".to_string(), bazel.target.clone()],
                )
            }
        }
        _ => {
            return Err(crate::sys::ProcessError::Spawn {
                cmd: name.to_string(),
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "service has no run command or preset",
                ),
            }
            .into());
        }
    };
    let (mut env, _warnings) = merge_env(
        name,
        Some(service_dir),
        &resolved.env_file,
        &resolved.env,
        listen_fds_env,
    )?;
    secrets.strip_undeclared(&mut env, &resolved.secrets);
    secrets.inject(&mut env, &resolved.secrets);
    // Expose downloaded binaries on PATH so other services/tasks can call them.
    crate::sys::env::prepend_to_path(&mut env, &base_dir.join(".don").join("bin"));

    // Bazel launcher scripts (`rules_*`-generated wrappers) commonly read
    // `BUILD_WORKSPACE_DIRECTORY` under `set -u`. When don launches the built
    // artifact directly (via `resolve_binary_paths`) it bypasses `bazel run`,
    // which is what would normally set the var. Fill it in ourselves based
    // on the service's workspace root so those launchers don't bomb out.
    // For the `bazel run` fallback path bazel overwrites whatever we set,
    // so this is safe either way.
    if matches!(resolved.kind, Some(ServiceKind::Bazel(_)))
        && let Some(workspace) = crate::build_tool::bazel::find_workspace_root(service_dir)
    {
        env.insert(
            "BUILD_WORKSPACE_DIRECTORY".to_string(),
            workspace.to_string_lossy().into_owned(),
        );
    }

    // Expand ${VAR} references in the command and args against the env map.
    // This lets proxy-injected vars like PORT be used in run args.
    let cmd = crate::sys::env::expand_env_vars(&cmd, &env);
    let args: Vec<String> = args
        .iter()
        .map(|a| crate::sys::env::expand_env_vars(a, &env))
        .collect();

    // Build PGID file path.
    std::fs::create_dir_all(pid_dir).map_err(crate::sys::ProcessError::Io)?;
    let pgid_file_path = pid_dir.join(name);

    if let Some(em) = emitter {
        em.debug_spawn(name, &cmd, &args);
    }

    // Spawn the process. Listen-fd services still use PTY mode when available;
    // fd placement is handled in the spawn pre_exec hook for both PTY and pipe
    // fallback paths.
    let (handle, child_output) = spawn_process(SpawnConfig {
        cmd: &cmd,
        args: &args,
        dir: Some(service_dir),
        env,
        pgid_file_path: Some(pgid_file_path),
        // `tty = false` spawns with plain pipes and no controlling terminal, so
        // background-process-group children that touch the tty aren't
        // SIGTTOU/SIGTTIN-suspended.
        force_pipe: !resolved.tty,
        listen_fds: listen_fds.to_vec(),
    })
    .await?;

    Ok(StartResult {
        handle: ServiceHandle::Process(handle),
        child_output,
    })
}

/// Run a ready check with retry loop.
///
/// Checks every `interval` up to `retries` times.
/// Returns `Ok(())` when the check passes, or `Err` when retries are exhausted.
pub(crate) async fn run_ready_check(ready: &ReadyCheck) -> Result<(), ServiceError> {
    let interval = parse_duration(&ready.interval)?;
    let timeout = parse_duration(&ready.timeout)?;
    let retries = ready.retries;

    for attempt in 0..retries {
        if attempt > 0 {
            tokio::time::sleep(interval).await;
        }

        if run_one_check_with_timeout(ready, timeout).await.is_ok() {
            return Ok(());
        }
    }

    Err(ServiceError::ReadyCheckExhausted { retries })
}

/// Run one ready-check probe with the configured per-attempt timeout.
pub(crate) async fn run_one_check_with_config_timeout(
    ready: &ReadyCheck,
) -> Result<(), ServiceError> {
    let timeout = parse_duration(&ready.timeout)?;
    run_one_check_with_timeout(ready, timeout).await
}

async fn run_one_check_with_timeout(
    ready: &ReadyCheck,
    timeout: Duration,
) -> Result<(), ServiceError> {
    match tokio::time::timeout(timeout, run_one_check(ready)).await {
        Ok(result) => result,
        Err(_) => Err(ServiceError::ReadyCheckError(format!(
            "ready check timed out after {}",
            ready.timeout
        ))),
    }
}

/// Run a single ready-check probe (one HTTP/TCP/exec attempt). Returns
/// `Ok(())` if the configured check passes, otherwise `Err` describing
/// the failure. Returns `Ok(())` when no check type is set — there is
/// nothing to check.
pub(crate) async fn run_one_check(ready: &ReadyCheck) -> Result<(), ServiceError> {
    if let Some(ref tcp_addr) = ready.tcp {
        check_tcp(tcp_addr).await
    } else if let Some(ref http_url) = ready.http {
        check_http(http_url).await
    } else if let Some(ref exec_cmd) = ready.exec {
        check_exec(exec_cmd).await
    } else {
        Ok(())
    }
}

/// TCP ready check: attempt to connect to the address.
async fn check_tcp(addr: &str) -> Result<(), ServiceError> {
    tokio::net::TcpStream::connect(addr)
        .await
        .map(|_| ())
        .map_err(|e| ServiceError::ReadyCheckError(format!("tcp connect failed: {e}")))
}

/// HTTP ready check: GET the URL and check for 2xx status.
async fn check_http(url: &str) -> Result<(), ServiceError> {
    let resp = reqwest::get(url)
        .await
        .map_err(|e| ServiceError::ReadyCheckError(format!("http request failed: {e}")))?;

    if resp.status().is_success() {
        Ok(())
    } else {
        Err(ServiceError::ReadyCheckError(format!(
            "http status {}",
            resp.status()
        )))
    }
}

/// Exec ready check: run the command, exit code 0 = ready.
async fn check_exec(cmd: &crate::config::Command) -> Result<(), ServiceError> {
    let env: HashMap<String, String> = std::env::vars().collect();
    let (mut handle, _output) = spawn_process(SpawnConfig {
        cmd: &cmd.cmd,
        args: &cmd.args,
        dir: None,
        env,
        pgid_file_path: None,
        force_pipe: true,
        listen_fds: vec![],
    })
    .await?;

    let status = handle.wait().await?;
    if status.success() {
        Ok(())
    } else {
        Err(ServiceError::ReadyCheckError(format!(
            "exec check exited with code {}",
            status.code().unwrap_or(-1)
        )))
    }
}

/// Parse a signal name string (e.g. "SIGTERM") into a nix Signal.
fn parse_signal(s: &str) -> Signal {
    match s {
        "SIGINT" => Signal::SIGINT,
        "SIGQUIT" => Signal::SIGQUIT,
        "SIGHUP" => Signal::SIGHUP,
        "SIGUSR1" => Signal::SIGUSR1,
        "SIGUSR2" => Signal::SIGUSR2,
        _ => Signal::SIGTERM,
    }
}

/// Stop a service: send signal, wait with timeout, escalate to SIGKILL.
///
/// `wait_full_exit`: after the parent process is reaped, poll the process
/// group until it's empty. Enable this for restarts of services that have
/// a fixed backend port (see `ProxyMode::Forward`) or any other exclusive
/// resource where the old and new instances can't coexist briefly.
/// Leaving it `false` lets the restart proceed with a tiny window of
/// overlap — fine for env / listenfd proxy modes where each instance
/// uses a distinct backend.
pub(crate) async fn stop_service(
    mut handle: ServiceHandle,
    shutdown_config: Option<&ShutdownConfig>,
    force: bool,
    wait_full_exit: bool,
    debug: Option<StopDebug>,
) -> Result<(), ServiceError> {
    match handle {
        ServiceHandle::Process(ref mut process) => {
            let skip_graceful = force || shutdown_config.is_some_and(|config| !config.graceful);
            let (signal, timeout) = if skip_graceful {
                (Signal::SIGKILL, Duration::from_millis(500))
            } else {
                (
                    shutdown_config
                        .map(|c| parse_signal(&c.signal))
                        .unwrap_or(Signal::SIGTERM),
                    shutdown_config
                        .and_then(|c| parse_duration(&c.timeout).ok())
                        .unwrap_or(Duration::from_secs(10)),
                )
            };
            if wait_full_exit {
                let debug = debug.clone();
                process
                    .terminate_process_group_with_signal_callback(
                        signal,
                        timeout,
                        move |sig, pgid| {
                            if let Some(debug) = debug.as_ref() {
                                debug.signal(sig, pgid);
                            }
                        },
                    )
                    .await?;
            } else {
                let debug = debug.clone();
                process
                    .terminate_with_signal_callback(signal, timeout, move |sig, pgid| {
                        if let Some(debug) = debug.as_ref() {
                            debug.signal(sig, pgid);
                        }
                    })
                    .await?;
            }
        }
        ServiceHandle::Docker(ref mut docker) => {
            let skip_graceful = force || shutdown_config.is_some_and(|config| !config.graceful);
            let (signal_name, timeout) = if skip_graceful {
                ("SIGKILL", Duration::from_millis(500))
            } else {
                (
                    shutdown_config
                        .map(|c| c.signal.as_str())
                        .unwrap_or("SIGTERM"),
                    shutdown_config
                        .and_then(|c| parse_duration(&c.timeout).ok())
                        .unwrap_or(Duration::from_secs(10)),
                )
            };
            if let Some(debug) = debug.as_ref() {
                debug.docker_stop(signal_name, timeout);
            }
            docker
                .stop(signal_name, timeout)
                .await
                .map_err(|e| ServiceError::Docker(e.to_string()))?;
        }
    }
    Ok(())
}

async fn wait_for_shutdown_flag(shutdown_rx: &mut watch::Receiver<bool>) {
    if *shutdown_rx.borrow() {
        return;
    }
    while shutdown_rx.changed().await.is_ok() {
        if *shutdown_rx.borrow() {
            return;
        }
    }
}

/// Stop a service like [`stop_service`], but let an external shutdown request
/// cut in and force an immediate SIGKILL/remove path.
pub(crate) async fn stop_service_interruptibly(
    mut handle: ServiceHandle,
    shutdown_config: Option<&ShutdownConfig>,
    wait_full_exit: bool,
    mut shutdown_rx: watch::Receiver<bool>,
    debug: Option<StopDebug>,
) -> Result<(), ServiceError> {
    match handle {
        ServiceHandle::Process(ref mut process) => {
            let (signal, timeout) = if shutdown_config.is_some_and(|config| !config.graceful) {
                (Signal::SIGKILL, Duration::from_millis(500))
            } else {
                (
                    shutdown_config
                        .map(|c| parse_signal(&c.signal))
                        .unwrap_or(Signal::SIGTERM),
                    shutdown_config
                        .and_then(|c| parse_duration(&c.timeout).ok())
                        .unwrap_or(Duration::from_secs(10)),
                )
            };

            if let Some(debug) = debug.as_ref() {
                debug.signal(signal, process.pgid());
            }
            if let Err(e) = process.signal(signal)
                && !matches!(
                    e,
                    crate::sys::ProcessError::Signal {
                        source: nix::Error::ESRCH,
                        ..
                    }
                )
            {
                return Err(ServiceError::Process(e));
            }

            let shutdown_requested = {
                let wait_fut = process.wait();
                let shutdown_fut = wait_for_shutdown_flag(&mut shutdown_rx);
                tokio::pin!(wait_fut);
                tokio::pin!(shutdown_fut);
                tokio::select! {
                    result = &mut wait_fut => {
                        result?;
                        false
                    }
                    _ = tokio::time::sleep(timeout) => true,
                    _ = &mut shutdown_fut => true,
                }
            };

            if shutdown_requested {
                if let Some(debug) = debug.as_ref() {
                    debug.signal(Signal::SIGKILL, process.pgid());
                }
                if let Err(e) = process.signal(Signal::SIGKILL)
                    && !matches!(
                        e,
                        crate::sys::ProcessError::Signal {
                            source: nix::Error::ESRCH,
                            ..
                        }
                    )
                {
                    return Err(ServiceError::Process(e));
                }
                tokio::time::timeout(Duration::from_millis(500), process.wait())
                    .await
                    .map_err(|_| crate::sys::ProcessError::Unkillable {
                        pgid: process.pgid(),
                    })??;
            }

            if wait_full_exit && !process.wait_pgroup_empty(Duration::from_secs(2)).await {
                if let Some(debug) = debug.as_ref() {
                    debug.signal(Signal::SIGKILL, process.pgid());
                }
                let _ = process.signal(Signal::SIGKILL);
                let _ = process.wait_pgroup_empty(Duration::from_millis(500)).await;
            }
        }
        ServiceHandle::Docker(ref mut docker) => {
            let (signal_name, timeout) = if shutdown_config.is_some_and(|config| !config.graceful) {
                ("SIGKILL", Duration::from_millis(500))
            } else {
                (
                    shutdown_config
                        .map(|c| c.signal.as_str())
                        .unwrap_or("SIGTERM"),
                    shutdown_config
                        .and_then(|c| parse_duration(&c.timeout).ok())
                        .unwrap_or(Duration::from_secs(10)),
                )
            };

            if let Some(debug) = debug.as_ref() {
                debug.docker_stop(signal_name, timeout);
            }
            let shutdown_requested = {
                let stop_fut = docker.stop(signal_name, timeout);
                let shutdown_fut = wait_for_shutdown_flag(&mut shutdown_rx);
                tokio::pin!(stop_fut);
                tokio::pin!(shutdown_fut);
                tokio::select! {
                    result = &mut stop_fut => {
                        result.map_err(|e| ServiceError::Docker(e.to_string()))?;
                        false
                    }
                    _ = &mut shutdown_fut => true,
                }
            };
            if shutdown_requested {
                docker
                    .remove()
                    .await
                    .map_err(|e| ServiceError::Docker(e.to_string()))?;
            }
        }
    }
    Ok(())
}

// --- Preset build command and binary path helpers ---

/// Construct `cargo build` arguments from a RustConfig.
pub(crate) fn rust_build_args(config: &RustConfig) -> Vec<String> {
    let mut args = vec![
        "build".to_string(),
        "--bin".to_string(),
        config.binary.clone(),
    ];
    if !config.features.is_empty() {
        args.push("--features".to_string());
        args.push(config.features.join(","));
    }
    if config.release {
        args.push("--release".to_string());
    }
    if let Some(ref target_dir) = config.target_dir {
        args.push("--target-dir".to_string());
        args.push(target_dir.to_string_lossy().into_owned());
    }
    args.extend(config.extra_args.clone());
    args
}

/// Resolve the path to the built Rust binary.
pub(crate) fn rust_binary_path(config: &RustConfig, base_dir: &Path) -> PathBuf {
    rust_binary_path_with(config, base_dir, std::env::var_os("CARGO_TARGET_DIR"))
}

/// Resolve the built binary's path from explicit inputs.
///
/// `CARGO_TARGET_DIR` has to be honoured because the build subprocess already
/// does: it inherits the ambient environment, so cargo writes wherever the
/// variable says. Ignoring it here meant the build reported success and Don
/// then failed to spawn a binary that had just been built somewhere else —
/// which is a baffling thing to be told.
///
/// Precedence is `rust.target_dir` (an explicit choice in `don.toml`), then
/// the environment, then cargo's default of `<dir>/target`. A relative
/// `CARGO_TARGET_DIR` resolves against the directory cargo runs in, matching
/// cargo's own behaviour.
///
/// Not covered: `build.target-dir` in `.cargo/config.toml`. Reading it
/// properly means asking cargo (`cargo metadata`, or parsing
/// `--message-format=json` build output), and doing that on every start and
/// watch-triggered restart would put a subprocess on the hot path to fix a
/// rarer case than the env var. `rust.target_dir` is the escape hatch.
pub(crate) fn rust_binary_path_with(
    config: &RustConfig,
    base_dir: &Path,
    cargo_target_dir: Option<std::ffi::OsString>,
) -> PathBuf {
    let from_env = cargo_target_dir
        .map(PathBuf::from)
        .filter(|dir| !dir.as_os_str().is_empty())
        .map(|dir| {
            if dir.is_absolute() {
                dir
            } else {
                base_dir.join(dir)
            }
        });

    let target_dir = config
        .target_dir
        .clone()
        .or(from_env)
        .unwrap_or_else(|| base_dir.join("target"));
    let profile = if config.release { "release" } else { "debug" };
    target_dir.join(profile).join(&config.binary)
}

/// Construct `go build` arguments from a GoConfig.
pub(crate) fn go_build_args(config: &GoConfig, output_path: &Path) -> Vec<String> {
    let mut args = vec![
        "build".to_string(),
        "-o".to_string(),
        output_path.to_string_lossy().into_owned(),
    ];
    args.extend(config.build_flags.clone());
    if let Some(ref ldflags) = config.ldflags {
        args.push("-ldflags".to_string());
        args.push(ldflags.clone());
    }
    args.push(config.package.clone());
    args
}

/// Resolve the output path for a Go binary.
///
/// If `output` is set, uses that relative to `.don/bin/`.
/// Otherwise derives from the package path (last component).
pub(crate) fn go_binary_path(config: &GoConfig, service_name: &str, base_dir: &Path) -> PathBuf {
    let bin_dir = base_dir.join(".don").join("bin");
    let binary_name = config.output.clone().unwrap_or_else(|| {
        // Extract last component: "./cmd/api" → "api"
        Path::new(&config.package)
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| service_name.to_string())
    });
    bin_dir.join(binary_name)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_rust_build_args() {
        struct Case {
            name: &'static str,
            config: RustConfig,
            expected: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                name: "minimal",
                config: RustConfig {
                    binary: "myapp".to_string(),
                    features: vec![],
                    release: false,
                    extra_args: vec![],
                    target_dir: None,
                },
                expected: vec!["build", "--bin", "myapp"],
            },
            Case {
                name: "full",
                config: RustConfig {
                    binary: "api".to_string(),
                    features: vec!["feat1".to_string(), "feat2".to_string()],
                    release: true,
                    extra_args: vec!["--jobs".to_string(), "4".to_string()],
                    target_dir: Some(PathBuf::from("./custom-target")),
                },
                expected: vec![
                    "build",
                    "--bin",
                    "api",
                    "--features",
                    "feat1,feat2",
                    "--release",
                    "--target-dir",
                    "./custom-target",
                    "--jobs",
                    "4",
                ],
            },
            Case {
                name: "release only",
                config: RustConfig {
                    binary: "server".to_string(),
                    features: vec![],
                    release: true,
                    extra_args: vec![],
                    target_dir: None,
                },
                expected: vec!["build", "--bin", "server", "--release"],
            },
        ];

        for case in cases {
            let result = rust_build_args(&case.config);
            let expected: Vec<String> = case.expected.iter().map(|s| s.to_string()).collect();
            assert_eq!(result, expected, "{}", case.name);
        }
    }

    #[tokio::test]
    async fn ready_exec_probe_timeout_is_bounded() {
        let ready = ReadyCheck {
            exec: Some(crate::config::Command {
                cmd: "sleep".to_string(),
                args: vec!["5".to_string()],
            }),
            tcp: None,
            http: None,
            interval: "10ms".to_string(),
            retries: 1,
            timeout: "100ms".to_string(),
            monitor: false,
            monitor_interval: "10s".to_string(),
            unhealthy_after: 3,
        };

        let start = std::time::Instant::now();
        let result = run_ready_check(&ready).await;
        assert!(matches!(
            result,
            Err(ServiceError::ReadyCheckExhausted { retries: 1 })
        ));
        assert!(
            start.elapsed() < std::time::Duration::from_secs(1),
            "ready check was not bounded: {:?}",
            start.elapsed()
        );
    }

    #[test]
    fn test_rust_binary_path() {
        struct Case {
            name: &'static str,
            config: RustConfig,
            base_dir: &'static str,
            /// Stands in for `$CARGO_TARGET_DIR`. Passed explicitly rather
            /// than read from the process so the test says the same thing on
            /// a machine that has the variable exported.
            cargo_target_dir: Option<&'static str>,
            expected: &'static str,
        }

        fn config(binary: &str, release: bool, target_dir: Option<&str>) -> RustConfig {
            RustConfig {
                binary: binary.to_string(),
                features: vec![],
                release,
                extra_args: vec![],
                target_dir: target_dir.map(PathBuf::from),
            }
        }

        let cases = vec![
            Case {
                name: "debug default target",
                config: config("myapp", false, None),
                base_dir: "/project",
                cargo_target_dir: None,
                expected: "/project/target/debug/myapp",
            },
            Case {
                name: "release default target",
                config: config("myapp", true, None),
                base_dir: "/project",
                cargo_target_dir: None,
                expected: "/project/target/release/myapp",
            },
            Case {
                name: "custom target dir",
                config: config("api", false, Some("/tmp/build")),
                base_dir: "/project",
                cargo_target_dir: None,
                expected: "/tmp/build/debug/api",
            },
            Case {
                name: "CARGO_TARGET_DIR is honoured, as the build subprocess already does",
                config: config("api", false, None),
                base_dir: "/project",
                cargo_target_dir: Some("/shared/target"),
                expected: "/shared/target/debug/api",
            },
            Case {
                name: "release profile under CARGO_TARGET_DIR",
                config: config("api", true, None),
                base_dir: "/project",
                cargo_target_dir: Some("/shared/target"),
                expected: "/shared/target/release/api",
            },
            Case {
                name: "an explicit rust.target_dir outranks the environment",
                config: config("api", false, Some("/tmp/build")),
                base_dir: "/project",
                cargo_target_dir: Some("/shared/target"),
                expected: "/tmp/build/debug/api",
            },
            Case {
                name: "a relative CARGO_TARGET_DIR resolves against cargo's working directory",
                config: config("api", false, None),
                base_dir: "/project",
                cargo_target_dir: Some("build/out"),
                expected: "/project/build/out/debug/api",
            },
            Case {
                name: "an empty CARGO_TARGET_DIR is ignored, not treated as the root",
                config: config("api", false, None),
                base_dir: "/project",
                cargo_target_dir: Some(""),
                expected: "/project/target/debug/api",
            },
        ];

        for case in cases {
            let result = rust_binary_path_with(
                &case.config,
                Path::new(case.base_dir),
                case.cargo_target_dir.map(std::ffi::OsString::from),
            );
            assert_eq!(result, PathBuf::from(case.expected), "{}", case.name);
        }
    }

    #[test]
    fn test_go_build_args() {
        struct Case {
            name: &'static str,
            config: GoConfig,
            output: &'static str,
            expected: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                name: "minimal",
                config: GoConfig {
                    package: "./cmd/api".to_string(),
                    output: None,
                    build_flags: vec![],
                    ldflags: None,
                },
                output: "/tmp/bin/api",
                expected: vec!["build", "-o", "/tmp/bin/api", "./cmd/api"],
            },
            Case {
                name: "full",
                config: GoConfig {
                    package: "./cmd/server".to_string(),
                    output: Some("server".to_string()),
                    build_flags: vec!["-race".to_string()],
                    ldflags: Some("-X main.version=1.0".to_string()),
                },
                output: "/tmp/bin/server",
                expected: vec![
                    "build",
                    "-o",
                    "/tmp/bin/server",
                    "-race",
                    "-ldflags",
                    "-X main.version=1.0",
                    "./cmd/server",
                ],
            },
        ];

        for case in cases {
            let result = go_build_args(&case.config, Path::new(case.output));
            let expected: Vec<String> = case.expected.iter().map(|s| s.to_string()).collect();
            assert_eq!(result, expected, "{}", case.name);
        }
    }

    #[test]
    fn test_go_binary_path() {
        struct Case {
            name: &'static str,
            config: GoConfig,
            service_name: &'static str,
            expected_suffix: &'static str,
        }

        let cases = vec![
            Case {
                name: "derived from package",
                config: GoConfig {
                    package: "./cmd/api".to_string(),
                    output: None,
                    build_flags: vec![],
                    ldflags: None,
                },
                service_name: "api-svc",
                expected_suffix: ".don/bin/api",
            },
            Case {
                name: "explicit output",
                config: GoConfig {
                    package: "./cmd/server".to_string(),
                    output: Some("my-server".to_string()),
                    build_flags: vec![],
                    ldflags: None,
                },
                service_name: "server",
                expected_suffix: ".don/bin/my-server",
            },
            Case {
                name: "root package falls back to service name",
                config: GoConfig {
                    package: ".".to_string(),
                    output: None,
                    build_flags: vec![],
                    ldflags: None,
                },
                service_name: "myapp",
                // "." has no file_name, but Path::new(".").file_name() is None on some platforms
                // The fallback should use the service name
                expected_suffix: ".don/bin/myapp",
            },
        ];

        for case in cases {
            let base = Path::new("/project");
            let result = go_binary_path(&case.config, case.service_name, base);
            assert!(
                result.ends_with(case.expected_suffix),
                "{}: expected to end with '{}', got '{}'",
                case.name,
                case.expected_suffix,
                result.display()
            );
        }
    }
}
