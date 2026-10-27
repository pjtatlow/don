//! Unix-socket HTTP API — CLI ↔ daemon communication.
//!
//! Serves an axum router over `.don/don.sock` so CLI subcommands like
//! `don status`, `don stop`, `don logs` can talk to the running daemon.
//! The runner binds the socket synchronously (so bind errors surface
//! immediately) and then spawns the accept loop as a background task.

pub(crate) mod attach;
pub(crate) mod routes;

use crate::runner::{EventLog, RunnerCommand};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::net::UnixListener;
use tokio::sync::mpsc;

/// Server errors.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("failed to bind unix socket '{}': {source}", path.display())]
    Bind {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to set socket permissions '{}': {source}", path.display())]
    Chmod {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("server accept error: {0}")]
    Accept(#[source] std::io::Error),
}

/// Map of active attach resize channels: service name → sender.
/// Per-process attach sessions: each client's requested terminal size plus a
/// sender into the process's PTY input gate. The effective PTY/grid size is
/// `min(cols) x min(rows)` over the attached clients (tmux-style), recomputed
/// on attach, resize, and detach. Zero clients: the last size is retained so
/// a running program sees no SIGWINCH churn.
pub(crate) struct NameSessions {
    pub next_id: u64,
    pub gate: mpsc::Sender<crate::output::PtyInput>,
    pub sizes: std::collections::HashMap<u64, (u16, u16)>,
}
type SessionMap = std::collections::HashMap<String, NameSessions>;

/// Shared state passed to all handlers.
#[derive(Clone)]
pub(crate) struct ApiState {
    pub cmd_tx: mpsc::UnboundedSender<RunnerCommand>,
    /// Runner event broadcast, used by `GET /events` to stream state changes.
    /// Subscribing per-request keeps the API decoupled from the runner loop —
    /// a slow HTTP client lags its own receiver and nobody else's.
    pub event_tx: EventLog,
    /// Read-only view of runner state, updated on every transition.
    ///
    /// Handlers read it without touching `cmd_tx`, so a status query stays
    /// answerable while the runner is busy, and they can *wait* on it from
    /// their own task — safe precisely because they are not the runner's
    /// command loop.
    pub state: crate::runner::StateReader,
    /// Read-only view of every process's buffered output. Same rationale as
    /// `state`: `GET /logs` answers off the ring buffers directly instead of
    /// queueing a command behind the runner's current work.
    pub logs: crate::output::LogReader,
    /// Task-param completion resolver — same rationale again: a slow
    /// completions command blocks its own request, never the runner.
    pub completions: crate::param_completions::CompletionResolver,
    /// Read-only handle for the global watch report — `GET /watch` queries
    /// the watch manager directly, without the runner.
    pub watch_status: crate::watch::report::WatchStatusReader,
    /// What a client may ask a process to do. Name checks are answered here
    /// without waking the scheduler; the rest is addressed at supervisors.
    pub control: crate::control::ProcessControl,
    /// Attach sessions, opened against the per-process output state where
    /// the supervisor registered the live spawn's PTY gate. Detach is the
    /// session guard dropping — there is no request for it.
    pub attach: crate::output::attach::AttachControl,
    /// Active attach sessions per process — see [`NameSessions`].
    pub attach_sessions: std::sync::Arc<tokio::sync::Mutex<SessionMap>>,
    /// Handle to the server-side terminal-emulator thread — resize
    /// requests keep the emulated grid in step with the attached client.
    pub emulator: crate::output::emulator::EmulatorHandle,
    /// Merged formatted log stream, used by `GET /logs?follow=true`.
    /// Subscribing per-request keeps followers independent: a slow client
    /// lags its own receiver and nobody else's.
    pub log_tap: crate::output::MergedLogTap,
    /// The server's shutdown signal, observed by *streaming* handlers.
    ///
    /// A follow response holds an `ApiState` clone — and with it live
    /// `event_tx` / `log_tap` senders — for as long as it streams. If the
    /// stream only ended on channel closure, a connected follower would keep
    /// the very senders open whose closure it is waiting for, and the
    /// process could never exit. (The TUI is exactly such a follower.)
    /// Streams must end when this flips instead.
    pub shutdown: tokio::sync::watch::Receiver<bool>,
}

/// Bind the unix socket at `socket_path` and chmod it to 0o600 so only the
/// owner can connect. Removes any stale socket file first. Returns the
/// listener on success; errors surface synchronously so the runner can log
/// them visibly at startup.
pub fn bind_api(socket_path: &Path) -> Result<UnixListener, ServerError> {
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| ServerError::Bind {
            path: socket_path.to_path_buf(),
            source,
        })?;
    }
    // Remove stale socket file (crashed previous run, etc.).
    let _ = std::fs::remove_file(socket_path);

    let listener = UnixListener::bind(socket_path).map_err(|source| ServerError::Bind {
        path: socket_path.to_path_buf(),
        source,
    })?;

    // Restrict to owner-only. The API can stop services / read logs; anyone
    // else on the box shouldn't get to drive it.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o600)).map_err(
            |source| ServerError::Chmod {
                path: socket_path.to_path_buf(),
                source,
            },
        )?;
    }

    Ok(listener)
}

/// Serve the API on a pre-bound listener until `shutdown` is signalled.
///
/// The socket file at `socket_path` is removed on exit (including panic,
/// via the [`SocketGuard`] Drop impl).
#[allow(clippy::too_many_arguments)]
pub async fn serve_api(
    listener: UnixListener,
    socket_path: PathBuf,
    cmd_tx: mpsc::UnboundedSender<RunnerCommand>,
    event_tx: EventLog,
    state: crate::runner::StateReader,
    emulator: crate::output::emulator::EmulatorHandle,
    log_tap: crate::output::MergedLogTap,
    logs: crate::output::LogReader,
    completions: crate::param_completions::CompletionResolver,
    watch_status: crate::watch::report::WatchStatusReader,
    attach: crate::output::attach::AttachControl,
    control: crate::control::ProcessControl,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), ServerError> {
    let _guard = SocketGuard(socket_path);
    let state = Arc::new(ApiState {
        cmd_tx,
        event_tx,
        state,
        emulator,
        attach_sessions: std::sync::Arc::new(tokio::sync::Mutex::new(
            std::collections::HashMap::new(),
        )),
        log_tap,
        logs,
        completions,
        watch_status,
        attach,
        control,
        shutdown: shutdown.clone(),
    });
    let app = routes::build_router(state);
    accept_loop(listener, app, shutdown).await
}

/// Bind this project's API socket and serve it for `runner`.
///
/// Lives here, not on the runner: a runner has no business knowing an API
/// exists, and `server -> runner` is the direction that doesn't close a
/// cycle. Returns the shutdown sender, which the caller hands back with
/// [`Runner::set_api_shutdown`] so the runner can stop accepting at the point
/// in teardown it already chose.
///
/// Binding is synchronous so the socket exists before this returns — a client
/// that sees the process start can connect immediately, with no window where
/// `.don/don.sock` is missing.
///
/// [`Runner::set_api_shutdown`]: crate::Runner::set_api_shutdown
pub fn serve_for_runner(
    runner: &crate::runner::Runner,
) -> Result<tokio::sync::watch::Sender<bool>, ServerError> {
    let socket_path = runner.base_dir().join(".don").join("don.sock");
    let socket_path = socket_path.as_path();
    let emitter = runner.lifecycle_emitter();
    let listener = bind_api(socket_path)?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let cmd_tx = runner.command_sender();
    let event_tx = runner.subscribe_sender();
    let state = runner.state_reader();
    let emulator = runner.emulator_handle();
    let log_tap = runner.log_stream_sender();
    let logs = runner.log_reader();
    let completions = runner.completion_resolver();
    let watch_status = runner.watch_status_reader();
    let attach = runner.attach_control();
    let control = runner.process_control();
    let path = socket_path.to_path_buf();
    let display = socket_path.display().to_string();
    // Deliberately NO LifecycleEmitter in this task: an emitter holds a
    // sink sender, and this task outlives the output flush (it ends on the
    // post-flush shutdown flip) — holding one would stall the flush until
    // its 2s abort. Accept-loop errors are rare and terminal; stderr is
    // honest enough (in fork mode that's `.don/logs/runner.log`).
    tokio::spawn(async move {
        if let Err(e) = serve_api(
            listener,
            path,
            cmd_tx,
            event_tx,
            state,
            emulator,
            log_tap,
            logs,
            completions,
            watch_status,
            attach,
            control,
            shutdown_rx,
        )
        .await
        {
            eprintln!("don: api server error: {e}");
        }
    });
    emitter.lifecycle_event(&format!("api listening on {display}"));
    Ok(shutdown_tx)
}

/// Serve an arbitrary router on a pre-bound unix listener until `shutdown`.
///
/// Same lifecycle as [`serve_api`] — including removing the socket file on
/// exit — but for callers that build their own router. The daemon's control
/// plane ([`crate::daemon`]) uses this.
pub(crate) async fn serve_router(
    listener: UnixListener,
    socket_path: PathBuf,
    app: axum::Router,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), ServerError> {
    let _guard = SocketGuard(socket_path);
    accept_loop(listener, app, shutdown).await
}

/// RAII guard that removes the socket file on drop (normal exit or panic).
struct SocketGuard(PathBuf);

impl Drop for SocketGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Serve `app` on `listener` until `shutdown` flips to true.
///
/// Shared by the project API and the daemon's control socket
/// ([`crate::daemon`]) — both are axum routers over a `UnixListener`, so
/// neither needs its own accept loop.
pub(crate) async fn accept_loop(
    listener: UnixListener,
    app: axum::Router,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), ServerError> {
    loop {
        tokio::select! {
            accept = listener.accept() => {
                let (stream, _addr) = accept.map_err(ServerError::Accept)?;
                let io = TokioIo::new(stream);
                let tower_service = app.clone();
                tokio::spawn(async move {
                    let hyper_service =
                        hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                            use tower::ServiceExt;
                            tower_service.clone().oneshot(req)
                        });
                    let _ = auto::Builder::new(TokioExecutor::new())
                        .serve_connection_with_upgrades(io, hyper_service)
                        .await;
                });
            }
            changed = shutdown.changed() => {
                // `Err` means the sender is gone. Treat that as shutdown:
                // `changed()` would return immediately and forever, spinning
                // this loop, and an owner that dropped the signal is not
                // coming back to set it.
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
        }
    }
}
