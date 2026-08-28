//! HTTP endpoints for the unix socket API.

use super::ApiState;
use crate::runner::{CommandError, CompletionError, ProcessStatus, RunnerCommand, WatchReport};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::oneshot;

/// Build the axum router for the API.
pub(crate) fn build_router(state: Arc<ApiState>) -> Router {
    Router::new()
        .route("/status", get(get_status))
        .route("/ready", get(get_ready))
        .route("/events", get(get_events))
        .route("/watch", get(get_watch))
        .route("/start/{name}", post(post_start))
        .route("/stop/{name}", post(post_stop))
        .route("/restart/{name}", post(post_restart))
        .route("/hard-restart/{name}", post(post_hard_restart))
        .route("/shutdown", post(post_shutdown))
        .route("/logs", get(get_all_logs))
        .route("/logs/{name}", get(get_logs))
        .route("/attach/{name}", get(super::attach::attach_handler))
        .route("/attach/{name}/resize", post(super::attach::resize_handler))
        .route("/run/{name}", post(post_run_task))
        .route(
            "/completions/{task}/{param}",
            post(post_resolve_completions),
        )
        .with_state(state)
}

/// Query params for the status endpoint.
#[derive(serde::Deserialize)]
struct StatusQuery {
    #[serde(default)]
    verbose: bool,
    /// Restrict the response to a single service/task and include its full
    /// resolved watch path list. Omit to list all processes (path list elided).
    #[serde(default)]
    name: Option<String>,
}

/// `GET /status` — list all services/tasks and their current state.
///
/// The non-verbose answer comes straight out of the state projection, so it
/// stays fast while the runner is mid-startup. Verbose still goes through the
/// command channel: it needs a watch-manager round trip and per-service ready
/// check resolution, neither of which belongs in a snapshot republished on
/// every transition.
async fn get_status(
    State(state): State<Arc<ApiState>>,
    axum::extract::Query(query): axum::extract::Query<StatusQuery>,
) -> Response {
    if !query.verbose {
        let snapshot = state.state.snapshot();
        let processes = match query.name {
            Some(want) => snapshot
                .processes
                .iter()
                .filter(|process| process_name(process) == want)
                .cloned()
                .collect(),
            None => snapshot.processes.clone(),
        };
        return Json(StatusResponse { processes }).into_response();
    }
    let (tx, rx) = oneshot::channel();
    if state
        .cmd_tx
        .send(RunnerCommand::Status {
            verbose: query.verbose,
            name: query.name,
            reply: tx,
        })
        .is_err()
    {
        return runner_unavailable();
    }
    match rx.await {
        Ok(statuses) => Json(StatusResponse {
            processes: statuses,
        })
        .into_response(),
        Err(_) => runner_unavailable(),
    }
}

#[derive(Serialize)]
struct StatusResponse {
    processes: Vec<ProcessStatus>,
}

/// The name of a service or task, for filtering a snapshot by `?name=`.
fn process_name(process: &ProcessStatus) -> &str {
    match process {
        ProcessStatus::Service { name, .. } | ProcessStatus::Task { name, .. } => name,
    }
}

#[derive(Serialize)]
struct ReadyResponse {
    /// Whether the initial startup sweep has decided every process.
    startup_complete: bool,
    /// The runner's crate version. A long-lived runner can outlive a `don`
    /// upgrade; clients compare and warn instead of misbehaving quietly.
    version: &'static str,
}

/// `GET /ready` — whether the runner has finished its initial sweep.
///
/// Answers immediately either way; it's a status read, not a wait. Clients
/// that would rather not offer a "run" button during startup can poll this or
/// watch for `startup_settled` on `GET /events`.
async fn get_ready(State(state): State<Arc<ApiState>>) -> Response {
    Json(ReadyResponse {
        startup_complete: state.state.snapshot().startup_complete,
        version: env!("CARGO_PKG_VERSION"),
    })
    .into_response()
}

/// `GET /events` — stream runner state changes as newline-delimited JSON.
///
/// The first record is always `{"type":"snapshot","processes":[...],
/// "startup_complete":bool}` — the full current state, so a client that
/// connects (or reconnects) starts consistent instead of stale-until-the-
/// next-event. Existing consumers that switch on `type` ignore it.
///
/// Each line is a serialized [`RunnerEvent`] (`{"type": "...", ...}`). The
/// stream stays open until the client disconnects or the runner shuts down;
/// consumers should treat `shutdown_complete` as the terminator.
///
/// A consumer too slow to keep up with the broadcast gets
/// `{"type":"lagged","skipped":N}` instead of silently missing transitions —
/// the correct response is to refetch `GET /status` and resync.
async fn get_events(State(state): State<Arc<ApiState>>) -> Response {
    // Subscribe *before* reading the snapshot: an event that fires between
    // the two lands in the subscription and is applied after the snapshot —
    // replay-safe. The other order loses it entirely.
    let mut events_rx = state.event_tx.subscribe();
    let snapshot = state.state.snapshot();
    let preamble = serde_json::json!({
        "type": "snapshot",
        "processes": snapshot.processes,
        "startup_complete": snapshot.startup_complete,
    });

    // Forwarder task rather than stream combinators so the stream can end on
    // the server's shutdown signal — see `ApiState::shutdown` for why ending
    // only on channel closure would deadlock the exit path.
    let (tx, rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(256);
    let mut shutdown = state.shutdown.clone();
    tokio::spawn(async move {
        let mut chunk = serde_json::to_vec(&preamble).unwrap_or_default();
        chunk.push(b'\n');
        if tx.send(bytes::Bytes::from(chunk)).await.is_err() {
            return;
        }
        loop {
            let value = tokio::select! {
                process = events_rx.recv() => match process {
                    Ok(event) => serde_json::to_value(&event).unwrap_or_else(|_| {
                        serde_json::json!({ "type": "error", "message": "event serialization failed" })
                    }),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        serde_json::json!({ "type": "lagged", "skipped": skipped })
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                },
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        // Deliver what's already buffered — the shutdown
                        // narration was published before this signal fired
                        // (the runner flips it after the output flush) —
                        // then end the stream.
                        while let Ok(event) = events_rx.try_recv() {
                            let value = serde_json::to_value(&event)
                                .unwrap_or_else(|_| serde_json::json!({ "type": "error" }));
                            let mut chunk = serde_json::to_vec(&value).unwrap_or_default();
                            chunk.push(b'\n');
                            if tx.send(bytes::Bytes::from(chunk)).await.is_err() {
                                return;
                            }
                        }
                        return;
                    }
                    continue;
                }
            };
            let mut chunk = serde_json::to_vec(&value).unwrap_or_default();
            chunk.push(b'\n');
            if tx.send(bytes::Bytes::from(chunk)).await.is_err() {
                return;
            }
        }
    });

    use tokio_stream::{StreamExt, wrappers::ReceiverStream};
    let stream = ReceiverStream::new(rx).map(Ok::<_, std::convert::Infallible>);

    match axum::response::Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/x-ndjson")
        .body(axum::body::Body::from_stream(stream))
    {
        Ok(resp) => resp,
        Err(_) => runner_unavailable(),
    }
}

/// `GET /watch` — global file-watch state (inotify dirs + per-process patterns).
async fn get_watch(State(state): State<Arc<ApiState>>) -> Response {
    // Straight to the watch manager — the runner is not involved. `None`
    // (no watcher, or it didn't answer) serializes as JSON null, exactly
    // the old "no watches active" reply.
    let watch = state.watch_status.report().await;
    Json(WatchResponse { watch }).into_response()
}

#[derive(Serialize)]
struct WatchResponse {
    /// `None` when no watches are active (serialized as JSON `null`).
    watch: Option<WatchReport>,
}

/// `POST /start/:name` — start a stopped service.
async fn post_start(State(state): State<Arc<ApiState>>, Path(name): Path<String>) -> Response {
    let reply = state.control.start(&name).await;
    map_command_reply(&name, reply, StatusCode::INTERNAL_SERVER_ERROR).await
}

/// `POST /stop/:name` — stop a running service, or end a running task's run.
async fn post_stop(State(state): State<Arc<ApiState>>, Path(name): Path<String>) -> Response {
    let reply = state.control.stop(&name).await;
    map_command_reply(&name, reply, StatusCode::INTERNAL_SERVER_ERROR).await
}

/// `POST /restart/:name` — restart a service.
async fn post_restart(State(state): State<Arc<ApiState>>, Path(name): Path<String>) -> Response {
    let reply = state.control.restart(&name).await;
    map_command_reply(&name, reply, StatusCode::INTERNAL_SERVER_ERROR).await
}

/// `POST /hard-restart/:name` — kill and restart without graceful stop.
async fn post_hard_restart(
    State(state): State<Arc<ApiState>>,
    Path(name): Path<String>,
) -> Response {
    let reply = state.control.hard_restart(&name).await;
    map_command_reply(&name, reply, StatusCode::INTERNAL_SERVER_ERROR).await
}

#[derive(Deserialize)]
struct ShutdownQuery {
    #[serde(default)]
    force: bool,
}

/// `POST /shutdown` — gracefully stop the daemon.
///
/// `?force=true` escalates exactly like a second Ctrl+C: in-flight
/// graceful stops SIGKILL their process groups and shutdown stops
/// waiting. Idempotent with the graceful request — the runner's shutdown
/// guard ignores the repeat, and the escalation flag does the rest.
async fn post_shutdown(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<ShutdownQuery>,
) -> Response {
    if query.force {
        crate::signals::request_force_shutdown();
    }
    if state.control.shutdown().is_err() {
        return runner_unavailable();
    }
    StatusCode::NO_CONTENT.into_response()
}

#[derive(Deserialize)]
struct LogsQuery {
    #[serde(default = "default_last")]
    last: usize,
    #[serde(default)]
    follow: bool,
    /// Resume the merged stream from this id. A client that was disconnected
    /// asks for exactly what it missed instead of guessing with `last`.
    #[serde(default)]
    since: Option<u64>,
}

fn default_last() -> usize {
    100
}

#[derive(Serialize)]
struct LogsResponse {
    name: String,
    lines: Vec<String>,
}

/// `GET /logs/:name?last=N[&follow=true]` — read from the ring buffer.
///
/// - Without `follow`: returns last N lines as JSON.
/// - With `follow=true`: streams newline-delimited JSON objects (NDJSON)
///   — one `{"line":"..."}` per log line. Closes when the client disconnects.
async fn get_logs(
    State(state): State<Arc<ApiState>>,
    Path(name): Path<String>,
    Query(query): Query<LogsQuery>,
) -> Response {
    if query.follow {
        return follow_logs(state, name, query.last).await;
    }

    // Straight off the ring buffer — no runner round trip, so reading logs
    // never queues behind whatever the runner is currently doing.
    match state.logs.read_logs(&name, query.last).await {
        Some(raw) => {
            let raw = String::from_utf8_lossy(&raw);
            let lines: Vec<String> = if raw.is_empty() {
                Vec::new()
            } else {
                raw.split('\n').map(String::from).collect()
            };
            Json(LogsResponse { name, lines }).into_response()
        }
        None => not_found(&name),
    }
}

/// `GET /logs?follow=true` — stream the merged, formatted log stream: every
/// process plus `[don]` lifecycle events, in arrival order, exactly what the
/// terminal sees. NDJSON, one record per line:
///
/// - `{"id":42,"name":"api","lifecycle":false,"line":"..."}` — a log line.
///   `id` is its place in the merged stream, the same number every client sees
///   for that line. `name` is the owning process; `lifecycle` is true for
///   `[don]` events. `line` is the formatted bytes (ANSI colors included) as
///   lossy UTF-8, no trailing newline.
/// - `{"dropped":n,"resumed_at":500}` — `n` lines are gone for good and the
///   stream continues from `resumed_at`. Only emitted when the server's own
///   history could not cover the gap; an ordinary slow reader is caught up
///   silently. A gap you can see beats a stream you wrongly trust.
///
/// `since=<id>` resumes exactly where a disconnected client stopped. Without
/// it, `last=N` (default 100) preloads the tail of the merged history, so a
/// late joiner sees what was happening just before it attached. The overlap
/// between preload and live is spliced by id, so nothing is doubled and
/// nothing is missing.
async fn get_all_logs(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<LogsQuery>,
) -> Response {
    if !query.follow {
        return (
            StatusCode::BAD_REQUEST,
            "merged logs are follow-only: use /logs?follow=true, or /logs/:name for history",
        )
            .into_response();
    }
    // One cursor covers preload, live, the overlap between them, and its own
    // lag — see `MergedLogCursor`.
    let mut cursor = state
        .log_tap
        .cursor(query.since.map(crate::output::LogId), query.last)
        .await;
    // Bridge cursor → mpsc so a gone client (send error) tears the forwarder
    // down. Ends on the shutdown signal too — see `ApiState::shutdown`.
    let (tx, rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(256);
    let mut shutdown = state.shutdown.clone();
    tokio::spawn(async move {
        fn record(event: crate::output::MergedEvent) -> bytes::Bytes {
            let value = match event {
                crate::output::MergedEvent::Line(entry) => serde_json::json!({
                    "id": entry.id,
                    "name": entry.line.name,
                    "lifecycle": entry.line.is_lifecycle,
                    "verbose": entry.line.is_verbose,
                    // The rendered name column, sent beside the message rather
                    // than glued to it: a client that wants one string joins
                    // them, one that lays the name out as a column already has
                    // the split. See `FormattedLogLine::prefix`.
                    "prefix": String::from_utf8_lossy(&entry.line.prefix),
                    "line": String::from_utf8_lossy(&entry.line.bytes),
                }),
                crate::output::MergedEvent::Dropped { count, resumed_at } => serde_json::json!({
                    "dropped": count,
                    "resumed_at": resumed_at,
                }),
            };
            let mut chunk = serde_json::to_vec(&value).unwrap_or_default();
            chunk.push(b'\n');
            bytes::Bytes::from(chunk)
        }

        loop {
            let event = tokio::select! {
                event = cursor.recv() => match event {
                    Some(event) => event,
                    None => return,
                },
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        // Same contract as the events stream: the final lines
                        // are already buffered in the cursor, so deliver them
                        // before closing.
                        while let Some(event) = cursor.try_recv() {
                            if tx.send(record(event)).await.is_err() {
                                return;
                            }
                        }
                        return;
                    }
                    continue;
                }
            };
            if tx.send(record(event)).await.is_err() {
                return;
            }
        }
    });

    use tokio_stream::{StreamExt, wrappers::ReceiverStream};
    let stream = ReceiverStream::new(rx).map(Ok::<_, std::convert::Infallible>);
    match axum::response::Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/x-ndjson")
        .body(axum::body::Body::from_stream(stream))
    {
        Ok(resp) => resp,
        Err(_) => runner_unavailable(),
    }
}

async fn follow_logs(state: Arc<ApiState>, name: String, last: usize) -> Response {
    // 256-line buffer — slow HTTP clients will drop lines (and get pruned on
    // disconnect) rather than blocking service output.
    let Some(sink_rx) = state.logs.add_follow_sink(&name, last, 256).await else {
        return not_found(&name);
    };

    // Build an NDJSON stream: one `{"line":"..."}\n` per SinkLine.
    use tokio_stream::{StreamExt, wrappers::ReceiverStream};
    let stream = ReceiverStream::new(sink_rx).map(|sink_line| {
        let line_str = String::from_utf8_lossy(&sink_line.line).into_owned();
        let json = serde_json::json!({ "line": line_str, "verbose": sink_line.is_verbose });
        let mut chunk = serde_json::to_vec(&json).unwrap_or_default();
        chunk.push(b'\n');
        Ok::<_, std::convert::Infallible>(bytes::Bytes::from(chunk))
    });

    match axum::response::Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/x-ndjson")
        .body(axum::body::Body::from_stream(stream))
    {
        Ok(resp) => resp,
        Err(_) => runner_unavailable(),
    }
}

/// Body of `POST /run/:name` — carries the user-supplied param values.
/// Empty / absent body is accepted and treated as `params = {}`.
#[derive(Default, Deserialize)]
struct RunTaskBody {
    #[serde(default)]
    params: HashMap<String, String>,
    #[serde(default)]
    wait: bool,
    #[serde(default)]
    wait_timeout: Option<String>,
}

/// `POST /run/:name` — run a specific task by name, bypassing auto_run.
/// The request body may carry `{"params": {...}}` with user-supplied values
/// for any declared task params.
async fn post_run_task(
    State(state): State<Arc<ApiState>>,
    Path(name): Path<String>,
    body: Option<Json<RunTaskBody>>,
) -> Response {
    let body = body.map(|Json(b)| b).unwrap_or_default();
    let wait = body.wait || body.wait_timeout.is_some();

    // Asked for now, not once startup settles. This used to wait for the whole
    // sweep to finish, because during it every task has a worker attached
    // working out skip/pending/run and that was indistinguishable from a real
    // run — so a request landing here came back "already running" for a task
    // that was not. The supervisor tells those apart now (see `resolve_command`
    // and its `spawned` flag), and the wait was the worse half of the bargain:
    // in a workspace whose startup takes minutes, pressing run meant waiting
    // minutes for something unrelated.
    let reply = state
        .control
        .run_task(&name, body.params, wait, body.wait_timeout)
        .await;
    let failed_status = if wait {
        StatusCode::UNPROCESSABLE_ENTITY
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    map_command_reply(&name, reply, failed_status).await
}

/// Shared command-reply-to-response mapping, shared between `post_run_task`
/// (which can't use `dispatch_control_cmd` because the command has a third
/// field beyond `name` + `reply`) and the other control endpoints.
async fn map_command_reply(
    name: &str,
    reply: crate::control::ControlResult,
    failed_status: StatusCode,
) -> Response {
    match reply {
        Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err(CommandError::UnknownService { .. } | CommandError::UnknownTask { .. })) => {
            not_found(name)
        }
        Ok(Err(e @ (CommandError::NotAService { .. } | CommandError::NotATask { .. }))) => {
            (StatusCode::BAD_REQUEST, Json(error_body(&e.to_string()))).into_response()
        }
        Ok(Err(e @ CommandError::InvalidState { .. })) => {
            (StatusCode::CONFLICT, Json(error_body(&e.to_string()))).into_response()
        }
        Ok(Err(e @ CommandError::InvalidParams { .. })) => {
            (StatusCode::BAD_REQUEST, Json(error_body(&e.to_string()))).into_response()
        }
        Ok(Err(e @ CommandError::Failed { .. })) => {
            (failed_status, Json(error_body(&e.to_string()))).into_response()
        }
        Ok(Err(e @ CommandError::TimedOut { .. })) => (
            StatusCode::REQUEST_TIMEOUT,
            Json(error_body(&e.to_string())),
        )
            .into_response(),
        Err(_) => runner_unavailable(),
    }
}

/// Body of `POST /completions/:task/:param`.
#[derive(Default, Deserialize)]
struct CompletionsBody {
    /// Already-entered values for *other* params in the form, exposed to
    /// the completion command as `DON_PARAM_<NAME>`.
    #[serde(default)]
    partial: HashMap<String, String>,
    /// When true, skip the cache and rerun the command.
    #[serde(default)]
    force_refresh: bool,
}

#[derive(Serialize)]
struct CompletionsResponse {
    values: Vec<String>,
}

/// `POST /completions/:task/:param` — resolve candidate values for one
/// param. Runs the configured `completions` command (or returns static
/// `choices`). Errors carry a `log_path` when a failure log was written.
async fn post_resolve_completions(
    State(state): State<Arc<ApiState>>,
    Path((task, param)): Path<(String, String)>,
    body: Option<Json<CompletionsBody>>,
) -> Response {
    let body = body.map(|Json(b)| b).unwrap_or_default();
    // Resolved right here on the request task — task configs are fixed at
    // construction and the cache is shared, so a slow completion command
    // blocks only this request, never the runner.
    match state
        .completions
        .resolve_param(&task, &param, &body.partial, body.force_refresh)
        .await
    {
        Ok(values) => Json(CompletionsResponse { values }).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, Json(completion_error_body(&e))).into_response(),
    }
}

fn completion_error_body(e: &CompletionError) -> serde_json::Value {
    serde_json::json!({
        "error": e.message,
        "log_path": e.log_path,
    })
}

// --- helpers ---

fn runner_unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(error_body("runner is shutting down")),
    )
        .into_response()
}

fn not_found(name: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(error_body(&format!("no service or task named '{name}'"))),
    )
        .into_response()
}

fn error_body(message: &str) -> serde_json::Value {
    serde_json::json!({ "error": message })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::runner::{ServiceState, StateReader};
    use crate::state_store::{self, StateSnapshot};
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    /// Build a router whose command channel is already dead.
    ///
    /// Any handler that reaches for `cmd_tx` answers 503 here, so a 200 is
    /// proof the response came from the state projection alone.
    fn router_without_a_runner(state: StateReader) -> Router {
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        drop(cmd_rx);
        let (event_tx, _) = tokio::sync::broadcast::channel(16);
        let log_tap = crate::output::MergedLogTap::for_tests();
        let logs = crate::output::LogReader::for_tests();
        let completions = crate::param_completions::CompletionResolver::for_tests();
        let watch_status = crate::watch::report::WatchStatusReader::for_tests();
        let attach = crate::output::attach::AttachControl::for_tests();
        let control = crate::control::ProcessControl::for_tests();
        let (shutdown_tx, shutdown) = tokio::sync::watch::channel(false);
        // Leak the sender so the receiver stays live for the router's
        // lifetime — tests never signal shutdown.
        std::mem::forget(shutdown_tx);
        build_router(Arc::new(ApiState {
            cmd_tx,
            event_tx,
            state,
            emulator: crate::output::emulator::spawn_emulator_thread(),
            attach_sessions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            log_tap,
            logs,
            completions,
            watch_status,
            attach,
            control,
            shutdown,
        }))
    }

    async fn get(router: Router, uri: &str) -> (StatusCode, serde_json::Value) {
        let response = router
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, value)
    }

    fn snapshot() -> StateSnapshot {
        StateSnapshot {
            processes: vec![
                ProcessStatus::Service {
                    runtime: None,
                    name: "api".to_string(),
                    state: ServiceState::Ready,
                    failed_dependencies: Vec::new(),
                    verbose: None,
                },
                ProcessStatus::Service {
                    runtime: None,
                    name: "web".to_string(),
                    state: ServiceState::Starting,
                    failed_dependencies: Vec::new(),
                    verbose: None,
                },
            ],
            startup_complete: true,
        }
    }

    /// The whole point of the projection: the endpoints a client polls while
    /// the runner is busy must not queue behind the runner's command loop.
    /// These answer with the command channel severed entirely.
    #[tokio::test]
    async fn read_endpoints_answer_without_touching_the_command_channel() {
        struct Case {
            name: &'static str,
            uri: &'static str,
            want_status: StatusCode,
            /// Process names expected in `processes`, in order. Empty for `/ready`.
            want_processes: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                name: "status lists every process",
                uri: "/status",
                want_status: StatusCode::OK,
                want_processes: vec!["api", "web"],
            },
            Case {
                name: "status filters by name",
                uri: "/status?name=web",
                want_status: StatusCode::OK,
                want_processes: vec!["web"],
            },
            Case {
                name: "an unknown name is an empty list, not an error",
                uri: "/status?name=nope",
                want_status: StatusCode::OK,
                want_processes: vec![],
            },
            Case {
                name: "ready reports the startup flag",
                uri: "/ready",
                want_status: StatusCode::OK,
                want_processes: vec![],
            },
            // Verbose still needs the runner — it resolves ready checks and
            // queries the watch manager, neither of which is in the snapshot.
            Case {
                name: "verbose status still goes through the runner",
                uri: "/status?verbose=true",
                want_status: StatusCode::SERVICE_UNAVAILABLE,
                want_processes: vec![],
            },
        ];

        for case in cases {
            let (writer, reader) = state_store::channel(snapshot());
            let (status, body) = get(router_without_a_runner(reader), case.uri).await;
            assert_eq!(status, case.want_status, "{}: status code", case.name);
            if case.want_status == StatusCode::OK && case.uri.starts_with("/status") {
                let names: Vec<&str> = body["processes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|process| process["name"].as_str().unwrap())
                    .collect();
                assert_eq!(names, case.want_processes, "{}: processes", case.name);
            }
            if case.uri == "/ready" {
                assert_eq!(body["startup_complete"], true, "{}: flag", case.name);
            }
            drop(writer);
        }
    }

    /// `/status` reflects a transition as soon as the runner publishes it —
    /// the reader is not a cache with its own lifetime.
    #[tokio::test]
    async fn status_tracks_republished_state() {
        let (writer, reader) = state_store::channel(snapshot());
        let router = router_without_a_runner(reader);

        let (_, before) = get(router.clone(), "/status?name=web").await;
        assert_eq!(before["processes"][0]["state"], "starting");

        writer.publish_processes(vec![ProcessStatus::Service {
            runtime: None,
            name: "web".to_string(),
            state: ServiceState::Ready,
            failed_dependencies: Vec::new(),
            verbose: None,
        }]);

        let (_, after) = get(router, "/status?name=web").await;
        assert_eq!(after["processes"][0]["state"], "ready");
    }
}
