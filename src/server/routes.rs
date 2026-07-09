//! HTTP endpoints for the unix socket API.

use super::ApiState;
use crate::runner::{CommandError, CompletionError, ItemStatus, RunnerCommand, WatchReport};
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
        .route("/info", get(get_info))
        .route("/status", get(get_status))
        .route("/watch", get(get_watch))
        .route("/snapshot", get(super::stream::get_snapshot))
        .route("/events", get(super::stream::get_events))
        .route("/logstream", get(super::stream::get_logstream))
        .route("/start/{name}", post(post_start))
        .route("/stop/{name}", post(post_stop))
        .route("/restart/{name}", post(post_restart))
        .route("/hard-restart/{name}", post(post_hard_restart))
        .route("/verbose", post(post_verbose))
        .route("/shutdown", post(post_shutdown))
        .route("/logs/{name}", get(get_logs))
        .route("/attach/{name}", get(super::attach::attach_handler))
        .route("/attach/{name}/resize", post(super::attach::resize_handler))
        .route("/run-pending", post(post_run_pending))
        .route("/run/{name}", post(post_run_task))
        .route(
            "/completions/{task}/{param}",
            post(post_resolve_completions),
        )
        .with_state(state)
}

/// `GET /info` — daemon identity: version and whether it owns an interactive
/// terminal. `headless: true` means foreground tasks run on parked PTYs and
/// clients must bridge in via attach.
async fn get_info() -> Response {
    Json(serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "headless": !crate::runner::has_interactive_terminal(),
    }))
    .into_response()
}

/// Query params for the status endpoint.
#[derive(serde::Deserialize)]
struct StatusQuery {
    #[serde(default)]
    verbose: bool,
    /// Restrict the response to a single service/task and include its full
    /// resolved watch path list. Omit to list all items (path list elided).
    #[serde(default)]
    name: Option<String>,
}

/// `GET /status` — list all services/tasks and their current state.
async fn get_status(
    State(state): State<Arc<ApiState>>,
    axum::extract::Query(query): axum::extract::Query<StatusQuery>,
) -> Response {
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
        Ok(statuses) => Json(StatusResponse { items: statuses }).into_response(),
        Err(_) => runner_unavailable(),
    }
}

#[derive(Serialize)]
struct StatusResponse {
    items: Vec<ItemStatus>,
}

/// `GET /watch` — global file-watch state (inotify dirs + per-item patterns).
async fn get_watch(State(state): State<Arc<ApiState>>) -> Response {
    let (tx, rx) = oneshot::channel();
    if state
        .cmd_tx
        .send(RunnerCommand::WatchStatus { reply: tx })
        .is_err()
    {
        return runner_unavailable();
    }
    match rx.await {
        Ok(watch) => Json(WatchResponse { watch }).into_response(),
        Err(_) => runner_unavailable(),
    }
}

#[derive(Serialize)]
struct WatchResponse {
    /// `None` when no watches are active (serialized as JSON `null`).
    watch: Option<WatchReport>,
}

/// `POST /start/:name` — start a stopped service.
async fn post_start(State(state): State<Arc<ApiState>>, Path(name): Path<String>) -> Response {
    dispatch_control_cmd(state, &name, |name, reply| RunnerCommand::Start {
        name,
        reply,
    })
    .await
}

/// `POST /stop/:name` — stop a running service.
async fn post_stop(State(state): State<Arc<ApiState>>, Path(name): Path<String>) -> Response {
    dispatch_control_cmd(state, &name, |name, reply| RunnerCommand::Stop {
        name,
        reply,
    })
    .await
}

/// `POST /restart/:name` — restart a service.
async fn post_restart(State(state): State<Arc<ApiState>>, Path(name): Path<String>) -> Response {
    dispatch_control_cmd(state, &name, |name, reply| RunnerCommand::Restart {
        name,
        reply,
    })
    .await
}

/// `POST /hard-restart/:name` — rebuild then restart a service. Used by a
/// remote `don tui` frontend's `R` key.
async fn post_hard_restart(
    State(state): State<Arc<ApiState>>,
    Path(name): Path<String>,
) -> Response {
    dispatch_control_cmd(state, &name, |name, reply| RunnerCommand::HardRestart {
        name,
        reply,
    })
    .await
}

/// Query params for `POST /verbose`.
#[derive(Deserialize)]
struct VerboseQuery {
    enabled: bool,
}

/// `POST /verbose?enabled=true` — toggle the daemon's verbose output. Used by
/// a remote `don tui` frontend so the `v` key affects the daemon's formatting.
async fn post_verbose(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<VerboseQuery>,
) -> Response {
    if state
        .cmd_tx
        .send(RunnerCommand::SetVerbose {
            enabled: query.enabled,
        })
        .is_err()
    {
        return runner_unavailable();
    }
    StatusCode::NO_CONTENT.into_response()
}

/// `POST /shutdown` — gracefully stop the daemon.
async fn post_shutdown(State(state): State<Arc<ApiState>>) -> Response {
    if state.cmd_tx.send(RunnerCommand::Shutdown).is_err() {
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

    let (tx, rx) = oneshot::channel();
    if state
        .cmd_tx
        .send(RunnerCommand::Logs {
            name: name.clone(),
            last_n: query.last,
            reply: tx,
        })
        .is_err()
    {
        return runner_unavailable();
    }
    match rx.await {
        Ok(Some(raw)) => {
            let lines: Vec<String> = if raw.is_empty() {
                Vec::new()
            } else {
                raw.split('\n').map(String::from).collect()
            };
            Json(LogsResponse { name, lines }).into_response()
        }
        Ok(None) => not_found(&name),
        Err(_) => runner_unavailable(),
    }
}

async fn follow_logs(state: Arc<ApiState>, name: String, last: usize) -> Response {
    let (tx, rx) = oneshot::channel();
    if state
        .cmd_tx
        .send(RunnerCommand::LogsFollow {
            name: name.clone(),
            last_n: last,
            reply: tx,
        })
        .is_err()
    {
        return runner_unavailable();
    }
    let sink_rx = match rx.await {
        Ok(Some(r)) => r,
        Ok(None) => return not_found(&name),
        Err(_) => return runner_unavailable(),
    };

    // Build an NDJSON stream: one `{"line":"..."}\n` per SinkLine.
    use tokio_stream::{StreamExt, wrappers::ReceiverStream};
    let stream = ReceiverStream::new(sink_rx).map(|sink_line| {
        let line_str = String::from_utf8_lossy(&sink_line.line).into_owned();
        let json = serde_json::json!({ "line": line_str });
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
    let (tx, rx) = oneshot::channel();
    if state
        .cmd_tx
        .send(RunnerCommand::RunTask {
            name: name.clone(),
            params: body.params,
            wait,
            wait_timeout: body.wait_timeout,
            reply: tx,
        })
        .is_err()
    {
        return runner_unavailable();
    }
    let failed_status = if wait {
        StatusCode::UNPROCESSABLE_ENTITY
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    map_command_reply(&name, rx.await, failed_status).await
}

/// Shared command-reply-to-response mapping, shared between `post_run_task`
/// (which can't use `dispatch_control_cmd` because the command has a third
/// field beyond `name` + `reply`) and the other control endpoints.
async fn map_command_reply(
    name: &str,
    reply: Result<Result<(), CommandError>, oneshot::error::RecvError>,
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
    let (tx, rx) = oneshot::channel();
    if state
        .cmd_tx
        .send(RunnerCommand::ResolveCompletions {
            task,
            param,
            partial: body.partial,
            force_refresh: body.force_refresh,
            reply: tx,
        })
        .is_err()
    {
        return runner_unavailable();
    }
    match rx.await {
        Ok(Ok(values)) => Json(CompletionsResponse { values }).into_response(),
        Ok(Err(e)) => (StatusCode::BAD_GATEWAY, Json(completion_error_body(&e))).into_response(),
        Err(_) => runner_unavailable(),
    }
}

fn completion_error_body(e: &CompletionError) -> serde_json::Value {
    serde_json::json!({
        "error": e.message,
        "log_path": e.log_path,
    })
}

/// `POST /run-pending` — run all tasks in PendingRun state.
async fn post_run_pending(State(state): State<Arc<ApiState>>) -> Response {
    let (tx, rx) = oneshot::channel();
    if state
        .cmd_tx
        .send(RunnerCommand::RunPendingTasks { reply: tx })
        .is_err()
    {
        return runner_unavailable();
    }
    match rx.await {
        Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(error_body(&e.to_string())),
        )
            .into_response(),
        Err(_) => runner_unavailable(),
    }
}

// --- helpers ---

async fn dispatch_control_cmd<F>(state: Arc<ApiState>, name: &str, build: F) -> Response
where
    F: FnOnce(String, oneshot::Sender<Result<(), CommandError>>) -> RunnerCommand,
{
    let (tx, rx) = oneshot::channel();
    if state.cmd_tx.send(build(name.to_string(), tx)).is_err() {
        return runner_unavailable();
    }
    map_command_reply(name, rx.await, StatusCode::INTERNAL_SERVER_ERROR).await
}

pub(crate) fn runner_unavailable() -> Response {
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
