//! Streaming + snapshot endpoints that back the remote `don tui` frontend.
//!
//! - `GET /snapshot` — one-shot seed of the active item set + current state.
//! - `GET /events`   — NDJSON stream of [`RunnerEvent`] (low volume).
//! - `GET /logstream`— binary-framed stream of [`FormattedLogLine`] (hot path;
//!   see [`crate::wire`] for the frame format).
//!
//! Both streams bridge a broadcast receiver into a bounded mpsc whose
//! [`ReceiverStream`] becomes the HTTP body. When the client disconnects the
//! body is dropped, the mpsc receiver is dropped, and the bridge task exits on
//! its next send — so a vanished frontend never leaks a task or stalls the
//! daemon.

use super::ApiState;
use crate::runner::RunnerCommand;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;

/// Bounded buffer between the broadcast receiver and the HTTP body. Caps how
/// far a slow socket write can run ahead before the bridge task parks.
const STREAM_BUFFER: usize = 1024;

/// `GET /snapshot` — seed data for a freshly attached frontend.
pub(crate) async fn get_snapshot(State(state): State<Arc<ApiState>>) -> Response {
    let (tx, rx) = oneshot::channel();
    if state
        .cmd_tx
        .send(RunnerCommand::Snapshot { reply: tx })
        .is_err()
    {
        return super::routes::runner_unavailable();
    }
    match rx.await {
        Ok(snapshot) => axum::Json(snapshot).into_response(),
        Err(_) => super::routes::runner_unavailable(),
    }
}

/// `GET /events` — NDJSON stream of runner events.
pub(crate) async fn get_events(State(state): State<Arc<ApiState>>) -> Response {
    let mut events = state.event_tx.subscribe();
    let (tx, rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(STREAM_BUFFER);

    tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(event) => {
                    let Ok(mut json) = serde_json::to_vec(&event) else {
                        continue;
                    };
                    json.push(b'\n');
                    if tx.send(bytes::Bytes::from(json)).await.is_err() {
                        break; // client gone
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    body_stream_response(rx, "application/x-ndjson")
}

/// `GET /logstream` — binary-framed stream of every formatted log line.
///
/// Sends the daemon's recent-history snapshot first (so a frontend connecting
/// to a long-running daemon sees what just happened, not an empty pane), then
/// live frames. The snapshot and the live subscription are taken atomically
/// inside [`crate::output::LogTaps`], so every line is delivered exactly once
/// — no dups across the boundary, no gaps either.
pub(crate) async fn get_logstream(State(state): State<Arc<ApiState>>) -> Response {
    let (snapshot, mut logs) = state.log_taps.snapshot_and_subscribe();
    let (tx, rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(STREAM_BUFFER);

    tokio::spawn(async move {
        // Backfill — recent ring first so the connecting frontend sees what
        // just happened, not an empty pane.
        for line in snapshot {
            let frame = crate::wire::encode_log_frame(&line);
            if tx.send(bytes::Bytes::from(frame)).await.is_err() {
                return;
            }
        }
        // Then live broadcast. Atomic snapshot+subscribe inside `log_taps`
        // means every live line is delivered exactly once across the
        // boundary — no dups, no gaps.
        loop {
            match logs.recv().await {
                Ok(line) => {
                    let frame = crate::wire::encode_log_frame(&line);
                    if tx.send(bytes::Bytes::from(frame)).await.is_err() {
                        break;
                    }
                }
                // A lagging viewer drops the oldest lines (broadcast semantics).
                // Keep streaming the lines we can still see rather than tearing
                // the connection down.
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    body_stream_response(rx, "application/octet-stream")
}

/// Wrap a `bytes::Bytes` receiver as a chunked HTTP body with `content_type`.
fn body_stream_response(rx: tokio::sync::mpsc::Receiver<bytes::Bytes>, content_type: &str) -> Response {
    use tokio_stream::StreamExt;
    let stream = ReceiverStream::new(rx).map(Ok::<_, std::convert::Infallible>);
    match Response::builder()
        .status(StatusCode::OK)
        .header("content-type", content_type)
        .body(axum::body::Body::from_stream(stream))
    {
        Ok(resp) => resp,
        Err(_) => super::routes::runner_unavailable(),
    }
}
