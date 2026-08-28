//! HTTP client for the daemon's unix-socket API.
//!
//! Speaks the same protocol as `server/routes.rs` — talks raw HTTP/1.1
//! over a `UnixStream` (no hyper on the client side). Each request opens
//! a fresh connection and sends `Connection: close`; the server fulfills
//! and closes. Follow-mode log streams use chunked transfer encoding.

pub mod attach;
pub mod attach_session;

use serde::Deserialize;

// Re-exported (and used here) so client-side consumers (the TUI) can name
// every type the API surface speaks without importing from `runner` — the
// types live there, but the *dependency* reads `crate::client`, which is
// the module edge the TUI separation enforces.
pub use crate::runner::{
    CompletionError, ProcessStatus, RunnerEvent, ServiceRuntime, ServiceState, StateSnapshot,
    TaskState,
};

/// One parsed record from `GET /events`.
#[derive(Debug, Clone)]
pub enum EventStreamItem {
    /// The stream's first record: full current state, so a connecting (or
    /// reconnecting) client starts consistent instead of
    /// stale-until-the-next-event.
    Snapshot {
        processes: Vec<ProcessStatus>,
        startup_complete: bool,
    },
    /// A runner event, exactly as broadcast.
    Event(RunnerEvent),
    /// This follower fell `n` events behind and they are unrecoverable —
    /// resync from `GET /status`.
    Lagged(u64),
}
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

/// Errors returned by the client.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// The daemon socket does not exist or refuses connections.
    #[error("don daemon not running — start it with `don start` (socket: {})", path.display())]
    NotRunning { path: PathBuf },
    /// A service/task name is not known to the daemon.
    #[error("{message}")]
    NotFound { message: String },
    /// Command rejected because it's misapplied (e.g. stop on a task).
    #[error("{message}")]
    BadRequest { message: String },
    /// Command rejected because the target is in the wrong state.
    #[error("{message}")]
    Conflict { message: String },
    /// A synchronous task run exceeded the requested wait deadline.
    #[error("{message}")]
    WaitTimeout { message: String },
    /// Command reached the daemon but the requested operation failed.
    #[error("{message}")]
    CommandFailed { message: String },
    /// Any other non-2xx response.
    #[error("server error (HTTP {status}): {message}")]
    Server { status: u16, message: String },
    /// I/O error talking to the socket.
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),
    /// Malformed HTTP response (protocol-level problem).
    #[error("invalid response: {0}")]
    Invalid(String),
    /// JSON (de)serialisation failure.
    #[error("invalid json: {0}")]
    Json(#[from] serde_json::Error),
    /// Server ran a completion command for us, and it failed. Carries the
    /// original [`CompletionError`] so the CLI can show the log path the
    /// server wrote.
    #[error("{0}")]
    Completion(CompletionError),
}

/// Deserialised body of `GET /status`.
#[derive(Debug, Deserialize)]
pub struct StatusResponse {
    pub processes: Vec<ProcessStatus>,
}

/// Deserialised body of `GET /watch`.
#[derive(Debug, Deserialize)]
pub struct WatchResponse {
    /// `None` when no watches are active.
    pub watch: Option<crate::runner::WatchReport>,
}

/// Deserialised body of `GET /logs/:name`.
#[derive(Debug, Deserialize)]
pub struct LogsResponse {
    pub lines: Vec<String>,
}

/// Options for running a task through the daemon API.
/// One record from the merged log stream (`GET /logs?follow=true`).
///
/// Untagged: a record is either a log line or a lag notice, distinguished
/// by shape. See the endpoint docs in `server/routes.rs` for field
/// semantics.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(untagged)]
pub enum LogStreamEvent {
    /// A formatted log line, ANSI colors included, no trailing newline.
    Line {
        /// This line's place in the merged stream — the same number every
        /// client sees for it. Resume from `id + 1` after a disconnect.
        id: crate::output::LogId,
        /// Owning process name; `[don]` lifecycle events carry their own
        /// sentinel name (see [`crate::output::LIFECYCLE_EVENT_NAME`]).
        name: String,
        /// True for `[don]`-prefixed lifecycle events.
        lifecycle: bool,
        /// True for verbose diagnostic messages — always present in the
        /// stream; the reader decides whether to display them.
        #[serde(default)]
        verbose: bool,
        /// The rendered name column — padded, coloured, with its separator,
        /// and the elapsed stamp when verbose. Empty from a runner that
        /// predates the split, in which case `line` carries it inline.
        #[serde(default)]
        prefix: String,
        /// The message, with whatever styling the process emitted. No prefix.
        line: String,
    },
    /// `dropped` lines are gone for good and the stream continues from
    /// `resumed_at`. Only sent when the server's own history could not cover
    /// the gap — an ordinary slow reader is caught up silently.
    Dropped {
        dropped: u64,
        resumed_at: crate::output::LogId,
    },
}

#[derive(Debug, Clone, Default)]
pub struct RunTaskOptions {
    /// Wait until the task process exits before returning.
    pub wait: bool,
    /// Maximum time the daemon should wait for task completion.
    pub wait_timeout: Option<String>,
}

/// Deserialised body of `GET /ready`.
#[derive(Debug, Deserialize)]
pub struct ReadyInfo {
    /// Whether the initial startup sweep has decided every process.
    pub startup_complete: bool,
    /// The runner's crate version. `None` from a runner that predates the
    /// field.
    #[serde(default)]
    pub version: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    error: String,
}

/// Client for the don daemon unix-socket API.
pub struct Client {
    socket_path: PathBuf,
}

impl Client {
    /// Create a client pointed at `<base>/.don/don.sock`.
    pub fn new(base_dir: &Path) -> Self {
        Self {
            socket_path: base_dir.join(".don").join("don.sock"),
        }
    }

    /// The unix socket path this client talks to.
    pub fn socket_path(&self) -> &std::path::Path {
        &self.socket_path
    }

    /// Directly wrap an existing socket path (tests, non-standard layouts).
    pub fn with_socket_path(socket_path: PathBuf) -> Self {
        Self { socket_path }
    }

    /// `GET /status`
    ///
    /// When `name` is `Some`, the response is restricted to that single
    /// service/task and includes its full resolved watch path list (the
    /// all-processes view omits the path list — see the server's `collect_status`).
    /// A name that matches nothing yields an empty list, not an error.
    pub async fn status(
        &self,
        verbose: bool,
        name: Option<&str>,
    ) -> Result<Vec<ProcessStatus>, ClientError> {
        let mut path = String::from("/status");
        let mut sep = '?';
        if verbose {
            path.push_str("?verbose=true");
            sep = '&';
        }
        if let Some(name) = name {
            path.push(sep);
            path.push_str("name=");
            path.push_str(&urlencode(name));
        }
        let (status, body) = self.request("GET", &path, false).await?;
        ensure_ok(status, &body)?;
        let parsed: StatusResponse = serde_json::from_slice(&body)?;
        Ok(parsed.processes)
    }

    /// `GET /watch` — global file-watch state (inotify dirs + per-process
    /// patterns). `Ok(None)` means no watches are active.
    pub async fn watch(&self) -> Result<Option<crate::runner::WatchReport>, ClientError> {
        let (status, body) = self.request("GET", "/watch", false).await?;
        ensure_ok(status, &body)?;
        let parsed: WatchResponse = serde_json::from_slice(&body)?;
        Ok(parsed.watch)
    }

    /// `GET /ready` — whether the runner has finished its initial sweep.
    ///
    /// The API socket is bound before the runner starts, so its presence says
    /// nothing about readiness. This is that signal: wait for it before
    /// issuing a control action whose meaning depends on startup being over.
    pub async fn ready(&self) -> Result<bool, ClientError> {
        Ok(self.ready_info().await?.startup_complete)
    }

    /// `GET /ready`, full body — readiness plus the runner's version, for
    /// clients that want to warn about skew against a long-lived runner.
    pub async fn ready_info(&self) -> Result<ReadyInfo, ClientError> {
        let (status, body) = self.request("GET", "/ready", false).await?;
        ensure_ok(status, &body)?;
        Ok(serde_json::from_slice(&body)?)
    }

    /// `POST /start/:name`
    pub async fn start(&self, name: &str) -> Result<(), ClientError> {
        self.control("/start/", name).await
    }

    /// `POST /stop/:name`
    ///
    /// Names a service or a task: a task's run is a process too, and this is
    /// what ends it.
    pub async fn stop(&self, name: &str) -> Result<(), ClientError> {
        self.control("/stop/", name).await
    }

    /// `POST /restart/:name`
    pub async fn restart(&self, name: &str) -> Result<(), ClientError> {
        self.control("/restart/", name).await
    }

    /// `POST /hard-restart/:name` — kill and restart, skipping graceful stop.
    pub async fn hard_restart(&self, name: &str) -> Result<(), ClientError> {
        self.control("/hard-restart/", name).await
    }

    /// `POST /shutdown?force=true` — escalate like a second Ctrl+C:
    /// in-flight graceful stops SIGKILL their process groups.
    pub async fn shutdown_force(&self) -> Result<(), ClientError> {
        let (status, body) = self.request("POST", "/shutdown?force=true", false).await?;
        if status == 204 {
            return Ok(());
        }
        ensure_ok(status, &body)?;
        Ok(())
    }

    /// `POST /shutdown` — gracefully stop the daemon and all running services.
    pub async fn shutdown(&self) -> Result<(), ClientError> {
        let (status, body) = self.request("POST", "/shutdown", false).await?;
        if status == 204 {
            return Ok(());
        }
        Err(classify_error(status, &body))
    }

    /// `POST /run/:name` — run a specific task, bypassing auto_run.
    ///
    /// `params` is the user-supplied param map (empty for tasks without
    /// declared params). Sent as `{"params": {...}}` JSON body.
    pub async fn run_task(
        &self,
        name: &str,
        params: HashMap<String, String>,
    ) -> Result<(), ClientError> {
        self.run_task_with_options(name, params, RunTaskOptions::default())
            .await
    }

    /// `POST /run/:name` with options such as synchronous completion waiting.
    pub async fn run_task_with_options(
        &self,
        name: &str,
        params: HashMap<String, String>,
        options: RunTaskOptions,
    ) -> Result<(), ClientError> {
        let path = format!("/run/{}", urlencode(name));
        let body = serde_json::to_vec(&serde_json::json!({
            "params": params,
            "wait": options.wait,
            "wait_timeout": options.wait_timeout,
        }))?;
        let (status, body) = self
            .request_with_body("POST", &path, Some(body.as_slice()))
            .await?;
        if status == 204 {
            return Ok(());
        }
        Err(classify_error(status, &body))
    }

    /// `POST /completions/:task/:param` — resolve candidate values for one
    /// task param. `partial` carries already-entered values for other params
    /// (exposed to the completion command as `DON_PARAM_<NAME>`).
    pub async fn resolve_completions(
        &self,
        task: &str,
        param: &str,
        partial: HashMap<String, String>,
        force_refresh: bool,
    ) -> Result<Vec<String>, ClientError> {
        #[derive(Deserialize)]
        struct Resp {
            values: Vec<String>,
        }
        #[derive(Deserialize)]
        struct ErrResp {
            error: String,
            #[serde(default)]
            log_path: Option<PathBuf>,
        }

        let path = format!("/completions/{}/{}", urlencode(task), urlencode(param));
        let body = serde_json::to_vec(&serde_json::json!({
            "partial": partial,
            "force_refresh": force_refresh,
        }))?;
        let (status, body) = self
            .request_with_body("POST", &path, Some(body.as_slice()))
            .await?;
        if (200..300).contains(&status) {
            let parsed: Resp = serde_json::from_slice(&body)?;
            return Ok(parsed.values);
        }
        // A 502 from the server carries {error, log_path} — surface it as
        // a structured CompletionError via the Completions variant.
        if let Ok(parsed) = serde_json::from_slice::<ErrResp>(&body) {
            return Err(ClientError::Completion(CompletionError {
                message: parsed.error,
                log_path: parsed.log_path,
            }));
        }
        Err(classify_error(status, &body))
    }

    /// `GET /logs/:name?last=N`
    pub async fn logs(&self, name: &str, last: usize) -> Result<Vec<String>, ClientError> {
        let path = format!("/logs/{}?last={last}", urlencode(name));
        let (status, body) = self.request("GET", &path, false).await?;
        ensure_ok(status, &body)?;
        let parsed: LogsResponse = serde_json::from_slice(&body)?;
        Ok(parsed.lines)
    }

    /// `GET /logs/:name?last=N&follow=true` — opens a streaming connection
    /// and invokes `on_line` for each NDJSON line the server sends. Returns
    /// when the server closes the stream or the callback returns `Err`.
    pub async fn logs_follow<F>(
        &self,
        name: &str,
        last: usize,
        on_line: F,
    ) -> Result<(), ClientError>
    where
        F: FnMut(&str) -> Result<(), ClientError>,
    {
        let path = format!("/logs/{}?last={last}&follow=true", urlencode(name));
        self.follow_ndjson(&path, on_line).await
    }

    /// `GET /logs?follow=true` — stream the merged log stream: every process
    /// plus `[don]` lifecycle events, in arrival order, with the metadata
    /// a renderer needs to filter and color. Returns when the server closes
    /// the stream or the callback returns `Err`.
    /// `since` resumes exactly where a previous session stopped; `None` asks
    /// for everything the server still holds.
    ///
    /// That `last` is the tap's whole capacity rather than the route's default
    /// is the point. The route answers ad-hoc clients too — a `curl` wants the
    /// end of the log, not this morning — but a TUI attaching to a running
    /// project is asking for the scrollback, and the one sharing a process
    /// with the runner already preloads exactly this much. Anything less and
    /// reattaching from a second terminal silently shows a fraction of what
    /// the first one has, which reads as the log having been lost.
    pub async fn logs_follow_all<F>(
        &self,
        since: Option<crate::output::LogId>,
        mut on_event: F,
    ) -> Result<(), ClientError>
    where
        F: FnMut(LogStreamEvent) -> Result<(), ClientError>,
    {
        let path = match since {
            // `last` is ignored when resuming: the id says exactly what was
            // missed, which is both cheaper and more accurate than a count.
            Some(since) => format!("/logs?follow=true&since={since}"),
            None => format!(
                "/logs?follow=true&last={}",
                crate::output::DEFAULT_MERGED_HISTORY_CAPACITY
            ),
        };
        self.follow_ndjson(&path, |line| {
            let event: LogStreamEvent = serde_json::from_str(line)?;
            on_event(event)
        })
        .await
    }

    /// `GET /events`, parsed. Each record is either a [`RunnerEvent`] or a
    /// lag notice; records that parse as neither (a serialization-error
    /// notice, an event variant this binary predates) are skipped, which is
    /// what makes version skew between a long-lived runner and a newer
    /// client degrade to "some events invisible" instead of a hard error.
    pub async fn events_follow_typed<F>(&self, mut on_event: F) -> Result<(), ClientError>
    where
        F: FnMut(EventStreamItem) -> Result<(), ClientError>,
    {
        self.follow_ndjson("/events", |line| {
            let value: serde_json::Value = match serde_json::from_str(line) {
                Ok(value) => value,
                Err(_) => return Ok(()),
            };
            let process = match value.get("type").and_then(|t| t.as_str()) {
                Some("lagged") => EventStreamItem::Lagged(
                    value.get("skipped").and_then(|n| n.as_u64()).unwrap_or(0),
                ),
                Some("snapshot") => {
                    #[derive(Deserialize)]
                    struct Snapshot {
                        processes: Vec<ProcessStatus>,
                        #[serde(default)]
                        startup_complete: bool,
                    }
                    match serde_json::from_value::<Snapshot>(value) {
                        Ok(snapshot) => EventStreamItem::Snapshot {
                            processes: snapshot.processes,
                            startup_complete: snapshot.startup_complete,
                        },
                        Err(_) => return Ok(()),
                    }
                }
                _ => match serde_json::from_value::<RunnerEvent>(value) {
                    Ok(event) => EventStreamItem::Event(event),
                    Err(_) => return Ok(()),
                },
            };
            on_event(process)
        })
        .await
    }

    /// `GET /events` — stream runner state changes, one JSON object per line.
    ///
    /// Same shape as [`Self::logs_follow`], and the same lifetime: the call
    /// returns when the daemon closes the stream (normally at shutdown) or
    /// when `on_line` asks to stop by returning `Err`.
    pub async fn events_follow<F>(&self, on_line: F) -> Result<(), ClientError>
    where
        F: FnMut(&str) -> Result<(), ClientError>,
    {
        self.follow_ndjson("/events", on_line).await
    }

    /// Read a newline-delimited JSON stream, invoking `on_line` per line.
    ///
    /// Handles both chunked and read-until-close bodies, since the API uses
    /// whichever hyper picks for a given response.
    async fn follow_ndjson<F>(&self, path: &str, mut on_line: F) -> Result<(), ClientError>
    where
        F: FnMut(&str) -> Result<(), ClientError>,
    {
        let mut stream = self.connect().await?;
        write_request(&mut stream, "GET", path, false).await?;
        // Parse status line + headers.
        let (status, headers, mut leftover) = read_head(&mut stream).await?;
        if status != 200 {
            // Drain body to read the error payload.
            let body = drain_body(&mut stream, &headers, leftover).await?;
            return Err(classify_error(status, &body));
        }
        let chunked = headers
            .iter()
            .any(|(k, v)| k.eq_ignore_ascii_case("transfer-encoding") && v.contains("chunked"));

        // Buffer raw body bytes, decode chunks if needed, split on \n.
        let mut pending = Vec::<u8>::new();
        loop {
            let data: Vec<u8> = if chunked {
                match read_one_chunk(&mut stream, &mut leftover).await? {
                    Some(bytes) => bytes,
                    None => break, // terminator chunk
                }
            } else {
                // Plain body: read directly until EOF.
                let mut buf = [0u8; 4096];
                let n = stream.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                buf[..n].to_vec()
            };
            pending.extend_from_slice(&data);
            while let Some(nl) = pending.iter().position(|b| *b == b'\n') {
                let line_bytes: Vec<u8> = pending.drain(..=nl).collect();
                let line_slice = &line_bytes[..line_bytes.len() - 1]; // drop \n
                if line_slice.is_empty() {
                    continue;
                }
                let text = String::from_utf8_lossy(line_slice);
                on_line(&text)?;
            }
        }
        Ok(())
    }

    // --- internals ---

    async fn control(&self, prefix: &str, name: &str) -> Result<(), ClientError> {
        let path = format!("{prefix}{}", urlencode(name));
        let (status, body) = self.request("POST", &path, false).await?;
        if status == 204 {
            return Ok(());
        }
        Err(classify_error(status, &body))
    }

    async fn connect(&self) -> Result<UnixStream, ClientError> {
        connect_unix(&self.socket_path).await
    }

    async fn request(
        &self,
        method: &str,
        path: &str,
        has_body: bool,
    ) -> Result<(u16, Vec<u8>), ClientError> {
        let mut stream = self.connect().await?;
        write_request(&mut stream, method, path, has_body).await?;
        let (status, headers, leftover) = read_head(&mut stream).await?;
        let body = drain_body(&mut stream, &headers, leftover).await?;
        Ok((status, body))
    }

    /// Like [`Self::request`] but with an optional JSON request body.
    async fn request_with_body(
        &self,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
    ) -> Result<(u16, Vec<u8>), ClientError> {
        unix_request(&self.socket_path, method, path, body).await
    }
}

/// Connect to a unix socket, mapping "nothing is listening" to the friendlier
/// [`ClientError::NotRunning`] rather than a bare io error.
pub(crate) async fn connect_unix(socket_path: &Path) -> Result<UnixStream, ClientError> {
    match UnixStream::connect(socket_path).await {
        Ok(s) => Ok(s),
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
            ) =>
        {
            Err(ClientError::NotRunning {
                path: socket_path.to_path_buf(),
            })
        }
        Err(e) => Err(ClientError::Io(e)),
    }
}

/// One-shot HTTP request over a unix socket, returning `(status, body)`.
///
/// Shared by the project API client above and the daemon control client
/// (`crate::daemon::client`) — both speak plain HTTP/1.1 with
/// `Connection: close` over a `UnixStream`, so neither needs its own
/// request/response plumbing.
pub(crate) async fn unix_request(
    socket_path: &Path,
    method: &str,
    path: &str,
    body: Option<&[u8]>,
) -> Result<(u16, Vec<u8>), ClientError> {
    let mut stream = connect_unix(socket_path).await?;
    write_request_with_body(&mut stream, method, path, body).await?;
    let (status, headers, leftover) = read_head(&mut stream).await?;
    let response = drain_body(&mut stream, &headers, leftover).await?;
    Ok((status, response))
}

fn ensure_ok(status: u16, body: &[u8]) -> Result<(), ClientError> {
    if (200..300).contains(&status) {
        Ok(())
    } else {
        Err(classify_error(status, body))
    }
}

pub(crate) fn classify_error(status: u16, body: &[u8]) -> ClientError {
    let message = extract_error_message(body)
        .unwrap_or_else(|| String::from_utf8_lossy(body).trim().to_string());
    match status {
        404 => ClientError::NotFound { message },
        400 => ClientError::BadRequest { message },
        408 => ClientError::WaitTimeout { message },
        409 => ClientError::Conflict { message },
        422 => ClientError::CommandFailed { message },
        other => ClientError::Server {
            status: other,
            message,
        },
    }
}

fn extract_error_message(body: &[u8]) -> Option<String> {
    let parsed: ErrorBody = serde_json::from_slice(body).ok()?;
    Some(parsed.error)
}

/// Write a POST with a JSON request body. Emits `Content-Type: application/json`
/// and `Content-Length: <len>`, then writes the body bytes before returning.
pub(crate) async fn write_request_with_body(
    stream: &mut UnixStream,
    method: &str,
    path: &str,
    body: Option<&[u8]>,
) -> Result<(), ClientError> {
    let body_bytes = body.unwrap_or(&[]);
    let req = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n",
        body_bytes.len(),
    );
    stream.write_all(req.as_bytes()).await?;
    if !body_bytes.is_empty() {
        stream.write_all(body_bytes).await?;
    }
    Ok(())
}

pub(crate) async fn write_request(
    stream: &mut UnixStream,
    method: &str,
    path: &str,
    has_body: bool,
) -> Result<(), ClientError> {
    // Content-Length: 0 makes POSTs unambiguous for servers that want it.
    let cl = if has_body {
        ""
    } else {
        "Content-Length: 0\r\n"
    };
    let req = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         {cl}Connection: close\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await?;
    Ok(())
}

/// Read the status line and headers from a fresh response. Returns the
/// status code, header pairs, and any leftover bytes that belong to the body.
pub(crate) async fn read_head(
    stream: &mut UnixStream,
) -> Result<(u16, Vec<(String, String)>, Vec<u8>), ClientError> {
    let mut buf = Vec::<u8>::new();
    let mut scratch = [0u8; 1024];
    loop {
        let n = stream.read(&mut scratch).await?;
        if n == 0 {
            return Err(ClientError::Invalid(
                "connection closed before headers".into(),
            ));
        }
        buf.extend_from_slice(&scratch[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        // Guard against absurd header sizes.
        if buf.len() > 64 * 1024 {
            return Err(ClientError::Invalid("headers too large".into()));
        }
    }
    let header_end = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| ClientError::Invalid("no header terminator".into()))?;
    let head_bytes = &buf[..header_end];
    let leftover = buf[header_end + 4..].to_vec();
    let head_text = std::str::from_utf8(head_bytes)
        .map_err(|_| ClientError::Invalid("non-utf8 headers".into()))?;
    let mut lines = head_text.split("\r\n");
    let status_line = lines
        .next()
        .ok_or_else(|| ClientError::Invalid("missing status line".into()))?;
    let status = parse_status(status_line)?;
    let mut headers = Vec::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }
    Ok((status, headers, leftover))
}

fn parse_status(line: &str) -> Result<u16, ClientError> {
    let mut parts = line.split(' ');
    let _version = parts.next();
    let code = parts
        .next()
        .ok_or_else(|| ClientError::Invalid(format!("bad status line: {line}")))?;
    code.parse::<u16>()
        .map_err(|_| ClientError::Invalid(format!("bad status code: {code}")))
}

/// Read the entire response body. Supports Content-Length, chunked, or
/// read-until-close (our server sets `Connection: close` on non-stream
/// responses).
pub(crate) async fn drain_body(
    stream: &mut UnixStream,
    headers: &[(String, String)],
    mut leftover: Vec<u8>,
) -> Result<Vec<u8>, ClientError> {
    let content_length = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.parse::<usize>().ok());
    let chunked = headers.iter().any(|(k, v)| {
        k.eq_ignore_ascii_case("transfer-encoding") && v.to_ascii_lowercase().contains("chunked")
    });

    if chunked {
        let mut out = Vec::new();
        while let Some(chunk) = read_one_chunk(stream, &mut leftover).await? {
            out.extend_from_slice(&chunk);
        }
        return Ok(out);
    }

    if let Some(len) = content_length {
        while leftover.len() < len {
            let mut scratch = [0u8; 4096];
            let n = stream.read(&mut scratch).await?;
            if n == 0 {
                break;
            }
            leftover.extend_from_slice(&scratch[..n]);
        }
        leftover.truncate(len);
        return Ok(leftover);
    }

    // Read-until-EOF.
    let mut out = leftover;
    let mut scratch = [0u8; 4096];
    loop {
        let n = stream.read(&mut scratch).await?;
        if n == 0 {
            break;
        }
        out.extend_from_slice(&scratch[..n]);
    }
    Ok(out)
}

/// Read one chunk from a chunked-encoded body, consuming from `leftover` first
/// and then the stream as needed. Returns `Ok(None)` on the terminator chunk.
async fn read_one_chunk(
    stream: &mut UnixStream,
    leftover: &mut Vec<u8>,
) -> Result<Option<Vec<u8>>, ClientError> {
    // Read the size line.
    let size_line = read_line(stream, leftover).await?;
    let size_str = size_line.trim();
    // Ignore optional chunk extensions (";...").
    let size_str = size_str.split(';').next().unwrap_or("").trim();
    let size = usize::from_str_radix(size_str, 16)
        .map_err(|_| ClientError::Invalid(format!("bad chunk size: {size_str:?}")))?;
    if size == 0 {
        // Consume the trailing CRLF after the zero chunk.
        let _ = read_line(stream, leftover).await;
        return Ok(None);
    }
    // Read `size` bytes + trailing CRLF.
    let mut data = Vec::with_capacity(size);
    while data.len() < size {
        if !leftover.is_empty() {
            let need = size - data.len();
            let take = need.min(leftover.len());
            data.extend(leftover.drain(..take));
        } else {
            let mut scratch = [0u8; 4096];
            let n = stream.read(&mut scratch).await?;
            if n == 0 {
                return Err(ClientError::Invalid("unexpected EOF in chunk body".into()));
            }
            leftover.extend_from_slice(&scratch[..n]);
        }
    }
    // Consume trailing CRLF.
    while leftover.len() < 2 {
        let mut scratch = [0u8; 16];
        let n = stream.read(&mut scratch).await?;
        if n == 0 {
            break;
        }
        leftover.extend_from_slice(&scratch[..n]);
    }
    if leftover.len() >= 2 {
        leftover.drain(..2);
    }
    Ok(Some(data))
}

/// Read a CRLF-terminated line from `leftover` + `stream`.
async fn read_line(stream: &mut UnixStream, leftover: &mut Vec<u8>) -> Result<String, ClientError> {
    loop {
        if let Some(pos) = leftover.windows(2).position(|w| w == b"\r\n") {
            let line_bytes: Vec<u8> = leftover.drain(..pos).collect();
            leftover.drain(..2); // consume CRLF
            return String::from_utf8(line_bytes)
                .map_err(|_| ClientError::Invalid("non-utf8 chunk line".into()));
        }
        let mut scratch = [0u8; 256];
        let n = stream.read(&mut scratch).await?;
        if n == 0 {
            return Err(ClientError::Invalid("unexpected EOF reading line".into()));
        }
        leftover.extend_from_slice(&scratch[..n]);
    }
}

/// Minimal percent-encoder for the path segment (names may contain `/`, `%`, `?`).
pub(crate) fn urlencode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.as_bytes() {
        let b = *byte;
        let ok = b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~');
        if ok {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn urlencode_handles_special_chars() {
        assert_eq!(urlencode("api"), "api");
        assert_eq!(urlencode("a/b"), "a%2Fb");
        assert_eq!(urlencode("ab c"), "ab%20c");
        assert_eq!(urlencode("foo-bar_baz.v1"), "foo-bar_baz.v1");
    }

    #[test]
    fn parse_status_extracts_code() {
        assert_eq!(parse_status("HTTP/1.1 200 OK").unwrap(), 200);
        assert_eq!(parse_status("HTTP/1.1 404 Not Found").unwrap(), 404);
        assert_eq!(parse_status("HTTP/1.1 500 ").unwrap(), 500);
    }

    #[test]
    fn classify_error_maps_statuses() {
        let err = classify_error(404, br#"{"error":"no such name 'ghost'"}"#);
        assert!(matches!(err, ClientError::NotFound { .. }));
        let err = classify_error(400, br#"{"error":"not a service"}"#);
        assert!(matches!(err, ClientError::BadRequest { .. }));
        let err = classify_error(409, br#"{"error":"already running"}"#);
        assert!(matches!(err, ClientError::Conflict { .. }));
        let err = classify_error(408, br#"{"error":"task did not finish"}"#);
        assert!(matches!(err, ClientError::WaitTimeout { .. }));
        let err = classify_error(422, br#"{"error":"task failed"}"#);
        assert!(matches!(err, ClientError::CommandFailed { .. }));
        let err = classify_error(500, br#"{"error":"boom"}"#);
        assert!(matches!(err, ClientError::Server { status: 500, .. }));
    }

    #[test]
    fn extract_error_message_handles_missing_field() {
        assert_eq!(
            extract_error_message(br#"{"error":"oops"}"#),
            Some("oops".to_string())
        );
        assert_eq!(extract_error_message(b"not json"), None);
    }
}
