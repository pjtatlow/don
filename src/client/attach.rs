//! Client-side interactive attach — connects to the daemon's HTTP API,
//! upgrades to a raw byte stream, and bridges stdin/stdout directly.
//!
//! Ctrl+C or Ctrl+D detaches without killing the service. A `Drop` guard
//! ensures raw mode is always restored. If the server disconnects (e.g.
//! task rerun), the client automatically reconnects.

use super::ClientError;
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

/// RAII guard that restores the terminal from raw mode on drop.
struct RawModeGuard;

impl RawModeGuard {
    fn enable() -> Result<Self, ClientError> {
        crossterm::terminal::enable_raw_mode()
            .map_err(|e| ClientError::Io(std::io::Error::other(format!("enable raw mode: {e}"))))?;
        Ok(Self)
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        let _ = crossterm::terminal::disable_raw_mode();
    }
}

/// Why the bridge loop exited.
enum DisconnectReason {
    /// User pressed Ctrl+C or Ctrl+D.
    UserDetach,
    /// Server closed the connection (rerun, restart, shutdown).
    ServerDisconnect,
    /// An error occurred.
    Error(ClientError),
}

/// Connect to the daemon and run an interactive attach session.
///
/// Bridges stdin↔raw stream↔PTY and PTY↔raw stream↔stdout.
/// Auto-reconnects when the server disconnects (e.g. task rerun or
/// service restart). Returns when the user detaches with Ctrl+C/Ctrl+D.
pub async fn run_attach(socket_path: &Path, name: &str) -> Result<(), ClientError> {
    loop {
        match attach_once(socket_path, name).await {
            DisconnectReason::UserDetach => return Ok(()),
            DisconnectReason::Error(e) => return Err(e),
            DisconnectReason::ServerDisconnect => {
                // Write a notice — the next attach_once will block on the
                // server side until the process starts again.
                let mut stdout = tokio::io::stdout();
                let _ = stdout.write_all(b"\r\n[waiting for process...]\r\n").await;
                let _ = stdout.flush().await;
            }
        }
    }
}

/// Attach for a single session: returns when the server closes the stream
/// (the process exited) or the user detaches. Used by `don run` to bridge a
/// foreground task on a headless daemon — unlike [`run_attach`], a task
/// exiting ends the session instead of waiting for the next spawn.
pub async fn run_attach_session(socket_path: &Path, name: &str) -> Result<(), ClientError> {
    match attach_once(socket_path, name).await {
        DisconnectReason::UserDetach | DisconnectReason::ServerDisconnect => Ok(()),
        DisconnectReason::Error(e) => Err(e),
    }
}

/// Run a single attach session. Returns the reason for disconnection.
async fn attach_once(socket_path: &Path, name: &str) -> DisconnectReason {
    let mut stream = match UnixStream::connect(socket_path).await {
        Ok(s) => s,
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            ) =>
        {
            return DisconnectReason::Error(ClientError::NotRunning {
                path: socket_path.to_path_buf(),
            });
        }
        Err(e) => return DisconnectReason::Error(ClientError::Io(e)),
    };

    // Build HTTP upgrade request with pid, cols, rows as query params.
    let pid = std::process::id();
    let (cols, rows) = crossterm::terminal::size().unwrap_or((80, 24));
    let path = format!(
        "/attach/{}?pid={pid}&cols={cols}&rows={rows}",
        super::urlencode(name),
    );
    let req = format!(
        "GET {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Connection: Upgrade\r\n\
         Upgrade: don-attach\r\n\
         \r\n"
    );
    if let Err(e) = stream.write_all(req.as_bytes()).await {
        return DisconnectReason::Error(ClientError::Io(e));
    }

    // Read HTTP response.
    let (status, headers, leftover) = match super::read_head(&mut stream).await {
        Ok(r) => r,
        Err(e) => return DisconnectReason::Error(e),
    };

    if status != 101 {
        // Not an upgrade — read error body.
        let body = match super::drain_body(&mut stream, &headers, leftover).await {
            Ok(b) => b,
            Err(e) => return DisconnectReason::Error(e),
        };
        return DisconnectReason::Error(super::classify_error(status, &body));
    }

    // 101 — the stream is now raw. Enter raw mode.
    let _guard = match RawModeGuard::enable() {
        Ok(g) => g,
        Err(e) => return DisconnectReason::Error(e),
    };

    // Write any leftover bytes (from header read) to stdout — this is
    // the start of the output stream.
    if !leftover.is_empty() {
        let mut stdout = tokio::io::stdout();
        let _ = stdout.write_all(&leftover).await;
        let _ = stdout.flush().await;
    }

    // Bridge stdin/stdout with the raw stream.
    // Stream closes when we drop it.
    bridge_terminal(&mut stream, socket_path, name, pid).await
}

/// Check data for detach triggers: Ctrl+C (\x03) or Ctrl+D (\x04).
fn should_detach(data: &[u8]) -> bool {
    data.iter().any(|b| matches!(b, 0x03 | 0x04))
}

/// Bridge stdin↔raw stream↔stdout. Returns the reason for exiting.
async fn bridge_terminal(
    stream: &mut UnixStream,
    socket_path: &Path,
    name: &str,
    pid: u32,
) -> DisconnectReason {
    let (mut stream_read, mut stream_write) = stream.split();
    let mut stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();

    // Resize event channel.
    let (resize_tx, mut resize_rx) = tokio::sync::mpsc::channel::<(u16, u16)>(4);

    // Spawn a task to watch for terminal resize events.
    let resize_handle = tokio::spawn({
        use futures_util::StreamExt;
        async move {
            let mut reader = crossterm::event::EventStream::new();
            while let Some(Ok(event)) = reader.next().await {
                if let crossterm::event::Event::Resize(cols, rows) = event
                    && resize_tx.send((cols, rows)).await.is_err()
                {
                    break;
                }
            }
        }
    });

    let socket_path = socket_path.to_path_buf();
    let name = name.to_string();

    let mut stdin_buf = [0u8; 4096];
    let mut stream_buf = [0u8; 8192];
    let reason = loop {
        tokio::select! {
            // stdin → server (raw bytes, zero framing)
            read_result = stdin.read(&mut stdin_buf) => {
                match read_result {
                    Ok(0) => break DisconnectReason::UserDetach,
                    Ok(n) => {
                        let data = &stdin_buf[..n];
                        if should_detach(data) {
                            break DisconnectReason::UserDetach;
                        }
                        if stream_write.write_all(data).await.is_err() {
                            break DisconnectReason::ServerDisconnect;
                        }
                    }
                    Err(e) => break DisconnectReason::Error(ClientError::Io(e)),
                }
            }
            // server → stdout (raw bytes, zero framing)
            read_result = stream_read.read(&mut stream_buf) => {
                match read_result {
                    Ok(0) => break DisconnectReason::ServerDisconnect,
                    Ok(n) => {
                        if stdout.write_all(&stream_buf[..n]).await.is_err() {
                            break DisconnectReason::UserDetach;
                        }
                        let _ = stdout.flush().await;
                    }
                    Err(_) => break DisconnectReason::ServerDisconnect,
                }
            }
            // Terminal resize → separate HTTP request
            Some((cols, rows)) = resize_rx.recv() => {
                let sp = socket_path.clone();
                let n = name.clone();
                tokio::spawn(async move {
                    let _ = send_resize(&sp, &n, pid, cols, rows).await;
                });
            }
        }
    };

    resize_handle.abort();
    reason
}

/// Send a resize request via a separate HTTP connection.
async fn send_resize(
    socket_path: &Path,
    name: &str,
    _pid: u32,
    cols: u16,
    rows: u16,
) -> Result<(), ClientError> {
    let mut stream = UnixStream::connect(socket_path).await?;
    let body = serde_json::json!({"cols": cols, "rows": rows});
    let body_bytes = serde_json::to_vec(&body).unwrap_or_default();
    let path = format!("/attach/{}/resize", super::urlencode(name));
    let req = format!(
        "POST {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        body_bytes.len(),
    );
    stream.write_all(req.as_bytes()).await?;
    stream.write_all(&body_bytes).await?;
    // Read and discard response.
    let mut buf = [0u8; 256];
    let _ = stream.read(&mut buf).await;
    Ok(())
}
