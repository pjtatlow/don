#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod helpers;

use don::config::{Config, LogConfig, Platform};
use don::output::OutputManager;
use don::runner::{Runner, TerminalCoordinator};
use helpers::config::ConfigBuilder;
use helpers::tempdir::TempDir;
use helpers::timeout::run_with_timeout;
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::mpsc;

const PLATFORM: Platform = Platform::LinuxX86_64;

/// Wait for the socket file to exist.
async fn wait_for_socket(path: &Path, timeout: Duration) -> bool {
    let start = tokio::time::Instant::now();
    while !path.exists() {
        if start.elapsed() > timeout {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    true
}

/// Spawn a runner. Returns (socket_path, shutdown_tx, join_handle).
async fn spawn_runner(
    toml: &str,
    base_dir: &Path,
) -> (
    std::path::PathBuf,
    mpsc::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let config: Config = toml.parse().unwrap();
    config.validate(PLATFORM).unwrap();

    let service_configs: Vec<(&str, &LogConfig)> = config
        .services
        .iter()
        .map(|(n, s)| (n.as_str(), &s.log))
        .collect();
    let task_configs: Vec<(&str, &LogConfig)> = config
        .tasks
        .iter()
        .map(|(n, t)| (n.as_str(), &t.log))
        .collect();
    let all_configs: Vec<(&str, &LogConfig)> =
        service_configs.into_iter().chain(task_configs).collect();

    let output_manager = OutputManager::new(&all_configs, tokio::io::sink())
        .await
        .unwrap();
    let (shutdown_tx, shutdown_rx) = mpsc::channel(2);
    let runner = Runner::new(
        config,
        PLATFORM,
        output_manager,
        base_dir.to_path_buf(),
        None,
        shutdown_rx,
        TerminalCoordinator::detached(),
    )
    .await
    .unwrap();
    let socket_path = base_dir.join(".don").join("don.sock");
    let handle = tokio::spawn(async move {
        if let Err(err) = runner.run().await {
            eprintln!("runner failed: {err}");
        }
    });
    (socket_path, shutdown_tx, handle)
}

/// Connect to the daemon using the raw HTTP upgrade protocol.
/// Returns the stream ready for raw I/O, plus any leftover bytes from the header read.
async fn raw_attach(socket_path: &Path, name: &str, pid: u32) -> (UnixStream, Vec<u8>) {
    let mut stream = UnixStream::connect(socket_path).await.unwrap();
    let req = format!(
        "GET /attach/{name}?pid={pid}&cols=80&rows=24 HTTP/1.1\r\n\
         Host: localhost\r\n\
         Connection: Upgrade\r\n\
         Upgrade: don-attach\r\n\
         \r\n"
    );
    stream.write_all(req.as_bytes()).await.unwrap();

    // Read response headers.
    let mut buf = Vec::new();
    let mut scratch = [0u8; 1024];
    loop {
        let n = stream.read(&mut scratch).await.unwrap();
        assert!(n > 0, "connection closed before headers");
        buf.extend_from_slice(&scratch[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    let header_end = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let head = std::str::from_utf8(&buf[..header_end]).unwrap();
    let status_line = head.split("\r\n").next().unwrap();
    let leftover = buf[header_end + 4..].to_vec();

    // Verify 101.
    assert!(
        status_line.contains("101"),
        "expected 101 Switching Protocols, got: {status_line}"
    );
    (stream, leftover)
}

/// Connect and expect a non-101 error response. Returns (status, body_text).
async fn raw_attach_error(socket_path: &Path, name: &str, pid: u32) -> (u16, String) {
    let mut stream = UnixStream::connect(socket_path).await.unwrap();
    let req = format!(
        "GET /attach/{name}?pid={pid}&cols=80&rows=24 HTTP/1.1\r\n\
         Host: localhost\r\n\
         Connection: close\r\n\
         Upgrade: don-attach\r\n\
         \r\n"
    );
    stream.write_all(req.as_bytes()).await.unwrap();

    // Read headers.
    let mut buf = Vec::new();
    let mut scratch = [0u8; 1024];
    loop {
        let n = stream.read(&mut scratch).await.unwrap();
        assert!(n > 0, "connection closed before headers");
        buf.extend_from_slice(&scratch[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    let header_end = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let head = std::str::from_utf8(&buf[..header_end]).unwrap();
    let status_line = head.split("\r\n").next().unwrap();
    let status: u16 = status_line
        .split(' ')
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    // Parse content-length from headers.
    let content_length: usize = head
        .split("\r\n")
        .find(|h| h.to_ascii_lowercase().starts_with("content-length:"))
        .and_then(|h| h.split_once(':').map(|(_, v)| v.trim()))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    // Read body.
    let mut body_bytes = buf[header_end + 4..].to_vec();
    while body_bytes.len() < content_length {
        let n = stream.read(&mut scratch).await.unwrap();
        if n == 0 {
            break;
        }
        body_bytes.extend_from_slice(&scratch[..n]);
    }
    body_bytes.truncate(content_length);
    let body = String::from_utf8_lossy(&body_bytes).to_string();
    (status, body)
}

/// Collect bytes from the stream until timeout.
async fn collect_bytes(stream: &mut UnixStream, timeout: Duration) -> Vec<u8> {
    let mut all = Vec::new();
    let mut buf = [0u8; 4096];
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, stream.read(&mut buf)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => all.extend_from_slice(&buf[..n]),
            _ => break,
        }
    }
    all
}

/// Send a resize via separate HTTP POST.
async fn send_resize(socket_path: &Path, name: &str, cols: u16, rows: u16) {
    let mut stream = UnixStream::connect(socket_path).await.unwrap();
    let body = format!("{{\"cols\":{cols},\"rows\":{rows}}}");
    let req = format!(
        "POST /attach/{name}/resize HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len(),
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut buf = [0u8; 256];
    let _ = stream.read(&mut buf).await;
}

// --- Integration tests ---

#[test]
fn integration_attach_send_input_and_receive_output() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("attach-io");
        let toml = ConfigBuilder::new()
            .add_custom_service("echoer", "cat", &[])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        tokio::time::sleep(Duration::from_millis(300)).await;

        let (mut stream, _leftover) = raw_attach(&socket, "echoer", 12345).await;

        // Drain any initial output (brief pause).
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send some input.
        stream.write_all(b"hello\n").await.unwrap();

        // Wait for the echo back.
        let output = collect_bytes(&mut stream, Duration::from_secs(2)).await;
        let text = String::from_utf8_lossy(&output);
        assert!(
            text.contains("hello"),
            "expected echoed input in output; got: {text:?}"
        );

        drop(stream);
        tokio::time::sleep(Duration::from_millis(100)).await;

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_second_attach_rejected_with_pid() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("attach-lock");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        tokio::time::sleep(Duration::from_millis(300)).await;

        // First attach should succeed (101).
        let (_stream1, _) = raw_attach(&socket, "keeper", 11111).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Second attach should be rejected (409 Conflict).
        let (status, body) = raw_attach_error(&socket, "keeper", 22222).await;
        assert_eq!(status, 409, "expected 409 Conflict, got {status}");
        assert!(
            body.contains("11111"),
            "error should mention first PID; got: {body}"
        );
        assert!(
            body.contains("attached"),
            "error should mention 'attached'; got: {body}"
        );

        drop(_stream1);
        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_second_attach_succeeds_after_first_disconnects() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("attach-release");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        tokio::time::sleep(Duration::from_millis(300)).await;

        // First attach.
        let (stream1, _) = raw_attach(&socket, "keeper", 11111).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Disconnect first.
        drop(stream1);
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Second attach should succeed (101).
        let (_stream2, _) = raw_attach(&socket, "keeper", 22222).await;
        // If we got here, the upgrade succeeded.

        drop(_stream2);
        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_service_keeps_running_after_detach() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("attach-detach-alive");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Attach and then disconnect.
        let (stream, _) = raw_attach(&socket, "keeper", 12345).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(stream);
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Check that the service is still running via the status endpoint.
        let mut status_stream = UnixStream::connect(&socket).await.unwrap();
        let req = "GET /status HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        status_stream.write_all(req.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        status_stream.read_to_end(&mut response).await.unwrap();
        let body = String::from_utf8_lossy(&response);
        assert!(
            body.contains("\"state\":\"running\"") || body.contains("\"state\":\"ready\""),
            "service should still be running after detach; got: {body}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_attach_to_nonexistent_service_returns_error() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("attach-unknown");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        tokio::time::sleep(Duration::from_millis(300)).await;

        let (status, body) = raw_attach_error(&socket, "ghost", 12345).await;
        assert_eq!(status, 404, "expected 404, got {status}");
        assert!(
            body.contains("ghost"),
            "error should mention service name; got: {body}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_resize_propagates_to_subprocess_pty() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("attach-resize");
        let toml = ConfigBuilder::new()
            .add_custom_service(
                "resizer",
                "bash",
                &["-c", "trap 'stty size' WINCH; while true; do sleep 1; done"],
            )
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        tokio::time::sleep(Duration::from_millis(300)).await;

        let (mut stream, _) = raw_attach(&socket, "resizer", 12345).await;
        // Drain any initial output.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let _ = collect_bytes(&mut stream, Duration::from_millis(100)).await;

        // Send resize via separate HTTP POST: 42 rows × 133 cols.
        send_resize(&socket, "resizer", 133, 42).await;

        // Wait for the SIGWINCH handler to fire and `stty size` to output.
        let output = collect_bytes(&mut stream, Duration::from_secs(3)).await;
        let text = String::from_utf8_lossy(&output);
        assert!(
            text.contains("42 133"),
            "expected '42 133' in output after resize; got: {text:?}"
        );

        drop(stream);
        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// Poll the status endpoint until the named task reaches `state`, or fail.
async fn wait_for_task_state(
    socket_path: &Path,
    name: &str,
    state: don::runner::TaskItemState,
    timeout: Duration,
) -> bool {
    let client = don::client::Client::with_socket_path(socket_path.to_path_buf());
    let start = tokio::time::Instant::now();
    while start.elapsed() < timeout {
        if let Ok(items) = client.status(false, Some(name)).await
            && let [don::runner::ItemStatus::Task { state: s, .. }] = items.as_slice()
            && *s == state
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

#[test]
fn integration_foreground_task_without_terminal_runs_on_pty() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("fg-headless");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["300"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_task("setup", "sh", &["-c", "echo setup done"])
            .auto_run_mode("once")
            .terminal_mode("foreground")
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);

        assert!(
            wait_for_task_state(
                &socket,
                "setup",
                don::runner::TaskItemState::Completed,
                Duration::from_secs(5),
            )
            .await,
            "foreground task should fall back to a PTY spawn and complete \
             when the runner has no terminal"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_foreground_task_bridges_input_and_closes_on_exit() {
    run_with_timeout(Duration::from_secs(20), async {
        let dir = TempDir::new("fg-bridge");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["300"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_task("interactive", "sh", &["-c", "read x; echo got: $x"])
            .auto_run(false)
            .terminal_mode("foreground")
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);

        // Attach first (registers a waiter), then trigger the run — the
        // ordering `don run` uses so a fast task can't slip past the bridge.
        let socket_clone = socket.clone();
        let attach = tokio::spawn(async move { raw_attach(&socket_clone, "interactive", 4242).await });

        let client = don::client::Client::with_socket_path(socket.clone());
        client
            .run_task_with_options(
                "interactive",
                std::collections::HashMap::new(),
                don::client::RunTaskOptions {
                    wait: false,
                    wait_timeout: None,
                },
            )
            .await
            .unwrap();

        let (mut stream, _leftover) = attach.await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        stream.write_all(b"abc\n").await.unwrap();

        // The task echoes the input and exits; the server must then close
        // the attach stream rather than leaving the client hanging.
        let output = collect_bytes(&mut stream, Duration::from_secs(5)).await;
        let text = String::from_utf8_lossy(&output);
        assert!(text.contains("got: abc"), "expected task output; got: {text:?}");

        let mut probe = [0u8; 16];
        let closed = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut probe)).await;
        assert!(
            matches!(closed, Ok(Ok(0)) | Ok(Err(_))),
            "attach stream should close when the task exits"
        );

        assert!(
            wait_for_task_state(
                &socket,
                "interactive",
                don::runner::TaskItemState::Completed,
                Duration::from_secs(3),
            )
            .await,
            "task should be recorded as completed"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}
