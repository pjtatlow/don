#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod helpers;

use don::config::{Config, LogConfig, Platform};
use don::output::OutputManager;
use don::runner::Runner;
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
    let mut runner = Runner::new(
        config,
        PLATFORM,
        output_manager,
        base_dir.to_path_buf(),
        None,
        shutdown_rx,
        true,
    )
    .await
    .unwrap();
    // The runner no longer binds its own API socket; the binary does,
    // and so must anything else that wants CLI/daemon access.
    let api_shutdown = don::server::serve_for_runner(&runner).unwrap();
    runner.set_api_shutdown(api_shutdown);

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

/// Attach is multi-client (tmux-style shared input): a second client gets
/// its own session, both see the process's output, and input from either
/// interleaves at the PTY without shearing.
#[test]
fn integration_second_attach_shares_input_and_output() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("attach-multi");
        let toml = ConfigBuilder::new()
            .add_custom_service("echoer", "cat", &[])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        tokio::time::sleep(Duration::from_millis(300)).await;

        let (mut stream1, _) = raw_attach(&socket, "echoer", 11111).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (mut stream2, _) = raw_attach(&socket, "echoer", 22222).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Input from the second client reaches the process, and BOTH
        // clients see the echoed output.
        stream2.write_all(b"from-two\n").await.unwrap();
        let out1 = collect_bytes(&mut stream1, Duration::from_secs(2)).await;
        let out2 = collect_bytes(&mut stream2, Duration::from_secs(2)).await;
        let text1 = String::from_utf8_lossy(&out1);
        let text2 = String::from_utf8_lossy(&out2);
        assert!(
            text1.contains("from-two"),
            "first client should see the echo; got: {text1:?}"
        );
        assert!(
            text2.contains("from-two"),
            "second client should see the echo; got: {text2:?}"
        );

        // Input from the first client still works with two attached.
        stream1.write_all(b"from-one\n").await.unwrap();
        let out2 = collect_bytes(&mut stream2, Duration::from_secs(2)).await;
        let text2 = String::from_utf8_lossy(&out2);
        assert!(
            text2.contains("from-one"),
            "second client should see the first's input echoed; got: {text2:?}"
        );

        drop(stream1);
        drop(stream2);
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
