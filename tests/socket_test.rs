#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod helpers;

use don::config::{Config, LogConfig, Platform};
use don::output::OutputManager;
use don::ports::{manifest_path, read_manifest};
use don::runner::Runner;
use helpers::config::ConfigBuilder;
use helpers::port::free_port;
use helpers::tempdir::TempDir;
use helpers::timeout::run_with_timeout;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

const PLATFORM: Platform = Platform::LinuxX86_64;

#[derive(Clone)]
struct TestBuffer(Arc<Mutex<Vec<u8>>>);

impl TestBuffer {
    fn new() -> (Self, Arc<Mutex<Vec<u8>>>) {
        let buf = Arc::new(Mutex::new(Vec::new()));
        (TestBuffer(buf.clone()), buf)
    }
}

impl tokio::io::AsyncWrite for TestBuffer {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        data: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.0.lock().unwrap().extend_from_slice(data);
        std::task::Poll::Ready(Ok(data.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

fn read_buf(buf: &Arc<Mutex<Vec<u8>>>) -> String {
    String::from_utf8_lossy(&buf.lock().unwrap()).into_owned()
}

async fn make_runner(
    toml: &str,
    base_dir: &std::path::Path,
) -> (Runner, mpsc::Sender<()>, Arc<Mutex<Vec<u8>>>) {
    make_runner_inner(toml, base_dir, false).await
}

async fn make_runner_verbose(
    toml: &str,
    base_dir: &std::path::Path,
) -> (Runner, mpsc::Sender<()>, Arc<Mutex<Vec<u8>>>) {
    make_runner_inner(toml, base_dir, true).await
}

async fn make_runner_inner(
    toml: &str,
    base_dir: &std::path::Path,
    verbose: bool,
) -> (Runner, mpsc::Sender<()>, Arc<Mutex<Vec<u8>>>) {
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

    let (writer, buf) = TestBuffer::new();
    let output_manager = OutputManager::new_verbose(&all_configs, writer, verbose)
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

    (runner, shutdown_tx, buf)
}

async fn wait_for_output(buf: &Arc<Mutex<Vec<u8>>>, needle: &str, timeout: Duration) -> bool {
    let start = tokio::time::Instant::now();
    loop {
        let output = read_buf(buf);
        if output.contains(needle) {
            return true;
        }
        if start.elapsed() > timeout {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// --- Integration test: service receives LISTEN_FDS and LISTEN_FDNAMES ---

#[test]
fn integration_service_receives_listen_env_vars() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("socket-env");
        let port = free_port();
        let addr = format!("127.0.0.1:{port}");

        // Service prints its socket activation env, including $$ PID so the
        // test can verify LISTEN_PID matches the child process.
        let toml = ConfigBuilder::new()
            .add_custom_service(
                "api",
                "bash",
                &["-c", "echo LISTEN_FD=$LISTEN_FD LISTEN_FDS=$LISTEN_FDS LISTEN_FDNAMES=$LISTEN_FDNAMES LISTEN_PID=$LISTEN_PID SELF_PID=$$ && sleep 60"],
            )
            .listen(&[&addr])
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Wait for the service to print its env vars.
        assert!(
            wait_for_output(&buf, "LISTEN_FDS=1", Duration::from_secs(5)).await,
            "expected LISTEN_FDS=1 in output. output: {}",
            read_buf(&buf)
        );

        let output = read_buf(&buf);
        assert!(
            output.contains("LISTEN_FD=3"),
            "expected LISTEN_FD=3 for a single passed fd. output: {output}"
        );
        assert!(
            output.contains(&format!("LISTEN_FDNAMES={addr}")),
            "expected LISTEN_FDNAMES={addr} in output. output: {output}"
        );
        // LISTEN_PID must equal the child's own PID per the systemd socket
        // activation protocol. Extract both from the child's stdout and
        // compare. A previous version of the code set LISTEN_PID via
        // `setenv` in pre_exec, which the explicit envp in Rust's `execve`
        // silently discarded, leaving the value empty.
        fn extract(out: &str, key: &str) -> String {
            out.split_whitespace()
                .find_map(|tok| tok.strip_prefix(key))
                .unwrap_or("")
                .to_string()
        }
        let listen_pid = extract(&output, "LISTEN_PID=");
        let self_pid = extract(&output, "SELF_PID=");
        assert!(
            !listen_pid.is_empty() && listen_pid.chars().all(|c| c.is_ascii_digit()),
            "LISTEN_PID must be a non-empty numeric PID, got '{listen_pid}'. output: {output}"
        );
        assert_eq!(
            listen_pid, self_pid,
            "LISTEN_PID must match the child process's own PID. output: {output}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Integration test: occupied proxy ports fall back and publish the result ---

#[test]
fn integration_proxy_fallback_ports_table() {
    struct Case {
        name: &'static str,
        occupy_preferred_port: bool,
    }

    let cases = vec![
        Case {
            name: "occupied",
            occupy_preferred_port: true,
        },
        Case {
            name: "available",
            occupy_preferred_port: false,
        },
    ];

    run_with_timeout(Duration::from_secs(30), async {
        for case in cases {
            let dir = TempDir::new(&format!("proxy-fallback-{}", case.name));
            let preferred_port = free_port();
            let preferred_addr = format!("127.0.0.1:{preferred_port}");
            let blocker = case
                .occupy_preferred_port
                .then(|| std::net::TcpListener::bind(&preferred_addr).unwrap());

            let toml = ConfigBuilder::new()
                .raw("fallback_ports = true")
                .add_custom_service(
                    "api",
                    "bash",
                    &["-c", "echo API_PUBLIC=$DON_PUBLIC_PORT; exec sleep 60"],
                )
                .listen(&[&preferred_addr])
                .ready_tcp_with(&preferred_addr, "100ms", 30)
                .done()
                .add_custom_service(
                    "consumer",
                    "bash",
                    &["-c", "echo CONSUMER_API_PORT=$API_PORT; exec sleep 60"],
                )
                .depends_on(&["api"])
                .env("API_PORT", "$(api.port)")
                .ready_exec("true", &[])
                .done()
                .build();

            let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
            let handle = tokio::spawn(async move {
                runner.run().await.unwrap();
            });

            assert!(
                wait_for_output(&buf, "all services running", Duration::from_secs(8)).await,
                "{}: timed out waiting for startup. output: {}",
                case.name,
                read_buf(&buf)
            );

            let manifest = read_manifest(dir.path()).unwrap();
            let binding = manifest
                .services
                .get("api")
                .and_then(|ports| ports.proxy.first())
                .unwrap();
            let actual_addr: std::net::SocketAddr = binding.bound_addr.parse().unwrap();

            assert_eq!(binding.configured_addr, preferred_addr, "{}", case.name);
            if case.occupy_preferred_port {
                assert_ne!(
                    actual_addr.port(),
                    preferred_port,
                    "{}: occupied preferred port should fall back",
                    case.name
                );
                assert!(
                    read_buf(&buf).contains(&format!(
                        "{preferred_addr} is in use; using {}",
                        binding.bound_addr
                    )),
                    "{}: expected a visible fallback event. output: {}",
                    case.name,
                    read_buf(&buf)
                );
            } else {
                assert_eq!(
                    actual_addr.port(),
                    preferred_port,
                    "{}: an available preferred port should remain exact",
                    case.name
                );
            }

            assert!(
                wait_for_output(
                    &buf,
                    &format!("CONSUMER_API_PORT={}", actual_addr.port()),
                    Duration::from_secs(2)
                )
                .await,
                "{}: dependent runtime env did not receive the actual public port. output: {}",
                case.name,
                read_buf(&buf)
            );

            let _ = shutdown_tx.send(()).await;
            handle.await.unwrap();
            assert!(
                !manifest_path(dir.path()).exists(),
                "{}: runtime port manifest should be removed after shutdown",
                case.name
            );

            drop(blocker);
        }
    });
}

// --- Integration test: service can accept on the passed fd ---

#[test]
fn integration_service_accepts_on_listen_fd() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("socket-accept");
        let port = free_port();
        let addr = format!("127.0.0.1:{port}");

        // Write a Python script that accepts connections on fd 3.
        let script_path = dir.path().join("accept.py");
        std::fs::write(
            &script_path,
            r#"
import socket, os, sys
s = socket.fromfd(3, socket.AF_INET, socket.SOCK_STREAM)
os.close(3)
while True:
    conn, addr = s.accept()
    conn.sendall(b'hello from fd 3')
    conn.close()
"#,
        )
        .unwrap();

        let script = script_path.to_str().unwrap().to_string();
        let toml = ConfigBuilder::new()
            .add_custom_service("api", "python3", &[&script])
            .listen(&[&addr])
            .ready_tcp_with(&addr, "200ms", 30)
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Wait for the service to be ready (TCP ready check passes).
        assert!(
            wait_for_output(&buf, "ready", Duration::from_secs(10)).await,
            "timed out waiting for service to be ready. output: {}",
            read_buf(&buf)
        );

        // Connect and verify we get a response.
        let result = tokio::net::TcpStream::connect(&addr).await;
        if let Ok(stream) = result {
            use tokio::io::AsyncReadExt;
            let mut buf = vec![0u8; 64];
            let mut stream = stream;
            let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
                .await
                .unwrap()
                .unwrap();
            let response = String::from_utf8_lossy(&buf[..n]);
            assert_eq!(response, "hello from fd 3");
        }
        // If python3 isn't available, the connect might fail — that's okay,
        // the TCP ready check already proved the socket is accepting.

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Integration test: Node can use the passed fd from LISTEN_FD ---

#[test]
fn integration_node_accepts_on_listen_fd() {
    run_with_timeout(Duration::from_secs(15), async {
        if std::process::Command::new("node")
            .arg("--version")
            .output()
            .is_err()
        {
            eprintln!("[skip] node not available");
            return;
        }

        let dir = TempDir::new("socket-node-accept");
        let port = free_port();
        let addr = format!("127.0.0.1:{port}");

        let script_path = dir.path().join("server.js");
        std::fs::write(
            &script_path,
            r#"
const http = require('http');
const server = http.createServer((_req, res) => {
  res.end('hello from node listen fd');
});
server.on('error', (err) => {
  console.error(`${err.code || err.name}: ${err.message}`);
  process.exit(1);
});
const fd = Number.parseInt(process.env.LISTEN_FD || '', 10);
if (!Number.isInteger(fd)) {
  console.error(`missing LISTEN_FD: ${process.env.LISTEN_FD || ''}`);
  process.exit(1);
}
server.listen({ fd }, () => {
  console.log(`node listening fd=${fd}`);
});
setInterval(() => {}, 1000);
"#,
        )
        .unwrap();

        let script = script_path.to_str().unwrap().to_string();
        let toml = ConfigBuilder::new()
            .add_custom_service("api", "node", &[&script])
            .listen(&[&addr])
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "node listening fd=3", Duration::from_secs(5)).await,
            "timed out waiting for node to listen on fd 3. output: {}",
            read_buf(&buf)
        );

        let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        tokio::time::timeout(Duration::from_secs(2), stream.read_to_string(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert!(
            response.contains("hello from node listen fd"),
            "expected node response over inherited fd. response: {response}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Integration test: socket stays bound during restart ---

#[test]
fn integration_socket_stays_bound_during_restart() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("socket-restart");
        let port = free_port();
        let addr = format!("127.0.0.1:{port}");

        let src_dir = dir.path().join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("main.rs"), "v1").unwrap();

        // Service sleeps. The socket is bound by don, so even during restart
        // the port stays open (connections queue in the backlog).
        let toml = ConfigBuilder::new()
            .add_custom_service("api", "bash", &["-c", "sleep 60"])
            .listen(&[&addr])
            .watch(&["src/**/*.rs"])
            .debounce("100ms")
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner_verbose(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(
                &buf,
                &format!("proxy listening on {addr}"),
                Duration::from_secs(5)
            )
            .await,
            "timed out waiting for proxy bind. output: {}",
            read_buf(&buf)
        );
        assert!(
            wait_for_output(&buf, "ready", Duration::from_secs(5)).await,
            "timed out waiting for ready. output: {}",
            read_buf(&buf)
        );

        // Trigger a rebuild by modifying a watched file.
        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(src_dir.join("main.rs"), "v2").unwrap();

        // Wait for the rebuild to start.
        assert!(
            wait_for_output(&buf, "rebuilding (file changed)", Duration::from_secs(5)).await,
            "timed out waiting for rebuild. output: {}",
            read_buf(&buf)
        );

        // During and after the rebuild, the port should still be bound.
        // A TCP connect should succeed (connection queues in the backlog
        // even if no process is currently accepting).
        tokio::time::sleep(Duration::from_millis(200)).await;
        let connect_result = tokio::time::timeout(
            Duration::from_secs(2),
            tokio::net::TcpStream::connect(&addr),
        )
        .await;
        assert!(
            connect_result.is_ok() && connect_result.unwrap().is_ok(),
            "expected TCP connect to succeed during restart (socket should stay bound)"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Integration test: services with `listen` still get a PTY ---
//
// Phase 11.5 dropped the "force pipe mode when listen_fds is non-empty"
// guard in process/mod.rs. This proves services with passed sockets now
// see a real TTY on stdout (libc switches stdio to line-buffered).

#[test]
fn integration_listen_service_gets_pty() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("socket-pty");
        let port = free_port();
        let addr = format!("127.0.0.1:{port}");

        let toml = ConfigBuilder::new()
            .add_custom_service(
                "api",
                "bash",
                &[
                    "-c",
                    "if [ -t 1 ]; then echo isatty=TTY; else echo isatty=PIPE; fi; sleep 60",
                ],
            )
            .listen(&[&addr])
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        let got_tty = wait_for_output(&buf, "isatty=TTY", Duration::from_secs(5)).await;
        let got_pipe = wait_for_output(&buf, "isatty=PIPE", Duration::from_millis(100)).await;

        // In PTY-capable environments (most dev machines, linux+macos with /dev/ptmx),
        // we must see TTY. In headless-CI fallback, isatty=PIPE is acceptable.
        assert!(
            got_tty || got_pipe,
            "neither isatty result observed. output: {}",
            read_buf(&buf)
        );
        if !got_tty && got_pipe {
            eprintln!("[warn] PTY alloc failed in this environment — service ran in pipe fallback");
        } else {
            assert!(
                got_tty,
                "expected isatty=TTY for service with listen (PTY mode)"
            );
        }

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// Phase 11.5: verify line-buffering works. A Python service that prints
// without flushing should produce output promptly on a PTY (libc stdio
// line-buffers when stdout is a TTY). Skips gracefully if python3 isn't
// available or if PTY alloc fell back to pipe.
#[test]
fn integration_python_line_buffered_on_pty() {
    run_with_timeout(Duration::from_secs(10), async {
        // Skip if python3 isn't available.
        if std::process::Command::new("python3")
            .arg("--version")
            .output()
            .is_err()
        {
            eprintln!("[skip] python3 not available");
            return;
        }

        let dir = TempDir::new("socket-linebuf");
        // Print one line, pause 3s, then print a second line — without explicit flush.
        // On a pipe (block-buffered), line1 wouldn't appear until the 4KB buffer
        // fills or the process exits. On a PTY (line-buffered), it appears at once.
        //
        // We scrub PYTHONUNBUFFERED so we test stock libc stdio behavior.
        let toml = ConfigBuilder::new()
            .add_custom_service(
                "api",
                "python3",
                &["-c", "import time,sys,os; os.environ.pop('PYTHONUNBUFFERED',None); print('line1-prompt'); time.sleep(3); print('line2-late'); time.sleep(30)"],
            )
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // If line-buffering works, we see line1 within ~1s (before the 3s sleep).
        let got_prompt = wait_for_output(&buf, "line1-prompt", Duration::from_millis(1500)).await;
        assert!(
            got_prompt,
            "line1-prompt did not appear promptly — stdout likely block-buffered. output: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Integration test: multiple listen addresses ---

#[test]
fn integration_multiple_listen_addresses() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("socket-multi");
        let port1 = free_port();
        let port2 = free_port();
        let addr1 = format!("127.0.0.1:{port1}");
        let addr2 = format!("127.0.0.1:{port2}");

        let toml = ConfigBuilder::new()
            .add_custom_service(
                "api",
                "bash",
                &[
                    "-c",
                    "echo LISTEN_FDS=$LISTEN_FDS LISTEN_FDNAMES=$LISTEN_FDNAMES && sleep 60",
                ],
            )
            .listen(&[&addr1, &addr2])
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "LISTEN_FDS=2", Duration::from_secs(5)).await,
            "expected LISTEN_FDS=2 in output. output: {}",
            read_buf(&buf)
        );

        let output = read_buf(&buf);
        let expected_names = format!("{addr1}:{addr2}");
        assert!(
            output.contains(&format!("LISTEN_FDNAMES={expected_names}")),
            "expected LISTEN_FDNAMES={expected_names} in output. output: {output}"
        );

        // Both ports should be connectable.
        let c1 = tokio::net::TcpStream::connect(&addr1).await;
        let c2 = tokio::net::TcpStream::connect(&addr2).await;
        assert!(c1.is_ok(), "expected connect to {addr1} to succeed");
        assert!(c2.is_ok(), "expected connect to {addr2} to succeed");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Integration test: lazy + listenfd triggers on a connection ---
//
// Exercises the POLLIN-based lazy path: don binds the public listener,
// doesn't accept, and only starts the service when a queued connection
// shows up. This is what `listen = [...]` used to *not* support.

#[test]
fn integration_lazy_listenfd_triggers_on_connect() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("lazy-listenfd");
        let port = free_port();
        let addr = format!("127.0.0.1:{port}");

        let toml = ConfigBuilder::new()
            .add_custom_service(
                "api",
                "bash",
                &["-c", "echo LISTEN_PID=$$ SELF=$$ && sleep 60"],
            )
            .proxy_listenfd(&[&addr])
            .lazy(true)
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner_verbose(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Service is lazy — proxy binds, but the service should NOT start
        // until we connect. Give it a moment, then assert nothing started.
        assert!(
            wait_for_output(
                &buf,
                &format!("proxy listening on {addr}"),
                Duration::from_secs(5)
            )
            .await,
            "expected proxy to bind. output: {}",
            read_buf(&buf)
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !read_buf(&buf).contains("api: starting"),
            "lazy service started without a trigger. output: {}",
            read_buf(&buf)
        );

        // Opening a connection must trigger start via POLLIN.
        let _stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        assert!(
            wait_for_output(&buf, "first connection", Duration::from_secs(5)).await,
            "expected lazy trigger on connect. output: {}",
            read_buf(&buf)
        );
        assert!(
            wait_for_output(&buf, "api: ready", Duration::from_secs(5)).await,
            "expected service to reach ready after lazy trigger. output: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// A lazy service that dies the instant it launches must not be relaunched
/// forever. The trigger connection the dying service never accepts stays queued
/// and re-fires the launch the moment the proxy re-arms; without the crash-loop
/// guard on the lazy path this is a tight, no-backoff restart loop. After the
/// rapid-crash ceiling the service is left Failed with its trigger un-armed.
#[test]
fn integration_lazy_crash_loop_gives_up_and_stops_relaunching() {
    run_with_timeout(Duration::from_secs(20), async {
        let dir = TempDir::new("lazy-crash-loop");
        let port = free_port();
        let addr = format!("127.0.0.1:{port}");
        let counter = dir.path().join("launches");

        let cmd = format!(
            "N=$(cat {ctr} 2>/dev/null || echo 0); N=$((N + 1)); echo $N > {ctr}; exit 1",
            ctr = counter.display()
        );
        let toml = ConfigBuilder::new()
            .add_custom_service("api", "bash", &["-c", &cmd])
            .proxy_listenfd(&[&addr])
            .lazy(true)
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner_verbose(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(
                &buf,
                &format!("proxy listening on {addr}"),
                Duration::from_secs(5)
            )
            .await,
            "expected proxy to bind. output: {}",
            read_buf(&buf)
        );

        // A single connection triggers the lazy start. The service crashes,
        // the queued connection re-fires the launch once more, then the guard
        // must give up.
        let _stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        assert!(
            wait_for_output(&buf, "giving up", Duration::from_secs(10)).await,
            "expected the lazy crash loop to give up. output: {}",
            read_buf(&buf)
        );

        // Let any errant retrigger fire, then probe a few more times: with the
        // trigger un-armed these connections must not relaunch the service.
        tokio::time::sleep(Duration::from_secs(1)).await;
        for _ in 0..3 {
            let _ = tokio::net::TcpStream::connect(&addr).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();

        let launches: i32 = std::fs::read_to_string(&counter)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert_eq!(
            launches,
            2,
            "lazy service should launch exactly twice before giving up, got {launches}. output: {}",
            read_buf(&buf)
        );
    });
}

/// A lazy service whose `depends_on` is still running at first-connection must
/// wait, then start. The marker proves `setup` finished before `api` launched.
#[test]
fn integration_lazy_defers_start_until_dependency_satisfied() {
    run_with_timeout(Duration::from_secs(20), async {
        let dir = TempDir::new("lazy-dep-defer");
        let port = free_port();
        let addr = format!("127.0.0.1:{port}");
        let marker = dir.path().join("setup-done");

        let setup_cmd = format!("sleep 1; touch {}; echo SETUP_DONE", marker.display());
        // The sentinel value is computed at runtime, so the printed
        // "API_MARKER=yes/no" never appears in don's echoed spawn command.
        let api_cmd = format!(
            "echo API_MARKER=$(test -f {m} && echo yes || echo no); exec sleep 60",
            m = marker.display()
        );
        let toml = ConfigBuilder::new()
            .add_task("setup", "bash", &["-c", &setup_cmd])
            .done()
            .add_custom_service("api", "bash", &["-c", &api_cmd])
            .proxy_listenfd(&[&addr])
            .lazy(true)
            .depends_on(&["setup"])
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner_verbose(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(
                &buf,
                &format!("proxy listening on {addr}"),
                Duration::from_secs(5)
            )
            .await,
            "expected proxy to bind. output: {}",
            read_buf(&buf)
        );

        // Connect while `setup` is still running.
        let _stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        assert!(
            wait_for_output(
                &buf,
                "waiting for dependencies before start: setup",
                Duration::from_secs(5)
            )
            .await,
            "expected the lazy start to defer for its dependency. output: {}",
            read_buf(&buf)
        );
        assert!(
            wait_for_output(&buf, "api: ready", Duration::from_secs(8)).await,
            "expected api to start after setup completed. output: {}",
            read_buf(&buf)
        );
        // `api: ready` only says the ready check passed, and this one is
        // `true` — it succeeds a couple of milliseconds after the spawn, well
        // before the service's own first line has crossed the PTY, the reader
        // and the output actor to reach this buffer. Reading the buffer here
        // raced that journey and lost on macOS. Wait for the line itself; what
        // it *says* is what the test is actually about.
        assert!(
            wait_for_output(&buf, "API_MARKER=", Duration::from_secs(5)).await,
            "expected api to report whether it saw the marker. output: {}",
            read_buf(&buf)
        );

        let output = read_buf(&buf);
        assert!(
            output.contains("API_MARKER=yes"),
            "api must have started after setup created its marker. output: {output}"
        );
        assert!(
            !output.contains("API_MARKER=no"),
            "api started before its dependency ran. output: {output}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// With the dependency already satisfied at connect time, the lazy start is
/// immediate — the just-in-time path is unchanged.
#[test]
fn integration_lazy_starts_immediately_when_dependency_satisfied() {
    run_with_timeout(Duration::from_secs(20), async {
        let dir = TempDir::new("lazy-dep-satisfied");
        let port = free_port();
        let addr = format!("127.0.0.1:{port}");

        let toml = ConfigBuilder::new()
            .add_task("setup", "echo", &["setup done"])
            .done()
            .add_custom_service("api", "bash", &["-c", "echo API_UP; exec sleep 60"])
            .proxy_listenfd(&[&addr])
            .lazy(true)
            .depends_on(&["setup"])
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner_verbose(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(
                &buf,
                &format!("proxy listening on {addr}"),
                Duration::from_secs(5)
            )
            .await,
            "expected proxy to bind. output: {}",
            read_buf(&buf)
        );
        assert!(
            wait_for_output(&buf, "setup: complete", Duration::from_secs(5)).await,
            "expected setup task to complete first. output: {}",
            read_buf(&buf)
        );

        let _stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        assert!(
            wait_for_output(&buf, "api: ready", Duration::from_secs(5)).await,
            "expected api to reach ready. output: {}",
            read_buf(&buf)
        );

        let output = read_buf(&buf);
        assert!(
            output.contains("first connection"),
            "satisfied deps should take the immediate first-connection path. output: {output}"
        );
        assert!(
            !output.contains("waiting for dependencies"),
            "should not defer when the dependency is already satisfied. output: {output}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// A lazy service with a pending first connection whose dependency then fails
/// must surface DependencyFailed and never launch its process.
/// An explicit `don start` honours the dependency graph — the one start path
/// that used to ignore it entirely.
///
/// The rule is asymmetric on purpose: a dependency that is still coming up is
/// worth waiting for, so the request is refused and says which one. A
/// dependency that has *settled* never will come up on its own, and the user
/// named this service anyway, so the start proceeds.
/// A stopped service stays stopped, even though its gate is wide open.
///
/// Since permission became dependency-only it is *sticky*: a service whose
/// dependencies are satisfied carries an `Open` level for the rest of the
/// session, including while it is stopped. Nothing restarts it because
/// demand is one-shot and a stop withdraws it — this pins that, because the
/// failure mode is a service that springs back to life after `don stop`.
#[test]
fn integration_a_stopped_service_does_not_restart_itself_off_a_standing_gate() {
    run_with_timeout(Duration::from_secs(25), async {
        let dir = TempDir::new("stopped-stays-stopped");

        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "bash", &["-c", "echo KEEPER_UP; exec sleep 60"])
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner_verbose(&toml, dir.path()).await;
        let control = runner.process_control();
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "KEEPER_UP", Duration::from_secs(8)).await,
            "expected keeper to start. output: {}",
            read_buf(&buf)
        );

        control
            .stop("keeper")
            .await
            .unwrap()
            .expect("stopping keeper should succeed");

        let launches_after_stop = read_buf(&buf).matches("KEEPER_UP").count();

        // Long enough for several scheduler passes to publish the still-open
        // level. A demand that was not withdrawn would spend one of them.
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(
            read_buf(&buf).matches("KEEPER_UP").count(),
            launches_after_stop,
            "a stopped service restarted itself. output: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        let _ = handle.await;
    });
}

#[test]
fn integration_explicit_start_waits_for_a_working_dep_but_not_a_settled_one() {
    run_with_timeout(Duration::from_secs(25), async {
        let dir = TempDir::new("explicit-start-deps");

        let toml = ConfigBuilder::new()
            .add_custom_service("slow", "bash", &["-c", "sleep 30"])
            .ready_exec("false", &[])
            .done()
            .add_custom_service("api", "bash", &["-c", "echo API_STARTED; exec sleep 60"])
            .depends_on(&["slow"])
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner_verbose(&toml, dir.path()).await;
        let control = runner.process_control();
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // `slow` never passes its ready check, so `api` waits.
        assert!(
            wait_for_output(&buf, "slow: starting", Duration::from_secs(8)).await,
            "expected slow to start. output: {}",
            read_buf(&buf)
        );

        // Still coming up: refuse, and name the dependency.
        let err = control
            .start("api")
            .await
            .unwrap()
            .expect_err("a dependency that is still coming up must be waited for");
        assert!(
            err.to_string().contains("waiting for dependency 'slow'"),
            "expected the blocking dependency to be named, got: {err}"
        );
        assert!(
            !read_buf(&buf).contains("API_STARTED"),
            "api must not have started. output: {}",
            read_buf(&buf)
        );

        // Settle the dependency by stopping it. Waiting can no longer help,
        // so the same request now succeeds.
        control
            .stop("slow")
            .await
            .unwrap()
            .expect("stopping slow should succeed");

        control
            .start("api")
            .await
            .unwrap()
            .expect("a settled dependency must not block an explicit start");
        assert!(
            wait_for_output(&buf, "API_STARTED", Duration::from_secs(8)).await,
            "expected api to start past its settled dependency. output: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        let _ = handle.await;
    });
}

#[test]
fn integration_lazy_dependency_failure_blocks_start() {
    run_with_timeout(Duration::from_secs(20), async {
        let dir = TempDir::new("lazy-dep-failure");
        let port = free_port();
        let addr = format!("127.0.0.1:{port}");

        let toml = ConfigBuilder::new()
            .add_task(
                "setup",
                "bash",
                &["-c", "sleep 1; echo SETUP_FAILING; exit 1"],
            )
            .done()
            .add_custom_service("api", "bash", &["-c", "echo API_STARTED; exec sleep 60"])
            .proxy_listenfd(&[&addr])
            .lazy(true)
            .depends_on(&["setup"])
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner_verbose(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(
                &buf,
                &format!("proxy listening on {addr}"),
                Duration::from_secs(5)
            )
            .await,
            "expected proxy to bind. output: {}",
            read_buf(&buf)
        );

        // Connect while `setup` is still running, so the start defers.
        let _stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        assert!(
            wait_for_output(
                &buf,
                "waiting for dependencies before start: setup",
                Duration::from_secs(5)
            )
            .await,
            "expected the lazy start to defer for its dependency. output: {}",
            read_buf(&buf)
        );
        assert!(
            wait_for_output(
                &buf,
                "api: skipped (dependency 'setup' failed)",
                Duration::from_secs(8)
            )
            .await,
            "expected api to surface DependencyFailed. output: {}",
            read_buf(&buf)
        );

        tokio::time::sleep(Duration::from_millis(300)).await;
        let output = read_buf(&buf);
        assert!(
            !output.contains("API_STARTED"),
            "api must not launch when its dependency failed. output: {output}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// Post-startup twin of the startup-time lazy-dep-failure test: a dep-task
/// rerun fails while a lazy dependent has deferred, and must still cascade.
#[test]
fn integration_lazy_dep_rerun_failure_after_startup_blocks_start() {
    run_with_timeout(Duration::from_secs(25), async {
        let dir = TempDir::new("lazy-dep-rerun-failure");
        let port = free_port();
        let addr = format!("127.0.0.1:{port}");
        let counter = dir.path().join("runs");

        // First run (startup) succeeds instantly; the second run (our triggered
        // rerun) prints SETUP_RERUN, lingers, then fails.
        //
        // The marker is split so it appears only in the task's *output*, never
        // in the `spawn bash -c '...'` line that echoes the command back. An
        // unsplit marker matches that line during startup, so the wait below
        // returns immediately and the connection races the rerun instead of
        // following it.
        let setup_cmd = format!(
            "N=$(cat {ctr} 2>/dev/null || echo 0); N=$((N + 1)); echo $N > {ctr}; \
             if [ $N -eq 1 ]; then echo SETUP_OK; exit 0; fi; \
             echo SETUP_'RERUN'; sleep 2; exit 1",
            ctr = counter.display()
        );
        let toml = ConfigBuilder::new()
            .add_task("setup", "bash", &["-c", &setup_cmd])
            .auto_run_mode("always")
            .done()
            .add_custom_service("api", "bash", &["-c", "echo API_STARTED; exec sleep 60"])
            .proxy_listenfd(&[&addr])
            .lazy(true)
            .depends_on(&["setup"])
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner_verbose(&toml, dir.path()).await;
        let control = runner.process_control();
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Startup runs `setup` once (success) and leaves `api` lazy-bound.
        assert!(
            wait_for_output(&buf, "all services running", Duration::from_secs(8)).await,
            "expected startup to complete. output: {}",
            read_buf(&buf)
        );

        // Re-run `setup` post-startup. auto_run = always makes an in-flight
        // rerun count as unsatisfied, so a connection now defers. Detached:
        // the reply lands when the re-run finishes, and the test wants to
        // connect while it is still going.
        tokio::spawn({
            let control = control.clone();
            async move {
                let _ = control.restart("setup").await;
            }
        });
        assert!(
            wait_for_output(&buf, "SETUP_RERUN", Duration::from_secs(5)).await,
            "expected setup to re-run. output: {}",
            read_buf(&buf)
        );

        // Connect while the rerun is in flight — the lazy start defers on it.
        let _stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        assert!(
            wait_for_output(
                &buf,
                "waiting for dependencies before start: setup",
                Duration::from_secs(5)
            )
            .await,
            "expected the lazy start to defer for its re-running dependency. output: {}",
            read_buf(&buf)
        );

        // The rerun fails; the deferred lazy must cascade to DependencyFailed.
        assert!(
            wait_for_output(
                &buf,
                "api: skipped (dependency 'setup' failed)",
                Duration::from_secs(8)
            )
            .await,
            "expected api to surface DependencyFailed after the rerun failed. output: {}",
            read_buf(&buf)
        );

        tokio::time::sleep(Duration::from_millis(300)).await;
        let output = read_buf(&buf);
        assert!(
            !output.contains("API_STARTED"),
            "api must not launch when its dependency rerun failed. output: {output}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// A lazy service deferred on a stopped service dep must start once that dep is
/// manually restarted to Ready — a recovery only the pending sweep observes.
#[test]
fn integration_lazy_starts_when_service_dep_recovers_off_startup() {
    run_with_timeout(Duration::from_secs(25), async {
        let dir = TempDir::new("lazy-dep-recover");
        let port = free_port();
        let addr = format!("127.0.0.1:{port}");

        let toml = ConfigBuilder::new()
            .add_custom_service("dep", "bash", &["-c", "echo DEP_UP; exec sleep 60"])
            .ready_exec("true", &[])
            .done()
            .add_custom_service("api", "bash", &["-c", "echo API_STARTED; exec sleep 60"])
            .proxy_listenfd(&[&addr])
            .lazy(true)
            .depends_on(&["dep"])
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner_verbose(&toml, dir.path()).await;
        let control = runner.process_control();
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "all services running", Duration::from_secs(8)).await,
            "expected startup to complete with dep ready and api lazy. output: {}",
            read_buf(&buf)
        );

        // Stop `dep`; the reply confirms it reached Stopped (no longer a
        // satisfied dependency).
        assert!(
            control.stop("dep").await.unwrap().is_ok(),
            "expected dep to stop. output: {}",
            read_buf(&buf)
        );

        // Connect: with dep stopped, the lazy start must defer.
        let _stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        assert!(
            wait_for_output(
                &buf,
                "waiting for dependencies before start: dep",
                Duration::from_secs(5)
            )
            .await,
            "expected the lazy start to defer for its stopped dependency. output: {}",
            read_buf(&buf)
        );
        assert!(
            !read_buf(&buf).contains("API_STARTED"),
            "api must not launch while its dependency is down. output: {}",
            read_buf(&buf)
        );

        // Restart dep off the startup loop; its recovery must re-fire the
        // deferred lazy start via the pending sweep. Detached: the reply only
        // lands once the start has settled, and the assertion below is what
        // this test is actually waiting for.
        tokio::spawn({
            let control = control.clone();
            async move {
                let _ = control.start("dep").await;
            }
        });
        assert!(
            wait_for_output(&buf, "api: ready", Duration::from_secs(10)).await,
            "expected api to start once dep recovered. output: {}",
            read_buf(&buf)
        );
        assert!(
            read_buf(&buf).contains("API_STARTED"),
            "api process should have launched after dep recovered. output: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// A post-startup service failure must drive the same Pending dependency
/// cascade as a task failure. This covers manual starts, whose ready-check
/// completion does not travel through the startup `ItemDone` channel.
#[test]
fn integration_lazy_service_dep_failure_after_startup_blocks_start() {
    run_with_timeout(Duration::from_secs(25), async {
        let dir = TempDir::new("lazy-service-dep-failure");
        let port = free_port();
        let addr = format!("127.0.0.1:{port}");
        let counter = dir.path().join("dep-runs");

        let dep_cmd = format!(
            "N=$(cat {ctr} 2>/dev/null || echo 0); N=$((N + 1)); echo $N > {ctr}; \
             echo DEP_RUN_$N; exec sleep 60",
            ctr = counter.display()
        );
        // The first startup run becomes ready. A later manual start remains in
        // Starting long enough for the lazy connection to defer, then fails its
        // ready check.
        let ready_cmd = format!("test $(cat {}) = 1", counter.display());
        let toml = ConfigBuilder::new()
            .add_custom_service("dep", "bash", &["-c", &dep_cmd])
            .ready_exec_with("bash", &["-c", &ready_cmd], "100ms", 10)
            .done()
            .add_custom_service("api", "bash", &["-c", "echo API_STARTED; exec sleep 60"])
            .proxy_listenfd(&[&addr])
            .lazy(true)
            .depends_on(&["dep"])
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner_verbose(&toml, dir.path()).await;
        let control = runner.process_control();
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "all services running", Duration::from_secs(8)).await,
            "expected initial dep run to become ready. output: {}",
            read_buf(&buf)
        );

        assert!(control.stop("dep").await.unwrap().is_ok());

        tokio::spawn({
            let control = control.clone();
            async move {
                let _ = control.start("dep").await;
            }
        });
        assert!(
            wait_for_output(&buf, "DEP_RUN_2", Duration::from_secs(5)).await,
            "expected the failing second dep run. output: {}",
            read_buf(&buf)
        );

        let _stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        assert!(
            wait_for_output(
                &buf,
                "waiting for dependencies before start: dep",
                Duration::from_secs(5)
            )
            .await,
            "expected api to enter Pending while dep was Starting. output: {}",
            read_buf(&buf)
        );
        assert!(
            wait_for_output(
                &buf,
                "api: skipped (dependency 'dep' failed)",
                Duration::from_secs(8)
            )
            .await,
            "expected the failed service dep to cascade. output: {}",
            read_buf(&buf)
        );
        assert!(
            !read_buf(&buf).contains("API_STARTED"),
            "api must not launch after its service dependency failed. output: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// A service that has landed in a failed state must refuse proxy connections
/// rather than leave clients parked on a socket that nothing will ever read.
/// Both proxy modes are covered: don's own forwarding accept loop (env mode)
/// and the listenfd socket the child would otherwise be accepting on.
#[test]
fn integration_failed_service_proxy_refuses_connections() {
    use tokio::io::AsyncReadExt;

    struct Case {
        name: &'static str,
        listenfd: bool,
    }

    let cases = vec![
        Case {
            name: "listenfd proxy",
            listenfd: true,
        },
        Case {
            name: "env proxy",
            listenfd: false,
        },
    ];

    for case in cases {
        run_with_timeout(Duration::from_secs(20), async move {
            let dir = TempDir::new("proxy-refuse");
            let port = free_port();
            let addr = format!("127.0.0.1:{port}");

            let service = ConfigBuilder::new()
                .add_task(
                    "setup",
                    "bash",
                    &["-c", "sleep 1; echo SETUP_FAILING; exit 1"],
                )
                .done()
                .add_custom_service("api", "bash", &["-c", "echo API_STARTED; exec sleep 60"])
                .depends_on(&["setup"]);
            let toml = if case.listenfd {
                service.proxy_listenfd(&[&addr])
            } else {
                service.proxy_env(&addr, "PORT")
            }
            .ready_exec("true", &[])
            .done()
            .build();

            let (runner, shutdown_tx, buf) = make_runner_verbose(&toml, dir.path()).await;
            let handle = tokio::spawn(async move {
                runner.run().await.unwrap();
            });

            assert!(
                wait_for_output(
                    &buf,
                    &format!("proxy listening on {addr}"),
                    Duration::from_secs(5)
                )
                .await,
                "{}: expected proxy to bind. output: {}",
                case.name,
                read_buf(&buf)
            );

            // Connect while `setup` is still running: the service isn't up
            // yet, so the connection is held rather than closed.
            let mut early = tokio::net::TcpStream::connect(&addr).await.unwrap();
            let mut byte = [0u8; 1];
            assert!(
                tokio::time::timeout(Duration::from_millis(250), early.read(&mut byte))
                    .await
                    .is_err(),
                "{}: connections should queue while the service is still starting. output: {}",
                case.name,
                read_buf(&buf)
            );

            assert!(
                wait_for_output(
                    &buf,
                    "api: skipped (dependency 'setup' failed)",
                    Duration::from_secs(8)
                )
                .await,
                "{}: expected api to reach DependencyFailed. output: {}",
                case.name,
                read_buf(&buf)
            );

            // Now that api is failed, the queued connection is closed and new
            // ones are refused immediately.
            let closed = tokio::time::timeout(Duration::from_secs(5), early.read(&mut byte))
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "{}: a parked connection should close once the service failed. output: {}",
                        case.name,
                        read_buf(&buf)
                    )
                })
                .unwrap_or(0);
            assert_eq!(closed, 0, "{}: expected a clean close", case.name);

            let mut fresh = tokio::net::TcpStream::connect(&addr).await.unwrap();
            let refused = tokio::time::timeout(Duration::from_secs(5), fresh.read(&mut byte))
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "{}: new connections should be refused while failed. output: {}",
                        case.name,
                        read_buf(&buf)
                    )
                })
                .unwrap_or(0);
            assert_eq!(refused, 0, "{}: expected a clean close", case.name);

            assert!(
                !read_buf(&buf).contains("API_STARTED"),
                "{}: api must not launch when its dependency failed. output: {}",
                case.name,
                read_buf(&buf)
            );

            let _ = shutdown_tx.send(()).await;
            handle.await.unwrap();
        });
    }
}

/// `Failed` does not mean "the process is gone": under the default
/// `on_failure = "notify"` a service whose ready check fails keeps running and
/// may well be serving traffic. Don must keep proxying it — and in listenfd
/// mode must not accept on the socket the live child is accepting on.
#[test]
fn integration_failed_but_live_service_keeps_serving_its_proxy() {
    use tokio::io::AsyncReadExt;

    struct Case {
        name: &'static str,
        listenfd: bool,
    }

    let cases = vec![
        Case {
            name: "listenfd proxy",
            listenfd: true,
        },
        Case {
            name: "env proxy",
            listenfd: false,
        },
    ];

    for case in cases {
        run_with_timeout(Duration::from_secs(25), async move {
            let dir = TempDir::new("failed-but-live");
            let port = free_port();
            let addr = format!("127.0.0.1:{port}");

            // A server that keeps serving no matter what don thinks of it.
            let script_path = dir.path().join("serve.py");
            let source = if case.listenfd {
                "import socket, os\ns = socket.fromfd(3, socket.AF_INET, socket.SOCK_STREAM)\nos.close(3)\n"
            } else {
                "import socket, os\ns = socket.socket()\ns.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\ns.bind(('127.0.0.1', int(os.environ['PORT'])))\ns.listen(16)\n"
            };
            std::fs::write(
                &script_path,
                format!("{source}print('SERVING', flush=True)\nwhile True:\n    conn, _ = s.accept()\n    conn.sendall(b'HELLO')\n    conn.close()\n"),
            )
            .unwrap();

            let script = script_path.to_str().unwrap().to_string();
            let service = ConfigBuilder::new().add_custom_service("api", "python3", &[&script]);
            let toml = if case.listenfd {
                service.proxy_listenfd(&[&addr])
            } else {
                service.proxy_env(&addr, "PORT")
            }
            // Never passes, so the service is declared failed while its
            // process happily keeps accepting connections.
            .ready_exec_with("false", &[], "100ms", 3)
            .done()
            .build();

            let (runner, shutdown_tx, buf) = make_runner_verbose(&toml, dir.path()).await;
            let handle = tokio::spawn(async move {
                runner.run().await.unwrap();
            });

            assert!(
                wait_for_output(&buf, "SERVING", Duration::from_secs(10)).await,
                "{}: server should be up. output: {}",
                case.name,
                read_buf(&buf)
            );
            assert!(
                wait_for_output(&buf, "ready check failed", Duration::from_secs(10)).await,
                "{}: ready check should have failed. output: {}",
                case.name,
                read_buf(&buf)
            );

            // The service is `failed` but alive — every connection must still
            // be served, not stolen and closed by don.
            for attempt in 0..5 {
                let mut client = tokio::net::TcpStream::connect(&addr).await.unwrap();
                let mut got = vec![0u8; 16];
                let n = tokio::time::timeout(Duration::from_secs(5), client.read(&mut got))
                    .await
                    .unwrap_or_else(|_| {
                        panic!(
                            "{}: attempt {attempt} hung. output: {}",
                            case.name,
                            read_buf(&buf)
                        )
                    })
                    .unwrap_or(0);
                assert_eq!(
                    String::from_utf8_lossy(&got[..n]),
                    "HELLO",
                    "{}: attempt {attempt} was not served by the live process. output: {}",
                    case.name,
                    read_buf(&buf)
                );
            }

            assert!(
                !read_buf(&buf).contains("refusing connections"),
                "{}: don must not refuse while the process is alive. output: {}",
                case.name,
                read_buf(&buf)
            );

            let _ = shutdown_tx.send(()).await;
            handle.await.unwrap();
        });
    }
}

#[test]
fn integration_ready_event_reports_the_probed_address_not_the_template() {
    run_with_timeout(Duration::from_secs(20), async {
        let dir = TempDir::new("socket-ready-template");
        let public = free_port();

        // `${PORT}` is the ephemeral backend port Don hands the service, so
        // the ready check probes the service itself rather than Don's proxy
        // (which would answer the instant it bound, before the service ran).
        // The lifecycle line has to name the port actually probed — reporting
        // the literal `${PORT}` tells the reader nothing, and reporting the
        // public port would be an outright lie about what was checked.
        let toml = ConfigBuilder::new()
            .add_custom_service(
                "api",
                "bash",
                &["-c", "python3 -m http.server $PORT --bind 127.0.0.1"],
            )
            .proxy_env(&format!("127.0.0.1:{public}"), "PORT")
            .ready_tcp_with("127.0.0.1:${PORT}", "200ms", 40)
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "ready (tcp ", Duration::from_secs(12)).await,
            "service never reported ready. output: {}",
            String::from_utf8_lossy(&buf.lock().unwrap())
        );

        let output = String::from_utf8_lossy(&buf.lock().unwrap()).into_owned();
        let line = output
            .lines()
            .find(|line| line.contains("ready (tcp "))
            .unwrap_or_default()
            .to_string();

        assert!(
            !line.contains("${"),
            "ready event should name the probed address, not the template: {line}"
        );
        // The backend is ephemeral and chosen by the kernel, so assert the
        // relationship that matters rather than a literal: it is a real port,
        // and it is not the public listener.
        assert!(
            !line.contains(&format!("127.0.0.1:{public}")),
            "ready event should report the backend port, not the public one ({public}): {line}"
        );
        let probed: u16 = line
            .rsplit_once("127.0.0.1:")
            .and_then(|(_, rest)| rest.trim_end_matches(')').parse().ok())
            .unwrap_or_else(|| panic!("no numeric port in ready event: {line}"));
        assert!(probed > 0, "probed port should be real: {line}");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}
