#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod helpers;

use don::config::{Config, LogConfig, Platform};
use don::output::OutputManager;
use don::runner::Runner;
use helpers::config::{ConfigBuilder, parse_config};
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
    let config: Config = parse_config(toml);
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
    let output_manager = OutputManager::new(&all_configs, writer).await.unwrap();
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
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// --- Go preset: builds and runs a Go binary ---

#[test]
fn go_preset_builds_and_runs() {
    run_with_timeout(Duration::from_secs(30), async {
        let dir = TempDir::new("go-preset");

        // Create a minimal Go project.
        std::fs::write(
            dir.path().join("go.mod"),
            "module example.com/test\n\ngo 1.21\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("main.go"),
            r#"package main

import "fmt"

func main() {
    fmt.Println("hello from go preset")
    // Block forever so the service stays running.
    select {}
}
"#,
        )
        .unwrap();

        let toml = ConfigBuilder::new()
            .add_go_service("api", ".")
            // Disable VCS stamping for the build subprocess. Without this,
            // `go build` walks up the temp dir looking for `.git` and can
            // trip over an unrelated ancestor repo (e.g. a stray `/tmp/.git`
            // on the host) with `error obtaining VCS status: exit 128`.
            // The flag has no effect on Don's behavior — it just makes the
            // test independent of whatever VCS state the temp parent has.
            .env("GOFLAGS", "-buildvcs=false")
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Should see the build succeed and the output from the Go program.
        assert!(
            wait_for_output(&buf, "go build succeeded", Duration::from_secs(20)).await,
            "expected go build to succeed. output: {}",
            read_buf(&buf)
        );
        assert!(
            wait_for_output(&buf, "hello from go preset", Duration::from_secs(5)).await,
            "expected output from go binary. output: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Go preset with build flags ---

#[test]
fn go_preset_with_flags() {
    run_with_timeout(Duration::from_secs(30), async {
        let dir = TempDir::new("go-flags");

        std::fs::write(
            dir.path().join("go.mod"),
            "module example.com/test\n\ngo 1.21\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("main.go"),
            r#"package main

import "fmt"

var version = "unknown"

func main() {
    fmt.Printf("version=%s\n", version)
    select {}
}
"#,
        )
        .unwrap();

        // Use ldflags to inject a version string. GOFLAGS disables VCS
        // stamping so the test isn't subject to the host's temp-parent
        // VCS state — see the matching note in `go_preset_builds_and_runs`.
        let toml = r#"
[services.api]
go.package = "."
go.ldflags = "-X main.version=1.2.3"
env.GOFLAGS = "-buildvcs=false"
ready.exec.cmd = "true"
"#
        .to_string();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "version=1.2.3", Duration::from_secs(20)).await,
            "expected version=1.2.3 in output. output: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Rust preset: builds and runs a Rust binary ---
// This test compiles a real Rust project, so it's slow.

#[test]
fn rust_preset_builds_and_runs() {
    run_with_timeout(Duration::from_secs(120), async {
        let dir = TempDir::new("rust-preset");

        // Create a minimal Cargo project.
        std::fs::write(
            dir.path().join("Cargo.toml"),
            r#"[package]
name = "test-app"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "test-app"
path = "main.rs"
"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("main.rs"),
            r#"fn main() {
    println!("hello from rust preset");
    loop { std::thread::sleep(std::time::Duration::from_secs(60)); }
}
"#,
        )
        .unwrap();

        let toml = ConfigBuilder::new()
            .add_rust_service("app", "test-app")
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "cargo build succeeded", Duration::from_secs(90)).await,
            "expected cargo build to succeed. output: {}",
            read_buf(&buf)
        );
        assert!(
            wait_for_output(&buf, "hello from rust preset", Duration::from_secs(10)).await,
            "expected output from rust binary. output: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}
