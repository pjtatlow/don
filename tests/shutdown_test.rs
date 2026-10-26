#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
//! Integration tests for Phase 14: shutdown refinement.

mod helpers;

use don::config::{Config, LogConfig, Platform};
use don::output::OutputManager;
use don::runner::{Runner, RunnerEvent, ServiceState};
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

async fn wait_for_output(buf: &Arc<Mutex<Vec<u8>>>, needle: &str, timeout: Duration) -> bool {
    let start = tokio::time::Instant::now();
    loop {
        if read_buf(buf).contains(needle) {
            return true;
        }
        if start.elapsed() > timeout {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn spawn_runner(
    toml: &str,
    base_dir: &std::path::Path,
) -> (
    mpsc::Sender<()>,
    tokio::task::JoinHandle<()>,
    Arc<Mutex<Vec<u8>>>,
) {
    spawn_runner_with(toml, base_dir, |_| {}).await
}

async fn spawn_runner_with<F: FnOnce(&OutputManager)>(
    toml: &str,
    base_dir: &std::path::Path,
    configure: F,
) -> (
    mpsc::Sender<()>,
    tokio::task::JoinHandle<()>,
    Arc<Mutex<Vec<u8>>>,
) {
    let config_path = base_dir.join("don.toml");
    std::fs::write(&config_path, toml).unwrap();

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
    let output_manager = OutputManager::new(&all_configs, writer).await.unwrap();
    configure(&output_manager);
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

    let handle = tokio::spawn(async move {
        let _ = runner.run().await;
    });

    (shutdown_tx, handle, buf)
}

async fn make_runner(
    toml: &str,
    base_dir: &std::path::Path,
) -> (Runner, mpsc::Sender<()>, Arc<Mutex<Vec<u8>>>) {
    let config_path = base_dir.join("don.toml");
    std::fs::write(&config_path, toml).unwrap();

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

// --- Tests ---

/// Lifecycle events ("send SIGTERM…", "stopping…") share the stdout sink
/// with regular service output. If a noisy service spams during shutdown
/// and the channel were bounded, lifecycle events sent via `try_send`
/// would silently drop and the user would see "shutting down gracefully"
/// followed by nothing but spam. Regression test for the bounded-channel
/// design that produced exactly that symptom in the redo monorepo
/// (kafka-relay flooding "can't connect" after kafka shutdown).
#[test]
fn shutdown_lifecycle_events_survive_service_spam() {
    use std::os::unix::fs::PermissionsExt;

    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("shutdown-spam");
        // A service that emits a tight burst of lines on shutdown — exactly
        // the load shape that drowns lifecycle events under a bounded sink.
        let script_path = dir.path().join("spam.sh");
        std::fs::write(
            &script_path,
            "#!/bin/sh\n\
             trap 'i=1; while [ $i -le 5000 ]; do echo spam line $i; i=$((i+1)); done; exit 0' TERM\n\
             while true; do sleep 1; done\n",
        )
        .unwrap();
        std::fs::set_permissions(&script_path, PermissionsExt::from_mode(0o755)).unwrap();

        let toml = ConfigBuilder::new()
            .add_custom_service("noisy", &script_path.display().to_string(), &[])
            .log("stdout")
            .ready_exec("true", &[])
            .shutdown("SIGTERM", "5s")
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();

        let output = read_buf(&buf);
        assert!(
            output.contains("noisy: send SIGTERM to pgid"),
            "lifecycle SIGTERM event should not be dropped under spam. output: {output:?}"
        );
        assert!(
            output.contains("noisy: stopping"),
            "lifecycle 'stopping' event should not be dropped. output: {output:?}"
        );
        assert!(
            output.contains("shutdown complete"),
            "shutdown complete should appear after spam. output: {output:?}"
        );
        // Service output itself should also show up — we didn't trade
        // control-plane reliability for service-output drops.
        assert!(
            output.contains("spam line 5000"),
            "noisy service's own output should still be delivered. output: {output:?}"
        );
    });
}

/// Verify services stop in reverse dependency order: C (depends on B, which
/// depends on A) stops first, then B, then A.
#[test]
fn shutdown_reverse_dependency_order() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("shutdown-order");
        let toml = ConfigBuilder::new()
            .add_custom_service("a", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_custom_service("b", "sleep", &["60"])
            .log("ignore")
            .depends_on(&["a"])
            .ready_exec("true", &[])
            .done()
            .add_custom_service("c", "sleep", &["60"])
            .log("ignore")
            .depends_on(&["b"])
            .ready_exec("true", &[])
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        // Trigger graceful shutdown.
        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();

        let output = read_buf(&buf);

        // Find the positions of "X: stopping..." messages to verify order.
        let c_stop = output.find("c: stopping...");
        let b_stop = output.find("b: stopping...");
        let a_stop = output.find("a: stopping...");

        assert!(c_stop.is_some(), "c should be stopped. output: {output}");
        assert!(b_stop.is_some(), "b should be stopped. output: {output}");
        assert!(a_stop.is_some(), "a should be stopped. output: {output}");

        // C (deepest dependent) must stop before B, B before A.
        assert!(
            c_stop.unwrap() < b_stop.unwrap(),
            "c should stop before b. output: {output}"
        );
        assert!(
            b_stop.unwrap() < a_stop.unwrap(),
            "b should stop before a. output: {output}"
        );

        assert!(output.contains("shutdown complete"), "output: {output}");
    });
}

#[test]
fn shutdown_broadcasts_stopping_before_stopped() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("shutdown-state-events");
        let toml = ConfigBuilder::new()
            .add_custom_service("api", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let mut events = runner.subscribe();
        let handle = tokio::spawn(async move {
            let _ = runner.run().await;
        });
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        let _ = shutdown_tx.send(()).await;

        let mut shutdown_states = Vec::new();
        while shutdown_states.len() < 2 {
            let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
                .await
                .unwrap()
                .unwrap();
            if let RunnerEvent::ServiceStateChanged { name, state, .. } = event
                && name == "api"
                && matches!(state, ServiceState::Stopping | ServiceState::Stopped)
            {
                shutdown_states.push(state);
            }
        }

        handle.await.unwrap();
        assert_eq!(
            shutdown_states,
            vec![ServiceState::Stopping, ServiceState::Stopped]
        );
    });
}

/// Verify PID files and socket are cleaned up after shutdown.
#[test]
fn shutdown_cleans_up_state() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("shutdown-cleanup");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        // Verify state files exist before shutdown.
        let don_dir = dir.path().join(".don");
        assert!(don_dir.join("don.pid").exists(), "don.pid should exist");
        // Socket exists (API server).
        assert!(don_dir.join("don.sock").exists(), "don.sock should exist");

        // Trigger shutdown.
        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();

        // After shutdown, PID file and socket should be gone.
        assert!(
            !don_dir.join("don.sock").exists(),
            "don.sock should be cleaned up"
        );
        // don.pid is released (flock) and removed by PidFile::Drop.
        // The file may or may not exist (depends on Drop ordering), but the
        // flock is definitely released, which is what matters.

        // Service PID files should be gone.
        let pids_dir = don_dir.join("pids");
        if pids_dir.exists() {
            let entries: Vec<_> = std::fs::read_dir(&pids_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .collect();
            assert!(
                entries.is_empty(),
                "pid files should be cleaned up: {entries:?}"
            );
        }
    });
}

/// Verify a running task is killed during shutdown.
#[test]
fn shutdown_kills_running_task() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("shutdown-task");
        // A service that starts immediately, plus a task that takes a long time.
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_task("slow", "sleep", &["300"])
            .log("ignore")
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&toml, dir.path()).await;
        // Wait for the service to be ready (task may still be running).
        assert!(wait_for_output(&buf, "keeper: ready", Duration::from_secs(5)).await);
        // Give the task a moment to start.
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Trigger shutdown.
        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();

        let output = read_buf(&buf);
        // The task's own supervisor signals it and says so. There is no
        // aggregate "killing N tasks" line any more — that was the runner
        // speaking on everyone's behalf, which is exactly what teardown
        // stopped doing.
        assert!(
            output.contains("slow: send SIGKILL to task pgid"),
            "expected the task's supervisor to signal its own run. output: {output}"
        );
        assert!(output.contains("shutdown complete"), "output: {output}");
    });
}

/// Verify that shutdown completes with the graceful message.
#[test]
fn shutdown_graceful_message() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("shutdown-graceful-msg");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();

        let output = read_buf(&buf);
        assert!(
            output.contains("shutting down gracefully"),
            "expected graceful message. output: {output}"
        );
        assert!(
            output.contains("Ctrl+C again to force"),
            "expected force hint. output: {output}"
        );
    });
}

/// Verify shutdown interrupts a long startup-time service build instead of
/// waiting for the build command to finish.
#[test]
fn shutdown_interrupts_startup_build() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("shutdown-startup-build");
        let toml = ConfigBuilder::new()
            .add_custom_service("builder", "sleep", &["60"])
            .build_cmd("sleep", &["300"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&toml, dir.path()).await;
        assert!(
            wait_for_output(
                &buf,
                "builder: running sleep build...",
                Duration::from_secs(5)
            )
            .await,
            "startup build never started. output: {}",
            read_buf(&buf)
        );

        let start = std::time::Instant::now();
        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
        let elapsed = start.elapsed();

        let output = read_buf(&buf);
        assert!(
            output.contains("cancelled by shutdown"),
            "expected cancellation message. output: {output}"
        );
        assert!(output.contains("shutdown complete"), "output: {output}");
        assert!(
            elapsed < Duration::from_secs(5),
            "shutdown during startup build took too long ({elapsed:?})"
        );
    });
}

#[test]
fn shutdown_interrupts_manual_stop_worker() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("shutdown-manual-stop");
        let toml = ConfigBuilder::new()
            .add_custom_service(
                "stubborn",
                "bash",
                &["-c", "trap '' TERM; echo started; sleep 60"],
            )
            .log("ignore")
            .ready_exec("true", &[])
            .shutdown("SIGTERM", "10s")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let control = runner.process_control();
        let handle = tokio::spawn(async move {
            let _ = runner.run().await;
        });

        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        // Detached: this stop is deliberately slow, and the point of the test
        // is what a shutdown does while it is still in flight.
        let stop = tokio::spawn({
            let control = control.clone();
            async move { control.stop("stubborn").await }
        });

        assert!(wait_for_output(&buf, "stopping... (requested)", Duration::from_secs(2)).await);

        let start = std::time::Instant::now();
        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
        let elapsed = start.elapsed();
        let _ = stop.await;

        let output = read_buf(&buf);
        assert!(output.contains("shutdown complete"), "output: {output}");
        assert!(
            elapsed < Duration::from_secs(3),
            "shutdown during manual stop took too long ({elapsed:?})"
        );
    });
}

#[test]
fn shutdown_interrupts_manual_restart_worker() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("shutdown-manual-restart");
        let toml = ConfigBuilder::new()
            .add_custom_service(
                "stubborn",
                "bash",
                &["-c", "trap '' TERM; echo started; sleep 60"],
            )
            .log("ignore")
            .ready_exec("true", &[])
            .shutdown("SIGTERM", "10s")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let control = runner.process_control();
        let handle = tokio::spawn(async move {
            let _ = runner.run().await;
        });

        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        // Detached: this restart's stop half is deliberately slow, and the
        // test is about what shutdown does while it is still in flight.
        let restart = tokio::spawn({
            let control = control.clone();
            async move { control.restart("stubborn").await }
        });

        assert!(
            wait_for_output(
                &buf,
                "stopping... (requested restart)",
                Duration::from_secs(2),
            )
            .await
        );

        let start = std::time::Instant::now();
        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
        let elapsed = start.elapsed();
        let _ = restart.await;

        let output = read_buf(&buf);
        assert!(output.contains("shutdown complete"), "output: {output}");
        assert!(
            elapsed < Duration::from_secs(3),
            "shutdown during manual restart took too long ({elapsed:?})"
        );
    });
}

#[test]
fn shutdown_interrupts_manual_start_worker() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("shutdown-manual-start");
        let listen_addr = format!("127.0.0.1:{}", free_port());
        let toml = ConfigBuilder::new()
            .add_custom_service("lazy-builder", "sleep", &["60"])
            .build_cmd("sleep", &["300"])
            .proxy_listenfd(&[&listen_addr])
            .lazy(true)
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let control = runner.process_control();
        let handle = tokio::spawn(async move {
            let _ = runner.run().await;
        });

        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        // Detached: the reply only lands once the start has settled, and this
        // test is waiting for the build it kicks off.
        tokio::spawn({
            let control = control.clone();
            async move {
                let _ = control.start("lazy-builder").await;
            }
        });

        assert!(
            wait_for_output(
                &buf,
                "lazy-builder: running sleep build...",
                Duration::from_secs(5),
            )
            .await,
            "manual start build never started. output: {}",
            read_buf(&buf)
        );

        let start = std::time::Instant::now();
        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
        let elapsed = start.elapsed();

        let output = read_buf(&buf);
        assert!(output.contains("cancelled by shutdown"), "output: {output}");
        assert!(output.contains("shutdown complete"), "output: {output}");
        assert!(
            elapsed < Duration::from_secs(5),
            "shutdown during manual start took too long ({elapsed:?})"
        );
    });
}

#[test]
fn shutdown_interrupts_rebuild_worker() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("shutdown-rebuild-worker");
        let toml = ConfigBuilder::new()
            .add_custom_service("builder", "sleep", &["60"])
            .build_cmd(
                "bash",
                &[
                    "-c",
                    "if [ -f slow-build ]; then sleep 300; else exit 0; fi",
                ],
            )
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let control = runner.process_control();
        let handle = tokio::spawn(async move {
            let _ = runner.run().await;
        });

        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        std::fs::write(dir.path().join("slow-build"), "1").unwrap();

        // A rebuild by name — the same cycle a watched file starts, minus
        // waiting for the debounce. Detached: the reply lands when the build
        // is accepted, and this test is watching what the build then does.
        tokio::spawn({
            let control = control.clone();
            async move {
                let _ = control.hard_restart("builder").await;
            }
        });

        assert!(
            wait_for_output(
                &buf,
                "builder: rebuilding (file changed)",
                Duration::from_secs(5),
            )
            .await,
            "rebuild worker never started. output: {}",
            read_buf(&buf)
        );

        let start = std::time::Instant::now();
        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
        let elapsed = start.elapsed();

        let output = read_buf(&buf);
        assert!(
            output.contains("rebuild cancelled by shutdown"),
            "output: {output}"
        );
        assert!(output.contains("shutdown complete"), "output: {output}");
        assert!(
            elapsed < Duration::from_secs(5),
            "shutdown during rebuild took too long ({elapsed:?})"
        );
    });
}

/// Verify per-service shutdown.timeout is respected — a service ignoring
/// SIGTERM gets SIGKILL after the configured timeout.
#[test]
fn shutdown_timeout_escalates_to_sigkill() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("shutdown-timeout");
        // Service that traps and ignores SIGTERM.
        let toml = ConfigBuilder::new()
            .add_custom_service(
                "stubborn",
                "bash",
                &["-c", "trap '' TERM; echo started; sleep 60"],
            )
            .log("ignore")
            .ready_exec("true", &[])
            .shutdown("SIGTERM", "1s")
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        let start = std::time::Instant::now();
        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
        let elapsed = start.elapsed();

        let output = read_buf(&buf);
        assert!(output.contains("shutdown complete"), "output: {output}");
        assert!(
            output.contains("stubborn: send SIGTERM to pgid"),
            "expected SIGTERM send log. output: {output}"
        );
        assert!(
            output.contains("stubborn: send SIGKILL to pgid"),
            "expected SIGKILL send log. output: {output}"
        );

        // Should take ~1s (the timeout) + a bit for SIGKILL, not the full 10s default.
        assert!(
            elapsed < Duration::from_secs(5),
            "shutdown took too long ({elapsed:?}) — timeout may not be respected"
        );
        assert!(
            elapsed >= Duration::from_millis(800),
            "shutdown was too fast ({elapsed:?}) — SIGTERM should wait ~1s"
        );
    });
}

#[test]
fn shutdown_graceful_disabled_sends_sigkill_immediately() {
    run_with_timeout(Duration::from_secs(15), async {
        struct Case {
            name: &'static str,
            toml: String,
        }

        let cases = vec![
            Case {
                name: "global",
                toml: ConfigBuilder::new()
                    .raw(
                        r#"
[shutdown]
graceful = false
"#,
                    )
                    .add_custom_service(
                        "stubborn",
                        "bash",
                        &[
                            "-c",
                            "trap 'echo got-term; sleep 2; exit 0' TERM; echo started; sleep 60",
                        ],
                    )
                    .log("stdout")
                    .ready_exec("true", &[])
                    .shutdown("SIGTERM", "5s")
                    .done()
                    .build(),
            },
            Case {
                name: "per-service",
                toml: ConfigBuilder::new()
                    .add_custom_service(
                        "stubborn",
                        "bash",
                        &[
                            "-c",
                            "trap 'echo got-term; sleep 2; exit 0' TERM; echo started; sleep 60",
                        ],
                    )
                    .log("stdout")
                    .ready_exec("true", &[])
                    .shutdown("SIGTERM", "5s")
                    .graceful_shutdown(false)
                    .done()
                    .build(),
            },
        ];

        for case in cases {
            let dir = TempDir::new(&format!("shutdown-no-graceful-{}", case.name));
            let (shutdown_tx, handle, buf) = spawn_runner(&case.toml, dir.path()).await;
            assert!(
                wait_for_output(&buf, "all services running", Duration::from_secs(5)).await,
                "{}: service did not start",
                case.name
            );

            let start = std::time::Instant::now();
            let _ = shutdown_tx.send(()).await;
            handle.await.unwrap();
            let elapsed = start.elapsed();

            let output = read_buf(&buf);
            assert!(
                output.contains("shutdown complete"),
                "{}: output: {output}",
                case.name
            );
            assert!(
                output.contains("stubborn: send SIGKILL to pgid"),
                "{}: expected immediate SIGKILL. output: {output}",
                case.name
            );
            assert!(
                !output.contains("stubborn: send SIGTERM to pgid"),
                "{}: graceful SIGTERM should be skipped. output: {output}",
                case.name
            );
            assert!(
                !output.contains("got-term"),
                "{}: process should not receive SIGTERM. output: {output}",
                case.name
            );
            assert!(
                elapsed < Duration::from_millis(900),
                "{}: shutdown should not wait for graceful timeout ({elapsed:?})",
                case.name
            );
        }
    });
}

#[test]
fn shutdown_signals_unhealthy_services() {
    use std::os::unix::fs::PermissionsExt;

    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("shutdown-unhealthy-service");
        let sentinel = dir.path().join("healthy.flag");
        std::fs::write(&sentinel, "ok").unwrap();

        let check_script = dir.path().join("check.sh");
        std::fs::write(
            &check_script,
            format!(
                "#!/bin/sh\n[ -f {} ] && exit 0 || exit 1\n",
                sentinel.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&check_script, PermissionsExt::from_mode(0o755)).unwrap();

        let toml = format!(
            r#"
min_version = "0.0.0"
[services.svc]
run.cmd = "sleep"
run.args = ["300"]
log = "ignore"

[services.svc.ready]
exec.cmd = "{}"
interval = "100ms"
retries = 20
monitor = true
monitor_interval = "100ms"
unhealthy_after = 1
"#,
            check_script.display()
        );

        let (shutdown_tx, handle, buf) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_output(&buf, "svc: ready", Duration::from_secs(5)).await);

        std::fs::remove_file(&sentinel).unwrap();
        assert!(wait_for_output(&buf, "svc: unhealthy", Duration::from_secs(5)).await);

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();

        let output = read_buf(&buf);
        assert!(
            output.contains("svc: send SIGTERM to pgid"),
            "expected unhealthy service to be signalled. output: {output}"
        );
        assert!(output.contains("shutdown complete"), "output: {output}");
    });
}

#[test]
fn shutdown_waits_for_process_group_and_drains_logs() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("shutdown-pgroup-logs");
        let script_path = dir.child("process_tree.py");
        std::fs::write(
            &script_path,
            r#"
import os
import signal
import sys
import time

pid = os.fork()
if pid == 0:
    signal.signal(signal.SIGHUP, signal.SIG_IGN)

    def child_term(_signum, _frame):
        print("child-term", flush=True)
        time.sleep(1)
        print("child-done", flush=True)
        sys.exit(0)

    signal.signal(signal.SIGTERM, child_term)
    while True:
        time.sleep(10)


def parent_term(_signum, _frame):
    print("parent-term", flush=True)
    while True:
        try:
            os.waitpid(pid, 0)
            break
        except InterruptedError:
            continue
    sys.exit(0)


signal.signal(signal.SIGTERM, parent_term)
print("started", flush=True)
while True:
    time.sleep(10)
"#,
        )
        .unwrap();
        let toml = ConfigBuilder::new()
            .add_custom_service("tree", "python3", &[script_path.to_str().unwrap()])
            .log("stdout")
            .ready_exec("true", &[])
            .shutdown("SIGTERM", "3s")
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);
        assert!(wait_for_output(&buf, "started", Duration::from_secs(5)).await);

        let start = std::time::Instant::now();
        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
        let elapsed = start.elapsed();

        let output = read_buf(&buf);
        assert!(output.contains("parent-term"), "output: {output}");
        assert!(output.contains("child-term"), "output: {output}");
        assert!(output.contains("child-done"), "output: {output}");
        assert!(output.contains("shutdown complete"), "output: {output}");
        let child_done = output.find("child-done").unwrap();
        let shutdown_complete = output.find("shutdown complete").unwrap();
        assert!(
            child_done < shutdown_complete,
            "shutdown complete should be emitted after final service logs. output: {output}"
        );
        assert!(
            elapsed >= Duration::from_millis(800),
            "shutdown returned before descendant cleanup finished ({elapsed:?})"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "shutdown took too long ({elapsed:?})"
        );
    });
}
