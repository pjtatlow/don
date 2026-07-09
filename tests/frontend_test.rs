#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
//! Tests for the remote `don tui` frontend wiring: the daemon's `/snapshot`,
//! `/events`, and `/logstream` endpoints, and the `TuiBridge` that adapts them
//! into the channels `run_tui` consumes. The runner is spawned in-process (no
//! second `don` process); the client talks to its `.don/don.sock`.

mod helpers;

use don::client::Client;
use don::client::bridge::TuiBridge;
use don::config::{Config, LogConfig, Platform};
use don::output::OutputManager;
use don::runner::{ItemStatus, Runner, RunnerEvent, ServiceState, TerminalCoordinator};
use helpers::config::ConfigBuilder;
use helpers::tempdir::TempDir;
use helpers::timeout::run_with_timeout;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

const PLATFORM: Platform = Platform::LinuxX86_64;

async fn spawn_runner(toml: &str, base_dir: &Path) -> (PathBuf, mpsc::Sender<()>, JoinHandle<()>) {
    let config: Config = toml.parse().unwrap();
    config.validate(PLATFORM).unwrap();

    let all_configs: Vec<(&str, &LogConfig)> = config
        .services
        .iter()
        .map(|(n, s)| (n.as_str(), &s.log))
        .chain(config.tasks.iter().map(|(n, t)| (n.as_str(), &t.log)))
        .collect();

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
        let _ = runner.run().await;
    });
    (socket_path, shutdown_tx, handle)
}

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

/// Poll the snapshot until `pred` holds or the timeout elapses.
async fn wait_for_snapshot<F>(client: &Client, timeout: Duration, pred: F) -> bool
where
    F: Fn(&don::runner::TuiSnapshot) -> bool,
{
    let start = tokio::time::Instant::now();
    loop {
        if let Ok(snap) = client.snapshot().await
            && pred(&snap)
        {
            return true;
        }
        if start.elapsed() > timeout {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn two_service_config() -> String {
    ConfigBuilder::new()
        .add_custom_service("api", "sleep", &["60"])
        .ready_exec("true", &[])
        .done()
        .add_custom_service("worker", "sleep", &["60"])
        .ready_exec("true", &[])
        .done()
        .add_task("migrate", "true", &[])
        .done()
        .build()
}

#[test]
fn snapshot_reports_active_items_and_ready_state() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("frontend-snapshot");
        let (socket, shutdown_tx, handle) = spawn_runner(&two_service_config(), dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        let client = Client::with_socket_path(socket);

        // Both services should reach ready (ready_exec `true` passes immediately).
        let ready = wait_for_snapshot(&client, Duration::from_secs(5), |snap| {
            snap.statuses.iter().filter(|s| {
                matches!(s, ItemStatus::Service { state: ServiceState::Ready, .. })
            }).count()
                == 2
        })
        .await;
        assert!(ready, "both services should report ready in the snapshot");

        let snap = client.snapshot().await.unwrap();
        let mut services = snap.service_names.clone();
        services.sort();
        assert_eq!(services, vec!["api".to_string(), "worker".to_string()]);
        assert_eq!(snap.task_names, vec!["migrate".to_string()]);
        assert!(!snap.verbose);

        let _ = shutdown_tx.send(()).await;
        let _ = handle.await;
    });
}

#[test]
fn events_stream_delivers_state_changes_on_restart() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("frontend-events");
        let (socket, shutdown_tx, handle) = spawn_runner(&two_service_config(), dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        let client = Client::with_socket_path(socket.clone());
        assert!(
            wait_for_snapshot(&client, Duration::from_secs(5), |snap| {
                snap.statuses.iter().any(|s| matches!(
                    s,
                    ItemStatus::Service { name, state: ServiceState::Ready, .. } if name == "api"
                ))
            })
            .await
        );

        // Collect events in a background task.
        let (ev_tx, mut ev_rx) = mpsc::unbounded_channel::<RunnerEvent>();
        let stream = tokio::spawn(async move {
            let c = Client::with_socket_path(socket);
            let _ = c.stream_events(move |e| {
                let _ = ev_tx.send(e);
            })
            .await;
        });

        // Give the stream a moment to connect, then restart `api`.
        tokio::time::sleep(Duration::from_millis(200)).await;
        client.restart("api").await.unwrap();

        // Expect to see api transition through stopping/…/ready.
        let mut saw_api_change = false;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(500), ev_rx.recv()).await {
                Ok(Some(RunnerEvent::ServiceStateChanged { name, .. })) if name == "api" => {
                    saw_api_change = true;
                    break;
                }
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(_) => {}
            }
        }
        assert!(saw_api_change, "should observe an api state change over /events");

        stream.abort();
        let _ = shutdown_tx.send(()).await;
        let _ = handle.await;
    });
}

#[test]
fn logstream_delivers_formatted_service_output() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("frontend-logstream");
        let toml = ConfigBuilder::new()
            .add_custom_service(
                "talker",
                "bash",
                &["-c", "while true; do echo HELLO_FRONTEND; sleep 0.3; done"],
            )
            .ready_exec("true", &[])
            .done()
            .build();
        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);

        let (line_tx, mut line_rx) = mpsc::unbounded_channel::<(String, String)>();
        let socket_for_stream = socket.clone();
        let stream = tokio::spawn(async move {
            let c = Client::with_socket_path(socket_for_stream);
            let _ = c.stream_logs(move |line| {
                let text = String::from_utf8_lossy(&line.bytes).into_owned();
                let _ = line_tx.send((line.name, text));
            })
            .await;
        });

        let mut saw_hello = false;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(6);
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(500), line_rx.recv()).await {
                Ok(Some((_name, text))) if text.contains("HELLO_FRONTEND") => {
                    saw_hello = true;
                    break;
                }
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(_) => {}
            }
        }
        assert!(saw_hello, "should receive the talker's HELLO_FRONTEND line over /logstream");

        stream.abort();
        let _ = shutdown_tx.send(()).await;
        let _ = handle.await;
    });
}

#[test]
fn logstream_replays_history_to_a_late_connecting_client() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("frontend-backfill");
        let toml = ConfigBuilder::new()
            .add_custom_service(
                "chatty",
                "bash",
                &["-c", "for i in 1 2 3 4 5; do echo BACKFILL-$i; sleep 0.05; done; sleep 60"],
            )
            .ready_exec("true", &[])
            .done()
            .build();
        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);

        // Let the service emit all five lines BEFORE the client subscribes.
        tokio::time::sleep(Duration::from_millis(800)).await;

        // Connect to /logstream after the fact — backfill should replay history.
        let (line_tx, mut line_rx) = mpsc::unbounded_channel::<String>();
        let socket_for_stream = socket.clone();
        let stream = tokio::spawn(async move {
            let c = Client::with_socket_path(socket_for_stream);
            let _ = c
                .stream_logs(move |line| {
                    let _ = line_tx.send(String::from_utf8_lossy(&line.bytes).into_owned());
                })
                .await;
        });

        let mut seen: Vec<&str> = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(300), line_rx.recv()).await {
                Ok(Some(text)) => {
                    for tag in ["BACKFILL-1", "BACKFILL-2", "BACKFILL-3", "BACKFILL-4", "BACKFILL-5"] {
                        if text.contains(tag) && !seen.contains(&tag) {
                            seen.push(tag);
                        }
                    }
                    if seen.len() == 5 {
                        break;
                    }
                }
                Ok(None) => break,
                Err(_) => {}
            }
        }
        assert_eq!(
            seen.len(),
            5,
            "late-connecting frontend should see all 5 backfilled lines; got {seen:?}"
        );

        stream.abort();
        let _ = shutdown_tx.send(()).await;
        let _ = handle.await;
    });
}

#[test]
fn bridge_connect_seeds_snapshot_and_state() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("frontend-bridge");
        let (socket, shutdown_tx, handle) = spawn_runner(&two_service_config(), dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        let client = Client::with_socket_path(socket);
        assert!(
            wait_for_snapshot(&client, Duration::from_secs(5), |snap| {
                snap.statuses.iter().filter(|s| matches!(
                    s,
                    ItemStatus::Service { state: ServiceState::Ready, .. }
                )).count()
                    == 2
            })
            .await
        );

        let mut bridge = TuiBridge::connect(dir.path()).await.unwrap();
        // Snapshot carried through.
        assert_eq!(bridge.snapshot.service_names.len(), 2);
        // Initial state was replayed as synthetic events on the events channel.
        let mut seeded = Vec::new();
        while let Ok(ev) = bridge.events_rx.try_recv() {
            seeded.push(ev);
        }
        let service_events = seeded
            .iter()
            .filter(|e| matches!(e, RunnerEvent::ServiceStateChanged { .. }))
            .count();
        assert!(
            service_events >= 2,
            "bridge should replay current state as synthetic events, got {service_events}"
        );

        drop(bridge.guard);
        let _ = shutdown_tx.send(()).await;
        let _ = handle.await;
    });
}
