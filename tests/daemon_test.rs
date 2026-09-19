#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
//! End-to-end tests for the broker daemon: a project announces itself, the
//! daemon lists it, and it disappears again on shutdown.
//!
//! The daemon runs in-process against a temp state directory rather than as a
//! spawned binary, so failures point at a stack frame instead of a log file.

mod helpers;

use don::client::ClientError;
use don::config::{Config, LogConfig, Platform};
use don::daemon::{DaemonClient, DaemonOptions, DaemonPaths};
use don::output::OutputManager;
use don::runner::Runner;
use helpers::config::ConfigBuilder;
use helpers::tempdir::TempDir;
use helpers::timeout::run_with_timeout;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

const PLATFORM: Platform = Platform::LinuxX86_64;

/// A daemon running in the background against a temp state directory.
struct TestDaemon {
    paths: DaemonPaths,
    handle: tokio::task::JoinHandle<()>,
}

impl TestDaemon {
    async fn start(state_root: &Path) -> Self {
        let paths = DaemonPaths::with_root(state_root.to_path_buf());
        let options = DaemonOptions {
            paths: paths.clone(),
            web_addr: None,
        };
        let handle = tokio::spawn(async move {
            let report: don::daemon::Reporter = Arc::new(|_line: &str| {});
            let _ = don::daemon::run(options, report).await;
        });

        // Wait for the control socket to appear.
        let deadline = Instant::now() + Duration::from_secs(5);
        while !paths.socket().exists() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(paths.socket().exists(), "daemon socket never appeared");

        Self { paths, handle }
    }

    fn client(&self) -> DaemonClient {
        DaemonClient::new(self.paths.socket())
    }

    async fn stop(self) {
        let _ = self.client().shutdown().await;
        let _ = tokio::time::timeout(Duration::from_secs(5), self.handle).await;
    }
}

/// Build a runner for `base_dir`, optionally announcing itself to `daemon_socket`.
async fn spawn_runner(
    toml: &str,
    base_dir: &Path,
    daemon_socket: Option<std::path::PathBuf>,
) -> (mpsc::Sender<()>, tokio::task::JoinHandle<()>) {
    let config: Config = toml.parse().unwrap();
    config.validate(PLATFORM).unwrap();

    let all_configs: Vec<(&str, &LogConfig)> = config
        .services
        .iter()
        .map(|(n, s)| (n.as_str(), &s.log))
        .collect();

    let output_manager = OutputManager::new(&all_configs, tokio::io::sink())
        .await
        .unwrap();
    let (shutdown_tx, shutdown_rx) = mpsc::channel(2);
    let emitter = output_manager.clone_lifecycle_emitter();
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

    if let Some(socket) = daemon_socket {
        // Exactly what the binary does — registration is the embedder's job,
        // driven off the runner's event stream.
        don::daemon::registration::spawn(
            runner.subscribe(),
            socket,
            runner.base_dir().to_path_buf(),
            Some("dev".to_string()),
            emitter,
        );
    }
    let handle = tokio::spawn(async move {
        let _ = runner.run().await;
    });
    (shutdown_tx, handle)
}

fn keeper_config() -> String {
    ConfigBuilder::new()
        .add_custom_service("keeper", "sleep", &["60"])
        .log("ignore")
        .ready_exec("true", &[])
        .done()
        .build()
}

/// Poll the daemon until it reports `want` projects, or give up.
async fn wait_for_project_count(client: &DaemonClient, want: usize, timeout: Duration) -> usize {
    let deadline = Instant::now() + timeout;
    loop {
        let count = client.projects().await.map(|p| p.len()).unwrap_or(0);
        if count == want || Instant::now() >= deadline {
            return count;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[test]
fn integration_project_registers_and_deregisters() {
    run_with_timeout(Duration::from_secs(20), async {
        let dir = TempDir::new("daemon-register");
        let daemon = TestDaemon::start(&dir.child("state")).await;
        let client = daemon.client();

        assert!(
            client.projects().await.unwrap().is_empty(),
            "a fresh daemon knows about nothing"
        );

        let project_dir = dir.child("proj");
        std::fs::create_dir_all(&project_dir).unwrap();
        let (shutdown_tx, handle) =
            spawn_runner(&keeper_config(), &project_dir, Some(daemon.paths.socket())).await;

        assert_eq!(
            wait_for_project_count(&client, 1, Duration::from_secs(5)).await,
            1,
            "the project should have announced itself"
        );

        let projects = client.projects().await.unwrap();
        let project = &projects[0];
        assert_eq!(project.name, "proj");
        assert_eq!(project.profile, Some("dev".to_string()));
        assert_eq!(project.pid, std::process::id());
        // The socket recorded must be the one the web layer will proxy to.
        assert!(
            project.socket.exists(),
            "recorded socket {} should exist",
            project.socket.display()
        );

        // A clean shutdown withdraws the registration.
        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
        assert_eq!(
            wait_for_project_count(&client, 0, Duration::from_secs(5)).await,
            0,
            "shutting down should remove the project"
        );

        daemon.stop().await;
    });
}

#[test]
fn integration_registering_twice_replaces_rather_than_duplicates() {
    run_with_timeout(Duration::from_secs(30), async {
        let dir = TempDir::new("daemon-reregister");
        let daemon = TestDaemon::start(&dir.child("state")).await;
        let client = daemon.client();

        let project_dir = dir.child("proj");
        std::fs::create_dir_all(&project_dir).unwrap();

        for _ in 0..2 {
            let (shutdown_tx, handle) =
                spawn_runner(&keeper_config(), &project_dir, Some(daemon.paths.socket())).await;
            assert_eq!(
                wait_for_project_count(&client, 1, Duration::from_secs(5)).await,
                1,
                "the same project should occupy exactly one slot"
            );
            let _ = shutdown_tx.send(()).await;
            handle.await.unwrap();
            assert_eq!(
                wait_for_project_count(&client, 0, Duration::from_secs(5)).await,
                0
            );
        }

        daemon.stop().await;
    });
}

#[test]
fn integration_start_is_unaffected_when_no_daemon_is_running() {
    run_with_timeout(Duration::from_secs(20), async {
        let dir = TempDir::new("daemon-absent");
        let project_dir = dir.child("proj");
        std::fs::create_dir_all(&project_dir).unwrap();

        // Point registration at a socket that will never exist. The stack
        // must come up and shut down exactly as if the flag were off — this
        // is the common case, since most users won't install a daemon.
        let ghost = dir.child("state").join("daemon.sock");

        let started = Instant::now();
        let (shutdown_tx, handle) =
            spawn_runner(&keeper_config(), &project_dir, Some(ghost.clone())).await;

        let socket = project_dir.join(".don").join("don.sock");
        let deadline = Instant::now() + Duration::from_secs(5);
        while !socket.exists() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(socket.exists(), "the project API should still come up");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();

        // The registration timeout is 750ms and the deregistration is never
        // awaited, so a whole start+stop cycle against a dead daemon should
        // stay far below a single timeout's worth of delay.
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "an absent daemon must not slow startup or shutdown (took {:?})",
            started.elapsed()
        );
        assert!(!ghost.exists(), "nothing should have created the socket");
    });
}

#[test]
fn integration_daemon_client_reports_missing_daemon_distinctly() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("daemon-missing");
        let client = DaemonClient::new(dir.child("nope").join("daemon.sock"));

        // `don daemon status` relies on this variant to print "not running"
        // instead of an error, so it needs to stay distinguishable.
        assert!(
            matches!(client.info().await, Err(ClientError::NotRunning { .. })),
            "a missing daemon should surface as NotRunning"
        );
        assert!(matches!(
            client.projects().await,
            Err(ClientError::NotRunning { .. })
        ));
    });
}

#[test]
fn integration_daemon_survives_restart_with_registry_intact() {
    run_with_timeout(Duration::from_secs(30), async {
        let dir = TempDir::new("daemon-restart");
        let state = dir.child("state");
        let daemon = TestDaemon::start(&state).await;

        let project_dir = dir.child("proj");
        std::fs::create_dir_all(&project_dir).unwrap();
        let (shutdown_tx, handle) =
            spawn_runner(&keeper_config(), &project_dir, Some(daemon.paths.socket())).await;
        assert_eq!(
            wait_for_project_count(&daemon.client(), 1, Duration::from_secs(5)).await,
            1
        );

        // Restart the daemon while the project keeps running. Stopping the
        // broker must not disturb anything it brokered.
        daemon.stop().await;
        let daemon = TestDaemon::start(&state).await;

        assert_eq!(
            wait_for_project_count(&daemon.client(), 1, Duration::from_secs(5)).await,
            1,
            "a restarted daemon should pick the project back up"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
        daemon.stop().await;
    });
}
