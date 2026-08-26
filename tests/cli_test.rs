#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
//! CLI subcommand tests — exercise the `don` binary against an in-process
//! runner. The runner is spawned in a tokio task (so we don't fork two
//! `don` processes); the CLI binary connects to its `.don/don.sock`.

mod helpers;

use don::client::Client;
use don::config::{Config, LogConfig, Platform};
use don::output::OutputManager;
use don::runner::Runner;
use helpers::config::ConfigBuilder;
use helpers::tempdir::TempDir;
use helpers::timeout::run_with_timeout;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

const PLATFORM: Platform = Platform::LinuxX86_64;

/// Spawn an in-process runner for the given TOML. Returns socket path,
/// shutdown sender, and the join handle. Mirrors the pattern in
/// tests/server_test.rs.
async fn spawn_runner(toml: &str, base_dir: &Path) -> (PathBuf, mpsc::Sender<()>, JoinHandle<()>) {
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

/// Build a minimal config with one service and (optionally) one task.
fn keeper_config() -> String {
    ConfigBuilder::new()
        .add_custom_service("keeper", "sleep", &["60"])
        .log("ignore")
        .ready_exec("true", &[])
        .done()
        .build()
}

/// Run the `don` binary with args and return (exit_code, stdout, stderr).
fn run_cli(config_path: &Path, extra: &[&str]) -> (i32, String, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_don"));
    cmd.arg("--config").arg(config_path);
    for a in extra {
        cmd.arg(a);
    }
    let output = cmd.output().unwrap();
    (
        output.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

// --- tests ---

#[test]
fn cli_tui_without_runner_errors_actionably() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("cli-tui-no-runner");
        let toml = keeper_config();
        let config_path = dir.path().join("don.toml");
        std::fs::write(&config_path, &toml).unwrap();

        // No runner: the error must point at `don start` (or, headless, at
        // the terminal requirement) *before* the terminal is touched.
        let (code, _stdout, stderr) =
            tokio::task::spawn_blocking(move || run_cli(&config_path, &["attach"]))
                .await
                .unwrap();
        assert_ne!(code, 0, "attaching with no runner must fail");
        assert!(
            stderr.contains("don start") || stderr.contains("needs a terminal"),
            "error should be actionable; got: {stderr}"
        );
    });
}

#[test]
fn cli_status_against_running_daemon() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("cli-status");
        let toml = keeper_config();
        let config_path = dir.path().join("don.toml");
        std::fs::write(&config_path, &toml).unwrap();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        // Give service time to become ready.
        tokio::time::sleep(Duration::from_millis(400)).await;

        let (code, stdout, stderr) =
            tokio::task::spawn_blocking(move || run_cli(&config_path, &["status"]))
                .await
                .unwrap();
        assert_eq!(code, 0, "stderr: {stderr}");
        assert!(stdout.contains("KIND"), "stdout: {stdout}");
        assert!(stdout.contains("keeper"), "stdout: {stdout}");
        assert!(stdout.contains("service"), "stdout: {stdout}");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn cli_status_json_against_running_daemon() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("cli-status-json");
        let toml = keeper_config();
        let config_path = dir.path().join("don.toml");
        std::fs::write(&config_path, &toml).unwrap();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        // Give the service time to pass its ready check (ready_exec "true").
        tokio::time::sleep(Duration::from_millis(400)).await;

        let (code, stdout, stderr) =
            tokio::task::spawn_blocking(move || run_cli(&config_path, &["status", "--json"]))
                .await
                .unwrap();
        assert_eq!(code, 0, "stderr: {stderr}");

        // Output must be valid JSON (no human table noise).
        let parsed: serde_json::Value =
            serde_json::from_str(&stdout).unwrap_or_else(|e| panic!("bad json {e}: {stdout}"));

        // The keeper service has a passing ready check, so the stack is ready.
        assert_eq!(parsed["ready"], serde_json::json!(true), "stdout: {stdout}");

        let processes = parsed["processes"].as_array().expect("processes array");
        let keeper = processes
            .iter()
            .find(|i| i["name"] == serde_json::json!("keeper"))
            .expect("keeper process present");
        assert_eq!(keeper["kind"], serde_json::json!("service"));
        assert_eq!(keeper["state"], serde_json::json!("ready"));

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn cli_stop_daemon_not_running_gives_clear_error() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("cli-stop-no-daemon");
        let toml = keeper_config();
        let config_path = dir.path().join("don.toml");
        std::fs::write(&config_path, &toml).unwrap();
        // Don't spawn a runner — the socket won't exist.

        let (code, _stdout, stderr) =
            tokio::task::spawn_blocking(move || run_cli(&config_path, &["stop", "api"]))
                .await
                .unwrap();
        assert_eq!(code, 1, "stderr: {stderr}");
        assert!(
            stderr.contains("daemon not running"),
            "stderr should mention daemon not running: {stderr}"
        );
        assert!(
            stderr.contains("don start"),
            "stderr should suggest `don start`: {stderr}"
        );
    });
}

#[test]
fn cli_stop_unknown_name_404() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("cli-stop-unknown");
        let toml = keeper_config();
        let config_path = dir.path().join("don.toml");
        std::fs::write(&config_path, &toml).unwrap();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);

        let path_for_cli = config_path.clone();
        let (code, _stdout, stderr) =
            tokio::task::spawn_blocking(move || run_cli(&path_for_cli, &["stop", "ghost"]))
                .await
                .unwrap();
        assert_eq!(code, 1);
        assert!(stderr.contains("ghost"), "stderr: {stderr}");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn cli_stop_without_name_stops_daemon() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("cli-stop-daemon");
        let toml = keeper_config();
        let config_path = dir.path().join("don.toml");
        std::fs::write(&config_path, &toml).unwrap();

        let (socket, _shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);

        let (code, stdout, stderr) =
            tokio::task::spawn_blocking(move || run_cli(&config_path, &["stop"]))
                .await
                .unwrap();
        assert_eq!(code, 0, "stderr: {stderr}");
        assert!(stdout.contains("don daemon stopped"), "stdout: {stdout}");

        handle.await.unwrap();
    });
}

#[test]
fn cli_detached_start_and_stop_daemon() {
    run_with_timeout(Duration::from_secs(20), async {
        let dir = TempDir::new("cli-detached-start");
        let toml = keeper_config();
        let config_path = dir.path().join("don.toml");
        std::fs::write(&config_path, &toml).unwrap();

        let cp = config_path.clone();
        let (code, stdout, stderr) =
            tokio::task::spawn_blocking(move || run_cli(&cp, &["start", "--detached"]))
                .await
                .unwrap();
        assert_eq!(code, 0, "stderr: {stderr}");
        assert!(
            stdout.contains("don started in background"),
            "stdout: {stdout}"
        );

        let socket = dir.path().join(".don").join("don.sock");
        assert!(wait_for_socket(&socket, Duration::from_secs(5)).await);
        let client = Client::new(dir.path());
        client.status(false, None).await.unwrap();

        let cp = config_path.clone();
        let (code, _stdout, stderr) = tokio::task::spawn_blocking(move || run_cli(&cp, &["stop"]))
            .await
            .unwrap();
        assert_eq!(code, 0, "stderr: {stderr}");
    });
}

#[test]
fn cli_stop_and_restart_flow() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("cli-stop-restart");
        let toml = keeper_config();
        let config_path = dir.path().join("don.toml");
        std::fs::write(&config_path, &toml).unwrap();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        tokio::time::sleep(Duration::from_millis(400)).await;

        // stop keeper
        let cp = config_path.clone();
        let (code, _, stderr) =
            tokio::task::spawn_blocking(move || run_cli(&cp, &["stop", "keeper"]))
                .await
                .unwrap();
        assert_eq!(code, 0, "stderr: {stderr}");

        // status should show stopped
        tokio::time::sleep(Duration::from_millis(200)).await;
        let client = Client::new(dir.path());
        let statuses = client.status(false, None).await.unwrap();
        let joined = format!("{statuses:?}");
        assert!(
            joined.to_lowercase().contains("stopped"),
            "statuses: {joined}"
        );

        // restart keeper
        let cp = config_path.clone();
        let (code, _, stderr) =
            tokio::task::spawn_blocking(move || run_cli(&cp, &["restart", "keeper"]))
                .await
                .unwrap();
        assert_eq!(code, 0, "stderr: {stderr}");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn cli_logs_last_returns_recent_lines() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("cli-logs");
        let toml = ConfigBuilder::new()
            .add_custom_service(
                "chatty",
                "bash",
                &[
                    "-c",
                    "echo line1; echo line2; echo line3; echo line4; echo line5; sleep 60",
                ],
            )
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();
        let config_path = dir.path().join("don.toml");
        std::fs::write(&config_path, &toml).unwrap();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        tokio::time::sleep(Duration::from_millis(600)).await;

        let cp = config_path.clone();
        let (code, stdout, stderr) =
            tokio::task::spawn_blocking(move || run_cli(&cp, &["logs", "chatty", "--last", "3"]))
                .await
                .unwrap();
        assert_eq!(code, 0, "stderr: {stderr}");
        assert!(stdout.contains("line5"), "stdout: {stdout}");
        assert!(stdout.contains("line3"), "stdout: {stdout}");
        // last=3 should not include line1.
        assert!(
            !stdout.contains("line1"),
            "stdout should not include line1: {stdout}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn cli_stop_on_task_returns_bad_request() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("cli-stop-task");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_task("prep", "true", &[])
            .log("ignore")
            .done()
            .build();
        let config_path = dir.path().join("don.toml");
        std::fs::write(&config_path, &toml).unwrap();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);

        let cp = config_path.clone();
        let (code, _stdout, stderr) =
            tokio::task::spawn_blocking(move || run_cli(&cp, &["stop", "prep"]))
                .await
                .unwrap();
        assert_eq!(code, 1);
        assert!(stderr.contains("task"), "stderr: {stderr}");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn cli_start_subcommand_starts_stopped_service() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("cli-start-stopped");
        let toml = keeper_config();
        let config_path = dir.path().join("don.toml");
        std::fs::write(&config_path, &toml).unwrap();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        tokio::time::sleep(Duration::from_millis(400)).await;

        // Stop the service via the API client.
        let client = Client::new(dir.path());
        client.stop("keeper").await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Now use `don start keeper` to restart it.
        let cp = config_path.clone();
        let (code, _stdout, stderr) =
            tokio::task::spawn_blocking(move || run_cli(&cp, &["start", "keeper"]))
                .await
                .unwrap();
        assert_eq!(code, 0, "stderr: {stderr}");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn cli_run_wait_after_task_name_waits_for_completion() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("cli-run-wait");
        let out_path = dir.path().join("task-output.txt");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_task(
                "slow",
                "sh",
                &[
                    "-c",
                    &format!("sleep 0.2; echo done >> {}", out_path.display()),
                ],
            )
            .log("ignore")
            .auto_run(false)
            .done()
            .build();
        let config_path = dir.path().join("don.toml");
        std::fs::write(&config_path, &toml).unwrap();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        tokio::time::sleep(Duration::from_millis(400)).await;

        let cp = config_path.clone();
        let (code, _stdout, stderr) =
            tokio::task::spawn_blocking(move || run_cli(&cp, &["run", "--wait", "slow"]))
                .await
                .unwrap();
        assert_eq!(code, 0, "stderr: {stderr}");

        let cp = config_path.clone();
        let (code, _stdout, stderr) =
            tokio::task::spawn_blocking(move || run_cli(&cp, &["run", "slow", "--wait"]))
                .await
                .unwrap();
        assert_eq!(code, 0, "stderr: {stderr}");

        let cp = config_path.clone();
        let (code, _stdout, stderr) =
            tokio::task::spawn_blocking(move || run_cli(&cp, &["run", "--timeout", "5s", "slow"]))
                .await
                .unwrap();
        assert_eq!(code, 0, "stderr: {stderr}");
        let captured = std::fs::read_to_string(&out_path).unwrap();
        assert_eq!(
            captured.lines().collect::<Vec<_>>(),
            vec!["done", "done", "done"]
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn cli_run_timeout_after_task_name_limits_wait() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("cli-run-timeout");
        let out_path = dir.path().join("task-output.txt");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_task(
                "slow",
                "sh",
                &[
                    "-c",
                    &format!("sleep 2; echo done > {}", out_path.display()),
                ],
            )
            .log("ignore")
            .auto_run(false)
            .done()
            .build();
        let config_path = dir.path().join("don.toml");
        std::fs::write(&config_path, &toml).unwrap();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        tokio::time::sleep(Duration::from_millis(400)).await;

        let cp = config_path.clone();
        let (code, _stdout, stderr) = tokio::task::spawn_blocking(move || {
            run_cli(&cp, &["run", "slow", "--timeout", "100ms"])
        })
        .await
        .unwrap();
        assert_eq!(code, 124, "stderr: {stderr}");
        assert!(
            stderr.contains("did not finish within 100ms"),
            "stderr: {stderr}"
        );
        assert!(
            !out_path.exists(),
            "task should still be sleeping when the CLI wait times out"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}
