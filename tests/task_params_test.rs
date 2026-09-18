//! End-to-end tests for task params: schema, CLI parsing, runner dispatch,
//! template substitution, and the file-watch/dependency gate that only parks
//! param'd tasks in PendingRun when they are actually needed.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod helpers;

use don::config::{Config, LogConfig, Platform};
use don::output::OutputManager;
use don::runner::{Runner, RunnerEvent, TaskState};
use helpers::config::parse_config;
use helpers::tempdir::TempDir;
use helpers::timeout::run_with_timeout;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};

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

#[allow(dead_code)]
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

/// Wait for a `TaskStateChanged` to `target` for `task_name`. Returns once
/// observed or the broadcast closes (returns `false`).
async fn wait_for_task_state(
    events: &mut broadcast::Receiver<RunnerEvent>,
    task_name: &str,
    target: TaskState,
) -> bool {
    loop {
        match events.recv().await {
            Ok(RunnerEvent::TaskStateChanged { name, state, .. })
                if name == task_name && state == target =>
            {
                return true;
            }
            Ok(_) => continue,
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => return false,
        }
    }
}

/// Poll the output buffer until `needle` appears, then return the whole thing.
///
/// A state event and the lifecycle line that describes it travel by different
/// routes — the broadcast, and the output actor's writer — so observing the
/// event says nothing about the line having been written yet. Reading the
/// buffer the instant the event lands is a race that only shows up when the
/// machine is loaded enough to delay the sink task.
async fn wait_for_buf(buf: &Arc<Mutex<Vec<u8>>>, needle: &str) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let contents = read_buf(buf);
        if contents.contains(needle) || tokio::time::Instant::now() >= deadline {
            return contents;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_line_count(path: &std::path::Path, count: usize) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(contents) = std::fs::read_to_string(path)
            && contents.lines().count() >= count
        {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Common setup: a "keeper" service that idles long enough for the test to
/// exercise commands, plus the task definition the test passes in.
fn toml_with_keeper(task_toml: &str) -> String {
    format!(
        r#"
[services.keeper]
run.cmd = "sleep"
run.args = ["60"]
log = "ignore"
ready.exec.cmd = "true"

{task_toml}
"#
    )
}

#[test]
fn integration_task_with_params_substitutes_values() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-params-substitute");
        let out_path = dir.path().join("captured.txt");

        // Use `sh -c` to write the substituted args + DON_PARAM_INDEX into a
        // file we can inspect after the task completes.
        let toml = toml_with_keeper(&format!(
            r#"
[tasks.sync]
cmd = "sh"
args = ["-c", "echo index={{{{index}}}} batch={{{{batch_size}}}} env=$DON_PARAM_INDEX > {}"]
log = "ignore"

[[tasks.sync.params]]
name = "index"
required = true

[[tasks.sync.params]]
name = "batch_size"
kind = "int"
default = "100"
"#,
            out_path.display()
        ));
        let (runner, shutdown_tx, _buf) = make_runner(&toml, dir.path()).await;
        let control = runner.process_control();
        let mut events = runner.subscribe();

        let runner_handle = tokio::spawn(async move { runner.run().await });
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Trigger the task with --index=users.
        let mut params = HashMap::new();
        params.insert("index".to_string(), "users".to_string());
        control
            .run_task("sync", params, false, None)
            .await
            .unwrap()
            .unwrap();

        // Wait for completion.
        assert!(
            wait_for_task_state(&mut events, "sync", TaskState::Completed).await,
            "task didn't reach Completed"
        );

        // Verify substituted values landed in the output file.
        let captured = std::fs::read_to_string(&out_path).unwrap();
        assert!(
            captured.contains("index=users"),
            "missing index=users in {captured:?}"
        );
        assert!(
            captured.contains("batch=100"),
            "missing batch=100 (default) in {captured:?}"
        );
        assert!(
            captured.contains("env=users"),
            "missing DON_PARAM_INDEX export in {captured:?}"
        );

        let _ = shutdown_tx.send(()).await;
        let _ = runner_handle.await;
    });
}

#[test]
fn integration_task_logs_rendered_spawn_command() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-params-spawn-log");
        let out_path = dir.path().join("captured.txt");

        let toml = toml_with_keeper(&format!(
            r#"
[tasks.sync]
cmd = "sh"
args = ["-c", "echo index={{{{index}}}} batch={{{{batch_size}}}} > {}"]
log = "ignore"

[[tasks.sync.params]]
name = "index"
required = true

[[tasks.sync.params]]
name = "batch_size"
kind = "int"
default = "100"
"#,
            out_path.display()
        ));
        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let control = runner.process_control();
        let mut events = runner.subscribe();

        let runner_handle = tokio::spawn(async move { runner.run().await });
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut params = HashMap::new();
        params.insert("index".to_string(), "users".to_string());
        control
            .run_task("sync", params, false, None)
            .await
            .unwrap()
            .unwrap();

        assert!(
            wait_for_task_state(&mut events, "sync", TaskState::Completed).await,
            "task didn't reach Completed"
        );

        // The spawn line is emitted after the trigger line and carries the
        // rendered params, so waiting for it covers all three assertions.
        let output = wait_for_buf(&buf, "sync: spawn sh -c").await;
        assert!(
            output.contains("sync: running (manual trigger)"),
            "missing manual trigger line in {output:?}"
        );
        assert!(
            output.contains("sync: spawn sh -c"),
            "missing spawn line in {output:?}"
        );
        assert!(
            output.contains("index=users batch=100"),
            "spawn line did not include rendered params in {output:?}"
        );

        let _ = shutdown_tx.send(()).await;
        let _ = runner_handle.await;
    });
}

#[test]
fn integration_task_without_params_keeps_working() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-params-backcompat");
        let out_path = dir.path().join("captured.txt");

        // auto_run = false keeps the task manual; with no watch inputs or
        // dependents it starts skipped, but RunTask should still work.
        let toml = toml_with_keeper(&format!(
            r#"
[tasks.plain]
cmd = "sh"
args = ["-c", "echo plain > {}"]
log = "ignore"
auto_run = false
"#,
            out_path.display()
        ));
        let (runner, shutdown_tx, _buf) = make_runner(&toml, dir.path()).await;
        let control = runner.process_control();
        let mut events = runner.subscribe();
        let runner_handle = tokio::spawn(async move { runner.run().await });

        // Wait until the keeper is up and the runner is processing commands.
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Empty params map mirrors what `dispatch_action` sends for plain
        // run actions.
        control
            .run_task("plain", HashMap::new(), false, None)
            .await
            .unwrap()
            .unwrap();

        assert!(
            wait_for_task_state(&mut events, "plain", TaskState::Completed).await,
            "task didn't complete"
        );
        let captured = std::fs::read_to_string(&out_path).unwrap();
        assert_eq!(captured.trim(), "plain");

        let _ = shutdown_tx.send(()).await;
        let _ = runner_handle.await;
    });
}

#[test]
fn integration_task_can_be_run_twice_manually() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-rerun-manual");
        let out_path = dir.path().join("captured.txt");

        let toml = toml_with_keeper(&format!(
            r#"
[tasks.plain]
cmd = "sh"
args = ["-c", "echo plain >> {}"]
log = "ignore"
auto_run = false
"#,
            out_path.display()
        ));
        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let control = runner.process_control();
        let mut events = runner.subscribe();

        let runner_handle = tokio::spawn(async move { runner.run().await });
        tokio::time::sleep(Duration::from_millis(300)).await;

        for _ in 0..2 {
            control
                .run_task("plain", HashMap::new(), false, None)
                .await
                .unwrap()
                .unwrap();
            assert!(
                wait_for_task_state(&mut events, "plain", TaskState::Running).await,
                "task didn't reach Running"
            );
            assert!(
                wait_for_task_state(&mut events, "plain", TaskState::Completed).await,
                "task didn't reach Completed"
            );
        }

        let captured = std::fs::read_to_string(&out_path).unwrap();
        assert_eq!(
            captured.lines().count(),
            2,
            "captured: {captured:?}\noutput: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        let _ = runner_handle.await;
    });
}

#[test]
fn integration_run_task_wait_replies_after_task_exits() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-run-wait");
        let out_path = dir.path().join("captured.txt");

        let toml = toml_with_keeper(&format!(
            r#"
[tasks.slow]
cmd = "sh"
args = ["-c", "sleep 0.2; echo done > {}"]
log = "ignore"
auto_run = false
"#,
            out_path.display()
        ));
        let (runner, shutdown_tx, _buf) = make_runner(&toml, dir.path()).await;
        let control = runner.process_control();

        let runner_handle = tokio::spawn(async move { runner.run().await });
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Pinned rather than spawned: the first poll is what delivers the
        // request, so the timeout below also establishes that the supervisor
        // has it before the conflicting one is sent.
        let mut waited = std::pin::pin!(control.run_task("slow", HashMap::new(), true, None));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut waited)
                .await
                .is_err(),
            "wait reply arrived before the task could exit"
        );

        let result = control
            .run_task("slow", HashMap::new(), true, None)
            .await
            .unwrap();
        assert!(result.is_err(), "second run should reject while running");

        waited.await.unwrap().unwrap();
        assert!(
            wait_for_line_count(&out_path, 1).await,
            "task did not write output"
        );

        let _ = shutdown_tx.send(()).await;
        let _ = runner_handle.await;
    });
}

#[test]
fn integration_run_task_wait_returns_task_failure() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-run-wait-failure");

        let toml = toml_with_keeper(
            r#"
[tasks.fail]
cmd = "sh"
args = ["-c", "exit 7"]
log = "ignore"
auto_run = false
"#,
        );
        let (runner, shutdown_tx, _buf) = make_runner(&toml, dir.path()).await;
        let control = runner.process_control();

        let runner_handle = tokio::spawn(async move { runner.run().await });
        tokio::time::sleep(Duration::from_millis(300)).await;

        let result = control
            .run_task("fail", HashMap::new(), true, None)
            .await
            .unwrap();
        match result {
            Err(don::runner::CommandError::Failed { name, message }) => {
                assert_eq!(name, "fail");
                assert!(
                    message.contains("exit code 7"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("expected task failure, got {other:?}"),
        }

        let _ = shutdown_tx.send(()).await;
        let _ = runner_handle.await;
    });
}

#[test]
fn integration_run_task_waiter_is_failed_on_shutdown() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-run-wait-shutdown");

        let toml = toml_with_keeper(
            r#"
[tasks.long]
cmd = "sleep"
args = ["60"]
log = "ignore"
auto_run = false
"#,
        );
        let (runner, shutdown_tx, _buf) = make_runner(&toml, dir.path()).await;
        let control = runner.process_control();
        let mut events = runner.subscribe();

        let runner_handle = tokio::spawn(async move { runner.run().await });
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut waited = std::pin::pin!(control.run_task("long", HashMap::new(), true, None));
        // Poll once so the request is delivered, then let the task start.
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut waited)
                .await
                .is_err(),
            "a `sleep 60` cannot have finished already"
        );
        assert!(
            wait_for_task_state(&mut events, "long", TaskState::Running).await,
            "task did not start"
        );

        let _ = shutdown_tx.send(()).await;
        let result = tokio::time::timeout(Duration::from_secs(2), waited)
            .await
            .expect("wait reply hung during shutdown")
            .unwrap();
        match result {
            Err(don::runner::CommandError::Failed { name, message }) => {
                assert_eq!(name, "long");
                assert!(
                    message.contains("cancelled by shutdown"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("expected shutdown cancellation, got {other:?}"),
        }

        let _ = runner_handle.await;
    });
}

#[test]
fn integration_restart_running_task_reuses_last_params() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-restart-running");
        let value_path = dir.path().join("values.txt");
        let pid_path = dir.path().join("pids.txt");

        let toml = toml_with_keeper(&format!(
            r#"
[tasks.sync]
cmd = "sh"
args = ["-c", "echo $DON_PARAM_INDEX >> {}; echo $$ >> {}; sleep 300"]
log = "ignore"
auto_run = false

[[tasks.sync.params]]
name = "index"
required = true
"#,
            value_path.display(),
            pid_path.display()
        ));
        let (runner, shutdown_tx, _buf) = make_runner(&toml, dir.path()).await;
        let control = runner.process_control();
        let mut events = runner.subscribe();

        let runner_handle = tokio::spawn(async move { runner.run().await });
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut params = HashMap::new();
        params.insert("index".to_string(), "users".to_string());
        control
            .run_task("sync", params, false, None)
            .await
            .unwrap()
            .unwrap();
        assert!(
            wait_for_task_state(&mut events, "sync", TaskState::Running).await,
            "task didn't reach Running"
        );
        assert!(
            wait_for_line_count(&pid_path, 1).await,
            "pid file was not written"
        );

        control.restart("sync").await.unwrap().unwrap();

        assert!(
            wait_for_line_count(&pid_path, 2).await,
            "task did not restart"
        );

        let values = std::fs::read_to_string(&value_path).unwrap();
        assert_eq!(values.lines().collect::<Vec<_>>(), vec!["users", "users"]);

        let pids = std::fs::read_to_string(&pid_path).unwrap();
        let pid_lines: Vec<&str> = pids.lines().collect();
        assert_eq!(pid_lines.len(), 2, "pids: {pids:?}");
        assert_ne!(pid_lines[0], pid_lines[1], "pids: {pids:?}");

        let _ = shutdown_tx.send(()).await;
        let _ = runner_handle.await;
    });
}

#[test]
fn integration_missing_required_param_errors() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-params-missing");

        let toml = toml_with_keeper(
            r#"
[tasks.sync]
cmd = "sh"
args = ["-c", "true"]
log = "ignore"

[[tasks.sync.params]]
name = "index"
required = true
"#,
        );
        let (runner, shutdown_tx, _buf) = make_runner(&toml, dir.path()).await;
        let control = runner.process_control();
        let runner_handle = tokio::spawn(async move { runner.run().await });
        tokio::time::sleep(Duration::from_millis(300)).await;

        let result = control
            .run_task("sync", HashMap::new(), false, None)
            .await
            .unwrap();
        match result {
            Err(don::runner::CommandError::InvalidParams { name, message }) => {
                assert_eq!(name, "sync");
                assert!(
                    message.contains("missing required"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("expected InvalidParams, got {other:?}"),
        }

        let _ = shutdown_tx.send(()).await;
        let _ = runner_handle.await;
    });
}

#[test]
fn integration_unknown_param_errors() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-params-unknown");

        let toml = toml_with_keeper(
            r#"
[tasks.sync]
cmd = "sh"
args = ["-c", "true"]
log = "ignore"

[[tasks.sync.params]]
name = "index"
"#,
        );
        let (runner, shutdown_tx, _buf) = make_runner(&toml, dir.path()).await;
        let control = runner.process_control();
        let runner_handle = tokio::spawn(async move { runner.run().await });
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut params = HashMap::new();
        params.insert("nope".to_string(), "x".to_string());
        let result = control.run_task("sync", params, false, None).await.unwrap();
        match result {
            Err(don::runner::CommandError::InvalidParams { message, .. }) => {
                assert!(
                    message.contains("unknown param '--nope'"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("expected InvalidParams, got {other:?}"),
        }

        let _ = shutdown_tx.send(()).await;
        let _ = runner_handle.await;
    });
}

#[test]
fn integration_paramd_task_without_watch_or_dependents_starts_skipped() {
    // Param'd tasks that are neither watched nor required by dependents are
    // not considered "needed", so startup should skip them rather than
    // parking them in PendingRun.
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-params-pending");

        let toml = toml_with_keeper(
            r#"
[tasks.interactive]
cmd = "sh"
args = ["-c", "true"]
log = "ignore"

[[tasks.interactive.params]]
name = "x"
"#,
        );
        let (runner, shutdown_tx, _buf) = make_runner(&toml, dir.path()).await;
        let mut events = runner.subscribe();
        let runner_handle = tokio::spawn(async move { runner.run().await });

        let mut saw_skipped = false;
        let mut saw_run = false;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let timeout = deadline
                .checked_duration_since(tokio::time::Instant::now())
                .unwrap_or(Duration::from_millis(0));
            match tokio::time::timeout(timeout, events.recv()).await {
                Ok(Ok(RunnerEvent::TaskStateChanged { name, state, .. }))
                    if name == "interactive" =>
                {
                    match state {
                        TaskState::Skipped => saw_skipped = true,
                        TaskState::Running | TaskState::Completed => saw_run = true,
                        _ => {}
                    }
                }
                Ok(_) => continue,
                Err(_) => break,
            }
        }
        assert!(saw_skipped, "expected Skipped state for param'd task");
        assert!(!saw_run, "param'd task should NOT have auto-run at startup");

        let _ = shutdown_tx.send(()).await;
        let _ = runner_handle.await;
    });
}

#[test]
fn integration_completion_failure_writes_log() {
    // Resolving completions for a param whose command fails returns an
    // error AND writes a log file under .don/logs/completions/.
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-params-completion-fail");

        let toml = toml_with_keeper(
            r#"
[tasks.sync]
cmd = "sh"
args = ["-c", "true"]
log = "ignore"

[[tasks.sync.params]]
name = "index"

[tasks.sync.params.completions]
cmd = "false"
"#,
        );
        let (runner, shutdown_tx, _buf) = make_runner(&toml, dir.path()).await;
        let completions = runner.completion_resolver();
        let runner_handle = tokio::spawn(async move { runner.run().await });
        tokio::time::sleep(Duration::from_millis(300)).await;

        let result = completions
            .resolve_param("sync", "index", &HashMap::new(), false)
            .await;
        let err = result.expect_err("completion command exits 1, should be an error");
        let log_path = err.log_path.expect("error should carry a log path");
        assert!(log_path.exists(), "log file should have been written");
        let body = std::fs::read_to_string(&log_path).unwrap();
        assert!(body.contains("task: sync"), "log missing task name: {body}");
        assert!(
            body.contains("param: index"),
            "log missing param name: {body}"
        );

        let _ = shutdown_tx.send(()).await;
        let _ = runner_handle.await;
    });
}

#[test]
fn integration_static_choices_resolve_without_shelling_out() {
    // Static `choices` skip the shell-out path — the runner returns the
    // configured list directly.
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-params-static-choices");

        let toml = toml_with_keeper(
            r#"
[tasks.sync]
cmd = "sh"
args = ["-c", "true"]
log = "ignore"

[[tasks.sync.params]]
name = "env"
choices = ["dev", "staging", "prod"]
"#,
        );
        let (runner, shutdown_tx, _buf) = make_runner(&toml, dir.path()).await;
        let completions = runner.completion_resolver();
        let runner_handle = tokio::spawn(async move { runner.run().await });
        tokio::time::sleep(Duration::from_millis(300)).await;

        let values = completions
            .resolve_param("sync", "env", &HashMap::new(), false)
            .await
            .unwrap();
        assert_eq!(values, vec!["dev", "staging", "prod"]);

        let _ = shutdown_tx.send(()).await;
        let _ = runner_handle.await;
    });
}
