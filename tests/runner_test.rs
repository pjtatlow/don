#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod helpers;

use don::config::{Config, LogConfig, Platform};
use don::output::OutputManager;
use don::runner::{ProcessStatus, Runner, RunnerCommand, ServiceState};
use don::task_state::TaskStateStore;
use helpers::config::ConfigBuilder;
use helpers::port::free_port;
use helpers::tempdir::TempDir;
use helpers::timeout::run_with_timeout;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

const PLATFORM: Platform = Platform::LinuxX86_64;

/// A test buffer that implements Write and allows reading back contents.
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

/// Read the test buffer as a string.
fn read_buf(buf: &Arc<Mutex<Vec<u8>>>) -> String {
    String::from_utf8_lossy(&buf.lock().unwrap()).into_owned()
}

/// Helper: parse a config string, validate, create OutputManager, Runner,
/// and a shutdown sender for test control.
async fn make_runner(
    toml: &str,
    base_dir: &std::path::Path,
) -> (Runner, mpsc::Sender<()>, Arc<Mutex<Vec<u8>>>) {
    make_runner_verbose(toml, base_dir, false).await
}

async fn make_runner_verbose(
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

// --- Parallel executor test ---

#[test]
fn integration_parallel_services_start_concurrently() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("parallel-exec");

        let toml = ConfigBuilder::new()
            .add_custom_service("svc-a", "sleep", &["1"])
            .log("ignore")
            .done()
            .add_custom_service("svc-b", "sleep", &["1"])
            .log("ignore")
            .done()
            .add_custom_service("svc-c", "sleep", &["1"])
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let start = std::time::Instant::now();

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Give services time to start concurrently.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let _ = shutdown_tx.send(()).await;

        handle.await.unwrap();
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_secs(5),
            "parallel start should complete quickly, took {elapsed:?}"
        );

        let output = read_buf(&buf);
        assert!(output.contains("svc-a: starting"), "should start svc-a");
        assert!(output.contains("svc-b: starting"), "should start svc-b");
        assert!(output.contains("svc-c: starting"), "should start svc-c");
    });
}

// --- Dependency ordering test ---

#[test]
fn integration_dependency_ordering_a_before_b() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("dep-order");
        let port = free_port();

        let listen_script = dir.path().join("listen.sh");
        std::fs::write(
            &listen_script,
            format!(
                "#!/bin/sh\n\
                 exec python3 -c \"\nimport socket, time\ns = socket.socket()\ns.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\ns.bind(('127.0.0.1', {port}))\ns.listen(1)\nwhile True: time.sleep(60)\n\"\n"
            ),
        )
        .unwrap();
        std::fs::set_permissions(
            &listen_script,
            std::os::unix::fs::PermissionsExt::from_mode(0o755),
        )
        .unwrap();

        let toml = ConfigBuilder::new()
            .add_custom_service("svc-a", listen_script.to_str().unwrap(), &[])
            .log("ignore")
            .done()
            .add_custom_service("svc-b", "sleep", &["300"])
            .depends_on(&["svc-a"])
            .ready_tcp_with(&format!("127.0.0.1:{port}"), "200ms", 30)
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        tokio::time::sleep(Duration::from_secs(5)).await;
        let _ = shutdown_tx.send(()).await;

        handle.await.unwrap();
        let output = read_buf(&buf);

        let a_start = output.find("svc-a: starting");
        let b_start = output.find("svc-b: starting");

        assert!(a_start.is_some(), "svc-a should start: {output}");
        if let (Some(a), Some(b)) = (a_start, b_start) {
            assert!(a < b, "svc-a should start before svc-b: a at {a}, b at {b}");
        }
    });
}

// --- Task depends on service ---

#[test]
fn integration_task_depends_on_service() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-dep-svc");
        let port = free_port();

        let listen_script = dir.path().join("listen.sh");
        std::fs::write(
            &listen_script,
            format!(
                "#!/bin/sh\nexec python3 -c \"\nimport socket, time\ns = socket.socket()\ns.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\ns.bind(('127.0.0.1', {port}))\ns.listen(1)\nwhile True: time.sleep(60)\n\"\n"
            ),
        )
        .unwrap();
        std::fs::set_permissions(
            &listen_script,
            std::os::unix::fs::PermissionsExt::from_mode(0o755),
        )
        .unwrap();

        let toml = ConfigBuilder::new()
            .add_custom_service("mydb", listen_script.to_str().unwrap(), &[])
            .ready_tcp_with(&format!("127.0.0.1:{port}"), "200ms", 30)
            .log("ignore")
            .done()
            .add_task("migrate", "echo", &["migration done"])
            .depends_on(&["mydb"])
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        tokio::time::sleep(Duration::from_secs(5)).await;
        let _ = shutdown_tx.send(()).await;

        handle.await.unwrap();
        let output = read_buf(&buf);

        let svc_start = output.find("mydb: starting");
        let task_run = output.find("migrate: running");
        assert!(svc_start.is_some(), "mydb should start: {output}");
        assert!(task_run.is_some(), "migrate should run: {output}");

        if let (Some(s), Some(t)) = (svc_start, task_run) {
            assert!(s < t, "mydb should start before migrate runs");
        }
    });
}

#[test]
fn integration_headless_task_uses_command_override_and_is_not_interactive() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("headless-task-override");
        let output_path = dir.path().join("task-output.txt");
        let toml = format!(
            r#"
[tasks.push]
cmd = "sh"
args = ["-c", "printf interactive > {}"]
interactive = true
headless = {{ args = ["-c", "printf headless > {}"] }}
log = "ignore"
"#,
            output_path.display(),
            output_path.display()
        );

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        wait_for_substr(&buf, "push: complete", Duration::from_secs(5)).await;
        let task_output = std::fs::read_to_string(&output_path).unwrap();
        assert_eq!(task_output, "headless");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_manual_task_dependency_unblocks_service_after_run() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("manual-task-dep");
        let port = free_port();

        let listen_script = dir.path().join("listen.sh");
        std::fs::write(
            &listen_script,
            format!(
                "#!/bin/sh\nexec python3 -c \"\nimport socket, time\ns = socket.socket()\ns.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\ns.bind(('127.0.0.1', {port}))\ns.listen(1)\nwhile True: time.sleep(60)\n\"\n"
            ),
        )
        .unwrap();
        std::fs::set_permissions(
            &listen_script,
            std::os::unix::fs::PermissionsExt::from_mode(0o755),
        )
        .unwrap();

        let toml = ConfigBuilder::new()
            .add_task("migrate", "echo", &["migration done"])
            .auto_run(false)
            .log("ignore")
            .done()
            .add_custom_service("api", listen_script.to_str().unwrap(), &[])
            .depends_on(&["migrate"])
            .ready_tcp_with(&format!("127.0.0.1:{port}"), "200ms", 30)
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let control = runner.process_control();
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        wait_for_substr(
            &buf,
            "migrate: pending — required by dependents, run manually",
            Duration::from_secs(5),
        )
        .await;

        let output = read_buf(&buf);
        assert!(
            !output.contains("api: starting"),
            "api should remain blocked before migrate runs: {output}"
        );

        control
            .run_task("migrate", std::collections::HashMap::new(), false, None)
            .await
            .unwrap()
            .unwrap();

        wait_for_substr(&buf, "api: starting", Duration::from_secs(5)).await;
        assert!(
            wait_for_any_substr(
                &buf,
                &["api: ready (tcp", "api: started"],
                Duration::from_secs(5)
            )
            .await,
            "api should start after migrate completes: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_auto_run_once_only_runs_on_first_startup() {
    run_with_timeout(Duration::from_secs(20), async {
        let dir = TempDir::new("auto-run-once");
        let port = free_port();
        let schema_dir = dir.path().join("schema");
        std::fs::create_dir_all(&schema_dir).unwrap();
        let schema_file = schema_dir.join("schema.sql");
        std::fs::write(&schema_file, "create table users (id int);").unwrap();

        let listen_script = dir.path().join("listen.sh");
        std::fs::write(
            &listen_script,
            format!(
                "#!/bin/sh\nexec python3 -c \"\nimport socket, time\ns = socket.socket()\ns.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\ns.bind(('127.0.0.1', {port}))\ns.listen(1)\nwhile True: time.sleep(60)\n\"\n"
            ),
        )
        .unwrap();
        std::fs::set_permissions(
            &listen_script,
            std::os::unix::fs::PermissionsExt::from_mode(0o755),
        )
        .unwrap();

        let toml = ConfigBuilder::new()
            .add_task("migrate", "echo", &["migration done"])
            .watch(&["schema/**/*.sql"])
            .auto_run_mode("once")
            .log("ignore")
            .done()
            .add_custom_service("api", listen_script.to_str().unwrap(), &[])
            .depends_on(&["migrate"])
            .ready_tcp_with(&format!("127.0.0.1:{port}"), "200ms", 30)
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        wait_for_substr(&buf, "migrate: running", Duration::from_secs(5)).await;
        wait_for_substr(&buf, "api: starting", Duration::from_secs(5)).await;
        assert!(
            wait_for_any_substr(
                &buf,
                &["api: ready (tcp", "api: started"],
                Duration::from_secs(5)
            )
            .await,
            "api should become ready on first startup: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();

        std::fs::write(&schema_file, "create table users (id int, name text);").unwrap();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        wait_for_substr(
            &buf,
            "migrate: pending — required by dependents, auto_run = once",
            Duration::from_secs(5),
        )
        .await;

        assert!(
            wait_for_output_order(
                &buf,
                "migrate: pending — required by dependents, auto_run = once",
                "api: starting",
                Duration::from_secs(5),
            )
            .await,
            "api should start even though the once task is pending after prior success: {}",
            read_buf(&buf)
        );
        assert!(
            wait_for_any_substr(
                &buf,
                &["api: ready (tcp", "api: started"],
                Duration::from_secs(5)
            )
            .await,
            "api should become ready on later startup: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- TCP ready check ---

#[test]
fn integration_tcp_ready_check() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("tcp-ready");
        let port = free_port();

        let listen_script = dir.path().join("listen.sh");
        std::fs::write(
            &listen_script,
            format!(
                "#!/bin/sh\nexec python3 -c \"\nimport socket, time\ns = socket.socket()\ns.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\ns.bind(('127.0.0.1', {port}))\ns.listen(1)\nwhile True: time.sleep(60)\n\"\n"
            ),
        )
        .unwrap();
        std::fs::set_permissions(
            &listen_script,
            std::os::unix::fs::PermissionsExt::from_mode(0o755),
        )
        .unwrap();

        let toml = ConfigBuilder::new()
            .add_custom_service("tcpsvc", listen_script.to_str().unwrap(), &[])
            .ready_tcp_with(&format!("127.0.0.1:{port}"), "200ms", 30)
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        tokio::time::sleep(Duration::from_secs(5)).await;
        let _ = shutdown_tx.send(()).await;

        handle.await.unwrap();
        let output = read_buf(&buf);

        assert!(
            output.contains("tcpsvc") && output.contains("ready"),
            "should show tcpsvc ready: {output}"
        );
    });
}

/// A client watching only the event stream must learn every service's pid.
///
/// The wire that records custody lands *after* the transition that announced
/// the start, so the "started" event cannot carry a pid. A service with a ready
/// check gets a second transition afterwards and its pid rides along on that;
/// one without never does. Both must end up announced, or the TUI's PID column
/// is blank for exactly the services that are quickest to come up.
#[test]
fn integration_every_service_announces_its_pid_on_the_event_stream() {
    struct Case {
        name: &'static str,
        service: &'static str,
        ready_check: bool,
    }

    let cases = [
        Case {
            name: "a second transition would have carried it anyway",
            service: "checked",
            ready_check: true,
        },
        Case {
            name: "ready at start, so the wire is the only chance",
            service: "plain",
            ready_check: false,
        },
    ];

    for case in cases {
        run_with_timeout(Duration::from_secs(15), async {
            let dir = TempDir::new("pid-events");
            let mut builder = ConfigBuilder::new()
                .add_custom_service(case.service, "sleep", &["60"])
                .log("ignore");
            if case.ready_check {
                builder = builder.ready_exec("true", &[]);
            }
            let toml = builder.done().build();

            let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
            let mut events = runner.subscribe();
            let handle = tokio::spawn(async move {
                let _ = runner.run().await;
            });
            wait_for_substr(&buf, "all services running", Duration::from_secs(5)).await;

            let mut announced = None;
            while announced.is_none() {
                let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
                    .await
                    .unwrap()
                    .unwrap();
                if let don::runner::RunnerEvent::ServiceStateChanged { name, pid, .. } = event
                    && name == case.service
                {
                    announced = pid;
                }
            }

            let _ = shutdown_tx.send(()).await;
            handle.await.unwrap();
            assert!(
                announced.is_some_and(|pid| pid > 0),
                "{}: {} never announced a pid",
                case.name,
                case.service
            );
        });
    }
}

// --- Exec ready check ---

#[test]
fn integration_exec_ready_check_with_retries() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("exec-ready");

        let counter_file = dir.path().join("counter");
        std::fs::write(&counter_file, "0").unwrap();

        let check_script = dir.path().join("check.sh");
        std::fs::write(
            &check_script,
            format!(
                "#!/bin/sh\n\
                 COUNT=$(cat {})\n\
                 COUNT=$((COUNT + 1))\n\
                 echo $COUNT > {}\n\
                 if [ $COUNT -ge 3 ]; then exit 0; else exit 1; fi\n",
                counter_file.display(),
                counter_file.display(),
            ),
        )
        .unwrap();
        std::fs::set_permissions(
            &check_script,
            std::os::unix::fs::PermissionsExt::from_mode(0o755),
        )
        .unwrap();

        let toml = ConfigBuilder::new()
            .add_custom_service("execsvc", "sleep", &["300"])
            .ready_exec_with(check_script.to_str().unwrap(), &[], "200ms", 10)
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        tokio::time::sleep(Duration::from_secs(5)).await;
        let _ = shutdown_tx.send(()).await;

        handle.await.unwrap();
        let output = read_buf(&buf);

        assert!(
            output.contains("execsvc") && output.contains("ready"),
            "exec ready check should pass after retries: {output}"
        );

        let count: i32 = std::fs::read_to_string(&counter_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert!(
            count >= 3,
            "check should have run at least 3 times, ran {count}"
        );
    });
}

// --- Health monitor: unhealthy + auto-restart ---

#[test]
fn integration_health_monitor_marks_unhealthy_and_auto_restarts() {
    use std::os::unix::fs::PermissionsExt;

    run_with_timeout(Duration::from_secs(30), async {
        let dir = TempDir::new("health-restart");

        // Sentinel-driven exec ready check: passes iff the file exists.
        // Lets the test flip the service between healthy and unhealthy by
        // creating/deleting one file.
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

        // ConfigBuilder doesn't expose monitor fields — drop to raw TOML.
        let toml = format!(
            r#"
[services.svc]
run.cmd = "sleep"
run.args = ["300"]
log = "ignore"
on_failure = "restart"

[services.svc.ready]
exec.cmd = "{}"
interval = "150ms"
retries = 20
monitor = true
monitor_interval = "150ms"
unhealthy_after = 2
"#,
            check_script.display()
        );

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Wait for the initial ready event so we know the monitor is running.
        wait_for_substr(&buf, "ready (exec)", Duration::from_secs(8)).await;

        // Knock the service into Unhealthy by removing the sentinel. With
        // monitor_interval=150ms and unhealthy_after=2, the transition
        // should land within ~500ms.
        std::fs::remove_file(&sentinel).unwrap();
        wait_for_substr(&buf, "unhealthy", Duration::from_secs(3)).await;

        // First-attempt backoff is 1s. Restore the sentinel during that
        // window so the auto-restart's new instance can reach Ready and
        // we get a clean shutdown after.
        tokio::time::sleep(Duration::from_millis(400)).await;
        std::fs::write(&sentinel, "ok").unwrap();

        // Verify the auto-restart actually fired (it might not, if the
        // recovery probe beat the backoff timer — accept either path).
        let recovered_or_restarted = wait_for_any_substr(
            &buf,
            &["auto-restart firing", "recovered"],
            Duration::from_secs(5),
        )
        .await;
        assert!(
            recovered_or_restarted,
            "expected either auto-restart or recovery: {}",
            read_buf(&buf)
        );

        // Let the system settle so the new instance reaches Ready (or the
        // recovery path emits its event) before tearing down.
        tokio::time::sleep(Duration::from_secs(2)).await;

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();

        let output = read_buf(&buf);
        // Sequence assertions: initial ready precedes unhealthy, which
        // precedes either the recovery event or the auto-restart attempt.
        let ready_pos = output.find("ready (exec)").expect("initial ready");
        let unhealthy_pos = output[ready_pos..]
            .find("unhealthy")
            .map(|p| p + ready_pos)
            .expect("unhealthy after ready");
        let restart_pos = output[unhealthy_pos..]
            .find("auto-restart firing")
            .map(|p| p + unhealthy_pos);
        let recover_pos = output[unhealthy_pos..]
            .find("recovered")
            .map(|p| p + unhealthy_pos);
        assert!(
            restart_pos.is_some() || recover_pos.is_some(),
            "expected auto-restart or recovery after unhealthy: {output}"
        );
    });
}

/// Poll the test buffer for a substring, panicking on timeout. Used to
/// synchronize between the test and the runner without sleep-and-pray.
async fn wait_for_substr(buf: &Arc<Mutex<Vec<u8>>>, needle: &str, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if read_buf(buf).contains(needle) {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "timeout waiting for {needle:?} in output:\n{}",
                read_buf(buf)
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_output_order(
    buf: &Arc<Mutex<Vec<u8>>>,
    first: &str,
    second: &str,
    timeout: Duration,
) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let snapshot = read_buf(buf);
        if let Some(first_pos) = snapshot.find(first)
            && let Some(second_pos) = snapshot.find(second)
        {
            return first_pos < second_pos;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Variant that returns true on the first matching needle, false on timeout.
async fn wait_for_any_substr(
    buf: &Arc<Mutex<Vec<u8>>>,
    needles: &[&str],
    timeout: Duration,
) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let snapshot = read_buf(buf);
        if needles.iter().any(|n| snapshot.contains(n)) {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A python one-liner that opens `port`, blocks until something connects, and
/// then returns — for services that must reach `ready` and *then* exit.
///
/// `accept()` rather than a sleep on purpose. The service's whole job here is
/// to stay alive across exactly one event: don's TCP ready check connecting.
/// A fixed sleep guesses how long that takes, and a guess that's too short on a
/// loaded machine makes the service exit before don ever observes it ready —
/// which surfaces as "timeout waiting for ready (tcp)" and looks like a runner
/// bug rather than a test that ran out of patience.
fn listen_until_ready_check(port: u16) -> String {
    format!(
        "\nimport socket\ns = socket.socket()\ns.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\ns.bind(('127.0.0.1', {port}))\ns.listen(1)\ns.accept()\n"
    )
}

// --- Crash detection + on_failure policy ---

#[test]
fn integration_clean_exit_status_zero_marks_stopped_not_failed() {
    use std::os::unix::fs::PermissionsExt;

    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("clean-exit");
        let port = free_port();

        // Service that opens its ready port, waits for the ready check to
        // actually connect, then exits 0 — i.e. simulates a long-running
        // service that decides to terminate cleanly.
        let script = dir.path().join("script.sh");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\n\
                 python3 -c \"{listen_until_ready_check}\" \n\
                 exit 0\n",
                listen_until_ready_check = listen_until_ready_check(port)
            ),
        )
        .unwrap();
        std::fs::set_permissions(&script, PermissionsExt::from_mode(0o755)).unwrap();

        // on_failure = "restart" — but exit 0 should bypass the restart
        // policy entirely, so we should *not* see auto-restart fire.
        let toml = format!(
            r#"
[services.cleanly]
run.cmd = "{}"
log = "ignore"
on_failure = "restart"

[services.cleanly.ready]
tcp = "127.0.0.1:{port}"
interval = "100ms"
retries = 30
"#,
            script.display()
        );

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        wait_for_substr(&buf, "ready (tcp", Duration::from_secs(8)).await;
        wait_for_substr(&buf, "exited cleanly (status 0)", Duration::from_secs(5)).await;

        // Give the system a beat to make sure no auto-restart sneaks in.
        tokio::time::sleep(Duration::from_secs(2)).await;

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();

        let output = read_buf(&buf);
        assert!(
            !output.contains("auto-restart"),
            "exit 0 must not trigger auto-restart even with on_failure=restart: {output}"
        );
        assert!(
            !output.contains("exited unexpectedly"),
            "exit 0 must not be reported as unexpected: {output}"
        );
    });
}

#[test]
fn integration_crash_triggers_auto_restart_when_on_failure_restart() {
    use std::os::unix::fs::PermissionsExt;

    run_with_timeout(Duration::from_secs(20), async {
        let dir = TempDir::new("crash-restart");
        let port = free_port();

        // Tracks how many times the service was launched. Each launch
        // crashes (exit 7) after the ready check passes — proving that
        // on_failure = restart actually re-spawns after a crash.
        let counter = dir.path().join("launches");
        std::fs::write(&counter, "0").unwrap();

        let script = dir.path().join("crash-and-count.sh");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\n\
                 N=$(cat {ctr})\n\
                 N=$((N + 1))\n\
                 echo $N > {ctr}\n\
                 python3 -c \"{listen_until_ready_check}\" \n\
                 exit 7\n",
                ctr = counter.display(),
                listen_until_ready_check = listen_until_ready_check(port)
            ),
        )
        .unwrap();
        std::fs::set_permissions(&script, PermissionsExt::from_mode(0o755)).unwrap();

        let toml = format!(
            r#"
[services.crashy]
run.cmd = "{}"
log = "ignore"
on_failure = "restart"

[services.crashy.ready]
tcp = "127.0.0.1:{port}"
interval = "100ms"
retries = 30
"#,
            script.display()
        );

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // First launch reaches ready, then exits 7 → handler emits the
        // unexpected-exit event AND schedules an auto-restart at attempt 1
        // (1s backoff). The "auto-restart firing" event then fires.
        wait_for_substr(
            &buf,
            "exited unexpectedly with status 7",
            Duration::from_secs(8),
        )
        .await;
        wait_for_substr(
            &buf,
            "auto-restart firing (attempt 1)",
            Duration::from_secs(5),
        )
        .await;

        // Give the new instance time to launch and the script time to bump
        // the counter past 1 — proving an actual respawn happened.
        tokio::time::sleep(Duration::from_secs(2)).await;

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();

        let launches: i32 = std::fs::read_to_string(&counter)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert!(
            launches >= 2,
            "expected the auto-restart to launch the script at least twice, got {launches}"
        );
        // And *bounded*. A lower bound alone would pass under an infinite
        // respawn loop, which is exactly what a permission level that is
        // sticky across a crash would produce if demand were not one-shot
        // (see `crate::gate` and `Demand`). The crash-loop guard gives up
        // after MAX_RAPID_CRASHES, so the script cannot run many times.
        assert!(
            launches <= 4,
            "expected the crash-loop guard to bound respawns, got {launches}"
        );
    });
}

// --- Crash detection ---

#[test]
fn integration_crash_after_ready_marks_failed_with_exit_code() {
    use std::os::unix::fs::PermissionsExt;

    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("crash-detect");

        // A service that:
        //   1. opens the ready-check port,
        //   2. stays up until the ready check connects, so we observe Ready,
        //   3. exits with status 42.
        // Using `sh -c` keeps the script self-contained and exit-code-honest
        // (no signal complications).
        let port = free_port();
        let crash_script = dir.path().join("crash.sh");
        std::fs::write(
            &crash_script,
            format!(
                "#!/bin/sh\n\
                 python3 -c \"{listen_until_ready_check}\" \n\
                 exit 42\n",
                listen_until_ready_check = listen_until_ready_check(port)
            ),
        )
        .unwrap();
        std::fs::set_permissions(&crash_script, PermissionsExt::from_mode(0o755)).unwrap();

        let toml = ConfigBuilder::new()
            .add_custom_service("crashy", crash_script.to_str().unwrap(), &[])
            .ready_tcp_with(&format!("127.0.0.1:{port}"), "100ms", 30)
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Wait for ready, then for the unexpected-exit lifecycle event.
        wait_for_substr(&buf, "ready (tcp", Duration::from_secs(8)).await;
        wait_for_substr(
            &buf,
            "exited unexpectedly with status 42",
            Duration::from_secs(8),
        )
        .await;

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();

        let output = read_buf(&buf);
        let ready_pos = output.find("ready (tcp").expect("ready event");
        let exit_pos = output[ready_pos..]
            .find("exited unexpectedly with status 42")
            .expect("exit event after ready");
        // Sanity: the exit event must come after Ready. (find returns
        // an offset within the slice — non-zero means it followed.)
        assert!(exit_pos > 0, "crash event should follow ready: {output}");
    });
}

#[test]
fn integration_graceful_stop_does_not_emit_crash_event() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("graceful-stop-no-crash");
        let port = free_port();

        let listen_script = dir.path().join("listen.sh");
        std::fs::write(
            &listen_script,
            format!(
                "#!/bin/sh\nexec python3 -c \"\nimport socket, time\ns = socket.socket()\ns.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\ns.bind(('127.0.0.1', {port}))\ns.listen(1)\nwhile True: time.sleep(60)\n\"\n"
            ),
        )
        .unwrap();
        std::fs::set_permissions(
            &listen_script,
            std::os::unix::fs::PermissionsExt::from_mode(0o755),
        )
        .unwrap();

        let toml = ConfigBuilder::new()
            .add_custom_service("svc", listen_script.to_str().unwrap(), &[])
            .ready_tcp_with(&format!("127.0.0.1:{port}"), "100ms", 30)
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        wait_for_substr(&buf, "ready (tcp", Duration::from_secs(8)).await;

        // Graceful runner shutdown — service is killed by stop_service,
        // which reaps the child itself. The crash watcher's EOF arrives
        // after the runner already transitioned the service away from
        // Ready/Unhealthy, so the handler must short-circuit and not log
        // an "exited unexpectedly" event.
        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();

        let output = read_buf(&buf);
        assert!(
            !output.contains("exited unexpectedly"),
            "graceful shutdown should not log a crash event: {output}"
        );
    });
}

// --- Ready check exhaustion ---

#[test]
fn integration_ready_check_exhausted() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("ready-exhausted");

        let toml = ConfigBuilder::new()
            .add_custom_service("badsvc", "sleep", &["300"])
            .ready_exec_with("false", &[], "100ms", 3)
            .log("ignore")
            .done()
            .add_custom_service("dependent", "sleep", &["300"])
            .depends_on(&["badsvc"])
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        tokio::time::sleep(Duration::from_secs(5)).await;
        let _ = shutdown_tx.send(()).await;

        handle.await.unwrap();
        let output = read_buf(&buf);

        assert!(
            output.contains("badsvc") && output.contains("retries"),
            "badsvc should show retry exhaustion: {output}"
        );
        assert!(
            output.contains("dependent") && output.contains("dependency 'badsvc' failed"),
            "dependent should be skipped: {output}"
        );
    });
}

#[test]
fn integration_restart_failed_ready_check_stops_live_process_first() {
    run_with_timeout(Duration::from_secs(20), async {
        let dir = TempDir::new("restart-failed-ready");
        let ready_file = dir.path().join("ready");

        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["300"])
            .log("ignore")
            .done()
            .add_custom_service("badsvc", "sleep", &["300"])
            .ready_exec_with("test", &["-f", ready_file.to_str().unwrap()], "100ms", 3)
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let cmd_tx = runner.command_sender();
        let control = runner.process_control();
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        wait_for_substr(&buf, "badsvc", Duration::from_secs(8)).await;
        wait_for_substr(&buf, "retries", Duration::from_secs(8)).await;

        std::fs::write(&ready_file, "ok").unwrap();
        let restarted = control.restart("badsvc").await.unwrap();
        assert!(
            restarted.is_ok(),
            "manual restart should accept a failed service"
        );

        wait_for_substr(
            &buf,
            "badsvc: stopping... (requested restart)",
            Duration::from_secs(5),
        )
        .await;
        let deadline = std::time::Instant::now() + Duration::from_secs(8);
        let mut reached_ready = false;
        while std::time::Instant::now() < deadline {
            let (status_tx, status_rx) = oneshot::channel();
            cmd_tx
                .send(RunnerCommand::Status {
                    verbose: false,
                    name: None,
                    reply: status_tx,
                })
                .unwrap();
            let statuses = status_rx.await.unwrap();
            reached_ready = statuses.iter().any(|item| {
                matches!(
                    item,
                    ProcessStatus::Service {
                        name,
                        state: ServiceState::Ready,
                        ..
                    } if name == "badsvc"
                )
            });
            if reached_ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            reached_ready,
            "badsvc should reach Ready after manual restart. output: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_startup_failures_backoff_then_give_up() {
    use std::os::unix::fs::PermissionsExt;

    run_with_timeout(Duration::from_secs(20), async {
        let dir = TempDir::new("startup-restart-give-up");
        let counter = dir.path().join("launches");
        std::fs::write(&counter, "0").unwrap();

        let script = dir.path().join("start-and-count.sh");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\n\
                 N=$(cat {ctr})\n\
                 N=$((N + 1))\n\
                 echo $N > {ctr}\n\
                 sleep 60\n",
                ctr = counter.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&script, PermissionsExt::from_mode(0o755)).unwrap();

        let toml = format!(
            r#"
[services.flaky]
run.cmd = "{}"
log = "ignore"
on_failure = "restart"

[services.flaky.ready]
exec.cmd = "false"
interval = "50ms"
retries = 1
"#,
            script.display()
        );

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        wait_for_substr(
            &buf,
            "auto-restart in 1s (attempt 1)",
            Duration::from_secs(5),
        )
        .await;
        wait_for_substr(
            &buf,
            "auto-restart firing (attempt 1)",
            Duration::from_secs(5),
        )
        .await;
        wait_for_substr(
            &buf,
            "auto-restart in 2s (attempt 2)",
            Duration::from_secs(5),
        )
        .await;
        wait_for_substr(
            &buf,
            "auto-restart firing (attempt 2)",
            Duration::from_secs(5),
        )
        .await;
        wait_for_substr(
            &buf,
            "giving up after 3 failed starts without becoming ready",
            Duration::from_secs(5),
        )
        .await;

        // Give a stale attempt-3 timer a chance to fire if one was scheduled.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();

        let output = read_buf(&buf);
        assert!(
            !output.contains("auto-restart firing (attempt 3)"),
            "attempt 3 should give up instead of scheduling another restart: {output}"
        );
        let launches: i32 = std::fs::read_to_string(&counter)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert_eq!(
            launches, 3,
            "initial start plus two retries should produce exactly three failed starts"
        );
    });
}

#[test]
fn integration_rapid_crash_loop_gives_up_after_two_starts() {
    use std::os::unix::fs::PermissionsExt;

    run_with_timeout(Duration::from_secs(20), async {
        let dir = TempDir::new("rapid-crash-give-up");
        let counter = dir.path().join("launches");
        std::fs::write(&counter, "0").unwrap();

        // Becomes "ready" instantly (no ready check), runs briefly so the
        // Ready transition is processed, then crashes inside the 5s
        // rapid-crash window. Two such fast crashes should trip the
        // crash-loop ceiling regardless of the unlimited `restart` policy.
        let script = dir.path().join("crash-fast.sh");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\n\
                 N=$(cat {ctr})\n\
                 N=$((N + 1))\n\
                 echo $N > {ctr}\n\
                 sleep 0.3\n\
                 exit 1\n",
                ctr = counter.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&script, PermissionsExt::from_mode(0o755)).unwrap();

        let toml = format!(
            r#"
[services.crasher]
run.cmd = "{}"
log = "ignore"
on_failure = "restart"
"#,
            script.display()
        );

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        wait_for_substr(
            &buf,
            "crashed within 5s of starting 2 times in a row — giving up",
            Duration::from_secs(12),
        )
        .await;

        // Give any stale backoff timer a chance to (wrongly) fire a 3rd start.
        tokio::time::sleep(Duration::from_secs(2)).await;

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
            "initial start plus one retry should produce exactly two fast crashes \
             before giving up. output: {}",
            read_buf(&buf)
        );
    });
}

#[test]
fn integration_restart_crashed_service_without_ready_check() {
    run_with_timeout(Duration::from_secs(20), async {
        let dir = TempDir::new("restart-crashed-no-ready");
        let gate_file = dir.path().join("keep-running");
        let launches = dir.path().join("launches");
        let script = dir.path().join("crash-until-gated.sh");

        std::fs::write(&launches, "0").unwrap();
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\n\
                 N=$(cat {launches})\n\
                 N=$((N + 1))\n\
                 echo $N > {launches}\n\
                 if [ ! -f {gate} ]; then\n\
                   exit 7\n\
                 fi\n\
                 exec sleep 300\n",
                launches = launches.display(),
                gate = gate_file.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&script, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();

        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["300"])
            .log("ignore")
            .done()
            .add_custom_service("crashy", script.to_str().unwrap(), &[])
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let cmd_tx = runner.command_sender();
        let control = runner.process_control();
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        wait_for_substr(
            &buf,
            "crashy: exited unexpectedly with status 7",
            Duration::from_secs(8),
        )
        .await;

        std::fs::write(&gate_file, "ok").unwrap();
        let restarted = control.restart("crashy").await.unwrap();
        assert!(
            restarted.is_ok(),
            "manual restart should accept a crashed service"
        );

        wait_for_substr(&buf, "crashy: starting", Duration::from_secs(5)).await;

        let deadline = std::time::Instant::now() + Duration::from_secs(8);
        let mut reached_ready = false;
        while std::time::Instant::now() < deadline {
            let (status_tx, status_rx) = oneshot::channel();
            cmd_tx
                .send(RunnerCommand::Status {
                    verbose: false,
                    name: None,
                    reply: status_tx,
                })
                .unwrap();
            let statuses = status_rx.await.unwrap();
            reached_ready = statuses.iter().any(|item| {
                matches!(
                    item,
                    ProcessStatus::Service {
                        name,
                        state: ServiceState::Ready,
                        ..
                    } if name == "crashy"
                )
            });
            if reached_ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            reached_ready,
            "crashy should be Ready after restart. output: {}",
            read_buf(&buf)
        );
        assert_eq!(std::fs::read_to_string(&launches).unwrap().trim(), "2");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// A non-blocking (`blocking = false`) dependency orders startup without
/// gating on success: when it fails, the dependent starts anyway while a
/// sibling that depends on it the normal way is skipped.
#[test]
fn integration_non_blocking_dependency_failure_does_not_block_dependent() {
    run_with_timeout(Duration::from_secs(30), async {
        let dir = TempDir::new("optional-dep-failure");

        let toml = ConfigBuilder::new()
            .add_task("seed", "bash", &["-c", "echo SEED_FAILING; exit 1"])
            .auto_run_mode("always")
            .done()
            .add_custom_service("api", "sleep", &["300"])
            .depends_on_kinds(&[("seed", false)])
            .log("ignore")
            .done()
            .add_custom_service("worker", "sleep", &["300"])
            .depends_on(&["seed"])
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let cmd_tx = runner.command_sender();
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        wait_for_substr(
            &buf,
            "worker: skipped (dependency 'seed' failed)",
            Duration::from_secs(10),
        )
        .await;
        wait_for_substr(
            &buf,
            "api: starting without non-blocking dependency 'seed'",
            Duration::from_secs(10),
        )
        .await;
        wait_for_substr(&buf, "api: started", Duration::from_secs(10)).await;

        let (status_tx, status_rx) = oneshot::channel();
        cmd_tx
            .send(RunnerCommand::Status {
                verbose: false,
                name: Some("api".to_string()),
                reply: status_tx,
            })
            .unwrap();
        let statuses = status_rx.await.unwrap();
        assert!(
            statuses.iter().any(|item| matches!(
                item,
                ProcessStatus::Service {
                    state: ServiceState::Ready | ServiceState::Running,
                    ..
                }
            )),
            "api should be running despite its non-blocking dependency failing: {statuses:?}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// A task that only ever runs on a manual trigger has settled as far as a
/// non-blocking dependent is concerned — waiting for it would mean waiting
/// forever. It must also not be parked as "required by dependents" when the
/// only thing pointing at it is a non-blocking edge.
#[test]
fn integration_non_blocking_dependency_on_manual_task_does_not_block() {
    run_with_timeout(Duration::from_secs(30), async {
        let dir = TempDir::new("optional-dep-manual-task");

        let toml = ConfigBuilder::new()
            .add_task("seed", "bash", &["-c", "echo SEED_RAN"])
            .auto_run(false)
            .done()
            .add_custom_service("api", "sleep", &["300"])
            .depends_on_kinds(&[("seed", false)])
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        wait_for_substr(&buf, "api: started", Duration::from_secs(10)).await;
        assert!(
            !read_buf(&buf).contains("SEED_RAN"),
            "a manual task must not be auto-run for a non-blocking dependent. output: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// Stopping a service settles a non-blocking edge, and the dependent has to
/// be swept promptly — not left pending until something unrelated happens.
#[test]
fn integration_non_blocking_dependency_unblocks_when_dependency_is_stopped() {
    run_with_timeout(Duration::from_secs(30), async {
        let dir = TempDir::new("optional-dep-stopped");
        // Nothing ever listens here, so `dep` stays Running and un-ready for
        // the whole test: the optional gate is genuinely closed until we stop it.
        let unused_port = free_port();

        let toml = ConfigBuilder::new()
            .add_custom_service("dep", "sleep", &["300"])
            .ready_tcp_with(&format!("127.0.0.1:{unused_port}"), "1s", 60)
            .log("ignore")
            .done()
            .add_custom_service("api", "sleep", &["300"])
            .depends_on_kinds(&[("dep", false)])
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let control = runner.process_control();
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // `dep` is up but not ready, so `api` waits.
        wait_for_substr(&buf, "dep: starting", Duration::from_secs(10)).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            !read_buf(&buf).contains("api: starting"),
            "api should wait while its non-blocking dependency is still coming up. output: {}",
            read_buf(&buf)
        );

        assert!(
            control.stop("dep").await.unwrap().is_ok(),
            "stopping dep should succeed"
        );

        wait_for_substr(&buf, "api: started", Duration::from_secs(10)).await;

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// Non-blocking is not the same as ignored: the dependent still starts
/// *after* the dependency settles.
#[test]
fn integration_non_blocking_dependency_still_orders_startup() {
    run_with_timeout(Duration::from_secs(30), async {
        let dir = TempDir::new("optional-dep-ordering");

        let toml = ConfigBuilder::new()
            .add_task("seed", "bash", &["-c", "sleep 1; echo SEED_DONE"])
            .auto_run_mode("always")
            .done()
            .add_custom_service("api", "sleep", &["300"])
            .depends_on_kinds(&[("seed", false)])
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        wait_for_substr(&buf, "api: started", Duration::from_secs(15)).await;

        let output = read_buf(&buf);
        let seed_done = output
            .find("SEED_DONE")
            .unwrap_or_else(|| panic!("seed should have run. output: {output}"));
        let api_started = output
            .find("api: starting")
            .unwrap_or_else(|| panic!("api should have started. output: {output}"));
        assert!(
            seed_done < api_started,
            "api should start only after its non-blocking dependency finished. output: {output}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_dependency_failed_service_recovers_downstream_start() {
    run_with_timeout(Duration::from_secs(30), async {
        let dir = TempDir::new("dep-failed-recovery");
        let port = free_port();
        let gate_file = dir.path().join("allow-start");
        let service_script = dir.path().join("serve-when-enabled.sh");

        std::fs::write(
            &service_script,
            "#!/bin/sh\n\
             if [ ! -f \"$1\" ]; then\n\
               exit 1\n\
             fi\n\
             exec python3 -c \"\n\
import socket, sys, time\n\
s = socket.socket()\n\
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\n\
s.bind(('127.0.0.1', int(sys.argv[1])))\n\
s.listen(1)\n\
while True: time.sleep(60)\n\
\" \"$2\"\n",
        )
        .unwrap();
        std::fs::set_permissions(
            &service_script,
            std::os::unix::fs::PermissionsExt::from_mode(0o755),
        )
        .unwrap();

        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["300"])
            .log("ignore")
            .done()
            .add_custom_service(
                "db",
                service_script.to_str().unwrap(),
                &[gate_file.to_str().unwrap(), &port.to_string()],
            )
            .ready_tcp_with(&format!("127.0.0.1:{port}"), "100ms", 10)
            .log("ignore")
            .done()
            .add_custom_service("api", "sleep", &["300"])
            .depends_on(&["db"])
            .log("ignore")
            .done()
            .add_custom_service("web", "sleep", &["300"])
            .depends_on(&["api"])
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let cmd_tx = runner.command_sender();
        let control = runner.process_control();
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        wait_for_substr(
            &buf,
            "api: skipped (dependency 'db' failed)",
            Duration::from_secs(8),
        )
        .await;

        let (status_tx, status_rx) = oneshot::channel();
        cmd_tx
            .send(RunnerCommand::Status {
                verbose: false,
                name: Some("api".to_string()),
                reply: status_tx,
            })
            .unwrap();
        let statuses = status_rx.await.unwrap();
        assert!(
            statuses.iter().any(|item| matches!(
                item,
                ProcessStatus::Service {
                    state: ServiceState::DependencyFailed,
                    failed_dependencies,
                    ..
                } if failed_dependencies == &["db".to_string()]
            )),
            "api status should name db as its failed dependency: {statuses:?}"
        );

        wait_for_substr(
            &buf,
            "web: skipped (dependency 'db' failed)",
            Duration::from_secs(8),
        )
        .await;

        std::fs::write(&gate_file, "ok").unwrap();
        let restart_result = control.restart("db").await.unwrap();
        assert!(
            restart_result.is_ok(),
            "manual db restart should succeed, got {restart_result:?}. output: {}",
            read_buf(&buf)
        );

        wait_for_substr(&buf, "api: starting", Duration::from_secs(15)).await;
        wait_for_substr(&buf, "api: started", Duration::from_secs(15)).await;
        wait_for_substr(&buf, "web: started", Duration::from_secs(15)).await;

        let (status_tx, status_rx) = oneshot::channel();
        cmd_tx
            .send(RunnerCommand::Status {
                verbose: false,
                name: Some("api".to_string()),
                reply: status_tx,
            })
            .unwrap();
        let statuses = status_rx.await.unwrap();
        assert!(
            statuses.iter().any(|item| matches!(
                item,
                ProcessStatus::Service {
                    state: ServiceState::Ready,
                    failed_dependencies,
                    ..
                } if failed_dependencies.is_empty()
            )),
            "api should clear its failed dependency after recovery: {statuses:?}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_dependency_failure_refreshes_while_item_remains_blocked() {
    run_with_timeout(Duration::from_secs(30), async {
        let dir = TempDir::new("dep-failed-refresh");
        let db_gate = dir.path().join("db-ready");
        let cache_gate = dir.path().join("cache-ready");
        let cache_runs = dir.path().join("cache-runs");

        let db_cmd = format!(
            "if [ ! -f {gate} ]; then echo DB_FAIL; exit 1; fi; \
             echo DB_RECOVERING; sleep 2; echo DB_OK",
            gate = db_gate.display()
        );
        let cache_cmd = format!(
            "N=$(cat {runs} 2>/dev/null || echo 0); N=$((N + 1)); echo $N > {runs}; \
             if [ $N -gt 1 ] && [ ! -f {gate} ]; then echo CACHE_FAIL; exit 1; fi; \
             echo CACHE_OK",
            runs = cache_runs.display(),
            gate = cache_gate.display()
        );
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["300"])
            .log("ignore")
            .done()
            .add_task("db", "bash", &["-c", &db_cmd])
            .auto_run_mode("always")
            .done()
            .add_task("cache", "bash", &["-c", &cache_cmd])
            .auto_run_mode("always")
            .done()
            .add_custom_service("api", "sleep", &["300"])
            .depends_on(&["db", "cache"])
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let cmd_tx = runner.command_sender();
        let control = runner.process_control();
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        wait_for_substr(
            &buf,
            "api: skipped (dependency 'db' failed)",
            Duration::from_secs(8),
        )
        .await;

        std::fs::write(&db_gate, "ok").unwrap();
        tokio::spawn({
            let control = control.clone();
            async move { control.restart("db").await }
        });
        wait_for_substr(&buf, "DB_RECOVERING", Duration::from_secs(5)).await;

        let mut cleared_recovered_root = false;
        for _ in 0..20 {
            let (status_tx, status_rx) = oneshot::channel();
            cmd_tx
                .send(RunnerCommand::Status {
                    verbose: false,
                    name: Some("api".to_string()),
                    reply: status_tx,
                })
                .unwrap();
            let statuses = status_rx.await.unwrap();
            cleared_recovered_root = statuses.iter().any(|item| {
                matches!(
                    item,
                    ProcessStatus::Service {
                        state: ServiceState::Pending,
                        failed_dependencies,
                        ..
                    } if failed_dependencies.is_empty()
                )
            });
            if cleared_recovered_root {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            cleared_recovered_root,
            "api should stop naming recovered db while db is still running: {}",
            read_buf(&buf)
        );

        tokio::spawn({
            let control = control.clone();
            async move { control.restart("cache").await }
        });
        wait_for_substr(&buf, "CACHE_FAIL", Duration::from_secs(5)).await;

        let mut refreshed = false;
        for _ in 0..50 {
            let (status_tx, status_rx) = oneshot::channel();
            cmd_tx
                .send(RunnerCommand::Status {
                    verbose: false,
                    name: Some("api".to_string()),
                    reply: status_tx,
                })
                .unwrap();
            let statuses = status_rx.await.unwrap();
            refreshed = statuses.iter().any(|item| {
                matches!(
                    item,
                    ProcessStatus::Service {
                        state: ServiceState::DependencyFailed,
                        failed_dependencies,
                        ..
                    } if failed_dependencies == &["cache".to_string()]
                )
            });
            if refreshed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            refreshed,
            "api should replace recovered db with newly failed cache: {}",
            read_buf(&buf)
        );

        std::fs::write(&cache_gate, "ok").unwrap();
        tokio::spawn({
            let control = control.clone();
            async move { control.restart("cache").await }
        });
        wait_for_substr(&buf, "api: started", Duration::from_secs(10)).await;

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Task with watch files: skip on no changes ---

#[test]
fn integration_task_watch_skip() {
    run_with_timeout(Duration::from_secs(15), async {
        struct Case {
            name: &'static str,
            verbose: bool,
            should_log_skip: bool,
        }

        let cases = vec![
            Case {
                name: "default",
                verbose: false,
                should_log_skip: false,
            },
            Case {
                name: "verbose",
                verbose: true,
                should_log_skip: true,
            },
        ];

        for case in cases {
            let dir = TempDir::new(&format!("task-watch-skip-{}", case.name));

            std::fs::write(dir.path().join("data.sql"), "CREATE TABLE test;").unwrap();

            let task_state = don::TaskStateStore::new(dir.path().join(".don").join("task-state"));
            let patterns = vec![format!("{}/*.sql", dir.path().display())];
            task_state
                .record_success("migrate", &patterns, &[], None)
                .await
                .unwrap();

            let toml = ConfigBuilder::new()
                .add_task("migrate", "echo", &["running migration"])
                .watch(&[&format!("{}/*.sql", dir.path().display())])
                .done()
                .build();

            // Task-only config — runner exits on its own when no services remain.
            let (runner, _shutdown_tx, buf) =
                make_runner_verbose(&toml, dir.path(), case.verbose).await;
            runner.run().await.unwrap();

            let output = read_buf(&buf);
            assert_eq!(
                output.contains("skipped (no changes)"),
                case.should_log_skip,
                "unexpected skip logging for case {}: {output}",
                case.name
            );
        }
    });
}

#[test]
fn integration_task_global_watch_ignore_skip() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-watch-global-ignore-skip");
        let generated_dir = dir.path().join("generated");
        std::fs::create_dir_all(&generated_dir).unwrap();
        std::fs::write(generated_dir.join("data.sql"), "CREATE TABLE test;").unwrap();

        let task_state = don::TaskStateStore::new(dir.path().join(".don").join("task-state"));
        let patterns = vec![format!("{}/**/*.sql", dir.path().display())];
        let ignore_patterns = vec![format!("{}/generated/**", dir.path().display())];
        task_state
            .record_success("migrate", &patterns, &ignore_patterns, None)
            .await
            .unwrap();

        std::fs::write(generated_dir.join("data.sql"), "CREATE TABLE test_v2;").unwrap();

        let toml = ConfigBuilder::new()
            .watch_ignore(&["generated/**"])
            .add_task("migrate", "echo", &["running migration"])
            .watch(&["**/*.sql"])
            .done()
            .build();

        let (runner, _shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        runner.run().await.unwrap();

        let output = read_buf(&buf);
        assert!(
            !output.contains("skipped (no changes)"),
            "task skip should be hidden without verbose logging: {output}"
        );
    });
}

// --- Task with watch files: run on changes ---

#[test]
fn integration_task_watch_run_on_change() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-watch-run");

        std::fs::write(dir.path().join("data.sql"), "CREATE TABLE test;").unwrap();

        let task_state = don::TaskStateStore::new(dir.path().join(".don").join("task-state"));
        let patterns = vec![format!("{}/*.sql", dir.path().display())];
        task_state
            .record_success("migrate", &patterns, &[], None)
            .await
            .unwrap();

        std::fs::write(dir.path().join("data.sql"), "CREATE TABLE test_v2;").unwrap();

        let toml = ConfigBuilder::new()
            .add_task("migrate", "echo", &["running migration"])
            .watch(&[&format!("{}/*.sql", dir.path().display())])
            .done()
            .build();

        let (runner, _shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        runner.run().await.unwrap();

        let output = read_buf(&buf);
        assert!(
            output.contains("migrate: running") && !output.contains("skipped"),
            "task should run when files changed: {output}"
        );
        assert!(
            output.contains("migrate: complete"),
            "task should complete: {output}"
        );
    });
}

// --- Task timeout ---

#[test]
fn integration_task_timeout() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-timeout");

        let toml = ConfigBuilder::new()
            .add_task("slow-task", "sleep", &["300"])
            .timeout("1s")
            .done()
            .build();

        let (runner, _shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let start = std::time::Instant::now();
        runner.run().await.unwrap();
        let elapsed = start.elapsed();

        let output = read_buf(&buf);
        assert!(
            output.contains("slow-task") && output.contains("failed"),
            "timed out task should be reported as failed: {output}"
        );

        assert!(
            elapsed < Duration::from_secs(10),
            "should have timed out quickly, took {elapsed:?}"
        );
    });
}

// --- HTTP ready check ---

#[test]
fn integration_http_ready_check() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("http-ready");
        let port = free_port();

        let server_script = dir.path().join("server.sh");
        std::fs::write(
            &server_script,
            format!(
                "#!/bin/sh\nexec python3 -c \"\nfrom http.server import HTTPServer, BaseHTTPRequestHandler\nclass H(BaseHTTPRequestHandler):\n    def do_GET(self):\n        self.send_response(200)\n        self.end_headers()\n        self.wfile.write(b'ok')\n    def log_message(self, format, *args): pass\nHTTPServer(('127.0.0.1', {port}), H).serve_forever()\n\"\n"
            ),
        )
        .unwrap();
        std::fs::set_permissions(
            &server_script,
            std::os::unix::fs::PermissionsExt::from_mode(0o755),
        )
        .unwrap();

        let toml = ConfigBuilder::new()
            .add_custom_service("httpsvc", server_script.to_str().unwrap(), &[])
            .ready_http_with(&format!("http://127.0.0.1:{port}/"), "200ms", 30)
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        tokio::time::sleep(Duration::from_secs(5)).await;
        let _ = shutdown_tx.send(()).await;

        handle.await.unwrap();
        let output = read_buf(&buf);

        assert!(
            output.contains("httpsvc") && output.contains("ready"),
            "HTTP ready check should pass: {output}"
        );
    });
}

// --- Don PID file prevents double start ---

#[test]
fn integration_don_pid_file_prevents_double_start() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("don-pid-double");

        let toml = ConfigBuilder::new()
            .add_custom_service("svc", "sleep", &["1"])
            .log("ignore")
            .done()
            .build();

        let config: Config = toml.parse().unwrap();
        config.validate(PLATFORM).unwrap();

        let (writer1, _buf1) = TestBuffer::new();
        let output_manager = OutputManager::new(&[("svc", &LogConfig::Ignore)], writer1)
            .await
            .unwrap();
        let (_shutdown_tx1, shutdown_rx1) = mpsc::channel(2);

        // First runner acquires the PID file.
        let mut _runner1 = Runner::new(
            config,
            PLATFORM,
            output_manager,
            dir.path().to_path_buf(),
            None,
            shutdown_rx1,
            true,
        )
        .await
        .unwrap();
        // The runner no longer binds its own API socket; the binary does,
        // and so must anything else that wants CLI/daemon access.
        let api_shutdown = don::server::serve_for_runner(&_runner1).unwrap();
        _runner1.set_api_shutdown(api_shutdown);

        // Second runner should fail — PID file is held.
        let config2: Config = toml.parse().unwrap();
        config2.validate(PLATFORM).unwrap();
        let (writer2, _buf2) = TestBuffer::new();
        let output_manager2 = OutputManager::new(&[("svc", &LogConfig::Ignore)], writer2)
            .await
            .unwrap();
        let (_shutdown_tx2, shutdown_rx2) = mpsc::channel(2);
        let result = Runner::new(
            config2,
            PLATFORM,
            output_manager2,
            dir.path().to_path_buf(),
            None,
            shutdown_rx2,
            true,
        )
        .await;

        assert!(result.is_err(), "second runner should fail");
        let err = match result {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("already running"),
            "error should mention already running: {err}"
        );
    });
}

/// A task that fails before it spawns still has to leave a record saying so.
///
/// Without one, `.last-run.json` keeps describing the last run that reached a
/// process, so the tables read `failed` in the state column and `ok` in the
/// result column — and the failure vanishes completely on the next start,
/// where every phase begins at `Pending` and the record is read off disk.
#[test]
fn integration_task_that_never_spawns_records_its_failure() {
    run_with_timeout(Duration::from_secs(20), async {
        let dir = TempDir::new("task-prepare-failure");

        // This task ran, and worked, before today.
        let state = TaskStateStore::new(dir.path().join(".don").join("task-state"));
        state.record_success("sync", &[], &[], None).await.unwrap();
        assert!(
            state.last_run("sync").await.unwrap().unwrap().success,
            "the run before this one succeeded"
        );

        // And now its command isn't on PATH, so nothing is ever spawned.
        let toml = ConfigBuilder::new()
            .add_task("sync", "don-test-no-such-command", &[])
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, _buf) = make_runner(&toml, dir.path()).await;
        let control = runner.process_control();
        let handle = tokio::spawn(async move { runner.run().await.unwrap() });
        tokio::time::sleep(Duration::from_millis(300)).await;
        let _ = control
            .run_task("sync", std::collections::HashMap::new(), false, None)
            .await;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let recorded = loop {
            match state.last_run("sync").await.unwrap() {
                Some(run) if !run.success => break Some(run),
                _ if tokio::time::Instant::now() >= deadline => break None,
                _ => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        };

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();

        let recorded = recorded.expect("a run that never spawned must still be recorded");
        assert!(
            recorded.message.is_some(),
            "the record carries why: {recorded:?}"
        );
        assert_eq!(
            (recorded.duration_ms, recorded.exit_code),
            (None, None),
            "nothing ran, so nothing took time or exited: {recorded:?}"
        );
        // The gates are the other half. A failure that never ran must not
        // touch the success marker, or the task would stop blocking its
        // dependents on the strength of a run that didn't happen.
        assert!(
            state.has_success("sync").await.unwrap(),
            "the success marker belongs to the last run that actually worked"
        );
    });
}
