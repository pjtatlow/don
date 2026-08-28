#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod helpers;

use don::config::{Config, LogConfig, Platform};
use don::output::OutputManager;
use don::runner::Runner;
use helpers::config::ConfigBuilder;
use helpers::port::free_port;
use helpers::tempdir::TempDir;
use helpers::timeout::run_with_timeout;
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::mpsc;

const PLATFORM: Platform = Platform::LinuxX86_64;

/// Minimal HTTP-over-unix-socket client for tests. Returns (status, body).
async fn request(socket_path: &Path, method: &str, path: &str) -> (u16, String) {
    request_with_body(socket_path, method, path, None).await
}

async fn request_with_body(
    socket_path: &Path,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> (u16, String) {
    let mut stream = UnixStream::connect(socket_path).await.unwrap();
    let body = body.unwrap_or("");
    let req = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n{}",
        body.len(),
        body,
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let text = String::from_utf8_lossy(&response);
    let first_line = text.lines().next().unwrap_or("");
    let status: u16 = first_line
        .split(' ')
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("failed to parse response: {text:?}"));
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    (status, body)
}

/// Wait for the socket file to exist (server finished binding).
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

/// Poll `GET /status` until `name` reports `state`. Returns false on timeout.
///
/// The API socket is bound early in `Runner::run`, well before the startup
/// sweep has decided what to do with each task. A `POST /run/:name` issued
/// right after `wait_for_socket` therefore lands mid-sweep — which is allowed,
/// and covered on purpose by
/// `integration_run_starts_the_task_without_waiting_for_the_rest_of_startup`.
/// Tests that want to assert on what a run *did* wait for the task's settled
/// state first (`skipped` for an `auto_run = false` task with nothing to do),
/// so the outcome they read is the run's and not the sweep's.
async fn wait_for_process_state(
    socket_path: &Path,
    name: &str,
    state: &str,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let (status, body) = request(socket_path, "GET", "/status").await;
        if status == 200
            && let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&body)
            && let Some(processes) = parsed.get("processes").and_then(|i| i.as_array())
            && processes.iter().any(|process| {
                process.get("name").and_then(|n| n.as_str()) == Some(name)
                    && process.get("state").and_then(|s| s.as_str()) == Some(state)
            })
        {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Poll a GET endpoint until its body contains `needle`. Returns false on
/// timeout. Used instead of sleeping for a service's output to reach the ring
/// buffer — how long a spawn plus a first read takes is load-dependent.
async fn wait_for_body_contains(
    socket_path: &Path,
    path: &str,
    needle: &str,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let (status, body) = request(socket_path, "GET", path).await;
        if status == 200 && body.contains(needle) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Make a runner + background task. Returns the socket path, shutdown tx,
/// and join handle.
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
        if let Err(e) = runner.run().await {
            eprintln!("RUNNER ERROR: {e}");
        }
    });
    (socket_path, shutdown_tx, handle)
}

// --- Integration tests ---

#[test]
fn integration_status_endpoint_returns_processes() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("server-status");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);

        let (status, body) = request(&socket, "GET", "/status").await;
        assert_eq!(status, 200, "body: {body}");
        assert!(body.contains("\"processes\""), "body: {body}");
        assert!(body.contains("\"name\":\"keeper\""), "body: {body}");
        assert!(body.contains("\"kind\":\"service\""), "body: {body}");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_verbose_status_includes_proxy_active_connections() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("server-status-proxy-connections");
        let addr = format!("127.0.0.1:{}", free_port());
        let toml = ConfigBuilder::new()
            .add_custom_service("api", "sleep", &["60"])
            .log("ignore")
            .proxy_env(&addr, "PORT")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);

        let (status, body) = request(&socket, "GET", "/status?verbose=true").await;
        assert_eq!(status, 200, "body: {body}");
        assert!(
            body.contains("\"proxy_active_connections\":0"),
            "body: {body}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_verbose_status_summarizes_watch_paths() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("server-status-watch-summary");
        let toml = ConfigBuilder::new()
            .add_custom_service("api", "sleep", &["60"])
            .log("ignore")
            .watch(&["src/hidden-by-default.ts", "src/also-hidden.ts"])
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);

        let (status, body) = request(&socket, "GET", "/status?verbose=true").await;
        assert_eq!(status, 200, "body: {body}");
        assert!(body.contains("\"watch_count\":2"), "body: {body}");
        assert!(
            !body.contains("hidden-by-default.ts"),
            "body should not include verbose watch paths by default: {body}"
        );
        assert!(
            !body.contains("also-hidden.ts"),
            "body should not include verbose watch paths by default: {body}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_verbose_status_single_process_lists_watch_paths() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("server-status-watch-single");
        let toml = ConfigBuilder::new()
            .add_custom_service("api", "sleep", &["60"])
            .log("ignore")
            .watch(&["src/one.ts", "src/two.ts"])
            .ready_exec("true", &[])
            .done()
            .add_custom_service("web", "sleep", &["60"])
            .log("ignore")
            .watch(&["web/only.ts"])
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);

        // Drilling into a single item expands its full watch path list...
        let (status, body) = request(&socket, "GET", "/status?verbose=true&name=api").await;
        assert_eq!(status, 200, "body: {body}");
        assert!(body.contains("src/one.ts"), "body: {body}");
        assert!(body.contains("src/two.ts"), "body: {body}");
        // ...and returns only that item — no sibling service.
        assert!(!body.contains("\"name\":\"web\""), "body: {body}");
        assert!(!body.contains("web/only.ts"), "body: {body}");

        // A name that matches nothing is an empty list, not an error.
        let (status, body) = request(&socket, "GET", "/status?verbose=true&name=nope").await;
        assert_eq!(status, 200, "body: {body}");
        assert!(body.contains("\"processes\":[]"), "body: {body}");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_watch_endpoint_reports_dirs_and_patterns() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("server-watch-report");
        let toml = ConfigBuilder::new()
            .watch_ignore(&["**/node_modules/**"])
            .add_custom_service("api", "sleep", &["60"])
            .log("ignore")
            .watch(&["src/**/*.ts"])
            .ready_exec("true", &[])
            .done()
            .build();

        // Pre-create `src` with an ignored `node_modules` and a real
        // `components` dir. Every non-ignored directory is watched
        // non-recursively; the ignored `node_modules` is never watched.
        std::fs::create_dir_all(dir.path().join("src/node_modules/left-pad")).unwrap();
        std::fs::create_dir_all(dir.path().join("src/components")).unwrap();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);

        let (status, body) = request(&socket, "GET", "/watch").await;
        assert_eq!(status, 200, "body: {body}");
        // Registered watch directories for the resolved `src` watch.
        assert!(body.contains("\"directories\""), "body: {body}");

        // The registration strategy is platform-dependent (see
        // `watch::desired_watches`): Linux/inotify registers one non-recursive
        // watch per directory (so `src/components` shows up individually), while
        // macOS/FSEvents registers a single recursive watch at the glob base
        // `src` that covers the whole subtree.
        #[cfg(not(target_os = "macos"))]
        {
            // All watches are non-recursive so notify never auto-descends into a
            // runtime-created ignored dir.
            assert!(body.contains("\"mode\":\"non-recursive\""), "body: {body}");
            // The real `components` dir is watched directly.
            assert!(body.contains("components"), "body: {body}");
        }
        #[cfg(target_os = "macos")]
        {
            // One recursive watch at `src` covers `components` (and everything
            // else) without a per-directory registration.
            assert!(body.contains("\"mode\":\"recursive\""), "body: {body}");
        }

        // The ignored node_modules must not appear as a registered dir on either
        // platform: Linux prunes it from the walk; macOS never registers per-dir
        // entries under the recursive `src` watch at all.
        assert!(
            !body.contains("node_modules/left-pad"),
            "ignored dir should not be watched, body: {body}"
        );
        // The item and its (absolute) glob pattern.
        assert!(body.contains("\"name\":\"api\""), "body: {body}");
        assert!(body.contains("\"kind\":\"service\""), "body: {body}");
        assert!(body.contains("src/**/*.ts"), "body: {body}");
        // Workspace-wide ignore is reported once at the top level.
        assert!(body.contains("node_modules"), "body: {body}");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_watch_endpoint_null_when_no_watches() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("server-watch-none");
        // A service with no watch patterns registers no watches at all.
        let toml = ConfigBuilder::new()
            .add_custom_service("api", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);

        let (status, body) = request(&socket, "GET", "/watch").await;
        assert_eq!(status, 200, "body: {body}");
        assert!(body.contains("\"watch\":null"), "body: {body}");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_status_endpoint_includes_task_last_run() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("server-task-last-run");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_task("prep", "true", &[])
            .log("ignore")
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);

        let mut body = String::new();
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(3) {
            let (status, next_body) = request(&socket, "GET", "/status").await;
            assert_eq!(status, 200, "body: {next_body}");
            body = next_body;
            if body.contains("\"name\":\"prep\"") && body.contains("\"state\":\"completed\"") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert!(body.contains("\"name\":\"prep\""), "body: {body}");
        assert!(body.contains("\"state\":\"completed\""), "body: {body}");
        assert!(body.contains("\"last_run\""), "body: {body}");
        assert!(body.contains("\"success\":true"), "body: {body}");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_stop_endpoint_stops_service() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("server-stop");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        // Wait for it to actually be up, rather than hoping 300ms was enough
        // — a sleep that fires early would leave this asserting nothing.
        assert!(
            wait_for_process_state(&socket, "keeper", "ready", Duration::from_secs(5)).await,
            "keeper should be ready before we stop it"
        );

        let (status, _) = request(&socket, "POST", "/stop/keeper").await;
        assert_eq!(status, 204);

        // No sleep here, deliberately: 204 means the runner has folded
        // `Stopped`, so the very next read must already see it. If the reply
        // ever starts coming from somewhere that only knows the *process*
        // died, this is the assertion that catches it.
        let (_, body) = request(&socket, "GET", "/status").await;
        assert!(
            body.contains("\"state\":\"stopped\""),
            "service should be stopped; body: {body}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_events_endpoint_streams_state_changes() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("server-events");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        // Let startup settle so the events we assert on are the ones this
        // test triggers, not leftovers from the initial start sequence.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Trigger a stop *after* the subscriber is attached — the endpoint
        // streams live events only, so the request has to land second.
        let socket_for_stop = socket.clone();
        let stopper = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            request(&socket_for_stop, "POST", "/stop/keeper").await
        });

        // A stop produces two transitions: Stopping, then Stopped.
        let lines = follow_lines(
            &socket,
            "/events",
            "\"state\":\"stopped\"",
            Duration::from_secs(8),
        )
        .await;
        let (stop_status, _) = stopper.await.unwrap();
        assert_eq!(stop_status, 204);

        let joined = lines.join("\n");
        assert!(
            joined.contains("\"type\":\"service_state_changed\""),
            "events should be internally tagged; got: {joined}"
        );
        assert!(
            joined.contains("\"name\":\"keeper\""),
            "events should name the service; got: {joined}"
        );
        assert!(
            joined.contains("\"state\":\"stopped\""),
            "stop should reach the event stream; got: {joined}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_restart_endpoint_restarts_service() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("server-restart");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        tokio::time::sleep(Duration::from_millis(300)).await;

        let (status, _) = request(&socket, "POST", "/restart/keeper").await;
        assert_eq!(status, 204);

        // After restart, service should still be ready/running.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let (_, body) = request(&socket, "GET", "/status").await;
        assert!(
            body.contains("\"state\":\"running\"") || body.contains("\"state\":\"ready\""),
            "service should be running after restart; body: {body}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_stop_ends_a_running_task() {
    // `stop` is polymorphic like `restart`: a task's run is a process, and
    // this is the verb that ends it. A task with nothing in flight is a 409
    // ("not running"), the same answer a stopped service gives — not the 400
    // "that's a task" this used to return for every task, running or not.
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("server-task-stop");
        let out_path = dir.path().join("finished.txt");
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
                    &format!("sleep 30; echo done > {}", out_path.display()),
                ],
            )
            .log("ignore")
            .auto_run(false)
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        assert!(
            wait_for_process_state(&socket, "slow", "skipped", Duration::from_secs(3)).await,
            "task should settle before the manual run"
        );

        // Nothing in flight yet.
        let (status, body) = request(&socket, "POST", "/stop/slow").await;
        assert_eq!(status, 409, "body: {body}");
        assert!(body.contains("not running"), "body: {body}");

        let (status, body) = request_with_body(&socket, "POST", "/run/slow", Some("{}")).await;
        assert_eq!(status, 204, "body: {body}");
        assert!(
            wait_for_process_state(&socket, "slow", "running", Duration::from_secs(3)).await,
            "task should be running before we stop it"
        );

        let (status, body) = request(&socket, "POST", "/stop/slow").await;
        assert_eq!(status, 204, "body: {body}");
        assert!(
            wait_for_process_state(&socket, "slow", "pending_run", Duration::from_secs(5)).await,
            "a stopped task waits for a trigger — `running` with nothing \
             running is a row nobody can act on"
        );
        assert!(
            !out_path.exists(),
            "the sleep should have been killed, not left to finish"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_run_task_wait_endpoint_returns_after_success() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("server-run-wait-success");
        let out_path = dir.path().join("ran.txt");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_task(
                "once",
                "sh",
                &[
                    "-c",
                    &format!("sleep 0.1; echo done > {}", out_path.display()),
                ],
            )
            .log("ignore")
            .auto_run(false)
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);

        assert!(
            wait_for_process_state(&socket, "once", "skipped", Duration::from_secs(3)).await,
            "task should settle as skipped before the manual run"
        );

        let (status, body) =
            request_with_body(&socket, "POST", "/run/once", Some(r#"{"wait":true}"#)).await;
        assert_eq!(status, 204, "body: {body}");
        let captured = std::fs::read_to_string(&out_path).unwrap();
        assert_eq!(captured.trim(), "done");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_run_task_wait_endpoint_maps_task_failure_to_422() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("server-run-wait-failure");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_task("fail", "sh", &["-c", "exit 7"])
            .log("ignore")
            .auto_run(false)
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);

        assert!(
            wait_for_process_state(&socket, "fail", "skipped", Duration::from_secs(3)).await,
            "task should settle as skipped before the manual run"
        );

        let (status, body) =
            request_with_body(&socket, "POST", "/run/fail", Some(r#"{"wait":true}"#)).await;
        assert_eq!(status, 422, "body: {body}");
        assert!(body.contains("exit code 7"), "body: {body}");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_run_task_wait_timeout_returns_408_without_stopping_task() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("server-run-wait-timeout");
        let out_path = dir.path().join("ran.txt");
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
                    &format!("sleep 0.3; echo done > {}", out_path.display()),
                ],
            )
            .log("ignore")
            .auto_run(false)
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);

        assert!(
            wait_for_process_state(&socket, "slow", "skipped", Duration::from_secs(3)).await,
            "task should settle as skipped before the manual run"
        );

        let (status, body) = request_with_body(
            &socket,
            "POST",
            "/run/slow",
            Some(r#"{"wait_timeout":"50ms"}"#),
        )
        .await;
        assert_eq!(status, 408, "body: {body}");
        assert!(body.contains("did not finish within 50ms"), "body: {body}");
        assert!(
            !out_path.exists(),
            "task should still be sleeping when the wait times out"
        );

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !out_path.exists() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let captured = std::fs::read_to_string(&out_path).unwrap();
        assert_eq!(captured.trim(), "done");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_socket_permissions_are_0600() {
    use std::os::unix::fs::PermissionsExt;
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("server-perms");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);

        let mode = std::fs::metadata(&socket).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "socket should be owner-only");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_unknown_name_returns_404() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("server-404");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);

        let (status, body) = request(&socket, "POST", "/stop/ghost").await;
        assert_eq!(status, 404, "body: {body}");
        assert!(body.contains("ghost"), "body should mention name: {body}");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// Stream an NDJSON-body request until a record containing `until` has been
/// seen or `timeout` elapses. Returns the collected NDJSON records.
///
/// Stopping on a *record count* would be wrong: the follow sink forwards each
/// chunk the output pipeline produces, and a chunk is whatever a single read
/// from the service's PTY returned — one log line, several, or half of one. A
/// service that emits N lines can therefore produce more (or fewer) than N
/// NDJSON records, so the only reliable stop condition is the content the test
/// is actually waiting for.
async fn follow_lines(
    socket_path: &Path,
    path: &str,
    until: &str,
    timeout: Duration,
) -> Vec<String> {
    let mut stream = UnixStream::connect(socket_path).await.unwrap();
    let req = format!(
        "GET {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Length: 0\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let deadline = tokio::time::Instant::now() + timeout;
    let mut buffer = Vec::new();
    let mut headers_consumed = false;
    // Dechunked body bytes not yet terminated by a newline. An NDJSON record is
    // only complete at its `\n`, and nothing guarantees a record and an HTTP
    // chunk share a boundary.
    let mut pending: Vec<u8> = Vec::new();
    let mut lines: Vec<String> = Vec::new();
    loop {
        if tokio::time::Instant::now() > deadline {
            break;
        }
        let read_fut = async {
            let mut chunk = [0u8; 1024];
            let n = stream.read(&mut chunk).await.unwrap_or(0);
            if n == 0 {
                None
            } else {
                Some(chunk[..n].to_vec())
            }
        };
        let timeout_left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let chunk = match tokio::time::timeout(timeout_left, read_fut).await {
            Ok(Some(c)) => c,
            _ => break,
        };
        buffer.extend_from_slice(&chunk);

        if !headers_consumed && let Some(pos) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
            // Drain the header portion.
            buffer.drain(..pos + 4);
            headers_consumed = true;
        }
        if !headers_consumed {
            continue;
        }
        // Parse chunked transfer encoding: <size hex>\r\n<data>\r\n...
        while let Some(rn) = buffer.windows(2).position(|w| w == b"\r\n") {
            let size_str = std::str::from_utf8(&buffer[..rn]).unwrap_or("").trim();
            let size = usize::from_str_radix(size_str, 16).unwrap_or(0);
            if size == 0 {
                break;
            }
            if buffer.len() < rn + 2 + size + 2 {
                break; // not enough data yet
            }
            pending.extend_from_slice(&buffer[rn + 2..rn + 2 + size]);
            buffer.drain(..rn + 2 + size + 2);
            while let Some(nl) = pending.iter().position(|b| *b == b'\n') {
                let line_bytes: Vec<u8> = pending.drain(..nl + 1).collect();
                let text = String::from_utf8_lossy(&line_bytes[..nl]).into_owned();
                if text.is_empty() {
                    continue;
                }
                lines.push(text);
            }
        }
        if lines.iter().any(|l| l.contains(until)) {
            break;
        }
    }
    lines
}

/// A second terminal attaching to a running project sees the same scrollback
/// as the first one, not the tail of it.
///
/// The route's own default is small on purpose — it answers ad-hoc clients too
/// — so the TUI's client asks for the tap's whole capacity. Without that,
/// reattaching showed the last hundred lines of a log with thousands in it,
/// which reads as the history having been thrown away.
#[test]
fn integration_a_reattaching_client_gets_the_whole_history() {
    run_with_timeout(Duration::from_secs(30), async {
        let dir = TempDir::new("server-merged-preload");
        // Comfortably more than the route's ad-hoc default of 100.
        let toml = ConfigBuilder::new()
            .add_custom_service(
                "chatty",
                "bash",
                &[
                    "-c",
                    "for i in $(seq 1 400); do echo tick$i; done; sleep 60",
                ],
            )
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(5)).await);
        assert!(
            wait_for_body_contains(
                &socket,
                "/logs/chatty?last=5",
                "tick400",
                Duration::from_secs(10)
            )
            .await,
            "the service should have finished emitting before anyone follows"
        );

        let client = don::client::Client::with_socket_path(socket.clone());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let follower = tokio::spawn(async move {
            let _ = client
                .logs_follow_all(None, |event| {
                    if let don::client::LogStreamEvent::Line { line, .. } = &event {
                        let _ = tx.send(line.clone());
                    }
                    Ok(())
                })
                .await;
        });

        // The oldest line is the whole point: a client capped at the route's
        // default would start somewhere in the three hundreds.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let mut ticks: Vec<String> = Vec::new();
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
                Ok(Some(line)) => {
                    let line = line.trim().to_string();
                    // Only the service's own output. don's narration includes
                    // a line quoting the command, which contains "tick" too.
                    if line.starts_with("tick") {
                        let last = line == "tick400";
                        ticks.push(line);
                        if last {
                            break;
                        }
                    }
                }
                Ok(None) | Err(_) => break,
            }
        }
        follower.abort();

        assert_eq!(
            ticks.first().map(String::as_str),
            Some("tick1"),
            "the preload should start at the first line; got {} lines starting {:?}",
            ticks.len(),
            &ticks[..ticks.len().min(3)]
        );
        assert_eq!(ticks.len(), 400, "and carry all of them");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_merged_logs_follow_carries_name_and_lifecycle() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("server-merged-follow");
        // Emit continuously; ticks both before the connection (preload path)
        // and after it (live path) get asserted on.
        let toml = ConfigBuilder::new()
            .add_custom_service(
                "chatty",
                "bash",
                &[
                    "-c",
                    "for i in $(seq 1 100); do echo tick$i; sleep 0.2; done",
                ],
            )
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        // Let tick1 land before connecting, so seeing it proves the preload
        // path — a live-only stream could never deliver it.
        assert!(
            wait_for_body_contains(
                &socket,
                "/logs/chatty?last=5",
                "tick1",
                Duration::from_secs(5)
            )
            .await,
            "service should have emitted tick1 before the follower connects"
        );

        // Follow until tick3: tick1 arrives from the preload, tick3 arrives
        // live, so the stream provably crosses the preload/live boundary —
        // which is also where a dedupe failure would double a line.
        let lines = follow_lines(
            &socket,
            "/logs?follow=true",
            "tick3",
            Duration::from_secs(8),
        )
        .await;
        assert!(!lines.is_empty(), "merged follow produced no records");

        // Every record must parse and carry the structured fields — this is
        // the contract that lets a client filter and color without
        // in-process access.
        let mut saw_service_line = false;
        for line in &lines {
            let v: serde_json::Value =
                serde_json::from_str(line).unwrap_or_else(|e| panic!("not NDJSON: {line:?} ({e})"));
            if v.get("lagged").is_some() {
                continue;
            }
            assert!(v.get("name").is_some(), "missing 'name': {line}");
            assert!(v.get("lifecycle").is_some(), "missing 'lifecycle': {line}");
            let text = v["line"].as_str().unwrap_or_default();
            if v["name"] == "chatty" && v["lifecycle"] == false && text.contains("tick") {
                saw_service_line = true;
            }
        }
        assert!(
            saw_service_line,
            "expected a non-lifecycle 'chatty' tick record; got: {lines:?}"
        );
        // tick1 was emitted before the connection — only the history preload
        // can have delivered it.
        assert!(
            lines.iter().any(|l| l.contains("tick1")),
            "expected preloaded tick1; got: {lines:?}"
        );
        // And the preload/live overlap must be deduplicated: every tick
        // record appears at most once.
        for n in 1..=3 {
            let needle = format!("tick{n}\"");
            let count = lines.iter().filter(|l| l.contains(&needle)).count();
            assert!(
                count <= 1,
                "tick{n} appeared {count} times — preload/live dedupe failed: {lines:?}"
            );
        }

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_logs_follow_streams_lines() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("server-follow");
        // Emit 2 lines immediately (pre), pause 2s to let the subscriber
        // connect, then emit 3 more (live). Subscriber asks for last=2 so
        // it should see pre1/pre2 from the snapshot + live1..3 from the stream.
        let toml = ConfigBuilder::new()
            .add_custom_service(
                "chatty",
                "bash",
                &[
                    "-c",
                    "echo pre1; echo pre2; sleep 2; for i in 1 2 3; do echo live$i; sleep 0.2; done; sleep 60",
                ],
            )
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        // The follow snapshot can only replay what the ring buffer already
        // holds, so wait for pre2 to land rather than guessing at spawn time.
        assert!(
            wait_for_body_contains(
                &socket,
                "/logs/chatty?last=2",
                "pre2",
                Duration::from_secs(3)
            )
            .await,
            "service should have emitted pre1/pre2 into the ring buffer"
        );

        let lines = follow_lines(
            &socket,
            "/logs/chatty?last=2&follow=true",
            "live3",
            Duration::from_secs(5),
        )
        .await;

        // Should include preloaded tail (last=2) + live lines as they're emitted.
        let joined: String = lines.join(" | ");
        assert!(
            joined.contains("pre1"),
            "expected pre1 in snapshot; got: {joined}"
        );
        assert!(
            joined.contains("pre2"),
            "expected pre2 in snapshot; got: {joined}"
        );
        assert!(
            joined.contains("live1"),
            "expected live1 from stream; got: {joined}"
        );
        assert!(
            joined.contains("live3"),
            "expected live3 from stream; got: {joined}"
        );
        // Each line should be valid NDJSON.
        for line in &lines {
            let v: serde_json::Value =
                serde_json::from_str(line).unwrap_or_else(|e| panic!("not NDJSON: {line:?} ({e})"));
            assert!(v.get("line").is_some(), "missing 'line' field: {line}");
        }

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_logs_endpoint_returns_ring_buffer() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("server-logs");
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

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        // Wait for the service to emit its lines.
        assert!(
            wait_for_body_contains(
                &socket,
                "/logs/chatty?last=10",
                "line5",
                Duration::from_secs(3)
            )
            .await,
            "service should have emitted all five lines"
        );

        let (status, body) = request(&socket, "GET", "/logs/chatty?last=3").await;
        assert_eq!(status, 200);
        assert!(body.contains("line5"), "body: {body}");
        assert!(body.contains("line3"), "body: {body}");
        // last=3 → should NOT include line1 (oldest, evicted).
        assert!(
            !body.contains("line1"),
            "body should not include line1: {body}"
        );

        // Logs for a service with log=ignore should still be accessible
        // (ring buffer is fed regardless of log routing).
        let (status, body) = request(&socket, "GET", "/logs/chatty?last=10").await;
        assert_eq!(status, 200);
        assert!(
            body.contains("line1"),
            "ring buffer should have all lines: {body}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// "Run" means now, whatever else the workspace is doing.
///
/// Two things used to stand between pressing run and the task running, and
/// this covers both. The task carries a worker deciding skip/pending/run,
/// which read as "already running" and got the request refused. The route's
/// answer to that was to wait for the *whole* startup sweep to settle first —
/// which in a workspace whose slowest service takes minutes meant pressing run
/// and watching nothing happen for minutes.
#[test]
fn integration_run_starts_the_task_without_waiting_for_the_rest_of_startup() {
    run_with_timeout(Duration::from_secs(45), async {
        let dir = TempDir::new("server-run-during-startup");
        let out_path = dir.path().join("ran.txt");
        // The service is ready only once this appears, which is never: nothing
        // creates it. So startup cannot settle for the duration of the test,
        // and any wait on it would be the full timeout.
        let never = dir.path().join("never.txt");

        // A watch set large enough that hashing it takes measurably longer
        // than binding the API socket — that window is where a request lands
        // while the task's own worker is still deciding.
        let data = dir.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        for i in 0..3000 {
            std::fs::write(data.join(format!("f{i}.txt")), b"x").unwrap();
        }

        let toml = ConfigBuilder::new()
            .add_custom_service("slow-to-be-ready", "sleep", &["60"])
            .log("ignore")
            .ready_exec("test", &["-f", never.to_str().unwrap()])
            .done()
            .add_task(
                "chore",
                "sh",
                &["-c", &format!("echo done > {}", out_path.display())],
            )
            .log("ignore")
            .auto_run(false)
            .watch(&["data/**"])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(5)).await);

        // Deliberately no settling sleep — issuing the request into the
        // window is the whole point of the test.
        let asked_at = tokio::time::Instant::now();
        let (status, body) = request_with_body(&socket, "POST", "/run/chore", Some("{}")).await;
        assert_eq!(
            status, 204,
            "run should be admitted, not refused; body: {body}"
        );

        let deadline = asked_at + Duration::from_secs(15);
        while !out_path.exists() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            out_path.exists(),
            "the task should have run while the service was still not ready"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_ready_endpoint_reports_startup_progress() {
    run_with_timeout(Duration::from_secs(20), async {
        let dir = TempDir::new("server-ready");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(5)).await);

        // Must answer immediately whether or not startup has settled — it's a
        // status read, not a wait, and a client asking "can I act yet?" during
        // startup is exactly when it needs an answer.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let (status, body) = request(&socket, "GET", "/ready").await;
            assert_eq!(status, 200, "body: {body}");
            assert!(body.contains("startup_complete"), "body: {body}");
            if body.contains("\"startup_complete\":true") {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "startup never settled; last body: {body}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}
