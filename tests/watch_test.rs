#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod helpers;

use don::config::{Config, LogConfig, Platform};
use don::output::OutputManager;
use don::runner::Runner;
use helpers::config::ConfigBuilder;
use helpers::tempdir::TempDir;
use helpers::timeout::run_with_timeout;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

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

fn read_buf(buf: &Arc<Mutex<Vec<u8>>>) -> String {
    String::from_utf8_lossy(&buf.lock().unwrap()).into_owned()
}

async fn make_runner(
    toml: &str,
    base_dir: &std::path::Path,
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

/// Like [`make_runner`] but with verbose output enabled, so the `[don]`
/// `watch:` diagnostics are emitted (and thus assertable).
async fn make_runner_verbose(
    toml: &str,
    base_dir: &std::path::Path,
) -> (Runner, mpsc::Sender<()>, Arc<Mutex<Vec<u8>>>) {
    let config: Config = toml.parse().unwrap();
    config.validate(PLATFORM).unwrap();

    let all_configs: Vec<(&str, &LogConfig)> = config
        .services
        .iter()
        .map(|(n, s)| (n.as_str(), &s.log))
        .chain(config.tasks.iter().map(|(n, t)| (n.as_str(), &t.log)))
        .collect();

    let (writer, buf) = TestBuffer::new();
    let output_manager = OutputManager::new_verbose(&all_configs, writer, true)
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

/// Wait until the output buffer contains the given string, with a timeout.
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

// --- Integration test: service restarts on file change ---

#[test]
fn integration_service_restarts_on_file_change() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("watch-restart");

        // Create a watched file.
        let src_dir = dir.path().join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("main.rs"), "initial content").unwrap();

        // Service that prints its PID. When it restarts, a new PID appears.
        let toml = ConfigBuilder::new()
            .add_custom_service("api", "bash", &["-c", "echo PID=$$ && sleep 60"])
            .watch(&["src/**/*.rs"])
            .debounce("100ms")
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Wait for "all services running".
        assert!(
            wait_for_output(&buf, "all services running", Duration::from_secs(5)).await,
            "timed out waiting for services to start. output: {}",
            read_buf(&buf)
        );

        // Modify the watched file.
        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(src_dir.join("main.rs"), "modified content").unwrap();

        // Wait for rebuild lifecycle event.
        assert!(
            wait_for_output(&buf, "rebuilding (file changed)", Duration::from_secs(5)).await,
            "timed out waiting for rebuild. output: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// A change to a file matched by the workspace-wide `watch_ignore` must produce
/// no verbose `watch:` diagnostics at all — not even the raw event line — while
/// a change to a genuinely watched file is still reported and triggers a
/// rebuild.
#[test]
fn integration_global_watch_ignore_is_silent_in_verbose() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("watch-global-ignore");
        std::fs::create_dir_all(dir.path().join("ignored")).unwrap();
        std::fs::create_dir_all(dir.path().join("watched")).unwrap();
        std::fs::write(dir.path().join("ignored/a.txt"), "init").unwrap();
        std::fs::write(dir.path().join("watched/a.txt"), "init").unwrap();

        let toml = r#"
watch_ignore = ["**/ignored/**"]

[services.app]
run.cmd = "bash"
run.args = ["-c", "sleep 60"]
watch = ["**/*.txt"]
debounce = "100ms"
log = "ignore"
ready.exec.cmd = "true"
"#;

        let (runner, shutdown_tx, buf) = make_runner_verbose(toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "all services running", Duration::from_secs(5)).await,
            "services did not start. output: {}",
            read_buf(&buf)
        );

        // Touch the globally-ignored file first, give its event time to flow,
        // then touch the watched file. When the watched rebuild fires we know
        // the earlier ignored event has already been handled (and, with the
        // fix, dropped silently).
        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(dir.path().join("ignored/a.txt"), "changed").unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        std::fs::write(dir.path().join("watched/a.txt"), "changed").unwrap();

        assert!(
            wait_for_output(&buf, "rebuilding (file changed)", Duration::from_secs(5)).await,
            "watched file change did not trigger a rebuild. output: {}",
            read_buf(&buf)
        );

        let output = read_buf(&buf);
        // The watched file is reported; the globally-ignored file is not — no
        // "watch:" line should ever mention it.
        assert!(
            output.contains("watched/a.txt"),
            "expected the watched file to be reported. output: {output}"
        );
        assert!(
            !output.contains("ignored/a.txt"),
            "globally-ignored file produced a verbose log line. output: {output}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// A directory created at runtime under a watched directory must be picked up,
/// so files created inside it trigger a rebuild. Because all watches are
/// non-recursive, notify won't auto-watch the new dir — the create backstop has
/// to register it and replay any files that already landed inside it.
#[test]
fn integration_new_dir_at_runtime_triggers_rebuild() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("watch-new-dir-runtime");
        std::fs::create_dir_all(dir.path().join("src")).unwrap();
        std::fs::write(dir.path().join("src/main.rs"), "v1").unwrap();

        let toml = r#"
watch_ignore = ["**/node_modules/**"]

[services.api]
run.cmd = "bash"
run.args = ["-c", "sleep 60"]
watch = ["src/**/*.rs"]
debounce = "100ms"
log = "ignore"
ready.exec.cmd = "true"
"#;

        let (runner, shutdown_tx, buf) = make_runner(toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "all services running", Duration::from_secs(5)).await,
            "services did not start. output: {}",
            read_buf(&buf)
        );

        // Create a brand-new directory under `src` and drop a matching file into
        // it. The backstop must register `src/feature` and replay `new.rs`.
        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::create_dir_all(dir.path().join("src/feature")).unwrap();
        std::fs::write(dir.path().join("src/feature/new.rs"), "v1").unwrap();

        assert!(
            wait_for_output(&buf, "rebuilding (file changed)", Duration::from_secs(6)).await,
            "new file under a runtime-created directory did not trigger a rebuild. output: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// A whole *nested* directory tree created at once (e.g. `mkdir -p a/b/c` then a
/// file) must be picked up, even though only the top of the tree fires a create
/// event under a non-recursive watch. The backstop's subtree walk has to
/// register every level and replay files it finds.
#[test]
fn integration_deep_new_dir_tree_at_runtime_triggers_rebuild() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("watch-deep-new-dir");
        std::fs::create_dir_all(dir.path().join("src")).unwrap();
        std::fs::write(dir.path().join("src/main.rs"), "v1").unwrap();

        let toml = r#"
watch_ignore = ["**/node_modules/**"]

[services.api]
run.cmd = "bash"
run.args = ["-c", "sleep 60"]
watch = ["src/**/*.rs"]
debounce = "100ms"
log = "ignore"
ready.exec.cmd = "true"
"#;

        let (runner, shutdown_tx, buf) = make_runner(toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "all services running", Duration::from_secs(5)).await,
            "services did not start. output: {}",
            read_buf(&buf)
        );

        // Create several levels at once, with the matching file at the bottom.
        // Only `src` is watched at this point, so this exercises the backstop's
        // recursive registration + replay-scan, not per-level create events.
        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::create_dir_all(dir.path().join("src/a/b/c")).unwrap();
        std::fs::write(dir.path().join("src/a/b/c/deep.rs"), "v1").unwrap();

        assert!(
            wait_for_output(&buf, "rebuilding (file changed)", Duration::from_secs(6)).await,
            "file deep in a runtime-created tree did not trigger a rebuild. output: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// A `watch_ignore`d directory that is created *at runtime* under a watched
/// directory must not be registered with the watcher (and must not trigger a
/// rebuild). This is why every watch is non-recursive: a recursive watch would
/// let notify auto-descend into the fresh `node_modules` and re-register the
/// whole heavy subtree we asked to ignore.
#[test]
fn integration_runtime_node_modules_is_not_watched() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("watch-runtime-node-modules");
        std::fs::create_dir_all(dir.path().join("src")).unwrap();
        std::fs::write(dir.path().join("src/main.rs"), "v1").unwrap();

        let toml = r#"
watch_ignore = ["**/node_modules/**"]

[services.api]
run.cmd = "bash"
run.args = ["-c", "sleep 60"]
watch = ["src/**/*.rs"]
debounce = "100ms"
log = "ignore"
ready.exec.cmd = "true"
"#;

        let (runner, shutdown_tx, buf) = make_runner_verbose(toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "all services running", Duration::from_secs(5)).await,
            "services did not start. output: {}",
            read_buf(&buf)
        );

        // Simulate `npm install`: a node_modules tree appears at runtime under
        // the watched `src` dir, then a real source edit lands.
        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::create_dir_all(dir.path().join("src/node_modules/left-pad")).unwrap();
        std::fs::write(
            dir.path().join("src/node_modules/left-pad/index.js"),
            "module.exports = 1",
        )
        .unwrap();
        // Give the create event time to be processed by the backstop.
        tokio::time::sleep(Duration::from_millis(400)).await;
        std::fs::write(dir.path().join("src/main.rs"), "v2").unwrap();

        // The real edit still rebuilds — the watcher is alive and working.
        assert!(
            wait_for_output(&buf, "rebuilding (file changed)", Duration::from_secs(5)).await,
            "watched edit did not rebuild. output: {}",
            read_buf(&buf)
        );

        let output = read_buf(&buf);
        // The runtime-created ignored dir must stay invisible to the watcher: no
        // event, match, or registration diagnostic. The only legitimate mention
        // is the startup summary echoing the configured ignore patterns
        // (`... ignore=[...]`), which we skip.
        for line in output.lines() {
            if line.contains("ignore=[") {
                continue;
            }
            assert!(
                !line.contains("node_modules"),
                "node_modules leaked into watch handling: {line}"
            );
        }

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Integration test: build then restart on file change ---

#[test]
fn integration_build_then_restart_on_file_change() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("watch-build");

        let src_dir = dir.path().join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("app.rs"), "v1").unwrap();

        // Build writes a marker file, service reads it.
        let toml = ConfigBuilder::new()
            .add_custom_service(
                "api",
                "bash",
                &["-c", "cat built.txt 2>/dev/null; sleep 60"],
            )
            .build_cmd("bash", &["-c", "echo build-ran > built.txt"])
            .watch(&["src/**/*.rs"])
            .debounce("100ms")
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "all services running", Duration::from_secs(5)).await,
            "timed out waiting for services to start"
        );

        // Modify watched file to trigger rebuild.
        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(src_dir.join("app.rs"), "v2").unwrap();

        // Should see rebuild lifecycle events (the initial build also ran,
        // so we check for "rebuilding" to confirm the file-watch triggered).
        assert!(
            wait_for_output(&buf, "rebuilding (file changed)", Duration::from_secs(5)).await,
            "timed out waiting for rebuild trigger. output: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Integration test: edit during build skips intermediate restart ---

#[test]
fn integration_edit_during_build_skips_intermediate_restart() {
    run_with_timeout(Duration::from_secs(20), async {
        let dir = TempDir::new("watch-build-stale");

        let src_dir = dir.path().join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("app.rs"), "v1").unwrap();

        let toml = ConfigBuilder::new()
            .add_custom_service("api", "bash", &["-c", "echo PID=$$ && sleep 60"])
            .build_cmd(
                "bash",
                &[
                    "-c",
                    "if [ -f slow-build ]; then sleep 1; rm -f slow-build; fi",
                ],
            )
            .watch(&["src/**/*.rs"])
            .debounce("100ms")
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "all services running", Duration::from_secs(5)).await,
            "timed out waiting for services to start. output: {}",
            read_buf(&buf)
        );

        std::fs::write(dir.path().join("slow-build"), "1").unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(src_dir.join("app.rs"), "v2").unwrap();

        assert!(
            wait_for_output(&buf, "rebuilding (file changed)", Duration::from_secs(5)).await,
            "timed out waiting for first rebuild. output: {}",
            read_buf(&buf)
        );

        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(src_dir.join("app.rs"), "v3").unwrap();

        let start = tokio::time::Instant::now();
        loop {
            let output = read_buf(&buf);
            let rebuilds = output.matches("rebuilding (file changed)").count();
            let restarts = output.matches("restarting...").count();
            let build_successes = output.matches("bash build succeeded").count();
            if rebuilds >= 2 && build_successes >= 3 && restarts >= 1 {
                assert_eq!(
                    restarts, 1,
                    "expected only one restart after two rebuild cycles. output: {output}"
                );
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(10),
                "timed out waiting for stale rebuild flow. output: {output}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        tokio::time::sleep(Duration::from_millis(300)).await;
        let output = read_buf(&buf);
        assert_eq!(
            output.matches("restarting...").count(),
            1,
            "unexpected extra restart after stale rebuild. output: {output}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Integration test: build failure keeps old process ---

#[test]
fn integration_build_failure_keeps_old_process() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("watch-build-fail");

        let src_dir = dir.path().join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("app.rs"), "v1").unwrap();

        // Build script: succeeds the first time (creates marker), fails on subsequent runs.
        let build_script_path = dir.path().join("build.sh");
        let marker = dir.path().join("build-done");
        std::fs::write(
            &build_script_path,
            format!(
                "#!/bin/bash\nif [ -f '{}' ]; then exit 1; fi\ntouch '{}'\n",
                marker.display(),
                marker.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(
            &build_script_path,
            std::os::unix::fs::PermissionsExt::from_mode(0o755),
        )
        .unwrap();

        let toml = ConfigBuilder::new()
            .add_custom_service("api", "bash", &["-c", "echo running && sleep 60"])
            .build_cmd(build_script_path.to_str().unwrap(), &[])
            .watch(&["src/**/*.rs"])
            .debounce("100ms")
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "all services running", Duration::from_secs(5)).await,
            "timed out waiting for services to start"
        );

        // Trigger a rebuild.
        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(src_dir.join("app.rs"), "v2").unwrap();

        // Should see build failure.
        assert!(
            wait_for_output(&buf, "build failed", Duration::from_secs(5)).await,
            "timed out waiting for build failure. output: {}",
            read_buf(&buf)
        );

        // Old service is still running — no "restarted" or "stopped" message.
        let output = read_buf(&buf);
        assert!(
            !output.contains("restarted"),
            "service should not have restarted after build failure"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Integration test: rapid-fire changes result in one restart ---

#[test]
fn integration_rapid_fire_changes_one_restart() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("watch-rapid");

        let src_dir = dir.path().join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("main.rs"), "v0").unwrap();

        let toml = ConfigBuilder::new()
            .add_custom_service("api", "bash", &["-c", "sleep 60"])
            .watch(&["src/**/*.rs"])
            // A generous debounce so all 5 writes coalesce even under the
            // coarser event-delivery latency of macOS FSEvents (a tight window
            // makes this test ~flaky there). The window still comfortably
            // exceeds the 250ms write span on Linux/inotify.
            .debounce("1s")
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "all services running", Duration::from_secs(5)).await,
            "timed out waiting for services to start"
        );

        // Fire 5 rapid changes with 50ms gaps (total 250ms < 1s debounce).
        tokio::time::sleep(Duration::from_millis(200)).await;
        for i in 1..=5 {
            std::fs::write(src_dir.join("main.rs"), format!("v{i}")).unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Wait for the single rebuild.
        assert!(
            wait_for_output(&buf, "rebuilding (file changed)", Duration::from_secs(5)).await,
            "timed out waiting for rebuild. output: {}",
            read_buf(&buf)
        );

        // Give a moment for any extra rebuilds to appear.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Count how many times "rebuilding" appears — should be exactly 1.
        let output = read_buf(&buf);
        let rebuild_count = output.matches("rebuilding (file changed)").count();
        assert_eq!(
            rebuild_count, 1,
            "expected exactly 1 rebuild, got {rebuild_count}. output: {output}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Integration test: file edit during startup triggers rebuild ---

#[test]
fn integration_file_edit_during_startup_triggers_rebuild() {
    use std::os::unix::fs::PermissionsExt;

    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("watch-during-startup");

        let src_dir = dir.path().join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("main.rs"), "v1").unwrap();

        // Ready check script: succeeds only when a marker file exists.
        // This lets us control exactly when startup finishes.
        let ready_script = dir.path().join("ready.sh");
        std::fs::write(&ready_script, "#!/bin/bash\n[ -f \"$1\" ]").unwrap();
        std::fs::set_permissions(&ready_script, std::fs::Permissions::from_mode(0o755)).unwrap();

        let marker = dir.path().join("ready-marker");

        let toml = ConfigBuilder::new()
            .add_custom_service("api", "bash", &["-c", "sleep 60"])
            .watch(&["src/**/*.rs"])
            .debounce("100ms")
            .log("ignore")
            .ready_exec_with(
                ready_script.to_str().unwrap(),
                &[marker.to_str().unwrap()],
                "200ms",
                30,
            )
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Wait for the service to start (but not be ready yet).
        assert!(
            wait_for_output(&buf, "api: starting", Duration::from_secs(5)).await,
            "timed out waiting for service to start. output: {}",
            read_buf(&buf)
        );

        // Edit a watched file while the ready check is still retrying.
        tokio::time::sleep(Duration::from_millis(300)).await;
        std::fs::write(src_dir.join("main.rs"), "v2").unwrap();

        // Now let startup complete by creating the marker file.
        tokio::time::sleep(Duration::from_millis(300)).await;
        std::fs::write(&marker, "ready").unwrap();

        // The service should become ready, then the queued rebuild should fire.
        assert!(
            wait_for_output(&buf, "rebuilding (file changed)", Duration::from_secs(5)).await,
            "expected rebuild from edit during startup. output: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Integration test: reload = false prevents file-watch restart ---

#[test]
fn integration_reload_false_skips_file_watch() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("watch-reload-false");

        let src_dir = dir.path().join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("main.rs"), "initial").unwrap();

        // Service with reload = false and explicit watch patterns.
        // Even though watch patterns are set, reload = false should prevent
        // don from setting up file watches entirely.
        let toml = ConfigBuilder::new()
            .add_custom_service("frontend", "bash", &["-c", "echo STARTED && sleep 60"])
            .watch(&["src/**/*.rs"])
            .debounce("100ms")
            .reload(false)
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "all services running", Duration::from_secs(5)).await,
            "timed out waiting for services to start. output: {}",
            read_buf(&buf)
        );

        // Modify the watched file — should NOT trigger a rebuild.
        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(src_dir.join("main.rs"), "modified").unwrap();

        // Wait long enough for a rebuild to have been triggered (debounce + margin).
        tokio::time::sleep(Duration::from_millis(800)).await;

        let output = read_buf(&buf);
        assert!(
            !output.contains("rebuilding"),
            "reload = false should prevent rebuilds on file change. output: {output}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_global_watch_ignore_skips_service_rebuild() {
    run_with_timeout(Duration::from_secs(15), async {
        // Distinct from `integration_global_watch_ignore_is_silent_in_verbose`:
        // TempDir names are only namespaced by PID, so two tests sharing a name
        // share a directory — and each `TempDir::new` wipes it on creation.
        let dir = TempDir::new("watch-global-ignore-rebuild");

        let src_dir = dir.path().join("src");
        let generated_dir = src_dir.join("generated");
        std::fs::create_dir_all(&generated_dir).unwrap();
        std::fs::write(src_dir.join("main.rs"), "initial").unwrap();
        std::fs::write(generated_dir.join("schema.rs"), "initial").unwrap();

        let toml = ConfigBuilder::new()
            .watch_ignore(&["src/generated/**"])
            .add_custom_service("api", "bash", &["-c", "echo STARTED && sleep 60"])
            .watch(&["src/**/*.rs"])
            .debounce("100ms")
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "all services running", Duration::from_secs(5)).await,
            "timed out waiting for services to start. output: {}",
            read_buf(&buf)
        );

        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(generated_dir.join("schema.rs"), "modified").unwrap();
        tokio::time::sleep(Duration::from_millis(800)).await;

        let output = read_buf(&buf);
        assert!(
            !output.contains("rebuilding"),
            "global watch_ignore should prevent rebuilds on ignored file change. output: {output}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Integration test: task with auto_run=false skips initial run and goes pending on change ---

#[test]
fn integration_task_auto_run_false_skips_initial_and_goes_pending() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("watch-task-manual");

        let defs_dir = dir.path().join("definitions");
        std::fs::create_dir_all(&defs_dir).unwrap();
        let schema = defs_dir.join("users.sql");
        std::fs::write(&schema, "CREATE TABLE users (id INT);").unwrap();

        // A service to keep don running after the task completes,
        // plus a task with auto_run = false.
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "bash", &["-c", "sleep 60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_task("migrate", "echo", &["migrating"])
            .watch(&["definitions/**/*.sql"])
            .auto_run(false)
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner_verbose(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Task should NOT run at startup. Its watch inputs are considered
        // changed on first evaluation, so it starts in PendingRun.
        assert!(
            wait_for_output(
                &buf,
                "pending — watch inputs changed, auto_run = false",
                Duration::from_secs(5)
            )
            .await,
            "migrate should be pending at startup. output: {}",
            read_buf(&buf)
        );

        let startup_output = read_buf(&buf);
        for expected in [
            "task state: watched input check started",
            "task state: expanding watch glob",
            "task state: glob complete",
            "matched_files=1",
            "task state: hashing watched file contents files=1",
            "task state: hash complete files=1",
            "task state: watched input check complete changed=true",
        ] {
            assert!(
                startup_output.contains(expected),
                "missing verbose task-state diagnostic {expected:?}. output: {startup_output}"
            );
        }

        // Modify the watched file.
        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(&schema, "CREATE TABLE users (id INT, name TEXT);").unwrap();

        // Should log pending again, NOT actually run the task.
        assert!(
            wait_for_output(&buf, "files changed (pending", Duration::from_secs(5)).await,
            "expected pending event on file change. output: {}",
            read_buf(&buf)
        );

        // Give any rogue run a chance to start.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let output = read_buf(&buf);
        assert_eq!(
            output.matches("migrate complete").count(),
            0,
            "migrate should never have run; output: {output}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_task_auto_run_once_runs_initially_then_goes_pending_on_change() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("watch-task-once");

        let defs_dir = dir.path().join("definitions");
        std::fs::create_dir_all(&defs_dir).unwrap();
        let schema = defs_dir.join("users.sql");
        std::fs::write(&schema, "CREATE TABLE users (id INT);").unwrap();

        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "bash", &["-c", "sleep 60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_task("migrate", "echo", &["migrating"])
            .watch(&["definitions/**/*.sql"])
            .auto_run_mode("once")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "migrate: running", Duration::from_secs(5)).await,
            "migrate should auto-run on first startup. output: {}",
            read_buf(&buf)
        );

        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(&schema, "CREATE TABLE users (id INT, name TEXT);").unwrap();

        assert!(
            wait_for_output(
                &buf,
                "files changed (pending — auto_run = once)",
                Duration::from_secs(5)
            )
            .await,
            "expected pending event on file change. output: {}",
            read_buf(&buf)
        );

        let output = read_buf(&buf);
        let first_run = output.find("migrate: running");
        let pending = output.find("files changed (pending — auto_run = once)");
        assert!(
            first_run.is_some() && pending.is_some() && first_run < pending,
            "task should run first, then become pending on later changes. output: {output}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// don's own state directory and git's are never watched, whatever the config's
/// globs say.
///
/// `.don/` is the load-bearing one: the runner writes every lifecycle line it
/// emits into `.don/logs/runner.log`, so a project-wide glob like `**/*.ts`
/// made don's own logging fire watch events that don logged, at tens of
/// thousands of lines a second. `.git/` is the same shape without the loop —
/// ref and index churn nobody asked to watch.
#[test]
fn integration_don_and_git_directories_are_never_watched() {
    struct Case {
        name: &'static str,
        /// Written relative to the project root, with parents created.
        path: &'static str,
        want_silent: bool,
    }

    let cases = [
        Case {
            name: "don's own log is what made this a feedback loop",
            path: ".don/logs/runner.log",
            want_silent: true,
        },
        Case {
            name: "git ref churn is noise, not source",
            path: ".git/refs/remotes/origin/some-branch",
            want_silent: true,
        },
        Case {
            name: "an ordinary file the glob covers still reports",
            path: "src/main.ts",
            want_silent: false,
        },
    ];

    for case in cases {
        run_with_timeout(Duration::from_secs(15), async {
            let dir = TempDir::new("watch-builtin-ignore");
            let target = dir.path().join(case.path);
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(&target, "initial").unwrap();
            std::fs::create_dir_all(dir.path().join("src")).unwrap();
            std::fs::write(dir.path().join("src/other.ts"), "initial").unwrap();

            // A project-wide glob, which is what pulls the dot directories in.
            let toml = r#"
[services.app]
run.cmd = "bash"
run.args = ["-c", "sleep 60"]
watch = ["**/*"]
debounce = "100ms"
log = "ignore"
ready.exec.cmd = "true"
"#;

            let (runner, shutdown_tx, buf) = make_runner_verbose(toml, dir.path()).await;
            let handle = tokio::spawn(async move {
                runner.run().await.unwrap();
            });
            assert!(
                wait_for_output(&buf, "all services running", Duration::from_secs(5)).await,
                "{}: services did not start. output: {}",
                case.name,
                read_buf(&buf)
            );

            tokio::time::sleep(Duration::from_millis(200)).await;
            let before = read_buf(&buf).len();
            std::fs::write(&target, "modified").unwrap();
            tokio::time::sleep(Duration::from_millis(800)).await;

            let after = read_buf(&buf);
            let new_output = after.get(before..).unwrap_or_default();
            let mentioned = new_output.contains(case.path);
            assert_eq!(
                !mentioned, case.want_silent,
                "{}: writing {} produced: {new_output}",
                case.name, case.path
            );

            let _ = shutdown_tx.send(()).await;
            handle.await.unwrap();
        });
    }
}

/// An event nothing matches costs one line, not one per watch item.
///
/// The per-item narration fired only in the branch where every item failed for
/// the same reason the summary line already gives, so on a stack with dozens of
/// services every stray write under the project turned into dozens of lines.
#[test]
fn integration_unmatched_event_does_not_narrate_every_item() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("watch-unmatched-fanout");
        std::fs::create_dir_all(dir.path().join("src")).unwrap();
        std::fs::write(dir.path().join("src/a.rs"), "initial").unwrap();
        std::fs::write(dir.path().join("src/stray.txt"), "initial").unwrap();

        // Three services watching the same thing: enough that a per-item
        // fan-out is unmistakable in the count.
        let toml = r#"
[services.one]
run.cmd = "bash"
run.args = ["-c", "sleep 60"]
watch = ["src/**/*.rs"]
debounce = "100ms"
log = "ignore"
ready.exec.cmd = "true"

[services.two]
run.cmd = "bash"
run.args = ["-c", "sleep 60"]
watch = ["src/**/*.rs"]
debounce = "100ms"
log = "ignore"
ready.exec.cmd = "true"

[services.three]
run.cmd = "bash"
run.args = ["-c", "sleep 60"]
watch = ["src/**/*.rs"]
debounce = "100ms"
log = "ignore"
ready.exec.cmd = "true"
"#;

        let (runner, shutdown_tx, buf) = make_runner_verbose(toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });
        assert!(
            wait_for_output(&buf, "all services running", Duration::from_secs(5)).await,
            "services did not start. output: {}",
            read_buf(&buf)
        );

        tokio::time::sleep(Duration::from_millis(200)).await;
        let before = read_buf(&buf).len();
        std::fs::write(dir.path().join("src/stray.txt"), "modified").unwrap();
        tokio::time::sleep(Duration::from_millis(800)).await;

        let after = read_buf(&buf);
        let new_output = after.get(before..).unwrap_or_default();
        let mentions = new_output.matches("stray.txt").count();
        assert!(
            new_output.contains("matched no item"),
            "the one summary line should still be emitted. output: {new_output}"
        );
        assert!(
            mentions <= 2,
            "expected the raw event line and one summary, got {mentions} mentions: {new_output}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}
