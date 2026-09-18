#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
//! Integration tests for Phase 15: profile support.

mod helpers;

use don::config::resolve_profile_processes;
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

// --- Unit tests for resolve_profile_processes ---

#[test]
fn resolve_profile_transitive_deps() {
    struct Case {
        name: &'static str,
        toml: &'static str,
        profile_name: &'static str,
        expected: Vec<&'static str>,
    }

    let cases = vec![
        Case {
            name: "direct services only",
            toml: r#"
                [services.api]
                run.cmd = "api"
                [services.worker]
                run.cmd = "worker"
                [profiles.frontend]
                services = ["api"]
            "#,
            profile_name: "frontend",
            expected: vec!["api"],
        },
        Case {
            name: "transitive dep chain: api -> migrate -> postgres",
            toml: r#"
                [services.postgres]
                run.cmd = "pg"
                [tasks.migrate]
                cmd = "dbmate"
                depends_on = ["postgres"]
                [services.api]
                run.cmd = "api"
                depends_on = ["migrate"]
                [profiles.backend]
                services = ["api"]
            "#,
            profile_name: "backend",
            expected: vec!["api", "migrate", "postgres"],
        },
        Case {
            name: "overlapping deps — no duplicates",
            toml: r#"
                [services.db]
                run.cmd = "db"
                [services.api]
                run.cmd = "api"
                depends_on = ["db"]
                [services.worker]
                run.cmd = "worker"
                depends_on = ["db"]
                [profiles.all]
                services = ["api", "worker"]
            "#,
            profile_name: "all",
            expected: vec!["api", "db", "worker"],
        },
        Case {
            name: "profile with only tasks",
            toml: r#"
                [services.postgres]
                run.cmd = "pg"
                [tasks.migrate]
                cmd = "dbmate"
                depends_on = ["postgres"]
                [profiles.setup]
                tasks = ["migrate"]
            "#,
            profile_name: "setup",
            expected: vec!["migrate", "postgres"],
        },
        Case {
            name: "profile with tasks and services",
            toml: r#"
                [services.db]
                run.cmd = "db"
                [services.api]
                run.cmd = "api"
                depends_on = ["db"]
                [tasks.seed]
                cmd = "seed"
                depends_on = ["db"]
                [profiles.dev]
                services = ["api"]
                tasks = ["seed"]
            "#,
            profile_name: "dev",
            expected: vec!["api", "db", "seed"],
        },
        Case {
            name: "profile expands service groups and dependency groups",
            toml: r#"
                [services.postgres]
                run.cmd = "pg"
                [services.redis]
                run.cmd = "redis"
                [services.api]
                run.cmd = "api"
                depends_on = ["datastores"]
                [service_groups]
                datastores = ["postgres", "redis"]
                [profiles.backend]
                services = ["api", "datastores"]
            "#,
            profile_name: "backend",
            expected: vec!["api", "postgres", "redis"],
        },
        Case {
            name: "profile expands nested service groups",
            toml: r#"
                [services.postgres]
                run.cmd = "pg"
                [services.redis]
                run.cmd = "redis"
                [services.api]
                run.cmd = "api"
                [service_groups]
                datastores = ["postgres", "redis"]
                backend = ["datastores", "api"]
                [profiles.dev]
                services = ["backend"]
            "#,
            profile_name: "dev",
            expected: vec!["api", "postgres", "redis"],
        },
        Case {
            name: "profile picks up group-level depends_on transitively",
            toml: r#"
                [services.api]
                run.cmd = "api"
                [services.web]
                run.cmd = "web"
                [services.admin]
                run.cmd = "admin"
                [service_groups."web-stack"]
                members = ["web", "admin"]
                [service_groups.frontend]
                members = ["web-stack"]
                depends_on = ["api"]
                [profiles.dev]
                services = ["frontend"]
            "#,
            profile_name: "dev",
            expected: vec!["web", "admin", "api"],
        },
    ];

    for case in cases {
        let config: Config = parse_config(case.toml);
        let profile = config.profiles.get(case.profile_name).unwrap();
        let result = resolve_profile_processes(&config, profile);
        let mut result_sorted: Vec<String> = result.into_iter().collect();
        result_sorted.sort();
        let mut expected_sorted: Vec<String> =
            case.expected.iter().map(|s| s.to_string()).collect();
        expected_sorted.sort();
        assert_eq!(
            result_sorted, expected_sorted,
            "case '{}': expected {expected_sorted:?}, got {result_sorted:?}",
            case.name
        );
    }
}

// --- Integration tests ---

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

async fn spawn_runner_with_profile(
    toml: &str,
    base_dir: &std::path::Path,
    profile: Option<&str>,
) -> (
    mpsc::Sender<()>,
    tokio::task::JoinHandle<()>,
    Arc<Mutex<Vec<u8>>>,
) {
    let config_path = base_dir.join("don.toml");
    std::fs::write(&config_path, toml).unwrap();

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
        profile,
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

/// Start with a profile — only profiled services run.
#[test]
fn profile_starts_only_selected_services() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("profile-selected");
        let toml = ConfigBuilder::new()
            .add_custom_service("api", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_custom_service("worker", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_profile("frontend", &["api"], &[])
            .build();

        let (shutdown_tx, handle, buf) =
            spawn_runner_with_profile(&toml, dir.path(), Some("frontend")).await;
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        let output = read_buf(&buf);
        assert!(
            output.contains("api: starting..."),
            "api should start. output: {output}"
        );
        assert!(
            !output.contains("worker: starting..."),
            "worker should NOT start. output: {output}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// Profile with transitive deps — the dep is started even though
/// it's not explicitly listed in the profile.
#[test]
fn profile_includes_transitive_deps() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("profile-transitive");
        let toml = ConfigBuilder::new()
            .add_custom_service("db", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_custom_service("api", "sleep", &["60"])
            .log("ignore")
            .depends_on(&["db"])
            .ready_exec("true", &[])
            .done()
            .add_custom_service("worker", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_profile("backend", &["api"], &[])
            .build();

        let (shutdown_tx, handle, buf) =
            spawn_runner_with_profile(&toml, dir.path(), Some("backend")).await;
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        let output = read_buf(&buf);
        assert!(
            output.contains("db: starting..."),
            "db (transitive dep) should start. output: {output}"
        );
        assert!(
            output.contains("api: starting..."),
            "api should start. output: {output}"
        );
        assert!(
            !output.contains("worker: starting..."),
            "worker should NOT start. output: {output}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// Services NOT in the profile are excluded from the status API.
#[test]
fn profile_excluded_services_absent_from_status() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("profile-status");
        let toml = ConfigBuilder::new()
            .add_custom_service("api", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_custom_service("worker", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_profile("frontend", &["api"], &[])
            .build();

        let (shutdown_tx, handle, buf) =
            spawn_runner_with_profile(&toml, dir.path(), Some("frontend")).await;

        // Wait for socket to be ready.
        let sock = dir.path().join(".don").join("don.sock");
        let start = tokio::time::Instant::now();
        while !sock.exists() && start.elapsed() < Duration::from_secs(3) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        // Check status via API — worker should not appear.
        let client = don::client::Client::new(dir.path());
        let statuses = client.status(false, None).await.unwrap();
        let names: Vec<String> = statuses
            .iter()
            .map(|i| match i {
                don::runner::ProcessStatus::Service { name, .. } => name.clone(),
                don::runner::ProcessStatus::Task { name, .. } => name.clone(),
            })
            .collect();
        assert!(
            names.contains(&"api".to_string()),
            "api should be in status: {names:?}"
        );
        assert!(
            !names.contains(&"worker".to_string()),
            "worker should NOT be in status: {names:?}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// Profiles can include a service group directly.
#[test]
fn profile_service_group_starts_group_members() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("profile-service-group");
        let toml = ConfigBuilder::new()
            .add_custom_service("postgres", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_custom_service("redis", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_custom_service("worker", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_service_group("datastores", &["postgres", "redis"])
            .add_profile("backend", &["datastores"], &[])
            .build();

        let (shutdown_tx, handle, buf) =
            spawn_runner_with_profile(&toml, dir.path(), Some("backend")).await;
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        let output = read_buf(&buf);
        assert!(
            output.contains("postgres: starting..."),
            "postgres should start. output: {output}"
        );
        assert!(
            output.contains("redis: starting..."),
            "redis should start. output: {output}"
        );
        assert!(
            !output.contains("worker: starting..."),
            "worker should NOT start. output: {output}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Profile overrides ---

/// A base file whose `prod` profile retargets the same services the `dev`
/// profile runs — the local-prod shape: same stack, different conf.
const OVERRIDE_CONFIG: &str = r#"
default_profile = "dev"

[services.api]
run.cmd = "api"
run.args = ["--dev"]
env = { CONF = "dev", SHARED = "base" }

[tasks.migrate]
cmd = "migrate"

[profiles.dev]
services = ["api"]
tasks = ["migrate"]

[profiles.prod]
services = ["api"]
tasks = ["migrate"]

[profiles.prod.overrides.services.api]
run.args = ["--prod"]
env = { CONF = "prod" }

[profiles.prod.overrides.tasks.migrate]
args = ["--prod"]
"#;

fn write_config(dir: &TempDir, base: &str, local: Option<&str>) -> std::path::PathBuf {
    let config_path = dir.child("don.toml");
    std::fs::write(&config_path, base).unwrap();
    if let Some(local) = local {
        std::fs::write(dir.child("don.local.toml"), local).unwrap();
    }
    config_path
}

fn api_args(config: &Config) -> Vec<String> {
    let Some(don::config::ServiceKind::Custom { run, .. }) =
        config.services["api"].resolve(PLATFORM).kind
    else {
        panic!("expected custom api service");
    };
    run.args
}

#[test]
fn profile_overrides_apply_to_the_active_profile_only() {
    struct Case {
        name: &'static str,
        requested: Option<&'static str>,
        conf: &'static str,
        args: Vec<&'static str>,
        task_args: Vec<&'static str>,
    }

    let cases = vec![
        Case {
            name: "default profile, which has no overrides",
            requested: None,
            conf: "dev",
            args: vec!["--dev"],
            task_args: vec![],
        },
        Case {
            name: "requested profile's overrides apply",
            requested: Some("prod"),
            conf: "prod",
            args: vec!["--prod"],
            task_args: vec!["--prod"],
        },
        Case {
            name: "an inactive profile's overrides stay out",
            requested: Some("dev"),
            conf: "dev",
            args: vec!["--dev"],
            task_args: vec![],
        },
    ];

    for case in cases {
        let dir = TempDir::new(&format!(
            "profile-overrides-{}",
            case.name.replace(' ', "-").replace(',', "")
        ));
        let config_path = write_config(&dir, OVERRIDE_CONFIG, None);

        let config = Config::from_file_with_profile(&config_path, case.requested).unwrap();
        config.validate(PLATFORM).unwrap();

        assert_eq!(
            config.services["api"].env["CONF"], case.conf,
            "{}",
            case.name
        );
        assert_eq!(
            config.services["api"].env["SHARED"], "base",
            "{}: an override table merges field by field",
            case.name
        );
        assert_eq!(api_args(&config), case.args, "{}", case.name);
        assert_eq!(
            config.tasks["migrate"].args, case.task_args,
            "{}",
            case.name
        );
    }
}

#[test]
fn default_profile_overrides_apply_without_a_requested_profile() {
    let dir = TempDir::new("profile-overrides-default");
    let base = OVERRIDE_CONFIG.replace(r#"default_profile = "dev""#, r#"default_profile = "prod""#);
    let config_path = write_config(&dir, &base, None);

    let config = Config::from_file(&config_path).unwrap();

    assert_eq!(config.services["api"].env["CONF"], "prod");
    assert_eq!(api_args(&config), vec!["--prod"]);
}

#[test]
fn local_file_outranks_profile_overrides_and_can_move_the_active_profile() {
    let dir = TempDir::new("profile-overrides-local");
    let config_path = write_config(
        &dir,
        OVERRIDE_CONFIG,
        Some(
            r#"
default_profile = "prod"

[services.api]
env = { CONF = "mine" }

[profiles.prod.overrides.services.api]
env = { EXTRA = "from-local" }
"#,
        ),
    );

    let config = Config::from_file(&config_path).unwrap();

    assert_eq!(
        config.services["api"].env["CONF"], "mine",
        "the developer's own file wins over a profile the repo ships"
    );
    assert_eq!(
        config.services["api"].env["EXTRA"], "from-local",
        "a local file can extend the active profile's overrides"
    );
    assert_eq!(
        api_args(&config),
        vec!["--prod"],
        "local `default_profile` selects which overrides apply"
    );
}

#[test]
fn bad_profile_overrides_are_rejected_with_actionable_errors() {
    struct Case {
        name: &'static str,
        toml: &'static str,
        expected: &'static str,
    }

    let cases = vec![
        Case {
            name: "misspelled overrides key",
            toml: r#"
                [services.api]
                run.cmd = "api"
                [profiles.prod]
                services = ["api"]
                [profiles.prod.overides.services.api]
                env = { CONF = "prod" }
            "#,
            expected: "unknown key 'overides' — did you mean 'overrides'?",
        },
        Case {
            name: "overrides selecting another profile",
            toml: r#"
                [services.api]
                run.cmd = "api"
                [profiles.prod]
                services = ["api"]
                [profiles.prod.overrides]
                default_profile = "dev"
            "#,
            expected: "cannot set 'default_profile'",
        },
        Case {
            name: "overrides redefining profiles",
            toml: r#"
                [services.api]
                run.cmd = "api"
                [profiles.prod]
                services = ["api"]
                [profiles.prod.overrides.profiles.dev]
                services = ["api"]
            "#,
            expected: "cannot set 'profiles'",
        },
    ];

    for case in cases {
        let dir = TempDir::new(&format!(
            "profile-overrides-invalid-{}",
            case.name.replace(' ', "-")
        ));
        let config_path = write_config(&dir, case.toml, None);

        let err = Config::from_file_with_profile(&config_path, Some("prod")).unwrap_err();
        let don::config::ConfigError::Validation { errors } = err else {
            panic!("{}: expected a validation error", case.name);
        };
        assert!(
            errors.iter().any(|e| e.contains(case.expected)),
            "{}: errors {errors:?} should mention {}",
            case.name,
            case.expected
        );
    }
}

#[test]
fn profile_overrides_retarget_the_global_env() {
    let dir = TempDir::new("profile-overrides-global-env");
    let config_path = write_config(
        &dir,
        r#"
default_profile = "dev"
env = { CONF_PATH = "conf.sh.dev" }

[services.api]
run.cmd = "api"

[tasks.migrate]
cmd = "migrate"

[profiles.dev]
services = ["api"]
tasks = ["migrate"]

[profiles.prod]
services = ["api"]
tasks = ["migrate"]

[profiles.prod.overrides.env]
CONF_PATH = "conf.sh.prod"
"#,
        None,
    );

    let dev = Config::from_file(&config_path).unwrap();
    assert_eq!(dev.services["api"].env["CONF_PATH"], "conf.sh.dev");
    assert_eq!(dev.tasks["migrate"].env["CONF_PATH"], "conf.sh.dev");

    let prod = Config::from_file_with_profile(&config_path, Some("prod")).unwrap();
    assert_eq!(
        prod.services["api"].env["CONF_PATH"], "conf.sh.prod",
        "one key in the overrides block retargets the whole stack"
    );
    assert_eq!(prod.tasks["migrate"].env["CONF_PATH"], "conf.sh.prod");
}
