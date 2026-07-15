#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod helpers;

use don::config::{Config, ConfigError, Platform, ServiceKind};
use helpers::config::ConfigBuilder;
use helpers::tempdir::TempDir;
use helpers::timeout::run_with_timeout;
use std::time::Duration;

const TEST_PLATFORM: Platform = Platform::LinuxX86_64;

macro_rules! bounded_test {
    ($name:ident, $body:ident) => {
        #[test]
        fn $name() {
            run_with_timeout(Duration::from_secs(10), async {
                $body();
            });
        }
    };
}

fn validate_valid_config_body() {
    let dir = TempDir::new("validate-valid");
    let config_path = ConfigBuilder::new()
        .add_custom_service("api", "mybin", &["serve"])
        .done()
        .add_docker_service("postgres", "postgres:16")
        .done()
        .write_to(dir.path());

    let config = Config::from_file(&config_path).unwrap();
    assert!(config.validate(TEST_PLATFORM).is_ok());
}

fn validate_invalid_config_unknown_dep_body() {
    let dir = TempDir::new("validate-invalid-dep");
    let config_path = ConfigBuilder::new()
        .add_custom_service("api", "mybin", &[])
        .depends_on(&["ghost"])
        .write_to(dir.path());

    let config = Config::from_file(&config_path).unwrap();
    let err = config.validate(TEST_PLATFORM).unwrap_err();
    let ConfigError::Validation { errors } = &err else {
        panic!("expected validation error");
    };
    assert!(errors[0].contains("unknown service, task, or service group 'ghost'"));
}

fn validate_missing_config_file_body() {
    let dir = TempDir::new("validate-missing");
    let config_path = dir.child("nonexistent.toml");

    let err = Config::from_file(&config_path).unwrap_err();
    assert!(matches!(err, ConfigError::ReadFile { .. }));
}

fn validate_malformed_toml_body() {
    let dir = TempDir::new("validate-malformed");
    let config_path = dir.child("don.toml");
    std::fs::write(&config_path, "this is not [valid toml").unwrap();

    let err = Config::from_file(&config_path).unwrap_err();
    assert!(matches!(err, ConfigError::Parse(_)));
}

fn validate_empty_config_is_valid_body() {
    let dir = TempDir::new("validate-empty");
    let config_path = dir.child("don.toml");
    std::fs::write(&config_path, "").unwrap();

    let config = Config::from_file(&config_path).unwrap();
    assert!(config.validate(TEST_PLATFORM).is_ok());
}

fn validate_config_with_tasks_and_profiles_body() {
    let dir = TempDir::new("validate-full");
    let config_path = ConfigBuilder::new()
        .add_docker_service("postgres", "postgres:16")
        .done()
        .add_custom_service("api", "api-server", &["serve"])
        .depends_on(&["migrate"])
        .done()
        .add_task("migrate", "dbmate", &["up"])
        .done()
        .add_profile("frontend", &["api", "postgres"], &["migrate"])
        .write_to(dir.path());

    let config = Config::from_file(&config_path).unwrap();
    assert!(config.validate(TEST_PLATFORM).is_ok());
    assert_eq!(config.services.len(), 2);
    assert_eq!(config.tasks.len(), 1);
    assert_eq!(config.profiles.len(), 1);
}

fn validate_shutdown_graceful_flags_body() {
    let toml = r#"
[shutdown]
graceful = false

[services.api]
run.cmd = "api"
shutdown.timeout = "250ms"

[services.worker]
run.cmd = "worker"
shutdown.graceful = true
shutdown.signal = "SIGINT"
"#;

    let config: Config = toml.parse().unwrap();
    config.validate(TEST_PLATFORM).unwrap();
    assert!(!config.shutdown.graceful);
    assert_eq!(config.shutdown.signal, "SIGTERM");
    assert_eq!(config.shutdown.timeout, "10s");

    let api_shutdown = config.services["api"]
        .resolve(TEST_PLATFORM)
        .shutdown
        .unwrap();
    assert!(api_shutdown.graceful);
    assert_eq!(api_shutdown.timeout, "250ms");

    let worker_shutdown = config.services["worker"]
        .resolve(TEST_PLATFORM)
        .shutdown
        .unwrap();
    assert!(worker_shutdown.graceful);
    assert_eq!(worker_shutdown.signal, "SIGINT");
}

fn validate_log_filter_keep_regex_body() {
    let toml = r#"
log_filter = ["^global keep"]

[services.api]
run.cmd = "api"
log_filter = ["service keep [0-9]+"]
"#;

    let config: Config = toml.parse().unwrap();
    config.validate(TEST_PLATFORM).unwrap();
    assert_eq!(config.log_filter.patterns, vec!["^global keep"]);
    assert_eq!(
        config.services["api"]
            .resolve(TEST_PLATFORM)
            .log_filter
            .patterns,
        vec!["service keep [0-9]+"]
    );
}

fn validate_log_filter_rejects_invalid_regex_body() {
    let toml = r#"
[services.api]
run.cmd = "api"
log_filter = ["["]
"#;

    let config: Config = toml.parse().unwrap();
    let err = config.validate(TEST_PLATFORM).unwrap_err();
    let ConfigError::Validation { errors } = err else {
        panic!("expected validation error");
    };
    assert!(
        errors
            .iter()
            .any(|e| e.contains("service 'api' log_filter") && e.contains("invalid keep regex")),
        "errors: {errors:?}"
    );
}

fn config_from_file_merges_local_override_body() {
    struct LocalOverrideCase {
        name: &'static str,
        config_name: &'static str,
        local_name: &'static str,
    }

    let cases = vec![
        LocalOverrideCase {
            name: "default don.toml",
            config_name: "don.toml",
            local_name: "don.local.toml",
        },
        LocalOverrideCase {
            name: "custom config path",
            config_name: "workspace.toml",
            local_name: "workspace.local.toml",
        },
    ];

    for case in cases {
        let dir = TempDir::new(&format!("local-override-{}", case.name.replace(' ', "-")));
        let config_path = dir.child(case.config_name);
        std::fs::write(
            &config_path,
            r#"
default_profile = "base"
watch_ignore = ["target/**"]

[services.api]
run.cmd = "api"
run.args = ["serve"]
env = { BASE = "base", SHARED = "base" }
watch = ["src/**"]

[tasks.migrate]
cmd = "migrate"
env = { BASE = "base" }

[profiles.base]
services = ["api"]
tasks = ["migrate"]
"#,
        )
        .unwrap();
        std::fs::write(
            dir.child(case.local_name),
            r#"
default_profile = "local"
watch_ignore = ["node_modules/**"]

[services.api]
dir = "./local-api"
run.args = ["dev"]
env = { SHARED = "local", LOCAL = "yes" }

[services.worker]
run.cmd = "worker"

[tasks.migrate]
args = ["up"]
env = { LOCAL = "yes" }

[profiles.local]
services = ["worker"]
tasks = ["migrate"]
"#,
        )
        .unwrap();

        let config = Config::from_file(&config_path).unwrap();
        config.validate(TEST_PLATFORM).unwrap();

        assert_eq!(
            config.default_profile.as_deref(),
            Some("local"),
            "{}: default_profile should be overridden",
            case.name
        );
        assert_eq!(
            config.watch_ignore,
            vec!["node_modules/**"],
            "{}: arrays should be replaced by local config",
            case.name
        );

        let api = config.services["api"].resolve(TEST_PLATFORM);
        assert_eq!(
            api.dir.as_deref(),
            Some(std::path::Path::new("./local-api"))
        );
        assert_eq!(api.env["BASE"], "base");
        assert_eq!(api.env["SHARED"], "local");
        assert_eq!(api.env["LOCAL"], "yes");
        let Some(ServiceKind::Custom { run, .. }) = api.kind else {
            panic!("{}: expected custom api service", case.name);
        };
        assert_eq!(run.cmd, "api");
        assert_eq!(run.args, vec!["dev"]);

        assert!(config.services.contains_key("worker"));
        assert_eq!(config.tasks["migrate"].cmd, "migrate");
        assert_eq!(config.tasks["migrate"].args, vec!["up"]);
        assert_eq!(config.tasks["migrate"].env["BASE"], "base");
        assert_eq!(config.tasks["migrate"].env["LOCAL"], "yes");
        assert!(config.profiles.contains_key("base"));
        assert_eq!(config.profiles["local"].services, vec!["worker"]);
    }
}

fn malformed_local_override_is_parse_error_body() {
    let dir = TempDir::new("local-override-malformed");
    let config_path = dir.child("don.toml");
    std::fs::write(&config_path, "").unwrap();
    std::fs::write(dir.child("don.local.toml"), "this is not [valid toml").unwrap();

    let err = Config::from_file(&config_path).unwrap_err();
    assert!(matches!(err, ConfigError::Parse(_)));
}

fn validate_task_params_reject_run_flag_collisions_body() {
    let toml = r#"
[tasks.sync]
cmd = "true"

[[tasks.sync.params]]
name = "timeout"
"#;
    let config: Config = toml.parse().unwrap();
    let err = config.validate(TEST_PLATFORM).unwrap_err();
    let ConfigError::Validation { errors } = &err else {
        panic!("expected validation error");
    };
    assert!(
        errors
            .iter()
            .any(|e| e.contains("param name 'timeout'") && e.contains("built-in")),
        "expected reserved param error, got: {errors:?}"
    );
}

fn validate_reconcile_dependents_table_body() {
    let cases = [
        ("valid", "watch = [\"schema/**\"]", true),
        ("requires watch", "", false),
        (
            "requires auto run",
            "watch = [\"schema/**\"]\nauto_run = false",
            false,
        ),
        (
            "rejects params",
            "watch = [\"schema/**\"]\n[[tasks.t.params]]\nname = \"scope\"",
            false,
        ),
        (
            "rejects foreground",
            "watch = [\"schema/**\"]\nterminal = \"foreground\"",
            false,
        ),
        (
            "rejects derived watches",
            "watch = [\"schema/**\"]\nbazel.target = \"//tools:t\"",
            false,
        ),
    ];
    for (name, fields, valid) in cases {
        let toml = format!("[tasks.t]\ncmd = \"echo\"\nreconcile_dependents = true\n{fields}");
        let config: Config = toml.parse().unwrap();
        assert_eq!(config.validate(TEST_PLATFORM).is_ok(), valid, "{name}");
    }
}

fn validate_cycle_detected_body() {
    let dir = TempDir::new("validate-cycle");
    let config_path = ConfigBuilder::new()
        .add_custom_service("a", "a", &[])
        .depends_on(&["b"])
        .done()
        .add_custom_service("b", "b", &[])
        .depends_on(&["a"])
        .write_to(dir.path());

    let config = Config::from_file(&config_path).unwrap();
    let err = config.validate(TEST_PLATFORM).unwrap_err();
    let ConfigError::Validation { errors } = &err else {
        panic!("expected validation error");
    };
    assert!(errors.iter().any(|e| e.contains("dependency cycle")));
}

fn validate_invalid_duration_strings_body() {
    let dir = TempDir::new("validate-duration");
    let config_path = ConfigBuilder::new()
        .raw(
            r#"
            [services.api]
            run.cmd = "api"
            debounce = "not-a-duration"
        "#,
        )
        .write_to(dir.path());

    let config = Config::from_file(&config_path).unwrap();
    let err = config.validate(TEST_PLATFORM).unwrap_err();
    let ConfigError::Validation { errors } = &err else {
        panic!("expected validation error");
    };
    assert!(errors.iter().any(|e| e.contains("invalid debounce")));
}

fn validate_invalid_watch_pattern_body() {
    let dir = TempDir::new("validate-watch-pattern");
    let config_path = ConfigBuilder::new()
        .raw(
            r#"
            [services.api]
            run.cmd = "api"
            watch = ["src/[*.rs"]

            [tasks.build]
            cmd = "make"
            watch = ["lib/[bad"]
        "#,
        )
        .write_to(dir.path());

    let config = Config::from_file(&config_path).unwrap();
    let err = config.validate(TEST_PLATFORM).unwrap_err();
    let ConfigError::Validation { errors } = &err else {
        panic!("expected validation error");
    };
    assert!(
        errors
            .iter()
            .any(|e| e.contains("service 'api'") && e.contains("invalid watch pattern")),
        "expected service watch pattern error, got: {errors:?}"
    );
    assert!(
        errors
            .iter()
            .any(|e| e.contains("task 'build'") && e.contains("invalid watch pattern")),
        "expected task watch pattern error, got: {errors:?}"
    );
}

fn validate_invalid_global_watch_ignore_pattern_body() {
    let dir = TempDir::new("validate-global-watch-ignore-pattern");
    let config_path = ConfigBuilder::new()
        .raw(
            r#"
            watch_ignore = ["generated/[*.rs"]

            [services.api]
            run.cmd = "api"
        "#,
        )
        .write_to(dir.path());

    let config = Config::from_file(&config_path).unwrap();
    let err = config.validate(TEST_PLATFORM).unwrap_err();
    let ConfigError::Validation { errors } = &err else {
        panic!("expected validation error");
    };
    assert!(
        errors
            .iter()
            .any(|e| e.contains("invalid global watch_ignore pattern")),
        "expected global watch_ignore pattern error, got: {errors:?}"
    );
}

fn validate_tcp_ready_check_on_listen_address_warns_body() {
    let toml = ConfigBuilder::new()
        .add_custom_service("api", "mybin", &[])
        .listen(&["0.0.0.0:3000"])
        .ready_tcp("0.0.0.0:3000")
        .done()
        .build();

    let config: Config = toml.parse().unwrap();
    let warnings = config.validate(TEST_PLATFORM).unwrap();
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("TCP ready check") && w.contains("don holds that socket")),
        "expected warning about TCP ready check on listen address, got: {warnings:?}"
    );
}

fn validate_tcp_ready_check_on_different_address_no_warning_body() {
    let toml = ConfigBuilder::new()
        .add_custom_service("api", "mybin", &[])
        .listen(&["0.0.0.0:3000"])
        .ready_tcp("0.0.0.0:4000")
        .done()
        .build();

    let config: Config = toml.parse().unwrap();
    let warnings = config.validate(TEST_PLATFORM).unwrap();
    assert!(
        warnings.is_empty(),
        "expected no warnings when TCP check is on a different address, got: {warnings:?}"
    );
}

fn don_validate_cli_valid_config_body() {
    let dir = TempDir::new("cli-validate-valid");
    ConfigBuilder::new()
        .add_custom_service("api", "mybin", &[])
        .write_to(dir.path());

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_don"))
        .args([
            "--config",
            dir.child("don.toml").to_str().unwrap(),
            "validate",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Config is valid"));
}

fn don_validate_cli_invalid_config_body() {
    let dir = TempDir::new("cli-validate-invalid");
    let config_path = dir.child("don.toml");
    std::fs::write(&config_path, "[services.broken]\nenv = { FOO = \"bar\" }").unwrap();

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_don"))
        .args(["--config", config_path.to_str().unwrap(), "validate"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("validation failed"));
}

fn don_validate_cli_missing_config_body() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_don"))
        .args([
            "--config",
            "/tmp/don-test-nonexistent-12345.toml",
            "validate",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("failed to read config file"));
}

// --- Download validation ---

fn download_toml(extra_lines: &str) -> String {
    format!(
        r#"
[services.tool]
run.cmd = "tool"

[services.tool.download.platform.linux-x86_64]
url = "https://example.com/tool.tar.gz"
sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
path = "tool"
{extra_lines}
"#
    )
}

fn validate_download_bad_sha256_length_body() {
    let toml = r#"
[services.tool]
run.cmd = "tool"

[services.tool.download.platform.linux-x86_64]
url = "https://example.com/tool"
sha256 = "tooshort"
"#;
    let config: Config = toml.parse().unwrap();
    let err = config.validate(TEST_PLATFORM).unwrap_err();
    let ConfigError::Validation { errors } = &err else {
        panic!("expected validation error");
    };
    assert!(
        errors.iter().any(|e| e.contains("64 hex characters")),
        "expected sha256 length error, got: {errors:?}"
    );
}

fn validate_download_bad_url_scheme_body() {
    let toml = r#"
[services.tool]
run.cmd = "tool"

[services.tool.download.platform.linux-x86_64]
url = "file:///etc/passwd"
sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
"#;
    let config: Config = toml.parse().unwrap();
    let err = config.validate(TEST_PLATFORM).unwrap_err();
    let ConfigError::Validation { errors } = &err else {
        panic!("expected validation error");
    };
    assert!(
        errors.iter().any(|e| e.contains("must start with http")),
        "expected url scheme error, got: {errors:?}"
    );
}

fn validate_download_without_run_cmd_body() {
    // Download but no run.cmd → should error.
    let toml = r#"
[services.tool]

[services.tool.download.platform.linux-x86_64]
url = "https://example.com/tool"
sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
"#;
    let config: Config = toml.parse().unwrap();
    let err = config.validate(TEST_PLATFORM).unwrap_err();
    let ConfigError::Validation { errors } = &err else {
        panic!("expected validation error");
    };
    assert!(
        errors
            .iter()
            .any(|e| e.contains("download requires a run command")),
        "expected run command error, got: {errors:?}"
    );
}

fn validate_valid_download_config_passes_body() {
    let toml = download_toml("");
    let config: Config = toml.parse().unwrap();
    assert!(config.validate(TEST_PLATFORM).is_ok());
}

fn validate_download_bin_name_collision_body() {
    // Two services download different binaries that would both link to
    // `.don/bin/cockroach` — must error unless disambiguated.
    let toml = r#"
[services.crdb_v25]
run.cmd = "cockroach"
[services.crdb_v25.download.platform.linux-x86_64]
url = "https://example.com/v25.tgz"
sha256 = "0000000000000000000000000000000000000000000000000000000000000001"
path = "cockroach-v25/cockroach"

[services.crdb_v24]
run.cmd = "cockroach"
[services.crdb_v24.download.platform.linux-x86_64]
url = "https://example.com/v24.tgz"
sha256 = "0000000000000000000000000000000000000000000000000000000000000002"
path = "cockroach-v24/cockroach"
"#;
    let config: Config = toml.parse().unwrap();
    let err = config.validate(TEST_PLATFORM).unwrap_err();
    let ConfigError::Validation { errors } = &err else {
        panic!("expected validation error");
    };
    assert!(
        errors.iter().any(|e| e.contains("bin_name 'cockroach'")
            && e.contains("crdb_v25")
            && e.contains("crdb_v24")),
        "expected bin_name collision error, got: {errors:?}"
    );
}

fn validate_download_bin_name_override_resolves_collision_body() {
    // Same two-crdb setup but with explicit bin_names → passes.
    let toml = r#"
[services.crdb_v25]
run.cmd = "cockroach"
[services.crdb_v25.download]
bin_name = "cockroach-v25"
[services.crdb_v25.download.platform.linux-x86_64]
url = "https://example.com/v25.tgz"
sha256 = "0000000000000000000000000000000000000000000000000000000000000001"
path = "cockroach-v25/cockroach"

[services.crdb_v24]
run.cmd = "cockroach"
[services.crdb_v24.download]
bin_name = "cockroach-v24"
[services.crdb_v24.download.platform.linux-x86_64]
url = "https://example.com/v24.tgz"
sha256 = "0000000000000000000000000000000000000000000000000000000000000002"
path = "cockroach-v24/cockroach"
"#;
    let config: Config = toml.parse().unwrap();
    assert!(
        config.validate(TEST_PLATFORM).is_ok(),
        "explicit bin_names should resolve the collision"
    );
}

fn validate_download_missing_current_platform_warns_body() {
    // TEST_PLATFORM is LinuxX86_64. Provide only macos entries — should warn.
    let toml = r#"
[services.tool]
run.cmd = "tool"

[services.tool.download.platform.macos-aarch64]
url = "https://example.com/tool-mac.tar.gz"
sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
"#;
    let config: Config = toml.parse().unwrap();
    let warnings = config.validate(TEST_PLATFORM).unwrap();
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("no download entry for current platform")
                && w.contains("linux-x86_64")),
        "expected platform warning, got: {warnings:?}"
    );
}

bounded_test!(validate_valid_config, validate_valid_config_body);
bounded_test!(
    validate_invalid_config_unknown_dep,
    validate_invalid_config_unknown_dep_body
);
bounded_test!(
    validate_missing_config_file,
    validate_missing_config_file_body
);
bounded_test!(validate_malformed_toml, validate_malformed_toml_body);
bounded_test!(
    validate_empty_config_is_valid,
    validate_empty_config_is_valid_body
);
bounded_test!(
    validate_config_with_tasks_and_profiles,
    validate_config_with_tasks_and_profiles_body
);
bounded_test!(
    validate_shutdown_graceful_flags,
    validate_shutdown_graceful_flags_body
);
bounded_test!(
    validate_log_filter_keep_regex,
    validate_log_filter_keep_regex_body
);
bounded_test!(
    validate_log_filter_rejects_invalid_regex,
    validate_log_filter_rejects_invalid_regex_body
);
bounded_test!(
    config_from_file_merges_local_override,
    config_from_file_merges_local_override_body
);
bounded_test!(
    malformed_local_override_is_parse_error,
    malformed_local_override_is_parse_error_body
);
bounded_test!(
    validate_task_params_reject_run_flag_collisions,
    validate_task_params_reject_run_flag_collisions_body
);
bounded_test!(validate_cycle_detected, validate_cycle_detected_body);
bounded_test!(
    validate_reconcile_dependents_table,
    validate_reconcile_dependents_table_body
);
bounded_test!(
    validate_invalid_duration_strings,
    validate_invalid_duration_strings_body
);
bounded_test!(
    validate_invalid_watch_pattern,
    validate_invalid_watch_pattern_body
);
bounded_test!(
    validate_invalid_global_watch_ignore_pattern,
    validate_invalid_global_watch_ignore_pattern_body
);
bounded_test!(
    validate_tcp_ready_check_on_listen_address_warns,
    validate_tcp_ready_check_on_listen_address_warns_body
);
bounded_test!(
    validate_tcp_ready_check_on_different_address_no_warning,
    validate_tcp_ready_check_on_different_address_no_warning_body
);
bounded_test!(
    don_validate_cli_valid_config,
    don_validate_cli_valid_config_body
);
bounded_test!(
    don_validate_cli_invalid_config,
    don_validate_cli_invalid_config_body
);
bounded_test!(
    don_validate_cli_missing_config,
    don_validate_cli_missing_config_body
);
bounded_test!(
    validate_download_bad_sha256_length,
    validate_download_bad_sha256_length_body
);
bounded_test!(
    validate_download_bad_url_scheme,
    validate_download_bad_url_scheme_body
);
bounded_test!(
    validate_download_without_run_cmd,
    validate_download_without_run_cmd_body
);
bounded_test!(
    validate_valid_download_config_passes,
    validate_valid_download_config_passes_body
);
bounded_test!(
    validate_download_bin_name_collision,
    validate_download_bin_name_collision_body
);
bounded_test!(
    validate_download_bin_name_override_resolves_collision,
    validate_download_bin_name_override_resolves_collision_body
);
bounded_test!(
    validate_download_missing_current_platform_warns,
    validate_download_missing_current_platform_warns_body
);
