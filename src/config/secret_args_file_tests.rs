#![allow(clippy::unwrap_used)]

use crate::config::{Config, Platform};
use crate::secrets::{SecretKind, SecretStore};
use std::collections::HashMap;

const CONFIG: &str = r#"
default_profile = "dev"
[profiles.dev]
services = ["api"]
[[profiles.dev.overrides.secrets]]
aws-ssm = { profile = "dev", args-file = { path = "secrets.args", group = "development" } }
[[profiles.dev.overrides.secrets]]
aws-ssm = { region = "us-east-1", profile = "dev" }
vars = { DD_API_KEY = "/app/datadog" }
groups = { datadog = ["DD_API_KEY"] }
[services.api]
run.cmd = "true"
[profiles.dev.overrides.services.api]
secrets = ["development"]
[tasks.bootstrap]
cmd = "true"
[profiles.dev.overrides.tasks.bootstrap]
secrets = ["development"]
"#;

#[test]
fn manifest_addition_reaches_bootstrap_and_services_without_a_second_list() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("don.toml");
    let manifest_path = dir.path().join("secrets.args");
    std::fs::write(&config_path, CONFIG).unwrap();
    let original = "--region\nus-east-1\n--var\nSTRIPE_SECRET_KEY\n/app/stripe\n";
    std::fs::write(&manifest_path, original).unwrap();
    let config = Config::from_file(&config_path).unwrap();
    config.validate(Platform::LinuxX86_64).unwrap();
    assert_eq!(
        config.secrets[0].expand(&["development".into()]).unwrap(),
        ["STRIPE_SECRET_KEY"]
    );

    // A new required credential changes only the authoritative manifest.
    std::fs::write(
        &manifest_path,
        format!("{original}--var\nENTRI_SECRET\n/app/entri\n"),
    )
    .unwrap();
    let config = Config::from_file(&config_path).unwrap();
    config.validate(Platform::LinuxX86_64).unwrap();
    assert_eq!(config.secrets[0].vars["ENTRI_SECRET"], "/app/entri");
    let SecretKind::AwsSsm(aws) = &config.secrets[0].kind;
    assert_eq!(aws.region.as_deref(), Some("us-east-1"));
    assert_eq!(aws.profile.as_deref(), Some("dev"));

    let values = HashMap::from([
        ("STRIPE_SECRET_KEY".into(), "stripe-test".into()),
        ("ENTRI_SECRET".into(), "entri-test".into()),
        ("DD_API_KEY".into(), "datadog-test".into()),
    ]);
    let store = SecretStore::from_sources(&config.secrets, values);
    for refs in [
        config.services["api"].secrets.as_deref().unwrap(),
        config.tasks["bootstrap"].secrets.as_deref().unwrap(),
    ] {
        let mut env = HashMap::from([("DD_API_KEY".into(), "inherited-test".into())]);
        store.apply(&mut env, refs);
        assert_eq!(env["ENTRI_SECRET"], "entri-test");
        assert_eq!(env["STRIPE_SECRET_KEY"], "stripe-test");
        assert!(!env.contains_key("DD_API_KEY"));
    }
    let mut telemetry_env = HashMap::from([("ENTRI_SECRET".into(), "inherited-test".into())]);
    store.apply(&mut telemetry_env, &["datadog".into()]);
    assert_eq!(
        telemetry_env,
        HashMap::from([("DD_API_KEY".into(), "datadog-test".into())])
    );
}

#[test]
fn file_loading_table() {
    struct Case {
        name: &'static str,
        source: &'static str,
        manifest: Option<&'static str>,
        error: Option<&'static str>,
    }
    for case in [
        Case {
            name: "ambient credentials and identical duplicate",
            source: "aws-ssm = { args-file = { path = 'secrets.args', group = 'dev' } }",
            manifest: Some("--region\nus-east-1\n--var\nKEY\n/app/key\n--var\nKEY\n/app/key\n"),
            error: None,
        },
        Case {
            name: "missing file",
            source: "aws-ssm = { args-file = { path = 'secrets.args', group = 'dev' } }",
            manifest: None,
            error: Some("failed to read SSM args-file"),
        },
        Case {
            name: "malformed file",
            source: "aws-ssm = { args-file = { path = 'secrets.args', group = 'dev' } }",
            manifest: Some("--var\nKEY"),
            error: Some("line 1: --var needs a path"),
        },
        Case {
            name: "inline region",
            source: "aws-ssm = { region = 'us-east-1', args-file = { path = 'secrets.args', group = 'dev' } }",
            manifest: None,
            error: Some("cannot be combined"),
        },
        Case {
            name: "inline vars",
            source: "aws-ssm = { args-file = { path = 'secrets.args', group = 'dev' } }\nvars = { KEY = '/app/key' }",
            manifest: None,
            error: Some("cannot be combined"),
        },
        Case {
            name: "inline groups",
            source: "aws-ssm = { args-file = { path = 'secrets.args', group = 'dev' } }\ngroups = { other = ['KEY'] }",
            manifest: None,
            error: Some("cannot be combined"),
        },
        Case {
            name: "group collides with var",
            source: "aws-ssm = { args-file = { path = 'secrets.args', group = 'KEY' } }",
            manifest: Some("--region\nus-east-1\n--var\nKEY\n/app/key"),
            error: Some("same name as a var"),
        },
    ] {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("don.toml");
        std::fs::write(
            &config_path,
            format!(
                "[[secrets]]\n{}\n[services.api]\nrun.cmd = 'true'",
                case.source
            ),
        )
        .unwrap();
        if let Some(manifest) = case.manifest {
            std::fs::write(dir.path().join("secrets.args"), manifest).unwrap();
        }
        let result = Config::from_file(&config_path);
        if let Some(message) = case.error {
            let error = result.unwrap_err().to_string();
            assert!(
                error.contains(message),
                "{}: expected {message} in {error}",
                case.name
            );
        } else {
            let config = result.unwrap();
            config.validate(Platform::LinuxX86_64).unwrap();
            assert_eq!(config.secrets[0].expand(&["dev".into()]).unwrap(), ["KEY"]);
            let SecretKind::AwsSsm(aws) = &config.secrets[0].kind;
            assert_eq!(aws.profile, None);
        }
    }
}

#[test]
fn local_override_selects_the_file_before_it_is_read() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("don.toml"), CONFIG).unwrap();
    std::fs::write(
        dir.path().join("don.local.toml"),
        "[[secrets]]\naws-ssm = { args-file = { path = 'local.args', group = 'development' } }",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("local.args"),
        "--region\nus-east-2\n--var\nKEY\n/local/key",
    )
    .unwrap();
    let config = Config::from_file(&dir.path().join("don.toml")).unwrap();
    config.validate(Platform::LinuxX86_64).unwrap();
    assert_eq!(config.secrets.len(), 1);
    assert_eq!(config.secrets[0].vars["KEY"], "/local/key");
}
