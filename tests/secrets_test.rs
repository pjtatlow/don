#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod helpers;

use don::config::{Config, LogConfig, Platform};
use don::output::OutputManager;
use don::runner::Runner;
use helpers::config::ConfigBuilder;
use helpers::tempdir::TempDir;
use helpers::timeout::run_with_timeout;
use std::os::unix::fs::PermissionsExt;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

const PLATFORM: Platform = Platform::LinuxX86_64;

#[derive(Clone)]
struct TestBuffer(Arc<Mutex<Vec<u8>>>);

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

fn write_fake_aws(dir: &std::path::Path) -> std::path::PathBuf {
    let aws = dir.join("aws");
    std::fs::write(
        &aws,
        r#"#!/usr/bin/env python3
import json, sys
args = sys.argv[1:]
names = []
if "--names" in args:
    i = args.index("--names") + 1
    while i < len(args) and not args[i].startswith("-"):
        names.append(args[i])
        i += 1
values = {
    "/app/StripeSecretKey": "injected-secret-value",
    "/app/Datadog/ApiKey": "dd-api-key-value",
}
json.dump(
    {
        "Parameters": [{"Name": n, "Value": values[n]} for n in names],
        "InvalidParameters": [],
    },
    sys.stdout,
)
"#,
    )
    .unwrap();
    std::fs::set_permissions(&aws, PermissionsExt::from_mode(0o755)).unwrap();
    aws
}

#[test]
fn integration_declared_secrets_are_injected_stripped_and_redacted() {
    run_with_timeout(Duration::from_secs(20), async {
        let dir = TempDir::new("secrets-inject");
        let aws = write_fake_aws(dir.path());

        std::fs::write(
            dir.child("check.sh"),
            r#"#!/bin/sh
if [ "$STRIPE_SECRET_KEY" = "injected-secret-value" ]; then
  echo STRIPE=ok
else
  echo STRIPE=bad
fi
echo "DD=${DD_API_KEY:-empty}"
echo leaked=injected-secret-value
exec sleep 60
"#,
        )
        .unwrap();
        std::fs::set_permissions(dir.child("check.sh"), PermissionsExt::from_mode(0o755)).unwrap();

        let toml = ConfigBuilder::new()
            .raw(
                r#"
[[secrets]]
aws-ssm = { region = "us-east-1" }
[secrets.vars]
STRIPE_SECRET_KEY = "/app/StripeSecretKey"
DD_API_KEY = "/app/Datadog/ApiKey"
"#,
            )
            .add_custom_service("api", "./check.sh", &[])
            .secrets(&["STRIPE_SECRET_KEY"])
            .done()
            .build();
        std::fs::write(dir.child("don.toml"), &toml).unwrap();

        let original_aws = std::env::var("KEY_AWS").ok();
        unsafe {
            std::env::set_var("KEY_AWS", aws.to_string_lossy().as_ref());
            std::env::set_var("STRIPE_SECRET_KEY", "from-shell");
            std::env::set_var("DD_API_KEY", "from-shell");
        }

        let config = Config::from_file(&dir.child("don.toml")).unwrap();
        config.validate(PLATFORM).unwrap();
        let service_configs: Vec<(&str, &LogConfig)> = config
            .services
            .iter()
            .map(|(n, s)| (n.as_str(), &s.log))
            .collect();
        let buf = Arc::new(Mutex::new(Vec::new()));
        let output_manager = OutputManager::new(&service_configs, TestBuffer(buf.clone()))
            .await
            .unwrap();
        let (shutdown_tx, shutdown_rx) = mpsc::channel(2);
        let runner = Runner::new(
            config,
            PLATFORM,
            output_manager,
            dir.path().to_path_buf(),
            None,
            shutdown_rx,
            true,
        )
        .await
        .unwrap();
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        loop {
            let output = read_buf(&buf);
            if output.contains("STRIPE=ok")
                && output.contains("DD=empty")
                && output.contains("leaked=***")
                && !output.contains("injected-secret-value")
            {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                let _ = shutdown_tx.send(()).await;
                panic!("missing inject/strip/redact lines in output:\n{output}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
        unsafe {
            match original_aws {
                Some(value) => std::env::set_var("KEY_AWS", value),
                None => std::env::remove_var("KEY_AWS"),
            }
            std::env::remove_var("STRIPE_SECRET_KEY");
            std::env::remove_var("DD_API_KEY");
        }
    });
}
