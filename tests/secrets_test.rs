#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod helpers;

use don::config::{Config, LogConfig, Platform};
use don::output::OutputManager;
use don::runner::Runner;
use helpers::config::ConfigBuilder;
use helpers::tempdir::TempDir;
use helpers::timeout::run_with_timeout;
use serde::Deserialize;
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
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

#[derive(Deserialize)]
struct GetParametersRequest {
    #[serde(rename = "Names")]
    names: Vec<String>,
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn content_length(headers: &str) -> usize {
    for line in headers.lines() {
        let line = line.trim();
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("content-length") {
            return value.trim().parse().unwrap_or(0);
        }
    }
    0
}

async fn read_http_request(stream: &mut tokio::net::TcpStream) -> std::io::Result<Option<Vec<u8>>> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            return Ok(if buf.is_empty() { None } else { Some(buf) });
        }
        buf.extend_from_slice(&tmp[..n]);
        let Some(header_end) = find_header_end(&buf) else {
            continue;
        };
        let headers = std::str::from_utf8(&buf[..header_end]).unwrap_or("");
        let body_start = header_end + 4;
        let needed = body_start + content_length(headers);
        while buf.len() < needed {
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
        }
        return Ok(Some(buf));
    }
}

fn get_parameters_response(body: &[u8], values: &HashMap<String, String>) -> String {
    let request: GetParametersRequest = match serde_json::from_slice(body) {
        Ok(request) => request,
        Err(_) => GetParametersRequest { names: Vec::new() },
    };
    let mut parameters = Vec::new();
    let mut invalid = Vec::new();
    for name in request.names {
        match values.get(&name) {
            Some(value) => parameters.push(serde_json::json!({
                "Name": name,
                "Type": "SecureString",
                "Value": value,
            })),
            None => invalid.push(name),
        }
    }
    serde_json::json!({
        "Parameters": parameters,
        "InvalidParameters": invalid,
    })
    .to_string()
}

async fn serve_ssm(listener: TcpListener, values: HashMap<String, String>) {
    let values = Arc::new(values);
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        let values = Arc::clone(&values);
        tokio::spawn(async move {
            let Some(request) = read_http_request(&mut stream).await.ok().flatten() else {
                return;
            };
            let header_end = find_header_end(&request).unwrap_or(request.len());
            let body = if header_end + 4 <= request.len() {
                &request[header_end + 4..]
            } else {
                &[]
            };
            let payload = get_parameters_response(body, &values);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/x-amz-json-1.1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                payload.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.shutdown().await;
        });
    }
}

struct EnvRestore(Vec<(&'static str, Option<String>)>);

impl EnvRestore {
    fn apply(pairs: &[(&'static str, Option<&str>)]) -> Self {
        let mut prior = Vec::new();
        for (key, value) in pairs {
            prior.push((*key, std::env::var(key).ok()));
            unsafe {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
        Self(prior)
    }
}

impl Drop for EnvRestore {
    fn drop(&mut self) {
        for (key, value) in &self.0 {
            unsafe {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }
}

#[test]
fn integration_declared_secrets_are_injected_stripped_and_redacted() {
    run_with_timeout(Duration::from_secs(20), async {
        let dir = TempDir::new("secrets-inject");

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

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let mut values = HashMap::new();
        values.insert(
            "/app/StripeSecretKey".to_string(),
            "injected-secret-value".to_string(),
        );
        values.insert(
            "/app/Datadog/ApiKey".to_string(),
            "dd-api-key-value".to_string(),
        );
        tokio::spawn(serve_ssm(listener, values));

        let _env = EnvRestore::apply(&[
            ("AWS_ACCESS_KEY_ID", Some("test")),
            ("AWS_SECRET_ACCESS_KEY", Some("test")),
            ("AWS_SESSION_TOKEN", None),
            ("AWS_PROFILE", None),
            ("AWS_DEFAULT_REGION", Some("us-east-1")),
            ("AWS_REGION", Some("us-east-1")),
            ("AWS_EC2_METADATA_DISABLED", Some("true")),
            ("AWS_ENDPOINT_URL", Some(endpoint.as_str())),
            ("AWS_ENDPOINT_URL_SSM", Some(endpoint.as_str())),
            ("AWS_CONFIG_FILE", Some("/dev/null")),
            ("AWS_SHARED_CREDENTIALS_FILE", Some("/dev/null")),
            ("STRIPE_SECRET_KEY", Some("from-shell")),
            ("DD_API_KEY", Some("from-shell")),
        ]);

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
    });
}
