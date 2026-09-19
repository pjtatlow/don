#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
//! End-to-end tests for the web UI server.
//!
//! These drive the real router over a real loopback socket, because the two
//! things most worth testing here — the origin guard and the proxying to a
//! project's unix socket — only exist at that boundary.

mod helpers;

use don::config::{Config, LogConfig, Platform};
use don::daemon::ProjectEntry;
use helpers::config::ConfigBuilder;
use helpers::tempdir::TempDir;
use helpers::timeout::run_with_timeout;
use std::path::Path;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

const PLATFORM: Platform = Platform::LinuxX86_64;

/// A web UI serving one project, plus the runner behind it.
struct Harness {
    port: u16,
    project_id: String,
    runner_shutdown: mpsc::Sender<()>,
    runner: tokio::task::JoinHandle<()>,
    _web_shutdown: tokio::sync::watch::Sender<bool>,
}

impl Harness {
    async fn start(project_dir: &Path) -> Self {
        std::fs::create_dir_all(project_dir).unwrap();
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let config: Config = toml.parse().unwrap();
        config.validate(PLATFORM).unwrap();
        let service_configs: Vec<(&str, &LogConfig)> = config
            .services
            .iter()
            .map(|(n, s)| (n.as_str(), &s.log))
            .collect();
        let output_manager = don::output::OutputManager::new(&service_configs, tokio::io::sink())
            .await
            .unwrap();
        let (runner_shutdown, shutdown_rx) = mpsc::channel(2);
        let mut runner = don::runner::Runner::new(
            config,
            PLATFORM,
            output_manager,
            project_dir.to_path_buf(),
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
        let runner_handle = tokio::spawn(async move {
            let _ = runner.run().await;
        });

        // Wait for the project's API socket, which is what the web layer proxies to.
        let socket = project_dir.join(".don").join("don.sock");
        let deadline = Instant::now() + Duration::from_secs(5);
        while !socket.exists() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(socket.exists(), "project socket never appeared");

        // The socket now exists from before `run()` is even polled, so its
        // presence no longer implies the runner has finished starting.
        // `GET /ready` is that signal — without waiting on it, a control
        // action below can land mid-startup and race the sweep.
        let client = don::client::Client::with_socket_path(socket.clone());
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if client.ready().await.unwrap_or(false) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let root = std::fs::canonicalize(project_dir).unwrap();
        let entry = ProjectEntry::new(root, std::process::id(), None);
        let project_id = entry.id.clone();

        let (listener, addr) = don::web::bind(([127, 0, 0, 1], 0).into()).await.unwrap();
        let (web_shutdown, web_shutdown_rx) = tokio::sync::watch::channel(false);
        tokio::spawn(don::web::serve_single(
            listener,
            entry.into(),
            web_shutdown_rx,
        ));

        Self {
            port: addr.port(),
            project_id,
            runner_shutdown,
            runner: runner_handle,
            _web_shutdown: web_shutdown,
        }
    }

    async fn stop(self) {
        let _ = self.runner_shutdown.send(()).await;
        let _ = tokio::time::timeout(Duration::from_secs(10), self.runner).await;
    }

    /// Issue a request that the origin guard should accept.
    async fn get(&self, path: &str) -> (u16, String) {
        self.request("GET", path, "localhost").await
    }

    async fn request(&self, method: &str, path: &str, host: &str) -> (u16, String) {
        let mut stream = TcpStream::connect(("127.0.0.1", self.port)).await.unwrap();
        let host_header = if host.contains(':') || host.is_empty() {
            host.to_string()
        } else {
            format!("{host}:{}", self.port)
        };
        let request = format!(
            "{method} {path} HTTP/1.1\r\n\
             Host: {host_header}\r\n\
             Content-Length: 0\r\n\
             Connection: close\r\n\r\n"
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        parse_response(&response)
    }
}

fn parse_response(bytes: &[u8]) -> (u16, String) {
    let text = String::from_utf8_lossy(bytes);
    let status = text
        .lines()
        .next()
        .and_then(|line| line.split(' ').nth(1))
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("unparseable response: {text:?}"));
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    (status, body)
}

#[test]
fn integration_web_api_proxies_to_the_project() {
    run_with_timeout(Duration::from_secs(30), async {
        let dir = TempDir::new("web-proxy");
        let harness = Harness::start(&dir.child("proj")).await;

        let (status, body) = harness.get("/api/projects").await;
        assert_eq!(status, 200, "body: {body}");
        assert!(body.contains(&harness.project_id), "body: {body}");
        assert!(body.contains("\"name\":\"proj\""), "body: {body}");

        let (status, body) = harness
            .get(&format!("/api/projects/{}/status", harness.project_id))
            .await;
        assert_eq!(status, 200, "body: {body}");
        assert!(body.contains("\"name\":\"keeper\""), "body: {body}");

        // A control action must actually reach the runner.
        let (status, body) = harness
            .request(
                "POST",
                &format!("/api/projects/{}/stop/keeper", harness.project_id),
                "localhost",
            )
            .await;
        assert_eq!(status, 204, "body: {body}");

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let (_, body) = harness
                .get(&format!("/api/projects/{}/status", harness.project_id))
                .await;
            if body.contains("\"state\":\"stopped\"") || Instant::now() > deadline {
                assert!(
                    body.contains("\"state\":\"stopped\""),
                    "stop via the web api should reach the runner; body: {body}"
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        harness.stop().await;
    });
}

#[test]
fn integration_web_api_rejects_rebound_requests() {
    run_with_timeout(Duration::from_secs(30), async {
        let dir = TempDir::new("web-origin");
        let harness = Harness::start(&dir.child("proj")).await;

        // The UI doesn't authenticate — anything that can reach the port is
        // already on this machine. What it does refuse is a request that
        // isn't addressed to it, which is what a DNS-rebound request from a
        // page the user merely visited looks like.
        struct Case {
            name: &'static str,
            host: &'static str,
            expect: u16,
        }

        let cases = vec![
            Case {
                name: "localhost is served",
                host: "localhost",
                expect: 200,
            },
            Case {
                name: "127.0.0.1 is served",
                host: "127.0.0.1",
                expect: 200,
            },
            // A reverse proxy forwards the port it was reached on, not the
            // one this server bound. Don's own proxy does exactly this when
            // the daemon runs behind `proxy = { ... }` — checking the port
            // here would break the repo's own `don start`.
            Case {
                name: "a loopback host on a proxy's port is served",
                host: "127.0.0.1:59999",
                expect: 200,
            },
            Case {
                name: "a rebound hostname is refused",
                host: "evil.example.com",
                expect: 421,
            },
            Case {
                name: "a hostname that resolves to loopback is still refused",
                host: "localtest.me",
                expect: 421,
            },
            Case {
                name: "missing host header",
                host: "",
                expect: 421,
            },
        ];

        for case in cases {
            let (status, _) = harness.request("GET", "/api/projects", case.host).await;
            assert_eq!(status, case.expect, "case: {}", case.name);
        }

        harness.stop().await;
    });
}

#[test]
fn integration_web_serves_the_app_shell_for_unknown_routes() {
    run_with_timeout(Duration::from_secs(30), async {
        let dir = TempDir::new("web-assets");
        let harness = Harness::start(&dir.child("proj")).await;

        // Client-side routes must return the shell so a deep link works.
        // The bundle isn't committed, so a fresh clone won't have one until
        // npm has run; CI builds it first, which is where this bites.
        let (status, body) = harness.get("/projects/whatever").await;
        if status == 500 && body.contains("without its web ui bundle") {
            eprintln!(
                "skipping: no web ui bundle — run `npm --prefix web run build` to exercise this"
            );
            harness.stop().await;
            return;
        }
        assert_eq!(status, 200);
        assert!(
            body.contains("<html"),
            "expected the app shell; got: {body}"
        );

        // A missing asset must fail loudly rather than being handed HTML.
        let (status, _) = harness.get("/assets/missing.js").await;
        assert_eq!(status, 404);

        harness.stop().await;
    });
}

#[test]
fn integration_web_api_404s_for_projects_it_does_not_serve() {
    run_with_timeout(Duration::from_secs(30), async {
        let dir = TempDir::new("web-unknown");
        let harness = Harness::start(&dir.child("proj")).await;

        // `--with-ui` serves exactly one project; anything else is unknown,
        // which is what stops one project's UI from driving another's stack.
        for path in [
            "/api/projects/deadbeefcafe/status",
            "/api/projects/deadbeefcafe/logs/keeper",
            "/api/projects/deadbeefcafe/ports",
        ] {
            let (status, body) = harness.get(path).await;
            assert_eq!(status, 404, "path {path} body: {body}");
            assert!(body.contains("no running project"), "body: {body}");
        }

        harness.stop().await;
    });
}
