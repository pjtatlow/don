#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
//! Docker integration tests — require a running Docker daemon.
//!
//! Set `DON_TEST_DOCKER=1` to enable. Skipped by default.

mod helpers;

use don::config::{Config, LogConfig, Platform};
use don::output::OutputManager;
use don::ports::{manifest_path, read_manifest};
use don::runner::Runner;
use helpers::config::parse_config;
use helpers::tempdir::TempDir;
use helpers::timeout::run_with_timeout;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

const PLATFORM: Platform = Platform::LinuxX86_64;

fn docker_available() -> bool {
    std::env::var("DON_TEST_DOCKER").is_ok()
}

macro_rules! skip_unless_docker {
    () => {
        if !docker_available() {
            eprintln!("skipping: DON_TEST_DOCKER not set");
            return;
        }
    };
}

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
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Clean up a test container if it exists (best-effort).
async fn cleanup_container(name: &str) {
    if let Ok(docker) = bollard::Docker::connect_with_socket_defaults() {
        use bollard::query_parameters::{
            RemoveContainerOptionsBuilder, StopContainerOptionsBuilder,
        };
        let stop_opts = StopContainerOptionsBuilder::new().t(1).build();
        let _ = docker.stop_container(name, Some(stop_opts)).await;
        let rm_opts = RemoveContainerOptionsBuilder::new().force(true).build();
        let _ = docker.remove_container(name, Some(rm_opts)).await;
    }
}

// --- Docker service starts and produces output ---

#[test]
fn docker_service_starts_and_outputs() {
    skip_unless_docker!();

    run_with_timeout(Duration::from_secs(30), async {
        let dir = TempDir::new("docker-start");
        let container_name = "don-test-start";

        // Clean up from any previous failed run.
        cleanup_container(container_name).await;

        let toml = format!(
            r#"
[services.echo-svc]
docker.image = "alpine:latest"
docker.container = "{container_name}"
docker.command = ["echo", "hello from docker"]
"#
        );

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Wait for the echo output to appear.
        let found = wait_for_output(&buf, "hello from docker", Duration::from_secs(15)).await;
        assert!(
            found,
            "expected 'hello from docker' in output. got: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();

        // Container should be cleaned up.
        cleanup_container(container_name).await;
    });
}

// --- Docker service receives docker.env_file vars ---

#[test]
fn docker_service_env_file_reaches_container() {
    skip_unless_docker!();

    run_with_timeout(Duration::from_secs(30), async {
        let dir = TempDir::new("docker-envfile");
        let container_name = "don-test-envfile";

        cleanup_container(container_name).await;

        // A docker-scoped env_file. Regression: this used to be parsed and then
        // silently dropped, never reaching the container.
        let env_file = dir.path().join("container.env");
        std::fs::write(&env_file, "DON_ENVFILE_TEST=envfile_works\n").unwrap();

        let toml = format!(
            r#"
[services.envcheck]
docker.image = "alpine:latest"
docker.container = "{container_name}"
docker.env_file = ["{env_file}"]
docker.command = ["sh", "-c", "echo value=$DON_ENVFILE_TEST"]
"#,
            env_file = env_file.display()
        );

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        let found = wait_for_output(&buf, "value=envfile_works", Duration::from_secs(15)).await;
        assert!(
            found,
            "expected env var from docker.env_file in output. got: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();

        cleanup_container(container_name).await;
    });
}

// --- Docker service built from a Dockerfile with no explicit image ---

#[test]
fn docker_service_builds_from_dockerfile_without_image() {
    skip_unless_docker!();

    run_with_timeout(Duration::from_secs(60), async {
        // Temp dir and container name must be unique per test: TempDir paths are
        // only namespaced by PID, and docker container names are global. Sharing
        // either with `docker_build_and_run` means one test can delete the
        // other's state.
        let dir = TempDir::new("docker-build-no-image");
        let container_name = "don-test-build-no-image";

        cleanup_container(container_name).await;
        // Remove any image left over from a previous run so we prove the build ran.
        if let Ok(docker) = bollard::Docker::connect_with_socket_defaults() {
            use bollard::query_parameters::RemoveImageOptionsBuilder;
            let _ = docker
                .remove_image(
                    "don-buildsvc",
                    Some(RemoveImageOptionsBuilder::new().force(true).build()),
                    None,
                )
                .await;
        }

        // A Dockerfile with no corresponding public image — don must build it.
        std::fs::write(
            dir.path().join("Dockerfile"),
            "FROM alpine:latest\nCMD [\"echo\", \"built-and-running\"]\n",
        )
        .unwrap();

        // No `docker.image`: the image is built from the Dockerfile and tagged
        // don-<service> automatically.
        let toml = format!(
            r#"
[services.buildsvc]
docker.container = "{container_name}"
docker.build.context = "."
"#
        );

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        let found = wait_for_output(&buf, "built-and-running", Duration::from_secs(45)).await;
        assert!(
            found,
            "expected output from the built image. got: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
        cleanup_container(container_name).await;
    });
}

// --- Docker service with port mapping ---

#[test]
fn docker_service_with_port_mapping() {
    skip_unless_docker!();

    run_with_timeout(Duration::from_secs(30), async {
        let dir = TempDir::new("docker-ports");
        let container_name = "don-test-ports";
        let port = helpers::port::free_port();

        cleanup_container(container_name).await;

        // Use nginx to serve on a mapped port.
        let toml = format!(
            r#"
[services.web]
docker.image = "nginx:alpine"
docker.container = "{container_name}"
docker.ports = ["{port}:80"]
ready.http = "http://127.0.0.1:{port}/"
ready.interval = "500ms"
ready.retries = 20
"#
        );

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Wait for ready check to pass.
        let found = wait_for_output(&buf, "ready", Duration::from_secs(20)).await;
        assert!(
            found,
            "expected service to become ready. got: {}",
            read_buf(&buf)
        );

        // Verify we can connect to the mapped port.
        let resp = reqwest::get(&format!("http://127.0.0.1:{port}/")).await;
        assert!(resp.is_ok(), "expected HTTP request to succeed");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
        cleanup_container(container_name).await;
    });
}

// --- Occupied Docker host port falls back and publishes the actual mapping ---

#[test]
fn docker_service_falls_back_from_occupied_host_port() {
    skip_unless_docker!();

    run_with_timeout(Duration::from_secs(40), async {
        let dir = TempDir::new("docker-port-fallback");
        let container_name = "don-test-port-fallback";
        let blocker = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let preferred_port = blocker.local_addr().unwrap().port();

        cleanup_container(container_name).await;

        let toml = format!(
            r#"
fallback_ports = true

[services.web]
docker.image = "nginx:alpine"
docker.container = "{container_name}"
docker.ports = ["127.0.0.1:{preferred_port}:80"]
ready.http = "http://127.0.0.1:{preferred_port}/"
ready.interval = "200ms"
ready.retries = 50

[services.consumer]
run.cmd = "bash"
run.args = ["-c", "echo WEB_PORT=$WEB_PORT; exec sleep 60"]
depends_on = ["web"]
env = {{ WEB_PORT = "$(web.PORT_80)" }}
ready.exec.cmd = "true"
"#
        );

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "all services running", Duration::from_secs(25)).await,
            "expected Docker fallback service to become ready. output: {}",
            read_buf(&buf)
        );

        let manifest = read_manifest(dir.path()).unwrap();
        let binding = manifest
            .services
            .get("web")
            .and_then(|ports| ports.docker.first())
            .expect("missing Docker runtime port binding");
        let actual_addr: std::net::SocketAddr = binding.host_addr.parse().unwrap();
        assert_ne!(actual_addr.port(), preferred_port);
        assert_eq!(binding.container_port, "80");
        assert!(
            wait_for_output(
                &buf,
                &format!("WEB_PORT={}", actual_addr.port()),
                Duration::from_secs(3)
            )
            .await,
            "dependent service did not receive the actual Docker port. output: {}",
            read_buf(&buf)
        );

        let response = reqwest::get(format!("http://127.0.0.1:{}/", actual_addr.port())).await;
        assert!(response.is_ok(), "expected fallback HTTP port to respond");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
        assert!(!manifest_path(dir.path()).exists());
        cleanup_container(container_name).await;
        drop(blocker);
    });
}

// --- Stale container cleanup ---

#[test]
fn docker_stale_container_cleaned_up() {
    skip_unless_docker!();

    run_with_timeout(Duration::from_secs(30), async {
        let dir = TempDir::new("docker-stale");
        let container_name = "don-test-stale";

        // Pre-create a stale container.
        let docker = bollard::Docker::connect_with_socket_defaults().unwrap();
        cleanup_container(container_name).await;

        use bollard::models::ContainerCreateBody;
        use bollard::query_parameters::CreateContainerOptionsBuilder;
        let create_options = CreateContainerOptionsBuilder::new()
            .name(container_name)
            .build();
        let config = ContainerCreateBody {
            image: Some("alpine:latest".to_string()),
            cmd: Some(vec!["sleep".to_string(), "300".to_string()]),
            ..Default::default()
        };
        docker
            .create_container(Some(create_options), config)
            .await
            .unwrap();
        docker.start_container(container_name, None).await.unwrap();

        // Verify the stale container is running.
        let info = docker
            .inspect_container(container_name, None)
            .await
            .unwrap();
        assert!(
            info.state.as_ref().and_then(|s| s.running).unwrap_or(false),
            "stale container should be running"
        );

        // Now start don with the same container name — it should clean up the stale one.
        let toml = format!(
            r#"
[services.db]
docker.image = "alpine:latest"
docker.container = "{container_name}"
docker.command = ["echo", "fresh start"]
"#
        );

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Wait for the fresh container's output.
        let found = wait_for_output(&buf, "fresh start", Duration::from_secs(15)).await;
        assert!(
            found,
            "expected 'fresh start' in output (stale container should have been replaced). got: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
        cleanup_container(container_name).await;
    });
}

// --- Docker build + run ---

#[test]
fn docker_build_and_run() {
    skip_unless_docker!();

    run_with_timeout(Duration::from_secs(60), async {
        let dir = TempDir::new("docker-build");
        let container_name = "don-test-build";

        cleanup_container(container_name).await;

        // Create a minimal Dockerfile.
        std::fs::write(
            dir.path().join("Dockerfile"),
            "FROM alpine:latest\nCMD [\"echo\", \"built and running\"]\n",
        )
        .unwrap();

        let toml = format!(
            r#"
[services.app]
docker.image = "don-test-build-img:latest"
docker.container = "{container_name}"
docker.build.context = "."
"#
        );

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        let found = wait_for_output(&buf, "built and running", Duration::from_secs(30)).await;
        assert!(
            found,
            "expected 'built and running' in output. got: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
        cleanup_container(container_name).await;

        // Clean up the built image.
        if let Ok(docker) = bollard::Docker::connect_with_socket_defaults() {
            let _ = docker
                .remove_image(
                    "don-test-build-img:latest",
                    None::<bollard::query_parameters::RemoveImageOptions>,
                    None,
                )
                .await;
        }
    });
}
