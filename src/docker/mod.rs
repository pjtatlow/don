//! Docker service lifecycle — container creation, starting, stopping, and log streaming.
//!
//! Uses the bollard crate to communicate with the Docker daemon via its Unix socket.
//! Each Docker service gets a [`DockerHandle`] that wraps the container ID and
//! provides stop/remove operations analogous to process cleanup.

pub(crate) mod build;
pub(crate) mod parse;
pub(crate) mod stream;

use bollard::Docker;
use bollard::models::{ContainerCreateBody, HostConfig};
use bollard::query_parameters::{
    CreateContainerOptionsBuilder, LogsOptionsBuilder, RemoveContainerOptionsBuilder,
    StopContainerOptionsBuilder,
};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::Path;
use std::time::Duration;

use crate::config::service::DockerConfig;
use crate::sys::ChildOutput;
use stream::DockerLogReader;

/// Errors from Docker operations.
#[derive(Debug, thiserror::Error)]
pub enum DockerError {
    #[error("docker API error: {0}")]
    Api(#[from] bollard::errors::Error),
    #[error("docker build failed: {0}")]
    BuildFailed(String),
    #[error("failed to create build context: {0}")]
    Tar(#[source] std::io::Error),
    #[error("invalid port mapping '{0}': {1}")]
    InvalidPort(String, String),
    #[error("env file error: {0}")]
    EnvFile(#[source] std::io::Error),
    #[error("failed to probe host port for mapping '{mapping}': {source}")]
    PortProbe {
        mapping: String,
        #[source]
        source: std::io::Error,
    },
    #[error("Docker did not report a runtime port for mapping '{mapping}'")]
    MissingPortBinding { mapping: String },
    #[error("Docker reported invalid host port '{value}' for mapping '{mapping}'")]
    InvalidRuntimePort { mapping: String, value: String },
    #[error("Docker reported invalid host IP '{value}' for mapping '{mapping}'")]
    InvalidRuntimeHostIp { mapping: String, value: String },
    #[error(
        "Docker {operation} failed: {failure}; additionally failed to remove container: {cleanup}"
    )]
    CleanupAfterFailure {
        operation: &'static str,
        failure: String,
        cleanup: bollard::errors::Error,
    },
}

/// An authoritative Docker host-port binding observed after container start.
///
/// `host_addr` describes the interface Docker bound. `connect_addr` converts
/// wildcard interfaces to loopback for local clients and dependent services.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DockerPortBinding {
    pub(crate) configured: String,
    pub(crate) configured_host_port: u16,
    pub(crate) host_ip: IpAddr,
    pub(crate) host_port: u16,
    pub(crate) container_port: u16,
    pub(crate) protocol: String,
}

impl DockerPortBinding {
    pub(crate) fn host_addr(&self) -> SocketAddr {
        SocketAddr::new(self.host_ip, self.host_port)
    }

    pub(crate) fn connect_addr(&self) -> SocketAddr {
        let ip = match self.host_ip {
            IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
            ip => ip,
        };
        SocketAddr::new(ip, self.host_port)
    }

    pub(crate) fn used_fallback(&self) -> bool {
        self.configured_host_port == 0 || self.host_port != self.configured_host_port
    }
}

/// Standard `DON_PUBLIC_*` variables for a service's Docker mappings. Generic
/// names refer to the first mapping; every mapping also gets an indexed name.
///
/// Free function so it can be unit-tested without a live Docker client.
/// Render each binding the way `don status -v` shows it. Formatted once, at
/// the moment custody is recorded, so the projection carries display-ready
/// lines instead of the bindings themselves.
pub(crate) fn describe_port_bindings(bindings: &[DockerPortBinding]) -> Vec<String> {
    bindings
        .iter()
        .map(|binding| {
            format!(
                "{} → {} ({}/{})",
                binding.configured,
                binding.connect_addr(),
                binding.container_port,
                binding.protocol
            )
        })
        .collect()
}

pub(crate) fn public_env_vars(bindings: &[DockerPortBinding]) -> HashMap<String, String> {
    let mut vars = HashMap::new();
    for (index, binding) in bindings.iter().enumerate() {
        let addr = binding.connect_addr().to_string();
        let port = binding.host_port.to_string();
        vars.insert(format!("DON_PUBLIC_ADDR_{index}"), addr.clone());
        vars.insert(format!("DON_PUBLIC_PORT_{index}"), port.clone());
        if index == 0 {
            vars.insert("DON_PUBLIC_ADDR".to_string(), addr);
            vars.insert("DON_PUBLIC_PORT".to_string(), port);
        }
    }
    vars
}

/// Runtime-reference values for dependent service env fields. Alongside generic
/// and indexed aliases, a container-port-qualified alias is included only when
/// that container port is unambiguous.
///
/// Free function so it can be unit-tested without a live Docker client.
pub(crate) fn env_reference_values(bindings: &[DockerPortBinding]) -> HashMap<String, String> {
    let mut refs = HashMap::new();
    let mut container_port_counts: HashMap<u16, usize> = HashMap::new();
    for binding in bindings {
        let count = container_port_counts
            .entry(binding.container_port)
            .or_insert(0);
        *count = count.saturating_add(1);
    }

    for (index, binding) in bindings.iter().enumerate() {
        let addr = binding.connect_addr().to_string();
        let port = binding.host_port.to_string();
        refs.insert(format!("addr_{index}"), addr.clone());
        refs.insert(format!("ADDR_{index}"), addr.clone());
        refs.insert(format!("port_{index}"), port.clone());
        refs.insert(format!("PORT_{index}"), port.clone());
        if index == 0 {
            refs.insert("addr".to_string(), addr.clone());
            refs.insert("ADDR".to_string(), addr.clone());
            refs.insert("port".to_string(), port.clone());
            refs.insert("PORT".to_string(), port.clone());
        }
    }

    for binding in bindings {
        if container_port_counts.get(&binding.container_port) == Some(&1) {
            let addr_key = format!("ADDR_{}", binding.container_port);
            let port_key = format!("PORT_{}", binding.container_port);
            refs.entry(addr_key)
                .or_insert_with(|| binding.connect_addr().to_string());
            refs.entry(port_key)
                .or_insert_with(|| binding.host_port.to_string());
        }
    }
    refs
}

/// Handle to a running Docker container.
///
/// Provides stop/remove operations analogous to [`crate::sys::ProcessHandle`].
/// The container is identified by ID and name.
pub struct DockerHandle {
    client: Docker,
    container_id: String,
    container_name: String,
    port_bindings: Vec<DockerPortBinding>,
}

impl std::fmt::Debug for DockerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DockerHandle")
            .field("container_id", &self.container_id)
            .field("container_name", &self.container_name)
            .field("port_bindings", &self.port_bindings)
            .finish()
    }
}

impl DockerHandle {
    pub(crate) fn port_bindings(&self) -> &[DockerPortBinding] {
        &self.port_bindings
    }

    /// Stop the container with the given signal and timeout, then remove it.
    pub async fn stop(&mut self, signal: &str, timeout: Duration) -> Result<(), DockerError> {
        let timeout_secs = timeout.as_secs().max(1) as i32;
        let stop_options = StopContainerOptionsBuilder::new()
            .signal(signal)
            .t(timeout_secs)
            .build();
        // Stop error is intentionally ignored — container may already be stopped.
        // We proceed to force-remove regardless.
        let _ = self
            .client
            .stop_container(&self.container_id, Some(stop_options))
            .await;
        let remove_options = RemoveContainerOptionsBuilder::new().force(true).build();
        self.client
            .remove_container(&self.container_id, Some(remove_options))
            .await?;
        Ok(())
    }

    /// Force-remove the container (for cleanup).
    pub async fn remove(&self) -> Result<(), DockerError> {
        let options = RemoveContainerOptionsBuilder::new().force(true).build();
        self.client
            .remove_container(&self.container_id, Some(options))
            .await?;
        Ok(())
    }
}

/// Start a Docker service: clean up stale containers, create, start, stream logs.
///
/// Returns a `DockerHandle` for lifecycle management and a `ChildOutput` for
/// log streaming (compatible with the existing output system).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn start_docker_service(
    client: &Docker,
    name: &str,
    config: &DockerConfig,
    fallback_ports: bool,
    prior_bindings: &[DockerPortBinding],
    service_env: &HashMap<String, String>,
    env_files: &[std::path::PathBuf],
    base_dir: &std::path::Path,
    writer: Option<&crate::output::ServiceWriter>,
    secrets: &crate::secrets::SecretStore,
    secret_refs: &[String],
) -> Result<(DockerHandle, ChildOutput), DockerError> {
    let container_name = container_name(base_dir, name, config, fallback_ports);

    // Clean up any stale container with the same name.
    cleanup_stale_container(client, &container_name).await?;

    // The tag to run — and to tag a build as. Explicit `image`, else derived
    // from the service name.
    let image_tag = config.image_tag(name);

    // Build the image if a build config is present.
    if let Some(ref build_config) = config.build
        && let Some(w) = writer
    {
        build::build_image(client, build_config, &image_tag, base_dir, w).await?;
    }

    // Build container configuration.
    let mut prepared = parse::prepare_port_mappings(&config.ports, fallback_ports, prior_bindings)?;
    let env_vars = parse::build_container_env(service_env, env_files, &config.env_file)?;
    let mut env_map = std::collections::HashMap::new();
    for entry in &env_vars {
        if let Some((key, value)) = entry.split_once('=') {
            env_map.insert(key.to_string(), value.to_string());
        }
    }
    secrets.apply(&mut env_map, secret_refs);
    let env_vars: Vec<String> = env_map
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect();
    let mut retried_dynamic = false;

    let (container_id, actual_bindings) = loop {
        let container_config = build_container_config(config, &image_tag, &env_vars, &prepared);
        let create_options = CreateContainerOptionsBuilder::new()
            .name(&container_name)
            .build();
        let response = client
            .create_container(Some(create_options), container_config)
            .await?;
        let container_id = response.id;

        if let Err(source) = client.start_container(&container_id, None).await {
            let should_retry = fallback_ports
                && !retried_dynamic
                && is_port_allocation_error(&source)
                && !prepared.specs().is_empty();
            // Prefer dynamic-izing only the conflicting host port(s) so any
            // other mappings on this container keep their preferred ports;
            // fall back to reassigning them all if the message names none.
            let conflict_ports = if should_retry {
                parse::conflict_ports_in_message(server_error_message(&source))
            } else {
                std::collections::HashSet::new()
            };
            let failure = DockerError::Api(source);
            remove_after_failed_start(client, &container_id, "start", &failure).await?;
            if should_retry {
                if conflict_ports.is_empty() || !prepared.force_dynamic_conflicts(&conflict_ports) {
                    prepared.force_dynamic();
                }
                retried_dynamic = true;
                continue;
            }
            return Err(failure);
        }

        let inspection = match client.inspect_container(&container_id, None).await {
            Ok(inspection) => inspection,
            Err(source) => {
                let failure = DockerError::Api(source);
                remove_after_failed_start(client, &container_id, "inspect", &failure).await?;
                return Err(failure);
            }
        };
        let actual_ports = inspection
            .network_settings
            .and_then(|settings| settings.ports)
            .unwrap_or_default();
        let bindings = match parse::resolve_actual_port_bindings(prepared.specs(), &actual_ports) {
            Ok(bindings) => bindings,
            Err(failure) => {
                remove_after_failed_start(client, &container_id, "resolve runtime ports", &failure)
                    .await?;
                return Err(failure);
            }
        };
        break (container_id, bindings);
    };

    // Start log streaming.
    let log_options = LogsOptionsBuilder::new()
        .follow(true)
        .stdout(true)
        .stderr(true)
        .build();
    let log_stream = client.logs(&container_id, Some(log_options));
    let log_reader = DockerLogReader::new(Box::pin(log_stream));
    let child_output = ChildOutput::DockerLogs(log_reader);

    let handle = DockerHandle {
        client: client.clone(),
        container_id,
        container_name,
        port_bindings: actual_bindings,
    };

    Ok((handle, child_output))
}

fn build_container_config(
    config: &DockerConfig,
    image_tag: &str,
    env_vars: &[String],
    prepared: &parse::PreparedPortMappings,
) -> ContainerCreateBody {
    ContainerCreateBody {
        image: Some(image_tag.to_string()),
        env: Some(env_vars.to_vec()),
        exposed_ports: if prepared.exposed().is_empty() {
            None
        } else {
            Some(prepared.exposed().to_vec())
        },
        cmd: if config.command.is_empty() {
            None
        } else {
            Some(config.command.clone())
        },
        host_config: Some(HostConfig {
            port_bindings: if prepared.bindings().is_empty() {
                None
            } else {
                Some(prepared.bindings().clone())
            },
            binds: if config.volumes.is_empty() {
                None
            } else {
                Some(config.volumes.clone())
            },
            network_mode: config.network.clone(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn remove_after_failed_start(
    client: &Docker,
    container_id: &str,
    operation: &'static str,
    failure: &DockerError,
) -> Result<(), DockerError> {
    let options = RemoveContainerOptionsBuilder::new().force(true).build();
    client
        .remove_container(container_id, Some(options))
        .await
        .map_err(|cleanup| DockerError::CleanupAfterFailure {
            operation,
            failure: failure.to_string(),
            cleanup,
        })
}

/// The server-supplied message for a Docker API error, or `""` for other
/// error kinds. Used to mine the conflicting host port out of a failed start.
fn server_error_message(error: &bollard::errors::Error) -> &str {
    match error {
        bollard::errors::Error::DockerResponseServerError { message, .. } => message,
        _ => "",
    }
}

fn is_port_allocation_error(error: &bollard::errors::Error) -> bool {
    let bollard::errors::Error::DockerResponseServerError { message, .. } = error else {
        return false;
    };
    let message = message.to_ascii_lowercase();
    message.contains("port is already allocated")
        || message.contains("port is already in use")
        || message.contains("ports are not available")
        || message.contains("address already in use")
        || message.contains("failed to bind")
}

/// Determine the managed Docker container name for a service.
///
/// Fallback-port mode namespaces generated names by the canonical project
/// directory so concurrent worktrees cannot remove one another's containers.
/// Explicit names are always preserved exactly.
pub(crate) fn container_name(
    base_dir: &Path,
    service_name: &str,
    config: &DockerConfig,
    fallback_ports: bool,
) -> String {
    if let Some(name) = &config.container {
        return name.clone();
    }
    if !fallback_ports {
        return format!("don-{service_name}");
    }

    let hash = worktree_hash(base_dir);
    format!("don-{}-{hash}", sanitize_container_name_part(service_name))
}

fn worktree_hash(base_dir: &Path) -> String {
    use sha2::{Digest, Sha256};

    // 5 bytes (40 bits) is ample to disambiguate the handful of worktrees a
    // developer runs concurrently while keeping the container name short.
    let normalized = std::fs::canonicalize(base_dir).unwrap_or_else(|_| base_dir.to_path_buf());
    let mut hasher = Sha256::new();
    hasher.update(normalized.to_string_lossy().as_bytes());
    let digest = hasher.finalize();
    digest
        .get(..5)
        .map(hex::encode)
        .unwrap_or_else(|| hex::encode(digest))
}

fn sanitize_container_name_part(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.') {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    let trimmed = sanitized.trim_matches(['-', '.']);
    if trimmed.is_empty() {
        "service".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Clean up a stale container by name (from a previous don run that crashed).
///
/// Returns `Ok(true)` if a container was found and removed, `Ok(false)` if
/// no container by that name existed.
pub(crate) async fn cleanup_stale_container(
    client: &Docker,
    name: &str,
) -> Result<bool, DockerError> {
    match client.inspect_container(name, None).await {
        Ok(_) => {
            // Container exists — stop and remove it.
            let stop_options = StopContainerOptionsBuilder::new().t(5).build();
            let _ = client.stop_container(name, Some(stop_options)).await;
            let remove_options = RemoveContainerOptionsBuilder::new().force(true).build();
            client.remove_container(name, Some(remove_options)).await?;
            Ok(true)
        }
        Err(bollard::errors::Error::DockerResponseServerError {
            status_code: 404, ..
        }) => Ok(false),
        Err(e) => Err(DockerError::Api(e)),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn docker_config(container: Option<&str>) -> DockerConfig {
        DockerConfig {
            image: Some("alpine:latest".to_string()),
            container: container.map(str::to_string),
            ports: Vec::new(),
            volumes: Vec::new(),
            network: None,
            command: Vec::new(),
            env_file: Vec::new(),
            build: None,
        }
    }

    fn binding(
        configured: &str,
        configured_host_port: u16,
        host_ip: &str,
        host_port: u16,
        container_port: u16,
        protocol: &str,
    ) -> DockerPortBinding {
        DockerPortBinding {
            configured: configured.to_string(),
            configured_host_port,
            host_ip: host_ip.parse().unwrap(),
            host_port,
            container_port,
            protocol: protocol.to_string(),
        }
    }

    #[test]
    fn test_container_name_table() {
        struct Case {
            name: &'static str,
            base_dir: &'static str,
            service: &'static str,
            explicit: Option<&'static str>,
            fallback_ports: bool,
            expected: Option<&'static str>,
        }

        let cases = vec![
            Case {
                name: "legacy generated name without fallback",
                base_dir: "/tmp/worktree-a",
                service: "api",
                explicit: None,
                fallback_ports: false,
                expected: Some("don-api"),
            },
            Case {
                name: "explicit name without fallback",
                base_dir: "/tmp/worktree-a",
                service: "api",
                explicit: Some("my-api"),
                fallback_ports: false,
                expected: Some("my-api"),
            },
            Case {
                name: "explicit name with fallback",
                base_dir: "/tmp/worktree-a",
                service: "api",
                explicit: Some("my-api"),
                fallback_ports: true,
                expected: Some("my-api"),
            },
            Case {
                name: "generated fallback name",
                base_dir: "/tmp/worktree-a",
                service: "API odd/name",
                explicit: None,
                fallback_ports: true,
                expected: None,
            },
        ];

        for case in cases {
            let config = docker_config(case.explicit);
            let actual = container_name(
                Path::new(case.base_dir),
                case.service,
                &config,
                case.fallback_ports,
            );
            if let Some(expected) = case.expected {
                assert_eq!(actual, expected, "{}", case.name);
            } else {
                assert!(
                    actual.starts_with("don-api-odd-name-"),
                    "{}: {actual}",
                    case.name
                );
            }
        }

        let config = docker_config(None);
        let first = container_name(Path::new("/tmp/worktree-a"), "api", &config, true);
        let second = container_name(Path::new("/tmp/worktree-b"), "api", &config, true);
        assert_ne!(first, second);
    }

    #[test]
    fn test_runtime_binding_addresses_and_fallback() {
        struct Case {
            name: &'static str,
            binding: DockerPortBinding,
            expected_host: &'static str,
            expected_connect: &'static str,
            expected_fallback: bool,
        }

        let cases = vec![
            Case {
                name: "ipv4 wildcard connects over loopback",
                binding: binding("5432:5432", 5432, "0.0.0.0", 49152, 5432, "tcp"),
                expected_host: "0.0.0.0:49152",
                expected_connect: "127.0.0.1:49152",
                expected_fallback: true,
            },
            Case {
                name: "ipv6 wildcard connects over loopback",
                binding: binding("[::]:8080:80", 8080, "::", 8080, 80, "tcp"),
                expected_host: "[::]:8080",
                expected_connect: "[::1]:8080",
                expected_fallback: false,
            },
            Case {
                name: "explicit host remains connect target",
                binding: binding("127.0.0.1:5353:53/udp", 5353, "127.0.0.1", 5353, 53, "udp"),
                expected_host: "127.0.0.1:5353",
                expected_connect: "127.0.0.1:5353",
                expected_fallback: false,
            },
        ];

        for case in cases {
            assert_eq!(
                case.binding.host_addr().to_string(),
                case.expected_host,
                "{}",
                case.name
            );
            assert_eq!(
                case.binding.connect_addr().to_string(),
                case.expected_connect,
                "{}",
                case.name
            );
            assert_eq!(
                case.binding.used_fallback(),
                case.expected_fallback,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn test_handle_discovery_values() {
        // Operate on the free functions directly so this needs no live Docker
        // client (the daemon is absent on some CI runners, e.g. macOS).
        let bindings = vec![
            binding("0:5432", 0, "0.0.0.0", 49152, 5432, "tcp"),
            binding("127.0.0.1:8443:443", 8443, "127.0.0.1", 8443, 443, "tcp"),
        ];

        let public = public_env_vars(&bindings);
        assert_eq!(
            public.get("DON_PUBLIC_ADDR").map(String::as_str),
            Some("127.0.0.1:49152")
        );
        assert_eq!(
            public.get("DON_PUBLIC_PORT_1").map(String::as_str),
            Some("8443")
        );

        let refs = env_reference_values(&bindings);
        assert_eq!(refs.get("port").map(String::as_str), Some("49152"));
        assert_eq!(
            refs.get("addr_1").map(String::as_str),
            Some("127.0.0.1:8443")
        );
        assert_eq!(refs.get("PORT_5432").map(String::as_str), Some("49152"));
        assert_eq!(
            refs.get("ADDR_443").map(String::as_str),
            Some("127.0.0.1:8443")
        );
    }

    #[test]
    fn test_container_port_aliases_require_unambiguous_port() {
        let bindings = vec![
            binding("8053:53/tcp", 8053, "127.0.0.1", 8053, 53, "tcp"),
            binding("8053:53/udp", 8053, "127.0.0.1", 8053, 53, "udp"),
        ];

        let refs = env_reference_values(&bindings);
        assert!(!refs.contains_key("PORT_53"));
        assert!(!refs.contains_key("ADDR_53"));
        assert_eq!(refs.get("PORT_0").map(String::as_str), Some("8053"));
        assert_eq!(refs.get("PORT_1").map(String::as_str), Some("8053"));
    }

    #[test]
    fn test_port_allocation_error_detection_table() {
        struct Case {
            name: &'static str,
            message: &'static str,
            expected: bool,
        }

        let cases = vec![
            Case {
                name: "linux docker allocation error",
                message: "Bind for 0.0.0.0:5432 failed: port is already allocated",
                expected: true,
            },
            Case {
                name: "docker desktop allocation error",
                message: "Ports are not available: bind: address already in use",
                expected: true,
            },
            Case {
                name: "unrelated start failure",
                message: "failed to create task for container: invalid mount config",
                expected: false,
            },
        ];

        for case in cases {
            let error = bollard::errors::Error::DockerResponseServerError {
                status_code: 500,
                message: case.message.to_string(),
            };
            assert_eq!(
                is_port_allocation_error(&error),
                case.expected,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn test_docker_runtime_port_selection_table() {
        if std::env::var("DON_TEST_DOCKER").is_err() {
            eprintln!("skipping: DON_TEST_DOCKER not set");
            return;
        }

        struct Case {
            name: &'static str,
            container_name: &'static str,
            occupy_preferred: bool,
            expect_fallback: bool,
        }

        let cases = vec![
            Case {
                name: "available preferred port is preserved",
                container_name: "don-unit-docker-port-preferred",
                occupy_preferred: false,
                expect_fallback: false,
            },
            Case {
                name: "occupied preferred port uses docker allocation",
                container_name: "don-unit-docker-port-fallback",
                occupy_preferred: true,
                expect_fallback: true,
            },
        ];

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let client = Docker::connect_with_socket_defaults().unwrap();
            for case in cases {
                let preferred_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                let preferred_port = preferred_listener.local_addr().unwrap().port();
                let preferred_listener = if case.occupy_preferred {
                    Some(preferred_listener)
                } else {
                    drop(preferred_listener);
                    None
                };

                let mut config = docker_config(Some(case.container_name));
                config.image = Some("nginx:alpine".to_string());
                config.ports = vec![format!("127.0.0.1:{preferred_port}:80")];
                let _ = cleanup_stale_container(&client, case.container_name).await;

                let (mut handle, child_output) = start_docker_service(
                    &client,
                    "web",
                    &config,
                    true,
                    &[],
                    &HashMap::new(),
                    &[],
                    Path::new("/tmp"),
                    None,
                    &crate::secrets::SecretStore::empty(),
                    &[],
                )
                .await
                .unwrap_or_else(|e| panic!("{}: {e}", case.name));
                drop(child_output);

                let binding = handle
                    .port_bindings()
                    .first()
                    .unwrap_or_else(|| panic!("{}: missing runtime binding", case.name));
                assert_eq!(
                    binding.used_fallback(),
                    case.expect_fallback,
                    "{}",
                    case.name
                );
                if case.expect_fallback {
                    assert_ne!(binding.host_port, preferred_port, "{}", case.name);
                } else {
                    assert_eq!(binding.host_port, preferred_port, "{}", case.name);
                }

                let connect_addr = binding.connect_addr();
                let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
                loop {
                    if tokio::net::TcpStream::connect(connect_addr).await.is_ok() {
                        break;
                    }
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "{}: container never accepted connections on {connect_addr}",
                        case.name
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }

                handle
                    .stop("SIGTERM", Duration::from_secs(1))
                    .await
                    .unwrap_or_else(|e| panic!("{}: cleanup failed: {e}", case.name));
                drop(preferred_listener);
            }
        });
    }
}
