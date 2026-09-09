use super::service;
use crate::config::Platform;
use std::collections::HashMap;
use std::os::unix::io::RawFd;

#[derive(Clone, Copy)]
pub(crate) enum ServiceStartMode {
    Full,
    SpawnOnly,
}

#[derive(Clone)]
pub(crate) struct ServiceStartContext {
    pub(crate) resolved: crate::config::ResolvedService,
    pub(crate) batch_built: bool,
    pub(crate) listen_fds: Vec<RawFd>,
    pub(crate) listen_fds_env: HashMap<String, String>,
    pub(crate) fallback_ports: bool,
    pub(crate) prior_docker_port_bindings: Vec<crate::docker::DockerPortBinding>,
    pub(crate) secrets: crate::secrets::SecretStore,
}

pub(crate) async fn ensure_download_for_config_worker(
    base_dir: &std::path::Path,
    platform: Platform,
    name: &str,
    download: Option<&crate::config::DownloadConfig>,
    service_writer: Option<&crate::output::ServiceWriter>,
    emitter: &crate::output::LifecycleEmitter,
) -> Result<(), crate::download::DownloadError> {
    let download = match download {
        Some(dl) => dl,
        None => return Ok(()),
    };
    let artifact = match download.for_platform(platform) {
        Some(a) => a,
        None => return Ok(()),
    };
    let cache_base = base_dir.join(".don").join("cache");
    let bin_dir = base_dir.join(".don").join("bin");
    emitter.service_event(name, "ensuring artifact...");
    crate::download::ensure_artifact(artifact, &cache_base, name, service_writer).await?;
    if let Some(bin_name) = download.effective_bin_name(platform) {
        crate::download::link_binary(artifact, &cache_base, name, &bin_name, &bin_dir)?;
    }
    emitter.service_event(name, "artifact ready");
    Ok(())
}

async fn run_preset_build_worker(
    base_dir: &std::path::Path,
    emitter: &crate::output::LifecycleEmitter,
    name: &str,
    cmd: &str,
    args: &[String],
    resolved: &crate::config::ResolvedService,
    secrets: &crate::secrets::SecretStore,
) -> Result<(), String> {
    emitter.service_event(name, &format!("running {cmd} build..."));

    let work_dir = match resolved.dir.as_deref() {
        Some(d) => base_dir.join(d),
        None => base_dir.to_path_buf(),
    };
    let work_dir = work_dir.as_path();
    let mut env: HashMap<String, String> = std::env::vars().collect();
    env.extend(resolved.env.clone());
    secrets.apply(&mut env, &resolved.secrets);

    match crate::sys::spawn_process(crate::sys::SpawnConfig {
        cmd,
        args,
        dir: Some(work_dir),
        env,
        pgid_file_path: None,
        force_pipe: true,
        listen_fds: vec![],
    })
    .await
    {
        Ok((mut handle, child_output)) => {
            let build_name = name.to_string();
            let emitter_clone = emitter.clone();
            tokio::spawn(async move {
                let mut reader = tokio::io::BufReader::new(child_output);
                let mut line_buf = Vec::new();
                loop {
                    line_buf.clear();
                    match tokio::io::AsyncBufReadExt::read_until(&mut reader, b'\n', &mut line_buf)
                        .await
                    {
                        Ok(0) => break,
                        Ok(_) => {
                            if line_buf.last() == Some(&b'\n') {
                                line_buf.pop();
                            }
                            if line_buf.last() == Some(&b'\r') {
                                line_buf.pop();
                            }
                            let text = String::from_utf8_lossy(&line_buf);
                            emitter_clone.service_event(&build_name, &text);
                        }
                        Err(e) if e.raw_os_error() == Some(libc::EIO) => break,
                        Err(_) => break,
                    }
                }
            });

            match handle.wait().await {
                Ok(status) if status.success() => {
                    emitter.service_event(name, &format!("{cmd} build succeeded"));
                    Ok(())
                }
                Ok(status) => {
                    let code = status.code().unwrap_or(-1);
                    Err(format!("{cmd} build failed (exit code {code})"))
                }
                Err(e) => Err(format!("{cmd} build error: {e}")),
            }
        }
        Err(e) => Err(format!("failed to start {cmd} build: {e}")),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_service_build_worker(
    base_dir: &std::path::Path,
    docker_client: Option<&bollard::Docker>,
    emitter: &crate::output::LifecycleEmitter,
    name: &str,
    resolved: &crate::config::ResolvedService,
    batch_built: bool,
    service_writer: Option<&crate::output::ServiceWriter>,
    secrets: &crate::secrets::SecretStore,
) -> Result<(), String> {
    if batch_built {
        return Ok(());
    }

    match &resolved.kind {
        Some(crate::config::ServiceKind::Docker(docker_config)) => {
            if let Some(build_config) = &docker_config.build {
                emitter.service_event(name, "building docker image...");
                if let Some(client) = docker_client
                    && let Some(writer) = service_writer
                {
                    crate::docker::build::build_image(
                        client,
                        build_config,
                        &docker_config.image_tag(name),
                        base_dir,
                        writer,
                    )
                    .await
                    .map_err(|e| format!("docker build failed: {e}"))?;
                    emitter.service_event(name, "docker build succeeded");
                }
            }
            Ok(())
        }
        Some(crate::config::ServiceKind::Rust(rust_config)) => {
            let build_args = service::rust_build_args(rust_config);
            run_preset_build_worker(
                base_dir,
                emitter,
                name,
                "cargo",
                &build_args,
                resolved,
                secrets,
            )
            .await
        }
        Some(crate::config::ServiceKind::Go(go_config)) => {
            let output_path = service::go_binary_path(go_config, name, base_dir);
            if let Some(parent) = output_path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let build_args = service::go_build_args(go_config, &output_path);
            run_preset_build_worker(
                base_dir,
                emitter,
                name,
                "go",
                &build_args,
                resolved,
                secrets,
            )
            .await
        }
        Some(crate::config::ServiceKind::Custom { build, .. }) => {
            if let Some(build_cmd) = build {
                run_preset_build_worker(
                    base_dir,
                    emitter,
                    name,
                    &build_cmd.cmd,
                    &build_cmd.args,
                    resolved,
                    secrets,
                )
                .await
            } else {
                Ok(())
            }
        }
        _ => Ok(()),
    }
}

/// Why a start could not be prepared.
///
/// The distinction is the whole point: a build that failed will fail the same
/// way on the next attempt, because the sources have not changed. The restart
/// policy is for failures where waiting can plausibly change the answer.
pub(crate) struct StartFailure {
    pub(crate) message: String,
    /// The build tool refused. Never retried.
    pub(crate) from_build: bool,
}

impl StartFailure {
    fn other(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            from_build: false,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn start_service_worker(
    base_dir: &std::path::Path,
    pid_dir: &std::path::Path,
    platform: Platform,
    docker_client: Option<&bollard::Docker>,
    emitter: &crate::output::LifecycleEmitter,
    name: &str,
    context: &ServiceStartContext,
    mode: ServiceStartMode,
    service_writer: Option<&crate::output::ServiceWriter>,
) -> Result<service::StartResult, StartFailure> {
    if matches!(mode, ServiceStartMode::Full) {
        ensure_download_for_config_worker(
            base_dir,
            platform,
            name,
            context.resolved.download.as_ref(),
            service_writer,
            emitter,
        )
        .await
        .map_err(|e| StartFailure::other(format!("download failed: {e}")))?;

        run_service_build_worker(
            base_dir,
            docker_client,
            emitter,
            name,
            &context.resolved,
            context.batch_built,
            service_writer,
            &context.secrets,
        )
        .await
        .map_err(|message| StartFailure {
            message,
            from_build: true,
        })?;
    }

    service::start_service(
        name,
        &context.resolved,
        base_dir,
        pid_dir,
        &context.listen_fds,
        &context.listen_fds_env,
        docker_client,
        service_writer,
        platform,
        Some(emitter),
        context.fallback_ports,
        &context.prior_docker_port_bindings,
        &context.secrets,
    )
    .await
    .map_err(|e| StartFailure::other(e.to_string()))
}
