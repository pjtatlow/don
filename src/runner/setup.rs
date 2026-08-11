use super::baked_bazel_manifest::BakedBazelManifest;
use super::profile::resolve_profile_items_for_platform;
use super::{RunnerError, RuntimeService, RuntimeTask, ServiceState, TaskItemState};
use crate::config::{Config, Platform, ServiceKind};
use crate::output::OutputManager;
use crate::process::pid_file::{PidFile, PidFileError};
use crate::task_state::TaskState;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

pub(in crate::runner) fn canonicalize_base_dir(base_dir: &Path) -> Result<PathBuf, RunnerError> {
    std::fs::canonicalize(base_dir).map_err(RunnerError::Io)
}

pub(in crate::runner) fn ensure_don_dir(base_dir: &Path) -> Result<PathBuf, RunnerError> {
    let don_dir = base_dir.join(".don");
    std::fs::create_dir_all(&don_dir).map_err(RunnerError::Io)?;
    Ok(don_dir)
}

pub(in crate::runner) async fn acquire_don_pid_file(
    don_dir: &Path,
) -> Result<PidFile, RunnerError> {
    let don_pid_path = don_dir.join("don.pid");
    PidFile::acquire(don_pid_path.clone(), std::process::id() as i32)
        .await
        .map_err(|e| match e {
            PidFileError::AlreadyLocked => RunnerError::AlreadyRunning {
                path: don_pid_path.display().to_string(),
            },
            other => RunnerError::PidFile(other),
        })
}

pub(in crate::runner) async fn cleanup_stale_state(
    config: &Config,
    platform: Platform,
    base_dir: &Path,
    output_manager: &OutputManager,
) {
    let docker_names: Vec<String> = config
        .services
        .iter()
        .filter_map(|(name, svc)| {
            let resolved = svc.resolve(platform);
            if let Some(ServiceKind::Docker(d)) = &resolved.kind {
                Some(crate::ports::managed_docker_container_name(
                    base_dir,
                    name,
                    d,
                    config.fallback_ports,
                ))
            } else {
                None
            }
        })
        .collect();
    let cleanup_report = crate::process::cleanup::run_cleanup(base_dir, &docker_names).await;
    if cleanup_report.pid_files_removed > 0
        || cleanup_report.sock_removed
        || cleanup_report.containers_removed > 0
    {
        output_manager.lifecycle_event(&format!("cleaned stale state: {cleanup_report}"));
    }
    for warning in &cleanup_report.warnings {
        output_manager.error_event(warning);
    }
}

pub(in crate::runner) fn connect_docker_if_needed(
    config: &Config,
    platform: Platform,
) -> Result<Option<bollard::Docker>, RunnerError> {
    let has_docker = config
        .services
        .values()
        .any(|service| matches!(service.resolve(platform).kind, Some(ServiceKind::Docker(_))));
    if !has_docker {
        return Ok(None);
    }

    bollard::Docker::connect_with_socket_defaults()
        .map(Some)
        .map_err(|e| RunnerError::Config(format!("docker connection failed: {e}")))
}

pub(in crate::runner) fn resolve_active_items(
    config: &Config,
    platform: Platform,
    profile: Option<&str>,
) -> Result<Option<HashSet<String>>, RunnerError> {
    let Some(profile_name) = profile else {
        return Ok(None);
    };
    let prof = config
        .profiles
        .get(profile_name)
        .ok_or_else(|| RunnerError::Config(format!("unknown profile '{profile_name}'")))?;
    Ok(Some(resolve_profile_items_for_platform(
        config, prof, platform,
    )))
}

pub(in crate::runner) fn filter_active_services(
    config: &Config,
    active_items: Option<&HashSet<String>>,
) -> HashSet<String> {
    config
        .services
        .keys()
        .filter(|name| active_items.is_none_or(|s| s.contains(*name)))
        .cloned()
        .collect()
}

pub(in crate::runner) fn filter_active_tasks(
    config: &Config,
    active_items: Option<&HashSet<String>>,
) -> HashSet<String> {
    config
        .tasks
        .keys()
        .filter(|name| active_items.is_none_or(|s| s.contains(*name)))
        .cloned()
        .collect()
}

pub(in crate::runner) fn prune_download_cache(
    config: &Config,
    platform: Platform,
    don_dir: &Path,
    output_manager: &OutputManager,
) {
    let cache_base = don_dir.join("cache");
    let mut keep: HashSet<(String, String)> = HashSet::new();
    for (name, svc) in &config.services {
        let resolved = svc.resolve(platform);
        if let Some(ref dl) = resolved.download {
            for artifact in dl.platform.values() {
                keep.insert((name.clone(), artifact.composite_hash()));
            }
        }
    }
    for (name, task) in &config.tasks {
        if let Some(ref dl) = task.download {
            for artifact in dl.platform.values() {
                keep.insert((name.clone(), artifact.composite_hash()));
            }
        }
    }
    if let Ok(removed) = crate::download::prune_cache(&cache_base, &keep)
        && !removed.is_empty()
    {
        output_manager.lifecycle_event(&format!(
            "pruned {} stale cache entr{}",
            removed.len(),
            if removed.len() == 1 { "y" } else { "ies" }
        ));
    }
}

pub(in crate::runner) async fn build_runtime_maps(
    config: &Config,
    platform: Platform,
    base_dir: &Path,
    active_services: &HashSet<String>,
    active_tasks: &HashSet<String>,
    headless: bool,
) -> (
    HashMap<String, RuntimeService>,
    HashMap<String, RuntimeTask>,
) {
    let mut services = HashMap::new();
    for (name, svc) in &config.services {
        if active_services.contains(name) {
            let mut resolved = svc.resolve(platform);
            resolved.depends_on = config.effective_depends_on(name, &resolved.depends_on);
            services.insert(
                name.clone(),
                RuntimeService::new(resolved, ServiceState::Pending),
            );
        }
    }

    let task_state = TaskState::new(base_dir.join(".don").join("task-state"));
    let mut tasks = HashMap::new();
    for (name, task) in &config.tasks {
        if active_tasks.contains(name) {
            let mut task = task.clone();
            task.depends_on = config.effective_depends_on(name, &task.depends_on);
            if headless {
                task.apply_headless_override();
            }
            let has_success = task_state.has_success(name).await.unwrap_or(false);
            let last_run = task_state.last_run(name).await.unwrap_or(None);
            tasks.insert(
                name.clone(),
                RuntimeTask::new(task, TaskItemState::Pending, has_success, last_run),
            );
        }
    }
    (services, tasks)
}

/// Apply a matching baked manifest; reject it as a whole on any stale or missing output.
pub(in crate::runner) async fn apply_baked_bazel_launch_manifest(
    config: &Config,
    platform: Platform,
    base_dir: &Path,
    don_dir: &Path,
    services: &mut HashMap<String, RuntimeService>,
) -> Result<Option<usize>, String> {
    let Some(manifest) = BakedBazelManifest::load(don_dir)? else {
        return Ok(None);
    };
    let workspace = crate::build_tool::bazel::find_workspace_root(base_dir).ok_or_else(|| {
        format!(
            "could not find a Bazel workspace above {}",
            base_dir.display()
        )
    })?;
    let git_output = tokio::process::Command::new("git")
        .arg("-C")
        .arg(&workspace)
        .args(["rev-parse", "HEAD"])
        .output()
        .await
        .map_err(|error| format!("failed to read workspace commit: {error}"))?;
    if !git_output.status.success() {
        return Err(format!(
            "failed to read workspace commit: {}",
            String::from_utf8_lossy(&git_output.stderr).trim()
        ));
    }
    let current_commit = String::from_utf8_lossy(&git_output.stdout)
        .trim()
        .to_string();
    if !manifest.matches_commit(&current_commit) {
        return Err(format!(
            "manifest commit does not match workspace HEAD ({current_commit})"
        ));
    }

    let mut binaries = HashMap::new();
    for (name, runtime) in services.iter() {
        let Some(bazel) = runtime.resolved.bazel_config() else {
            continue;
        };
        if runtime.resolved.build_tool_watch_enabled() {
            return Err(format!(
                "{name} has Bazel source watching enabled and cannot use a baked manifest"
            ));
        }
        let executable = manifest.executable_for_target(&bazel.target, &workspace)?;
        binaries.insert(name.clone(), executable.to_string_lossy().into_owned());
    }

    for (name, binary) in &binaries {
        let Some(runtime) = services.get_mut(name) else {
            continue;
        };
        let Some(service) = config.services.get(name) else {
            continue;
        };
        let mut resolved = service.resolve_with_bazel_binary(platform, binary);
        resolved.depends_on = config.effective_depends_on(name, &resolved.depends_on);
        runtime.resolved = resolved;
        runtime.bazel_binary_path = Some(binary.clone());
        runtime.batch_built = true;
    }

    Ok(Some(binaries.len()))
}
