//! Configuration parsing, validation, and platform resolution for don.
//!
//! The config is loaded from a `don.toml` file and defines services, tasks,
//! and profiles for a dev environment.

pub(crate) mod dependency;
mod download;
mod group;
pub(crate) mod param;
mod platform;
pub(crate) mod ports;
pub(crate) mod profile;
mod secrets;
pub(crate) mod service;
pub(crate) mod task;
pub(crate) mod template;
pub(crate) mod types;

pub use self::dependency::Dependency;
pub use self::download::{DownloadConfig, PlatformDownload};
pub use self::group::ServiceGroup;
pub use self::param::{CompletionParse, Completions, ParamKind, ParamValidate, TaskParam};
pub use self::platform::Platform;
pub use self::profile::Profile;
pub use self::service::{
    DockerBuildConfig, DockerConfig, ResolvedService, RustConfig, Service, ServiceKind,
};
pub use self::task::{Task, TaskAutoRun, TaskHeadless};
pub use self::types::{
    BazelConfig, BazelDefaults, Command, LogConfig, LogFilterConfig, LogFilters, OnFailure,
    ProxyEntry, ProxyMode, ReadyCheck, ShutdownConfig,
};
pub use crate::secrets::{Group as SecretGroup, SecretsConfig};
pub use profile::resolve_profile_processes;

pub use self::service::{GoConfig, ServiceOverride};

use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};

/// Whether `name` is usable as a `--config=<name>` argument.
///
/// Bazel resolves the name against the `build:<name>` lines in the `.rc`
/// files and fails the build if it finds none, so a name that could never
/// match one is worth catching while we can still say which service it came
/// from. Anything past that — whether the config is actually defined — only
/// Bazel can answer, and it does.
fn is_usable_bazel_config(name: &str) -> bool {
    !name.trim().is_empty() && !name.starts_with('-') && !name.contains(char::is_whitespace)
}

fn default_true() -> bool {
    true
}

/// Top-level don configuration, typically loaded from `don.toml`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Config {
    /// Long-running services (databases, APIs, workers, etc.).
    #[serde(default)]
    pub services: HashMap<String, Service>,
    /// Named groups of services or other service groups that can be referenced
    /// from `depends_on` and `profiles.*.services`. A group may also declare
    /// its own `depends_on`, which is applied to every (transitive) member.
    #[serde(default)]
    pub service_groups: HashMap<String, ServiceGroup>,
    /// One-shot tasks (migrations, codegen, etc.).
    /// Only re-run when watched files change since last successful run.
    #[serde(default)]
    pub tasks: HashMap<String, Task>,
    /// Named profiles — subsets of services/tasks to run.
    #[serde(default)]
    pub profiles: HashMap<String, Profile>,
    /// Profile used by `don start` when no `--profile` flag is given.
    /// If unset, `don start` runs everything.
    #[serde(default)]
    pub default_profile: Option<String>,
    /// File glob patterns, relative to the workspace root, ignored by all
    /// file-watch and watch-derived change detection.
    #[serde(default)]
    pub watch_ignore: Vec<String>,
    /// Environment handed to every service and task. A process's own `env`
    /// wins on conflict, as does a platform override's.
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// If true, Don first tries each configured proxy listener or Docker host
    /// port, then falls back to an OS-assigned port on the same host/IP when
    /// the preferred port is already in use.
    #[serde(default)]
    pub fallback_ports: bool,
    /// Default shutdown behavior for services that do not define their own
    /// `[services.<name>.shutdown]` table.
    #[serde(default)]
    pub shutdown: ShutdownConfig,
    /// Global regex-based log filter. Service filters are added on top.
    #[serde(default)]
    pub log_filter: LogFilterConfig,
    /// Global regex-based log exclusion — lines to drop from every stream.
    /// Service exclusions are added on top, and an exclusion beats a keep.
    #[serde(default)]
    pub log_exclude: LogFilterConfig,
    /// Whether failed services/tasks should be added to the TUI log filter
    /// automatically. Individual services/tasks can override this with their
    /// own `auto_filter_on_failure` setting. Defaults to `true`.
    #[serde(default = "default_true")]
    pub auto_filter_on_failure: bool,
    /// Workspace-wide Bazel settings. Services and tasks that build with
    /// Bazel take their defaults from here.
    #[serde(default)]
    pub bazel: BazelDefaults,
    /// Same table as key.toml, nested here. Names and paths only; never values.
    #[serde(default)]
    /// `[[secrets]]` entries, in order. A later source wins a name collision.
    pub secrets: Vec<SecretsConfig>,
}

impl std::str::FromStr for Config {
    type Err = toml::de::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut value: toml::Value = toml::from_str(s)?;
        reject_removed_keys(&value).map_err(serde::de::Error::custom)?;
        apply_compat_keys(&mut value).map_err(serde::de::Error::custom)?;
        let mut config: Config = value.try_into()?;
        config.apply_global_env();
        Ok(config)
    }
}

/// Config keys that used to configure a feature don no longer has.
///
/// Deserialization ignores unknown keys, so without this an upgraded user
/// whose `don.toml` still says `turbo.task = "dev"` is told the service
/// "must have one of: bazel, docker, rust, go, or run" — true, and useless
/// about why a config that worked yesterday doesn't today.
const REMOVED_KEYS: &[(&str, &str)] = &[(
    "turbo",
    "Turborepo support has been removed. Replace it with a `run` command \
         (plus `build` if you need one), or use the bazel preset.",
)];

/// Fold spellings don has replaced into the ones it reads, in place.
///
/// `terminal = "foreground"` is the older way of saying `interactive = true`.
/// It named a mechanism don no longer has — a task taking the terminal away
/// from everything else — but the *intent* survived the mechanism, and a
/// config that says it is still saying something true: this task wants a human
/// at a keyboard. So it is honoured rather than rejected, and honoured by
/// rewriting it into today's key rather than by carrying two fields that mean
/// one thing.
///
/// `screen` had no successor and is ignored: there is no takeover to choose a
/// screen for.
fn apply_compat_keys(value: &mut toml::Value) -> Result<(), String> {
    let Some(tasks) = value.get_mut("tasks").and_then(toml::Value::as_table_mut) else {
        return Ok(());
    };
    for (name, task) in tasks.iter_mut() {
        let Some(table) = task.as_table_mut() else {
            continue;
        };
        let Some(terminal) = table.remove("terminal") else {
            continue;
        };
        let mode = match &terminal {
            toml::Value::String(mode) => Some(mode.as_str()),
            toml::Value::Table(table) => match table.get("mode") {
                Some(toml::Value::String(mode)) => Some(mode.as_str()),
                // A table with no mode is the default, which is muxed.
                None => Some("muxed"),
                Some(_) => None,
            },
            _ => None,
        };
        let interactive = match mode {
            Some("foreground") => true,
            Some("muxed") => false,
            _ => {
                return Err(format!(
                    "task '{name}': unknown terminal value {terminal}, expected \"muxed\" or \
                     \"foreground\". `terminal` is the older spelling of `interactive`; \
                     `foreground` means `interactive = true`"
                ));
            }
        };
        // An explicit `interactive` is the current key and the one the reader
        // most likely meant; a config carrying both is mid-migration.
        table.entry("interactive").or_insert(interactive.into());
    }
    Ok(())
}

/// Reject a config that still uses a removed key, by name and location.
fn reject_removed_keys(value: &toml::Value) -> Result<(), String> {
    for section in ["services", "tasks"] {
        let Some(items) = value.get(section).and_then(toml::Value::as_table) else {
            continue;
        };
        let kind = section.trim_end_matches('s');
        for (name, item) in items {
            for (key, guidance) in REMOVED_KEYS {
                if item.get(key).is_some() {
                    return Err(format!(
                        "{kind} '{name}': '{key}' is no longer supported. {guidance}"
                    ));
                }
            }
        }
    }
    Ok(())
}

const VALID_SIGNALS: &[&str] = &[
    "SIGTERM", "SIGINT", "SIGQUIT", "SIGHUP", "SIGUSR1", "SIGUSR2",
];

fn is_valid_signal(s: &str) -> bool {
    VALID_SIGNALS.contains(&s)
}

/// Check that a PlatformDownload's url and sha256 fields are well-formed.
fn validate_platform_download(artifact: &PlatformDownload) -> Result<(), String> {
    // Validate URL shape: must have http:// or https:// scheme.
    if !artifact.url.starts_with("http://") && !artifact.url.starts_with("https://") {
        return Err(format!(
            "url '{}' must start with http:// or https://",
            artifact.url
        ));
    }
    // Validate sha256 is 64 lowercase hex chars.
    if artifact.sha256.len() != 64 {
        return Err(format!(
            "sha256 must be 64 hex characters, got {} characters",
            artifact.sha256.len()
        ));
    }
    if !artifact.sha256.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(format!(
            "sha256 '{}' must contain only hex characters (0-9, a-f, A-F)",
            artifact.sha256
        ));
    }
    Ok(())
}

fn validate_log_filter(label: &str, filter: &LogFilterConfig, errors: &mut Vec<String>) {
    for pattern in &filter.patterns {
        if let Err(e) = regex::bytes::Regex::new(pattern) {
            errors.push(format!("{label}: invalid regex '{pattern}': {e}"));
        }
    }
}

impl Config {
    /// Load and parse a config from a file path, applying the active profile's
    /// `overrides` block and then a sibling `.local.toml` override file if one
    /// exists.
    pub fn from_file(path: &std::path::Path) -> Result<Self, ConfigError> {
        Self::from_file_with_profile(path, None)
    }

    /// Load a config knowing which profile the caller is about to start.
    ///
    /// `profile` is the `--profile` name; `None` falls back to
    /// `default_profile`. The named profile's `overrides` block lands between
    /// the base file and the local file, so a repo-shipped profile can retarget
    /// a service while a developer's own `.local.toml` still has the last word.
    pub fn from_file_with_profile(
        path: &std::path::Path,
        profile: Option<&str>,
    ) -> Result<Self, ConfigError> {
        let mut config: Self = Self::merged_toml(path, profile)?.try_into()?;
        config.apply_global_env();
        Ok(config)
    }

    /// Push the workspace-wide `env` down into every service and task, where a
    /// process's own entry wins. Done once at load so every consumer —
    /// spawning, ready checks, `don status -v` — sees one env per process.
    fn apply_global_env(&mut self) {
        if self.env.is_empty() {
            return;
        }
        let envs = self
            .services
            .values_mut()
            .map(|service| &mut service.env)
            .chain(self.tasks.values_mut().map(|task| &mut task.env));
        for env in envs {
            for (key, value) in &self.env {
                env.entry(key.clone()).or_insert_with(|| value.clone());
            }
        }
    }

    /// The config as don sees it once every layer is applied: the base file,
    /// the active profile's `overrides`, and the sibling `.local.toml`. This is
    /// the value `from_file_with_profile` deserializes, and what
    /// `don validate --show` prints.
    pub fn merged_toml(
        path: &std::path::Path,
        profile: Option<&str>,
    ) -> Result<toml::Value, ConfigError> {
        let content = std::fs::read_to_string(path).map_err(|source| ConfigError::ReadFile {
            path: path.to_path_buf(),
            source,
        })?;
        let mut value: toml::Value = toml::from_str(&content)?;
        let local = Self::read_local_override(path)?;

        // Which profile is active, and what it overrides, are both things the
        // local file may have moved — so resolve them against the merged view
        // rather than the base file alone.
        let mut merged = value.clone();
        if let Some(local) = &local {
            merge_toml_values(&mut merged, local.clone());
        }
        validate_profile_keys(&merged).map_err(|error| ConfigError::Validation {
            errors: vec![error],
        })?;
        let active = profile.map(str::to_string).or_else(|| {
            merged
                .get("default_profile")
                .and_then(toml::Value::as_str)
                .map(str::to_string)
        });
        if let Some(name) = &active
            && let Some(overrides) = merged
                .get("profiles")
                .and_then(|profiles| profiles.get(name))
                .and_then(|profile| profile.get(PROFILE_OVERRIDES_KEY))
                .cloned()
        {
            validate_profile_overrides(name, &overrides).map_err(|error| {
                ConfigError::Validation {
                    errors: vec![error],
                }
            })?;
            merge_toml_values(&mut value, overrides);
        }
        if let Some(local) = local {
            merge_toml_values(&mut value, local);
        }

        reject_removed_keys(&value).map_err(|error| ConfigError::Validation {
            errors: vec![error],
        })?;
        apply_compat_keys(&mut value).map_err(|error| ConfigError::Validation {
            errors: vec![error],
        })?;
        // The applied block is folded in now, so leaving it would read as
        // pending. Other profiles keep theirs — nothing of theirs was applied.
        if let Some(name) = &active
            && let Some(profile) = value
                .get_mut("profiles")
                .and_then(|profiles| profiles.get_mut(name.as_str()))
                .and_then(toml::Value::as_table_mut)
        {
            profile.remove(PROFILE_OVERRIDES_KEY);
        }
        Ok(value)
    }

    fn read_local_override(path: &Path) -> Result<Option<toml::Value>, ConfigError> {
        let local_path = Self::local_override_path(path);
        match std::fs::read_to_string(&local_path) {
            Ok(content) => Ok(Some(toml::from_str(&content)?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(ConfigError::ReadFile {
                path: local_path,
                source,
            }),
        }
    }

    fn local_override_path(path: &Path) -> PathBuf {
        let parent = path.parent().unwrap_or_else(|| Path::new(""));
        let mut file_name = path
            .file_stem()
            .filter(|stem| !stem.is_empty())
            .map(OsString::from)
            .unwrap_or_else(|| OsString::from("don"));
        file_name.push(".local");
        match path.extension() {
            Some(ext) => {
                file_name.push(".");
                file_name.push(ext);
            }
            None => file_name.push(".toml"),
        }
        parent.join(file_name)
    }

    /// All known names (services + tasks) for dependency validation.
    fn all_names(&self) -> HashSet<&str> {
        self.services
            .keys()
            .chain(self.tasks.keys())
            .map(|s| s.as_str())
            .collect()
    }

    fn all_service_names(&self) -> HashSet<&str> {
        self.services.keys().map(|s| s.as_str()).collect()
    }

    fn dependency_reference_names(&self) -> HashSet<&str> {
        let mut names = self.all_names();
        names.extend(self.service_groups.keys().map(|s| s.as_str()));
        names
    }

    fn profile_service_reference_names(&self) -> HashSet<&str> {
        let mut names = self.all_service_names();
        names.extend(self.service_groups.keys().map(|s| s.as_str()));
        names
    }

    /// Expand `depends_on` entries: every service-group reference becomes its
    /// transitive members, each inheriting the blocking-ness declared on the
    /// group reference. Duplicates collapse to the strictest edge.
    pub(crate) fn expand_dependency_refs(&self, refs: &[Dependency]) -> Vec<Dependency> {
        let mut expanded = Vec::new();
        let mut group_stack = Vec::new();

        for dep in refs {
            self.expand_dependency_ref(dep, &mut expanded, &mut group_stack);
        }

        expanded
    }

    fn expand_dependency_ref(
        &self,
        dep: &Dependency,
        expanded: &mut Vec<Dependency>,
        group_stack: &mut Vec<String>,
    ) {
        if self.service_groups.contains_key(&dep.name) {
            self.expand_service_group(dep, expanded, group_stack);
        } else {
            dependency::push_dependency(expanded, dep.clone());
        }
    }

    fn expand_service_group(
        &self,
        dep: &Dependency,
        expanded: &mut Vec<Dependency>,
        group_stack: &mut Vec<String>,
    ) {
        if group_stack.contains(&dep.name) {
            return;
        }

        group_stack.push(dep.name.clone());
        if let Some(group) = self.service_groups.get(&dep.name) {
            for member in &group.members {
                self.expand_dependency_ref(&dep.with_name(member), expanded, group_stack);
            }
        }
        group_stack.pop();
    }

    /// Expand plain service/group name references (profiles) to leaf service
    /// and task names.
    pub(crate) fn expand_service_refs(&self, refs: &[String]) -> Vec<String> {
        let deps: Vec<Dependency> = refs.iter().cloned().map(Dependency::from).collect();
        dependency::dependency_names(&self.expand_dependency_refs(&deps))
    }

    pub(crate) fn expand_profile_services(&self, refs: &[String]) -> Vec<String> {
        self.expand_service_refs(refs)
    }

    /// Effective `depends_on` for a service or task — its own declared deps
    /// plus the `depends_on` from every group whose transitive member set
    /// contains `name`. The result is fully expanded (group refs resolved to
    /// leaf names) and deduplicated.
    pub(crate) fn effective_depends_on(
        &self,
        name: &str,
        own_deps: &[Dependency],
    ) -> Vec<Dependency> {
        let mut all: Vec<Dependency> = own_deps.to_vec();
        for (group_name, group) in &self.service_groups {
            if group.depends_on.is_empty() {
                continue;
            }
            let members = self.expand_service_refs(std::slice::from_ref(group_name));
            if members.iter().any(|m| m == name) {
                all.extend(group.depends_on.iter().cloned());
            }
        }
        self.expand_dependency_refs(&all)
    }

    pub(crate) fn effective_secrets(&self, name: &str, own: Option<&[String]>) -> Vec<String> {
        if let Some(refs) = own {
            return refs.to_vec();
        }
        let mut all = Vec::new();
        for (group_name, group) in &self.service_groups {
            if group.secrets.is_empty() {
                continue;
            }
            let members = self.expand_service_refs(std::slice::from_ref(group_name));
            if members.iter().any(|m| m == name) {
                all.extend(group.secrets.iter().cloned());
            }
        }
        let mut seen = HashSet::new();
        all.retain(|item| seen.insert(item.clone()));
        all
    }

    /// Validate the entire config for a given platform.
    ///
    /// Checks preset validity, ready check configuration, dependency references,
    /// profile references, and dependency cycles.
    pub fn validate(&self, platform: Platform) -> Result<Vec<String>, ConfigError> {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        let service_group_reference_names = self.profile_service_reference_names();
        let dependency_reference_names = self.dependency_reference_names();
        let profile_service_reference_names = self.profile_service_reference_names();

        for pattern in &self.watch_ignore {
            if let Err(e) = glob::Pattern::new(pattern) {
                errors.push(format!(
                    "invalid global watch_ignore pattern '{pattern}': {e}"
                ));
            }
        }
        // Bazel rejects `--config=` with an undefined or empty name outright,
        // and it does it after the build has been asked for — so catch it here
        // rather than letting every bazel service fail at startup.
        if let Some(name) = &self.bazel.config
            && !is_usable_bazel_config(name)
        {
            errors.push(format!(
                "global bazel: invalid config name '{name}' — \
                 name a configuration defined in .bazelrc, like 'build:{}'",
                name.trim()
            ));
        }
        if let Err(e) = crate::duration::parse_duration(&self.shutdown.timeout) {
            errors.push(format!("global shutdown: invalid timeout: {e}"));
        }
        if !is_valid_signal(&self.shutdown.signal) {
            errors.push(format!(
                "global shutdown: unknown signal '{}' (expected SIGTERM, SIGINT, SIGQUIT, SIGHUP, SIGUSR1, or SIGUSR2)",
                self.shutdown.signal
            ));
        }
        validate_log_filter("global log_filter", &self.log_filter, &mut errors);
        validate_log_filter("global log_exclude", &self.log_exclude, &mut errors);

        secrets::validate_secrets(
            &self.secrets,
            &self.services,
            &self.tasks,
            &self.service_groups,
            suggest_typo,
            &mut errors,
        );

        // Check for name collisions between services and tasks
        for name in self.services.keys() {
            if self.tasks.contains_key(name) {
                errors.push(format!("'{name}' is defined as both a service and a task"));
            }
        }

        for name in self.service_groups.keys() {
            if self.services.contains_key(name) {
                errors.push(format!(
                    "service group '{name}' conflicts with service '{name}'"
                ));
            }
            if self.tasks.contains_key(name) {
                errors.push(format!(
                    "service group '{name}' conflicts with task '{name}'"
                ));
            }
        }

        for (name, group) in &self.service_groups {
            for member in &group.members {
                if !service_group_reference_names.contains(member.as_str()) {
                    let suggestion = suggest_typo(member, &service_group_reference_names);
                    errors.push(format!(
                        "service group '{name}': references unknown service or service group '{member}'{suggestion}"
                    ));
                }
            }
            for dep in &group.depends_on {
                if !dependency_reference_names.contains(dep.name.as_str()) {
                    let suggestion = suggest_typo(&dep.name, &dependency_reference_names);
                    errors.push(format!(
                        "service group '{name}': depends on unknown service, task, or service group '{}'{suggestion}",
                        dep.name
                    ));
                }
            }
        }

        // Validate services
        for (name, svc) in &self.services {
            let resolved = svc.resolve(platform);
            // ServiceKind must be set (either on the base or via a platform override).
            if resolved.kind.is_none() {
                errors.push(format!(
                    "service '{name}': must have one of: bazel, docker, rust, go, or run"
                ));
            }
            if let Some(ref ready) = resolved.ready {
                let check_count = ready.exec.is_some() as u8
                    + ready.tcp.is_some() as u8
                    + ready.http.is_some() as u8;
                if check_count == 0 {
                    errors.push(format!(
                        "service '{name}': ready check must have one of: exec, tcp, or http"
                    ));
                } else if check_count > 1 {
                    errors.push(format!(
                        "service '{name}': ready check must have only one of: exec, tcp, or http"
                    ));
                }
            }
            // Warn if a service with a proxy entry uses a TCP ready check
            // against the proxy's listen address — the TCP connect will
            // succeed immediately against don's socket without proving the
            // service is actually accepting connections.
            if let Some(ref ready) = resolved.ready
                && let Some(ref tcp_addr) = ready.tcp
                && resolved.proxy.iter().any(|p| p.listen == *tcp_addr)
            {
                warnings.push(format!(
                    "service '{name}': TCP ready check on '{tcp_addr}' will pass \
                     immediately because don holds that socket — use an HTTP or \
                     exec ready check instead"
                ));
            }
            for dep in &resolved.depends_on {
                if !dependency_reference_names.contains(dep.name.as_str()) {
                    let suggestion = suggest_typo(&dep.name, &dependency_reference_names);
                    errors.push(format!(
                        "service '{name}': depends on unknown service, task, or service group '{}'{suggestion}",
                        dep.name
                    ));
                }
            }
            // Validate duration strings
            for pattern in &resolved.watch {
                if let Err(e) = glob::Pattern::new(pattern) {
                    errors.push(format!(
                        "service '{name}': invalid watch pattern '{pattern}': {e}"
                    ));
                }
            }
            for pattern in &resolved.ignore {
                if let Err(e) = glob::Pattern::new(pattern) {
                    errors.push(format!(
                        "service '{name}': invalid ignore pattern '{pattern}': {e}"
                    ));
                }
            }
            if let Some(ref debounce) = resolved.debounce
                && let Err(e) = crate::duration::parse_duration(debounce)
            {
                errors.push(format!("service '{name}': invalid debounce: {e}"));
            }
            if let Some(ref ready) = resolved.ready
                && let Err(e) = crate::duration::parse_duration(&ready.interval)
            {
                errors.push(format!("service '{name}': invalid ready interval: {e}"));
            }
            if let Some(ref ready) = resolved.ready {
                if let Err(e) = crate::duration::parse_duration(&ready.timeout) {
                    errors.push(format!("service '{name}': invalid ready timeout: {e}"));
                }
                if let Err(e) = crate::duration::parse_duration(&ready.monitor_interval) {
                    errors.push(format!(
                        "service '{name}': invalid ready monitor_interval: {e}"
                    ));
                }
                if !ready.monitor && ready.monitor_interval != "10s" {
                    warnings.push(format!(
                        "service '{name}': ready.monitor_interval set but monitor = false — it will be ignored"
                    ));
                }
                if ready.monitor && ready.unhealthy_after == 0 {
                    errors.push(format!(
                        "service '{name}': ready.unhealthy_after must be >= 1"
                    ));
                }
            }
            if let Some(ref shutdown) = resolved.shutdown {
                if let Err(e) = crate::duration::parse_duration(&shutdown.timeout) {
                    errors.push(format!("service '{name}': invalid shutdown timeout: {e}"));
                }
                if !is_valid_signal(&shutdown.signal) {
                    errors.push(format!(
                        "service '{name}': unknown shutdown signal '{}' (expected SIGTERM, SIGINT, SIGQUIT, SIGHUP, SIGUSR1, or SIGUSR2)",
                        shutdown.signal
                    ));
                }
            }
            validate_log_filter(
                &format!("service '{name}' log_filter"),
                &resolved.log_filter,
                &mut errors,
            );
            validate_log_filter(
                &format!("service '{name}' log_exclude"),
                &resolved.log_exclude,
                &mut errors,
            );
            if let Some(ServiceKind::Docker(docker)) = &resolved.kind {
                // A docker service must have something to run: an explicit
                // image, or a build config (whose image is tagged
                // `don-<service>`).
                if docker.image.is_none() && docker.build.is_none() {
                    errors.push(format!(
                        "service '{name}': docker service must set `docker.image` or `docker.build`"
                    ));
                }
                if let Err(error) = ports::parse_port_specs(&docker.ports) {
                    errors.push(format!("service '{name}': {error}"));
                }
                if self.fallback_ports && docker.container.is_some() {
                    warnings.push(format!(
                        "service '{name}': explicit docker.container names can collide across \
                         worktrees even when fallback_ports is enabled — omit docker.container \
                         to use Don's generated worktree-specific name"
                    ));
                }
            }

            // Validate download config.
            if let Some(ref download) = resolved.download {
                // Downloads only apply to custom services (with run.cmd).
                // Rust/Go/Docker presets have their own binary resolution.
                match &resolved.kind {
                    Some(ServiceKind::Rust(_))
                    | Some(ServiceKind::Go(_))
                    | Some(ServiceKind::Docker(_)) => {
                        errors.push(format!(
                            "service '{name}': download is not supported with rust/go/docker presets"
                        ));
                    }
                    Some(ServiceKind::Custom { .. }) => {}
                    _ => {
                        errors.push(format!("service '{name}': download requires a run command"));
                    }
                }
                for (platform_key, artifact) in &download.platform {
                    if let Err(msg) = validate_platform_download(artifact) {
                        errors.push(format!(
                            "service '{name}': download.platform.{platform_key}: {msg}"
                        ));
                    }
                }
                // Warn if the current platform has no download entry — the
                // service will silently fall back to a PATH lookup of run.cmd.
                if download.for_platform(platform).is_none()
                    && matches!(resolved.kind, Some(ServiceKind::Custom { .. }))
                {
                    let available: Vec<String> =
                        download.platform.keys().map(|p| p.to_string()).collect();
                    warnings.push(format!(
                        "service '{name}': no download entry for current platform {platform} \
                         (available: {}) — will use run.cmd from PATH",
                        available.join(", ")
                    ));
                }
            }
        }

        // Validate proxy and lazy across all services.
        let mut proxy_addrs: HashMap<&str, &str> = HashMap::new(); // addr -> service name
        for (name, svc) in &self.services {
            let resolved = svc.resolve(platform);
            if let Some(bazel) = resolved.bazel_config()
                && let Some(config) = &bazel.config
                && !is_usable_bazel_config(config)
            {
                errors.push(format!(
                    "service '{name}': invalid bazel config name '{config}' — \
                     name a configuration defined in .bazelrc, like 'build:{}'",
                    config.trim()
                ));
            }
            // lazy needs at least one proxy entry to trigger on.
            if resolved.lazy && resolved.proxy.is_empty() {
                errors.push(format!(
                    "service '{name}': 'lazy = true' requires at least one 'proxy' entry"
                ));
            }
            // Validate each proxy listen address parses as host:port.
            // The env/listenfd mutual-exclusion is enforced at deserialize
            // time — by the time we get here, the mode is a single enum.
            for entry in &resolved.proxy {
                if entry.listen.parse::<std::net::SocketAddr>().is_err() {
                    errors.push(format!(
                        "service '{name}': invalid proxy listen address '{}' \
                         — expected host:port (e.g. \"127.0.0.1:3000\")",
                        entry.listen
                    ));
                }
            }
        }
        // Check for duplicate proxy listen addresses across services.
        // Uses resolved configs so platform overrides are accounted for.
        let resolved_proxies: Vec<(String, Vec<String>)> = self
            .services
            .iter()
            .map(|(name, svc)| {
                let resolved = svc.resolve(platform);
                let addrs: Vec<String> = resolved
                    .proxy
                    .iter()
                    .filter_map(|entry| {
                        let addr = entry.listen.parse::<std::net::SocketAddr>().ok()?;
                        (addr.port() != 0).then(|| entry.listen.clone())
                    })
                    .collect();
                (name.clone(), addrs)
            })
            .collect();
        for (name, addrs) in &resolved_proxies {
            for addr in addrs {
                if let Some(other) = proxy_addrs.get(addr.as_str()) {
                    errors.push(format!(
                        "service '{name}': proxy listen address '{addr}' is already used by service '{other}'"
                    ));
                } else {
                    proxy_addrs.insert(addr, name);
                }
            }
        }

        // Validate tasks
        for (name, task) in &self.tasks {
            if let Some(bazel) = &task.bazel
                && let Some(config) = &bazel.config
                && !is_usable_bazel_config(config)
            {
                errors.push(format!(
                    "task '{name}': invalid bazel config name '{config}' — \
                     name a configuration defined in .bazelrc, like 'build:{}'",
                    config.trim()
                ));
            }
            for dep in &task.depends_on {
                if !dependency_reference_names.contains(dep.name.as_str()) {
                    let suggestion = suggest_typo(&dep.name, &dependency_reference_names);
                    errors.push(format!(
                        "task '{name}': depends on unknown service, task, or service group '{}'{suggestion}",
                        dep.name
                    ));
                }
            }
            for pattern in &task.watch {
                if let Err(e) = glob::Pattern::new(pattern) {
                    errors.push(format!(
                        "task '{name}': invalid watch pattern '{pattern}': {e}"
                    ));
                }
            }
            for pattern in &task.ignore {
                if let Err(e) = glob::Pattern::new(pattern) {
                    errors.push(format!(
                        "task '{name}': invalid ignore pattern '{pattern}': {e}"
                    ));
                }
            }
            if let Some(ref debounce) = task.debounce
                && let Err(e) = crate::duration::parse_duration(debounce)
            {
                errors.push(format!("task '{name}': invalid debounce: {e}"));
            }
            if let Some(ref timeout) = task.timeout
                && let Err(e) = crate::duration::parse_duration(timeout)
            {
                errors.push(format!("task '{name}': invalid timeout: {e}"));
            }
            // Something has to run. `bazel.target` supplies the executable
            // once the build has resolved it, so it stands in for `cmd`.
            if task.cmd.is_none() && task.bazel.is_none() {
                errors.push(format!(
                    "task '{name}': must set `cmd` or `bazel.target` — there is nothing to run"
                ));
            }
            // Validate params and any placeholders that reference them.
            validate_task_params(name, task, &mut errors);
            // Validate download config.
            if let Some(ref download) = task.download {
                for (platform_key, artifact) in &download.platform {
                    if let Err(msg) = validate_platform_download(artifact) {
                        errors.push(format!(
                            "task '{name}': download.platform.{platform_key}: {msg}"
                        ));
                    }
                }
                // Warn if the current platform has no download entry.
                if download.for_platform(platform).is_none() {
                    let available: Vec<String> =
                        download.platform.keys().map(|p| p.to_string()).collect();
                    warnings.push(format!(
                        "task '{name}': no download entry for current platform {platform} \
                         (available: {}) — will use cmd from PATH",
                        available.join(", ")
                    ));
                }
            }
        }

        // Validate profiles
        for (name, profile) in &self.profiles {
            for svc in &profile.services {
                if !profile_service_reference_names.contains(svc.as_str()) {
                    let suggestion = suggest_typo(svc, &profile_service_reference_names);
                    errors.push(format!(
                        "profile '{name}': references unknown service or service group '{svc}'{suggestion}"
                    ));
                }
            }
            for task in &profile.tasks {
                if !self.tasks.contains_key(task) {
                    errors.push(format!(
                        "profile '{name}': references unknown task '{task}'"
                    ));
                }
            }
        }

        // Validate default_profile
        if let Some(ref default) = self.default_profile
            && !self.profiles.contains_key(default)
        {
            errors.push(format!(
                "default_profile references unknown profile '{default}'"
            ));
        }

        // Detect dependency cycles
        if let Some(cycle) = self.detect_service_group_cycle() {
            errors.push(format!("service group cycle: {}", cycle.join(" -> ")));
        }
        if let Some(cycle) = self.detect_cycle(platform) {
            errors.push(format!("dependency cycle: {}", cycle.join(" -> ")));
        }

        // Detect bin_name collisions across all downloads — the symlinks at
        // .don/bin/<name> must be unique.
        let mut bin_names: HashMap<String, Vec<String>> = HashMap::new();
        for (name, svc) in &self.services {
            let resolved = svc.resolve(platform);
            if let Some(ref dl) = resolved.download
                && let Some(bin_name) = dl.effective_bin_name(platform)
            {
                bin_names
                    .entry(bin_name)
                    .or_default()
                    .push(format!("service '{name}'"));
            }
        }
        for (name, task) in &self.tasks {
            if let Some(ref dl) = task.download
                && let Some(bin_name) = dl.effective_bin_name(platform)
            {
                bin_names
                    .entry(bin_name)
                    .or_default()
                    .push(format!("task '{name}'"));
            }
        }
        for (bin_name, owners) in &bin_names {
            if owners.len() > 1 {
                errors.push(format!(
                    "download bin_name '{bin_name}' is used by multiple owners ({}) \
                     — add an explicit bin_name to disambiguate",
                    owners.join(", ")
                ));
            }
        }

        if errors.is_empty() {
            Ok(warnings)
        } else {
            Err(ConfigError::Validation { errors })
        }
    }

    /// Detect cycles among service groups. Group cycles are invalid even if
    /// no service currently depends on the group, because expansion would be
    /// ambiguous and could otherwise recurse forever.
    fn detect_service_group_cycle(&self) -> Option<Vec<String>> {
        #[derive(Clone, Copy, PartialEq)]
        enum State {
            Unvisited,
            Visiting,
            Visited,
        }

        let mut state: HashMap<String, State> = self
            .service_groups
            .keys()
            .map(|k| (k.clone(), State::Unvisited))
            .collect();
        let mut path: Vec<String> = Vec::new();

        fn dfs(
            node: &str,
            groups: &HashMap<String, ServiceGroup>,
            state: &mut HashMap<String, State>,
            path: &mut Vec<String>,
        ) -> Option<Vec<String>> {
            state.insert(node.to_string(), State::Visiting);
            path.push(node.to_string());

            if let Some(group) = groups.get(node) {
                for member in &group.members {
                    if !groups.contains_key(member) {
                        continue;
                    }
                    match state.get(member.as_str()) {
                        Some(State::Visiting) => {
                            let cycle_start = path.iter().position(|n| n == member)?;
                            let mut cycle: Vec<String> = path[cycle_start..].to_vec();
                            cycle.push(member.clone());
                            return Some(cycle);
                        }
                        Some(State::Unvisited) | None => {
                            if let Some(cycle) = dfs(member, groups, state, path) {
                                return Some(cycle);
                            }
                        }
                        Some(State::Visited) => {}
                    }
                }
            }

            path.pop();
            state.insert(node.to_string(), State::Visited);
            None
        }

        let all_groups: Vec<String> = self.service_groups.keys().cloned().collect();
        for group in &all_groups {
            if state.get(group) == Some(&State::Unvisited)
                && let Some(cycle) = dfs(group, &self.service_groups, &mut state, &mut path)
            {
                return Some(cycle);
            }
        }

        None
    }

    /// Detect dependency cycles using DFS. Returns the cycle path if one exists.
    fn detect_cycle(&self, platform: Platform) -> Option<Vec<String>> {
        let mut deps: HashMap<String, Vec<String>> = HashMap::new();
        for (name, svc) in &self.services {
            let resolved = svc.resolve(platform);
            deps.insert(
                name.clone(),
                dependency::dependency_names(
                    &self.effective_depends_on(name, &resolved.depends_on),
                ),
            );
        }
        for (name, task) in &self.tasks {
            deps.insert(
                name.clone(),
                dependency::dependency_names(&self.effective_depends_on(name, &task.depends_on)),
            );
        }

        #[derive(Clone, Copy, PartialEq)]
        enum State {
            Unvisited,
            Visiting,
            Visited,
        }

        let mut state: HashMap<String, State> =
            deps.keys().map(|k| (k.clone(), State::Unvisited)).collect();
        let mut path: Vec<String> = Vec::new();

        fn dfs(
            node: &str,
            deps: &HashMap<String, Vec<String>>,
            state: &mut HashMap<String, State>,
            path: &mut Vec<String>,
        ) -> Option<Vec<String>> {
            state.insert(node.to_string(), State::Visiting);
            path.push(node.to_string());

            if let Some(neighbors) = deps.get(node) {
                for dep in neighbors {
                    match state.get(dep.as_str()) {
                        Some(State::Visiting) => {
                            let cycle_start = path.iter().position(|n| n == dep)?;
                            let mut cycle: Vec<String> = path[cycle_start..].to_vec();
                            cycle.push(dep.clone());
                            return Some(cycle);
                        }
                        Some(State::Unvisited) | None => {
                            if let Some(cycle) = dfs(dep, deps, state, path) {
                                return Some(cycle);
                            }
                        }
                        Some(State::Visited) => {}
                    }
                }
            }

            path.pop();
            state.insert(node.to_string(), State::Visited);
            None
        }

        let all_nodes: Vec<String> = deps.keys().cloned().collect();
        for node in &all_nodes {
            if state.get(node) == Some(&State::Unvisited)
                && let Some(cycle) = dfs(node, &deps, &mut state, &mut path)
            {
                return Some(cycle);
            }
        }

        None
    }
}

fn merge_toml_values(base: &mut toml::Value, override_value: toml::Value) {
    match (base, override_value) {
        (toml::Value::Table(base_table), toml::Value::Table(override_table)) => {
            for (key, value) in override_table {
                match base_table.get_mut(&key) {
                    Some(base_value) => merge_toml_values(base_value, value),
                    None => {
                        base_table.insert(key, value);
                    }
                }
            }
        }
        (base_value, override_value) => *base_value = override_value,
    }
}

/// Key under `[profiles.<name>]` holding a config fragment merged over the base
/// file whenever that profile is the active one.
const PROFILE_OVERRIDES_KEY: &str = "overrides";

const PROFILE_KEYS: &[&str] = &["services", "tasks", PROFILE_OVERRIDES_KEY];

/// A misspelled key under `[profiles.<name>]` would otherwise be ignored in
/// silence, and "my profile's overrides did nothing" is a miserable thing to
/// debug.
fn validate_profile_keys(value: &toml::Value) -> Result<(), String> {
    let Some(profiles) = value.get("profiles").and_then(toml::Value::as_table) else {
        return Ok(());
    };
    let known: HashSet<&str> = PROFILE_KEYS.iter().copied().collect();
    for (name, profile) in profiles {
        let Some(table) = profile.as_table() else {
            continue;
        };
        for key in table.keys() {
            if !known.contains(key.as_str()) {
                let suggestion = suggest_typo(key, &known);
                return Err(format!("profile '{name}': unknown key '{key}'{suggestion}"));
            }
        }
    }
    Ok(())
}

fn validate_profile_overrides(profile: &str, overrides: &toml::Value) -> Result<(), String> {
    let Some(table) = overrides.as_table() else {
        return Err(format!(
            "profile '{profile}': '{PROFILE_OVERRIDES_KEY}' must be a table of \
             don.toml sections, like [profiles.{profile}.{PROFILE_OVERRIDES_KEY}.services.api]"
        ));
    };
    for key in ["profiles", "default_profile"] {
        if table.contains_key(key) {
            return Err(format!(
                "profile '{profile}': '{PROFILE_OVERRIDES_KEY}' cannot set '{key}' — an \
                 overrides block changes services and tasks, not which profile is active"
            ));
        }
    }
    Ok(())
}

/// Errors that can occur when loading or validating a don config.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The config file could not be read from disk.
    #[error("failed to read config file '{}': {source}", path.display())]
    ReadFile {
        path: PathBuf,
        source: std::io::Error,
    },
    /// The config file contains invalid TOML or doesn't match the expected schema.
    #[error("failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),
    /// The config is syntactically valid but contains semantic errors.
    #[error("config validation failed:\n{}", errors.join("\n"))]
    Validation { errors: Vec<String> },
}

/// Levenshtein edit distance between two strings.
fn levenshtein(a: &str, b: &str) -> usize {
    let n = b.len();
    let mut prev: Vec<usize> = (0..=n).collect();
    let mut curr = vec![0; n + 1];
    for (i, ca) in a.chars().enumerate() {
        curr[0] = i + 1;
        for (j, cb) in b.chars().enumerate() {
            let cost = usize::from(ca != cb);
            curr[j + 1] = (prev[j] + cost).min(prev[j + 1] + 1).min(curr[j] + 1);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[n]
}

/// Param names that can't be declared because they collide with flags on
/// `don run` (or global flags like `--config`/`--verbose`). Validated at
/// config-parse time so the collision is caught before the user tries to
/// invoke the task.
const RESERVED_PARAM_NAMES: &[&str] = &[
    "no-prompt",
    "wait",
    "timeout",
    "help",
    "version",
    "config",
    "verbose",
];

/// Validate a task's `params` declarations and the `{{name}}` placeholders
/// in its command surface (`cmd`, `args`, `env`, `dir`). Appends to `errors`.
fn validate_task_params(task_name: &str, task: &Task, errors: &mut Vec<String>) {
    // Build the declared-name set for placeholder lookup. Also catch
    // duplicate param names — serde won't flag them because `params` is a
    // list, not a map.
    let mut declared: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for p in &task.params {
        if p.name.is_empty() {
            errors.push(format!("task '{task_name}': param has an empty name"));
            continue;
        }
        if !is_valid_param_ident(&p.name) {
            errors.push(format!(
                "task '{task_name}': param '{}' has an invalid name — \
                 must be an identifier (letters, digits, underscore, dash) \
                 starting with a letter or underscore",
                p.name
            ));
            continue;
        }
        if RESERVED_PARAM_NAMES.contains(&p.name.as_str()) {
            errors.push(format!(
                "task '{task_name}': param name '{}' collides with a built-in \
                 `don run` flag — pick a different name",
                p.name
            ));
        }
        if !declared.insert(p.name.as_str()) {
            errors.push(format!(
                "task '{task_name}': param '{}' is declared more than once",
                p.name
            ));
        }
        // Either static choices OR a completions command, never both.
        if !p.choices.is_empty() && p.completions.is_some() {
            errors.push(format!(
                "task '{task_name}': param '{}' has both 'choices' and \
                 'completions' — pick one",
                p.name
            ));
        }
        // Int range sanity.
        if matches!(p.kind, param::ParamKind::Int)
            && let Some(v) = p.validate
            && let (Some(min), Some(max)) = (v.min, v.max)
            && min > max
        {
            errors.push(format!(
                "task '{task_name}': param '{}' validate.min ({min}) > validate.max ({max})",
                p.name
            ));
        }
        // `validate` only applies to int kind today.
        if p.validate.is_some() && !matches!(p.kind, param::ParamKind::Int) {
            errors.push(format!(
                "task '{task_name}': param '{}' has 'validate' but kind is not 'int'",
                p.name
            ));
        }
        // Choices requires string/choice kind.
        if !p.choices.is_empty()
            && !matches!(p.kind, param::ParamKind::String | param::ParamKind::Choice)
        {
            errors.push(format!(
                "task '{task_name}': param '{}' has 'choices' but kind is not 'string' or 'choice'",
                p.name
            ));
        }
        // Completion cache / timeout durations must parse.
        if let Some(ref comp) = p.completions {
            if comp.cmd.is_empty() {
                errors.push(format!(
                    "task '{task_name}': param '{}' completions.cmd is empty",
                    p.name
                ));
            }
            if let Some(ref cache) = comp.cache
                && let Err(e) = crate::duration::parse_duration(cache)
            {
                errors.push(format!(
                    "task '{task_name}': param '{}' completions.cache: {e}",
                    p.name
                ));
            }
            if let Some(ref timeout) = comp.timeout
                && let Err(e) = crate::duration::parse_duration(timeout)
            {
                errors.push(format!(
                    "task '{task_name}': param '{}' completions.timeout: {e}",
                    p.name
                ));
            }
        }
        // Default for bool must be parseable.
        if matches!(p.kind, param::ParamKind::Bool)
            && let Some(ref d) = p.default
            && parse_bool_value(d).is_none()
        {
            errors.push(format!(
                "task '{task_name}': param '{}' bool default '{d}' must be 'true' or 'false'",
                p.name
            ));
        }
        // Default for int must parse and satisfy the range.
        if matches!(p.kind, param::ParamKind::Int)
            && let Some(ref d) = p.default
        {
            match d.parse::<i64>() {
                Ok(n) => {
                    if let Some(v) = p.validate {
                        if let Some(min) = v.min
                            && n < min
                        {
                            errors.push(format!(
                                "task '{task_name}': param '{}' default {n} is less than min {min}",
                                p.name
                            ));
                        }
                        if let Some(max) = v.max
                            && n > max
                        {
                            errors.push(format!(
                                "task '{task_name}': param '{}' default {n} is greater than max {max}",
                                p.name
                            ));
                        }
                    }
                }
                Err(_) => errors.push(format!(
                    "task '{task_name}': param '{}' int default '{d}' is not a valid integer",
                    p.name
                )),
            }
        }
        // Default for choice must be among the static choices if any.
        if !p.choices.is_empty()
            && let Some(ref d) = p.default
            && !p.choices.contains(d)
        {
            errors.push(format!(
                "task '{task_name}': param '{}' default '{d}' is not among choices [{}]",
                p.name,
                p.choices.join(", ")
            ));
        }
    }

    // Now scan every string in the task's command surface for `{{name}}`
    // references and confirm they match a declared param. Unknown
    // placeholders get a typo suggestion from the declared set.
    let mut scan = |context: &str, s: &str| {
        for r in template::collect_references(s) {
            if !declared.contains(r.as_str()) {
                let suggestion = suggest_typo(&r, &declared);
                errors.push(format!(
                    "task '{task_name}': {context} references undeclared param '{{{{{r}}}}}'{suggestion}"
                ));
            }
        }
    };
    if let Some(cmd) = &task.cmd {
        scan("cmd", cmd);
    }
    for (idx, arg) in task.args.iter().enumerate() {
        scan(&format!("args[{idx}]"), arg);
    }
    if let Some(headless) = &task.headless {
        if let Some(cmd) = &headless.cmd {
            scan("headless.cmd", cmd);
        }
        if let Some(args) = &headless.args {
            for (idx, arg) in args.iter().enumerate() {
                scan(&format!("headless.args[{idx}]"), arg);
            }
        }
    }
    for (k, v) in &task.env {
        scan(&format!("env['{k}']"), v);
    }
    if let Some(ref dir) = task.dir {
        scan("dir", &dir.to_string_lossy());
    }
}

fn is_valid_param_ident(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Parse a string value as a bool, matching `ParamKind::Bool` semantics.
/// Returns `None` on any unrecognized value.
pub(crate) fn parse_bool_value(s: &str) -> Option<bool> {
    match s.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Suggest a typo correction for `input` from `candidates`.
/// Returns ` — did you mean '<best>'?` or empty string if no close match.
fn suggest_typo(input: &str, candidates: &std::collections::HashSet<&str>) -> String {
    let max_distance = match input.len() {
        0..=2 => 1,
        3..=5 => 2,
        _ => 3,
    };
    let mut best: Option<(&str, usize)> = None;
    for &candidate in candidates {
        let d = levenshtein(input, candidate);
        if d <= max_distance && d > 0 && best.is_none_or(|(_, bd)| d < bd) {
            best = Some((candidate, d));
        }
    }
    match best {
        Some((name, _)) => format!(" — did you mean '{name}'?"),
        None => String::new(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    const TEST_PLATFORM: Platform = Platform::LinuxX86_64;

    /// Which `.bazelrc` configuration a target builds under: its own if it
    /// names one, else the workspace's, else none at all. Resolved once, where
    /// the build request is built, so nothing downstream has to know a
    /// workspace default exists.
    #[test]
    fn a_bazel_target_takes_its_configuration_from_itself_then_the_workspace() {
        struct Case {
            name: &'static str,
            toml: &'static str,
            want_service: Option<&'static str>,
            want_task: Option<&'static str>,
        }

        let cases = vec![
            Case {
                name: "nothing named anywhere",
                toml: "[services.api]\nbazel.target = \"//api\"\n\
                       [tasks.gen]\ncmd = \"true\"\nbazel.target = \"//gen\"\n",
                want_service: None,
                want_task: None,
            },
            Case {
                name: "the workspace names one and both inherit it",
                toml: "[bazel]\nconfig = \"don\"\n\
                       [services.api]\nbazel.target = \"//api\"\n\
                       [tasks.gen]\ncmd = \"true\"\nbazel.target = \"//gen\"\n",
                want_service: Some("don"),
                want_task: Some("don"),
            },
            Case {
                name: "an item that names its own overrides the workspace",
                toml: "[bazel]\nconfig = \"don\"\n\
                       [services.api]\nbazel.target = \"//api\"\nbazel.config = \"api-dev\"\n\
                       [tasks.gen]\ncmd = \"true\"\nbazel.target = \"//gen\"\nbazel.config = \"gen\"\n",
                want_service: Some("api-dev"),
                want_task: Some("gen"),
            },
            Case {
                name: "an item may name one where the workspace does not",
                toml: "[services.api]\nbazel.target = \"//api\"\nbazel.config = \"api-dev\"\n\
                       [tasks.gen]\ncmd = \"true\"\nbazel.target = \"//gen\"\n",
                want_service: Some("api-dev"),
                want_task: None,
            },
        ];

        for case in cases {
            let config: Config = case.toml.parse().unwrap();
            let workspace = config.bazel.config.as_deref();

            let service = config.services.get("api").unwrap().resolve(TEST_PLATFORM);
            let resolved = service
                .bazel_config()
                .cloned()
                .unwrap()
                .with_workspace_default(workspace);
            assert_eq!(
                resolved.config.as_deref(),
                case.want_service,
                "{}: service",
                case.name
            );

            let task = config.tasks.get("gen").unwrap();
            let resolved = task
                .bazel
                .clone()
                .unwrap()
                .with_workspace_default(workspace);
            assert_eq!(
                resolved.config.as_deref(),
                case.want_task,
                "{}: task",
                case.name
            );
        }
    }

    /// Naming a configuration bazel could never resolve fails the config
    /// rather than every bazel build in it. Whether the name is actually
    /// defined is bazel's to answer, and it does — loudly.
    #[test]
    fn an_unusable_bazel_configuration_name_is_rejected() {
        struct Case {
            name: &'static str,
            config: &'static str,
            want_err: bool,
        }

        let cases = vec![
            Case {
                name: "an ordinary name",
                config: "don",
                want_err: false,
            },
            Case {
                name: "names with punctuation bazel allows",
                config: "don-dev_2",
                want_err: false,
            },
            Case {
                name: "empty",
                config: "",
                want_err: true,
            },
            Case {
                name: "blank",
                config: "   ",
                want_err: true,
            },
            Case {
                name: "a flag, not a name",
                config: "--noshow_progress",
                want_err: true,
            },
            Case {
                name: "several flags crammed in",
                config: "don --noshow_progress",
                want_err: true,
            },
        ];

        for case in cases {
            for (label, toml) in [
                (
                    "global",
                    format!(
                        "[bazel]\nconfig = \"{}\"\n[services.api]\nbazel.target = \"//api\"\n",
                        case.config
                    ),
                ),
                (
                    "service",
                    format!(
                        "[services.api]\nbazel.target = \"//api\"\nbazel.config = \"{}\"\n",
                        case.config
                    ),
                ),
                (
                    "task",
                    format!(
                        "[tasks.gen]\ncmd = \"true\"\nbazel.target = \"//gen\"\nbazel.config = \"{}\"\n",
                        case.config
                    ),
                ),
            ] {
                let config: Config = toml.parse().unwrap();
                let result = config.validate(TEST_PLATFORM);
                assert_eq!(
                    result.is_err(),
                    case.want_err,
                    "{} ({label}): {result:?}",
                    case.name
                );
                if case.want_err {
                    let message = format!("{}", result.unwrap_err());
                    assert!(
                        message.contains(".bazelrc"),
                        "{} ({label}): the error should say where a config comes from: {message}",
                        case.name
                    );
                }
            }
        }
    }

    #[test]
    fn test_validate_platform_download() {
        let good_sha = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        struct Case {
            name: &'static str,
            url: String,
            sha256: String,
            expect_err: Option<&'static str>,
        }
        let cases = vec![
            Case {
                name: "valid https url and sha256",
                url: "https://example.com/tool.tar.gz".to_string(),
                sha256: good_sha.to_string(),
                expect_err: None,
            },
            Case {
                name: "valid http url",
                url: "http://example.com/tool".to_string(),
                sha256: good_sha.to_string(),
                expect_err: None,
            },
            Case {
                name: "missing scheme",
                url: "example.com/tool.tar.gz".to_string(),
                sha256: good_sha.to_string(),
                expect_err: Some("must start with http"),
            },
            Case {
                name: "ftp scheme",
                url: "ftp://example.com/tool".to_string(),
                sha256: good_sha.to_string(),
                expect_err: Some("must start with http"),
            },
            Case {
                name: "sha256 too short",
                url: "https://example.com/tool".to_string(),
                sha256: "abc123".to_string(),
                expect_err: Some("64 hex characters"),
            },
            Case {
                name: "sha256 too long",
                url: "https://example.com/tool".to_string(),
                sha256: "a".repeat(70),
                expect_err: Some("64 hex characters"),
            },
            Case {
                name: "sha256 non-hex",
                url: "https://example.com/tool".to_string(),
                sha256: "g".repeat(64),
                expect_err: Some("hex characters"),
            },
        ];
        for case in cases {
            let artifact = PlatformDownload {
                url: case.url,
                sha256: case.sha256,
                path: None,
                setup: None,
                headers: std::collections::HashMap::new(),
            };
            let result = validate_platform_download(&artifact);
            match (result, case.expect_err) {
                (Ok(()), None) => {}
                (Err(msg), Some(needle)) => assert!(
                    msg.contains(needle),
                    "case '{}': expected error containing '{}', got '{}'",
                    case.name,
                    needle,
                    msg
                ),
                (Ok(()), Some(needle)) => {
                    panic!(
                        "case '{}': expected error containing '{}' but got Ok",
                        case.name, needle
                    )
                }
                (Err(msg), None) => {
                    panic!("case '{}': expected Ok but got error: {}", case.name, msg)
                }
            }
        }
    }

    #[derive(Debug)]
    struct ConfigTestCase {
        name: &'static str,
        input: &'static str,
        expect_err: bool,
        check: fn(&Config),
    }

    #[test]
    fn test_config_parsing() {
        let cases = vec![
            ConfigTestCase {
                name: "fallback ports defaults off",
                input: r#"
                    [services.api]
                    run.cmd = "api"
                "#,
                expect_err: false,
                check: |config| {
                    assert!(!config.fallback_ports);
                },
            },
            ConfigTestCase {
                name: "fallback ports parses",
                input: r#"
                    fallback_ports = true

                    [services.api]
                    run.cmd = "api"
                "#,
                expect_err: false,
                check: |config| {
                    assert!(config.fallback_ports);
                },
            },
            ConfigTestCase {
                name: "global watch ignore parses",
                input: r#"
                    watch_ignore = ["target/**", ".don/**"]

                    [services.api]
                    run.cmd = "api"
                "#,
                expect_err: false,
                check: |config| {
                    assert_eq!(config.watch_ignore, vec!["target/**", ".don/**"]);
                },
            },
            ConfigTestCase {
                name: "docker service with all fields",
                input: r#"
                    [services.postgres]
                    dir = "/data"
                    docker.image = "postgres:16"
                    docker.container = "my-postgres"
                    docker.ports = ["5432:5432"]
                    docker.volumes = ["pgdata:/var/lib/postgresql/data"]
                    docker.network = "my-net"
                    docker.command = ["postgres", "-c", "max_connections=200"]
                    docker.env_file = [".env.postgres.docker"]
                    env = { POSTGRES_PASSWORD = "dev" }
                    env_file = [".env.shared"]

                    [services.postgres.ready]
                    exec.cmd = "pg_isready"
                    exec.args = ["-h", "localhost"]
                    interval = "500ms"
                    retries = 60

                    [services.postgres.shutdown]
                    signal = "SIGINT"
                    timeout = "30s"
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["postgres"].resolve(TEST_PLATFORM);
                    assert_eq!(resolved.dir.as_deref(), Some(std::path::Path::new("/data")));
                    let Some(ServiceKind::Docker(docker)) = &resolved.kind else {
                        panic!("expected docker preset");
                    };
                    assert_eq!(docker.image.as_deref(), Some("postgres:16"));
                    assert_eq!(docker.container.as_deref(), Some("my-postgres"));
                    assert_eq!(docker.ports, vec!["5432:5432"]);
                    assert_eq!(docker.volumes, vec!["pgdata:/var/lib/postgresql/data"]);
                    assert_eq!(docker.network.as_deref(), Some("my-net"));
                    assert_eq!(
                        docker.command,
                        vec!["postgres", "-c", "max_connections=200"]
                    );
                    assert_eq!(docker.env_file, vec![PathBuf::from(".env.postgres.docker")]);
                    assert_eq!(resolved.env["POSTGRES_PASSWORD"], "dev");
                    assert_eq!(resolved.env_file, vec![PathBuf::from(".env.shared")]);

                    let ready = resolved.ready.as_ref().unwrap();
                    let exec = ready.exec.as_ref().unwrap();
                    assert_eq!(exec.cmd, "pg_isready");
                    assert_eq!(exec.args, vec!["-h", "localhost"]);
                    assert_eq!(ready.interval, "500ms");
                    assert_eq!(ready.retries, 60);

                    let shutdown = resolved.shutdown.as_ref().unwrap();
                    assert_eq!(shutdown.signal, "SIGINT");
                    assert_eq!(shutdown.timeout, "30s");
                },
            },
            ConfigTestCase {
                name: "docker service with build",
                input: r#"
                    [services.api]
                    docker.image = "myapp:dev"
                    docker.ports = ["3000:3000"]
                    docker.build.context = "./services/api"
                    docker.build.dockerfile = "Dockerfile.dev"
                    docker.build.target = "development"
                    docker.build.args = { RUST_VERSION = "1.80" }
                    watch = ["services/api/src/**/*.rs", "services/api/Dockerfile.dev"]
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["api"].resolve(TEST_PLATFORM);
                    let Some(ServiceKind::Docker(docker)) = &resolved.kind else {
                        panic!("expected docker preset");
                    };
                    assert_eq!(docker.image.as_deref(), Some("myapp:dev"));
                    let build = docker.build.as_ref().unwrap();
                    assert_eq!(build.context, "./services/api");
                    assert_eq!(build.dockerfile.as_deref(), Some("Dockerfile.dev"));
                    assert_eq!(build.target.as_deref(), Some("development"));
                    assert_eq!(build.args["RUST_VERSION"], "1.80");
                    assert_eq!(
                        resolved.watch,
                        vec!["services/api/src/**/*.rs", "services/api/Dockerfile.dev",]
                    );
                },
            },
            ConfigTestCase {
                name: "docker service builds without an explicit image",
                input: r#"
                    [services.db]
                    docker.build.context = "."
                    docker.ports = ["5432:5432"]
                "#,
                expect_err: false,
                check: |config| {
                    // A build config alone is enough — no `image` required.
                    assert!(config.validate(TEST_PLATFORM).is_ok());
                    let resolved = config.services["db"].resolve(TEST_PLATFORM);
                    let Some(ServiceKind::Docker(docker)) = &resolved.kind else {
                        panic!("expected docker preset");
                    };
                    assert!(docker.image.is_none());
                    // The built image is tagged from the service name.
                    assert_eq!(docker.image_tag("db"), "don-db");
                },
            },
            ConfigTestCase {
                name: "docker service without image or build is a validation error",
                input: r#"
                    [services.db]
                    docker.ports = ["5432:5432"]
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(
                        errors
                            .iter()
                            .any(|e| e.contains("must set `docker.image` or `docker.build`")),
                        "got: {errors:?}"
                    );
                },
            },
            ConfigTestCase {
                name: "rust service with all fields",
                input: r#"
                    [services.api]
                    dir = "./api"
                    rust.binary = "api-server"
                    rust.features = ["dev"]
                    rust.release = true
                    rust.extra_args = ["--jobs", "4"]
                    rust.target_dir = "./target-api"
                    depends_on = ["postgres"]
                    proxy = "0.0.0.0:3000"

                    [services.api.ready]
                    http = "http://localhost:3000/healthz"

                    [services.api.shutdown]
                    timeout = "5s"
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["api"].resolve(TEST_PLATFORM);
                    assert_eq!(resolved.dir.as_deref(), Some(std::path::Path::new("./api")));
                    let Some(ServiceKind::Rust(rust)) = &resolved.kind else {
                        panic!("expected rust preset");
                    };
                    assert_eq!(rust.binary, "api-server");
                    assert_eq!(rust.features, vec!["dev"]);
                    assert!(rust.release);
                    assert_eq!(rust.extra_args, vec!["--jobs", "4"]);
                    assert_eq!(
                        rust.target_dir.as_deref(),
                        Some(std::path::Path::new("./target-api"))
                    );
                    assert_eq!(resolved.depends_on, vec![Dependency::blocking("postgres")]);
                    assert_eq!(resolved.proxy.len(), 1);
                    assert_eq!(resolved.proxy[0].listen, "0.0.0.0:3000");
                    assert_eq!(resolved.proxy[0].mode, crate::config::ProxyMode::Listenfd);

                    let ready = resolved.ready.as_ref().unwrap();
                    assert_eq!(ready.http.as_deref(), Some("http://localhost:3000/healthz"));
                    assert!(ready.exec.is_none());
                    assert!(ready.tcp.is_none());
                    assert_eq!(ready.interval, "1s");
                    assert_eq!(ready.retries, 30);

                    let shutdown = resolved.shutdown.as_ref().unwrap();
                    assert_eq!(shutdown.signal, "SIGTERM");
                    assert_eq!(shutdown.timeout, "5s");
                },
            },
            ConfigTestCase {
                name: "custom service with cmd and args",
                input: r#"
                    [services.worker]
                    dir = "./worker"
                    run.cmd = "node"
                    run.args = ["worker.js"]
                    build.cmd = "npm"
                    build.args = ["run", "build"]
                    watch = ["src/**/*.js"]

                    [services.worker.ready]
                    tcp = "localhost:9090"
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["worker"].resolve(TEST_PLATFORM);
                    let Some(ServiceKind::Custom { run, build }) = &resolved.kind else {
                        panic!("expected custom preset");
                    };
                    assert_eq!(run.cmd, "node");
                    assert_eq!(run.args, vec!["worker.js"]);
                    let build = build.as_ref().unwrap();
                    assert_eq!(build.cmd, "npm");
                    assert_eq!(build.args, vec!["run", "build"]);
                    assert_eq!(
                        resolved.dir.as_deref(),
                        Some(std::path::Path::new("./worker"))
                    );
                    assert_eq!(resolved.watch, vec!["src/**/*.js"]);

                    let ready = resolved.ready.as_ref().unwrap();
                    assert_eq!(ready.tcp.as_deref(), Some("localhost:9090"));
                },
            },
            ConfigTestCase {
                name: "custom service with no args",
                input: r#"
                    [services.simple]
                    run.cmd = "/usr/bin/myservice"
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["simple"].resolve(TEST_PLATFORM);
                    let Some(ServiceKind::Custom { run, build }) = &resolved.kind else {
                        panic!("expected custom preset");
                    };
                    assert_eq!(run.cmd, "/usr/bin/myservice");
                    assert!(run.args.is_empty());
                    assert!(build.is_none());
                    assert!(resolved.ready.is_none());
                    assert!(resolved.shutdown.is_none());
                },
            },
            ConfigTestCase {
                name: "custom service with download and setup",
                input: r#"
                    [services.crdb]
                    run.cmd = "cockroach"
                    run.args = ["start-single-node", "--insecure"]

                    [services.crdb.download.platform.linux-x86_64]
                    url = "https://binaries.cockroachdb.com/cockroach-v24.1.0.linux-amd64.tgz"
                    sha256 = "abcdef1234567890"
                    path = "cockroach-v24.1.0.linux-amd64/cockroach"

                    [services.crdb.download.platform.macos-aarch64]
                    url = "https://binaries.cockroachdb.com/cockroach-v24.1.0.darwin-11.0-arm64.tgz"
                    sha256 = "fedcba0987654321"
                    path = "cockroach-v24.1.0.darwin-11.0-arm64/cockroach"
                    setup.cmd = "chmod"
                    setup.args = ["+x", "cockroach-v24.1.0.darwin-11.0-arm64/cockroach"]
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["crdb"].resolve(TEST_PLATFORM);
                    let download = resolved.download.as_ref().unwrap();
                    assert_eq!(download.platform.len(), 2);

                    let linux = &download.platform[&Platform::LinuxX86_64];
                    assert_eq!(linux.sha256, "abcdef1234567890");
                    assert!(linux.setup.is_none());

                    let macos = &download.platform[&Platform::MacosAarch64];
                    let setup = macos.setup.as_ref().unwrap();
                    assert_eq!(setup.cmd, "chmod");
                },
            },
            ConfigTestCase {
                name: "platform override switches preset to docker",
                input: r#"
                    [services.crdb]
                    run.cmd = "cockroach"
                    run.args = ["start-single-node", "--insecure"]
                    env = { COCKROACH_PORT = "26257" }

                    [services.crdb.download.platform.linux-x86_64]
                    url = "https://binaries.cockroachdb.com/cockroach-v24.1.0.linux-amd64.tgz"
                    sha256 = "abcdef1234567890"
                    path = "cockroach-v24.1.0.linux-amd64/cockroach"

                    [services.crdb.platform.macos-aarch64]
                    docker.image = "cockroachdb/cockroach:v24.1.0"
                    docker.ports = ["26257:26257"]
                "#,
                expect_err: false,
                check: |config| {
                    let linux = config.services["crdb"].resolve(Platform::LinuxX86_64);
                    let Some(ServiceKind::Custom { run, .. }) = &linux.kind else {
                        panic!("expected custom preset on linux");
                    };
                    assert_eq!(run.cmd, "cockroach");
                    assert!(linux.download.is_some());
                    assert_eq!(linux.env["COCKROACH_PORT"], "26257");

                    let macos = config.services["crdb"].resolve(Platform::MacosAarch64);
                    let Some(ServiceKind::Docker(docker)) = &macos.kind else {
                        panic!("expected docker preset on macos");
                    };
                    assert_eq!(
                        docker.image.as_deref(),
                        Some("cockroachdb/cockroach:v24.1.0")
                    );
                    assert_eq!(macos.env["COCKROACH_PORT"], "26257");
                    assert!(macos.download.is_some());
                },
            },
            ConfigTestCase {
                name: "platform override merges env",
                input: r#"
                    [services.api]
                    rust.binary = "api-server"
                    env = { PORT = "3000", LOG_LEVEL = "info" }

                    [services.api.platform.linux-x86_64]
                    env = { LOG_LEVEL = "debug", EXTRA = "linux-only" }
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["api"].resolve(Platform::LinuxX86_64);
                    assert_eq!(resolved.env["PORT"], "3000");
                    assert_eq!(resolved.env["LOG_LEVEL"], "debug");
                    assert_eq!(resolved.env["EXTRA"], "linux-only");
                },
            },
            ConfigTestCase {
                name: "platform override replaces watch list",
                input: r#"
                    [services.api]
                    rust.binary = "api-server"
                    watch = ["src/**/*.rs"]

                    [services.api.platform.linux-x86_64]
                    watch = ["src/**/*.rs", "config/**/*.toml"]
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["api"].resolve(Platform::LinuxX86_64);
                    assert_eq!(resolved.watch, vec!["src/**/*.rs", "config/**/*.toml"]);

                    let other = config.services["api"].resolve(Platform::MacosAarch64);
                    assert_eq!(other.watch, vec!["src/**/*.rs"]);
                },
            },
            ConfigTestCase {
                name: "no base preset but valid via platform override",
                input: r#"
                    [services.crdb]
                    env = { PORT = "26257" }

                    [services.crdb.platform.linux-x86_64]
                    run.cmd = "cockroach"
                    run.args = ["start-single-node", "--insecure"]

                    [services.crdb.platform.macos-aarch64]
                    docker.image = "cockroachdb/cockroach:v24.1.0"
                "#,
                expect_err: false,
                check: |config| {
                    let linux = config.services["crdb"].resolve(Platform::LinuxX86_64);
                    let Some(ServiceKind::Custom { run, .. }) = &linux.kind else {
                        panic!("expected custom on linux");
                    };
                    assert_eq!(run.cmd, "cockroach");

                    let macos = config.services["crdb"].resolve(Platform::MacosAarch64);
                    let Some(ServiceKind::Docker(docker)) = &macos.kind else {
                        panic!("expected docker on macos");
                    };
                    assert_eq!(
                        docker.image.as_deref(),
                        Some("cockroachdb/cockroach:v24.1.0")
                    );

                    // Platform without an override and no base preset should fail
                    let other = config.services["crdb"].resolve(Platform::LinuxAarch64);
                    assert!(other.kind.is_none());
                },
            },
            ConfigTestCase {
                name: "invalid platform key",
                input: r#"
                    [services.bad]
                    run.cmd = "./bad"

                    [services.bad.download.platform.ubuntu-amd64]
                    url = "https://example.com/bad"
                    sha256 = "bad"
                "#,
                expect_err: true,
                check: |_| {},
            },
            ConfigTestCase {
                name: "log defaults to stdout",
                input: r#"
                    [services.svc]
                    run.cmd = "mybin"
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["svc"].resolve(TEST_PLATFORM);
                    assert!(matches!(resolved.log, LogConfig::Stdout));
                },
            },
            ConfigTestCase {
                name: "log ignore",
                input: r#"
                    [services.svc]
                    run.cmd = "mybin"
                    log = "ignore"
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["svc"].resolve(TEST_PLATFORM);
                    assert!(matches!(resolved.log, LogConfig::Ignore));
                },
            },
            ConfigTestCase {
                name: "log to file via string",
                input: r#"
                    [services.svc]
                    run.cmd = "mybin"
                    log = "logs/mybin.log"
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["svc"].resolve(TEST_PLATFORM);
                    let LogConfig::File(path) = &resolved.log else {
                        panic!("expected file log config");
                    };
                    assert_eq!(path, &PathBuf::from("logs/mybin.log"));
                },
            },
            ConfigTestCase {
                name: "log to file via table",
                input: r#"
                    [services.svc]
                    run.cmd = "mybin"
                    log.file = "logs/mybin.log"
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["svc"].resolve(TEST_PLATFORM);
                    let LogConfig::File(path) = &resolved.log else {
                        panic!("expected file log config");
                    };
                    assert_eq!(path, &PathBuf::from("logs/mybin.log"));
                },
            },
            ConfigTestCase {
                name: "log explicit stdout",
                input: r#"
                    [services.svc]
                    run.cmd = "mybin"
                    log = "stdout"
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["svc"].resolve(TEST_PLATFORM);
                    assert!(matches!(resolved.log, LogConfig::Stdout));
                },
            },
            ConfigTestCase {
                name: "task with file watching",
                input: r#"
                    [services.postgres]
                    docker.image = "postgres:16"
                    [services.postgres.ready]
                    tcp = "localhost:5432"

                    [tasks.migrate]
                    cmd = "dbmate"
                    args = ["up"]
                    depends_on = ["postgres"]
                    watch = ["db/migrations/**/*.sql"]
                    dir = "./db"
                    env = { DATABASE_URL = "postgres://localhost:5432/dev" }

                    [tasks.seed]
                    cmd = "psql"
                    args = ["-f", "seed.sql"]
                    depends_on = ["migrate"]
                    watch = ["db/seed.sql"]
                    log = "ignore"
                "#,
                expect_err: false,
                check: |config| {
                    assert_eq!(config.tasks.len(), 2);

                    let migrate = &config.tasks["migrate"];
                    assert_eq!(migrate.cmd.as_deref(), Some("dbmate"));
                    assert_eq!(migrate.args, vec!["up"]);
                    assert_eq!(migrate.depends_on, vec![Dependency::blocking("postgres")]);
                    assert_eq!(migrate.watch, vec!["db/migrations/**/*.sql"]);
                    assert_eq!(migrate.dir.as_deref(), Some(std::path::Path::new("./db")));
                    assert_eq!(migrate.env["DATABASE_URL"], "postgres://localhost:5432/dev");

                    let seed = &config.tasks["seed"];
                    assert_eq!(seed.depends_on, vec![Dependency::blocking("migrate")]);
                    assert!(matches!(seed.log, LogConfig::Ignore));

                    assert!(config.validate(TEST_PLATFORM).is_ok());
                },
            },
            ConfigTestCase {
                name: "task with no watch always runs",
                input: r#"
                    [tasks.setup]
                    cmd = "echo"
                    args = ["hello"]
                "#,
                expect_err: false,
                check: |config| {
                    let task = &config.tasks["setup"];
                    assert!(task.watch.is_empty());
                    assert!(task.depends_on.is_empty());
                    assert!(config.validate(TEST_PLATFORM).is_ok());
                },
            },
            ConfigTestCase {
                name: "service depends on task",
                input: r#"
                    [tasks.migrate]
                    cmd = "dbmate"
                    args = ["up"]

                    [services.api]
                    rust.binary = "api-server"
                    depends_on = ["migrate"]
                "#,
                expect_err: false,
                check: |config| {
                    assert!(config.validate(TEST_PLATFORM).is_ok());
                },
            },
            ConfigTestCase {
                name: "task depends on unknown name is a validation error",
                input: r#"
                    [tasks.migrate]
                    cmd = "dbmate"
                    args = ["up"]
                    depends_on = ["nonexistent"]
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(
                        errors[0].contains("unknown service, task, or service group 'nonexistent'")
                    );
                },
            },
            ConfigTestCase {
                name: "service and task with same name is a validation error",
                input: r#"
                    [services.foo]
                    run.cmd = "foo"

                    [tasks.foo]
                    cmd = "foo"
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(errors[0].contains("both a service and a task"));
                },
            },
            ConfigTestCase {
                name: "service depends on unknown name is a validation error",
                input: r#"
                    [services.api]
                    run.cmd = "api"
                    depends_on = ["ghost"]
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(errors[0].contains("unknown service, task, or service group 'ghost'"));
                },
            },
            ConfigTestCase {
                name: "service groups are valid in dependencies and profiles",
                input: r#"
                    [services.postgres]
                    run.cmd = "postgres"

                    [services.redis]
                    run.cmd = "redis"

                    [services.api]
                    run.cmd = "api"
                    depends_on = ["datastores"]

                    [tasks.seed]
                    cmd = "seed"
                    depends_on = ["datastores"]

                    [service_groups]
                    datastores = ["postgres", "redis"]

                    [profiles.dev]
                    services = ["api", "datastores"]
                    tasks = ["seed"]
                "#,
                expect_err: false,
                check: |config| {
                    assert!(config.validate(TEST_PLATFORM).is_ok());
                    let group = config.service_groups.get("datastores").unwrap();
                    assert_eq!(group.members, vec!["postgres", "redis"]);
                    assert!(group.depends_on.is_empty());
                },
            },
            ConfigTestCase {
                name: "service groups can reference other service groups",
                input: r#"
                    [services.postgres]
                    run.cmd = "postgres"

                    [services.redis]
                    run.cmd = "redis"

                    [services.api]
                    run.cmd = "api"

                    [services.worker]
                    run.cmd = "worker"
                    depends_on = ["backend"]

                    [service_groups]
                    datastores = ["postgres", "redis"]
                    backend = ["datastores", "api"]

                    [profiles.dev]
                    services = ["backend"]
                "#,
                expect_err: false,
                check: |config| {
                    assert!(config.validate(TEST_PLATFORM).is_ok());
                    assert_eq!(
                        config.expand_service_refs(&["backend".to_string()]),
                        vec![
                            "postgres".to_string(),
                            "redis".to_string(),
                            "api".to_string(),
                        ]
                    );
                },
            },
            ConfigTestCase {
                name: "service group with unknown service is a validation error",
                input: r#"
                    [services.api]
                    run.cmd = "api"

                    [service_groups]
                    datastores = ["postgres"]
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(errors.iter().any(|e| {
                        e.contains("service group 'datastores'")
                            && e.contains("unknown service or service group 'postgres'")
                    }));
                },
            },
            ConfigTestCase {
                name: "service group cycle is a validation error",
                input: r#"
                    [services.api]
                    run.cmd = "api"

                    [service_groups]
                    first = ["second"]
                    second = ["first"]
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(errors.iter().any(|e| e.contains("service group cycle")));
                },
            },
            ConfigTestCase {
                name: "service group name colliding with service is a validation error",
                input: r#"
                    [services.api]
                    run.cmd = "api"

                    [service_groups]
                    api = ["api"]
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(errors.iter().any(|e| {
                        e.contains("service group 'api'") && e.contains("conflicts with service")
                    }));
                },
            },
            ConfigTestCase {
                name: "dependency cycle is a validation error",
                input: r#"
                    [services.a]
                    run.cmd = "a"
                    depends_on = ["b"]

                    [services.b]
                    run.cmd = "b"
                    depends_on = ["c"]

                    [tasks.c]
                    cmd = "c"
                    depends_on = ["a"]
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(errors.iter().any(|e| e.contains("dependency cycle")));
                },
            },
            ConfigTestCase {
                name: "service group with depends_on parses",
                input: r#"
                    [services.api]
                    run.cmd = "api"

                    [services.web]
                    run.cmd = "web"

                    [service_groups.frontend]
                    members = ["web"]
                    depends_on = ["api"]
                "#,
                expect_err: false,
                check: |config| {
                    assert!(config.validate(TEST_PLATFORM).is_ok());
                    let group = config.service_groups.get("frontend").unwrap();
                    assert_eq!(group.members, vec!["web"]);
                    assert_eq!(group.depends_on, vec![Dependency::blocking("api")]);
                },
            },
            ConfigTestCase {
                name: "service group secrets inherit unless the member declares its own",
                input: r#"
                    [[secrets]]
                    aws-ssm = {}
                    [secrets.vars]
                    LAUNCH_DARKLY_SDK_KEY = "/app/Ld"
                    STRIPE_WEBHOOK_SECRET = "/app/StripeWebhook"
                    OTHER_KEY = "/app/Other"
                    [secrets.groups]
                    production = ["LAUNCH_DARKLY_SDK_KEY"]
                    other-secrets-group = ["OTHER_KEY"]

                    [services.api-server]
                    run.cmd = "api"
                    secrets = ["other-secrets-group", "STRIPE_WEBHOOK_SECRET"]

                    [services.admin-server]
                    run.cmd = "admin"

                    [services.cockroachdb]
                    run.cmd = "db"

                    [service_groups.application-services]
                    members = ["api-server", "admin-server"]
                    secrets = ["production"]
                "#,
                expect_err: false,
                check: |config| {
                    assert!(config.validate(TEST_PLATFORM).is_ok());
                    let mut got = config.effective_secrets(
                        "api-server",
                        config.services["api-server"].secrets.as_deref(),
                    );
                    got.sort();
                    assert_eq!(got, vec!["STRIPE_WEBHOOK_SECRET", "other-secrets-group"]);
                    assert_eq!(
                        config.effective_secrets(
                            "admin-server",
                            config.services["admin-server"].secrets.as_deref(),
                        ),
                        vec!["production"]
                    );
                    assert!(
                        config
                            .effective_secrets(
                                "cockroachdb",
                                config.services["cockroachdb"].secrets.as_deref()
                            )
                            .is_empty()
                    );
                },
            },
            ConfigTestCase {
                name: "service group with depends_on but no members parses",
                input: r#"
                    [services.api]
                    run.cmd = "api"

                    [service_groups.frontend]
                    depends_on = ["api"]
                "#,
                expect_err: false,
                check: |config| {
                    assert!(config.validate(TEST_PLATFORM).is_ok());
                    let group = config.service_groups.get("frontend").unwrap();
                    assert!(group.members.is_empty());
                    assert_eq!(group.depends_on, vec![Dependency::blocking("api")]);
                },
            },
            ConfigTestCase {
                name: "group depends_on applies to direct member",
                input: r#"
                    [services.api]
                    run.cmd = "api"

                    [services.web]
                    run.cmd = "web"
                    depends_on = ["self-only"]

                    [services."self-only"]
                    run.cmd = "self-only"

                    [service_groups.frontend]
                    members = ["web"]
                    depends_on = ["api"]
                "#,
                expect_err: false,
                check: |config| {
                    assert!(config.validate(TEST_PLATFORM).is_ok());
                    let mut deps =
                        config.effective_depends_on("web", &[Dependency::blocking("self-only")]);
                    deps.sort();
                    assert_eq!(
                        deps,
                        vec![
                            Dependency::blocking("api"),
                            Dependency::blocking("self-only"),
                        ]
                    );
                },
            },
            ConfigTestCase {
                name: "group depends_on applies transitively to nested members",
                input: r#"
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
                "#,
                expect_err: false,
                check: |config| {
                    assert!(config.validate(TEST_PLATFORM).is_ok());
                    assert_eq!(
                        config.effective_depends_on("web", &[]),
                        vec![Dependency::blocking("api")],
                    );
                    assert_eq!(
                        config.effective_depends_on("admin", &[]),
                        vec![Dependency::blocking("api")],
                    );
                },
            },
            ConfigTestCase {
                name: "group depends_on can reference another group",
                input: r#"
                    [services.web]
                    run.cmd = "web"

                    [services.api]
                    run.cmd = "api"

                    [services.worker]
                    run.cmd = "worker"

                    [service_groups.backend]
                    members = ["api", "worker"]

                    [service_groups.frontend]
                    members = ["web"]
                    depends_on = ["backend"]
                "#,
                expect_err: false,
                check: |config| {
                    assert!(config.validate(TEST_PLATFORM).is_ok());
                    let mut deps = config.effective_depends_on("web", &[]);
                    deps.sort();
                    assert_eq!(
                        deps,
                        vec![Dependency::blocking("api"), Dependency::blocking("worker")]
                    );
                },
            },
            ConfigTestCase {
                name: "a non-blocking group reference makes every member non-blocking",
                input: r#"
                    [services.web]
                    run.cmd = "web"
                    depends_on = [{ name = "backend", blocking = false }]

                    [services.api]
                    run.cmd = "api"

                    [services.worker]
                    run.cmd = "worker"

                    [service_groups.backend]
                    members = ["api", "worker"]
                "#,
                expect_err: false,
                check: |config| {
                    assert!(config.validate(TEST_PLATFORM).is_ok());
                    let web = config.services["web"].resolve(TEST_PLATFORM);
                    let mut deps = config.effective_depends_on("web", &web.depends_on);
                    deps.sort();
                    assert_eq!(
                        deps,
                        vec![
                            Dependency::non_blocking("api"),
                            Dependency::non_blocking("worker")
                        ]
                    );
                },
            },
            ConfigTestCase {
                name: "a name reached both ways keeps the blocking edge",
                input: r#"
                    [services.web]
                    run.cmd = "web"
                    depends_on = [{ name = "backend", blocking = false }, "api"]

                    [services.api]
                    run.cmd = "api"

                    [services.worker]
                    run.cmd = "worker"

                    [service_groups.backend]
                    members = ["api", "worker"]
                "#,
                expect_err: false,
                check: |config| {
                    assert!(config.validate(TEST_PLATFORM).is_ok());
                    let web = config.services["web"].resolve(TEST_PLATFORM);
                    let mut deps = config.effective_depends_on("web", &web.depends_on);
                    deps.sort();
                    assert_eq!(
                        deps,
                        vec![
                            Dependency::blocking("api"),
                            Dependency::non_blocking("worker")
                        ]
                    );
                },
            },
            ConfigTestCase {
                name: "group with unknown depends_on target is a validation error",
                input: r#"
                    [services.web]
                    run.cmd = "web"

                    [service_groups.frontend]
                    members = ["web"]
                    depends_on = ["postgres"]
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(errors.iter().any(|e| {
                        e.contains("service group 'frontend'")
                            && e.contains(
                                "depends on unknown service, task, or service group 'postgres'",
                            )
                    }));
                },
            },
            ConfigTestCase {
                name: "group depends_on typo gets a suggestion",
                input: r#"
                    [services.postgres]
                    run.cmd = "postgres"

                    [services.web]
                    run.cmd = "web"

                    [service_groups.frontend]
                    members = ["web"]
                    depends_on = ["postgre"]
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(
                        errors
                            .iter()
                            .any(|e| e.contains("did you mean 'postgres'?"))
                    );
                },
            },
            ConfigTestCase {
                name: "cycle introduced by group depends_on is detected",
                input: r#"
                    [services.api]
                    run.cmd = "api"

                    [services.web]
                    run.cmd = "web"
                    depends_on = ["api"]

                    [service_groups.frontend]
                    members = ["api"]
                    depends_on = ["web"]
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(errors.iter().any(|e| e.contains("dependency cycle")));
                },
            },
            ConfigTestCase {
                name: "dependency cycle through service group is a validation error",
                input: r#"
                    [services.api]
                    run.cmd = "api"
                    depends_on = ["datastores"]

                    [services.postgres]
                    run.cmd = "postgres"
                    depends_on = ["api"]

                    [service_groups]
                    datastores = ["postgres"]
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(errors.iter().any(|e| e.contains("dependency cycle")));
                },
            },
            ConfigTestCase {
                name: "self-referencing dependency is a cycle",
                input: r#"
                    [services.loop]
                    run.cmd = "loop"
                    depends_on = ["loop"]
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(errors.iter().any(|e| e.contains("dependency cycle")));
                },
            },
            ConfigTestCase {
                name: "profiles with valid references",
                input: r#"
                    [services.api]
                    rust.binary = "api"

                    [services.postgres]
                    docker.image = "postgres:16"

                    [tasks.migrate]
                    cmd = "dbmate"
                    args = ["up"]

                    [profiles.frontend]
                    services = ["api", "postgres"]
                    tasks = ["migrate"]
                "#,
                expect_err: false,
                check: |config| {
                    assert!(config.validate(TEST_PLATFORM).is_ok());
                    let profile = &config.profiles["frontend"];
                    assert_eq!(profile.services, vec!["api", "postgres"]);
                    assert_eq!(profile.tasks, vec!["migrate"]);
                },
            },
            ConfigTestCase {
                name: "profile with unknown service is a validation error",
                input: r#"
                    [services.api]
                    rust.binary = "api"

                    [profiles.bad]
                    services = ["api", "nonexistent"]
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(errors[0].contains("unknown service or service group 'nonexistent'"));
                },
            },
            ConfigTestCase {
                name: "profile with unknown task is a validation error",
                input: r#"
                    [services.api]
                    rust.binary = "api"

                    [profiles.bad]
                    tasks = ["ghost"]
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(errors[0].contains("unknown task 'ghost'"));
                },
            },
            ConfigTestCase {
                name: "default_profile references a known profile",
                input: r#"
                    default_profile = "dev"

                    [services.api]
                    rust.binary = "api"

                    [profiles.dev]
                    services = ["api"]
                "#,
                expect_err: false,
                check: |config| {
                    assert!(config.validate(TEST_PLATFORM).is_ok());
                    assert_eq!(config.default_profile.as_deref(), Some("dev"));
                },
            },
            ConfigTestCase {
                name: "default_profile referencing unknown profile is a validation error",
                input: r#"
                    default_profile = "ghost"

                    [services.api]
                    rust.binary = "api"

                    [profiles.dev]
                    services = ["api"]
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(
                        errors
                            .iter()
                            .any(|e| e.contains("default_profile") && e.contains("ghost"))
                    );
                },
            },
            ConfigTestCase {
                name: "task with timeout",
                input: r#"
                    [tasks.slow]
                    cmd = "make"
                    args = ["build"]
                    timeout = "5m"
                "#,
                expect_err: false,
                check: |config| {
                    let task = &config.tasks["slow"];
                    assert_eq!(task.timeout.as_deref(), Some("5m"));
                },
            },
            ConfigTestCase {
                name: "empty config",
                input: "",
                expect_err: false,
                check: |config| {
                    assert!(config.services.is_empty());
                },
            },
            ConfigTestCase {
                name: "no preset is a validation error",
                input: r#"
                    [services.broken]
                    env = { FOO = "bar" }
                "#,
                expect_err: false,
                check: |config| {
                    assert!(config.validate(TEST_PLATFORM).is_err());
                },
            },
            ConfigTestCase {
                name: "conflicting presets is a parse error",
                input: r#"
                    [services.broken]
                    docker.image = "postgres:16"
                    run.cmd = "something"
                "#,
                expect_err: true,
                check: |_| {},
            },
            ConfigTestCase {
                name: "ready check with no check type is a validation error",
                input: r#"
                    [services.broken]
                    run.cmd = "myservice"
                    [services.broken.ready]
                    interval = "1s"
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(errors[0].contains("ready check must have one of"));
                },
            },
            ConfigTestCase {
                name: "ready check with multiple check types is a validation error",
                input: r#"
                    [services.broken]
                    run.cmd = "myservice"
                    [services.broken.ready]
                    tcp = "localhost:8080"
                    http = "http://localhost:8080/health"
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(errors[0].contains("ready check must have only one of"));
                },
            },
            ConfigTestCase {
                name: "invalid debounce duration is a validation error",
                input: r#"
                    [services.api]
                    run.cmd = "api"
                    debounce = "banana"
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(errors.iter().any(|e| e.contains("invalid debounce")));
                },
            },
            ConfigTestCase {
                name: "a task's invalid debounce duration is a validation error",
                input: r#"
                    [tasks.gen]
                    cmd = "gen"
                    watch = ["src/**"]
                    debounce = "banana"
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(errors.iter().any(|e| e.contains("invalid debounce")));
                },
            },
            ConfigTestCase {
                name: "a task's debounce is parsed and kept",
                input: r#"
                    [tasks.gen]
                    cmd = "gen"
                    watch = ["src/**"]
                    debounce = "1s"
                "#,
                expect_err: false,
                check: |config| {
                    config.validate(TEST_PLATFORM).unwrap();
                    assert_eq!(
                        config.tasks.get("gen").unwrap().debounce.as_deref(),
                        Some("1s")
                    );
                },
            },
            ConfigTestCase {
                name: "invalid ready interval is a validation error",
                input: r#"
                    [services.api]
                    run.cmd = "api"
                    [services.api.ready]
                    tcp = "localhost:3000"
                    interval = "nope"
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(errors.iter().any(|e| e.contains("invalid ready interval")));
                },
            },
            ConfigTestCase {
                name: "monitor + on_failure default to off / Notify",
                input: r#"
                    [services.api]
                    run.cmd = "api"
                    [services.api.ready]
                    http = "http://localhost:3000/health"
                "#,
                expect_err: false,
                check: |config| {
                    config.validate(TEST_PLATFORM).unwrap();
                    let resolved = config.services["api"].resolve(TEST_PLATFORM);
                    let ready = resolved.ready.as_ref().unwrap();
                    assert!(!ready.monitor);
                    assert_eq!(ready.unhealthy_after, 3);
                    assert_eq!(ready.monitor_interval, "10s");
                    assert_eq!(ready.timeout, "5s");
                    assert_eq!(resolved.on_failure, OnFailure::Notify);
                },
            },
            ConfigTestCase {
                name: "invalid ready timeout is a validation error",
                input: r#"
                    [services.api]
                    run.cmd = "api"
                    [services.api.ready]
                    http = "http://localhost:3000/health"
                    timeout = "eventually"
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(
                        errors.iter().any(|e| e.contains("invalid ready timeout")),
                        "got: {errors:?}"
                    );
                },
            },
            ConfigTestCase {
                name: "monitor + on_failure=restart parses cleanly",
                input: r#"
                    [services.api]
                    run.cmd = "api"
                    on_failure = "restart"
                    [services.api.ready]
                    http = "http://localhost:3000/health"
                    monitor = true
                    monitor_interval = "5s"
                    unhealthy_after = 2
                "#,
                expect_err: false,
                check: |config| {
                    config.validate(TEST_PLATFORM).unwrap();
                    let resolved = config.services["api"].resolve(TEST_PLATFORM);
                    let ready = resolved.ready.as_ref().unwrap();
                    assert!(ready.monitor);
                    assert_eq!(ready.monitor_interval.as_str(), "5s");
                    assert_eq!(ready.unhealthy_after, 2);
                    assert_eq!(resolved.on_failure, OnFailure::Restart);
                },
            },
            ConfigTestCase {
                name: "on_failure works without a ready check (crash-only restarts)",
                input: r#"
                    [services.api]
                    run.cmd = "api"
                    on_failure = "restart"
                "#,
                expect_err: false,
                check: |config| {
                    config.validate(TEST_PLATFORM).unwrap();
                    let resolved = config.services["api"].resolve(TEST_PLATFORM);
                    assert!(resolved.ready.is_none());
                    assert_eq!(resolved.on_failure, OnFailure::Restart);
                },
            },
            ConfigTestCase {
                name: "invalid monitor_interval is a validation error",
                input: r#"
                    [services.api]
                    run.cmd = "api"
                    [services.api.ready]
                    http = "http://localhost:3000/health"
                    monitor = true
                    monitor_interval = "soonish"
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(
                        errors
                            .iter()
                            .any(|e| e.contains("invalid ready monitor_interval")),
                        "got: {errors:?}"
                    );
                },
            },
            ConfigTestCase {
                name: "unhealthy_after = 0 with monitor enabled is a validation error",
                input: r#"
                    [services.api]
                    run.cmd = "api"
                    [services.api.ready]
                    http = "http://localhost:3000/health"
                    monitor = true
                    unhealthy_after = 0
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(
                        errors
                            .iter()
                            .any(|e| e.contains("unhealthy_after must be >= 1")),
                        "got: {errors:?}"
                    );
                },
            },
            ConfigTestCase {
                name: "invalid shutdown timeout is a validation error",
                input: r#"
                    [services.api]
                    run.cmd = "api"
                    [services.api.shutdown]
                    timeout = "forever"
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(
                        errors
                            .iter()
                            .any(|e| e.contains("invalid shutdown timeout"))
                    );
                },
            },
            ConfigTestCase {
                name: "invalid task timeout is a validation error",
                input: r#"
                    [tasks.build]
                    cmd = "make"
                    timeout = "lots"
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(errors.iter().any(|e| e.contains("invalid timeout")));
                },
            },
            ConfigTestCase {
                name: "invalid shutdown signal is a validation error",
                input: r#"
                    [services.api]
                    run.cmd = "api"
                    [services.api.shutdown]
                    signal = "SIGBANANA"
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(errors.iter().any(|e| e.contains("unknown shutdown signal")));
                },
            },
            ConfigTestCase {
                name: "valid shutdown signals pass validation",
                input: r#"
                    [services.api]
                    run.cmd = "api"
                    [services.api.shutdown]
                    signal = "SIGINT"
                "#,
                expect_err: false,
                check: |config| {
                    assert!(config.validate(TEST_PLATFORM).is_ok());
                },
            },
            ConfigTestCase {
                name: "reload defaults to true",
                input: r#"
                    [services.api]
                    run.cmd = "api"
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["api"].resolve(TEST_PLATFORM);
                    assert!(resolved.reload);
                },
            },
            ConfigTestCase {
                name: "reload = false disables file watching",
                input: r#"
                    [services.frontend]
                    bazel.target = "//frontend:frontend"
                    reload = false
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["frontend"].resolve(TEST_PLATFORM);
                    assert!(!resolved.reload);
                },
            },
            ConfigTestCase {
                name: "reload = false with platform override",
                input: r#"
                    [services.api]
                    run.cmd = "api"
                    reload = false

                    [services.api.platform.linux-x86_64]
                    reload = true
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["api"].resolve(Platform::LinuxX86_64);
                    assert!(
                        resolved.reload,
                        "platform override should set reload = true"
                    );

                    let resolved_mac = config.services["api"].resolve(Platform::MacosAarch64);
                    assert!(
                        !resolved_mac.reload,
                        "non-matching platform should keep reload = false"
                    );
                },
            },
            ConfigTestCase {
                name: "tty defaults to true",
                input: r#"
                    [services.api]
                    run.cmd = "api"
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["api"].resolve(TEST_PLATFORM);
                    assert!(resolved.tty);
                },
            },
            ConfigTestCase {
                name: "tty = false spawns without a controlling PTY",
                input: r#"
                    [services.data]
                    run.cmd = "data"
                    tty = false
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["data"].resolve(TEST_PLATFORM);
                    assert!(!resolved.tty);
                },
            },
            ConfigTestCase {
                name: "tty platform override",
                input: r#"
                    [services.api]
                    run.cmd = "api"
                    tty = false

                    [services.api.platform.linux-x86_64]
                    tty = true
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["api"].resolve(Platform::LinuxX86_64);
                    assert!(resolved.tty, "platform override should set tty = true");

                    let resolved_mac = config.services["api"].resolve(Platform::MacosAarch64);
                    assert!(
                        !resolved_mac.tty,
                        "non-matching platform should keep tty = false"
                    );
                },
            },
        ];

        for case in &cases {
            let result = case.input.parse::<Config>();
            if case.expect_err {
                assert!(
                    result.is_err(),
                    "case '{}': expected parse error",
                    case.name
                );
                continue;
            }
            let config =
                result.unwrap_or_else(|e| panic!("case '{}': unexpected error: {e}", case.name));
            (case.check)(&config);
        }
    }

    #[test]
    fn test_fallback_port_validation() {
        struct Case {
            name: &'static str,
            input: &'static str,
            expected_error: Option<&'static str>,
            expected_warning: Option<&'static str>,
        }

        let cases = vec![
            Case {
                name: "duplicate random proxy addresses are allowed",
                input: r#"
                    [services.api]
                    run.cmd = "api"
                    proxy = "127.0.0.1:0"

                    [services.web]
                    run.cmd = "web"
                    proxy = "127.0.0.1:0"
                "#,
                expected_error: None,
                expected_warning: None,
            },
            Case {
                name: "duplicate preferred proxy addresses remain invalid",
                input: r#"
                    fallback_ports = true

                    [services.api]
                    run.cmd = "api"
                    proxy = "127.0.0.1:3000"

                    [services.web]
                    run.cmd = "web"
                    proxy = "127.0.0.1:3000"
                "#,
                expected_error: Some("proxy listen address '127.0.0.1:3000'"),
                expected_warning: None,
            },
            Case {
                name: "docker port mappings validate with the config",
                input: r#"
                    [services.database]
                    docker.image = "postgres:16"
                    docker.ports = ["not-a-port-mapping"]
                "#,
                expected_error: Some("invalid port mapping 'not-a-port-mapping'"),
                expected_warning: None,
            },
            Case {
                name: "explicit docker container warns with fallback ports",
                input: r#"
                    fallback_ports = true

                    [services.database]
                    docker.image = "postgres:16"
                    docker.container = "shared-postgres"
                "#,
                expected_error: None,
                expected_warning: Some("explicit docker.container names can collide"),
            },
            Case {
                name: "explicit docker container does not warn by default",
                input: r#"
                    [services.database]
                    docker.image = "postgres:16"
                    docker.container = "shared-postgres"
                "#,
                expected_error: None,
                expected_warning: None,
            },
        ];

        for case in cases {
            let config: Config = case.input.parse().unwrap();
            match (config.validate(TEST_PLATFORM), case.expected_error) {
                (Err(ConfigError::Validation { errors }), Some(needle)) => assert!(
                    errors.iter().any(|error| error.contains(needle)),
                    "case '{}': expected error containing '{needle}', got {errors:?}",
                    case.name
                ),
                (Ok(warnings), None) => match case.expected_warning {
                    Some(needle) => assert!(
                        warnings.iter().any(|warning| warning.contains(needle)),
                        "case '{}': expected warning containing '{needle}', got {warnings:?}",
                        case.name
                    ),
                    None => assert!(
                        warnings.is_empty(),
                        "case '{}': expected no warnings, got {warnings:?}",
                        case.name
                    ),
                },
                (Err(error), None) => {
                    panic!("case '{}': expected valid config, got {error}", case.name)
                }
                (Err(error), Some(needle)) => panic!(
                    "case '{}': expected validation error containing '{needle}', got {error}",
                    case.name
                ),
                (Ok(_), Some(needle)) => panic!(
                    "case '{}': expected validation error containing '{needle}'",
                    case.name
                ),
            }
        }
    }

    #[test]
    fn test_resolved_run_cmd() {
        /// Expected executable path — either a literal (for no-download cases)
        /// or the archive's relative binary path (composed with the cache base,
        /// service name, and composite hash at test time).
        enum Expected {
            Literal(&'static str),
            FromDownload { relative_path: &'static str },
        }

        struct RunCmdTestCase {
            name: &'static str,
            input: &'static str,
            platform: Platform,
            cache_base: &'static str,
            expected: Expected,
            expect_args: &'static [&'static str],
        }

        let cases = vec![
            RunCmdTestCase {
                name: "no download uses cmd directly",
                input: r#"
                    [services.svc]
                    run.cmd = "mybin"
                    run.args = ["--port", "8080"]
                "#,
                platform: Platform::LinuxX86_64,
                cache_base: "/tmp/don-cache",
                expected: Expected::Literal("mybin"),
                expect_args: &["--port", "8080"],
            },
            RunCmdTestCase {
                name: "download with archive path resolves to cache",
                input: r#"
                    [services.svc]
                    run.cmd = "cockroach"
                    run.args = ["start-single-node", "--insecure"]

                    [services.svc.download.platform.linux-x86_64]
                    url = "https://example.com/cockroach-v24.tgz"
                    sha256 = "abc123"
                    path = "cockroach-v24/cockroach"
                "#,
                platform: Platform::LinuxX86_64,
                cache_base: "/tmp/don-cache",
                expected: Expected::FromDownload {
                    relative_path: "cockroach-v24/cockroach",
                },
                expect_args: &["start-single-node", "--insecure"],
            },
            RunCmdTestCase {
                name: "download without archive path uses url filename",
                input: r#"
                    [services.svc]
                    run.cmd = "mytool"
                    run.args = ["serve"]

                    [services.svc.download.platform.linux-x86_64]
                    url = "https://example.com/releases/mytool-linux-amd64"
                    sha256 = "def456"
                "#,
                platform: Platform::LinuxX86_64,
                cache_base: "/tmp/don-cache",
                expected: Expected::FromDownload {
                    relative_path: "mytool-linux-amd64",
                },
                expect_args: &["serve"],
            },
            RunCmdTestCase {
                name: "no download for this platform falls back to cmd",
                input: r#"
                    [services.svc]
                    run.cmd = "cockroach"
                    run.args = ["start"]

                    [services.svc.download.platform.linux-x86_64]
                    url = "https://example.com/cockroach-linux.tgz"
                    sha256 = "abc123"
                    path = "cockroach"
                "#,
                platform: Platform::MacosAarch64,
                cache_base: "/tmp/don-cache",
                expected: Expected::Literal("cockroach"),
                expect_args: &["start"],
            },
        ];

        for case in &cases {
            let config = case
                .input
                .parse::<Config>()
                .unwrap_or_else(|e| panic!("case '{}': parse error: {e}", case.name));
            let resolved = config.services["svc"].resolve(case.platform);
            let (executable, args) = resolved
                .resolved_run_cmd(
                    case.platform,
                    "svc",
                    Some(std::path::Path::new(case.cache_base)),
                )
                .unwrap_or_else(|e| panic!("case '{}': resolve error: {e}", case.name));

            let expected_exec = match &case.expected {
                Expected::Literal(s) => PathBuf::from(s),
                Expected::FromDownload { relative_path } => {
                    // Compute the expected path from the actual download config.
                    let artifact = resolved
                        .download
                        .as_ref()
                        .expect("download case must have download config")
                        .for_platform(case.platform)
                        .expect("download case must match platform");
                    PathBuf::from(case.cache_base)
                        .join("svc")
                        .join(artifact.composite_hash())
                        .join(relative_path)
                }
            };
            assert_eq!(
                executable, expected_exec,
                "case '{}': executable mismatch",
                case.name
            );
            let expected_args: Vec<String> =
                case.expect_args.iter().map(|s| s.to_string()).collect();
            assert_eq!(
                args,
                &expected_args[..],
                "case '{}': args mismatch",
                case.name
            );
        }
    }

    /// An upgraded `don.toml` that still configures a removed feature must
    /// say so by name. Unknown keys are otherwise ignored, which would leave
    /// the user with a generic "must have one of: ..." and no idea why a
    /// config that worked yesterday stopped.
    #[test]
    fn removed_keys_are_rejected_by_name() {
        struct Case {
            name: &'static str,
            input: &'static str,
            want_fragment: Option<&'static str>,
        }

        let cases = vec![
            Case {
                name: "turbo service",
                input: "[services.web]\nturbo.task = \"dev\"\n",
                want_fragment: Some("service 'web': 'turbo' is no longer supported"),
            },
            Case {
                name: "turbo task",
                input: "[tasks.build]\ncmd = \"true\"\nturbo.task = \"build\"\n",
                want_fragment: Some("task 'build': 'turbo' is no longer supported"),
            },
            Case {
                name: "the key that replaced terminal parses",
                input: "[tasks.console]\ncmd = \"true\"\ninteractive = true\n",
                want_fragment: None,
            },
            Case {
                name: "and so does terminal itself",
                input: "[tasks.console]\ncmd = \"true\"\nterminal = \"foreground\"\n",
                want_fragment: None,
            },
            Case {
                name: "a terminal value that never existed is still an error",
                input: "[tasks.console]\ncmd = \"true\"\nterminal = \"tmux\"\n",
                want_fragment: Some("expected \"muxed\" or \"foreground\""),
            },
            Case {
                name: "bazel is untouched",
                input: "[services.api]\nbazel.target = \"//api:api\"\n",
                want_fragment: None,
            },
        ];

        for case in cases {
            let parsed = case.input.parse::<Config>();
            match case.want_fragment {
                Some(fragment) => {
                    let err = parsed.expect_err(case.name).to_string();
                    assert!(
                        err.contains(fragment),
                        "{}: expected {fragment:?} in {err:?}",
                        case.name
                    );
                }
                None => {
                    assert!(parsed.is_ok(), "{}: should still parse", case.name);
                }
            }
        }
    }

    #[test]
    fn test_parse_bazel_config() {
        let toml = r#"
[services.api]
bazel.target = "//services/api:api"
"#;
        let config: Config = toml.parse().unwrap();
        let svc = config.services.get("api").unwrap();
        let Some(ServiceKind::Bazel(bazel)) = &svc.kind else {
            panic!("expected bazel kind");
        };
        assert_eq!(bazel.target, "//services/api:api");
        assert!(bazel.watch);
    }

    #[test]
    fn test_parse_bazel_watch_false() {
        let toml = r#"
[services.api]
bazel.target = "//services/api:api"
bazel.watch = false
"#;
        let config: Config = toml.parse().unwrap();
        let svc = config.services.get("api").unwrap();
        let Some(ServiceKind::Bazel(bazel)) = &svc.kind else {
            panic!("expected bazel kind");
        };
        assert_eq!(bazel.target, "//services/api:api");
        assert!(!bazel.watch);
    }

    #[test]
    fn test_parse_task_with_bazel() {
        let toml = r#"
[tasks.codegen]
cmd = "bazel"
args = ["build", "//tools/codegen:all"]
bazel.target = "//tools/codegen:all"
"#;
        let config: Config = toml.parse().unwrap();
        let task = config.tasks.get("codegen").unwrap();
        assert_eq!(task.bazel.as_ref().unwrap().target, "//tools/codegen:all");
    }

    #[test]
    fn test_parse_rejects_multiple_service_kinds() {
        // Multiple service kinds are rejected at parse time, not validation.
        let toml = r#"
[services.api]
run.cmd = "./api"
bazel.target = "//services/api:api"
"#;
        let result: Result<Config, _> = toml.parse();
        assert!(
            result.is_err(),
            "expected parse error for conflicting kinds"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("only one of"),
            "expected mutual exclusivity error, got: {err}"
        );
    }

    #[test]
    fn test_build_tool_config_platform_override() {
        let toml = r#"
[services.api]
bazel.target = "//services/api:linux"

[services.api.platform.macos-aarch64]
bazel.target = "//services/api:macos_arm64"
"#;
        let config: Config = toml.parse().unwrap();
        let svc = config.services.get("api").unwrap();

        // Base config
        let resolved = svc.resolve(Platform::LinuxX86_64);
        let Some(ServiceKind::Bazel(bazel)) = &resolved.kind else {
            panic!("expected bazel kind");
        };
        assert_eq!(bazel.target, "//services/api:linux");

        // Platform override
        let resolved_mac = svc.resolve(Platform::MacosAarch64);
        let Some(ServiceKind::Bazel(bazel_mac)) = &resolved_mac.kind else {
            panic!("expected bazel kind on macos");
        };
        assert_eq!(bazel_mac.target, "//services/api:macos_arm64");
    }

    #[test]
    fn validate_task_params_table() {
        struct Case {
            name: &'static str,
            toml: &'static str,
            want: Option<&'static str>, // substring that must appear in some error
        }

        let cases = vec![
            Case {
                name: "no params is fine",
                toml: r#"
                    [tasks.t]
                    cmd = "echo"
                "#,
                want: None,
            },
            Case {
                name: "placeholder referencing declared param is fine",
                toml: r#"
                    [tasks.t]
                    cmd = "echo"
                    args = ["{{name}}"]
                    [[tasks.t.params]]
                    name = "name"
                "#,
                want: None,
            },
            Case {
                name: "headless placeholder referencing declared param is fine",
                toml: r#"
                    [tasks.t]
                    cmd = "echo"
                    headless = { args = ["{{name}}"] }
                    [[tasks.t.params]]
                    name = "name"
                "#,
                want: None,
            },
            Case {
                name: "headless placeholder referencing unknown param errors",
                toml: r#"
                    [tasks.t]
                    cmd = "echo"
                    headless = { cmd = "{{nme}}" }
                    [[tasks.t.params]]
                    name = "name"
                "#,
                want: Some("headless.cmd references undeclared param"),
            },
            Case {
                name: "placeholder referencing unknown param errors with suggestion",
                toml: r#"
                    [tasks.t]
                    cmd = "echo"
                    args = ["{{nme}}"]
                    [[tasks.t.params]]
                    name = "name"
                "#,
                want: Some("did you mean 'name'"),
            },
            Case {
                name: "reserved name collides",
                toml: r#"
                    [tasks.t]
                    cmd = "echo"
                    [[tasks.t.params]]
                    name = "config"
                "#,
                want: Some("collides with a built-in"),
            },
            Case {
                name: "choices and completions are mutually exclusive",
                toml: r#"
                    [tasks.t]
                    cmd = "echo"
                    [[tasks.t.params]]
                    name = "x"
                    choices = ["a", "b"]
                    [tasks.t.params.completions]
                    cmd = "ls"
                "#,
                want: Some("pick one"),
            },
            Case {
                name: "int validate min > max",
                toml: r#"
                    [tasks.t]
                    cmd = "echo"
                    [[tasks.t.params]]
                    name = "n"
                    kind = "int"
                    validate = { min = 10, max = 1 }
                "#,
                want: Some("min (10)"),
            },
            Case {
                name: "validate requires int kind",
                toml: r#"
                    [tasks.t]
                    cmd = "echo"
                    [[tasks.t.params]]
                    name = "s"
                    validate = { min = 1 }
                "#,
                want: Some("'validate' but kind is not 'int'"),
            },
            Case {
                name: "choices on bool is rejected",
                toml: r#"
                    [tasks.t]
                    cmd = "echo"
                    [[tasks.t.params]]
                    name = "b"
                    kind = "bool"
                    choices = ["a"]
                "#,
                want: Some("'choices' but kind is not"),
            },
            Case {
                name: "bad bool default",
                toml: r#"
                    [tasks.t]
                    cmd = "echo"
                    [[tasks.t.params]]
                    name = "b"
                    kind = "bool"
                    default = "maybe"
                "#,
                want: Some("must be 'true' or 'false'"),
            },
            Case {
                name: "bad int default",
                toml: r#"
                    [tasks.t]
                    cmd = "echo"
                    [[tasks.t.params]]
                    name = "n"
                    kind = "int"
                    default = "abc"
                "#,
                want: Some("not a valid integer"),
            },
            Case {
                name: "int default outside range",
                toml: r#"
                    [tasks.t]
                    cmd = "echo"
                    [[tasks.t.params]]
                    name = "n"
                    kind = "int"
                    default = "99"
                    validate = { max = 10 }
                "#,
                want: Some("greater than max"),
            },
            Case {
                name: "choice default must be among choices",
                toml: r#"
                    [tasks.t]
                    cmd = "echo"
                    [[tasks.t.params]]
                    name = "c"
                    kind = "choice"
                    choices = ["a", "b"]
                    default = "z"
                "#,
                want: Some("is not among choices"),
            },
            Case {
                name: "duplicate param names",
                toml: r#"
                    [tasks.t]
                    cmd = "echo"
                    [[tasks.t.params]]
                    name = "x"
                    [[tasks.t.params]]
                    name = "x"
                "#,
                want: Some("declared more than once"),
            },
            Case {
                name: "invalid identifier",
                toml: r#"
                    [tasks.t]
                    cmd = "echo"
                    [[tasks.t.params]]
                    name = "1bad"
                "#,
                want: Some("invalid name"),
            },
            Case {
                name: "bad cache duration",
                toml: r#"
                    [tasks.t]
                    cmd = "echo"
                    [[tasks.t.params]]
                    name = "x"
                    [tasks.t.params.completions]
                    cmd = "ls"
                    cache = "forever"
                "#,
                want: Some("completions.cache"),
            },
            Case {
                name: "placeholder in env resolves",
                toml: r#"
                    [tasks.t]
                    cmd = "echo"
                    env = { X = "{{val}}" }
                    [[tasks.t.params]]
                    name = "val"
                "#,
                want: None,
            },
            Case {
                name: "placeholder in dir resolves",
                toml: r#"
                    [tasks.t]
                    cmd = "echo"
                    dir = "{{where}}"
                    [[tasks.t.params]]
                    name = "where"
                "#,
                want: None,
            },
        ];

        for case in cases {
            let config: Config = case.toml.parse().unwrap();
            let res = config.validate(TEST_PLATFORM);
            match (&res, case.want) {
                (Ok(_), None) => {}
                (Ok(_), Some(needle)) => {
                    panic!(
                        "case '{}': expected error containing '{needle}' but got Ok",
                        case.name
                    )
                }
                (Err(ConfigError::Validation { errors }), Some(needle)) => {
                    assert!(
                        errors.iter().any(|e| e.contains(needle)),
                        "case '{}': no error contains '{needle}' — got {errors:?}",
                        case.name,
                    );
                }
                (Err(ConfigError::Validation { errors }), None) => {
                    panic!(
                        "case '{}': expected Ok but got validation errors {errors:?}",
                        case.name
                    )
                }
                (Err(e), _) => panic!("case '{}': unexpected error kind {e}", case.name),
            }
        }
    }

    /// A task needs something to run, and `bazel.target` counts.
    #[test]
    fn a_task_needs_a_cmd_or_a_bazel_target() {
        struct Case {
            name: &'static str,
            toml: &'static str,
            want_error: bool,
        }

        let cases = vec![
            Case {
                name: "a plain command",
                toml: "[tasks.t]\ncmd = \"true\"\n",
                want_error: false,
            },
            Case {
                name: "defined by its bazel target alone",
                toml: "[tasks.t]\nbazel.target = \"//a:b\"\n",
                want_error: false,
            },
            Case {
                name: "both — the target is then watch-only",
                toml: "[tasks.t]\ncmd = \"make\"\nbazel.target = \"//a:b\"\n",
                want_error: false,
            },
            Case {
                name: "neither: nothing to run",
                toml: "[tasks.t]\nwatch = [\"src/**\"]\n",
                want_error: true,
            },
        ];

        for case in cases {
            let config: Config = case.toml.parse().unwrap();
            let result = config.validate(Platform::LinuxX86_64);
            match (result, case.want_error) {
                (Err(ConfigError::Validation { errors }), true) => assert!(
                    errors
                        .iter()
                        .any(|e| e.contains("must set `cmd` or `bazel.target`")),
                    "{}: wrong error {errors:?}",
                    case.name
                ),
                (Ok(_), false) => {}
                (other, _) => panic!("{}: unexpected {other:?}", case.name),
            }
        }
    }

    /// `log_exclude` parses at both levels and survives a platform override,
    /// exactly as `log_filter` does — the two are the same shape and must not
    /// drift apart.
    #[test]
    fn log_exclude_parses_globally_and_per_service() {
        struct Case {
            name: &'static str,
            service: &'static str,
            want_filter: Vec<&'static str>,
            want_exclude: Vec<&'static str>,
        }

        let config: Config = r#"
            log_filter = ["ERROR"]
            log_exclude = ["/health"]

            [services.api]
            run.cmd = "true"
            log_exclude = ["favicon"]

            [services.worker]
            run.cmd = "true"
            log_filter = ["^job "]

            [services.plain]
            run.cmd = "true"
        "#
        .parse()
        .unwrap();

        assert_eq!(config.log_filter.patterns, vec!["ERROR".to_string()]);
        assert_eq!(config.log_exclude.patterns, vec!["/health".to_string()]);

        let cases = vec![
            Case {
                name: "a service's own exclude sits beside the global one",
                service: "api",
                want_filter: vec![],
                want_exclude: vec!["favicon"],
            },
            Case {
                name: "a keep list and no exclude",
                service: "worker",
                want_filter: vec!["^job "],
                want_exclude: vec![],
            },
            Case {
                name: "neither, so the global lists stand alone",
                service: "plain",
                want_filter: vec![],
                want_exclude: vec![],
            },
        ];

        for case in cases {
            let resolved = config.services[case.service].resolve(Platform::LinuxX86_64);
            assert_eq!(
                resolved.log_filter.patterns, case.want_filter,
                "{}: log_filter",
                case.name
            );
            assert_eq!(
                resolved.log_exclude.patterns, case.want_exclude,
                "{}: log_exclude",
                case.name
            );
        }

        // The merge the runner actually applies: global first, service on top.
        let api = config.services["api"].resolve(Platform::LinuxX86_64);
        assert_eq!(
            config.log_exclude.merged_with(&api.log_exclude).patterns,
            vec!["/health".to_string(), "favicon".to_string()],
        );
    }

    /// A bad pattern is a config error naming the key it came from, so the
    /// message points at the line to fix rather than just "invalid regex".
    #[test]
    fn an_invalid_log_exclude_regex_fails_validation() {
        let config: Config = r#"
            [services.api]
            run.cmd = "true"
            log_exclude = ["("]
        "#
        .parse()
        .unwrap();

        let Err(ConfigError::Validation { errors }) = config.validate(Platform::LinuxX86_64) else {
            panic!("expected a validation error");
        };
        assert!(
            errors
                .iter()
                .any(|e| e.contains("service 'api' log_exclude") && e.contains("invalid regex")),
            "errors: {errors:?}"
        );
    }

    #[test]
    fn auto_filter_on_failure_defaults_and_overrides_parse() {
        let config: Config = r#"
            auto_filter_on_failure = false

            [services.api]
            run.cmd = "true"
            auto_filter_on_failure = true

            [services.worker]
            run.cmd = "true"

            [tasks.lint]
            cmd = "true"
            auto_filter_on_failure = true

            [tasks.test]
            cmd = "true"
        "#
        .parse()
        .unwrap();

        assert!(!config.auto_filter_on_failure);
        assert_eq!(config.services["api"].auto_filter_on_failure, Some(true));
        assert_eq!(config.services["worker"].auto_filter_on_failure, None);
        assert_eq!(config.tasks["lint"].auto_filter_on_failure, Some(true));
        assert_eq!(config.tasks["test"].auto_filter_on_failure, None);
    }

    #[test]
    fn auto_filter_on_failure_defaults_to_enabled() {
        let config: Config = r#"
            [services.api]
            run.cmd = "true"

            [tasks.lint]
            cmd = "true"
        "#
        .parse()
        .unwrap();

        assert!(config.auto_filter_on_failure);
        assert_eq!(config.services["api"].auto_filter_on_failure, None);
        assert_eq!(config.tasks["lint"].auto_filter_on_failure, None);
    }
}
