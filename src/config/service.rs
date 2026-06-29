use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

use super::download::{DownloadConfig, default_cache_base};

fn default_true() -> bool {
    true
}
use super::platform::Platform;
use super::types::{
    BazelConfig, Command, LogConfig, LogFilterConfig, NotifyConfig, OnFailure, ProxyEntry,
    ReadyCheck, ShutdownConfig, TurboConfig, deserialize_proxy, deserialize_proxy_option,
};

/// The kind of service — exactly one of these must be set.
///
/// Replaces the old set of mutually-exclusive optional fields (docker, rust, go, run, bazel, turbo).
/// The compiler now enforces that a service has exactly one kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServiceKind {
    /// Bazel build tool integration — auto-resolve watch patterns from the build graph.
    Bazel(BazelConfig),
    /// Turborepo build tool integration — auto-resolve watch patterns from the task graph.
    Turbo(TurboConfig),
    /// Docker container configuration.
    Docker(DockerConfig),
    /// Rust/Cargo service configuration.
    Rust(RustConfig),
    /// Go service configuration.
    Go(GoConfig),
    /// Custom service: a run command and an optional build command.
    Custom {
        run: Command,
        build: Option<Command>,
    },
}

/// A long-running service. Uses exactly one kind: bazel, turbo, docker, rust, go, or custom (run).
#[derive(Debug, Clone, PartialEq)]
pub struct Service {
    /// Working directory for the service. Defaults to the current directory.
    pub dir: Option<PathBuf>,
    /// Environment variables. No env vars are loaded by default.
    pub env: HashMap<String, String>,
    /// Paths to env files to load. Don also auto-loads `.env.<service-name>` if it exists.
    pub env_file: Vec<PathBuf>,
    /// File glob patterns to watch for rebuilding/restarting.
    pub watch: Vec<String>,
    /// File glob patterns to ignore when watching (e.g. "**/*.log", "target/**").
    pub ignore: Vec<String>,
    /// Debounce window for file watch events (e.g. "500ms" or "1s"). Defaults to "200ms".
    pub debounce: Option<String>,
    /// Services that must be started before this one.
    pub depends_on: Vec<String>,
    /// Proxy entries: Don binds each entry's `listen` address and either
    /// forwards to an ephemeral backend port (env mode) or hands the bound
    /// listener to the service via `LISTEN_FDS` (listenfd mode). Don holds
    /// the listeners across restarts so traffic is never dropped.
    pub proxy: Vec<ProxyEntry>,
    /// If true, don't start the service until the first connection arrives on
    /// a proxy address. Requires at least one proxy entry.
    pub lazy: bool,
    /// Optional binary download configuration for this service.
    pub download: Option<DownloadConfig>,
    /// Ready check — used to gate dependents until this service is accepting traffic.
    pub ready: Option<ReadyCheck>,
    /// Shutdown behavior.
    pub shutdown: Option<ShutdownConfig>,
    /// Where to send stdout/stderr. Defaults to stdout.
    pub log: LogConfig,
    /// Regex-based service log filter. When set, only matching output lines are kept.
    pub log_filter: LogFilterConfig,
    /// Whether don should watch files and rebuild/restart this service on changes.
    /// Defaults to `true`. Set to `false` for services that handle their own
    /// hot-reloading internally (e.g. vite, webpack dev server).
    pub reload: bool,
    /// Whether to give the service a controlling PTY. Defaults to `true`.
    /// Set to `false` to spawn with plain pipes and no controlling terminal —
    /// required for processes that launch background-process-group children
    /// which touch the tty (e.g. installers/JVMs doing `tcsetattr`), which
    /// would otherwise be `SIGTTOU`/`SIGTTIN`-suspended under a PTY. Disables
    /// interactive `don attach` for this service.
    pub tty: bool,
    /// What to do when this service fails — either marked `Unhealthy` by the
    /// health monitor or exits with a non-zero status. Defaults to `Notify`.
    /// `Restart` reuses the same backoff machinery for both kinds of failure.
    pub on_failure: OnFailure,
    /// Per-platform overrides. If the current platform has an entry here,
    /// its fields are merged on top of the base service config.
    pub platform: HashMap<Platform, ServiceOverride>,
    /// Whether this service's log output is hidden by default in the TUI
    /// filter. Users can still unhide it interactively from the filter view.
    /// Defaults to `false` (visible).
    pub hidden: bool,
    /// Override the top-level `auto_filter_on_failure` setting for this
    /// service. When enabled, a service failure adds this service to the TUI
    /// log filter.
    pub auto_filter_on_failure: Option<bool>,
    /// Per-service desktop-notification policy, layered over the global
    /// `[notify]` table. Not platform-overridable.
    pub notify: NotifyConfig,

    /// The service kind. `None` when the base service has no preset
    /// and relies on a platform override to supply one.
    pub kind: Option<ServiceKind>,
}

/// Intermediate struct for TOML deserialization. Has the flat optional fields
/// that get converted into a `ServiceKind` discriminant.
#[derive(Deserialize)]
struct RawService {
    dir: Option<PathBuf>,
    #[serde(default)]
    env: HashMap<String, String>,
    #[serde(default)]
    env_file: Vec<PathBuf>,
    #[serde(default)]
    watch: Vec<String>,
    #[serde(default)]
    ignore: Vec<String>,
    debounce: Option<String>,
    #[serde(default)]
    depends_on: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_proxy")]
    proxy: Vec<ProxyEntry>,
    #[serde(default)]
    lazy: bool,
    download: Option<DownloadConfig>,
    ready: Option<ReadyCheck>,
    shutdown: Option<ShutdownConfig>,
    #[serde(default)]
    log: LogConfig,
    #[serde(default)]
    log_filter: LogFilterConfig,
    #[serde(default = "default_true")]
    reload: bool,
    #[serde(default = "default_true")]
    tty: bool,
    #[serde(default)]
    on_failure: OnFailure,
    #[serde(default)]
    platform: HashMap<Platform, ServiceOverride>,
    #[serde(default)]
    hidden: bool,
    #[serde(default)]
    auto_filter_on_failure: Option<bool>,
    #[serde(default)]
    notify: NotifyConfig,

    bazel: Option<BazelConfig>,
    turbo: Option<TurboConfig>,
    docker: Option<DockerConfig>,
    rust: Option<RustConfig>,
    go: Option<GoConfig>,
    run: Option<Command>,
    build: Option<Command>,
}

fn raw_fields_to_kind(
    bazel: Option<BazelConfig>,
    turbo: Option<TurboConfig>,
    docker: Option<DockerConfig>,
    rust: Option<RustConfig>,
    go: Option<GoConfig>,
    run: Option<Command>,
    build: Option<Command>,
) -> Result<Option<ServiceKind>, String> {
    let count = bazel.is_some() as u8
        + turbo.is_some() as u8
        + docker.is_some() as u8
        + rust.is_some() as u8
        + go.is_some() as u8
        + run.is_some() as u8;

    if count > 1 {
        return Err(
            "service must have only one of: bazel, turbo, docker, rust, go, or run".to_string(),
        );
    }

    if count == 0 {
        if build.is_some() {
            return Err("'build' requires 'run' to be set".to_string());
        }
        return Ok(None);
    }

    if let Some(bazel) = bazel {
        Ok(Some(ServiceKind::Bazel(bazel)))
    } else if let Some(turbo) = turbo {
        Ok(Some(ServiceKind::Turbo(turbo)))
    } else if let Some(docker) = docker {
        Ok(Some(ServiceKind::Docker(docker)))
    } else if let Some(rust) = rust {
        Ok(Some(ServiceKind::Rust(rust)))
    } else if let Some(go) = go {
        Ok(Some(ServiceKind::Go(go)))
    } else if let Some(run) = run {
        Ok(Some(ServiceKind::Custom { run, build }))
    } else {
        Ok(None)
    }
}

impl TryFrom<RawService> for Service {
    type Error = String;

    fn try_from(raw: RawService) -> Result<Self, String> {
        let kind = raw_fields_to_kind(
            raw.bazel, raw.turbo, raw.docker, raw.rust, raw.go, raw.run, raw.build,
        )?;

        Ok(Service {
            dir: raw.dir,
            env: raw.env,
            env_file: raw.env_file,
            watch: raw.watch,
            ignore: raw.ignore,
            debounce: raw.debounce,
            depends_on: raw.depends_on,
            proxy: raw.proxy,
            lazy: raw.lazy,
            download: raw.download,
            ready: raw.ready,
            shutdown: raw.shutdown,
            log: raw.log,
            log_filter: raw.log_filter,
            reload: raw.reload,
            tty: raw.tty,
            on_failure: raw.on_failure,
            platform: raw.platform,
            hidden: raw.hidden,
            auto_filter_on_failure: raw.auto_filter_on_failure,
            notify: raw.notify,
            kind,
        })
    }
}

impl<'de> Deserialize<'de> for Service {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = RawService::deserialize(deserializer)?;
        Service::try_from(raw).map_err(serde::de::Error::custom)
    }
}

/// Platform-specific overrides for a service. Any field set here replaces the
/// corresponding base field. For `env`, entries are merged (override wins on conflict).
/// If a kind field (docker/rust/run/etc.) is set, it completely replaces the base kind.
#[derive(Debug, Clone, PartialEq)]
pub struct ServiceOverride {
    pub dir: Option<PathBuf>,
    pub env: HashMap<String, String>,
    pub env_file: Option<Vec<PathBuf>>,
    pub watch: Option<Vec<String>>,
    pub ignore: Option<Vec<String>>,
    pub debounce: Option<String>,
    pub depends_on: Option<Vec<String>>,
    pub proxy: Option<Vec<ProxyEntry>>,
    pub lazy: Option<bool>,
    pub download: Option<DownloadConfig>,
    pub ready: Option<ReadyCheck>,
    pub shutdown: Option<ShutdownConfig>,
    pub log: Option<LogConfig>,
    pub log_filter: Option<LogFilterConfig>,
    pub reload: Option<bool>,
    pub tty: Option<bool>,
    pub on_failure: Option<OnFailure>,
    pub auto_filter_on_failure: Option<bool>,

    /// If set, completely replaces the base service kind.
    pub kind: Option<ServiceKind>,
}

/// Intermediate struct for TOML deserialization of ServiceOverride.
#[derive(Deserialize)]
struct RawServiceOverride {
    dir: Option<PathBuf>,
    #[serde(default)]
    env: HashMap<String, String>,
    env_file: Option<Vec<PathBuf>>,
    watch: Option<Vec<String>>,
    ignore: Option<Vec<String>>,
    debounce: Option<String>,
    depends_on: Option<Vec<String>>,
    #[serde(default, deserialize_with = "deserialize_proxy_option")]
    proxy: Option<Vec<ProxyEntry>>,
    lazy: Option<bool>,
    download: Option<DownloadConfig>,
    ready: Option<ReadyCheck>,
    shutdown: Option<ShutdownConfig>,
    log: Option<LogConfig>,
    log_filter: Option<LogFilterConfig>,
    reload: Option<bool>,
    tty: Option<bool>,
    on_failure: Option<OnFailure>,
    auto_filter_on_failure: Option<bool>,

    bazel: Option<BazelConfig>,
    turbo: Option<TurboConfig>,
    docker: Option<DockerConfig>,
    rust: Option<RustConfig>,
    go: Option<GoConfig>,
    run: Option<Command>,
    build: Option<Command>,
}

impl TryFrom<RawServiceOverride> for ServiceOverride {
    type Error = String;

    fn try_from(raw: RawServiceOverride) -> Result<Self, String> {
        let kind = raw_fields_to_kind(
            raw.bazel, raw.turbo, raw.docker, raw.rust, raw.go, raw.run, raw.build,
        )?;

        Ok(ServiceOverride {
            dir: raw.dir,
            env: raw.env,
            env_file: raw.env_file,
            watch: raw.watch,
            ignore: raw.ignore,
            debounce: raw.debounce,
            depends_on: raw.depends_on,
            proxy: raw.proxy,
            lazy: raw.lazy,
            download: raw.download,
            ready: raw.ready,
            shutdown: raw.shutdown,
            log: raw.log,
            log_filter: raw.log_filter,
            reload: raw.reload,
            tty: raw.tty,
            on_failure: raw.on_failure,
            auto_filter_on_failure: raw.auto_filter_on_failure,
            kind,
        })
    }
}

impl<'de> Deserialize<'de> for ServiceOverride {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = RawServiceOverride::deserialize(deserializer)?;
        ServiceOverride::try_from(raw).map_err(serde::de::Error::custom)
    }
}

/// A fully resolved service with platform overrides applied.
#[derive(Debug, Clone)]
pub struct ResolvedService {
    pub dir: Option<PathBuf>,
    pub env: HashMap<String, String>,
    pub env_file: Vec<PathBuf>,
    pub watch: Vec<String>,
    pub ignore: Vec<String>,
    pub debounce: Option<String>,
    pub depends_on: Vec<String>,
    pub proxy: Vec<ProxyEntry>,
    pub lazy: bool,
    pub download: Option<DownloadConfig>,
    pub ready: Option<ReadyCheck>,
    pub shutdown: Option<ShutdownConfig>,
    pub log: LogConfig,
    pub log_filter: LogFilterConfig,
    /// Whether don should watch files and rebuild/restart this service on changes.
    pub reload: bool,
    /// Whether to give the service a controlling PTY (vs plain pipes). `false`
    /// spawns without a controlling terminal — see [`Service::tty`].
    pub tty: bool,
    /// What to do when this service fails (Unhealthy or non-zero crash).
    pub on_failure: OnFailure,
    /// Optional per-service override for automatic log-filter selection on
    /// failure.
    pub auto_filter_on_failure: Option<bool>,
    /// Desktop-notification policy (global `[notify]` is layered in at the
    /// transition site, not here).
    pub notify: NotifyConfig,

    /// The resolved service kind. `None` only if validation hasn't caught
    /// a missing preset (shouldn't happen after validation).
    pub kind: Option<ServiceKind>,

    /// For Bazel services: the absolute path to the target's built binary,
    /// resolved via `bazel cquery --output=files` after the startup batch
    /// build. When `Some`, the spawn path launches this binary directly
    /// instead of going through `bazel run`, which skips bazel's per-launch
    /// analysis overhead. `None` means the binary path wasn't resolved yet
    /// (e.g. lazy service pre-first-connection) and the spawn path falls
    /// back to `bazel run <target>`.
    ///
    /// The kind is NOT rewritten to `Custom` when this is set — leaving it
    /// as `Bazel` keeps `is_build_tool_managed()` truthful so file-watch
    /// rebuilds correctly route through the batch-build path and actually
    /// re-invoke `bazel build` on source changes.
    pub resolved_binary_path: Option<String>,
}

/// Docker container configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct DockerConfig {
    /// Docker image to run (e.g. "postgres:16").
    /// For built images, this is also used as the tag for `docker build -t`.
    pub image: String,
    /// Container name — used to check if it's already running.
    pub container: Option<String>,
    /// Port mappings (e.g. ["5432:5432"]).
    #[serde(default)]
    pub ports: Vec<String>,
    /// Volume mounts (e.g. ["pgdata:/var/lib/postgresql/data"]).
    #[serde(default)]
    pub volumes: Vec<String>,
    /// Docker network to attach to.
    pub network: Option<String>,
    /// Override the container's default command / entrypoint args.
    #[serde(default)]
    pub command: Vec<String>,
    /// Env files to pass to docker via --env-file.
    #[serde(default)]
    pub env_file: Vec<PathBuf>,
    /// Build configuration — if set, don builds the image before running.
    pub build: Option<DockerBuildConfig>,
}

/// Docker image build configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct DockerBuildConfig {
    /// Build context path (e.g. "." or "./services/api").
    pub context: String,
    /// Path to the Dockerfile, relative to context. Defaults to "Dockerfile".
    pub dockerfile: Option<String>,
    /// Build target for multi-stage builds (e.g. "development").
    pub target: Option<String>,
    /// Build arguments passed via --build-arg.
    #[serde(default)]
    pub args: HashMap<String, String>,
}

/// Rust/Cargo service configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RustConfig {
    /// Name of the binary target to build and run.
    pub binary: String,
    /// Cargo features to enable.
    #[serde(default)]
    pub features: Vec<String>,
    /// Build in release mode (default: false).
    #[serde(default)]
    pub release: bool,
    /// Extra arguments to pass to `cargo build`.
    #[serde(default)]
    pub extra_args: Vec<String>,
    /// Override the cargo target directory.
    pub target_dir: Option<PathBuf>,
}

/// Go service configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct GoConfig {
    /// Go package path to build (e.g. "./cmd/api").
    pub package: String,
    /// Output binary name. Defaults to the last component of the package path.
    pub output: Option<String>,
    /// Extra flags to pass to `go build` (e.g. ["-race"]).
    #[serde(default)]
    pub build_flags: Vec<String>,
    /// Linker flags passed via `-ldflags` (e.g. "-X main.version=1.0").
    pub ldflags: Option<String>,
}

impl Service {
    /// Resolve the service for a specific platform, applying overrides if present.
    /// For Bazel services with a known binary path, use `resolve_with_bazel_binary` instead.
    pub fn resolve(&self, platform: Platform) -> ResolvedService {
        self.resolve_inner(platform)
    }

    /// Resolve with a known Bazel binary path (from `bazel cquery`).
    /// Sets the kind to `Custom` with the binary path as the run command.
    pub fn resolve_with_bazel_binary(
        &self,
        platform: Platform,
        bazel_binary: &str,
    ) -> ResolvedService {
        let mut resolved = self.resolve_inner(platform);
        // Only attach the binary path for Bazel services — for any other
        // kind this accessor is a no-op (shouldn't be called, but defensive).
        if matches!(resolved.kind, Some(ServiceKind::Bazel(_))) {
            resolved.resolved_binary_path = Some(bazel_binary.to_string());
        }
        resolved
    }

    fn resolve_inner(&self, platform: Platform) -> ResolvedService {
        match self.platform.get(&platform) {
            None => ResolvedService {
                dir: self.dir.clone(),
                env: self.env.clone(),
                env_file: self.env_file.clone(),
                watch: self.watch.clone(),
                ignore: self.ignore.clone(),
                debounce: self.debounce.clone(),
                depends_on: self.depends_on.clone(),
                proxy: self.proxy.clone(),
                lazy: self.lazy,
                download: self.download.clone(),
                ready: self.ready.clone(),
                shutdown: self.shutdown.clone(),
                log: self.log.clone(),
                log_filter: self.log_filter.clone(),
                reload: self.reload,
                tty: self.tty,
                on_failure: self.on_failure,
                auto_filter_on_failure: self.auto_filter_on_failure,
                notify: self.notify,
                kind: self.kind.clone(),
                resolved_binary_path: None,
            },
            Some(ov) => {
                let mut env = self.env.clone();
                env.extend(ov.env.clone());

                // If the override has a kind, use it; otherwise keep the base kind.
                let kind = if ov.kind.is_some() {
                    ov.kind.clone()
                } else {
                    self.kind.clone()
                };

                ResolvedService {
                    dir: ov.dir.clone().or_else(|| self.dir.clone()),
                    env,
                    env_file: ov.env_file.clone().unwrap_or_else(|| self.env_file.clone()),
                    watch: ov.watch.clone().unwrap_or_else(|| self.watch.clone()),
                    ignore: ov.ignore.clone().unwrap_or_else(|| self.ignore.clone()),
                    debounce: ov.debounce.clone().or_else(|| self.debounce.clone()),
                    depends_on: ov
                        .depends_on
                        .clone()
                        .unwrap_or_else(|| self.depends_on.clone()),
                    proxy: ov.proxy.clone().unwrap_or_else(|| self.proxy.clone()),
                    lazy: ov.lazy.unwrap_or(self.lazy),
                    download: ov.download.clone().or_else(|| self.download.clone()),
                    ready: ov.ready.clone().or_else(|| self.ready.clone()),
                    shutdown: ov.shutdown.clone().or_else(|| self.shutdown.clone()),
                    log: ov.log.clone().unwrap_or_else(|| self.log.clone()),
                    log_filter: ov
                        .log_filter
                        .clone()
                        .unwrap_or_else(|| self.log_filter.clone()),
                    reload: ov.reload.unwrap_or(self.reload),
                    tty: ov.tty.unwrap_or(self.tty),
                    on_failure: ov.on_failure.unwrap_or(self.on_failure),
                    auto_filter_on_failure: ov
                        .auto_filter_on_failure
                        .or(self.auto_filter_on_failure),
                    notify: self.notify,
                    kind,
                    resolved_binary_path: None,
                }
            }
        }
    }
}

impl ResolvedService {
    /// Returns the `DockerConfig` if this is a Docker service.
    pub fn docker_config(&self) -> Option<&DockerConfig> {
        match &self.kind {
            Some(ServiceKind::Docker(d)) => Some(d),
            _ => None,
        }
    }

    /// Returns the `RustConfig` if this is a Rust service.
    pub fn rust_config(&self) -> Option<&RustConfig> {
        match &self.kind {
            Some(ServiceKind::Rust(r)) => Some(r),
            _ => None,
        }
    }

    /// Returns the `GoConfig` if this is a Go service.
    pub fn go_config(&self) -> Option<&GoConfig> {
        match &self.kind {
            Some(ServiceKind::Go(g)) => Some(g),
            _ => None,
        }
    }

    /// Returns the `BazelConfig` if this is a Bazel service.
    pub fn bazel_config(&self) -> Option<&BazelConfig> {
        match &self.kind {
            Some(ServiceKind::Bazel(b)) => Some(b),
            _ => None,
        }
    }

    /// Returns the `TurboConfig` if this is a Turbo service.
    pub fn turbo_config(&self) -> Option<&TurboConfig> {
        match &self.kind {
            Some(ServiceKind::Turbo(t)) => Some(t),
            _ => None,
        }
    }

    /// Returns the run command if this is a Custom service.
    pub fn run_cmd(&self) -> Option<&Command> {
        match &self.kind {
            Some(ServiceKind::Custom { run, .. }) => Some(run),
            _ => None,
        }
    }

    /// Returns the build command if this is a Custom service with a build step.
    pub fn build_cmd(&self) -> Option<&Command> {
        match &self.kind {
            Some(ServiceKind::Custom { build, .. }) => build.as_ref(),
            _ => None,
        }
    }

    /// Returns true if this is a Bazel or Turbo service (build-tool managed).
    pub fn is_build_tool_managed(&self) -> bool {
        matches!(
            &self.kind,
            Some(ServiceKind::Bazel(_)) | Some(ServiceKind::Turbo(_))
        )
    }

    pub(crate) fn build_tool_watch_enabled(&self) -> bool {
        if !self.reload {
            return false;
        }

        match &self.kind {
            Some(ServiceKind::Bazel(bazel)) => bazel.watch,
            Some(ServiceKind::Turbo(turbo)) => turbo.watch,
            _ => false,
        }
    }

    /// Resolve the run command for a custom service, taking downloads into account.
    ///
    /// If a download exists for this platform, the binary path from the download
    /// replaces `run.cmd`. The original `run.args` are preserved.
    /// Returns `(executable_path, args)`.
    pub fn resolved_run_cmd(
        &self,
        platform: Platform,
        service_name: &str,
        cache_base: Option<&std::path::Path>,
    ) -> Result<(PathBuf, &[String]), String> {
        let run = self.run_cmd().ok_or("service has no run command")?;

        let cache_base = cache_base
            .map(PathBuf::from)
            .unwrap_or_else(default_cache_base);

        let executable = match &self.download {
            Some(dl) => match dl.for_platform(platform) {
                Some(artifact) => artifact
                    .binary_path(&cache_base, service_name)
                    .ok_or_else(|| format!("download url has no filename: {}", artifact.url))?,
                None => PathBuf::from(&run.cmd),
            },
            None => PathBuf::from(&run.cmd),
        };

        Ok((executable, &run.args))
    }
}
