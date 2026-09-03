use serde::Deserialize;
use std::path::PathBuf;

fn default_true() -> bool {
    true
}

/// Bazel build tool integration for a service or task.
///
/// When configured, Don queries Bazel at startup to determine which source
/// packages contribute to the target, and watches those directories for changes.
/// This replaces the need for manually maintaining `watch` patterns.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct BazelConfig {
    /// Bazel target label (e.g. `"//services/api:api"`).
    pub target: String,
    /// Whether Don should auto-watch source packages resolved from the Bazel
    /// graph, restarting the service when any of them changes.
    ///
    /// **Services only.** A task builds its target before every run, so it is
    /// never stale and has nothing to gain from watching the graph; what makes
    /// a *task* run again is its own `watch` globs, its `depends_on`, or you.
    /// Wanting a bazel graph watched is what makes something a service.
    #[serde(default = "default_true")]
    pub watch: bool,
    /// Name of a `.bazelrc` configuration to build this target under, passed
    /// as `--config=<name>`. Falls back to the workspace-wide
    /// [`BazelDefaults::config`] when unset.
    ///
    /// See that field for why this is a name rather than a list of flags.
    #[serde(default)]
    pub config: Option<String>,
}

impl BazelConfig {
    /// This config with the workspace-wide `--config` name filled in where it
    /// named none of its own.
    ///
    /// Applied once, where the build request is built, so everything
    /// downstream reads a single resolved name and no part of the build
    /// pipeline has to know a workspace default exists.
    pub fn with_workspace_default(mut self, workspace: Option<&str>) -> Self {
        if self.config.is_none() {
            self.config = workspace.map(str::to_string);
        }
        self
    }
}

/// Workspace-wide Bazel settings, from the top-level `[bazel]` table.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BazelDefaults {
    /// Name of a `.bazelrc` configuration every `bazel build` Don runs should
    /// use, passed as `--config=<name>`. Individual services and tasks may
    /// name a different one.
    ///
    /// A name rather than a list of flags, because `.bazelrc` is where Bazel
    /// already keeps this and it says everything a flag list could and more —
    /// inheritance between configs, platform-specific variants, comments:
    ///
    /// ```text
    /// # .bazelrc
    /// build:don --noshow_progress
    /// ```
    ///
    /// Naming a config that no `.rc` file defines is a hard Bazel error, not a
    /// warning, which is why this is opt-in and has no default. Don's own
    /// `--curses=no` and `--color=yes` are passed *before* it, so a config
    /// that sets them wins — including in the ways that stop Don reading the
    /// output as lines.
    #[serde(default)]
    pub config: Option<String>,
}

/// A single proxy entry. Don binds `listen` once at startup and holds the
/// port across service restarts.
///
/// Exactly one of two modes must be chosen:
///
/// - **Forwarding with env injection.** Don accepts on `listen`, allocates an
///   ephemeral port for the backend, sets `env` to the ephemeral port number,
///   and forwards bytes between client and backend. The service binds the
///   ephemeral port using the value of the env var.
/// - **Listenfd handoff.** Don binds `listen` and passes the bound listener
///   to the service as `fd 3` via the systemd `LISTEN_FDS` protocol. The
///   service accepts on the fd directly — no proxy involved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyEntry {
    /// Address Don binds on (e.g. "127.0.0.1:3000").
    pub listen: String,
    /// Mode selector. Exactly one variant applies per entry.
    pub mode: ProxyMode,
}

/// How a given [`ProxyEntry`] exposes the public listener to the service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProxyMode {
    /// Accept on the public address, forward to an ephemeral backend port.
    /// The ephemeral port number is injected into the service's environment
    /// under the contained variable name.
    Env(String),
    /// Hand the bound listener to the service via `LISTEN_FDS` / `LISTEN_FDNAMES`
    /// / `LISTEN_PID`. No forwarding — the service accepts directly.
    Listenfd,
    /// Accept on the public address, forward to the fixed backend address
    /// `addr`. The service is expected to bind `addr` on its own — don
    /// does NOT inject an env var, and does NOT allocate a port. Use this
    /// when the service has a compile-time or config-baked backend port
    /// that can't come from the environment.
    ///
    /// Restart semantics differ from env/listenfd: don waits for the old
    /// instance's process group to fully exit before starting the new one,
    /// because two processes trying to bind the same fixed port would race.
    Forward(String),
}

impl<'de> Deserialize<'de> for ProxyEntry {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum ForwardRaw {
            Addr(String),
            Port(u16),
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            String(String),
            Table {
                listen: String,
                env: Option<String>,
                #[serde(default)]
                listenfd: bool,
                forward: Option<ForwardRaw>,
            },
        }

        match Raw::deserialize(deserializer)? {
            // Shorthand `proxy = "127.0.0.1:3000"` means listenfd mode — the
            // simpler of the two modes and the common case for services that
            // speak the systemd socket-activation protocol.
            Raw::String(s) => Ok(ProxyEntry {
                listen: s,
                mode: ProxyMode::Listenfd,
            }),
            Raw::Table {
                listen,
                env,
                listenfd,
                forward,
            } => {
                let forward_addr = forward.map(|f| match f {
                    ForwardRaw::Addr(a) => a,
                    ForwardRaw::Port(p) => format!("127.0.0.1:{p}"),
                });
                let mode = match (env, listenfd, forward_addr) {
                    (Some(e), false, None) => ProxyMode::Env(e),
                    (None, true, None) => ProxyMode::Listenfd,
                    (None, false, Some(a)) => ProxyMode::Forward(a),
                    (None, false, None) => {
                        return Err(serde::de::Error::custom(
                            "proxy entry: one of 'env = \"VAR\"', 'listenfd = true', or 'forward = \"addr\"' must be set",
                        ));
                    }
                    _ => {
                        return Err(serde::de::Error::custom(
                            "proxy entry: 'env', 'listenfd', and 'forward' are mutually exclusive",
                        ));
                    }
                };
                Ok(ProxyEntry { listen, mode })
            }
        }
    }
}

/// Deserialize the `proxy` field which accepts:
/// - A single string: `proxy = "127.0.0.1:3000"` (listenfd mode)
/// - A single table: `proxy = { listen = "127.0.0.1:3000", env = "PORT" }`
///   or `{ listen = "127.0.0.1:3000", listenfd = true }`
/// - An array of strings/tables.
pub(crate) fn deserialize_proxy<'de, D>(deserializer: D) -> Result<Vec<ProxyEntry>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum RawProxy {
        Single(ProxyEntry),
        List(Vec<ProxyEntry>),
    }

    match RawProxy::deserialize(deserializer)? {
        RawProxy::Single(entry) => Ok(vec![entry]),
        RawProxy::List(entries) => Ok(entries),
    }
}

/// Deserialize an optional `proxy` field (for `ServiceOverride`).
/// Returns `None` when the field is absent, `Some(vec)` when present.
pub(crate) fn deserialize_proxy_option<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<ProxyEntry>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_proxy(deserializer).map(Some)
}

/// A command to execute: a binary/program name plus arguments.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Command {
    /// The binary or program to execute.
    pub cmd: String,
    /// Arguments to pass to the binary.
    #[serde(default)]
    pub args: Vec<String>,
}

/// Ready check configuration. Exactly one of `exec`, `tcp`, or `http` must be set.
///
/// Used to gate dependent services — they won't start until this check passes.
/// When `monitor = true`, the same check keeps running after the service is
/// ready and can mark it `Unhealthy` if it starts failing.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ReadyCheck {
    /// Run a command — exit code 0 means ready.
    pub exec: Option<Command>,
    /// Connect to a TCP address — successful connection means ready.
    pub tcp: Option<String>,
    /// Hit an HTTP endpoint — 2xx response means ready.
    pub http: Option<String>,
    /// How often to check during startup (e.g. "1s", "500ms"). Defaults to "1s".
    #[serde(default = "ReadyCheck::default_interval")]
    pub interval: String,
    /// How many times to retry before giving up. Defaults to 30.
    #[serde(default = "ReadyCheck::default_retries")]
    pub retries: u32,
    /// Maximum time allowed for one probe attempt. Defaults to "5s".
    ///
    /// This bounds slow HTTP connects and stuck exec probes so `retries` and
    /// `interval` remain meaningful even when a target stops responding.
    #[serde(default = "ReadyCheck::default_timeout")]
    pub timeout: String,
    /// If true, keep running the same check after the service reaches Ready.
    /// Consecutive failures will mark the service Unhealthy. The service-level
    /// `on_failure` field controls what happens on that transition.
    #[serde(default)]
    pub monitor: bool,
    /// Interval between checks while monitoring. Defaults to "10s".
    #[serde(default = "ReadyCheck::default_monitor_interval")]
    pub monitor_interval: String,
    /// Consecutive monitor failures required to transition Ready → Unhealthy.
    /// Defaults to 3.
    #[serde(default = "ReadyCheck::default_unhealthy_after")]
    pub unhealthy_after: u32,
}

impl ReadyCheck {
    fn default_interval() -> String {
        "1s".to_string()
    }

    fn default_monitor_interval() -> String {
        "10s".to_string()
    }

    fn default_retries() -> u32 {
        30
    }

    fn default_timeout() -> String {
        "5s".to_string()
    }

    fn default_unhealthy_after() -> u32 {
        3
    }
}

/// Action to take when a service fails. A "failure" is either:
///   - the health monitor marking the service `Unhealthy`, or
///   - the process exiting with a non-zero status (or terminating signal).
///
/// Clean exits (status 0) are treated as intentional shutdowns and never
/// trigger this policy — the service transitions to `Stopped`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnFailure {
    /// Mark the service `Unhealthy` / `Failed` and emit a lifecycle event.
    /// No restart.
    #[default]
    Notify,
    /// Restart the service with escalating backoff (1, 2, 4, 8, 16, 32, 60s
    /// capped at 60s). Backoff attempts reset when the service recovers
    /// to `Ready` or a restart succeeds.
    ///
    /// A crash-loop ceiling overrides this: if the process exits within 5s of
    /// being (re)started twice in a row, don stops auto-restarting and leaves
    /// the service `Failed` rather than hammering a binary that dies on
    /// launch. The streak is cleared once a run survives past that window.
    Restart,
}

/// Shutdown behavior for services.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShutdownConfig {
    /// If false, skip the graceful signal/timeout window and send SIGKILL immediately.
    pub graceful: bool,
    /// Signal to send for graceful shutdown (e.g. "SIGTERM", "SIGINT"). Defaults to "SIGTERM".
    pub signal: String,
    /// Time to wait for graceful shutdown before sending SIGKILL (e.g. "10s"). Defaults to "10s".
    pub timeout: String,
    graceful_set: bool,
    signal_set: bool,
    timeout_set: bool,
}

impl ShutdownConfig {
    fn default_signal() -> String {
        "SIGTERM".to_string()
    }

    fn default_timeout() -> String {
        "10s".to_string()
    }

    pub(crate) fn merged_over(&self, base: &Self) -> Self {
        Self {
            graceful: if self.graceful_set {
                self.graceful
            } else {
                base.graceful
            },
            signal: if self.signal_set {
                self.signal.clone()
            } else {
                base.signal.clone()
            },
            timeout: if self.timeout_set {
                self.timeout.clone()
            } else {
                base.timeout.clone()
            },
            graceful_set: true,
            signal_set: true,
            timeout_set: true,
        }
    }
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            graceful: true,
            signal: Self::default_signal(),
            timeout: Self::default_timeout(),
            graceful_set: false,
            signal_set: false,
            timeout_set: false,
        }
    }
}

impl<'de> Deserialize<'de> for ShutdownConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct RawShutdownConfig {
            graceful: Option<bool>,
            signal: Option<String>,
            timeout: Option<String>,
        }

        let raw = RawShutdownConfig::deserialize(deserializer)?;
        let graceful_set = raw.graceful.is_some();
        let signal_set = raw.signal.is_some();
        let timeout_set = raw.timeout.is_some();

        Ok(Self {
            graceful: raw.graceful.unwrap_or(true),
            signal: raw.signal.unwrap_or_else(Self::default_signal),
            timeout: raw.timeout.unwrap_or_else(Self::default_timeout),
            graceful_set,
            signal_set,
            timeout_set,
        })
    }
}

/// A list of regexes matched against complete output lines.
///
/// What a match *means* comes from the field this sits on: `log_filter` keeps
/// matching lines, `log_exclude` drops them. See [`LogFilters`] for how the
/// two combine.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LogFilterConfig {
    /// Regex patterns, as written in TOML: `log_filter = ["..."]` or
    /// `log_exclude = ["..."]`.
    pub patterns: Vec<String>,
}

impl LogFilterConfig {
    /// Return true when the filter has no active keep patterns.
    pub fn is_empty(&self) -> bool {
        self.patterns.is_empty()
    }

    /// Combine global and item-specific filters. Global patterns run first;
    /// service-specific patterns extend them.
    pub fn merged_with(&self, item: &Self) -> Self {
        let mut patterns = Vec::with_capacity(self.patterns.len() + item.patterns.len());
        patterns.extend(self.patterns.iter().cloned());
        patterns.extend(item.patterns.iter().cloned());
        Self { patterns }
    }
}

impl<'de> Deserialize<'de> for LogFilterConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let patterns = Vec::<String>::deserialize(deserializer)?;
        Ok(Self { patterns })
    }
}

/// Both regex filters in force for one process's output, already merged from
/// the global and per-item settings.
///
/// A line survives when it is *not* excluded and — if there are any keep
/// patterns at all — it matches one of them. Exclude wins on a line that
/// matches both: saying "drop anything with this in it" is a statement about
/// noise, and a keep pattern that happens to also match should not drag it
/// back in.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LogFilters {
    /// Lines to keep. Empty means "everything is a candidate".
    pub keep: LogFilterConfig,
    /// Lines to drop, whatever `keep` says.
    pub exclude: LogFilterConfig,
}

impl LogFilters {
    /// True when neither filter would ever reject a line, which lets the
    /// output path skip splitting chunks into lines at all.
    pub fn is_empty(&self) -> bool {
        self.keep.is_empty() && self.exclude.is_empty()
    }
}

/// Where to send a service's or task's stdout/stderr.
///
/// In TOML, this is either a string shorthand or a table:
/// - `log = "stdout"` (default)
/// - `log = "ignore"`
/// - `log = "path/to/file.log"`
/// - `log = { file = "path/to/file.log" }` (equivalent to the string form)
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum LogConfig {
    /// Print to don's stdout, prefixed with the service name.
    #[default]
    Stdout,
    /// Discard all output.
    Ignore,
    /// Write to a file.
    File(PathBuf),
}

impl<'de> Deserialize<'de> for LogConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            String(String),
            Table { file: PathBuf },
        }

        match Raw::deserialize(deserializer)? {
            Raw::String(s) => match s.as_str() {
                "stdout" => Ok(Self::Stdout),
                "ignore" => Ok(Self::Ignore),
                path => Ok(Self::File(PathBuf::from(path))),
            },
            Raw::Table { file } => Ok(Self::File(file)),
        }
    }
}
