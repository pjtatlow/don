use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

use super::download::DownloadConfig;
use super::param::TaskParam;
use super::platform::Platform;
use super::types::{BazelConfig, LogConfig, TurboConfig};

/// Automatic run policy for a task.
///
/// This controls whether don may start the task without an explicit manual
/// trigger when the task is needed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TaskAutoRun {
    /// Run automatically whenever the runner decides the task is needed.
    #[default]
    Always,
    /// Never run automatically; move to `PendingRun` instead.
    Never,
    /// Run automatically only on startup, and only until the task has one
    /// successful run recorded. After that, the task becomes manual forever
    /// unless the user explicitly triggers it.
    Once,
}

impl TaskAutoRun {
    pub(crate) fn runs_automatically_on_startup(self, has_success: bool) -> bool {
        match self {
            Self::Always => true,
            Self::Never => false,
            Self::Once => !has_success,
        }
    }

    pub(crate) fn runs_automatically_on_watch(self) -> bool {
        matches!(self, Self::Always)
    }
}

impl<'de> Deserialize<'de> for TaskAutoRun {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum RawTaskAutoRun {
            Bool(bool),
            String(String),
        }

        match RawTaskAutoRun::deserialize(deserializer)? {
            RawTaskAutoRun::Bool(true) => Ok(TaskAutoRun::Always),
            RawTaskAutoRun::Bool(false) => Ok(TaskAutoRun::Never),
            RawTaskAutoRun::String(value) => match value.as_str() {
                "always" => Ok(TaskAutoRun::Always),
                "never" => Ok(TaskAutoRun::Never),
                "once" => Ok(TaskAutoRun::Once),
                _ => Err(serde::de::Error::custom(format!(
                    "unknown auto_run value '{value}', expected true, false, \"always\", \"never\", or \"once\""
                ))),
            },
        }
    }
}

/// How a task is connected to the user's terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskTerminal {
    /// Whether the task uses Don's log multiplexer or takes over the terminal.
    pub mode: TaskTerminalMode,
    /// Which terminal screen a foreground task should use.
    pub screen: TaskTerminalScreen,
    /// What a foreground task does when no controlling terminal is available.
    pub fallback: TaskTerminalFallback,
}

impl Default for TaskTerminal {
    fn default() -> Self {
        Self {
            mode: TaskTerminalMode::Muxed,
            screen: TaskTerminalScreen::Main,
            fallback: TaskTerminalFallback::Error,
        }
    }
}

impl TaskTerminal {
    /// Returns true when this task takes exclusive ownership of the terminal.
    pub fn is_foreground(self) -> bool {
        matches!(self.mode, TaskTerminalMode::Foreground)
    }

    /// True when a foreground task should degrade to muxed (run headless)
    /// instead of failing with no controlling terminal.
    pub fn falls_back_to_muxed(self) -> bool {
        matches!(self.fallback, TaskTerminalFallback::Muxed)
    }

    /// Downgrade a `fallback = "muxed"` foreground task to muxed for headless
    /// runs. Idempotent; a no-op otherwise.
    pub fn downgrade_for_detached(&mut self) {
        if self.is_foreground() && self.falls_back_to_muxed() {
            self.mode = TaskTerminalMode::Muxed;
        }
    }
}

impl<'de> Deserialize<'de> for TaskTerminal {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum RawTaskTerminal {
            String(String),
            Table(TaskTerminalTable),
        }

        match RawTaskTerminal::deserialize(deserializer)? {
            RawTaskTerminal::String(value) => match value.as_str() {
                "muxed" => Ok(Self::default()),
                "foreground" => Ok(Self {
                    mode: TaskTerminalMode::Foreground,
                    screen: TaskTerminalScreen::Alternate,
                    fallback: TaskTerminalFallback::Error,
                }),
                _ => Err(serde::de::Error::custom(format!(
                    "unknown terminal value '{value}', expected \"muxed\" or \"foreground\""
                ))),
            },
            RawTaskTerminal::Table(table) => Ok(Self {
                mode: table.mode,
                screen: table.screen.unwrap_or(match table.mode {
                    TaskTerminalMode::Muxed => TaskTerminalScreen::Main,
                    TaskTerminalMode::Foreground => TaskTerminalScreen::Alternate,
                }),
                fallback: table.fallback.unwrap_or(TaskTerminalFallback::Error),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct TaskTerminalTable {
    mode: TaskTerminalMode,
    #[serde(default)]
    screen: Option<TaskTerminalScreen>,
    #[serde(default)]
    fallback: Option<TaskTerminalFallback>,
}

/// Task terminal ownership mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TaskTerminalMode {
    /// Route output through Don's prefixed log multiplexer.
    Muxed,
    /// Run the task alone with stdin/stdout/stderr attached to the user's terminal.
    Foreground,
}

/// Screen used while a foreground task owns the terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TaskTerminalScreen {
    /// Use the current terminal screen.
    Main,
    /// Enter the terminal alternate screen for the task, then restore the main screen.
    Alternate,
}

/// What a foreground task does with no controlling terminal (detached daemon,
/// `--no-tui`, or non-tty stdin).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TaskTerminalFallback {
    /// Fail — the task genuinely needs a terminal (e.g. interactive login).
    Error,
    /// Degrade to muxed and run headless. The command must run non-interactively
    /// (detect a non-tty stdin and skip prompts, or take a `--force`-style flag).
    Muxed,
}

/// A one-shot task that runs to completion.
///
/// Tasks can depend on services (waits for ready) and other tasks.
/// File watching determines whether the task needs to re-run.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Task {
    /// The command to execute.
    pub cmd: String,
    /// Arguments to pass to the command.
    #[serde(default)]
    pub args: Vec<String>,
    /// Working directory.
    pub dir: Option<PathBuf>,
    /// Environment variables.
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Services or tasks that must be ready/complete before this task runs.
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// File glob patterns — task only re-runs if these files changed since last success.
    /// If empty, the task always runs.
    #[serde(default)]
    pub watch: Vec<String>,
    /// File glob patterns to ignore when watching (e.g. "**/*.log", "target/**").
    #[serde(default)]
    pub ignore: Vec<String>,
    /// Maximum time the task is allowed to run (e.g. "5m", "30s"). No timeout by default.
    pub timeout: Option<String>,
    /// Where to send stdout/stderr. Defaults to stdout.
    #[serde(default)]
    pub log: LogConfig,
    /// How the task is connected to the terminal.
    ///
    /// Defaults to `muxed`, which routes output through Don's prefixed log
    /// pipeline. `foreground` gives the task exclusive terminal ownership
    /// while it runs.
    #[serde(default)]
    pub terminal: TaskTerminal,
    /// Whether the task runs automatically.
    ///
    /// Supported values:
    /// - `true` / `"always"`: run automatically whenever needed
    /// - `false` / `"never"`: never auto-run; enter `PendingRun` when needed
    /// - `"once"`: auto-run on startup until the first successful run, then
    ///   become manual forever unless explicitly triggered
    ///
    /// “Needed” means a dependent is waiting on the task, or watched inputs
    /// have changed. Defaults to `true`.
    #[serde(default)]
    pub auto_run: TaskAutoRun,
    /// Optional download configuration — artifacts to fetch before running.
    /// When a download exists for the current platform, its binary path
    /// replaces `cmd`. Without a matching platform entry, `cmd` is looked up on PATH.
    pub download: Option<DownloadConfig>,
    /// Bazel build tool integration — auto-resolve watch patterns from the build graph.
    /// Mutually exclusive with `turbo`.
    pub bazel: Option<BazelConfig>,
    /// Turborepo build tool integration — auto-resolve watch patterns from the task graph.
    /// Mutually exclusive with `bazel`.
    pub turbo: Option<TurboConfig>,
    /// Optional parameter declarations. When non-empty, the task is
    /// considered "interactive" — when the task is needed, file-watch
    /// changes or dependent startup will park it in `PendingRun` instead
    /// of auto-running, and the user supplies values via `don run <task>
    /// --<name>=<value>` or the TUI form.
    /// Values substitute into `cmd`/`args`/`env`/`dir` via `{{name}}`
    /// placeholders.
    #[serde(default)]
    pub params: Vec<TaskParam>,
    /// Whether this task's log output is hidden by default in the TUI
    /// filter. Users can still unhide it interactively from the filter view.
    /// Defaults to `false` (visible).
    #[serde(default)]
    pub hidden: bool,
    /// Override the top-level `auto_filter_on_failure` setting for this task.
    /// When enabled, a task failure adds this task to the TUI log filter.
    #[serde(default)]
    pub auto_filter_on_failure: Option<bool>,
}

impl Task {
    pub(crate) fn build_tool_watch_enabled(&self) -> bool {
        self.bazel.as_ref().is_some_and(|bazel| bazel.watch)
            || self.turbo.as_ref().is_some_and(|turbo| turbo.watch)
    }

    /// Resolve the task's command path, using the cached download binary
    /// if one is configured for this platform.
    pub fn resolved_cmd(
        &self,
        platform: Platform,
        task_name: &str,
        cache_base: Option<&std::path::Path>,
    ) -> Result<PathBuf, String> {
        let cache_base = cache_base
            .map(PathBuf::from)
            .unwrap_or_else(super::download::default_cache_base);

        let executable = match &self.download {
            Some(dl) => match dl.for_platform(platform) {
                Some(artifact) => artifact
                    .binary_path(&cache_base, task_name)
                    .ok_or_else(|| format!("download url has no filename: {}", artifact.url))?,
                None => PathBuf::from(&self.cmd),
            },
            None => PathBuf::from(&self.cmd),
        };
        Ok(executable)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn terminal_defaults_to_muxed() {
        let task: Task = toml::from_str(r#"cmd = "true""#).unwrap();
        assert_eq!(task.terminal.mode, TaskTerminalMode::Muxed);
        assert_eq!(task.terminal.screen, TaskTerminalScreen::Main);
    }

    #[test]
    fn terminal_foreground_string_uses_alternate_screen() {
        let task: Task = toml::from_str(
            r#"
            cmd = "vim"
            terminal = "foreground"
            "#,
        )
        .unwrap();
        assert_eq!(task.terminal.mode, TaskTerminalMode::Foreground);
        assert_eq!(task.terminal.screen, TaskTerminalScreen::Alternate);
    }

    #[test]
    fn terminal_foreground_table_can_use_main_screen() {
        let task: Task = toml::from_str(
            r#"
            cmd = "vim"
            terminal = { mode = "foreground", screen = "main" }
            "#,
        )
        .unwrap();
        assert_eq!(task.terminal.mode, TaskTerminalMode::Foreground);
        assert_eq!(task.terminal.screen, TaskTerminalScreen::Main);
    }

    #[test]
    fn terminal_fallback_defaults_to_error() {
        let task: Task = toml::from_str(r#"cmd = "true""#).unwrap();
        assert_eq!(task.terminal.fallback, TaskTerminalFallback::Error);
        assert!(!task.terminal.falls_back_to_muxed());
    }

    #[test]
    fn terminal_foreground_can_opt_into_muxed_fallback() {
        let task: Task = toml::from_str(
            r#"
            cmd = "scurry push"
            terminal = { mode = "foreground", fallback = "muxed" }
            "#,
        )
        .unwrap();
        assert_eq!(task.terminal.mode, TaskTerminalMode::Foreground);
        assert!(task.terminal.falls_back_to_muxed());
    }

    #[test]
    fn downgrade_for_detached_table() {
        struct Case {
            name: &'static str,
            terminal: TaskTerminal,
            expected_mode: TaskTerminalMode,
        }
        let fg = |fallback| TaskTerminal {
            mode: TaskTerminalMode::Foreground,
            screen: TaskTerminalScreen::Alternate,
            fallback,
        };
        let cases = [
            Case {
                name: "foreground + muxed fallback -> muxed",
                terminal: fg(TaskTerminalFallback::Muxed),
                expected_mode: TaskTerminalMode::Muxed,
            },
            Case {
                name: "foreground + error fallback -> unchanged",
                terminal: fg(TaskTerminalFallback::Error),
                expected_mode: TaskTerminalMode::Foreground,
            },
            Case {
                name: "already muxed -> unchanged",
                terminal: TaskTerminal::default(),
                expected_mode: TaskTerminalMode::Muxed,
            },
        ];
        for case in cases {
            let mut terminal = case.terminal;
            terminal.downgrade_for_detached();
            assert_eq!(terminal.mode, case.expected_mode, "case '{}'", case.name);
            terminal.downgrade_for_detached();
            assert_eq!(
                terminal.mode, case.expected_mode,
                "case '{}' not idempotent",
                case.name
            );
        }
    }
}
