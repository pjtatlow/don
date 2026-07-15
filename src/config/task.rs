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
    /// Run every startup regardless of the saved watch hash, and on watched changes.
    AlwaysOnStart,
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
            Self::Always | Self::AlwaysOnStart => true,
            Self::Never => false,
            Self::Once => !has_success,
        }
    }

    pub(crate) fn runs_automatically_on_watch(self) -> bool {
        matches!(self, Self::Always | Self::AlwaysOnStart)
    }

    pub(crate) fn runs_on_every_startup(self) -> bool {
        matches!(self, Self::AlwaysOnStart)
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
                "always-on-start" => Ok(TaskAutoRun::AlwaysOnStart),
                "never" => Ok(TaskAutoRun::Never),
                "once" => Ok(TaskAutoRun::Once),
                _ => Err(serde::de::Error::custom(format!(
                    "unknown auto_run value '{value}', expected true, false, \"always\", \"always-on-start\", \"never\", or \"once\""
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
}

impl Default for TaskTerminal {
    fn default() -> Self {
        Self {
            mode: TaskTerminalMode::Muxed,
            screen: TaskTerminalScreen::Main,
        }
    }
}

impl TaskTerminal {
    /// Returns true when this task takes exclusive ownership of the terminal.
    pub fn is_foreground(self) -> bool {
        matches!(self.mode, TaskTerminalMode::Foreground)
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

/// Command overrides used when a task runs without the interactive TUI.
///
/// Omitted fields inherit from the task's top-level `cmd` and `args`. An
/// explicitly empty `args` list clears the task's normal arguments.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
pub struct TaskHeadless {
    /// Command to use instead of the task's normal command.
    pub cmd: Option<String>,
    /// Arguments to use instead of the task's normal arguments.
    pub args: Option<Vec<String>>,
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
    /// Command overrides to use in non-TUI runs.
    ///
    /// When present, the task uses muxed output instead of taking foreground
    /// terminal ownership. Omitted `cmd` or `args` fields inherit their
    /// top-level values.
    pub headless: Option<TaskHeadless>,
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

    pub(crate) fn apply_headless_override(&mut self) {
        let Some(headless) = &self.headless else {
            return;
        };
        if let Some(cmd) = &headless.cmd {
            self.cmd.clone_from(cmd);
        }
        if let Some(args) = &headless.args {
            self.args.clone_from(args);
        }
        self.terminal = TaskTerminal::default();
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
    fn auto_run_values_deserialize() {
        for (value, expected) in [
            ("true", TaskAutoRun::Always),
            ("\"always\"", TaskAutoRun::Always),
            ("\"always-on-start\"", TaskAutoRun::AlwaysOnStart),
            ("false", TaskAutoRun::Never),
            ("\"never\"", TaskAutoRun::Never),
            ("\"once\"", TaskAutoRun::Once),
        ] {
            let task: Task =
                toml::from_str(&format!("cmd = \"true\"\nauto_run = {value}")).unwrap();
            assert_eq!(task.auto_run, expected, "value {value}");
        }
        let error = toml::from_str::<Task>("cmd = \"true\"\nauto_run = \"sometimes\"")
            .unwrap_err()
            .to_string();
        assert!(error.contains("\"always-on-start\""), "{error}");
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
    fn headless_override_inherits_omitted_command_and_replaces_args() {
        let mut task: Task = toml::from_str(
            r#"
            cmd = "scurry"
            args = ["push"]
            terminal = "foreground"
            headless = { args = ["push", "--force"] }
            "#,
        )
        .unwrap();

        task.apply_headless_override();

        assert_eq!(task.cmd, "scurry");
        assert_eq!(task.args, vec!["push", "--force"]);
        assert_eq!(task.terminal, TaskTerminal::default());
    }

    #[test]
    fn headless_override_can_replace_command_and_clear_args() {
        let mut task: Task = toml::from_str(
            r#"
            cmd = "interactive"
            args = ["one"]
            terminal = "foreground"
            headless = { cmd = "batch", args = [] }
            "#,
        )
        .unwrap();

        task.apply_headless_override();

        assert_eq!(task.cmd, "batch");
        assert!(task.args.is_empty());
        assert_eq!(task.terminal, TaskTerminal::default());
    }
}
