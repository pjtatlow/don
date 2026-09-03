use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

use super::dependency::Dependency;
use super::download::DownloadConfig;
use super::param::TaskParam;
use super::platform::Platform;
use super::types::{BazelConfig, LogConfig};

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
    ///
    /// Optional because a task can instead be defined by `bazel.target`, in
    /// which case it runs the built artifact. Validation requires one or the
    /// other; a task with neither has nothing to run.
    #[serde(default)]
    pub cmd: Option<String>,
    /// Arguments to pass to the command.
    #[serde(default)]
    pub args: Vec<String>,
    /// Working directory.
    pub dir: Option<PathBuf>,
    /// Environment variables.
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Services or tasks that must be ready/complete before this task runs.
    /// A blocking entry also gates on success; a non-blocking one only orders
    /// startup.
    #[serde(default)]
    pub depends_on: Vec<Dependency>,
    /// File glob patterns — task only re-runs if these files changed since last success.
    /// If empty, the task always runs.
    #[serde(default)]
    pub watch: Vec<String>,
    /// File glob patterns to ignore when watching (e.g. "**/*.log", "target/**").
    #[serde(default)]
    pub ignore: Vec<String>,
    /// How long to wait after the last matching change before re-running
    /// (e.g. "500ms", "2s"). Defaults to 200ms. A generator whose own output
    /// lands in bursts wants a longer window than an editor save does.
    pub debounce: Option<String>,
    /// Maximum time the task is allowed to run (e.g. "5m", "30s"). No timeout by default.
    pub timeout: Option<String>,
    /// Where to send stdout/stderr. Defaults to stdout.
    #[serde(default)]
    pub log: LogConfig,
    /// Whether this task expects a human at its terminal.
    ///
    /// Every task runs on a PTY and every task can be reached with `don
    /// attach`, so this changes nothing about how the task is spawned or where
    /// its output goes. It is a declaration, because it is the one thing don
    /// cannot work out for itself: a task blocked reading stdin looks exactly
    /// like a task that has hung. Saying so is what lets don point the user at
    /// `don attach` instead of leaving them to guess.
    #[serde(default)]
    pub interactive: bool,
    /// Command overrides to use in non-TUI runs.
    ///
    /// When present, the task is not interactive — there is no client to
    /// attach — and omitted `cmd` or `args` fields inherit their top-level
    /// values.
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
    /// Bazel build tool integration.
    ///
    /// The target is built immediately before every run of this task, so what
    /// runs is never older than the sources. With no `cmd`, the task *is* the
    /// built artifact. `bazel.watch` has no effect here — see
    /// [`BazelConfig::watch`].
    pub bazel: Option<BazelConfig>,
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
    pub(crate) fn apply_headless_override(&mut self) {
        let Some(headless) = &self.headless else {
            return;
        };
        if let Some(cmd) = &headless.cmd {
            self.cmd = Some(cmd.clone());
        }
        if let Some(args) = &headless.args {
            self.args.clone_from(args);
        }
        // Running headless means nothing is going to attach, so a prompt to do
        // so would send the user after a client that does not exist.
        self.interactive = false;
    }

    /// Resolve the task's command path, using the cached download binary
    /// if one is configured for this platform.
    /// `None` when the task names no command of its own — a `bazel.target`
    /// task, whose executable is only known once the build has resolved it.
    pub fn resolved_cmd(
        &self,
        platform: Platform,
        task_name: &str,
        cache_base: Option<&std::path::Path>,
    ) -> Result<Option<PathBuf>, String> {
        let cache_base = cache_base
            .map(PathBuf::from)
            .unwrap_or_else(super::download::default_cache_base);

        let own = || self.cmd.as_ref().map(PathBuf::from);
        let executable = match &self.download {
            Some(dl) => match dl.for_platform(platform) {
                Some(artifact) => Some(
                    artifact
                        .binary_path(&cache_base, task_name)
                        .ok_or_else(|| format!("download url has no filename: {}", artifact.url))?,
                ),
                None => own(),
            },
            None => own(),
        };
        Ok(executable)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    /// `terminal = "foreground"` is the older spelling of `interactive = true`.
    /// It named a mechanism don no longer has, but the intent outlived the
    /// mechanism, so a config that still says it is honoured rather than
    /// rejected.
    #[test]
    fn terminal_is_read_as_the_interactive_it_became() {
        struct Case {
            name: &'static str,
            toml: &'static str,
            want: bool,
        }

        let cases = vec![
            Case {
                name: "foreground wanted a human at the terminal",
                toml: "[tasks.console]\ncmd = \"vim\"\nterminal = \"foreground\"\n",
                want: true,
            },
            Case {
                name: "muxed was the ordinary case, and still is",
                toml: "[tasks.console]\ncmd = \"true\"\nterminal = \"muxed\"\n",
                want: false,
            },
            Case {
                name: "the table spelling, whose screen key has no successor",
                toml: "[tasks.console]\ncmd = \"vim\"\nterminal = { mode = \"foreground\", screen = \"alternate\" }\n",
                want: true,
            },
            Case {
                name: "a table with no mode is the default",
                toml: "[tasks.console]\ncmd = \"true\"\nterminal = { screen = \"main\" }\n",
                want: false,
            },
            Case {
                // Mid-migration: the current key is the one to believe.
                name: "an explicit interactive wins over the old spelling",
                toml: "[tasks.console]\ncmd = \"vim\"\nterminal = \"muxed\"\ninteractive = true\n",
                want: true,
            },
        ];

        for case in cases {
            let config: crate::config::Config = case.toml.parse().unwrap();
            let task = config.tasks.get("console").unwrap();
            assert_eq!(task.interactive, case.want, "{}", case.name);
        }
    }

    #[test]
    fn a_task_is_not_interactive_unless_it_says_so() {
        struct Case {
            name: &'static str,
            toml: &'static str,
            want: bool,
        }

        let cases = vec![
            Case {
                name: "the default is a task nobody has to watch",
                toml: r#"cmd = "true""#,
                want: false,
            },
            Case {
                name: "declared",
                toml: "cmd = \"vim\"\ninteractive = true",
                want: true,
            },
            Case {
                name: "declared false is the default said out loud",
                toml: "cmd = \"vim\"\ninteractive = false",
                want: false,
            },
        ];

        for case in cases {
            let task: Task = toml::from_str(case.toml).unwrap();
            assert_eq!(task.interactive, case.want, "{}", case.name);
        }
    }

    /// The headless override is what a non-TUI run gets. Nothing can attach in
    /// that mode, so the interactivity claim has to come off with it —
    /// otherwise the run points the user at a client that will not be there.
    #[test]
    fn the_headless_override_replaces_the_command_and_clears_interactivity() {
        struct Case {
            name: &'static str,
            toml: &'static str,
            want_cmd: &'static str,
            want_args: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                name: "an omitted cmd inherits, args are replaced",
                toml: r#"
                    cmd = "scurry"
                    args = ["push"]
                    interactive = true
                    headless = { args = ["push", "--force"] }
                "#,
                want_cmd: "scurry",
                want_args: vec!["push", "--force"],
            },
            Case {
                name: "an explicit cmd replaces, an empty args list clears",
                toml: r#"
                    cmd = "prompt-me"
                    args = ["one"]
                    interactive = true
                    headless = { cmd = "batch", args = [] }
                "#,
                want_cmd: "batch",
                want_args: vec![],
            },
        ];

        for case in cases {
            let mut task: Task = toml::from_str(case.toml).unwrap();
            task.apply_headless_override();

            assert_eq!(
                task.cmd.as_deref(),
                Some(case.want_cmd),
                "{}: cmd",
                case.name
            );
            assert_eq!(task.args, case.want_args, "{}: args", case.name);
            assert!(!task.interactive, "{}: interactivity", case.name);
        }
    }
}
