use super::paths::{resolve_watch_ignore_patterns, working_dir_for};
use super::service_worker::ensure_download_for_config_worker;
use super::task;
use super::terminal::TerminalCoordinator;
use crate::config::{Platform, TaskAutoRun};
use crate::task_state::{TaskHashProgress, TaskState};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Clone, Copy)]
pub(in crate::runner) enum TaskRunMode {
    Startup { has_dependents: bool },
    Triggered,
}

pub(in crate::runner) enum TaskRunPrepared {
    PendingRun { message: String },
    Skipped { message: String },
    Spawned(Box<task::TaskSpawn>),
    ForegroundSpawned(Box<task::ForegroundTaskSpawn>),
}

pub(in crate::runner) struct TaskWorkerContext {
    pub(in crate::runner) base_dir: PathBuf,
    pub(in crate::runner) platform: Platform,
    pub(in crate::runner) emitter: crate::output::LifecycleEmitter,
    pub(in crate::runner) global_watch_ignore: Vec<String>,
    pub(in crate::runner) terminal_coordinator: TerminalCoordinator,
}

fn format_hash_progress(progress: TaskHashProgress) -> String {
    match progress {
        TaskHashProgress::GlobStarted { pattern } => {
            format!("task state: expanding watch glob pattern={pattern:?}")
        }
        TaskHashProgress::GlobProgress {
            pattern,
            entries_seen,
            files_matched,
            files_ignored,
            elapsed,
        } => format!(
            "task state: glob progress pattern={pattern:?} entries={entries_seen} matched_files={files_matched} ignored_files={files_ignored} elapsed={elapsed:?}"
        ),
        TaskHashProgress::GlobFinished {
            pattern,
            entries_seen,
            files_matched,
            files_ignored,
            elapsed,
        } => format!(
            "task state: glob complete pattern={pattern:?} entries={entries_seen} matched_files={files_matched} ignored_files={files_ignored} elapsed={elapsed:?}"
        ),
        TaskHashProgress::HashStarted { total_files } => {
            format!("task state: hashing watched file contents files={total_files}")
        }
        TaskHashProgress::HashProgress {
            files_hashed,
            total_files,
            bytes_hashed,
            elapsed,
        } => format!(
            "task state: hash progress files={files_hashed}/{total_files} bytes={bytes_hashed} elapsed={elapsed:?}"
        ),
        TaskHashProgress::HashFinished {
            files_hashed,
            bytes_hashed,
            elapsed,
        } => format!(
            "task state: hash complete files={files_hashed} bytes={bytes_hashed} elapsed={elapsed:?}"
        ),
    }
}

/// Inputs to the startup run/skip/pending decision for a single task.
struct StartupTaskInputs {
    auto_run: TaskAutoRun,
    /// Whether the task declares any `params` (interactive task).
    has_params: bool,
    /// Whether the task declares any `watch` patterns.
    has_watch: bool,
    /// Whether the watched inputs changed since the last recorded success.
    /// Always `false` when `has_watch` is `false`.
    needs_watch_run: bool,
    /// Whether the task has at least one recorded successful run.
    has_success: bool,
    /// Whether any other item depends on this task.
    has_dependents: bool,
}

/// What to do with a task during the startup sweep.
#[derive(Debug, PartialEq, Eq)]
enum StartupTaskDecision {
    /// Don't run; report the reason and leave the task idle.
    Skip { message: &'static str },
    /// Don't auto-run, but mark the task as needing a manual run.
    PendingRun { message: String },
    /// Spawn the task now.
    Run,
}

/// Decide whether a task should run, be parked as pending, or be skipped on
/// startup.
///
/// Kept as a pure function so the run/skip/pending truth table is exhaustively
/// table-tested without spinning up a runner. A task with `auto_run = false`
/// (or `params`) never auto-runs, but it must still be parked in `PendingRun`
/// when its watched inputs changed — otherwise a `don start` after editing the
/// inputs would silently drop the work the user expects to see queued.
fn decide_startup_task(inputs: StartupTaskInputs) -> StartupTaskDecision {
    let StartupTaskInputs {
        auto_run,
        has_params,
        has_watch,
        needs_watch_run,
        has_success,
        has_dependents,
    } = inputs;

    if auto_run.runs_on_every_startup() && !has_params {
        return StartupTaskDecision::Run;
    }

    if has_watch && !needs_watch_run {
        return StartupTaskDecision::Skip {
            message: "skipped (no changes)",
        };
    }

    let should_run_or_prompt = if has_params {
        needs_watch_run || (!has_success && has_dependents)
    } else {
        match auto_run {
            TaskAutoRun::Always | TaskAutoRun::AlwaysOnStart => !has_watch || needs_watch_run,
            TaskAutoRun::Never => needs_watch_run || (!has_success && has_dependents),
            TaskAutoRun::Once => !has_success || needs_watch_run,
        }
    };
    if !should_run_or_prompt {
        return StartupTaskDecision::Skip {
            message: "skipped (not needed)",
        };
    }

    if has_params {
        return StartupTaskDecision::PendingRun {
            message: if has_dependents {
                "pending — required by dependents, task has params".to_string()
            } else {
                "pending — watch inputs changed, task has params".to_string()
            },
        };
    }
    if !auto_run.runs_automatically_on_startup(has_success) {
        return StartupTaskDecision::PendingRun {
            message: match auto_run {
                TaskAutoRun::Always | TaskAutoRun::AlwaysOnStart => {
                    "pending — run manually".to_string()
                }
                TaskAutoRun::Never => {
                    if has_dependents {
                        "pending — required by dependents, run manually".to_string()
                    } else {
                        "pending — watch inputs changed, auto_run = false".to_string()
                    }
                }
                TaskAutoRun::Once => {
                    if has_dependents {
                        "pending — required by dependents, auto_run = once".to_string()
                    } else {
                        "pending — watch inputs changed, auto_run = once".to_string()
                    }
                }
            },
        };
    }

    StartupTaskDecision::Run
}

pub(in crate::runner) async fn run_task_worker(
    ctx: TaskWorkerContext,
    name: &str,
    task_cfg: &crate::config::Task,
    params: &HashMap<String, String>,
    mode: TaskRunMode,
) -> Result<TaskRunPrepared, String> {
    let TaskWorkerContext {
        base_dir,
        platform,
        emitter,
        global_watch_ignore,
        terminal_coordinator,
    } = ctx;
    if let TaskRunMode::Startup { has_dependents } = mode {
        let has_watch = !task_cfg.watch.is_empty();
        let watch_base = working_dir_for(&base_dir, task_cfg.dir.as_deref());
        let ignore_patterns = resolve_watch_ignore_patterns(
            &watch_base,
            &task_cfg.ignore,
            &base_dir,
            &global_watch_ignore,
        );
        let task_state = TaskState::new(base_dir.join(".don").join("task-state"));
        let needs_watch_run = if has_watch && !task_cfg.auto_run.runs_on_every_startup() {
            let check_started = Instant::now();
            emitter.service_debug_event(
                name,
                &format!(
                    "task state: watched input check started base={} patterns={:?} ignore_patterns={}",
                    watch_base.display(),
                    task_cfg.watch,
                    ignore_patterns.len()
                ),
            );
            let progress_emitter = emitter.clone();
            let progress_name = name.to_string();
            match task_state
                .needs_run_with_progress(
                    name,
                    &task_cfg.watch,
                    &ignore_patterns,
                    Some(&watch_base),
                    move |progress| {
                        progress_emitter
                            .service_debug_event(&progress_name, &format_hash_progress(progress));
                    },
                )
                .await
            {
                Ok(needs_run) => {
                    emitter.service_debug_event(
                        name,
                        &format!(
                            "task state: watched input check complete changed={needs_run} elapsed={:?}",
                            check_started.elapsed()
                        ),
                    );
                    needs_run
                }
                Err(e) => {
                    emitter.service_debug_event(
                        name,
                        &format!(
                            "task state: watched input check failed after {:?}: {e}; treating inputs as changed",
                            check_started.elapsed()
                        ),
                    );
                    true
                }
            }
        } else {
            false
        };
        let has_success = match task_state.has_success(name).await {
            Ok(has_success) => has_success,
            Err(e) => {
                emitter.service_debug_event(
                    name,
                    &format!(
                        "task state: failed to read prior success marker: {e}; treating task as never successful"
                    ),
                );
                false
            }
        };

        match decide_startup_task(StartupTaskInputs {
            auto_run: task_cfg.auto_run,
            has_params: !task_cfg.params.is_empty(),
            has_watch,
            needs_watch_run,
            has_success,
            has_dependents,
        }) {
            StartupTaskDecision::Skip { message } => {
                return Ok(TaskRunPrepared::Skipped {
                    message: message.to_string(),
                });
            }
            StartupTaskDecision::PendingRun { message } => {
                return Ok(TaskRunPrepared::PendingRun { message });
            }
            StartupTaskDecision::Run => {}
        }
    }

    ensure_download_for_config_worker(
        &base_dir,
        platform,
        name,
        task_cfg.download.as_ref(),
        None,
        &emitter,
    )
    .await
    .map_err(|e| format!("download failed: {e}"))?;

    if task_cfg.terminal.is_foreground() {
        // Pause the TUI before spawn_foreground_task touches termios /
        // tcsetpgrp so the inline viewport, raw mode, and input task are
        // gone before the child takes over the terminal.
        terminal_coordinator.acquire().await;
        let spawn =
            match task::spawn_foreground_task(task_cfg, name, &base_dir, platform, params).await {
                Ok(spawn) => spawn,
                Err(e) => {
                    // The fg task never took the terminal — restore the TUI.
                    terminal_coordinator.release().await;
                    return Err(e.to_string());
                }
            };
        Ok(TaskRunPrepared::ForegroundSpawned(Box::new(spawn)))
    } else {
        let spawn = task::spawn_task(task_cfg, name, &base_dir, platform, params)
            .await
            .map_err(|e| e.to_string())?;
        Ok(TaskRunPrepared::Spawned(Box::new(spawn)))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn skip(message: &'static str) -> StartupTaskDecision {
        StartupTaskDecision::Skip { message }
    }

    fn pending(message: &str) -> StartupTaskDecision {
        StartupTaskDecision::PendingRun {
            message: message.to_string(),
        }
    }

    #[test]
    fn startup_task_decision_table() {
        struct Case {
            name: &'static str,
            auto_run: TaskAutoRun,
            has_params: bool,
            has_watch: bool,
            needs_watch_run: bool,
            has_success: bool,
            has_dependents: bool,
            expected: StartupTaskDecision,
        }

        // `needs_watch_run` is only ever true when `has_watch` is true — the
        // caller enforces that invariant, so the table mirrors it.
        let cases = vec![
            Case {
                name: "always: no watch, never run -> run",
                auto_run: TaskAutoRun::Always,
                has_params: false,
                has_watch: false,
                needs_watch_run: false,
                has_success: false,
                has_dependents: false,
                expected: StartupTaskDecision::Run,
            },
            Case {
                name: "always: watch changed -> run",
                auto_run: TaskAutoRun::Always,
                has_params: false,
                has_watch: true,
                needs_watch_run: true,
                has_success: true,
                has_dependents: false,
                expected: StartupTaskDecision::Run,
            },
            Case {
                name: "always: watch unchanged -> skip no changes",
                auto_run: TaskAutoRun::Always,
                has_params: false,
                has_watch: true,
                needs_watch_run: false,
                has_success: true,
                has_dependents: false,
                expected: skip("skipped (no changes)"),
            },
            Case {
                name: "always-on-start: watch unchanged after success -> run",
                auto_run: TaskAutoRun::AlwaysOnStart,
                has_params: false,
                has_watch: true,
                needs_watch_run: false,
                has_success: true,
                has_dependents: false,
                expected: StartupTaskDecision::Run,
            },
            // Regression: a `auto_run = false` task that already succeeded must
            // still be parked as pending when its watched inputs change. Before
            // the fix this returned `skipped (not needed)`.
            Case {
                name: "never: watch changed after success -> pending",
                auto_run: TaskAutoRun::Never,
                has_params: false,
                has_watch: true,
                needs_watch_run: true,
                has_success: true,
                has_dependents: false,
                expected: pending("pending — watch inputs changed, auto_run = false"),
            },
            Case {
                name: "never: watch changed, never succeeded -> pending",
                auto_run: TaskAutoRun::Never,
                has_params: false,
                has_watch: true,
                needs_watch_run: true,
                has_success: false,
                has_dependents: false,
                expected: pending("pending — watch inputs changed, auto_run = false"),
            },
            Case {
                name: "never: watch changed with dependents -> pending (deps)",
                auto_run: TaskAutoRun::Never,
                has_params: false,
                has_watch: true,
                needs_watch_run: true,
                has_success: true,
                has_dependents: true,
                expected: pending("pending — required by dependents, run manually"),
            },
            Case {
                name: "never: watch unchanged -> skip no changes",
                auto_run: TaskAutoRun::Never,
                has_params: false,
                has_watch: true,
                needs_watch_run: false,
                has_success: true,
                has_dependents: false,
                expected: skip("skipped (no changes)"),
            },
            Case {
                name: "never: no watch, no deps, no success -> skip not needed",
                auto_run: TaskAutoRun::Never,
                has_params: false,
                has_watch: false,
                needs_watch_run: false,
                has_success: false,
                has_dependents: false,
                expected: skip("skipped (not needed)"),
            },
            Case {
                name: "never: no watch, dependents, no success -> pending (deps)",
                auto_run: TaskAutoRun::Never,
                has_params: false,
                has_watch: false,
                needs_watch_run: false,
                has_success: false,
                has_dependents: true,
                expected: pending("pending — required by dependents, run manually"),
            },
            Case {
                name: "never: no watch, dependents already satisfied -> skip not needed",
                auto_run: TaskAutoRun::Never,
                has_params: false,
                has_watch: false,
                needs_watch_run: false,
                has_success: true,
                has_dependents: true,
                expected: skip("skipped (not needed)"),
            },
            Case {
                name: "once: no watch, never run -> run",
                auto_run: TaskAutoRun::Once,
                has_params: false,
                has_watch: false,
                needs_watch_run: false,
                has_success: false,
                has_dependents: false,
                expected: StartupTaskDecision::Run,
            },
            Case {
                name: "once: no watch after success -> skip not needed",
                auto_run: TaskAutoRun::Once,
                has_params: false,
                has_watch: false,
                needs_watch_run: false,
                has_success: true,
                has_dependents: false,
                expected: skip("skipped (not needed)"),
            },
            Case {
                name: "once: watch changed after success -> pending",
                auto_run: TaskAutoRun::Once,
                has_params: false,
                has_watch: true,
                needs_watch_run: true,
                has_success: true,
                has_dependents: false,
                expected: pending("pending — watch inputs changed, auto_run = once"),
            },
            Case {
                name: "once: watch changed after success with dependents -> pending (deps)",
                auto_run: TaskAutoRun::Once,
                has_params: false,
                has_watch: true,
                needs_watch_run: true,
                has_success: true,
                has_dependents: true,
                expected: pending("pending — required by dependents, auto_run = once"),
            },
            Case {
                name: "params: watch changed -> pending",
                auto_run: TaskAutoRun::Always,
                has_params: true,
                has_watch: true,
                needs_watch_run: true,
                has_success: true,
                has_dependents: false,
                expected: pending("pending — watch inputs changed, task has params"),
            },
            Case {
                name: "params: no watch, dependents, no success -> pending (deps)",
                auto_run: TaskAutoRun::Always,
                has_params: true,
                has_watch: false,
                needs_watch_run: false,
                has_success: false,
                has_dependents: true,
                expected: pending("pending — required by dependents, task has params"),
            },
            Case {
                name: "params: no watch, no deps, no success -> skip not needed",
                auto_run: TaskAutoRun::Always,
                has_params: true,
                has_watch: false,
                needs_watch_run: false,
                has_success: false,
                has_dependents: false,
                expected: skip("skipped (not needed)"),
            },
        ];

        for case in cases {
            let got = decide_startup_task(StartupTaskInputs {
                auto_run: case.auto_run,
                has_params: case.has_params,
                has_watch: case.has_watch,
                needs_watch_run: case.needs_watch_run,
                has_success: case.has_success,
                has_dependents: case.has_dependents,
            });
            assert_eq!(got, case.expected, "case '{}'", case.name);
        }
    }
}
