use super::paths::{resolve_watch_ignore_patterns, working_dir_for};
use super::task;
use super::task_worker::{TaskRunMode, TaskRunPrepared, TaskWorkerContext, run_task_worker};
use super::{
    CommandError, CommandResult, ItemDone, NodeKind, Runner, RunnerEvent, RunnerInternalCommand,
    TaskItemState, TaskRunIntent, TaskRunWaiter, resolve_task_params,
};
use crate::config::TaskAutoRun;
use crate::duration::parse_duration;
use crate::task_state::TaskState;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

impl Runner {
    pub(in crate::runner) fn spawn_task_worker(
        &mut self,
        name: &str,
        task_cfg: crate::config::Task,
        params: HashMap<String, String>,
        mode: TaskRunMode,
        intent: TaskRunIntent,
    ) -> Result<u64, CommandError> {
        let Some(rt) = self.tasks.get_mut(name) else {
            return Err(CommandError::UnknownTask {
                name: name.to_string(),
            });
        };
        rt.run_generation = rt.run_generation.saturating_add(1);
        let op_id = rt.run_generation;

        let cmd_tx = self.internal_tx.clone();
        let base_dir = self.base_dir.clone();
        let platform = self.platform;
        let emitter = self.output_manager.clone_lifecycle_emitter();
        let name_owned = name.to_string();
        let task_cfg_for_worker = task_cfg.clone();
        let global_watch_ignore = self.config.watch_ignore.clone();
        let terminal_coordinator = self.terminal_coordinator.clone();
        if task_cfg.terminal.is_foreground() {
            self.output_manager.pause_visible_output();
        }
        let worker = tokio::spawn(async move {
            let ctx = TaskWorkerContext {
                base_dir,
                platform,
                emitter,
                global_watch_ignore,
                terminal_coordinator,
            };
            let result =
                run_task_worker(ctx, &name_owned, &task_cfg_for_worker, &params, mode).await;
            let _ = cmd_tx
                .send(RunnerInternalCommand::TaskRunPrepared {
                    name: name_owned,
                    op_id,
                    task_cfg: Box::new(task_cfg),
                    intent,
                    result,
                })
                .await;
        });
        rt.run_worker = Some(worker);
        Ok(op_id)
    }

    pub(in crate::runner) async fn handle_task_run_prepared(
        &mut self,
        name: &str,
        op_id: u64,
        task_cfg: &crate::config::Task,
        intent: TaskRunIntent,
        result: Result<TaskRunPrepared, String>,
    ) {
        if self.shutting_down {
            self.stop_late_task_start(name.to_string(), result).await;
            if task_cfg.terminal.is_foreground() {
                self.output_manager.resume_visible_output();
                // Release the TUI even though we're shutting down — the
                // worker had already paused it before the foreground spawn.
                self.terminal_coordinator.release().await;
            }
            return;
        }
        let is_current = self
            .tasks
            .get(name)
            .is_some_and(|rt| rt.run_generation == op_id);
        if !is_current {
            match result {
                Ok(TaskRunPrepared::Spawned(spawn)) => {
                    let task::TaskSpawn {
                        handle,
                        child_output,
                        rendered_cmdline: _rendered_cmdline,
                    } = *spawn;
                    drop(child_output);
                    self.output_manager.service_event(
                        name,
                        &format!("send SIGKILL to stale task pgid {}", handle.pgid()),
                    );
                    tokio::spawn(async move {
                        let mut handle = handle;
                        let _ = handle
                            .terminate(
                                nix::sys::signal::Signal::SIGKILL,
                                std::time::Duration::from_millis(500),
                            )
                            .await;
                    });
                }
                Ok(TaskRunPrepared::ForegroundSpawned(spawn)) => {
                    let task::ForegroundTaskSpawn {
                        handle,
                        rendered_cmdline: _rendered_cmdline,
                    } = *spawn;
                    self.output_manager.service_event(
                        name,
                        &format!(
                            "send SIGKILL to stale foreground task pgid {}",
                            handle.pgid()
                        ),
                    );
                    tokio::spawn(async move {
                        let mut handle = handle;
                        let _ = handle
                            .terminate(
                                nix::sys::signal::Signal::SIGKILL,
                                std::time::Duration::from_millis(500),
                            )
                            .await;
                    });
                }
                Ok(TaskRunPrepared::PendingRun { .. })
                | Ok(TaskRunPrepared::Skipped { .. })
                | Err(_) => {}
            }
            if task_cfg.terminal.is_foreground() {
                self.output_manager.resume_visible_output();
                // For the ForegroundSpawned branch above, the worker had
                // already paused the TUI before the spawn; release it.
                // The TUI ignores Release while it's not paused, so this
                // is safe in the other branches too (where the worker
                // never acquired in the first place).
                self.terminal_coordinator.release().await;
            }
            return;
        }
        if let Some(rt) = self.tasks.get_mut(name) {
            rt.run_worker = None;
        }

        match result {
            Ok(TaskRunPrepared::PendingRun { message }) => {
                if task_cfg.terminal.is_foreground() {
                    self.output_manager.resume_visible_output();
                }
                if let Some(rt) = self.tasks.get_mut(name) {
                    rt.set_needs_run_now(true);
                }
                self.set_task_state(name, TaskItemState::PendingRun);
                self.output_manager.service_event(name, &message);
                if let TaskRunIntent::Scheduled { done_tx } = intent {
                    let _ = done_tx
                        .send(ItemDone {
                            name: name.to_string(),
                            kind: NodeKind::Task,
                            success: true,
                            message: None,
                            elapsed: None,
                            last_run: None,
                            service_start_generation: None,
                            task_run_generation: None,
                        })
                        .await;
                }
            }
            Ok(TaskRunPrepared::Skipped { message }) => {
                if task_cfg.terminal.is_foreground() {
                    self.output_manager.resume_visible_output();
                }
                if let Some(rt) = self.tasks.get_mut(name) {
                    rt.set_needs_run_now(false);
                }
                self.set_task_state(name, TaskItemState::Skipped);
                self.output_manager.service_debug_event(name, &message);
                if let TaskRunIntent::Scheduled { done_tx } = intent {
                    let _ = done_tx
                        .send(ItemDone {
                            name: name.to_string(),
                            kind: NodeKind::Task,
                            success: true,
                            message: None,
                            elapsed: None,
                            last_run: None,
                            service_start_generation: None,
                            task_run_generation: None,
                        })
                        .await;
                }
            }
            Ok(TaskRunPrepared::Spawned(spawn)) => {
                if matches!(intent, TaskRunIntent::Scheduled { .. })
                    && let Some(rt) = self.tasks.get_mut(name)
                {
                    rt.set_needs_run_now(true);
                }
                self.output_manager.service_debug_event(
                    name,
                    &format!("process spawned (pid {})", spawn.handle.pgid()),
                );
                self.output_manager
                    .service_event(name, &format!("spawn {}", spawn.rendered_cmdline));
                let done_tx = match intent {
                    TaskRunIntent::Scheduled { done_tx } => {
                        self.output_manager.service_event(name, "running...");
                        self.set_task_state(name, TaskItemState::Running);
                        Some(done_tx)
                    }
                    TaskRunIntent::Background => None,
                };
                self.wire_task_output_and_wait(name, *spawn, task_cfg, done_tx)
                    .await;
            }
            Ok(TaskRunPrepared::ForegroundSpawned(spawn)) => {
                if matches!(intent, TaskRunIntent::Scheduled { .. })
                    && let Some(rt) = self.tasks.get_mut(name)
                {
                    rt.set_needs_run_now(true);
                }
                let done_tx = match intent {
                    TaskRunIntent::Scheduled { done_tx } => {
                        self.set_task_state(name, TaskItemState::Running);
                        Some(done_tx)
                    }
                    TaskRunIntent::Background => None,
                };
                self.wire_foreground_task_and_wait(name, *spawn, task_cfg, done_tx)
                    .await;
            }
            Err(message) => {
                if task_cfg.terminal.is_foreground() {
                    self.output_manager.resume_visible_output();
                }
                if matches!(intent, TaskRunIntent::Scheduled { .. })
                    && let Some(rt) = self.tasks.get_mut(name)
                {
                    rt.set_needs_run_now(true);
                }
                self.set_task_state(name, TaskItemState::Failed);
                self.output_manager.service_error_event(name, &message);
                match intent {
                    TaskRunIntent::Scheduled { done_tx } => {
                        let _ = done_tx
                            .send(ItemDone {
                                name: name.to_string(),
                                kind: NodeKind::Task,
                                success: false,
                                message: Some(message),
                                elapsed: None,
                                last_run: None,
                                service_start_generation: None,
                                task_run_generation: None,
                            })
                            .await;
                    }
                    TaskRunIntent::Background => {
                        if let Some(rt) = self.tasks.get_mut(name)
                            && let Some(waiter) = rt.run_waiter.take()
                        {
                            waiter.complete(Err(CommandError::Failed {
                                name: name.to_string(),
                                message: message.clone(),
                            }));
                        }
                        let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                            name: name.to_string(),
                            success: false,
                        });
                    }
                }
            }
        }
    }

    /// Wire up a spawned task's output and wait for completion.
    ///
    /// Starts output capture, spawns a background task to wait for exit,
    /// records success in task state, and sends completion events.
    /// - If `done_tx` is `Some`, sends `ItemDone` (initial startup path).
    /// - If `done_tx` is `None`, sends `TaskRerunComplete` (file-watch rerun path).
    async fn wire_task_output_and_wait(
        &mut self,
        name: &str,
        spawn: task::TaskSpawn,
        task_cfg: &crate::config::Task,
        done_tx: Option<mpsc::Sender<ItemDone>>,
    ) {
        let task::TaskSpawn {
            mut handle,
            child_output,
            rendered_cmdline: _rendered_cmdline,
        } = spawn;

        let pgid = handle.pgid();

        // Add OSC response sink if we have a PTY write handle.
        if let Some(pty) = handle.take_pty_write()
            && let Some(osc_handle) = self.output_manager.add_osc_sink(name, pty).await
            && let Some(rt) = self.tasks.get_mut(name)
        {
            rt.osc_sink = Some(osc_handle);
        }

        if let Some(rt) = self.tasks.get_mut(name) {
            rt.pgid = Some(pgid);
        }

        // Fulfill any pending attach waiter for this task.
        self.fulfill_pending_waiter(name).await;

        let output_worker = self.output_manager.service_writer(name).map(|svc_writer| {
            tokio::spawn(async move {
                let _ = svc_writer.process_stream(child_output).await;
            })
        });
        if let Some(rt) = self.tasks.get_mut(name) {
            if let Some(old_worker) = rt.output_worker.take() {
                old_worker.abort();
            }
            rt.output_worker = output_worker;
        }

        let name_owned = name.to_string();
        let task_cfg_clone = task_cfg.clone();
        let base_dir_owned = self.base_dir.clone();
        let global_watch_ignore = self.config.watch_ignore.clone();
        let task_state = TaskState::new(base_dir_owned.join(".don").join("task-state"));
        let cmd_tx = self.internal_tx.clone();
        let rerun = done_tx.is_none();

        tokio::spawn(async move {
            let start = std::time::Instant::now();
            let result = task::wait_for_task(&mut handle, task_cfg_clone.timeout.as_deref()).await;
            let elapsed = start.elapsed();

            let (success, exit_code, message) = match result {
                Ok(status) => {
                    if status.success() {
                        (true, status.code(), None)
                    } else {
                        let code = status.code().unwrap_or(-1);
                        (false, status.code(), Some(format!("exit code {code}")))
                    }
                }
                Err(e) => (false, None, Some(e.to_string())),
            };
            let last_run = crate::task_state::TaskRunInfo::finished_now(
                success,
                Some(elapsed),
                exit_code,
                message.clone(),
            );
            if success {
                let task_dir = working_dir_for(&base_dir_owned, task_cfg_clone.dir.as_deref());
                let ignore_patterns = resolve_watch_ignore_patterns(
                    &task_dir,
                    &task_cfg_clone.ignore,
                    &base_dir_owned,
                    &global_watch_ignore,
                );
                let _ = task_state
                    .record_success_with_info(
                        &name_owned,
                        &task_cfg_clone.watch,
                        &ignore_patterns,
                        Some(&task_dir),
                        &last_run,
                    )
                    .await;
            } else {
                let _ = task_state.record_run(&name_owned, &last_run).await;
            }

            if let Some(done_tx) = done_tx {
                let _ = done_tx
                    .send(ItemDone {
                        name: name_owned,
                        kind: NodeKind::Task,
                        success,
                        message,
                        elapsed: Some(elapsed),
                        last_run: Some(last_run),
                        service_start_generation: None,
                        task_run_generation: None,
                    })
                    .await;
            } else {
                let _ = cmd_tx
                    .send(RunnerInternalCommand::TaskExited(super::TaskExit {
                        name: name_owned,
                        pgid,
                        success,
                        message,
                        elapsed: Some(elapsed),
                        last_run: Some(last_run),
                        rerun,
                    }))
                    .await;
            }
        });
    }

    async fn wire_foreground_task_and_wait(
        &mut self,
        name: &str,
        spawn: task::ForegroundTaskSpawn,
        task_cfg: &crate::config::Task,
        done_tx: Option<mpsc::Sender<ItemDone>>,
    ) {
        let task::ForegroundTaskSpawn {
            mut handle,
            rendered_cmdline: _rendered_cmdline,
        } = spawn;

        let pgid = handle.pgid();
        if let Some(rt) = self.tasks.get_mut(name) {
            rt.pgid = Some(pgid);
        }

        let name_owned = name.to_string();
        let task_cfg_clone = task_cfg.clone();
        let base_dir_owned = self.base_dir.clone();
        let global_watch_ignore = self.config.watch_ignore.clone();
        let task_state = TaskState::new(base_dir_owned.join(".don").join("task-state"));
        let cmd_tx = self.internal_tx.clone();
        let terminal_coordinator = self.terminal_coordinator.clone();
        let rerun = done_tx.is_none();

        tokio::spawn(async move {
            let start = std::time::Instant::now();
            let result =
                task::wait_for_foreground_task(&mut handle, task_cfg_clone.timeout.as_deref())
                    .await;
            let elapsed = start.elapsed();
            drop(handle);
            // Hand the terminal back to the TUI now that the child has
            // released it. Drop happened above; tcsetpgrp/tcsetattr already
            // restored pgrp + termios.
            terminal_coordinator.release().await;

            let (success, exit_code, message) = match result {
                Ok(status) => {
                    if status.success() {
                        (true, status.code(), None)
                    } else {
                        let code = status.code().unwrap_or(-1);
                        (false, status.code(), Some(format!("exit code {code}")))
                    }
                }
                Err(e) => (false, None, Some(e.to_string())),
            };
            let last_run = crate::task_state::TaskRunInfo::finished_now(
                success,
                Some(elapsed),
                exit_code,
                message.clone(),
            );
            if success {
                let task_dir = working_dir_for(&base_dir_owned, task_cfg_clone.dir.as_deref());
                let ignore_patterns = resolve_watch_ignore_patterns(
                    &task_dir,
                    &task_cfg_clone.ignore,
                    &base_dir_owned,
                    &global_watch_ignore,
                );
                let _ = task_state
                    .record_success_with_info(
                        &name_owned,
                        &task_cfg_clone.watch,
                        &ignore_patterns,
                        Some(&task_dir),
                        &last_run,
                    )
                    .await;
            } else {
                let _ = task_state.record_run(&name_owned, &last_run).await;
            }

            if let Some(done_tx) = done_tx {
                let _ = done_tx
                    .send(ItemDone {
                        name: name_owned,
                        kind: NodeKind::Task,
                        success,
                        message,
                        elapsed: Some(elapsed),
                        last_run: Some(last_run),
                        service_start_generation: None,
                        task_run_generation: None,
                    })
                    .await;
            } else {
                let _ = cmd_tx
                    .send(RunnerInternalCommand::TaskExited(super::TaskExit {
                        name: name_owned,
                        pgid,
                        success,
                        message,
                        elapsed: Some(elapsed),
                        last_run: Some(last_run),
                        rerun,
                    }))
                    .await;
            }
        });
    }

    async fn stop_task_pgid(&mut self, name: &str, pgid: i32) -> CommandResult {
        if self.remove_attach_lock(name) {
            self.output_manager.resume_stdout_sink(name).await;
        }
        if let Some(writer) = self.output_manager.service_writer(name) {
            writer.close_follow_sinks().await;
        }

        self.output_manager
            .service_event(name, "stopping... (requested)");
        self.output_manager
            .service_event(name, &format!("send SIGKILL to task pgid {pgid}"));

        match nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(pgid),
            nix::sys::signal::Signal::SIGKILL,
        ) {
            Ok(()) | Err(nix::Error::ESRCH) => {}
            Err(e) => {
                return Err(CommandError::Failed {
                    name: name.to_string(),
                    message: format!("failed to kill task pgid {pgid}: {e}"),
                });
            }
        }

        Ok(())
    }

    pub(in crate::runner) async fn handle_restart_task_cmd(&mut self, name: &str) -> CommandResult {
        if self.graph_cycle_owns_task(name) {
            return Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "task is running in dependency reconciliation".to_string(),
            });
        }
        let (task_cfg, last_params, state, pgid) = match self.tasks.get(name) {
            Some(rt) => (
                rt.config.clone(),
                rt.last_params.clone(),
                rt.state(),
                rt.pgid,
            ),
            None => {
                return Err(CommandError::UnknownTask {
                    name: name.to_string(),
                });
            }
        };

        if !task_cfg.params.is_empty() && last_params.len() < task_cfg.params.len() {
            return Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "task has params and no previous invocation to restart; use `don run`"
                    .to_string(),
            });
        }

        if matches!(state, TaskItemState::Running | TaskItemState::Building)
            && let Some(pgid) = pgid
        {
            self.stop_task_pgid(name, pgid).await?;
        }

        self.spawn_task_rerun(
            name,
            &task_cfg,
            &last_params,
            "restarting (manual trigger)",
            None,
        )
        .await;
        Ok(())
    }

    /// Handle a file-watch-triggered task re-run.
    ///
    /// Respects the task's auto-run policy — tasks that should not auto-rerun
    /// from a watch event transition to `PendingRun` instead of spawning.
    /// Explicit-run paths (the user triggering a task via `don run <name>` or
    /// `--all-pending`) bypass this gate by calling [`spawn_task_rerun`]
    /// directly.
    pub(in crate::runner) async fn handle_task_rerun(&mut self, name: &str) {
        let task_cfg = match self.tasks.get(name) {
            Some(rt) => rt.config.clone(),
            None => {
                self.output_manager
                    .service_error_event(name, "rerun requested for unknown task");
                let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                    name: name.to_string(),
                    success: false,
                });
                return;
            }
        };

        if self
            .tasks
            .get(name)
            .is_some_and(|rt| rt.state() == TaskItemState::Building)
        {
            let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                name: name.to_string(),
                success: true,
            });
            return;
        }

        // Skip the needs_run hash check — the file watcher already confirmed
        // a matching file changed. The hash check is only needed at startup
        // (to skip tasks whose inputs haven't changed since the last run).

        // Only `auto_run = true` / `"always"` allows watch-triggered reruns.
        // `"once"` is intentionally startup-only, and `false` / `"never"`
        // keeps the task manual forever.
        if !task_cfg.auto_run.runs_automatically_on_watch() {
            if let Some(rt) = self.tasks.get_mut(name) {
                rt.set_needs_run_now(true);
            }
            self.set_task_state(name, TaskItemState::PendingRun);
            let message = match task_cfg.auto_run {
                TaskAutoRun::Always => "files changed (pending)",
                TaskAutoRun::Never => "files changed (pending — auto_run = false)",
                TaskAutoRun::Once => "files changed (pending — auto_run = once)",
            };
            self.output_manager.service_event(name, message);
            let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                name: name.to_string(),
                success: true,
            });
            return;
        }

        // Tasks that declare params require user-supplied values. File-watch
        // triggers park them in PendingRun so the user can run them explicitly
        // (via the palette's form or `don run <task> --<param>=<value>`).
        if !task_cfg.params.is_empty() {
            if let Some(rt) = self.tasks.get_mut(name) {
                rt.set_needs_run_now(true);
            }
            self.set_task_state(name, TaskItemState::PendingRun);
            self.output_manager.service_event(
                name,
                "files changed (pending — task has params, run manually)",
            );
            let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                name: name.to_string(),
                success: true,
            });
            return;
        }

        self.spawn_task_rerun(
            name,
            &task_cfg,
            &HashMap::new(),
            "re-running (file changed)",
            None,
        )
        .await;
    }

    /// Actually spawn a task re-run: release any attach lock, flip to
    /// `Running`, spawn, and wire output. Used by both the file-watch path
    /// ([`handle_task_rerun`]) and the explicit-run paths (`don run <name>`,
    /// `don run --all-pending`).
    ///
    /// `params` is the user-supplied value map; empty for param-less tasks.
    /// Values are substituted into the task's `cmd`/`args`/`env`/`dir` via
    /// `{{name}}` placeholders in [`task::spawn_task`].
    async fn spawn_task_rerun(
        &mut self,
        name: &str,
        task_cfg: &crate::config::Task,
        params: &HashMap<String, String>,
        start_message: &str,
        wait_reply: Option<(oneshot::Sender<CommandResult>, Option<String>)>,
    ) {
        if task_cfg.terminal.is_foreground() && self.is_another_foreground_task_running(name) {
            if let Some(rt) = self.tasks.get_mut(name) {
                rt.last_params = params.clone();
                rt.set_needs_run_now(true);
            }
            self.set_task_state(name, TaskItemState::PendingRun);
            self.output_manager
                .service_event(name, "pending — another foreground task owns the terminal");
            if let Some(reply) = wait_reply {
                let _ = reply.0.send(Err(CommandError::InvalidState {
                    name: name.to_string(),
                    message: "another foreground task owns the terminal".to_string(),
                }));
            }
            return;
        }

        if let Some(rt) = self.tasks.get_mut(name) {
            if let Some(waiter) = rt.run_waiter.take() {
                waiter.complete(Err(CommandError::Failed {
                    name: name.to_string(),
                    message: "task run was superseded".to_string(),
                }));
            }
            rt.last_params = params.clone();
            rt.set_needs_run_now(true);
        }
        // Release attach lock and close follow sinks so any active attach
        // session exits cleanly before the new process starts.
        if self.remove_attach_lock(name) {
            self.output_manager.resume_stdout_sink(name).await;
        }
        if let Some(writer) = self.output_manager.service_writer(name) {
            writer.close_follow_sinks().await;
        }

        self.output_manager.service_event(name, start_message);
        self.set_task_state(name, TaskItemState::Running);

        self.output_manager
            .service_debug_event(name, "spawning process...");
        match self.spawn_task_worker(
            name,
            task_cfg.clone(),
            params.clone(),
            TaskRunMode::Triggered,
            TaskRunIntent::Background,
        ) {
            Ok(generation) => {
                if let Some((reply, timeout)) = wait_reply {
                    self.register_task_run_waiter(name, generation, reply, timeout);
                }
            }
            Err(e) => {
                self.set_task_state(name, TaskItemState::Failed);
                self.output_manager
                    .service_error_event(name, &format!("failed to start: {e}"));
                if let Some((reply, _)) = wait_reply {
                    let _ = reply.send(Err(CommandError::Failed {
                        name: name.to_string(),
                        message: format!("failed to start: {e}"),
                    }));
                }
                let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                    name: name.to_string(),
                    success: false,
                });
            }
        }
    }

    fn register_task_run_waiter(
        &mut self,
        name: &str,
        generation: u64,
        reply: oneshot::Sender<CommandResult>,
        timeout: Option<String>,
    ) {
        let timeout_task = timeout.as_ref().and_then(|timeout| {
            let duration = parse_duration(timeout).ok()?;
            let cmd_tx = self.internal_tx.clone();
            let name = name.to_string();
            let timeout = timeout.clone();
            Some(tokio::spawn(async move {
                tokio::time::sleep(duration).await;
                let _ = cmd_tx
                    .send(RunnerInternalCommand::TaskRunWaitTimedOut {
                        name,
                        generation,
                        timeout,
                    })
                    .await;
            }))
        });
        if let Some(rt) = self.tasks.get_mut(name) {
            rt.run_waiter = Some(TaskRunWaiter::new(generation, reply, timeout_task));
        }
    }

    pub(in crate::runner) fn handle_task_run_wait_timeout(
        &mut self,
        name: &str,
        generation: u64,
        timeout: &str,
    ) {
        let Some(rt) = self.tasks.get_mut(name) else {
            return;
        };
        let is_matching_waiter = rt
            .run_waiter
            .as_ref()
            .is_some_and(|waiter| waiter.generation() == generation);
        if !is_matching_waiter {
            return;
        }
        if let Some(waiter) = rt.run_waiter.take() {
            waiter.complete(Err(CommandError::TimedOut {
                name: name.to_string(),
                timeout: timeout.to_string(),
            }));
        }
    }

    fn is_another_foreground_task_running(&self, name: &str) -> bool {
        self.tasks.iter().any(|(task_name, rt)| {
            task_name != name
                && rt.config.terminal.is_foreground()
                && rt.state() == TaskItemState::Running
        })
    }

    /// Run all tasks currently in PendingRun state.
    pub(in crate::runner) async fn handle_run_pending_tasks(
        &mut self,
        reply: oneshot::Sender<CommandResult>,
    ) {
        let pending: Vec<(String, crate::config::Task)> = self
            .tasks
            .iter()
            .filter(|(_, rt)| rt.state() == TaskItemState::PendingRun)
            .map(|(name, rt)| (name.clone(), rt.config.clone()))
            .collect();

        if pending.is_empty() {
            self.output_manager
                .lifecycle_event("no pending tasks to run");
            let _ = reply.send(Ok(()));
            return;
        }

        // Param'd tasks can't be run here — they need user-supplied values.
        // Skip with a note so the user knows to use the palette or `don run`.
        let (runnable, needs_params): (Vec<_>, Vec<_>) = pending
            .into_iter()
            .partition(|(_, cfg)| cfg.params.is_empty());

        for (name, _) in &needs_params {
            self.output_manager
                .service_event(name, "skipped — task has params, run manually");
        }

        if runnable.is_empty() {
            self.output_manager
                .lifecycle_event("no pending tasks to run (param'd tasks skipped)");
            let _ = reply.send(Ok(()));
            return;
        }

        self.output_manager.lifecycle_event(&format!(
            "running {} pending task{}...",
            runnable.len(),
            if runnable.len() == 1 { "" } else { "s" }
        ));

        let empty_params = HashMap::new();
        for (name, cfg) in &runnable {
            // Explicit-run path — bypass the auto_run gate in handle_task_rerun.
            self.spawn_task_rerun(name, cfg, &empty_params, "running (manual trigger)", None)
                .await;
        }

        let _ = reply.send(Ok(()));
    }

    /// Run a single task by name, bypassing the `auto_run` gate. Used by
    /// `don run <name>`.
    pub(in crate::runner) async fn handle_run_task(
        &mut self,
        name: &str,
        params: HashMap<String, String>,
        wait: bool,
        wait_timeout: Option<String>,
        reply: oneshot::Sender<CommandResult>,
    ) {
        // Services and unknown names get a dedicated error. Services don't go
        // through "run" at all — that's what start/restart is for.
        if self.services.contains_key(name) {
            let _ = reply.send(Err(CommandError::NotATask {
                name: name.to_string(),
            }));
            return;
        }
        let cfg = match self.tasks.get(name) {
            Some(rt) => rt.config.clone(),
            None => {
                let _ = reply.send(Err(CommandError::UnknownTask {
                    name: name.to_string(),
                }));
                return;
            }
        };

        if self.graph_cycle_owns_task(name) {
            let _ = reply.send(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "task is running in dependency reconciliation".to_string(),
            }));
            return;
        }

        // Reject while already in flight — otherwise we'd race two spawns of
        // the same task and the output would interleave unpredictably.
        let already_in_flight = self.tasks.get(name).is_some_and(|rt| {
            matches!(rt.state(), TaskItemState::Running | TaskItemState::Building)
                || rt.run_worker.is_some()
        });
        if already_in_flight {
            let _ = reply.send(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "task is already running".to_string(),
            }));
            return;
        }

        // Resolve params: apply defaults, reject unknown keys, reject
        // missing required values, apply per-kind validation.
        let resolved = match resolve_task_params(name, &cfg, params) {
            Ok(p) => p,
            Err(message) => {
                let _ = reply.send(Err(CommandError::InvalidParams {
                    name: name.to_string(),
                    message,
                }));
                return;
            }
        };

        let wait = wait || wait_timeout.is_some();
        if let Some(timeout) = wait_timeout.as_deref()
            && let Err(e) = parse_duration(timeout)
        {
            let _ = reply.send(Err(CommandError::InvalidParams {
                name: name.to_string(),
                message: format!("invalid wait timeout: {e}"),
            }));
            return;
        }

        if cfg.reconcile_dependents && resolved.is_empty() {
            self.handle_manual_graph_cycle(name, wait, wait_timeout, reply)
                .await;
            return;
        }

        if wait {
            self.spawn_task_rerun(
                name,
                &cfg,
                &resolved,
                "running (manual trigger)",
                Some((reply, wait_timeout)),
            )
            .await;
        } else {
            self.spawn_task_rerun(name, &cfg, &resolved, "running (manual trigger)", None)
                .await;
            let _ = reply.send(Ok(()));
        }
    }
}
