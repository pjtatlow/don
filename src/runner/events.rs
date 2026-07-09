use super::support::format_duration;
use super::{
    CommandError, CommandResult, NodeKind, Runner, RunnerEvent, ServiceState, TaskItemState,
};

/// Completion notification from a spawned item.
pub(in crate::runner) struct ItemDone {
    pub(in crate::runner) name: String,
    pub(in crate::runner) kind: NodeKind,
    pub(in crate::runner) success: bool,
    pub(in crate::runner) message: Option<String>,
    /// How long the item took (for tasks).
    pub(in crate::runner) elapsed: Option<std::time::Duration>,
    /// Metadata for a task process that actually ran.
    pub(in crate::runner) last_run: Option<crate::task_state::TaskRunInfo>,
    /// Run generation for manually-triggered task completions that need to
    /// re-notify startup dependency resolution. `None` for normal startup
    /// item completions.
    pub(in crate::runner) task_run_generation: Option<u64>,
}

pub(in crate::runner) struct TaskExit {
    pub(in crate::runner) name: String,
    pub(in crate::runner) pgid: i32,
    pub(in crate::runner) success: bool,
    pub(in crate::runner) message: Option<String>,
    pub(in crate::runner) elapsed: Option<std::time::Duration>,
    pub(in crate::runner) last_run: Option<crate::task_state::TaskRunInfo>,
    pub(in crate::runner) rerun: bool,
}

impl Runner {
    /// Handle an item completion notification.
    pub(in crate::runner) fn handle_item_done(&mut self, item: &ItemDone) {
        match item.kind {
            NodeKind::Service => self.handle_service_done(item),
            NodeKind::Task => self.handle_task_done(item),
        }
    }

    fn handle_service_done(&mut self, item: &ItemDone) {
        if item.success {
            let message = self
                .services
                .get(&item.name)
                .map(|rs| match &rs.resolved.ready {
                    Some(r) if r.tcp.is_some() => {
                        format!("ready (tcp {})", r.tcp.as_deref().unwrap_or("unknown"))
                    }
                    Some(r) if r.http.is_some() => {
                        format!("ready (http {})", r.http.as_deref().unwrap_or("unknown"))
                    }
                    Some(r) if r.exec.is_some() => "ready (exec)".to_string(),
                    _ => "started".to_string(),
                });
            // Activate proxy backend before state flip so the proxy is ready
            // to forward the moment observers see `Ready`.
            if let Some(rs) = self.services.get(&item.name)
                && let Some(ref proxy) = rs.proxy
            {
                proxy.set_backend();
            }
            // Reaching Ready resets the backoff counter, but not the
            // rapid-crash streak — see `handle_service_exited`, which clears
            // that only once the process has survived past the crash window.
            if let Some(rs) = self.services.get_mut(&item.name) {
                if let Some(handle) = rs.pending_restart.take() {
                    handle.abort();
                }
                rs.restart_attempts = 0;
            }
            self.set_service_state(&item.name, ServiceState::Ready);
            self.unblock_dependency_failed_items();
            if let Some(message) = message {
                self.output_manager.service_event(&item.name, &message);
            }
        } else {
            // If a lazy service fails, reset to Lazy so the next connection
            // can re-trigger it instead of leaving it permanently failed.
            let is_lazy = self
                .services
                .get(&item.name)
                .is_some_and(|rs| rs.resolved.lazy && rs.proxy.is_some());
            if is_lazy {
                // Route through the crash-loop guard: returns to `Lazy` and
                // re-arms the proxy trigger normally, but gives up (leaving it
                // `Failed`, trigger un-armed) once it has crashed on launch too
                // many times in a row — otherwise a still-queued connection
                // relaunches a dying service in a tight, no-backoff loop.
                self.handle_lazy_launch_failure(&item.name, item.message.as_deref());
            } else {
                self.set_service_state(&item.name, ServiceState::Failed);
                if let Some(ref msg) = item.message {
                    self.output_manager.service_error_event(&item.name, msg);
                }
                let policy = self
                    .services
                    .get(&item.name)
                    .map(|rs| rs.resolved.on_failure)
                    .unwrap_or_default();
                if matches!(policy, crate::config::OnFailure::Restart) {
                    let reason = item
                        .message
                        .as_deref()
                        .unwrap_or("service failed before becoming ready");
                    self.schedule_auto_restart(&item.name, reason, true);
                }
            }
        }
    }

    fn handle_task_done(&mut self, item: &ItemDone) {
        if self
            .tasks
            .get(&item.name)
            .is_some_and(|rt| rt.config.terminal.is_foreground())
        {
            self.output_manager.resume_visible_output();
        }
        if let Some(task_generation) = item.task_run_generation
            && self
                .tasks
                .get(&item.name)
                .is_some_and(|rt| rt.run_generation != task_generation)
        {
            return;
        }
        if let Some(rt) = self.tasks.get_mut(&item.name)
            && rt.pgid.take().is_some()
        {
            // Release attach lock if held.
            rt.attach_lock = None;
            // Can't await here (sync fn), but the stdout sink resume
            // will happen naturally when the follow sink closes.
        }
        let timing = item.elapsed.map(format_duration).unwrap_or_default();

        if item.success {
            let cur = self.tasks.get(&item.name).map(|rt| rt.state());
            if cur != Some(TaskItemState::Skipped)
                && cur != Some(TaskItemState::PendingRun)
                && let Some(rt) = self.tasks.get_mut(&item.name)
            {
                rt.mark_success();
                rt.last_run = item.last_run.clone();
            }
            if cur != Some(TaskItemState::Skipped)
                && cur != Some(TaskItemState::PendingRun)
                && cur != Some(TaskItemState::Completed)
            {
                self.set_task_state(&item.name, TaskItemState::Completed);
                let msg = if timing.is_empty() {
                    "complete".to_string()
                } else {
                    format!("complete ({timing})")
                };
                self.output_manager.service_event(&item.name, &msg);
            }
            self.unblock_dependency_failed_items();
        } else {
            if let Some(rt) = self.tasks.get_mut(&item.name) {
                rt.set_needs_run_now(true);
                rt.last_run = item.last_run.clone();
            }
            self.set_task_state(&item.name, TaskItemState::Failed);
            if let Some(ref err_msg) = item.message {
                let msg = if timing.is_empty() {
                    format!("failed ({err_msg})")
                } else {
                    format!("failed ({err_msg}, {timing})")
                };
                self.output_manager.service_error_event(&item.name, &msg);
            }
        }
    }

    pub(in crate::runner) async fn handle_task_exit(&mut self, exit: TaskExit) {
        let TaskExit {
            name,
            pgid,
            success,
            message,
            elapsed,
            last_run,
            rerun,
        } = exit;
        let name = name.as_str();
        if self.tasks.get(name).is_none_or(|rt| rt.pgid != Some(pgid)) {
            return;
        }

        if self
            .tasks
            .get(name)
            .is_some_and(|rt| rt.config.terminal.is_foreground())
        {
            self.output_manager.resume_visible_output();
        }

        // Release any active attach session: restore the stdout sink, then
        // close the transient follow sinks so a bridged client (attach or a
        // foreground `don run`) sees the stream end instead of hanging on an
        // exited task.
        if self.remove_attach_lock(name) {
            self.output_manager.resume_stdout_sink(name).await;
        }
        if let Some(writer) = self.output_manager.service_writer(name) {
            writer.close_follow_sinks().await;
        }
        if let Some(rt) = self.tasks.get_mut(name) {
            rt.pgid = None;
        }

        let timing = elapsed.map(format_duration).unwrap_or_default();
        let wait_result: CommandResult = if success {
            Ok(())
        } else {
            Err(CommandError::Failed {
                name: name.to_string(),
                message: message.clone().unwrap_or_else(|| "task failed".to_string()),
            })
        };
        if success {
            if let Some(rt) = self.tasks.get_mut(name) {
                rt.mark_success();
                rt.last_run = last_run;
            }
            self.set_task_state(name, TaskItemState::Completed);
            self.unblock_dependency_failed_items();
            let msg = if timing.is_empty() {
                "complete".to_string()
            } else {
                format!("complete ({timing})")
            };
            self.output_manager.service_event(name, &msg);
            let run_generation = self.tasks.get(name).map(|rt| rt.run_generation);
            if let Some(done_tx) = self.done_tx.clone() {
                let name = name.to_string();
                tokio::spawn(async move {
                    let _ = done_tx
                        .send(ItemDone {
                            name,
                            kind: NodeKind::Task,
                            success: true,
                            message: None,
                            elapsed: None,
                            last_run: None,
                            task_run_generation: run_generation,
                        })
                        .await;
                });
            }
        } else {
            if let Some(rt) = self.tasks.get_mut(name) {
                rt.set_needs_run_now(true);
                rt.last_run = last_run;
            }
            self.set_task_state(name, TaskItemState::Failed);
            if let Some(ref err_msg) = message {
                let msg = if timing.is_empty() {
                    format!("failed ({err_msg})")
                } else {
                    format!("failed ({err_msg}, {timing})")
                };
                self.output_manager.service_error_event(name, &msg);
            }
        }

        if rerun {
            let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                name: name.to_string(),
                success,
            });
        }
        if let Some(rt) = self.tasks.get_mut(name)
            && rt
                .run_waiter
                .as_ref()
                .is_some_and(|waiter| waiter.generation() == rt.run_generation)
            && let Some(waiter) = rt.run_waiter.take()
        {
            waiter.complete(wait_result);
        }
    }
}
