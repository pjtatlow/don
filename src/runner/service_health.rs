use super::health::{format_unexpected_exit, unhealthy_restart_backoff_secs};
use super::service::ServiceHandle;
use super::service_worker::ServiceStartMode;
use super::{Runner, RunnerInternalCommand, ServiceState};
use std::time::Duration;
use tokio::sync::oneshot;

const MAX_STARTUP_FAILURES_BEFORE_GIVE_UP: u32 = 3;

/// A process that exits within this window of being started is treated as a
/// crash on launch (a likely crash loop) rather than a normal failure.
const RAPID_CRASH_WINDOW: Duration = Duration::from_secs(5);

/// Maximum number of back-to-back rapid crashes before don gives up
/// auto-restarting a service, regardless of `on_failure`. Two strikes: the
/// initial start plus one retry that also dies inside [`RAPID_CRASH_WINDOW`].
const MAX_RAPID_CRASHES: u32 = 2;

/// Update the rapid-crash streak after a non-clean process exit.
///
/// `lived` is how long the process ran since its last start (`None` when that
/// is unknown, treated as an immediate crash). Returns the new streak count
/// and whether don should give up instead of scheduling another auto-restart.
/// A process that ran at least [`RAPID_CRASH_WINDOW`] clears the streak — it
/// wasn't stuck in a tight crash loop.
fn rapid_crash_outcome(lived: Option<Duration>, prior: u32) -> (u32, bool) {
    let rapid = lived.map(|d| d < RAPID_CRASH_WINDOW).unwrap_or(true);
    if !rapid {
        return (0, false);
    }
    let count = prior.saturating_add(1);
    (count, count >= MAX_RAPID_CRASHES)
}

impl Runner {
    /// Apply a health-monitor probe transition for a service.
    ///
    /// Only acts when the service is in `Ready` (failure -> `Unhealthy`)
    /// or `Unhealthy` (recovery -> `Ready`). Stale messages from a monitor
    /// task whose service has since stopped/restarted are ignored.
    pub(in crate::runner) async fn handle_service_health_changed(
        &mut self,
        name: &str,
        healthy: bool,
    ) {
        let current = match self.services.get(name) {
            Some(rs) => rs.state(),
            None => return,
        };
        if healthy {
            if current != ServiceState::Unhealthy {
                return;
            }
            self.set_service_state(name, ServiceState::Ready);
            let attempts = self
                .services
                .get(name)
                .map(|rs| rs.restart_attempts)
                .unwrap_or(0);
            // Health recovery resets the backoff counter only; the
            // rapid-crash streak is cleared by the lifetime check on the next
            // actual crash, not by a transient return to Ready.
            if let Some(rs) = self.services.get_mut(name) {
                if let Some(handle) = rs.pending_restart.take() {
                    handle.abort();
                }
                rs.restart_attempts = 0;
            }
            let msg = if attempts > 0 {
                "recovered (cancelled pending restart, attempts reset)"
            } else {
                "recovered (health check passing)"
            };
            self.output_manager.service_event(name, msg);
        } else {
            if current != ServiceState::Ready {
                return;
            }
            self.set_service_state(name, ServiceState::Unhealthy);
            let policy = self
                .services
                .get(name)
                .map(|rs| rs.resolved.on_failure)
                .unwrap_or_default();
            match policy {
                crate::config::OnFailure::Notify => {
                    self.output_manager
                        .service_error_event(name, "unhealthy (health check failing)");
                }
                crate::config::OnFailure::Restart => {
                    self.schedule_auto_restart(name, "unhealthy", false);
                }
            }
        }
    }

    /// Schedule an automatic restart for a failed service. Used for both
    /// `Unhealthy` (monitor-driven) and `Failed` (crash-driven) failures.
    /// Uses exponential backoff (1, 2, 4, 8, 16, 32, 60s) on consecutive
    /// attempts. Replaces any already-scheduled restart for this service.
    /// `reason` is included verbatim in the lifecycle event so a reader
    /// can tell why the restart was scheduled.
    pub(in crate::runner) fn schedule_auto_restart(
        &mut self,
        name: &str,
        reason: &str,
        limit_startup_failures: bool,
    ) {
        if self.graph_cycle_owns_service(name) {
            return;
        }
        let attempt = self
            .services
            .get(name)
            .map(|rs| rs.restart_attempts.saturating_add(1))
            .unwrap_or(1);
        if limit_startup_failures && attempt >= MAX_STARTUP_FAILURES_BEFORE_GIVE_UP {
            if let Some(rs) = self.services.get_mut(name) {
                rs.restart_attempts = attempt;
                if let Some(prev) = rs.pending_restart.take() {
                    prev.abort();
                }
            }
            self.output_manager.service_error_event(
                name,
                &format!(
                    "{reason} — giving up after {attempt} failed starts without becoming ready"
                ),
            );
            return;
        }
        let backoff_secs = unhealthy_restart_backoff_secs(attempt);
        self.output_manager.service_error_event(
            name,
            &format!("{reason} — auto-restart in {backoff_secs}s (attempt {attempt})"),
        );
        let cmd_tx = self.internal_tx.clone();
        let name_owned = name.to_string();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
            let _ = cmd_tx
                .send(RunnerInternalCommand::AutoRestart {
                    name: name_owned,
                    attempt,
                })
                .await;
        });
        if let Some(rs) = self.services.get_mut(name) {
            rs.restart_attempts = attempt;
            if let Some(prev) = rs.pending_restart.replace(handle) {
                prev.abort();
            }
        }
    }

    /// Handle an unexpected exit reported by the per-spawn crash watcher.
    ///
    /// The watcher fires whenever the child's output stream EOFs. That happens
    /// for both crashes and graceful stops, so the handler filters stale/known
    /// stop paths before reaping and applying the on_failure policy.
    pub(in crate::runner) async fn handle_service_exited(&mut self, name: &str, pgid: i32) {
        let state = match self.services.get(name) {
            Some(rs) => rs.state(),
            None => return,
        };
        if !state.is_live() {
            return;
        }
        let current_pgid = self.services.get(name).and_then(|rs| match &rs.handle {
            Some(ServiceHandle::Process(p)) => Some(p.pgid()),
            _ => None,
        });
        if current_pgid != Some(pgid) {
            return;
        }
        let handle = match self.services.get_mut(name).and_then(|rs| rs.handle.take()) {
            Some(h) => h,
            None => return,
        };
        let status = if let ServiceHandle::Process(mut proc) = handle {
            proc.wait().await.ok()
        } else {
            None
        };
        if let Some(rs) = self.services.get_mut(name) {
            rs.stop_health_tracking();
        }

        let clean_exit = status.as_ref().is_some_and(|s| s.success());
        if clean_exit {
            if let Some(rs) = self.services.get_mut(name) {
                rs.reset_restart_tracking();
                rs.pgid = None;
            }
            self.set_service_state(name, ServiceState::Stopped);
            self.output_manager
                .service_event(name, "exited cleanly (status 0)");
            if let Some(writer) = self.output_manager.service_writer(name) {
                writer.close_follow_sinks().await;
            }
            return;
        }

        if let Some(rs) = self.services.get_mut(name) {
            rs.pgid = None;
        }
        let exit_msg = format_unexpected_exit(status);
        self.output_manager.service_error_event(name, &exit_msg);
        if self.graph_cycle_owns_service(name) {
            self.set_service_state(name, ServiceState::Failed);
            return;
        }
        let is_lazy = self
            .services
            .get(name)
            .is_some_and(|rs| rs.resolved.lazy && rs.proxy.is_some());
        if is_lazy {
            // A lazy service restarts on a proxy connection, not on the
            // backoff timer, so route its crash through the connection-aware
            // crash-loop guard rather than scheduling an auto-restart. This
            // keeps a service that dies on launch from being relaunched in a
            // tight loop by its still-queued trigger connection.
            self.handle_lazy_launch_failure(name, Some(&exit_msg));
        } else {
            self.set_service_state(name, ServiceState::Failed);
            let policy = self
                .services
                .get(name)
                .map(|rs| rs.resolved.on_failure)
                .unwrap_or_default();
            if matches!(policy, crate::config::OnFailure::Restart) {
                // Crash-loop guard: a process that dies within
                // RAPID_CRASH_WINDOW of starting is failing on launch. After
                // MAX_RAPID_CRASHES such back-to-back fast deaths, stop
                // retrying and leave it Failed — a hard ceiling that no backoff
                // or `on_failure` policy overrides.
                let lived = self
                    .services
                    .get(name)
                    .and_then(|rs| rs.last_start)
                    .map(|started| started.elapsed());
                let prior = self
                    .services
                    .get(name)
                    .map(|rs| rs.rapid_crashes)
                    .unwrap_or(0);
                let (rapid_crashes, give_up) = rapid_crash_outcome(lived, prior);
                if let Some(rs) = self.services.get_mut(name) {
                    rs.rapid_crashes = rapid_crashes;
                }
                if give_up {
                    if let Some(rs) = self.services.get_mut(name)
                        && let Some(prev) = rs.pending_restart.take()
                    {
                        prev.abort();
                    }
                    self.output_manager.service_error_event(
                        name,
                        &format!(
                            "crashed within {}s of starting {} times in a row — giving up (not auto-restarting)",
                            RAPID_CRASH_WINDOW.as_secs(),
                            rapid_crashes
                        ),
                    );
                } else {
                    self.schedule_auto_restart(name, &exit_msg, state == ServiceState::Running);
                }
            } else if let Some(rs) = self.services.get_mut(name) {
                rs.reset_restart_tracking();
            }
        }
        if let Some(writer) = self.output_manager.service_writer(name) {
            writer.close_follow_sinks().await;
        }
    }

    /// Route a lazy service's failed launch through the crash-loop guard.
    ///
    /// Lazy services restart on proxy connections, not on the auto-restart
    /// backoff timer, so [`Self::handle_service_exited`]'s guard doesn't bound
    /// them: a connection the dying service never accepts stays queued and
    /// re-fires the launch the instant the proxy re-arms — a tight, no-backoff
    /// crash loop. This applies the same rapid-crash ceiling. Each failed
    /// launch bumps the streak (cleared by a launch that survives
    /// [`RAPID_CRASH_WINDOW`]); once it trips we leave the service `Failed` and
    /// do **not** re-arm the proxy trigger, so the queued connection stops
    /// relaunching it. Otherwise the service returns to `Lazy` and re-arms so a
    /// later connection can retry.
    ///
    /// A failed launch can surface twice — via the ready-check/`ItemDone` path
    /// and via the crash watcher. The state check makes this idempotent: the
    /// first caller transitions the service out of a live state, and the second
    /// sees `Lazy`/`Failed` and returns, so the streak counts one per launch.
    pub(in crate::runner) fn handle_lazy_launch_failure(
        &mut self,
        name: &str,
        message: Option<&str>,
    ) {
        if !matches!(
            self.services.get(name).map(|rs| rs.state()),
            Some(ServiceState::Running | ServiceState::Ready | ServiceState::Unhealthy)
        ) {
            return;
        }
        let lived = self
            .services
            .get(name)
            .and_then(|rs| rs.last_start)
            .map(|started| started.elapsed());
        let prior = self
            .services
            .get(name)
            .map(|rs| rs.rapid_crashes)
            .unwrap_or(0);
        let (rapid_crashes, give_up) = rapid_crash_outcome(lived, prior);
        if let Some(rs) = self.services.get_mut(name) {
            rs.rapid_crashes = rapid_crashes;
            if let Some(prev) = rs.pending_restart.take() {
                prev.abort();
            }
            // Drop the failed launch's process handle and OSC sink. If a ready
            // check failed while the process was still alive (e.g. it never
            // bound its port), nothing else stops it — without this it lingers
            // running and, via the OSC sink, holds the PTY master open. The
            // handle's `kill_on_drop` reaps the process and the `OscSinkHandle`
            // drop releases the PTY. On the crash path the handle is already
            // gone, so these are no-ops. The output worker drains and drops the
            // read half on EOF once the process is gone.
            rs.handle = None;
            rs.osc_sink = None;
        }
        if give_up {
            self.set_service_state(name, ServiceState::Failed);
            self.output_manager.service_error_event(
                name,
                &format!(
                    "crashed within {}s of starting {} times in a row — giving up; \
                     not re-arming the lazy trigger (run `don restart {name}` to retry)",
                    RAPID_CRASH_WINDOW.as_secs(),
                    rapid_crashes
                ),
            );
        } else {
            self.set_service_state(name, ServiceState::Lazy);
            self.unblock_dependency_failed_items();
            if let Some(rs) = self.services.get_mut(name)
                && let Some(ref mut proxy) = rs.proxy
            {
                proxy.rearm_lazy_watchers();
            }
            if let Some(msg) = message {
                self.output_manager
                    .service_error_event(name, &format!("{msg} (will retry on next connection)"));
            }
        }
    }

    /// Handle a backoff-timer-fired auto-restart.
    pub(in crate::runner) async fn handle_auto_restart(&mut self, name: &str, attempt: u32) {
        if self.graph_cycle_owns_service(name) {
            return;
        }
        let state = match self.services.get(name) {
            Some(rs) => rs.state(),
            None => return,
        };
        if !matches!(state, ServiceState::Unhealthy | ServiceState::Failed) {
            return;
        }
        if let Some(rs) = self.services.get_mut(name) {
            rs.pending_restart = None;
        }
        self.output_manager
            .service_event(name, &format!("auto-restart firing (attempt {attempt})"));
        if self
            .services
            .get(name)
            .is_some_and(|rs| rs.handle.is_some())
        {
            let (reply_tx, _reply_rx) = oneshot::channel();
            self.handle_auto_restart_running_service(name, reply_tx)
                .await;
        } else {
            let _ = self.queue_background_service_start(name, ServiceStartMode::Full);
        }
    }

    async fn handle_auto_restart_running_service(
        &mut self,
        name: &str,
        reply: oneshot::Sender<super::CommandResult>,
    ) {
        if let Some(rs) = self.services.get_mut(name) {
            rs.stop_health_tracking();
        }
        let handle = match self.services.get_mut(name).and_then(|rs| rs.handle.take()) {
            Some(h) => h,
            None => {
                let _ =
                    reply.send(self.queue_background_service_start(name, ServiceStartMode::Full));
                return;
            }
        };
        let shutdown_config = self.effective_shutdown_config(name);
        if self.remove_attach_lock(name) {
            self.output_manager.resume_stdout_sink(name).await;
        }
        self.set_service_state(name, ServiceState::Stopping);
        self.output_manager
            .service_event(name, "stopping... (auto-restart)");
        self.spawn_manual_service_stop_worker(
            name,
            handle,
            shutdown_config,
            false,
            reply,
            super::ServiceStopAction::RestartFull,
        );
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::{MAX_RAPID_CRASHES, RAPID_CRASH_WINDOW, rapid_crash_outcome};
    use std::time::Duration;

    #[test]
    fn rapid_crash_outcome_streak_and_give_up() {
        struct Case {
            name: &'static str,
            lived: Option<Duration>,
            prior: u32,
            expect_count: u32,
            expect_give_up: bool,
        }

        let just_under = RAPID_CRASH_WINDOW - Duration::from_millis(1);
        let cases = vec![
            Case {
                name: "first fast crash, unknown lifetime",
                lived: None,
                prior: 0,
                expect_count: 1,
                expect_give_up: false,
            },
            Case {
                name: "first fast crash",
                lived: Some(Duration::from_millis(200)),
                prior: 0,
                expect_count: 1,
                expect_give_up: false,
            },
            Case {
                name: "second fast crash hits the cap",
                lived: Some(Duration::from_millis(200)),
                prior: 1,
                expect_count: MAX_RAPID_CRASHES,
                expect_give_up: true,
            },
            Case {
                name: "just inside the window still counts",
                lived: Some(just_under),
                prior: 1,
                expect_count: 2,
                expect_give_up: true,
            },
            Case {
                name: "exactly at the window clears the streak",
                lived: Some(RAPID_CRASH_WINDOW),
                prior: 1,
                expect_count: 0,
                expect_give_up: false,
            },
            Case {
                name: "long-lived crash clears a large streak",
                lived: Some(Duration::from_secs(60)),
                prior: 5,
                expect_count: 0,
                expect_give_up: false,
            },
            Case {
                name: "unknown lifetime past the cap gives up",
                lived: None,
                prior: MAX_RAPID_CRASHES,
                expect_count: MAX_RAPID_CRASHES + 1,
                expect_give_up: true,
            },
        ];

        for case in cases {
            let (count, give_up) = rapid_crash_outcome(case.lived, case.prior);
            assert_eq!(count, case.expect_count, "{}: count", case.name);
            assert_eq!(give_up, case.expect_give_up, "{}: give_up", case.name);
        }
    }
}
