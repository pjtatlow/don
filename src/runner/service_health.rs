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
        if !matches!(
            state,
            ServiceState::Running | ServiceState::Ready | ServiceState::Unhealthy
        ) {
            return;
        }
        let current_pgid = self.services.get(name).and_then(|rs| match &rs.handle {
            Some(ServiceHandle::Process(p)) => Some(p.pgid()),
            _ => None,
        });
        if current_pgid != Some(pgid) {
            return;
        }
        let mut handle = match self.services.get_mut(name).and_then(|rs| rs.handle.take()) {
            Some(h) => h,
            None => return,
        };
        let status = match &mut handle {
            ServiceHandle::Process(proc) => proc.wait().await.ok(),
            ServiceHandle::Docker(_) => None,
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

        let exit_msg = format_unexpected_exit(status);
        self.output_manager.service_error_event(name, &exit_msg);
        let is_lazy = self
            .services
            .get(name)
            .is_some_and(|rs| rs.resolved.lazy && rs.proxy.is_some());
        if is_lazy {
            // `wait` only reaps the direct child. Keep its ProcessHandle (and
            // therefore the PGID) available to lazy failure recovery so it can
            // kill and await any same-group descendants before the proxy is
            // re-armed. A descendant may still hold an inherited listenfd.
            if let Some(rs) = self.services.get_mut(name) {
                rs.handle = Some(handle);
            }
            // A lazy service restarts on a proxy connection, not on the
            // backoff timer, so route its crash through the connection-aware
            // crash-loop guard rather than scheduling an auto-restart. This
            // keeps a service that dies on launch from being relaunched in a
            // tight loop by its still-queued trigger connection.
            self.handle_lazy_launch_failure(name, Some(&exit_msg));
        } else {
            if let Some(rs) = self.services.get_mut(name) {
                rs.pgid = None;
            }
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
    /// do **not** re-arm the proxy trigger. Otherwise a detached cleanup first
    /// confirms the old process group has exited, drains the failed connection
    /// cohort, and only then returns the service to `Lazy`.
    ///
    /// A failed launch can surface twice — via the ready-check/`ItemDone` path
    /// and via the crash watcher. The state check makes this idempotent: the
    /// first caller transitions the service out of a live state, and the second
    /// sees the cleanup in `Stopping` and returns, so the streak counts one per
    /// launch.
    pub(in crate::runner) fn handle_lazy_launch_failure(
        &mut self,
        name: &str,
        message: Option<&str>,
    ) {
        let can_begin_recovery = self.services.get(name).is_some_and(|rs| {
            rs.control_worker.is_none()
                && matches!(
                    rs.state(),
                    ServiceState::Building
                        | ServiceState::Starting
                        | ServiceState::Running
                        | ServiceState::Ready
                        | ServiceState::Unhealthy
                )
        });
        if !can_begin_recovery {
            return;
        }
        let lived = self
            .services
            .get(name)
            .and_then(|rs| {
                if matches!(
                    rs.state(),
                    ServiceState::Running | ServiceState::Ready | ServiceState::Unhealthy
                ) {
                    rs.last_start
                } else {
                    None
                }
            })
            .map(|started| started.elapsed());
        let prior = self
            .services
            .get(name)
            .map(|rs| rs.rapid_crashes)
            .unwrap_or(0);
        let (rapid_crashes, give_up) = rapid_crash_outcome(lived, prior);
        let (handle, op_id) = match self.services.get_mut(name) {
            Some(rs) => {
                if rs.proxy.is_none() {
                    return;
                }
                rs.control_generation = rs.control_generation.saturating_add(1);
                let op_id = rs.control_generation;
                rs.stop_health_tracking();
                rs.rapid_crashes = rapid_crashes;
                rs.last_start = None;
                if let Some(prev) = rs.pending_restart.take() {
                    prev.abort();
                }
                rs.osc_sink = None;
                (rs.handle.take(), op_id)
            }
            None => return,
        };
        self.set_service_state(name, ServiceState::Stopping);
        let recovery = match self.services.get_mut(name).and_then(|rs| rs.proxy.as_mut()) {
            Some(proxy) => {
                proxy.clear_backend();
                proxy.begin_lazy_failure_recovery()
            }
            None => return,
        };

        if give_up {
            self.output_manager.service_error_event(
                name,
                &format!(
                    "crashed within {}s of starting {} times in a row — giving up; \
                     not re-arming the lazy trigger (run `don restart {name}` to retry)",
                    RAPID_CRASH_WINDOW.as_secs(),
                    rapid_crashes
                ),
            );
        } else if let Some(msg) = message {
            self.output_manager.service_error_event(
                name,
                &format!("{msg} (closing failed connections; will retry on next connection)"),
            );
        }

        let shutdown_config = self.effective_shutdown_config(name);
        let cmd_tx = self.internal_tx.clone();
        let name_owned = name.to_string();
        let debug = super::service::StopDebug::new(
            name_owned.clone(),
            self.output_manager.clone_lifecycle_emitter(),
        );
        let worker = tokio::spawn(async move {
            let stop_result = match handle {
                Some(handle) => super::service::stop_service(
                    handle,
                    Some(&shutdown_config),
                    true,
                    true,
                    Some(debug),
                )
                .await
                .map_err(|error| error.to_string()),
                None => Ok(()),
            };
            let result = match stop_result {
                Ok(()) => recovery.wait().await,
                Err(error) => Err(error),
            };
            let _ = cmd_tx
                .send(RunnerInternalCommand::LazyFailureRecoveryComplete {
                    name: name_owned,
                    op_id,
                    rearm: !give_up,
                    result,
                })
                .await;
        });
        if let Some(rs) = self.services.get_mut(name) {
            rs.control_worker = Some(worker);
        }
    }

    pub(in crate::runner) fn handle_lazy_failure_recovery_complete(
        &mut self,
        name: &str,
        op_id: u64,
        rearm: bool,
        result: Result<(), String>,
    ) {
        let is_current = self.services.get(name).is_some_and(|rs| {
            rs.control_generation == op_id && rs.state() == ServiceState::Stopping
        });
        if !is_current {
            return;
        }
        if let Some(rs) = self.services.get_mut(name) {
            rs.control_worker = None;
            rs.pgid = None;
        }
        match result {
            Ok(()) if rearm => {
                let barrier = self
                    .services
                    .get(name)
                    .and_then(|rs| rs.proxy.as_ref())
                    .map(|proxy| proxy.begin_lazy_rearm());
                let Some(barrier) = barrier else {
                    self.set_service_state(name, ServiceState::Failed);
                    self.output_manager.service_error_event(
                        name,
                        "failed to prepare lazy proxy: proxy is unavailable",
                    );
                    return;
                };
                let cmd_tx = self.internal_tx.clone();
                let name_owned = name.to_string();
                tokio::spawn(async move {
                    let result = barrier.wait().await;
                    let _ = cmd_tx
                        .send(RunnerInternalCommand::LazyProxyPrepareComplete {
                            name: name_owned,
                            op_id,
                            result,
                        })
                        .await;
                });
            }
            Ok(()) => self.set_service_state(name, ServiceState::Failed),
            Err(error) => {
                self.set_service_state(name, ServiceState::Failed);
                self.output_manager.service_error_event(
                    name,
                    &format!("failed to finish lazy launch cleanup: {error}"),
                );
            }
        }
    }

    pub(in crate::runner) fn handle_lazy_proxy_prepare_complete(
        &mut self,
        name: &str,
        op_id: u64,
        result: Result<(), String>,
    ) {
        let is_current = self.services.get(name).is_some_and(|rs| {
            rs.control_generation == op_id && rs.state() == ServiceState::Stopping
        });
        if !is_current {
            return;
        }
        match result {
            Ok(()) => self.set_service_state(name, ServiceState::Lazy),
            Err(error) => {
                self.set_service_state(name, ServiceState::Failed);
                self.output_manager
                    .service_error_event(name, &format!("failed to prepare lazy proxy: {error}"));
            }
        }
    }

    /// Handle a backoff-timer-fired auto-restart.
    pub(in crate::runner) async fn handle_auto_restart(&mut self, name: &str, attempt: u32) {
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
