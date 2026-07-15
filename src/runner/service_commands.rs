use super::service::{self, ServiceHandle};
use super::service_worker::{
    ServiceStartContext, ServiceStartMode, run_service_build_worker, start_service_worker,
};
use super::{
    CommandError, CommandResult, ItemDone, NodeKind, Runner, RunnerEvent, RunnerInternalCommand,
    ServiceStartIntent, ServiceState, ServiceStopAction,
};
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

impl Runner {
    fn lookup_service(&self, name: &str) -> Result<&crate::config::Service, CommandError> {
        if let Some(svc) = self.config.services.get(name) {
            return Ok(svc);
        }
        if self.config.tasks.contains_key(name) {
            return Err(CommandError::NotAService {
                name: name.to_string(),
            });
        }
        Err(CommandError::UnknownService {
            name: name.to_string(),
        })
    }

    fn service_start_snapshot(&self, name: &str) -> Result<ServiceStartContext, CommandError> {
        let mut resolved = match self.services.get(name) {
            Some(rs) => rs.resolved.clone(),
            None => {
                return Err(CommandError::UnknownService {
                    name: name.to_string(),
                });
            }
        };
        let (listen_fds, listen_fds_env) = if let Some(rs) = self.services.get(name)
            && let Some(ref proxy) = rs.proxy
        {
            resolved.env.extend(proxy.env_vars());
            (proxy.listenfd_raw_fds(), proxy.listenfd_env())
        } else {
            (Vec::new(), HashMap::new())
        };
        let batch_built = self.services.get(name).is_some_and(|rs| rs.batch_built);
        Ok(ServiceStartContext {
            resolved,
            batch_built,
            listen_fds,
            listen_fds_env,
        })
    }

    fn spawn_service_start_worker(
        &mut self,
        name: &str,
        context: ServiceStartContext,
        mode: ServiceStartMode,
        intent: ServiceStartIntent,
    ) -> Result<(), CommandError> {
        let Some(rs) = self.services.get_mut(name) else {
            return Err(CommandError::UnknownService {
                name: name.to_string(),
            });
        };
        rs.start_generation = rs.start_generation.saturating_add(1);
        let op_id = rs.start_generation;

        let cmd_tx = self.internal_tx.clone();
        let name_owned = name.to_string();
        let base_dir = self.base_dir.clone();
        let pid_dir = self.base_dir.join(".don").join("pids");
        let platform = self.platform;
        let emitter = self.output_manager.clone_lifecycle_emitter();
        let service_writer = self.output_manager.service_writer(name);
        let docker_client = self.docker_client.clone();
        let context_for_worker = context.clone();

        let worker = tokio::spawn(async move {
            let result = start_service_worker(
                &base_dir,
                &pid_dir,
                platform,
                docker_client.as_ref(),
                &emitter,
                &name_owned,
                &context_for_worker,
                mode,
                service_writer.as_ref(),
            )
            .await;

            let _ = cmd_tx
                .send(RunnerInternalCommand::ServiceStartPrepared {
                    name: name_owned,
                    op_id,
                    context: Box::new(context),
                    intent,
                    result: result.map(Box::new),
                })
                .await;
        });
        rs.start_worker = Some(worker);
        Ok(())
    }

    pub(in crate::runner) async fn handle_service_start_prepared(
        &mut self,
        name: &str,
        op_id: u64,
        context: Box<ServiceStartContext>,
        intent: ServiceStartIntent,
        result: Result<Box<service::StartResult>, String>,
    ) {
        if self.shutting_down {
            self.stop_late_service_start(name.to_string(), context, result)
                .await;
            return;
        }
        let is_current = self
            .services
            .get(name)
            .is_some_and(|rs| rs.start_generation == op_id);
        if !is_current {
            if let Ok(start_result) = result {
                let shutdown_config = context
                    .resolved
                    .shutdown
                    .clone()
                    .map(|shutdown| shutdown.merged_over(&self.config.shutdown))
                    .unwrap_or_else(|| self.config.shutdown.clone());
                let debug = service::StopDebug::new(
                    name.to_string(),
                    self.output_manager.clone_lifecycle_emitter(),
                );
                tokio::spawn(async move {
                    let start_result = *start_result;
                    let service::StartResult {
                        handle,
                        child_output,
                    } = start_result;
                    drop(child_output);
                    let _ = service::stop_service(
                        handle,
                        Some(&shutdown_config),
                        true,
                        false,
                        Some(debug),
                    )
                    .await;
                });
            }
            return;
        }
        if let Some(rs) = self.services.get_mut(name) {
            rs.start_worker = None;
        }

        match result {
            Ok(start_result) => match intent {
                ServiceStartIntent::Scheduled { done_tx } => {
                    self.wire_service_output_and_ready_check(
                        name,
                        op_id,
                        *start_result,
                        &context.resolved,
                        Some(done_tx),
                    )
                    .await;
                }
                ServiceStartIntent::Reply { reply } => {
                    self.wire_service_output_and_ready_check(
                        name,
                        op_id,
                        *start_result,
                        &context.resolved,
                        None,
                    )
                    .await;
                    let _ = reply.send(Ok(()));
                }
                ServiceStartIntent::Background => {
                    self.wire_service_output_and_ready_check(
                        name,
                        op_id,
                        *start_result,
                        &context.resolved,
                        None,
                    )
                    .await;
                }
            },
            Err(message) => {
                self.set_service_state(name, ServiceState::Failed);
                self.output_manager.service_error_event(name, &message);
                let should_auto_restart = matches!(intent, ServiceStartIntent::Background)
                    && self.services.get(name).is_some_and(|rs| {
                        rs.resolved.on_failure == crate::config::OnFailure::Restart
                    });
                if should_auto_restart {
                    self.schedule_auto_restart(name, &message, true);
                } else if let Some(rs) = self.services.get_mut(name) {
                    rs.reset_restart_tracking();
                }
                match intent {
                    ServiceStartIntent::Scheduled { done_tx } => {
                        let _ = done_tx
                            .send(ItemDone {
                                name: name.to_string(),
                                kind: NodeKind::Service,
                                success: false,
                                message: Some(message),
                                elapsed: None,
                                last_run: None,
                                service_start_generation: Some(op_id),
                                task_run_generation: None,
                            })
                            .await;
                    }
                    ServiceStartIntent::Reply { reply } => {
                        let _ = reply.send(Err(CommandError::Failed {
                            name: name.to_string(),
                            message,
                        }));
                    }
                    ServiceStartIntent::Background => {}
                }
            }
        }
    }

    pub(in crate::runner) fn spawn_service_rebuild_worker(
        &mut self,
        name: &str,
        resolved: crate::config::ResolvedService,
    ) -> Result<(), CommandError> {
        let Some(rs) = self.services.get_mut(name) else {
            return Err(CommandError::UnknownService {
                name: name.to_string(),
            });
        };
        rs.rebuild_generation = rs.rebuild_generation.saturating_add(1);
        let op_id = rs.rebuild_generation;

        let cmd_tx = self.internal_tx.clone();
        let name_owned = name.to_string();
        let base_dir = self.base_dir.clone();
        let emitter = self.output_manager.clone_lifecycle_emitter();
        let docker_client = self.docker_client.clone();
        let service_writer = self.output_manager.service_writer(name);
        let worker = tokio::spawn(async move {
            let result = run_service_build_worker(
                &base_dir,
                docker_client.as_ref(),
                &emitter,
                &name_owned,
                &resolved,
                false,
                service_writer.as_ref(),
            )
            .await;
            let _ = cmd_tx
                .send(RunnerInternalCommand::ServiceRebuildPrepared {
                    name: name_owned,
                    op_id,
                    result,
                })
                .await;
        });
        rs.rebuild_worker = Some(worker);
        Ok(())
    }

    pub(in crate::runner) async fn continue_rebuild_restart(&mut self, name: &str) {
        if self.shutting_down {
            return;
        }
        let has_proxy = self.services.get(name).is_some_and(|rs| rs.proxy.is_some());
        if has_proxy
            && let Some(rs) = self.services.get(name)
            && let Some(ref proxy) = rs.proxy
        {
            proxy.clear_backend();
        }

        if let Some(handle) = self.services.get_mut(name).and_then(|rs| rs.handle.take()) {
            if self.remove_attach_lock(name) {
                self.output_manager.resume_stdout_sink(name).await;
            }
            self.set_service_state(name, ServiceState::Stopping);
            let shutdown_config = self.effective_shutdown_config(name);
            let wait_full = self
                .services
                .get(name)
                .and_then(|rs| rs.proxy.as_ref())
                .is_some_and(|p| p.requires_full_exit_on_restart());
            let (reply_tx, _reply_rx) = oneshot::channel();
            self.spawn_manual_service_stop_worker(
                name,
                handle,
                shutdown_config,
                wait_full,
                reply_tx,
                ServiceStopAction::RestartSpawnOnly,
            );
            return;
        }

        if let Err(e) = self.queue_rebuild_service_start(name).await {
            self.fail_rebuild(name, &e.to_string());
        }
    }

    pub(in crate::runner) async fn handle_service_rebuild_prepared(
        &mut self,
        name: &str,
        op_id: u64,
        result: Result<(), String>,
    ) {
        let is_current = self
            .services
            .get(name)
            .is_some_and(|rs| rs.rebuild_generation == op_id);
        if !is_current {
            return;
        }
        if let Some(rs) = self.services.get_mut(name) {
            rs.rebuild_worker = None;
        }

        match result {
            Ok(()) => {
                if self.take_rebuild_stale(name) {
                    let _ = self.event_tx.send(RunnerEvent::RebuildComplete {
                        name: name.to_string(),
                        success: true,
                    });
                } else {
                    self.continue_rebuild_restart(name).await;
                }
            }
            Err(message) if message == "shutdown requested" => {
                self.initiate_shutdown().await;
            }
            Err(message) => self.fail_rebuild(name, &message),
        }
    }

    pub(in crate::runner) fn queue_background_service_start(
        &mut self,
        name: &str,
        mode: ServiceStartMode,
    ) -> Result<(), CommandError> {
        if self.shutting_down {
            return Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "shutdown in progress".to_string(),
            });
        }
        let context = self.service_start_snapshot(name)?;
        self.set_service_state(name, ServiceState::Starting);
        self.output_manager.service_event(name, "starting...");
        self.spawn_service_start_worker(name, context, mode, ServiceStartIntent::Background)
    }

    pub(in crate::runner) async fn queue_rebuild_service_start(
        &mut self,
        name: &str,
    ) -> Result<(), CommandError> {
        if self.shutting_down {
            return Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "shutdown in progress".to_string(),
            });
        }
        let realloc_result = if let Some(rs) = self.services.get_mut(name) {
            if let Some(ref mut proxy) = rs.proxy {
                Some(proxy.reallocate_ephemeral_ports().await)
            } else {
                None
            }
        } else {
            return Err(CommandError::UnknownService {
                name: name.to_string(),
            });
        };
        if let Some(Err(e)) = realloc_result {
            return Err(CommandError::Failed {
                name: name.to_string(),
                message: format!("failed to allocate ephemeral ports: {e}"),
            });
        }

        let context = self.service_start_snapshot(name)?;
        self.set_service_state(name, ServiceState::Starting);
        self.output_manager.service_event(name, "restarting...");
        self.spawn_service_start_worker(
            name,
            context,
            ServiceStartMode::SpawnOnly,
            ServiceStartIntent::Background,
        )
    }

    pub(in crate::runner) fn queue_scheduled_service_start(
        &mut self,
        name: &str,
        done_tx: mpsc::Sender<ItemDone>,
        mode: ServiceStartMode,
    ) -> Result<(), CommandError> {
        if self.shutting_down {
            return Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "shutdown in progress".to_string(),
            });
        }
        let context = self.service_start_snapshot(name)?;
        self.set_service_state(name, ServiceState::Starting);
        self.output_manager.service_event(name, "starting...");
        self.spawn_service_start_worker(
            name,
            context,
            mode,
            ServiceStartIntent::Scheduled { done_tx },
        )
    }

    pub(in crate::runner) async fn handle_start_service_cmd(
        &mut self,
        name: &str,
        reply: oneshot::Sender<CommandResult>,
    ) {
        if let Err(e) = self.lookup_service(name) {
            let _ = reply.send(Err(e));
            return;
        }
        let state = self.services.get(name).map(|rs| rs.state());
        let operation_in_progress = self
            .services
            .get(name)
            .is_some_and(|rs| rs.start_worker.is_some() || rs.state().is_transitioning());
        if operation_in_progress {
            let state = state.unwrap_or(ServiceState::Stopped);
            let _ = reply.send(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: format!("cannot start while {state:?}"),
            }));
            return;
        }
        if self
            .services
            .get(name)
            .is_some_and(|rs| rs.handle.is_some())
        {
            let _ = reply.send(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "already running".to_string(),
            }));
            return;
        }
        let context = match self.service_start_snapshot(name) {
            Ok(snapshot) => snapshot,
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };
        self.set_service_state(name, ServiceState::Starting);
        self.output_manager
            .service_event(name, "starting... (requested)");
        if let Err(e) = self.spawn_service_start_worker(
            name,
            context,
            ServiceStartMode::Full,
            ServiceStartIntent::Reply { reply },
        ) {
            self.output_manager
                .service_error_event(name, &e.to_string());
        }
    }

    pub(in crate::runner) async fn handle_hard_restart_service_cmd(
        &mut self,
        name: &str,
        reply: oneshot::Sender<CommandResult>,
    ) {
        if let Err(e) = self.lookup_service(name) {
            let _ = reply.send(Err(e));
            return;
        }

        let state = match self.services.get(name) {
            Some(rs) => rs.state(),
            None => {
                let _ = reply.send(Err(CommandError::UnknownService {
                    name: name.to_string(),
                }));
                return;
            }
        };
        if state.is_transitioning() {
            let _ = reply.send(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: format!("cannot hard restart while {state:?}"),
            }));
            return;
        }
        if self.services.get(name).is_some_and(|rs| {
            rs.rebuild_worker.is_some() || rs.control_worker.is_some() || rs.start_worker.is_some()
        }) {
            let _ = reply.send(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "operation already in progress".to_string(),
            }));
            return;
        }

        self.clear_rebuild_stale(name);
        self.output_manager
            .service_event(name, "rebuilding (requested hard restart)");

        let resolved = match self.services.get(name) {
            Some(rs) => rs.resolved.clone(),
            None => {
                let _ = reply.send(Err(CommandError::UnknownService {
                    name: name.to_string(),
                }));
                return;
            }
        };

        let result = if resolved.is_build_tool_managed() {
            self.spawn_forced_build_tool_rebuild(name)
        } else {
            self.spawn_service_rebuild_worker(name, resolved)
        };

        match result {
            Ok(()) => {
                let _ = reply.send(Ok(()));
            }
            Err(e) => {
                self.output_manager
                    .service_error_event(name, &e.to_string());
                let _ = reply.send(Err(e));
            }
        }
    }

    pub(in crate::runner) fn spawn_manual_service_stop_worker(
        &mut self,
        name: &str,
        handle: ServiceHandle,
        shutdown_config: crate::config::ShutdownConfig,
        wait_full_exit: bool,
        reply: oneshot::Sender<CommandResult>,
        stop_action: ServiceStopAction,
    ) {
        let Some(rs) = self.services.get_mut(name) else {
            let _ = reply.send(Err(CommandError::UnknownService {
                name: name.to_string(),
            }));
            return;
        };
        rs.control_generation = rs.control_generation.saturating_add(1);
        let op_id = rs.control_generation;
        rs.control_reply = Some(reply);
        rs.stop_action = stop_action;

        let cmd_tx = self.internal_tx.clone();
        let name_owned = name.to_string();
        let shutdown_rx = self.shutdown_flag_tx.subscribe();
        let debug = service::StopDebug::new(
            name_owned.clone(),
            self.output_manager.clone_lifecycle_emitter(),
        );
        let worker = tokio::spawn(async move {
            let result = service::stop_service_interruptibly(
                handle,
                Some(&shutdown_config),
                wait_full_exit,
                shutdown_rx,
                Some(debug),
            )
            .await
            .map_err(|e| e.to_string());
            let _ = cmd_tx
                .send(RunnerInternalCommand::ServiceStopComplete {
                    name: name_owned,
                    op_id,
                    result,
                })
                .await;
        });
        rs.control_worker = Some(worker);
    }

    pub(in crate::runner) async fn handle_service_stop_complete(
        &mut self,
        name: &str,
        op_id: u64,
        result: Result<(), String>,
    ) {
        let is_current = self
            .services
            .get(name)
            .is_some_and(|rs| rs.control_generation == op_id);
        if !is_current {
            return;
        }

        self.record_graph_cycle_stop_result(name, &result);

        let (reply, stop_action) = match self.services.get_mut(name) {
            Some(rs) => {
                rs.control_worker = None;
                (rs.control_reply.take(), std::mem::take(&mut rs.stop_action))
            }
            None => return,
        };

        if let Some(writer) = self.output_manager.service_writer(name) {
            writer.close_follow_sinks().await;
        }

        match result {
            Ok(()) => {
                if let Some(rs) = self.services.get_mut(name) {
                    rs.pgid = None;
                }
                self.set_service_state(name, ServiceState::Stopped);
                let next_result = match stop_action {
                    ServiceStopAction::None => Ok(()),
                    ServiceStopAction::RestartFull => {
                        self.queue_background_service_start(name, ServiceStartMode::Full)
                    }
                    ServiceStopAction::RestartSpawnOnly => {
                        if self.take_rebuild_stale(name) {
                            let _ = self.event_tx.send(RunnerEvent::RebuildComplete {
                                name: name.to_string(),
                                success: true,
                            });
                            Ok(())
                        } else {
                            self.queue_rebuild_service_start(name).await
                        }
                    }
                };
                if let Some(reply) = reply {
                    let _ = reply.send(next_result);
                }
            }
            Err(message) => {
                if let Some(rs) = self.services.get_mut(name) {
                    rs.pgid = None;
                }
                self.set_service_state(name, ServiceState::Failed);
                self.output_manager.service_error_event(name, &message);
                if let Some(reply) = reply {
                    let _ = reply.send(Err(CommandError::Failed {
                        name: name.to_string(),
                        message,
                    }));
                }
            }
        }
    }

    /// Handle an API-initiated Stop command.
    pub(in crate::runner) async fn handle_stop_cmd(
        &mut self,
        name: &str,
        reply: oneshot::Sender<CommandResult>,
    ) {
        if let Err(e) = self.lookup_service(name) {
            let _ = reply.send(Err(e));
            return;
        }
        // An untriggered or dependency-blocked lazy service has no process —
        // just mark it Stopped. Pending means its proxy received a connection,
        // but the dependency scheduler has not started it yet.
        if self.services.get(name).is_some_and(|rs| {
            rs.resolved.lazy && matches!(rs.state(), ServiceState::Lazy | ServiceState::Pending)
        }) {
            self.set_service_state(name, ServiceState::Stopped);
            self.output_manager
                .service_event(name, "stopped before lazy start");
            let _ = reply.send(Ok(()));
            return;
        }
        // A failed service has no live process — drop any exited handle and
        // mark it Stopped so the user can clear it without a restart.
        if self
            .services
            .get(name)
            .is_some_and(|rs| rs.state().is_failure())
        {
            if let Some(rs) = self.services.get_mut(name) {
                rs.stop_health_tracking();
                rs.reset_restart_tracking();
                rs.handle = None;
            }
            self.set_service_state(name, ServiceState::Stopped);
            self.output_manager
                .service_event(name, "stopped (was failed)");
            let _ = reply.send(Ok(()));
            return;
        }
        // Cancel monitor + any pending auto-restart before tearing down the
        // process so a recovery probe doesn't race with the stop.
        if let Some(rs) = self.services.get_mut(name) {
            rs.stop_health_tracking();
            rs.reset_restart_tracking();
        }
        let handle = self
            .services
            .get_mut(name)
            .and_then(|rs| rs.handle.take())
            .ok_or_else(|| CommandError::InvalidState {
                name: name.to_string(),
                message: "not running".to_string(),
            });
        let handle = match handle {
            Ok(h) => h,
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };
        let shutdown_config = self.effective_shutdown_config(name);
        // Release attach lock if held — the PTY write in the attach session
        // becomes invalid once the service stops (process gone).
        if self.remove_attach_lock(name) {
            self.output_manager.resume_stdout_sink(name).await;
        }
        self.set_service_state(name, ServiceState::Stopping);
        self.output_manager
            .service_event(name, "stopping... (requested)");
        self.spawn_manual_service_stop_worker(
            name,
            handle,
            shutdown_config,
            false,
            reply,
            ServiceStopAction::None,
        );
    }

    /// Runner-internal handler for [`RunnerInternalCommand::ReadyCheckComplete`].
    ///
    /// Emitted by the async ready-check task inside
    /// [`wire_service_output_and_ready_check`] when there's no `done_tx`
    /// (manual start or rebuild). Updates the runner's own state — the
    /// broadcast follows via `set_service_state`.
    ///
    /// On failure, mirrors `handle_service_done`'s lazy-retry behaviour so
    /// a proxied lazy service resets to `Lazy` instead of getting stuck on
    /// `Failed`.
    pub(in crate::runner) fn handle_ready_check_complete(
        &mut self,
        name: &str,
        generation: u64,
        success: bool,
        message: Option<String>,
    ) {
        let is_current_running_generation = self.services.get(name).is_some_and(|rs| {
            rs.start_generation == generation && rs.state() == ServiceState::Running
        });
        if !is_current_running_generation {
            return;
        }
        if success {
            // Backoff resets on Ready; the rapid-crash streak does not (it is
            // cleared only by a sufficiently long-lived run, a clean exit, or
            // manual intervention) so a service that flaps ready→crash→ready
            // can still trip the crash-loop ceiling.
            if let Some(rs) = self.services.get_mut(name) {
                if let Some(handle) = rs.pending_restart.take() {
                    handle.abort();
                }
                rs.restart_attempts = 0;
            }
            self.set_service_state(name, ServiceState::Ready);
            self.unblock_dependency_failed_items();
            return;
        }
        let is_lazy = self
            .services
            .get(name)
            .is_some_and(|rs| rs.resolved.lazy && rs.proxy.is_some());
        if is_lazy {
            // Route through the crash-loop guard so a lazy service that keeps
            // dying on launch is eventually left `Failed` with its proxy
            // trigger un-armed, instead of being relaunched in a tight loop by
            // a still-queued connection. See `handle_lazy_launch_failure`.
            self.handle_lazy_launch_failure(name, message.as_deref());
        } else {
            self.set_service_state(name, ServiceState::Failed);
            let policy = self
                .services
                .get(name)
                .map(|rs| rs.resolved.on_failure)
                .unwrap_or_default();
            if matches!(policy, crate::config::OnFailure::Restart) {
                self.schedule_auto_restart(
                    name,
                    message
                        .as_deref()
                        .unwrap_or("service failed before becoming ready"),
                    true,
                );
            }
        }
    }

    pub(in crate::runner) async fn handle_restart_service_cmd(
        &mut self,
        name: &str,
        reply: oneshot::Sender<CommandResult>,
    ) {
        if let Err(e) = self.lookup_service(name) {
            let _ = reply.send(Err(e));
            return;
        }
        let state = match self.services.get(name) {
            Some(rs) => rs.state(),
            None => {
                let _ = reply.send(Err(CommandError::UnknownService {
                    name: name.to_string(),
                }));
                return;
            }
        };
        if state.is_transitioning()
            || self
                .services
                .get(name)
                .is_some_and(|rs| rs.start_worker.is_some())
        {
            let _ = reply.send(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: format!("cannot restart while {state:?}"),
            }));
            return;
        }
        if state == ServiceState::Failed && self.reap_exited_process_handle(name).await {
            let _ = reply.send(self.queue_background_service_start(name, ServiceStartMode::Full));
            return;
        }
        if matches!(state, ServiceState::Lazy | ServiceState::Stopped) {
            let _ = reply.send(self.queue_background_service_start(name, ServiceStartMode::Full));
            return;
        }

        if let Some(rs) = self.services.get_mut(name) {
            rs.stop_health_tracking();
            rs.reset_restart_tracking();
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
            .service_event(name, "stopping... (requested restart)");
        self.spawn_manual_service_stop_worker(
            name,
            handle,
            shutdown_config,
            false,
            reply,
            ServiceStopAction::RestartFull,
        );
    }

    async fn reap_exited_process_handle(&mut self, name: &str) -> bool {
        let exited = match self
            .services
            .get_mut(name)
            .and_then(|rs| rs.handle.as_mut())
        {
            Some(ServiceHandle::Process(process)) => match process.try_wait() {
                Ok(Some(_)) => true,
                Ok(None) | Err(_) => false,
            },
            _ => false,
        };
        if !exited {
            return false;
        }
        if let Some(rs) = self.services.get_mut(name) {
            rs.handle = None;
            rs.stop_health_tracking();
        }
        if let Some(writer) = self.output_manager.service_writer(name) {
            writer.close_follow_sinks().await;
        }
        true
    }
}
