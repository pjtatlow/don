use super::graph::topological_sort;
use super::service::ServiceHandle;
use super::signals::force_shutdown_requested;
use super::task_worker::TaskRunPrepared;
use super::{Runner, RunnerInternalCommand, ServiceState, ServiceStopAction};
use crate::runner::service::stop_service;
use std::collections::{BTreeMap, HashMap};
use tokio::task::JoinSet;

impl Runner {
    /// Initiate graceful shutdown of all services.
    pub(in crate::runner) async fn initiate_shutdown(&mut self) {
        if self.shutting_down {
            return;
        }
        self.shutting_down = true;
        // A foreground task pauses the stdout/TUI sink while it owns the
        // terminal, and normally releases it from `handle_task_exit` /
        // `handle_task_done`. During shutdown those completion paths can be
        // skipped — the run_worker may be aborted before it sends
        // `TaskRunPrepared`, or the foreground task gets SIGKILL'd at the
        // end of this function and its `TaskExited` arrives after the main
        // loop has already broken out. If we leave the pause engaged, every
        // lifecycle event we emit below ("send SIGTERM…", "stopping…",
        // "shutdown complete") and every service's own shutdown output is
        // silently dropped by `stdout_sink_task`. Force-clear it up front so
        // the user actually sees what shutdown is doing.
        self.output_manager.resume_visible_output();
        let _ = self.event_tx.send(super::RunnerEvent::ShutdownStarted);
        let _ = self.shutdown_flag_tx.send(true);
        self.output_manager
            .lifecycle_event("shutting down gracefully... (Ctrl+C again to force)");

        // Abort the detached batch-build task and await its termination so
        // it can't keep any `LifecycleEmitter`/`SinkHandle` clones alive
        // past shutdown. The `Child` inside has `kill_on_drop(true)`, so
        // dropping the aborted future SIGKILLs the bazel/turbo client;
        // awaiting the JoinHandle guarantees the drop has actually run
        // before we continue. A 5s timeout guards against the pathological
        // case where the inner reader tasks don't drop promptly — we'd
        // rather continue shutdown than wedge on a stuck bazel pipe.
        if let Some(guard) = self.batch_build_handle.take()
            && let Some(handle) = guard.into_inner()
        {
            handle.abort();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
        }
        if let Some(guard) = self.rebuild_batch_handle.take()
            && let Some(handle) = guard.into_inner()
        {
            handle.abort();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
        }
        if let Some(guard) = self.graph_requery_handle.take()
            && let Some(handle) = guard.into_inner()
        {
            handle.abort();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
        }
        if let Some(handle) = self.update_check_handle.take() {
            handle.abort();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
        }

        let mut service_worker_handles = Vec::new();
        for (name, rs) in &mut self.services {
            if let Some(worker) = rs.start_worker.take() {
                self.output_manager
                    .service_event(name, "start cancelled by shutdown");
                worker.abort();
                service_worker_handles.push(worker);
            }
            if let Some(worker) = rs.rebuild_worker.take() {
                self.output_manager
                    .service_event(name, "rebuild cancelled by shutdown");
                worker.abort();
                service_worker_handles.push(worker);
            }
        }
        for worker in service_worker_handles {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), worker).await;
        }

        let mut task_worker_handles = Vec::new();
        for (name, rt) in &mut self.tasks {
            if let Some(waiter) = rt.run_waiter.take() {
                waiter.complete(Err(super::CommandError::Failed {
                    name: name.clone(),
                    message: "run cancelled by shutdown".to_string(),
                }));
            }
            if let Some(worker) = rt.run_worker.take() {
                self.output_manager
                    .service_event(name, "run cancelled by shutdown");
                worker.abort();
                task_worker_handles.push(worker);
            }
        }
        for worker in task_worker_handles {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), worker).await;
        }

        // Same treatment for any in-flight JIT lazy builds. These are
        // spawned when a lazy service's proxy gets its first connection
        // and, until this was tracked, would keep streaming bazel/turbo
        // output long past "shutdown complete".
        let lazy_handles: Vec<tokio::task::JoinHandle<()>> = self
            .lazy_build_handles
            .drain()
            .filter_map(|(_, guard)| guard.into_inner())
            .collect();
        for h in &lazy_handles {
            h.abort();
        }
        for h in lazy_handles {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), h).await;
        }

        self.drain_late_worker_results().await;

        // Shut down all proxy listeners first (stop accepting new connections).
        for rs in self.services.values_mut() {
            if let Some(proxy) = rs.proxy.take() {
                proxy.shutdown();
            }
        }

        // Tell the API server to stop accepting connections.
        if let Some(tx) = self.server_shutdown_tx.take() {
            let _ = tx.send(true);
        }

        // Build reverse dependency order for shutdown.
        // Services at the same depth (no dependency relationship) stop concurrently.
        let dep_map = self.build_dep_map();
        let order = match topological_sort(&dep_map) {
            Ok(o) => o,
            Err(cycle) => {
                self.output_manager.error_event(&format!(
                    "shutdown: dependency graph has a cycle ({cycle:?}) — \
                     stopping live services in arbitrary order"
                ));
                self.services.keys().cloned().collect()
            }
        };

        // Compute depth of each service node for grouping.
        let mut depths: HashMap<String, usize> = HashMap::new();
        for name in &order {
            let node_deps = dep_map.get(name).cloned().unwrap_or_default();
            let max_dep_depth = node_deps
                .iter()
                .filter_map(|d| depths.get(d))
                .max()
                .copied()
                .unwrap_or(0);
            let depth = if node_deps.is_empty() {
                0
            } else {
                max_dep_depth + 1
            };
            depths.insert(name.clone(), depth);
        }

        // Group live services by depth, then iterate from highest depth
        // (most dependent) to lowest (least dependent). A service handle is
        // the source of truth here: states like Unhealthy still have a live
        // process and must be signalled during shutdown.
        let mut by_depth: BTreeMap<usize, Vec<String>> = BTreeMap::new();
        for name in &order {
            let Some(service) = self.services.get(name) else {
                continue;
            };
            if service.handle.is_none() {
                continue;
            }
            let depth = depths.get(name).copied().unwrap_or(0);
            by_depth.entry(depth).or_default().push(name.clone());
        }

        let mut remaining: usize = by_depth.values().map(|v| v.len()).sum();

        // Stop from highest depth to lowest (dependents first).
        for (_depth, names) in by_depth.into_iter().rev() {
            for name in &names {
                self.set_service_state(name, ServiceState::Stopping);
                self.output_manager
                    .service_event(name, &format!("stopping... ({remaining} remaining)"));
            }

            // Track PGIDs of services being stopped so we can SIGKILL
            // them if a second Ctrl+C arrives during graceful shutdown.
            let mut stopping_pgids: HashMap<String, i32> = HashMap::new();
            let mut join_set: JoinSet<String> = JoinSet::new();
            for name in &names {
                if let Some(handle) = self.services.get_mut(name).and_then(|rs| rs.handle.take()) {
                    if let ServiceHandle::Process(ref proc) = handle {
                        stopping_pgids.insert(name.clone(), proc.pgid());
                    }
                    let shutdown_config = self.effective_shutdown_config(name);
                    let force = force_shutdown_requested();
                    let name_owned = name.clone();
                    let debug = super::service::StopDebug::new(
                        name.clone(),
                        self.output_manager.clone_lifecycle_emitter(),
                    );
                    join_set.spawn(async move {
                        let _ =
                            stop_service(handle, Some(&shutdown_config), force, true, Some(debug))
                                .await;
                        name_owned
                    });
                }
            }

            // Wait for graceful stops, but if a second Ctrl+C arrives,
            // SIGKILL all processes being stopped and abort the futures.
            loop {
                if force_shutdown_requested() && !join_set.is_empty() {
                    self.output_manager
                        .lifecycle_event("forcing immediate shutdown");
                    // SIGKILL all processes that are still being stopped.
                    let names: Vec<String> = stopping_pgids
                        .iter()
                        .map(|(name, pgid)| {
                            self.output_manager.service_event(
                                name,
                                &format!("send SIGKILL to pgid {pgid} (force shutdown)"),
                            );
                            let _ = nix::sys::signal::killpg(
                                nix::unistd::Pid::from_raw(*pgid),
                                nix::sys::signal::Signal::SIGKILL,
                            );
                            name.clone()
                        })
                        .collect();
                    for name in names {
                        if let Some(rs) = self.services.get_mut(&name) {
                            rs.pgid = None;
                        }
                        self.set_service_state(&name, ServiceState::Stopped);
                    }
                    join_set.abort_all();
                    while join_set.join_next().await.is_some() {}
                    remaining = 0;
                    break;
                }

                // Poll for the next completed stop, with a short sleep so
                // we can re-check the force flag promptly.
                match tokio::time::timeout(
                    std::time::Duration::from_millis(100),
                    join_set.join_next(),
                )
                .await
                {
                    Ok(Some(Ok(name))) => {
                        stopping_pgids.remove(&name);
                        if let Some(rs) = self.services.get_mut(&name) {
                            rs.pgid = None;
                        }
                        self.set_service_state(&name, ServiceState::Stopped);
                        self.drain_service_output(&name).await;
                        remaining -= 1;
                        self.output_manager
                            .service_event(&name, &format!("stopped ({remaining} remaining)"));
                    }
                    Ok(Some(Err(_))) => {
                        remaining = remaining.saturating_sub(1);
                    }
                    Ok(None) => break,  // All tasks done.
                    Err(_) => continue, // Timeout — re-check force flag.
                }
            }

            if remaining == 0 {
                break;
            }
        }

        // Kill any still-running task process groups.
        let running_task_pgids: Vec<(String, i32)> = self
            .tasks
            .iter()
            .filter_map(|(name, rt)| rt.pgid.map(|pgid| (name.clone(), pgid)))
            .collect();
        if !running_task_pgids.is_empty() {
            self.output_manager.lifecycle_event(&format!(
                "killing {} running task{}",
                running_task_pgids.len(),
                if running_task_pgids.len() == 1 {
                    ""
                } else {
                    "s"
                }
            ));
            for (name, pgid) in &running_task_pgids {
                self.output_manager
                    .service_event(name, &format!("send SIGKILL to task pgid {pgid}"));
                if let Err(e) = nix::sys::signal::killpg(
                    nix::unistd::Pid::from_raw(*pgid),
                    nix::sys::signal::Signal::SIGKILL,
                ) {
                    // ESRCH = already dead, which is fine.
                    if e != nix::Error::ESRCH {
                        self.output_manager.service_error_event(
                            name,
                            &format!("failed to kill task pgid {pgid}: {e}"),
                        );
                    }
                }
                if let Some(rt) = self.tasks.get_mut(name) {
                    rt.pgid = None;
                }
            }
        }
    }

    /// Wait for remaining async tasks to finish after shutdown.
    pub(in crate::runner) async fn wait_for_shutdown(&mut self) {
        // All handles should already be stopped by initiate_shutdown.
        // Drop remaining handles, release sockets, clear attach state.
        for rs in self.services.values_mut() {
            if let Some(worker) = rs.control_worker.take() {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(1), worker).await;
            }
            if let Some(worker) = rs.start_worker.take() {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(1), worker).await;
            }
            if let Some(worker) = rs.rebuild_worker.take() {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(1), worker).await;
            }
            if let Some(worker) = rs.output_worker.take() {
                Self::await_output_worker(worker).await;
            }
            rs.handle = None;
            rs.attach_lock = None;
            rs.attach_waiter = None;
            rs.control_reply = None;
            rs.stop_action = ServiceStopAction::None;
        }
        for rt in self.tasks.values_mut() {
            if let Some(worker) = rt.run_worker.take() {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(1), worker).await;
            }
            if let Some(worker) = rt.output_worker.take() {
                Self::await_output_worker(worker).await;
            }
            rt.attach_lock = None;
            rt.attach_waiter = None;
        }
    }

    async fn drain_late_worker_results(&mut self) {
        while let Ok(cmd) = self.internal_rx.try_recv() {
            match cmd {
                RunnerInternalCommand::ServiceStartPrepared {
                    name,
                    context,
                    result,
                    ..
                } => {
                    self.stop_late_service_start(name, context, result).await;
                }
                RunnerInternalCommand::TaskRunPrepared { name, result, .. } => {
                    self.stop_late_task_start(name, result).await;
                }
                RunnerInternalCommand::ServiceStopComplete { .. }
                | RunnerInternalCommand::ServiceRebuildPrepared { .. }
                | RunnerInternalCommand::GraphRequeryComplete(_)
                | RunnerInternalCommand::TaskExited(_)
                | RunnerInternalCommand::TaskRunWaitTimedOut { .. }
                | RunnerInternalCommand::BatchBuildComplete(_)
                | RunnerInternalCommand::RebuildBatchComplete(_)
                | RunnerInternalCommand::LazyBuildComplete { .. }
                | RunnerInternalCommand::ServiceHealthChanged { .. }
                | RunnerInternalCommand::AutoRestart { .. }
                | RunnerInternalCommand::ServiceExited { .. }
                | RunnerInternalCommand::ReadyCheckComplete { .. }
                | RunnerInternalCommand::UpdateCheckComplete(_) => {}
            }
        }
    }

    pub(in crate::runner) async fn stop_late_service_start(
        &mut self,
        name: String,
        context: Box<super::service_worker::ServiceStartContext>,
        result: Result<Box<crate::runner::service::StartResult>, String>,
    ) {
        let Ok(start_result) = result else {
            return;
        };
        self.output_manager
            .service_event(&name, "start cancelled by shutdown");
        let crate::runner::service::StartResult {
            handle,
            child_output,
        } = *start_result;

        let output_worker = self.output_manager.service_writer(&name).map(|writer| {
            tokio::spawn(async move {
                let _ = writer.process_stream(child_output).await;
            })
        });
        let shutdown_config = context
            .resolved
            .shutdown
            .clone()
            .map(|shutdown| shutdown.merged_over(&self.config.shutdown))
            .unwrap_or_else(|| self.config.shutdown.clone());
        let _ = stop_service(
            handle,
            Some(&shutdown_config),
            force_shutdown_requested(),
            true,
            Some(super::service::StopDebug::new(
                name.clone(),
                self.output_manager.clone_lifecycle_emitter(),
            )),
        )
        .await;
        if let Some(worker) = output_worker {
            Self::await_output_worker(worker).await;
        }
    }

    pub(in crate::runner) async fn stop_late_task_start(
        &mut self,
        name: String,
        result: Result<TaskRunPrepared, String>,
    ) {
        let Ok(prepared) = result else {
            return;
        };
        match prepared {
            TaskRunPrepared::Spawned(spawn) => {
                self.output_manager
                    .service_event(&name, "run cancelled by shutdown");
                let crate::runner::task::TaskSpawn {
                    mut handle,
                    child_output,
                    rendered_cmdline: _rendered_cmdline,
                } = *spawn;
                let output_worker = self.output_manager.service_writer(&name).map(|writer| {
                    tokio::spawn(async move {
                        let _ = writer.process_stream(child_output).await;
                    })
                });
                self.output_manager.service_event(
                    &name,
                    &format!("send SIGKILL to task pgid {}", handle.pgid()),
                );
                let _ = handle
                    .terminate(
                        nix::sys::signal::Signal::SIGKILL,
                        std::time::Duration::from_millis(500),
                    )
                    .await;
                if let Some(worker) = output_worker {
                    Self::await_output_worker(worker).await;
                }
            }
            TaskRunPrepared::ForegroundSpawned(spawn) => {
                self.output_manager
                    .service_event(&name, "run cancelled by shutdown");
                let crate::runner::task::ForegroundTaskSpawn {
                    mut handle,
                    rendered_cmdline: _rendered_cmdline,
                } = *spawn;
                self.output_manager.service_event(
                    &name,
                    &format!("send SIGKILL to foreground task pgid {}", handle.pgid()),
                );
                let _ = handle
                    .terminate(
                        nix::sys::signal::Signal::SIGKILL,
                        std::time::Duration::from_millis(500),
                    )
                    .await;
            }
            TaskRunPrepared::PendingRun { .. } | TaskRunPrepared::Skipped { .. } => {}
        }
    }

    async fn drain_service_output(&mut self, name: &str) {
        let Some(worker) = self
            .services
            .get_mut(name)
            .and_then(|rs| rs.output_worker.take())
        else {
            return;
        };

        Self::await_output_worker(worker).await;
    }

    async fn await_output_worker(worker: tokio::task::JoinHandle<()>) {
        let mut worker = worker;
        match tokio::time::timeout(std::time::Duration::from_secs(2), &mut worker).await {
            Ok(_) => {}
            Err(_) => {
                worker.abort();
                let _ = worker.await;
            }
        }
    }
}
