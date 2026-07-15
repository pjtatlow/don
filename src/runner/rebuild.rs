use super::build_tools::{
    BazelRebuildItem, GraphRequeryOutcomeItem, GraphRequeryRequestItem, RebuildBatchOutcome,
    RebuildBatchRequest, TurboRebuildItem, run_graph_requery_worker, run_rebuild_batch_worker,
    send_watch_update,
};
use super::paths::{resolve_watch_ignore_patterns, working_dir_for};
use super::{
    Runner, RunnerEvent, RunnerInternalCommand, ServiceState, should_rebuild_after_graph_requery,
};

impl Runner {
    /// Flush all pending build-tool rebuilds as a single batch.
    ///
    /// Collects Bazel targets and Turbo filters from the queued services,
    /// runs one build per tool, then restarts each affected service.
    pub(in crate::runner) async fn flush_pending_rebuilds(&mut self) {
        let mut names = std::mem::take(&mut self.pending_bt_rebuilds);
        self.bt_rebuild_deadline = None;

        if names.is_empty() {
            return;
        }
        if self.rebuild_batch_handle.is_some() {
            for name in names {
                if !self.pending_bt_rebuilds.contains(&name) {
                    self.pending_bt_rebuilds.push(name);
                }
            }
            self.bt_rebuild_deadline =
                Some(tokio::time::Instant::now() + std::time::Duration::from_millis(50));
            return;
        }

        // Defer services that haven't finished coming up. Rebuilding a service
        // that is still building or starting would race its in-flight build or
        // double-start it before the startup path attaches a process handle.
        // Re-queue them; they're retried once they reach a running state. (This
        // is what keeps a file edited mid-build from being lost: the rebuild
        // waits here instead of running against a half-started service.)
        let mut deferred: Vec<String> = Vec::new();
        names.retain(|name| {
            let coming_up = self.services.get(name).is_some_and(|rs| {
                rs.state().is_transitioning() && rs.state() != ServiceState::Stopping
            });
            if coming_up {
                deferred.push(name.clone());
                false
            } else {
                true
            }
        });
        if !deferred.is_empty() {
            for name in deferred {
                if !self.pending_bt_rebuilds.contains(&name) {
                    self.pending_bt_rebuilds.push(name);
                }
            }
            self.bt_rebuild_deadline =
                Some(tokio::time::Instant::now() + std::time::Duration::from_millis(50));
        }
        if names.is_empty() {
            return;
        }

        let mut bazel_items: Vec<BazelRebuildItem> = Vec::new();
        let mut turbo_items: Vec<TurboRebuildItem> = Vec::new();
        // Services without a build tool target (shouldn't happen, but handle gracefully)
        let mut plain_rebuilds: Vec<String> = Vec::new();

        for name in &names {
            if let Some(rs) = self.services.get(name) {
                match &rs.resolved.kind {
                    Some(crate::config::ServiceKind::Bazel(bazel)) => {
                        bazel_items.push(BazelRebuildItem {
                            name: name.clone(),
                            target: bazel.target.clone(),
                            working_dir: working_dir_for(
                                &self.base_dir,
                                rs.resolved.dir.as_deref(),
                            ),
                        });
                    }
                    Some(crate::config::ServiceKind::Turbo(turbo)) => {
                        let build_task = turbo
                            .build_task
                            .clone()
                            .unwrap_or_else(|| "build".to_string());
                        if !build_task.is_empty()
                            && let Some(ref filter) = turbo.filter
                        {
                            turbo_items.push(TurboRebuildItem {
                                name: name.clone(),
                                filter: filter.clone(),
                                build_task,
                                working_dir: working_dir_for(
                                    &self.base_dir,
                                    rs.resolved.dir.as_deref(),
                                ),
                            });
                        } else {
                            plain_rebuilds.push(name.clone());
                        }
                    }
                    _ => {
                        plain_rebuilds.push(name.clone());
                    }
                }
            }
        }
        let request = RebuildBatchRequest {
            bazel_items,
            turbo_items,
            plain_rebuilds,
            force: false,
        };
        let cmd_tx = self.internal_tx.clone();
        let emitter = self.output_manager.clone_lifecycle_emitter();
        let bazel_build_mutex = self.bazel_build_mutex.clone();
        let handle = tokio::spawn(async move {
            let outcome = run_rebuild_batch_worker(request, emitter, bazel_build_mutex).await;
            let _ = cmd_tx
                .send(RunnerInternalCommand::RebuildBatchComplete(outcome))
                .await;
        });
        self.rebuild_batch_handle = Some(crate::build_tool::AbortOnDrop::new(handle));
    }

    pub(in crate::runner) fn fail_rebuild(&self, name: &str, message: &str) {
        self.output_manager.service_error_event(name, message);
        let _ = self.event_tx.send(RunnerEvent::RebuildComplete {
            name: name.to_string(),
            success: false,
        });
    }

    pub(in crate::runner) fn mark_rebuild_stale(&mut self, name: &str) {
        if let Some(rs) = self.services.get_mut(name) {
            rs.rebuild_stale = true;
        }
    }

    pub(in crate::runner) fn clear_rebuild_stale(&mut self, name: &str) {
        if let Some(rs) = self.services.get_mut(name) {
            rs.rebuild_stale = false;
        }
    }

    pub(in crate::runner) fn take_rebuild_stale(&mut self, name: &str) -> bool {
        self.services.get_mut(name).is_some_and(|rs| {
            let stale = rs.rebuild_stale;
            rs.rebuild_stale = false;
            stale
        })
    }

    /// Read and clear the "running process is behind the latest build" flag.
    /// See [`crate::runner::state::RuntimeService::artifact_ahead_of_process`].
    pub(in crate::runner) fn take_artifact_ahead_of_process(&mut self, name: &str) -> bool {
        self.services.get_mut(name).is_some_and(|rs| {
            let ahead = rs.artifact_ahead_of_process;
            rs.artifact_ahead_of_process = false;
            ahead
        })
    }

    pub(in crate::runner) async fn handle_rebuild_batch_complete(
        &mut self,
        outcome: RebuildBatchOutcome,
    ) {
        for (name, message) in &outcome.failed {
            self.fail_rebuild(name, message);
        }
        for name in &outcome.up_to_date {
            // Normally up-to-date means the running process already has the
            // current artifact, so there's nothing to do. But if an earlier
            // stale build deferred this service's restart, the process is still
            // behind the last successful build — restart into it now rather
            // than no-op (up-to-date is measured against the last build, not
            // the running process).
            if self.take_artifact_ahead_of_process(name) {
                self.output_manager.service_debug_event(
                    name,
                    "up to date, but process is behind last build — restarting",
                );
                self.do_rebuild(name).await;
                continue;
            }
            self.output_manager
                .service_debug_event(name, "skipped (no changes)");
            let _ = self.event_tx.send(RunnerEvent::RebuildComplete {
                name: name.clone(),
                success: true,
            });
        }
        for name in &outcome.build_succeeded {
            if let Some(rs) = self.services.get_mut(name) {
                rs.batch_built = true;
            }
            if self.take_rebuild_stale(name) {
                // A watched file changed mid-build. Skip restarting into the
                // artifact we just built and let the follow-up cycle pick up
                // the newer change — but record that the running process is now
                // behind a successful build, so the follow-up restarts even if
                // the build tool then reports up-to-date.
                if let Some(rs) = self.services.get_mut(name) {
                    rs.artifact_ahead_of_process = true;
                }
                let _ = self.event_tx.send(RunnerEvent::RebuildComplete {
                    name: name.clone(),
                    success: true,
                });
                continue;
            }
            self.do_rebuild(name).await;
        }
        for name in &outcome.plain_rebuilds {
            if self.take_rebuild_stale(name) {
                let _ = self.event_tx.send(RunnerEvent::RebuildComplete {
                    name: name.clone(),
                    success: true,
                });
                continue;
            }
            self.do_rebuild(name).await;
        }
        if !self.pending_bt_rebuilds.is_empty() {
            self.bt_rebuild_deadline =
                Some(tokio::time::Instant::now() + std::time::Duration::from_millis(50));
        }
    }

    pub(in crate::runner) fn spawn_forced_build_tool_rebuild(
        &mut self,
        name: &str,
    ) -> Result<(), super::CommandError> {
        if self.rebuild_batch_handle.is_some() {
            return Err(super::CommandError::InvalidState {
                name: name.to_string(),
                message: "build-tool rebuild already in progress".to_string(),
            });
        }

        let rs = self
            .services
            .get(name)
            .ok_or_else(|| super::CommandError::UnknownService {
                name: name.to_string(),
            })?;
        let mut bazel_items: Vec<BazelRebuildItem> = Vec::new();
        let mut turbo_items: Vec<TurboRebuildItem> = Vec::new();
        let mut plain_rebuilds: Vec<String> = Vec::new();

        match &rs.resolved.kind {
            Some(crate::config::ServiceKind::Bazel(bazel)) => {
                bazel_items.push(BazelRebuildItem {
                    name: name.to_string(),
                    target: bazel.target.clone(),
                    working_dir: working_dir_for(&self.base_dir, rs.resolved.dir.as_deref()),
                });
            }
            Some(crate::config::ServiceKind::Turbo(turbo)) => {
                let build_task = turbo
                    .build_task
                    .clone()
                    .unwrap_or_else(|| "build".to_string());
                if !build_task.is_empty()
                    && let Some(ref filter) = turbo.filter
                {
                    turbo_items.push(TurboRebuildItem {
                        name: name.to_string(),
                        filter: filter.clone(),
                        build_task,
                        working_dir: working_dir_for(&self.base_dir, rs.resolved.dir.as_deref()),
                    });
                } else {
                    plain_rebuilds.push(name.to_string());
                }
            }
            _ => plain_rebuilds.push(name.to_string()),
        }
        self.pending_bt_rebuilds.retain(|queued| queued != name);

        let request = RebuildBatchRequest {
            bazel_items,
            turbo_items,
            plain_rebuilds,
            force: true,
        };
        let cmd_tx = self.internal_tx.clone();
        let emitter = self.output_manager.clone_lifecycle_emitter();
        let bazel_build_mutex = self.bazel_build_mutex.clone();
        let handle = tokio::spawn(async move {
            let outcome = run_rebuild_batch_worker(request, emitter, bazel_build_mutex).await;
            let _ = cmd_tx
                .send(RunnerInternalCommand::RebuildBatchComplete(outcome))
                .await;
        });
        self.rebuild_batch_handle = Some(crate::build_tool::AbortOnDrop::new(handle));
        Ok(())
    }

    pub(in crate::runner) async fn handle_graph_requery_complete(
        &mut self,
        outcomes: Vec<GraphRequeryOutcomeItem>,
    ) {
        let watch_update_tx = self.watch_update_tx.clone();
        let mut services_to_rebuild: Vec<String> = Vec::new();
        let mut tasks_to_rerun: Vec<String> = Vec::new();
        let global_watch_ignore = resolve_watch_ignore_patterns(
            &self.base_dir,
            &[],
            &self.base_dir,
            &self.config.watch_ignore,
        );

        for outcome in outcomes {
            match outcome.result {
                Ok(info) => {
                    let count = info.watch_paths.len();
                    self.output_manager.service_event(
                        &outcome.name,
                        &format!(
                            "updated watch paths ({count} path{})",
                            if count == 1 { "" } else { "s" }
                        ),
                    );
                    if let Some(rs) = self.services.get_mut(&outcome.name) {
                        rs.resolved_watch_paths = info.watch_paths.clone();
                    } else if let Some(rt) = self.tasks.get_mut(&outcome.name) {
                        rt.resolved_watch_paths = info.watch_paths.clone();
                    }
                    if outcome.watch_enabled
                        && let Some(ref tx) = watch_update_tx
                    {
                        let kind = if self.services.contains_key(&outcome.name) {
                            crate::watch::WatchItemKind::Service
                        } else {
                            crate::watch::WatchItemKind::Task
                        };
                        send_watch_update(
                            tx,
                            outcome.name.clone(),
                            kind,
                            info.watch_paths.clone(),
                            outcome.ignore_patterns,
                            self.base_dir.clone(),
                        )
                        .await;
                        send_watch_update(
                            tx,
                            format!("{}__graph", outcome.name),
                            crate::watch::WatchItemKind::BuildGraph,
                            info.graph_definition_globs,
                            global_watch_ignore.clone(),
                            self.base_dir.clone(),
                        )
                        .await;
                    }

                    if outcome.watch_enabled
                        && let Some(rs) = self.services.get(&outcome.name)
                    {
                        if should_rebuild_after_graph_requery(rs)
                            && !services_to_rebuild.contains(&outcome.name)
                        {
                            services_to_rebuild.push(outcome.name.clone());
                        }
                    } else if outcome.watch_enabled
                        && self.tasks.contains_key(&outcome.name)
                        && !tasks_to_rerun.contains(&outcome.name)
                    {
                        tasks_to_rerun.push(outcome.name.clone());
                    }
                }
                Err(e) => {
                    self.output_manager.service_error_event(
                        &outcome.name,
                        &format!(
                            "build tool re-query failed: {e} — keeping existing watch patterns"
                        ),
                    );
                }
            }
        }
        if !self.pending_graph_requery.is_empty() {
            self.bt_requery_deadline =
                Some(tokio::time::Instant::now() + std::time::Duration::from_millis(100));
        }

        for name in services_to_rebuild {
            self.output_manager
                .service_event(&name, "build graph changed — rebuilding");
            self.handle_service_watch_trigger(&name).await;
        }

        for name in tasks_to_rerun {
            self.output_manager
                .service_event(&name, "build graph changed — re-running");
            self.handle_task_watch_trigger(&name).await;
        }
    }

    /// Handle a build graph change event (BUILD files, package.json, etc. changed).
    ///
    /// Queues the item for a batched re-query instead of spawning immediately.
    /// This prevents redundant concurrent queries when a single BUILD file
    /// change affects multiple services.
    pub(in crate::runner) async fn handle_build_graph_changed(&mut self, name: &str) {
        if name == crate::watch::WORKSPACE_GRAPH_ITEM_NAME {
            let service_names: Vec<String> = self
                .services
                .iter()
                .filter(|(_, rs)| rs.resolved.build_tool_watch_enabled())
                .map(|(service_name, _)| service_name.clone())
                .collect();
            let task_names: Vec<String> = self
                .tasks
                .iter()
                .filter(|(_, rt)| rt.config.build_tool_watch_enabled())
                .map(|(task_name, _)| task_name.clone())
                .collect();
            for item_name in service_names.into_iter().chain(task_names) {
                if !self.pending_graph_requery.contains(&item_name) {
                    self.pending_graph_requery.push(item_name);
                }
            }
        } else if !self.pending_graph_requery.contains(&name.to_string()) {
            self.pending_graph_requery.push(name.to_string());
        }
        self.bt_requery_deadline =
            Some(tokio::time::Instant::now() + std::time::Duration::from_millis(100));
    }

    /// Flush all pending build-graph re-queries.
    ///
    /// Runs build tool queries for each queued item and sends updated watch
    /// patterns to the WatchManager. Uses stale-while-revalidate: old watch
    /// patterns remain active during the re-query.
    pub(in crate::runner) async fn flush_pending_graph_requery(&mut self) {
        let names = std::mem::take(&mut self.pending_graph_requery);
        self.bt_requery_deadline = None;

        if names.is_empty() {
            return;
        }
        if self.watch_update_tx.is_none() {
            return;
        }
        if self.graph_requery_handle.is_some() {
            for name in names {
                if !self.pending_graph_requery.contains(&name) {
                    self.pending_graph_requery.push(name);
                }
            }
            self.bt_requery_deadline =
                Some(tokio::time::Instant::now() + std::time::Duration::from_millis(100));
            return;
        }

        self.output_manager.lifecycle_event(&format!(
            "re-querying build tool for {} item{}...",
            names.len(),
            if names.len() == 1 { "" } else { "s" }
        ));

        let mut items = Vec::new();
        for name in &names {
            let (bazel, turbo, watch_enabled, item_dir, ignore_patterns) =
                if let Some(rs) = self.services.get(name) {
                    if !rs.resolved.build_tool_watch_enabled() {
                        continue;
                    }
                    (
                        rs.resolved.bazel_config().cloned(),
                        rs.resolved.turbo_config().cloned(),
                        rs.resolved.build_tool_watch_enabled(),
                        rs.resolved.dir.clone(),
                        rs.resolved.ignore.clone(),
                    )
                } else if let Some(rt) = self.tasks.get(name) {
                    if !rt.config.build_tool_watch_enabled() {
                        continue;
                    }
                    (
                        rt.config.bazel.clone(),
                        rt.config.turbo.clone(),
                        rt.config.build_tool_watch_enabled(),
                        rt.config.dir.clone(),
                        rt.config.ignore.clone(),
                    )
                } else {
                    continue;
                };
            if bazel.is_none() && turbo.is_none() {
                continue;
            }
            let working_dir = working_dir_for(&self.base_dir, item_dir.as_deref());
            let ignore_patterns = resolve_watch_ignore_patterns(
                &working_dir,
                &ignore_patterns,
                &self.base_dir,
                &self.config.watch_ignore,
            );
            items.push(GraphRequeryRequestItem {
                name: name.clone(),
                bazel,
                turbo,
                watch_enabled,
                working_dir,
                ignore_patterns,
            });
        }
        if items.is_empty() {
            return;
        }
        let cmd_tx = self.internal_tx.clone();
        let emitter = self.output_manager.clone_lifecycle_emitter();
        let handle = tokio::spawn(async move {
            let outcomes = run_graph_requery_worker(items, emitter).await;
            let _ = cmd_tx
                .send(RunnerInternalCommand::GraphRequeryComplete(outcomes))
                .await;
        });
        self.graph_requery_handle = Some(crate::build_tool::AbortOnDrop::new(handle));
    }

    /// Runs the build (if any), stops the old process, starts a new one.
    /// If the build fails, the old process is kept running.
    /// Broadcasts `RebuildComplete` when done.
    ///
    /// For proxy services: clears the proxy backend (new connections queue),
    /// allocates fresh ephemeral ports, starts the new instance, and sets the
    /// backend once the ready check passes. The proxy never drops — clients
    /// see a brief pause, not a connection refused.
    pub(in crate::runner) async fn handle_rebuild(&mut self, name: &str) {
        self.clear_rebuild_stale(name);
        let rs = match self.services.get(name) {
            Some(rs) => rs,
            None => {
                self.fail_rebuild(name, "rebuild requested for unknown service");
                return;
            }
        };

        // For build-tool-managed services, queue the rebuild into a batch.
        // Multiple services sharing the same source files will be batched into
        // one `bazel build //a //b //c` invocation instead of separate builds.
        //
        // A service that is still mid-build (`Building`, e.g. the initial or
        // first-connection bazel build) is queued too rather than dropped —
        // `flush_pending_rebuilds` holds it until the service has come up, so a
        // file edited during the build still triggers a rebuild instead of
        // being silently lost.
        if rs.resolved.is_build_tool_managed() {
            if !self.pending_bt_rebuilds.contains(&name.to_string()) {
                self.pending_bt_rebuilds.push(name.to_string());
            }
            // Set or extend the batch window (50ms). This allows multiple
            // Rebuild commands from the watch module (which fire per-service
            // after their individual debounce timers) to coalesce.
            self.bt_rebuild_deadline =
                Some(tokio::time::Instant::now() + std::time::Duration::from_millis(50));
            return;
        }

        self.do_rebuild(name).await;
    }

    /// Execute a rebuild for a single service: build, stop old, restart.
    ///
    /// This is the core rebuild logic, called either directly (non-build-tool
    /// services) or after a batch build completes (build-tool services).
    async fn do_rebuild(&mut self, name: &str) {
        // We're committing to a restart, so the process will be brought up to
        // the current artifact — clear the "behind the latest build" flag.
        if let Some(rs) = self.services.get_mut(name) {
            rs.artifact_ahead_of_process = false;
        }
        let resolved = match self.services.get(name) {
            Some(rs) => rs.resolved.clone(),
            None => {
                self.fail_rebuild(name, "rebuild requested for unknown service");
                return;
            }
        };
        // For build-tool-managed services the batch build has already run by
        // the time we reach `do_rebuild`, and the actual restart is surfaced
        // later by `queue_rebuild_service_start`'s "restarting..." event.
        // Emitting another pre-stop "restarting" here just creates log noise.
        //
        // For other kinds, the detached rebuild worker will kick off the
        // build after this lifecycle event, so "rebuilding" still lands
        // before the build output.
        let message = if resolved.is_build_tool_managed() {
            None
        } else {
            Some("rebuilding (file changed)")
        };
        if let Some(message) = message {
            self.output_manager.service_event(name, message);
        }
        if resolved.is_build_tool_managed() {
            self.continue_rebuild_restart(name).await;
            return;
        }
        if let Err(e) = self.spawn_service_rebuild_worker(name, resolved) {
            self.fail_rebuild(name, &e.to_string());
        }
    }
}
