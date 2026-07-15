use super::build_tools::{
    BatchBuildItem, BatchBuildOutcome, BatchBuildReplayItem, run_batch_build_chain,
};
use super::graph::topological_sort;
use super::paths::{resolve_watch_ignore_patterns, working_dir_for};
use super::service_worker::ServiceStartMode;
use super::signals::shutdown_requested;
use super::task_worker::TaskRunMode;
use super::{
    NodeKind, Runner, RunnerCommand, RunnerInternalCommand, RuntimeService, ServiceState,
    TaskItemState, TaskRunIntent,
};
use std::collections::HashMap;

impl Runner {
    /// Apply the outcome of the detached batch-build chain: mutate the
    /// runtime state (watch paths, binary paths, `batch_built` flag) and
    /// transition `Building` items to `Pending` (on success) or `Failed`
    /// (on build failure). The caller is responsible for dropping its
    /// cached batch-build handle. State transitions schedule the normal
    /// pending-item sweep so newly-unblocked items start.
    pub(in crate::runner) fn apply_batch_build_outcome(&mut self, outcome: BatchBuildOutcome) {
        for warning in &outcome.warnings {
            self.output_manager.error_event(warning);
        }

        for (name, kind, paths) in outcome.resolved_watches {
            match kind {
                NodeKind::Service => {
                    if let Some(rs) = self.services.get_mut(&name) {
                        rs.resolved_watch_paths = paths;
                    }
                }
                NodeKind::Task => {
                    if let Some(rt) = self.tasks.get_mut(&name) {
                        rt.resolved_watch_paths = paths;
                    }
                }
            }
        }

        // Binary-path resolution only applies to bazel services — swap in
        // the binary-backed resolved config so subsequent spawns go direct
        // instead of through `bazel run`.
        for (name, path_str) in outcome.binary_paths {
            if let Some(rs) = self.services.get_mut(&name) {
                rs.bazel_binary_path = Some(path_str.clone());
                if let Some(svc) = self.config.services.get(&name) {
                    let mut resolved = svc.resolve_with_bazel_binary(self.platform, &path_str);
                    // Re-expand `depends_on` against the config's service
                    // groups. `resolve_with_bazel_binary` walks back to the
                    // raw user-supplied list (group refs and all) — without
                    // this, a bazel service that lists a group as a dep
                    // ends up with an unexpanded `["mongo-search-deps"]` in
                    // its runtime state, and shutdown's `topological_sort`
                    // bails because the group name isn't a real node.
                    resolved.depends_on = self
                        .config
                        .effective_depends_on(&name, &resolved.depends_on);
                    rs.resolved = resolved;
                }
            }
        }

        for name in outcome.succeeded {
            let was_building = if let Some(rs) = self.services.get_mut(&name) {
                rs.batch_built = true;
                rs.state() == ServiceState::Building
            } else {
                false
            };
            if was_building {
                self.set_service_state(&name, ServiceState::Pending);
                continue;
            }
            if self.tasks.contains_key(&name) {
                self.set_task_state(&name, TaskItemState::Pending);
            }
        }

        for (name, msg) in outcome.failed {
            self.output_manager
                .service_error_event(&name, &format!("batch build failed: {msg}"));
            if self.services.contains_key(&name) {
                self.set_service_state(&name, ServiceState::Failed);
            }
            if self.tasks.contains_key(&name) {
                self.set_task_state(&name, TaskItemState::Failed);
            }
        }
    }

    fn collect_batch_build_item_by_name(&self, name: &str) -> Option<BatchBuildItem> {
        if let Some(rs) = self.services.get(name) {
            if rs.resolved.is_build_tool_managed() {
                return Some(self.build_batch_item(name, NodeKind::Service, rs));
            }
            return None;
        }

        let rt = self.tasks.get(name)?;
        if rt.config.bazel.is_none() && rt.config.turbo.is_none() {
            return None;
        }
        let working_dir = working_dir_for(&self.base_dir, rt.config.dir.as_deref());
        let ignore = resolve_watch_ignore_patterns(
            &working_dir,
            &rt.config.ignore,
            &self.base_dir,
            &self.config.watch_ignore,
        );
        Some(BatchBuildItem {
            name: name.to_string(),
            kind: NodeKind::Task,
            bazel: rt.config.bazel.clone(),
            turbo: rt.config.turbo.clone(),
            watch_enabled: rt.config.build_tool_watch_enabled(),
            working_dir,
            ignore,
        })
    }

    pub(in crate::runner) fn spawn_startup_batch_build(&mut self, items: Vec<BatchBuildItem>) {
        if items.is_empty() {
            return;
        }

        let cmd_tx = self.internal_tx.clone();
        let base_dir = self.base_dir.clone();
        let emitter = self.output_manager.clone_lifecycle_emitter();
        let watch_update_tx = self.watch_update_tx.clone();
        let global_watch_ignore = resolve_watch_ignore_patterns(
            &self.base_dir,
            &[],
            &self.base_dir,
            &self.config.watch_ignore,
        );
        let handle = tokio::spawn(async move {
            let outcome = run_batch_build_chain(
                items,
                base_dir,
                emitter,
                watch_update_tx,
                global_watch_ignore,
            )
            .await;
            let _ = cmd_tx
                .send(RunnerInternalCommand::BatchBuildComplete(outcome))
                .await;
        });
        self.batch_build_handle = Some(crate::build_tool::AbortOnDrop::new(handle));
    }

    pub(in crate::runner) fn spawn_lazy_build(&mut self, name: &str, item: BatchBuildItem) {
        let generation = match self.services.get_mut(name) {
            Some(rs) => {
                rs.start_generation = rs.start_generation.saturating_add(1);
                rs.start_generation
            }
            None => return,
        };
        let cmd_tx = self.internal_tx.clone();
        let base_dir = self.base_dir.clone();
        let emitter = self.output_manager.clone_lifecycle_emitter();
        let watch_update_tx = self.watch_update_tx.clone();
        let global_watch_ignore = resolve_watch_ignore_patterns(
            &self.base_dir,
            &[],
            &self.base_dir,
            &self.config.watch_ignore,
        );
        let svc_name = name.to_string();
        let handle = tokio::spawn(async move {
            let outcome = run_batch_build_chain(
                vec![item],
                base_dir,
                emitter,
                watch_update_tx,
                global_watch_ignore,
            )
            .await;
            let _ = cmd_tx
                .send(RunnerInternalCommand::LazyBuildComplete {
                    name: svc_name,
                    generation,
                    outcome,
                })
                .await;
        });
        self.lazy_build_handles.insert(
            name.to_string(),
            (generation, crate::build_tool::AbortOnDrop::new(handle)),
        );
    }

    pub(in crate::runner) fn schedule_startup_batch_replays(
        &mut self,
        replay_items: &[BatchBuildReplayItem],
    ) {
        let mut replay_batch = Vec::new();

        for replay in replay_items {
            let Some(item) = self.collect_batch_build_item_by_name(&replay.name) else {
                continue;
            };
            let message = match (replay.source_changed, replay.graph_changed, replay.kind) {
                (true, true, NodeKind::Service) => {
                    "files changed during build — rebuilding before start"
                }
                (true, false, NodeKind::Service) => {
                    "source files changed during build — rebuilding before start"
                }
                (false, true, NodeKind::Service) => {
                    "build graph changed during build — rebuilding before start"
                }
                (true, true, NodeKind::Task) => {
                    "files changed during build — re-running build before start"
                }
                (true, false, NodeKind::Task) => {
                    "source files changed during build — re-running build before start"
                }
                (false, true, NodeKind::Task) => {
                    "build graph changed during build — re-running build before start"
                }
                (false, false, _) => continue,
            };
            self.output_manager.service_event(&replay.name, message);
            match replay.kind {
                NodeKind::Service => self.set_service_state(&replay.name, ServiceState::Building),
                NodeKind::Task => self.set_task_state(&replay.name, TaskItemState::Building),
            }
            replay_batch.push(item);
        }

        self.spawn_startup_batch_build(replay_batch);
    }

    pub(in crate::runner) fn schedule_lazy_build_replay(
        &mut self,
        replay: &BatchBuildReplayItem,
    ) -> bool {
        let Some(item) = self.collect_batch_build_item_by_name(&replay.name) else {
            return false;
        };
        let message = match (replay.source_changed, replay.graph_changed) {
            (true, true) => "files changed during build — rebuilding before start",
            (true, false) => "source files changed during build — rebuilding before start",
            (false, true) => "build graph changed during build — rebuilding before start",
            (false, false) => return false,
        };
        self.output_manager.service_event(&replay.name, message);
        self.set_service_state(&replay.name, ServiceState::Building);
        self.spawn_lazy_build(&replay.name, item);
        true
    }

    /// Check if a dependency is satisfied.
    pub(in crate::runner) fn is_dep_satisfied(&self, dep: &str) -> bool {
        if let Some(rs) = self.services.get(dep) {
            return rs.state().is_satisfied();
        }
        if let Some(rt) = self.tasks.get(dep) {
            return rt.dependency_satisfied();
        }
        false
    }

    /// Check if a dependency has failed (including the transitive
    /// `DependencyFailed` cascade — if A fails, B depends on A, C depends
    /// on B, then C also needs to be marked).
    pub(in crate::runner) fn is_dep_failed(&self, dep: &str) -> bool {
        if let Some(rs) = self.services.get(dep) {
            return rs.state().is_failure();
        }
        if let Some(rt) = self.tasks.get(dep) {
            return rt.state().is_failure();
        }
        false
    }

    /// Re-queue items stranded in `DependencyFailed` once every upstream
    /// dependency is satisfied. A dependency that is merely retrying is not
    /// enough; starting descendants before it reaches Ready violates the
    /// same dependency contract that stranded them in the first place.
    fn restore_dependency_failed_items(&mut self) {
        let dep_map = self.build_dep_map();
        let order = match topological_sort(&dep_map) {
            Ok(order) => order,
            Err(_) => return,
        };

        for name in order {
            let service_dep_failed = self
                .services
                .get(&name)
                .is_some_and(|rs| rs.state() == ServiceState::DependencyFailed);
            let task_dep_failed = self
                .tasks
                .get(&name)
                .is_some_and(|rt| rt.state() == TaskItemState::DependencyFailed);
            if (!service_dep_failed && !task_dep_failed) || self.item_waits_for_graph_cycle(&name) {
                continue;
            }

            let deps = dep_map.get(&name).cloned().unwrap_or_default();
            if deps.iter().any(|dep| self.is_dep_failed(dep)) {
                continue;
            }
            if deps.iter().any(|dep| !self.is_dep_satisfied(dep)) {
                continue;
            }

            if service_dep_failed {
                // A lazy service can only reach DependencyFailed after a
                // connection moved it out of Lazy. Returning it to Pending
                // preserves that queued request and lets the normal scheduler
                // resume it without a separate lazy-request flag.
                self.set_service_state(&name, ServiceState::Pending);
            } else if task_dep_failed {
                self.set_task_state(&name, TaskItemState::Pending);
            }

            self.output_manager
                .service_debug_event(&name, "dependency recovered; re-queued");
        }
    }

    /// Ask the runner loop to re-check `Pending` items on its own task.
    pub(in crate::runner) fn schedule_start_pending(&self) {
        let _ = self.cmd_tx.send(RunnerCommand::StartPending);
    }

    /// Recover descendants stranded in `DependencyFailed`. Transitioning them
    /// to `Pending` schedules the normal pending-item sweep.
    pub(in crate::runner) fn unblock_dependency_failed_items(&mut self) {
        self.restore_dependency_failed_items();
    }

    /// Snapshot of a batch-buildable service or task — everything the
    /// standalone [`run_batch_build_chain`] needs. Taken at startup before
    /// the detached task runs so the task doesn't touch `self`.
    pub(in crate::runner) fn collect_batch_build_items(&self) -> Vec<BatchBuildItem> {
        let mut items: Vec<BatchBuildItem> = Vec::new();

        for (name, rs) in &self.services {
            if !rs.resolved.is_build_tool_managed() {
                continue;
            }
            // Lazy bazel/turbo services defer their query+build+cquery to
            // first connection (JIT in the `lazy_start_rx` handler). Pulling
            // them into the startup batch would query and build services
            // the user may never touch this session.
            if rs.resolved.lazy {
                continue;
            }
            items.push(self.build_batch_item(name, NodeKind::Service, rs));
        }
        for (name, rt) in &self.tasks {
            if rt.config.bazel.is_none() && rt.config.turbo.is_none() {
                continue;
            }
            let working_dir = working_dir_for(&self.base_dir, rt.config.dir.as_deref());
            let ignore = resolve_watch_ignore_patterns(
                &working_dir,
                &rt.config.ignore,
                &self.base_dir,
                &self.config.watch_ignore,
            );
            items.push(BatchBuildItem {
                name: name.clone(),
                kind: NodeKind::Task,
                bazel: rt.config.bazel.clone(),
                turbo: rt.config.turbo.clone(),
                watch_enabled: rt.config.build_tool_watch_enabled(),
                working_dir,
                ignore,
            });
        }

        items
    }

    /// Snapshot a single service into a [`BatchBuildItem`] for the JIT
    /// lazy-build path. Shares the field layout with
    /// [`Self::collect_batch_build_items`] so the chain logic doesn't care
    /// whether the build is startup-batched or JIT.
    pub(in crate::runner) fn build_batch_item(
        &self,
        name: &str,
        kind: NodeKind,
        rs: &RuntimeService,
    ) -> BatchBuildItem {
        let working_dir = working_dir_for(&self.base_dir, rs.resolved.dir.as_deref());
        let ignore = resolve_watch_ignore_patterns(
            &working_dir,
            &rs.resolved.ignore,
            &self.base_dir,
            &self.config.watch_ignore,
        );
        BatchBuildItem {
            name: name.to_string(),
            kind,
            bazel: rs.resolved.bazel_config().cloned(),
            turbo: rs.resolved.turbo_config().cloned(),
            watch_enabled: rs.resolved.build_tool_watch_enabled(),
            working_dir,
            ignore,
        }
    }

    /// Start every Pending service or task whose dependencies are satisfied.
    ///
    /// This is the only dependency scheduler. Initial services begin in
    /// `Pending`, while lazy services enter `Pending` on their first proxy
    /// connection; both are claimed and launched by this same sweep.
    pub(in crate::runner) async fn start_pending_items(&mut self) {
        let dep_map = self.build_dep_map();
        let order = match topological_sort(&dep_map) {
            Ok(o) => o,
            Err(_) => return,
        };

        let failed_items: Vec<String> = order
            .iter()
            .filter(|name| self.is_item_pending(name))
            .filter(|name| {
                dep_map
                    .get(name.as_str())
                    .is_some_and(|deps| deps.iter().any(|dep| self.is_dep_failed(dep)))
            })
            .cloned()
            .collect();

        for name in failed_items {
            self.set_service_state(&name, ServiceState::DependencyFailed);
            self.set_task_state(&name, TaskItemState::DependencyFailed);
            self.output_manager
                .service_error_event(&name, "skipped (dependency failed)");
        }

        let mut ready: Vec<String> = order
            .iter()
            .filter(|name| self.is_item_pending(name))
            .filter(|name| {
                dep_map
                    .get(name.as_str())
                    .is_none_or(|deps| deps.iter().all(|dep| self.is_dep_satisfied(dep)))
            })
            .cloned()
            .collect();

        // Foreground tasks own the terminal exclusively. If one is ready,
        // claim only it in this sweep; its completion schedules the next one.
        if let Some(foreground_name) = ready.iter().find(|name| {
            self.tasks
                .get(name.as_str())
                .is_some_and(|rt| rt.config.terminal.is_foreground())
        }) {
            ready = vec![foreground_name.clone()];
        }

        let Some(done_tx) = self.done_tx.clone() else {
            return;
        };

        for name in ready {
            if shutdown_requested() {
                return;
            }

            let is_pending_svc = self
                .services
                .get(&name)
                .is_some_and(|rs| rs.state() == ServiceState::Pending);
            let is_pending_task = self
                .tasks
                .get(&name)
                .is_some_and(|rt| rt.state() == TaskItemState::Pending);

            if is_pending_svc {
                // A build-tool-managed lazy service takes a JIT build detour.
                // Successful completion returns it to Pending, where this
                // scheduler checks dependencies again before starting it.
                if self.start_lazy_build_if_needed(&name) {
                    continue;
                }
                self.output_manager
                    .service_debug_event(&name, "start triggered (deps satisfied)");
                if let Err(e) = self.queue_scheduled_service_start(
                    &name,
                    done_tx.clone(),
                    ServiceStartMode::Full,
                ) {
                    self.set_service_state(&name, ServiceState::Failed);
                    self.output_manager
                        .service_error_event(&name, &e.to_string());
                }
                continue;
            }

            if !is_pending_task {
                continue;
            }

            let Some((task_cfg, needs_startup_evaluation)) = self
                .tasks
                .get(&name)
                .map(|rt| (rt.config.clone(), !rt.dependency_evaluated))
            else {
                continue;
            };

            if self.graph_cycle_executes_task(&name) {
                if let Err(e) = self.spawn_task_worker(
                    &name,
                    task_cfg,
                    HashMap::new(),
                    TaskRunMode::Triggered,
                    TaskRunIntent::Scheduled {
                        done_tx: done_tx.clone(),
                    },
                ) {
                    self.set_task_state(&name, TaskItemState::Failed);
                    self.output_manager
                        .service_error_event(&name, &e.to_string());
                }
            } else if needs_startup_evaluation {
                let has_dependents = dep_map
                    .values()
                    .any(|deps| deps.iter().any(|dep| dep == &name));
                if let Err(e) = self.spawn_task_worker(
                    &name,
                    task_cfg,
                    HashMap::new(),
                    TaskRunMode::Startup { has_dependents },
                    TaskRunIntent::Scheduled {
                        done_tx: done_tx.clone(),
                    },
                ) {
                    self.set_task_state(&name, TaskItemState::Failed);
                    self.output_manager
                        .service_error_event(&name, &e.to_string());
                }
            } else {
                self.handle_task_rerun(&name).await;
            }
        }
    }

    fn is_item_pending(&self, name: &str) -> bool {
        if self.item_waits_for_graph_cycle(name) {
            return false;
        }
        self.services
            .get(name)
            .is_some_and(|rs| rs.state() == ServiceState::Pending)
            || self
                .tasks
                .get(name)
                .is_some_and(|rt| rt.state() == TaskItemState::Pending && rt.run_worker.is_none())
    }

    /// Whether every item participating in initial startup has settled.
    /// Lazy services are listeners, not startup work, even if a connection
    /// happens to request one while the initial graph is still progressing.
    pub(in crate::runner) fn initial_startup_settled(&self) -> bool {
        let service_work = self.services.values().any(|rs| {
            !rs.resolved.lazy
                && matches!(
                    rs.state(),
                    ServiceState::Pending
                        | ServiceState::Building
                        | ServiceState::Starting
                        | ServiceState::Running
                )
        });
        let task_work = self.tasks.values().any(|rt| {
            matches!(
                rt.state(),
                TaskItemState::Pending | TaskItemState::Building | TaskItemState::Running
            )
        });

        !service_work && !task_work
    }
}
