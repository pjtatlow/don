use super::build_tools::{
    BatchBuildItem, BatchBuildOutcome, BatchBuildReplayItem, run_batch_build_chain,
};
use super::graph::{dep_name_map, topological_sort};
use super::paths::{resolve_watch_ignore_patterns, working_dir_for};
use super::service_worker::ServiceStartMode;
use super::signals::shutdown_requested;
use super::task_worker::TaskRunMode;
use super::{
    NodeKind, Runner, RunnerCommand, RunnerInternalCommand, RuntimeService, ServiceState,
    TaskItemState, TaskRunIntent,
};
use crate::config::Dependency;
use std::collections::HashMap;

fn push_unique_name(names: &mut Vec<String>, name: &str) {
    if !names.iter().any(|existing| existing == name) {
        names.push(name.to_string());
    }
}

fn format_dependency_failure(dependencies: &[String]) -> String {
    match dependencies {
        [dependency] => format!("dependency '{dependency}' failed"),
        dependencies => format!("dependencies '{}' failed", dependencies.join("', '")),
    }
}

fn format_non_blocking_dependencies(dependencies: &[String]) -> String {
    match dependencies {
        [dependency] => format!("dependency '{dependency}'"),
        dependencies => format!("dependencies '{}'", dependencies.join("', '")),
    }
}

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
            if rs.resolved.is_build_tool_managed() && !rs.batch_built {
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

    /// Whether a dependency has stopped making progress: it either failed or
    /// was stopped, and nothing is going to move it to a satisfied state
    /// without another explicit request. Only non-blocking (ordering-only)
    /// edges use this — it is what lets a dependent start anyway.
    fn is_dep_settled(&self, dep: &str) -> bool {
        if let Some(rs) = self.services.get(dep) {
            return matches!(
                rs.state(),
                ServiceState::Failed | ServiceState::DependencyFailed | ServiceState::Stopped
            );
        }
        if let Some(rt) = self.tasks.get(dep) {
            // `PendingRun`/`Skipped` are settled too: the task is waiting for
            // a manual trigger (or was judged unnecessary) and will not run on
            // its own, so a non-blocking dependent would otherwise wait
            // forever.
            return matches!(
                rt.state(),
                TaskItemState::Failed
                    | TaskItemState::DependencyFailed
                    | TaskItemState::PendingRun
                    | TaskItemState::Skipped
            );
        }
        false
    }

    /// Whether one `depends_on` edge no longer blocks its dependent.
    ///
    /// A blocking edge opens only when the dependency is satisfied. A
    /// non-blocking edge is ordering-only: it also opens once the dependency
    /// has settled into a failed or stopped state, so the dependent still
    /// starts *after* it, but is not held hostage by it.
    pub(in crate::runner) fn is_dep_gate_open(&self, dep: &Dependency) -> bool {
        if self.is_dep_satisfied(&dep.name) {
            return true;
        }
        !dep.blocking && self.is_dep_settled(&dep.name)
    }

    /// Announce the non-blocking dependencies this item is not waiting for.
    fn report_skipped_non_blocking_dependencies(&self, name: &str, skipped: &[String]) {
        if skipped.is_empty() {
            return;
        }
        self.output_manager.service_event(
            name,
            &format!(
                "starting without non-blocking {}",
                format_non_blocking_dependencies(skipped)
            ),
        );
    }

    /// Non-blocking dependencies that settled unsuccessfully — reported when
    /// the dependent starts anyway so the log explains why it didn't wait.
    fn skipped_non_blocking_dependencies(&self, dependencies: &[Dependency]) -> Vec<String> {
        dependencies
            .iter()
            .filter(|dep| !dep.blocking && !self.is_dep_satisfied(&dep.name))
            .filter(|dep| self.is_dep_settled(&dep.name))
            .map(|dep| dep.name.clone())
            .collect()
    }

    /// Check if a dependency has failed (including the transitive
    /// `DependencyFailed` cascade — if A fails, B depends on A, C depends
    /// on B, then C also needs to be marked).
    pub(in crate::runner) fn is_dep_failed(&self, dep: &str) -> bool {
        if let Some(rs) = self.services.get(dep) {
            return matches!(
                rs.state(),
                ServiceState::Failed | ServiceState::DependencyFailed
            );
        }
        if let Some(rt) = self.tasks.get(dep) {
            return matches!(
                rt.state(),
                TaskItemState::Failed | TaskItemState::DependencyFailed
            );
        }
        false
    }

    /// Ask the runner loop to re-check `Pending` items on its own task.
    pub(in crate::runner) fn schedule_start_pending(&self) {
        let _ = self.cmd_tx.send(RunnerCommand::StartPending);
    }

    /// Resolve failed direct dependencies to their root failures. An
    /// intermediate `DependencyFailed` item contributes the roots it already
    /// recorded, so a chain such as api -> worker -> db reports `db`.
    ///
    /// Non-blocking edges are ignored: their whole point is that a failure on
    /// the other end must not cascade.
    fn failed_dependency_roots(&self, dependencies: &[Dependency]) -> Vec<String> {
        let mut roots = Vec::new();
        for dependency in dependencies.iter().filter(|dep| dep.blocking) {
            let dependency = &dependency.name;
            let inherited = if let Some(rs) = self.services.get(dependency) {
                match rs.state() {
                    ServiceState::Failed => Some(std::slice::from_ref(dependency)),
                    ServiceState::DependencyFailed => Some(rs.failed_dependencies()),
                    _ => None,
                }
            } else if let Some(rt) = self.tasks.get(dependency) {
                match rt.state() {
                    TaskItemState::Failed => Some(std::slice::from_ref(dependency)),
                    TaskItemState::DependencyFailed => Some(rt.failed_dependencies()),
                    _ => None,
                }
            } else {
                None
            };

            let Some(inherited) = inherited else {
                continue;
            };
            if inherited.is_empty() {
                push_unique_name(&mut roots, dependency);
                continue;
            }
            for root in inherited {
                push_unique_name(&mut roots, root);
            }
        }
        roots
    }

    /// Refresh dependency-failure causes and return recovered items to the
    /// pending scheduler. Iterating in topological order lets a root-cause
    /// update flow through every descendant in one sweep.
    fn reconcile_dependency_failures(
        &mut self,
        dep_map: &HashMap<String, Vec<Dependency>>,
        order: &[String],
    ) {
        for name in order {
            let service_state = self.services.get(name).map(RuntimeService::state);
            let task_state = self.tasks.get(name).map(|rt| rt.state());
            let is_pending = service_state == Some(ServiceState::Pending)
                || task_state == Some(TaskItemState::Pending);
            let is_dependency_failed = service_state == Some(ServiceState::DependencyFailed)
                || task_state == Some(TaskItemState::DependencyFailed);
            if !is_pending && !is_dependency_failed {
                continue;
            }

            let dependencies = dep_map.get(name).map(Vec::as_slice).unwrap_or_default();
            let failed_dependencies = self.failed_dependency_roots(dependencies);
            if !failed_dependencies.is_empty() {
                let state_changed = if service_state.is_some() {
                    self.mark_service_dependency_failed(name, failed_dependencies.clone())
                } else {
                    self.mark_task_dependency_failed(name, failed_dependencies.clone())
                };
                if state_changed {
                    self.output_manager.service_error_event(
                        name,
                        &format!(
                            "skipped ({})",
                            format_dependency_failure(&failed_dependencies)
                        ),
                    );
                }
                continue;
            }

            if is_dependency_failed {
                if service_state.is_some() {
                    // A lazy service reaches DependencyFailed only after a
                    // connection moved it out of Lazy. Pending preserves that
                    // queued request for the normal scheduler. That scheduler
                    // still waits for every dependency gate to open —
                    // blocking edges on a satisfied dependency, non-blocking
                    // ones on a dependency that has settled either way.
                    self.set_service_state(name, ServiceState::Pending);
                } else {
                    self.set_task_state(name, TaskItemState::Pending);
                }
                self.output_manager
                    .service_debug_event(name, "dependency recovered; re-queued");
            }
        }
    }

    /// Snapshot of a batch-buildable service or task — everything the
    /// standalone [`run_batch_build_chain`] needs. Taken at startup before
    /// the detached task runs so the task doesn't touch `self`.
    pub(in crate::runner) fn collect_batch_build_items(&self) -> Vec<BatchBuildItem> {
        let mut items: Vec<BatchBuildItem> = Vec::new();

        for (name, rs) in &self.services {
            if !rs.resolved.is_build_tool_managed() || rs.batch_built {
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
        let order = match topological_sort(&dep_name_map(&dep_map)) {
            Ok(o) => o,
            Err(_) => return,
        };

        self.reconcile_dependency_failures(&dep_map, &order);

        let mut ready: Vec<String> = order
            .iter()
            .filter(|name| self.is_item_pending(name))
            .filter(|name| {
                dep_map
                    .get(name.as_str())
                    .is_none_or(|deps| deps.iter().all(|dep| self.is_dep_gate_open(dep)))
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

            // Non-blocking dependencies we are deliberately not waiting on.
            // Reported at the moment the item actually starts, so a start
            // that follows a visible failure doesn't look like don ignored
            // the dependency graph.
            let skipped = dep_map
                .get(&name)
                .map(|deps| self.skipped_non_blocking_dependencies(deps))
                .unwrap_or_default();

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
                self.report_skipped_non_blocking_dependencies(&name, &skipped);
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

            self.report_skipped_non_blocking_dependencies(&name, &skipped);
            if needs_startup_evaluation {
                // Only a *blocking* dependent makes a manual task worth
                // parking for. A non-blocking dependent is happy either way,
                // so counting it would park the task as "required by
                // dependents" and then block the very dependent that didn't
                // care.
                let has_dependents = dep_map
                    .values()
                    .any(|deps| deps.iter().any(|dep| dep.blocking && dep.name == name));
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
