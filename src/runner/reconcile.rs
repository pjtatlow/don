use super::graph::topological_sort;
use super::{
    CommandError, CommandResult, Runner, RunnerEvent, ServiceState, ServiceStopAction,
    TaskItemState,
};
use std::collections::HashSet;
use tokio::sync::oneshot;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Quiescing,
    Stopping,
    Executing,
    Cleanup,
}

pub(in crate::runner) enum DeferredGraphCommand {
    Task(String),
    Service(String),
    BuildGraph(String),
}

pub(in crate::runner) struct GraphCycle {
    root: String,
    watch_triggered: bool,
    affected: HashSet<String>,
    targets: HashSet<String>,
    stop_order: Vec<String>,
    stop_index: usize,
    failed_services: HashSet<String>,
    completion: Option<oneshot::Sender<CommandResult>>,
    phase: Phase,
}

impl Runner {
    fn graph_cycle_contains(&self, name: &str) -> bool {
        self.graph_cycle
            .as_ref()
            .is_some_and(|cycle| cycle.affected.contains(name))
    }

    fn defer_graph_command(&mut self, command: DeferredGraphCommand) -> bool {
        if self.graph_cycle.is_none() {
            return false;
        }
        self.deferred_graph_commands.push_back(command);
        true
    }

    pub(in crate::runner) async fn handle_task_watch_trigger(&mut self, name: &str) {
        if self.defer_graph_command(DeferredGraphCommand::Task(name.to_string())) {
            return;
        }
        if self
            .tasks
            .get(name)
            .is_some_and(|task| task.config.reconcile_dependents)
        {
            self.begin_graph_cycle(name, None, true);
            self.advance_graph_cycle().await;
        } else {
            self.handle_task_rerun(name).await;
        }
    }

    pub(in crate::runner) async fn handle_service_watch_trigger(&mut self, name: &str) {
        if self
            .failed_graph_cycles
            .values()
            .any(|targets| targets.contains(name))
        {
            let _ = self.event_tx.send(RunnerEvent::RebuildComplete {
                name: name.to_string(),
                success: false,
            });
            return;
        }
        if self.defer_graph_command(DeferredGraphCommand::Service(name.to_string())) {
            return;
        }
        if self.services.get(name).is_some_and(|service| {
            matches!(service.state(), ServiceState::Lazy | ServiceState::Stopped)
        }) {
            let _ = self.event_tx.send(RunnerEvent::RebuildComplete {
                name: name.to_string(),
                success: true,
            });
        } else {
            self.handle_rebuild(name).await;
        }
    }

    pub(in crate::runner) async fn handle_build_graph_watch_trigger(&mut self, name: &str) {
        if !self.defer_graph_command(DeferredGraphCommand::BuildGraph(name.to_string())) {
            self.handle_build_graph_changed(name).await;
        }
    }

    pub(in crate::runner) async fn handle_manual_graph_cycle(
        &mut self,
        name: &str,
        wait: bool,
        wait_timeout: Option<String>,
        reply: oneshot::Sender<CommandResult>,
    ) {
        if wait_timeout.is_some() {
            let _ = reply.send(Err(CommandError::InvalidParams {
                name: name.to_string(),
                message: "dependency reconciliation does not support wait timeouts".into(),
            }));
            return;
        }
        if self.graph_cycle.is_some() {
            let _ = reply.send(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "a dependency reconciliation is already running".to_string(),
            }));
            return;
        }
        let completion = if wait {
            Some(reply)
        } else {
            let _ = reply.send(Ok(()));
            None
        };
        self.begin_graph_cycle(name, completion, false);
        self.advance_graph_cycle().await;
    }

    pub(in crate::runner) fn begin_graph_cycle(
        &mut self,
        root: &str,
        completion: Option<oneshot::Sender<CommandResult>>,
        watch_triggered: bool,
    ) {
        let dep_map = self.build_dep_map();
        let mut stop_order = topological_sort(&dep_map).unwrap_or_default();
        let mut affected = HashSet::from([root.to_string()]);
        for name in &stop_order {
            if dep_map
                .get(name)
                .is_some_and(|deps| deps.iter().any(|dep| affected.contains(dep)))
            {
                affected.insert(name.clone());
            }
        }
        let targets = self
            .failed_graph_cycles
            .remove(root)
            .unwrap_or_else(|| self.active_cycle_targets(&affected));
        stop_order.retain(|name| targets.contains(name));
        stop_order.reverse();
        for name in &targets {
            if let Some(service) = self.services.get_mut(name) {
                service.stop_health_tracking();
            }
        }
        self.graph_cycle = Some(GraphCycle {
            root: root.to_string(),
            watch_triggered,
            affected,
            targets,
            stop_order,
            stop_index: 0,
            failed_services: HashSet::new(),
            completion,
            phase: Phase::Quiescing,
        });
    }

    fn active_cycle_targets(&self, affected: &HashSet<String>) -> HashSet<String> {
        affected
            .iter()
            .filter(|name| {
                self.services.get(name.as_str()).is_some_and(|service| {
                    let state = service.state();
                    state.is_live()
                        || (state.is_transitioning() && state != ServiceState::Stopping)
                        || (state == ServiceState::Stopping
                            && !matches!(service.stop_action, ServiceStopAction::None))
                })
            })
            .cloned()
            .collect()
    }

    pub(in crate::runner) fn graph_cycle_owns_service(&self, name: &str) -> bool {
        self.services.contains_key(name) && self.graph_cycle_contains(name)
    }

    pub(in crate::runner) fn graph_cycle_owns_task(&self, name: &str) -> bool {
        self.tasks.contains_key(name) && self.graph_cycle_contains(name)
    }

    pub(in crate::runner) fn graph_cycle_executes_task(&self, name: &str) -> bool {
        self.graph_cycle
            .as_ref()
            .is_some_and(|cycle| cycle.phase == Phase::Executing && cycle.affected.contains(name))
    }

    pub(in crate::runner) fn item_waits_for_graph_cycle(&self, name: &str) -> bool {
        self.failed_graph_cycles
            .values()
            .any(|targets| targets.contains(name))
            || self.graph_cycle.as_ref().is_some_and(|cycle| {
                cycle.phase != Phase::Executing && cycle.affected.contains(name)
            })
    }

    pub(in crate::runner) fn graph_cycle_service_control_reply(
        &self,
        name: &str,
        reply: oneshot::Sender<CommandResult>,
    ) -> Option<oneshot::Sender<CommandResult>> {
        if self.graph_cycle_owns_service(name) {
            let _ = reply.send(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "service is owned by dependency reconciliation".to_string(),
            }));
            None
        } else {
            Some(reply)
        }
    }

    pub(in crate::runner) fn record_graph_cycle_stop_result(
        &mut self,
        name: &str,
        result: &Result<(), String>,
    ) {
        if let Some(cycle) = self.graph_cycle.as_mut()
            && cycle.targets.contains(name)
            && result.is_err()
        {
            cycle.failed_services.insert(name.to_string());
            cycle.phase = Phase::Cleanup;
            cycle.stop_index = 0;
        }
    }

    pub(in crate::runner) async fn advance_graph_cycle(&mut self) {
        loop {
            let Some(phase) = self.graph_cycle.as_ref().map(|cycle| cycle.phase) else {
                return;
            };
            match phase {
                Phase::Quiescing if !self.graph_cycle_quiescent() => return,
                Phase::Quiescing => {
                    if let Some(cycle) = self.graph_cycle.as_mut() {
                        cycle.phase = Phase::Stopping;
                    }
                }
                Phase::Stopping | Phase::Cleanup => {
                    self.stop_next_cycle_service().await;
                    if self
                        .graph_cycle
                        .as_ref()
                        .is_some_and(|cycle| cycle.stop_index < cycle.stop_order.len())
                    {
                        return;
                    }
                    if phase == Phase::Cleanup {
                        self.finish_graph_cycle(false).await;
                    } else {
                        self.start_graph_cycle().await;
                    }
                }
                Phase::Executing => match self.graph_cycle_outcome() {
                    Some(true) => self.finish_graph_cycle(true).await,
                    Some(false) => {
                        if let Some(cycle) = self.graph_cycle.as_mut() {
                            cycle.phase = Phase::Cleanup;
                            cycle.stop_index = 0;
                        }
                    }
                    None => return,
                },
            }
        }
    }

    fn graph_cycle_quiescent(&self) -> bool {
        let Some(cycle) = self.graph_cycle.as_ref() else {
            return true;
        };
        !cycle.affected.iter().any(|name| {
            self.tasks.get(name).is_some_and(|task| {
                task.run_worker.is_some()
                    || matches!(
                        task.state(),
                        TaskItemState::Building | TaskItemState::Running
                    )
            })
        }) && !cycle.affected.iter().any(|name| {
            self.services.get(name).is_some_and(|service| {
                service.start_worker.is_some()
                    || service.control_worker.is_some()
                    || service.rebuild_worker.is_some()
                    || self.lazy_build_handles.contains_key(name)
                    || (service.state().is_transitioning()
                        && service.state() != ServiceState::Pending)
                    || service.state() == ServiceState::Running
            })
        }) && self.rebuild_batch_handle.is_none()
            && !self
                .pending_bt_rebuilds
                .iter()
                .any(|name| cycle.affected.contains(name))
    }

    async fn stop_next_cycle_service(&mut self) {
        loop {
            let Some(cycle) = self.graph_cycle.as_mut() else {
                return;
            };
            let Some(name) = cycle.stop_order.get(cycle.stop_index).cloned() else {
                return;
            };
            if self
                .services
                .get(&name)
                .is_some_and(|service| service.state() == ServiceState::Stopped)
            {
                cycle.stop_index += 1;
                continue;
            }
            if self
                .services
                .get(&name)
                .is_some_and(|service| service.state() == ServiceState::Failed)
                && let Some(cycle) = self.graph_cycle.as_mut()
            {
                cycle.failed_services.insert(name.clone());
            }
            if self.services.get(&name).is_some_and(|service| {
                service.control_worker.is_some()
                    || service.start_worker.is_some()
                    || service.rebuild_worker.is_some()
            }) {
                return;
            }
            let handle = self.services.get_mut(&name).and_then(|service| {
                service.stop_health_tracking();
                service.handle.take()
            });
            let Some(handle) = handle else {
                self.set_service_state(&name, ServiceState::Stopped);
                continue;
            };
            if self.remove_attach_lock(&name) {
                self.output_manager.resume_stdout_sink(&name).await;
            }
            self.set_service_state(&name, ServiceState::Stopping);
            let (reply, _rx) = oneshot::channel();
            self.spawn_manual_service_stop_worker(
                &name,
                handle,
                self.effective_shutdown_config(&name),
                false,
                reply,
                ServiceStopAction::None,
            );
            return;
        }
    }

    async fn start_graph_cycle(&mut self) {
        let Some(cycle) = self.graph_cycle.as_ref() else {
            return;
        };
        let affected = cycle.affected.clone();
        let targets = cycle.targets.clone();
        for name in affected {
            if let Some(task) = self.tasks.get_mut(&name) {
                task.set_needs_run_now(true);
                self.set_task_state(&name, TaskItemState::Pending);
            }
        }
        for name in targets {
            if let Some(service) = self.services.get_mut(&name) {
                service.reset_restart_tracking();
            }
            self.set_service_state(&name, ServiceState::Pending);
        }
        if let Some(cycle) = self.graph_cycle.as_mut() {
            cycle.phase = Phase::Executing;
        }
        self.start_pending_items().await;
    }

    fn graph_cycle_outcome(&self) -> Option<bool> {
        let cycle = self.graph_cycle.as_ref()?;
        let task_failed = cycle
            .affected
            .iter()
            .filter_map(|name| self.tasks.get(name))
            .any(|task| task.state().is_failure() || task.state() == TaskItemState::PendingRun);
        let service_failed = cycle
            .targets
            .iter()
            .filter_map(|name| self.services.get(name))
            .any(|service| service.state().is_failure());
        if task_failed || service_failed {
            return Some(false);
        }
        let complete = cycle.affected.iter().all(|name| {
            self.tasks
                .get(name)
                .is_none_or(|task| task.dependency_satisfied())
        }) && cycle.targets.iter().all(|name| {
            self.services
                .get(name)
                .is_some_and(|service| service.state().is_satisfied())
        });
        complete.then_some(true)
    }

    async fn finish_graph_cycle(&mut self, success: bool) {
        let Some(mut cycle) = self.graph_cycle.take() else {
            return;
        };
        if let Some(completion) = cycle.completion.take() {
            let result = if success {
                Ok(())
            } else {
                Err(CommandError::Failed {
                    name: cycle.root.clone(),
                    message: "dependency reconciliation failed".to_string(),
                })
            };
            let _ = completion.send(result);
        }
        if cycle.watch_triggered {
            let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                name: cycle.root.clone(),
                success,
            });
        }
        if !success {
            for name in &cycle.targets {
                let state = if cycle.failed_services.contains(name) {
                    ServiceState::Failed
                } else {
                    ServiceState::DependencyFailed
                };
                self.set_service_state(name, state);
            }
            self.failed_graph_cycles
                .insert(cycle.root.clone(), cycle.targets);
        }
        self.replay_deferred_graph_command().await;
    }

    async fn replay_deferred_graph_command(&mut self) {
        while self.graph_cycle.is_none() {
            let Some(command) = self.deferred_graph_commands.pop_front() else {
                return;
            };
            match command {
                DeferredGraphCommand::Task(name) => {
                    if self
                        .tasks
                        .get(&name)
                        .is_some_and(|task| task.config.reconcile_dependents)
                    {
                        self.begin_graph_cycle(&name, None, true);
                    } else {
                        self.handle_task_rerun(&name).await;
                    }
                }
                DeferredGraphCommand::Service(name) => {
                    self.handle_service_watch_trigger(&name).await
                }
                DeferredGraphCommand::BuildGraph(name) => {
                    self.handle_build_graph_changed(&name).await
                }
            }
        }
    }
}
