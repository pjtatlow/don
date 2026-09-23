//! Reading the startup picture out of the merged facts.
//!
//! Nothing decides here. Every process reconciles its own dependencies against
//! the snapshot — a supervisor strands or re-queues *itself*, because the roots
//! it needs are one hop away in facts it can already read. What is left is the
//! whole-stack question no single process can answer: has startup finished?

use super::{Runner, ServiceState, TaskState};
use crate::config::Dependency;
use crate::facts::Phase;

impl Runner {
    /// Whether one `depends_on` edge no longer blocks its dependent.
    pub(in crate::runner) fn is_dep_gate_open(&self, dep: &Dependency) -> bool {
        let facts = self.facts_snapshot();
        facts.satisfied(&dep.name) || (!dep.blocking && facts.settled(&dep.name))
    }

    /// Whether a dependency has failed, itself or transitively.
    pub(in crate::runner) fn is_dep_failed(&self, dep: &str) -> bool {
        !self.facts_snapshot().failed_roots(dep).is_empty()
    }

    /// Whether every process participating in initial startup has settled.
    ///
    /// Lazy services are listeners, not startup work, even if a connection
    /// happens to request one while the initial graph is still progressing.
    pub(in crate::runner) fn initial_startup_settled(&self) -> bool {
        let lazy: std::collections::HashSet<&str> = self
            .services
            .iter()
            .filter(|(_, rs)| rs.resolved.lazy)
            .map(|(name, _)| name.as_str())
            .collect();
        !self.facts_snapshot().iter().any(|(name, facts)| {
            facts.report_pending
                || match facts.phase {
                    Phase::Service(state) => {
                        !lazy.contains(name.as_str())
                            && matches!(
                                state,
                                ServiceState::Pending
                                    | ServiceState::Building
                                    | ServiceState::Starting
                                    | ServiceState::Running
                            )
                    }
                    Phase::Task(state) => matches!(
                        state,
                        TaskState::Pending | TaskState::Building | TaskState::Running
                    ),
                }
        })
    }
}
