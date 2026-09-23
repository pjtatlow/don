//! What each process says about itself, and the one consistent picture those
//! answers add up to.
//!
//! This is the vocabulary that replaces the scheduler's fold. A supervisor
//! knows everything about its own process — what phase it is in, whether a
//! dependent may treat it as up, what it is stranded behind — because every
//! input those questions need is something it observed itself. So it says so,
//! and anything that cares *observes*. Nothing decides on the way through.
//!
//! [`Aggregator`] merges what every process publishes into one
//! [`FactsSnapshot`]. It has no branch on the *content* of what it merges —
//! that discipline is what keeps it from growing back into a scheduler.
//!
//! # Why one snapshot rather than a value per peer
//!
//! A process could read each dependency separately, and that works for one
//! dependency. It breaks for two: `api` depends on `db` and `cache`, reads
//! them from separate places, and can see `db` as it was a moment ago next to
//! `cache` as it is now. Acting on a torn read starts a service whose
//! dependency is mid-restart.
//!
//! So the facts are read as one [`FactsSnapshot`], covering every process at a
//! single instant. That is the consistency the scheduler used to provide by
//! owning every state in one `HashMap` — without owning any of them.
//!
//! # Why publishing is deduplicated
//!
//! Facts flow in a cycle: a process publishes, the merge republishes, every
//! dependent wakes and recomputes, and some of them publish. If a republish
//! that changed nothing still counted as a change, that cycle would never
//! settle — N supervisors would spin forever recomputing the same answer.
//!
//! Both halves therefore drop no-ops: [`FactsPublisher::publish`] sends only
//! when this process's facts actually differ, and [`Aggregator::apply`] swaps
//! the snapshot only when the merge differs. `ProcessFacts` derives
//! `PartialEq` for exactly that reason — it is load-bearing, not convenience.
//! Because the dependency graph is a validated DAG, a change then propagates
//! at most one level per round and settles in at most `depth` rounds.

use crate::process::{ServiceState, TaskState};
use crate::state_store::ServiceRuntime;
use crate::task_state::TaskRunInfo;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

/// A process's lifecycle phase, in the vocabulary of its kind.
///
/// Both vocabularies are unchanged from when the scheduler owned them —
/// including `Pending` and `DependencyFailed`, which look like scheduling
/// verdicts and are not. A supervisor that can read a [`FactsSnapshot`]
/// derives both from its own demand and its own dependencies' facts, which is
/// why nothing above it needs to exist.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    Service(ServiceState),
    Task(TaskState),
}

/// Per-kind runtime detail: what this process's supervisor holds right now.
///
/// The snapshot *is* the record of custody, not a copy of one — there is no
/// second place a pid is written, so there is nothing for it to disagree with.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub(crate) enum Detail {
    /// Nothing held, and no kind-specific detail to report yet.
    #[default]
    None,
    Service {
        runtime: Option<ServiceRuntime>,
    },
    Task {
        pid: Option<i32>,
        last_run: Option<TaskRunInfo>,
    },
}

/// Everything one process says about itself.
///
/// The three booleans below are deliberately answered *here* rather than
/// derived by each dependent from `phase`. A task's satisfaction depends on
/// its run history, its `auto_run` policy and whether it declares params a
/// file change cannot supply — facts that live in its supervisor and nowhere
/// else. Publishing the conclusion rather than the inputs is what stops every
/// dependent needing to understand every kind of dependency.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProcessFacts {
    /// Lifecycle phase, for display and for anything asking "what is it doing".
    pub(crate) phase: Phase,
    /// Whether a *blocking* dependent may treat this as up and start.
    pub(crate) satisfied: bool,
    /// Whether this has stopped making progress — failed, stopped, or parked
    /// awaiting a human. Waiting on it will not end, so a non-blocking
    /// dependent may proceed and an explicitly requested one may too.
    pub(crate) settled: bool,
    /// Root failures this process is stranded behind, already resolved
    /// transitively: each process inherits its blocking dependencies' roots,
    /// so `api -> worker -> db` reports `db` rather than `worker`.
    pub(crate) failed_roots: Vec<String>,
    /// What the supervisor holds, if anything.
    pub(crate) detail: Detail,
    /// Whether this process's supervisor has an auto-restart armed.
    ///
    /// Not the same as `settled`: a service inside a backoff *will* move on
    /// its own, so a stack sitting in one has not finished starting — but a
    /// dependent must still not treat it as up. Only the supervisor that
    /// armed the timer can answer this.
    pub(crate) restart_pending: bool,
    /// A terminal phase is visible, but its completion report is not queued yet.
    /// The root must keep receiving until the publisher clears this flag.
    pub(crate) report_pending: bool,
}

impl ProcessFacts {
    /// The same facts with the root failures this process is stranded behind
    /// recorded, whatever phase it is in. For tests that need a dependency
    /// that already carries a cascade.
    #[cfg(test)]
    pub(crate) fn stranded_behind(mut self, roots: Vec<String>) -> Self {
        self.failed_roots = roots;
        self.settled = true;
        self
    }

    /// The same facts with an armed auto-restart recorded.
    pub(crate) fn with_restart_pending(mut self, restart_pending: bool) -> Self {
        self.restart_pending = restart_pending;
        self
    }

    /// What a service in `phase` presents to its dependents.
    ///
    /// `stranded_behind` is what [`crate::gate::failed_roots`] resolved for
    /// this process's own dependencies; it is read only in the phase that can
    /// carry it. A service that failed *itself* names itself instead, which is
    /// what terminates the chain.
    pub(crate) fn for_service(
        name: &str,
        phase: ServiceState,
        runtime: Option<ServiceRuntime>,
        stranded_behind: Vec<String>,
    ) -> Self {
        Self {
            phase: Phase::Service(phase),
            satisfied: phase.is_satisfied(),
            settled: matches!(
                phase,
                ServiceState::Failed | ServiceState::DependencyFailed | ServiceState::Stopped
            ),
            failed_roots: match phase {
                ServiceState::Failed => vec![name.to_string()],
                ServiceState::DependencyFailed => stranded_behind,
                _ => Vec::new(),
            },
            detail: Detail::Service { runtime },
            restart_pending: false,
            report_pending: false,
        }
    }

    /// What a task in `phase` presents to its dependents.
    ///
    /// `satisfied` is passed rather than derived: a completed task with an
    /// outstanding re-run does not satisfy its dependents, and that depends on
    /// the task's run history, its `auto_run` policy and whether it declares
    /// params — none of which are readable from the phase.
    pub(crate) fn for_task(
        name: &str,
        phase: TaskState,
        satisfied: bool,
        pid: Option<i32>,
        last_run: Option<TaskRunInfo>,
        stranded_behind: Vec<String>,
    ) -> Self {
        Self {
            phase: Phase::Task(phase),
            satisfied,
            // `PendingRun`/`Skipped` are settled too: the task is waiting for
            // a manual trigger, or was judged unnecessary, and will not run on
            // its own — so a non-blocking dependent would otherwise wait
            // forever.
            settled: matches!(
                phase,
                TaskState::Failed
                    | TaskState::DependencyFailed
                    | TaskState::PendingRun
                    | TaskState::Skipped
            ),
            failed_roots: match phase {
                TaskState::Failed => vec![name.to_string()],
                TaskState::DependencyFailed => stranded_behind,
                _ => Vec::new(),
            },
            detail: Detail::Task { pid, last_run },
            restart_pending: false,
            report_pending: false,
        }
    }

    /// Whether this process's supervisor is holding nothing.
    ///
    /// The teardown predicate, and deliberately uniform across kinds: a
    /// service with no custody and a task with no run are the same answer to
    /// the only question teardown asks — *is there still something of yours
    /// that has to die before mine can?*
    ///
    /// Phase would be the wrong thing to read. A task ends `Completed` or
    /// `Failed`, never `Stopped`, and a service can sit in `Failed` with its
    /// process still alive. Custody is the fact; the phase is commentary.
    pub(crate) fn holds_nothing(&self) -> bool {
        match &self.detail {
            Detail::Service { runtime } => runtime.is_none(),
            Detail::Task { pid, .. } => pid.is_none(),
            Detail::None => true,
        }
    }
}

/// Every process's facts, as of one instant.
///
/// Handed out behind an `Arc` rather than as a `watch::Ref` for the reason
/// [`crate::state_store`] documents: a `Ref` holds a read lock for as long as
/// it lives, and returning an `Arc` makes "held across an `.await`"
/// unrepresentable rather than merely documented against.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct FactsSnapshot {
    processes: HashMap<String, ProcessFacts>,
}

impl FactsSnapshot {
    /// One process's facts, or `None` for a name this stack does not run.
    pub(crate) fn get(&self, name: &str) -> Option<&ProcessFacts> {
        self.processes.get(name)
    }

    /// Whether a blocking dependent may treat `name` as up.
    ///
    /// An unknown name is not satisfied. Config validation rejects unknown
    /// dependencies before anything starts, so this is the profile-filtered
    /// case: a dependency outside the active process set.
    pub(crate) fn satisfied(&self, name: &str) -> bool {
        self.get(name).is_some_and(|facts| facts.satisfied)
    }

    /// Whether waiting on `name` will never end without an explicit request.
    pub(crate) fn settled(&self, name: &str) -> bool {
        self.get(name).is_some_and(|facts| facts.settled)
    }

    /// The root failures `name` is stranded behind, or `name` itself if it is
    /// the root. Empty when it has not failed.
    ///
    /// This is the transitive resolution, done one hop at a time: a process
    /// that is itself stranded has already inherited its own dependencies'
    /// roots, so reading one hop reads the whole chain.
    pub(crate) fn failed_roots(&self, name: &str) -> &[String] {
        self.get(name)
            .map(|facts| facts.failed_roots.as_slice())
            .unwrap_or_default()
    }

    /// Every process, for the whole-stack questions no single one can answer.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&String, &ProcessFacts)> {
        self.processes.iter()
    }

    /// Whether every one of `names` is holding nothing.
    ///
    /// What a supervisor waits on during teardown: its *dependents* must be
    /// gone before it may end what they were talking to. A name this stack
    /// does not run is vacuously done — a profile can leave a dependent out.
    pub(crate) fn all_hold_nothing<'a>(&self, names: impl Iterator<Item = &'a String>) -> bool {
        names
            .filter_map(|name| self.get(name))
            .all(|facts| facts.holds_nothing())
    }

    /// Build a snapshot directly, for tests that need a fixed picture to
    /// evaluate a pure function against.
    #[cfg(test)]
    pub(crate) fn from_pairs(pairs: impl IntoIterator<Item = (String, ProcessFacts)>) -> Self {
        Self {
            processes: pairs.into_iter().collect(),
        }
    }
}

/// One process's write end.
///
/// A supervisor holds exactly one and it carries its own name, so there is no
/// way to publish facts about a process you do not own — the same
/// enforcement-by-ownership as [`crate::state_store::StateWriter`], at
/// per-process granularity.
pub(crate) struct FactsPublisher {
    name: String,
    tx: mpsc::UnboundedSender<(String, ProcessFacts)>,
    /// What was last sent, so a republish that changes nothing sends nothing.
    /// See the module doc: without this the merge cycle never settles.
    last: ProcessFacts,
}

impl FactsPublisher {
    /// The root failures last published, so a caller can tell a *changed*
    /// cascade from a repeated one without keeping a second copy.
    pub(crate) fn current_roots(&self) -> &[String] {
        &self.last.failed_roots
    }

    /// Publish this process's facts, if they differ from the last ones sent.
    ///
    /// Returns whether anything was sent. A closed channel means the stack is
    /// being torn down, which is not an error.
    pub(crate) fn publish(&mut self, facts: ProcessFacts) -> bool {
        if facts == self.last {
            return false;
        }
        self.last = facts.clone();
        let _ = self.tx.send((self.name.clone(), facts));
        true
    }
}

/// The read end: the merged picture, and a wait for it to change.
#[derive(Clone, Debug)]
pub(crate) struct FactsReader {
    rx: watch::Receiver<Arc<FactsSnapshot>>,
}

impl FactsReader {
    /// The current snapshot. Correct after missing any number of changes,
    /// which is the point — a supervisor busy through a docker pull or a build
    /// sees the current answer when it finishes, not a notification it slept
    /// through.
    pub(crate) fn snapshot(&self) -> Arc<FactsSnapshot> {
        Arc::clone(&self.rx.borrow())
    }

    /// Wait for the merged picture to change. Cancel-safe, so it can be a
    /// `select!` arm.
    ///
    /// `None` once the merge is gone — treat that as "nothing will change
    /// again" and stop selecting on it. A `watch::Receiver` whose sender has
    /// dropped returns `Err` immediately and *forever*, so a caller that keeps
    /// polling spins at 100% CPU.
    pub(crate) async fn changed(&mut self) -> Option<()> {
        self.rx.changed().await.ok()
    }
}

/// The merge. Owns the receiving half and the published snapshot, and does
/// nothing else.
///
/// There is deliberately no branch here on the *content* of what is merged.
/// That is the discipline that keeps this from growing back into a scheduler,
/// and it is the same one [`crate::endpoints`] already lives under.
pub(crate) struct Aggregator {
    rx: mpsc::UnboundedReceiver<(String, ProcessFacts)>,
    tx: watch::Sender<Arc<FactsSnapshot>>,
    snapshot: FactsSnapshot,
}

impl Aggregator {
    /// Await one process's update. `None` once every publisher is gone.
    ///
    /// Cancel-safe, so this can be a `select!` arm alongside whatever else the
    /// holder is driving.
    pub(crate) async fn recv(&mut self) -> Option<(String, ProcessFacts)> {
        self.rx.recv().await
    }

    /// Take one update without waiting. `Err` when nothing is queued.
    ///
    /// For a holder that is not running its own loop — teardown, which must
    /// still let what processes say about themselves reach the projections.
    pub(crate) fn try_recv(&mut self) -> Result<(String, ProcessFacts), mpsc::error::TryRecvError> {
        self.rx.try_recv()
    }

    /// Merge one update and republish if the picture changed.
    ///
    /// Returns whether anything was published, so a caller can skip the work
    /// that only a real change justifies.
    pub(crate) fn apply(&mut self, name: String, facts: ProcessFacts) -> bool {
        if self.snapshot.processes.get(&name) == Some(&facts) {
            return false;
        }
        self.snapshot.processes.insert(name, facts);
        self.tx.send_replace(Arc::new(self.snapshot.clone()));
        true
    }

    /// The merged picture, for a holder that needs it without a channel hop.
    pub(crate) fn snapshot(&self) -> &FactsSnapshot {
        &self.snapshot
    }
}

/// Create the merge, one publisher per process, and the shared read end.
///
/// The name set is fixed here, for the same reason
/// [`crate::process::registry::ProcessRegistry`]'s is: the process set is
/// decided at construction, so there is nothing to synchronise.
///
/// Each publisher is seeded with the facts its process starts in, and the
/// snapshot with the same values, so a dependent that reads before anything
/// has published sees the truth rather than an absence.
pub(crate) fn channel(
    seeds: impl Iterator<Item = (String, ProcessFacts)>,
) -> (Aggregator, HashMap<String, FactsPublisher>, FactsReader) {
    let (tx, rx) = mpsc::unbounded_channel();
    let mut processes = HashMap::new();
    let mut publishers = HashMap::new();
    for (name, facts) in seeds {
        publishers.insert(
            name.clone(),
            FactsPublisher {
                name: name.clone(),
                tx: tx.clone(),
                last: facts.clone(),
            },
        );
        processes.insert(name, facts);
    }
    // Dropped here: every remaining sender lives in a publisher, so the
    // channel closes exactly when the last one is gone.
    drop(tx);
    let snapshot = FactsSnapshot { processes };
    let (watch_tx, watch_rx) = watch::channel(Arc::new(snapshot.clone()));
    (
        Aggregator {
            rx,
            tx: watch_tx,
            snapshot,
        },
        publishers,
        FactsReader { rx: watch_rx },
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn stranded(phase: ServiceState, roots: &[&str]) -> ProcessFacts {
        ProcessFacts {
            settled: true,
            failed_roots: roots.iter().map(|r| (*r).to_string()).collect(),
            ..ProcessFacts::for_service("dep", phase, None, Vec::new())
        }
    }

    #[test]
    fn an_unknown_name_is_neither_satisfied_nor_settled() {
        // Config validation rejects unknown dependencies, so this is the
        // profile-filtered case: a dependency outside the active set.
        let snapshot = FactsSnapshot::from_pairs([(
            "db".to_string(),
            ProcessFacts {
                satisfied: true,
                ..ProcessFacts::for_service("db", ServiceState::Ready, None, Vec::new())
            },
        )]);
        assert!(snapshot.satisfied("db"));
        assert!(!snapshot.satisfied("nope"));
        assert!(!snapshot.settled("nope"));
        assert!(snapshot.failed_roots("nope").is_empty());
    }

    /// Root causes resolve one hop at a time: each process inherits its
    /// blocking dependencies' roots, so reading one hop reads the whole chain
    /// and nothing has to walk the graph.
    #[test]
    fn failed_roots_read_one_hop() {
        let snapshot = FactsSnapshot::from_pairs([
            ("db".to_string(), stranded(ServiceState::Failed, &["db"])),
            (
                "worker".to_string(),
                stranded(ServiceState::DependencyFailed, &["db"]),
            ),
            (
                "api".to_string(),
                stranded(ServiceState::DependencyFailed, &["db"]),
            ),
        ]);
        assert_eq!(snapshot.failed_roots("db"), ["db".to_string()]);
        assert_eq!(snapshot.failed_roots("worker"), ["db".to_string()]);
        assert_eq!(snapshot.failed_roots("api"), ["db".to_string()]);
        assert!(snapshot.settled("worker"));
    }

    /// The phase→facts mapping, which used to be spread across
    /// `ServiceState::is_satisfied`, the scheduler's `is_dep_settled` and its
    /// `failed_dependency_roots`. One table, one place.
    #[test]
    fn a_service_phase_decides_what_it_presents() {
        struct Case {
            phase: ServiceState,
            want_satisfied: bool,
            want_settled: bool,
            want_roots: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                phase: ServiceState::Ready,
                want_satisfied: true,
                want_settled: false,
                want_roots: vec![],
            },
            Case {
                // Alive and possibly serving; dependents are left alone.
                phase: ServiceState::Unhealthy,
                want_satisfied: true,
                want_settled: false,
                want_roots: vec![],
            },
            Case {
                // Bound and listening: a dependent may start against it,
                // because connecting is what brings it up.
                phase: ServiceState::Lazy,
                want_satisfied: true,
                want_settled: false,
                want_roots: vec![],
            },
            Case {
                phase: ServiceState::Starting,
                want_satisfied: false,
                want_settled: false,
                want_roots: vec![],
            },
            Case {
                // A root failure names itself — that is what terminates the
                // chain a stranded dependent walks.
                phase: ServiceState::Failed,
                want_satisfied: false,
                want_settled: true,
                want_roots: vec!["api"],
            },
            Case {
                phase: ServiceState::DependencyFailed,
                want_satisfied: false,
                want_settled: true,
                want_roots: vec!["db"],
            },
            Case {
                phase: ServiceState::Stopped,
                want_satisfied: false,
                want_settled: true,
                want_roots: vec![],
            },
        ];

        for case in cases {
            let facts = ProcessFacts::for_service("api", case.phase, None, vec!["db".to_string()]);
            assert_eq!(facts.satisfied, case.want_satisfied, "{:?}", case.phase);
            assert_eq!(facts.settled, case.want_settled, "{:?}", case.phase);
            assert_eq!(facts.failed_roots, case.want_roots, "{:?}", case.phase);
        }
    }

    #[test]
    fn a_task_and_a_service_carry_their_own_vocabulary() {
        let snapshot = FactsSnapshot::from_pairs([
            (
                "api".to_string(),
                ProcessFacts::for_service("db", ServiceState::Ready, None, Vec::new()),
            ),
            (
                "migrate".to_string(),
                ProcessFacts::for_task(
                    "migrate",
                    TaskState::Completed,
                    true,
                    None,
                    None,
                    Vec::new(),
                ),
            ),
        ]);
        assert_eq!(
            snapshot.get("api").map(|f| f.phase),
            Some(Phase::Service(ServiceState::Ready))
        );
        assert_eq!(
            snapshot.get("migrate").map(|f| f.phase),
            Some(Phase::Task(TaskState::Completed))
        );
    }
}
