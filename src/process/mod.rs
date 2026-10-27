//! Everything a service or task does for itself: the supervisors that own
//! processes from prepare to reap, the spawn/stop machinery they drive, the
//! health monitor, ready resolution, and the vocabulary they speak.
//!
//! The edge is deliberate and greppable: **this module imports nothing from
//! `crate::runner`.** Processes produce reports; the runner folds them and
//! decides what starts next. Commands flow down, reports flow up, and the
//! scheduler never reaches into an process's internals.

pub(crate) mod env_refs;
pub(crate) mod health;
pub(crate) mod params;
pub(crate) mod paths;
pub(crate) mod ready;
pub(crate) mod registry;
pub(crate) mod service;
pub(crate) mod service_supervisor;
pub(crate) mod service_worker;
pub mod state;
pub(crate) mod task;
pub(crate) mod task_supervisor;
pub(crate) mod task_worker;
pub(crate) mod watch_dispatch;

pub use state::{ServiceState, TaskState};

pub(crate) use state::{Demand, ProcessKind, ServiceHandleIdentity};
use tokio::sync::oneshot;

/// Wait until every dependent is holding nothing, so this process can end
/// without pulling the rug from under something still talking to it.
///
/// This is teardown's reverse-dependency order, expressed as each process
/// waiting rather than one actor sequencing everyone. The graph is a validated
/// DAG, so something always has no dependents and goes first; the rest follow.
///
/// Two escapes, both necessary. A second Ctrl+C means the user has stopped
/// caring about graceful order — `force` fires and everyone stops at once. And
/// a dependent whose supervisor has already ended publishes nothing further,
/// which is why the predicate is *holds nothing* rather than a phase: it stays
/// true of a process that is simply gone.
pub(crate) async fn await_dependents_gone(
    name: &str,
    emitter: &crate::output::LifecycleEmitter,
    world: &mut crate::facts::FactsReader,
    dependents: &[String],
    force: &mut tokio::sync::watch::Receiver<bool>,
) {
    if dependents.is_empty() {
        return;
    }
    let mut announced = false;
    loop {
        let snapshot = world.snapshot();
        if snapshot.all_hold_nothing(dependents.iter()) {
            return;
        }
        if *force.borrow() {
            emitter.service_debug_event(name, "forced: stopping without waiting for dependents");
            return;
        }
        if !announced {
            announced = true;
            let waiting: Vec<&str> = dependents
                .iter()
                .filter(|dep| !snapshot.get(dep).is_none_or(|f| f.holds_nothing()))
                .map(String::as_str)
                .collect();
            emitter.service_debug_event(
                name,
                &format!("waiting for dependents to stop: {}", waiting.join(", ")),
            );
        }
        tokio::select! {
            changed = world.changed() => {
                if changed.is_none() {
                    // Nothing will publish again; waiting cannot end.
                    return;
                }
            }
            _ = force.changed() => {}
        }
    }
}

pub(crate) enum ServiceStartIntent {
    /// The dependency sweep asked for this start; the ready outcome drives
    /// the sweep-visible transition.
    Scheduled,
    Reply {
        reply: oneshot::Sender<crate::command::CommandResult>,
    },
    Background,
}

pub(crate) enum TaskRunIntent {
    /// The dependency sweep asked for this run; its exit report drives the
    /// sweep-visible transition.
    Scheduled,
    /// A file change or a dependency's re-run asked for this run. Like a
    /// scheduled run, it waits for its dependencies.
    Rerun,
    /// Someone asked for this run by name. Settled dependencies do not hold
    /// it back; one still in progress makes it wait for them to succeed.
    Background,
}

pub(crate) struct TaskExit {
    pub(crate) name: String,
    pub(crate) success: bool,
    pub(crate) message: Option<String>,
    pub(crate) elapsed: Option<std::time::Duration>,
    pub(crate) last_run: Option<crate::task_state::TaskRunInfo>,
    /// A `don run --wait` caller, answered by the fold so the reply means
    /// "the scheduler has applied this" like every other command reply.
    pub(crate) reply: Option<oneshot::Sender<crate::command::CommandResult>>,
}

/// Runner-private messages emitted by detached workers.
/// What an process tells the scheduler, on the lossless report channel.
///
/// This is the up-direction of the supervisor architecture: processes report,
/// the runner folds. It is `mpsc`, not `broadcast`, because the scheduler
/// must never miss one — lossy observation is for peers and edges, which
/// resync from the snapshot. Starts with lazy demand; per-process lifecycle
/// reports (exit, transitions) migrate here as supervisors absorb them.
pub(crate) enum ProcessReport {
    /// A lazy service's proxy saw its first connection. Demand originates
    /// inside the process, but the *reaction* belongs to the scheduler: a lazy
    /// service has dependencies, and starting it is a scheduling decision
    /// like any other.
    Demand { name: String, demand: Demand },
    /// A service's process died, its supervisor reaped it, and the phase that
    /// followed is already published.
    ///
    /// Carries nothing but the name: the exit status, the policy verdict and
    /// the resulting phase were all decided and published by the supervisor.
    /// This exists only to end the projections that span process generations —
    /// endpoints, the ports manifest, the follow sinks.
    ServiceExited { name: String },
    /// A task process exited after an explicit run/restart.
    TaskExited(TaskExit),
    /// A service's supervisor settled a start request — wired (metadata
    /// only; custody stays with the supervisor) or failed to prepare.
    ServiceStartPrepared {
        name: String,
        intent: ServiceStartIntent,
        result: Result<Box<service_supervisor::ServiceWired>, String>,
    },
    /// A service's supervisor finished executing a stop.
    ///
    /// No operation id: a supervisor runs one stop at a time and reports it
    /// before taking the next command, so the Nth completion is the Nth stop
    /// by construction.
    ServiceStopComplete {
        name: String,
        result: Result<(), String>,
        /// The requester's reply, carried down with the stop and back up
        /// here. Answered by the fold rather than the supervisor, because
        /// callers read a stop reply as "the scheduler has applied this" —
        /// `don stop` returning means the service is no longer a satisfied
        /// dependency.
        reply: Option<tokio::sync::oneshot::Sender<crate::command::CommandResult>>,
    },
    /// A task's supervisor picked up a triggered run and is preparing it.
    ///
    /// The task-side twin of [`ServiceStarting`](Self::ServiceStarting):
    /// preparation hashes inputs and resolves downloads, so a run that said
    /// nothing until it spawned would look ignored. The scheduler folds it
    /// into `Running` and says what the supervisor asked it to say.
    TaskStarting { name: String, message: String },
    /// A task's supervisor settled a run request.
    TaskRunPrepared {
        name: String,
        task_cfg: Box<crate::config::Task>,
        intent: TaskRunIntent,
        result: Result<task_supervisor::TaskRunReport, String>,
    },
}
