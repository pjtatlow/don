//! Per-task run supervision: the whole pipeline — prepare (resolve params,
//! hash inputs, decide whether to run at all) → spawn → wire → wait for
//! exit → record the outcome — owned by one task per task.
//!
//! Being the single producer of a task's messages, on the one lossless
//! report channel, is what deleted the old generation counters: a
//! completion can only arrive after its own prepared report and before
//! anything a later run produces. What remains in `task_commands` is the
//! part only the runner may do: transition process state, which drives the
//! cross-process dependency scheduler.

use super::TaskExit;
use super::paths::{resolve_watch_ignore_patterns, working_dir_for};
use crate::task_state::{TaskRunInfo, TaskStateStore};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;

/// One request to run a task, as handed to its supervisor.
///
/// It carries no copy of the task's config: the supervisor was built from it
/// and holds it for as long as it lives, so a request that brought its own
/// would be offering a second opinion about a fact the recipient already owns.
pub(crate) struct RunRequest {
    /// User-supplied parameter values, *unresolved*. Defaults, unknown keys
    /// and missing required values are settled by the supervisor, against the
    /// config it holds.
    pub(crate) params: std::collections::HashMap<String, String>,
    pub(crate) mode: super::task_worker::TaskRunMode,
    pub(crate) intent: super::TaskRunIntent,
    /// Someone waiting to hear how this went.
    pub(crate) reply: Option<RunReply>,
    /// What to say when this run is picked up, before preparing it.
    ///
    /// Preparation hashes files and resolves downloads, so a triggered run
    /// that said nothing until it was ready to spawn would look ignored.
    /// `None` for the startup sweep, which the scheduler is already
    /// narrating.
    pub(crate) start_message: Option<String>,
}

/// A caller waiting to hear how a run request went, and when they expect the
/// answer.
pub(crate) enum RunReply {
    /// Answered the moment the run is admitted — `don run`. The run's own
    /// outcome travels separately, as lifecycle output.
    OnStart(tokio::sync::oneshot::Sender<crate::command::CommandResult>),
    /// Answered when the run finishes — `don run --wait`.
    OnExit(RunWait),
}

impl RunReply {
    /// Answer now, whichever timing was asked for. A request that is refused,
    /// or that settles without spawning, collapses the two cases: there is no
    /// exit left to wait for.
    fn settle(self, result: crate::command::CommandResult) {
        let _ = match self {
            RunReply::OnStart(reply) => reply,
            RunReply::OnExit(wait) => wait.reply,
        }
        .send(result);
    }
}

/// Answer a run request now, if anyone was listening.
fn settle_run(reply: Option<RunReply>, result: crate::command::CommandResult) {
    if let Some(reply) = reply {
        reply.settle(result);
    }
}

/// A caller blocked on a run's outcome.
///
/// The supervisor holds this for the run it belongs to, which is what makes
/// the old `waiter_token` unnecessary: it ran one run at a time and the
/// timeout was a detached task reporting through a shared channel, so the
/// runner needed an identity to match answers to askers. One actor holding
/// both the timer and the run needs no such thing.
pub(crate) struct RunWait {
    pub(crate) reply: tokio::sync::oneshot::Sender<crate::command::CommandResult>,
    /// Parsed `--wait` deadline; `None` waits indefinitely.
    pub(crate) timeout: Option<(std::time::Duration, String)>,
}

/// What a task's supervisor can be asked to do.
///
/// A run used to be the only thing in this mailbox, because killing a run and
/// restarting one were done *to* the task by the scheduler: it kept the run's
/// pgid and the parameters the last run used, and signalled the process group
/// itself. Both of those are things the owner of the run already has, so both
/// arrive here now and the scheduler keeps neither.
pub(crate) enum TaskCommand {
    /// Run this task.
    ///
    /// Every pre-check `don run` used to get from the scheduler is answered
    /// here instead: whether a run is already in flight, and whether the
    /// parameters supplied resolve against the ones this task declares. Both
    /// are about things this supervisor holds.
    Run(RunRequest),
    /// This task's watched files changed.
    ///
    /// Whether that means a run is decided here, from three facts this
    /// supervisor already has: the task's `auto_run` policy, whether it
    /// declares params a watch event cannot supply, and whether its artifact
    /// is still being built. The scheduler used to answer all three, which
    /// meant the file watcher had to wait to hear how it went.
    Rerun,
    /// This task's build-graph definition files changed, so the watch
    /// patterns resolved from them may no longer be right. The supervisor
    /// asks the build manager to re-query and re-runs if they moved.
    BuildGraphChanged,
    /// End the run in flight, if any, then run again with the parameters the
    /// last run used.
    ///
    /// The "no previous invocation to restart" check comes with it: the
    /// parameters it reads about are held here.
    Restart {
        reply: Option<tokio::sync::oneshot::Sender<crate::command::CommandResult>>,
    },
    /// End the run in flight, if any, and do not run again — teardown.
    ///
    /// `done` fires once the run is gone. Teardown waits on it: the
    /// supervisors are aborted immediately afterwards, and aborting one that
    /// has not read this yet would drop a live process on the floor.
    Kill {
        done: Option<tokio::sync::oneshot::Sender<()>>,
    },
}

/// Owner half for tasks. See [`Supervisors`].
///
/// [`Supervisors`]: super::registry::Supervisors
pub(crate) type TaskSupervisors = super::registry::Supervisors<TaskCommand>;

/// What the runner receives for a spawned, wired run. The supervisor keeps
/// the process handle and the output reader; this is what the runner's
/// bookkeeping (shadows for attach/status, spawn lines) needs.
pub(crate) struct TaskWired {
    pub(crate) pgid: i32,
    pub(crate) rendered_cmdline: String,
}

/// What a run request settled into, as reported to the runner. The spawned
/// case carries wired metadata, never the process — custody stays here.
pub(crate) enum TaskRunReport {
    PendingRun { message: String },
    Skipped { message: Option<String> },
    Running(TaskWired),
}

/// Start one run supervisor per task.
///
/// Every task gets one up front so the registry is immutable — see
/// [`Supervisors::spawn_all`].
///
/// [`Supervisors::spawn_all`]: super::registry::Supervisors::spawn_all
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_supervisors<'a>(
    names: impl Iterator<Item = &'a String>,
    ctx: &super::task_worker::TaskWorkerContext,
    outputs: &dyn Fn(&str) -> Option<crate::output::ProcessOutput>,
    config: &dyn Fn(&str) -> Option<StartupConfig>,
    dependents: &dyn Fn(&str) -> Vec<String>,
    report_tx: &mpsc::UnboundedSender<super::ProcessReport>,
    facts: &crate::facts::FactsReader,
    publishers: &mut std::collections::HashMap<String, crate::facts::FactsPublisher>,
    released: &tokio::sync::watch::Receiver<bool>,
    shutdown_rx: &tokio::sync::watch::Receiver<bool>,
    batcher_tx: &mpsc::UnboundedSender<crate::build_tool::batcher::BatchRequest>,
) -> TaskSupervisors {
    TaskSupervisors::spawn_all(names, |name, rx, busy| {
        let output = outputs(&name);
        let startup = config(&name);
        let publisher = publishers.remove(&name);
        let dependents = dependents(&name);
        supervise(
            name,
            rx,
            ctx.clone(),
            output,
            report_tx.clone(),
            busy,
            startup,
            dependents,
            facts.clone(),
            publisher,
            released.clone(),
            shutdown_rx.clone(),
            batcher_tx.clone(),
        )
    })
}

/// Ask the build manager to re-resolve this task's watch paths.
fn request_requery(
    name: &str,
    task_cfg: &crate::config::Task,
    ctx: &super::task_worker::TaskWorkerContext,
    batcher_tx: &mpsc::UnboundedSender<crate::build_tool::batcher::BatchRequest>,
    outcome: &mpsc::UnboundedSender<crate::build_tool::batch::RequeryOutcome>,
) {
    let working_dir = working_dir_for(&ctx.base_dir, task_cfg.dir.as_deref());
    let ignore_patterns = resolve_watch_ignore_patterns(
        &working_dir,
        &task_cfg.ignore,
        &ctx.base_dir,
        &ctx.global_watch_ignore,
    );
    let _ = batcher_tx.send(crate::build_tool::batcher::BatchRequest::QueueRequery {
        item: crate::build_tool::batch::GraphRequeryRequestItem {
            name: name.to_string(),
            kind: super::ProcessKind::Task,
            bazel: task_cfg.bazel.clone(),
            watch_enabled: task_cfg.build_tool_watch_enabled(),
            working_dir,
            ignore_patterns,
            global_watch_ignore: ctx.global_watch_ignore.clone(),
        },
        outcome: outcome.clone(),
    });
}

/// Ask the build manager for this task's artifact, and tell the scheduler a
/// build is under way. Returns whether a request is now outstanding.
///
/// The service side's rule applies unchanged: asked for at construction, not
/// at gate-open, so the whole workspace coalesces into one invocation. See
/// [`super::service_supervisor`].
fn request_artifact(
    name: &str,
    task_cfg: &crate::config::Task,
    ctx: &super::task_worker::TaskWorkerContext,
    outcome: &mpsc::UnboundedSender<crate::build_tool::batch::PrepareOutcome>,
    batcher_tx: &mpsc::UnboundedSender<crate::build_tool::batcher::BatchRequest>,
) -> bool {
    let working_dir = working_dir_for(&ctx.base_dir, task_cfg.dir.as_deref());
    let ignore = resolve_watch_ignore_patterns(
        &working_dir,
        &task_cfg.ignore,
        &ctx.base_dir,
        &ctx.global_watch_ignore,
    );
    batcher_tx
        .send(crate::build_tool::batcher::BatchRequest::QueuePrepare {
            item: Box::new(crate::build_tool::batch::BatchBuildItem {
                name: name.to_string(),
                kind: super::ProcessKind::Task,
                bazel: task_cfg
                    .bazel
                    .clone()
                    .map(|b| b.with_workspace_default(ctx.bazel_config.as_deref())),
                watch_enabled: task_cfg.build_tool_watch_enabled(),
                working_dir,
                ignore,
            }),
            outcome: outcome.clone(),
        })
        .is_ok()
}

/// What a task needs to issue its own startup run when permitted.
pub(crate) struct StartupConfig {
    pub(crate) task_cfg: Box<crate::config::Task>,
    /// Whether any *blocking* dependent is waiting on this task. A
    /// non-blocking dependent is happy either way, so counting it would park
    /// a manual task as "required by dependents" and then block the very
    /// dependent that did not care. Fixed at construction, like the name set.
    pub(crate) has_dependents: bool,
    /// Whether this task has ever completed successfully, read from
    /// `.don/task-state` at construction. A task that succeeded in a previous
    /// session satisfies its dependents without re-running.
    pub(crate) has_success: bool,
    /// Metadata for the most recent run, likewise carried across sessions.
    pub(crate) last_run: Option<TaskRunInfo>,
}

/// What a command means once resolved against what this supervisor holds —
/// the task's config and the parameters its last run used.
///
/// Resolving up front is what stops the three places a command can arrive
/// (idle, mid-preparation, mid-run) from each re-deriving "what does a
/// restart mean here".
enum Ask {
    /// Start this run.
    Run(RunRequest),
    /// Ask the build manager to re-resolve this task's watch paths.
    Requery,
    /// Do not run; park the task for a manual trigger and say why.
    ///
    /// Reported as an ordinary prepared-run outcome, so the scheduler folds
    /// it into `PendingRun` exactly as it does one this supervisor reaches by
    /// preparing a run and finding nothing to do.
    Park(String),
    /// End the run in hand; start `then` once it is gone, and fire `done`
    /// when it is.
    Cancel {
        then: Option<RunRequest>,
        done: Option<tokio::sync::oneshot::Sender<()>>,
    },
    /// Nothing to do. Any reply has already been answered.
    Nothing,
}

/// Answer a command's reply channel, if it had one.
fn answer(
    reply: Option<tokio::sync::oneshot::Sender<crate::command::CommandResult>>,
    result: crate::command::CommandResult,
) {
    if let Some(reply) = reply {
        let _ = reply.send(result);
    }
}

/// `phase` and `spawned` are what admission reads: the phase this supervisor
/// has published, and whether a run of this task is actually *executing*.
/// Together they are exactly the pair the scheduler used to consult — a state
/// map plus a busy flag — except that here both belong to the thing being
/// asked.
///
/// `spawned` is deliberately narrower than "busy". A supervisor working out
/// whether its startup run is needed at all — hashing watch inputs, checking
/// `auto_run` — is busy, but nothing is running, and refusing a manual run
/// there would be a lie. Such a run is queued behind the evaluation instead,
/// which takes milliseconds.
/// How long after a run finishes a watch trigger is still attributed to that
/// run's own writes.
///
/// Long enough for a `rm -rf` plus a copy of a few thousand files to drain
/// through notify and past the item's debounce window; short enough that a
/// save made while reading the last run's output is still the user's. Being
/// wrong either way costs one hash, not a run.
const SELF_WRITE_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

/// Whether a watch trigger arriving now could be the last run's own output.
fn is_self_write_suspect(last_run_ended: Option<std::time::Instant>) -> bool {
    last_run_ended.is_some_and(|at| at.elapsed() < SELF_WRITE_GRACE)
}

/// What this supervisor currently is, at the moment a command arrives.
///
/// Grouped rather than passed loose because every field answers the same
/// question — "what is this task in the middle of?" — and the call sites read
/// as a description of the supervisor's own state instead of a bool run.
#[derive(Clone, Copy)]
struct RunContext {
    /// A build this run's artifact depends on is still in flight.
    awaiting_artifact: bool,
    /// The phase this supervisor has published for itself.
    phase: super::TaskState,
    /// Whether a run of it is actually executing.
    spawned: bool,
    /// Whether a watch trigger arriving now could be the last run's own
    /// output landing back under its watch patterns.
    self_write_suspect: bool,
}

fn resolve_command(
    command: TaskCommand,
    name: &str,
    startup: Option<&StartupConfig>,
    last_params: &std::collections::HashMap<String, String>,
    ctx: RunContext,
) -> Ask {
    let RunContext {
        awaiting_artifact,
        phase,
        spawned,
        self_write_suspect,
    } = ctx;
    match command {
        TaskCommand::Run(request) => {
            let Some(startup) = startup else {
                settle_run(
                    request.reply,
                    Err(crate::command::CommandError::UnknownTask {
                        name: name.to_string(),
                    }),
                );
                return Ask::Nothing;
            };
            let RunRequest {
                params,
                mode,
                intent,
                reply,
                start_message,
            } = request;

            if spawned
                || matches!(
                    phase,
                    super::TaskState::Running | super::TaskState::Building
                )
            {
                // Two runs of one task at once would interleave their output
                // unpredictably. A watch trigger supersedes because it is the
                // same run again; an explicit one is refused, because the
                // caller asked for a run that would not be theirs.
                settle_run(
                    reply,
                    Err(crate::command::CommandError::InvalidState {
                        name: name.to_string(),
                        message: "task is already running".to_string(),
                    }),
                );
                return Ask::Nothing;
            }

            // Apply defaults, reject unknown keys, reject missing required
            // values, validate per kind — against this task's own declaration.
            match crate::process::params::resolve_task_params(name, &startup.task_cfg, params) {
                Ok(params) => Ask::Run(RunRequest {
                    params,
                    mode,
                    intent,
                    reply,
                    start_message,
                }),
                Err(message) => {
                    settle_run(
                        reply,
                        Err(crate::command::CommandError::InvalidParams {
                            name: name.to_string(),
                            message,
                        }),
                    );
                    Ask::Nothing
                }
            }
        }
        TaskCommand::Kill { done } => Ask::Cancel { then: None, done },
        TaskCommand::BuildGraphChanged => Ask::Requery,
        TaskCommand::Rerun => {
            let Some(startup) = startup else {
                return Ask::Nothing;
            };
            // The artifact this run would use is still being built. Its
            // completion starts the run; a second one here would race it.
            if awaiting_artifact {
                return Ask::Nothing;
            }
            // Only `auto_run = true` / `"always"` re-runs from a watch event.
            // `"once"` is startup-only and `false` / `"never"` is manual
            // forever — both park instead.
            if !startup.task_cfg.auto_run.runs_automatically_on_watch() {
                return Ask::Park(
                    match startup.task_cfg.auto_run {
                        crate::config::TaskAutoRun::Always => "files changed (pending)",
                        crate::config::TaskAutoRun::Never => {
                            "files changed (pending — auto_run = false)"
                        }
                        crate::config::TaskAutoRun::Once => {
                            "files changed (pending — auto_run = once)"
                        }
                    }
                    .to_string(),
                );
            }
            // A task with params needs values a file change cannot supply.
            if !startup.task_cfg.params.is_empty() {
                return Ask::Park(
                    "files changed (pending — task has params, run manually)".to_string(),
                );
            }
            // Normally no hash check: the watcher already confirmed a matching
            // file changed, and that check exists for startup, to skip a task
            // whose inputs have not moved since its last run. The exception is
            // a change that landed while this task was running or moments
            // after — that one may be the task's own output, and taking the
            // watcher's word for it is what makes a generator rewriting its
            // inputs re-trigger itself forever.
            Ask::Run(RunRequest {
                params: std::collections::HashMap::new(),
                mode: if self_write_suspect {
                    super::task_worker::TaskRunMode::Verify
                } else {
                    super::task_worker::TaskRunMode::Triggered
                },
                intent: super::TaskRunIntent::Background,
                reply: None,
                start_message: Some("re-running (file changed)".to_string()),
            })
        }
        TaskCommand::Restart { reply } => {
            let Some(startup) = startup else {
                answer(
                    reply,
                    Err(crate::command::CommandError::UnknownTask {
                        name: name.to_string(),
                    }),
                );
                return Ask::Nothing;
            };
            // A param'd task has nothing to reuse until it has been run once
            // with values supplied.
            if !startup.task_cfg.params.is_empty()
                && last_params.len() < startup.task_cfg.params.len()
            {
                answer(
                    reply,
                    Err(crate::command::CommandError::InvalidState {
                        name: name.to_string(),
                        message:
                            "task has params and no previous invocation to restart; use `don run`"
                                .to_string(),
                    }),
                );
                return Ask::Nothing;
            }
            // Accepted: answered now, as it was when the scheduler executed
            // the restart itself. The run's own outcome travels separately.
            answer(reply, Ok(()));
            Ask::Cancel {
                done: None,
                then: Some(RunRequest {
                    // Already resolved: these are the values the last run
                    // actually used.
                    params: last_params.clone(),
                    mode: super::task_worker::TaskRunMode::Triggered,
                    intent: super::TaskRunIntent::Background,
                    reply: None,
                    start_message: Some("restarting (manual trigger)".to_string()),
                }),
            }
        }
    }
}

/// SIGKILL a run this supervisor is holding.
///
/// The supervisor owns the process, so it signals the group directly rather
/// than asking anyone: the pgid is its own, and the `wait` it is already
/// parked on is what reaps the result.
fn kill_run(emitter: &crate::output::LifecycleEmitter, name: &str, pgid: i32) {
    emitter.service_event(name, &format!("send SIGKILL to task pgid {pgid}"));
    if let Err(e) = nix::sys::signal::killpg(
        nix::unistd::Pid::from_raw(pgid),
        nix::sys::signal::Signal::SIGKILL,
    ) && e != nix::Error::ESRCH
    {
        emitter.service_error_event(name, &format!("failed to kill task pgid {pgid}: {e}"));
    }
}

/// Drive one task's runs, strictly in order.
///
/// The shape that matters is that a superseded run is **finished, not
/// aborted**. `run_task_worker` may already have spawned a process by the
/// time a newer request arrives; dropping that future would take the handle
/// with it and leave a child nothing will ever reap. So the worker always
/// runs to completion and the result is then killed off explicitly.
#[allow(clippy::too_many_arguments)]
async fn supervise(
    name: String,
    mut rx: mpsc::UnboundedReceiver<TaskCommand>,
    ctx: super::task_worker::TaskWorkerContext,
    output: Option<crate::output::ProcessOutput>,
    report_tx: mpsc::UnboundedSender<super::ProcessReport>,
    busy: Arc<AtomicBool>,
    startup: Option<StartupConfig>,
    dependents: Vec<String>,
    world: crate::facts::FactsReader,
    facts: Option<crate::facts::FactsPublisher>,
    mut released_rx: tokio::sync::watch::Receiver<bool>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    batcher_tx: mpsc::UnboundedSender<crate::build_tool::batcher::BatchRequest>,
) {
    // Names come from the same map the configs do, so `None` is unreachable;
    // ending the supervisor beats panicking.
    let (Some(facts), Some(startup_cfg)) = (facts, startup.as_ref()) else {
        return;
    };
    // The config, held once for as long as this supervisor lives. Every run
    // uses it, so no request needs to bring a copy.
    let task_cfg = startup_cfg.task_cfg.clone();
    // This task's phase is this supervisor's to answer for. It starts
    // `Pending` with whatever history `.don/task-state` carried across from a
    // previous session.
    let mut owner = TaskPhaseOwner {
        name: name.clone(),
        deps: startup_cfg.task_cfg.depends_on.clone(),
        config: (*startup_cfg.task_cfg).clone(),
        world: world.clone(),
        facts,
        phase: crate::process::TaskState::Pending,
        has_success: startup_cfg.has_success,
        needs_run_now: false,
        evaluated: false,
        pid: None,
        last_run: startup_cfg.last_run.clone(),
    };
    owner.publish();
    let service_writer = output.as_ref().map(|output| output.writer());
    let mut pending: Option<RunRequest> = None;
    let mut mailbox_closed = false;
    // When the last run finished, for `is_self_write_suspect`. Only a run that
    // actually spawned sets it — nothing else of this task's wrote anything.
    let mut last_run_ended: Option<std::time::Instant> = None;
    // The parameters the last run used, for a restart to reuse. Held here
    // because a restart is executed here; the scheduler kept a copy only to
    // hand it back on the way in.
    let mut last_params: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    // Where the build manager delivers this task's artifact. A task with a
    // bazel target needs it built before it runs, exactly like a service.
    let (prepare_tx, mut prepare_rx) =
        mpsc::unbounded_channel::<crate::build_tool::batch::PrepareOutcome>();
    // Where the build manager delivers this task's share of a build-graph
    // re-query.
    let (requery_tx, mut requery_rx) =
        mpsc::unbounded_channel::<crate::build_tool::batch::RequeryOutcome>();
    // Asked for after the owner exists, so the `Building` phase this enters is
    // published by the thing that entered it.
    let mut awaiting_artifact = match startup.as_ref() {
        Some(startup) if startup.task_cfg.bazel.is_some() => {
            owner.set(crate::process::TaskState::Building);
            request_artifact(&name, &startup.task_cfg, &ctx, &prepare_tx, &batcher_tx)
        }
        _ => false,
    };
    // A task is wanted from the moment it exists; its startup evaluation
    // decides whether it actually needs to run.
    let mut demand = super::Demand::Scheduled;
    // What every peer says about itself, and whether setup has released the
    // stack. Both are read at the level check below rather than delivered, so
    // demand and permission are never out of step — see `current_level`.
    let mut world = world;
    let mut watching_world = true;
    let mut stack_released = *released_rx.borrow();
    // Whoever is blocked on the run in hand, if anyone.
    let mut waiter: Option<tokio::sync::oneshot::Sender<crate::command::CommandResult>> = None;

    loop {
        let request = match pending.take() {
            Some(request) => request,
            None => {
                // Idle only here: everywhere else there is work in hand.
                busy.store(false, Ordering::Relaxed);
                // Level read, exactly as the service side: permission means
                // "your dependencies are satisfied", and the *decision* to
                // run — skip-if-unchanged, auto_run, params — belongs to the
                // worker below, which already owns it.
                let permitted = startup
                    .as_ref()
                    .filter(|_| !awaiting_artifact)
                    .filter(|startup| {
                        demand.permitted_by(current_level(
                            stack_released,
                            &world,
                            &startup.task_cfg.depends_on,
                        ))
                    })
                    .map(|startup| {
                        // One-shot, like the service side: a run is spent
                        // here, and only a fresh demand re-arms it.
                        demand = super::Demand::None;
                        RunRequest {
                            reply: None,
                            params: std::collections::HashMap::new(),
                            mode: super::task_worker::TaskRunMode::Startup {
                                has_dependents: startup.has_dependents,
                            },
                            intent: super::TaskRunIntent::Scheduled,
                            start_message: None,
                        }
                    });
                match permitted {
                    Some(request) => {
                        busy.store(true, Ordering::Relaxed);
                        request
                    }
                    None => {
                        tokio::select! {
                            received = rx.recv() => match received {
                                Some(command) => {
                                    // Nothing is in hand, so a cancel has
                                    // nothing to kill — only its follow-up
                                    // run, if it has one, survives.
                                    match resolve_command(
                                        command,
                                        &name,
                                        startup.as_ref(),
                                        &last_params,
                                        RunContext {
                                            awaiting_artifact,
                                            phase: owner.phase,
                                            spawned: false,
                                            self_write_suspect: is_self_write_suspect(
                                                last_run_ended,
                                            ),
                                        },
                                    ) {
                                        Ask::Run(request)
                                        | Ask::Cancel { then: Some(request), .. } => {
                                            busy.store(true, Ordering::Relaxed);
                                            // A mailbox run supersedes standing
                                            // demand; withdrawing it here keeps
                                            // the task from running twice.
                                            demand = super::Demand::None;
                                            request
                                        }
                                        Ask::Cancel { then: None, done } => {
                                            // Nothing in hand: the kill is
                                            // already true.
                                            if let Some(done) = done {
                                                let _ = done.send(());
                                            }
                                            continue;
                                        }
                                        Ask::Requery => {
                                            if let Some(startup) = startup.as_ref() {
                                                request_requery(
                                                    &name,
                                                    &startup.task_cfg,
                                                    &ctx,
                                                    &batcher_tx,
                                                    &requery_tx,
                                                );
                                            }
                                            continue;
                                        }
                                        Ask::Park(message) => {
                                            // Waiting for a human, not for a
                                            // dependency. Say so, then publish
                                            // — in that order, so the line
                                            // cannot land behind a dependent
                                            // the phase just unblocked.
                                            let outcome =
                                                NoSpawnOutcome::pending_run(message.clone());
                                            outcome.emit(&ctx.emitter, &name);
                                            if let Some(needs) = outcome.needs_run_now() {
                                                owner.set_needs_run_now(needs);
                                            }
                                            owner.set(outcome.state);
                                            if report_tx
                                                .send(super::ProcessReport::TaskRunPrepared {
                                                    name: name.clone(),
                                                    task_cfg: task_cfg.clone(),
                                                    intent: super::TaskRunIntent::Background,
                                                    result: Ok(TaskRunReport::PendingRun {
                                                        message,
                                                    }),
                                                })
                                                .is_err()
                                            {
                                                return;
                                            }
                                            continue;
                                        }
                                        Ask::Nothing => continue,
                                    }
                                }
                                None => return,
                            },
                            // A peer's facts moved, or setup released the
                            // stack; loop back to the level read. Both waits
                            // end permanently once their sender is gone, so a
                            // `None` means stop selecting rather than spin.
                            changed = world.changed(), if watching_world => {
                                if changed.is_none() {
                                    watching_world = false;
                                }
                                // A blocking dependency may have failed while
                                // this task was waiting on it — or recovered.
                                match owner.reconcile_dependencies() {
                                    Some(message) if message.is_empty() => {
                                        ctx.emitter.service_debug_event(
                                            &name,
                                            "dependency recovered; re-queued",
                                        );
                                    }
                                    Some(message) => {
                                        ctx.emitter.service_error_event(&name, &message);
                                    }
                                    None => {}
                                }
                                continue;
                            }
                            released = released_rx.changed(), if !stack_released => {
                                stack_released =
                                    released.is_err() || *released_rx.borrow();
                                continue;
                            }
                            // A re-query this supervisor asked for. The new
                            // patterns are already registered; a graph that
                            // moved means the task should run against it.
                            outcome = requery_rx.recv() => {
                                use crate::build_tool::batch::RequeryOutcome;
                                if outcome != Some(RequeryOutcome::Updated) {
                                    continue;
                                }
                                ctx.emitter
                                    .service_event(&name, "build graph changed — re-running");
                                // Same policy a watched-file change goes
                                // through: auto_run and declared params still
                                // decide whether this means a run.
                                match resolve_command(
                                    TaskCommand::Rerun,
                                    &name,
                                    startup.as_ref(),
                                    &last_params,
                                    RunContext {
                                        awaiting_artifact,
                                        phase: owner.phase,
                                        spawned: false,
                                        // The build graph moved, not the
                                        // watched inputs — a hash over the
                                        // latter would rule out a run for
                                        // the wrong reason.
                                        self_write_suspect: false,
                                    },
                                ) {
                                    Ask::Run(request) => {
                                        busy.store(true, Ordering::Relaxed);
                                        demand = super::Demand::None;
                                        request
                                    }
                                    _ => continue,
                                }
                            }
                            // This task's artifact, from the build manager.
                            outcome = prepare_rx.recv() => {
                                use crate::build_tool::batch::PrepareOutcome;
                                let Some(outcome) = outcome else { continue };
                                match outcome {
                                    // Nothing to record: a task runs the
                                    // command it was configured with, and the
                                    // build only had to make the target exist.
                                    PrepareOutcome::Ready { .. } => {
                                        awaiting_artifact = false;
                                        if owner.phase == crate::process::TaskState::Building {
                                            owner.set(crate::process::TaskState::Pending);
                                        }
                                    }
                                    // Sources changed mid-build; the build
                                    // manager said so. Ask again. The task
                                    // stays `Building` throughout.
                                    PrepareOutcome::Stale => {
                                        awaiting_artifact = match startup.as_ref() {
                                            Some(startup) => request_artifact(
                                                &name,
                                                &startup.task_cfg,
                                                &ctx,
                                                &prepare_tx,
                                                &batcher_tx,
                                            ),
                                            None => false,
                                        };
                                    }
                                    PrepareOutcome::Failed(message) => {
                                        awaiting_artifact = false;
                                        // Not retried — see the service side.
                                        demand = super::Demand::None;
                                        // Classified like any other run that
                                        // never spawned: one was asked for,
                                        // and it did not happen. Says the
                                        // same thing and records the same
                                        // thing as a failure to prepare.
                                        let outcome = NoSpawnOutcome::failed(format!(
                                            "build failed: {message}"
                                        ));
                                        outcome.emit(&ctx.emitter, &name);
                                        let last_run =
                                            outcome.record(&ctx.base_dir, &name).await;
                                        owner.settle_without_run(outcome.state, last_run);
                                    }
                                }
                                continue;
                            }
                        }
                    }
                }
            }
        };
        let RunRequest {
            params,
            mode,
            intent,
            reply,
            start_message,
        } = request;
        // Admitted. `don run` is answered here and stops caring; `don run
        // --wait` keeps its channel and is answered by whatever this run
        // settles into.
        let request_wait = match reply {
            Some(RunReply::OnStart(reply)) => {
                let _ = reply.send(Ok(()));
                None
            }
            Some(RunReply::OnExit(wait)) => Some(wait),
            None => None,
        };

        // A triggered run announces itself before preparing, and takes over
        // as the run a restart would reuse. Both used to happen on the
        // scheduler, which is why it kept a copy of the parameters.
        if matches!(intent, super::TaskRunIntent::Background) {
            last_params = params.clone();
        }
        if let Some(message) = start_message {
            // Any follower of the previous run should end cleanly before the
            // next process starts writing.
            if let Some(writer) = service_writer.as_ref() {
                writer.close_follow_sinks().await;
            }
            // A triggered run is outstanding from here, and `Running` is what
            // this task now is. Published before the report, so a dependent
            // reading the snapshot cannot see it as still satisfied.
            owner.set_needs_run_now(true);
            owner.set(crate::process::TaskState::Running);
            if report_tx
                .send(super::ProcessReport::TaskStarting {
                    name: name.clone(),
                    message,
                })
                .is_err()
            {
                return;
            }
        }

        let worker = super::task_worker::run_task_worker(
            ctx.clone(),
            &name,
            task_cfg.as_ref(),
            &params,
            mode,
        );
        tokio::pin!(worker);

        // Watch for a newer command while the current run prepares, keeping
        // only the most recent — anything older is already superseded too.
        let mut superseded: Option<RunRequest> = None;
        // A cancel that lands mid-preparation cannot stop the spawn (dropping
        // the worker would take the handle with it), so it is recorded and
        // paid out below once preparation has finished.
        let mut abandoned = false;
        let mut cancel_done: Option<tokio::sync::oneshot::Sender<()>> = None;
        let result = loop {
            tokio::select! {
                result = &mut worker => break result,
                next = rx.recv(), if !mailbox_closed => match next {
                    Some(command) => {
                        // Nothing has spawned yet — the worker is still
                        // deciding — so a run arriving now is queued behind
                        // that decision rather than refused.
                        match resolve_command(
                            command,
                            &name,
                            startup.as_ref(),
                            &last_params,
                            RunContext {
                                awaiting_artifact: false,
                                phase: owner.phase,
                                spawned: false,
                                self_write_suspect: is_self_write_suspect(last_run_ended),
                            },
                        ) {
                            Ask::Run(request) => superseded = Some(request),
                            Ask::Cancel { then, done } => {
                                abandoned = true;
                                superseded = then;
                                cancel_done = done.or(cancel_done);
                            }
                            // A run is already being prepared, so there is
                            // nothing to park — it will settle on its own —
                            // and a graph change it should react to arrives
                            // again as its own re-query outcome.
                            Ask::Park(_) | Ask::Requery | Ask::Nothing => {}
                        }
                    }
                    // Guarded so a closed mailbox doesn't spin this select:
                    // `recv` on a closed channel returns immediately, forever.
                    None => mailbox_closed = true,
                },
            }
        };

        if abandoned || superseded.is_some() {
            if let Ok(prepared) = result {
                kill_superseded_spawn(&ctx.emitter, &name, prepared);
            }
            if let Some(done) = cancel_done {
                let _ = done.send(());
            }
            pending = superseded;
            continue;
        }

        // Translate the worker's outcome into the runner-facing report; a
        // spawned run is wired here, by its owner, and held to exit.
        let (report, run) = match result {
            Ok(super::task_worker::TaskRunPrepared::PendingRun { message }) => {
                (Ok(TaskRunReport::PendingRun { message }), None)
            }
            Ok(super::task_worker::TaskRunPrepared::Skipped { message }) => (
                Ok(TaskRunReport::Skipped {
                    message: Some(message),
                }),
                None,
            ),
            Ok(super::task_worker::TaskRunPrepared::Spawned(spawn)) => {
                let super::task::TaskSpawn {
                    mut handle,
                    child_output,
                    rendered_cmdline,
                } = *spawn;
                let pgid = handle.pgid();
                // Wire the spawn: PTY input gate, server-side screen, OSC
                // scanner, output reader — all owned here now.
                let pty_write = handle.take_pty_write();
                let pty_input = match (pty_write, output.as_ref()) {
                    (Some(pty), Some(output)) => {
                        output.register_emulator(80, 24).await;
                        let pty_input = crate::output::spawn_pty_gate(pty);
                        // The scanner handle's drop removes its sink; tying it
                        // to this run's scope is exactly the lifetime we want.
                        let osc = output.add_osc_sink(pty_input.clone()).await;
                        // Attach goes through the output state, not the
                        // runner: register this run's gate for clients.
                        output.set_attach_pty(pty_input.clone()).await;
                        Some((pty_input, osc))
                    }
                    _ => None,
                };
                let reader = service_writer.as_ref().map(|writer| {
                    let writer = writer.clone();
                    tokio::spawn(async move {
                        let _ = writer.process_stream(child_output).await;
                    })
                });
                let osc = pty_input.map(|(_, osc)| osc);
                (
                    Ok(TaskRunReport::Running(TaskWired {
                        pgid,
                        rendered_cmdline,
                    })),
                    Some((handle, reader, osc)),
                )
            }
            Err(message) => (Err(message), None),
        };

        // Everything the exit half needs, owned before the request's parts
        // move into the prepared report.
        let outcome = run.as_ref().map(|(handle, _, _)| TaskRunOutcome {
            name: name.clone(),
            task_cfg: (*task_cfg).clone(),
            base_dir: ctx.base_dir.clone(),
            global_watch_ignore: ctx.global_watch_ignore.clone(),
            pgid: handle.pgid(),
            report_tx: report_tx.clone(),
        });

        // Land the phase this run settled into. A run that spawned is `Running`
        // with a pid until its exit half lands above; one that never spawned is
        // classified by `NoSpawnOutcome`.
        let settled: Option<NoSpawnOutcome> = match &report {
            Ok(TaskRunReport::Running(wired)) => {
                owner.set_needs_run_now(true);
                owner.set_pid(Some(wired.pgid));
                owner.set(crate::process::TaskState::Running);
                None
            }
            Ok(TaskRunReport::PendingRun { message }) => {
                Some(NoSpawnOutcome::pending_run(message.clone()))
            }
            Ok(TaskRunReport::Skipped { message }) => Some(NoSpawnOutcome::skipped(
                message.clone().unwrap_or_else(|| "skipped".to_string()),
            )),
            Err(message) => Some(NoSpawnOutcome::failed(message.clone())),
        };
        // What a `--wait` caller is owed if this run never spawns: there is no
        // exit coming, so the outcome that settled it is the answer.
        let no_spawn = settled
            .as_ref()
            .map(|outcome| (outcome.success, outcome.message.clone()));
        if let Some(outcome) = settled {
            // Say why *before* publishing. Publishing is what unblocks this
            // task's dependents, and their supervisors decide the moment they
            // see it — a line emitted afterwards lands behind the "starting..."
            // it was supposed to explain.
            outcome.emit(&ctx.emitter, &name);
            let last_run = outcome.record(&ctx.base_dir, &name).await;
            if let Some(needs_run_now) = outcome.needs_run_now() {
                owner.set_needs_run_now(needs_run_now);
            }
            owner.settle_without_run(outcome.state, last_run);
        }
        if report_tx
            .send(super::ProcessReport::TaskRunPrepared {
                name: name.clone(),
                task_cfg: task_cfg.clone(),
                intent,
                result: report,
            })
            .is_err()
        {
            return;
        }

        // Hold the run to exit. A request arriving mid-run parks and runs
        // strictly after — owning the exit is what makes run N+1 unable to
        // start early, which is the race the old `run_requested` flag and
        // duplicate-pgid guard papered over.
        let Some((mut handle, reader, osc)) = run else {
            // Nothing spawned. Answer the waiter here rather than dropping the
            // channel — a dropped one reads as "the stack went away", which is
            // not what a task that failed to prepare should look like.
            if let Some(run_wait) = request_wait {
                let _ = run_wait.reply.send(match no_spawn {
                    Some((false, message)) => Err(crate::command::CommandError::Failed {
                        name: name.clone(),
                        message,
                    }),
                    _ => Ok(()),
                });
            }
            continue;
        };
        // This run supersedes whatever the last one's waiter was told to
        // expect. Answering here rather than leaving it to a fold is what
        // lets the token go: only one run is ever in hand.
        if let Some(previous) = waiter.take() {
            let _ = previous.send(Err(crate::command::CommandError::Failed {
                name: name.clone(),
                message: "task run was superseded".to_string(),
            }));
        }
        let mut wait_deadline = None;
        if let Some(run_wait) = request_wait {
            waiter = Some(run_wait.reply);
            wait_deadline = run_wait.timeout;
        }
        let Some(outcome) = outcome else { continue };
        let timeout = task_cfg.timeout.clone();
        let start = std::time::Instant::now();
        // Captured before the wait borrows the handle: a cancel arriving
        // mid-run signals the group this supervisor owns.
        let pgid = outcome.pgid;
        let mut cancelled = false;
        // Teardown runs once per run: a second pass would re-signal a group
        // that is already dying.
        let mut tearing_down = false;
        let mut force_rx = crate::signals::force_watch();
        let wait = super::task::wait_for_task(&mut handle, timeout.as_deref());
        tokio::pin!(wait);
        let deadline = wait_deadline
            .as_ref()
            .map(|(duration, _)| tokio::time::Instant::now() + *duration);
        let result = loop {
            tokio::select! {
                result = &mut wait => break result,
                // The `--wait` deadline. The run itself continues: a caller
                // giving up waiting is not a reason to kill their task.
                () = wait_until(&deadline), if waiter.is_some() && deadline.is_some() => {
                    if let (Some(reply), Some((_, spelling))) =
                        (waiter.take(), wait_deadline.as_ref())
                    {
                        let _ = reply.send(Err(crate::command::CommandError::TimedOut {
                            name: name.clone(),
                            timeout: spelling.clone(),
                        }));
                    }
                }
                // Teardown. Answer the caller now, while there is still a
                // channel to answer on, then end this run — but only once the
                // processes that depend on this task are gone, which is what
                // makes teardown reverse-dependency ordered without anything
                // sequencing it.
                _ = shutdown_rx.changed(), if !tearing_down => {
                    if *shutdown_rx.borrow() {
                        tearing_down = true;
                        if let Some(reply) = waiter.take() {
                            let _ = reply.send(Err(crate::command::CommandError::Failed {
                                name: name.clone(),
                                message: "run cancelled by shutdown".to_string(),
                            }));
                        }
                        super::await_dependents_gone(
                            &name,
                            &ctx.emitter,
                            &mut world,
                            &dependents,
                            &mut force_rx,
                        )
                        .await;
                        if !cancelled {
                            cancelled = true;
                            kill_run(&ctx.emitter, &name, pgid);
                        }
                    }
                }
                next = rx.recv(), if !mailbox_closed => match next {
                    Some(command) => {
                        match resolve_command(
                            command,
                            &name,
                            startup.as_ref(),
                            &last_params,
                            RunContext {
                                awaiting_artifact: false,
                                phase: owner.phase,
                                spawned: true,
                                // This task is running right now, so anything
                                // landing under its watch patterns is its own
                                // output until the hash says otherwise.
                                self_write_suspect: true,
                            },
                        ) {
                            // A run queued behind this one starts strictly
                            // after it — owning the exit is what makes that
                            // ordering structural rather than checked.
                            Ask::Run(request) => pending = Some(request),
                            Ask::Cancel { then, done } => {
                                if !cancelled {
                                    cancelled = true;
                                    // A cancel that runs again narrates the
                                    // stop; teardown narrates in bulk.
                                    if then.is_some() {
                                        ctx.emitter
                                            .service_event(&name, "stopping... (requested)");
                                    }
                                    kill_run(&ctx.emitter, &name, pgid);
                                }
                                cancel_done = done.or(cancel_done);
                                pending = then;
                            }
                            // Running now; nothing to park.
                            Ask::Park(_) | Ask::Requery | Ask::Nothing => {}
                        }
                    }
                    None => mailbox_closed = true,
                },
            }
        };
        // Drain the reader before reporting, so "complete" never outruns
        // the task's final output. Then the scanner handle drops with this
        // scope, removing its sink.
        if let Some(reader) = reader {
            await_reader(reader).await;
        }
        drop(osc);
        last_run_ended = Some(std::time::Instant::now());
        // The run is over: unregister attach so new clients are refused and
        // muted stdout resumes before the completion message lands.
        if let Some(output) = output.as_ref() {
            output.clear_attach().await;
        }
        if cancelled {
            // A run somebody ended is not an outcome to fold: its exit status
            // describes the SIGKILL, not the task, and the kill was narrated
            // where it happened. Nothing is recorded either — the scheduler
            // used to reach the same result by dropping the exit report,
            // having compared its pgid against a copy it kept.
            //
            // Custody is still published, though. Whether the *exit* means
            // anything and whether this supervisor still *holds* a process are
            // different questions, and teardown waits on the second one — a
            // cancelled run that never said it let go would hold the whole
            // stack open.
            owner.set_pid(None);
            if let Some(reply) = waiter.take() {
                let _ = reply.send(Err(crate::command::CommandError::Failed {
                    name: name.clone(),
                    message: "task run was cancelled".to_string(),
                }));
            }
            if let Some(done) = cancel_done.take() {
                let _ = done.send(());
            }
            continue;
        }
        outcome
            .finish(
                result,
                start.elapsed(),
                waiter.take(),
                |success, last_run| {
                    if success {
                        owner.complete(last_run);
                    } else {
                        owner.fail(last_run);
                    }
                },
            )
            .await;
    }
}

/// Sleep until a `--wait` deadline, parking forever when there is none.
async fn wait_until(deadline: &Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(*deadline).await,
        None => std::future::pending().await,
    }
}

/// This task's phase, and the write end that tells the rest of the stack
/// about it.
///
/// The task-side twin of `service_supervisor::PhaseOwner`, with one extra job:
/// whether a task *satisfies* a dependent is not readable from its phase. A
/// `Completed` task with an outstanding re-run does not satisfy one, and that
/// depends on its run history, its `auto_run` policy, and whether it declares
/// params a file change cannot supply. All three live here, so the conclusion
/// is computed here and published rather than left for every dependent to
/// re-derive.
struct TaskPhaseOwner {
    name: String,
    deps: Vec<crate::config::Dependency>,
    /// Read for `auto_run` and `params` when deciding satisfaction.
    config: crate::config::Task,
    world: crate::facts::FactsReader,
    facts: crate::facts::FactsPublisher,
    phase: crate::process::TaskState,
    /// Whether this task has ever completed successfully.
    has_success: bool,
    /// Whether it needs another run to bring its watched inputs up to date.
    needs_run_now: bool,
    /// Whether its startup dependency evaluation has happened at all. Until it
    /// has, a `Pending` task satisfies nobody — otherwise a dependent would
    /// start against a task that has not yet decided whether it must run.
    evaluated: bool,
    pid: Option<i32>,
    last_run: Option<TaskRunInfo>,
}

impl TaskPhaseOwner {
    /// Whether a blocking dependent may treat this task as done.
    fn satisfied(&self) -> bool {
        use crate::config::TaskAutoRun;
        use crate::process::TaskState;

        if self.phase == TaskState::DependencyFailed {
            return false;
        }
        if !self.evaluated && self.phase == TaskState::Pending {
            return false;
        }
        if !self.has_success {
            return false;
        }
        // An outstanding run of an unconditional, param-less task means the
        // dependent would start against stale output.
        if self.needs_run_now
            && self.config.params.is_empty()
            && matches!(self.config.auto_run, TaskAutoRun::Always)
        {
            return false;
        }
        true
    }

    fn publish(&mut self) {
        let stranded = if self.phase == crate::process::TaskState::DependencyFailed {
            crate::gate::failed_roots(&self.deps, &self.world.snapshot())
        } else {
            Vec::new()
        };
        self.facts.publish(crate::facts::ProcessFacts::for_task(
            &self.name,
            self.phase,
            self.satisfied(),
            self.pid,
            self.last_run.clone(),
            stranded,
        ));
    }

    fn set(&mut self, phase: crate::process::TaskState) {
        self.phase = phase;
        self.publish();
    }

    /// The phase a run that never spawned settled into, plus the record of the
    /// attempt when there is one, in a single publication.
    ///
    /// Deliberately narrower than [`Self::fail`]: what the task owes its
    /// dependents is the caller's answer, not this one's. The prepare path
    /// takes it from `NoSpawnOutcome::needs_run_now`, and a failed build
    /// leaves it as it was — a task whose build broke has the same run
    /// history it had a moment ago.
    fn settle_without_run(
        &mut self,
        phase: crate::process::TaskState,
        last_run: Option<TaskRunInfo>,
    ) {
        if last_run.is_some() {
            self.last_run = last_run;
        }
        self.phase = phase;
        self.publish();
    }

    /// Record that a run is outstanding (or no longer is). Also marks the
    /// startup evaluation as having happened — reaching this point *is* that
    /// evaluation.
    fn set_needs_run_now(&mut self, needs_run_now: bool) {
        self.evaluated = true;
        self.needs_run_now = needs_run_now;
        self.publish();
    }

    fn set_pid(&mut self, pid: Option<i32>) {
        self.pid = pid;
        self.publish();
    }

    /// A run succeeded: history recorded, nothing outstanding, phase settled.
    /// One publication — a reader that saw `Completed` against a stale
    /// `needs_run_now` would conclude this task still blocks its dependents.
    fn complete(&mut self, last_run: Option<TaskRunInfo>) {
        self.has_success = true;
        self.evaluated = true;
        self.needs_run_now = false;
        self.last_run = last_run;
        self.pid = None;
        self.phase = crate::process::TaskState::Completed;
        self.publish();
    }

    /// A run failed: still outstanding, so dependents stay blocked.
    fn fail(&mut self, last_run: Option<TaskRunInfo>) {
        self.evaluated = true;
        self.needs_run_now = true;
        self.last_run = last_run;
        self.pid = None;
        self.phase = crate::process::TaskState::Failed;
        self.publish();
    }

    /// Reconcile against the dependencies' current facts. See
    /// `service_supervisor::PhaseOwner::reconcile_dependencies`.
    fn reconcile_dependencies(&mut self) -> Option<String> {
        use crate::process::TaskState;
        if !matches!(self.phase, TaskState::Pending | TaskState::DependencyFailed) {
            return None;
        }
        let roots = crate::gate::failed_roots(&self.deps, &self.world.snapshot());
        if !roots.is_empty() {
            let was_stranded = self.phase == TaskState::DependencyFailed;
            let previous = self.facts.current_roots().to_vec();
            self.set(TaskState::DependencyFailed);
            if was_stranded && previous == roots {
                return None;
            }
            return Some(match roots.as_slice() {
                [one] => format!("skipped (dependency '{one}' failed)"),
                many => format!("skipped (dependencies '{}' failed)", many.join("', '")),
            });
        }
        if self.phase == TaskState::DependencyFailed {
            self.set(TaskState::Pending);
            return Some(String::new());
        }
        None
    }
}

/// How far this task's dependencies currently let it go.
///
/// Read at the moment it is needed rather than delivered, which is what makes
/// the old revision stamp unnecessary: demand and permission are both held by
/// this loop, so neither can be staler than the other.
fn current_level(
    released: bool,
    world: &crate::facts::FactsReader,
    depends_on: &[crate::config::Dependency],
) -> crate::gate::Gate {
    if !released {
        return crate::gate::Gate::Blocked;
    }
    crate::gate::level(depends_on, &world.snapshot())
}

/// Join the finished reader, bounded — a wedged sink must not hold the
/// supervisor hostage.
async fn await_reader(handle: tokio::task::JoinHandle<()>) {
    let mut handle = handle;
    if tokio::time::timeout(std::time::Duration::from_secs(2), &mut handle)
        .await
        .is_err()
    {
        handle.abort();
        let _ = handle.await;
    }
}

/// How prominently a settled run's message is reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Report {
    /// Normal lifecycle line.
    Info,
    /// Verbose-only — the run was a no-op and nobody asked.
    Debug,
    /// The run failed.
    Error,
}

/// A prepared run that ended without leaving a process behind.
///
/// Three of the five outcomes of preparing a run never spawn: the task is
/// waiting on something (`PendingRun`), its inputs were unchanged so it was
/// skipped, or preparation itself failed. They were three near-identical
/// branches on the runner; the differences between them are exactly the
/// fields here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NoSpawnOutcome {
    /// Lifecycle state the task enters.
    pub(crate) state: super::TaskState,
    /// What the dependency scheduler is told. A skipped or deferred task is
    /// still a *success* — it didn't fail, it just didn't run.
    pub(crate) success: bool,
    pub(crate) message: String,
    pub(crate) report: Report,
}

impl NoSpawnOutcome {
    /// The task can't run yet and is waiting on something.
    pub(crate) fn pending_run(message: String) -> Self {
        Self {
            state: super::TaskState::PendingRun,
            success: true,
            message,
            report: Report::Info,
        }
    }

    /// The task's watched inputs were unchanged, so it didn't need to run.
    pub(crate) fn skipped(message: String) -> Self {
        Self {
            state: super::TaskState::Skipped,
            success: true,
            message,
            report: Report::Debug,
        }
    }

    /// Preparing the run failed before anything was spawned.
    pub(crate) fn failed(message: String) -> Self {
        Self {
            state: super::TaskState::Failed,
            success: false,
            message,
            report: Report::Error,
        }
    }

    /// Whether to update `needs_run_now`, and to what. `None` leaves it alone.
    ///
    /// A run that failed to prepare has not run, however it was triggered, so
    /// the task still needs one. This used to depend on *who asked*: a
    /// scheduled failure set the flag and a background `don run` failure left
    /// it alone, which meant a task could fail under `don run` and the next
    /// startup sweep would see nothing outstanding and skip it.
    pub(crate) fn needs_run_now(&self) -> Option<bool> {
        match self.state {
            super::TaskState::PendingRun | super::TaskState::Failed => Some(true),
            super::TaskState::Skipped => Some(false),
            _ => None,
        }
    }

    /// Persist this outcome as a run record, when it is one, and return what
    /// was written so the phase can be published with it.
    ///
    /// A prepare failure is a run that was asked for and did not happen, so it
    /// belongs in the record. Without it the record keeps describing the last
    /// run that *spawned*: the task reads `failed` in the state column and
    /// `ok` in the result column, and a restart — which starts every phase at
    /// `Pending` and reads the record back off disk — loses the failure
    /// entirely.
    ///
    /// Only the run record is written. The success marker and the
    /// watched-input hashes are deliberately left alone, exactly as for a run
    /// that spawned and failed, so this task still does not satisfy its
    /// dependents and is still not skipped next time.
    ///
    /// `PendingRun` and `Skipped` record nothing: no run was attempted, so the
    /// existing record still describes the last run there was.
    pub(crate) async fn record(&self, base_dir: &Path, name: &str) -> Option<TaskRunInfo> {
        if self.state != super::TaskState::Failed {
            return None;
        }
        // No duration and no exit code: nothing ran, so there is nothing to
        // report but when it was tried and why it wasn't.
        let last_run = TaskRunInfo::finished_now(false, None, None, Some(self.message.clone()));
        let task_state = TaskStateStore::new(base_dir.join(".don").join("task-state"));
        let _ = task_state.record_run(name, &last_run).await;
        Some(last_run)
    }

    /// Emit this outcome's message at its own level.
    pub(crate) fn emit(&self, emitter: &crate::output::LifecycleEmitter, name: &str) {
        match self.report {
            Report::Info => emitter.service_event(name, &self.message),
            Report::Debug => emitter.service_debug_event(name, &self.message),
            Report::Error => emitter.service_error_event(name, &self.message),
        }
    }
}

/// How long a superseded process gets to die politely before SIGKILL lands.
const SUPERSEDED_KILL_GRACE: Duration = Duration::from_millis(500);

/// Kill the process from a run that has been superseded by a newer one.
///
/// A run that loses a race may already have spawned; the process is live and
/// nothing else will ever reap it, so it has to be killed here. Today the
/// runner discovers this by comparing generations after the fact. Once a
/// supervisor owns the run it will call this directly when it cancels one —
/// same work, but as cleanup of something it owns rather than as recovery
/// from a race it could not prevent.
///
/// Detached on purpose: the caller is on the runner's command loop, and
/// waiting out a grace period there would stall every other process.
///
/// Takes the untagged emitter rather than an `ProcessOutput` so the kill can
/// never be gated on a name lookup succeeding — failing to log is a cosmetic
/// problem, failing to kill leaks a process nothing will ever reap.
pub(crate) fn kill_superseded_spawn(
    emitter: &crate::output::LifecycleEmitter,
    name: &str,
    prepared: super::task_worker::TaskRunPrepared,
) {
    use super::task_worker::TaskRunPrepared;

    match prepared {
        TaskRunPrepared::Spawned(spawn) => {
            let super::task::TaskSpawn {
                mut handle,
                child_output,
                rendered_cmdline: _,
            } = *spawn;
            // Drop the read half first: nothing is going to consume it, and
            // holding it open keeps the child's pipe alive.
            drop(child_output);
            emitter.service_event(
                name,
                &format!("send SIGKILL to stale task pgid {}", handle.pgid()),
            );
            tokio::spawn(async move {
                let _ = handle
                    .terminate(nix::sys::signal::Signal::SIGKILL, SUPERSEDED_KILL_GRACE)
                    .await;
            });
        }
        // Nothing was spawned, so there is nothing to clean up.
        TaskRunPrepared::PendingRun { .. } | TaskRunPrepared::Skipped { .. } => {}
    }
}

/// Everything the exit half of a task run needs, owned outright.
///
/// Owned rather than borrowed because this outlives the runner's command loop
/// — the exit wait is a detached task, and holding a reference into runner
/// state across it is what the whole decomposition is trying to stop.
pub(crate) struct TaskRunOutcome {
    pub(crate) name: String,
    pub(crate) task_cfg: crate::config::Task,
    pub(crate) base_dir: PathBuf,
    pub(crate) global_watch_ignore: Vec<String>,
    /// Process group of the run that just ended.
    pub(crate) pgid: i32,
    /// Exit reports for non-scheduled runs travel on the processes' lossless
    /// report channel, like service exits.
    pub(crate) report_tx: mpsc::UnboundedSender<super::ProcessReport>,
}

impl TaskRunOutcome {
    /// Record a finished run and send exactly one completion message.
    ///
    /// Both the background and foreground wait paths end here, so the rules
    /// for what counts as success, what gets persisted, and who is told stay
    /// in one place — they were duplicated verbatim before, which is how they
    /// drift.
    ///
    /// A successful run records its watched inputs alongside the run info, so
    /// the next startup can skip it when nothing changed; a failed one records
    /// only the run info, leaving the previous input hashes stale on purpose
    /// so the task is not skipped next time.
    ///
    /// Returns the run info so the caller can publish this task's phase
    /// *before* the report goes out. That ordering is load-bearing: the
    /// scheduler drains facts before handling any report, which only makes
    /// "the reply implies the phase is visible" true if the facts were sent
    /// first.
    pub(crate) async fn finish(
        self,
        result: Result<std::process::ExitStatus, super::task::TaskError>,
        elapsed: Duration,
        reply: Option<tokio::sync::oneshot::Sender<crate::command::CommandResult>>,
        publish: impl FnOnce(bool, Option<TaskRunInfo>),
    ) {
        let (success, exit_code, message) = match result {
            Ok(status) if status.success() => (true, status.code(), None),
            Ok(status) => {
                let code = status.code().unwrap_or(-1);
                (false, status.code(), Some(format!("exit code {code}")))
            }
            Err(e) => (false, None, Some(e.to_string())),
        };
        let last_run =
            TaskRunInfo::finished_now(success, Some(elapsed), exit_code, message.clone());

        let task_state = TaskStateStore::new(self.base_dir.join(".don").join("task-state"));
        if success {
            let task_dir = working_dir_for(&self.base_dir, self.task_cfg.dir.as_deref());
            let ignore_patterns = resolve_watch_ignore_patterns(
                &task_dir,
                &self.task_cfg.ignore,
                &self.base_dir,
                &self.global_watch_ignore,
            );
            let _ = task_state
                .record_success_with_info(
                    &self.name,
                    &self.task_cfg.watch,
                    &ignore_patterns,
                    Some(&task_dir),
                    &last_run,
                )
                .await;
        } else {
            let _ = task_state.record_run(&self.name, &last_run).await;
        }

        // Phase first, then the report that carries the reply.
        publish(success, Some(last_run.clone()));
        let _ = self
            .report_tx
            .send(super::ProcessReport::TaskExited(TaskExit {
                name: self.name,
                success,
                message,
                elapsed: Some(elapsed),
                last_run: Some(last_run),
                reply,
            }));
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;

    /// A real parsed task, so the defaults here are the product's defaults.
    fn test_task() -> crate::config::Task {
        let config: crate::config::Config = "[tasks.build]\ncmd = \"true\"\n".parse().unwrap();
        config.tasks.get("build").unwrap().clone()
    }

    /// A task declaring one required param, for the restart-reuse rules.
    fn param_task() -> crate::config::Task {
        let config: crate::config::Config =
            "[tasks.seed]\ncmd = \"true\"\n[[tasks.seed.params]]\nname = \"env\"\nrequired = true\n"
                .parse()
                .unwrap();
        config.tasks.get("seed").unwrap().clone()
    }

    /// A run request as a client sends one: unresolved params, and a reply
    /// waiting to hear whether it was admitted.
    fn run(
        params: Vec<(&'static str, &'static str)>,
    ) -> impl Fn(Option<tokio::sync::oneshot::Sender<crate::command::CommandResult>>) -> TaskCommand
    {
        move |reply| {
            TaskCommand::Run(RunRequest {
                params: params
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect(),
                mode: super::super::task_worker::TaskRunMode::Triggered,
                intent: super::super::TaskRunIntent::Background,
                reply: reply.map(RunReply::OnStart),
                start_message: None,
            })
        }
    }

    /// Every command, resolved against what the supervisor holds. This is the
    /// whole of what moved off the scheduler: whether a run is admitted at all
    /// depends on the phase this supervisor published and the run it has in
    /// hand, and a restart's meaning depends on the parameters of the last run.
    /// All three live here now.
    #[tokio::test]
    async fn commands_resolve_against_what_the_supervisor_holds() {
        struct Case {
            name: &'static str,
            command: Box<
                dyn Fn(
                    Option<tokio::sync::oneshot::Sender<crate::command::CommandResult>>,
                ) -> TaskCommand,
            >,
            task: crate::config::Task,
            last_params: Vec<(&'static str, &'static str)>,
            /// The phase this supervisor has published for itself.
            phase: super::super::TaskState,
            /// Whether a run of it is actually executing.
            spawned: bool,
            /// Whether a watch trigger arriving now could be the last run's
            /// own output landing back under its watch patterns.
            self_write_suspect: bool,
            want: &'static str,
            /// The mode the resolved run carries, when it makes one.
            want_mode: Option<&'static str>,
            /// Parameters the resolved run carries, when it makes one.
            want_params: Vec<(&'static str, &'static str)>,
            want_reply: Option<bool>,
        }

        let cases = vec![
            Case {
                name: "an idle task admits a run",
                command: Box::new(run(vec![])),
                task: test_task(),
                last_params: vec![],
                phase: super::super::TaskState::Pending,
                spawned: false,
                self_write_suspect: false,
                want_mode: None,
                want: "run",
                want_params: vec![],
                // Admission is not the answer: `don run` is told it was
                // accepted only once the run is picked up.
                want_reply: None,
            },
            Case {
                // The check that used to be `is_busy` on the scheduler.
                name: "a task with a run executing refuses a second",
                command: Box::new(run(vec![])),
                task: test_task(),
                last_params: vec![],
                phase: super::super::TaskState::Pending,
                spawned: true,
                self_write_suspect: false,
                want_mode: None,
                want: "nothing",
                want_params: vec![],
                want_reply: Some(false),
            },
            Case {
                // Busy is not the same as running. During the startup sweep
                // every task has a worker deciding skip/pending/run; refusing
                // there is what made pressing "run" wait out the whole sweep.
                name: "a task still deciding whether to run admits one",
                command: Box::new(run(vec![])),
                task: test_task(),
                last_params: vec![],
                phase: super::super::TaskState::Skipped,
                spawned: false,
                self_write_suspect: false,
                want_mode: None,
                want: "run",
                want_params: vec![],
                want_reply: None,
            },
            Case {
                // …and the check that used to read the scheduler's state map.
                name: "a running task refuses a run",
                command: Box::new(run(vec![])),
                task: test_task(),
                last_params: vec![],
                phase: super::super::TaskState::Running,
                spawned: false,
                self_write_suspect: false,
                want_mode: None,
                want: "nothing",
                want_params: vec![],
                want_reply: Some(false),
            },
            Case {
                name: "params are resolved against the task's own declaration",
                command: Box::new(run(vec![("env", "staging")])),
                task: param_task(),
                last_params: vec![],
                phase: super::super::TaskState::Pending,
                spawned: false,
                self_write_suspect: false,
                want_mode: None,
                want: "run",
                want_params: vec![("env", "staging")],
                want_reply: None,
            },
            Case {
                name: "a missing required param is refused, not run",
                command: Box::new(run(vec![])),
                task: param_task(),
                last_params: vec![],
                phase: super::super::TaskState::Pending,
                spawned: false,
                self_write_suspect: false,
                want_mode: None,
                want: "nothing",
                want_params: vec![],
                want_reply: Some(false),
            },
            Case {
                name: "a watch trigger outside the self-write window is taken at its word",
                command: Box::new(|_| TaskCommand::Rerun),
                task: test_task(),
                last_params: vec![],
                phase: super::super::TaskState::Completed,
                spawned: false,
                self_write_suspect: false,
                want_mode: Some("triggered"),
                want: "run",
                want_params: vec![],
                want_reply: None,
            },
            Case {
                name: "a watch trigger inside the self-write window is verified first",
                command: Box::new(|_| TaskCommand::Rerun),
                task: test_task(),
                last_params: vec![],
                phase: super::super::TaskState::Completed,
                spawned: false,
                self_write_suspect: true,
                want_mode: Some("verify"),
                want: "run",
                want_params: vec![],
                want_reply: None,
            },
            Case {
                name: "a kill cancels and does not run again",
                command: Box::new(|_| TaskCommand::Kill { done: None }),
                task: test_task(),
                last_params: vec![],
                phase: super::super::TaskState::Running,
                spawned: true,
                self_write_suspect: false,
                want_mode: None,
                want: "cancel-only",
                want_params: vec![],
                want_reply: None,
            },
            Case {
                name: "a param-less restart reuses nothing and is accepted",
                command: Box::new(|reply| TaskCommand::Restart { reply }),
                task: test_task(),
                last_params: vec![],
                phase: super::super::TaskState::Completed,
                spawned: false,
                self_write_suspect: false,
                want_mode: None,
                want: "cancel-then-run",
                want_params: vec![],
                want_reply: Some(true),
            },
            Case {
                name: "a param'd task with a previous run reuses its values",
                command: Box::new(|reply| TaskCommand::Restart { reply }),
                task: param_task(),
                last_params: vec![("env", "staging")],
                phase: super::super::TaskState::Completed,
                spawned: false,
                self_write_suspect: false,
                want_mode: None,
                want: "cancel-then-run",
                want_params: vec![("env", "staging")],
                want_reply: Some(true),
            },
            Case {
                // The check that used to read the scheduler's copy of the
                // parameters. Nothing to reuse means nothing to restart.
                name: "a param'd task with no previous run is refused",
                command: Box::new(|reply| TaskCommand::Restart { reply }),
                task: param_task(),
                last_params: vec![],
                phase: super::super::TaskState::Completed,
                spawned: false,
                self_write_suspect: false,
                want_mode: None,
                want: "nothing",
                want_params: vec![],
                want_reply: Some(false),
            },
        ];

        for case in cases {
            let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
            let command = (case.command)(Some(reply_tx));
            let startup = StartupConfig {
                task_cfg: Box::new(case.task),
                has_dependents: false,
                has_success: false,
                last_run: None,
            };
            let last_params: std::collections::HashMap<String, String> = case
                .last_params
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect();

            let ask = resolve_command(
                command,
                "seed",
                Some(&startup),
                &last_params,
                RunContext {
                    awaiting_artifact: false,
                    phase: case.phase,
                    spawned: case.spawned,
                    self_write_suspect: case.self_write_suspect,
                },
            );
            let mode_label = |mode: &super::super::task_worker::TaskRunMode| match mode {
                super::super::task_worker::TaskRunMode::Startup { .. } => "startup",
                super::super::task_worker::TaskRunMode::Triggered => "triggered",
                super::super::task_worker::TaskRunMode::Verify => "verify",
            };
            let (got, params, mode) = match &ask {
                Ask::Run(request) => (
                    "run",
                    Some(request.params.clone()),
                    Some(mode_label(&request.mode)),
                ),
                Ask::Cancel { then: None, .. } => ("cancel-only", None, None),
                Ask::Cancel {
                    then: Some(request),
                    ..
                } => (
                    "cancel-then-run",
                    Some(request.params.clone()),
                    Some(mode_label(&request.mode)),
                ),
                Ask::Park(_) => ("park", None, None),
                Ask::Requery => ("requery", None, None),
                Ask::Nothing => ("nothing", None, None),
            };
            // An admitted run still holds the reply — it is answered when the
            // run is picked up, not here. Drop it so "nobody answered" is a
            // closed channel rather than a wait that never ends.
            drop(ask);
            assert_eq!(got, case.want, "{}", case.name);
            if let Some(want_mode) = case.want_mode {
                assert_eq!(mode, Some(want_mode), "{}: run mode", case.name);
            }
            if let Some(params) = params {
                let want: std::collections::HashMap<String, String> = case
                    .want_params
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect();
                assert_eq!(params, want, "{}: run params", case.name);
            }
            match case.want_reply {
                Some(ok) => assert_eq!(reply_rx.await.unwrap().is_ok(), ok, "{}: reply", case.name),
                // Nothing answered it, so the sender dropped with the command.
                None => assert!(reply_rx.await.is_err(), "{}: unexpected reply", case.name),
            }
        }
    }

    fn outcome(
        name: &str,
        base_dir: &std::path::Path,
        report_tx: mpsc::UnboundedSender<super::super::ProcessReport>,
    ) -> TaskRunOutcome {
        TaskRunOutcome {
            name: name.to_string(),
            task_cfg: test_task(),
            base_dir: base_dir.to_path_buf(),
            global_watch_ignore: Vec::new(),
            pgid: 4242,
            report_tx,
        }
    }

    #[test]
    fn no_spawn_outcomes_classify_consistently() {
        use super::super::TaskState;

        struct Case {
            label: &'static str,
            outcome: NoSpawnOutcome,
            want_state: TaskState,
            want_success: bool,
            want_report: Report,
            want_needs: Option<bool>,
        }

        let cases = vec![
            Case {
                label: "deferred",
                outcome: NoSpawnOutcome::pending_run("waiting on deps".to_string()),
                want_state: TaskState::PendingRun,
                // Not a failure: it just hasn't run yet.
                want_success: true,
                want_report: Report::Info,
                want_needs: Some(true),
            },
            Case {
                label: "skipped",
                outcome: NoSpawnOutcome::skipped("no changes".to_string()),
                want_state: TaskState::Skipped,
                want_success: true,
                // Verbose-only: nobody asked for a no-op to be announced.
                want_report: Report::Debug,
                want_needs: Some(false),
            },
            Case {
                label: "prepare failed",
                outcome: NoSpawnOutcome::failed("bad param".to_string()),
                want_state: TaskState::Failed,
                want_success: false,
                want_report: Report::Error,
                // A failed run hasn't run, whoever asked for it — so the
                // task still needs one. This used to be `None` for a
                // background `don run`, which let the next startup sweep
                // skip a task that had just failed.
                want_needs: Some(true),
            },
        ];

        for case in cases {
            assert_eq!(case.outcome.state, case.want_state, "{}: state", case.label);
            assert_eq!(
                case.outcome.success, case.want_success,
                "{}: success",
                case.label
            );
            assert_eq!(
                case.outcome.report, case.want_report,
                "{}: report level",
                case.label
            );
            assert_eq!(
                case.outcome.needs_run_now(),
                case.want_needs,
                "{}: needs_run_now",
                case.label
            );
        }
    }

    /// A run that was asked for and never happened is still a run, and the
    /// record has to say so. Otherwise it keeps describing the last run that
    /// spawned — the table reads `failed` in the state column and `ok` in the
    /// result column — and a restart, which starts every phase at `Pending`
    /// and reads the record off disk, loses the failure entirely.
    #[tokio::test]
    async fn a_run_that_never_spawned_records_what_it_can() {
        struct Case {
            label: &'static str,
            outcome: NoSpawnOutcome,
            /// Whether this attempt belongs in the run record at all.
            want_record: bool,
            /// What the record says afterwards. `true` for the outcomes that
            /// write nothing: the previous successful run still stands.
            want_stored_success: bool,
        }

        let cases = vec![
            Case {
                label: "prepare failed",
                outcome: NoSpawnOutcome::failed("bad param".to_string()),
                want_record: true,
                want_stored_success: false,
            },
            Case {
                label: "deferred",
                outcome: NoSpawnOutcome::pending_run("waiting on deps".to_string()),
                want_record: false,
                want_stored_success: true,
            },
            Case {
                label: "skipped",
                outcome: NoSpawnOutcome::skipped("no changes".to_string()),
                want_record: false,
                want_stored_success: true,
            },
        ];

        for case in cases {
            let temp = tempfile::tempdir().unwrap();
            let state = TaskStateStore::new(temp.path().join(".don").join("task-state"));
            // A task that has run, and worked, before this attempt.
            state.record_success("build", &[], &[], None).await.unwrap();

            let published = case.outcome.record(temp.path(), "build").await;
            let stored = state.last_run("build").await.unwrap().unwrap();

            assert_eq!(
                published.is_some(),
                case.want_record,
                "{}: published a record",
                case.label
            );
            assert_eq!(
                stored.success, case.want_stored_success,
                "{}: what the record says now",
                case.label
            );
            if case.want_record {
                assert_eq!(
                    published.as_ref(),
                    Some(&stored),
                    "{}: what was published is what was stored",
                    case.label
                );
                assert_eq!(
                    stored.message.as_deref(),
                    Some("bad param"),
                    "{}: the record carries why",
                    case.label
                );
                assert_eq!(
                    (stored.duration_ms, stored.exit_code),
                    (None, None),
                    "{}: nothing ran, so nothing took time or exited",
                    case.label
                );
            }
            // The other half: recording the attempt must not touch the gates.
            // A failure that never ran leaves the success marker alone, so the
            // task still doesn't satisfy its dependents on the strength of it,
            // and leaves the input hashes alone, so it isn't skipped next time.
            assert!(
                state.has_success("build").await.unwrap(),
                "{}: success marker untouched",
                case.label
            );
        }
    }

    fn owner_for(task_toml: &str, has_success: bool) -> TaskPhaseOwner {
        let config: crate::config::Config = task_toml.parse().unwrap();
        let (name, task) = config.tasks.iter().next().unwrap();
        let (aggregator, mut publishers, reader) = crate::facts::channel(std::iter::once((
            name.clone(),
            crate::facts::ProcessFacts::for_task(
                name,
                crate::process::TaskState::Pending,
                false,
                None,
                None,
                Vec::new(),
            ),
        )));
        std::mem::forget(aggregator);
        TaskPhaseOwner {
            name: name.clone(),
            deps: task.depends_on.clone(),
            config: task.clone(),
            world: reader,
            facts: publishers.remove(name).unwrap(),
            phase: crate::process::TaskState::Completed,
            has_success,
            needs_run_now: false,
            evaluated: true,
            pid: None,
            last_run: None,
        }
    }

    /// Satisfaction is not readable from the phase: a `Completed` task with an
    /// outstanding run must still block its dependents, or they start against
    /// stale output. The task supervisor's `NoSpawnOutcome` classifier tests
    /// pin the other half of this — what *makes* a run outstanding.
    #[test]
    fn an_outstanding_run_blocks_dependents_despite_a_successful_history() {
        let mut owner = owner_for("[tasks.build]\ncmd = \"true\"\n", true);
        assert!(owner.satisfied(), "a completed task satisfies dependents");

        owner.set_needs_run_now(true);
        assert!(
            !owner.satisfied(),
            "an outstanding run must block dependents"
        );
    }

    /// Settling a run that never spawned records the attempt and decides
    /// nothing else. What the task owes its dependents belongs to the caller —
    /// the prepare path answers it from `NoSpawnOutcome::needs_run_now`, and a
    /// failed build leaves it alone — so this must not quietly answer it too.
    #[test]
    fn settling_without_a_run_records_the_attempt_and_leaves_the_gates_alone() {
        use crate::process::TaskState;

        struct Case {
            label: &'static str,
            phase: TaskState,
            last_run: Option<TaskRunInfo>,
            want_message: Option<&'static str>,
        }

        let attempt =
            |message: &str| TaskRunInfo::finished_now(false, None, None, Some(message.to_string()));

        let cases = vec![
            Case {
                label: "a failure that never ran",
                phase: TaskState::Failed,
                last_run: Some(attempt("build failed: exit code 2")),
                want_message: Some("build failed: exit code 2"),
            },
            Case {
                label: "an outcome with nothing to record",
                phase: TaskState::Skipped,
                last_run: None,
                // Nothing was attempted, so the previous record stands —
                // here, no record at all.
                want_message: None,
            },
        ];

        for case in cases {
            let mut owner = owner_for("[tasks.build]\ncmd = \"true\"\n", true);
            owner.set_needs_run_now(true);

            owner.settle_without_run(case.phase, case.last_run);

            assert_eq!(owner.phase, case.phase, "{}: phase", case.label);
            assert_eq!(
                owner
                    .last_run
                    .as_ref()
                    .and_then(|run| run.message.as_deref()),
                case.want_message,
                "{}: the record",
                case.label
            );
            assert!(
                owner.needs_run_now,
                "{}: what the task owes its dependents is not this call's to change",
                case.label
            );
        }
    }

    /// …but only for a task that would re-run on its own. A param'd task, or
    /// one that is not `auto_run = always`, is waiting for a human — holding
    /// its dependents for that would deadlock startup.
    #[test]
    fn an_outstanding_run_on_a_manual_task_does_not_block_dependents() {
        for toml in [
            "[tasks.build]\ncmd = \"true\"\nauto_run = false\n",
            "[tasks.build]\ncmd = \"true\"\n[[tasks.build.params]]\nname = \"target\"\n",
        ] {
            let mut owner = owner_for(toml, true);
            owner.set_needs_run_now(true);
            assert!(
                owner.satisfied(),
                "a task waiting for a human must not block its dependents: {toml}"
            );
        }
    }

    /// A task that has never succeeded satisfies nobody, whatever its phase.
    #[test]
    fn a_task_with_no_successful_history_satisfies_nobody() {
        let owner = owner_for("[tasks.build]\ncmd = \"true\"\n", false);
        assert!(!owner.satisfied());
    }

    /// The registry is the addressing half and nothing more: a clone can
    /// reach a task, and an unknown name is `None` rather than something
    /// created on demand. If lookups ever started inserting, the map would
    /// need synchronising and the lock-free `Arc<HashMap<_, _>>` would go.
    #[tokio::test]
    async fn the_registry_addresses_tasks_without_creating_them() {
        let temp = tempfile::tempdir().unwrap();
        let output = crate::output::OutputManager::new(&[], tokio::io::sink())
            .await
            .unwrap();
        let ctx = super::super::task_worker::TaskWorkerContext {
            bazel_config: None,
            base_dir: temp.path().to_path_buf(),
            platform: crate::config::Platform::LinuxX86_64,
            emitter: output.clone_lifecycle_emitter(),
            global_watch_ignore: Vec::new(),
            endpoints: {
                let (writer, reader) = crate::endpoints::channel();
                // Keep the writer alive for the reader's lifetime.
                std::mem::forget(writer);
                reader
            },
        };
        let names = ["build".to_string(), "migrate".to_string()];
        let (report_tx, _report_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        std::mem::forget(shutdown_tx);
        let (facts_aggregator, facts_publishers, facts) = crate::facts::channel(std::iter::empty());
        // Keep the write ends alive for the reader's lifetime.
        std::mem::forget(facts_aggregator);
        let mut facts_publishers = facts_publishers;
        let (released_tx, released_rx) = tokio::sync::watch::channel(true);
        std::mem::forget(released_tx);
        let mut supervisors = spawn_supervisors(
            names.iter(),
            &ctx,
            &|_| None,
            &|_| None,
            &|_| Vec::new(),
            &report_tx,
            &facts,
            &mut facts_publishers,
            &released_rx,
            &shutdown_rx,
            &{
                let (tx, rx) = mpsc::unbounded_channel();
                // Keep the receiver alive so sends succeed; nothing drains it.
                std::mem::forget(rx);
                tx
            },
        );
        let registry = supervisors.registry().clone();

        assert!(registry.get("build").is_some());
        assert!(registry.get("migrate").is_some());
        assert!(
            registry.get("never-declared").is_none(),
            "an unknown name must not be conjured into existence"
        );
        assert!(
            !registry.is_busy("never-declared"),
            "an unknown name is not busy — callers ask this to decide if they may start it"
        );
        assert!(!registry.is_busy("build"), "nothing queued yet");

        // Aborting drops the receivers, so every outstanding handle — this
        // clone included — reports failure rather than queueing into a void.
        for (_, join) in supervisors.abort_all() {
            let _ = join.await;
        }
        let handle = registry.get("build").unwrap().clone();
        assert!(
            !handle.request(TaskCommand::Run(RunRequest {
                reply: None,
                params: std::collections::HashMap::new(),
                mode: super::super::task_worker::TaskRunMode::Triggered,
                intent: super::super::TaskRunIntent::Background,
                start_message: None,
            })),
            "a handle to a stopped supervisor must report the failure"
        );
    }

    /// The bug this classifier used to encode, stated as the behaviour a
    /// user would see: a task whose preparation fails under `don run` must
    /// still look outstanding to the next startup sweep. Previously the
    /// background case returned `None`, leaving `needs_run_now` false, so a
    /// task that had just failed was treated as satisfied.
    #[test]
    fn a_failed_run_leaves_the_task_needing_one_however_it_was_triggered() {
        let failed = NoSpawnOutcome::failed("bad param".to_string());
        assert_eq!(
            failed.needs_run_now(),
            Some(true),
            "a failed run has not run, whoever asked for it"
        );
    }

    /// Every finished run reports exactly one `TaskExited` on the report
    /// channel — arrival order there IS the fold order, which is what let
    /// the run/done split (and its generation guard) be deleted.
    #[tokio::test]
    async fn a_finished_run_reports_exactly_once() {
        struct Case {
            name: &'static str,
            status: std::process::ExitStatus,
            want_success: bool,
            want_message: Option<&'static str>,
        }

        let cases = vec![
            Case {
                name: "scheduled success",
                status: ExitStatusExt::from_raw(0),
                want_success: true,
                want_message: None,
            },
            Case {
                name: "scheduled failure carries the exit code",
                status: ExitStatusExt::from_raw(3 << 8),
                want_success: false,
                want_message: Some("exit code 3"),
            },
            Case {
                name: "rerun success",
                status: ExitStatusExt::from_raw(0),
                want_success: true,
                want_message: None,
            },
            Case {
                name: "rerun failure",
                status: ExitStatusExt::from_raw(1 << 8),
                want_success: false,
                want_message: Some("exit code 1"),
            },
        ];

        for case in cases {
            let temp = tempfile::tempdir().unwrap();
            let (report_tx, mut report_rx) = mpsc::unbounded_channel();

            outcome("build", temp.path(), report_tx)
                .finish(Ok(case.status), Duration::from_millis(5), None, |_, _| {})
                .await;

            let Ok(super::super::ProcessReport::TaskExited(exit)) = report_rx.try_recv() else {
                panic!("{}: expected a TaskExited", case.name);
            };
            assert_eq!(exit.name, "build", "{}", case.name);
            assert_eq!(exit.success, case.want_success, "{}", case.name);
            assert_eq!(exit.message.as_deref(), case.want_message, "{}", case.name);
            assert!(
                report_rx.try_recv().is_err(),
                "{}: exactly one report per run",
                case.name
            );
        }
    }

    /// Only a successful run records its input hashes. Recording them on
    /// failure would let the next startup skip a task that never worked.
    #[tokio::test]
    async fn only_success_records_watched_inputs() {
        for (label, status, want_success) in [
            ("success", ExitStatusExt::from_raw(0), true),
            ("failure", ExitStatusExt::from_raw(1 << 8), false),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let (report_tx, _report_rx) = mpsc::unbounded_channel();
            outcome("build", temp.path(), report_tx)
                .finish(Ok(status), Duration::from_millis(1), None, |_, _| {})
                .await;

            let state = TaskStateStore::new(temp.path().join(".don").join("task-state"));
            assert_eq!(
                state.has_success("build").await.unwrap(),
                want_success,
                "{label}: has_success"
            );
            assert!(
                state.last_run("build").await.unwrap().is_some(),
                "{label}: every run records its outcome"
            );
        }
    }
}
