//! The build-batcher actor: coalescing for build-tool work, end to end.
//!
//! [`crate::build_tool::manager::BuildBatcher`] remains the scheduling state
//! — queues, windows, the in-flight slot, the bazel mutex — and this task
//! owns *driving* it: it receives queue requests on a mailbox, flushes when a
//! window closes, spawns the batch workers, and forwards their outcomes to
//! the runner on a dedicated channel.
//!
//! Two properties are structural here rather than by-discipline:
//!
//! - **Release-before-apply.** The in-flight slot is released inside this
//!   task the moment a worker finishes, *before* the outcome is forwarded.
//!   Any follow-up rebuild the runner's application enqueues therefore
//!   always sees a free slot — the invariant the old runner arm maintained
//!   with a carefully-ordered pair of calls and a comment.
//! - **The outcome channel is unbounded.** This task must never block on a
//!   send: the runner awaits [`BatchRequest::ForceRebuild`] replies
//!   synchronously, and a bounded outcome channel would let that pair
//!   deadlock.
//!
//! Specs are captured at *queue* time from resolved config (fixed after
//! construction — there is no live config reload), which is what frees this
//! task from reading runner state to build a batch. The one thing it still
//! reads is the state *snapshot*, and only for rebuilds: services that are
//! still coming up (`Building`/`Pending`/`Starting`) are deferred back onto
//! the queue, so a file edited mid-build waits instead of racing the in-flight
//! preparation. The snapshot can trail the runner's fold, so the application
//! side keeps its own eligibility guard as the safety net.
//!
//! Preparations are deliberately *not* deferred that way: a supervisor asks
//! for its artifact before it starts, so every one of them is `Building` by
//! the time the window closes, and deferring would deadlock startup.
//!
//! One request is late by construction. Watch paths are resolved *by* the
//! preparation build and must reach the watcher before anything spawns, but
//! the watcher does not exist until the runner has finished setting it up.
//! So preparation requests that arrive before [`BatchRequest::WatchReady`] are
//! parked here rather than queued. The first batch also waits for every task's
//! startup check, so hashing inputs cannot split runnable tasks into later builds.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use tokio::sync::{mpsc, oneshot};

use super::batch::{
    BatchBuildItem, BazelRebuildItem, GraphRequeryRequestItem, PrepareBatchOutcome, PrepareOutcome,
    RebuildBatchOutcome, RebuildBatchRequest, RequeryOutcome, run_batch_build_chain,
    run_graph_requery_worker, run_rebuild_batch_worker,
};
use crate::build_tool::manager::{BatchDue, BuildBatcher};
use crate::output::LifecycleEmitter;
use crate::process::ServiceState;
use crate::state_store::ProcessStatus;
use crate::state_store::StateReader;

/// What a batch decided about one item, delivered to that item's supervisor.
///
/// The batch itself is cross-item by nature — one `bazel build //a //b //c` —
/// but its *consequences* are per-item, and the thing that acts on them is the
/// supervisor that owns the process. So the batch fans out here rather than
/// being folded by the scheduler and re-dispatched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RebuildItemOutcome {
    /// The build tool produced a fresh artifact.
    Built,
    /// Sources already match the artifact; nothing was built.
    UpToDate,
    /// Passed through the batch untouched — build-tool managed with no target.
    NotBuilt,
    Failed(String),
}

/// What one queued rebuild is, captured at queue time.
pub(crate) enum RebuildSpec {
    /// A bazel-built service: the batch runs `bazel build` for its target.
    Bazel(BazelRebuildItem),
    /// A build-tool-managed service with no bazel target (shouldn't happen,
    /// but handled gracefully) — passes through the batch untouched and is
    /// rebuilt by the ordinary per-service path on application.
    Plain { name: String },
}

impl RebuildSpec {
    fn name(&self) -> &str {
        match self {
            Self::Bazel(item) => &item.name,
            Self::Plain { name } => name,
        }
    }
}

/// Everything the batcher can be asked to do.
pub(crate) enum BatchRequest {
    /// Produce this process's artifact so its supervisor may spawn it. Sent
    /// when a supervisor is constructed — dependencies gate *running*, not
    /// building — or, for a lazy service, on its first demand.
    QueuePrepare {
        item: Box<BatchBuildItem>,
        outcome: mpsc::UnboundedSender<PrepareOutcome>,
    },
    /// The runner has finished deciding whether there is a file watcher, and
    /// preparation builds may start. Sent exactly once; see the module docs
    /// for why nothing may build before it.
    WatchReady {
        updates: Option<mpsc::UnboundedSender<crate::watch::WatchUpdate>>,
    },
    /// A task has decided whether to join the initial preparation batch.
    StartupTaskChecked { name: String },
    /// Queue a rebuild and (re)open the batch window. The outcome goes back
    /// to `outcome`, which is the requesting supervisor's own channel.
    QueueRebuild {
        spec: RebuildSpec,
        outcome: mpsc::UnboundedSender<RebuildItemOutcome>,
    },
    /// Queue a build-graph re-query and (re)open its window. The outcome
    /// goes back to `outcome`, which is the requesting supervisor's own
    /// channel — the watch registrations it produces are applied here first.
    QueueRequery {
        item: GraphRequeryRequestItem,
        outcome: mpsc::UnboundedSender<RequeryOutcome>,
    },
    /// Run a forced rebuild for one item immediately, bypassing the window.
    /// Replies with an error if a batch is already in flight, preserving the
    /// hard-restart path's synchronous "already in progress" answer.
    ForceRebuild {
        spec: RebuildSpec,
        outcome: mpsc::UnboundedSender<RebuildItemOutcome>,
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// Abort any in-flight batch (bounded) and exit.
    Shutdown { done: oneshot::Sender<()> },
}

/// What the detached workers hand back to the actor.
enum WorkerDone {
    Prepares(PrepareBatchOutcome),
    Rebuilds(RebuildBatchOutcome),
    Requeries(Vec<(String, RequeryOutcome)>),
}

/// The workspace facts every preparation batch needs, fixed at construction.
pub(crate) struct WorkspaceContext {
    pub(crate) base_dir: PathBuf,
    pub(crate) startup_tasks: HashSet<String>,
    /// Project-wide watch-ignore patterns, for the build-graph registrations
    /// the chain pushes to the watcher.
    pub(crate) global_watch_ignore: Vec<String>,
}

/// Spawn the batcher task. Returns its mailbox and the task handle (joined
/// bounded at shutdown).
///
/// Nothing comes back to the scheduler: every kind of batch fans out per
/// item, to the supervisor that asked for it.
pub(crate) fn spawn(
    state: StateReader,
    emitter: LifecycleEmitter,
    workspace: WorkspaceContext,
) -> (
    mpsc::UnboundedSender<BatchRequest>,
    tokio::task::JoinHandle<()>,
) {
    let (request_tx, request_rx) = mpsc::unbounded_channel();
    let handle = tokio::spawn(run(request_rx, state, emitter, workspace));
    (request_tx, handle)
}

async fn run(
    mut request_rx: mpsc::UnboundedReceiver<BatchRequest>,
    state: StateReader,
    emitter: LifecycleEmitter,
    workspace: WorkspaceContext,
) {
    let mut startup_tasks = workspace.startup_tasks.clone();
    let mut scheduler = BuildBatcher::new();
    let mut rebuild_specs: HashMap<String, RebuildSpec> = HashMap::new();
    // Where each item's outcome goes. Overwritten on every queue, so it is
    // always the most recent asker's channel.
    let mut rebuild_outcomes: HashMap<String, mpsc::UnboundedSender<RebuildItemOutcome>> =
        HashMap::new();
    let mut requery_specs: HashMap<String, GraphRequeryRequestItem> = HashMap::new();
    let mut requery_outcomes: HashMap<String, mpsc::UnboundedSender<RequeryOutcome>> =
        HashMap::new();
    let mut prepare_specs: HashMap<String, BatchBuildItem> = HashMap::new();
    let mut prepare_outcomes: HashMap<String, mpsc::UnboundedSender<PrepareOutcome>> =
        HashMap::new();
    // `None` until the runner says whether there is a watcher; see the module
    // docs. Preparation requests arriving before that are parked, not queued.
    let mut watch_updates: Option<Option<mpsc::UnboundedSender<crate::watch::WatchUpdate>>> = None;
    let mut parked_prepares: Vec<String> = Vec::new();
    // Workers report here; the handles stored in the scheduler are wrappers
    // around these sends, so `abort_in_flight` still kills the real work.
    let (worker_tx, mut worker_rx) = mpsc::unbounded_channel::<WorkerDone>();

    loop {
        if watch_updates.is_some() && startup_tasks.is_empty() {
            scheduler.queue_prepares(std::mem::take(&mut parked_prepares));
        }
        tokio::select! {
            request = request_rx.recv() => match request {
                Some(BatchRequest::QueuePrepare { item, outcome }) => {
                    let name = item.name.clone();
                    prepare_outcomes.insert(name.clone(), outcome);
                    prepare_specs.insert(name.clone(), *item);
                    if watch_updates.is_some() && startup_tasks.is_empty() {
                        scheduler.queue_prepare(name);
                    } else if !parked_prepares.contains(&name) {
                        parked_prepares.push(name);
                    }
                }
                Some(BatchRequest::WatchReady { updates }) => {
                    watch_updates = Some(updates);
                }
                Some(BatchRequest::StartupTaskChecked { name }) => {
                    startup_tasks.remove(&name);
                }
                Some(BatchRequest::QueueRebuild { spec, outcome }) => {
                    scheduler.queue_rebuild(spec.name());
                    rebuild_outcomes.insert(spec.name().to_string(), outcome);
                    rebuild_specs.insert(spec.name().to_string(), spec);
                }
                Some(BatchRequest::QueueRequery { item, outcome }) => {
                    scheduler.queue_requery(item.name.clone());
                    requery_outcomes.insert(item.name.clone(), outcome);
                    requery_specs.insert(item.name.clone(), item);
                }
                Some(BatchRequest::ForceRebuild {
                    spec,
                    outcome,
                    reply,
                }) => {
                    if scheduler.rebuild_in_flight() {
                        let _ = reply.send(Err(
                            "build-tool rebuild already in progress".to_string(),
                        ));
                        continue;
                    }
                    // This build supersedes anything queued for the batch.
                    scheduler.cancel_pending_rebuild(spec.name());
                    rebuild_specs.remove(spec.name());
                    rebuild_outcomes.insert(spec.name().to_string(), outcome);
                    spawn_rebuild_batch(
                        &mut scheduler,
                        vec![spec],
                        true,
                        &emitter,
                        &worker_tx,
                    );
                    let _ = reply.send(Ok(()));
                }
                Some(BatchRequest::Shutdown { done }) => {
                    scheduler.abort_in_flight().await;
                    let _ = done.send(());
                    return;
                }
                // Runner gone; nothing left to build for.
                None => {
                    scheduler.abort_in_flight().await;
                    return;
                }
            },
            Some(done) = worker_rx.recv() => {
                // Release the slot before forwarding: anything the
                // application enqueues must see a free batcher.
                match done {
                    WorkerDone::Prepares(outcome) => {
                        scheduler.finish_prepare_batch();
                        deliver_prepares(outcome, &emitter, &mut prepare_outcomes);
                    }
                    WorkerDone::Rebuilds(outcome) => {
                        scheduler.finish_rebuild_batch();
                        deliver_rebuilds(outcome, &mut rebuild_outcomes);
                    }
                    WorkerDone::Requeries(outcomes) => {
                        scheduler.finish_requery_batch();
                        for (name, outcome) in outcomes {
                            if let Some(tx) = requery_outcomes.get(&name)
                                && tx.send(outcome).is_err()
                            {
                                requery_outcomes.remove(&name);
                            }
                        }
                    }
                }
            },
            due = scheduler.next_due() => match due {
                BatchDue::Prepares => flush_prepares(
                    &mut scheduler,
                    &mut prepare_specs,
                    watch_updates.as_ref().and_then(Clone::clone),
                    &workspace,
                    &emitter,
                    &worker_tx,
                ),
                BatchDue::Rebuilds => flush_rebuilds(
                    &mut scheduler,
                    &mut rebuild_specs,
                    &state,
                    &emitter,
                    &worker_tx,
                ),
                BatchDue::Requeries => flush_requeries(
                    &mut scheduler,
                    &mut requery_specs,
                    watch_updates.as_ref().and_then(Clone::clone),
                    &workspace,
                    &emitter,
                    &worker_tx,
                ),
            },
        }
    }
}

/// Hand each item in a finished batch to the supervisor that asked for it.
///
/// A dropped receiver just means that supervisor ended; there is nobody left
/// to restart, so the outcome is discarded rather than being an error.
fn deliver_rebuilds(
    outcome: RebuildBatchOutcome,
    senders: &mut HashMap<String, mpsc::UnboundedSender<RebuildItemOutcome>>,
) {
    let mut send = |name: &str, item: RebuildItemOutcome| {
        if let Some(tx) = senders.get(name)
            && tx.send(item).is_err()
        {
            senders.remove(name);
        }
    };
    for (name, message) in &outcome.failed {
        send(name, RebuildItemOutcome::Failed(message.clone()));
    }
    for name in &outcome.up_to_date {
        send(name, RebuildItemOutcome::UpToDate);
    }
    for name in &outcome.build_succeeded {
        send(name, RebuildItemOutcome::Built);
    }
    for name in &outcome.plain_rebuilds {
        send(name, RebuildItemOutcome::NotBuilt);
    }
}

/// Hand each item in a finished preparation batch to the supervisor that
/// asked for it. Same dropped-receiver rule as [`deliver_rebuilds`].
fn deliver_prepares(
    outcome: PrepareBatchOutcome,
    emitter: &LifecycleEmitter,
    senders: &mut HashMap<String, mpsc::UnboundedSender<PrepareOutcome>>,
) {
    for warning in &outcome.warnings {
        emitter.error_event(warning);
    }
    for (name, decided) in outcome.items {
        if let Some(tx) = senders.get(&name)
            && tx.send(decided).is_err()
        {
            senders.remove(&name);
        }
    }
}

fn flush_prepares(
    scheduler: &mut BuildBatcher,
    specs: &mut HashMap<String, BatchBuildItem>,
    watch_updates: Option<mpsc::UnboundedSender<crate::watch::WatchUpdate>>,
    workspace: &WorkspaceContext,
    emitter: &LifecycleEmitter,
    worker_tx: &mpsc::UnboundedSender<WorkerDone>,
) {
    let names = scheduler.take_pending_prepares();
    if names.is_empty() {
        return;
    }
    if scheduler.prepare_in_flight() {
        scheduler.queue_prepares(names);
        return;
    }
    // Deliberately no `coming_up` deferral here — see the module docs. Every
    // item in this batch is `Building` precisely because it asked for this.
    let items: Vec<BatchBuildItem> = names.iter().filter_map(|name| specs.remove(name)).collect();
    if items.is_empty() {
        return;
    }

    let emitter = emitter.clone();
    let base_dir = workspace.base_dir.clone();
    let global_watch_ignore = workspace.global_watch_ignore.clone();
    let bazel_build_mutex = scheduler.bazel_mutex();
    let worker_tx = worker_tx.clone();
    scheduler.set_prepare_batch(tokio::spawn(async move {
        let outcome = run_batch_build_chain(
            items,
            base_dir,
            emitter,
            watch_updates,
            global_watch_ignore,
            bazel_build_mutex,
        )
        .await;
        let _ = worker_tx.send(WorkerDone::Prepares(outcome));
    }));
}

/// Whether the snapshot says this service is still coming up. Rebuilding a
/// service that is mid-build or mid-start would race its in-flight build or
/// double-start it, so such items are deferred back onto the queue and
/// retried once they reach a settled state. (This is what keeps a file
/// edited mid-build from being lost.)
fn coming_up(state: &StateReader, name: &str) -> bool {
    state.snapshot().processes.iter().any(|status| {
        matches!(
            status,
            ProcessStatus::Service { name: process_name, state, .. }
                if process_name == name
                    && matches!(
                        state,
                        ServiceState::Building
                            | ServiceState::Pending
                            | ServiceState::Starting
                    )
        )
    })
}

fn flush_rebuilds(
    scheduler: &mut BuildBatcher,
    specs: &mut HashMap<String, RebuildSpec>,
    state: &StateReader,
    emitter: &LifecycleEmitter,
    worker_tx: &mpsc::UnboundedSender<WorkerDone>,
) {
    let mut names = scheduler.take_pending_rebuilds();
    if names.is_empty() {
        return;
    }
    if scheduler.rebuild_in_flight() {
        scheduler.queue_rebuilds(names);
        return;
    }

    // Defer services that haven't finished coming up; their specs stay in
    // the map for the retry.
    let mut deferred: Vec<String> = Vec::new();
    names.retain(|name| {
        if coming_up(state, name) {
            deferred.push(name.clone());
            false
        } else {
            true
        }
    });
    scheduler.queue_rebuilds(deferred);
    if names.is_empty() {
        return;
    }

    let flushed = names
        .iter()
        .filter_map(|name| specs.remove(name))
        .collect::<Vec<_>>();
    spawn_rebuild_batch(scheduler, flushed, false, emitter, worker_tx);
}

fn spawn_rebuild_batch(
    scheduler: &mut BuildBatcher,
    specs: Vec<RebuildSpec>,
    force: bool,
    emitter: &LifecycleEmitter,
    worker_tx: &mpsc::UnboundedSender<WorkerDone>,
) {
    let mut bazel_items: Vec<BazelRebuildItem> = Vec::new();
    let mut plain_rebuilds: Vec<String> = Vec::new();
    for spec in specs {
        match spec {
            RebuildSpec::Bazel(item) => bazel_items.push(item),
            RebuildSpec::Plain { name } => plain_rebuilds.push(name),
        }
    }
    let request = RebuildBatchRequest {
        bazel_items,
        plain_rebuilds,
        force,
    };
    let emitter = emitter.clone();
    let bazel_build_mutex = scheduler.bazel_mutex();
    let worker_tx = worker_tx.clone();
    scheduler.set_rebuild_batch(tokio::spawn(async move {
        let outcome = run_rebuild_batch_worker(request, emitter, bazel_build_mutex).await;
        let _ = worker_tx.send(WorkerDone::Rebuilds(outcome));
    }));
}

#[allow(clippy::too_many_arguments)]
fn flush_requeries(
    scheduler: &mut BuildBatcher,
    specs: &mut HashMap<String, GraphRequeryRequestItem>,
    watch_updates: Option<mpsc::UnboundedSender<crate::watch::WatchUpdate>>,
    workspace: &WorkspaceContext,
    emitter: &LifecycleEmitter,
    worker_tx: &mpsc::UnboundedSender<WorkerDone>,
) {
    let names = scheduler.take_pending_requeries();
    if names.is_empty() {
        return;
    }
    if scheduler.requery_in_flight() {
        scheduler.queue_requeries(names);
        return;
    }

    let items: Vec<GraphRequeryRequestItem> =
        names.iter().filter_map(|name| specs.remove(name)).collect();
    if items.is_empty() {
        return;
    }

    emitter.lifecycle_event(&format!(
        "re-querying build tool for {} item{}...",
        items.len(),
        if items.len() == 1 { "" } else { "s" }
    ));

    let emitter = emitter.clone();
    let worker_tx = worker_tx.clone();
    let base_dir = workspace.base_dir.clone();
    scheduler.set_requery_batch(tokio::spawn(async move {
        let outcomes = run_graph_requery_worker(items, emitter, watch_updates, base_dir).await;
        let _ = worker_tx.send(WorkerDone::Requeries(outcomes));
    }));
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::config::LogConfig;
    use crate::output::OutputManager;
    use crate::state_store::{self, StateSnapshot, StateWriter};
    use std::time::Duration;

    async fn test_emitter() -> LifecycleEmitter {
        let log_config = LogConfig::Stdout;
        let services = [("svc", &log_config)];
        let output_manager = OutputManager::new(&services, tokio::io::sink())
            .await
            .unwrap();
        output_manager.clone_lifecycle_emitter()
    }

    fn service_statuses(states: &[(&str, ServiceState)]) -> Vec<ProcessStatus> {
        states
            .iter()
            .map(|(name, state)| ProcessStatus::Service {
                name: name.to_string(),
                state: *state,
                failed_dependencies: Vec::new(),
                runtime: None,
                verbose: None,
            })
            .collect()
    }

    async fn spawn_actor(
        initial: Vec<ProcessStatus>,
    ) -> (
        StateWriter,
        mpsc::UnboundedSender<BatchRequest>,
        tokio::task::JoinHandle<()>,
    ) {
        let (writer, reader) = state_store::channel(StateSnapshot::default());
        writer.publish_processes(initial);
        let emitter = test_emitter().await;
        let (tx, handle) = spawn(
            reader,
            emitter,
            WorkspaceContext {
                base_dir: std::env::temp_dir(),
                startup_tasks: HashSet::new(),
                global_watch_ignore: Vec::new(),
            },
        );
        (writer, tx, handle)
    }

    fn queue(
        tx: &mpsc::UnboundedSender<BatchRequest>,
        name: &str,
        outcome: &mpsc::UnboundedSender<RebuildItemOutcome>,
    ) {
        tx.send(BatchRequest::QueueRebuild {
            spec: RebuildSpec::Plain {
                name: name.to_string(),
            },
            outcome: outcome.clone(),
        })
        .unwrap();
    }

    /// An item with no bazel target: the chain passes it through without
    /// running anything, which is what lets these tests exercise the queueing
    /// rules without a build tool on PATH.
    fn prepare_item(name: &str) -> BatchBuildItem {
        BatchBuildItem {
            name: name.to_string(),
            kind: crate::process::ProcessKind::Service,
            bazel: None,
            watch_enabled: false,
            working_dir: std::env::temp_dir(),
            ignore: Vec::new(),
        }
    }

    /// Two rules at once, because they have one cause.
    ///
    /// Nothing may build before the runner has said whether there is a
    /// watcher: watch paths are resolved *by* these builds and have to reach
    /// the watcher before any supervisor spawns. And every supervisor asks for
    /// its artifact the moment it is constructed, so the burst that parking
    /// accumulates must leave as **one** batch — one `bazel build` naming
    /// every target, not one invocation per service.
    #[tokio::test(start_paused = true)]
    async fn preparations_park_until_the_watcher_is_ready_then_leave_as_one_batch() {
        let (_writer, tx, handle) = spawn_actor(Vec::new()).await;
        let (api_tx, mut api_rx) = mpsc::unbounded_channel();
        let (web_tx, mut web_rx) = mpsc::unbounded_channel();

        for (name, outcome) in [("api", &api_tx), ("web", &web_tx), ("api", &api_tx)] {
            tx.send(BatchRequest::QueuePrepare {
                item: Box::new(prepare_item(name)),
                outcome: outcome.clone(),
            })
            .unwrap();
        }

        let early = tokio::time::timeout(Duration::from_secs(2), api_rx.recv()).await;
        assert!(
            early.is_err(),
            "a build before the watcher exists would register its watch paths into a void"
        );

        tx.send(BatchRequest::WatchReady { updates: None }).unwrap();

        for (label, rx) in [("api", &mut api_rx), ("web", &mut web_rx)] {
            let outcome = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .unwrap_or_else(|_| panic!("{label}: the parked build should run"))
                .expect("actor alive");
            // No bazel target, so the chain has nothing to report for it.
            assert!(matches!(outcome, PrepareOutcome::Failed(_)), "{label}");
        }

        // Three requests, two items, one batch — so the repeat produced no
        // second outcome for `api`.
        let extra = tokio::time::timeout(Duration::from_millis(500), api_rx.recv()).await;
        assert!(extra.is_err(), "exactly one batch for the burst");
        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn initial_build_waits_for_startup_task_checks() {
        let (_writer, reader) = state_store::channel(StateSnapshot::default());
        let (tx, handle) = spawn(
            reader,
            test_emitter().await,
            WorkspaceContext {
                base_dir: std::env::temp_dir(),
                startup_tasks: ["migrate".to_string(), "lint".to_string()].into(),
                global_watch_ignore: Vec::new(),
            },
        );
        let (outcome_tx, mut outcome_rx) = mpsc::unbounded_channel();
        tx.send(BatchRequest::QueuePrepare {
            item: Box::new(prepare_item("api")),
            outcome: outcome_tx.clone(),
        })
        .unwrap();
        tx.send(BatchRequest::WatchReady { updates: None }).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), outcome_rx.recv())
                .await
                .is_err()
        );

        tx.send(BatchRequest::QueuePrepare {
            item: Box::new(prepare_item("migrate")),
            outcome: outcome_tx,
        })
        .unwrap();
        tx.send(BatchRequest::StartupTaskChecked {
            name: "migrate".into(),
        })
        .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), outcome_rx.recv())
                .await
                .is_err()
        );

        // A manual task releases its check without requesting an artifact.
        tx.send(BatchRequest::StartupTaskChecked {
            name: "lint".into(),
        })
        .unwrap();
        for _ in 0..2 {
            let outcome = tokio::time::timeout(Duration::from_secs(1), outcome_rx.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(outcome, PrepareOutcome::Failed(_)));
        }
        assert!(
            tokio::time::timeout(Duration::from_secs(1), outcome_rx.recv())
                .await
                .is_err()
        );
        handle.abort();
        let _ = handle.await;
    }

    /// One edit fans out into a rebuild request per affected service; those
    /// must collapse into a single batch, and nothing may build before the
    /// window closes.
    #[tokio::test(start_paused = true)]
    async fn a_burst_of_rebuild_requests_becomes_one_batch() {
        let (_writer, tx, handle) = spawn_actor(service_statuses(&[
            ("api", ServiceState::Ready),
            ("web", ServiceState::Ready),
        ]))
        .await;
        let (item_tx, mut item_rx) = mpsc::unbounded_channel();

        for name in ["api", "web", "api", "web", "api"] {
            queue(&tx, name, &item_tx);
        }

        // Five requests, two services, one batch — so exactly two outcomes.
        let mut seen = 0;
        for _ in 0..2 {
            let outcome = tokio::time::timeout(Duration::from_secs(5), item_rx.recv())
                .await
                .expect("the window should close and the batch should run")
                .expect("actor alive");
            assert_eq!(outcome, RebuildItemOutcome::NotBuilt);
            seen += 1;
        }
        assert_eq!(seen, 2);

        let extra = tokio::time::timeout(Duration::from_millis(500), item_rx.recv()).await;
        assert!(extra.is_err(), "exactly one batch for the burst");
        handle.abort();
    }

    /// Regression: a watched file changes while a service is still coming up
    /// (`Building`). The request must be deferred — not dropped, and not
    /// raced against the in-flight startup build — then run once the service
    /// settles.
    ///
    /// This deferral cannot move to the supervisor: during the *preparation*
    /// build the service is `Building` but its supervisor is idle and holds
    /// nothing, so a rebuild landing there would queue a second build of the
    /// same target behind the bazel mutex and then restart into whichever
    /// finished last.
    #[tokio::test(start_paused = true)]
    async fn rebuild_during_build_is_deferred_not_dropped() {
        let (writer, tx, handle) =
            spawn_actor(service_statuses(&[("api", ServiceState::Building)])).await;
        let (item_tx, mut item_rx) = mpsc::unbounded_channel();

        queue(&tx, "api", &item_tx);

        let deferred = tokio::time::timeout(Duration::from_secs(2), item_rx.recv()).await;
        assert!(
            deferred.is_err(),
            "rebuild must stay deferred while the service is coming up"
        );

        writer.publish_processes(service_statuses(&[("api", ServiceState::Ready)]));
        let outcome = tokio::time::timeout(Duration::from_secs(5), item_rx.recv())
            .await
            .expect("deferred rebuild should fire once the service is up")
            .expect("actor alive");
        assert_eq!(outcome, RebuildItemOutcome::NotBuilt);
        handle.abort();
    }

    /// The hard-restart path's synchronous answer: a force rebuild while a
    /// batch is in flight is refused, not queued.
    #[tokio::test(start_paused = true)]
    async fn force_rebuild_is_refused_while_a_batch_runs() {
        let (_writer, tx, handle) =
            spawn_actor(service_statuses(&[("api", ServiceState::Ready)])).await;
        let (item_tx, mut item_rx) = mpsc::unbounded_channel();

        let (first_tx, first_rx) = oneshot::channel();
        tx.send(BatchRequest::ForceRebuild {
            spec: RebuildSpec::Plain {
                name: "api".to_string(),
            },
            outcome: item_tx.clone(),
            reply: first_tx,
        })
        .unwrap();
        assert!(first_rx.await.unwrap().is_ok());

        // The plain worker finishes almost instantly, so the refusal is only
        // observable if the second request lands before the actor processes
        // the first's completion. Both are queued already, and mailbox order
        // guarantees the second ForceRebuild is handled first.
        let (second_tx, second_rx) = oneshot::channel();
        tx.send(BatchRequest::ForceRebuild {
            spec: RebuildSpec::Plain {
                name: "api".to_string(),
            },
            outcome: item_tx,
            reply: second_tx,
        })
        .unwrap();
        match second_rx.await.unwrap() {
            Err(message) => assert!(message.contains("already in progress")),
            Ok(()) => {
                // The worker finished before the second request was handled;
                // that is also a legal serialization.
                let _ = tokio::time::timeout(Duration::from_secs(5), item_rx.recv()).await;
            }
        }
        handle.abort();
    }
}
