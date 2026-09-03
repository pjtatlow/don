//! Batching and debouncing for build-tool work.
//!
//! Rebuilds and build-graph re-queries both arrive one item at a time — the
//! watch manager fires per service after each item's own debounce — but they
//! must not *run* one at a time. A single edit under a Bazel workspace can
//! touch a dozen targets, and running `bazel build` a dozen times contends for
//! Bazel's server lock and takes an order of magnitude longer than one
//! invocation naming a dozen targets.
//!
//! The same is true of the *first* build. Every supervisor asks for its
//! artifact the moment it is constructed — dependencies gate running, not
//! building — so a workspace of thirty services fires thirty requests inside
//! a millisecond, and they must become one `bazel build` naming thirty
//! targets.
//!
//! [`BuildBatcher`] owns that coalescing: the queues, the batch windows, the
//! in-flight handles, and the mutex that serialises Bazel. It deliberately
//! does *not* decide what a build means — which items are eligible, what
//! target each maps to, and what happens when the build finishes belong to
//! the supervisor that owns the process. This owns *when* work runs and
//! *that only one batch of a kind runs at a time*.
//!
//! The batch windows are the reason [`queue_rebuild`](BuildBatcher::queue_rebuild)
//! and friends arm the deadline themselves rather than leaving it to callers.
//! Queueing without arming is a silently stuck rebuild, and that mistake was
//! previously available at seven separate call sites.

use super::AbortOnDrop;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;

/// How long to collect rebuild requests before running them as one build.
///
/// Short: the watch manager has already applied each item's own debounce, so
/// this window only has to be wide enough to catch the near-simultaneous
/// requests one edit fans out into.
const REBUILD_BATCH_WINDOW: Duration = Duration::from_millis(50);

/// How long to collect build-graph re-query requests before running them.
///
/// Wider than the rebuild window: a BUILD-file edit typically invalidates the
/// graph for many items at once, and a re-query is pure overhead if a second
/// one is about to follow.
const GRAPH_REQUERY_WINDOW: Duration = Duration::from_millis(100);

/// How long to collect artifact-preparation requests before running them.
///
/// Services ask as they are constructed, which is one synchronous burst, and
/// tasks ask as their runs are admitted — which for everything released
/// together is another one. This only has to be wide enough to span a burst.
/// Getting it wrong is the one regression batching exists to avoid: a window
/// that closed too early would give bazel one invocation per process.
const PREPARE_BATCH_WINDOW: Duration = Duration::from_millis(50);

/// Coalesces build-tool rebuilds and build-graph re-queries.
///
/// One per runner. See the module docs for what this does and does not own.
pub(crate) struct BuildBatcher {
    /// Serialises `bazel build` invocations. Concurrent builds contend for
    /// Bazel's server lock, so they queue here instead. Shared with the
    /// detached workers, which is why it is an `Arc`.
    bazel_mutex: Arc<Mutex<()>>,

    /// Items queued for the next rebuild batch, in arrival order.
    pending_rebuilds: Vec<String>,
    /// When the current rebuild window closes. `None` when nothing is queued.
    rebuild_deadline: Option<Instant>,
    /// The in-flight rebuild batch. At most one runs at a time — a second
    /// concurrent `bazel build` would just queue on the mutex anyway, and
    /// holding the batch lets the next one coalesce more work.
    rebuild_handle: Option<AbortOnDrop<()>>,

    /// Items queued for the next build-graph re-query batch.
    pending_requeries: Vec<String>,
    /// When the current re-query window closes.
    requery_deadline: Option<Instant>,
    /// The in-flight re-query batch.
    requery_handle: Option<AbortOnDrop<()>>,

    /// Items queued for the next artifact-preparation batch.
    pending_prepares: Vec<String>,
    /// When the current preparation window closes.
    prepare_deadline: Option<Instant>,
    /// The in-flight preparation batch.
    prepare_handle: Option<AbortOnDrop<()>>,
}

impl BuildBatcher {
    pub(crate) fn new() -> Self {
        Self {
            bazel_mutex: Arc::new(Mutex::new(())),
            pending_rebuilds: Vec::new(),
            rebuild_deadline: None,
            rebuild_handle: None,
            pending_requeries: Vec::new(),
            requery_deadline: None,
            requery_handle: None,
            pending_prepares: Vec::new(),
            prepare_deadline: None,
            prepare_handle: None,
        }
    }

    /// A handle on the Bazel serialisation mutex, for passing to a worker.
    pub(crate) fn bazel_mutex(&self) -> Arc<Mutex<()>> {
        Arc::clone(&self.bazel_mutex)
    }

    // -- rebuild batch ----------------------------------------------------

    /// Queue `name` for the next rebuild batch and (re)open the window.
    ///
    /// Idempotent per name: queueing an item that is already pending extends
    /// the window without duplicating the entry, which is what lets several
    /// services sharing one source file collapse into a single build.
    pub(crate) fn queue_rebuild(&mut self, name: impl Into<String>) {
        let name = name.into();
        if !self.pending_rebuilds.contains(&name) {
            self.pending_rebuilds.push(name);
        }
        self.rebuild_deadline = Some(Instant::now() + REBUILD_BATCH_WINDOW);
    }

    /// Queue several items in one go. No-op (and no window) when empty, so
    /// callers can pass a filtered list without guarding first.
    pub(crate) fn queue_rebuilds<I, S>(&mut self, names: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for name in names {
            self.queue_rebuild(name);
        }
    }

    /// Take everything queued and close the window.
    ///
    /// The caller owns the returned list: anything it cannot act on now must
    /// be handed back via [`queue_rebuilds`](Self::queue_rebuilds), which
    /// reopens the window.
    pub(crate) fn take_pending_rebuilds(&mut self) -> Vec<String> {
        self.rebuild_deadline = None;
        std::mem::take(&mut self.pending_rebuilds)
    }

    /// Drop `name` from the rebuild queue — it is being built by other means.
    pub(crate) fn cancel_pending_rebuild(&mut self, name: &str) {
        self.pending_rebuilds.retain(|queued| queued != name);
    }

    /// Whether a rebuild batch is currently running.
    pub(crate) fn rebuild_in_flight(&self) -> bool {
        self.rebuild_handle.is_some()
    }

    /// Adopt a spawned rebuild batch. Aborted on drop, so a runner that goes
    /// away mid-build takes the build with it.
    pub(crate) fn set_rebuild_batch(&mut self, handle: tokio::task::JoinHandle<()>) {
        self.rebuild_handle = Some(AbortOnDrop::new(handle));
    }

    /// Release the finished batch and reopen the window if work arrived while
    /// it was running.
    ///
    /// Dropping the handle matters: leaving it live would abort a task that
    /// has already returned — harmless, but it makes the abort meaningless as
    /// a signal.
    pub(crate) fn finish_rebuild_batch(&mut self) {
        self.rebuild_handle = None;
        if !self.pending_rebuilds.is_empty() {
            self.rebuild_deadline = Some(Instant::now() + REBUILD_BATCH_WINDOW);
        }
    }

    // -- graph re-query batch ---------------------------------------------

    /// Queue `name` for the next build-graph re-query and (re)open the window.
    pub(crate) fn queue_requery(&mut self, name: impl Into<String>) {
        let name = name.into();
        if !self.pending_requeries.contains(&name) {
            self.pending_requeries.push(name);
        }
        self.requery_deadline = Some(Instant::now() + GRAPH_REQUERY_WINDOW);
    }

    pub(crate) fn queue_requeries<I, S>(&mut self, names: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for name in names {
            self.queue_requery(name);
        }
    }

    /// Take everything queued and close the window. Same contract as
    /// [`take_pending_rebuilds`](Self::take_pending_rebuilds).
    pub(crate) fn take_pending_requeries(&mut self) -> Vec<String> {
        self.requery_deadline = None;
        std::mem::take(&mut self.pending_requeries)
    }

    pub(crate) fn requery_in_flight(&self) -> bool {
        self.requery_handle.is_some()
    }

    pub(crate) fn set_requery_batch(&mut self, handle: tokio::task::JoinHandle<()>) {
        self.requery_handle = Some(AbortOnDrop::new(handle));
    }

    pub(crate) fn finish_requery_batch(&mut self) {
        self.requery_handle = None;
        if !self.pending_requeries.is_empty() {
            self.requery_deadline = Some(Instant::now() + GRAPH_REQUERY_WINDOW);
        }
    }

    // -- artifact preparation batch ---------------------------------------

    /// Queue `name` for the next preparation batch and (re)open the window.
    pub(crate) fn queue_prepare(&mut self, name: impl Into<String>) {
        let name = name.into();
        if !self.pending_prepares.contains(&name) {
            self.pending_prepares.push(name);
        }
        self.prepare_deadline = Some(Instant::now() + PREPARE_BATCH_WINDOW);
    }

    /// Take everything queued and close the window. Same contract as
    /// [`take_pending_rebuilds`](Self::take_pending_rebuilds).
    pub(crate) fn take_pending_prepares(&mut self) -> Vec<String> {
        self.prepare_deadline = None;
        std::mem::take(&mut self.pending_prepares)
    }

    pub(crate) fn queue_prepares<I, S>(&mut self, names: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for name in names {
            self.queue_prepare(name);
        }
    }

    pub(crate) fn prepare_in_flight(&self) -> bool {
        self.prepare_handle.is_some()
    }

    pub(crate) fn set_prepare_batch(&mut self, handle: tokio::task::JoinHandle<()>) {
        self.prepare_handle = Some(AbortOnDrop::new(handle));
    }

    pub(crate) fn finish_prepare_batch(&mut self) {
        self.prepare_handle = None;
        if !self.pending_prepares.is_empty() {
            self.prepare_deadline = Some(Instant::now() + PREPARE_BATCH_WINDOW);
        }
    }

    // -- driving ----------------------------------------------------------

    /// Resolve once a batch window closes.
    ///
    /// Never resolves while both queues are empty, so this can sit in a
    /// `select!` arm unconditionally. The returned [`BatchDue`] says which
    /// queue to flush.
    pub(crate) async fn next_due(&self) -> BatchDue {
        let soonest = [
            (self.prepare_deadline, BatchDue::Prepares),
            (self.rebuild_deadline, BatchDue::Rebuilds),
            (self.requery_deadline, BatchDue::Requeries),
        ]
        .into_iter()
        .filter_map(|(deadline, kind)| deadline.map(|at| (at, kind)))
        .min_by_key(|(at, _)| *at);
        match soonest {
            Some((at, kind)) => {
                tokio::time::sleep_until(at).await;
                kind
            }
            None => std::future::pending().await,
        }
    }

    /// Abort every in-flight batch and wait for them to actually unwind.
    ///
    /// Awaiting matters as much as aborting. The workers hold
    /// [`LifecycleEmitter`] clones and the `Child` inside has
    /// `kill_on_drop(true)`, so dropping the aborted future is what SIGKILLs
    /// the bazel client — and `OutputManager::shutdown` blocks until
    /// every sink handle is gone. The 5s bound is shared across all three
    /// rather than paid per batch: it guards the pathological case of a stuck
    /// bazel pipe, and teardown's own budget is finite.
    ///
    /// [`LifecycleEmitter`]: crate::output::LifecycleEmitter
    pub(crate) async fn abort_in_flight(&mut self) {
        let handles: Vec<tokio::task::JoinHandle<()>> = [
            self.prepare_handle.take(),
            self.rebuild_handle.take(),
            self.requery_handle.take(),
        ]
        .into_iter()
        .flatten()
        .filter_map(AbortOnDrop::into_inner)
        .collect();
        for handle in &handles {
            handle.abort();
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        for handle in handles {
            let _ = tokio::time::timeout_at(deadline, handle).await;
        }
    }
}

/// Which queue's batch window has closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BatchDue {
    Prepares,
    Rebuilds,
    Requeries,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn queueing_dedupes_and_take_clears_the_window() {
        struct Case {
            name: &'static str,
            queue: Vec<&'static str>,
            want_taken: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                name: "nothing queued",
                queue: vec![],
                want_taken: vec![],
            },
            Case {
                name: "arrival order is preserved",
                queue: vec!["b", "a", "c"],
                want_taken: vec!["b", "a", "c"],
            },
            Case {
                // The whole point of the window: one edit fanning out to the
                // same service repeatedly is still one build.
                name: "repeats collapse",
                queue: vec!["a", "a", "b", "a"],
                want_taken: vec!["a", "b"],
            },
        ];

        for case in cases {
            let mut batcher = BuildBatcher::new();
            batcher.queue_rebuilds(case.queue.clone());
            assert_eq!(
                batcher.take_pending_rebuilds(),
                case.want_taken,
                "{}: taken",
                case.name
            );
            assert!(
                batcher.rebuild_deadline.is_none(),
                "{}: taking must close the window",
                case.name
            );
            // And the same for re-queries, which share the shape.
            batcher.queue_requeries(case.queue);
            assert_eq!(
                batcher.take_pending_requeries(),
                case.want_taken,
                "{}: taken requeries",
                case.name
            );
            assert!(
                batcher.requery_deadline.is_none(),
                "{}: taking must close the requery window",
                case.name
            );
        }
    }

    #[tokio::test]
    async fn an_empty_queue_never_opens_a_window() {
        let mut batcher = BuildBatcher::new();
        batcher.queue_rebuilds(Vec::<String>::new());
        batcher.queue_requeries(Vec::<String>::new());
        assert!(batcher.rebuild_deadline.is_none());
        assert!(batcher.requery_deadline.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn next_due_reports_the_earlier_window_and_waits_when_idle() {
        let mut batcher = BuildBatcher::new();
        // Idle: must not resolve, or the runner's select loop would spin.
        assert!(
            tokio::time::timeout(Duration::from_secs(5), batcher.next_due())
                .await
                .is_err(),
            "an idle batcher must never come due"
        );

        // A re-query queued first still loses to a rebuild queued later,
        // because the rebuild window is shorter.
        batcher.queue_requery("graph");
        batcher.queue_rebuild("api");
        assert_eq!(batcher.next_due().await, BatchDue::Rebuilds);
        batcher.take_pending_rebuilds();
        assert_eq!(batcher.next_due().await, BatchDue::Requeries);
    }

    #[tokio::test(start_paused = true)]
    async fn requeueing_reopens_the_window() {
        let mut batcher = BuildBatcher::new();
        batcher.queue_rebuild("api");
        let taken = batcher.take_pending_rebuilds();
        assert!(batcher.rebuild_deadline.is_none());

        // Deferred work handed back must come due again — dropping it here is
        // how a rebuild requested mid-build gets silently lost.
        batcher.queue_rebuilds(taken);
        assert_eq!(batcher.next_due().await, BatchDue::Rebuilds);
        assert_eq!(batcher.take_pending_rebuilds(), vec!["api".to_string()]);
    }

    #[tokio::test(start_paused = true)]
    async fn finishing_a_batch_reopens_the_window_only_when_work_is_waiting() {
        let mut batcher = BuildBatcher::new();
        batcher.set_rebuild_batch(tokio::spawn(async {}));
        assert!(batcher.rebuild_in_flight());

        batcher.finish_rebuild_batch();
        assert!(!batcher.rebuild_in_flight());
        assert!(
            batcher.rebuild_deadline.is_none(),
            "nothing queued — finishing must not arm a window"
        );

        batcher.set_rebuild_batch(tokio::spawn(async {}));
        batcher.queue_rebuild("api");
        batcher.take_pending_rebuilds();
        batcher.queue_rebuild("web");
        batcher.finish_rebuild_batch();
        assert_eq!(batcher.next_due().await, BatchDue::Rebuilds);
    }

    #[tokio::test]
    async fn cancelling_removes_only_the_named_item() {
        let mut batcher = BuildBatcher::new();
        batcher.queue_rebuilds(vec!["api", "web"]);
        batcher.cancel_pending_rebuild("api");
        assert_eq!(batcher.take_pending_rebuilds(), vec!["web".to_string()]);
    }

    #[tokio::test]
    async fn aborting_waits_for_in_flight_batches_to_unwind() {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let mut batcher = BuildBatcher::new();
        batcher.set_rebuild_batch(tokio::spawn(async move {
            // `tx` stands in for the emitter clones a real worker holds:
            // shutdown must not proceed until they are dropped.
            let _tx = tx;
            std::future::pending::<()>().await;
        }));
        batcher.set_requery_batch(tokio::spawn(std::future::pending()));
        batcher.set_prepare_batch(tokio::spawn(std::future::pending()));

        batcher.abort_in_flight().await;

        assert!(!batcher.rebuild_in_flight());
        assert!(!batcher.requery_in_flight());
        assert!(!batcher.prepare_in_flight());
        assert!(
            rx.await.is_err(),
            "abort_in_flight must await the task, not just fire an abort"
        );
    }
}
