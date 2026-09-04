//! Per-service start supervision.
//!
//! The mirror of [`super::task_supervisor`] for the other process kind, and it
//! exists for the same reason: preparing a start is slow (downloads, builds,
//! docker pulls, port allocation) so it has always been detached, and
//! detaching onto a shared completion channel is what forced the runner to
//! ask "is this still the current start?" when the answer landed —
//! `start_generation`, compared on arrival, plus a branch to stop whatever
//! the losing attempt had already brought up.
//!
//! One supervisor per service removes the question. It is the only thing that
//! reports a prepared start for its service, and only for the start it is
//! committed to.
//!
//! The supervisor owns its service's process from wire to reap — prepare,
//! spawn, output reader, OSC sink, crash detection, stop-with-drain — and
//! its proxy, whose listeners span process generations. The runner keeps
//! what is genuinely cross-process: scheduling, state folds, ready resolution
//! and completion (which cross channels — see the plan's ordering note),
//! restart policy, and shutdown sequencing. Proxy *decisions* likewise stay
//! with the runner and arrive as [`ProxyDirective`]s.

use super::ServiceStartIntent;
use super::service;
use super::service_worker::ServiceStartContext;
use super::service_worker::{ServiceStartMode, start_service_worker};
use crate::config::ShutdownConfig;
use crate::output::{LifecycleEmitter, ProcessOutput};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;

/// One request to start a service, as handed to its supervisor.
pub(crate) struct StartRequest {
    pub(crate) mode: ServiceStartMode,
    pub(crate) intent: ServiceStartIntent,
    /// Allocate new ephemeral backend ports before spawning. Set on the
    /// restart path so the new process binds a fresh port while draining
    /// connections to the old one finish undisturbed.
    pub(crate) fresh_backend_ports: bool,
}

/// One request to stop and immediately start again, as one operation.
///
/// Restart is a single command rather than a stop the scheduler follows up
/// on, because every step of it belongs to the owner: the proxy whose backend
/// must be cleared first, the process being ended, and the spawn that
/// replaces it. As one mailbox item it also cannot interleave with anything
/// else this service was asked to do.
pub(crate) struct RestartRequest {
    pub(crate) wait_full_exit: bool,
    /// See [`StopRequest::interrupt`].
    pub(crate) interrupt: Option<tokio::sync::watch::Receiver<bool>>,
    /// Clear forwarding backends before stopping, so connections arriving
    /// mid-restart queue instead of racing the dying process.
    pub(crate) clear_backend_first: bool,
    pub(crate) start_mode: ServiceStartMode,
    /// See [`StartRequest::fresh_backend_ports`].
    pub(crate) fresh_backend_ports: bool,
    pub(crate) intent: ServiceStartIntent,
    /// Answered by the fold when the *stop* half lands; the start that
    /// follows reports its own progress.
    pub(crate) reply: Option<tokio::sync::oneshot::Sender<crate::command::CommandResult>>,
    /// See [`super::ProcessReport::ServiceStarting::restarting`].
    pub(crate) announce_restarting: bool,
    /// Clear the failure history first. An explicitly requested restart is a
    /// fresh chance; the restart policy's *own* retry must not be, or the
    /// streak that bounds a crash loop would be wiped by every attempt it
    /// scheduled — and the loop would never end.
    pub(crate) reset_policy: bool,
}

/// One request to rebuild: produce a fresh artifact, then restart into it.
/// Why a rebuild was asked for, which decides what happens if one is already
/// running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RebuildSource {
    /// Watched files changed. If a cycle is already running, its artifact is
    /// already out of date: record that and let it start another when it
    /// finishes, rather than racing a second build against the first.
    ///
    /// The file watcher used to hold this back itself, sending a distinct
    /// "mark stale" signal instead of a second rebuild — which is why it had
    /// to know when a cycle ended, and needed a completion channel to find
    /// out.
    FileChange,
    /// Somebody asked for this rebuild by name. It supersedes.
    Requested,
}

pub(crate) struct RebuildRequest {
    /// Skip the build tool's up-to-date check — the hard-restart path.
    pub(crate) forced: bool,
    /// See [`RebuildSource`].
    pub(crate) source: RebuildSource,
    /// Answered as soon as the build is *accepted*, not when it finishes.
    /// A forced rebuild refused because a batch is already running is the
    /// hard-restart path's synchronous "already in progress".
    pub(crate) reply: Option<tokio::sync::oneshot::Sender<crate::command::CommandResult>>,
}

/// Everything a service's supervisor can be asked to do.
pub(crate) enum ServiceCommand {
    /// Begin a start — or supersede the one being prepared.
    Start(StartRequest),
    /// End the held process and start a fresh one. See [`RestartRequest`].
    Restart(Box<RestartRequest>),
    /// Build this service's artifact and restart into it. See
    /// [`RebuildRequest`] and the cycle it drives in `supervise`.
    Rebuild(RebuildRequest),
    /// This service's build-graph definition files changed (a BUILD file, a
    /// `package.json`, …), so the watch patterns resolved from them may no
    /// longer be right.
    ///
    /// The supervisor asks the build manager to re-query — the same way it
    /// asks for any other build — and decides what to do with the answer.
    BuildGraphChanged,
    /// End the held process: graceful signal per the config, bounded wait,
    /// SIGKILL fallback — the body of the old runner-side stop worker.
    /// The reply waits for the output reader to drain, so "stopped" can
    /// never outrun the process's last lines.
    Stop(StopRequest),
}

/// A service's bound proxy and its lazy-demand channel, handed to the
/// supervisor at spawn. Bound by the runner during construction so port
/// conflicts still fail startup before anything spawns.
pub(crate) struct ProxyAssets {
    pub(crate) proxy: crate::proxy::ServiceProxy,
    /// The receiving half of the proxy's lazy trigger channel — `Some` only
    /// for lazy services. The supervisor forwards each trigger as
    /// [`super::ProcessReport::Demand`].
    pub(crate) demand_rx: Option<mpsc::Receiver<String>>,
}

/// What the runner receives for a spawned, wired start.
///
/// The supervisor keeps the process handle and the output reader; this is
/// everything the runner's bookkeeping and ready-check paths need —
/// extracted once, at wire time, by the owner.
pub(crate) struct ServiceWired {
    pub(crate) identity: super::state::ServiceHandleIdentity,
    pub(crate) pgid: Option<i32>,
    pub(crate) docker_port_bindings: Vec<crate::docker::DockerPortBinding>,
    /// The proxy's env-mode backend vars this spawn was launched with —
    /// `Some` iff the service has a proxy. The runner refreshes its
    /// `ProxyView` shadow from this, so ready checks written against
    /// `${PORT}` resolve to the port the new process was actually told to
    /// bind. Wiring precedes ready resolution, so the shadow is always
    /// current where it is read.
    pub(crate) proxy_backend_env: Option<std::collections::HashMap<String, String>>,
}

/// Parameters for [`ServiceCommand::Stop`].
///
/// No shutdown config: how long this service's grace period is, and which
/// signal starts it, is merged from its own config over the workspace default
/// by the supervisor that holds it. Every caller used to compute the same
/// merge and hand back an answer the owner already had.
pub(crate) struct StopRequest {
    /// Skip the graceful signal entirely (force-shutdown path).
    pub(crate) force: bool,
    pub(crate) wait_full_exit: bool,
    /// When set, a mid-stop force request (second Ctrl+C) escalates the
    /// in-flight graceful wait — the manual-stop paths pass the runner's
    /// shutdown flag here.
    pub(crate) interrupt: Option<tokio::sync::watch::Receiver<bool>>,
    /// Where completion goes; see [`StopNotify`].
    pub(crate) notify: StopNotify,
    /// See [`RestartRequest::reset_policy`]. A user stopping a service clears
    /// its failure history; a stop that is one step of a rebuild does not.
    pub(crate) reset_policy: bool,
}

/// How a stop's completion travels back.
pub(crate) enum StopNotify {
    /// The manual path: [`super::ProcessReport::ServiceStopComplete`] on the
    /// report channel, carrying the requester's reply for the fold to answer.
    Reply(Option<tokio::sync::oneshot::Sender<crate::command::CommandResult>>),
    /// The shutdown path: a plain done-signal the reverse-dependency loop
    /// joins on, per depth. (The internal channel is not read during
    /// shutdown, so it cannot carry this.)
    Done(tokio::sync::oneshot::Sender<()>),
}

/// Everything a supervisor needs that doesn't vary per request.
#[derive(Clone)]
pub(crate) struct StartEnv {
    pub(crate) base_dir: PathBuf,
    pub(crate) pid_dir: PathBuf,
    pub(crate) platform: crate::config::Platform,
    pub(crate) docker_client: Option<bollard::Docker>,
    pub(crate) emitter: LifecycleEmitter,
    /// Global shutdown defaults, for stopping a start that lost a race.
    pub(crate) shutdown: ShutdownConfig,
    /// Whether a bound port may fall back to an ephemeral one. A workspace
    /// constant, so it belongs here rather than on every start request.
    pub(crate) fallback_ports: bool,
    /// Where every peer can be reached, for rendering this service's
    /// `$(peer.KEY)` env references at the moment it starts.
    pub(crate) endpoints: crate::endpoints::EndpointReader,
    /// Set once teardown begins. Checked before every self-started start, so
    /// a supervisor cannot spawn into a shutdown the runner has already
    /// planned around — which is why the gate needs no teardown revocation.
    pub(crate) shutdown_rx: tokio::sync::watch::Receiver<bool>,
    /// The build manager's mailbox. Every build this supervisor needs —
    /// the first one as much as a rebuild — is asked for here, because
    /// coalescing is cross-service (one `bazel build` for N targets) even
    /// though the cycle it feeds belongs to each supervisor.
    pub(crate) batcher_tx: mpsc::UnboundedSender<crate::build_tool::batcher::BatchRequest>,
    /// Project-wide watch-ignore patterns, for the build spec this supervisor
    /// hands the build manager.
    pub(crate) global_watch_ignore: Vec<String>,
    /// Workspace-wide `.bazelrc` configuration name, for a service that builds
    /// with Bazel and names none of its own.
    pub(crate) bazel_config: Option<String>,
    /// What every process says about itself. This supervisor computes its own
    /// permission from its dependencies' entries — see [`crate::gate::level`].
    pub(crate) facts: crate::facts::FactsReader,
    /// Set once setup is far enough along for anything to start. A supervisor
    /// answers its own "may I run?", and a service with no dependencies is
    /// permitted from the moment its facts exist — which is before the
    /// watcher does.
    pub(crate) released: tokio::sync::watch::Receiver<bool>,
    pub(crate) secrets: crate::secrets::SecretStore,
}

/// Owner half for services.
pub(crate) type ServiceStarts = super::registry::Supervisors<ServiceCommand>;

/// Start one start-supervisor per service, each taking ownership of its
/// bound proxy (if any) from `proxies`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_supervisors<'a>(
    names: impl Iterator<Item = &'a String>,
    env: &StartEnv,
    outputs: &dyn Fn(&str) -> Option<ProcessOutput>,
    resolved: &dyn Fn(&str) -> Option<crate::config::ResolvedService>,
    dependents: &dyn Fn(&str) -> Vec<String>,
    report_tx: &mpsc::UnboundedSender<super::ProcessReport>,
    proxies: &mut std::collections::HashMap<String, ProxyAssets>,
    publishers: &mut std::collections::HashMap<String, crate::facts::FactsPublisher>,
) -> ServiceStarts {
    ServiceStarts::spawn_all(names, |name, rx, busy| {
        let output = outputs(&name);
        let assets = proxies.remove(&name);
        let resolved = resolved(&name);
        let dependents = dependents(&name);
        let facts = publishers.remove(&name);
        supervise(
            name,
            rx,
            env.clone(),
            output,
            report_tx.clone(),
            busy,
            assets,
            resolved,
            dependents,
            facts,
        )
    })
}

/// Assemble the context for one start.
///
/// Everything here is either the supervisor's own (its resolved config, its
/// last docker mapping) or read from a published projection — which is what
/// lets a supervisor start itself without asking the scheduler for anything.
fn build_context(
    name: &str,
    resolved: &crate::config::ResolvedService,
    batch_built: bool,
    env: &StartEnv,
    last_docker_bindings: &[crate::docker::DockerPortBinding],
) -> Result<Box<ServiceStartContext>, String> {
    let mut resolved = resolved.clone();
    crate::endpoints::render_env(&env.endpoints.snapshot(), name, &mut resolved.env)
        .map_err(|error| error.to_string())?;
    Ok(Box::new(ServiceStartContext {
        resolved,
        batch_built,
        // The proxy's contribution is filled in per spawn below.
        listen_fds: Vec::new(),
        listen_fds_env: std::collections::HashMap::new(),
        fallback_ports: env.fallback_ports,
        prior_docker_port_bindings: last_docker_bindings.to_vec(),
        secrets: env.secrets.clone(),
    }))
}

/// Land a failed service in the phase its restart policy implies.
///
/// A lazy service that may still retry returns to `Lazy` so its proxy re-arms;
/// everything else — including a lazy service past the crash ceiling — stays
/// `Failed`.
fn apply_failure_phase(owner: &mut PhaseOwner, policy: &super::health::PolicyOutcome) {
    let phase = match policy {
        super::health::PolicyOutcome::LazyRearm { give_up: false, .. } => {
            crate::process::ServiceState::Lazy
        }
        _ => crate::process::ServiceState::Failed,
    };
    owner.transition(phase, policy.restart_pending());
}

/// This service's phase, and the write end that tells the rest of the stack
/// about it.
///
/// Bundled because the three things that make up a service's published facts —
/// what phase it is in, what it holds, whether it has a retry armed — change at
/// different moments but must always be published *together*: a reader that saw
/// a new phase against a stale pid would draw the wrong conclusion about
/// liveness. Every mutator here republishes the whole value.
///
/// `DependencyFailed` is resolved here rather than passed in: it is a fact
/// about this process's own dependencies, and the roots come from the same
/// snapshot every other question is answered from.
struct PhaseOwner {
    name: String,
    deps: Vec<crate::config::Dependency>,
    world: crate::facts::FactsReader,
    facts: crate::facts::FactsPublisher,
    phase: crate::process::ServiceState,
    runtime: Option<crate::state_store::ServiceRuntime>,
    restart_pending: bool,
}

impl PhaseOwner {
    fn publish(&mut self) {
        let stranded = if self.phase == crate::process::ServiceState::DependencyFailed {
            crate::gate::failed_roots(&self.deps, &self.world.snapshot())
        } else {
            Vec::new()
        };
        self.facts.publish(
            crate::facts::ProcessFacts::for_service(
                &self.name,
                self.phase,
                self.runtime.clone(),
                stranded,
            )
            .with_restart_pending(self.restart_pending),
        );
    }

    /// Enter `phase`. Idempotent — the publisher drops a republish that says
    /// nothing new, which is what keeps the merge cycle from spinning.
    fn set(&mut self, phase: crate::process::ServiceState) {
        self.phase = phase;
        self.publish();
    }

    /// Record what this supervisor now holds. Published independently of any
    /// phase change: a wire can land while the service is already `Running`,
    /// and custody is the liveness answer every reader wants.
    fn set_runtime(&mut self, runtime: Option<crate::state_store::ServiceRuntime>) {
        self.runtime = runtime;
        self.publish();
    }

    fn set_restart_pending(&mut self, restart_pending: bool) {
        self.restart_pending = restart_pending;
        self.publish();
    }

    /// Enter `phase` and record what the restart policy decided, in **one**
    /// publication.
    ///
    /// Two would let a reader observe the failure before the retry that is
    /// already armed beside it — and a reader asking "is anything still coming
    /// up?" would answer no, and tear the stack down. Publishing them together
    /// is what the scheduler's old single-threaded fold gave for free.
    fn transition(&mut self, phase: crate::process::ServiceState, restart_pending: bool) {
        self.phase = phase;
        self.restart_pending = restart_pending;
        self.publish();
    }

    /// Reconcile against the dependencies' current facts.
    ///
    /// Only a process that is waiting — or already stranded — has anything to
    /// reconcile: one that is building, starting, running or stopped is past
    /// the point where a dependency's failure retroactively strands it.
    /// Returns what to narrate, if anything.
    fn reconcile_dependencies(&mut self) -> Option<String> {
        use crate::process::ServiceState;
        if !matches!(
            self.phase,
            ServiceState::Pending | ServiceState::DependencyFailed
        ) {
            return None;
        }
        let roots = crate::gate::failed_roots(&self.deps, &self.world.snapshot());
        if !roots.is_empty() {
            let was_stranded = self.phase == ServiceState::DependencyFailed;
            let previous = self.facts.current_roots().to_vec();
            self.set(ServiceState::DependencyFailed);
            if was_stranded && previous == roots {
                return None;
            }
            return Some(match roots.as_slice() {
                [one] => format!("skipped (dependency '{one}' failed)"),
                many => format!("skipped (dependencies '{}' failed)", many.join("', '")),
            });
        }
        if self.phase == ServiceState::DependencyFailed {
            // A lazy service reaches here only after a connection moved it out
            // of `Lazy`; `Pending` preserves that queued request.
            self.set(ServiceState::Pending);
            return Some(String::new());
        }
        None
    }
}

/// How far this service's dependencies currently let it go.
///
/// Read at the moment it is needed rather than delivered, which is what makes
/// the old revision stamp unnecessary: demand and permission are now both held
/// by this loop, so there is no window in which one can be staler than the
/// other. Nothing may go anywhere until setup releases the stack.
fn current_level(
    released: bool,
    world: &crate::facts::FactsReader,
    resolved: &crate::config::ResolvedService,
) -> crate::gate::Gate {
    if !released {
        return crate::gate::Gate::Blocked;
    }
    crate::gate::level(&resolved.depends_on, &world.snapshot())
}

/// Stop a service brought up by a start that has since been superseded.
///
/// The losing attempt may already have a live process; nothing else knows
/// about it, so it has to be stopped here. Detached, because the caller is
/// the supervisor loop and the next start shouldn't wait on the old one's
/// shutdown grace period.
fn stop_superseded_start(
    env: &StartEnv,
    name: &str,
    context: &ServiceStartContext,
    start_result: service::StartResult,
) {
    let shutdown_config = context
        .resolved
        .shutdown
        .clone()
        .map(|shutdown| shutdown.merged_over(&env.shutdown))
        .unwrap_or_else(|| env.shutdown.clone());
    let debug = service::StopDebug::new(name.to_string(), env.emitter.clone());
    tokio::spawn(async move {
        let service::StartResult {
            handle,
            child_output,
        } = start_result;
        // Nothing will consume this, and holding it open keeps the child's
        // pipe alive.
        drop(child_output);
        let _ =
            service::stop_service(handle, Some(&shutdown_config), true, false, Some(debug)).await;
    });
}

/// Drive one service's starts, strictly in order.
///
/// Same rule as the task supervisor: a superseded start is **finished, not
/// aborted**. `start_service_worker` may already have a process up by the
/// time a newer request arrives, and dropping that future would strand it.
#[allow(clippy::too_many_arguments)]
async fn supervise(
    name: String,
    mut rx: mpsc::UnboundedReceiver<ServiceCommand>,
    env: StartEnv,
    output: Option<ProcessOutput>,
    report_tx: mpsc::UnboundedSender<super::ProcessReport>,
    busy: Arc<AtomicBool>,
    proxy_assets: Option<ProxyAssets>,
    resolved: Option<crate::config::ResolvedService>,
    dependents: Vec<String>,
    facts: Option<crate::facts::FactsPublisher>,
) {
    // Names come from the same map the configs do, so `None` is unreachable;
    // ending the supervisor beats panicking, and callers already treat a dead
    // mailbox as "supervisor is gone".
    let Some(mut resolved) = resolved else { return };
    let Some(facts) = facts else { return };
    let service_writer = output.as_ref().map(|output| output.writer());
    // What every peer says about itself, and whether the stack is open for
    // business yet. Both are read at the top of the idle loop — the single
    // place demand is spent — and merely woken from the select below.
    let mut world = env.facts.clone();
    let mut watching_world = true;
    let mut released_rx = env.released.clone();
    let mut stack_released = *released_rx.borrow();
    // Whether the build manager has produced this service's artifact, so the
    // per-service build inside `start_service_worker` must not run again.
    let mut batch_built = false;
    // Where the build manager delivers this service's artifact.
    let (prepare_tx, mut prepare_rx) =
        mpsc::unbounded_channel::<crate::build_tool::batch::PrepareOutcome>();
    // Whether anything wants this service running. One-shot: see `Demand`.
    // A lazy service starts life unwanted and is demanded by its first
    // connection; everything else is wanted from the moment it exists.
    let mut demand = if resolved.lazy {
        super::Demand::None
    } else {
        super::Demand::Scheduled
    };
    // The docker host-port mapping this supervisor's last spawn got. Retained
    // across stops so the next start can request the same ports.
    let mut last_docker_bindings: Vec<crate::docker::DockerPortBinding> = Vec::new();
    let mut pending: Option<ServiceCommand> = None;
    let mut mailbox_closed = false;
    // The proxy outlives individual starts — its listeners span process
    // generations, which is what makes zero-downtime restart possible.
    let (mut proxy, mut demand_rx) = match proxy_assets {
        Some(assets) => (Some(assets.proxy), assets.demand_rx),
        None => (None, None),
    };
    // This service's phase is this supervisor's to answer for, because every
    // input it depends on is observed here. A lazy service with a bound proxy
    // starts life merely listening; everything else starts life wanted.
    let mut owner = PhaseOwner {
        name: name.clone(),
        deps: resolved.depends_on.clone(),
        world: world.clone(),
        facts,
        phase: if resolved.lazy && proxy.is_some() {
            super::ServiceState::Lazy
        } else {
            super::ServiceState::Pending
        },
        runtime: None,
        restart_pending: false,
    };
    owner.publish();
    // An artifact request is outstanding. A supervisor that needs an artifact
    // does not spawn until it has one, whatever its dependencies allow — an
    // artifact is as much a precondition as a dependency, and getting it is
    // its own job.
    //
    // Asked for *after* the owner exists, so the `Building` phase this puts
    // the service into is published by the thing that entered it.
    let mut awaiting_artifact = resolved.is_build_tool_managed() && !resolved.lazy && {
        owner.set(super::ServiceState::Building);
        request_artifact(&name, &resolved, &env, &prepare_tx)
    };
    // The process this supervisor currently owns, from wire to reap/stop.
    let mut held: Option<service::ServiceHandle> = None;
    // The output reader for the held process, and its end-of-stream signal.
    // Stop drains the reader before notifying; end-of-stream while idle is
    // the crash path (reap + report).
    let mut reader: Option<tokio::task::JoinHandle<()>> = None;
    let mut reader_eof: Option<tokio::sync::oneshot::Receiver<()>> = None;
    // Dropping this ends the held process's health monitor, if one ran.
    let mut monitor_cancel: Option<tokio::sync::oneshot::Sender<()>> = None;
    // A rebuild cycle in flight: a build was asked for and the restart it
    // implies has not happened yet. `stale` records that a watched file
    // changed *during* the cycle, which is what makes the artifact it is
    // about to produce already out of date.
    let mut cycle: Option<CycleState> = None;
    // A build succeeded but its restart was skipped because the cycle went
    // stale, so the running process is behind the artifact. Up-to-date is
    // measured against the last *build*, not the running process, so the
    // follow-up cycle must restart even when the build tool says there is
    // nothing to do. Cleared only by a restart that actually happens.
    let mut artifact_ahead = false;
    // Where the batcher delivers this service's share of a batch.
    let (rebuild_tx, mut rebuild_rx) =
        mpsc::unbounded_channel::<crate::build_tool::batcher::RebuildItemOutcome>();
    // Where the batcher delivers this service's share of a build-graph
    // re-query.
    let (requery_tx, mut requery_rx) =
        mpsc::unbounded_channel::<crate::build_tool::batch::RequeryOutcome>();
    // The OSC response scanner for the held spawn. It holds a sender into the
    // PTY gate, which holds the master's write half, so a stale one keeps the
    // PTY open — it belongs with the process, not with a shadow of it.
    let mut osc_sink: Option<crate::output::OscSinkHandle> = None;
    // Health transitions from the monitor this supervisor spawns. They land
    // here rather than on the report channel so the restart policy sees them
    // before the scheduler does.
    let (health_tx, mut health_rx) = mpsc::unbounded_channel::<bool>();
    // Restart policy. Every input it needs — a failed prepare, a failed ready
    // check, a health transition, how long this spawn lived — is something
    // this loop observed itself, which is what lets the whole of it live here.
    let mut policy =
        super::health::RestartPolicy::new(resolved.on_failure, resolved.lazy && proxy.is_some());
    // When the armed auto-restart is due, and which attempt it is.
    let mut backoff: Option<(tokio::time::Instant, u32)> = None;
    // Facts about the spawn currently held.
    let mut spawned_at: Option<std::time::Instant> = None;
    let mut reached_ready = false;
    // This spawn failed its ready check but was left running (the `notify`
    // policy). Its eventual exit is then old news, not a fresh failure — the
    // scheduler already marked it `Failed` and reported why.
    let mut ready_failed = false;
    // The in-flight ready racer's outcome, forwarded by THIS loop onto the
    // report channel so it always trails its own prepared report (single
    // producer, one channel). Cleared on Start/Stop so a superseded run's
    // outcome can never be forwarded after a newer prepared.
    let mut ready_pending: Option<tokio::sync::oneshot::Receiver<ReadyOutcome>> = None;
    // How to narrate this spawn's ready check, resolved at wire time against
    // the proxy and docker state the probe will actually run against.
    let mut ready_description = String::from("started");
    // Whether the start in flight is one the dependency graph asked for. A
    // manual `don start` or a rebuild's respawn narrates differently, and
    // nothing is waiting on its outcome.
    let mut scheduled_start = false;
    // Teardown runs exactly once. A second pass would re-stop a process that
    // is already gone and re-publish a phase nothing changed.
    let mut torn_down = false;
    // Watched as a `select!` arm, not just polled: without it the supervisor
    // sleeps through the shutdown signal until some unrelated event happens to
    // wake it.
    let mut teardown_rx = env.shutdown_rx.clone();
    // The escalation watch: a second Ctrl+C cuts every grace period short,
    // including the wait for dependents.
    let mut force_rx = crate::signals::force_watch();

    loop {
        sync_connection_policy(&name, &env.emitter, &mut proxy, &owner);
        let command = match pending.take() {
            Some(command) => command,
            None => loop {
                sync_connection_policy(&name, &env.emitter, &mut proxy, &owner);
                busy.store(false, Ordering::Relaxed);
                // Teardown, decided here rather than sequenced from outside:
                // wait for the processes that depend on this one to be gone,
                // then end what this one holds. Runs once — after it, the
                // supervisor has nothing left to own.
                if *env.shutdown_rx.borrow() && !torn_down {
                    torn_down = true;
                    busy.store(true, Ordering::Relaxed);
                    demand = super::Demand::None;
                    backoff = None;
                    super::await_dependents_gone(
                        &name,
                        &env.emitter,
                        &mut world,
                        &dependents,
                        &mut force_rx,
                    )
                    .await;
                    reader_eof = None;
                    monitor_cancel = None;
                    osc_sink = None;
                    ready_pending = None;
                    // Narrated by the process it is happening to, and only
                    // when there is something to stop. Its position in the log
                    // *is* the teardown order — `shutdown_test` reads exactly
                    // that to prove dependents go first.
                    let was_holding = held.is_some();
                    if was_holding {
                        owner.set(super::ServiceState::Stopping);
                        env.emitter.service_event(&name, "stopping...");
                    }
                    let shutdown_config = effective_shutdown(&resolved, &env);
                    // Read out, not borrowed across the await: a `watch::Ref`
                    // held over one is the hazard `state_store` documents.
                    let forced = *force_rx.borrow();
                    let result = run_stop(
                        &name,
                        &env,
                        output.as_ref(),
                        &mut held,
                        &mut reader,
                        &shutdown_config,
                        forced,
                        true,
                        Some(force_rx.clone()),
                    )
                    .await;
                    owner.set_runtime(None);
                    owner.transition(
                        match &result {
                            Ok(()) => super::ServiceState::Stopped,
                            Err(_) => super::ServiceState::Failed,
                        },
                        false,
                    );
                    if was_holding {
                        match &result {
                            Ok(()) => env.emitter.service_event(&name, "stopped"),
                            Err(message) => env.emitter.service_error_event(&name, message),
                        }
                    }
                    // The listener outlived individual processes; it does not
                    // outlive the stack.
                    if let Some(p) = proxy.take() {
                        p.shutdown();
                    }
                    let _ = report_tx.send(super::ProcessReport::ServiceStopComplete {
                        name: name.clone(),
                        result,
                        reply: None,
                    });
                    busy.store(false, Ordering::Relaxed);
                    continue;
                }
                // Start when something wants this running, its dependencies
                // allow it, and it is holding nothing. The level is read here
                // rather than waited on, so a grant published while this
                // supervisor was busy is not missed.
                //
                // `demand` is cleared in this same step — that one-shot-ness
                // is what stops a crashing service relaunching off a gate
                // that stays open across the crash. See `Demand`.
                if held.is_none()
                    && !awaiting_artifact
                    && !*env.shutdown_rx.borrow()
                    && demand.permitted_by(current_level(stack_released, &world, &resolved))
                {
                    demand = super::Demand::None;
                    env.emitter
                        .service_debug_event(&name, "start triggered (deps satisfied)");
                    // Say which non-blocking dependencies we are deliberately
                    // not waiting for, so a start that follows a visible
                    // failure doesn't look like don ignored the graph.
                    for skipped in
                        crate::gate::skipped_non_blocking(&resolved.depends_on, &world.snapshot())
                    {
                        env.emitter.service_event(
                            &name,
                            &format!("starting without non-blocking dependency '{skipped}'"),
                        );
                    }
                    // The phase moves in the same synchronous step demand is
                    // spent, so nothing can observe a service that is starting
                    // but still says it is pending.
                    owner.set(super::ServiceState::Starting);
                    env.emitter.service_event(&name, "starting...");
                    busy.store(true, Ordering::Relaxed);
                    break ServiceCommand::Start(StartRequest {
                        mode: ServiceStartMode::Full,
                        intent: super::ServiceStartIntent::Scheduled,
                        fresh_backend_ports: false,
                    });
                }
                tokio::select! {
                    received = rx.recv() => match received {
                        Some(command) => {
                            busy.store(true, Ordering::Relaxed);
                            break command;
                        }
                        None => return,
                    },
                    // The held process's output ended — it died. Reap and
                    // report; this is the crash path, and watching our own
                    // reader is what replaced the detached crash watcher.
                    _ = wait_eof(&mut reader_eof), if reader_eof.is_some() => {
                        busy.store(true, Ordering::Relaxed);
                        reader_eof = None;
                        monitor_cancel = None;
                        osc_sink = None;
                        if let Some(handle) = reader.take() {
                            await_reader(handle).await;
                        }
                        // The spawn is dead: unregister attach so new clients
                        // are refused and muted stdout resumes.
                        if let Some(output) = output.as_ref() {
                            output.clear_attach().await;
                        }
                        if reap_and_report(
                            &name,
                            &mut owner,
                            &mut held,
                            &report_tx,
                            &env,
                            &mut policy,
                            &mut backoff,
                            spawned_at.take(),
                            reached_ready,
                            ready_failed,
                        )
                        .await
                        .is_err()
                        {
                            return;
                        }
                        reached_ready = false;
                        ready_failed = false;
                        continue;
                    }
                    // The ready racer settled — forward through this loop
                    // so the outcome trails its own prepared report.
                    outcome = wait_ready(&mut ready_pending), if ready_pending.is_some() => {
                        ready_pending = None;
                        if let Some(outcome) = outcome {
                            let policy_outcome = if outcome.success {
                                reached_ready = true;
                                policy.on_ready();
                                backoff = None;
                                super::health::PolicyOutcome::None
                            } else {
                                ready_failed = true;
                                let decided = policy.decide(super::health::FailureKind::Ready);
                                arm_backoff(&name, &env, &decided, &mut backoff, outcome.message.as_deref());
                                // A lazy service that failed its ready check
                                // may still be running — it never bound its
                                // port, say. Nothing else will end it, and
                                // while it lives it holds the PTY open.
                                if matches!(decided, super::health::PolicyOutcome::LazyRearm { .. }) {
                                    reader_eof = None;
                                    monitor_cancel = None;
                                    osc_sink = None;
                                    let _ = run_stop(
                                        &name,
                                        &env,
                                        output.as_ref(),
                                        &mut held,
                                        &mut reader,
                                        &effective_shutdown(&resolved, &env),
                                        true,
                                        false,
                                        None,
                                    )
                                    .await;
                                }
                                decided
                            };
                            // Only a service still `Running` is settling its
                            // own ready check: a crash's reap already moved it.
                            if owner.phase == super::ServiceState::Running {
                                if outcome.success {
                                    // Re-assert the backend on ready. This
                                    // supervisor owns the listener, so it is a
                                    // call rather than a directive that could
                                    // land after the start it belongs to.
                                    if scheduled_start
                                        && let Some(proxy) = proxy.as_ref()
                                    {
                                        proxy.set_backend();
                                    }
                                    owner.transition(super::ServiceState::Ready, false);
                                    if scheduled_start {
                                        env.emitter.service_event(&name, &ready_description);
                                    } else if !outcome.had_check {
                                        // A checkless restart announces itself;
                                        // "restarting..." already said a cycle
                                        // began.
                                        env.emitter.service_event(&name, "restarted");
                                    }
                                } else {
                                    apply_failure_phase(&mut owner, &policy_outcome);
                                    if scheduled_start
                                        && matches!(
                                            policy_outcome,
                                            super::health::PolicyOutcome::None
                                        )
                                        && let Some(ref message) = outcome.message
                                    {
                                        env.emitter.service_error_event(&name, message);
                                    }
                                }
                            }
                            scheduled_start = false;
                        }
                        continue;
                    }
                    // This service's artifact, from the build manager.
                    // Nothing has spawned yet, and nothing can until this
                    // lands — which is also what puts the watch registrations
                    // this build resolved in place before the first start.
                    outcome = prepare_rx.recv() => {
                        use crate::build_tool::batch::PrepareOutcome;
                        let Some(outcome) = outcome else { continue };
                        busy.store(true, Ordering::Relaxed);
                        match outcome {
                            PrepareOutcome::Ready { binary_path } => {
                                awaiting_artifact = false;
                                batch_built = true;
                                // Written onto the config this supervisor
                                // already holds — the build taught us the path
                                // and nothing else.
                                if let Some(path) = binary_path {
                                    resolved.resolved_binary_path = Some(path);
                                }
                                // The artifact exists; back to waiting on
                                // dependencies like anything else.
                                if owner.phase == super::ServiceState::Building {
                                    owner.set(super::ServiceState::Pending);
                                }
                            }
                            // A watched file changed while the build ran, so
                            // the artifact is already out of date. The build
                            // manager narrated why; ask again. The service
                            // stays `Building` throughout, so nothing starts
                            // against it.
                            PrepareOutcome::Stale => {
                                awaiting_artifact = request_artifact(
                                    &name, &resolved, &env, &prepare_tx,
                                );
                            }
                            PrepareOutcome::Failed(message) => {
                                awaiting_artifact = false;
                                // The end of the road, not a crash. Retrying a
                                // compile that just failed recompiles the same
                                // broken sources, so withdrawing demand here is
                                // what keeps this away from the restart policy.
                                demand = super::Demand::None;
                                env.emitter
                                    .service_error_event(&name, &format!("build failed: {message}"));
                                owner.set(super::ServiceState::Failed);
                            }
                        }
                        continue;
                    }
                    // This service's share of a finished batch. The cycle
                    // continues here rather than in the scheduler, so the
                    // build, the stop and the spawn are one sequence in one
                    // place.
                    // A re-query this supervisor asked for. The patterns are
                    // already registered; what is left is whether the process
                    // it is running was built from a graph that has moved —
                    // which only something holding that process can answer.
                    outcome = requery_rx.recv() => {
                        use crate::build_tool::batch::RequeryOutcome;
                        let Some(outcome) = outcome else { continue };
                        if outcome == RequeryOutcome::Updated && held.is_some() {
                            env.emitter
                                .service_event(&name, "build graph changed — rebuilding");
                            busy.store(true, Ordering::Relaxed);
                            break rebuild_again();
                        }
                        continue;
                    }
                    outcome = rebuild_rx.recv() => {
                        let Some(outcome) = outcome else { continue };
                        busy.store(true, Ordering::Relaxed);
                        match settle_cycle(
                            &name,
                            &env,
                            outcome,
                            &mut cycle,
                            &mut artifact_ahead,
                            &mut batch_built,
                        ) {
                            CycleNext::Done => continue,
                            CycleNext::Again => break rebuild_again(),
                            CycleNext::Restart => {
                                break ServiceCommand::Restart(Box::new(RestartRequest {
                                    wait_full_exit: resolved.requires_full_exit_on_restart(),
                                    interrupt: None,
                                    // Connections arriving mid-restart queue
                                    // instead of racing the dying process.
                                    clear_backend_first: true,
                                    start_mode: ServiceStartMode::SpawnOnly,
                                    fresh_backend_ports: true,
                                    intent: super::ServiceStartIntent::Background,
                                    reply: None,
                                    announce_restarting: true,
                                    // A rebuild is not a user giving up on
                                    // the service; the failure history rides
                                    // across it.
                                    reset_policy: false,
                                }));
                            }
                        }
                    }
                    // The monitor this supervisor started saw the service
                    // change health. The policy decides before the scheduler
                    // hears about it.
                    transition = health_rx.recv() => {
                        let Some(healthy) = transition else { continue };
                        busy.store(true, Ordering::Relaxed);
                        let policy_outcome = if healthy {
                            // Recovery clears the backoff counter only; the
                            // rapid-crash streak is cleared by a spawn that
                            // outlives the crash window, not by a transient
                            // return to Ready.
                            policy.on_ready();
                            backoff = None;
                            super::health::PolicyOutcome::None
                        } else {
                            let decided = policy.decide(super::health::FailureKind::Unhealthy);
                            arm_backoff(&name, &env, &decided, &mut backoff, Some("unhealthy"));
                            decided
                        };
                        // Only these two phases participate: a probe that
                        // settles after a stop or restart is about a process
                        // this supervisor no longer holds.
                        if healthy && owner.phase == super::ServiceState::Unhealthy {
                            owner.transition(super::ServiceState::Ready, false);
                            env.emitter
                                .service_event(&name, "recovered (health check passing)");
                        } else if !healthy && owner.phase == super::ServiceState::Ready {
                            owner.transition(
                                super::ServiceState::Unhealthy,
                                policy_outcome.restart_pending(),
                            );
                            if matches!(policy_outcome, super::health::PolicyOutcome::None) {
                                // `notify`: nothing was scheduled, so this line
                                // is the only thing that will say so.
                                env.emitter
                                    .service_error_event(&name, "unhealthy (health check failing)");
                            }
                        } else {
                            owner.set_restart_pending(policy_outcome.restart_pending());
                        }
                        continue;
                    }
                    // The armed auto-restart came due. It runs as an ordinary
                    // internal restart, so the stop-then-start sequence and
                    // its reports are the same ones a manual restart makes.
                    () = wait_backoff(&backoff) => {
                        busy.store(true, Ordering::Relaxed);
                        let attempt = backoff.take().map_or(1, |(_, attempt)| attempt);
                        env.emitter
                            .service_event(&name, &format!("auto-restart firing (attempt {attempt})"));
                        break ServiceCommand::Restart(Box::new(RestartRequest {
                            wait_full_exit: false,
                            interrupt: None,
                            clear_backend_first: false,
                            start_mode: ServiceStartMode::Full,
                            fresh_backend_ports: false,
                            intent: super::ServiceStartIntent::Background,
                            reply: None,
                            announce_restarting: false,
                            // This IS the policy retrying. Keeping the streak
                            // is what lets the ceiling ever be reached.
                            reset_policy: false,
                        }));
                    }
                    // A peer's facts moved, or setup released the stack.
                    // Nothing is decided here: the level is recomputed at the
                    // top of this loop, which is the single place demand is
                    // spent. Both waits end permanently once their sender is
                    // gone, so a `None` means stop selecting rather than spin.
                    // Teardown began. Nothing is decided here: the check at the
                    // top of this loop is the single place it runs, and this
                    // is only what wakes the loop to reach it.
                    _ = teardown_rx.changed(), if !torn_down => {
                        continue;
                    }
                    changed = world.changed(), if watching_world => {
                        if changed.is_none() {
                            watching_world = false;
                        }
                        // A blocking dependency may have failed while this
                        // service was waiting on it — or recovered.
                        match owner.reconcile_dependencies() {
                            Some(message) if message.is_empty() => {
                                env.emitter.service_debug_event(
                                    &name,
                                    "dependency recovered; re-queued",
                                );
                            }
                            Some(message) => env.emitter.service_error_event(&name, &message),
                            None => {}
                        }
                        continue;
                    }
                    released = released_rx.changed(), if !stack_released => {
                        stack_released = released.is_err() || *released_rx.borrow();
                        continue;
                    }
                    // The lazy proxy saw a connection: demand. The runner
                    // gates on service state, so duplicates are harmless.
                    trigger = wait_demand(&mut demand_rx), if demand_rx.is_some() => {
                        match trigger {
                            Some(_) => {
                                demand = demand.max(super::Demand::Scheduled);
                                if owner.phase == super::ServiceState::Lazy {
                                    owner.set(super::ServiceState::Pending);
                                }
                                let _ = report_tx.send(super::ProcessReport::Demand {
                                    name: name.clone(),
                                    demand,
                                });
                                // A lazy service is the one thing that does
                                // not build at construction: nobody had asked
                                // for it yet. Now somebody has — and it still
                                // builds before its dependencies are checked,
                                // for the same reason everything else does.
                                if resolved.is_build_tool_managed()
                                    && !batch_built
                                    && !awaiting_artifact
                                {
                                    env.emitter.service_event(
                                        &name,
                                        "first connection — building before start",
                                    );
                                    awaiting_artifact = request_artifact(
                                        &name, &resolved, &env, &prepare_tx,
                                    );
                                }
                            }
                            // Every trigger sender is gone (proxy shut down);
                            // stop selecting on a closed channel.
                            None => demand_rx = None,
                        }
                        continue;
                    }
                }
            },
        };
        let start_request = match command {
            ServiceCommand::Start(request) => {
                // A start someone asked for by name is admitted here, by the
                // only thing that knows whether it can be honoured. The
                // scheduler used to answer this from a shadow of the phase and
                // a shared busy flag, and then this loop re-checked anyway.
                match admit_requested_start(
                    &name,
                    &owner,
                    held.is_some(),
                    &resolved.depends_on,
                    current_level(stack_released, &world, &resolved),
                    &world,
                    &request.intent,
                ) {
                    Ok(()) => request,
                    Err(refusal) => {
                        if let super::ServiceStartIntent::Reply { reply } = request.intent {
                            let _ = reply.send(Err(refusal));
                        }
                        continue;
                    }
                }
            }
            ServiceCommand::BuildGraphChanged => {
                request_requery(&name, &resolved, &env, &requery_tx);
                continue;
            }
            ServiceCommand::Rebuild(request) => {
                // A hard restart asked for by name is admitted here, for the
                // same reason a start is: the phase it would interrupt is this
                // supervisor's.
                if request.source == RebuildSource::Requested
                    && request.reply.is_some()
                    && matches!(
                        owner.phase,
                        super::ServiceState::Building
                            | super::ServiceState::Starting
                            | super::ServiceState::Stopping
                    )
                {
                    if let Some(reply) = request.reply {
                        let _ = reply.send(Err(crate::command::CommandError::InvalidState {
                            name: name.clone(),
                            message: format!("cannot hard restart while {:?}", owner.phase),
                        }));
                    }
                    continue;
                }
                // A change that arrives while a cycle is running does not
                // start a second one: the artifact this cycle is about to
                // produce is already out of date, so the cycle records that
                // and runs another when it finishes. Only a request by name
                // supersedes.
                if request.source == RebuildSource::FileChange
                    && let Some(cycle) = cycle.as_mut()
                {
                    cycle.stale = true;
                    if let Some(reply) = request.reply {
                        let _ = reply.send(Ok(()));
                    }
                    continue;
                }
                // A new cycle: staleness is per-cycle, so it starts clear.
                // `artifact_ahead` deliberately does not — it records that
                // the *process* is behind, which no new request changes.
                cycle = Some(CycleState { stale: false });
                if !resolved.is_build_tool_managed() {
                    // Not batched: run this service's own build command here,
                    // *before* the stop, so a failed build leaves the version
                    // that works still running.
                    env.emitter
                        .service_event(&name, "rebuilding (file changed)");
                    if let Some(reply) = request.reply {
                        let _ = reply.send(Ok(()));
                    }
                    let build = super::service_worker::run_service_build_worker(
                        &env.base_dir,
                        env.docker_client.as_ref(),
                        &env.emitter,
                        &name,
                        &resolved,
                        false,
                        service_writer.as_ref(),
                        &env.secrets,
                    );
                    // Raced against the mailbox so a Stop or a shutdown can
                    // cut a slow build short — the build child is
                    // `kill_on_drop`, so abandoning the future ends it.
                    tokio::pin!(build);
                    let mut shutdown_rx = env.shutdown_rx.clone();
                    let outcome = loop {
                        tokio::select! {
                            result = &mut build => break result,
                            // Teardown must not wait out a slow build. The
                            // build child is `kill_on_drop`, so abandoning the
                            // future here ends it.
                            _ = shutdown_rx.changed() => {
                                if *shutdown_rx.borrow() {
                                    env.emitter.service_event(
                                        &name,
                                        "rebuild cancelled by shutdown",
                                    );
                                    cycle = None;
                                    break Err("shutdown requested".to_string());
                                }
                            }
                            next = rx.recv(), if !mailbox_closed => match next {
                                Some(ServiceCommand::Rebuild(request))
                                    if request.source == RebuildSource::FileChange =>
                                {
                                    if let Some(cycle) = cycle.as_mut() {
                                        cycle.stale = true;
                                    }
                                    if let Some(reply) = request.reply {
                                        let _ = reply.send(Ok(()));
                                    }
                                }
                                Some(next) => {
                                    // Superseded: drop the build and take the
                                    // newer command.
                                    cycle = None;
                                    pending = Some(next);
                                    break Err("superseded".to_string());
                                }
                                None => mailbox_closed = true,
                            },
                        }
                    };
                    if cycle.is_none() {
                        continue;
                    }
                    let item = match outcome {
                        Ok(()) => crate::build_tool::batcher::RebuildItemOutcome::NotBuilt,
                        Err(message) => {
                            crate::build_tool::batcher::RebuildItemOutcome::Failed(message)
                        }
                    };
                    match settle_cycle(
                        &name,
                        &env,
                        item,
                        &mut cycle,
                        &mut artifact_ahead,
                        &mut batch_built,
                    ) {
                        CycleNext::Done => continue,
                        CycleNext::Again => {
                            pending = Some(rebuild_again());
                            continue;
                        }
                        CycleNext::Restart => {
                            pending = Some(ServiceCommand::Restart(Box::new(RestartRequest {
                                wait_full_exit: resolved.requires_full_exit_on_restart(),
                                interrupt: None,
                                clear_backend_first: true,
                                start_mode: ServiceStartMode::SpawnOnly,
                                fresh_backend_ports: true,
                                intent: super::ServiceStartIntent::Background,
                                reply: None,
                                announce_restarting: true,
                                reset_policy: false,
                            })));
                            continue;
                        }
                    }
                }
                let spec = rebuild_spec_for(&name, &resolved, &env);
                let accepted = queue_build(&env, spec, &request, &rebuild_tx).await;
                if let Some(reply) = request.reply {
                    let _ = reply.send(accepted.clone().map_err(|message| {
                        crate::command::CommandError::InvalidState {
                            name: name.clone(),
                            message,
                        }
                    }));
                }
                if accepted.is_err() {
                    cycle = None;
                }
                continue;
            }
            ServiceCommand::Stop(request) => {
                // A stop someone asked for is admitted here. Holding nothing
                // is only an error if there was nothing to clear either: a
                // lazy service that never triggered, or one parked in a
                // failure, is *stoppable* — that is how a user clears it —
                // and `run_stop` below is a no-op that lands `Stopped`.
                if let StopNotify::Reply(Some(_)) = &request.notify
                    && held.is_none()
                    && !matches!(
                        owner.phase,
                        super::ServiceState::Lazy
                            | super::ServiceState::Pending
                            | super::ServiceState::Failed
                            | super::ServiceState::DependencyFailed
                    )
                {
                    if let StopNotify::Reply(Some(reply)) = request.notify {
                        let _ = reply.send(Err(crate::command::CommandError::InvalidState {
                            name: name.clone(),
                            message: "not running".to_string(),
                        }));
                    }
                    continue;
                }
                if let StopNotify::Reply(Some(_)) = &request.notify {
                    env.emitter.service_event(
                        &name,
                        match (held.is_some(), owner.phase) {
                            (false, super::ServiceState::Lazy | super::ServiceState::Pending) => {
                                "stopped before lazy start"
                            }
                            (false, _) => "stopped (was failed)",
                            (true, _) => "stopping... (requested)",
                        },
                    );
                }
                reader_eof = None;
                monitor_cancel = None;
                osc_sink = None;
                ready_pending = None;
                // A stop withdraws demand: nothing wants this running now, so
                // an open gate cannot undo it. A restart's follow-up start is
                // part of the same command, so it does not consult demand.
                demand = super::Demand::None;
                if request.reset_policy {
                    policy.reset();
                }
                // Whatever the policy had queued is moot: this process is
                // going away by request.
                backoff = None;
                owner.set_restart_pending(false);
                spawned_at = None;
                reached_ready = false;
                ready_failed = false;
                // Say so before the grace period, not after: a user watching
                // `don status` during a slow shutdown is asking exactly this.
                owner.set(super::ServiceState::Stopping);
                let result = run_stop(
                    &name,
                    &env,
                    output.as_ref(),
                    &mut held,
                    &mut reader,
                    &effective_shutdown(&resolved, &env),
                    request.force,
                    request.wait_full_exit,
                    request.interrupt,
                )
                .await;
                owner.set_runtime(None);
                owner.set(match &result {
                    Ok(()) => super::ServiceState::Stopped,
                    Err(_) => super::ServiceState::Failed,
                });
                match request.notify {
                    StopNotify::Reply(reply) => {
                        if report_tx
                            .send(super::ProcessReport::ServiceStopComplete {
                                name: name.clone(),
                                result,
                                reply,
                            })
                            .is_err()
                        {
                            return;
                        }
                    }
                    StopNotify::Done(done) => {
                        let _ = done.send(());
                    }
                }
                continue;
            }
            ServiceCommand::Restart(request) => {
                // A restart asked for by name, admitted the same way a start
                // is. `Pending` *is* refused here, unlike a start: a restart
                // means "replace what is running", and there is nothing to
                // replace while the graph is still waiting.
                if request.reply.is_some()
                    && matches!(
                        owner.phase,
                        super::ServiceState::Pending
                            | super::ServiceState::Building
                            | super::ServiceState::Starting
                            | super::ServiceState::Stopping
                    )
                {
                    if let Some(reply) = request.reply {
                        let _ = reply.send(Err(crate::command::CommandError::InvalidState {
                            name: name.clone(),
                            message: format!("cannot restart while {:?}", owner.phase),
                        }));
                    }
                    continue;
                }
                if request.reply.is_some() {
                    env.emitter.service_event(
                        &name,
                        if held.is_some() {
                            "stopping... (requested restart)"
                        } else {
                            // Nothing to replace: the stop below is a no-op and
                            // this is really just a start.
                            "starting..."
                        },
                    );
                }
                reader_eof = None;
                monitor_cancel = None;
                osc_sink = None;
                ready_pending = None;
                demand = super::Demand::None;
                if request.reset_policy {
                    policy.reset();
                }
                backoff = None;
                owner.set_restart_pending(false);
                spawned_at = None;
                reached_ready = false;
                ready_failed = false;
                owner.set(super::ServiceState::Stopping);
                // Owning the proxy makes this a call rather than a mailbox
                // hop, so it cannot arrive after the stop it must precede.
                if request.clear_backend_first
                    && let Some(proxy) = proxy.as_ref()
                {
                    proxy.clear_backend();
                }
                // Bound outside the future: `run_stop` borrows it, and a
                // temporary would not outlive the `select!` below.
                let shutdown_config = effective_shutdown(&resolved, &env);
                let stop = run_stop(
                    &name,
                    &env,
                    output.as_ref(),
                    &mut held,
                    &mut reader,
                    &shutdown_config,
                    false,
                    request.wait_full_exit,
                    request.interrupt,
                );
                // Race the mailbox while stopping. A `MarkStale` landing here
                // is the one the runner used to catch by processing
                // `RebuildStale` during a detached stop; without this the
                // change is lost, because the watcher sends no second
                // `Rebuild` for a cycle it believes is already running.
                tokio::pin!(stop);
                let result = loop {
                    tokio::select! {
                        result = &mut stop => break result,
                        next = rx.recv(), if !mailbox_closed => match next {
                            Some(ServiceCommand::Rebuild(request))
                                if request.source == RebuildSource::FileChange =>
                            {
                                if let Some(cycle) = cycle.as_mut() {
                                    cycle.stale = true;
                                }
                                if let Some(reply) = request.reply {
                                    let _ = reply.send(Ok(()));
                                }
                            }
                            Some(next) => {
                                pending = Some(next);
                                break (&mut stop).await;
                            }
                            None => mailbox_closed = true,
                        },
                    }
                };
                // A stale cycle skips its spawn: the service stays stopped and
                // the follow-up cycle — queued here, by the thing that knows
                // this cycle just ended — brings it up on the newer sources.
                let stale_now = cycle.as_ref().is_some_and(|cycle| cycle.stale);
                if stale_now && cycle.take().is_some() {
                    pending = Some(rebuild_again());
                }
                // A failed stop leaves nothing safe to start over, and a
                // teardown that began mid-restart wants no new process.
                let restarting = result.is_ok() && !stale_now && !*env.shutdown_rx.borrow();
                // The stop half lands its own phase, so the pair a restart has
                // always shown — Stopped then Starting — is two publications
                // from one owner rather than two folds in another actor.
                owner.set_runtime(None);
                owner.set(match &result {
                    Ok(()) => super::ServiceState::Stopped,
                    Err(_) => super::ServiceState::Failed,
                });
                if report_tx
                    .send(super::ProcessReport::ServiceStopComplete {
                        name: name.clone(),
                        result,
                        reply: request.reply,
                    })
                    .is_err()
                {
                    return;
                }
                if !restarting {
                    continue;
                }
                owner.set(super::ServiceState::Starting);
                env.emitter.service_event(
                    &name,
                    if request.announce_restarting {
                        "restarting..."
                    } else {
                        "starting..."
                    },
                );
                // Committing to a spawn brings the process up to the current
                // artifact.
                artifact_ahead = false;
                cycle = None;
                StartRequest {
                    mode: request.start_mode,
                    intent: request.intent,
                    fresh_backend_ports: request.fresh_backend_ports,
                }
            }
        };
        // However this start arrived — spent demand, a restart's second half,
        // or a command from a client — it is `Starting` from here. Idempotent:
        // the paths that announce themselves have already said so.
        let StartRequest {
            mode,
            intent,
            fresh_backend_ports,
        } = start_request;
        owner.set(super::ServiceState::Starting);
        scheduled_start = matches!(intent, super::ServiceStartIntent::Scheduled);

        // A new start supersedes the previous run's in-flight ready
        // outcome; forwarding it after this run's prepared report would be
        // exactly the stale-completion race this loop exists to prevent.
        ready_pending = None;

        // Build this start's context here, as the thing that owns the start.
        // `$(peer.KEY)` references resolve against the endpoint projection at
        // *this* moment, so a peer that moved to a new port since the last
        // start is picked up without anyone re-issuing the request.
        let mut context =
            match build_context(&name, &resolved, batch_built, &env, &last_docker_bindings) {
                Ok(context) => context,
                Err(message) => {
                    let decided = match intent {
                        super::ServiceStartIntent::Background => {
                            let decided = policy.decide(super::health::FailureKind::Prepare);
                            arm_backoff(&name, &env, &decided, &mut backoff, Some(&message));
                            decided
                        }
                        _ => {
                            policy.reset();
                            super::health::PolicyOutcome::None
                        }
                    };
                    // This start is over before it began, so land the phase
                    // here — nothing further in this loop will.
                    apply_failure_phase(&mut owner, &decided);
                    if report_tx
                        .send(super::ProcessReport::ServiceStartPrepared {
                            name: name.clone(),
                            intent,
                            result: Err(message),
                        })
                        .is_err()
                    {
                        return;
                    }
                    continue;
                }
            };

        // The proxy's per-spawn contribution: fresh ephemeral backend ports
        // on restart, the backend/public env vars, and the listenfd sockets
        // the child inherits.
        if let Some(p) = proxy.as_mut() {
            if fresh_backend_ports && let Err(error) = p.reallocate_ephemeral_ports().await {
                let message = format!("failed to allocate ephemeral ports: {error}");
                let decided = match intent {
                    super::ServiceStartIntent::Background => {
                        let decided = policy.decide(super::health::FailureKind::Prepare);
                        arm_backoff(&name, &env, &decided, &mut backoff, Some(&message));
                        decided
                    }
                    _ => {
                        policy.reset();
                        super::health::PolicyOutcome::None
                    }
                };
                apply_failure_phase(&mut owner, &decided);
                if report_tx
                    .send(super::ProcessReport::ServiceStartPrepared {
                        name: name.clone(),
                        intent,
                        result: Err(message),
                    })
                    .is_err()
                {
                    return;
                }
                continue;
            }
            context.resolved.env.extend(p.env_vars());
            context.resolved.env.extend(p.public_env_vars());
            context.listen_fds = p.listenfd_raw_fds();
            context.listen_fds_env = p.listenfd_env();
        }

        // Clone the context the worker borrows so the original can move into
        // the completion message afterwards.
        let context_for_worker = context.clone();
        let worker = start_service_worker(
            &env.base_dir,
            &env.pid_dir,
            env.platform,
            env.docker_client.as_ref(),
            &env.emitter,
            &name,
            context_for_worker.as_ref(),
            mode,
            service_writer.as_ref(),
        );
        tokio::pin!(worker);

        let mut superseded: Option<ServiceCommand> = None;
        let result = loop {
            tokio::select! {
                result = &mut worker => break result,
                next = rx.recv(), if !mailbox_closed => match next {
                    // Proxy directives apply immediately, not as a
                    // supersession. A directive queued *behind* a parked
                    // Start/Stop does run ahead of it, which is safe: the
                    // one order-sensitive pair the runner sends is
                    // ClearBackend-then-Stop, and that order is preserved
                    // because the directive comes first in the mailbox.
                    Some(next) => superseded = Some(next),
                    // Guarded so a closed mailbox doesn't spin this select.
                    None => mailbox_closed = true,
                },
            }
        };

        match superseded {
            Some(next) => {
                if let Ok(start_result) = result {
                    stop_superseded_start(&env, &name, context.as_ref(), start_result);
                }
                pending = Some(next);
            }
            None => {
                // Wire the spawn here, as its owner: extract what the runner
                // needs, keep the handle and the reader. A failed prepare
                // passes through untouched.
                let (wired, ready_parts) = match result {
                    Ok(start_result) => {
                        let (wired, exit_rx, cancel_rx) = wire_spawn(
                            output.as_ref(),
                            service_writer.as_ref(),
                            start_result,
                            proxy.as_ref(),
                            &mut held,
                            &mut reader,
                            &mut reader_eof,
                            &mut monitor_cancel,
                            &mut osc_sink,
                        )
                        .await;
                        // Resolution reads this spawn's live proxy and docker
                        // state — authoritative only after the spawn, which is
                        // why it happens here and not at queue time.
                        let ready = resolve_supervisor_ready(
                            &context.resolved,
                            proxy.as_ref(),
                            &wired.docker_port_bindings,
                        );
                        // Captured now, while the probe target is resolved.
                        ready_description = describe_ready(ready.as_ref());
                        // Remember the mapping so a restart can request the
                        // same host ports. This supervisor produced them, so
                        // its copy is the authoritative one.
                        last_docker_bindings = wired.docker_port_bindings.clone();
                        // The crash ceiling measures from here.
                        spawned_at = Some(std::time::Instant::now());
                        reached_ready = false;
                        ready_failed = false;
                        (Ok(Box::new(wired)), Some((ready, exit_rx, cancel_rx)))
                    }
                    Err(message) => (Err(message), None),
                };
                // A start that could not be prepared is a failure like any
                // other *unless the build tool refused*: retrying a build
                // recompiles sources that have not changed, so no amount of
                // backoff can change the answer. Only a background start is
                // retried at all — the others have someone waiting on a reply.
                let prepare_policy = match (&wired, &intent) {
                    (Err(failure), super::ServiceStartIntent::Background)
                        if !failure.from_build =>
                    {
                        let decided = policy.decide(super::health::FailureKind::Prepare);
                        arm_backoff(&name, &env, &decided, &mut backoff, Some(&failure.message));
                        decided
                    }
                    (Err(_), _) => {
                        policy.reset();
                        backoff = None;
                        super::health::PolicyOutcome::None
                    }
                    (Ok(_), _) => super::health::PolicyOutcome::None,
                };
                // Custody and phase move together. `Running` means "this
                // supervisor holds a process"; the runtime detail beside it is
                // the record of *which*, not a copy of one.
                match &wired {
                    Ok(w) => {
                        owner.set_runtime(Some(crate::state_store::ServiceRuntime {
                            pid: w.pgid,
                            docker: w.identity == super::ServiceHandleIdentity::Docker,
                            docker_ports: crate::docker::describe_port_bindings(
                                &w.docker_port_bindings,
                            ),
                        }));
                        owner.set(super::ServiceState::Running);
                    }
                    Err(_) => apply_failure_phase(&mut owner, &prepare_policy),
                }
                if report_tx
                    .send(super::ProcessReport::ServiceStartPrepared {
                        name: name.clone(),
                        intent,
                        result: wired.map_err(|failure| failure.message),
                    })
                    .is_err()
                {
                    return;
                }
                // Start the ready check — or, with none configured, be ready
                // now. A service with no check has nothing to wait for, so the
                // phase moves here rather than through a probe that would
                // report success immediately.
                match ready_parts {
                    Some((Some(ready), exit_rx, cancel_rx)) => {
                        ready_pending = Some(spawn_ready_racer(
                            ready,
                            exit_rx,
                            cancel_rx,
                            health_tx.clone(),
                        ));
                    }
                    Some((None, ..)) => {
                        policy.on_ready();
                        backoff = None;
                        reached_ready = true;
                        if scheduled_start && let Some(proxy) = proxy.as_ref() {
                            proxy.set_backend();
                        }
                        owner.transition(super::ServiceState::Ready, false);
                        if scheduled_start {
                            env.emitter.service_event(&name, &ready_description);
                        } else {
                            // A checkless restart announces itself;
                            // "restarting..." already said a cycle began.
                            env.emitter.service_event(&name, "restarted");
                        }
                        scheduled_start = false;
                    }
                    None => {}
                }
            }
        }
    }
}

/// A rebuild cycle in flight.
struct CycleState {
    /// A watched file changed since this cycle began.
    stale: bool,
}

/// What a settled batch outcome asks the loop to do next.
enum CycleNext {
    /// The cycle is over; nothing to start.
    Done,
    /// Stop and restart into the artifact.
    Restart,
    /// Sources changed while this cycle ran, so its artifact is already out
    /// of date: run another one now.
    ///
    /// The file watcher used to do this, by holding the item in `Rebuilding`
    /// and re-firing when the completion came back. Ending a cycle is a fact
    /// this supervisor has first and the watcher only ever learned second
    /// hand.
    Again,
}

/// Ask the build manager for this service's artifact, and tell the scheduler
/// a build is under way. Returns whether a request is now outstanding.
///
/// Sent when the supervisor is *constructed*, not when its gate opens.
/// An artifact does not depend on postgres listening, so building at gate-open
/// would serialise every build along the dependency chain — and hand bazel one
/// invocation per service instead of one for the whole workspace. Dependencies
/// gate *running*.
///
/// The report goes first so that "a build is outstanding" is never claimed
/// without the scheduler having been told; a dead scheduler means this
/// supervisor is about to end anyway.
fn request_artifact(
    name: &str,
    resolved: &crate::config::ResolvedService,
    env: &StartEnv,
    outcome: &mpsc::UnboundedSender<crate::build_tool::batch::PrepareOutcome>,
) -> bool {
    let working_dir = super::paths::working_dir_for(&env.base_dir, resolved.dir.as_deref());
    let ignore = super::paths::resolve_watch_ignore_patterns(
        &working_dir,
        &resolved.ignore,
        &env.base_dir,
        &env.global_watch_ignore,
    );
    env.batcher_tx
        .send(crate::build_tool::batcher::BatchRequest::QueuePrepare {
            item: Box::new(crate::build_tool::batch::BatchBuildItem {
                name: name.to_string(),
                kind: super::ProcessKind::Service,
                bazel: resolved
                    .bazel_config()
                    .cloned()
                    .map(|b| b.with_workspace_default(env.bazel_config.as_deref())),
                watch_enabled: resolved.build_tool_watch_enabled(),
                working_dir,
                ignore,
            }),
            outcome: outcome.clone(),
        })
        .is_ok()
}

/// Ask the build manager to re-resolve this service's watch paths.
///
/// The registrations come back through the watcher, applied by the manager
/// that resolved them; what lands on [`RequeryOutcome`] is only whether they
/// changed, which is all this supervisor needs to decide about its process.
fn request_requery(
    name: &str,
    resolved: &crate::config::ResolvedService,
    env: &StartEnv,
    outcome: &mpsc::UnboundedSender<crate::build_tool::batch::RequeryOutcome>,
) {
    let working_dir = super::paths::working_dir_for(&env.base_dir, resolved.dir.as_deref());
    let ignore_patterns = super::paths::resolve_watch_ignore_patterns(
        &working_dir,
        &resolved.ignore,
        &env.base_dir,
        &env.global_watch_ignore,
    );
    let _ = env
        .batcher_tx
        .send(crate::build_tool::batcher::BatchRequest::QueueRequery {
            item: crate::build_tool::batch::GraphRequeryRequestItem {
                name: name.to_string(),
                kind: super::ProcessKind::Service,
                bazel: resolved.bazel_config().cloned(),
                watch_enabled: resolved.build_tool_watch_enabled(),
                working_dir,
                ignore_patterns,
                global_watch_ignore: env.global_watch_ignore.clone(),
            },
            outcome: outcome.clone(),
        });
}

/// Capture what the batcher needs to rebuild this service. Resolved config is
/// fixed after construction, so a spec built now equals one built at flush
/// time — which is what frees the batcher from reading anyone's state.
fn rebuild_spec_for(
    name: &str,
    resolved: &crate::config::ResolvedService,
    env: &StartEnv,
) -> crate::build_tool::batcher::RebuildSpec {
    use crate::build_tool::batcher::RebuildSpec;
    match &resolved.kind {
        Some(crate::config::ServiceKind::Bazel(bazel)) => {
            RebuildSpec::Bazel(crate::build_tool::batch::BazelRebuildItem {
                name: name.to_string(),
                target: bazel.target.clone(),
                working_dir: super::paths::working_dir_for(&env.base_dir, resolved.dir.as_deref()),
                config: bazel.config.clone().or_else(|| env.bazel_config.clone()),
            })
        }
        _ => RebuildSpec::Plain {
            name: name.to_string(),
        },
    }
}

/// Ask the batcher to build this service, forced or coalesced.
///
/// Awaiting the forced reply inline is safe: the batcher never blocks on a
/// send (its outcome channels are unbounded), so the answer is immediate.
async fn queue_build(
    env: &StartEnv,
    spec: crate::build_tool::batcher::RebuildSpec,
    request: &RebuildRequest,
    outcome: &mpsc::UnboundedSender<crate::build_tool::batcher::RebuildItemOutcome>,
) -> Result<(), String> {
    use crate::build_tool::batcher::BatchRequest;
    let gone = || "build batcher is shutting down".to_string();
    if !request.forced {
        return env
            .batcher_tx
            .send(BatchRequest::QueueRebuild {
                spec,
                outcome: outcome.clone(),
            })
            .map_err(|_| gone());
    }
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    env.batcher_tx
        .send(BatchRequest::ForceRebuild {
            spec,
            outcome: outcome.clone(),
            reply: reply_tx,
        })
        .map_err(|_| gone())?;
    match reply_rx.await {
        Ok(result) => result,
        Err(_) => Err(gone()),
    }
}

/// The follow-up cycle for sources that changed while one was running.
///
/// `Requested` rather than `FileChange`: the cycle it belongs to has already
/// ended, so there is nothing to fold into and it must actually start.
fn rebuild_again() -> ServiceCommand {
    ServiceCommand::Rebuild(RebuildRequest {
        forced: false,
        source: RebuildSource::Requested,
        reply: None,
    })
}

/// Decide what a batch outcome means for the cycle it belongs to.
///
/// This is the whole of the rebuild state machine, and it reads only state
/// this supervisor owns. The asymmetry between the arms is deliberate and
/// pinned by tests: `UpToDate` consults `artifact_ahead` but not staleness
/// (nothing was built, so nothing went stale), `Built` consults staleness and
/// *sets* `artifact_ahead`, and a pass-through build sets neither.
fn settle_cycle(
    name: &str,
    env: &StartEnv,
    outcome: crate::build_tool::batcher::RebuildItemOutcome,
    cycle: &mut Option<CycleState>,
    artifact_ahead: &mut bool,
    batch_built: &mut bool,
) -> CycleNext {
    use crate::build_tool::batcher::RebuildItemOutcome as Item;
    let stale = cycle.as_ref().is_some_and(|cycle| cycle.stale);
    let done = |cycle: &mut Option<CycleState>| {
        *cycle = None;
        CycleNext::Done
    };
    // Sources moved under this cycle; end it and start another on them.
    let again = |cycle: &mut Option<CycleState>| {
        *cycle = None;
        CycleNext::Again
    };

    match outcome {
        Item::Failed(message) => {
            // The old process keeps running: a failed build is no reason to
            // take away the version that works.
            env.emitter.service_error_event(name, &message);
            done(cycle)
        }
        Item::Built => {
            *batch_built = true;
            if stale {
                // Skip restarting into an artifact already known to be out of
                // date, but remember that the process is now behind a
                // successful build.
                *artifact_ahead = true;
                return again(cycle);
            }
            CycleNext::Restart
        }
        Item::NotBuilt => {
            if stale {
                return again(cycle);
            }
            CycleNext::Restart
        }
        Item::UpToDate => {
            if *artifact_ahead {
                env.emitter.service_debug_event(
                    name,
                    "up to date, but process is behind last build — restarting",
                );
                return CycleNext::Restart;
            }
            env.emitter
                .service_debug_event(name, "skipped (no changes)");
            done(cycle)
        }
    }
}

/// End the held process and let its reader finish — the body both `Stop` and
/// `Restart` run.
///
/// Attach is unregistered *before* the reader is awaited: the registration
/// holds a PTY-gate sender and the gate holds the master's write half, so the
/// reader cannot see EOF until every sender is dropped. Awaiting it first
/// would deadlock against that until the 2s bound. Waiting for the drain at
/// all is what stops "stopped" outrunning the process's final lines.
#[allow(clippy::too_many_arguments)]
async fn run_stop(
    name: &str,
    env: &StartEnv,
    output: Option<&ProcessOutput>,
    held: &mut Option<service::ServiceHandle>,
    reader: &mut Option<tokio::task::JoinHandle<()>>,
    config: &ShutdownConfig,
    force: bool,
    wait_full_exit: bool,
    interrupt: Option<tokio::sync::watch::Receiver<bool>>,
) -> Result<(), String> {
    let result = match held.take() {
        Some(handle) => {
            let debug = service::StopDebug::new(name.to_string(), env.emitter.clone());
            match interrupt {
                Some(shutdown_rx) => service::stop_service_interruptibly(
                    handle,
                    Some(config),
                    wait_full_exit,
                    shutdown_rx,
                    Some(debug),
                )
                .await
                .map_err(|e| e.to_string()),
                None => {
                    service::stop_service(handle, Some(config), force, wait_full_exit, Some(debug))
                        .await
                        .map_err(|e| e.to_string())
                }
            }
        }
        // Nothing held: the process already exited and was reaped. Stopping
        // something stopped succeeds.
        None => Ok(()),
    };
    if let Some(output) = output {
        output.clear_attach().await;
    }
    if let Some(handle) = reader.take() {
        await_reader(handle).await;
    }
    result
}

/// Apply a runner proxy decision to the owned proxy. No-op for services
/// without one, and after `Shutdown`.
///
/// Whether a policy is a *change* is answered by the proxy itself rather than
/// by a shadow of what was last commanded — the owner knows. Narrating the
/// refusal edge belongs here for the same reason.
/// Keep the listener's connection policy in step with the service.
///
/// Queuing connections is right while a service is starting or restarting — a
/// client waits a moment and gets served. It is wrong once the service has
/// failed *with no process left*: nothing is going to read that socket, so the
/// connection is refused instead of left hanging.
///
/// The liveness half matters. `Failed` does not imply "the process is gone": a
/// service whose ready check fails keeps running under the default
/// `on_failure = "notify"` and may well be serving traffic. Don must not close
/// its clients' connections — and in listenfd mode must not race that live
/// child for accepts. Only a service that has both failed and lost its process
/// refuses.
///
/// Derived from the two things this supervisor already owns, which is why it
/// is a call and not a directive: a directive computed elsewhere could arrive
/// after the start it was meant to describe.
/// Whether a start someone asked for by name can be honoured.
///
/// Only [`ServiceStartIntent::Reply`] starts are admitted: the graph's own
/// starts already waited for permission, and a restart's second half is part
/// of an operation this supervisor is already committed to.
///
/// Every input is this supervisor's own — the phase it published, the process
/// it holds, the level it computes. The scheduler used to answer this from a
/// copy of the first, a projection of the second and a shared `AtomicBool`,
/// and this loop re-checked it anyway.
///
/// [`ServiceStartIntent::Reply`]: super::ServiceStartIntent::Reply
fn admit_requested_start(
    name: &str,
    owner: &PhaseOwner,
    holding: bool,
    deps: &[crate::config::Dependency],
    level: crate::gate::Gate,
    world: &crate::facts::FactsReader,
    intent: &super::ServiceStartIntent,
) -> Result<(), crate::command::CommandError> {
    use crate::command::CommandError;
    use crate::process::ServiceState;

    if !matches!(intent, super::ServiceStartIntent::Reply { .. }) {
        return Ok(());
    }
    // `Pending` is deliberately absent. It does not mean "busy", it means
    // "wanted, waiting for dependencies" — and overriding that wait is exactly
    // what an explicit start is for. It falls through to the dependency rule.
    if matches!(
        owner.phase,
        ServiceState::Building | ServiceState::Starting | ServiceState::Stopping
    ) {
        return Err(CommandError::InvalidState {
            name: name.to_string(),
            message: format!("cannot start while {:?}", owner.phase),
        });
    }
    if holding {
        return Err(CommandError::InvalidState {
            name: name.to_string(),
            message: "already running".to_string(),
        });
    }
    // An explicit start honours the dependency graph, on the *relaxed* rule: a
    // dependency still coming up is worth waiting for, so refuse and say so;
    // one that has settled never will be, so proceed — they asked for this by
    // name.
    if !super::Demand::Requested.permitted_by(level) {
        let snapshot = world.snapshot();
        let waiting: Vec<&str> = deps
            .iter()
            .filter(|dep| !snapshot.satisfied(&dep.name) && !snapshot.settled(&dep.name))
            .map(|dep| dep.name.as_str())
            .collect();
        return Err(CommandError::InvalidState {
            name: name.to_string(),
            message: format!("waiting for dependency '{}'", waiting.join("', '")),
        });
    }
    Ok(())
}

fn connection_policy_for(
    phase: crate::process::ServiceState,
    live: bool,
) -> crate::proxy::ConnectionPolicy {
    if phase == crate::process::ServiceState::Lazy {
        crate::proxy::ConnectionPolicy::LazyTrigger
    } else if phase.refuses_connections(live) {
        crate::proxy::ConnectionPolicy::Refuse
    } else {
        crate::proxy::ConnectionPolicy::Serve
    }
}

fn sync_connection_policy(
    name: &str,
    emitter: &LifecycleEmitter,
    proxy: &mut Option<crate::proxy::ServiceProxy>,
    owner: &PhaseOwner,
) {
    let Some(p) = proxy.as_mut() else { return };
    let policy = connection_policy_for(owner.phase, owner.runtime.is_some());
    if !p.set_policy(policy) {
        return;
    }
    // Only the refusal edge is worth a line, and it belongs in the normal log:
    // a dev staring at `ECONNRESET` in their browser shouldn't have to rerun
    // with `--verbose` to find out why.
    match policy {
        crate::proxy::ConnectionPolicy::Refuse => {
            emitter.service_error_event(name, "proxy refusing connections (service failed)");
        }
        _ => emitter.service_event(name, "proxy accepting connections again"),
    }
}

/// Await a lazy trigger without consuming the select slot on `None`.
async fn wait_demand(demand_rx: &mut Option<mpsc::Receiver<String>>) -> Option<String> {
    match demand_rx.as_mut() {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// Await the end-of-stream signal without consuming the slot on `None`.
async fn wait_eof(reader_eof: &mut Option<tokio::sync::oneshot::Receiver<()>>) {
    match reader_eof.as_mut() {
        // Err means the reader dropped the sender without sending — same
        // meaning: the stream is over.
        Some(rx) => {
            let _ = rx.await;
        }
        None => std::future::pending().await,
    }
}

/// Join the finished reader, bounded — a wedged sink must not hold the
/// supervisor (and with it shutdown) hostage.
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

/// Take ownership of a fresh spawn: extract everything the runner's
/// bookkeeping needs, start the output reader, activate the proxy backend,
/// and hold the handle.
#[allow(clippy::too_many_arguments)]
async fn wire_spawn(
    output: Option<&ProcessOutput>,
    service_writer: Option<&crate::output::ServiceWriter>,
    start_result: service::StartResult,
    proxy: Option<&crate::proxy::ServiceProxy>,
    held: &mut Option<service::ServiceHandle>,
    reader: &mut Option<tokio::task::JoinHandle<()>>,
    reader_eof: &mut Option<tokio::sync::oneshot::Receiver<()>>,
    monitor_cancel: &mut Option<tokio::sync::oneshot::Sender<()>>,
    osc_sink: &mut Option<crate::output::OscSinkHandle>,
) -> (
    ServiceWired,
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Receiver<()>,
) {
    let service::StartResult {
        mut handle,
        child_output,
    } = start_result;

    let (identity, pgid) = match &handle {
        service::ServiceHandle::Process(proc) => (
            super::state::ServiceHandleIdentity::Process { pgid: proc.pgid() },
            Some(proc.pgid()),
        ),
        service::ServiceHandle::Docker(_) => (super::state::ServiceHandleIdentity::Docker, None),
    };
    let docker_port_bindings = match &handle {
        service::ServiceHandle::Docker(docker) => docker.port_bindings().to_vec(),
        service::ServiceHandle::Process(_) => Vec::new(),
    };
    let pty = match &mut handle {
        service::ServiceHandle::Process(process) => process.take_pty_write(),
        service::ServiceHandle::Docker(_) => None,
    };
    // Held by this supervisor for the spawn's lifetime. Dropping it ends the
    // scanner and releases its PTY-gate sender, so it must be dropped
    // wherever the process is: stop, reap, or the next wire replacing it.
    *osc_sink = match (pty, output) {
        (Some(pty), Some(output)) => {
            // Feed the server-side screen from process start — a correct
            // repaint on attach requires having seen the setup sequences.
            // Matches the PTY's initial 80x24 size.
            output.register_emulator(80, 24).await;
            // The gate owns the write half for this spawn's lifetime; the
            // scanner, the attach registration and any bridges hold senders
            // into it — the last one dropping (scanner + registration both
            // clear at reap) is what ends the gate.
            let pty_input = crate::output::spawn_pty_gate(pty);
            let osc_sink = output.add_osc_sink(pty_input.clone()).await;
            // Attach goes through the output state, not the runner: register
            // this spawn's gate so any client can attach from here on.
            output.set_attach_pty(pty_input).await;
            Some(osc_sink)
        }
        _ => None,
    };

    // Fan the reader's end out twice: once to the ready check (races its
    // retry loop), once to this supervisor (the crash path). If there is no
    // registered output, both fire immediately — which is what the old
    // wiring did too.
    let (exit_tx, exit_rx) = tokio::sync::oneshot::channel();
    let (eof_tx, eof_rx) = tokio::sync::oneshot::channel();
    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
    *reader = service_writer.map(|writer| {
        let writer = writer.clone();
        tokio::spawn(async move {
            let _ = writer.process_stream(child_output).await;
            let _ = exit_tx.send(());
            let _ = eof_tx.send(());
        })
    });
    *reader_eof = Some(eof_rx);
    *held = Some(handle);
    *monitor_cancel = Some(cancel_tx);

    // Activate forwarding immediately — the proxy's connect loop retries
    // with backoff, so a service that hasn't bound its port yet just makes
    // early connections wait, exactly as when the runner did this on wiring.
    if let Some(p) = proxy {
        p.set_backend();
    }

    (
        ServiceWired {
            identity,
            pgid,
            docker_port_bindings,
            proxy_backend_env: proxy.map(|p| p.env_vars()),
        },
        exit_rx,
        cancel_rx,
    )
}

/// What the ready racer settles into, forwarded by the supervisor loop.
struct ReadyOutcome {
    success: bool,
    message: Option<String>,
    had_check: bool,
}

/// Await the racer's outcome without consuming the slot on `None`.
async fn wait_ready(
    ready_pending: &mut Option<tokio::sync::oneshot::Receiver<ReadyOutcome>>,
) -> Option<ReadyOutcome> {
    match ready_pending.as_mut() {
        Some(rx) => rx.await.ok(),
        None => std::future::pending().await,
    }
}

/// Resolve this spawn's ready check against its live proxy and docker
/// state — the same algorithm the runner's status path runs over shadows.
/// How to describe a ready check that just passed, in terms of what was
/// actually probed rather than what the config asked for.
///
/// Said by the supervisor that ran the probe. It used to be assembled by the
/// scheduler from the endpoint projection, which meant re-deriving what this
/// loop already resolved — and, once phase moved here, reading a phase that
/// had not arrived yet.
fn describe_ready(ready: Option<&crate::config::ReadyCheck>) -> String {
    match ready {
        Some(r) if r.tcp.is_some() => {
            format!("ready (tcp {})", r.tcp.as_deref().unwrap_or("unknown"))
        }
        Some(r) if r.http.is_some() => {
            format!("ready (http {})", r.http.as_deref().unwrap_or("unknown"))
        }
        Some(r) if r.exec.is_some() => "ready (exec)".to_string(),
        _ => "started".to_string(),
    }
}

fn resolve_supervisor_ready(
    resolved: &crate::config::ResolvedService,
    proxy: Option<&crate::proxy::ServiceProxy>,
    docker_bindings: &[crate::docker::DockerPortBinding],
) -> Option<crate::config::ReadyCheck> {
    let backend_env = proxy.map(|p| p.env_vars()).unwrap_or_default();
    let mut public_env = crate::docker::public_env_vars(docker_bindings);
    if let Some(p) = proxy {
        public_env.extend(p.public_env_vars());
    }
    let replacements = super::ready::port_replacements_for(
        proxy.map(|p| p.bindings()).unwrap_or(&[]),
        docker_bindings,
    );
    super::ready::resolve_ready_check(
        resolved.ready.as_ref(),
        &resolved.env,
        &backend_env,
        &public_env,
        &replacements,
    )
}

/// Run the ready check racing this spawn's end-of-stream, then start the
/// health monitor on success (its cancel sender lives with the supervisor,
/// so monitor lifetime stays tied to custody). The outcome goes back to the
/// supervisor loop, which forwards it on the report channel.
fn spawn_ready_racer(
    ready: crate::config::ReadyCheck,
    exit_rx: tokio::sync::oneshot::Receiver<()>,
    cancel_rx: tokio::sync::oneshot::Receiver<()>,
    health_tx: mpsc::UnboundedSender<bool>,
) -> tokio::sync::oneshot::Receiver<ReadyOutcome> {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let monitor_cancel_rx = ready.monitor.then_some(cancel_rx);
    tokio::spawn(async move {
        let result = tokio::select! {
            result = service::run_ready_check(&ready) => result,
            _ = exit_rx => Err(service::ServiceError::ProcessExitedDuringReadyCheck),
        };
        let success = result.is_ok();
        if success && let Some(cancel_rx) = monitor_cancel_rx {
            // Health transitions go to the supervisor, not the scheduler: the
            // restart policy they feed lives there now.
            tokio::spawn(async move {
                super::health::run_health_monitor(ready, health_tx, cancel_rx).await;
            });
        }
        let _ = ready_tx.send(ReadyOutcome {
            success,
            message: result.err().map(|e| e.to_string()),
            had_check: true,
        });
    });
    ready_rx
}

/// Reap the held process after its output ended, and report the exit.
///
/// Docker containers are held but not reaped here — the bollard stream's
/// EOF semantics aren't a death certificate, matching the old crash
/// watcher's docker exclusion.
#[allow(clippy::too_many_arguments)]
async fn reap_and_report(
    name: &str,
    owner: &mut PhaseOwner,
    held: &mut Option<service::ServiceHandle>,
    report_tx: &mpsc::UnboundedSender<super::ProcessReport>,
    env: &StartEnv,
    policy: &mut super::health::RestartPolicy,
    backoff: &mut Option<(tokio::time::Instant, u32)>,
    spawned_at: Option<std::time::Instant>,
    reached_ready: bool,
    ready_failed: bool,
) -> Result<(), ()> {
    if !matches!(held.as_ref(), Some(service::ServiceHandle::Process(_))) {
        return Ok(());
    }
    let Some(service::ServiceHandle::Process(mut proc)) = held.take() else {
        return Ok(());
    };
    let _pgid = proc.pgid();
    // The reader already hit end-of-stream, so this wait returns promptly.
    let status = proc.wait().await.ok();
    let clean = status.as_ref().is_some_and(|s| s.success());
    let decided = if clean {
        policy.reset();
        *backoff = None;
        super::health::PolicyOutcome::None
    } else if ready_failed {
        // This spawn already failed its ready check and was reported then.
        // Its exit is the tail of that failure, not a fresh one — counting it
        // again would double-charge the crash ceiling.
        super::health::PolicyOutcome::None
    } else {
        // Narrated here, beside the decision it causes, so "auto-restart in
        // 1s" can never print before the death that prompted it.
        let message = super::health::format_unexpected_exit(status);
        env.emitter.service_error_event(name, &message);
        let decided = policy.decide(super::health::FailureKind::Crash {
            lived: spawned_at.map(|at| at.elapsed()),
            reached_ready,
        });
        arm_backoff(name, env, &decided, backoff, Some(&message));
        decided
    };
    // Custody is over either way. A service can sit in `Failed` with its
    // process still alive — a ready check that failed under `notify` — so a
    // phase that is already `Failed` stays put and only the runtime clears.
    owner.set_runtime(None);
    if clean {
        owner.transition(crate::process::ServiceState::Stopped, false);
        env.emitter.service_event(name, "exited cleanly (status 0)");
    } else if owner.phase != crate::process::ServiceState::Failed {
        apply_failure_phase(owner, &decided);
    } else {
        owner.set_restart_pending(decided.restart_pending());
    }
    report_tx
        .send(super::ProcessReport::ServiceExited {
            name: name.to_string(),
        })
        .map_err(|_| ())
}

/// Narrate a policy decision and arm the timer it asks for.
///
/// Both halves live here because the decision does: a line that explains a
/// restart belongs next to the code that scheduled it.
fn arm_backoff(
    name: &str,
    env: &StartEnv,
    outcome: &super::health::PolicyOutcome,
    backoff: &mut Option<(tokio::time::Instant, u32)>,
    reason: Option<&str>,
) {
    use super::health::PolicyOutcome;
    let window = super::health::RAPID_CRASH_WINDOW.as_secs();
    match outcome {
        PolicyOutcome::None => {}
        PolicyOutcome::RestartScheduled {
            attempt,
            backoff_secs,
        } => {
            env.emitter.service_error_event(
                name,
                &format!(
                    "{} — auto-restart in {backoff_secs}s (attempt {attempt})",
                    reason.unwrap_or("failed")
                ),
            );
            *backoff = Some((
                tokio::time::Instant::now() + std::time::Duration::from_secs(*backoff_secs),
                *attempt,
            ));
        }
        PolicyOutcome::GaveUpStarting { attempts } => {
            *backoff = None;
            env.emitter.service_error_event(
                name,
                &format!(
                    "{} — giving up after {attempts} failed starts without becoming ready",
                    reason.unwrap_or("failed")
                ),
            );
        }
        PolicyOutcome::GaveUpCrashing { rapid_crashes } => {
            *backoff = None;
            env.emitter.service_error_event(
                name,
                &format!(
                    "crashed within {window}s of starting {rapid_crashes} times in a row — \
                     giving up (not auto-restarting)"
                ),
            );
        }
        PolicyOutcome::LazyRearm {
            give_up,
            rapid_crashes,
        } => {
            *backoff = None;
            if *give_up {
                env.emitter.service_error_event(
                    name,
                    &format!(
                        "crashed within {window}s of starting {rapid_crashes} times in a row — \
                         giving up; not re-arming the lazy trigger \
                         (run `don restart {name}` to retry)"
                    ),
                );
            } else if let Some(message) = reason {
                env.emitter.service_error_event(
                    name,
                    &format!("{message} (will retry on next connection)"),
                );
            }
        }
    }
}

/// This service's shutdown settings layered over the workspace defaults —
/// the supervisor's copy of the runner's `effective_shutdown_config`.
fn effective_shutdown(resolved: &crate::config::ResolvedService, env: &StartEnv) -> ShutdownConfig {
    resolved
        .shutdown
        .clone()
        .map(|shutdown| shutdown.merged_over(&env.shutdown))
        .unwrap_or_else(|| env.shutdown.clone())
}

/// Sleep until an armed auto-restart is due, or pend forever when none is.
async fn wait_backoff(backoff: &Option<(tokio::time::Instant, u32)>) {
    match backoff {
        Some((due, _)) => tokio::time::sleep_until(*due).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::build_tool::batch::{BatchBuildItem, PrepareOutcome};
    use crate::build_tool::batcher::BatchRequest;
    use crate::config::{LogConfig, Platform, ProxyEntry, ProxyMode};
    use crate::output::OutputManager;
    use crate::proxy::ServiceProxy;
    use std::time::Duration;

    /// An env that has *not* been released, matching a supervisor spawned
    /// before setup finishes: it holds its own permission back, so nothing
    /// self-starts and a test sees only what it asks for.
    async fn test_env() -> StartEnv {
        test_env_with_batcher().await.0
    }

    /// The same env, with the build manager's mailbox handed back so a test
    /// can see what this supervisor asks for — and answer it.
    async fn test_env_with_batcher() -> (StartEnv, mpsc::UnboundedReceiver<BatchRequest>) {
        let (env, batcher_rx, shutdown_tx) = env_with_batcher(false).await;
        std::mem::forget(shutdown_tx);
        (env, batcher_rx)
    }

    /// A released env: setup is done, so a service whose dependencies are
    /// satisfied — including one that declares none — may start itself.
    async fn released_env_with_batcher() -> (StartEnv, mpsc::UnboundedReceiver<BatchRequest>) {
        let (env, batcher_rx, shutdown_tx) = env_with_batcher(true).await;
        std::mem::forget(shutdown_tx);
        (env, batcher_rx)
    }

    /// The shutdown sender is returned rather than leaked: teardown is driven
    /// by this signal now, so a test that wants to exercise it needs to be
    /// able to raise it.
    async fn env_with_batcher(
        released: bool,
    ) -> (
        StartEnv,
        mpsc::UnboundedReceiver<BatchRequest>,
        tokio::sync::watch::Sender<bool>,
    ) {
        let log_config = LogConfig::Stdout;
        let services = [("svc", &log_config)];
        let output_manager = OutputManager::new(&services, tokio::io::sink())
            .await
            .unwrap();
        let (batcher_tx, batcher_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let env = StartEnv {
            bazel_config: None,
            batcher_tx,
            base_dir: std::env::temp_dir(),
            pid_dir: std::env::temp_dir(),
            platform: Platform::LinuxX86_64,
            docker_client: None,
            emitter: output_manager.clone_lifecycle_emitter(),
            shutdown: ShutdownConfig::default(),
            fallback_ports: false,
            global_watch_ignore: Vec::new(),
            shutdown_rx: shutdown_rx.clone(),
            endpoints: {
                let (writer, reader) = crate::endpoints::channel();
                writer.seed(std::iter::once("svc".to_string()));
                // Keep the writer alive for the reader's lifetime.
                std::mem::forget(writer);
                reader
            },
            facts: {
                let (aggregator, publishers, reader) = crate::facts::channel(std::iter::once((
                    "svc".to_string(),
                    crate::facts::ProcessFacts::for_service(
                        "svc",
                        crate::process::ServiceState::Pending,
                        None,
                        Vec::new(),
                    ),
                )));
                // Keep the write ends alive for the reader's lifetime.
                std::mem::forget(aggregator);
                std::mem::forget(publishers);
                reader
            },
            released: {
                let (tx, rx) = tokio::sync::watch::channel(released);
                std::mem::forget(tx);
                rx
            },
            secrets: crate::secrets::SecretStore::empty(),
        };
        (env, batcher_rx, shutdown_tx)
    }

    /// A `PhaseOwner` over a world the test controls, so the cascade can be
    /// driven by what the dependencies say rather than by running them.
    fn owner_over(
        deps: &[(&str, bool)],
        world: &[(&str, crate::facts::ProcessFacts)],
    ) -> (PhaseOwner, crate::facts::FactsReader) {
        let (aggregator, mut publishers, reader) = crate::facts::channel(
            std::iter::once((
                "api".to_string(),
                crate::facts::ProcessFacts::for_service(
                    "api",
                    crate::process::ServiceState::Pending,
                    None,
                    Vec::new(),
                ),
            ))
            .chain(
                world
                    .iter()
                    .map(|(name, facts)| ((*name).to_string(), facts.clone())),
            ),
        );
        std::mem::forget(aggregator);
        let owner = PhaseOwner {
            name: "api".to_string(),
            deps: deps
                .iter()
                .map(|(name, blocking)| crate::config::Dependency {
                    name: (*name).to_string(),
                    blocking: *blocking,
                })
                .collect(),
            world: reader.clone(),
            facts: publishers.remove("api").unwrap(),
            phase: crate::process::ServiceState::Pending,
            runtime: None,
            restart_pending: false,
        };
        (owner, reader)
    }

    fn stranded(roots: &[&str]) -> crate::facts::ProcessFacts {
        crate::facts::ProcessFacts::for_service(
            "dep",
            crate::process::ServiceState::Failed,
            None,
            Vec::new(),
        )
        .stranded_behind(roots.iter().map(|r| (*r).to_string()).collect())
    }

    fn up() -> crate::facts::ProcessFacts {
        crate::facts::ProcessFacts::for_service(
            "dep",
            crate::process::ServiceState::Ready,
            None,
            Vec::new(),
        )
    }

    /// Neither satisfied nor settled: still making progress, so waiting on it
    /// will end.
    fn coming_up() -> crate::facts::ProcessFacts {
        crate::facts::ProcessFacts::for_service(
            "dep",
            crate::process::ServiceState::Starting,
            None,
            Vec::new(),
        )
    }

    /// Failure blocking as the user sees it: a chain reports the ROOT cause,
    /// non-blocking edges never cascade, and a recovered root returns the
    /// dependent to waiting. Each supervisor answers for itself, reading one
    /// hop — the chain is already collapsed by the time it gets here.
    #[test]
    fn a_supervisor_strands_itself_behind_the_root_cause() {
        struct Case {
            name: &'static str,
            deps: Vec<(&'static str, bool)>,
            world: Vec<(&'static str, crate::facts::ProcessFacts)>,
            want_phase: crate::process::ServiceState,
            want_roots: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                name: "a healthy dependency leaves it waiting",
                deps: vec![("db", true)],
                world: vec![("db", up())],
                want_phase: crate::process::ServiceState::Pending,
                want_roots: vec![],
            },
            Case {
                name: "a failed dependency strands it, naming the root",
                deps: vec![("db", true)],
                world: vec![("db", stranded(&["db"]))],
                want_phase: crate::process::ServiceState::DependencyFailed,
                want_roots: vec!["db"],
            },
            Case {
                name: "a stranded dependency collapses to the root it inherited",
                deps: vec![("worker", true)],
                world: vec![("worker", stranded(&["db"]))],
                want_phase: crate::process::ServiceState::DependencyFailed,
                want_roots: vec!["db"],
            },
            Case {
                name: "a non-blocking edge never cascades",
                deps: vec![("worker", false)],
                world: vec![("worker", stranded(&["db"]))],
                want_phase: crate::process::ServiceState::Pending,
                want_roots: vec![],
            },
        ];

        for case in cases {
            let (mut owner, _reader) = owner_over(&case.deps, &case.world);
            owner.reconcile_dependencies();
            assert_eq!(owner.phase, case.want_phase, "{}", case.name);
            assert_eq!(
                owner.facts.current_roots(),
                case.want_roots
                    .iter()
                    .map(|r| (*r).to_string())
                    .collect::<Vec<_>>(),
                "{}",
                case.name
            );
        }
    }

    /// A root that recovers returns its dependents to waiting, rather than
    /// leaving them stranded behind a failure that is over.
    #[test]
    fn a_recovered_root_returns_a_stranded_dependent_to_waiting() {
        let (mut owner, _reader) = owner_over(&[("db", true)], &[("db", stranded(&["db"]))]);
        owner.reconcile_dependencies();
        assert_eq!(owner.phase, crate::process::ServiceState::DependencyFailed);

        // The same supervisor, now reading a world where the root is back.
        let (mut recovered, _reader) = owner_over(&[("db", true)], &[("db", up())]);
        recovered.phase = crate::process::ServiceState::DependencyFailed;
        let narration = recovered.reconcile_dependencies();
        assert_eq!(recovered.phase, crate::process::ServiceState::Pending);
        assert_eq!(
            narration,
            Some(String::new()),
            "recovery narrates as a re-queue, not a failure"
        );
    }

    /// A start asked for by name is admitted by the supervisor, from the phase
    /// it published, the process it holds and the level it computes. The
    /// scheduler used to answer this from a shadow of all three.
    #[test]
    fn a_requested_start_is_admitted_by_the_supervisor() {
        use crate::command::CommandError;
        use crate::process::ServiceState;

        struct Case {
            name: &'static str,
            phase: ServiceState,
            holding: bool,
            world: Vec<(&'static str, crate::facts::ProcessFacts)>,
            want: Option<&'static str>,
        }

        let deps = [("db", true)];
        let cases = vec![
            Case {
                name: "stopped with its dependency up",
                phase: ServiceState::Stopped,
                holding: false,
                world: vec![("db", up())],
                want: None,
            },
            Case {
                // `Pending` means "wanted, waiting" — overriding that wait is
                // exactly what an explicit start is for.
                name: "pending is not busy",
                phase: ServiceState::Pending,
                holding: false,
                world: vec![("db", up())],
                want: None,
            },
            Case {
                name: "mid-build",
                phase: ServiceState::Building,
                holding: false,
                world: vec![("db", up())],
                want: Some("cannot start while Building"),
            },
            Case {
                name: "already stopping",
                phase: ServiceState::Stopping,
                holding: true,
                world: vec![("db", up())],
                want: Some("cannot start while Stopping"),
            },
            Case {
                name: "already holding a process",
                phase: ServiceState::Ready,
                holding: true,
                world: vec![("db", up())],
                want: Some("already running"),
            },
            Case {
                // Worth waiting for: the wait will end.
                name: "a dependency still coming up is refused, and named",
                phase: ServiceState::Stopped,
                holding: false,
                world: vec![("db", coming_up())],
                want: Some("waiting for dependency 'db'"),
            },
            Case {
                // Waiting would never end, and they asked for it by name.
                name: "a settled dependency does not block an explicit start",
                phase: ServiceState::Stopped,
                holding: false,
                world: vec![("db", stranded(&["db"]))],
                want: None,
            },
        ];

        for case in cases {
            let (mut owner, world) = owner_over(&deps, &case.world);
            owner.phase = case.phase;
            let (reply_tx, _reply_rx) = tokio::sync::oneshot::channel();
            let got = admit_requested_start(
                "api",
                &owner,
                case.holding,
                &owner.deps.clone(),
                crate::gate::level(&owner.deps.clone(), &world.snapshot()),
                &world,
                &super::super::ServiceStartIntent::Reply { reply: reply_tx },
            );
            match (got, case.want) {
                (Ok(()), None) => {}
                (Err(CommandError::InvalidState { message, .. }), Some(want)) => {
                    assert_eq!(message, want, "{}", case.name);
                }
                (got, want) => panic!("{}: got {got:?} wanted {want:?}", case.name),
            }
        }
    }

    /// Queue-vs-refuse-vs-trigger, derived from the two things the supervisor
    /// owns. This used to be computed by the scheduler and sent as a
    /// directive, which meant a policy describing a start could arrive after
    /// that start had been superseded.
    #[test]
    fn the_connection_policy_follows_phase_and_custody() {
        use crate::process::ServiceState;
        use crate::proxy::ConnectionPolicy;

        struct Case {
            name: &'static str,
            phase: ServiceState,
            live: bool,
            want: ConnectionPolicy,
        }

        let cases = vec![
            Case {
                name: "a listening lazy service triggers on connect",
                phase: ServiceState::Lazy,
                live: false,
                want: ConnectionPolicy::LazyTrigger,
            },
            Case {
                name: "a starting service queues — the client waits and is served",
                phase: ServiceState::Starting,
                live: false,
                want: ConnectionPolicy::Serve,
            },
            Case {
                name: "a ready service serves",
                phase: ServiceState::Ready,
                live: true,
                want: ConnectionPolicy::Serve,
            },
            Case {
                // Nothing is going to read that socket, so hanging the client
                // is worse than refusing it.
                name: "failed with no process left refuses",
                phase: ServiceState::Failed,
                live: false,
                want: ConnectionPolicy::Refuse,
            },
            Case {
                // The liveness half: a ready check that failed under the
                // default `notify` leaves the process running and possibly
                // serving. Don must not close its clients' connections.
                name: "failed but still holding a process keeps serving",
                phase: ServiceState::Failed,
                live: true,
                want: ConnectionPolicy::Serve,
            },
            Case {
                name: "stranded behind a failed dependency refuses",
                phase: ServiceState::DependencyFailed,
                live: false,
                want: ConnectionPolicy::Refuse,
            },
        ];

        for case in cases {
            assert_eq!(
                connection_policy_for(case.phase, case.live),
                case.want,
                "{}",
                case.name
            );
        }
    }

    /// A write end for the harness, with the merge kept alive behind it so
    /// publishing succeeds and nothing has to drain it.
    fn test_publisher() -> crate::facts::FactsPublisher {
        let (aggregator, mut publishers, reader) = crate::facts::channel(std::iter::once((
            "svc".to_string(),
            crate::facts::ProcessFacts::for_service(
                "svc",
                crate::process::ServiceState::Pending,
                None,
                Vec::new(),
            ),
        )));
        std::mem::forget(aggregator);
        std::mem::forget(reader);
        publishers.remove("svc").unwrap()
    }

    /// A minimal service config for the supervisor harness.
    fn test_resolved() -> crate::config::ResolvedService {
        let config: crate::config::Config = "[services.svc]\nrun = { cmd = \"true\" }\n"
            .parse()
            .unwrap();
        config
            .services
            .get("svc")
            .unwrap()
            .resolve(Platform::LinuxX86_64)
    }

    /// A bazel-managed service — one that cannot spawn until the build
    /// manager has produced its artifact.
    fn bazel_resolved(lazy: bool) -> crate::config::ResolvedService {
        let config: crate::config::Config = "[services.svc]\nbazel.target = \"//svc:svc\"\n"
            .parse()
            .unwrap();
        let mut resolved = config
            .services
            .get("svc")
            .unwrap()
            .resolve(Platform::LinuxX86_64);
        resolved.lazy = lazy;
        resolved
    }

    struct Harness {
        tx: mpsc::UnboundedSender<ServiceCommand>,
        report_rx: mpsc::UnboundedReceiver<super::super::ProcessReport>,
        handle: tokio::task::JoinHandle<()>,
    }

    async fn spawn_harness(assets: Option<ProxyAssets>) -> Harness {
        let (tx, rx) = mpsc::unbounded_channel();
        let (report_tx, report_rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(supervise(
            "svc".to_string(),
            rx,
            test_env().await,
            None,
            report_tx,
            Arc::new(AtomicBool::new(false)),
            assets,
            Some(test_resolved()),
            Vec::new(),
            Some(test_publisher()),
        ));
        Harness {
            tx,
            report_rx,
            handle,
        }
    }

    async fn bind_env_proxy(lazy_tx: Option<mpsc::Sender<String>>) -> ServiceProxy {
        let entries = vec![ProxyEntry {
            listen: "127.0.0.1:0".to_string(),
            mode: ProxyMode::Env("PORT".to_string()),
        }];
        let log_config = LogConfig::Stdout;
        let services = [("svc", &log_config)];
        let output_manager = OutputManager::new(&services, tokio::io::sink())
            .await
            .unwrap();
        ServiceProxy::bind(
            &entries,
            false,
            lazy_tx,
            "svc",
            output_manager.clone_lifecycle_emitter(),
        )
        .await
        .unwrap()
    }

    /// Take the artifact request this supervisor made, or report that it
    /// never made one.
    async fn next_prepare(
        batcher_rx: &mut mpsc::UnboundedReceiver<BatchRequest>,
    ) -> Option<(BatchBuildItem, mpsc::UnboundedSender<PrepareOutcome>)> {
        match tokio::time::timeout(Duration::from_secs(2), batcher_rx.recv()).await {
            Ok(Some(BatchRequest::QueuePrepare { item, outcome })) => Some((*item, outcome)),
            Ok(Some(_)) => panic!("expected a preparation request"),
            Ok(None) | Err(_) => None,
        }
    }

    /// **Dependencies gate running, not building.** A supervisor asks for its
    /// artifact the moment it is constructed — before any gate has been
    /// published, and without one at all here — because that is what lets one
    /// `bazel build` cover the whole workspace. Asking at gate-open would
    /// serialise every build along the dependency chain.
    ///
    /// A lazy service is the one exception, and for the reason that makes the
    /// rule: nothing wants it yet. It asks on its first connection.
    #[tokio::test]
    async fn an_artifact_is_asked_for_at_construction_but_lazily_on_demand() {
        struct Case {
            name: &'static str,
            lazy: bool,
            /// Whether a request is expected before any demand arrives.
            want_eager: bool,
        }
        let cases = [
            Case {
                name: "an ordinary bazel service builds immediately",
                lazy: false,
                want_eager: true,
            },
            Case {
                name: "a lazy bazel service waits for a connection",
                lazy: true,
                want_eager: false,
            },
        ];

        for case in cases {
            let (env, mut batcher_rx) = test_env_with_batcher().await;
            let (lazy_tx, demand_rx) = mpsc::channel(16);
            let proxy = bind_env_proxy(Some(lazy_tx.clone())).await;
            let (_tx, rx) = mpsc::unbounded_channel();
            let (report_tx, _report_rx) = mpsc::unbounded_channel::<super::super::ProcessReport>();
            let handle = tokio::spawn(supervise(
                "svc".to_string(),
                rx,
                env,
                None,
                report_tx,
                Arc::new(AtomicBool::new(false)),
                Some(ProxyAssets {
                    proxy,
                    demand_rx: Some(demand_rx),
                }),
                Some(bazel_resolved(case.lazy)),
                Vec::new(),
                Some(test_publisher()),
            ));

            let eager = next_prepare(&mut batcher_rx).await;
            assert_eq!(eager.is_some(), case.want_eager, "{}", case.name);
            if let Some((item, _)) = &eager {
                assert_eq!(item.name, "svc", "{}", case.name);
                assert_eq!(
                    item.bazel.as_ref().map(|bazel| bazel.target.as_str()),
                    Some("//svc:svc"),
                    "{}",
                    case.name
                );
            }

            if !case.want_eager {
                lazy_tx.send("svc".to_string()).await.unwrap();
                assert!(
                    next_prepare(&mut batcher_rx).await.is_some(),
                    "{}: a first connection must ask for the artifact",
                    case.name
                );
            }
            handle.abort();
        }
    }

    /// An artifact is as much a precondition as a dependency, and it is the
    /// supervisor's to obtain — so an open gate does not start a service whose
    /// build is still running. That hold is also what puts the watch paths the
    /// build resolves in place before the first spawn: the build manager
    /// registers them with the watcher before it reports an outcome, and this
    /// supervisor does not move until that outcome arrives.
    #[tokio::test]
    async fn an_open_gate_does_not_start_a_service_still_waiting_on_its_build() {
        // Released, and this service declares no dependencies, so its level
        // is `Open` throughout — the hold under test is the artifact's, not
        // the graph's.
        let (env, mut batcher_rx) = released_env_with_batcher().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let (report_tx, mut report_rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(supervise(
            "svc".to_string(),
            rx,
            env,
            None,
            report_tx,
            Arc::new(AtomicBool::new(false)),
            None,
            Some(bazel_resolved(false)),
            Vec::new(),
            Some(test_publisher()),
        ));

        // Asking the build manager is the *first* thing it does — before any
        // report, because there is nothing to report until the artifact
        // exists.
        let (_, outcome) = next_prepare(&mut batcher_rx)
            .await
            .expect("a bazel service must ask for its artifact");

        // Dependencies allow it and demand is standing, yet nothing may spawn.
        assert!(
            tokio::time::timeout(Duration::from_secs(2), report_rx.recv())
                .await
                .is_err(),
            "a satisfied dependency must not start a service whose artifact does not exist yet"
        );

        outcome
            .send(PrepareOutcome::Ready { binary_path: None })
            .unwrap();

        match tokio::time::timeout(Duration::from_secs(5), report_rx.recv()).await {
            Ok(Some(super::super::ProcessReport::ServiceStartPrepared { name, .. })) => {
                assert_eq!(name, "svc");
            }
            _ => panic!("the start should follow the artifact, in that order"),
        }
        handle.abort();
    }

    /// The rebuild cycle's decision table, which is the whole of the
    /// staleness machinery now that one owner runs build, stop and spawn.
    ///
    /// These carry the semantics of two runner tests that drove the old
    /// fold directly (`stale_build_then_up_to_date_followup_still_restarts`
    /// and `deferred_restart_survives_watch_retrigger`). The asymmetry
    /// between the arms is the point and is easy to break:
    /// `UpToDate` consults `artifact_ahead` but not staleness — nothing was
    /// built, so nothing went stale — while `Built` consults staleness and is
    /// the only thing that *sets* `artifact_ahead`.
    #[tokio::test]
    async fn the_rebuild_cycle_decides_when_to_restart() {
        use crate::build_tool::batcher::RebuildItemOutcome as Item;

        struct Case {
            name: &'static str,
            /// Outcomes applied in order; `stale` marks the cycle stale
            /// before that step, as a `MarkStale` mid-cycle would.
            steps: Vec<(Item, bool)>,
            want_restart: bool,
            want_artifact_ahead: bool,
        }

        let cases = vec![
            Case {
                name: "a fresh build restarts into it",
                steps: vec![(Item::Built, false)],
                want_restart: true,
                want_artifact_ahead: false,
            },
            Case {
                name: "up to date with nothing pending is a no-op",
                steps: vec![(Item::UpToDate, false)],
                want_restart: false,
                want_artifact_ahead: false,
            },
            Case {
                name: "a build that went stale defers its restart",
                steps: vec![(Item::Built, true)],
                want_restart: false,
                want_artifact_ahead: true,
            },
            Case {
                // The pin: up-to-date is measured against the last *build*,
                // not the running process, so the follow-up must still
                // restart even though the build tool had nothing to do.
                name: "a stale build then up-to-date still restarts",
                steps: vec![(Item::Built, true), (Item::UpToDate, false)],
                want_restart: true,
                want_artifact_ahead: true,
            },
            Case {
                // A re-trigger must not lose the deferred restart: a new
                // cycle clears staleness but never `artifact_ahead`.
                name: "a deferred restart survives a re-trigger",
                steps: vec![
                    (Item::Built, true),
                    (Item::Built, true),
                    (Item::UpToDate, false),
                ],
                want_restart: true,
                want_artifact_ahead: true,
            },
            Case {
                name: "a failed build keeps the old process",
                steps: vec![(Item::Failed("boom".to_string()), false)],
                want_restart: false,
                want_artifact_ahead: false,
            },
            Case {
                name: "a pass-through build restarts",
                steps: vec![(Item::NotBuilt, false)],
                want_restart: true,
                want_artifact_ahead: false,
            },
            Case {
                name: "a stale pass-through does not",
                steps: vec![(Item::NotBuilt, true)],
                want_restart: false,
                want_artifact_ahead: false,
            },
        ];

        for case in cases {
            let env = test_env().await;
            let (_report_tx, _report_rx) = mpsc::unbounded_channel::<super::super::ProcessReport>();
            let mut artifact_ahead = false;
            let mut batch_built = false;
            let mut restarted = false;
            for (outcome, stale) in case.steps {
                // Each step is its own cycle, as a fresh Rebuild would be.
                let mut cycle = Some(CycleState { stale });
                restarted = matches!(
                    settle_cycle(
                        "svc",
                        &env,
                        outcome,
                        &mut cycle,
                        &mut artifact_ahead,
                        &mut batch_built,
                    ),
                    CycleNext::Restart
                );
                // A restart that happens brings the process up to date.
                if restarted {
                    artifact_ahead = false;
                }
            }
            assert_eq!(restarted, case.want_restart, "{}: restart", case.name);
            assert_eq!(
                artifact_ahead && !restarted,
                case.want_artifact_ahead && !case.want_restart,
                "{}: artifact_ahead",
                case.name
            );
        }
    }

    /// `Restart` is one operation and its reports say so: the stop half
    /// lands first, then the start is announced — the transition pair a
    /// restart has always shown, now produced by one owner instead of a stop
    /// the scheduler followed up on.
    ///
    /// The requester's reply travels *through*, unanswered. Callers read a
    /// stop reply as "the scheduler has applied this" (`don stop` returning
    /// means the service is no longer a satisfied dependency), which only the
    /// fold can promise.
    #[tokio::test]
    async fn restart_reports_its_stop_then_announces_the_start() {
        struct Case {
            name: &'static str,
            clear_backend_first: bool,
            with_reply: bool,
        }
        let cases = [
            Case {
                name: "plain restart",
                clear_backend_first: false,
                with_reply: true,
            },
            Case {
                name: "restart clearing the proxy backend first",
                clear_backend_first: true,
                with_reply: true,
            },
            Case {
                name: "restart nobody is waiting on",
                clear_backend_first: false,
                with_reply: false,
            },
        ];

        for case in cases {
            let mut harness = spawn_harness(None).await;
            // Settle into `Stopped` first. A restart someone asked for is
            // refused while the service is still `Pending` — it means "replace
            // what is running", and nothing is. `Done` notification skips
            // admission entirely, which is what teardown uses.
            let (settled_tx, settled_rx) = tokio::sync::oneshot::channel();
            harness
                .tx
                .send(ServiceCommand::Stop(StopRequest {
                    force: false,
                    wait_full_exit: false,
                    interrupt: None,
                    notify: StopNotify::Done(settled_tx),
                    reset_policy: true,
                }))
                .unwrap();
            settled_rx.await.unwrap();

            let (reply_tx, mut reply_rx) = tokio::sync::oneshot::channel();
            harness
                .tx
                .send(ServiceCommand::Restart(Box::new(RestartRequest {
                    wait_full_exit: false,
                    interrupt: None,
                    clear_backend_first: case.clear_backend_first,
                    start_mode: ServiceStartMode::Full,
                    fresh_backend_ports: false,
                    intent: ServiceStartIntent::Background,
                    reply: case.with_reply.then_some(reply_tx),
                    announce_restarting: false,
                    reset_policy: true,
                })))
                .unwrap();

            let carried = match harness.report_rx.recv().await {
                Some(super::super::ProcessReport::ServiceStopComplete {
                    name,
                    result,
                    reply,
                }) => {
                    assert_eq!(name, "svc", "{}", case.name);
                    // Nothing is held, so stopping succeeds trivially.
                    assert!(result.is_ok(), "{}: {result:?}", case.name);
                    assert_eq!(reply.is_some(), case.with_reply, "{}", case.name);
                    reply
                }
                _ => panic!("{}: expected the stop half first", case.name),
            };
            if case.with_reply {
                // Unanswered while its sender is still alive — proof the
                // supervisor handed it on rather than resolving it.
                assert!(
                    matches!(
                        reply_rx.try_recv(),
                        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
                    ),
                    "{}: the supervisor answered a reply the fold owes",
                    case.name
                );
            }
            drop(carried);

            match harness.report_rx.recv().await {
                Some(super::super::ProcessReport::ServiceStartPrepared { name, .. }) => {
                    assert_eq!(name, "svc", "{}", case.name);
                }
                _ => panic!("{}: expected the start to be announced", case.name),
            }
            harness.handle.abort();
        }
    }

    /// A supervisor tears itself down on the shutdown signal, and the listener
    /// it owns goes with it.
    ///
    /// Nothing asks it to. The proxy outlives individual processes — that is
    /// what makes zero-downtime restart possible — but it does not outlive the
    /// stack, and the only thing that can end it is the supervisor holding it.
    #[tokio::test]
    async fn a_supervisor_drops_its_listener_when_the_stack_shuts_down() {
        // The proxy's ephemeral backend port is allocated bind-and-drop, so
        // any other process (or parallel test) can steal it before the
        // stand-in backend below rebinds it. A steal is a setup failure, not a
        // regression — retry with a fresh proxy.
        const SETUP_ATTEMPTS: usize = 10;
        let mut attempt = 0;
        let (proxy, backend) = loop {
            attempt += 1;
            let proxy = bind_env_proxy(None).await;
            let backend_port: u16 = proxy.env_vars().get("PORT").unwrap().parse().unwrap();
            match tokio::net::TcpListener::bind(("127.0.0.1", backend_port)).await {
                Ok(listener) => break (proxy, listener),
                Err(error) => {
                    assert!(
                        attempt < SETUP_ATTEMPTS,
                        "could not claim backend port: {error}"
                    );
                }
            }
        };
        let public_addr = proxy.view().bindings[0].bound_addr;

        // A stand-in service on the ephemeral backend port.
        let backend_task = tokio::spawn(async move {
            while let Ok((mut conn, _)) = backend.accept().await {
                use tokio::io::AsyncWriteExt;
                let _ = conn.write_all(b"x").await;
            }
        });
        let (env, _batcher_rx, shutdown_tx) = env_with_batcher(false).await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let (report_tx, _report_rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(supervise(
            "svc".to_string(),
            rx,
            env,
            None,
            report_tx,
            Arc::new(AtomicBool::new(false)),
            Some(ProxyAssets {
                proxy,
                demand_rx: None,
            }),
            Some(test_resolved()),
            // No dependents, so nothing to wait for.
            Vec::new(),
            Some(test_publisher()),
        ));

        assert!(
            tokio::net::TcpStream::connect(public_addr).await.is_ok(),
            "the listener should be up before shutdown"
        );
        shutdown_tx.send(true).unwrap();

        // Teardown is asynchronous; poll until the listener is gone.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the listener outlived the shutdown signal"
            );
            if tokio::net::TcpStream::connect(public_addr).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        backend_task.abort();
        handle.abort();
    }

    /// A lazy proxy's trigger reaches the runner as a demand report, and a
    /// closed trigger channel leaves the supervisor alive.
    #[tokio::test]
    async fn lazy_trigger_forwards_as_demand_report() {
        let (lazy_tx, demand_rx) = mpsc::channel(16);
        let proxy = bind_env_proxy(Some(lazy_tx.clone())).await;
        let mut harness = spawn_harness(Some(ProxyAssets {
            proxy,
            demand_rx: Some(demand_rx),
        }))
        .await;

        lazy_tx.send("svc".to_string()).await.unwrap();
        let report = tokio::time::timeout(Duration::from_secs(5), harness.report_rx.recv())
            .await
            .expect("demand should be forwarded")
            .expect("report channel open");
        match report {
            super::super::ProcessReport::Demand { name, .. } => assert_eq!(name, "svc"),
            _ => panic!("expected a demand report"),
        }

        // Dropping every trigger sender must not end the supervisor: it still
        // has a mailbox to answer.
        drop(lazy_tx);
        harness.tx.send(ServiceCommand::BuildGraphChanged).unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !harness.handle.is_finished(),
            "supervisor must survive its demand channel closing"
        );
        harness.handle.abort();
    }
}
