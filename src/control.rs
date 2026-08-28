//! What a client may ask a process to do.
//!
//! A start, a stop, a restart or a run is a request to a *process*. It used
//! to travel through the runner's command loop because that is where the
//! state behind the pre-checks lived — but the supervisors own their
//! processes now, and the state a pre-check reads is published
//! ([`StateReader`](crate::state_store::StateReader)) or fixed at
//! construction ([`ProcessCatalog`]).
//!
//! So this is the control plane: a cloneable handle the API server holds,
//! which addresses supervisors directly and leaves the runner's mailbox to
//! the things that are genuinely scheduling decisions.
//!
//! # Who answers what
//!
//! `start` and `stop` are addressed straight at the supervisor. Every
//! pre-check they need — `cannot start while Stopping`, `already running`,
//! `not running`, `waiting for dependency 'x'` — is about the phase it
//! published, the process it holds, or the level it computes. The scheduler
//! used to answer all three from copies, and the supervisor re-checked anyway.
//!
//! `stop` and `restart` dispatch on kind: a running task is a process too, and
//! the supervisor that owns its run is the one that can end it.
//!
//! The reply still rides *down* with the request and comes back up on the
//! report channel, so it keeps meaning what it always meant: the scheduler has
//! applied this. `socket_test` reads a stop reply as "no longer a satisfied
//! dependency", and that is only true once the merge has absorbed `Stopped` —
//! which the runner guarantees by draining facts before handling any report.
//!
//! Name resolution never needed the loop either: a typo answers 404 from
//! [`ProcessCatalog`] without waking anything.
//!
//! Every verb is addressed at a supervisor now. `Status` and `Shutdown` are
//! all that is left of the runner's mailbox: a read of the projection, and
//! "begin teardown".

use std::collections::{HashMap, HashSet};

use crate::command::{CommandError, CommandResult};
use crate::process::task_supervisor;
use crate::runner::RunnerCommand;

/// Every configured process name and what kind it is.
///
/// Fixed at construction, so "is this a service?" and "is this a task?" are
/// answered without waking the scheduler — which is what keeps a 404 for a
/// typo'd name off the runner's critical path.
pub(crate) struct ProcessCatalog {
    /// Every *configured* service and task, active in this profile or not.
    /// Control commands (`start`/`stop`/`restart`) resolve against these.
    configured_services: HashSet<String>,
    configured_tasks: HashSet<String>,
    /// The profile-selected subset the supervisor registries were built from.
    /// `run` resolves against these.
    active_services: HashSet<String>,
    active_tasks: HashSet<String>,
}

impl ProcessCatalog {
    pub(crate) fn new(
        config: &crate::config::Config,
        active_services: HashSet<String>,
        active_tasks: HashSet<String>,
    ) -> Self {
        Self {
            configured_services: config.services.keys().cloned().collect(),
            configured_tasks: config.tasks.keys().cloned().collect(),
            active_services,
            active_tasks,
        }
    }

    /// Resolve a name for a service-only control command.
    ///
    /// Checks *configured* services, then configured tasks — so `don start` on
    /// a task is a 400 "that's a task", not a 404. A configured service the
    /// active profile excluded resolves fine here and fails later as "not
    /// running", which is a 409; that asymmetry is deliberate and pinned by
    /// `server_test`.
    ///
    /// Only the verbs a task has no version of come through here. `stop` and
    /// `restart` check [`Self::is_task`] first and address the task registry
    /// instead.
    pub(crate) fn require_service(&self, name: &str) -> Result<(), CommandError> {
        if self.configured_services.contains(name) {
            return Ok(());
        }
        if self.configured_tasks.contains(name) {
            return Err(CommandError::NotAService {
                name: name.to_string(),
            });
        }
        Err(CommandError::UnknownService {
            name: name.to_string(),
        })
    }

    /// Resolve a name for `don run`.
    ///
    /// Checks *active* services then active tasks — a different set and a
    /// different order from [`Self::require_service`]. Both are pinned; do
    /// not unify them.
    pub(crate) fn require_task(&self, name: &str) -> Result<(), CommandError> {
        if self.active_services.contains(name) {
            return Err(CommandError::NotATask {
                name: name.to_string(),
            });
        }
        if self.active_tasks.contains(name) {
            return Ok(());
        }
        Err(CommandError::UnknownTask {
            name: name.to_string(),
        })
    }

    /// Whether `name` is a task, for the polymorphic `stop` and `restart`
    /// dispatch.
    pub(crate) fn is_task(&self, name: &str) -> bool {
        self.configured_tasks.contains(name)
    }
}

/// The runner is gone and nothing can be asked of it. Maps to 503.
#[derive(Debug)]
pub struct Unavailable;

/// A control request's outcome: the command's own result, or the runner
/// having gone away underneath it.
pub type ControlResult = Result<CommandResult, Unavailable>;

/// Cloneable handle for asking processes to do things.
///
/// Held by the API server. `pub` because `server::serve_api` is, but every
/// field is private — callers get the six verbs and nothing else.
#[derive(Clone)]
pub struct ProcessControl {
    catalog: std::sync::Arc<ProcessCatalog>,
    services: crate::process::registry::ProcessRegistry<
        crate::process::service_supervisor::ServiceCommand,
    >,
    tasks: crate::process::registry::ProcessRegistry<crate::process::task_supervisor::TaskCommand>,
    /// Set once teardown begins. Rides down with a manual stop so don shutting
    /// down cuts an in-flight grace period short instead of waiting it out —
    /// a stop with a 10s grace must not hold exit hostage for 10s.
    shutting_down: tokio::sync::watch::Receiver<bool>,
    cmd_tx: tokio::sync::mpsc::UnboundedSender<RunnerCommand>,
}

impl ProcessControl {
    pub(crate) fn new(
        catalog: std::sync::Arc<ProcessCatalog>,
        services: crate::process::registry::ProcessRegistry<
            crate::process::service_supervisor::ServiceCommand,
        >,
        tasks: crate::process::registry::ProcessRegistry<
            crate::process::task_supervisor::TaskCommand,
        >,
        shutting_down: tokio::sync::watch::Receiver<bool>,
        cmd_tx: tokio::sync::mpsc::UnboundedSender<RunnerCommand>,
    ) -> Self {
        Self {
            catalog,
            services,
            tasks,
            shutting_down,
            cmd_tx,
        }
    }

    /// Start a stopped service.
    ///
    /// Addressed straight at the supervisor, which admits or refuses it from
    /// the phase, the process and the dependency level it already owns. The
    /// reply rides down with the request and comes back on the report channel,
    /// so it still means what it always meant: the scheduler has applied this.
    pub async fn start(&self, name: &str) -> ControlResult {
        if let Err(e) = self.catalog.require_service(name) {
            return Ok(Err(e));
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let sent = self.services.get(name).is_some_and(|handle| {
            handle.request(crate::process::service_supervisor::ServiceCommand::Start(
                crate::process::service_supervisor::StartRequest {
                    mode: crate::process::service_worker::ServiceStartMode::Full,
                    intent: crate::process::ServiceStartIntent::Reply { reply: tx },
                    fresh_backend_ports: false,
                },
            ))
        });
        if !sent {
            // Configured but not in this profile's active set, so no
            // supervisor exists to ask. "Not running" is the pinned answer.
            return Ok(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "not running".to_string(),
            }));
        }
        rx.await.map_err(|_| Unavailable)
    }

    /// Stop a running service, or end a task's run.
    ///
    /// Like [`Self::start`], addressed at the supervisor: whether there is
    /// anything to stop, and whether stopping it is meaningful, are questions
    /// about the process it holds.
    ///
    /// Polymorphic like [`Self::restart`], and for the same reason — a task
    /// that is running is a process somebody may want to end, and `don stop
    /// <name>` is what they will type for it. A task used to answer
    /// [`CommandError::NotAService`] here, which said "not this verb" about
    /// the one verb that fits.
    pub async fn stop(&self, name: &str) -> ControlResult {
        if !self.catalog.is_task(name)
            && let Err(e) = self.catalog.require_service(name)
        {
            return Ok(Err(e));
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let sent = if self.catalog.is_task(name) {
            self.tasks.get(name).is_some_and(|handle| {
                handle
                    .request(crate::process::task_supervisor::TaskCommand::Stop { reply: Some(tx) })
            })
        } else {
            self.services.get(name).is_some_and(|handle| {
                handle.request(crate::process::service_supervisor::ServiceCommand::Stop(
                    crate::process::service_supervisor::StopRequest {
                        force: false,
                        wait_full_exit: false,
                        interrupt: Some(self.shutting_down.clone()),
                        notify: crate::process::service_supervisor::StopNotify::Reply(Some(tx)),
                        reset_policy: true,
                    },
                ))
            })
        };
        if !sent {
            return Ok(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "not running".to_string(),
            }));
        }
        rx.await.map_err(|_| Unavailable)
    }

    /// Restart a service, or re-run a task with its last parameters.
    pub async fn restart(&self, name: &str) -> ControlResult {
        if !self.catalog.is_task(name)
            && let Err(e) = self.catalog.require_service(name)
        {
            return Ok(Err(e));
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        // A task re-runs with the parameters its supervisor still holds; a
        // service stops what it holds and starts again. Both are one mailbox
        // item to the owner, which is what stops either interleaving with
        // anything else that process was asked to do.
        let sent = if self.catalog.is_task(name) {
            self.tasks.get(name).is_some_and(|handle| {
                handle.request(crate::process::task_supervisor::TaskCommand::Restart {
                    reply: Some(tx),
                })
            })
        } else {
            self.services.get(name).is_some_and(|handle| {
                handle.request(crate::process::service_supervisor::ServiceCommand::Restart(
                    Box::new(crate::process::service_supervisor::RestartRequest {
                        wait_full_exit: false,
                        interrupt: Some(self.shutting_down.clone()),
                        clear_backend_first: false,
                        start_mode: crate::process::service_worker::ServiceStartMode::Full,
                        fresh_backend_ports: false,
                        intent: crate::process::ServiceStartIntent::Background,
                        reply: Some(tx),
                        announce_restarting: false,
                        // An explicit request clears the failure history;
                        // only the policy's own retry keeps it.
                        reset_policy: true,
                    }),
                ))
            })
        };
        if !sent {
            return Ok(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "not running".to_string(),
            }));
        }
        rx.await.map_err(|_| Unavailable)
    }

    /// Force a rebuild, then restart.
    pub async fn hard_restart(&self, name: &str) -> ControlResult {
        if let Err(e) = self.catalog.require_service(name) {
            return Ok(Err(e));
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let sent = self.services.get(name).is_some_and(|handle| {
            handle.request(crate::process::service_supervisor::ServiceCommand::Rebuild(
                crate::process::service_supervisor::RebuildRequest {
                    forced: true,
                    source: crate::process::service_supervisor::RebuildSource::Requested,
                    reply: Some(tx),
                },
            ))
        });
        if !sent {
            return Ok(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "not running".to_string(),
            }));
        }
        rx.await.map_err(|_| Unavailable)
    }

    /// Run a task, optionally waiting for it to finish.
    ///
    /// Addressed at the supervisor, which owns every remaining pre-check: it
    /// holds the run that would make this one a duplicate, and the config the
    /// supplied params are resolved against. What is settled here is only what
    /// needs no state at all — the name, and whether the `--wait` spelling
    /// parses.
    pub async fn run_task(
        &self,
        name: &str,
        params: HashMap<String, String>,
        wait: bool,
        wait_timeout: Option<String>,
    ) -> ControlResult {
        if let Err(e) = self.catalog.require_task(name) {
            return Ok(Err(e));
        }
        let wait = wait || wait_timeout.is_some();
        let timeout = match wait_timeout {
            Some(spelling) => match crate::duration::parse_duration(&spelling) {
                Ok(duration) => Some((duration, spelling)),
                Err(e) => {
                    return Ok(Err(CommandError::InvalidParams {
                        name: name.to_string(),
                        message: format!("invalid wait timeout: {e}"),
                    }));
                }
            },
            None => None,
        };

        let (tx, rx) = tokio::sync::oneshot::channel();
        let reply = if wait {
            // The answer rides the run to its exit, which is what `--wait`
            // asked for.
            task_supervisor::RunReply::OnExit(task_supervisor::RunWait { reply: tx, timeout })
        } else {
            task_supervisor::RunReply::OnStart(tx)
        };
        let sent = self.tasks.get(name).is_some_and(|handle| {
            handle.request(task_supervisor::TaskCommand::Run(
                task_supervisor::RunRequest {
                    params,
                    mode: crate::process::task_worker::TaskRunMode::Triggered,
                    intent: crate::process::TaskRunIntent::Background,
                    reply: Some(reply),
                    start_message: Some("running (manual trigger)".to_string()),
                },
            ))
        });
        if !sent {
            return Ok(Err(CommandError::Failed {
                name: name.to_string(),
                message: "task supervisor is shutting down".to_string(),
            }));
        }
        rx.await.map_err(|_| Unavailable)
    }

    /// Begin graceful shutdown. Fire-and-forget: teardown narrates itself.
    pub fn shutdown(&self) -> Result<(), Unavailable> {
        self.cmd_tx
            .send(RunnerCommand::Shutdown)
            .map_err(|_| Unavailable)
    }

    /// A control plane whose runner is already gone, for router tests: every
    /// request answers `Unavailable`, so a 200 proves the response came from
    /// a projection rather than the command channel.
    #[cfg(test)]
    pub(crate) fn for_tests() -> Self {
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        drop(cmd_rx);
        Self::new(
            std::sync::Arc::new(ProcessCatalog {
                configured_services: HashSet::new(),
                configured_tasks: HashSet::new(),
                active_services: HashSet::new(),
                active_tasks: HashSet::new(),
            }),
            crate::process::registry::ProcessRegistry::empty(),
            crate::process::registry::ProcessRegistry::empty(),
            tokio::sync::watch::channel(false).1,
            cmd_tx,
        )
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn catalog() -> ProcessCatalog {
        let config: crate::config::Config = concat!(
            "[services.api]\nrun = { cmd = \"true\" }\n",
            "[services.excluded]\nrun = { cmd = \"true\" }\n",
            "[tasks.setup]\ncmd = \"true\"\n",
        )
        .parse()
        .unwrap();
        // `excluded` is configured but not active — a profile left it out.
        ProcessCatalog::new(
            &config,
            HashSet::from(["api".to_string()]),
            HashSet::from(["setup".to_string()]),
        )
    }

    /// The two lookups differ in both the set they read and the order they
    /// read it in, and each difference is a pinned HTTP status.
    #[test]
    fn control_and_run_resolve_names_differently() {
        struct Case {
            name: &'static str,
            process: &'static str,
            control: Result<(), &'static str>,
            run: Result<(), &'static str>,
        }

        let cases = vec![
            Case {
                name: "an active service",
                process: "api",
                control: Ok(()),
                run: Err("is a service"),
            },
            Case {
                name: "an active task, for a verb only a service has",
                process: "setup",
                control: Err("is a task"),
                run: Ok(()),
            },
            Case {
                name: "a configured service the profile excluded still \
                       resolves for control — it fails later as 'not running'",
                process: "excluded",
                control: Ok(()),
                run: Err("unknown task"),
            },
            Case {
                name: "a name nobody declared",
                process: "ghost",
                control: Err("unknown service"),
                run: Err("unknown task"),
            },
        ];

        let catalog = catalog();
        for case in cases {
            match (catalog.require_service(case.process), case.control) {
                (Ok(()), Ok(())) => {}
                (Err(e), Err(want)) => assert!(
                    e.to_string().contains(want),
                    "{}: control error was {e}",
                    case.name
                ),
                (got, want) => panic!("{}: control {got:?} wanted {want:?}", case.name),
            }
            match (catalog.require_task(case.process), case.run) {
                (Ok(()), Ok(())) => {}
                (Err(e), Err(want)) => assert!(
                    e.to_string().contains(want),
                    "{}: run error was {e}",
                    case.name
                ),
                (got, want) => panic!("{}: run {got:?} wanted {want:?}", case.name),
            }
        }
    }

    fn dead_control() -> ProcessControl {
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        drop(cmd_rx);
        ProcessControl::new(
            std::sync::Arc::new(catalog()),
            crate::process::registry::ProcessRegistry::empty(),
            crate::process::registry::ProcessRegistry::empty(),
            tokio::sync::watch::channel(false).1,
            cmd_tx,
        )
    }

    /// What a client can be told without waking anything: whether the name
    /// exists, and whether the `--wait` spelling parses. Neither reads state,
    /// so neither may report the stack as unavailable — a typo answering 503
    /// instead of 404 is the failure this pins.
    #[tokio::test]
    async fn checks_that_need_no_state_are_answered_locally() {
        let answered = dead_control()
            .run_task("ghost", HashMap::new(), false, None)
            .await
            .expect("a name check must not need a supervisor");
        assert!(
            answered.unwrap_err().to_string().contains("unknown task"),
            "a typo must answer as the command's error, not as 503"
        );

        let answered = dead_control()
            .run_task("setup", HashMap::new(), true, Some("banana".to_string()))
            .await
            .expect("parsing a duration must not need a supervisor");
        assert!(
            answered
                .unwrap_err()
                .to_string()
                .contains("invalid wait timeout"),
            "an unparseable --wait spelling is the caller's error"
        );
    }

    /// Every verb is addressed at a supervisor now, so a process this profile
    /// left out — or a stack that has already torn its supervisors down — has
    /// nothing to ask, and a dead runner is not what went wrong. Each answers
    /// as the command's own error rather than 503, which is the asymmetry
    /// `server_test` pins.
    #[tokio::test]
    async fn a_verb_with_no_supervisor_answers_as_a_command_error() {
        for answered in [
            dead_control().start("excluded").await,
            dead_control().stop("excluded").await,
            dead_control().restart("excluded").await,
            dead_control().hard_restart("excluded").await,
            // A task with no mailbox: `stop` addresses the task registry for
            // it, so it takes the same route as an absent service rather than
            // refusing the verb.
            dead_control().stop("setup").await,
        ] {
            let answered =
                answered.expect("a missing supervisor must not read as the runner being gone");
            assert!(
                answered.unwrap_err().to_string().contains("not running"),
                "a service outside the active profile is not running"
            );
        }

        // A *task* resolves against the active set, so a missing mailbox here
        // means the supervisor has gone rather than never existed.
        let answered = dead_control()
            .run_task("setup", HashMap::new(), false, None)
            .await
            .expect("a missing supervisor must not read as the runner being gone");
        assert!(
            answered.unwrap_err().to_string().contains("shutting down"),
            "an active task with no mailbox has a supervisor that ended"
        );
    }
}
