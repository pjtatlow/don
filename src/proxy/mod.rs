//! TCP proxy — Don binds each configured `proxy` address and either forwards
//! to an ephemeral backend port (env mode) or hands the bound listener to
//! the service via `LISTEN_FDS` (listenfd mode).
//!
//! Each service with `proxy` entries gets a [`ServiceProxy`] that outlives
//! individual service restarts. For env entries, the proxy uses a `watch`
//! channel to track the current backend address, enabling atomic zero-
//! downtime switches. For listenfd entries, the bound listener's fd is
//! passed to the child — no forwarding at the don layer.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::io::unix::AsyncFd;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError, mpsc, watch};
use tokio::task::JoinHandle;

use crate::config::{ProxyEntry, ProxyMode};

const PROXY_FD_RESERVE: u64 = 128;
const DEFAULT_PROXY_CONNECTION_LIMIT: u64 = 16_384;
const MIN_PROXY_CONNECTION_LIMIT: u64 = 16;

static PROXY_CONNECTION_POOL: OnceLock<ProxyConnectionPool> = OnceLock::new();

#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("failed to bind proxy listener on '{addr}': {source}")]
    Bind {
        addr: String,
        source: std::io::Error,
    },
    #[error("failed to allocate ephemeral port: {0}")]
    EphemeralPort(std::io::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// A lazy-start request tagged with the connection cohort that produced it.
pub(crate) struct LazyProxyTrigger {
    /// Service whose proxy received the connection.
    pub(crate) service_name: String,
    /// Failure epoch observed when the connection was accepted.
    pub(crate) failure_epoch: u64,
}

/// A forwarding listener: don accepts on the public address and shuttles
/// bytes to/from a backend that the service itself binds. The backend
/// address is either ephemeral (don allocates, injects as env var) or
/// fixed (service binds a known port on its own).
struct ForwardListener {
    backend: ForwardBackend,
    backend_tx: watch::Sender<Option<SocketAddr>>,
    controller: Controller<ForwardControl>,
    accept_handle: JoinHandle<()>,
}

/// How the backend address for a [`ForwardListener`] is chosen.
enum ForwardBackend {
    /// Don allocated an ephemeral port and injected it into the service's
    /// env under `env_name`. The service must read the env var to bind.
    Ephemeral { env_name: String, addr: SocketAddr },
    /// Service binds a known fixed address on its own. Don just forwards.
    /// No env var injected.
    Fixed(SocketAddr),
}

impl ForwardBackend {
    fn addr(&self) -> SocketAddr {
        match self {
            Self::Ephemeral { addr, .. } => *addr,
            Self::Fixed(addr) => *addr,
        }
    }
}

/// A listenfd listener: don holds the bound public listener and passes its
/// fd to the child. Its controller watches POLLIN to trigger a lazy start and
/// temporarily accepts connections only while a failed service is unavailable.
struct ListenfdListener {
    listen_addr: SocketAddr,
    /// `std::net::TcpListener` (not tokio's) because we need `AsRawFd` and
    /// stable fd semantics for passing into the child via `LISTEN_FDS`.
    /// Wrapped in an `Arc` so the POLLIN watcher can hold its own handle
    /// that survives across re-arms.
    listener: std::sync::Arc<std::net::TcpListener>,
    controller: Controller<ListenfdControl>,
    control_handle: JoinHandle<()>,
}

/// What Don should do with connections on a forwarding listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ForwardControl {
    Accepting,
    Paused,
    Rejecting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ControllerCommand<Control> {
    control: Control,
    /// Incremented exactly once when a new service failure starts.
    ///
    /// Connections retain the epoch in which they were accepted. A later
    /// accepting command deliberately keeps the new epoch, so connections
    /// from the failed attempt close even if watch updates coalesce.
    failure_epoch: u64,
}

type ControllerStatus<Control> = Result<ControllerCommand<Control>, String>;

#[derive(Clone)]
struct Controller<Control> {
    command_tx: watch::Sender<ControllerCommand<Control>>,
    status_rx: watch::Receiver<ControllerStatus<Control>>,
}

impl<Control> Controller<Control>
where
    Control: Copy + PartialEq,
{
    fn set(&self, control: Control) -> ControllerCommand<Control> {
        let mut issued = *self.command_tx.borrow();
        let _ = self.command_tx.send_if_modified(|current| {
            issued = *current;
            if current.control == control {
                return false;
            }
            current.control = control;
            issued = *current;
            true
        });
        issued
    }

    /// Enter rejection and advance to a new failed connection cohort.
    ///
    /// A later attempt cannot fail until the controller first leaves rejection,
    /// so an already-matching phase still represents the same failure.
    fn reject(&self, control: Control) -> ControllerCommand<Control> {
        let mut issued = *self.command_tx.borrow();
        let _ = self.command_tx.send_if_modified(|current| {
            issued = *current;
            if current.control == control {
                return false;
            }
            current.control = control;
            current.failure_epoch = current.failure_epoch.saturating_add(1);
            issued = *current;
            true
        });
        issued
    }

    async fn wait_for(&mut self, command: ControllerCommand<Control>) -> Result<(), String> {
        loop {
            match &*self.status_rx.borrow_and_update() {
                Ok(actual) if *actual == command => {
                    if *self.command_tx.borrow() == command {
                        return Ok(());
                    }
                    return Err("proxy controller command was superseded".to_string());
                }
                Err(message) => return Err(message.clone()),
                Ok(_) => {}
            }
            if *self.command_tx.borrow() != command {
                return Err("proxy controller command was superseded".to_string());
            }
            if self.status_rx.changed().await.is_err() {
                return Err("proxy controller stopped unexpectedly".to_string());
            }
        }
    }
}

/// What Don should do with connections queued on a listenfd listener.
///
/// The controller is permanent so only one [`AsyncFd`] registration exists
/// for the blocking listener at a time. The child remains the sole acceptor
/// while disarmed; Don accepts and closes connections only while a failed
/// service is unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListenfdControl {
    Armed,
    Disarmed,
    Rejecting,
}

/// Runtime metadata for one proxy listener, in configuration declaration
/// order. The runner uses this to expose the actual public address without
/// depending on the proxy's internal listener ownership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProxyBinding {
    pub(crate) configured_addr: String,
    pub(crate) bound_addr: SocketAddr,
    pub(crate) mode: ProxyBindingMode,
    pub(crate) used_fallback: bool,
}

impl ProxyBinding {
    /// Address local clients should use. Wildcard bind addresses are replaced
    /// with their matching loopback address.
    pub(crate) fn connect_addr(&self) -> SocketAddr {
        let ip = match self.bound_addr.ip() {
            IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
            ip => ip,
        };
        SocketAddr::new(ip, self.bound_addr.port())
    }
}

/// Runtime proxy mode retained alongside a [`ProxyBinding`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ProxyBindingMode {
    Env { env_name: String },
    Forward { target: SocketAddr },
    Listenfd,
}

/// A set of proxy listeners for a single service.
pub(crate) struct ServiceProxy {
    forward: Vec<ForwardListener>,
    listenfd: Vec<ListenfdListener>,
    bindings: Vec<ProxyBinding>,
    active_forward_connections: Arc<AtomicUsize>,
}

/// Waits until every listenfd controller has stopped accepting and restored
/// the inherited listener to blocking mode before a child is spawned.
#[derive(Clone, Default)]
pub(crate) struct ListenfdStartBarrier {
    controllers: Vec<Controller<ListenfdControl>>,
}

impl ListenfdStartBarrier {
    pub(crate) async fn wait(mut self) -> Result<(), String> {
        for controller in &mut self.controllers {
            let command = *controller.command_tx.borrow();
            if command.control != ListenfdControl::Disarmed {
                return Err("listenfd proxy was not prepared for child handoff".to_string());
            }
            controller.wait_for(command).await?;
        }
        Ok(())
    }
}

/// Exact controller commands awaited across a detached runner operation.
///
/// This preserves ownership of an acknowledgement: a superseding phase or
/// failure epoch makes the wait fail instead of satisfying stale work.
pub(crate) struct LazyProxyBarrier {
    forward: Vec<(
        Controller<ForwardControl>,
        ControllerCommand<ForwardControl>,
    )>,
    listenfd: Vec<(
        Controller<ListenfdControl>,
        ControllerCommand<ListenfdControl>,
    )>,
}

impl LazyProxyBarrier {
    /// Wait for every controller to acknowledge its exact requested phase.
    pub(crate) async fn wait(self) -> Result<(), String> {
        wait_for_controllers(self.forward).await?;
        wait_for_controllers(self.listenfd).await
    }
}

async fn wait_for_controllers<Control>(
    controllers: Vec<(Controller<Control>, ControllerCommand<Control>)>,
) -> Result<(), String>
where
    Control: Copy + PartialEq,
{
    for (mut controller, command) in controllers {
        controller.wait_for(command).await?;
    }
    Ok(())
}

enum PendingListener {
    Forward {
        listener: TcpListener,
        backend: ForwardBackend,
    },
    Listenfd {
        listener: std::net::TcpListener,
        listen_addr: SocketAddr,
    },
}

struct BoundListener<T> {
    listener: T,
    addr: SocketAddr,
    used_fallback: bool,
}

#[derive(Clone)]
struct ProxyConnectionAccounting {
    permits: Arc<Semaphore>,
    max_connections: usize,
    global_active_connections: Arc<AtomicUsize>,
    service_active_connections: Arc<AtomicUsize>,
}

struct ForwardControllerConfig {
    backend_rx: watch::Receiver<Option<SocketAddr>>,
    control_rx: watch::Receiver<ControllerCommand<ForwardControl>>,
    status_tx: watch::Sender<ControllerStatus<ForwardControl>>,
    initial_command: ControllerCommand<ForwardControl>,
    lazy_tx: Option<mpsc::Sender<LazyProxyTrigger>>,
    service_name: String,
    emitter: crate::output::LifecycleEmitter,
}

impl ServiceProxy {
    /// Bind proxy listeners for a service's proxy entries.
    ///
    /// Env-mode entries bind a public listener, allocate an ephemeral backend
    /// port, and spawn a forwarding accept loop. Listenfd-mode entries bind
    /// the public listener and stop there; if `lazy_tx` is provided, a POLLIN
    /// watcher fires the lazy trigger on the first queued connection.
    ///
    /// All listeners are bound before any accept loop or lazy watcher is
    /// spawned. If a later entry fails, dropping the pending listeners releases
    /// every earlier port instead of leaving detached tasks holding them open.
    pub(crate) async fn bind(
        entries: &[ProxyEntry],
        fallback_ports: bool,
        lazy_tx: Option<mpsc::Sender<LazyProxyTrigger>>,
        service_name: &str,
        emitter: crate::output::LifecycleEmitter,
    ) -> Result<Self, ProxyError> {
        let mut pending = Vec::with_capacity(entries.len());
        let mut bindings = Vec::with_capacity(entries.len());

        // Phase one: reserve every public listener and allocate env-mode
        // backends. No tasks are spawned until the whole set succeeds.
        for entry in entries {
            let configured_addr: SocketAddr =
                entry.listen.parse().map_err(|e| ProxyError::Bind {
                    addr: entry.listen.clone(),
                    source: std::io::Error::new(std::io::ErrorKind::InvalidInput, e),
                })?;

            match &entry.mode {
                ProxyMode::Env(env_name) => {
                    let bound = bind_tokio_listener(configured_addr, fallback_ports)
                        .await
                        .map_err(|source| ProxyError::Bind {
                            addr: entry.listen.clone(),
                            source,
                        })?;
                    let ephemeral_addr = allocate_ephemeral_port().await?;
                    bindings.push(ProxyBinding {
                        configured_addr: entry.listen.clone(),
                        bound_addr: bound.addr,
                        mode: ProxyBindingMode::Env {
                            env_name: env_name.clone(),
                        },
                        used_fallback: bound.used_fallback,
                    });
                    pending.push(PendingListener::Forward {
                        listener: bound.listener,
                        backend: ForwardBackend::Ephemeral {
                            env_name: env_name.clone(),
                            addr: ephemeral_addr,
                        },
                    });
                }
                ProxyMode::Forward(target) => {
                    let backend_addr: SocketAddr =
                        target.parse().map_err(|e| ProxyError::Bind {
                            addr: target.clone(),
                            source: std::io::Error::new(std::io::ErrorKind::InvalidInput, e),
                        })?;
                    let bound = bind_tokio_listener(configured_addr, fallback_ports)
                        .await
                        .map_err(|source| ProxyError::Bind {
                            addr: entry.listen.clone(),
                            source,
                        })?;
                    bindings.push(ProxyBinding {
                        configured_addr: entry.listen.clone(),
                        bound_addr: bound.addr,
                        mode: ProxyBindingMode::Forward {
                            target: backend_addr,
                        },
                        used_fallback: bound.used_fallback,
                    });
                    pending.push(PendingListener::Forward {
                        listener: bound.listener,
                        backend: ForwardBackend::Fixed(backend_addr),
                    });
                }
                ProxyMode::Listenfd => {
                    // `std::net::TcpListener` gives us stable fd semantics for
                    // the LISTEN_FDS handoff. It deliberately remains blocking:
                    // O_NONBLOCK is shared across dup/dup2, so the controller
                    // restores blocking mode and acknowledges the handoff
                    // before a child can inherit the fd.
                    let bound =
                        bind_std_listener(configured_addr, fallback_ports).map_err(|source| {
                            ProxyError::Bind {
                                addr: entry.listen.clone(),
                                source,
                            }
                        })?;
                    bindings.push(ProxyBinding {
                        configured_addr: entry.listen.clone(),
                        bound_addr: bound.addr,
                        mode: ProxyBindingMode::Listenfd,
                        used_fallback: bound.used_fallback,
                    });
                    pending.push(PendingListener::Listenfd {
                        listener: bound.listener,
                        listen_addr: bound.addr,
                    });
                }
            }
        }

        // Phase two: now that all reservations succeeded, start the work that
        // owns them for the lifetime of ServiceProxy.
        let mut forward = Vec::new();
        let mut listenfd = Vec::new();
        let active_forward_connections = Arc::new(AtomicUsize::new(0));

        for listener in pending {
            match listener {
                PendingListener::Forward { listener, backend } => {
                    let (backend_tx, backend_rx) = watch::channel(None);
                    let initial_command = ControllerCommand {
                        control: ForwardControl::Accepting,
                        failure_epoch: 0,
                    };
                    let (control_tx, control_rx) = watch::channel(initial_command);
                    let (status_tx, status_rx) = watch::channel(Ok(initial_command));
                    let controller = Controller {
                        command_tx: control_tx,
                        status_rx,
                    };
                    let accept_handle = tokio::spawn(proxy_accept_loop(
                        listener,
                        ForwardControllerConfig {
                            backend_rx,
                            control_rx,
                            status_tx,
                            initial_command,
                            lazy_tx: lazy_tx.clone(),
                            service_name: service_name.to_string(),
                            emitter: emitter.clone(),
                        },
                        active_forward_connections.clone(),
                    ));
                    forward.push(ForwardListener {
                        backend,
                        backend_tx,
                        controller,
                        accept_handle,
                    });
                }
                PendingListener::Listenfd {
                    listener,
                    listen_addr,
                } => {
                    let listener = std::sync::Arc::new(listener);
                    let initial_control = if lazy_tx.is_some() {
                        ListenfdControl::Armed
                    } else {
                        ListenfdControl::Disarmed
                    };
                    let initial_command = ControllerCommand {
                        control: initial_control,
                        failure_epoch: 0,
                    };
                    let (control_tx, control_rx) = watch::channel(initial_command);
                    let (status_tx, status_rx) = watch::channel(Ok(initial_command));
                    let controller = Controller {
                        command_tx: control_tx.clone(),
                        status_rx,
                    };
                    let control_handle = spawn_listenfd_controller(
                        listener.clone(),
                        control_rx,
                        status_tx,
                        initial_command,
                        lazy_tx.clone(),
                        service_name.to_string(),
                        emitter.clone(),
                    );
                    listenfd.push(ListenfdListener {
                        listen_addr,
                        listener,
                        controller,
                        control_handle,
                    });
                }
            }
        }

        Ok(ServiceProxy {
            forward,
            listenfd,
            bindings,
            active_forward_connections,
        })
    }

    /// Point every forwarding backend at its configured address — the
    /// ephemeral one for `Env` mode, the fixed one for `Forward` mode.
    /// Listenfd entries are unaffected (the child owns the fd directly).
    pub(crate) fn set_backend(&self) {
        for fwd in &self.forward {
            let _ = fwd.backend_tx.send(Some(fwd.backend.addr()));
        }
    }

    /// Clear all forwarding backends. New connections queue until a
    /// backend is set again.
    pub(crate) fn clear_backend(&self) {
        for fwd in &self.forward {
            let _ = fwd.backend_tx.send(None);
        }
    }

    /// Apply the runner-owned proxy phase for a lazy service state.
    pub(crate) fn set_connection_state(&self, rejecting: bool, armed: bool) {
        for listener in &self.forward {
            if rejecting {
                listener.controller.reject(ForwardControl::Rejecting);
            } else {
                listener.controller.set(ForwardControl::Accepting);
            }
        }
        for listener in &self.listenfd {
            if rejecting {
                listener.controller.reject(ListenfdControl::Rejecting);
            } else if armed {
                listener.controller.set(ListenfdControl::Armed);
            } else {
                listener.controller.set(ListenfdControl::Disarmed);
            }
        }
    }

    /// Enter durable rejection and return its exact acknowledgement barrier.
    pub(crate) fn begin_lazy_failure_recovery(&mut self) -> LazyProxyBarrier {
        let forward = self
            .forward
            .iter()
            .map(|listener| {
                let controller = listener.controller.clone();
                let command = controller.reject(ForwardControl::Rejecting);
                (controller, command)
            })
            .collect();
        let listenfd = self
            .listenfd
            .iter()
            .map(|listener| {
                let controller = listener.controller.clone();
                let command = controller.reject(ListenfdControl::Rejecting);
                (controller, command)
            })
            .collect();
        LazyProxyBarrier { forward, listenfd }
    }

    /// Drain every failed backlog into a non-triggering prepared phase.
    pub(crate) fn begin_lazy_rearm(&self) -> LazyProxyBarrier {
        let forward = self
            .forward
            .iter()
            .map(|listener| {
                let controller = listener.controller.clone();
                let command = controller.set(ForwardControl::Paused);
                (controller, command)
            })
            .collect();
        let listenfd = self
            .listenfd
            .iter()
            .map(|listener| {
                let controller = listener.controller.clone();
                let command = controller.set(ListenfdControl::Disarmed);
                (controller, command)
            })
            .collect();
        LazyProxyBarrier { forward, listenfd }
    }

    /// Whether every listener is enabled for the trigger's connection cohort.
    pub(crate) fn accepts_lazy_trigger(&self, failure_epoch: u64) -> bool {
        (!self.forward.is_empty() || !self.listenfd.is_empty())
            && self.forward.iter().all(|listener| {
                let command = listener.controller.command_tx.borrow();
                command.control == ForwardControl::Accepting
                    && command.failure_epoch == failure_epoch
            })
            && self.listenfd.iter().all(|listener| {
                let command = listener.controller.command_tx.borrow();
                command.control == ListenfdControl::Armed && command.failure_epoch == failure_epoch
            })
    }

    /// Barrier used by start workers before inheriting listenfd descriptors.
    pub(crate) fn listenfd_start_barrier(&self) -> ListenfdStartBarrier {
        ListenfdStartBarrier {
            controllers: self
                .listenfd
                .iter()
                .map(|listener| listener.controller.clone())
                .collect(),
        }
    }

    /// Allocate new ephemeral ports for env-mode entries. Used on restart so
    /// the old port is gone before the new process tries to bind it. Fixed
    /// `Forward` entries are left alone — their address is user-provided
    /// and stable across restarts.
    pub(crate) async fn reallocate_ephemeral_ports(&mut self) -> Result<(), ProxyError> {
        for fwd in &mut self.forward {
            if let ForwardBackend::Ephemeral { addr, .. } = &mut fwd.backend {
                *addr = allocate_ephemeral_port().await?;
            }
        }
        Ok(())
    }

    /// Env var map for env-mode entries, e.g. `{"PORT": "49152"}`. Fixed
    /// `Forward` entries contribute nothing — the service already knows its
    /// port.
    pub(crate) fn env_vars(&self) -> HashMap<String, String> {
        let mut vars = HashMap::new();
        for fwd in &self.forward {
            if let ForwardBackend::Ephemeral { env_name, addr } = &fwd.backend {
                vars.insert(env_name.clone(), addr.port().to_string());
            }
        }
        vars
    }

    /// Public address env vars for this service. These describe Don's
    /// externally reachable listeners, which can differ from the configured
    /// addresses when fallback ports or explicit port 0 are used.
    pub(crate) fn public_env_vars(&self) -> HashMap<String, String> {
        let mut vars = HashMap::new();

        for (idx, binding) in self.bindings.iter().enumerate() {
            let addr = binding.connect_addr();
            vars.insert(format!("DON_PUBLIC_ADDR_{idx}"), addr.to_string());
            vars.insert(format!("DON_PUBLIC_PORT_{idx}"), addr.port().to_string());

            if idx == 0 {
                vars.insert("DON_PUBLIC_ADDR".to_string(), addr.to_string());
                vars.insert("DON_PUBLIC_PORT".to_string(), addr.port().to_string());
            }

            if let ProxyBindingMode::Env { env_name } = &binding.mode {
                let suffix = sanitize_env_suffix(env_name);
                if !suffix.is_empty() {
                    vars.insert(format!("DON_PUBLIC_{suffix}"), addr.port().to_string());
                    vars.insert(format!("DON_PUBLIC_{suffix}_ADDR"), addr.to_string());
                    vars.insert(format!("DON_PUBLIC_{suffix}_PORT"), addr.port().to_string());
                }
            }
        }

        vars
    }

    /// Runtime reference values for other services' inline env expansion.
    /// Values always describe public listener addresses, never env-mode
    /// backend ports. For example, `$(database.PORT)` resolves to the public
    /// port for `proxy = { ..., env = "PORT" }`.
    pub(crate) fn env_reference_values(&self) -> HashMap<String, String> {
        let mut vars = HashMap::new();

        for (idx, binding) in self.bindings.iter().enumerate() {
            let addr = binding.connect_addr();
            vars.insert(format!("addr_{idx}"), addr.to_string());
            vars.insert(format!("port_{idx}"), addr.port().to_string());
            vars.insert(format!("ADDR_{idx}"), addr.to_string());
            vars.insert(format!("PORT_{idx}"), addr.port().to_string());

            if idx == 0 {
                vars.insert("addr".to_string(), addr.to_string());
                vars.insert("port".to_string(), addr.port().to_string());
                vars.insert("ADDR".to_string(), addr.to_string());
                vars.insert("PORT".to_string(), addr.port().to_string());
            }

            if let ProxyBindingMode::Env { env_name } = &binding.mode {
                let suffix = sanitize_env_suffix(env_name);
                if !suffix.is_empty() {
                    vars.insert(suffix.clone(), addr.port().to_string());
                    vars.insert(format!("{suffix}_ADDR"), addr.to_string());
                    vars.insert(format!("{suffix}_PORT"), addr.port().to_string());
                }
            }
        }

        vars
    }

    /// True if any proxy entry requires serial (no-overlap) restart. Fixed
    /// `Forward` backends can't have two processes binding the same port at
    /// once, so the caller must fully tear down the old instance before
    /// starting the new one.
    pub(crate) fn requires_full_exit_on_restart(&self) -> bool {
        self.forward
            .iter()
            .any(|f| matches!(f.backend, ForwardBackend::Fixed(_)))
    }

    /// Socket-activation env vars for listenfd entries. Empty if the service
    /// has no listenfd proxy entries. `LISTEN_FD=3` is a single-fd convenience
    /// for Node-style bootstraps; `LISTEN_FDS` / `LISTEN_FDNAMES` remain the
    /// systemd-compatible source of truth. `LISTEN_PID` is set by the shell
    /// shim at spawn time — see `process::mod::listen_pid_shim`.
    pub(crate) fn listenfd_env(&self) -> HashMap<String, String> {
        let mut env = HashMap::new();
        if self.listenfd.is_empty() {
            return env;
        }
        if self.listenfd.len() == 1 {
            env.insert("LISTEN_FD".to_string(), "3".to_string());
        }
        env.insert("LISTEN_FDS".to_string(), self.listenfd.len().to_string());
        let names: Vec<String> = self
            .listenfd
            .iter()
            .map(|l| l.listen_addr.to_string())
            .collect();
        env.insert("LISTEN_FDNAMES".to_string(), names.join(":"));
        env
    }

    /// Raw fds of the listenfd listeners, in declaration order. Each fd is a
    /// bound listening socket that the child will see at fd 3, 4, ….
    pub(crate) fn listenfd_raw_fds(&self) -> Vec<RawFd> {
        self.listenfd
            .iter()
            .map(|l| l.listener.as_raw_fd())
            .collect()
    }

    /// Cloneable configured-to-actual binding metadata in declaration order.
    pub(crate) fn bindings(&self) -> &[ProxyBinding] {
        &self.bindings
    }

    /// Addresses Don is listening on, in original declaration order.
    pub(crate) fn listen_addrs(&self) -> Vec<SocketAddr> {
        self.bindings
            .iter()
            .map(|binding| binding.bound_addr)
            .collect()
    }

    /// Human-readable entries using the actual bound public addresses, in
    /// original declaration order.
    pub(crate) fn descriptions(&self) -> Vec<String> {
        self.bindings
            .iter()
            .map(|binding| match &binding.mode {
                ProxyBindingMode::Env { env_name } => {
                    format!("{} (env={env_name})", binding.bound_addr)
                }
                ProxyBindingMode::Forward { target } => {
                    format!("{} → {target}", binding.bound_addr)
                }
                ProxyBindingMode::Listenfd => {
                    format!("{} (listenfd)", binding.bound_addr)
                }
            })
            .collect()
    }

    /// User-facing messages for listeners that could not claim their
    /// configured port and were moved to an OS-selected fallback port.
    pub(crate) fn fallback_descriptions(&self) -> Vec<String> {
        self.bindings
            .iter()
            .filter(|binding| binding.used_fallback)
            .map(|binding| {
                format!(
                    "{} is in use; using {}",
                    binding.configured_addr, binding.bound_addr
                )
            })
            .collect()
    }

    /// Active env/forward proxy connections owned by Don. Listenfd-mode
    /// sockets are accepted by the child process, so Don cannot count them.
    pub(crate) fn active_forward_connections(&self) -> Option<usize> {
        if self.forward.is_empty() {
            return None;
        }
        Some(self.active_forward_connections.load(Ordering::Relaxed))
    }

    /// Shut down all proxy work — abort forwarding accept loops and listenfd
    /// controllers.
    pub(crate) fn shutdown(&self) {
        for fwd in &self.forward {
            fwd.accept_handle.abort();
        }
        for l in &self.listenfd {
            l.control_handle.abort();
        }
    }
}

async fn bind_tokio_listener(
    configured_addr: SocketAddr,
    fallback_ports: bool,
) -> Result<BoundListener<TcpListener>, std::io::Error> {
    match TcpListener::bind(configured_addr).await {
        Ok(listener) => {
            let addr = listener.local_addr()?;
            Ok(BoundListener {
                listener,
                addr,
                used_fallback: false,
            })
        }
        Err(error) if should_fallback(configured_addr, fallback_ports, &error) => {
            let listener = TcpListener::bind(fallback_addr(configured_addr)).await?;
            let addr = listener.local_addr()?;
            Ok(BoundListener {
                listener,
                addr,
                used_fallback: true,
            })
        }
        Err(error) => Err(error),
    }
}

fn bind_std_listener(
    configured_addr: SocketAddr,
    fallback_ports: bool,
) -> Result<BoundListener<std::net::TcpListener>, std::io::Error> {
    match std::net::TcpListener::bind(configured_addr) {
        Ok(listener) => {
            let addr = listener.local_addr()?;
            Ok(BoundListener {
                listener,
                addr,
                used_fallback: false,
            })
        }
        Err(error) if should_fallback(configured_addr, fallback_ports, &error) => {
            let listener = std::net::TcpListener::bind(fallback_addr(configured_addr))?;
            let addr = listener.local_addr()?;
            Ok(BoundListener {
                listener,
                addr,
                used_fallback: true,
            })
        }
        Err(error) => Err(error),
    }
}

fn should_fallback(
    configured_addr: SocketAddr,
    fallback_ports: bool,
    error: &std::io::Error,
) -> bool {
    fallback_ports && configured_addr.port() != 0 && error.kind() == std::io::ErrorKind::AddrInUse
}

fn fallback_addr(mut configured_addr: SocketAddr) -> SocketAddr {
    configured_addr.set_port(0);
    configured_addr
}

fn sanitize_env_suffix(name: &str) -> String {
    name.chars()
        .filter_map(|ch| {
            if ch.is_ascii_alphanumeric() {
                Some(ch.to_ascii_uppercase())
            } else if ch == '_' {
                Some('_')
            } else {
                None
            }
        })
        .collect()
}

/// Spawn the permanent controller for a listenfd listener.
///
/// In `Armed` mode it observes POLLIN without accepting and triggers lazy
/// startup. In `Disarmed` mode it leaves the listener entirely to the child.
/// In `Rejecting` mode the child is terminally unavailable, so it accepts and
/// closes every queued connection until the runner begins a recovery. When the
/// desired mode leaves `Rejecting`, the controller drains to an empty accept
/// queue and restores blocking mode before acknowledging the target mode.
fn spawn_listenfd_controller(
    listener: std::sync::Arc<std::net::TcpListener>,
    mut control_rx: watch::Receiver<ControllerCommand<ListenfdControl>>,
    status_tx: watch::Sender<ControllerStatus<ListenfdControl>>,
    initial_command: ControllerCommand<ListenfdControl>,
    lazy_tx: Option<mpsc::Sender<LazyProxyTrigger>>,
    service_name: String,
    emitter: crate::output::LifecycleEmitter,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(message) = run_listenfd_controller(
            listener,
            &mut control_rx,
            &status_tx,
            initial_command,
            lazy_tx,
            &service_name,
        )
        .await
        {
            emitter.service_error_event(&service_name, &message);
            let _ = status_tx.send_replace(Err(message));
        }
    })
}

async fn run_listenfd_controller(
    listener: std::sync::Arc<std::net::TcpListener>,
    control_rx: &mut watch::Receiver<ControllerCommand<ListenfdControl>>,
    status_tx: &watch::Sender<ControllerStatus<ListenfdControl>>,
    initial_command: ControllerCommand<ListenfdControl>,
    lazy_tx: Option<mpsc::Sender<LazyProxyTrigger>>,
    service_name: &str,
) -> Result<(), String> {
    let raw_fd = listener.as_raw_fd();
    let async_fd = AsyncFd::new(listener)
        .map_err(|error| format!("failed to register listenfd proxy: {error}"))?;
    let mut applied = initial_command;
    let mut nonblocking = false;
    loop {
        let desired = *control_rx.borrow_and_update();
        let needs_drain = desired.control != ListenfdControl::Rejecting
            && (applied.control == ListenfdControl::Rejecting
                || desired.failure_epoch > applied.failure_epoch);
        if needs_drain {
            if !nonblocking {
                async_fd
                    .get_ref()
                    .set_nonblocking(true)
                    .map_err(|error| format!("failed to change listenfd proxy mode: {error}"))?;
                nonblocking = true;
            }
            match drain_pending_batch(raw_fd, control_rx, desired) {
                Ok(true) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Ok(false) if *control_rx.borrow() != desired => continue,
                Ok(false) => {}
                Err(error) => {
                    return Err(format!("failed to drain listenfd connections: {error}"));
                }
            }
        }

        let should_be_nonblocking = desired.control == ListenfdControl::Rejecting;
        if should_be_nonblocking != nonblocking {
            async_fd
                .get_ref()
                .set_nonblocking(should_be_nonblocking)
                .map_err(|error| format!("failed to change listenfd proxy mode: {error}"))?;
            nonblocking = should_be_nonblocking;
        }
        applied = desired;
        let _ = status_tx.send_replace(Ok(applied));

        match applied.control {
            ListenfdControl::Armed => {
                let Some(tx) = lazy_tx.as_ref() else {
                    if control_rx.changed().await.is_err() {
                        return Ok(());
                    }
                    continue;
                };
                tokio::select! {
                    ready = async_fd.readable() => {
                        let mut guard = match ready {
                            Ok(guard) => guard,
                            Err(_) => return Ok(()),
                        };
                        // AsyncFd readiness can be a false positive. A
                        // level-triggered poll confirms that the accept
                        // queue is non-empty without consuming it.
                        if has_pending_connection(raw_fd)
                            && *control_rx.borrow() == applied
                        {
                            tokio::select! {
                                    result = tx.send(LazyProxyTrigger {
                                        service_name: service_name.to_string(),
                                        failure_epoch: applied.failure_epoch,
                                    }) => {
                                        if result.is_err() {
                                            return Ok(());
                                        }
                                        if control_rx.changed().await.is_err() {
                                            return Ok(());
                                        }
                                    }
                                changed = control_rx.changed() => {
                                    if changed.is_err() {
                                        return Ok(());
                                    }
                                }
                            }
                        } else {
                            guard.clear_ready();
                        }
                    }
                    changed = control_rx.changed() => {
                        if changed.is_err() {
                            return Ok(());
                        }
                    }
                }
            }
            ListenfdControl::Disarmed => {
                if control_rx.changed().await.is_err() {
                    return Ok(());
                }
            }
            ListenfdControl::Rejecting => {
                tokio::select! {
                    ready = async_fd.readable() => {
                        let mut guard = match ready {
                            Ok(guard) => guard,
                            Err(_) => return Ok(()),
                        };
                        drain_pending_batch(raw_fd, control_rx, applied).map_err(
                            |error| format!("failed to reject listenfd connection: {error}"),
                        )?;
                        if !has_pending_connection(raw_fd) {
                            guard.clear_ready();
                        }
                        tokio::task::yield_now().await;
                    }
                    changed = control_rx.changed() => {
                        if changed.is_err() {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

const MAX_REJECT_DRAIN_BATCH: usize = 64;

/// Drain a bounded batch while the requested controller mode is unchanged.
///
/// Returns `true` when the batch limit was reached and more work may remain,
/// or `false` once `accept` reports that the kernel queue is empty.
fn drain_pending_batch<Control>(
    fd: RawFd,
    control_rx: &watch::Receiver<ControllerCommand<Control>>,
    expected: ControllerCommand<Control>,
) -> Result<bool, std::io::Error>
where
    Control: Copy + PartialEq,
{
    for _ in 0..MAX_REJECT_DRAIN_BATCH {
        if *control_rx.borrow() != expected {
            return Ok(false);
        }
        if !close_pending_nonblocking(fd)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Non-blocking check for a queued connection on a listening fd.
///
/// `poll(2)` with a zero timeout returns immediately with the fd's current
/// readiness. POLLIN on a listening socket = accept queue non-empty.
/// Non-consuming.
fn has_pending_connection(fd: RawFd) -> bool {
    let mut pollfd = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    // Safety: pollfd is a valid initialized struct on the stack; poll()
    // reads one entry as specified by the count argument (1).
    let ret = unsafe { libc::poll(&mut pollfd, 1, 0) };
    ret > 0 && (pollfd.revents & libc::POLLIN) != 0
}

impl Drop for ServiceProxy {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Allocate an ephemeral port by binding to port 0, reading the assigned port,
/// then dropping the listener. There is a tiny TOCTOU window, but acceptable
/// for local dev tooling.
async fn allocate_ephemeral_port() -> Result<SocketAddr, ProxyError> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(ProxyError::EphemeralPort)?;
    let addr = listener.local_addr().map_err(ProxyError::EphemeralPort)?;
    drop(listener);
    Ok(addr)
}

/// Accept loop for a single proxy listener.
///
/// Accepts TCP connections, optionally triggers lazy start on the first one,
/// waits for a backend to be available, then spawns per-connection forwarding.
async fn proxy_accept_loop(
    listener: TcpListener,
    config: ForwardControllerConfig,
    active_connections: Arc<AtomicUsize>,
) {
    let connection_pool = proxy_connection_pool();
    let accounting = ProxyConnectionAccounting {
        permits: connection_pool.permits,
        max_connections: connection_pool.max_connections,
        global_active_connections: connection_pool.active_connections,
        service_active_connections: active_connections,
    };
    proxy_accept_loop_with_permits(listener, config, accounting).await;
}

async fn proxy_accept_loop_with_permits(
    listener: TcpListener,
    config: ForwardControllerConfig,
    accounting: ProxyConnectionAccounting,
) {
    let ForwardControllerConfig {
        backend_rx,
        mut control_rx,
        status_tx,
        initial_command,
        lazy_tx,
        service_name,
        emitter,
    } = config;
    let mut consecutive_errors: u32 = 0;
    let mut connection_limit_reported = false;
    let listen_addr = listener
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();
    let mut applied = initial_command;
    loop {
        let desired = *control_rx.borrow_and_update();
        let needs_drain = desired.control != ForwardControl::Rejecting
            && (applied.control == ForwardControl::Rejecting
                || desired.failure_epoch > applied.failure_epoch);
        if needs_drain {
            match drain_pending_batch(listener.as_raw_fd(), &control_rx, desired) {
                Ok(true) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Ok(false) if *control_rx.borrow() != desired => continue,
                Ok(false) => {}
                Err(error) => {
                    let message = format!(
                        "{service_name}: failed to drain forward proxy {listen_addr}: {error}"
                    );
                    emitter.lifecycle_event(&message);
                    let _ = status_tx.send_replace(Err(message));
                    return;
                }
            }
        }
        applied = desired;
        let _ = status_tx.send_replace(Ok(applied));
        match applied.control {
            ForwardControl::Accepting => {
                let accepted = tokio::select! {
                    accepted = listener.accept() => Some(accepted),
                    changed = control_rx.changed() => {
                        if changed.is_err() {
                            return;
                        }
                        None
                    }
                };
                let Some(accepted) = accepted else {
                    continue;
                };
                let (client, _peer) = match accepted {
                    Ok(connection) => {
                        consecutive_errors = 0;
                        connection
                    }
                    Err(error) => {
                        consecutive_errors = consecutive_errors.saturating_add(1);
                        let delay = std::time::Duration::from_millis(
                            (10 * consecutive_errors.min(100)) as u64,
                        );
                        emitter.lifecycle_event(&format!(
                            "{service_name}: proxy {listen_addr} accept error: {error} (backoff {delay:?})"
                        ));
                        tokio::select! {
                            () = tokio::time::sleep(delay) => {}
                            changed = control_rx.changed() => {
                                if changed.is_err() {
                                    return;
                                }
                            }
                        }
                        continue;
                    }
                };

                let connection_guard = match accounting.permits.clone().try_acquire_owned() {
                    Ok(permit) => {
                        connection_limit_reported = false;
                        ProxyConnectionGuard::new(
                            permit,
                            accounting.service_active_connections.clone(),
                            emitter.clone(),
                            service_name.clone(),
                            listen_addr.clone(),
                            accounting.max_connections,
                            accounting.global_active_connections.clone(),
                        )
                    }
                    Err(TryAcquireError::NoPermits) => {
                        if !connection_limit_reported {
                            connection_limit_reported = true;
                            let max_connections = accounting.max_connections;
                            emitter.lifecycle_event(&format!(
                                "{service_name}: proxy {listen_addr} connection limit reached; \
                                 closing new connections ({max_connections}/{max_connections} active)"
                            ));
                        }
                        let max_connections = accounting.max_connections;
                        emitter.service_debug_event(
                            &service_name,
                            &format!(
                                "proxy {listen_addr} closed overflow connection \
                                 ({max_connections}/{max_connections} active)"
                            ),
                        );
                        drop(client);
                        continue;
                    }
                    Err(TryAcquireError::Closed) => return,
                };

                tokio::spawn(handle_forward_client(
                    client,
                    backend_rx.clone(),
                    control_rx.clone(),
                    applied.failure_epoch,
                    lazy_tx.clone(),
                    service_name.clone(),
                    connection_guard,
                ));
            }
            ForwardControl::Rejecting => {
                tokio::select! {
                    accepted = listener.accept() => {
                        match accepted {
                            Ok((client, _)) => drop(client),
                            Err(error) => {
                                emitter.lifecycle_event(&format!(
                                    "{service_name}: proxy {listen_addr} reject accept error: {error}"
                                ));
                                tokio::task::yield_now().await;
                            }
                        }
                    }
                    changed = control_rx.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                }
            }
            ForwardControl::Paused => {
                if control_rx.changed().await.is_err() {
                    return;
                }
            }
        }
    }
}

fn close_pending_nonblocking(fd: RawFd) -> Result<bool, std::io::Error> {
    loop {
        // Safety: `fd` is a live nonblocking listening socket owned by the
        // forwarding controller. Null address pointers omit peer metadata.
        let accepted = unsafe { libc::accept(fd, std::ptr::null_mut(), std::ptr::null_mut()) };
        if accepted >= 0 {
            // Safety: accept returned a new owned descriptor. OwnedFd closes
            // it exactly once at the end of this scope.
            let _accepted = unsafe { OwnedFd::from_raw_fd(accepted) };
            return Ok(true);
        }
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::WouldBlock {
            return Ok(false);
        }
        if is_retryable_accept_error(&error) {
            continue;
        }
        return Err(error);
    }
}

/// Errors tied to one queued peer do not invalidate the listening socket.
///
/// POSIX explicitly allows these network/protocol errors to surface from
/// `accept`; BSD-derived stacks commonly report `ECONNABORTED` when a peer
/// disconnects before Don drains it.
fn is_retryable_accept_error(error: &std::io::Error) -> bool {
    if matches!(
        error.kind(),
        std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::NetworkDown
            | std::io::ErrorKind::NetworkUnreachable
            | std::io::ErrorKind::HostUnreachable
    ) {
        return true;
    }
    matches!(
        error.raw_os_error(),
        Some(libc::EPROTO | libc::ENOPROTOOPT | libc::EHOSTDOWN | libc::EOPNOTSUPP)
    )
}

async fn handle_forward_client(
    mut client: TcpStream,
    backend_rx: watch::Receiver<Option<SocketAddr>>,
    mut control_rx: watch::Receiver<ControllerCommand<ForwardControl>>,
    accepted_failure_epoch: u64,
    lazy_tx: Option<mpsc::Sender<LazyProxyTrigger>>,
    service_name: String,
    connection_guard: ProxyConnectionGuard,
) {
    if !forward_command_accepts(&control_rx.borrow(), accepted_failure_epoch) {
        let _ = client.shutdown().await;
        return;
    }
    if let Some(tx) = lazy_tx {
        let triggered = tokio::select! {
            result = tx.send(LazyProxyTrigger {
                service_name,
                failure_epoch: accepted_failure_epoch,
            }) => result.is_ok(),
            () = wait_for_forward_rejection(&mut control_rx, accepted_failure_epoch) => false,
        };
        if !triggered {
            let _ = client.shutdown().await;
            return;
        }
    }

    let Some(backend_addr) =
        wait_for_backend_or_rejection(backend_rx, &mut control_rx, accepted_failure_epoch).await
    else {
        let _ = client.shutdown().await;
        return;
    };
    proxy_connection(
        client,
        backend_addr,
        control_rx,
        accepted_failure_epoch,
        connection_guard,
    )
    .await;
}

/// Wait until forwarding can begin, unless the runner reports a terminal
/// service failure first.
async fn wait_for_backend_or_rejection(
    mut backend_rx: watch::Receiver<Option<SocketAddr>>,
    control_rx: &mut watch::Receiver<ControllerCommand<ForwardControl>>,
    accepted_failure_epoch: u64,
) -> Option<SocketAddr> {
    loop {
        if !forward_command_accepts(&control_rx.borrow(), accepted_failure_epoch) {
            return None;
        }
        if let Some(addr) = *backend_rx.borrow() {
            return Some(addr);
        }
        tokio::select! {
            changed = backend_rx.changed() => {
                if changed.is_err() {
                    return None;
                }
            }
            () = wait_for_forward_rejection(control_rx, accepted_failure_epoch) => return None,
        }
    }
}

struct ProxyConnectionGuard {
    _permit: OwnedSemaphorePermit,
    active_connections: Arc<AtomicUsize>,
    emitter: crate::output::LifecycleEmitter,
    service_name: String,
    listen_addr: String,
    max_connections: usize,
    global_active_connections: Arc<AtomicUsize>,
}

impl ProxyConnectionGuard {
    fn new(
        permit: OwnedSemaphorePermit,
        active_connections: Arc<AtomicUsize>,
        emitter: crate::output::LifecycleEmitter,
        service_name: String,
        listen_addr: String,
        max_connections: usize,
        global_active_connections: Arc<AtomicUsize>,
    ) -> Self {
        active_connections.fetch_add(1, Ordering::Relaxed);
        let active = global_active_connections.fetch_add(1, Ordering::Relaxed) + 1;
        emitter.service_debug_event(
            &service_name,
            &format!("proxy {listen_addr} accepted connection ({active}/{max_connections} active)"),
        );
        Self {
            _permit: permit,
            active_connections,
            emitter,
            service_name,
            listen_addr,
            max_connections,
            global_active_connections,
        }
    }
}

impl Drop for ProxyConnectionGuard {
    fn drop(&mut self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
        let active = self
            .global_active_connections
            .fetch_sub(1, Ordering::Relaxed)
            .saturating_sub(1);
        self.emitter.service_debug_event(
            &self.service_name,
            &format!(
                "proxy {} closed connection ({}/{} active)",
                self.listen_addr, active, self.max_connections
            ),
        );
    }
}

/// Forward traffic bidirectionally between client and backend.
///
/// Retries the backend connection with exponential backoff if the service
/// isn't listening yet (common during startup before the process binds its port).
async fn proxy_connection(
    mut client: TcpStream,
    backend_addr: SocketAddr,
    mut control_rx: watch::Receiver<ControllerCommand<ForwardControl>>,
    accepted_failure_epoch: u64,
    _connection_guard: ProxyConnectionGuard,
) {
    let backend_candidates = backend_connect_candidates(backend_addr);
    let mut backend = None;
    for attempt in 0..20u32 {
        for candidate in &backend_candidates {
            let result = tokio::select! {
                result = TcpStream::connect(candidate) => result,
                () = wait_for_forward_rejection(&mut control_rx, accepted_failure_epoch) => {
                    let _ = client.shutdown().await;
                    return;
                }
            };
            match result {
                Ok(stream) => {
                    backend = Some(stream);
                    break;
                }
                Err(_) => continue,
            }
        }

        if backend.is_some() {
            break;
        }

        // Exponential backoff: 10ms, 20ms, 40ms, ... capped at 500ms.
        let delay = std::time::Duration::from_millis((10 * (1 << attempt.min(6))).min(500));
        tokio::select! {
            () = tokio::time::sleep(delay) => {}
            () = wait_for_forward_rejection(&mut control_rx, accepted_failure_epoch) => {
                let _ = client.shutdown().await;
                return;
            }
        }
    }

    let Some(mut backend) = backend else {
        // Backend never became reachable — close client connection.
        let _ = client.shutdown().await;
        return;
    };

    // Shovel bytes in both directions until either side closes. A terminal
    // runner failure also closes the client promptly even if the backend
    // socket has not observed the process exit yet.
    let rejected = tokio::select! {
        _ = tokio::io::copy_bidirectional(&mut client, &mut backend) => false,
        () = wait_for_forward_rejection(&mut control_rx, accepted_failure_epoch) => true,
    };
    if rejected {
        let _ = client.shutdown().await;
        let _ = backend.shutdown().await;
    }
}

fn forward_command_accepts(
    command: &ControllerCommand<ForwardControl>,
    accepted_failure_epoch: u64,
) -> bool {
    command.control == ForwardControl::Accepting && command.failure_epoch == accepted_failure_epoch
}

async fn wait_for_forward_rejection(
    control_rx: &mut watch::Receiver<ControllerCommand<ForwardControl>>,
    accepted_failure_epoch: u64,
) {
    loop {
        if !forward_command_accepts(&control_rx.borrow(), accepted_failure_epoch) {
            return;
        }
        if control_rx.changed().await.is_err() {
            return;
        }
    }
}

#[derive(Clone)]
struct ProxyConnectionPool {
    permits: Arc<Semaphore>,
    max_connections: usize,
    active_connections: Arc<AtomicUsize>,
}

fn proxy_connection_pool() -> ProxyConnectionPool {
    PROXY_CONNECTION_POOL
        .get_or_init(|| {
            let max_connections = proxy_connection_limit();
            ProxyConnectionPool {
                permits: Arc::new(Semaphore::new(max_connections)),
                max_connections,
                active_connections: Arc::new(AtomicUsize::new(0)),
            }
        })
        .clone()
}

fn proxy_connection_limit() -> usize {
    let soft_limit = current_nofile_soft_limit().unwrap_or(DEFAULT_PROXY_CONNECTION_LIMIT * 2);
    proxy_connection_limit_for_soft_nofile(soft_limit) as usize
}

fn proxy_connection_limit_for_soft_nofile(soft_limit: u64) -> u64 {
    let fd_backed_limit = soft_limit.saturating_sub(PROXY_FD_RESERVE) / 2;
    fd_backed_limit.clamp(MIN_PROXY_CONNECTION_LIMIT, DEFAULT_PROXY_CONNECTION_LIMIT)
}

fn current_nofile_soft_limit() -> Option<u64> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // Safety: `limit` points at a valid initialized rlimit struct for
    // getrlimit() to fill.
    let ret = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };
    if ret != 0 || limit.rlim_cur == libc::RLIM_INFINITY {
        return None;
    }
    Some(limit.rlim_cur)
}

fn backend_connect_candidates(backend_addr: SocketAddr) -> Vec<SocketAddr> {
    let mut candidates = vec![backend_addr];
    match backend_addr.ip() {
        std::net::IpAddr::V4(ip) if ip.is_loopback() => {
            candidates.push(SocketAddr::new(
                std::net::Ipv6Addr::LOCALHOST.into(),
                backend_addr.port(),
            ));
        }
        std::net::IpAddr::V6(ip) if ip.is_loopback() => {
            candidates.push(SocketAddr::new(
                std::net::Ipv4Addr::LOCALHOST.into(),
                backend_addr.port(),
            ));
        }
        _ => {}
    }
    candidates
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
    use std::os::fd::AsRawFd;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::io::AsyncReadExt;
    use tokio::sync::{Semaphore, mpsc, watch};

    use crate::config::{LogConfig, ProxyEntry, ProxyMode};
    use crate::output::{LifecycleEmitter, OutputManager};

    use super::{
        Controller, ControllerCommand, DEFAULT_PROXY_CONNECTION_LIMIT, ForwardControl,
        ForwardControllerConfig, ListenfdControl, ListenfdStartBarrier, MIN_PROXY_CONNECTION_LIMIT,
        PROXY_FD_RESERVE, ProxyBinding, ProxyBindingMode, ProxyConnectionAccounting, ProxyError,
        ServiceProxy, backend_connect_candidates, is_retryable_accept_error,
        proxy_accept_loop_with_permits, proxy_connection_limit_for_soft_nofile,
        spawn_listenfd_controller, wait_for_forward_rejection,
    };

    #[tokio::test]
    async fn proxy_bind_port_selection_cases() {
        struct Case {
            name: &'static str,
            mode: ProxyMode,
            occupied: bool,
            configured_port_zero: bool,
            fallback_ports: bool,
            expect_success: bool,
            expect_fallback: bool,
        }

        let cases = vec![
            Case {
                name: "preferred env port remains unchanged",
                mode: ProxyMode::Env("PORT".to_string()),
                occupied: false,
                configured_port_zero: false,
                fallback_ports: true,
                expect_success: true,
                expect_fallback: false,
            },
            Case {
                name: "occupied env port falls back",
                mode: ProxyMode::Env("PORT".to_string()),
                occupied: true,
                configured_port_zero: false,
                fallback_ports: true,
                expect_success: true,
                expect_fallback: true,
            },
            Case {
                name: "occupied listenfd port falls back",
                mode: ProxyMode::Listenfd,
                occupied: true,
                configured_port_zero: false,
                fallback_ports: true,
                expect_success: true,
                expect_fallback: true,
            },
            Case {
                name: "disabled fallback preserves bind error",
                mode: ProxyMode::Listenfd,
                occupied: true,
                configured_port_zero: false,
                fallback_ports: false,
                expect_success: false,
                expect_fallback: false,
            },
            Case {
                name: "explicit port zero records actual address",
                mode: ProxyMode::Listenfd,
                occupied: false,
                configured_port_zero: true,
                fallback_ports: true,
                expect_success: true,
                expect_fallback: false,
            },
        ];

        let emitter = test_lifecycle_emitter().await;
        for case in cases {
            let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let reserved_addr = reservation.local_addr().unwrap();
            let configured_addr = if case.configured_port_zero {
                SocketAddr::new(reserved_addr.ip(), 0)
            } else {
                reserved_addr
            };
            let blocker = if case.occupied {
                Some(reservation)
            } else {
                drop(reservation);
                None
            };
            let entry = ProxyEntry {
                listen: configured_addr.to_string(),
                mode: case.mode,
            };

            let result = ServiceProxy::bind(
                &[entry],
                case.fallback_ports,
                None,
                case.name,
                emitter.clone(),
            )
            .await;

            if case.expect_success {
                let proxy = result.unwrap_or_else(|error| {
                    panic!("{}: expected bind success, got {error}", case.name)
                });
                let binding = proxy
                    .bindings()
                    .first()
                    .unwrap_or_else(|| panic!("{}: missing binding metadata", case.name));
                assert_eq!(
                    binding.bound_addr.ip(),
                    configured_addr.ip(),
                    "{}",
                    case.name
                );
                assert_ne!(binding.bound_addr.port(), 0, "{}", case.name);
                assert_eq!(binding.used_fallback, case.expect_fallback, "{}", case.name);
                if case.expect_fallback {
                    assert_ne!(
                        binding.bound_addr.port(),
                        configured_addr.port(),
                        "{}",
                        case.name
                    );
                    assert_eq!(proxy.fallback_descriptions().len(), 1, "{}", case.name);
                } else if configured_addr.port() != 0 {
                    assert_eq!(binding.bound_addr, configured_addr, "{}", case.name);
                    assert!(proxy.fallback_descriptions().is_empty(), "{}", case.name);
                } else {
                    assert!(proxy.fallback_descriptions().is_empty(), "{}", case.name);
                }
            } else {
                match result {
                    Err(ProxyError::Bind { source, .. }) => {
                        assert_eq!(
                            source.kind(),
                            std::io::ErrorKind::AddrInUse,
                            "{}",
                            case.name
                        );
                    }
                    Err(error) => panic!("{}: unexpected error: {error}", case.name),
                    Ok(_) => panic!("{}: expected bind failure", case.name),
                }
            }

            drop(blocker);
        }
    }

    #[tokio::test]
    async fn proxy_metadata_preserves_mixed_declaration_order() {
        let entries = vec![
            ProxyEntry {
                listen: "127.0.0.1:0".to_string(),
                mode: ProxyMode::Listenfd,
            },
            ProxyEntry {
                listen: "127.0.0.1:0".to_string(),
                mode: ProxyMode::Env("api_port".to_string()),
            },
            ProxyEntry {
                listen: "127.0.0.1:0".to_string(),
                mode: ProxyMode::Forward("127.0.0.1:9".to_string()),
            },
            ProxyEntry {
                listen: "127.0.0.1:0".to_string(),
                mode: ProxyMode::Listenfd,
            },
        ];
        let emitter = test_lifecycle_emitter().await;
        let proxy = ServiceProxy::bind(&entries, true, None, "mixed", emitter)
            .await
            .unwrap();
        let bindings = proxy.bindings();

        assert_eq!(bindings.len(), 4);
        assert!(matches!(&bindings[0].mode, ProxyBindingMode::Listenfd));
        assert!(matches!(
            &bindings[1].mode,
            ProxyBindingMode::Env { env_name } if env_name == "api_port"
        ));
        assert!(matches!(
            &bindings[2].mode,
            ProxyBindingMode::Forward { target } if *target == "127.0.0.1:9".parse().unwrap()
        ));
        assert!(matches!(&bindings[3].mode, ProxyBindingMode::Listenfd));

        let actual_addrs: Vec<SocketAddr> =
            bindings.iter().map(|binding| binding.bound_addr).collect();
        assert_eq!(proxy.listen_addrs(), actual_addrs);
        assert_eq!(
            proxy.descriptions(),
            vec![
                format!("{} (listenfd)", actual_addrs[0]),
                format!("{} (env=api_port)", actual_addrs[1]),
                format!("{} → 127.0.0.1:9", actual_addrs[2]),
                format!("{} (listenfd)", actual_addrs[3]),
            ]
        );

        let public_env = proxy.public_env_vars();
        for (idx, addr) in actual_addrs.iter().enumerate() {
            assert_eq!(
                public_env.get(&format!("DON_PUBLIC_ADDR_{idx}")),
                Some(&addr.to_string())
            );
            assert_eq!(
                public_env.get(&format!("DON_PUBLIC_PORT_{idx}")),
                Some(&addr.port().to_string())
            );
        }
        assert_eq!(
            public_env.get("DON_PUBLIC_ADDR"),
            Some(&actual_addrs[0].to_string())
        );
        assert_eq!(
            public_env.get("DON_PUBLIC_API_PORT"),
            Some(&actual_addrs[1].port().to_string())
        );
        assert_eq!(
            public_env.get("DON_PUBLIC_API_PORT_ADDR"),
            Some(&actual_addrs[1].to_string())
        );

        let references = proxy.env_reference_values();
        assert_eq!(references.get("addr"), Some(&actual_addrs[0].to_string()));
        assert_eq!(
            references.get("port_2"),
            Some(&actual_addrs[2].port().to_string())
        );
        assert_eq!(
            references.get("API_PORT"),
            Some(&actual_addrs[1].port().to_string())
        );
        assert_eq!(
            references.get("API_PORT_ADDR"),
            Some(&actual_addrs[1].to_string())
        );

        let listenfd_env = proxy.listenfd_env();
        assert_eq!(
            listenfd_env.get("LISTEN_FDNAMES"),
            Some(&format!("{}:{}", actual_addrs[0], actual_addrs[3]))
        );
        assert!(proxy.fallback_descriptions().is_empty());

        let cloned = proxy.bindings().to_vec();
        drop(proxy);
        assert_eq!(cloned.len(), 4);
        assert_eq!(cloned[1].bound_addr, actual_addrs[1]);
    }

    #[test]
    fn wildcard_public_addresses_use_loopback_for_clients() {
        let cases = vec![
            (
                SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 3000),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3000),
            ),
            (
                SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 3000),
                SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 3000),
            ),
        ];

        for (bound_addr, expected) in cases {
            let binding = ProxyBinding {
                configured_addr: bound_addr.to_string(),
                bound_addr,
                mode: ProxyBindingMode::Listenfd,
                used_fallback: false,
            };
            assert_eq!(binding.connect_addr(), expected);
        }
    }

    #[tokio::test]
    async fn failed_bind_releases_all_earlier_pending_listeners() {
        let first_reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let first_addr = first_reservation.local_addr().unwrap();
        drop(first_reservation);

        let _blocker = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let blocked_addr = _blocker.local_addr().unwrap();
        let entries = vec![
            ProxyEntry {
                listen: first_addr.to_string(),
                mode: ProxyMode::Env("PORT".to_string()),
            },
            ProxyEntry {
                listen: blocked_addr.to_string(),
                mode: ProxyMode::Listenfd,
            },
        ];
        let emitter = test_lifecycle_emitter().await;

        let result = ServiceProxy::bind(&entries, false, None, "transactional", emitter).await;
        assert!(matches!(result, Err(ProxyError::Bind { .. })));

        let rebound = tokio::net::TcpListener::bind(first_addr).await;
        assert!(
            rebound.is_ok(),
            "the first listener remained owned after a later bind failed"
        );
    }

    #[test]
    fn loopback_ipv4_backends_try_ipv6_loopback_too() {
        let candidates =
            backend_connect_candidates(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 46165));

        assert_eq!(
            candidates,
            vec![
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 46165),
                SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 46165),
            ]
        );
    }

    #[test]
    fn loopback_ipv6_backends_try_ipv4_loopback_too() {
        let candidates =
            backend_connect_candidates(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 46165));

        assert_eq!(
            candidates,
            vec![
                SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 46165),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 46165),
            ]
        );
    }

    #[test]
    fn non_loopback_backends_keep_single_target() {
        let target = IpAddr::V6(Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 0x0010));
        let candidates = backend_connect_candidates(SocketAddr::new(target, 46165));

        assert_eq!(candidates, vec![SocketAddr::new(target, 46165)]);
    }

    #[test]
    fn proxy_connection_limit_reserves_fds_for_non_proxy_work() {
        let soft_limit = 1024;

        assert_eq!(
            proxy_connection_limit_for_soft_nofile(soft_limit),
            (soft_limit - PROXY_FD_RESERVE) / 2
        );
    }

    #[test]
    fn proxy_connection_limit_is_capped_for_large_fd_limits() {
        assert_eq!(
            proxy_connection_limit_for_soft_nofile(1_000_000),
            DEFAULT_PROXY_CONNECTION_LIMIT
        );
    }

    #[test]
    fn proxy_connection_limit_keeps_a_small_floor() {
        assert_eq!(
            proxy_connection_limit_for_soft_nofile(64),
            MIN_PROXY_CONNECTION_LIMIT
        );
    }

    #[test]
    fn accept_error_retry_classification_table() {
        struct Case {
            name: &'static str,
            errno: i32,
            retryable: bool,
        }

        let cases = vec![
            Case {
                name: "peer aborted before accept",
                errno: libc::ECONNABORTED,
                retryable: true,
            },
            Case {
                name: "protocol error belongs to queued peer",
                errno: libc::EPROTO,
                retryable: true,
            },
            Case {
                name: "listener descriptor failure is fatal",
                errno: libc::EBADF,
                retryable: false,
            },
        ];

        for case in cases {
            let error = std::io::Error::from_raw_os_error(case.errno);
            assert_eq!(
                is_retryable_accept_error(&error),
                case.retryable,
                "case '{}'",
                case.name,
            );
        }
    }

    #[tokio::test]
    async fn proxy_accept_loop_does_not_reserve_permits_while_idle() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = listener.local_addr().unwrap();
        let (_backend_tx, backend_rx) = watch::channel(None);
        let initial_command = ControllerCommand {
            control: ForwardControl::Accepting,
            failure_epoch: 0,
        };
        let (control_tx, control_rx) = watch::channel(initial_command);
        let (status_tx, _status_rx) = watch::channel(Ok(initial_command));
        let (lazy_tx, mut lazy_rx) = mpsc::channel(1);
        let permits = Arc::new(Semaphore::new(1));
        let global_active_connections = Arc::new(AtomicUsize::new(0));
        let active_connections = Arc::new(AtomicUsize::new(0));
        let emitter = test_lifecycle_emitter().await;
        let accounting = ProxyConnectionAccounting {
            permits: permits.clone(),
            max_connections: 1,
            global_active_connections: global_active_connections.clone(),
            service_active_connections: active_connections.clone(),
        };

        let handle = tokio::spawn(proxy_accept_loop_with_permits(
            listener,
            ForwardControllerConfig {
                backend_rx,
                control_rx,
                status_tx,
                initial_command,
                lazy_tx: Some(lazy_tx),
                service_name: "svc".to_string(),
                emitter,
            },
            accounting,
        ));

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(
            permits.available_permits(),
            1,
            "idle listeners must not consume active connection permits"
        );
        assert_eq!(
            active_connections.load(Ordering::Relaxed),
            0,
            "idle listeners must not count as active connections"
        );
        assert_eq!(
            global_active_connections.load(Ordering::Relaxed),
            0,
            "idle listeners must not count against the global pool"
        );

        let _client = tokio::net::TcpStream::connect(listen_addr).await.unwrap();
        let triggered = tokio::time::timeout(std::time::Duration::from_secs(1), lazy_rx.recv())
            .await
            .unwrap();
        assert_eq!(
            triggered
                .as_ref()
                .map(|trigger| trigger.service_name.as_str()),
            Some("svc")
        );
        assert_eq!(
            active_connections.load(Ordering::Relaxed),
            1,
            "accepted connections waiting for a backend should be counted"
        );
        assert_eq!(
            global_active_connections.load(Ordering::Relaxed),
            1,
            "accepted connections should count against the global pool"
        );

        drop(control_tx);
        handle.abort();
        let _ = handle.await;
        assert_eq!(
            active_connections.load(Ordering::Relaxed),
            0,
            "abandoned connections should release active counts"
        );
        assert_eq!(
            global_active_connections.load(Ordering::Relaxed),
            0,
            "abandoned connections should release global active counts"
        );
    }

    #[tokio::test]
    async fn listenfd_start_barrier_ignores_stale_disarmed_status() {
        let desired = ControllerCommand {
            control: ListenfdControl::Disarmed,
            failure_epoch: 2,
        };
        let (control_tx, _control_rx) = watch::channel(desired);
        let (status_tx, status_rx) = watch::channel(Ok(ControllerCommand {
            control: ListenfdControl::Disarmed,
            failure_epoch: 0,
        }));
        let barrier = ListenfdStartBarrier {
            controllers: vec![super::Controller {
                command_tx: control_tx,
                status_rx,
            }],
        };
        let mut waiter = tokio::spawn(barrier.wait());

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut waiter)
                .await
                .is_err(),
            "a stale Disarmed acknowledgement must not release the start"
        );
        let _ = status_tx.send_replace(Ok(ControllerCommand {
            control: ListenfdControl::Disarmed,
            failure_epoch: 2,
        }));
        assert!(waiter.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn lazy_rearm_prepares_all_controllers_before_global_enable() {
        let entries = vec![
            ProxyEntry {
                listen: "127.0.0.1:0".to_string(),
                mode: ProxyMode::Env("PORT".to_string()),
            },
            ProxyEntry {
                listen: "127.0.0.1:0".to_string(),
                mode: ProxyMode::Listenfd,
            },
        ];
        let (lazy_tx, mut lazy_rx) = mpsc::channel(8);
        let mut proxy = ServiceProxy::bind(
            &entries,
            false,
            Some(lazy_tx),
            "svc",
            test_lifecycle_emitter().await,
        )
        .await
        .unwrap();

        proxy.begin_lazy_failure_recovery().wait().await.unwrap();
        proxy.begin_lazy_rearm().wait().await.unwrap();
        let mut clients = Vec::new();
        for addr in proxy.listen_addrs() {
            clients.push(tokio::net::TcpStream::connect(addr).await.unwrap());
        }

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), lazy_rx.recv())
                .await
                .is_err(),
            "prepared controllers must not trigger before global enable"
        );

        proxy.set_connection_state(false, true);
        for _ in 0..entries.len() {
            let triggered = tokio::time::timeout(std::time::Duration::from_secs(1), lazy_rx.recv())
                .await
                .unwrap();
            assert_eq!(
                triggered
                    .as_ref()
                    .map(|trigger| trigger.service_name.as_str()),
                Some("svc")
            );
        }
        drop(clients);
    }

    #[tokio::test]
    async fn forward_connection_observes_coalesced_failure_epoch() {
        let initial = ControllerCommand {
            control: ForwardControl::Accepting,
            failure_epoch: 0,
        };
        let (control_tx, mut control_rx) = watch::channel(initial);
        control_tx.send_replace(ControllerCommand {
            control: ForwardControl::Rejecting,
            failure_epoch: 1,
        });
        control_tx.send_replace(ControllerCommand {
            control: ForwardControl::Accepting,
            failure_epoch: 1,
        });

        tokio::time::timeout(
            std::time::Duration::from_millis(50),
            wait_for_forward_rejection(&mut control_rx, initial.failure_epoch),
        )
        .await
        .expect("the failed connection epoch should close immediately");
    }

    #[tokio::test]
    async fn forward_actor_drains_backlog_after_coalesced_failure_epoch() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = listener.local_addr().unwrap();
        let initial = ControllerCommand {
            control: ForwardControl::Accepting,
            failure_epoch: 0,
        };
        let final_command = ControllerCommand {
            control: ForwardControl::Accepting,
            failure_epoch: 1,
        };
        let (control_tx, control_rx) = watch::channel(initial);
        let (status_tx, status_rx) = watch::channel(Ok(initial));
        let (_backend_tx, backend_rx) = watch::channel(None);
        let (lazy_tx, mut lazy_rx) = mpsc::channel(8);
        let stale_clients = vec![
            tokio::net::TcpStream::connect(listen_addr).await.unwrap(),
            tokio::net::TcpStream::connect(listen_addr).await.unwrap(),
            tokio::net::TcpStream::connect(listen_addr).await.unwrap(),
        ];

        control_tx.send_replace(ControllerCommand {
            control: ForwardControl::Rejecting,
            failure_epoch: 1,
        });
        control_tx.send_replace(final_command);

        let permits = Arc::new(Semaphore::new(8));
        let handle = tokio::spawn(proxy_accept_loop_with_permits(
            listener,
            ForwardControllerConfig {
                backend_rx,
                control_rx,
                status_tx,
                initial_command: initial,
                lazy_tx: Some(lazy_tx),
                service_name: "svc".to_string(),
                emitter: test_lifecycle_emitter().await,
            },
            ProxyConnectionAccounting {
                permits,
                max_connections: 8,
                global_active_connections: Arc::new(AtomicUsize::new(0)),
                service_active_connections: Arc::new(AtomicUsize::new(0)),
            },
        ));
        let mut controller = Controller {
            command_tx: control_tx,
            status_rx,
        };
        controller.wait_for(final_command).await.unwrap();

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), lazy_rx.recv())
                .await
                .is_err(),
            "failed-cohort backlog must not trigger a new lazy start"
        );
        for client in stale_clients {
            assert_stream_closed(client).await;
        }

        let _fresh_client = tokio::net::TcpStream::connect(listen_addr).await.unwrap();
        let triggered = tokio::time::timeout(std::time::Duration::from_secs(1), lazy_rx.recv())
            .await
            .unwrap();
        assert_eq!(
            triggered
                .as_ref()
                .map(|trigger| trigger.service_name.as_str()),
            Some("svc")
        );

        handle.abort();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn listenfd_actor_drains_coalesced_epoch_and_restores_blocking() {
        let listener = Arc::new(std::net::TcpListener::bind("127.0.0.1:0").unwrap());
        let listen_addr = listener.local_addr().unwrap();
        let initial = ControllerCommand {
            control: ListenfdControl::Armed,
            failure_epoch: 0,
        };
        let final_command = ControllerCommand {
            control: ListenfdControl::Armed,
            failure_epoch: 1,
        };
        let (control_tx, control_rx) = watch::channel(initial);
        let (status_tx, status_rx) = watch::channel(Ok(initial));
        let (lazy_tx, mut lazy_rx) = mpsc::channel(8);
        let stale_clients = vec![
            tokio::net::TcpStream::connect(listen_addr).await.unwrap(),
            tokio::net::TcpStream::connect(listen_addr).await.unwrap(),
            tokio::net::TcpStream::connect(listen_addr).await.unwrap(),
        ];

        control_tx.send_replace(ControllerCommand {
            control: ListenfdControl::Rejecting,
            failure_epoch: 1,
        });
        control_tx.send_replace(final_command);

        let handle = spawn_listenfd_controller(
            listener.clone(),
            control_rx,
            status_tx,
            initial,
            Some(lazy_tx),
            "svc".to_string(),
            test_lifecycle_emitter().await,
        );
        let mut controller = Controller {
            command_tx: control_tx,
            status_rx,
        };
        controller.wait_for(final_command).await.unwrap();

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), lazy_rx.recv())
                .await
                .is_err(),
            "failed-cohort listenfd backlog must not trigger a new lazy start"
        );
        for client in stale_clients {
            assert_stream_closed(client).await;
        }
        // Safety: fcntl reads the status flags for the live listener fd.
        let flags = unsafe { libc::fcntl(listener.as_raw_fd(), libc::F_GETFL) };
        assert!(flags >= 0);
        assert_eq!(
            flags & libc::O_NONBLOCK,
            0,
            "listenfd must be blocking before the armed phase is acknowledged"
        );

        let _fresh_client = tokio::net::TcpStream::connect(listen_addr).await.unwrap();
        let triggered = tokio::time::timeout(std::time::Duration::from_secs(1), lazy_rx.recv())
            .await
            .unwrap();
        assert_eq!(
            triggered
                .as_ref()
                .map(|trigger| trigger.service_name.as_str()),
            Some("svc")
        );

        handle.abort();
        let _ = handle.await;
    }

    async fn assert_stream_closed(mut stream: tokio::net::TcpStream) {
        let mut byte = [0_u8; 1];
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(1), stream.read(&mut byte))
                .await
                .expect("failed-cohort connection did not close");
        match result {
            Ok(0) | Err(_) => {}
            Ok(count) => panic!("failed-cohort connection returned {count} unexpected bytes"),
        }
    }

    async fn test_lifecycle_emitter() -> LifecycleEmitter {
        let log_config = LogConfig::Stdout;
        let services = [("svc", &log_config)];
        let output_manager = OutputManager::new(&services, tokio::io::sink())
            .await
            .unwrap();
        output_manager.clone_lifecycle_emitter()
    }
}
