//! Bridges the daemon's unix-socket API into the in-process channels that
//! [`crate::run_tui`] consumes, so a remote `don tui` frontend drives the exact
//! same render loop as an in-process runner would.
//!
//! ```text
//!   daemon socket                         run_tui inputs
//!   ─────────────                         ──────────────
//!   GET /logstream ─┐                  ┌▶ log_rx        (FormattedLogLine)
//!   local emitter  ─┴─(merge)──────────┘
//!   GET /events ──────(deserialize)───▶  events_rx     (RunnerEvent)
//!   POST /start,/stop,… ◀─(translate)──  command_tx    (RunnerCommand)
//!   POST /verbose ◀────(sync)──────────  verbosity      (v-key toggle)
//! ```
//!
//! The local serviceless [`OutputManager`] exists only to supply a real
//! [`VerbosityControl`] and [`LifecycleEmitter`] (for instant local action
//! feedback, formatted identically to the daemon), plus the merge half of the
//! log stream. When the daemon's `/logstream` hits EOF (shutdown or lost
//! socket) the bridge tears its tasks down so `log_rx` closes and `run_tui`
//! exits cleanly.

use crate::client::{Client, ClientError, RunTaskOptions};
use crate::output::{FormattedLogLine, LifecycleEmitter, OutputManager, VerbosityControl};
use crate::runner::{
    CommandError, CompletionError, ItemStatus, RunnerCommand, RunnerEvent, TerminalRequest,
    TuiSnapshot,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

/// Capacity of the local event rebroadcast channel. Generous so the TUI never
/// lags behind low-frequency runner events.
const EVENT_CHANNEL_CAP: usize = 1024;
/// How often the bridge reconciles the local verbose flag with the daemon.
const VERBOSE_SYNC_INTERVAL: Duration = Duration::from_millis(300);

/// The `run_tui` inputs produced by the bridge, plus a [`BridgeGuard`] that
/// must stay alive for the lifetime of the TUI.
pub struct TuiBridge {
    /// Daemon-authoritative seed data (active set, current state, flags).
    pub snapshot: TuiSnapshot,
    pub log_rx: mpsc::UnboundedReceiver<FormattedLogLine>,
    pub events_rx: broadcast::Receiver<RunnerEvent>,
    pub command_tx: mpsc::UnboundedSender<RunnerCommand>,
    pub verbosity: VerbosityControl,
    pub lifecycle_emitter: LifecycleEmitter,
    pub terminal_request_rx: mpsc::Receiver<TerminalRequest>,
    /// Keep-alive for background tasks and the local output manager. Dropping
    /// it aborts every bridge task and tears down the local output pipeline.
    pub guard: BridgeGuard,
}

/// Owns the bridge's background tasks and the local output manager. Aborts all
/// tasks on drop so a finished TUI leaves nothing running.
pub struct BridgeGuard {
    _output_manager: OutputManager,
    /// Held so `events_rx` stays open even across an event-stream reconnect.
    _events_tx: broadcast::Sender<RunnerEvent>,
    /// Held so the TUI's `terminal_request_rx.recv()` parks instead of seeing
    /// a closed channel and busy-spinning. A remote frontend has no terminal
    /// to hand to foreground tasks, so this sender is never driven — foreground
    /// tasks run on their PTY and surface in the log pane; `don run`/`don attach`
    /// from a terminal bridges them (see the headless foreground handling).
    _terminal_request_tx: mpsc::Sender<TerminalRequest>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for BridgeGuard {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl TuiBridge {
    /// Connect to the daemon at `<base>/.don/don.sock`, fetch the seed
    /// snapshot, and wire up the streaming + command bridge tasks.
    pub async fn connect(base_dir: &Path) -> Result<Self, ClientError> {
        let base = base_dir.to_path_buf();
        let client = Client::new(&base);

        // Snapshot first: confirms the daemon is reachable and seeds the view.
        let snapshot = client.snapshot().await?;

        // Local serviceless output manager → verbosity + lifecycle emitter +
        // the local feedback half of the log stream.
        let (output_manager, mut local_log_rx) =
            OutputManager::new_with_tui_and_log_filters(&[], &HashMap::new(), snapshot.verbose)
                .await
                .map_err(|e| ClientError::Invalid(format!("local output init: {e}")))?;
        let verbosity = output_manager.verbosity_control();
        let lifecycle_emitter = output_manager.clone_lifecycle_emitter();

        // Merged log stream feeding run_tui.
        let (log_tx, log_rx) = mpsc::unbounded_channel::<FormattedLogLine>();
        // Event rebroadcast feeding run_tui.
        let (events_tx, events_rx) = broadcast::channel::<RunnerEvent>(EVENT_CHANNEL_CAP);

        // Seed the frontend with current state by replaying the snapshot as
        // synthetic events. The live `/events` stream only carries changes
        // from here on, so without this an attaching viewer would show every
        // item as Pending until something next transitions. `events_rx` is
        // already subscribed above, so these buffer for run_tui to drain first.
        for status in &snapshot.statuses {
            let event = match status {
                ItemStatus::Service { name, state, .. } => RunnerEvent::ServiceStateChanged {
                    name: name.clone(),
                    state: *state,
                    pid: None,
                },
                ItemStatus::Task {
                    name,
                    state,
                    last_run,
                    ..
                } => RunnerEvent::TaskStateChanged {
                    name: name.clone(),
                    state: *state,
                    last_run: last_run.clone(),
                },
            };
            let _ = events_tx.send(event);
        }
        // Commands from the TUI → HTTP translator.
        let (command_tx, command_rx) = mpsc::unbounded_channel::<RunnerCommand>();
        // Foreground-task terminal coordination (driven in Phase 4).
        let (terminal_request_tx, terminal_request_rx) = mpsc::channel::<TerminalRequest>(8);

        let mut tasks: Vec<JoinHandle<()>> = Vec::new();

        // Merge task: local emitter lines → merged log stream.
        let merge_handle = {
            let log_tx = log_tx.clone();
            tokio::spawn(async move {
                while let Some(line) = local_log_rx.recv().await {
                    if log_tx.send(line).is_err() {
                        break;
                    }
                }
            })
        };
        let merge_abort = merge_handle.abort_handle();
        tasks.push(merge_handle);

        // Log stream task: daemon /logstream → merged log stream. On EOF
        // (daemon gone), abort the merge task so both senders drop, closing
        // `log_rx` and exiting run_tui.
        tasks.push({
            let base = base.clone();
            let log_tx = log_tx.clone();
            tokio::spawn(async move {
                let client = Client::new(&base);
                let _ = client
                    .stream_logs(|line| {
                        let _ = log_tx.send(line);
                    })
                    .await;
                merge_abort.abort();
            })
        });
        // Drop the bridge's own retained sender so the only remaining clones
        // live inside the two tasks above; once both end, `log_rx` closes.
        drop(log_tx);

        // Event stream task: daemon /events → local rebroadcast feeding the
        // TUI's apply_runner_event.
        tasks.push({
            let base = base.clone();
            let events_tx = events_tx.clone();
            tokio::spawn(async move {
                let client = Client::new(&base);
                let _ = client
                    .stream_events(|event| {
                        let _ = events_tx.send(event);
                    })
                    .await;
            })
        });

        // Command translator task: TUI RunnerCommands → HTTP calls.
        tasks.push({
            let base = base.clone();
            tokio::spawn(async move { run_command_bridge(base, command_rx).await })
        });

        // Verbose sync task: mirror the local `v` toggle onto the daemon.
        tasks.push({
            let base = base.clone();
            let verbosity = verbosity.clone();
            let mut last = snapshot.verbose;
            tokio::spawn(async move {
                let client = Client::new(&base);
                let mut tick = tokio::time::interval(VERBOSE_SYNC_INTERVAL);
                loop {
                    tick.tick().await;
                    let current = verbosity.is_enabled();
                    if current != last {
                        last = current;
                        let _ = client.set_verbose(current).await;
                    }
                }
            })
        });

        Ok(Self {
            snapshot,
            log_rx,
            events_rx,
            command_tx,
            verbosity,
            lifecycle_emitter,
            terminal_request_rx,
            guard: BridgeGuard {
                _output_manager: output_manager,
                _events_tx: events_tx,
                _terminal_request_tx: terminal_request_tx,
                tasks,
            },
        })
    }
}

/// Consume `RunnerCommand`s from the TUI and fulfil them via HTTP. Only the
/// commands the TUI actually emits are handled; the rest are runner-internal.
async fn run_command_bridge(base: PathBuf, mut command_rx: mpsc::UnboundedReceiver<RunnerCommand>) {
    let client = Client::new(&base);
    while let Some(cmd) = command_rx.recv().await {
        match cmd {
            RunnerCommand::Start { name, reply } => {
                let _ = reply.send(to_command_result(&name, client.start(&name).await));
            }
            RunnerCommand::Stop { name, reply } => {
                let _ = reply.send(to_command_result(&name, client.stop(&name).await));
            }
            RunnerCommand::Restart { name, reply } => {
                let _ = reply.send(to_command_result(&name, client.restart(&name).await));
            }
            RunnerCommand::HardRestart { name, reply } => {
                let _ = reply.send(to_command_result(&name, client.hard_restart(&name).await));
            }
            RunnerCommand::RunTask {
                name,
                params,
                wait,
                wait_timeout,
                reply,
            } => {
                let result = client
                    .run_task_with_options(&name, params, RunTaskOptions { wait, wait_timeout })
                    .await;
                let _ = reply.send(to_command_result(&name, result));
            }
            RunnerCommand::ResolveCompletions {
                task,
                param,
                partial,
                force_refresh,
                reply,
            } => {
                let result = client
                    .resolve_completions(&task, &param, partial, force_refresh)
                    .await;
                let mapped = match result {
                    Ok(values) => Ok(values),
                    Err(ClientError::Completion(ce)) => Err(ce),
                    Err(e) => Err(CompletionError {
                        message: e.to_string(),
                        log_path: None,
                    }),
                };
                let _ = reply.send(mapped);
            }
            RunnerCommand::Shutdown => {
                let _ = client.shutdown().await;
            }
            // Every other command is runner-internal (watch triggers, attach,
            // status, snapshot) and is never sent by the TUI render loop.
            _ => {}
        }
    }
}

/// Convert an HTTP control result into the `CommandResult` the TUI expects,
/// avoiding a doubled `"name: "` prefix — daemon error messages already carry
/// the item name.
fn to_command_result(name: &str, result: Result<(), ClientError>) -> Result<(), CommandError> {
    result.map_err(|e| {
        let raw = e.to_string();
        let message = raw
            .strip_prefix(&format!("{name}: "))
            .map(str::to_string)
            .unwrap_or(raw);
        CommandError::Failed {
            name: name.to_string(),
            message,
        }
    })
}
