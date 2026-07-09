//! Coordination of terminal ownership between the runner and the TUI for
//! foreground tasks.
//!
//! A foreground task takes over stdin/stdout/stderr and the controlling
//! terminal's foreground process group. While it runs, the TUI must release
//! the terminal — drop raw mode, leave the inline viewport, stop the input
//! task — so the task's prompts and reads work without interference. After
//! the task exits, the TUI re-acquires the terminal and resumes rendering.
//!
//! The runner uses [`TerminalCoordinator::acquire`] just before launching a
//! foreground task and [`TerminalCoordinator::release`] after the task
//! completes. In non-TUI runs the coordinator is detached and the calls are
//! no-ops.

use tokio::sync::{mpsc, oneshot};

/// Request from the runner to the TUI for terminal ownership transitions.
pub enum TerminalRequest {
    /// Pause the TUI: tear down terminal-side state and ack so the runner
    /// can launch the foreground task.
    Acquire(oneshot::Sender<()>),
    /// Resume the TUI: re-take terminal ownership after the foreground task
    /// has released it.
    Release,
}

/// Handle the runner uses to coordinate with whoever owns the terminal.
///
/// Cloneable so worker tasks (which actually spawn the foreground process)
/// can call `acquire`/`release` independently of the runner's main loop.
#[derive(Clone)]
pub struct TerminalCoordinator {
    tx: Option<mpsc::Sender<TerminalRequest>>,
}

impl TerminalCoordinator {
    /// No-op coordinator for non-TUI runs (pipe mode, `--no-tui`, non-tty).
    pub fn detached() -> Self {
        Self { tx: None }
    }

    /// Coordinator that drives a [`TerminalRequest`] consumer (the TUI).
    pub fn with_channel(tx: mpsc::Sender<TerminalRequest>) -> Self {
        Self { tx: Some(tx) }
    }

    /// True when no interactive terminal is attached (pipe mode, `--no-tui`,
    /// non-tty, or a detached daemon) — foreground tasks can't take the terminal.
    pub fn is_detached(&self) -> bool {
        self.tx.is_none()
    }

    /// Pause the TUI and wait for it to confirm it has released the
    /// terminal. No-op when detached.
    pub async fn acquire(&self) {
        let Some(tx) = &self.tx else { return };
        let (ack_tx, ack_rx) = oneshot::channel();
        if tx.send(TerminalRequest::Acquire(ack_tx)).await.is_err() {
            return;
        }
        let _ = ack_rx.await;
    }

    /// Tell the TUI to resume rendering. No-op when detached.
    pub async fn release(&self) {
        let Some(tx) = &self.tx else { return };
        let _ = tx.send(TerminalRequest::Release).await;
    }
}
