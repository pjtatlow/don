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
    terminal: bool,
}

impl TerminalCoordinator {
    /// No-op coordinator for runs without an interactive terminal (detached
    /// daemon, tests, piped stdout). Foreground tasks fall back to a normal
    /// PTY spawn and clients bridge in via attach.
    pub fn detached() -> Self {
        Self {
            tx: None,
            terminal: false,
        }
    }

    /// No-op coordinator for pipe-mode runs that still own a real terminal
    /// (`don start --no-tui` in an interactive shell). Foreground tasks take
    /// over the terminal directly; there is just no TUI to pause.
    pub fn detached_with_terminal() -> Self {
        Self {
            tx: None,
            terminal: true,
        }
    }

    /// Coordinator that drives a [`TerminalRequest`] consumer (the TUI).
    pub fn with_channel(tx: mpsc::Sender<TerminalRequest>) -> Self {
        Self {
            tx: Some(tx),
            terminal: true,
        }
    }

    /// Whether this run can hand a foreground task the real terminal.
    pub fn terminal_available(&self) -> bool {
        self.terminal
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

/// Whether this process's stdin is an interactive terminal — the same check
/// [`crate::process`]'s foreground spawn performs before taking the terminal.
pub(crate) fn has_interactive_terminal() -> bool {
    use std::io::IsTerminal;
    std::io::stdin().is_terminal()
}
