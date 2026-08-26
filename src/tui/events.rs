//! Internal event type bridging crossterm input to the main TUI loop.
//!
//! The input task forwards raw key events; the main loop interprets them
//! based on the current [`ViewMode`](super::app::ViewMode). This keeps the
//! input pump oblivious to UI state, which avoids the Arc<Mutex<_>> dance.

use crossterm::event::{KeyEvent, MouseEvent};

use crate::client::{CompletionError, ProcessStatus};

/// Event delivered from the input task to the main TUI loop.
#[derive(Debug, Clone)]
pub(crate) enum AppEvent {
    /// A key press (release events are filtered out upstream).
    Key(KeyEvent),
    /// A mouse click, drag, release or wheel tick. Bare motion is filtered out
    /// upstream; its column and row are screen coordinates, which only the
    /// renderer's layout can turn into "which pane, which row".
    ///
    /// Stamped by the input task when it read the event, not by the loop when
    /// it got to it. Multi-click is a question about the gap between two
    /// presses, and on a slow link the loop can be several hundred
    /// milliseconds behind the terminal — long enough for a real double click
    /// to be handled far enough apart to read as two single ones.
    Mouse(MouseEvent, std::time::Instant),
    /// Terminal was resized. Ratatui picks up the new size on the next draw;
    /// this event just triggers an immediate repaint.
    Resize,
    /// Async result of a `RunnerCommand::ResolveCompletions` request.
    /// Delivered back into the main TUI loop from a detached tokio task —
    /// that way a slow completion command doesn't stall rendering or key
    /// handling.
    CompletionsReady {
        /// The param name the result belongs to. The form ignores the event
        /// if the user has since moved past or cancelled this field.
        param: String,
        /// Token identifying the specific request. Lets the form drop stale
        /// replies (e.g. user typed faster than the resolver returned).
        request_id: u64,
        /// Either the list of candidate values, or the resolver's error
        /// (which the form renders inline with a pointer to the log file).
        result: Result<Vec<String>, CompletionError>,
    },
    /// A fresh state projection, fetched after the event stream reported
    /// lag. Injected through the input channel so it applies in order with
    /// user input rather than racing the render loop.
    StateResync {
        processes: Vec<ProcessStatus>,
        startup_complete: bool,
    },
    /// The attached process's screen changed, or the session ended.
    ///
    /// Arrives through the same channel as input so a redraw of the window
    /// orders with the keys that caused it, rather than racing them.
    Attach(AttachEvent),
}

/// What the attach session has to say to the main loop.
#[derive(Debug, Clone)]
pub(crate) enum AttachEvent {
    /// New output landed; the window's grid needs re-reading.
    Output,
    /// The session is over — the process exited or the connection failed.
    ///
    /// Carries no reason. The window stays up showing the process's last
    /// screen, and the state don already publishes for that process says what
    /// became of it far better than a message composed at the socket could.
    Ended,
}
