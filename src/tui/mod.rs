//! Interactive terminal UI for `don start`.
//!
//! The TUI owns the whole screen: raw mode, the alternate screen, one ratatui
//! [`Terminal`] with a full-screen viewport. Pipe mode (non-TTY) bypasses this
//! module entirely — [`OutputManager`] still writes prefixed bytes directly to
//! stdout in that case.
//!
//! Heavy formatting (color prefix, ANSI sanitization, verbose timestamps) is
//! already done upstream; we parse those pre-rendered ANSI bytes back into
//! styled [`Line`]s via `ansi-to-tui` so ratatui can render them.
//!
//! ## Why one screen
//!
//! This used to be a hybrid: logs went into the terminal's *native* scrollback
//! through `Terminal::insert_before`, a one-row inline viewport held the status
//! bar, and every full-screen view rendered into a **second** alt-screen
//! `Terminal` layered on top. Two screens meant two histories, and the seam
//! between them needed constant repair — replay checkpoints to re-emit lines
//! that arrived while a modal was up, a clear-and-replay pass on every filter
//! change, and a backend wrapper whose entire job was dodging the cursor-position
//! query that an inline viewport forces.
//!
//! None of that exists here. There is one buffer, and [`LogStore`] is the only
//! history. A filter change is a different *view* of the same store rather than
//! a screen wipe and a replay, which is what makes it impossible for the log to
//! drift out of sync with what the user asked to see.
//!
//! ## Concurrency
//!
//! One `tokio::select!` loop owns [`App`], the [`Terminal`] and the
//! [`LogStore`]. Its arms never draw — they mutate state and mark it dirty, and
//! a single rate-capped arm draws the whole screen. So a burst of ten thousand
//! log lines costs one repaint rather than ten thousand writes, and no arm has
//! to know what any other arm would have wanted redrawn.
//!
//! Four side channels feed it:
//! - Merged log events from [`OutputManager`], carrying don's own [`LogId`]s.
//! - [`RunnerEvent`]s from the runner broadcast (consumed directly, no side task).
//! - Input events from the input task (interpretation is mode-dependent).
//! - A timer, for the spinner and relative timestamps.
//!
//! [`OutputManager`]: crate::output::OutputManager
//! [`Terminal`]: ratatui::Terminal
//! [`LogId`]: crate::output::LogId
//! [`Line`]: ratatui::text::Line

mod app;
mod attach_session;
mod attach_window;
mod events;
mod failure_summary;
mod filter;
mod form;
mod fuzzy;
mod input;
mod keys;
mod log_store;
mod logs;
mod panes;
mod render;
mod selection;
mod status_table;
mod view_index;
mod writer;

use ansi_to_tui::IntoText;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
use crossterm::execute;
use crossterm::style::Print;
use crossterm::terminal::{EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::Rect;
use ratatui::text::Text;
use ratatui::{TerminalOptions, Viewport};
use tokio::sync::mpsc;

use crate::client::{Client, EventStreamItem, RunnerEvent, ServiceState, StateSnapshot};
use crate::config::ParamKind;
use crate::output::{FormattedLogLine, LifecycleEmitter};
use app::{App, AppInit, ViewMode};
use events::AppEvent;
use log_store::{DEBUG_CAPACITY, DEFAULT_CAPACITY, LogStore};
use status_table::StatusTableKeyOutcome;

/// Errors that can escape the TUI event loop.
#[derive(Debug, thiserror::Error)]
pub enum TuiError {
    /// Raw mode toggle, cursor operations, ratatui backend IO.
    #[error("terminal io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Owns the terminal for as long as the TUI does.
///
/// Raw mode and the alternate screen go up together and come down together in
/// reverse order, from `Drop` — so a `?` anywhere in the loop still gives the
/// user their terminal back, and so does a panic unwinding through it.
struct TerminalGuard {
    /// The writer thread, so that giving the screen back always waits for the
    /// frames already queued for it. Held here rather than beside the terminal
    /// because `Drop` is the only path every exit shares — including a `?` in
    /// the loop and a panic unwinding through it.
    writer: writer::Writer,
}

impl TerminalGuard {
    fn enter(writer: writer::Writer) -> Result<Self, TuiError> {
        crossterm::terminal::enable_raw_mode()?;
        execute!(std::io::stdout(), EnterAlternateScreen, Print(MOUSE_ON))?;
        Ok(Self { writer })
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        // Before the restore, not after. These sequences go straight to the
        // fd, so one written while the writer still had a frame in hand would
        // interleave with it and leave the user looking at a screen assembled
        // from both — which reads as don having eaten their scrollback.
        self.writer.finish();
        let _ = execute!(std::io::stdout(), Print(MOUSE_OFF), LeaveAlternateScreen);
        let _ = crossterm::terminal::disable_raw_mode();
    }
}

/// The mouse reporting modes don actually uses.
///
/// Deliberately *not* crossterm's `EnableMouseCapture`, which also turns on
/// `?1003h` — "report every motion event". Nothing here consumes bare motion
/// (shift-hover did once, and even gated it made the terminal emit and the
/// input task parse a report for every cell the pointer crossed — paid on all
/// mouse movement, whether or not the feature was ever used). Every one of
/// those reports would be parsed and thrown away; what they cost is a flood
/// of input for as long as the pointer merely crosses the window.
///
/// `?1000h` reports press and release. `?1002h` adds motion *while a button is
/// held*, which is what drag-select and the divider drag need and all they
/// need. `?1006h` asks for SGR coordinates so columns past 223 are reportable
/// — crossterm also sets the older `?1015h` (RXVT) alongside it, which some
/// terminals answer in *both* encodings.
const MOUSE_ON: &str = "\x1b[?1000h\x1b[?1002h\x1b[?1006h";
/// The same modes, reset in reverse.
const MOUSE_OFF: &str = "\x1b[?1006l\x1b[?1002l\x1b[?1000l";

type TuiTerminal = Terminal<CrosstermBackend<writer::FrameSink>>;

/// Shortest gap between full repaints.
///
/// The loop marks state dirty and this decides when that becomes pixels. Under
/// a log flood the cost of the TUI is therefore bounded by the frame rate
/// rather than by the line rate — which is the property the old
/// `insert_before` model could not have, since every line was a write.
const FRAME_INTERVAL: std::time::Duration = std::time::Duration::from_millis(16);

/// Whether this TUI shares a process with the runner or attached over the
/// socket. The two differ in exactly two keys:
///
/// - **Ctrl+C**: in-process raises SIGINT alongside the shutdown request so
///   the runner's two-press force-kill escalation still works; a remote
///   client must not signal itself (there is no runner in this process to
///   catch it — just a TUI to kill mid-raw-mode) and settles for the
///   graceful request.
/// - **Ctrl+D**: remote detaches — exit the TUI, leave the stack running.
///   In-process there is nothing to detach *to* — the runner shares this
///   process — so it is ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TuiMode {
    /// The TUI shares a process with the runner (`don start` today).
    InProcess,
    /// The TUI attached to a running project over the socket (`don attach`).
    Remote,
}

#[derive(Clone)]
struct TuiControls {
    lifecycle_emitter: LifecycleEmitter,
    mode: TuiMode,
    /// The writer's queue, for the one thing that is a request to the terminal
    /// rather than a cell to paint: an OSC 52 clipboard write.
    terminal_out: writer::TerminalOut,
}

/// Flatten one merged-stream event into the batch the render loop consumes.
///
/// A drop is rendered as a lifecycle line so it lands in the log where the
/// missing lines would have been, rather than in a corner of the status bar.
/// It carries the id the stream resumed at, so its position in the store is
/// the truth about where the hole is.
fn push_merged_event(
    batch: &mut Vec<(crate::output::LogId, FormattedLogLine)>,
    event: crate::output::MergedEvent,
) {
    match event {
        crate::output::MergedEvent::Line(entry) => {
            batch.push((entry.id, (*entry.line).clone()));
        }
        crate::output::MergedEvent::Dropped { count, resumed_at } => batch.push((
            resumed_at,
            FormattedLogLine {
                name: crate::output::LIFECYCLE_EVENT_NAME.to_string(),
                is_lifecycle: true,
                is_verbose: false,
                // The gap notice is the TUI's own, so it has no prefix from
                // the sink to sit under.
                prefix: Vec::new(),
                bytes: format!(
                    "{count} log line(s) dropped — history did not reach back far enough"
                )
                .into_bytes(),
            },
        )),
    }
}

/// Run the interactive TUI until the runner shuts down or the user quits.
///
/// Ctrl+C raises SIGINT to our own process so the installed signal handler
/// drives graceful shutdown (identical behavior to pipe mode, including the
/// two-Ctrl+C force-kill escalation).
#[allow(clippy::too_many_arguments)]
pub async fn run_tui(
    mut log_rx: mpsc::UnboundedReceiver<crate::output::MergedEvent>,
    client: Client,
    mode: TuiMode,
    lifecycle_emitter: LifecycleEmitter,
    service_names: Vec<String>,
    task_names: Vec<String>,
    build_tool_names: Vec<String>,
    task_configs: std::collections::HashMap<String, crate::config::Task>,
    task_last_runs: std::collections::HashMap<String, crate::task_state::TaskRunInfo>,
    hidden_names: std::collections::HashSet<String>,
    auto_filter_on_failure_names: std::collections::HashSet<String>,
    cli_log_filter: Option<std::collections::HashSet<String>>,
) -> Result<(), TuiError> {
    // The writer thread starts here rather than beside the terminal because
    // `controls` carries the handle to it, and a clipboard request has to go
    // down the same queue as the frames to stay in order with them.
    //
    // One completion per frame the writer lands, counted against the frames
    // rendered so the loop can tell whether the terminal is keeping up.
    let (frame_done_tx, mut frame_done_rx) = tokio::sync::mpsc::unbounded_channel();
    let (sink, terminal_out, writer) = writer::spawn(frame_done_tx)?;
    let controls = TuiControls {
        lifecycle_emitter,
        mode,
        terminal_out,
    };
    let client = std::sync::Arc::new(client);

    // Follow the runner's event stream as a client. The first record is a
    // state snapshot (see `GET /events`), so the view starts consistent no
    // matter when the connection lands relative to startup. The task ends
    // when the server closes the stream (shutdown) or the TUI drops the
    // receiver; either way the loop's `None` arm takes over.
    let (events_tx, mut events_rx) = mpsc::unbounded_channel::<EventStreamItem>();
    {
        let client = client.clone();
        tokio::spawn(async move {
            let _ = client
                .events_follow_typed(|item| {
                    events_tx
                        .send(item)
                        .map_err(|_| crate::client::ClientError::Invalid("tui closed".into()))
                })
                .await;
        });
    }

    let mut app = App::new(AppInit {
        service_names,
        task_names,
        build_tool_names,
        task_configs,
        task_last_runs,
        hidden_names,
        auto_filter_on_failure_names,
        cli_log_filter,
    });
    // Separate budgets, deliberately unequal: the output is scrollback, the
    // diagnostics are for answering a question you have right now.
    let mut stores = LogStores {
        output: LogStore::with_capacity(DEFAULT_CAPACITY),
        debug: LogStore::with_capacity(DEBUG_CAPACITY),
    };

    let (input_tx, mut input_rx) = mpsc::channel::<AppEvent>(64);
    // Publish the sender so background tasks (completion replies) can
    // inject events back into the loop. Set once for the lifetime of the
    // process — across pause/resume cycles we keep the same sender so
    // pending replies can still land.
    let _ = INPUT_TX.set(input_tx.clone());
    // Tracks whether the input task's channel is still open. When the input
    // task exits (crossterm EventStream error), we gate its select arm off
    // so the select loop doesn't busy-spin on a perpetually-ready None.
    let mut input_open = true;

    // The terminal, owned for as long as the TUI runs — and so is the input
    // task. Attaching used to interrupt both; now the window draws on don's
    // own screen and its input comes off this same event stream.
    let _guard = TerminalGuard::enter(writer)?;
    let mut terminal = build_terminal(sink)?;
    let input_handle = tokio::spawn(input::run(input_tx.clone()));
    // The live attach session, when there is one. Owned here rather than on
    // `App` because it is tasks and a socket, not view state.
    let mut attach: Option<attach_session::Session> = None;

    // Drives the spinner and relative timestamps ("5s ago"), which move
    // without any event arriving.
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(100));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Nothing below draws. Arms mutate `app` and set this; one arm turns it
    // into pixels, no more often than `FRAME_INTERVAL`. That is what bounds
    // the TUI's cost by frame rate rather than by log rate, and what stops
    // each arm having to know what any other arm would have wanted redrawn.
    let mut dirty = true;
    // Frames rendered but not yet reported written. The frame arm will not
    // produce another while this is non-zero, so frames are paced by what the
    // terminal can take rather than by a timer that assumes writes are free.
    //
    // The saving is not in rendering less — that costs about one percent of a
    // core — it is that the state a frame is rendered from is whatever is true
    // when the writer comes free. Ten scroll steps arriving during one slow
    // write become one repaint of where the scroll ended up rather than ten,
    // and a frame costs what the pane costs however far it moved.
    let mut in_flight = 0usize;
    // One timer, reset after each frame — not a fresh `sleep_until` per loop
    // iteration. `select!` builds every branch's future each time round, so a
    // new sleep meant registering and cancelling a timer entry per iteration,
    // and under a log flood the loop goes round tens of thousands of times a
    // second.
    let frame = tokio::time::sleep_until(tokio::time::Instant::now());
    tokio::pin!(frame);

    // Cap on log events drained per select round. Large bursts still clear in
    // a few rounds, while runner events and input keep getting a turn — so
    // Ctrl+C stays responsive under a flood.
    const LOG_BATCH_LIMIT: usize = 512;

    loop {
        tokio::select! {
            maybe_event = log_rx.recv() => {
                match maybe_event {
                    Some(first) => {
                        let mut batch: Vec<(crate::output::LogId, FormattedLogLine)> =
                            Vec::with_capacity(LOG_BATCH_LIMIT);
                        push_merged_event(&mut batch, first);
                        while batch.len() < LOG_BATCH_LIMIT {
                            match log_rx.try_recv() {
                                Ok(event) => push_merged_event(&mut batch, event),
                                Err(_) => break,
                            }
                        }
                        for (id, line) in batch {
                            if is_shutdown_start_line(&line) && !app.shutdown_started {
                                app.begin_shutdown();
                            }
                            stores.route(id, line);
                        }
                        dirty = true;
                    }
                    None => break, // runner closed the log channel — shut down
                }
            }
            runner_result = events_rx.recv() => {
                match runner_result {
                    Some(EventStreamItem::Event(RunnerEvent::ShutdownStarted)) => {
                        if !app.shutdown_started {
                            app.begin_shutdown();
                        }
                    }
                    Some(EventStreamItem::Event(event)) => {
                        apply_runner_event(event, &mut app);
                    }
                    Some(EventStreamItem::Snapshot { processes, startup_complete }) => {
                        // The stream's opening record — the authoritative state
                        // at connect time. Later events are newer or equal, so
                        // applying them after this is safe.
                        app.resync_from(&StateSnapshot { processes, startup_complete });
                    }
                    Some(EventStreamItem::Lagged(_)) => {
                        // Transitions were dropped, so the incremental view is
                        // wrong about an unknown set of processes and would stay
                        // wrong. Refetch off-loop and inject the result as an
                        // input event; awaiting here would wedge rendering
                        // behind a slow server.
                        spawn_state_resync(&client);
                    }
                    None => {}
                }
                let lazy: std::collections::HashSet<String> = app
                    .services_state
                    .iter()
                    .filter(|(_, s)| matches!(s, ServiceState::Lazy))
                    .map(|(n, _)| n.clone())
                    .collect();
                app.filter.set_hidden_from_display(lazy);
                // A task that declared itself interactive may have just
                // started, or the one whose window is open may have just
                // finished. Both are decided in `app`, and settled here.
                let area: Rect = terminal.size()?.into();
                settle_attach(&mut app, &mut attach, &client, &controls, &input_tx, area).await;
                dirty = true;
            }
            maybe_event = input_rx.recv(), if input_open => {
                match maybe_event {
                    Some(event) => {
                        let area: Rect = terminal.size()?.into();
                        dispatch_event(
                            event,
                            &mut app,
                            &mut stores,
                            &mut attach,
                            &client,
                            &controls,
                            area,
                        )
                        .await?;
                        // Input arrives in bursts — a drag reports once per cell
                        // the pointer crosses — and handling one per `select!`
                        // round means a round trip each, so a burst spreads
                        // across frames and the UI lags behind the hand moving
                        // it. Draining what is already queued keeps a burst
                        // inside one frame.
                        loop {
                            if app.exit_requested || app.bridge_request.is_some() {
                                break;
                            }
                            match input_rx.try_recv() {
                                Ok(event) => {
                                    dispatch_event(
                                        event,
                                        &mut app,
                                        &mut stores,
                                        &mut attach,
                                        &client,
                                        &controls,
                                        area,
                                    )
                                    .await?;
                                }
                                Err(_) => break,
                            }
                        }
                        dirty = true;
                        if app.exit_requested {
                            break;
                        }
                        settle_attach(
                            &mut app, &mut attach, &client, &controls, &input_tx, area,
                        )
                        .await;
                    }
                    None => input_open = false,
                }
            }
            _ = ticker.tick() => {
                app.spinner_frame = app.spinner_frame.wrapping_add(1);
                // The copy badge answers something the user just did; once it
                // has been read it is only taking up the slot the update notice
                // wants. OSC 52 never replies, so time is the only thing that
                // can retire it.
                if app
                    .copy_notice
                    .as_ref()
                    .is_some_and(|(_, at)| at.elapsed() > COPY_NOTICE_TTL)
                {
                    app.copy_notice = None;
                }
                dirty = true;
            }
            Some(written) = frame_done_rx.recv() => {
                // A terminal that cannot be written to ends the TUI, exactly as
                // it did when the write came straight out of `draw`. Carrying
                // on would mean don running against a screen nobody can see.
                written?;
                in_flight = in_flight.saturating_sub(1);
            }
            () = &mut frame, if dirty && in_flight == 0 => {
                let showing = app.debug_view;
                draw(&mut terminal, &mut app, stores.active_mut(showing))?;
                dirty = false;
                in_flight += 1;
                frame
                    .as_mut()
                    .reset(tokio::time::Instant::now() + FRAME_INTERVAL);
            }
        }
    }

    // One last frame so the final state — "shutdown complete" — is on screen
    // before the guard puts the user's terminal back.
    let showing = app.debug_view;
    let _ = draw(&mut terminal, &mut app, stores.active_mut(showing));
    input_handle.abort();
    let _ = input_handle.await;
    Ok(())
}

/// Build the full-screen terminal.
///
/// `Viewport::Fullscreen` never probes the cursor position, so there is no DSR
/// response to race the input task's stdin reader for — the whole reason the
/// old inline viewport needed a backend wrapper around
/// `get_cursor_position`.
fn build_terminal(sink: writer::FrameSink) -> Result<TuiTerminal, TuiError> {
    let backend = CrosstermBackend::new(sink);
    let terminal = Terminal::with_options(
        backend,
        TerminalOptions {
            viewport: Viewport::Fullscreen,
        },
    )?;
    Ok(terminal)
}

/// The two records the log pane can show.
///
/// Kept apart rather than filtered out of one: they were sharing a single
/// retention budget, so a chatty watch could push the output a reader actually
/// wanted past the end of the buffer without that being visible anywhere. And
/// "which lines count" was part of the row index's key, so changing your mind
/// threw the index away and rebuilt it over the whole store.
struct LogStores {
    /// What the processes wrote.
    output: LogStore,
    /// What don said about them — see `LifecycleEmitter::debug_event`.
    debug: LogStore,
}

impl LogStores {
    /// File a line under the record it belongs to. The tag decides, once.
    fn route(&mut self, id: crate::output::LogId, line: FormattedLogLine) {
        if line.is_verbose {
            self.debug.push(id, line);
        } else {
            self.output.push(id, line);
        }
    }

    fn active_mut(&mut self, debug_view: bool) -> &mut LogStore {
        if debug_view {
            &mut self.debug
        } else {
            &mut self.output
        }
    }
}

/// Throw away what the renderer believes is on screen/// Throw away what the renderer believes is on screen, so the next draw paints
/// every cell.
///
/// Deliberately not `Terminal::clear`. As of ratatui-core 0.1.2 that reads the
/// cursor position first, in order to put it back afterwards — and reading it
/// means writing `ESC[6n` and waiting for the terminal to answer on stdin,
/// which this TUI's input task is already reading. The reply goes to the input
/// task, the read times out after two seconds, and the whole TUI exits with
/// "the cursor position could not be read within a normal duration". A
/// dependency resolving one patch version forward was enough to turn opening a
/// pane into a crash.
///
/// `resize` to the area it already has does what is actually wanted — clear the
/// viewport, reset both buffers so the next diff has nothing to match against —
/// and touches the cursor only on the inline viewport, which this is not.
fn force_full_repaint(terminal: &mut TuiTerminal, area: Rect) -> Result<(), TuiError> {
    terminal.resize(area)?;
    Ok(())
}

/// Paint the whole screen from `app` and `store`.
///
/// The store is reflowed to the log pane's width first: row counts have to be
/// current before the view can place its scroll anchor, and a resize is the
/// only time that costs anything.
fn draw(terminal: &mut TuiTerminal, app: &mut App, store: &mut LogStore) -> Result<(), TuiError> {
    let area: Rect = terminal.size()?.into();
    // No panel means the log has the keys — enforced here rather than at
    // every site that closes a panel, of which there are many and will be
    // more. A stale Panel focus with nothing focusable is unrepresentable on
    // screen, so it should be unrepresentable in state.
    if !app.panel_open() {
        app.focus = panes::Focus::Logs;
    }
    let panel_open = app.panel_open();
    // A layout change moves what every cell means, and a diffing renderer only
    // rewrites the cells it believes changed — so whatever the old layout drew
    // outside the new one's reach stays on screen. Clearing costs a full
    // repaint, which is why it is done on the layout change rather than every
    // frame: opening, moving or resizing a pane is a thing people do
    // occasionally, not sixty times a second.
    if app.painted_layout != Some((app.panel, panel_open)) || app.repaint_requested {
        force_full_repaint(terminal, area)?;
        app.painted_layout = Some((app.panel, panel_open));
        app.repaint_requested = false;
    }
    store.reflow(render::log_pane_width(area, app.panel, panel_open));
    // Marks on lines that have aged out of history describe nothing.
    app.prune_blank_marks(store.oldest_id());
    // Hold history where the reader is looking, so a busy stack cannot evict
    // their place out from under them between frames.
    store.set_pin(app.log_scroll.anchor());
    terminal.draw(|frame| render::draw(frame, app, store))?;
    Ok(())
}

fn is_shutdown_start_line(line: &FormattedLogLine) -> bool {
    line.name == crate::output::LIFECYCLE_EVENT_NAME
        && String::from_utf8_lossy(&line.bytes).contains("shutting down gracefully")
}

/// Apply one [`RunnerEvent`] to the cached state on [`App`].
fn apply_runner_event(event: RunnerEvent, app: &mut App) -> bool {
    match event {
        RunnerEvent::ServiceStateChanged {
            name,
            state,
            pid,
            failed_dependencies,
        } => app.apply_service_runtime(name, state, pid, failed_dependencies),
        RunnerEvent::TaskStateChanged {
            name,
            state,
            last_run,
            failed_dependencies,
        } => app.apply_task_state(name, state, last_run, failed_dependencies),
        RunnerEvent::UpdateCheck {
            current_version,
            latest_version,
        } => {
            app.set_update_check(current_version, latest_version);
            false
        }
        // The TUI already shows per-item states, so it learns nothing extra
        // from the sweep finishing — that signal is for API clients deciding
        // whether it's meaningful to ask the runner to run something.
        RunnerEvent::StartupSettled
        | RunnerEvent::ShutdownStarted
        | RunnerEvent::ShutdownComplete => false,
    }
}

/// Dispatch an input or resize event.
fn handle_app_event(
    event: AppEvent,
    app: &mut App,
    store: &mut LogStore,
    client: &std::sync::Arc<Client>,
    controls: &TuiControls,
) -> Result<(), TuiError> {
    match event {
        AppEvent::Resize => {
            // Nothing to do but redraw, which the caller has already asked for
            // by marking the frame dirty. Geometry is read fresh every frame
            // and the store reflows itself when the width moves; the scroll
            // anchor is a line id, so it means the same thing at any size.
        }
        AppEvent::Key(key) => handle_key(key, app, store, client, controls)?,
        AppEvent::Mouse(mouse, at) => handle_mouse(mouse, at, app, store),
        // Handled by the loop, which owns the live session; by the time an
        // event reaches here the loop has already dealt with it.
        AppEvent::Attach(_) => {}
        AppEvent::CompletionsReady {
            param,
            request_id,
            result,
        } => {
            if let Some(form) = app.form.as_mut() {
                form.apply_completions(&param, request_id, result);
            }
        }
        AppEvent::StateResync {
            processes,
            startup_complete,
        } => {
            app.resync_from(&StateSnapshot {
                processes,
                startup_complete,
            });
        }
    }
    Ok(())
}

fn handle_key(
    key: KeyEvent,
    app: &mut App,
    store: &mut LogStore,
    client: &std::sync::Arc<Client>,
    controls: &TuiControls,
) -> Result<(), TuiError> {
    // Ctrl+C: belt-and-suspenders shutdown. We both send a `Shutdown` command
    // directly down the runner channel AND raise SIGINT. The direct command
    // works even if the signal handler task has died or isn't being polled
    // (e.g., the runner is stuck pre-loop), and SIGINT preserves the
    // two-press force-kill escalation via the runner's signal counter.
    if key.modifiers.contains(KeyModifiers::CONTROL) {
        match key.code {
            KeyCode::Char('c') => {
                // Second press escalates, matching the runner's own
                // "Ctrl+C again to force" contract — raw mode means the key
                // never becomes a SIGINT, so the escalation goes over the
                // API instead of through the signal counter.
                let force = app.shutdown_started;
                {
                    let client = client.clone();
                    tokio::spawn(async move {
                        let _ = if force {
                            client.shutdown_force().await
                        } else {
                            client.shutdown().await
                        };
                    });
                }
                if controls.mode == TuiMode::InProcess {
                    let _ = nix::sys::signal::kill(
                        nix::unistd::Pid::this(),
                        nix::sys::signal::Signal::SIGINT,
                    );
                }
            }
            // Detach: leave the stack running, exit this client. Only
            // meaningful for a remote TUI — see [`TuiMode`].
            KeyCode::Char('d') if controls.mode == TuiMode::Remote => {
                app.exit_requested = true;
            }
            // Swap which record the pane shows: what the processes wrote, or
            // what don said about them. Deliberately unadvertised — it answers
            // "why did that not rebuild", which is a question you go looking
            // for, not one the status bar should spend a slot on.
            //
            // Purely local: both records are always received and stored, and
            // each keeps its own scroll position, so coming back lands where
            // you left.
            KeyCode::Char('v') | KeyCode::Char('V') => app.swap_log_view(),
            // Resize the split from the keyboard: the arrow moves the border
            // in its own direction, whichever pane has focus. For a
            // right-docked panel that is also "grow the pane you are in" from
            // both sides — Ctrl+Right grows a focused log and shrinks a
            // focused panel, which are the same motion.
            KeyCode::Left if app.panel_open() && app.panel.side == panes::PaneSide::Right => {
                nudge_panel(app, RESIZE_STEP_COLUMNS);
            }
            KeyCode::Right if app.panel_open() && app.panel.side == panes::PaneSide::Right => {
                nudge_panel(app, -RESIZE_STEP_COLUMNS);
            }
            KeyCode::Up if app.panel_open() && app.panel.side == panes::PaneSide::Bottom => {
                nudge_panel(app, RESIZE_STEP_ROWS);
            }
            KeyCode::Down if app.panel_open() && app.panel.side == panes::PaneSide::Bottom => {
                nudge_panel(app, -RESIZE_STEP_ROWS);
            }
            _ => {}
        }
        return Ok(());
    }

    if app.shutdown_started {
        return Ok(());
    }

    // The panel-management keys work from either side of the split, so they
    // are handled before the per-view routing — with one carve-out: while a
    // search box is being typed into (the filter's, or a table's `/` query),
    // every character belongs to the query, and while the filter's query is
    // up Tab already means "back to the list" there.
    let typing = match app.view_mode {
        ViewMode::Filter => app.filter.focus() == filter::FilterFocus::Query,
        ViewMode::Services => app.services_table.filtering,
        ViewMode::Tasks => app.tasks_table.filtering,
        _ => false,
    };
    if app.panel_open() && !typing {
        match key.code {
            // Tab moves the keys between the log and the panel.
            KeyCode::Tab => {
                app.focus = match app.focus {
                    panes::Focus::Logs => panes::Focus::Panel,
                    panes::Focus::Panel => panes::Focus::Logs,
                };
                return Ok(());
            }
            // Each panel's own key is a toggle, and another panel's key
            // switches — from either focus. `l` is only the filter's toggle
            // when the filter is up: in the tables it opens the log popup for
            // the highlighted row, which the table handlers own.
            KeyCode::Char('s') if app.view_mode == ViewMode::Services => {
                return_to_logs(app);
                return Ok(());
            }
            KeyCode::Char('s') => {
                open_services_panel(app);
                return Ok(());
            }
            KeyCode::Char('t') if app.view_mode == ViewMode::Tasks => {
                return_to_logs(app);
                return Ok(());
            }
            KeyCode::Char('t') => {
                open_tasks_panel(app);
                return Ok(());
            }
            KeyCode::Char('f') if app.view_mode == ViewMode::Filter => {
                return_to_logs(app);
                return Ok(());
            }
            // The third panel's key, and a toggle like the other two. `f` for
            // filter, which is what it is — it was `l`, for "logs", and that
            // collided with the `l` the tables use for one row's log, so
            // reaching for the filter while reading a table opened a popup.
            KeyCode::Char('f') => {
                open_filter_panel(app);
                return Ok(());
            }
            _ => {}
        }
    }

    // A panel does not take the log away, and it does not take the keyboard
    // away either: focus decides. The log side keeps its whole vocabulary —
    // scrolling, selection, verbose, even opening a different panel.
    if app.panel_open() && app.focus == panes::Focus::Logs {
        return handle_normal_key(key, app, store, &controls.terminal_out);
    }

    match app.view_mode {
        ViewMode::Normal => handle_normal_key(key, app, store, &controls.terminal_out)?,
        ViewMode::Filter => handle_filter_key(key, app, store)?,
        ViewMode::Tasks => handle_tasks_key(key, app, store, client)?,
        ViewMode::Services => {
            handle_services_key(key, app, client, controls)?;
        }
        ViewMode::Failures => handle_failure_summary_key(key, app, store)?,
        ViewMode::Form => handle_form_key(key, app, store, client)?,
    }
    Ok(())
}

fn handle_normal_key(
    key: KeyEvent,
    app: &mut App,
    store: &mut LogStore,
    out: &writer::TerminalOut,
) -> Result<(), TuiError> {
    // The half-finished `gg` chord: taken here so any key other than the
    // second `g` clears it just by arriving.
    let awaiting_second_g = std::mem::take(&mut app.pending_g);
    match key.code {
        // Held above the tail, Enter is the way back down — the gesture
        // everyone tries first. Above means *actually* above: a view merely
        // pinned at the tail (a selection does that) looks identical to
        // following, and routing its Enter here would swallow the press —
        // resume changes nothing visible, and the blank never happens.
        KeyCode::Enter if app.log_scroll != logs::Scroll::Follow && !at_tail(app) => {
            resume_following(app);
        }
        KeyCode::Enter => {
            // Mark the last line *on screen* as followed by a blank row.
            //
            // Not `store.latest_id()`: the store holds every process, and on a
            // stack with hidden services the newest stored line is usually one
            // the filter does not admit — a mark on an invisible line renders
            // nothing, which reads as Enter being dead. The bottom visible row
            // is, by definition, admitted.
            //
            // A mark rather than a pushed blank line, because a pushed blank
            // needs an id and the only one going spare is the id the next real
            // line will arrive with — see `App::blank_after`.
            if let Some(source) = app.log_row_sources.last() {
                app.mark_blank_after(source.id);
            }
            // A pin left over from a settled selection would hold the view
            // still while new output runs past it. Enter at the tail means
            // "give me space for what comes next", so following resumes.
            resume_following(app);
        }
        // The conventional "something has scribbled on my screen" key. An
        // escape hatch, not a fix: anything that leaves the terminal and this
        // renderer disagreeing is a bug, but the user should not have to
        // restart the stack to get a clean screen back.
        KeyCode::Char('l') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.repaint_requested = true;
        }
        // The panel views. Opening one focuses it — the point of pressing the
        // key is to use the thing. The toggles and switches for an
        // already-open panel live in `handle_key`, before routing, so they
        // work from either side of the split.
        KeyCode::Char('f') => open_filter_panel(app),
        KeyCode::Char('t') => open_tasks_panel(app),
        KeyCode::Char('s') => open_services_panel(app),
        KeyCode::Char('i') if app.has_failure_summary() => {
            app.open_failure_summary();
        }
        KeyCode::Char('R') => {
            app.filter.reset_to_defaults();
        }
        // Scrolling the log. The pane's own history is the only history now,
        // so these are load-bearing rather than a convenience over the
        // terminal's scrollback.
        KeyCode::Up | KeyCode::Char('k') => scroll_log(app, |p| p.rows -= 1),
        KeyCode::Down | KeyCode::Char('j') => scroll_log(app, |p| p.rows += 1),
        KeyCode::PageUp => scroll_log(app, |p| p.pages -= 1),
        KeyCode::PageDown => scroll_log(app, |p| p.pages += 1),
        // Vim's verticals. `gg` is a chord: the first press arms it, the
        // second lands it, anything in between disarms it by arriving.
        // Ctrl+D/Ctrl+U half-pages are deliberately absent — Ctrl+D is
        // detach on a remote TUI, and a movement key that detaches you on
        // one invocation and scrolls on the other is worse than no key.
        KeyCode::Char('g') if awaiting_second_g => scroll_log(app, |p| p.to_top = true),
        KeyCode::Char('g') => app.pending_g = true,
        KeyCode::Home => scroll_log(app, |p| p.to_top = true),
        KeyCode::End | KeyCode::Char('G') => {
            app.log_selection.clear();
            app.follow_paused_for_selection = false;
            app.log_scroll = logs::Scroll::Follow;
        }
        // Ctrl+C is shutdown and cannot double as copy, so the keyboard route
        // to the clipboard is `y` — vi's yank, over the current selection.
        KeyCode::Char('y') => copy_selection(app, store, out),
        // With a panel open but the log focused, Esc means "done with the
        // panel" — it is the dismiss key every panel view already answers to,
        // and it should not need a focus switch first.
        KeyCode::Esc if app.panel_open() => {
            app.log_selection.clear();
            return_to_logs(app);
        }
        KeyCode::Esc => clear_selection(app),
        KeyCode::Char('P') if app.panel_open() => {
            app.panel.side = app.panel.side.toggled();
            // Extents mean different things on the two axes; start from the
            // default for the new one rather than carrying a column count over
            // into rows.
            app.panel.extent = match app.panel.side {
                panes::PaneSide::Right => 48,
                panes::PaneSide::Bottom => 12,
            };
        }
        _ => {}
    }
    Ok(())
}

/// One event, routed by what is in front of the reader.
///
/// The attach window is the only view that changes what a key *means* rather
/// than what it does, so it is the only one resolved out here: while it is
/// open, keys belong to the process and everything else — mouse, resize, the
/// log still flowing behind — carries on as usual.
async fn dispatch_event(
    event: AppEvent,
    app: &mut App,
    stores: &mut LogStores,
    attach: &mut Option<attach_session::Session>,
    client: &std::sync::Arc<Client>,
    controls: &TuiControls,
    area: Rect,
) -> Result<(), TuiError> {
    match event {
        AppEvent::Attach(attach_event) => {
            handle_attach_event(attach_event, app, attach).await;
        }
        AppEvent::Key(key) if attach.is_some() && !attach_ended(app) => {
            match attach_window::route(key) {
                attach_window::AttachInput::Forward(bytes) => {
                    if let Some(session) = attach.as_ref() {
                        session.send(bytes);
                    }
                }
                attach_window::AttachInput::Detach => end_attach(app, attach),
                attach_window::AttachInput::Nothing => {}
            }
        }
        // The process is gone but its last screen is still up. There is
        // nothing to type at, so every key dismisses it — anyone reaching for
        // one wants the window out of the way, and making them guess which is
        // just a smaller version of the ceremony this replaced.
        AppEvent::Key(_) if app.attach.is_some() => {
            end_attach(app, attach);
        }
        // A click inside the window would select text in the log underneath
        // it, where nobody can see it. Outside, the log is visible and the
        // mouse still belongs to it.
        AppEvent::Mouse(mouse, _)
            if app
                .attach
                .as_ref()
                .is_some_and(|view| view.window.contains(mouse.column, mouse.row)) => {}
        // The window is placed in screen coordinates, so a smaller terminal
        // can leave it hanging off the edge. Refit before anything draws.
        AppEvent::Resize if app.attach.is_some() => {
            if let Some(view) = app.attach.as_mut() {
                let refitted = view.window.fitted(area);
                if refitted != view.window {
                    view.window = refitted;
                    // No-op once the session is gone: an ended window still
                    // has to fit the screen, but there is nobody left to tell.
                    resize_process_grid(view.window, attach.as_ref(), client);
                }
            }
        }
        other => {
            let showing = app.debug_view;
            handle_app_event(other, app, stores.active_mut(showing), client, controls)?;
        }
    }
    Ok(())
}

/// Apply one attach event: redraw the window, or end the session.
///
/// Lives here rather than in `handle_app_event` because it needs the live
/// session, which the loop owns — the connection outlives no frame and
/// belongs to nothing in `App`.
async fn handle_attach_event(
    event: events::AttachEvent,
    app: &mut App,
    attach: &mut Option<attach_session::Session>,
) {
    let Some(session) = attach.as_ref() else {
        return;
    };
    match event {
        events::AttachEvent::Output => {
            // Re-read the whole grid rather than tracking damage: it is one
            // channel round trip for a screenful, and the alternative is
            // teaching don's side which cells ghostty changed.
            let grid = session.emulator.grid(&session.name).await;
            if let Some(view) = app.attach.as_mut() {
                view.grid = grid;
            }
        }
        // The connection is over, but the window is not. Keeping the last
        // screen is the whole point of attaching to something that finishes:
        // the output worth reading is the output it wrote last.
        events::AttachEvent::Ended => {
            if let Some(session) = attach.take() {
                session.shutdown();
            }
            if let Some(view) = app.attach.as_mut() {
                view.ended = true;
            }
        }
    }
}

/// Tell the process its terminal changed size, because for it the window *is*
/// the terminal.
fn resize_process_grid(
    window: attach_window::WindowRect,
    session: Option<&attach_session::Session>,
    client: &std::sync::Arc<Client>,
) {
    let Some(session) = session else {
        return;
    };
    let (cols, rows) = window.grid_size();
    attach_session::notify_resize(
        std::sync::Arc::new(client.socket_path().to_path_buf()),
        session.name.clone(),
        session.session_id,
        cols,
        rows,
        session.emulator.clone(),
    );
}

/// Open or close the attach window, if anything has asked for it.
///
/// The decision is `app`'s — a key the reader pressed, or an interactive task
/// starting or finishing — and the session is the loop's, so the two meet here.
/// Called from both arms that can move that state, because an interactive task
/// starting is a runner event and pressing `a` is an input one.
async fn settle_attach(
    app: &mut App,
    attach: &mut Option<attach_session::Session>,
    client: &std::sync::Arc<Client>,
    controls: &TuiControls,
    input_tx: &mpsc::Sender<AppEvent>,
    area: Rect,
) {
    if std::mem::take(&mut app.attach_dismiss_requested) {
        end_attach(app, attach);
    }
    let Some(name) = app.bridge_request.take() else {
        return;
    };
    // The window draws on don's own screen and takes its input off the stream
    // this loop is already reading, so nothing is handed over: no terminal
    // teardown, no second reader of stdin.
    let window = attach_window::WindowRect::centred_in(area);
    let (cols, rows) = window.grid_size();
    match attach_session::start(client.socket_path(), &name, cols, rows, input_tx.clone()).await {
        Ok(session) => {
            app.attach = Some(app::AttachView {
                name: name.clone(),
                window,
                grid: None,
                ended: false,
            });
            *attach = Some(session);
        }
        Err(e) => {
            app.attach_opened_automatically = false;
            controls
                .lifecycle_emitter
                .lifecycle_event(&format!("attach '{name}' failed: {e}"));
        }
    }
}

/// Whether the window on screen is a record rather than a live terminal.
fn attach_ended(app: &App) -> bool {
    app.attach.as_ref().is_some_and(|view| view.ended)
}

/// Close the window. The process keeps running; don just stops watching it.
fn end_attach(app: &mut App, attach: &mut Option<attach_session::Session>) {
    if let Some(session) = attach.take() {
        session.shutdown();
    }
    app.attach = None;
    app.attach_opened_automatically = false;
}

/// Leave whatever view is up: back to the plain log, keys back to the log.
///
/// One function for panels and full-screen views alike, because dismissing
/// either means the same thing — there is nothing else to go back to.
fn return_to_logs(app: &mut App) {
    app.set_view_mode(ViewMode::Normal);
    app.focus = panes::Focus::Logs;
}

/// Columns one Ctrl+arrow press moves the split when docked right, and rows
/// when docked bottom. Big enough that resizing does not feel like sanding,
/// small enough to stop where you meant.
const RESIZE_STEP_COLUMNS: i16 = 4;
const RESIZE_STEP_ROWS: i16 = 2;

/// Grow the panel by `delta` (shrink for negative).
///
/// The stored extent is safe to build on because the renderer writes the
/// granted size back into it every frame — stored and on-screen cannot
/// diverge. Nudging the *drawn* rectangle instead was the first version, and
/// it collapsed a burst of presses into one: the rect only moves when a frame
/// is drawn, so every press in the burst computed the same target.
fn nudge_panel(app: &mut App, delta: i16) {
    app.panel.extent = app.panel.extent.saturating_add_signed(delta).max(1);
    app.panel_extent_customized = true;
}

fn open_services_panel(app: &mut App) {
    app.services_table.reset();
    app.freeze_services_order();
    app.set_view_mode(ViewMode::Services);
    app.focus = panes::Focus::Panel;
    fit_panel_to_terminal(app);
}

fn open_tasks_panel(app: &mut App) {
    app.tasks_table.reset();
    app.freeze_tasks_order();
    app.set_view_mode(ViewMode::Tasks);
    app.focus = panes::Focus::Panel;
    fit_panel_to_terminal(app);
}

fn open_filter_panel(app: &mut App) {
    app.filter.enter_edit();
    app.set_view_mode(ViewMode::Filter);
    app.focus = panes::Focus::Panel;
    fit_panel_to_terminal(app);
}

/// Give the opening panel a width suited to the terminal, unless the reader
/// has sized it themselves — their number is their number.
///
/// Two fifths of the width, floored at the old fixed 48 and capped at twice
/// it: a wide terminal has room for the tables' wider columns (the pid, a
/// task's last run), and pinning the panel at 48 there wasted the width on
/// log lines that rarely need 200 columns. The bar spans the whole terminal,
/// which is how the width is known here without a frame in hand.
fn fit_panel_to_terminal(app: &mut App) {
    if app.panel_extent_customized {
        return;
    }
    let width = app.panes.bar.width;
    if app.panel.side == panes::PaneSide::Right && width > 0 {
        app.panel.extent = (width.saturating_mul(2) / 5).clamp(48, 96);
    }
}

/// Whether a screen position is inside the open side panel.
fn over_panel(app: &App, column: u16, row: u16) -> bool {
    app.panel_open() && app.panes.hit(column, row) == Some(panes::Focus::Panel)
}

/// Move the panel's highlight by one step — the wheel's version of j/k.
fn panel_step(app: &mut App, delta: isize) {
    match app.view_mode {
        ViewMode::Services => {
            let total = app.service_items().len();
            step_table(&mut app.services_table, total, delta);
        }
        ViewMode::Tasks => {
            let total = app.task_items().len();
            step_table(&mut app.tasks_table, total, delta);
        }
        ViewMode::Filter => {
            if delta < 0 {
                app.filter.highlight_prev();
            } else {
                app.filter.highlight_next();
            }
        }
        _ => {}
    }
}

fn step_table(table: &mut status_table::StatusTableState, total: usize, delta: isize) {
    if total == 0 {
        return;
    }
    let max = total - 1;
    table.highlight = table
        .highlight
        .min(max)
        .saturating_add_signed(delta)
        .min(max);
}

/// Rows a single wheel tick moves the log.
///
/// Three is the near-universal terminal default; matching it is what makes the
/// pane feel like the scrollback it replaced rather than like a widget.
const WHEEL_ROWS: isize = 3;

/// Apply a mouse event.
///
/// Wheel scrolling works in every mode: a full-screen table on top does not
/// mean the user has stopped caring where the log is, and moving it costs
/// nothing while it is hidden.
fn handle_mouse(mouse: MouseEvent, at: std::time::Instant, app: &mut App, store: &LogStore) {
    match mouse.kind {
        // The wheel works on whatever is under the pointer — scrolling the log
        // that is hidden *behind* the panel you are pointing at is the kind of
        // spooky action nobody wants.
        MouseEventKind::ScrollUp if over_panel(app, mouse.column, mouse.row) => {
            panel_step(app, -1);
        }
        MouseEventKind::ScrollDown if over_panel(app, mouse.column, mouse.row) => {
            panel_step(app, 1);
        }
        MouseEventKind::ScrollUp => scroll_log(app, |p| p.rows -= WHEEL_ROWS),
        MouseEventKind::ScrollDown => scroll_log(app, |p| p.rows += WHEEL_ROWS),
        // A click in the log pane takes focus back from any overlay, which is
        // the gesture people reach for before they remember `esc`.
        MouseEventKind::Down(MouseButton::Left) => {
            // The divider is checked first: it is one cell wide and sits
            // between two panes, so "did they mean to drag it" has to be
            // answered before "which pane did they click".
            if app.panes.on_divider(mouse.column, mouse.row) {
                app.dragging_divider = true;
                return;
            }
            let Some(focus) = app.panes.hit(mouse.column, mouse.row) else {
                return;
            };
            app.focus = focus;
            // Selection needs the click to be on the log, and the log to be on
            // screen. A side panel leaves it on screen — the guard used to say
            // `view_mode != Normal` from the days when every other mode took
            // the whole frame, which made selecting dead whenever a panel was
            // open. Only the true full-screen overlays exclude it now.
            if focus != panes::Focus::Logs
                || matches!(app.view_mode, ViewMode::Failures | ViewMode::Form)
            {
                return;
            }
            match click_count(app, mouse.column, mouse.row, at) {
                // A drag is about to start, or a plain click clearing what was
                // selected before.
                1 => {
                    clear_selection(app);
                    if let Some(at) = point_at(app, mouse.column, mouse.row) {
                        app.log_selection.begin(at);
                    }
                }
                2 => select_word(app, store, mouse.column, mouse.row),
                // Triple-click means "this message" — all of it, however many
                // rows it wrapped across.
                _ => select_message(app, mouse.row),
            }
        }
        MouseEventKind::Drag(MouseButton::Left) => {
            if app.dragging_divider {
                let area = ratatui::layout::Rect::new(
                    0,
                    0,
                    app.panes.logs.width + app.panes.status.map_or(0, |s| s.width + 1),
                    app.panes.logs.height + app.panes.status.map_or(0, |s| s.height + 1),
                );
                app.panel.extent =
                    panes::extent_from_drag(area, app.panel.side, mouse.column, mouse.row);
                app.panel_extent_customized = true;
                return;
            }
            // The rows under a selection have to stop moving, or output
            // arriving mid-drag pulls the text out from under the pointer.
            pause_following_for_selection(app);
            if let Some(at) = point_at(app, mouse.column, mouse.row) {
                app.log_selection.extend(at);
            }
        }
        MouseEventKind::Up(MouseButton::Left) => {
            app.dragging_divider = false;
            app.log_selection.finish();
        }
        _ => {}
    }
}

/// How long two clicks may be apart and still count as a double click.
///
/// The usual desktop default. Long enough not to demand a fast hand, short
/// enough that two deliberate clicks on the same word are not mistaken for one.
const MULTI_CLICK_WINDOW: std::time::Duration = std::time::Duration::from_millis(400);

/// How long the copy badge stays up. Long enough to read, short enough that it
/// is clearly about the thing you just pressed.
const COPY_NOTICE_TTL: std::time::Duration = std::time::Duration::from_secs(4);

/// How many clicks have now landed in the same place in a row: 1, 2 or 3.
///
/// Terminals report a double click as two ordinary presses; only the gap and
/// the position tell them apart, so the counting has to happen here.
fn click_count(app: &mut App, column: u16, row: u16, at: std::time::Instant) -> u8 {
    let count = match app.last_click {
        Some((last_col, last_row, last_at, count))
            if last_row == row
                && last_col.abs_diff(column) <= 1
                // Arrival to arrival. Measuring from when *this* loop reached
                // the two clicks folds in however long it spent elsewhere —
                // and on a slow link that is a full frame's write, which is
                // enough to push a real double click outside the window.
                && at.duration_since(last_at) <= MULTI_CLICK_WINDOW =>
        {
            // Past a triple, start over rather than inventing a quadruple
            // click nothing has a meaning for.
            if count >= 3 { 1 } else { count + 1 }
        }
        _ => 1,
    };
    app.last_click = Some((column, row, at, count));
    count
}

/// Select whatever `span` picks out of the clicked row.
///
/// Resolved against the rows the last frame drew, so a double click lands on
/// the word actually under the pointer — after wrapping, filtering and scroll,
/// Where in the log a screen cell points.
///
/// `None` outside the pane, or on a row the last frame did not draw — the row
/// sources are that frame's answer, and nothing else knows how the text was
/// laid out.
fn point_at(app: &App, column: u16, row: u16) -> Option<selection::Point> {
    let (origin_x, origin_y) = app.log_pane_origin;
    let index = usize::from(row.checked_sub(origin_y)?);
    let source = *app.log_row_sources.get(index)?;
    Some(selection::Point::at(
        source,
        usize::from(column.saturating_sub(origin_x)),
    ))
}

/// Double-click: the word under the pointer.
///
/// Found in the message rather than in the rendered row, so a word the pane
/// wrapped across two rows still comes out whole.
fn select_word(app: &mut App, store: &LogStore, column: u16, row: u16) {
    let Some(at) = point_at(app, column, row) else {
        return;
    };
    let Some(message) = store.get(at.id).map(|entry| entry.message_text()) else {
        return;
    };
    let Some((start, end)) = selection::word_at(&message, at.offset) else {
        clear_selection(app);
        return;
    };
    pause_following_for_selection(app);
    app.log_selection.begin(selection::Point {
        id: at.id,
        offset: start,
    });
    app.log_selection.extend(selection::Point {
        id: at.id,
        offset: end,
    });
    app.log_selection.finish();
}

/// Triple-click: the whole message, however many rows it wrapped across.
///
/// The end is left open rather than measured — `selected_text` clamps to the
/// message it finds, so this stays right if the line is a progress frame that
/// repaints itself into something shorter.
fn select_message(app: &mut App, row: u16) {
    let Some(at) = point_at(app, 0, row) else {
        return;
    };
    pause_following_for_selection(app);
    app.log_selection.begin(selection::Point {
        id: at.id,
        offset: 0,
    });
    app.log_selection.extend(selection::Point {
        id: at.id,
        offset: usize::MAX,
    });
    app.log_selection.finish();
}

/// Hold the view still while a selection stands.
///
/// A selection is screen coordinates over the rows a frame drew. Following
/// means those rows move, so a selection made while following would be
/// pointing at different text a frame later — which is why copy used to have
/// to happen on mouse-release. Freezing instead is what lets the copy be
/// explicit: the selection stays exactly what the user dragged across until
/// they act on it.
fn pause_following_for_selection(app: &mut App) {
    if app.log_scroll != logs::Scroll::Follow {
        return;
    }
    // Asked for, not computed: "hold where you are" needs to know where that is,
    // which only the renderer does.
    app.pending_scroll.pin = true;
    app.follow_paused_for_selection = true;
}

/// Drop the selection, and go back to the live tail if it was what stopped us
/// following. A reader who had scrolled up on purpose stays where they were.
fn clear_selection(app: &mut App) {
    app.log_selection.clear();
    if app.follow_paused_for_selection {
        resume_following(app);
    }
}

fn resume_following(app: &mut App) {
    app.follow_paused_for_selection = false;
    app.log_scroll = logs::Scroll::Follow;
}

/// Whether the view's bottom edge is at the newest admitted content —
/// following, or pinned in the place following would be.
///
/// From the geometry the last frame measured, which is the same answer the
/// user is looking at.
fn at_tail(app: &App) -> bool {
    app.log_total_rows <= app.log_rows_above + usize::from(app.log_pane_height)
}

/// Put the current selection on the clipboard, and say so.
fn copy_selection(app: &mut App, store: &LogStore, out: &writer::TerminalOut) {
    let Some(text) = selection::selected_text(&app.log_selection, &app.view_index, store) else {
        return;
    };
    let lines = text.lines().count();
    let now = std::time::Instant::now();
    app.copy_notice = Some(match selection::copy_to_clipboard(out, &text) {
        // OSC 52 is a request with no reply: a terminal that has it turned off
        // discards it silently. Reporting what was sent is the only honest
        // thing available — "copied" here means "asked the terminal to".
        Ok(()) => (format!("copied {lines} line(s)"), now),
        Err(e) => (format!("copy failed: {e}"), now),
    });
}

/// Record a scroll the reader asked for. Resolved when the pane is next drawn.
///
/// Geometry deliberately absent: how far this can go depends on how much
/// admitted content there is and how tall the pane came out, and both are known
/// only while drawing. Deciding here meant deciding from what the last frame
/// measured, which anything between frames — a filter change most of all —
/// could have invalidated.
fn scroll_log(app: &mut App, ask: impl FnOnce(&mut app::PendingScroll)) {
    // The selection stays: the renderer moves it by however far the view
    // moved, so it keeps covering the text it was dragged across rather than
    // the coordinates that text used to occupy. Scrolling is still a
    // deliberate choice of where to be, though, so it takes ownership of the
    // view from the selection that paused following.
    app.follow_paused_for_selection = false;
    ask(&mut app.pending_scroll);
}

fn handle_failure_summary_key(
    key: KeyEvent,
    app: &mut App,
    _store: &mut LogStore,
) -> Result<(), TuiError> {
    match key.code {
        KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('i') => {
            return_to_logs(app);
            app.failure_summary_scroll = 0;
            return Ok(());
        }
        KeyCode::Up | KeyCode::Char('k') => app.scroll_failure_summary_by(-1),
        KeyCode::Down | KeyCode::Char('j') => app.scroll_failure_summary_by(1),
        KeyCode::PageUp => app.scroll_failure_summary_by(-10),
        KeyCode::PageDown => app.scroll_failure_summary_by(10),
        KeyCode::Home | KeyCode::Char('g') => app.scroll_failure_summary_to_top(),
        KeyCode::End | KeyCode::Char('G') => app.scroll_failure_summary_to_bottom(),
        _ => return Ok(()),
    }
    Ok(())
}

/// Keys for the filter panel. There is no commit and no revert: every toggle
/// applies to the pane the moment it happens — the reader watches it work
/// beside the panel — so Esc dismisses without undoing, and Enter is just
/// another way out rather than a "yes, really" step.
fn handle_filter_key(key: KeyEvent, app: &mut App, _store: &mut LogStore) -> Result<(), TuiError> {
    if app.filter.query_editing() {
        match key.code {
            KeyCode::Enter => {
                let close_after_apply = app.filter.query_has_single_match();
                app.filter.apply_query();
                app.filter.end_query_edit();
                if close_after_apply {
                    return_to_logs(app);
                }
            }
            KeyCode::Tab => {
                app.filter.end_query_edit();
            }
            KeyCode::Backspace => {
                app.filter.pop_query_char();
            }
            KeyCode::Char(c) => {
                app.filter.push_query_char(c);
            }
            // One level up, not all the way out: the query goes, the list
            // stays, and the selection the query produced is untouched.
            KeyCode::Esc => {
                app.filter.clear_query();
            }
            _ => {}
        }
        return Ok(());
    }

    match key.code {
        KeyCode::Enter | KeyCode::Esc => return_to_logs(app),
        KeyCode::Char('R') => {
            app.filter.reset_to_defaults();
        }
        KeyCode::Char(' ') => {
            app.filter.toggle_highlighted();
        }
        KeyCode::Char('o') => {
            app.filter.select_only_highlighted();
        }
        KeyCode::Char('/') => {
            app.filter.begin_query_edit();
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.filter.highlight_prev();
        }
        KeyCode::Down | KeyCode::Char('j') => {
            app.filter.highlight_next();
        }
        _ => {}
    }
    Ok(())
}

fn handle_tasks_key(
    key: KeyEvent,
    app: &mut App,
    store: &mut LogStore,
    client: &std::sync::Arc<Client>,
) -> Result<(), TuiError> {
    let total = app.task_items().len();
    if key.code == KeyCode::Esc && app.widen_log_from_narrow() {
        return Ok(());
    }
    match app.tasks_table.handle_key(key, total) {
        StatusTableKeyOutcome::Redraw => {
            return Ok(());
        }
        StatusTableKeyOutcome::Close => {
            return_to_logs(app);
            return Ok(());
        }
        StatusTableKeyOutcome::None => {}
    }

    if key.code == KeyCode::Enter {
        let Some(item) = highlighted_task_item(app) else {
            return Ok(());
        };
        if !item.runnable() {
            return Ok(());
        }
        if item.has_params {
            open_form_for_task(app, &item.name, client)?;
        } else {
            let task_name = item.name;
            dispatch_run_task(client, task_name.clone());
            after_task_run(&task_name, app, store)?;
        }
    } else if key.code == KeyCode::Char('l') {
        let Some(item) = highlighted_task_item(app) else {
            return Ok(());
        };
        app.narrow_log_to(&item.name);
    } else if key.code == KeyCode::Char('a') {
        // Bridge into the highlighted task's PTY — the interactive-task flow.
        if let Some(item) = highlighted_task_item(app) {
            app.bridge_request = Some(item.name);
        }
    }
    Ok(())
}

fn handle_services_key(
    key: KeyEvent,
    app: &mut App,
    client: &std::sync::Arc<Client>,
    controls: &TuiControls,
) -> Result<(), TuiError> {
    let total = app.service_items().len();
    if key.code == KeyCode::Esc && app.widen_log_from_narrow() {
        return Ok(());
    }
    match app.services_table.handle_key(key, total) {
        StatusTableKeyOutcome::Redraw => {
            return Ok(());
        }
        StatusTableKeyOutcome::Close => {
            return_to_logs(app);
            return Ok(());
        }
        StatusTableKeyOutcome::None => {}
    }

    match key.code {
        KeyCode::Enter => {
            // Start or stop the highlighted service, depending on its state.
            if let Some(cmd) = overlay_toggle_command(app) {
                dispatch_overlay_command(client, &controls.lifecycle_emitter, cmd);
            }
        }
        KeyCode::Char('r') => {
            // Restart the highlighted service, if it's in a state that can
            // be restarted.
            if let Some(cmd) = highlighted_service_restart_command(app) {
                dispatch_overlay_command(client, &controls.lifecycle_emitter, cmd);
            }
        }
        KeyCode::Char('R') => {
            // Hard restart the highlighted service: force a rebuild, then
            // start/restart it on success.
            if let Some(cmd) = highlighted_service_hard_restart_command(app) {
                dispatch_overlay_command(client, &controls.lifecycle_emitter, cmd);
            }
        }
        KeyCode::Char('l') => {
            let Some(item) = highlighted_service_item(app) else {
                return Ok(());
            };
            app.narrow_log_to(&item.name);
        }
        KeyCode::Char('a') => {
            // Bridge into the highlighted service's PTY.
            if let Some(item) = highlighted_service_item(app) {
                app.bridge_request = Some(item.name);
            }
        }
        _ => {}
    }
    Ok(())
}

fn highlighted_task_item(app: &App) -> Option<app::TaskStatusItem> {
    let items = app.task_items();
    let idx = app.tasks_table.selected_index(items.len())?;
    items.get(idx).cloned()
}

fn highlighted_service_item(app: &App) -> Option<app::OverlayItem> {
    let items = app.service_items();
    let idx = app.services_table.selected_index(items.len())?;
    items.get(idx).cloned()
}

/// Build the Start/Stop command for the highlighted row, if it's an
/// actionable service. Returns `None` for in-flight services or when no row
/// is highlighted.
fn overlay_toggle_command(app: &App) -> Option<OverlayCommand> {
    let items = app.service_items();
    let idx = app.services_table.selected_index(items.len())?;
    let item = items.get(idx)?;
    match item.state {
        ServiceState::Ready | ServiceState::Running | ServiceState::Unhealthy => {
            Some(overlay_stop_command(item.name.clone()))
        }
        ServiceState::Stopped | ServiceState::Lazy => {
            Some(overlay_start_command(item.name.clone()))
        }
        // Failure is not a direction, so the process decides. A failed ready
        // check under `on_failure = "notify"` leaves something alive worth
        // stopping; a crash, or a dependency that failed before this ever got
        // to try, leaves nothing — and stopping what is already stopped is not
        // what anyone pressing enter on it meant.
        ServiceState::Failed | ServiceState::DependencyFailed => Some(if item.pid.is_some() {
            overlay_stop_command(item.name.clone())
        } else {
            overlay_start_command(item.name.clone())
        }),
        ServiceState::Pending
        | ServiceState::Building
        | ServiceState::Starting
        | ServiceState::Stopping => None,
    }
}

/// Restart command for `r` — only services in a restartable state.
fn highlighted_service_restart_command(app: &App) -> Option<OverlayCommand> {
    let items = app.service_items();
    let idx = app.services_table.selected_index(items.len())?;
    let item = items.get(idx)?;
    match item.state {
        ServiceState::Ready
        | ServiceState::Running
        | ServiceState::Unhealthy
        | ServiceState::Failed
        | ServiceState::DependencyFailed
        | ServiceState::Stopped => Some(overlay_restart_command(item.name.clone())),
        _ => None,
    }
}

/// Hard restart command for `R` — only services in a restartable state.
fn highlighted_service_hard_restart_command(app: &App) -> Option<OverlayCommand> {
    let items = app.service_items();
    let idx = app.services_table.selected_index(items.len())?;
    let item = items.get(idx)?;
    match item.state {
        ServiceState::Ready
        | ServiceState::Running
        | ServiceState::Unhealthy
        | ServiceState::Failed
        | ServiceState::DependencyFailed
        | ServiceState::Stopped
        | ServiceState::Lazy => Some(overlay_hard_restart_command(item.name.clone())),
        _ => None,
    }
}

/// Which control endpoint an overlay action maps to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlAction {
    Start,
    Stop,
    Restart,
    HardRestart,
}

impl ControlAction {
    fn label(self) -> &'static str {
        match self {
            ControlAction::Start => "start",
            ControlAction::Stop => "stop",
            ControlAction::Restart => "restart",
            ControlAction::HardRestart => "hard restart",
        }
    }
}

struct OverlayCommand {
    name: String,
    action: ControlAction,
}

fn overlay_start_command(name: String) -> OverlayCommand {
    OverlayCommand {
        name,
        action: ControlAction::Start,
    }
}

fn overlay_stop_command(name: String) -> OverlayCommand {
    OverlayCommand {
        name,
        action: ControlAction::Stop,
    }
}

fn overlay_restart_command(name: String) -> OverlayCommand {
    OverlayCommand {
        name,
        action: ControlAction::Restart,
    }
}

fn overlay_hard_restart_command(name: String) -> OverlayCommand {
    OverlayCommand {
        name,
        action: ControlAction::HardRestart,
    }
}

fn dispatch_overlay_command(
    client: &std::sync::Arc<Client>,
    emitter: &LifecycleEmitter,
    pending: OverlayCommand,
) {
    let client = client.clone();
    let emitter = emitter.clone();
    tokio::spawn(async move {
        let label = pending.action.label();
        emitter.service_event(&pending.name, &format!("{label} requested"));
        let result = match pending.action {
            ControlAction::Start => client.start(&pending.name).await,
            ControlAction::Stop => client.stop(&pending.name).await,
            ControlAction::Restart => client.restart(&pending.name).await,
            ControlAction::HardRestart => client.hard_restart(&pending.name).await,
        };
        if let Err(e) = result {
            emitter.service_error_event(&pending.name, &format!("{label} failed: {e}"));
        }
    });
}

/// Refetch the state projection off-loop and inject it as an input event.
///
/// Used when the event stream reports lag: the fetch must not run on the
/// render loop (a slow server would freeze the UI), and the result must
/// come back through the input channel so it is applied in order with
/// whatever the user is doing.
fn spawn_state_resync(client: &std::sync::Arc<Client>) {
    let Some(input_tx) = app_input_tx().cloned() else {
        return;
    };
    let client = client.clone();
    tokio::spawn(async move {
        let Ok(processes) = client.status(false, None).await else {
            return;
        };
        let startup_complete = client.ready().await.unwrap_or(false);
        let _ = input_tx
            .send(AppEvent::StateResync {
                processes,
                startup_complete,
            })
            .await;
    });
}

fn after_task_run(task_name: &str, app: &mut App, _store: &LogStore) -> Result<(), TuiError> {
    // Make sure the task's own output is admitted, so pressing enter is
    // followed by seeing something happen.
    let filter_changed = app.filter.select_name(task_name);

    // The panel stays open. This used to close it — right when the tasks
    // table was full-screen and running something had to hand the logs back,
    // wrong now that the logs are already beside it. Closing meant enter
    // ran the task *and* dismissed the list you were running things from.
    //
    // Nothing to redraw here: the filter change is a different view over the
    // same store, and the loop paints it on the next frame.
    let _ = filter_changed;
    Ok(())
}

/// Fire a param-less task run without waiting for the outcome. State
/// updates come through the event stream like any other transition.
fn dispatch_run_task(client: &std::sync::Arc<Client>, name: String) {
    dispatch_run_task_with_params(client, name, std::collections::HashMap::new());
}

/// Fire a task run with the params map the user just submitted via the
/// form modal. The HTTP result is swallowed on success; failures surface
/// through the event stream (`task_state_changed` → failed).
fn dispatch_run_task_with_params(
    client: &std::sync::Arc<Client>,
    name: String,
    params: std::collections::HashMap<String, String>,
) {
    let client = client.clone();
    tokio::spawn(async move {
        let _ = client.run_task(&name, params).await;
    });
}

/// Build the form state for `task_name`, transition to `ViewMode::Form`,
/// and kick off any completion fetches for fields with dynamic sources.
///
/// `command_tx` is used to send [`RunnerCommand::ResolveCompletions`] and
/// to relay completion replies back into the TUI loop via the shared
/// input channel. We reuse that channel — rather than opening a second
/// async source — so the main `select!` doesn't have to grow another arm.
fn open_form_for_task(
    app: &mut App,
    task_name: &str,
    client: &std::sync::Arc<Client>,
) -> Result<(), TuiError> {
    let Some(task) = app.task_configs.get(task_name).cloned() else {
        // Palette built the action from task_configs, so a missing entry
        // here is impossible. Keep the early-return rather than unwrapping.
        return Ok(());
    };
    let Some(form) = form::FormState::new(task_name, &task) else {
        return Ok(());
    };

    let dyn_fields: Vec<String> = form
        .fields
        .iter()
        .filter(|f| f.has_dynamic_completions)
        .map(|f| f.name.clone())
        .collect();

    app.form = Some(form);
    app.set_view_mode(ViewMode::Form);

    // Kick off an initial fetch for every field that needs it. The replies
    // come back through `input_tx` so they land in the same event queue
    // the main loop already reads.
    for param in dyn_fields {
        request_form_completion(app, task_name, &param, false, client);
    }
    Ok(())
}

/// Spawn the background request/reply wiring for one completion fetch.
/// The reply is converted into `AppEvent::CompletionsReady` and sent to
/// the TUI loop through the global input channel.
fn request_form_completion(
    app: &mut App,
    task: &str,
    param: &str,
    force_refresh: bool,
    client: &std::sync::Arc<Client>,
) {
    let Some(form) = app.form.as_mut() else {
        return;
    };
    let partial: std::collections::HashMap<String, String> = form
        .fields
        .iter()
        .filter(|f| f.name != param && !f.value.is_empty())
        .map(|f| (f.name.clone(), f.value.clone()))
        .collect();
    let request_id = form.start_request(param);

    let client = client.clone();
    let Some(input_tx) = app_input_tx().cloned() else {
        return;
    };
    let task = task.to_string();
    let param = param.to_string();
    tokio::spawn(async move {
        let result = client
            .resolve_completions(&task, &param, partial, force_refresh)
            .await
            .map_err(|e| match e {
                // The server ran the completion command and it failed —
                // structured, with the log path the form renders.
                crate::client::ClientError::Completion(err) => err,
                // Transport-level failure — degrade to a plain message.
                other => crate::client::CompletionError {
                    message: other.to_string(),
                    log_path: None,
                },
            });
        let _ = input_tx
            .send(AppEvent::CompletionsReady {
                param,
                request_id,
                result,
            })
            .await;
    });
}

/// Shared, lazily-populated handle to the TUI loop's input channel. Set
/// once at the top of `run_tui` and cloned by background tasks that want
/// to inject events (e.g. completion replies). Using an `OnceLock` keeps
/// the API clean without threading the sender through every key handler.
static INPUT_TX: std::sync::OnceLock<mpsc::Sender<AppEvent>> = std::sync::OnceLock::new();

/// Fetch the input-channel handle. Returns `None` before [`run_tui`] runs
/// (e.g. in unit tests that exercise individual key handlers directly);
/// background tasks skip injection in that case rather than panicking.
fn app_input_tx() -> Option<&'static mpsc::Sender<AppEvent>> {
    INPUT_TX.get()
}

/// Handle keys while the form modal is open. Navigation, per-kind input,
/// candidate selection, and submit/cancel live here.
fn handle_form_key(
    key: KeyEvent,
    app: &mut App,
    store: &mut LogStore,
    client: &std::sync::Arc<Client>,
) -> Result<(), TuiError> {
    // Grab these up front so later `app.form` borrows don't conflict.
    let task_name = match app.form.as_ref() {
        Some(f) => f.task.clone(),
        None => return Ok(()),
    };

    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    match key.code {
        KeyCode::Esc => {
            app.form = None;
            return_to_logs(app);
            return Ok(());
        }
        KeyCode::Enter if ctrl => {
            // Submit regardless of focused field.
            try_submit_form(app, client, store)?;
            return Ok(());
        }
        KeyCode::Enter => {
            if let Some(form) = app.form.as_mut()
                && let Some(field) = form.focused_mut()
                && !matches!(field.kind, ParamKind::Bool)
                && !field.visible_candidates().is_empty()
            {
                field.accept_highlighted_candidate();
            }
            // If focused field is on the last row → submit. Otherwise advance.
            if let Some(form) = app.form.as_ref()
                && form.focus + 1 >= form.fields.len()
            {
                try_submit_form(app, client, store)?;
                return Ok(());
            }
            if let Some(form) = app.form.as_mut() {
                form.focus_next();
            }
        }
        KeyCode::Tab => {
            // Tab on a dynamic field = refresh completions; on others = move focus.
            let refresh = app
                .form
                .as_ref()
                .and_then(|f| f.focused())
                .is_some_and(|f| {
                    matches!(
                        f.candidates,
                        form::CandidateState::Loaded(_)
                            | form::CandidateState::Failed { .. }
                            | form::CandidateState::Loading
                    )
                });
            let focused_param = app
                .form
                .as_ref()
                .and_then(|f| f.focused())
                .map(|f| f.name.clone());
            if refresh && let Some(param) = focused_param {
                request_form_completion(app, &task_name, &param, true, client);
            } else if let Some(form) = app.form.as_mut() {
                form.focus_next();
            }
        }
        KeyCode::BackTab => {
            if let Some(form) = app.form.as_mut() {
                form.focus_prev();
            }
        }
        KeyCode::Up => {
            if let Some(form) = app.form.as_mut()
                && let Some(field) = form.focused_mut()
            {
                match field.kind {
                    ParamKind::Int => field.step_int(1),
                    _ => {
                        // Move candidate highlight up.
                        if field.candidate_highlight > 0 {
                            field.candidate_highlight -= 1;
                        }
                    }
                }
            }
        }
        KeyCode::Down => {
            if let Some(form) = app.form.as_mut()
                && let Some(field) = form.focused_mut()
            {
                match field.kind {
                    ParamKind::Int => field.step_int(-1),
                    _ => {
                        let max = field.visible_candidates().len().saturating_sub(1);
                        if field.candidate_highlight < max {
                            field.candidate_highlight += 1;
                        }
                    }
                }
            }
        }
        KeyCode::Char(' ') => {
            if let Some(form) = app.form.as_mut()
                && let Some(field) = form.focused_mut()
            {
                match field.kind {
                    ParamKind::Bool => field.toggle_bool(),
                    _ => {
                        field.value.push(' ');
                        field.candidate_highlight = 0;
                    }
                }
            }
        }
        KeyCode::Char(c) => {
            if let Some(form) = app.form.as_mut()
                && let Some(field) = form.focused_mut()
            {
                match field.kind {
                    ParamKind::Bool => {
                        // Letters don't map to a bool value — ignore.
                    }
                    ParamKind::Int => {
                        if c.is_ascii_digit() || (c == '-' && field.value.is_empty()) {
                            field.value.push(c);
                        }
                    }
                    _ => {
                        field.value.push(c);
                        field.candidate_highlight = 0;
                    }
                }
            }
        }
        KeyCode::Backspace => {
            if let Some(form) = app.form.as_mut()
                && let Some(field) = form.focused_mut()
            {
                field.value.pop();
            }
        }
        KeyCode::Right => {
            // On a field with candidates, Right accepts the highlight.
            if let Some(form) = app.form.as_mut()
                && let Some(field) = form.focused_mut()
            {
                field.accept_highlighted_candidate();
            }
        }
        _ => {}
    }
    Ok(())
}

/// Attempt to submit the form. On success: dispatch `RunnerCommand::RunTask`,
/// close the modal, return to Normal. On validation error: record it on the
/// form so the renderer can show it, and stay open.
fn try_submit_form(
    app: &mut App,
    client: &std::sync::Arc<Client>,
    store: &mut LogStore,
) -> Result<(), TuiError> {
    let (task_name, params) = {
        let Some(form) = app.form.as_mut() else {
            return Ok(());
        };
        match form.submit() {
            Ok(p) => {
                form.submit_error = None;
                (form.task.clone(), p)
            }
            Err(msg) => {
                form.submit_error = Some(msg);
                return Ok(());
            }
        }
    };
    dispatch_run_task_with_params(client, task_name.clone(), params);
    app.form = None;
    after_task_run(&task_name, app, store)?;
    Ok(())
}

/// Parse pre-rendered ANSI bytes into a styled ratatui [`Text`]. On parse
/// error fall back to rendering the bytes as lossy UTF-8 so we never drop a
/// log line entirely — a garbled line is better than a silent one.
fn parse_ansi(bytes: &[u8]) -> Text<'static> {
    match bytes.into_text() {
        Ok(text) => text,
        Err(_) => Text::raw(String::from_utf8_lossy(bytes).into_owned()),
    }
}

/// Parse one upstream-formatted line into its styled form.
///
/// Upstream guarantees one line per message, so a multi-line parse result can
/// only come from embedded newlines that sanitization let through; joining them
/// keeps the store's "one entry, one logical line" invariant, which the scroll
/// anchor depends on.
pub(crate) fn parse_ansi_line(bytes: &[u8]) -> ratatui::text::Line<'static> {
    let text = parse_ansi(bytes);
    let mut spans: Vec<ratatui::text::Span<'static>> = Vec::new();
    for line in text.lines {
        spans.extend(line.spans);
    }
    ratatui::text::Line::from(spans)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    /// An app that knows about one task, interactive or not.
    fn app_with_interactive_task(name: &str, interactive: bool) -> App {
        let toml = format!("[tasks.{name}]\ncmd = \"true\"\ninteractive = {interactive}\n");
        let config: crate::config::Config = toml.parse().unwrap();
        App::new(AppInit {
            service_names: Vec::new(),
            task_names: vec![name.to_string()],
            build_tool_names: Vec::new(),
            task_configs: config.tasks.clone().into_iter().collect(),
            task_last_runs: HashMap::new(),
            hidden_names: HashSet::new(),
            auto_filter_on_failure_names: HashSet::new(),
            cli_log_filter: None,
        })
    }

    fn app_with_service_state(state: ServiceState) -> App {
        let mut app = App::new(AppInit {
            service_names: vec!["api".to_string()],
            // Registered here and not only via `apply_task_state`: the filter
            // takes its names at construction, and a task it has never heard
            // of cannot be narrowed to.
            task_names: vec!["migrate".to_string()],
            build_tool_names: Vec::new(),
            task_configs: HashMap::new(),
            task_last_runs: HashMap::new(),
            hidden_names: HashSet::new(),
            auto_filter_on_failure_names: HashSet::new(),
            cli_log_filter: None,
        });
        app.apply_service_runtime("api".to_string(), state, None, Vec::new());
        app
    }

    /// A layout change has to force a full repaint, and the user needs a way to
    /// ask for one — the ordering of the `l` arms decides whether Ctrl+L is
    /// reachable at all, and an unguarded arm above it would silently swallow
    /// the chord into the log filter.
    #[test]
    fn a_layout_change_or_ctrl_l_asks_for_a_full_repaint() {
        use crossterm::event::{KeyEvent, KeyModifiers};

        struct Case {
            name: &'static str,
            key: KeyEvent,
            want_repaint: bool,
            want_filter_open: bool,
        }

        let cases = [
            Case {
                name: "ctrl+l asks for a repaint",
                key: KeyEvent::new(KeyCode::Char('l'), KeyModifiers::CONTROL),
                want_repaint: true,
                want_filter_open: false,
            },
            Case {
                // The chord and the letter are different keys now — `f` opens
                // the filter — but the arm order still has to hold: an
                // unguarded `l` above the Ctrl+L one would swallow the chord.
                name: "plain l is not a repaint",
                key: KeyEvent::new(KeyCode::Char('l'), KeyModifiers::NONE),
                want_repaint: false,
                want_filter_open: false,
            },
            Case {
                name: "f opens the filter",
                key: KeyEvent::new(KeyCode::Char('f'), KeyModifiers::NONE),
                want_repaint: false,
                want_filter_open: true,
            },
        ];

        for case in cases {
            let mut app = app_with_service_state(ServiceState::Ready);
            let mut store = LogStore::with_capacity(10);
            handle_normal_key(
                case.key,
                &mut app,
                &mut store,
                &writer::TerminalOut::discarding(),
            )
            .unwrap();
            assert_eq!(
                app.repaint_requested, case.want_repaint,
                "{}: repaint requested",
                case.name
            );
            assert_eq!(
                app.view_mode == ViewMode::Filter,
                case.want_filter_open,
                "{}: filter opened",
                case.name
            );
        }
    }

    /// A triple-click takes the message, not the row it landed on — from any
    /// row of it, including a continuation row. It no longer has to work out
    /// which rows the message occupies: the selection is the line's id, and
    /// every row of it carries the same one.
    #[test]
    fn triple_click_takes_the_whole_wrapped_message() {
        use crate::output::LogId;
        use logs::RowSource;

        // A three-row message between two one-row ones.
        let sources = [
            RowSource {
                id: LogId(1),
                offset: 0,
                indent: 6,
            },
            RowSource {
                id: LogId(2),
                offset: 0,
                indent: 6,
            },
            RowSource {
                id: LogId(2),
                offset: 14,
                indent: 6,
            },
            RowSource {
                id: LogId(2),
                offset: 28,
                indent: 6,
            },
            RowSource {
                id: LogId(3),
                offset: 0,
                indent: 6,
            },
        ];

        for (click, want) in [(0usize, 1u64), (1, 2), (2, 2), (3, 2), (4, 3)] {
            let mut app = app_with_service_state(ServiceState::Ready);
            app.log_pane_origin = (0, 0);
            app.log_row_sources = sources.to_vec();

            select_message(&mut app, u16::try_from(click).unwrap());

            let (start, end) = app.log_selection.span().expect("a selection");
            assert_eq!(start.id, LogId(want), "clicking row {click}");
            assert_eq!(end.id, LogId(want), "clicking row {click}");
            assert_eq!(start.offset, 0, "from the start of the message");
            assert_eq!(end.offset, usize::MAX, "to the end of it");
        }
    }

    /// Ctrl+arrow moves the split border in the arrow's own direction, and a
    /// burst of presses stacks — each builds on the last, not on whatever the
    /// previous *frame* happened to draw. (The renderer writes the granted
    /// size back into the stored extent each frame, which is what makes the
    /// stored number safe to build on.)
    #[test]
    fn ctrl_arrows_resize_the_split_and_bursts_stack() {
        struct Case {
            name: &'static str,
            side: panes::PaneSide,
            start_extent: u16,
            keys: &'static [KeyCode],
            want_extent: u16,
        }

        let cases = [
            Case {
                name: "right dock: left grows the panel",
                side: panes::PaneSide::Right,
                start_extent: 40,
                keys: &[KeyCode::Left],
                want_extent: 44,
            },
            Case {
                name: "right dock: right shrinks it",
                side: panes::PaneSide::Right,
                start_extent: 40,
                keys: &[KeyCode::Right],
                want_extent: 36,
            },
            Case {
                name: "a burst of presses stacks, one step each",
                side: panes::PaneSide::Right,
                start_extent: 40,
                keys: &[KeyCode::Left, KeyCode::Left, KeyCode::Left, KeyCode::Left],
                want_extent: 56,
            },
            Case {
                name: "bottom dock: up grows the panel",
                side: panes::PaneSide::Bottom,
                start_extent: 8,
                keys: &[KeyCode::Up],
                want_extent: 10,
            },
            Case {
                name: "bottom dock: down shrinks it",
                side: panes::PaneSide::Bottom,
                start_extent: 8,
                keys: &[KeyCode::Down],
                want_extent: 6,
            },
        ];

        for case in cases {
            let mut app = app_with_service_state(ServiceState::Ready);
            app.view_mode = ViewMode::Services;
            app.panel.side = case.side;
            app.panel.extent = case.start_extent;
            let mut store = LogStore::with_capacity(10);
            let client = std::sync::Arc::new(Client::with_socket_path("/dev/null".into()));
            let controls = TuiControls {
                terminal_out: writer::TerminalOut::discarding(),
                lifecycle_emitter: LifecycleEmitter::discarding(),
                mode: TuiMode::InProcess,
            };

            for key in case.keys {
                handle_key(
                    KeyEvent::new(*key, KeyModifiers::CONTROL),
                    &mut app,
                    &mut store,
                    &client,
                    &controls,
                )
                .unwrap();
            }

            assert_eq!(app.panel.extent, case.want_extent, "{}", case.name);
        }
    }

    /// Opening a panel sizes it to the terminal — until the reader has sized
    /// it themselves, after which their number is their number.
    #[test]
    fn panel_width_follows_the_terminal_until_customized() {
        use ratatui::layout::Rect;

        struct Case {
            name: &'static str,
            terminal_width: u16,
            customized: bool,
            prior_extent: u16,
            want_extent: u16,
        }

        let cases = [
            Case {
                name: "narrow terminal keeps the floor",
                terminal_width: 120,
                customized: false,
                prior_extent: 48,
                want_extent: 48,
            },
            Case {
                name: "wide terminal grows the panel",
                terminal_width: 200,
                customized: false,
                prior_extent: 48,
                want_extent: 80,
            },
            Case {
                name: "very wide caps at twice the old fixed width",
                terminal_width: 300,
                customized: false,
                prior_extent: 48,
                want_extent: 96,
            },
            Case {
                name: "a hand-sized panel is left alone",
                terminal_width: 300,
                customized: true,
                prior_extent: 30,
                want_extent: 30,
            },
        ];

        for case in cases {
            let mut app = app_with_service_state(ServiceState::Ready);
            app.panel_extent_customized = case.customized;
            app.panel.extent = case.prior_extent;
            app.panes.bar = Rect::new(0, 21, case.terminal_width, 3);

            open_services_panel(&mut app);

            assert_eq!(app.panel.extent, case.want_extent, "{}", case.name);
        }
    }

    /// Selecting log text works while a panel is open — the log is still on
    /// screen, and the guard that said otherwise dated from when every
    /// non-normal view took the whole frame.
    #[test]
    fn selection_works_beside_an_open_panel() {
        use crossterm::event::{MouseButton, MouseEvent, MouseEventKind};
        use ratatui::layout::Rect;

        struct Case {
            name: &'static str,
            view_mode: ViewMode,
            want_selection: bool,
        }

        let cases = [
            Case {
                name: "plain log",
                view_mode: ViewMode::Normal,
                want_selection: true,
            },
            Case {
                name: "services panel open",
                view_mode: ViewMode::Services,
                want_selection: true,
            },
            Case {
                name: "filter panel open",
                view_mode: ViewMode::Filter,
                want_selection: true,
            },
            Case {
                name: "full-screen failure summary: no log on screen",
                view_mode: ViewMode::Failures,
                want_selection: false,
            },
        ];

        for case in cases {
            let mut app = app_with_service_state(ServiceState::Ready);
            app.view_mode = case.view_mode;
            app.panes.logs = Rect::new(0, 0, 60, 20);
            app.panes.status = Some(Rect::new(60, 0, 40, 20));
            // A frame's worth of rows to point at: a selection is a place in
            // the log, so there has to be a log under the pointer.
            app.log_pane_origin = (0, 0);
            app.log_row_sources = (0..20u64)
                .map(|row| logs::RowSource {
                    id: LogId(row),
                    offset: 0,
                    indent: 0,
                })
                .collect();

            let mouse = |kind: MouseEventKind, column: u16| MouseEvent {
                kind,
                column,
                row: 5,
                modifiers: KeyModifiers::NONE,
            };
            let store = LogStore::with_capacity(4);
            handle_mouse(
                mouse(MouseEventKind::Down(MouseButton::Left), 10),
                std::time::Instant::now(),
                &mut app,
                &store,
            );
            handle_mouse(
                mouse(MouseEventKind::Drag(MouseButton::Left), 30),
                std::time::Instant::now(),
                &mut app,
                &store,
            );
            handle_mouse(
                mouse(MouseEventKind::Up(MouseButton::Left), 30),
                std::time::Instant::now(),
                &mut app,
                &store,
            );

            assert_eq!(
                app.log_selection.span().is_some(),
                case.want_selection,
                "{}",
                case.name
            );
        }
    }

    /// Enter is start-or-stop, and for a failure the process decides which.
    ///
    /// A service stranded by a failed dependency was never started, so the one
    /// thing enter could not usefully mean on it was "stop" — which is what it
    /// meant, leaving the row in `stopped` having done nothing anyone asked
    /// for.
    #[test]
    fn enter_on_a_failure_starts_it_unless_something_is_still_running() {
        struct Case {
            name: &'static str,
            state: ServiceState,
            pid: Option<i32>,
            want_start: bool,
        }

        let cases = [
            Case {
                name: "stranded by a dependency, never started",
                state: ServiceState::DependencyFailed,
                pid: None,
                want_start: true,
            },
            Case {
                name: "crashed, nothing left running",
                state: ServiceState::Failed,
                pid: None,
                want_start: true,
            },
            Case {
                name: "ready check failed under notify, process still alive",
                state: ServiceState::Failed,
                pid: Some(4242),
                want_start: false,
            },
            Case {
                name: "ready, so enter stops it",
                state: ServiceState::Ready,
                pid: Some(4242),
                want_start: false,
            },
        ];

        for case in cases {
            let mut app = app_with_service_state(ServiceState::Pending);
            app.apply_service_runtime("api".to_string(), case.state, case.pid, Vec::new());
            app.view_mode = ViewMode::Services;

            let command = overlay_toggle_command(&app)
                .unwrap_or_else(|| panic!("{}: enter should do something here", case.name));
            assert_eq!(
                matches!(command.action, ControlAction::Start),
                case.want_start,
                "{}: got {:?}",
                case.name,
                command.action
            );
        }
    }

    /// Acting on a row leaves the panel up. Running a task used to dismiss
    /// it — correct when the table was full-screen and running something had
    /// to hand the logs back, wrong once the logs are already beside it, and
    /// surprising either way: enter ran the task *and* closed the list you
    /// were running things from.
    // Dispatch spawns the command onto the runtime, so this needs one.
    #[tokio::test]
    async fn acting_on_a_row_keeps_the_panel_open() {
        struct Case {
            name: &'static str,
            mode: ViewMode,
            key: KeyCode,
        }

        let cases = [
            Case {
                name: "running a task",
                mode: ViewMode::Tasks,
                key: KeyCode::Enter,
            },
            Case {
                name: "starting or stopping a service",
                mode: ViewMode::Services,
                key: KeyCode::Enter,
            },
            Case {
                name: "restarting a service",
                mode: ViewMode::Services,
                key: KeyCode::Char('r'),
            },
        ];

        for case in cases {
            let mut app = app_with_service_state(ServiceState::Ready);
            app.apply_task_state(
                "migrate".to_string(),
                crate::client::TaskState::Pending,
                None,
                Vec::new(),
            );
            app.view_mode = case.mode;
            app.focus = panes::Focus::Panel;
            let mut store = LogStore::with_capacity(10);
            let client = std::sync::Arc::new(Client::with_socket_path("/dev/null".into()));
            let controls = TuiControls {
                terminal_out: writer::TerminalOut::discarding(),
                lifecycle_emitter: LifecycleEmitter::discarding(),
                mode: TuiMode::InProcess,
            };

            handle_key(
                KeyEvent::new(case.key, KeyModifiers::NONE),
                &mut app,
                &mut store,
                &client,
                &controls,
            )
            .unwrap();

            assert_eq!(app.view_mode, case.mode, "{}: panel stays open", case.name);
            assert_eq!(
                app.focus,
                panes::Focus::Panel,
                "{}: and keeps the keys",
                case.name
            );
        }
    }

    use crate::output::LogId;

    /// A task that says it wants a human gets the window opened for it, and
    /// gets it closed again when it succeeds. `interactive = true` used to
    /// print "run `don attach x`" and leave the reader to do it — a strange
    /// thing to ask of someone already watching the screen it would open on.
    #[test]
    fn an_interactive_task_opens_and_closes_its_own_window() {
        struct Case {
            name: &'static str,
            /// What the run settles into.
            finished: crate::client::TaskState,
            want_dismiss: bool,
        }

        let cases = [
            Case {
                name: "success takes the window with it",
                finished: crate::client::TaskState::Completed,
                want_dismiss: true,
            },
            Case {
                // The last screen a failed task drew is the reason it failed.
                name: "failure keeps it up to be read",
                finished: crate::client::TaskState::Failed,
                want_dismiss: false,
            },
        ];

        for case in cases {
            let mut app = app_with_interactive_task("conf-init", true);
            app.apply_task_state(
                "conf-init".to_string(),
                crate::client::TaskState::Running,
                None,
                Vec::new(),
            );
            assert_eq!(
                app.bridge_request.as_deref(),
                Some("conf-init"),
                "{}: the window is asked for",
                case.name
            );
            assert!(app.attach_opened_automatically, "{}", case.name);

            // The loop would have opened it; stand in for that.
            let name = app.bridge_request.take().unwrap();
            app.attach = Some(app::AttachView {
                name,
                window: attach_window::WindowRect::centred_in(Rect::new(0, 0, 80, 24)),
                grid: None,
                ended: false,
            });

            app.apply_task_state("conf-init".to_string(), case.finished, None, Vec::new());
            assert_eq!(
                app.attach_dismiss_requested, case.want_dismiss,
                "{}: dismissed?",
                case.name
            );
        }
    }

    /// The window opens for a task that asked for it, and for nothing else —
    /// and never over an attach the reader chose.
    #[test]
    fn nothing_else_opens_a_window_by_itself() {
        // An ordinary task runs without one.
        let mut plain = app_with_interactive_task("build", false);
        plain.apply_task_state(
            "build".to_string(),
            crate::client::TaskState::Running,
            None,
            Vec::new(),
        );
        assert!(
            plain.bridge_request.is_none(),
            "an ordinary task opens nothing"
        );

        // An attach already on screen is not replaced: one the reader asked
        // for outranks one nobody did.
        let mut busy = app_with_interactive_task("conf-init", true);
        busy.attach = Some(app::AttachView {
            name: "something-else".to_string(),
            window: attach_window::WindowRect::centred_in(Rect::new(0, 0, 80, 24)),
            grid: None,
            ended: false,
        });
        busy.apply_task_state(
            "conf-init".to_string(),
            crate::client::TaskState::Running,
            None,
            Vec::new(),
        );
        assert!(
            busy.bridge_request.is_none(),
            "an open window is not stolen"
        );

        // Re-publishing the same state is not a fresh start, so a window the
        // reader detached from does not spring back.
        let mut again = app_with_interactive_task("conf-init", true);
        again.apply_task_state(
            "conf-init".to_string(),
            crate::client::TaskState::Running,
            None,
            Vec::new(),
        );
        again.bridge_request = None;
        again.apply_task_state(
            "conf-init".to_string(),
            crate::client::TaskState::Running,
            None,
            Vec::new(),
        );
        assert!(
            again.bridge_request.is_none(),
            "only the transition into running opens one"
        );
    }

    /// The marks are bookkeeping over lines the store still holds, and the
    /// index is keyed on them — so both the key and the map have to behave
    /// when history moves underneath them.
    #[test]
    fn blank_marks_are_keyed_cheaply_and_do_not_outlive_their_lines() {
        let mut app = app_with_service_state(ServiceState::Ready);

        // Adding a mark must invalidate the row index: the line it belongs to
        // just got taller.
        let before = app.log_filter_fingerprint();
        app.mark_blank_after(LogId(7));
        let after_one = app.log_filter_fingerprint();
        assert_ne!(before, after_one, "a new mark re-keys the index");
        app.mark_blank_after(LogId(7));
        assert_ne!(
            after_one,
            app.log_filter_fingerprint(),
            "and so does a second blank on the same line"
        );
        assert_eq!(
            app.blank_after.get(&LogId(7)),
            Some(&2),
            "counted, not a set"
        );

        // Forgetting a mark for a line that is gone changes no row count, so
        // it must not re-key — otherwise every eviction of a marked line costs
        // a full rebuild of the index.
        app.mark_blank_after(LogId(20));
        let keyed = app.log_filter_fingerprint();
        app.prune_blank_marks(Some(LogId(20)));
        assert_eq!(
            app.blank_after.keys().copied().collect::<Vec<_>>(),
            vec![LogId(20)],
            "marks older than the store's oldest line are dropped"
        );
        assert_eq!(
            app.log_filter_fingerprint(),
            keyed,
            "dropping a mark on a line that is gone re-keys nothing"
        );

        // An empty store holds no lines, so it can hold no marks.
        app.prune_blank_marks(None);
        assert!(app.blank_after.is_empty());
    }

    /// Swapping the record in front swaps the marks with it, so the two views
    /// cannot write on each other.
    #[test]
    fn swapping_the_record_takes_the_screen_state_with_it() {
        let mut app = app_with_service_state(ServiceState::Ready);
        app.log_row_sources = vec![logs::RowSource {
            id: LogId(3),
            offset: 0,
            indent: 0,
        }];
        app.log_visible_rows = vec!["api | hello".to_string()];
        app.mark_blank_after(LogId(3));

        app.swap_log_view();
        assert!(app.debug_view, "the other record is in front");
        assert!(
            app.blank_after.is_empty(),
            "the other record has its own marks"
        );
        // The ids describe rows that are no longer on screen. Left behind,
        // Enter would mark a line belonging to the record that is now hidden —
        // an id this store does not hold, so it renders nothing and Enter
        // reads as dead.
        assert!(app.log_row_sources.is_empty(), "and its own screen");
        assert!(app.log_visible_rows.is_empty());

        app.swap_log_view();
        assert!(!app.debug_view);
        assert_eq!(
            app.blank_after.get(&LogId(3)),
            Some(&1),
            "coming back finds the marks where they were left"
        );
    }

    /// The filter panel answers to `f`, and the tables keep `l` for the
    /// highlighted row's log. They collided while the filter was on `l`:
    /// reaching for it from a table opened a popup instead.
    #[test]
    fn the_filter_and_a_row_s_log_have_their_own_keys() {
        struct Case {
            name: &'static str,
            from: ViewMode,
            key: KeyCode,
            want_mode: ViewMode,
            /// Whether `l` narrowed the log pane to the highlighted row.
            want_narrowed: bool,
        }

        let cases = [
            Case {
                name: "f reaches the filter from the services table",
                from: ViewMode::Services,
                key: KeyCode::Char('f'),
                want_mode: ViewMode::Filter,
                want_narrowed: false,
            },
            Case {
                name: "and from the tasks table",
                from: ViewMode::Tasks,
                key: KeyCode::Char('f'),
                want_mode: ViewMode::Filter,
                want_narrowed: false,
            },
            Case {
                name: "and is its own toggle",
                from: ViewMode::Filter,
                key: KeyCode::Char('f'),
                want_mode: ViewMode::Normal,
                want_narrowed: false,
            },
            Case {
                name: "l narrows the log to the highlighted service",
                from: ViewMode::Services,
                key: KeyCode::Char('l'),
                // The panel stays: the pane beside it is the log, so
                // narrowing it is showing the row's log.
                want_mode: ViewMode::Services,
                want_narrowed: true,
            },
            Case {
                name: "and to the highlighted task",
                from: ViewMode::Tasks,
                key: KeyCode::Char('l'),
                want_mode: ViewMode::Tasks,
                want_narrowed: true,
            },
        ];

        for case in cases {
            let mut app = app_with_service_state(ServiceState::Ready);
            app.apply_task_state(
                "migrate".to_string(),
                crate::client::TaskState::Pending,
                None,
                Vec::new(),
            );
            app.view_mode = case.from;
            app.focus = panes::Focus::Panel;
            let mut store = LogStore::with_capacity(10);
            let client = std::sync::Arc::new(Client::with_socket_path("/dev/null".into()));
            let controls = TuiControls {
                terminal_out: writer::TerminalOut::discarding(),
                lifecycle_emitter: LifecycleEmitter::discarding(),
                mode: TuiMode::InProcess,
            };

            handle_key(
                KeyEvent::new(case.key, KeyModifiers::NONE),
                &mut app,
                &mut store,
                &client,
                &controls,
            )
            .unwrap();

            assert_eq!(app.view_mode, case.want_mode, "{}", case.name);
            assert_eq!(
                app.filter_narrowed_from.is_some(),
                case.want_narrowed,
                "{}: narrowed?",
                case.name
            );
        }
    }

    /// The panel keys are toggles, focus follows the panel, and Esc always
    /// finds its way out — the routing invariants a reader depends on without
    /// thinking about them.
    #[test]
    fn panel_keys_toggle_and_route_focus() {
        struct Case {
            name: &'static str,
            keys: &'static [KeyCode],
            want_mode: ViewMode,
            want_focus: panes::Focus,
        }

        let cases = [
            Case {
                name: "opening a panel focuses it",
                keys: &[KeyCode::Char('s')],
                want_mode: ViewMode::Services,
                want_focus: panes::Focus::Panel,
            },
            Case {
                name: "the same key closes it again",
                keys: &[KeyCode::Char('t'), KeyCode::Char('t')],
                want_mode: ViewMode::Normal,
                want_focus: panes::Focus::Logs,
            },
            Case {
                name: "tab hands the keys to the log and back",
                keys: &[KeyCode::Char('s'), KeyCode::Tab, KeyCode::Tab],
                want_mode: ViewMode::Services,
                want_focus: panes::Focus::Panel,
            },
            Case {
                name: "with the log focused, a panel key switches panels",
                keys: &[KeyCode::Char('s'), KeyCode::Tab, KeyCode::Char('t')],
                want_mode: ViewMode::Tasks,
                want_focus: panes::Focus::Panel,
            },
            Case {
                name: "esc from the panel side closes it",
                keys: &[KeyCode::Char('s'), KeyCode::Esc],
                want_mode: ViewMode::Normal,
                want_focus: panes::Focus::Logs,
            },
            Case {
                name: "esc from the log side closes it too",
                keys: &[KeyCode::Char('s'), KeyCode::Tab, KeyCode::Esc],
                want_mode: ViewMode::Normal,
                want_focus: panes::Focus::Logs,
            },
        ];

        for case in cases {
            let mut app = app_with_service_state(ServiceState::Ready);
            let mut store = LogStore::with_capacity(10);
            let client = std::sync::Arc::new(Client::with_socket_path("/dev/null".into()));
            let controls = TuiControls {
                terminal_out: writer::TerminalOut::discarding(),
                lifecycle_emitter: LifecycleEmitter::discarding(),
                mode: TuiMode::InProcess,
            };
            for key in case.keys {
                handle_key(
                    KeyEvent::new(*key, KeyModifiers::NONE),
                    &mut app,
                    &mut store,
                    &client,
                    &controls,
                )
                .unwrap();
            }
            assert_eq!(app.view_mode, case.want_mode, "{}: mode", case.name);
            assert_eq!(app.focus, case.want_focus, "{}: focus", case.name);
        }
    }

    /// Enter's blank goes after the last line the reader can *see*, and only
    /// a view genuinely above the tail treats Enter as "take me back down".
    ///
    /// The failure modes this pins: marking the store's newest line put the
    /// blank on a filtered-out line and rendered nothing, and a view pinned
    /// at the tail by a settled selection swallowed the first Enter into a
    /// visually inert "resume".
    #[test]
    fn enter_marks_the_last_visible_line() {
        use crate::output::LogId;

        struct Case {
            name: &'static str,
            scroll: logs::Scroll,
            /// Geometry as the last frame measured it.
            rows_above: usize,
            total_rows: usize,
            /// Ids of the rows on screen, bottom-most last.
            visible: &'static [u64],
            /// The id that should carry a blank mark afterwards, if any.
            want_mark: Option<u64>,
            want_follow: bool,
        }

        let cases = [
            Case {
                name: "following: blank goes after the bottom visible row",
                scroll: logs::Scroll::Follow,
                rows_above: 80,
                total_rows: 100,
                visible: &[7, 9, 12],
                want_mark: Some(12),
                want_follow: true,
            },
            Case {
                name: "pinned at the tail still adds the blank, then follows",
                scroll: logs::Scroll::At {
                    id: LogId(7),
                    row: 0,
                },
                rows_above: 80,
                total_rows: 100,
                visible: &[7, 9, 12],
                want_mark: Some(12),
                want_follow: true,
            },
            Case {
                name: "held above the tail, Enter is the way back down",
                scroll: logs::Scroll::At {
                    id: LogId(3),
                    row: 0,
                },
                rows_above: 10,
                total_rows: 100,
                visible: &[3, 4, 5],
                want_mark: None,
                want_follow: true,
            },
            Case {
                name: "an empty pane has nothing to mark",
                scroll: logs::Scroll::Follow,
                rows_above: 0,
                total_rows: 0,
                visible: &[],
                want_mark: None,
                want_follow: true,
            },
        ];

        for case in cases {
            let mut app = app_with_service_state(ServiceState::Ready);
            app.log_scroll = case.scroll;
            app.log_rows_above = case.rows_above;
            app.log_total_rows = case.total_rows;
            app.log_pane_height = 20;
            app.log_row_sources = case
                .visible
                .iter()
                .map(|id| logs::RowSource {
                    id: LogId(*id),
                    offset: 0,
                    indent: 0,
                })
                .collect();
            let mut store = LogStore::with_capacity(10);

            handle_normal_key(
                KeyEvent::new(KeyCode::Enter, crossterm::event::KeyModifiers::NONE),
                &mut app,
                &mut store,
                &writer::TerminalOut::discarding(),
            )
            .unwrap();

            match case.want_mark {
                Some(id) => assert_eq!(
                    app.blank_after.get(&LogId(id)),
                    Some(&1),
                    "{}: the mark",
                    case.name
                ),
                None => assert!(
                    app.blank_after.is_empty(),
                    "{}: nothing should be marked",
                    case.name
                ),
            }
            assert_eq!(
                app.log_scroll == logs::Scroll::Follow,
                case.want_follow,
                "{}: following afterwards",
                case.name
            );
        }
    }

    /// The vim verticals map onto the same intent the arrows record, and `gg`
    /// is a chord — the cases that matter are the ones where the chord has to
    /// disarm: a `g` followed by anything that is not another `g`.
    #[test]
    fn vim_keys_record_the_same_intent_as_the_arrows() {
        use super::app::PendingScroll;
        use crossterm::event::{KeyEvent, KeyModifiers};

        struct Case {
            name: &'static str,
            keys: &'static [char],
            want: PendingScroll,
            /// Whether the view should have returned to following the tail.
            want_follow: bool,
        }

        let cases = [
            Case {
                name: "k is up one row",
                keys: &['k'],
                want: PendingScroll {
                    rows: -1,
                    ..PendingScroll::default()
                },
                want_follow: false,
            },
            Case {
                name: "j is down one row",
                keys: &['j'],
                want: PendingScroll {
                    rows: 1,
                    ..PendingScroll::default()
                },
                want_follow: false,
            },
            Case {
                name: "gg jumps to the top",
                keys: &['g', 'g'],
                want: PendingScroll {
                    to_top: true,
                    ..PendingScroll::default()
                },
                want_follow: false,
            },
            Case {
                name: "a broken chord is just the second key",
                keys: &['g', 'j'],
                want: PendingScroll {
                    rows: 1,
                    ..PendingScroll::default()
                },
                want_follow: false,
            },
            Case {
                name: "G returns to the tail",
                keys: &['G'],
                want: PendingScroll::default(),
                want_follow: true,
            },
        ];

        for case in cases {
            let mut app = app_with_service_state(ServiceState::Ready);
            // Start held somewhere, so returning to the tail is observable.
            app.log_scroll = logs::Scroll::At {
                id: crate::output::LogId(3),
                row: 0,
            };
            let mut store = LogStore::with_capacity(10);
            for key in case.keys {
                handle_normal_key(
                    KeyEvent::new(KeyCode::Char(*key), KeyModifiers::NONE),
                    &mut app,
                    &mut store,
                    &writer::TerminalOut::discarding(),
                )
                .unwrap();
            }
            assert_eq!(app.pending_scroll, case.want, "{}: intent", case.name);
            assert_eq!(
                app.log_scroll == logs::Scroll::Follow,
                case.want_follow,
                "{}: following",
                case.name
            );
        }
    }

    /// Wheel events arrive in bursts, several per frame, and each has to build
    /// on the last. They now accumulate as intent and are resolved once, when
    /// the pane is drawn and the geometry is true — so a burst is a sum, and a
    /// filter change between the burst and the frame cannot make it land
    /// somewhere unrelated.
    #[test]
    fn a_burst_of_scrolls_accumulates_and_resolves_once() {
        use super::app::PendingScroll;

        struct Case {
            name: &'static str,
            /// Deltas delivered without a frame in between.
            burst: &'static [isize],
            want: PendingScroll,
        }

        let cases = [
            Case {
                name: "one notch",
                burst: &[-3],
                want: PendingScroll {
                    rows: -3,
                    ..PendingScroll::default()
                },
            },
            Case {
                name: "ten notches are ten notches, not one",
                burst: &[-3, -3, -3, -3, -3, -3, -3, -3, -3, -3],
                want: PendingScroll {
                    rows: -30,
                    ..PendingScroll::default()
                },
            },
            Case {
                name: "back and forth nets out",
                burst: &[-3, -3, 3],
                want: PendingScroll {
                    rows: -3,
                    ..PendingScroll::default()
                },
            },
        ];

        for case in cases {
            let mut app = app_with_service_state(ServiceState::Ready);
            for delta in case.burst {
                let delta = *delta;
                scroll_log(&mut app, move |p| p.rows += delta);
            }
            assert_eq!(app.pending_scroll, case.want, "{}", case.name);
        }
    }

    #[test]
    fn dependency_failure_events_refresh_tui_detail() {
        struct Case {
            name: &'static str,
            state: ServiceState,
            dependencies: Vec<String>,
            want: Vec<String>,
        }
        let cases = vec![
            Case {
                name: "initial root cause",
                state: ServiceState::DependencyFailed,
                dependencies: vec!["db".to_string()],
                want: vec!["db".to_string()],
            },
            Case {
                name: "changed root cause without state change",
                state: ServiceState::DependencyFailed,
                dependencies: vec!["cache".to_string()],
                want: vec!["cache".to_string()],
            },
            Case {
                name: "recovery clears detail",
                state: ServiceState::Pending,
                dependencies: Vec::new(),
                want: Vec::new(),
            },
        ];

        let mut app = app_with_service_state(ServiceState::Pending);
        for case in cases {
            apply_runner_event(
                RunnerEvent::ServiceStateChanged {
                    name: "api".to_string(),
                    state: case.state,
                    pid: None,
                    failed_dependencies: case.dependencies,
                },
                &mut app,
            );
            let item = app
                .service_items()
                .into_iter()
                .find(|item| item.name() == "api")
                .unwrap();
            assert_eq!(item.failed_dependencies, case.want, "case: {}", case.name);
        }
    }

    #[test]
    fn overlay_uppercase_r_hard_restarts_highlighted_service() {
        let app = app_with_service_state(ServiceState::Ready);
        let Some(command) = highlighted_service_hard_restart_command(&app) else {
            panic!("expected hard restart command");
        };
        assert_eq!(command.action, ControlAction::HardRestart);
        assert_eq!(command.name, "api");
    }
}
