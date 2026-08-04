//! Interactive terminal UI for `don start`.
//!
//! The TUI owns stdout when we're attached to a real terminal. Log lines
//! stream into the terminal's native scrollback via [`Terminal::insert_before`],
//! and a persistent inline viewport at the bottom renders the status bar.
//! Pipe mode (non-TTY) bypasses this module entirely — [`OutputManager`]
//! still writes prefixed bytes directly to stdout in that case.
//!
//! Heavy formatting (color prefix, ANSI sanitization, verbose timestamps) is
//! already done upstream; we parse those pre-rendered ANSI bytes back into
//! a styled [`Text`] via `ansi-to-tui` so ratatui can render them into its
//! buffer model.
//!
//! ## Viewport model
//!
//! The inline [`Terminal`] is created once at startup with `Viewport::Inline(1)`
//! and never rebuilt. All non-Normal modes (Filter, task/service tables) render
//! into a separate alt-screen [`Terminal`] ([`Modal`]) that overlays the main
//! screen. Leaving the modal restores the main screen's previous contents;
//! new log lines received during the modal are inserted afterward so the
//! user sees what happened without replaying the whole retained log buffer.
//!
//! Avoiding inline rebuilds sidesteps a nasty crossterm race: `get_cursor_position`
//! reads stdin for the DSR response, and racing with the input task's
//! `EventStream` produces either a 2-second block or a misplaced viewport.
//!
//! ## Concurrency
//!
//! One `tokio::select!` loop owns [`App`], the ratatui [`Terminal`], [`LogStore`],
//! and the `Option<Modal>`. Three side channels feed it:
//! - Log lines from the upstream [`OutputManager`].
//! - [`RunnerEvent`]s from the runner broadcast (consumed directly, no side task).
//! - Raw key events from the input task (interpretation is mode-dependent).
//!
//! [`OutputManager`]: crate::output::OutputManager
//! [`Terminal`]: ratatui::Terminal
//! [`Terminal::insert_before`]: ratatui::Terminal::insert_before
//! [`Text`]: ratatui::text::Text

mod app;
mod backend;
mod events;
mod failure_summary;
mod filter;
mod form;
mod fuzzy;
mod input;
mod log_store;
mod render;
mod status_table;

use ansi_to_tui::IntoText;
use crossterm::cursor::MoveTo;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::Rect;
use ratatui::text::Text;
use ratatui::widgets::{Paragraph, Widget, Wrap};
use ratatui::{TerminalOptions, Viewport};
use tokio::sync::{broadcast, mpsc, oneshot};

use backend::FixedBottomBackend;

use crate::config::ParamKind;
use crate::output::{FormattedLogLine, LifecycleEmitter, VerbosityControl};
use crate::runner::{CommandResult, RunnerCommand, RunnerEvent, ServiceState, TerminalRequest};
use app::{App, AppInit, ViewMode, line_matches_log_popup};
use events::AppEvent;
use log_store::{DEFAULT_CAPACITY, LogStore};
use status_table::StatusTableKeyOutcome;

/// Errors that can escape the TUI event loop.
#[derive(Debug, thiserror::Error)]
pub enum TuiError {
    /// Raw mode toggle, cursor operations, ratatui backend IO.
    #[error("terminal io error: {0}")]
    Io(#[from] std::io::Error),
}

/// RAII guard that leaves raw mode on drop.
struct RawModeGuard;

impl RawModeGuard {
    fn enter() -> Result<Self, TuiError> {
        crossterm::terminal::enable_raw_mode()?;
        Ok(Self)
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        let _ = crossterm::terminal::disable_raw_mode();
    }
}

type TuiTerminal = Terminal<FixedBottomBackend<std::io::Stdout>>;

#[derive(Clone)]
struct TuiControls {
    verbosity: VerbosityControl,
    lifecycle_emitter: LifecycleEmitter,
}

/// Alt-screen full-screen terminal used by Filter, Palette, and Overlay
/// modes. RAII: entering/leaving alt screen is tied to construction/drop,
/// so an error mid-draw still restores the main screen.
///
/// A Fullscreen viewport avoids `compute_inline_size`'s cursor-position
/// probe, which would race with the input task's stdin reader. Because
/// of that, modals don't need the [`FixedBottomBackend`] wrapper the
/// inline terminal uses.
struct Modal {
    terminal: Terminal<CrosstermBackend<std::io::Stdout>>,
    replay_checkpoint: u64,
}

impl Modal {
    fn enter(replay_checkpoint: u64) -> Result<Self, TuiError> {
        execute!(std::io::stdout(), EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(std::io::stdout());
        let terminal = Terminal::with_options(
            backend,
            TerminalOptions {
                viewport: Viewport::Fullscreen,
            },
        )?;
        Ok(Self {
            terminal,
            replay_checkpoint,
        })
    }

    fn draw(&mut self, app: &mut App) -> Result<(), TuiError> {
        let size = self.terminal.size()?;
        let area: Rect = size.into();
        app.sync_log_popup_scroll(render::log_popup_visible_rows(area));
        if app.view_mode == ViewMode::Failures {
            let max_scroll = render::failure_summary_max_scroll(area, app);
            app.sync_failure_summary_scroll(max_scroll);
        }
        self.terminal.draw(|f| render::draw_modal(f, app))?;
        Ok(())
    }
}

impl Drop for Modal {
    fn drop(&mut self) {
        let _ = execute!(std::io::stdout(), LeaveAlternateScreen);
    }
}

/// Bundle of terminal-side state owned only while the TUI controls the
/// terminal. Torn down when a foreground task acquires the terminal and
/// rebuilt when it releases. State that has to survive the handoff
/// (`App`, [`LogStore`], the input channel) lives outside this struct.
struct ActiveTerm {
    _raw_guard: RawModeGuard,
    terminal: TuiTerminal,
    modal: Option<Modal>,
    input_handle: tokio::task::JoinHandle<()>,
}

impl ActiveTerm {
    fn enter(input_tx: &mpsc::Sender<AppEvent>) -> Result<Self, TuiError> {
        let raw_guard = RawModeGuard::enter()?;
        let terminal = build_inline_terminal()?;
        let input_handle = tokio::spawn(input::run(input_tx.clone()));
        Ok(Self {
            _raw_guard: raw_guard,
            terminal,
            modal: None,
            input_handle,
        })
    }

    /// Tear down terminal-side state cleanly so a foreground task can take
    /// the tty. `_raw_guard`'s Drop disables raw mode after `terminal.clear`
    /// flushes the inline viewport wipe.
    async fn tear_down(self) -> Result<(), TuiError> {
        self.input_handle.abort();
        // Wait until EventStream is dropped so stdin has a single owner
        // before the foreground task or rebuilt TUI starts reading it.
        let _ = self.input_handle.await;
        // `modal` drops first (LeaveAlternateScreen) before we touch the main
        // screen below.
        drop(self.modal);
        let mut terminal = self.terminal;
        // Clear the inline viewport rows so the foreground task doesn't
        // start writing on top of a stale status bar.
        let _ = terminal.clear();
        // Drop the terminal explicitly so any pending writes flush before
        // we disable raw mode (via `_raw_guard.drop`).
        drop(terminal);
        Ok(())
    }
}

/// Run the interactive TUI until the runner shuts down or the user quits.
///
/// Ctrl+C raises SIGINT to our own process so the installed signal handler
/// drives graceful shutdown (identical behavior to pipe mode, including the
/// two-Ctrl+C force-kill escalation).
#[allow(clippy::too_many_arguments)]
pub async fn run_tui(
    mut log_rx: mpsc::UnboundedReceiver<FormattedLogLine>,
    mut runner_events: broadcast::Receiver<RunnerEvent>,
    command_tx: mpsc::UnboundedSender<RunnerCommand>,
    verbosity: VerbosityControl,
    lifecycle_emitter: LifecycleEmitter,
    service_names: Vec<String>,
    task_names: Vec<String>,
    build_tool_names: Vec<String>,
    task_configs: std::collections::HashMap<String, crate::config::Task>,
    task_last_runs: std::collections::HashMap<String, crate::task_state::TaskRunInfo>,
    hidden_names: std::collections::HashSet<String>,
    auto_filter_on_failure_names: std::collections::HashSet<String>,
    cli_log_filter: Option<std::collections::HashSet<String>>,
    mut terminal_request_rx: mpsc::Receiver<TerminalRequest>,
) -> Result<(), TuiError> {
    let controls = TuiControls {
        verbosity,
        lifecycle_emitter,
    };

    let mut app = App::new(AppInit {
        service_names,
        task_names,
        build_tool_names,
        task_configs,
        task_last_runs,
        hidden_names,
        auto_filter_on_failure_names,
        cli_log_filter,
        verbose_enabled: controls.verbosity.is_enabled(),
    });
    let mut store = LogStore::with_capacity(DEFAULT_CAPACITY);

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

    // Terminal-side state — Some while the TUI owns the terminal, None
    // while a foreground task does. We start in the active state.
    let mut active: Option<ActiveTerm> = Some(ActiveTerm::enter(&input_tx)?);
    // Snapshot of `LogStore::next_id` taken when we hand the terminal to a
    // foreground task. On resume we replay only entries with id ≥ this so
    // pre-pause lines (already in the user's scrollback) aren't repeated.
    // Lines that arrived during the pause get rendered above the new
    // viewport via `insert_before`, preserving the foreground task's
    // output that's already in scrollback above them.
    let mut paused_checkpoint: Option<u64> = None;
    if let Some(act) = active.as_mut() {
        // Seed the viewport so the terminal reserves the bottom region
        // before the first `insert_before` call. Raw `draw` (not the
        // park-then-draw helper) avoids moving the cursor away from where
        // FixedBottomBackend just placed it.
        act.terminal.draw(|f| render::draw_bar(f, &app))?;
        // Seed the height the resize handler compares against, so the first
        // resize knows whether the height actually changed.
        app.last_screen_height = act.terminal.size()?.height;
    }

    // Drives the spinner and any other time-based UI. Skip-on-miss so the
    // spinner doesn't catch up in a burst after a slow render.
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(100));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Relative timestamps in modals ("5s ago") need wall-clock invalidation
    // even when no runner/key event arrives.
    let mut wall_clock_ticker = tokio::time::interval(std::time::Duration::from_secs(1));
    wall_clock_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Cached terminal width — refreshed at the start of each log batch.
    // Avoids a syscall per rendered log line, which becomes a real
    // bottleneck under noisy services (one syscall × thousands of lines/sec
    // stalls the TUI loop long enough that runner_events / shutdown
    // signaling can't keep up). Initialized lazily; the first log batch
    // refreshes it before the first `insert_line` call.
    #[allow(unused_assignments)]
    let mut cached_width: u16 = 80;

    // Cap on log lines drained per `tokio::select!` round. Picked so that:
    //  - large bursts (kafka spam, build output) still drain in a few rounds
    //  - the runner_events / input arms still get a turn often enough that
    //    state transitions (Stopping/Stopped) and Ctrl+C remain snappy.
    const LOG_BATCH_LIMIT: usize = 64;

    loop {
        let active_present = active.is_some();
        tokio::select! {
            maybe_line = log_rx.recv() => {
                match maybe_line {
                    Some(first) => {
                        if let Some(act) = active.as_mut() {
                            cached_width = act.terminal.size()?.width.max(1);
                        }

                        // Drain up to LOG_BATCH_LIMIT lines without yielding
                        // back to select. Each `insert_before` is a stdout
                        // write; batching lets us amortize the bar redraw
                        // (the *expensive* part — full back-buffer rebuild)
                        // across many lines instead of one redraw per line.
                        let mut batch: Vec<FormattedLogLine> = Vec::with_capacity(LOG_BATCH_LIMIT);
                        batch.push(first);
                        while batch.len() < LOG_BATCH_LIMIT {
                            match log_rx.try_recv() {
                                Ok(line) => batch.push(line),
                                Err(_) => break,
                            }
                        }

                        let mut bar_dirty = false;
                        let mut modal_dirty = false;
                        for line in batch {
                            if is_shutdown_start_line(&line) && !app.shutdown_started {
                                app.begin_shutdown();
                                if let Some(act) = active.as_mut() {
                                    act.modal = None;
                                }
                                bar_dirty = true;
                            }
                            // Only render to the terminal if we own it.
                            // While paused, the line still lands in
                            // `LogStore` so it can be replayed on resume.
                            if let Some(act) = active.as_mut()
                                && act.modal.is_none()
                                && app.should_render_log(&line.name, line.is_lifecycle)
                            {
                                insert_line(&mut act.terminal, &line, cached_width)?;
                                bar_dirty = true;
                            }
                            modal_dirty |= app.append_log_popup_line(&line);
                            let _ = store.push(line);
                        }
                        if let Some(act) = active.as_mut() {
                            if modal_dirty {
                                if let Some(m) = act.modal.as_mut() {
                                    m.draw(&mut app)?;
                                }
                            } else if bar_dirty {
                                draw_inline_bar(&mut act.terminal, &app)?;
                            }
                        }
                    }
                    None => break, // runner closed the log channel — shut down
                }
            }
            runner_result = runner_events.recv() => {
                match runner_result {
                    Ok(RunnerEvent::ShutdownStarted) => {
                        if let Some(act) = active.as_mut() {
                            enter_shutdown_mode(&mut app, &mut act.terminal, &mut act.modal)?;
                        } else if !app.shutdown_started {
                            app.begin_shutdown();
                        }
                    }
                    Ok(event) => {
                        let filter_changed = apply_runner_event(event, &mut app);
                        let lazy: std::collections::HashSet<String> = app
                            .services_state
                            .iter()
                            .filter(|(_, s)| matches!(s, crate::runner::ServiceState::Lazy))
                            .map(|(n, _)| n.clone())
                            .collect();
                        app.filter.set_hidden_from_display(lazy);
                        if let Some(act) = active.as_mut() {
                            if let Some(m) = act.modal.as_mut() {
                                m.draw(&mut app)?;
                            } else if filter_changed {
                                clear_and_replay(&mut act.terminal, &store, &app)?;
                            } else {
                                draw_inline_bar(&mut act.terminal, &app)?;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => {}
                }
            }
            maybe_event = input_rx.recv(), if input_open && active_present => {
                match maybe_event {
                    Some(event) => {
                        if let Some(act) = active.as_mut() {
                            handle_app_event(
                                event,
                                &mut app,
                                &mut act.terminal,
                                &mut store,
                                &command_tx,
                                &controls,
                                &mut act.modal,
                            )?;
                        }
                    }
                    None => {
                        input_open = false;
                    }
                }
            }
            terminal_req = terminal_request_rx.recv() => {
                match terminal_req {
                    Some(TerminalRequest::Acquire(ack)) => {
                        if let Some(act) = active.take() {
                            // Snapshot the log boundary so we know which
                            // lines arrived during the pause. Lines already
                            // in scrollback at this moment must NOT be
                            // replayed — they'd appear above the foreground
                            // task's output, which is jarring.
                            paused_checkpoint = Some(store.next_id());
                            act.tear_down().await?;
                        }
                        let _ = ack.send(());
                    }
                    Some(TerminalRequest::Release) if active.is_none() => {
                        {
                            // Don't clear the screen — the foreground task's
                            // output stays in scrollback. Build a fresh
                            // inline terminal anchored at the current cursor
                            // row (the row right after the task's output)
                            // via the DSR FixedBottomBackend issues during
                            // construction.
                            let mut act = ActiveTerm::enter(&input_tx)?;
                            act.terminal.draw(|f| render::draw_bar(f, &app))?;
                            cached_width = act.terminal.size()?.width.max(1);
                            // Replay only lines that arrived during the
                            // pause via `insert_before`. They land in
                            // scrollback right after the foreground task's
                            // output — the bar drifts down accordingly.
                            let since = paused_checkpoint.take().unwrap_or(0);
                            let mut replayed_any = false;
                            for entry in store.iter_since(since) {
                                if app.should_render_log(&entry.line.name, entry.line.is_lifecycle) {
                                    insert_line(&mut act.terminal, &entry.line, cached_width)?;
                                    replayed_any = true;
                                }
                            }
                            if replayed_any {
                                draw_inline_bar(&mut act.terminal, &app)?;
                            }
                            active = Some(act);
                        }
                    }
                    Some(TerminalRequest::Release) => {
                        // Already active — nothing to do. Lets the runner
                        // be conservative with release calls in error paths.
                    }
                    None => {
                        // Coordinator dropped — runner is gone. Loop exits
                        // via log_rx close or shutdown event.
                    }
                }
            }
            _ = ticker.tick(), if active_present => {
                app.spinner_frame = app.spinner_frame.wrapping_add(1);
                if let Some(act) = active.as_mut()
                    && act.modal.is_none()
                {
                    draw_inline_bar(&mut act.terminal, &app)?;
                }
            }
            _ = wall_clock_ticker.tick(), if active_present => {
                if app.view_mode.needs_wall_clock_redraw()
                    && let Some(act) = active.as_mut()
                    && let Some(m) = act.modal.as_mut()
                {
                    m.draw(&mut app)?;
                }
            }
        }
    }

    if let Some(act) = active.take() {
        act.tear_down().await?;
    }
    Ok(())
}

/// Build the persistent inline terminal used for log flow + bar.
///
/// Reserves [`render::BAR_VIEWPORT_HEIGHT`] rows at the bottom of the screen:
/// one blank buffer row, plus a bordered status box (top border + content
/// row + bottom border).
///
/// Note: no cursor parking here. [`FixedBottomBackend`] does a real DSR
/// on its first `get_cursor_position` call (inside `Terminal::with_options`)
/// so the viewport anchors right below the shell's pre-start output. That
/// keeps scrollback gap-free — the trade-off is that the bar starts at the
/// cursor row and drifts to the bottom as the first few log lines flow in.
fn build_inline_terminal() -> Result<TuiTerminal, TuiError> {
    let inner = CrosstermBackend::new(std::io::stdout());
    let backend = FixedBottomBackend::new(inner);
    let term = Terminal::with_options(
        backend,
        TerminalOptions {
            viewport: Viewport::Inline(render::BAR_VIEWPORT_HEIGHT),
        },
    )?;
    Ok(term)
}

/// Move the real cursor to `(0, screen_height - 1)`. Used before any
/// operation that may trigger ratatui's `autoresize` (which calls
/// `compute_inline_size` → `get_cursor_position` → [`FixedBottomBackend`])
/// so the fake "bottom of screen" cursor the wrapper reports actually
/// matches where `\n`s from `append_lines` will land and scroll.
fn park_cursor_at_bottom() -> Result<(), TuiError> {
    let (_cols, rows) = crossterm::terminal::size()?;
    let bottom = rows.saturating_sub(1);
    execute!(std::io::stdout(), MoveTo(0, bottom))?;
    Ok(())
}

/// Draw the inline bar, first parking the real cursor at the screen's
/// bottom row. If `terminal.draw`'s internal `autoresize` fires because
/// the terminal was resized since the last draw, the wrapper's fake
/// cursor (bottom of screen) and the real cursor will be at the same row
/// so `append_lines` scrolls correctly.
fn draw_inline_bar(terminal: &mut TuiTerminal, app: &App) -> Result<(), TuiError> {
    park_cursor_at_bottom()?;
    terminal.draw(|f| render::draw_bar(f, app))?;
    Ok(())
}

/// Reposition and redraw the inline bar after a terminal resize.
///
/// Unlike [`clear_and_replay`], this does **not** re-emit the retained log
/// history: a resize doesn't change which lines are visible, and the terminal
/// emulator reflows its own scrollback. Replaying here is what produced the
/// multi-second "scrollback takeover" on resize with a large history.
///
/// When `clear_for_ghost` is set, we first issue a `Clear(ClearType::All)`
/// (`\x1b[2J`) — *not* `Purge` (`\x1b[3J`). `2J` wipes the visible screen,
/// erasing any ghost of the bar that ratatui's autoresize left at its previous
/// row (its internal `clear()` only repaints the *new* viewport region), while
/// leaving the terminal's scrollback buffer intact so the user can still scroll
/// back through the full history. The caller only sets this on a height change:
/// a width-only resize keeps the bar on the same bottom row, so there is no
/// ghost to erase and we preserve the on-screen logs. [`draw_inline_bar`] then
/// re-anchors the viewport at the new bottom.
fn resize_inline_bar(
    terminal: &mut TuiTerminal,
    app: &App,
    clear_for_ghost: bool,
) -> Result<(), TuiError> {
    if clear_for_ghost {
        execute!(std::io::stdout(), Clear(ClearType::All))?;
    }
    draw_inline_bar(terminal, app)?;
    Ok(())
}

fn is_shutdown_start_line(line: &FormattedLogLine) -> bool {
    line.name == crate::output::LIFECYCLE_EVENT_NAME
        && String::from_utf8_lossy(&line.bytes).contains("shutting down gracefully")
}

fn enter_shutdown_mode(
    app: &mut App,
    terminal: &mut TuiTerminal,
    modal: &mut Option<Modal>,
) -> Result<(), TuiError> {
    if app.shutdown_started {
        return Ok(());
    }
    app.begin_shutdown();
    *modal = None;
    draw_inline_bar(terminal, app)?;
    Ok(())
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
        RunnerEvent::UpdateCheckComplete {
            current_version,
            latest_version,
        } => {
            app.set_update_check(current_version, latest_version);
            false
        }
        RunnerEvent::RebuildComplete { .. }
        | RunnerEvent::TaskRerunComplete { .. }
        | RunnerEvent::ShutdownStarted
        | RunnerEvent::ShutdownComplete => false,
    }
}

/// Wipe the entire visible area and replay every [`LogStore`] entry that
/// passes the current filter. Used after a filter commit/clear and when
/// returning from any modal that may have hidden new log lines.
///
/// Scrollback *buffer* (history above the visible area, managed by the
/// terminal emulator) is preserved — only on-screen pixels get wiped. So
/// the user can still scroll up to see their full log history including
/// pre-filter content.
fn clear_and_replay(
    terminal: &mut TuiTerminal,
    store: &LogStore,
    app: &App,
) -> Result<(), TuiError> {
    // Move the real cursor to (0, 0) and tell the wrapper to report the
    // same from its next `get_cursor_position` call. The subsequent
    // `terminal.resize` anchors the inline viewport at the top of the
    // screen; `insert_before` will then fill rows 0..N with replayed
    // log lines while the bar drifts downward, rather than pinning the
    // bar to the bottom with blank space above the replay content.
    execute!(std::io::stdout(), MoveTo(0, 0))?;
    terminal.backend_mut().force_next_cursor_top();
    // Re-place the viewport using the override. `resize` unconditionally
    // recomputes viewport placement (autoresize would skip when size is
    // unchanged) and its internal `self.clear()` wipes the visible
    // screen and resets ratatui's back buffer.
    let size = terminal.size()?;
    let area = Rect {
        x: 0,
        y: 0,
        width: size.width,
        height: size.height,
    };
    terminal.resize(area)?;
    // `resize` cleared the *visible* area but not the scrollback buffer.
    // Purge it (`\x1b[3J`) so pre-clear content and blank bands from
    // past `insert_before` scroll_ups don't linger when the user scrolls
    // up. Supported by most modern terminals; older ones silently ignore.
    execute!(std::io::stdout(), Clear(ClearType::Purge))?;
    let width = terminal.size()?.width.max(1);
    for entry in store.iter() {
        if app.should_render_log(&entry.line.name, entry.line.is_lifecycle) {
            insert_line(terminal, &entry.line, width)?;
        }
    }
    terminal.draw(|f| render::draw_bar(f, app))?;
    Ok(())
}

/// Leave the alt screen and insert only logs that arrived while it was open.
///
/// This preserves the user's current scrollback instead of clearing the
/// screen and replaying the whole retained [`LogStore`]. Filter commits still
/// use [`clear_and_replay`] when the active selection changed, because that is
/// the case where visible lines may need to disappear.
fn close_modal_and_replay_new_logs(
    terminal: &mut TuiTerminal,
    store: &LogStore,
    app: &App,
    modal: &mut Option<Modal>,
) -> Result<(), TuiError> {
    let Some(since) = modal.take().map(|m| m.replay_checkpoint) else {
        draw_inline_bar(terminal, app)?;
        return Ok(());
    };

    let width = terminal.size()?.width.max(1);
    for entry in store.iter_since(since) {
        if app.should_render_log(&entry.line.name, entry.line.is_lifecycle) {
            insert_line(terminal, &entry.line, width)?;
        }
    }
    draw_inline_bar(terminal, app)?;
    Ok(())
}

/// Dispatch an input or resize event.
fn handle_app_event(
    event: AppEvent,
    app: &mut App,
    terminal: &mut TuiTerminal,
    store: &mut LogStore,
    command_tx: &mpsc::UnboundedSender<RunnerCommand>,
    controls: &TuiControls,
    modal: &mut Option<Modal>,
) -> Result<(), TuiError> {
    match event {
        AppEvent::Resize => {
            // `terminal.size()` already reflects the new geometry by the time
            // this event is dispatched.
            let new_height = terminal.size()?.height;
            if let Some(m) = modal.as_mut() {
                m.draw(app)?;
            } else {
                // A resize changes geometry, not content: the active filter and
                // the set of visible lines are unchanged, and the terminal
                // emulator already reflows its own scrollback for us. Running
                // the full `clear_and_replay` here (purge scrollback + re-emit
                // every retained line via `insert_before`) floods the screen
                // for seconds on a large history and needlessly destroys the
                // user's real, larger scrollback. Just re-place and redraw the
                // inline bar. Only a height change moves the bar's bottom row
                // (and can leave a ghost of it behind), so only then do we
                // clear; a width-only resize keeps the reflowed logs on screen.
                resize_inline_bar(terminal, app, new_height != app.last_screen_height)?;
            }
            app.last_screen_height = new_height;
            // Caller-side state (cached_width in run_tui) is refreshed on the
            // next iteration via terminal.size() — handle_app_event doesn't
            // own that cache. The autoresize path inside ratatui has already
            // adopted the new size by this point.
        }
        AppEvent::Key(key) => handle_key(key, app, terminal, store, command_tx, controls, modal)?,
        AppEvent::CompletionsReady {
            param,
            request_id,
            result,
        } => {
            if let Some(form) = app.form.as_mut() {
                form.apply_completions(&param, request_id, result);
                redraw_modal(modal, app)?;
            }
        }
    }
    Ok(())
}

fn handle_key(
    key: KeyEvent,
    app: &mut App,
    terminal: &mut TuiTerminal,
    store: &mut LogStore,
    command_tx: &mpsc::UnboundedSender<RunnerCommand>,
    controls: &TuiControls,
    modal: &mut Option<Modal>,
) -> Result<(), TuiError> {
    // Ctrl+C: belt-and-suspenders shutdown. We both send a `Shutdown` command
    // directly down the runner channel AND raise SIGINT. The direct command
    // works even if the signal handler task has died or isn't being polled
    // (e.g., the runner is stuck pre-loop), and SIGINT preserves the
    // two-press force-kill escalation via the runner's signal counter.
    if key.modifiers.contains(KeyModifiers::CONTROL) {
        match key.code {
            KeyCode::Char('c') => {
                let _ = command_tx.send(RunnerCommand::Shutdown);
                let _ = nix::sys::signal::kill(
                    nix::unistd::Pid::this(),
                    nix::sys::signal::Signal::SIGINT,
                );
                enter_shutdown_mode(app, terminal, modal)?;
            }
            KeyCode::Char('v') | KeyCode::Char('V') => {
                let enabled = controls.verbosity.toggle();
                app.set_verbose_enabled(enabled);
                controls.lifecycle_emitter.lifecycle_event(if enabled {
                    "verbose logging enabled"
                } else {
                    "verbose logging disabled"
                });
                redraw_current_view(app, terminal, modal)?;
            }
            _ => {}
        }
        return Ok(());
    }

    if app.shutdown_started {
        return Ok(());
    }

    match app.view_mode {
        ViewMode::Normal => handle_normal_key(key, app, terminal, store, modal)?,
        ViewMode::Filter => handle_filter_key(key, app, terminal, store, modal)?,
        ViewMode::Tasks => handle_tasks_key(key, app, terminal, store, command_tx, modal)?,
        ViewMode::Services => {
            handle_services_key(key, app, terminal, store, command_tx, controls, modal)?;
        }
        ViewMode::Failures => handle_failure_summary_key(key, app, terminal, store, modal)?,
        ViewMode::Form => handle_form_key(key, app, terminal, store, command_tx, modal)?,
    }
    Ok(())
}

fn redraw_current_view(
    app: &mut App,
    terminal: &mut TuiTerminal,
    modal: &mut Option<Modal>,
) -> Result<(), TuiError> {
    if let Some(m) = modal.as_mut() {
        m.draw(app)?;
    } else {
        draw_inline_bar(terminal, app)?;
    }
    Ok(())
}

fn handle_normal_key(
    key: KeyEvent,
    app: &mut App,
    terminal: &mut TuiTerminal,
    store: &mut LogStore,
    modal: &mut Option<Modal>,
) -> Result<(), TuiError> {
    match key.code {
        KeyCode::Enter => {
            terminal.insert_before(1, |_buf| {})?;
            draw_inline_bar(terminal, app)?;
            let _ = store.push(FormattedLogLine {
                name: String::new(),
                is_lifecycle: false,
                bytes: Vec::new(),
            });
        }
        KeyCode::Char('l') => {
            app.filter.enter_edit();
            app.view_mode = ViewMode::Filter;
            let mut m = Modal::enter(store.next_id())?;
            m.draw(app)?;
            *modal = Some(m);
        }
        KeyCode::Char('t') => {
            app.tasks_table.reset();
            app.view_mode = ViewMode::Tasks;
            let mut m = Modal::enter(store.next_id())?;
            m.draw(app)?;
            *modal = Some(m);
        }
        KeyCode::Char('s') => {
            app.services_table.reset();
            app.view_mode = ViewMode::Services;
            let mut m = Modal::enter(store.next_id())?;
            m.draw(app)?;
            *modal = Some(m);
        }
        KeyCode::Char('i') if app.has_failure_summary() => {
            app.open_failure_summary();
            let mut m = Modal::enter(store.next_id())?;
            m.draw(app)?;
            *modal = Some(m);
        }
        KeyCode::Char('R') if app.filter.reset_to_defaults() => {
            clear_and_replay(terminal, store, app)?;
        }
        _ => {}
    }
    Ok(())
}

fn handle_failure_summary_key(
    key: KeyEvent,
    app: &mut App,
    terminal: &mut TuiTerminal,
    store: &mut LogStore,
    modal: &mut Option<Modal>,
) -> Result<(), TuiError> {
    match key.code {
        KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('i') => {
            app.view_mode = ViewMode::Normal;
            app.failure_summary_scroll = 0;
            close_modal_and_replay_new_logs(terminal, store, app, modal)?;
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
    redraw_current_view(app, terminal, modal)?;
    Ok(())
}

fn handle_filter_key(
    key: KeyEvent,
    app: &mut App,
    terminal: &mut TuiTerminal,
    store: &mut LogStore,
    modal: &mut Option<Modal>,
) -> Result<(), TuiError> {
    if app.filter.query_editing() {
        match key.code {
            KeyCode::Enter => {
                let close_after_apply = app.filter.query_has_single_match();
                app.filter.apply_query();
                let filter_changed = app.filter.selection_changed_from_snapshot();
                app.filter.end_query_edit();
                if close_after_apply {
                    app.filter.commit();
                    app.view_mode = ViewMode::Normal;
                    if filter_changed {
                        *modal = None;
                        clear_and_replay(terminal, store, app)?;
                    } else {
                        close_modal_and_replay_new_logs(terminal, store, app, modal)?;
                    }
                } else {
                    redraw_modal(modal, app)?;
                }
            }
            KeyCode::Tab => {
                app.filter.end_query_edit();
                redraw_modal(modal, app)?;
            }
            KeyCode::Backspace => {
                app.filter.pop_query_char();
                redraw_modal(modal, app)?;
            }
            KeyCode::Char(c) => {
                app.filter.push_query_char(c);
                redraw_modal(modal, app)?;
            }
            KeyCode::Esc => {
                app.filter.cancel_edit();
                app.view_mode = ViewMode::Normal;
                close_modal_and_replay_new_logs(terminal, store, app, modal)?;
            }
            _ => {}
        }
        return Ok(());
    }

    match key.code {
        KeyCode::Enter => {
            let filter_changed = app.filter.selection_changed_from_snapshot();
            app.filter.commit();
            app.view_mode = ViewMode::Normal;
            if filter_changed {
                *modal = None; // drops, leaves alt screen
                clear_and_replay(terminal, store, app)?;
            } else {
                close_modal_and_replay_new_logs(terminal, store, app, modal)?;
            }
        }
        KeyCode::Esc => {
            app.filter.cancel_edit();
            app.view_mode = ViewMode::Normal;
            close_modal_and_replay_new_logs(terminal, store, app, modal)?;
        }
        KeyCode::Char('R') => {
            app.filter.reset_edit_to_defaults();
            redraw_modal(modal, app)?;
        }
        KeyCode::Char(' ') => {
            app.filter.toggle_highlighted();
            redraw_modal(modal, app)?;
        }
        KeyCode::Char('o') => {
            app.filter.select_only_highlighted();
            redraw_modal(modal, app)?;
        }
        KeyCode::Char('/') => {
            app.filter.begin_query_edit();
            redraw_modal(modal, app)?;
        }
        KeyCode::Tab => {
            app.filter.begin_query_edit();
            redraw_modal(modal, app)?;
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.filter.highlight_prev();
            redraw_modal(modal, app)?;
        }
        KeyCode::Down | KeyCode::Char('j') => {
            app.filter.highlight_next();
            redraw_modal(modal, app)?;
        }
        _ => {}
    }
    Ok(())
}

fn handle_tasks_key(
    key: KeyEvent,
    app: &mut App,
    terminal: &mut TuiTerminal,
    store: &mut LogStore,
    command_tx: &mpsc::UnboundedSender<RunnerCommand>,
    modal: &mut Option<Modal>,
) -> Result<(), TuiError> {
    let total = app.task_items().len();
    if app.log_popup.is_some() {
        handle_log_popup_key(key, app);
        redraw_modal(modal, app)?;
        return Ok(());
    }
    match app.tasks_table.handle_key(key, total) {
        StatusTableKeyOutcome::Redraw => {
            redraw_modal(modal, app)?;
            return Ok(());
        }
        StatusTableKeyOutcome::Close => {
            app.view_mode = ViewMode::Normal;
            close_modal_and_replay_new_logs(terminal, store, app, modal)?;
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
            open_form_for_task(app, &item.name, command_tx)?;
            redraw_modal(modal, app)?;
        } else {
            let task_name = item.name;
            dispatch_run_task(command_tx, task_name.clone());
            return_to_logs_after_task_run(&task_name, app, terminal, store, modal)?;
        }
    } else if key.code == KeyCode::Char('l') {
        let Some(item) = highlighted_task_item(app) else {
            return Ok(());
        };
        open_log_popup_for_name(app, store, item.name);
        redraw_modal(modal, app)?;
    } else if key.code == KeyCode::Char('s') {
        let Some(item) = highlighted_task_item(app) else {
            return Ok(());
        };
        dispatch_stop_task(command_tx, item.name);
        redraw_modal(modal, app)?;
    }
    Ok(())
}

fn handle_services_key(
    key: KeyEvent,
    app: &mut App,
    terminal: &mut TuiTerminal,
    store: &mut LogStore,
    command_tx: &mpsc::UnboundedSender<RunnerCommand>,
    controls: &TuiControls,
    modal: &mut Option<Modal>,
) -> Result<(), TuiError> {
    let total = app.service_items().len();
    if app.log_popup.is_some() {
        handle_log_popup_key(key, app);
        redraw_modal(modal, app)?;
        return Ok(());
    }
    match app.services_table.handle_key(key, total) {
        StatusTableKeyOutcome::Redraw => {
            redraw_modal(modal, app)?;
            return Ok(());
        }
        StatusTableKeyOutcome::Close => {
            app.view_mode = ViewMode::Normal;
            close_modal_and_replay_new_logs(terminal, store, app, modal)?;
            return Ok(());
        }
        StatusTableKeyOutcome::None => {}
    }

    match key.code {
        KeyCode::Enter => {
            // Start or stop the highlighted service, depending on its state.
            if let Some(cmd) = overlay_toggle_command(app) {
                dispatch_overlay_command(command_tx, &controls.lifecycle_emitter, cmd);
            }
        }
        KeyCode::Char('r') => {
            // Restart the highlighted service, if it's in a state that can
            // be restarted.
            if let Some(cmd) = highlighted_service_restart_command(app) {
                dispatch_overlay_command(command_tx, &controls.lifecycle_emitter, cmd);
            }
        }
        KeyCode::Char('R') => {
            // Hard restart the highlighted service: force a rebuild, then
            // start/restart it on success.
            if let Some(cmd) = highlighted_service_hard_restart_command(app) {
                dispatch_overlay_command(command_tx, &controls.lifecycle_emitter, cmd);
            }
        }
        KeyCode::Char('l') => {
            let Some(item) = highlighted_service_item(app) else {
                return Ok(());
            };
            open_log_popup_for_name(app, store, item.name);
            redraw_modal(modal, app)?;
        }
        _ => {}
    }
    Ok(())
}

fn handle_log_popup_key(key: KeyEvent, app: &mut App) {
    match key.code {
        KeyCode::Esc | KeyCode::Char('q') => app.close_log_popup(),
        KeyCode::Up | KeyCode::Char('k') => app.scroll_log_popup_by(-1),
        KeyCode::Down | KeyCode::Char('j') => app.scroll_log_popup_by(1),
        KeyCode::PageUp => app.scroll_log_popup_by(-10),
        KeyCode::PageDown => app.scroll_log_popup_by(10),
        KeyCode::Home | KeyCode::Char('g') => app.scroll_log_popup_to_top(),
        KeyCode::End | KeyCode::Char('G') => app.scroll_log_popup_to_bottom(),
        _ => {}
    }
}

fn open_log_popup_for_name(app: &mut App, store: &LogStore, name: String) {
    let lines = store
        .iter()
        .filter(|entry| line_matches_log_popup(&name, &entry.line))
        .map(|entry| entry.line.bytes.clone())
        .collect();
    app.open_log_popup(name, lines);
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
        ServiceState::Failed | ServiceState::DependencyFailed => {
            Some(overlay_stop_command(item.name.clone()))
        }
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

struct OverlayCommand {
    name: String,
    action: &'static str,
    command: RunnerCommand,
    reply: oneshot::Receiver<CommandResult>,
}

fn overlay_start_command(name: String) -> OverlayCommand {
    let (reply, rx) = oneshot::channel();
    OverlayCommand {
        name: name.clone(),
        action: "start",
        command: RunnerCommand::Start { name, reply },
        reply: rx,
    }
}

fn overlay_stop_command(name: String) -> OverlayCommand {
    let (reply, rx) = oneshot::channel();
    OverlayCommand {
        name: name.clone(),
        action: "stop",
        command: RunnerCommand::Stop { name, reply },
        reply: rx,
    }
}

fn overlay_restart_command(name: String) -> OverlayCommand {
    let (reply, rx) = oneshot::channel();
    OverlayCommand {
        name: name.clone(),
        action: "restart",
        command: RunnerCommand::Restart { name, reply },
        reply: rx,
    }
}

fn overlay_hard_restart_command(name: String) -> OverlayCommand {
    let (reply, rx) = oneshot::channel();
    OverlayCommand {
        name: name.clone(),
        action: "hard restart",
        command: RunnerCommand::HardRestart { name, reply },
        reply: rx,
    }
}

fn dispatch_overlay_command(
    command_tx: &mpsc::UnboundedSender<RunnerCommand>,
    emitter: &LifecycleEmitter,
    pending: OverlayCommand,
) {
    let command_tx = command_tx.clone();
    let emitter = emitter.clone();
    tokio::spawn(async move {
        emitter.service_event(&pending.name, &format!("{} requested", pending.action));
        if command_tx.send(pending.command).is_err() {
            emitter.service_error_event(&pending.name, "control failed: runner unavailable");
            return;
        }
        match pending.reply.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => emitter
                .service_error_event(&pending.name, &format!("{} failed: {e}", pending.action)),
            Err(_) => emitter.service_error_event(
                &pending.name,
                &format!("{} failed: runner dropped reply", pending.action),
            ),
        }
    });
}

fn redraw_modal(modal: &mut Option<Modal>, app: &mut App) -> Result<(), TuiError> {
    if let Some(m) = modal.as_mut() {
        m.draw(app)?;
    }
    Ok(())
}

fn return_to_logs_after_task_run(
    task_name: &str,
    app: &mut App,
    terminal: &mut TuiTerminal,
    store: &LogStore,
    modal: &mut Option<Modal>,
) -> Result<(), TuiError> {
    let filter_changed = app.filter.select_name(task_name);
    app.view_mode = ViewMode::Normal;
    app.log_popup = None;

    if filter_changed {
        *modal = None;
        clear_and_replay(terminal, store, app)?;
    } else {
        close_modal_and_replay_new_logs(terminal, store, app, modal)?;
    }
    Ok(())
}

/// Fire a param-less [`RunnerCommand::RunTask`] without waiting for the reply.
/// State updates come through the runner event broadcast.
fn dispatch_run_task(command_tx: &mpsc::UnboundedSender<RunnerCommand>, name: String) {
    let command_tx = command_tx.clone();
    tokio::spawn(async move {
        let (reply_tx, _reply_rx) = oneshot::channel();
        let cmd = RunnerCommand::RunTask {
            name,
            params: std::collections::HashMap::new(),
            wait: false,
            wait_timeout: None,
            reply: reply_tx,
        };
        let _ = command_tx.send(cmd);
    });
}

/// Fire a `RunnerCommand::Stop` for a task without waiting for the reply. The
/// runner SIGKILLs a running task's process group; a no-op for non-running.
fn dispatch_stop_task(command_tx: &mpsc::UnboundedSender<RunnerCommand>, name: String) {
    let command_tx = command_tx.clone();
    tokio::spawn(async move {
        let (reply_tx, _reply_rx) = oneshot::channel();
        let _ = command_tx.send(RunnerCommand::Stop {
            name,
            reply: reply_tx,
        });
    });
}

/// Fire `RunnerCommand::RunTask` with the params map the user just submitted
/// via the form modal. Reply is swallowed; state updates come through the
/// event broadcast like any other runner command.
fn dispatch_run_task_with_params(
    command_tx: &mpsc::UnboundedSender<RunnerCommand>,
    name: String,
    params: std::collections::HashMap<String, String>,
) {
    let command_tx = command_tx.clone();
    tokio::spawn(async move {
        let (reply_tx, _reply_rx) = oneshot::channel();
        let _ = command_tx.send(RunnerCommand::RunTask {
            name,
            params,
            wait: false,
            wait_timeout: None,
            reply: reply_tx,
        });
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
    command_tx: &mpsc::UnboundedSender<RunnerCommand>,
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
    app.view_mode = ViewMode::Form;

    // Kick off an initial fetch for every field that needs it. The replies
    // come back through `input_tx` so they land in the same event queue
    // the main loop already reads.
    for param in dyn_fields {
        request_form_completion(app, task_name, &param, false, command_tx);
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
    command_tx: &mpsc::UnboundedSender<RunnerCommand>,
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

    let command_tx = command_tx.clone();
    let Some(input_tx) = app_input_tx().cloned() else {
        return;
    };
    let task = task.to_string();
    let param = param.to_string();
    tokio::spawn(async move {
        let (reply_tx, reply_rx) = oneshot::channel();
        if command_tx
            .send(RunnerCommand::ResolveCompletions {
                task,
                param: param.clone(),
                partial,
                force_refresh,
                reply: reply_tx,
            })
            .is_err()
        {
            return;
        }
        match reply_rx.await {
            Ok(result) => {
                let _ = input_tx
                    .send(AppEvent::CompletionsReady {
                        param,
                        request_id,
                        result,
                    })
                    .await;
            }
            Err(_) => {
                // Runner dropped the reply channel (shutting down) — nothing
                // useful to display; the form stays in Loading until the
                // user moves on.
            }
        }
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
    terminal: &mut TuiTerminal,
    store: &mut LogStore,
    command_tx: &mpsc::UnboundedSender<RunnerCommand>,
    modal: &mut Option<Modal>,
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
            app.view_mode = ViewMode::Normal;
            close_modal_and_replay_new_logs(terminal, store, app, modal)?;
            return Ok(());
        }
        KeyCode::Enter if ctrl => {
            // Submit regardless of focused field.
            try_submit_form(app, command_tx, terminal, store, modal)?;
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
                try_submit_form(app, command_tx, terminal, store, modal)?;
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
                request_form_completion(app, &task_name, &param, true, command_tx);
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
    redraw_modal(modal, app)?;
    Ok(())
}

/// Attempt to submit the form. On success: dispatch `RunnerCommand::RunTask`,
/// close the modal, return to Normal. On validation error: record it on the
/// form so the renderer can show it, and stay open.
fn try_submit_form(
    app: &mut App,
    command_tx: &mpsc::UnboundedSender<RunnerCommand>,
    terminal: &mut TuiTerminal,
    store: &mut LogStore,
    modal: &mut Option<Modal>,
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
                redraw_modal(modal, app)?;
                return Ok(());
            }
        }
    };
    dispatch_run_task_with_params(command_tx, task_name.clone(), params);
    app.form = None;
    return_to_logs_after_task_run(&task_name, app, terminal, store, modal)?;
    Ok(())
}

/// Insert a single formatted log line into the scrollback above the inline
/// viewport. Returns the number of terminal rows actually consumed.
fn insert_line(
    terminal: &mut TuiTerminal,
    line: &FormattedLogLine,
    width: u16,
) -> Result<u16, TuiError> {
    let text = parse_ansi(&line.bytes);
    let height = Paragraph::new(text.clone())
        .wrap(Wrap { trim: false })
        .line_count(width)
        .max(1) as u16;

    terminal.insert_before(height, |buf| {
        let area = Rect {
            x: 0,
            y: 0,
            width: buf.area.width,
            height: buf.area.height,
        };
        Paragraph::new(text)
            .wrap(Wrap { trim: false })
            .render(area, buf);
    })?;

    Ok(height)
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    fn app_with_service_state(state: ServiceState) -> App {
        let mut app = App::new(AppInit {
            service_names: vec!["api".to_string()],
            task_names: Vec::new(),
            build_tool_names: Vec::new(),
            task_configs: HashMap::new(),
            task_last_runs: HashMap::new(),
            hidden_names: HashSet::new(),
            auto_filter_on_failure_names: HashSet::new(),
            cli_log_filter: None,
            verbose_enabled: false,
        });
        app.apply_service_runtime("api".to_string(), state, None, Vec::new());
        app
    }

    #[test]
    fn overlay_enter_stops_failed_service_rows() {
        struct Case {
            name: &'static str,
            state: ServiceState,
        }

        let cases = vec![
            Case {
                name: "failed",
                state: ServiceState::Failed,
            },
            Case {
                name: "dependency failed",
                state: ServiceState::DependencyFailed,
            },
        ];

        for case in cases {
            let app = app_with_service_state(case.state);
            let Some(command) = overlay_toggle_command(&app) else {
                panic!("{}: expected command", case.name);
            };
            match command.command {
                RunnerCommand::Stop { name, .. } => {
                    assert_eq!(name, "api", "{}: wrong service", case.name);
                }
                _ => panic!("{}: expected stop command", case.name),
            }
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
        match command.command {
            RunnerCommand::HardRestart { name, .. } => {
                assert_eq!(name, "api");
            }
            _ => panic!("expected hard restart command"),
        }
    }
}
