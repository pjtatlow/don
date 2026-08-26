//! TUI application state — the single source of truth for what to render.
//!
//! Derived from runner events (status counts, service/task state for the
//! status tables) and from user input (view mode, filter, tables). Kept
//! deliberately small so rendering is a pure function of this struct plus
//! the terminal size.
//!
//! The main TUI loop is the only mutator — there's no shared `Arc<Mutex<_>>`.

use std::collections::{HashMap, HashSet};

use super::failure_summary::{self, FailureSummaryItem};
use super::filter::FilterState;
use super::form::FormState;
use super::status_table::{StatusTableState, retain_fuzzy_matches};
use crate::client::{ServiceState, TaskState};
use crate::config::Task;
use crate::output::LIFECYCLE_EVENT_NAME;
use crate::task_state::TaskRunInfo;

/// Top-level view mode. Determines how keys are interpreted and how the
/// inline viewport is laid out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum ViewMode {
    /// Log flow + status bar. Keys trigger mode changes or scrollback actions.
    #[default]
    Normal,
    /// Log-filter panel. Every toggle applies to the pane immediately; `/`
    /// enters query input, and Enter/Esc just close.
    Filter,
    /// Full-screen tasks table. Arrow keys move a highlight; Enter runs the
    /// selected task or opens its param form.
    Tasks,
    /// Full-screen services table (alternate screen). Arrow keys move a
    /// highlight; Enter toggles start/stop on the selected service, `r`
    /// restarts it, `R` hard-restarts it, `Esc` dismisses.
    Services,
    /// Full-screen summary of root failures and dependency-blocked items.
    Failures,
    /// Param-entry form for a task. Opened from the task table when the user
    /// selects a task with declared `params`. Collects values and, on
    /// submit, dispatches `RunnerCommand::RunTask { name, params, reply }`.
    Form,
}

/// A row in the services status table.
/// Exposed so the render path and the key handler agree on which row is
/// highlighted.
#[derive(Debug, Clone)]
pub(crate) struct OverlayItem {
    pub(crate) name: String,
    pub(crate) state: ServiceState,
    pub(crate) pid: Option<i32>,
    pub(crate) failed_dependencies: Vec<String>,
}

/// A row in the tasks status table.
#[derive(Debug, Clone)]
pub(crate) struct TaskStatusItem {
    pub(crate) name: String,
    pub(crate) state: TaskState,
    pub(crate) failed_dependencies: Vec<String>,
    pub(crate) last_run: Option<TaskRunInfo>,
    pub(crate) has_params: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UpdateBadge {
    pub(crate) current_version: String,
    pub(crate) latest_version: String,
}

impl TaskStatusItem {
    pub(crate) fn name(&self) -> &str {
        self.name.as_str()
    }

    pub(crate) fn runnable(&self) -> bool {
        !matches!(self.state, TaskState::Running | TaskState::Building)
    }

    fn sort_bucket(&self) -> u8 {
        match self.state {
            TaskState::Failed => 0,
            TaskState::DependencyFailed => 1,
            TaskState::PendingRun => 2,
            TaskState::Running | TaskState::Building => 3,
            TaskState::Pending => 4,
            TaskState::Completed => 5,
            TaskState::Skipped => 6,
        }
    }
}

impl OverlayItem {
    pub(crate) fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Sort bucket: actionable rows first, settled rows last. Putting
    /// `DependencyFailed` below `Failed` keeps the actual culprit at the top
    /// so the user sees the thing they need to look at, not the stranded
    /// dependents.
    fn sort_bucket(&self) -> u8 {
        match self.state {
            ServiceState::Failed | ServiceState::Unhealthy => 0,
            ServiceState::DependencyFailed => 1,
            ServiceState::Pending | ServiceState::Building | ServiceState::Starting => 2,
            ServiceState::Running => 3,
            ServiceState::Ready => 4,
            ServiceState::Stopping => 5,
            ServiceState::Stopped => 6,
            ServiceState::Lazy => 7,
        }
    }
}

/// The half of a log pane's state that is swapped out when the other pane is
/// brought to the front.
///
/// The TUI shows one log pane at a time: don's record of the processes, or
/// don's record of itself. Each keeps its own scroll position and its own row
/// index, so switching between them does not move either one or throw away work
/// — the index is the expensive thing, and rebuilding it on every toggle is
/// what made pressing `v` flash.
#[derive(Debug, Default)]
pub(crate) struct StashedView {
    pub(crate) index: super::view_index::ViewIndex,
    pub(crate) scroll: super::logs::Scroll,
    pub(crate) rows_above: usize,
    pub(crate) total_rows: usize,
    pub(crate) blank_after: HashMap<crate::output::LogId, u16>,
    pub(crate) blank_epoch: u64,
}

/// An attached process on screen: which one, where its window is, and the
/// last grid its emulator produced.
///
/// The grid is cached rather than fetched during the draw because reading it
/// is a round trip to the emulator thread, and `draw` is synchronous. The
/// session task refreshes it whenever output lands.
pub(crate) struct AttachView {
    pub(crate) name: String,
    pub(crate) window: super::attach_window::WindowRect,
    pub(crate) grid: Option<crate::output::emulator::Grid>,
    /// The process is gone, and this window is its last screen.
    ///
    /// The window used to vanish the moment the process exited, which for a
    /// task is the moment its answer finished being printed — so what you
    /// attached to watch was taken away at the instant it became worth
    /// reading. It stays until dismissed instead.
    pub(crate) ended: bool,
}

impl std::fmt::Debug for AttachView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AttachView")
            .field("name", &self.name)
            .field("window", &self.window)
            .field("grid", &self.grid.as_ref().map(|g| (g.cols, g.rows)))
            .field("ended", &self.ended)
            .finish()
    }
}

/// A scroll the reader asked for, in units that do not need geometry to
/// express: rows, pages, and the two ends.
///
/// Accumulates between frames — a wheel spin is many events per frame — and is
/// resolved once, against the geometry that actually exists when the pane is
/// drawn. Nothing here is clamped: clamping needs to know how much content
/// there is, which is exactly the knowledge this defers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PendingScroll {
    pub(crate) rows: isize,
    pub(crate) pages: isize,
    /// Jump to the oldest line held.
    pub(crate) to_top: bool,
    /// Stop following and hold the current position, without moving it.
    pub(crate) pin: bool,
}

impl PendingScroll {
    pub(crate) fn is_empty(self) -> bool {
        self == Self::default()
    }
}

/// Aggregate counts derived from service/task state, displayed on the bar.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct StatusCounts {
    pub(crate) services_total: usize,
    pub(crate) services_ready: usize,
    pub(crate) services_failed: usize,
    /// Services running but with a failing health-check monitor.
    pub(crate) services_unhealthy: usize,
    /// Services actively transitioning (Pending, Starting, Running-not-ready,
    /// Stopping). Used to light up the spinner — `Ready`/`Stopped`/`Failed`/
    /// `Lazy` don't count as "doing work".
    pub(crate) services_active: usize,
    pub(crate) tasks_running: usize,
    pub(crate) tasks_pending_run: usize,
    pub(crate) tasks_failed: usize,
}

impl StatusCounts {
    /// Derive counts from the current service/task state maps.
    ///
    /// Services in [`ServiceState::Lazy`] are excluded from `services_total`:
    /// they haven't been started (and may never be, if no connection arrives),
    /// so counting them makes `N/M services ready` look permanently behind.
    /// Once a lazy service is triggered it leaves the `Lazy` state and
    /// rejoins the count.
    pub(crate) fn from_state(
        services: &HashMap<String, ServiceState>,
        tasks: &HashMap<String, TaskState>,
    ) -> Self {
        let mut counts = Self::default();
        for state in services.values() {
            if matches!(state, ServiceState::Lazy) {
                continue;
            }
            counts.services_total += 1;
            match state {
                ServiceState::Ready => counts.services_ready += 1,
                // DependencyFailed rolls into failed — from the user's
                // perspective it's still "not running because something broke".
                ServiceState::Failed | ServiceState::DependencyFailed => {
                    counts.services_failed += 1
                }
                ServiceState::Unhealthy => counts.services_unhealthy += 1,
                ServiceState::Pending
                | ServiceState::Building
                | ServiceState::Starting
                | ServiceState::Running
                | ServiceState::Stopping => counts.services_active += 1,
                ServiceState::Stopped | ServiceState::Lazy => {}
            }
        }
        for state in tasks.values() {
            match state {
                TaskState::PendingRun => counts.tasks_pending_run += 1,
                TaskState::Running => counts.tasks_running += 1,
                TaskState::Failed | TaskState::DependencyFailed => counts.tasks_failed += 1,
                _ => {}
            }
        }
        counts
    }

    /// True when the runner is actively working on something — drives the
    /// spinner on the status bar.
    pub(crate) fn is_working(&self) -> bool {
        self.services_active > 0 || self.tasks_running > 0
    }
}

/// Top-level TUI app state. Owns everything the renderer reads from.
#[derive(Debug)]
pub(crate) struct App {
    /// Set by key handling (remote-mode Ctrl+D) to ask the main loop to
    /// exit cleanly, restoring the terminal, without a shutdown.
    pub(crate) exit_requested: bool,
    /// Set by the overlay 'a' key: bridge the terminal into this item's
    /// PTY. Consumed by the main loop, which tears the TUI down, runs the
    /// bridge, and rebuilds.
    pub(crate) bridge_request: Option<String>,
    /// Whether the window on screen opened itself rather than being asked for.
    /// Only an automatic one closes itself again.
    pub(crate) attach_opened_automatically: bool,
    /// The loop should close the attach window. Set from here, acted on there,
    /// like [`Self::bridge_request`] — this side has the state that decides,
    /// that side owns the session.
    pub(crate) attach_dismiss_requested: bool,
    /// The process whose screen is in the floating window, and where that
    /// window sits. `None` when nothing is attached.
    pub(crate) attach: Option<AttachView>,
    pub(crate) counts: StatusCounts,
    pub(crate) view_mode: ViewMode,
    /// Graceful shutdown is in progress: the inline bar becomes
    /// non-interactive and `[don]`-prefixed lifecycle events bypass the
    /// committed filter (raw service stdout still respects it).
    pub(crate) shutdown_started: bool,
    pub(crate) filter: FilterState,
    /// Monotonically incrementing frame counter, driven by the TUI's timer
    /// tick. The renderer mods into the spinner frame table. Wraps freely.
    pub(crate) spinner_frame: usize,
    /// Current service state — tracked here (not in a side task) because the
    /// status tables read it at render time. Seeded with every service name
    /// in `ServiceState::Pending` so the bar shows `0/N ready` from frame 1.
    pub(crate) services_state: HashMap<String, ServiceState>,
    pub(crate) service_pids: HashMap<String, Option<i32>>,
    pub(crate) tasks_state: HashMap<String, TaskState>,
    failed_dependencies: HashMap<String, Vec<String>>,
    pub(crate) tasks_last_run: HashMap<String, TaskRunInfo>,
    pub(crate) update_badge: Option<UpdateBadge>,
    pub(crate) services_table: StatusTableState,
    pub(crate) tasks_table: StatusTableState,
    /// Vertical scroll offset for the wrapped failure-summary view.
    pub(crate) failure_summary_scroll: usize,
    /// Static task-config snapshot — populated at TUI startup so the
    /// table/form can inspect declared params without reaching back into
    /// the runner. Immutable for the session; the runner re-validates on
    /// submit anyway.
    pub(crate) task_configs: HashMap<String, Task>,
    /// Names that should be inserted into the committed log filter when they
    /// fail. Derived from top-level/service/task config at TUI startup.
    auto_filter_on_failure_names: HashSet<String>,
    /// Active form modal, or `None` when not in [`ViewMode::Form`].
    pub(crate) form: Option<FormState>,
    /// Active service/task log popup shown over the services/tasks table.
    /// The selection `l` replaced, so Esc can put it back.
    ///
    /// Narrowing to one process is a look, not a decision — the filter you
    /// had built is what you want back afterwards, and rebuilding it by hand
    /// is the reason a modal felt necessary in the first place.
    pub(crate) filter_narrowed_from: Option<std::collections::HashSet<String>>,
    /// Where the panes ended up in the last frame. Written by the renderer and
    /// read by mouse handling, so a click resolves against the rectangles that
    /// were actually drawn rather than a second computation of them.
    pub(crate) panes: super::panes::Panes,
    /// Where the side panel docks and how big it is. Whether it is *open* is
    /// the view mode's fact — see [`Self::panel_open`].
    pub(crate) panel: super::panes::Panel,
    /// Whether the reader has sized the panel themselves — a divider drag or a
    /// Ctrl+arrow. Until they have, opening a panel picks a width from the
    /// terminal's; once they have, their number is their number.
    pub(crate) panel_extent_customized: bool,
    /// The layout the screen was last painted with, and whether a full repaint
    /// has been asked for. A pane opening, moving or resizing changes which
    /// cells mean what, and a diffing renderer only rewrites cells it believes
    /// changed — so anything the old layout drew that the new one does not
    /// reach stays on screen. The divider is the visible case: a dashed rule
    /// left behind in the middle of the log.
    pub(crate) painted_layout: Option<(super::panes::Panel, bool)>,
    /// Set by the redraw key, cleared by the next paint.
    pub(crate) repaint_requested: bool,
    /// Which pane takes keys both could claim.
    pub(crate) focus: super::panes::Focus,
    /// Set while the divider is being dragged, so motion resizes instead of
    /// selecting text.
    pub(crate) dragging_divider: bool,
    /// A `g` was pressed and the next key decides: another `g` jumps to the
    /// top, anything else is just itself. Vim's chord, minus the timeout —
    /// a stale half-chord is cleared by whatever key comes next.
    pub(crate) pending_g: bool,
    /// The row order each status table was opened with.
    ///
    /// The tables sort by state — failures first — which is what you want when
    /// the view opens and exactly what you do not want afterwards: starting or
    /// stopping something changes its bucket, so the row moves out from under
    /// the cursor that acted on it. Capturing the order at open keeps the
    /// useful sort and makes the list hold still. Empty means "sort by state",
    /// which is how the first render populates it.
    pub(crate) services_order: Vec<String>,
    pub(crate) tasks_order: Vec<String>,
    /// The admitted-lines index the pane positions itself with. Mended once a
    /// frame; see [`super::view_index`] for why it is not recomputed.
    pub(crate) view_index: super::view_index::ViewIndex,
    /// Where the log pane is looking. `Follow` until the user scrolls away.
    pub(crate) log_scroll: super::logs::Scroll,
    /// The drag in progress, or the last one that settled. Screen coordinates,
    /// so it is discarded whenever the view moves under it.
    pub(crate) log_selection: super::selection::Selection,
    /// What the last copy did, and when it was said. OSC 52 gets no reply, so
    /// this is the only feedback there can be — and it is transient, because a
    /// badge that never leaves stops reading as an answer to what you just did
    /// and starts reading as part of the furniture.
    pub(crate) copy_notice: Option<(String, std::time::Instant)>,
    /// A click's position, time and how many clicks have landed there in a
    /// row. Double- and triple-click are the same button event as a single
    /// one; only the gap between them tells them apart.
    pub(crate) last_click: Option<(u16, u16, std::time::Instant, u8)>,
    /// Set when a selection paused following, so clearing it can resume.
    /// Without this, `esc` after selecting would strand a reader who had
    /// deliberately scrolled up before selecting.
    pub(crate) follow_paused_for_selection: bool,
    /// The plain text of the rows the last frame drew, and where the pane
    /// started. Written by the renderer so a copy resolves against exactly what
    /// was on screen rather than re-deriving wrapping, filtering and scroll.
    pub(crate) log_visible_rows: Vec<String>,
    /// The log line each visible row belongs to, parallel to
    /// `log_visible_rows`. Lets a triple-click take the whole message when it
    /// wrapped across several rows.
    pub(crate) log_row_sources: Vec<super::logs::RowSource>,
    pub(crate) log_pane_origin: (u16, u16),
    /// Geometry the last frame produced, so the input layer can move the
    /// scroll anchor without re-deriving what only the renderer knows: how
    /// tall the pane came out and how much admitted content there is at this
    /// width. Written by the renderer, read by key and mouse handling.
    /// What the reader has asked the view to do, not yet resolved.
    ///
    /// Input records intent; the renderer resolves it. Scroll arithmetic needs
    /// three things — how much admitted content there is, where the view
    /// currently sits in it, and how tall the pane is — and all three are known
    /// only while rendering. Resolving at input time meant using the numbers
    /// the *previous* frame measured, so anything that changed the view between
    /// frames (a verbose toggle rebuilding the index, lines arriving, lines
    /// evicting, a resize) made one keypress land somewhere unrelated.
    pub(crate) pending_scroll: PendingScroll,
    /// Lines the reader asked for a blank row after, by pressing Enter at the
    /// tail — the terminal gesture for "start a fresh patch of screen".
    ///
    /// A mark, not a stored line. A blank pushed into the store had to be given
    /// an id, and the only one available was the id the *next* real line would
    /// arrive with, so the two collided: either the real line replaced the
    /// blank, or both sat under one id and the store's binary searches started
    /// answering with whichever came first.
    ///
    /// Counted, not a set: pressing Enter twice on a quiet stack means two
    /// blank rows, the same as it would in a shell.
    pub(crate) blank_after: HashMap<crate::output::LogId, u16>,
    /// Bumped whenever a mark is *added*, and the only thing the row index is
    /// keyed on. The key used to be a hash folded over the whole map, which
    /// cost a hasher per mark on every frame and — worse — meant dropping a
    /// mark for a line that had already been evicted looked like a change and
    /// forced a full rebuild. Adding a mark changes how tall a line is;
    /// forgetting one that no longer exists cannot change anything.
    blank_epoch: u64,
    /// Whether the pane is showing don's diagnostics rather than the processes'
    /// output. Two separate records with separate stores; this says which one
    /// is on screen.
    pub(crate) debug_view: bool,
    /// The other pane's state, waiting its turn. See [`StashedView`].
    pub(crate) stashed_view: StashedView,
    /// Rows above the top edge, as last drawn. For the scrollbar only —    /// Rows above the top edge, as last drawn. For the scrollbar only —
    /// scrolling must not read it, or it is back to deciding from stale
    /// geometry.
    pub(crate) log_rows_above: usize,
    pub(crate) log_total_rows: usize,
    pub(crate) log_pane_height: u16,
}

pub(crate) struct AppInit {
    pub(crate) service_names: Vec<String>,
    pub(crate) task_names: Vec<String>,
    pub(crate) build_tool_names: Vec<String>,
    pub(crate) task_configs: HashMap<String, Task>,
    pub(crate) task_last_runs: HashMap<String, TaskRunInfo>,
    pub(crate) hidden_names: HashSet<String>,
    pub(crate) auto_filter_on_failure_names: HashSet<String>,
    pub(crate) cli_log_filter: Option<HashSet<String>>,
}

impl App {
    pub(crate) fn new(init: AppInit) -> Self {
        let AppInit {
            service_names,
            task_names,
            build_tool_names,
            task_configs,
            task_last_runs,
            hidden_names,
            auto_filter_on_failure_names,
            cli_log_filter,
        } = init;
        let services_state: HashMap<String, ServiceState> = service_names
            .iter()
            .map(|n| (n.clone(), ServiceState::Pending))
            .collect();
        let service_pids: HashMap<String, Option<i32>> =
            service_names.iter().map(|n| (n.clone(), None)).collect();
        let tasks_state: HashMap<String, TaskState> = task_names
            .iter()
            .map(|n| (n.clone(), TaskState::Pending))
            .collect();

        let mut all_filter_names = service_names;
        all_filter_names.extend(task_names);
        // The synthetic build-tool stream ("bazel") emits under its
        // own prefix, not under a service/task name. Without a filter entry
        // they're silently gated out — the user sees nothing while bazel
        // crunches. Add them only when the config actually uses them, so
        // they don't show up as empty rows in unrelated projects.
        all_filter_names.extend(build_tool_names);
        // Expose `[don]` lifecycle events as their own filter entry so the
        // user can opt in/out explicitly, rather than having them always
        // bleed through an active filter.
        all_filter_names.push(LIFECYCLE_EVENT_NAME.to_string());

        let counts = StatusCounts::from_state(&services_state, &tasks_state);

        Self {
            exit_requested: false,
            bridge_request: None,
            attach_opened_automatically: false,
            attach_dismiss_requested: false,
            attach: None,
            counts,
            view_mode: ViewMode::Normal,
            shutdown_started: false,
            filter: FilterState::new(all_filter_names, &hidden_names, cli_log_filter.as_ref()),
            spinner_frame: 0,
            services_state,
            service_pids,
            tasks_state,
            failed_dependencies: HashMap::new(),
            tasks_last_run: task_last_runs,
            update_badge: None,
            services_table: StatusTableState::default(),
            tasks_table: StatusTableState::default(),
            failure_summary_scroll: 0,
            task_configs,
            auto_filter_on_failure_names,
            form: None,
            filter_narrowed_from: None,
            panes: super::panes::Panes::empty(),
            panel: super::panes::Panel::default(),
            panel_extent_customized: false,
            painted_layout: None,
            repaint_requested: false,
            focus: super::panes::Focus::Logs,
            dragging_divider: false,
            pending_g: false,
            services_order: Vec::new(),
            tasks_order: Vec::new(),
            view_index: super::view_index::ViewIndex::default(),
            log_scroll: super::logs::Scroll::Follow,
            log_selection: super::selection::Selection::default(),
            copy_notice: None,
            last_click: None,
            follow_paused_for_selection: false,
            log_visible_rows: Vec::new(),
            log_row_sources: Vec::new(),
            log_pane_origin: (0, 0),
            pending_scroll: PendingScroll::default(),
            blank_after: HashMap::new(),
            blank_epoch: 0,
            debug_view: false,
            stashed_view: StashedView::default(),
            log_rows_above: 0,
            log_total_rows: 0,
            log_pane_height: 0,
        }
    }

    pub(crate) fn begin_shutdown(&mut self) {
        self.shutdown_started = true;
        self.set_view_mode(ViewMode::Normal);
        self.services_table.reset();
        self.tasks_table.reset();
        self.failure_summary_scroll = 0;
        self.form = None;
    }

    pub(crate) fn set_update_check(
        &mut self,
        current_version: String,
        latest_version: Option<String>,
    ) {
        self.update_badge = latest_version.map(|latest_version| UpdateBadge {
            current_version,
            latest_version,
        });
    }

    /// A fingerprint of everything [`Self::should_render_log`] consults.
    ///
    /// Hashed rather than hand-incremented on each mutation: the filter has
    /// more mutators than anyone will remember to keep in step, and a missed
    /// bump would leave the pane indexing against a filter the user has
    /// already changed. Cost is the number of *process names*, not lines.
    /// Whether the side panel is on screen.
    ///
    /// Derived from the view mode rather than stored: what the panel shows and
    /// whether it is showing are one decision, and a second flag would let the
    /// two drift. Failures and the param form are full-screen overlays, not
    /// panels — they take the whole screen because they demand a decision,
    /// where a panel is for acting while still watching output.
    pub(crate) fn panel_open(&self) -> bool {
        matches!(
            self.view_mode,
            ViewMode::Services | ViewMode::Tasks | ViewMode::Filter
        )
    }

    /// Bring the other log pane to the front, putting this one away as it is.
    ///
    /// A swap, not a rebuild. Each pane keeps its own index and its own scroll
    /// position, so coming back to one lands where it was left and costs
    /// nothing — the index is the expensive thing, and throwing it away on
    /// every toggle is what made this flash.
    pub(crate) fn swap_log_view(&mut self) {
        std::mem::swap(&mut self.view_index, &mut self.stashed_view.index);
        std::mem::swap(&mut self.log_scroll, &mut self.stashed_view.scroll);
        std::mem::swap(&mut self.log_rows_above, &mut self.stashed_view.rows_above);
        std::mem::swap(&mut self.log_total_rows, &mut self.stashed_view.total_rows);
        std::mem::swap(&mut self.blank_after, &mut self.stashed_view.blank_after);
        std::mem::swap(&mut self.blank_epoch, &mut self.stashed_view.blank_epoch);
        // These describe the rows on screen, and the rows on screen are about
        // to be a different record's. Stale, they would send Enter's blank
        // mark to a line id belonging to the store that is no longer in front
        // — where it renders nothing, so Enter reads as dead.
        self.log_visible_rows.clear();
        self.log_row_sources.clear();
        // A selection is screen coordinates over content that is about to be
        // entirely different text.
        self.log_selection.clear();
        self.follow_paused_for_selection = false;
        self.pending_scroll = PendingScroll::default();
        self.debug_view = !self.debug_view;
    }

    pub(crate) fn log_filter_fingerprint(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.shutdown_started.hash(&mut hasher);
        self.filter.fingerprint(&mut hasher);
        // Blank marks change how tall a line is, so they belong to the same key
        // the row index is built against.
        self.blank_epoch.hash(&mut hasher);
        hasher.finish()
    }

    /// Ask for a blank row after `id` — what Enter at the tail does.
    pub(crate) fn mark_blank_after(&mut self, id: crate::output::LogId) {
        let count = self.blank_after.entry(id).or_insert(0);
        *count = count.saturating_add(1);
        self.blank_epoch = self.blank_epoch.wrapping_add(1);
    }

    /// Forget marks on lines the store no longer holds.
    ///
    /// Without this the map is a slow leak: one entry per line the reader ever
    /// pressed Enter on, kept for the life of the session, long after the line
    /// it describes has scrolled out of history. Deliberately does not bump the
    /// epoch — a mark on a line that is gone was already contributing nothing.
    pub(crate) fn prune_blank_marks(&mut self, oldest: Option<crate::output::LogId>) {
        if self.blank_after.is_empty() {
            return;
        }
        match oldest {
            Some(oldest) => self.blank_after.retain(|id, _| *id >= oldest),
            None => self.blank_after.clear(),
        }
    }

    /// Whether the pane shows this line.
    ///
    /// Verbose is not an admission question: it decided which *store* the line
    /// went into, and a store holds only its own kind. What is left is the
    /// name filter, and the shutdown override.
    /// Show only `name` in the log pane, remembering what to go back to.
    ///
    /// Returns whether anything changed, so the caller can leave the keypress
    /// alone when it did not.
    pub(crate) fn narrow_log_to(&mut self, name: &str) -> bool {
        let Some(previous) = self.filter.narrow_to(name) else {
            return false;
        };
        // Only the first narrowing records a restore point: `l` on one row
        // then another should still go back to what you had before either.
        self.filter_narrowed_from.get_or_insert(previous);
        true
    }

    /// Put back the selection `l` replaced, if it replaced one.
    pub(crate) fn widen_log_from_narrow(&mut self) -> bool {
        let Some(previous) = self.filter_narrowed_from.take() else {
            return false;
        };
        self.filter.restore(previous);
        true
    }

    pub(crate) fn should_render_log(&self, name: &str, _is_lifecycle: bool) -> bool {
        // During shutdown, every line bypasses the filter — the user wants
        // to see what's happening as each service tears down, including
        // service stdout from previously-hidden services (kafka, mongo, …).
        // The TUI render loop batches inserts and amortizes the bar redraw,
        // so a noisy service can't grind shutdown to a halt the way it
        // could before batching landed.
        if self.shutdown_started {
            return true;
        }
        self.filter.passes(name)
    }

    /// Sorted rows for the services table: errors → running → exited →
    /// lazy, alphabetical within a bucket. When its query is non-empty,
    /// rows are narrowed by fuzzy name-match before sorting.
    pub(crate) fn service_items(&self) -> Vec<OverlayItem> {
        let mut items: Vec<OverlayItem> = self
            .services_state
            .iter()
            .map(|(name, state)| OverlayItem {
                name: name.clone(),
                state: *state,
                pid: self.service_pids.get(name).copied().flatten(),
                failed_dependencies: self
                    .failed_dependencies
                    .get(name)
                    .cloned()
                    .unwrap_or_default(),
            })
            .collect();
        retain_fuzzy_matches(&self.services_table.query, &mut items, OverlayItem::name);
        let order = &self.services_order;
        items.sort_by(|a, b| {
            match (
                Self::ordered_position(order, a.name()),
                Self::ordered_position(order, b.name()),
            ) {
                (Some(x), Some(y)) => x.cmp(&y),
                // Anything the captured order does not know about goes after
                // everything it does, so a new arrival cannot displace a row.
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => a
                    .sort_bucket()
                    .cmp(&b.sort_bucket())
                    .then_with(|| a.name().cmp(b.name())),
            }
        });
        items
    }

    pub(crate) fn task_items(&self) -> Vec<TaskStatusItem> {
        let mut items: Vec<TaskStatusItem> = self
            .tasks_state
            .iter()
            .map(|(name, state)| TaskStatusItem {
                name: name.clone(),
                state: *state,
                failed_dependencies: self
                    .failed_dependencies
                    .get(name)
                    .cloned()
                    .unwrap_or_default(),
                last_run: self.tasks_last_run.get(name).cloned(),
                has_params: self
                    .task_configs
                    .get(name)
                    .is_some_and(|task| !task.params.is_empty()),
            })
            .collect();
        retain_fuzzy_matches(&self.tasks_table.query, &mut items, TaskStatusItem::name);
        let order = &self.tasks_order;
        items.sort_by(|a, b| {
            match (
                Self::ordered_position(order, a.name()),
                Self::ordered_position(order, b.name()),
            ) {
                (Some(x), Some(y)) => x.cmp(&y),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => a
                    .sort_bucket()
                    .cmp(&b.sort_bucket())
                    .then_with(|| a.name().cmp(b.name())),
            }
        });
        items
    }

    /// Where `name` sits in a captured order, or `None` if it arrived after
    /// the view opened. Newcomers sort after everything remembered, so a
    /// service that appears mid-session lands at the end instead of shuffling
    /// the rows above it.
    fn ordered_position(order: &[String], name: &str) -> Option<usize> {
        order.iter().position(|held| held == name)
    }

    /// Capture the order the tables should hold, from the state sort. Called
    /// when a table is opened.
    pub(crate) fn freeze_services_order(&mut self) {
        self.services_order.clear();
        self.services_order = self
            .service_items()
            .into_iter()
            .map(|item| item.name)
            .collect();
    }

    pub(crate) fn freeze_tasks_order(&mut self) {
        self.tasks_order.clear();
        self.tasks_order = self
            .task_items()
            .into_iter()
            .map(|item| item.name)
            .collect();
    }

    pub(crate) fn has_failure_summary(&self) -> bool {
        failure_summary::has_failures(&self.services_state, &self.tasks_state)
    }

    pub(crate) fn failure_summary_items(&self) -> Vec<FailureSummaryItem> {
        failure_summary::collect(
            &self.services_state,
            &self.tasks_state,
            &self.failed_dependencies,
        )
    }

    pub(crate) fn open_failure_summary(&mut self) {
        self.failure_summary_scroll = 0;
        self.set_view_mode(ViewMode::Failures);
    }

    /// Change which view is up.
    ///
    /// The one thing this does beyond the assignment is drop the log popup,
    /// and it is why the assignment is a method at all. That popup is a detail
    /// view of one *row* — opened with `l` from the services or tasks table —
    /// so it has no meaning once that table is gone. Leaving it behind
    /// stranded it: `s` and `t` close their panel from anywhere, and the only
    /// key that dismissed the popup lived inside the table handlers, so a
    /// popup that outlived its table could not be dismissed at all.
    pub(crate) fn set_view_mode(&mut self, mode: ViewMode) {
        self.view_mode = mode;
    }

    pub(crate) fn scroll_failure_summary_by(&mut self, delta: isize) {
        if delta < 0 {
            self.failure_summary_scroll = self
                .failure_summary_scroll
                .saturating_sub(delta.unsigned_abs());
        } else {
            self.failure_summary_scroll = self
                .failure_summary_scroll
                .saturating_add(delta.unsigned_abs());
        }
    }

    pub(crate) fn scroll_failure_summary_to_top(&mut self) {
        self.failure_summary_scroll = 0;
    }

    pub(crate) fn scroll_failure_summary_to_bottom(&mut self) {
        self.failure_summary_scroll = usize::MAX;
    }

    pub(crate) fn sync_failure_summary_scroll(&mut self, max_scroll: usize) {
        self.failure_summary_scroll = self.failure_summary_scroll.min(max_scroll);
    }

    /// Apply a runner-emitted state change. Returns `true` when counts
    /// changed (so the main loop can limit redraws to interesting events).
    pub(crate) fn apply_service_runtime(
        &mut self,
        name: String,
        state: ServiceState,
        pid: Option<i32>,
        failed_dependencies: Vec<String>,
    ) -> bool {
        let filter_changed = state == ServiceState::Failed
            && self.auto_filter_on_failure_names.contains(&name)
            && self.filter.select_name(&name);
        self.services_state.insert(name.clone(), state);
        self.service_pids.insert(name.clone(), pid);
        // A service that stopped, failed or is restarting has taken its
        // terminal with it, whatever the socket has noticed.
        if !matches!(
            state,
            ServiceState::Starting
                | ServiceState::Running
                | ServiceState::Ready
                | ServiceState::Unhealthy
        ) {
            self.note_attached_process_gone(&name);
        }
        self.apply_failed_dependencies(name, failed_dependencies);
        self.counts = StatusCounts::from_state(&self.services_state, &self.tasks_state);
        filter_changed
    }

    /// The process an attach window is showing has finished.
    ///
    /// don's own record is what settles this, not the connection. A task's
    /// attach socket stays open past its exit — the sink it feeds outlives the
    /// run — so waiting for the stream to close meant a finished task sat
    /// there looking live, which is precisely the question someone attached to
    /// a task is asking.
    ///
    /// The session is left running. Anything still in flight keeps landing in
    /// the grid, so marking the window ended can never clip the last thing the
    /// process wrote; the connection is closed when the reader dismisses it.
    /// A task that declared it wants a human has started running.
    ///
    /// `interactive = true` — and the `terminal = "foreground"` it grew out of
    /// — says the task will sit there waiting for input. don used to answer
    /// that by printing "run `don attach x`" and leaving the reader to do it,
    /// which is a strange thing to ask of someone watching the screen the task
    /// is already on. The window opens itself instead.
    ///
    /// Only on the transition into `Running`, and only when nothing else is
    /// attached: an attach the reader asked for outranks one nobody did, and
    /// a window they detached from must not spring back.
    fn note_interactive_task_started(&mut self, name: &str) {
        if self.attach.is_some() || self.bridge_request.is_some() {
            return;
        }
        if !self
            .task_configs
            .get(name)
            .is_some_and(|task| task.interactive)
        {
            return;
        }
        self.bridge_request = Some(name.to_string());
        self.attach_opened_automatically = true;
    }

    /// A task whose window opened itself has stopped running.
    ///
    /// Success takes the window with it: it opened to let the reader answer a
    /// prompt, the prompt is answered, and making them dismiss it would be one
    /// more keypress for something they already watched happen. Failure keeps
    /// it, because the last screen a failed task drew is the reason it failed.
    fn note_auto_attached_task_finished(&mut self, name: &str, state: TaskState) {
        if !self.attach_opened_automatically || state == TaskState::Failed {
            return;
        }
        if self.attach.as_ref().is_some_and(|view| view.name == name) {
            self.attach_dismiss_requested = true;
        }
    }

    fn note_attached_process_gone(&mut self, name: &str) {
        if let Some(view) = self.attach.as_mut()
            && view.name == name
        {
            view.ended = true;
        }
    }

    pub(crate) fn apply_task_state(
        &mut self,
        name: String,
        state: TaskState,
        last_run: Option<TaskRunInfo>,
        failed_dependencies: Vec<String>,
    ) -> bool {
        let filter_changed = state == TaskState::Failed
            && self.auto_filter_on_failure_names.contains(&name)
            && self.filter.select_name(&name);
        let was = self.tasks_state.insert(name.clone(), state);
        if state == TaskState::Running && was != Some(TaskState::Running) {
            self.note_interactive_task_started(&name);
        }
        if !matches!(state, TaskState::Running | TaskState::Building) {
            self.note_attached_process_gone(&name);
            self.note_auto_attached_task_finished(&name, state);
        }
        self.apply_failed_dependencies(name.clone(), failed_dependencies);
        if let Some(last_run) = last_run {
            self.tasks_last_run.insert(name, last_run);
        }
        self.counts = StatusCounts::from_state(&self.services_state, &self.tasks_state);
        filter_changed
    }

    /// Reload every item's state from the runner's projection.
    ///
    /// The TUI applies transitions incrementally from the event broadcast,
    /// which is correct right up until that broadcast *lags*. A dropped event
    /// leaves this view silently wrong about the item it described, and it
    /// stays wrong until that item happens to move again — a service can sit
    /// on the status bar as `starting` for the rest of the session. Reloading
    /// from the projection turns that unrecoverable drift into a missed frame.
    ///
    /// Deliberately not a replacement for event handling: the auto-filter on
    /// failure is edge-triggered, and re-firing it here for every service that
    /// is already failed would yank the user's filter out from under them.
    pub(crate) fn resync_from(&mut self, snapshot: &crate::client::StateSnapshot) {
        for status in &snapshot.processes {
            match status {
                crate::client::ProcessStatus::Service {
                    name,
                    state,
                    failed_dependencies,
                    runtime,
                    ..
                } => {
                    // The snapshot is the record for runtime detail, so take
                    // the pid from it rather than keeping whatever we last
                    // saw on an event. This is the *only* way a client that
                    // attached after startup learns a pid at all: state
                    // events fire on transitions, and by the time a `don
                    // start` TUI subscribes the spawns have already happened.
                    self.service_pids
                        .insert(name.clone(), runtime.as_ref().and_then(|rt| rt.pid));
                    self.services_state.insert(name.clone(), *state);
                    self.apply_failed_dependencies(name.clone(), failed_dependencies.clone());
                }
                crate::client::ProcessStatus::Task {
                    name,
                    state,
                    failed_dependencies,
                    last_run,
                    ..
                } => {
                    self.tasks_state.insert(name.clone(), *state);
                    self.apply_failed_dependencies(name.clone(), failed_dependencies.clone());
                    if let Some(last_run) = last_run {
                        self.tasks_last_run.insert(name.clone(), last_run.clone());
                    }
                }
            }
        }
        self.counts = StatusCounts::from_state(&self.services_state, &self.tasks_state);
    }

    fn apply_failed_dependencies(&mut self, name: String, dependencies: Vec<String>) {
        if dependencies.is_empty() {
            self.failed_dependencies.remove(&name);
        } else {
            self.failed_dependencies.insert(name, dependencies);
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn services(entries: &[(&str, ServiceState)]) -> HashMap<String, ServiceState> {
        entries.iter().map(|(n, s)| (n.to_string(), *s)).collect()
    }

    fn tasks(entries: &[(&str, TaskState)]) -> HashMap<String, TaskState> {
        entries.iter().map(|(n, s)| (n.to_string(), *s)).collect()
    }

    fn app_with_names(service_names: Vec<String>, task_names: Vec<String>) -> App {
        app_with_names_and_auto_filter(service_names, task_names, HashSet::new())
    }

    fn app_with_names_and_auto_filter(
        service_names: Vec<String>,
        task_names: Vec<String>,
        auto_filter_on_failure_names: HashSet<String>,
    ) -> App {
        App::new(AppInit {
            service_names,
            task_names,
            build_tool_names: vec![],
            task_configs: HashMap::new(),
            task_last_runs: HashMap::new(),
            hidden_names: HashSet::new(),
            auto_filter_on_failure_names,
            cli_log_filter: None,
        })
    }

    fn apply_service(app: &mut App, name: &str, state: ServiceState, pid: Option<i32>) -> bool {
        app.apply_service_runtime(name.to_string(), state, pid, Vec::new())
    }

    fn apply_task(app: &mut App, name: &str, state: TaskState) -> bool {
        app.apply_task_state(name.to_string(), state, None, Vec::new())
    }

    #[test]
    fn resync_from_replaces_drifted_state() {
        use crate::client::{ProcessStatus, StateSnapshot};

        fn snapshot_service(name: &str, state: ServiceState, pid: Option<i32>) -> ProcessStatus {
            ProcessStatus::Service {
                runtime: pid.map(|pid| crate::client::ServiceRuntime {
                    pid: Some(pid),
                    ..Default::default()
                }),
                name: name.to_string(),
                state,
                failed_dependencies: Vec::new(),
                verbose: None,
            }
        }

        struct Case {
            name: &'static str,
            /// State the app believes, applied from events before the lag.
            before: Vec<(&'static str, ServiceState, Option<i32>)>,
            snapshot: Vec<ProcessStatus>,
            want_states: Vec<(&'static str, ServiceState)>,
            want_pids: Vec<(&'static str, Option<i32>)>,
            want_counts_ready: usize,
        }

        let cases = vec![
            Case {
                name: "a dropped transition is picked up",
                before: vec![("api", ServiceState::Starting, Some(42))],
                snapshot: vec![snapshot_service("api", ServiceState::Ready, Some(42))],
                want_states: vec![("api", ServiceState::Ready)],
                want_pids: vec![("api", Some(42))],
                want_counts_ready: 1,
            },
            Case {
                // The bug this path exists for: a client that subscribes
                // after the spawns have happened sees no state transitions,
                // so the snapshot is the only place a pid can come from.
                name: "a pid we never saw an event for arrives with the snapshot",
                before: vec![("api", ServiceState::Ready, None)],
                snapshot: vec![snapshot_service("api", ServiceState::Ready, Some(42))],
                want_states: vec![("api", ServiceState::Ready)],
                want_pids: vec![("api", Some(42))],
                want_counts_ready: 1,
            },
            Case {
                name: "the snapshot's pid replaces a stale one",
                before: vec![("api", ServiceState::Ready, Some(42))],
                snapshot: vec![snapshot_service("api", ServiceState::Ready, Some(99))],
                want_states: vec![("api", ServiceState::Ready)],
                want_pids: vec![("api", Some(99))],
                want_counts_ready: 1,
            },
            Case {
                // No runtime means no local process — a docker service, or one
                // that has stopped. Either way the pid we hold is a corpse.
                name: "no runtime in the snapshot clears the pid",
                before: vec![("api", ServiceState::Ready, Some(42))],
                snapshot: vec![snapshot_service("api", ServiceState::Ready, None)],
                want_states: vec![("api", ServiceState::Ready)],
                want_pids: vec![("api", None)],
                want_counts_ready: 1,
            },
            Case {
                name: "several items resync independently",
                before: vec![
                    ("api", ServiceState::Ready, Some(1)),
                    ("web", ServiceState::Starting, Some(2)),
                ],
                snapshot: vec![
                    snapshot_service("api", ServiceState::Ready, Some(1)),
                    snapshot_service("web", ServiceState::Failed, None),
                ],
                want_states: vec![("api", ServiceState::Ready), ("web", ServiceState::Failed)],
                want_pids: vec![("api", Some(1)), ("web", None)],
                want_counts_ready: 1,
            },
        ];

        for case in cases {
            let names: Vec<String> = case.before.iter().map(|(n, ..)| n.to_string()).collect();
            let mut app = app_with_names(names, vec![]);
            for (name, state, pid) in &case.before {
                apply_service(&mut app, name, *state, *pid);
            }

            app.resync_from(&StateSnapshot {
                processes: case.snapshot,
                startup_complete: true,
            });

            for (name, want) in case.want_states {
                assert_eq!(
                    app.services_state.get(name),
                    Some(&want),
                    "{}: state of {name}",
                    case.name
                );
            }
            for (name, want) in case.want_pids {
                assert_eq!(
                    app.service_pids.get(name).copied().flatten(),
                    want,
                    "{}: pid of {name}",
                    case.name
                );
            }
            assert_eq!(
                app.counts.services_ready, case.want_counts_ready,
                "{}: counts recomputed",
                case.name
            );
        }
    }

    #[test]
    fn resync_does_not_fire_the_auto_filter_on_already_failed_items() {
        use crate::client::{ProcessStatus, StateSnapshot};

        // Auto-filter-on-failure is edge-triggered. Resyncing is not an edge:
        // re-selecting every already-failed service would yank the user's
        // filter out from under them on every broadcast lag.
        let mut app = app_with_names_and_auto_filter(
            vec!["api".to_string()],
            vec![],
            HashSet::from(["api".to_string()]),
        );
        let before = app.filter.clone();

        app.resync_from(&StateSnapshot {
            processes: vec![ProcessStatus::Service {
                runtime: None,
                name: "api".to_string(),
                state: ServiceState::Failed,
                failed_dependencies: Vec::new(),
                verbose: None,
            }],
            startup_complete: true,
        });

        assert_eq!(app.services_state.get("api"), Some(&ServiceState::Failed));
        assert_eq!(
            format!("{:?}", app.filter),
            format!("{before:?}"),
            "resync must not touch the filter"
        );
    }

    #[test]
    fn from_state_counts_ready_failed_and_pending_run() {
        struct Case {
            name: &'static str,
            services: Vec<(&'static str, ServiceState)>,
            tasks: Vec<(&'static str, TaskState)>,
            want: StatusCounts,
        }

        let cases = vec![
            Case {
                name: "empty",
                services: vec![],
                tasks: vec![],
                want: StatusCounts::default(),
            },
            Case {
                name: "all services ready, no tasks",
                services: vec![
                    ("api", ServiceState::Ready),
                    ("worker", ServiceState::Ready),
                ],
                tasks: vec![],
                want: StatusCounts {
                    services_total: 2,
                    services_ready: 2,
                    services_failed: 0,
                    services_unhealthy: 0,
                    services_active: 0,
                    tasks_running: 0,
                    tasks_pending_run: 0,
                    tasks_failed: 0,
                },
            },
            Case {
                name: "mixed states — lazy excluded from total",
                services: vec![
                    ("api", ServiceState::Ready),
                    ("db", ServiceState::Failed),
                    ("queue", ServiceState::Starting),
                    ("cache", ServiceState::Lazy),
                ],
                tasks: vec![
                    ("migrate", TaskState::PendingRun),
                    ("seed", TaskState::Completed),
                    ("backup", TaskState::PendingRun),
                    ("build", TaskState::Running),
                    ("lint", TaskState::Failed),
                ],
                want: StatusCounts {
                    services_total: 3, // cache (Lazy) doesn't count
                    services_ready: 1,
                    services_failed: 1,
                    services_unhealthy: 0,
                    services_active: 1, // queue (Starting)
                    tasks_running: 1,   // build
                    tasks_pending_run: 2,
                    tasks_failed: 1,
                },
            },
            Case {
                name: "running service counts as active",
                services: vec![("svc", ServiceState::Running)],
                tasks: vec![],
                want: StatusCounts {
                    services_total: 1,
                    services_ready: 0,
                    services_failed: 0,
                    services_unhealthy: 0,
                    services_active: 1,
                    tasks_running: 0,
                    tasks_pending_run: 0,
                    tasks_failed: 0,
                },
            },
        ];

        for case in cases {
            let got = StatusCounts::from_state(&services(&case.services), &tasks(&case.tasks));
            assert_eq!(got, case.want, "case: {}", case.name);
        }
    }

    #[test]
    fn is_working_table() {
        struct Case {
            name: &'static str,
            counts: StatusCounts,
            want: bool,
        }
        let cases = vec![
            Case {
                name: "all idle",
                counts: StatusCounts::default(),
                want: false,
            },
            Case {
                name: "service transitioning",
                counts: StatusCounts {
                    services_active: 1,
                    ..Default::default()
                },
                want: true,
            },
            Case {
                name: "task running",
                counts: StatusCounts {
                    tasks_running: 1,
                    ..Default::default()
                },
                want: true,
            },
            Case {
                name: "pending-run tasks don't count — waiting on user",
                counts: StatusCounts {
                    tasks_pending_run: 3,
                    ..Default::default()
                },
                want: false,
            },
            Case {
                name: "failed service doesn't count",
                counts: StatusCounts {
                    services_total: 1,
                    services_failed: 1,
                    ..Default::default()
                },
                want: false,
            },
        ];
        for case in cases {
            assert_eq!(case.counts.is_working(), case.want, "case: {}", case.name);
        }
    }

    #[test]
    fn apply_state_refreshes_counts() {
        let mut app = app_with_names(vec!["api".into(), "db".into()], vec![]);
        assert_eq!(app.counts.services_ready, 0);
        apply_service(&mut app, "api", ServiceState::Ready, None);
        assert_eq!(app.counts.services_ready, 1);
        apply_service(&mut app, "db", ServiceState::Ready, None);
        assert_eq!(app.counts.services_ready, 2);
        apply_service(&mut app, "db", ServiceState::Stopping, None);
        assert_eq!(app.counts.services_ready, 1);
        assert_eq!(app.counts.services_active, 1);
        apply_service(&mut app, "api", ServiceState::Failed, None);
        assert_eq!(app.counts.services_ready, 0);
        assert_eq!(app.counts.services_failed, 1);
    }

    #[test]
    fn failed_service_is_added_to_log_filter_when_configured() {
        let mut app = app_with_names_and_auto_filter(
            vec!["api".into(), "db".into()],
            vec![],
            HashSet::from(["db".to_string()]),
        );
        app.filter.enter_edit();
        app.filter.select_only_highlighted(); // [all] row keeps everything selected.
        app.filter.toggle_highlighted(); // clear all

        assert!(!app.should_render_log("db", false));
        let changed = apply_service(&mut app, "db", ServiceState::Failed, None);

        assert!(changed);
        assert!(app.should_render_log("db", false));
        assert!(!app.should_render_log("api", false));
    }

    #[test]
    fn dependency_failed_service_does_not_auto_filter() {
        let mut app = app_with_names_and_auto_filter(
            vec!["api".into()],
            vec![],
            HashSet::from(["api".to_string()]),
        );
        app.filter.enter_edit();
        app.filter.toggle_highlighted(); // clear all

        let changed = apply_service(&mut app, "api", ServiceState::DependencyFailed, None);

        assert!(!changed);
        assert!(!app.should_render_log("api", false));
    }

    #[test]
    fn failed_task_is_added_to_log_filter_when_configured() {
        let mut app = app_with_names_and_auto_filter(
            vec![],
            vec!["build".into(), "lint".into()],
            HashSet::from(["lint".to_string()]),
        );
        app.filter.enter_edit();
        app.filter.toggle_highlighted(); // clear all

        let changed = apply_task(&mut app, "lint", TaskState::Failed);

        assert!(changed);
        assert!(app.should_render_log("lint", false));
        assert!(!app.should_render_log("build", false));
    }

    #[test]
    fn service_items_prioritize_service_states_and_exclude_tasks() {
        struct Case {
            name: &'static str,
            services: Vec<(&'static str, ServiceState)>,
            tasks: Vec<(&'static str, TaskState)>,
            want: Vec<&'static str>,
        }

        let cases = vec![Case {
            name: "mixed services and tasks",
            services: vec![
                ("svc-ready", ServiceState::Ready),
                ("svc-building", ServiceState::Building),
                ("svc-running", ServiceState::Running),
                ("svc-stopped", ServiceState::Stopped),
                ("svc-lazy", ServiceState::Lazy),
                ("svc-failed", ServiceState::Failed),
                ("svc-dep", ServiceState::DependencyFailed),
                ("svc-stopping", ServiceState::Stopping),
            ],
            tasks: vec![
                ("task-skipped", TaskState::Skipped),
                ("task-completed", TaskState::Completed),
                ("task-building", TaskState::Building),
                ("task-pending-run", TaskState::PendingRun),
                ("task-failed", TaskState::Failed),
                ("task-dep", TaskState::DependencyFailed),
            ],
            want: vec![
                "svc-failed",
                "svc-dep",
                "svc-building",
                "svc-running",
                "svc-ready",
                "svc-stopping",
                "svc-stopped",
                "svc-lazy",
            ],
        }];

        for case in cases {
            let service_names = case
                .services
                .iter()
                .map(|(name, _)| (*name).to_string())
                .collect();
            let task_names = case
                .tasks
                .iter()
                .map(|(name, _)| (*name).to_string())
                .collect();
            let mut app = app_with_names(service_names, task_names);
            for (name, state) in case.services {
                apply_service(&mut app, name, state, None);
            }
            for (name, state) in case.tasks {
                apply_task(&mut app, name, state);
            }

            let items = app.service_items();
            let got: Vec<&str> = items.iter().map(OverlayItem::name).collect();
            assert_eq!(got, case.want, "case: {}", case.name);
        }
    }

    #[test]
    fn service_items_include_service_pid() {
        let mut app = app_with_names(vec!["api".into()], vec![]);

        apply_service(&mut app, "api", ServiceState::Running, Some(12_345));

        let items = app.service_items();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].name(), "api");
        assert_eq!(items[0].pid, Some(12_345));
    }

    #[test]
    fn shutdown_mode_bypasses_filter_for_all_logs() {
        let mut app = app_with_names(vec!["api".into(), "worker".into()], vec![]);
        app.filter.enter_edit();
        app.filter.push_query_char('a');
        app.filter.select_only_highlighted();

        // Without shutdown: filter passes "api", rejects "worker" — for both
        // service stdout (is_lifecycle=false) and lifecycle events
        // (is_lifecycle=true).
        assert!(app.should_render_log("api", false));
        assert!(app.should_render_log("api", true));
        assert!(!app.should_render_log("worker", false));
        assert!(!app.should_render_log("worker", true));

        app.begin_shutdown();

        // After shutdown: every line passes regardless of filter — the user
        // wants visibility into everything happening as services tear down.
        assert!(app.should_render_log("api", false));
        assert!(app.should_render_log("api", true));
        assert!(app.should_render_log("worker", false));
        assert!(app.should_render_log("worker", true));
    }

    #[test]
    fn begin_shutdown_returns_to_normal_view() {
        let mut app = app_with_names(vec!["api".into()], vec![]);
        app.view_mode = ViewMode::Services;
        app.services_table.query = "api".into();
        app.services_table.filtering = true;

        app.begin_shutdown();

        assert!(app.shutdown_started);
        assert_eq!(app.view_mode, ViewMode::Normal);
        assert!(app.services_table.query.is_empty());
        assert!(!app.services_table.filtering);
    }

    #[test]
    fn task_items_prioritize_actionable_states_and_include_metadata() {
        let mut app = app_with_names(
            vec![],
            vec![
                "completed".into(),
                "failed".into(),
                "pending-run".into(),
                "running".into(),
            ],
        );
        apply_task(&mut app, "completed", TaskState::Completed);
        apply_task(&mut app, "failed", TaskState::Failed);
        apply_task(&mut app, "pending-run", TaskState::PendingRun);
        apply_task(&mut app, "running", TaskState::Running);
        app.tasks_last_run.insert(
            "completed".into(),
            TaskRunInfo {
                finished_at_unix_secs: 1,
                duration_ms: Some(42),
                success: true,
                exit_code: Some(0),
                message: None,
            },
        );

        let items = app.task_items();
        let got: Vec<&str> = items.iter().map(TaskStatusItem::name).collect();

        assert_eq!(got, vec!["failed", "pending-run", "running", "completed"]);
        assert_eq!(items[3].last_run.as_ref().unwrap().duration_ms, Some(42));
    }

    /// The tables sort by state so failures surface when the view opens — and
    /// then must hold still, because acting on a row changes its state and
    /// would otherwise move it out from under the cursor that acted.
    #[test]
    fn a_table_holds_the_order_it_opened_with() {
        let mut app = App::new(AppInit {
            service_names: vec!["alpha".into(), "beta".into(), "gamma".into()],
            task_names: Vec::new(),
            build_tool_names: Vec::new(),
            task_configs: HashMap::new(),
            task_last_runs: HashMap::new(),
            hidden_names: HashSet::new(),
            auto_filter_on_failure_names: HashSet::new(),
            cli_log_filter: None,
        });
        // beta is broken, so it opens at the top.
        apply_service(&mut app, "alpha", ServiceState::Ready, Some(1));
        apply_service(&mut app, "beta", ServiceState::Failed, None);
        apply_service(&mut app, "gamma", ServiceState::Ready, Some(3));

        let opened: Vec<String> = app
            .service_items()
            .into_iter()
            .map(|item| item.name)
            .collect();
        assert_eq!(opened, vec!["beta", "alpha", "gamma"], "state sort on open");

        app.freeze_services_order();

        // Now act on things: beta recovers, alpha stops. Neither may move.
        apply_service(&mut app, "beta", ServiceState::Ready, Some(2));
        apply_service(&mut app, "alpha", ServiceState::Stopped, None);
        let after: Vec<String> = app
            .service_items()
            .into_iter()
            .map(|item| item.name)
            .collect();
        assert_eq!(after, opened, "rows hold still once the view is open");

        // A service that appears later joins at the end rather than displacing
        // anything above it.
        apply_service(&mut app, "delta", ServiceState::Failed, None);
        let with_newcomer: Vec<String> = app
            .service_items()
            .into_iter()
            .map(|item| item.name)
            .collect();
        assert_eq!(with_newcomer, vec!["beta", "alpha", "gamma", "delta"]);

        // Reopening re-sorts: alpha is stopped, delta failed.
        app.freeze_services_order();
        let reopened: Vec<String> = app
            .service_items()
            .into_iter()
            .map(|item| item.name)
            .collect();
        assert_eq!(reopened[0], "delta", "reopening surfaces the failure again");
    }
}
