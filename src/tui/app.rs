//! TUI application state — the single source of truth for what to render.
//!
//! Derived from runner events (status counts, service/task state for the
//! status tables) and from user input (view mode, filter, tables). Kept
//! deliberately small so rendering is a pure function of this struct plus
//! the terminal size.
//!
//! The main TUI loop is the only mutator — there's no shared `Arc<Mutex<_>>`.

use std::collections::{HashMap, HashSet};

use super::filter::FilterState;
use super::form::FormState;
use super::status_table::{StatusTableState, retain_fuzzy_matches};
use crate::config::Task;
use crate::output::{FormattedLogLine, LIFECYCLE_EVENT_NAME};
use crate::runner::{ServiceState, TaskItemState};
use crate::task_state::TaskRunInfo;

const LOG_POPUP_MAX_LINES: usize = 500;
const LOG_POPUP_DEFAULT_VISIBLE_LINES: usize = 30;

/// Top-level view mode. Determines how keys are interpreted and how the
/// inline viewport is laid out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum ViewMode {
    /// Log flow + status bar. Keys trigger mode changes or scrollback actions.
    #[default]
    Normal,
    /// Log-filter modal. Navigation edits the pending selection; `/` enters
    /// query input, Enter commits, Esc cancels.
    Filter,
    /// Full-screen tasks table. Arrow keys move a highlight; Enter runs the
    /// selected task or opens its param form.
    Tasks,
    /// Full-screen services table (alternate screen). Arrow keys move a
    /// highlight; Enter toggles start/stop on the selected service, `r`
    /// restarts it, `R` hard-restarts it, `Esc` dismisses.
    Services,
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
}

/// A row in the tasks status table.
#[derive(Debug, Clone)]
pub(crate) struct TaskStatusItem {
    pub(crate) name: String,
    pub(crate) state: TaskItemState,
    pub(crate) last_run: Option<TaskRunInfo>,
    pub(crate) has_params: bool,
}

/// In-table popup showing recent logs for the highlighted service/task.
#[derive(Debug, Clone)]
pub(crate) struct LogPopup {
    pub(crate) name: String,
    pub(crate) lines: Vec<Vec<u8>>,
    pub(crate) scroll: usize,
    pub(crate) follow_tail: bool,
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
        !matches!(self.state, TaskItemState::Running | TaskItemState::Building)
    }

    fn sort_bucket(&self) -> u8 {
        match self.state {
            TaskItemState::Failed => 0,
            TaskItemState::DependencyFailed => 1,
            TaskItemState::PendingRun => 2,
            TaskItemState::Running | TaskItemState::Building => 3,
            TaskItemState::Pending => 4,
            TaskItemState::Completed => 5,
            TaskItemState::Skipped => 6,
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
        tasks: &HashMap<String, TaskItemState>,
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
                TaskItemState::PendingRun => counts.tasks_pending_run += 1,
                TaskItemState::Running => counts.tasks_running += 1,
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
    pub(crate) counts: StatusCounts,
    pub(crate) view_mode: ViewMode,
    pub(crate) verbose_enabled: bool,
    /// Set when the user presses Ctrl+C in detach mode (`don tui` frontend).
    /// The event loop checks it after each key and breaks, returning from
    /// `run_tui` without shutting the daemon down.
    pub(crate) should_detach: bool,
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
    pub(crate) tasks_state: HashMap<String, TaskItemState>,
    pub(crate) tasks_last_run: HashMap<String, TaskRunInfo>,
    pub(crate) update_badge: Option<UpdateBadge>,
    pub(crate) services_table: StatusTableState,
    pub(crate) tasks_table: StatusTableState,
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
    pub(crate) log_popup: Option<LogPopup>,
    /// Set by the task/form handlers when a foreground task is launched from
    /// the `don tui` frontend. The event loop consumes it: tears the dashboard
    /// down, bridges stdin/stdout to the daemon PTY, and rebuilds on exit.
    pub(crate) pending_foreground_run: Option<ForegroundRun>,
    /// Terminal height the inline bar was last drawn at. The resize handler
    /// compares against this to decide whether a resize changed the height
    /// (and thus moved the bar, requiring a screen clear to erase the ghost)
    /// or only the width (where the reflowed logs can stay on screen).
    pub(crate) last_screen_height: u16,
}

/// A foreground task launch the `don tui` frontend defers to the event loop,
/// which owns the terminal teardown/rebuild the interactive bridge needs.
#[derive(Debug)]
pub(crate) struct ForegroundRun {
    pub(crate) name: String,
    pub(crate) params: HashMap<String, String>,
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
    pub(crate) verbose_enabled: bool,
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
            verbose_enabled,
        } = init;
        let services_state: HashMap<String, ServiceState> = service_names
            .iter()
            .map(|n| (n.clone(), ServiceState::Pending))
            .collect();
        let service_pids: HashMap<String, Option<i32>> =
            service_names.iter().map(|n| (n.clone(), None)).collect();
        let tasks_state: HashMap<String, TaskItemState> = task_names
            .iter()
            .map(|n| (n.clone(), TaskItemState::Pending))
            .collect();

        let mut all_filter_names = service_names;
        all_filter_names.extend(task_names);
        // Synthetic build-tool streams ("bazel", "turbo") emit under their
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
            counts,
            view_mode: ViewMode::Normal,
            verbose_enabled,
            should_detach: false,
            shutdown_started: false,
            filter: FilterState::new(all_filter_names, &hidden_names, cli_log_filter.as_ref()),
            spinner_frame: 0,
            services_state,
            service_pids,
            tasks_state,
            tasks_last_run: task_last_runs,
            update_badge: None,
            services_table: StatusTableState::default(),
            tasks_table: StatusTableState::default(),
            task_configs,
            auto_filter_on_failure_names,
            form: None,
            log_popup: None,
            pending_foreground_run: None,
            last_screen_height: 0,
        }
    }

    pub(crate) fn begin_shutdown(&mut self) {
        self.shutdown_started = true;
        self.view_mode = ViewMode::Normal;
        self.services_table.reset();
        self.tasks_table.reset();
        self.form = None;
        self.log_popup = None;
    }

    pub(crate) fn set_verbose_enabled(&mut self, verbose_enabled: bool) {
        self.verbose_enabled = verbose_enabled;
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
            })
            .collect();
        retain_fuzzy_matches(&self.services_table.query, &mut items, OverlayItem::name);
        items.sort_by(|a, b| {
            a.sort_bucket()
                .cmp(&b.sort_bucket())
                .then_with(|| a.name().cmp(b.name()))
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
                last_run: self.tasks_last_run.get(name).cloned(),
                has_params: self
                    .task_configs
                    .get(name)
                    .is_some_and(|task| !task.params.is_empty()),
            })
            .collect();
        retain_fuzzy_matches(&self.tasks_table.query, &mut items, TaskStatusItem::name);
        items.sort_by(|a, b| {
            a.sort_bucket()
                .cmp(&b.sort_bucket())
                .then_with(|| a.name().cmp(b.name()))
        });
        items
    }

    /// Apply a runner-emitted state change. Returns `true` when counts
    /// changed (so the main loop can limit redraws to interesting events).
    pub(crate) fn apply_service_runtime(
        &mut self,
        name: String,
        state: ServiceState,
        pid: Option<i32>,
    ) -> bool {
        let filter_changed = state == ServiceState::Failed
            && self.auto_filter_on_failure_names.contains(&name)
            && self.filter.select_name(&name);
        self.services_state.insert(name.clone(), state);
        self.service_pids.insert(name, pid);
        self.counts = StatusCounts::from_state(&self.services_state, &self.tasks_state);
        filter_changed
    }

    pub(crate) fn apply_task_state(
        &mut self,
        name: String,
        state: TaskItemState,
        last_run: Option<TaskRunInfo>,
    ) -> bool {
        let filter_changed = state == TaskItemState::Failed
            && self.auto_filter_on_failure_names.contains(&name)
            && self.filter.select_name(&name);
        self.tasks_state.insert(name.clone(), state);
        if let Some(last_run) = last_run {
            self.tasks_last_run.insert(name, last_run);
        }
        self.counts = StatusCounts::from_state(&self.services_state, &self.tasks_state);
        filter_changed
    }

    pub(crate) fn open_log_popup(&mut self, name: String, mut lines: Vec<Vec<u8>>) {
        if lines.len() > LOG_POPUP_MAX_LINES {
            lines.drain(0..lines.len() - LOG_POPUP_MAX_LINES);
        }
        let scroll = lines.len().saturating_sub(LOG_POPUP_DEFAULT_VISIBLE_LINES);
        self.log_popup = Some(LogPopup {
            name,
            lines,
            scroll,
            follow_tail: true,
        });
    }

    pub(crate) fn close_log_popup(&mut self) {
        self.log_popup = None;
    }

    pub(crate) fn append_log_popup_line(&mut self, line: &FormattedLogLine) -> bool {
        let Some(popup) = self.log_popup.as_mut() else {
            return false;
        };
        if !line_matches_log_popup(&popup.name, line) {
            return false;
        }
        popup.lines.push(line.bytes.clone());
        if popup.lines.len() > LOG_POPUP_MAX_LINES {
            popup.lines.remove(0);
            if !popup.follow_tail {
                popup.scroll = popup.scroll.saturating_sub(1);
            }
        }
        if popup.follow_tail {
            popup.scroll = popup
                .lines
                .len()
                .saturating_sub(LOG_POPUP_DEFAULT_VISIBLE_LINES);
        }
        true
    }

    pub(crate) fn scroll_log_popup_by(&mut self, delta: isize) {
        let Some(popup) = self.log_popup.as_mut() else {
            return;
        };
        popup.follow_tail = false;
        if delta < 0 {
            popup.scroll = popup.scroll.saturating_sub(delta.unsigned_abs());
        } else {
            popup.scroll = popup
                .scroll
                .saturating_add(delta as usize)
                .min(popup.lines.len().saturating_sub(1));
        }
    }

    pub(crate) fn scroll_log_popup_to_top(&mut self) {
        if let Some(popup) = self.log_popup.as_mut() {
            popup.scroll = 0;
            popup.follow_tail = false;
        }
    }

    pub(crate) fn scroll_log_popup_to_bottom(&mut self) {
        if let Some(popup) = self.log_popup.as_mut() {
            popup.scroll = popup
                .lines
                .len()
                .saturating_sub(LOG_POPUP_DEFAULT_VISIBLE_LINES);
            popup.follow_tail = true;
        }
    }

    pub(crate) fn sync_log_popup_scroll(&mut self, visible_rows: usize) {
        let Some(popup) = self.log_popup.as_mut() else {
            return;
        };
        let max_scroll = log_popup_max_scroll(popup.lines.len(), visible_rows);
        if popup.follow_tail {
            popup.scroll = max_scroll;
        } else {
            popup.scroll = popup.scroll.min(max_scroll);
        }
    }
}

fn log_popup_max_scroll(line_count: usize, visible_rows: usize) -> usize {
    if visible_rows == 0 {
        0
    } else {
        line_count.saturating_sub(visible_rows)
    }
}

pub(crate) fn line_matches_log_popup(name: &str, line: &FormattedLogLine) -> bool {
    if line.name == name {
        return true;
    }
    if line.name != LIFECYCLE_EVENT_NAME {
        return false;
    }
    String::from_utf8_lossy(&line.bytes).contains(&format!("{name}:"))
}

impl ViewMode {
    pub(crate) fn needs_wall_clock_redraw(self) -> bool {
        matches!(self, Self::Tasks)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn services(entries: &[(&str, ServiceState)]) -> HashMap<String, ServiceState> {
        entries.iter().map(|(n, s)| (n.to_string(), *s)).collect()
    }

    fn tasks(entries: &[(&str, TaskItemState)]) -> HashMap<String, TaskItemState> {
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
            verbose_enabled: false,
        })
    }

    #[test]
    fn from_state_counts_ready_failed_and_pending_run() {
        struct Case {
            name: &'static str,
            services: Vec<(&'static str, ServiceState)>,
            tasks: Vec<(&'static str, TaskItemState)>,
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
                    ("migrate", TaskItemState::PendingRun),
                    ("seed", TaskItemState::Completed),
                    ("backup", TaskItemState::PendingRun),
                    ("build", TaskItemState::Running),
                ],
                want: StatusCounts {
                    services_total: 3, // cache (Lazy) doesn't count
                    services_ready: 1,
                    services_failed: 1,
                    services_unhealthy: 0,
                    services_active: 1, // queue (Starting)
                    tasks_running: 1,   // build
                    tasks_pending_run: 2,
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
        app.apply_service_runtime("api".into(), ServiceState::Ready, None);
        assert_eq!(app.counts.services_ready, 1);
        app.apply_service_runtime("db".into(), ServiceState::Ready, None);
        assert_eq!(app.counts.services_ready, 2);
        app.apply_service_runtime("db".into(), ServiceState::Stopping, None);
        assert_eq!(app.counts.services_ready, 1);
        assert_eq!(app.counts.services_active, 1);
        app.apply_service_runtime("api".into(), ServiceState::Failed, None);
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
        app.filter.commit();

        assert!(!app.should_render_log("db", false));
        let changed = app.apply_service_runtime("db".into(), ServiceState::Failed, None);

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
        app.filter.commit();

        let changed = app.apply_service_runtime("api".into(), ServiceState::DependencyFailed, None);

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
        app.filter.commit();

        let changed = app.apply_task_state("lint".into(), TaskItemState::Failed, None);

        assert!(changed);
        assert!(app.should_render_log("lint", false));
        assert!(!app.should_render_log("build", false));
    }

    #[test]
    fn log_popup_matches_source_and_named_lifecycle_lines() {
        let direct = FormattedLogLine {
            name: "api".to_string(),
            is_lifecycle: false,
            bytes: b"api output".to_vec(),
        };
        let lifecycle = FormattedLogLine {
            name: LIFECYCLE_EVENT_NAME.to_string(),
            is_lifecycle: true,
            bytes: b"[don] api: started".to_vec(),
        };
        let other = FormattedLogLine {
            name: LIFECYCLE_EVENT_NAME.to_string(),
            is_lifecycle: true,
            bytes: b"[don] worker: started".to_vec(),
        };

        assert!(line_matches_log_popup("api", &direct));
        assert!(line_matches_log_popup("api", &lifecycle));
        assert!(!line_matches_log_popup("api", &other));
    }

    #[test]
    fn log_popup_sync_clamps_to_actual_visible_rows() {
        struct Case {
            name: &'static str,
            line_count: usize,
            follow_tail: bool,
            initial_scroll: usize,
            visible_rows: usize,
            want_scroll: usize,
        }

        let cases = vec![
            Case {
                name: "tail uses real taller viewport",
                line_count: 100,
                follow_tail: true,
                initial_scroll: 70,
                visible_rows: 40,
                want_scroll: 60,
            },
            Case {
                name: "tail uses real shorter viewport",
                line_count: 100,
                follow_tail: true,
                initial_scroll: 70,
                visible_rows: 10,
                want_scroll: 90,
            },
            Case {
                name: "manual over-scroll clamps to last full page",
                line_count: 100,
                follow_tail: false,
                initial_scroll: 99,
                visible_rows: 40,
                want_scroll: 60,
            },
            Case {
                name: "hidden popup area cannot accumulate scroll debt",
                line_count: 100,
                follow_tail: false,
                initial_scroll: 99,
                visible_rows: 0,
                want_scroll: 0,
            },
            Case {
                name: "viewport larger than content",
                line_count: 5,
                follow_tail: false,
                initial_scroll: 4,
                visible_rows: 40,
                want_scroll: 0,
            },
        ];

        for case in cases {
            let mut app = app_with_names(vec!["api".to_string()], vec![]);
            let lines = (0..case.line_count)
                .map(|i| format!("line {i}").into_bytes())
                .collect();
            app.open_log_popup("api".to_string(), lines);
            let popup = app.log_popup.as_mut().unwrap();
            popup.follow_tail = case.follow_tail;
            popup.scroll = case.initial_scroll;

            app.sync_log_popup_scroll(case.visible_rows);

            assert_eq!(
                app.log_popup.as_ref().unwrap().scroll,
                case.want_scroll,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn service_items_prioritize_service_states_and_exclude_tasks() {
        struct Case {
            name: &'static str,
            services: Vec<(&'static str, ServiceState)>,
            tasks: Vec<(&'static str, TaskItemState)>,
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
                ("task-skipped", TaskItemState::Skipped),
                ("task-completed", TaskItemState::Completed),
                ("task-building", TaskItemState::Building),
                ("task-pending-run", TaskItemState::PendingRun),
                ("task-failed", TaskItemState::Failed),
                ("task-dep", TaskItemState::DependencyFailed),
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
                app.apply_service_runtime(name.to_string(), state, None);
            }
            for (name, state) in case.tasks {
                app.apply_task_state(name.to_string(), state, None);
            }

            let items = app.service_items();
            let got: Vec<&str> = items.iter().map(OverlayItem::name).collect();
            assert_eq!(got, case.want, "case: {}", case.name);
        }
    }

    #[test]
    fn service_items_include_service_pid() {
        let mut app = app_with_names(vec!["api".into()], vec![]);

        app.apply_service_runtime("api".into(), ServiceState::Running, Some(12_345));

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
        app.filter.commit();

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
        app.apply_task_state("completed".into(), TaskItemState::Completed, None);
        app.apply_task_state("failed".into(), TaskItemState::Failed, None);
        app.apply_task_state("pending-run".into(), TaskItemState::PendingRun, None);
        app.apply_task_state("running".into(), TaskItemState::Running, None);
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

    #[test]
    fn only_task_table_needs_wall_clock_redraws() {
        struct Case {
            mode: ViewMode,
            want: bool,
        }

        let cases = vec![
            Case {
                mode: ViewMode::Tasks,
                want: true,
            },
            Case {
                mode: ViewMode::Services,
                want: false,
            },
            Case {
                mode: ViewMode::Normal,
                want: false,
            },
            Case {
                mode: ViewMode::Filter,
                want: false,
            },
            Case {
                mode: ViewMode::Form,
                want: false,
            },
        ];

        for case in cases {
            assert_eq!(case.mode.needs_wall_clock_redraw(), case.want);
        }
    }
}
