//! Rendering primitives for the TUI.
//!
//! Two entry points:
//! - [`draw_bar`] fills the single-row inline viewport with the status bar.
//!   It's called on every state change while the inline terminal is active.
//! - [`draw_modal`] renders full-screen content (filter, task/service tables,
//!   form) into an alt-screen [`Terminal`]. It's called whenever the
//!   modal's app state changes.
//!
//! All UI output is a pure function of the [`App`] state plus the frame size —
//! no cursor math, no incremental writes.
//!
//! Log lines are *not* rendered here; they go into scrollback above the inline
//! viewport via [`Terminal::insert_before`].
//!
//! [`Terminal`]: ratatui::Terminal
//! [`Terminal::insert_before`]: ratatui::Terminal::insert_before

use std::collections::HashMap;

use ansi_to_tui::IntoText;
use crossterm::style::Color as CrosstermColor;
use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{
    Block, Borders, Cell, Clear, List, ListItem, ListState, Paragraph, Row, Wrap,
};
use std::borrow::Cow;

use super::app::{App, OverlayItem, StatusCounts, TaskStatusItem, ViewMode};
use super::failure_summary;
use super::filter::{FilterFocus, FilterRow, FilterState};
use super::status_table::{StatusTableView, draw_status_table};
use crate::runner::{ServiceState, TaskItemState};
use crate::task_state::TaskRunInfo;

/// Total rows the inline viewport reserves: 1 blank buffer row + 3 rows
/// for the bordered status box (top border + content + bottom border).
pub(crate) const BAR_VIEWPORT_HEIGHT: u16 = 4;

/// Spinner frames — the standard "dots" set. Rotate with `app.spinner_frame`.
const SPINNER_FRAMES: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

/// Draw the status bar (blank buffer row + bordered box) into the inline
/// viewport.
pub(crate) fn draw_bar(frame: &mut Frame<'_>, app: &App) {
    let area = frame.area();
    if area.height < BAR_VIEWPORT_HEIGHT || area.width < 2 {
        return;
    }
    // Row 0 (blank) gives breathing room between scrollback logs and the box.
    // Rows 1..=3 render the bordered box.
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Length(3)])
        .split(area);
    let box_area = layout[1];

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));
    let inner = block.inner(box_area);
    frame.render_widget(block, box_area);

    // Count only services for the filter badge — tasks and synthetic
    // streams (don/bazel/turbo) are filterable too, but the bar should
    // echo what the user thinks of as "my services." Lazy services are
    // excluded so the denominator matches `counts.services_total` (which
    // excludes Lazy for the same "not-yet-started" reason).
    let countable = || {
        app.services_state
            .iter()
            .filter(|(_, s)| !matches!(s, crate::runner::ServiceState::Lazy))
    };
    let visible_services = countable()
        .filter(|(name, _)| app.filter.passes(name))
        .count();
    let total_services = countable().count();
    let left_line = if app.shutdown_started {
        shutdown_bar_line(&app.counts, app.spinner_frame)
    } else {
        normal_bar_line(
            &app.counts,
            &app.filter,
            app.spinner_frame,
            visible_services,
            total_services,
            app.verbose_enabled,
            app.has_failure_summary(),
        )
    };
    let update_badge = (!app.shutdown_started)
        .then(|| app.update_badge.as_ref().map(update_badge_line))
        .flatten();

    if let Some(right_line) = update_badge {
        let right_width = u16::try_from(line_width(&right_line)).unwrap_or(u16::MAX);
        if right_width > 0 && right_width.saturating_add(2) < inner.width {
            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Min(1),
                    Constraint::Length(right_width.saturating_add(1)),
                ])
                .split(inner);
            frame.render_widget(Paragraph::new(left_line), chunks[0]);
            frame.render_widget(
                Paragraph::new(right_line).alignment(Alignment::Right),
                chunks[1],
            );
            return;
        }
    }

    frame.render_widget(Paragraph::new(left_line), inner);
}

/// Dispatch to the full-screen render function for the current view mode.
/// Callers should only invoke this when `app.view_mode != Normal`.
pub(crate) fn draw_modal(frame: &mut Frame<'_>, app: &App) {
    match app.view_mode {
        ViewMode::Filter => draw_filter_modal(frame, app),
        ViewMode::Tasks => draw_tasks_table(frame, app),
        ViewMode::Services => draw_services_table(frame, app),
        ViewMode::Failures => draw_failure_summary(frame, app),
        ViewMode::Form => draw_form_modal(frame, app),
        ViewMode::Normal => {}
    }
    draw_log_popup(frame, app);
}

fn draw_filter_modal(frame: &mut Frame<'_>, app: &App) {
    let area = frame.area();
    if area.height < 3 || area.width == 0 {
        return;
    }

    // Border + title wraps the whole modal. Inside: list at top, bar at bottom.
    let title = match app.filter.focus() {
        FilterFocus::List => {
            " Filter logs — [j/k ↑↓] move  [space] toggle  [o] only this  [/] search  [enter] done  [esc] revert "
        }
        FilterFocus::Query => {
            " Filter logs — [type] search  [enter] apply/close if single  [tab] back to list  [esc] revert "
        }
    };
    let outer = Block::default().borders(Borders::ALL).title(title);
    let inner = outer.inner(area);
    frame.render_widget(outer, area);

    if inner.height < 3 {
        return;
    }
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(1),
            Constraint::Length(1),
        ])
        .split(inner);
    let name_colors = log_name_colors(app);
    let query = Paragraph::new(filter_query_line(&app.filter));
    frame.render_widget(query, layout[0]);
    draw_filter_list(frame, layout[1], &app.filter, &name_colors);
    let bar = Paragraph::new(filter_bar_line(&app.counts, &app.filter));
    frame.render_widget(bar, layout[2]);
}

fn draw_tasks_table(frame: &mut Frame<'_>, app: &App) {
    let area = frame.area();
    let name_colors = log_name_colors(app);
    let items = app.task_items();
    let layout = task_table_layout(area.width);
    let header = Row::new(layout.columns.iter().map(|column| column.label())).style(
        Style::default()
            .add_modifier(Modifier::BOLD)
            .fg(Color::Cyan),
    );
    let rows = items
        .iter()
        .map(|item| task_table_row(item, &name_colors, &layout.columns))
        .collect();
    draw_status_table(
        frame,
        area,
        StatusTableView {
            title: " don tasks — [j/k ↑↓] move  [enter] run/form  [s] stop  [l] logs  [/] filter  [esc] clear/dismiss "
                .to_string(),
            header,
            rows,
            widths: layout.widths,
            state: &app.tasks_table,
            empty_label: "(no tasks)",
            selected_hint: task_selected_hint(app),
        },
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskTableColumn {
    Name,
    State,
    LastRun,
    Result,
    Duration,
}

impl TaskTableColumn {
    fn label(self) -> &'static str {
        match self {
            Self::Name => "NAME",
            Self::State => "STATE",
            Self::LastRun => "LAST RUN",
            Self::Result => "RESULT",
            Self::Duration => "DURATION",
        }
    }
}

struct TaskTableLayout {
    columns: Vec<TaskTableColumn>,
    widths: Vec<Constraint>,
}

fn task_table_layout(width: u16) -> TaskTableLayout {
    if width >= 110 {
        return TaskTableLayout {
            columns: vec![
                TaskTableColumn::Name,
                TaskTableColumn::State,
                TaskTableColumn::LastRun,
                TaskTableColumn::Result,
                TaskTableColumn::Duration,
            ],
            widths: vec![
                Constraint::Percentage(32),
                Constraint::Min(24),
                Constraint::Length(14),
                Constraint::Length(10),
                Constraint::Length(10),
            ],
        };
    }
    if width >= 70 {
        return TaskTableLayout {
            columns: vec![
                TaskTableColumn::Name,
                TaskTableColumn::State,
                TaskTableColumn::Result,
            ],
            widths: vec![
                Constraint::Percentage(32),
                Constraint::Min(24),
                Constraint::Length(10),
            ],
        };
    }
    TaskTableLayout {
        columns: vec![TaskTableColumn::Name, TaskTableColumn::State],
        widths: vec![Constraint::Percentage(40), Constraint::Min(20)],
    }
}

fn draw_failure_summary(frame: &mut Frame<'_>, app: &App) {
    let area = frame.area();
    if area.height == 0 || area.width == 0 {
        return;
    }

    let outer = failure_summary_block();
    let inner = outer.inner(area);
    frame.render_widget(outer, area);
    if inner.height == 0 || inner.width == 0 {
        return;
    }

    let max_scroll = failure_summary_max_scroll(area, app);
    let scroll = app.failure_summary_scroll.min(max_scroll);
    let scroll = u16::try_from(scroll).unwrap_or(u16::MAX);
    let text = failure_summary::text(&app.failure_summary_items());
    frame.render_widget(
        Paragraph::new(text)
            .wrap(Wrap { trim: false })
            .scroll((scroll, 0)),
        inner,
    );
}

fn failure_summary_block() -> Block<'static> {
    Block::default()
        .borders(Borders::ALL)
        .title(" don failures — [j/k ↑↓] scroll  [home/end] top/bottom  [esc] dismiss ")
}

pub(crate) fn failure_summary_max_scroll(area: Rect, app: &App) -> usize {
    let inner = failure_summary_block().inner(area);
    if inner.height == 0 || inner.width == 0 {
        return 0;
    }
    let text = failure_summary::text(&app.failure_summary_items());
    Paragraph::new(text)
        .wrap(Wrap { trim: false })
        .line_count(inner.width)
        .saturating_sub(inner.height as usize)
}

/// Render the full-screen services table — a table of every known service
/// with its current state, sorted errors → running → exited → lazy then
/// alphabetical within each bucket.
fn draw_services_table(frame: &mut Frame<'_>, app: &App) {
    let area = frame.area();
    let header = Row::new(vec!["NAME", "PID", "STATE"]).style(
        Style::default()
            .add_modifier(Modifier::BOLD)
            .fg(Color::Cyan),
    );
    let name_colors = log_name_colors(app);
    let rows = app
        .service_items()
        .iter()
        .map(|item| service_table_row(item, &name_colors))
        .collect();
    draw_status_table(
        frame,
        area,
        StatusTableView {
            title:
                " don services — [j/k ↑↓] move  [enter] start/stop  [r] restart  [R] hard restart  [l] logs  [/] filter  [esc] clear/dismiss "
                    .to_string(),
            header,
            rows,
            widths: vec![
                Constraint::Percentage(45),
                Constraint::Length(10),
                Constraint::Percentage(55),
            ],
            state: &app.services_table,
            empty_label: "(no services)",
            selected_hint: service_selected_hint(app),
        },
    );
}

fn draw_log_popup(frame: &mut Frame<'_>, app: &App) {
    let Some(popup) = app.log_popup.as_ref() else {
        return;
    };
    let area = centered_rect(frame.area(), 86, 72);
    if area.height < 3 || area.width < 8 {
        return;
    }

    frame.render_widget(Clear, area);
    let title = format!(
        " logs: {} — [esc] close  [j/k ↑↓] scroll  [home/end] top/bottom ",
        popup.name
    );
    let block = Block::default().borders(Borders::ALL).title(title);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.height == 0 {
        return;
    }

    if popup.lines.is_empty() {
        frame.render_widget(
            Paragraph::new(Line::from(vec![dim("(no logs captured yet)")])),
            inner,
        );
        return;
    }

    let visible_rows = inner.height as usize;
    let max_scroll = popup.lines.len().saturating_sub(visible_rows);
    let scroll = popup.scroll.min(max_scroll);
    let mut text = Text::default();
    for bytes in popup.lines.iter().skip(scroll).take(visible_rows) {
        let parsed = parse_ansi_text(bytes);
        if parsed.lines.is_empty() {
            text.lines.push(Line::default());
        } else {
            text.lines.extend(parsed.lines);
        }
    }

    frame.render_widget(Paragraph::new(text), inner);
}

pub(crate) fn log_popup_visible_rows(area: Rect) -> usize {
    let area = centered_rect(area, 86, 72);
    if area.height < 3 || area.width < 8 {
        return 0;
    }
    area.height.saturating_sub(2) as usize
}

fn centered_rect(area: Rect, percent_x: u16, percent_y: u16) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vertical[1]);
    horizontal[1]
}

fn parse_ansi_text(bytes: &[u8]) -> Text<'static> {
    bytes
        .into_text()
        .unwrap_or_else(|_| Text::raw(String::from_utf8_lossy(bytes).into_owned()))
}

fn service_table_row(item: &OverlayItem, name_colors: &HashMap<String, Color>) -> Row<'static> {
    let name = item.name.clone();
    let pid = item
        .pid
        .map(|p| p.to_string())
        .unwrap_or_else(|| "-".to_string());
    let state_cell = Cell::from(service_state_label(item.state, &item.failed_dependencies))
        .style(Style::default().fg(service_state_color(item.state)));
    let name_style = name_colors
        .get(&name)
        .copied()
        .map(|color| Style::default().fg(color))
        .unwrap_or_default();
    Row::new(vec![
        Cell::from(name).style(name_style),
        Cell::from(pid).style(Style::default().fg(Color::DarkGray)),
        state_cell,
    ])
}

fn task_table_row(
    item: &TaskStatusItem,
    name_colors: &HashMap<String, Color>,
    columns: &[TaskTableColumn],
) -> Row<'static> {
    let name_style = name_colors
        .get(&item.name)
        .copied()
        .map(|color| Style::default().fg(color))
        .unwrap_or_default();
    let cells = columns.iter().map(|column| match column {
        TaskTableColumn::Name => Cell::from(item.name.clone()).style(name_style),
        TaskTableColumn::State => {
            Cell::from(task_state_label(item.state, &item.failed_dependencies))
                .style(Style::default().fg(task_state_color(item.state)))
        }
        TaskTableColumn::LastRun => Cell::from(format_task_last_run_time(item.last_run.as_ref()))
            .style(Style::default().fg(Color::Gray)),
        TaskTableColumn::Result => Cell::from(format_task_result(item.last_run.as_ref())).style(
            Style::default().fg(item
                .last_run
                .as_ref()
                .map(task_result_color)
                .unwrap_or(Color::DarkGray)),
        ),
        TaskTableColumn::Duration => Cell::from(format_task_duration(
            item.last_run
                .as_ref()
                .and_then(|last_run| last_run.duration_ms),
        ))
        .style(Style::default().fg(Color::Gray)),
    });
    Row::new(cells)
}

fn draw_filter_list(
    frame: &mut Frame<'_>,
    area: Rect,
    filter: &FilterState,
    name_colors: &HashMap<String, Color>,
) {
    if area.height == 0 {
        return;
    }
    let rows = filter.rows();
    // Pass every row to ratatui; the list's `ListState` scrolls automatically
    // to keep the selected index in view when rows overflow the area.
    let items: Vec<ListItem<'static>> = rows
        .iter()
        .map(|row| match row {
            FilterRow::All => {
                let selected = filter.all_selected_in_edit();
                let checkbox = if selected { "[x] " } else { "[ ] " };
                let checkbox_style = if selected {
                    Style::default().fg(Color::Green)
                } else {
                    Style::default().fg(Color::DarkGray)
                };
                ListItem::new(Line::from(vec![
                    Span::styled(checkbox, checkbox_style),
                    Span::styled(
                        "all",
                        Style::default()
                            .fg(Color::Cyan)
                            .add_modifier(Modifier::BOLD),
                    ),
                ]))
            }
            FilterRow::Name(name) => {
                let selected = filter.is_edit_selected(name);
                let checkbox = if selected { "[x] " } else { "[ ] " };
                let checkbox_style = if selected {
                    Style::default().fg(Color::Green)
                } else {
                    Style::default().fg(Color::DarkGray)
                };
                let name_style = name_colors
                    .get(name)
                    .copied()
                    .map(|color| Style::default().fg(color))
                    .unwrap_or_default();
                ListItem::new(Line::from(vec![
                    Span::styled(checkbox, checkbox_style),
                    Span::styled(name.clone(), name_style),
                ]))
            }
        })
        .collect();

    let list = List::new(items)
        .highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("▸ ");
    let mut state = ListState::default().with_selected(if rows.is_empty() {
        None
    } else {
        Some(filter.highlight())
    });
    frame.render_stateful_widget(list, area, &mut state);
}

fn service_selected_hint(app: &App) -> Option<String> {
    let items = app.service_items();
    let item = app
        .services_table
        .selected_index(items.len())
        .and_then(|idx| items.get(idx))?;
    match item.state {
        ServiceState::Ready | ServiceState::Running | ServiceState::Unhealthy => {
            Some(format!("enter stop {}", item.name))
        }
        ServiceState::Stopped | ServiceState::Lazy => Some(format!("enter start {}", item.name)),
        ServiceState::Failed | ServiceState::DependencyFailed => {
            Some(format!("enter stop {}", item.name))
        }
        ServiceState::Pending
        | ServiceState::Building
        | ServiceState::Starting
        | ServiceState::Stopping => Some("transitioning".to_string()),
    }
}

fn task_selected_hint(app: &App) -> Option<String> {
    let items = app.task_items();
    let item = app
        .tasks_table
        .selected_index(items.len())
        .and_then(|idx| items.get(idx))?;
    if !item.runnable() {
        return Some("transitioning".to_string());
    }
    if item.has_params {
        Some(format!("enter form {}", item.name))
    } else {
        Some(format!("enter run {}", item.name))
    }
}

fn format_task_last_run_time(last_run: Option<&TaskRunInfo>) -> String {
    last_run
        .map(|run| format_relative_unix_secs(run.finished_at_unix_secs))
        .unwrap_or_else(|| "-".to_string())
}

fn format_task_result(last_run: Option<&TaskRunInfo>) -> String {
    match last_run {
        Some(run) if run.success => "ok".to_string(),
        Some(_) => "failed".to_string(),
        None => "-".to_string(),
    }
}

fn task_result_color(last_run: &TaskRunInfo) -> Color {
    if last_run.success {
        Color::Green
    } else {
        Color::Red
    }
}

fn format_relative_unix_secs(timestamp: u64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(timestamp, |duration| duration.as_secs());
    if timestamp > now.saturating_add(5) {
        return "in the future".to_string();
    }
    let elapsed = now.saturating_sub(timestamp);
    match elapsed {
        0..=4 => "just now".to_string(),
        5..=59 => format!("{elapsed}s ago"),
        60..=3_599 => format!("{}m ago", elapsed / 60),
        3_600..=86_399 => format!("{}h ago", elapsed / 3_600),
        _ => format!("{}d ago", elapsed / 86_400),
    }
}

fn format_task_duration(duration_ms: Option<u64>) -> String {
    let Some(duration_ms) = duration_ms else {
        return "-".to_string();
    };
    if duration_ms < 1_000 {
        format!("{duration_ms}ms")
    } else {
        let seconds = duration_ms / 1_000;
        let tenths = (duration_ms % 1_000) / 100;
        if tenths == 0 {
            format!("{seconds}s")
        } else {
            format!("{seconds}.{tenths}s")
        }
    }
}

fn normal_bar_line(
    counts: &StatusCounts,
    filter: &FilterState,
    spinner_frame: usize,
    visible_services: usize,
    total_services: usize,
    verbose_enabled: bool,
    has_failure_summary: bool,
) -> Line<'static> {
    let mut spans: Vec<Span<'static>> = Vec::new();

    // Spinner slot is always present so the bar doesn't shift as work
    // starts/stops. When idle, render a space.
    let spinner_glyph = if counts.is_working() {
        SPINNER_FRAMES[spinner_frame % SPINNER_FRAMES.len()]
    } else {
        " "
    };
    spans.push(Span::styled(
        format!(" {spinner_glyph} "),
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    ));

    spans.extend(base_count_spans(counts, has_failure_summary));

    if counts.tasks_running > 0 {
        spans.push(separator());
        let label = if counts.tasks_running == 1 {
            "1 task running".to_string()
        } else {
            format!("{} tasks running", counts.tasks_running)
        };
        spans.push(Span::styled(
            label,
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ));
    }

    spans.push(separator());
    spans.push(dim("[l] logs"));
    if filter.is_active() {
        spans.push(dim(format!(" ({visible_services}/{total_services})")));
        spans.push(dim("  [R] reset"));
    }
    // Parked tasks are easy to miss as a lone `*`, so tint the whole hotkey.
    if counts.tasks_pending_run > 0 {
        spans.push(Span::styled("  [t] tasks", Style::default().fg(Color::Yellow)));
        spans.push(Span::styled(
            "*",
            Style::default()
                .fg(Color::LightYellow)
                .add_modifier(Modifier::BOLD),
        ));
    } else {
        spans.push(dim("  [t] tasks"));
    }
    spans.push(dim("  [s] services"));
    if verbose_enabled {
        spans.push(separator());
        spans.push(dim("verbose"));
    }
    Line::from(spans)
}

fn filter_bar_line(counts: &StatusCounts, filter: &FilterState) -> Line<'static> {
    let mut spans = base_count_spans(counts, false);
    spans.push(separator());
    let hint = match filter.focus() {
        FilterFocus::List => {
            "[j/k] move  [space] toggle  [o] only this  [/] search  [R] defaults".to_string()
        }
        FilterFocus::Query => {
            "[type] search  [enter] apply/close if single  [tab] back to list".to_string()
        }
    };
    spans.push(dim(hint));
    Line::from(spans)
}

fn filter_query_line(filter: &FilterState) -> Line<'static> {
    let mut spans = vec![bold_cyan("search: ")];
    let query_style = match filter.focus() {
        FilterFocus::Query => Style::default().fg(Color::White).bg(Color::DarkGray),
        FilterFocus::List => Style::default().fg(Color::White),
    };
    let query_text = if filter.query().is_empty() && filter.focus() == FilterFocus::List {
        Span::styled(
            "[/] to search".to_string(),
            Style::default().fg(Color::DarkGray),
        )
    } else {
        Span::styled(filter.query().to_string(), query_style)
    };
    spans.push(query_text);
    if filter.focus() == FilterFocus::Query {
        spans.push(Span::styled(
            "▌",
            Style::default()
                .fg(Color::White)
                .bg(Color::DarkGray)
                .add_modifier(Modifier::SLOW_BLINK),
        ));
    }
    Line::from(spans)
}

fn log_name_colors(app: &App) -> HashMap<String, Color> {
    let names: Vec<&str> = app
        .services_state
        .keys()
        .map(String::as_str)
        .chain(app.tasks_state.keys().map(String::as_str))
        .collect();
    crate::output::assign_colors(&names)
        .into_iter()
        .map(|(name, color)| (name, crossterm_color_to_ratatui(color)))
        .collect()
}

fn crossterm_color_to_ratatui(color: CrosstermColor) -> Color {
    match color {
        CrosstermColor::Reset => Color::Reset,
        CrosstermColor::Black => Color::Black,
        CrosstermColor::DarkGrey => Color::DarkGray,
        CrosstermColor::Red => Color::LightRed,
        CrosstermColor::DarkRed => Color::Red,
        CrosstermColor::Green => Color::LightGreen,
        CrosstermColor::DarkGreen => Color::Green,
        CrosstermColor::Yellow => Color::LightYellow,
        CrosstermColor::DarkYellow => Color::Yellow,
        CrosstermColor::Blue => Color::LightBlue,
        CrosstermColor::DarkBlue => Color::Blue,
        CrosstermColor::Magenta => Color::LightMagenta,
        CrosstermColor::DarkMagenta => Color::Magenta,
        CrosstermColor::Cyan => Color::LightCyan,
        CrosstermColor::DarkCyan => Color::Cyan,
        CrosstermColor::White => Color::White,
        CrosstermColor::Grey => Color::Gray,
        CrosstermColor::Rgb { r, g, b } => Color::Rgb(r, g, b),
        CrosstermColor::AnsiValue(value) => Color::Indexed(value),
    }
}

fn shutdown_bar_line(counts: &StatusCounts, spinner_frame: usize) -> Line<'static> {
    let mut spans: Vec<Span<'static>> = Vec::new();
    let spinner_glyph = if counts.is_working() {
        SPINNER_FRAMES[spinner_frame % SPINNER_FRAMES.len()]
    } else {
        " "
    };
    spans.push(Span::styled(
        format!(" {spinner_glyph} "),
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    ));
    spans.push(Span::styled(
        "shutting down",
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    ));
    spans.push(separator());
    spans.extend(base_count_spans(counts, false));
    if counts.tasks_running > 0 {
        spans.push(separator());
        let label = if counts.tasks_running == 1 {
            "1 task running".to_string()
        } else {
            format!("{} tasks running", counts.tasks_running)
        };
        spans.push(Span::styled(
            label,
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ));
    }
    Line::from(spans)
}

fn update_badge_line(update: &super::app::UpdateBadge) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            format!(
                "upgrade available {}->{}",
                update.current_version, update.latest_version
            ),
            Style::default()
                .fg(Color::LightYellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(" ".to_string(), Style::default()),
    ])
}

fn line_width(line: &Line<'_>) -> usize {
    line.spans
        .iter()
        .map(|span| span.content.chars().count())
        .sum()
}

fn base_count_spans(counts: &StatusCounts, show_failure_info: bool) -> Vec<Span<'static>> {
    let mut spans = Vec::new();

    let ready_color = if counts.services_failed > 0 {
        Color::Red
    } else if counts.services_unhealthy > 0 {
        Color::LightRed
    } else if counts.services_total > 0 && counts.services_ready == counts.services_total {
        Color::Green
    } else {
        Color::Yellow
    };
    spans.push(Span::styled(
        format!(
            "{}/{} services ready",
            counts.services_ready, counts.services_total
        ),
        Style::default().fg(ready_color),
    ));

    if counts.services_failed > 0 {
        spans.push(separator());
        let info_hint = if show_failure_info { " [i]" } else { "" };
        spans.push(Span::styled(
            format!("{} failed{info_hint}", counts.services_failed),
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ));
    } else if counts.tasks_failed > 0 {
        spans.push(separator());
        let noun = if counts.tasks_failed == 1 {
            "task"
        } else {
            "tasks"
        };
        let info_hint = if show_failure_info { " [i]" } else { "" };
        spans.push(Span::styled(
            format!("{} {noun} failed{info_hint}", counts.tasks_failed),
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ));
    }

    if counts.services_unhealthy > 0 {
        spans.push(separator());
        spans.push(Span::styled(
            format!("{} unhealthy", counts.services_unhealthy),
            Style::default()
                .fg(Color::LightRed)
                .add_modifier(Modifier::BOLD),
        ));
    }

    spans
}

fn service_state_label(state: ServiceState, failed_dependencies: &[String]) -> Cow<'static, str> {
    match state {
        ServiceState::Pending => Cow::Borrowed("pending"),
        ServiceState::Building => Cow::Borrowed("building"),
        ServiceState::Lazy => Cow::Borrowed("lazy"),
        ServiceState::Starting => Cow::Borrowed("starting"),
        ServiceState::Running => Cow::Borrowed("running"),
        ServiceState::Ready => Cow::Borrowed("ready"),
        ServiceState::Unhealthy => Cow::Borrowed("unhealthy"),
        ServiceState::Stopping => Cow::Borrowed("stopping"),
        ServiceState::Stopped => Cow::Borrowed("stopped"),
        ServiceState::Failed => Cow::Borrowed("failed"),
        ServiceState::DependencyFailed => dependency_failed_label(failed_dependencies),
    }
}

fn task_state_label(state: TaskItemState, failed_dependencies: &[String]) -> Cow<'static, str> {
    match state {
        TaskItemState::Pending => Cow::Borrowed("pending"),
        TaskItemState::Building => Cow::Borrowed("building"),
        TaskItemState::Running => Cow::Borrowed("running"),
        TaskItemState::Completed => Cow::Borrowed("completed"),
        TaskItemState::Skipped => Cow::Borrowed("skipped"),
        TaskItemState::Failed => Cow::Borrowed("failed"),
        TaskItemState::DependencyFailed => dependency_failed_label(failed_dependencies),
        TaskItemState::PendingRun => Cow::Borrowed("pending run"),
    }
}

fn dependency_failed_label(failed_dependencies: &[String]) -> Cow<'static, str> {
    if failed_dependencies.is_empty() {
        Cow::Borrowed("dep failed")
    } else {
        Cow::Owned(format!("dep failed: {}", failed_dependencies.join(", ")))
    }
}

fn service_state_color(state: ServiceState) -> Color {
    match state {
        ServiceState::Ready | ServiceState::Running => Color::Green,
        ServiceState::Starting
        | ServiceState::Building
        | ServiceState::Pending
        | ServiceState::Stopping => Color::Yellow,
        ServiceState::Lazy => Color::Cyan,
        ServiceState::Stopped => Color::DarkGray,
        ServiceState::Unhealthy => Color::LightRed,
        ServiceState::Failed => Color::Red,
        // Dim red: same family as Failed but visually quieter, reflecting
        // that it's a downstream casualty, not the root cause.
        ServiceState::DependencyFailed => Color::Rgb(150, 60, 60),
    }
}

fn task_state_color(state: TaskItemState) -> Color {
    match state {
        TaskItemState::Completed | TaskItemState::Skipped => Color::Green,
        TaskItemState::Running | TaskItemState::Pending | TaskItemState::Building => Color::Yellow,
        TaskItemState::PendingRun => Color::Cyan,
        TaskItemState::Failed => Color::Red,
        TaskItemState::DependencyFailed => Color::Rgb(150, 60, 60),
    }
}

fn separator() -> Span<'static> {
    Span::styled("  │  ", Style::default().fg(Color::DarkGray))
}

fn bold_cyan<S: Into<String>>(text: S) -> Span<'static> {
    Span::styled(
        text.into(),
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )
}

fn dim<S: Into<String>>(text: S) -> Span<'static> {
    Span::styled(text.into(), Style::default().fg(Color::DarkGray))
}

/// Render the param-entry form. Each declared param occupies one row
/// (prompt + input + inline hint); the focused field optionally renders a
/// candidate dropdown beneath itself.
fn draw_form_modal(frame: &mut Frame<'_>, app: &App) {
    use super::form::{CandidateState, FormState};
    use crate::config::ParamKind;

    let area = frame.area();
    if area.height < 3 || area.width == 0 {
        return;
    }
    let Some(form): Option<&FormState> = app.form.as_ref() else {
        return;
    };

    let title = format!(
        " Run {}  — [tab] next/refresh  [↑↓] move  [enter] accept/next/submit  [ctrl-enter] submit  [esc] cancel ",
        form.task
    );
    let outer = Block::default().borders(Borders::ALL).title(title);
    let inner = outer.inner(area);
    frame.render_widget(outer, area);

    if inner.height < 2 {
        return;
    }
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(1)])
        .split(inner);

    // One paragraph with a Line per field, stacked vertically. The field
    // rows render into the same area, so we split by exact row counts.
    let rows = layout[0];
    let mut y = rows.y;
    let available = rows.height as usize;
    let mut used = 0usize;
    for (idx, field) in form.fields.iter().enumerate() {
        let is_focused = idx == form.focus;
        let remaining_fields = form.fields.len().saturating_sub(idx + 1);
        let max_rows_for_field = available.saturating_sub(used + remaining_fields);
        let field_rows = field_render_rows(field, is_focused, max_rows_for_field);
        if used + field_rows.len() > available {
            break;
        }
        for line in field_rows {
            if y >= rows.y + rows.height {
                break;
            }
            let row_area = Rect {
                x: rows.x,
                y,
                width: rows.width,
                height: 1,
            };
            frame.render_widget(Paragraph::new(line), row_area);
            y += 1;
            used += 1;
        }
    }

    // Footer line with submit error (if any) or a contextual hint.
    let footer = match form.submit_error.as_deref() {
        Some(err) => Line::from(vec![Span::styled(
            format!("⚠ {err}"),
            Style::default().fg(Color::Red),
        )]),
        None => {
            let hint = form
                .focused()
                .map(|f| match f.kind {
                    ParamKind::Bool => "space flips the toggle",
                    ParamKind::Int => "↑/↓ steps the value",
                    _ => "↑/↓ selects candidate · enter/→ accepts",
                })
                .unwrap_or("");
            Line::from(vec![dim(hint)])
        }
    };
    frame.render_widget(Paragraph::new(footer), layout[1]);
    // Borrow to satisfy the unused-import lint on variants we don't reach.
    let _ = CandidateState::None;
}

/// Build the lines for one field — at least one row for the input itself,
/// plus optional dropdown rows when the field is focused and has candidates.
fn field_render_rows(
    field: &super::form::Field,
    is_focused: bool,
    max_total_rows: usize,
) -> Vec<Line<'static>> {
    use super::form::CandidateState;
    use crate::config::ParamKind;

    let marker = if is_focused { "▶ " } else { "  " };
    let required_mark = if field.required { "*" } else { "" };
    let prompt = format!("{marker}{}{required_mark}: ", field.prompt);
    let prompt_style = if is_focused {
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::White)
    };

    let value_str = match field.kind {
        ParamKind::Bool => {
            if field.value.trim() == "true" {
                "[x] true".to_string()
            } else {
                "[ ] false".to_string()
            }
        }
        _ => field.value.clone(),
    };
    let cursor = if is_focused && !matches!(field.kind, ParamKind::Bool) {
        "▎"
    } else {
        ""
    };

    let mut lines = Vec::new();
    lines.push(Line::from(vec![
        Span::styled(prompt, prompt_style),
        Span::styled(value_str, Style::default().fg(Color::White)),
        Span::styled(cursor, Style::default().fg(Color::DarkGray)),
    ]));
    if lines.len() >= max_total_rows {
        return lines;
    }

    // Error / status banner.
    match &field.candidates {
        CandidateState::Loading if is_focused => {
            lines.push(Line::from(dim("  loading completions…")));
        }
        CandidateState::Failed { message, log_path } if is_focused => {
            let hint = match log_path {
                Some(p) => format!("  ⚠ {message} (log: {})", p.display()),
                None => format!("  ⚠ {message}"),
            };
            lines.push(Line::from(Span::styled(
                hint,
                Style::default().fg(Color::Red),
            )));
        }
        _ => {}
    }
    if lines.len() >= max_total_rows {
        return lines;
    }

    if is_focused {
        let remaining_rows = max_total_rows.saturating_sub(lines.len());
        let candidate_rows = remaining_rows.min(field.visible_candidates().len());
        if candidate_rows == 0 {
            return lines;
        }
        let window = field.visible_candidate_window(candidate_rows);
        let spare_rows = remaining_rows.saturating_sub(window.items.len());
        let show_above = window.hidden_above > 0 && spare_rows >= 2;
        let show_below = window.hidden_below > 0 && spare_rows > usize::from(show_above);

        if show_above {
            lines.push(Line::from(dim(format!(
                "    … {} above",
                window.hidden_above
            ))));
        }
        for (i, cand) in window.items.iter().enumerate() {
            let style = if i == window.highlight {
                Style::default()
                    .bg(Color::DarkGray)
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::Gray)
            };
            lines.push(Line::from(Span::styled(format!("    {cand}"), style)));
        }
        if show_below {
            lines.push(Line::from(dim(format!(
                "    … {} more",
                window.hidden_below
            ))));
        }
    }

    lines
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::config::ParamKind;
    use crate::tui::app::AppInit;
    use crate::tui::form::{CandidateState, Field};

    fn line_text(line: Line<'static>) -> String {
        line.spans
            .into_iter()
            .map(|span| span.content.into_owned())
            .collect::<Vec<String>>()
            .join("")
    }

    #[test]
    fn dependency_failed_label_names_blocking_dependencies() {
        struct Case {
            name: &'static str,
            dependencies: Vec<String>,
            want: &'static str,
        }

        let cases = vec![
            Case {
                name: "unknown dependency remains backward compatible",
                dependencies: Vec::new(),
                want: "dep failed",
            },
            Case {
                name: "one dependency",
                dependencies: vec!["db".to_string()],
                want: "dep failed: db",
            },
            Case {
                name: "multiple dependencies",
                dependencies: vec!["db".to_string(), "cache".to_string()],
                want: "dep failed: db, cache",
            },
        ];

        for case in cases {
            assert_eq!(
                dependency_failed_label(&case.dependencies),
                case.want,
                "case: {}",
                case.name
            );
        }
    }

    #[test]
    fn shutdown_bar_hides_interactive_controls() {
        let text = line_text(shutdown_bar_line(&StatusCounts::default(), 0));
        assert!(text.contains("shutting down"));
        assert!(!text.contains("[/] logs"));
        assert!(!text.contains("[t] tasks"));
        assert!(!text.contains("[s] services"));
    }

    #[test]
    fn normal_bar_marks_tasks_shortcut_when_tasks_are_pending() {
        let line = normal_bar_line(
            &StatusCounts {
                tasks_pending_run: 2,
                ..Default::default()
            },
            &FilterState::new(Vec::new(), &std::collections::HashSet::new(), None),
            0,
            0,
            0,
            false,
            false,
        );
        let star = line
            .spans
            .iter()
            .find(|span| span.content.as_ref() == "*")
            .expect("pending-task marker missing");

        assert_eq!(star.style.fg, Some(Color::LightYellow));
        assert!(star.style.add_modifier.contains(Modifier::BOLD));

        let hotkey = line
            .spans
            .iter()
            .find(|span| span.content.as_ref() == "  [t] tasks")
            .expect("tasks hotkey missing");
        assert_eq!(hotkey.style.fg, Some(Color::Yellow));

        let text = line_text(line);
        assert!(text.contains("[t] tasks*"));
        assert!(!text.contains("tasks pending"));
    }

    #[test]
    fn normal_bar_leaves_tasks_shortcut_unmarked_without_pending_tasks() {
        let line = normal_bar_line(
            &StatusCounts::default(),
            &FilterState::new(Vec::new(), &std::collections::HashSet::new(), None),
            0,
            0,
            0,
            false,
            false,
        );

        let hotkey = line
            .spans
            .iter()
            .find(|span| span.content.as_ref() == "  [t] tasks")
            .expect("tasks hotkey missing");
        assert_eq!(hotkey.style.fg, Some(Color::DarkGray));

        let text = line_text(line);
        assert!(text.contains("[t] tasks"));
        assert!(!text.contains("[t] tasks*"));
    }

    #[test]
    fn normal_bar_shows_reset_hint_when_filter_is_active() {
        let mut filter = FilterState::new(
            vec!["api".to_string(), "worker".to_string()],
            &std::collections::HashSet::new(),
            None,
        );
        filter.enter_edit();
        filter.push_query_char('a');
        filter.select_only_highlighted();
        filter.commit();

        let text = line_text(normal_bar_line(
            &StatusCounts::default(),
            &filter,
            0,
            1,
            2,
            false,
            false,
        ));

        assert!(text.contains("[l] logs (1/2)  [R] reset"));
    }

    #[test]
    fn normal_bar_marks_failed_count_with_info_shortcut() {
        struct Case {
            name: &'static str,
            counts: StatusCounts,
            want: &'static str,
            reject: &'static str,
        }

        let cases = vec![
            Case {
                name: "service failure details available",
                counts: StatusCounts {
                    services_total: 2,
                    services_failed: 2,
                    ..Default::default()
                },
                want: "2 failed [i]",
                reject: "2 failed  [i]",
            },
            Case {
                name: "task-only failure details available",
                counts: StatusCounts {
                    tasks_failed: 1,
                    ..Default::default()
                },
                want: "1 task failed [i]",
                reject: "1 tasks failed",
            },
        ];

        for case in cases {
            let text = line_text(normal_bar_line(
                &case.counts,
                &FilterState::new(Vec::new(), &std::collections::HashSet::new(), None),
                0,
                0,
                2,
                false,
                true,
            ));

            assert!(text.contains(case.want), "case: {}", case.name);
            assert!(!text.contains(case.reject), "case: {}", case.name);
        }
    }

    #[test]
    fn task_state_column_stays_visible_across_terminal_widths() {
        struct Case {
            width: u16,
            want_headers: &'static [&'static str],
            reject_headers: &'static [&'static str],
            want_state: &'static str,
        }

        let cases = vec![
            Case {
                width: 50,
                want_headers: &["NAME", "STATE"],
                reject_headers: &["LAST RUN", "RESULT", "DURATION"],
                want_state: "dep failed:",
            },
            Case {
                width: 60,
                want_headers: &["NAME", "STATE"],
                reject_headers: &["LAST RUN", "RESULT", "DURATION"],
                want_state: "configure-kafka-topics",
            },
            Case {
                width: 80,
                want_headers: &["NAME", "STATE", "RESULT"],
                reject_headers: &["LAST RUN", "DURATION"],
                want_state: "configure-kafka-topics",
            },
        ];

        for case in cases {
            let rendered = rendered_task_table(case.width);
            for header in case.want_headers {
                assert!(
                    rendered.contains(header),
                    "width {} should show {header}: {rendered}",
                    case.width
                );
            }
            for header in case.reject_headers {
                assert!(
                    !rendered.contains(header),
                    "width {} should hide {header}: {rendered}",
                    case.width
                );
            }
            assert!(
                rendered.contains(case.want_state),
                "width {} should preserve task state detail: {rendered}",
                case.width
            );
        }
    }

    fn rendered_task_table(width: u16) -> String {
        let mut app = App::new(AppInit {
            service_names: Vec::new(),
            task_names: vec!["configure-everything".to_string()],
            build_tool_names: Vec::new(),
            task_configs: HashMap::new(),
            task_last_runs: HashMap::new(),
            hidden_names: std::collections::HashSet::new(),
            auto_filter_on_failure_names: std::collections::HashSet::new(),
            cli_log_filter: None,
            verbose_enabled: false,
        });
        app.apply_task_state(
            "configure-everything".to_string(),
            TaskItemState::DependencyFailed,
            None,
            vec!["configure-kafka-topics".to_string()],
        );
        app.view_mode = ViewMode::Tasks;

        let backend = ratatui::backend::TestBackend::new(width, 8);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        terminal.draw(|frame| draw_modal(frame, &app)).unwrap();
        terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect()
    }

    #[test]
    fn failure_summary_wraps_instead_of_truncating_root_causes() {
        let mut app = App::new(AppInit {
            service_names: vec!["api".to_string()],
            task_names: Vec::new(),
            build_tool_names: Vec::new(),
            task_configs: HashMap::new(),
            task_last_runs: HashMap::new(),
            hidden_names: std::collections::HashSet::new(),
            auto_filter_on_failure_names: std::collections::HashSet::new(),
            cli_log_filter: None,
            verbose_enabled: false,
        });
        app.apply_service_runtime(
            "api".to_string(),
            ServiceState::DependencyFailed,
            None,
            vec![
                "configure-kafka-topics".to_string(),
                "configure-mongo-collections".to_string(),
            ],
        );
        app.open_failure_summary();

        let backend = ratatui::backend::TestBackend::new(45, 12);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        terminal.draw(|frame| draw_modal(frame, &app)).unwrap();
        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(rendered.contains("configure-kafka-topics"));
        assert!(rendered.contains("configure-mongo-collections"));
    }

    #[test]
    fn update_badge_shows_current_and_latest_versions() {
        let text = line_text(update_badge_line(&crate::tui::app::UpdateBadge {
            current_version: "0.4.1".to_string(),
            latest_version: "0.4.2".to_string(),
        }));

        assert!(text.contains("upgrade available 0.4.1->0.4.2"));
    }

    #[test]
    fn focused_field_uses_available_space_for_candidates() {
        let field = Field {
            name: "index".into(),
            prompt: "index".into(),
            required: false,
            kind: ParamKind::String,
            value: String::new(),
            static_choices: vec![
                "c0".into(),
                "c1".into(),
                "c2".into(),
                "c3".into(),
                "c4".into(),
                "c5".into(),
                "c6".into(),
            ],
            has_dynamic_completions: false,
            candidates: CandidateState::Static(vec![
                "c0".into(),
                "c1".into(),
                "c2".into(),
                "c3".into(),
                "c4".into(),
                "c5".into(),
                "c6".into(),
            ]),
            candidate_highlight: 0,
            error: None,
            int_min: None,
            int_max: None,
        };

        let rows = field_render_rows(&field, true, 8);
        let texts: Vec<String> = rows.into_iter().map(line_text).collect();

        assert_eq!(texts.len(), 8);
        assert!(texts.iter().any(|t| t.contains("c0")));
        assert!(texts.iter().any(|t| t.contains("c6")));
    }

    #[test]
    fn task_table_row_shows_last_run_result_and_duration() {
        let item = TaskStatusItem {
            name: "lint".to_string(),
            state: TaskItemState::Completed,
            failed_dependencies: Vec::new(),
            last_run: Some(TaskRunInfo {
                finished_at_unix_secs: 0,
                duration_ms: Some(1_250),
                success: true,
                exit_code: Some(0),
                message: None,
            }),
            has_params: false,
        };

        let layout = task_table_layout(120);
        let _row = task_table_row(&item, &HashMap::new(), &layout.columns);

        assert_eq!(format_task_result(item.last_run.as_ref()), "ok");
        assert_eq!(
            format_task_duration(item.last_run.as_ref().and_then(|run| run.duration_ms)),
            "1.2s"
        );
    }
}
