//! Rendering for the TUI.
//!
//! One entry point, [`draw`], which paints the whole screen every frame: the
//! log pane, the status bar, and whichever full-screen view or overlay the
//! current mode calls for. All of it is a pure function of [`App`], the
//! [`LogStore`] and the frame size — no cursor math, no incremental writes, no
//! knowledge of what changed since last time.
//!
//! That last part is the point. The old renderer had two entry points against
//! two different terminals, and every caller had to decide which one to invoke
//! and whether the log flow underneath needed replaying. Painting everything,
//! every frame, from one source of truth removes the question.
//!
//! [`LogStore`]: super::log_store::LogStore

use std::collections::HashMap;

use crossterm::style::Color as CrosstermColor;
use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, Borders, Cell, Clear, List, ListItem, ListState, Paragraph, Row, Wrap,
};
use std::borrow::Cow;

use super::app::{App, OverlayItem, StatusCounts, TaskStatusItem, ViewMode};
use super::failure_summary;
use super::filter::{FilterFocus, FilterRow, FilterState};
use super::status_table::{StatusTableView, draw_status_table};
use crate::client::{ServiceState, TaskState};
use crate::task_state::TaskRunInfo;

/// Rows the status bar occupies at the bottom of the screen: top border,
/// content, bottom border.
pub(crate) const BAR_HEIGHT: u16 = 3;

/// The rectangle log text actually occupies: the log pane minus its border.
///
/// The one definition of that inset. The store wraps against this width, the
/// selection clamps against this origin, and the renderer paints inside it —
/// three readers, and any two of them computing the inset independently is a
/// one-cell disagreement that shows up as a selection off by a column.
pub(crate) fn log_text_area(area: Rect, panel: super::panes::Panel, panel_open: bool) -> Rect {
    inner(super::panes::layout(area, BAR_HEIGHT, panel, panel_open).logs)
}

/// `Block::inner` for a plain full border, without needing the block.
fn inner(area: Rect) -> Rect {
    Rect::new(
        area.x.saturating_add(1),
        area.y.saturating_add(1),
        area.width.saturating_sub(2),
        area.height.saturating_sub(2),
    )
}

/// Width available to log text, which is what the store wraps against.
pub(crate) fn log_pane_width(area: Rect, panel: super::panes::Panel, panel_open: bool) -> u16 {
    log_text_area(area, panel, panel_open).width.max(1)
}

/// Paint the whole screen.
pub(crate) fn draw(frame: &mut Frame<'_>, app: &mut App, store: &super::log_store::LogStore) {
    let area = frame.area();
    if area.width == 0 || area.height == 0 {
        return;
    }
    let panes = super::panes::layout(area, BAR_HEIGHT, app.panel, app.panel_open());
    app.panes = panes;
    // Write the extent the layout actually granted back to the stored one, so
    // the two can never drift. A stored extent past what the terminal honours
    // would make keyboard resizing dead for as many presses as the overshoot —
    // adjusting a number the screen is not showing.
    if let Some(rect) = panes.status {
        app.panel.extent = match app.panel.side {
            super::panes::PaneSide::Right => rect.width,
            super::panes::PaneSide::Bottom => rect.height,
        };
    }

    // Clamp the scroll positions the overlays own before drawing them: they are
    // bounded by geometry only this function knows.
    if app.view_mode == ViewMode::Failures {
        let max_scroll = failure_summary_max_scroll(area, app);
        app.sync_failure_summary_scroll(max_scroll);
    }

    draw_log_pane(frame, app, store, panes.logs);

    // The services, tasks and filter views live in the side panel, beside a
    // log that keeps flowing — acting on a process and watching what it prints
    // are one activity, and the old full-screen tables forced a choice.
    // Cleared first because the widgets only paint the cells they use, and a
    // shrinking table would otherwise leave its old rows behind.
    if let Some(panel_area) = panes.status {
        frame.render_widget(Clear, panel_area);
        match app.view_mode {
            ViewMode::Services => draw_services_table(frame, app, panel_area),
            ViewMode::Tasks => draw_tasks_table(frame, app, panel_area),
            ViewMode::Filter => draw_filter_modal(frame, app, panel_area),
            _ => {}
        }
    }
    draw_bar(frame, app, panes.bar);

    // The failure summary and the param form stay full-screen: both demand a
    // decision, where a panel is for acting while still watching output. They
    // wipe what is under them first — the widgets only paint their own cells.
    if matches!(app.view_mode, ViewMode::Failures | ViewMode::Form) {
        frame.render_widget(Clear, area);
    }
    match app.view_mode {
        ViewMode::Failures => draw_failure_summary(frame, app),
        ViewMode::Form => draw_form_modal(frame, app),
        _ => {}
    }
    // The attached process floats above everything: it owns the keyboard
    // while it is open, so it should look like it does.
    draw_attach_window(frame, app);
}

/// What became of the process a window was attached to.
///
/// Read from don's own record rather than from anything the connection said,
/// because the connection only knows that it closed — the difference between a
/// task that completed and one that failed lives here.
fn attached_process_state(app: &App, name: &str) -> Cow<'static, str> {
    if let Some(state) = app.tasks_state.get(name) {
        return task_state_label(*state, &[]);
    }
    if let Some(state) = app.services_state.get(name) {
        return service_state_label(*state, false, &[]);
    }
    Cow::Borrowed("ended")
}

/// Draw the attached process's screen into its floating window.
///
/// Cell by cell rather than as text, because a terminal grid is not lines:
/// each cell carries its own colours and attributes, and a program that draws
/// a box or a status bar depends on every one of them landing where it put it.
fn draw_attach_window(frame: &mut Frame<'_>, app: &App) {
    use crate::output::emulator::CellColor;

    let Some(view) = app.attach.as_ref() else {
        return;
    };
    let area = view.window.to_rect();
    if area.width < 3 || area.height < 3 {
        return;
    }
    frame.render_widget(Clear, area);
    // An ended window is a record, not a terminal: dimmed, and titled with
    // what became of the process rather than with keys that no longer do
    // anything to it.
    let (border, title) = if view.ended {
        (
            Color::DarkGray,
            format!(
                " {} — {} · any key dismisses ",
                view.name,
                attached_process_state(app, &view.name)
            ),
        )
    } else {
        (Color::Cyan, format!(" {} — [^D] detach ", view.name))
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(border))
        .title(title);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.width == 0 || inner.height == 0 {
        return;
    }

    let Some(grid) = view.grid.as_ref() else {
        frame.render_widget(
            Paragraph::new(Line::from(Span::styled(
                "connecting…",
                Style::default().fg(Color::DarkGray),
            ))),
            inner,
        );
        return;
    };

    let convert = |color: CellColor, fallback: Option<Color>| -> Option<Color> {
        match color {
            CellColor::Default => fallback,
            CellColor::Palette(index) => Some(Color::Indexed(index)),
            CellColor::Rgb(r, g, b) => Some(Color::Rgb(r, g, b)),
        }
    };

    let buffer = frame.buffer_mut();
    for row in 0..inner.height.min(grid.rows) {
        for col in 0..inner.width.min(grid.cols) {
            let Some(cell) = grid
                .cells
                .get(usize::from(row) * usize::from(grid.cols) + usize::from(col))
            else {
                continue;
            };
            let Some(target) = buffer.cell_mut((inner.x + col, inner.y + row)) else {
                continue;
            };
            // An empty grapheme is the tail of a wide character, whose head
            // already painted both columns — leave whatever it put there.
            if cell.text.is_empty() {
                continue;
            }
            target.set_symbol(&cell.text);
            let mut style = Style::default();
            if let Some(fg) = convert(cell.fg, None) {
                style = style.fg(fg);
            }
            if let Some(bg) = convert(cell.bg, None) {
                style = style.bg(bg);
            }
            if cell.bold {
                style = style.add_modifier(Modifier::BOLD);
            }
            if cell.faint {
                style = style.add_modifier(Modifier::DIM);
            }
            if cell.italic {
                style = style.add_modifier(Modifier::ITALIC);
            }
            if cell.underline {
                style = style.add_modifier(Modifier::UNDERLINED);
            }
            if cell.inverse {
                style = style.add_modifier(Modifier::REVERSED);
            }
            if cell.strikethrough {
                style = style.add_modifier(Modifier::CROSSED_OUT);
            }
            target.set_style(style);
        }
    }

    // Put the real cursor where the process thinks it is, so anything that
    // asks the reader to type shows them where.
    if grid.cursor_visible {
        let (x, y) = grid.cursor;
        if x < inner.width && y < inner.height {
            frame.set_cursor_position((inner.x + x, inner.y + y));
        }
    }
}

/// A table's title, which depends on whether `l` has narrowed the log.
///
/// `l` is pressed while looking at a table, so the table is where the state it
/// produced has to be legible — the log pane names the process it is showing,
/// but the eye that pressed the key is over here.
///
/// While narrowed the way out leads, and `[l] logs` gives up its slot. Both
/// are forced by width: the services title is already sixty-six columns
/// against a panel half a screen wide, so whatever sits at the end is what
/// ratatui truncates — which would have hidden this hint in exactly the state
/// that needs it. `l` still narrows to another row while narrowed; it is the
/// less useful of the two things to say.
fn table_title(app: &App, table: &str, keys: &str) -> String {
    match app.narrowed_to() {
        Some(_) => format!(" {table} — [esc] all logs  {keys} "),
        None => format!(" {table} — {keys}  [l] logs "),
    }
}

/// What the log pane calls itself.
///
/// A pane narrowed by `l` is showing a fraction of the log, and nothing else
/// on screen says so — the filter panel need not be open, and rows that are
/// missing cannot advertise their absence. The title names the process being
/// shown and the key that gives the rest back, in the same shape the tables
/// announce their keys.
fn log_pane_title(app: &App) -> String {
    if app.debug_view {
        return " don's log ".to_string();
    }
    match app.narrowed_to() {
        Some(name) => format!(" logs — {name}  [esc] all "),
        None => " logs ".to_string(),
    }
}

/// Render the visible slice of the log, plus a scroll indicator when the view
/// is not pinned to the newest line.
fn draw_log_pane(
    frame: &mut Frame<'_>,
    app: &mut App,
    store: &super::log_store::LogStore,
    area: Rect,
) {
    if area.height == 0 || area.width == 0 {
        return;
    }
    // The pane is a bordered box like everything else on screen, so the lines
    // between regions are all the same kind of line. Text lives in the inner
    // rect; every geometry below uses it, and the standalone helpers
    // (`log_text_area`) apply the same inset so the input layer agrees.
    let focused = app.focus == super::panes::Focus::Logs;
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(if focused {
            Color::Cyan
        } else {
            Color::DarkGray
        }))
        .title(log_pane_title(app));
    let text_area = block.inner(area);
    frame.render_widget(block, area);
    if text_area.height == 0 || text_area.width == 0 {
        return;
    }
    let area = text_area;
    // Mend the index before reading it: this is the one place that knows the
    // pane's width, and the filter may have moved since the last frame.
    let key = super::view_index::ViewKey {
        width: area.width,
        filter: app.log_filter_fingerprint(),
    };
    let mut index = std::mem::take(&mut app.view_index);
    index.sync(store, key, &app.blank_after, |entry| {
        app.should_render_log(&entry.line.name, entry.line.is_lifecycle)
    });
    // The one place scroll position is decided, now that the index is current
    // and the pane's height is known.
    app.log_scroll =
        super::logs::resolve_scroll(&index, app.log_scroll, app.pending_scroll, area.height);
    app.pending_scroll = super::app::PendingScroll::default();
    let view = super::logs::build_view(
        store,
        &index,
        &app.blank_after,
        app.log_scroll,
        area.width,
        area.height,
    );
    app.view_index = index;
    // Remembered for the input layer: scrolling needs to know how far it can
    // go, and only the renderer knows how tall the pane came out.
    app.log_rows_above = view.rows_above;
    app.log_total_rows = view.total_rows;
    app.log_pane_height = area.height;

    // Top-aligned: a log shorter than the pane starts at the top and grows
    // down, the way a terminal does. Bottom-anchoring it would leave the first
    // line of a fresh run stranded under a screenful of blanks.
    let following = view.following;
    let rows_below = view
        .total_rows
        .saturating_sub(view.rows_above + area.height as usize);

    // The plain text of what is about to be on screen, kept so a copy resolves
    // against exactly the rows the user dragged across — after wrapping, after
    // filtering, after scrolling, with no need to re-derive any of it.
    app.log_visible_rows = view
        .rows
        .iter()
        .map(|line| {
            line.spans
                .iter()
                .map(|span| span.content.as_ref())
                .collect::<String>()
        })
        .collect();
    app.log_row_sources = view.row_sources.clone();
    app.log_pane_origin = (area.x, area.y);

    let selection = app.log_selection;
    let rows: Vec<Line<'_>> = if selection.is_empty() {
        view.rows
    } else {
        view.rows
            .into_iter()
            .zip(view.row_sources)
            .map(|(line, source)| highlight_selected(line, &selection, source))
            .collect()
    };
    frame.render_widget(Paragraph::new(rows), area);

    // Only when something is actually below the view. A selection pins the
    // view at the tail without following, and announcing "scrolled — 0 rows
    // below" for that state is a badge contradicting itself; the moment new
    // output puts real rows below, it appears with a true number.
    if !following && rows_below > 0 {
        draw_scroll_badge(frame, area, rows_below);
    }
}

/// Re-style the cells of one row that fall inside the selection.
///
/// Splits spans at the selection boundary rather than styling whole spans: a
/// selection almost never lines up with where the upstream formatter changed
/// colour, and highlighting the whole span would make the selection look like
/// it covers more than it does.
fn highlight_selected<'a>(
    line: Line<'a>,
    selection: &super::selection::Selection,
    source: super::logs::RowSource,
) -> Line<'a> {
    let mut out: Vec<Span<'a>> = Vec::with_capacity(line.spans.len());
    // Columns within the row, so the prefix is simply below the message's
    // first offset and never selectable.
    let mut column = 0usize;
    for span in line.spans {
        let mut run = String::new();
        let mut run_selected: Option<bool> = None;
        for ch in span.content.chars() {
            let selected = column >= source.indent
                && selection.contains(super::selection::Point::at(source, column));
            if run_selected != Some(selected) && !run.is_empty() {
                out.push(styled_run(&run, span.style, run_selected == Some(true)));
                run.clear();
            }
            run_selected = Some(selected);
            run.push(ch);
            column += 1;
        }
        if !run.is_empty() {
            out.push(styled_run(&run, span.style, run_selected == Some(true)));
        }
    }
    Line::from(out)
}

fn styled_run(text: &str, style: Style, selected: bool) -> Span<'static> {
    if selected {
        Span::styled(text.to_string(), style.add_modifier(Modifier::REVERSED))
    } else {
        Span::styled(text.to_string(), style)
    }
}

/// A small right-aligned marker saying the view is held above the live tail.
///
/// Without it, a scrolled-up pane during a quiet period is indistinguishable
/// from a stalled one.
fn draw_scroll_badge(frame: &mut Frame<'_>, area: Rect, rows_below: usize) {
    let label = format!(" ↑ scrolled — {rows_below} row(s) below · [end] follow ");
    let width = u16::try_from(label.chars().count()).unwrap_or(u16::MAX);
    if width >= area.width {
        return;
    }
    let badge = Rect::new(
        area.x + area.width - width,
        area.y + area.height.saturating_sub(1),
        width,
        1,
    );
    frame.render_widget(Clear, badge);
    frame.render_widget(
        Paragraph::new(Line::from(Span::styled(
            label,
            Style::default()
                .fg(Color::Black)
                .bg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ))),
        badge,
    );
}

/// Spinner frames — the standard "dots" set. Rotate with `app.spinner_frame`.
const SPINNER_FRAMES: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

/// Draw the status bar into the rows reserved for it.
fn draw_bar(frame: &mut Frame<'_>, app: &App, box_area: Rect) {
    if box_area.height < BAR_HEIGHT || box_area.width < 2 {
        return;
    }

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));
    let inner = block.inner(box_area);
    frame.render_widget(block, box_area);

    // Count only services for the filter badge — tasks and synthetic
    // streams (don/bazel) are filterable too, but the bar should
    // echo what the user thinks of as "my services." Lazy services are
    // excluded so the denominator matches `counts.services_total` (which
    // excludes Lazy for the same "not-yet-started" reason).
    let countable = || {
        app.services_state
            .iter()
            .filter(|(_, s)| !matches!(s, ServiceState::Lazy))
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
            app.debug_view,
            app.has_failure_summary(),
            !app.log_selection.is_empty(),
        )
    };
    // OSC 52 has no acknowledgement, so this line is the only sign a copy
    // happened. It takes the right-hand slot over the update badge: the user
    // just acted, and an answer to that beats a background notice.
    let copy_badge = app.copy_notice.as_ref().map(|(notice, _)| {
        Line::from(Span::styled(
            format!(" {notice} "),
            Style::default()
                .fg(Color::Black)
                .bg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ))
    });
    let update_badge = copy_badge.or_else(|| {
        (!app.shutdown_started)
            .then(|| app.update_badge.as_ref().map(update_badge_line))
            .flatten()
    });

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

fn draw_filter_modal(frame: &mut Frame<'_>, app: &App, area: Rect) {
    if area.height < 3 || area.width == 0 {
        return;
    }

    // Border + title wraps the whole panel. Inside: list at top, bar at bottom.
    let title = match app.filter.focus() {
        FilterFocus::List => " filter — [space] toggle  [o] only  [/] search  [R] reset ",
        FilterFocus::Query => " filter — [type] search  [enter] apply  [esc] clear ",
    };
    let outer = Block::default()
        .borders(Borders::ALL)
        .border_style(panel_border_style(app))
        .title(title);
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

fn draw_tasks_table(frame: &mut Frame<'_>, app: &App, area: Rect) {
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
            title: table_title(app, "tasks", "[enter] run  [a] attach  [/] filter"),
            border_style: panel_border_style(app),
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
/// Panel width below which the PID column is dropped.
///
/// A narrow panel is for names and states; a pid squeezed in leaves neither
/// the name nor the failure detail room to say anything. Wide enough to want
/// it back, it returns — the same shape the task table's columns follow.
const SERVICES_PID_MIN_WIDTH: u16 = 60;

fn draw_services_table(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let show_pid = area.width >= SERVICES_PID_MIN_WIDTH;
    let labels = if show_pid {
        vec!["NAME", "PID", "STATE"]
    } else {
        vec!["NAME", "STATE"]
    };
    let widths = if show_pid {
        vec![
            Constraint::Percentage(45),
            Constraint::Length(10),
            Constraint::Percentage(55),
        ]
    } else {
        vec![Constraint::Percentage(45), Constraint::Percentage(55)]
    };
    let header = Row::new(labels).style(
        Style::default()
            .add_modifier(Modifier::BOLD)
            .fg(Color::Cyan),
    );
    let name_colors = log_name_colors(app);
    let rows = app
        .service_items()
        .iter()
        .map(|item| service_table_row(item, &name_colors, show_pid))
        .collect();
    draw_status_table(
        frame,
        area,
        StatusTableView {
            title: table_title(
                app,
                "services",
                "[enter] start/stop  [r] restart  [a] attach",
            ),
            border_style: panel_border_style(app),
            header,
            rows,
            widths,
            state: &app.services_table,
            empty_label: "(no services)",
            selected_hint: service_selected_hint(app),
        },
    );
}

/// The side panel's border: cyan when it has the keys, grey when the log does.
/// The same rule the log pane uses, so focus is legible at a glance.
fn panel_border_style(app: &App) -> Style {
    Style::default().fg(if app.focus == super::panes::Focus::Panel {
        Color::Cyan
    } else {
        Color::DarkGray
    })
}

fn service_table_row(
    item: &OverlayItem,
    name_colors: &HashMap<String, Color>,
    show_pid: bool,
) -> Row<'static> {
    let name = item.name.clone();
    let state_cell = Cell::from(service_state_label(
        item.state,
        item.pid.is_some(),
        &item.failed_dependencies,
    ))
    .style(Style::default().fg(service_state_color(item.state)));
    let name_style = name_colors
        .get(&name)
        .copied()
        .map(|color| Style::default().fg(color))
        .unwrap_or_default();
    let mut cells = vec![Cell::from(name).style(name_style)];
    if show_pid {
        let pid = item
            .pid
            .map(|p| p.to_string())
            .unwrap_or_else(|| "-".to_string());
        cells.push(Cell::from(pid).style(Style::default().fg(Color::DarkGray)));
    }
    cells.push(state_cell);
    Row::new(cells)
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

#[allow(clippy::too_many_arguments)]
fn normal_bar_line(
    counts: &StatusCounts,
    filter: &FilterState,
    spinner_frame: usize,
    visible_services: usize,
    total_services: usize,
    debug_view: bool,
    has_failure_summary: bool,
    has_selection: bool,
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
    if has_selection {
        // Copying is explicit now, so the key that does it has to be visible
        // at the moment there is something to copy.
        spans.push(Span::styled(
            "[y] copy",
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ));
        spans.push(dim("  [esc] clear"));
        spans.push(separator());
    }
    spans.push(dim("[f] filter"));
    if filter.is_active() {
        spans.push(dim(format!(" ({visible_services}/{total_services})")));
        spans.push(dim("  [R] reset"));
    }
    // Parked tasks are easy to miss as a lone `*`, so tint the whole hotkey.
    if counts.tasks_pending_run > 0 {
        spans.push(Span::styled(
            "  [t] tasks",
            Style::default().fg(Color::Yellow),
        ));
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
    // Says which record is on screen, but never how to get to it. Reaching
    // don's own log is for when you have gone looking for why something did
    // not rebuild; the bar's slots belong to the things a reader needs
    // without being told.
    if debug_view {
        spans.push(separator());
        spans.push(dim("don's log"));
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

/// What to call each state in the table.
///
/// `live` is whether the service still has a process, and it changes what
/// `Failed` means. A service whose ready check failed under `on_failure =
/// "notify"` is left running on purpose and may well be serving traffic —
/// calling that "failed" beside its own pid reads as a contradiction. It is
/// not "unhealthy" either: that state means the service *was* ready and its
/// health monitor has since started failing, which is a different thing that
/// has already earned its own word.
fn service_state_label(
    state: ServiceState,
    live: bool,
    failed_dependencies: &[String],
) -> Cow<'static, str> {
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
        ServiceState::Failed if live => Cow::Borrowed("ready check failed"),
        ServiceState::Failed => Cow::Borrowed("failed"),
        ServiceState::DependencyFailed => dependency_failed_label(failed_dependencies),
    }
}

fn task_state_label(state: TaskState, failed_dependencies: &[String]) -> Cow<'static, str> {
    match state {
        TaskState::Pending => Cow::Borrowed("pending"),
        TaskState::Building => Cow::Borrowed("building"),
        TaskState::Running => Cow::Borrowed("running"),
        TaskState::Completed => Cow::Borrowed("completed"),
        TaskState::Skipped => Cow::Borrowed("skipped"),
        TaskState::Failed => Cow::Borrowed("failed"),
        TaskState::DependencyFailed => dependency_failed_label(failed_dependencies),
        TaskState::PendingRun => Cow::Borrowed("pending run"),
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

fn task_state_color(state: TaskState) -> Color {
    match state {
        TaskState::Completed | TaskState::Skipped => Color::Green,
        TaskState::Running | TaskState::Pending | TaskState::Building => Color::Yellow,
        TaskState::PendingRun => Color::Cyan,
        TaskState::Failed => Color::Red,
        TaskState::DependencyFailed => Color::Rgb(150, 60, 60),
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

    /// The tables say it too, because that is where `l` was pressed.
    ///
    /// The log pane names the process it is showing, but a reader who has just
    /// pressed `l` is looking at the table — and `l` is advertised there, so
    /// the key that undoes it belongs in the same line.
    #[test]
    fn a_table_title_offers_the_way_out_of_a_narrow() {
        let mut app = super::super::app::App::new(super::super::app::AppInit {
            service_names: vec!["api".to_string(), "web".to_string()],
            task_names: Vec::new(),
            build_tool_names: Vec::new(),
            task_configs: std::collections::HashMap::new(),
            task_last_runs: std::collections::HashMap::new(),
            hidden_names: std::collections::HashSet::new(),
            auto_filter_on_failure_names: std::collections::HashSet::new(),
            cli_log_filter: None,
        });
        assert_eq!(
            table_title(&app, "services", "[enter] start/stop"),
            " services — [enter] start/stop  [l] logs "
        );

        assert!(app.narrow_log_to("api"));
        let narrowed = table_title(&app, "services", "[enter] start/stop");
        assert_eq!(narrowed, " services — [esc] all logs  [enter] start/stop ");
        assert!(
            narrowed.find("[esc]").unwrap() < narrowed.find("[enter]").unwrap(),
            "the way out leads, so a truncated title still carries it: {narrowed}"
        );

        assert!(app.widen_log_from_narrow());
        assert_eq!(
            table_title(&app, "services", "[enter] start/stop"),
            " services — [enter] start/stop  [l] logs ",
            "and back again"
        );
    }

    /// A narrowed pane says so, and says how to undo it.
    ///
    /// `l` hides most of the log, and the rows it hides cannot announce their
    /// own absence — without this the pane looks like a project that has gone
    /// quiet.
    #[test]
    fn the_log_pane_title_says_when_it_is_narrowed() {
        struct Case {
            name: &'static str,
            narrow_to: Option<&'static str>,
            debug_view: bool,
            want: &'static str,
        }

        for case in [
            Case {
                name: "showing everything",
                narrow_to: None,
                debug_view: false,
                want: " logs ",
            },
            Case {
                name: "narrowed to one process",
                narrow_to: Some("api"),
                debug_view: false,
                want: " logs — api  [esc] all ",
            },
            Case {
                // The diagnostics record is its own store with its own
                // filter; `l` does not narrow it and must not claim to.
                name: "don's own log is never the narrowed one",
                narrow_to: Some("api"),
                debug_view: true,
                want: " don's log ",
            },
        ] {
            let mut app = super::super::app::App::new(super::super::app::AppInit {
                service_names: vec!["api".to_string(), "web".to_string()],
                task_names: Vec::new(),
                build_tool_names: Vec::new(),
                task_configs: std::collections::HashMap::new(),
                task_last_runs: std::collections::HashMap::new(),
                hidden_names: std::collections::HashSet::new(),
                auto_filter_on_failure_names: std::collections::HashSet::new(),
                cli_log_filter: None,
            });
            if let Some(name) = case.narrow_to {
                assert!(app.narrow_log_to(name), "{}: narrowing", case.name);
            }
            app.debug_view = case.debug_view;
            assert_eq!(log_pane_title(&app), case.want, "{}", case.name);
        }
    }

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

    /// "failed" printed next to a live pid reads as a contradiction, and the
    /// service it happens to is one that may be serving traffic right now.
    #[test]
    fn a_failure_that_is_still_running_says_which_check_failed() {
        struct Case {
            name: &'static str,
            state: ServiceState,
            live: bool,
            want: &'static str,
        }

        let cases = [
            Case {
                name: "the process is gone, so it simply failed",
                state: ServiceState::Failed,
                live: false,
                want: "failed",
            },
            Case {
                name: "on_failure = notify leaves it running",
                state: ServiceState::Failed,
                live: true,
                want: "ready check failed",
            },
            Case {
                // A different situation with its own word: this one *was*
                // ready. Liveness must not blur the two together.
                name: "unhealthy is unaffected by liveness",
                state: ServiceState::Unhealthy,
                live: true,
                want: "unhealthy",
            },
            Case {
                name: "and so is everything else",
                state: ServiceState::Ready,
                live: true,
                want: "ready",
            },
        ];

        for case in cases {
            assert_eq!(
                service_state_label(case.state, case.live, &[]),
                case.want,
                "{}",
                case.name
            );
        }
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

        let text = line_text(normal_bar_line(
            &StatusCounts::default(),
            &filter,
            0,
            1,
            2,
            false,
            false,
            false,
        ));

        assert!(text.contains("[f] filter (1/2)  [R] reset"));
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
                false,
            ));

            assert!(text.contains(case.want), "case: {}", case.name);
            assert!(!text.contains(case.reject), "case: {}", case.name);
        }
    }

    /// The scroll badge appears only when rows are genuinely below the view.
    /// A selection pins the view at the tail without following, and a badge
    /// reading "scrolled — 0 rows below" there contradicts itself.
    #[test]
    fn scroll_badge_only_shows_when_rows_are_below() {
        use crate::output::{FormattedLogLine, LogId};

        struct Case {
            name: &'static str,
            scroll: super::super::logs::Scroll,
            want_badge: bool,
        }

        let cases = [
            Case {
                name: "following: no badge",
                scroll: super::super::logs::Scroll::Follow,
                want_badge: false,
            },
            Case {
                name: "pinned at the tail: nothing below, no badge",
                scroll: super::super::logs::Scroll::At {
                    id: LogId(29),
                    row: 0,
                },
                want_badge: false,
            },
            Case {
                name: "held above the tail: badge with a real count",
                scroll: super::super::logs::Scroll::At {
                    id: LogId(0),
                    row: 0,
                },
                want_badge: true,
            },
        ];

        for case in cases {
            let mut app = App::new(AppInit {
                service_names: vec!["api".to_string()],
                task_names: Vec::new(),
                build_tool_names: Vec::new(),
                task_configs: HashMap::new(),
                task_last_runs: HashMap::new(),
                hidden_names: std::collections::HashSet::new(),
                auto_filter_on_failure_names: std::collections::HashSet::new(),
                cli_log_filter: None,
            });
            app.log_scroll = case.scroll;

            let mut store = super::super::log_store::LogStore::with_capacity(100);
            store.reflow(58);
            for id in 0..30u64 {
                store.push(
                    LogId(id),
                    FormattedLogLine {
                        name: "api".to_string(),
                        is_lifecycle: false,
                        is_verbose: false,
                        prefix: b"api \xe2\x94\x82 ".to_vec(),
                        bytes: format!("line {id}").into_bytes(),
                    },
                );
            }

            let backend = ratatui::backend::TestBackend::new(60, 12);
            let mut terminal = ratatui::Terminal::new(backend).unwrap();
            terminal
                .draw(|frame| draw(frame, &mut app, &store))
                .unwrap();
            let text: String = terminal
                .backend()
                .buffer()
                .content
                .iter()
                .map(|cell| cell.symbol())
                .collect();

            assert_eq!(text.contains("scrolled"), case.want_badge, "{}", case.name);
        }
    }

    /// A narrow panel drops the PID column: names and states are what a
    /// glance needs, and a pid squeezed in leaves neither room to say
    /// anything. Wide enough to want it back, it returns.
    #[test]
    fn services_pid_column_hides_below_the_width_threshold() {
        struct Case {
            width: u16,
            want_pid: bool,
        }

        let cases = [
            Case {
                width: 48,
                want_pid: false,
            },
            Case {
                width: SERVICES_PID_MIN_WIDTH,
                want_pid: true,
            },
            Case {
                width: 90,
                want_pid: true,
            },
        ];

        for case in cases {
            let mut app = App::new(AppInit {
                service_names: vec!["api".to_string()],
                task_names: Vec::new(),
                build_tool_names: Vec::new(),
                task_configs: HashMap::new(),
                task_last_runs: HashMap::new(),
                hidden_names: std::collections::HashSet::new(),
                auto_filter_on_failure_names: std::collections::HashSet::new(),
                cli_log_filter: None,
            });
            app.apply_service_runtime(
                "api".to_string(),
                ServiceState::Ready,
                Some(4242),
                Vec::new(),
            );
            app.view_mode = ViewMode::Services;

            let backend = ratatui::backend::TestBackend::new(case.width, 8);
            let mut terminal = ratatui::Terminal::new(backend).unwrap();
            terminal
                .draw(|frame| draw_services_table(frame, &app, frame.area()))
                .unwrap();
            let text: String = terminal
                .backend()
                .buffer()
                .content
                .iter()
                .map(|cell| cell.symbol())
                .collect();

            assert_eq!(
                text.contains("PID"),
                case.want_pid,
                "width {}: PID header",
                case.width
            );
            assert_eq!(
                text.contains("4242"),
                case.want_pid,
                "width {}: the pid itself",
                case.width
            );
            assert!(
                text.contains("api") && text.contains("ready"),
                "width {}: name and state always present",
                case.width
            );
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
        });
        app.apply_task_state(
            "configure-everything".to_string(),
            TaskState::DependencyFailed,
            None,
            vec!["configure-kafka-topics".to_string()],
        );
        app.view_mode = ViewMode::Tasks;

        // The width under test is the *panel's* width now, not the terminal's:
        // the table lives in the side panel and adapts its columns to however
        // wide the user has dragged it.
        let backend = ratatui::backend::TestBackend::new(width, 8);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| draw_tasks_table(frame, &app, frame.area()))
            .unwrap();
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
        let store = super::super::log_store::LogStore::with_capacity(0);
        terminal
            .draw(|frame| draw(frame, &mut app, &store))
            .unwrap();
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
            state: TaskState::Completed,
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
