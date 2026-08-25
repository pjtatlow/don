//! The log pane's view over [`LogStore`].
//!
//! The store holds every line don sent this client, unfiltered. What the user
//! sees is a *view*: the subset the filter admits, wrapped to the pane's width,
//! positioned by a scroll anchor. Nothing is thrown away to render, which is
//! why changing the filter is free — it selects differently over the same
//! store instead of wiping the screen and replaying into it.
//!
//! ## Anchoring
//!
//! Scroll position is a [`LogId`] plus a row offset *within* that logical line,
//! never an absolute row count. Rows are a function of width: the same log line
//! is one row at 200 columns and four at 60. An offset measured in rows would
//! therefore mean something different after every resize, and the view would
//! jump. An id survives resizes, eviction of older lines, and filter changes.
//!
//! Following is the default and is its own state rather than "anchored to the
//! last line" — a distinction that matters when new lines arrive, since the
//! anchor would otherwise need rewriting on every push.

use ratatui::text::Line;

use super::log_store::LogStore;
use crate::output::LogId;

/// Where the log pane is looking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Scroll {
    /// Pinned to the newest line. New output pushes the view along.
    #[default]
    Follow,
    /// Held at a position the user chose. New output does not move it.
    At {
        /// The logical line at the top of the pane.
        id: LogId,
        /// How many wrapped rows of that line are above the top edge. Zero
        /// unless a line taller than the pane is scrolled through.
        row: u16,
    },
}

impl Scroll {
    /// The line the view is held at, if it is held at one.
    ///
    /// The store keeps history from here onward rather than evicting it under
    /// the reader — see [`LogStore::set_pin`].
    pub(crate) fn anchor(self) -> Option<LogId> {
        match self {
            Scroll::Follow => None,
            Scroll::At { id, .. } => Some(id),
        }
    }
}

/// What the renderer needs to paint the pane, and what the input layer needs
/// to know about how far it can move.
pub(crate) struct LogView<'a> {
    /// Wrapped rows, top of pane first, exactly `height` of them or fewer if
    /// the whole log is shorter than the pane.
    pub(crate) rows: Vec<Line<'a>>,
    /// Where each row came from, one per entry in `rows`.
    ///
    /// Reported by the code that did the wrapping rather than recovered later
    /// by looking at the text, which is a guess about layout the layout already
    /// knows the answer to.
    pub(crate) row_sources: Vec<RowSource>,
    /// Whether the view is pinned to the newest line.
    pub(crate) following: bool,
    /// Rows of admitted content above the top edge — the scrollbar's position.
    pub(crate) rows_above: usize,
    /// Total rows the admitted content occupies at this width.
    pub(crate) total_rows: usize,
}

/// What a rendered row is a view of: a place in the log, not a place on the
/// screen.
///
/// A message can wrap across several rows, and the reader thinks of it as one
/// thing — triple-click takes the message, not the row it landed on, and a
/// selection is a range of *text*, which is why `offset` is here. Screen rows
/// move under the reader constantly; `(id, offset)` does not move at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RowSource {
    /// The log line this row is part of.
    pub(crate) id: LogId,
    /// How many characters of that line's message come before this row.
    pub(crate) offset: usize,
    /// Columns of don's own prefix — the real one on a message's first row, an
    /// indent on the rest — before the message starts.
    pub(crate) indent: usize,
}

impl RowSource {
    /// The message offset a screen column on this row points at.
    ///
    /// Columns inside the prefix clamp to the row's start: the name column is
    /// don's furniture and pasting `api    │ ` in front of every line is never
    /// what anyone wanted.
    pub(crate) fn offset_at(self, column: usize) -> usize {
        self.offset + column.saturating_sub(self.indent)
    }
}

/// Message characters one row can hold at this width, and the prefix width the
/// pane actually used.
///
/// Mirrors the degenerate-width fallback in [`wrap_line`]: a pane too narrow to
/// hold the prefix and anything else drops the prefix, and the offsets have to
/// agree with the rows that produced them.
pub(crate) fn row_metrics(prefix_cols: usize, width: u16) -> (usize, usize) {
    let width = usize::from(width.max(1));
    let indent = if prefix_cols + 1 >= width {
        0
    } else {
        prefix_cols
    };
    (width - indent, indent)
}

/// The left column on a row that is a continuation of the row above.
///
/// Blank where the name would be, but the separator is carried down, so the
/// boundary between the two columns is a line the eye can follow rather than
/// something that only exists on whichever rows happen to start a message.
fn continuation_indent(prefix_cols: usize) -> String {
    match prefix_cols.checked_sub(2) {
        Some(pad) => format!("{}│ ", " ".repeat(pad)),
        None => " ".repeat(prefix_cols),
    }
}

/// Split one already-styled line into rows, holding the prefix in its own
/// column.
///
/// The first row carries the real `name | ` prefix; every row after it is
/// indented to the same width so the message forms a single column down the
/// pane. Wrapping a line back to column zero is what makes a wall of output
/// unreadable — you cannot tell a continuation from a new line, or see which
/// process said what without tracing upwards.
///
/// Wrapping is done here rather than by ratatui's `Wrap` because the pane needs
/// the row *count* before it renders — to place the scroll anchor, to size the
/// scrollbar, and to know whether it is at the bottom. Asking the widget after
/// the fact would be a frame too late.
pub(crate) fn wrap_line<'a>(line: &Line<'a>, prefix_cols: usize, width: u16) -> Vec<Line<'a>> {
    let width = width.max(1) as usize;
    // A pane too narrow to hold the prefix and anything else falls back to
    // plain full-width wrapping; a zero-width message column cannot progress.
    let prefix_cols = if prefix_cols + 1 >= width {
        0
    } else {
        prefix_cols
    };

    let mut rows: Vec<Line<'a>> = Vec::new();
    let mut current: Vec<ratatui::text::Span<'a>> = Vec::new();
    let mut used = 0usize;
    // Columns consumed so far across the whole logical line, so we know when we
    // are still inside the prefix.
    let mut seen = 0usize;

    for span in &line.spans {
        let mut rest: &str = span.content.as_ref();
        while !rest.is_empty() {
            if used >= width {
                rows.push(Line::from(std::mem::take(&mut current)));
                current.push(ratatui::text::Span::raw(continuation_indent(prefix_cols)));
                used = prefix_cols;
            }
            let room = width - used;
            // Never break inside the prefix: it is one cell of the column.
            let room = if seen < prefix_cols {
                room.min(prefix_cols - seen)
            } else {
                room
            };
            let take = rest
                .char_indices()
                .take(room)
                .last()
                .map(|(idx, ch)| idx + ch.len_utf8())
                .unwrap_or(rest.len());
            let (head, tail) = rest.split_at(take);
            let taken = head.chars().count();
            current.push(ratatui::text::Span::styled(head.to_string(), span.style));
            used += taken;
            seen += taken;
            rest = tail;
        }
    }
    if !current.is_empty() || rows.is_empty() {
        rows.push(Line::from(current));
    }
    rows
}

/// How many rows [`wrap_line`] would produce, without producing them.
///
/// The store measures every line it ingests, and only ever paints a screenful
/// — so measuring by wrapping meant allocating the full wrapped form of every
/// line and dropping it, on every push and every reflow.
///
/// Every row reserves `prefix_cols` — the real prefix on the first, an indent on
/// the rest — so the message column is the same width throughout and the count
/// is just the message over that width.
///
/// Kept beside `wrap_line` and pinned against it by a test, because the two
/// disagreeing would put the scroll anchor somewhere the content isn't.
pub(crate) fn count_wrapped_rows(line: &Line<'_>, prefix_cols: usize, width: u16) -> usize {
    let width = width.max(1) as usize;
    let prefix_cols = if prefix_cols + 1 >= width {
        0
    } else {
        prefix_cols
    };
    let chars: usize = line
        .spans
        .iter()
        .map(|span| span.content.chars().count())
        .sum();
    let body = chars.saturating_sub(prefix_cols);
    body.div_ceil(width - prefix_cols).max(1)
}

/// Build the visible rows for a pane of `width` × `height`.
///
/// Which lines are in view is the index's decision, not this function's: it
/// selects the admitted lines and counts their rows, and this walks that
/// selection and paints it. Deciding twice — counting rows from the index and
/// then re-filtering the store while drawing — is how the two drifted apart,
/// which reads as a view that jumps somewhere other than where it was scrolled.
///
/// The filter lives in the index rather than at ingest because the store keeps
/// everything: widening the filter reveals history that a render-time filter
/// would have thrown away, which is why a filter change no longer wipes the
/// screen and replays into it.
///
/// The store must have been reflowed to `width` first; row counts come from its
/// cache, and only the rows that actually land on screen are wrapped.
pub(crate) fn build_view<'a>(
    store: &'a LogStore,
    index: &super::view_index::ViewIndex,
    blanks: &std::collections::HashMap<LogId, u16>,
    scroll: Scroll,
    width: u16,
    height: u16,
) -> LogView<'a> {
    let height = height.max(1) as usize;
    let total_rows = index.total_rows();
    let max_above = total_rows.saturating_sub(height);

    let rows_above = match scroll {
        Scroll::Follow => max_above,
        Scroll::At { id, row } => match index.rows_above(id) {
            Some(above) => (above + row as usize).min(max_above),
            // The anchor was evicted or filtered away. The oldest line that
            // survives is where the reader was heading, not the live tail —
            // being yanked to the bottom because history aged out from under
            // you is the "jumpy" a busy log produces once it is at capacity.
            None => 0,
        },
    };

    // Only the visible window is wrapped, and only from the line that owns the
    // top row — no walk of everything above it.
    let mut rows: Vec<Line<'a>> = Vec::with_capacity(height);
    let mut row_sources: Vec<RowSource> = Vec::with_capacity(height);
    if let Some((first_id, skip_within)) = index.line_at(rows_above) {
        let mut skip = usize::from(skip_within);
        for entry in index.ids_from(first_id).filter_map(|id| store.get(id)) {
            let mut line_rows = wrap_line(&entry.parsed, entry.prefix_cols(), width);
            let (per_row, indent) = row_metrics(entry.prefix_cols(), width);
            let wrapped_rows = line_rows.len();
            // The blank the reader asked for belongs to this line, so it scrolls
            // with it and the index counted it as one of its rows.
            for _ in 0..blanks.get(&entry.id).copied().unwrap_or(0) {
                line_rows.push(Line::default());
            }
            for (row, wrapped) in line_rows.into_iter().enumerate().skip(skip) {
                rows.push(wrapped);
                row_sources.push(RowSource {
                    id: entry.id,
                    // A blank the reader asked for is not part of the message,
                    // so it points at the end of it rather than past it.
                    offset: row.min(wrapped_rows.saturating_sub(1)) * per_row,
                    indent,
                });
                if rows.len() == height {
                    return LogView {
                        rows,
                        row_sources,
                        following: matches!(scroll, Scroll::Follow),
                        rows_above,
                        total_rows,
                    };
                }
            }
            skip = 0;
        }
    }

    LogView {
        rows,
        row_sources,
        following: matches!(scroll, Scroll::Follow),
        rows_above,
        total_rows,
    }
}

/// The anchor for whichever line currently owns row `rows_above`.
///
/// The lookup [`scrolled`] does, without its "at the bottom means follow"
/// shortcut — because pinning the view *while* it sits at the bottom is exactly
/// what this is for. Selecting text freezes the view, so that the rows under
/// the selection stay the rows the user dragged across; without the freeze, one
/// line of new output invalidates the whole thing.
pub(crate) fn anchor_at(index: &super::view_index::ViewIndex, rows_above: usize) -> Scroll {
    match index.line_at(rows_above) {
        Some((id, row)) => Scroll::At { id, row },
        None => Scroll::Follow,
    }
}

/// Turn the reader's pending intent into an anchor, against the geometry that
/// exists right now.
///
/// The one place scroll position is decided. It runs while drawing, because
/// that is the only moment all three inputs are true at once: how much admitted
/// content there is, where the view sits in it, and how tall the pane is.
/// Deciding at input time meant reading what the previous frame measured — and
/// a verbose toggle rebuilds the index, so one arrow key could move the view by
/// the difference between two entirely different filters.
pub(crate) fn resolve_scroll(
    index: &super::view_index::ViewIndex,
    scroll: Scroll,
    pending: super::app::PendingScroll,
    height: u16,
) -> Scroll {
    if pending.is_empty() {
        return scroll;
    }
    let height = height.max(1) as usize;
    let total_rows = index.total_rows();
    let max_above = total_rows.saturating_sub(height);

    // Where the view sits now, measured the same way the pane will measure it.
    let current = match scroll {
        Scroll::Follow => max_above,
        Scroll::At { id, row } => match index.rows_above(id) {
            Some(above) => (above + row as usize).min(max_above),
            None => 0,
        },
    };

    if pending.to_top {
        return anchor_at(index, 0);
    }
    // Pinning is "stop following, stay exactly here" — a selection starting
    // holds the rows it was dragged across still.
    if pending.pin && pending.rows == 0 && pending.pages == 0 {
        return anchor_at(index, current);
    }

    let page = isize::try_from(height.saturating_sub(1).max(1)).unwrap_or(1);
    let delta = pending
        .rows
        .saturating_add(pending.pages.saturating_mul(page));
    let target = current.saturating_add_signed(delta).min(max_above);
    if target >= max_above {
        return Scroll::Follow;
    }
    anchor_at(index, target)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    /// No blank marks — the default for a test that is not about them.
    fn no_marks() -> std::collections::HashMap<LogId, u16> {
        std::collections::HashMap::new()
    }
    use ratatui::text::Span;

    fn styled(text: &str) -> Line<'static> {
        Line::from(vec![Span::raw(text.to_string())])
    }

    fn row_text(line: &Line<'_>) -> String {
        line.spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<String>()
    }

    /// The rows the pane draws must be the rows the index counted. If the view
    /// renders lines the filter excludes while positioning itself by filtered
    /// row counts, every scroll lands somewhere else than it says.
    #[test]
    fn the_view_draws_only_what_the_filter_admits() {
        let mut store = LogStore::with_capacity(100);
        store.reflow(40);
        for (id, name, body) in [
            (0u64, "api", "api one"),
            (1, "web", "web one"),
            (2, "api", "api two"),
            (3, "web", "web two"),
        ] {
            store.push(
                LogId(id),
                crate::output::FormattedLogLine {
                    name: name.to_string(),
                    is_lifecycle: false,
                    is_verbose: false,
                    prefix: Vec::new(),
                    bytes: body.as_bytes().to_vec(),
                },
            );
        }
        let mut index = super::super::view_index::ViewIndex::default();
        index.sync(
            &store,
            super::super::view_index::ViewKey {
                width: 40,
                filter: 0,
            },
            &no_marks(),
            |entry: &super::super::log_store::StoredLogLine| entry.line.name == "api",
        );

        let view = build_view(&store, &index, &no_marks(), Scroll::Follow, 40, 10);
        let text: Vec<String> = view.rows.iter().map(row_text).collect();
        assert_eq!(
            text,
            vec!["api one".to_string(), "api two".to_string()],
            "the pane drew lines the filter excludes"
        );
    }

    /// The bug this indirection exists for: one arrow key must move one row,
    /// even when the view it is moving in was rebuilt since the last frame.
    ///
    /// Resolving at input time used the previous frame's row count, so toggling
    /// verbose — which admits or hides thousands of lines — made the next
    /// keypress land hundreds of rows from where it should.
    #[test]
    fn a_scroll_resolves_against_the_view_that_exists_now() {
        use super::super::app::PendingScroll;
        use crate::output::FormattedLogLine;

        // A store where half the lines are verbose, so a filter change moves
        // the row count a long way.
        let mut store = LogStore::with_capacity(1000);
        store.reflow(40);
        for id in 0..600u64 {
            store.push(
                LogId(id),
                FormattedLogLine {
                    name: "api".to_string(),
                    is_lifecycle: false,
                    is_verbose: id % 2 == 0,
                    prefix: b"api | ".to_vec(),
                    bytes: b"a line".to_vec(),
                },
            );
        }

        let height = 20u16;
        let quiet = |entry: &super::super::log_store::StoredLogLine| !entry.line.is_verbose;

        // Verbose on: everything admitted, following the tail.
        let mut index = super::super::view_index::ViewIndex::default();
        index.sync(&store, key_for(0), &no_marks(), |_| true);
        assert_eq!(index.total_rows(), 600);

        // Now verbose goes off and the index rebuilds — 300 rows, not 600 —
        // and only then does the arrow key get resolved.
        index.sync(&store, key_for(1), &no_marks(), quiet);
        assert_eq!(index.total_rows(), 300);

        let up_once = PendingScroll {
            rows: -1,
            ..PendingScroll::default()
        };
        let scroll = resolve_scroll(&index, Scroll::Follow, up_once, height);

        // One row above the tail of the *current* view: 300 rows, 20 visible,
        // so the tail sits at 280 and one up is 279.
        let view = build_view(&store, &index, &no_marks(), scroll, 40, height);
        assert_eq!(
            view.rows_above, 279,
            "one arrow key moves one row in the view that exists now"
        );
    }

    fn key_for(filter: u64) -> super::super::view_index::ViewKey {
        super::super::view_index::ViewKey { width: 40, filter }
    }

    /// The prefix is a column, not part of the text: what wraps lands under the
    /// message, not back at the left edge.
    #[test]
    fn wrapping_indents_under_the_message_column() {
        struct Case {
            name: &'static str,
            input: &'static str,
            /// Columns the name column takes — the sink tells the store this,
            /// so a test states it rather than recovering it from the text.
            prefix: usize,
            width: u16,
            want: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                name: "a continuation lines up under the message",
                // prefix "api | " is 6 columns, so the message column is 6 wide
                // at width 12.
                input: "api | abcdefghijkl",
                prefix: 6,
                width: 12,
                want: vec!["api | abcdef", "    │ ghijkl"],
            },
            Case {
                name: "three rows keep the same indent",
                input: "api | abcdefghijklmnopqr",
                prefix: 6,
                width: 12,
                want: vec!["api | abcdef", "    │ ghijkl", "    │ mnopqr"],
            },
            Case {
                name: "a short line is one row and is not padded out",
                input: "api | hi",
                prefix: 6,
                width: 12,
                want: vec!["api | hi"],
            },
            Case {
                name: "no prefix means no column, and it wraps full width",
                input: "abcdefghijklmn",
                prefix: 0,
                width: 12,
                want: vec!["abcdefghijkl", "mn"],
            },
            Case {
                name: "a pane too narrow for the column falls back to full width",
                input: "api | abcdef",
                prefix: 6,
                width: 6,
                want: vec!["api | ", "abcdef"],
            },
        ];

        for case in cases {
            let line = styled(case.input);
            let prefix = case.prefix;
            let rows = wrap_line(&line, prefix, case.width);
            let got: Vec<String> = rows.iter().map(row_text).collect();
            assert_eq!(got, case.want, "{}", case.name);
            assert_eq!(
                count_wrapped_rows(&line, prefix, case.width),
                rows.len(),
                "{}: the count must match what the wrap produced",
                case.name
            );
        }
    }

    /// Wrapping owns the row count, so it has to be exact: the pane places its
    /// scroll anchor and sizes its scrollbar from these numbers before anything
    /// is drawn.
    #[test]
    fn wrapping_splits_at_the_pane_width() {
        struct Case {
            name: &'static str,
            input: &'static str,
            width: u16,
            want: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                name: "a line that fits is one row",
                input: "short",
                width: 10,
                want: vec!["short"],
            },
            Case {
                name: "an exact fit does not spill into an empty row",
                input: "exactly10!",
                width: 10,
                want: vec!["exactly10!"],
            },
            Case {
                name: "a long line splits at the width",
                input: "abcdefghij",
                width: 4,
                want: vec!["abcd", "efgh", "ij"],
            },
            Case {
                name: "an empty line still occupies a row",
                input: "",
                width: 10,
                want: vec![""],
            },
            Case {
                name: "width of one degrades rather than looping",
                input: "abc",
                width: 1,
                want: vec!["a", "b", "c"],
            },
        ];

        for case in cases {
            let rows = wrap_line(&styled(case.input), 0, case.width);
            let got: Vec<String> = rows.iter().map(row_text).collect();
            assert_eq!(got, case.want, "{}", case.name);
        }
    }

    /// Multi-byte characters must not be split mid-codepoint, and must count as
    /// the columns they occupy rather than the bytes they take.
    #[test]
    fn wrapping_counts_characters_not_bytes() {
        let rows = wrap_line(&styled("äöüßé"), 0, 2);
        let got: Vec<String> = rows.iter().map(row_text).collect();
        assert_eq!(got, vec!["äö", "üß", "é"]);
    }

    /// Styling survives a split: a wrapped line keeps the colours the upstream
    /// formatter gave it, on both halves.
    #[test]
    fn wrapping_preserves_span_styles() {
        let line = Line::from(vec![
            Span::styled(
                "red".to_string(),
                ratatui::style::Style::default().fg(ratatui::style::Color::Red),
            ),
            Span::styled(
                "blue".to_string(),
                ratatui::style::Style::default().fg(ratatui::style::Color::Blue),
            ),
        ]);
        let rows = wrap_line(&line, 0, 4);
        assert_eq!(rows.len(), 2, "7 columns over a width of 4");
        assert_eq!(
            rows[0].spans[0].style.fg,
            Some(ratatui::style::Color::Red),
            "the first row keeps its colour"
        );
        assert_eq!(
            rows[1].spans.last().unwrap().style.fg,
            Some(ratatui::style::Color::Blue),
            "and so does the second"
        );
    }

    /// The count and the wrap must never disagree: the store places the scroll
    /// anchor from the count and the renderer paints from the wrap, so a
    /// mismatch puts the view somewhere the content is not.
    #[test]
    fn counting_rows_agrees_with_actually_wrapping() {
        struct Case {
            name: &'static str,
            spans: Vec<&'static str>,
            width: u16,
        }

        let cases = vec![
            Case {
                name: "empty",
                spans: vec![],
                width: 20,
            },
            Case {
                name: "one empty span",
                spans: vec![""],
                width: 20,
            },
            Case {
                name: "fits",
                spans: vec!["short"],
                width: 20,
            },
            Case {
                name: "exact fit",
                spans: vec!["exactly10!"],
                width: 10,
            },
            Case {
                name: "one over",
                spans: vec!["exactly10!x"],
                width: 10,
            },
            Case {
                name: "several rows",
                spans: vec!["abcdefghijklmnopqrst"],
                width: 4,
            },
            Case {
                name: "split across spans",
                spans: vec!["abc", "defgh", "ij"],
                width: 4,
            },
            Case {
                name: "multibyte",
                spans: vec!["äöüßé", "àèìòù"],
                width: 3,
            },
            Case {
                name: "width of one",
                spans: vec!["abcde"],
                width: 1,
            },
            Case {
                name: "a span boundary landing exactly on the width",
                spans: vec!["abcd", "efgh"],
                width: 4,
            },
        ];

        for case in cases {
            let line = Line::from(
                case.spans
                    .iter()
                    .map(|text| ratatui::text::Span::raw((*text).to_string()))
                    .collect::<Vec<_>>(),
            );
            // Both with and without a prefix column: the count and the wrap
            // must agree either way, since the anchor is placed from one and
            // the content drawn from the other.
            for prefix in [0, 6] {
                assert_eq!(
                    count_wrapped_rows(&line, prefix, case.width),
                    wrap_line(&line, prefix, case.width).len(),
                    "{} (prefix {prefix})",
                    case.name
                );
            }
        }
    }

    /// The pane must draw exactly the rows the index counted, through every
    /// combination of the things that change underneath it: lines arriving,
    /// eviction, blank marks the reader adds, the filter moving, and the width
    /// moving. Drift between the two is not a cosmetic bug — the view
    /// positions itself with one set of row counts and paints with another, so
    /// it lands somewhere other than where it says, which is what "the logs
    /// jumped" and "the logs vanished" both look like from outside.
    #[test]
    fn what_is_drawn_matches_what_the_index_counted() {
        use std::collections::HashMap;
        use std::hash::{Hash, Hasher};

        // A deterministic sequence, so a failure is reproducible.
        let mut seed = 0x5eed_1234_u64;
        let mut roll = move |n: usize| {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((seed >> 33) as usize) % n.max(1)
        };

        let mut store = LogStore::with_capacity(32);
        let mut index = super::super::view_index::ViewIndex::default();
        let mut blanks: HashMap<LogId, u16> = HashMap::new();
        let mut width = 40u16;
        let height = 12u16;
        let mut hide_web = false;
        let mut next_id = 0u64;
        let mut scroll = Scroll::Follow;

        for step in 0..300 {
            match roll(10) {
                0 => width = [20u16, 33, 40, 61, 80][roll(5)],
                1 => hide_web = !hide_web,
                _ => {}
            }
            // A burst of output, sometimes long enough to wrap several times.
            for _ in 0..roll(4) {
                let body = "x".repeat(1 + roll(90));
                let name = if roll(2) == 0 { "api" } else { "web" };
                store.push(
                    LogId(next_id),
                    crate::output::FormattedLogLine {
                        name: name.to_string(),
                        is_lifecycle: false,
                        is_verbose: false,
                        prefix: format!("{name:<5}│ ").into_bytes(),
                        bytes: body.into_bytes(),
                    },
                );
                next_id += 1;
            }
            store.reflow(width);
            store.set_pin(scroll.anchor());

            let admits = |entry: &super::super::log_store::StoredLogLine| {
                !(hide_web && entry.line.name == "web")
            };
            // The key the app builds, blank marks and all.
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            hide_web.hash(&mut hasher);
            let mut marks = 0u64;
            for mark in &blanks {
                let mut one = std::collections::hash_map::DefaultHasher::new();
                mark.hash(&mut one);
                marks = marks.wrapping_add(Hasher::finish(&one));
            }
            marks.hash(&mut hasher);
            let key = super::super::view_index::ViewKey {
                width,
                filter: hasher.finish(),
            };
            index.sync(&store, key, &blanks, admits);

            // What a walk of the store says, which is the definition the
            // mended index is only ever an optimisation of.
            let naive: usize = store
                .iter()
                .filter(|entry| admits(entry))
                .map(|entry| {
                    entry.wrapped_rows().max(1)
                        + usize::from(blanks.get(&entry.id).copied().unwrap_or(0))
                })
                .sum();
            assert_eq!(
                index.total_rows(),
                naive,
                "step {step}: index total vs walking the store"
            );

            let view = build_view(&store, &index, &blanks, scroll, width, height);
            assert_eq!(
                view.total_rows, naive,
                "step {step}: the view reports the index's total"
            );
            assert_eq!(
                view.rows.len(),
                view.row_sources.len(),
                "step {step}: a row id per row"
            );
            // Every row the index says is on screen must actually be painted.
            // Falling short here is the pane going blank or short; overshooting
            // is impossible (the builder stops at `height`).
            let expected = view
                .total_rows
                .saturating_sub(view.rows_above)
                .min(height as usize);
            assert_eq!(
                view.rows.len(),
                expected,
                "step {step}: rows drawn vs rows the index placed \
                 (width {width}, above {}, total {})",
                view.rows_above,
                view.total_rows
            );

            // The reader does things too: scrolls, and marks blank rows at the
            // bottom of what is on screen — which is what Enter does.
            match roll(6) {
                0 => scroll = Scroll::Follow,
                1 => scroll = anchor_at(&index, roll(view.total_rows.max(1))),
                2 => {
                    if let Some(id) = view.row_sources.last().map(|s| s.id) {
                        let count = blanks.entry(id).or_insert(0);
                        *count = count.saturating_add(1);
                    }
                }
                3 => {
                    // A wheel notch or an arrow, resolved the way the pane
                    // resolves it — against the geometry that exists now.
                    let pending = super::super::app::PendingScroll {
                        rows: (roll(21) as isize) - 10,
                        ..Default::default()
                    };
                    scroll = resolve_scroll(&index, scroll, pending, height);
                }
                _ => {}
            }
        }
    }
}
