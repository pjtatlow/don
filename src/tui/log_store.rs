//! The TUI's copy of don's merged log stream.
//!
//! Every line don sends this client lands here, unfiltered and unabridged.
//! What the user sees is a view over it (see [`super::logs`]); the store itself
//! never decides what is worth keeping, because the alternative — filtering at
//! ingest — makes widening a filter reveal nothing, since the lines it would
//! have shown were discarded on the way in.
//!
//! Entries are keyed by don's own [`LogId`], not by a number this store mints.
//! That is what lets two clients refer to the same line, lets a reconnecting
//! client ask for exactly what it missed, and lets a gap be reported as a
//! measured hole rather than a silent thinning.
//!
//! ## What is cached, and why
//!
//! Two things are expensive per frame and cheap per line:
//!
//! - **Parsing.** Upstream hands us pre-rendered ANSI bytes. Turning those into
//!   a styled [`Line`] is done once, at push. A full-screen repaint touches
//!   every visible line every frame, so parsing at render would tie frame cost
//!   to screen height *and* re-do identical work forever.
//! - **Wrapped row counts.** How many rows a line occupies depends on the pane
//!   width, and the pane needs the total before it draws — to place the scroll
//!   anchor and size the scrollbar. Recomputed only when the width changes.

use std::collections::VecDeque;

use ratatui::text::Line;

use crate::output::{FormattedLogLine, LogId};

/// Default cap on retained lines.
///
/// This is the TUI's scrollback now, not a replay cache for the terminal's own
/// history — there is no terminal history to fall back on in the alternate
/// screen. Sized to match don's merged store, so what the user can scroll to is
/// what don still holds.
pub(crate) const DEFAULT_CAPACITY: usize = crate::output::DEFAULT_MERGED_HISTORY_CAPACITY;

/// Cap on retained diagnostics.
///
/// don's own narration is for answering "why did that not rebuild" in the
/// moment, not for scrolling back through, so it gets a smaller budget than
/// the processes' output.
///
/// Five hundred was too small to read at. A reader who scrolls back holds
/// their place with a pin bounded by [`PINNED_OVERDRAFT`], so once the buffer
/// has taken `capacity * PINNED_OVERDRAFT` lines it evicts out from under them
/// — their anchor goes, the view falls back to the oldest surviving line, and
/// since that keeps moving the pane empties while the new lines pile up below
/// the window they are looking at. At five hundred a single busy moment
/// crossed that in under a second. This is sized so that reading the
/// diagnostics while a stack is busy is possible at all.
pub(crate) const DEBUG_CAPACITY: usize = 5_000;

/// How far past `capacity` the store will grow to keep a scrolled reader's
/// place. Beyond this their anchor is dropped like anything else — a reader who
/// has walked away is not worth an unbounded buffer.
const PINNED_OVERDRAFT: usize = 2;

/// A bounded, time-ordered buffer of the merged stream.
pub(crate) struct LogStore {
    entries: VecDeque<StoredLogLine>,
    capacity: usize,
    next_id: LogId,
    /// The width `wrapped_rows` was computed against. `None` before the first
    /// reflow.
    wrapped_at: Option<u16>,
    /// The oldest id the view still needs, set while the reader is scrolled
    /// back. See [`Self::set_pin`].
    pin: Option<LogId>,
    /// Columns the name column takes. Every prefix the sink renders is padded
    /// to the same width, so one number describes the whole pane; lines with no
    /// prefix at all (the TUI's own notices) do not move it.
    name_column: usize,
}

/// One stored line: what don sent, parsed once, measured once.
pub(crate) struct StoredLogLine {
    pub(crate) id: LogId,
    pub(crate) line: FormattedLogLine,
    /// The styled form, parsed at push. Owned (`'static`) so the store can hand
    /// it out without tying the borrow to the raw bytes.
    pub(crate) parsed: Line<'static>,
    /// Rows this line occupies at the store's current wrap width.
    wrapped_rows: usize,
    /// Columns the `name | ` prefix occupies, so the pane can hold it in its
    /// own column and indent what wraps underneath it. Measured from the
    /// prefix the sink sent, not recovered from the text.
    prefix_cols: usize,
    /// The message as plain text, built once here for the same reason the
    /// parse and the row count are: `/` asks every line in the store whether
    /// it matches on every keystroke, and building this per question meant an
    /// allocation per line per character typed.
    message: String,
}

impl StoredLogLine {
    /// Rows this line occupies at the width the store last reflowed to.
    pub(crate) fn wrapped_rows(&self) -> usize {
        self.wrapped_rows
    }

    /// Columns the process-name column takes on every row of this line.
    pub(crate) fn prefix_cols(&self) -> usize {
        self.prefix_cols
    }

    /// The message as plain text, without don's `name │ ` column.
    ///
    /// Taken from the parsed form rather than the raw bytes so the offsets it
    /// yields are the ones the pane laid out — the raw bytes still carry the
    /// escape sequences that became styles, and counting through those would
    /// put every offset past the first colour in the wrong place.
    pub(crate) fn message_text(&self) -> &str {
        &self.message
    }
}

impl LogStore {
    /// Create a new store with the given capacity. A capacity of 0 silently
    /// drops every push.
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            capacity,
            next_id: LogId::ZERO,
            wrapped_at: None,
            pin: None,
            name_column: 0,
        }
    }

    /// Hold history from `id` onward while the reader is scrolled back there.
    ///
    /// Without this the buffer evicts under a reader who is looking at it: the
    /// line they anchored to ages out, the view falls back to the oldest line
    /// that survives, and since that keeps changing the pane walks away from
    /// them. How fast depends only on the line rate — at fifty thousand lines
    /// of capacity and fifty thousand lines a second, an anchor lasts about a
    /// second, which is what a stack in a logging feedback loop felt like.
    ///
    /// `None` while following, which is the normal state and evicts as usual.
    /// Bounded by [`PINNED_OVERDRAFT`], so a reader who scrolls up and leaves
    /// cannot grow this without limit.
    pub(crate) fn set_pin(&mut self, pin: Option<LogId>) {
        self.pin = pin;
    }

    /// Store a line under the id don's merged stream gave it, evicting the
    /// oldest if at capacity.
    pub(crate) fn push(&mut self, id: LogId, line: FormattedLogLine) {
        self.next_id = LogId(id.0.saturating_add(1));
        if self.capacity == 0 {
            return;
        }
        // The two halves arrive already separated, so the column boundary is
        // known rather than recovered: parse the prefix on its own to measure
        // it, then the whole row for rendering.
        let prefix_cols = super::parse_ansi_line(&line.prefix)
            .spans
            .iter()
            .map(|span| span.content.chars().count())
            .sum();
        if prefix_cols > 0 {
            self.name_column = prefix_cols;
        }
        let mut joined = Vec::with_capacity(line.prefix.len() + line.bytes.len());
        joined.extend_from_slice(&line.prefix);
        joined.extend_from_slice(&line.bytes);
        let parsed = super::parse_ansi_line(&joined);
        let wrapped_rows = match self.wrapped_at {
            Some(width) => super::logs::count_wrapped_rows(&parsed, prefix_cols, width),
            None => 1,
        };
        let ceiling = self.capacity.saturating_mul(PINNED_OVERDRAFT);
        while self.entries.len() >= self.capacity {
            let holding_the_readers_place = self.entries.len() < ceiling
                && match (self.pin, self.entries.front()) {
                    (Some(pin), Some(front)) => front.id >= pin,
                    _ => false,
                };
            if holding_the_readers_place {
                break;
            }
            self.entries.pop_front();
        }
        let message = parsed
            .spans
            .iter()
            .flat_map(|span| span.content.chars())
            .skip(prefix_cols)
            .collect();
        self.entries.push_back(StoredLogLine {
            id,
            line,
            parsed,
            wrapped_rows,
            prefix_cols,
            message,
        });
    }

    /// Recompute cached row counts for a new pane width.
    ///
    /// A no-op when the width has not moved, which is every frame but the ones
    /// straddling a resize.
    pub(crate) fn reflow(&mut self, width: u16) {
        if self.wrapped_at == Some(width) {
            return;
        }
        self.wrapped_at = Some(width);
        for entry in &mut self.entries {
            entry.wrapped_rows =
                super::logs::count_wrapped_rows(&entry.parsed, entry.prefix_cols, width);
        }
    }

    /// Iterate oldest-first over everything held.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &StoredLogLine> {
        self.entries.iter()
    }

    /// The oldest id still held, for a reader deciding what it has missed.
    pub(crate) fn oldest_id(&self) -> Option<LogId> {
        self.entries.front().map(|entry| entry.id)
    }

    /// Iterate from the first entry with `id >= from`.
    ///
    /// A binary search rather than a scan: ids ascend, and the callers that
    /// want this — the view index mending itself, the renderer taking the
    /// visible window — would otherwise walk the whole store to reach a
    /// screenful near the end of it.
    pub(crate) fn iter_from(&self, from: LogId) -> impl Iterator<Item = &StoredLogLine> {
        let at = self.entries.partition_point(|entry| entry.id < from);
        self.entries.iter().skip(at)
    }

    /// The line stored under `id`, if it is still held.
    ///
    /// Ids ascend, so this is a binary search. Used by the pane to fetch the
    /// lines the view index selected, rather than re-deciding which lines those
    /// are — see [`super::logs::build_view`].
    pub(crate) fn get(&self, id: LogId) -> Option<&StoredLogLine> {
        let at = self.entries.partition_point(|entry| entry.id < id);
        self.entries.get(at).filter(|entry| entry.id == id)
    }

    /// Id the next stream line is expected to have.
    ///
    /// Test-only now. It used to be handed out to the blank that Enter
    /// inserted, which is exactly the collision that stopped being a good idea
    /// — an id belongs to the stream, and nothing local should spend one.
    #[cfg(test)]
    pub(crate) fn next_id(&self) -> LogId {
        self.next_id
    }

    /// Id of the most recently stored line, if any.
    ///
    /// Test-only. Enter's blank mark used it once, and marking the newest
    /// *stored* line was the bug: with hidden services chattering, the newest
    /// stored line is usually one the filter does not admit, so the mark
    /// rendered nothing. What the display marks is the newest *visible* line,
    /// which only the renderer knows.
    #[cfg(test)]
    pub(crate) fn latest_id(&self) -> Option<LogId> {
        self.entries.back().map(|entry| entry.id)
    }

    /// Number of lines currently stored.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn line(name: &str, body: &str) -> FormattedLogLine {
        FormattedLogLine {
            name: name.to_string(),
            is_lifecycle: false,
            is_verbose: false,
            prefix: Vec::new(),
            bytes: body.as_bytes().to_vec(),
        }
    }

    /// A blank the reader asked for must not be a stored line.
    ///
    /// It used to be pushed under `next_id` — the id the next real line would
    /// arrive with. So either that line replaced the blank, or both sat under
    /// one id and the store's binary searches began answering with whichever
    /// came first, which is how logs appeared in the blank space and then
    /// vanished.
    #[test]
    fn a_stream_line_never_lands_on_top_of_a_local_blank() {
        let mut store = LogStore::with_capacity(10);
        store.reflow(80);
        store.push(LogId(0), line("api", "first"));

        // What Enter does now: mark a line it can see, touching nothing.
        let marked = store.latest_id().unwrap();
        assert_eq!(marked, LogId(0));
        assert_eq!(store.next_id(), LogId(1), "the mark takes no id");

        // The next real line arrives under the id the blank used to steal.
        store.push(LogId(1), line("api", "second"));
        let got: Vec<String> = store
            .iter()
            .map(|entry| String::from_utf8_lossy(&entry.line.bytes).into_owned())
            .collect();
        assert_eq!(got, vec!["first", "second"], "both lines survive");
        assert_eq!(
            store.get(LogId(1)).map(|e| e.id),
            Some(LogId(1)),
            "and each id still resolves to its own line"
        );
    }

    /// A reader scrolled back keeps their place: the lines they are looking at
    /// are not evicted under them just because newer ones arrived.
    #[test]
    fn a_pin_holds_history_where_the_reader_is() {
        struct Case {
            name: &'static str,
            capacity: usize,
            pin: Option<u64>,
            /// Lines pushed, ids 0..n.
            pushes: u64,
            want_oldest: u64,
            want_len: usize,
        }

        let cases = [
            Case {
                name: "following evicts as usual",
                capacity: 10,
                pin: None,
                pushes: 30,
                want_oldest: 20,
                want_len: 10,
            },
            Case {
                name: "a pin holds everything from it onward",
                capacity: 10,
                pin: Some(15),
                pushes: 30,
                want_oldest: 15,
                want_len: 15,
            },
            Case {
                // A pin inside the window the store would keep anyway changes
                // nothing: it only ever prevents eviction, never forces it.
                name: "a pin ahead of the normal window is a no-op",
                capacity: 10,
                pin: Some(25),
                pushes: 30,
                want_oldest: 20,
                want_len: 10,
            },
            Case {
                // Twice capacity is the ceiling: a reader who scrolled up and
                // wandered off does not get an unbounded buffer.
                name: "the overdraft is bounded",
                capacity: 10,
                pin: Some(0),
                pushes: 100,
                want_oldest: 80,
                want_len: 20,
            },
        ];

        for case in cases {
            let mut store = LogStore::with_capacity(case.capacity);
            store.reflow(80);
            store.set_pin(case.pin.map(LogId));
            for id in 0..case.pushes {
                store.push(LogId(id), line("svc", "a line of output"));
            }
            assert_eq!(
                store.oldest_id(),
                Some(LogId(case.want_oldest)),
                "{}: oldest retained",
                case.name
            );
            assert_eq!(store.len(), case.want_len, "{}: retained count", case.name);
        }
    }

    /// Releasing the pin lets the backlog drain back to capacity, so returning
    /// to the tail does not leave the overdraft held forever.
    #[test]
    fn clearing_the_pin_drains_the_overdraft() {
        let mut store = LogStore::with_capacity(10);
        store.reflow(80);
        store.set_pin(Some(LogId(0)));
        for id in 0..20 {
            store.push(LogId(id), line("svc", "x"));
        }
        assert_eq!(store.len(), 20, "held while pinned");

        store.set_pin(None);
        store.push(LogId(20), line("svc", "x"));
        assert_eq!(store.len(), 10, "back to capacity once the reader follows");
    }

    #[test]
    fn push_grows_length_up_to_capacity() {
        let mut store = LogStore::with_capacity(10);
        store.push(LogId(0), line("a", "first"));
        store.push(LogId(1), line("b", "second"));
        store.push(LogId(2), line("a", "third"));
        assert_eq!(store.len(), 3);
    }

    #[test]
    fn push_at_capacity_evicts_oldest() {
        let mut store = LogStore::with_capacity(3);
        for id in 0..4 {
            store.push(LogId(id), line("a", "x"));
        }
        assert_eq!(store.len(), 3);
        assert_eq!(
            store.iter().next().map(|entry| entry.id),
            Some(LogId(1)),
            "the oldest goes first"
        );
    }

    #[test]
    fn zero_capacity_drops_every_push() {
        let mut store = LogStore::with_capacity(0);
        store.push(LogId(0), line("a", "1"));
        store.push(LogId(1), line("a", "2"));
        assert_eq!(store.len(), 0);
    }

    /// Ids come from don's merged stream, so the store carries whatever it is
    /// given rather than counting for itself — a client resuming mid-stream
    /// starts at a non-zero id, and one that lost lines skips.
    #[test]
    fn stored_ids_are_the_streams_own() {
        let mut store = LogStore::with_capacity(10);
        assert_eq!(store.latest_id(), None);
        assert_eq!(store.next_id(), LogId::ZERO);

        store.push(LogId(500), line("a", "resumed"));
        assert_eq!(store.latest_id(), Some(LogId(500)));
        assert_eq!(store.next_id(), LogId(501));

        store.push(LogId(900), line("a", "after a drop"));
        assert_eq!(store.latest_id(), Some(LogId(900)));
        assert_eq!(store.iter().count(), 2, "both survive; ids simply skip");
    }

    /// The pane needs a row count before it draws, so the store measures at
    /// push and re-measures only when the width moves.
    #[test]
    fn row_counts_track_the_wrap_width() {
        struct Case {
            name: &'static str,
            width: u16,
            want_rows: usize,
        }

        let cases = vec![
            Case {
                name: "wide enough for one row",
                width: 40,
                want_rows: 1,
            },
            Case {
                name: "half the width doubles it",
                width: 10,
                want_rows: 2,
            },
            Case {
                name: "a quarter quadruples it",
                width: 5,
                want_rows: 4,
            },
        ];

        for case in cases {
            let mut store = LogStore::with_capacity(10);
            store.reflow(case.width);
            store.push(LogId(0), line("a", "12345678901234567890"));
            assert_eq!(
                store.iter().next().unwrap().wrapped_rows(),
                case.want_rows,
                "{}: at push",
                case.name
            );

            // And a line pushed before the width was known catches up on
            // reflow rather than staying wrong.
            let mut later = LogStore::with_capacity(10);
            later.push(LogId(0), line("a", "12345678901234567890"));
            later.reflow(case.width);
            assert_eq!(
                later.iter().next().unwrap().wrapped_rows(),
                case.want_rows,
                "{}: after reflow",
                case.name
            );
        }
    }

    /// Parsing happens once, at push — a repaint touches every visible line
    /// every frame, so doing it at render would tie frame cost to screen height
    /// and redo identical work forever.
    #[test]
    fn ansi_is_parsed_into_styles_at_push() {
        let mut store = LogStore::with_capacity(10);
        store.push(LogId(0), line("a", "\x1b[31mred\x1b[0m plain"));
        let entry = store.iter().next().unwrap();
        assert!(
            entry.parsed.spans.len() >= 2,
            "the escape should have split the line into styled spans"
        );
        assert_eq!(
            entry.parsed.spans[0].style.fg,
            Some(ratatui::style::Color::Red),
            "and the colour should have survived"
        );
    }
}
