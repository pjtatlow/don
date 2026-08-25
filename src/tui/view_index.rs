//! An index over the log lines the filter admits.
//!
//! The pane needs three things before it can draw: how many rows the admitted
//! content occupies, which line owns the row at the top edge, and the lines
//! from there down. Computing those by walking the store meant touching every
//! retained line on every frame *and* on every scroll event — at fifty
//! thousand lines and a real filter that is ~670µs a frame and ~465µs a wheel
//! notch, which is what made scrolling feel like wading.
//!
//! None of it needs recomputing. The admitted set only changes when a line
//! arrives, when a line is evicted, when the filter changes, or when the width
//! changes. So this keeps a running index and mends it incrementally: a push is
//! O(1), an eviction is O(1), and finding the line at a row is a binary search.
//! A full rebuild happens only when the filter or the width moves.
//!
//! ## Why cumulative rows are absolute
//!
//! Each entry records how many rows came before it *since the beginning of
//! time*, not since the front of the buffer. Eviction then costs nothing:
//! dropping the front moves a single `base` rather than rewriting every entry
//! behind it. The current row offset of an entry is `cum - base`.

use std::collections::{HashMap, VecDeque};

use super::log_store::{LogStore, StoredLogLine};
use crate::output::LogId;

/// Rows a line occupies, including the blank the reader marked after it.
fn rows_for(entry: &StoredLogLine, blanks: &HashMap<LogId, u16>) -> u32 {
    let blank = u32::from(blanks.get(&entry.id).copied().unwrap_or(0));
    u32::try_from(entry.wrapped_rows().max(1))
        .unwrap_or(u32::MAX)
        .saturating_add(blank)
}

/// One admitted line's place in the view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Indexed {
    id: LogId,
    /// Rows occupied by every admitted line before this one, ever.
    cum: u64,
    rows: u32,
}

/// What the index was built against. When any of it moves, the index is stale
/// and must be rebuilt rather than mended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct ViewKey {
    pub(crate) width: u16,
    /// A fingerprint of everything `should_render_log` consults. Hashed rather
    /// than hand-incremented: a missed bump would leave the pane rendering a
    /// filter the user has already changed, and there are more mutators than
    /// anyone will remember to keep in step.
    pub(crate) filter: u64,
}

/// A mendable index over the admitted lines.
#[derive(Debug, Default)]
pub(crate) struct ViewIndex {
    entries: VecDeque<Indexed>,
    /// Rows before the first surviving entry. Only eviction moves this.
    base: u64,
    /// Rows counted so far, so an appended line knows where it starts.
    next_cum: u64,
    /// The id after the last line examined, so a mend knows where to resume.
    next_id: LogId,
    key: ViewKey,
    built: bool,
}

impl ViewIndex {
    /// Bring the index up to date with the store.
    ///
    /// Cheap in the common case — a few appends — and a full rebuild only when
    /// the filter or the pane width has moved.
    pub(crate) fn sync<F>(
        &mut self,
        store: &LogStore,
        key: ViewKey,
        blanks: &HashMap<LogId, u16>,
        admits: F,
    ) where
        F: Fn(&StoredLogLine) -> bool,
    {
        if !self.built || self.key != key {
            self.rebuild(store, key, blanks, &admits);
            return;
        }
        // Drop what the store has evicted. Entries are ordered by id, so this
        // is a prefix.
        if let Some(oldest) = store.oldest_id() {
            while self.entries.front().is_some_and(|entry| entry.id < oldest) {
                let gone = self.entries.pop_front().unwrap_or(Indexed {
                    id: oldest,
                    cum: self.base,
                    rows: 0,
                });
                self.base = gone.cum + u64::from(gone.rows);
            }
        } else {
            self.entries.clear();
            self.base = self.next_cum;
        }
        // Nothing already indexed can have changed: a line is measured at
        // push and never moves again, so eviction above and appends below are
        // between them the whole of what a sync has to catch up on.
        // Append what has arrived.
        for entry in store.iter_from(self.next_id) {
            self.push(entry, blanks, &admits);
        }
    }

    fn rebuild<F>(
        &mut self,
        store: &LogStore,
        key: ViewKey,
        blanks: &HashMap<LogId, u16>,
        admits: &F,
    ) where
        F: Fn(&StoredLogLine) -> bool,
    {
        self.entries.clear();
        self.base = 0;
        self.next_cum = 0;
        self.next_id = LogId::ZERO;
        self.key = key;
        self.built = true;
        for entry in store.iter() {
            self.push(entry, blanks, admits);
        }
    }

    fn push<F>(&mut self, entry: &StoredLogLine, blanks: &HashMap<LogId, u16>, admits: &F)
    where
        F: Fn(&StoredLogLine) -> bool,
    {
        self.next_id = LogId(entry.id.0.saturating_add(1));
        if !admits(entry) {
            return;
        }
        let rows = rows_for(entry, blanks);
        self.entries.push_back(Indexed {
            id: entry.id,
            cum: self.next_cum,
            rows,
        });
        self.next_cum += u64::from(rows);
    }

    /// Rows the admitted content occupies at the indexed width.
    pub(crate) fn total_rows(&self) -> usize {
        usize::try_from(self.next_cum.saturating_sub(self.base)).unwrap_or(usize::MAX)
    }

    /// Rows above the admitted line `id`, or `None` if it is not admitted.
    ///
    /// Binary search: ids ascend, so the index is sorted by construction.
    pub(crate) fn rows_above(&self, id: LogId) -> Option<usize> {
        let at = self.entries.partition_point(|entry| entry.id < id);
        let entry = self.entries.get(at)?;
        usize::try_from(entry.cum - self.base).ok()
    }

    /// The admitted lines from `id` onwards, in order.
    ///
    /// This is what the pane draws from. Drawing by re-walking the store and
    /// re-applying the filter is how the rows on screen and the rows this index
    /// counted drifted apart — and a view that positions itself by one set of
    /// row counts while painting another lands somewhere different from where
    /// it says on every scroll.
    pub(crate) fn ids_from(&self, id: LogId) -> impl Iterator<Item = LogId> + '_ {
        let at = self.entries.partition_point(|entry| entry.id < id);
        self.entries.iter().skip(at).map(|entry| entry.id)
    }

    /// Which admitted line owns row `offset`, and how far into it that row is.
    pub(crate) fn line_at(&self, offset: usize) -> Option<(LogId, u16)> {
        let want = self.base + u64::try_from(offset).unwrap_or(u64::MAX);
        // The last entry whose span starts at or before `want`.
        let at = self
            .entries
            .partition_point(|entry| entry.cum <= want)
            .checked_sub(1)?;
        let entry = self.entries.get(at)?;
        let within = u16::try_from(want - entry.cum).unwrap_or(u16::MAX);
        Some((entry.id, within))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    /// No blank marks — the default for a test that is not about them.
    fn no_marks() -> HashMap<LogId, u16> {
        HashMap::new()
    }
    use crate::output::FormattedLogLine;

    fn store_of(lines: &[(u64, &str, &str)], width: u16) -> LogStore {
        let mut store = LogStore::with_capacity(100);
        store.reflow(width);
        for (id, name, body) in lines {
            store.push(
                LogId(*id),
                FormattedLogLine {
                    name: (*name).to_string(),
                    is_lifecycle: false,
                    is_verbose: false,
                    prefix: Vec::new(),
                    bytes: body.as_bytes().to_vec(),
                },
            );
        }
        store
    }

    fn key(width: u16, filter: u64) -> ViewKey {
        ViewKey { width, filter }
    }

    /// The index has to agree with a naive walk of the store, or the pane
    /// draws one thing and positions itself by another.
    #[test]
    fn the_index_agrees_with_walking_the_store() {
        // 10-wide pane: "aaaaaaaaaaaaaaa" (15 chars) is two rows, "b" is one.
        let store = store_of(
            &[
                (0, "api", "aaaaaaaaaaaaaaa"),
                (1, "web", "b"),
                (2, "api", "ccccccccccccccccccccccccc"),
                (3, "web", "d"),
            ],
            10,
        );
        let admits = |entry: &StoredLogLine| entry.line.name == "api";

        let mut index = ViewIndex::default();
        index.sync(&store, key(10, 0), &no_marks(), admits);

        // api lines only: 2 rows + 3 rows.
        assert_eq!(index.total_rows(), 5);
        assert_eq!(index.rows_above(LogId(0)), Some(0));
        assert_eq!(index.rows_above(LogId(2)), Some(2));
        assert_eq!(
            index.rows_above(LogId(1)),
            Some(2),
            "rounds up to the next admitted line"
        );

        assert_eq!(index.line_at(0), Some((LogId(0), 0)));
        assert_eq!(index.line_at(1), Some((LogId(0), 1)));
        assert_eq!(index.line_at(2), Some((LogId(2), 0)));
        assert_eq!(index.line_at(4), Some((LogId(2), 2)));
    }

    /// A mend must land in the same place a rebuild would. This is the whole
    /// bet of the index: that appending and evicting is equivalent to
    /// recomputing, so the fast path and the correct path never diverge.
    #[test]
    fn mending_matches_rebuilding() {
        let admits = |entry: &StoredLogLine| entry.line.name != "quiet";
        let mut mended = ViewIndex::default();

        let mut store = LogStore::with_capacity(6);
        store.reflow(12);
        for step in 0..12u64 {
            let name = if step % 3 == 0 { "quiet" } else { "loud" };
            store.push(
                LogId(step),
                FormattedLogLine {
                    name: name.to_string(),
                    is_lifecycle: false,
                    is_verbose: false,
                    // Lengths vary so row counts do too.
                    prefix: Vec::new(),
                    bytes: "x".repeat(1 + (step as usize % 4) * 9).into_bytes(),
                },
            );
            mended.sync(&store, key(12, 0), &no_marks(), admits);

            let mut fresh = ViewIndex::default();
            fresh.sync(&store, key(12, 1), &no_marks(), admits); // different key forces a rebuild
            fresh.key = mended.key;

            assert_eq!(
                mended.total_rows(),
                fresh.total_rows(),
                "total rows after {step} pushes (capacity forces eviction)"
            );
            for offset in 0..fresh.total_rows() {
                assert_eq!(
                    mended.line_at(offset),
                    fresh.line_at(offset),
                    "row {offset} after {step} pushes"
                );
            }
        }
    }

    /// Changing the filter or the width invalidates everything, because both
    /// change which lines count and how tall each one is.
    #[test]
    fn a_changed_key_rebuilds() {
        let store = store_of(&[(0, "api", "aaaaaaaaaaaaaaa"), (1, "web", "bbb")], 10);
        let mut index = ViewIndex::default();

        index.sync(&store, key(10, 0), &no_marks(), |e| e.line.name == "api");
        assert_eq!(index.total_rows(), 2);

        // Same width, different filter.
        index.sync(&store, key(10, 1), &no_marks(), |_| true);
        assert_eq!(index.total_rows(), 3, "web's single row joins");

        // Same filter, wider pane: the long line stops wrapping.
        let mut wide = store_of(&[(0, "api", "aaaaaaaaaaaaaaa"), (1, "web", "bbb")], 40);
        wide.reflow(40);
        index.sync(&wide, key(40, 1), &no_marks(), |_| true);
        assert_eq!(index.total_rows(), 2, "one row each at 40 columns");
    }
}
