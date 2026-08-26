//! Log filter — multi-select set of service/task names.
//!
//! The filter is *always on*. `active_selected` is the set of names whose
//! log lines are rendered. Defaults come from the config: each service or
//! task declares `hidden = true` to start outside the active set. The user
//! can narrow or widen the selection interactively at any time.
//!
//! Every change applies the moment it happens. Space, `o` and `R` mutate
//! `active_selected` directly, and the log pane beside the panel shows the
//! effect immediately — so there is no commit step and no revert. An Esc that
//! silently undid changes the reader just watched work would be a trap, and
//! an Enter whose only job was "yes, keep what I already have" a ceremony.
//! Closing the panel changes nothing; what it shows *is* the filter.
//!
//! Typing does nothing until the user presses `/` to enter query-input
//! sub-mode. Pressing Enter in query focus applies the current query result
//! to the active selection and returns focus to the list; Esc drops the
//! query (one level up) while keeping whatever selection it produced.
//!
//! Blank spacer lines (`name == ""`, inserted when the user presses Enter
//! in Normal mode) always pass regardless of the filter — they're UI
//! whitespace, not log content. `[don]` lifecycle events are *not* special:
//! they carry the `"don"` name and are gated like any other entry, and the
//! `hidden` flag can be set per-service/task to hide them by default.
//!
//! `all_names` is the source of truth — the union of service and task names
//! the runner knows about at TUI startup, plus the synthetic `"don"` entry
//! for lifecycle events. Fixed for the session.
//!
//! ## The "all" synthetic row
//!
//! The edit view prepends a synthetic `[all]` row when the query is empty.
//! Toggling it with Space flips every name between "all selected" and
//! "none selected". The row disappears once the user starts typing a query.

use std::collections::HashSet;

use super::fuzzy::fuzzy_match;

/// A row displayed in the filter edit list. `All` is a synthetic convenience
/// row that toggles every name at once; `Name` holds a real service/task
/// name that can be individually toggled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FilterRow {
    /// Synthetic "select/deselect all" row. Only appears when the query is
    /// empty.
    All,
    /// A real service or task name. Membership in the active set controls
    /// whether its log lines render.
    Name(String),
}

/// Which part of the filter modal currently owns keyboard focus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FilterFocus {
    /// The query input owns character keys.
    Query,
    /// The list owns navigation and selection keys.
    List,
}

/// Filter state, persistent across mode switches.
#[derive(Debug, Clone)]
pub(crate) struct FilterState {
    /// Every filterable name (services + tasks + "don"), sorted and deduped.
    all_names: Vec<String>,
    /// Names the modal list should skip when rendering rows. Used for
    /// services that are currently in [`ServiceState::Lazy`] — they haven't
    /// been started and would otherwise clutter the filter list. Updated
    /// dynamically as services transition in/out of Lazy state.
    ///
    /// Membership in this set does NOT remove a name from `all_names` or
    /// the selection — once the service leaves Lazy, it reappears in the
    /// list and its prior selection state is preserved.
    hidden_from_display: HashSet<String>,
    /// The config-derived default selection: `all_names - hidden`. Used by
    /// [`FilterState::reset_to_defaults`].
    default_selected: HashSet<String>,
    /// Query buffer visible only in filter-edit mode.
    query: String,
    /// Rows for the current query, in display order. Includes a synthetic
    /// [`FilterRow::All`] at position 0 when the query is empty. Names in
    /// `hidden_from_display` are omitted.
    rows: Vec<FilterRow>,
    /// Which part of the modal currently owns keyboard focus.
    focus: FilterFocus,
    /// The selection. Log lines pass iff their name is in this set, and every
    /// toggle edits it directly — there is no pending copy waiting on an
    /// Enter, because the pane beside the panel shows the effect immediately.
    active_selected: HashSet<String>,
    /// Highlighted index within `rows` — what Up/Down move.
    highlight: usize,
}

impl FilterState {
    /// Initialize with the full set of filterable names. When `cli_override`
    /// is `Some`, it takes precedence over `hidden`: the seeded selection is
    /// `cli_override ∩ all_names` plus the synthetic `[don]` lifecycle entry
    /// (so users always see runner control output even when narrowing logs).
    /// Otherwise `active_selected` is `all_names - hidden`. The seeded set
    /// becomes the reset target — pressing Reset restores it.
    pub(crate) fn new(
        mut all_names: Vec<String>,
        hidden: &HashSet<String>,
        cli_override: Option<&HashSet<String>>,
    ) -> Self {
        all_names.sort();
        all_names.dedup();
        let default_selected: HashSet<String> = match cli_override {
            Some(allow) => all_names
                .iter()
                .filter(|n| {
                    allow.contains(n.as_str()) || n.as_str() == crate::output::LIFECYCLE_EVENT_NAME
                })
                .cloned()
                .collect(),
            None => all_names
                .iter()
                .filter(|n| !hidden.contains(n.as_str()))
                .cloned()
                .collect(),
        };
        let active_selected = default_selected.clone();
        let hidden_from_display = HashSet::new();
        let rows = build_rows(&all_names, "", &hidden_from_display);
        Self {
            all_names,
            hidden_from_display,
            default_selected,
            query: String::new(),
            rows,
            focus: FilterFocus::List,
            active_selected,
            highlight: 0,
        }
    }

    /// Replace the set of names hidden from the modal list (e.g. Lazy
    /// services) and recompute rows. Highlight is clamped if it fell off
    /// the end. Call this whenever service state changes — it's cheap
    /// (single pass over `all_names`).
    pub(crate) fn set_hidden_from_display(&mut self, hidden: HashSet<String>) {
        if self.hidden_from_display == hidden {
            return;
        }
        self.hidden_from_display = hidden;
        self.recompute_rows();
    }

    /// True when the current active selection hides any names. Used by the
    /// status bar to decide between "filter: api, db [esc] reset" and the
    /// normal idle hint.
    pub(crate) fn is_active(&self) -> bool {
        self.active_selected.len() != self.all_names.len()
    }

    /// True if the given log line name passes the currently-active filter.
    /// Blank spacer lines (empty name) always pass; real names must be in
    /// the active selection.
    /// Feed everything [`Self::passes`] depends on into a hasher, so a caller
    /// can tell whether the answer for *any* name may have changed.
    pub(crate) fn fingerprint(&self, hasher: &mut impl std::hash::Hasher) {
        use std::hash::Hash;
        // Order-independent: a set has none, and two runs must agree.
        let mut sum: u64 = 0;
        for name in &self.active_selected {
            let mut one = std::collections::hash_map::DefaultHasher::new();
            name.hash(&mut one);
            sum = sum.wrapping_add(std::hash::Hasher::finish(&one));
        }
        sum.hash(hasher);
        self.active_selected.len().hash(hasher);
    }

    pub(crate) fn passes(&self, name: &str) -> bool {
        if name.is_empty() {
            return true;
        }
        self.active_selected.contains(name)
    }

    /// Take note of a process the filter has not seen before.
    ///
    /// The name list is built once, from the config the TUI started with, and
    /// a process that turns up later — a config reload adding a service, a
    /// client attaching before the daemon has reported everything — was
    /// invisible to it. It had no row in the filter panel and could not be
    /// narrowed to, so `l` on its table row did nothing at all and said
    /// nothing about why.
    ///
    /// A newcomer belongs in the defaults, so "reset" shows it. It joins the
    /// *active* selection only when the filter was showing everything: a
    /// reader who has narrowed to one process did not ask for whatever turned
    /// up next, and a narrow that quietly widened would be worse than a
    /// process that waits to be selected.
    pub(crate) fn learn_name(&mut self, name: &str) {
        if name.is_empty() || self.all_names.iter().any(|n| n == name) {
            return;
        }
        let was_showing_everything = !self.is_active();
        self.all_names.push(name.to_string());
        self.all_names.sort();
        self.default_selected.insert(name.to_string());
        if was_showing_everything {
            self.active_selected.insert(name.to_string());
        }
    }

    /// Show only `name`, returning the selection it replaced.
    ///
    /// This is what `l` on a table row does: the log pane beside the panel is
    /// the log view, so narrowing it *is* showing that row's log — there is no
    /// second pane to open, no second history to fill, and everything the log
    /// pane can do it can still do while narrowed.
    ///
    /// `None` when `name` is not filterable or the selection is already
    /// exactly it, so the caller does not record a restore point for a
    /// narrowing that did not happen.
    pub(crate) fn narrow_to(&mut self, name: &str) -> Option<HashSet<String>> {
        if !self.all_names.iter().any(|n| n == name) {
            return None;
        }
        if self.active_selected.len() == 1 && self.active_selected.contains(name) {
            return None;
        }
        let previous = std::mem::take(&mut self.active_selected);
        self.active_selected.insert(name.to_string());
        Some(previous)
    }

    /// Put back a selection [`Self::narrow_to`] replaced.
    pub(crate) fn restore(&mut self, previous: HashSet<String>) {
        self.active_selected = previous;
    }

    /// Add a name to the selection. Returns `true` when this made the active
    /// filter more permissive.
    pub(crate) fn select_name(&mut self, name: &str) -> bool {
        if !self.all_names.iter().any(|n| n == name) {
            return false;
        }
        self.active_selected.insert(name.to_string())
    }

    /// Open the panel's list fresh: query cleared, highlight at the top.
    ///
    /// No snapshot, no commit, no revert. Toggling applies to the pane the
    /// moment it happens — the reader watches it work beside the panel — so an
    /// Esc that silently undid what they just watched was a trap, and an Enter
    /// whose only job was "yes, keep what I already have" was a ceremony.
    /// What the panel shows *is* the filter; closing it changes nothing.
    pub(crate) fn enter_edit(&mut self) {
        self.reset_edit_state();
    }

    /// Reset the selection to the config-derived defaults. Returns
    /// `true` when the selection changed.
    pub(crate) fn reset_to_defaults(&mut self) -> bool {
        if self.active_selected == self.default_selected {
            return false;
        }
        self.active_selected = self.default_selected.clone();
        true
    }

    /// Enter query-input sub-mode so subsequent typed characters refine the
    /// visible row list.
    pub(crate) fn begin_query_edit(&mut self) {
        self.focus = FilterFocus::Query;
    }

    /// Leave query-input sub-mode, keeping the current query active.
    pub(crate) fn end_query_edit(&mut self) {
        self.focus = FilterFocus::List;
    }

    /// Apply the current query result to the active selection.
    ///
    /// An empty query restores "all names selected". Non-empty queries use
    /// the visible rows, which are already filtered for hidden/lazy names.
    pub(crate) fn apply_query(&mut self) {
        if self.query.is_empty() {
            self.active_selected = self.all_names.iter().cloned().collect();
            return;
        }

        self.active_selected.clear();
        for row in &self.rows {
            if let FilterRow::Name(name) = row {
                self.active_selected.insert(name.clone());
            }
        }
    }

    /// True when the current non-empty query narrows to exactly one real
    /// service/task row, so Enter can apply-and-close in one step.
    pub(crate) fn query_has_single_match(&self) -> bool {
        !self.query.is_empty() && matches!(self.rows.as_slice(), [FilterRow::Name(_)])
    }

    /// True while the modal is capturing keystrokes into the query buffer.
    pub(crate) fn query_editing(&self) -> bool {
        self.focus == FilterFocus::Query
    }

    /// Which part of the modal currently owns keyboard focus.
    pub(crate) fn focus(&self) -> FilterFocus {
        self.focus
    }

    /// Append a character to the query and recompute rows.
    pub(crate) fn push_query_char(&mut self, c: char) {
        self.query.push(c);
        self.recompute_rows();
    }

    /// Remove the last character of the query (noop if empty).
    pub(crate) fn pop_query_char(&mut self) {
        if self.query.pop().is_some() {
            self.recompute_rows();
        }
    }

    /// Move highlight up with wrap-around.
    pub(crate) fn highlight_prev(&mut self) {
        if self.rows.is_empty() {
            return;
        }
        self.highlight = if self.highlight == 0 {
            self.rows.len() - 1
        } else {
            self.highlight - 1
        };
    }

    /// Move highlight down with wrap-around.
    pub(crate) fn highlight_next(&mut self) {
        if self.rows.is_empty() {
            return;
        }
        self.highlight = (self.highlight + 1) % self.rows.len();
    }

    /// Toggle the highlighted row in the active selection. For `Name(n)`, flip
    /// membership. For `All`, select every name (if any were missing) or
    /// clear every name (if all were already present).
    pub(crate) fn toggle_highlighted(&mut self) {
        match self.rows.get(self.highlight) {
            Some(FilterRow::Name(name)) => {
                if self.active_selected.contains(name) {
                    self.active_selected.remove(name);
                } else {
                    self.active_selected.insert(name.clone());
                }
            }
            Some(FilterRow::All) => {
                if self.all_selected_in_edit() {
                    self.active_selected.clear();
                } else {
                    self.active_selected.extend(self.all_names.iter().cloned());
                }
            }
            None => {}
        }
    }

    /// Replace the active selection with only the highlighted row.
    pub(crate) fn select_only_highlighted(&mut self) {
        self.active_selected.clear();
        match self.rows.get(self.highlight) {
            Some(FilterRow::Name(name)) => {
                self.active_selected.insert(name.clone());
            }
            Some(FilterRow::All) => {
                self.active_selected.extend(self.all_names.iter().cloned());
            }
            None => {}
        }
    }

    /// Current query string (for bar rendering).
    pub(crate) fn query(&self) -> &str {
        &self.query
    }

    /// Currently visible rows (for list rendering).
    pub(crate) fn rows(&self) -> &[FilterRow] {
        &self.rows
    }

    /// Currently highlighted row index.
    pub(crate) fn highlight(&self) -> usize {
        self.highlight
    }

    /// Whether a name is in the active selection (used to draw the
    /// checkbox for a [`FilterRow::Name`]).
    pub(crate) fn is_edit_selected(&self, name: &str) -> bool {
        self.active_selected.contains(name)
    }

    /// Whether every name is currently in the active selection. Used to
    /// draw the checkbox for the synthetic [`FilterRow::All`] row.
    pub(crate) fn all_selected_in_edit(&self) -> bool {
        !self.all_names.is_empty() && self.active_selected.len() == self.all_names.len()
    }

    #[cfg(test)]
    fn active_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.active_selected.iter().map(String::as_str).collect();
        names.sort();
        names
    }

    fn recompute_rows(&mut self) {
        self.rows = build_rows(&self.all_names, &self.query, &self.hidden_from_display);
        if self.highlight >= self.rows.len() {
            self.highlight = 0;
        }
    }

    fn reset_edit_state(&mut self) {
        self.query.clear();
        self.focus = FilterFocus::List;
        self.highlight = 0;
        self.rows = build_rows(&self.all_names, "", &self.hidden_from_display);
    }

    /// Drop the query and return to the list — Esc's first level. The
    /// selection the query produced stays; only the narrowing goes.
    pub(crate) fn clear_query(&mut self) {
        self.query.clear();
        self.focus = FilterFocus::List;
        self.highlight = 0;
        self.rows = build_rows(&self.all_names, "", &self.hidden_from_display);
    }
}

/// Build the row list for the given query. The synthetic [`FilterRow::All`]
/// row only appears when the query is empty — a query implies the user is
/// narrowing to specific names, so the select-all affordance would be noise.
/// Names in `hidden_from_display` are skipped (e.g. Lazy services).
fn build_rows(
    all_names: &[String],
    query: &str,
    hidden_from_display: &HashSet<String>,
) -> Vec<FilterRow> {
    let visible = |name: &String| !hidden_from_display.contains(name);
    if query.is_empty() {
        let mut rows = Vec::with_capacity(all_names.len() + 1);
        rows.push(FilterRow::All);
        rows.extend(
            all_names
                .iter()
                .filter(|n| visible(n))
                .cloned()
                .map(FilterRow::Name),
        );
        rows
    } else {
        // `fuzzy_match` doesn't know about hidden_from_display; filter its
        // output instead of pre-filtering the candidate list to preserve
        // the matcher's scoring view of the full set.
        fuzzy_match(query, all_names)
            .into_iter()
            .filter(|n| !hidden_from_display.contains(n))
            .map(FilterRow::Name)
            .collect()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    /// `l` narrows the pane to one process and Esc gives back what was there.
    ///
    /// The pane beside the panel *is* the log view, so narrowing it is how a
    /// row's log gets shown — which only works if getting back is one key,
    /// since rebuilding a filter by hand is the reason a modal looked
    /// necessary in the first place.
    #[test]
    fn narrowing_to_one_name_is_reversible() {
        struct Case {
            name: &'static str,
            /// Selected before the narrow.
            before: &'static [&'static str],
            narrow_to: &'static str,
            /// `None` when nothing was narrowed and there is nothing to undo.
            want_narrow: Option<&'static [&'static str]>,
        }

        let cases = vec![
            Case {
                name: "from everything",
                before: &["api", "web", "worker"],
                narrow_to: "web",
                want_narrow: Some(&["web"]),
            },
            Case {
                name: "from an already-narrowed selection",
                before: &["api", "web"],
                narrow_to: "api",
                want_narrow: Some(&["api"]),
            },
            Case {
                name: "already showing exactly that one changes nothing",
                before: &["web"],
                narrow_to: "web",
                want_narrow: None,
            },
            Case {
                name: "a name the filter has never heard of changes nothing",
                before: &["api", "web"],
                narrow_to: "nosuch",
                want_narrow: None,
            },
        ];

        for case in cases {
            let all: Vec<String> = ["api", "web", "worker"]
                .iter()
                .map(|n| (*n).to_string())
                .collect();
            let mut filter = FilterState::new(all, &HashSet::new(), None);
            let before: HashSet<String> = case.before.iter().map(|n| (*n).to_string()).collect();
            filter.restore(before.clone());

            let previous = filter.narrow_to(case.narrow_to);

            match case.want_narrow {
                Some(want) => {
                    let got: HashSet<String> = want.iter().map(|n| (*n).to_string()).collect();
                    assert!(
                        want.iter().all(|n| filter.passes(n)),
                        "{}: the narrowed name shows",
                        case.name
                    );
                    for other in ["api", "web", "worker"] {
                        assert_eq!(
                            filter.passes(other),
                            got.contains(other),
                            "{}: only the narrowed name shows ({other})",
                            case.name
                        );
                    }
                    filter.restore(previous.expect("a narrowing records its restore point"));
                }
                None => assert!(
                    previous.is_none(),
                    "{}: nothing changed, so there is nothing to undo",
                    case.name
                ),
            }

            for name in ["api", "web", "worker"] {
                assert_eq!(
                    filter.passes(name),
                    before.contains(name),
                    "{}: back to what it was ({name})",
                    case.name
                );
            }
        }
    }

    fn state(names: &[&str]) -> FilterState {
        FilterState::new(
            names.iter().map(|s| s.to_string()).collect(),
            &HashSet::new(),
            None,
        )
    }

    fn state_with_hidden(names: &[&str], hidden: &[&str]) -> FilterState {
        let hidden_set: HashSet<String> = hidden.iter().map(|s| s.to_string()).collect();
        FilterState::new(
            names.iter().map(|s| s.to_string()).collect(),
            &hidden_set,
            None,
        )
    }

    #[test]
    fn default_filter_passes_all_when_nothing_hidden() {
        let s = state(&["api", "worker"]);
        assert!(s.passes("api"));
        assert!(s.passes("worker"));
        assert!(s.passes(""), "blank spacer lines always pass");
        // "filter is active" reports diverges-from-full-set; with no hidden
        // names, every name is selected and the bar treats it as inactive.
        assert!(!s.is_active());
    }

    #[test]
    fn hidden_names_are_gated_by_default() {
        let s = state_with_hidden(&["api", "worker", "db"], &["db"]);
        assert!(s.passes("api"));
        assert!(s.passes("worker"));
        assert!(!s.passes("db"), "db hidden by default");
        assert!(
            s.is_active(),
            "filter reports active when any name is hidden"
        );
    }

    #[test]
    fn select_name_admits_hidden_name_immediately() {
        let mut s = state_with_hidden(&["api", "db"], &["db"]);
        assert!(!s.passes("db"));

        s.enter_edit();
        assert!(s.select_name("db"));

        assert!(s.passes("db"));
    }

    #[test]
    fn unknown_names_are_blocked_even_without_config() {
        // With the filter always on, names the runner didn't declare at
        // startup are unrecognized — gated out rather than silently passing.
        let s = state(&["api"]);
        assert!(!s.passes("mystery"));
    }

    #[test]
    fn a_query_alone_narrows_visibility_not_the_selection() {
        let mut s = state(&["api", "worker", "db"]);
        s.enter_edit();
        s.push_query_char('a');
        assert!(s.passes("api"));
        assert!(s.passes("worker"));
        assert!(s.passes("db"));
        assert!(s.passes(""), "blank spacer lines always pass");
    }

    #[test]
    fn don_lifecycle_gated_when_not_selected() {
        let mut s = state(&["api", "don"]);
        s.enter_edit();
        s.push_query_char('a'); // matches "api" but not "don"
        s.select_only_highlighted();
        assert!(s.passes("api"));
        assert!(!s.passes("don"), "don gated when not in selection");
    }

    #[test]
    fn space_multiselect_applies_immediately() {
        let mut s = state(&["api", "worker", "db", "cache"]);
        s.enter_edit();
        // Sorted rows: [All, api, cache, db, worker]. Highlight starts at 0 (All).
        // Clear the pre-seeded full selection via the All row, then pick two.
        s.toggle_highlighted(); // All → clear
        s.highlight_next(); // api
        s.highlight_next(); // cache
        s.toggle_highlighted(); // cache
        s.highlight_next(); // db
        s.toggle_highlighted(); // db
        let mut active: Vec<String> = s.active_names().iter().map(|n| n.to_string()).collect();
        active.sort();
        assert_eq!(active, vec!["cache".to_string(), "db".to_string()]);
        assert!(s.passes("cache"));
        assert!(s.passes("db"));
        assert!(!s.passes("api"));
    }

    #[test]
    fn only_highlighted_explicitly_narrows_selection() {
        let mut s = state(&["api", "worker"]);
        s.enter_edit();
        s.push_query_char('a'); // fuzzy matches "api"
        s.select_only_highlighted();
        let active: Vec<&str> = s.active_names();
        assert_eq!(active, vec!["api"]);
    }

    #[test]
    fn cli_override_seeds_active_and_overrides_hidden() {
        struct Case {
            name: &'static str,
            all: &'static [&'static str],
            hidden: &'static [&'static str],
            cli: &'static [&'static str],
            want_passes: &'static [&'static str],
            want_blocks: &'static [&'static str],
        }

        // `don` is always added to `all_names` in the production wiring
        // (App::new), so test cases mirror that — the FilterState itself
        // doesn't synthesize unknown names.
        let cases = vec![
            Case {
                name: "cli filter overrides hidden defaults",
                all: &["api", "worker", "db", "don"],
                hidden: &["db"],
                cli: &["worker"],
                want_passes: &["worker", "don"],
                want_blocks: &["api", "db"],
            },
            Case {
                name: "cli filter still admits don lifecycle",
                all: &["api", "worker", "don"],
                hidden: &[],
                cli: &["api"],
                want_passes: &["api", "don"],
                want_blocks: &["worker"],
            },
            Case {
                name: "cli filter that re-enables a hidden name shows it",
                all: &["api", "db", "don"],
                hidden: &["db"],
                cli: &["db"],
                want_passes: &["db", "don"],
                want_blocks: &["api"],
            },
        ];

        for case in cases {
            let hidden: HashSet<String> = case.hidden.iter().map(|s| (*s).to_string()).collect();
            let cli: HashSet<String> = case.cli.iter().map(|s| (*s).to_string()).collect();
            let s = FilterState::new(
                case.all.iter().map(|s| (*s).to_string()).collect(),
                &hidden,
                Some(&cli),
            );
            for n in case.want_passes {
                assert!(s.passes(n), "case {}: expected pass for '{n}'", case.name);
            }
            for n in case.want_blocks {
                assert!(!s.passes(n), "case {}: expected block for '{n}'", case.name);
            }
        }
    }

    #[test]
    fn reset_to_defaults_restores_config_selection() {
        // api hidden by default — after a custom filter, reset should bring
        // back the config defaults (api hidden, worker visible).
        let mut s = state_with_hidden(&["api", "worker"], &["api"]);
        assert!(!s.passes("api"));
        assert!(s.passes("worker"));

        s.enter_edit();
        s.push_query_char('a');
        s.select_only_highlighted();
        assert!(s.passes("api"));
        assert!(!s.passes("worker"));

        s.enter_edit();
        s.reset_to_defaults();
        assert!(!s.passes("api"));
        assert!(s.passes("worker"));
    }

    #[test]
    fn reset_to_defaults_reports_changes() {
        let mut s = state_with_hidden(&["api", "worker"], &["api"]);
        assert!(!s.reset_to_defaults());

        s.enter_edit();
        s.push_query_char('a');
        s.select_only_highlighted();
        assert!(s.passes("api"));
        assert!(!s.passes("worker"));

        assert!(s.reset_to_defaults());
        assert!(!s.passes("api"));
        assert!(s.passes("worker"));
        assert!(!s.reset_to_defaults());
    }

    #[test]
    fn highlight_wraps_on_both_ends() {
        let mut s = state(&["a", "b", "c"]);
        s.enter_edit();
        // Rows: [All, a, b, c] — length 4.
        assert_eq!(s.highlight(), 0);
        s.highlight_prev();
        assert_eq!(s.highlight(), 3, "up from top wraps to bottom");
        s.highlight_next();
        assert_eq!(s.highlight(), 0, "down from bottom wraps to top");
    }

    #[test]
    fn highlight_clamps_when_rows_shrink_under_it() {
        let mut s = state(&["api", "db", "worker"]);
        s.enter_edit();
        // Rows: [All, api, db, worker]. Highlight last.
        s.highlight_next();
        s.highlight_next();
        s.highlight_next();
        assert_eq!(s.highlight(), 3);
        // Typing 'a' narrows rows to just "api" (no All row once query is
        // non-empty) — highlight clamps to 0.
        s.push_query_char('a');
        assert_eq!(s.rows().len(), 1);
        assert_eq!(s.highlight(), 0);
    }

    #[test]
    fn all_row_toggle_flips_between_all_and_none() {
        let mut s = state(&["api", "worker", "db"]);
        s.enter_edit();
        // Enter edit seeds editing_selected from active_selected — all names
        // are in by default, so the All row shows checked.
        assert!(s.all_selected_in_edit());
        s.toggle_highlighted(); // highlight is on All row → clear all
        assert!(!s.all_selected_in_edit());
        assert!(!s.is_edit_selected("api"));
        s.toggle_highlighted(); // flip back to all
        assert!(s.all_selected_in_edit());
        assert!(s.is_edit_selected("api"));
    }

    #[test]
    fn all_row_hidden_when_query_is_non_empty() {
        let mut s = state(&["api", "worker"]);
        s.enter_edit();
        assert!(matches!(s.rows().first(), Some(FilterRow::All)));
        s.push_query_char('a');
        // No All row in the filtered results — all entries are names.
        assert!(s.rows().iter().all(|r| matches!(r, FilterRow::Name(_))));
    }

    #[test]
    fn explicit_toggle_applies_even_when_result_is_empty() {
        let mut s = state(&["api", "worker"]);
        s.enter_edit();
        s.toggle_highlighted(); // All → clear everything
        assert!(s.active_names().is_empty());
        assert!(!s.passes("api"));
        assert!(!s.passes("worker"));
    }

    /// Esc's first level while typing: the query goes, the selection the
    /// query produced stays. There is no revert — the reader watched every
    /// change apply, so silently undoing them on the way out was a trap.
    #[test]
    fn clearing_the_query_keeps_the_selection_it_produced() {
        let mut s = state(&["api", "worker"]);
        s.enter_edit();
        s.begin_query_edit();
        s.push_query_char('a');
        s.select_only_highlighted();

        s.clear_query();

        assert!(s.passes("api"));
        assert!(!s.passes("worker"), "the narrowing survives");
        assert_eq!(s.query(), "");
        assert!(!s.query_editing());
    }

    #[test]
    fn toggle_applies_immediately_while_modal_is_open() {
        let mut s = state(&["api", "worker"]);
        s.enter_edit();
        s.highlight_next(); // api
        s.toggle_highlighted(); // remove api immediately

        assert!(!s.passes("api"));
        assert!(s.passes("worker"));
    }

    #[test]
    fn apply_query_selects_all_visible_matches() {
        let mut s = state(&["api", "worker", "web"]);
        s.enter_edit();
        s.begin_query_edit();
        s.push_query_char('w');
        s.apply_query();
        s.end_query_edit();

        assert!(!s.passes("api"));
        assert!(s.passes("worker"));
        assert!(s.passes("web"));
    }

    #[test]
    fn query_has_single_match_only_for_one_name() {
        let mut s = state(&["api", "worker", "web"]);
        s.enter_edit();
        s.begin_query_edit();
        assert!(!s.query_has_single_match(), "empty query does not count");

        s.push_query_char('a');
        assert!(s.query_has_single_match(), "one matching row");

        s.pop_query_char();
        s.push_query_char('w');
        assert!(!s.query_has_single_match(), "multiple matching rows");
    }

    #[test]
    fn passes_table_for_multi_select_active_filter() {
        struct Case {
            name: &'static str,
            selected: Vec<&'static str>,
            probe: &'static str,
            want: bool,
        }

        let cases = vec![
            Case {
                name: "selected name passes",
                selected: vec!["api"],
                probe: "api",
                want: true,
            },
            Case {
                name: "unselected name gated",
                selected: vec!["api"],
                probe: "worker",
                want: false,
            },
            Case {
                name: "lifecycle (empty name) always passes",
                selected: vec!["api"],
                probe: "",
                want: true,
            },
            Case {
                name: "multi-selected: one of many passes",
                selected: vec!["api", "worker"],
                probe: "worker",
                want: true,
            },
            Case {
                name: "multi-selected: not in set gated",
                selected: vec!["api", "worker"],
                probe: "db",
                want: false,
            },
        ];

        for case in cases {
            let mut s = state(&["api", "worker", "db"]);
            s.enter_edit();
            // Start from a clean edit selection: toggle-all off (highlight
            // is on All row, seeded with everything selected).
            s.toggle_highlighted();
            for selected in &case.selected {
                // Walk rows in order to find the target name. Bounded by
                // rows.len() so a missing name doesn't loop forever.
                for _ in 0..s.rows().len() {
                    match s.rows().get(s.highlight()) {
                        Some(FilterRow::Name(n)) if n == *selected => break,
                        _ => s.highlight_next(),
                    }
                }
                s.toggle_highlighted();
            }
            assert_eq!(s.passes(case.probe), case.want, "case: {}", case.name);
        }
    }

    #[test]
    fn hidden_from_display_omits_names_from_rows_but_keeps_selection() {
        struct Case {
            name: &'static str,
            hidden: Vec<&'static str>,
            query: &'static str,
            want_rows: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                name: "no hidden names — all rows present",
                hidden: vec![],
                query: "",
                want_rows: vec!["all", "api", "db", "worker"],
            },
            Case {
                name: "hides a single name from the empty-query list",
                hidden: vec!["db"],
                query: "",
                want_rows: vec!["all", "api", "worker"],
            },
            Case {
                name: "fuzzy matches still drop hidden names",
                hidden: vec!["api"],
                query: "a",
                want_rows: vec![],
            },
        ];

        for case in cases {
            let mut s = state(&["api", "worker", "db"]);
            let hidden: HashSet<String> = case.hidden.iter().map(|s| s.to_string()).collect();
            s.set_hidden_from_display(hidden);
            for ch in case.query.chars() {
                s.push_query_char(ch);
            }
            let actual: Vec<String> = s
                .rows()
                .iter()
                .map(|r| match r {
                    FilterRow::All => "all".to_string(),
                    FilterRow::Name(n) => n.clone(),
                })
                .collect();
            let want: Vec<String> = case.want_rows.iter().map(|s| s.to_string()).collect();
            assert_eq!(actual, want, "case: {}", case.name);
        }
    }

    #[test]
    fn hidden_from_display_does_not_affect_passes_for_visible_names() {
        let mut s = state(&["api", "worker", "db"]);
        // "db" still passes (it was in the default selection); hidden_from_display
        // only scopes the modal list, not the log-line allowlist. This matters
        // when a Lazy service later transitions and produces output — its
        // selection state must be preserved.
        let hidden: HashSet<String> = ["db"].iter().map(|s| s.to_string()).collect();
        s.set_hidden_from_display(hidden);
        assert!(s.passes("db"));
        assert!(s.passes("api"));
    }
}
