//! The log pane's `/` search: which lines are shown, and what is lit up.
//!
//! Two jobs, deliberately the same predicate. A line is admitted when it
//! matches, and the parts of it that matched are the parts highlighted — so
//! the pane can never show a line without showing why it is there.
//!
//! Matching runs against the message, not the rendered row: the `name │`
//! column is don's, not the process's, and searching for a service's name
//! would otherwise match every line it ever wrote.
//!
//! ## Substring by default
//!
//! The pane refilters on every keystroke, so the common case has to be one
//! where every intermediate state of a half-typed query is still a valid
//! query. A substring always is. Case-insensitive until the query contains a
//! capital, which is the `rg` and vim convention and the one people already
//! have in their fingers.
//!
//! Regex is a keystroke away (Ctrl+R) for the times a substring will not do.
//! It brings the thing substring matching avoids — a pattern that does not
//! compile, which is *most* patterns while they are being typed — so an
//! invalid pattern admits everything and says so, rather than emptying the
//! pane and leaving the reader to guess whether nothing matched or nothing
//! could.

/// How the query is interpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Mode {
    #[default]
    Substring,
    Regex,
}

/// The `/` search over the log pane.
#[derive(Debug, Default)]
pub(crate) struct LogSearch {
    query: String,
    /// Whether the prompt has the keyboard. The query outlives it: Enter
    /// confirms and leaves the search in force, as `/` does everywhere else.
    editing: bool,
    mode: Mode,
    /// The compiled pattern, rebuilt whenever the query or mode moves.
    /// `Some(Err)` is a regex that did not compile — kept rather than
    /// discarded so the prompt can say so.
    compiled: Option<Result<regex::Regex, String>>,
}

impl LogSearch {
    /// Open the prompt, keeping whatever was already being searched for.
    pub(crate) fn begin(&mut self) {
        self.editing = true;
    }

    /// Leave the prompt with the search still in force.
    pub(crate) fn confirm(&mut self) {
        self.editing = false;
    }

    /// Leave the prompt and stop searching.
    pub(crate) fn cancel(&mut self) {
        self.editing = false;
        self.query.clear();
        self.compiled = None;
    }

    pub(crate) fn editing(&self) -> bool {
        self.editing
    }

    pub(crate) fn query(&self) -> &str {
        &self.query
    }

    pub(crate) fn mode(&self) -> Mode {
        self.mode
    }

    /// Whether a search is in force — being typed or already confirmed.
    pub(crate) fn is_active(&self) -> bool {
        !self.query.is_empty()
    }

    /// The reason the current pattern matches nothing, if it cannot.
    pub(crate) fn error(&self) -> Option<&str> {
        match self.compiled.as_ref() {
            Some(Err(message)) => Some(message),
            _ => None,
        }
    }

    pub(crate) fn push(&mut self, c: char) {
        self.query.push(c);
        self.recompile();
    }

    /// Returns whether anything was removed, so a Backspace on an empty query
    /// can fall through to whatever else that key means.
    pub(crate) fn backspace(&mut self) -> bool {
        let popped = self.query.pop().is_some();
        if popped {
            self.recompile();
        }
        popped
    }

    /// Swap between substring and regex, keeping the query.
    pub(crate) fn toggle_mode(&mut self) {
        self.mode = match self.mode {
            Mode::Substring => Mode::Regex,
            Mode::Regex => Mode::Substring,
        };
        self.recompile();
    }

    fn recompile(&mut self) {
        self.compiled = match self.mode {
            Mode::Substring => None,
            Mode::Regex if self.query.is_empty() => None,
            Mode::Regex => Some(
                regex::RegexBuilder::new(&self.query)
                    .case_insensitive(!has_uppercase(&self.query))
                    .build()
                    .map_err(|e| {
                        // The last line of a regex error is the useful one;
                        // the rest is the pattern echoed back with a caret,
                        // which does not fit in a prompt.
                        e.to_string()
                            .lines()
                            .next_back()
                            .unwrap_or("invalid pattern")
                            .trim()
                            .to_string()
                    }),
            ),
        };
    }

    /// Whether `message` should be shown.
    ///
    /// Everything matches when nothing is being searched for, and everything
    /// matches when the pattern cannot compile — an empty pane is the one
    /// answer that cannot be told apart from "your query is wrong".
    pub(crate) fn admits(&self, message: &str) -> bool {
        if !self.is_active() {
            return true;
        }
        match self.compiled.as_ref() {
            Some(Ok(re)) => re.is_match(message),
            Some(Err(_)) => true,
            None => contains_smart_case(message, &self.query),
        }
    }

    /// Character ranges of `message` that matched, for highlighting.
    ///
    /// Character offsets rather than byte offsets, because the pane lays out
    /// and wraps in characters — a byte range would land in the wrong column
    /// the moment a line contains anything outside ASCII.
    pub(crate) fn match_ranges(&self, message: &str) -> Vec<(usize, usize)> {
        if !self.is_active() {
            return Vec::new();
        }
        let byte_ranges: Vec<(usize, usize)> = match self.compiled.as_ref() {
            Some(Ok(re)) => re
                .find_iter(message)
                .filter(|m| !m.is_empty())
                .map(|m| (m.start(), m.end()))
                .collect(),
            Some(Err(_)) => Vec::new(),
            None => substring_ranges(message, &self.query),
        };
        to_char_ranges(message, &byte_ranges)
    }

    /// Feed everything the pane's admission depends on into a hasher.
    ///
    /// The row index is keyed on this: a query that changed which lines are
    /// admitted without changing the key would leave the pane positioned by
    /// one set of rows and painted from another.
    pub(crate) fn fingerprint(&self, hasher: &mut impl std::hash::Hasher) {
        use std::hash::Hash;
        self.query.hash(hasher);
        // Same query, different meaning.
        (self.mode == Mode::Regex).hash(hasher);
        // An invalid pattern admits everything; a valid one does not.
        self.error().is_some().hash(hasher);
    }
}

fn has_uppercase(query: &str) -> bool {
    query.chars().any(char::is_uppercase)
}

/// Substring search, case-insensitive until the query contains a capital.
fn contains_smart_case(haystack: &str, needle: &str) -> bool {
    if has_uppercase(needle) {
        haystack.contains(needle)
    } else {
        haystack.to_lowercase().contains(&needle.to_lowercase())
    }
}

/// Byte ranges of every occurrence of `needle`, under the same case rule.
fn substring_ranges(haystack: &str, needle: &str) -> Vec<(usize, usize)> {
    if needle.is_empty() {
        return Vec::new();
    }
    let (hay, pin) = if has_uppercase(needle) {
        (haystack.to_string(), needle.to_string())
    } else {
        (haystack.to_lowercase(), needle.to_lowercase())
    };
    // Lowercasing can change byte lengths, so offsets from the folded string
    // are only usable when it is the same length as what the pane will draw.
    if hay.len() != haystack.len() {
        return Vec::new();
    }
    let mut found = Vec::new();
    let mut from = 0;
    while let Some(at) = hay[from..].find(&pin) {
        let start = from + at;
        found.push((start, start + pin.len()));
        from = start + pin.len();
    }
    found
}

/// Byte ranges to character ranges, in one pass over the string.
fn to_char_ranges(text: &str, byte_ranges: &[(usize, usize)]) -> Vec<(usize, usize)> {
    if byte_ranges.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(byte_ranges.len());
    for &(start, end) in byte_ranges {
        let chars_before = text[..start.min(text.len())].chars().count();
        let chars_in = text
            .get(start..end)
            .map_or(0, |slice| slice.chars().count());
        out.push((chars_before, chars_before + chars_in));
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    /// What is admitted and what is lit up are the same question asked twice,
    /// so they are tested together: a line shown with nothing highlighted
    /// would be a line with no visible reason to be there.
    #[test]
    fn a_line_is_admitted_exactly_when_something_in_it_is_highlighted() {
        struct Case {
            name: &'static str,
            query: &'static str,
            regex: bool,
            message: &'static str,
            want_admitted: bool,
            /// Character ranges, as the pane counts columns.
            want_ranges: &'static [(usize, usize)],
        }

        let cases = vec![
            Case {
                name: "a plain substring",
                query: "error",
                regex: false,
                message: "GET /api error 500",
                want_admitted: true,
                want_ranges: &[(9, 14)],
            },
            Case {
                name: "every occurrence is lit, not just the first",
                query: "ab",
                regex: false,
                message: "ab cd ab",
                want_admitted: true,
                want_ranges: &[(0, 2), (6, 8)],
            },
            Case {
                name: "lowercase query ignores case",
                query: "error",
                regex: false,
                message: "GET /api ERROR 500",
                want_admitted: true,
                want_ranges: &[(9, 14)],
            },
            Case {
                name: "a capital makes it case-sensitive",
                query: "Error",
                regex: false,
                message: "GET /api error 500",
                want_admitted: false,
                want_ranges: &[],
            },
            Case {
                name: "no match, no line",
                query: "missing",
                regex: false,
                message: "GET /api error 500",
                want_admitted: false,
                want_ranges: &[],
            },
            Case {
                // Columns, not bytes: the pane wraps and highlights by
                // character, so a multi-byte prefix must not shift the range.
                name: "offsets are columns, not bytes",
                query: "world",
                regex: false,
                message: "héllo wörld",
                want_admitted: false,
                want_ranges: &[],
            },
            Case {
                name: "a multi-byte line still highlights the right columns",
                query: "wörld",
                regex: false,
                message: "héllo wörld",
                want_admitted: true,
                want_ranges: &[(6, 11)],
            },
            Case {
                name: "regex alternation",
                query: "warn|error",
                regex: true,
                message: "a warn and an error",
                want_admitted: true,
                want_ranges: &[(2, 6), (14, 19)],
            },
            Case {
                name: "an empty query admits everything and lights nothing",
                query: "",
                regex: false,
                message: "GET /api error 500",
                want_admitted: true,
                want_ranges: &[],
            },
        ];

        for case in cases {
            let mut search = LogSearch::default();
            if case.regex {
                search.toggle_mode();
            }
            for c in case.query.chars() {
                search.push(c);
            }
            assert_eq!(
                search.admits(case.message),
                case.want_admitted,
                "{}: admitted",
                case.name
            );
            assert_eq!(
                search.match_ranges(case.message),
                case.want_ranges,
                "{}: ranges",
                case.name
            );
        }
    }

    /// A pattern being typed is a pattern that does not compile yet.
    ///
    /// Emptying the pane on every keystroke of `(foo|bar)` would make regex
    /// mode unusable, and an empty pane cannot be told apart from a query that
    /// genuinely matched nothing. So an invalid pattern admits everything and
    /// the prompt carries the reason.
    #[test]
    fn a_half_typed_pattern_shows_everything_and_says_why() {
        let mut search = LogSearch::default();
        search.toggle_mode();
        for c in "(foo".chars() {
            search.push(c);
        }

        assert!(search.error().is_some(), "an unclosed group cannot compile");
        assert!(
            search.admits("nothing like the query"),
            "an invalid pattern must not empty the pane"
        );
        assert!(
            search.match_ranges("nothing like the query").is_empty(),
            "and lights nothing, since nothing matched"
        );

        search.push(')');
        assert!(search.error().is_none(), "closing it compiles");
        assert!(!search.admits("nothing like the query"));
        assert!(search.admits("foo"));
    }

    /// Enter leaves the search in force; Esc ends it. The query outliving the
    /// prompt is what makes `/` a filter rather than a modal.
    #[test]
    fn confirming_keeps_the_search_and_cancelling_ends_it() {
        let mut search = LogSearch::default();
        search.begin();
        for c in "error".chars() {
            search.push(c);
        }
        assert!(search.editing() && search.is_active());

        search.confirm();
        assert!(!search.editing(), "the prompt is gone");
        assert!(search.is_active(), "the search is not");
        assert!(!search.admits("no match here"));

        search.begin();
        search.cancel();
        assert!(!search.editing() && !search.is_active());
        assert!(search.admits("no match here"), "everything is back");
    }

    /// The row index is keyed on this, so anything that changes which lines
    /// are admitted has to change it.
    #[test]
    fn the_fingerprint_moves_with_everything_admission_depends_on() {
        let print = |f: &dyn Fn(&mut LogSearch)| {
            use std::hash::Hasher;
            let mut search = LogSearch::default();
            f(&mut search);
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            search.fingerprint(&mut hasher);
            hasher.finish()
        };

        let empty = print(&|_| {});
        let typed = print(&|s| s.push('a'));
        let more = print(&|s| {
            s.push('a');
            s.push('b');
        });
        let as_regex = print(&|s| {
            s.toggle_mode();
            s.push('a');
        });

        assert_ne!(empty, typed, "typing changes what is admitted");
        assert_ne!(typed, more, "and so does typing more");
        assert_ne!(typed, as_regex, "the same query means something else");

        // Confirming the prompt admits exactly what it already did, so the
        // index must not be thrown away for it.
        let live = print(&|s| {
            s.begin();
            s.push('a');
        });
        let confirmed = print(&|s| {
            s.begin();
            s.push('a');
            s.confirm();
        });
        assert_eq!(live, confirmed, "closing the prompt admits no differently");
    }
}
