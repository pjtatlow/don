//! Text selection in the log pane, and getting it onto the clipboard.
//!
//! In the alternate screen the terminal's own drag-select is gone: mouse
//! capture takes the events before the emulator sees them. So don owns
//! selection, which turns out to be the better deal — it can copy the *log
//! text* without the `api    | ` prefix don itself added, which native
//! selection never could.
//!
//! ## What a selection is
//!
//! Two places in the *log*: a line id and a character offset into that line's
//! message. Not two places on the screen.
//!
//! Screen coordinates were the first design and they read as the honest one —
//! what the reader dragged across is what was on screen. But the screen is the
//! one thing here that will not hold still. Output arrives, lines are evicted,
//! the reader scrolls, the terminal is resized, the filter changes: every one
//! of those moves the text under the coordinates, and a selection pinned to
//! them ends up marking whatever landed there instead. The scroll anchor in
//! [`super::logs`] gave up screen rows for a line id for exactly this reason.
//!
//! An id and an offset survive all of it. A line that is evicted takes its end
//! of the selection with it, which is right — that text is genuinely gone.
//!
//! The offset is into the message, so don's own `api    │ ` column simply is
//! not addressable: the highlight and the copied text cannot disagree about
//! where the log starts, because neither of them can point at the prefix.
//!
//! ## When it copies
//!
//! When the button comes up, whatever the gesture was — a drag, or the word or
//! message a multi-click picked out. Highlighting text is the request, the same
//! as it is in the terminal's own drag-select this replaces, so nothing further
//! is asked of the reader. `y` re-copies a selection that still stands.
//!
//! The release is the only moment that copies, because it is the only one that
//! knows what was selected: a double click picks a word on the press, but the
//! reader may then drag out of it without ever letting go.
//!
//! ## Clipboard transport
//!
//! A local macOS session writes through `pbcopy`. Everywhere else, copying
//! writes `ESC ] 52 ; c ; <base64> BEL` to the terminal, which asks it to set
//! the system clipboard. OSC 52 works over ssh and inside tmux, where reaching
//! for a local clipboard API would not: the escape travels the same path as the
//! rest of the output.
//!
//! OSC 52 has neither a capability probe nor a success acknowledgement. `$TERM`
//! is not enough to infer support — Terminal.app and terminals that implement
//! OSC 52 all commonly advertise `xterm-256color`. Known-negative terminal
//! identities are rejected, and every other OSC 52 write is reported as sent,
//! not confirmed copied.

use super::log_store::LogStore;
use super::logs::RowSource;
use super::view_index::ViewIndex;
use crate::output::LogId;

/// One end of a selection: a line, and how far into its message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Point {
    pub(crate) id: LogId,
    pub(crate) offset: usize,
}

impl Point {
    /// The place a screen column on `row` points at.
    pub(crate) fn at(row: RowSource, column: usize) -> Self {
        Self {
            id: row.id,
            offset: row.offset_at(column),
        }
    }
}

/// A drag in progress, or a finished selection.
///
/// The fixed end is a *range*, not a point, which is what a double-click needs:
/// having picked out a word, dragging past either side of it has to keep the
/// whole word in — dragging left keeps its end, dragging right keeps its start.
/// A plain drag is the degenerate case where the range is one point wide, and
/// falls out of the same arithmetic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Selection {
    anchor: Option<Point>,
    /// The far side of the anchor range. Equal to `anchor` for a plain drag.
    anchor_end: Option<Point>,
    cursor: Option<Point>,
    /// True once the button is released: the selection stands until the next
    /// drag starts, or the reader dismisses it.
    settled: bool,
}

impl Selection {
    /// Start a drag.
    pub(crate) fn begin(&mut self, at: Point) {
        self.begin_range(at, at);
    }

    /// Start a drag from a range that is already selected — the word a double
    /// click found, or the message a triple click did.
    ///
    /// The button is still down at this point, so this deliberately does *not*
    /// settle: what follows may well be a drag, and treating it as finished is
    /// what used to stop a double-click-drag from scrolling or copying.
    pub(crate) fn begin_range(&mut self, start: Point, end: Point) {
        self.anchor = Some(start);
        self.anchor_end = Some(end);
        self.cursor = Some(end);
        self.settled = false;
    }

    /// Move the loose end.
    pub(crate) fn extend(&mut self, to: Point) {
        if self.anchor.is_some() {
            self.cursor = Some(to);
        }
    }

    /// The button came up.
    pub(crate) fn finish(&mut self) {
        if self.anchor.is_some() {
            self.settled = true;
        }
    }

    pub(crate) fn clear(&mut self) {
        *self = Self::default();
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.span().is_none()
    }

    /// Whether a drag is in flight: begun, and the button not yet up.
    ///
    /// Distinct from "has a span" — a drag that has not moved off its anchor
    /// yet is still a drag, and is exactly when edge-autoscroll should be
    /// arming rather than waiting for the reader to select a character first.
    pub(crate) fn is_dragging(&self) -> bool {
        self.anchor.is_some() && !self.settled
    }

    /// Normalised (start, end) in reading order, or `None` for a click that
    /// never became a drag.
    ///
    /// The cursor extends whichever side of the anchor range it has passed,
    /// and the range itself is never given up: a cursor still inside it — a
    /// double click that has not been dragged anywhere yet — leaves the word
    /// exactly as it was found. Ids ascend with time and offsets ascend within
    /// a line, so comparing points gives reading order for free.
    pub(crate) fn span(&self) -> Option<(Point, Point)> {
        let (start, end) = (self.anchor?, self.anchor_end?);
        let cursor = self.cursor?;
        let (start, end) = if cursor < start {
            (cursor, end)
        } else if cursor > end {
            (start, cursor)
        } else {
            (start, end)
        };
        if start == end {
            return None;
        }
        Some((start, end))
    }

    /// Whether a character of the log falls inside the selection. Half-open at
    /// the end, like a drag: the cell the pointer stopped on is not included.
    pub(crate) fn contains(&self, at: Point) -> bool {
        let Some((start, end)) = self.span() else {
            return false;
        };
        at >= start && at < end
    }
}

/// Punctuation that always ends a word.
///
/// Given by exclusion — anything not listed here and not a joiner is part of a
/// word — so letters outside ASCII are word characters without having to be
/// enumerated, and so `-`, `_`, `/`, `@`, `+`, `~`, `#`, `$` and `%` keep
/// holding together the identifiers, paths, flags and scoped names that a log
/// is mostly made of.
fn ends_word(c: char) -> bool {
    c.is_whitespace()
        || matches!(
            c,
            '"' | '\''
                | '`'
                | '('
                | ')'
                | '['
                | ']'
                | '{'
                | '}'
                | '<'
                | '>'
                | ','
                | ';'
                | '='
                | '\\'
                | '|'
                | '&'
                | '!'
                | '?'
                | '*'
                | '^'
        )
}

/// Punctuation that belongs to a word only when it sits *between* two word
/// characters.
///
/// `.` and `:` are load-bearing in the middle of a thing and noise at the edge
/// of one. In the middle they hold together `1.5s`, `v1.2.3`, `127.0.0.1:8080`
/// and `//tools/nodejs:install`; at the edge they are the full stop ending
/// "build completed." and the separator in `"key":42`, where taking them along
/// hands the reader `completed.` and `:42`.
fn joins_word(c: char) -> bool {
    matches!(c, '.' | ':')
}

/// Whether the character at `at` is part of a word.
fn is_word_char(chars: &[char], at: usize) -> bool {
    let Some(&c) = chars.get(at) else {
        return false;
    };
    if ends_word(c) {
        return false;
    }
    if !joins_word(c) {
        return true;
    }
    // A joiner needs something on both sides to join. Another joiner does not
    // count, so `a..b` is two words rather than one.
    let solid = |index: Option<usize>| {
        index
            .and_then(|index| chars.get(index))
            .is_some_and(|&c| !ends_word(c) && !joins_word(c))
    };
    solid(at.checked_sub(1)) && solid(Some(at + 1))
}

/// What kind of thing a character is, for the purpose of a double-click.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    Space,
    Word,
    Punct,
}

/// The class of the character at `at`, or `None` past the end.
///
/// Position-aware, because a joiner's class depends on what surrounds it: the
/// `:` in `//pkg:target` is [`Class::Word`] and the one in `"key":42` is
/// [`Class::Punct`]. That is what lets a run be grown by comparing classes
/// alone, with the joiner rule falling out rather than being re-stated.
fn class_at(chars: &[char], at: usize) -> Option<Class> {
    let &c = chars.get(at)?;
    if c.is_whitespace() {
        return Some(Class::Space);
    }
    Some(if is_word_char(chars, at) {
        Class::Word
    } else {
        Class::Punct
    })
}

/// The half-open character range of the thing under `offset`, if there is one.
///
/// A double-click takes a maximal run of one class. On a word that is the
/// word; on punctuation it is the punctuation, which is what terminals do and
/// what makes `":` in `{"key":42}` selectable as a unit.
///
/// Two rules decide where a word ends: a small set of punctuation always ends
/// one, and `.`/`:` end one unless they sit between two word characters.
/// Everything else — letters, digits, `-`, `_`, `/`, `@` and the rest — holds
/// a word together.
///
/// This was whitespace-delimited once, on the reasoning that a log's paths,
/// ids and durations should come out whole and a table of word characters
/// argues with itself over `/`, `-`, `:` and `.`. It does, and the rules above
/// are what settle those four. What the old rule could not survive was
/// structured output: JSON has no spaces to speak of, so a double-click
/// anywhere inside `{"name":"api","count":42}` selected the whole object.
///
/// `None` past the end or on whitespace, so a double-click on empty space
/// selects nothing rather than something arbitrary. Punctuation is not empty
/// space — there is something under the pointer, and it comes back.
///
/// Runs over the whole message rather than one rendered row, so a word the
/// pane wrapped in the middle still comes out whole.
pub(crate) fn word_at(message: &str, offset: usize) -> Option<(usize, usize)> {
    let chars: Vec<char> = message.chars().collect();
    let class = class_at(&chars, offset)?;
    if class == Class::Space {
        return None;
    }
    let mut start = offset;
    while start > 0 && class_at(&chars, start - 1) == Some(class) {
        start -= 1;
    }
    let mut end = offset + 1;
    while class_at(&chars, end) == Some(class) {
        end += 1;
    }
    Some((start, end))
}

/// The selected text, read from the store.
///
/// From the store and not from the rows on screen, because the selection is
/// allowed to outlive the view of it: scroll away from a selection and it is
/// still a selection, and copying it should still give the text. The index
/// supplies which lines are admitted, so a filtered-out line inside the span
/// is skipped exactly as it is on screen.
pub(crate) fn selected_text(
    selection: &Selection,
    index: &ViewIndex,
    store: &LogStore,
) -> Option<String> {
    let (start, end) = selection.span()?;
    let mut lines: Vec<String> = Vec::new();
    for id in index.ids_from(start.id) {
        if id > end.id {
            break;
        }
        let Some(entry) = store.get(id) else {
            continue;
        };
        let message: Vec<char> = entry.message_text().chars().collect();
        let from = if id == start.id { start.offset } else { 0 };
        let until = if id == end.id {
            end.offset.min(message.len())
        } else {
            message.len()
        };
        let from = from.min(until);
        lines.push(
            message
                .get(from..until)
                .unwrap_or_default()
                .iter()
                .collect(),
        );
    }

    // Trailing blanks are the padding a pane row carries, not content.
    while lines.last().is_some_and(|line| line.trim().is_empty()) {
        lines.pop();
    }
    if lines.is_empty() {
        return None;
    }
    Some(
        lines
            .iter()
            .map(|line| line.trim_end())
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

/// How a successful clipboard write was transported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CopyMethod {
    /// A local clipboard program exited successfully.
    Native,
    /// An unacknowledged request was sent to the terminal.
    Osc52,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClipboardStrategy {
    NativeMacOs,
    Osc52,
    Unsupported,
}

/// Put `text` on the clipboard using the most reliable available transport.
pub(crate) fn copy_to_clipboard(text: &str) -> std::io::Result<CopyMethod> {
    let remote = remote_session();
    let term_program = std::env::var_os("TERM_PROGRAM");
    match clipboard_strategy(cfg!(target_os = "macos"), remote, term_program.as_deref()) {
        ClipboardStrategy::NativeMacOs => {
            copy_with_pbcopy(text)?;
            Ok(CopyMethod::Native)
        }
        ClipboardStrategy::Osc52 => {
            write_osc52(text)?;
            Ok(CopyMethod::Osc52)
        }
        ClipboardStrategy::Unsupported => Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Terminal.app does not support OSC 52; hold Shift while selecting, then press Cmd+C",
        )),
    }
}

fn clipboard_strategy(
    macos: bool,
    remote: bool,
    term_program: Option<&std::ffi::OsStr>,
) -> ClipboardStrategy {
    if macos && !remote {
        ClipboardStrategy::NativeMacOs
    } else if term_program.is_some_and(|program| program == "Apple_Terminal") {
        ClipboardStrategy::Unsupported
    } else {
        ClipboardStrategy::Osc52
    }
}

fn remote_session() -> bool {
    ["SSH_CONNECTION", "SSH_CLIENT", "SSH_TTY", "MOSH_CONNECTION"]
        .iter()
        .any(|name| std::env::var_os(name).is_some())
}

/// Write through the local macOS pasteboard helper.
fn copy_with_pbcopy(text: &str) -> std::io::Result<()> {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let mut child = Command::new("/usr/bin/pbcopy")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;

    let write_result = match child.stdin.take() {
        Some(mut stdin) => stdin.write_all(text.as_bytes()),
        None => Err(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "pbcopy stdin was not available",
        )),
    };
    let status_result = child.wait();
    write_result?;
    let status = status_result?;
    if status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "pbcopy exited with {status}"
        )))
    }
}

/// Ask the terminal to put `text` on the system clipboard, via OSC 52.
///
/// Written straight to stdout rather than through ratatui: it is a request to
/// the terminal, not a cell to paint, and it must not be diffed away.
fn write_osc52(text: &str) -> std::io::Result<()> {
    use std::io::Write;
    let mut stdout = std::io::stdout();
    let encoded = base64_encode(text.as_bytes());
    write!(stdout, "\x1b]52;c;{encoded}\x07")?;
    stdout.flush()
}

/// Base64, standard alphabet with padding.
///
/// Hand-rolled to avoid a dependency for forty lines; OSC 52 is the only thing
/// in don that needs it.
fn base64_encode(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[(triple >> 18) as usize & 0x3F] as char);
        out.push(ALPHABET[(triple >> 12) as usize & 0x3F] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(triple >> 6) as usize & 0x3F] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[triple as usize & 0x3F] as char
        } else {
            '='
        });
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn clipboard_strategy_uses_native_macos_and_rejects_terminal_app_osc52() {
        struct Case {
            name: &'static str,
            macos: bool,
            remote: bool,
            term_program: Option<&'static str>,
            want: ClipboardStrategy,
        }

        let cases = vec![
            Case {
                name: "local Terminal.app uses the macOS pasteboard",
                macos: true,
                remote: false,
                term_program: Some("Apple_Terminal"),
                want: ClipboardStrategy::NativeMacOs,
            },
            Case {
                name: "every local macOS terminal uses the macOS pasteboard",
                macos: true,
                remote: false,
                term_program: Some("iTerm.app"),
                want: ClipboardStrategy::NativeMacOs,
            },
            Case {
                name: "remote Terminal.app is known not to support OSC 52",
                macos: false,
                remote: true,
                term_program: Some("Apple_Terminal"),
                want: ClipboardStrategy::Unsupported,
            },
            Case {
                name: "remote macOS must target the local terminal clipboard",
                macos: true,
                remote: true,
                term_program: Some("iTerm.app"),
                want: ClipboardStrategy::Osc52,
            },
            Case {
                name: "unknown terminals get the portable request",
                macos: false,
                remote: false,
                term_program: None,
                want: ClipboardStrategy::Osc52,
            },
        ];

        for case in cases {
            assert_eq!(
                clipboard_strategy(
                    case.macos,
                    case.remote,
                    case.term_program.map(std::ffi::OsStr::new),
                ),
                case.want,
                "{}",
                case.name
            );
        }
    }

    /// Exercise the real pasteboard only on an isolated macOS CI runner. A
    /// normal local test run must never replace the developer's clipboard.
    #[cfg(target_os = "macos")]
    #[test]
    fn native_macos_clipboard_round_trips_in_ci() {
        if std::env::var_os("CI").is_none() {
            return;
        }

        let text = "don macOS clipboard smoke test";
        assert_eq!(copy_to_clipboard(text).unwrap(), CopyMethod::Native);

        let output = std::process::Command::new("/usr/bin/pbpaste")
            .output()
            .unwrap();
        assert!(output.status.success(), "pbpaste failed: {}", output.status);
        assert_eq!(output.stdout, text.as_bytes());
    }

    #[test]
    fn base64_matches_the_standard_alphabet_and_padding() {
        struct Case {
            input: &'static str,
            want: &'static str,
        }

        // The RFC 4648 test vectors: the padding cases are where hand-rolled
        // encoders go wrong, and a mispadded OSC 52 payload is silently ignored
        // by the terminal rather than reported.
        let cases = vec![
            Case {
                input: "",
                want: "",
            },
            Case {
                input: "f",
                want: "Zg==",
            },
            Case {
                input: "fo",
                want: "Zm8=",
            },
            Case {
                input: "foo",
                want: "Zm9v",
            },
            Case {
                input: "foob",
                want: "Zm9vYg==",
            },
            Case {
                input: "fooba",
                want: "Zm9vYmE=",
            },
            Case {
                input: "foobar",
                want: "Zm9vYmFy",
            },
        ];

        for case in cases {
            assert_eq!(
                base64_encode(case.input.as_bytes()),
                case.want,
                "input {:?}",
                case.input
            );
        }
    }

    fn store_of(lines: &[(u64, &str, &str)], width: u16) -> LogStore {
        let mut store = LogStore::with_capacity(100);
        store.reflow(width);
        for (id, name, body) in lines {
            store.push(
                LogId(*id),
                crate::output::FormattedLogLine {
                    name: (*name).to_string(),
                    is_lifecycle: false,
                    is_verbose: false,
                    prefix: format!("{name} | ").into_bytes(),
                    bytes: body.as_bytes().to_vec(),
                },
            );
        }
        store
    }

    fn index_of(store: &LogStore, width: u16, hide: &str) -> ViewIndex {
        let mut index = ViewIndex::default();
        index.sync(
            store,
            super::super::view_index::ViewKey { width, filter: 0 },
            &std::collections::HashMap::new(),
            |entry| entry.line.name != hide,
        );
        index
    }

    fn at(id: u64, offset: usize) -> Point {
        Point {
            id: LogId(id),
            offset,
        }
    }

    fn drag(from: Point, to: Point) -> Selection {
        let mut selection = Selection::default();
        selection.begin(from);
        selection.extend(to);
        selection.finish();
        selection
    }

    /// Dragging out of a double-clicked word keeps the whole word, whichever
    /// way it goes: the fixed end is the *far side* of the word, so dragging
    /// backwards holds its end rather than collapsing onto its start.
    #[test]
    fn a_drag_out_of_a_word_keeps_the_whole_word() {
        struct Case {
            name: &'static str,
            cursor: Point,
            want: Option<(Point, Point)>,
        }

        // A double click that found the word spanning offsets 6..11 on line 2.
        let (word_start, word_end) = (at(2, 6), at(2, 11));

        let cases = vec![
            Case {
                name: "not dragged anywhere: the word, as found",
                cursor: word_end,
                want: Some((word_start, word_end)),
            },
            Case {
                name: "dragged back within the word: still the whole word",
                cursor: at(2, 8),
                want: Some((word_start, word_end)),
            },
            Case {
                name: "dragged left on the same line keeps the word's end",
                cursor: at(2, 2),
                want: Some((at(2, 2), word_end)),
            },
            Case {
                name: "dragged up a line keeps the word's end",
                cursor: at(1, 4),
                want: Some((at(1, 4), word_end)),
            },
            Case {
                name: "dragged right keeps the word's start",
                cursor: at(2, 20),
                want: Some((word_start, at(2, 20))),
            },
            Case {
                name: "dragged down a line keeps the word's start",
                cursor: at(3, 3),
                want: Some((word_start, at(3, 3))),
            },
        ];

        for case in cases {
            let mut selection = Selection::default();
            selection.begin_range(word_start, word_end);
            selection.extend(case.cursor);
            assert_eq!(selection.span(), case.want, "{}", case.name);
        }
    }

    /// A multi-click leaves the button down, so the selection it produces is
    /// still a drag in flight — which is what lets it scroll at the edges and
    /// copy when released.
    #[test]
    fn a_multi_click_selection_is_still_a_drag() {
        let mut selection = Selection::default();
        selection.begin_range(at(2, 6), at(2, 11));
        assert!(selection.is_dragging(), "the button has not come up yet");

        selection.finish();
        assert!(!selection.is_dragging(), "released");

        // And a plain drag behaves the same way through the same states.
        let mut plain = Selection::default();
        plain.begin(at(1, 0));
        assert!(plain.is_dragging());
        plain.extend(at(1, 5));
        assert!(plain.is_dragging());
        plain.finish();
        assert!(!plain.is_dragging());
    }

    /// A drag is direction-agnostic and half-open at the far end, so dragging
    /// back over a character deselects it rather than leaving it stuck.
    #[test]
    fn a_selection_reads_the_same_dragged_either_way() {
        struct Case {
            name: &'static str,
            selection: Selection,
            inside: Vec<Point>,
            outside: Vec<Point>,
        }

        let cases = vec![
            Case {
                name: "within one line",
                selection: drag(at(0, 2), at(0, 5)),
                inside: vec![at(0, 2), at(0, 4)],
                outside: vec![at(0, 1), at(0, 5), at(1, 0)],
            },
            Case {
                name: "dragged back is the same span",
                selection: drag(at(0, 5), at(0, 2)),
                inside: vec![at(0, 2), at(0, 4)],
                outside: vec![at(0, 1), at(0, 5)],
            },
            Case {
                name: "across lines takes everything between",
                selection: drag(at(0, 3), at(2, 1)),
                inside: vec![at(0, 3), at(0, 99), at(1, 0), at(1, 40), at(2, 0)],
                outside: vec![at(0, 2), at(2, 1), at(3, 0)],
            },
            Case {
                name: "a click that never moved is not a selection",
                selection: drag(at(1, 4), at(1, 4)),
                inside: vec![],
                outside: vec![at(1, 4)],
            },
        ];

        for case in cases {
            for point in case.inside {
                assert!(case.selection.contains(point), "{}: {point:?}", case.name);
            }
            for point in case.outside {
                assert!(!case.selection.contains(point), "{}: {point:?}", case.name);
            }
        }
    }

    /// The point of anchoring to the log: everything that moves the text on
    /// screen — scrolling, a resize, a filter change — leaves the selection
    /// covering the same characters.
    #[test]
    fn a_selection_survives_everything_that_moves_the_screen() {
        let store = store_of(
            &[
                (0, "api", "the first line"),
                (1, "web", "a line from the other service"),
                (
                    2,
                    "api",
                    "a much longer line that will wrap differently at each width",
                ),
            ],
            40,
        );
        // "line that will" out of the third message, chosen to straddle a wrap
        // boundary at one width and not at another.
        let selection = drag(at(2, 14), at(2, 28));

        struct Case {
            name: &'static str,
            width: u16,
            hide: &'static str,
        }

        for case in [
            Case {
                name: "as laid out",
                width: 40,
                hide: "",
            },
            Case {
                name: "narrower, so the line wraps more",
                width: 24,
                hide: "",
            },
            Case {
                name: "wide enough not to wrap at all",
                width: 200,
                hide: "",
            },
            Case {
                name: "with the other service filtered out",
                width: 40,
                hide: "web",
            },
        ] {
            let mut store = store_of(
                &[
                    (0, "api", "the first line"),
                    (1, "web", "a line from the other service"),
                    (
                        2,
                        "api",
                        "a much longer line that will wrap differently at each width",
                    ),
                ],
                case.width,
            );
            store.reflow(case.width);
            let index = index_of(&store, case.width, case.hide);
            assert_eq!(
                selected_text(&selection, &index, &store).as_deref(),
                Some("line that will"),
                "{}",
                case.name
            );
        }
        let _ = store;
    }

    /// don's `name | ` column is not part of the message, so no offset can
    /// point at it and the copied text can never contain it.
    #[test]
    fn the_name_column_is_not_selectable() {
        let store = store_of(&[(0, "api", "hello world")], 40);
        let index = index_of(&store, 40, "");
        // A row whose message starts six columns in; columns inside the prefix
        // clamp to the message's first character.
        let row = RowSource {
            id: LogId(0),
            offset: 0,
            indent: 6,
        };
        assert_eq!(Point::at(row, 0).offset, 0, "the far left is the message");
        assert_eq!(
            Point::at(row, 5).offset,
            0,
            "and so is the last prefix cell"
        );
        assert_eq!(Point::at(row, 6).offset, 0);
        assert_eq!(Point::at(row, 9).offset, 3);

        let whole = drag(Point::at(row, 0), Point::at(row, 100));
        assert_eq!(
            selected_text(&whole, &index, &store).as_deref(),
            Some("hello world"),
            "no prefix in the copy"
        );
    }

    /// A message the pane wrapped is one thing to the reader; the wrap is this
    /// pane's layout, not something the process wrote.
    #[test]
    fn a_wrapped_message_is_copied_as_one_line() {
        let store = store_of(&[(0, "api", "one two three four five six seven")], 20);
        let index = index_of(&store, 20, "");
        let all = drag(at(0, 0), at(0, usize::MAX));
        let text = selected_text(&all, &index, &store).unwrap();
        assert_eq!(text, "one two three four five six seven");
        assert!(!text.contains('\n'), "the wrap is not a newline");
    }

    /// Lines the filter excludes are not on screen, so they are not in the
    /// copy either — even when the selection spans across them.
    #[test]
    fn a_filtered_out_line_inside_the_span_is_skipped() {
        let store = store_of(
            &[
                (0, "api", "first"),
                (1, "web", "hidden"),
                (2, "api", "second"),
            ],
            40,
        );
        let index = index_of(&store, 40, "web");
        let across = drag(at(0, 0), at(2, usize::MAX));
        assert_eq!(
            selected_text(&across, &index, &store).as_deref(),
            Some("first\nsecond")
        );
    }

    /// A double-click finds the word in the message, so one the pane wrapped
    /// still comes out whole — and finds a *word*, not everything between two
    /// spaces, which in structured output is the whole line.
    #[test]
    fn word_at_takes_whole_words_out_of_the_message() {
        struct Case {
            name: &'static str,
            message: &'static str,
            /// Clicked on the first character of this substring.
            at: &'static str,
            want: Option<&'static str>,
        }

        let json =
            r#"level=info msg={"name":"redo-analytics","count":42,"path":"/foo/bar"} took=14ms"#;

        for case in [
            // The reason this changed: every one of these used to select the
            // entire `msg={...}` blob, because JSON has no spaces in it.
            Case {
                name: "a json key",
                message: json,
                at: "name",
                want: Some("name"),
            },
            Case {
                name: "a json string value keeps its hyphen",
                message: json,
                at: "redo-analytics",
                want: Some("redo-analytics"),
            },
            Case {
                name: "an unquoted json value leaves the colon behind",
                message: json,
                at: "42",
                want: Some("42"),
            },
            Case {
                name: "a path inside json is still one word",
                message: json,
                at: "/foo/bar",
                want: Some("/foo/bar"),
            },
            Case {
                name: "an equals splits a key from its value",
                message: json,
                at: "info",
                want: Some("info"),
            },
            Case {
                name: "a duration keeps its unit",
                message: json,
                at: "14ms",
                want: Some("14ms"),
            },
            // What the whitespace rule got right, and still does.
            Case {
                name: "a path is one word",
                message: "GET /api/v1/users 200 14ms",
                at: "/api/v1/users",
                want: Some("/api/v1/users"),
            },
            Case {
                name: "from the first character of one",
                message: "GET /api/v1/users 200 14ms",
                at: "GET",
                want: Some("GET"),
            },
            Case {
                name: "a bazel label keeps its colon",
                message: "building //tools/nodejs:install now",
                at: "//tools",
                want: Some("//tools/nodejs:install"),
            },
            Case {
                name: "a host and port stay together",
                message: "listening on 127.0.0.1:8080",
                at: "127",
                want: Some("127.0.0.1:8080"),
            },
            Case {
                name: "a version keeps its dots",
                message: "don v1.2.3-rc.1 starting",
                at: "v1",
                want: Some("v1.2.3-rc.1"),
            },
            Case {
                name: "a full stop is not part of the word it follows",
                message: "build completed.",
                at: "completed",
                want: Some("completed"),
            },
            Case {
                name: "a bracketed level comes out without its brackets",
                message: "[INFO] ready",
                at: "INFO",
                want: Some("INFO"),
            },
            Case {
                name: "whitespace selects nothing",
                message: "GET /api/v1/users",
                at: " /api",
                want: None,
            },
            // Punctuation is a run of its own — what terminals do, and what
            // makes the separator in a JSON object selectable as a unit.
            Case {
                name: "punctuation comes back as its own run",
                message: r#"{"a":1}"#,
                at: r#"{"#,
                want: Some(r#"{""#),
            },
            Case {
                name: "a quote and the colon beside it are one run",
                message: r#"{"key":42}"#,
                at: r#"":4"#,
                want: Some(r#"":"#),
            },
            Case {
                name: "a full stop on its own is punctuation, not a word",
                message: "build completed.",
                at: ".",
                want: Some("."),
            },
            Case {
                name: "an opening bracket is one character wide",
                message: "[INFO] ready",
                at: "[",
                want: Some("["),
            },
        ] {
            let byte = case.message.find(case.at).expect(case.name);
            let offset = case.message[..byte].chars().count();
            let got = word_at(case.message, offset).map(|(start, end)| {
                case.message
                    .chars()
                    .skip(start)
                    .take(end - start)
                    .collect::<String>()
            });
            assert_eq!(got.as_deref(), case.want, "{}", case.name);
        }
    }

    /// Past the end is not a word, however far past.
    #[test]
    fn word_at_past_the_end_selects_nothing() {
        assert_eq!(word_at("GET /api", 500), None);
        assert_eq!(word_at("", 0), None);
    }
}
