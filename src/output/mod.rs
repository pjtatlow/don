//! Output handling — line buffering, color-coded prefixing, and lifecycle events.
//!
//! Each output destination (stdout, log files) is a **sink** — an independent
//! writer task with an `mpsc` channel. Services and lifecycle events send lines
//! to sinks via the channel sender. A single stdout sink task ensures no
//! interleaving. Sinks can be added/removed at runtime for `don logs` tailing.
//!
//! Each service has a [`ServiceWriter`] handle that pushes lines to the per-service
//! ring buffer and fans out to the service's current sinks. The ring buffer persists
//! across restarts, and [`ServiceWriter`] is cloneable for reuse.

mod actor;
pub mod attach;
pub mod emulator;
pub(crate) mod osc;
pub(crate) mod ring_buffer;
pub(crate) mod sanitize;

use bytes::Bytes;
use crossterm::style::{Attribute, Color, ResetColor, SetAttribute, SetForegroundColor};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::{Mutex, broadcast, mpsc, watch};
use tokio::task::JoinHandle;

/// Default ring buffer capacity per service (lines).
const DEFAULT_RING_BUFFER_CAPACITY: usize = 10_000;
const MAX_FILTER_PENDING: usize = 16 * 1024;

/// Name stamped on `[don]` lifecycle events so the TUI filter can treat them
/// like any other service/task: gated when a filter is active, selectable
/// from the filter dropdown. Must not collide with a user service/task name.
pub const LIFECYCLE_EVENT_NAME: &str = "don";

/// Terminal colors for normal service/task name prefixes.
const SERVICE_COLORS: &[Color] = &[
    Color::Cyan,
    Color::Yellow,
    Color::Magenta,
    Color::Green,
    Color::Blue,
    Color::Red,
    Color::DarkCyan,
    Color::DarkYellow,
    Color::DarkMagenta,
    Color::DarkGreen,
    Color::DarkBlue,
    Color::DarkRed,
    Color::AnsiValue(208),
    Color::AnsiValue(45),
    Color::AnsiValue(141),
    Color::AnsiValue(118),
    Color::AnsiValue(81),
    Color::AnsiValue(203),
    Color::AnsiValue(220),
    Color::AnsiValue(39),
];

/// Dedicated build-tool colors so these synthetic streams stay neutral and
/// never collide with the rotating service palette.
const BAZEL_COLOR: Color = Color::Grey;

/// One unit of input to a PTY-backed process, funneled through its gate.
#[derive(Debug)]
pub enum PtyInput {
    /// Bytes to write. Frame-atomic: the gate performs one write per frame,
    /// so interleaved writers (several attached clients, the OSC scanner)
    /// cannot shear a paste mid-blob.
    Frame(Vec<u8>),
    /// Resize the PTY to `(cols, rows)`.
    Resize(u16, u16),
}

/// Spawn the input gate for one PTY-backed spawn. The gate owns the write
/// half for the process's whole lifetime; everything that wants to write —
/// attach bridges, the OSC scanner, resize — holds a sender. The gate (and
/// with it the PTY master's write side) ends when the last sender drops:
/// the process's stored sender on exit, the scanner with its sink, and each
/// bridge on disconnect.
pub(crate) fn spawn_pty_gate(mut pty_write: pty_process::OwnedWritePty) -> mpsc::Sender<PtyInput> {
    let (tx, mut rx) = mpsc::channel::<PtyInput>(64);
    tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        while let Some(input) = rx.recv().await {
            match input {
                PtyInput::Frame(bytes) => {
                    if pty_write.write_all(&bytes).await.is_err() {
                        return;
                    }
                }
                PtyInput::Resize(cols, rows) => {
                    let _ = pty_write.resize(pty_process::Size::new(rows, cols));
                }
            }
        }
    });
    tx
}

/// Handle to an active OSC response scanner. Exists for its [`Drop`]: a
/// restart replaces the process's handle, and dropping the old one removes its
/// sink and releases its gate sender.
pub struct OscSinkHandle {
    /// Our copy of the sender. Dropping it *and* removing the process's copy
    /// from the sinks list closes the channel, stopping the task.
    handle: Option<SinkHandle>,
    /// The scanner task, which holds a gate sender until its channel closes.
    join: Option<JoinHandle<()>>,
    output: actor::OutputHandle,
}

impl Drop for OscSinkHandle {
    /// Stop the scanner when the handle is dropped — e.g. when a restart
    /// replaces `osc_sink` or a service stops. Without this the task lives
    /// on (the process's copy of the channel sender stays in the sinks
    /// list, so the channel never closes), keeping a gate sender alive and
    /// with it one PTY master per restart.
    fn drop(&mut self) {
        // Aborting the task drops its gate sender.
        if let Some(join) = self.join.take() {
            join.abort();
        }
        // Remove the process's lingering copy of our sender so the dead sink
        // stops receiving lines. This used to need a spawned task purely to
        // take the sinks lock from a `Drop`; posting a message needs neither
        // a runtime nor an await.
        if let Some(handle) = self.handle.take() {
            self.output.remove_sink(handle);
        }
    }
}

/// Register an OSC response sink on a process and start its scanner.
///
/// The sink uses bounded-drop semantics so it never blocks the output
/// pipeline.
fn spawn_osc_sink(output: actor::OutputHandle, pty_input: mpsc::Sender<PtyInput>) -> OscSinkHandle {
    let (tx, rx) = mpsc::channel::<SinkLine>(16);
    let handle = SinkHandle::BoundedDrop(tx);
    output.add_sink(handle.clone());
    OscSinkHandle {
        handle: Some(handle),
        join: Some(tokio::spawn(osc_sink_task(rx, pty_input))),
        output,
    }
}

/// A message to a sink. Sinks receive these and write them to their destination.
pub struct SinkLine {
    /// Formatted prefix (color-coded service name, or bold [don] for lifecycle).
    /// Empty for file sinks (raw output).
    pub prefix: Bytes,
    /// The raw line content (no newline).
    pub line: Bytes,
    /// The service/task name this line belongs to. `[don]` lifecycle events
    /// use [`LIFECYCLE_EVENT_NAME`] so they participate in the TUI filter
    /// like any other entry — selectable, but hidden when the filter is
    /// active and doesn't include them.
    pub name: String,
    /// True for `[don]`-prefixed lifecycle events ("api: stopping...",
    /// "shutting down gracefully", build progress). False for raw service
    /// stdout/stderr. The TUI uses this to keep lifecycle events visible
    /// during shutdown even for services the user has filtered out — a
    /// hidden kafka should still surface "send SIGTERM" without flooding
    /// the screen with kafka's own (filtered) shutdown chatter.
    pub is_lifecycle: bool,
    /// True for verbose diagnostic messages. Always emitted and always
    /// recorded — each consumer decides whether to *display* them (the TUI's
    /// local `v` toggle, the stdout writer's `-v` flag, a follower's own
    /// filter). Tagging at emission instead of gating there is what makes
    /// verbose history revealable after the fact.
    pub is_verbose: bool,
}

/// A handle to a sink. Clone the sender to subscribe a service to it.
///
/// Two flavors:
///
/// - **Unbounded**: don's own internal pipeline (stdout/TUI sink, file sinks,
///   lifecycle events). Send always succeeds unless the receiver has been
///   dropped. We don't impose backpressure here — a noisy service is the
///   user's problem to filter, not ours to throttle. Better to let memory
///   grow briefly than silently drop control-plane events ("send SIGTERM…",
///   "stopping…") because some service is spamming.
/// - **BoundedDrop**: slow external consumers (`don logs --follow` HTTP
///   clients, OSC response sinks). When their channel fills, drop the sink
///   entirely so a stuck client can't stall don's output.
#[derive(Clone)]
pub(crate) enum SinkHandle {
    Unbounded(mpsc::UnboundedSender<SinkLine>),
    BoundedDrop(mpsc::Sender<SinkLine>),
    /// Feeds the server-side terminal emulator (see [`emulator`]). Carries
    /// only the raw bytes; the emulator thread keys screens by the line's
    /// process name.
    Emulator(mpsc::UnboundedSender<emulator::EmulatorRequest>),
    /// The stdout writer: unbounded, so an ordinary burst is never dropped and
    /// a lifecycle event can always be sent from a sync context — but *metered*,
    /// so the writer can tell a burst from a process that will outrun it
    /// forever. Without the meter the queue is the only thing between a runaway
    /// service and the machine's memory, and it has no bound at all.
    Metered {
        tx: mpsc::UnboundedSender<SinkLine>,
        /// Bytes queued and not yet consumed. Added here, subtracted by
        /// `stdout_sink_task` as it takes each line.
        queued: Arc<std::sync::atomic::AtomicUsize>,
        /// Lines shed since the writer last said so.
        shed: Arc<std::sync::atomic::AtomicU64>,
    },
}

impl SinkHandle {
    /// Send a line. Returns `Err(())` if the sink should be pruned —
    /// receiver dropped, or (for `BoundedDrop`) the consumer is too slow.
    pub fn send(&self, msg: SinkLine) -> Result<(), ()> {
        match self {
            Self::Unbounded(tx) => tx.send(msg).map_err(|_| ()),
            Self::BoundedDrop(tx) => tx.try_send(msg).map_err(|_| ()),
            Self::Emulator(tx) => tx
                .send(emulator::EmulatorRequest::Feed {
                    name: msg.name,
                    bytes: msg.line.to_vec(),
                })
                .map_err(|_| ()),
            Self::Metered { tx, queued, shed } => {
                // Shed here rather than in the writer, because the writer may
                // be parked inside a blocking write to a destination that has
                // stopped draining — a terminal that has stopped reading, a
                // pipe nobody is emptying. In that state it never reaches a
                // check of its own, and the queue is the only thing between a
                // runaway process and the machine's memory.
                //
                // don's own narration is never the flood and is always kept:
                // losing it loses the explanation for what is happening.
                if !msg.is_lifecycle && queued.load(Ordering::Relaxed) > SHED_HIGH_WATER {
                    shed.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
                queued.fetch_add(msg.line.len(), Ordering::Relaxed);
                tx.send(msg).map_err(|_| ())
            }
        }
    }

    pub fn is_closed(&self) -> bool {
        match self {
            Self::Unbounded(tx) => tx.is_closed(),
            Self::BoundedDrop(tx) => tx.is_closed(),
            Self::Emulator(tx) => tx.is_closed(),
            Self::Metered { tx, .. } => tx.is_closed(),
        }
    }

    pub fn same_channel(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Unbounded(a), Self::Unbounded(b)) => a.same_channel(b),
            (Self::BoundedDrop(a), Self::BoundedDrop(b)) => a.same_channel(b),
            (Self::Emulator(a), Self::Emulator(b)) => a.same_channel(b),
            (Self::Metered { tx: a, .. }, Self::Metered { tx: b, .. }) => a.same_channel(b),
            _ => false,
        }
    }

    /// True when this sink should be dropped on `close_follow_sinks` —
    /// follow/OSC sinks for transient external clients, not stdout/file.
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::BoundedDrop(_))
    }
}

/// A fully formatted, sanitized log line emitted by the stdout pipeline.
///
/// Emitted on the merged log-stream tap ([`OutputManager::log_stream_sender`])
/// for every follower — the TUI feeds each line into `terminal.insert_before`
/// (preserving native scrollback) and stamps the `name` for filter matching.
/// The bytes already include any verbose-mode timestamp and the color-coded
/// service prefix; the consumer just renders them as-is.
#[derive(Debug, Clone)]
pub struct FormattedLogLine {
    /// Owning service/task name. `[don]` lifecycle events carry
    /// [`LIFECYCLE_EVENT_NAME`] so the filter treats them as a selectable
    /// entry rather than always passing them.
    pub name: String,
    /// True for `[don]`-prefixed lifecycle events; false for raw service
    /// stdout/stderr. Lets the TUI keep lifecycle events visible even when
    /// the source service is filtered out (esp. during shutdown).
    pub is_lifecycle: bool,
    /// True for verbose diagnostic messages; see [`SinkLine::is_verbose`].
    pub is_verbose: bool,
    /// The rendered left column: the padded, coloured process name and its
    /// separator, plus the elapsed-time stamp when verbose is on. Empty for a
    /// line that has no owning process.
    ///
    /// Sent beside the message rather than glued to the front of it. A consumer
    /// that wants one string concatenates the two; one that wants to lay the
    /// name out as a column — the TUI, which indents what wraps underneath it —
    /// has the split already, instead of searching the text for a separator to
    /// recover something the sink had just finished joining.
    ///
    /// Rendered here rather than at each consumer so that padding and colour
    /// are decided once: pipe mode and the TUI cannot disagree about how wide
    /// the column is or what colour a service gets.
    pub prefix: Vec<u8>,
    /// The message, sanitized, with whatever styling the process itself emitted.
    /// No prefix and no trailing newline — the renderer supplies both.
    pub bytes: Vec<u8>,
}

/// Shared runtime control for verbose lifecycle and watch logging.
///
/// Clones point at the same atomic flag, so the TUI can toggle verbosity
/// live while the stdout sink and background emitters observe the change
/// immediately.
#[derive(Clone, Debug)]
pub struct VerbosityControl {
    enabled: Arc<AtomicBool>,
}

impl VerbosityControl {
    fn new(enabled: bool) -> Self {
        Self {
            enabled: Arc::new(AtomicBool::new(enabled)),
        }
    }

    /// Return whether verbose logging is currently enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Set verbose logging on or off for all current emitters and sinks.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }
}

/// Allowlist applied at the stdout sink so pipe-mode runs can scope output
/// to a subset of services without a TUI filter modal. When the inner
/// `OnceLock` is unset, no filter is active and everything passes.
///
/// When set, a line passes when any of:
/// - its `name` is in the allowlist (the user's chosen services), or
/// - its `name` is [`LIFECYCLE_EVENT_NAME`] (top-level `[don]` events —
///   shutdown progress, errors, restart info — are control plane and stay
///   unconditionally visible so the user can still tell what the runner
///   is doing), or
/// - its `name` is empty (only the end-of-stream accumulator flush emits
///   nameless lines; partial lines at shutdown should always surface).
///
/// Per-service lifecycle events (`[don] api: restarted`) carry the service
/// name, so they're allowlisted with the service — matching the TUI filter.
///
/// Set once at startup via [`OutputManager::set_log_filter`] before the
/// runner spawns any service. Subsequent set attempts are silently ignored.
#[derive(Clone, Debug, Default)]
struct LogFilterControl {
    allowlist: Arc<std::sync::OnceLock<HashSet<String>>>,
}

impl LogFilterControl {
    fn passes(&self, name: &str) -> bool {
        match self.allowlist.get() {
            None => true,
            Some(set) => name.is_empty() || name == LIFECYCLE_EVENT_NAME || set.contains(name),
        }
    }

    fn set(&self, names: HashSet<String>) {
        let _ = self.allowlist.set(names);
    }
}

/// Which processes must not reach the terminal's stdout.
///
/// Two questions used to be one: what don *records* for its clients, and what
/// it *writes to the terminal*. They were answered together by sink wiring — a
/// service with `log = "file"` simply had no stdout sink — which meant it never
/// reached the stdout writer, and therefore never reached the merged log tap
/// either. Every client except the TUI could see that service (the ring buffer
/// is filled upstream of any sink), so the TUI was the one consumer with an
/// incomplete record.
///
/// Now every process feeds the writer, and the terminal decision happens at the
/// end, after the tap has already been told. Two sources feed it: `log = "file"`
/// and `log = "ignore"`, fixed at construction; and processes with a client
/// attached, muted for as long as the attach lasts so the attached terminal is
/// the only one drawing them.
/// The two sets are kept apart on purpose: a config mute is permanent, an
/// attach mute is for the length of the attach, and a process can be under
/// both. Merging them would let a detach hand a `log = "ignore"` service a
/// terminal it was never meant to have — which is exactly the bug the old
/// `stdout_paused` flag existed to prevent.
#[derive(Clone, Default)]
struct StdoutMuteControl {
    by_config: Arc<std::sync::RwLock<HashSet<String>>>,
    attached: Arc<std::sync::RwLock<HashSet<String>>>,
}

impl StdoutMuteControl {
    fn new(by_config: HashSet<String>) -> Self {
        Self {
            by_config: Arc::new(std::sync::RwLock::new(by_config)),
            attached: Arc::new(std::sync::RwLock::new(HashSet::new())),
        }
    }

    /// Whether this process's output should be kept off the terminal.
    ///
    /// Two uncontended read locks per emitted line, against an async publish
    /// and a formatted-byte build in the same function — not a hot path worth
    /// contorting for. A poisoned lock reads as "not muted": showing too much
    /// beats silently swallowing the terminal's output.
    fn is_muted(&self, name: &str) -> bool {
        self.contains(&self.by_config, name) || self.is_attached(name)
    }

    /// Whether a client currently holds this process's terminal.
    ///
    /// Narrower than [`Self::is_muted`], and the difference is the whole point:
    /// a config mute keeps output off the terminal but still records it, while
    /// an attach means the output is not lines at all. An interactive program
    /// under an attach is redrawing a screen — key echo, prompt repaints,
    /// cursor moves — and every fragment of that became a log entry, shredded
    /// through the merged stream between the lines other processes were
    /// writing. The attached client is watching that screen in its window; the
    /// log has nothing to gain by also holding the wreckage of it.
    fn is_attached(&self, name: &str) -> bool {
        self.contains(&self.attached, name)
    }

    fn contains(&self, set: &std::sync::RwLock<HashSet<String>>, name: &str) -> bool {
        set.read().map(|set| set.contains(name)).unwrap_or(false)
    }

    /// Mute a process added after construction with `log = "ignore"`.
    fn mute_by_config(&self, name: &str) {
        if let Ok(mut set) = self.by_config.write() {
            set.insert(name.to_string());
        }
    }

    /// Mute `name` while a client holds its terminal.
    fn attach(&self, name: &str) {
        if let Ok(mut set) = self.attached.write() {
            set.insert(name.to_string());
        }
    }

    /// Give `name` back to the terminal.
    fn release(&self, name: &str) {
        if let Ok(mut set) = self.attached.write() {
            set.remove(name);
        }
    }
}

#[derive(Clone, Debug, Default)]
struct CompiledLogKeepFilter {
    patterns: Vec<regex::bytes::Regex>,
}

impl CompiledLogKeepFilter {
    fn from_config(
        name: &str,
        config: Option<&crate::config::LogFilterConfig>,
    ) -> Result<Self, OutputError> {
        let Some(config) = config else {
            return Ok(Self::default());
        };
        let mut patterns = Vec::with_capacity(config.patterns.len());
        for pattern in &config.patterns {
            let compiled = regex::bytes::Regex::new(pattern).map_err(|source| {
                OutputError::InvalidLogFilter {
                    name: name.to_string(),
                    pattern: pattern.clone(),
                    source,
                }
            })?;
            patterns.push(compiled);
        }
        Ok(Self { patterns })
    }

    fn is_empty(&self) -> bool {
        self.patterns.is_empty()
    }

    fn keeps(&self, line: &[u8]) -> bool {
        self.patterns.iter().any(|pattern| pattern.is_match(line))
    }
}

/// Where the stdout sink task sends its formatted lines.
///
/// Pipe-mode output writes bytes directly to an `AsyncWrite` (today's stdout).
/// The TUI is not a target: it follows [`OutputManager::log_stream_sender`]
/// and TUI-mode callers pass a null writer.
enum StdoutTarget<W: tokio::io::AsyncWrite + Unpin + Send> {
    Writer(tokio::io::BufWriter<W>),
}

/// How much output the writer batches before it has to hit the OS.
///
/// This used to be unbuffered: two `write_all` calls — the line, then the
/// newline — per log line, so two syscalls each. A process logging faster than
/// syscalls retire meant the writer fell permanently behind its queue, and the
/// queue is unbounded. Buffering raises the ceiling by orders of magnitude and
/// costs nothing, because the buffer is flushed the moment the writer catches
/// up (see `stdout_sink_task`) — so output is never held back when someone is
/// watching for it, only when there is already a backlog.
const WRITE_BUFFER: usize = 64 * 1024;

/// Queued output past which lines start being shed rather than enqueued.
///
/// Deliberately generous: a quarter of a gigabyte is far more than any burst a
/// build or a test run produces, so nothing anyone wanted to read is ever at
/// risk. It is a backstop against a process in a loop, not a throttle — a
/// service is never slowed down, it just stops being able to add to a backlog
/// nothing will ever read.
const SHED_HIGH_WATER: usize = 256 * 1024 * 1024;

impl<W: tokio::io::AsyncWrite + Unpin + Send> StdoutTarget<W> {
    fn new(writer: W) -> Self {
        Self::Writer(tokio::io::BufWriter::with_capacity(WRITE_BUFFER, writer))
    }

    /// Push buffered bytes to the OS. Called when the queue drains.
    async fn flush(&mut self) {
        use tokio::io::AsyncWriteExt;
        match self {
            Self::Writer(writer) => {
                let _ = writer.flush().await;
            }
        }
    }
}

/// Per-service handle for writing output. Cloneable, reusable across restarts.
///
/// Holds an `Arc` to the service's state in `OutputManager`. Multiple
/// writers can be created for the same service (e.g. on restart), all
/// sharing the same ring buffer.
#[derive(Clone)]
pub struct ServiceWriter {
    output: actor::OutputHandle,
}

impl ServiceWriter {
    /// Process an async readable stream (from a child process) as raw chunks.
    ///
    /// Reads raw byte chunks and broadcasts them to all sinks. Each sink
    /// decides its own buffering strategy (the stdout sink accumulates
    /// per-service until `\n`, the ring buffer splits on `\n`, attach/follow
    /// sinks forward immediately, the OSC sink detects terminal queries and
    /// writes responses). No UTF-8 assumption — binary output is handled
    /// correctly. Runs until EOF (the child closes its output).
    pub async fn process_stream<R: AsyncRead + Unpin>(
        &self,
        mut reader: R,
    ) -> Result<(), OutputError> {
        let mut buf = [0u8; 8192];

        loop {
            match read_chunk(&mut reader, &mut buf).await {
                Ok(0) => break, // EOF
                // Awaiting the send is the backpressure: a child that
                // outruns its own output actor is made to wait, exactly as
                // it waited on the lock this replaced.
                Ok(n) => self.output.chunk(Bytes::copy_from_slice(&buf[..n])).await,
                Err(e) => return Err(e),
            }
        }

        // Flush any partial line remaining in the ring buffer, and wait for
        // it: whoever awaits this reader is entitled to assume the process's
        // output has landed, not merely been queued.
        self.output.flush().await;
        Ok(())
    }

    /// Close all transient (follow/attach) sinks. Called when the process
    /// stream ends (process exited) so that attach sessions and log followers
    /// detect the closure and exit instead of blocking forever.
    ///
    /// Only removes transient sinks (follow/OSC). Persistent sinks (stdout,
    /// file) are kept for the next process lifecycle.
    pub async fn close_follow_sinks(&self) {
        self.output.close_follow_sinks();
    }

    /// Write a single line to the ring buffer and sinks.
    ///
    /// Used for structured output like Docker build progress that arrives
    /// as individual text lines rather than a byte stream. Appends `\n` to
    /// the data so sinks can flush immediately.
    pub async fn write_line(&self, line: &str) {
        self.output.line(Bytes::from(format!("{line}\n"))).await;
    }
}

fn reserved_color(name: &str) -> Option<Color> {
    match name {
        "bazel" => Some(BAZEL_COLOR),
        _ => None,
    }
}

/// Assigns a deterministic color to each service/task/build-tool name.
pub(crate) fn assign_colors(names: &[&str]) -> HashMap<String, Color> {
    let mut sorted: Vec<&str> = names.to_vec();
    sorted.sort_unstable();

    let mut next_service_color = 0usize;
    let mut assigned = HashMap::with_capacity(sorted.len());
    for name in sorted {
        let color = reserved_color(name).unwrap_or_else(|| {
            let color = SERVICE_COLORS[next_service_color % SERVICE_COLORS.len()];
            next_service_color += 1;
            color
        });
        assigned.insert(name.to_string(), color);
    }
    assigned
}

/// Format a command + args pair into a shell-ish one-liner for debug logs.
///
/// Any argument containing whitespace, quotes, or shell metacharacters gets
/// single-quoted with embedded `'` escaped as `'\''`. Safe to copy-paste
/// into a shell.
pub(crate) fn format_cmdline<S: AsRef<str>>(cmd: &str, args: &[S]) -> String {
    let mut out = shell_quote(cmd);
    for arg in args {
        out.push(' ');
        out.push_str(&shell_quote(arg.as_ref()));
    }
    out
}

fn shell_quote(s: &str) -> String {
    let needs_quoting = s.is_empty()
        || s.bytes().any(|b| {
            !(b.is_ascii_alphanumeric()
                || b == b'_'
                || b == b'-'
                || b == b'.'
                || b == b'/'
                || b == b':'
                || b == b'='
                || b == b',')
        });
    if !needs_quoting {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        if ch == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

/// Build a formatted prefix as bytes for a service name.
///
/// The separator is the box-drawing `│`, not the ASCII pipe: `│` fills the
/// cell top to bottom, so a column of them down the pane joins into one solid
/// rule, where `|` leaves a gap above and below and reads as a dashed line.
fn format_prefix(name: &str, color: Color, max_name_len: usize) -> Bytes {
    Bytes::from(format!(
        "{}{:width$}{} │ ",
        SetForegroundColor(color),
        name,
        ResetColor,
        width = max_name_len,
    ))
}

/// A cloneable, read-only handle to every process's buffered output.
///
/// This is to logs what [`StateReader`](crate::state_store::StateReader) is
/// to state: the server holds one and answers `GET /logs` (and the follow
/// variant) without a runner round trip, so reading logs never queues behind
/// whatever the runner is currently doing. The registry is republished
/// through a `watch`, so a name registered late (the build-tool prefix) is
/// visible to handles minted before it existed.
#[derive(Clone)]
pub struct LogReader {
    services: watch::Receiver<Arc<HashMap<String, actor::OutputHandle>>>,
}

impl LogReader {
    /// An empty reader for tests that need an `ApiState` without an
    /// `OutputManager`. Every lookup answers `None`.
    #[cfg(test)]
    pub(crate) fn for_tests() -> Self {
        let (tx, rx) = watch::channel(Arc::new(HashMap::new()));
        std::mem::forget(tx);
        Self { services: rx }
    }

    /// Read the last N lines from a process's ring buffer, joined by
    /// newlines. `None` if the name is not registered.
    pub async fn read_logs(&self, name: &str, n: usize) -> Option<Bytes> {
        let output = self.services.borrow().get(name)?.clone();
        Some(output.read_logs(n).await)
    }

    /// Attach a follow sink (see [`OutputManager::add_follow_sink`]).
    /// `None` if the name is not registered.
    pub async fn add_follow_sink(
        &self,
        name: &str,
        last_n: usize,
        live_capacity: usize,
    ) -> Option<mpsc::Receiver<SinkLine>> {
        let output = self.services.borrow().get(name)?.clone();
        output.follow_sink(last_n, live_capacity).await
    }
}

/// Manages output for all services — creates sinks, spawns writer tasks,
/// and provides lifecycle event formatting.
pub struct OutputManager {
    /// Per-service output state, retained for the lifetime of the program.
    services: HashMap<String, actor::OutputHandle>,
    /// The same registry, republished for [`LogReader`] handles. Almost
    /// always set once at construction; `register_service` republishes for
    /// the rare late registration (the build-tool prefix).
    services_watch: watch::Sender<Arc<HashMap<String, actor::OutputHandle>>>,
    /// The sink senders [`attach::AttachControl`] borrows, published so
    /// [`Self::shutdown`] can take them back before flushing — see
    /// `AttachSinks`.
    attach_sinks: watch::Sender<Option<attach::AttachSinks>>,
    /// The formatted `[don]` prefix, padded to align with service prefixes.
    don_prefix: String,
    /// Stdout sink sender — used for lifecycle events and service output.
    stdout_sink: SinkHandle,
    /// Which processes are kept off the terminal. Held so services added after
    /// construction can join the set.
    mute: StdoutMuteControl,
    /// Writer task JoinHandles for clean shutdown.
    writer_handles: Vec<JoinHandle<()>>,
    /// Shared runtime verbose mode — enables extra diagnostic lifecycle events
    /// and timestamps on stdout/TUI log lines.
    verbosity: VerbosityControl,
    /// Handle to the server-side terminal-emulator thread. Screens register
    /// per PTY-backed spawn; see [`emulator`].
    emulator: emulator::EmulatorHandle,
    /// Allowlist applied to stdout-bound lines. None until
    /// [`Self::set_log_filter`] is called; once set, only lines whose name
    /// is in the allowlist (or empty, for `[don]` lifecycle events) reach
    /// the writer. Ring buffers and file sinks are unaffected.
    log_filter: LogFilterControl,
    /// Cached formatted prefix for the synthetic "bazel" stream. Populated
    /// by [`Self::register_build_tool`]; `None` means build output falls
    /// back to a `[don]`-prefixed lifecycle event with a `bazel:` text
    /// prefix.
    bazel_prefix: Option<Bytes>,
    /// Broadcast tap + bounded history of the merged, formatted log stream.
    /// See [`MergedLogTap`].
    log_tap: MergedLogTap,
}

/// A line's place in the merged stream.
///
/// Assigned once, at publish, by the single producer — so every client numbers
/// the same line the same way. That is what makes "all clients see the same
/// view" a checkable claim rather than an aspiration: two clients can compare
/// cursors, a client can say exactly where it stopped, and a gap can be
/// measured instead of guessed at.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct LogId(pub u64);

impl LogId {
    /// The id before any line has been published.
    pub const ZERO: LogId = LogId(0);

    fn next(self) -> LogId {
        LogId(self.0.saturating_add(1))
    }
}

impl std::fmt::Display for LogId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// One line of the merged stream, with its place in it.
#[derive(Clone, Debug)]
pub struct MergedLine {
    pub id: LogId,
    pub line: Arc<FormattedLogLine>,
}

/// What the tap still holds from some point onward.
pub struct Catchup {
    /// Lines from the requested point that survived eviction, oldest first.
    pub lines: Vec<MergedLine>,
    /// The oldest id still held. Greater than what the caller asked for means
    /// the head was evicted and the difference is gone for good.
    pub first_id: LogId,
    /// The id the next published line will receive.
    pub next_id: LogId,
}

/// How many merged lines the tap holds by default.
///
/// This is the TUI's scrollback as well as a late joiner's preload, so it is
/// sized for "scroll back through this morning's startup", not just "what was
/// happening as I connected". The per-service rings behind `don logs <name>`
/// answer a different question — one service, deeper — and are bounded
/// separately.
pub const DEFAULT_MERGED_HISTORY_CAPACITY: usize = 50_000;

struct MergedHistory {
    entries: std::collections::VecDeque<MergedLine>,
    capacity: usize,
    next_id: LogId,
}

impl MergedHistory {
    /// Give `line` the next id and retain it, evicting the oldest if full.
    ///
    /// Returns the entry to broadcast. A capacity of zero still assigns an id —
    /// the stream is numbered whether or not anything is kept.
    fn append(&mut self, line: Arc<FormattedLogLine>) -> MergedLine {
        let id = self.next_id;
        self.next_id = id.next();
        let entry = MergedLine { id, line };
        if self.capacity > 0 {
            while self.entries.len() >= self.capacity {
                self.entries.pop_front();
            }
            self.entries.push_back(entry.clone());
        }
        entry
    }
}

/// The merged log stream's fan-out point: a broadcast of every line
/// [`stdout_sink_task`] emits (post filter, post sanitize), plus a bounded
/// history of the same lines.
///
/// History and broadcast carry the *same* `Arc`s — one allocation per line.
/// They also carry the same [`LogId`], which is what lets a follower that
/// subscribed first and snapshotted second splice the two exactly, and lets a
/// follower that fell behind ask for precisely what it missed. See
/// [`MergedLogCursor`], which does both.
#[derive(Clone)]
pub struct MergedLogTap {
    tx: broadcast::Sender<MergedLine>,
    history: Arc<Mutex<MergedHistory>>,
}

impl MergedLogTap {
    fn with_capacity(capacity: usize) -> Self {
        // Broadcast capacity trades memory for how far a slow follower may
        // fall behind before the tap's history has to cover for it; entries
        // are a pointer and an id.
        let (tx, _) = broadcast::channel(4096);
        Self {
            tx,
            history: Arc::new(Mutex::new(MergedHistory {
                entries: std::collections::VecDeque::new(),
                capacity,
                next_id: LogId::ZERO,
            })),
        }
    }

    /// Record and fan out one line, returning the id it was given.
    ///
    /// The id is assigned under the history lock, so ids and history order can
    /// never disagree.
    ///
    /// Every line published here is a new line, including a frame of an
    /// in-place progress redraw. A process that ends a line with a bare `\r`
    /// is repainting it, and this used to model that by re-publishing the
    /// existing id so the newest line was replaced in place. That made a
    /// line's height change after it had been laid out, which dragged the
    /// whole log pane up and down under a reader following the tail, and it
    /// only ever collapsed a repaint whose frame happened to still be the
    /// newest line in the *merged* stream — so two processes repainting at
    /// once collapsed neither, and no single answer could be right for two
    /// clients filtering differently. The terminal writer and the web UI
    /// always did append every frame; the tap is now the same.
    async fn publish(&self, line: Arc<FormattedLogLine>) -> LogId {
        let entry = {
            let mut history = self.history.lock().await;
            history.append(line)
        };
        // Send on a receiver-less broadcast is a cheap no-op.
        let id = entry.id;
        let _ = self.tx.send(entry);
        id
    }

    /// Subscribe to lines from now on. Prefer [`Self::cursor`], which heals
    /// its own gaps.
    pub fn subscribe(&self) -> broadcast::Receiver<MergedLine> {
        self.tx.subscribe()
    }

    /// A gap-healing cursor over the stream, starting from `since`.
    ///
    /// `None` starts at the tail: the last `tail` lines, then live.
    pub async fn cursor(&self, since: Option<LogId>, tail: usize) -> MergedLogCursor {
        // Subscribe *before* reading history, so anything published in between
        // arrives on the receiver rather than falling into the seam.
        let rx = self.tx.subscribe();
        let catchup = match since {
            Some(since) => self.catch_up(since).await,
            None => self.tail(tail).await,
        };
        let next = catchup
            .lines
            .last()
            .map(|entry| entry.id.next())
            .unwrap_or(catchup.next_id);
        MergedLogCursor {
            // The history handle, deliberately not the whole tap: holding a
            // tap clone would keep its broadcast sender alive, so the cursor
            // could never see the stream close — and every consumer that ends
            // when its log channel ends would hang instead.
            history: Arc::clone(&self.history),
            rx,
            next,
            pending: catchup.lines.into(),
        }
    }

    /// An empty tap for tests that need an `ApiState` without an
    /// `OutputManager`.
    #[cfg(test)]
    pub(crate) fn for_tests() -> Self {
        Self::with_capacity(DEFAULT_MERGED_HISTORY_CAPACITY)
    }

    /// Everything still held with `id >= since`.
    pub async fn catch_up(&self, since: LogId) -> Catchup {
        catch_up_from(&self.history, since).await
    }

    /// The last `n` lines, oldest first.
    pub async fn tail(&self, n: usize) -> Catchup {
        let history = self.history.lock().await;
        let skip = history.entries.len().saturating_sub(n);
        Catchup {
            lines: history.entries.iter().skip(skip).cloned().collect(),
            first_id: history
                .entries
                .front()
                .map(|e| e.id)
                .unwrap_or(history.next_id),
            next_id: history.next_id,
        }
    }
}

/// Everything a history still holds with `id >= since`.
async fn catch_up_from(history: &Mutex<MergedHistory>, since: LogId) -> Catchup {
    let history = history.lock().await;
    Catchup {
        lines: history
            .entries
            .iter()
            .filter(|entry| entry.id >= since)
            .cloned()
            .collect(),
        first_id: history
            .entries
            .front()
            .map(|entry| entry.id)
            .unwrap_or(history.next_id),
        next_id: history.next_id,
    }
}

/// What a [`MergedLogCursor`] hands back.
#[derive(Clone, Debug)]
pub enum MergedEvent {
    /// The next line in the stream.
    Line(MergedLine),
    /// The cursor fell behind and the tap's history had already moved past the
    /// gap, so `count` lines are gone for good. Reported rather than papered
    /// over: a client that silently thins its log is worse than one that says
    /// where the hole is.
    Dropped { count: u64, resumed_at: LogId },
}

/// A cursor over the merged stream that heals its own gaps.
///
/// A broadcast receiver that falls behind loses lines permanently, and every
/// consumer used to deal with that alone — the TUI wrote "log stream lagged"
/// into its own output and carried on with a hole; the API told the client a
/// number and no way to act on it. But the tap's history almost always still
/// has those lines. This re-reads them, so falling behind is invisible unless
/// the history itself has moved on.
pub struct MergedLogCursor {
    history: Arc<Mutex<MergedHistory>>,
    rx: broadcast::Receiver<MergedLine>,
    /// The id this cursor expects next. Everything below it has been handed
    /// out; everything at or above it has not.
    next: LogId,
    pending: std::collections::VecDeque<MergedLine>,
}

impl MergedLogCursor {
    /// The next event, or `None` once the stream has closed and the buffered
    /// lines are exhausted.
    pub async fn recv(&mut self) -> Option<MergedEvent> {
        loop {
            if let Some(entry) = self.pending.pop_front() {
                self.next = entry.id.next();
                return Some(MergedEvent::Line(entry));
            }
            match self.rx.recv().await {
                Ok(entry) => {
                    // Already handed out — the subscribe/snapshot overlap. Ids
                    // make this exact; it used to be pointer identity. Every
                    // id is published exactly once, so an id at or below the
                    // last one handed out is always the replay.
                    if entry.id < self.next {
                        continue;
                    }
                    self.next = entry.id.next();
                    return Some(MergedEvent::Line(entry));
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    let catchup = catch_up_from(&self.history, self.next).await;
                    if catchup.first_id > self.next {
                        let count = catchup.first_id.0.saturating_sub(self.next.0);
                        self.next = catchup.first_id;
                        self.pending = catchup.lines.into();
                        return Some(MergedEvent::Dropped {
                            count,
                            resumed_at: catchup.first_id,
                        });
                    }
                    self.pending = catchup.lines.into();
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }

    /// Whatever is already in hand, without waiting for more.
    ///
    /// For draining at shutdown: the tap has published its last lines and they
    /// are sitting in this receiver, but nothing further is coming, so
    /// [`Self::recv`] would park forever. Healing a gap needs an await, so a
    /// lag here is reported rather than repaired — a best-effort flush is the
    /// right shape for a stream that is closing anyway.
    pub fn try_recv(&mut self) -> Option<MergedEvent> {
        loop {
            if let Some(entry) = self.pending.pop_front() {
                self.next = entry.id.next();
                return Some(MergedEvent::Line(entry));
            }
            match self.rx.try_recv() {
                Ok(entry) => {
                    if entry.id < self.next {
                        continue;
                    }
                    self.next = entry.id.next();
                    return Some(MergedEvent::Line(entry));
                }
                Err(broadcast::error::TryRecvError::Lagged(count)) => {
                    return Some(MergedEvent::Dropped {
                        count,
                        resumed_at: self.next,
                    });
                }
                Err(_) => return None,
            }
        }
    }
}

/// Errors from output handling.
#[derive(Debug, thiserror::Error)]
pub enum OutputError {
    /// Failed to open a log file.
    #[error("failed to open log file '{path}': {source}")]
    FileOpen {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// I/O error reading from child output.
    #[error("error reading service output: {0}")]
    Read(#[from] std::io::Error),
    /// Invalid regex in a log keep filter.
    #[error("service '{name}': invalid log_filter regex '{pattern}': {source}")]
    InvalidLogFilter {
        name: String,
        pattern: String,
        #[source]
        source: regex::Error,
    },
}

impl OutputManager {
    /// Create a new output manager for the given services.
    ///
    /// `writer` is the stdout destination — `std::io::stdout()` in production,
    /// a test buffer in tests. It is consumed by a spawned writer task.
    /// Colors are assigned deterministically based on sorted service names.
    /// Prefixes are padded to the longest name for column alignment.
    pub async fn new<W: tokio::io::AsyncWrite + Unpin + Send + 'static>(
        services: &[(&str, &crate::config::LogConfig)],
        writer: W,
    ) -> Result<Self, OutputError> {
        Self::new_verbose(services, writer, false).await
    }

    /// Create a new output manager with per-service regex keep filters.
    pub async fn new_with_log_filters<W: tokio::io::AsyncWrite + Unpin + Send + 'static>(
        services: &[(&str, &crate::config::LogConfig)],
        log_filters: &HashMap<String, crate::config::LogFilterConfig>,
        writer: W,
    ) -> Result<Self, OutputError> {
        Self::new_verbose_with_log_filters(services, log_filters, writer, false).await
    }

    /// Create a new output manager. When `verbose` is true, every output
    /// line is prefixed with an elapsed timestamp.
    pub async fn new_verbose<W: tokio::io::AsyncWrite + Unpin + Send + 'static>(
        services: &[(&str, &crate::config::LogConfig)],
        writer: W,
        verbose: bool,
    ) -> Result<Self, OutputError> {
        Self::new_inner(
            services,
            &HashMap::new(),
            verbose,
            StdoutTarget::new(writer),
        )
        .await
    }

    /// Create a new verbose output manager with per-service regex keep filters.
    pub async fn new_verbose_with_log_filters<W: tokio::io::AsyncWrite + Unpin + Send + 'static>(
        services: &[(&str, &crate::config::LogConfig)],
        log_filters: &HashMap<String, crate::config::LogFilterConfig>,
        writer: W,
        verbose: bool,
    ) -> Result<Self, OutputError> {
        Self::new_inner(services, log_filters, verbose, StdoutTarget::new(writer)).await
    }

    // The TUI has no dedicated constructor: it subscribes to
    // [`Self::log_stream_sender`] like any other follower, and TUI-mode
    // callers pass `tokio::io::sink()` as the writer so nothing touches the
    // real stdout while the TUI owns the screen.

    async fn new_inner<W: tokio::io::AsyncWrite + Unpin + Send + 'static>(
        services: &[(&str, &crate::config::LogConfig)],
        log_filters: &HashMap<String, crate::config::LogFilterConfig>,
        verbose: bool,
        target: StdoutTarget<W>,
    ) -> Result<Self, OutputError> {
        let names: Vec<&str> = services.iter().map(|(n, _)| *n).collect();
        let color_map = assign_colors(&names);
        let max_name_len = names.iter().map(|n| n.len()).max().unwrap_or(0).max(5);
        let verbosity = VerbosityControl::new(verbose);
        let log_filter = LogFilterControl::default();
        let emulator = emulator::spawn_emulator_thread();

        // What the terminal must not show. Everything still reaches the
        // writer, and therefore the merged tap — see [`StdoutMuteControl`].
        let mute = StdoutMuteControl::new(
            services
                .iter()
                .filter(|(_, config)| {
                    matches!(
                        config,
                        crate::config::LogConfig::File(_) | crate::config::LogConfig::Ignore
                    )
                })
                .map(|(name, _)| (*name).to_string())
                .collect(),
        );

        // Spawn stdout sink task.
        let (stdout_tx, stdout_rx) = mpsc::unbounded_channel();
        let queued = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let shed = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let log_tap = MergedLogTap::with_capacity(DEFAULT_MERGED_HISTORY_CAPACITY);
        let stdout_handle = tokio::spawn(stdout_sink_task(
            stdout_rx,
            target,
            verbosity.clone(),
            log_filter.clone(),
            log_tap.clone(),
            mute.clone(),
            Arc::clone(&queued),
            Arc::clone(&shed),
        ));
        let stdout_sink = SinkHandle::Metered {
            tx: stdout_tx,
            queued,
            shed,
        };

        // Spawn file sink tasks (deduplicated by path).
        let mut file_sinks: HashMap<PathBuf, SinkHandle> = HashMap::new();
        let mut writer_handles = vec![stdout_handle];

        for (_, config) in services {
            if let crate::config::LogConfig::File(path) = config
                && !file_sinks.contains_key(path)
            {
                let file = open_log_file(path).await?;
                let (tx, rx) = mpsc::unbounded_channel();
                writer_handles.push(tokio::spawn(file_sink_task(rx, file)));
                file_sinks.insert(path.clone(), SinkHandle::Unbounded(tx));
            }
        }

        // Build per-service state.
        let mut service_map = HashMap::new();
        for (name, config) in services {
            // Every process feeds the stdout writer, whatever its `log`
            // setting: that is the only path to the merged tap, and the tap is
            // what every client reads. Whether the *terminal* sees a line is
            // decided at the far end, by `StdoutMuteControl`.
            let mut sinks = vec![stdout_sink.clone()];
            if let crate::config::LogConfig::File(path) = config {
                // The file sink gets the raw line, unprefixed and unsanitized.
                // Always present: the file sinks were created from this list.
                if let Some(sink) = file_sinks.get(path).cloned() {
                    sinks.push(sink);
                }
            }

            let color = color_map.get(*name).copied().unwrap_or(Color::White);
            let prefix = format_prefix(name, color, max_name_len);
            let log_keep_filter = CompiledLogKeepFilter::from_config(
                name,
                log_filters.get(*name).filter(|filter| !filter.is_empty()),
            )?;

            service_map.insert(
                name.to_string(),
                actor::spawn(
                    (*name).to_string(),
                    prefix,
                    sinks,
                    stdout_sink.clone(),
                    mute.clone(),
                    log_keep_filter,
                ),
            );
        }

        let don_prefix = format!(
            "{}{:width$}{} │ ",
            SetAttribute(Attribute::Bold),
            "[don]",
            SetAttribute(Attribute::Reset),
            width = max_name_len,
        );

        let (services_watch, _) = watch::channel(Arc::new(service_map.clone()));
        let (attach_sinks, _) = watch::channel(Some(attach::AttachSinks {
            emitter: LifecycleEmitter {
                don_prefix: don_prefix.clone(),
                stdout_sink: stdout_sink.clone(),
                bazel_prefix: None,
            },
        }));
        Ok(Self {
            services: service_map,
            services_watch,
            attach_sinks,
            don_prefix,
            stdout_sink,
            mute,
            writer_handles,
            verbosity,
            emulator,
            log_filter,
            bazel_prefix: None,
            log_tap,
        })
    }

    /// A handle on the merged log stream — subscriptions plus bounded
    /// history — for components that hand out follows (the API server, the
    /// in-process TUI forwarder). Holding the handle rather than a receiver
    /// means idle handles cost nothing and never lag.
    pub fn log_stream_sender(&self) -> MergedLogTap {
        self.log_tap.clone()
    }

    /// Register a synthetic "tool" service (`bazel`) so build
    /// output gets its own color-coded prefix column instead of an inline
    /// `bazel: …` text prefix on a `[don]` lifecycle event.
    ///
    /// Idempotent: if already registered, just returns — the cached prefix
    /// stays intact. Name must be `"bazel"` (panics
    /// otherwise in debug builds; silently drops the prefix cache in
    /// release).
    /// Restrict stdout-bound output to lines whose source name is in
    /// `allowlist`. Top-level `[don]` lifecycle events (whose `name` is
    /// empty) always pass; per-service lifecycle events (`[don] api: ...`)
    /// pass only when `api` is in the allowlist, mirroring the TUI filter.
    ///
    /// File sinks and ring buffers are unaffected — `don logs <name>`
    /// still returns the full output, and `log = { file = ... }` continues
    /// to receive every line.
    ///
    /// Idempotent guard: only the first call takes effect. Call this once
    /// at startup, before the runner spawns any service. Pass an empty set
    /// to silence every per-service line while keeping `[don]` events
    /// visible.
    pub fn set_log_filter(&self, allowlist: HashSet<String>) {
        self.log_filter.set(allowlist);
    }

    pub async fn register_build_tool(&mut self, name: &str) {
        self.register_service(name, &crate::config::LogConfig::Stdout)
            .await;
        let prefix = self
            .services
            .get(name)
            .map(|output| output.prefix().clone());
        match name {
            "bazel" => self.bazel_prefix = prefix,
            _ => debug_assert!(false, "register_build_tool: unknown tool '{name}'"),
        }
    }

    /// Emit a line prefixed as bazel tool output. Falls back to a
    /// `[don]`-prefixed `bazel: {message}` lifecycle event if
    /// [`Self::register_build_tool`] wasn't called.
    pub fn bazel_event(&self, message: &str) {
        match self.bazel_prefix.as_ref() {
            Some(prefix) => {
                let _ = self.stdout_sink.send(SinkLine {
                    prefix: prefix.clone(),
                    line: Bytes::from(format!("{message}\n")),
                    name: "bazel".to_string(),
                    is_lifecycle: true,
                    is_verbose: false,
                });
            }
            None => self.lifecycle_event(&format!("bazel: {message}")),
        }
    }

    /// Get a writer handle for a service. Cloneable, reusable across restarts.
    ///
    /// Returns `None` if the service name is not registered.
    /// Can be called multiple times — each call returns a new handle
    /// pointing to the same underlying ring buffer and sinks.
    pub fn service_writer(&self, name: &str) -> Option<ServiceWriter> {
        self.services
            .get(name)
            .cloned()
            .map(|output| ServiceWriter { output })
    }

    /// Attach a follow sink: a freshly-created mpsc channel preloaded with
    /// the last N buffered lines, then registered as a sink for this service.
    /// New lines are delivered until the receiver is dropped (or the client
    /// is too slow and the sink's buffer fills — it then gets disconnected).
    ///
    /// `live_capacity` is the headroom for live lines *on top of* the preloaded
    /// snapshot, so slow readers don't block immediately after connection.
    ///
    /// Returns `None` if the service name is unknown.
    pub async fn add_follow_sink(
        &self,
        name: &str,
        last_n: usize,
        live_capacity: usize,
    ) -> Option<mpsc::Receiver<SinkLine>> {
        let output = self.services.get(name)?;
        output.follow_sink(last_n, live_capacity).await
    }

    /// A handle to the emulator thread, for the server's resize path.
    pub(crate) fn emulator_handle(&self) -> emulator::EmulatorHandle {
        self.emulator.clone()
    }

    /// Add an OSC response sink to a service. The sink scans each chunk for
    /// terminal queries (OSC 10/11, cursor position) and writes responses
    /// directly to the PTY write handle.
    ///
    /// The sink uses bounded-drop semantics so it never blocks the output
    /// pipeline. Returns a [`OscSinkHandle`] that can be used to reclaim
    /// the PTY write handle (e.g., for attach).
    pub async fn add_osc_sink(
        &self,
        name: &str,
        pty_input: mpsc::Sender<PtyInput>,
    ) -> Option<OscSinkHandle> {
        let output = self.services.get(name)?.clone();
        Some(spawn_osc_sink(output, pty_input))
    }

    /// Read the last N lines from a service's ring buffer, joined by newlines.
    ///
    /// Returns `None` if the service is not registered.
    pub async fn read_logs(&self, name: &str, n: usize) -> Option<Bytes> {
        let output = self.services.get(name)?;
        Some(output.read_logs(n).await)
    }

    /// Register a service that wasn't known at OutputManager construction
    /// (currently used by `register_build_tool` to give `bazel`
    /// their own prefix column). Creates a ring buffer, assigns a color,
    /// and wires up sinks based on the log config. Existing services are
    /// left unchanged.
    pub async fn register_service(&mut self, name: &str, log_config: &crate::config::LogConfig) {
        if self.services.contains_key(name) {
            return;
        }
        // Determine max_name_len from existing prefix width. New services use
        // the wider of the current alignment and their own name length.
        let current_max = self
            .services
            .keys()
            .map(|n| n.len())
            .max()
            .unwrap_or(0)
            .max(5);
        let max_name_len = current_max.max(name.len());
        let mut all_names: Vec<&str> = self.services.keys().map(String::as_str).collect();
        all_names.push(name);
        let color = assign_colors(&all_names)
            .get(name)
            .copied()
            .unwrap_or(Color::White);
        let prefix = format_prefix(name, color, max_name_len);

        // Every process feeds the writer — that is the only path to the merged
        // tap. A file-mode service added at runtime gets no file sink (opening
        // one would mean spawning a task here), but it is recorded like any
        // other; `log = "ignore"` is muted at the terminal, not unwired.
        let sinks = vec![self.stdout_sink.clone()];
        if matches!(log_config, crate::config::LogConfig::Ignore) {
            self.mute.mute_by_config(name);
        }

        self.services.insert(
            name.to_string(),
            actor::spawn(
                name.to_string(),
                prefix,
                sinks,
                self.stdout_sink.clone(),
                self.mute.clone(),
                CompiledLogKeepFilter::default(),
            ),
        );
        let _ = self.services_watch.send(Arc::new(self.services.clone()));
    }

    /// Mint the attach handle for the API server. Call once — each call
    /// spawns its own detach worker.
    pub fn attach_control(&self) -> attach::AttachControl {
        attach::AttachControl::spawn(
            self.services_watch.subscribe(),
            self.attach_sinks.subscribe(),
            self.emulator.clone(),
        )
    }

    /// Get a cloneable read-only handle to every process's buffered output.
    pub fn log_reader(&self) -> LogReader {
        LogReader {
            services: self.services_watch.subscribe(),
        }
    }

    /// Get a shared runtime controller for verbose logging.
    pub fn verbosity_control(&self) -> VerbosityControl {
        self.verbosity.clone()
    }

    /// Get a cloneable output handle scoped to one registered process.
    ///
    /// Returns `None` for an unregistered name — every service and task is
    /// registered at construction, so `None` means a genuine name mismatch,
    /// not a timing window.
    pub fn process_output(&self, name: &str) -> Option<ProcessOutput> {
        Some(ProcessOutput {
            name: name.to_string(),
            output: self.services.get(name)?.clone(),
            events: self.clone_lifecycle_emitter(),
            emulator: self.emulator.clone(),
        })
    }

    /// Get a lightweight, cloneable handle for emitting `[don]` lifecycle
    /// events from spawned tasks (e.g. build output).
    pub fn clone_lifecycle_emitter(&self) -> LifecycleEmitter {
        LifecycleEmitter {
            don_prefix: self.don_prefix.clone(),
            stdout_sink: self.stdout_sink.clone(),
            bazel_prefix: self.bazel_prefix.clone(),
        }
    }

    /// Emit a `[don]` lifecycle event.
    pub fn lifecycle_event(&self, message: &str) {
        let _ = self.stdout_sink.send(SinkLine {
            prefix: Bytes::from(self.don_prefix.clone()),
            line: Bytes::from(format!("{message}\n")),
            name: LIFECYCLE_EVENT_NAME.to_string(),
            is_lifecycle: true,
            is_verbose: false,
        });
    }

    /// Emit a verbose-tagged `[don]` diagnostic event. Always emitted —
    /// consumers filter on the tag (see [`SinkLine::is_verbose`]).
    pub fn debug_event(&self, message: &str) {
        let _ = self.stdout_sink.send(SinkLine {
            prefix: Bytes::from(self.don_prefix.clone()),
            line: Bytes::from(format!("{message}\n")),
            name: LIFECYCLE_EVENT_NAME.to_string(),
            is_lifecycle: true,
            is_verbose: true,
        });
    }

    /// Log (verbose-tagged) the exact executable and arguments about
    /// to be passed to `execve`. See [`LifecycleEmitter::debug_spawn`].
    pub fn debug_spawn<S: AsRef<str>>(&self, label: &str, cmd: &str, args: &[S]) {
        self.service_debug_event(label, &format!("spawn {}", format_cmdline(cmd, args)));
    }

    /// Emit a `[don]` lifecycle event scoped to a service/task. The message
    /// is prefixed with `{service}: ` and the line is tagged with `service`
    /// so it passes the TUI filter when the service is selected — `[don] api:
    /// restarted` shows up under the "api" filter, not under "don".
    pub fn service_event(&self, service: &str, message: &str) {
        let _ = self.stdout_sink.send(SinkLine {
            prefix: Bytes::from(self.don_prefix.clone()),
            line: Bytes::from(format!("{service}: {message}\n")),
            name: service.to_string(),
            is_lifecycle: true,
            is_verbose: false,
        });
    }

    /// Emit a verbose-tagged `[don]` diagnostic event scoped to a
    /// service/task. Always emitted — consumers filter on the tag.
    pub fn service_debug_event(&self, service: &str, message: &str) {
        let _ = self.stdout_sink.send(SinkLine {
            prefix: Bytes::from(self.don_prefix.clone()),
            line: Bytes::from(format!("{service}: {message}\n")),
            name: service.to_string(),
            is_lifecycle: true,
            is_verbose: true,
        });
    }

    /// Emit a `[don]` error event.
    pub fn error_event(&self, message: &str) {
        let _ = self.stdout_sink.send(SinkLine {
            prefix: Bytes::from(self.don_prefix.clone()),
            line: Bytes::from(format!("{message}\n")),
            name: LIFECYCLE_EVENT_NAME.to_string(),
            is_lifecycle: true,
            is_verbose: false,
        });
    }

    /// Emit a `[don]` error event for a specific service. Tagged with the
    /// service name so the filter surfaces it alongside that service's own
    /// output. See [`Self::service_event`] for rationale.
    pub fn service_error_event(&self, service: &str, message: &str) {
        let _ = self.stdout_sink.send(SinkLine {
            prefix: Bytes::from(self.don_prefix.clone()),
            line: Bytes::from(format!("{service}: {message}\n")),
            name: service.to_string(),
            is_lifecycle: true,
            is_verbose: false,
        });
    }

    /// Shut down the output system. Clears all sink lists, drops senders,
    /// and waits for writer tasks to drain remaining messages.
    ///
    /// A writer task only exits once every clone of its sink sender has
    /// dropped. If some detached task in the process is still holding a
    /// [`LifecycleEmitter`] (and thus a sender clone) — the API server,
    /// a slow-to-abort proxy accept loop, a lingering background build —
    /// `handle.await` blocks forever. That would hang the daemon long
    /// past the `shutdown complete` lifecycle event, preventing `don`
    /// from returning to the shell.
    ///
    /// To make that failure mode self-correcting, each writer gets a
    /// bounded drain window: we abort stragglers so shutdown completes
    /// within a predictable time. The downside is occasional truncation
    /// of the very last log line under load, which is preferable to the
    /// daemon silently refusing to exit.
    pub async fn shutdown(self) {
        // Take the attach sinks back first. `AttachControl` lives in the API
        // server's state, which outlives this flush; a sender clone held
        // there would keep the writer channels open and cost the full 2s
        // straggler wait below on every shutdown.
        let _ = self.attach_sinks.send(None);
        for output in self.services.values() {
            output.clear_sinks();
        }
        drop(self.stdout_sink);
        drop(self.services);
        for mut handle in self.writer_handles {
            let wait = std::time::Duration::from_secs(2);
            match tokio::time::timeout(wait, &mut handle).await {
                Ok(_) => {}
                Err(_) => {
                    // A straggler task is still holding a sender clone
                    // (the API server task, a slow-to-abort proxy accept
                    // loop, a lingering build). `abort()` cancels the
                    // writer task so its `log_tx` inside the TUI plumbing
                    // actually drops — without that, the TUI's
                    // `log_rx.recv()` blocks forever and `don` never
                    // returns to the shell after "shutdown complete".
                    handle.abort();
                    let _ = handle.await;
                }
            }
        }
    }
}

/// Everything one process needs to do its own output, in a cloneable handle.
///
/// [`OutputManager`] is deliberately not `Clone` — it owns the writer task
/// handles that `shutdown` must join, so exactly one thing may own it. But a
/// per-process supervisor still has to write its child's output, attach an OSC
/// sink, mute the terminal for a foreground run, and emit its own lifecycle
/// events. This hands out that slice, scoped to a single name.
///
/// "Process" rather than "service" because tasks use the same machinery — and so
/// does the synthetic `bazel` stream. The name is whatever the
/// stream was registered under.
#[derive(Clone)]
pub struct ProcessOutput {
    name: String,
    output: actor::OutputHandle,
    events: LifecycleEmitter,
    emulator: emulator::EmulatorHandle,
}

impl ProcessOutput {
    /// The name this handle is scoped to.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// A writer for piping this process's child process output.
    ///
    /// Several may exist at once (a restart creates a new one before the old
    /// reader has finished draining); they share one ring buffer.
    pub fn writer(&self) -> ServiceWriter {
        ServiceWriter {
            output: self.output.clone(),
        }
    }

    /// Attach an OSC response sink so terminal queries from the child reach
    /// its PTY. See [`OutputManager::add_osc_sink`].
    pub async fn add_osc_sink(&self, pty_input: mpsc::Sender<PtyInput>) -> OscSinkHandle {
        spawn_osc_sink(self.output.clone(), pty_input)
    }

    /// (Re)register this process's server-side screen and route its output
    /// bytes into the emulator. See [`OutputManager::register_emulator`].
    /// Register the live spawn's PTY input-gate sender for attach. Call at
    /// wire time for PTY spawns; cleared by [`Self::clear_attach`] at reap.
    pub async fn set_attach_pty(&self, pty_input: mpsc::Sender<PtyInput>) {
        self.output.set_attach_pty(Some(pty_input));
    }

    /// The spawn is gone: drop the attach registration, reset the client
    /// count and resume prefixed stdout if any client had it paused. The
    /// bridges themselves end on their own when the output sinks close;
    /// their late detach notifications no-op against a zero count.
    pub async fn clear_attach(&self) {
        self.output.clear_attach();
    }

    pub async fn register_emulator(&self, cols: u16, rows: u16) {
        self.emulator.register(&self.name, cols, rows);
        self.output
            .add_sink_once(SinkHandle::Emulator(self.emulator.feed_sender()));
    }

    /// Emit a `[don]` event tagged with this process's name.
    pub fn event(&self, message: &str) {
        self.events.service_event(&self.name, message);
    }

    /// Emit a `[don]` error event tagged with this process's name.
    pub fn error_event(&self, message: &str) {
        self.events.service_error_event(&self.name, message);
    }

    /// Emit a `[don]` event tagged with this process's name, verbose mode only.
    pub fn debug_event(&self, message: &str) {
        self.events.service_debug_event(&self.name, message);
    }

    /// The untagged lifecycle emitter, for handing to spawned helpers.
    pub fn emitter(&self) -> &LifecycleEmitter {
        &self.events
    }
}

/// A lightweight, cloneable handle for emitting `[don]` lifecycle events
/// from spawned tasks. Does not carry the full `OutputManager` state.
#[derive(Clone)]
pub struct LifecycleEmitter {
    don_prefix: String,
    stdout_sink: SinkHandle,
    bazel_prefix: Option<Bytes>,
}

impl LifecycleEmitter {
    /// An emitter whose lines go nowhere, for tests that need one as
    /// plumbing rather than as the thing under test.
    #[cfg(test)]
    pub(crate) fn discarding() -> Self {
        let (tx, _) = mpsc::unbounded_channel();
        Self {
            don_prefix: "[don] │ ".to_string(),
            stdout_sink: SinkHandle::Unbounded(tx),
            bazel_prefix: None,
        }
    }

    /// Emit a `[don]` lifecycle event.
    pub fn lifecycle_event(&self, message: &str) {
        let _ = self.stdout_sink.send(SinkLine {
            prefix: Bytes::from(self.don_prefix.clone()),
            line: Bytes::from(format!("{message}\n")),
            name: LIFECYCLE_EVENT_NAME.to_string(),
            is_lifecycle: true,
            is_verbose: false,
        });
    }

    /// Emit a `[don]` lifecycle event scoped to a service/task. Tagged with
    /// the service name so the TUI filter shows it when the service is
    /// selected. See [`OutputManager::service_event`] for rationale.
    pub fn service_event(&self, service: &str, message: &str) {
        let _ = self.stdout_sink.send(SinkLine {
            prefix: Bytes::from(self.don_prefix.clone()),
            line: Bytes::from(format!("{service}: {message}\n")),
            name: service.to_string(),
            is_lifecycle: true,
            is_verbose: false,
        });
    }

    /// Emit an untagged `[don]` error event — a workspace-level problem with
    /// no single process to blame, such as a build-tool query that failed for
    /// a whole bazel workspace.
    pub fn error_event(&self, message: &str) {
        let _ = self.stdout_sink.send(SinkLine {
            prefix: Bytes::from(self.don_prefix.clone()),
            line: Bytes::from(format!("{message}\n")),
            name: LIFECYCLE_EVENT_NAME.to_string(),
            is_lifecycle: true,
            is_verbose: false,
        });
    }

    /// Emit a `[don]` error event scoped to a service. Tagged with the
    /// service name.
    pub fn service_error_event(&self, service: &str, message: &str) {
        let _ = self.stdout_sink.send(SinkLine {
            prefix: Bytes::from(self.don_prefix.clone()),
            line: Bytes::from(format!("{service}: {message}\n")),
            name: service.to_string(),
            is_lifecycle: true,
            is_verbose: false,
        });
    }

    /// Emit a verbose-tagged `[don]` diagnostic event. Always emitted —
    /// consumers filter on the tag (see [`SinkLine::is_verbose`]).
    pub fn debug_event(&self, message: &str) {
        let _ = self.stdout_sink.send(SinkLine {
            prefix: Bytes::from(self.don_prefix.clone()),
            line: Bytes::from(format!("{message}\n")),
            name: LIFECYCLE_EVENT_NAME.to_string(),
            is_lifecycle: true,
            is_verbose: true,
        });
    }

    /// Emit a verbose-tagged, service-scoped `[don]` diagnostic event.
    pub fn service_debug_event(&self, service: &str, message: &str) {
        let _ = self.stdout_sink.send(SinkLine {
            prefix: Bytes::from(self.don_prefix.clone()),
            line: Bytes::from(format!("{service}: {message}\n")),
            name: service.to_string(),
            is_lifecycle: true,
            is_verbose: true,
        });
    }

    /// Log (verbose-tagged) the exact executable and arguments about
    /// to be passed to `execve`. Use before every `Command::spawn()` so
    /// verbose consumers see what don is actually asking the kernel to run.
    ///
    /// `label` is a short tag (e.g. service name, "bazel") to
    /// help the user identify the source of the spawn.
    pub fn debug_spawn<S: AsRef<str>>(&self, label: &str, cmd: &str, args: &[S]) {
        self.service_debug_event(label, &format!("spawn {}", format_cmdline(cmd, args)));
    }

    /// Emit a line prefixed as bazel tool output. Falls back to a
    /// `[don]`-prefixed `bazel: {message}` event if the synthetic tool
    /// service wasn't registered.
    pub fn bazel_event(&self, message: &str) {
        match self.bazel_prefix.as_ref() {
            Some(prefix) => {
                let _ = self.stdout_sink.send(SinkLine {
                    prefix: prefix.clone(),
                    line: Bytes::from(format!("{message}\n")),
                    name: "bazel".to_string(),
                    is_lifecycle: true,
                    is_verbose: false,
                });
            }
            None => self.lifecycle_event(&format!("bazel: {message}")),
        }
    }
}

/// Private-mode sequences that take the alternate screen. `47` is the
/// original, `1047` and `1049` the xterm refinements; applications in the wild
/// still emit all three, and `1049` is what anything modern uses.
const ALT_SCREEN_ENTER: [&[u8]; 3] = [b"\x1b[?1049h", b"\x1b[?1047h", b"\x1b[?47h"];
/// The same modes, reset — the process handing the screen back.
const ALT_SCREEN_LEAVE: [&[u8]; 3] = [b"\x1b[?1049l", b"\x1b[?1047l", b"\x1b[?47l"];
/// Trailing bytes to keep while suppressing: enough to recognise the longest
/// sequence above, which is what ends the suppression.
const ALT_SCREEN_SCAN: usize = 8;

/// The length of whichever candidate `acc` ends with, if any.
fn ends_with_any(acc: &[u8], candidates: &[&[u8]]) -> Option<usize> {
    candidates
        .iter()
        .find(|candidate| acc.ends_with(candidate))
        .map(|candidate| candidate.len())
}

/// Stdout sink writer task. Receives raw byte chunks and accumulates
/// per-service until `\n` or overflow, then emits the formatted line to
/// the configured target (pipe writer or TUI channel).
///
/// Each service's partial output is buffered independently so that
/// interleaved chunks from different services don't produce garbled output.
/// Runs until all senders are dropped.
#[allow(clippy::too_many_arguments)]
async fn stdout_sink_task<W: tokio::io::AsyncWrite + Unpin + Send>(
    mut rx: mpsc::UnboundedReceiver<SinkLine>,
    mut target: StdoutTarget<W>,
    verbosity: VerbosityControl,
    filter: LogFilterControl,
    tap: MergedLogTap,
    mute: StdoutMuteControl,
    queued: Arc<std::sync::atomic::AtomicUsize>,
    shed: Arc<std::sync::atomic::AtomicU64>,
) {
    use bytes::BytesMut;

    let start = std::time::Instant::now();
    /// Maximum bytes to accumulate per-service before forcing a flush.
    const MAX_LINE: usize = 16 * 1024;

    // Per-service line accumulator, keyed by prefix bytes.
    let mut accumulators: HashMap<Bytes, BytesMut> = HashMap::new();
    // Accumulators holding a \r whose meaning is not settled yet: a repaint if
    // the next byte is anything else, the CR of a CRLF if it is a newline. See
    // the branch that resolves it.
    let mut cr_pending: HashSet<Bytes> = HashSet::new();
    // Processes that have taken the alternate screen and not yet given it
    // back. See the suppression below.
    let mut alt_screen: HashSet<Bytes> = HashSet::new();
    loop {
        let msg = match rx.try_recv() {
            Ok(msg) => msg,
            // Nothing queued: about to park, so push what is buffered first.
            // Buffering exists to amortise syscalls against a backlog — with
            // no backlog it must never hold a line back from someone watching
            // for it. Flushing here rather than at the bottom of the loop also
            // covers the paths that `continue` past it, one of which (the
            // end-of-stream marker) is always the last message a run produces.
            Err(_) => {
                target.flush().await;
                match rx.recv().await {
                    Some(msg) => msg,
                    None => break,
                }
            }
        };
        queued.fetch_sub(msg.line.len(), Ordering::Relaxed);

        // Say what the send side had to drop. Reported from here so it lands
        // in the stream in order, rather than from a sender that has no idea
        // where in the output it is.
        let dropped = shed.swap(0, Ordering::Relaxed);
        if dropped > 0 {
            let notice =
                format!("… {dropped} line(s) dropped — output outran what could be written\n");
            emit_line(
                &mut target,
                &tap,
                &mute,
                &msg.name,
                &msg.prefix,
                notice.as_bytes(),
                true,
                false,
                &verbosity,
                start,
            )
            .await;
        }

        // Drop messages whose source service isn't in the active allowlist.
        // Wipe any partial accumulator for that prefix too — carrying half a
        // line across a filter change would replay it the next time the
        // service became visible. Empty `name` (top-level lifecycle events)
        // always passes; see `LogFilterControl::passes`.
        if !filter.passes(&msg.name) {
            accumulators.remove(&msg.prefix);
            cr_pending.remove(&msg.prefix);
            alt_screen.remove(&msg.prefix);
            continue;
        }

        // The end of a process's stream — see
        // `ServiceOutputState::mark_stream_end`. Flush whatever it left
        // mid-line, then drop every piece of state that run built up so the
        // next one starts clean.
        if msg.line.is_empty() && !msg.is_lifecycle {
            let held = accumulators.remove(&msg.prefix);
            // A \r still held at the end of the stream was a repaint that
            // nothing followed — the last frame a progress bar drew before the
            // process exited. It lands as its own line like every other frame;
            // the carriage return itself is not part of what it painted.
            let ended_on_repaint = cr_pending.remove(&msg.prefix);
            let held = held.map(|mut acc| {
                if ended_on_repaint && acc.last() == Some(&b'\r') {
                    acc.truncate(acc.len() - 1);
                }
                acc
            });
            let owned_screen = alt_screen.remove(&msg.prefix);
            // A process that died holding the screen leaves escape fragments,
            // not a line anyone wants to read.
            if let Some(acc) = held
                && !acc.is_empty()
                && !owned_screen
            {
                let sanitized = if msg.prefix.is_empty() {
                    acc.to_vec()
                } else {
                    sanitize::sanitize_terminal_output(&acc)
                };
                emit_line(
                    &mut target,
                    &tap,
                    &mute,
                    &msg.name,
                    &msg.prefix,
                    &sanitized,
                    msg.is_lifecycle,
                    msg.is_verbose,
                    &verbosity,
                    start,
                )
                .await;
            }
            continue;
        }

        let acc = accumulators.entry(msg.prefix.clone()).or_default();

        for &byte in msg.line.iter() {
            // While a process owns the alternate screen its output is frames,
            // not lines: cursor moves and clears, with no `\n` or `\r` between
            // them. Sanitizing strips the positioning and the line splitter
            // never sees a boundary, so the multiplexed view would show one
            // endless line of concatenated frames — and show nothing at all
            // until the process exits. Say where to watch it properly instead.
            // The ring buffer, file sinks and the emulator behind `don attach`
            // are fed upstream of here and lose nothing.
            if alt_screen.contains(&msg.prefix) {
                acc.extend_from_slice(&[byte]);
                if byte == b'l' && ends_with_any(acc, &ALT_SCREEN_LEAVE).is_some() {
                    alt_screen.remove(&msg.prefix);
                    acc.clear();
                } else if acc.len() > ALT_SCREEN_SCAN {
                    // Keep only enough trailing bytes to recognise the
                    // sequence that ends this; the rest is a frame nobody
                    // here can render.
                    let stale = acc.len() - ALT_SCREEN_SCAN;
                    bytes::Buf::advance(acc, stale);
                }
                continue;
            }

            // A held \r that no newline followed was a repaint after all, so
            // flush what it painted over before this byte starts the next one.
            if byte != b'\n' && cr_pending.remove(&msg.prefix) {
                if acc.last() == Some(&b'\r') {
                    acc.truncate(acc.len() - 1);
                }
                if !acc.is_empty() {
                    let sanitized = if msg.prefix.is_empty() {
                        acc.to_vec()
                    } else {
                        sanitize::sanitize_terminal_output(acc)
                    };
                    emit_line(
                        &mut target,
                        &tap,
                        &mute,
                        &msg.name,
                        &msg.prefix,
                        &sanitized,
                        msg.is_lifecycle,
                        msg.is_verbose,
                        &verbosity,
                        start,
                    )
                    .await;
                }
                acc.clear();
            }

            acc.extend_from_slice(&[byte]);

            if byte == b'h'
                && let Some(len) = ends_with_any(acc, &ALT_SCREEN_ENTER)
            {
                acc.truncate(acc.len() - len);
                // Whatever preceded the switch is a real partial line.
                if !acc.is_empty() {
                    let sanitized = sanitize::sanitize_terminal_output(acc);
                    emit_line(
                        &mut target,
                        &tap,
                        &mute,
                        &msg.name,
                        &msg.prefix,
                        &sanitized,
                        msg.is_lifecycle,
                        msg.is_verbose,
                        &verbosity,
                        start,
                    )
                    .await;
                }
                acc.clear();
                let notice = format!(
                    "entered full-screen mode — run 'don attach {}' to see it",
                    msg.name
                );
                emit_line(
                    &mut target,
                    &tap,
                    &mute,
                    &msg.name,
                    &msg.prefix,
                    notice.as_bytes(),
                    true,
                    false,
                    &verbosity,
                    start,
                )
                .await;
                alt_screen.insert(msg.prefix.clone());
                cr_pending.remove(&msg.prefix);
                continue;
            }

            if byte == b'\n' {
                // Complete line — strip \r\n, sanitize, emit prefixed output.
                acc.truncate(acc.len() - 1); // remove \n
                if acc.last() == Some(&b'\r') {
                    acc.truncate(acc.len() - 1); // remove \r
                }
                // The \r of a \r\n was a line ending, not a repaint.
                cr_pending.remove(&msg.prefix);
                {
                    let sanitized = if msg.prefix.is_empty() {
                        acc.to_vec()
                    } else {
                        sanitize::sanitize_terminal_output(acc)
                    };
                    emit_line(
                        &mut target,
                        &tap,
                        &mute,
                        &msg.name,
                        &msg.prefix,
                        &sanitized,
                        msg.is_lifecycle,
                        msg.is_verbose,
                        &verbosity,
                        start,
                    )
                    .await;
                }
                acc.clear();
            } else if byte == b'\r' {
                // Might be a repaint, might be the CR of a CRLF. A process on a
                // PTY has every newline it writes translated to \r\n by the
                // terminal discipline, so calling it here would make every line
                // of ordinary output a progress frame — and, now that frames
                // supersede one another, collapse a service's entire output
                // onto a single line. Hold it; the next byte says which it was.
                cr_pending.insert(msg.prefix.clone());
            } else {
                // Non-control byte — any pending \r suppression is stale.
                cr_pending.remove(&msg.prefix);
                if acc.len() >= MAX_LINE {
                    // Overflow — flush without stripping.
                    let sanitized = if msg.prefix.is_empty() {
                        acc.to_vec()
                    } else {
                        sanitize::sanitize_terminal_output(acc)
                    };
                    emit_line(
                        &mut target,
                        &tap,
                        &mute,
                        &msg.name,
                        &msg.prefix,
                        &sanitized,
                        msg.is_lifecycle,
                        msg.is_verbose,
                        &verbosity,
                        start,
                    )
                    .await;
                    acc.clear();
                }
            }
        }
    }

    // Flush remaining accumulators on shutdown. The name is lost here — the
    // accumulator map is keyed on prefix bytes — so end-of-stream partial
    // lines arrive at the TUI with an empty name (unfilterable). Partial
    // lines at shutdown are rare and the user probably wants to see them.
    for (prefix, acc) in &accumulators {
        // A process still holding the alternate screen has escape fragments
        // in hand, not a line.
        if !acc.is_empty() && !alt_screen.contains(prefix) {
            let sanitized = if prefix.is_empty() {
                acc.to_vec()
            } else {
                sanitize::sanitize_terminal_output(acc)
            };
            // End-of-stream partial line: lifecycle vs not is unknowable at
            // this point (the accumulator key is the prefix, not the source
            // flag). Defaulting to false is correct — these are usually
            // service-stdout fragments that didn't end in `\n` before the
            // child closed its pipe.
            emit_line(
                &mut target,
                &tap,
                &mute,
                "",
                prefix,
                &sanitized,
                false,
                false,
                &verbosity,
                start,
            )
            .await;
        }
    }
    target.flush().await;
}

/// Build the formatted line bytes (optional verbose timestamp + prefix + content).
/// No trailing newline — the consumer decides how to terminate the line.
fn build_formatted_bytes(
    prefix: &[u8],
    line: &[u8],
    verbosity: &VerbosityControl,
    start: std::time::Instant,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(prefix.len() + line.len() + 16);
    if verbosity.is_enabled() {
        let elapsed = start.elapsed();
        let ts = format!("{:.3}s ", elapsed.as_secs_f64());
        out.extend_from_slice(ts.as_bytes());
    }
    out.extend_from_slice(prefix);
    out.extend_from_slice(line);
    out
}

/// Emit a complete formatted line to the target — either write to the pipe
/// writer with a trailing `\n`, or ship it to the TUI as a [`FormattedLogLine`].
///
/// Verbose lines always reach the tap (history and followers filter for
/// themselves); the pipe writer is its own consumer and skips them unless
/// `-v` display mode is on.
#[allow(clippy::too_many_arguments)]
async fn emit_line<W: tokio::io::AsyncWrite + Unpin + Send>(
    target: &mut StdoutTarget<W>,
    tap: &MergedLogTap,
    mute: &StdoutMuteControl,
    name: &str,
    prefix: &[u8],
    line: &[u8],
    is_lifecycle: bool,
    is_verbose: bool,
    verbosity: &VerbosityControl,
    start: std::time::Instant,
) {
    let bytes = build_formatted_bytes(prefix, line, verbosity, start);
    let formatted = Arc::new(FormattedLogLine {
        name: name.to_string(),
        is_lifecycle,
        is_verbose,
        // Everything `bytes` has that the message does not: the timestamp when
        // verbose, then the prefix. Split rather than re-derived, so no
        // consumer has to work out where one ends and the other begins.
        prefix: bytes
            .get(..bytes.len() - line.len())
            .unwrap_or_default()
            .to_vec(),
        bytes: line.to_vec(),
    });
    // Feed the tap first, so history and followers see the line even if
    // the writer below blocks. Unless a client is attached: what this process
    // is writing then is a screen, not a record — see `is_attached`. don's own
    // narration about it still goes through, which is what keeps the log
    // saying "migrate: complete (2.4s)" while its window is open.
    if is_lifecycle || !mute.is_attached(name) {
        tap.publish(formatted).await;
    }
    if is_verbose && !verbosity.is_enabled() {
        return;
    }
    // Everything below is about the *terminal*, and only the terminal. The
    // record above is already complete — see [`StdoutMuteControl`].
    //
    // Lifecycle lines are exempt. `log = "ignore"` and an attach both silence a
    // process's *output*; don's narration about it — "api: starting…", "send
    // SIGTERM" — is don speaking, not the process, and the user still needs it.
    // The old wiring got this for free: muting meant unwiring the service's own
    // sinks, and lifecycle lines never travelled through those.
    if !is_lifecycle && mute.is_muted(name) {
        return;
    }
    match target {
        StdoutTarget::Writer(writer) => {
            use tokio::io::AsyncWriteExt;
            let _ = writer.write_all(&bytes).await;
            let _ = writer.write_all(b"\n").await;
        }
    }
}

/// File sink writer task. Receives raw byte chunks and writes them directly.
/// Runs until all senders are dropped.
async fn file_sink_task(rx: mpsc::UnboundedReceiver<SinkLine>, file: tokio::fs::File) {
    use tokio::io::AsyncWriteExt;
    let mut rx = rx;
    // Buffered for the same reason as the stdout writer: a write per line is a
    // syscall per line, and a process that outruns them leaves its output
    // piling up in a queue with no bound. Flushed whenever the queue drains,
    // so a quiet service's line still reaches the file immediately.
    let mut file = tokio::io::BufWriter::with_capacity(WRITE_BUFFER, file);
    while let Some(msg) = rx.recv().await {
        let _ = file.write_all(&msg.line).await;
        if rx.is_empty() {
            let _ = file.flush().await;
        }
    }
    let _ = file.flush().await;
}

/// OSC response sink task. Scans each chunk for terminal queries and
/// writes responses directly to the PTY write handle. Returns the PTY
/// write handle when the channel closes (process exit or sink removal)
/// so it can be reclaimed by the caller.
async fn osc_sink_task(mut rx: mpsc::Receiver<SinkLine>, pty_input: mpsc::Sender<PtyInput>) {
    while let Some(msg) = rx.recv().await {
        for response in osc::find_responses(&msg.line) {
            if pty_input
                .send(PtyInput::Frame(response.to_vec()))
                .await
                .is_err()
            {
                return;
            }
        }
    }
}

/// Open a log file for appending, creating parent directories as needed.
async fn open_log_file(path: &std::path::Path) -> Result<tokio::fs::File, OutputError> {
    if let Some(parent) = path.parent() {
        let os_str = parent.as_os_str();
        if !os_str.is_empty() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|source| OutputError::FileOpen {
                    path: path.to_path_buf(),
                    source,
                })?;
        }
    }
    tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .map_err(|source| OutputError::FileOpen {
            path: path.to_path_buf(),
            source,
        })
}

/// Read a chunk of bytes from the reader.
///
/// Returns the number of bytes read (0 = EOF).
/// For PTY reads, an `EIO` error signals the child exited — treated as EOF.
async fn read_chunk<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut [u8],
) -> Result<usize, OutputError> {
    match reader.read(buf).await {
        Ok(n) => Ok(n),
        Err(e) if e.raw_os_error() == Some(libc::EIO) => Ok(0),
        Err(e) => Err(OutputError::Read(e)),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// A test buffer that implements AsyncWrite and allows reading back contents.
    #[derive(Clone)]
    struct TestBuffer(Arc<Mutex<Vec<u8>>>);

    impl TestBuffer {
        fn new() -> (Self, Arc<Mutex<Vec<u8>>>) {
            let buf = Arc::new(Mutex::new(Vec::new()));
            (TestBuffer(buf.clone()), buf)
        }
    }

    impl tokio::io::AsyncWrite for TestBuffer {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            data: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            self.0.lock().unwrap().extend_from_slice(data);
            std::task::Poll::Ready(Ok(data.len()))
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    /// Read the test buffer as a string.
    fn read_buf(buf: &Arc<Mutex<Vec<u8>>>) -> String {
        String::from_utf8_lossy(&buf.lock().unwrap()).into_owned()
    }

    /// Strip ANSI escape sequences from bytes.
    fn strip_ansi(s: &[u8]) -> String {
        let mut result = Vec::with_capacity(s.len());
        let mut i = 0;
        while i < s.len() {
            if s[i] == b'\x1b' {
                i += 1;
                while i < s.len() && s[i] != b'm' {
                    i += 1;
                }
                i += 1;
            } else {
                result.push(s[i]);
                i += 1;
            }
        }
        String::from_utf8_lossy(&result).into_owned()
    }

    #[test]
    fn test_color_assignment_deterministic() {
        struct Case {
            name: &'static str,
            names: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                name: "same names, same order",
                names: vec!["api", "worker", "postgres"],
            },
            Case {
                name: "same names, different order",
                names: vec!["worker", "postgres", "api"],
            },
        ];

        let mut prev_result: Option<HashMap<String, Color>> = None;
        for case in &cases {
            let result = assign_colors(&case.names);
            if let Some(ref prev) = prev_result {
                assert_eq!(
                    &result, prev,
                    "case: {} — color assignment should be deterministic",
                    case.name
                );
            }
            prev_result = Some(result);
        }
    }

    #[test]
    fn test_color_assignment_distinct() {
        let names = vec!["a", "b", "c", "d"];
        let result = assign_colors(&names);
        let colors: Vec<Color> = {
            let mut sorted_names: Vec<&str> = names.clone();
            sorted_names.sort_unstable();
            sorted_names.iter().map(|n| result[*n]).collect()
        };
        let unique: std::collections::HashSet<Color> = colors.iter().copied().collect();
        assert_eq!(unique.len(), colors.len());
    }

    #[test]
    fn test_color_assignment_wraps() {
        let names: Vec<String> = (0..SERVICE_COLORS.len() + 3)
            .map(|i| format!("svc{i}"))
            .collect();
        let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let result = assign_colors(&name_refs);
        assert_eq!(result.len(), names.len());
    }

    #[test]
    fn test_build_tool_colors_are_reserved() {
        let result = assign_colors(&["api", "bazel", "worker"]);
        assert_eq!(result["bazel"], BAZEL_COLOR);
        assert_eq!(result["bazel"], Color::Grey);
    }

    #[test]
    fn test_prefix_alignment() {
        struct Case {
            name: &'static str,
            service_name: &'static str,
            max_len: usize,
        }

        let cases = vec![
            Case {
                name: "short padded",
                service_name: "api",
                max_len: 8,
            },
            Case {
                name: "exact",
                service_name: "postgres",
                max_len: 8,
            },
            Case {
                name: "single char",
                service_name: "a",
                max_len: 8,
            },
        ];

        for case in cases {
            let prefix = format_prefix(case.service_name, Color::Cyan, case.max_len);
            let stripped = strip_ansi(&prefix);
            let expected = format!("{:width$} │ ", case.service_name, width = case.max_len);
            assert_eq!(stripped, expected, "case: {}", case.name);
        }
    }

    /// A process that takes the alternate screen writes frames, not lines —
    /// cursor moves and clears with no `\n` between them. Sanitizing strips the
    /// positioning and the splitter never sees a boundary, so without this the
    /// multiplexed view shows one endless line of concatenated frames, and
    /// shows it only once the process exits.
    #[tokio::test]
    async fn alternate_screen_output_is_replaced_by_a_pointer_at_attach() {
        struct Case {
            name: &'static str,
            stream: &'static [u8],
            want: Vec<&'static str>,
            unwanted: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                name: "frames are suppressed, output after the handback is not",
                stream: b"before\n\x1b[?1049h\x1b[2J\x1b[HFRAME 1\x1b[HFRAME 2\x1b[?1049lafter\n",
                want: vec!["before", "entered full-screen mode", "after"],
                unwanted: vec!["FRAME 1", "FRAME 2"],
            },
            Case {
                name: "a partial line before the switch is still flushed",
                stream: b"mid-line\x1b[?1049hFRAME\x1b[?1049l\n",
                want: vec!["mid-line", "entered full-screen mode"],
                unwanted: vec!["FRAME"],
            },
            Case {
                name: "the older 47 and 1047 spellings count too",
                stream: b"\x1b[?47hFRAME\x1b[?47l\x1b[?1047hOTHER\x1b[?1047ldone\n",
                want: vec!["entered full-screen mode", "done"],
                unwanted: vec!["FRAME", "OTHER"],
            },
            Case {
                name: "a process that never switches is untouched",
                stream: b"plain one\nplain two\n",
                want: vec!["plain one", "plain two"],
                unwanted: vec!["full-screen"],
            },
        ];

        for case in cases {
            let (writer, buf) = TestBuffer::new();
            let config = crate::config::LogConfig::Stdout;
            let mgr = OutputManager::new(&[("api", &config)], writer)
                .await
                .unwrap();
            let svc = mgr.service_writer("api").unwrap();
            svc.process_stream(std::io::Cursor::new(case.stream.to_vec()))
                .await
                .unwrap();
            mgr.shutdown().await;

            let output = read_buf(&buf);
            for fragment in case.want {
                assert!(
                    output.contains(fragment),
                    "{}: expected {fragment:?} in {output:?}",
                    case.name
                );
            }
            for fragment in case.unwanted {
                assert!(
                    !output.contains(fragment),
                    "{}: did not expect {fragment:?} in {output:?}",
                    case.name
                );
            }
        }
    }

    /// The alternate screen is given back by a sequence a killed process never
    /// sends. The writer keeps its state per process across runs, so without an
    /// end-of-stream reset the *next* run's output would be suppressed too —
    /// `don restart` on an interactive task would silence it for good.
    #[tokio::test]
    async fn a_run_that_dies_holding_the_screen_does_not_silence_the_next_one() {
        let (writer, buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("api", &config)], writer)
            .await
            .unwrap();
        let svc = mgr.service_writer("api").unwrap();

        // A run killed mid-frame: it takes the screen and never gives it back.
        svc.process_stream(std::io::Cursor::new(
            b"\x1b[?1049h\x1b[2J\x1b[HHOLDING".to_vec(),
        ))
        .await
        .unwrap();
        // The next run, on the same process's output.
        svc.process_stream(std::io::Cursor::new(b"second run speaks\n".to_vec()))
            .await
            .unwrap();
        mgr.shutdown().await;

        let output = read_buf(&buf);
        assert!(
            output.contains("second run speaks"),
            "the next run must not inherit the dead one's screen: {output:?}"
        );
        assert!(
            !output.contains("HOLDING"),
            "the dead run's frame is not a line: {output:?}"
        );
    }

    #[tokio::test]
    async fn test_line_buffering_complete_lines() {
        let (writer, buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("api", &config)], writer)
            .await
            .unwrap();
        let svc = mgr.service_writer("api").unwrap();

        let data = b"hello world\nsecond line\n";
        let cursor = std::io::Cursor::new(data.to_vec());
        svc.process_stream(cursor).await.unwrap();

        let logs = mgr.read_logs("api", 10).await.unwrap();
        assert_eq!(logs.as_ref(), b"hello world\nsecond line");

        mgr.shutdown().await;
        let output = read_buf(&buf);
        assert!(output.contains("hello world"), "should contain first line");
        assert!(output.contains("second line"), "should contain second line");
    }

    #[tokio::test]
    async fn test_line_buffering_no_trailing_newline() {
        let (writer, _buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("svc", &config)], writer)
            .await
            .unwrap();
        let svc = mgr.service_writer("svc").unwrap();

        let data = b"line one\npartial";
        let cursor = std::io::Cursor::new(data.to_vec());
        svc.process_stream(cursor).await.unwrap();

        let logs = mgr.read_logs("svc", 10).await.unwrap();
        assert_eq!(logs.as_ref(), b"line one\npartial");

        mgr.shutdown().await;
    }

    #[tokio::test]
    async fn follow_preload_lines_have_no_trailing_newline() {
        // Regression: `don logs -f` replays the last N ring-buffer lines as the
        // initial snapshot. Ring-buffer entries carry a trailing `\n`, but
        // `SinkLine.line` must be newline-free — the follow route embeds it in a
        // JSON "line" value and the client prints it with its own newline. If
        // the `\n` leaks through, every replayed line renders a blank line after
        // it.
        let (writer, _buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("api", &config)], writer)
            .await
            .unwrap();
        let svc = mgr.service_writer("api").unwrap();

        svc.process_stream(std::io::Cursor::new(b"alpha\nbeta\ngamma\n".to_vec()))
            .await
            .unwrap();

        let mut rx = mgr.add_follow_sink("api", 10, 8).await.unwrap();
        let mut lines: Vec<String> = Vec::new();
        while let Ok(sink_line) = rx.try_recv() {
            let text = String::from_utf8_lossy(&sink_line.line).into_owned();
            assert!(
                !text.ends_with('\n'),
                "preloaded follow line must not carry a trailing newline: {text:?}",
            );
            lines.push(text);
        }
        assert_eq!(lines, vec!["alpha", "beta", "gamma"]);

        mgr.shutdown().await;
    }

    #[tokio::test]
    async fn test_non_utf8_output() {
        let (writer, _buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("bin", &config)], writer)
            .await
            .unwrap();
        let svc = mgr.service_writer("bin").unwrap();

        let data: Vec<u8> = vec![0xff, 0xfe, b'h', b'i', b'\n', 0x80, 0x81, b'\n'];
        let cursor = std::io::Cursor::new(data);
        svc.process_stream(cursor).await.unwrap();

        let logs = mgr.read_logs("bin", 10).await.unwrap();
        let expected: Vec<u8> = vec![0xff, 0xfe, b'h', b'i', b'\n', 0x80, 0x81];
        assert_eq!(logs.as_ref(), expected.as_slice());

        mgr.shutdown().await;
    }

    #[tokio::test]
    async fn test_service_writer_reusable() {
        let (writer, _buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("api", &config)], writer)
            .await
            .unwrap();

        // Can get multiple writers for the same service.
        let w1 = mgr.service_writer("api");
        let w2 = mgr.service_writer("api");
        assert!(w1.is_some());
        assert!(w2.is_some());

        // Both share the same ring buffer.
        let w1 = w1.unwrap();
        let data = std::io::Cursor::new(b"from w1\n".to_vec());
        w1.process_stream(data).await.unwrap();

        let logs = mgr.read_logs("api", 10).await.unwrap();
        assert_eq!(logs.as_ref(), b"from w1");

        mgr.shutdown().await;
    }

    #[tokio::test]
    async fn test_unknown_service_returns_none() {
        let (writer, _buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("api", &config)], writer)
            .await
            .unwrap();
        assert!(mgr.service_writer("nonexistent").is_none());
        mgr.shutdown().await;
    }

    #[tokio::test]
    async fn test_ignore_mode_no_stdout() {
        let (writer, buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Ignore;
        let mgr = OutputManager::new(&[("quiet", &config)], writer)
            .await
            .unwrap();
        let svc = mgr.service_writer("quiet").unwrap();

        let data = b"secret\n";
        let cursor = std::io::Cursor::new(data.to_vec());
        svc.process_stream(cursor).await.unwrap();

        // Ring buffer should have the line.
        let logs = mgr.read_logs("quiet", 10).await.unwrap();
        assert_eq!(logs.as_ref(), b"secret");

        mgr.shutdown().await;

        // Stdout should be empty.
        let output = read_buf(&buf);
        assert!(output.is_empty(), "ignore mode should not write to stdout");
    }

    /// What don records for its clients must not depend on what it shows the
    /// terminal. A service logging to a file and one told to be quiet are both
    /// silent on stdout — and both used to be silent in the merged stream too,
    /// because the merged stream is published from inside the stdout writer and
    /// neither was wired to it. The TUI reads that stream, so the TUI was the
    /// one client with an incomplete record.
    #[tokio::test]
    async fn the_merged_record_is_complete_whatever_the_terminal_shows() {
        struct Case {
            label: &'static str,
            config: crate::config::LogConfig,
            want_on_stdout: bool,
        }

        let dir = std::env::temp_dir().join(format!("don-mute-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        let cases = vec![
            Case {
                label: "log = stdout",
                config: crate::config::LogConfig::Stdout,
                want_on_stdout: true,
            },
            Case {
                label: "log = ignore",
                config: crate::config::LogConfig::Ignore,
                want_on_stdout: false,
            },
            Case {
                label: "log = file",
                config: crate::config::LogConfig::File(dir.join("svc.log")),
                want_on_stdout: false,
            },
        ];

        for case in cases {
            let (writer, buf) = TestBuffer::new();
            let mgr = OutputManager::new(&[("svc", &case.config)], writer)
                .await
                .unwrap();
            let mut tap = mgr.log_stream_sender().subscribe();
            let svc = mgr.service_writer("svc").unwrap();
            svc.process_stream(std::io::Cursor::new(b"recorded\n".to_vec()))
                .await
                .unwrap();

            // Read the tap before shutdown drops the sender.
            let line = tokio::time::timeout(std::time::Duration::from_secs(5), tap.recv())
                .await
                .unwrap_or_else(|_| panic!("{}: nothing reached the merged tap", case.label))
                .unwrap();
            assert!(
                String::from_utf8_lossy(&line.line.bytes).contains("recorded"),
                "{}: the merged record must carry the line",
                case.label
            );

            // Muting a process must not mute don talking *about* it.
            mgr.clone_lifecycle_emitter()
                .service_event("svc", "starting...");
            mgr.shutdown().await;

            let output = read_buf(&buf);
            assert_eq!(
                output.contains("recorded"),
                case.want_on_stdout,
                "{}: terminal output",
                case.label
            );
            assert!(
                output.contains("svc: starting..."),
                "{}: lifecycle narration is don speaking, not the process",
                case.label
            );
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A process that outruns the writer must not be able to consume the
    /// machine. This is the bound, tested at the send side because that is the
    /// only place that still works when the writer is parked inside a write to
    /// a destination that has stopped draining — which is exactly the state
    /// that took a 91 GB machine down before the bound existed.
    #[test]
    fn output_past_the_watermark_is_shed_rather_than_queued() {
        let (tx, rx) = mpsc::unbounded_channel();
        let queued = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let shed = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let sink = SinkHandle::Metered {
            tx,
            queued: Arc::clone(&queued),
            shed: Arc::clone(&shed),
        };

        let line = |lifecycle: bool| SinkLine {
            prefix: Bytes::from_static(b"svc | "),
            line: Bytes::from(vec![b'x'; 1024]),
            name: "svc".to_string(),
            is_lifecycle: lifecycle,
            is_verbose: false,
        };

        // Below the watermark everything is queued.
        assert!(sink.send(line(false)).is_ok());
        assert_eq!(queued.load(Ordering::Relaxed), 1024);
        assert_eq!(shed.load(Ordering::Relaxed), 0);

        // Simulate a writer that has stopped consuming: the backlog is past
        // the watermark and nothing is draining it.
        queued.store(SHED_HIGH_WATER + 1, Ordering::Relaxed);

        for _ in 0..10 {
            assert!(sink.send(line(false)).is_ok(), "shedding is not an error");
        }
        assert_eq!(shed.load(Ordering::Relaxed), 10, "every one was shed");
        assert_eq!(
            queued.load(Ordering::Relaxed),
            SHED_HIGH_WATER + 1,
            "a shed line must not add to the backlog it was shed for"
        );

        // don's own narration is never the flood, and losing it loses the
        // explanation for what is happening.
        assert!(sink.send(line(true)).is_ok());
        assert_eq!(shed.load(Ordering::Relaxed), 10, "lifecycle lines are kept");
        assert!(queued.load(Ordering::Relaxed) > SHED_HIGH_WATER + 1);

        drop(sink);
        // One process line from before the watermark, plus the lifecycle line.
        let received: Vec<SinkLine> = std::iter::from_fn({
            let mut rx = rx;
            move || rx.try_recv().ok()
        })
        .collect();
        assert_eq!(received.len(), 2, "ten shed lines never reached the queue");
        assert!(received[1].is_lifecycle);
    }

    /// A config mute is permanent and an attach mute lasts as long as the
    /// attach. Keeping them in one set would let a detach hand a `log =
    /// "ignore"` service a terminal it was never meant to have.
    #[test]
    fn mute_sources_do_not_leak_into_each_other() {
        let mute = StdoutMuteControl::new(HashSet::from(["quiet".to_string()]));

        assert!(mute.is_muted("quiet"), "config mute applies immediately");
        assert!(!mute.is_muted("noisy"), "nothing else is muted");

        mute.attach("noisy");
        mute.attach("quiet");
        assert!(
            mute.is_muted("noisy"),
            "an attached process leaves the terminal"
        );
        assert!(mute.is_muted("quiet"), "and stays muted if it already was");

        mute.release("noisy");
        mute.release("quiet");
        assert!(!mute.is_muted("noisy"), "detaching gives the terminal back");
        assert!(
            mute.is_muted("quiet"),
            "but a detach must not undo what the config asked for"
        );

        mute.mute_by_config("added-later");
        assert!(
            mute.is_muted("added-later"),
            "services can join after construction"
        );

        // The narrower question, which decides whether the *record* gets the
        // output too. A config mute must never answer it: `log = "ignore"`
        // keeps a service off the terminal and still in the log.
        assert!(
            !mute.is_attached("added-later"),
            "a config mute is not an attach"
        );
        mute.attach("added-later");
        assert!(mute.is_attached("added-later"));
        mute.release("added-later");
        assert!(!mute.is_attached("added-later"));
    }

    /// While a client is attached, the process is drawing a screen — key echo,
    /// prompt repaints, cursor moves — and each fragment of it used to become
    /// a log entry, shredded between the lines other processes were writing.
    /// don's own narration about the process still goes through, which is what
    /// keeps "complete (2.4s)" arriving while the window is open.
    #[tokio::test]
    async fn an_attached_process_writes_to_its_window_not_to_the_log() {
        struct Case {
            name: &'static str,
            attached: bool,
            is_lifecycle: bool,
            want_in_log: bool,
        }

        let cases = [
            Case {
                name: "ordinary output, nobody attached",
                attached: false,
                is_lifecycle: false,
                want_in_log: true,
            },
            Case {
                name: "ordinary output while attached",
                attached: true,
                is_lifecycle: false,
                want_in_log: false,
            },
            Case {
                name: "don's own narration while attached",
                attached: true,
                is_lifecycle: true,
                want_in_log: true,
            },
        ];

        for case in cases {
            let tap = MergedLogTap::with_capacity(16);
            let mute = StdoutMuteControl::new(HashSet::new());
            if case.attached {
                mute.attach("shell");
            }
            let (writer, _buf) = TestBuffer::new();
            let mut target = StdoutTarget::new(writer);

            emit_line(
                &mut target,
                &tap,
                &mute,
                "shell",
                b"shell | ",
                b"some output",
                case.is_lifecycle,
                false,
                &VerbosityControl::new(false),
                std::time::Instant::now(),
            )
            .await;

            let lines = tap.tail(16).await.lines;
            assert_eq!(
                !lines.is_empty(),
                case.want_in_log,
                "{}: log holds {:?}",
                case.name,
                lines
                    .iter()
                    .map(|l| String::from_utf8_lossy(&l.line.bytes).to_string())
                    .collect::<Vec<_>>()
            );
        }
    }

    #[tokio::test]
    async fn test_lifecycle_event_format() {
        let (writer, buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("postgres", &config)], writer)
            .await
            .unwrap();

        mgr.lifecycle_event("loading don.toml");
        mgr.shutdown().await;

        let output = read_buf(&buf);
        let stripped = strip_ansi(output.as_bytes());
        assert!(stripped.contains("[don]") && stripped.contains("loading don.toml"));
    }

    /// Every client numbers the same line the same way, and a client that
    /// falls behind is caught up from the tap's own history instead of being
    /// handed a hole. Only when history has itself moved past the gap does a
    /// client hear about it — and then it hears exactly how much is missing
    /// and where the stream resumes.
    #[tokio::test]
    async fn a_cursor_that_falls_behind_is_caught_up_not_cut_short() {
        struct Case {
            label: &'static str,
            capacity: usize,
            published: u64,
            /// How many lines the cursor should still receive.
            want_lines: usize,
            want_dropped: Option<u64>,
        }

        // The broadcast holds 4096, so publishing past that with nobody
        // reading is what forces a lag.
        let cases = vec![
            Case {
                label: "history covers the gap, so the client never learns of it",
                capacity: 50_000,
                published: 6_000,
                want_lines: 6_000,
                want_dropped: None,
            },
            Case {
                label: "history moved on: the client is told exactly what it lost",
                capacity: 1_000,
                published: 6_000,
                want_lines: 1_000,
                want_dropped: Some(5_000),
            },
        ];

        for case in cases {
            let tap = MergedLogTap::with_capacity(case.capacity);
            let mut cursor = tap.cursor(None, 0).await;

            for n in 0..case.published {
                tap.publish(Arc::new(FormattedLogLine {
                    name: "api".to_string(),
                    is_lifecycle: false,
                    is_verbose: false,
                    prefix: Vec::new(),
                    bytes: format!("line {n}").into_bytes(),
                }))
                .await;
            }
            drop(tap); // so the cursor ends rather than parking

            let mut lines = Vec::new();
            let mut dropped = None;
            while let Some(event) = cursor.recv().await {
                match event {
                    MergedEvent::Line(entry) => lines.push(entry.id),
                    MergedEvent::Dropped { count, resumed_at } => {
                        dropped = Some((count, resumed_at));
                    }
                }
            }

            assert_eq!(lines.len(), case.want_lines, "{}: lines", case.label);
            assert_eq!(
                dropped.map(|(count, _)| count),
                case.want_dropped,
                "{}: dropped",
                case.label
            );
            // Whatever survived is contiguous and ends at the newest line:
            // healing must not reorder or duplicate.
            assert!(
                lines.windows(2).all(|pair| pair[1].0 == pair[0].0 + 1),
                "{}: ids must stay contiguous",
                case.label
            );
            assert_eq!(
                lines.last().copied(),
                Some(LogId(case.published - 1)),
                "{}: the newest line must arrive",
                case.label
            );
            if let Some((_, resumed_at)) = dropped {
                assert_eq!(
                    Some(resumed_at),
                    lines.first().copied(),
                    "{}: the stream resumes where the drop said it would",
                    case.label
                );
            }
        }
    }

    /// The name travels as a rendered column beside the message, not glued to
    /// the front of it. A consumer that wants the name as a column should not
    /// have to search the text for a separator to recover something the sink
    /// had just finished joining.
    #[tokio::test]
    async fn a_line_carries_its_prefix_and_message_apart() {
        struct Case {
            name: &'static str,
            emit: fn(&OutputManager),
            /// What the left column should read, once styling is stripped.
            want_prefix: &'static str,
            want_message: &'static str,
        }

        let cases = [
            Case {
                name: "a lifecycle event is spoken by don",
                emit: |mgr| mgr.lifecycle_event("loading don.toml"),
                want_prefix: "[don]    │ ",
                want_message: "loading don.toml",
            },
            Case {
                name: "a service-scoped event is still don speaking",
                emit: |mgr| mgr.service_event("postgres", "started"),
                want_prefix: "[don]    │ ",
                want_message: "postgres: started",
            },
        ];

        for case in cases {
            let (writer, _buf) = TestBuffer::new();
            let config = crate::config::LogConfig::Stdout;
            let mgr = OutputManager::new(&[("postgres", &config)], writer)
                .await
                .unwrap();
            let mut tap = mgr.log_stream_sender().subscribe();
            (case.emit)(&mgr);
            mgr.shutdown().await;

            let entry = tap.recv().await.unwrap();
            let prefix = strip_ansi(&entry.line.prefix);
            let message = strip_ansi(&entry.line.bytes);
            assert_eq!(prefix, case.want_prefix, "{}: prefix", case.name);
            assert_eq!(message, case.want_message, "{}: message", case.name);
            // And the message must not still be carrying the name.
            assert!(
                !message.contains('|'),
                "{}: the message still has the prefix in it: {message:?}",
                case.name
            );
        }
    }

    /// What a process writes with `\r`, and what the merged stream makes of it.
    ///
    /// A frame of an in-place repaint is a line like any other. It used to
    /// collapse onto the previous frame by re-publishing its id, which meant a
    /// line already laid out could change height and drag the whole log pane
    /// up and down under a reader following the tail. The terminal writer and
    /// the web UI always appended every frame; the tap now agrees with them.
    ///
    /// The CRLF cases are the ones that must never move: a PTY translates
    /// every newline the child writes into `\r\n`, so reading that CR as a
    /// repaint collapsed a service's entire output onto one line — 2,000 lines
    /// became 1.
    #[tokio::test]
    async fn carriage_return_frames_are_ordinary_lines() {
        struct Case {
            name: &'static str,
            /// Raw bytes, as the child writes them.
            emit: &'static [(&'static str, &'static [u8])],
            want: &'static [&'static str],
        }

        let cases = [
            Case {
                name: "every frame of a progress bar is its own line",
                emit: &[("builder", b"10%\r50%\r90%\r")],
                want: &["10%", "50%", "90%"],
            },
            Case {
                name: "the newline that ends it keeps the final frame",
                emit: &[("builder", b"10%\r90%\rdone\n")],
                want: &["10%", "90%", "done"],
            },
            Case {
                name: "CRLF is a line ending, not a repaint",
                emit: &[("builder", b"line one\r\nline two\r\nline three\r\n")],
                want: &["line one", "line two", "line three"],
            },
            Case {
                name: "a repaint mixed in among CRLF lines is still a repaint",
                emit: &[("builder", b"start\r\n10%\r90%\rdone\r\n")],
                want: &["start", "10%", "90%", "done"],
            },
            Case {
                // The case that used to depend on who logged last: a frame
                // only collapsed while it was still the newest line in the
                // merged stream, so two processes repainting at once collapsed
                // neither. Now there is nothing for the interleaving to change.
                name: "a second process interleaves",
                emit: &[
                    ("builder", b"10%\r"),
                    ("api", b"listening\n"),
                    ("builder", b"90%\r"),
                ],
                want: &["10%", "listening", "90%"],
            },
        ];

        for case in cases {
            let (writer, _buf) = TestBuffer::new();
            let config = crate::config::LogConfig::Stdout;
            let mgr = OutputManager::new(&[("builder", &config), ("api", &config)], writer)
                .await
                .unwrap();

            for (name, bytes) in case.emit {
                let service = mgr.service_writer(name).unwrap();
                service
                    .process_stream(std::io::Cursor::new(*bytes))
                    .await
                    .unwrap();
                // Each write is a separate child flush; let the sink drain it
                // so ordering between processes is the order they wrote in.
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            let tap = mgr.log_stream_sender().clone();
            mgr.shutdown().await;

            let got: Vec<String> = tap
                .tail(100)
                .await
                .lines
                .iter()
                .map(|entry| String::from_utf8_lossy(&entry.line.bytes).into_owned())
                .collect();
            assert_eq!(got, case.want, "{}", case.name);

            // Every line burns its own id, so a consumer keyed on the id never
            // sees one arrive twice and never has to revisit a line it has
            // already laid out.
            let ids: Vec<u64> = tap.tail(100).await.lines.iter().map(|e| e.id.0).collect();
            let expected: Vec<u64> = (0..case.want.len() as u64).collect();
            assert_eq!(ids, expected, "{}: one id per line, in order", case.name);
        }
    }

    #[tokio::test]
    async fn log_tap_mirrors_the_emitted_stream() {
        struct Case {
            name: &'static str,
            emit: fn(&OutputManager),
            want_name: &'static str,
            want_contains: &'static str,
            want_lifecycle: bool,
        }

        let cases = vec![
            Case {
                name: "top-level lifecycle event",
                emit: |mgr| mgr.lifecycle_event("loading don.toml"),
                want_name: LIFECYCLE_EVENT_NAME,
                want_contains: "loading don.toml",
                want_lifecycle: true,
            },
            Case {
                // Tagged with the service so followers can filter it the way
                // the TUI does.
                name: "service-scoped lifecycle event",
                emit: |mgr| mgr.service_event("postgres", "started"),
                want_name: "postgres",
                want_contains: "postgres: started",
                want_lifecycle: true,
            },
        ];

        for case in cases {
            let (writer, _buf) = TestBuffer::new();
            let config = crate::config::LogConfig::Stdout;
            let mgr = OutputManager::new(&[("postgres", &config)], writer)
                .await
                .unwrap();
            // Subscribe before emitting — the tap is a broadcast, not a ring.
            let mut tap = mgr.log_stream_sender().subscribe();
            (case.emit)(&mgr);
            mgr.shutdown().await;

            let line = tap.recv().await.unwrap();
            assert_eq!(line.line.name, case.want_name, "{}: name", case.name);
            assert_eq!(
                line.line.is_lifecycle, case.want_lifecycle,
                "{}: lifecycle flag",
                case.name
            );
            let text = strip_ansi(&line.line.bytes);
            assert!(
                text.contains(case.want_contains),
                "{}: expected {:?} in {:?}",
                case.name,
                case.want_contains,
                text
            );
        }
    }

    #[tokio::test]
    async fn test_empty_service_list() {
        let (writer, buf) = TestBuffer::new();
        let mgr = OutputManager::new(&[], writer).await.unwrap();
        mgr.lifecycle_event("hello");
        mgr.shutdown().await;

        let output = read_buf(&buf);
        let stripped = strip_ansi(output.as_bytes());
        assert!(stripped.contains("[don]") && stripped.contains("hello"));
    }

    #[tokio::test]
    async fn item_output_is_a_view_of_the_manager_not_a_copy() {
        struct Case {
            name: &'static str,
            want: &'static str,
            registered: bool,
        }

        let cases = vec![
            Case {
                name: "alpha",
                want: "alpha",
                registered: true,
            },
            Case {
                name: "beta",
                want: "beta",
                registered: true,
            },
            Case {
                name: "never-registered",
                want: "",
                registered: false,
            },
        ];

        let (writer, buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("alpha", &config), ("beta", &config)], writer)
            .await
            .unwrap();

        for case in cases {
            let output = mgr.process_output(case.name);
            assert_eq!(
                output.is_some(),
                case.registered,
                "{}: registered processes get a handle and nothing else does",
                case.name
            );
            if let Some(output) = output {
                assert_eq!(output.name(), case.want, "{}: scoped name", case.name);
                output
                    .writer()
                    .process_stream(std::io::Cursor::new(
                        format!(
                            "{} via handle
",
                            case.name
                        )
                        .into_bytes(),
                    ))
                    .await
                    .unwrap();
            }
        }

        mgr.shutdown().await;

        let out = read_buf(&buf);
        assert!(
            out.contains("alpha via handle"),
            "handle writer reached stdout: {out}"
        );
        assert!(
            out.contains("beta via handle"),
            "handle writer reached stdout: {out}"
        );
    }

    #[tokio::test]
    async fn test_concurrent_services_both_write() {
        let (writer, buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("alpha", &config), ("beta", &config)], writer)
            .await
            .unwrap();

        let alpha = mgr.service_writer("alpha").unwrap();
        let beta = mgr.service_writer("beta").unwrap();

        let (r_a, r_b) = tokio::join!(
            alpha.process_stream(std::io::Cursor::new(b"alpha line\n".to_vec())),
            beta.process_stream(std::io::Cursor::new(b"beta line\n".to_vec())),
        );
        r_a.unwrap();
        r_b.unwrap();

        mgr.shutdown().await;

        let output = read_buf(&buf);
        assert!(output.contains("alpha"), "should have alpha output");
        assert!(output.contains("beta"), "should have beta output");
    }

    #[tokio::test]
    async fn test_runtime_verbose_toggle_updates_emitters_and_stdout_formatting() {
        let (writer, buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new_verbose(&[("svc", &config)], writer, false)
            .await
            .unwrap();
        let verbosity = mgr.verbosity_control();
        let svc = mgr.service_writer("svc").unwrap();

        mgr.debug_event("hidden while verbose is off");
        svc.write_line("before").await;
        tokio::task::yield_now().await;

        verbosity.set_enabled(true);
        mgr.debug_event("visible after toggle");
        svc.write_line("after").await;
        tokio::task::yield_now().await;

        mgr.shutdown().await;

        let output = strip_ansi(read_buf(&buf).as_bytes());
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(
            lines.len(),
            3,
            "expected one normal line, one lifecycle line, one verbose line"
        );
        assert!(
            !lines[0]
                .chars()
                .next()
                .is_some_and(|ch| ch.is_ascii_digit()),
            "first line should not have a verbose timestamp: {:?}",
            lines[0]
        );
        assert!(
            lines[1].contains("visible after toggle"),
            "lifecycle debug event should become visible after toggle"
        );
        assert!(
            lines[2]
                .chars()
                .next()
                .is_some_and(|ch| ch.is_ascii_digit()),
            "third line should gain a verbose timestamp after toggle: {:?}",
            lines[2]
        );
    }

    #[test]
    fn log_filter_passes_table() {
        struct Case {
            name: &'static str,
            allowlist: Option<&'static [&'static str]>,
            line_name: &'static str,
            want: bool,
        }

        let cases = vec![
            Case {
                name: "no filter set passes everything",
                allowlist: None,
                line_name: "api",
                want: true,
            },
            Case {
                name: "no filter set passes don",
                allowlist: None,
                line_name: LIFECYCLE_EVENT_NAME,
                want: true,
            },
            Case {
                name: "filter passes allowlisted name",
                allowlist: Some(&["api"]),
                line_name: "api",
                want: true,
            },
            Case {
                name: "filter rejects non-allowlisted name",
                allowlist: Some(&["api"]),
                line_name: "worker",
                want: false,
            },
            Case {
                name: "filter passes lifecycle even when not in allowlist",
                allowlist: Some(&["api"]),
                line_name: LIFECYCLE_EVENT_NAME,
                want: true,
            },
            Case {
                name: "filter passes empty-name shutdown flush",
                allowlist: Some(&["api"]),
                line_name: "",
                want: true,
            },
            Case {
                name: "empty allowlist still passes lifecycle",
                allowlist: Some(&[]),
                line_name: LIFECYCLE_EVENT_NAME,
                want: true,
            },
            Case {
                name: "empty allowlist drops services",
                allowlist: Some(&[]),
                line_name: "api",
                want: false,
            },
            Case {
                name: "build-tool name is filterable like services",
                allowlist: Some(&["bazel"]),
                line_name: "bazel",
                want: true,
            },
        ];

        for case in cases {
            let filter = LogFilterControl::default();
            if let Some(names) = case.allowlist {
                filter.set(names.iter().map(|s| (*s).to_string()).collect());
            }
            assert_eq!(
                filter.passes(case.line_name),
                case.want,
                "case: {}",
                case.name
            );
        }
    }

    #[tokio::test]
    async fn set_log_filter_drops_non_allowlisted_service_output() {
        let (writer, buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("api", &config), ("worker", &config)], writer)
            .await
            .unwrap();
        mgr.set_log_filter(["api".to_string()].into_iter().collect());

        let api = mgr.service_writer("api").unwrap();
        let worker = mgr.service_writer("worker").unwrap();
        api.process_stream(std::io::Cursor::new(b"api line\n".to_vec()))
            .await
            .unwrap();
        worker
            .process_stream(std::io::Cursor::new(b"worker line\n".to_vec()))
            .await
            .unwrap();
        // Lifecycle events stay visible regardless of the allowlist.
        mgr.lifecycle_event("shutdown complete");
        // Per-service lifecycle events are tagged with the service name, so
        // they follow the allowlist alongside that service's own output.
        mgr.service_event("worker", "send SIGTERM");
        mgr.service_event("api", "ready");
        mgr.shutdown().await;

        let output = strip_ansi(read_buf(&buf).as_bytes());
        assert!(output.contains("api line"), "api stdout should pass");
        assert!(
            !output.contains("worker line"),
            "worker stdout should be filtered out: {output:?}"
        );
        assert!(
            output.contains("shutdown complete"),
            "top-level lifecycle should pass: {output:?}"
        );
        assert!(
            !output.contains("worker: send SIGTERM"),
            "per-service lifecycle for filtered name should drop: {output:?}"
        );
        assert!(
            output.contains("api: ready"),
            "per-service lifecycle for allowlisted name should pass: {output:?}"
        );
    }

    /// Dropping an `OscSinkHandle` (as a restart/stop does when it replaces or
    /// clears `osc_sink`) must remove the sink and stop its task — otherwise
    /// the scanner keeps a gate sender alive, keeping the PTY master's write
    /// half open and leaking one PTY per drop on any exit path that skips
    /// `close_follow_sinks` (e.g. lazy ready-check failures).
    #[tokio::test]
    async fn dropping_osc_sink_handle_removes_sink() {
        let config = crate::config::LogConfig::Stdout;
        let (writer, _buf) = TestBuffer::new();
        let mgr = OutputManager::new(&[("api", &config)], writer)
            .await
            .unwrap();

        let output = mgr.services.get("api").unwrap().clone();
        let before = output.sink_count().await;

        // Hand a real PTY's write half to a gate, and its sender to the
        // scanner — the shape every PTY-backed wire produces.
        let (pty, _pts) = pty_process::open().unwrap();
        let (_read, write) = pty.into_split();
        let pty_input = spawn_pty_gate(write);
        let handle = mgr.add_osc_sink("api", pty_input).await.unwrap();
        assert_eq!(
            output.sink_count().await,
            before + 1,
            "osc sink should be registered"
        );

        drop(handle);

        // Drop aborts the task and posts the sink removal; let the actor
        // apply it.
        let mut removed = false;
        for _ in 0..50 {
            tokio::task::yield_now().await;
            if output.sink_count().await == before {
                removed = true;
                break;
            }
        }
        assert!(
            removed,
            "osc sink must be removed when its handle is dropped (PTY leak guard)"
        );
    }
}
