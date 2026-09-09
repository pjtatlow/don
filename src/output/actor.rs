//! One actor per process, owning that process's output state.
//!
//! Every field here used to sit behind an `Arc<Mutex<_>>` that the writer,
//! the attach sessions, the log reader and the OSC scanner all took in turn.
//! None of them held it across an await, so it was never a deadlock risk —
//! but it was the last shared mutable state in don, and it made two things
//! awkward that are natural for an actor: the "first attaching client mutes
//! stdout" transition needed a comment explaining which mutations had to
//! happen under one lock, and dropping an `OscSinkHandle` had to spawn a
//! task just to take the lock and remove its own sink.
//!
//! The split into two channels is load bearing:
//!
//! - **Output is bounded and strictly ordered.** A child that floods must be
//!   made to wait, which is what the lock did for free; an unbounded channel
//!   would turn a noisy service into unbounded memory growth. Chunks, whole
//!   lines and the end-of-stream flush all ride this one channel, because
//!   they are the same stream and must not overtake each other.
//! - **Everything else is unbounded, and drained first.** An attach, or the
//!   reap that clears one, must not queue behind a burst of output — with the
//!   lock they never did, because they could take it between chunks.
//!
//! The end-of-stream flush is acknowledged, and that is what preserves the
//! guarantee the supervisors depend on: awaiting a process's output reader
//! means its output has been *recorded and fanned out*, not merely handed
//! over. "stopped" still cannot outrun a process's last lines.

use super::ring_buffer::RingBuffer;
use super::{CompiledLineFilter, MAX_FILTER_PENDING, PtyInput, SinkHandle, SinkLine};
use bytes::{Bytes, BytesMut};
use tokio::sync::{mpsc, oneshot};

/// How many chunks may be in flight to an actor before its reader waits.
///
/// Deep enough that a normal burst never blocks the reader, shallow enough
/// that a service spraying output cannot grow the queue without bound.
const CHUNK_QUEUE_DEPTH: usize = 256;

/// A piece of a process's output stream, in order.
enum Output {
    /// Raw bytes from the child.
    Chunk(Bytes),
    /// A whole line, newline included — structured output (docker build
    /// progress) that never was a byte stream.
    Line(Bytes),
    /// End of stream: flush the partial line the filter is holding, then
    /// answer, so the caller knows everything it wrote has landed.
    Flush { ack: oneshot::Sender<()> },
}

/// Per-process output state. Owned outright by [`run`], the actor task for
/// that process — this is why none of it is behind a lock.
struct ServiceOutputState {
    /// Service/task name — stamped onto every emitted `SinkLine` so the TUI
    /// can filter without having to reverse-map the prefix bytes.
    pub(super) name: String,
    prefix: Bytes,
    ring_buffer: RingBuffer,
    line_filter: CompiledLineFilter,
    filter_pending: BytesMut,
    /// Dynamic list of sinks this service writes to.
    sinks: Vec<SinkHandle>,
    /// The live spawn's PTY input-gate sender, registered by the supervisor
    /// at wire time and cleared at reap. `None` = nothing attachable
    /// (stopped, docker, or pipe mode). See [`attach`].
    attach_pty: Option<mpsc::Sender<PtyInput>>,
    /// Attached clients, counted by [`attach::AttachControl`]; drives the
    /// stdout-sink pause. Reset by the supervisor's reap clear.
    attach_clients: usize,
    secret_redactor: super::redact::SecretRedactor,
}

impl ServiceOutputState {
    fn output_chunks(&mut self, chunk: Bytes) -> Vec<Bytes> {
        let chunk = Bytes::from(self.secret_redactor.redact_bytes(&chunk));
        if self.line_filter.is_empty() {
            self.ring_buffer.push_chunk(chunk.as_ref());
            return vec![chunk];
        }

        self.filter_chunk(chunk.as_ref(), false)
    }

    fn flush_output(&mut self) -> Vec<Bytes> {
        if self.line_filter.is_empty() {
            self.ring_buffer.flush_pending();
            return Vec::new();
        }

        self.filter_chunk(&[], true)
    }

    fn filter_chunk(&mut self, chunk: &[u8], flush: bool) -> Vec<Bytes> {
        self.filter_pending.extend_from_slice(chunk);
        let mut accepted = Vec::new();

        loop {
            let end = if let Some(pos) = self.filter_pending.iter().position(|&b| b == b'\n') {
                pos + 1
            } else if self.filter_pending.len() >= MAX_FILTER_PENDING {
                MAX_FILTER_PENDING
            } else {
                break;
            };

            let line = self.filter_pending.split_to(end).freeze();
            self.accept_line(line, &mut accepted);
        }

        if flush && !self.filter_pending.is_empty() {
            let line = self.filter_pending.split().freeze();
            self.accept_line(line, &mut accepted);
            self.ring_buffer.flush_pending();
        }

        accepted
    }

    fn accept_line(&mut self, line: Bytes, accepted: &mut Vec<Bytes>) {
        if !self.line_filter.keeps(line.as_ref()) {
            return;
        }
        let line = Bytes::from(self.secret_redactor.redact_bytes(&line));
        self.ring_buffer.push_chunk(line.as_ref());
        accepted.push(line);
    }
}

/// What a process's output actor can be asked to do.
///
/// Only [`Chunk`](Self::Chunk) and [`Flush`](Self::Flush) ride the bounded
/// channel; the rest are control and never wait behind output.
enum OutputMsg {
    /// Drop transient (follow / attach / OSC) sinks. Persistent ones —
    /// stdout, file — survive for the next spawn.
    CloseFollowSinks,
    AddSink(SinkHandle),
    /// Remove one sink by channel identity. The OSC scanner's handle does
    /// this on drop; it used to have to spawn a task to take the lock.
    RemoveSink(SinkHandle),
    /// Add a sink unless an equal one is already registered.
    AddSinkOnce(SinkHandle),
    /// Register (or clear) the live spawn's PTY input gate.
    SetAttachPty(Option<mpsc::Sender<PtyInput>>),
    /// The spawn is gone: forget the gate, reset the client count, and undo
    /// any stdout pause those clients caused.
    ClearAttach,
    /// One client is attaching. Answers with the gate and the resulting
    /// client count, having muted stdout if this is the first — one message,
    /// so the transition cannot race a concurrent attach or the reap clear.
    Attach {
        reply: oneshot::Sender<Option<(mpsc::Sender<PtyInput>, usize)>>,
    },
    /// One client detached. Answers with the remaining count; `None` if the
    /// count was already zero (a late notification after the reap clear).
    Detach {
        reply: oneshot::Sender<Option<usize>>,
    },
    /// The last `n` ring-buffer lines, joined, trailing newline stripped.
    ReadLogs {
        n: usize,
        reply: oneshot::Sender<Bytes>,
    },
    /// A fresh follow sink, preloaded with the last `last_n` lines.
    FollowSink {
        last_n: usize,
        live_capacity: usize,
        reply: oneshot::Sender<mpsc::Receiver<SinkLine>>,
    },
    /// A fresh attach sink, plus a repaint of the screen as it stood the
    /// instant that sink was added.
    ///
    /// Both halves are cut at the same point *by this actor*, which is the
    /// whole reason it does the asking. The repaint used to be fetched by the
    /// caller and handed in, leaving a window between the snapshot and the
    /// sink landing: anything the process wrote in it reached the screen and
    /// the log but never the client, so attaching to something that printed
    /// once and waited — a prompt — showed an empty window about one time in
    /// five. Here the request to the emulator is queued behind exactly the
    /// bytes this sink will not receive, because the same thread that fans
    /// bytes out is the one issuing it.
    AttachSink {
        capacity: usize,
        reply: oneshot::Sender<(
            mpsc::Receiver<SinkLine>,
            oneshot::Receiver<Option<super::emulator::RepaintFrame>>,
        )>,
    },
    /// Drop every sink. Shutdown, so the writer tasks can drain and exit.
    ClearSinks,
    /// How many sinks are registered. Tests only — it is the observable that
    /// pins the OSC scanner's drop behaviour (a leaked sink is a leaked PTY).
    #[cfg(test)]
    SinkCount {
        reply: oneshot::Sender<usize>,
    },
}

/// Handle to one process's output actor. Cloneable; reusable across restarts.
#[derive(Clone)]
pub(super) struct OutputHandle {
    /// Fixed at registration, so a caller that only needs the prefix — the
    /// build-tool column — costs no round trip.
    prefix: Bytes,
    output: mpsc::Sender<Output>,
    control: mpsc::UnboundedSender<OutputMsg>,
}

impl OutputHandle {
    pub(super) fn prefix(&self) -> &Bytes {
        &self.prefix
    }

    /// Hand a chunk of child output to the actor, waiting if it is behind.
    /// Waiting is the point — see the module docs.
    pub(super) async fn chunk(&self, chunk: Bytes) {
        let _ = self.output.send(Output::Chunk(chunk)).await;
    }

    pub(super) async fn line(&self, line: Bytes) {
        let _ = self.output.send(Output::Line(line)).await;
    }

    /// Flush the stream and wait for everything sent before it to be
    /// recorded and fanned out.
    pub(super) async fn flush(&self) {
        let (ack, done) = oneshot::channel();
        if self.output.send(Output::Flush { ack }).await.is_ok() {
            let _ = done.await;
        }
    }

    fn send(&self, msg: OutputMsg) {
        let _ = self.control.send(msg);
    }

    pub(super) fn close_follow_sinks(&self) {
        self.send(OutputMsg::CloseFollowSinks);
    }

    pub(super) fn add_sink(&self, sink: SinkHandle) {
        self.send(OutputMsg::AddSink(sink));
    }

    pub(super) fn add_sink_once(&self, sink: SinkHandle) {
        self.send(OutputMsg::AddSinkOnce(sink));
    }

    pub(super) fn remove_sink(&self, sink: SinkHandle) {
        self.send(OutputMsg::RemoveSink(sink));
    }

    pub(super) fn set_attach_pty(&self, pty: Option<mpsc::Sender<PtyInput>>) {
        self.send(OutputMsg::SetAttachPty(pty));
    }

    pub(super) fn clear_attach(&self) {
        self.send(OutputMsg::ClearAttach);
    }

    pub(super) fn clear_sinks(&self) {
        self.send(OutputMsg::ClearSinks);
    }

    #[cfg(test)]
    pub(super) async fn sink_count(&self) -> usize {
        let (reply, rx) = oneshot::channel();
        self.send(OutputMsg::SinkCount { reply });
        rx.await.unwrap_or_default()
    }

    pub(super) async fn attach(&self) -> Option<(mpsc::Sender<PtyInput>, usize)> {
        let (reply, rx) = oneshot::channel();
        self.send(OutputMsg::Attach { reply });
        rx.await.ok().flatten()
    }

    pub(super) async fn detach(&self) -> Option<usize> {
        let (reply, rx) = oneshot::channel();
        self.send(OutputMsg::Detach { reply });
        rx.await.ok().flatten()
    }

    pub(super) async fn read_logs(&self, n: usize) -> Bytes {
        let (reply, rx) = oneshot::channel();
        self.send(OutputMsg::ReadLogs { n, reply });
        rx.await.unwrap_or_default()
    }

    pub(super) async fn follow_sink(
        &self,
        last_n: usize,
        live_capacity: usize,
    ) -> Option<mpsc::Receiver<SinkLine>> {
        let (reply, rx) = oneshot::channel();
        self.send(OutputMsg::FollowSink {
            last_n,
            live_capacity,
            reply,
        });
        rx.await.ok()
    }

    /// A sink for one attach session, and the repaint that precedes it.
    ///
    /// The repaint arrives on its own channel rather than inside the sink: it
    /// has to reach the client *first*, and the sink starts taking live bytes
    /// the moment it exists.
    pub(super) async fn attach_sink(
        &self,
        capacity: usize,
    ) -> Option<(
        mpsc::Receiver<SinkLine>,
        oneshot::Receiver<Option<super::emulator::RepaintFrame>>,
    )> {
        let (reply, rx) = oneshot::channel();
        self.send(OutputMsg::AttachSink { capacity, reply });
        rx.await.ok()
    }
}

/// Start one process's output actor.
#[allow(clippy::too_many_arguments)]
pub(super) fn spawn(
    name: String,
    prefix: Bytes,
    sinks: Vec<SinkHandle>,
    stdout_sink: SinkHandle,
    mute: super::StdoutMuteControl,
    line_filter: CompiledLineFilter,
    secret_redactor: super::redact::SecretRedactor,
) -> OutputHandle {
    let (output_tx, output_rx) = mpsc::channel(CHUNK_QUEUE_DEPTH);
    let (control_tx, control_rx) = mpsc::unbounded_channel();
    let state = ServiceOutputState {
        name: name.clone(),
        prefix: prefix.clone(),
        ring_buffer: RingBuffer::new(super::DEFAULT_RING_BUFFER_CAPACITY),
        line_filter,
        filter_pending: BytesMut::new(),
        sinks,
        attach_pty: None,
        attach_clients: 0,
        secret_redactor,
    };
    // Detached: the actor ends when its channels close, which happens when
    // the last handle drops — or immediately on `ClearSinks`, which is how
    // shutdown releases the sink senders the writer tasks are waiting on.
    tokio::spawn(run(state, stdout_sink, mute, output_rx, control_rx));
    OutputHandle {
        prefix,
        output: output_tx,
        control: control_tx,
    }
}

/// One process's output loop.
async fn run(
    mut state: ServiceOutputState,
    stdout_sink: SinkHandle,
    mute: super::StdoutMuteControl,
    mut output: mpsc::Receiver<Output>,
    mut control: mpsc::UnboundedReceiver<OutputMsg>,
) {
    loop {
        tokio::select! {
            // Control first: an attach, or the reap that clears one, must
            // never wait out a backlog of output. See the module docs.
            biased;
            Some(msg) = control.recv() => {
                if !state.apply(msg, &mute) {
                    return;
                }
            }
            Some(piece) = output.recv() => match piece {
                Output::Chunk(chunk) | Output::Line(chunk) => {
                    let emitted = state.output_chunks(chunk);
                    state.fan_out(emitted);
                }
                Output::Flush { ack } => {
                    let emitted = state.flush_output();
                    state.fan_out(emitted);
                    // Ordered after the fan-out: the marker means "everything
                    // this run wrote is already past you".
                    state.mark_stream_end(&stdout_sink);
                    let _ = ack.send(());
                }
            },
            else => return,
        }
    }
}

impl ServiceOutputState {
    /// Send these chunks to every sink, pruning any that have gone away.
    ///
    /// Pruning is just a retain now. It used to be a second lock acquisition
    /// after the sends, because the sinks had to be cloned out and released
    /// before anything could be written to them.
    fn fan_out(&mut self, chunks: Vec<Bytes>) {
        if chunks.is_empty() {
            return;
        }
        let mut dropped: Vec<SinkHandle> = Vec::new();
        for chunk in chunks {
            for sink in &self.sinks {
                let msg = SinkLine {
                    prefix: self.prefix.clone(),
                    line: chunk.clone(),
                    name: self.name.clone(),
                    is_lifecycle: false,
                    is_verbose: false,
                };
                if sink.send(msg).is_err() {
                    dropped.push(sink.clone());
                }
            }
        }
        if !dropped.is_empty() {
            self.sinks
                .retain(|s| !dropped.iter().any(|d| d.same_channel(s)));
        }
        self.sinks.retain(|s| !s.is_closed());
    }

    /// Tell the stdout writer that the process writing here has stopped.
    ///
    /// A zero-length line is the marker, and only the stdout sink is ever sent
    /// one: [`Self::fan_out`] carries non-empty read chunks, `line()` appends a
    /// newline, and lifecycle events carry text — so nothing else can produce
    /// it. Sent directly rather than through `self.sinks` so a paused stdout
    /// still gets it; it is a control marker, not output.
    ///
    /// The writer keeps state per process across runs, and some of it a run can
    /// end without unwinding: a process SIGKILLed while it owned the alternate
    /// screen never sends the sequence that gives the screen back, and its
    /// successor's output would be swallowed too. This is where that is
    /// dropped, and where a final partial line gets flushed.
    fn mark_stream_end(&self, stdout_sink: &SinkHandle) {
        let _ = stdout_sink.send(SinkLine {
            prefix: self.prefix.clone(),
            line: Bytes::new(),
            name: self.name.clone(),
            is_lifecycle: false,
            is_verbose: false,
        });
    }

    /// Apply one control message. Returns `false` when the actor should end.
    ///
    /// An attach *mutes* this process's terminal output rather than unwiring
    /// its stdout sink: the sink is how the merged log tap is fed, and every
    /// client reads that. Unwiring it would blind them all for the duration of
    /// somebody else's attach. A service with `log = "ignore"` is muted by
    /// configuration, which is a separate set, so releasing an attach can never
    /// hand it a terminal it was never meant to have.
    fn apply(&mut self, msg: OutputMsg, mute: &super::StdoutMuteControl) -> bool {
        match msg {
            OutputMsg::CloseFollowSinks => self.sinks.retain(|s| !s.is_transient()),
            OutputMsg::AddSink(sink) => self.sinks.push(sink),
            OutputMsg::AddSinkOnce(sink) => {
                if !self.sinks.iter().any(|s| s.same_channel(&sink)) {
                    self.sinks.push(sink);
                }
            }
            OutputMsg::RemoveSink(sink) => self.sinks.retain(|s| !s.same_channel(&sink)),
            OutputMsg::SetAttachPty(pty) => self.attach_pty = pty,
            OutputMsg::ClearAttach => {
                self.attach_pty = None;
                self.attach_clients = 0;
                mute.release(&self.name);
            }
            OutputMsg::Attach { reply } => {
                let answer = self.attach_pty.clone().map(|pty| {
                    self.attach_clients += 1;
                    if self.attach_clients == 1 {
                        mute.attach(&self.name);
                    }
                    (pty, self.attach_clients)
                });
                let _ = reply.send(answer);
            }
            OutputMsg::Detach { reply } => {
                let answer = if self.attach_clients == 0 {
                    None
                } else {
                    self.attach_clients -= 1;
                    if self.attach_clients == 0 {
                        mute.release(&self.name);
                    }
                    Some(self.attach_clients)
                };
                let _ = reply.send(answer);
            }
            OutputMsg::ReadLogs { n, reply } => {
                let mut result: Vec<u8> = Vec::new();
                for part in self.ring_buffer.last_n(n) {
                    result.extend_from_slice(part);
                }
                // Strip the trailing `\n` for clean output.
                if result.last() == Some(&b'\n') {
                    result.pop();
                }
                let _ = reply.send(Bytes::from(result));
            }
            OutputMsg::FollowSink {
                last_n,
                live_capacity,
                reply,
            } => {
                // Capacity must hold the preloaded snapshot *and* live
                // headroom, or a freshly-connected client is dropped for
                // being slow before it has read anything.
                let capacity = last_n.saturating_add(live_capacity).max(1);
                let (tx, rx) = mpsc::channel::<SinkLine>(capacity);
                for line in self.ring_buffer.last_n(last_n) {
                    // Ring-buffer entries keep their trailing `\n`, but
                    // `SinkLine.line` is contractually newline-free.
                    let line = line.strip_suffix(b"\n").unwrap_or(line);
                    // The channel is empty and has `capacity` slots.
                    let _ = tx.try_send(SinkLine {
                        prefix: self.prefix.clone(),
                        line: Bytes::copy_from_slice(line),
                        name: self.name.clone(),
                        is_lifecycle: false,
                        is_verbose: false,
                    });
                }
                self.sinks.push(SinkHandle::BoundedDrop(tx));
                let _ = reply.send(rx);
            }
            OutputMsg::AttachSink { capacity, reply } => {
                let (tx, rx) = mpsc::channel::<SinkLine>(capacity);
                let (frame_tx, frame_rx) = oneshot::channel();
                // Ask for the repaint before adding the sink, on the same
                // channel the screen is fed through: the request queues behind
                // every byte already fanned out and ahead of every byte this
                // sink is about to get. Ordering the two cuts is the point —
                // see `OutputMsg::AttachSink`.
                let emulator = self.sinks.iter().find_map(|sink| match sink {
                    SinkHandle::Emulator(emulator) => Some(emulator.clone()),
                    _ => None,
                });
                match emulator {
                    Some(emulator) => {
                        let _ = emulator.send(super::emulator::EmulatorRequest::Repaint {
                            name: self.name.clone(),
                            reply: frame_tx,
                        });
                    }
                    // No screen to repaint from — a pipe spawn, or an
                    // emulator backend that would not start. Preload the last
                    // lines instead, so attaching still opens on something,
                    // and drop the sender so the caller gets `None` rather
                    // than waiting for a reply nobody will send.
                    None => {
                        drop(frame_tx);
                        for line in self.ring_buffer.last_n(50) {
                            let line = line.strip_suffix(b"\n").unwrap_or(line);
                            let _ = tx.try_send(SinkLine {
                                prefix: self.prefix.clone(),
                                line: Bytes::copy_from_slice(line),
                                name: self.name.clone(),
                                is_lifecycle: false,
                                is_verbose: false,
                            });
                        }
                    }
                }
                self.sinks.push(SinkHandle::BoundedDrop(tx));
                let _ = reply.send((rx, frame_rx));
            }
            OutputMsg::ClearSinks => {
                self.sinks.clear();
                return false;
            }
            #[cfg(test)]
            OutputMsg::SinkCount { reply } => {
                let _ = reply.send(self.sinks.len());
            }
        }
        true
    }
}
