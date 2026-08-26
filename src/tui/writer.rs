//! The terminal write, off the loop.
//!
//! Rendering is cheap — building the buffer and diffing it against the last
//! one is about one percent of a core, whatever the history holds. Handing the
//! result to the terminal is not: a full-pane repaint is several kilobytes,
//! and on a link that carries a few tens of kilobytes a second that write
//! takes hundreds of milliseconds. `terminal.draw()` writes to the fd
//! directly, so all of that time was spent inside the one select! loop, which
//! was therefore not reading input. Clicks queued, drags stuttered, and
//! double-clicks were pulled apart far enough to read as two single ones.
//!
//! So the bytes go to a thread instead. [`FrameSink`] is an [`std::io::Write`]
//! that collects a frame in memory and, when ratatui flushes it, sends it on;
//! a dedicated thread owns the real stdout and does the blocking write there.
//! An OS thread rather than a task, because that write blocks and would
//! otherwise hold a runtime worker.
//!
//! ## What this does not do
//!
//! It does not queue. A frame is a *diff* against the frame before it, so a
//! backlog cannot be thinned by dropping the stale ones — every one of them
//! has to be applied for the next to make sense. Left unbounded it would grow
//! memory and show a screen that lags reality by however far behind it had
//! fallen.
//!
//! The loop instead declines to *produce* a frame while one is in flight (see
//! the `in_flight` gate in [`super::run`]). Frames are then paced by what the
//! terminal can actually take, and the state they are rendered from is
//! whatever is true when the writer comes free — so ten scroll steps that
//! arrive during one slow write become one repaint of where the scroll ended
//! up, not ten. Since the cost of a frame is bounded by the size of the pane
//! and not by how much scrolled past, rendering less often is strictly
//! cheaper.

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc as std_mpsc;

use tokio::sync::mpsc;

/// What the writer thread accepts.
enum Msg {
    /// Bytes for the terminal, in the order they must arrive.
    Bytes(Vec<u8>),
    /// Stop once everything queued ahead of this has been written.
    ///
    /// An explicit message rather than "every sender was dropped", so that
    /// shutting the writer down does not depend on nobody still holding a
    /// [`TerminalOut`]. Waiting on the last handle to go away is a hang that
    /// only shows up on the paths where one outlives the guard.
    Stop,
}

/// Where a rendered frame goes: into memory, then to the writer thread.
///
/// Handed to `CrosstermBackend` in place of `std::io::Stdout`. Everything
/// ratatui emits for one frame lands in `buf`; the `flush` at the end of
/// `Terminal::draw` is what releases it.
pub(crate) struct FrameSink {
    buf: Vec<u8>,
    tx: std_mpsc::Sender<Msg>,
}

impl Write for FrameSink {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.buf.extend_from_slice(data);
        Ok(data.len())
    }

    /// Release the frame collected so far.
    ///
    /// Sent even when it is empty, which is not a formality. ratatui flushes
    /// once per `Terminal::draw`, and the loop counts one completion for every
    /// frame it renders; a frame whose diff came out empty — the spinner
    /// ticking with nothing of it on screen, say — would otherwise be a
    /// completion that never arrives and a gate that never reopens.
    fn flush(&mut self) -> std::io::Result<()> {
        let frame = std::mem::take(&mut self.buf);
        self.tx.send(Msg::Bytes(frame)).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "terminal writer thread has stopped",
            )
        })
    }
}

/// A handle for sending bytes that are not part of a frame.
///
/// OSC 52 is the one caller: a clipboard request is not a diff and does not
/// touch the screen, but it must not land in the middle of a frame the writer
/// is part-way through. Sharing the writer's queue is what orders it.
#[derive(Clone)]
pub(crate) struct TerminalOut(std_mpsc::Sender<Msg>);

impl TerminalOut {
    /// Queue `bytes` behind whatever the writer is already holding.
    pub(crate) fn send(&self, bytes: Vec<u8>) -> std::io::Result<()> {
        self.0.send(Msg::Bytes(bytes)).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "terminal writer thread has stopped",
            )
        })
    }
}

#[cfg(test)]
impl TerminalOut {
    /// A handle with nothing behind it, for tests that do not care where the
    /// bytes went. Sends fail, which is what "there is no terminal" should
    /// look like to a caller.
    pub(crate) fn discarding() -> Self {
        let (tx, _rx) = std_mpsc::channel();
        Self(tx)
    }
}

/// The writer thread, and the means to wait for it.
pub(crate) struct Writer {
    tx: std_mpsc::Sender<Msg>,
    abandon: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Writer {
    /// Stop the writer and wait for it, without waiting for its backlog.
    ///
    /// Called before the alternate screen is given back, and the two things it
    /// has to balance pull opposite ways.
    ///
    /// The frames still queued must *not* be waited for. On a slow link that
    /// backlog is seconds of writing, and Ctrl+C would sit behind all of it —
    /// measured at four seconds against a hundred and fifty milliseconds when
    /// this drained the queue. A user interrupting is never made to wait for
    /// output they have just said they are done with.
    ///
    /// The frame being written *now* is finished, though, because the restore
    /// sequence goes to the fd directly: overtaking a half-written escape
    /// sequence would split it, and the terminal would render the remains as
    /// text. What is abandoned is only whole frames not yet started — and the
    /// alternate screen they would have painted is about to be thrown away
    /// anyway.
    pub(crate) fn finish(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        self.abandon.store(true, Ordering::Relaxed);
        let _ = self.tx.send(Msg::Stop);
        let _ = handle.join();
    }
}

/// Start the writer thread.
///
/// Returns the sink to give ratatui, a handle for out-of-band bytes, and the
/// thread itself. `done` receives one result per frame written — the loop
/// counts those against the frames it rendered to know whether the terminal is
/// keeping up, and an `Err` ends the TUI.
///
/// Failing to start the thread is returned rather than absorbed. Nothing can
/// be drawn without it, and a TUI that comes up unable to draw is worse than
/// one that says why it did not come up.
pub(crate) fn spawn(
    done: mpsc::UnboundedSender<std::io::Result<()>>,
) -> std::io::Result<(FrameSink, TerminalOut, Writer)> {
    let (tx, rx) = std_mpsc::channel::<Msg>();
    let abandon = Arc::new(AtomicBool::new(false));
    let thread_abandon = Arc::clone(&abandon);
    let handle = std::thread::Builder::new()
        .name("don-tui-writer".to_string())
        .spawn(move || {
            let mut stdout = std::io::stdout();
            // Ends on `Stop`, which is queued behind everything already sent
            // — that is what makes `finish` a real barrier — or when the last
            // sender goes away.
            while let Ok(msg) = rx.recv() {
                let Msg::Bytes(frame) = msg else { break };
                // Teardown has started and this frame has not been begun:
                // drop it rather than make the exit wait for it.
                if thread_abandon.load(Ordering::Relaxed) {
                    let _ = done.send(Ok(()));
                    continue;
                }
                // A terminal that cannot be written to ends the TUI, which is
                // what happened when the write was inline and the error came
                // straight back out of `draw`. Reporting success here instead
                // would leave don running perfectly well against a screen
                // nobody can see.
                let result = stdout.write_all(&frame).and_then(|()| stdout.flush());
                let failed = result.is_err();
                let _ = done.send(result);
                if failed {
                    break;
                }
            }
        })?;
    Ok((
        FrameSink {
            buf: Vec::new(),
            tx: tx.clone(),
        },
        TerminalOut(tx.clone()),
        Writer {
            tx,
            abandon,
            handle: Some(handle),
        },
    ))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    /// A frame is released by the flush at the end of it, not by the writes
    /// that build it up — ratatui emits a frame in many small writes, and one
    /// message per write would be one blocking write per cell.
    #[test]
    fn a_frame_is_one_message_released_by_its_flush() {
        let (done_tx, _done_rx) = mpsc::unbounded_channel();
        let (mut sink, _out, mut writer) = spawn(done_tx).unwrap();
        let (probe_tx, probe_rx) = std_mpsc::channel();
        sink.tx = probe_tx;

        sink.write_all(b"abc").unwrap();
        sink.write_all(b"def").unwrap();
        assert!(
            probe_rx.try_recv().is_err(),
            "nothing should go out until the flush"
        );

        sink.flush().unwrap();
        match probe_rx.try_recv().unwrap() {
            Msg::Bytes(frame) => assert_eq!(frame, b"abcdef"),
            Msg::Stop => panic!("a flush is not a stop"),
        }

        // An empty frame is still reported. The loop waits for one completion
        // per frame it rendered, and a draw whose diff came out empty must not
        // leave it waiting for an arrival that never comes.
        sink.flush().unwrap();
        match probe_rx.try_recv().unwrap() {
            Msg::Bytes(frame) => assert!(frame.is_empty(), "empty frame, still announced"),
            Msg::Stop => panic!("a flush is not a stop"),
        }

        drop(sink);
        writer.finish();
    }

    /// A frame that could not be written is reported as a failure, not as a
    /// frame that went out.
    ///
    /// When the write was inline the error came straight back out of `draw`
    /// and ended the TUI. Off the loop it has to be carried, or a terminal
    /// that has gone away leaves don running perfectly well against a screen
    /// nobody can see — with the in-flight count returning to zero each time,
    /// so nothing even looks stuck.
    #[test]
    fn a_write_that_fails_is_reported_as_a_failure() {
        // A sink whose queue has no thread behind it: the send fails, which is
        // what the frame path sees when the writer has gone.
        let (tx, rx) = std_mpsc::channel();
        drop(rx);
        let mut sink = FrameSink {
            buf: Vec::new(),
            tx: tx.clone(),
        };
        sink.write_all(b"frame").unwrap();
        let err = sink.flush().expect_err("a send with no receiver must fail");
        assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);

        // And the same for an out-of-band write, so a clipboard request cannot
        // quietly evaporate either.
        let out = TerminalOut(tx);
        assert_eq!(
            out.send(b"osc".to_vec()).unwrap_err().kind(),
            std::io::ErrorKind::BrokenPipe
        );
    }

    /// Every frame written is reported, so the loop's in-flight count returns
    /// to zero and it renders again.
    #[test]
    fn each_written_frame_is_reported_once() {
        let (done_tx, mut done_rx) = mpsc::unbounded_channel();
        let (mut sink, out, mut writer) = spawn(done_tx).unwrap();

        for _ in 0..3 {
            sink.write_all(b"x").unwrap();
            sink.flush().unwrap();
        }
        out.send(b"osc".to_vec()).unwrap();
        drop(sink);
        drop(out);
        writer.finish();

        let mut reported = 0;
        while let Ok(result) = done_rx.try_recv() {
            result.unwrap();
            reported += 1;
        }
        assert_eq!(reported, 4, "three frames and the out-of-band write");
    }
}
