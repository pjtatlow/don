#!/usr/bin/env python3
"""Check that a repainting progress bar never moves the log above it.

Why this exists
---------------
Following the tail is bottom-anchored: the pane places itself at
`total_rows - height`, so anything that changes the total moves every row on
screen. Content arriving does that, and it is meant to — the log scrolls up.
What must never happen is a row moving back *down*, because nothing was
appended below to push it there.

A process that ends a line with a bare `\\r` is repainting it, and don used to
model that by re-publishing the line's id so the newest line was replaced in
place. A progress bar's text changes length every frame, so when it crossed the
wrap width its height flipped between one row and two, the total moved with it,
and the whole history above jumped up a row and back down several times a
second. Frames are ordinary lines now — nothing already laid out ever changes
height — and this is the invariant that says so.

The check is direction, not stillness: a line may leave the top of the pane, it
just may not descend. That holds whether or not anything else is logging, which
is what makes it a usable assertion while a progress bar is producing lines.

The load it needs is a service that repaints with frames of swinging length,
long enough to wrap, plus numbered output to track:

    cargo build --release
    python3 tools/tui_drive_progress.py target/release/don /tmp/don-progress

Writes its own don.toml into the project directory if there isn't one.

Install `pyte` for this one. The fallback emulator mis-renders escape
sequences split across reads, which shows up as text corruption that looks
like a rendering bug and is not one.
"""

import os
import re
import sys
import time

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from tui_emulator import HAVE_PYTE, Session  # noqa: E402

# `bazel` alternates a short frame with one long enough to wrap, so its height
# swings; `chatter` fills the pane with lines to track and then goes quiet, so
# what happens next is the progress bar's doing and nothing else's.
CONFIG = """\
[services.bazel]
run.cmd = "sh"
run.args = ["-c", "s='[1,234 / 5,678] Compiling src/foo.cc'; l='[1,235 / 5,678] Compiling a/very/long/path/that/goes/on/and/on/for/quite/a/while/deep/in/the/source/tree/some_target_name.cc; 12s remote-cache, linux-sandbox; 4 actions running, 2 queued'; i=0; while true; do i=$((i+1)); if [ $((i % 2)) -eq 0 ]; then printf '%s\\\\r' \\"$s\\"; else printf '%s\\\\r' \\"$l\\"; fi; sleep 0.2; done"]

[services.chatter]
run.cmd = "sh"
run.args = ["-c", "i=0; while [ $i -lt 60 ]; do echo \\"tick $i steady line that should hold its place\\"; i=$((i+1)); sleep 0.2; done; sleep 100000"]
"""

TRACKED = re.compile(r"tick (\d+)\b")

# Longer than the repaint interval, so consecutive samples cannot land on the
# same phase of it. Sampling in lockstep with the thing under test is how an
# earlier version of this probe reported a clean run against a binary that was
# visibly jumping.
SAMPLE_PUMP = 0.22


def tlog(start, message):
    print("[%6dms] %s" % ((time.time() - start) * 1000, message), file=sys.stderr)


def rows_by_line(session):
    """Screen row of each tracked line currently visible."""
    found = {}
    for row, text in enumerate(session.screen.display()):
        match = TRACKED.search(text)
        if match:
            found[int(match.group(1))] = row
    return found


def main():
    if len(sys.argv) < 3:
        print("usage: tui_drive_progress.py <don-binary> <project-dir>", file=sys.stderr)
        return 2
    binary, project = sys.argv[1], sys.argv[2]
    samples = int(sys.argv[3]) if len(sys.argv) > 3 else 20

    os.makedirs(project, exist_ok=True)
    config = os.path.join(project, "don.toml")
    if not os.path.exists(config):
        with open(config, "w") as handle:
            handle.write(CONFIG)

    start = time.time()
    session = Session(binary, project, cols=120, rows=40)
    tlog(start, "spawned don pid=%s (pyte: %s)" % (session.pid, HAVE_PYTE))

    try:
        # Wait for chatter's *last* line: only then is the pane full, and a
        # pane that is not full is top-anchored and cannot show this bug.
        if not session.wait_for_screen(re.compile(r"tick 59\b"), 90):
            tlog(start, "FAIL: the pane never filled")
            return 1
        tlog(start, "pane full, ordinary output stopped")

        tracks = {}
        for _ in range(samples):
            session.pump(SAMPLE_PUMP)
            session.settle()
            for line, row in rows_by_line(session).items():
                tracks.setdefault(line, []).append(row)
    finally:
        session.interrupt()
        session.wait_exit(10)
        session.kill()

    descended = {}
    for line, rows in tracks.items():
        falls = [(a, b) for a, b in zip(rows, rows[1:]) if b > a]
        if falls:
            descended[line] = rows

    tlog(start, "samples: %d" % samples)
    tlog(start, "lines tracked: %d" % len(tracks))
    tlog(start, "lines that moved back down: %d" % len(descended))
    for line in sorted(descended)[:6]:
        tlog(start, "  tick %d: %s" % (line, descended[line]))

    if not tracks:
        tlog(start, "FAIL: nothing to track")
        return 1
    if descended:
        tlog(start, "RESULT: FAIL")
        return 1
    tlog(start, "RESULT: ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
