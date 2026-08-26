#!/usr/bin/env python3
"""Drive an attached don TUI under a real PTY for detach/shutdown checks.

Usage: tui_attach_drive.py <don-binary> <project-dir> <ctrl-d|ctrl-c> <linger-s> [subcommand]

Spawns `don <subcommand>` (default `attach`; pass `start` for the fork model)
under a PTY, answers DSR cursor queries, sets a real window size, sends the
requested control key after <linger-s> seconds, and reports what it saw:

    exit=0 bytes=7427
    saw_tick=True saw_bar=True saw_detached=True saw_shutdown=False

Ctrl+D must leave the runner alive (check `don status` after); Ctrl+C must
shut the stack down. A verdict of HANG is only issued after reaping — a
master-EOF with a clean child exit is exit=0, not a hang (a lesson learned
the confusing way).
"""
import os, pty, sys, time, select, signal, re, fcntl, termios, struct

binary, cwd, keys, linger = os.path.abspath(sys.argv[1]), sys.argv[2], sys.argv[3], float(sys.argv[4])
subcmd = sys.argv[5] if len(sys.argv) > 5 else "attach"
KEYMAP = {"ctrl-d": b"\x04", "ctrl-c": b"\x03"}

pid, master = pty.fork()
if pid == 0:
    os.chdir(cwd)
    os.environ["TERM"] = "xterm-256color"
    os.execv(binary, [binary] + subcmd.split())
    os._exit(127)

fcntl.ioctl(master, termios.TIOCSWINSZ, struct.pack("HHHH", 40, 120, 0, 0))
flags = fcntl.fcntl(master, fcntl.F_GETFL)
fcntl.fcntl(master, fcntl.F_SETFL, flags | os.O_NONBLOCK)

captured = bytearray()
deadline = time.time() + 30
sent = False
send_at = time.time() + linger
exit_code = None
dsr_re = re.compile(rb"\x1b\[6n")

while time.time() < deadline:
    r, _, _ = select.select([master], [], [], 0.05)
    if r:
        try:
            data = os.read(master, 65536)
        except BlockingIOError:
            data = b""
        except OSError:
            break
        if data:
            captured.extend(data)
            for _ in dsr_re.finditer(data):
                os.write(master, b"\x1b[1;1R")
    if not sent and time.time() >= send_at:
        os.write(master, KEYMAP[keys])
        sent = True
    done, status = os.waitpid(pid, os.WNOHANG)
    if done:
        # Drain what's left.
        time.sleep(0.2)
        try:
            captured.extend(os.read(master, 65536))
        except OSError:
            pass
        exit_code = os.waitstatus_to_exitcode(status)
        break

if exit_code is None:
    # Master EOF usually means the child exited; give it a moment to reap
    # before declaring a hang.
    for _ in range(20):
        done, status = os.waitpid(pid, os.WNOHANG)
        if done:
            exit_code = os.waitstatus_to_exitcode(status)
            break
        time.sleep(0.1)
    if exit_code is None:
        os.kill(pid, signal.SIGKILL)
        os.waitpid(pid, 0)
        exit_code = "HANG"

text = captured.decode("utf-8", "replace")
plain = re.sub(r"\x1b\[[0-9;?]*[A-Za-z]|\x1b[()][0-9A-B]", "", text)
print(f"exit={exit_code} bytes={len(captured)}", file=sys.stderr)
print(f"saw_tick={'tick' in plain}", file=sys.stderr)
print(f"saw_bar={'ticker' in plain}", file=sys.stderr)
print(f"saw_detached={'detached' in plain}", file=sys.stderr)
print(f"saw_shutdown={'shutting down' in plain or 'shutdown complete' in plain}", file=sys.stderr)
