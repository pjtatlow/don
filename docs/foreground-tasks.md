# Foreground tasks and terminal job control

A task with `terminal = { mode = "foreground" }` takes over the user's
controlling terminal for as long as it runs. `spawn_foreground_process`
(`src/process/mod.rs`) inherits stdin/stdout/stderr into the child, puts the
child in its own process group, and calls `tcsetpgrp` to make the child's group
the terminal's **foreground** process group.

## The wedge this creates

While the child owns the terminal, don's own process group is a **background**
group for that terminal. POSIX job control then stops the whole group whenever
any thread:

- **reads** the controlling tty — the kernel raises `SIGTTIN`, or
- **changes** the tty's foreground pgrp or attributes (`tcsetpgrp`,
  `tcsetattr`), or writes it when `TOSTOP` is set — the kernel raises
  `SIGTTOU`.

The default disposition of both signals is to **STOP** the entire process
group. A stopped daemon can't run its tokio runtime, so it never reaps the
finished child or processes its exit: the task is stuck in `running` forever and
the TUI/IPC freeze until don is manually `SIGCONT`ed or restarted. In production
this showed up as don in process state `T` (stopped) with an already-exited
`bazel` child left unreaped.

The most likely read vector is crossterm's `EventStream`. Its Unix source uses
a **detached** OS reader thread that `read()`s the tty *after* `poll()` reports
it readable (`poll()` alone does not raise `SIGTTIN`). The TUI tears that reader
down (`ActiveTerm::tear_down`: abort + join + `EventStream` drop) before the
child is foregrounded and only rebuilds it on `Release`, after the child exits
and the terminal is restored — so in the normal flow no don thread reads stdin
during the window. But `EventStream`'s thread is not joined on drop, leaving a
narrow race where it executes its `read()` at the instant don goes background.
Any such residual/racy read is enough to STOP the daemon.

## The fix: `JobControlStopGuard`

Rather than chase the exact racing thread, don makes the **whole process**
immune to the job-control stop signals for the entire foreground window.
`TerminalGuard` (which brackets the window from just before `enter` until the
child's exit restores the terminal) holds a `JobControlStopGuard` that sets
`SIGTTIN` and `SIGTTOU` to `SIG_IGN` on construction and restores the previous
dispositions on drop. Dispositions are process-wide, so this protects every
thread regardless of which one touches the tty.

Ordering matters in `TerminalGuard::drop`: its `tcsetpgrp`/`tcsetattr` restore
runs while don is *still* background and would itself raise `SIGTTOU`. The
`_stop_guard` field is declared last so it drops **after** the `Drop` body runs,
keeping the restore protected.

With `SIGTTIN` ignored, a background tty read returns `EIO` instead of stopping.
That converts the catastrophic daemon-wide stop into, at worst, a bounded error
on whichever thread read — strictly better. As defense-in-depth the TUI input
task (`src/tui/input.rs`) backs off on consecutive read errors so an `EIO`
storm can't turn into a busy-spin.

### Why `SIGTSTP` is intentionally not handled

Ctrl+Z at the terminal is delivered by the line discipline to the terminal's
**foreground** process group, which during the window is the child's group, not
don's. So don cannot be stopped by Ctrl+Z while a foreground task runs, and the
child stopping is the child's own job-control concern. An explicit
`kill -TSTP <don-pid>` could still stop don, but silently swallowing that would
be surprising and is out of scope for terminal job control.

### Other background-pgrp paths (audited)

- **Non-TUI / pipe mode** uses `TerminalCoordinator::detached()`, but
  `spawn_foreground_process` still runs `TerminalGuard`, so the guard covers
  this path too. (`TerminalGuard::enter` requires an interactive stdin, so pure
  pipe mode never reaches the foreground path.)
- **`don attach`** reads from a unix-socket upgrade on the daemon side; the tty
  reader is the separate `don attach` client process, which is itself a normal
  foreground process. The daemon never reads its controlling tty for attach.

## The foreground child inherits the ignored dispositions (on purpose)

`spawn_foreground_process`'s child `pre_exec` puts the child in its own process
group (`setpgid(0, 0)`) but deliberately does **not** touch `SIGTTIN`/`SIGTTOU`.
The child is forked while `TerminalGuard` holds the `JobControlStopGuard`, so it
inherits don's `SIG_IGN` for both — and an ignored disposition survives `execve`
(exec resets *caught* handlers to `SIG_DFL` but leaves `SIG_IGN` in place). The
child therefore starts life with both signals ignored.

**Why this is protective, not a leak.** There is a race window between the
child's `setpgid` (it is now a **background** pgrp for the tty) and don's
`make_foreground`/`tcsetpgrp` on the parent side (which promotes that pgrp to the
terminal's foreground). A child that reads the tty inside that window — an
interactive shell grabbing input the instant it starts is the common case —
would, with `SIG_DFL`, take `SIGTTIN` and **STOP** before it was ever
foregrounded, hanging the task the same way a stopped daemon does. Inheriting
`SIG_IGN` turns that read into a bounded `EIO` instead; once `tcsetpgrp` lands,
the child is foreground and tty reads behave normally.

**The visible consequence.** A *non-interactive* shell-script foreground task
(`cmd = "bash deploy.sh"`) does not reset its signal dispositions, so it passes
the inherited `SIG_IGN` down to *its* children. If one of those grandchildren
does a background tty read it gets `EIO` rather than a job-control stop, which
can surprise programs that rely on being stopped and continued. *Interactive*
shells are unaffected: they install their own job control at startup and reset
`SIGTTIN`/`SIGTTOU` to `SIG_DFL` for themselves and their children.

**Trap for future maintainers.** Do **not** "clean this up" by resetting
`SIGTTIN`/`SIGTTOU` to `SIG_DFL` in the foreground child's `pre_exec` — that
reintroduces the race-window stop and the hang it causes. If child job-control
purity is ever genuinely required, the correct pattern is *tcsetpgrp-self, then
reset*: after `setpgid(0, 0)`, have the child itself `tcsetpgrp` its own group to
the terminal's foreground **before** restoring `SIG_DFL`, so it is never a
background reader running the default disposition. Background **service**
children are the opposite case and *do* reset to `SIG_DFL` in their `pre_exec`
(see `spawn_pty`/`spawn_pipe`): they run under `setsid`/their own pgrp and never
own don's controlling tty, so a concurrent foreground window's `SIG_IGN` leaking
into them through fork is a real bug rather than a safeguard.

## Manual verification recipe

An automated, faithful tty job-control test is not shipped: it needs a
controlling-terminal + process-group setup that is inherently flaky in CI (and
can pass for the wrong reason), and `fork`-without-`exec` inside the
multithreaded test binary is unsafe. The `JobControlStopGuard` RAII contract is
unit-tested in `src/process/mod.rs`; the end-to-end behavior is verified
manually:

1. Add a foreground task that takes the terminal for a while, e.g. in a scratch
   `don.toml`:

   ```toml
   [tasks.shell]
   cmd = "bash"
   terminal = { mode = "foreground" }
   ```

2. `cargo build --release && ./target/release/don start` in a **real terminal**
   (not a pipe).
3. Launch the foreground task from the TUI (Tasks table → `shell`). The child
   `bash` takes over the terminal; don is now backgrounded.
4. In the `bash`, mash keys / paste a blob (exercise the tty read path), then
   run something slow (`bazel build ...` or `sleep 30`), then `exit`.
5. In another terminal, confirm don never enters state `T` during the window:

   ```sh
   watch -n0.2 'ps -o pid,stat,comm -p $(pgrep -x don)'
   ```

   Before the fix, `STAT` flips to `T` (stopped) and the task stays `running`
   after `bash` exits. After the fix, `STAT` stays `S`/`R`, the terminal is
   handed back cleanly, and the task transitions to its final state.
