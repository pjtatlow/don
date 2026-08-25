# Don - Development Guidelines

## Project Overview

Don is a dev environment orchestrator. See `docs/design.md` for the full
design document, and `docs/ownership.md` for who owns what — the four actors,
the commands-down/reports-up invariant, and the module edges enforced by
`tests/module_edges_test.rs`. Read it before moving state between modules. The crate is both a library (`don`) and a CLI binary — the library exposes all core functionality so other Rust tools can embed it.

## Build & Test

```sh
npm --prefix web ci && npm --prefix web run build   # once after cloning
cargo build          # build
cargo test           # run all tests
cargo clippy         # lint
```

Building also requires **Zig 0.15.2** (exactly — ghostty pins it) on `PATH`:
`libghostty-vt-sys` compiles ghostty's VT core with `zig build` at build
time. Install from <https://ziglang.org/download/>; a version mismatch fails
the build with a clear message from ghostty's build script.

The web UI bundle is a build artifact and is **not** committed — it's
gitignored, built by CI, and shipped inside the published crate via the
`include` list in `Cargo.toml`. Build it once after cloning or the binary has
no UI (it will say so rather than serving a broken page), and the couple of
tests that assert on real assets skip themselves.

### Running don on don

There's a `don.toml` in this repo, so `don start` here brings up the dev loop:
a dev daemon rebuilt whenever `src/` changes, the Vite dev server proxying to
it, and a throwaway stack (`dev/demo/`) registered with the dev daemon so the
web UI has something to render. Clippy runs on every Rust save and the web
bundle is rebuilt on every frontend save, so the binary you build always has
a current UI.

```sh
don start                  # everything
don start --profile rust   # no npm: just the daemon and clippy
don ports                  # where things actually landed — open the `web` one
don run test               # the suite is manual — it'd fight the cargo lock
```

Several checkouts can run at once. `fallback_ports = true` gives the first
instance the memorable ports (3777 / 5173) and later ones whatever is free, and
the dev daemon's state lives in each checkout's own `.don/dev-daemon` rather
than the XDG location — a daemon installed with `don daemon install` holds an
flock on `daemon.pid` there, so sharing would make the dev daemon refuse to
start whenever the real one is running.

Both services sit behind Don proxies, so the address you opened survives the
daemon restarting under you on every save. That means the daemon's *own* port
is ephemeral and differs from the one you browse to — `don ports` is the source
of truth, and `$(daemon.addr)` is how the Vite service learns it.

For interactive testing of the TUI under realistic load (shutdown, log spam,
hang detection), see [Testing the TUI](#testing-the-tui) — this is the
preferred way to validate any change that touches `src/tui/`, `src/output/`,
or the runner shutdown path.

## Core Principles

### No Panics in Production Code

**No `unwrap()`, `expect()`, or `panic!()` outside of `#[cfg(test)]` blocks.** Don manages child processes and holds PID file locks. A panic means orphaned processes, stale sockets, and ports held hostage. Every fallible operation must use `Result` or `Option` with proper error propagation.

This includes:
- No `unwrap()` on `Option` or `Result` — use `?`, `ok_or()`, `map_err()`, etc.
- No array/slice indexing that could panic — use `.get()` and handle `None`
- No `unreachable!()` in match arms that could theoretically be reached
- `#[cfg(test)]` code and test helper functions may use `unwrap()` freely

### Developer Experience First

Don exists to make devs' lives easier. Every decision should optimize for the person running `don start` at 9am on Monday. Specifically:

- **Error messages must be actionable.** "service 'api': depends on unknown service 'postgre' — did you mean 'postgres'?" is better than "validation failed."
- **Fail fast, fail clearly.** Validate everything before starting anything. Don't start 5 services and then fail on the 6th because of a config typo.
- **Output must be scannable.** Service prefixes, aligned columns, lifecycle events in brackets. A dev glancing at the terminal should immediately know what's running, what failed, and why.
- **Respect the user's terminal.** Clean up on exit. Don't leave the terminal in a weird state. Handle Ctrl+C gracefully. Don't eat their scrollback with unnecessary output.

### Error Handling

Use `thiserror` for error types. Each module should define its own error enum. Errors should carry enough context to produce a good user-facing message.

```rust
// Good
Err(ConfigError::UnknownDependency { service: name.clone(), dependency: dep.clone() })

// Bad
Err("unknown dependency".into())
```

Avoid `anyhow` in the library — it erases types and makes it hard for consumers to match on errors. The binary can use `anyhow` at the top level if needed for convenience.

### Resource Safety

Don manages external resources (child processes, PID files, sockets, docker containers). All resource cleanup must go through structured ownership:

- Use RAII / `Drop` implementations for resources that need cleanup
- PID files must be cleaned up even if the operation that follows fails
- When spawning a child process, the PGID file lock must be acquired *before* the spawn — never after
- Signal handlers must be async-signal-safe — set a flag, let the main loop handle cleanup

### Shutdown Responsiveness

**The runner must stay interruptible at all times.** A user pressing `Ctrl+C` should never be trapped behind a slow build, download, query, or lock wait.

- Do not await long-running external work inline on the runner task unless it is explicitly raced against shutdown.
- Any subprocess awaited from the runner task must be cancellation-safe: dropping the future must stop the underlying work (`kill_on_drop`, abort handle, or equivalent).
- Any network/download future awaited from the runner task must be raced against shutdown so the runner can abandon it immediately.
- If a piece of work cannot be made cancellation-safe, it does not belong on the runner task. Move it to a detached task and communicate back via channels.
- New startup/rebuild/watch paths must answer this question in code review: "what happens if the user hits Ctrl+C right here?"

### Testing

- **Use table-driven tests.** Define a struct for test cases, put them in a `Vec`, iterate. This is the standard pattern in this codebase.
- **Tests may use `unwrap()`.** Panicking in tests is expected behavior.
- **Test the library, not the binary.** Business logic lives in `lib.rs` and its modules. The binary is a thin CLI wrapper.
- **Use temp directories for filesystem tests.** Clean up after yourself. Don't leave test artifacts in the working directory.
- **Integration tests live in `tests/`.** Use the helpers in `tests/helpers/` (TempDir, ConfigBuilder, free_port, run_with_timeout).
- **Tests must work without a real TTY.** PTY allocation can fail in CI/headless environments. Process spawning code must have a pipe-based fallback, and tests must not assume a PTY is available.
- **Every integration test gets a timeout.** Use `run_with_timeout()` to prevent hangs from blocking CI.

### Testing the TUI

Pipe-mode integration tests cover the runner and `OutputManager`, but they
don't exercise ratatui rendering, the input task, mouse handling, or the
interplay between those and the merged log stream. Several of the worst
regressions in this codebase have been TUI-only — pipe mode fine, TUI hangs /
loses lifecycle events / freezes under load. **If you touch `src/tui/`,
`src/output/`, or the runner's shutdown path, you need to validate against a
real PTY.**

**Assert on the rendered screen, never on the raw byte stream.** The TUI owns
the alternate screen, so its output is cursor moves and styled cells: a message
arrives split across escape sequences, a repainted region arrives twice, and a
line that scrolled away an hour ago is still in the byte stream. Grepping the
bytes gives both false negatives and false positives. The tools below render
the stream into a screen and assert on that.

There are five tools in `tools/`:

- **`tools/tui_emulator.py`** — the shared harness: a PTY session plus a
  terminal emulator (uses `pyte` when installed, falls back to a built-in
  subset of CUP/ED/EL/SGR/alt-screen otherwise). The other drivers import it.
- **`tools/tui_drive.py`** — runs `don start` under a real PTY, waits for
  "all services running" *on screen*, lingers, sends Ctrl+C, and checks that
  shutdown narrates itself, that don exits promptly, and that the alternate
  screen is handed back. Prints a structured summary on stderr and the raw
  stream on stdout.
- **`tools/tui_drive_resize.py`** — scrolls up out of follow mode, then
  resizes several times and checks the line at the top of the pane is still
  the same line. That is the property the scroll anchor in `src/tui/logs.rs`
  exists for: rows are a function of width, so a row-counted offset would move
  the view on every resize. Also budgets the bytes each resize costs, which
  catches a repaint turning into a history replay. Override the sequence with
  `DON_RESIZE_SIZES` (e.g. `"50x140,50x100"`).
- **`tools/tui_drive_logs.py`** — drives the log pane through everything that
  moves underneath it (output arriving, Enter's blank marks, the Ctrl+V record
  swap, scrolling out of follow, resizing) and checks after each step that the
  numbered lines on screen still ascend from top to bottom with no repeats.
  That one invariant catches the whole family of index-vs-pane drift bugs:
  duplicated rows, skipped rows, a pane that positions itself by one set of row
  counts and paints another. Writes its own `don.toml` if the project has none.
- **`tools/tui_drive_progress.py`** — fills the pane, stops the ordinary
  output, and leaves a service repainting a progress bar with `\r` frames of
  swinging length. Checks that no line on screen ever moves back *down*.
  Following the tail is bottom-anchored, so anything that changes the total row
  count moves everything above it: content arriving is supposed to (the log
  scrolls up), a line already laid out changing height is not. Samples across
  the repaint interval rather than in step with it — a probe that samples in
  lockstep reports a clean run against a binary that is visibly jumping.
  Writes its own `don.toml` if the project has none.
- **`tools/gen_stress_config.py`** — generates a synthetic `don.toml` plus
  per-service scripts mirroring a busy monorepo: hidden infra services, TERM-trap
  floods, dozens of lazy `app-NN` services with `listenfd` proxies. This is the
  load shape that exposes render-rate bugs.

The standard workflow:

```sh
# Build (release — debug is too slow to expose perf issues at scale).
cargo build --release

python3 tools/gen_stress_config.py /tmp/don-stress
rm -rf /tmp/don-stress/.don
python3 tools/tui_drive.py target/release/don /tmp/don-stress 4 \
    > /tmp/tui-stdout.bin 2> /tmp/tui-stderr.log
tail -15 /tmp/tui-stderr.log

rm -rf /tmp/don-stress/.don
python3 tools/tui_drive_resize.py target/release/don /tmp/don-stress

rm -rf /tmp/don-logview
python3 tools/tui_drive_logs.py target/release/don /tmp/don-logview

rm -rf /tmp/don-progress
python3 tools/tui_drive_progress.py target/release/don /tmp/don-progress
```

The stress config's proxies start at port 18000, which collides with plenty of
real projects. `DON_STRESS_PORT=29000` moves them.

**Read the screen only after `Session.settle()`.** A frame is one burst of
writes; sampling in the middle of one shows half the new screen over half the
old, which looks exactly like the rendering bugs these drivers exist to catch.

They each print `RESULT: ok` or `RESULT: FAIL` and exit accordingly. Things worth
reading in the summary:

- `reached 'all services running' on screen` — startup settled.
- `alternate screen entered` / `handed back` — a TUI that exits without
  restoring the main screen looks to the user like their scrollback vanished.
- `bytes written during 2s idle` — should be small and roughly constant. The
  loop marks state dirty and draws at most once per frame, so an idle screen
  costs almost nothing. Growth here means something is dirtying every tick.
- `captured bytes total` — a jump from tens of KB to megabytes without a config
  change means the TUI is repainting far more than it should.
- `don exit code: 0` and no `HANG` line.

For a quick look at what the TUI actually renders — for instance while working
on layout — `tools/tui_screen.py <binary> <dir> [linger] [keys]` prints the
screen, optionally after sending some keys:

```sh
python3 tools/tui_screen.py target/release/don /tmp/don-stress 4 "p"
```

When TUI behavior changes, these are the tests of record. A `cargo test` green
run is necessary but not sufficient — run the drivers and include before/after
summaries in the change description.

### Code Organization

```
src/
  lib.rs                    # library root — re-exports public API
  main.rs                   # CLI binary — thin wrapper around the library
  duration.rs               # human-readable duration string parsing ("200ms", "1s", "5m")
  config/
    mod.rs                  # Config struct, parsing, validation, FromStr, Levenshtein typo suggestions
    diff.rs                 # config diffing for live reload (added/removed/changed detection)
    service.rs              # Service, ServiceOverride, ResolvedService, presets
    task.rs                 # Task config
    profile.rs              # Profile config + profile → process-set resolution
    platform.rs             # Platform enum, deserialization
    download.rs             # DownloadConfig, PlatformDownload, cache paths
    types.rs                # Shared types: Command, ReadyCheck, ShutdownConfig, LogConfig
  command.rs                # CommandError / CommandResult — shared reply vocabulary (process + runner)
  control.rs                # ProcessControl / ProcessCatalog — what a client may ask a process to do
  gate.rs                   # per-process permission to run: the scheduler's whole output
  endpoints.rs              # where every service can be reached; supervisors render $(peer.KEY) from it
  process/
    mod.rs                  # per-process mechanism root; the edge rule: imports nothing from runner/
    registry.rs             # ProcessRegistry — addressed, send-only handles to supervisors
    service_supervisor.rs   # service lifecycle owner: spawn → ready → reap, stop, restart, backoff
    service_process.rs      # service spawn/stop mechanics (process + docker)
    service_worker.rs       # service start preparation (build, ports, env, proxy)
    task_supervisor.rs      # task run owner: prepare → wire → exit, mailbox supersession
    task_process.rs         # task execution: timeout, skip-if-unchanged
    task_worker.rs          # task run preparation (params, hashing, spawn)
    health.rs               # health monitor loop + RestartPolicy (backoff, crash ceilings)
    ready.rs                # ready-check resolution against live runtime ports
    params.rs               # task param value resolution
    env_refs.rs             # $(service.key) runtime env reference rendering
    state.rs                # ServiceState / TaskState machines, ProcessKind
    paths.rs                # watch-path staleness checks
  runner/
    mod.rs                  # scheduler — dependency graph, publishes gates, folds ProcessReports
    startup.rs              # publish_start_gates: who may run, and why not
  state_store.rs            # runner-written / world-readable state projection + ProcessStatus
  param_completions.rs      # task-param completion engine: cache, shell-out, parse, failure log
  sys/
    mod.rs                  # process group management, PTY spawning, identity tracking
    pid_file.rs             # PID file locking (flock-based) for single-instance guard
    identity.rs             # (pgid, start_time) capture for crash-recovery identity checks
    cleanup.rs              # stale state detection and cleanup (pid files, sockets, docker)
    env.rs                  # .env file parsing, env merging
    socket.rs               # LISTEN_FDS socket binding and fd passing
  watch/
    mod.rs                  # file watching, debounce, change-during-build state machine, config reload
  output/
    mod.rs                  # line buffering, service name prefixing, color assignment, sink management
    attach.rs               # attach sessions over the live spawn's PTY gate; detach = guard drop
    actor.rs                # per-process output actor: ring buffer, sink fan-out, attach state
    ring_buffer.rs          # bounded per-service output buffer
    sanitize.rs             # ANSI escape sequence filtering (strip cursor/screen, keep colors)
  tui/
    mod.rs                  # the one select! loop: arms mark dirty, one rate-capped arm draws
    app.rs                  # all view state; nothing else mutates it
    render.rs               # one `draw` for the whole screen — log pane, status pane, bar, overlays
    logs.rs                 # the log view: wrapping, the (LogId, row) scroll anchor, follow mode
    log_store.rs            # this client's copy of the merged stream, parsed and measured once
    panes.rs                # pane rectangles, focus, divider drag — computed once, read by all
    selection.rs            # drag selection over rendered rows, and OSC 52 copy
    input.rs                # crossterm events → AppEvent; no interpretation
  client/
    mod.rs                  # HTTP-over-unix-socket client for CLI ↔ daemon communication
  server/
    mod.rs                  # unix socket HTTP API (axum over hyper-util)
    routes.rs               # API endpoints: status, start, stop, restart, logs (incl. follow)
  docker/
    mod.rs                  # Docker service lifecycle via bollard API
    build.rs                # Docker image building (tar context, streamed output)
    parse.rs                # Port mapping, env merging for docker
    stream.rs               # DockerLogReader: AsyncRead adapter over bollard log stream
  download.rs               # artifact downloading, SHA-256 verification, archive extraction, caching
  task_state.rs             # task file hash tracking for skip detection
```

Don't be afraid to create directories and nested modules. A flat list of 15 files in `src/` is harder to navigate than a well-organized tree. Group related functionality into directories with a `mod.rs` that exposes only what the rest of the crate needs. Each file should be small enough to read in one sitting.

### Modularity & API Surface

Lean hard towards small, focused modules. Each file should do one thing. If a file is getting long, split it. If a struct has methods that serve two different concerns, those concerns probably belong in separate modules.

**Public API discipline:**

- Default to `pub(crate)`. Only make something `pub` if it's part of the library's external API — something another Rust crate consuming `don` would need.
- Every `pub` item must have a doc comment explaining what it does, when to use it, and any important invariants.
- Re-export the public API from `lib.rs` so consumers get a clean `don::Config`, `don::runner::ProcessStatus`, etc. without reaching into submodules.
- Internal helpers, intermediate types, and implementation details stay `pub(crate)` or private.

**Module boundaries:**

- Modules should communicate through well-defined types, not by reaching into each other's internals.
- If module A needs something from module B, B should expose a clean function or type for it — not have A poke at B's fields directly.
- Prefer passing values and references over shared mutable state. When shared state is unavoidable, contain it in one module that owns the state and exposes an API for it.

### Cross-Module Communication

Modules communicate via **tokio channels**, not shared mutable state:

- **`mpsc`** for commands into the runner (CLI/API -> runner). The runner owns an `mpsc::Receiver<RunnerCommand>` and processes commands sequentially. This gives it a clean command loop with no shared mutable state.
- **`broadcast`** for events out of the runner (runner -> output/API/watch). Service state changes (started, ready, stopped, failed) are broadcast so multiple consumers can observe without coupling.
- **`oneshot`** for request/reply (e.g., verbose status). The API sends a command with a `oneshot::Sender` for the reply, the runner fills it.
**Two module edges are enforced by a test, not by convention:** `src/process/`
must not reference `crate::runner`, and neither must `src/tui/`. Both are load
bearing — the first is the direction of the whole supervisor design, the second
is what lets the TUI detach and reattach — and both erode by someone reaching
for a runner type that already has the fact they want.
`tests/module_edges_test.rs` fails the build if either does.

- **`watch`** for the projections out of the runner — state (`state_store.rs`), endpoints (`endpoints.rs`) and per-process run permission (`gate.rs`). Readers see the latest value, not every intermediate one.

**No `Arc<Mutex<_>>` for shared state.** The runner owns all *scheduling* state in plain `HashMap`s and is the only thing that may mutate it. This avoids deadlocks and contention. Per-process runtime state — the handle, the reader, the OSC sink, the proxy, the restart counters — belongs to that process's supervisor; the runner learns about it from reports and keeps no second copy.

**Reading that state does not go through the command channel.** A status query is the one thing that should never queue behind whatever the runner is currently doing. `state_store.rs` publishes a projection — every process's `ProcessStatus`, plus `startup_complete` — through a `watch`, and splits the handle by type so single-writer is enforced by ownership rather than by discipline:

- `StateWriter` is **not** `Clone` and is moved into the runner at construction, so no other component can obtain one.
- `StateReader` is `Clone` and exposes reads only. The server, the TUI and the web UI hold one.

`Arc<RwLock<State>>` was rejected for exactly this reason: it hands every holder a `.write()`.

Three rules for the projection:

- **It carries only what is cheap to recompute on every transition.** Verbose status stays a command — it needs a watch-manager round trip and a per-service ready-check resolution.
- **Reads return `Arc<StateSnapshot>`, never `watch::Ref`.** A `Ref` holds a read lock for as long as it lives; returning an `Arc` makes "held across an `.await`" unrepresentable rather than merely documented against.
- **For runtime detail, the snapshot is the record, not a copy of one.** A service's pid, docker-ness and port mappings live *only* in `ProcessStatus::Service.runtime`, written by the custody funnels from the supervisor's wired report. The runner keeps no shadow of them, which is why `publish_processes` **merges** (the fold rebuilds the list from scheduling state and knows nothing about pids) and why the patch methods **publish independently of state transitions** (a wire landing while a service is already `Running` changes no state, and `set_state` would no-op).

The state store and the `RunnerEvent` broadcast update on exactly the same transitions, which is what lets a consumer that missed an event resync from the snapshot and get a consistent answer. The TUI does this on `RecvError::Lagged`.

```rust
// The actual command enum (see runner/mod.rs for the full definition)
enum RunnerCommand {
    Start { name: String, reply: oneshot::Sender<CommandResult> },
    Stop { name: String, reply: oneshot::Sender<CommandResult> },
    Restart { name: String, reply: oneshot::Sender<CommandResult> },
    Rebuild { name: String },           // file watch triggered
    TaskRerun { name: String },         // file watch triggered
    Status { reply: oneshot::Sender<Vec<ProcessStatus>> },
    StartPending,                       // deferred retry for unsatisfied deps
    Shutdown,
}
```

### Async

The runtime is tokio. All I/O operations should be async. Avoid `block_on` inside async contexts. CPU-heavy work (like hashing files for task state) should use `tokio::task::spawn_blocking`.

### Dependencies

Be conservative with new dependencies. Before adding a crate, consider:
- Is it well-maintained? (check last publish date, open issues)
- Does it pull in a large dependency tree?
- Could we do this with std or an existing dependency?

Current dependency choices and rationale are documented in `docs/design.md`.

### Linting & Warnings

All code must pass `cargo clippy` with no warnings and compile with no warnings. Treat warnings as errors — don't leave `#[allow(unused)]` or dead code lying around. If something is temporarily unused during development, remove it or gate it behind a feature.

Formatting is `cargo fmt` with default settings — there is no `rustfmt.toml`
and no house deviation. CI runs `cargo fmt --check`, so run `cargo fmt` before
committing. The one-off reformat that established this is listed in
`.git-blame-ignore-revs`; enable it locally with
`git config blame.ignoreRevsFile .git-blame-ignore-revs`.

Specifically:
- `cargo clippy -- -D warnings` must pass
- `cargo fmt --check` must pass
- `cargo build 2>&1 | grep warning` must be empty
- No `#[allow(dead_code)]` or `#[allow(unused)]` in committed code
- Prefer explicit imports over glob imports (`use std::path::PathBuf`, not `use std::path::*`)

### Git

- Commit messages should be concise and describe *why*, not *what*
- One logical change per commit
- Keep the main branch clean — no broken builds

## Platform Support

Don targets Unix systems (Linux and macOS). Windows is not supported due to reliance on Unix sockets, process groups, signals, and `LISTEN_FDS`. Platform-specific code should use `cfg(target_os)` guards where needed.

## State Directory

All mutable *project* state goes under `.don/` in the project root. This directory must be in `.gitignore`. See `docs/design.md` for the full layout.

Never store project state outside `.don/` — a don-managed project should be fully self-contained and leave no footprint beyond this directory.

**The system-wide daemon is the one documented exception.** `don daemon` is not scoped to a project, so it has nowhere project-local to put its socket, project registry, and web-UI auth token. Those live in a single directory resolved by `src/daemon/paths.rs`: `$DON_STATE_DIR`, else `$XDG_STATE_HOME/don`, else `~/.local/state/don` (`~/Library/Application Support/don` on macOS). Nothing else may write outside `.don/`, and the daemon writes nothing into any project's directory.
