# don

Boss of your dev environment. One command starts your whole stack — and shuts it down clean when you're done.

```sh
don start
```

Don reads a `don.toml` in your project root and orchestrates your entire dev stack: databases, API servers, background workers, migration tasks, file watchers — all with dependency ordering, ready checks, and color-coded output in a single terminal. No loose ends.

## Install

```sh
# From source (requires Zig 0.15.2 — see below)
cargo install --path .

# Or via Homebrew
brew install pjtatlow/tap/don
```

Building from source requires **Zig 0.15.2** on your `PATH` in addition to
the Rust toolchain: don embeds ghostty's terminal-emulation core
([`libghostty-vt`](https://crates.io/crates/libghostty-vt)) for server-side
attach screens, and its build compiles ghostty with `zig build` — there are
no prebuilt libraries. Ghostty pins the Zig version exactly, so a newer Zig
will refuse to build. Grab the matching tarball from
[ziglang.org/download](https://ziglang.org/download/). Prebuilt binaries
(Homebrew, GitHub releases) don't need Zig.

## Quick Start

Run `don init` to drop a starter `don.toml` in the current directory, or create one by hand:

```toml
[services.postgres]
docker.image = "postgres:16"
docker.ports = ["5432:5432"]
docker.env_file = [".env"]
ready.tcp = "127.0.0.1:5432"

[tasks.migrate]
cmd = "dbmate"
args = ["up"]
depends_on = ["postgres"]

[services.api]
run.cmd = "cargo"
run.args = ["run", "--bin", "api"]
depends_on = ["migrate"]
watch = ["src/**/*.rs", "Cargo.toml"]
env = { DATABASE_URL = "postgres://localhost:5432/myapp" }
ready.http = "http://localhost:3000/health"

[services.worker]
run.cmd = "cargo"
run.args = ["run", "--bin", "worker"]
depends_on = ["migrate"]
watch = ["src/**/*.rs", "Cargo.toml"]
```

Run it:

```sh
don start
```

Don will:
1. Start postgres (docker)
2. Wait for it to accept connections (TCP ready check)
3. Run migrations
4. Start api and worker in parallel (both depend on migrate)
5. Watch for file changes and rebuild/restart automatically

## Features

### Interactive TUI

When stdout is a TTY, `don start` runs a ratatui-driven full-screen interface: a
scrollable log pane, an optional panel beside it, and a bordered bar along
the bottom with ready counts, running tasks, a spinner during transitions and
contextual key hints.

Don keeps its own copy of every log line — the same stream, with the same line
numbering, that `don logs` and the web UI read — so filtering only changes what
is *shown*. Widening a filter reveals history that was there all along, and
nothing is lost by opening an overlay.

| Key | Action |
|-----|--------|
| `↑` `↓` `PgUp` `PgDn` `Home` | Scroll the log |
| `End` or `Enter` | Jump back to following the live tail |
| wheel | Scroll the log |
| drag | Select text. Double-click takes a word, triple-click the message |
| `y` | Copy the selection, without the `name \| ` prefix |
| `Esc` | Clear the selection |
| `j` `k` `g g` `G` | Vim's verticals: line down, line up, top, live tail |
| `s` `t` `f` | Open the services, tasks or filter panel — each key also closes its own |
| `Tab` | Move focus between the log and the panel |
| `P` | Move the panel between right and bottom |
| Ctrl+`←` `→` | Resize the split. Grows whichever pane has focus |
| `l` | In a table: narrow the log to the highlighted process (`Esc` restores the filter) |
| `a` | In a table: attach to the highlighted process, in a window |
| `enter` `r` `R` | In a table: run or start/stop, restart, hard restart |
| Ctrl+L | Repaint, if something else has scribbled on the screen |
| Ctrl+D | Detach, leaving the stack running (`don attach` only) |
| Ctrl+C | Graceful shutdown (second press force-kills) |

A selection is a place in the log rather than a place on the screen — a line
and an offset into it — so it stays on the text you dragged across while you
scroll, resize the terminal, or change the filter. Dragging holds the view
still as well, so output arriving mid-drag doesn't pull the text out from under
you; the log resumes following when you clear the selection. Copying is
deliberate — `y`, never a side effect of releasing the mouse.

Copying goes through OSC 52, so it reaches your system clipboard over ssh and
inside tmux. Terminals with OSC 52 disabled ignore it silently — the status bar
reports what was sent, which is the only acknowledgement the protocol allows.

⌘ is not available to a terminal application: it has no encoding in the
traditional input stream, and macOS terminals claim it for their own shortcuts
before an application sees it. To use ⌘C, hold **Shift while dragging** — that
bypasses don's mouse capture and gives you the terminal's own selection.

Pipe mode (non-TTY) writes prefixed lines directly to stdout unchanged.

Pass `--log-filter=<name1,name2,...>` to scope visible output to a subset of
services or tasks — useful in pipe mode (CI, log capture) and as a way to
seed the TUI filter from the command line, overriding any `hidden = true`
defaults. `[don]` lifecycle events stay visible regardless of the filter.
Ring buffers and file sinks are unaffected, so `don logs <name>` still
returns the full output for filtered services.

Use `log_filter` to keep only matching log lines before any output routing.
Top-level `log_filter` applies globally; service-level `log_filter` adds
service-specific regexes.

```toml
log_filter = ["ERROR", "WARN"]

[services.api]
log_filter = ["request_id=abc", "^database:"]
```

### Services

Long-running processes (servers, databases, workers). Don keeps them alive and restarts them on file changes.

```toml
[services.api]
run.cmd = "node"
run.args = ["server.js"]
env = { PORT = "3000" }
watch = ["src/**/*.js"]
ready.http = "http://localhost:3000/health"
shutdown.signal = "SIGTERM"
shutdown.timeout = "5s"
```

Set `reload = false` to opt out of Don-managed file watching for a service — useful for services that handle their own hot-reloading (vite, webpack dev server, etc.):

```toml
[services.web]
run.cmd = "npm"
run.args = ["run", "dev"]
reload = false   # no watch registration, rebuilds, or restarts for this service
```

Bazel services/tasks also have a nested `watch` flag. It is narrower: it only disables auto-resolved build-tool watch paths while still using Bazel for startup builds. Explicit service `watch = [...]` patterns still work unless `reload = false` is set.

```toml
[services.api]
bazel.target = "//services/api:api"
bazel.watch = false  # disable bazel query-derived watches only
watch = ["services/api/**/*.py"]
```

### Tasks

One-shot commands that run to completion (migrations, codegen, seeding). Only re-run when watched files change.

```toml
[tasks.migrate]
cmd = "dbmate"
args = ["up"]
depends_on = ["postgres"]
watch = ["db/migrations/**/*.sql"]
```

Set `auto_run = false` to defer execution — when the task actually needs to run (because watched inputs changed, or another item depends on it), it moves to `pending_run` until you explicitly trigger it:

```toml
[tasks.seed]
cmd = "./scripts/seed-db"
auto_run = false
depends_on = ["migrate"]
```

Run deferred tasks with `don run <name>`, or from the TUI's task view (`t`).

Set `auto_run = "once"` to run a task automatically on startup until it has
one successful run, then require manual triggers forever after:

```toml
[tasks.bootstrap]
cmd = "./scripts/bootstrap-db"
auto_run = "once"
depends_on = ["postgres"]
```

Some tasks wait for a human: REPLs, editors, interactive migrations, test
watchers, or anything that prompts on stdin. Say so:

```toml
[tasks.console]
cmd = "rails"
args = ["console"]
depends_on = ["postgres"]
interactive = true
```

Every task already runs on a PTY and any task can be reached with `don attach
<task>`, so this doesn't change how the task runs. It changes who has to do
something about it. In the TUI, an interactive task **opens its own attach
window** when it starts and closes it again when it succeeds — you answer the
prompt where you were already looking. If it fails, the window stays up, dimmed
and titled with the outcome, so its last screen is still there to read; any key
dismisses it. Outside the TUI there is no client to attach, so Don says
`waiting for input — run 'don attach console'` instead, because a task blocked
on input is otherwise indistinguishable from one that has hung.

`terminal = "foreground"` is the older spelling of `interactive = true` and
still works. It named a mechanism Don no longer has — a task taking the
terminal away from everything else — but what it was *for* outlived the
mechanism, so a config that still says it keeps working. `terminal = "muxed"`
is the ordinary case and means `interactive = false`.

A process that takes the terminal's **alternate screen** — vim, htop, lazygit,
anything full-screen — is detected regardless of how it's configured. Its
frames can't be rendered in a multiplexed line-oriented view, so Don writes one
line and stops:

```
console | entered full-screen mode — run 'don attach console' to see it
```

Output resumes when the process hands the screen back. Nothing is lost: `don
logs` and `don attach` are fed upstream of this and see every byte.

Tasks can also declare parameters. Parametrized tasks are interactive: values are supplied at run time via `don run <task> --<name>=<value>` or the TUI form, then substituted into `cmd`, `args`, `env`, and `dir` via `{{name}}` placeholders.

```toml
[tasks.sync]
cmd = "sh"
args = ["-c", "echo index={{index}} batch={{batch_size}} dry_run=$DON_PARAM_DRY_RUN"]
auto_run = false

[[tasks.sync.params]]
name = "index"
required = true

[[tasks.sync.params]]
name = "batch_size"
kind = "int"
default = "100"
validate = { min = 1, max = 10000 }

[[tasks.sync.params]]
name = "dry_run"
kind = "bool"
default = "false"
```

Run it with:

```sh
don run sync --index=users --batch_size=500 --dry_run
don run sync --wait --timeout=30s --index=users
```

Accepted CLI forms are `--name=value`, `--name value`, and bare `--flag` for bool params. Add `--wait` to block until the task exits; `--timeout=<duration>` sets a maximum wait time, implies `--wait`, and stops waiting without stopping the task. Param names cannot collide with built-in `don run` flags such as `wait` and `timeout`. Task params are also exported to the child process as `DON_PARAM_<NAME>` environment variables.

For fixed or dynamic candidate values, use `choices` or `completions`:

```toml
[[tasks.deploy.params]]
name = "environment"
choices = ["dev", "staging", "prod"]

[[tasks.deploy.params]]
name = "service"

[tasks.deploy.params.completions]
cmd = "./scripts/list-services"
args = ["--json"]
parse = "json"
cache = "5m"
```

### Dependency Graph

Services and tasks declare `depends_on`. Don topologically sorts them and starts everything in parallel, gating on ready checks:

```toml
[services.db]
# ...
ready.tcp = "127.0.0.1:5432"

[tasks.migrate]
depends_on = ["db"]

[services.api]
depends_on = ["migrate"]
```

#### Blocking vs. non-blocking dependencies

A plain string is a **blocking** dependency: it must become ready (services)
or complete (tasks) first, and if it fails the dependent is skipped with
`dep failed`.

Write an entry in table form with `blocking = false` to make it **ordering
only**. The dependent still waits for that dependency to settle — it never
starts before it — but a failure doesn't hold it back:

```toml
[services.api]
depends_on = [
  "postgres",                                    # blocking: api won't start without it
  { name = "otel-collector", blocking = false }, # nice to have: start anyway if it fails
]
```

Don logs `starting without non-blocking dependency 'otel-collector'` when it
makes that call, so a start that follows a visible failure is never a mystery.

Non-blocking edges still count for ordering, shutdown order, and profile
resolution. A group reference marked non-blocking makes every member of that
group non-blocking; if a name is reachable both ways, the blocking edge wins.

### Service Groups

Bundle related services under a name and reference the group from any
`depends_on`. Group references expand to every member, including nested
groups:

```toml
[service_groups]
datastores = ["postgres", "redis"]

[services.api]
depends_on = ["datastores"]   # waits for postgres AND redis
```

A group can also declare its own `depends_on`. Those dependencies are
**additive** — they apply to every (transitive) member of the group, on
top of whatever each member already declares:

```toml
[service_groups.frontend]
members = ["web", "admin"]
depends_on = ["api"]          # both web and admin now depend on api
```

Group-level `depends_on` may reference services, tasks, or other groups,
and propagates through nested groups (a member of a member group still
inherits the outer group's deps).

### Ready Checks

Don waits for services to be ready before starting dependents:

- **TCP**: `ready.tcp = "127.0.0.1:5432"` — connects to a port
- **HTTP**: `ready.http = "http://localhost:3000/health"` — expects 2xx
- **Exec**: `ready.exec = { cmd = "pg_isready" }` — expects exit code 0

```toml
ready.interval = "500ms"   # how often to check (default: 1s)
ready.retries = 30         # max attempts (default: 30)
```

### Health Monitoring & Auto-Restart

Set `ready.monitor = true` to keep polling the ready check after startup. Consecutive failures mark the service `unhealthy`; `on_failure` decides what happens next:

```toml
[services.api]
run.cmd = "./api-server"
ready.http = "http://localhost:3000/health"
ready.monitor = true              # keep checking after Ready
ready.monitor_interval = "2s"     # interval while monitoring (default: ready.interval)
ready.unhealthy_after = 3         # consecutive failures → Unhealthy (default: 3)
on_failure = "restart"            # "notify" (default) or "restart"
```

`on_failure` also fires when the process exits with a non-zero status (or terminating signal) — clean exits still transition to `stopped`. Restarts use escalating backoff (1, 2, 4, 8, 16, 32, capped at 60s) and reset when the service recovers to Ready.

### File Watching

Services with `watch` patterns automatically rebuild and restart on changes. `watch` is always a list of glob strings, not a boolean:

```toml
[services.api]
watch = ["src/**/*.rs", "Cargo.toml"]
ignore = ["src/generated/**"]
debounce = "500ms"   # default: 200ms
build.cmd = "cargo"
build.args = ["build", "--bin", "api"]
```

Rust and Go presets get default watch patterns when `watch` is omitted. Docker and custom `run` services only watch files when `watch = [...]` is set.

Use `reload = false` as the service-level master switch when Don should not watch, rebuild, or restart that service at all:

```toml
[services.web]
run.cmd = "npm"
run.args = ["run", "dev"]
reload = false
```

Bazel has a second, narrower switch: `bazel.watch = false` disables build-tool-resolved watch paths, but does not disable explicit service `watch = [...]` patterns.

### Docker Services

Run containers alongside native processes:

```toml
[services.postgres]
docker.image = "postgres:16"
docker.ports = ["5432:5432"]
docker.volumes = ["pgdata:/var/lib/postgresql/data"]
docker.env_file = [".env"]
```

When `fallback_ports = true`, generated container names include a hash of the
project checkout so the same config can run from multiple worktrees. An
explicit `docker.container` value is preserved exactly and can still collide
across worktrees.

Build from a Dockerfile:

```toml
[services.api]
docker.image = "my-api:dev"
docker.build.context = "."
docker.build.dockerfile = "Dockerfile.dev"
docker.ports = ["3000:3000"]
```

### Presets

Built-in support for Rust and Go with automatic build commands and default watch patterns:

```toml
# Rust — runs `cargo build --bin api`, watches src/**/*.rs
[services.api]
rust.binary = "api"
rust.features = ["dev"]
# `$CARGO_TARGET_DIR` is honoured. Set `rust.target_dir` to override it, or if
# your target directory comes from `build.target-dir` in .cargo/config.toml,
# which Don doesn't read.

# Go — runs `go build -o .don/bin/api ./cmd/api`, watches **/*.go
[services.api]
go.package = "./cmd/api"
go.ldflags = "-X main.version=dev"
```

### Downloads

Fetch, verify, and cache binary artifacts per-platform:

```toml
[services.crdb]
run.cmd = "cockroach"
run.args = ["start-single-node", "--insecure"]

[services.crdb.download.platform.linux-x86_64]
url = "https://binaries.cockroachdb.com/cockroach-v25.4.0.linux-amd64.tgz"
sha256 = "c07247f245426f6d94e2f901f848946fa50d179cd8409422608805475bc95c51"
path = "cockroach-v25.4.0.linux-amd64/cockroach"
```

Cached in `.don/cache/`, symlinked to `.don/bin/`, and added to child PATH.

### Bazel Integration

Point a service at a Bazel target and Don handles everything — build, run, watch, and rebuild:

```toml
[services.api]
bazel.target = "//services/api:api"
proxy = { listen = "127.0.0.1:8080", env = "PORT" }
```

Don will:
1. Query `bazel query` to discover source packages → auto-set watch patterns
2. Run `bazel build` at startup (batched across all targets)
3. Resolve the output binary via `bazel cquery` and run it directly
4. Watch for source changes and rebuild/restart automatically
5. Watch BUILD files and re-query the build graph when they change

Multiple services sharing the same source files are batched into one `bazel build` invocation.

#### Build flags: use `.bazelrc`

Don passes `--curses=no` and `--color=yes` so it can read build output as
lines, and otherwise leaves the command alone — a plain `build --flag` line in
your workspace `.bazelrc` already applies to the builds Don runs.

When you want a flag only when *Don* builds, and not when you run `bazel build`
yourself, name a configuration and point Don at it:

```
# .bazelrc
build:don --noshow_progress
```

```toml
[bazel]
config = "don"          # every Bazel build Don runs
```

Individual services and tasks may name a different one, and targets built under
different configurations are built separately — one `bazel build` takes one
`--config`:

```toml
[services.api]
bazel.target = "//services/api:api"
bazel.config = "api-dev"
```

Don passes `--config` after its own two flags, so a configuration that sets
`--curses` or `--color` wins. Naming a configuration that no `.rc` file defines
is a hard Bazel error, which is why there is no default.

Set `bazel.watch = false` to keep Bazel startup builds/runs but skip Bazel-derived watch paths. This is useful when Bazel queries are too broad or too expensive and you want explicit watch globs instead:

```toml
[services.api]
bazel.target = "//services/api:api"
bazel.watch = false
watch = ["services/api/**/*.py", "libs/common/**/*.py"]
```

### TCP Proxy

Don listens on a port and forwards connections to the service on an ephemeral port. The proxy stays open across restarts — no dropped connections:

```toml
[services.api]
run.cmd = "./api-server"
proxy = { listen = "127.0.0.1:3000", env = "PORT" }
```

Don injects `PORT=<ephemeral>` into the service's environment. On restart, the proxy queues new connections while the service restarts. If the service has failed *and* its process is gone — a crash, or a blocking dependency that failed — queuing would just leave clients hanging, so Don refuses connections instead: queued and new connections are closed immediately until the service recovers. A service that failed but is **still running** (a failing ready check under the default `on_failure = "notify"`) keeps being served — a misconfigured health probe shouldn't take your app down. Both proxy modes behave the same way; `don status --verbose` marks a refusing listener `refusing (service failed)`.

Supports multiple proxy entries and lazy start (delay service startup until first connection):

```toml
[services.api]
proxy = { listen = "127.0.0.1:3000", env = "PORT" }
lazy = true
```

A lazy service with `depends_on` moves from `lazy` to `pending` on its first connection and won't start until those dependencies are ready. While it waits, the browser tab shows nothing — the connection just sits queued. Don logs `waiting for dependencies before start` while deferred, and `don status` reports `pending`. If a blocking dependency has failed, the service goes to `dep failed` and the proxy closes the connection rather than holding it.

#### Fallback Ports

Port fallback is opt-in. Keep the preferred ports in `don.toml` and set the
top-level `fallback_ports = true`. Don uses each configured proxy listener or
Docker host port when it is available; if another process already owns it, Don
asks the OS for an available port on the same host/IP.

```toml
fallback_ports = true

[services.api]
run.cmd = "./api-server"
proxy = { listen = "127.0.0.1:3000", env = "PORT" }

[services.database]
docker.image = "postgres:16"
docker.ports = ["127.0.0.1:5432:5432"]
```

This applies only to ports Don can see explicitly: `proxy.listen` and the host
side of `docker.ports`. Don does not choose ports by inferring them from
`env.PORT`, command arguments, or a fixed proxy `forward` target. TCP/HTTP
ready checks that target one of those explicitly configured public ports are
updated to the actual binding. A bind failure other than “address already in
use” remains an error. Without `fallback_ports = true`, conflicts retain the
fail-fast behavior.

Use host port `0` to request an OS-assigned port unconditionally, for example
`proxy = "127.0.0.1:0"` or `docker.ports = ["127.0.0.1:0:80"]`. Explicit port
`0` works whether or not fallback mode is enabled.

For an env-mode proxy, the configured env variable still names the private
backend port that the service must bind. Don separately injects the public
listener selected at runtime:

| Variable | Value |
|----------|-------|
| `DON_PUBLIC_PORT`, `DON_PUBLIC_ADDR` | First public listener |
| `DON_PUBLIC_PORT_0`, `DON_PUBLIC_ADDR_0` | Public listener by declaration index |
| `DON_PUBLIC_<NAME>` | Public port for proxy `env = "NAME"` |
| `DON_PUBLIC_<NAME>_PORT`, `DON_PUBLIC_<NAME>_ADDR` | Named public port/address |

`<NAME>` is uppercased and non-alphanumeric characters other than `_` are
removed. For example, `env = "CRDB_PORT"` exposes
`DON_PUBLIC_CRDB_PORT`, `DON_PUBLIC_CRDB_PORT_PORT`, and
`DON_PUBLIC_CRDB_PORT_ADDR`. Ready HTTP/TCP checks may use
`${DON_PUBLIC_PORT}`. The same runner-side `DON_PUBLIC_*` values are available
when evaluating Docker ready checks, but cannot be injected into a container
whose host port is assigned only after that container starts.

Dependent services and tasks can copy another service's public runtime port
into their own inline `env` values:

```toml
[services.web]
depends_on = ["database"]
env = {
  DATABASE_PORT = "$(database.port)",
  DATABASE_ADDR = "$(database.addr)",
  DATABASE_PORT_BY_CONTAINER = "$(database.PORT_5432)",
}

[tasks.migrate]
cmd = "dbmate"
depends_on = ["database"]
env = { DATABASE_PORT = "$(database.port)" }
```

`port` / `addr` select the first public binding. Indexed forms such as
`port_0` / `addr_0` select by declaration order; uppercase forms are aliases.
For env-mode proxies, the configured env name (for example
`$(api.PORT)`) selects that proxy's public port. Docker mappings can also be
selected by container port with `PORT_<container-port>` and
`ADDR_<container-port>` when that container port is unambiguous. Use `$$(...)`
for a literal `$(`. A referenced runtime service should be in `depends_on` so
its mapping exists before the consumer starts.

Run `don ports` to see configured and actual addresses, or `don ports --json`
to print the versioned `.don/ports.json` manifest. `don status -v` also shows
the actual proxy and Docker listener addresses.

```json
{
  "version": 1,
  "generated_at_unix_secs": 1770000000,
  "services": {
    "api": {
      "proxy": [{
        "configured_addr": "127.0.0.1:3000",
        "bound_addr": "127.0.0.1:49152",
        "mode": "env",
        "env": "PORT"
      }]
    },
    "database": {
      "docker": [{
        "configured": "127.0.0.1:5432:5432",
        "host_addr": "127.0.0.1:49153",
        "container_port": "5432",
        "protocol": "tcp"
      }]
    }
  }
}
```

### Socket Passing

Zero-downtime restarts via the systemd `LISTEN_FDS` protocol. Don binds the port and passes the socket fd to the child:

```toml
[services.api]
run.cmd = "./api-server"
proxy = { listen = "127.0.0.1:3000", listenfd = true }
watch = ["src/**/*.rs"]
```

During a file-watch restart, the port stays bound (connections queue in the
kernel backlog). `LISTEN_FDNAMES` contains the actual bound addresses, including
any fallback selected at startup.

### Profiles

Run a subset of services for focused work:

```toml
default_profile = "frontend"   # used by bare `don start` (optional)

[profiles.frontend]
services = ["api"]
tasks = ["migrate"]

[profiles.backend]
services = ["api", "worker"]
tasks = ["migrate"]
```

```sh
don start                       # uses default_profile, or everything if unset
don start --profile backend     # override
```

Transitive dependencies are included automatically — if `api` depends on `postgres`, it starts too.

### Config Auto-Reload

Edit `don.toml` while don is running. Don detects the change, diffs it, and applies it live:
- Added services start
- Removed services stop
- Changed services restart with the new config
- Invalid configs are rejected (old config continues)

### CLI Commands

```sh
don init                     # scaffold a starter don.toml
don start                    # start this project's stack (bare `don` prints help)
don start --profile <name>   # start a subset
don start <name>             # start a stopped service in the running daemon
don stop                     # stop this project's stack
don stop <name>              # stop a running service
don restart <name>           # restart a service
don status                   # show all services and their states
don status -v                # verbose: watch paths, ports, commands, build targets
don ports                    # show configured and actual runtime ports
don ports --json             # print the .don/ports.json manifest
don logs <name>              # view recent output
don logs <name> --follow     # stream output
don logs <name> --last 50    # last N lines
don attach                   # bring up the TUI over a running stack
don attach <name>            # attach stdin/stdout to one service
don run <name>               # run a specific task (bypasses auto_run)
don run <name> --wait        # run a task and wait for it to finish
don run <name> --timeout 30s # wait up to 30s without stopping the task
don exec <cmd> [args...]     # run a command with .don/bin on PATH
don validate                 # check config without starting
don cleanup                  # remove stale state from a crashed run
don cleanup --force          # kill a running daemon and clean up
don completions <shell>      # print a completion script for bash/zsh/fish/...

don ui                       # open the web UI in your browser
don daemon                   # run the system-wide daemon in the foreground
don daemon install           # install it as a user service (systemd/launchd)
don daemon status            # what's running, and which projects it can see
don daemon restart           # restart it, e.g. after upgrading don
don daemon stop              # stop it (your projects keep running)
don daemon uninstall         # remove the user service
```

Completions are dynamic: service, task, and profile names from your `don.toml` tab-complete on subcommands that take them (`stop`, `restart`, `run`, `logs`, `attach`). Install with e.g. `don completions bash > ~/.local/share/bash-completion/completions/don` or `don completions zsh > "${fpath[1]}/_don"`.


### Web UI

Don can serve a browser UI over every project you have running — service and
task states that update live, streaming logs with their colors intact, and the
same start/stop/restart/run controls the CLI has.

```sh
don daemon install     # once: run the daemon as a user service
don ui                 # open the UI
```

The daemon is a **broker, not an owner**. `don start` behaves exactly as it
always has — same terminal, same TUI, same process group — and additionally
tells the daemon where to find its socket. So:

- Stopping the daemon doesn't touch a single running service; you just lose the
  dashboard.
- A project whose daemon isn't running starts exactly as fast. Registration is
  best-effort and never blocks startup or Ctrl+C.
- Nothing is written into your project. Daemon state lives in
  `$XDG_STATE_HOME/don` (`~/Library/Application Support/don` on macOS), which is
  the only thing don ever writes outside a project's `.don/`.

Don't want a system-wide daemon? Serve a UI for one project from the process
that's already running it:

```sh
don start --with-ui          # port 3667; the daemon owns 3666
don start --with-ui=8100
don start --no-daemon        # no UI, and don't register with the daemon either
```

The web UI binds loopback only and doesn't authenticate: anything that can
reach the port is already running on your machine, and so can already do
anything don can. It does refuse requests whose `Host` isn't a loopback name,
which is what a DNS-rebound request from a page you merely visited looks like
— that blocks such a page from *reading* your logs and project paths, though
not from firing a blind request it can't see the result of. The port isn't
checked, so the UI works behind a reverse proxy.

### Daemon API

Don exposes a unix socket API at `.don/don.sock` for programmatic control:

```
GET  /ready                  → whether the initial startup sweep has settled
GET  /status                 → service/task states
GET  /status?verbose=true    → states plus actual proxy/Docker addresses, and task params
GET  /events                 → streaming NDJSON of state changes
POST /start/:name            → start a stopped service
POST /stop/:name             → stop a service
POST /restart/:name          → restart a service
POST /run/:name              → run a specific task (body: {"params": {...}, "wait": true})
GET  /logs/:name?last=N      → ring buffer output
GET  /logs/:name?follow=true → streaming NDJSON
GET  /attach/:name           → raw-stream attach (stdin/stdout)
```

The web UI is built on exactly these endpoints — it holds no logic of its own,
so it can't drift from what the CLI does.

### Terminal Safety

Service output is sanitized before display — colors and text styles pass through, but cursor movement, screen clearing, and alternate screen mode are stripped. Rogue ncurses apps can't corrupt don's terminal.

### Graceful Shutdown

- First Ctrl+C: graceful shutdown in reverse dependency order (dependents stop first), respecting per-service `shutdown.signal` and `shutdown.timeout`
- Second Ctrl+C: immediate SIGKILL on all processes
- Running tasks are killed
- PID files, sockets, and docker containers are cleaned up

### Crash Recovery

Managed service crashes (non-zero exits or terminating signals) route through `on_failure`: `"notify"` marks the service failed and emits a lifecycle event; `"restart"` reuses the same backoff machinery as monitor failures.

If don itself crashes, the next `don start` automatically:
- Detects orphaned service processes via `(pgid, start_time)` identity
- Kills confirmed orphans (safe against PID recycling)
- Removes stale PID files, sockets, and docker containers

## Configuration Reference

See [`examples/`](examples/) for complete working configs.

| Field | Type | Description |
|-------|------|-------------|
| `run.cmd` | string | Command to execute |
| `run.args` | [string] | Arguments |
| `dir` | string | Working directory |
| `env` | {key: value} | Environment variables. Values expand `${VAR}` against the inherited environment, env files, and Don-injected vars like `PORT` and `PWD` — but not against each other |
| `env_file` | [string] | Env files to load |
| `depends_on` | [string \| {name, blocking}] | Services/tasks to wait for. A string (or `blocking = true`) also gates on success; `blocking = false` orders startup only |
| `watch` | [string] | Glob patterns to watch for changes; not a boolean |
| `ignore` | [string] | Glob patterns to exclude from watch |
| `debounce` | string | Debounce duration ("200ms", "1s") |
| `ready.tcp` | string | TCP ready check address |
| `ready.http` | string | HTTP ready check URL |
| `ready.exec` | {cmd, args} | Exec ready check command |
| `ready.interval` | string | Check interval (default: "1s") |
| `ready.retries` | u32 | Max attempts (default: 30) |
| `ready.monitor` | bool | Keep polling after Ready to detect unhealthy (default: false) |
| `ready.monitor_interval` | string | Poll interval while monitoring (default: "10s") |
| `ready.unhealthy_after` | u32 | Consecutive monitor failures → Unhealthy (default: 3) |
| `on_failure` | string | `"notify"` or `"restart"` on crash/unhealthy (default: "notify") |
| `reload` | bool | Service-level master switch for Don-managed watches, rebuilds, and restarts (default: true) |
| `auto_run` | bool or string | (tasks) `true`/`"always"`, `false`/`"never"`, or `"once"` for startup-only until first success (default: true) |
| `interactive` | bool | (tasks) `true` when the task waits for a human at its terminal; the TUI opens its attach window (default: false) |
| `terminal` | string | (tasks) Older spelling of `interactive`: `"foreground"` = `true`, `"muxed"` = `false` |
| `params` | [[table]] | (tasks) Declare run-time parameters for interactive tasks |
| `params.name` | string | Parameter name, referenced as `{{name}}` and passed as `--name=value` |
| `params.prompt` | string | Optional prompt shown in the TUI form |
| `params.required` | bool | Require an explicit value unless `default` is set |
| `params.default` | string | Default value when the user omits the param |
| `params.kind` | string | `"string"`, `"int"`, `"bool"`, or `"choice"` |
| `params.choices` | [string] | Fixed candidate values; constrains the accepted set |
| `params.validate.min` | i64 | Minimum allowed value for `kind = "int"` |
| `params.validate.max` | i64 | Maximum allowed value for `kind = "int"` |
| `params.completions.cmd` | string | Command to resolve dynamic candidate values |
| `params.completions.args` | [string] | Arguments for the completions command |
| `params.completions.parse` | string | Parse mode: `"lines"`, `"null_separated"`, or `"json"` |
| `params.completions.cache` | string | Cache TTL for completion results |
| `params.completions.timeout` | string | Completion command timeout (default: `"10s"`) |
| `shutdown.signal` | string | Shutdown signal (default: "SIGTERM") |
| `shutdown.timeout` | string | Grace period (default: "10s") |
| `log` | string | Output routing: "stdout", "ignore", or a file path |
| `log_filter` | [string] | Regexes for service output lines to keep before routing |
| `docker.image` | string | Docker image |
| `docker.ports` | [string] | Port mappings |
| `docker.volumes` | [string] | Volume mounts |
| `docker.build` | table | Dockerfile build config |
| `rust.binary` | string | Rust binary target name |
| `go.package` | string | Go package path |
| `proxy` | string or table | TCP listener: `"addr"` (listenfd) or `{ listen, env/listenfd/forward }` |
| `lazy` | bool | Delay start until first proxy connection |
| `bazel.target` | string | Bazel target label (auto watch/build/run) |
| `bazel.watch` | bool | Auto-resolve Bazel watch paths from the build graph (default: true); does not disable explicit service `watch` |
| `bazel.config` | string | `.bazelrc` configuration to build this target under, as `--config=<name>` |
| `bazel.config` (top-level `[bazel]`) | string | Same, for every Bazel build Don runs; per-service/task settings override it |
| `download.platform.<platform>` | table | Per-platform download config |
| `default_profile` | string | Top-level: profile used by bare `don start` |
| `fallback_ports` | bool | Top-level: use an OS-assigned proxy/Docker host port when the preferred port is in use |

## Platform Support

Linux and macOS. Windows is not supported (relies on Unix sockets, process groups, signals, and `LISTEN_FDS`).

## License

MIT
