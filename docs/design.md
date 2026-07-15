# Don - Design Document

Don is a dev environment orchestrator. You write a `don.toml` config that defines your services, tasks, and their dependencies, and don runs everything for you.

## Architecture

Don is built as both a library (`don`) and a CLI binary. The library exposes all core functionality so other Rust tools can embed don's capabilities.

At runtime, don:

1. Parses `don.toml` (or a custom path via `-c`/`--config`)
2. Resolves platform-specific overrides for the current machine
3. Validates the config (preset conflicts, dependency references, ready check validity)
4. Builds a dependency graph across services and tasks
5. Downloads/extracts any required artifacts
6. Starts services and runs tasks in dependency order
7. Exposes an HTTP API over a Unix socket for CLI interaction with the running process

### Runtime

Don uses `tokio` as its async runtime. Key dependencies:

- **axum + hyper-util** — HTTP server over Unix sockets for CLI-to-daemon communication
- **notify** — file system watching for rebuild/restart triggers
- **sha2 + glob** — task state hashing to skip unchanged tasks
- **pty-process** — PTY allocation with native tokio async support (`AsyncRead`/`AsyncWrite`) so child processes see a real terminal (line-buffered output, colors)

#### Shutdown Responsiveness

Shutdown responsiveness is a hard architectural requirement:

- The runner task must never become uninterruptible while awaiting external work.
- Any slow subprocess, build-tool query, download, or lock wait that is awaited from the runner must race against shutdown.
- Dropping an in-flight future during shutdown must be enough to make progress toward exit. For subprocess-backed work that means `kill_on_drop`, an abort handle, or another explicit cancellation path.
- If work cannot satisfy that rule, it must run off the runner task and report completion back over channels.
- Graceful shutdown coordination may live in the runner, but process-exit authority must live outside it. `main` should be able to abort a wedged runner after a bounded grace period so `Ctrl+C` always still exits the program.

This is not an implementation detail. "Can the user still exit right here?" is part of the design contract for every new startup, rebuild, watch, and API control path.

## Config File (`don.toml`)

The config has `[services]`, `[tasks]`, `[service_groups]`, and `[profiles]`
sections, plus a few optional top-level keys such as `default_profile` and
`watch_ignore`.

Don also loads a sibling local override file when it exists. For the default
config path, that file is `don.local.toml`; for `--config workspace.toml`, it is
`workspace.local.toml`. The local file is intended for user-specific changes and
should not be committed. Tables are merged recursively, so a local file can add
new services, tasks, groups, or profiles and can override individual fields on
existing entries without repeating the whole entry. Scalars and arrays replace
the base value.

```toml
# Ignore generated and state files across every watcher in the workspace.
watch_ignore = ["target/**", ".don/**", "src/generated/**"]
```

```toml
# don.local.toml
default_profile = "pj"

[services.api]
dir = "./api-local"
env = { DATABASE_URL = "postgres://localhost:5433/dev" }

[services.scratch]
run.cmd = "node"
run.args = ["scratch.js"]

[profiles.pj]
services = ["api", "scratch"]
```

### Services

Services are long-running processes. Each service uses exactly one **preset** that determines how it's run:

#### Custom Preset

Run any binary with explicit command and args:

```toml
[services.worker]
run.cmd = "node"
run.args = ["worker.js"]
build.cmd = "npm"
build.args = ["run", "build"]
dir = "./worker"
watch = ["src/**/*.js"]
```

#### Docker Preset

Run a container, optionally building the image first:

```toml
[services.postgres]
docker.image = "postgres:16"
docker.container = "my-postgres"
docker.ports = ["5432:5432"]
docker.volumes = ["pgdata:/var/lib/postgresql/data"]
docker.network = "my-net"
docker.command = ["postgres", "-c", "max_connections=200"]
docker.env_file = [".env.postgres.docker"]
```

Docker build support for custom images:

```toml
[services.api]
docker.image = "myapp:dev"
docker.ports = ["3000:3000"]
docker.build.context = "./services/api"
docker.build.dockerfile = "Dockerfile.dev"
docker.build.target = "development"
docker.build.args = { RUST_VERSION = "1.80" }
```

#### Rust Preset

Build and run a Cargo binary:

```toml
[services.api]
rust.binary = "api-server"
rust.features = ["dev"]
rust.release = true
rust.extra_args = ["--jobs", "4"]
rust.target_dir = "./target-api"
```

### Common Service Fields

These fields are available on all service presets:

| Field | Type | Description |
|-------|------|-------------|
| `dir` | path | Working directory for the service |
| `env` | map | Environment variables |
| `env_file` | list of paths | Env files to load. Don also auto-loads `.env.<service-name>` if it exists |
| `watch` | list of globs | File patterns to watch for rebuilding/restarting |
| `debounce` | duration string | Debounce window for watch events (default "200ms") |
| `depends_on` | list of names | Services or tasks that must be ready/complete first |
| `listen` | list of addresses | Addresses for don to bind and pass to the service via `LISTEN_FDS` |
| `ready` | table | Ready check configuration (see below) |
| `shutdown` | table | Shutdown behavior (see below) |
| `log` | string or table | Logging output destination (see below) |
| `log_filter` | list of regex strings | Per-service regex keep filters for log lines |
| `download` | table | Binary download configuration (see below) |
| `on_failure` | string | `"notify"` or `"restart"`; controls service crash/unhealthy handling |
| `platform` | map | Per-platform overrides (see below) |

### Tasks

Tasks are one-shot commands that run to completion. They share the same dependency namespace as services — tasks can depend on services (waits for the service's ready check) and other tasks, and services can depend on tasks (waits for the task to complete).

```toml
[tasks.migrate]
cmd = "dbmate"
args = ["up"]
dir = "./db"
env = { DATABASE_URL = "postgres://localhost:5432/dev" }
depends_on = ["postgres"]
watch = ["db/migrations/**/*.sql"]
log = "ignore"
```

| Field | Type | Description |
|-------|------|-------------|
| `cmd` | string | The binary or program to execute |
| `args` | list | Arguments to pass |
| `dir` | path | Working directory |
| `env` | map | Environment variables |
| `depends_on` | list of names | Services or tasks that must be ready/complete first |
| `watch` | list of globs | File patterns — task only re-runs if these changed since last success. Empty = always runs |
| `auto_run` | bool or string | Automatic run policy; `"always-on-start"` runs every startup without trusting the saved watch hash and also on watched changes |
| `timeout` | duration string | Maximum time the task is allowed to run (e.g. "5m"). No timeout by default |
| `log` | string or table | Logging output destination |
| `terminal` | string or table | `"muxed"` (default) routes through Don output; `"foreground"` gives the task exclusive terminal ownership |
| `headless` | table | Optional `cmd` and/or `args` overrides for non-TUI runs |

Foreground terminal tasks are for interactive commands that need stdin and an
unprefixed terminal, such as REPLs, editors, interactive migrations, or test
watchers:

```toml
[tasks.console]
cmd = "rails"
args = ["console"]
terminal = "foreground"
```

`terminal = "foreground"` enters the terminal alternate screen by default.
Use a table to choose the main screen explicitly:

```toml
terminal = { mode = "foreground", screen = "main" }
```

During startup, a foreground task is exclusive. When it becomes ready, Don
does not start other newly-ready services/tasks until that task exits. Already
running dependencies continue to run, and their output is still captured in
ring buffers and log files while visible Don output is paused. Watch-triggered
foreground tasks may also steal the terminal during development.

Foreground tasks can provide a non-interactive command variant for
`--no-tui`, redirected-output, and detached runs. Fields omitted from
`headless` inherit their normal values, and headless execution uses muxed
output instead of foreground terminal ownership:

```toml
[tasks.push]
cmd = "scurry"
args = ["push"]
terminal = "foreground"
headless = { args = ["push", "--force"] }
```

#### Task State Tracking

Don tracks whether tasks need to re-run by hashing the contents of all files matching the `watch` patterns. The hash is stored in `.don/task-state/<task-name>.sha256`.

The hash covers:
- The sorted list of matched file paths (so adding or removing files triggers a re-run)
- The contents of each matched file

**The hash is only written after the task exits with code 0.** A failed task will always retry on the next run, even if the files haven't changed.

### Ready Checks

Ready checks gate dependents — a service's dependents won't start until the check passes. Exactly one check type must be set:

```toml
# Command check — exit code 0 means ready
[services.postgres.ready]
exec.cmd = "pg_isready"
exec.args = ["-h", "localhost"]
interval = "500ms"
retries = 60

# TCP check — successful connection means ready
[services.redis.ready]
tcp = "localhost:6379"

# HTTP check — 2xx response means ready
[services.api.ready]
http = "http://localhost:3000/healthz"
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `exec` | command | | Run a command, exit 0 = ready |
| `tcp` | string | | TCP connect succeeds = ready |
| `http` | string | | HTTP 2xx = ready |
| `interval` | duration string | `"1s"` | How often to check |
| `retries` | integer | `30` | How many times to retry before giving up |

### Shutdown

Controls graceful shutdown behavior. If a service doesn't exit within the timeout after receiving the signal, it gets `SIGKILL`.
Set a top-level `[shutdown]` table to change the default for every service.
Set `[services.<name>.shutdown]` to override that default for one service.

```toml
[shutdown]
graceful = true
signal = "SIGTERM"
timeout = "10s"

[services.api.shutdown]
signal = "SIGINT"
timeout = "30s"

[services.worker.shutdown]
graceful = false
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `graceful` | boolean | `true` | If `false`, skip the graceful window and send `SIGKILL` immediately |
| `signal` | string | `"SIGTERM"` | Signal to send for graceful shutdown |
| `timeout` | duration string | `"10s"` | Time to wait before SIGKILL |

### Logging

Controls where a service or task's stdout/stderr goes. Defaults to `"stdout"`.

```toml
# Default — print to don's stdout, prefixed with service name
log = "stdout"

# Discard all output
log = "ignore"

# Write to a file (string shorthand)
log = "logs/myservice.log"

# Write to a file (table form)
log.file = "logs/myservice.log"

# Keep only matching output lines before stdout/file/ring-buffer routing
log_filter = ["ERROR", "WARN"]

[services.api]
log_filter = ["request_id=abc", "^database:"]
```

Top-level `log_filter = [...]` applies to every service, task, and synthetic
build-tool stream. A service-level `log_filter = [...]` adds service-specific
keep patterns. Filters are line-based regexes; when any filter is configured
for a stream, only matching service output reaches stdout/TUI, file sinks,
`don logs`, and `don logs --follow`. Lifecycle events are not filtered by
these regexes.

### Downloads

Services can declare binary downloads with per-platform artifacts. Don downloads, verifies the SHA-256 hash, extracts archives, and optionally runs a setup command.

Downloaded artifacts are cached in `.don/cache/<sha256>/` (project-local).

```toml
[services.crdb]
run.cmd = "cockroach"
run.args = ["start-single-node", "--insecure"]

[services.crdb.download.platform.linux-x86_64]
url = "https://binaries.cockroachdb.com/cockroach-v24.1.0.linux-amd64.tgz"
sha256 = "abcdef1234567890..."
path = "cockroach-v24.1.0.linux-amd64/cockroach"

[services.crdb.download.platform.macos-aarch64]
url = "https://binaries.cockroachdb.com/cockroach-v24.1.0.darwin-arm64.tgz"
sha256 = "fedcba0987654321..."
path = "cockroach-v24.1.0.darwin-arm64/cockroach"
setup.cmd = "chmod"
setup.args = ["+x", "cockroach-v24.1.0.darwin-arm64/cockroach"]
```

| Field | Type | Description |
|-------|------|-------------|
| `url` | string | URL to download the artifact from |
| `sha256` | string | SHA-256 hash of the downloaded file for verification |
| `path` | string | Path to the binary inside the archive. If omitted, the downloaded file itself is the binary |
| `setup` | command | Optional command to run after download/extraction (cwd = cache dir). Only runs once; don writes a marker file after success |

When a download is configured, don resolves `run.cmd` to the full path inside the cache directory. If no download exists for the current platform, `run.cmd` is used as-is (resolved via `$PATH`).

#### Supported Platforms

Platform keys use Rust's `std::env::consts` naming:

- `linux-x86_64`
- `linux-aarch64`
- `macos-x86_64`
- `macos-aarch64`

### Platform Overrides

Any service field can be overridden per-platform, including switching the preset entirely (e.g., native binary on Linux, Docker on macOS):

```toml
[services.crdb]
run.cmd = "cockroach"
run.args = ["start-single-node", "--insecure"]
env = { COCKROACH_PORT = "26257" }

[services.crdb.download.platform.linux-x86_64]
url = "https://binaries.cockroachdb.com/cockroach-v24.1.0.linux-amd64.tgz"
sha256 = "abcdef1234567890..."
path = "cockroach-v24.1.0.linux-amd64/cockroach"

# On macOS, use Docker instead of a native binary
[services.crdb.platform.macos-aarch64]
docker.image = "cockroachdb/cockroach:v24.1.0"
docker.ports = ["26257:26257"]
```

Override merge rules:
- **`env`**: merged — override entries win on conflict, base entries are preserved
- **`watch`, `depends_on`, `listen`, `env_file`**: replaced entirely if set in the override
- **Preset fields** (`docker`, `rust`, `run`): if the override sets any preset field, all base preset fields are cleared and replaced
- **`build`, `dir`, `ready`, `shutdown`, `log`, `log_filter`, `download`**: override wins if set, otherwise base is kept

A service can have no base preset and define it entirely via platform overrides. Platforms without an override and no base preset will fail validation.

## Socket Passing (`listen`)

Don can own TCP sockets on behalf of services. When `listen` addresses are configured, don:

1. Binds the TCP sockets itself
2. Passes them to the child process as inherited file descriptors
3. Sets `LISTEN_FDS` and `LISTEN_FDNAMES` environment variables (systemd socket activation protocol)
4. Keeps the sockets open across service restarts so traffic is never dropped — the old process handles in-flight requests while the new one starts up

```toml
[services.api]
rust.binary = "api-server"
listen = ["0.0.0.0:3000", "0.0.0.0:3001"]
```

## Dependency Graph

Services and tasks share a single dependency namespace. The `depends_on` field accepts names of both services and tasks:

- A service depending on a **service**: waits for the dependency's ready check to pass
- A service depending on a **task**: waits for the task to complete successfully
- A task depending on a **service**: waits for the dependency's ready check to pass
- A task depending on a **task**: waits for the dependency to complete successfully

Names must be unique across services and tasks (validated).

You can also define named groups of services and reference those group names
from `depends_on`:

```toml
[service_groups]
datastores = ["postgres", "redis"]
backend = ["datastores", "api"]

[services.api]
rust.binary = "api-server"
depends_on = ["datastores"]
```

Depending on a service group is equivalent to depending on each member service.
Groups may include other groups; nested groups are expanded recursively and
cycles between groups are rejected during validation.

A group may also declare its own `depends_on`, which is **additive** on top
of each member's own dependencies. The group-level deps apply transitively
to every member, including members reached through nested groups:

```toml
[services.api]
rust.binary = "api"

[services.web]
rust.binary = "web"

[services.admin]
rust.binary = "admin"

[service_groups."web-stack"]
members = ["web", "admin"]

[service_groups.frontend]
members = ["web-stack"]
depends_on = ["api"]
```

Here both `web` and `admin` effectively depend on `api`. Group-level
`depends_on` may reference services, tasks, or other groups, just like a
service's own `depends_on`.

## Project-Local State

Don stores all mutable state under `.don/` in the project directory:

| Path | Purpose |
|------|---------|
| `.don/cache/<sha256>/` | Downloaded and extracted artifacts |
| `.don/task-state/<task-name>.sha256` | Task file hashes for skip detection |
| `.don/don.pid` | Don's own PID (for detecting stale instances) |
| `.don/don.sock` | Unix socket for CLI-to-daemon communication |
| `.don/pids/<service-name>` | PGID for each running service (for stale cleanup) |

This directory should be added to `.gitignore`.

## Environment Variables

Environment variables are resolved in this order (later wins):

1. `.env.<service-name>` — auto-loaded if the file exists (convention)
2. `env_file` — explicitly listed env files, loaded in order
3. `env` — inline variables from the config
4. Don-injected variables (`LISTEN_FDS`, `LISTEN_FDNAMES`, `DON_DOWNLOAD_DIR`, etc.)

## Terminal UI

When don runs in a terminal, all service and task output is multiplexed to stdout with a prefix identifying the source. Each service gets a color-coded name prefix so you can visually distinguish output from different services at a glance:

```
postgres | LOG:  database system is ready to accept connections
migrate  | Applying: 20240101_create_users.sql
migrate  | Applying: 20240201_add_email.sql
api      | listening on 0.0.0.0:3000
worker   | connected to queue
```

The prefix is padded to the length of the longest service/task name so the output columns align.

### Lifecycle Messages

Don prints its own status messages for lifecycle events, always prefixed with `[don]` to be visually distinct from service output. Service-specific events include the service name after the prefix.

**Startup:**

```
[don] loading don.toml
[don] validated 5 services, 2 tasks
[don] starting postgres...
[don] starting redis...
[don] postgres ready (tcp localhost:5432)
[don] redis ready (tcp localhost:6379)
[don] running migrate... (3 files changed)
[don] migrate complete (0.8s)
[don] running seed... (skipped, no changes)
[don] starting api...
[don] starting worker...
[don] api ready (http localhost:3000/healthz)
[don] worker started
[don] all services running
```

**Watch / rebuild:**

```
[don] api: file change detected (src/main.rs)
[don] api: building...
[don] api: build complete (2.1s)
[don] api: restarting...
[don] api: ready (http localhost:3000/healthz)
```

**Build failure:**

```
[don] api: file change detected (src/main.rs)
[don] api: building...
[don] api: build failed (exit code 1)
[don] api: keeping current process running
```

**Service crashes:**

```
[don] worker exited with code 1
[don] postgres exited with signal SIGKILL
```

**Tasks:**

```
[don] running migrate... (3 files changed)
[don] migrate complete (0.8s)
[don] running migrate... (skipped, no changes)
[don] migrate failed (exit code 1, 0.3s)
[don] migrate timed out after 5m
```

**Shutdown (with live remaining count):**

```
[don] shutting down gracefully... (Ctrl+C again to force)
[don] stopping worker... (4 remaining)
[don] worker stopped (3 remaining)
[don] stopping api... (3 remaining)
[don] api stopped (2 remaining)
[don] stopping migrate... (2 remaining)
[don] migrate stopped (1 remaining)
[don] stopping postgres... (1 remaining)
[don] postgres stopped (0 remaining)
[don] shutdown complete
```

**Shutdown with stuck process:**

```
[don] stopping api... (3 remaining)
[don] api: waiting for graceful shutdown (8s remaining)
[don] api: waiting for graceful shutdown (3s remaining)
[don] api: did not exit within 10s, sending SIGKILL
[don] api stopped (2 remaining)
```

**Config reload:**

```
[don] don.toml changed, reloading...
[don] added service: worker
[don] removed service: old-api
[don] restarting api (config changed)
```

**Stale cleanup:**

```
[don] cleaning up stale state...
[don] killed orphaned process group for api (pgid 12345)
[don] removed stale socket .don/don.sock
```

**Principles:**
- Always prefixed with `[don]` — visually distinct from service output
- Service-specific events include the service name: `[don] api: ...`
- Include timing where useful (build duration, task duration, shutdown countdown)
- Include the reason (which file changed, how many files, why skipped)
- Ready checks say what passed (tcp address, http endpoint)
- Shutdown shows a live count of remaining processes so it's clear if something is stuck
- Ring the terminal bell (`\x07`) on error events: build failures, service crashes, task failures, ready check exhaustion. This way the user gets an audible alert even if they're in another window.

### PTY Allocation

Each child process is spawned with its own pseudo-terminal (PTY). This is important because most programs change their buffering behavior based on whether stdout is a TTY:

- **Connected to a TTY** (what don does): line-buffered, ANSI color output enabled
- **Connected to a pipe** (what would happen without a PTY): fully-buffered (output sits in a buffer until it fills or the process exits), colors often disabled

By allocating a PTY, services behave exactly as they would in a real terminal — output appears line-by-line in real time, colors and progress bars work correctly, and there's no need for per-language hacks (`PYTHONUNBUFFERED`, `stdbuf`, etc.).

Stdout and stderr from each child are merged into a single stream — this matches how most programs interleave their error output with normal output, and avoids two separate prefixed streams per service fighting for the terminal.

Don reads from the PTY output, buffers per-line, applies the service name prefix, and writes to the appropriate destination (terminal, file, or discard). Per-line buffering ensures that concurrent output from multiple services never interleaves mid-line.

### Output Buffer

Don keeps an in-memory ring buffer of recent output per service (raw, without prefix). This buffer is exposed via the Unix socket API so that:

- `don logs api --last 100` can show recent output without scrolling the terminal
- AI agents or other tools can read service output programmatically via the API

The buffer size is bounded (e.g. last 10,000 lines or 1MB per service) to avoid unbounded memory growth.

### Log Routing

When `log` is set on a service:
- `"stdout"` (default) — output goes to the terminal with the service name prefix
- `"ignore"` — output is discarded, but lifecycle events still print. Output is still captured in the ring buffer.
- `"path/to/file.log"` — output is written to the file without the prefix (raw output), lifecycle events still print to the terminal. Output is still captured in the ring buffer.

`log_filter = [...]` is applied before all log routing. If any regex is
configured for a service or task, Don keeps only matching output lines in
stdout, files, follow streams, and the ring buffer.

### Build Failures

When a watched file changes and triggers a rebuild, but the build fails:

- The old process keeps running — don does not stop or restart it
- The build error is printed to the terminal
- Don stays in the "watching" state and will re-trigger the build on the next file change

This means a syntax error doesn't take down your running service.

### Service Crashes

If a service exits, don prints the exit code (or signal). By default
(`on_failure = "notify"`), it leaves the service stopped and the user can
manually restart it via the CLI.

```
[don] api exited with code 1
```

Set `on_failure = "restart"` to have Don restart crashed or unhealthy services
with exponential backoff: 1s, 2s, 4s, 8s, 16s, 32s, then 60s. If a service
fails three starts in a row without ever becoming ready, Don gives up and leaves
it Failed. The failure streak resets as soon as the service reaches Ready.

## Execution Model

### Dependency Resolution and Parallel Startup

Don topologically sorts the dependency graph across services and tasks. Everything whose dependencies are already satisfied starts concurrently — max parallelism by default.

If the dependency graph contains a cycle, don reports the cycle path and exits during validation (before starting anything).

### Startup Sequence

1. Validate config (presets, dependency references, cycles, ready checks)
2. Check for stale state and clean up (see below)
3. Warn if `.don/` is not in `.gitignore`
4. Pre-check declared ports for conflicts
5. Download/extract any required artifacts
6. Execute the dependency graph: start services and run tasks in topological order, with max parallelism

### Process Groups

Both services and tasks are spawned in their own process group via `setpgid(0, 0)`. This ensures that killing a process also kills any children it spawned (e.g. `make` spawning compiler processes).

**Services** get process groups plus PID file locking (see below) for:

- **Selective control** — restart or stop a single service (and all its child processes) without affecting others via `killpg(pgid, signal)`
- **Clean shutdown** — kill the entire process tree for a service, not just the top-level PID
- **Stale cleanup** — stored PGIDs let us find and kill orphaned process trees on next startup

**Tasks** get process groups but no PID file locking — they're short-lived, so stale cleanup isn't a concern. If a task is running during shutdown, don sends it `SIGTERM` via `killpg`, waits briefly, then `SIGKILL`s the group. If a task has a `timeout` configured and exceeds it, don kills its process group and treats it as a failure (the task state hash is not written, so it will retry next run).

On graceful shutdown (`SIGTERM`/`SIGINT` to don), don kills each service's process group in reverse dependency order. If don itself is `SIGKILL`'d, the service children become orphans — the stale cleanup on next startup handles this.

### PID File Locking

Each service gets a PID file at `.don/pids/<service-name>` that serves double duty: it stores the PGID **and** holds an `flock` for the lifetime of the process. This solves the PID recycling problem — a stale PGID might point to an unrelated process that happened to get the same ID, but the flock tells us definitively whether our process is still around.

**Service startup:**

1. Open `.don/pids/<service-name>` with `O_CREAT`
2. `flock(fd, LOCK_EX | LOCK_NB)` — if it fails, the service is still alive from a previous run
3. Spawn the service in its own process group
4. Write the PGID to the file
5. Hold the fd open for the lifetime of the service

**Stale cleanup** (on startup and via `don cleanup`):

1. Open `.don/pids/<service-name>`
2. `flock(fd, LOCK_EX | LOCK_NB)` — if it **succeeds**, the lock is stale: read the PGID, `killpg` it, delete the file
3. If the lock **fails**, the process is legitimately alive — skip

Don itself uses the same pattern with `.don/don.pid` to detect if another don instance is already running.

### Stale State Cleanup

If don crashes or gets killed, it can leave behind orphaned state. On startup (and via `don cleanup`), don checks for and cleans up:

- **Stale socket** — if `.don/don.sock` exists, don tries to connect. If it can't, the socket is stale and gets removed.
- **Don PID file** — `flock` on `.don/don.pid`. If the lock succeeds, no other don is running and stale resources can be cleaned up.
- **Orphaned process groups** — for each file in `.don/pids/`, attempt to acquire the flock. If it succeeds, the owning process is dead: read the PGID, `killpg` it, delete the file.
- **Docker containers** — for services with `docker.container` set, don checks if the container already exists and is orphaned from a previous run, and stops/removes it.

### Port Conflict Detection

Before starting services, don pre-checks for port conflicts:

- **`listen` addresses**: don binds these itself, so conflicts are caught immediately with a clear error.
- **Docker `ports` and other declared ports**: don does a quick TCP connect to check if the port is already in use and warns early instead of letting the service fail with a cryptic error.

### Config Auto-Reload

Don watches `don.toml` for changes while running. When the config file changes:

1. Parse and validate the new config
2. Diff against the running config
3. Stop services that were removed
4. Restart services whose config changed
5. Start newly added services (respecting dependency order)

If the new config is invalid, don logs the validation errors and keeps running with the current config.

### Gitignore Check

On startup, don checks whether `.don/` is covered by `.gitignore`. If not, it prints a one-time warning:

```
[don] warning: .don/ is not in .gitignore — consider adding it to avoid committing cache and state files
```

## Profiles

Profiles let you run a subset of your services. By default (no profile), don starts everything.

```toml
default_profile = "frontend"

[service_groups]
datastores = ["postgres", "redis"]

[profiles.frontend]
services = ["api", "postgres"]
tasks = ["migrate"]

[profiles.backend]
services = ["api", "worker", "datastores"]
tasks = ["migrate", "seed"]
```

```
don start                        # use default_profile (or everything if unset)
don start --profile frontend     # start only frontend profile
```

Profiles may list individual service names or service group names in
`services`. Services and tasks listed in a profile automatically include their
transitive dependencies. If `api` depends on `migrate` which depends on
`postgres`, listing just `api` is enough — don resolves the full chain.

Set `default_profile = "<name>"` at the top level to pick a profile automatically when `don start` is run without `--profile`. Leave it unset to have `don start` run everything.

## CLI

```
don [OPTIONS] [COMMAND]

Options:
  -c, --config <PATH>       Path to the config file [default: don.toml]
  -p, --profile <NAME>      Run only services/tasks in the named profile

Commands:
  init                      Scaffold a starter don.toml in the current directory
  start                     Start all services and run tasks
  restart <name>            Restart a specific service
  stop <name>               Stop a specific service
  status                    Show the status of all services and tasks
  logs <name>               Tail the logs for a specific service
  run <name>                Run a specific task (bypasses auto_run)
  run --all-pending         Run every task currently in pending_run
  exec <cmd> [args...]      Run a command with .don/bin on PATH
  attach <name>             Interactively attach stdin/stdout to a running service
  cleanup                   Kill orphaned processes, remove stale sockets/containers
  validate                  Check the config for errors without running anything
  completions <shell>       Print a shell completion script (bash/zsh/fish/...)
```

When no subcommand is given, `don` prints the help text. Use `don start` to bring your dev environment up.

### Interacting with a Running Instance

Don exposes an HTTP API over a Unix socket at `.don/don.sock`. The CLI commands (`restart`, `stop`, `status`, `logs`) communicate with a running don process through this socket. If no don process is running, these commands exit with an error.

This means you can have don running in one terminal and use `don restart api` from another terminal to restart a specific service without disrupting the rest of the environment.

### Watch-Triggered Restarts

When a service has `watch` patterns configured and a watched file changes:

1. **For services with a `build` step**: don runs the build command first, then restarts the service on success
2. **For services without a build step**: don restarts the service directly
3. **For docker services with `docker.build`**: don rebuilds the image, then recreates the container
4. **For rust services**: don runs `cargo build`, then restarts the binary

If the service has `listen` addresses, don keeps the sockets open during the restart so traffic is never dropped. The new process gets the sockets via `LISTEN_FDS` once it's ready.

#### Debouncing

File change events are debounced — don waits 200ms after the first event, collecting any further changes, then triggers a single rebuild/restart cycle. This handles rapid-fire saves, IDE auto-formatting, `git checkout`, etc.

The debounce window is optionally configurable per service via `debounce` (e.g. `debounce = "500ms"`). The default of 200ms should be fine for almost all cases.

#### Changes During Build/Restart

If files change while a build is already running, don does **not** kill the current build. It lets the build finish, then immediately triggers another cycle with the latest changes. This avoids wasting work on a half-finished build.

The same applies if files change during a restart (while waiting for the ready check) — let the restart finish, then restart again.

```
Idle ──[file change]──▶ Debouncing ──[200ms]──▶ Building ──[done]──▶ Restarting ──[ready]──▶ Idle
                                                    │                     │
                                              [file change]         [file change]
                                                    │                     │
                                                    ▼                     ▼
                                              mark stale,           mark stale,
                                              rebuild after         restart after
```

### Shutdown

**First `SIGINT`/`SIGTERM`** (e.g., Ctrl+C) — graceful shutdown:

1. Print `[don] shutting down gracefully... (Ctrl+C again to force)`
2. Stop services in reverse dependency order (no dependents first)
3. For services with `shutdown.graceful = true`, send each service its configured `shutdown.signal` (default `SIGTERM`) via `killpg`
4. Wait up to `shutdown.timeout` (default `10s`) for graceful exit
5. Send `SIGKILL` to any service that hasn't exited; services with `shutdown.graceful = false` receive `SIGKILL` immediately

**Second `SIGINT`/`SIGTERM`** — immediate kill:

1. Print `[don] forcing immediate shutdown`
2. Send `SIGKILL` to all service process groups immediately, no waiting
3. Clean up PID files and exit

### Interactive Attach

`don attach <name>` connects your terminal directly to a running service's PTY, giving you full interactive stdin/stdout access.

```
User's terminal ◄──raw mode──► don CLI ◄──WebSocket over unix socket──► don daemon ◄──PTY──► subprocess
```

**How it works:**

1. CLI sends `GET /attach/:name` to the unix socket API, which upgrades to a WebSocket
2. Daemon checks the attach lock (see below) — if another process is already attached, returns an error with the holder's PID
3. Daemon replays recent output from the ring buffer so the user has context (equivalent to `don logs --last N` followed by a live tail)
4. CLI puts the terminal in raw mode (via crossterm) so keypresses go through immediately
5. Bidirectional bridge: user input → WebSocket → PTY stdin, PTY stdout → WebSocket → user terminal
6. Terminal resize events are detected by the CLI and sent as WebSocket control messages, which the daemon translates to PTY resize calls

**Attach lock:**

Only one process can be attached to a service at a time. The daemon tracks which PID holds the attach lock:

- On attach: record the CLI process PID (sent in the initial WebSocket handshake)
- On detach/disconnect: release the lock
- If a second process tries to attach: reject with `"process 82648 is currently attached to 'my-task'"`
- If the holding process dies (WebSocket disconnects), the lock is automatically released

**Detaching:**

The escape sequence `~.` (ssh-style) detaches without killing the process. The CLI intercepts this before sending to the WebSocket. On detach:

1. CLI restores the terminal from raw mode
2. WebSocket closes cleanly
3. Daemon releases the attach lock
4. Normal prefixed output for the service resumes in the don terminal

**Output interaction:**

While a service is attached interactively:
- Prefixed output in the don terminal pauses for that service (other services continue normally)
- The ring buffer continues to be fed (so `don logs` still works)
- When the attach ends, prefixed output resumes
