# Don - Design Document

Don is a dev environment orchestrator. You write a `don.toml` config that defines your services, tasks, and their dependencies, and don runs everything for you.

## Architecture

> Component responsibilities — who owns what, the signals between them, and
> the one boundary that is not yet where it belongs — are in
> [`ownership.md`](ownership.md).

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
sections, plus a few optional top-level keys such as `default_profile`,
`watch_ignore`, and `fallback_ports`.

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

### Secrets

Secrets are first-class in Don. Values never live in config, and Don never
writes them to disk. `src/secrets/` is shaped to extract later as the `key`
crate; Don does not shell out to a Key binary.

Each `[[secrets]]` entry is one source. The provider is named by its key, the
way a service names its kind, so settings only that provider understands sit
inside it rather than at the top level. A source that names no provider is a
config error.

```toml
[[secrets]]
aws-ssm = { region = "us-east-1", profile = "dev" }
vars = { STRIPE_SECRET_KEY = "/app/StripeSecretKey" }
groups = { app = ["STRIPE_SECRET_KEY"] }

[services.api]
run.cmd = "./api"
secrets = ["app"]        # only these keys are exported to this process
```

Because it is a list, each entry fetches with its own credentials, so one run
can read parameters that live in separate accounts:

```toml
[[secrets]]
aws-ssm = { profile = "dev" }
vars = { STRIPE_SECRET_KEY = "/RedoDevelopment/StripeSecretKey" }

[[secrets]]
aws-ssm = { profile = "prod" }
vars = { LAUNCH_DARKLY_SDK_KEY = "/RedoProduction/LaunchDarklySdkKey" }
```

Groups and managed names are the union across sources, so a process may declare
a group from one and a key from another. A name supplied by more than one source
takes the value of the last source that supplies it.

A profile override replaces the list rather than merging into it, so a profile
states its sources in full. A provider named only in one profile does not exist
in any other, which is what keeps a dev stack from needing production
credentials.

`[service_groups.*] secrets` is the grant for members that omit `secrets`.
A member that sets `secrets` replaces the group list (it is not merged).
`secrets = []` is an explicit empty grant. Processes not in the group get
nothing.

```toml
[service_groups.application-services]
members = ["api-server", "admin-server"]
secrets = ["production"]

[services.admin-server]
run.cmd = "./admin"

[services.api-server]
run.cmd = "./api"
secrets = ["production", "other-secrets-group", "STRIPE_WEBHOOK_SECRET"]
```


At startup Don calls `aws ssm get-parameters --with-decryption` (raced against
Ctrl+C), injects declared keys, and strips undeclared managed keys from
inherited env. Known secret values are replaced with `***` in process logs
(TUI, `GET /logs`, `.don/logs/runner.log`) before they hit any sink. Don does
not put pulled values into its own environment. Expired AWS SSO credentials
print `aws sso login --profile <name>`; Don does not log in for you.

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

When the top-level `fallback_ports = true`, generated container names include a
hash of the canonical project directory so separate worktrees do not remove
each other's containers. Explicit `docker.container` names remain unchanged and
can still collide.

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
| `depends_on` | list of names or `{name, blocking}` tables | Services or tasks that must be ready/complete first. `blocking = false` makes an entry ordering-only |
| `proxy` | address or list | Public listeners using env forwarding, fixed forwarding, or `LISTEN_FDS` handoff |
| `ready` | table | Ready check configuration (see below) |
| `shutdown` | table | Shutdown behavior (see below) |
| `log` | string or table | Logging output destination (see below) |
| `log_filter` | list of regex strings | Per-service regex keep filters for log lines |
| `log_exclude` | list of regex strings | Per-service regex drop filters for log lines; applied before `log_filter` |
| `bazel.target` (tasks) | label | Builds the target; with no `cmd`, the task runs the built artifact directly |
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
| `depends_on` | list of names or `{name, blocking}` tables | Services or tasks that must be ready/complete first. `blocking = false` makes an entry ordering-only |
| `watch` | list of globs | File patterns — task only re-runs if these changed since last success. Empty = always runs |
| `timeout` | duration string | Maximum time the task is allowed to run (e.g. "5m"). No timeout by default |
| `log` | string or table | Logging output destination |
| `interactive` | bool | `false` (default). `true` says the task waits for a human at its terminal |
| `terminal` | string | Older spelling of `interactive`: `"foreground"` = `true`, `"muxed"` = `false` |
| `headless` | table | Optional `cmd` and/or `args` overrides for non-TUI runs |

Every task runs on a PTY and every task can be reached with `don attach`, so
`interactive` changes nothing about how a task is spawned or where its output
goes. It exists because it is the one thing Don cannot work out for itself: a
task blocked reading stdin looks exactly like a task that has hung.

Declaring it is what lets the TUI open the task's attach window when it starts
and close it again when it succeeds — a failed one keeps its window, since its
last screen is the reason it failed. Without a TUI there is nothing to attach,
so Don says `waiting for input — run 'don attach <task>'` instead. Use it for
REPLs, editors, interactive migrations, and prompting deploy scripts:

`terminal = "foreground"` is accepted as the older spelling of `interactive =
true`, and `"muxed"` as `false`. The mechanism it named — one task owning the
terminal — is gone; the declaration it carried is not, so configs predating the
rename keep working rather than being rejected.

```toml
[tasks.console]
cmd = "rails"
args = ["console"]
interactive = true
```

An interactive task can provide a non-interactive command variant for
`--no-tui`, redirected-output, and detached runs. Fields omitted from
`headless` inherit their normal values. Applying the override also clears
`interactive`, because nothing is going to attach in that mode:

```toml
[tasks.push]
cmd = "scurry"
args = ["push"]
interactive = true
headless = { args = ["push", "--force"] }
```

> `terminal = "muxed" | "foreground"` was the earlier spelling, back when a
> foreground task took exclusive ownership of Don's terminal. Per-task PTYs and
> `don attach` replaced that; the key is rejected with a message pointing here.

Independently of `interactive`, the stdout writer tracks the alternate-screen
private modes (`?1049`, `?1047`, `?47`) per process. A process holding the
screen is emitting frames — cursor moves and clears with no line boundaries —
so the multiplexed view would otherwise show one endless line of concatenated
frames, and show it only once the process exited. Instead Don emits `entered
full-screen mode — run 'don attach <name>' to see it` once and suppresses
output until the screen is handed back. Ring buffers, file sinks and the
server-side emulator behind `don attach` are fed upstream and lose nothing.

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

# Drop matching output lines, whatever log_filter says
log_exclude = ["/health"]

[services.api]
log_filter = ["request_id=abc", "^database:"]
log_exclude = ["/favicon"]
```

Top-level `log_filter = [...]` applies to every service, task, and synthetic
build-tool stream. A service-level `log_filter = [...]` adds service-specific
keep patterns. Filters are line-based regexes; when any filter is configured
for a stream, only matching service output reaches stdout/TUI, file sinks,
`don logs`, and `don logs --follow`. Lifecycle events are not filtered by
these regexes.

`log_exclude = [...]` nests identically and is evaluated first: a line
matching any exclude pattern is dropped even if a keep pattern also matches
it. That ordering is what makes "keep this whole category, minus the noisy
part of it" expressible. An exclude list with no keep list keeps everything
that does not match — it is a subtraction, not an allowlist.

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
- **`watch`, `depends_on`, `proxy`, `env_file`**: replaced entirely if set in the override
- **Preset fields** (`docker`, `rust`, `run`): if the override sets any preset field, all base preset fields are cleared and replaced
- **`build`, `dir`, `ready`, `shutdown`, `log`, `log_filter`, `download`**: override wins if set, otherwise base is kept

A service can have no base preset and define it entirely via platform overrides. Platforms without an override and no base preset will fail validation.

## Socket Passing (`proxy.listenfd`)

Don can own TCP sockets on behalf of services. When listenfd-mode `proxy`
entries are configured, don:

1. Binds the TCP sockets itself
2. Passes them to the child process as inherited file descriptors
3. Sets `LISTEN_FDS` and `LISTEN_FDNAMES` environment variables (systemd socket activation protocol)
4. Keeps the sockets open across service restarts so traffic is never dropped — the old process handles in-flight requests while the new one starts up

```toml
[services.api]
rust.binary = "api-server"
proxy = [
    { listen = "0.0.0.0:3000", listenfd = true },
    { listen = "0.0.0.0:3001", listenfd = true },
]
```

### Connection policy while a service is down

Don's listeners outlive the process, so what happens to a connection depends
on *why* the service isn't answering:

| Service state | Behavior |
|---------------|----------|
| starting, restarting, not yet ready | connections are held — env/forward entries park until the backend appears, listenfd entries sit in the kernel accept queue |
| failed, **process still alive** | connections are served normally |
| failed, **no process left** | connections are **refused**: already-queued and new connections are closed immediately |

Refusal exists because queuing is only kind when someone is coming back. A
service that failed and has no process leaves clients blocked on a socket
nothing will ever read.

The liveness half of that rule matters. `failed` does not imply the process
is gone: under the default `on_failure = "notify"`, a service whose ready
check fails keeps running and is often still serving traffic — a misconfigured
health probe must not take the app down. Don only refuses once the service has
both failed and lost its process (a crash, or a dependency failure where
nothing was ever spawned).

For env/forward entries the accept loop closes the client. For listenfd
entries Don drains and closes the accept queue itself; that is only safe
because the liveness rule guarantees no child is accepting on that socket at
the same time. Each listenfd socket has exactly one supervisor task — a
second `AsyncFd` registration on the same descriptor fails with `EEXIST`, so
the lazy start trigger and the refusal drain are two modes of one task rather
than two tasks. `don status --verbose` marks a refusing listener.

## Dependency Graph

Services and tasks share a single dependency namespace. The `depends_on` field accepts names of both services and tasks:

- A service depending on a **service**: waits for the dependency's ready check to pass
- A service depending on a **task**: waits for the task to complete successfully
- A task depending on a **service**: waits for the dependency's ready check to pass
- A task depending on a **task**: waits for the dependency to complete successfully

Names must be unique across services and tasks (validated).

### Blocking and non-blocking dependencies

Each `depends_on` entry is either a bare name or a table carrying the edge
kind:

```toml
[services.api]
rust.binary = "api-server"
depends_on = [
  "postgres",                                    # blocking (default)
  { name = "otel-collector", blocking = false }, # ordering only
]
```

| Kind | Gate | On dependency failure |
|------|------|-----------------------|
| blocking (default) | waits for ready/complete | dependent is skipped, `DependencyFailed`, with the root cause named |
| non-blocking (`blocking = false`) | waits for the dependency to *settle* | dependent starts anyway and logs `starting without non-blocking dependency '<name>'` |

"Settled" means the dependency is done deciding: a service that is ready,
failed, or stopped; a task that completed, failed, or is parked waiting for a
manual trigger (`auto_run = false`). A non-blocking edge also does not make a
manual task "required by dependents" — only a blocking edge does.

Non-blocking is not the same as "ignored": the edge still orders startup,
reverse shutdown order, and profile resolution — it only stops a failure from
cascading. Group references carry their kind to every member, and when the
same name is reachable through both a blocking and a non-blocking edge, the
blocking edge wins.

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
| `.don/ports.json` | Versioned manifest of configured and actual proxy/Docker ports |

This directory should be added to `.gitignore`.

### System-wide daemon state

The `don daemon` that serves the web UI is not scoped to any project, so it is
the one component that writes outside a `.don/` directory. Its state lives in a
single directory resolved by `src/daemon/paths.rs` — `$DON_STATE_DIR`, else
`$XDG_STATE_HOME/don`, else `~/.local/state/don` (`~/Library/Application
Support/don` on macOS):

| Path | Purpose |
|------|---------|
| `daemon.sock` | Unix socket for the daemon's control API (register/deregister/list) |
| `daemon.pid` | Flock'd single-instance guard |
| `registry.json` | Cached list of running projects, so a daemon restart doesn't lose sight of them |
| `logs/daemon.log` | Daemon output when run under systemd/launchd |

Nothing here belongs to a project, and the daemon never writes into one.

## Web UI and the Broker Daemon

The daemon is a **broker**: it holds a registry of running projects and serves a
UI over them, reverse-proxying each project's existing `.don/don.sock`. It never
spawns or supervises a service.

```
terminal A: don start ──runner──> services      terminal B: don start ──runner──> services
              │ .don/don.sock                                 │ .don/don.sock
              └──── register(name, root, sock) ───┬───────────┘
                                                  ▼
                                         don daemon (registry)
                                           unix sock: control
                                           tcp 127.0.0.1:3666: web UI ──> browser
```

That choice is what keeps the rest of don unchanged. `don start` still owns its
process group, its `.don/`, and its terminal. Consequences worth stating:

- **Registration is best-effort and never awaited on the runner task.** Most
  users won't install a daemon, so a project that can't reach one must start and
  stop exactly as fast. Deregistration is fired at the top of the shutdown path
  and never awaited — per the shutdown-responsiveness rules, nothing may sit
  between the user's Ctrl+C and the stack going down.
- **Liveness is derived, not tracked.** There are no heartbeats. Reading the
  registry probes each project's socket and drops the ones that don't answer, so
  a `kill -9`'d runner cleans itself up without a background timer, and a
  deregistration that never landed costs nothing.
- **Stopping the daemon is harmless.** Every registered project keeps running.

### Two hosts, one router

The web router is built against a `ProjectDirectory` that either reads the
daemon's registry or holds a single project (`don start --with-ui`). No handler
knows which mode it is in, and every handler forwards through
`crate::client::Client` to the project API — the UI holds no orchestration logic
of its own and so cannot drift from the CLI.

### Web UI access

The web UI binds loopback and does not authenticate. Every other don API is a
unix socket chmod'd to 0600, where the filesystem does the work; a TCP port has
no equivalent, so in principle any local process can drive the UI's API. That is
an accepted trade: anything able to run a process on this machine can already do
everything don can, so a shared secret would add ceremony without adding a
guarantee.

One case is *not* covered by that reasoning, because the attacker has no access
to the machine at all — a web page the user merely visits. Same-origin policy
stops such a page reading a response from 127.0.0.1, but DNS rebinding defeats
it: point `evil.example.com` at 127.0.0.1 and the page becomes same-origin with
don, free to read project paths, logs, and config. A rebound request carries the
attacker's hostname, so `src/web/origin.rs` rejects any `Host` whose name isn't
a loopback one.

Only the name is checked, not the port. A browser always sends the authority it
connected to, so the port can't disagree in a way that signals an attack —
while comparing it breaks every reverse proxy, which legitimately forwards a
different one. Don's own proxy does this whenever the daemon runs behind
`proxy = { ... }`, as it does in this repo's `don.toml`.

That guard protects confidentiality, not integrity. A blind cross-origin `POST`
that ignores the response carries the correct `Host` and still goes through, so
a visited page can trigger an action here without seeing its result. Closing
that would take a custom-header requirement (which forces a CORS preflight the
server fails) — deliberately not done, on the same "it's your machine" grounds.

## Environment Variables

Environment variables are resolved in this order (later wins):

0. The inherited environment, plus `PWD` set to the directory the child will
   actually run in — Don changes the child's cwd, so leaving `PWD` pointing at
   Don's own would be a lie that shell children silently correct and everything
   else silently believes.
1. `.env.<service-name>` — auto-loaded if the file exists (convention)
2. `env_file` — explicitly listed env files, loaded in order
3. `env` — inline variables from the config
4. Don-injected variables (`LISTEN_FDS`, `LISTEN_FDNAMES`, `DON_PUBLIC_*`, `DON_DOWNLOAD_DIR`, etc.)

### `${VAR}` expansion

`cmd`, `args`, `ready.tcp`/`ready.http`, and inline `env` values expand
`${VAR}` references. Unknown names are left verbatim, so a value that merely
resembles a reference survives rather than being silently emptied.

Inline `env` values expand against everything *except* the `env` block itself —
the inherited environment (including `PWD`), env files, and Don's injected
variables. They deliberately cannot reference each other: config env is a map,
map order is arbitrary, and `A = "${B}"` beside `B = "x"` would otherwise
resolve differently from run to run.

```toml
[services.api]
env = { DATABASE_URL = "postgres://localhost:${PORT}/app", STATE = "${PWD}/.state" }
```

An env-mode proxy keeps its configured env variable (for example `PORT`) for
the private backend port the service binds. Its actual public listener is
available through `DON_PUBLIC_PORT` / `DON_PUBLIC_ADDR`, indexed
`DON_PUBLIC_PORT_0` / `DON_PUBLIC_ADDR_0` forms, and aliases derived from the
proxy env name. Ready TCP/HTTP checks can expand `${DON_PUBLIC_PORT}`.
Docker ready checks receive equivalent runner-side values after the container
starts and Don has inspected its assigned host bindings; these values cannot be
injected into that already-running container.

Inline env values on dependent services and tasks may reference another
service's runtime public binding with `$(service.port)` /
`$(service.addr)`. Indexed keys (`port_0`, `addr_0`), env-mode proxy names
(`$(api.PORT)`), and unambiguous Docker container-port keys
(`$(database.PORT_5432)`) are also supported. `depends_on` establishes the
startup ordering needed for Docker bindings to exist before expansion.

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
- **Docker containers** — don checks the effective container name for an orphaned container and stops/removes it. With `fallback_ports = true`, generated names include a project-directory hash; explicit names are preserved and can still collide across worktrees.

### Port Conflict Detection

Before starting services, don pre-checks for port conflicts:

- **Proxy listeners**: Don binds `proxy.listen` itself, so conflicts are caught immediately with a clear error.
- **Fallback ports**: when `fallback_ports = true`, Don first tries each configured proxy or Docker host address. If it is already in use, Don requests an OS-assigned port on the same host/IP. Permission errors, invalid addresses, and other failures do not fall back.
- **Scope**: fallback applies only to explicit `proxy.listen` addresses and the host side of `docker.ports`. Don does not choose ports by inferring them from arbitrary env values, command arguments, or fixed proxy backend targets. TCP/HTTP ready checks that target an explicitly configured public port are rewritten to its actual binding.
- **Docker authority**: Docker's inspected post-start port bindings are authoritative. This avoids reporting a probe port that Docker did not actually acquire and gives ready checks the real host port.
- **Runtime references**: immediately before a dependent service or task starts, Don renders `$(service.key)` references in its inline env after proxy pre-bind and after Docker dependencies have started.

The configured choice remains the stable preferred address. A fallback is held
for the lifetime of a Don proxy; Docker restarts try to retain their resolved
mapping. Don records both configured and actual values in `.don/ports.json`,
shows them with `don ports`, and uses actual addresses for `LISTEN_FDNAMES` and
verbose status.

A configured proxy or Docker host port of `0` skips the preferred-port attempt
and always requests an OS-assigned port, independently of `fallback_ports`.

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

### Profile Overrides

A profile may carry an `overrides` table: a `don.toml` fragment applied whenever
that profile is the active one. It answers "same stack, different target" —
pointing every service at a different database or conf without a second config
file and without duplicating the service definitions.

```toml
[profiles.prod]
services = ["api", "worker"]

[profiles.prod.overrides.services.api]
env = { DATABASE_URL = "postgres://prod.internal:5432/app" }
```

A header per service, or dotted keys under one header — the same table either
way, since the merge works on the parsed value:

```toml
[profiles.prod.overrides]
services.api.env = { DATABASE_URL = "postgres://prod.internal:5432/app" }
```

The one constraint is TOML's own: a path spelled as a header cannot also appear
as a dotted key under a parent header, so one form per service.

Overrides merge with the same recursive table merge the local file uses, and the
three layers apply in order:

1. the base file
2. the active profile's `overrides` — the profile named by `--profile`, else
   `default_profile`
3. the sibling `.local.toml`

The local file goes last so a developer's own overrides outrank a profile the
repo ships; it may also extend a profile's `overrides` block, or move
`default_profile` to select a different one. An overrides block may not itself
set `profiles` or `default_profile` — an override that changed which overrides
apply would be a merge that reads differently depending on where you start.

A top-level `env` table pairs with this directly: it is handed to every service
and task (a process's own `env` wins, and a platform override wins over both),
so `[profiles.prod.overrides.env]` moves an entire stack onto a different
database with one key rather than one block per service.

The merge is the local file's: tables merge key by key, scalars and arrays
replace. An overridden `run.args` or `watch` is the whole new list. What the
block may contain is the file's root, so `tasks`, `service_groups`, and
top-level keys such as `watch_ignore` merge alongside `services`.

Overriding is not selecting. The profile's `services` and `tasks` lists decide
what runs; the `overrides` block only decides what those things are. A service
defined solely in an overrides block is a configured service the profile did not
pick, and it stays stopped — the same state any service outside the active
profile is in.

`don validate --profile <name>` loads the config exactly as `don start
--profile <name>` would, so the merged result can be checked before anything
starts. Adding `--show` prints that merged config as TOML, with the active
profile's `overrides` block removed — it has already been folded into the
services and tasks above it, and leaving it in would read as though it were
still pending. Every other profile keeps its block: those have not been applied,
and hiding them would misreport what the file says. A bare `don validate`
answers only whether the merge is legal, and says on stderr that `--show` will
print it.

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
  ports                     Show configured and actual runtime ports
  logs <name>               Tail the logs for a specific service
  run <name>                Run a specific task (bypasses auto_run)
  exec <cmd> [args...]      Run a command with .don/bin on PATH
  attach [name]             Attach the TUI to a running project, or one service's PTY
  cleanup                   Kill orphaned processes, remove stale sockets/containers
  validate [--profile] [--show]
                            Check the config for errors, or print the merged result
  completions <shell>       Print a shell completion script (bash/zsh/fish/...)
```

When no subcommand is given, `don` prints the help text. Use `don start` to bring your dev environment up.

### Interacting with a Running Instance

Don exposes an HTTP API over a Unix socket at `.don/don.sock`. The CLI commands
(`restart`, `stop`, `status`, `logs`) communicate with a running don process
through this socket. If no don process is running, these commands exit with an
error. `GET /status?verbose=true` reports actual proxy and Docker addresses;
`.don/ports.json` is the structured source for both proxy and Docker mappings.

This means you can have don running in one terminal and use `don restart api` from another terminal to restart a specific service without disrupting the rest of the environment.

### Watch-Triggered Restarts

When a service has `watch` patterns configured and a watched file changes:

1. **For services with a `build` step**: don runs the build command first, then restarts the service on success
2. **For services without a build step**: don restarts the service directly
3. **For docker services with `docker.build`**: don rebuilds the image, then recreates the container
4. **For rust services**: don runs `cargo build`, then restarts the binary

If the service has proxy listeners, Don keeps them open during the restart so
traffic is never dropped. A listenfd-mode process receives the sockets via
`LISTEN_FDS`, with actual bound addresses in `LISTEN_FDNAMES`.

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
