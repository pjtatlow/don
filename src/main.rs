// The CLI binary legitimately uses stdout — it IS the user-facing output.
#![allow(clippy::print_stdout)]
#![allow(clippy::print_stderr)]

mod wisdom;

use clap::{Parser, Subcommand};
use crossterm::style::{Attribute, Color, ResetColor, SetAttribute, SetForegroundColor};
use don::TaskRunInfo;
use don::client::{Client, ClientError, RunTaskOptions};
use don::runner::{ItemStatus, ServiceState, TaskItemState};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Write a line to stderr. Used for CLI error messages so they stay on the
/// error stream (scripts / tests / shell redirection expect it). We avoid
/// the `eprintln!` macro because it was frequently abused for debug-trace
/// output elsewhere in the codebase; keeping stderr writes behind this
/// single helper makes intentional error output easy to grep for.
fn errln(msg: impl std::fmt::Display) {
    let _ = write!(std::io::stderr(), "{msg}\r\n");
}

#[derive(Parser, Debug)]
#[command(
    name = "don",
    about = "Boss of your dev environment",
    long_about = "Boss of your dev environment.\n\n\
        Tell the Don what services and tasks you run. He'll see they get \
        started in the right order, stay alive, and shut down clean. \
        No loose ends.",
    version,
    arg_required_else_help = true
)]
struct Cli {
    /// Path to the config file
    #[arg(short, long, default_value = "don.toml", global = true)]
    config: PathBuf,

    /// Enable verbose output (timing info for lifecycle events)
    #[arg(short, long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start services and run tasks
    Start {
        /// Run only services/tasks in this profile
        #[arg(short, long)]
        profile: Option<String>,
        /// Start don in the background and exit once the daemon is launching
        #[arg(short = 'd', long, conflicts_with = "name")]
        detached: bool,
        /// Name of a stopped service to start (omit to start the daemon)
        name: Option<String>,
        /// Force pipe-mode output instead of the TUI, even on a TTY. Useful
        /// for CI, log capture, scripted shutdown tests, and any environment
        /// where the terminal doesn't reliably answer DSR cursor queries.
        #[arg(long)]
        no_tui: bool,
        /// Restrict displayed log lines to the given comma-separated set of
        /// service/task names. Affects pipe-mode output and seeds the TUI
        /// filter on startup (overriding any `hidden = true` defaults).
        /// Ring buffers and file sinks are unaffected — `don logs <name>`
        /// still returns full output. Example: `--log-filter=web,api,db`.
        #[arg(long, value_delimiter = ',')]
        log_filter: Vec<String>,
    },
    /// Stop the daemon, or stop one running service when a name is given
    Stop {
        /// Name of the service to stop (omit to stop the daemon)
        name: Option<String>,
    },
    /// Restart a running service
    Restart {
        /// Name of the service to restart
        name: String,
    },
    /// Show status of all services and tasks, or a single one when NAME is given
    Status {
        /// Name of a single service or task to inspect (omit to show all).
        /// Combine with `--verbose` to list its fully-resolved watch paths —
        /// useful for debugging why a build-tool service isn't reloading.
        name: Option<String>,
        /// Show detailed info: watch paths, ports, build tool targets, commands
        #[arg(short, long)]
        verbose: bool,
        /// Emit machine-readable JSON instead of the table: an object with a
        /// top-level `ready` bool (true when every service is Ready or Lazy),
        /// a `tasks_pending_run` count (tasks parked awaiting a manual run —
        /// the headless equivalent of the TUI's `*` flag), and an `items`
        /// array. Useful for scripts and agents polling for stack readiness.
        #[arg(long)]
        json: bool,
    },
    /// Show everything don is currently watching: the inotify directories it has
    /// registered plus the per-item glob patterns that trigger reloads. Useful
    /// for confirming a file actually falls under a watch — especially for
    /// build-tool services whose paths are resolved dynamically.
    Watch {
        /// Emit machine-readable JSON instead of the human-readable report.
        #[arg(long)]
        json: bool,
    },
    /// View logs for a service or task
    Logs {
        /// Name of the service or task
        name: String,
        /// Show last N lines
        #[arg(short, long, default_value_t = 100)]
        last: usize,
        /// Follow the log output
        #[arg(short, long)]
        follow: bool,
    },
    /// Interactively attach stdin/stdout to a running service
    Attach {
        /// Name of the service to attach to
        name: String,
    },
    /// Clean up stale state from a previous run
    Cleanup {
        /// Kill a running daemon first, then clean up
        #[arg(long)]
        force: bool,
    },
    /// Run a task (bypasses auto_run)
    Run {
        /// Name of a specific task to run (mutually exclusive with --all-pending)
        name: Option<String>,
        /// Run all tasks currently in pending_run state
        #[arg(long, conflicts_with = "name")]
        all_pending: bool,
        /// Never prompt for missing required params — error instead. Implicit
        /// when stdin isn't a TTY. Useful in scripts / CI.
        #[arg(long, conflicts_with = "all_pending")]
        no_prompt: bool,
        /// Wait until the task exits before returning
        #[arg(long)]
        wait: bool,
        /// Maximum time to wait for task completion (implies --wait)
        #[arg(long, value_name = "DURATION")]
        timeout: Option<String>,
        /// Per-param flags. Parsed dynamically against the task's declared
        /// params: `--<param>=<value>`, `--<param> <value>`, or bare
        /// `--<flag>` (treated as `"true"` for bool params).
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        raw: Vec<String>,
    },
    /// Validate the config file
    Validate,
    /// Print the don version
    Version,
    #[command(hide = true)]
    Wisdom,
    /// Print shell completion script to stdout
    Completions {
        /// Target shell (bash, zsh, fish, powershell, elvish)
        shell: clap_complete::Shell,
    },
    /// Scaffold a starter don.toml in the current directory
    Init {
        /// Overwrite an existing don.toml
        #[arg(long)]
        force: bool,
    },
    /// Run a command with .don/bin on PATH (for downloaded binaries)
    Exec {
        /// Command to run
        cmd: String,
        /// Arguments passed to the command
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Internal: list names for shell completion scripts. Hidden from help.
    #[command(name = "__complete", hide = true)]
    Complete {
        /// One of: services, tasks, items, profiles
        kind: String,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let exit_code = run(cli.config, cli.verbose, cli.command).await;
    std::process::exit(exit_code);
}

async fn run(config_path: PathBuf, verbose: bool, command: Commands) -> i32 {
    match command {
        Commands::Version => {
            println!("don {}", env!("CARGO_PKG_VERSION"));
            0
        }
        Commands::Wisdom => {
            let q = wisdom::random();
            println!("\"{}\"", q.text);
            println!("    — {}, {}", q.speaker, q.source);
            0
        }
        Commands::Validate => match validate(&config_path) {
            Ok(()) => {
                println!("Config is valid.");
                0
            }
            Err(e) => {
                errln(e);
                1
            }
        },
        Commands::Start {
            profile,
            name: None,
            detached,
            no_tui,
            log_filter,
        } => {
            let result = if detached {
                run_start_detached(&config_path, profile.as_deref(), verbose, log_filter).await
            } else {
                run_start(
                    &config_path,
                    profile.as_deref(),
                    verbose,
                    no_tui,
                    log_filter,
                )
                .await
            };
            match result {
                Ok(()) => 0,
                Err(e) => {
                    errln(e);
                    1
                }
            }
        }
        Commands::Start {
            name: Some(name), ..
        } => run_client(&config_path, |c| async move { c.start(&name).await }).await,
        Commands::Stop { name: Some(name) } => {
            run_client(&config_path, |c| async move { c.stop(&name).await }).await
        }
        Commands::Stop { name: None } => run_stop_daemon(&config_path).await,
        Commands::Restart { name } => {
            run_client(&config_path, |c| async move { c.restart(&name).await }).await
        }
        Commands::Status {
            name,
            verbose,
            json,
        } => run_status(&config_path, name.as_deref(), verbose, json).await,
        Commands::Watch { json } => run_watch(&config_path, json).await,
        Commands::Logs { name, last, follow } => run_logs(&config_path, &name, last, follow).await,
        Commands::Attach { name } => run_attach(&config_path, &name).await,
        Commands::Cleanup { force } => run_cleanup_command(&config_path, force).await,
        Commands::Run {
            name,
            all_pending,
            raw,
            no_prompt,
            wait,
            timeout,
        } => match (name, all_pending) {
            (Some(n), _) => run_run_task(&config_path, n, raw, no_prompt, wait, timeout).await,
            (None, true) => {
                if wait || timeout.is_some() {
                    errln("don run --all-pending does not support --wait or --timeout");
                    return 2;
                }
                run_client(&config_path, |c| async move { c.run_pending().await }).await
            }
            (None, false) => {
                errln("don run: provide a task name or --all-pending");
                1
            }
        },
        Commands::Completions { shell } => {
            let mut out = std::io::stdout();
            match don::completions::emit_script::<_, Cli>(shell, "don", &mut out) {
                Ok(()) => 0,
                Err(e) => {
                    errln(format!("failed to write completion script: {e}"));
                    1
                }
            }
        }
        Commands::Complete { kind } => {
            // Silent-on-error: invoked inside tab-completion. If the kind is
            // unknown or the config is broken, print nothing and exit 0 so
            // the user's shell doesn't spew errors mid-tab-press.
            if let Ok(kind) = kind.parse::<don::completions::CompleteKind>() {
                for name in don::completions::list_names(kind, &config_path) {
                    println!("{name}");
                }
            }
            0
        }
        Commands::Init { force } => match don::init::write_starter_config(&config_path, force) {
            Ok(()) => {
                println!("created {}", config_path.display());
                0
            }
            Err(e) => {
                errln(e);
                1
            }
        },
        Commands::Exec { cmd, args } => {
            let base = base_dir(&config_path);
            match don::exec::exec_with_don_path(&base, &cmd, &args) {
                Ok(()) => 0, // unreachable — execvp returns only on error
                Err(e) => {
                    errln(format!("don exec {cmd}: {e}"));
                    127
                }
            }
        }
    }
}

fn client_for(config_path: &Path) -> Client {
    Client::new(base_dir(config_path).as_path())
}

/// Handle `don run <task> [flags]`. Parses `raw` against the task's declared
/// params and dispatches via the client.
async fn run_run_task(
    config_path: &Path,
    name: String,
    raw: Vec<String>,
    no_prompt: bool,
    wait: bool,
    timeout: Option<String>,
) -> i32 {
    // Load config to look up the task's params. This duplicates what the
    // runner does server-side, but we need the param list *here* to
    // parse the trailing args correctly.
    let config = match don::config::Config::from_file(config_path) {
        Ok(c) => c,
        Err(e) => {
            errln(format!("failed to load config: {e}"));
            return 1;
        }
    };
    let Some(task) = config.tasks.get(&name) else {
        errln(format!("unknown task '{name}'"));
        return 1;
    };

    let (wait, wait_timeout, raw) = match split_run_flags(&raw, wait, timeout) {
        Ok(v) => v,
        Err(msg) => {
            errln(msg);
            return 2;
        }
    };

    let parsed = match parse_task_args(&raw, &task.params) {
        Ok(p) => p,
        Err(msg) => {
            errln(msg);
            return 2;
        }
    };

    // Interactive TTY mode could open a form here for missing required
    // params. Until the TUI form is wired into `don run` itself, we keep
    // the CLI in strict mode: error out when required params are missing
    // and the user hasn't supplied `--no-prompt` (it's implied).
    let _ = no_prompt;
    for p in &task.params {
        if p.required && !parsed.contains_key(&p.name) && p.default.is_none() {
            errln(format!(
                "missing required param --{} (run `don start` and use the palette form, \
                 or pass --{}=<value>)",
                p.name, p.name
            ));
            return 2;
        }
    }

    if let Some(timeout_str) = wait_timeout.as_deref()
        && let Err(e) = don::duration::parse_duration(timeout_str)
    {
        errln(format!("invalid --timeout: {e}"));
        return 2;
    }

    let client = client_for(config_path);
    let options = RunTaskOptions { wait, wait_timeout };
    match client.run_task_with_options(&name, parsed, options).await {
        Ok(()) => 0,
        Err(ClientError::WaitTimeout { message }) => {
            errln(message);
            124
        }
        Err(e) => {
            errln(e);
            1
        }
    }
}

/// Split `don run`'s own control flags out of the trailing task-param args.
/// Clap parses `don run --wait task`, but `don run task --wait` lands in
/// `raw` because task params are intentionally accepted as arbitrary flags.
fn split_run_flags(
    raw: &[String],
    initial_wait: bool,
    initial_timeout: Option<String>,
) -> Result<(bool, Option<String>, Vec<String>), String> {
    let mut wait = initial_wait || initial_timeout.is_some();
    let mut timeout = initial_timeout;
    let mut params = Vec::new();
    let mut i = 0;
    while i < raw.len() {
        let arg = &raw[i];
        if arg == "--wait" {
            wait = true;
            i += 1;
            continue;
        }
        if arg == "--timeout" {
            let Some(value) = raw.get(i + 1) else {
                return Err("missing value for --timeout (use --timeout=<duration>)".to_string());
            };
            if value.starts_with("--") {
                return Err("missing value for --timeout (use --timeout=<duration>)".to_string());
            }
            timeout = Some(value.clone());
            wait = true;
            i += 2;
            continue;
        }
        if let Some(value) = arg.strip_prefix("--timeout=") {
            timeout = Some(value.to_string());
            wait = true;
            i += 1;
            continue;
        }
        params.push(arg.clone());
        i += 1;
    }
    Ok((wait, timeout, params))
}

/// Parse the trailing raw args from `don run <task> …` against the task's
/// declared params. Accepts:
/// - `--<name>=<value>`
/// - `--<name> <value>`
/// - bare `--<flag>` (kind = Bool → "true")
///
/// Returns a map of user-supplied values, or a user-facing error string.
fn parse_task_args(
    raw: &[String],
    params: &[don::config::TaskParam],
) -> Result<std::collections::HashMap<String, String>, String> {
    use don::config::ParamKind;

    let by_name: std::collections::HashMap<&str, &don::config::TaskParam> =
        params.iter().map(|p| (p.name.as_str(), p)).collect();
    let known_names: Vec<&str> = params.iter().map(|p| p.name.as_str()).collect();

    let mut out = std::collections::HashMap::new();
    let mut i = 0;
    while i < raw.len() {
        let arg = &raw[i];
        if !arg.starts_with("--") {
            return Err(format!(
                "unexpected positional arg '{arg}' — don run expects --<name>=<value> flags"
            ));
        }
        let stripped = &arg[2..];
        let (name, value) = match stripped.split_once('=') {
            Some((k, v)) => (k.to_string(), Some(v.to_string())),
            None => (stripped.to_string(), None),
        };
        let Some(param) = by_name.get(name.as_str()) else {
            let valid = if known_names.is_empty() {
                "task declares no params".to_string()
            } else {
                format!("valid: --{}", known_names.join(", --"))
            };
            return Err(format!("unknown param '--{name}' ({valid})"));
        };
        let resolved = match value {
            Some(v) => v,
            None => match param.kind {
                ParamKind::Bool => {
                    // Bare `--flag` = true for bools.
                    i += 1;
                    out.insert(name, "true".to_string());
                    continue;
                }
                _ => {
                    // Expect the next token to be the value.
                    match raw.get(i + 1) {
                        Some(v) if !v.starts_with("--") => {
                            let v = v.clone();
                            i += 2;
                            out.insert(name, v);
                            continue;
                        }
                        _ => {
                            return Err(format!(
                                "param --{name} is missing a value (use --{name}=<value>)"
                            ));
                        }
                    }
                }
            },
        };
        i += 1;
        out.insert(name, resolved);
    }
    Ok(out)
}

fn base_dir(config_path: &Path) -> PathBuf {
    match config_path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

/// Run a client command that returns `()` on success. Prints a friendly
/// error and returns exit code 1 on failure.
async fn run_client<F, Fut>(config_path: &Path, make_call: F) -> i32
where
    F: FnOnce(Client) -> Fut,
    Fut: std::future::Future<Output = Result<(), ClientError>>,
{
    let client = client_for(config_path);
    match make_call(client).await {
        Ok(()) => 0,
        Err(e) => {
            errln(e);
            1
        }
    }
}

async fn run_stop_daemon(config_path: &Path) -> i32 {
    let base = base_dir(config_path);
    let client = Client::new(&base);
    if let Err(e) = client.shutdown().await {
        errln(e);
        return 1;
    }

    let socket_path = base.join(".don").join("don.sock");
    match wait_for_daemon_socket_gone(&socket_path, std::time::Duration::from_secs(60)).await {
        Ok(()) => {
            println!("don daemon stopped");
            0
        }
        Err(e) => {
            errln(e);
            1
        }
    }
}

async fn wait_for_daemon_socket_gone(
    socket_path: &Path,
    timeout: std::time::Duration,
) -> Result<(), String> {
    let client = Client::with_socket_path(socket_path.to_path_buf());
    let start = tokio::time::Instant::now();
    loop {
        if let Err(ClientError::NotRunning { .. }) = client.status(false, None).await {
            return Ok(());
        }
        if start.elapsed() >= timeout {
            return Err(format!(
                "shutdown requested, but don daemon did not stop within {}s",
                timeout.as_secs()
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

async fn run_status(config_path: &Path, name: Option<&str>, verbose: bool, json: bool) -> i32 {
    let client = client_for(config_path);
    match client.status(verbose, name).await {
        Ok(mut items) => {
            // A named query is filtered server-side; an empty result means the
            // name didn't match anything. Fetch the full list to offer a
            // did-you-mean before failing.
            if let Some(name) = name
                && items.is_empty()
            {
                let suggestion = match client.status(false, None).await {
                    Ok(all) => {
                        let available: std::collections::HashSet<&str> =
                            all.iter().map(item_name).collect();
                        suggest_name_typo(name, &available)
                    }
                    Err(_) => String::new(),
                };
                errln(format!("no service or task named '{name}'{suggestion}"));
                return 1;
            }
            items.sort_by(|a, b| {
                status_sort_bucket(a)
                    .cmp(&status_sort_bucket(b))
                    .then_with(|| item_name(a).cmp(item_name(b)))
            });
            if json {
                #[derive(serde::Serialize)]
                struct StatusJson<'a> {
                    ready: bool,
                    tasks_pending_run: usize,
                    items: &'a [ItemStatus],
                }
                let payload = StatusJson {
                    ready: don::runner::all_services_ready(&items),
                    tasks_pending_run: count_tasks_pending_run(&items),
                    items: &items,
                };
                return match serde_json::to_string_pretty(&payload) {
                    Ok(s) => {
                        println!("{s}");
                        0
                    }
                    Err(e) => {
                        errln(format!("failed to serialize status as JSON: {e}"));
                        1
                    }
                };
            }
            print_status_table(&items, verbose, name.is_some());
            0
        }
        Err(e) => {
            errln(e);
            1
        }
    }
}

async fn run_watch(config_path: &Path, json: bool) -> i32 {
    let client = client_for(config_path);
    match client.watch().await {
        Ok(report) => {
            if json {
                #[derive(serde::Serialize)]
                struct WatchJson<'a> {
                    watch: Option<&'a don::WatchReport>,
                }
                return match serde_json::to_string_pretty(&WatchJson {
                    watch: report.as_ref(),
                }) {
                    Ok(s) => {
                        println!("{s}");
                        0
                    }
                    Err(e) => {
                        errln(format!("failed to serialize watch report as JSON: {e}"));
                        1
                    }
                };
            }
            match report {
                Some(report) => print_watch_report(&report),
                None => println!(
                    "don is not watching any files (no service or task has watch patterns enabled)"
                ),
            }
            0
        }
        Err(e) => {
            errln(e);
            1
        }
    }
}

fn print_watch_report(report: &don::WatchReport) {
    let dim = SetAttribute(Attribute::Dim);
    let reset = SetAttribute(Attribute::Reset);

    // Headline: the actual inotify registrations — what don is truly watching.
    if report.directories.is_empty() {
        println!("watching 0 directories");
    } else {
        let mode_w = report
            .directories
            .iter()
            .map(|d| d.mode.len())
            .max()
            .unwrap_or(0);
        println!(
            "watching {} {} (inotify):",
            report.directories.len(),
            if report.directories.len() == 1 {
                "directory"
            } else {
                "directories"
            }
        );
        for dir in &report.directories {
            println!("  {:<mode_w$}  {}", dir.mode, dir.path);
        }
    }

    if !report.global_ignore.is_empty() {
        println!();
        println!("{dim}global ignore:{reset}");
        for pattern in &report.global_ignore {
            println!("  {pattern}");
        }
    }

    println!();
    println!(
        "{} watched {}:",
        report.items.len(),
        if report.items.len() == 1 {
            "item"
        } else {
            "items"
        }
    );
    for item in &report.items {
        let stale = if item.stale { " stale" } else { "" };
        println!(
            "  {}  {dim}{} · {} · debounce {}ms{}{reset}",
            item.name, item.kind, item.state, item.debounce_ms, stale
        );
        for pattern in &item.patterns {
            println!("      {pattern}");
        }
        for pattern in &item.ignore_patterns {
            println!("      {dim}! {pattern}{reset}");
        }
        if let Some(ref err) = item.last_error {
            println!("      {dim}error: {err}{reset}");
        }
    }

    // Diagnostics worth surfacing: a non-zero count here usually explains a
    // "didn't reload" report (dropped events or an item stuck mid-rebuild).
    if report.notify_error_count > 0 || report.runner_event_lag_count > 0 {
        println!();
        if report.notify_error_count > 0 {
            let last = report.last_notify_error.as_deref().unwrap_or("");
            println!(
                "{dim}notify errors:{reset} {} (last: {last})",
                report.notify_error_count
            );
        }
        if report.runner_event_lag_count > 0 {
            println!(
                "{dim}runner-event lag:{reset} {} — an item may be stuck mid-rebuild",
                report.runner_event_lag_count
            );
        }
    }
}

/// Sort bucket for the status table: actionable rows first, settled rows
/// last. Putting `DependencyFailed` *below* `Failed` surfaces the actual
/// culprit — the thing the user needs to look at — above everything that
/// merely got stranded.
fn status_sort_bucket(item: &ItemStatus) -> u8 {
    match item {
        ItemStatus::Service { state, .. } => match state {
            ServiceState::Failed | ServiceState::Unhealthy => 0,
            ServiceState::DependencyFailed => 1,
            ServiceState::Pending | ServiceState::Building | ServiceState::Starting => 2,
            ServiceState::Running => 3,
            ServiceState::Ready => 4,
            ServiceState::Stopping => 5,
            ServiceState::Stopped => 6,
            ServiceState::Lazy => 7,
        },
        ItemStatus::Task { state, .. } => match state {
            TaskItemState::Failed => 0,
            TaskItemState::DependencyFailed => 1,
            TaskItemState::Pending | TaskItemState::Building | TaskItemState::Running => 2,
            TaskItemState::PendingRun => 7,
            TaskItemState::Completed => 8,
            TaskItemState::Skipped => 9,
        },
    }
}

fn item_name(item: &ItemStatus) -> &str {
    match item {
        ItemStatus::Service { name, .. } | ItemStatus::Task { name, .. } => name.as_str(),
    }
}

/// Count tasks parked in `PendingRun` — maintenance work awaiting a manual run.
/// Mirrors the TUI's `*` flag so headless `--json` callers can detect it.
fn count_tasks_pending_run(items: &[ItemStatus]) -> usize {
    items
        .iter()
        .filter(|item| {
            matches!(
                item,
                ItemStatus::Task {
                    state: TaskItemState::PendingRun,
                    ..
                }
            )
        })
        .count()
}

async fn run_logs(config_path: &Path, name: &str, last: usize, follow: bool) -> i32 {
    let client = client_for(config_path);
    if follow {
        match client
            .logs_follow(name, last, |line| {
                // Each NDJSON frame is `{"line":"..."}`.
                match serde_json::from_str::<serde_json::Value>(line) {
                    Ok(v) => {
                        if let Some(s) = v.get("line").and_then(|x| x.as_str()) {
                            println!("{s}");
                        }
                    }
                    Err(_) => {
                        // Fall back to raw line — shouldn't happen with this server.
                        println!("{line}");
                    }
                }
                Ok(())
            })
            .await
        {
            Ok(()) => 0,
            Err(e) => {
                errln(e);
                1
            }
        }
    } else {
        match client.logs(name, last).await {
            Ok(lines) => {
                for line in lines {
                    println!("{line}");
                }
                0
            }
            Err(e) => {
                errln(e);
                1
            }
        }
    }
}

async fn run_attach(config_path: &Path, name: &str) -> i32 {
    let base = base_dir(config_path);
    let socket_path = base.join(".don").join("don.sock");
    match don::client::attach::run_attach(&socket_path, name).await {
        Ok(()) => {
            println!("\r\ndetached from '{name}'");
            0
        }
        Err(e) => {
            errln(e);
            1
        }
    }
}

fn print_status_table(items: &[ItemStatus], verbose: bool, show_watch_paths: bool) {
    if items.is_empty() {
        println!("(no services or tasks)");
        return;
    }
    // Compute column widths.
    let kind_w = "KIND".len().max(
        items
            .iter()
            .map(|i| match i {
                ItemStatus::Service { .. } => "service".len(),
                ItemStatus::Task { .. } => "task".len(),
            })
            .max()
            .unwrap_or(0),
    );
    let name_w = "NAME".len().max(
        items
            .iter()
            .map(|i| match i {
                ItemStatus::Service { name, .. } | ItemStatus::Task { name, .. } => name.len(),
            })
            .max()
            .unwrap_or(0),
    );

    let state_w = "STATE".len().max(
        items
            .iter()
            .map(|i| match i {
                ItemStatus::Service { state, .. } => service_state_label(*state).len(),
                ItemStatus::Task { state, .. } => task_state_label(*state).len(),
            })
            .max()
            .unwrap_or(0),
    );

    println!(
        "{:<kind_w$}  {:<name_w$}  {:<state_w$}  LAST RUN  RESULT  DURATION",
        "KIND", "NAME", "STATE"
    );
    for item in items {
        let (kind, name, state_str, color, last_run, result, duration, verbose_info) = match item {
            ItemStatus::Service {
                name,
                state,
                verbose,
            } => (
                "service",
                name.as_str(),
                service_state_label(*state),
                service_state_color(*state),
                "-".to_string(),
                "-".to_string(),
                "-".to_string(),
                verbose.as_ref(),
            ),
            ItemStatus::Task {
                name,
                state,
                last_run,
                verbose,
            } => (
                "task",
                name.as_str(),
                task_state_label(*state),
                task_state_color(*state),
                format_last_run_time(last_run.as_ref()),
                format_last_run_result(last_run.as_ref()),
                format_last_run_duration(last_run.as_ref()),
                verbose.as_ref(),
            ),
        };
        println!(
            "{:<kind_w$}  {:<name_w$}  {}{:<state_w$}{}  {:<8}  {:<6}  {}",
            kind,
            name,
            SetForegroundColor(color),
            state_str,
            ResetColor,
            last_run,
            result,
            duration,
        );

        if verbose && let Some(info) = verbose_info {
            print_verbose_info(info, show_watch_paths);
        }
    }
}

fn format_last_run_time(last_run: Option<&TaskRunInfo>) -> String {
    let Some(last_run) = last_run else {
        return "-".to_string();
    };
    format_relative_unix_secs(last_run.finished_at_unix_secs)
}

fn format_last_run_result(last_run: Option<&TaskRunInfo>) -> String {
    match last_run {
        Some(last_run) if last_run.success => "ok".to_string(),
        Some(_) => "failed".to_string(),
        None => "-".to_string(),
    }
}

fn format_last_run_duration(last_run: Option<&TaskRunInfo>) -> String {
    last_run
        .and_then(|run| run.duration_ms)
        .map(format_duration_ms)
        .unwrap_or_else(|| "-".to_string())
}

fn format_relative_unix_secs(timestamp: u64) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(timestamp, |duration| duration.as_secs());
    if timestamp > now.saturating_add(5) {
        return "in the future".to_string();
    }
    let elapsed = now.saturating_sub(timestamp);
    match elapsed {
        0..=4 => "just now".to_string(),
        5..=59 => format!("{elapsed}s ago"),
        60..=3_599 => format!("{}m ago", elapsed / 60),
        3_600..=86_399 => format!("{}h ago", elapsed / 3_600),
        _ => format!("{}d ago", elapsed / 86_400),
    }
}

fn format_duration_ms(duration_ms: u64) -> String {
    if duration_ms < 1_000 {
        format!("{duration_ms}ms")
    } else {
        let seconds = duration_ms / 1_000;
        let tenths = (duration_ms % 1_000) / 100;
        if tenths == 0 {
            format!("{seconds}s")
        } else {
            format!("{seconds}.{tenths}s")
        }
    }
}

/// Print verbose details for a single item, indented under the status line.
#[allow(clippy::print_stdout)]
fn print_verbose_info(info: &don::runner::VerboseInfo, show_watch_paths: bool) {
    let dim = SetAttribute(Attribute::Dim);
    let reset = SetAttribute(Attribute::Reset);

    if let Some(ref cmd) = info.cmd {
        println!("  {dim}cmd:{reset}    {cmd}");
    }
    if !info.depends_on.is_empty() {
        println!("  {dim}deps:{reset}   {}", info.depends_on.join(", "));
    }
    if !info.proxy.is_empty() {
        println!("  {dim}proxy:{reset}  {}", info.proxy.join(", "));
    }
    if let Some(active) = info.proxy_active_connections {
        println!("  {dim}proxy connections:{reset}  {active} active");
    }
    if let Some(ref ready) = info.ready {
        println!("  {dim}ready:{reset}  {ready}");
    }
    if let Some(ref target) = info.bazel_target {
        println!("  {dim}bazel:{reset}  {target}");
    }
    if let Some(ref task) = info.turbo_task {
        println!("  {dim}turbo:{reset}  {task}");
    }
    // When inspecting a single item, expand the full resolved watch path list
    // (these are dynamically resolved for build-tool services, so the count
    // alone hides what's actually being watched). In the all-items view keep it
    // to a count so a large stack stays scannable.
    if show_watch_paths && !info.watch.is_empty() {
        println!(
            "  {dim}watch:{reset}  {}",
            info.watch.first().map(String::as_str).unwrap_or("")
        );
        for pattern in info.watch.iter().skip(1) {
            println!("         {pattern}");
        }
    } else if info.watch_count > 0 {
        println!("  {dim}watch:{reset}  {} paths", info.watch_count);
    }
    if let Some(ref watch_state) = info.watch_state {
        println!("  {dim}watch state:{reset}  {watch_state}");
    }
    if !info.watch_notes.is_empty() {
        println!("  {dim}watch diag:{reset}   {}", info.watch_notes[0]);
        for note in info.watch_notes.iter().skip(1) {
            println!("               {note}");
        }
    }
}

fn service_state_label(s: ServiceState) -> &'static str {
    match s {
        ServiceState::Pending => "pending",
        ServiceState::Building => "building",
        ServiceState::Lazy => "lazy",
        ServiceState::Starting => "starting",
        ServiceState::Running => "running",
        ServiceState::Ready => "ready",
        ServiceState::Unhealthy => "unhealthy",
        ServiceState::Stopping => "stopping",
        ServiceState::Stopped => "stopped",
        ServiceState::Failed => "failed",
        ServiceState::DependencyFailed => "dep failed",
    }
}

fn service_state_color(s: ServiceState) -> Color {
    match s {
        ServiceState::Ready | ServiceState::Running => Color::Green,
        ServiceState::Starting
        | ServiceState::Building
        | ServiceState::Pending
        | ServiceState::Stopping => Color::Yellow,
        ServiceState::Lazy => Color::Cyan,
        ServiceState::Stopped => Color::DarkGrey,
        ServiceState::Unhealthy => Color::Red,
        ServiceState::Failed => Color::Red,
        // Dim red: same hue family as Failed so the user sees it's in the
        // error-neighbourhood, but visually quieter than the culprit above it.
        ServiceState::DependencyFailed => Color::DarkRed,
    }
}

fn task_state_label(s: TaskItemState) -> &'static str {
    match s {
        TaskItemState::Pending => "pending",
        TaskItemState::Building => "building",
        TaskItemState::Running => "running",
        TaskItemState::Completed => "completed",
        TaskItemState::Skipped => "skipped",
        TaskItemState::Failed => "failed",
        TaskItemState::DependencyFailed => "dep failed",
        TaskItemState::PendingRun => "pending_run",
    }
}

fn task_state_color(s: TaskItemState) -> Color {
    match s {
        TaskItemState::Completed | TaskItemState::Skipped => Color::Green,
        TaskItemState::Running | TaskItemState::Pending | TaskItemState::Building => Color::Yellow,
        TaskItemState::PendingRun => Color::Cyan,
        TaskItemState::Failed => Color::Red,
        TaskItemState::DependencyFailed => Color::DarkRed,
    }
}

async fn run_cleanup_command(config_path: &std::path::Path, force: bool) -> i32 {
    let base = base_dir(config_path);
    let don_dir = base.join(".don");
    let _ = std::fs::create_dir_all(&don_dir);

    // Acquire the PID file lock so we don't race with a running daemon.
    let don_pid_path = don_dir.join("don.pid");
    let pid_lock = match don::process::pid_file::PidFile::acquire(
        don_pid_path.clone(),
        std::process::id() as i32,
    )
    .await
    {
        Ok(lock) => lock,
        Err(don::process::pid_file::PidFileError::AlreadyLocked) => {
            if !force {
                println!("don daemon is running — nothing to clean up (use --force to kill it)");
                return 0;
            }
            // --force: read the running daemon's PID and kill it.
            errln("killing running don daemon...");
            if let Err(e) = kill_running_daemon(&don_pid_path).await {
                errln(format!("failed to kill daemon: {e}"));
                return 1;
            }
            // Now re-acquire the lock.
            match don::process::pid_file::PidFile::acquire(don_pid_path, std::process::id() as i32)
                .await
            {
                Ok(lock) => lock,
                Err(e) => {
                    errln(format!("failed to acquire pid lock after kill: {e}"));
                    return 1;
                }
            }
        }
        Err(e) => {
            errln(format!("failed to acquire pid lock: {e}"));
            return 1;
        }
    };

    // Load config to discover docker container names. If config doesn't
    // exist or is invalid, still clean up what we can (pid files and socket).
    let docker_names: Vec<String> = match don::config::Config::from_file(config_path) {
        Ok(config) => config
            .services
            .iter()
            .filter_map(|(name, svc)| {
                if let Some(don::config::ServiceKind::Docker(d)) = &svc.kind {
                    Some(d.container.clone().unwrap_or_else(|| format!("don-{name}")))
                } else {
                    None
                }
            })
            .collect(),
        Err(e) => {
            errln(format!(
                "Warning: could not load config for docker cleanup: {e}"
            ));
            vec![]
        }
    };

    let report = don::process::cleanup::run_cleanup(&base, &docker_names).await;
    println!("{report}");
    for warning in &report.warnings {
        errln(format!("Warning: {warning}"));
    }

    // Hold lock until cleanup finishes, then release.
    drop(pid_lock);
    0
}

/// Read the PID from don.pid, send two SIGINTs (triggering the daemon's
/// own two-signal shutdown protocol: first = graceful, second = force SIGKILL
/// on all children), then wait for the process to exit.
async fn kill_running_daemon(pid_path: &std::path::Path) -> Result<(), String> {
    let content = std::fs::read_to_string(pid_path)
        .map_err(|e| format!("failed to read {}: {e}", pid_path.display()))?;
    let pid: i32 = content.trim().parse().map_err(|_| {
        format!(
            "invalid pid in {}: '{}'",
            pid_path.display(),
            content.trim()
        )
    })?;

    let nix_pid = nix::unistd::Pid::from_raw(pid);

    // First SIGINT — triggers graceful shutdown.
    if nix::sys::signal::kill(nix_pid, nix::sys::signal::Signal::SIGINT).is_err() {
        return Ok(()); // Already dead.
    }

    // Brief pause so the daemon registers the first signal and enters shutdown.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Second SIGINT — sets the force flag, daemon SIGKILLs all children.
    let _ = nix::sys::signal::kill(nix_pid, nix::sys::signal::Signal::SIGINT);

    // Wait for the daemon to actually exit (up to 1s — it needs time to
    // reap children after SIGKILL).
    for _ in 0..10 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        if nix::sys::signal::kill(nix_pid, None).is_err() {
            return Ok(()); // Process is gone.
        }
    }

    // Last resort — the daemon itself is stuck.
    errln("daemon did not exit after 1s, sending SIGKILL");
    let _ = nix::sys::signal::kill(nix_pid, nix::sys::signal::Signal::SIGKILL);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    Ok(())
}

fn validate(config_path: &std::path::Path) -> Result<(), String> {
    let config = don::config::Config::from_file(config_path).map_err(|e| format!("Error: {e}"))?;

    let platform = don::config::Platform::current().ok_or_else(|| {
        format!(
            "Unsupported platform: {}-{}",
            std::env::consts::OS,
            std::env::consts::ARCH
        )
    })?;

    let warnings = config
        .validate(platform)
        .map_err(|e| format!("Error: {e}"))?;
    for warning in &warnings {
        errln(format!("Warning: {warning}"));
    }
    Ok(())
}

/// Lightweight Levenshtein-style typo suggestion for CLI-supplied names
/// (`--log-filter` entries, `status <name>`, …). Returns
/// ` — did you mean '<best>'?` or empty if nothing close is found. Mirrors the
/// shape used by `config::suggest_typo` so error messages read the same on both
/// surfaces; kept local to avoid widening the config module's public API for a
/// CLI-side caller.
fn suggest_name_typo(input: &str, candidates: &std::collections::HashSet<&str>) -> String {
    fn distance(a: &str, b: &str) -> usize {
        let a: Vec<char> = a.chars().collect();
        let b: Vec<char> = b.chars().collect();
        let mut prev: Vec<usize> = (0..=b.len()).collect();
        let mut curr = vec![0usize; b.len() + 1];
        for i in 1..=a.len() {
            curr[0] = i;
            for j in 1..=b.len() {
                let cost = usize::from(a[i - 1] != b[j - 1]);
                curr[j] = (prev[j] + 1).min(curr[j - 1] + 1).min(prev[j - 1] + cost);
            }
            std::mem::swap(&mut prev, &mut curr);
        }
        prev[b.len()]
    }
    let max = match input.len() {
        0..=2 => 1,
        3..=5 => 2,
        _ => 3,
    };
    let mut best: Option<(&str, usize)> = None;
    for &cand in candidates {
        let d = distance(input, cand);
        if d > 0 && d <= max && best.is_none_or(|(_, bd)| d < bd) {
            best = Some((cand, d));
        }
    }
    best.map(|(n, _)| format!(" — did you mean '{n}'?"))
        .unwrap_or_default()
}

async fn run_start_detached(
    config_path: &Path,
    profile: Option<&str>,
    verbose: bool,
    log_filter: Vec<String>,
) -> Result<(), String> {
    let base = base_dir(config_path);
    let client = Client::new(&base);
    match client.status(false, None).await {
        Ok(_) => return Err("don daemon is already running".to_string()),
        Err(ClientError::NotRunning { .. }) => {}
        Err(e) => return Err(format!("failed to check daemon status: {e}")),
    }

    let log_path = base.join(".don").join("logs").join("detached.log");
    if let Some(parent) = log_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create {}: {e}", parent.display()))?;
    }
    append_detached_log_header(&log_path)?;

    let exe = std::env::current_exe().map_err(|e| format!("failed to locate don binary: {e}"))?;
    let stdout = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .map_err(|e| format!("failed to open {}: {e}", log_path.display()))?;
    let stderr = stdout
        .try_clone()
        .map_err(|e| format!("failed to clone {}: {e}", log_path.display()))?;

    let mut cmd = std::process::Command::new(exe);
    cmd.arg("--config")
        .arg(config_path)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::from(stdout))
        .stderr(std::process::Stdio::from(stderr));
    if verbose {
        cmd.arg("--verbose");
    }
    cmd.arg("start").arg("--no-tui");
    if let Some(profile_name) = profile {
        cmd.arg("--profile").arg(profile_name);
    }
    if !log_filter.is_empty() {
        cmd.arg("--log-filter").arg(log_filter.join(","));
    }

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // Run the daemon in a new session so terminal-generated signals for
        // the parent shell do not also hit the detached don process.
        unsafe {
            cmd.pre_exec(|| {
                if nix::libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }

    let mut child = cmd
        .spawn()
        .map_err(|e| format!("failed to spawn detached don: {e}"))?;
    let pid = child.id();
    wait_for_detached_start(&mut child, &base, &log_path, pid).await
}

fn append_detached_log_header(log_path: &Path) -> Result<(), String> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .map_err(|e| format!("failed to open {}: {e}", log_path.display()))?;
    writeln!(file, "\n--- don start --detached ---")
        .map_err(|e| format!("failed to write {}: {e}", log_path.display()))?;
    Ok(())
}

async fn wait_for_detached_start(
    child: &mut std::process::Child,
    base: &Path,
    log_path: &Path,
    pid: u32,
) -> Result<(), String> {
    let client = Client::new(base);
    let start = tokio::time::Instant::now();
    let timeout = std::time::Duration::from_secs(5);

    loop {
        match client.status(false, None).await {
            Ok(_) => {
                println!(
                    "don started in background (pid {pid}, log: {})",
                    log_path.display()
                );
                return Ok(());
            }
            Err(ClientError::NotRunning { .. }) => {}
            Err(_) => {}
        }

        match child
            .try_wait()
            .map_err(|e| format!("failed to check detached don process: {e}"))?
        {
            Some(status) => {
                let status_text = match status.code() {
                    Some(code) => format!("exit code {code}"),
                    None => "terminated by signal".to_string(),
                };
                let tail = read_log_tail(log_path, 20);
                let mut msg = format!(
                    "detached don exited before the daemon was ready ({status_text}); log: {}",
                    log_path.display()
                );
                if !tail.is_empty() {
                    msg.push_str("\n\n");
                    msg.push_str(&tail);
                }
                return Err(msg);
            }
            None if start.elapsed() >= timeout => {
                println!(
                    "don started in background (pid {pid}, still initializing; log: {})",
                    log_path.display()
                );
                return Ok(());
            }
            None => {}
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

fn read_log_tail(path: &Path, max_lines: usize) -> String {
    let Ok(content) = std::fs::read_to_string(path) else {
        return String::new();
    };
    let mut lines: Vec<&str> = content.lines().rev().take(max_lines).collect();
    lines.reverse();
    lines.join("\n")
}

async fn run_start(
    config_path: &std::path::Path,
    profile: Option<&str>,
    verbose: bool,
    no_tui: bool,
    log_filter: Vec<String>,
) -> Result<(), String> {
    use std::io::IsTerminal;

    let config = don::config::Config::from_file(config_path).map_err(|e| format!("Error: {e}"))?;

    let platform = don::config::Platform::current().ok_or_else(|| {
        format!(
            "Unsupported platform: {}-{}",
            std::env::consts::OS,
            std::env::consts::ARCH
        )
    })?;

    let warnings = config
        .validate(platform)
        .map_err(|e| format!("Error: {e}"))?;
    for warning in &warnings {
        errln(format!("Warning: {warning}"));
    }

    let base = base_dir(config_path);
    let is_tty = !no_tui && std::io::stdout().is_terminal();

    // Fall back to the config's default_profile when `--profile` is not given.
    // Validation above guarantees default_profile (if set) is a known profile,
    // so the lookup below cannot miss. Own the string so later code can move
    // `config` into `Runner::new` while we still hold the profile name.
    let profile: Option<String> = profile
        .map(str::to_string)
        .or_else(|| config.default_profile.clone());
    let profile_ref: Option<&str> = profile.as_deref();

    // Resolve the active item set up front so the output manager and TUI
    // only see items that will actually run. Without this, prefix padding
    // is sized for the longest name in the whole config, and the TUI
    // service menu lists items the profile excludes. The runner re-runs
    // this inside `Runner::new` to build its own filtered state.
    let active_items: Option<std::collections::HashSet<String>> =
        if let Some(profile_name) = profile_ref {
            let prof = config
                .profiles
                .get(profile_name)
                .ok_or_else(|| format!("Error: unknown profile '{profile_name}'"))?;
            Some(don::runner::resolve_profile_items(&config, prof))
        } else {
            None
        };

    let is_active = |name: &str| active_items.as_ref().is_none_or(|s| s.contains(name));
    let has_foreground_tasks = config
        .tasks
        .iter()
        .any(|(name, task)| is_active(name) && task.terminal.is_foreground());

    // Collect service names and their log configs for OutputManager.
    let service_configs: Vec<(&str, &don::config::LogConfig)> = config
        .services
        .iter()
        .filter(|(name, _)| is_active(name))
        .map(|(name, svc)| (name.as_str(), &svc.log))
        .collect();

    // Also include tasks in the output manager so they get prefixed output.
    let task_configs: Vec<(&str, &don::config::LogConfig)> = config
        .tasks
        .iter()
        .filter(|(name, _)| is_active(name))
        .map(|(name, task)| (name.as_str(), &task.log))
        .collect();

    // Synthetic build-tool stream names should participate in the initial
    // output palette so color choice and prefix width depend only on the
    // config-derived item set, not on later registration order.
    let service_kinds = || {
        config.services.values().flat_map(|svc| {
            std::iter::once(svc.kind.as_ref())
                .chain(svc.platform.values().map(|ov| ov.kind.as_ref()))
                .flatten()
        })
    };
    let uses_bazel = service_kinds().any(|k| matches!(k, don::config::ServiceKind::Bazel(_)))
        || config.tasks.values().any(|t| t.bazel.is_some());
    let uses_turbo = service_kinds().any(|k| matches!(k, don::config::ServiceKind::Turbo(_)))
        || config.tasks.values().any(|t| t.turbo.is_some());
    let build_tool_log = don::config::LogConfig::Stdout;
    let build_tool_configs: Vec<(&str, &don::config::LogConfig)> = [
        uses_bazel.then_some(("bazel", &build_tool_log)),
        uses_turbo.then_some(("turbo", &build_tool_log)),
    ]
    .into_iter()
    .flatten()
    .collect();

    let all_configs: Vec<(&str, &don::config::LogConfig)> = service_configs
        .into_iter()
        .chain(task_configs)
        .chain(build_tool_configs.iter().copied())
        .collect();

    let mut log_keep_filters: std::collections::HashMap<String, don::config::LogFilterConfig> =
        std::collections::HashMap::new();
    for (name, svc) in config.services.iter().filter(|(name, _)| is_active(name)) {
        let effective = config
            .log_filter
            .merged_with(&svc.resolve(platform).log_filter);
        if !effective.is_empty() {
            log_keep_filters.insert(name.clone(), effective);
        }
    }
    for name in config.tasks.keys().filter(|name| is_active(name)) {
        if !config.log_filter.is_empty() {
            log_keep_filters.insert(name.clone(), config.log_filter.clone());
        }
    }
    for (name, _) in &build_tool_configs {
        if !config.log_filter.is_empty() {
            log_keep_filters.insert((*name).to_string(), config.log_filter.clone());
        }
    }

    // Validate `--log-filter` against the active item set so typos surface
    // before the runner spawns anything. The synthetic `[don]` lifecycle
    // entry is implicitly allowed everywhere — `don::output` keeps it
    // visible regardless of the allowlist — so accepting "don" here is a
    // harmless no-op rather than an error.
    let log_filter_set: Option<std::collections::HashSet<String>> = if log_filter.is_empty() {
        None
    } else {
        let valid: std::collections::HashSet<&str> = all_configs
            .iter()
            .map(|(name, _)| *name)
            .chain(std::iter::once(don::output::LIFECYCLE_EVENT_NAME))
            .collect();
        let mut invalid: Vec<(String, String)> = Vec::new();
        for name in &log_filter {
            if !valid.contains(name.as_str()) {
                invalid.push((name.clone(), suggest_name_typo(name, &valid)));
            }
        }
        if !invalid.is_empty() {
            let msg = invalid
                .iter()
                .map(|(n, s)| format!("'{n}'{s}"))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(format!(
                "Error: --log-filter references unknown service or task: {msg}"
            ));
        }
        Some(log_filter.into_iter().collect())
    };

    // Install signal handlers before building the runner so Ctrl+C still
    // reaches the graceful-shutdown path even during a slow startup.
    let shutdown_rx = don::runner::install_signal_handlers()
        .await
        .map_err(|e| format!("Error installing signal handlers: {e}"))?;

    let _ = has_foreground_tasks; // logged earlier; TUI now handles fg tasks via pause/resume

    if is_tty {
        let (output_manager, log_rx) = don::output::OutputManager::new_with_tui_and_log_filters(
            &all_configs,
            &log_keep_filters,
            verbose,
        )
        .await
        .map_err(|e| format!("Error creating output manager: {e}"))?;
        let verbosity = output_manager.verbosity_control();
        let lifecycle_emitter = output_manager.clone_lifecycle_emitter();

        // Channel that lets the runner ask the TUI to release/re-take the
        // terminal when a foreground task is about to run.
        let (terminal_request_tx, terminal_request_rx) = tokio::sync::mpsc::channel(8);
        let terminal_coordinator =
            don::runner::TerminalCoordinator::with_channel(terminal_request_tx);

        let service_names: Vec<String> = config
            .services
            .keys()
            .filter(|name| is_active(name))
            .cloned()
            .collect();
        let task_names: Vec<String> = config
            .tasks
            .keys()
            .filter(|name| is_active(name))
            .cloned()
            .collect();

        // Snapshot the task configs before moving `config` into the runner —
        // the TUI form needs the param schema to render prompts and to route
        // per-param completion requests.
        let task_configs: std::collections::HashMap<String, don::config::Task> =
            config.tasks.clone();
        let task_state = don::TaskState::new(base.join(".don").join("task-state"));
        let mut task_last_runs = std::collections::HashMap::new();
        for name in &task_names {
            if let Ok(Some(last_run)) = task_state.last_run(name).await {
                task_last_runs.insert(name.clone(), last_run);
            }
        }

        // Synthetic build-tool stream names that should appear in the TUI
        // filter. Without these entries, lines emitted by the bazel/turbo
        // clients (which carry `name = "bazel"` / `"turbo"`) are silently
        // dropped by the filter's allowlist — the user sees nothing during
        // the build phase.
        let build_tool_names: Vec<String> = build_tool_configs
            .iter()
            .map(|(name, _)| (*name).to_string())
            .collect();

        // Collect names whose `hidden = true` flag should start them outside
        // the TUI filter's default selection. Both services and tasks can
        // opt in — the filter treats them identically.
        let hidden_names: std::collections::HashSet<String> = config
            .services
            .iter()
            .filter(|(name, svc)| is_active(name) && svc.hidden)
            .map(|(name, _)| name.clone())
            .chain(
                config
                    .tasks
                    .iter()
                    .filter(|(name, task)| is_active(name) && task.hidden)
                    .map(|(name, _)| name.clone()),
            )
            .collect();

        let auto_filter_on_failure_names: std::collections::HashSet<String> = config
            .services
            .iter()
            .filter(|(name, svc)| {
                is_active(name)
                    && svc
                        .resolve(platform)
                        .auto_filter_on_failure
                        .unwrap_or(config.auto_filter_on_failure)
            })
            .map(|(name, _)| name.clone())
            .chain(
                config
                    .tasks
                    .iter()
                    .filter(|(name, task)| {
                        is_active(name)
                            && task
                                .auto_filter_on_failure
                                .unwrap_or(config.auto_filter_on_failure)
                    })
                    .map(|(name, _)| name.clone()),
            )
            .collect();

        let runner = await_with_shutdown_supervision(
            tokio::spawn({
                let profile = profile.clone();
                async move {
                    don::runner::Runner::new(
                        config,
                        platform,
                        output_manager,
                        base,
                        profile.as_deref(),
                        shutdown_rx,
                        terminal_coordinator,
                    )
                    .await
                    .map_err(|e| format!("Error: {e}"))
                }
            }),
            "starting runner",
        )
        .await?;

        let events = runner.subscribe();
        let commands = runner.command_sender();

        // Wrap the TUI so that if it exits unexpectedly (e.g. a terminal IO
        // error or panic), we signal the runner to shut down instead of
        // leaving the daemon alive while the user's terminal is in cooked
        // mode. Without this, raw mode gets disabled but logs keep streaming —
        // the user sees a free-floating cursor and can type into the shell
        // while the runner runs unattended. The log_rx closing (runner
        // shutdown) returns Ok(()) so the normal exit path is unaffected.
        let tui_log_filter = log_filter_set.clone();
        let tui = tokio::spawn(async move {
            let result = don::run_tui(
                log_rx,
                events,
                commands,
                verbosity,
                lifecycle_emitter,
                service_names,
                task_names,
                build_tool_names,
                task_configs,
                task_last_runs,
                hidden_names,
                auto_filter_on_failure_names,
                tui_log_filter,
                terminal_request_rx,
            )
            .await;
            if result.is_err() {
                let _ = nix::sys::signal::kill(
                    nix::unistd::Pid::this(),
                    nix::sys::signal::Signal::SIGINT,
                );
            }
            result
        });

        let runner_task =
            tokio::spawn(async move { runner.run().await.map_err(|e| format!("Error: {e}")) });
        let runner_result =
            await_with_shutdown_supervision(runner_task, "waiting for runner shutdown").await;
        if runner_result.is_err() {
            tui.abort();
        }

        // Surface any TUI error so unexpected exits are visible instead of
        // silently dropped. Runner errors take precedence.
        match tui.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => errln(format!("TUI error: {e}")),
            Err(join_err) if join_err.is_panic() => {
                errln(format!("TUI task panicked: {join_err}"));
            }
            Err(_) => {} // cancelled — expected on shutdown
        }

        runner_result
    } else {
        let output_manager = don::output::OutputManager::new_verbose_with_log_filters(
            &all_configs,
            &log_keep_filters,
            tokio::io::stdout(),
            verbose,
        )
        .await
        .map_err(|e| format!("Error creating output manager: {e}"))?;

        if let Some(allow) = log_filter_set {
            output_manager.set_log_filter(allow);
        }

        let runner = await_with_shutdown_supervision(
            tokio::spawn({
                let profile = profile.clone();
                async move {
                    don::runner::Runner::new(
                        config,
                        platform,
                        output_manager,
                        base,
                        profile.as_deref(),
                        shutdown_rx,
                        don::runner::TerminalCoordinator::detached(),
                    )
                    .await
                    .map_err(|e| format!("Error: {e}"))
                }
            }),
            "starting runner",
        )
        .await?;

        let runner_task =
            tokio::spawn(async move { runner.run().await.map_err(|e| format!("Error: {e}")) });
        await_with_shutdown_supervision(runner_task, "waiting for runner shutdown").await
    }
}

async fn await_with_shutdown_supervision<T>(
    mut handle: tokio::task::JoinHandle<Result<T, String>>,
    phase: &str,
) -> Result<T, String>
where
    T: Send + 'static,
{
    // Process-level shutdown supervision lives outside the runner. The runner
    // gets the first chance to unwind cleanly. `main` only force-aborts the
    // runner task if a second Ctrl+C arrives, mirroring the daemon's own
    // two-signal shutdown semantics.
    let poll_interval = std::time::Duration::from_millis(100);

    loop {
        if don::runner::signal_count() >= 2 {
            errln(format!("forcing exit while {phase}"));
            handle.abort();
            let _ = handle.await;
            return Err(format!("forced exit while {phase}"));
        }

        tokio::select! {
            result = &mut handle => return map_join_result(result, phase),
            _ = tokio::time::sleep(poll_interval) => {}
        }
    }
}

fn map_join_result<T>(
    result: Result<Result<T, String>, tokio::task::JoinError>,
    phase: &str,
) -> Result<T, String> {
    match result {
        Ok(inner) => inner,
        Err(join_err) if join_err.is_cancelled() => Err(format!("cancelled while {phase}")),
        Err(join_err) => Err(format!("task failed while {phase}: {join_err}")),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::{item_name, parse_task_args, split_run_flags, status_sort_bucket};
    use don::config::{ParamKind, TaskParam};
    use don::runner::{ItemStatus, ServiceState, TaskItemState};

    fn p(name: &str) -> TaskParam {
        TaskParam {
            name: name.to_string(),
            prompt: None,
            required: false,
            default: None,
            kind: ParamKind::String,
            choices: vec![],
            completions: None,
            validate: None,
        }
    }

    fn service(name: &str, state: ServiceState) -> ItemStatus {
        ItemStatus::Service {
            name: name.to_string(),
            state,
            verbose: None,
        }
    }

    fn task(name: &str, state: TaskItemState) -> ItemStatus {
        ItemStatus::Task {
            name: name.to_string(),
            state,
            last_run: None,
            verbose: None,
        }
    }

    #[test]
    fn count_tasks_pending_run_counts_only_pending_tasks() {
        let items = vec![
            service("svc-ready", ServiceState::Ready),
            task("task-pending-a", TaskItemState::PendingRun),
            task("task-pending-b", TaskItemState::PendingRun),
            task("task-completed", TaskItemState::Completed),
            task("task-failed", TaskItemState::Failed),
        ];
        assert_eq!(super::count_tasks_pending_run(&items), 2);

        let none = vec![
            service("svc-ready", ServiceState::Ready),
            task("task-completed", TaskItemState::Completed),
        ];
        assert_eq!(super::count_tasks_pending_run(&none), 0);

        assert_eq!(super::count_tasks_pending_run(&[]), 0);
    }

    #[test]
    fn status_sort_prioritizes_actionable_states() {
        struct Case {
            name: &'static str,
            items: Vec<ItemStatus>,
            want: Vec<&'static str>,
        }

        let cases = vec![Case {
            name: "mixed services and tasks",
            items: vec![
                service("svc-ready", ServiceState::Ready),
                service("svc-building", ServiceState::Building),
                service("svc-running", ServiceState::Running),
                service("svc-stopped", ServiceState::Stopped),
                service("svc-lazy", ServiceState::Lazy),
                service("svc-failed", ServiceState::Failed),
                service("svc-dep", ServiceState::DependencyFailed),
                service("svc-stopping", ServiceState::Stopping),
                task("task-skipped", TaskItemState::Skipped),
                task("task-completed", TaskItemState::Completed),
                task("task-building", TaskItemState::Building),
                task("task-pending-run", TaskItemState::PendingRun),
                task("task-failed", TaskItemState::Failed),
                task("task-dep", TaskItemState::DependencyFailed),
            ],
            want: vec![
                "svc-failed",
                "task-failed",
                "svc-dep",
                "task-dep",
                "svc-building",
                "task-building",
                "svc-running",
                "svc-ready",
                "svc-stopping",
                "svc-stopped",
                "svc-lazy",
                "task-pending-run",
                "task-completed",
                "task-skipped",
            ],
        }];

        for mut case in cases {
            case.items.sort_by(|a, b| {
                status_sort_bucket(a)
                    .cmp(&status_sort_bucket(b))
                    .then_with(|| item_name(a).cmp(item_name(b)))
            });
            let got: Vec<&str> = case.items.iter().map(item_name).collect();
            assert_eq!(got, case.want, "case: {}", case.name);
        }
    }

    #[test]
    fn parse_table() {
        struct Case {
            name: &'static str,
            params: Vec<TaskParam>,
            raw: Vec<&'static str>,
            want_ok: Option<Vec<(&'static str, &'static str)>>,
            want_err: Option<&'static str>,
        }

        let cases = vec![
            Case {
                name: "empty args",
                params: vec![],
                raw: vec![],
                want_ok: Some(vec![]),
                want_err: None,
            },
            Case {
                name: "key=value",
                params: vec![p("index")],
                raw: vec!["--index=users"],
                want_ok: Some(vec![("index", "users")]),
                want_err: None,
            },
            Case {
                name: "separated",
                params: vec![p("index")],
                raw: vec!["--index", "users"],
                want_ok: Some(vec![("index", "users")]),
                want_err: None,
            },
            Case {
                name: "mixed",
                params: vec![p("a"), p("b")],
                raw: vec!["--a=1", "--b", "two"],
                want_ok: Some(vec![("a", "1"), ("b", "two")]),
                want_err: None,
            },
            Case {
                name: "bool bare flag",
                params: vec![TaskParam {
                    kind: ParamKind::Bool,
                    ..p("enabled")
                }],
                raw: vec!["--enabled"],
                want_ok: Some(vec![("enabled", "true")]),
                want_err: None,
            },
            Case {
                name: "bool explicit value",
                params: vec![TaskParam {
                    kind: ParamKind::Bool,
                    ..p("enabled")
                }],
                raw: vec!["--enabled=false"],
                want_ok: Some(vec![("enabled", "false")]),
                want_err: None,
            },
            Case {
                name: "unknown flag",
                params: vec![p("a")],
                raw: vec!["--b=1"],
                want_ok: None,
                want_err: Some("unknown param '--b'"),
            },
            Case {
                name: "missing value",
                params: vec![p("a")],
                raw: vec!["--a"],
                want_ok: None,
                want_err: Some("missing a value"),
            },
            Case {
                name: "positional rejected",
                params: vec![p("a")],
                raw: vec!["stray"],
                want_ok: None,
                want_err: Some("unexpected positional"),
            },
            Case {
                name: "value that looks like a flag consumed by separated form errors",
                params: vec![p("a")],
                raw: vec!["--a", "--b"],
                want_ok: None,
                want_err: Some("missing a value"),
            },
            Case {
                name: "value starting with dash via equals form is fine",
                params: vec![p("a")],
                raw: vec!["--a=-x"],
                want_ok: Some(vec![("a", "-x")]),
                want_err: None,
            },
        ];

        for case in cases {
            let raw: Vec<String> = case.raw.iter().map(|s| s.to_string()).collect();
            let got = parse_task_args(&raw, &case.params);
            match (got, case.want_ok, case.want_err) {
                (Ok(m), Some(want), None) => {
                    let want_map: std::collections::HashMap<String, String> = want
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect();
                    assert_eq!(m, want_map, "{}", case.name);
                }
                (Err(e), None, Some(needle)) => {
                    assert!(
                        e.contains(needle),
                        "{}: err '{e}' missing '{needle}'",
                        case.name
                    );
                }
                (got, ok, err) => panic!(
                    "{}: got {:?}, want ok={:?} err={:?}",
                    case.name, got, ok, err
                ),
            }
        }
    }

    #[test]
    fn split_run_flags_accepts_flags_after_task_name() {
        let raw = vec![
            "--wait".to_string(),
            "--timeout".to_string(),
            "2s".to_string(),
            "--index=users".to_string(),
        ];
        let (wait, timeout, params) = split_run_flags(&raw, false, None).unwrap();
        assert!(wait);
        assert_eq!(timeout.as_deref(), Some("2s"));
        assert_eq!(params, vec!["--index=users"]);
    }

    #[test]
    fn split_run_flags_timeout_implies_wait() {
        let raw = vec!["--timeout=100ms".to_string(), "--dry_run".to_string()];
        let (wait, timeout, params) = split_run_flags(&raw, false, None).unwrap();
        assert!(wait);
        assert_eq!(timeout.as_deref(), Some("100ms"));
        assert_eq!(params, vec!["--dry_run"]);
    }
}
