// The CLI binary legitimately uses stdout — it IS the user-facing output.
#![allow(clippy::print_stdout)]
#![allow(clippy::print_stderr)]

mod wisdom;

use clap::{Parser, Subcommand};
use crossterm::style::{Attribute, Color, ResetColor, SetAttribute, SetForegroundColor};
use don::TaskRunInfo;
use don::client::{Client, ClientError, RunTaskOptions};
use don::runner::{ProcessStatus, ServiceState, TaskState};
use std::borrow::Cow;
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
        /// Internal: set by the fork-model parent so the runner child knows
        /// an interactive client is attaching (disables headless task
        /// overrides).
        #[arg(long, hide = true)]
        attached: bool,
        /// Restrict displayed log lines to the given comma-separated set of
        /// service/task names. Affects pipe-mode output and seeds the TUI
        /// filter on startup (overriding any `hidden = true` defaults).
        /// Ring buffers and file sinks are unaffected — `don logs <name>`
        /// still returns full output. Example: `--log-filter=web,api,db`.
        #[arg(long, value_delimiter = ',')]
        log_filter: Vec<String>,
        /// Don't announce this project to the system-wide don daemon, so it
        /// won't appear in the web UI. Registration is best-effort and never
        /// affects the stack itself; this flag is for when you'd rather the
        /// daemon not know about a project at all.
        #[arg(long)]
        no_daemon: bool,
        /// Serve a web UI for this project from this process, instead of
        /// relying on a system-wide daemon. Implies `--no-daemon`: the UI
        /// shows only this project and lives exactly as long as it does.
        /// Defaults to port 3667 (the daemon owns 3666); pass
        /// `--with-ui=PORT` to choose another.
        #[arg(
            long,
            value_name = "PORT",
            num_args = 0..=1,
            default_missing_value = "3667",
            conflicts_with = "name"
        )]
        with_ui: Option<u16>,
    },
    /// Run or manage the system-wide daemon that serves the web UI
    ///
    /// The daemon is a broker: it tracks which don projects are running and
    /// serves a UI over them. It never owns your services, so stopping it
    /// leaves every running stack untouched.
    Daemon {
        /// Port to serve the web UI on. Use 0 to let the OS pick one.
        #[arg(long, env = "DON_UI_PORT", default_value_t = don::web::DEFAULT_PORT)]
        port: u16,
        /// Run the control plane without a web UI. Useful for debugging
        /// registration on its own.
        #[arg(long, conflicts_with = "port")]
        no_web: bool,
        #[command(subcommand)]
        command: Option<DaemonCommands>,
    },
    /// Open the don web UI in your browser
    Ui {
        /// Print the URL instead of opening a browser
        #[arg(long)]
        print: bool,
    },
    /// Stop this project's stack, or one running service when a name is given
    ///
    /// This is the project you're in, not the system-wide daemon — use
    /// `don daemon stop` for that.
    Stop {
        /// Name of the service to stop (omit to stop the whole stack)
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
        /// the headless equivalent of the TUI's `*` flag), and an `processes`
        /// array. Useful for scripts and agents polling for stack readiness.
        #[arg(long)]
        json: bool,
    },
    /// Show the proxy and Docker addresses Don actually bound
    Ports {
        /// Emit the runtime port manifest as machine-readable JSON
        #[arg(long)]
        json: bool,
    },
    /// Show everything don is currently watching: the inotify directories it has
    /// registered plus the per-process glob patterns that trigger reloads. Useful
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
    /// Attach the full TUI to an already-running project. Ctrl+D detaches
    /// (the stack keeps running); Ctrl+C requests a graceful shutdown.
    Tui,
    /// Clean up stale state from a previous run
    Cleanup {
        /// Kill a running daemon first, then clean up
        #[arg(long)]
        force: bool,
    },
    /// Run a task (bypasses auto_run)
    Run {
        /// Name of the task to run
        name: String,
        /// Never prompt for missing required params — error instead. Implicit
        /// when stdin isn't a TTY. Useful in scripts / CI.
        #[arg(long)]
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
        /// One of: services, tasks, processes, profiles
        kind: String,
    },
}

#[derive(Subcommand, Debug)]
enum DaemonCommands {
    /// Show whether a daemon is running and which projects it knows about
    Status {
        /// Emit machine-readable JSON instead of the human-readable report
        #[arg(long)]
        json: bool,
    },
    /// Stop the running daemon. Registered projects keep running.
    ///
    /// When the daemon is installed as a service this goes through the
    /// supervisor, so it stays stopped instead of being restarted underneath
    /// you.
    Stop,
    /// Restart the daemon, e.g. to pick up an upgraded `don` binary
    ///
    /// Requires the daemon to be installed as a service — there's nothing to
    /// restart a bare foreground `don daemon` with.
    Restart,
    /// Install the daemon as a per-user service so it starts on login
    Install {
        /// Port the installed service should serve the web UI on
        #[arg(long, default_value_t = don::web::DEFAULT_PORT)]
        port: u16,
        /// Print the service file that would be written, without touching
        /// anything
        #[arg(long)]
        dry_run: bool,
    },
    /// Stop the installed service and remove it
    Uninstall,
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
            attached,
            log_filter,
            no_daemon,
            with_ui,
        } => {
            let result = if detached {
                run_start_detached(
                    &config_path,
                    profile.as_deref(),
                    verbose,
                    no_daemon,
                    with_ui,
                )
                .await
            } else if should_auto_attach(&config_path, no_tui) {
                run_start_attached(
                    &config_path,
                    profile.as_deref(),
                    verbose,
                    log_filter,
                    no_daemon,
                    with_ui,
                )
                .await
            } else {
                run_start(
                    &config_path,
                    profile.as_deref(),
                    verbose,
                    no_tui,
                    attached,
                    log_filter,
                    no_daemon,
                    with_ui,
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
        Commands::Daemon {
            port,
            no_web,
            command,
        } => run_daemon_command(port, no_web, command).await,
        Commands::Ui { print } => run_ui(print).await,
        Commands::Ports { json } => run_ports(&config_path, json),
        Commands::Watch { json } => run_watch(&config_path, json).await,
        Commands::Logs { name, last, follow } => run_logs(&config_path, &name, last, follow).await,
        Commands::Attach { name } => run_attach(&config_path, &name).await,
        Commands::Tui => run_attach_tui(&config_path).await,
        Commands::Cleanup { force } => run_cleanup_command(&config_path, force).await,
        Commands::Run {
            name,
            raw,
            no_prompt,
            wait,
            timeout,
        } => run_run_task(&config_path, name, raw, no_prompt, wait, timeout).await,
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

fn run_ports(config_path: &Path, json: bool) -> i32 {
    let base = base_dir(config_path);
    let manifest = match don::ports::read_manifest(&base) {
        Ok(manifest) => manifest,
        Err(don::ports::PortManifestError::Read { source, .. })
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            errln(format!(
                "no runtime port manifest found at {} — start don first",
                don::ports::manifest_path(&base).display()
            ));
            return 1;
        }
        Err(error) => {
            errln(error);
            return 1;
        }
    };

    if json {
        return match serde_json::to_string_pretty(&manifest) {
            Ok(output) => {
                println!("{output}");
                0
            }
            Err(error) => {
                errln(format!("failed to serialize runtime ports: {error}"));
                1
            }
        };
    }

    let mut rows = Vec::new();
    for (service, ports) in manifest.services {
        for proxy in ports.proxy {
            rows.push((
                service.clone(),
                "proxy".to_string(),
                proxy.configured_addr,
                proxy.bound_addr,
                proxy.mode,
            ));
        }
        for docker in ports.docker {
            rows.push((
                service.clone(),
                "docker".to_string(),
                docker.configured,
                docker.host_addr,
                format!("{}/{}", docker.container_port, docker.protocol),
            ));
        }
    }

    if rows.is_empty() {
        println!("No runtime ports.");
        return 0;
    }

    let service_width = rows
        .iter()
        .map(|(service, _, _, _, _)| service.len())
        .max()
        .unwrap_or("SERVICE".len())
        .max("SERVICE".len());
    let kind_width = rows
        .iter()
        .map(|(_, kind, _, _, _)| kind.len())
        .max()
        .unwrap_or("KIND".len())
        .max("KIND".len());
    let configured_width = rows
        .iter()
        .map(|(_, _, configured, _, _)| configured.len())
        .max()
        .unwrap_or("CONFIGURED".len())
        .max("CONFIGURED".len());
    let bound_width = rows
        .iter()
        .map(|(_, _, _, bound, _)| bound.len())
        .max()
        .unwrap_or("BOUND".len())
        .max("BOUND".len());

    println!(
        "{:<service_width$}  {:<kind_width$}  {:<configured_width$}  {:<bound_width$}  DETAIL",
        "SERVICE", "KIND", "CONFIGURED", "BOUND"
    );
    for (service, kind, configured, bound, detail) in rows {
        println!(
            "{service:<service_width$}  {kind:<kind_width$}  \
             {configured:<configured_width$}  {bound:<bound_width$}  {detail}"
        );
    }
    0
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

/// Dispatch `don daemon [status|stop]`. No subcommand runs the daemon in the
/// foreground — that's what the systemd unit / launchd agent execs.
async fn run_daemon_command(port: u16, no_web: bool, command: Option<DaemonCommands>) -> i32 {
    let paths = match don::daemon::DaemonPaths::from_process_env() {
        Ok(paths) => paths,
        Err(e) => {
            errln(format!("Error: {e}"));
            return 1;
        }
    };

    match command {
        None => run_daemon_foreground(paths, port, no_web).await,
        Some(DaemonCommands::Status { json }) => run_daemon_status(paths, json).await,
        Some(DaemonCommands::Stop) => run_daemon_stop(paths).await,
        Some(DaemonCommands::Restart) => run_daemon_restart(&paths),
        Some(DaemonCommands::Install {
            port: install_port,
            dry_run,
        }) => run_daemon_install(&paths, install_port, dry_run),
        Some(DaemonCommands::Uninstall) => run_daemon_uninstall(&paths),
    }
}

fn run_daemon_install(paths: &don::daemon::DaemonPaths, port: u16, dry_run: bool) -> i32 {
    let plan = match don::daemon::install::plan(paths, port) {
        Ok(plan) => plan,
        Err(e) => {
            errln(format!("Error: {e}"));
            return 1;
        }
    };

    if dry_run {
        println!("would write {}:\n", plan.unit_path.display());
        println!("{}", plan.contents);
        return 0;
    }

    match don::daemon::install::install(&plan) {
        Ok(messages) => {
            for message in messages {
                println!("{message}");
            }
            println!("open the ui with `don ui`");
            0
        }
        Err(e) => {
            errln(format!("Error: {e}"));
            // The unit is on disk even when the supervisor call failed, so
            // point at the manual path rather than leaving a dead end.
            if plan.unit_path.exists() {
                errln(format!(
                    "the service file was written to {} — you can start it by hand",
                    plan.unit_path.display()
                ));
            }
            1
        }
    }
}

fn run_daemon_uninstall(paths: &don::daemon::DaemonPaths) -> i32 {
    // Port doesn't matter for removal; only the unit path is used.
    let plan = match don::daemon::install::plan(paths, don::web::DEFAULT_PORT) {
        Ok(plan) => plan,
        Err(e) => {
            errln(format!("Error: {e}"));
            return 1;
        }
    };
    match don::daemon::install::uninstall(&plan) {
        Ok(messages) => {
            for message in messages {
                println!("{message}");
            }
            0
        }
        Err(e) => {
            errln(format!("Error: {e}"));
            1
        }
    }
}

async fn run_daemon_foreground(paths: don::daemon::DaemonPaths, port: u16, no_web: bool) -> i32 {
    // The daemon has no OutputManager, so its progress goes straight to
    // stderr — which is where systemd and launchd capture it from anyway.
    let report: don::daemon::Reporter = std::sync::Arc::new(|line: &str| {
        errln(format!("[don daemon] {line}"));
    });

    let options = don::daemon::DaemonOptions {
        paths,
        web_addr: (!no_web).then(|| std::net::SocketAddr::from(([127, 0, 0, 1], port))),
    };
    match don::daemon::run(options, report).await {
        Ok(()) => 0,
        Err(e) => {
            errln(format!("Error: {e}"));
            1
        }
    }
}

async fn run_daemon_status(paths: don::daemon::DaemonPaths, json: bool) -> i32 {
    let client = don::daemon::DaemonClient::new(paths.socket());
    let info = match client.info().await {
        Ok(info) => info,
        Err(ClientError::NotRunning { .. }) => {
            if json {
                println!("{}", serde_json::json!({ "running": false }));
            } else {
                println!("don daemon is not running");
                println!("  start it with `don daemon`, or install it as a service");
                println!("  with `don daemon install` so it comes back on login.");
            }
            // Not an error — "is it running?" was answered.
            return 0;
        }
        Err(e) => {
            errln(format!("Error: {e}"));
            return 1;
        }
    };

    let projects = client.projects().await.unwrap_or_default();

    if json {
        let body = serde_json::json!({
            "running": true,
            "version": info.version,
            "pid": info.pid,
            "web_addr": info.web_addr,
            "projects": projects,
        });
        match serde_json::to_string_pretty(&body) {
            Ok(text) => println!("{text}"),
            Err(e) => {
                errln(format!("Error: {e}"));
                return 1;
            }
        }
        return 0;
    }

    println!("don daemon {} (pid {})", info.version, info.pid);
    match &info.web_addr {
        Some(addr) => println!("  web ui: http://{addr}"),
        None => println!("  web ui: disabled"),
    }
    println!("  socket: {}", paths.socket().display());
    if projects.is_empty() {
        println!("  projects: none registered");
    } else {
        println!("  projects:");
        for project in &projects {
            let profile = project
                .profile
                .as_ref()
                .map(|p| format!(" [{p}]"))
                .unwrap_or_default();
            println!(
                "    {}{profile}  pid {}  {}",
                project.name,
                project.pid,
                project.root.display()
            );
        }
    }
    0
}

/// `don ui` — open the daemon's web UI in a browser.
///
/// Asks the daemon where it bound rather than assuming the default port, so
/// this works against a daemon started with `--port` or with port 0.
async fn run_ui(print_only: bool) -> i32 {
    let paths = match don::daemon::DaemonPaths::from_process_env() {
        Ok(paths) => paths,
        Err(e) => {
            errln(format!("Error: {e}"));
            return 1;
        }
    };

    let client = don::daemon::DaemonClient::new(paths.socket());
    let info = match client.info().await {
        Ok(info) => info,
        Err(ClientError::NotRunning { .. }) => {
            errln(
                "don daemon is not running, so there's no web ui to open.\n  \
                 Start one with `don daemon`, install it with `don daemon install`,\n  \
                 or serve a single project with `don start --with-ui`.",
            );
            return 1;
        }
        Err(e) => {
            errln(format!("Error: {e}"));
            return 1;
        }
    };

    let Some(addr) = info.web_addr else {
        errln(
            "the don daemon is running with its web ui disabled.\n  \
             Restart it without `--no-web` to enable it.",
        );
        return 1;
    };

    let url = format!("http://{addr}/");

    if print_only {
        println!("{url}");
        return 0;
    }

    match open_in_browser(&url) {
        Ok(()) => {
            println!("opened {url}");
            0
        }
        Err(e) => {
            errln(format!(
                "could not open a browser ({e}) — open this instead:"
            ));
            println!("{url}");
            0
        }
    }
}

/// Hand a URL to the platform's URL opener.
fn open_in_browser(url: &str) -> Result<(), String> {
    let opener = if cfg!(target_os = "macos") {
        "open"
    } else {
        "xdg-open"
    };
    let status = std::process::Command::new(opener)
        .arg(url)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map_err(|e| format!("{opener}: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{opener} exited with {status}"))
    }
}

/// `don daemon stop`.
///
/// An installed daemon is stopped through its supervisor. Asking it to exit on
/// its own would leave systemd or launchd thinking it stopped for reasons of
/// its own — and since a clean exit isn't covered by `Restart=on-failure`, the
/// service would sit inactive until something started it again, which is a
/// confusing state to leave someone in.
async fn run_daemon_stop(paths: don::daemon::DaemonPaths) -> i32 {
    if let Ok(plan) = don::daemon::install::plan(&paths, don::web::DEFAULT_PORT)
        && plan.installed()
    {
        return match don::daemon::install::supervisor_stop(&plan) {
            Ok(()) => {
                println!("don daemon stopped (running projects were left alone)");
                println!("  it will start again on login — `don daemon uninstall` to prevent that");
                0
            }
            Err(e) => {
                errln(format!("Error: {e}"));
                1
            }
        };
    }

    let socket = paths.socket();
    let client = don::daemon::DaemonClient::new(socket.clone());
    match client.shutdown().await {
        Ok(()) => {}
        Err(ClientError::NotRunning { .. }) => {
            println!("don daemon is not running");
            return 0;
        }
        Err(e) => {
            errln(format!("Error: {e}"));
            return 1;
        }
    }

    match wait_for_daemon_socket_gone(&socket, std::time::Duration::from_secs(10)).await {
        Ok(()) => {
            println!("don daemon stopped (running projects were left alone)");
            0
        }
        Err(e) => {
            errln(e);
            1
        }
    }
}

/// `don daemon restart` — mainly for picking up an upgraded `don` binary.
///
/// Supervisor-only by design. Restarting a bare foreground `don daemon` would
/// mean spawning a detached replacement, which is process management this
/// command has no business inventing — better to say so.
fn run_daemon_restart(paths: &don::daemon::DaemonPaths) -> i32 {
    let plan = match don::daemon::install::plan(paths, don::web::DEFAULT_PORT) {
        Ok(plan) => plan,
        Err(e) => {
            errln(format!("Error: {e}"));
            return 1;
        }
    };

    if !plan.installed() {
        errln(format!(
            "the don daemon isn't installed as a service, so there's nothing to restart it with.\n  \
             Expected a unit at {}.\n  \
             Install it with `don daemon install`, or stop and start your `don daemon` by hand.",
            plan.unit_path.display()
        ));
        return 1;
    }

    match don::daemon::install::supervisor_restart(&plan) {
        Ok(()) => {
            println!("don daemon restarted (running projects were left alone)");
            0
        }
        Err(e) => {
            errln(format!("Error: {e}"));
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
        Ok(mut processes) => {
            // A named query is filtered server-side; an empty result means the
            // name didn't match anything. Fetch the full list to offer a
            // did-you-mean before failing.
            if let Some(name) = name
                && processes.is_empty()
            {
                let suggestion = match client.status(false, None).await {
                    Ok(all) => {
                        let available: std::collections::HashSet<&str> =
                            all.iter().map(process_name).collect();
                        suggest_name_typo(name, &available)
                    }
                    Err(_) => String::new(),
                };
                errln(format!("no service or task named '{name}'{suggestion}"));
                return 1;
            }
            processes.sort_by(|a, b| {
                status_sort_bucket(a)
                    .cmp(&status_sort_bucket(b))
                    .then_with(|| process_name(a).cmp(process_name(b)))
            });
            if json {
                #[derive(serde::Serialize)]
                struct StatusJson<'a> {
                    ready: bool,
                    tasks_pending_run: usize,
                    processes: &'a [ProcessStatus],
                }
                let payload = StatusJson {
                    ready: don::runner::all_services_ready(&processes),
                    tasks_pending_run: count_tasks_pending_run(&processes),
                    processes: &processes,
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
            print_status_table(&processes, verbose, name.is_some());
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
        println!(
            "  {}  {dim}{} · {} · debounce {}ms{reset}",
            item.name, item.kind, item.state, item.debounce_ms
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
    // "didn't reload" report.
    if report.notify_error_count > 0 {
        println!();
        let last = report.last_notify_error.as_deref().unwrap_or("");
        println!(
            "{dim}notify errors:{reset} {} (last: {last})",
            report.notify_error_count
        );
    }
}

/// Sort bucket for the status table: actionable rows first, settled rows
/// last. Putting `DependencyFailed` *below* `Failed` surfaces the actual
/// culprit — the thing the user needs to look at — above everything that
/// merely got stranded.
fn status_sort_bucket(item: &ProcessStatus) -> u8 {
    match item {
        ProcessStatus::Service { state, .. } => match state {
            ServiceState::Failed | ServiceState::Unhealthy => 0,
            ServiceState::DependencyFailed => 1,
            ServiceState::Pending | ServiceState::Building | ServiceState::Starting => 2,
            ServiceState::Running => 3,
            ServiceState::Ready => 4,
            ServiceState::Stopping => 5,
            ServiceState::Stopped => 6,
            ServiceState::Lazy => 7,
        },
        ProcessStatus::Task { state, .. } => match state {
            TaskState::Failed => 0,
            TaskState::DependencyFailed => 1,
            TaskState::Pending | TaskState::Building | TaskState::Running => 2,
            TaskState::PendingRun => 7,
            TaskState::Completed => 8,
            TaskState::Skipped => 9,
        },
    }
}

fn process_name(item: &ProcessStatus) -> &str {
    match item {
        ProcessStatus::Service { name, .. } | ProcessStatus::Task { name, .. } => name.as_str(),
    }
}

/// Count tasks parked in `PendingRun` — maintenance work awaiting a manual run.
/// Mirrors the TUI's `*` flag so headless `--json` callers can detect it.
fn count_tasks_pending_run(processes: &[ProcessStatus]) -> usize {
    processes
        .iter()
        .filter(|process| {
            matches!(
                process,
                ProcessStatus::Task {
                    state: TaskState::PendingRun,
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

/// TTY-mode `don start`: spawn the runner as a background child and attach
/// the TUI as a client — the fork model.
///
/// The runner never has a terminal; the thing in your terminal is an
/// ordinary client of the socket API, identical to `don tui`. Ctrl+C keeps
/// its contract (graceful stack shutdown — the TUI requests it over the
/// API; a terminal-delivered SIGINT is forwarded to the child, so a second
/// one still reaches the runner's force-kill escalation). Ctrl+D detaches,
/// leaving the stack running.
///
/// The parent gates on the *socket*, not on startup completing — watching
/// startup happen is what the TUI is for. A child that dies before the
/// socket answers gets its log tail relayed, so startup failures surface
/// exactly where the user is looking.
async fn run_start_attached(
    config_path: &Path,
    profile: Option<&str>,
    verbose: bool,
    log_filter: Vec<String>,
    no_daemon: bool,
    with_ui: Option<u16>,
) -> Result<(), String> {
    let base = base_dir(config_path);

    // Already running: `don start` is idempotent in TTY mode — attach the
    // TUI to the stack that's up (tmux-attach muscle memory) instead of
    // erroring about it. Decided with PJ 2026-08-06.
    let probe = Client::new(&base);
    match probe.status(false, None).await {
        Ok(_) => {
            println!("don is already running here — attaching (Ctrl+D detaches, `don stop` stops)");
            let log_filter_set: Option<std::collections::HashSet<String>> = if log_filter.is_empty()
            {
                None
            } else {
                Some(log_filter.into_iter().collect())
            };
            return attach_tui_inner(config_path, log_filter_set).await;
        }
        Err(ClientError::NotRunning { .. }) => {}
        Err(e) => return Err(format!("failed to check for a running don: {e}")),
    }

    let (mut child, log_path) =
        spawn_runner_child(config_path, profile, verbose, no_daemon, with_ui)?;
    let child_pid = child.id();

    // Wait for the API socket to answer. Generous only about the socket —
    // binding happens before the runner's main loop, so a healthy child
    // answers almost immediately.
    let client = Client::new(&base);
    let start = tokio::time::Instant::now();
    let socket_timeout = std::time::Duration::from_secs(10);
    loop {
        if client.ready().await.is_ok() {
            break;
        }
        if let Some(status) = child
            .try_wait()
            .map_err(|e| format!("failed to check the runner process: {e}"))?
        {
            let status_text = match status.code() {
                Some(code) => format!("exit code {code}"),
                None => "terminated by signal".to_string(),
            };
            let tail = read_log_tail(&log_path, 20);
            let mut msg = format!(
                "the runner exited before its API came up ({status_text}); log: {}",
                log_path.display()
            );
            if !tail.is_empty() {
                msg.push_str("\n\n");
                msg.push_str(&tail);
            }
            return Err(msg);
        }
        if start.elapsed() >= socket_timeout {
            return Err(format!(
                "the runner did not answer within {socket_timeout:?} (pid {child_pid}); log: {}",
                log_path.display()
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    }

    // Forward terminal signals to the runner child. The TUI's raw mode
    // means Ctrl+C arrives as a key (handled over the API), so this exists
    // for signals sent from *outside* — `kill -INT`, a closing terminal
    // emulator — and it preserves the two-press force-kill escalation,
    // because the child's own signal counter sees each forwarded SIGINT.
    tokio::spawn(async move {
        use tokio::signal::unix::{SignalKind, signal};
        let Ok(mut sigint) = signal(SignalKind::interrupt()) else {
            return;
        };
        let Ok(mut sigterm) = signal(SignalKind::terminate()) else {
            return;
        };
        loop {
            tokio::select! {
                received = sigint.recv() => { if received.is_none() { return; } }
                received = sigterm.recv() => { if received.is_none() { return; } }
            }
            let _ = nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(child_pid as i32),
                nix::sys::signal::Signal::SIGINT,
            );
        }
    });

    let log_filter_set: Option<std::collections::HashSet<String>> = if log_filter.is_empty() {
        None
    } else {
        Some(log_filter.into_iter().collect())
    };
    attach_tui_inner(config_path, log_filter_set).await?;

    // After a shutdown the child has exited (or is about to); reap it so a
    // fast exit doesn't leave a zombie for the brief rest of our lifetime.
    // After a detach it is alive and this is a no-op — init inherits it.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let _ = child.try_wait();
    Ok(())
}

/// Whether TTY-mode `don start` should use the fork model.
///
/// Conservative: any config that fails to load or validate falls back to
/// the in-process path, which reports the error exactly as it always has.
/// Interactive (foreground) tasks are no obstacle — they run on
/// runner-owned PTYs and clients bridge to them over the socket.
fn should_auto_attach(config_path: &Path, no_tui: bool) -> bool {
    use std::io::IsTerminal;
    if no_tui || !std::io::stdout().is_terminal() {
        return false;
    }
    let Ok(config) = don::config::Config::from_file(config_path) else {
        return false;
    };
    let Some(platform) = don::config::Platform::current() else {
        return false;
    };
    config.validate(platform).is_ok()
}

/// `don tui` — attach the full TUI to an already-running project.
///
/// A pure client of the socket API: the process set comes from `GET /status`
/// (which doubles as the "is anything running?" check, answered before the
/// terminal is touched), logs from the merged follow with history preload,
/// events from `GET /events` (whose snapshot preamble makes the view
/// consistent at connect). Ctrl+D detaches, leaving the stack running;
/// Ctrl+C requests a graceful shutdown and the TUI exits when the runner's
/// streams close.
async fn run_attach_tui(config_path: &Path) -> i32 {
    match attach_tui_inner(config_path, None).await {
        Ok(()) => 0,
        Err(message) => {
            eprintln!("{message}");
            1
        }
    }
}

async fn attach_tui_inner(
    config_path: &Path,
    cli_log_filter: Option<std::collections::HashSet<String>>,
) -> Result<(), String> {
    use don::client::{Client, LogStreamEvent};

    {
        use std::io::IsTerminal;
        if !std::io::stdout().is_terminal() {
            return Err(
                "don tui needs a terminal — use `don status` or `don logs` in scripts".into(),
            );
        }
    }

    let base = base_dir(config_path);
    let client = Client::new(&base);

    // Authoritative process set from the runner. Everything below intersects
    // against it, so profile filtering and config drift can't invent processes
    // the runner doesn't have.
    let processes = client
        .status(false, None)
        .await
        .map_err(|e| format!("cannot attach: {e}"))?;
    let mut service_names: Vec<String> = Vec::new();
    let mut task_names: Vec<String> = Vec::new();
    for process in &processes {
        match process {
            don::client::ProcessStatus::Service { name, .. } => service_names.push(name.clone()),
            don::client::ProcessStatus::Task { name, .. } => task_names.push(name.clone()),
        }
    }
    let active: std::collections::HashSet<&String> =
        service_names.iter().chain(task_names.iter()).collect();

    // Config supplies what /status doesn't: task param schemas, hidden and
    // auto-filter flags, whether a bazel stream exists. Best-effort — the
    // file may have drifted since the runner started.
    let config = don::config::Config::from_file(config_path).map_err(|e| format!("Error: {e}"))?;
    let platform =
        don::config::Platform::current().ok_or_else(|| "unsupported platform".to_string())?;

    let task_configs: std::collections::HashMap<String, don::config::Task> = config
        .tasks
        .iter()
        .filter(|(name, _)| active.contains(name))
        .map(|(name, task)| (name.clone(), task.clone()))
        .collect();
    let hidden_names: std::collections::HashSet<String> = config
        .services
        .iter()
        .filter(|(name, svc)| active.contains(name) && svc.hidden)
        .map(|(name, _)| name.clone())
        .chain(
            config
                .tasks
                .iter()
                .filter(|(name, task)| active.contains(name) && task.hidden)
                .map(|(name, _)| name.clone()),
        )
        .collect();
    let auto_filter_on_failure_names: std::collections::HashSet<String> = config
        .services
        .iter()
        .filter(|(name, svc)| {
            active.contains(name)
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
                    active.contains(name)
                        && task
                            .auto_filter_on_failure
                            .unwrap_or(config.auto_filter_on_failure)
                })
                .map(|(name, _)| name.clone()),
        )
        .collect();
    let has_build_tool =
        config.services.iter().any(|(name, svc)| {
            active.contains(name) && svc.resolve(platform).bazel_config().is_some()
        }) || config
            .tasks
            .iter()
            .any(|(name, task)| active.contains(name) && task.bazel.is_some());
    let build_tool_names: Vec<String> = if has_build_tool {
        vec!["bazel".to_string()]
    } else {
        Vec::new()
    };

    let task_state = don::TaskStateStore::new(base.join(".don").join("task-state"));
    let mut task_last_runs = std::collections::HashMap::new();
    for name in &task_names {
        if let Ok(Some(last_run)) = task_state.last_run(name).await {
            task_last_runs.insert(name.clone(), last_run);
        }
    }

    // A local, null-writer output manager gives the TUI a lifecycle emitter
    // whose feedback lines ("restart requested") render locally with the
    // same formatting as runner output, plus the verbosity control the `v`
    // key flips. The emitter's lines flow local sink task -> tap -> merger
    // below, exactly like runner lines flow into the socket stream.
    let local_output = don::output::OutputManager::new(&[], tokio::io::sink())
        .await
        .map_err(|e| format!("Error: {e}"))?;
    let lifecycle_emitter = local_output.clone_lifecycle_emitter();
    // Local narration this client generates itself (bridge notices, errors).
    // A cursor rather than a raw subscription so the two sides of the merge
    // behave the same way when either falls behind.
    let mut local_tap = local_output.log_stream_sender().cursor(None, 0).await;

    // A long-lived runner can outlive a `don` upgrade. Warn about skew as a
    // rendered line rather than misbehaving quietly.
    if let Ok(info) = client.ready_info().await {
        let mine = env!("CARGO_PKG_VERSION");
        match info.version.as_deref() {
            Some(theirs) if theirs != mine => lifecycle_emitter.lifecycle_event(&format!(
                "version skew: this client is {mine}, the runner is {theirs} — restart the stack to align"
            )),
            None => lifecycle_emitter.lifecycle_event(&format!(
                "version skew: this client is {mine}, the runner predates version reporting — restart the stack to align"
            )),
            _ => {}
        }
    }

    // Remote merged stream -> intermediate channel. The spawned follow ends
    // when the server closes the stream (shutdown) or the merger drops the
    // receiver (TUI exited).
    let (remote_tx, mut remote_rx) = tokio::sync::mpsc::unbounded_channel::<LogStreamEvent>();
    {
        let follow = Client::new(&base);
        tokio::spawn(async move {
            let _ = follow
                .logs_follow_all(None, |event| {
                    remote_tx
                        .send(event)
                        .map_err(|_| don::client::ClientError::Invalid("tui closed".into()))
                })
                .await;
        });
    }

    // One merger owns the TUI's log sender. That ownership is the exit
    // path: when the remote stream ends (runner gone), the merger returns,
    // the sender drops, and the TUI loop sees its log channel close.
    let (log_tx, log_rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                remote = remote_rx.recv() => match remote {
                    Some(LogStreamEvent::Line { id, name, lifecycle, verbose, prefix, line }) => {
                        let event = don::output::MergedEvent::Line(don::output::MergedLine {
                            id,
                            line: std::sync::Arc::new(don::output::FormattedLogLine {
                                name,
                                is_lifecycle: lifecycle,
                                is_verbose: verbose,
                                prefix: prefix.into_bytes(),
                                bytes: line.into_bytes(),
                            }),
                        });
                        if log_tx.send(event).is_err() {
                            return;
                        }
                    }
                    Some(LogStreamEvent::Dropped { dropped, resumed_at }) => {
                        // The server already tried its own history; if it is
                        // telling us, the lines are genuinely gone.
                        if log_tx
                            .send(don::output::MergedEvent::Dropped {
                                count: dropped,
                                resumed_at,
                            })
                            .is_err()
                        {
                            return;
                        }
                    }
                    None => return,
                },
                local = local_tap.recv() => match local {
                    Some(event) => {
                        if log_tx.send(event).is_err() {
                            return;
                        }
                    }
                    // Local feedback is best-effort; the remote stream is
                    // the one whose end must end the session.
                    None => continue,
                },
            }
        }
    });

    don::run_tui(
        log_rx,
        client,
        don::TuiMode::Remote,
        lifecycle_emitter,
        service_names,
        task_names,
        build_tool_names,
        task_configs,
        task_last_runs,
        hidden_names,
        auto_filter_on_failure_names,
        cli_log_filter,
    )
    .await
    .map_err(|e| format!("tui error: {e}"))?;

    // Keep the local sink alive for the whole session (its task carries the
    // emitter's feedback lines into the merger above).
    drop(local_output);

    // Distinguish detach from shutdown for the user: after Ctrl+C the
    // runner's socket is already closing by the time the TUI exits, so this
    // probe fails and stays quiet; after Ctrl+D it answers.
    let probe = Client::new(&base);
    if probe.ready().await.is_ok() {
        println!(
            "detached — the stack is still running (`don tui` to reattach, `don stop` to stop)"
        );
    }
    Ok(())
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

fn print_status_table(processes: &[ProcessStatus], verbose: bool, show_watch_paths: bool) {
    if processes.is_empty() {
        println!("(no services or tasks)");
        return;
    }
    // Compute column widths.
    let kind_w = "KIND".len().max(
        processes
            .iter()
            .map(|i| match i {
                ProcessStatus::Service { .. } => "service".len(),
                ProcessStatus::Task { .. } => "task".len(),
            })
            .max()
            .unwrap_or(0),
    );
    let name_w = "NAME".len().max(
        processes
            .iter()
            .map(|i| match i {
                ProcessStatus::Service { name, .. } | ProcessStatus::Task { name, .. } => {
                    name.len()
                }
            })
            .max()
            .unwrap_or(0),
    );

    let state_w = "STATE".len().max(
        processes
            .iter()
            .map(process_state_label)
            .map(|label| label.len())
            .max()
            .unwrap_or(0),
    );

    println!(
        "{:<kind_w$}  {:<name_w$}  {:<state_w$}  LAST RUN  RESULT  DURATION",
        "KIND", "NAME", "STATE"
    );
    for process in processes {
        let (kind, name, state_str, color, last_run, result, duration, verbose_info) = match process
        {
            ProcessStatus::Service {
                name,
                state,
                failed_dependencies,
                verbose,
                ..
            } => (
                "service",
                name.as_str(),
                state_label(service_state_label(*state), failed_dependencies),
                service_state_color(*state),
                "-".to_string(),
                "-".to_string(),
                "-".to_string(),
                verbose.as_ref(),
            ),
            ProcessStatus::Task {
                name,
                state,
                failed_dependencies,
                last_run,
                verbose,
                ..
            } => (
                "task",
                name.as_str(),
                state_label(task_state_label(*state), failed_dependencies),
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

/// Print verbose details for a single process, indented under the status line.
#[allow(clippy::print_stdout)]
fn print_verbose_info(info: &don::runner::VerboseInfo, show_watch_paths: bool) {
    let dim = SetAttribute(Attribute::Dim);
    let reset = SetAttribute(Attribute::Reset);

    if let Some(ref cmd) = info.cmd {
        println!("  {dim}cmd:{reset}    {cmd}");
    }
    if !info.depends_on.is_empty() {
        let deps: Vec<String> = info.depends_on.iter().map(ToString::to_string).collect();
        println!("  {dim}deps:{reset}   {}", deps.join(", "));
    }
    if !info.proxy.is_empty() {
        println!("  {dim}proxy:{reset}  {}", info.proxy.join(", "));
    }
    if !info.docker_ports.is_empty() {
        println!("  {dim}docker:{reset} {}", info.docker_ports.join(", "));
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
    // When inspecting a single process, expand the full resolved watch path list
    // (these are dynamically resolved for build-tool services, so the count
    // alone hides what's actually being watched). In the all-processes view keep it
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

fn task_state_label(s: TaskState) -> &'static str {
    match s {
        TaskState::Pending => "pending",
        TaskState::Building => "building",
        TaskState::Running => "running",
        TaskState::Completed => "completed",
        TaskState::Skipped => "skipped",
        TaskState::Failed => "failed",
        TaskState::DependencyFailed => "dep failed",
        TaskState::PendingRun => "pending_run",
    }
}

fn task_state_color(s: TaskState) -> Color {
    match s {
        TaskState::Completed | TaskState::Skipped => Color::Green,
        TaskState::Running | TaskState::Pending | TaskState::Building => Color::Yellow,
        TaskState::PendingRun => Color::Cyan,
        TaskState::Failed => Color::Red,
        TaskState::DependencyFailed => Color::DarkRed,
    }
}

fn process_state_label(process: &ProcessStatus) -> Cow<'static, str> {
    match process {
        ProcessStatus::Service {
            state,
            failed_dependencies,
            ..
        } => state_label(service_state_label(*state), failed_dependencies),
        ProcessStatus::Task {
            state,
            failed_dependencies,
            ..
        } => state_label(task_state_label(*state), failed_dependencies),
    }
}

fn state_label(base: &'static str, failed_dependencies: &[String]) -> Cow<'static, str> {
    if failed_dependencies.is_empty() {
        Cow::Borrowed(base)
    } else {
        Cow::Owned(format!("{base}: {}", failed_dependencies.join(", ")))
    }
}

async fn run_cleanup_command(config_path: &std::path::Path, force: bool) -> i32 {
    let base = base_dir(config_path);
    let don_dir = base.join(".don");
    let _ = std::fs::create_dir_all(&don_dir);

    // Acquire the PID file lock so we don't race with a running daemon.
    let don_pid_path = don_dir.join("don.pid");
    let pid_lock =
        match don::sys::pid_file::PidFile::acquire(don_pid_path.clone(), std::process::id() as i32)
            .await
        {
            Ok(lock) => lock,
            Err(don::sys::pid_file::PidFileError::AlreadyLocked) => {
                if !force {
                    println!(
                        "don daemon is running — nothing to clean up (use --force to kill it)"
                    );
                    return 0;
                }
                // --force: read the running daemon's PID and kill it.
                errln("killing running don daemon...");
                if let Err(e) = kill_running_daemon(&don_pid_path).await {
                    errln(format!("failed to kill daemon: {e}"));
                    return 1;
                }
                // Now re-acquire the lock.
                match don::sys::pid_file::PidFile::acquire(don_pid_path, std::process::id() as i32)
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
        Ok(config) => match don::config::Platform::current() {
            Some(platform) => config
                .services
                .iter()
                .filter_map(|(name, svc)| {
                    let resolved = svc.resolve(platform);
                    if let Some(don::config::ServiceKind::Docker(d)) = &resolved.kind {
                        Some(don::ports::managed_docker_container_name(
                            &base,
                            name,
                            d,
                            config.fallback_ports,
                        ))
                    } else {
                        None
                    }
                })
                .collect(),
            None => {
                errln("Warning: unsupported platform; Docker cleanup names may be incomplete");
                Vec::new()
            }
        },
        Err(e) => {
            errln(format!(
                "Warning: could not load config for docker cleanup: {e}"
            ));
            vec![]
        }
    };

    let report = don::sys::cleanup::run_cleanup(&base, &docker_names).await;
    println!("{report}");
    for warning in &report.warnings {
        errln(format!("Warning: {warning}"));
    }
    if let Err(error) = don::ports::remove_manifest(&base) {
        errln(format!("Warning: failed to remove runtime ports: {error}"));
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
    no_daemon: bool,
    with_ui: Option<u16>,
) -> Result<(), String> {
    let base = base_dir(config_path);
    let client = Client::new(&base);
    match client.status(false, None).await {
        Ok(_) => return Err("don daemon is already running".to_string()),
        Err(ClientError::NotRunning { .. }) => {}
        Err(e) => return Err(format!("failed to check daemon status: {e}")),
    }

    let (mut child, log_path) =
        spawn_runner_child(config_path, profile, verbose, no_daemon, with_ui)?;
    let pid = child.id();
    wait_for_detached_start(&mut child, &base, &log_path, pid).await
}

/// Spawn the runner as a background child: new session (no controlling
/// terminal, so terminal-generated signals never reach it), stdin null,
/// stdout+stderr appended to `.don/logs/runner.log` — the post-mortem for a
/// runner nobody is attached to.
///
/// Used by both `don start -d` (spawn and exit) and TTY-mode `don start`
/// (spawn and attach).
fn spawn_runner_child(
    config_path: &Path,
    profile: Option<&str>,
    verbose: bool,
    no_daemon: bool,
    with_ui: Option<u16>,
) -> Result<(std::process::Child, PathBuf), String> {
    let base = base_dir(config_path);
    let log_path = base.join(".don").join("logs").join("runner.log");
    if let Some(parent) = log_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create {}: {e}", parent.display()))?;
    }
    append_runner_log_header(&log_path)?;

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
    cmd.arg("start").arg("--no-tui").arg("--attached");
    if no_daemon {
        cmd.arg("--no-daemon");
    }
    if let Some(port) = with_ui {
        cmd.arg(format!("--with-ui={port}"));
    }
    if let Some(profile_name) = profile {
        cmd.arg("--profile").arg(profile_name);
    }
    // `--log-filter` is deliberately NOT forwarded: in pipe mode it
    // filters stdout, and the child's stdout is runner.log — the
    // post-mortem log should be complete. The filter is presentation;
    // the attached TUI applies it client-side. (`-d` callers who want a
    // filtered background log can be catered to later if anyone asks.)

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // Run the runner in a new session so terminal-generated signals for
        // the parent shell do not also hit it. Signal *forwarding* (for the
        // attached case) is explicit and pid-targeted.
        unsafe {
            cmd.pre_exec(|| {
                if nix::libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }

    let child = cmd
        .spawn()
        .map_err(|e| format!("failed to spawn don runner: {e}"))?;
    Ok((child, log_path))
}

fn append_runner_log_header(log_path: &Path) -> Result<(), String> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .map_err(|e| format!("failed to open {}: {e}", log_path.display()))?;
    writeln!(file, "\n--- don runner (background) ---")
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

#[allow(clippy::too_many_arguments)]
async fn run_start(
    config_path: &std::path::Path,
    profile: Option<&str>,
    verbose: bool,
    no_tui: bool,
    attached: bool,
    log_filter: Vec<String>,
    no_daemon: bool,
    with_ui: Option<u16>,
) -> Result<(), String> {
    use std::io::IsTerminal;

    // Serving a UI from this process means not depending on a daemon at all.
    let no_daemon = no_daemon || with_ui.is_some();

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

    // Resolve the active process set up front so the output manager and TUI
    // only see processes that will actually run. Without this, prefix padding
    // is sized for the longest name in the whole config, and the TUI
    // service menu lists processes the profile excludes. The runner re-runs
    // this inside `Runner::new` to build its own filtered state.
    let active_processes: Option<std::collections::HashSet<String>> =
        if let Some(profile_name) = profile_ref {
            let prof = config
                .profiles
                .get(profile_name)
                .ok_or_else(|| format!("Error: unknown profile '{profile_name}'"))?;
            Some(don::config::resolve_profile_processes(&config, prof))
        } else {
            None
        };

    let is_active = |name: &str| active_processes.as_ref().is_none_or(|s| s.contains(name));
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
    // config-derived process set, not on later registration order.
    let service_kinds = || {
        config.services.values().flat_map(|svc| {
            std::iter::once(svc.kind.as_ref())
                .chain(svc.platform.values().map(|ov| ov.kind.as_ref()))
                .flatten()
        })
    };
    let uses_bazel = service_kinds().any(|k| matches!(k, don::config::ServiceKind::Bazel(_)))
        || config.tasks.values().any(|t| t.bazel.is_some());
    let build_tool_log = don::config::LogConfig::Stdout;
    let build_tool_configs: Vec<(&str, &don::config::LogConfig)> = uses_bazel
        .then_some(("bazel", &build_tool_log))
        .into_iter()
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

    // Validate `--log-filter` against the active process set so typos surface
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
    let shutdown_rx = don::signals::install_signal_handlers()
        .await
        .map_err(|e| format!("Error installing signal handlers: {e}"))?;

    // Bring the UI up before the runner so a port conflict fails before
    // anything has been spawned — the same "validate everything before
    // starting anything" rule the config checks above follow. Held until
    // `run_start` returns, which is what stops the server.
    let _ui = match with_ui {
        Some(port) => Some(start_with_ui(port, &base, profile_ref).await?),
        None => None,
    };

    if is_tty {
        // TUI mode: nothing may write to the real stdout while the TUI owns
        // the screen, so the writer is a null sink and the TUI follows the
        // merged log stream — the same tap `GET /logs?follow=true` serves,
        // subscribed before the runner starts so no startup line is missed.
        let output_manager = don::output::OutputManager::new_verbose_with_log_filters(
            &all_configs,
            &log_keep_filters,
            tokio::io::sink(),
            verbose,
        )
        .await
        .map_err(|e| format!("Error creating output manager: {e}"))?;
        let (log_tx, log_rx) = tokio::sync::mpsc::unbounded_channel();
        // A cursor, not a raw subscription: falling behind is repaired from the
        // tap's own history rather than punched into the TUI's log as a hole.
        // The remote TUI is fed the same `MergedEvent`s off the wire, so the
        // two clients differ in transport and nothing else.
        let mut tap = output_manager
            .log_stream_sender()
            .cursor(None, don::output::DEFAULT_MERGED_HISTORY_CAPACITY)
            .await;
        tokio::spawn(async move {
            while let Some(event) = tap.recv().await {
                if log_tx.send(event).is_err() {
                    return; // TUI is gone; stop following.
                }
            }
        });
        let lifecycle_emitter = output_manager.clone_lifecycle_emitter();

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
        let task_state = don::TaskStateStore::new(base.join(".don").join("task-state"));
        let mut task_last_runs = std::collections::HashMap::new();
        for name in &task_names {
            if let Ok(Some(last_run)) = task_state.last_run(name).await {
                task_last_runs.insert(name.clone(), last_run);
            }
        }

        // Synthetic build-tool stream names that should appear in the TUI
        // filter. Without these entries, lines emitted by the bazel
        // client (which carries `name = "bazel"`) are silently
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

        // Clone before `output_manager` moves into the runner — the daemon
        // registration watcher reports failures at debug verbosity.
        let daemon_emitter = output_manager.clone_lifecycle_emitter();
        let mut runner = await_with_shutdown_supervision(
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
                        // A TUI is present and interactive tasks are
                        // bridgeable, so headless overrides don't apply.
                        false,
                    )
                    .await
                    .map_err(|e| format!("Error: {e}"))
                }
            }),
            "starting runner",
        )
        .await?;
        serve_project_api(&mut runner, &daemon_emitter);
        spawn_daemon_registration(&runner, daemon_emitter, profile_ref, no_daemon);

        // The TUI is a client of the socket API — same surface a detached
        // client will use. The socket is already bound (serve_project_api
        // above), so the connection cannot race the bind.
        // `base` moved into the runner spawn; the canonical root is on the
        // runner anyway, and canonical is the safer of the two.
        let tui_client = don::client::Client::new(runner.base_dir());

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
                tui_client,
                don::TuiMode::InProcess,
                lifecycle_emitter,
                service_names,
                task_names,
                build_tool_names,
                task_configs,
                task_last_runs,
                hidden_names,
                auto_filter_on_failure_names,
                tui_log_filter,
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

        let daemon_emitter = output_manager.clone_lifecycle_emitter();
        let mut runner = await_with_shutdown_supervision(
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
                        // Headless unless the fork-model parent told us an
                        // interactive client is attaching: with no TUI and
                        // no attaching parent, nobody is there to drive an
                        // interactive task, so `headless` overrides apply.
                        !attached,
                    )
                    .await
                    .map_err(|e| format!("Error: {e}"))
                }
            }),
            "starting runner",
        )
        .await?;
        serve_project_api(&mut runner, &daemon_emitter);
        spawn_daemon_registration(&runner, daemon_emitter, profile_ref, no_daemon);

        let runner_task =
            tokio::spawn(async move { runner.run().await.map_err(|e| format!("Error: {e}")) });
        await_with_shutdown_supervision(runner_task, "waiting for runner shutdown").await
    }
}

/// Bind and serve this project's unix-socket API.
///
/// Done here rather than inside `Runner::run` so the runner never names the
/// server — that edge was half of a dependency cycle. A bind failure is
/// reported and the stack still starts: the API is how you *inspect* a
/// running project, and losing it should not stop the project running.
fn serve_project_api(runner: &mut don::runner::Runner, emitter: &don::LifecycleEmitter) {
    match don::server::serve_for_runner(runner) {
        Ok(shutdown_tx) => runner.set_api_shutdown(shutdown_tx),
        Err(e) => emitter.lifecycle_event(&format!("api server disabled: {e}")),
    }
}

/// Announce this project to the system-wide daemon so it shows up in the web
/// UI, and withdraw it again on shutdown.
///
/// This lives in the binary, not the runner: which projects a daemon should
/// list is a deployment policy, and the runner has no business knowing one
/// exists. It learns the two moments that matter from the event stream —
/// `ApiListening` (the socket a daemon would proxy to now exists) and
/// `ShutdownStarted`.
///
/// Best-effort throughout. A state directory that can't be resolved (no
/// `$HOME`, an unusual sandbox) or a daemon that isn't running costs you the
/// UI listing and nothing else — never the ability to start a stack. Nothing
/// here is ever awaited by the runner, so a slow or absent daemon cannot add
/// a millisecond to startup or to Ctrl+C.
fn spawn_daemon_registration(
    runner: &don::runner::Runner,
    emitter: don::output::LifecycleEmitter,
    profile: Option<&str>,
    no_daemon: bool,
) {
    if no_daemon {
        return;
    }
    let Ok(paths) = don::daemon::DaemonPaths::from_process_env() else {
        return;
    };
    don::daemon::registration::spawn(
        runner.subscribe(),
        paths.socket(),
        // The runner's canonical root, not the path we passed in — the daemon
        // keys projects by a hash of it. See `Runner::base_dir`.
        runner.base_dir().to_path_buf(),
        profile.map(str::to_string),
        emitter,
    );
}

/// A web UI served by `don start --with-ui`, alive for as long as this value.
///
/// Dropping the sender ends the server's graceful-shutdown wait, so the UI
/// goes away with the stack it belongs to — no explicit teardown call to
/// forget on one of `run_start`'s exit paths.
struct WithUiGuard {
    _shutdown_tx: tokio::sync::watch::Sender<bool>,
}

/// Start a project-local web UI on `port`.
///
/// Errors here are fatal rather than best-effort: unlike daemon
/// registration, the user explicitly asked for this UI, so silently not
/// getting one would be worse than a clear failure.
async fn start_with_ui(
    port: u16,
    base: &Path,
    profile: Option<&str>,
) -> Result<WithUiGuard, String> {
    let root = std::fs::canonicalize(base).unwrap_or_else(|_| base.to_path_buf());
    // Built as a registry row (one id-derivation to rule them all), then
    // converted to the web layer's own type. In fork mode this runs in the
    // runner child, so the pid really is the runner's.
    let entry: don::web::Project =
        don::daemon::ProjectEntry::new(root, std::process::id(), profile.map(str::to_string))
            .into();

    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let (listener, addr) = don::web::bind(addr).await.map_err(|e| {
        format!(
            "Error: {e}\n\
             Another process is likely using port {port} — pass `--with-ui=PORT` to pick another."
        )
    })?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(don::web::serve_single(listener, entry, shutdown_rx));

    println!("don web ui: http://{addr}/");
    Ok(WithUiGuard {
        _shutdown_tx: shutdown_tx,
    })
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
        if don::signals::signal_count() >= 2 {
            // Escalation requested. Do NOT abort yet: the runner polls the
            // same flag (100ms) and its force branch is what SIGKILLs child
            // process groups — aborting the task here races that sweep and
            // strands children as orphans, which is the one thing shutdown
            // must never do. A healthy runner finishes the sweep well inside
            // this window; the abort below is only for a wedged one.
            match tokio::time::timeout(std::time::Duration::from_secs(3), &mut handle).await {
                Ok(result) => return map_join_result(result, phase),
                Err(_) => {
                    errln(format!(
                        "runner unresponsive after force — exiting while {phase}"
                    ));
                    handle.abort();
                    let _ = handle.await;
                    return Err(format!("forced exit while {phase}"));
                }
            }
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
    use super::{
        Cli, Commands, parse_task_args, process_name, split_run_flags, status_sort_bucket,
    };
    use clap::Parser;
    use don::config::{ParamKind, TaskParam};
    use don::runner::{ProcessStatus, ServiceState, TaskState};

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

    fn service(name: &str, state: ServiceState) -> ProcessStatus {
        ProcessStatus::Service {
            runtime: None,
            name: name.to_string(),
            state,
            failed_dependencies: Vec::new(),
            verbose: None,
        }
    }

    fn task(name: &str, state: TaskState) -> ProcessStatus {
        ProcessStatus::Task {
            pid: None,
            name: name.to_string(),
            state,
            failed_dependencies: Vec::new(),
            last_run: None,
            verbose: None,
        }
    }

    #[test]
    fn ports_command_parses_json_flag() {
        struct Case {
            name: &'static str,
            args: &'static [&'static str],
            expected_json: bool,
        }

        let cases = [
            Case {
                name: "table output",
                args: &["don", "ports"],
                expected_json: false,
            },
            Case {
                name: "json output",
                args: &["don", "ports", "--json"],
                expected_json: true,
            },
        ];

        for case in cases {
            let cli = Cli::try_parse_from(case.args).unwrap();
            let Commands::Ports { json } = cli.command else {
                panic!("case '{}': expected ports command", case.name);
            };
            assert_eq!(json, case.expected_json, "case '{}'", case.name);
        }
    }

    #[test]
    fn count_tasks_pending_run_counts_only_pending_tasks() {
        let processes = vec![
            service("svc-ready", ServiceState::Ready),
            task("task-pending-a", TaskState::PendingRun),
            task("task-pending-b", TaskState::PendingRun),
            task("task-completed", TaskState::Completed),
            task("task-failed", TaskState::Failed),
        ];
        assert_eq!(super::count_tasks_pending_run(&processes), 2);

        let none = vec![
            service("svc-ready", ServiceState::Ready),
            task("task-completed", TaskState::Completed),
        ];
        assert_eq!(super::count_tasks_pending_run(&none), 0);

        assert_eq!(super::count_tasks_pending_run(&[]), 0);
    }

    #[test]
    fn status_sort_prioritizes_actionable_states() {
        struct Case {
            name: &'static str,
            processes: Vec<ProcessStatus>,
            want: Vec<&'static str>,
        }

        let cases = vec![Case {
            name: "mixed services and tasks",
            processes: vec![
                service("svc-ready", ServiceState::Ready),
                service("svc-building", ServiceState::Building),
                service("svc-running", ServiceState::Running),
                service("svc-stopped", ServiceState::Stopped),
                service("svc-lazy", ServiceState::Lazy),
                service("svc-failed", ServiceState::Failed),
                service("svc-dep", ServiceState::DependencyFailed),
                service("svc-stopping", ServiceState::Stopping),
                task("task-skipped", TaskState::Skipped),
                task("task-completed", TaskState::Completed),
                task("task-building", TaskState::Building),
                task("task-pending-run", TaskState::PendingRun),
                task("task-failed", TaskState::Failed),
                task("task-dep", TaskState::DependencyFailed),
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
            case.processes.sort_by(|a, b| {
                status_sort_bucket(a)
                    .cmp(&status_sort_bucket(b))
                    .then_with(|| process_name(a).cmp(process_name(b)))
            });
            let got: Vec<&str> = case.processes.iter().map(process_name).collect();
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
