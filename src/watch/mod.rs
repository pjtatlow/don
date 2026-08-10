//! File watching with per-service debounce and change-during-build handling.
//!
//! The [`WatchManager`] sets up `notify` watchers for services and tasks with
//! `watch` patterns, debounces events per-service, and sends [`RunnerCommand::Rebuild`]
//! or [`RunnerCommand::TaskRerun`] to the runner when a rebuild cycle should start.
//!
//! Each watched service has its own state machine:
//!
//! ```text
//! Idle → Debouncing → Rebuilding → Idle
//!                         ↓ (stale)
//!                      Rebuilding (another cycle)
//! ```
//!
//! The watch module subscribes to [`RunnerEvent::RebuildComplete`] to know when
//! a cycle finishes, and checks the `stale` flag to decide whether to immediately
//! start another cycle.

use crate::config::{Config, Platform};
use crate::duration::parse_duration;
use crate::globwalk::{matches_glob, matches_ignore};
use crate::output::LifecycleEmitter;
use crate::runner::{RunnerCommand, RunnerEvent};
use glob::Pattern;
use ignore::overrides::{Override, OverrideBuilder};
use notify::{EventKind, PathsMut, PollWatcher, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::Instant;

/// Default debounce window when none is configured.
const DEFAULT_DEBOUNCE: Duration = Duration::from_millis(200);
/// Synthetic watch item used for workspace-level build graph files.
pub(crate) const WORKSPACE_GRAPH_ITEM_NAME: &str = "__workspace_graph__";

/// Errors from the watch module.
#[derive(Debug, thiserror::Error)]
pub enum WatchError {
    #[error("notify error: {0}")]
    Notify(#[from] notify::Error),
    #[error("invalid debounce duration: {0}")]
    Duration(#[from] crate::duration::DurationError),
    #[error("failed to create watch directory {}: {}", .0.display(), .1)]
    Io(PathBuf, std::io::Error),
}

/// Per-item state machine for file-watch-triggered rebuilds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WatchState {
    /// No pending changes. Watching for events.
    Idle,
    /// Events received, waiting for debounce window to expire.
    Debouncing,
    /// A rebuild/rerun cycle is in progress.
    Rebuilding,
}

/// What command to send when this item's debounce timer fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WatchItemKind {
    /// Send `RunnerCommand::Rebuild { name }`.
    Service,
    /// Send `RunnerCommand::TaskRerun { name }`.
    Task,
    /// Send `RunnerCommand::BuildGraphChanged { name }` — tier-1 watch for
    /// build tool definition files (BUILD, package.json, etc.). No rebuild cycle;
    /// the runner re-queries the build tool and updates tier-2 watch patterns.
    BuildGraph,
}

/// Per-item watch tracking.
struct WatchedItem {
    state: WatchState,
    debounce_duration: Duration,
    /// When in Debouncing state, the deadline at which to fire.
    debounce_deadline: Option<Instant>,
    /// True when events arrived during a rebuild — triggers another cycle on completion.
    stale: bool,
    /// What kind of item this is — determines the command to send.
    kind: WatchItemKind,
    /// Glob patterns for matching file events.
    patterns: Vec<Pattern>,
    /// Glob patterns for ignoring file events (checked before watch patterns).
    ignore_patterns: Vec<Pattern>,
    /// Last diagnostic associated with this item's watch registration or state.
    last_error: Option<String>,
}

/// An update to the watch patterns for a specific item.
///
/// Sent from the runner to the watch manager after a build tool re-query
/// completes, containing the new tier-2 watch patterns.
pub(crate) struct WatchUpdate {
    /// The service or task name (matches the key in `items`).
    pub name: String,
    /// What kind of item this is (Service or Task). Used when creating
    /// a new watch item that didn't exist during initial setup.
    pub kind: WatchItemKind,
    /// New glob patterns to watch (replaces existing patterns).
    pub patterns: Vec<String>,
    /// New ignore patterns (replaces existing ignore patterns).
    pub ignore_patterns: Vec<String>,
    /// Base directory to resolve patterns against.
    pub base_dir: PathBuf,
    /// Optional completion signal sent once the watch manager has applied the
    /// update and registered any needed directories.
    pub applied_tx: Option<oneshot::Sender<()>>,
}

#[derive(Debug, Clone)]
pub(crate) struct WatchItemSnapshot {
    pub kind: &'static str,
    pub state: &'static str,
    pub stale: bool,
    pub debounce_ms: u64,
    pub last_error: Option<String>,
    /// Compiled (absolute) glob patterns this item matches file events against.
    pub patterns: Vec<String>,
    /// Item-specific ignore globs. Workspace-wide `watch_ignore` entries are
    /// reported once on the snapshot (see [`WatchSnapshot::global_ignore`]) and
    /// filtered out here so they aren't repeated under every item.
    pub ignore_patterns: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct WatchSnapshot {
    pub items: HashMap<String, WatchItemSnapshot>,
    /// The actual inotify registrations the watcher holds, as
    /// `(directory, mode)` where mode is `"recursive"` / `"non-recursive"`.
    /// This is the ground truth of what don is watching at the OS level.
    pub registered_dirs: Vec<(PathBuf, &'static str)>,
    /// Workspace-wide `watch_ignore` globs (apply to every item).
    pub global_ignore: Vec<String>,
    pub notify_error_count: u64,
    pub runner_event_lag_count: u64,
    pub last_notify_error: Option<String>,
}

pub(crate) struct WatchQuery {
    pub reply: oneshot::Sender<WatchSnapshot>,
}

/// Manages file watchers for all services and tasks with watch patterns.
///
/// Runs as a background tokio task, communicating with the runner via channels.
pub(crate) struct WatchManager {
    /// The notify watcher handle — kept alive to maintain watches.
    /// Named (not `_watcher`) so we can add new watch directories at runtime.
    watcher: Option<NotifyBackend>,
    /// Sender captured by the notify callback. Kept so deferred watch
    /// registration can allocate the backend only when a real directory exists.
    notify_tx: mpsc::UnboundedSender<notify::Result<notify::Event>>,
    /// Channel receiving raw notify events.
    event_rx: mpsc::UnboundedReceiver<notify::Result<notify::Event>>,
    /// Per-item (service or task) state.
    items: HashMap<String, WatchedItem>,
    /// Sender to the runner's command channel.
    cmd_tx: mpsc::UnboundedSender<RunnerCommand>,
    /// Receiver for runner events (rebuild/rerun completion).
    runner_events: broadcast::Receiver<RunnerEvent>,
    /// Receiver for watch pattern updates from the runner (build tool re-queries).
    update_rx: mpsc::UnboundedReceiver<WatchUpdate>,
    /// Receiver for debug/status queries from the runner.
    query_rx: mpsc::Receiver<WatchQuery>,
    /// Directories already registered with the watcher, keyed by path with
    /// the mode the watch was registered under.
    ///
    /// The mode matters for coverage checks: a NonRecursive watch at
    /// `redo/server` sees direct-child events only; it does NOT cover a
    /// subsequent Recursive request for the same path. Treating it as
    /// coverage causes the Recursive registration to be silently skipped,
    /// and nested files never trigger events.
    registered_dirs: HashMap<PathBuf, RecursiveMode>,
    /// Emitter for `[don]` verbose-mode diagnostics.
    emitter: LifecycleEmitter,
    /// Count of notify backend errors seen since startup.
    notify_error_count: u64,
    /// Count of broadcast lag incidents while consuming runner events.
    runner_event_lag_count: u64,
    /// Most recent notify backend error.
    last_notify_error: Option<String>,
    /// Workspace-wide `watch_ignore` patterns (resolved to absolute globs).
    /// A path matching any of these is dropped up front in [`Self::handle_event`]
    /// — it can't trigger anything and isn't worth a verbose log line, so we
    /// skip it before the (noisy) per-item match/ignore diagnostics. These are
    /// also merged into each item's `ignore_patterns` for the change-scan path;
    /// `watch_ignore` is fixed for the runner's lifetime, so this stays in sync.
    global_ignore: Vec<Pattern>,
    /// Workspace-wide `watch_ignore` compiled as an `ignore` override matcher,
    /// rooted at the (canonical) base dir. Used to prune ignored subtrees when
    /// deciding which directories to register with the notify backend — both at
    /// startup and when a new directory appears at runtime (see
    /// [`Self::register_new_directory`]). Fixed for the runner's lifetime.
    overrides: Override,
}

enum NotifyBackend {
    Native(RecommendedWatcher),
    Poll(PollWatcher),
}

impl NotifyBackend {
    fn label(&self) -> &'static str {
        match self {
            Self::Native(_) => "native",
            Self::Poll(_) => "poll",
        }
    }

    fn watch(&mut self, path: &Path, mode: RecursiveMode) -> notify::Result<()> {
        match self {
            Self::Native(watcher) => watcher.watch(path, mode),
            Self::Poll(watcher) => watcher.watch(path, mode),
        }
    }

    fn unwatch(&mut self, path: &Path) -> notify::Result<()> {
        match self {
            Self::Native(watcher) => watcher.unwatch(path),
            Self::Poll(watcher) => watcher.unwatch(path),
        }
    }

    fn paths_mut(&mut self) -> Box<dyn PathsMut + '_> {
        match self {
            Self::Native(watcher) => watcher.paths_mut(),
            Self::Poll(watcher) => watcher.paths_mut(),
        }
    }
}

impl WatchManager {
    /// Create a new watch manager from the config.
    ///
    /// Sets up notify watchers for all services and tasks with `watch` patterns.
    /// Creates missing watch directories so we get precise inotify coverage.
    ///
    /// Returns `(Self, warnings)` where warnings are non-fatal issues like
    /// invalid glob patterns (which should have been caught by validation).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        config: &Config,
        platform: Platform,
        base_dir: &Path,
        cmd_tx: mpsc::UnboundedSender<RunnerCommand>,
        runner_events: broadcast::Receiver<RunnerEvent>,
        update_rx: mpsc::UnboundedReceiver<WatchUpdate>,
        query_rx: mpsc::Receiver<WatchQuery>,
        emitter: LifecycleEmitter,
    ) -> Result<(Self, Vec<String>), WatchError> {
        let setup_started = Instant::now();
        emitter.debug_event(&format!(
            "watch: initial setup started base={} services={} tasks={} watch_ignore={} backend_preference={}",
            base_dir.display(),
            config.services.len(),
            config.tasks.len(),
            config.watch_ignore.len(),
            preferred_watcher_label()
        ));
        let mut warnings: Vec<String> = Vec::new();
        let (notify_tx, event_rx) = mpsc::unbounded_channel();

        // `follow_symlinks(false)` is load-bearing: a bazel workspace root
        // has `bazel-*` convenience symlinks into the user-wide bazel cache
        // (millions of generated files, thousands of external repos).
        // Without this, any `RecursiveMode::Recursive` registration at or
        // above the root walks the entire cache and blows through
        // `fs.inotify.max_user_watches` while blocking for minutes.
        let mut watcher = None;

        // Canonicalize base_dir so glob patterns are absolute and match the
        // absolute paths that notify reports in events. Without this, a base_dir
        // of `.` produces patterns like `./definitions/**/*.sql` that don't match
        // the absolute paths notify returns.
        let canonicalize_started = Instant::now();
        let base_dir = std::fs::canonicalize(base_dir)
            .map_err(|e| WatchError::Io(base_dir.to_path_buf(), e))?;
        let base_dir = base_dir.as_path();
        emitter.debug_event(&format!(
            "watch: canonicalized base={} elapsed={:?}",
            base_dir.display(),
            canonicalize_started.elapsed()
        ));
        let mut global_ignore_patterns: Vec<String> = Vec::new();
        for pattern in &config.watch_ignore {
            global_ignore_patterns.push(
                resolve_pattern(base_dir, pattern)
                    .to_string_lossy()
                    .into_owned(),
            );
            // A contents glob (`node_modules/**`) matches files *inside* the dir
            // but not the bare directory itself, so the directory's own creation
            // event would slip past the ignore filter and be logged as noise.
            // Also ignore the directory node, mirroring the walk-pruning in
            // `build_watch_ignore_overrides` so the two dialects stay in sync.
            if let Some(dir) = pattern.strip_suffix("/**") {
                global_ignore_patterns.push(
                    resolve_pattern(base_dir, dir)
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }

        // Compile `watch_ignore` into an `ignore` override matcher rooted at the
        // canonical base dir. This is what prunes ignored subtrees when we walk
        // to decide which directories to register with the notify backend.
        let ignore_setup_started = Instant::now();
        let overrides = build_watch_ignore_overrides(base_dir, &config.watch_ignore, &mut warnings);
        emitter.debug_event(&format!(
            "watch: ignore setup complete resolved_patterns={} warnings={} elapsed={:?}",
            global_ignore_patterns.len(),
            warnings.len(),
            ignore_setup_started.elapsed()
        ));

        let mut items: HashMap<String, WatchedItem> = HashMap::new();
        // Track which directories we've already registered, with the mode we
        // registered each under. See `WatchManager::registered_dirs` for why
        // the mode matters.
        let mut registered_dirs: HashMap<PathBuf, RecursiveMode> = HashMap::new();
        // Collect every service/task request before touching the backend. On
        // macOS this lets us discard descendant roots covered by a recursive
        // ancestor and add the minimal set to FSEvents in one stream rebuild.
        let mut initial_desired_watches: Vec<(PathBuf, RecursiveMode)> = Vec::new();

        // Process services.
        for (name, svc) in &config.services {
            let resolved = svc.resolve(platform);

            // Skip services that handle their own hot-reloading.
            if !resolved.reload {
                continue;
            }

            // Use configured watch patterns, or inject preset defaults.
            let watch_patterns: Vec<String> = if !resolved.watch.is_empty() {
                resolved.watch.clone()
            } else if resolved.rust_config().is_some() {
                vec![
                    "src/**/*.rs".to_string(),
                    "Cargo.toml".to_string(),
                    "Cargo.lock".to_string(),
                ]
            } else if resolved.go_config().is_some() {
                vec![
                    "**/*.go".to_string(),
                    "go.mod".to_string(),
                    "go.sum".to_string(),
                ]
            } else {
                // Docker and custom services require explicit watch config.
                Vec::new()
            };
            if watch_patterns.is_empty() {
                continue;
            }

            let item_setup_started = Instant::now();
            let desired_before = initial_desired_watches.len();
            emitter.service_debug_event(
                name,
                &format!(
                    "watch: initial service setup started patterns={watch_patterns:?} dir={:?}",
                    resolved.dir
                ),
            );

            // Resolve svc_dir relative to the (canonical) base_dir so patterns
            // are absolute and can match notify's absolute event paths.
            // Canonicalize to eliminate `./` components (e.g. dir = "./app"
            // joined with base_dir would produce `/foo/./app` which won't
            // match notify's canonical event paths).
            let svc_dir = match resolved.dir.as_deref() {
                Some(d) => {
                    let joined = base_dir.join(d);
                    std::fs::canonicalize(&joined).unwrap_or(joined)
                }
                None => base_dir.to_path_buf(),
            };

            let debounce = match &resolved.debounce {
                Some(d) => parse_duration(d)?,
                None => DEFAULT_DEBOUNCE,
            };

            let mut compiled_patterns = Vec::new();
            for pattern_str in &watch_patterns {
                let full_pattern = resolve_pattern(&svc_dir, pattern_str);
                match Pattern::new(&full_pattern.to_string_lossy()) {
                    Ok(pat) => compiled_patterns.push(pat),
                    Err(e) => {
                        warnings.push(format!(
                            "{name}: invalid watch pattern '{pattern_str}': {e}"
                        ));
                        continue;
                    }
                }

                let watch_dir = glob_base_dir(&full_pattern);
                collect_initial_dirs_ignoring(
                    &watch_dir,
                    &overrides,
                    &mut initial_desired_watches,
                    InitialWatchRegistrationLog {
                        emitter: &emitter,
                        item: name,
                        pattern: pattern_str,
                    },
                )?;
            }

            let mut compiled_ignore = Vec::new();
            let ignore_patterns: Vec<String> = resolved
                .ignore
                .iter()
                .cloned()
                .chain(global_ignore_patterns.iter().cloned())
                .collect();
            for pattern_str in &ignore_patterns {
                let full_pattern = resolve_pattern(&svc_dir, pattern_str);
                match Pattern::new(&full_pattern.to_string_lossy()) {
                    Ok(pat) => compiled_ignore.push(pat),
                    Err(e) => {
                        warnings.push(format!(
                            "{name}: invalid ignore pattern '{pattern_str}': {e}"
                        ));
                    }
                }
            }

            items.insert(
                name.clone(),
                WatchedItem {
                    state: WatchState::Idle,
                    debounce_duration: debounce,
                    debounce_deadline: None,
                    stale: false,
                    kind: WatchItemKind::Service,
                    patterns: compiled_patterns,
                    ignore_patterns: compiled_ignore,
                    last_error: None,
                },
            );
            emitter.service_debug_event(
                name,
                &format!(
                    "watch: initial service setup complete requested_directories={} total_requests={} elapsed={:?}",
                    initial_desired_watches.len().saturating_sub(desired_before),
                    initial_desired_watches.len(),
                    item_setup_started.elapsed()
                ),
            );
        }

        // Process tasks.
        for (name, task) in &config.tasks {
            if task.watch.is_empty() {
                continue;
            }

            let item_setup_started = Instant::now();
            let desired_before = initial_desired_watches.len();
            emitter.service_debug_event(
                name,
                &format!(
                    "watch: initial task setup started patterns={:?} dir={:?}",
                    task.watch, task.dir
                ),
            );

            let task_dir = match task.dir.as_deref() {
                Some(d) => {
                    let joined = base_dir.join(d);
                    std::fs::canonicalize(&joined).unwrap_or(joined)
                }
                None => base_dir.to_path_buf(),
            };

            let mut compiled_patterns = Vec::new();
            for pattern_str in &task.watch {
                let full_pattern = resolve_pattern(&task_dir, pattern_str);
                match Pattern::new(&full_pattern.to_string_lossy()) {
                    Ok(pat) => compiled_patterns.push(pat),
                    Err(e) => {
                        warnings.push(format!(
                            "{name}: invalid watch pattern '{pattern_str}': {e}"
                        ));
                        continue;
                    }
                }

                let watch_dir = glob_base_dir(&full_pattern);
                collect_initial_dirs_ignoring(
                    &watch_dir,
                    &overrides,
                    &mut initial_desired_watches,
                    InitialWatchRegistrationLog {
                        emitter: &emitter,
                        item: name,
                        pattern: pattern_str,
                    },
                )?;
            }

            let mut compiled_ignore = Vec::new();
            let ignore_patterns: Vec<String> = task
                .ignore
                .iter()
                .cloned()
                .chain(global_ignore_patterns.iter().cloned())
                .collect();
            for pattern_str in &ignore_patterns {
                let full_pattern = resolve_pattern(&task_dir, pattern_str);
                match Pattern::new(&full_pattern.to_string_lossy()) {
                    Ok(pat) => compiled_ignore.push(pat),
                    Err(e) => {
                        warnings.push(format!(
                            "{name}: invalid ignore pattern '{pattern_str}': {e}"
                        ));
                    }
                }
            }

            items.insert(
                name.clone(),
                WatchedItem {
                    state: WatchState::Idle,
                    debounce_duration: DEFAULT_DEBOUNCE, // Tasks use default debounce.
                    debounce_deadline: None,
                    stale: false,
                    kind: WatchItemKind::Task,
                    patterns: compiled_patterns,
                    ignore_patterns: compiled_ignore,
                    last_error: None,
                },
            );
            emitter.service_debug_event(
                name,
                &format!(
                    "watch: initial task setup complete requested_directories={} total_requests={} elapsed={:?}",
                    initial_desired_watches.len().saturating_sub(desired_before),
                    initial_desired_watches.len(),
                    item_setup_started.elapsed()
                ),
            );
        }

        apply_initial_watch_batch(
            &mut watcher,
            &notify_tx,
            initial_desired_watches,
            &mut registered_dirs,
            &emitter,
        )?;

        // Register tier-1 build graph watches for workspace-level files.
        //
        // Per-package BUILD / package.json watches are NOT seeded here —
        // they're registered lazily via `WatchUpdate { kind: BuildGraph, .. }`
        // once `run_batch_build_chain` resolves the actual package list from
        // `bazel query` / `turbo run --dry-run`. Seeding them from `**/BUILD`
        // would force a recursive `watcher.watch` on the workspace root,
        // which follows `bazel-*` symlinks into the bazel cache and takes
        // minutes on large monorepos (3,000+ external repos under
        // `execroot/_main/external/`).
        //
        // What IS seeded: a single non-recursive watch on the workspace root
        // for workspace-level files (WORKSPACE, MODULE.bazel, turbo.json,
        // pnpm-workspace.yaml). These change rarely but must trigger a full
        // build-graph re-query.
        {
            let has_bazel = config.services.values().any(|s| {
                let resolved = s.resolve(platform);
                resolved
                    .bazel_config()
                    .is_some_and(|bazel| resolved.reload && bazel.watch)
            }) || config
                .tasks
                .values()
                .any(|t| t.bazel.as_ref().is_some_and(|bazel| bazel.watch));
            let has_turbo = config.services.values().any(|s| {
                let resolved = s.resolve(platform);
                resolved
                    .turbo_config()
                    .is_some_and(|turbo| resolved.reload && turbo.watch)
            }) || config
                .tasks
                .values()
                .any(|t| t.turbo.as_ref().is_some_and(|turbo| turbo.watch));

            if has_bazel || has_turbo {
                let graph_setup_started = Instant::now();
                emitter.debug_event(&format!(
                    "watch: workspace graph setup started bazel={has_bazel} turbo={has_turbo} root={}",
                    base_dir.display()
                ));
                let mut root_file_names: Vec<&str> = Vec::new();
                if has_bazel {
                    root_file_names.extend(["WORKSPACE", "WORKSPACE.bazel", "MODULE.bazel"]);
                }
                if has_turbo {
                    root_file_names.extend(["turbo.json", "turbo.jsonc", "pnpm-workspace.yaml"]);
                }

                let mut compiled_patterns = Vec::new();
                for file_name in &root_file_names {
                    let full_pattern = resolve_pattern(base_dir, file_name);
                    if let Ok(pat) = Pattern::new(&full_pattern.to_string_lossy()) {
                        compiled_patterns.push(pat);
                    }
                }
                let compiled_ignore: Vec<Pattern> = global_ignore_patterns
                    .iter()
                    .filter_map(|pattern| Pattern::new(pattern).ok())
                    .collect();

                // Non-recursive watch on the workspace root is enough for
                // these specific filenames. No symlink spelunking.
                if !is_covered(base_dir, RecursiveMode::NonRecursive, &registered_dirs) {
                    let registration_started = Instant::now();
                    emitter.debug_event(&format!(
                        "watch: workspace graph backend registration started root={} mode=non-recursive backend={}",
                        base_dir.display(),
                        watcher_label(watcher.as_ref())
                    ));
                    match register_single_dir(
                        &mut watcher,
                        &notify_tx,
                        base_dir,
                        RecursiveMode::NonRecursive,
                        &mut registered_dirs,
                    ) {
                        Ok(()) => {
                            emitter.debug_event(&format!(
                                "watch: workspace graph backend registration complete root={} backend={} elapsed={:?}",
                                base_dir.display(),
                                watcher_label(watcher.as_ref()),
                                registration_started.elapsed()
                            ));
                        }
                        Err(e) => warnings.push(format!(
                            "workspace watch registration failed for {}: {e}",
                            base_dir.display()
                        )),
                    }
                }

                let compiled_pattern_count = compiled_patterns.len();
                if !compiled_patterns.is_empty() {
                    items.insert(
                        WORKSPACE_GRAPH_ITEM_NAME.to_string(),
                        WatchedItem {
                            state: WatchState::Idle,
                            debounce_duration: DEFAULT_DEBOUNCE,
                            debounce_deadline: None,
                            stale: false,
                            kind: WatchItemKind::BuildGraph,
                            patterns: compiled_patterns,
                            ignore_patterns: compiled_ignore,
                            last_error: None,
                        },
                    );
                }
                emitter.debug_event(&format!(
                    "watch: workspace graph setup complete patterns={} total_directories={} elapsed={:?}",
                    compiled_pattern_count,
                    registered_dirs.len(),
                    graph_setup_started.elapsed()
                ));
            }
        }

        // Verbose setup summary: per-item patterns/ignore/debounce, plus the
        // full list of registered directories. This is the first thing a user
        // hitting "nothing reloaded" will want to see.
        let mut names: Vec<&String> = items.keys().collect();
        names.sort();
        for name in names {
            let Some(item) = items.get(name) else {
                continue;
            };
            let pats: Vec<&str> = item.patterns.iter().map(Pattern::as_str).collect();
            let igs: Vec<&str> = item.ignore_patterns.iter().map(Pattern::as_str).collect();
            emitter.service_debug_event(
                name,
                &format!(
                    "watch: registered kind={:?} debounce={:?} patterns={:?} ignore={:?}",
                    item.kind, item.debounce_duration, pats, igs
                ),
            );
        }
        let mut dirs: Vec<(&PathBuf, &RecursiveMode)> = registered_dirs.iter().collect();
        dirs.sort_by(|a, b| a.0.cmp(b.0));
        for (dir, mode) in &dirs {
            emitter.debug_event(&format!("watch: backend dir {:?} mode={:?}", dir, mode));
        }
        emitter.debug_event(&format!(
            "watch: setup complete — {} items, {} directories registered, backend={}, elapsed={:?}",
            items.len(),
            registered_dirs.len(),
            watcher_label(watcher.as_ref()),
            setup_started.elapsed()
        ));

        let global_ignore: Vec<Pattern> = global_ignore_patterns
            .iter()
            .filter_map(|pattern| Pattern::new(pattern).ok())
            .collect();

        Ok((
            Self {
                watcher,
                notify_tx,
                event_rx,
                items,
                cmd_tx,
                runner_events,
                update_rx,
                query_rx,
                registered_dirs,
                emitter,
                notify_error_count: 0,
                runner_event_lag_count: 0,
                last_notify_error: None,
                global_ignore,
                overrides,
            },
            warnings,
        ))
    }

    /// Returns true if there are any items being watched.
    pub(crate) fn has_watches(&self) -> bool {
        !self.items.is_empty()
    }

    /// Run the watch event loop until the runner shuts down.
    ///
    /// This consumes the manager and runs until channels close.
    pub(crate) async fn run(mut self) {
        loop {
            let next_deadline = self.nearest_debounce_deadline();

            tokio::select! {
                Some(event_result) = self.event_rx.recv() => {
                    match event_result {
                        Ok(event) => self.handle_notify_event(&event).await,
                        Err(err) => self.record_notify_error(&err.to_string()),
                    }
                }
                _ = sleep_until_or_pending(next_deadline) => {
                    self.fire_debounce_timers().await;
                }
                result = self.runner_events.recv() => {
                    match result {
                        Ok(event) => self.handle_runner_event(&event).await,
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            // Missed n events. If one was a RebuildComplete,
                            // the corresponding WatchedItem is stuck in
                            // `Rebuilding`. A pattern update cannot safely reset
                            // it because updates also race active rebuilds.
                            self.runner_event_lag_count =
                                self.runner_event_lag_count.saturating_add(n);
                            self.emitter.lifecycle_event(&format!(
                                "watch: broadcast lag — missed {n} runner events; restart don if edits stop triggering rebuilds"
                            ));
                        }
                    }
                }
                Some(update) = self.update_rx.recv() => {
                    self.apply_watch_update(update);
                }
                Some(query) = self.query_rx.recv() => {
                    let _ = query.reply.send(self.snapshot());
                }
            }
        }
    }

    /// Apply a watch update from the runner (build tool re-query completed).
    ///
    /// Replaces the watch patterns for the named item and registers any
    /// new watch directories with the notify watcher.
    fn apply_watch_update(&mut self, mut update: WatchUpdate) {
        // Canonicalize the base so the compiled globs are absolute and match
        // the cwd-prefixed absolute paths that notify reports in events.
        // Without this, a runner base_dir of `.` produces patterns like
        // `./auth/jwt/**` that will never match `/abs/cwd/./auth/jwt/foo.ts`.
        // The initial-setup path in `WatchManager::new` already canonicalizes;
        // this keeps the build-tool-resolved update path in sync.
        let base_dir =
            std::fs::canonicalize(&update.base_dir).unwrap_or_else(|_| update.base_dir.clone());

        let mut compiled_patterns = Vec::new();
        for pattern_str in &update.patterns {
            let full_pattern = resolve_pattern(&base_dir, pattern_str);
            if let Ok(pat) = Pattern::new(&full_pattern.to_string_lossy()) {
                compiled_patterns.push(pat);

                // Always register the watch directory for build-tool-resolved
                // patterns. A parent recursive watch may not reliably cover
                // all subdirectories (e.g. when bazel symlinks cause inotify
                // to miss directories during the initial recursive walk).
                let watch_dir = glob_base_dir(&full_pattern);
                if watch_dir.exists() {
                    let result = match update.kind {
                        // Tier-1 BuildGraph updates land on specific filename
                        // patterns (`<pkg>/BUILD`, `<pkg>/package.json`) — a
                        // non-recursive watch on the package directory is
                        // exactly right, no subtree walk.
                        WatchItemKind::BuildGraph => register_single_dir(
                            &mut self.watcher,
                            &self.notify_tx,
                            &watch_dir,
                            RecursiveMode::NonRecursive,
                            &mut self.registered_dirs,
                        ),
                        // Tier-2 Service/Task updates are directory-level globs
                        // (`<pkg>/**`) that need recursive coverage — routed
                        // through the same `watch_ignore`-aware registration as
                        // startup so ignored subtrees stay pruned on reload.
                        WatchItemKind::Service | WatchItemKind::Task => register_dirs_ignoring(
                            &mut self.watcher,
                            &self.notify_tx,
                            &watch_dir,
                            &self.overrides,
                            &mut self.registered_dirs,
                        ),
                    };
                    if let Err(e) = result {
                        self.record_item_error(
                            &update.name,
                            format!(
                                "watch: failed to register watch for {}: {e}",
                                watch_dir.display()
                            ),
                        );
                    }
                }
            }
        }

        let mut compiled_ignore = Vec::new();
        for pattern_str in &update.ignore_patterns {
            let full_pattern = resolve_pattern(&base_dir, pattern_str);
            if let Ok(pat) = Pattern::new(&full_pattern.to_string_lossy()) {
                compiled_ignore.push(pat);
            }
        }

        if let Some(item) = self.items.get_mut(&update.name) {
            refresh_item_definition(item, update.kind, compiled_patterns, compiled_ignore);
            let pats: Vec<&str> = item.patterns.iter().map(Pattern::as_str).collect();
            let igs: Vec<&str> = item.ignore_patterns.iter().map(Pattern::as_str).collect();
            self.emitter.service_debug_event(
                &update.name,
                &format!(
                    "watch: patterns updated kind={:?} patterns={:?} ignore={:?}",
                    update.kind, pats, igs
                ),
            );
        } else {
            // Item doesn't exist yet — create it (happens when build tool
            // resolution completes after startup for a service with no
            // explicit watch patterns).
            let pats: Vec<&str> = compiled_patterns.iter().map(Pattern::as_str).collect();
            let igs: Vec<&str> = compiled_ignore.iter().map(Pattern::as_str).collect();
            self.emitter.service_debug_event(
                &update.name,
                &format!(
                    "watch: item created kind={:?} patterns={:?} ignore={:?}",
                    update.kind, pats, igs
                ),
            );
            self.items.insert(
                update.name.clone(),
                WatchedItem {
                    state: WatchState::Idle,
                    debounce_duration: DEFAULT_DEBOUNCE,
                    debounce_deadline: None,
                    stale: false,
                    kind: update.kind,
                    patterns: compiled_patterns,
                    ignore_patterns: compiled_ignore,
                    last_error: None,
                },
            );
        }

        if let Some(applied_tx) = update.applied_tx.take() {
            let _ = applied_tx.send(());
        }
    }

    /// Find the soonest debounce deadline across all items.
    fn nearest_debounce_deadline(&self) -> Option<Instant> {
        self.items
            .values()
            .filter(|item| item.state == WatchState::Debouncing)
            .filter_map(|item| item.debounce_deadline)
            .min()
    }

    /// Route a notify event to the affected items and update their state machines.
    async fn handle_notify_event(&mut self, event: &notify::Event) {
        // Only care about create, modify, and remove events. Renames
        // (vim, sed -i) are reported as Modify(Name(_)) by notify.
        if !matches!(
            event.kind,
            EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
        ) {
            // Don't log these — Access/Other events fire constantly (every
            // open/close/stat) and drown out the signal.
            return;
        }

        // Workspace-wide `watch_ignore` is a top-level filter: a matching path
        // can't trigger any item. Drop such paths up front and *before* any
        // logging — emitting verbose diagnostics for files the user globally
        // ignored (node_modules, build output, …) is pure noise. If every path
        // in the event is ignored, there's nothing to report at all.
        let paths: Vec<PathBuf> = event
            .paths
            .iter()
            .filter(|path| {
                let path_str = path.to_string_lossy();
                !self
                    .global_ignore
                    .iter()
                    .any(|pattern| matches_ignore(pattern, &path_str))
            })
            .cloned()
            .collect();
        if paths.is_empty() {
            return;
        }

        self.emitter.debug_event(&format!(
            "watch: event kind={:?} paths={:?}",
            event.kind, paths
        ));

        // Backstop for the recursion we deliberately gave up by using only
        // non-recursive watches: when a new directory appears, notify won't
        // auto-watch it, so register it here (skipping ignored dirs) and replay
        // any files that already landed inside before the watch took effect
        // (e.g. `git checkout` of a branch that adds a whole tree). An ignored
        // directory created at runtime — a fresh `node_modules` — is skipped
        // here and never watched.
        if matches!(event.kind, EventKind::Create(_)) {
            for path in &paths {
                self.register_new_directory(path);
            }
        }

        self.process_changed_paths(&paths);
    }

    /// Match changed paths against watched items and advance their debounce
    /// state machines. Shared by live notify events and the new-directory
    /// backstop (which replays pre-existing files under a freshly-watched dir).
    fn process_changed_paths(&mut self, paths: &[PathBuf]) {
        // Find which items are affected by this event's paths.
        // Ignore patterns are checked first — if any ignore pattern matches,
        // the event is skipped for that item.
        let mut affected: Vec<String> = Vec::new();
        for path in paths {
            let path_str = path.to_string_lossy();
            let mut matched_any = false;
            let mut ignored_by: Vec<String> = Vec::new();
            let mut unmatched: Vec<String> = Vec::new();
            for (name, item) in &self.items {
                let state = watch_state_label(item.state);
                if let Some(ig) = item
                    .ignore_patterns
                    .iter()
                    .find(|pattern| matches_ignore(pattern, &path_str))
                {
                    self.emitter.service_debug_event(
                        name,
                        &format!(
                            "watch: ignored path={:?} state={} ignore={:?}",
                            path,
                            state,
                            ig.as_str()
                        ),
                    );
                    ignored_by.push(name.clone());
                    continue;
                }
                if let Some(pat) = item
                    .patterns
                    .iter()
                    .find(|pattern| matches_glob(pattern, &path_str))
                {
                    self.emitter.service_debug_event(
                        name,
                        &format!(
                            "watch: matched path={:?} state={} pattern={:?}",
                            path,
                            state,
                            pat.as_str()
                        ),
                    );
                    matched_any = true;
                    if !affected.contains(name) {
                        affected.push(name.clone());
                    }
                } else {
                    unmatched.push(name.clone());
                }
            }
            if !matched_any {
                if ignored_by.is_empty() {
                    self.emitter
                        .debug_event(&format!("watch: no item matched {:?}", path));
                } else {
                    self.emitter.debug_event(&format!(
                        "watch: no rebuild match for {:?} (ignored by {})",
                        path,
                        ignored_by.join(", ")
                    ));
                }
                for name in unmatched {
                    if let Some(item) = self.items.get(&name) {
                        self.emitter.service_debug_event(
                            &name,
                            &format!(
                                "watch: did not match path={:?} state={} reason=no pattern matched",
                                path,
                                watch_state_label(item.state)
                            ),
                        );
                    }
                }
            }
        }

        let now = Instant::now();
        let mut stale_services: Vec<String> = Vec::new();
        for name in affected {
            if let Some(item) = self.items.get_mut(&name) {
                match item.state {
                    // Idle → Debouncing: first change starts the debounce window.
                    WatchState::Idle => {
                        item.state = WatchState::Debouncing;
                        item.debounce_deadline = Some(now + item.debounce_duration);
                        self.emitter.service_debug_event(
                            &name,
                            &format!(
                                "watch: Idle → Debouncing (deadline in {:?})",
                                item.debounce_duration
                            ),
                        );
                    }
                    // Debouncing → Debouncing: sliding window resets the deadline
                    // so rapid consecutive saves coalesce into one rebuild.
                    WatchState::Debouncing => {
                        item.debounce_deadline = Some(now + item.debounce_duration);
                        self.emitter.service_debug_event(
                            &name,
                            &format!(
                                "watch: Debouncing — deadline bumped ({:?})",
                                item.debounce_duration
                            ),
                        );
                    }
                    // Rebuilding: can't start another cycle now. Set stale so we
                    // trigger a new rebuild when the current one completes.
                    WatchState::Rebuilding => {
                        item.stale = true;
                        if item.kind == WatchItemKind::Service && !stale_services.contains(&name) {
                            stale_services.push(name.clone());
                        }
                        self.emitter.service_debug_event(
                            &name,
                            "watch: Rebuilding — marked stale (will re-run after completion)",
                        );
                    }
                }
            }
        }

        for name in stale_services {
            let _ = self.cmd_tx.send(RunnerCommand::RebuildStale { name });
        }
    }

    /// Register a directory that appeared at runtime, then replay files already
    /// inside it.
    ///
    /// Because all watches are non-recursive (see [`register_dirs_ignoring`]),
    /// notify does not auto-watch new subdirectories — a directory created at
    /// runtime would otherwise go unwatched. This registers the new subtree
    /// (skipping ignored directories) and, because files may have been written
    /// between the directory's creation and our watch taking effect, feeds any
    /// pre-existing files through the normal matching path. A no-op when the
    /// path isn't a directory, is already covered by an existing watch, or is
    /// itself ignored — so a fresh `node_modules` is never watched.
    fn register_new_directory(&mut self, path: &Path) {
        let is_dir = std::fs::metadata(path).map(|m| m.is_dir()).unwrap_or(false);
        if !is_dir {
            return;
        }
        // Already covered by an exact watch (or a recursive ancestor, e.g. a
        // build-tool-registered dir) — nothing to do.
        if is_covered(path, RecursiveMode::NonRecursive, &self.registered_dirs) {
            return;
        }
        // Never watch a directory the user asked to ignore.
        if self.overrides.matched(path, true).is_ignore() {
            return;
        }

        if let Err(e) = register_dirs_ignoring(
            &mut self.watcher,
            &self.notify_tx,
            path,
            &self.overrides,
            &mut self.registered_dirs,
        ) {
            self.emitter.debug_event(&format!(
                "watch: failed to register new directory {:?}: {e}",
                path
            ));
            return;
        }
        self.emitter
            .debug_event(&format!("watch: registered new directory {:?}", path));

        // Replay files that already exist under the new subtree so a burst that
        // populated it before the watch landed still triggers the right rebuild.
        let mut files = Vec::new();
        collect_files_recursive(path, &self.overrides, &mut files);
        if !files.is_empty() {
            self.process_changed_paths(&files);
        }
    }

    /// Fire debounce timers that have expired — send rebuild/rerun commands.
    async fn fire_debounce_timers(&mut self) {
        let now = Instant::now();
        let mut to_fire: Vec<(String, WatchItemKind)> = Vec::new();

        for (name, item) in &self.items {
            if item.state == WatchState::Debouncing
                && let Some(deadline) = item.debounce_deadline
                && now >= deadline
            {
                to_fire.push((name.clone(), item.kind));
            }
        }

        for (name, kind) in to_fire {
            if let Some(item) = self.items.get_mut(&name) {
                item.debounce_deadline = None;

                let (cmd, label) = match kind {
                    WatchItemKind::Task => {
                        item.state = WatchState::Rebuilding;
                        (RunnerCommand::TaskRerun { name: name.clone() }, "TaskRerun")
                    }
                    WatchItemKind::Service => {
                        item.state = WatchState::Rebuilding;
                        (RunnerCommand::Rebuild { name: name.clone() }, "Rebuild")
                    }
                    WatchItemKind::BuildGraph => {
                        // Build graph change has no rebuild/complete cycle —
                        // the runner re-queries the build tool asynchronously.
                        // Extract the service/task name by stripping "__graph" suffix.
                        item.state = WatchState::Idle;
                        let item_name = build_graph_command_name(&name);
                        (
                            RunnerCommand::BuildGraphChanged { name: item_name },
                            "BuildGraphChanged",
                        )
                    }
                };
                self.emitter.service_debug_event(
                    &name,
                    &format!(
                        "watch: debounce fired → sending {} (state={:?})",
                        label, item.state
                    ),
                );
                // If the channel is closed, the runner is shutting down.
                if self.cmd_tx.send(cmd).is_err() {
                    self.emitter.service_debug_event(
                        &name,
                        "watch: command channel closed — runner is shutting down",
                    );
                }
            }
        }
    }

    /// Handle a runner event — mainly looking for rebuild/rerun completion.
    async fn handle_runner_event(&mut self, event: &RunnerEvent) {
        match event {
            RunnerEvent::RebuildComplete { name, success } => {
                if let Some(item) = self.items.get_mut(name) {
                    if item.stale {
                        // More changes came in during the rebuild — trigger another cycle.
                        item.stale = false;
                        item.state = WatchState::Rebuilding;
                        self.emitter.service_debug_event(
                            name,
                            &format!(
                                "watch: RebuildComplete(success={success}) stale=true — re-running"
                            ),
                        );
                        let _ = self
                            .cmd_tx
                            .send(RunnerCommand::Rebuild { name: name.clone() });
                    } else {
                        item.state = WatchState::Idle;
                        self.emitter.service_debug_event(
                            name,
                            &format!("watch: RebuildComplete(success={success}) → Idle"),
                        );
                    }
                } else {
                    self.emitter
                        .debug_event(&format!("watch: RebuildComplete for unknown item {name:?}"));
                }
            }
            RunnerEvent::TaskRerunComplete { name, success } => {
                if let Some(item) = self.items.get_mut(name) {
                    if item.stale {
                        item.stale = false;
                        item.state = WatchState::Rebuilding;
                        self.emitter.service_debug_event(
                            name,
                            &format!(
                                "watch: TaskRerunComplete(success={success}) stale=true — re-running"
                            ),
                        );
                        let _ = self
                            .cmd_tx
                            .send(RunnerCommand::TaskRerun { name: name.clone() });
                    } else {
                        item.state = WatchState::Idle;
                        self.emitter.service_debug_event(
                            name,
                            &format!("watch: TaskRerunComplete(success={success}) → Idle"),
                        );
                    }
                } else {
                    self.emitter.debug_event(&format!(
                        "watch: TaskRerunComplete for unknown item {name:?}"
                    ));
                }
            }
            RunnerEvent::ShutdownComplete => {
                // Stop watching.
            }
            _ => {}
        }
    }

    fn snapshot(&self) -> WatchSnapshot {
        // Global `watch_ignore` is merged into every item's `ignore_patterns` at
        // setup. Filter it back out per item so the report shows item-specific
        // ignores only and lists the workspace-wide set once.
        let global_set: std::collections::HashSet<&str> =
            self.global_ignore.iter().map(Pattern::as_str).collect();

        let items = self
            .items
            .iter()
            .map(|(name, item)| {
                (
                    name.clone(),
                    WatchItemSnapshot {
                        kind: watch_item_kind_label(item.kind),
                        state: watch_state_label(item.state),
                        stale: item.stale,
                        debounce_ms: item.debounce_duration.as_millis() as u64,
                        last_error: item.last_error.clone(),
                        patterns: item
                            .patterns
                            .iter()
                            .map(|p| p.as_str().to_string())
                            .collect(),
                        ignore_patterns: item
                            .ignore_patterns
                            .iter()
                            .map(Pattern::as_str)
                            .filter(|p| !global_set.contains(p))
                            .map(str::to_string)
                            .collect(),
                    },
                )
            })
            .collect();

        let registered_dirs = self
            .registered_dirs
            .iter()
            .map(|(dir, mode)| (dir.clone(), recursive_mode_label(*mode)))
            .collect();

        WatchSnapshot {
            items,
            registered_dirs,
            global_ignore: self
                .global_ignore
                .iter()
                .map(|p| p.as_str().to_string())
                .collect(),
            notify_error_count: self.notify_error_count,
            runner_event_lag_count: self.runner_event_lag_count,
            last_notify_error: self.last_notify_error.clone(),
        }
    }

    fn record_notify_error(&mut self, error: &str) {
        self.notify_error_count = self.notify_error_count.saturating_add(1);
        self.last_notify_error = Some(error.to_string());
        self.emitter
            .lifecycle_event(&format!("watch: notify backend error: {error}"));
    }

    fn record_item_error(&mut self, name: &str, error: String) {
        if let Some(item) = self.items.get_mut(name) {
            item.last_error = Some(error.clone());
        }
        self.emitter.service_debug_event(name, &error);
        self.emitter.lifecycle_event(&error);
    }
}

/// Is a watch on `path` with `mode` already covered by something in `existing`?
///
/// A `Recursive` request is covered only by a `Recursive` ancestor (including
/// exact-match). A `NonRecursive` ancestor sees only direct-child events and
/// does NOT cover descendants.
///
/// A `NonRecursive` request is covered by a `Recursive` ancestor (which sees
/// everything under it), or by an exact same-path watch of any mode (which
/// already sees direct-child events at `path`).
fn is_covered(
    path: &Path,
    mode: RecursiveMode,
    existing: &HashMap<PathBuf, RecursiveMode>,
) -> bool {
    if existing
        .iter()
        .any(|(dir, m)| *m == RecursiveMode::Recursive && path.starts_with(dir))
    {
        return true;
    }
    if mode == RecursiveMode::NonRecursive && existing.contains_key(path) {
        return true;
    }
    false
}

/// Build an `ignore` override matcher from the workspace `watch_ignore` globs,
/// rooted at `base_dir` so patterns anchor consistently no matter which subtree
/// is later walked. Invalid globs are reported into `warnings` rather than
/// silently dropped, so a typo'd ignore surfaces to the user.
fn build_watch_ignore_overrides(
    base_dir: &Path,
    watch_ignore: &[String],
    warnings: &mut Vec<String>,
) -> Override {
    let mut builder = OverrideBuilder::new(base_dir);
    for glob in watch_ignore {
        // `ignore` overrides invert gitignore semantics: a leading `!` marks an
        // *ignore* (blacklist) pattern, which is exactly what `watch_ignore` is.
        // With no whitelist patterns present, anything not matching an ignore
        // glob is simply `Match::None` (kept).
        if let Err(e) = builder.add(&format!("!{glob}")) {
            warnings.push(format!("invalid watch_ignore pattern '{glob}': {e}"));
        }
        // A contents glob like `node_modules/**` or `target/**` matches only the
        // *contents* of the directory, not the directory itself — which would
        // leave a shallow watch on it. Users who ignore `foo/**` mean "don't
        // watch foo at all", so also prune the directory node itself. Any error
        // here is already surfaced by the primary `add` above (same glob body).
        if let Some(dir) = glob.strip_suffix("/**") {
            let _ = builder.add(&format!("!{dir}"));
        }
    }
    match builder.build() {
        Ok(overrides) => overrides,
        Err(e) => {
            // Falling back to an empty matcher means nothing is pruned and heavy
            // ignored dirs get watched — surface it rather than degrade silently.
            warnings.push(format!("failed to compile watch_ignore globs: {e}"));
            Override::empty()
        }
    }
}

/// A directory to register with the notify backend, and how.
#[derive(Debug, Clone, PartialEq, Eq)]
struct WatchAction {
    dir: PathBuf,
    mode: RecursiveMode,
    /// When true, an existing NonRecursive watch at `dir` must be unwatched
    /// before the (Recursive) re-watch. notify's inotify backend treats the
    /// same path under a different mode as a distinct registration, so leaving
    /// the old one leaks a watch descriptor.
    replace: bool,
}

struct InitialWatchRegistrationLog<'a> {
    emitter: &'a LifecycleEmitter,
    item: &'a str,
    pattern: &'a str,
}

/// Collect an initial service/task watch with verbose phase timings.
///
/// Keeping these boundaries visible is important because directory creation,
/// subtree discovery, and backend registration can each block independently on
/// a large or remote workspace. Backend registration is deliberately deferred
/// until every initial request has been collected so recursive ancestors can
/// eliminate redundant descendant roots.
fn collect_initial_dirs_ignoring(
    root: &Path,
    overrides: &Override,
    initial_desired_watches: &mut Vec<(PathBuf, RecursiveMode)>,
    log: InitialWatchRegistrationLog<'_>,
) -> Result<(), WatchError> {
    let pattern_started = Instant::now();
    let filesystem_started = Instant::now();
    log.emitter.service_debug_event(
        log.item,
        &format!(
            "watch: initial pattern filesystem setup started pattern={:?} root={}",
            log.pattern,
            root.display()
        ),
    );
    std::fs::create_dir_all(root).map_err(|e| WatchError::Io(root.to_path_buf(), e))?;
    log.emitter.service_debug_event(
        log.item,
        &format!(
            "watch: initial pattern filesystem setup complete pattern={:?} root={} elapsed={:?}",
            log.pattern,
            root.display(),
            filesystem_started.elapsed()
        ),
    );

    let discovery_started = Instant::now();
    log.emitter.service_debug_event(
        log.item,
        &format!(
            "watch: initial directory discovery started pattern={:?} root={} strategy={}",
            log.pattern,
            root.display(),
            initial_watch_strategy_label()
        ),
    );
    let desired = desired_watches(root, overrides);
    log.emitter.service_debug_event(
        log.item,
        &format!(
            "watch: initial directory discovery complete pattern={:?} root={} desired_directories={} elapsed={:?}",
            log.pattern,
            root.display(),
            desired.len(),
            discovery_started.elapsed()
        ),
    );

    let recursive_requests = desired
        .iter()
        .filter(|(_, mode)| *mode == RecursiveMode::Recursive)
        .count();
    initial_desired_watches.extend(desired);
    log.emitter.service_debug_event(
        log.item,
        &format!(
            "watch: initial watch request queued pattern={:?} root={} recursive_requests={} total_requests={} pattern_elapsed={:?}",
            log.pattern,
            root.display(),
            recursive_requests,
            initial_desired_watches.len(),
            pattern_started.elapsed()
        ),
    );
    Ok(())
}

/// Reconcile all initial requests together, then register only the minimal set.
///
/// On macOS, `notify`'s FSEvents backend restarts its single stream whenever
/// paths are mutated. Applying the reconciled actions as one batch avoids both
/// redundant descendant roots and one stream restart per configured pattern.
fn apply_initial_watch_batch(
    watcher: &mut Option<NotifyBackend>,
    notify_tx: &mpsc::UnboundedSender<notify::Result<notify::Event>>,
    desired: Vec<(PathBuf, RecursiveMode)>,
    registered_dirs: &mut HashMap<PathBuf, RecursiveMode>,
    emitter: &LifecycleEmitter,
) -> Result<(), WatchError> {
    let requested = desired.len();
    let actions = reconcile_watches(&desired, registered_dirs);
    let retained = actions.len();
    let pruned = requested.saturating_sub(retained);
    let registration_started = Instant::now();
    emitter.debug_event(&format!(
        "watch: initial backend batch started requests={requested} retained={retained} pruned={pruned} backend={} preference={}",
        watcher_label(watcher.as_ref()),
        preferred_watcher_label()
    ));
    apply_watch_actions(watcher, notify_tx, actions, registered_dirs)?;
    emitter.debug_event(&format!(
        "watch: initial backend batch complete registered={} backend={} elapsed={:?}",
        registered_dirs.len(),
        watcher_label(watcher.as_ref()),
        registration_started.elapsed()
    ));
    Ok(())
}

/// Register the notify watches needed to cover `root`, honoring `watch_ignore`.
///
/// The strategy is platform-dependent (see [`desired_watches`]): on Linux
/// (inotify) every non-ignored directory under `root` gets its own
/// `NonRecursive` watch and ignored subtrees are pruned from the walk; on macOS
/// (FSEvents) a single `Recursive` watch is registered at `root`.
///
/// On Linux we deliberately do **not** use recursive watches. A recursive watch
/// makes notify auto-descend into every directory created beneath it at runtime
/// — including a fresh ignored directory (a new `node_modules`, `target`, …),
/// re-registering the whole heavy subtree we set out to prune. There is no way
/// to stop that descent (notify has no ignore hook), only to unwatch it *after*
/// the spike — by which point thousands of watch descriptors may already have
/// been allocated. Non-recursive watches never auto-descend, so a runtime
/// ignored dir is simply skipped by the create backstop
/// ([`WatchManager::register_new_directory`]); the cost is that new directories
/// must be registered by that backstop rather than by notify. The kernel watch
/// count is identical either way (inotify allocates one descriptor per directory
/// even for a recursive watch).
///
/// That reasoning does **not** transfer to macOS, where a per-directory scheme
/// is pathologically slow — see [`desired_watches`].
fn register_dirs_ignoring(
    watcher: &mut Option<NotifyBackend>,
    notify_tx: &mpsc::UnboundedSender<notify::Result<notify::Event>>,
    root: &Path,
    overrides: &Override,
    registered_dirs: &mut HashMap<PathBuf, RecursiveMode>,
) -> Result<(), WatchError> {
    let desired = desired_watches(root, overrides);
    let actions = reconcile_watches(&desired, registered_dirs);
    apply_watch_actions(watcher, notify_tx, actions, registered_dirs)
}

/// Register a single directory under `mode`, reconciling against existing
/// watches (skip if already covered). Used for tier-1 build-graph patterns,
/// which are exact filenames in a package dir and need no subtree walk.
fn register_single_dir(
    watcher: &mut Option<NotifyBackend>,
    notify_tx: &mpsc::UnboundedSender<notify::Result<notify::Event>>,
    dir: &Path,
    mode: RecursiveMode,
    registered_dirs: &mut HashMap<PathBuf, RecursiveMode>,
) -> Result<(), WatchError> {
    let actions = reconcile_watches(&[(dir.to_path_buf(), mode)], registered_dirs);
    apply_watch_actions(watcher, notify_tx, actions, registered_dirs)
}

/// Compute the desired watch set for `root`'s subtree.
///
/// **Linux (inotify):** every non-ignored directory under `root` gets its own
/// `NonRecursive` watch; ignored subtrees are pruned. inotify allocates one
/// descriptor per directory regardless of mode, so this costs the same as a
/// recursive watch while honoring `watch_ignore` at registration and never
/// auto-descending into a runtime `node_modules`.
///
/// **macOS (FSEvents):** a single `Recursive` watch at `root`. FSEvents is
/// natively recursive and keeps *one* stream for all watched paths — every
/// `watcher.watch()` call tears that stream down and rebuilds it over the full
/// accumulated path set (see notify's `fsevent.rs`: `watch_inner` = `stop()` +
/// `append_path` + `run()`). Registering one watch per directory therefore makes
/// startup O(N²) in stream rebuilds plus N runloop-thread spawn/join cycles —
/// minutes on a large monorepo (this was the 0.5.8 Mac startup regression). One
/// recursive watch is one cheap stream, matching pre-0.5.8 behavior. `watch_ignore`
/// is instead enforced at the event layer (`global_ignore` in
/// [`WatchManager::handle_notify_event`]); FSEvents allocates no per-directory
/// descriptors, so there is nothing to leak by not pruning here.
fn desired_watches(root: &Path, overrides: &Override) -> Vec<(PathBuf, RecursiveMode)> {
    // `cfg!` (not `#[cfg]`) so `collect_watch_dirs` stays compiled and
    // referenced on every platform — no dead-code warnings, and the walk logic
    // remains testable on Linux CI.
    if cfg!(target_os = "macos") {
        return vec![(root.to_path_buf(), RecursiveMode::Recursive)];
    }
    let mut out = Vec::new();
    collect_watch_dirs(root, overrides, &mut out);
    out
}

/// Recursively emit a `NonRecursive` watch for `dir` and every non-ignored
/// directory beneath it. Ignored directories are pruned (not descended into);
/// symlinked directories are skipped, mirroring the notify backend's
/// `follow_symlinks(false)`.
fn collect_watch_dirs(dir: &Path, overrides: &Override, out: &mut Vec<(PathBuf, RecursiveMode)>) {
    out.push((dir.to_path_buf(), RecursiveMode::NonRecursive));

    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut child_dirs: Vec<PathBuf> = Vec::new();
    for entry in entries.flatten() {
        // `file_type()` on a `DirEntry` does not traverse symlinks, so a
        // symlinked directory reports `is_symlink()` and is skipped here.
        let Ok(ft) = entry.file_type() else { continue };
        if !ft.is_dir() {
            continue;
        }
        let path = entry.path();
        if overrides.matched(&path, true).is_ignore() {
            continue;
        }
        child_dirs.push(path);
    }
    // Deterministic order for stable debug output and tests.
    child_dirs.sort();
    for child in &child_dirs {
        collect_watch_dirs(child, overrides, out);
    }
}

/// Reconcile a desired watch set against what is already registered, producing
/// the minimal set of watch/unwatch actions. Pure (no I/O) so it is
/// table-testable; the actual `watch()`/`unwatch()` calls happen in
/// [`apply_watch_actions`].
fn reconcile_watches(
    desired: &[(PathBuf, RecursiveMode)],
    registered: &HashMap<PathBuf, RecursiveMode>,
) -> Vec<WatchAction> {
    // Simulate the registration as we go so a recursive watch planned early in
    // the batch covers its descendants later in the batch.
    let mut sim = registered.clone();
    // Ancestors first: a recursive ancestor added early covers descendants.
    let mut ordered: Vec<&(PathBuf, RecursiveMode)> = desired.iter().collect();
    ordered.sort_by_key(|(dir, _)| dir.components().count());

    let mut actions = Vec::new();
    for (dir, mode) in ordered {
        if is_covered(dir, *mode, &sim) {
            continue;
        }
        let replace =
            *mode == RecursiveMode::Recursive && sim.get(dir) == Some(&RecursiveMode::NonRecursive);
        actions.push(WatchAction {
            dir: dir.clone(),
            mode: *mode,
            replace,
        });
        sim.insert(dir.clone(), *mode);
    }
    actions
}

/// Apply reconciled watch actions to the notify backend and record them in
/// `registered_dirs`. Stops at the first hard error and returns it; the caller
/// decides whether that is fatal (startup) or per-item (runtime reload).
fn apply_watch_actions(
    watcher: &mut Option<NotifyBackend>,
    notify_tx: &mpsc::UnboundedSender<notify::Result<notify::Event>>,
    actions: Vec<WatchAction>,
    registered_dirs: &mut HashMap<PathBuf, RecursiveMode>,
) -> Result<(), WatchError> {
    if actions.is_empty() {
        return Ok(());
    }

    // Replacements are uncommon runtime upgrades and need ordered best-effort
    // unwatch + watch handling. Fresh registrations can use `paths_mut`, which
    // lets FSEvents stop and restart its shared stream once for the whole set.
    if actions.iter().any(|action| action.replace) {
        for action in actions {
            let backend = ensure_notify_watcher(watcher, notify_tx)?;
            if action.replace {
                // Best-effort: an unwatch failure shouldn't block the re-watch.
                let _ = backend.unwatch(&action.dir);
            }
            backend.watch(&action.dir, action.mode)?;
            registered_dirs.insert(action.dir, action.mode);
        }
        return Ok(());
    }

    let backend = ensure_notify_watcher(watcher, notify_tx)?;
    let mut paths = backend.paths_mut();
    let mut added = Vec::new();
    let mut add_error = None;
    for action in &actions {
        match paths.add(&action.dir, action.mode) {
            Ok(()) => added.push(action),
            Err(e) => {
                add_error = Some(e);
                break;
            }
        }
    }
    let commit_result = paths.commit();
    if commit_result.is_ok() {
        for action in added {
            registered_dirs.insert(action.dir.clone(), action.mode);
        }
    }
    if let Some(e) = add_error {
        return Err(e.into());
    }
    commit_result?;

    Ok(())
}

/// Recursively collect non-ignored file paths under `dir` (skipping ignored
/// subtrees and symlinks). Used to replay files that already exist inside a
/// directory that appeared before its watch took effect.
fn collect_files_recursive(dir: &Path, overrides: &Override, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(ft) = entry.file_type() else { continue };
        let path = entry.path();
        if ft.is_dir() {
            if overrides.matched(&path, true).is_ignore() {
                continue;
            }
            collect_files_recursive(&path, overrides, out);
        } else if ft.is_file() && !overrides.matched(&path, false).is_ignore() {
            out.push(path);
        }
        // Symlinks are skipped, mirroring `follow_symlinks(false)`.
    }
}

fn refresh_item_definition(
    item: &mut WatchedItem,
    kind: WatchItemKind,
    patterns: Vec<Pattern>,
    ignore_patterns: Vec<Pattern>,
) {
    // Definition updates are independent of the in-flight state machine.
    // Resetting here loses pending debounces or stale follow-up cycles.
    item.kind = kind;
    item.patterns = patterns;
    item.ignore_patterns = ignore_patterns;
    item.last_error = None;
}

fn build_graph_command_name(name: &str) -> String {
    if name == WORKSPACE_GRAPH_ITEM_NAME {
        return WORKSPACE_GRAPH_ITEM_NAME.to_string();
    }

    name.strip_suffix("__graph").unwrap_or(name).to_string()
}

fn watch_state_label(state: WatchState) -> &'static str {
    match state {
        WatchState::Idle => "idle",
        WatchState::Debouncing => "debouncing",
        WatchState::Rebuilding => "rebuilding",
    }
}

fn recursive_mode_label(mode: RecursiveMode) -> &'static str {
    match mode {
        RecursiveMode::Recursive => "recursive",
        RecursiveMode::NonRecursive => "non-recursive",
    }
}

fn watch_item_kind_label(kind: WatchItemKind) -> &'static str {
    match kind {
        WatchItemKind::Service => "service",
        WatchItemKind::Task => "task",
        WatchItemKind::BuildGraph => "build_graph",
    }
}

/// Extract the base directory from a glob pattern.
///
/// Returns the longest directory prefix before the first glob metacharacter.
/// The result is always a directory path, never a file:
/// - `src/**/*.rs` → `src` (stopped at `**`, so `src` is the directory)
/// - `*.txt` → `.` (first component is a glob, so the directory is `.`)
/// - `a/b/c/*.log` → `a/b/c`
/// - `src/main.rs` → `src` (no glob found, so we take the parent directory)
fn glob_base_dir(pattern: &Path) -> PathBuf {
    let mut base = PathBuf::new();
    let mut hit_glob = false;
    for component in pattern.components() {
        let s = component.as_os_str().to_string_lossy();
        if s.contains('*') || s.contains('?') || s.contains('[') {
            hit_glob = true;
            break;
        }
        base.push(component);
    }
    // If no glob was found, the path is a literal file (e.g. `src/main.rs`).
    // Take its parent directory so we don't create a directory named after the file.
    if !hit_glob {
        base = base.parent().map(Path::to_path_buf).unwrap_or_default();
    }
    // Fall back to current directory if the base is empty (e.g. pattern is `*.txt`
    // or a bare filename like `Makefile`).
    if base.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        base
    }
}

fn resolve_pattern(base_dir: &Path, pattern: &str) -> PathBuf {
    let pattern_path = Path::new(pattern);
    if pattern_path.is_absolute() {
        pattern_path.to_path_buf()
    } else {
        base_dir.join(pattern_path)
    }
}

fn ensure_notify_watcher<'a>(
    watcher: &'a mut Option<NotifyBackend>,
    notify_tx: &mpsc::UnboundedSender<notify::Result<notify::Event>>,
) -> Result<&'a mut NotifyBackend, WatchError> {
    if watcher.is_none() {
        *watcher = Some(if prefer_poll_watcher() {
            create_poll_watcher(notify_tx)?
        } else {
            match create_native_watcher(notify_tx) {
                Ok(watcher) => watcher,
                Err(_) => create_poll_watcher(notify_tx)?,
            }
        });
    }
    watcher
        .as_mut()
        .ok_or_else(|| notify::Error::generic("failed to initialize notify watcher").into())
}

fn prefer_poll_watcher() -> bool {
    cfg!(debug_assertions) && std::env::var_os("DON_NATIVE_WATCH").is_none()
}

fn preferred_watcher_label() -> &'static str {
    if prefer_poll_watcher() {
        "poll"
    } else {
        "native-with-poll-fallback"
    }
}

fn watcher_label(watcher: Option<&NotifyBackend>) -> &'static str {
    watcher.map_or("not-initialized", NotifyBackend::label)
}

fn initial_watch_strategy_label() -> &'static str {
    if cfg!(target_os = "macos") {
        "recursive-root"
    } else {
        "non-recursive-directory-walk"
    }
}

fn create_native_watcher(
    notify_tx: &mpsc::UnboundedSender<notify::Result<notify::Event>>,
) -> notify::Result<NotifyBackend> {
    let tx = notify_tx.clone();
    RecommendedWatcher::new(
        move |res| {
            let _ = tx.send(res);
        },
        notify::Config::default().with_follow_symlinks(false),
    )
    .map(NotifyBackend::Native)
}

fn create_poll_watcher(
    notify_tx: &mpsc::UnboundedSender<notify::Result<notify::Event>>,
) -> notify::Result<NotifyBackend> {
    let tx = notify_tx.clone();
    PollWatcher::new(
        move |res| {
            let _ = tx.send(res);
        },
        notify::Config::default()
            .with_follow_symlinks(false)
            .with_poll_interval(Duration::from_millis(250))
            .with_compare_contents(true),
    )
    .map(NotifyBackend::Poll)
}

/// Sleep until the given instant, or pend forever if `None`.
async fn sleep_until_or_pending(deadline: Option<Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(d).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_base_dir() {
        struct Case {
            pattern: &'static str,
            expected: &'static str,
        }

        let cases = vec![
            Case {
                pattern: "src/**/*.rs",
                expected: "src",
            },
            Case {
                pattern: "*.txt",
                expected: ".",
            },
            Case {
                pattern: "a/b/c/*.log",
                expected: "a/b/c",
            },
            Case {
                pattern: "a/b/*/d.txt",
                expected: "a/b",
            },
            // No glob: take parent directory, not the file itself.
            Case {
                pattern: "exact/path/file.txt",
                expected: "exact/path",
            },
            Case {
                pattern: "src/[abc]/*.rs",
                expected: "src",
            },
            // Single literal filename: parent is `.`
            Case {
                pattern: "Makefile",
                expected: ".",
            },
        ];

        for case in cases {
            let result = glob_base_dir(Path::new(case.pattern));
            assert_eq!(
                result,
                PathBuf::from(case.expected),
                "glob_base_dir({:?}) = {:?}, expected {:?}",
                case.pattern,
                result,
                case.expected
            );
        }
    }

    #[test]
    fn test_is_covered() {
        struct Case {
            name: &'static str,
            path: &'static str,
            mode: RecursiveMode,
            existing: Vec<(&'static str, RecursiveMode)>,
            expected: bool,
        }

        let cases = vec![
            Case {
                name: "recursive ancestor covers recursive",
                path: "/a/b/c",
                mode: RecursiveMode::Recursive,
                existing: vec![("/a", RecursiveMode::Recursive)],
                expected: true,
            },
            Case {
                name: "recursive ancestor covers non-recursive",
                path: "/a/b/c",
                mode: RecursiveMode::NonRecursive,
                existing: vec![("/a", RecursiveMode::Recursive)],
                expected: true,
            },
            Case {
                // This is the bug we're fixing: a non-recursive ancestor must
                // NOT count as covering a recursive request — descendants
                // would not receive events.
                name: "non-recursive ancestor does NOT cover recursive",
                path: "/a/b",
                mode: RecursiveMode::Recursive,
                existing: vec![("/a", RecursiveMode::NonRecursive)],
                expected: false,
            },
            Case {
                name: "non-recursive ancestor does NOT cover recursive at same path",
                path: "/a",
                mode: RecursiveMode::Recursive,
                existing: vec![("/a", RecursiveMode::NonRecursive)],
                expected: false,
            },
            Case {
                name: "exact same path non-recursive covers non-recursive",
                path: "/a",
                mode: RecursiveMode::NonRecursive,
                existing: vec![("/a", RecursiveMode::NonRecursive)],
                expected: true,
            },
            Case {
                name: "empty existing never covers",
                path: "/a",
                mode: RecursiveMode::Recursive,
                existing: vec![],
                expected: false,
            },
            Case {
                name: "sibling does not cover",
                path: "/a/b",
                mode: RecursiveMode::Recursive,
                existing: vec![("/a/c", RecursiveMode::Recursive)],
                expected: false,
            },
        ];

        for case in cases {
            let map: HashMap<PathBuf, RecursiveMode> = case
                .existing
                .iter()
                .map(|(p, m)| (PathBuf::from(p), *m))
                .collect();
            assert_eq!(
                is_covered(Path::new(case.path), case.mode, &map),
                case.expected,
                "case: {}",
                case.name,
            );
        }
    }

    #[test]
    fn test_reconcile_watches() {
        use RecursiveMode::{NonRecursive, Recursive};

        struct Case {
            name: &'static str,
            desired: Vec<(&'static str, RecursiveMode)>,
            registered: Vec<(&'static str, RecursiveMode)>,
            expected: Vec<(&'static str, RecursiveMode, bool)>,
        }

        let cases = vec![
            Case {
                name: "fresh recursive root",
                desired: vec![("/a", Recursive)],
                registered: vec![],
                expected: vec![("/a", Recursive, false)],
            },
            Case {
                name: "recursive ancestor covers later descendants in batch",
                desired: vec![("/a/b", Recursive), ("/a", Recursive)],
                registered: vec![],
                // Sorted ancestors-first: /a registers and covers /a/b.
                expected: vec![("/a", Recursive, false)],
            },
            Case {
                name: "recursive ancestor prunes multiple earlier sibling requests",
                desired: vec![
                    ("/a/prisma/models", Recursive),
                    ("/a/redo/kafka/topics/src", Recursive),
                    ("/a", Recursive),
                ],
                registered: vec![],
                expected: vec![("/a", Recursive, false)],
            },
            Case {
                name: "spine non-recursive plus recursive clean child",
                desired: vec![("/a", NonRecursive), ("/a/clean", Recursive)],
                registered: vec![],
                expected: vec![("/a", NonRecursive, false), ("/a/clean", Recursive, false)],
            },
            Case {
                name: "already covered by recursive ancestor is skipped",
                desired: vec![("/a/b", Recursive)],
                registered: vec![("/a", Recursive)],
                expected: vec![],
            },
            Case {
                name: "upgrade existing non-recursive to recursive replaces it",
                desired: vec![("/a", Recursive)],
                registered: vec![("/a", NonRecursive)],
                expected: vec![("/a", Recursive, true)],
            },
            Case {
                name: "exact non-recursive already registered is skipped",
                desired: vec![("/a", NonRecursive)],
                registered: vec![("/a", NonRecursive)],
                expected: vec![],
            },
            Case {
                name: "non-recursive request under recursive ancestor is skipped",
                desired: vec![("/a/b", NonRecursive)],
                registered: vec![("/a", Recursive)],
                expected: vec![],
            },
        ];

        for case in cases {
            let desired: Vec<(PathBuf, RecursiveMode)> = case
                .desired
                .iter()
                .map(|(p, m)| (PathBuf::from(p), *m))
                .collect();
            let registered: HashMap<PathBuf, RecursiveMode> = case
                .registered
                .iter()
                .map(|(p, m)| (PathBuf::from(p), *m))
                .collect();
            let expected: Vec<WatchAction> = case
                .expected
                .iter()
                .map(|(p, m, replace)| WatchAction {
                    dir: PathBuf::from(p),
                    mode: *m,
                    replace: *replace,
                })
                .collect();

            let got = reconcile_watches(&desired, &registered);
            assert_eq!(got, expected, "case: {}", case.name);
        }
    }

    // On macOS `desired_watches` short-circuits to a single recursive watch, so
    // the per-directory walk asserted below only runs on other platforms.
    #[cfg(not(target_os = "macos"))]
    #[test]
    fn test_desired_watches() {
        use RecursiveMode::NonRecursive;

        // Build a tree:
        //   root/
        //     clean/
        //       sub/
        //     pkg/
        //       node_modules/   (ignored -> never watched)
        //         dep/
        //       src/
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        for rel in ["clean/sub", "pkg/node_modules/dep", "pkg/src"] {
            std::fs::create_dir_all(root.join(rel)).unwrap();
        }

        let mut warnings = Vec::new();
        let overrides =
            build_watch_ignore_overrides(root, &["**/node_modules/**".to_string()], &mut warnings);
        assert!(warnings.is_empty(), "unexpected warnings: {warnings:?}");

        let mut got: Vec<(PathBuf, RecursiveMode)> = desired_watches(root, &overrides);
        got.sort();

        // Every non-ignored directory is watched non-recursively; the ignored
        // `node_modules` subtree is pruned entirely.
        let mut expected: Vec<(PathBuf, RecursiveMode)> = vec![
            (root.to_path_buf(), NonRecursive),
            (root.join("clean"), NonRecursive),
            (root.join("clean/sub"), NonRecursive),
            (root.join("pkg"), NonRecursive),
            (root.join("pkg/src"), NonRecursive),
        ];
        expected.sort();

        assert_eq!(got, expected);
        // node_modules must never appear in the watch set.
        assert!(
            !got.iter()
                .any(|(p, _)| p.to_string_lossy().contains("node_modules")),
            "node_modules should be pruned: {got:?}"
        );
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn test_desired_watches_prunes_nested_ignored_dir_created_anywhere() {
        // A `**/node_modules/**` pattern must prune node_modules at any depth,
        // not just directly under root.
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        std::fs::create_dir_all(root.join("a/b/node_modules/dep")).unwrap();
        std::fs::create_dir_all(root.join("a/b/keep")).unwrap();

        let mut warnings = Vec::new();
        let overrides =
            build_watch_ignore_overrides(root, &["**/node_modules/**".to_string()], &mut warnings);

        let mut got = desired_watches(root, &overrides);
        got.sort();

        let mut expected: Vec<(PathBuf, RecursiveMode)> = vec![
            (root.to_path_buf(), RecursiveMode::NonRecursive),
            (root.join("a"), RecursiveMode::NonRecursive),
            (root.join("a/b"), RecursiveMode::NonRecursive),
            (root.join("a/b/keep"), RecursiveMode::NonRecursive),
        ];
        expected.sort();
        assert_eq!(got, expected);
    }

    // On macOS the notify backend is FSEvents, where each `watcher.watch()` call
    // rebuilds a single shared stream over all accumulated paths — a per-dir walk
    // is O(N²). `desired_watches` must therefore collapse to one recursive watch
    // at the root regardless of the subtree shape, ignored dirs included (they're
    // filtered at the event layer instead).
    #[cfg(target_os = "macos")]
    #[test]
    fn test_desired_watches_macos_single_recursive() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        std::fs::create_dir_all(root.join("pkg/src")).unwrap();
        std::fs::create_dir_all(root.join("pkg/node_modules/dep")).unwrap();

        let mut warnings = Vec::new();
        let overrides =
            build_watch_ignore_overrides(root, &["**/node_modules/**".to_string()], &mut warnings);

        let got = desired_watches(root, &overrides);
        assert_eq!(got, vec![(root.to_path_buf(), RecursiveMode::Recursive)]);
    }

    #[tokio::test]
    async fn test_initial_watch_batch_prunes_recursive_descendants() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_path_buf();
        let prisma = root.join("prisma/models");
        let kafka = root.join("redo/kafka/topics/src");
        std::fs::create_dir_all(&prisma).unwrap();
        std::fs::create_dir_all(&kafka).unwrap();

        let output = crate::output::OutputManager::new(&[], tokio::io::sink())
            .await
            .unwrap();
        let emitter = output.clone_lifecycle_emitter();
        let (notify_tx, _notify_rx) = mpsc::unbounded_channel();
        let mut watcher = None;
        let mut registered = HashMap::new();

        apply_initial_watch_batch(
            &mut watcher,
            &notify_tx,
            vec![
                (prisma, RecursiveMode::Recursive),
                (kafka, RecursiveMode::Recursive),
                (root.clone(), RecursiveMode::Recursive),
            ],
            &mut registered,
            &emitter,
        )
        .unwrap();

        assert_eq!(
            registered,
            HashMap::from([(root, RecursiveMode::Recursive)])
        );
    }

    #[test]
    fn test_build_watch_ignore_overrides_reports_invalid_glob() {
        let temp = tempfile::tempdir().unwrap();
        let mut warnings = Vec::new();
        // An unclosed character class is an invalid glob.
        let _ = build_watch_ignore_overrides(
            temp.path(),
            &["good/**".to_string(), "bad[".to_string()],
            &mut warnings,
        );
        assert_eq!(warnings.len(), 1, "warnings: {warnings:?}");
        assert!(warnings[0].contains("bad["), "warnings: {warnings:?}");
    }

    #[tokio::test]
    async fn test_build_tool_watch_opt_outs_skip_workspace_graph_watch() {
        struct Case {
            name: &'static str,
            toml: &'static str,
            expect_watches: bool,
        }

        let cases = vec![
            Case {
                name: "bazel watch false",
                toml: r#"
[services.api]
bazel.target = "//services/api:api"
bazel.watch = false
"#,
                expect_watches: false,
            },
            Case {
                name: "bazel reload false",
                toml: r#"
[services.api]
bazel.target = "//services/api:api"
reload = false
"#,
                expect_watches: false,
            },
            Case {
                name: "bazel default watches workspace graph",
                toml: r#"
[services.api]
bazel.target = "//services/api:api"
"#,
                expect_watches: true,
            },
        ];

        for case in cases {
            let temp = tempfile::tempdir().unwrap();
            let config: crate::config::Config = case.toml.parse().unwrap();
            let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel();
            let (_event_tx, event_rx) = broadcast::channel(8);
            let (_update_tx, update_rx) = mpsc::unbounded_channel();
            let (_query_tx, query_rx) = mpsc::channel(8);
            let output = crate::output::OutputManager::new(
                &[("api", &crate::config::LogConfig::Stdout)],
                tokio::io::sink(),
            )
            .await
            .unwrap();

            let (watch_mgr, warnings) = WatchManager::new(
                &config,
                crate::config::Platform::LinuxX86_64,
                temp.path(),
                cmd_tx,
                event_rx,
                update_rx,
                query_rx,
                output.clone_lifecycle_emitter(),
            )
            .unwrap();

            if case.expect_watches {
                // Positive cases need a real notify backend. Some developer
                // machines can exhaust Linux's per-user inotify instance
                // ceiling; that should not hide regressions in the opt-out
                // cases this table primarily covers.
                assert!(
                    warnings.is_empty()
                        || warnings
                            .iter()
                            .all(|warning| warning.contains("workspace watch registration failed")),
                    "case: {} warnings: {:?}",
                    case.name,
                    warnings
                );
            } else {
                assert!(warnings.is_empty(), "case: {}", case.name);
            }
            assert_eq!(
                watch_mgr.has_watches(),
                case.expect_watches,
                "case: {}",
                case.name,
            );
        }
    }

    #[test]
    fn test_glob_pattern_matches_files_in_watched_dirs() {
        struct Case {
            name: &'static str,
            pattern: &'static str,
            path: &'static str,
            expected: bool,
        }

        let cases = vec![
            Case {
                name: "** matches nested file",
                pattern: "/app/src/**/*.rs",
                path: "/app/src/foo/bar.rs",
                expected: true,
            },
            Case {
                name: "** matches deeply nested",
                pattern: "/app/src/**/*.rs",
                path: "/app/src/a/b/c.rs",
                expected: true,
            },
            Case {
                name: "** matches file directly in src",
                pattern: "/app/src/**/*.rs",
                path: "/app/src/main.rs",
                expected: true,
            },
            Case {
                name: "literal file matches",
                pattern: "/app/Cargo.toml",
                path: "/app/Cargo.toml",
                expected: true,
            },
            Case {
                name: "literal file does not match other",
                pattern: "/app/Cargo.toml",
                path: "/app/Cargo.lock",
                expected: false,
            },
            Case {
                name: "single star matches one path component",
                pattern: "/app/src/*.rs",
                path: "/app/src/main.rs",
                expected: true,
            },
            Case {
                name: "single star does not cross path components",
                pattern: "/app/src/*.rs",
                path: "/app/src/nested/main.rs",
                expected: false,
            },
            Case {
                name: "matching is case sensitive",
                pattern: "/app/src/*.rs",
                path: "/app/src/main.RS",
                expected: false,
            },
            Case {
                name: "does not match outside dir",
                pattern: "/app/src/**/*.rs",
                path: "/other/src/main.rs",
                expected: false,
            },
            // Build tool integration patterns end with /** (no file extension filter)
            Case {
                name: "/** matches nested file",
                pattern: "/app/services/api/**",
                path: "/app/services/api/src/main.py",
                expected: true,
            },
            Case {
                name: "/** matches direct child",
                pattern: "/app/services/api/**",
                path: "/app/services/api/main.py",
                expected: true,
            },
            Case {
                name: "/** does not match sibling dir",
                pattern: "/app/services/api/**",
                path: "/app/services/web/main.py",
                expected: false,
            },
        ];

        for case in cases {
            let pat = Pattern::new(case.pattern).unwrap();
            assert_eq!(
                matches_glob(&pat, case.path),
                case.expected,
                "case: {} — pattern {:?} vs path {:?}",
                case.name,
                case.pattern,
                case.path,
            );
        }
    }

    #[tokio::test]
    async fn test_state_machine_debounce_coalesces_events() {
        // Simulate: 10 events arrive in quick succession. Only one rebuild fires.
        tokio::time::pause();

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

        let mut items = HashMap::new();
        items.insert(
            "api".to_string(),
            WatchedItem {
                state: WatchState::Idle,
                debounce_duration: Duration::from_millis(200),
                debounce_deadline: None,
                stale: false,
                kind: WatchItemKind::Service,
                patterns: vec![Pattern::new("src/**/*.rs").unwrap()],
                ignore_patterns: vec![],
                last_error: None,
            },
        );

        // Create a minimal event to feed the state machine.
        let make_event = || notify::Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![PathBuf::from("src/main.rs")],
            attrs: Default::default(),
        };

        // Feed 10 events rapidly (every 10ms).
        let mut mgr_items = items;
        for _ in 0..10 {
            let event = make_event();
            handle_notify_event_standalone(&mut mgr_items, &event, &cmd_tx).await;
            tokio::time::advance(Duration::from_millis(10)).await;
        }

        // All items should be in Debouncing state.
        assert_eq!(mgr_items["api"].state, WatchState::Debouncing);

        // Advance past the debounce window (200ms from the last event).
        tokio::time::advance(Duration::from_millis(200)).await;

        // Fire timers.
        fire_debounce_timers_standalone(&mut mgr_items, &cmd_tx).await;
        assert_eq!(mgr_items["api"].state, WatchState::Rebuilding);

        // Should have received exactly one Rebuild command.
        let cmd = cmd_rx.try_recv().unwrap();
        assert!(matches!(cmd, RunnerCommand::Rebuild { ref name } if name == "api"));
        assert!(cmd_rx.try_recv().is_err());

        // Clean up: send rebuild complete event to reset state.
        let event = RunnerEvent::RebuildComplete {
            name: "api".to_string(),
            success: true,
        };
        handle_runner_event_standalone(&mut mgr_items, &event, &cmd_tx).await;
        assert_eq!(mgr_items["api"].state, WatchState::Idle);
    }

    #[tokio::test]
    async fn test_events_after_debounce_trigger_new_cycle() {
        tokio::time::pause();

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

        let mut items = HashMap::new();
        items.insert(
            "api".to_string(),
            WatchedItem {
                state: WatchState::Idle,
                debounce_duration: Duration::from_millis(200),
                debounce_deadline: None,
                stale: false,
                kind: WatchItemKind::Service,
                patterns: vec![Pattern::new("src/**/*.rs").unwrap()],
                ignore_patterns: vec![],
                last_error: None,
            },
        );

        let event = notify::Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![PathBuf::from("src/lib.rs")],
            attrs: Default::default(),
        };

        // First event: start debouncing.
        handle_notify_event_standalone(&mut items, &event, &cmd_tx).await;
        tokio::time::advance(Duration::from_millis(200)).await;
        fire_debounce_timers_standalone(&mut items, &cmd_tx).await;
        let _ = cmd_rx.try_recv().unwrap(); // consume first Rebuild

        // Simulate rebuild completion.
        let complete = RunnerEvent::RebuildComplete {
            name: "api".to_string(),
            success: true,
        };
        handle_runner_event_standalone(&mut items, &complete, &cmd_tx).await;
        assert_eq!(items["api"].state, WatchState::Idle);

        // Second event: should start a new cycle.
        handle_notify_event_standalone(&mut items, &event, &cmd_tx).await;
        assert_eq!(items["api"].state, WatchState::Debouncing);

        tokio::time::advance(Duration::from_millis(200)).await;
        fire_debounce_timers_standalone(&mut items, &cmd_tx).await;
        let cmd = cmd_rx.try_recv().unwrap();
        assert!(matches!(cmd, RunnerCommand::Rebuild { ref name } if name == "api"));
    }

    #[tokio::test]
    async fn test_custom_debounce_duration() {
        tokio::time::pause();

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

        let mut items = HashMap::new();
        items.insert(
            "api".to_string(),
            WatchedItem {
                state: WatchState::Idle,
                debounce_duration: Duration::from_millis(500),
                debounce_deadline: None,
                stale: false,
                kind: WatchItemKind::Service,
                patterns: vec![Pattern::new("src/**/*.rs").unwrap()],
                ignore_patterns: vec![],
                last_error: None,
            },
        );

        let event = notify::Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![PathBuf::from("src/main.rs")],
            attrs: Default::default(),
        };

        handle_notify_event_standalone(&mut items, &event, &cmd_tx).await;

        // At 200ms: should NOT have fired yet (debounce is 500ms).
        tokio::time::advance(Duration::from_millis(200)).await;
        fire_debounce_timers_standalone(&mut items, &cmd_tx).await;
        assert_eq!(items["api"].state, WatchState::Debouncing);
        assert!(cmd_rx.try_recv().is_err());

        // At 500ms: should fire.
        tokio::time::advance(Duration::from_millis(300)).await;
        fire_debounce_timers_standalone(&mut items, &cmd_tx).await;
        assert_eq!(items["api"].state, WatchState::Rebuilding);
        assert!(cmd_rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn test_change_during_build_triggers_second_rebuild() {
        tokio::time::pause();

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

        let mut items = HashMap::new();
        items.insert(
            "api".to_string(),
            WatchedItem {
                state: WatchState::Rebuilding,
                debounce_duration: Duration::from_millis(200),
                debounce_deadline: None,
                stale: false,
                kind: WatchItemKind::Service,
                patterns: vec![Pattern::new("src/**/*.rs").unwrap()],
                ignore_patterns: vec![],
                last_error: None,
            },
        );

        // Event during build — should set stale.
        let event = notify::Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![PathBuf::from("src/main.rs")],
            attrs: Default::default(),
        };
        handle_notify_event_standalone(&mut items, &event, &cmd_tx).await;
        assert!(items["api"].stale);
        assert_eq!(items["api"].state, WatchState::Rebuilding);
        let cmd = cmd_rx.try_recv().unwrap();
        assert!(matches!(cmd, RunnerCommand::RebuildStale { ref name } if name == "api"));

        // Build completes — should trigger another Rebuild because stale.
        let complete = RunnerEvent::RebuildComplete {
            name: "api".to_string(),
            success: true,
        };
        handle_runner_event_standalone(&mut items, &complete, &cmd_tx).await;
        assert_eq!(items["api"].state, WatchState::Rebuilding);
        assert!(!items["api"].stale);
        let cmd = cmd_rx.try_recv().unwrap();
        assert!(matches!(cmd, RunnerCommand::Rebuild { ref name } if name == "api"));
    }

    #[tokio::test]
    async fn test_multiple_events_during_build_one_followup() {
        tokio::time::pause();

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

        let mut items = HashMap::new();
        items.insert(
            "api".to_string(),
            WatchedItem {
                state: WatchState::Rebuilding,
                debounce_duration: Duration::from_millis(200),
                debounce_deadline: None,
                stale: false,
                kind: WatchItemKind::Service,
                patterns: vec![Pattern::new("src/**/*.rs").unwrap()],
                ignore_patterns: vec![],
                last_error: None,
            },
        );

        let event = notify::Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![PathBuf::from("src/main.rs")],
            attrs: Default::default(),
        };

        // 5 events during build.
        for _ in 0..5 {
            handle_notify_event_standalone(&mut items, &event, &cmd_tx).await;
        }
        assert!(items["api"].stale);
        let mut stale_count = 0;
        while let Ok(cmd) = cmd_rx.try_recv() {
            if matches!(cmd, RunnerCommand::RebuildStale { ref name } if name == "api") {
                stale_count += 1;
            }
        }
        assert_eq!(stale_count, 5);

        // Build completes — only one follow-up rebuild.
        let complete = RunnerEvent::RebuildComplete {
            name: "api".to_string(),
            success: true,
        };
        handle_runner_event_standalone(&mut items, &complete, &cmd_tx).await;
        let cmd = cmd_rx.try_recv().unwrap();
        assert!(matches!(cmd, RunnerCommand::Rebuild { ref name } if name == "api"));
        assert!(cmd_rx.try_recv().is_err()); // No extra commands.
    }

    #[tokio::test]
    async fn test_state_machine_full_cycle() {
        tokio::time::pause();

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

        let mut items = HashMap::new();
        items.insert(
            "api".to_string(),
            WatchedItem {
                state: WatchState::Idle,
                debounce_duration: Duration::from_millis(200),
                debounce_deadline: None,
                stale: false,
                kind: WatchItemKind::Service,
                patterns: vec![Pattern::new("src/**/*.rs").unwrap()],
                ignore_patterns: vec![],
                last_error: None,
            },
        );

        let event = notify::Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![PathBuf::from("src/main.rs")],
            attrs: Default::default(),
        };

        // Idle -> Debouncing
        assert_eq!(items["api"].state, WatchState::Idle);
        handle_notify_event_standalone(&mut items, &event, &cmd_tx).await;
        assert_eq!(items["api"].state, WatchState::Debouncing);

        // Debouncing -> Rebuilding
        tokio::time::advance(Duration::from_millis(200)).await;
        fire_debounce_timers_standalone(&mut items, &cmd_tx).await;
        assert_eq!(items["api"].state, WatchState::Rebuilding);
        let _ = cmd_rx.try_recv().unwrap();

        // Events during rebuild set stale.
        handle_notify_event_standalone(&mut items, &event, &cmd_tx).await;
        assert!(items["api"].stale);
        assert_eq!(items["api"].state, WatchState::Rebuilding);
        let cmd = cmd_rx.try_recv().unwrap();
        assert!(matches!(cmd, RunnerCommand::RebuildStale { ref name } if name == "api"));

        // Rebuild completes with stale -> immediately Rebuilding again.
        let complete = RunnerEvent::RebuildComplete {
            name: "api".to_string(),
            success: true,
        };
        handle_runner_event_standalone(&mut items, &complete, &cmd_tx).await;
        assert_eq!(items["api"].state, WatchState::Rebuilding);
        assert!(!items["api"].stale);
        let _ = cmd_rx.try_recv().unwrap();

        // Second rebuild completes without stale -> Idle.
        handle_runner_event_standalone(&mut items, &complete, &cmd_tx).await;
        assert_eq!(items["api"].state, WatchState::Idle);
        assert!(cmd_rx.try_recv().is_err());
    }

    // --- Test helpers: standalone versions of WatchManager methods ---

    async fn handle_notify_event_standalone(
        items: &mut HashMap<String, WatchedItem>,
        event: &notify::Event,
        cmd_tx: &mpsc::UnboundedSender<RunnerCommand>,
    ) {
        if !matches!(
            event.kind,
            EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
        ) {
            return;
        }

        let mut affected: Vec<String> = Vec::new();
        for path in &event.paths {
            let path_str = path.to_string_lossy();
            for (name, item) in items.iter() {
                if item
                    .ignore_patterns
                    .iter()
                    .any(|pattern| matches_ignore(pattern, &path_str))
                {
                    continue;
                }
                if item
                    .patterns
                    .iter()
                    .any(|pattern| matches_glob(pattern, &path_str))
                    && !affected.contains(name)
                {
                    affected.push(name.clone());
                }
            }
        }

        let now = Instant::now();
        let mut stale_services: Vec<String> = Vec::new();
        for name in affected {
            if let Some(item) = items.get_mut(&name) {
                match item.state {
                    WatchState::Idle => {
                        item.state = WatchState::Debouncing;
                        item.debounce_deadline = Some(now + item.debounce_duration);
                    }
                    WatchState::Debouncing => {
                        item.debounce_deadline = Some(now + item.debounce_duration);
                    }
                    WatchState::Rebuilding => {
                        item.stale = true;
                        if item.kind == WatchItemKind::Service && !stale_services.contains(&name) {
                            stale_services.push(name.clone());
                        }
                    }
                }
            }
        }

        for name in stale_services {
            let _ = cmd_tx.send(RunnerCommand::RebuildStale { name });
        }
    }

    async fn fire_debounce_timers_standalone(
        items: &mut HashMap<String, WatchedItem>,
        cmd_tx: &mpsc::UnboundedSender<RunnerCommand>,
    ) {
        let now = Instant::now();
        let mut to_fire: Vec<(String, WatchItemKind)> = Vec::new();

        for (name, item) in items.iter() {
            if item.state == WatchState::Debouncing
                && let Some(deadline) = item.debounce_deadline
                && now >= deadline
            {
                to_fire.push((name.clone(), item.kind));
            }
        }

        for (name, kind) in to_fire {
            if let Some(item) = items.get_mut(&name) {
                item.debounce_deadline = None;
                let cmd = match kind {
                    WatchItemKind::Task => {
                        item.state = WatchState::Rebuilding;
                        RunnerCommand::TaskRerun { name }
                    }
                    WatchItemKind::Service => {
                        item.state = WatchState::Rebuilding;
                        RunnerCommand::Rebuild { name }
                    }
                    WatchItemKind::BuildGraph => {
                        item.state = WatchState::Idle;
                        let item_name = build_graph_command_name(&name);
                        RunnerCommand::BuildGraphChanged { name: item_name }
                    }
                };
                let _ = cmd_tx.send(cmd);
            }
        }
    }

    async fn handle_runner_event_standalone(
        items: &mut HashMap<String, WatchedItem>,
        event: &RunnerEvent,
        cmd_tx: &mpsc::UnboundedSender<RunnerCommand>,
    ) {
        match event {
            RunnerEvent::RebuildComplete { name, .. } => {
                if let Some(item) = items.get_mut(name) {
                    if item.stale {
                        item.stale = false;
                        item.state = WatchState::Rebuilding;
                        let _ = cmd_tx.send(RunnerCommand::Rebuild { name: name.clone() });
                    } else {
                        item.state = WatchState::Idle;
                    }
                }
            }
            RunnerEvent::TaskRerunComplete { name, .. } => {
                if let Some(item) = items.get_mut(name) {
                    if item.stale {
                        item.stale = false;
                        item.state = WatchState::Rebuilding;
                        let _ = cmd_tx.send(RunnerCommand::TaskRerun { name: name.clone() });
                    } else {
                        item.state = WatchState::Idle;
                    }
                }
            }
            _ => {}
        }
    }

    #[tokio::test]
    async fn test_build_graph_kind_sends_build_graph_changed() {
        tokio::time::pause();

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

        let mut items = HashMap::new();
        items.insert(
            "api__graph".to_string(),
            WatchedItem {
                state: WatchState::Idle,
                debounce_duration: Duration::from_millis(200),
                debounce_deadline: None,
                stale: false,
                kind: WatchItemKind::BuildGraph,
                patterns: vec![Pattern::new("**/BUILD.bazel").unwrap()],
                ignore_patterns: vec![],
                last_error: None,
            },
        );

        let event = notify::Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![PathBuf::from("services/api/BUILD.bazel")],
            attrs: Default::default(),
        };

        // Trigger the event.
        handle_notify_event_standalone(&mut items, &event, &cmd_tx).await;
        assert_eq!(items["api__graph"].state, WatchState::Debouncing);

        // Wait for debounce.
        tokio::time::advance(Duration::from_millis(200)).await;
        fire_debounce_timers_standalone(&mut items, &cmd_tx).await;

        // BuildGraph kind goes straight to Idle (no rebuild cycle).
        assert_eq!(items["api__graph"].state, WatchState::Idle);

        // Should receive BuildGraphChanged with the service name (not the __graph suffix).
        let cmd = cmd_rx.try_recv().unwrap();
        assert!(
            matches!(cmd, RunnerCommand::BuildGraphChanged { ref name } if name == "api"),
            "expected BuildGraphChanged for 'api', got different command"
        );
    }

    #[tokio::test]
    async fn test_build_graph_kind_no_rebuild_cycle() {
        // Build graph changes should NOT enter the Rebuilding state.
        // They go Idle -> Debouncing -> Idle (fire) directly.
        tokio::time::pause();

        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel();

        let mut items = HashMap::new();
        items.insert(
            "web__graph".to_string(),
            WatchedItem {
                state: WatchState::Idle,
                debounce_duration: Duration::from_millis(200),
                debounce_deadline: None,
                stale: false,
                kind: WatchItemKind::BuildGraph,
                patterns: vec![Pattern::new("**/package.json").unwrap()],
                ignore_patterns: vec![],
                last_error: None,
            },
        );

        let event = notify::Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![PathBuf::from("apps/web/package.json")],
            attrs: Default::default(),
        };

        handle_notify_event_standalone(&mut items, &event, &cmd_tx).await;
        tokio::time::advance(Duration::from_millis(200)).await;
        fire_debounce_timers_standalone(&mut items, &cmd_tx).await;

        // Should be back to Idle, not Rebuilding.
        assert_eq!(items["web__graph"].state, WatchState::Idle);
        // And stale should still be false.
        assert!(!items["web__graph"].stale);
    }

    #[tokio::test]
    async fn test_workspace_build_graph_kind_preserves_workspace_sentinel() {
        tokio::time::pause();

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

        let mut items = HashMap::new();
        items.insert(
            WORKSPACE_GRAPH_ITEM_NAME.to_string(),
            WatchedItem {
                state: WatchState::Idle,
                debounce_duration: Duration::from_millis(200),
                debounce_deadline: None,
                stale: false,
                kind: WatchItemKind::BuildGraph,
                patterns: vec![Pattern::new("**/MODULE.bazel").unwrap()],
                ignore_patterns: vec![],
                last_error: None,
            },
        );

        let event = notify::Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![PathBuf::from("MODULE.bazel")],
            attrs: Default::default(),
        };

        handle_notify_event_standalone(&mut items, &event, &cmd_tx).await;
        tokio::time::advance(Duration::from_millis(200)).await;
        fire_debounce_timers_standalone(&mut items, &cmd_tx).await;

        let cmd = cmd_rx.try_recv().unwrap();
        assert!(
            matches!(cmd, RunnerCommand::BuildGraphChanged { ref name } if name == WORKSPACE_GRAPH_ITEM_NAME),
            "expected BuildGraphChanged for workspace sentinel, got different command"
        );
    }

    #[tokio::test]
    async fn test_watch_refresh_during_stale_rebuild_keeps_follow_up_cycle() {
        tokio::time::pause();

        let temp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(temp.path().join("src")).unwrap();
        let initial_path = temp.path().join("src/main.rs");
        let refreshed_path = temp.path().join("src/schema.txt");
        std::fs::write(&initial_path, "fn main() {}").unwrap();
        std::fs::write(&refreshed_path, "schema").unwrap();
        let config: Config = r#"
[services.api]
run.cmd = "true"
watch = ["src/**/*.rs"]
"#
        .parse()
        .unwrap();
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
        let (_event_tx, event_rx) = broadcast::channel(8);
        let (_update_tx, update_rx) = mpsc::unbounded_channel();
        let (_query_tx, query_rx) = mpsc::channel(8);
        let output = crate::output::OutputManager::new(
            &[("api", &crate::config::LogConfig::Stdout)],
            tokio::io::sink(),
        )
        .await
        .unwrap();
        let (mut manager, _warnings) = WatchManager::new(
            &config,
            Platform::LinuxX86_64,
            temp.path(),
            cmd_tx,
            event_rx,
            update_rx,
            query_rx,
            output.clone_lifecycle_emitter(),
        )
        .unwrap();
        let initial_event = notify::Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![initial_path],
            attrs: Default::default(),
        };
        let refreshed_event = notify::Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![refreshed_path],
            attrs: Default::default(),
        };
        let watch_update = || WatchUpdate {
            name: "api".to_string(),
            kind: WatchItemKind::Service,
            patterns: vec!["src/**".to_string()],
            ignore_patterns: Vec::new(),
            base_dir: temp.path().to_path_buf(),
            applied_tx: None,
        };

        manager.handle_notify_event(&initial_event).await;
        let debounce_deadline = manager.items["api"].debounce_deadline;
        manager.apply_watch_update(watch_update());
        assert_eq!(manager.items["api"].state, WatchState::Debouncing);
        assert_eq!(manager.items["api"].debounce_deadline, debounce_deadline);
        tokio::time::advance(Duration::from_millis(200)).await;
        manager.fire_debounce_timers().await;
        assert!(matches!(
            cmd_rx.try_recv().unwrap(),
            RunnerCommand::Rebuild { ref name } if name == "api"
        ));

        manager.handle_notify_event(&refreshed_event).await;
        assert!(matches!(
            cmd_rx.try_recv().unwrap(),
            RunnerCommand::RebuildStale { ref name } if name == "api"
        ));

        manager.apply_watch_update(watch_update());
        assert_eq!(manager.items["api"].state, WatchState::Rebuilding);
        assert!(manager.items["api"].stale);

        manager
            .handle_runner_event(&RunnerEvent::RebuildComplete {
                name: "api".to_string(),
                success: true,
            })
            .await;
        assert!(matches!(
            cmd_rx.try_recv().unwrap(),
            RunnerCommand::Rebuild { ref name } if name == "api"
        ));
        assert_eq!(manager.items["api"].state, WatchState::Rebuilding);
        assert!(!manager.items["api"].stale);
        assert!(cmd_rx.try_recv().is_err());

        manager
            .handle_runner_event(&RunnerEvent::RebuildComplete {
                name: "api".to_string(),
                success: true,
            })
            .await;
        assert_eq!(manager.items["api"].state, WatchState::Idle);
        assert!(cmd_rx.try_recv().is_err());
    }
}
