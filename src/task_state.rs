//! Task state tracking — determines whether a task needs to re-run
//! based on file content hashes.
//!
//! State is stored in `.don/task-state/`:
//! - `<task-name>.sha256` stores the watched-input hash for watch-based reruns
//! - `<task-name>.success` records that the task has succeeded at least once
//!
//! Success state is only written after a task exits successfully (exit code 0).

use crate::globwalk::{
    glob_pattern_base_dir, has_glob_metacharacters, matches_glob, matches_ignore,
};
use hex::encode;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const HASH_PROGRESS_INTERVAL: Duration = Duration::from_secs(1);

/// Metadata for the most recent task process that actually ran.
///
/// This is separate from the success marker: failed runs update this record
/// without making dependency gates consider the task satisfied.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskRunInfo {
    /// Unix timestamp, in seconds, when the run finished.
    pub finished_at_unix_secs: u64,
    /// Process runtime in milliseconds, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// Whether the process exited successfully.
    pub success: bool,
    /// Exit code for normal process exits. Timeouts/signals may not have one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// Short failure description, when the run failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl TaskRunInfo {
    /// Build a metadata record for a run that finished now.
    pub fn finished_now(
        success: bool,
        elapsed: Option<Duration>,
        exit_code: Option<i32>,
        message: Option<String>,
    ) -> Self {
        Self {
            finished_at_unix_secs: current_unix_secs(),
            duration_ms: elapsed.map(duration_millis_saturating),
            success,
            exit_code,
            message,
        }
    }
}

/// Manages task state — tracks file hashes to determine whether a task needs to re-run.
///
/// State is stored in `.don/task-state/`.
/// Successful task runs persist a generic success marker, and watch-based
/// tasks also persist a content hash for their watched inputs.
#[derive(Clone)]
pub struct TaskStateStore {
    state_dir: PathBuf,
}

/// Progress emitted while collecting and hashing a task's watched inputs.
pub(crate) enum TaskHashProgress {
    GlobStarted {
        pattern: String,
    },
    GlobProgress {
        pattern: String,
        entries_seen: usize,
        files_matched: usize,
        files_ignored: usize,
        elapsed: Duration,
    },
    GlobFinished {
        pattern: String,
        entries_seen: usize,
        files_matched: usize,
        files_ignored: usize,
        elapsed: Duration,
    },
    HashStarted {
        total_files: usize,
    },
    HashProgress {
        files_hashed: usize,
        total_files: usize,
        bytes_hashed: u64,
        elapsed: Duration,
    },
    HashFinished {
        files_hashed: usize,
        bytes_hashed: u64,
        elapsed: Duration,
    },
}

impl Default for TaskStateStore {
    fn default() -> Self {
        Self::new(PathBuf::from(".don").join("task-state"))
    }
}

impl TaskStateStore {
    /// Create a new `TaskStateStore` that stores hashes in the given directory.
    pub fn new(state_dir: PathBuf) -> Self {
        Self { state_dir }
    }

    /// Check whether a task needs to run based on its watch patterns.
    ///
    /// Returns `true` if:
    /// - The task has no watch patterns (always runs)
    /// - There is no stored hash or success marker (never succeeded before)
    /// - The current file hash differs from the stored hash
    ///
    /// `base_dir` is prepended to glob patterns so they resolve relative to the
    /// task's working directory, not don's cwd. Pass `None` to resolve from cwd.
    ///
    /// Runs filesystem I/O on a blocking thread to avoid stalling the tokio runtime.
    pub async fn needs_run(
        &self,
        task_name: &str,
        watch_patterns: &[String],
        ignore_patterns: &[String],
        base_dir: Option<&Path>,
    ) -> Result<bool, TaskStateError> {
        self.needs_run_with_progress(task_name, watch_patterns, ignore_patterns, base_dir, |_| {})
            .await
    }

    pub(crate) async fn needs_run_with_progress<F>(
        &self,
        task_name: &str,
        watch_patterns: &[String],
        ignore_patterns: &[String],
        base_dir: Option<&Path>,
        mut progress: F,
    ) -> Result<bool, TaskStateError>
    where
        F: FnMut(TaskHashProgress) + Send + 'static,
    {
        let this = self.clone();
        let task_name = task_name.to_string();
        let watch_patterns = watch_patterns.to_vec();
        let ignore_patterns = ignore_patterns.to_vec();
        let base_dir = base_dir.map(Path::to_path_buf);
        tokio::task::spawn_blocking(move || {
            this.needs_run_sync_with_progress(
                &task_name,
                &watch_patterns,
                &ignore_patterns,
                base_dir.as_deref(),
                &mut progress,
            )
        })
        .await
        .map_err(|e| TaskStateError::Io(std::io::Error::other(e)))?
    }

    /// Record a successful task run by writing the current file hash.
    /// Only call this after the task exits with code 0.
    ///
    /// `base_dir` must match what was passed to `needs_run`.
    ///
    /// Runs filesystem I/O on a blocking thread to avoid stalling the tokio runtime.
    pub async fn record_success(
        &self,
        task_name: &str,
        watch_patterns: &[String],
        ignore_patterns: &[String],
        base_dir: Option<&Path>,
    ) -> Result<(), TaskStateError> {
        let this = self.clone();
        let task_name = task_name.to_string();
        let watch_patterns = watch_patterns.to_vec();
        let ignore_patterns = ignore_patterns.to_vec();
        let base_dir = base_dir.map(Path::to_path_buf);
        tokio::task::spawn_blocking(move || {
            let run_info = TaskRunInfo::finished_now(true, None, Some(0), None);
            this.record_success_sync(
                &task_name,
                &watch_patterns,
                &ignore_patterns,
                base_dir.as_deref(),
                Some(&run_info),
            )
        })
        .await
        .map_err(|e| TaskStateError::Io(std::io::Error::other(e)))?
    }

    /// Record a successful task run with caller-supplied run metadata.
    ///
    /// This writes both the success marker/hash and the latest run metadata.
    pub async fn record_success_with_info(
        &self,
        task_name: &str,
        watch_patterns: &[String],
        ignore_patterns: &[String],
        base_dir: Option<&Path>,
        run_info: &TaskRunInfo,
    ) -> Result<(), TaskStateError> {
        let this = self.clone();
        let task_name = task_name.to_string();
        let watch_patterns = watch_patterns.to_vec();
        let ignore_patterns = ignore_patterns.to_vec();
        let base_dir = base_dir.map(Path::to_path_buf);
        let run_info = run_info.clone();
        tokio::task::spawn_blocking(move || {
            this.record_success_sync(
                &task_name,
                &watch_patterns,
                &ignore_patterns,
                base_dir.as_deref(),
                Some(&run_info),
            )
        })
        .await
        .map_err(|e| TaskStateError::Io(std::io::Error::other(e)))?
    }

    /// Record the most recent task run without changing success/hash state.
    ///
    /// Use this for failed runs: status can show the failure, but dependency
    /// gates still require the previous successful run.
    pub async fn record_run(
        &self,
        task_name: &str,
        run_info: &TaskRunInfo,
    ) -> Result<(), TaskStateError> {
        let task_name = task_name.to_string();
        let run_info = run_info.clone();
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.record_run_sync(&task_name, &run_info))
            .await
            .map_err(|e| TaskStateError::Io(std::io::Error::other(e)))?
    }

    /// Return whether the task has at least one recorded successful run.
    pub async fn has_success(&self, task_name: &str) -> Result<bool, TaskStateError> {
        let task_name = task_name.to_string();
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.has_success_sync(&task_name))
            .await
            .map_err(|e| TaskStateError::Io(std::io::Error::other(e)))?
    }

    /// Return metadata for the most recent actual task run, if recorded.
    pub async fn last_run(&self, task_name: &str) -> Result<Option<TaskRunInfo>, TaskStateError> {
        let task_name = task_name.to_string();
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.last_run_sync(&task_name))
            .await
            .map_err(|e| TaskStateError::Io(std::io::Error::other(e)))?
    }

    /// Clear stored state for a task, forcing it to re-run next time.
    ///
    /// Runs filesystem I/O on a blocking thread to avoid stalling the tokio runtime.
    pub async fn clear(&self, task_name: &str) -> Result<(), TaskStateError> {
        let task_name = task_name.to_string();
        remove_file_if_exists(self.hash_file_path(&task_name)).await?;
        remove_file_if_exists(self.success_file_path(&task_name)).await?;
        remove_file_if_exists(self.last_run_file_path(&task_name)).await
    }

    fn needs_run_sync_with_progress<F>(
        &self,
        task_name: &str,
        watch_patterns: &[String],
        ignore_patterns: &[String],
        base_dir: Option<&Path>,
        progress: &mut F,
    ) -> Result<bool, TaskStateError>
    where
        F: FnMut(TaskHashProgress),
    {
        if watch_patterns.is_empty() {
            return Ok(true);
        }

        match self.check_watched_inputs_sync(
            task_name,
            watch_patterns,
            ignore_patterns,
            base_dir,
            usize::MAX,
            progress,
        )? {
            WatchedInputs::Changed => Ok(true),
            WatchedInputs::Unchanged => Ok(false),
            // Unreachable with an uncapped check; a task whose inputs cannot
            // be verified is one that needs to run.
            WatchedInputs::Unverifiable { .. } => Ok(true),
        }
    }

    /// Compare the watched inputs against the hash recorded by the last
    /// successful run, giving up rather than hashing past `max_files`.
    ///
    /// This is [`needs_run`](Self::needs_run) with the cap made visible:
    /// callers that use the answer to *suppress* a run need to know the
    /// difference between "identical" and "not checked", where a caller
    /// deciding whether to run at startup can treat both as "run".
    ///
    /// Runs filesystem I/O on a blocking thread.
    pub(crate) async fn check_watched_inputs<F>(
        &self,
        task_name: &str,
        watch_patterns: &[String],
        ignore_patterns: &[String],
        base_dir: Option<&Path>,
        max_files: usize,
        mut progress: F,
    ) -> Result<WatchedInputs, TaskStateError>
    where
        F: FnMut(TaskHashProgress) + Send + 'static,
    {
        let this = self.clone();
        let task_name = task_name.to_string();
        let watch_patterns = watch_patterns.to_vec();
        let ignore_patterns = ignore_patterns.to_vec();
        let base_dir = base_dir.map(Path::to_path_buf);
        tokio::task::spawn_blocking(move || {
            this.check_watched_inputs_sync(
                &task_name,
                &watch_patterns,
                &ignore_patterns,
                base_dir.as_deref(),
                max_files,
                &mut progress,
            )
        })
        .await
        .map_err(|e| TaskStateError::Io(std::io::Error::other(e)))?
    }

    fn check_watched_inputs_sync<F>(
        &self,
        task_name: &str,
        watch_patterns: &[String],
        ignore_patterns: &[String],
        base_dir: Option<&Path>,
        max_files: usize,
        progress: &mut F,
    ) -> Result<WatchedInputs, TaskStateError>
    where
        F: FnMut(TaskHashProgress),
    {
        // A task with nothing declared has no inputs to compare, so there is
        // nothing that could rule a run out.
        if watch_patterns.is_empty() {
            return Ok(WatchedInputs::Changed);
        }

        let current_hash = match self.compute_hash_with_progress(
            watch_patterns,
            ignore_patterns,
            base_dir,
            max_files,
            progress,
        )? {
            HashOutcome::Hash(hash) => hash,
            HashOutcome::TooLarge { files } => return Ok(WatchedInputs::Unverifiable { files }),
        };
        // No hash, or a hash whose run died before writing its success
        // marker, means no successful run to compare against.
        if !self.has_success_sync(task_name)? {
            return Ok(WatchedInputs::Changed);
        }
        match self.read_stored_hash(task_name)? {
            Some(stored) if stored == current_hash => Ok(WatchedInputs::Unchanged),
            _ => Ok(WatchedInputs::Changed),
        }
    }

    fn record_success_sync(
        &self,
        task_name: &str,
        watch_patterns: &[String],
        ignore_patterns: &[String],
        base_dir: Option<&Path>,
        run_info: Option<&TaskRunInfo>,
    ) -> Result<(), TaskStateError> {
        std::fs::create_dir_all(&self.state_dir)?;
        if !watch_patterns.is_empty() {
            let hash = self.compute_hash(watch_patterns, ignore_patterns, base_dir)?;
            let hash_path = self.hash_file_path(task_name);
            std::fs::write(&hash_path, hash.as_bytes())?;
        }
        std::fs::write(self.success_file_path(task_name), b"success\n")?;
        if let Some(run_info) = run_info {
            self.record_run_sync(task_name, run_info)?;
        }
        Ok(())
    }

    /// Compute a combined SHA-256 hash of all files matching the watch patterns.
    ///
    /// The hash includes:
    /// - The sorted list of matched file paths (so adding/removing files triggers a change)
    /// - The contents of each file
    fn compute_hash(
        &self,
        watch_patterns: &[String],
        ignore_patterns: &[String],
        base_dir: Option<&Path>,
    ) -> Result<String, TaskStateError> {
        match self.compute_hash_with_progress(
            watch_patterns,
            ignore_patterns,
            base_dir,
            usize::MAX,
            &mut |_| {},
        )? {
            HashOutcome::Hash(hash) => Ok(hash),
            HashOutcome::TooLarge { files } => Err(TaskStateError::TooManyFiles { files }),
        }
    }

    fn compute_hash_with_progress<F>(
        &self,
        watch_patterns: &[String],
        ignore_patterns: &[String],
        base_dir: Option<&Path>,
        max_files: usize,
        progress: &mut F,
    ) -> Result<HashOutcome, TaskStateError>
    where
        F: FnMut(TaskHashProgress),
    {
        let compiled_watch: Vec<glob::Pattern> = watch_patterns
            .iter()
            .map(|pattern| compile_pattern(base_dir, pattern))
            .collect::<Result<_, _>>()?;
        let compiled_ignore: Vec<glob::Pattern> = ignore_patterns
            .iter()
            .map(|pattern| compile_pattern(base_dir, pattern))
            .collect::<Result<_, _>>()?;
        // A `dir/**` ignore covers every descendant of `dir`, so its prefix
        // (matching `dir` itself) can prune the whole subtree from the walk.
        let prune_prefixes: Vec<glob::Pattern> = ignore_patterns
            .iter()
            .filter_map(|pattern| {
                let full = resolve_pattern(base_dir, pattern)
                    .to_string_lossy()
                    .into_owned();
                glob::Pattern::new(full.strip_suffix("/**")?).ok()
            })
            .collect();

        let mut paths = Vec::new();

        // A literal (no-metacharacter) watch resolves to one path; stat it directly
        // instead of walking its parent (a repo-root literal would walk the whole tree).
        let mut glob_roots_and_patterns: Vec<(PathBuf, String)> = Vec::new();
        for pattern in watch_patterns {
            let resolved = resolve_pattern(base_dir, pattern);
            if has_glob_metacharacters(&resolved) {
                glob_roots_and_patterns.push((
                    glob_pattern_base_dir(&resolved),
                    resolved.to_string_lossy().into_owned(),
                ));
            } else {
                consider_literal(&resolved, &compiled_ignore, &mut paths)?;
            }
        }

        // Drop roots nested under another root to avoid walking a subtree twice.
        let mut roots: Vec<PathBuf> = glob_roots_and_patterns
            .iter()
            .map(|(root, _)| root.clone())
            .collect();
        roots.sort();
        roots.dedup();
        let mut walk_roots: Vec<PathBuf> = Vec::new();
        for root in roots {
            if walk_roots.iter().any(|kept| root.starts_with(kept)) {
                continue;
            }
            walk_roots.push(root);
        }

        for root in &walk_roots {
            let root_display = glob_roots_and_patterns
                .iter()
                .filter(|(pattern_root, _)| pattern_root.starts_with(root))
                .map(|(_, pattern)| pattern.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            progress(TaskHashProgress::GlobStarted {
                pattern: root_display.clone(),
            });
            let glob_started = Instant::now();
            let mut last_progress = glob_started;
            let mut stats = GlobWalkStats::default();
            {
                let mut report_progress = |stats: GlobWalkStats| {
                    if last_progress.elapsed() >= HASH_PROGRESS_INTERVAL {
                        progress(TaskHashProgress::GlobProgress {
                            pattern: root_display.clone(),
                            entries_seen: stats.entries_seen,
                            files_matched: stats.files_matched,
                            files_ignored: stats.files_ignored,
                            elapsed: glob_started.elapsed(),
                        });
                        last_progress = Instant::now();
                    }
                };
                collect_matching_files(
                    root,
                    &compiled_watch,
                    &compiled_ignore,
                    &prune_prefixes,
                    &mut paths,
                    &mut stats,
                    &mut report_progress,
                )?;
            }
            progress(TaskHashProgress::GlobFinished {
                pattern: root_display,
                entries_seen: stats.entries_seen,
                files_matched: stats.files_matched,
                files_ignored: stats.files_ignored,
                elapsed: glob_started.elapsed(),
            });
            // Checked between roots rather than inside the walk: the point is
            // to not *read* a six-figure file set, and one root's walk is a
            // bounded price for keeping the collector's signature simple.
            if paths.len() > max_files {
                return Ok(HashOutcome::TooLarge { files: paths.len() });
            }
        }
        paths.sort();
        paths.dedup();
        if paths.len() > max_files {
            return Ok(HashOutcome::TooLarge { files: paths.len() });
        }

        let mut hasher = Sha256::new();

        // Hash the file list itself so adding/removing files is detected
        for path in &paths {
            hasher.update(path.to_string_lossy().as_bytes());
            hasher.update(b"\0");
        }

        let total_files = paths.len();
        progress(TaskHashProgress::HashStarted { total_files });
        let hash_started = Instant::now();
        let mut last_progress = hash_started;
        let mut files_hashed = 0usize;
        let mut bytes_hashed = 0u64;

        // Hash each file's contents.
        for path in &paths {
            let contents = std::fs::read(path)?;
            hasher.update(&contents);
            hasher.update(b"\0");
            files_hashed = files_hashed.saturating_add(1);
            bytes_hashed =
                bytes_hashed.saturating_add(u64::try_from(contents.len()).unwrap_or(u64::MAX));
            if last_progress.elapsed() >= HASH_PROGRESS_INTERVAL {
                progress(TaskHashProgress::HashProgress {
                    files_hashed,
                    total_files,
                    bytes_hashed,
                    elapsed: hash_started.elapsed(),
                });
                last_progress = Instant::now();
            }
        }

        progress(TaskHashProgress::HashFinished {
            files_hashed,
            bytes_hashed,
            elapsed: hash_started.elapsed(),
        });

        Ok(HashOutcome::Hash(encode(hasher.finalize())))
    }

    fn hash_file_path(&self, task_name: &str) -> PathBuf {
        self.state_dir.join(format!("{task_name}.sha256"))
    }

    fn success_file_path(&self, task_name: &str) -> PathBuf {
        self.state_dir.join(format!("{task_name}.success"))
    }

    fn last_run_file_path(&self, task_name: &str) -> PathBuf {
        self.state_dir.join(format!("{task_name}.last-run.json"))
    }

    fn has_success_sync(&self, task_name: &str) -> Result<bool, TaskStateError> {
        match std::fs::metadata(self.success_file_path(task_name)) {
            Ok(meta) => Ok(meta.is_file()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(TaskStateError::Io(e)),
        }
    }

    fn read_stored_hash(&self, task_name: &str) -> Result<Option<String>, TaskStateError> {
        let path = self.hash_file_path(task_name);
        match std::fs::read_to_string(&path) {
            Ok(hash) => Ok(Some(hash)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(TaskStateError::Io(e)),
        }
    }

    fn record_run_sync(
        &self,
        task_name: &str,
        run_info: &TaskRunInfo,
    ) -> Result<(), TaskStateError> {
        std::fs::create_dir_all(&self.state_dir)?;
        let bytes = serde_json::to_vec(run_info)?;
        std::fs::write(self.last_run_file_path(task_name), bytes)?;
        Ok(())
    }

    fn last_run_sync(&self, task_name: &str) -> Result<Option<TaskRunInfo>, TaskStateError> {
        let path = self.last_run_file_path(task_name);
        match std::fs::read(&path) {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(TaskStateError::Io(e)),
        }
    }
}

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

fn duration_millis_saturating(duration: Duration) -> u64 {
    let millis = duration.as_millis();
    if millis > u128::from(u64::MAX) {
        u64::MAX
    } else {
        millis as u64
    }
}

fn resolve_pattern(base_dir: Option<&Path>, pattern: &str) -> PathBuf {
    let pattern_path = Path::new(pattern);
    if pattern_path.is_absolute() {
        pattern_path.to_path_buf()
    } else {
        match base_dir {
            Some(dir) => dir.join(pattern_path),
            None => pattern_path.to_path_buf(),
        }
    }
}

/// Compile a watch/ignore pattern into an absolute `glob::Pattern`.
fn compile_pattern(
    base_dir: Option<&Path>,
    pattern: &str,
) -> Result<glob::Pattern, TaskStateError> {
    let full = resolve_pattern(base_dir, pattern);
    glob::Pattern::new(&full.to_string_lossy()).map_err(|e| TaskStateError::Glob(e.to_string()))
}

/// Recursively collect files under `dir` matching a watch pattern and no ignore
/// pattern. Symlinked directories are never descended, bounding cycle walks.
#[derive(Clone, Copy, Default)]
struct GlobWalkStats {
    entries_seen: usize,
    files_matched: usize,
    files_ignored: usize,
}

fn collect_matching_files(
    dir: &Path,
    watch: &[glob::Pattern],
    ignore: &[glob::Pattern],
    prune_prefixes: &[glob::Pattern],
    out: &mut Vec<PathBuf>,
    stats: &mut GlobWalkStats,
    report_progress: &mut dyn FnMut(GlobWalkStats),
) -> Result<(), TaskStateError> {
    if is_pruned(dir, prune_prefixes) || is_ignored(dir, ignore) {
        return Ok(());
    }
    match std::fs::symlink_metadata(dir) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
        Ok(_) => return Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(TaskStateError::Io(e)),
    }

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        stats.entries_seen = stats.entries_seen.saturating_add(1);
        let file_type = entry.file_type()?;
        let path = entry.path();
        if file_type.is_dir() {
            if is_pruned(&path, prune_prefixes) {
                report_progress(*stats);
                continue;
            }
            collect_matching_files(
                &path,
                watch,
                ignore,
                prune_prefixes,
                out,
                stats,
                report_progress,
            )?;
        } else if file_type.is_file() {
            stats.record(consider_file(&path, watch, ignore, out));
        } else if file_type.is_symlink() {
            // A symlinked file is a candidate; a symlinked dir is skipped to bound cycles.
            // A broken link (NotFound) is a no-match; other metadata errors propagate.
            match std::fs::metadata(&path) {
                Ok(meta) if meta.is_file() => {
                    stats.record(consider_file(&path, watch, ignore, out));
                }
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(TaskStateError::Io(e)),
            }
        }
        report_progress(*stats);
    }
    Ok(())
}

impl GlobWalkStats {
    fn record(&mut self, outcome: FileMatch) {
        match outcome {
            FileMatch::Matched => self.files_matched = self.files_matched.saturating_add(1),
            FileMatch::Ignored => self.files_ignored = self.files_ignored.saturating_add(1),
            FileMatch::Unmatched => {}
        }
    }
}

enum FileMatch {
    Matched,
    Ignored,
    Unmatched,
}

/// Push `path` onto `out` if it matches a watch pattern and no ignore pattern.
fn consider_file(
    path: &Path,
    watch: &[glob::Pattern],
    ignore: &[glob::Pattern],
    out: &mut Vec<PathBuf>,
) -> FileMatch {
    let normalized = strip_current_dir_prefix(path);
    let path_str = normalized.to_string_lossy();
    if !watch.iter().any(|pattern| matches_glob(pattern, &path_str)) {
        return FileMatch::Unmatched;
    }
    if ignore
        .iter()
        .any(|pattern| matches_ignore(pattern, &path_str))
    {
        return FileMatch::Ignored;
    }
    out.push(normalized.to_path_buf());
    FileMatch::Matched
}

/// Push a literal watch target directly if it resolves to a file and isn't
/// ignored — the metacharacter-free fast path that skips the directory walk.
fn consider_literal(
    path: &Path,
    ignore: &[glob::Pattern],
    out: &mut Vec<PathBuf>,
) -> Result<(), TaskStateError> {
    // A missing literal (NotFound, incl. a broken symlink) is a no-match;
    // other metadata errors propagate so an unreadable path surfaces.
    match std::fs::metadata(path) {
        Ok(meta) if meta.is_file() => {}
        Ok(_) => return Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(TaskStateError::Io(e)),
    }
    let normalized = strip_current_dir_prefix(path);
    let path_str = normalized.to_string_lossy();
    if ignore
        .iter()
        .any(|pattern| matches_ignore(pattern, &path_str))
    {
        return Ok(());
    }
    out.push(normalized.to_path_buf());
    Ok(())
}

/// Whether `dir` is fully covered by a `dir/**` ignore and can be skipped.
fn is_pruned(dir: &Path, prune_prefixes: &[glob::Pattern]) -> bool {
    let dir_str = strip_current_dir_prefix(dir).to_string_lossy();
    prune_prefixes
        .iter()
        .any(|pattern| matches_glob(pattern, &dir_str))
}

fn is_ignored(path: &Path, ignore: &[glob::Pattern]) -> bool {
    let path_str = strip_current_dir_prefix(path).to_string_lossy();
    ignore
        .iter()
        .any(|pattern| matches_ignore(pattern, &path_str))
}

fn strip_current_dir_prefix(path: &Path) -> &Path {
    path.strip_prefix(Path::new(".")).map_or(path, |stripped| {
        if stripped.as_os_str().is_empty() {
            path
        } else {
            stripped
        }
    })
}

async fn remove_file_if_exists(path: PathBuf) -> Result<(), TaskStateError> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(TaskStateError::Io(e)),
    }
}

/// Errors from task state operations.
#[derive(Debug, thiserror::Error)]
pub enum TaskStateError {
    /// A filesystem operation failed (reading files, writing state, etc.).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// A watch glob pattern was invalid.
    #[error("glob error: {0}")]
    Glob(String),
    /// Latest-run metadata could not be serialized or parsed.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// A hash that must be exact was asked for over a file set past the cap.
    #[error("watched inputs expanded to {files} files, past the hashing cap")]
    TooManyFiles {
        /// How many files the glob had matched when the walk gave up.
        files: usize,
    },
}

/// The most files a self-write check will hash before giving up.
///
/// The check exists to catch a generator task rewriting its own watched
/// inputs with identical content. Hashing answers that exactly, but it is
/// linear in the tree, and a watch glob over a monorepo can expand to six
/// figures — at that size the check costs more than the run it would save, so
/// the caller falls back to the suppression window alone. Applied per walk
/// root, so one oversized root is walked before the bail-out.
pub(crate) const MAX_VERIFY_FILES: usize = 20_000;

/// What a watched-input check concluded.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum WatchedInputs {
    /// The content hash moved since the last recorded run.
    Changed,
    /// The content hash is identical: nothing this task watches actually
    /// changed, whatever the file watcher saw move.
    Unchanged,
    /// The glob expanded past the cap, so no hash was computed and the caller
    /// has to decide without one.
    Unverifiable {
        /// How many files had matched when the walk gave up.
        files: usize,
    },
}

/// A hash over a capped file set: either the digest, or the count that
/// exceeded the cap before any file was read.
enum HashOutcome {
    Hash(String),
    TooLarge { files: usize },
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::fs;

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir()
                .join("don-test")
                .join(name)
                .join(format!("{}", std::process::id()));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[tokio::test]
    async fn test_task_state() {
        struct TestCase {
            name: &'static str,
            setup: fn(&Path),
            patterns: Vec<String>,
            ignore_patterns: Vec<String>,
            expect_needs_run_before: bool,
            record_success: bool,
            mutate: Option<fn(&Path)>,
            expect_needs_run_after: bool,
        }

        let cases = vec![
            TestCase {
                name: "no watch patterns always needs run",
                setup: |_| {},
                patterns: vec![],
                ignore_patterns: vec![],
                expect_needs_run_before: true,
                record_success: true,
                mutate: None,
                expect_needs_run_after: true,
            },
            TestCase {
                name: "first run always needs run",
                setup: |dir| {
                    fs::write(dir.join("a.sql"), "CREATE TABLE a;").unwrap();
                },
                patterns: vec!["PLACEHOLDER/*.sql".to_string()],
                ignore_patterns: vec![],
                expect_needs_run_before: true,
                record_success: true,
                mutate: None,
                expect_needs_run_after: false,
            },
            TestCase {
                name: "unchanged files skip",
                setup: |dir| {
                    fs::write(dir.join("a.sql"), "CREATE TABLE a;").unwrap();
                },
                patterns: vec!["PLACEHOLDER/*.sql".to_string()],
                ignore_patterns: vec![],
                expect_needs_run_before: true,
                record_success: true,
                mutate: None,
                expect_needs_run_after: false,
            },
            TestCase {
                name: "modified file triggers re-run",
                setup: |dir| {
                    fs::write(dir.join("a.sql"), "CREATE TABLE a;").unwrap();
                },
                patterns: vec!["PLACEHOLDER/*.sql".to_string()],
                ignore_patterns: vec![],
                expect_needs_run_before: true,
                record_success: true,
                mutate: Some(|dir| {
                    fs::write(dir.join("a.sql"), "CREATE TABLE a_v2;").unwrap();
                }),
                expect_needs_run_after: true,
            },
            TestCase {
                name: "new file triggers re-run",
                setup: |dir| {
                    fs::write(dir.join("a.sql"), "CREATE TABLE a;").unwrap();
                },
                patterns: vec!["PLACEHOLDER/*.sql".to_string()],
                ignore_patterns: vec![],
                expect_needs_run_before: true,
                record_success: true,
                mutate: Some(|dir| {
                    fs::write(dir.join("b.sql"), "CREATE TABLE b;").unwrap();
                }),
                expect_needs_run_after: true,
            },
            TestCase {
                name: "deleted file triggers re-run",
                setup: |dir| {
                    fs::write(dir.join("a.sql"), "CREATE TABLE a;").unwrap();
                    fs::write(dir.join("b.sql"), "CREATE TABLE b;").unwrap();
                },
                patterns: vec!["PLACEHOLDER/*.sql".to_string()],
                ignore_patterns: vec![],
                expect_needs_run_before: true,
                record_success: true,
                mutate: Some(|dir| {
                    fs::remove_file(dir.join("b.sql")).unwrap();
                }),
                expect_needs_run_after: true,
            },
            TestCase {
                name: "ignored file changes do not trigger re-run",
                setup: |dir| {
                    fs::create_dir_all(dir.join("generated")).unwrap();
                    fs::write(dir.join("generated/schema.sql"), "CREATE TABLE a;").unwrap();
                },
                patterns: vec!["PLACEHOLDER/**/*.sql".to_string()],
                ignore_patterns: vec!["PLACEHOLDER/generated/**".to_string()],
                expect_needs_run_before: true,
                record_success: true,
                mutate: Some(|dir| {
                    fs::write(dir.join("generated/schema.sql"), "CREATE TABLE a_v2;").unwrap();
                }),
                expect_needs_run_after: false,
            },
            TestCase {
                name: "failed task still needs run",
                setup: |dir| {
                    fs::write(dir.join("a.sql"), "CREATE TABLE a;").unwrap();
                },
                patterns: vec!["PLACEHOLDER/*.sql".to_string()],
                ignore_patterns: vec![],
                expect_needs_run_before: true,
                record_success: false, // simulate failure
                mutate: None,
                expect_needs_run_after: true,
            },
        ];

        for case in &cases {
            let dir = TempDir::new(case.name);
            let state_dir = dir.path().join(".don-state");
            let state = TaskStateStore::new(state_dir);

            (case.setup)(dir.path());

            let patterns: Vec<String> = case
                .patterns
                .iter()
                .map(|p| p.replace("PLACEHOLDER", &dir.path().to_string_lossy()))
                .collect();
            let ignore_patterns: Vec<String> = case
                .ignore_patterns
                .iter()
                .map(|p| p.replace("PLACEHOLDER", &dir.path().to_string_lossy()))
                .collect();

            let needs_run = state
                .needs_run("test-task", &patterns, &ignore_patterns, None)
                .await
                .unwrap();
            assert_eq!(
                needs_run, case.expect_needs_run_before,
                "case '{}': needs_run before",
                case.name
            );

            if case.record_success {
                state
                    .record_success("test-task", &patterns, &ignore_patterns, None)
                    .await
                    .unwrap();
            }

            if let Some(mutate) = case.mutate {
                mutate(dir.path());
            }

            let needs_run = state
                .needs_run("test-task", &patterns, &ignore_patterns, None)
                .await
                .unwrap();
            assert_eq!(
                needs_run, case.expect_needs_run_after,
                "case '{}': needs_run after",
                case.name
            );

            let has_success = state.has_success("test-task").await.unwrap();
            assert_eq!(
                has_success, case.record_success,
                "case '{}': has_success after record_success",
                case.name
            );
        }
    }

    /// The self-write check is what stops a generator that rewrites its own
    /// watched inputs from re-triggering itself forever. It has to tell three
    /// situations apart: content that really moved, content rewritten
    /// byte-for-byte (mtime only, which is what an unconditional `rm -rf` plus
    /// copy produces), and a file set too large to be worth hashing.
    #[tokio::test]
    async fn test_check_watched_inputs_separates_rewritten_from_changed() {
        struct Case {
            name: &'static str,
            /// What the file holds when the hash is recorded.
            before: &'static str,
            /// What it holds when the check runs. Rewriting the same bytes is
            /// the self-write case.
            after: &'static str,
            /// Extra files to create, to push the set past the cap.
            extra_files: usize,
            max_files: usize,
            want: WatchedInputs,
        }

        let cases = vec![
            Case {
                name: "a byte-for-byte rewrite is not a change",
                before: "CREATE TABLE t;",
                after: "CREATE TABLE t;",
                extra_files: 0,
                max_files: MAX_VERIFY_FILES,
                want: WatchedInputs::Unchanged,
            },
            Case {
                name: "different contents are a change",
                before: "CREATE TABLE t;",
                after: "CREATE TABLE t2;",
                extra_files: 0,
                max_files: MAX_VERIFY_FILES,
                want: WatchedInputs::Changed,
            },
            Case {
                name: "a file set past the cap is not hashed at all",
                before: "CREATE TABLE t;",
                after: "CREATE TABLE t;",
                extra_files: 4,
                max_files: 3,
                want: WatchedInputs::Unverifiable { files: 5 },
            },
        ];

        for case in cases {
            let dir = TempDir::new("self-write-check");
            let watched = dir.path().join("watched.sql");
            fs::write(&watched, case.before).unwrap();
            for i in 0..case.extra_files {
                fs::write(dir.path().join(format!("extra{i}.sql")), "-- filler").unwrap();
            }

            let state = TaskStateStore::new(dir.path().join(".don-state"));
            let patterns = vec!["**/*.sql".to_string()];
            state
                .record_success("gen", &patterns, &[], Some(dir.path()))
                .await
                .unwrap();

            fs::write(&watched, case.after).unwrap();

            let got = state
                .check_watched_inputs(
                    "gen",
                    &patterns,
                    &[],
                    Some(dir.path()),
                    case.max_files,
                    |_| {},
                )
                .await
                .unwrap();
            assert_eq!(got, case.want, "{}", case.name);
        }
    }

    /// A task that has never succeeded has no hash to compare against, so the
    /// check must not read "unchanged" from a missing record and suppress the
    /// very first run.
    #[tokio::test]
    async fn test_check_watched_inputs_without_a_recorded_run_is_a_change() {
        let dir = TempDir::new("self-write-no-record");
        fs::write(dir.path().join("watched.sql"), "CREATE TABLE t;").unwrap();
        let state = TaskStateStore::new(dir.path().join(".don-state"));

        let got = state
            .check_watched_inputs(
                "gen",
                &["**/*.sql".to_string()],
                &[],
                Some(dir.path()),
                MAX_VERIFY_FILES,
                |_| {},
            )
            .await
            .unwrap();
        assert_eq!(got, WatchedInputs::Changed);
    }

    #[tokio::test]
    async fn test_needs_run_reports_hash_progress() {
        let dir = TempDir::new("hash-progress");
        let generated = dir.path().join("generated");
        fs::create_dir_all(&generated).unwrap();
        fs::write(dir.path().join("schema.sql"), "CREATE TABLE schema;").unwrap();
        fs::write(generated.join("ignored.sql"), "CREATE TABLE ignored;").unwrap();

        let state = TaskStateStore::new(dir.path().join(".don-state"));
        let patterns = vec!["**/*.sql".to_string()];
        let ignore_patterns = vec!["generated/**".to_string()];
        let (progress_tx, progress_rx) = std::sync::mpsc::channel();

        let needs_run = state
            .needs_run_with_progress(
                "schema",
                &patterns,
                &ignore_patterns,
                Some(dir.path()),
                move |progress| {
                    progress_tx.send(progress).unwrap();
                },
            )
            .await
            .unwrap();
        assert!(needs_run);

        let events: Vec<TaskHashProgress> = progress_rx.into_iter().collect();
        assert!(matches!(
            events.first(),
            Some(TaskHashProgress::GlobStarted { pattern })
                if pattern.ends_with("/**/*.sql")
        ));
        assert!(events.iter().any(|event| matches!(
            event,
            TaskHashProgress::GlobFinished {
                entries_seen: 2,
                files_matched: 1,
                files_ignored: 0,
                ..
            }
        )));
        assert!(
            events
                .iter()
                .any(|event| matches!(event, TaskHashProgress::HashStarted { total_files: 1 }))
        );
        assert!(events.iter().any(|event| matches!(
            event,
            TaskHashProgress::HashFinished {
                files_hashed: 1,
                bytes_hashed: 20,
                ..
            }
        )));
    }

    #[tokio::test]
    async fn test_base_dir_resolution() {
        let dir = TempDir::new("base-dir");
        let sub = dir.path().join("subdir");
        fs::create_dir_all(&sub).unwrap();
        fs::write(sub.join("a.sql"), "CREATE TABLE a;").unwrap();

        let state = TaskStateStore::new(dir.path().join(".don-state"));
        let patterns = vec!["*.sql".to_string()];

        // Without base_dir: glob resolves from cwd, won't find files in subdir
        let needs_run_no_base = state.needs_run("test", &patterns, &[], None).await.unwrap();
        // With base_dir pointing to subdir: should find the file
        let needs_run_with_base = state
            .needs_run("test", &patterns, &[], Some(&sub))
            .await
            .unwrap();

        // The cwd-relative glob likely finds nothing (no *.sql in cwd), so always needs run
        assert!(needs_run_no_base);
        // The subdir glob finds a.sql, and there's no stored hash, so also needs run
        assert!(needs_run_with_base);

        // Record success with base_dir
        state
            .record_success("test", &patterns, &[], Some(&sub))
            .await
            .unwrap();
        // Now it should skip
        assert!(
            !state
                .needs_run("test", &patterns, &[], Some(&sub))
                .await
                .unwrap()
        );
        // But without base_dir it still needs run (different glob results)
        assert!(state.needs_run("test", &patterns, &[], None).await.unwrap());
    }

    #[tokio::test]
    async fn test_clear() {
        let dir = TempDir::new("clear");
        let state = TaskStateStore::new(dir.path().join(".don-state"));

        fs::write(dir.path().join("a.txt"), "hello").unwrap();
        let patterns = vec![format!("{}/*.txt", dir.path().to_string_lossy())];

        state
            .record_success("my-task", &patterns, &[], None)
            .await
            .unwrap();
        assert!(state.has_success("my-task").await.unwrap());
        assert!(
            !state
                .needs_run("my-task", &patterns, &[], None)
                .await
                .unwrap()
        );

        state.clear("my-task").await.unwrap();
        assert!(!state.has_success("my-task").await.unwrap());
        assert!(
            state
                .needs_run("my-task", &patterns, &[], None)
                .await
                .unwrap()
        );

        // Clear on non-existent is fine
        state.clear("never-existed").await.unwrap();
    }

    #[tokio::test]
    async fn test_watchless_task_still_records_success() {
        let dir = TempDir::new("watchless-success");
        let state = TaskStateStore::new(dir.path().join(".don-state"));

        assert!(!state.has_success("bootstrap").await.unwrap());
        state
            .record_success("bootstrap", &[], &[], None)
            .await
            .unwrap();
        assert!(state.has_success("bootstrap").await.unwrap());
        assert!(state.needs_run("bootstrap", &[], &[], None).await.unwrap());
    }

    #[tokio::test]
    async fn test_last_run_metadata() {
        let dir = TempDir::new("last-run");
        let state = TaskStateStore::new(dir.path().join(".don-state"));

        assert!(state.last_run("build").await.unwrap().is_none());

        let failed = TaskRunInfo::finished_now(
            false,
            Some(Duration::from_millis(42)),
            Some(2),
            Some("exit code 2".to_string()),
        );
        state.record_run("build", &failed).await.unwrap();
        assert!(!state.has_success("build").await.unwrap());
        assert_eq!(state.last_run("build").await.unwrap(), Some(failed));

        let succeeded =
            TaskRunInfo::finished_now(true, Some(Duration::from_millis(7)), Some(0), None);
        state
            .record_success_with_info("build", &[], &[], None, &succeeded)
            .await
            .unwrap();
        assert!(state.has_success("build").await.unwrap());
        assert_eq!(state.last_run("build").await.unwrap(), Some(succeeded));
    }

    fn collect_paths(root: &Path, watch: &[&str], ignore: &[&str]) -> Vec<PathBuf> {
        let watch: Vec<glob::Pattern> = watch
            .iter()
            .map(|p| glob::Pattern::new(p).unwrap())
            .collect();
        let ignore_pats: Vec<glob::Pattern> = ignore
            .iter()
            .map(|p| glob::Pattern::new(p).unwrap())
            .collect();
        let prune: Vec<glob::Pattern> = ignore
            .iter()
            .filter_map(|p| {
                p.strip_suffix("/**")
                    .and_then(|pre| glob::Pattern::new(pre).ok())
            })
            .collect();
        let mut out = Vec::new();
        let mut stats = GlobWalkStats::default();
        collect_matching_files(
            root,
            &watch,
            &ignore_pats,
            &prune,
            &mut out,
            &mut stats,
            &mut |_| {},
        )
        .unwrap();
        out.sort();
        out
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_symlink_cycle_terminates() {
        use std::os::unix::fs::symlink;
        let dir = TempDir::new("symlink-cycle");
        let base = dir.path().to_path_buf();
        fs::create_dir_all(base.join("a")).unwrap();
        fs::write(base.join("watched.txt"), "hi").unwrap();
        // base/a/loop -> base makes base/a/loop/a/loop/... an infinite path.
        symlink(&base, base.join("a").join("loop")).unwrap();

        let state = TaskStateStore::new(base.join(".don-state"));
        let watch = format!("{}/**/*.txt", base.display());
        let patterns = vec![watch.clone()];

        // The old glob walk spins forever here; the timeout catches a regression.
        let needs = tokio::time::timeout(
            Duration::from_secs(20),
            state.needs_run("t", &patterns, &[], None),
        )
        .await
        .expect("needs_run must terminate on a symlink cycle")
        .unwrap();
        assert!(needs, "first run with no stored hash must run");

        assert_eq!(
            collect_paths(&base, &[&watch], &[]),
            vec![base.join("watched.txt")]
        );

        tokio::time::timeout(
            Duration::from_secs(20),
            state.record_success("t", &patterns, &[], None),
        )
        .await
        .expect("record_success must terminate on a symlink cycle")
        .unwrap();
        assert!(
            !state.needs_run("t", &patterns, &[], None).await.unwrap(),
            "unchanged tree must skip"
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_symlinked_directory_not_descended() {
        use std::os::unix::fs::symlink;
        let dir = TempDir::new("symlink-dir");
        let base = dir.path().to_path_buf();
        fs::create_dir_all(base.join("real")).unwrap();
        fs::write(base.join("real/inside.txt"), "x").unwrap();
        symlink(base.join("real"), base.join("link")).unwrap();

        let watch = format!("{}/**/*.txt", base.display());
        // base/link/inside.txt is reachable only through the symlinked dir.
        assert_eq!(
            collect_paths(&base, &[&watch], &[]),
            vec![base.join("real/inside.txt")]
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_symlinked_walk_root_not_descended() {
        use std::os::unix::fs::symlink;
        let dir = TempDir::new("symlink-root");
        let base = dir.path().to_path_buf();
        fs::create_dir_all(base.join("real/nested")).unwrap();
        fs::write(base.join("real/nested/inside.txt"), "x").unwrap();
        symlink(base.join("real"), base.join("link")).unwrap();

        let watch = format!("{}/link/**/*.txt", base.display());
        assert!(collect_paths(&base.join("link"), &[&watch], &[]).is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn test_symlink_to_file_included() {
        use std::os::unix::fs::symlink;
        let dir = TempDir::new("symlink-to-file");
        let base = dir.path().to_path_buf();
        fs::write(base.join("hidden_target"), "payload").unwrap();
        symlink(base.join("hidden_target"), base.join("a.txt")).unwrap();

        // a.txt is a symlink to a file; hidden_target does not match by name.
        let watch = format!("{}/*.txt", base.display());
        assert_eq!(
            collect_paths(&base, &[&watch], &[]),
            vec![base.join("a.txt")]
        );
    }

    #[test]
    fn test_ignored_subtree_pruned() {
        let dir = TempDir::new("prune-subtree");
        let base = dir.path().to_path_buf();
        fs::write(base.join("keep.sql"), "1").unwrap();
        fs::create_dir_all(base.join("skip/nested")).unwrap();
        for f in ["a.sql", "b.sql", "c.sql"] {
            fs::write(base.join("skip").join(f), "x").unwrap();
        }
        fs::write(base.join("skip/nested/d.sql"), "x").unwrap();
        // A sibling whose name merely starts with "skip" must NOT be pruned.
        fs::create_dir_all(base.join("skipper")).unwrap();
        fs::write(base.join("skipper/keep2.sql"), "2").unwrap();

        let watch = format!("{}/**/*.sql", base.display());
        let ignore = format!("{}/**/skip/**", base.display());
        assert_eq!(
            collect_paths(&base, &[&watch], &[&ignore]),
            vec![base.join("keep.sql"), base.join("skipper/keep2.sql")]
        );
    }

    #[test]
    fn test_separator_semantics_match_glob_glob() {
        struct Case {
            name: &'static str,
            setup: fn(&Path),
            watch: &'static str,
            ignore: &'static [&'static str],
            want: &'static [&'static str],
        }

        let cases = [
            Case {
                name: "star does not cross separator",
                setup: |base| {
                    fs::create_dir_all(base.join("src/a")).unwrap();
                    fs::write(base.join("src/x.ts"), "1").unwrap();
                    fs::write(base.join("src/a/b.ts"), "2").unwrap();
                },
                watch: "src/*.ts",
                ignore: &[],
                want: &["src/x.ts"],
            },
            Case {
                name: "globstar crosses separators",
                setup: |base| {
                    fs::create_dir_all(base.join("a/x/y")).unwrap();
                    fs::write(base.join("a/x.ts"), "1").unwrap();
                    fs::write(base.join("a/x/y/z.ts"), "2").unwrap();
                },
                watch: "a/**/*.ts",
                ignore: &[],
                want: &["a/x/y/z.ts", "a/x.ts"],
            },
            Case {
                name: "globstar prune matches zero components",
                setup: |base| {
                    fs::create_dir_all(base.join("a/node_modules")).unwrap();
                    fs::write(base.join("a/keep.ts"), "1").unwrap();
                    fs::write(base.join("a/node_modules/dep.ts"), "2").unwrap();
                },
                watch: "a/**/*.ts",
                ignore: &["a/**/node_modules/**"],
                want: &["a/keep.ts"],
            },
            Case {
                name: "sibling with shared prefix not pruned",
                setup: |base| {
                    fs::write(base.join("keep.sql"), "1").unwrap();
                    fs::create_dir_all(base.join("skip")).unwrap();
                    fs::write(base.join("skip/x.sql"), "2").unwrap();
                    fs::create_dir_all(base.join("skipper")).unwrap();
                    fs::write(base.join("skipper/y.sql"), "3").unwrap();
                },
                watch: "**/*.sql",
                ignore: &["**/skip/**"],
                want: &["keep.sql", "skipper/y.sql"],
            },
            Case {
                name: "matching is case sensitive",
                setup: |base| {
                    fs::create_dir_all(base.join("src")).unwrap();
                    fs::write(base.join("src/lower.ts"), "1").unwrap();
                    fs::write(base.join("src/UPPER.TS"), "2").unwrap();
                },
                watch: "src/*.ts",
                ignore: &[],
                want: &["src/lower.ts"],
            },
            Case {
                name: "ignore matching is case sensitive",
                setup: |base| {
                    fs::create_dir_all(base.join("src")).unwrap();
                    fs::write(base.join("src/lower.ts"), "1").unwrap();
                    fs::write(base.join("src/UPPER.TS"), "2").unwrap();
                },
                watch: "src/*.*",
                ignore: &["src/*.ts"],
                want: &["src/UPPER.TS"],
            },
            Case {
                name: "ignore star covers a matched directory subtree",
                setup: |base| {
                    fs::create_dir_all(base.join("generated/nested")).unwrap();
                    fs::write(base.join("generated/direct.ts"), "1").unwrap();
                    fs::write(base.join("generated/nested/deep.ts"), "2").unwrap();
                },
                watch: "generated/**/*.ts",
                ignore: &["generated/*"],
                want: &[],
            },
        ];

        for case in cases {
            let dir = TempDir::new(case.name);
            let base = dir.path().to_path_buf();
            (case.setup)(&base);

            let watch = format!("{}/{}", base.display(), case.watch);
            let ignore: Vec<String> = case
                .ignore
                .iter()
                .map(|pattern| format!("{}/{}", base.display(), pattern))
                .collect();
            let ignore_refs: Vec<&str> = ignore.iter().map(String::as_str).collect();

            let got = collect_paths(&base, &[&watch], &ignore_refs);
            let want: Vec<PathBuf> = case.want.iter().map(|rel| base.join(rel)).collect();
            assert_eq!(got, want, "case '{}'", case.name);
        }
    }

    #[tokio::test]
    async fn test_missing_glob_root_is_an_empty_match_set() {
        let dir = TempDir::new("missing-glob-root");
        let base = dir.path().to_path_buf();
        let state = TaskStateStore::new(base.join(".don-state"));
        let watch = vec![format!("{}/generated/**/*.sql", base.display())];

        state.record_success("t", &watch, &[], None).await.unwrap();
        assert!(!state.needs_run("t", &watch, &[], None).await.unwrap());

        fs::create_dir_all(base.join("generated")).unwrap();
        fs::write(base.join("generated/schema.sql"), "create table t;").unwrap();
        assert!(state.needs_run("t", &watch, &[], None).await.unwrap());
    }

    #[test]
    fn test_current_dir_prefix_does_not_break_top_level_glob() {
        let paths = collect_paths(Path::new("."), &["*.toml"], &[]);
        assert!(paths.contains(&PathBuf::from("Cargo.toml")));
    }

    #[tokio::test]
    async fn test_literal_watch_tracks_only_target_file() {
        let dir = TempDir::new("literal-fast-path");
        let base = dir.path().to_path_buf();
        fs::write(base.join("package.json"), "{\"v\":1}").unwrap();
        // A large-ish sibling subtree the literal watch must not track.
        fs::create_dir_all(base.join("node_modules/dep/sub")).unwrap();
        for i in 0..50 {
            fs::write(base.join(format!("node_modules/dep/sub/f{i}.js")), "x").unwrap();
        }

        let state = TaskStateStore::new(base.join(".don-state"));
        let watch = vec![format!("{}/package.json", base.display())];

        assert!(state.needs_run("t", &watch, &[], None).await.unwrap());
        state.record_success("t", &watch, &[], None).await.unwrap();
        assert!(!state.needs_run("t", &watch, &[], None).await.unwrap());

        // A change to an unrelated sibling must not re-trigger the task.
        fs::write(base.join("node_modules/dep/sub/f0.js"), "changed").unwrap();
        assert!(!state.needs_run("t", &watch, &[], None).await.unwrap());

        // A change to the literal target itself must re-trigger it.
        fs::write(base.join("package.json"), "{\"v\":2}").unwrap();
        assert!(state.needs_run("t", &watch, &[], None).await.unwrap());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_literal_symlink_watch_tracked() {
        use std::os::unix::fs::symlink;
        let dir = TempDir::new("literal-symlink");
        let base = dir.path().to_path_buf();
        fs::write(base.join("real.json"), "{\"v\":1}").unwrap();
        symlink(base.join("real.json"), base.join("link.json")).unwrap();

        let state = TaskStateStore::new(base.join(".don-state"));
        // A literal watch on a symlink-to-file is tracked and its content hashed.
        let watch = vec![format!("{}/link.json", base.display())];

        state.record_success("t", &watch, &[], None).await.unwrap();
        assert!(!state.needs_run("t", &watch, &[], None).await.unwrap());

        fs::write(base.join("real.json"), "{\"v\":2}").unwrap();
        assert!(state.needs_run("t", &watch, &[], None).await.unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn test_broken_symlink_is_no_match_without_error() {
        use std::os::unix::fs::symlink;
        let dir = TempDir::new("broken-symlink");
        let base = dir.path().to_path_buf();
        fs::write(base.join("real.txt"), "x").unwrap();
        symlink(base.join("does-not-exist"), base.join("dangling.txt")).unwrap();

        // The dangling link must not match and must not error the walk (collect_paths
        // unwraps, so an unexpected Err here would panic the test).
        let watch = format!("{}/*.txt", base.display());
        assert_eq!(
            collect_paths(&base, &[&watch], &[]),
            vec![base.join("real.txt")]
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_unreadable_dir_surfaces_error() {
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new("unreadable-dir");
        let base = dir.path().to_path_buf();
        let locked = base.join("locked");
        fs::create_dir_all(&locked).unwrap();
        fs::write(locked.join("a.txt"), "x").unwrap();

        struct RestorePerms(PathBuf);
        impl Drop for RestorePerms {
            fn drop(&mut self) {
                let _ = fs::set_permissions(&self.0, fs::Permissions::from_mode(0o755));
            }
        }
        let _restore = RestorePerms(locked.clone());
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o000)).unwrap();

        // Root bypasses permission bits; only assert when the dir is truly unreadable.
        if fs::read_dir(&locked).is_ok() {
            return;
        }

        let state = TaskStateStore::new(base.join(".don-state"));
        let watch = vec![format!("{}/**/*.txt", base.display())];
        let result = state.compute_hash(&watch, &[], None);
        assert!(
            matches!(result, Err(TaskStateError::Io(_))),
            "unreadable dir must surface as an Io error, got {result:?}"
        );
    }
}
