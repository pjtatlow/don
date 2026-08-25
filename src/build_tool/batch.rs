use crate::process::ProcessKind;
use crate::process::paths::any_glob_path_changed_since;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

pub(crate) struct RebuildBatchRequest {
    pub(crate) bazel_items: Vec<BazelRebuildItem>,
    pub(crate) plain_rebuilds: Vec<String>,
    pub(crate) force: bool,
}

/// What makes two targets shareable in one `bazel build`: the directory the
/// labels belong to, and the `.bazelrc` configuration to build them under.
/// A single invocation takes one `--config`, so both have to match.
type BazelBatchKey = (PathBuf, Option<String>);

/// Prepare-side groups: the items in each batch, paired with their labels.
type BazelPrepareGroups = HashMap<BazelBatchKey, Vec<(BatchBuildItem, String)>>;

pub(crate) struct BazelRebuildItem {
    pub(crate) name: String,
    pub(crate) target: String,
    pub(crate) working_dir: PathBuf,
    /// `.bazelrc` configuration to build under, already resolved against the
    /// workspace default. Part of the batching key: targets built under
    /// different configurations are different builds and cannot share one
    /// `bazel build`.
    pub(crate) config: Option<String>,
}

pub(crate) struct RebuildBatchOutcome {
    pub(crate) build_succeeded: Vec<String>,
    pub(crate) up_to_date: Vec<String>,
    pub(crate) failed: Vec<(String, String)>,
    pub(crate) plain_rebuilds: Vec<String>,
}

#[derive(Clone)]
pub(crate) struct GraphRequeryRequestItem {
    pub(crate) name: String,
    pub(crate) kind: ProcessKind,
    pub(crate) bazel: Option<crate::config::BazelConfig>,
    pub(crate) watch_enabled: bool,
    pub(crate) working_dir: PathBuf,
    pub(crate) ignore_patterns: Vec<String>,
    /// Project-wide watch-ignore globs, for the tier-1 graph registration
    /// this item's re-query rewrites alongside its own.
    pub(crate) global_watch_ignore: Vec<String>,
}

struct GraphRequeryOutcomeItem {
    name: String,
    watch_enabled: bool,
    ignore_patterns: Vec<String>,
    kind: ProcessKind,
    global_watch_ignore: Vec<String>,
    result: Result<crate::build_tool::ResolvedBuildInfo, String>,
}

/// What a re-query decided about one item, delivered to its supervisor.
///
/// The watch registrations it produces are applied before this is sent — the
/// build manager owns watch-path resolution, and pushing them is the same
/// step it already runs after a preparation build. What is left for the
/// supervisor is the only part that depends on lifecycle: whether the
/// process it is running should be rebuilt from the graph that just moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequeryOutcome {
    /// Watch patterns were re-resolved and registered.
    Updated,
    /// The re-query failed; the existing patterns still stand.
    Failed,
}

/// Snapshot of a service or task that needs its artifact built. Owned — the
/// detached chain runs entirely off this and never touches live scheduler
/// state.
#[derive(Clone)]
pub(crate) struct BatchBuildItem {
    pub(crate) name: String,
    pub(crate) kind: ProcessKind,
    pub(crate) bazel: Option<crate::config::BazelConfig>,
    /// Whether build-tool-resolved source and graph paths should be watched.
    pub(crate) watch_enabled: bool,
    /// Absolute directory where the build tool should be invoked.
    pub(crate) working_dir: PathBuf,
    /// Ignore patterns to carry through to the watch manager.
    pub(crate) ignore: Vec<String>,
}

/// What the chain decided about one item.
///
/// Per item rather than per batch because the batch is cross-item by nature —
/// one `bazel build //a //b //c` — but its *consequences* belong to whoever
/// owns that process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PrepareOutcome {
    /// The artifact exists and the process may run. `binary_path` is the
    /// absolute executable `bazel cquery --output=files` reported; `None`
    /// means fall back to `bazel run`.
    Ready { binary_path: Option<String> },
    /// The build succeeded, but a watched source or BUILD file changed while
    /// it was running — the artifact is already out of date, so ask again
    /// before starting. This is the *only* cover for that window: the watcher
    /// is not yet watching those paths, because this build is what resolves
    /// them.
    Stale,
    /// The build tool refused. Never retried — recompiling unchanged sources
    /// cannot change the answer.
    Failed(String),
}

/// Everything one run of the chain produces.
pub(crate) struct PrepareBatchOutcome {
    /// Non-fatal warnings (query failures, binary-path cquery failures).
    pub(crate) warnings: Vec<String>,
    /// Exactly one outcome per requested item, in request order.
    pub(crate) items: Vec<(String, PrepareOutcome)>,
}

pub(crate) async fn run_rebuild_batch_worker(
    request: RebuildBatchRequest,
    emitter: crate::output::LifecycleEmitter,
    bazel_build_mutex: Arc<tokio::sync::Mutex<()>>,
) -> RebuildBatchOutcome {
    let mut build_succeeded: HashSet<String> = HashSet::new();
    let mut up_to_date: HashSet<String> = HashSet::new();
    let mut failed: Vec<(String, String)> = Vec::new();

    // Keyed on the configuration as well as the directory: `bazel build`
    // takes one `--config`, so targets wanting different ones are different
    // builds however close together they were asked for.
    let mut bazel_by_dir: HashMap<BazelBatchKey, Vec<BazelRebuildItem>> = HashMap::new();
    for item in request.bazel_items {
        bazel_by_dir
            .entry((
                bazel_graph_requery_group_dir(&item.working_dir),
                item.config.clone(),
            ))
            .or_default()
            .push(item);
    }

    if !bazel_by_dir.is_empty() {
        let _guard = bazel_build_mutex.lock().await;

        for ((working_dir, config), items) in bazel_by_dir {
            let targets: Vec<String> = items.iter().map(|item| item.target.clone()).collect();
            let target_to_names: HashMap<String, Vec<String>> = {
                let mut names: HashMap<String, Vec<String>> = HashMap::new();
                for item in &items {
                    names
                        .entry(item.target.clone())
                        .or_default()
                        .push(item.name.clone());
                }
                names
            };

            let resolver =
                crate::build_tool::bazel::BazelResolver::new().with_emitter(emitter.clone());
            let all_up_to_date = if request.force {
                false
            } else {
                resolver
                    .check_up_to_date(&targets, &working_dir, config.as_deref())
                    .await
                    .unwrap_or_default()
            };
            if all_up_to_date {
                let count = targets.len();
                emitter.bazel_event(&format!(
                    "{count} target{} up to date, skipping rebuild",
                    if count == 1 { "" } else { "s" }
                ));
                for item in &items {
                    up_to_date.insert(item.name.clone());
                }
            } else {
                let count = targets.len();
                emitter.bazel_event(&format!(
                    "rebuilding {count} target{}...",
                    if count == 1 { "" } else { "s" }
                ));
                let line_emitter = emitter.clone();
                match resolver
                    .build_targets(
                        &targets,
                        &working_dir,
                        config.as_deref(),
                        move |line| {
                            line_emitter.bazel_event(line);
                        },
                        Some(&emitter),
                    )
                    .await
                {
                    Ok(result) => {
                        for target in &result.succeeded {
                            if let Some(names) = target_to_names.get(target) {
                                for name in names {
                                    build_succeeded.insert(name.clone());
                                }
                            }
                        }
                        for (target, message) in &result.failed {
                            if let Some(names) = target_to_names.get(target) {
                                for name in names {
                                    failed.push((
                                        name.clone(),
                                        format!("bazel build failed: {message}"),
                                    ));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        for item in &items {
                            failed.push((item.name.clone(), format!("bazel build error: {e}")));
                        }
                    }
                }
            }
        }
    }

    RebuildBatchOutcome {
        build_succeeded: build_succeeded.into_iter().collect(),
        up_to_date: up_to_date.into_iter().collect(),
        failed,
        plain_rebuilds: request.plain_rebuilds,
    }
}

pub(crate) fn bazel_graph_requery_group_dir(working_dir: &Path) -> PathBuf {
    crate::build_tool::bazel::find_workspace_root(working_dir)
        .unwrap_or_else(|| working_dir.to_path_buf())
}

fn watch_resolution_items(items: &[BatchBuildItem]) -> Vec<&BatchBuildItem> {
    items
        .iter()
        .filter(|i| i.watch_enabled && i.bazel.is_some())
        .collect()
}

pub(crate) async fn send_watch_update(
    tx: &mpsc::UnboundedSender<crate::watch::WatchUpdate>,
    name: String,
    kind: crate::watch::WatchItemKind,
    patterns: Vec<String>,
    ignore_patterns: Vec<String>,
    base_dir: PathBuf,
) {
    let (applied_tx, applied_rx) = oneshot::channel();
    if tx
        .send(crate::watch::WatchUpdate {
            name,
            kind,
            patterns,
            ignore_patterns,
            base_dir,
            applied_tx: Some(applied_tx),
        })
        .is_ok()
    {
        let _ = applied_rx.await;
    }
}

/// Re-query the build tool for these items, register the watch patterns it
/// resolves, and report per item.
///
/// The registration half runs here rather than at the call site for the same
/// reason it does in [`run_batch_build_chain`]: watch paths are resolved *by*
/// this query, so the thing that resolves them is the thing that should
/// deliver them. What comes back is only the part that depends on lifecycle.
pub(crate) async fn run_graph_requery_worker(
    items: Vec<GraphRequeryRequestItem>,
    emitter: crate::output::LifecycleEmitter,
    watch_update_tx: Option<mpsc::UnboundedSender<crate::watch::WatchUpdate>>,
    base_dir: PathBuf,
) -> Vec<(String, RequeryOutcome)> {
    let mut outcomes: Vec<Option<GraphRequeryOutcomeItem>> =
        std::iter::repeat_with(|| None).take(items.len()).collect();
    let mut bazel_groups: HashMap<PathBuf, Vec<(usize, GraphRequeryRequestItem)>> = HashMap::new();

    for (idx, item) in items.into_iter().enumerate() {
        if !item.watch_enabled {
            outcomes[idx] = Some(GraphRequeryOutcomeItem {
                name: item.name,
                watch_enabled: false,
                ignore_patterns: item.ignore_patterns,
                kind: item.kind,
                global_watch_ignore: item.global_watch_ignore,
                result: Ok(crate::build_tool::ResolvedBuildInfo {
                    watch_paths: Vec::new(),
                    graph_definition_globs: Vec::new(),
                }),
            });
        } else if item.bazel.is_some() {
            bazel_groups
                .entry(bazel_graph_requery_group_dir(&item.working_dir))
                .or_default()
                .push((idx, item));
        }
        // Bazel is the only build tool, so a watch-enabled item without a
        // bazel config has nothing to re-query. `flush_pending_graph_requery`
        // already filters those out; leaving no outcome drops it either way.
    }

    for (working_dir, group) in bazel_groups {
        let targets: Vec<String> = group
            .iter()
            .filter_map(|(_, item)| item.bazel.as_ref().map(|b| b.target.clone()))
            .collect();
        let resolver = crate::build_tool::bazel::BazelResolver::new().with_emitter(emitter.clone());
        match resolver.resolve_per_target(&targets, &working_dir).await {
            Ok(info_by_target) => {
                for (idx, item) in group {
                    let result = if let Some(ref bazel) = item.bazel {
                        Ok(info_by_target.get(&bazel.target).cloned().unwrap_or(
                            crate::build_tool::ResolvedBuildInfo {
                                watch_paths: Vec::new(),
                                graph_definition_globs: Vec::new(),
                            },
                        ))
                    } else {
                        Err("missing bazel config for batched graph re-query".to_string())
                    };
                    outcomes[idx] = Some(GraphRequeryOutcomeItem {
                        name: item.name,
                        watch_enabled: item.watch_enabled,
                        ignore_patterns: item.ignore_patterns,
                        kind: item.kind,
                        global_watch_ignore: item.global_watch_ignore,
                        result,
                    });
                }
            }
            Err(e) => {
                let message = e.to_string();
                for (idx, item) in group {
                    outcomes[idx] = Some(GraphRequeryOutcomeItem {
                        name: item.name,
                        watch_enabled: item.watch_enabled,
                        ignore_patterns: item.ignore_patterns,
                        kind: item.kind,
                        global_watch_ignore: item.global_watch_ignore,
                        result: Err(message.clone()),
                    });
                }
            }
        }
    }

    let mut delivered = Vec::new();
    for outcome in outcomes.into_iter().flatten() {
        let info = match outcome.result {
            Ok(info) => info,
            Err(e) => {
                emitter.service_error_event(
                    &outcome.name,
                    &format!("build tool re-query failed: {e} — keeping existing watch patterns"),
                );
                delivered.push((outcome.name, RequeryOutcome::Failed));
                continue;
            }
        };
        let count = info.watch_paths.len();
        emitter.service_event(
            &outcome.name,
            &format!(
                "updated watch paths ({count} path{})",
                if count == 1 { "" } else { "s" }
            ),
        );
        if outcome.watch_enabled
            && let Some(ref tx) = watch_update_tx
        {
            let watch_kind = match outcome.kind {
                ProcessKind::Service => crate::watch::WatchItemKind::Service,
                ProcessKind::Task => crate::watch::WatchItemKind::Task,
            };
            send_watch_update(
                tx,
                outcome.name.clone(),
                watch_kind,
                info.watch_paths,
                outcome.ignore_patterns,
                base_dir.clone(),
            )
            .await;
            send_watch_update(
                tx,
                format!("{}__graph", outcome.name),
                crate::watch::WatchItemKind::BuildGraph,
                info.graph_definition_globs,
                crate::process::paths::resolve_watch_ignore_patterns(
                    &base_dir,
                    &[],
                    &base_dir,
                    &outcome.global_watch_ignore,
                ),
                base_dir.clone(),
            )
            .await;
        }
        delivered.push((outcome.name, RequeryOutcome::Updated));
    }
    delivered
}

/// Run the full artifact-preparation chain: optional watch resolution →
/// batch build → bazel binary-path cquery → the mtime scan that catches an
/// edit made while the build ran. Pure off-task function that takes owned
/// inputs and returns one [`PrepareOutcome`] per item.
///
/// Sends [`crate::watch::WatchUpdate`]s directly to the watch manager as they
/// resolve — before the builds complete, and therefore before any supervisor
/// waiting on an outcome can spawn.
pub(crate) async fn run_batch_build_chain(
    items: Vec<BatchBuildItem>,
    base_dir: PathBuf,
    emitter: crate::output::LifecycleEmitter,
    watch_update_tx: Option<mpsc::UnboundedSender<crate::watch::WatchUpdate>>,
    global_watch_ignore: Vec<String>,
    bazel_build_mutex: Arc<tokio::sync::Mutex<()>>,
) -> PrepareBatchOutcome {
    let scan_since = SystemTime::now();
    let mut outcome = PrepareBatchOutcome {
        warnings: Vec::new(),
        items: Vec::new(),
    };
    if items.is_empty() {
        return outcome;
    }
    // Held for the whole chain. `bazel query`, `bazel build` and
    // `bazel cquery` all contend for the same workspace analysis lock, so a
    // rebuild batch must not interleave with a preparation — which used to be
    // true only because the scheduler serialised the two by construction.
    let _bazel_guard = bazel_build_mutex.lock().await;
    let mut succeeded: HashSet<String> = HashSet::new();
    let mut failed: HashMap<String, String> = HashMap::new();
    let mut binary_paths: HashMap<String, String> = HashMap::new();

    // Step 1: resolve watch paths with one query per build-tool working set.
    //
    // Previously we ran N parallel `bazel query` subprocesses, one per item.
    // Bazel's server has a workspace-wide analysis lock, so those parallel
    // queries mostly serialised inside bazel anyway — and each process
    // startup cost stacked. Now we issue one
    // `deps(T1 + ... + Tn) --output=xml` per Bazel workspace and DFS-walk per
    // target client-side to keep accurate per-service attribution.

    // Partition items by tool. Each item keeps its own ignore patterns
    // (configured per service/task) but gets the same tier-1/tier-2 globs.
    let bazel_items = watch_resolution_items(&items);

    let mut resolved_info_by_item: HashMap<String, crate::build_tool::ResolvedBuildInfo> =
        HashMap::new();

    if !bazel_items.is_empty() {
        let mut bazel_by_dir: HashMap<PathBuf, Vec<&BatchBuildItem>> = HashMap::new();
        for item in &bazel_items {
            bazel_by_dir
                .entry(bazel_graph_requery_group_dir(&item.working_dir))
                .or_default()
                .push(*item);
        }

        for (working_dir, items_for_dir) in bazel_by_dir {
            let targets: Vec<String> = items_for_dir
                .iter()
                .filter_map(|i| i.bazel.as_ref().map(|b| b.target.clone()))
                .collect();
            emitter.bazel_event(&format!(
                "querying bazel watch paths for {} target{}...",
                targets.len(),
                if targets.len() == 1 { "" } else { "s" },
            ));
            let resolver =
                crate::build_tool::bazel::BazelResolver::new().with_emitter(emitter.clone());
            match resolver.resolve_per_target(&targets, &working_dir).await {
                Ok(info_by_target) => {
                    let unique: HashSet<&String> = info_by_target
                        .values()
                        .flat_map(|i| i.watch_paths.iter())
                        .collect();
                    emitter.bazel_event(&format!(
                        "resolved {} unique watch path{} across {} target{}",
                        unique.len(),
                        if unique.len() == 1 { "" } else { "s" },
                        targets.len(),
                        if targets.len() == 1 { "" } else { "s" },
                    ));
                    for item in items_for_dir {
                        let Some(target) = item.bazel.as_ref().map(|b| &b.target) else {
                            continue;
                        };
                        if let Some(info) = info_by_target.get(target) {
                            resolved_info_by_item.insert(item.name.clone(), info.clone());
                        }
                    }
                }
                Err(e) => {
                    outcome.warnings.push(format!(
                        "bazel query failed in {}: {e}",
                        working_dir.display()
                    ));
                }
            }
        }
    }

    // Attribute resolved watch paths to each item. Bazel items get their
    // own per-target result (computed by DFS from the unified XML graph).
    // Each item emits its own
    // `WatchUpdate` so the watch manager keeps a per-service/task
    // `WatchedItem` entry keyed by name. Directories in the watcher dedup
    // via `registered_dirs`, so the actual inotify cost is paid once per
    // directory regardless of how many items claim it.
    for item in &items {
        let info = resolved_info_by_item.get(&item.name);
        let Some(info) = info else {
            continue; // query failed for this tool — warning already pushed
        };

        if item.watch_enabled
            && let Some(ref tx) = watch_update_tx
        {
            let watch_kind = match item.kind {
                ProcessKind::Service => crate::watch::WatchItemKind::Service,
                ProcessKind::Task => crate::watch::WatchItemKind::Task,
            };
            send_watch_update(
                tx,
                item.name.clone(),
                watch_kind,
                info.watch_paths.clone(),
                item.ignore.clone(),
                base_dir.clone(),
            )
            .await;
            if !info.graph_definition_globs.is_empty() {
                send_watch_update(
                    tx,
                    format!("{}__graph", item.name),
                    crate::watch::WatchItemKind::BuildGraph,
                    info.graph_definition_globs.clone(),
                    global_watch_ignore.clone(),
                    base_dir.clone(),
                )
                .await;
            }
        }
    }

    // Step 2: batch builds. Bazel groups run concurrently, but each group
    // stays inside the working directory its labels belong to.
    let mut bazel_by_dir: BazelPrepareGroups = HashMap::new();

    for item in &items {
        if let Some(ref bazel) = item.bazel {
            bazel_by_dir
                .entry((
                    bazel_graph_requery_group_dir(&item.working_dir),
                    bazel.config.clone(),
                ))
                .or_default()
                .push((item.clone(), bazel.target.clone()));
        }
    }

    let mut build_set: JoinSet<crate::build_tool::BatchBuildResult> = JoinSet::new();

    for ((working_dir, config), bazel_items) in bazel_by_dir {
        let targets: Vec<String> = bazel_items.iter().map(|(_, t)| t.clone()).collect();
        let target_to_names: HashMap<String, Vec<String>> = {
            let mut m: HashMap<String, Vec<String>> = HashMap::new();
            for (item, target) in &bazel_items {
                m.entry(target.clone()).or_default().push(item.name.clone());
            }
            m
        };
        let count = targets.len();
        let em = emitter.clone();
        let em_spawn = emitter.clone();
        emitter.bazel_event(&format!(
            "building {count} target{}...",
            if count == 1 { "" } else { "s" }
        ));
        build_set.spawn(async move {
            let resolver = crate::build_tool::bazel::BazelResolver::new();
            let result = resolver
                .build_targets(
                    &targets,
                    &working_dir,
                    config.as_deref(),
                    move |line| {
                        em.bazel_event(line);
                    },
                    Some(&em_spawn),
                )
                .await;
            match result {
                Ok(batch) => {
                    let mut succeeded: Vec<String> = Vec::new();
                    for target in &batch.succeeded {
                        if let Some(names) = target_to_names.get(target) {
                            succeeded.extend(names.clone());
                        }
                    }
                    let mut failed: Vec<(String, String)> = Vec::new();
                    for (target, msg) in &batch.failed {
                        if let Some(names) = target_to_names.get(target) {
                            for n in names {
                                failed.push((n.clone(), msg.clone()));
                            }
                        }
                    }
                    crate::build_tool::BatchBuildResult { succeeded, failed }
                }
                // Resolver errored before producing per-target results
                // (e.g. bazel client missing, I/O failure). Mark every item
                // in this batch as failed so the runner doesn't leave them
                // sitting in `Building` forever.
                Err(e) => {
                    let msg = format!("bazel build error: {e}");
                    let failed: Vec<(String, String)> = target_to_names
                        .values()
                        .flatten()
                        .map(|n| (n.clone(), msg.clone()))
                        .collect();
                    crate::build_tool::BatchBuildResult {
                        succeeded: Vec::new(),
                        failed,
                    }
                }
            }
        });
    }

    while let Some(result) = build_set.join_next().await {
        match result {
            Ok(batch) => {
                for name in batch.succeeded {
                    succeeded.insert(name);
                }
                for (name, msg) in batch.failed {
                    failed.insert(name, msg);
                }
            }
            Err(e) => outcome
                .warnings
                .push(format!("batch build task panicked: {e}")),
        }
    }

    let built_count = succeeded.len();
    if built_count > 0 {
        emitter.lifecycle_event(&format!(
            "batch build complete: {built_count} item{} built",
            if built_count == 1 { "" } else { "s" }
        ));
    }

    // Step 3: resolve every succeeded bazel service's built-binary path,
    // grouped by workspace. Lets a supervisor spawn the artifact directly
    // instead of via `bazel run`. Tasks don't need this.
    let bazel_services_to_resolve: Vec<&BatchBuildItem> = items
        .iter()
        .filter(|i| {
            i.kind == ProcessKind::Service && i.bazel.is_some() && succeeded.contains(&i.name)
        })
        .collect();

    if !bazel_services_to_resolve.is_empty() {
        let mut services_by_dir: HashMap<PathBuf, Vec<&BatchBuildItem>> = HashMap::new();
        for item in &bazel_services_to_resolve {
            services_by_dir
                .entry(bazel_graph_requery_group_dir(&item.working_dir))
                .or_default()
                .push(*item);
        }

        for (working_dir, items_for_dir) in services_by_dir {
            let targets: Vec<String> = items_for_dir
                .iter()
                .filter_map(|i| i.bazel.as_ref().map(|b| b.target.clone()))
                .collect();
            emitter.bazel_event(&format!(
                "resolving bazel binary paths for {} target{}...",
                targets.len(),
                if targets.len() == 1 { "" } else { "s" },
            ));
            let resolver =
                crate::build_tool::bazel::BazelResolver::new().with_emitter(emitter.clone());
            match resolver.resolve_binary_paths(&targets, &working_dir).await {
                Ok(paths_by_label) => {
                    for item in &items_for_dir {
                        let Some(ref bazel) = item.bazel else {
                            continue;
                        };
                        match paths_by_label.get(&bazel.target) {
                            Some(rel_path) => {
                                let abs_path =
                                    resolve_bazel_binary_abs_path(&item.working_dir, rel_path);
                                let path_str = abs_path.to_string_lossy().to_string();
                                emitter.service_debug_event(
                                    &item.name,
                                    &format!("resolved binary {rel_path}"),
                                );
                                binary_paths.insert(item.name.clone(), path_str);
                            }
                            None => {
                                outcome.warnings.push(format!(
                                    "{}: no binary output for {} — falling back to bazel run",
                                    item.name, bazel.target
                                ));
                            }
                        }
                    }
                }
                Err(e) => {
                    outcome.warnings.push(format!(
                        "bazel cquery for binary paths failed in {}: {e} — falling back to bazel run for {} service{}",
                        working_dir.display(),
                        items_for_dir.len(),
                        if items_for_dir.len() == 1 { "" } else { "s" },
                    ));
                }
            }
        }
    }

    // Step 4: decide each item, newest-mtime scan included. Every requested
    // name gets exactly one outcome — a supervisor is waiting on it and
    // silence would leave it parked forever.
    for item in &items {
        let decided = if let Some(message) = failed.get(&item.name) {
            PrepareOutcome::Failed(message.clone())
        } else if succeeded.contains(&item.name) {
            if let Some(reason) = stale_since(
                item,
                resolved_info_by_item.get(&item.name),
                &base_dir,
                scan_since,
            ) {
                emitter.service_event(&item.name, reason);
                PrepareOutcome::Stale
            } else {
                PrepareOutcome::Ready {
                    binary_path: binary_paths.get(&item.name).cloned(),
                }
            }
        } else {
            // The resolver returned neither a success nor a failure for this
            // target — a panicked worker, or a target the batch never
            // covered. Reporting it as failed is what keeps the process out
            // of a permanent `Building`.
            PrepareOutcome::Failed("build tool returned no result for this target".to_string())
        };
        outcome.items.push((item.name.clone(), decided));
    }

    outcome
}

/// Whether an input changed while this item was being built, and how to say
/// so. The scan is against the timestamp taken *before* the chain started, so
/// it covers the whole query → build → cquery pipeline.
///
/// This is not the rebuild cycle's staleness flag and cannot be merged with
/// it: that one is learned from the watcher, and during this build nothing is
/// watching these paths yet — they are what this build resolves.
fn stale_since(
    item: &BatchBuildItem,
    info: Option<&crate::build_tool::ResolvedBuildInfo>,
    base_dir: &Path,
    scan_since: SystemTime,
) -> Option<&'static str> {
    if !item.watch_enabled {
        return None;
    }
    let info = info?;
    let source_changed =
        any_glob_path_changed_since(base_dir, &info.watch_paths, &item.ignore, scan_since);
    let graph_changed =
        any_glob_path_changed_since(base_dir, &info.graph_definition_globs, &[], scan_since);
    match (source_changed, graph_changed, item.kind) {
        (true, true, ProcessKind::Service) => {
            Some("files changed during build — rebuilding before start")
        }
        (true, false, ProcessKind::Service) => {
            Some("source files changed during build — rebuilding before start")
        }
        (false, true, ProcessKind::Service) => {
            Some("build graph changed during build — rebuilding before start")
        }
        (true, true, ProcessKind::Task) => {
            Some("files changed during build — re-running build before start")
        }
        (true, false, ProcessKind::Task) => {
            Some("source files changed during build — re-running build before start")
        }
        (false, true, ProcessKind::Task) => {
            Some("build graph changed during build — re-running build before start")
        }
        (false, false, _) => None,
    }
}

/// Resolve a bazel-reported executable path to an absolute path.
///
/// `bazel cquery` reports `target.files_to_run.executable.path` relative to the
/// **workspace root** (e.g. `bazel-out/.../bin/...`), not relative to the
/// service's working_dir. When a `don.toml` lives in a subdirectory of the
/// workspace, the working_dir is below the root, so joining the path with the
/// working_dir points at a non-existent `…/<subdir>/bazel-out/…`. Resolve
/// against the workspace root instead; fall back to the working_dir if no
/// workspace marker is found above it (no worse than the previous behaviour).
fn resolve_bazel_binary_abs_path(working_dir: &Path, rel_path: &str) -> PathBuf {
    let root = crate::build_tool::bazel::find_workspace_root(working_dir)
        .unwrap_or_else(|| working_dir.to_path_buf());
    root.join(rel_path)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::config::{BazelConfig, LogConfig};

    #[test]
    fn resolve_bazel_binary_abs_path_uses_workspace_root_not_working_dir() {
        // workspace_root/ has the WORKSPACE marker; the service's working_dir
        // is a subdirectory (as for a `sandbox/don.toml`).
        let tmp = tempfile::tempdir().unwrap();
        let workspace_root = tmp.path();
        std::fs::write(workspace_root.join("WORKSPACE.bazel"), "").unwrap();
        let working_dir = workspace_root.join("sandbox");
        std::fs::create_dir(&working_dir).unwrap();

        let rel = "bazel-out/k8-fastbuild/bin/sandbox/data_session";
        let resolved = resolve_bazel_binary_abs_path(&working_dir, rel);

        // Must resolve against the workspace root, not the subdir working_dir.
        assert_eq!(resolved, workspace_root.join(rel));
        assert_ne!(resolved, working_dir.join(rel));
    }

    #[test]
    fn resolve_bazel_binary_abs_path_falls_back_to_working_dir_without_marker() {
        // No workspace marker anywhere above: fall back to working_dir.join,
        // which is no worse than the previous behaviour.
        let tmp = tempfile::tempdir().unwrap();
        let working_dir = tmp.path().join("no-marker-here");
        std::fs::create_dir(&working_dir).unwrap();

        let rel = "bazel-out/k8-fastbuild/bin/x";
        let resolved = resolve_bazel_binary_abs_path(&working_dir, rel);
        assert_eq!(resolved, working_dir.join(rel));
    }

    fn bazel_item(name: &str, watch_enabled: bool) -> BatchBuildItem {
        BatchBuildItem {
            name: name.to_string(),
            kind: ProcessKind::Service,
            bazel: Some(BazelConfig {
                target: format!("//services/{name}:{name}"),
                config: None,
                watch: watch_enabled,
            }),
            watch_enabled,
            working_dir: PathBuf::from("."),
            ignore: Vec::new(),
        }
    }

    #[test]
    fn watch_resolution_items_excludes_watch_disabled_build_tools() {
        let items = vec![bazel_item("api", true), bazel_item("worker", false)];

        assert_eq!(
            watch_resolution_items(&items)
                .iter()
                .map(|item| item.name.as_str())
                .collect::<Vec<_>>(),
            vec!["api"],
            "an item with watch disabled must not be queried for watch paths"
        );
    }

    #[tokio::test]
    async fn graph_requery_worker_skips_watch_disabled_build_tools() {
        let output =
            crate::output::OutputManager::new(&[("api", &LogConfig::Stdout)], tokio::io::sink())
                .await
                .unwrap();
        let outcomes = run_graph_requery_worker(
            vec![GraphRequeryRequestItem {
                name: "api".to_string(),
                kind: ProcessKind::Service,
                bazel: Some(BazelConfig {
                    target: "//services/api:api".to_string(),
                    config: None,
                    watch: false,
                }),
                watch_enabled: false,
                working_dir: PathBuf::from("."),
                ignore_patterns: Vec::new(),
                global_watch_ignore: Vec::new(),
            }],
            output.clone_lifecycle_emitter(),
            // No watcher: an item with watching disabled must resolve
            // nothing, so there is nothing to register either.
            None,
            PathBuf::from("."),
        )
        .await;

        assert_eq!(
            outcomes,
            vec![("api".to_string(), RequeryOutcome::Updated)],
            "a watch-disabled item settles without querying the build tool"
        );
    }
}
