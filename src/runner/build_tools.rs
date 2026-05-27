use super::NodeKind;
use super::paths::any_glob_path_changed_since;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

pub(crate) struct RebuildBatchRequest {
    pub(crate) bazel_items: Vec<BazelRebuildItem>,
    pub(crate) turbo_items: Vec<TurboRebuildItem>,
    pub(crate) plain_rebuilds: Vec<String>,
    pub(crate) force: bool,
}

pub(crate) struct BazelRebuildItem {
    pub(crate) name: String,
    pub(crate) target: String,
    pub(crate) working_dir: PathBuf,
}

pub(crate) struct TurboRebuildItem {
    pub(crate) name: String,
    pub(crate) filter: String,
    pub(crate) build_task: String,
    pub(crate) working_dir: PathBuf,
}

pub(crate) struct RebuildBatchOutcome {
    pub(crate) build_succeeded: Vec<String>,
    pub(crate) up_to_date: Vec<String>,
    pub(crate) failed: Vec<(String, String)>,
    pub(crate) plain_rebuilds: Vec<String>,
}

pub(crate) struct GraphRequeryRequestItem {
    pub(crate) name: String,
    pub(crate) bazel: Option<crate::config::BazelConfig>,
    pub(crate) turbo: Option<crate::config::TurboConfig>,
    pub(crate) watch_enabled: bool,
    pub(crate) working_dir: PathBuf,
    pub(crate) ignore_patterns: Vec<String>,
}

pub(crate) struct GraphRequeryOutcomeItem {
    pub(crate) name: String,
    pub(crate) watch_enabled: bool,
    pub(crate) ignore_patterns: Vec<String>,
    pub(crate) result: Result<crate::build_tool::ResolvedBuildInfo, String>,
}

#[derive(Clone)]
pub(crate) struct BatchBuildReplayItem {
    pub(crate) name: String,
    pub(crate) kind: NodeKind,
    pub(crate) source_changed: bool,
    pub(crate) graph_changed: bool,
}

/// Snapshot of a service or task that needs a batch build. Owned — the
/// detached batch-build task runs entirely off this and never touches the
/// live [`super::Runner`] state.
#[derive(Clone)]
pub(crate) struct BatchBuildItem {
    pub(crate) name: String,
    pub(crate) kind: NodeKind,
    pub(crate) bazel: Option<crate::config::BazelConfig>,
    pub(crate) turbo: Option<crate::config::TurboConfig>,
    /// Whether build-tool-resolved source and graph paths should be watched.
    pub(crate) watch_enabled: bool,
    /// Absolute directory where the build tool should be invoked.
    pub(crate) working_dir: PathBuf,
    /// Ignore patterns to carry through to the watch manager.
    pub(crate) ignore: Vec<String>,
}

/// Everything the detached batch-build task produces. Applied to runner
/// state in the main loop when [`super::RunnerInternalCommand::BatchBuildComplete`]
/// arrives — keeps all `&mut self` mutations on the runner task.
pub(crate) struct BatchBuildOutcome {
    /// Per-item resolved watch paths — applied to `resolved_watch_paths` on
    /// the runtime service/task entry.
    pub(crate) resolved_watches: Vec<(String, NodeKind, Vec<String>)>,
    /// Non-fatal warnings (query failures, binary-path cquery failures).
    pub(crate) warnings: Vec<String>,
    /// Names whose batch build succeeded — transition `Building` → `Pending`.
    pub(crate) succeeded: HashSet<String>,
    /// `(name, message)` for items whose batch build failed — transition
    /// `Building` → `Failed` and surface the message as an error event.
    pub(crate) failed: Vec<(String, String)>,
    /// Absolute binary paths from `bazel cquery --output=files`, keyed by
    /// service name. Only populated for bazel services whose build succeeded.
    pub(crate) binary_paths: HashMap<String, String>,
    /// Items whose source and/or build-graph inputs changed while the batch
    /// query/build/cquery pipeline was in flight and need another cycle.
    pub(crate) replay_items: Vec<BatchBuildReplayItem>,
}

pub(crate) async fn run_rebuild_batch_worker(
    request: RebuildBatchRequest,
    emitter: crate::output::LifecycleEmitter,
    bazel_build_mutex: Arc<tokio::sync::Mutex<()>>,
) -> RebuildBatchOutcome {
    let mut build_succeeded: HashSet<String> = HashSet::new();
    let mut up_to_date: HashSet<String> = HashSet::new();
    let mut failed: Vec<(String, String)> = Vec::new();

    let mut bazel_by_dir: HashMap<PathBuf, Vec<BazelRebuildItem>> = HashMap::new();
    for item in request.bazel_items {
        bazel_by_dir
            .entry(bazel_graph_requery_group_dir(&item.working_dir))
            .or_default()
            .push(item);
    }

    if !bazel_by_dir.is_empty() {
        let _guard = bazel_build_mutex.lock().await;

        for (working_dir, items) in bazel_by_dir {
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
                    .check_up_to_date(&targets, &working_dir)
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

    let mut turbo_by_group: HashMap<(PathBuf, String), Vec<TurboRebuildItem>> = HashMap::new();
    for item in request.turbo_items {
        turbo_by_group
            .entry((item.working_dir.clone(), item.build_task.clone()))
            .or_default()
            .push(item);
    }

    for ((working_dir, build_task), items) in turbo_by_group {
        let filters: Vec<String> = items.iter().map(|item| item.filter.clone()).collect();
        let filter_to_names: HashMap<String, Vec<String>> = {
            let mut names: HashMap<String, Vec<String>> = HashMap::new();
            for item in &items {
                names
                    .entry(item.filter.clone())
                    .or_default()
                    .push(item.name.clone());
            }
            names
        };
        let count = filters.len();
        emitter.turbo_event(&format!(
            "rebuilding '{build_task}' for {count} package{}...",
            if count == 1 { "" } else { "s" }
        ));

        let line_emitter = emitter.clone();
        let resolver = crate::build_tool::turbo::TurboResolver::new(&build_task, None);
        match resolver
            .build_packages(
                &build_task,
                &filters,
                &working_dir,
                move |line| {
                    line_emitter.turbo_event(line);
                },
                Some(&emitter),
            )
            .await
        {
            Ok(result) => {
                for filter in &result.succeeded {
                    if let Some(names) = filter_to_names.get(filter) {
                        for name in names {
                            build_succeeded.insert(name.clone());
                        }
                    }
                }
                for (filter, message) in &result.failed {
                    if let Some(names) = filter_to_names.get(filter) {
                        for name in names {
                            failed.push((name.clone(), format!("turbo build failed: {message}")));
                        }
                    }
                }
            }
            Err(e) => {
                for item in &items {
                    failed.push((item.name.clone(), format!("turbo build error: {e}")));
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

fn watch_resolution_items(
    items: &[BatchBuildItem],
) -> (Vec<&BatchBuildItem>, Vec<&BatchBuildItem>) {
    let bazel_items: Vec<&BatchBuildItem> = items
        .iter()
        .filter(|i| i.watch_enabled && i.bazel.is_some())
        .collect();
    let turbo_items: Vec<&BatchBuildItem> = items
        .iter()
        .filter(|i| i.watch_enabled && i.turbo.is_some())
        .collect();

    (bazel_items, turbo_items)
}

pub(crate) async fn send_watch_update(
    tx: &mpsc::Sender<crate::watch::WatchUpdate>,
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
        .await
        .is_ok()
    {
        let _ = applied_rx.await;
    }
}

pub(crate) async fn run_graph_requery_worker(
    items: Vec<GraphRequeryRequestItem>,
    emitter: crate::output::LifecycleEmitter,
) -> Vec<GraphRequeryOutcomeItem> {
    use crate::build_tool::BuildGraphResolver;

    let mut outcomes: Vec<Option<GraphRequeryOutcomeItem>> =
        std::iter::repeat_with(|| None).take(items.len()).collect();
    let mut bazel_groups: HashMap<PathBuf, Vec<(usize, GraphRequeryRequestItem)>> = HashMap::new();
    let mut other_items: Vec<(usize, GraphRequeryRequestItem)> = Vec::new();

    for (idx, item) in items.into_iter().enumerate() {
        if !item.watch_enabled {
            outcomes[idx] = Some(GraphRequeryOutcomeItem {
                name: item.name,
                watch_enabled: false,
                ignore_patterns: item.ignore_patterns,
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
        } else {
            other_items.push((idx, item));
        }
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
                        result: Err(message.clone()),
                    });
                }
            }
        }
    }

    for (idx, item) in other_items {
        let result = if let Some(ref turbo) = item.turbo {
            let resolver =
                crate::build_tool::turbo::TurboResolver::new(&turbo.task, turbo.filter.as_deref());
            resolver
                .resolve(&turbo.task, &item.working_dir)
                .await
                .map_err(|e| e.to_string())
        } else {
            continue;
        };
        outcomes[idx] = Some(GraphRequeryOutcomeItem {
            name: item.name,
            watch_enabled: item.watch_enabled,
            ignore_patterns: item.ignore_patterns,
            result,
        });
    }

    outcomes.into_iter().flatten().collect()
}

/// Run the full startup-phase batch build: optional watch resolution → batch
/// build → bazel binary-path cquery. Pure off-task function that takes owned
/// inputs and returns an [`BatchBuildOutcome`] the main loop applies.
///
/// Sends [`crate::watch::WatchUpdate`]s directly to the watch manager as
/// they resolve so file watching is live before the builds complete.
pub(crate) async fn run_batch_build_chain(
    items: Vec<BatchBuildItem>,
    base_dir: PathBuf,
    emitter: crate::output::LifecycleEmitter,
    watch_update_tx: Option<mpsc::Sender<crate::watch::WatchUpdate>>,
    global_watch_ignore: Vec<String>,
) -> BatchBuildOutcome {
    let scan_since = SystemTime::now();
    let mut outcome = BatchBuildOutcome {
        resolved_watches: Vec::new(),
        warnings: Vec::new(),
        succeeded: HashSet::new(),
        failed: Vec::new(),
        binary_paths: HashMap::new(),
        replay_items: Vec::new(),
    };
    if items.is_empty() {
        return outcome;
    }

    // Step 1: resolve watch paths with one query per build-tool working set.
    //
    // Previously we ran N parallel `bazel query` / `turbo run --dry-run`
    // subprocesses, one per item. Bazel's server has a workspace-wide
    // analysis lock, so those parallel queries mostly serialised inside
    // bazel anyway — and each process startup cost stacked. Now we issue
    // one `deps(T1 + ... + Tn) --output=xml` per Bazel workspace and
    // DFS-walk per target client-side to keep accurate per-service attribution.
    // Turbo uses one dry-run per `(working_dir, task)` group; each group still
    // shares union watch attribution within that Turbo invocation.

    // Partition items by tool. Each item keeps its own ignore patterns
    // (configured per service/task) but gets the same tier-1/tier-2 globs.
    let (bazel_items, turbo_items) = watch_resolution_items(&items);

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

    if !turbo_items.is_empty() {
        let mut turbo_by_group: HashMap<(PathBuf, String), Vec<&BatchBuildItem>> = HashMap::new();
        for item in &turbo_items {
            let Some(turbo) = item.turbo.as_ref() else {
                continue;
            };
            turbo_by_group
                .entry((item.working_dir.clone(), turbo.task.clone()))
                .or_default()
                .push(*item);
        }

        for ((working_dir, task), items_for_group) in turbo_by_group {
            let filters: Vec<String> = items_for_group
                .iter()
                .filter_map(|i| i.turbo.as_ref().and_then(|t| t.filter.clone()))
                .collect();
            let resolver = crate::build_tool::turbo::TurboResolver::new(&task, None);
            match resolver.resolve_union(&filters, &working_dir).await {
                Ok(info) => {
                    emitter.turbo_event(&format!(
                        "resolved {} watch path{} across {} filter{}",
                        info.watch_paths.len(),
                        if info.watch_paths.len() == 1 { "" } else { "s" },
                        filters.len(),
                        if filters.len() == 1 { "" } else { "s" },
                    ));
                    for item in items_for_group {
                        resolved_info_by_item.insert(item.name.clone(), info.clone());
                    }
                }
                Err(e) => {
                    outcome.warnings.push(format!(
                        "turbo query failed in {} for task '{}': {e}",
                        working_dir.display(),
                        task
                    ));
                }
            }
        }
    }

    // Attribute resolved watch paths to each item. Bazel items get their
    // own per-target result (computed by DFS from the unified XML graph);
    // turbo items still share the union. Each item emits its own
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
                NodeKind::Service => crate::watch::WatchItemKind::Service,
                NodeKind::Task => crate::watch::WatchItemKind::Task,
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
        outcome
            .resolved_watches
            .push((item.name.clone(), item.kind, info.watch_paths.clone()));
    }

    // Step 2: batch builds. Bazel/Turbo groups run concurrently, but each
    // group stays inside the working directory its labels/filters belong to.
    let mut bazel_by_dir: HashMap<PathBuf, Vec<(BatchBuildItem, String)>> = HashMap::new();
    let mut turbo_by_group: HashMap<(PathBuf, String), Vec<(BatchBuildItem, String)>> =
        HashMap::new();

    for item in &items {
        if let Some(ref bazel) = item.bazel {
            bazel_by_dir
                .entry(bazel_graph_requery_group_dir(&item.working_dir))
                .or_default()
                .push((item.clone(), bazel.target.clone()));
        } else if let Some(ref turbo) = item.turbo {
            let build_task = turbo
                .build_task
                .clone()
                .unwrap_or_else(|| "build".to_string());
            if !build_task.is_empty() {
                if let Some(ref filter) = turbo.filter {
                    turbo_by_group
                        .entry((item.working_dir.clone(), build_task))
                        .or_default()
                        .push((item.clone(), filter.clone()));
                } else {
                    outcome.warnings.push(format!(
                        "{}: turbo.filter is required for batch builds — skipping batch build",
                        item.name
                    ));
                }
            }
        }
    }

    let mut build_set: JoinSet<crate::build_tool::BatchBuildResult> = JoinSet::new();

    for (working_dir, bazel_items) in bazel_by_dir {
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

    for ((working_dir, build_task), items_for_task) in turbo_by_group {
        let filters: Vec<String> = items_for_task.iter().map(|(_, f)| f.clone()).collect();
        let filter_to_names: HashMap<String, Vec<String>> = {
            let mut m: HashMap<String, Vec<String>> = HashMap::new();
            for (item, filter) in &items_for_task {
                m.entry(filter.clone()).or_default().push(item.name.clone());
            }
            m
        };
        let count = filters.len();
        let em = emitter.clone();
        let em_spawn = emitter.clone();
        let bt = build_task.clone();
        emitter.turbo_event(&format!(
            "running '{build_task}' for {count} package{}...",
            if count == 1 { "" } else { "s" }
        ));
        build_set.spawn(async move {
            let resolver = crate::build_tool::turbo::TurboResolver::new(&bt, None);
            let result = resolver
                .build_packages(
                    &bt,
                    &filters,
                    &working_dir,
                    move |line| {
                        em.turbo_event(line);
                    },
                    Some(&em_spawn),
                )
                .await;
            match result {
                Ok(batch) => {
                    let mut succeeded = Vec::new();
                    for filter in &batch.succeeded {
                        if let Some(names) = filter_to_names.get(filter) {
                            succeeded.extend(names.clone());
                        }
                    }
                    let mut failed = Vec::new();
                    for (filter, msg) in &batch.failed {
                        if let Some(names) = filter_to_names.get(filter) {
                            for n in names {
                                failed.push((n.clone(), msg.clone()));
                            }
                        }
                    }
                    crate::build_tool::BatchBuildResult { succeeded, failed }
                }
                // See bazel branch above — convert resolver errors to
                // per-item failures so services don't get stuck in `Building`.
                Err(e) => {
                    let msg = format!("turbo build error: {e}");
                    let failed: Vec<(String, String)> = filter_to_names
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
                    outcome.succeeded.insert(name);
                }
                for (name, msg) in batch.failed {
                    outcome.failed.push((name, msg));
                }
            }
            Err(e) => outcome
                .warnings
                .push(format!("batch build task panicked: {e}")),
        }
    }

    let built_count = outcome.succeeded.len();
    if built_count > 0 {
        emitter.lifecycle_event(&format!(
            "batch build complete: {built_count} item{} built",
            if built_count == 1 { "" } else { "s" }
        ));
    }

    // Step 3: resolve every succeeded bazel service's built-binary path,
    // grouped by workspace. Lets the runner spawn the artifact directly
    // instead of via `bazel run`. Tasks and turbo services don't need this.
    let bazel_services_to_resolve: Vec<&BatchBuildItem> = items
        .iter()
        .filter(|i| {
            i.kind == NodeKind::Service && i.bazel.is_some() && outcome.succeeded.contains(&i.name)
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
                                outcome.binary_paths.insert(item.name.clone(), path_str);
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

    for item in &items {
        if !item.watch_enabled || !outcome.succeeded.contains(&item.name) {
            continue;
        }
        let Some(info) = resolved_info_by_item.get(&item.name) else {
            continue;
        };
        let source_changed =
            any_glob_path_changed_since(&base_dir, &info.watch_paths, &item.ignore, scan_since);
        let graph_changed =
            any_glob_path_changed_since(&base_dir, &info.graph_definition_globs, &[], scan_since);
        if source_changed || graph_changed {
            outcome.replay_items.push(BatchBuildReplayItem {
                name: item.name.clone(),
                kind: item.kind,
                source_changed,
                graph_changed,
            });
        }
    }

    outcome
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
    use crate::config::{BazelConfig, LogConfig, TurboConfig};

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
            kind: NodeKind::Service,
            bazel: Some(BazelConfig {
                target: format!("//services/{name}:{name}"),
                watch: watch_enabled,
            }),
            turbo: None,
            watch_enabled,
            working_dir: PathBuf::from("."),
            ignore: Vec::new(),
        }
    }

    fn turbo_item(name: &str, watch_enabled: bool) -> BatchBuildItem {
        BatchBuildItem {
            name: name.to_string(),
            kind: NodeKind::Service,
            bazel: None,
            turbo: Some(TurboConfig {
                task: "dev".to_string(),
                watch: watch_enabled,
                build_task: None,
                filter: Some(format!("@test/{name}")),
            }),
            watch_enabled,
            working_dir: PathBuf::from("."),
            ignore: Vec::new(),
        }
    }

    #[test]
    fn watch_resolution_items_excludes_watch_disabled_build_tools() {
        let items = vec![
            bazel_item("api", true),
            bazel_item("worker", false),
            turbo_item("web", true),
            turbo_item("docs", false),
        ];

        let (bazel_items, turbo_items) = watch_resolution_items(&items);

        assert_eq!(
            bazel_items
                .iter()
                .map(|item| item.name.as_str())
                .collect::<Vec<_>>(),
            vec!["api"]
        );
        assert_eq!(
            turbo_items
                .iter()
                .map(|item| item.name.as_str())
                .collect::<Vec<_>>(),
            vec!["web"]
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
                bazel: Some(BazelConfig {
                    target: "//services/api:api".to_string(),
                    watch: false,
                }),
                turbo: None,
                watch_enabled: false,
                working_dir: PathBuf::from("."),
                ignore_patterns: Vec::new(),
            }],
            output.clone_lifecycle_emitter(),
        )
        .await;

        assert_eq!(outcomes.len(), 1);
        assert!(!outcomes[0].watch_enabled);
        let info = outcomes[0].result.as_ref().unwrap();
        assert!(info.watch_paths.is_empty());
        assert!(info.graph_definition_globs.is_empty());
    }
}
