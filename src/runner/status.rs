use super::{
    ItemStatus, Runner, TuiSnapshot, VerboseInfo, WatchDir, WatchReport, WatchReportItem,
};
use tokio::sync::oneshot;

impl Runner {
    /// Build the seed snapshot for a remote `don tui` frontend: the active
    /// item set (already filtered to this daemon's profile), daemon-wide flags,
    /// and current per-item state. The client merges this with task param
    /// schemas it loads from the local `don.toml`.
    pub(in crate::runner) async fn build_snapshot(&self) -> TuiSnapshot {
        let service_names: Vec<String> = self.services.keys().cloned().collect();
        let task_names: Vec<String> = self.tasks.keys().cloned().collect();

        let uses_bazel = self
            .services
            .values()
            .any(|rs| matches!(rs.resolved.kind, Some(crate::config::ServiceKind::Bazel(_))))
            || self.tasks.values().any(|rt| rt.config.bazel.is_some());
        let uses_turbo = self
            .services
            .values()
            .any(|rs| matches!(rs.resolved.kind, Some(crate::config::ServiceKind::Turbo(_))))
            || self.tasks.values().any(|rt| rt.config.turbo.is_some());
        let build_tool_names: Vec<String> = [
            uses_bazel.then(|| "bazel".to_string()),
            uses_turbo.then(|| "turbo".to_string()),
        ]
        .into_iter()
        .flatten()
        .collect();

        let mut hidden_names = Vec::new();
        let mut auto_filter_on_failure_names = Vec::new();
        let default_auto_filter = self.config.auto_filter_on_failure;
        for (name, rs) in &self.services {
            if self.config.services.get(name).is_some_and(|s| s.hidden) {
                hidden_names.push(name.clone());
            }
            if rs
                .resolved
                .auto_filter_on_failure
                .unwrap_or(default_auto_filter)
            {
                auto_filter_on_failure_names.push(name.clone());
            }
        }
        for (name, rt) in &self.tasks {
            if rt.config.hidden {
                hidden_names.push(name.clone());
            }
            if rt
                .config
                .auto_filter_on_failure
                .unwrap_or(default_auto_filter)
            {
                auto_filter_on_failure_names.push(name.clone());
            }
        }

        TuiSnapshot {
            verbose: self.output_manager.verbosity_control().is_enabled(),
            service_names,
            task_names,
            build_tool_names,
            hidden_names,
            auto_filter_on_failure_names,
            statuses: self.collect_status(false, None).await,
        }
    }

    pub(in crate::runner) async fn fetch_watch_snapshot(
        &self,
    ) -> Option<crate::watch::WatchSnapshot> {
        let tx = self.watch_query_tx.as_ref()?;
        let (reply_tx, reply_rx) = oneshot::channel();
        if tx
            .send(crate::watch::WatchQuery { reply: reply_tx })
            .await
            .is_err()
        {
            return None;
        }
        tokio::time::timeout(std::time::Duration::from_millis(250), reply_rx)
            .await
            .ok()
            .and_then(Result::ok)
    }

    /// Build a global [`WatchReport`] of every active inotify registration and
    /// per-item watch pattern. Returns `None` when no watches are active (no
    /// service/task declared watch patterns, or the watcher is unreachable).
    pub(in crate::runner) async fn collect_watch_report(&self) -> Option<WatchReport> {
        let snapshot = self.fetch_watch_snapshot().await?;

        let mut directories: Vec<WatchDir> = snapshot
            .registered_dirs
            .iter()
            .map(|(path, mode)| WatchDir {
                path: path.to_string_lossy().into_owned(),
                mode: (*mode).to_string(),
            })
            .collect();
        directories.sort_by(|a, b| a.path.cmp(&b.path));

        let mut items: Vec<WatchReportItem> = snapshot
            .items
            .iter()
            .map(|(name, item)| WatchReportItem {
                name: name.clone(),
                kind: item.kind.to_string(),
                state: item.state.to_string(),
                stale: item.stale,
                debounce_ms: item.debounce_ms,
                patterns: item.patterns.clone(),
                ignore_patterns: item.ignore_patterns.clone(),
                last_error: item.last_error.clone(),
            })
            .collect();
        items.sort_by(|a, b| a.name.cmp(&b.name));

        Some(WatchReport {
            directories,
            items,
            global_ignore: snapshot.global_ignore.clone(),
            notify_error_count: snapshot.notify_error_count,
            runner_event_lag_count: snapshot.runner_event_lag_count,
            last_notify_error: snapshot.last_notify_error.clone(),
        })
    }

    /// Collect status of all items.
    ///
    /// When `detail_name` is `Some`, only that single service/task is returned
    /// and its fully-resolved `watch` path list is included. The all-items view
    /// (`detail_name == None`) deliberately omits the path list — for a
    /// build-tool stack it can be hundreds of resolved paths per service, so the
    /// default verbose view reports only the count and callers drill in by name.
    pub(in crate::runner) async fn collect_status(
        &self,
        verbose: bool,
        detail_name: Option<&str>,
    ) -> Vec<ItemStatus> {
        let mut statuses = Vec::new();
        let watch_snapshot = if verbose {
            self.fetch_watch_snapshot().await
        } else {
            None
        };
        let watch_snapshot_available = watch_snapshot.is_some();
        for (name, rs) in &self.services {
            if detail_name.is_some_and(|want| want != name) {
                continue;
            }
            let verbose_info = if verbose {
                let resolved = &rs.resolved;
                let ready = resolved.ready.as_ref().map(|r| {
                    if let Some(ref tcp) = r.tcp {
                        format!("tcp {tcp}")
                    } else if let Some(ref http) = r.http {
                        format!("http {http}")
                    } else if let Some(ref exec) = r.exec {
                        format!("{} {}", exec.cmd, exec.args.join(" "))
                    } else {
                        "none".to_string()
                    }
                });
                let cmd = resolved.run_cmd().map(|r| {
                    if r.args.is_empty() {
                        r.cmd.clone()
                    } else {
                        format!("{} {}", r.cmd, r.args.join(" "))
                    }
                });
                // Use resolved build tool watch paths if explicit ones are empty.
                let watch = if resolved.watch.is_empty() {
                    rs.resolved_watch_paths.clone()
                } else {
                    resolved.watch.clone()
                };
                let watch_item = watch_snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.items.get(name));
                let mut watch_notes = Vec::new();
                if let Some(snapshot) = &watch_snapshot {
                    if snapshot.notify_error_count > 0
                        && let Some(ref last) = snapshot.last_notify_error
                    {
                        watch_notes.push(format!(
                            "notify errors={} last={last}",
                            snapshot.notify_error_count
                        ));
                    }
                    if snapshot.runner_event_lag_count > 0 {
                        watch_notes.push(format!(
                            "runner-event lag count={}",
                            snapshot.runner_event_lag_count
                        ));
                    }
                }
                if let Some(item) = watch_item {
                    if let Some(ref last_error) = item.last_error {
                        watch_notes.push(last_error.clone());
                    }
                } else if !watch.is_empty() && watch_snapshot_available {
                    watch_notes.push("watch item missing from watch manager".to_string());
                } else if !watch.is_empty() {
                    watch_notes.push("watch manager unavailable".to_string());
                }
                Some(VerboseInfo {
                    depends_on: resolved.depends_on.clone(),
                    watch_count: watch.len(),
                    // Full path list only on a single-item drill-in; see
                    // `collect_status` doc for why the all-items view omits it.
                    watch: if detail_name.is_some() {
                        watch
                    } else {
                        Vec::new()
                    },
                    proxy: resolved
                        .proxy
                        .iter()
                        .map(|p| match &p.mode {
                            crate::config::ProxyMode::Env(name) => {
                                format!("{} (env={name})", p.listen)
                            }
                            crate::config::ProxyMode::Listenfd => {
                                format!("{} (listenfd)", p.listen)
                            }
                            crate::config::ProxyMode::Forward(target) => {
                                format!("{} → {target}", p.listen)
                            }
                        })
                        .collect(),
                    proxy_active_connections: rs
                        .proxy
                        .as_ref()
                        .and_then(|proxy| proxy.active_forward_connections()),
                    bazel_target: resolved.bazel_config().map(|b| b.target.clone()),
                    turbo_task: resolved.turbo_config().map(|t| t.task.clone()),
                    ready,
                    cmd,
                    watch_state: watch_item.map(|item| {
                        format!(
                            "{} state={} stale={} debounce={}ms",
                            item.kind, item.state, item.stale, item.debounce_ms
                        )
                    }),
                    watch_notes,
                })
            } else {
                None
            };
            statuses.push(ItemStatus::Service {
                name: name.clone(),
                state: rs.state(),
                verbose: verbose_info,
            });
        }
        for (name, rt) in &self.tasks {
            if detail_name.is_some_and(|want| want != name) {
                continue;
            }
            let verbose_info = if verbose {
                let task = &rt.config;
                let cmd_str = if task.args.is_empty() {
                    task.cmd.clone()
                } else {
                    format!("{} {}", task.cmd, task.args.join(" "))
                };
                let watch = if task.watch.is_empty() {
                    rt.resolved_watch_paths.clone()
                } else {
                    task.watch.clone()
                };
                let watch_item = watch_snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.items.get(name));
                let mut watch_notes = Vec::new();
                if let Some(snapshot) = &watch_snapshot {
                    if snapshot.notify_error_count > 0
                        && let Some(ref last) = snapshot.last_notify_error
                    {
                        watch_notes.push(format!(
                            "notify errors={} last={last}",
                            snapshot.notify_error_count
                        ));
                    }
                    if snapshot.runner_event_lag_count > 0 {
                        watch_notes.push(format!(
                            "runner-event lag count={}",
                            snapshot.runner_event_lag_count
                        ));
                    }
                }
                if let Some(item) = watch_item {
                    if let Some(ref last_error) = item.last_error {
                        watch_notes.push(last_error.clone());
                    }
                } else if !watch.is_empty() && watch_snapshot_available {
                    watch_notes.push("watch item missing from watch manager".to_string());
                } else if !watch.is_empty() {
                    watch_notes.push("watch manager unavailable".to_string());
                }
                Some(VerboseInfo {
                    depends_on: task.depends_on.clone(),
                    watch_count: watch.len(),
                    // Full path list only on a single-item drill-in; see
                    // `collect_status` doc for why the all-items view omits it.
                    watch: if detail_name.is_some() {
                        watch
                    } else {
                        Vec::new()
                    },
                    proxy: Vec::new(),
                    proxy_active_connections: None,
                    bazel_target: task.bazel.as_ref().map(|b| b.target.clone()),
                    turbo_task: task.turbo.as_ref().map(|t| t.task.clone()),
                    ready: None,
                    cmd: Some(cmd_str),
                    watch_state: watch_item.map(|item| {
                        format!(
                            "{} state={} stale={} debounce={}ms",
                            item.kind, item.state, item.stale, item.debounce_ms
                        )
                    }),
                    watch_notes,
                })
            } else {
                None
            };
            statuses.push(ItemStatus::Task {
                name: name.clone(),
                state: rt.state(),
                last_run: rt.last_run.clone(),
                verbose: verbose_info,
            });
        }
        statuses
    }
}
