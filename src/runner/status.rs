use super::{ProcessStatus, Runner, VerboseInfo};

impl Runner {
    pub(in crate::runner) async fn fetch_watch_snapshot(
        &self,
    ) -> Option<crate::watch::WatchSnapshot> {
        self.watch.as_ref()?.snapshot().await
    }

    /// The cheap, allocation-only projection of every process's state.
    ///
    /// Synchronous by construction: it touches nothing but the runner's own
    /// maps. That is what lets it be republished on every state transition and
    /// read straight out of [`StateReader`] without a command round trip.
    ///
    /// [`StateReader`]: super::StateReader
    pub(in crate::runner) fn status_projection(
        &self,
        detail_name: Option<&str>,
    ) -> Vec<ProcessStatus> {
        let mut statuses = Vec::new();
        for name in self.services.keys() {
            if detail_name.is_some_and(|want| want != name) {
                continue;
            }
            statuses.push(ProcessStatus::Service {
                name: name.clone(),
                state: self.service_state(name),
                failed_dependencies: self.failed_dependencies(name),
                // Absent so the merge in `publish_processes` carries forward
                // what custody reported; the fold does not know pids.
                runtime: None,
                verbose: None,
            });
        }
        for name in self.tasks.keys() {
            if detail_name.is_some_and(|want| want != name) {
                continue;
            }
            statuses.push(ProcessStatus::Task {
                name: name.clone(),
                state: self.task_state(name),
                failed_dependencies: self.failed_dependencies(name),
                last_run: self.task_last_run(name),
                pid: None,
                verbose: None,
            });
        }
        statuses
    }

    /// Collect status of all processes.
    ///
    /// When `detail_name` is `Some`, only that single service/task is returned
    /// and its fully-resolved `watch` path list is included. The all-processes view
    /// (`detail_name == None`) deliberately omits the path list — for a
    /// build-tool stack it can be hundreds of resolved paths per service, so the
    /// default verbose view reports only the count and callers drill in by name.
    ///
    /// The non-verbose answer is [`status_projection`](Self::status_projection),
    /// so what a client reads from the state store and what it reads from this
    /// command cannot drift.
    pub(in crate::runner) async fn collect_status(
        &self,
        verbose: bool,
        detail_name: Option<&str>,
    ) -> Vec<ProcessStatus> {
        if !verbose {
            return self.status_projection(detail_name);
        }
        let mut statuses = Vec::new();
        let watch_snapshot = self.fetch_watch_snapshot().await;
        let watch_snapshot_available = watch_snapshot.is_some();
        for (name, rs) in &self.services {
            if detail_name.is_some_and(|want| want != name) {
                continue;
            }
            let live = self.service_runtime(name).is_some();
            let verbose_info = {
                let resolved = &rs.resolved;
                let ready = self.endpoint_ready_check(name, resolved).as_ref().map(|r| {
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
                let watch_item = watch_snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.items.get(name));
                // With no explicit `watch`, the patterns came from the build
                // tool — and went straight to the watcher, which is the one
                // thing that knows what is actually registered. Ask it rather
                // than keeping a copy that can only be less true.
                let watch = if resolved.watch.is_empty() {
                    watch_item
                        .map(|item| item.patterns.clone())
                        .unwrap_or_default()
                } else {
                    resolved.watch.clone()
                };
                let mut watch_notes = Vec::new();
                if let Some(snapshot) = &watch_snapshot
                    && snapshot.notify_error_count > 0
                    && let Some(ref last) = snapshot.last_notify_error
                {
                    watch_notes.push(format!(
                        "notify errors={} last={last}",
                        snapshot.notify_error_count
                    ));
                }
                if let Some(process) = watch_item {
                    if let Some(ref last_error) = process.last_error {
                        watch_notes.push(last_error.clone());
                    }
                } else if !watch.is_empty() && watch_snapshot_available {
                    watch_notes.push("watch process missing from watch manager".to_string());
                } else if !watch.is_empty() {
                    watch_notes.push("watch manager unavailable".to_string());
                }
                Some(VerboseInfo {
                    depends_on: resolved.depends_on.clone(),
                    // Services have no params; only tasks are run with values.
                    params: Vec::new(),
                    watch_count: watch.len(),
                    // Full path list only on a single-process drill-in; see
                    // `collect_status` doc for why the all-processes view omits it.
                    watch: if detail_name.is_some() {
                        watch
                    } else {
                        Vec::new()
                    },
                    proxy: {
                        // Bound addresses come from the endpoint projection;
                        // before bind (or for a service with none) fall back
                        // to what the config asked for.
                        let bound = self
                            .endpoints
                            .snapshot()
                            .get(name)
                            .map(|endpoints| endpoints.proxy.clone())
                            .unwrap_or_default();
                        if bound.is_empty() {
                            resolved
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
                                .collect()
                        } else {
                            let mut entries = crate::proxy::descriptions_for(&bound);
                            // A failed service's listeners still exist, so
                            // say why they are closing connections instead of
                            // leaving the address looking healthy.
                            if self.service_state(name).refuses_connections(live) {
                                for entry in &mut entries {
                                    entry.push_str(" — refusing (service failed)");
                                }
                            }
                            entries
                        }
                    },
                    docker_ports: self
                        .service_runtime(name)
                        .filter(|runtime| runtime.docker)
                        .map(|runtime| runtime.docker_ports)
                        .unwrap_or_default(),
                    proxy_active_connections: self
                        .proxy_connection_counters
                        .get(name)
                        .map(|count| count.load(std::sync::atomic::Ordering::Relaxed)),
                    bazel_target: resolved.bazel_config().map(|b| b.target.clone()),
                    ready,
                    cmd,
                    watch_state: watch_item.map(|process| {
                        format!(
                            "{} state={} debounce={}ms",
                            process.kind, process.state, process.debounce_ms
                        )
                    }),
                    watch_notes,
                })
            };
            statuses.push(ProcessStatus::Service {
                name: name.clone(),
                state: self.service_state(name),
                failed_dependencies: self.failed_dependencies(name),
                runtime: self.service_runtime(name),
                verbose: verbose_info,
            });
        }
        for (name, rt) in &self.tasks {
            if detail_name.is_some_and(|want| want != name) {
                continue;
            }
            let verbose_info = {
                let task = &rt.config;
                // A `bazel.target` task has no command of its own until the
                // build resolves one, so show the target it is defined by.
                let base_cmd = match (&task.cmd, &task.bazel) {
                    (Some(cmd), _) => cmd.clone(),
                    (None, Some(bazel)) => format!("bazel run {}", bazel.target),
                    (None, None) => String::new(),
                };
                let cmd_str = if task.args.is_empty() {
                    base_cmd
                } else {
                    format!("{} {}", base_cmd, task.args.join(" "))
                };
                let watch_item = watch_snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.items.get(name));
                // See the service branch: the watcher owns the resolved set.
                let watch = if task.watch.is_empty() {
                    watch_item
                        .map(|item| item.patterns.clone())
                        .unwrap_or_default()
                } else {
                    task.watch.clone()
                };
                let mut watch_notes = Vec::new();
                if let Some(snapshot) = &watch_snapshot
                    && snapshot.notify_error_count > 0
                    && let Some(ref last) = snapshot.last_notify_error
                {
                    watch_notes.push(format!(
                        "notify errors={} last={last}",
                        snapshot.notify_error_count
                    ));
                }
                if let Some(process) = watch_item {
                    if let Some(ref last_error) = process.last_error {
                        watch_notes.push(last_error.clone());
                    }
                } else if !watch.is_empty() && watch_snapshot_available {
                    watch_notes.push("watch process missing from watch manager".to_string());
                } else if !watch.is_empty() {
                    watch_notes.push("watch manager unavailable".to_string());
                }
                Some(VerboseInfo {
                    depends_on: task.depends_on.clone(),
                    watch_count: watch.len(),
                    // Full path list only on a single-process drill-in; see
                    // `collect_status` doc for why the all-processes view omits it.
                    watch: if detail_name.is_some() {
                        watch
                    } else {
                        Vec::new()
                    },
                    proxy: Vec::new(),
                    docker_ports: Vec::new(),
                    proxy_active_connections: None,
                    bazel_target: task.bazel.as_ref().map(|b| b.target.clone()),
                    params: task
                        .params
                        .iter()
                        .map(super::ParamInfo::from_config)
                        .collect(),
                    ready: None,
                    cmd: Some(cmd_str),
                    watch_state: watch_item.map(|process| {
                        format!(
                            "{} state={} debounce={}ms",
                            process.kind, process.state, process.debounce_ms
                        )
                    }),
                    watch_notes,
                })
            };
            statuses.push(ProcessStatus::Task {
                name: name.clone(),
                state: self.task_state(name),
                failed_dependencies: self.failed_dependencies(name),
                last_run: self.task_last_run(name),
                pid: self.task_pid(name),
                verbose: verbose_info,
            });
        }
        statuses
    }
}
