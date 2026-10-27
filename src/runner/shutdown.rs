//! Teardown, from the root's side.
//!
//! There is no stop order here. Every supervisor sees the same shutdown signal
//! and each waits for *its own* dependents to be holding nothing before ending
//! what it holds, so reverse-dependency order emerges from the graph rather
//! than being walked by anyone. See [`crate::process::await_dependents_gone`].
//!
//! What the root still owns is the part no single process can answer — is the
//! whole stack down? — plus the lifetimes it created: the build manager, the
//! update checker, the supervisor tasks themselves.

use super::Runner;
use crate::signals::force_shutdown_requested;

impl Runner {
    /// Initiate graceful shutdown of all services.
    pub(in crate::runner) async fn initiate_shutdown(&mut self) {
        // No gate revocation is needed: every supervisor watches the same
        // shutdown flag this sets, and refuses to self-start once it is set.

        if self.shutting_down {
            return;
        }
        self.shutting_down = true;

        // Read *before* raising the flag. Tearing down is itself work, so a
        // supervisor marks itself busy the moment it starts — asking afterwards
        // would report every process as "cancelled by shutdown".
        let interrupted: Vec<String> = self
            .service_starts
            .registry()
            .busy_names()
            .chain(self.task_supervisors.registry().busy_names())
            .map(str::to_string)
            .collect();

        let _ = self.event_tx.send(super::RunnerEvent::ShutdownStarted);
        let _ = self.shutdown_flag_tx.send(true);
        self.output_manager
            .lifecycle_event("shutting down gracefully... (Ctrl+C again to force)");

        // `ShutdownStarted` above is what withdraws this project from the
        // daemon — the binary subscribes and does it, detached. Deliberately
        // broadcast before any of the slow teardown below: the user is
        // waiting on Ctrl+C, and stopping services takes long enough that the
        // withdrawal normally lands well before we exit.

        // End the build manager. It aborts every in-flight batch — the first
        // build as much as a rebuild or a re-query — and awaits them, so no
        // `LifecycleEmitter`/`SinkHandle` clone outlives shutdown and no new
        // batch can spawn mid-teardown. The `Child` inside has
        // `kill_on_drop(true)`, so dropping the aborted future SIGKILLs the
        // bazel client; the bounded joins inside guarantee the drop has run
        // before this returns.
        let (batcher_done_tx, batcher_done_rx) = tokio::sync::oneshot::channel();
        if self
            .batcher_tx
            .send(super::build_batcher::BatchRequest::Shutdown {
                done: batcher_done_tx,
            })
            .is_ok()
        {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(11), batcher_done_rx).await;
        }
        if let Some(handle) = self.batcher_handle.take() {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
        }
        if let Some(handle) = self.update_check_handle.take() {
            handle.abort();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
        }

        // Whatever was mid-flight when the signal landed. Its supervisor will
        // abandon it; this only says so.
        for name in interrupted {
            self.output_manager
                .service_event(&name, "cancelled by shutdown");
        }
        // Neither supervisor set may be ended here: both own processes, and
        // both are in the middle of ending them. They are aborted at the
        // teardown tail, once every one of them reports holding nothing —
        // aborting earlier would drop held handles (`kill_on_drop`) before the
        // graceful path decided anything.

        self.drain_late_worker_results().await;

        // The API server is NOT told to stop here, deliberately. Streaming
        // responses (log/event followers) end on that signal, and firing it
        // before the wave below would cut every attached client off from the
        // entire teardown narration — the stopping/stopped lines they are
        // watching for. The flip happens at the end of teardown, after the
        // output flush, in `run`'s tail; serving reads during teardown is
        // harmless (commands land in a queue nobody reads and their reply
        // channels drop, which clients already handle).

        // Nothing is sequenced from here. Every supervisor saw the same
        // shutdown signal, and each waits for its own dependents to be holding
        // nothing before ending what it holds — so the reverse-dependency
        // order emerges rather than being walked. See
        // `crate::process::await_dependents_gone`.
        //
        // What is left for the root is the one question no single process can
        // answer: is the whole stack down? It watches the merge for that, and
        // narrates the countdown from it.
        let total_live = self
            .facts_snapshot()
            .iter()
            .filter(|(_, facts)| !facts.holds_nothing())
            .count();
        if total_live > 0 {
            self.output_manager
                .lifecycle_event(&format!("stopping {total_live} process(es)"));
        }

        let mut force_rx = crate::signals::force_watch();
        let mut announced_force = false;
        let mut remaining = total_live;
        // A backstop, not the mechanism: every grace period is bounded and a
        // second Ctrl+C collapses them all, so this only catches a supervisor
        // that has genuinely wedged.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        while remaining > 0 {
            tokio::select! {
                Some((name, facts)) = self.facts.recv() => {
                    let previous = self.facts_snapshot().get(&name).map(|f| f.phase);
                    if self.facts.apply(name.clone(), facts) {
                        self.absorb_facts(&name, previous);
                    }
                    let now = self
                        .facts_snapshot()
                        .iter()
                        .filter(|(_, facts)| !facts.holds_nothing())
                        .count();
                    if now < remaining {
                        remaining = now;
                        self.output_manager
                            .lifecycle_event(&format!("stopped ({remaining} remaining)"));
                    }
                }
                _ = force_rx.changed(), if !announced_force => {
                    if *force_rx.borrow() {
                        announced_force = true;
                        self.output_manager
                            .lifecycle_event("forcing immediate shutdown");
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    self.output_manager.error_event(
                        "shutdown: gave up waiting for processes to stop",
                    );
                    break;
                }
            }
        }
        // A task that is still holding a group here missed its own kill: the
        // backstop fired, or its supervisor is wedged. Aborting the supervisor
        // below only drops the handle, and `kill_on_drop` signals the direct
        // child alone — wrappers like `bazel run` leave the real workload
        // behind in the group, so kill the groups explicitly first.
        let live_groups: Vec<(String, i32)> = self
            .facts_snapshot()
            .live_task_groups()
            .into_iter()
            .map(|(name, pgid)| (name.to_string(), pgid))
            .collect();
        for (name, pgid) in live_groups {
            if crate::sys::kill_group(pgid) {
                self.output_manager
                    .service_event(&name, &format!("send SIGKILL to task pgid {pgid}"));
            }
        }
        // Every stop has been executed and joined; both supervisor sets are
        // idle (or their waits have completed against killed processes) and
        // can end now.
        // Abort-all-then-await keeps the 1s bound paid once, not once per
        // process.
        let supervisors = self.service_starts.abort_all();
        for (_, handle) in supervisors {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
        }
        let supervisors = self.task_supervisors.abort_all();
        for (_, handle) in supervisors {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
        }
    }

    /// Wait for remaining async tasks to finish after shutdown.
    pub(in crate::runner) async fn wait_for_shutdown(&mut self) {
        // Custody goes through the funnel even here, so the projection ends
        // teardown agreeing with reality.
        let names: Vec<String> = self.services.keys().cloned().collect();
        for name in names {
            self.clear_service_custody(&name);
        }
    }

    async fn drain_late_worker_results(&mut self) {
        // Late prepared reports carry spawned processes' identities; kill
        // those. Everything else is bookkeeping nobody needs mid-teardown.
        while let Ok(report) = self.report_rx.try_recv() {
            match report {
                super::ProcessReport::ServiceStartPrepared { name, result, .. } => {
                    self.stop_late_service_start(name, result).await;
                }
                super::ProcessReport::TaskRunPrepared { name, result, .. } => {
                    self.stop_late_task_start(name, result).await;
                }
                _ => {}
            }
        }
        while self.update_rx.try_recv().is_ok() {}
    }

    pub(in crate::runner) async fn stop_late_service_start(
        &mut self,
        name: String,
        result: Result<Box<super::service_supervisor::ServiceWired>, String>,
    ) {
        let Ok(_wired) = result else {
            return;
        };
        self.output_manager
            .service_event(&name, "start cancelled by shutdown");
        // The supervisor wired this spawn and holds the process — ask it to
        // stop. Its reader drains before the done-signal, so joining this
        // covers the output too (what the old inline reader+stop did).
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let sent = self.service_starts.registry().get(&name).is_some_and(|h| {
            h.request(super::service_supervisor::ServiceCommand::Stop(
                super::service_supervisor::StopRequest {
                    force: force_shutdown_requested(),
                    wait_full_exit: false,
                    interrupt: None,
                    notify: super::service_supervisor::StopNotify::Done(done_tx),
                    // Teardown: the failure history dies with the runner.
                    reset_policy: false,
                },
            ))
        });
        if sent {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(15), done_rx).await;
        }
    }

    pub(in crate::runner) async fn stop_late_task_start(
        &mut self,
        name: String,
        result: Result<super::task_supervisor::TaskRunReport, String>,
    ) {
        let Ok(super::task_supervisor::TaskRunReport::Running(wired)) = result else {
            return;
        };
        // The supervisor holds the process and is parked on its exit; ask it
        // to end the run. It signals the group and drains the reader before
        // answering, which is what this waits for.
        let _ = wired;
        self.output_manager
            .service_event(&name, "run cancelled by shutdown");
        if let Some(done) = self.send_task_kill(&name) {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), done).await;
        }
    }
}
