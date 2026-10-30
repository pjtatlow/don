//! Demand for a lazy service from something other than a connection: a queue
//! with work on it. Armed only while the service is `Lazy`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::config::{StartOn, TemporalQueue, TemporalServer};
use crate::facts::{FactsReader, Phase};
use crate::gate::Gate;
use crate::output::LifecycleEmitter;
use crate::process::ServiceState;

const CHECK_TIMEOUT: Duration = Duration::from_secs(5);
const CHECK_THROTTLE: Duration = Duration::from_secs(1);
const TEMPORAL_RECHECK: Duration = Duration::from_secs(60);
const TASK_QUEUE_TYPES: [&str; 2] = ["TASK_QUEUE_TYPE_WORKFLOW", "TASK_QUEUE_TYPE_ACTIVITY"];

/// Everything one service's triggers need. They end when `demand_tx`'s
/// receiver is dropped, which happens when the supervisor exits.
pub(crate) struct TriggerEnv {
    pub(crate) name: String,
    pub(crate) base_dir: PathBuf,
    pub(crate) world: FactsReader,
    pub(crate) emitter: LifecycleEmitter,
    pub(crate) demand_tx: mpsc::Sender<String>,
    pub(crate) depends_on: Vec<crate::config::Dependency>,
    pub(crate) file_watches: FileWatches,
    pub(crate) temporal: Arc<HashMap<String, TemporalServer>>,
}

/// One watcher per file, however many triggers care about it.
#[derive(Clone, Default)]
pub(crate) struct FileWatches(Arc<Mutex<HashMap<PathBuf, SharedWatch>>>);

struct SharedWatch {
    _watcher: RecommendedWatcher,
    changed: watch::Sender<()>,
}

impl FileWatches {
    /// `None` if the file's directory doesn't exist yet.
    fn subscribe(&self, path: &Path) -> Option<watch::Receiver<()>> {
        let mut watches = self.0.lock().ok()?;
        if let Some(shared) = watches.get(path) {
            return Some(shared.changed.subscribe());
        }
        let (changed, receiver) = watch::channel(());
        let watcher = watch_file(path, changed.clone())?;
        watches.insert(
            path.to_path_buf(),
            SharedWatch {
                _watcher: watcher,
                changed,
            },
        );
        Some(receiver)
    }
}

/// Run every trigger for a service until the supervisor goes away.
pub(crate) async fn run(env: TriggerEnv, start_on: StartOn) {
    let mut tasks = JoinSet::new();
    let client = match reqwest::Client::builder().timeout(CHECK_TIMEOUT).build() {
        Ok(client) => client,
        Err(e) => {
            env.emitter.service_error_event(
                &env.name,
                &format!("start trigger disabled, HTTP client failed: {e}"),
            );
            return;
        }
    };
    for queue in start_on.temporal {
        // Config validation rejected a name that isn't here.
        let Some(server) = env.temporal.get(&queue.server).cloned() else {
            continue;
        };
        let env = TriggerEnv {
            name: env.name.clone(),
            base_dir: env.base_dir.clone(),
            world: env.world.clone(),
            emitter: env.emitter.clone(),
            demand_tx: env.demand_tx.clone(),
            depends_on: env.depends_on.clone(),
            file_watches: env.file_watches.clone(),
            temporal: Arc::clone(&env.temporal),
        };
        tasks.spawn(run_trigger(env, server, queue, client.clone()));
    }
    while tasks.join_next().await.is_some() {}
}

/// Check when armed, when the watched file changes (throttled), and when any
/// service changes state. Retry every minute after a failed check, or while the
/// file can't be watched.
async fn run_trigger(
    mut env: TriggerEnv,
    server: TemporalServer,
    queue: TemporalQueue,
    client: reqwest::Client,
) {
    let mut changed: Option<watch::Receiver<()>> = None;
    let mut last_check: Option<Instant> = None;
    let mut last_error: Option<String> = None;
    let mut warned_unwatched = false;

    loop {
        if !wait_until_armed(&mut env).await {
            return;
        }
        if let Some(last) = last_check {
            tokio::select! {
                _ = tokio::time::sleep_until(last + CHECK_THROTTLE) => {}
                _ = env.demand_tx.closed() => return,
            }
        }
        if changed.is_none() {
            // Often absent until the server first runs, so retried on each check.
            changed = env
                .file_watches
                .subscribe(&env.base_dir.join(&server.database));
        }

        last_check = Some(Instant::now());
        match backlog(&client, &server, &queue).await {
            Ok(0) => {
                last_error = None;
                // The server is up, so its database should be watchable by now.
                if changed.is_none() && !warned_unwatched {
                    warned_unwatched = true;
                    env.emitter.service_event(
                        &env.name,
                        &format!(
                            "start trigger: can't watch {}, check the server's database path",
                            server.database.display()
                        ),
                    );
                }
            }
            Ok(tasks) => {
                env.emitter.service_event(
                    &env.name,
                    &format!(
                        "start trigger: {tasks} task(s) on Temporal queue {}/{}",
                        queue.namespace, queue.task_queue
                    ),
                );
                if env.demand_tx.send(env.name.clone()).await.is_err() {
                    return;
                }
                // Stay quiet until the supervisor has taken the demand.
                if !wait_until_not_lazy(&mut env).await {
                    return;
                }
                continue;
            }
            Err(e) => {
                // Expected until the server is up, so say it once.
                if last_error.as_ref() != Some(&e) {
                    env.emitter
                        .service_debug_event(&env.name, &format!("start trigger: {e}"));
                    last_error = Some(e);
                }
            }
        }

        tokio::select! {
            _ = file_changed(&mut changed) => {}
            _ = tokio::time::sleep(TEMPORAL_RECHECK), if last_error.is_some() || changed.is_none() => {}
            _ = env.world.changed() => {}
            _ = env.demand_tx.closed() => return,
        }
    }
}

fn is_lazy(env: &TriggerEnv) -> bool {
    env.world
        .snapshot()
        .get(&env.name)
        .is_some_and(|facts| facts.phase == Phase::Service(ServiceState::Lazy))
}

/// Lazy, with the service's dependencies up, so a server it depends on can be
/// asked. False once the supervisor is gone.
async fn wait_until_armed(env: &mut TriggerEnv) -> bool {
    while !is_lazy(env) || crate::gate::level(&env.depends_on, &env.world.snapshot()) != Gate::Open
    {
        tokio::select! {
            changed = env.world.changed() => if changed.is_none() { return false },
            _ = env.demand_tx.closed() => return false,
        }
    }
    !env.demand_tx.is_closed()
}

/// False once the supervisor is gone.
async fn wait_until_not_lazy(env: &mut TriggerEnv) -> bool {
    while is_lazy(env) {
        tokio::select! {
            changed = env.world.changed() => if changed.is_none() { return false },
            _ = env.demand_tx.closed() => return false,
        }
    }
    true
}

async fn file_changed(changed: &mut Option<watch::Receiver<()>>) {
    if let Some(changed) = changed.as_mut()
        && changed.changed().await.is_ok()
    {
        return;
    }
    std::future::pending().await
}

/// Watches the directory, because databases touch sidecars and replace files.
fn watch_file(path: &Path, changed: watch::Sender<()>) -> Option<RecommendedWatcher> {
    let dir = path.parent()?.to_path_buf();
    let file_name = path.file_name()?.to_string_lossy().into_owned();
    let mut watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        if let Ok(event) = event
            && event.paths.iter().any(|path| {
                path.file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with(&file_name))
            })
        {
            changed.send_replace(());
        }
    })
    .ok()?;
    watcher.watch(&dir, RecursiveMode::NonRecursive).ok()?;
    Some(watcher)
}

/// Tasks waiting on the queue, workflow and activity together.
async fn backlog(
    client: &reqwest::Client,
    server: &TemporalServer,
    queue: &TemporalQueue,
) -> Result<u64, String> {
    let mut url = task_queue_url(&server.url, queue)?;
    // ENHANCED mode is deprecated, but it reports both task types in one call,
    // which saves time. If it gets removed, we will have to move.
    {
        let mut query = url.query_pairs_mut();
        query
            .append_pair("apiMode", "DESCRIBE_TASK_QUEUE_MODE_ENHANCED")
            .append_pair("reportStats", "true");
        for queue_type in TASK_QUEUE_TYPES {
            query.append_pair("taskQueueTypes", queue_type);
        }
    }
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|e| format!("can't reach Temporal at {}: {}", server.url, e))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| format!("bad response from Temporal at {}: {}", server.url, e))?;
    if !status.is_success() {
        return Err(format!(
            "Temporal at {} returned {status}: {body}",
            server.url
        ));
    }
    parse_backlog(&body)
}

fn task_queue_url(base: &str, queue: &TemporalQueue) -> Result<reqwest::Url, String> {
    let mut url =
        reqwest::Url::parse(base).map_err(|e| format!("invalid Temporal URL {base}: {e}"))?;
    url.path_segments_mut()
        .map_err(|()| format!("invalid Temporal URL {base}"))?
        .pop_if_empty()
        .extend([
            "api",
            "v1",
            "namespaces",
            &queue.namespace,
            "task-queues",
            &queue.task_queue,
        ]);
    Ok(url)
}

/// Sums every version and task type. `approximateBacklogCount` is an int64,
/// which proto3 JSON writes as a string and omits entirely at zero.
fn parse_backlog(body: &str) -> Result<u64, String> {
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("bad response from Temporal: {e}"))?;
    let mut total = 0;
    let versions = value["versionsInfo"].as_object().into_iter().flatten();
    for (_, version) in versions {
        let types = version["typesInfo"].as_object().into_iter().flatten();
        for (_, task_type) in types {
            total += parse_count(&task_type["stats"]["approximateBacklogCount"])?;
        }
    }
    Ok(total)
}

fn parse_count(count: &serde_json::Value) -> Result<u64, String> {
    match count {
        serde_json::Value::Null => Ok(0),
        serde_json::Value::String(s) => s
            .parse()
            .map_err(|e| format!("bad backlog count {s:?} from Temporal: {e}")),
        other => other
            .as_u64()
            .ok_or_else(|| format!("bad backlog count {other} from Temporal")),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn parses_backlog_responses() {
        struct Case {
            name: &'static str,
            body: &'static str,
            expect: Result<u64, ()>,
        }
        let cases = [
            Case {
                name: "workflow and activity backlogs add up, as Temporal 1.28 sends them",
                body: r#"{"versionsInfo":{"":{"typesInfo":{"1":{"stats":{"approximateBacklogCount":"3"}},"2":{"stats":{"approximateBacklogCount":"2"}}}}}}"#,
                expect: Ok(5),
            },
            Case {
                name: "zero is omitted",
                body: r#"{"versionsInfo":{"":{"typesInfo":{"1":{"stats":{"approximateBacklogAge":"0s"}},"2":{"stats":{}}}}}}"#,
                expect: Ok(0),
            },
            Case {
                name: "no versions at all",
                body: "{}",
                expect: Ok(0),
            },
            Case {
                name: "numeric count",
                body: r#"{"versionsInfo":{"":{"typesInfo":{"1":{"stats":{"approximateBacklogCount":2}}}}}}"#,
                expect: Ok(2),
            },
            Case {
                name: "not a count",
                body: r#"{"versionsInfo":{"":{"typesInfo":{"1":{"stats":{"approximateBacklogCount":"many"}}}}}}"#,
                expect: Err(()),
            },
            Case {
                name: "not JSON",
                body: "<html>",
                expect: Err(()),
            },
        ];
        for case in cases {
            assert_eq!(
                parse_backlog(case.body).map_err(|_| ()),
                case.expect,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn validates_start_on_config() {
        struct Case {
            name: &'static str,
            config: String,
            error: Option<&'static str>,
        }
        const SERVER: &str =
            "[temporal.local]\ndatabase = \"x.db\"\nurl = \"http://127.0.0.1:7243\"\n";
        const TRIGGER: &str = "[[services.w.start_on.temporal]]\nnamespace = \"default\"\nserver = \"local\"\ntask_queue = \"default\"\n";
        let config = |server: &str, lazy: &str, trigger: &str| {
            format!(
                "min_version = \"0.0.0\"\n{server}[services.w]\nrun.cmd = \"true\"\n{lazy}\n{trigger}"
            )
        };
        let cases = [
            Case {
                name: "lazy with a trigger and no proxy",
                config: config(SERVER, "lazy = true", TRIGGER),
                error: None,
            },
            Case {
                name: "lazy with nothing to trigger it",
                config: config(SERVER, "lazy = true", ""),
                error: Some("requires a 'proxy' or 'start_on' entry"),
            },
            Case {
                name: "trigger without lazy",
                config: config(SERVER, "", TRIGGER),
                error: Some("no effect without 'lazy = true'"),
            },
            Case {
                name: "unknown server",
                config: config("", "lazy = true", TRIGGER),
                error: Some("unknown temporal server 'local'"),
            },
            Case {
                name: "gRPC address instead of an HTTP URL",
                config: config(
                    "[temporal.local]\ndatabase = \"x.db\"\nurl = \"127.0.0.1:7233\"\n",
                    "lazy = true",
                    TRIGGER,
                ),
                error: Some("must be an http(s) URL"),
            },
        ];
        for case in cases {
            let config: crate::config::Config = case.config.parse().unwrap();
            let result = config.validate(crate::config::Platform::LinuxX86_64);
            match case.error {
                None => assert!(result.is_ok(), "{}: {:?}", case.name, result.err()),
                Some(error) => assert!(
                    format!("{result:?}").contains(error),
                    "{}: expected {error}, got {result:?}",
                    case.name
                ),
            }
        }
    }

    #[tokio::test]
    async fn triggers_share_a_watcher_per_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("local.db");
        std::fs::write(&path, "a").unwrap();
        let watches = FileWatches::default();
        let mut first = watches.subscribe(&path).unwrap();
        let mut second = watches.subscribe(&path).unwrap();
        assert_eq!(watches.0.lock().unwrap().len(), 1);

        std::fs::write(&path, "b").unwrap();
        for receiver in [&mut first, &mut second] {
            tokio::time::timeout(Duration::from_secs(10), receiver.changed())
                .await
                .unwrap()
                .unwrap();
        }
    }

    #[test]
    fn builds_task_queue_urls() {
        struct Case {
            http: &'static str,
            task_queue: &'static str,
            expect: &'static str,
        }
        let cases = [
            Case {
                http: "http://127.0.0.1:7243",
                task_queue: "default",
                expect: "http://127.0.0.1:7243/api/v1/namespaces/ns/task-queues/default",
            },
            Case {
                http: "http://127.0.0.1:7243/",
                task_queue: "default",
                expect: "http://127.0.0.1:7243/api/v1/namespaces/ns/task-queues/default",
            },
            Case {
                http: "http://127.0.0.1:7243",
                task_queue: "a/b c",
                expect: "http://127.0.0.1:7243/api/v1/namespaces/ns/task-queues/a%2Fb%20c",
            },
        ];
        for case in cases {
            let queue = TemporalQueue {
                namespace: "ns".to_string(),
                server: "local".to_string(),
                task_queue: case.task_queue.to_string(),
            };
            assert_eq!(
                task_queue_url(case.http, &queue).unwrap().as_str(),
                case.expect
            );
        }
    }
}
