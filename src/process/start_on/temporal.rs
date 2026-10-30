//! Starts a service when a Temporal task queue has a backlog, prompted by the
//! server's database changing.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::watch;

use super::{RECHECK, WorkSource};
use crate::config::{TemporalQueue, TemporalServer};

const TASK_QUEUE_TYPES: [&str; 2] = ["TASK_QUEUE_TYPE_WORKFLOW", "TASK_QUEUE_TYPE_ACTIVITY"];

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

pub(super) struct TemporalSource {
    pub(super) base_dir: PathBuf,
    pub(super) changed: Option<watch::Receiver<()>>,
    pub(super) client: reqwest::Client,
    pub(super) file_watches: FileWatches,
    pub(super) queue: TemporalQueue,
    pub(super) server: TemporalServer,
}

impl WorkSource for TemporalSource {
    async fn wait_for_work(&mut self) -> Result<Option<String>, String> {
        if self.changed.is_none() {
            // Often absent until the server first runs, so retried on each check.
            let database = self.base_dir.join(&self.server.database);
            self.changed = self.file_watches.subscribe(&database);
        }
        let tasks = backlog(&self.client, &self.server, &self.queue).await?;
        if tasks > 0 {
            return Ok(Some(format!(
                "{tasks} task(s) on Temporal queue {}/{}",
                self.queue.namespace, self.queue.task_queue
            )));
        }
        match self.changed.as_mut() {
            Some(changed) => {
                changed.changed().await.map_err(|e| e.to_string())?;
            }
            None => tokio::time::sleep(RECHECK).await,
        }
        Ok(None)
    }

    async fn wait_to_retry(&mut self) {
        if let Some(changed) = self.changed.as_mut() {
            let _ = tokio::time::timeout(RECHECK, changed.changed()).await;
        } else {
            tokio::time::sleep(RECHECK).await;
        }
    }
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
    use std::time::Duration;

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
