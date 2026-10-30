//! `start_on`: demand for a lazy service from something other than a connection,
//! like a queue with work on it. Armed only while the service is `Lazy`.

mod kafka;
mod temporal;

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::config::{KafkaGroup, KafkaServer, StartOn, TemporalServer};
use crate::facts::{FactsReader, Phase};
use crate::gate::Gate;
use crate::output::LifecycleEmitter;
use crate::process::ServiceState;

use self::kafka::KafkaSource;
pub(crate) use self::temporal::FileWatches;
use self::temporal::TemporalSource;

const CHECK_TIMEOUT: Duration = Duration::from_secs(5);
const CHECK_THROTTLE: Duration = Duration::from_secs(1);
const RECHECK: Duration = Duration::from_secs(60);

/// Everything one service's triggers need. They end when `demand_tx`'s
/// receiver is dropped, which happens when the supervisor exits.
#[derive(Clone)]
pub(crate) struct TriggerEnv {
    pub(crate) name: String,
    pub(crate) base_dir: PathBuf,
    pub(crate) world: FactsReader,
    pub(crate) emitter: LifecycleEmitter,
    pub(crate) demand_tx: mpsc::Sender<String>,
    pub(crate) depends_on: Vec<crate::config::Dependency>,
    pub(crate) file_watches: FileWatches,
    pub(crate) kafka: Arc<HashMap<String, KafkaServer>>,
    pub(crate) temporal: Arc<HashMap<String, TemporalServer>>,
}

/// Something that can say when a lazy service has work.
trait WorkSource: Send {
    /// Waits for a while, returning a description of the work if any appeared.
    /// `None` just means to ask again. Dropped if a service changes state.
    fn wait_for_work(&mut self) -> impl Future<Output = Result<Option<String>, String>> + Send;

    /// Waits until asking again after a failure is worthwhile.
    fn wait_to_retry(&mut self) -> impl Future<Output = ()> + Send {
        tokio::time::sleep(RECHECK)
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
    // One long-polling fetch per broker, whatever the number of groups.
    let mut groups_by_server: HashMap<String, Vec<KafkaGroup>> = HashMap::new();
    for group in start_on.kafka {
        groups_by_server
            .entry(group.server.clone())
            .or_default()
            .push(group);
    }
    // Config validation rejected a server name that isn't here.
    for (name, groups) in groups_by_server {
        if let Some(server) = env.kafka.get(&name).cloned() {
            tasks.spawn(run_trigger(env.clone(), KafkaSource { groups, server }));
        }
    }
    for queue in start_on.temporal {
        if let Some(server) = env.temporal.get(&queue.server).cloned() {
            let source = TemporalSource {
                base_dir: env.base_dir.clone(),
                changed: None,
                client: client.clone(),
                file_watches: env.file_watches.clone(),
                queue,
                server,
            };
            tasks.spawn(run_trigger(env.clone(), source));
        }
    }
    while tasks.join_next().await.is_some() {}
}

/// Ask the source for work whenever the service is armed, at most once a second,
/// and retry every minute after a failure. A service changing state asks again.
async fn run_trigger(mut env: TriggerEnv, mut source: impl WorkSource) {
    let mut last_check: Option<Instant> = None;
    let mut last_error: Option<String> = None;

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
        last_check = Some(Instant::now());

        let result = tokio::select! {
            result = source.wait_for_work() => result,
            _ = env.world.changed() => continue,
            _ = env.demand_tx.closed() => return,
        };
        match result {
            Ok(None) => last_error = None,
            Ok(Some(work)) => {
                env.emitter
                    .service_event(&env.name, &format!("start trigger: {work}"));
                if env.demand_tx.send(env.name.clone()).await.is_err() {
                    return;
                }
                // Stay quiet until the supervisor has taken the demand.
                if !wait_until_not_lazy(&mut env).await {
                    return;
                }
            }
            Err(e) => {
                // Expected until the server is up, so say it once.
                if last_error.as_ref() != Some(&e) {
                    env.emitter
                        .service_debug_event(&env.name, &format!("start trigger: {e}"));
                    last_error = Some(e);
                }
                tokio::select! {
                    _ = source.wait_to_retry() => {}
                    _ = env.world.changed() => {}
                    _ = env.demand_tx.closed() => return,
                }
            }
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
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
        const KAFKA: &str = "[kafka.local]\nbootstrap = \"127.0.0.1:9092\"\n";
        const KAFKA_TRIGGER: &str = "[[services.w.start_on.kafka]]\nconsumer_group = \"g\"\nserver = \"local\"\ntopics = [\"t\"]\n";
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
                name: "kafka group and no proxy",
                config: config(KAFKA, "lazy = true", KAFKA_TRIGGER),
                error: None,
            },
            Case {
                name: "unknown kafka server",
                config: config("", "lazy = true", KAFKA_TRIGGER),
                error: Some("unknown kafka server 'local'"),
            },
            Case {
                name: "kafka group without topics",
                config: config(
                    KAFKA,
                    "lazy = true",
                    "[[services.w.start_on.kafka]]\nconsumer_group = \"g\"\nserver = \"local\"\ntopics = []\n",
                ),
                error: Some("consumer group 'g' has no topics"),
            },
            Case {
                name: "kafka bootstrap without a port",
                config: config(
                    "[kafka.local]\nbootstrap = \"localhost\"\n",
                    "lazy = true",
                    KAFKA_TRIGGER,
                ),
                error: Some("must be host:port"),
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
}
