//! Opt-in profiling against captured Bazel XML and an existing checkout.
use super::graph::BazelDepGraph;
use crate::build_tool::batch::send_watch_update;
use crate::watch::{WatchDispatch, WatchItemKind, WatchManager};
use std::time::Instant;

struct NoopDispatch;
impl WatchDispatch for NoopDispatch {
    fn changed(&self, _: &str, _: WatchItemKind) {}
}

/// DON_PROFILE_XML, DON_PROFILE_TARGETS (one label per line), and DON_PROFILE_ROOT
/// select the captured graph and checkout. Run with --release --ignored --nocapture.
#[tokio::test]
#[ignore = "requires a captured dependency graph and its checkout"]
#[allow(clippy::unwrap_used)]
async fn profile_dependency_watches() {
    let xml = std::fs::read(std::env::var("DON_PROFILE_XML").unwrap()).unwrap();
    let targets: Vec<_> = std::fs::read_to_string(std::env::var("DON_PROFILE_TARGETS").unwrap())
        .unwrap()
        .lines()
        .map(String::from)
        .collect();
    let base = std::fs::canonicalize(std::env::var("DON_PROFILE_ROOT").unwrap()).unwrap();
    let started = Instant::now();
    let graph = BazelDepGraph::parse_xml(xml.as_slice()).unwrap();
    eprintln!(
        "PROFILE parse bytes={} elapsed={:?}",
        xml.len(),
        started.elapsed()
    );
    let started = Instant::now();
    let packages: Vec<_> = targets
        .iter()
        .map(|target| graph.packages_for(target))
        .collect();
    eprintln!(
        "PROFILE attribution targets={} packages={} elapsed={:?}",
        targets.len(),
        packages.iter().map(Vec::len).sum::<usize>(),
        started.elapsed()
    );
    let mut toml = String::from("watch_ignore = [\"**/node_modules/**\", \"data/**\"]\n");
    for (i, target) in targets.iter().enumerate() {
        toml.push_str(&format!("[services.s{i}]\nbazel.target = {target:?}\n"));
    }
    let config = toml.parse().unwrap();
    let output = crate::output::OutputManager::new_verbose(&[], tokio::io::stderr(), true)
        .await
        .unwrap();
    let (updates, rx) = tokio::sync::mpsc::unbounded_channel();
    let (_queries, query_rx) = tokio::sync::mpsc::channel(1);
    let (manager, _) = WatchManager::new(
        &config,
        crate::config::Platform::current().unwrap(),
        &base,
        Box::new(NoopDispatch),
        rx,
        query_rx,
        output.clone_lifecycle_emitter(),
    )
    .unwrap();
    let handle = tokio::spawn(manager.run());
    let started = Instant::now();
    for (i, packages) in packages.into_iter().enumerate() {
        send_watch_update(
            &updates,
            format!("s{i}"),
            WatchItemKind::Service,
            packages.iter().map(|p| format!("{p}/**")).collect(),
            vec![],
            base.clone(),
        )
        .await;
        send_watch_update(
            &updates,
            format!("s{i}__graph"),
            WatchItemKind::BuildGraph,
            packages
                .iter()
                .flat_map(|p| [format!("{p}/BUILD"), format!("{p}/BUILD.bazel")])
                .collect(),
            vec![],
            base.clone(),
        )
        .await;
    }
    eprintln!("PROFILE registration elapsed={:?}", started.elapsed());
    handle.abort();
    let _ = handle.await;
    output.shutdown().await;
}
