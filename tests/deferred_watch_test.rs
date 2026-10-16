#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
//! Exercise the real runner with a controllable Bazel process. Each case runs
//! in its own test subprocess so PATH is never mutated in a multithreaded runtime.
mod helpers;

use don::{Config, OutputManager, Platform, Runner};
use helpers::timeout::run_with_timeout;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Duration;
use tokio::sync::mpsc;

const FAKE_BAZEL: &str = r##"#!/bin/bash
set -eu
[ "$1" = version ] && exit 0
if [ "$1" = run ]; then
  if [ -d .bazel-lock ]; then echo overlap >> commands; exit 1; fi
  printf '%s\n' "$*" >> commands
  for arg in "$@"; do
    case "$arg" in
      //a:app|//b:app)
        name=${arg#//}; name=${name%:app}
        exec "bazel-out/bin/$name" ;;
    esac
  done
  exit 1
fi
if ! mkdir .bazel-lock 2>/dev/null; then
  echo overlap >> commands
  exit 1
fi
trap 'rmdir .bazel-lock' EXIT
printf '%s\n' "$*" >> commands
case "$1" in
  build)
    check=false
    for arg in "$@"; do [ "$arg" != --check_up_to_date ] || check=true; done
    for arg in "$@"; do
      case "$arg" in
        //a:app|//b:app)
          name=${arg#//}; name=${name%:app}
          current="$(cat "$name/source" 2>/dev/null || echo deleted)$(cat "$name/BUILD.bazel" 2>/dev/null || true)"
          if $check; then
            [ "$current" = "$(cat "$name-built")" ] || exit 1
          else
            printf '%s\n' "$current" > "$name-built"
          fi ;;
      esac
    done
    if ! $check; then
      touch build-started
      while [ ! -f build-release ]; do sleep 0.02; done
    fi ;;
  cquery)
    [ ! -f fake-bin/cquery-fail ] || exit 1
    printf '//a:app\tbazel-out/bin/a\n//b:app\tbazel-out/bin/b\n' ;;
  query)
    touch query-started
    while [ ! -f query-release ]; do sleep 0.02; done
    [ ! -f query-delay ] || sleep 2.5
    [ ! -f fake-bin/query-fail ] || exit 1
    cat <<'XML'
<query>
<rule name="//a:app"><rule-input name="//a:source"/></rule>
<rule name="//b:app"><rule-input name="//b:source"/></rule>
<source-file name="//a:source"/><source-file name="//b:source"/>
</query>
XML
    ;;
esac
"##;

fn executable(path: &Path, contents: &str) {
    fs::write(path, contents).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

fn lines(path: &Path) -> usize {
    fs::read_to_string(path).unwrap_or_default().lines().count()
}

async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(8), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("condition timed out");
}

#[test]
fn deferred_dependency_watches() {
    if let Ok(case) = std::env::var("DON_WATCH_TEST_CASE") {
        run_with_timeout(Duration::from_secs(30), run_case(&case));
        return;
    }
    for case in [
        "gap-edit",
        "gap-delete",
        "gap-rename",
        "gap-directory-delete",
        "gap-build-graph",
        "watch-disabled",
        "during-build",
        "no-edit",
        "query-failure",
        "query-failure-recovery",
        "cquery-failure",
        "pending-build-graph",
        "task-once",
        "task-self-write",
        "shutdown",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("fake-bin");
        fs::create_dir(&bin).unwrap();
        executable(&bin.join("bazel"), FAKE_BAZEL);
        let path = std::env::join_paths(std::iter::once(bin).chain(std::env::split_paths(
            &std::env::var_os("PATH").unwrap_or_default(),
        )))
        .unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "deferred_dependency_watches", "--nocapture"])
            .env("DON_WATCH_TEST_CASE", case)
            .env("DON_WATCH_TEST_DIR", dir.path())
            .env("PATH", path)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{case}: {}\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
            fs::read_to_string(dir.path().join("output")).unwrap_or_default()
        );
    }
}

async fn run_case(case: &str) {
    let base = std::path::PathBuf::from(std::env::var_os("DON_WATCH_TEST_DIR").unwrap());
    fs::write(base.join("MODULE.bazel"), "").unwrap();
    fs::create_dir_all(base.join("bazel-out/bin")).unwrap();
    for name in ["a", "b"] {
        fs::create_dir(base.join(name)).unwrap();
        fs::write(base.join(name).join("source"), "v1").unwrap();
        fs::write(base.join(name).join("BUILD.bazel"), "").unwrap();
        executable(
            &base.join("bazel-out/bin").join(name),
            &format!(
                "#!/bin/bash\ncat {name}-built > {name}-running\necho started >> {name}-starts\nexec sleep 60\n"
            ),
        );
    }
    if case != "during-build" {
        fs::write(base.join("build-release"), "").unwrap();
    }
    if matches!(case, "query-failure" | "query-failure-recovery") {
        fs::write(base.join("fake-bin/query-fail"), "").unwrap();
    }
    if case == "cquery-failure" {
        fs::write(base.join("fake-bin/cquery-fail"), "").unwrap();
    }
    let task_case = matches!(case, "task-once" | "task-self-write");
    let config_text = if task_case {
        let writes = if case == "task-self-write" {
            fs::write(base.join("query-delay"), "").unwrap();
            "printf generated > a/generated\n"
        } else {
            ""
        };
        executable(
            &base.join("bazel-out/bin/a"),
            &format!("#!/bin/bash\ncat a-built >> a-starts\n{writes}"),
        );
        format!(
            r#"
[tasks.a]
bazel.target = "//a:app"
auto_run = "{}"
watch = ["a/**"]
[services.b]
bazel.target = "//b:app"
ready.exec.cmd = "true"
"#,
            if case == "task-once" {
                "once"
            } else {
                "always"
            }
        )
    } else if case == "pending-build-graph" {
        format!(
            r#"
[services.a]
bazel.target = "//a:app"
depends_on = ["gate"]
ready.exec.cmd = "true"
[services.b]
bazel.target = "//b:app"
ready.exec.cmd = "true"
[services.gate]
run.cmd = "sleep"
run.args = ["60"]
ready.exec.cmd = "test"
ready.exec.args = ["-f", {:?}]
"#,
            base.join("gate-release")
        )
    } else {
        r#"
[services.a]
bazel.target = "//a:app"
ready.exec.cmd = "true"
[services.b]
bazel.target = "//b:app"
ready.exec.cmd = "true"
"#
        .to_string()
    };
    let mut config: Config = format!(
        r#"watch_ignore = ["output", "commands", "*-built", "*-starts", "*-running", "query-*", "build-*", ".bazel-lock", "fake-bin/**", "gate-release"]
{config_text}"#
    )
    .parse()
    .unwrap();
    if case == "watch-disabled"
        && let Some(don::config::ServiceKind::Bazel(bazel)) =
            config.services.get_mut("b").unwrap().kind.as_mut()
    {
        bazel.watch = false;
    }
    let logs: Vec<_> = config
        .services
        .iter()
        .map(|(name, svc)| (name.as_str(), &svc.log))
        .chain(
            config
                .tasks
                .iter()
                .map(|(name, task)| (name.as_str(), &task.log)),
        )
        .collect();
    let output = OutputManager::new_verbose(
        &logs,
        tokio::fs::File::create(base.join("output")).await.unwrap(),
        true,
    )
    .await
    .unwrap();
    let (shutdown_tx, shutdown_rx) = mpsc::channel(2);
    let runner = Runner::new(
        config,
        Platform::LinuxX86_64,
        output,
        base.clone(),
        None,
        shutdown_rx,
        true,
    )
    .await
    .unwrap();
    let handle = tokio::spawn(runner.run());
    if case == "during-build" {
        until(|| base.join("build-started").exists()).await;
        fs::write(base.join("a/source"), "v2").unwrap();
        fs::write(base.join("build-release"), "").unwrap();
    }
    // The query remains blocked. Services and tasks can run; dependencies
    // retain their execution barriers.
    if case == "cquery-failure" {
        until(|| base.join("query-started").exists()).await;
        assert_eq!(lines(&base.join("a-starts")), 0);
        assert_eq!(lines(&base.join("b-starts")), 0);
        fs::write(base.join("query-release"), "").unwrap();
    }
    until(|| lines(&base.join("b-starts")) == 1).await;
    if !task_case && case != "pending-build-graph" {
        until(|| lines(&base.join("a-starts")) == 1).await;
    }
    until(|| base.join("query-started").exists()).await;
    if task_case {
        // Tasks use their explicit watches, installed before the build, and
        // must not wait for a service's dependency graph to finish querying.
        until(|| lines(&base.join("a-starts")) == 1).await;
        until(|| {
            fs::read_to_string(base.join("output"))
                .unwrap_or_default()
                .contains("a: complete")
        })
        .await;
        assert_eq!(
            fs::read_to_string(base.join("a-starts")).unwrap().trim(),
            "v1"
        );
        if case == "task-once" {
            fs::write(base.join("a/source"), "v2").unwrap();
        }
        fs::write(base.join("query-release"), "").unwrap();
        // Let the task's own write, any delayed prepare, and its debounce
        // settle. A generator must not execute a second time from that write.
        tokio::time::sleep(Duration::from_secs(4)).await;
        assert_eq!(lines(&base.join("a-starts")), 1, "task ran more than once");
    } else if case == "pending-build-graph" {
        assert_eq!(lines(&base.join("a-starts")), 0);
        fs::write(base.join("a/BUILD.bazel"), "graph-v2").unwrap();
        fs::write(base.join("query-release"), "").unwrap();
        until(|| {
            fs::read_to_string(base.join("a-built"))
                .unwrap_or_default()
                .contains("graph-v2")
        })
        .await;
        assert_eq!(
            lines(&base.join("a-starts")),
            0,
            "dependency barrier bypassed"
        );
        fs::write(base.join("gate-release"), "").unwrap();
        until(|| lines(&base.join("a-starts")) == 1).await;
        assert!(
            fs::read_to_string(base.join("a-running"))
                .unwrap()
                .contains("graph-v2")
        );
    } else if case == "query-failure-recovery" {
        fs::write(base.join("a/source"), "v2").unwrap();
        fs::write(base.join("query-release"), "").unwrap();
        until(|| lines(&base.join("a-starts")) == 2).await;
        assert!(
            fs::read_to_string(base.join("a-running"))
                .unwrap()
                .contains("v2")
        );
        // Changing the ignored failure switch cannot itself retrigger the
        // fallback watcher; only the nested source edit should recover it.
        fs::remove_file(base.join("fake-bin/query-fail")).unwrap();
        fs::write(base.join("a/source"), "v3").unwrap();
        until(|| lines(&base.join("a-starts")) == 3).await;
        assert!(
            fs::read_to_string(base.join("a-running"))
                .unwrap()
                .contains("v3")
        );
        // Fallback coverage is conservative. Once precise watches replace
        // it, a later ordinary edit must again affect only its owner.
        tokio::time::sleep(Duration::from_millis(400)).await;
        let b_runs = lines(&base.join("b-starts"));
        fs::write(base.join("a/source"), "v4").unwrap();
        until(|| lines(&base.join("a-starts")) == 4).await;
        assert_eq!(lines(&base.join("b-starts")), b_runs);
    } else {
        match case {
            "gap-edit" => fs::write(base.join("a/source"), "v2").unwrap(),
            "gap-directory-delete" => fs::remove_dir_all(base.join("a")).unwrap(),
            "gap-build-graph" => fs::write(base.join("a/BUILD.bazel"), "graph-v2").unwrap(),
            "gap-delete" => fs::remove_file(base.join("a/source")).unwrap(),
            "gap-rename" => fs::rename(base.join("a/source"), base.join("a/renamed")).unwrap(),
            _ => {}
        }
        if case != "shutdown" {
            fs::write(base.join("query-release"), "").unwrap();
            until(|| {
                fs::read_to_string(base.join("output"))
                    .unwrap_or_default()
                    .contains("startup gap check elapsed=")
            })
            .await;
            let changed = matches!(
                case,
                "gap-edit"
                    | "gap-delete"
                    | "gap-rename"
                    | "gap-directory-delete"
                    | "gap-build-graph"
                    | "during-build"
            );
            if changed {
                until(|| lines(&base.join("a-starts")) == 2).await;
            }
            if changed {
                let running = fs::read_to_string(base.join("a-running")).unwrap();
                assert!(
                    running.contains("v2") || running.contains("deleted"),
                    "{running}"
                );
            }
            assert_eq!(
                lines(&base.join("b-starts")),
                1,
                "unrelated service restarted"
            );
            if case != "query-failure" {
                // A subsequent ordinary event must still rebuild only its owner.
                fs::write(base.join("b/source"), "v2").unwrap();
                if case == "watch-disabled" {
                    tokio::time::sleep(Duration::from_millis(700)).await;
                    assert_eq!(lines(&base.join("b-starts")), 1);
                } else {
                    until(|| lines(&base.join("b-starts")) == 2).await;
                    assert!(
                        fs::read_to_string(base.join("b-running"))
                            .unwrap()
                            .contains("v2")
                    );
                }
                assert_eq!(lines(&base.join("a-starts")), if changed { 2 } else { 1 });
            }
        }
    }
    let commands = fs::read_to_string(base.join("commands")).unwrap();
    let mut sequence = commands.lines();
    assert!(
        sequence.next().unwrap().starts_with("build --curses"),
        "{commands}"
    );
    assert!(
        sequence.next().unwrap().starts_with("cquery "),
        "{commands}"
    );
    assert!(sequence.next().unwrap().starts_with("query "), "{commands}");
    assert!(!commands.contains("overlap"), "{commands}");
    shutdown_tx.send(()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), handle)
        .await
        .expect("shutdown blocked on query")
        .unwrap()
        .unwrap();
}
