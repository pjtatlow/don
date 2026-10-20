//! Bazel's Build Event Protocol, read while the build is still running.
//!
//! `bazel build --build_event_json_file=PATH` writes one event per line as
//! the build progresses. A `TargetComplete` event is Bazel saying that one
//! top-level target, and every output it was asked for, is done — which is
//! what lets a service start before the rest of its batch has finished.
//!
//! The JSON file rather than `--build_event_binary_file`: both are the same
//! `build_event_stream.proto` messages, the JSON one in proto3's canonical
//! JSON mapping, so the field names are as stable as the proto's. Reading it
//! needs only `serde_json`; the binary form would need the proto and its
//! imports vendored plus a protobuf code generator in the build, to read three
//! fields.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use tokio::io::AsyncBufReadExt;
use tokio::sync::{mpsc, oneshot};

/// How often to look for new events while the build runs. Bazel flushes the
/// file as events are posted, and a service waiting on its target notices
/// at most this late.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// What Bazel decided about one top-level target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TargetVerdict {
    /// The label as [`normalize_label`] spells it.
    pub(crate) label: String,
    /// `Err` carries Bazel's reason, for the service's own error line.
    pub(crate) result: Result<(), String>,
}

/// Where the build should write its events, and who wants each verdict.
pub(crate) struct BepOutput {
    pub(crate) path: PathBuf,
    /// Every verdict found in one read of the file, sent together. A warm
    /// build completes most targets within milliseconds of each other, and
    /// whoever acts on them can share work across one burst.
    pub(crate) verdicts: mpsc::UnboundedSender<Vec<TargetVerdict>>,
}

/// Spell a label the one way both don.toml and Bazel can be compared in.
///
/// Bazel reports main-repository labels as `//pkg:name` or, canonically,
/// `@@//pkg:name`, and always with an explicit name; a don.toml may write
/// `//pkg` for `//pkg:pkg`.
pub(crate) fn normalize_label(label: &str) -> String {
    let label = label
        .strip_prefix("@@//")
        .or_else(|| label.strip_prefix("@//"))
        .map_or_else(|| label.to_string(), |rest| format!("//{rest}"));
    let Some((repo, package)) = label.split_once("//") else {
        return label;
    };
    if package.contains(':') {
        return label;
    }
    let name = package.rsplit('/').next().unwrap_or(package);
    format!("{repo}//{package}:{name}")
}

/// The verdict in one line of the JSON event file, if it is a target's.
///
/// `success` is omitted when false, as proto3 JSON omits every default.
/// An `aborted` event is Bazel closing out a target it announced but never
/// finished — what a build that stops at its first failure leaves behind.
pub(crate) fn parse_line(line: &str) -> Option<TargetVerdict> {
    let event: serde_json::Value = serde_json::from_str(line).ok()?;
    let label = event
        .get("id")?
        .get("targetCompleted")?
        .get("label")?
        .as_str()?;
    let result = if let Some(completed) = event.get("completed") {
        if completed
            .get("success")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        {
            Ok(())
        } else {
            Err(completed
                .get("failureDetail")
                .and_then(|detail| detail.get("message"))
                .and_then(serde_json::Value::as_str)
                .filter(|message| !message.is_empty())
                .unwrap_or("bazel build failed")
                .to_string())
        }
    } else {
        let aborted = event.get("aborted")?;
        let description = aborted
            .get("description")
            .and_then(serde_json::Value::as_str)
            .filter(|description| !description.is_empty());
        Err(match description {
            Some(description) => {
                format!("bazel stopped before building this target: {description}")
            }
            None => "bazel stopped before building this target".to_string(),
        })
    };
    Some(TargetVerdict {
        label: normalize_label(label),
        result,
    })
}

fn is_last_message(line: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(line)
        .ok()
        .and_then(|event| event.get("lastMessage")?.as_bool())
        == Some(true)
}

/// Read the event file as Bazel writes it, sending each burst of verdicts as
/// it appears, until Bazel's last event or until `exited` fires and the file
/// has been read to its end.
///
/// Returns every verdict seen, keyed by label, so the caller can settle the
/// build from the events rather than from its exit code.
pub(crate) async fn follow(
    output: BepOutput,
    mut exited: oneshot::Receiver<()>,
) -> HashMap<String, Result<(), String>> {
    let mut seen: HashMap<String, Result<(), String>> = HashMap::new();
    let Ok(file) = tokio::fs::File::open(&output.path).await else {
        return seen;
    };
    let mut reader = tokio::io::BufReader::new(file);
    let mut line: Vec<u8> = Vec::new();
    let mut bazel_exited = false;
    loop {
        let mut burst: Vec<TargetVerdict> = Vec::new();
        let mut finished = false;
        // Read every complete line written so far. A line without its
        // newline is still being written: keep it and finish it next pass.
        loop {
            match reader.read_until(b'\n', &mut line).await {
                Ok(0) | Err(_) => break,
                Ok(_) if line.last() != Some(&b'\n') => break,
                Ok(_) => {
                    let text = String::from_utf8_lossy(&line);
                    if let Some(verdict) = parse_line(&text) {
                        seen.insert(verdict.label.clone(), verdict.result.clone());
                        burst.push(verdict);
                    }
                    finished = is_last_message(&text);
                    line.clear();
                    if finished {
                        break;
                    }
                }
            }
        }
        if !burst.is_empty() {
            let _ = output.verdicts.send(burst);
        }
        // Once Bazel has exited, the file is as long as it will get, and the
        // pass just made read it to the end.
        if finished || bazel_exited {
            return seen;
        }
        tokio::select! {
            _ = &mut exited => bazel_exited = true,
            () = tokio::time::sleep(POLL_INTERVAL) => {}
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn normalize_label_matches_what_bazel_reports() {
        struct Case {
            written: &'static str,
            want: &'static str,
        }
        let cases = [
            Case {
                written: "//redo/api/server:local",
                want: "//redo/api/server:local",
            },
            Case {
                written: "@@//redo/api/server:local",
                want: "//redo/api/server:local",
            },
            Case {
                written: "@//redo/api/server:local",
                want: "//redo/api/server:local",
            },
            Case {
                written: "//tools/caddy",
                want: "//tools/caddy:caddy",
            },
            Case {
                written: "//:root",
                want: "//:root",
            },
            Case {
                written: "@@rules_foo+//bin",
                want: "@@rules_foo+//bin:bin",
            },
        ];
        for case in cases {
            assert_eq!(normalize_label(case.written), case.want, "{}", case.written);
        }
    }

    /// Lines as Bazel 9 writes them, trimmed to the fields that matter.
    #[test]
    fn parse_line_reads_target_verdicts_and_nothing_else() {
        struct Case {
            name: &'static str,
            line: &'static str,
            want: Option<TargetVerdict>,
        }
        let ok = |label: &str| {
            Some(TargetVerdict {
                label: label.to_string(),
                result: Ok(()),
            })
        };
        let err = |label: &str, message: &str| {
            Some(TargetVerdict {
                label: label.to_string(),
                result: Err(message.to_string()),
            })
        };
        let cases = [
            Case {
                name: "success",
                line: r#"{"id":{"targetCompleted":{"label":"//a:a","configuration":{"id":"abc"}}},"completed":{"success":true,"outputGroup":[{"name":"default","fileSets":[{"id":"0"}]}]}}"#,
                want: ok("//a:a"),
            },
            Case {
                name: "canonical label",
                line: r#"{"id":{"targetCompleted":{"label":"@@//a:a"}},"completed":{"success":true}}"#,
                want: ok("//a:a"),
            },
            Case {
                name: "failure omits success and carries a reason",
                line: r#"{"id":{"targetCompleted":{"label":"//b:b"}},"completed":{"failureDetail":{"message":"compile failed","spawn":{"code":"NON_ZERO_EXIT"}}}}"#,
                want: err("//b:b", "compile failed"),
            },
            Case {
                name: "failure without a reason",
                line: r#"{"id":{"targetCompleted":{"label":"//b:b"}},"completed":{}}"#,
                want: err("//b:b", "bazel build failed"),
            },
            Case {
                name: "aborted after another target failed",
                line: r#"{"id":{"targetCompleted":{"label":"//c:c"}},"aborted":{"reason":"INCOMPLETE","description":"not built because of an earlier error"}}"#,
                want: err(
                    "//c:c",
                    "bazel stopped before building this target: not built because of an earlier error",
                ),
            },
            Case {
                name: "another event",
                line: r#"{"id":{"targetConfigured":{"label":"//a:a"}},"configured":{"targetKind":"sh_binary rule"}}"#,
                want: None,
            },
            Case {
                name: "not json",
                line: "garbage",
                want: None,
            },
        ];
        for case in cases {
            assert_eq!(parse_line(case.line), case.want, "{}", case.name);
        }
    }

    fn append(path: &std::path::Path, text: &str) {
        let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
        file.write_all(text.as_bytes()).unwrap();
    }

    /// A verdict is sent as soon as its line is complete — not when the
    /// build ends — and a line caught half-written is finished, not dropped.
    #[tokio::test]
    async fn follow_streams_verdicts_while_the_file_grows() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("bep.json");
        std::fs::write(&path, "").unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (exited_tx, exited_rx) = oneshot::channel();
        let follower = tokio::spawn(follow(
            BepOutput {
                path: path.clone(),
                verdicts: tx,
            },
            exited_rx,
        ));

        append(
            &path,
            "{\"id\":{\"started\":{}},\"started\":{}}\n{\"id\":{\"targetCompleted\":{\"label\":\"//a:a\"}},\"completed\":{\"success\":true}}\n",
        );
        let first = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("a completed target is reported while the build runs")
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].label, "//a:a");

        append(
            &path,
            "{\"id\":{\"targetCompleted\":{\"label\":\"//b:b\"}},",
        );
        tokio::time::sleep(POLL_INTERVAL * 3).await;
        assert!(rx.try_recv().is_err(), "half a line is not an event");
        append(&path, "\"completed\":{}}\n");
        let second = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second[0].label, "//b:b");
        assert!(second[0].result.is_err());

        exited_tx.send(()).unwrap();
        let seen = tokio::time::timeout(Duration::from_secs(5), follower)
            .await
            .expect("the follower ends once bazel has exited")
            .unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen.get("//a:a"), Some(&Ok(())));
    }

    /// Bazel's last event ends the read without waiting for the exit.
    #[tokio::test]
    async fn follow_stops_at_the_last_message() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("bep.json");
        std::fs::write(
            &path,
            "{\"id\":{\"targetCompleted\":{\"label\":\"//a:a\"}},\"completed\":{\"success\":true}}\n{\"id\":{\"buildFinished\":{}},\"lastMessage\":true,\"finished\":{}}\n",
        )
        .unwrap();
        let (tx, _rx) = mpsc::unbounded_channel();
        let (_exited_tx, exited_rx) = oneshot::channel();
        let seen = tokio::time::timeout(
            Duration::from_secs(5),
            follow(BepOutput { path, verdicts: tx }, exited_rx),
        )
        .await
        .expect("lastMessage ends the read");
        assert_eq!(seen.len(), 1);
    }
}
