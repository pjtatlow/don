//! Per-target dependency graph parsed from `bazel query --output=streamed_proto`.
//!
//! A single union query (`deps(T1 + T2 + ... + Tn)`) returns every rule and
//! source file reachable from any of the given targets. We stream-parse the
//! protobuf messages into an adjacency list keyed by rule label, then DFS per
//! target to get its own first-party source packages.
//!
//! External (`@...`-prefixed) labels are skipped during parsing: we're only
//! watching first-party source files. Traversals through external intermediary
//! rules terminate there, preserving the existing watch-attribution behavior.

use prost::Message;
use std::collections::{HashMap, HashSet};

use crate::build_tool::BuildToolError;

const TOOL: &str = "bazel";

/// Parsed first-party slice of a Bazel dependency graph.
pub(super) struct BazelDepGraph {
    /// Rule label -> deduplicated list of direct `rule-input` labels.
    rules: HashMap<String, Vec<String>>,
    /// First-party source file labels seen in the query output.
    sources: HashSet<String>,
}

impl BazelDepGraph {
    /// Decode Bazel's length-delimited Target messages, retaining the same
    /// first-party rule edges and source labels as the dependency query.
    pub(super) fn parse_proto(mut bytes: &[u8]) -> Result<Self, BuildToolError> {
        let mut rules = HashMap::new();
        let mut sources = HashSet::new();
        while !bytes.is_empty() {
            let target = QueryTarget::decode_length_delimited(&mut bytes).map_err(|error| {
                BuildToolError::ParseError {
                    tool: TOOL.to_string(),
                    message: format!("invalid dependency query protobuf: {error}"),
                }
            })?;
            if let Some(rule) = target.rule
                && !is_external(&rule.name)
            {
                let mut inputs = rule.inputs;
                inputs.sort_unstable();
                inputs.dedup();
                rules.insert(rule.name, inputs);
            }
            if let Some(source) = target.source
                && !is_external(&source.name)
            {
                sources.insert(source.name);
            }
        }
        Ok(Self { rules, sources })
    }

    /// First-party package directories that feed into `target`.
    ///
    /// DFS from `target` through `rule-input` edges, collecting every
    /// first-party source file reached. Each source file contributes its
    /// package (the path portion of its label).
    ///
    /// Returns sorted, deduplicated package paths. Empty if the target is
    /// unknown or has no first-party sources.
    ///
    /// Two package paths are filtered out even if reachable:
    /// - **The empty (root) package.** A service that reaches `//:foo.txt`
    ///   would otherwise contribute `""`, which downstream becomes `/**` —
    ///   an absolute pattern matching the entire filesystem. Root-level
    ///   BUILD / WORKSPACE / MODULE.bazel files are already covered by the
    ///   tier-1 build-graph watch; other root-level source files lose
    ///   coverage here, and that's the right tradeoff given the blast
    ///   radius of watching the whole repo root.
    /// - **`bazel-out/...` packages.** These are generated-file outputs,
    ///   not source files we should watch.
    pub(super) fn packages_for(&self, target: &str) -> Vec<String> {
        let mut visited: HashSet<&str> = HashSet::new();
        let mut stack: Vec<&str> = vec![target];
        let mut packages: HashSet<&str> = HashSet::new();

        while let Some(label) = stack.pop() {
            if !visited.insert(label) {
                continue;
            }
            if self.sources.contains(label) {
                if let Some(pkg) = package_of(label)
                    && !pkg.is_empty()
                    && !pkg.starts_with("bazel-out")
                {
                    packages.insert(pkg);
                }
                continue;
            }
            if let Some(deps) = self.rules.get(label) {
                for d in deps {
                    stack.push(d.as_str());
                }
            }
            // Missing rule (e.g. external, pruned during parse) terminates
            // the walk for this path. Intentional.
        }

        let mut out: Vec<String> = packages.into_iter().map(String::from).collect();
        out.sort();
        out
    }
}

fn is_external(label: &str) -> bool {
    label.starts_with('@')
}

/// Extract the package portion of a Bazel label:
/// `//a/b/c:target` -> `a/b/c`; `//a/b/c` -> `a/b/c`. Returns `None` if the
/// label is not absolute (`@` or relative).
fn package_of(label: &str) -> Option<&str> {
    let stripped = label.strip_prefix("//")?;
    let end = stripped.find(':').unwrap_or(stripped.len());
    Some(&stripped[..end])
}

// Only the fields Don consumes from Bazel's stable build.proto schema:
// https://github.com/bazelbuild/bazel/blob/9.3.0/src/main/protobuf/build.proto
// Prost skips unrelated attributes, generated targets, and future fields.
#[derive(Clone, PartialEq, Message)]
struct QueryTarget {
    #[prost(message, optional, tag = "2")]
    rule: Option<QueryRule>,
    #[prost(message, optional, tag = "3")]
    source: Option<QuerySource>,
}

#[derive(Clone, PartialEq, Message)]
struct QueryRule {
    #[prost(string, required, tag = "1")]
    name: String,
    #[prost(string, repeated, tag = "5")]
    inputs: Vec<String>,
}

#[derive(Clone, PartialEq, Message)]
struct QuerySource {
    #[prost(string, required, tag = "1")]
    name: String,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn parse(sources: &[&str], rules: &[(&str, &[&str])]) -> BazelDepGraph {
        let mut bytes = Vec::new();
        for name in sources {
            QueryTarget {
                rule: None,
                source: Some(QuerySource {
                    name: (*name).into(),
                }),
            }
            .encode_length_delimited(&mut bytes)
            .unwrap();
        }
        for (name, inputs) in rules {
            QueryTarget {
                rule: Some(QueryRule {
                    name: (*name).into(),
                    inputs: inputs.iter().map(|input| (*input).into()).collect(),
                }),
                source: None,
            }
            .encode_length_delimited(&mut bytes)
            .unwrap();
        }
        BazelDepGraph::parse_proto(&bytes).unwrap()
    }

    #[test]
    fn test_package_of() {
        struct Case {
            name: &'static str,
            input: &'static str,
            expected: Option<&'static str>,
        }

        let cases = vec![
            Case {
                name: "label with target",
                input: "//a/b/c:target",
                expected: Some("a/b/c"),
            },
            Case {
                name: "label without target",
                input: "//a/b/c",
                expected: Some("a/b/c"),
            },
            Case {
                name: "root package with target",
                input: "//:root",
                expected: Some(""),
            },
            Case {
                name: "nested source path",
                input: "//a/b:sub/dir/file.sh",
                expected: Some("a/b"),
            },
            Case {
                name: "external label",
                input: "@repo//a:b",
                expected: None,
            },
            Case {
                name: "relative label",
                input: ":target",
                expected: None,
            },
        ];

        for c in cases {
            assert_eq!(package_of(c.input), c.expected, "case: {}", c.name);
        }
    }

    #[test]
    fn test_parse_basic_graph() {
        let g = parse(
            &["//pkg/a:src.go", "//pkg/b:lib.go"],
            &[
                ("//pkg/a:bin", &["//pkg/a:src.go", "//pkg/b:lib"]),
                ("//pkg/b:lib", &["//pkg/b:lib.go"]),
            ],
        );
        assert_eq!(g.rules.len(), 2);
        assert_eq!(g.sources.len(), 2);
        assert!(g.sources.contains("//pkg/a:src.go"));
        assert!(g.sources.contains("//pkg/b:lib.go"));

        let packages = g.packages_for("//pkg/a:bin");
        assert_eq!(packages, vec!["pkg/a".to_string(), "pkg/b".to_string()]);

        let packages = g.packages_for("//pkg/b:lib");
        assert_eq!(packages, vec!["pkg/b".to_string()]);
    }

    #[test]
    fn test_filters_external_labels() {
        let g = parse(
            &["//pkg/a:src.go", "@rules_go//foo:ext.go"],
            &[
                ("//pkg/a:bin", &["//pkg/a:src.go", "@rules_go//foo:dep"]),
                ("@rules_go//foo:dep", &["@rules_go//foo:ext.go"]),
            ],
        );
        assert_eq!(g.rules.len(), 1, "only first-party rules kept");
        assert_eq!(g.sources.len(), 1, "only first-party sources kept");
        assert!(g.sources.contains("//pkg/a:src.go"));

        // Even though //pkg/a:bin has an edge to @rules_go//foo:dep, that edge
        // terminates at a missing rule — DFS stops and we don't accidentally
        // pick up external sources.
        let packages = g.packages_for("//pkg/a:bin");
        assert_eq!(packages, vec!["pkg/a".to_string()]);
    }

    #[test]
    fn test_dedup_rule_inputs() {
        let g = parse(
            &["//pkg/a:src.go"],
            &[(
                "//pkg/a:bin",
                &["//pkg/a:src.go", "//pkg/a:src.go", "//pkg/a:src.go"],
            )],
        );
        let inputs = g.rules.get("//pkg/a:bin").unwrap();
        assert_eq!(inputs.len(), 1);
    }

    #[test]
    fn test_cycle_safe() {
        // Not a real Bazel scenario, but defensively handle it anyway.
        let g = parse(
            &["//pkg/a:src.go"],
            &[
                ("//pkg/a:x", &["//pkg/a:y", "//pkg/a:src.go"]),
                ("//pkg/a:y", &["//pkg/a:x"]),
            ],
        );
        // DFS with visited set must not loop.
        let packages = g.packages_for("//pkg/a:x");
        assert_eq!(packages, vec!["pkg/a".to_string()]);
    }

    #[test]
    fn test_filters_root_and_bazel_out_packages() {
        // A service that reaches //:config.yaml (root package) and
        // //bazel-out/k8-fastbuild/bin/gen:out.go (generated) plus a normal
        // first-party source. Only the normal one should survive.
        let g = parse(
            &[
                "//:config.yaml",
                "//bazel-out/k8-fastbuild/bin/gen:out.go",
                "//pkg/a:src.go",
            ],
            &[(
                "//pkg/a:bin",
                &[
                    "//:config.yaml",
                    "//bazel-out/k8-fastbuild/bin/gen:out.go",
                    "//pkg/a:src.go",
                ],
            )],
        );
        let packages = g.packages_for("//pkg/a:bin");
        assert_eq!(
            packages,
            vec!["pkg/a".to_string()],
            "root `` and bazel-out packages must be filtered out — otherwise watch paths become /** and watch the whole filesystem"
        );
    }

    #[test]
    fn test_unknown_target() {
        let g = parse(&["//pkg/a:src.go"], &[("//pkg/a:bin", &["//pkg/a:src.go"])]);
        assert!(g.packages_for("//nope:missing").is_empty());
    }

    #[test]
    fn test_real_bazel_wire_format() {
        let bytes = include_bytes!("../../../tests/fixtures/bazel-deps.pb");
        let graph = BazelDepGraph::parse_proto(bytes).unwrap();
        for (target, expected) in [
            ("//:all", vec!["branches"]),
            ("//:roundtrip", vec![]),
            ("//aliases:chosen", vec!["branches"]),
            ("//branches:selected", vec!["branches"]),
            ("//generated:generator", vec!["generated"]),
        ] {
            assert_eq!(graph.packages_for(target), expected, "{target}");
        }
        for invalid in [&[0xff][..], &bytes[..bytes.len() - 1]] {
            assert!(matches!(
                BazelDepGraph::parse_proto(invalid),
                Err(BuildToolError::ParseError { .. })
            ));
        }
        assert!(BazelDepGraph::parse_proto(&[]).unwrap().rules.is_empty());
    }
}
