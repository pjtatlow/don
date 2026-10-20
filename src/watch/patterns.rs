//! An item's watch globs, indexed so matching a path costs its depth rather
//! than the number of globs.

use crate::globwalk::matches_glob;
use glob::Pattern;
use std::collections::HashMap;

/// An item's watch globs. A literal glob matches only itself and a
/// `<literal dir>/**` glob only paths under `<dir>/`, so both are looked up.
pub(crate) struct WatchPatterns {
    patterns: Vec<Pattern>,
    /// Patterns with no metacharacters, to their index in `patterns`.
    literals: HashMap<String, usize>,
    /// `<dir>/` of each `<dir>/**` pattern, to its index in `patterns`.
    subtrees: HashMap<String, usize>,
    /// Indices of the patterns that must be matched as globs.
    globs: Vec<usize>,
}

impl WatchPatterns {
    pub(crate) fn new(patterns: Vec<Pattern>) -> Self {
        let mut literals = HashMap::new();
        let mut subtrees = HashMap::new();
        let mut globs = Vec::new();
        for (index, pattern) in patterns.iter().enumerate() {
            let pattern = pattern.as_str();
            if is_literal(pattern) {
                literals.entry(pattern.to_string()).or_insert(index);
            } else if let Some(prefix) = subtree_prefix(pattern) {
                subtrees.entry(prefix.to_string()).or_insert(index);
            } else {
                globs.push(index);
            }
        }
        Self {
            patterns,
            literals,
            subtrees,
            globs,
        }
    }

    pub(crate) fn iter(&self) -> std::slice::Iter<'_, Pattern> {
        self.patterns.iter()
    }

    /// A pattern matching `path`, if any; agrees with [`matches_glob`].
    pub(crate) fn find_match(&self, path: &str) -> Option<&Pattern> {
        let indexed = self.literals.get(path).or_else(|| {
            path.match_indices('/')
                .find_map(|(slash, _)| self.subtrees.get(&path[..=slash]))
        });
        indexed
            .or_else(|| {
                self.globs
                    .iter()
                    .find(|&&index| matches_glob(&self.patterns[index], path))
            })
            .map(|&index| &self.patterns[index])
    }
}

fn is_literal(pattern: &str) -> bool {
    !pattern.contains(['*', '?', '[', ']'])
}

/// The `<dir>/` a `<dir>/**` glob matches under, when `<dir>` is literal.
fn subtree_prefix(pattern: &str) -> Option<&str> {
    let prefix = pattern.strip_suffix("**")?;
    (prefix.ends_with('/') && is_literal(prefix)).then_some(prefix)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    /// Indexing must not change what matches.
    #[test]
    fn test_find_match_agrees_with_glob_matching() {
        let patterns: Vec<Pattern> = [
            "/repo/a/**",
            "/repo/a/b/**",
            "/repo/ab/**",
            "/repo/c/BUILD",
            "/repo/c/BUILD.bazel",
            "/repo/d/**/*.ts",
            "/repo/e/*",
            "/repo/[f]/**",
            "/repo/g?/**",
            "/repo/h/*/**",
        ]
        .iter()
        .map(|p| Pattern::new(p).unwrap())
        .collect();
        let index = WatchPatterns::new(patterns.clone());

        let paths = [
            "/repo/a",
            "/repo/a/",
            "/repo/a/x.ts",
            "/repo/a/b/c/d.ts",
            "/repo/ab/x",
            "/repo/abc/x",
            "/repo/c/BUILD",
            "/repo/c/BUILD.bazel",
            "/repo/c/BUILD.bazelrc",
            "/repo/c/BUIL",
            "/repo/c/other",
            "/repo/d/x.ts",
            "/repo/d/deep/x.ts",
            "/repo/d/x.js",
            "/repo/e/x",
            "/repo/e/x/y",
            "/repo/f/x",
            "/repo/[f]/x",
            "/repo/g1/x",
            "/repo/h/x/y",
            "/repo/h/x",
            "/elsewhere/a/x",
            "",
        ];
        for path in paths {
            let linear = patterns.iter().any(|p| matches_glob(p, path));
            let found = index.find_match(path);
            assert_eq!(found.is_some(), linear, "path {path:?}");
            if let Some(pattern) = found {
                assert!(
                    matches_glob(pattern, path),
                    "{path:?} vs {}",
                    pattern.as_str()
                );
            }
        }
    }

    #[test]
    fn test_subtree_prefix() {
        struct Case {
            pattern: &'static str,
            want: Option<&'static str>,
        }
        let cases = [
            Case {
                pattern: "/repo/pkg/**",
                want: Some("/repo/pkg/"),
            },
            Case {
                pattern: "/repo/pkg/**/*.ts",
                want: None,
            },
            Case {
                pattern: "/repo/p*/**",
                want: None,
            },
            Case {
                pattern: "/repo/[p]/**",
                want: None,
            },
            Case {
                pattern: "/repo/pkg**",
                want: None,
            },
            Case {
                pattern: "/repo/pkg/BUILD",
                want: None,
            },
        ];
        for case in cases {
            assert_eq!(subtree_prefix(case.pattern), case.want, "{}", case.pattern);
        }
    }
}
