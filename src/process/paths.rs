use crate::globwalk::{
    glob_pattern_base_dir, has_glob_metacharacters, matches_glob, matches_ignore,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

pub(crate) fn working_dir_for(base_dir: &Path, dir: Option<&Path>) -> PathBuf {
    match dir {
        Some(dir) => base_dir.join(dir),
        None => base_dir.to_path_buf(),
    }
}

fn resolve_glob_pattern(base_dir: &Path, pattern: &str) -> String {
    let path = Path::new(pattern);
    if path.is_absolute() {
        path.to_string_lossy().into_owned()
    } else {
        base_dir.join(path).to_string_lossy().into_owned()
    }
}

pub(crate) fn resolve_watch_ignore_patterns(
    process_base_dir: &Path,
    item_ignore_patterns: &[String],
    workspace_base_dir: &Path,
    global_watch_ignore: &[String],
) -> Vec<String> {
    let mut patterns: Vec<String> = item_ignore_patterns
        .iter()
        .map(|pattern| resolve_glob_pattern(process_base_dir, pattern))
        .collect();
    patterns.extend(
        global_watch_ignore
            .iter()
            .map(|pattern| resolve_glob_pattern(workspace_base_dir, pattern)),
    );
    patterns
}

/// Filesystem observations shared only within one post-build batch.
#[derive(Default)]
pub(crate) struct FileChangeScan {
    metadata: HashMap<PathBuf, Option<std::fs::Metadata>>,
    directories: HashMap<PathBuf, Arc<[PathBuf]>>,
}

impl FileChangeScan {
    pub(crate) fn any_changed_since(
        &mut self,
        base_dir: &Path,
        patterns: &[String],
        ignore_patterns: &[String],
        since: SystemTime,
    ) -> bool {
        scan_changed_paths(base_dir, patterns, ignore_patterns, since, self)
    }

    fn metadata(&mut self, path: &Path) -> Option<std::fs::Metadata> {
        self.metadata
            .entry(path.to_path_buf())
            .or_insert_with(|| std::fs::symlink_metadata(path).ok())
            .clone()
    }

    fn entries(&mut self, path: &Path) -> Arc<[PathBuf]> {
        Arc::clone(
            self.directories
                .entry(path.to_path_buf())
                .or_insert_with(|| {
                    std::fs::read_dir(path)
                        .into_iter()
                        .flatten()
                        .flatten()
                        .map(|entry| entry.path())
                        .collect()
                }),
        )
    }
}

#[cfg(test)]
pub(crate) fn any_glob_path_changed_since(
    base_dir: &Path,
    patterns: &[String],
    ignore_patterns: &[String],
    since: SystemTime,
) -> bool {
    FileChangeScan::default().any_changed_since(base_dir, patterns, ignore_patterns, since)
}

fn scan_changed_paths(
    base_dir: &Path,
    patterns: &[String],
    ignore_patterns: &[String],
    since: SystemTime,
    scan: &mut FileChangeScan,
) -> bool {
    let absolute_ignore: Vec<glob::Pattern> = ignore_patterns
        .iter()
        .filter_map(|pattern| glob::Pattern::new(&resolve_glob_pattern(base_dir, pattern)).ok())
        .collect();
    let mut globs = Vec::new();
    for pattern in patterns {
        let resolved = PathBuf::from(resolve_glob_pattern(base_dir, pattern));
        if has_glob_metacharacters(&resolved) {
            if let Ok(pattern) = glob::Pattern::new(&resolved.to_string_lossy()) {
                globs.push((glob_pattern_base_dir(&resolved), pattern));
            }
        } else {
            // Literal BUILD paths need no directory walk, even when they are absent.
            if resolved.parent().is_some_and(|parent| {
                is_ignored(parent, &absolute_ignore)
                    || scan
                        .metadata(parent)
                        .is_some_and(|metadata| metadata.file_type().is_symlink())
            }) || is_ignored(&resolved, &absolute_ignore)
            {
                continue;
            }
            if scan
                .metadata(&resolved)
                .and_then(|metadata| metadata.modified().ok())
                .is_some_and(|modified| modified > since)
            {
                return true;
            }
        }
    }

    globs.sort_by(|(a, _), (b, _)| a.cmp(b));
    let mut walks: Vec<(PathBuf, Vec<glob::Pattern>)> = Vec::new();
    for (root, pattern) in globs {
        if let Some((_, patterns)) = walks.iter_mut().find(|(kept, _)| root.starts_with(kept)) {
            patterns.push(pattern);
        } else {
            walks.push((root, vec![pattern]));
        }
    }

    walks.into_iter().any(|(root, patterns)| {
        scan_tree_for_changes(&root, &patterns, &absolute_ignore, since, scan)
    })
}

fn is_ignored(path: &Path, patterns: &[glob::Pattern]) -> bool {
    let path = path.to_string_lossy();
    patterns
        .iter()
        .any(|pattern| matches_ignore(pattern, &path))
}

fn scan_tree_for_changes(
    path: &Path,
    patterns: &[glob::Pattern],
    ignore_patterns: &[glob::Pattern],
    since: SystemTime,
    scan: &mut FileChangeScan,
) -> bool {
    if is_ignored(path, ignore_patterns) {
        return false;
    }
    let Some(metadata) = scan.metadata(path) else {
        return false;
    };
    let path_str = path.to_string_lossy();
    if patterns
        .iter()
        .any(|pattern| matches_glob(pattern, &path_str))
    {
        let Ok(modified) = metadata.modified() else {
            return false;
        };
        if modified > since {
            return true;
        }
    }

    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return false;
    }

    let entries = scan.entries(path);
    for entry in entries.iter() {
        if scan_tree_for_changes(entry, patterns, ignore_patterns, since, scan) {
            return true;
        }
    }

    false
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::Duration;

    #[test]
    fn change_scan_preserves_matching_and_ignores() {
        struct Case {
            patterns: &'static [&'static str],
            ignore: &'static [&'static str],
            changed_file: &'static str,
            want: bool,
        }
        let cases = [
            Case {
                patterns: &["BUILD.bazel"],
                ignore: &[],
                changed_file: "BUILD.bazel",
                want: true,
            },
            Case {
                patterns: &["BUILD.bazel"],
                ignore: &[],
                changed_file: "src/app.rs",
                want: false,
            },
            Case {
                patterns: &["missing/BUILD"],
                ignore: &[],
                changed_file: "src/app.rs",
                want: false,
            },
            Case {
                patterns: &["src/app.rs"],
                ignore: &["src/app.rs"],
                changed_file: "src/app.rs",
                want: false,
            },
            Case {
                patterns: &["src/app.rs"],
                ignore: &["src"],
                changed_file: "src/app.rs",
                want: false,
            },
            Case {
                patterns: &["src/*.rs"],
                ignore: &[],
                changed_file: "src/nested/deep.rs",
                want: false,
            },
            Case {
                patterns: &["src/**/*.rs"],
                ignore: &[],
                changed_file: "src/nested/deep.rs",
                want: true,
            },
            Case {
                patterns: &["src/**/*.rs"],
                ignore: &["src/nested/**"],
                changed_file: "src/nested/deep.rs",
                want: false,
            },
            Case {
                patterns: &["src/**/*.rs", "src/nested/*.ts"],
                ignore: &[],
                changed_file: "src/nested/deep.ts",
                want: true,
            },
            Case {
                patterns: &["src/**/*.rs", "src/nested/*.ts", "src/**/*.rs"],
                ignore: &[],
                changed_file: "src/nested/deep.ts",
                want: true,
            },
            Case {
                patterns: &["src/**/*.rs", "src/nested/*.ts", "src/**/*.rs"],
                ignore: &["src/nested/**"],
                changed_file: "src/nested/deep.ts",
                want: false,
            },
            Case {
                patterns: &["src/**/*.rs", "src/nested/*.ts"],
                ignore: &["src/nested"],
                changed_file: "src/nested/deep.ts",
                want: false,
            },
            Case {
                patterns: &["BUILD.bazel", "src/**/*.rs"],
                ignore: &[],
                changed_file: "src/app.rs",
                want: true,
            },
        ];
        for case in cases {
            let temp = tempfile::tempdir().unwrap();
            let repo = temp.path();
            fs::create_dir_all(repo.join("src/nested")).unwrap();
            for file in [
                "BUILD.bazel",
                "src/app.rs",
                "src/nested/deep.rs",
                "src/nested/deep.ts",
            ] {
                fs::write(repo.join(file), "content").unwrap();
            }
            let since = SystemTime::now() + Duration::from_secs(60);
            fs::File::options()
                .write(true)
                .open(repo.join(case.changed_file))
                .unwrap()
                .set_times(fs::FileTimes::new().set_modified(since + Duration::from_secs(60)))
                .unwrap();
            let patterns = case
                .patterns
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>();
            let ignore = case
                .ignore
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>();
            assert_eq!(
                any_glob_path_changed_since(repo, &patterns, &ignore, since),
                case.want,
                "patterns={:?}, ignore={:?}, changed={}",
                case.patterns,
                case.ignore,
                case.changed_file
            );
        }
    }

    #[test]
    fn literal_scan_does_not_walk_parent_directories() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path();
        fs::create_dir_all(repo.join("pkg/src/nested")).unwrap();
        fs::write(repo.join("pkg/BUILD.bazel"), "").unwrap();
        fs::write(repo.join("pkg/src/nested/unrelated.rs"), "").unwrap();
        let cases = [
            vec!["pkg/BUILD.bazel".to_string()],
            vec!["pkg/BUILD".to_string()],
            vec![repo.join("pkg/BUILD.bazel").to_string_lossy().into_owned()],
            vec!["pkg/BUILD".to_string(), "pkg/BUILD.bazel".to_string()],
        ];
        for patterns in cases {
            let mut scan = FileChangeScan::default();
            assert!(!scan.any_changed_since(
                repo,
                &patterns,
                &[],
                SystemTime::now() + Duration::from_secs(60),
            ));
            assert!(scan.directories.is_empty(), "patterns={patterns:?}");
        }
    }

    #[test]
    fn shared_scan_keeps_each_services_patterns_ignores_and_cutoff() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path();
        fs::create_dir(repo.join("src")).unwrap();
        fs::write(repo.join("src/app.rs"), "").unwrap();
        let since = SystemTime::now() + Duration::from_secs(60);
        let modified = since + Duration::from_secs(60);
        fs::File::options()
            .write(true)
            .open(repo.join("src/app.rs"))
            .unwrap()
            .set_times(fs::FileTimes::new().set_modified(modified))
            .unwrap();
        let mut scan = FileChangeScan::default();
        for (pattern, ignore, cutoff, want) in [
            ("src/**/*.ts", "", since, false),
            ("src/**/*.rs", "src/app.rs", since, false),
            ("src/**/*.rs", "", since, true),
            ("src/*.rs", "", modified, false),
            ("src/app.rs", "src", since, false),
            ("src/app.rs", "", since, true),
        ] {
            let ignore = if ignore.is_empty() {
                Vec::new()
            } else {
                vec![ignore.into()]
            };
            assert_eq!(
                scan.any_changed_since(repo, &[pattern.into()], &ignore, cutoff),
                want,
                "pattern={pattern}, ignore={ignore:?}, cutoff={cutoff:?}"
            );
        }
    }

    #[test]
    fn scan_reuses_observations_until_next_batch() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path();
        fs::create_dir(repo.join("src")).unwrap();
        fs::write(repo.join("src/app.rs"), "").unwrap();
        let since = SystemTime::now() + Duration::from_secs(60);
        let patterns = ["src/app.rs", "src/*.ts", "src/new.ts"];
        let mut first = FileChangeScan::default();
        for pattern in patterns {
            assert!(!first.any_changed_since(repo, &[pattern.into()], &[], since));
        }
        for file in ["src/app.rs", "src/new.ts"] {
            fs::write(repo.join(file), "changed").unwrap();
            fs::File::options()
                .write(true)
                .open(repo.join(file))
                .unwrap()
                .set_times(fs::FileTimes::new().set_modified(since + Duration::from_secs(60)))
                .unwrap();
        }
        for pattern in patterns {
            assert!(!first.any_changed_since(repo, &[pattern.into()], &[], since));
        }
        drop(first);
        let mut second = FileChangeScan::default();
        for pattern in patterns {
            assert!(second.any_changed_since(repo, &[pattern.into()], &[], since));
        }
        fs::remove_file(repo.join("src/new.ts")).unwrap();
        for pattern in ["src/*.ts", "src/new.ts"] {
            assert!(second.any_changed_since(repo, &[pattern.into()], &[], since));
        }
        drop(second);
        let mut third = FileChangeScan::default();
        for pattern in ["src/*.ts", "src/new.ts"] {
            assert!(!third.any_changed_since(repo, &[pattern.into()], &[], since));
        }
    }

    #[test]
    fn scan_does_not_follow_symlinked_directories() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path();
        fs::create_dir_all(repo.join("real")).unwrap();
        fs::write(repo.join("real/BUILD.bazel"), "").unwrap();
        std::os::unix::fs::symlink(repo.join("real"), repo.join("link")).unwrap();
        for pattern in ["link/BUILD.bazel", "link/**/*.bazel"] {
            assert!(!any_glob_path_changed_since(
                repo,
                &[pattern.to_string()],
                &[],
                SystemTime::UNIX_EPOCH
            ));
        }
    }
}
