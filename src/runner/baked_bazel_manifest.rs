use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};

pub(super) const FILE_NAME: &str = "bazel-launch-manifest.json";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BakedBazelManifest {
    version: u32,
    commit: String,
    targets: HashMap<String, PathBuf>,
}

impl BakedBazelManifest {
    pub(super) fn load(don_dir: &Path) -> Result<Option<Self>, String> {
        let path = don_dir.join(FILE_NAME);
        let content = match std::fs::read_to_string(&path) {
            Ok(content) => content,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(format!("failed to read {}: {error}", path.display())),
        };

        let manifest: Self = serde_json::from_str(&content)
            .map_err(|error| format!("failed to parse {}: {error}", path.display()))?;
        if manifest.version != 1 {
            return Err(format!(
                "{} has unsupported version {} (expected 1)",
                path.display(),
                manifest.version
            ));
        }
        if manifest.commit.is_empty() {
            return Err(format!("{} has an empty commit", path.display()));
        }

        Ok(Some(manifest))
    }

    pub(super) fn matches_commit(&self, current_commit: &str) -> bool {
        self.commit == current_commit
    }

    pub(super) fn executable_for_target(
        &self,
        target: &str,
        workspace: &Path,
    ) -> Result<PathBuf, String> {
        let path = self
            .targets
            .get(target)
            .or_else(|| {
                target
                    .strip_prefix("@@")
                    .and_then(|short| self.targets.get(short))
            })
            .or_else(|| self.targets.get(&format!("@@{target}")))
            .ok_or_else(|| format!("manifest has no executable for {target}"))?;

        if path.is_absolute()
            || path
                .components()
                .any(|component| matches!(component, Component::ParentDir))
        {
            return Err(format!(
                "manifest executable for {target} must be a workspace-relative path"
            ));
        }

        let executable = workspace.join(path);
        let metadata = std::fs::metadata(&executable).map_err(|error| {
            format!(
                "manifest executable for {target} is unavailable at {}: {error}",
                executable.display()
            )
        })?;
        if !metadata.is_file() {
            return Err(format!(
                "manifest executable for {target} is not a file: {}",
                executable.display()
            ));
        }

        Ok(executable)
    }
}

#[cfg(test)]
mod tests {
    use super::BakedBazelManifest;
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn manifest(target_path: &str) -> BakedBazelManifest {
        BakedBazelManifest {
            version: 1,
            commit: "abc123".to_string(),
            targets: [("//app:server".to_string(), PathBuf::from(target_path))]
                .into_iter()
                .collect::<HashMap<_, _>>(),
        }
    }

    #[test]
    fn canonical_target_uses_stripped_manifest_key() {
        let workspace = tempfile::tempdir().unwrap();
        let executable = workspace.path().join("bazel-out/bin/server");
        std::fs::create_dir_all(executable.parent().unwrap()).unwrap();
        std::fs::write(&executable, "#!/bin/sh\n").unwrap();

        assert_eq!(
            manifest("bazel-out/bin/server")
                .executable_for_target("@@//app:server", workspace.path())
                .unwrap(),
            executable
        );
    }

    #[test]
    fn parent_path_is_rejected_even_when_it_exists() {
        let workspace = tempfile::tempdir().unwrap();
        let error = manifest("../outside")
            .executable_for_target("//app:server", workspace.path())
            .unwrap_err();

        assert!(error.contains("workspace-relative"));
    }
}
