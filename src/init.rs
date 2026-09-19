//! `don init` — scaffold a starter `don.toml` for a new project.
//!
//! Writes a commented template showing each preset (run, docker, rust, go,
//! bazel) plus tasks and profiles. `min_version` is required and uncommented
//! so `don validate` passes on the raw output.

use std::path::Path;

const STARTER_TEMPLATE: &str = r#"# don — dev environment orchestrator.
# Uncomment the sections you want. Each service/task/profile is optional.

# Pick a default profile to limit what `don start` brings up without `--profile`.
# default_profile = "dev"
# Ignore generated files across every watch rule in the workspace.
# watch_ignore = ["target/**", ".don/**"]
# Prefer the configured proxy/Docker host ports, but use OS-assigned ports when
# those ports are already occupied. Discover actual values with `don ports`.
# fallback_ports = true
# Required. Old don binaries ignore this key; this don refuses to start without it.
min_version = "__DON_VERSION__"

# ── Services ────────────────────────────────────────────────────────────────
# Long-running processes. Don keeps them alive and restarts on file changes.

# [services.api]
# run.cmd = "node"
# run.args = ["server.js"]
# proxy = { listen = "127.0.0.1:3000", env = "PORT" }
# watch = ["src/**/*.js"]
# ready.http = "http://127.0.0.1:${PORT}/health"

# Docker preset — run a container via the local Docker daemon.
# [services.postgres]
# docker.image = "postgres:16"
# docker.ports = ["5432:5432"]
# docker.env_file = [".env"]
# ready.tcp = "127.0.0.1:${DON_PUBLIC_PORT}"

# Docker build — build an image from a Dockerfile on demand instead of pulling
# one. No `docker.image` needed: the build is tagged `don-<service>`. (Set
# `docker.image` too if you want an explicit tag.)
# [services.app]
# docker.build.context = "."           # build context dir
# docker.build.dockerfile = "Dockerfile"  # optional, relative to context
# docker.ports = ["8080:8080"]

# Rust preset — runs `cargo build --bin <name>`, watches src/**/*.rs.
# [services.api]
# rust.binary = "api"

# Go preset — runs `go build -o .don/bin/<name> <package>`, watches **/*.go.
# [services.api]
# go.package = "./cmd/api"

# Bazel preset — auto-resolves watch patterns from `bazel query`.
# [services.api]
# bazel.target = "//services/api:api"
# bazel.watch = false  # disable auto-resolved file watches, keep startup build
# proxy = { listen = "127.0.0.1:8080", env = "PORT" }

# ── Tasks ───────────────────────────────────────────────────────────────────
# One-shot commands. Re-run only when watched files change.

# [tasks.migrate]
# cmd = "dbmate"
# args = ["up"]
# env = { DATABASE_PORT = "$(postgres.port)" }
# depends_on = ["postgres"]
# watch = ["db/migrations/**/*.sql"]

# Set auto_run = false to defer execution — when the task needs to run,
# trigger it with `don run <name>`.
# [tasks.seed]
# cmd = "./scripts/seed-db"
# auto_run = false
# depends_on = ["migrate"]

# Set auto_run = "once" to auto-run on startup until the first successful run,
# then require manual triggers forever after.
# [tasks.bootstrap]
# cmd = "./scripts/bootstrap-db"
# auto_run = "once"

# ── Profiles ────────────────────────────────────────────────────────────────
# Named subsets for focused work. Transitive deps are included automatically.

# [profiles.dev]
# services = ["api", "postgres"]
# tasks = ["migrate"]
"#;

/// Write the starter template at `path`. When `force` is false, returns an
/// error if a file already exists there.
pub fn write_starter_config(path: &Path, force: bool) -> Result<(), String> {
    if path.exists() && !force {
        return Err(format!(
            "{} already exists — pass --force to overwrite",
            path.display()
        ));
    }
    std::fs::write(path, starter_template())
        .map_err(|e| format!("failed to write {}: {e}", path.display()))
}

fn starter_template() -> String {
    STARTER_TEMPLATE.replace("__DON_VERSION__", env!("CARGO_PKG_VERSION"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    struct InitTestCase {
        name: &'static str,
        pre_exists: bool,
        force: bool,
        expect_err: bool,
    }

    #[test]
    fn write_starter_config_table() {
        let cases = [
            InitTestCase {
                name: "writes new file",
                pre_exists: false,
                force: false,
                expect_err: false,
            },
            InitTestCase {
                name: "refuses existing file without --force",
                pre_exists: true,
                force: false,
                expect_err: true,
            },
            InitTestCase {
                name: "overwrites with --force",
                pre_exists: true,
                force: true,
                expect_err: false,
            },
        ];

        for case in cases {
            let tmp = tempfile::tempdir().unwrap();
            let path = tmp.path().join("don.toml");
            if case.pre_exists {
                std::fs::write(&path, "existing contents").unwrap();
            }
            let result = write_starter_config(&path, case.force);
            assert_eq!(
                result.is_err(),
                case.expect_err,
                "{}: expected err={} got {:?}",
                case.name,
                case.expect_err,
                result
            );
            if !case.expect_err {
                let contents = std::fs::read_to_string(&path).unwrap();
                assert!(
                    contents.contains("# don — dev environment orchestrator"),
                    "{}: template header missing",
                    case.name
                );
            }
        }
    }

    #[test]
    fn starter_template_is_valid_toml() {
        let template = starter_template();
        let config: crate::config::Config = template.parse().unwrap();
        assert_eq!(
            config.min_version.as_deref(),
            Some(env!("CARGO_PKG_VERSION"))
        );
        config
            .validate(crate::config::Platform::LinuxX86_64)
            .unwrap();
    }
}
