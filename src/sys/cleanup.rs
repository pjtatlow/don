//! Stale state cleanup for crash recovery.
//!
//! When don crashes, child process groups survive as orphans and pid files
//! linger. This module scans `.don/pids/`, verifies each entry's identity
//! (PGID + start_time) against the running process table, kills confirmed
//! orphans, and removes stale files. Also cleans up leftover sockets and
//! Docker containers.

use super::identity;
use nix::sys::signal::{Signal, killpg};
use nix::unistd::Pid;
use std::fmt;
use std::path::Path;

/// Summary of what cleanup found and did.
pub struct CleanupReport {
    /// Number of pid files removed from `.don/pids/`.
    pub pid_files_removed: usize,
    /// Number of process groups actually killed (identity matched).
    pub pids_killed: usize,
    /// Whether a stale `.don/don.sock` was removed.
    pub sock_removed: bool,
    /// Number of Docker containers removed.
    pub containers_removed: usize,
    /// Non-fatal warnings encountered during cleanup (e.g. "docker socket
    /// permission denied"). The CLI surfaces these to the user; the daemon
    /// routes them through its `OutputManager`.
    pub warnings: Vec<String>,
}

impl fmt::Display for CleanupReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts = Vec::new();
        if self.pids_killed > 0 {
            parts.push(format!(
                "killed {} orphaned process group{}",
                self.pids_killed,
                if self.pids_killed == 1 { "" } else { "s" }
            ));
        }
        if self.pid_files_removed > 0 {
            parts.push(format!(
                "removed {} stale pid file{}",
                self.pid_files_removed,
                if self.pid_files_removed == 1 { "" } else { "s" }
            ));
        }
        if self.sock_removed {
            parts.push("removed stale don.sock".to_string());
        }
        if self.containers_removed > 0 {
            parts.push(format!(
                "removed {} stale container{}",
                self.containers_removed,
                if self.containers_removed == 1 {
                    ""
                } else {
                    "s"
                }
            ));
        }
        if parts.is_empty() {
            write!(f, "no stale state found")
        } else {
            write!(f, "{}", parts.join(", "))
        }
    }
}

/// Scan `.don/` for stale state from a previous (possibly crashed) don run
/// and clean it up. Best-effort — individual failures are logged but don't
/// abort the overall cleanup.
///
/// `docker_names` is the list of docker container names to check (derived
/// from `docker.container` or `"don-{service_name}"` in the config).
pub async fn run_cleanup(base_dir: &Path, docker_names: &[String]) -> CleanupReport {
    let mut report = CleanupReport {
        pid_files_removed: 0,
        pids_killed: 0,
        sock_removed: false,
        containers_removed: 0,
        warnings: Vec::new(),
    };

    // 1. Scan pid files.
    let pids_dir = base_dir.join(".don").join("pids");
    if pids_dir.is_dir()
        && let Ok(entries) = std::fs::read_dir(&pids_dir)
    {
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            cleanup_pid_file(&path, &mut report).await;
        }
    }

    // 2. Stale socket.
    let sock_path = base_dir.join(".don").join("don.sock");
    if sock_path.exists() {
        report.sock_removed = cleanup_socket(&sock_path);
    }

    // 3. Docker containers.
    if !docker_names.is_empty() {
        cleanup_docker_containers(docker_names, &mut report).await;
    }

    report
}

/// Process a single pid file: read identity, kill if still alive, remove file.
async fn cleanup_pid_file(path: &Path, report: &mut CleanupReport) {
    let ident = match super::read_pid_file_identity(path).await {
        Ok(Some(id)) => id,
        Ok(None) => return, // empty or doesn't exist
        Err(_) => {
            // Corrupt file — remove it.
            let _ = std::fs::remove_file(path);
            report.pid_files_removed += 1;
            return;
        }
    };

    let recorded_at = std::fs::metadata(path).and_then(|meta| meta.modified());
    if identity::still_alive(&ident)
        || recorded_at.is_ok_and(|at| identity::orphaned_group(&ident, at))
    {
        // Confirmed orphan — same PGID + same start_time, or a group whose
        // leader is gone but whose members remain. Kill it.
        let _ = killpg(Pid::from_raw(ident.pgid), Signal::SIGKILL);
        // Brief wait for the process group to actually exit.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        report.pids_killed += 1;
    }

    let _ = std::fs::remove_file(path);
    report.pid_files_removed += 1;
}

/// Try to connect to a unix socket. If the connection is refused (nobody
/// listening), the socket file is stale — remove it. If the connection
/// succeeds, a daemon is running — leave it alone.
fn cleanup_socket(sock_path: &Path) -> bool {
    match std::os::unix::net::UnixStream::connect(sock_path) {
        Ok(_stream) => {
            // Daemon is running on this socket.
            false
        }
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
            ) =>
        {
            let _ = std::fs::remove_file(sock_path);
            true
        }
        Err(_) => {
            // Some other error (permissions, etc.) — leave it alone.
            false
        }
    }
}

/// Try to clean up stale Docker containers. Best-effort — if Docker isn't
/// available, we skip silently.
async fn cleanup_docker_containers(names: &[String], report: &mut CleanupReport) {
    let client = match bollard::Docker::connect_with_socket_defaults() {
        Ok(c) => c,
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("permission") || msg.contains("EACCES") {
                report.warnings.push(
                    "docker cleanup skipped — permission denied on docker socket".to_string(),
                );
            }
            // Otherwise Docker is just not installed/running — skip silently.
            return;
        }
    };
    for name in names {
        if let Ok(true) = crate::docker::cleanup_stale_container(&client, name).await {
            report.containers_removed += 1;
        }
    }
}
