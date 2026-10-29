//! CLI-side supervision while the background runner is initializing.

use std::path::Path;
use std::process::Child;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncSeekExt, BufReader};
use tokio::signal::unix::{SignalKind, signal};

/// Keep signal forwarding alive through startup and the attached session.
pub(crate) struct SignalForwarder(tokio::task::JoinHandle<()>);

impl SignalForwarder {
    pub(crate) fn new(child_pid: u32) -> Result<Self, String> {
        // Register before returning, before raw mode or the first socket poll.
        let mut sigint = signal(SignalKind::interrupt()).map_err(|e| e.to_string())?;
        let mut sigterm = signal(SignalKind::terminate()).map_err(|e| e.to_string())?;
        Ok(Self(tokio::spawn(async move {
            loop {
                tokio::select! {
                    received = sigint.recv() => { if received.is_none() { return; } }
                    received = sigterm.recv() => { if received.is_none() { return; } }
                }
                let _ = nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(child_pid as i32),
                    nix::sys::signal::Signal::SIGINT,
                );
            }
        })))
    }
}

impl Drop for SignalForwarder {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Show only this invocation's output, even though runner.log is append-only.
pub(crate) async fn wait_for_runner(
    child: &mut Child,
    base: &Path,
    log_path: &Path,
    log_offset: u64,
) -> Result<(), String> {
    let (progress_tx, progress_rx) = tokio::sync::watch::channel(Vec::new());
    let work = async {
        let file = tokio::fs::File::open(log_path)
            .await
            .map_err(|e| format!("failed to read {}: {e}", log_path.display()))?;
        let mut log = BufReader::new(file);
        log.seek(std::io::SeekFrom::Start(log_offset))
            .await
            .map_err(|e| format!("failed to seek {}: {e}", log_path.display()))?;
        let client = don::client::Client::new(base);
        let start = tokio::time::Instant::now();
        let timeout = Duration::from_secs(10);
        let mut lines = std::collections::VecDeque::new();
        let mut pending = String::new();
        loop {
            // Bound each pass so a noisy runner cannot starve socket polling.
            for _ in 0..64 {
                let bytes = log
                    .read_line(&mut pending)
                    .await
                    .map_err(|e| e.to_string())?;
                if pending.ends_with('\n') {
                    lines.push_back(pending.trim_end().to_string());
                    pending.clear();
                    if lines.len() > 40 {
                        lines.pop_front();
                    }
                }
                if bytes == 0 {
                    break;
                }
            }
            progress_tx.send_replace(lines.iter().cloned().collect());
            if let Some(status) = child.try_wait().map_err(|e| e.to_string())? {
                let tail = lines.iter().cloned().collect::<Vec<_>>().join("\n");
                return Err(format!(
                    "the runner exited before its API came up ({status}); log: {}\n\n{tail}",
                    log_path.display()
                ));
            }
            if client.ready().await.is_ok() {
                return Ok(());
            }
            if start.elapsed() >= timeout {
                return Err(format!(
                    "the runner did not answer within {timeout:?} (pid {}); log: {}",
                    child.id(),
                    log_path.display()
                ));
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    };
    don::run_startup_tui(work, progress_rx)
        .await
        .map_err(|e| format!("tui error: {e}"))?
}

/// A failed attach must not leave its newly spawned runner behind.
pub(crate) async fn stop_failed_start(child: &mut Child) {
    if !matches!(child.try_wait(), Ok(None)) {
        return;
    }
    let pid = nix::unistd::Pid::from_raw(child.id() as i32);
    let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGINT);
    if wait_for_exit(child, Duration::from_secs(3)).await {
        return;
    }
    // Let the child's own force-shutdown supervision clean up resources and
    // reap it before returning to the shell.
    let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGINT);
    if !wait_for_exit(child, Duration::from_secs(4)).await {
        let _ = child.kill();
        let _ = wait_for_exit(child, Duration::from_secs(1)).await;
    }
}

async fn wait_for_exit(child: &mut Child, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if !matches!(child.try_wait(), Ok(None)) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    false
}
