//! Process management — spawning, signaling, and lifecycle management
//! for child processes in their own process groups.

pub mod cleanup;
pub mod env;
pub mod identity;
pub mod pid_file;
pub(crate) mod rlimit;
pub(crate) mod socket;
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
pub(crate) mod test_util;

pub use identity::ProcessIdentity;

use nix::sys::signal::{Signal, killpg};
use nix::unistd::Pid;
use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::ExitStatus;
use std::sync::{Mutex, PoisonError};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

/// The output stream from a child process — either a PTY or piped stdout.
///
/// Both variants implement [`AsyncRead`], allowing the caller to read
/// output uniformly regardless of how the child was spawned.
pub enum ChildOutput {
    /// Output from a PTY-spawned process (master read half).
    Pty(pty_process::OwnedReadPty),
    /// Piped stdout from a non-PTY process (stderr merged via dup2).
    Pipe(tokio::process::ChildStdout),
    /// Log stream from a Docker container (via bollard).
    DockerLogs(crate::DockerLogReader),
}

impl AsyncRead for ChildOutput {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            ChildOutput::Pty(pty) => Pin::new(pty).poll_read(cx, buf),
            ChildOutput::Pipe(stdout) => Pin::new(stdout).poll_read(cx, buf),
            ChildOutput::DockerLogs(reader) => Pin::new(reader).poll_read(cx, buf),
        }
    }
}

/// A handle to a spawned child process in its own process group.
///
/// Holds the child process and optionally a PGID file path.
/// The PGID file is written on spawn and deleted when the handle is dropped.
pub struct ProcessHandle {
    /// The process group ID. Equal to the child's PID since we use setpgid/setsid.
    pgid: i32,
    /// The child process (for waiting on exit).
    child: tokio::process::Child,
    /// The PTY write half, if PTY mode. Used for interactive attach (Phase 17).
    pty_write: Option<pty_process::OwnedWritePty>,
    /// Path to the PGID file. Cleaned up on drop.
    pgid_file_path: Option<PathBuf>,
}

/// A handle to a foreground process that temporarily owns the user's terminal.
pub struct ForegroundProcessHandle {
    /// The process group ID. Equal to the child's PID.
    pgid: i32,
    /// The foreground child process.
    child: tokio::process::Child,
    /// Restores terminal ownership and screen state when the child is done.
    _terminal: TerminalGuard,
}

/// Terminal screen used by a foreground process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForegroundScreen {
    /// Use the current terminal screen.
    Main,
    /// Enter alternate screen while the process runs.
    Alternate,
}

/// Configuration for spawning a process.
pub struct SpawnConfig<'a> {
    /// The executable to run.
    pub cmd: &'a str,
    /// Arguments to the executable.
    pub args: &'a [String],
    /// Working directory (None = inherit don's cwd).
    pub dir: Option<&'a Path>,
    /// Environment variables (complete set for the child).
    /// Must include PATH and other essentials — the child's env is fully replaced.
    pub env: HashMap<String, String>,
    /// Path for the PGID file. None = no PGID file (e.g., for tasks).
    pub pgid_file_path: Option<PathBuf>,
    /// Force pipe-based spawning instead of PTY (for testing fallback).
    pub force_pipe: bool,
    /// Raw fds to pass to the child at fd 3, 4, 5... (LISTEN_FDS protocol).
    /// Empty means no socket passing. Works in both PTY and pipe modes —
    /// fd placement happens in a pre_exec hook either way.
    pub listen_fds: Vec<std::os::unix::io::RawFd>,
}

/// Errors from process spawning and management.
#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
    /// PTY allocation failed.
    #[error("failed to allocate PTY: {0}")]
    PtyAlloc(#[source] pty_process::Error),
    /// Failed to spawn child process.
    #[error("failed to spawn process '{cmd}': {source}")]
    Spawn {
        cmd: String,
        #[source]
        source: std::io::Error,
    },
    /// Child exited before we could read its PID.
    #[error("child process '{cmd}' exited immediately, could not determine PGID")]
    ChildExitedEarly { cmd: String },
    /// PGID file error.
    #[error("pgid file error: {0}")]
    PgidFile(String),
    /// Failed to send signal to process group.
    #[error("failed to send {signal} to pgid {pgid}: {source}")]
    Signal {
        pgid: i32,
        signal: &'static str,
        #[source]
        source: nix::Error,
    },
    /// Process did not exit even after SIGKILL (e.g., stuck in uninterruptible sleep).
    #[error("process pgid {pgid} did not exit after SIGKILL (possibly in uninterruptible sleep)")]
    Unkillable { pgid: i32 },
    /// Foreground terminal setup failed.
    #[error("foreground terminal error: {0}")]
    ForegroundTerminal(String),
    /// I/O error during process management.
    #[error("process I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl ProcessHandle {
    /// The process group ID of this child.
    pub fn pgid(&self) -> i32 {
        self.pgid
    }

    /// Take the PTY write half for interactive attach.
    pub fn take_pty_write(&mut self) -> Option<pty_process::OwnedWritePty> {
        self.pty_write.take()
    }

    /// Return the PTY write half after an attach session ends.
    pub fn set_pty_write(&mut self, pty: pty_process::OwnedWritePty) {
        self.pty_write = Some(pty);
    }

    /// Wait for the child to exit, returning the exit status.
    pub async fn wait(&mut self) -> Result<ExitStatus, ProcessError> {
        self.child.wait().await.map_err(ProcessError::Io)
    }

    /// Check whether the direct child has exited without blocking.
    pub fn try_wait(&mut self) -> Result<Option<ExitStatus>, ProcessError> {
        self.child.try_wait().map_err(ProcessError::Io)
    }

    /// Send a signal to the entire process group.
    pub fn signal(&self, sig: Signal) -> Result<(), ProcessError> {
        killpg(Pid::from_raw(self.pgid), sig).map_err(|source| ProcessError::Signal {
            pgid: self.pgid,
            signal: signal_name(sig),
            source,
        })
    }

    /// Poll `killpg(pgid, 0)` until it returns ESRCH — i.e. the process
    /// group has no remaining members. Returns `true` when the group is
    /// empty, `false` on timeout.
    ///
    /// `terminate` only reaps the *parent* process via `child.wait()`, so
    /// any descendants in the same pgroup may still be alive (slow
    /// graceful-shutdown children, daemons that hold file locks, etc.).
    /// Callers that care about "is the old instance fully gone before I
    /// start a new one?" — e.g. the restart path for a DB with an
    /// exclusive data-dir lock — should await this after `terminate`.
    pub async fn wait_pgroup_empty(&self, timeout: std::time::Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Err(nix::Error::ESRCH) = killpg(Pid::from_raw(self.pgid), None) {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    /// Send a signal to the process group, wait up to `timeout` for exit,
    /// then send SIGKILL if the process hasn't exited.
    pub async fn terminate(
        &mut self,
        sig: Signal,
        timeout: std::time::Duration,
    ) -> Result<ExitStatus, ProcessError> {
        self.terminate_with_signal_callback(sig, timeout, |_, _| {})
            .await
    }

    /// Like [`terminate`](Self::terminate), but invokes `before_signal`
    /// immediately before each signal send attempt.
    pub(crate) async fn terminate_with_signal_callback<F>(
        &mut self,
        sig: Signal,
        timeout: std::time::Duration,
        mut before_signal: F,
    ) -> Result<ExitStatus, ProcessError>
    where
        F: FnMut(Signal, i32),
    {
        // Send the requested signal. Ignore ESRCH (process already gone).
        before_signal(sig, self.pgid);
        if let Err(e) = self.signal(sig)
            && !matches!(
                e,
                ProcessError::Signal {
                    source: nix::Error::ESRCH,
                    ..
                }
            )
        {
            return Err(e);
        }

        // Wait with timeout
        match tokio::time::timeout(timeout, self.child.wait()).await {
            Ok(result) => result.map_err(ProcessError::Io),
            Err(_elapsed) => {
                // Timeout — escalate to SIGKILL
                before_signal(Signal::SIGKILL, self.pgid);
                if let Err(e) = self.signal(Signal::SIGKILL)
                    && !matches!(
                        e,
                        ProcessError::Signal {
                            source: nix::Error::ESRCH,
                            ..
                        }
                    )
                {
                    return Err(e);
                }
                // Wait again with a generous timeout. SIGKILL is normally instant,
                // but a process in uninterruptible sleep (D state, e.g. stuck NFS)
                // cannot be killed and wait() would block forever.
                match tokio::time::timeout(std::time::Duration::from_millis(500), self.child.wait())
                    .await
                {
                    Ok(result) => result.map_err(ProcessError::Io),
                    Err(_) => Err(ProcessError::Unkillable { pgid: self.pgid }),
                }
            }
        }
    }

    /// Send a signal to the process group and wait for the whole group to
    /// disappear before the graceful timeout expires.
    ///
    /// [`terminate`](Self::terminate) only waits for the direct child. This
    /// variant is for shutdown paths where descendants in the same process
    /// group must also get their graceful window before Don declares the
    /// service stopped.
    pub async fn terminate_process_group(
        &mut self,
        sig: Signal,
        timeout: std::time::Duration,
    ) -> Result<ExitStatus, ProcessError> {
        self.terminate_process_group_with_signal_callback(sig, timeout, |_, _| {})
            .await
    }

    /// Like [`terminate_process_group`](Self::terminate_process_group), but
    /// invokes `before_signal` immediately before each signal send attempt.
    pub(crate) async fn terminate_process_group_with_signal_callback<F>(
        &mut self,
        sig: Signal,
        timeout: std::time::Duration,
        mut before_signal: F,
    ) -> Result<ExitStatus, ProcessError>
    where
        F: FnMut(Signal, i32),
    {
        let deadline = tokio::time::Instant::now() + timeout;

        before_signal(sig, self.pgid);
        if let Err(e) = self.signal(sig)
            && !matches!(
                e,
                ProcessError::Signal {
                    source: nix::Error::ESRCH,
                    ..
                }
            )
        {
            return Err(e);
        }

        let status = match tokio::time::timeout(timeout, self.child.wait()).await {
            Ok(result) => result.map_err(ProcessError::Io)?,
            Err(_) => {
                self.signal_sigkill(&mut before_signal)?;
                let status = match tokio::time::timeout(
                    std::time::Duration::from_millis(500),
                    self.child.wait(),
                )
                .await
                {
                    Ok(result) => result.map_err(ProcessError::Io)?,
                    Err(_) => return Err(ProcessError::Unkillable { pgid: self.pgid }),
                };
                if !self
                    .wait_pgroup_empty(std::time::Duration::from_millis(500))
                    .await
                {
                    return Err(ProcessError::Unkillable { pgid: self.pgid });
                }
                return Ok(status);
            }
        };

        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if !self.wait_pgroup_empty(remaining).await {
            self.signal_sigkill(&mut before_signal)?;
            if !self
                .wait_pgroup_empty(std::time::Duration::from_millis(500))
                .await
            {
                return Err(ProcessError::Unkillable { pgid: self.pgid });
            }
        }

        Ok(status)
    }

    fn signal_sigkill<F>(&self, before_signal: &mut F) -> Result<(), ProcessError>
    where
        F: FnMut(Signal, i32),
    {
        before_signal(Signal::SIGKILL, self.pgid);
        if let Err(e) = self.signal(Signal::SIGKILL)
            && !matches!(
                e,
                ProcessError::Signal {
                    source: nix::Error::ESRCH,
                    ..
                }
            )
        {
            return Err(e);
        }
        Ok(())
    }
}

impl Drop for ProcessHandle {
    fn drop(&mut self) {
        if let Some(path) = self.pgid_file_path.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

impl ForegroundProcessHandle {
    /// The process group ID of this child.
    pub fn pgid(&self) -> i32 {
        self.pgid
    }

    /// Wait for the foreground child to exit.
    pub async fn wait(&mut self) -> Result<ExitStatus, ProcessError> {
        self.child.wait().await.map_err(ProcessError::Io)
    }

    /// Send a signal to the entire process group.
    pub fn signal(&self, sig: Signal) -> Result<(), ProcessError> {
        killpg(Pid::from_raw(self.pgid), sig).map_err(|source| ProcessError::Signal {
            pgid: self.pgid,
            signal: signal_name(sig),
            source,
        })
    }

    /// Send a signal to the process group, wait up to `timeout` for exit,
    /// then send SIGKILL if the process hasn't exited.
    pub async fn terminate(
        &mut self,
        sig: Signal,
        timeout: std::time::Duration,
    ) -> Result<ExitStatus, ProcessError> {
        if let Err(e) = self.signal(sig)
            && !matches!(
                e,
                ProcessError::Signal {
                    source: nix::Error::ESRCH,
                    ..
                }
            )
        {
            return Err(e);
        }

        match tokio::time::timeout(timeout, self.child.wait()).await {
            Ok(result) => result.map_err(ProcessError::Io),
            Err(_elapsed) => {
                if let Err(e) = self.signal(Signal::SIGKILL)
                    && !matches!(
                        e,
                        ProcessError::Signal {
                            source: nix::Error::ESRCH,
                            ..
                        }
                    )
                {
                    return Err(e);
                }
                match tokio::time::timeout(std::time::Duration::from_millis(500), self.child.wait())
                    .await
                {
                    Ok(result) => result.map_err(ProcessError::Io),
                    Err(_) => Err(ProcessError::Unkillable { pgid: self.pgid }),
                }
            }
        }
    }
}

/// Process-wide refcount and saved baseline for [`JobControlStopGuard`];
/// `count == 0` means no foreground window is open and `saved` is `None`.
struct JobControlStopState {
    count: usize,
    saved: Option<(libc::sigaction, libc::sigaction)>,
}

static JOB_CONTROL_STATE: Mutex<JobControlStopState> = Mutex::new(JobControlStopState {
    count: 0,
    saved: None,
});

/// Ignores `SIGTTIN`/`SIGTTOU` process-wide so a foreground task can't STOP the
/// daemon; refcounted for overlapping windows. See `docs/foreground-tasks.md`.
struct JobControlStopGuard;

impl JobControlStopGuard {
    /// The first guard saves the true baseline and sets `SIG_IGN`; later
    /// overlapping guards only bump the count.
    fn install() -> Self {
        let mut state = JOB_CONTROL_STATE
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if state.count == 0 {
            // Capture the real baseline before the first ignore is installed.
            let ttin = ignore_signal(libc::SIGTTIN);
            let ttou = ignore_signal(libc::SIGTTOU);
            state.saved = Some((ttin, ttou));
        }
        state.count += 1;
        Self
    }
}

impl Drop for JobControlStopGuard {
    fn drop(&mut self) {
        let mut state = JOB_CONTROL_STATE
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        // Last guard out restores the baseline; earlier drops only decrement,
        // so an inner window never lifts an outer window's ignore.
        state.count = state.count.saturating_sub(1);
        if state.count == 0
            && let Some((ttin, ttou)) = state.saved.take()
        {
            restore_signal(libc::SIGTTIN, ttin);
            restore_signal(libc::SIGTTOU, ttou);
        }
    }
}

struct TerminalGuard {
    fd: libc::c_int,
    original_pgrp: libc::pid_t,
    original_termios: Option<libc::termios>,
    alternate_screen: bool,
    /// Declared last so it drops *after* `TerminalGuard::drop`'s background-pgrp
    /// `tcsetpgrp`/`tcsetattr` restore, keeping those calls signal-protected.
    _stop_guard: JobControlStopGuard,
}

impl TerminalGuard {
    fn enter(screen: ForegroundScreen) -> Result<Self, ProcessError> {
        let fd = libc::STDIN_FILENO;
        // Safety: isatty only reads fd metadata.
        let is_tty = unsafe { libc::isatty(fd) } == 1;
        if !is_tty {
            return Err(ProcessError::ForegroundTerminal(
                "foreground tasks require an interactive terminal".to_string(),
            ));
        }

        // Safety: tcgetpgrp reads the foreground pgroup for a valid terminal fd.
        let original_pgrp = unsafe { libc::tcgetpgrp(fd) };
        if original_pgrp < 0 {
            return Err(ProcessError::ForegroundTerminal(format!(
                "tcgetpgrp failed: {}",
                std::io::Error::last_os_error()
            )));
        }

        let mut termios = MaybeUninit::<libc::termios>::uninit();
        // Safety: termios points to valid uninitialised storage for tcgetattr.
        let original_termios = if unsafe { libc::tcgetattr(fd, termios.as_mut_ptr()) } == 0 {
            // Safety: tcgetattr returned success and initialised termios.
            Some(unsafe { termios.assume_init() })
        } else {
            None
        };

        let alternate_screen = matches!(screen, ForegroundScreen::Alternate);
        if alternate_screen {
            crossterm::execute!(std::io::stdout(), crossterm::terminal::EnterAlternateScreen)
                .map_err(|e| {
                    ProcessError::ForegroundTerminal(format!("enter alternate screen failed: {e}"))
                })?;
        }

        // Installed last: no fallible step follows, so an early return above
        // never leaks an altered disposition. don is still foreground here.
        Ok(Self {
            fd,
            original_pgrp,
            original_termios,
            alternate_screen,
            _stop_guard: JobControlStopGuard::install(),
        })
    }

    fn make_foreground(&self, pgid: i32) -> Result<(), ProcessError> {
        // Safety: tcsetpgrp updates terminal foreground ownership for a valid tty fd.
        if unsafe { libc::tcsetpgrp(self.fd, pgid) } != 0 {
            return Err(ProcessError::ForegroundTerminal(format!(
                "tcsetpgrp failed: {}",
                std::io::Error::last_os_error()
            )));
        }
        Ok(())
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        // Runs from a background pgrp; `_stop_guard` (dropped after this body)
        // keeps SIGTTIN/SIGTTOU ignored so the STOP-prone calls below are safe.
        let _ = unsafe { libc::tcsetpgrp(self.fd, self.original_pgrp) };
        if let Some(termios) = self.original_termios.as_ref() {
            // Safety: termios was captured by tcgetattr for this fd.
            let _ = unsafe { libc::tcsetattr(self.fd, libc::TCSANOW, termios) };
        }
        if self.alternate_screen {
            let _ =
                crossterm::execute!(std::io::stdout(), crossterm::terminal::LeaveAlternateScreen);
        }
    }
}

/// Set `signum`'s disposition to SIG_IGN and return the previous sigaction
/// so it can be put back via [`restore_signal`].
fn ignore_signal(signum: libc::c_int) -> libc::sigaction {
    // Safety: sigaction reads/writes a struct sigaction we own; SIG_IGN is a
    // valid handler value.
    unsafe {
        let mut prev: libc::sigaction = std::mem::zeroed();
        let mut new: libc::sigaction = std::mem::zeroed();
        new.sa_sigaction = libc::SIG_IGN;
        libc::sigemptyset(&mut new.sa_mask);
        let ret = libc::sigaction(signum, &new, &mut prev);
        debug_assert_eq!(
            ret,
            0,
            "sigaction(SIG_IGN) failed: {}",
            std::io::Error::last_os_error()
        );
        prev
    }
}

/// Restore a signal disposition saved by [`ignore_signal`].
fn restore_signal(signum: libc::c_int, prev: libc::sigaction) {
    // Safety: prev was produced by sigaction in `ignore_signal`.
    unsafe {
        libc::sigaction(signum, &prev, std::ptr::null_mut());
    }
}

/// Spawn a child process in its own process group.
///
/// 1. Tries PTY allocation first (for terminal-like behavior).
/// 2. Falls back to pipe-based spawning if PTY fails or `force_pipe` is set.
/// 3. In PTY mode, the child gets its own session via `setsid()` (handled by pty-process).
///    In pipe mode, the child gets its own process group via `setpgid(0, 0)`.
/// 4. If `pgid_file_path` is set, writes the PGID to the file after spawn.
pub async fn spawn_process(
    config: SpawnConfig<'_>,
) -> Result<(ProcessHandle, ChildOutput), ProcessError> {
    // Default to PTY for all services — this gives children a real TTY, which
    // flips libc stdio from block-buffered back to line-buffered, so logs from
    // Python/C/C++/Java network services appear as they're written rather than
    // stalling in a 4KB pipe buffer. PTY allocation can fail in headless/CI
    // environments, in which case we fall back to pipe mode.
    if !config.force_pipe {
        match spawn_pty(&config) {
            Ok((child, read_pty, write_pty)) => {
                let pgid = child_pgid(&child, config.cmd)?;
                write_pgid_file(config.pgid_file_path.as_deref(), pgid).await?;
                let output = ChildOutput::Pty(read_pty);
                let handle = ProcessHandle {
                    pgid,
                    child,
                    pty_write: Some(write_pty),
                    pgid_file_path: config.pgid_file_path,
                };
                Ok((handle, output))
            }
            Err(_pty_err) => {
                // PTY allocation failed (typically exhausted in CI / container
                // envs with tight pty caps). Fall back silently — pipe mode
                // works; services that specifically need a terminal will
                // surface their own errors downstream.
                spawn_pipe_handle(&config).await
            }
        }
    } else {
        spawn_pipe_handle(&config).await
    }
}

/// Spawn a process that owns the user's foreground terminal until it exits.
///
/// The child inherits stdin/stdout/stderr, runs in its own process group,
/// and becomes the terminal's foreground process group. Dropping the returned
/// handle restores the parent process group and terminal attributes.
pub async fn spawn_foreground_process(
    config: SpawnConfig<'_>,
    screen: ForegroundScreen,
) -> Result<ForegroundProcessHandle, ProcessError> {
    let terminal = TerminalGuard::enter(screen)?;
    let (prog, prog_args) = listen_pid_shim(&config);
    let mut cmd = tokio::process::Command::new(prog);
    cmd.args(&prog_args);
    cmd.stdin(std::process::Stdio::inherit());
    cmd.stdout(std::process::Stdio::inherit());
    cmd.stderr(std::process::Stdio::inherit());
    cmd.kill_on_drop(true);
    cmd.env_clear().envs(&config.env);

    if let Some(dir) = config.dir {
        cmd.current_dir(dir);
    }

    let listen_fds = config.listen_fds.clone();
    // Safety: setpgid, dup/fcntl/close via place_fds_for_exec, and prctl are
    // async-signal-safe. Runs in the child between fork and exec.
    unsafe {
        cmd.pre_exec(move || {
            #[cfg(target_os = "linux")]
            {
                if libc::prctl(
                    libc::PR_SET_PDEATHSIG,
                    libc::SIGKILL as libc::c_ulong,
                    0,
                    0,
                    0,
                ) != 0
                {
                    return Err(std::io::Error::last_os_error());
                }
            }
            socket::place_fds_for_exec(&listen_fds)?;
            nix::unistd::setpgid(Pid::from_raw(0), Pid::from_raw(0))
                .map_err(std::io::Error::other)?;
            Ok(())
        });
    }

    let child = cmd.spawn().map_err(|source| ProcessError::Spawn {
        cmd: config.cmd.to_string(),
        source,
    })?;
    let pgid = child_pgid(&child, config.cmd)?;
    write_pgid_file(config.pgid_file_path.as_deref(), pgid).await?;
    terminal.make_foreground(pgid)?;
    Ok(ForegroundProcessHandle {
        pgid,
        child,
        _terminal: terminal,
    })
}

/// Build a ProcessHandle + ChildOutput from a pipe-mode spawn.
async fn spawn_pipe_handle(
    config: &SpawnConfig<'_>,
) -> Result<(ProcessHandle, ChildOutput), ProcessError> {
    let mut child = spawn_pipe(config)?;
    let pgid = child_pgid(&child, config.cmd)?;
    write_pgid_file(config.pgid_file_path.as_deref(), pgid).await?;
    let stdout = child.stdout.take().ok_or_else(|| ProcessError::Spawn {
        cmd: config.cmd.to_string(),
        source: std::io::Error::other("child process has no stdout"),
    })?;

    let output = ChildOutput::Pipe(stdout);
    let handle = ProcessHandle {
        pgid,
        child,
        pty_write: None,
        pgid_file_path: config.pgid_file_path.clone(),
    };
    Ok((handle, output))
}

fn spawn_pty(
    config: &SpawnConfig<'_>,
) -> Result<
    (
        tokio::process::Child,
        pty_process::OwnedReadPty,
        pty_process::OwnedWritePty,
    ),
    ProcessError,
> {
    let (pty, pts) = pty_process::open().map_err(ProcessError::PtyAlloc)?;

    // Set a reasonable default size so programs that query terminal
    // dimensions at startup don't stall on a 0x0 PTY.
    let _ = pty.resize(pty_process::Size::new(24, 80));

    let (prog, prog_args) = listen_pid_shim(config);
    let mut cmd = pty_process::Command::new(prog);
    cmd = cmd.args(&prog_args);

    // SIGKILL the child if the handle is ever dropped without an explicit
    // `terminate()`. The happy path calls `terminate()` which reaps cleanly;
    // this guards mishaps where `rs.handle` gets replaced (e.g. a respawn
    // path that bypassed the stop) or the runner itself panics — without
    // this, the child survives as an orphan still bound to inherited listen
    // fds. Only kills the immediate child PID, not the whole PG, but for
    // don's services the child IS the PG leader (setsid via pty-process).
    cmd = cmd.kill_on_drop(true);

    // Clear the parent env first, then set exactly the merged set. Without
    // `env_clear()`, `envs()` only ADDS — so anything we stripped from
    // `config.env` (e.g. bazel's leaked `RUNFILES_*` / `BASH_FUNC_*`) still
    // leaks through from don's own env into the child. `merge_env()` already
    // seeds `std::env::vars()` minus the strip list, so clearing here is
    // safe and gives us an exact child env.
    cmd = cmd.env_clear().envs(&config.env);

    if let Some(dir) = config.dir {
        cmd = cmd.current_dir(dir);
    }

    // Note: pty-process calls setsid() in its session_leader pre_exec hook,
    // which creates a new session AND process group (PGID = PID).
    // No additional setpgid needed — setsid handles it.
    //
    // Catch-the-parent-dying: set PR_SET_PDEATHSIG so the child gets SIGKILL
    // if don dies without cleanly stopping it (external `kill -9`, segfault,
    // power loss mid-run). Without this, the child keeps running, reparented
    // to init, and holds any inherited listen fds hostage until someone
    // notices and kills it by hand. `kill_on_drop(true)` on the Command
    // handles the graceful-drop case; this handles the parent-not-graceful
    // case. Linux-only — macOS doesn't expose a portable equivalent. The
    // setting survives execve(2) in the common (non-setuid) case.
    let listen_fds = config.listen_fds.clone();
    // Safety: pty-process runs this after its session_leader hook, between fork
    // and exec; signal, prctl, and place_fds_for_exec are async-signal-safe.
    cmd = unsafe {
        cmd.pre_exec(move || {
            // Reset SIGTTIN/SIGTTOU to SIG_DFL: a concurrent foreground window
            // may leak SIG_IGN via fork; this child never owns don's tty.
            libc::signal(libc::SIGTTIN, libc::SIG_DFL);
            libc::signal(libc::SIGTTOU, libc::SIG_DFL);
            #[cfg(target_os = "linux")]
            {
                if libc::prctl(
                    libc::PR_SET_PDEATHSIG,
                    libc::SIGKILL as libc::c_ulong,
                    0,
                    0,
                    0,
                ) != 0
                {
                    return Err(std::io::Error::last_os_error());
                }
            }
            socket::place_fds_for_exec(&listen_fds)?;
            Ok(())
        })
    };

    let child = cmd.spawn(pts).map_err(|e| ProcessError::Spawn {
        cmd: config.cmd.to_string(),
        source: std::io::Error::other(e),
    })?;

    let (read_pty, write_pty) = pty.into_split();
    Ok((child, read_pty, write_pty))
}

fn spawn_pipe(config: &SpawnConfig<'_>) -> Result<tokio::process::Child, ProcessError> {
    let (prog, prog_args) = listen_pid_shim(config);
    let mut cmd = tokio::process::Command::new(prog);
    cmd.args(&prog_args);
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::piped());
    // Matching `spawn_pty`: SIGKILL on drop as a safety net against orphans
    // when a handle is replaced or the runner panics before `terminate()`.
    cmd.kill_on_drop(true);

    // Clear the parent env first — see the matching comment in `spawn_pty`.
    cmd.env_clear().envs(&config.env);

    if let Some(dir) = config.dir {
        cmd.current_dir(dir);
    }

    // Clone listen fds for the pre_exec closure.
    let listen_fds = config.listen_fds.clone();

    // Safety: setpgid, dup2, dup, fcntl, prctl, signal, and close are async-signal-safe.
    // dup2(1, 2) merges stderr into stdout. This works because tokio has
    // already set up fd 1 as the pipe's write end before pre_exec runs.
    //
    // PR_SET_PDEATHSIG: see the matching block in `spawn_pty` for the why.
    // Linux-only.
    //
    // LISTEN_PID is NOT set here: `setenv` in `pre_exec` is clobbered by the
    // explicit envp that Rust's `Command` hands to `execve` once any env
    // mutator is called. The shim command from `listen_pid_shim` sets
    // `LISTEN_PID=$$` in a shell and `exec`s the real binary, so the PID the
    // child reads matches its own process (exec preserves the PID).
    unsafe {
        cmd.pre_exec(move || {
            // Reset SIGTTIN/SIGTTOU to SIG_DFL: a concurrent foreground window
            // may leak SIG_IGN via fork; this child never owns don's tty.
            libc::signal(libc::SIGTTIN, libc::SIG_DFL);
            libc::signal(libc::SIGTTOU, libc::SIG_DFL);
            #[cfg(target_os = "linux")]
            {
                if libc::prctl(
                    libc::PR_SET_PDEATHSIG,
                    libc::SIGKILL as libc::c_ulong,
                    0,
                    0,
                    0,
                ) != 0
                {
                    return Err(std::io::Error::last_os_error());
                }
            }

            // Place listen fds at fd 3, 4, 5... and clear CLOEXEC.
            socket::place_fds_for_exec(&listen_fds)?;

            nix::unistd::setpgid(Pid::from_raw(0), Pid::from_raw(0))
                .map_err(std::io::Error::other)?;
            if libc::dup2(libc::STDOUT_FILENO, libc::STDERR_FILENO) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }

    cmd.spawn().map_err(|source| ProcessError::Spawn {
        cmd: config.cmd.to_string(),
        source,
    })
}

/// Get the child's PGID from its PID. With setpgid(0,0) or setsid(), PGID == PID.
fn child_pgid(child: &tokio::process::Child, cmd: &str) -> Result<i32, ProcessError> {
    child
        .id()
        .map(|id| id as i32)
        .ok_or_else(|| ProcessError::ChildExitedEarly {
            cmd: cmd.to_string(),
        })
}

/// Write the PGID (and start_time if available) to a file. Creates parent
/// directories if needed. Format: `<pgid>\n<start_time>` or just `<pgid>`
/// if the child exited before we could capture its start_time.
async fn write_pgid_file(path: Option<&Path>, pgid: i32) -> Result<(), ProcessError> {
    let Some(path) = path else { return Ok(()) };
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(ProcessError::Io)?;
    }
    // Capture start_time synchronously — this reads /proc/<pgid>/stat (Linux)
    // or calls sysctl (macOS). If the child already exited (unlikely race),
    // we fall back to writing just the PGID.
    let content = match identity::capture(pgid) {
        Ok(Some(ident)) => format!("{}\n{}", ident.pgid, ident.start_time),
        _ => pgid.to_string(),
    };
    tokio::fs::write(path, content).await.map_err(|e| {
        ProcessError::PgidFile(format!("failed to write pgid to '{}': {e}", path.display()))
    })?;
    Ok(())
}

/// Read the PGID from a file. Returns `None` if the file does not exist.
pub async fn read_pgid_file(path: &Path) -> Result<Option<i32>, ProcessError> {
    match tokio::fs::read_to_string(path).await {
        Ok(content) => {
            let content = content.trim();
            if content.is_empty() {
                return Ok(None);
            }
            // First line is the PGID (second line, if present, is start_time).
            let first_line = content.lines().next().unwrap_or("").trim();
            let pgid: i32 = first_line.parse().map_err(|_| {
                ProcessError::PgidFile(format!(
                    "invalid pgid in '{}': '{first_line}'",
                    path.display()
                ))
            })?;
            Ok(Some(pgid))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(ProcessError::PgidFile(format!(
            "failed to read pgid from '{}': {e}",
            path.display()
        ))),
    }
}

/// Read a pid file as a full `ProcessIdentity`. Returns `None` if the file
/// does not exist. If the file has the old single-line format, returns
/// `ProcessIdentity { pgid, start_time: 0 }`.
pub async fn read_pid_file_identity(path: &Path) -> Result<Option<ProcessIdentity>, ProcessError> {
    match tokio::fs::read_to_string(path).await {
        Ok(content) => {
            let content = content.trim();
            if content.is_empty() {
                return Ok(None);
            }
            let mut lines = content.lines();
            let pgid_str = lines.next().unwrap_or("");
            let pgid: i32 = pgid_str.trim().parse().map_err(|_| {
                ProcessError::PgidFile(format!(
                    "invalid pgid in '{}': '{pgid_str}'",
                    path.display()
                ))
            })?;
            let start_time: u64 = match lines.next() {
                // `0` means "unknown" — the crash-recovery identity check
                // treats missing/unknown start_time as "matches any", which
                // is the correct permissive behaviour for both a malformed
                // second line and the legacy one-line pgid file format.
                Some(s) => s.trim().parse().unwrap_or(0),
                None => 0,
            };
            Ok(Some(ProcessIdentity { pgid, start_time }))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(ProcessError::PgidFile(format!(
            "failed to read identity from '{}': {e}",
            path.display()
        ))),
    }
}

/// Delete a PGID file from disk. Idempotent — does not error if already gone.
pub async fn cleanup_pgid_file(path: &Path) -> Result<(), std::io::Error> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Wrap a command in a shell shim when listen fds are being passed, so the
/// final binary sees `LISTEN_PID` set to its own PID.
///
/// The systemd socket-activation protocol requires `LISTEN_PID` to equal the
/// PID of the process that will read the fds. We can't set it from
/// `pre_exec`: `std::process::Command` uses `execve` with an explicit envp
/// (once any env method is called on the command), and `setenv` writes to
/// `environ` which `execve` ignores. Rolling our own `execvpe` or calling
/// `setenv` from `pre_exec` before a manual exec would also require `malloc`
/// after fork, which isn't safe while tokio worker threads might have held
/// allocator locks in the parent.
///
/// The shell shim sets `LISTEN_PID=$$` and `exec`s the real binary. `exec`
/// preserves the PID, so the final process reads `LISTEN_PID = getpid()`.
/// Fds at 3+ pass through untouched (CLOEXEC was already cleared by
/// `place_fds_for_exec`, and `sh` doesn't touch fds >=3).
fn listen_pid_shim<'a>(config: &'a SpawnConfig<'a>) -> (String, Vec<String>) {
    if config.listen_fds.is_empty() {
        return (config.cmd.to_string(), config.args.to_vec());
    }
    // `$$` is the shell's PID; `exec "$@"` replaces the shell with the real
    // binary, preserving that PID. The literal `sh` becomes `$0`; the actual
    // command and its args follow as `$@`.
    let script = r#"LISTEN_PID=$$; export LISTEN_PID; exec "$@""#;
    let mut shim_args: Vec<String> = vec![
        "-c".to_string(),
        script.to_string(),
        "sh".to_string(),
        config.cmd.to_string(),
    ];
    shim_args.extend(config.args.iter().cloned());
    ("/bin/sh".to_string(), shim_args)
}

pub(crate) fn signal_name(sig: Signal) -> &'static str {
    match sig {
        Signal::SIGTERM => "SIGTERM",
        Signal::SIGKILL => "SIGKILL",
        Signal::SIGINT => "SIGINT",
        Signal::SIGQUIT => "SIGQUIT",
        Signal::SIGHUP => "SIGHUP",
        Signal::SIGUSR1 => "SIGUSR1",
        Signal::SIGUSR2 => "SIGUSR2",
        _ => "unknown",
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    /// Read `signum`'s current handler without changing the disposition.
    fn current_handler(signum: libc::c_int) -> libc::sighandler_t {
        // Safety: a null `act` retrieves the current disposition into `cur`.
        unsafe {
            let mut cur: libc::sigaction = std::mem::zeroed();
            libc::sigaction(signum, std::ptr::null(), &mut cur);
            cur.sa_sigaction
        }
    }

    /// The dispositions and the guard refcount are process-wide globals, so
    /// this stays the only test touching them; a concurrent one would race.
    #[test]
    fn job_control_stop_guard_refcounts_overlapping_windows() {
        let orig_ttin = current_handler(libc::SIGTTIN);
        let orig_ttou = current_handler(libc::SIGTTOU);

        // A single window ignores on install and restores the baseline on drop.
        {
            let _guard = JobControlStopGuard::install();
            assert_eq!(
                current_handler(libc::SIGTTIN),
                libc::SIG_IGN,
                "SIGTTIN must be ignored while guarded"
            );
            assert_eq!(
                current_handler(libc::SIGTTOU),
                libc::SIG_IGN,
                "SIGTTOU must be ignored while guarded"
            );
        }
        assert_eq!(
            current_handler(libc::SIGTTIN),
            orig_ttin,
            "SIGTTIN disposition must be restored after drop"
        );
        assert_eq!(
            current_handler(libc::SIGTTOU),
            orig_ttou,
            "SIGTTOU disposition must be restored after drop"
        );

        // Overlapping windows: the ignore holds across the union, and only the
        // last guard out restores the baseline — regardless of drop order.
        let a = JobControlStopGuard::install();
        assert_eq!(current_handler(libc::SIGTTIN), libc::SIG_IGN);
        let b = JobControlStopGuard::install();
        assert_eq!(current_handler(libc::SIGTTIN), libc::SIG_IGN);

        drop(a);
        assert_eq!(
            current_handler(libc::SIGTTIN),
            libc::SIG_IGN,
            "an inner drop must not lift the ignore while another guard is live"
        );
        assert_eq!(current_handler(libc::SIGTTOU), libc::SIG_IGN);

        drop(b);
        assert_eq!(
            current_handler(libc::SIGTTIN),
            orig_ttin,
            "the last drop must restore the baseline SIGTTIN disposition"
        );
        assert_eq!(
            current_handler(libc::SIGTTOU),
            orig_ttou,
            "the last drop must restore the baseline SIGTTOU disposition"
        );
    }
}
