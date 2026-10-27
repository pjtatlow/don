//! Process identity tracking for crash-recovery cleanup.
//!
//! Records `(pgid, start_time)` at spawn so that stale-state cleanup can
//! distinguish a genuinely orphaned process group from a recycled PGID.
//! The start_time comes from `/proc/<pgid>/stat` (Linux) or `proc_pidinfo`
//! (macOS).

use std::io;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Identity of a child process group at the time it was spawned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessIdentity {
    pub pgid: i32,
    pub start_time: u64,
}

/// Capture the identity of a running process.
///
/// Returns `Ok(None)` if the process does not exist (ESRCH / no `/proc` entry).
/// Returns `Ok(Some(identity))` on success.
pub fn capture(pgid: i32) -> io::Result<Option<ProcessIdentity>> {
    let start_time = read_start_time(pgid)?;
    Ok(start_time.map(|st| ProcessIdentity {
        pgid,
        start_time: st,
    }))
}

/// Check whether a previously captured identity still matches a running process.
///
/// Returns `false` if:
/// - `start_time` is 0 (old-format pid file, identity unknown)
/// - The process no longer exists
/// - The process exists but has a different start_time (PGID was recycled)
pub fn still_alive(ident: &ProcessIdentity) -> bool {
    if ident.start_time == 0 {
        return false;
    }
    match read_start_time(ident.pgid) {
        Ok(Some(st)) => st == ident.start_time,
        _ => false,
    }
}

/// Whether a group recorded since boot has outlived its leader (`bazel run` leaves its
/// workload behind). A record from before boot may name any leaderless group, so it never does.
pub fn orphaned_group(ident: &ProcessIdentity, recorded_at: SystemTime) -> bool {
    if ident.start_time == 0
        || ident.pgid <= 1
        || ident.pgid == nix::unistd::getpgrp().as_raw()
        || !boot_time().is_ok_and(|boot| recorded_at >= boot)
    {
        return false;
    }
    matches!(read_start_time(ident.pgid), Ok(None))
        && nix::sys::signal::killpg(nix::unistd::Pid::from_raw(ident.pgid), None).is_ok()
}

#[cfg(target_os = "linux")]
fn boot_time() -> io::Result<SystemTime> {
    std::fs::read_to_string("/proc/stat")?
        .lines()
        .find_map(|line| line.strip_prefix("btime ")?.trim().parse().ok())
        .map(|secs| UNIX_EPOCH + Duration::from_secs(secs))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no btime in /proc/stat"))
}

#[cfg(target_os = "macos")]
fn boot_time() -> io::Result<SystemTime> {
    let mut boot = std::mem::MaybeUninit::<libc::timeval>::zeroed();
    let mut len = std::mem::size_of::<libc::timeval>();
    let mut mib = [libc::CTL_KERN, libc::KERN_BOOTTIME];
    let ret = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            2,
            boot.as_mut_ptr().cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if ret != 0 {
        return Err(io::Error::last_os_error());
    }
    let boot = unsafe { boot.assume_init() };
    Ok(UNIX_EPOCH
        + Duration::from_secs(boot.tv_sec as u64)
        + Duration::from_micros(boot.tv_usec as u64))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn boot_time() -> io::Result<SystemTime> {
    Err(io::Error::from(io::ErrorKind::Unsupported))
}

// --- Platform-specific start_time readers ---

/// Read the start_time of a process by PID/PGID. Returns `Ok(None)` if the
/// process does not exist.
#[cfg(target_os = "linux")]
fn read_start_time(pid: i32) -> io::Result<Option<u64>> {
    let stat_path = format!("/proc/{pid}/stat");
    match std::fs::read_to_string(&stat_path) {
        Ok(contents) => Ok(parse_starttime_from_stat(&contents)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Parse the starttime (field 22, 1-indexed) from the contents of `/proc/<pid>/stat`.
///
/// Format: `<pid> (<comm>) <state> <fields...>`
///
/// The `<comm>` field can contain spaces and closing parentheses, so we must
/// find the **last** `)` to split reliably. After `<state>`, starttime is
/// the 20th whitespace-delimited token (fields 4–22, so index 19 from 0).
#[cfg(target_os = "linux")]
fn parse_starttime_from_stat(stat: &str) -> Option<u64> {
    // Find the position after the last ')'.
    let after_comm = stat.rfind(')')? + 1;
    let fields_str = stat.get(after_comm..)?.trim();
    // fields_str starts with state (field 3), then fields 4..N.
    // starttime is field 22 overall, which is field 20 after the comm.
    // From fields_str, that's index 19 (state=0, ppid=1, ... starttime=19).
    let field = fields_str.split_whitespace().nth(19)?;
    field.parse::<u64>().ok()
}

/// Read the start_time of a process by PID/PGID on macOS.
///
/// Uses `proc_pidinfo` with `PROC_PIDTBSDINFO`, which exposes the process
/// start time without depending on private `kinfo_proc` layout offsets.
#[cfg(target_os = "macos")]
fn read_start_time(pid: i32) -> io::Result<Option<u64>> {
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let expected = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
    let ret = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            expected,
        )
    };

    if ret < 0 {
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ESRCH) {
            return Ok(None);
        }
        return Err(err);
    }
    if ret == 0 {
        return Ok(None);
    }
    if ret != expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "proc_pidinfo returned truncated process info",
        ));
    }

    let info = unsafe { info.assume_init() };
    let micros = info.pbi_start_tvsec * 1_000_000 + info.pbi_start_tvusec;
    Ok(Some(micros))
}

// Fallback for unsupported platforms — always returns None (cleanup will
// treat entries as stale and delete the file without killing).
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn read_start_time(_pid: i32) -> io::Result<Option<u64>> {
    Ok(None)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    // --- parse_starttime_from_stat (Linux only) ---

    #[cfg(target_os = "linux")]
    mod stat_parsing {
        use super::*;

        struct Case {
            name: &'static str,
            input: &'static str,
            expected: Option<u64>,
        }

        #[test]
        fn table() {
            let cases = vec![
                Case {
                    name: "normal",
                    input: "1234 (bash) S 1233 1234 1234 0 -1 4194304 100 0 0 0 10 5 0 0 20 0 1 0 98765 12345678 100 18446744073709551615",
                    expected: Some(98765),
                },
                Case {
                    name: "spaces in comm",
                    input: "1234 (my cool app) S 1233 1234 1234 0 -1 4194304 100 0 0 0 10 5 0 0 20 0 1 0 55555 12345678 100 18446744073709551615",
                    expected: Some(55555),
                },
                Case {
                    name: "closing paren in comm",
                    input: "1234 (app (v2)) S 1233 1234 1234 0 -1 4194304 100 0 0 0 10 5 0 0 20 0 1 0 77777 12345678 100 18446744073709551615",
                    expected: Some(77777),
                },
                Case {
                    name: "empty",
                    input: "",
                    expected: None,
                },
                Case {
                    name: "truncated after comm",
                    input: "1234 (bash) S 1233",
                    expected: None,
                },
                Case {
                    name: "no closing paren",
                    input: "1234 (bash S 1233",
                    expected: None,
                },
            ];

            for case in cases {
                let result = parse_starttime_from_stat(case.input);
                assert_eq!(
                    result, case.expected,
                    "case '{}': input={:?}",
                    case.name, case.input
                );
            }
        }
    }

    // --- capture / still_alive ---

    #[test]
    fn capture_current_process() {
        let pid = std::process::id() as i32;
        let ident = capture(pid).unwrap();
        assert!(ident.is_some(), "should capture own process");
        let ident = ident.unwrap();
        assert_eq!(ident.pgid, pid);
        // On supported platforms, start_time > 0.
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert!(ident.start_time > 0, "start_time should be > 0");
    }

    #[test]
    fn capture_nonexistent_process() {
        // PID 4194304 is above Linux's default pid_max (4194304 is the limit,
        // not a valid PID). Use a very high number unlikely to be in use.
        let ident = capture(4_194_300).unwrap();
        assert!(ident.is_none());
    }

    #[test]
    fn still_alive_current_process() {
        let pid = std::process::id() as i32;
        if let Some(ident) = capture(pid).unwrap() {
            assert!(still_alive(&ident));
        }
    }

    #[test]
    fn still_alive_wrong_starttime() {
        let pid = std::process::id() as i32;
        let fake = ProcessIdentity {
            pgid: pid,
            start_time: 1, // wrong
        };
        assert!(!still_alive(&fake));
    }

    #[test]
    fn still_alive_zero_starttime() {
        let pid = std::process::id() as i32;
        let fake = ProcessIdentity {
            pgid: pid,
            start_time: 0,
        };
        assert!(!still_alive(&fake));
    }

    #[test]
    fn still_alive_dead_process() {
        let fake = ProcessIdentity {
            pgid: 4_194_300,
            start_time: 99999,
        };
        assert!(!still_alive(&fake));
    }

    #[test]
    fn orphaned_group_never_names_pgid_0_or_1_or_our_own_group() {
        for pgid in [0, 1, nix::unistd::getpgrp().as_raw()] {
            let ident = ProcessIdentity {
                pgid,
                start_time: 1,
            };
            assert!(!orphaned_group(&ident, SystemTime::now()), "pgid {pgid}");
        }
    }
}
