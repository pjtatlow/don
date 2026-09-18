//! Semver-shaped comparison for don's binary and `min_version`.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ParsedVersion {
    major: u64,
    minor: u64,
    patch: u64,
    prerelease: bool,
}

impl ParsedVersion {
    pub(crate) fn parse(raw: &str) -> Option<Self> {
        let raw = raw.strip_prefix('v').unwrap_or(raw).trim();
        let core = raw.split_once('+').map_or(raw, |(core, _)| core);
        let (core, prerelease) = match core.split_once('-') {
            Some((core, _)) => (core, true),
            None => (core, false),
        };
        let mut parts = core.split('.');
        let major = parse_numeric_part(parts.next()?)?;
        let minor = parse_numeric_part(parts.next()?)?;
        let patch = parse_numeric_part(parts.next()?)?;
        if parts.next().is_some() {
            return None;
        }
        Some(Self {
            major,
            minor,
            patch,
            prerelease,
        })
    }

    pub(crate) fn is_newer_than(&self, other: &Self) -> bool {
        (self.major, self.minor, self.patch) > (other.major, other.minor, other.patch)
            || ((self.major, self.minor, self.patch) == (other.major, other.minor, other.patch)
                && other.prerelease
                && !self.prerelease)
    }
}

pub(crate) fn is_newer_version(candidate: &str, current: &str) -> bool {
    let Some(candidate) = ParsedVersion::parse(candidate) else {
        return false;
    };
    let Some(current) = ParsedVersion::parse(current) else {
        return false;
    };
    candidate.is_newer_than(&current)
}

/// How `current` compares to a required minimum version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MinimumCheck {
    Met,
    Unmet,
    InvalidMinimum,
    InvalidCurrent,
}

pub(crate) fn check_minimum(current: &str, minimum: &str) -> MinimumCheck {
    let Some(minimum) = ParsedVersion::parse(minimum) else {
        return MinimumCheck::InvalidMinimum;
    };
    let Some(current) = ParsedVersion::parse(current) else {
        return MinimumCheck::InvalidCurrent;
    };
    if minimum.is_newer_than(&current) {
        MinimumCheck::Unmet
    } else {
        MinimumCheck::Met
    }
}

fn parse_numeric_part(raw: &str) -> Option<u64> {
    if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    raw.parse().ok()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn detects_newer_versions() {
        struct Case {
            name: &'static str,
            candidate: &'static str,
            current: &'static str,
            want: bool,
        }
        let cases = [
            Case {
                name: "patch increase",
                candidate: "0.4.2",
                current: "0.4.1",
                want: true,
            },
            Case {
                name: "minor increase",
                candidate: "0.5.0",
                current: "0.4.9",
                want: true,
            },
            Case {
                name: "major increase",
                candidate: "1.0.0",
                current: "0.99.99",
                want: true,
            },
            Case {
                name: "same version",
                candidate: "0.4.1",
                current: "0.4.1",
                want: false,
            },
            Case {
                name: "older version",
                candidate: "0.4.0",
                current: "0.4.1",
                want: false,
            },
            Case {
                name: "v prefix",
                candidate: "v0.4.2",
                current: "0.4.1",
                want: true,
            },
            Case {
                name: "stable beats same prerelease",
                candidate: "0.4.2",
                current: "0.4.2-beta.1",
                want: true,
            },
            Case {
                name: "prerelease does not beat stable",
                candidate: "0.4.2-beta.1",
                current: "0.4.2",
                want: false,
            },
            Case {
                name: "invalid candidate",
                candidate: "latest",
                current: "0.4.1",
                want: false,
            },
            Case {
                name: "invalid current",
                candidate: "0.4.2",
                current: "dev",
                want: false,
            },
        ];

        for case in cases {
            assert_eq!(
                is_newer_version(case.candidate, case.current),
                case.want,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn check_minimum_table() {
        struct Case {
            name: &'static str,
            current: &'static str,
            minimum: &'static str,
            want: MinimumCheck,
        }
        let cases = [
            Case {
                name: "equal versions meet",
                current: "0.8.1",
                minimum: "0.8.1",
                want: MinimumCheck::Met,
            },
            Case {
                name: "newer binary meets",
                current: "0.9.0",
                minimum: "0.8.1",
                want: MinimumCheck::Met,
            },
            Case {
                name: "older binary is unmet",
                current: "0.8.0",
                minimum: "0.8.1",
                want: MinimumCheck::Unmet,
            },
            Case {
                name: "v prefix on minimum",
                current: "0.8.1",
                minimum: "v0.8.1",
                want: MinimumCheck::Met,
            },
            Case {
                name: "prerelease does not meet same stable",
                current: "0.8.1-beta.1",
                minimum: "0.8.1",
                want: MinimumCheck::Unmet,
            },
            Case {
                name: "stable meets same prerelease minimum",
                current: "0.8.1",
                minimum: "0.8.1-beta.1",
                want: MinimumCheck::Met,
            },
            Case {
                name: "invalid minimum",
                current: "0.8.1",
                minimum: "latest",
                want: MinimumCheck::InvalidMinimum,
            },
            Case {
                name: "invalid current",
                current: "dev",
                minimum: "0.8.1",
                want: MinimumCheck::InvalidCurrent,
            },
            Case {
                name: "whitespace around version",
                current: "0.8.1",
                minimum: " 0.8.1 ",
                want: MinimumCheck::Met,
            },
        ];
        for case in cases {
            assert_eq!(
                check_minimum(case.current, case.minimum),
                case.want,
                "{}",
                case.name
            );
        }
    }
}
