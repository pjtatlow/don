//! Service group configuration.
//!
//! A service group is a named bundle of services (and optionally other groups)
//! that can be referenced from `depends_on` and `profiles.*.services` as a
//! single name. Groups may also declare their own `depends_on` — those
//! dependencies are applied to every (transitive) member of the group, in
//! addition to whatever each member declares for itself.

use serde::Deserialize;

use super::dependency::Dependency;

/// A named bundle of services (and/or nested groups) that can be referenced
/// as a single name from `depends_on` and `profiles.*.services`.
///
/// Group `depends_on` is additive: every transitive member of a group inherits
/// the group's `depends_on` on top of its own.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(from = "RawServiceGroup")]
pub struct ServiceGroup {
    /// Names of services or other groups that belong to this group.
    pub members: Vec<String>,
    /// Dependencies applied to every (transitive) member of this group.
    /// May reference services, tasks, or other groups.
    pub depends_on: Vec<Dependency>,
    /// Secret refs applied to every (transitive) member, additive with the member's own.
    pub secrets: Vec<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RawServiceGroup {
    /// Shorthand: `name = ["a", "b"]` — a bare member list with no group-level deps.
    Simple(Vec<String>),
    /// Detailed: `[service_groups.frontend] members = [...] depends_on = [...]`.
    Detailed {
        #[serde(default)]
        members: Vec<String>,
        #[serde(default)]
        depends_on: Vec<Dependency>,
        #[serde(default)]
        secrets: Vec<String>,
    },
}

impl From<RawServiceGroup> for ServiceGroup {
    fn from(raw: RawServiceGroup) -> Self {
        match raw {
            RawServiceGroup::Simple(members) => ServiceGroup {
                members,
                depends_on: Vec::new(),
                secrets: Vec::new(),
            },
            RawServiceGroup::Detailed {
                members,
                depends_on,
                secrets,
            } => ServiceGroup {
                members,
                depends_on,
                secrets,
            },
        }
    }
}
