//! Narration for a lazy service's first demand.
//!
//! `Lazy` means nothing has requested the service yet. Its supervisor
//! moves itself to `Pending` in the same step it takes the demand — this only
//! explains the wait that follows, if there is one.

use super::Runner;

impl Runner {
    /// A supervisor reports that something now wants it running.
    ///
    /// Demand is only ever raised by a lazy service's proxy or start trigger,
    /// so there is no task case. A process already holding a live one says nothing: its phase
    /// is authoritative and a duplicate trigger changes nothing.
    pub(in crate::runner) fn handle_demand(&mut self, name: &str, _demand: super::Demand) {
        if self.service_runtime(name).is_some() {
            return;
        }
        if self.services.contains_key(name) {
            self.narrate_lazy_demand(name);
        }
    }

    /// Say why a first connection is or isn't about to start the service.
    fn narrate_lazy_demand(&mut self, name: &str) {
        let (deps, why) = match self.services.get(name) {
            Some(rs) if rs.resolved.proxy.is_empty() => {
                (rs.resolved.depends_on.clone(), "start triggered")
            }
            Some(rs) => (rs.resolved.depends_on.clone(), "first connection"),
            _ => return,
        };

        // Only a blocking dependency's failure strands a lazy service — a
        // non-blocking one lets it start anyway.
        if let Some(failed) = deps
            .iter()
            .filter(|dep| dep.blocking)
            .find(|dep| self.is_dep_failed(&dep.name))
        {
            self.output_manager.service_error_event(
                name,
                &format!("{why} — dependency '{}' has failed", failed.name),
            );
        } else {
            let unsatisfied: Vec<&str> = deps
                .iter()
                .filter(|dep| !self.is_dep_gate_open(dep))
                .map(|dep| dep.name.as_str())
                .collect();
            if unsatisfied.is_empty() {
                self.output_manager
                    .service_event(name, &format!("{why} — dependencies satisfied"));
            } else {
                self.output_manager.service_event(
                    name,
                    &format!(
                        "waiting for dependencies before start: {}",
                        unsatisfied.join(", ")
                    ),
                );
            }
        }
    }
}
