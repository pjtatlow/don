//! Desktop notifications for service/task lifecycle transitions.
//!
//! The runner calls [`notify_service`] / [`notify_task`] from its state-change
//! choke points with the effective [`NotifyConfig`] (per-item layered over the
//! global `[notify]`). The actual OS call is fire-and-forget on a detached
//! thread so a slow or absent notifier never stalls a transition, and errors
//! are swallowed — a missing notification is never worth failing a service over.

use crate::config::NotifyConfig;
use crate::runner::{ServiceState, TaskItemState};

/// The notifiable label for a service transition, or `None` to stay silent.
fn service_event(notify: &NotifyConfig, state: ServiceState) -> Option<&'static str> {
    if !notify.enabled() {
        return None;
    }
    match state {
        ServiceState::Ready if notify.on_ready() => Some("ready"),
        ServiceState::Failed | ServiceState::Unhealthy | ServiceState::DependencyFailed
            if notify.on_failed() =>
        {
            Some("failed")
        }
        ServiceState::Building if notify.on_building() => Some("building"),
        ServiceState::Stopped if notify.on_stopped() => Some("stopped"),
        _ => None,
    }
}

/// The notifiable label for a task transition, or `None` to stay silent.
fn task_event(notify: &NotifyConfig, state: TaskItemState) -> Option<&'static str> {
    if !notify.enabled() {
        return None;
    }
    match state {
        TaskItemState::Completed if notify.on_completed() => Some("completed"),
        TaskItemState::Failed | TaskItemState::DependencyFailed if notify.on_failed() => {
            Some("failed")
        }
        TaskItemState::Building if notify.on_building() => Some("building"),
        _ => None,
    }
}

/// Fire a desktop notification for a service transition, if the effective
/// config opts into this event.
pub(crate) fn notify_service(notify: &NotifyConfig, name: &str, state: ServiceState) {
    if let Some(event) = service_event(notify, state) {
        show(name, event);
    }
}

/// Fire a desktop notification for a task transition, if the effective config
/// opts into this event.
pub(crate) fn notify_task(notify: &NotifyConfig, name: &str, state: TaskItemState) {
    if let Some(event) = task_event(notify, state) {
        show(name, event);
    }
}

fn show(name: &str, event: &str) {
    let summary = format!("don · {name}");
    let body = format!("{name} {event}");
    std::thread::spawn(move || {
        let _ = notify_rust::Notification::new()
            .summary(&summary)
            .body(&body)
            .show();
    });
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn cfg(toml: &str) -> NotifyConfig {
        toml::from_str(toml).expect("valid notify config")
    }

    #[test]
    fn defaults_notify_ready_and_failed_only() {
        let c = NotifyConfig::default();
        assert_eq!(service_event(&c, ServiceState::Ready), Some("ready"));
        assert_eq!(service_event(&c, ServiceState::Failed), Some("failed"));
        assert_eq!(service_event(&c, ServiceState::Unhealthy), Some("failed"));
        assert_eq!(service_event(&c, ServiceState::Building), None);
        assert_eq!(service_event(&c, ServiceState::Stopped), None);
        assert_eq!(service_event(&c, ServiceState::Starting), None);
    }

    #[test]
    fn task_defaults_notify_failed_not_completed() {
        let c = NotifyConfig::default();
        assert_eq!(task_event(&c, TaskItemState::Failed), Some("failed"));
        assert_eq!(task_event(&c, TaskItemState::Completed), None);
    }

    #[test]
    fn master_disable_silences_everything() {
        let c = cfg("enabled = false\non_ready = true\non_failed = true");
        assert_eq!(service_event(&c, ServiceState::Ready), None);
        assert_eq!(service_event(&c, ServiceState::Failed), None);
        assert_eq!(task_event(&c, TaskItemState::Failed), None);
    }

    #[test]
    fn opting_into_building_and_stopped() {
        let c = cfg("on_building = true\non_stopped = true\non_completed = true");
        assert_eq!(service_event(&c, ServiceState::Building), Some("building"));
        assert_eq!(service_event(&c, ServiceState::Stopped), Some("stopped"));
        assert_eq!(task_event(&c, TaskItemState::Completed), Some("completed"));
    }

    #[test]
    fn per_item_merges_over_global() {
        let global = cfg("on_ready = false\non_stopped = true");
        let item = cfg("on_ready = true");
        let effective = item.merged_over(&global);
        // item wins where set
        assert_eq!(service_event(&effective, ServiceState::Ready), Some("ready"));
        // global fills the gap
        assert_eq!(service_event(&effective, ServiceState::Stopped), Some("stopped"));
    }
}
