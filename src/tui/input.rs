//! Input task — forwards crossterm key events to the TUI loop.
//!
//! Thin by design: we don't interpret keys here because interpretation
//! depends on the current view mode, which lives in the main loop. The
//! only filtering done here is dropping release events (which ratatui
//! also doesn't care about) and resize→no-op-payload conversion.

use crossterm::event::{Event, EventStream, KeyEventKind};
use futures_util::StreamExt;
use tokio::sync::mpsc;

use super::events::AppEvent;

/// Pump crossterm events into the TUI's `AppEvent` channel until stdout is
/// closed or the receiver is dropped.
pub(crate) async fn run(tx: mpsc::Sender<AppEvent>) {
    let mut reader = EventStream::new();
    // Consecutive read errors back off instead of hot-looping: EventStream
    // reports errors as ready items, and a background-pgrp read yields EIO.
    let mut consecutive_errors: u32 = 0;
    while let Some(result) = reader.next().await {
        let event = match result {
            Ok(event) => {
                consecutive_errors = 0;
                event
            }
            Err(_) => {
                consecutive_errors = consecutive_errors.saturating_add(1);
                let backoff_ms = (u64::from(consecutive_errors) * 5).min(100);
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                continue;
            }
        };
        let Some(app_event) = translate(event) else {
            continue;
        };
        if tx.send(app_event).await.is_err() {
            break;
        }
    }
}

/// Convert a crossterm event to an [`AppEvent`], returning `None` for events
/// the TUI doesn't care about (key releases, mouse, focus, paste).
fn translate(event: Event) -> Option<AppEvent> {
    match event {
        Event::Key(key) if key.kind == KeyEventKind::Release => None,
        Event::Key(key) => Some(AppEvent::Key(key)),
        Event::Resize(_, _) => Some(AppEvent::Resize),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyEvent, KeyEventState, KeyModifiers};

    fn press(code: KeyCode) -> Event {
        Event::Key(KeyEvent {
            code,
            modifiers: KeyModifiers::NONE,
            kind: KeyEventKind::Press,
            state: KeyEventState::NONE,
        })
    }

    #[test]
    fn translate_key_press_forwards() {
        let got = translate(press(KeyCode::Enter));
        assert!(matches!(got, Some(AppEvent::Key(_))));
    }

    #[test]
    fn translate_key_release_dropped() {
        let release = Event::Key(KeyEvent {
            code: KeyCode::Enter,
            modifiers: KeyModifiers::NONE,
            kind: KeyEventKind::Release,
            state: KeyEventState::NONE,
        });
        assert!(translate(release).is_none());
    }

    #[test]
    fn translate_resize_becomes_resize_variant() {
        assert!(matches!(
            translate(Event::Resize(80, 24)),
            Some(AppEvent::Resize)
        ));
    }
}
