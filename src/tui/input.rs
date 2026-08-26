//! Input task — forwards crossterm events to the TUI loop.
//!
//! Thin by design: we don't interpret input here because interpretation
//! depends on the current view mode and on where the panes ended up, both of
//! which live in the main loop. The only filtering done here is dropping
//! events nothing acts on — key releases, and the mouse *moves* that arrive
//! between drags, which would otherwise wake the loop thousands of times for
//! nothing.

use crossterm::event::{Event, EventStream, KeyEventKind, MouseEventKind};
use futures_util::StreamExt;
use tokio::sync::mpsc;

use super::events::AppEvent;

/// Pump crossterm events into the TUI's `AppEvent` channel until stdout is
/// closed or the receiver is dropped.
pub(crate) async fn run(tx: mpsc::Sender<AppEvent>) {
    let mut reader = EventStream::new();
    while let Some(result) = reader.next().await {
        let Ok(event) = result else { continue };
        let Some(app_event) = translate(event) else {
            continue;
        };
        if tx.send(app_event).await.is_err() {
            break;
        }
    }
}

/// Convert a crossterm event to an [`AppEvent`], returning `None` for events
/// the TUI doesn't act on (key releases, bare mouse motion, focus, paste).
fn translate(event: Event) -> Option<AppEvent> {
    match event {
        Event::Key(key) if key.kind == KeyEventKind::Release => None,
        Event::Key(key) => Some(AppEvent::Key(key)),
        Event::Resize(_, _) => Some(AppEvent::Resize),
        // Motion with no button held should not arrive at all — `?1003h` is
        // deliberately off, see MOUSE_ON — but a terminal that sends it
        // anyway must not wake the loop for events nothing acts on.
        Event::Mouse(mouse) if matches!(mouse.kind, MouseEventKind::Moved) => None,
        Event::Mouse(mouse) => Some(AppEvent::Mouse(mouse, std::time::Instant::now())),
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

    /// Bare motion never wakes the loop; a drag always does — dragging is how
    /// selection works.
    #[test]
    fn bare_motion_dropped_drags_forwarded() {
        use crossterm::event::{MouseButton, MouseEvent, MouseEventKind};

        let moved = Event::Mouse(MouseEvent {
            kind: MouseEventKind::Moved,
            column: 10,
            row: 5,
            modifiers: KeyModifiers::NONE,
        });
        assert!(translate(moved).is_none());

        let drag = Event::Mouse(MouseEvent {
            kind: MouseEventKind::Drag(MouseButton::Left),
            column: 1,
            row: 1,
            modifiers: KeyModifiers::NONE,
        });
        assert!(translate(drag).is_some());
    }
}
