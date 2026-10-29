//! Visible progress before the runner's API is available.

use std::future::Future;

use crossterm::event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers};
use futures_util::StreamExt;
use ratatui::layout::{Constraint, Layout};
use ratatui::text::Text;
use ratatui::widgets::{Paragraph, Wrap};
use tokio::sync::watch;

use super::{TerminalGuard, TuiError, build_terminal, parse_ansi_line};

/// Render startup progress while awaiting work that brings a runner online.
///
/// `progress` contains the current bounded log tail. Ctrl+C raises SIGINT, so
/// the caller must install its signal handler before calling this function.
/// The terminal is restored on completion, cancellation, or error; the caller
/// can then enter the main TUI. Dropping this future cancels `work` as well.
pub async fn run_startup_tui<T>(
    work: impl Future<Output = T>,
    progress: watch::Receiver<Vec<String>>,
) -> Result<T, TuiError> {
    let _guard = TerminalGuard::enter()?;
    let mut terminal = build_terminal()?;
    let mut input = EventStream::new();
    let mut input_open = true;
    let started = std::time::Instant::now();
    let mut stopping = false;
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(100));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tokio::pin!(work);

    let mut draw = |stopping: bool| -> Result<(), TuiError> {
        let lines = progress
            .borrow()
            .iter()
            .map(|line| parse_ansi_line(line.as_bytes()))
            .collect::<Vec<_>>();
        terminal.draw(|frame| {
            let [logs, status] =
                Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).areas(frame.area());
            let paragraph = Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false });
            let tail = paragraph
                .line_count(logs.width)
                .saturating_sub(usize::from(logs.height));
            let scroll = u16::try_from(tail).unwrap_or(u16::MAX);
            frame.render_widget(paragraph.scroll((scroll, 0)), logs);
            let phase = if stopping {
                "Stopping Don"
            } else {
                "Starting Don"
            };
            frame.render_widget(
                Paragraph::new(format!(
                    " {phase}... {:.1}s  |  Ctrl+C to stop",
                    started.elapsed().as_secs_f32()
                )),
                status,
            );
        })?;
        Ok(())
    };
    // Draw before polling work, including when it completes synchronously.
    draw(stopping)?;
    loop {
        tokio::select! {
            result = &mut work => return Ok(result),
            _ = ticker.tick() => draw(stopping)?,
            event = input.next(), if input_open => match event {
                Some(Ok(Event::Key(key)))
                    if key.kind != KeyEventKind::Release
                        && key.code == KeyCode::Char('c')
                        && key.modifiers.contains(KeyModifiers::CONTROL) => {
                    stopping = true;
                    let _ = nix::sys::signal::kill(
                        nix::unistd::Pid::this(),
                        nix::sys::signal::Signal::SIGINT,
                    );
                }
                Some(Err(error)) => return Err(error.into()),
                None => input_open = false,
                _ => {}
            }
        }
    }
}
