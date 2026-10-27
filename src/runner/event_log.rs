//! The runner's event broadcast, numbered and kept so `GET /events?since=N`
//! can replay what a late subscriber missed (recording and sending share a lock).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::broadcast;

use super::RunnerEvent;

/// Events kept for replay. A boot of a large stack emits a few per process.
pub const HISTORY_CAPACITY: usize = 8192;
const CHANNEL_CAPACITY: usize = 256;

/// An event as the runner sent it.
#[derive(Debug, Clone)]
pub struct RecordedEvent {
    /// Position in this run's event stream, starting at 1.
    pub seq: u64,
    /// Milliseconds since the Unix epoch when the runner sent it.
    pub wall_clock: u64,
    pub event: RunnerEvent,
}

/// A live receiver plus whatever history the caller asked to replay first.
pub struct EventSubscription {
    /// Requested events no longer in history, which precede `replay`.
    pub skipped: u64,
    pub replay: Vec<RecordedEvent>,
    /// Sequence number of the first event `live` will yield.
    pub next_seq: u64,
    pub live: broadcast::Receiver<RunnerEvent>,
}

/// Cloneable handle on the runner's event stream.
#[derive(Clone)]
pub struct EventLog {
    tx: broadcast::Sender<RunnerEvent>,
    history: Arc<Mutex<History>>,
}

struct History {
    next_seq: u64,
    capacity: usize,
    entries: VecDeque<RecordedEvent>,
}

impl History {
    fn recorded(&self, from: u64, to: u64) -> (u64, Vec<RecordedEvent>) {
        let kept: Vec<RecordedEvent> = self
            .entries
            .iter()
            .filter(|recorded| recorded.seq >= from && recorded.seq < to)
            .cloned()
            .collect();
        let wanted = to.saturating_sub(from);
        let kept_count = u64::try_from(kept.len()).unwrap_or(wanted);
        (wanted.saturating_sub(kept_count), kept)
    }
}

impl Default for EventLog {
    fn default() -> Self {
        Self::with_capacity(CHANNEL_CAPACITY, HISTORY_CAPACITY)
    }
}

impl EventLog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(channel: usize, history: usize) -> Self {
        let (tx, _) = broadcast::channel(channel);
        Self {
            tx,
            history: Arc::new(Mutex::new(History {
                next_seq: 1,
                capacity: history.max(1),
                entries: VecDeque::new(),
            })),
        }
    }

    /// Record `event` and broadcast it to every live subscriber.
    pub fn send(&self, event: RunnerEvent) {
        let mut history = self.lock();
        let seq = history.next_seq;
        history.next_seq += 1;
        if history.entries.len() == history.capacity {
            history.entries.pop_front();
        }
        history.entries.push_back(RecordedEvent {
            seq,
            wall_clock: now_ms(),
            event: event.clone(),
        });
        let _ = self.tx.send(event);
    }

    /// Live events only, as a plain broadcast receiver.
    pub fn subscribe(&self) -> broadcast::Receiver<RunnerEvent> {
        self.tx.subscribe()
    }

    /// Subscribe, replaying recorded events with `seq > since` first (`None`
    /// replays nothing). A `since` at or past this run's next `seq` can only
    /// come from an earlier run, so it replays the whole history.
    pub fn subscribe_since(&self, since: Option<u64>) -> EventSubscription {
        let history = self.lock();
        let live = self.tx.subscribe();
        let (skipped, replay) = match since {
            Some(since) if since < history.next_seq => {
                history.recorded(since + 1, history.next_seq)
            }
            Some(_) => history.recorded(1, history.next_seq),
            None => (0, Vec::new()),
        };
        EventSubscription {
            skipped,
            replay,
            next_seq: history.next_seq,
            live,
        }
    }

    /// Recorded events with `from <= seq < to`, and how many of those are no
    /// longer in history.
    pub fn recorded(&self, from: u64, to: u64) -> (u64, Vec<RecordedEvent>) {
        self.lock().recorded(from, to)
    }

    /// When the event numbered `seq` was sent, if it is still in history.
    pub fn wall_clock(&self, seq: u64) -> Option<u64> {
        let history = self.lock();
        let first = history.entries.front()?.seq;
        let index = usize::try_from(seq.checked_sub(first)?).ok()?;
        history
            .entries
            .get(index)
            .map(|recorded| recorded.wall_clock)
    }

    fn lock(&self) -> MutexGuard<'_, History> {
        self.history
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn settled(elapsed_ms: u64) -> RunnerEvent {
        RunnerEvent::StartupSettled { elapsed_ms }
    }

    fn elapsed(event: &RunnerEvent) -> u64 {
        match event {
            RunnerEvent::StartupSettled { elapsed_ms } => *elapsed_ms,
            other => panic!("unexpected event {other:?}"),
        }
    }

    #[test]
    fn replays_events_sent_before_subscribing() {
        let log = EventLog::new();
        log.send(settled(1));
        log.send(settled(2));

        let sub = log.subscribe_since(Some(0));

        let replayed: Vec<(u64, u64)> = sub
            .replay
            .iter()
            .map(|recorded| (recorded.seq, elapsed(&recorded.event)))
            .collect();
        assert_eq!(replayed, vec![(1, 1), (2, 2)]);
        assert_eq!(sub.next_seq, 3);
    }

    #[test]
    fn since_skips_what_the_caller_already_has() {
        let log = EventLog::new();
        for n in 1..=3 {
            log.send(settled(n));
        }

        let sub = log.subscribe_since(Some(2));

        let seqs: Vec<u64> = sub.replay.iter().map(|recorded| recorded.seq).collect();
        assert_eq!(seqs, vec![3]);
    }

    #[test]
    fn no_since_replays_nothing() {
        let log = EventLog::new();
        log.send(settled(1));

        assert!(log.subscribe_since(None).replay.is_empty());
    }

    #[test]
    fn live_continues_exactly_where_replay_ends() {
        let log = EventLog::new();
        log.send(settled(1));
        let mut sub = log.subscribe_since(Some(0));
        log.send(settled(2));

        assert_eq!(sub.replay.len(), 1);
        assert_eq!(sub.next_seq, 2);
        let live = sub.live.try_recv().expect("live event");
        assert_eq!(elapsed(&live), 2);
        assert!(sub.live.try_recv().is_err());
        assert!(log.wall_clock(sub.next_seq).is_some());
    }

    #[test]
    fn history_is_bounded() {
        let log = EventLog::with_capacity(16, 2);
        for n in 1..=5 {
            log.send(settled(n));
        }

        let sub = log.subscribe_since(Some(0));
        let seqs: Vec<u64> = sub.replay.iter().map(|recorded| recorded.seq).collect();
        assert_eq!(seqs, vec![4, 5]);
        assert_eq!(sub.skipped, 3);
        assert_eq!(log.wall_clock(1), None);
    }

    #[test]
    fn a_since_from_an_earlier_run_replays_the_whole_history() {
        let log = EventLog::new();
        log.send(settled(1));
        log.send(settled(2));

        let sub = log.subscribe_since(Some(50));

        let seqs: Vec<u64> = sub.replay.iter().map(|recorded| recorded.seq).collect();
        assert_eq!(seqs, vec![1, 2]);
        assert_eq!(sub.skipped, 0);
    }

    #[test]
    fn recorded_counts_what_left_history() {
        let log = EventLog::with_capacity(16, 2);
        for n in 1..=5 {
            log.send(settled(n));
        }

        let (gone, kept) = log.recorded(2, 6);

        assert_eq!(gone, 2);
        let seqs: Vec<u64> = kept.iter().map(|recorded| recorded.seq).collect();
        assert_eq!(seqs, vec![4, 5]);
    }
}
