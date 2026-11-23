//! Watermarks: how far event time has safely progressed.
//!
//! Each partition of the topic arrives in event-time order (the replayer sends
//! in order, and the idempotent producer keeps order within a partition), but
//! the partitions are read at slightly different speeds. A single clock for
//! the whole stream, "the newest event seen", runs ahead of the slowest
//! partition: after a restart it ran days ahead while one partition caught up
//! (see docs/observability.md). Anything evicted on that clock would be thrown
//! away while events that still belong to it are on their way.
//!
//! So each partition keeps its own clock, the newest event time it has
//! delivered, and the stream's watermark is the slowest of them. Nothing
//! older than the watermark should still arrive from an in-order partition.
//!
//! A partition that stops delivering (quiet, or moved to another consumer in
//! a rebalance) would hold the watermark back forever. After `idle_after` of
//! wall-clock silence it is left out until it delivers again. That is a
//! trade: if it resumes with older events, they arrive behind the watermark.
//! The graph still takes them if they are inside its window.

use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arrival {
    /// Not older than anything its partition delivered before.
    InOrder,
    /// Older than an event its own partition already delivered, by this many
    /// seconds of event time. Should not happen; counted to prove it.
    BehindPartition(i64),
}

#[derive(Debug, Clone)]
struct PartitionClock {
    newest: i64,
    last_seen: Instant,
    out_of_order: u64,
}

#[derive(Debug, Clone)]
pub struct Watermarks {
    partitions: Vec<Option<PartitionClock>>,
    idle_after: Duration,
    current: Option<i64>,
}

impl Watermarks {
    pub fn new(idle_after: Duration) -> Self {
        Self {
            partitions: Vec::new(),
            idle_after,
            current: None,
        }
    }

    /// Record an event from `partition` with event time `ts`, read at `now`.
    pub fn observe(&mut self, partition: u32, ts: i64, now: Instant) -> Arrival {
        let i = partition as usize;
        if i >= self.partitions.len() {
            self.partitions.resize(i + 1, None);
        }
        match &mut self.partitions[i] {
            None => {
                self.partitions[i] = Some(PartitionClock {
                    newest: ts,
                    last_seen: now,
                    out_of_order: 0,
                });
                Arrival::InOrder
            }
            Some(clock) => {
                clock.last_seen = now;
                if ts < clock.newest {
                    clock.out_of_order += 1;
                    Arrival::BehindPartition(clock.newest - ts)
                } else {
                    clock.newest = ts;
                    Arrival::InOrder
                }
            }
        }
    }

    /// Recompute the stream's watermark: the slowest partition that is not
    /// idle at `now`. If every partition is idle (the stream has stopped),
    /// all of them count. It never moves backwards.
    pub fn advance(&mut self, now: Instant) -> Option<i64> {
        let known = || self.partitions.iter().flatten();
        let active = known()
            .filter(|c| now.saturating_duration_since(c.last_seen) < self.idle_after)
            .map(|c| c.newest)
            .min();
        let slowest = active.or_else(|| known().map(|c| c.newest).min());
        if let Some(w) = slowest {
            self.current = Some(self.current.map_or(w, |c| c.max(w)));
        }
        self.current
    }

    /// The watermark as of the last `advance`.
    pub fn current(&self) -> Option<i64> {
        self.current
    }

    /// For each partition seen: (partition, events that arrived older than
    /// something it had already delivered).
    pub fn out_of_order(&self) -> impl Iterator<Item = (u32, u64)> + '_ {
        self.partitions
            .iter()
            .enumerate()
            .filter_map(|(i, c)| c.as_ref().map(|c| (i as u32, c.out_of_order)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const IDLE: Duration = Duration::from_secs(10);

    #[test]
    fn the_watermark_is_the_slowest_partition() {
        let t0 = Instant::now();
        let mut w = Watermarks::new(IDLE);
        w.observe(0, 100, t0);
        w.observe(1, 40, t0);
        w.observe(2, 70, t0);
        assert_eq!(w.advance(t0), Some(40));
        w.observe(1, 90, t0);
        assert_eq!(w.advance(t0), Some(70));
    }

    #[test]
    fn it_never_moves_backwards() {
        let t0 = Instant::now();
        let mut w = Watermarks::new(IDLE);
        w.observe(0, 100, t0);
        assert_eq!(w.advance(t0), Some(100));
        // a partition seen for the first time, behind the others
        w.observe(1, 20, t0);
        assert_eq!(w.advance(t0), Some(100));
    }

    #[test]
    fn idle_partitions_stop_holding_it_back() {
        let t0 = Instant::now();
        let mut w = Watermarks::new(IDLE);
        w.observe(0, 10, t0);
        w.observe(1, 50, t0);
        assert_eq!(w.advance(t0), Some(10));

        let later = t0 + Duration::from_secs(11);
        w.observe(1, 60, later);
        assert_eq!(w.advance(later), Some(60), "partition 0 is idle");

        // once it delivers again it counts again, but cannot pull the
        // watermark back
        w.observe(0, 30, later);
        assert_eq!(w.advance(later), Some(60));
        w.observe(0, 80, later);
        w.observe(1, 100, later);
        assert_eq!(w.advance(later), Some(80));
    }

    #[test]
    fn a_stopped_stream_keeps_its_last_watermark() {
        let t0 = Instant::now();
        let mut w = Watermarks::new(IDLE);
        w.observe(0, 10, t0);
        w.observe(1, 50, t0);
        let much_later = t0 + Duration::from_secs(60);
        // every partition idle: all count again, so the slowest still rules
        assert_eq!(w.advance(much_later), Some(10));
    }

    #[test]
    fn events_behind_their_own_partition_are_counted() {
        let t0 = Instant::now();
        let mut w = Watermarks::new(IDLE);
        assert_eq!(w.observe(0, 100, t0), Arrival::InOrder);
        assert_eq!(w.observe(0, 100, t0), Arrival::InOrder, "ties are in order");
        assert_eq!(w.observe(0, 70, t0), Arrival::BehindPartition(30));
        assert_eq!(w.observe(1, 5, t0), Arrival::InOrder);
        assert_eq!(w.out_of_order().collect::<Vec<_>>(), vec![(0, 1), (1, 0)]);
    }

    #[test]
    fn nothing_seen_means_no_watermark() {
        let mut w = Watermarks::new(IDLE);
        assert_eq!(w.advance(Instant::now()), None);
    }
}
