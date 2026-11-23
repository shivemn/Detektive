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
//! A partition assigned to this consumer that has not delivered anything yet
//! holds the watermark where it is: its first events could be far older than
//! what the other partitions have sent. Without this, a consumer catching up
//! on a backlog would read a burst from one partition, move the watermark
//! days ahead on that partition's clock alone, and then refuse everything
//! the other partitions bring as too late. `assign` tells it which
//! partitions to wait for.
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
    /// `None`: assigned, nothing delivered yet.
    newest: Option<i64>,
    /// Last delivery, or the assignment if nothing came yet.
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

    /// The partitions this consumer now owns, at `now`. New ones hold the
    /// watermark until they deliver (or go idle); ones no longer listed stop
    /// counting.
    pub fn assign(&mut self, partitions: &[u32], now: Instant) {
        for (i, clock) in self.partitions.iter_mut().enumerate() {
            if !partitions.contains(&(i as u32)) {
                *clock = None;
            }
        }
        for &p in partitions {
            self.slot(p).get_or_insert(PartitionClock {
                newest: None,
                last_seen: now,
                out_of_order: 0,
            });
        }
    }

    fn slot(&mut self, partition: u32) -> &mut Option<PartitionClock> {
        let i = partition as usize;
        if i >= self.partitions.len() {
            self.partitions.resize(i + 1, None);
        }
        &mut self.partitions[i]
    }

    /// Record an event from `partition` with event time `ts`, read at `now`.
    pub fn observe(&mut self, partition: u32, ts: i64, now: Instant) -> Arrival {
        let clock = self.slot(partition).get_or_insert(PartitionClock {
            newest: None,
            last_seen: now,
            out_of_order: 0,
        });
        clock.last_seen = now;
        match clock.newest {
            Some(newest) if ts < newest => {
                clock.out_of_order += 1;
                Arrival::BehindPartition(newest - ts)
            }
            _ => {
                clock.newest = Some(ts);
                Arrival::InOrder
            }
        }
    }

    /// Recompute the stream's watermark: the slowest partition that is not
    /// idle at `now`. It stays put while an active partition has delivered
    /// nothing yet. If every partition is idle (the stream has stopped), all
    /// of them count. It never moves backwards.
    pub fn advance(&mut self, now: Instant) -> Option<i64> {
        let known = || self.partitions.iter().flatten();
        let active: Vec<Option<i64>> = known()
            .filter(|c| now.saturating_duration_since(c.last_seen) < self.idle_after)
            .map(|c| c.newest)
            .collect();
        if active.contains(&None) {
            return self.current;
        }
        let slowest = match active.iter().flatten().min() {
            Some(&t) => Some(t),
            None => known().filter_map(|c| c.newest).min(),
        };
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
    fn an_assigned_partition_holds_it_until_it_delivers() {
        let t0 = Instant::now();
        let mut w = Watermarks::new(IDLE);
        w.assign(&[0, 1, 2], t0);
        // a backlog: partition 0 delivers a burst first
        w.observe(0, 500, t0);
        assert_eq!(w.advance(t0), None, "1 and 2 have sent nothing yet");
        w.observe(1, 100, t0);
        assert_eq!(w.advance(t0), None);
        w.observe(2, 300, t0);
        assert_eq!(w.advance(t0), Some(100));
    }

    #[test]
    fn a_silent_assigned_partition_goes_idle_too() {
        let t0 = Instant::now();
        let mut w = Watermarks::new(IDLE);
        w.assign(&[0, 1], t0);
        w.observe(0, 500, t0);
        let later = t0 + Duration::from_secs(11);
        w.observe(0, 600, later);
        assert_eq!(w.advance(later), Some(600));
    }

    #[test]
    fn revoked_partitions_stop_counting() {
        let t0 = Instant::now();
        let mut w = Watermarks::new(IDLE);
        w.observe(0, 50, t0);
        w.observe(1, 500, t0);
        assert_eq!(w.advance(t0), Some(50));
        w.assign(&[1], t0);
        assert_eq!(w.advance(t0), Some(500));
        assert_eq!(
            w.out_of_order().map(|(p, _)| p).collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn nothing_seen_means_no_watermark() {
        let mut w = Watermarks::new(IDLE);
        assert_eq!(w.advance(Instant::now()), None);
    }
}
