//! What the consumer does with each message, kept free of Kafka so it can be
//! tested directly.
//!
//! The stream is at-least-once: after a crash, messages processed but not yet
//! committed come back. Every event carries a unique `tx_id`, so repeats are
//! recognised and skipped instead of being counted twice.
//!
//! Every new payment goes into the time-windowed graph. The window moves with
//! the watermark, the slowest partition's clock, so a partition that is
//! behind never has its payments evicted before they arrive.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use detektive_graph::{Arrival, Features, Insert, Payment, Watermarks, WindowGraph};

use crate::bitset::BitSet;
use crate::event::{DecodeError, decode};

#[derive(Debug, PartialEq)]
pub enum Outcome {
    Processed {
        /// Seconds this event's time is behind the newest event seen before
        /// it, from any partition (0 when it arrives in order).
        lateness: i64,
        /// Seconds it is behind the newest event from its own partition.
        /// Should always be 0: each partition arrives in order.
        behind_partition: i64,
        /// Already older than the graph window, so not added to it.
        too_late: bool,
        /// Degree and fan features of the payment, read from the window
        /// right after it went in. `None` if it was too late to go in.
        features: Option<Features>,
    },
    Duplicate,
    Bad(DecodeError),
}

/// How much event time the graph keeps, and when a silent partition stops
/// holding the watermark back.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GraphSettings {
    pub window: Duration,
    pub idle_partition: Duration,
}

impl Default for GraphSettings {
    fn default() -> Self {
        Self {
            window: Duration::from_secs(24 * 3600),
            idle_partition: Duration::from_secs(10),
        }
    }
}

#[derive(Debug)]
pub struct State {
    seen_tx: BitSet,
    seen_accounts: BitSet,
    pub processed: u64,
    pub duplicates: u64,
    pub bad: u64,
    /// Events older than the newest one seen so far. With several partitions,
    /// or a sender running late, event time and arrival order differ.
    pub out_of_order: u64,
    /// Largest event time seen (seconds since the epoch).
    pub max_ts: Option<i64>,
    /// The latest any event arrived, in seconds of event time.
    pub max_lateness: i64,
    pub by_format: BTreeMap<String, u64>,
    pub watermarks: Watermarks,
    pub graph: WindowGraph,
}

impl Default for State {
    fn default() -> Self {
        Self::new(GraphSettings::default())
    }
}

impl State {
    pub fn new(settings: GraphSettings) -> Self {
        Self {
            seen_tx: BitSet::default(),
            seen_accounts: BitSet::default(),
            processed: 0,
            duplicates: 0,
            bad: 0,
            out_of_order: 0,
            max_ts: None,
            max_lateness: 0,
            by_format: BTreeMap::new(),
            watermarks: Watermarks::new(settings.idle_partition),
            graph: WindowGraph::new(settings.window.as_secs() as i64),
        }
    }

    /// Handle one message from `partition`, read at `now`.
    pub fn handle(&mut self, partition: u32, payload: Option<&[u8]>, now: Instant) -> Outcome {
        let event = match decode(payload) {
            Ok(e) => e,
            Err(e) => {
                self.bad += 1;
                return Outcome::Bad(e);
            }
        };
        if !self.seen_tx.insert(event.tx_id) {
            self.duplicates += 1;
            return Outcome::Duplicate;
        }

        self.processed += 1;
        self.seen_accounts.insert(event.src);
        self.seen_accounts.insert(event.dst);
        let lateness = match self.max_ts {
            Some(max) if event.ts < max => {
                self.out_of_order += 1;
                max - event.ts
            }
            _ => {
                self.max_ts = Some(event.ts);
                0
            }
        };
        self.max_lateness = self.max_lateness.max(lateness);
        // only allocates the first time a format is seen
        match self.by_format.get_mut(event.format.as_ref()) {
            Some(n) => *n += 1,
            None => {
                self.by_format.insert(event.format.into_owned(), 1);
            }
        }

        let behind_partition = match self.watermarks.observe(partition, event.ts, now) {
            Arrival::InOrder => 0,
            Arrival::BehindPartition(secs) => secs,
        };
        if let Some(w) = self.watermarks.advance(now) {
            self.graph.advance(w);
        }
        let payment = Payment {
            tx: event.tx_id,
            ts: event.ts,
            src: event.src,
            dst: event.dst,
            amount: event.amount,
        };
        let inserted = self.graph.insert(payment);
        Outcome::Processed {
            lateness,
            behind_partition,
            too_late: inserted == Insert::TooLate,
            features: (inserted == Insert::Added).then(|| self.graph.features(&payment)),
        }
    }

    /// The partitions this consumer now owns (after a rebalance).
    pub fn assign(&mut self, partitions: &[u32], now: Instant) {
        self.watermarks.assign(partitions, now);
    }

    /// Events that arrived older than something their own partition had
    /// already delivered, summed over partitions.
    pub fn behind_partition(&self) -> u64 {
        self.watermarks.out_of_order().map(|(_, n)| n).sum()
    }

    /// Every message looked at, whatever its outcome.
    pub fn handled(&self) -> u64 {
        self.processed + self.duplicates + self.bad
    }

    pub fn accounts(&self) -> usize {
        self.seen_accounts.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(tx_id: u32, ts: i64, src: u32, dst: u32, format: &str) -> Vec<u8> {
        format!(
            r#"{{"tx_id":{tx_id},"ts":{ts},"src":{src},"dst":{dst},"amount":1.0,"currency":"USD","received_amount":1.0,"received_currency":"USD","format":"{format}"}}"#
        )
        .into_bytes()
    }

    fn in_order(lateness: i64) -> Outcome {
        Outcome::Processed {
            lateness,
            behind_partition: 0,
            too_late: false,
            features: None,
        }
    }

    /// The outcome without its features, which have their own tests.
    fn strip(o: Outcome) -> Outcome {
        match o {
            Outcome::Processed {
                lateness,
                behind_partition,
                too_late,
                ..
            } => Outcome::Processed {
                lateness,
                behind_partition,
                too_late,
                features: None,
            },
            other => other,
        }
    }

    /// Everything from partition 0, read now; features stripped.
    fn handle(s: &mut State, payload: Option<&[u8]>) -> Outcome {
        strip(s.handle(0, payload, Instant::now()))
    }

    #[test]
    fn counts_events_accounts_and_formats() {
        let mut s = State::default();
        for (id, ts, src, dst, fmt) in [
            (0, 10, 1, 2, "ach"),
            (1, 20, 2, 3, "ach"),
            (2, 30, 3, 3, "wire"),
        ] {
            assert_eq!(
                handle(&mut s, Some(&event(id, ts, src, dst, fmt))),
                in_order(0)
            );
        }
        assert_eq!(s.processed, 3);
        assert_eq!(s.accounts(), 3);
        assert_eq!(s.by_format["ach"], 2);
        assert_eq!(s.max_ts, Some(30));
    }

    #[test]
    fn redelivered_events_are_skipped() {
        let mut s = State::default();
        let e = event(9, 10, 1, 2, "ach");
        assert_eq!(handle(&mut s, Some(&e)), in_order(0));
        assert_eq!(handle(&mut s, Some(&e)), Outcome::Duplicate);
        assert_eq!((s.processed, s.duplicates), (1, 1));
        assert_eq!(s.graph.stats().edges, 1, "a repeat is not a second edge");
    }

    #[test]
    fn late_events_are_counted_but_do_not_move_time_back() {
        let mut s = State::default();
        handle(&mut s, Some(&event(0, 100, 1, 2, "ach")));
        // from another partition: late for the stream, in order for its own
        let now = Instant::now();
        assert_eq!(
            strip(s.handle(1, Some(&event(1, 50, 1, 2, "ach")), now)),
            in_order(50)
        );
        assert_eq!(
            strip(s.handle(1, Some(&event(2, 90, 1, 2, "ach")), now)),
            in_order(10)
        );
        assert_eq!(s.out_of_order, 2);
        assert_eq!(s.max_ts, Some(100));
        assert_eq!(s.max_lateness, 50);
        assert_eq!(s.behind_partition(), 0);
    }

    #[test]
    fn an_event_behind_its_own_partition_is_flagged() {
        let mut s = State::default();
        handle(&mut s, Some(&event(0, 100, 1, 2, "ach")));
        assert_eq!(
            handle(&mut s, Some(&event(1, 40, 1, 2, "ach"))),
            Outcome::Processed {
                lateness: 60,
                behind_partition: 60,
                too_late: false,
                features: None,
            }
        );
        assert_eq!(s.behind_partition(), 1);
    }

    #[test]
    fn the_window_follows_the_slowest_partition() {
        let hour = 3600;
        let mut s = State::new(GraphSettings {
            window: Duration::from_secs(hour as u64),
            idle_partition: Duration::from_secs(10),
        });
        let now = Instant::now();
        s.handle(0, Some(&event(0, 0, 1, 2, "ach")), now);
        s.handle(1, Some(&event(1, 0, 3, 4, "ach")), now);
        // partition 0 races three hours ahead; partition 1 is still at 0,
        // so nothing may be evicted yet
        s.handle(0, Some(&event(2, 3 * hour, 5, 6, "ach")), now);
        assert_eq!(s.graph.stats().edges, 3);
        // partition 1 catches up: now the first two leave the window
        s.handle(1, Some(&event(3, 3 * hour, 7, 8, "ach")), now);
        assert_eq!(s.graph.stats().edges, 2);
        assert_eq!(s.graph.stats().evicted, 2);
    }

    #[test]
    fn payments_older_than_the_window_are_not_added() {
        let mut s = State::new(GraphSettings {
            window: Duration::from_secs(60),
            idle_partition: Duration::from_secs(10),
        });
        let now = Instant::now();
        s.handle(0, Some(&event(0, 1000, 1, 2, "ach")), now);
        let outcome = s.handle(1, Some(&event(1, 900, 1, 2, "ach")), now);
        assert!(matches!(outcome, Outcome::Processed { too_late: true, .. }));
        assert_eq!(s.graph.stats().edges, 1);
    }

    #[test]
    fn each_payment_comes_with_its_features() {
        let mut s = State::default();
        let now = Instant::now();
        s.handle(0, Some(&event(0, 10, 1, 9, "ach")), now);
        s.handle(0, Some(&event(1, 20, 2, 9, "ach")), now);
        let Outcome::Processed {
            features: Some(f), ..
        } = s.handle(0, Some(&event(2, 30, 9, 5, "wire")), now)
        else {
            panic!("expected features");
        };
        assert_eq!((f.src_fan_in, f.src_out_degree, f.pair_count), (2, 1, 1));
    }

    #[test]
    fn bad_messages_are_reported_not_fatal() {
        let mut s = State::default();
        assert!(matches!(handle(&mut s, Some(b"garbage")), Outcome::Bad(_)));
        assert_eq!(handle(&mut s, None), Outcome::Bad(DecodeError::Empty));
        assert_eq!((s.bad, s.processed), (2, 0));
    }
}
