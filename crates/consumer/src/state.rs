//! What the consumer does with each message, kept free of Kafka so it can be
//! tested directly.
//!
//! The stream is at-least-once: after a crash, messages processed but not yet
//! committed come back. Every event carries a unique `tx_id`, so repeats are
//! recognised and skipped instead of being counted twice.

use std::collections::BTreeMap;

use crate::bitset::BitSet;
use crate::event::{DecodeError, decode};

#[derive(Debug, PartialEq)]
pub enum Outcome {
    /// `lateness`: seconds this event's time is behind the newest event seen
    /// before it (0 when it arrives in order).
    Processed {
        lateness: i64,
    },
    Duplicate,
    Bad(DecodeError),
}

#[derive(Debug, Default)]
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
}

impl State {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn handle(&mut self, payload: Option<&[u8]>) -> Outcome {
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
        Outcome::Processed { lateness }
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

    #[test]
    fn counts_events_accounts_and_formats() {
        let mut s = State::new();
        for (id, ts, src, dst, fmt) in [
            (0, 10, 1, 2, "ach"),
            (1, 20, 2, 3, "ach"),
            (2, 30, 3, 3, "wire"),
        ] {
            assert_eq!(
                s.handle(Some(&event(id, ts, src, dst, fmt))),
                Outcome::Processed { lateness: 0 }
            );
        }
        assert_eq!(s.processed, 3);
        assert_eq!(s.accounts(), 3);
        assert_eq!(s.by_format["ach"], 2);
        assert_eq!(s.max_ts, Some(30));
    }

    #[test]
    fn redelivered_events_are_skipped() {
        let mut s = State::new();
        let e = event(9, 10, 1, 2, "ach");
        assert_eq!(s.handle(Some(&e)), Outcome::Processed { lateness: 0 });
        assert_eq!(s.handle(Some(&e)), Outcome::Duplicate);
        assert_eq!((s.processed, s.duplicates), (1, 1));
    }

    #[test]
    fn late_events_are_counted_but_do_not_move_time_back() {
        let mut s = State::new();
        s.handle(Some(&event(0, 100, 1, 2, "ach")));
        assert_eq!(
            s.handle(Some(&event(1, 50, 1, 2, "ach"))),
            Outcome::Processed { lateness: 50 }
        );
        assert_eq!(
            s.handle(Some(&event(2, 90, 1, 2, "ach"))),
            Outcome::Processed { lateness: 10 }
        );
        assert_eq!(s.out_of_order, 2);
        assert_eq!(s.max_ts, Some(100));
        assert_eq!(s.max_lateness, 50);
    }

    #[test]
    fn bad_messages_are_reported_not_fatal() {
        let mut s = State::new();
        assert!(matches!(s.handle(Some(b"garbage")), Outcome::Bad(_)));
        assert_eq!(s.handle(None), Outcome::Bad(DecodeError::Empty));
        assert_eq!((s.bad, s.processed), (2, 0));
    }
}
