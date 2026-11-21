//! Replays IBM AML transactions as a time-ordered payment stream.
//!
//! The pieces, in the order data flows through them:
//! `load` reads and sorts the CSV, `accounts` gives every account a small id,
//! `pace` decides when each event is due, `event` shapes what is sent, and
//! `sink` decides where it goes (`kafka` adds Redpanda, behind the `kafka`
//! feature).

pub mod accounts;
pub mod event;
pub mod india;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod load;
pub mod model;
pub mod pace;
pub mod sink;

use std::time::{Duration, Instant};

use anyhow::Result;

use crate::event::{Event, EventOptions};
use crate::model::Transaction;
use crate::pace::{Pacer, Speed};
use crate::sink::Sink;

#[derive(Debug, Clone, Copy)]
pub struct ReplayOptions {
    pub speed: Speed,
    pub limit: Option<usize>,
    pub event: EventOptions,
}

#[derive(Debug, Clone, Copy)]
pub struct ReplayStats {
    pub sent: usize,
    pub elapsed: Duration,
}

impl ReplayStats {
    pub fn per_second(&self) -> f64 {
        self.sent as f64 / self.elapsed.as_secs_f64().max(f64::EPSILON)
    }
}

/// Send `transactions` (already in time order) to `sink` at the chosen speed.
pub fn replay(
    transactions: &[Transaction],
    opts: ReplayOptions,
    sink: &mut dyn Sink,
) -> Result<ReplayStats> {
    let take = opts
        .limit
        .unwrap_or(transactions.len())
        .min(transactions.len());
    let mut pacer = Pacer::new(opts.speed);
    let started = Instant::now();

    for t in &transactions[..take] {
        pacer.wait_for(t.ts);
        sink.send(&Event::from_transaction(t, opts.event))?;
    }
    sink.flush()?;

    Ok(ReplayStats {
        sent: take,
        elapsed: started.elapsed(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::JsonLines;

    const SAMPLE: &str = include_str!("../../../ml/tests/fixtures/tiny_trans.csv");

    #[test]
    fn replays_in_time_order_and_respects_the_limit() {
        let data = load::from_reader(SAMPLE.as_bytes()).unwrap();
        let mut buf = Vec::new();
        let opts = ReplayOptions {
            speed: Speed::Max,
            limit: Some(3),
            event: EventOptions::default(),
        };
        let stats = replay(&data.transactions, opts, &mut JsonLines::new(&mut buf)).unwrap();

        assert_eq!(stats.sent, 3);
        let ids: Vec<u64> = String::from_utf8(buf)
            .unwrap()
            .lines()
            .map(|l| {
                serde_json::from_str::<serde_json::Value>(l).unwrap()["tx_id"]
                    .as_u64()
                    .unwrap()
            })
            .collect();
        assert_eq!(ids, vec![2, 3, 4]);
    }

    #[test]
    fn paced_replay_takes_roughly_the_scaled_time() {
        let data = load::from_reader(SAMPLE.as_bytes()).unwrap();
        // the fixture spans 20 minutes; at speed 6000 that is 0.2 seconds
        let opts = ReplayOptions {
            speed: Speed::Factor(6000.0),
            limit: None,
            event: EventOptions::default(),
        };
        let stats = replay(&data.transactions, opts, &mut sink::Discard).unwrap();
        assert!(
            stats.elapsed >= Duration::from_millis(195),
            "{:?}",
            stats.elapsed
        );
        assert!(
            stats.elapsed < Duration::from_millis(400),
            "{:?}",
            stats.elapsed
        );
    }
}
