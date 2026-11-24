//! The window graph, the pattern searches and the cycle counts per account,
//! moved together: one call to add a payment and get its features.
//!
//! **Batches in parallel.** About a third of the time per payment goes to the
//! cycle search, and searches only read the graph, so a batch of payments can
//! be searched on several threads at once. The catch is that the answer must
//! not change: each search has to see the graph exactly as it was right
//! after its own payment went in, not as it is after the whole batch.
//! `add_batch` does it in three passes:
//!
//! 1. one thread adds the batch payment by payment, moving the window as it
//!    goes, and reads the cheap features (degree, fan, scatter-gather) at
//!    the right moment. Payments that leave the window are counted out at
//!    once but left in the adjacency lists for now;
//! 2. many threads search for cycles, each payment against the window as it
//!    was for it: no payment older than its cutoff then, and none inserted
//!    after it (every edge carries its insertion order);
//! 3. one thread, in order again, folds the cycles found into the per-account
//!    counts and finishes each payment's features.
//!
//! Then the lists are tidied. The result is identical to adding the payments
//! one by one; `tests/batch.rs` checks that on random streams.

use rayon::prelude::*;

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use rustc_hash::{FxHashMap, FxHasher};

use crate::cycles::{CycleFinder, Cycles, Hop};
use crate::features::Features;
use crate::graph::{Insert, Payment, WindowGraph};
use crate::scatter_gather::{ScatterGather, scatter_gather};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EngineConfig {
    /// Seconds of event time the graph keeps. Cycles must fit inside it.
    pub window: i64,
    /// Longest cycle searched for, in payments.
    pub max_cycle_len: usize,
    /// Payments one cycle search may look at before it stops.
    pub cycle_budget: u32,
    /// Accounts one scatter-gather search may look at before it stops.
    pub scatter_gather_budget: u32,
}

impl Default for EngineConfig {
    /// Four days and twelve payments: every cycle in HI-Small's laundering
    /// patterns has 2 to 12 payments and finishes within 96 hours.
    fn default() -> Self {
        Self {
            window: 4 * 24 * 3600,
            max_cycle_len: 12,
            cycle_budget: 100_000,
            scatter_gather_budget: 100_000,
        }
    }
}

/// What the engine says about one payment.
#[derive(Debug, Clone, PartialEq)]
pub struct Observation {
    /// The payment this is about.
    pub tx: u32,
    pub features: Features,
    pub cycles: Cycles,
    pub scatter_gather: ScatterGather,
}

#[derive(Debug)]
pub struct Engine {
    graph: WindowGraph,
    finder: CycleFinder,
    members: CycleMembers,
    max_cycle_len: usize,
    cycle_budget: u32,
    sg_budget: u32,
}

/// A payment that went into the window, with what was read at that moment.
struct Placed {
    p: Payment,
    cutoff: i64,
    seq: u32,
    features: Features,
    scatter_gather: ScatterGather,
}

impl Engine {
    pub fn new(cfg: EngineConfig) -> Self {
        Self {
            graph: WindowGraph::new(cfg.window),
            finder: CycleFinder::new(cfg.max_cycle_len, cfg.cycle_budget),
            members: CycleMembers::default(),
            max_cycle_len: cfg.max_cycle_len,
            cycle_budget: cfg.cycle_budget,
            sg_budget: cfg.scatter_gather_budget,
        }
    }

    pub fn graph(&self) -> &WindowGraph {
        &self.graph
    }

    /// Move the window to end at `watermark`; returns payments evicted.
    pub fn advance(&mut self, watermark: i64) -> usize {
        let evicted = self.graph.advance(watermark);
        if let Some(cutoff) = self.graph.cutoff() {
            self.members.expire(cutoff);
        }
        evicted
    }

    /// Add a payment and read its features. `None` if it was already older
    /// than the window.
    pub fn add(&mut self, p: Payment) -> Option<Observation> {
        let placed = self.place(p)?;
        let cycles = self
            .finder
            .closing_as_of(&self.graph, &placed.p, placed.cutoff, placed.seq);
        Some(self.finish(placed, cycles))
    }

    /// Add a batch of payments, each with the watermark to move the window
    /// to first, searching for cycles on rayon's threads. Gives exactly what
    /// `advance` and `add` would, payment by payment.
    pub fn add_batch(&mut self, batch: &[(i64, Payment)]) -> Vec<Option<Observation>> {
        self.graph.defer_tidying();
        let placed: Vec<Option<Placed>> = batch
            .iter()
            .map(|&(watermark, p)| {
                // the per-account cycle counts expire in the last pass, at
                // each payment's own cutoff
                self.graph.advance(watermark);
                self.place(p)
            })
            .collect();

        let graph = &self.graph;
        let (max_len, budget) = (self.max_cycle_len, self.cycle_budget);
        let cycles: Vec<Option<Cycles>> = placed
            .par_iter()
            .map_init(
                || CycleFinder::new(max_len, budget),
                |finder, placed| {
                    placed
                        .as_ref()
                        .map(|pl| finder.closing_as_of(graph, &pl.p, pl.cutoff, pl.seq))
                },
            )
            .collect();

        let out = placed
            .into_iter()
            .zip(cycles)
            .map(|(placed, cycles)| Some(self.finish(placed?, cycles?)))
            .collect();
        self.graph.tidy();
        out
    }

    /// Insert `p` and read what does not depend on the cycle search.
    fn place(&mut self, p: Payment) -> Option<Placed> {
        if self.graph.insert(p) == Insert::TooLate {
            return None;
        }
        Some(Placed {
            p,
            cutoff: self.graph.cutoff().unwrap_or(i64::MIN),
            seq: self.graph.last_seq(),
            features: self.graph.degree_features(&p),
            scatter_gather: scatter_gather(&self.graph, &p, self.sg_budget),
        })
    }

    /// Fold in the cycles `placed` closes and finish its features.
    fn finish(&mut self, placed: Placed, cycles: Cycles) -> Observation {
        let Placed {
            p,
            cutoff,
            mut features,
            scatter_gather,
            ..
        } = placed;
        self.members.expire(cutoff);
        if !cycles.example.is_empty() {
            let ring = &cycles.example;
            // counted before this payment's own rings are added
            features.ring_repeats = self.members.closings(ring_key(ring.iter().map(|h| h.src)));
            features.ring_one_format = u32::from(ring.iter().all(|h| h.format == ring[0].format));
            features.ring_amount_ratio = amount_ratio(ring);
        }
        for c in &cycles.found {
            self.members.add(c.oldest, &c.accounts);
        }
        features.cycles = cycles.count;
        features.shortest_cycle = cycles.shortest;
        features.src_cycles = self.members.of(p.src);
        features.dst_cycles = self.members.of(p.dst);
        features.scatter_gather = scatter_gather.width();
        Observation {
            tx: p.tx,
            features,
            cycles,
            scatter_gather,
        }
    }

    /// Cycles currently counted against accounts (found inside the window).
    pub fn live_cycles(&self) -> usize {
        self.members.live
    }
}

/// Smallest amount in the ring over the largest, in thousandths.
fn amount_ratio(ring: &[Hop]) -> u32 {
    let (lo, hi) = ring.iter().fold((f64::INFINITY, 0f64), |(lo, hi), h| {
        (lo.min(h.amount), hi.max(h.amount))
    });
    if hi > 0.0 {
        (1000.0 * lo / hi).round() as u32
    } else {
        1000
    }
}

/// The same set of accounts gives the same key, whichever account the ring
/// is read from.
fn ring_key(accounts: impl Iterator<Item = u32>) -> u64 {
    let mut sorted: Vec<u32> = accounts.collect();
    sorted.sort_unstable();
    let mut h = FxHasher::default();
    sorted.hash(&mut h);
    h.finish()
}

/// How many cycles found inside the window each account is part of, and how
/// many times each set of accounts closed one. A cycle stops counting when
/// its oldest payment leaves the window.
#[derive(Debug, Default)]
struct CycleMembers {
    counts: Vec<u32>,
    rings: FxHashMap<u64, u32>,
    expiry: BTreeMap<i64, Vec<(u64, Vec<u32>)>>,
    live: usize,
}

impl CycleMembers {
    fn add(&mut self, oldest: i64, accounts: &[u32]) {
        for &a in accounts {
            let i = a as usize;
            if i >= self.counts.len() {
                self.counts.resize(i + 1, 0);
            }
            self.counts[i] += 1;
        }
        let key = ring_key(accounts.iter().copied());
        *self.rings.entry(key).or_insert(0) += 1;
        self.expiry
            .entry(oldest)
            .or_default()
            .push((key, accounts.to_vec()));
        self.live += 1;
    }

    fn expire(&mut self, cutoff: i64) {
        while let Some(entry) = self.expiry.first_entry() {
            if *entry.key() >= cutoff {
                break;
            }
            for (key, accounts) in entry.remove() {
                for a in accounts {
                    self.counts[a as usize] -= 1;
                }
                let n = self.rings.get_mut(&key).expect("a counted ring");
                *n -= 1;
                if *n == 0 {
                    self.rings.remove(&key);
                }
                self.live -= 1;
            }
        }
    }

    /// Cycles through exactly this set of accounts found inside the window.
    fn closings(&self, key: u64) -> u32 {
        self.rings.get(&key).copied().unwrap_or(0)
    }

    fn of(&self, account: u32) -> u32 {
        self.counts.get(account as usize).copied().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pay(tx: u32, ts: i64, src: u32, dst: u32) -> Payment {
        Payment {
            tx,
            ts,
            src,
            dst,
            amount: 1.0,
            format: 0,
        }
    }

    fn engine(window: i64) -> Engine {
        Engine::new(EngineConfig {
            window,
            ..EngineConfig::default()
        })
    }

    #[test]
    fn a_closed_ring_marks_its_accounts_until_it_leaves_the_window() {
        let mut e = engine(100);
        for p in [pay(0, 10, 1, 2), pay(1, 20, 2, 3)] {
            e.advance(p.ts);
            assert_eq!(e.add(p).unwrap().features.cycles, 0);
        }
        e.advance(30);
        let closing = e.add(pay(2, 30, 3, 1)).unwrap();
        assert_eq!(
            (closing.features.cycles, closing.features.shortest_cycle),
            (1, 3)
        );
        assert_eq!(
            (closing.features.src_cycles, closing.features.dst_cycles),
            (1, 1)
        );

        // a later payment by an account in the ring carries the finding
        e.advance(50);
        let later = e.add(pay(3, 50, 2, 9)).unwrap();
        assert_eq!((later.features.cycles, later.features.src_cycles), (0, 1));
        assert_eq!(later.features.dst_cycles, 0);

        // once the ring's first payment (t=10) leaves the window, it is gone
        e.advance(111);
        let after = e.add(pay(4, 111, 2, 9)).unwrap();
        assert_eq!(after.features.src_cycles, 0);
        assert_eq!(e.live_cycles(), 0);
    }

    #[test]
    fn a_ring_that_keeps_closing_is_counted() {
        let mut e = engine(1_000);
        let add = |e: &mut Engine, tx, ts, src, dst| {
            e.advance(ts);
            e.add(pay(tx, ts, src, dst)).unwrap().features
        };
        add(&mut e, 0, 10, 1, 2);
        add(&mut e, 1, 11, 2, 3);
        let first = add(&mut e, 2, 12, 3, 1);
        assert_eq!((first.cycles, first.ring_repeats), (1, 0));
        // 3 pays 1 again: the same three accounts close a ring once more
        let again = add(&mut e, 3, 20, 3, 1);
        assert_eq!((again.cycles, again.ring_repeats), (1, 1));
        assert_eq!(again.ring_one_format, 1);
        assert_eq!(again.ring_amount_ratio, 1000);
    }

    #[test]
    fn ring_shape_reads_formats_and_amounts() {
        let mut e = engine(1_000);
        let payments = [
            Payment {
                amount: 500.0,
                format: 1,
                ..pay(0, 10, 1, 2)
            },
            Payment {
                amount: 1000.0,
                format: 1,
                ..pay(1, 20, 2, 3)
            },
            Payment {
                amount: 250.0,
                format: 2,
                ..pay(2, 30, 3, 1)
            },
        ];
        let mut f = Features::default();
        for p in payments {
            e.advance(p.ts);
            f = e.add(p).unwrap().features;
        }
        assert_eq!((f.ring_one_format, f.ring_amount_ratio), (0, 250));
    }

    #[test]
    fn reports_the_widest_scatter_gather() {
        let mut e = engine(1_000);
        let mut obs = None;
        for (i, m) in [10, 11, 12, 13].into_iter().enumerate() {
            let i = i as u32;
            e.add(pay(2 * i, 10, 1, m));
            obs = e.add(pay(2 * i + 1, 20, m, 2));
        }
        let obs = obs.unwrap();
        assert_eq!(obs.features.scatter_gather, 4);
        assert_eq!(obs.scatter_gather.widest.unwrap().source, 1);
    }

    #[test]
    fn too_late_payments_are_not_observed() {
        let mut e = engine(100);
        e.advance(500);
        assert!(e.add(pay(0, 10, 1, 2)).is_none());
    }
}
