//! The window graph, the cycle search and the cycle counts per account,
//! moved together: one call to add a payment and get its features.

use std::collections::BTreeMap;

use crate::cycles::{CycleFinder, Cycles};
use crate::features::Features;
use crate::graph::{Insert, Payment, WindowGraph};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EngineConfig {
    /// Seconds of event time the graph keeps. Cycles must fit inside it.
    pub window: i64,
    /// Longest cycle searched for, in payments.
    pub max_cycle_len: usize,
    /// Payments one cycle search may look at before it stops.
    pub cycle_budget: u32,
}

impl Default for EngineConfig {
    /// Four days and twelve payments: every cycle in HI-Small's laundering
    /// patterns has 2 to 12 payments and finishes within 96 hours.
    fn default() -> Self {
        Self {
            window: 4 * 24 * 3600,
            max_cycle_len: 12,
            cycle_budget: 100_000,
        }
    }
}

/// What the engine says about one payment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observation {
    pub features: Features,
    pub cycles: Cycles,
}

#[derive(Debug)]
pub struct Engine {
    graph: WindowGraph,
    finder: CycleFinder,
    members: CycleMembers,
}

impl Engine {
    pub fn new(cfg: EngineConfig) -> Self {
        Self {
            graph: WindowGraph::new(cfg.window),
            finder: CycleFinder::new(cfg.max_cycle_len, cfg.cycle_budget),
            members: CycleMembers::default(),
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
        if self.graph.insert(p) == Insert::TooLate {
            return None;
        }
        let cycles = self.finder.closing(&self.graph, &p);
        for c in &cycles.found {
            self.members.add(c.oldest, &c.accounts);
        }
        let mut features = self.graph.degree_features(&p);
        features.cycles = cycles.count;
        features.shortest_cycle = cycles.shortest;
        features.src_cycles = self.members.of(p.src);
        features.dst_cycles = self.members.of(p.dst);
        Some(Observation { features, cycles })
    }

    /// Cycles currently counted against accounts (found inside the window).
    pub fn live_cycles(&self) -> usize {
        self.members.live
    }
}

/// How many cycles found inside the window each account is part of. A cycle
/// stops counting when its oldest payment leaves the window.
#[derive(Debug, Default)]
struct CycleMembers {
    counts: Vec<u32>,
    expiry: BTreeMap<i64, Vec<Vec<u32>>>,
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
        self.expiry
            .entry(oldest)
            .or_default()
            .push(accounts.to_vec());
        self.live += 1;
    }

    fn expire(&mut self, cutoff: i64) {
        while let Some(entry) = self.expiry.first_entry() {
            if *entry.key() >= cutoff {
                break;
            }
            for accounts in entry.remove() {
                for a in accounts {
                    self.counts[a as usize] -= 1;
                }
                self.live -= 1;
            }
        }
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
    fn too_late_payments_are_not_observed() {
        let mut e = engine(100);
        e.advance(500);
        assert!(e.add(pay(0, 10, 1, 2)).is_none());
    }
}
