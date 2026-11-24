//! Temporal cycles closed by a payment.
//!
//! A cycle is money that comes back: A pays B, B pays C, C pays A. In a
//! *temporal* cycle the payments also happen in that order: each one no
//! earlier than the one before. Laundering cycles are temporal (the money has
//! to arrive before it can move on); a cycle whose payments run backwards in
//! time is just accounts that happen to know each other.
//!
//! The engine looks for cycles at the moment they close. When a payment from
//! U to V arrives, every new temporal cycle it completes ends with it, so the
//! search is for chains V -> ... -> U whose payments are in time order and
//! no later than this one, inside the window, and simple (no account twice).
//!
//! **Search backwards.** The search starts at U and follows payments *into*
//! each account, going back in time. Two things keep it small:
//!
//! - time only runs one way: from an account reached by a payment at time t,
//!   only payments into it at or before t can extend the chain, and every
//!   step tightens that bound;
//! - it runs against the grain of fan-out. On HI-Small an account pays up to
//!   10,912 others in a day but is paid by at most 92 (week 6), so going
//!   backwards over incoming payments branches far less than going forwards.
//!
//! **Count accounts, not payments.** Ten payments from B to C and ten from C
//! to B are a hundred cycles if every combination counts, and one ring of two
//! accounts in fact. A cycle here is a sequence of accounts that has at least
//! one run of payments in time order. To decide that, the search takes the
//! latest suitable payment between each pair: the later that payment, the more
//! earlier payments can come before it, so if any run exists, this one does.
//!
//! **A budget.** Enumerating cycles is exponential in the worst case. Each
//! search may look at a fixed number of payments (`budget`); if it runs out,
//! it stops and says so (`truncated`), so one pathological payment can never
//! stall the stream.

use rustc_hash::FxHashMap;

use crate::graph::{Payment, WindowGraph};

/// One payment in a cycle.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Hop {
    pub tx: u32,
    pub ts: i64,
    pub src: u32,
    pub dst: u32,
    pub amount: f64,
    pub format: u8,
}

/// A cycle found by a search: its accounts, starting with the receiver of the
/// closing payment, and the time of its oldest payment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Found {
    pub oldest: i64,
    pub accounts: Vec<u32>,
}

/// Cycles recorded per search, at most. Counting goes on past this.
pub const MAX_RECORDED: usize = 64;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Cycles {
    /// Distinct cycles (as sequences of accounts) the payment closes.
    pub count: u32,
    /// Payments in the shortest of them, the closing one included; 0 if none.
    pub shortest: u32,
    /// The search ran out of budget, so there may be more.
    pub truncated: bool,
    /// The shortest cycle, oldest payment first, ending with the closing one.
    pub example: Vec<Hop>,
    /// The first `MAX_RECORDED` cycles found.
    pub found: Vec<Found>,
}

#[derive(Debug, Clone, Copy)]
struct Step {
    peer: u32,
    ts: i64,
    tx: u32,
    amount: f64,
    format: u8,
}

/// Searches for the cycles a payment closes. Keeps its scratch space between
/// searches, so a search allocates nothing unless it finds a cycle.
#[derive(Debug)]
pub struct CycleFinder {
    max_len: usize,
    budget: u32,
    /// Candidate predecessors, one list per depth.
    levels: Vec<Vec<Step>>,
    /// Scratch while building one level: predecessor -> index in its list.
    seen: FxHashMap<u32, usize>,
    /// The chain so far, from the closing payment backwards.
    path: Vec<Hop>,
    spent: u32,
    out: Cycles,
    /// The view of the graph being searched: payments no older than
    /// `cutoff`, inserted no later than `seq`.
    cutoff: i64,
    seq: u32,
}

impl CycleFinder {
    /// Cycles of 2 to `max_len` payments; at most `budget` payments looked at
    /// per search.
    pub fn new(max_len: usize, budget: u32) -> Self {
        Self {
            max_len,
            budget,
            levels: vec![Vec::new(); max_len],
            seen: FxHashMap::default(),
            path: Vec::with_capacity(max_len),
            spent: 0,
            out: Cycles::default(),
            cutoff: i64::MIN,
            seq: u32::MAX,
        }
    }

    /// The cycles `p` closes in `g`. Call right after inserting `p`.
    pub fn closing(&mut self, g: &WindowGraph, p: &Payment) -> Cycles {
        self.closing_as_of(g, p, g.cutoff().unwrap_or(i64::MIN), u32::MAX)
    }

    /// The cycles `p` closed when it went in: looking only at payments no
    /// older than `cutoff` (the window's cutoff then) and inserted no later
    /// than `seq` (its own insertion order). This is what lets searches for
    /// a batch of payments run in parallel after the whole batch is in; see
    /// `Engine::add_batch`.
    pub fn closing_as_of(&mut self, g: &WindowGraph, p: &Payment, cutoff: i64, seq: u32) -> Cycles {
        self.cutoff = cutoff;
        self.seq = seq;
        self.out = Cycles::default();
        self.spent = 0;
        self.path.clear();
        if p.src == p.dst || self.max_len < 2 {
            return std::mem::take(&mut self.out);
        }
        self.path.push(Hop {
            tx: p.tx,
            ts: p.ts,
            src: p.src,
            dst: p.dst,
            amount: p.amount,
            format: p.format,
        });
        self.search(g, p.src, p.ts, p.dst, 0);
        std::mem::take(&mut self.out)
    }

    /// Extend the chain backwards from `at`, using payments into it no later
    /// than `bound`, looking for `target`. Returns false once out of budget.
    fn search(&mut self, g: &WindowGraph, at: u32, bound: i64, target: u32, level: usize) -> bool {
        // the latest suitable payment from each account that paid `at`
        let mut steps = std::mem::take(&mut self.levels[level]);
        steps.clear();
        self.seen.clear();
        for h in g.in_edges_as_of(at, self.cutoff, self.seq) {
            self.spent += 1;
            if self.spent > self.budget {
                self.out.truncated = true;
                self.levels[level] = steps;
                return false;
            }
            if h.ts > bound {
                continue;
            }
            let step = Step {
                peer: h.peer,
                ts: h.ts,
                tx: h.tx,
                amount: h.amount,
                format: h.format,
            };
            match self.seen.get(&h.peer) {
                Some(&i) if steps[i].ts < h.ts => steps[i] = step,
                Some(_) => {}
                None => {
                    self.seen.insert(h.peer, steps.len());
                    steps.push(step);
                }
            }
        }

        // payments in the chain so far, the closing one included
        let len = self.path.len();
        let mut more = true;
        for s in &steps {
            let hop = Hop {
                tx: s.tx,
                ts: s.ts,
                src: s.peer,
                dst: at,
                amount: s.amount,
                format: s.format,
            };
            if s.peer == target {
                self.record(hop);
            } else if len + 1 < self.max_len && !self.path.iter().any(|h| h.src == s.peer) {
                self.path.push(hop);
                more = self.search(g, s.peer, s.ts, target, level + 1);
                self.path.pop();
                if !more {
                    break;
                }
            }
        }
        self.levels[level] = steps;
        more
    }

    /// `first` is the payment out of the target that completes the chain.
    fn record(&mut self, first: Hop) {
        let len = self.path.len() + 1;
        self.out.count += 1;
        let in_order = || std::iter::once(first).chain(self.path.iter().rev().copied());
        if self.out.shortest == 0 || (len as u32) < self.out.shortest {
            self.out.shortest = len as u32;
            self.out.example = in_order().collect();
        }
        if self.out.found.len() < MAX_RECORDED {
            self.out.found.push(Found {
                oldest: first.ts,
                accounts: in_order().map(|h| h.src).collect(),
            });
        }
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

    /// Insert every payment, then search for the cycles the last one closes.
    fn closing(payments: &[Payment], max_len: usize) -> Cycles {
        let mut g = WindowGraph::new(1_000);
        for p in payments {
            g.insert(*p);
        }
        CycleFinder::new(max_len, 1_000_000).closing(&g, payments.last().unwrap())
    }

    #[test]
    fn finds_a_ring_of_three_in_time_order() {
        let c = closing(&[pay(0, 10, 1, 2), pay(1, 20, 2, 3), pay(2, 30, 3, 1)], 12);
        assert_eq!((c.count, c.shortest, c.truncated), (1, 3, false));
        let txs: Vec<u32> = c.example.iter().map(|h| h.tx).collect();
        assert_eq!(txs, vec![0, 1, 2]);
        assert_eq!(c.found[0].accounts, vec![1, 2, 3]);
        assert_eq!(c.found[0].oldest, 10);
    }

    #[test]
    fn a_ring_running_backwards_in_time_is_not_a_cycle() {
        // 2->3 happened before 1->2, so money from 1 could not have gone on
        let c = closing(&[pay(0, 20, 1, 2), pay(1, 10, 2, 3), pay(2, 30, 3, 1)], 12);
        assert_eq!(c.count, 0);
    }

    #[test]
    fn payments_at_the_same_minute_still_count() {
        let c = closing(&[pay(0, 10, 1, 2), pay(1, 10, 2, 1)], 12);
        assert_eq!((c.count, c.shortest), (1, 2));
    }

    #[test]
    fn repeated_payments_between_the_same_accounts_are_one_cycle() {
        let mut ps: Vec<Payment> = (0..10).map(|i| pay(i, 10 + i as i64, 1, 2)).collect();
        ps.push(pay(10, 50, 2, 1));
        let c = closing(&ps, 12);
        assert_eq!(c.count, 1);
        // the latest payment from 1 to 2 is the one shown
        assert_eq!(c.example[0].tx, 9);
    }

    #[test]
    fn counts_every_ring_the_payment_closes() {
        // 1 -> 2 -> 4 and 1 -> 3 -> 4, then 4 pays 1: two rings of three,
        // and 1 -> 4 directly makes a ring of two
        let c = closing(
            &[
                pay(0, 10, 1, 2),
                pay(1, 11, 1, 3),
                pay(2, 12, 1, 4),
                pay(3, 20, 2, 4),
                pay(4, 21, 3, 4),
                pay(5, 30, 4, 1),
            ],
            12,
        );
        assert_eq!((c.count, c.shortest), (3, 2));
        assert_eq!(c.example.len(), 2);
    }

    #[test]
    fn respects_the_length_limit() {
        // a ring of five
        let ring: Vec<Payment> = (0..5)
            .map(|i| pay(i, 10 + i as i64, i, (i + 1) % 5))
            .collect();
        assert_eq!(closing(&ring, 5).count, 1);
        assert_eq!(closing(&ring, 4).count, 0);
    }

    #[test]
    fn accounts_may_not_repeat_inside_a_cycle() {
        // 1 -> 2 -> 3 -> 2 -> 1 passes 2 twice: only the ring 1, 2 counts
        let c = closing(
            &[
                pay(0, 10, 1, 2),
                pay(1, 11, 2, 3),
                pay(2, 12, 3, 2),
                pay(3, 13, 2, 1),
            ],
            12,
        );
        assert_eq!((c.count, c.shortest), (1, 2));
    }

    #[test]
    fn a_payment_to_oneself_inside_the_chain_is_not_followed() {
        // 3 paid itself; the ring 1 -> 3 -> 2 -> 1 must still be found once
        let c = closing(
            &[
                pay(0, 10, 1, 3),
                pay(1, 11, 3, 3),
                pay(2, 12, 3, 2),
                pay(3, 13, 2, 1),
            ],
            12,
        );
        assert_eq!((c.count, c.shortest), (1, 3));
    }

    #[test]
    fn payments_to_oneself_close_nothing() {
        assert_eq!(closing(&[pay(0, 10, 1, 1)], 12).count, 0);
    }

    #[test]
    fn stops_at_the_budget_and_says_so() {
        // 1 is paid by 300 accounts that were all paid by 0; 1 pays 0
        let mut ps = Vec::new();
        for i in 0..300u32 {
            ps.push(pay(2 * i, 10, 0, 100 + i));
            ps.push(pay(2 * i + 1, 20, 100 + i, 1));
        }
        ps.push(pay(1000, 30, 1, 0));
        let mut g = WindowGraph::new(1_000);
        for p in &ps {
            g.insert(*p);
        }
        let full = CycleFinder::new(12, 1_000_000).closing(&g, ps.last().unwrap());
        assert_eq!((full.count, full.truncated), (300, false));
        let cut = CycleFinder::new(12, 100).closing(&g, ps.last().unwrap());
        assert!(cut.truncated);
        assert!(cut.count < 300);
    }
}
