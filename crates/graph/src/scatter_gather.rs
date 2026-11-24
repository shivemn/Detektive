//! Scatter-gather: one account pays several others, which each pay the same
//! final account.
//!
//! ```text
//!          +--> m1 --+
//!   s -----+--> m2 --+----> t
//!          +--> m3 --+
//! ```
//!
//! The money leaves `s` in pieces and arrives at `t` in pieces, so no single
//! payment between them is large and no account in the middle looks unusual.
//! What gives it away is the shape: how many accounts sit between the same
//! source and target. That number is the size of a neighbour-set
//! intersection: the accounts `s` paid, intersected with the accounts that
//! paid `t`, inside the window. Call it the width.
//!
//! **When a payment arrives** it can be either half of a path through the
//! middle:
//!
//! - a payment `m -> t` (gathering): every account `s` that paid `m` is a
//!   possible source, so try each;
//! - a payment `s -> m` (scattering): every account `t` that `m` paid is a
//!   possible target, so try each.
//!
//! The feature is the widest scatter-gather the payment is part of. Time
//! order is not required: in 6 of HI-Small's 44 scatter-gather attempts,
//! money reaches `t` from an account before `s` has paid that account.
//!
//! **Cost.** Each intersection walks the smaller of the two sets and looks the
//! other up in a hash map, so it costs the smaller degree, not the larger.
//! The number of candidates is the fan-in of `m` (gathering) or the fan-out of
//! `m` (scattering); a payment into a hub that pays thousands of accounts
//! could try thousands of targets. A budget caps the work per payment, as for
//! cycles, and the result says when it was reached.

use crate::graph::{Payment, WindowGraph};

/// A scatter-gather: `source` paid `width` accounts that each paid `target`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fan {
    pub source: u32,
    pub target: u32,
    pub width: u32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScatterGather {
    /// The widest one this payment is part of; `None` if the payment lies on
    /// no path of two payments between distinct accounts.
    pub widest: Option<Fan>,
    /// The search stopped at its budget, so there may be a wider one.
    pub truncated: bool,
}

impl ScatterGather {
    pub fn width(&self) -> u32 {
        self.widest.map_or(0, |f| f.width)
    }
}

/// The widest scatter-gather `p` is part of in `g`, looking at no more than
/// `budget` accounts. Call right after inserting `p`.
pub fn scatter_gather(g: &WindowGraph, p: &Payment, budget: u32) -> ScatterGather {
    let mut out = ScatterGather::default();
    if p.src == p.dst {
        return out;
    }
    let mut spent = 0u32;
    let mut consider = |source: u32, target: u32, out: &mut ScatterGather| -> bool {
        if source == target {
            return true;
        }
        let Some(width) = width(g, source, target, budget, &mut spent) else {
            out.truncated = true;
            return false;
        };
        // ties go to the smallest (source, target), so the answer does not
        // depend on the order a hash map happens to list accounts in
        let better = match out.widest {
            None => width > 0,
            Some(w) => {
                (width, std::cmp::Reverse((source, target)))
                    > (w.width, std::cmp::Reverse((w.source, w.target)))
            }
        };
        if better {
            out.widest = Some(Fan {
                source,
                target,
                width,
            });
        }
        true
    };

    // p is m -> t: try every source that paid m
    let (m, t) = (p.src, p.dst);
    for s in g.in_peers(m) {
        if s != m && !consider(s, t, &mut out) {
            return out;
        }
    }
    // p is s -> m: try every target m paid
    let (s, m) = (p.src, p.dst);
    for t in g.out_peers(m) {
        if t != m && !consider(s, t, &mut out) {
            return out;
        }
    }
    out
}

/// Accounts other than `source` and `target` that `source` paid and that paid
/// `target`, inside the window. `None` if counting would pass the budget.
pub fn width(
    g: &WindowGraph,
    source: u32,
    target: u32,
    budget: u32,
    spent: &mut u32,
) -> Option<u32> {
    let paid = g.fan_out(source);
    let paid_by = g.fan_in(target);
    *spent = spent.saturating_add(paid.min(paid_by) + 1);
    if *spent > budget {
        return None;
    }
    let middle = |x: &u32| *x != source && *x != target;
    let n = if paid <= paid_by {
        g.out_peers(source)
            .filter(middle)
            .filter(|&x| g.pair_count(x, target) > 0)
            .count()
    } else {
        g.in_peers(target)
            .filter(middle)
            .filter(|&x| g.pair_count(source, x) > 0)
            .count()
    };
    Some(n as u32)
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
            ..Payment::default()
        }
    }

    /// Insert everything, then ask about the last payment.
    fn last(payments: &[Payment], budget: u32) -> ScatterGather {
        let mut g = WindowGraph::new(1_000);
        for p in payments {
            g.insert(*p);
        }
        scatter_gather(&g, payments.last().unwrap(), budget)
    }

    /// 1 pays 10, 11, 12; each of them pays 2.
    fn three_wide() -> Vec<Payment> {
        let mut ps = Vec::new();
        for (i, m) in [10, 11, 12].into_iter().enumerate() {
            ps.push(pay(2 * i as u32, 10, 1, m));
            ps.push(pay(2 * i as u32 + 1, 20, m, 2));
        }
        ps
    }

    #[test]
    fn the_last_gathering_payment_sees_the_whole_shape() {
        let sg = last(&three_wide(), 1_000);
        assert_eq!(
            sg.widest,
            Some(Fan {
                source: 1,
                target: 2,
                width: 3
            })
        );
        assert!(!sg.truncated);
    }

    #[test]
    fn so_does_a_scattering_payment_that_comes_last() {
        // the paths into 2 are already there when 1 pays 12
        let mut ps = three_wide();
        let scatter = ps.remove(4);
        ps.push(Payment { ts: 30, ..scatter });
        assert_eq!(last(&ps, 1_000).width(), 3);
    }

    #[test]
    fn a_lone_path_has_width_one() {
        assert_eq!(
            last(&[pay(0, 10, 1, 5), pay(1, 20, 5, 2)], 1_000).width(),
            1
        );
    }

    #[test]
    fn accounts_at_either_end_are_not_counted_as_middles() {
        // 1 also pays 2 directly, and 2 pays itself: neither is a middle
        let mut ps = three_wide();
        ps.insert(0, pay(100, 5, 1, 2));
        ps.insert(0, pay(101, 5, 2, 2));
        assert_eq!(last(&ps, 1_000).width(), 3);
    }

    #[test]
    fn a_payment_off_any_two_step_path_scores_nothing() {
        assert_eq!(last(&[pay(0, 10, 1, 2)], 1_000), ScatterGather::default());
        assert_eq!(last(&[pay(0, 10, 3, 3)], 1_000).width(), 0);
    }

    #[test]
    fn stops_at_the_budget_and_says_so() {
        // 1 pays 500 accounts that each pay 2; the last payment completes it
        let mut ps = Vec::new();
        for m in 0..500 {
            ps.push(pay(2 * m, 10, 1, 100 + m));
            ps.push(pay(2 * m + 1, 20, 100 + m, 2));
        }
        let full = last(&ps, 1_000_000);
        assert_eq!((full.width(), full.truncated), (500, false));
        let cut = last(&ps, 50);
        assert!(cut.truncated);
    }
}
