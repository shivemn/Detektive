//! Degree and fan features for one payment, read from the window right after
//! the payment went in (so they count the payment itself).
//!
//! For a payment from S to R:
//!
//! - degree: how many payments an account sent (out) or received (in) inside
//!   the window, repeats to the same counterparty included;
//! - fan: how many distinct accounts it paid (fan-out) or was paid by
//!   (fan-in);
//! - pair count: how many payments S made to R inside the window.
//!
//! Each is taken for both ends of the payment, in both directions. A mule
//! account shows up as a receiver with a high fan-in that also pays out
//! (high out-degree); a collector paid by many small senders, a scatterer
//! paying many fresh accounts, and a first payment to a new counterparty
//! (pair count 1) are the shapes the model will look for in these numbers.
//!
//! Degree and fan are the simplest graph features, and the ones the Graph
//! Feature Preprocessor paper (Blanuša et al., 2024) starts from.
//!
//! Then the cycle features (filled in by `Engine`, see `cycles.rs`):
//!
//! - cycles: how many distinct temporal cycles this payment closes;
//! - shortest cycle: payments in the shortest of them (0 if none);
//! - sender's and receiver's cycles: cycles found inside the window that the
//!   account is part of, this payment's included. A cycle is found when its
//!   last payment arrives, after its earlier payments were already scored;
//!   these two carry the finding forward to every later payment of the
//!   accounts in the ring.
//!
//! The shape of the shortest ring this payment closes, because a ring alone
//! is not enough: on HI-Small, businesses paying each other in a loop on a
//! schedule close the same ring again and again, with very different amounts
//! and mixed payment formats, while the laundering rings close once or
//! twice, in one format, moving similar sums:
//!
//! - ring repeats: how many times the same set of accounts already closed a
//!   ring inside the window;
//! - ring one format: 1 if every payment in the ring has the same format;
//! - ring amount ratio: smallest amount in the ring over the largest, in
//!   thousandths (1000 means all equal).
//!
//! Then scatter-gather and gather-scatter (`scatter_gather.rs`):
//!
//! - scatter-gather: the widest one the payment is part of, in accounts in
//!   the middle;
//! - sender's and receiver's gather-scatter: the smaller of the account's
//!   fan-in and fan-out, so high only when many accounts pay it *and* it pays
//!   many (money collected and spread again). All 0 to 3 for most accounts.

use crate::graph::{Payment, WindowGraph};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Features {
    pub src_out_degree: u32,
    pub src_fan_out: u32,
    pub src_in_degree: u32,
    pub src_fan_in: u32,
    pub dst_in_degree: u32,
    pub dst_fan_in: u32,
    pub dst_out_degree: u32,
    pub dst_fan_out: u32,
    pub pair_count: u32,
    pub cycles: u32,
    pub shortest_cycle: u32,
    pub src_cycles: u32,
    pub dst_cycles: u32,
    pub ring_repeats: u32,
    pub ring_one_format: u32,
    pub ring_amount_ratio: u32,
    pub scatter_gather: u32,
    pub src_gather_scatter: u32,
    pub dst_gather_scatter: u32,
}

impl Features {
    /// Column names, in the order of `values`.
    pub const NAMES: [&'static str; 19] = [
        "src_out_degree",
        "src_fan_out",
        "src_in_degree",
        "src_fan_in",
        "dst_in_degree",
        "dst_fan_in",
        "dst_out_degree",
        "dst_fan_out",
        "pair_count",
        "cycles",
        "shortest_cycle",
        "src_cycles",
        "dst_cycles",
        "ring_repeats",
        "ring_one_format",
        "ring_amount_ratio",
        "scatter_gather",
        "src_gather_scatter",
        "dst_gather_scatter",
    ];

    pub fn values(&self) -> [u32; 19] {
        [
            self.src_out_degree,
            self.src_fan_out,
            self.src_in_degree,
            self.src_fan_in,
            self.dst_in_degree,
            self.dst_fan_in,
            self.dst_out_degree,
            self.dst_fan_out,
            self.pair_count,
            self.cycles,
            self.shortest_cycle,
            self.src_cycles,
            self.dst_cycles,
            self.ring_repeats,
            self.ring_one_format,
            self.ring_amount_ratio,
            self.scatter_gather,
            self.src_gather_scatter,
            self.dst_gather_scatter,
        ]
    }
}

impl WindowGraph {
    /// The degree and fan features of `p` as the window stands now (cycle
    /// features left at 0). Call right after inserting `p`. Nine lookups,
    /// whatever the size of either account's neighbourhood.
    pub fn degree_features(&self, p: &Payment) -> Features {
        Features {
            src_out_degree: self.out_degree(p.src),
            src_fan_out: self.fan_out(p.src),
            src_in_degree: self.in_degree(p.src),
            src_fan_in: self.fan_in(p.src),
            dst_in_degree: self.in_degree(p.dst),
            dst_fan_in: self.fan_in(p.dst),
            dst_out_degree: self.out_degree(p.dst),
            dst_fan_out: self.fan_out(p.dst),
            pair_count: self.pair_count(p.src, p.dst),
            src_gather_scatter: self.fan_in(p.src).min(self.fan_out(p.src)),
            dst_gather_scatter: self.fan_in(p.dst).min(self.fan_out(p.dst)),
            ..Features::default()
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

    #[test]
    fn a_fan_in_then_a_payment_out() {
        // five accounts pay 9, one of them twice; then 9 pays 20
        let mut g = WindowGraph::new(3600);
        let mut tx = 0;
        for src in [1, 2, 3, 4, 5, 1] {
            g.insert(pay(tx, 100 + tx as i64, src, 9));
            tx += 1;
        }
        let out = pay(tx, 200, 9, 20);
        g.insert(out);
        let f = g.degree_features(&out);
        assert_eq!(
            f,
            Features {
                src_out_degree: 1,
                src_fan_out: 1,
                src_in_degree: 6,
                src_fan_in: 5,
                dst_in_degree: 1,
                dst_fan_in: 1,
                dst_out_degree: 0,
                dst_fan_out: 0,
                pair_count: 1,
                // 9 is paid by five and pays one
                src_gather_scatter: 1,
                ..Features::default()
            }
        );
    }

    #[test]
    fn values_line_up_with_names() {
        let f = Features {
            src_out_degree: 1,
            src_fan_out: 2,
            src_in_degree: 3,
            src_fan_in: 4,
            dst_in_degree: 5,
            dst_fan_in: 6,
            dst_out_degree: 7,
            dst_fan_out: 8,
            pair_count: 9,
            cycles: 10,
            shortest_cycle: 11,
            src_cycles: 12,
            dst_cycles: 13,
            ring_repeats: 14,
            ring_one_format: 15,
            ring_amount_ratio: 16,
            scatter_gather: 17,
            src_gather_scatter: 18,
            dst_gather_scatter: 19,
        };
        assert_eq!(f.values(), std::array::from_fn(|i| i as u32 + 1));
        assert_eq!(Features::NAMES.len(), f.values().len());
    }
}
