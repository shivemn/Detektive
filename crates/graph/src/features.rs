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
//! Feature Preprocessor paper (Blanuša et al., 2024) starts from. Cycles and
//! scatter-gather come in weeks 7 and 8.

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
}

impl Features {
    /// Column names, in the order of `values`.
    pub const NAMES: [&'static str; 9] = [
        "src_out_degree",
        "src_fan_out",
        "src_in_degree",
        "src_fan_in",
        "dst_in_degree",
        "dst_fan_in",
        "dst_out_degree",
        "dst_fan_out",
        "pair_count",
    ];

    pub fn values(&self) -> [u32; 9] {
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
        ]
    }
}

impl WindowGraph {
    /// Features of `p` as the window stands now. Call right after inserting
    /// `p`. Nine lookups, whatever the size of either account's
    /// neighbourhood.
    pub fn features(&self, p: &Payment) -> Features {
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
        let f = g.features(&out);
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
        };
        assert_eq!(f.values(), [1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert_eq!(Features::NAMES.len(), f.values().len());
    }
}
