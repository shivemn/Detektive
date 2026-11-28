//! Turning a feature and its value into words an analyst can read.
//!
//! An explanation says which features moved a score; this module says what
//! each of those features was, for this payment, in a sentence fragment.
//! "the receiver was paid by 14 different accounts in the last 4 days" reads
//! better than `dst_fan_in = 14`. The wording describes the value only. Which
//! way it pushed the score, and how far, is given alongside it.

use detektive_graph::Features;

use crate::row::{Row, hour_and_weekday};

const WEEKDAYS: [&str; 7] = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
];

fn plural(n: u32, one: &str, many: &str) -> String {
    if n == 1 {
        format!("1 {one}")
    } else {
        format!("{n} {many}")
    }
}

fn currency(c: Option<detektive_replay::model::Currency>) -> &'static str {
    c.map_or("an unknown currency", |c| c.name())
}

/// What feature `index` (row features first, then graph features) was for
/// this payment. `window` is how the window is described, e.g. "4 days".
pub fn describe(index: usize, row: &Row, graph: &Features, window: &str) -> String {
    let g = graph;
    match index {
        0 => format!(
            "{:.2} paid in {}",
            row.amount_paid,
            currency(row.paid_currency)
        ),
        1 => format!(
            "{:.2} received in {}",
            row.amount_received,
            currency(row.received_currency)
        ),
        2 => {
            if row.amount_received == row.amount_paid {
                "the same amount paid and received".to_owned()
            } else {
                format!(
                    "{:.4} received per unit paid",
                    row.amount_received / row.amount_paid
                )
            }
        }
        3 => {
            if row.paid_currency == row.received_currency {
                format!("paid and received in {}", currency(row.paid_currency))
            } else {
                format!(
                    "paid in {}, received in {}",
                    currency(row.paid_currency),
                    currency(row.received_currency)
                )
            }
        }
        4 => match (row.src_bank, row.dst_bank) {
            (Some(a), Some(b)) if a == b => "sender and receiver use the same bank".to_owned(),
            (Some(_), Some(_)) => "sender and receiver use different banks".to_owned(),
            _ => "the banks were not given".to_owned(),
        },
        5 => {
            if row.src == row.dst {
                "paid to the sender's own account".to_owned()
            } else {
                "paid to another account".to_owned()
            }
        }
        6 => {
            let (h, _) = hour_and_weekday(row.ts);
            format!("made between {h:02}:00 and {h:02}:59")
        }
        7 => {
            let (_, d) = hour_and_weekday(row.ts);
            format!("made on a {}", WEEKDAYS[(d - 1) as usize])
        }
        8 => format!(
            "paid by {}",
            row.format.map_or("an unknown method", |f| f.name())
        ),
        9 => format!("paid in {}", currency(row.paid_currency)),
        10 => format!("received in {}", currency(row.received_currency)),
        11 => format!(
            "the sender made {} in the last {window}",
            plural(g.src_out_degree, "payment", "payments")
        ),
        12 => format!(
            "the sender paid {} in the last {window}",
            plural(g.src_fan_out, "account", "different accounts")
        ),
        13 => format!(
            "the sender received {} in the last {window}",
            plural(g.src_in_degree, "payment", "payments")
        ),
        14 => format!(
            "the sender was paid by {} in the last {window}",
            plural(g.src_fan_in, "account", "different accounts")
        ),
        15 => format!(
            "the receiver received {} in the last {window}",
            plural(g.dst_in_degree, "payment", "payments")
        ),
        16 => format!(
            "the receiver was paid by {} in the last {window}",
            plural(g.dst_fan_in, "account", "different accounts")
        ),
        17 => format!(
            "the receiver made {} in the last {window}",
            plural(g.dst_out_degree, "payment", "payments")
        ),
        18 => format!(
            "the receiver paid {} in the last {window}",
            plural(g.dst_fan_out, "account", "different accounts")
        ),
        19 => match g.pair_count {
            1 => format!("the sender's first payment to this receiver in the last {window}"),
            n => format!("payment {n} from this sender to this receiver in the last {window}"),
        },
        20 => match g.cycles {
            0 => "closes no ring of payments".to_owned(),
            n => format!(
                "closes {} that returns money to the sender",
                plural(n, "ring", "rings")
            ),
        },
        21 => match g.shortest_cycle {
            0 => "closes no ring of payments".to_owned(),
            n => format!("the shortest ring it closes has {n} payments"),
        },
        22 => format!(
            "the sender is in {} found in the last {window}",
            plural(g.src_cycles, "ring", "rings")
        ),
        23 => format!(
            "the receiver is in {} found in the last {window}",
            plural(g.dst_cycles, "ring", "rings")
        ),
        24 => match (g.cycles, g.ring_repeats) {
            (0, _) => "closes no ring of payments".to_owned(),
            (_, 0) => format!("the first time these accounts closed a ring in the last {window}"),
            (_, n) => format!(
                "these accounts already closed this ring {} in the last {window}",
                plural(n, "time", "times")
            ),
        },
        25 => match (g.cycles, g.ring_one_format) {
            (0, _) => "closes no ring of payments".to_owned(),
            (_, 1) => "every payment in the ring used the same method".to_owned(),
            _ => "the ring's payments used different methods".to_owned(),
        },
        26 => match g.cycles {
            0 => "closes no ring of payments".to_owned(),
            _ => format!(
                "the ring's smallest payment is {:.0}% of its largest",
                f64::from(g.ring_amount_ratio) / 10.0
            ),
        },
        27 => match g.scatter_gather {
            0 => "part of no scatter-gather".to_owned(),
            n => format!(
                "the sender's money reached the receiver through {}",
                plural(n, "middle account", "different middle accounts")
            ),
        },
        28 => format!(
            "the sender both collects and spreads money ({} in and out)",
            plural(g.src_gather_scatter, "account", "accounts")
        ),
        29 => format!(
            "the receiver both collects and spreads money ({} in and out)",
            plural(g.dst_gather_scatter, "account", "accounts")
        ),
        _ => format!("feature {index}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::WIDTH;
    use detektive_replay::model::{Currency, PaymentFormat};

    fn row() -> Row {
        Row {
            ts: 1_662_768_000 + 3 * 3600, // Saturday 2022-09-10, 03:00
            src: 1,
            dst: 2,
            src_bank: Some(7),
            dst_bank: Some(8),
            amount_paid: 351.0,
            paid_currency: Some(Currency::UsDollar),
            amount_received: 351.0,
            received_currency: Some(Currency::UsDollar),
            format: Some(PaymentFormat::Ach),
        }
    }

    #[test]
    fn every_feature_has_words() {
        let g = Features {
            pair_count: 1,
            cycles: 1,
            shortest_cycle: 3,
            dst_fan_in: 14,
            ..Features::default()
        };
        for i in 0..WIDTH {
            let text = describe(i, &row(), &g, "4 days");
            assert!(!text.starts_with("feature "), "no words for feature {i}");
        }
    }

    #[test]
    fn reads_naturally() {
        let g = Features {
            pair_count: 1,
            cycles: 1,
            shortest_cycle: 3,
            dst_fan_in: 14,
            ring_amount_ratio: 873,
            ..Features::default()
        };
        let r = row();
        assert_eq!(describe(8, &r, &g, "4 days"), "paid by ACH");
        assert_eq!(describe(7, &r, &g, "4 days"), "made on a Saturday");
        assert_eq!(
            describe(6, &r, &g, "4 days"),
            "made between 03:00 and 03:59"
        );
        assert_eq!(
            describe(16, &r, &g, "4 days"),
            "the receiver was paid by 14 different accounts in the last 4 days"
        );
        assert_eq!(
            describe(19, &r, &g, "4 days"),
            "the sender's first payment to this receiver in the last 4 days"
        );
        assert_eq!(
            describe(20, &r, &g, "4 days"),
            "closes 1 ring that returns money to the sender"
        );
        assert_eq!(
            describe(26, &r, &g, "4 days"),
            "the ring's smallest payment is 87% of its largest"
        );
        assert_eq!(
            describe(4, &r, &g, "4 days"),
            "sender and receiver use different banks"
        );
    }
}
