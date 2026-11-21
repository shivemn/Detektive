//! What goes onto the stream for each transaction.
//!
//! The laundering label is left out by default. A live payment stream has no
//! labels, and if the engine ever saw them it could "detect" laundering by
//! reading the answer. Evaluation joins labels back on by `tx_id` instead.

use serde::Serialize;

use crate::india;
use crate::model::Transaction;

#[derive(Debug, Serialize)]
pub struct Event {
    pub tx_id: u32,
    /// Seconds since the Unix epoch.
    pub ts: i64,
    pub src: u32,
    pub dst: u32,
    pub amount: f64,
    pub currency: &'static str,
    pub received_amount: f64,
    pub received_currency: &'static str,
    pub format: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_laundering: Option<bool>,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub india: Option<IndiaView>,
}

#[derive(Debug, Serialize)]
pub struct IndiaView {
    pub src_vpa: String,
    pub dst_vpa: String,
    pub amount_inr: f64,
    pub time_ist: String,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct EventOptions {
    pub with_labels: bool,
    pub india: bool,
}

impl Event {
    pub fn from_transaction(t: &Transaction, opts: EventOptions) -> Self {
        Self {
            tx_id: t.tx_id,
            ts: t.ts,
            src: t.src,
            dst: t.dst,
            amount: t.amount_paid,
            currency: t.paid_currency.code(),
            received_amount: t.amount_received,
            received_currency: t.received_currency.code(),
            format: t.format.as_str(),
            is_laundering: opts.with_labels.then_some(t.is_laundering),
            india: opts.india.then(|| IndiaView {
                src_vpa: india::vpa(t.src),
                dst_vpa: india::vpa(t.dst),
                amount_inr: india::to_inr(t.amount_paid, t.paid_currency),
                time_ist: india::ist_label(t.ts),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Currency, PaymentFormat};

    fn tx() -> Transaction {
        Transaction {
            tx_id: 7,
            ts: 1_661_991_600,
            src: 1,
            dst: 2,
            amount_paid: 10.0,
            paid_currency: Currency::UsDollar,
            amount_received: 9.5,
            received_currency: Currency::Euro,
            format: PaymentFormat::Ach,
            is_laundering: true,
        }
    }

    #[test]
    fn labels_are_hidden_unless_asked_for() {
        let json = serde_json::to_string(&Event::from_transaction(&tx(), EventOptions::default()))
            .unwrap();
        assert!(!json.contains("is_laundering"), "{json}");
        assert!(!json.contains("vpa"), "{json}");
        assert!(json.contains(r#""format":"ach""#), "{json}");

        let opts = EventOptions {
            with_labels: true,
            india: false,
        };
        let json = serde_json::to_string(&Event::from_transaction(&tx(), opts)).unwrap();
        assert!(json.contains(r#""is_laundering":true"#), "{json}");
    }

    #[test]
    fn india_view_is_flattened_into_the_event() {
        let opts = EventOptions {
            with_labels: false,
            india: true,
        };
        let value = serde_json::to_value(Event::from_transaction(&tx(), opts)).unwrap();
        assert_eq!(value["src_vpa"], "acct0000001@dkbank");
        assert_eq!(value["amount_inr"], 800.0);
        assert_eq!(value["time_ist"], "2022-09-01T00:20:00+05:30");
        assert_eq!(value["currency"], "USD");
    }
}
