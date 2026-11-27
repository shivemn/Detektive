//! The features a payment carries by itself, computed exactly as the Python
//! side computes them (`baselines.raw_xgb.row_features`). The model was
//! trained on those values, so any difference here would quietly change its
//! scores. `tests/parity.rs` checks the two agree on real rows.

use detektive_replay::model::{Currency, PaymentFormat, Transaction};

/// Names, in the order the model reads them.
pub const NAMES: [&str; 11] = [
    "log_amount_paid",
    "log_amount_received",
    "amount_ratio",
    "cross_currency",
    "same_bank",
    "self_transfer",
    "hour",
    "weekday",
    "payment_format",
    "payment_currency",
    "receiving_currency",
];

/// What the row features need to know about a payment. Currencies and the
/// format are `None` when the stream sent a value this build does not know;
/// banks are `None` when the event did not say.
#[derive(Debug, Clone, Copy)]
pub struct Row {
    pub ts: i64,
    pub src: u32,
    pub dst: u32,
    pub src_bank: Option<u32>,
    pub dst_bank: Option<u32>,
    pub amount_paid: f64,
    pub paid_currency: Option<Currency>,
    pub amount_received: f64,
    pub received_currency: Option<Currency>,
    pub format: Option<PaymentFormat>,
}

impl From<&Transaction> for Row {
    fn from(t: &Transaction) -> Self {
        Self {
            ts: t.ts,
            src: t.src,
            dst: t.dst,
            src_bank: Some(t.src_bank),
            dst_bank: Some(t.dst_bank),
            amount_paid: t.amount_paid,
            paid_currency: Some(t.paid_currency),
            amount_received: t.amount_received,
            received_currency: Some(t.received_currency),
            format: Some(t.format),
        }
    }
}

/// Integer codes for the categorical columns, as learned on training rows.
/// A value training never saw is -1, as in Python.
#[derive(Debug, Clone)]
pub struct Codes {
    pub format: [f32; PaymentFormat::ALL.len()],
    pub paid_currency: [f32; Currency::ALL.len()],
    pub received_currency: [f32; Currency::ALL.len()],
}

impl Codes {
    fn format(&self, f: Option<PaymentFormat>) -> f32 {
        f.and_then(|f| PaymentFormat::ALL.iter().position(|&x| x == f))
            .map_or(-1.0, |i| self.format[i])
    }

    fn currency(table: &[f32], c: Option<Currency>) -> f32 {
        c.and_then(|c| Currency::ALL.iter().position(|&x| x == c))
            .map_or(-1.0, |i| table[i])
    }
}

/// Fill `out` (at least 11 long) with the row features.
pub fn features(row: &Row, codes: &Codes, out: &mut [f32]) {
    // Python computes in f64 and casts the column to f32 at the end
    let ratio = row.amount_received / row.amount_paid;
    let flag = |b: bool| if b { 1.0 } else { 0.0 };
    out[0] = row.amount_paid.ln_1p() as f32;
    out[1] = row.amount_received.ln_1p() as f32;
    // 0/0 is NaN, which Python fills with 1; x/0 stays infinite
    out[2] = if ratio.is_nan() { 1.0 } else { ratio as f32 };
    // Unknown currencies or banks give 0, never NaN. The model never saw a
    // missing value in training, and for such a feature LightGBM reads NaN
    // as 0; the ONNX file would instead send it down each split's default
    // branch. Passing 0 keeps the two in step.
    out[3] = match (row.paid_currency, row.received_currency) {
        (Some(a), Some(b)) => flag(a != b),
        _ => 0.0,
    };
    out[4] = match (row.src_bank, row.dst_bank) {
        (Some(a), Some(b)) => flag(a == b),
        _ => 0.0,
    };
    out[5] = flag(row.src == row.dst);
    let (hour, weekday) = hour_and_weekday(row.ts);
    out[6] = hour as f32;
    out[7] = weekday as f32;
    out[8] = codes.format(row.format);
    out[9] = Codes::currency(&codes.paid_currency, row.paid_currency);
    out[10] = Codes::currency(&codes.received_currency, row.received_currency);
}

/// Hour of day (0–23) and ISO weekday (Monday 1 … Sunday 7) of a timestamp in
/// seconds, read as UTC like the timestamps in the files.
pub fn hour_and_weekday(ts: i64) -> (u32, u32) {
    let days = ts.div_euclid(86_400);
    let hour = ts.rem_euclid(86_400) / 3600;
    // 1970-01-01 was a Thursday (4)
    let weekday = (days + 3).rem_euclid(7) + 1;
    (hour as u32, weekday as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn codes() -> Codes {
        Codes {
            format: [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            paid_currency: std::array::from_fn(|i| i as f32),
            received_currency: std::array::from_fn(|i| i as f32),
        }
    }

    fn row() -> Row {
        Row {
            ts: 1_661_991_600, // 2022-09-01 00:20, a Thursday
            src: 1,
            dst: 2,
            src_bank: Some(3),
            dst_bank: Some(3),
            amount_paid: 100.0,
            paid_currency: Some(Currency::UsDollar),
            amount_received: 90.0,
            received_currency: Some(Currency::Euro),
            format: Some(PaymentFormat::Wire),
        }
    }

    #[test]
    fn calendar() {
        assert_eq!(hour_and_weekday(0), (0, 4));
        assert_eq!(hour_and_weekday(1_661_991_600), (0, 4));
        assert_eq!(
            hour_and_weekday(1_661_991_600 + 4 * 86_400 + 23 * 3600),
            (23, 1)
        );
        assert_eq!(hour_and_weekday(-1), (23, 3), "before 1970 still works");
    }

    #[test]
    fn fills_every_column() {
        let mut x = [0.0; 11];
        features(&row(), &codes(), &mut x);
        assert_eq!(x[0], 100f64.ln_1p() as f32);
        assert_eq!(x[2], 0.9);
        assert_eq!(&x[3..8], &[1.0, 1.0, 0.0, 0.0, 4.0]);
        assert_eq!(&x[8..], &[4.0, 0.0, 1.0]);
    }

    #[test]
    fn zero_amounts_follow_python() {
        let mut x = [0.0; 11];
        let r = Row {
            amount_paid: 0.0,
            amount_received: 0.0,
            ..row()
        };
        features(&r, &codes(), &mut x);
        assert_eq!(x[2], 1.0);
        let r = Row {
            amount_paid: 0.0,
            amount_received: 5.0,
            ..row()
        };
        features(&r, &codes(), &mut x);
        assert_eq!(x[2], f32::INFINITY);
    }

    #[test]
    fn unknowns() {
        let mut c = codes();
        c.format[4] = -1.0; // wire never seen in training
        let mut x = [0.0; 11];
        let r = Row {
            src_bank: None,
            paid_currency: None,
            ..row()
        };
        features(&r, &c, &mut x);
        assert_eq!(&x[3..5], &[0.0, 0.0], "unknown is 0, not NaN");
        assert_eq!(x[8], -1.0);
        assert_eq!(x[9], -1.0);
    }
}
