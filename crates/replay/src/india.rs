//! An Indian-looking view of the synthetic data: UPI-style ids, INR amounts
//! and IST timestamps. It changes how events look, never what they contain,
//! and no model is trained on it.

use chrono::DateTime;

use crate::model::Currency;

/// A UPI-style virtual payment address for an account id. "@dkbank" is a made-up
/// handle; it does not belong to any real bank or payment app.
pub fn vpa(account: u32) -> String {
    format!("acct{account:07}@dkbank")
}

/// Rupees per one unit of `currency`.
///
/// ILLUSTRATIVE ONLY: rough, rounded values near September 2022 (when the IBM
/// data is dated). They are fixed constants, not live or exact rates, and
/// exist only so the demo can show amounts in rupees.
pub fn inr_per_unit(currency: Currency) -> f64 {
    match currency {
        Currency::Rupee => 1.0,
        Currency::UsDollar => 80.0,
        Currency::Euro => 80.0,
        Currency::UkPound => 92.0,
        Currency::SwissFranc => 82.0,
        Currency::Yuan => 11.5,
        Currency::Yen => 0.56,
        Currency::Ruble => 1.3,
        Currency::CanadianDollar => 61.0,
        Currency::AustralianDollar => 54.0,
        Currency::SaudiRiyal => 21.3,
        Currency::Shekel => 23.0,
        Currency::MexicanPeso => 4.0,
        Currency::BrazilReal => 15.4,
        Currency::Bitcoin => 1_600_000.0,
    }
}

pub fn to_inr(amount: f64, currency: Currency) -> f64 {
    (amount * inr_per_unit(currency) * 100.0).round() / 100.0
}

/// The data's wall-clock time, labelled as IST. The source files carry no time
/// zone, so this keeps the clock reading and only states the zone.
pub fn ist_label(ts: i64) -> String {
    match DateTime::from_timestamp(ts, 0) {
        Some(t) => t.naive_utc().format("%Y-%m-%dT%H:%M:%S+05:30").to_string(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vpa_is_stable_and_padded() {
        assert_eq!(vpa(42), "acct0000042@dkbank");
        assert_eq!(vpa(42), vpa(42));
    }

    #[test]
    fn rupees_stay_rupees_and_others_convert() {
        assert_eq!(to_inr(500.0, Currency::Rupee), 500.0);
        assert_eq!(to_inr(10.0, Currency::UsDollar), 800.0);
        assert_eq!(to_inr(0.333, Currency::Yen), 0.19);
    }

    #[test]
    fn keeps_clock_time_and_labels_the_zone() {
        // 2022/09/01 00:20 in the file
        assert_eq!(ist_label(1_661_991_600), "2022-09-01T00:20:00+05:30");
    }
}
