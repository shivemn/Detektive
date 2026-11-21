//! The transaction record as the rest of Detektive sees it.
//!
//! Text fields from the CSV (currencies, payment formats) become small enums,
//! and account ids become `u32` indices (see `accounts.rs`), so one record is
//! about 48 bytes and five million of them fit comfortably in memory.

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Currency {
    UsDollar,
    Euro,
    UkPound,
    SwissFranc,
    Yuan,
    Rupee,
    Yen,
    Ruble,
    CanadianDollar,
    AustralianDollar,
    SaudiRiyal,
    Shekel,
    MexicanPeso,
    BrazilReal,
    Bitcoin,
}

impl Currency {
    /// Parse the name exactly as the IBM files spell it.
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "US Dollar" => Self::UsDollar,
            "Euro" => Self::Euro,
            "UK Pound" => Self::UkPound,
            "Swiss Franc" => Self::SwissFranc,
            "Yuan" => Self::Yuan,
            "Rupee" => Self::Rupee,
            "Yen" => Self::Yen,
            "Ruble" => Self::Ruble,
            "Canadian Dollar" => Self::CanadianDollar,
            "Australian Dollar" => Self::AustralianDollar,
            "Saudi Riyal" => Self::SaudiRiyal,
            "Shekel" => Self::Shekel,
            "Mexican Peso" => Self::MexicanPeso,
            "Brazil Real" => Self::BrazilReal,
            "Bitcoin" => Self::Bitcoin,
            _ => return None,
        })
    }

    /// ISO 4217 code ("BTC" is the common unofficial one).
    pub fn code(self) -> &'static str {
        match self {
            Self::UsDollar => "USD",
            Self::Euro => "EUR",
            Self::UkPound => "GBP",
            Self::SwissFranc => "CHF",
            Self::Yuan => "CNY",
            Self::Rupee => "INR",
            Self::Yen => "JPY",
            Self::Ruble => "RUB",
            Self::CanadianDollar => "CAD",
            Self::AustralianDollar => "AUD",
            Self::SaudiRiyal => "SAR",
            Self::Shekel => "ILS",
            Self::MexicanPeso => "MXN",
            Self::BrazilReal => "BRL",
            Self::Bitcoin => "BTC",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PaymentFormat {
    Ach,
    Cheque,
    CreditCard,
    Cash,
    Wire,
    Reinvestment,
    Bitcoin,
}

impl PaymentFormat {
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "ACH" => Self::Ach,
            "Cheque" => Self::Cheque,
            "Credit Card" => Self::CreditCard,
            "Cash" => Self::Cash,
            "Wire" => Self::Wire,
            "Reinvestment" => Self::Reinvestment,
            "Bitcoin" => Self::Bitcoin,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ach => "ach",
            Self::Cheque => "cheque",
            Self::CreditCard => "credit_card",
            Self::Cash => "cash",
            Self::Wire => "wire",
            Self::Reinvestment => "reinvestment",
            Self::Bitcoin => "bitcoin",
        }
    }
}

impl fmt::Display for PaymentFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Transaction {
    /// Row number in the source file, starting at 0. Matches `tx_id` on the
    /// Python side, so results can be joined across the two.
    pub tx_id: u32,
    /// Seconds since the Unix epoch. The files carry no time zone.
    pub ts: i64,
    pub src: u32,
    pub dst: u32,
    pub amount_paid: f64,
    pub paid_currency: Currency,
    pub amount_received: f64,
    pub received_currency: Currency,
    pub format: PaymentFormat,
    pub is_laundering: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_currency_round_trips_to_a_distinct_code() {
        let names = [
            "US Dollar",
            "Euro",
            "UK Pound",
            "Swiss Franc",
            "Yuan",
            "Rupee",
            "Yen",
            "Ruble",
            "Canadian Dollar",
            "Australian Dollar",
            "Saudi Riyal",
            "Shekel",
            "Mexican Peso",
            "Brazil Real",
            "Bitcoin",
        ];
        let mut codes: Vec<_> = names
            .iter()
            .map(|n| Currency::parse(n).unwrap().code())
            .collect();
        codes.sort();
        codes.dedup();
        assert_eq!(codes.len(), names.len());
    }

    #[test]
    fn unknown_names_are_rejected() {
        assert_eq!(Currency::parse("Dogecoin"), None);
        assert_eq!(
            PaymentFormat::parse("ach"),
            None,
            "parsing is case-sensitive, like the files"
        );
    }

    #[test]
    fn transaction_stays_small() {
        assert!(std::mem::size_of::<Transaction>() <= 48);
    }
}
