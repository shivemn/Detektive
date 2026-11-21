//! The payment event as it arrives from the stream.
//!
//! Text fields borrow straight from the message bytes (`Cow` falls back to an
//! owned string only if a value contains escape characters), so decoding a
//! message allocates nothing for them.

use std::borrow::Cow;

use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq)]
pub struct PaymentEvent<'a> {
    pub tx_id: u32,
    pub ts: i64,
    pub src: u32,
    pub dst: u32,
    pub amount: f64,
    #[serde(borrow)]
    pub currency: Cow<'a, str>,
    pub received_amount: f64,
    #[serde(borrow)]
    pub received_currency: Cow<'a, str>,
    #[serde(borrow)]
    pub format: Cow<'a, str>,
}

#[derive(Debug, PartialEq)]
pub enum DecodeError {
    Empty,
    BadJson(String),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => f.write_str("empty payload"),
            Self::BadJson(e) => write!(f, "invalid event: {e}"),
        }
    }
}

pub fn decode(payload: Option<&[u8]>) -> Result<PaymentEvent<'_>, DecodeError> {
    match payload {
        None | Some([]) => Err(DecodeError::Empty),
        Some(bytes) => {
            serde_json::from_slice(bytes).map_err(|e| DecodeError::BadJson(e.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EVENT: &[u8] = br#"{"tx_id":7,"ts":1661991600,"src":1,"dst":2,"amount":10.0,"currency":"USD","received_amount":9.5,"received_currency":"EUR","format":"ach"}"#;

    #[test]
    fn decodes_without_copying_strings() {
        let e = decode(Some(EVENT)).unwrap();
        assert_eq!(e.tx_id, 7);
        assert_eq!(e.format, "ach");
        assert!(matches!(e.currency, Cow::Borrowed(_)));
    }

    #[test]
    fn ignores_extra_fields_like_the_india_view() {
        let with_extra = br#"{"tx_id":1,"ts":0,"src":1,"dst":2,"amount":1.0,"currency":"USD","received_amount":1.0,"received_currency":"USD","format":"wire","src_vpa":"acct0000001@dkbank"}"#;
        assert!(decode(Some(with_extra)).is_ok());
    }

    #[test]
    fn rejects_empty_and_broken_payloads() {
        assert_eq!(decode(None), Err(DecodeError::Empty));
        assert_eq!(decode(Some(b"")), Err(DecodeError::Empty));
        assert!(matches!(
            decode(Some(b"{not json")),
            Err(DecodeError::BadJson(_))
        ));
        assert!(matches!(
            decode(Some(br#"{"tx_id":1}"#)),
            Err(DecodeError::BadJson(_))
        ));
    }
}
