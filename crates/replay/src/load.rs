//! Reading an IBM `*_Trans.csv` file into memory, sorted by time.

use std::fs::File;
use std::io::Read;
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use chrono::NaiveDate;

use crate::accounts::AccountIndex;
use crate::model::{Currency, PaymentFormat, Transaction};

const COLUMNS: usize = 11;

pub struct Dataset {
    /// Sorted by (timestamp, tx_id): the order a live stream would deliver.
    pub transactions: Vec<Transaction>,
    pub accounts: AccountIndex,
}

pub fn load_csv(path: &Path) -> Result<Dataset> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    from_reader(file).with_context(|| format!("reading {}", path.display()))
}

pub fn from_reader<R: Read>(reader: R) -> Result<Dataset> {
    let mut csv = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(reader);
    let mut accounts = AccountIndex::new();
    let mut transactions = Vec::new();

    // one record buffer reused for every row instead of allocating per row
    let mut record = csv::StringRecord::new();
    let mut row: u32 = 0;
    while csv.read_record(&mut record)? {
        let tx = parse_row(&record, row, &mut accounts)
            .with_context(|| format!("row {} (line {})", row, row + 2))?;
        transactions.push(tx);
        row += 1;
    }

    // tx_id breaks ties, so the order is total and the same on every run
    transactions.sort_unstable_by_key(|t| (t.ts, t.tx_id));
    Ok(Dataset {
        transactions,
        accounts,
    })
}

fn parse_row(
    r: &csv::StringRecord,
    tx_id: u32,
    accounts: &mut AccountIndex,
) -> Result<Transaction> {
    if r.len() != COLUMNS {
        bail!("expected {COLUMNS} columns, found {}", r.len());
    }
    // the header names both account columns "Account", so go by position
    let field = |i: usize| &r[i];
    Ok(Transaction {
        tx_id,
        ts: parse_timestamp(field(0))?,
        src: accounts.intern(field(1), field(2)),
        dst: accounts.intern(field(3), field(4)),
        amount_received: parse_amount(field(5))?,
        received_currency: Currency::parse(field(6))
            .ok_or_else(|| anyhow!("unknown currency {:?}", field(6)))?,
        amount_paid: parse_amount(field(7))?,
        paid_currency: Currency::parse(field(8))
            .ok_or_else(|| anyhow!("unknown currency {:?}", field(8)))?,
        format: PaymentFormat::parse(field(9))
            .ok_or_else(|| anyhow!("unknown payment format {:?}", field(9)))?,
        is_laundering: match field(10) {
            "0" => false,
            "1" => true,
            other => bail!("bad laundering flag {other:?}"),
        },
    })
}

/// "2022/09/01 00:20" -> seconds since the epoch, treating the time as UTC.
pub fn parse_timestamp(s: &str) -> Result<i64> {
    let bad = || anyhow!("bad timestamp {s:?}, expected YYYY/MM/DD HH:MM");
    let b = s.as_bytes();
    if b.len() != 16 || b[4] != b'/' || b[7] != b'/' || b[10] != b' ' || b[13] != b':' {
        return Err(bad());
    }
    let num = |range: std::ops::Range<usize>| s[range].parse::<u32>().map_err(|_| bad());
    let date =
        NaiveDate::from_ymd_opt(num(0..4)? as i32, num(5..7)?, num(8..10)?).ok_or_else(bad)?;
    let time = date
        .and_hms_opt(num(11..13)?, num(14..16)?, 0)
        .ok_or_else(bad)?;
    Ok(time.and_utc().timestamp())
}

fn parse_amount(s: &str) -> Result<f64> {
    let v: f64 = s.parse().map_err(|_| anyhow!("bad amount {s:?}"))?;
    if !v.is_finite() || v < 0.0 {
        bail!("amount out of range: {s}");
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = include_str!("../../../ml/tests/fixtures/tiny_trans.csv");

    #[test]
    fn parses_timestamps() {
        assert_eq!(parse_timestamp("1970/01/01 00:01").unwrap(), 60);
        assert_eq!(parse_timestamp("2022/09/01 00:20").unwrap(), 1_661_991_600);
        for bad in [
            "2022-09-01 00:20",
            "2022/13/01 00:20",
            "2022/09/01 24:00",
            "2022/09/01 0:20",
            "",
        ] {
            assert!(parse_timestamp(bad).is_err(), "{bad} should fail");
        }
    }

    #[test]
    fn loads_the_fixture_in_time_order() {
        let data = from_reader(SAMPLE.as_bytes()).unwrap();
        let ids: Vec<u32> = data.transactions.iter().map(|t| t.tx_id).collect();
        // file rows 2 (00:00), 3 (00:02), 4 (00:06), then 0 and 1 (both 00:20, file order kept)
        assert_eq!(ids, vec![2, 3, 4, 0, 1]);
        assert!(data.transactions.windows(2).all(|w| w[0].ts <= w[1].ts));
    }

    #[test]
    fn reads_each_column_into_the_right_field() {
        let data = from_reader(SAMPLE.as_bytes()).unwrap();
        let t = data.transactions.iter().find(|t| t.tx_id == 4).unwrap();
        assert_eq!(t.paid_currency, Currency::Euro);
        assert_eq!(t.format, PaymentFormat::Ach);
        assert_eq!(t.amount_paid, 36682.97);
        assert!(!t.is_laundering);
        assert!(
            data.transactions
                .iter()
                .any(|t| t.tx_id == 3 && t.is_laundering)
        );

        let cheque = data.transactions.iter().find(|t| t.tx_id == 1).unwrap();
        assert_eq!(data.accounts.key(cheque.src), Some("3208:8000F4580"));
        assert_eq!(data.accounts.key(cheque.dst), Some("001:8000F5340"));
    }

    #[test]
    fn self_transfers_share_one_account_id() {
        let data = from_reader(SAMPLE.as_bytes()).unwrap();
        let t = data.transactions.iter().find(|t| t.tx_id == 0).unwrap();
        assert_eq!(t.src, t.dst);
    }

    #[test]
    fn bad_rows_say_where_they_are() {
        let broken = "h1,h2,h3,h4,h5,h6,h7,h8,h9,h10,h11\n2022/09/01 00:20,1,A,2,B,1.0,Euro,1.0,Euro,Teleport,0\n";
        let err = format!("{:#}", from_reader(broken.as_bytes()).err().unwrap());
        assert!(err.contains("row 0 (line 2)"), "{err}");
        assert!(err.contains("Teleport"), "{err}");
    }
}
