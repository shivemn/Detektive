//! Where replayed events go.
//!
//! Everything downstream talks to the `Sink` trait, so adding a Redpanda sink
//! in week 3 means one new type here and no change to the replay loop.

use std::io::{BufWriter, Write};

use anyhow::Result;

use crate::event::Event;

pub trait Sink {
    fn send(&mut self, event: &Event) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
}

/// One JSON object per line, to any writer (stdout, a file, a buffer).
pub struct JsonLines<W: Write> {
    out: BufWriter<W>,
}

impl<W: Write> JsonLines<W> {
    pub fn new(writer: W) -> Self {
        Self {
            out: BufWriter::with_capacity(1 << 16, writer),
        }
    }
}

impl<W: Write> Sink for JsonLines<W> {
    fn send(&mut self, event: &Event) -> Result<()> {
        serde_json::to_writer(&mut self.out, event)?;
        self.out.write_all(b"\n")?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.out.flush()?;
        Ok(())
    }
}

/// Throws events away. Measures the replay loop on its own.
pub struct Discard;

impl Sink for Discard {
    fn send(&mut self, _event: &Event) -> Result<()> {
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::EventOptions;
    use crate::model::{Currency, PaymentFormat, Transaction};

    #[test]
    fn writes_one_parseable_line_per_event() {
        let t = Transaction {
            tx_id: 1,
            ts: 0,
            src: 0,
            dst: 1,
            src_bank: 0,
            dst_bank: 0,
            amount_paid: 1.5,
            paid_currency: Currency::Euro,
            amount_received: 1.5,
            received_currency: Currency::Euro,
            format: PaymentFormat::Wire,
            is_laundering: false,
        };
        let mut buf = Vec::new();
        {
            let mut sink = JsonLines::new(&mut buf);
            for _ in 0..3 {
                sink.send(&Event::from_transaction(&t, EventOptions::default()))
                    .unwrap();
            }
            sink.flush().unwrap();
        }
        let text = String::from_utf8(buf).unwrap();
        let lines: Vec<_> = text.lines().collect();
        assert_eq!(lines.len(), 3);
        for line in lines {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(v["format"], "wire");
        }
    }
}
