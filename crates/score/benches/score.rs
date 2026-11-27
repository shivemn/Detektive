//! Time to score one payment with the model checked in for the tests.
//!
//!     cargo bench -p detektive-score

use std::path::Path;

use criterion::{Criterion, criterion_group, criterion_main};
use detektive_graph::Features;
use detektive_replay::model::{Currency, PaymentFormat};
use detektive_score::{Model, Row};

fn bench(c: &mut Criterion) {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let model = Model::load(&dir).expect("fixture model");
    let rows: Vec<(Row, Features)> = (0..1024u32)
        .map(|i| {
            let row = Row {
                ts: 1_661_990_400 + i64::from(i) * 37,
                src: i,
                dst: i * 7 % 1000,
                src_bank: Some(i % 13),
                dst_bank: Some(i % 11),
                amount_paid: f64::from(i % 500) * 13.7 + 1.0,
                paid_currency: Some(Currency::ALL[i as usize % Currency::ALL.len()]),
                amount_received: f64::from(i % 500) * 13.1 + 1.0,
                received_currency: Some(Currency::ALL[i as usize % 3]),
                format: Some(PaymentFormat::ALL[i as usize % PaymentFormat::ALL.len()]),
            };
            let features = Features {
                src_out_degree: i % 9 + 1,
                src_fan_out: i % 5 + 1,
                dst_in_degree: i % 4 + 1,
                dst_fan_in: i % 3 + 1,
                pair_count: i % 2 + 1,
                cycles: u32::from(i % 50 == 0),
                ..Features::default()
            };
            (row, features)
        })
        .collect();
    let mut i = 0;
    c.bench_function("score one payment (fixture model)", |b| {
        b.iter(|| {
            i = (i + 1) % rows.len();
            let (row, features) = &rows[i];
            std::hint::black_box(model.score(row, features))
        })
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
