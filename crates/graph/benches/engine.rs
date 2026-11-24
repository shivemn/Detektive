//! Micro-benchmarks for the engine's hot paths, on a synthetic stream shaped
//! like HI-Small: most accounts deal with one to three others, a few hubs
//! with many, payments a minute or so apart, some pairs repeating.
//!
//!     cargo bench -p detektive-graph
//!
//! Criterion runs each case until the timing is stable and reports a
//! confidence interval; compare runs on the same machine only. For
//! whole-file numbers use the `detektive-graph` tool on the real data.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use detektive_graph::{Engine, EngineConfig, Payment, WindowGraph};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

const PAYMENTS: usize = 200_000;

/// (watermark, payment) in time order, 200,000 payments over about four days.
fn stream() -> Vec<(i64, Payment)> {
    let mut rng = Rng(0x5eed);
    let accounts = 40_000;
    let mut ts = 0i64;
    (0..PAYMENTS)
        .map(|i| {
            ts += rng.below(4) as i64;
            let pick = |rng: &mut Rng| {
                if rng.below(50) == 0 {
                    rng.below(20) as u32 // hubs
                } else {
                    rng.below(accounts) as u32
                }
            };
            let src = pick(&mut rng);
            // a third of payments go back to someone the sender already knows
            let dst = if rng.below(3) == 0 {
                (src + 1 + rng.below(3) as u32) % accounts as u32
            } else {
                pick(&mut rng)
            };
            let p = Payment {
                tx: i as u32,
                ts,
                src,
                dst,
                amount: 100.0 + rng.below(10_000) as f64,
                format: rng.below(3) as u8,
            };
            (ts, p)
        })
        .collect()
}

fn graph_only(c: &mut Criterion) {
    let payments = stream();
    let mut group = c.benchmark_group("window graph");
    group.throughput(Throughput::Elements(PAYMENTS as u64));
    group.bench_function("insert, evict, degree features", |b| {
        b.iter(|| {
            let mut g = WindowGraph::new(24 * 3600);
            for (w, p) in &payments {
                g.advance(*w);
                g.insert(*p);
                black_box(g.degree_features(p));
            }
        })
    });
    group.finish();
}

fn engine(c: &mut Criterion) {
    let payments = stream();
    let cfg = EngineConfig {
        window: 24 * 3600,
        ..EngineConfig::default()
    };
    let mut group = c.benchmark_group("engine");
    group.throughput(Throughput::Elements(PAYMENTS as u64));
    group.sample_size(10);
    group.bench_function("one by one", |b| {
        b.iter(|| {
            let mut e = Engine::new(cfg);
            for (w, p) in &payments {
                e.advance(*w);
                black_box(e.add(*p));
            }
        })
    });
    for size in [256, 4096] {
        group.bench_with_input(BenchmarkId::new("in batches", size), &size, |b, &size| {
            b.iter(|| {
                let mut e = Engine::new(cfg);
                for batch in payments.chunks(size) {
                    black_box(e.add_batch(batch));
                }
            })
        });
    }
    group.finish();
}

criterion_group!(benches, graph_only, engine);
criterion_main!(benches);
