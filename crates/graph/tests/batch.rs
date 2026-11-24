//! Adding payments in parallel batches must give exactly what adding them
//! one by one gives: every feature, every cycle found (down to the example
//! payments chosen), every scatter-gather, in every random stream, for any
//! batch size and any number of threads.

use std::time::{Duration, Instant};

use detektive_graph::{Engine, EngineConfig, Observation, Payment, Watermarks};

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

/// A stream from three partitions read at their own pace, with late
/// payments, a few hubs and repeated pairs: each payment with the watermark
/// to move the window to before it.
fn stream(seed: u64, n: usize, accounts: u64) -> Vec<(i64, Payment)> {
    let mut rng = Rng(seed);
    let mut watermarks = Watermarks::new(Duration::from_secs(20));
    let mut clocks = [0i64; 3];
    let mut global = 0i64;
    let t0 = Instant::now();
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let part = rng.below(3) as usize;
        global += rng.below(30) as i64;
        clocks[part] = clocks[part].max(global - rng.below(200) as i64);
        let mut ts = clocks[part];
        if rng.below(40) == 0 {
            ts -= rng.below(2_000) as i64;
        }
        let pick = |rng: &mut Rng| {
            if rng.below(5) == 0 {
                rng.below(3) as u32
            } else {
                rng.below(accounts) as u32
            }
        };
        let src = pick(&mut rng);
        let dst = pick(&mut rng);
        let now = t0 + Duration::from_millis(i as u64 * 100);
        watermarks.observe(part as u32, ts, now);
        let w = watermarks.advance(now).unwrap();
        out.push((
            w,
            Payment {
                tx: i as u32,
                ts,
                src,
                dst,
                amount: 1.0 + rng.below(5_000) as f64,
                format: rng.below(3) as u8,
            },
        ));
    }
    out
}

fn config() -> EngineConfig {
    EngineConfig {
        window: 1_500,
        max_cycle_len: 8,
        ..EngineConfig::default()
    }
}

fn one_by_one(payments: &[(i64, Payment)]) -> Vec<Option<Observation>> {
    let mut e = Engine::new(config());
    payments
        .iter()
        .map(|&(w, p)| {
            e.advance(w);
            e.add(p)
        })
        .collect()
}

fn in_batches(payments: &[(i64, Payment)], seed: u64) -> Vec<Option<Observation>> {
    let mut rng = Rng(seed);
    let mut e = Engine::new(config());
    let mut out = Vec::new();
    let mut rest = payments;
    while !rest.is_empty() {
        let size = (1 + rng.below(200) as usize).min(rest.len());
        out.extend(e.add_batch(&rest[..size]));
        rest = &rest[size..];
    }
    out
}

#[test]
fn batches_give_exactly_what_one_by_one_gives() {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let (mut cycles, mut fans, mut refused) = (0, 0, 0);
    for seed in 1..=12 {
        let payments = stream(seed * 9_973, 3_000, 40);
        let expected = one_by_one(&payments);
        let got = pool.install(|| in_batches(&payments, seed));
        assert_eq!(got.len(), expected.len());
        for (i, (g, e)) in got.iter().zip(&expected).enumerate() {
            assert_eq!(g, e, "payment {i} of stream {seed}");
        }
        cycles += expected
            .iter()
            .flatten()
            .filter(|o| o.features.cycles > 0)
            .count();
        fans += expected
            .iter()
            .flatten()
            .filter(|o| o.features.scatter_gather >= 3)
            .count();
        refused += expected.iter().filter(|o| o.is_none()).count();
    }
    // the streams must exercise what could differ
    assert!(cycles > 1_000, "only {cycles} payments closed a cycle");
    assert!(fans > 100, "only {fans} wide scatter-gathers");
    assert!(refused > 10, "only {refused} payments refused as too late");
}

#[test]
fn a_batch_of_one_is_the_same_as_add() {
    let payments = stream(7, 500, 20);
    let mut e = Engine::new(config());
    let got: Vec<_> = payments.iter().flat_map(|&p| e.add_batch(&[p])).collect();
    assert_eq!(got, one_by_one(&payments));
}
