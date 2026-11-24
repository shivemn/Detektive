//! The windowed graph against the simplest possible implementation: keep
//! every payment ever inserted in a list, and answer each question by
//! scanning it for the ones inside the window. Too slow to use, too simple to
//! get wrong.
//!
//! The stream is random: several partitions, each mostly in time order,
//! interleaved unevenly, with some late payments, parallel edges, payments to
//! oneself, quiet partitions and a few hub accounts. After every step the
//! graph must agree with the list on every account touched so far.

use std::time::{Duration, Instant};

use detektive_graph::{Insert, Payment, Watermarks, WindowGraph};

/// xorshift: deterministic, no dependency, good enough to shuffle a stream
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

struct Reference {
    payments: Vec<Payment>,
}

impl Reference {
    fn live(&self, cutoff: Option<i64>) -> impl Iterator<Item = &Payment> {
        let cutoff = cutoff.unwrap_or(i64::MIN);
        self.payments.iter().filter(move |p| p.ts >= cutoff)
    }
}

fn sorted(mut v: Vec<u32>) -> Vec<u32> {
    v.sort_unstable();
    v
}

fn run(seed: u64, steps: usize, window: i64, accounts: u32) {
    let mut rng = Rng(seed);
    let partitions = 3u32;
    let mut clocks = vec![1_000_000i64; partitions as usize];
    let mut lag = vec![0i64; partitions as usize];
    let mut global = 1_000_000i64;
    let mut watermarks = Watermarks::new(Duration::from_secs(20));
    let mut graph = WindowGraph::new(window);
    let mut reference = Reference {
        payments: Vec::new(),
    };
    let t0 = Instant::now();

    for step in 0..steps {
        // partition 2 goes quiet for a while, so idleness gets exercised
        let quiet = (steps / 3..steps / 2).contains(&step);
        let p = loop {
            let p = rng.below(partitions as u64) as u32;
            if !(quiet && p == 2) {
                break p;
            }
        };
        // one clock for the world; each partition trails it by its own
        // wandering lag, and never goes back in time
        global += rng.below(30) as i64;
        let l = &mut lag[p as usize];
        *l = (*l + rng.below(21) as i64 - 10).clamp(0, window / 3);
        let c = &mut clocks[p as usize];
        *c = (*c).max(global - *l);
        let mut ts = clocks[p as usize];
        if rng.below(50) == 0 {
            ts -= window / 2 + rng.below(window as u64 * 2) as i64; // a late payment
        }
        // a few hubs take a large share of the payments
        let pick = |rng: &mut Rng| {
            if rng.below(4) == 0 {
                rng.below(3) as u32
            } else {
                rng.below(accounts as u64) as u32
            }
        };
        let src = pick(&mut rng);
        let dst = if rng.below(30) == 0 {
            src
        } else {
            pick(&mut rng)
        };
        let payment = Payment {
            tx: step as u32,
            ts,
            src,
            dst,
            amount: rng.below(10_000) as f64,
            format: 0,
        };

        // wall clock moves one second per step, so quiet partitions go idle
        let now = t0 + Duration::from_secs(step as u64);
        watermarks.observe(p, ts, now);
        if let Some(w) = watermarks.advance(now) {
            graph.advance(w);
        }
        match graph.insert(payment) {
            Insert::Added => reference.payments.push(payment),
            Insert::TooLate => {
                assert!(
                    payment.ts < graph.cutoff().unwrap(),
                    "refused but in window"
                )
            }
        }

        let cutoff = graph.cutoff();
        for a in 0..accounts {
            let out: Vec<u32> = reference
                .live(cutoff)
                .filter(|q| q.src == a)
                .map(|q| q.tx)
                .collect();
            let inc: Vec<u32> = reference
                .live(cutoff)
                .filter(|q| q.dst == a)
                .map(|q| q.tx)
                .collect();
            assert_eq!(
                graph.out_degree(a) as usize,
                out.len(),
                "out degree of {a} at step {step}"
            );
            assert_eq!(
                graph.in_degree(a) as usize,
                inc.len(),
                "in degree of {a} at step {step}"
            );
            assert_eq!(
                sorted(graph.out_edges(a).map(|h| h.tx).collect()),
                sorted(out)
            );
            assert_eq!(
                sorted(graph.in_edges(a).map(|h| h.tx).collect()),
                sorted(inc)
            );
            let distinct = |v: Vec<u32>| {
                let mut v = v;
                v.sort_unstable();
                v.dedup();
                v.len() as u32
            };
            let paid: Vec<u32> = reference
                .live(cutoff)
                .filter(|q| q.src == a)
                .map(|q| q.dst)
                .collect();
            let paid_by: Vec<u32> = reference
                .live(cutoff)
                .filter(|q| q.dst == a)
                .map(|q| q.src)
                .collect();
            assert_eq!(
                graph.fan_out(a),
                distinct(paid),
                "fan-out of {a} at step {step}"
            );
            assert_eq!(
                graph.fan_in(a),
                distinct(paid_by),
                "fan-in of {a} at step {step}"
            );
        }
        let pairs = reference
            .live(cutoff)
            .filter(|q| q.src == payment.src && q.dst == payment.dst)
            .count();
        assert_eq!(graph.pair_count(payment.src, payment.dst) as usize, pairs);
        let f = graph.degree_features(&payment);
        assert_eq!(f.src_fan_out, graph.fan_out(payment.src));
        assert_eq!(f.dst_in_degree, graph.in_degree(payment.dst));

        let live: Vec<&Payment> = reference.live(cutoff).collect();
        let stats = graph.stats();
        assert_eq!(stats.edges, live.len(), "live edges at step {step}");
        let mut touched: Vec<u32> = live.iter().flat_map(|q| [q.src, q.dst]).collect();
        touched.sort_unstable();
        touched.dedup();
        assert_eq!(
            stats.accounts,
            touched.len(),
            "active accounts at step {step}"
        );
        assert!(stats.stored >= 2 * stats.edges);
        let mut pairs: Vec<(u32, u32)> = live.iter().map(|q| (q.src, q.dst)).collect();
        pairs.sort_unstable();
        pairs.dedup();
        assert_eq!(stats.pairs, 2 * pairs.len(), "pairs at step {step}");
    }
    let s = graph.stats();
    // the stream must actually exercise eviction and refusal
    assert!(s.evicted > 0 && s.too_late > 0);
}

#[test]
fn matches_the_reference_on_random_streams() {
    for seed in 1..=20 {
        run(seed * 7919, 1500, 3000, 25);
    }
}

#[test]
fn matches_the_reference_with_a_tiny_window() {
    // most payments are evicted soon after they arrive
    for seed in 1..=10 {
        run(seed * 104_729, 1500, 60, 12);
    }
}

#[test]
fn memory_is_released_once_the_stream_moves_on() {
    let mut g = WindowGraph::new(3600);
    for i in 0..10_000u32 {
        g.insert(Payment {
            tx: i,
            ts: i as i64,
            src: i % 500,
            dst: (i * 7) % 500,
            amount: 1.0,
            format: 0,
        });
        g.advance(i as i64);
    }
    // a day later nothing is left
    g.advance(10_000 + 86_400);
    let s = g.stats();
    assert_eq!((s.edges, s.accounts, s.stored, s.pairs), (0, 0, 0, 0));
    assert_eq!(s.evicted, 10_000);
}
