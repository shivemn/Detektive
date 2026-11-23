//! The cycle search against brute force: for every payment, list every chain
//! of individual payments that runs from its receiver back to its sender in
//! time order, inside the window, without repeating an account, and group
//! them by the accounts they pass through. No pruning, no "latest payment
//! per pair" shortcut: if the engine's reasoning is wrong anywhere, the
//! counts differ.

use std::collections::{BTreeMap, BTreeSet};

use detektive_graph::{Engine, EngineConfig, Payment, cycles::MAX_RECORDED};

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

/// Every cycle `p` closes, as account sequences (receiver first, sender
/// last), each with the latest time its first payment can have.
fn brute(live: &[Payment], p: &Payment, max_len: usize) -> BTreeMap<Vec<u32>, i64> {
    let mut found = BTreeMap::new();
    if p.src == p.dst {
        return found;
    }
    // chains of payments starting at the receiver
    fn walk(
        live: &[Payment],
        p: &Payment,
        max_len: usize,
        accounts: &mut Vec<u32>,
        first_ts: Option<i64>,
        last_ts: i64,
        found: &mut BTreeMap<Vec<u32>, i64>,
    ) {
        let at = *accounts.last().unwrap();
        for e in live.iter().filter(|e| e.src == at) {
            if e.ts < last_ts || e.ts > p.ts || accounts.contains(&e.dst) && e.dst != p.src {
                continue;
            }
            if accounts.contains(&e.dst) {
                continue;
            }
            let first = first_ts.unwrap_or(e.ts);
            accounts.push(e.dst);
            if e.dst == p.src {
                // the chain plus the closing payment
                let entry = found.entry(accounts.clone()).or_insert(first);
                *entry = (*entry).max(first);
            } else if accounts.len() < max_len {
                walk(live, p, max_len, accounts, Some(first), e.ts, found);
            }
            accounts.pop();
        }
    }
    let mut accounts = vec![p.dst];
    walk(live, p, max_len, &mut accounts, None, i64::MIN, &mut found);
    found
}

fn run(seed: u64, steps: usize, window: i64, accounts: u64, max_len: usize) -> (u32, u32) {
    let mut rng = Rng(seed);
    let mut engine = Engine::new(EngineConfig {
        window,
        max_cycle_len: max_len,
        cycle_budget: u32::MAX,
    });
    let mut live: Vec<Payment> = Vec::new();
    // every cycle found so far: (latest first-payment time, accounts)
    let mut recorded: Vec<(i64, Vec<u32>)> = Vec::new();
    let mut all_recorded = true;
    let mut clock = 0i64;
    let mut watermark = i64::MIN;
    let (mut total, mut longest) = (0u32, 0u32);

    for step in 0..steps {
        clock += rng.below(20) as i64;
        let mut ts = clock;
        if rng.below(15) == 0 {
            ts -= rng.below(window as u64) as i64; // late, often still inside
        }
        let src = rng.below(accounts) as u32;
        let dst = if rng.below(25) == 0 {
            src
        } else {
            rng.below(accounts) as u32
        };
        let p = Payment {
            tx: step as u32,
            ts,
            src,
            dst,
            amount: 1.0,
        };

        watermark = watermark.max(ts);
        engine.advance(watermark);
        let cutoff = engine.graph().cutoff().unwrap();
        live.retain(|e| e.ts >= cutoff);
        recorded.retain(|(oldest, _)| *oldest >= cutoff);

        let expected = brute(&live, &p, max_len);
        let Some(obs) = engine.add(p) else {
            assert!(p.ts < cutoff, "refused a payment inside the window");
            continue;
        };
        live.push(p);
        let c = &obs.cycles;

        assert!(!c.truncated);
        assert_eq!(
            c.count as usize,
            expected.len(),
            "cycles closed by step {step}"
        );
        let shortest = expected.keys().map(|k| k.len() as u32).min().unwrap_or(0);
        assert_eq!(c.shortest, shortest, "shortest cycle at step {step}");
        assert_eq!(obs.features.cycles, c.count);
        assert_eq!(obs.features.shortest_cycle, c.shortest);

        if c.count as usize <= MAX_RECORDED {
            let got: BTreeMap<Vec<u32>, i64> = c
                .found
                .iter()
                .map(|f| (f.accounts.clone(), f.oldest))
                .collect();
            assert_eq!(got, expected, "cycles found at step {step}");
        } else {
            all_recorded = false;
        }

        // the example is a real cycle of the shortest length
        if c.count > 0 {
            let ex = &c.example;
            assert_eq!(ex.len() as u32, c.shortest);
            assert_eq!(
                *ex.last().unwrap(),
                detektive_graph::Hop {
                    tx: p.tx,
                    ts: p.ts,
                    src: p.src,
                    dst: p.dst
                }
            );
            assert_eq!(ex[0].src, p.dst);
            let mut seen = BTreeSet::new();
            for w in ex.windows(2) {
                assert_eq!(w[0].dst, w[1].src, "hops connect");
                assert!(w[0].ts <= w[1].ts, "hops in time order");
            }
            for h in ex {
                assert!(seen.insert(h.src), "no account twice");
                assert!(
                    live.iter()
                        .any(|e| e.tx == h.tx && e.src == h.src && e.dst == h.dst && e.ts == h.ts),
                    "hop {h:?} is a payment in the window"
                );
            }
        }

        // cycle counts per account
        for (accounts, oldest) in &expected {
            recorded.push((*oldest, accounts.clone()));
        }
        if all_recorded {
            for a in [p.src, p.dst] {
                let n = recorded.iter().filter(|(_, acc)| acc.contains(&a)).count() as u32;
                let f = &obs.features;
                let got = if a == p.src {
                    f.src_cycles
                } else {
                    f.dst_cycles
                };
                assert_eq!(got, n, "cycles of account {a} at step {step}");
            }
            assert_eq!(engine.live_cycles(), recorded.len());
        }

        total += c.count;
        longest = longest.max(expected.keys().map(|k| k.len() as u32).max().unwrap_or(0));
    }
    (total, longest)
}

#[test]
fn matches_brute_force_on_random_streams() {
    let mut total = 0;
    let mut longest = 0;
    for seed in 1..=40 {
        let (t, l) = run(seed * 6151, 1_000, 300, 9, 6);
        total += t;
        longest = longest.max(l);
    }
    // the streams must actually contain cycles, long ones included
    eprintln!("{total} cycles, longest {longest}");
    assert!(total > 500, "only {total} cycles");
    assert!(longest >= 5, "longest cycle {longest}");
}

#[test]
fn matches_brute_force_with_long_cycles_allowed() {
    for seed in 1..=20 {
        run(seed * 7727, 400, 200, 7, 12);
    }
}
