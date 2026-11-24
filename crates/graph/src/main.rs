use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::Parser;

use detektive_graph::{Engine, EngineConfig, Features, Observation, Payment, Stats, WindowGraph};
use detektive_replay::load;
use detektive_replay::model::Transaction;

/// Run an IBM AML transactions file through the graph engine, in time order:
/// report how big the window gets, how fast it runs, how degrees are
/// distributed and what cycles it finds, and optionally write each payment's
/// features.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// Path to an IBM *_Trans.csv file
    input: PathBuf,

    /// Window length, in hours of event time
    #[arg(long, default_value_t = 96.0)]
    window_hours: f64,

    /// Longest cycle searched for, in payments
    #[arg(long, default_value_t = 12)]
    max_cycle_len: usize,

    /// Payments one cycle search may look at before it gives up
    #[arg(long, default_value_t = 100_000)]
    cycle_budget: u32,

    /// Accounts one scatter-gather search may look at before it gives up
    #[arg(long, default_value_t = 100_000)]
    scatter_gather_budget: u32,

    /// Stop after this many transactions
    #[arg(long)]
    limit: Option<usize>,

    /// Write tx_id and the features of every payment to this CSV
    #[arg(long)]
    features: Option<PathBuf>,

    /// Add payments in batches of this many, searching for cycles on several
    /// threads (0: one by one). The features are the same either way.
    #[arg(long, default_value_t = 0)]
    batch: usize,

    /// Threads for batches (default: one per core)
    #[arg(long)]
    threads: Option<usize>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if let Some(n) = args.threads {
        rayon::ThreadPoolBuilder::new()
            .num_threads(n)
            .build_global()
            .context("setting up threads")?;
    }

    let started = Instant::now();
    let data = load::load_csv(&args.input)?;
    eprintln!(
        "loaded {} transactions, {} accounts in {:.1}s",
        data.transactions.len(),
        data.accounts.len(),
        started.elapsed().as_secs_f64()
    );
    let take = args
        .limit
        .unwrap_or(data.transactions.len())
        .min(data.transactions.len());

    let mut out = match &args.features {
        Some(path) => {
            let file =
                File::create(path).with_context(|| format!("creating {}", path.display()))?;
            let mut w = BufWriter::new(file);
            writeln!(w, "tx_id,{}", Features::NAMES.join(","))?;
            Some(w)
        }
        None => None,
    };

    let window = (args.window_hours * 3600.0).round() as i64;
    let mut engine = Engine::new(EngineConfig {
        window,
        max_cycle_len: args.max_cycle_len,
        cycle_budget: args.cycle_budget,
        scatter_gather_budget: args.scatter_gather_budget,
    });
    let mut peak = Stats::default();
    let mut cycles = CycleSummary::default();
    let mut fans = FanSummary::default();
    let mut degrees = Degrees::default();
    let mut next_snapshot = i64::MIN;
    let mut line = String::new();
    let started = Instant::now();
    // one stream in time order: the newest event is the watermark
    let payment = |t: &Transaction| Payment {
        tx: t.tx_id,
        ts: t.ts,
        src: t.src,
        dst: t.dst,
        amount: detektive_replay::india::to_inr(t.amount_paid, t.paid_currency),
        format: t.format as u8,
    };
    for group in data.transactions[..take].chunks(args.batch.max(1)) {
        let observed: Vec<Option<Observation>> = if args.batch == 0 {
            group
                .iter()
                .map(|t| {
                    engine.advance(t.ts);
                    engine.add(payment(t))
                })
                .collect()
        } else {
            let batch: Vec<(i64, Payment)> = group.iter().map(|t| (t.ts, payment(t))).collect();
            engine.add_batch(&batch)
        };
        // in time order nothing is ever too late
        for obs in observed.into_iter().flatten() {
            cycles.count(&obs.features, obs.cycles.truncated);
            fans.count(obs.features.scatter_gather, obs.scatter_gather.truncated);
            if let Some(w) = out.as_mut() {
                line.clear();
                line.push_str(&obs.tx.to_string());
                for v in obs.features.values() {
                    line.push(',');
                    line.push_str(&v.to_string());
                }
                line.push('\n');
                w.write_all(line.as_bytes())?;
            }
        }

        // checked once per batch in batch mode
        let s = engine.graph().stats();
        if s.edges > peak.edges {
            peak = s;
        }
        // once an hour of event time, keep the degree picture of the
        // fullest window seen so far
        let ts = group.last().map_or(0, |t| t.ts);
        if ts >= next_snapshot {
            next_snapshot = ts + 3600;
            if s.edges > degrees.edges {
                degrees = Degrees::of(engine.graph(), ts);
            }
        }
    }
    if let Some(mut w) = out {
        w.flush()?;
    }
    let elapsed = started.elapsed().as_secs_f64();

    let end = engine.graph().stats();
    eprintln!(
        "{take} payments through a {}h window in {elapsed:.2}s ({:.0}/s{}{})",
        args.window_hours,
        take as f64 / elapsed.max(f64::EPSILON),
        if args.batch > 0 {
            format!(
                ", batches of {} on {} threads",
                args.batch,
                rayon::current_num_threads()
            )
        } else {
            String::new()
        },
        if args.features.is_some() {
            ", writing features"
        } else {
            ""
        }
    );
    eprintln!(
        "largest window: {} edges, {} accounts, {} counterparty pairs, about {:.0} MB",
        peak.edges,
        peak.accounts,
        peak.pairs,
        peak.approx_bytes() as f64 / 1e6
    );
    eprintln!(
        "at the end: {} edges, {} accounts; {} evicted, {} refused as too late",
        end.edges, end.accounts, end.evicted, end.too_late
    );
    cycles.report(args.max_cycle_len);
    fans.report();
    degrees.report();
    if let Some(path) = &args.features {
        eprintln!("features written to {}", path.display());
    }
    Ok(())
}

/// What the cycle search found over the whole run.
#[derive(Default)]
struct CycleSummary {
    payments: u64,
    cycles: u64,
    truncated: u64,
    /// payments closing a cycle, by the length of the shortest one
    by_length: Vec<u64>,
}

impl CycleSummary {
    fn count(&mut self, f: &Features, truncated: bool) {
        if truncated {
            self.truncated += 1;
        }
        if f.cycles == 0 {
            return;
        }
        self.payments += 1;
        self.cycles += u64::from(f.cycles);
        let len = f.shortest_cycle as usize;
        if len >= self.by_length.len() {
            self.by_length.resize(len + 1, 0);
        }
        self.by_length[len] += 1;
    }

    fn report(&self, max_len: usize) {
        eprintln!(
            "\ncycles of 2 to {max_len} payments: {} payments closed {} cycles; {} searches stopped at the budget",
            self.payments, self.cycles, self.truncated
        );
        let shown: Vec<String> = self
            .by_length
            .iter()
            .enumerate()
            .filter(|(_, n)| **n > 0)
            .map(|(len, n)| format!("{len}: {n}"))
            .collect();
        if !shown.is_empty() {
            eprintln!("  payments by their shortest cycle: {}", shown.join(" | "));
        }
    }
}

/// Payments by the width of the widest scatter-gather they are part of.
#[derive(Default)]
struct FanSummary {
    /// widths 0, 1, 2, 3-4, 5-9, 10+
    bands: [u64; 6],
    truncated: u64,
}

impl FanSummary {
    const LABELS: [&'static str; 6] = ["none", "1", "2", "3-4", "5-9", "10+"];

    fn count(&mut self, width: u32, truncated: bool) {
        let band = match width {
            0..=2 => width as usize,
            3..=4 => 3,
            5..=9 => 4,
            _ => 5,
        };
        self.bands[band] += 1;
        self.truncated += u64::from(truncated);
    }

    fn report(&self) {
        let shown: Vec<String> = Self::LABELS
            .iter()
            .zip(self.bands)
            .map(|(l, n)| format!("{l}: {n}"))
            .collect();
        eprintln!(
            "scatter-gather width per payment: {}; {} searches stopped at the budget",
            shown.join(" | "),
            self.truncated
        );
    }
}

/// Fan-in and fan-out of every account in the window at one moment.
#[derive(Default)]
struct Degrees {
    at: i64,
    edges: usize,
    fan_in: Vec<u32>,
    fan_out: Vec<u32>,
}

impl Degrees {
    fn of(g: &WindowGraph, at: i64) -> Self {
        let accounts: Vec<u32> = g.accounts().collect();
        Self {
            at,
            edges: g.stats().edges,
            fan_in: accounts.iter().map(|&a| g.fan_in(a)).collect(),
            fan_out: accounts.iter().map(|&a| g.fan_out(a)).collect(),
        }
    }

    fn report(&self) {
        if self.fan_in.is_empty() {
            return;
        }
        let when = date(self.at);
        eprintln!(
            "\ndegrees in the fullest window (ending {when}, {} edges, {} accounts):",
            self.edges,
            self.fan_in.len()
        );
        describe("fan-in", &self.fan_in);
        describe("fan-out", &self.fan_out);
    }
}

/// Counts in doubling bins, the largest value, and a power-law exponent fitted
/// to the tail by maximum likelihood.
fn describe(name: &str, values: &[u32]) {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let n = sorted.len();
    let pct = |q: f64| sorted[((n - 1) as f64 * q).round() as usize];
    eprintln!(
        "  {name}: median {}, p99 {}, p99.9 {}, max {}",
        pct(0.5),
        pct(0.99),
        pct(0.999),
        sorted[n - 1]
    );

    // doubling bins: 0, 1, 2-3, 4-7, ...
    let below = |x: u32| sorted.partition_point(|&v| v < x);
    let mut bins = Vec::new();
    let (mut lo, mut hi) = (0u32, 1u32);
    while lo <= sorted[n - 1] {
        let count = below(hi) - below(lo);
        bins.push(if hi - lo == 1 {
            format!("{lo}: {count}")
        } else {
            format!("{lo}-{}: {count}", hi - 1)
        });
        lo = hi;
        hi = hi.saturating_mul(2);
    }
    eprintln!("    {}", bins.join(" | "));

    // discrete power law, approximate MLE (Clauset, Shalizi and Newman 2009):
    // alpha = 1 + k / sum(ln(x / (xmin - 0.5))) over the k values >= xmin
    let xmin = 5u32;
    let tail: Vec<f64> = sorted
        .iter()
        .filter(|&&v| v >= xmin)
        .map(|&v| v as f64)
        .collect();
    if tail.len() >= 50 {
        let s: f64 = tail.iter().map(|x| (x / (xmin as f64 - 0.5)).ln()).sum();
        let alpha = 1.0 + tail.len() as f64 / s;
        eprintln!(
            "    tail (>= {xmin}, {} accounts): power-law exponent about {alpha:.2}",
            tail.len()
        );
    }
}

/// YYYY-MM-DD HH:MM for a Unix time (the files carry no time zone).
fn date(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|d| d.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_else(|| ts.to_string())
}
