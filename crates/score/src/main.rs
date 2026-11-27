use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::Parser;

use detektive_graph::{Engine, EngineConfig, Observation, Payment};
use detektive_replay::load;
use detektive_replay::model::Transaction;
use detektive_score::{Model, Row};

/// Run an IBM AML transactions file through the graph engine and the model,
/// in time order, and report how many payments would alert and how long
/// scoring takes. Optionally write every payment's score.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// Path to an IBM *_Trans.csv file
    input: PathBuf,

    /// Directory holding model.onnx and model.json
    #[arg(long)]
    model: PathBuf,

    /// Write tx_id and score of every payment to this CSV
    #[arg(long)]
    scores: Option<PathBuf>,

    /// Window length, in hours of event time
    #[arg(long, default_value_t = 96.0)]
    window_hours: f64,

    /// Add payments in batches of this many, searching for cycles on several
    /// threads (0: one by one). Scores are the same either way.
    #[arg(long, default_value_t = 4096)]
    batch: usize,

    /// Stop after this many transactions
    #[arg(long)]
    limit: Option<usize>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let model = Model::load(&args.model)?;
    eprintln!(
        "model: {} trees, {} nodes, alert threshold {:.4}",
        model.forest().trees(),
        model.forest().nodes(),
        model.threshold()
    );

    let data = load::load_csv(&args.input)?;
    let take = args
        .limit
        .unwrap_or(data.transactions.len())
        .min(data.transactions.len());
    let rows = &data.transactions[..take];

    let mut out = match &args.scores {
        Some(path) => {
            let file =
                File::create(path).with_context(|| format!("creating {}", path.display()))?;
            let mut w = BufWriter::new(file);
            writeln!(w, "tx_id,score")?;
            Some(w)
        }
        None => None,
    };

    let mut engine = Engine::new(EngineConfig {
        window: (args.window_hours * 3600.0).round() as i64,
        ..EngineConfig::default()
    });
    let payment = |t: &Transaction| Payment {
        tx: t.tx_id,
        ts: t.ts,
        src: t.src,
        dst: t.dst,
        amount: detektive_replay::india::to_inr(t.amount_paid, t.paid_currency),
        format: t.format as u8,
    };

    // nanoseconds per payment: the model alone, and (one by one only) the
    // graph and the model together, as the consumer does it
    let mut timings: Vec<u32> = Vec::with_capacity(take);
    let mut whole: Vec<u32> = Vec::with_capacity(if args.batch == 0 { take } else { 0 });
    let nanos = |d: std::time::Duration| d.as_nanos().min(u128::from(u32::MAX)) as u32;
    let mut alerts = 0u64;
    let started = Instant::now();
    for group in rows.chunks(args.batch.max(1)) {
        let mut graph_took = Vec::new();
        let observed: Vec<Option<Observation>> = if args.batch == 0 {
            group
                .iter()
                .map(|t| {
                    let clock = Instant::now();
                    engine.advance(t.ts);
                    let obs = engine.add(payment(t));
                    graph_took.push(clock.elapsed());
                    obs
                })
                .collect()
        } else {
            let batch: Vec<(i64, Payment)> = group.iter().map(|t| (t.ts, payment(t))).collect();
            engine.add_batch(&batch)
        };
        for (k, (t, obs)) in group.iter().zip(observed).enumerate() {
            // in time order nothing is ever too late
            let Some(obs) = obs else { continue };
            let clock = Instant::now();
            let score = model.score(&Row::from(t), &obs.features);
            let took = clock.elapsed();
            timings.push(nanos(took));
            if let Some(g) = graph_took.get(k) {
                whole.push(nanos(*g + took));
            }
            if score >= model.threshold() {
                alerts += 1;
            }
            if let Some(w) = out.as_mut() {
                writeln!(w, "{},{score}", t.tx_id)?;
            }
        }
    }
    if let Some(mut w) = out {
        w.flush()?;
    }
    let elapsed = started.elapsed().as_secs_f64();

    eprintln!(
        "{} payments scored in {elapsed:.1}s, graph included ({:.0}/s)",
        timings.len(),
        timings.len() as f64 / elapsed
    );
    report("model alone", &mut timings);
    if !whole.is_empty() {
        report("graph and model", &mut whole);
    }
    eprintln!(
        "{alerts} alerts at the threshold ({:.3}% of payments)",
        alerts as f64 * 100.0 / timings.len() as f64
    );
    if let Some(path) = &args.scores {
        eprintln!("scores written to {}", path.display());
    }
    Ok(())
}

/// Percentiles of per-payment times given in nanoseconds, printed in µs.
fn report(what: &str, nanos: &mut [u32]) {
    if nanos.is_empty() {
        return;
    }
    nanos.sort_unstable();
    let pct = |p: f64| f64::from(nanos[((nanos.len() - 1) as f64 * p) as usize]) / 1000.0;
    let mean = nanos.iter().map(|&t| f64::from(t)).sum::<f64>() / nanos.len() as f64 / 1000.0;
    eprintln!(
        "{what}, per payment: mean {mean:.1} µs, p50 {:.1}, p99 {:.1}, p99.9 {:.1}, max {:.1}",
        pct(0.5),
        pct(0.99),
        pct(0.999),
        pct(1.0)
    );
}
