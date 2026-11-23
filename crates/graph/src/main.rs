use std::path::PathBuf;
use std::time::Instant;

use anyhow::Result;
use clap::Parser;

use detektive_graph::{Payment, Stats, WindowGraph};
use detektive_replay::load;

/// Run an IBM AML transactions file through the windowed graph, in time
/// order, and report how big the window gets and how fast it runs.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// Path to an IBM *_Trans.csv file
    input: PathBuf,

    /// Window length, in hours of event time
    #[arg(long, default_value_t = 24.0)]
    window_hours: f64,

    /// Stop after this many transactions
    #[arg(long)]
    limit: Option<usize>,
}

fn main() -> Result<()> {
    let args = Args::parse();

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

    let window = (args.window_hours * 3600.0).round() as i64;
    let mut graph = WindowGraph::new(window);
    let mut peak = Stats::default();
    let started = Instant::now();
    for t in &data.transactions[..take] {
        // one stream in time order: the newest event is the watermark
        graph.advance(t.ts);
        graph.insert(Payment {
            tx: t.tx_id,
            ts: t.ts,
            src: t.src,
            dst: t.dst,
            amount: t.amount_paid,
        });
        let s = graph.stats();
        if s.edges > peak.edges {
            peak = s;
        }
    }
    let elapsed = started.elapsed().as_secs_f64();

    let end = graph.stats();
    eprintln!(
        "{take} payments through a {}h window in {elapsed:.2}s ({:.0}/s)",
        args.window_hours,
        take as f64 / elapsed.max(f64::EPSILON)
    );
    eprintln!(
        "largest window: {} edges, {} accounts, about {:.0} MB",
        peak.edges,
        peak.accounts,
        peak.approx_bytes() as f64 / 1e6
    );
    eprintln!(
        "at the end: {} edges, {} accounts; {} evicted, {} refused as too late",
        end.edges, end.accounts, end.evicted, end.too_late
    );
    Ok(())
}
