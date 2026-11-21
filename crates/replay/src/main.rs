use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::Parser;

use detektive_replay::event::EventOptions;
use detektive_replay::pace::Speed;
use detektive_replay::sink::{Discard, JsonLines, Sink};
use detektive_replay::{ReplayOptions, load, replay};

/// Replay an IBM AML transactions file as a time-ordered stream of JSON lines.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// Path to an IBM *_Trans.csv file
    input: PathBuf,

    /// Replay speed: "max", or a factor like 1 (real time) or 100 (100x faster)
    #[arg(long, default_value = "max")]
    speed: Speed,

    /// Stop after this many transactions
    #[arg(long)]
    limit: Option<usize>,

    /// Where to write events: a file path, "-" for stdout, "none" to discard,
    /// or "kafka" to publish to Redpanda (needs the `kafka` feature)
    #[arg(long, default_value = "-")]
    out: String,

    /// Redpanda/Kafka bootstrap servers, used with --out kafka
    #[arg(long, default_value = "localhost:19092")]
    brokers: String,

    /// Topic to publish to, used with --out kafka
    #[arg(long, default_value = "payments")]
    topic: String,

    /// Add UPI-style ids, INR amounts and IST times to each event
    #[arg(long)]
    india: bool,

    /// Include the laundering label (off by default: a live stream has none)
    #[arg(long)]
    with_labels: bool,
}

#[cfg(feature = "kafka")]
fn kafka_sink(brokers: &str, topic: &str) -> Result<Box<dyn Sink>> {
    Ok(Box::new(detektive_replay::kafka::KafkaSink::new(
        brokers, topic,
    )?))
}

#[cfg(not(feature = "kafka"))]
fn kafka_sink(_brokers: &str, _topic: &str) -> Result<Box<dyn Sink>> {
    anyhow::bail!("this build has no Kafka support; rebuild with `--features kafka`")
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

    let mut sink: Box<dyn Sink> = match args.out.as_str() {
        "-" => Box::new(JsonLines::new(io::stdout().lock())),
        "none" => Box::new(Discard),
        "kafka" => kafka_sink(&args.brokers, &args.topic)?,
        path => Box::new(JsonLines::new(
            File::create(path).with_context(|| format!("creating {path}"))?,
        )),
    };

    let opts = ReplayOptions {
        speed: args.speed,
        limit: args.limit,
        event: EventOptions {
            with_labels: args.with_labels,
            india: args.india,
        },
    };
    let stats = replay(&data.transactions, opts, sink.as_mut())?;
    eprintln!(
        "replayed {} events in {:.2}s ({:.0} events/s)",
        stats.sent,
        stats.elapsed.as_secs_f64(),
        stats.per_second()
    );
    Ok(())
}
