use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::EnvFilter;

use detektive_consumer::{Config, run};

/// Consume the payments stream from Redpanda and keep running counts.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// Redpanda/Kafka bootstrap servers
    #[arg(long, default_value = "localhost:19092")]
    brokers: String,

    #[arg(long, default_value = "payments")]
    topic: String,

    /// Consumer group; offsets are tracked per group
    #[arg(long, default_value = "detektive-consumer")]
    group: String,

    /// Topic for messages that cannot be decoded ("none" to only log them)
    #[arg(long, default_value = "payments-dead-letter")]
    dead_letter_topic: String,

    /// Stop after this many messages
    #[arg(long)]
    stop_after: Option<u64>,

    /// Stop after this many seconds without a message
    #[arg(long)]
    idle_secs: Option<u64>,

    /// Seconds between progress lines
    #[arg(long, default_value_t = 5)]
    report_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let args = Args::parse();

    let cfg = Config {
        brokers: args.brokers,
        topic: args.topic,
        group: args.group,
        dead_letter_topic: (args.dead_letter_topic != "none").then_some(args.dead_letter_topic),
        stop_after: args.stop_after,
        idle_timeout: args.idle_secs.map(Duration::from_secs),
        report_every: Duration::from_secs(args.report_secs),
    };
    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    let summary = run(cfg, shutdown).await?;
    eprintln!(
        "processed {} | duplicates {} | bad {} (dead-lettered {}) | out of order {} | accounts {} | {:.1}s, {:.0} msgs/s",
        summary.processed,
        summary.duplicates,
        summary.bad,
        summary.dead_lettered,
        summary.out_of_order,
        summary.accounts,
        summary.elapsed.as_secs_f64(),
        summary.per_second()
    );
    Ok(())
}
