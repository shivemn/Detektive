use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

use detektive_consumer::metrics::{self, Metrics};
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

    /// Address for Prometheus to scrape /metrics from ("none" to turn off)
    #[arg(long, default_value = "0.0.0.0:9898")]
    metrics_addr: String,
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
    let metrics = Arc::new(Metrics::new()?);
    if args.metrics_addr != "none" {
        let listener = tokio::net::TcpListener::bind(&args.metrics_addr)
            .await
            .with_context(|| format!("binding metrics server to {}", args.metrics_addr))?;
        tracing::info!(addr = %listener.local_addr()?, "serving /metrics and /health");
        tokio::spawn(metrics::serve(listener, metrics.clone()));
    }

    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    let summary = run(cfg, metrics, shutdown).await?;
    eprintln!(
        "processed {} | duplicates {} | bad {} (dead-lettered {}) | out of order {} (latest by {}s of event time) | accounts {} | {:.2}s busy, {:.0} msgs/s",
        summary.processed,
        summary.duplicates,
        summary.bad,
        summary.dead_lettered,
        summary.out_of_order,
        summary.max_lateness,
        summary.accounts,
        summary.busy.as_secs_f64(),
        summary.per_second()
    );
    Ok(())
}
