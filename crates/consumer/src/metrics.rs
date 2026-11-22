//! Prometheus metrics for the consumer, served at `/metrics`.
//!
//! Three kinds of time show up here, and keeping them apart is the point:
//!
//! - **processing time**: how long handling one message takes (`handle_seconds`)
//! - **end-to-end latency**: wall-clock time from the producer writing a message
//!   to this service reading it (`end_to_end_latency_seconds`). Includes any
//!   backlog, so it grows when the consumer falls behind.
//! - **event-time lateness**: how far behind the newest event seen so far an
//!   arriving event's own timestamp is (`event_lateness_seconds`). This is about
//!   the data, not the machines: it tells the graph engine how long to keep a
//!   time window open for stragglers.

use std::sync::Arc;

use anyhow::Result;
use axum::Router;
use axum::extract::State as AxumState;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::routing::get;
use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry, TextEncoder,
};
use rdkafka::ClientContext;
use rdkafka::consumer::{ConsumerContext, Rebalance};
use rdkafka::statistics::Statistics;
use tokio::net::TcpListener;
use tracing::info;

pub struct Metrics {
    registry: Registry,
    pub messages: IntCounterVec,
    pub dead_lettered: IntCounter,
    pub out_of_order: IntCounter,
    pub accounts: IntGauge,
    pub event_time: IntGauge,
    pub lag: IntGaugeVec,
    pub end_to_end_latency: Histogram,
    pub event_lateness: Histogram,
    pub handle_time: Histogram,
}

impl Metrics {
    pub fn new() -> Result<Self> {
        let registry = Registry::new();
        let m = Self {
            messages: IntCounterVec::new(
                Opts::new(
                    "detektive_consumer_messages_total",
                    "Messages handled, by outcome",
                ),
                &["outcome"],
            )?,
            dead_lettered: IntCounter::new(
                "detektive_consumer_dead_lettered_total",
                "Messages written to the dead-letter topic",
            )?,
            out_of_order: IntCounter::new(
                "detektive_consumer_out_of_order_total",
                "Events older than the newest event already seen",
            )?,
            accounts: IntGauge::new("detektive_consumer_accounts", "Distinct accounts seen")?,
            event_time: IntGauge::new(
                "detektive_consumer_event_time_seconds",
                "Newest event timestamp seen (seconds since the epoch): the stream's clock",
            )?,
            lag: IntGaugeVec::new(
                Opts::new(
                    "detektive_consumer_lag_messages",
                    "Messages not yet read, per partition",
                ),
                &["partition"],
            )?,
            end_to_end_latency: Histogram::with_opts(
                HistogramOpts::new(
                    "detektive_consumer_end_to_end_latency_seconds",
                    "Wall-clock time from the producer writing a message to it being handled",
                )
                .buckets(vec![
                    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                    30.0, 60.0, 120.0, 300.0,
                ]),
            )?,
            event_lateness: Histogram::with_opts(
                HistogramOpts::new(
                    "detektive_consumer_event_lateness_seconds",
                    "How far an event's own time is behind the newest event seen (0 when in order)",
                )
                .buckets(vec![
                    0.0, 60.0, 300.0, 900.0, 1800.0, 3600.0, 7200.0, 14400.0, 28800.0, 43200.0,
                    86400.0, 172800.0,
                ]),
            )?,
            handle_time: Histogram::with_opts(
                HistogramOpts::new(
                    "detektive_consumer_handle_seconds",
                    "Time to decode and count one message",
                )
                .buckets(vec![
                    1e-6, 2.5e-6, 5e-6, 1e-5, 2.5e-5, 5e-5, 1e-4, 2.5e-4, 1e-3, 1e-2,
                ]),
            )?,
            registry,
        };
        m.registry.register(Box::new(m.messages.clone()))?;
        m.registry.register(Box::new(m.dead_lettered.clone()))?;
        m.registry.register(Box::new(m.out_of_order.clone()))?;
        m.registry.register(Box::new(m.accounts.clone()))?;
        m.registry.register(Box::new(m.event_time.clone()))?;
        m.registry.register(Box::new(m.lag.clone()))?;
        m.registry
            .register(Box::new(m.end_to_end_latency.clone()))?;
        m.registry.register(Box::new(m.event_lateness.clone()))?;
        m.registry.register(Box::new(m.handle_time.clone()))?;
        // make every outcome visible from the start, at 0
        for outcome in ["processed", "duplicate", "bad"] {
            m.messages.with_label_values(&[outcome]);
        }
        Ok(m)
    }

    /// Current values in Prometheus text format.
    pub fn render(&self) -> String {
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut buf)
            .expect("text encoding cannot fail");
        String::from_utf8(buf).expect("prometheus text is UTF-8")
    }
}

/// Plugs into librdkafka: receives its statistics every few seconds (the
/// source of per-partition lag) and logs partition assignments.
pub struct MetricsContext {
    pub metrics: Arc<Metrics>,
}

impl ClientContext for MetricsContext {
    fn stats(&self, stats: Statistics) {
        self.metrics.lag.reset();
        for topic in stats.topics.values() {
            for (id, p) in &topic.partitions {
                // -1 is librdkafka's internal "unassigned" partition; a lag of
                // -1 means not known yet
                if *id >= 0 && p.consumer_lag >= 0 {
                    self.metrics
                        .lag
                        .with_label_values(&[&id.to_string()])
                        .set(p.consumer_lag);
                }
            }
        }
    }
}

impl ConsumerContext for MetricsContext {
    fn post_rebalance(&self, _: &rdkafka::consumer::BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(list) => {
                let parts: Vec<i32> = list.elements().iter().map(|e| e.partition()).collect();
                info!(?parts, "partitions assigned");
            }
            Rebalance::Revoke(_) => info!("partitions revoked"),
            Rebalance::Error(e) => tracing::warn!(%e, "rebalance error"),
        }
    }
}

/// Serve `/metrics` and `/health` until the process ends.
pub async fn serve(listener: TcpListener, metrics: Arc<Metrics>) -> Result<()> {
    let app = Router::new()
        .route("/metrics", get(render_metrics))
        .route("/health", get(|| async { "ok" }))
        .with_state(metrics);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn render_metrics(AxumState(metrics): AxumState<Arc<Metrics>>) -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "text/plain; version=0.0.4")],
        metrics.render(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_every_metric_name() {
        let m = Metrics::new().unwrap();
        m.messages.with_label_values(&["processed"]).inc();
        m.lag.with_label_values(&["0"]).set(5);
        m.end_to_end_latency.observe(0.02);
        let text = m.render();
        for name in [
            "detektive_consumer_messages_total{outcome=\"processed\"} 1",
            "detektive_consumer_messages_total{outcome=\"bad\"} 0",
            "detektive_consumer_lag_messages{partition=\"0\"} 5",
            "detektive_consumer_end_to_end_latency_seconds_bucket",
            "detektive_consumer_event_lateness_seconds_count 0",
            "detektive_consumer_handle_seconds_sum",
            "detektive_consumer_dead_lettered_total 0",
            "detektive_consumer_event_time_seconds 0",
        ] {
            assert!(text.contains(name), "missing {name} in\n{text}");
        }
    }

    #[test]
    fn every_metric_the_dashboard_queries_is_exported() {
        let dashboard = include_str!("../../../observability/grafana/dashboards/detektive.json");
        let m = Metrics::new().unwrap();
        m.lag.with_label_values(&["0"]).set(0);
        let exported: std::collections::HashSet<String> = m
            .render()
            .lines()
            .filter(|l| !l.starts_with('#'))
            .filter_map(|l| l.split(['{', ' ']).next())
            .map(str::to_owned)
            .collect();

        let mut queried = 0;
        for (start, _) in dashboard.match_indices("detektive_consumer_") {
            let name: String = dashboard[start..]
                .chars()
                .take_while(|c| c.is_ascii_lowercase() || *c == '_')
                .collect();
            assert!(
                exported.contains(&name),
                "dashboard queries {name}, which is not exported"
            );
            queried += 1;
        }
        assert!(
            queried >= 10,
            "expected the dashboard to query the consumer's metrics"
        );
    }
}
