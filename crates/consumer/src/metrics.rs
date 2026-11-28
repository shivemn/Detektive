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
//!
//! The `detektive_graph_*` metrics describe the time-windowed graph: how much
//! it holds, how much it evicts, and where its watermark is. The
//! `detektive_score_*` metrics describe the model: what it scores, how long
//! that takes, and how often it alerts.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use axum::Router;
use axum::extract::State as AxumState;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::routing::get;
use prometheus::{
    Encoder, Gauge, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
    Opts, Registry, TextEncoder,
};
use rdkafka::ClientContext;
use rdkafka::consumer::{ConsumerContext, Rebalance};
use rdkafka::statistics::Statistics;
use tokio::net::TcpListener;
use tracing::info;

/// Fan counts are mostly 1 to 3, with rare hubs in the thousands.
const FAN_BUCKETS: [f64; 14] = [
    1.0, 2.0, 3.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 2000.0, 5000.0, 10000.0,
];

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
    pub watermark: IntGauge,
    pub behind_partition: IntCounterVec,
    pub graph_edges: IntGauge,
    pub graph_accounts: IntGauge,
    pub graph_bytes: IntGauge,
    pub graph_evicted: IntCounter,
    pub graph_too_late: IntCounter,
    pub receiver_fan_in: Histogram,
    pub sender_fan_out: Histogram,
    pub cycles: IntCounter,
    pub shortest_cycle: Histogram,
    pub cycle_truncated: IntCounter,
    pub live_cycles: IntGauge,
    pub scatter_gather: Histogram,
    pub scatter_gather_truncated: IntCounter,
    pub scored: IntCounter,
    pub alerts: IntCounter,
    pub alerts_published: IntCounter,
    pub score: Histogram,
    pub score_time: Histogram,
    pub explain_time: Histogram,
    pub threshold: Gauge,
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
                    "Time to handle one message: decode it, add it to the graph and score it",
                )
                .buckets(vec![
                    1e-6, 2.5e-6, 5e-6, 1e-5, 2.5e-5, 5e-5, 1e-4, 2.5e-4, 5e-4, 1e-3, 2.5e-3, 5e-3,
                    1e-2, 0.1,
                ]),
            )?,
            watermark: IntGauge::new(
                "detektive_consumer_watermark_seconds",
                "Event time every partition has reached (the slowest active partition's newest event)",
            )?,
            behind_partition: IntCounterVec::new(
                Opts::new(
                    "detektive_consumer_behind_partition_total",
                    "Events older than an event their own partition already delivered (expected: 0)",
                ),
                &["partition"],
            )?,
            graph_edges: IntGauge::new(
                "detektive_graph_edges",
                "Payments inside the graph window",
            )?,
            graph_accounts: IntGauge::new(
                "detektive_graph_accounts",
                "Accounts with at least one payment inside the window",
            )?,
            graph_bytes: IntGauge::new(
                "detektive_graph_memory_bytes",
                "Approximate memory held by the window's edges",
            )?,
            graph_evicted: IntCounter::new(
                "detektive_graph_evicted_total",
                "Payments evicted after leaving the window",
            )?,
            graph_too_late: IntCounter::new(
                "detektive_graph_too_late_total",
                "Payments already older than the window when they arrived, so not added",
            )?,
            receiver_fan_in: Histogram::with_opts(
                HistogramOpts::new(
                    "detektive_graph_receiver_fan_in",
                    "Distinct accounts that paid the receiver inside the window, per payment",
                )
                .buckets(FAN_BUCKETS.to_vec()),
            )?,
            sender_fan_out: Histogram::with_opts(
                HistogramOpts::new(
                    "detektive_graph_sender_fan_out",
                    "Distinct accounts the sender paid inside the window, per payment",
                )
                .buckets(FAN_BUCKETS.to_vec()),
            )?,
            cycles: IntCounter::new(
                "detektive_graph_cycles_total",
                "Temporal cycles closed by payments (distinct account sequences)",
            )?,
            shortest_cycle: Histogram::with_opts(
                HistogramOpts::new(
                    "detektive_graph_shortest_cycle",
                    "Payments in the shortest cycle, for each payment that closes one",
                )
                .buckets((2..=12).map(f64::from).collect()),
            )?,
            cycle_truncated: IntCounter::new(
                "detektive_graph_cycle_search_truncated_total",
                "Cycle searches stopped at their budget (there may have been more cycles)",
            )?,
            live_cycles: IntGauge::new(
                "detektive_graph_live_cycles",
                "Cycles found whose payments are all still inside the window",
            )?,
            scatter_gather: Histogram::with_opts(
                HistogramOpts::new(
                    "detektive_graph_scatter_gather_width",
                    "Accounts in the middle of the widest scatter-gather each payment is part of",
                )
                .buckets(vec![1.0, 2.0, 3.0, 5.0, 10.0, 20.0, 50.0]),
            )?,
            scatter_gather_truncated: IntCounter::new(
                "detektive_graph_scatter_gather_truncated_total",
                "Scatter-gather searches stopped at their budget (there may have been a wider one)",
            )?,
            scored: IntCounter::new(
                "detektive_score_payments_total",
                "Payments the model scored",
            )?,
            alerts: IntCounter::new(
                "detektive_score_alerts_total",
                "Payments whose score reached the alert threshold",
            )?,
            alerts_published: IntCounter::new(
                "detektive_score_alerts_published_total",
                "Alerts written to the alerts topic",
            )?,
            score: Histogram::with_opts(
                HistogramOpts::new(
                    "detektive_score",
                    "The model's score (probability of laundering), per payment",
                )
                .buckets(vec![
                    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.3, 0.5, 0.7, 0.9,
                    1.0,
                ]),
            )?,
            score_time: Histogram::with_opts(
                HistogramOpts::new(
                    "detektive_score_seconds",
                    "Time the model alone takes to score one payment",
                )
                .buckets(vec![
                    1e-6, 2.5e-6, 5e-6, 1e-5, 1.5e-5, 2e-5, 3e-5, 5e-5, 1e-4, 2.5e-4, 1e-3,
                ]),
            )?,
            explain_time: Histogram::with_opts(
                HistogramOpts::new(
                    "detektive_score_explain_seconds",
                    "Time to work out the reasons behind one alert",
                )
                .buckets(vec![
                    1e-4, 5e-4, 1e-3, 2e-3, 3e-3, 5e-3, 7.5e-3, 1e-2, 2.5e-2, 0.1,
                ]),
            )?,
            threshold: Gauge::new(
                "detektive_score_threshold",
                "The loaded model's alert threshold",
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
        m.registry.register(Box::new(m.watermark.clone()))?;
        m.registry.register(Box::new(m.behind_partition.clone()))?;
        m.registry.register(Box::new(m.graph_edges.clone()))?;
        m.registry.register(Box::new(m.graph_accounts.clone()))?;
        m.registry.register(Box::new(m.graph_bytes.clone()))?;
        m.registry.register(Box::new(m.graph_evicted.clone()))?;
        m.registry.register(Box::new(m.graph_too_late.clone()))?;
        m.registry.register(Box::new(m.receiver_fan_in.clone()))?;
        m.registry.register(Box::new(m.sender_fan_out.clone()))?;
        m.registry.register(Box::new(m.cycles.clone()))?;
        m.registry.register(Box::new(m.shortest_cycle.clone()))?;
        m.registry.register(Box::new(m.cycle_truncated.clone()))?;
        m.registry.register(Box::new(m.live_cycles.clone()))?;
        m.registry.register(Box::new(m.scatter_gather.clone()))?;
        m.registry
            .register(Box::new(m.scatter_gather_truncated.clone()))?;
        m.registry.register(Box::new(m.scored.clone()))?;
        m.registry.register(Box::new(m.alerts.clone()))?;
        m.registry.register(Box::new(m.alerts_published.clone()))?;
        m.registry.register(Box::new(m.score.clone()))?;
        m.registry.register(Box::new(m.score_time.clone()))?;
        m.registry.register(Box::new(m.explain_time.clone()))?;
        m.registry.register(Box::new(m.threshold.clone()))?;
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
/// source of per-partition lag) and hears about partition assignments.
pub struct MetricsContext {
    pub metrics: Arc<Metrics>,
    pub assignment: Arc<Assignment>,
}

/// The partitions this consumer owns, handed from librdkafka's rebalance
/// callback to the consume loop. The loop checks `take` on every message: one
/// atomic load, and the list is only copied when it changed.
#[derive(Default)]
pub struct Assignment {
    changed: AtomicBool,
    partitions: Mutex<Vec<u32>>,
}

impl Assignment {
    fn set(&self, partitions: Vec<u32>) {
        *self.partitions.lock().unwrap() = partitions;
        self.changed.store(true, Ordering::Release);
    }

    /// The new assignment, if it changed since the last call.
    pub fn take(&self) -> Option<Vec<u32>> {
        self.changed
            .swap(false, Ordering::Acquire)
            .then(|| self.partitions.lock().unwrap().clone())
    }
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
                let parts: Vec<u32> = list
                    .elements()
                    .iter()
                    .filter_map(|e| u32::try_from(e.partition()).ok())
                    .collect();
                info!(?parts, "partitions assigned");
                self.assignment.set(parts);
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
            "detektive_consumer_watermark_seconds 0",
            "detektive_graph_edges 0",
            "detektive_graph_evicted_total 0",
            "detektive_graph_too_late_total 0",
            "detektive_graph_cycles_total 0",
            "detektive_graph_live_cycles 0",
            "detektive_graph_shortest_cycle_bucket",
            "detektive_graph_scatter_gather_width_bucket",
            "detektive_graph_scatter_gather_truncated_total 0",
        ] {
            assert!(text.contains(name), "missing {name} in\n{text}");
        }
    }

    #[test]
    fn every_metric_the_dashboard_queries_is_exported() {
        let dashboard = include_str!("../../../observability/grafana/dashboards/detektive.json");
        let m = Metrics::new().unwrap();
        m.lag.with_label_values(&["0"]).set(0);
        m.behind_partition.with_label_values(&["0"]).inc_by(0);
        let exported: std::collections::HashSet<String> = m
            .render()
            .lines()
            .filter(|l| !l.starts_with('#'))
            .filter_map(|l| l.split(['{', ' ']).next())
            .map(str::to_owned)
            .collect();

        let mut queried = 0;
        for (start, _) in dashboard.match_indices("detektive_") {
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
            queried >= 15,
            "expected the dashboard to query the consumer's metrics"
        );
    }
}
