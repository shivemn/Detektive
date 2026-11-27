//! Reads the `payments` topic, decodes each event, keeps running counts,
//! adds every new payment to the time-windowed graph (`detektive-graph`) and,
//! with a model loaded, scores it (`detektive-score`). A payment that reaches
//! the model's threshold is logged and written to an alerts topic.
//!
//! Delivery is at-least-once, built from three rules:
//!
//! 1. An offset is stored only after its message has been handled. librdkafka
//!    then commits stored offsets every second. A crash can repeat a few
//!    messages but never skip one.
//! 2. A message that cannot be decoded goes to a dead-letter topic with the
//!    reason attached, and only then is its offset stored. One bad message
//!    never stops the stream, and it is never silently dropped.
//! 3. Repeats are recognised by `tx_id` and skipped (see `state.rs`).
//!
//! Alerts follow the same rule as dead letters: the broker must confirm an
//! alert before the payment's offset is stored. After a crash an alert can
//! therefore be written twice, never lost; whoever reads the alerts topic
//! should key on `tx_id`.

pub mod bitset;
pub mod event;
pub mod metrics;
pub mod state;

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use detektive_graph::Observation;
use detektive_score::Model;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{BorrowedMessage, Header, Message, OwnedHeaders, Timestamp};
use rdkafka::producer::{FutureProducer, FutureRecord};
use tracing::{info, warn};

use crate::metrics::{Assignment, Metrics, MetricsContext};
use crate::state::{Outcome, Scored, State};

pub use crate::state::GraphSettings;

#[derive(Debug, Clone)]
pub struct Config {
    pub brokers: String,
    pub topic: String,
    pub group: String,
    /// Where undecodable messages go. `None` logs and skips them instead.
    pub dead_letter_topic: Option<String>,
    /// Stop after handling this many messages (benchmarks and tests).
    pub stop_after: Option<u64>,
    /// Stop if no message arrives for this long (benchmarks and tests).
    pub idle_timeout: Option<Duration>,
    pub report_every: Duration,
    pub graph: GraphSettings,
    /// Score payments with this model. `None` builds the graph only.
    pub model: Option<Model>,
    /// Where alerts go. `None` only logs them.
    pub alerts_topic: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Summary {
    pub processed: u64,
    pub duplicates: u64,
    pub bad: u64,
    pub dead_lettered: u64,
    pub out_of_order: u64,
    /// Largest event-time lateness seen, in seconds.
    pub max_lateness: i64,
    pub accounts: usize,
    /// Events older than something their own partition had already
    /// delivered. Expected to be 0.
    pub behind_partition: u64,
    /// Payments inside the graph window when the consumer stopped.
    pub window_edges: usize,
    pub evicted: u64,
    /// Payments already older than the window when they arrived.
    pub too_late: u64,
    /// Payments the model scored, and how many of them alerted.
    pub scored: u64,
    pub alerts: u64,
    /// From the first message to the last: the time spent actually consuming,
    /// without the group join before it or the idle wait after it.
    pub busy: Duration,
}

impl Summary {
    pub fn per_second(&self) -> f64 {
        let handled = self.processed + self.duplicates + self.bad;
        handled as f64 / self.busy.as_secs_f64().max(f64::EPSILON)
    }
}

/// Run until `shutdown` resolves, `stop_after` messages are handled, or the
/// stream is idle for `idle_timeout`. Updates `metrics` as it goes.
pub async fn run(
    cfg: Config,
    metrics: Arc<Metrics>,
    shutdown: impl Future<Output = ()>,
) -> Result<Summary> {
    let assignment = Arc::new(Assignment::default());
    let consumer: StreamConsumer<MetricsContext> = ClientConfig::new()
        .set("bootstrap.servers", &cfg.brokers)
        .set("group.id", &cfg.group)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "true")
        .set("enable.auto.offset.store", "false") // rule 1: we store offsets ourselves
        .set("auto.commit.interval.ms", "1000")
        // a member that dies without leaving loses its partitions after 6 s
        // (default 45 s); heartbeats must come at least 3 times per session
        .set("session.timeout.ms", "6000")
        .set("heartbeat.interval.ms", "2000")
        // librdkafka reports per-partition lag (and more) every 5 s
        .set("statistics.interval.ms", "5000")
        .create_with_context(MetricsContext {
            metrics: metrics.clone(),
            assignment: assignment.clone(),
        })
        .with_context(|| format!("connecting consumer to {}", cfg.brokers))?;
    consumer.subscribe(&[&cfg.topic]).context("subscribing")?;

    // one producer for dead letters and alerts, if either has a topic
    let producer: Option<FutureProducer> =
        if cfg.dead_letter_topic.is_some() || cfg.alerts_topic.is_some() {
            Some(
                ClientConfig::new()
                    .set("bootstrap.servers", &cfg.brokers)
                    .set("enable.idempotence", "true")
                    .create()
                    .context("creating producer for dead letters and alerts")?,
            )
        } else {
            None
        };

    info!(topic = %cfg.topic, group = %cfg.group, "consuming");
    let mut state = State::new(cfg.graph);
    if let Some(model) = cfg.model.clone() {
        info!(
            trees = model.forest().trees(),
            threshold = model.threshold(),
            trained = model.card().created_utc.as_deref().unwrap_or("unknown"),
            "scoring with model"
        );
        metrics.threshold.set(model.threshold());
        state = state.with_model(model);
    }
    let mut dead_lettered = 0u64;
    let mut progress = Progress::new();
    let mut first_message: Option<Instant> = None;
    let mut last_message = Instant::now();
    let mut report = tokio::time::interval(cfg.report_every);
    report.tick().await; // the first tick fires at once; skip it
    tokio::pin!(shutdown);

    loop {
        if cfg.stop_after.is_some_and(|n| state.handled() >= n) {
            break;
        }
        // The idle deadline is its own timer, measured from the last message.
        // (Wrapping `recv` in a timeout instead would restart the timer every
        // time a progress report interrupted the wait, so it never fired.)
        let idle = async {
            match cfg.idle_timeout {
                Some(limit) => tokio::time::sleep_until((last_message + limit).into()).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            _ = &mut shutdown => {
                info!("shutdown requested");
                break;
            }
            _ = idle => {
                // still waiting to be given partitions (joining the group) is
                // not idleness: restart the clock and keep waiting
                if consumer.assignment().map_or(true, |a| a.count() == 0) {
                    last_message = Instant::now();
                    continue;
                }
                info!("no messages for {:?}, stopping", cfg.idle_timeout.unwrap_or_default());
                break;
            }
            _ = report.tick() => progress.log(&state),
            received = consumer.recv() => {
                let message = received.context("receiving")?;
                last_message = Instant::now();
                first_message.get_or_insert(last_message);
                if let Some(latency) = end_to_end_latency(message.timestamp()) {
                    metrics.end_to_end_latency.observe(latency);
                }
                // partitions just assigned hold the watermark until they deliver
                if let Some(partitions) = assignment.take() {
                    state.assign(&partitions, last_message);
                }
                let partition = u32::try_from(message.partition()).unwrap_or(0);
                let before = state.graph().stats();
                let outcome = state.handle(partition, message.payload(), last_message);
                metrics.handle_time.observe(last_message.elapsed().as_secs_f64());
                match outcome {
                    Outcome::Processed { lateness, behind_partition, observation, scored, .. } => {
                        metrics.messages.with_label_values(&["processed"]).inc();
                        metrics.event_lateness.observe(lateness as f64);
                        if lateness > 0 {
                            metrics.out_of_order.inc();
                        }
                        if behind_partition > 0 {
                            metrics.behind_partition.with_label_values(&[&partition.to_string()]).inc();
                        }
                        if let (Some(s), Some(o)) = (scored, &observation) {
                            metrics.scored.inc();
                            metrics.score.observe(s.score);
                            metrics.score_time.observe(s.took.as_secs_f64());
                            if s.alert {
                                metrics.alerts.inc();
                                let threshold = state.model.as_ref().map_or(1.0, Model::threshold);
                                let alert = alert_json(&message, o, s, threshold);
                                warn!(tx = o.tx, score = format!("{:.3}", s.score), threshold = format!("{threshold:.3}"), "alert");
                                if let (Some(producer), Some(topic)) = (&producer, &cfg.alerts_topic) {
                                    publish_alert(producer, topic, o.tx, &alert).await?;
                                    metrics.alerts_published.inc();
                                }
                            }
                        }
                        if let Some(o) = observation {
                            let f = o.features;
                            metrics.receiver_fan_in.observe(f64::from(f.dst_fan_in));
                            metrics.sender_fan_out.observe(f64::from(f.src_fan_out));
                            if o.cycles.truncated {
                                metrics.cycle_truncated.inc();
                            }
                            let sg = o.scatter_gather;
                            metrics.scatter_gather.observe(f64::from(f.scatter_gather));
                            if sg.truncated {
                                metrics.scatter_gather_truncated.inc();
                            }
                            if let Some(fan) = sg.widest.filter(|w| w.width >= 5) {
                                info!(
                                    tx = o.tx,
                                    source = fan.source,
                                    target = fan.target,
                                    width = fan.width,
                                    "payment is part of a wide scatter-gather"
                                );
                            }
                            if f.cycles > 0 {
                                metrics.cycles.inc_by(u64::from(f.cycles));
                                metrics.shortest_cycle.observe(f64::from(f.shortest_cycle));
                                info!(
                                    tx = o.tx,
                                    cycles = f.cycles,
                                    shortest = f.shortest_cycle,
                                    ring = ?o.cycles.example.iter().map(|h| h.src).collect::<Vec<_>>(),
                                    "payment closes a cycle"
                                );
                            }
                        }
                        record_graph(&metrics, &state, before);
                    }
                    Outcome::Duplicate => metrics.messages.with_label_values(&["duplicate"]).inc(),
                    Outcome::Bad(reason) => {
                        metrics.messages.with_label_values(&["bad"]).inc();
                        let reason = reason.to_string();
                        match (&producer, &cfg.dead_letter_topic) {
                            (Some(producer), Some(topic)) => {
                                dead_letter(producer, topic, &message, &reason).await?;
                                dead_lettered += 1;
                                metrics.dead_lettered.inc();
                            }
                            _ => warn!(partition = message.partition(), offset = message.offset(), %reason, "skipping bad message"),
                        }
                    }
                }
                metrics.accounts.set(state.accounts() as i64);
                metrics.event_time.set(state.max_ts.unwrap_or_default());
                consumer.store_offset_from_message(&message).context("storing offset")?;
            }
        }
    }

    // commit whatever was handled since the last automatic commit
    match consumer.commit_consumer_state(CommitMode::Sync) {
        Ok(()) | Err(KafkaError::ConsumerCommit(RDKafkaErrorCode::NoOffset)) => {}
        Err(e) => return Err(e).context("final commit"),
    }
    // leave the group now, so its partitions move to another member at once
    // instead of after this one's session times out
    consumer.unsubscribe();
    progress.log(&state);

    let graph = state.graph().stats();
    Ok(Summary {
        behind_partition: state.behind_partition(),
        window_edges: graph.edges,
        evicted: graph.evicted,
        too_late: graph.too_late,
        scored: metrics.scored.get(),
        alerts: state.alerts,
        processed: state.processed,
        duplicates: state.duplicates,
        bad: state.bad,
        dead_lettered,
        out_of_order: state.out_of_order,
        max_lateness: state.max_lateness,
        accounts: state.accounts(),
        busy: first_message
            .map(|first| last_message - first)
            .unwrap_or_default(),
    })
}

/// Update the graph metrics after a payment went in. `before` is the graph
/// as it was just before, so the counters move by exactly what changed.
fn record_graph(metrics: &Metrics, state: &State, before: detektive_graph::Stats) {
    let after = state.graph().stats();
    metrics.graph_edges.set(after.edges as i64);
    metrics.graph_accounts.set(after.accounts as i64);
    metrics.graph_bytes.set(after.approx_bytes() as i64);
    metrics.graph_evicted.inc_by(after.evicted - before.evicted);
    metrics
        .graph_too_late
        .inc_by(after.too_late - before.too_late);
    metrics.live_cycles.set(state.engine.live_cycles() as i64);
    if let Some(w) = state.watermarks.current() {
        metrics.watermark.set(w);
    }
}

/// Seconds since the producer wrote the message, from the timestamp it set.
/// `None` if the message has no create time; clamped at 0 against clock skew.
fn end_to_end_latency(ts: Timestamp) -> Option<f64> {
    let Timestamp::CreateTime(created_ms) = ts else {
        return None;
    };
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_millis() as i64;
    Some((now_ms - created_ms).max(0) as f64 / 1000.0)
}

/// What an alert says: the payment, its score, and the graph facts behind it.
/// The facts are the features as computed, not an explanation of the score;
/// explaining the model is a later step.
fn alert_json(msg: &BorrowedMessage<'_>, o: &Observation, s: Scored, threshold: f64) -> String {
    let f = &o.features;
    let payment: serde_json::Value = msg
        .payload()
        .and_then(|p| serde_json::from_slice(p).ok())
        .unwrap_or(serde_json::Value::Null);
    serde_json::json!({
        "tx_id": o.tx,
        "score": s.score,
        "threshold": threshold,
        "payment": payment,
        "graph": {
            "pair_count": f.pair_count,
            "sender_fan_out": f.src_fan_out,
            "receiver_fan_in": f.dst_fan_in,
            "cycles": f.cycles,
            "shortest_cycle": f.shortest_cycle,
            "ring_repeats": f.ring_repeats,
            "scatter_gather": f.scatter_gather,
            "ring": o.cycles.example.iter().map(|h| h.src).collect::<Vec<_>>(),
        },
    })
    .to_string()
}

/// Write an alert, keyed by tx_id, and wait for the broker to confirm it.
async fn publish_alert(producer: &FutureProducer, topic: &str, tx: u32, alert: &str) -> Result<()> {
    let key = tx.to_string();
    producer
        .send(
            FutureRecord::to(topic).key(&key).payload(alert),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(e, _)| e)
        .context("writing to the alerts topic")?;
    Ok(())
}

/// Copy a bad message to the dead-letter topic, with where it came from and
/// why it failed in its headers. Waits for the broker to confirm, so the
/// offset is only stored once the message is safe.
async fn dead_letter(
    producer: &FutureProducer,
    topic: &str,
    msg: &BorrowedMessage<'_>,
    reason: &str,
) -> Result<()> {
    let partition = msg.partition().to_string();
    let offset = msg.offset().to_string();
    let headers = OwnedHeaders::new()
        .insert(Header {
            key: "error",
            value: Some(reason),
        })
        .insert(Header {
            key: "source.topic",
            value: Some(msg.topic()),
        })
        .insert(Header {
            key: "source.partition",
            value: Some(&partition),
        })
        .insert(Header {
            key: "source.offset",
            value: Some(&offset),
        });
    let mut record = FutureRecord::<[u8], [u8]>::to(topic).headers(headers);
    if let Some(key) = msg.key() {
        record = record.key(key);
    }
    if let Some(payload) = msg.payload() {
        record = record.payload(payload);
    }
    producer
        .send(record, Duration::from_secs(10))
        .await
        .map_err(|(e, _)| e)
        .context("writing to dead-letter topic")?;
    warn!(partition = msg.partition(), offset = msg.offset(), %reason, "sent bad message to dead-letter topic");
    Ok(())
}

/// Logs counts and the rate since the previous report, so a quiet stream
/// shows 0/s instead of a slowly falling average.
struct Progress {
    last_handled: u64,
    last_at: Instant,
}

impl Progress {
    fn new() -> Self {
        Self {
            last_handled: 0,
            last_at: Instant::now(),
        }
    }

    fn log(&mut self, state: &State) {
        let handled = state.handled();
        let secs = self.last_at.elapsed().as_secs_f64().max(f64::EPSILON);
        let rate = (handled - self.last_handled) as f64 / secs;
        self.last_handled = handled;
        self.last_at = Instant::now();
        info!(
            processed = state.processed,
            duplicates = state.duplicates,
            bad = state.bad,
            out_of_order = state.out_of_order,
            behind_partition = state.behind_partition(),
            window_edges = state.graph().stats().edges,
            accounts = state.accounts(),
            rate = format!("{rate:.0}/s"),
            "progress"
        );
    }
}
