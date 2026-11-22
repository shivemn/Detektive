//! Reads the `payments` topic, decodes each event and keeps running counts.
//! The graph engine will live inside this service from week 5.
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

pub mod bitset;
pub mod event;
pub mod metrics;
pub mod state;

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{BorrowedMessage, Header, Message, OwnedHeaders, Timestamp};
use rdkafka::producer::{FutureProducer, FutureRecord};
use tracing::{info, warn};

use crate::metrics::{Metrics, MetricsContext};
use crate::state::{Outcome, State};

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
        })
        .with_context(|| format!("connecting consumer to {}", cfg.brokers))?;
    consumer.subscribe(&[&cfg.topic]).context("subscribing")?;

    let dead_letters: Option<FutureProducer> = match &cfg.dead_letter_topic {
        Some(_) => Some(
            ClientConfig::new()
                .set("bootstrap.servers", &cfg.brokers)
                .set("enable.idempotence", "true")
                .create()
                .context("creating dead-letter producer")?,
        ),
        None => None,
    };

    info!(topic = %cfg.topic, group = %cfg.group, "consuming");
    let mut state = State::new();
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
                let outcome = state.handle(message.payload());
                metrics.handle_time.observe(last_message.elapsed().as_secs_f64());
                match outcome {
                    Outcome::Processed { lateness } => {
                        metrics.messages.with_label_values(&["processed"]).inc();
                        metrics.event_lateness.observe(lateness as f64);
                        if lateness > 0 {
                            metrics.out_of_order.inc();
                        }
                    }
                    Outcome::Duplicate => metrics.messages.with_label_values(&["duplicate"]).inc(),
                    Outcome::Bad(reason) => {
                        metrics.messages.with_label_values(&["bad"]).inc();
                        let reason = reason.to_string();
                        match (&dead_letters, &cfg.dead_letter_topic) {
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

    Ok(Summary {
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
            accounts = state.accounts(),
            rate = format!("{rate:.0}/s"),
            "progress"
        );
    }
}
