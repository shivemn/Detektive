//! Replayer -> broker -> consumer, end to end, on librdkafka's in-process mock
//! cluster. The clients speak the real Kafka protocol; only the broker is a
//! stand-in, so these run anywhere `cargo test` does, CI included.

use std::collections::HashSet;
use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Headers, Message};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

use detektive_consumer::{Config, Summary, run};
use detektive_replay::event::EventOptions;
use detektive_replay::kafka::KafkaSink;
use detektive_replay::model::{Currency, PaymentFormat, Transaction};
use detektive_replay::pace::Speed;
use detektive_replay::{ReplayOptions, replay};

const TOPIC: &str = "payments";
const DEAD: &str = "payments-dead-letter";

fn cluster() -> (
    MockCluster<'static, rdkafka::producer::DefaultProducerContext>,
    String,
) {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic(TOPIC, 3, 1).unwrap();
    cluster.create_topic(DEAD, 1, 1).unwrap();
    let brokers = cluster.bootstrap_servers();
    (cluster, brokers)
}

fn transactions(n: u32) -> Vec<Transaction> {
    (0..n)
        .map(|i| Transaction {
            tx_id: i,
            ts: 1_661_990_400 + i64::from(i) * 60,
            src: i % 97,
            dst: (i * 7 + 3) % 131,
            amount_paid: 100.0 + f64::from(i),
            paid_currency: Currency::Euro,
            amount_received: 100.0 + f64::from(i),
            received_currency: Currency::Euro,
            format: if i % 3 == 0 {
                PaymentFormat::Ach
            } else {
                PaymentFormat::Cheque
            },
            is_laundering: false,
        })
        .collect()
}

fn publish(brokers: &str, txs: &[Transaction]) {
    let mut sink = KafkaSink::new(brokers, TOPIC).unwrap();
    let opts = ReplayOptions {
        speed: Speed::Max,
        limit: None,
        event: EventOptions::default(),
    };
    let stats = replay(txs, opts, &mut sink).unwrap();
    assert_eq!(sink.counter().delivered(), stats.sent as u64);
}

fn config(brokers: &str, group: &str) -> Config {
    Config {
        brokers: brokers.to_owned(),
        topic: TOPIC.to_owned(),
        group: group.to_owned(),
        dead_letter_topic: Some(DEAD.to_owned()),
        stop_after: None,
        idle_timeout: Some(Duration::from_secs(5)),
        report_every: Duration::from_secs(60),
    }
}

async fn consume(cfg: Config) -> Summary {
    run(cfg, std::future::pending()).await.unwrap()
}

fn distinct_accounts(txs: &[Transaction]) -> usize {
    txs.iter()
        .flat_map(|t| [t.src, t.dst])
        .collect::<HashSet<_>>()
        .len()
}

#[tokio::test(flavor = "multi_thread")]
async fn every_replayed_event_arrives_exactly_once() {
    let (_cluster, brokers) = cluster();
    let txs = transactions(3_000);
    publish(&brokers, &txs);

    let summary = consume(Config {
        stop_after: Some(3_000),
        ..config(&brokers, "once")
    })
    .await;

    assert_eq!(summary.processed, 3_000);
    assert_eq!((summary.duplicates, summary.bad), (0, 0));
    assert_eq!(summary.accounts, distinct_accounts(&txs));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_consumer_resumes_where_it_stopped() {
    let (_cluster, brokers) = cluster();
    publish(&brokers, &transactions(1_000));

    let first = consume(Config {
        stop_after: Some(400),
        ..config(&brokers, "resume")
    })
    .await;
    let second = consume(Config {
        idle_timeout: Some(Duration::from_secs(12)),
        ..config(&brokers, "resume")
    })
    .await;

    assert_eq!(first.processed, 400);
    // committed offsets mean the second run starts at message 401, not at 0
    assert_eq!(second.processed, 600);

    // a different group has its own offsets and reads everything again
    let other = consume(Config {
        stop_after: Some(1_000),
        ..config(&brokers, "another-group")
    })
    .await;
    assert_eq!(other.processed, 1_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_messages_go_to_the_dead_letter_topic_with_the_reason() {
    let (_cluster, brokers) = cluster();
    publish(&brokers, &transactions(50));

    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .unwrap();
    for payload in [&b"not json"[..], br#"{"tx_id":1}"#, b""] {
        producer
            .send(
                BaseRecord::<[u8], [u8]>::to(TOPIC)
                    .key(&b"k"[..])
                    .payload(payload),
            )
            .unwrap();
    }
    producer.flush(Duration::from_secs(10)).unwrap();

    let summary = consume(Config {
        stop_after: Some(53),
        ..config(&brokers, "dlq")
    })
    .await;
    assert_eq!(summary.processed, 50);
    assert_eq!((summary.bad, summary.dead_lettered), (3, 3));

    let reader: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "dlq-reader")
        .set("auto.offset.reset", "earliest")
        .create()
        .unwrap();
    reader.subscribe(&[DEAD]).unwrap();
    let mut reasons = Vec::new();
    for _ in 0..100 {
        if let Some(Ok(m)) = reader.poll(Duration::from_millis(200)) {
            let headers = m.headers().unwrap();
            let error = headers.iter().find(|h| h.key == "error").unwrap();
            reasons.push(String::from_utf8(error.value.unwrap().to_vec()).unwrap());
            assert!(headers.iter().any(|h| h.key == "source.offset"));
            if reasons.len() == 3 {
                break;
            }
        }
    }
    assert_eq!(reasons.len(), 3, "{reasons:?}");
    assert!(reasons.iter().any(|r| r == "empty payload"));
    assert!(
        reasons
            .iter()
            .filter(|r| r.starts_with("invalid event"))
            .count()
            == 2
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn replayed_duplicates_are_counted_once() {
    let (_cluster, brokers) = cluster();
    let txs = transactions(200);
    publish(&brokers, &txs);
    publish(&brokers, &txs[..50]); // a producer that crashed and re-sent

    let summary = consume(Config {
        stop_after: Some(250),
        ..config(&brokers, "dupes")
    })
    .await;
    assert_eq!(summary.processed, 200);
    assert_eq!(summary.duplicates, 50);
}

#[tokio::test(flavor = "multi_thread")]
async fn several_partitions_deliver_events_out_of_time_order() {
    let (_cluster, brokers) = cluster();
    publish(&brokers, &transactions(2_000));

    let summary = consume(Config {
        stop_after: Some(2_000),
        ..config(&brokers, "order")
    })
    .await;
    // 3 partitions interleave, so some events arrive after later ones: the
    // reason the graph engine will need event-time windows, not arrival order
    assert!(summary.out_of_order > 0, "{summary:?}");
}
