//! Replayer -> broker -> consumer, end to end, on librdkafka's in-process mock
//! cluster. The clients speak the real Kafka protocol; only the broker is a
//! stand-in, so these run anywhere `cargo test` does, CI included.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Headers, Message};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

use detektive_consumer::metrics::Metrics;
use detektive_consumer::{Config, GraphSettings, Summary, run};
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
            src_bank: i % 5,
            dst_bank: (i * 3) % 5,
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
    publish_to(brokers, TOPIC, txs);
}

fn publish_to(brokers: &str, topic: &str, txs: &[Transaction]) {
    let mut sink = KafkaSink::new(brokers, topic).unwrap();
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
        graph: Default::default(),
        model: None,
        alerts_topic: None,
    }
}

async fn consume(cfg: Config) -> Summary {
    run(
        cfg,
        Arc::new(Metrics::new().unwrap()),
        std::future::pending(),
    )
    .await
    .unwrap()
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
        graph: GraphSettings {
            window: Duration::from_secs(24 * 3600),
            ..GraphSettings::default()
        },
        ..config(&brokers, "once")
    })
    .await;

    assert_eq!(summary.processed, 3_000);
    assert_eq!((summary.duplicates, summary.bad), (0, 0));
    assert_eq!(summary.accounts, distinct_accounts(&txs));

    // three partitions, read at their own pace: each must still arrive in
    // order, and with the watermark on the slowest one nothing is refused
    assert_eq!(summary.behind_partition, 0);
    assert_eq!(summary.too_late, 0);
    // 50 hours of payments through a 24-hour window: every payment is either
    // still in it or was evicted from it
    assert!(summary.evicted > 0);
    assert_eq!(summary.window_edges as u64 + summary.evicted, 3_000);
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

#[tokio::test(flavor = "multi_thread")]
async fn stops_when_idle_even_with_frequent_progress_reports() {
    // regression: reports every 1 s used to restart a 3 s idle timer forever
    let (_cluster, brokers) = cluster();
    publish(&brokers, &transactions(100));

    let cfg = Config {
        idle_timeout: Some(Duration::from_secs(3)),
        report_every: Duration::from_secs(1),
        ..config(&brokers, "idle")
    };
    let summary = tokio::time::timeout(Duration::from_secs(30), consume(cfg))
        .await
        .expect("consumer never stopped after going idle");
    assert_eq!(summary.processed, 100);
    // busy time covers only the messages, not the join or the idle wait
    assert!(summary.busy < Duration::from_secs(2), "{summary:?}");
}

/// GET a path from a local HTTP server, the way Prometheus scrapes.
async fn http_get(addr: std::net::SocketAddr, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    response.split_once("\r\n\r\n").unwrap().1.to_owned()
}

fn metric(text: &str, name: &str) -> f64 {
    text.lines()
        .find_map(|l| l.strip_prefix(name).and_then(|rest| rest.strip_prefix(' ')))
        .unwrap_or_else(|| panic!("no {name} in\n{text}"))
        .trim()
        .parse()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_endpoint_reports_what_the_consumer_did() {
    let (_cluster, brokers) = cluster();
    publish(&brokers, &transactions(500));
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .unwrap();
    producer
        .send(
            BaseRecord::<[u8], [u8]>::to(TOPIC)
                .key(&b"k"[..])
                .payload(&b"broken"[..]),
        )
        .unwrap();
    producer.flush(Duration::from_secs(10)).unwrap();

    let metrics = Arc::new(Metrics::new().unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(detektive_consumer::metrics::serve(
        listener,
        metrics.clone(),
    ));
    assert_eq!(http_get(addr, "/health").await, "ok");

    // run past one 5 s statistics interval so per-partition lag gets reported
    let cfg = Config {
        idle_timeout: Some(Duration::from_secs(7)),
        ..config(&brokers, "metrics")
    };
    let summary = run(cfg, metrics, std::future::pending()).await.unwrap();
    let text = http_get(addr, "/metrics").await;

    assert_eq!(
        metric(
            &text,
            "detektive_consumer_messages_total{outcome=\"processed\"}"
        ),
        500.0
    );
    assert_eq!(
        metric(&text, "detektive_consumer_messages_total{outcome=\"bad\"}"),
        1.0
    );
    assert_eq!(metric(&text, "detektive_consumer_dead_lettered_total"), 1.0);
    assert_eq!(
        metric(&text, "detektive_consumer_accounts"),
        summary.accounts as f64
    );
    assert_eq!(
        metric(&text, "detektive_consumer_out_of_order_total"),
        summary.out_of_order as f64
    );
    assert_eq!(
        metric(&text, "detektive_consumer_event_lateness_seconds_count"),
        500.0
    );
    assert_eq!(
        metric(&text, "detektive_consumer_end_to_end_latency_seconds_count"),
        501.0
    );
    assert_eq!(
        metric(&text, "detektive_consumer_event_time_seconds"),
        (1_661_990_400 + 499 * 60) as f64
    );
    // 500 minutes of payments all fit in the 24-hour window
    assert_eq!(metric(&text, "detektive_graph_edges"), 500.0);
    assert_eq!(metric(&text, "detektive_graph_evicted_total"), 0.0);
    assert_eq!(summary.window_edges, 500);
    // one fan-in and one fan-out observation per payment
    assert_eq!(
        metric(&text, "detektive_graph_receiver_fan_in_count"),
        500.0
    );
    assert_eq!(metric(&text, "detektive_graph_sender_fan_out_count"), 500.0);
    assert_eq!(
        metric(&text, "detektive_graph_cycle_search_truncated_total"),
        0.0
    );
    let watermark = metric(&text, "detektive_consumer_watermark_seconds");
    assert!(watermark >= 1_661_990_400.0 && watermark <= (1_661_990_400 + 499 * 60) as f64);
    // everything was read, so every partition's lag is 0
    let lag: Vec<f64> = (0..3)
        .map(|p| {
            metric(
                &text,
                &format!("detektive_consumer_lag_messages{{partition=\"{p}\"}}"),
            )
        })
        .collect();
    assert_eq!(lag, vec![0.0, 0.0, 0.0], "{text}");
}

#[tokio::test(flavor = "multi_thread")]
async fn alerts_reach_the_alerts_topic_and_match_lightgbm() {
    // the scorer's fixture: a synthetic file, a model trained on it, and
    // LightGBM's score for every payment
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../score/tests/fixtures");
    let data = detektive_replay::load::load_csv(&fixtures.join("sample.csv")).unwrap();
    let model = detektive_score::Model::load(&fixtures).unwrap();
    let threshold = model.threshold();
    let lightgbm: Vec<(u32, f64)> = std::fs::read_to_string(fixtures.join("scores.csv"))
        .unwrap()
        .lines()
        .skip(1)
        .map(|l| {
            let (id, score) = l.split_once(',').unwrap();
            (id.parse().unwrap(), score.parse().unwrap())
        })
        .collect();

    // one partition, so payments arrive in time order and see exactly the
    // graph the batch tool saw
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("payments-one", 1, 1).unwrap();
    cluster.create_topic("alerts", 1, 1).unwrap();
    let brokers = cluster.bootstrap_servers();
    publish_to(&brokers, "payments-one", &data.transactions);

    let summary = consume(Config {
        topic: "payments-one".to_owned(),
        stop_after: Some(data.transactions.len() as u64),
        model: Some(model),
        alerts_topic: Some("alerts".to_owned()),
        ..config(&brokers, "scoring")
    })
    .await;
    assert_eq!(summary.scored, data.transactions.len() as u64);
    assert!(summary.alerts > 0);

    let reader: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "alerts-reader")
        .set("auto.offset.reset", "earliest")
        .create()
        .unwrap();
    reader.subscribe(&["alerts"]).unwrap();
    let mut alerted = HashSet::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        if let Some(Ok(m)) = reader.poll(Duration::from_millis(100)) {
            let alert: serde_json::Value = serde_json::from_slice(m.payload().unwrap()).unwrap();
            assert!(alert["score"].as_f64().unwrap() >= threshold);
            assert_eq!(alert["payment"]["tx_id"], alert["tx_id"]);
            alerted.insert(alert["tx_id"].as_u64().unwrap() as u32);
            if alerted.len() as u64 == summary.alerts {
                break;
            }
        }
    }
    assert_eq!(alerted.len() as u64, summary.alerts);

    // the same payments LightGBM would alert on, apart from any sitting
    // exactly on the threshold
    for (tx, score) in lightgbm {
        if (score - threshold).abs() > 1e-6 {
            assert_eq!(
                alerted.contains(&tx),
                score >= threshold,
                "payment {tx}, LightGBM score {score}"
            );
        }
    }
}
