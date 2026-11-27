//! The Rust scorer against LightGBM, on the fixture made by
//! scripts/make_score_fixture.py: same file in, same scores out.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use detektive_graph::{Engine, EngineConfig, Payment};
use detektive_replay::load;
use detektive_score::{Model, Row, onnx};

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn lightgbm_scores() -> HashMap<u32, f64> {
    let text = std::fs::read_to_string(fixtures().join("scores.csv")).unwrap();
    text.lines()
        .skip(1)
        .map(|line| {
            let (id, score) = line.split_once(',').unwrap();
            (id.parse().unwrap(), score.parse().unwrap())
        })
        .collect()
}

#[test]
fn every_payment_scores_as_lightgbm_scored_it() {
    let model = Model::load(&fixtures()).unwrap();
    assert_eq!(model.forest().trees(), 60);
    let expected = lightgbm_scores();
    let data = load::load_csv(&fixtures().join("sample.csv")).unwrap();
    assert_eq!(data.transactions.len(), expected.len());

    let mut engine = Engine::new(EngineConfig::default());
    let mut worst = 0f64;
    let mut flips = 0;
    for t in &data.transactions {
        engine.advance(t.ts);
        let payment = Payment {
            tx: t.tx_id,
            ts: t.ts,
            src: t.src,
            dst: t.dst,
            amount: detektive_replay::india::to_inr(t.amount_paid, t.paid_currency),
            format: t.format as u8,
        };
        let obs = engine.add(payment).expect("in time order nothing is late");
        let ours = model.score(&Row::from(t), &obs.features);
        let theirs = expected[&t.tx_id];
        worst = worst.max((ours - theirs).abs());
        // The threshold is itself the score of one validation payment, so
        // that payment sits exactly on it and may land either side of it.
        let clear = (theirs - model.threshold()).abs() > 1e-6;
        if clear && (ours >= model.threshold()) != (theirs >= model.threshold()) {
            flips += 1;
        }
    }
    // Both add up in f64. What is left is the file: ONNX stores each leaf's
    // weight as an f32, about 7 significant digits. A payment taking a
    // different path through any tree would move its score far more than this.
    assert!(worst < 1e-6, "largest difference from LightGBM: {worst:e}");
    assert_eq!(flips, 0);
}

#[test]
fn some_payments_alert_and_most_do_not() {
    let expected = lightgbm_scores();
    let model = Model::load(&fixtures()).unwrap();
    let alerts = expected
        .values()
        .filter(|&&s| s >= model.threshold())
        .count();
    assert!(alerts > 0 && alerts < expected.len() / 2, "{alerts} alerts");
}

#[test]
fn refuses_garbage_and_models_it_cannot_read() {
    assert!(onnx::decode(b"not a model").is_err());
    assert!(onnx::decode(&[]).is_err(), "an empty file has no graph");
}

#[test]
fn refuses_a_model_trained_on_other_features() {
    let dir = tempdir();
    std::fs::copy(fixtures().join("model.onnx"), dir.join("model.onnx")).unwrap();
    let card = std::fs::read_to_string(fixtures().join("model.json")).unwrap();
    // swap two feature names: same set, wrong order
    let swapped = card
        .replacen("\"hour\"", "\"@@\"", 1)
        .replacen("\"weekday\"", "\"hour\"", 1)
        .replacen("\"@@\"", "\"weekday\"", 1);
    std::fs::write(dir.join("model.json"), swapped).unwrap();
    let err = Model::load(&dir).unwrap_err().to_string();
    assert!(err.contains("expects features"), "{err}");
}

fn tempdir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("detektive-score-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
