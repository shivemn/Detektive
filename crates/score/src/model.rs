//! A trained model as the stream runs it: the trees from `model.onnx` plus
//! `model.json`, which says what the trees expect and where to alert.
//!
//! Loading is strict. If the model was trained on features in a different
//! order, or on a feature this build does not compute, it is refused at
//! start-up rather than scoring every payment wrongly.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result, anyhow, ensure};
use detektive_graph::Features;
use detektive_replay::model::{Currency, PaymentFormat};
use serde::Deserialize;

use crate::forest::Forest;
use crate::onnx;
use crate::reasons;
use crate::row::{self, Codes, Row};
use crate::shap::Shap;

/// Row features, then graph features.
pub const WIDTH: usize = row::NAMES.len() + Features::NAMES.len();

/// The parts of `model.json` the scorer uses.
#[derive(Debug, Clone, Deserialize)]
pub struct Card {
    pub features: Vec<String>,
    pub categories: HashMap<String, HashMap<String, i64>>,
    pub threshold: f64,
    #[serde(default)]
    pub trees: Option<u64>,
    #[serde(default)]
    pub created_utc: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Model {
    forest: Forest,
    codes: Codes,
    card: Card,
    /// Present when the export wrote `cover.json` next to the model.
    shap: Option<Shap>,
}

/// One reason behind a score: a feature, what it was for this payment, and how
/// far it moved the score.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Reason {
    pub feature: &'static str,
    /// The feature's value for this payment, in words.
    pub text: String,
    /// Its share of the raw score, in log-odds: positive pushed towards an
    /// alert, negative away from one.
    pub log_odds: f64,
}

/// Why a payment scored what it did.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Explanation {
    /// Log-odds before anything about the payment is known.
    pub expected_log_odds: f64,
    /// Log-odds of this payment: the expected value plus every feature's share.
    pub log_odds: f64,
    /// The features that moved the score most, largest first.
    pub reasons: Vec<Reason>,
}

/// Every feature name, row features first, as the model reads them.
pub fn feature_names() -> impl Iterator<Item = &'static str> {
    row::NAMES.iter().chain(Features::NAMES.iter()).copied()
}

impl Model {
    /// Load `model.onnx` and `model.json` from a directory, and `cover.json`
    /// if it is there (without it, scores cannot be explained).
    pub fn load(dir: &Path) -> Result<Self> {
        let trees = onnx::read(&dir.join("model.onnx"))?;
        let path = dir.join("model.json");
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?;
        let card: Card =
            serde_json::from_str(&text).with_context(|| format!("reading {}", path.display()))?;
        let mut model = Self::new(trees.forest()?, card)?;
        let path = dir.join("cover.json");
        if path.exists() {
            #[derive(Deserialize)]
            struct Cover {
                cover: Vec<f64>,
            }
            let text = std::fs::read_to_string(&path)
                .with_context(|| format!("reading {}", path.display()))?;
            let cover: Cover = serde_json::from_str(&text)
                .with_context(|| format!("reading {}", path.display()))?;
            let shap = Shap::new(&trees.nodes, &trees.roots, &cover.cover, WIDTH, trees.base)
                .with_context(|| format!("matching {} to the trees", path.display()))?;
            model.shap = Some(shap);
        }
        Ok(model)
    }

    pub fn new(forest: Forest, card: Card) -> Result<Self> {
        let expected: Vec<&str> = feature_names().collect();
        ensure!(
            card.features == expected,
            "the model expects features {:?}, this build computes {:?}",
            card.features,
            expected
        );
        ensure!(
            forest.features() <= WIDTH,
            "the trees read feature {}, but there are only {WIDTH}",
            forest.features()
        );
        ensure!(
            (0.0..=1.0).contains(&card.threshold),
            "threshold {} is not a probability",
            card.threshold
        );
        if let Some(trees) = card.trees {
            ensure!(
                trees as usize == forest.trees(),
                "model.json says {trees} trees, model.onnx has {}",
                forest.trees()
            );
        }
        let table = |column: &str| -> Result<&HashMap<String, i64>> {
            card.categories
                .get(column)
                .ok_or_else(|| anyhow!("model.json has no codes for {column}"))
        };
        let code = |m: &HashMap<String, i64>, name: &str| m.get(name).map_or(-1.0, |&c| c as f32);
        let formats = table("payment_format")?;
        let paid = table("payment_currency")?;
        let received = table("receiving_currency")?;
        let codes = Codes {
            format: PaymentFormat::ALL.map(|f| code(formats, f.name())),
            paid_currency: Currency::ALL.map(|c| code(paid, c.name())),
            received_currency: Currency::ALL.map(|c| code(received, c.name())),
        };
        Ok(Self {
            forest,
            codes,
            card,
            shap: None,
        })
    }

    /// Whether this model can explain its scores.
    pub fn explains(&self) -> bool {
        self.shap.is_some()
    }

    /// Each feature's share of the raw score, in feature order, or `None`
    /// without `cover.json`.
    pub fn contributions(&self, row: &Row, graph: &Features) -> Option<Vec<f64>> {
        let shap = self.shap.as_ref()?;
        Some(shap.contributions(&self.features(row, graph)))
    }

    /// Log-odds before anything about a payment is known, or `None` without
    /// `cover.json`.
    pub fn expected_log_odds(&self) -> Option<f64> {
        self.shap.as_ref().map(Shap::expected)
    }

    /// The `n` features that moved this payment's score most, in words.
    /// `window` describes the graph window, e.g. "4 days".
    pub fn explain(
        &self,
        row: &Row,
        graph: &Features,
        n: usize,
        window: &str,
    ) -> Option<Explanation> {
        let shap = self.shap.as_ref()?;
        let phi = shap.contributions(&self.features(row, graph));
        let expected = shap.expected();
        let log_odds = expected + phi.iter().sum::<f64>();
        let names: Vec<&'static str> = feature_names().collect();
        let mut order: Vec<usize> = (0..phi.len()).collect();
        order.sort_by(|&a, &b| phi[b].abs().total_cmp(&phi[a].abs()));
        let reasons = order
            .into_iter()
            .take(n)
            .map(|i| Reason {
                feature: names[i],
                text: reasons::describe(i, row, graph, window),
                log_odds: phi[i],
            })
            .collect();
        Some(Explanation {
            expected_log_odds: expected,
            log_odds,
            reasons,
        })
    }

    /// The feature vector the trees see for this payment.
    pub fn features(&self, row: &Row, graph: &Features) -> [f32; WIDTH] {
        let mut x = [0f32; WIDTH];
        row::features(row, &self.codes, &mut x);
        for (slot, v) in x[row::NAMES.len()..].iter_mut().zip(graph.values()) {
            *slot = v as f32;
        }
        x
    }

    /// Probability that the payment is laundering, as the model sees it.
    pub fn score(&self, row: &Row, graph: &Features) -> f64 {
        self.forest.score(&self.features(row, graph))
    }

    /// The alert threshold chosen on validation when the model was trained.
    pub fn threshold(&self) -> f64 {
        self.card.threshold
    }

    pub fn forest(&self) -> &Forest {
        &self.forest
    }

    pub fn card(&self) -> &Card {
        &self.card
    }
}
