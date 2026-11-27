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
use crate::row::{self, Codes, Row};

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
}

impl Model {
    /// Load `model.onnx` and `model.json` from a directory.
    pub fn load(dir: &Path) -> Result<Self> {
        let forest = onnx::read(&dir.join("model.onnx"))?;
        let path = dir.join("model.json");
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?;
        let card: Card =
            serde_json::from_str(&text).with_context(|| format!("reading {}", path.display()))?;
        Self::new(forest, card)
    }

    pub fn new(forest: Forest, card: Card) -> Result<Self> {
        let expected: Vec<&str> = row::NAMES
            .iter()
            .chain(Features::NAMES.iter())
            .copied()
            .collect();
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
