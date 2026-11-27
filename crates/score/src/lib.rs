//! Scoring payments with the trained model.
//!
//! The model is trained in Python and exported to ONNX (see
//! `ml/src/detektive_ml/models/export.py`). `onnx` reads the trees out of the
//! file, `forest` walks them, `row` computes the features a payment carries by
//! itself, and `model` puts these together with the graph engine's features.
//!
//! The trees are walked by this crate rather than by a general ONNX runtime.
//! A tree ensemble is a few hundred small trees, and walking them directly
//! takes microseconds. The pure-Rust runtime tried first (tract) took about
//! 30 ms per payment on the same model. See docs/scoring.md.

pub mod forest;
pub mod model;
pub mod onnx;
pub mod row;

pub use forest::Forest;
pub use model::{Card, Model, WIDTH};
pub use row::Row;
