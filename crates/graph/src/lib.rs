//! The graph engine: a live, time-windowed graph of who paid whom.
//!
//! `watermark` decides how far event time has safely progressed, from one
//! clock per partition.

pub mod watermark;

pub use watermark::{Arrival, Watermarks};
