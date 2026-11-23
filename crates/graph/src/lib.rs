//! The graph engine: a live, time-windowed graph of who paid whom.
//!
//! `watermark` decides how far event time has safely progressed, from one
//! clock per partition. `graph` keeps the payments inside a sliding window of
//! event time and evicts the rest, so memory stays bounded however long the
//! stream runs.

pub mod graph;
pub mod watermark;

pub use graph::{Half, Insert, Payment, Stats, WindowGraph};
pub use watermark::{Arrival, Watermarks};
