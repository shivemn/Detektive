//! The graph engine: a live, time-windowed graph of who paid whom.
//!
//! `watermark` decides how far event time has safely progressed, from one
//! clock per partition. `graph` keeps the payments inside a sliding window of
//! event time and evicts the rest, so memory stays bounded however long the
//! stream runs. `features` reads degree and fan counts for each payment off
//! the window, and `cycles` finds the temporal cycles a payment closes.
//! `engine` puts them together: add a payment, get its features.

pub mod cycles;
pub mod engine;
pub mod features;
pub mod graph;
pub mod watermark;

pub use cycles::{CycleFinder, Cycles, Found, Hop};
pub use engine::{Engine, EngineConfig, Observation};
pub use features::Features;
pub use graph::{Half, Insert, Payment, Stats, WindowGraph};
pub use watermark::{Arrival, Watermarks};
