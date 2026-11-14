//! Body-free projections of owner facts, with separate retention/delivery ports.
//! No execution, admission, history authority, or global event bus is retained.
pub mod activity;
pub mod diagnostics;
mod facts;
pub(crate) mod hooks;
#[cfg(feature = "wasm")]
pub(crate) mod wasm;

pub use facts::{FailureClass, ObservationKind, Position};
pub(crate) use facts::{FailureFacts, Observation};
