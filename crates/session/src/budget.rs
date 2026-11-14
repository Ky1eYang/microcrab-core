//! Shared budget decisions; resource owners retain accounting and effect witnesses.
mod check;
mod continuation;
mod limits;
mod violation;
pub use check::{check, reject};
pub use continuation::{
    ConfirmationKind, Exhaustion, RoundLimitCallback, RoundLimitDecision, RoundLimitRequest,
};
pub use limits::Limits;
pub use violation::{LimitError, LimitKind};
