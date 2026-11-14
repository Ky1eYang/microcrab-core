//! Embedding-owned human confirmation for one more bounded consumption block.
//! This port grants only the block it names; tool authorization and safety
//! boundaries remain independent owners.
use crate::Error;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Which renewable consumption checkpoint asks the question. Every variant keeps
/// its limit; a `Continue` decision adds one `block` in the variant's unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfirmationKind {
    /// Consecutive unattended model requests; the block is additional rounds.
    Rounds,
    /// Tool calls produced by one model response; the block is additional calls.
    ToolCalls,
    /// Model-facing conversation bytes; the block is additional bytes.
    ContextBytes,
}

/// Exhaustion grants only another bounded block. Other budgets and caller
/// cancellation remain independent; unanswered callbacks have no elapsed cap.
#[derive(Clone)]
pub enum Exhaustion {
    Stop,
    Ask(Arc<dyn RoundLimitCallback>),
    Continue,
}
impl Exhaustion {
    pub async fn resolve(
        &self,
        request: RoundLimitRequest,
        cancellation: CancellationToken,
    ) -> Result<RoundLimitDecision, Error> {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let decision = match self {
            Self::Stop => RoundLimitDecision::Stop,
            Self::Continue => RoundLimitDecision::Continue,
            Self::Ask(callback) => {
                tokio::select! { biased;
                    _ = cancellation.cancelled() => return Err(Error::Cancelled),
                    result = callback.request(request, cancellation.clone()) => result?,
                }
            }
        };
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        Ok(decision)
    }
}

/// One consumption checkpoint question. `used` and `block` share the unit named by
/// `kind`; no field carries conversation content.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoundLimitRequest {
    pub session_id: String,
    pub epoch: u64,
    pub turn_id: u64,
    pub kind: ConfirmationKind,
    pub used: usize,
    pub block: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoundLimitDecision {
    Continue,
    Stop,
}

/// An unanswered question waits indefinitely until an explicit answer or caller
/// cancellation. Implementations must retire their pending interaction when this
/// future is dropped, and must not interpret a transport reconnection or an
/// ordinary permission grant as user confirmation.
#[async_trait]
pub trait RoundLimitCallback: Send + Sync {
    async fn request(
        &self,
        request: RoundLimitRequest,
        cancellation: CancellationToken,
    ) -> Result<RoundLimitDecision, Error>;
}
