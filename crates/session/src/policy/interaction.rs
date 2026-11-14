//! Interaction transport and future-grant ports; Session policy owns settlement.
use super::*;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AskAttribution {
    pub controller_id: u64,
    pub owning_session_id: String,
}

/// A live user transport. `settle` must atomically prefer an already accepted
/// explicit answer and deny a revoked executor, including at grace expiry.
#[async_trait]
pub trait PermissionInteraction: Send + Sync {
    fn deadline(&self) -> Instant;
    fn validate_dispatch(&self) -> Result<(), PermissionError> {
        Ok(())
    }
    async fn user_reply(&self) -> Result<AskReply, PermissionError>;
    async fn review(
        &self,
        result: &ReviewResult,
        received_at: Instant,
    ) -> Result<Instant, PermissionError>;
    async fn settle(&self, decision: &Decision) -> Result<AskReply, PermissionError>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FutureGrantProposal {
    pub session_id: String,
    pub controller_id: u64,
    pub revision: u64,
    pub call_id: String,
    pub agent: String,
    pub mode: String,
    pub tool: String,
    pub args: Vec<ArgumentPredicate>,
}

/// Durable targeted Session policy CAS. Failure never publishes a live grant.
#[async_trait]
pub trait FutureGrantPersistence: Send + Sync {
    async fn persist(
        &self,
        owning_session_id: &str,
        controller_id: u64,
        expected: &PolicySnapshot,
        next: &PolicySnapshot,
    ) -> Result<(), PermissionError>;
}

#[async_trait]
pub trait FutureGrantAdoption: Send + Sync {
    async fn adopt(&self) -> Result<PolicySnapshot, PermissionError>;
}

/// Compatibility adapters retain their old callback while using the same Session
/// arbiter. Lease-backed applications implement `open_interaction` instead.
pub(super) struct CallbackInteraction {
    pub adapter: Arc<dyn SessionPermission>,
    pub request: PermissionRequest,
    pub deadline: Instant,
}
#[async_trait]
impl PermissionInteraction for CallbackInteraction {
    fn deadline(&self) -> Instant {
        self.deadline
    }
    async fn user_reply(&self) -> Result<AskReply, PermissionError> {
        self.adapter.ask(&self.request).await
    }
    async fn review(&self, _: &ReviewResult, _: Instant) -> Result<Instant, PermissionError> {
        Ok(Instant::now() + std::time::Duration::from_secs(5))
    }
    async fn settle(&self, decision: &Decision) -> Result<AskReply, PermissionError> {
        Ok(AskReply {
            action: decision.action,
            reason: if decision.source == "review" {
                "review_auto".into()
            } else {
                decision.reason.clone()
            },
        })
    }
}
