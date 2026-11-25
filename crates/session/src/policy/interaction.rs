//! Interaction transport and future-grant ports; Session policy owns settlement.
use super::*;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use crate::observation::diagnostics::{PermissionDecisionSource as Source, SafeDiagnosticReason};

/// The final denied interaction, not a provisional review suggestion.
#[derive(Debug)]
pub(super) struct Denial {
    pub(super) source: Source,
    pub(super) reason: SafeDiagnosticReason,
}

impl Denial {
    pub(super) fn unavailable() -> Self {
        Self::new(Source::Unavailable, "Permission interaction is unavailable.")
    }

    pub(super) fn invalid_reply() -> Self {
        Self::new(Source::Interaction, "Permission interaction returned an invalid decision.")
    }

    pub(super) fn from_reply(answer: &AskReply, submitted: Option<&Decision>, state: &crate::State) -> Self {
        match (answer.reason.as_str(), submitted) {
            ("review_auto", Some(decision)) if decision.source == "review" && decision.action == answer.action => {
                let safe = crate::runtime::execution::InvocationRedaction::from_snapshot(state.secret_snapshot())
                    .ok().map(|redaction| redaction.redact_text(decision.reason.trim()));
                match safe.as_deref().filter(|reason| !reason.is_empty()) {
                    Some(reason) => Self::new(Source::ActionReview, &format!("Action Review denied the operation: {reason}")),
                    None => Self::new(Source::ActionReview, "Action Review denied the operation."),
                }
            }
            ("timeout", _) => Self::new(Source::Timeout, "Permission expired without an Allow decision."),
            ("timeout_or_closed", _) => Self::new(Source::Interaction, "Permission was not granted before the interaction expired or closed."),
            (_, Some(decision)) if decision.source == "timeout" && decision.action == answer.action && decision.reason == answer.reason => {
                Self::new(Source::Timeout, "Permission expired without an Allow decision.")
            }
            ("executor_disconnected" | "session_closed", _) => Self::unavailable(),
            ("user_once", _) => Self::new(Source::User, "The user denied the operation."),
            _ => Self::new(Source::Interaction, "The permission interaction denied the operation."),
        }
    }

    fn new(source: Source, reason: &str) -> Self {
        Self { source, reason: SafeDiagnosticReason::new(reason) }
    }
}

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
