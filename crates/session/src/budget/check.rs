//! One inclusive comparison for owner-measured budgets. Accounting and checked
//! arithmetic remain with the owner; rejection never implies absence of effects.
use super::{LimitError, LimitKind};
use crate::Error;

pub fn check(kind: LimitKind, required: usize, budget: usize) -> Result<(), Error> {
    if required > budget {
        // Physical capacity is the one boundary that still rejects: waiting forever
        // would deadlock the owner. Record the rejection so it is auditable.
        if matches!(
            kind,
            LimitKind::Processes
                | LimitKind::ExecutionBuffers
                | LimitKind::PendingExecutions
                | LimitKind::ExecutionHistory
        ) {
            tracing::warn!(
                limit = kind.as_str(),
                required,
                budget,
                "resource capacity rejected an admission"
            );
        }
        let limit = LimitError::new(kind);
        let limit = match kind {
            LimitKind::Rounds
            | LimitKind::PendingExecutions
            | LimitKind::ExecutionHistory
            | LimitKind::ExecutionBuffers
            | LimitKind::Processes
            | LimitKind::CachePoints
            | LimitKind::AttachmentCount
            | LimitKind::ImagePixels
            | LimitKind::ImageSide
            | LimitKind::InboxEntries
            | LimitKind::ReferenceCount
            | LimitKind::HostOperations => limit.counts(required, budget),
            LimitKind::ToolCalls => limit.counts(required, budget).calls(required),
            LimitKind::OperationDeadline | LimitKind::TurnDeadline | LimitKind::Arithmetic => limit,
            _ => limit.bytes(required, budget),
        };
        Err(Error::Limit(limit))
    } else {
        Ok(())
    }
}

/// Preserve an owner's legacy diagnostic when exact numeric facts are unavailable.
/// Public transports must project only sanitized typed facts.
pub fn reject(kind: LimitKind, label: impl Into<String>) -> Error {
    Error::Limit(LimitError::new(kind).with_label(label))
}
