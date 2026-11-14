//! Identity-free lifecycle facts shared by invocations and Host producers.
//! These observations never establish ownership or replace either public DTO.
use super::{
    ExecutionSnapshot, ExecutionStatus,
    process::{SessionProcessState, SessionProcessStatus},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionKind {
    Invocation,
    Process,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StatusProjection {
    pub kind: ExecutionKind,
    pub status: ExecutionStatus,
    pub output_closed: bool,
    pub truncated: bool,
}
impl StatusProjection {
    pub fn is_pending(self) -> bool {
        self.status.is_pending()
    }
}
impl ExecutionSnapshot {
    pub fn status_projection(&self) -> StatusProjection {
        StatusProjection {
            kind: ExecutionKind::Invocation,
            status: self.status,
            output_closed: self.output.as_ref().is_none_or(|output| output.closed),
            truncated: self
                .output
                .as_ref()
                .is_some_and(|output| output.start_cursor > 0),
        }
    }
}
impl SessionProcessStatus {
    pub fn status_projection(&self) -> StatusProjection {
        StatusProjection {
            kind: ExecutionKind::Process,
            status: match &self.state {
                SessionProcessState::Running => ExecutionStatus::Running,
                SessionProcessState::Exited { code: 0 } => ExecutionStatus::Succeeded,
                SessionProcessState::Exited { .. } | SessionProcessState::Failed { .. } => {
                    ExecutionStatus::Failed
                }
                SessionProcessState::Killed => ExecutionStatus::Cancelled,
            },
            output_closed: self.closed,
            truncated: self.truncated,
        }
    }
}
