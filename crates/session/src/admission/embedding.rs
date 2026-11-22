//! Trusted embedding decisions are separate from every serialized user input.
use crate::{Error, ImportantInput, SessionInput, VariablesChange};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

/// The actual consuming boundary, not the originally requested inbox mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputAdmissionBoundary {
    Turn,
    Steer,
}

/// Supplies embedding-owned data for the exact consuming input. Core never
/// infers notices, executor identity, or which changes should be announced.
#[async_trait]
pub trait InputAdmissionResolver: Send + Sync {
    /// Prepare a decision without publishing effects. Called after ordinary
    /// input validation and hooks, under Session's variable/admission fence.
    /// Do not reenter Session admission or hold an exclusive guard shared with
    /// other resolvers in the same steer batch through commit.
    async fn prepare(
        &self,
        boundary: InputAdmissionBoundary,
        input: &SessionInput,
        cancellation: CancellationToken,
    ) -> Result<PreparedInputAdmission, Error>;
}

/// Data staged with the input and an optional runtime-only publication callback.
#[derive(Default)]
pub struct PreparedInputAdmission {
    pub(crate) variables: VariablesChange,
    pub(crate) important: Option<ImportantInput>,
    commit: Option<Box<dyn FnOnce() + Send>>,
}

impl PreparedInputAdmission {
    pub fn new(variables: VariablesChange, important: Option<ImportantInput>) -> Self {
        Self {
            variables,
            important,
            commit: None,
        }
    }

    /// Publish after the in-memory admission transaction, before provider work.
    /// This callback must be infallible, nonblocking and not reenter Session. It
    /// is not a Journal ACK and is dropped without invocation on rejection.
    pub fn with_commit(mut self, commit: impl FnOnce() + Send + 'static) -> Self {
        self.commit = Some(Box::new(commit));
        self
    }

    pub(crate) fn commit(self) {
        if let Some(commit) = self.commit {
            commit();
        }
    }
}

pub(crate) async fn resolve(
    resolver: Option<&dyn InputAdmissionResolver>,
    boundary: InputAdmissionBoundary,
    input: &SessionInput,
    cancellation: &CancellationToken,
) -> Result<PreparedInputAdmission, Error> {
    let Some(resolver) = resolver else {
        return Ok(PreparedInputAdmission::default());
    };
    let prepared = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(Error::Cancelled),
        result = resolver.prepare(boundary, input, cancellation.child_token()) => result?,
    };
    super::variables::validate_change(&prepared.variables)?;
    Ok(prepared)
}
