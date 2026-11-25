//! Object-safe intervention and nonblocking observation, independent of Session.
mod diff;
mod producer;
mod records;
#[cfg(test)]
mod tests;

pub use microcrab_protocol::{HookPayload, Moment, VariablesChange};
pub use producer::ChannelAuditor;
pub use records::{AuditBody, AuditOutcome, AuditRecord, ToolPresentation};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct AuditorError(pub String);
impl From<String> for AuditorError {
    fn from(value: String) -> Self {
        Self(value)
    }
}
impl From<&str> for AuditorError {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

/// Read-only live variable access. Applying returned changes belongs to the caller.
pub trait VariableView: Send + Sync {
    fn get(&self, name: &str) -> Option<serde_json::Value>;
    /// Explicit trusted read; legacy get remains model-visible only.
    fn get_all(&self, name: &str) -> Option<serde_json::Value> {
        self.get(name)
    }
    /// Opaque input attached only during its own admission; never an audit record.
    fn input(&self) -> Option<&microcrab_protocol::SessionInput> {
        None
    }
}
#[derive(Debug, Default, Clone, Copy)]
pub struct EmptyVariableView;
impl VariableView for EmptyVariableView {
    fn get(&self, _name: &str) -> Option<serde_json::Value> {
        None
    }
}

pub struct HookContext<'a> {
    pub epoch: u64,
    pub subagent: Option<String>,
    pub turn: Option<u64>,
    pub round: Option<u64>,
    pub call_id: Option<String>,
    /// Pre-rewrite body, present only for RequestPrepared. Never copied to records.
    pub request_body: Option<&'a str>,
    /// Actual serialized request byte budget, present only for RequestPrepared.
    /// Validate any replacement against this limit before committing variables.
    pub max_request_bytes: Option<usize>,
    pub variables: &'a dyn VariableView,
    pub payload: HookPayload,
}

impl HookContext<'_> {
    pub fn input(&self) -> Option<&microcrab_protocol::SessionInput> {
        self.variables.input()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Injection {
    #[serde(default)]
    pub variables: VariablesChange,
    /// Only RequestPrepared consumes a replacement body.
    #[serde(default)]
    pub request_body: Option<String>,
    /// Only ToolCall consumes replacement arguments. The Session validates an
    /// object and its serialized byte budget before committing any variables;
    /// permission and execution then see this value, while audit keeps the model's.
    #[serde(default)]
    pub tool_arguments: Option<serde_json::Value>,
}

/// Publish never blocks or fails a stage. Intervention is awaited and fallible;
/// the embedding stage owns its deadline, cancellation and atomic variable writes.
/// Session treats ToolResult as observation-only: Runtime publishes it, and the
/// Session-bound adapter rejects intervention before invoking the embedder hook.
/// Implementations may borrow a request-scoped observer. Owners that store an
/// auditor retain `Arc<dyn Auditor + 'static>` (the default object lifetime).
pub trait Auditor: Send + Sync {
    fn is_enabled(&self) -> bool;
    fn publish(&self, moment: Moment, body: AuditBody);
    fn intervene<'a>(
        &'a self,
        _moment: Moment,
        _context: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
        Box::pin(async { Ok(Injection::default()) })
    }
    fn flush(&self) -> BoxFuture<'_, Result<(), AuditorError>> {
        Box::pin(async { Ok(()) })
    }
}

impl<A: Auditor + ?Sized> Auditor for std::sync::Arc<A> {
    fn is_enabled(&self) -> bool {
        self.as_ref().is_enabled()
    }
    fn publish(&self, moment: Moment, body: AuditBody) {
        self.as_ref().publish(moment, body);
    }
    fn intervene<'a>(
        &'a self,
        moment: Moment,
        context: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
        self.as_ref().intervene(moment, context)
    }
    fn flush(&self) -> BoxFuture<'_, Result<(), AuditorError>> {
        self.as_ref().flush()
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopAuditor;
impl Auditor for NoopAuditor {
    fn is_enabled(&self) -> bool {
        false
    }
    fn publish(&self, _moment: Moment, _body: AuditBody) {}
}
