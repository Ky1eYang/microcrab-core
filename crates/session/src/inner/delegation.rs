//! Session owns child construction, parent control and acknowledged cleanup.
mod background;
mod control;
#[cfg(test)]
mod lifecycle_tests;
#[cfg(test)]
mod tests;
pub(super) use control::ChildRecord;
mod children;
use crate::runtime::{execution::process::ProcessControl, tool::ToolContext};
use crate::{DelegateExecutor, Error, Session, SessionInput};
use async_trait::async_trait;
pub use children::{Children, Completion};
use microcrab_auditor::Auditor;
use serde_json::json;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Usage remains with its external ledger owner; Session only projects totals.
pub trait DelegateUsage: Send + Sync {
    fn total(&self) -> microcrab_protocol::Usage;
}
/// The child has been built, but its lifecycle has not started. The shared owner
/// performs cancellable start/mode/submit and always acknowledges cleanup.
pub struct ChildSession {
    pub id: String,
    pub session: Session,
    pub mode: String,
    pub ledger: Arc<dyn DelegateUsage>,
    pub auditor: Arc<dyn Auditor>,
}

/// Builds an unstarted child. The returned future must be cancellation safe:
/// dropping it releases factory-owned partial resources, and any blocking worker
/// must own and drop an unpublished result if its receiver disappears. Do not
/// start a Session or publish a live child outside the returned ChildSession.
/// The delegate owner cancels assembly by dropping this future, then closes any
/// ChildSession that was already returned before reporting completion.
#[async_trait]
pub trait ChildFactory: Send + Sync {
    /// Capture route-sensitive assembly from the issuing operation's epoch before
    /// later asynchronous filesystem/audit work can observe another main Turn.
    async fn create_for_epoch(
        &self,
        caller_mode: Option<&str>,
        target: &str,
        requested_mode: Option<&str>,
        context_mode: &str,
        parent_processes: Arc<dyn ProcessControl>,
        _issuer_epoch: u64,
    ) -> Result<ChildSession, Error> {
        self.create(
            caller_mode,
            target,
            requested_mode,
            context_mode,
            parent_processes,
        )
        .await
    }
    async fn configure(&self, _child: &ChildSession) -> Result<(), Error> {
        Ok(())
    }
    /// Runs outside cancellable execution after close and audit flush settle.
    /// Display failures remain advisory and cannot change the child's outcome.
    async fn finalize(&self, _child: &ChildSession, _close_succeeded: bool) -> Result<(), Error> {
        Ok(())
    }

    async fn create(
        &self,
        caller_mode: Option<&str>,
        target: &str,
        requested_mode: Option<&str>,
        context_mode: &str,
        parent_processes: Arc<dyn ProcessControl>,
    ) -> Result<ChildSession, Error>;
}

#[derive(Clone)]
pub struct OwnedDelegateExecutor {
    children: Children,
    factory: Arc<dyn ChildFactory>,
    parent: Arc<
        std::sync::RwLock<
            Option<std::sync::Weak<dyn crate::runtime::tool::delegate::DelegateParent>>,
        >,
    >,
}

impl OwnedDelegateExecutor {
    pub fn new(children: Children, factory: Arc<dyn ChildFactory>) -> Self {
        Self {
            children,
            factory,
            parent: Arc::new(std::sync::RwLock::new(None)),
        }
    }
}

#[async_trait]
impl DelegateExecutor for OwnedDelegateExecutor {
    fn bind_parent(
        &self,
        parent: std::sync::Weak<dyn crate::runtime::tool::delegate::DelegateParent>,
    ) {
        *self.parent.write().unwrap_or_else(|e| e.into_inner()) = Some(parent);
    }
    async fn execute(
        &self,
        _target: &str,
        _input: &str,
        _mode: Option<&str>,
        _context_mode: &str,
        _timeout_ms: Option<u64>,
        _invocation: ToolContext,
    ) -> Result<String, Error> {
        Err(Error::tool(
            "delegation is Buffered; invoke through the owning Session and use explicit child follow-up/close controls",
        ))
    }
    async fn control(
        &self,
        action: &str,
        child_id: &str,
        target: &str,
        input: Option<&str>,
        _invocation: ToolContext,
    ) -> Result<crate::runtime::execution::ToolOutput, Error> {
        let parent = self
            .parent
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| Error::Closed)?;
        let preview = if action == "follow_up" || action == "resume" {
            json!({"child_id":child_id,"submitted":true,"reply_channel":"reply","closed":false})
        } else {
            json!({"child_id":child_id,"closed":true,"cleanup_acknowledged":true})
        };
        crate::budget::check(
            crate::LimitKind::ToolOutput,
            preview.to_string().len(),
            _invocation.state.snapshot().limits.max_tool_output_bytes,
        )?;
        let receipt = match action {
            "follow_up" => {
                let _outcome = parent
                    .submit_child(
                        child_id,
                        target,
                        input
                            .filter(|s| !s.trim().is_empty())
                            .ok_or_else(|| Error::tool("follow_up requires input"))?
                            .to_owned(),
                    )
                    .await?;
                json!({"child_id":child_id,"submitted":true,"reply_channel":"reply","closed":false})
            }
            "resume" => {
                let _outcome = parent.resume_child(child_id, target).await?;
                json!({"child_id":child_id,"submitted":true,"reply_channel":"reply","closed":false})
            }
            "close" => {
                parent.close_child(child_id, target).await?;
                json!({"child_id":child_id,"closed":true,"cleanup_acknowledged":true})
            }
            _ => return Err(Error::tool("unknown delegate action")),
        };
        Ok(crate::runtime::execution::ToolOutput::text(
            receipt.to_string(),
        ))
    }
    async fn execute_output(
        &self,
        target: &str,
        input: &str,
        mode: Option<&str>,
        context_mode: &str,
        timeout_ms: Option<u64>,
        invocation: ToolContext,
    ) -> Result<crate::runtime::execution::ToolOutput, Error> {
        background::start(
            self,
            target,
            input,
            mode,
            context_mode,
            timeout_ms,
            invocation,
        )
        .await
    }
}

// Execution snapshots are metadata only. Bound their count and the child outcome
// text with explicit omission/truncation feedback; never mutate durable results.
// Child identity, outcome counters and usage remain mandatory receipt metadata.
fn bound_delegate_receipt(mut value: serde_json::Value, limit: usize) -> Result<String, Error> {
    let original = value.clone();
    let count = value["executions"].as_array().map_or(0, Vec::len);
    let mut share = limit / count.saturating_add(2);
    let mut omitted = 0;
    loop {
        let encoded = value.to_string();
        if encoded.len() <= limit {
            return Ok(encoded);
        }
        if let Some(text) = original["outcome"]["content"].as_str() {
            let mut end = text.len().min(share);
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            value["outcome"]["content"] = text[..end].into();
            if end < text.len() {
                value["outcome"]["content_truncated"] = true.into();
            }
        }
        let encoded = value.to_string();
        if encoded.len() <= limit {
            return Ok(encoded);
        }
        if share > 0 {
            share /= 2;
            continue;
        }
        if value["executions"]
            .as_array_mut()
            .and_then(Vec::pop)
            .is_some()
        {
            omitted += 1;
            value["executions_omitted"] = omitted.into();
            continue;
        }
        return Err(Error::tool(
            "delegate result metadata exceeds output budget",
        ));
    }
}
