//! Provider-neutral model client; Session history never lives in this crate.

use crate::Content;
use crate::error::Error;
use microcrab_auditor::{Auditor, VariableView};
use microcrab_protocol::{Response, Usage};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;

/// Incremental model output. Tool arguments are fragments until the final response
/// has passed validation; consumers must never execute a delta as a tool call.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ModelEvent {
    ReasoningDelta {
        text: String,
    },
    TextDelta {
        text: String,
    },
    ToolCallDelta {
        index: u32,
        id: Option<String>,
        name: Option<String>,
        arguments: String,
    },
    Finished {
        usage: Usage,
    },
}

/// Object-safe model calls preserve the caller's auditor, variables and cancellation.
#[async_trait]
pub trait Client: Send + Sync + 'static {
    /// Session calls this only for items already admitted to Context, before the
    /// next provider request or immediately after an actual response/result append.
    /// The producer is the successful transport attempt, never a body/round guess.
    /// Display delivery failure must not revoke admission or retry a provider call.
    async fn observe_admitted(
        &self,
        _epoch: u64,
        _items: &[crate::ContentItem],
        _producer_call_id: Option<&str>,
    ) {
    }

    /// Exact runtime reservation for the last admitted item, a tool result.
    /// The assistant item and call slot refer to this same admitted prefix.
    /// This is display evidence only; it cannot reject or replay a tool result.
    async fn observe_tool_result_admitted(
        &self,
        epoch: u64,
        items: &[crate::ContentItem],
        _assistant_index: usize,
        _call_index: usize,
        _execution_id: u64,
        _terminal_status: Option<&str>,
    ) {
        self.observe_admitted(epoch, items, None).await;
    }

    /// Exact committed input occurrences from Session admission. Item indices refer
    /// to this epoch's admitted items, never text or request timing. Observation
    /// cannot reject committed input or authorize its replay.
    async fn observe_input_admitted(
        &self,
        _epoch: u64,
        _items: &[crate::ContentItem],
        _submissions: &[(usize, String)],
    ) {
    }

    /// The Session primary turn loop uses this port; auxiliary model calls keep
    /// using complete/stream and cannot become visible conversation producers.
    async fn stream_admitted(
        &self,
        content: &Content,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        cancellation: CancellationToken,
        events: Option<Sender<ModelEvent>>,
    ) -> Result<Response, Error> {
        self.stream(content, auditor, variables, cancellation, events)
            .await
    }

    async fn complete(
        &self,
        content: &Content,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        cancellation: CancellationToken,
    ) -> Result<Response, Error>;

    async fn stream(
        &self,
        content: &Content,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        cancellation: CancellationToken,
        _events: Option<Sender<ModelEvent>>,
    ) -> Result<Response, Error> {
        self.complete(content, auditor, variables, cancellation)
            .await
    }
}
