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

/// A positive Unix millisecond timestamp representable by JavaScript `Date`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseCompletedAt(u64);

impl ResponseCompletedAt {
    pub fn new(unix_ms: u64) -> Option<Self> {
        (1..=8_640_000_000_000_000)
            .contains(&unix_ms)
            .then_some(Self(unix_ms))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

/// Display-only evidence for the exact successful transport attempt. A normalized
/// response is not yet a Session admission, and older clients can omit evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelAttempt {
    pub call_id: String,
    pub response_completed_at_ms: Option<ResponseCompletedAt>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResponseOutcome {
    pub response: Response,
    pub attempt: Option<ModelAttempt>,
}

impl From<Response> for ResponseOutcome {
    fn from(response: Response) -> Self {
        Self {
            response,
            attempt: None,
        }
    }
}

/// Object-safe model calls preserve the caller's auditor, variables and cancellation.
#[async_trait]
pub trait Client: Send + Sync + 'static {
    /// Maximum attempts for a fresh HTTPS tool image, including the URL and
    /// optional inline attempts. Zero means the route cannot do this safely.
    fn public_https_image_attempts(&self) -> u8 {
        0
    }

    /// A mutable router may return an immutable route for one authorized image.
    fn pin_public_https_image_route(&self) -> Option<std::sync::Arc<dyn Client>> {
        None
    }

    /// One actual provider attempt, with no retry policy wrapped around it.
    async fn stream_public_https_image_attempt(
        &self,
        content: &Content,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        cancellation: CancellationToken,
        events: Option<Sender<ModelEvent>>,
    ) -> Result<ResponseOutcome, Error> {
        if self.public_https_image_attempts() == 0 {
            return Err(Error::config(
                "selected provider does not support public HTTPS tool images",
            ));
        }
        self.stream_admitted_outcome(content, auditor, variables, cancellation, events)
            .await
    }

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

    /// Session certifies the exact Assistant index in this admitted prefix only
    /// after validation and append. This cannot reject or replay a response.
    async fn observe_response_admitted(
        &self,
        epoch: u64,
        items: &[crate::ContentItem],
        _assistant_index: usize,
        attempt: Option<&ModelAttempt>,
    ) {
        self.observe_admitted(
            epoch,
            items,
            attempt.map(|attempt| attempt.call_id.as_str()),
        )
        .await;
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

    /// Returns the attempt alongside its own normalized response, independent
    /// of audit/preview delivery. Wrappers on the primary path must forward this
    /// port to retain evidence; legacy implementations return an untimed response.
    async fn stream_admitted_outcome(
        &self,
        content: &Content,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        cancellation: CancellationToken,
        events: Option<Sender<ModelEvent>>,
    ) -> Result<ResponseOutcome, Error> {
        self.stream_admitted(content, auditor, variables, cancellation, events)
            .await
            .map(ResponseOutcome::from)
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
