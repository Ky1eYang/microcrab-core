//! Typed tool outcomes and execution observations, never recovery authority.
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ExecutionId(pub u64);
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolFailureKind {
    Failed,
    Timeout,
    Cancelled,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolFailure {
    pub kind: ToolFailureKind,
    pub code: String,
    pub message: String,
    pub retryable: bool,
}
impl std::fmt::Display for ToolFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for ToolFailure {}
impl ToolFailure {
    const MAX_CODE_BYTES: usize = 128;
    const MAX_MESSAGE_BYTES: usize = 4096;

    /// Largest valid producer failure envelope. NUL bytes realize JSON's maximum
    /// six-byte escaping per source byte; false and cancelled are the widest
    /// boolean and closed failure-kind representations in this envelope.
    pub(crate) fn max_result_bytes() -> usize {
        Self::result_byte_bounds().0
    }
    pub(crate) fn max_context_result_bytes() -> usize {
        Self::result_byte_bounds().1
    }
    fn result_byte_bounds() -> (usize, usize) {
        static MAX: std::sync::OnceLock<(usize, usize)> = std::sync::OnceLock::new();
        *MAX.get_or_init(|| {
            let envelope = serde_json::json!({"content":"", "failure":Self {
                kind: ToolFailureKind::Cancelled,
                code: "\0".repeat(Self::MAX_CODE_BYTES),
                message: "\0".repeat(Self::MAX_MESSAGE_BYTES),
                retryable: false,
            }})
            .to_string();
            // Each NUL becomes \\u0000 in the inner JSON and \\\\u0000
            // in the outer string: seven bytes, rather than six times six.
            let context_bytes = serde_json::json!(&envelope).to_string().len() - 2;
            (envelope.len(), context_bytes)
        })
    }
    pub fn failed(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: ToolFailureKind::Failed,
            code: code.into(),
            message: message.into(),
            retryable: false,
        }
    }
    pub(crate) fn redacted(
        mut self,
        redaction: &super::InvocationRedaction,
    ) -> Result<Self, crate::Error> {
        self.validate()?;
        self.code = redaction.redact_text(&self.code);
        self.message = redaction.redact_text(&self.message);
        for (text, max) in [
            (&mut self.code, Self::MAX_CODE_BYTES),
            (&mut self.message, Self::MAX_MESSAGE_BYTES),
        ] {
            if text.len() > max {
                let mut boundary = max;
                while !text.is_char_boundary(boundary) {
                    boundary -= 1;
                }
                text.truncate(boundary);
            }
        }
        Ok(self)
    }
    pub(crate) fn validate(&self) -> Result<(), crate::Error> {
        if self.code.is_empty()
            || self.code.len() > Self::MAX_CODE_BYTES
            || self.message.len() > Self::MAX_MESSAGE_BYTES
        {
            return Err(crate::Error::tool("invalid tool-reported failure bounds"));
        }
        Ok(())
    }
}
/// Validated at the runtime boundary before admission; never a display DTO.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaAttachment {
    pub media_type: String,
    pub data: Vec<u8>,
}
impl std::fmt::Debug for MediaAttachment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MediaAttachment")
            .field("media_type", &self.media_type)
            .field("byte_len", &self.data.len())
            .finish()
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalOutput {
    pub content: String,
    pub failure: Option<ToolFailure>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<MediaAttachment>,
}
/// Typed completion travels to Session admission; it is not an execution snapshot.
#[derive(Debug)]
pub(crate) struct ResolvedOutput {
    pub content: String,
    pub attachments: Vec<MediaAttachment>,
}
impl ResolvedOutput {
    pub(crate) fn text(content: String) -> Self {
        Self {
            content,
            attachments: Vec::new(),
        }
    }
    /// Display/audit projection carries only encoded-image metadata, never bytes.
    pub(crate) fn observation_text(
        content: &str,
        attachments: &[MediaAttachment],
    ) -> Result<String, crate::Error> {
        if attachments.is_empty() {
            return Ok(content.to_owned());
        }
        let images = attachments.iter().map(|attachment| {
            let metadata = crate::admission::image::validate_image(&attachment.media_type, &attachment.data)?;
            Ok(serde_json::json!({"media_type": attachment.media_type, "width": metadata.width, "height": metadata.height, "byte_len": attachment.data.len()}))
        }).collect::<Result<Vec<_>, crate::Error>>()?;
        Ok(serde_json::json!({"content":content,"images":images}).to_string())
    }

    pub(crate) fn into_text(self) -> Result<String, crate::Error> {
        if !self.attachments.is_empty() {
            return Err(crate::Error::tool(
                "image output requires typed Session admission",
            ));
        }
        Ok(self.content)
    }
}
#[derive(Debug)]
pub enum ToolOutput {
    Final(FinalOutput),
    Buffered(super::BufferedOutput),
}
impl ToolOutput {
    pub fn text(content: impl Into<String>) -> Self {
        Self::Final(FinalOutput {
            content: content.into(),
            failure: None,
            attachments: Vec::new(),
        })
    }
    pub fn failed(content: impl Into<String>, failure: ToolFailure) -> Self {
        Self::Final(FinalOutput {
            content: content.into(),
            failure: Some(failure),
            attachments: Vec::new(),
        })
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    Running,
    Waiting,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
    Interrupted,
}
impl ExecutionStatus {
    pub fn is_pending(self) -> bool {
        matches!(self, Self::Running | Self::Waiting | Self::Cancelling)
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputOptions {
    pub channels: Vec<String>,
}
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionCapabilities {
    pub input: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputMetadata {
    pub channels: Vec<String>,
    pub bytes: u64,
    pub start_cursor: u64,
    pub end_cursor: u64,
    pub closed: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputChunk {
    pub channel: Option<String>,
    pub content: String,
    pub cursor: u64,
    pub next_cursor: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputPage {
    pub execution_id: ExecutionId,
    pub chunks: Vec<OutputChunk>,
    pub cursor: u64,
    pub next_cursor: u64,
    pub end_cursor: u64,
    pub start_cursor: u64,
    pub closed: bool,
    pub revision: u64,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionDelivery {
    pub exposed: bool,
    pub released: bool,
}
/// Whether the Session still owns a buffered output source. Final inline text is
/// inspected separately and does not imply a retained buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputAvailability {
    None,
    Retained,
    Released,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionContentField {
    Arguments,
    Result,
}
/// A nonconsuming UTF-8 page. This is display content; reading never consumes or releases output.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionContentPage {
    pub session_id: String,
    pub execution_id: ExecutionId,
    pub call_id: String,
    pub field: ExecutionContentField,
    pub content: String,
    pub cursor: u64,
    pub next_cursor: u64,
    pub end_cursor: u64,
    pub revision: u64,
    pub available: bool,
    pub source_hash: Option<String>,
}
/// Creator-confirmed child identity. Reply text never establishes this binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DelegateChild {
    pub session_id: String,
    pub target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionSnapshot {
    #[serde(
        default,
        skip_serializing_if = "crate::policy::InvocationSubject::is_tool"
    )]
    pub subject: crate::policy::InvocationSubject,
    pub execution_id: ExecutionId,
    pub session_id: String,
    pub call_id: String,
    pub tool: String,
    pub agent: String,
    pub mode: Option<String>,
    pub status: ExecutionStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegate_child: Option<DelegateChild>,
    #[serde(default)]
    pub capabilities: ExecutionCapabilities,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<ToolFailure>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<crate::observation::diagnostics::ToolDiagnostic>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<OutputMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<ExecutionDelivery>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arguments: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invocation_result: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_availability: Option<OutputAvailability>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arguments_available: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_available: Option<bool>,
}
/// Bounded semantic observations. The Session sink must not reenter State or
/// the registry: delivery runs under the registry observation fence.
#[derive(Debug, Clone)]
pub struct ExecutionNotice {
    pub row: ExecutionSnapshot,
    pub previous: Option<ExecutionSnapshot>,
    pub result_delivered: bool,
}

pub trait ExecutionNoticeSink: Send + Sync {
    fn changed(&self, notice: ExecutionNotice);
}

impl ExecutionSnapshot {
    pub fn metadata(&self) -> Self {
        let mut row = self.clone();
        row.arguments_available = Some(self.arguments.is_some());
        row.result_available = Some(
            self.invocation_result.is_some() || (self.output.is_none() && self.result.is_some()),
        );
        row.result = None;
        row.arguments = None;
        row.invocation_result = None;
        row.delivery = None;
        row
    }
    pub fn new(
        session_id: String,
        call_id: String,
        tool: String,
        agent: String,
        mode: Option<String>,
    ) -> Self {
        Self {
            subject: crate::policy::InvocationSubject::Tool,
            execution_id: ExecutionId(0),
            session_id,
            call_id,
            tool,
            agent,
            mode,
            status: ExecutionStatus::Running,
            delegate_child: None,
            capabilities: ExecutionCapabilities::default(),
            failure: None,
            diagnostic: None,
            output: None,
            started_at_unix_ms: None,
            elapsed_ms: None,
            delivery: None,
            result: None,
            arguments: None,
            invocation_result: None,
            output_availability: None,
            arguments_available: None,
            result_available: None,
        }
    }
}

#[cfg(test)]
mod result_budget_tests {
    use super::*;
    #[test]
    fn producer_failure_bounds_include_nested_escaping_without_multiplying_json_twice() {
        for kind in [
            ToolFailureKind::Failed,
            ToolFailureKind::Timeout,
            ToolFailureKind::Cancelled,
        ] {
            for character in ['\0', '"', '\\', 'x'] {
                let failure = ToolFailure {
                    kind,
                    code: character.to_string().repeat(ToolFailure::MAX_CODE_BYTES),
                    message: character.to_string().repeat(ToolFailure::MAX_MESSAGE_BYTES),
                    retryable: false,
                };
                failure.validate().unwrap();
                let payload = serde_json::json!({"content":"","failure":failure}).to_string();
                assert!(payload.len() <= ToolFailure::max_result_bytes());
                assert!(
                    serde_json::json!(&payload).to_string().len() - 2
                        <= ToolFailure::max_context_result_bytes()
                );
            }
        }
        assert!(ToolFailure::max_context_result_bytes() < 32 * 1024);
        assert!(ToolFailure::max_context_result_bytes() < ToolFailure::max_result_bytes() * 6);
    }
}
