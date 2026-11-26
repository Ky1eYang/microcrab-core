use microcrab_protocol::{HookPayload, Moment, Usage};
use serde::Serialize;

/// Record sent at a lifecycle point. Start and end records share a provider call id.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AuditRecord {
    pub sequence: u64,
    pub event: Moment,
    pub epoch: u64,
    pub subagent: Option<String>,
    pub turn_id: Option<u64>,
    pub round: Option<u64>,
    pub call_id: Option<String>,
    /// SHA-256 of the issued Context budget envelope, when the creator measured it.
    pub context_fingerprint: Option<String>,
    /// Public provider/model/variant selection associated with that issued Content.
    pub context_target: Option<String>,
    /// Exact conversation tool call identity; independent of provider call IDs.
    pub tool_call_id: Option<String>,
    pub payload: HookPayload,
    /// Exact UTF-8 JSON body supplied to the HTTP transport; excludes credentials/headers.
    pub sent_request: Option<String>,
    pub request_diff: Option<String>,
    /// Exact received UTF-8 body, before JSON parsing or provider normalization.
    pub response: Option<String>,
    /// Original bytes only when the received body is not UTF-8 (and `response` is absent).
    pub response_bytes: Option<Vec<u8>>,
    /// Whether the complete HTTP body was received, rather than a bounded/failed prefix.
    pub response_complete: Option<bool>,
    pub response_status: Option<u16>,
    pub response_content_type: Option<String>,
    /// Wall-clock call start; elapsed time uses a monotonic clock.
    pub started_at_unix_ms: Option<u64>,
    pub elapsed_ms: Option<u64>,
    /// Exact normalized-success terminal sample; not a Session admission certificate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_completed_at_ms: Option<u64>,
    /// Request start to first actual nonempty provider stream content delta.
    pub first_token_ms: Option<u64>,
    /// First stream content delta to successful stream EOF, excluding validation.
    pub generation_ms: Option<u64>,
    /// Clock provenance; absent for historical or nonstreaming responses.
    pub timing_source: Option<String>,
    pub outcome: Option<AuditOutcome>,
    pub usage: Option<Usage>,
    /// Normalized tool-call identities for a protected terminal, never arguments.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protected_tool_call_ids: Option<Vec<String>>,
}

/// Provider-call result names aligned with the existing model-audit outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    Accepted,
    Unavailable,
    InvalidResponse,
    LimitExceeded,
    Cancelled,
    TimedOut,
}

/// Closed, advisory field semantics certified by the tool producer. This does
/// not authorize effects or classify opaque historical content by spelling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ToolPresentation {
    CodingFile,
    MessageReceipt,
    Reference,
}

/// Content supplied by the producer. The auditor adds sequence and the request diff.
#[derive(Debug, Clone, Default)]
pub struct AuditBody {
    pub epoch: u64,
    pub subagent: Option<String>,
    pub turn_id: Option<u64>,
    pub round: Option<u64>,
    pub call_id: Option<String>,
    /// SHA-256 of the issued Context budget envelope, when the creator measured it.
    pub context_fingerprint: Option<String>,
    /// Public provider/model/variant selection associated with that issued Content.
    pub context_target: Option<String>,
    /// Exact conversation tool call identity; independent of provider call IDs.
    pub tool_call_id: Option<String>,
    /// Exact runtime reservation for this tool observation, not the provider call ID.
    pub tool_execution_id: Option<u64>,
    /// Live presentation metadata; not part of the canonical provider record.
    pub tool_presentation: Option<ToolPresentation>,
    pub payload: HookPayload,
    pub sent_request: Option<String>,
    pub response: Option<String>,
    pub response_bytes: Option<Vec<u8>>,
    pub response_complete: Option<bool>,
    pub response_status: Option<u16>,
    pub response_content_type: Option<String>,
    pub started_at_unix_ms: Option<u64>,
    pub elapsed_ms: Option<u64>,
    pub response_completed_at_ms: Option<u64>,
    /// Request start to first actual nonempty provider stream content delta.
    pub first_token_ms: Option<u64>,
    /// First stream content delta to successful stream EOF, excluding validation.
    pub generation_ms: Option<u64>,
    /// Clock provenance; absent for historical or nonstreaming responses.
    pub timing_source: Option<String>,
    pub outcome: Option<AuditOutcome>,
    pub usage: Option<Usage>,
    pub protected_tool_call_ids: Option<Vec<String>>,
}
