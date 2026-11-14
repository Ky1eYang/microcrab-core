//! The eight public hook points and owned, typed observation payloads.
use crate::{InputPart, Response, Usage};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Moment {
    SessionCreated,
    SessionRestored,
    InputAdmitted,
    RequestPrepared,
    RoundCompleted,
    ToolCall,
    ToolResult,
    SessionTerminated,
}

/// Content observed at a hook. Request bodies travel separately, only at
/// RequestPrepared, so authored provider bodies never enter durable Rust records.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[expect(
    clippy::large_enum_variant,
    reason = "Hook observers construct owned public payloads directly; boxing response would change that API"
)]
pub enum HookPayload {
    #[default]
    None,
    InputAdmitted {
        parts: Vec<InputPart>,
    },
    RoundCompleted {
        response: Option<Response>,
        status: Option<u16>,
        usage: Option<Usage>,
    },
    ToolCall {
        name: String,
        arguments: Value,
    },
    ToolResult {
        result: Option<String>,
        error: Option<String>,
    },
}
