//! Native application WebSocket envelope, independent from provider/model transport.
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const VERSION: u16 = 1;
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Command {
    pub v: u16,
    #[serde(rename = "type")]
    pub kind: CommandKind,
    pub request_id: String,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default = "empty_params")]
    pub params: Value,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandKind {
    Command,
}
fn empty_params() -> Value {
    serde_json::json!({})
}
impl Command {
    pub fn validate(&self) -> Result<(), WireError> {
        if self.v != VERSION {
            return Err(WireError::new(
                "unsupported_version",
                "Unsupported protocol version",
            ));
        }
        if self.request_id.is_empty()
            || self.request_id.len() > 128
            || self.method.is_empty()
            || self.method.len() > 96
            || self
                .session_id
                .as_ref()
                .is_some_and(|id| id.is_empty() || id.len() > 128)
            || !self.params.is_object()
        {
            return Err(WireError::new(
                "invalid_command",
                "Invalid command envelope",
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WireError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Box<Value>>,
}
impl WireError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable: false,
            details: None,
        }
    }
}
/// Correlated command result. Session events are not part of this shared
/// envelope: the server socket owns them as `SessionRuntimeEvent`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Frame {
    Result {
        v: u16,
        request_id: String,
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        data: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<WireError>,
    },
}
impl Frame {
    pub fn result(request_id: String, result: Result<Value, WireError>) -> Self {
        match result {
            Ok(data) => Self::Result {
                v: VERSION,
                request_id,
                ok: true,
                data: Some(data),
                error: None,
            },
            Err(error) => Self::Result {
                v: VERSION,
                request_id,
                ok: false,
                data: None,
                error: Some(error),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn optional_error_details_preserve_old_errors_and_structured_result_frames() {
        let legacy = serde_json::json!({"code":"limit","message":"Operation limit exceeded","retryable":false});
        let old: WireError = serde_json::from_value(legacy.clone()).unwrap();
        assert!(old.details.is_none());
        assert_eq!(serde_json::to_value(old).unwrap(), legacy);
        let mut error = WireError::new("limit", "Tool-result reservation exceeds context budget");
        error.details = Some(Box::new(
            serde_json::json!({"reason":"tool_result_reservation","calls":8,"required_bytes":3145728,"budget_bytes":2097152,"pre_effect":true}),
        ));
        let encoded = serde_json::to_vec(&Frame::result("req-limit".into(), Err(error))).unwrap();
        let decoded: Frame = serde_json::from_slice(&encoded).unwrap();
        let value = serde_json::to_value(decoded).unwrap();
        assert_eq!(
            value["error"]["details"]["reason"],
            "tool_result_reservation"
        );
        assert_eq!(value["error"]["details"]["calls"], 8);
        assert_eq!(value["error"]["retryable"], false);
    }

    #[test]
    fn rejects_ambiguous_envelopes_and_preserves_result_correlation() {
        let input =
            r#"{"v":1,"type":"command","request_id":"req-1","method":"session.list","params":{}}"#;
        let mut command: Command = serde_json::from_str(input).unwrap();
        command.validate().unwrap();
        command.v = 2;
        assert_eq!(command.validate().unwrap_err().code, "unsupported_version");
        assert!(
            serde_json::from_str::<Command>(&input.replace("\"params\":{}", "\"legacy\":true"))
                .is_err()
        );
        let frame = Frame::result(
            "req-1".into(),
            Err(WireError::new("not_found", "Session unavailable")),
        );
        let value = serde_json::to_value(frame).unwrap();
        assert_eq!(value["request_id"], "req-1");
        assert_eq!(value["ok"], false);
        assert!(value.get("data").is_none());
    }
}
