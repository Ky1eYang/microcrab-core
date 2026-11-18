//! Provider-neutral conversation AST and tool contracts.
pub mod admission;
pub mod application;
pub use admission::{AdmissionBatch, TransportImpact};
pub mod diagnostic;
mod hooks;
mod provider;
pub use provider::{AssistantPart, AssistantProviderMetadata, AssistantReplayHint};
mod types;
pub use hooks::{HookPayload, Moment};
pub use types::{
    ImageSource, InputPart, ScopedVariableSet, SessionInput, VariableMetadata, VariablePersistence,
    VariableRole, VariableVisibility, VariablesChange, secret_placeholder, valid_secret_key,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
    /// Opaque replay evidence, never tool arguments or framework authority.
    /// Absence preserves the exact legacy serialized shape and history hashes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_metadata: Option<ToolCallProviderMetadata>,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "protocol", rename_all = "snake_case")]
pub enum ToolCallProviderMetadata {
    Google { thought_signature: String },
}
impl std::fmt::Debug for ToolCallProviderMetadata {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Google { .. } => formatter
                .debug_struct("Google")
                .field("thought_signature", &"<redacted>")
                .finish(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "snake_case")]
pub enum Message {
    System {
        content: String,
    },
    User {
        parts: Vec<InputPart>,
    },
    Assistant {
        content: String,
        tool_calls: Vec<ToolCall>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_metadata: Option<AssistantProviderMetadata>,
    },
    Tool {
        call_id: String,
        content: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        parts: Vec<InputPart>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Request {
    pub epoch: u64,
    pub messages: Vec<Message>,
    pub tools: Vec<ToolDefinition>,
    pub max_output_tokens: u32,
}

/// Provider-neutral counts for one or more completed provider attempts.
///
/// Input includes cache reads/writes; output includes reasoning. `None` means
/// unavailable, never zero. Total is provider-reported when present, otherwise
/// derived only from known inclusive input/output. Raw objects retain provider
/// semantics and additional fields; callers bound retention by response/round budgets.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Usage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cache_read_tokens: Option<u64>,
    pub cache_write_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub uncached_input_tokens: Option<u64>,
    pub non_reasoning_output_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub provider_details: Vec<Value>,
    /// Zero is the aggregation identity; an attempt without usage has one sample.
    pub samples: u64,
}

impl Usage {
    /// Combine attempts without turning missing counts or overflow into estimates.
    /// Raw details append in attempt order; the caller owns retention budgets.
    pub fn accumulate(&mut self, other: &Self) {
        if other.samples == 0 {
            return;
        }
        if self.samples == 0 {
            self.clone_from(other);
            return;
        }
        fn sum(left: Option<u64>, right: Option<u64>) -> Option<u64> {
            left.zip(right)
                .and_then(|(left, right)| left.checked_add(right))
        }
        self.input_tokens = sum(self.input_tokens, other.input_tokens);
        self.output_tokens = sum(self.output_tokens, other.output_tokens);
        self.cache_read_tokens = sum(self.cache_read_tokens, other.cache_read_tokens);
        self.cache_write_tokens = sum(self.cache_write_tokens, other.cache_write_tokens);
        self.reasoning_tokens = sum(self.reasoning_tokens, other.reasoning_tokens);
        self.uncached_input_tokens = sum(self.uncached_input_tokens, other.uncached_input_tokens);
        self.non_reasoning_output_tokens = sum(
            self.non_reasoning_output_tokens,
            other.non_reasoning_output_tokens,
        );
        self.total_tokens = sum(self.total_tokens, other.total_tokens);
        self.provider_details
            .extend_from_slice(&other.provider_details);
        self.samples = self.samples.saturating_add(other.samples);
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Response {
    pub content: String,
    pub tool_calls: Vec<ToolCall>,
    pub usage: Usage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_metadata: Option<AssistantProviderMetadata>,
}

#[cfg(test)]
mod tool_call_tests {
    use super::*;

    #[test]
    fn assistant_metadata_absence_preserves_legacy_message_and_response_bytes() {
        let message = br#"{"role":"assistant","content":"answer","tool_calls":[]}"#;
        let restored: Message = serde_json::from_slice(message).unwrap();
        assert!(matches!(
            &restored,
            Message::Assistant {
                provider_metadata: None,
                ..
            }
        ));
        assert_eq!(serde_json::to_vec(&restored).unwrap(), message);
        let response = br#"{"content":"answer","tool_calls":[],"usage":{"input_tokens":null,"output_tokens":null,"cache_read_tokens":null,"cache_write_tokens":null,"reasoning_tokens":null,"uncached_input_tokens":null,"non_reasoning_output_tokens":null,"total_tokens":null,"provider_details":[],"samples":0}}"#;
        let restored: Response = serde_json::from_slice(response).unwrap();
        assert_eq!(restored.provider_metadata, None);
        assert_eq!(serde_json::to_vec(&restored).unwrap(), response);
    }

    #[test]
    fn assistant_native_parts_roundtrip_in_order_without_debug_disclosure() {
        let metadata = AssistantProviderMetadata::Google {
            parts: vec![
                serde_json::json!({"text":"private reasoning", "thought":true, "thoughtSignature":"private-signature"}),
                serde_json::json!({"text":"answer", "thoughtSignature":"answer-signature"}),
            ],
        };
        let response = Response {
            content: "answer".into(),
            tool_calls: vec![],
            usage: Usage::default(),
            provider_metadata: Some(metadata.clone()),
        };
        let encoded = serde_json::to_vec(&response).unwrap();
        assert_eq!(
            serde_json::from_slice::<Response>(&encoded).unwrap(),
            response
        );
        let value = serde_json::to_value(&metadata).unwrap();
        assert_eq!(value["protocol"], "google");
        assert_eq!(value["parts"][0]["text"], "private reasoning");
        assert_eq!(value["parts"][1]["text"], "answer");
        let debug = format!("{response:?}");
        assert!(!debug.contains("private reasoning"));
        assert!(!debug.contains("private-signature"));
        assert!(!debug.contains("answer-signature"));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn unsigned_tool_call_keeps_exact_legacy_json_bytes() {
        let legacy = br#"{"id":"old-call","name":"lookup","arguments":{"city":"Tokyo"}}"#;
        let call: ToolCall = serde_json::from_slice(legacy).unwrap();
        assert_eq!(call.provider_metadata, None);
        assert_eq!(serde_json::to_vec(&call).unwrap(), legacy.to_vec());
    }

    #[test]
    fn typed_google_evidence_roundtrips_but_never_appears_in_debug() {
        let signature = "b3BhcXVlLXN5bnRoZXRpYw==";
        let call = ToolCall {
            id: "signed-call".into(),
            name: "lookup".into(),
            arguments: serde_json::json!({}),
            provider_metadata: Some(ToolCallProviderMetadata::Google {
                thought_signature: signature.into(),
            }),
        };
        let restored: ToolCall =
            serde_json::from_slice(&serde_json::to_vec(&call).unwrap()).unwrap();
        assert_eq!(restored, call);
        assert!(!format!("{call:?}").contains(signature));
        assert_eq!(
            serde_json::to_value(&call).unwrap()["provider_metadata"],
            serde_json::json!({"protocol":"google","thought_signature":signature})
        );
    }
}
