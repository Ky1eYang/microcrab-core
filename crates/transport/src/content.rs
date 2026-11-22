//! Provider-neutral session content before text rendering.
use crate::Error;
use microcrab_protocol::{InputPart, ToolCall, ToolDefinition};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::path::Path;

/// Universal content handed from Context to an LLM client.
///
/// System, state, user text, and tools stay structured here. The client renders
/// text into the provider-neutral AST and then adapts that AST to a protocol.
#[derive(Clone, Debug, Serialize)]
pub struct Content {
    pub epoch: u64,
    /// Functional call identity for audit routing; never rendered to the provider.
    pub subagent: Option<String>,
    pub system: String,
    pub items: Vec<ContentItem>,
    pub tools: Vec<ToolDefinition>,
    pub max_output_tokens: u32,
    pub max_bytes: usize,
    pub turn_id: Option<u64>,
    pub round: Option<u64>,
}

/// One piece of session content. State is a judged frame, not rendered text.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[expect(
    clippy::large_enum_variant,
    reason = "Public content variants are constructed and matched by clients; boxing fields changes their ownership API"
)]
pub enum ContentItem {
    State(StateFrame),
    User {
        parts: Vec<InputPart>,
        /// Carrier origin is trusted Session metadata, never caller-authored text.
        /// Absent on historical/user input so their serialized bytes stay stable.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source: Option<UserSource>,
        /// Missing on frozen history and functional helper inputs.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        envelope: Option<CarrierEnvelopeVersion>,
    },
    Assistant {
        content: String,
        tool_calls: Vec<ToolCall>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_metadata: Option<microcrab_protocol::AssistantProviderMetadata>,
    },
    Tool {
        call_id: String,
        content: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        parts: Vec<InputPart>,
    },
}

/// Explicit framing version; absence preserves historical rendering.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CarrierEnvelopeVersion {
    V1,
    /// V1 direct-user framing with a literal, Session-authored notice.
    Important {
        notice: ImportantInput,
    },
    /// V1 framing with a frozen display alias, never an authorization witness.
    ExternalDialogue {
        alias: String,
    },
}

/// Bounded literal text in a trusted, frozen envelope. This type is not an
/// `InputPart` and is never accepted from the serialized `SessionInput`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct ImportantInput(String);

impl ImportantInput {
    pub const MAX_BYTES: usize = 8192;

    pub fn new(text: impl Into<String>) -> Result<Self, Error> {
        let text = text.into();
        if text.len() > Self::MAX_BYTES {
            return Err(Error::Limit("important input byte budget".into()));
        }
        if text.trim().is_empty() || text.contains('\0') {
            return Err(Error::config(
                "important input must be nonblank and contain no NUL",
            ));
        }
        Ok(Self(text))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ImportantInput {
    type Error = Error;

    fn try_from(text: String) -> Result<Self, Error> {
        Self::new(text)
    }
}

/// Trusted, durable carrier origin. Missing source is ordinary user input.
/// These labels never grant permission; Session authenticates them before admission.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserSource {
    Framework,
    Agent {
        agent_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exchange: Option<AgentExchange>,
    },
    External {
        identity: String,
    },
}

/// Session-authored exchange witness, kept out of the caller-authored body.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentExchange {
    pub exchange_id: String,
    pub intent: ExchangeIntent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExchangeIntent {
    Send,
    Reply,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[expect(
    clippy::large_enum_variant,
    reason = "Public content variants are constructed and matched by clients; boxing fields changes their ownership API"
)]
pub enum StateFrame {
    StateSnapshot {
        state: Value,
    },
    /// Replace changed top-level properties and explicitly remove missing keys.
    StatePatch {
        set: Map<String, Value>,
        remove: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        execution_delta: Option<Value>,
        /// Historical task/job deltas remain readable without changing their bytes.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task_delta: Option<Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        job_delta: Option<Value>,
    },
}

impl StateFrame {
    /// Current writers emit only execution metadata. Retired delta fields remain
    /// decode/replay data; their serialization preserves frozen historical frames.
    pub fn patch(
        set: Map<String, Value>,
        remove: Vec<String>,
        execution_delta: Option<Value>,
    ) -> Self {
        Self::StatePatch {
            set,
            remove,
            execution_delta,
            task_delta: None,
            job_delta: None,
        }
    }
}

/// Unresolved local paths share one extension guess. Each adapter still builds
/// its own protocol JSON and may reject paths instead of reading them.
pub(crate) fn read_local_image(path: &Path) -> Result<(&'static str, Vec<u8>), Error> {
    let bytes = std::fs::read(path).map_err(|_| Error::protocol("cannot read image path"))?;
    let media_type = match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("png") => "image/png",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        _ => "application/octet-stream",
    };
    Ok((media_type, bytes))
}

#[cfg(test)]
mod provenance_tests {
    use super::*;

    #[test]
    fn historical_tool_and_framework_bytes_remain_exact() {
        let historical = br#"{"Tool":{"call_id":"old-call","content":"old output"}}"#;
        let restored: ContentItem = serde_json::from_slice(historical).unwrap();
        assert!(matches!(&restored, ContentItem::Tool { parts, .. } if parts.is_empty()));
        assert_eq!(serde_json::to_vec(&restored).unwrap(), historical);

        let historical = br#"{"role":"tool","call_id":"old-call","content":"old output"}"#;
        let restored: microcrab_protocol::Message = serde_json::from_slice(historical).unwrap();
        assert!(
            matches!(&restored, microcrab_protocol::Message::Tool { parts, .. } if parts.is_empty())
        );
        assert_eq!(serde_json::to_vec(&restored).unwrap(), historical);

        let historical =
            br#"{"User":{"parts":[{"type":"text","text":"settled"}],"source":"framework"}}"#;
        let restored: ContentItem = serde_json::from_slice(historical).unwrap();
        assert!(matches!(
            &restored,
            ContentItem::User {
                envelope: None,
                source: Some(UserSource::Framework),
                ..
            }
        ));
        assert_eq!(serde_json::to_vec(&restored).unwrap(), historical);
    }

    #[test]
    fn historical_assistant_bytes_remain_exact_without_replay_metadata() {
        let historical = br#"{"Assistant":{"content":"answer","tool_calls":[]}}"#;
        let restored: ContentItem = serde_json::from_slice(historical).unwrap();
        assert!(matches!(
            &restored,
            ContentItem::Assistant {
                provider_metadata: None,
                ..
            }
        ));
        assert_eq!(serde_json::to_vec(&restored).unwrap(), historical);
    }

    #[test]
    fn historical_user_bytes_are_unchanged_and_framework_origin_roundtrips() {
        let historical = r#"{"User":{"parts":[{"type":"text","text":"ordinary"}]}}"#;
        let ordinary: ContentItem = serde_json::from_str(historical).unwrap();
        assert_eq!(serde_json::to_string(&ordinary).unwrap(), historical);
        let framework = ContentItem::User {
            parts: vec![InputPart::Text {
                text: "Background execution settled".into(),
            }],
            envelope: None,
            source: Some(UserSource::Framework),
        };
        let encoded = serde_json::to_vec(&framework).unwrap();
        assert_eq!(
            serde_json::from_slice::<ContentItem>(&encoded).unwrap(),
            framework
        );
        let content = |item| Content {
            epoch: 1,
            subagent: None,
            system: "frozen".into(),
            items: vec![item],
            tools: vec![],
            max_output_tokens: 32,
            max_bytes: 4096,
            turn_id: Some(1),
            round: Some(1),
        };
        let mut without_source = framework.clone();
        if let ContentItem::User { source, .. } = &mut without_source {
            *source = None;
        }
        let marked = crate::render_ast(&content(framework)).unwrap();
        let ordinary = crate::render_ast(&content(without_source)).unwrap();
        assert_eq!(marked.messages[0], ordinary.messages[0]);
        assert_eq!(marked.tools, ordinary.tools);
        assert_eq!(
            marked.messages[1],
            microcrab_protocol::Message::User {
                parts: vec![InputPart::Text {
                    text: "<framework>\nBackground execution settled\n</framework>\n".into()
                }],
            }
        );
        assert_eq!(
            ordinary.messages[1],
            microcrab_protocol::Message::User {
                parts: vec![InputPart::Text {
                    text: "Background execution settled".into()
                }],
            }
        );
    }
}
