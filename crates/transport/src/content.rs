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
        /// Framework origin is trusted Session metadata, never caller input.
        /// Absent on historical/user input so their serialized bytes stay stable.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source: Option<UserSource>,
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
    },
}

/// Durable origin marker for framework-generated user-role turns.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserSource {
    Framework,
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
