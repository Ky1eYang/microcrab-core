//! Ordered, protocol-neutral assistant replay semantics and optional transport hints.
use crate::ToolCall;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Semantic authority for replay. Tool references resolve only through canonical calls.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AssistantPart {
    Text {
        text: String,
    },
    Reasoning {
        text: String,
    },
    ToolCall {
        call_id: String,
    },
    /// The provider withheld plaintext; no transport can reconstruct it.
    RedactedReasoning,
}

/// Non-authoritative bytes usable only by the named transport. Never executable.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct AssistantReplayHint {
    pub part_index: usize,
    pub scope: String,
    pub kind: String,
    pub data: Value,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "protocol", rename_all = "snake_case")]
pub enum AssistantProviderMetadata {
    Neutral {
        parts: Vec<AssistantPart>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        replay_hints: Vec<AssistantReplayHint>,
    },
    /// Read-old carrier only. Immutable historical bytes are never rewritten.
    Google { parts: Vec<Value> },
}

impl AssistantProviderMetadata {
    /// Materialize historical semantics in memory; stored history remains immutable.
    pub fn normalized(&self, content: &str, calls: &[ToolCall]) -> Result<Self, String> {
        let result = match self {
            Self::Neutral { .. } => self.clone(),
            Self::Google { parts: old } => {
                let mut parts = Vec::new();
                let mut replay_hints = Vec::new();
                let mut calls_iter = calls.iter();
                for old in old {
                    let fields = old
                        .as_object()
                        .ok_or("legacy Google replay part must be an object")?;
                    if fields.keys().any(|key| {
                        !matches!(
                            key.as_str(),
                            "text" | "functionCall" | "thought" | "thoughtSignature"
                        )
                    }) || (old.get("text").is_some() && old.get("functionCall").is_some())
                        || old.get("text").is_some_and(|v| !v.is_string())
                        || old.get("thought").is_some_and(|v| !v.is_boolean())
                        || old.get("thoughtSignature").is_some_and(|v| !v.is_string())
                        || (old.get("text").is_none()
                            && old.get("functionCall").is_none()
                            && old.get("thoughtSignature").is_none())
                    {
                        return Err("unsupported or malformed legacy Google replay part".into());
                    }
                    let part = if let Some(function) = old.get("functionCall") {
                        let call = calls_iter
                            .next()
                            .ok_or("Google replay call projection missing")?;
                        if function.get("name").and_then(Value::as_str) != Some(call.name.as_str())
                            || function.get("args").unwrap_or(&json!({})) != &call.arguments
                            || function
                                .get("id")
                                .is_some_and(|id| id.as_str() != Some(call.id.as_str()))
                        {
                            return Err("Google replay differs from tool call projection".into());
                        }
                        AssistantPart::ToolCall {
                            call_id: call.id.clone(),
                        }
                    } else if let Some(text) = old.get("text").and_then(Value::as_str) {
                        if old.get("thought").and_then(Value::as_bool) == Some(true) {
                            AssistantPart::Reasoning { text: text.into() }
                        } else {
                            AssistantPart::Text { text: text.into() }
                        }
                    } else {
                        AssistantPart::RedactedReasoning
                    };
                    let mut shape = serde_json::Map::new();
                    for key in ["thought", "thoughtSignature"] {
                        if let Some(value) = old.get(key) {
                            shape.insert(key.into(), value.clone());
                        }
                    }
                    if let Some(function) = old.get("functionCall") {
                        shape.insert("id_present".into(), json!(function.get("id").is_some()));
                        shape.insert("args_present".into(), json!(function.get("args").is_some()));
                    }
                    replay_hints.push(AssistantReplayHint {
                        part_index: parts.len(),
                        scope: "google".into(),
                        kind: "shape".into(),
                        data: Value::Object(shape),
                    });
                    parts.push(part);
                }
                if calls_iter.next().is_some() {
                    return Err("Google replay call projection missing".into());
                }
                Self::Neutral {
                    parts,
                    replay_hints,
                }
            }
        };
        result.validate_projection(content, calls)?;
        Ok(result)
    }

    pub fn validate_projection(&self, content: &str, calls: &[ToolCall]) -> Result<(), String> {
        let Self::Neutral {
            parts,
            replay_hints,
        } = self
        else {
            return self.normalized(content, calls).map(|_| ());
        };
        let mut text = String::new();
        let mut refs = calls.iter();
        for part in parts {
            match part {
                AssistantPart::Text { text: value } => text.push_str(value),
                AssistantPart::ToolCall { call_id }
                    if refs.next().is_none_or(|call| &call.id != call_id) =>
                {
                    return Err("assistant replay differs from ordered tool call projection".into());
                }
                _ => {}
            }
        }
        if text != content || refs.next().is_some() {
            return Err("assistant replay differs from assistant projection".into());
        }
        let mut keys = std::collections::HashSet::new();
        for hint in replay_hints {
            if hint.part_index >= parts.len()
                || hint.scope.is_empty()
                || hint.kind.is_empty()
                || !keys.insert((hint.part_index, &hint.scope, &hint.kind))
            {
                return Err("invalid or duplicate assistant replay hint association".into());
            }
        }
        Ok(())
    }

    /// Clear transport coupling without dropping plaintext reasoning or ordering.
    /// Legacy carriers need canonical calls and must first use `normalized`.
    pub fn clear_replay_hints(&mut self) {
        if let Self::Neutral { replay_hints, .. } = self {
            replay_hints.clear();
        }
    }
}

impl std::fmt::Debug for AssistantPart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AssistantPart(<redacted>)")
    }
}
impl std::fmt::Debug for AssistantReplayHint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AssistantReplayHint(<redacted>)")
    }
}
impl std::fmt::Debug for AssistantProviderMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AssistantProviderMetadata(<redacted>)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn call() -> ToolCall {
        ToolCall {
            id: "call".into(),
            name: "echo".into(),
            arguments: json!({"x":1}),
            provider_metadata: None,
        }
    }
    #[test]
    fn neutral_roundtrip_keeps_semantics_when_transport_hints_are_cleared() {
        let metadata = AssistantProviderMetadata::Neutral {
            parts: vec![
                AssistantPart::Reasoning {
                    text: "private reasoning".into(),
                },
                AssistantPart::Text {
                    text: "before".into(),
                },
                AssistantPart::ToolCall {
                    call_id: "call".into(),
                },
                AssistantPart::Text {
                    text: "after".into(),
                },
                AssistantPart::RedactedReasoning,
            ],
            replay_hints: vec![AssistantReplayHint {
                part_index: 0,
                scope: "anthropic".into(),
                kind: "signature".into(),
                data: json!("private-signature"),
            }],
        };
        metadata
            .validate_projection("beforeafter", &[call()])
            .unwrap();
        let mut restored: AssistantProviderMetadata =
            serde_json::from_slice(&serde_json::to_vec(&metadata).unwrap()).unwrap();
        assert_eq!(restored, metadata);
        assert!(!format!("{restored:?}").contains("private reasoning"));
        assert!(!format!("{restored:?}").contains("private-signature"));
        restored.clear_replay_hints();
        let AssistantProviderMetadata::Neutral {
            parts,
            replay_hints,
        } = &restored
        else {
            panic!("neutral")
        };
        assert!(replay_hints.is_empty());
        assert!(matches!(&parts[0],AssistantPart::Reasoning{text} if text=="private reasoning"));
        restored
            .validate_projection("beforeafter", &[call()])
            .unwrap();
    }
    #[test]
    fn neutral_projection_rejects_disagreement_and_dangling_or_duplicate_hints() {
        let mut metadata = AssistantProviderMetadata::Neutral {
            parts: vec![
                AssistantPart::Text {
                    text: "answer".into(),
                },
                AssistantPart::ToolCall {
                    call_id: "call".into(),
                },
            ],
            replay_hints: vec![],
        };
        assert!(metadata.validate_projection("changed", &[call()]).is_err());
        assert!(metadata.validate_projection("answer", &[]).is_err());
        let mut changed = call();
        changed.id = "other".into();
        assert!(metadata.validate_projection("answer", &[changed]).is_err());
        let AssistantProviderMetadata::Neutral { replay_hints, .. } = &mut metadata else {
            unreachable!()
        };
        let hint = AssistantReplayHint {
            part_index: 2,
            scope: "google".into(),
            kind: "shape".into(),
            data: json!({}),
        };
        replay_hints.push(hint.clone());
        assert!(metadata.validate_projection("answer", &[call()]).is_err());
        let AssistantProviderMetadata::Neutral { replay_hints, .. } = &mut metadata else {
            unreachable!()
        };
        replay_hints.clear();
        let mut hint = hint;
        hint.part_index = 0;
        replay_hints.extend([hint.clone(), hint]);
        assert!(metadata.validate_projection("answer", &[call()]).is_err());
    }
    #[test]
    fn historical_native_bytes_stay_immutable_while_in_memory_semantics_are_extracted() {
        let bytes=br#"{"protocol":"google","parts":[{"text":"old thinking","thought":true},{"functionCall":{"name":"echo","args":{"x":1}}},{"text":"answer"}]}"#;
        let historical: AssistantProviderMetadata = serde_json::from_slice(bytes).unwrap();
        let before = serde_json::to_vec(&historical).unwrap();
        let mut normalized = historical.normalized("answer", &[call()]).unwrap();
        normalized.clear_replay_hints();
        assert_eq!(serde_json::to_vec(&historical).unwrap(), before);
        let AssistantProviderMetadata::Neutral {
            parts,
            replay_hints,
        } = normalized
        else {
            panic!("neutral")
        };
        assert!(replay_hints.is_empty());
        assert!(matches!(&parts[0],AssistantPart::Reasoning{text} if text=="old thinking"));
        assert!(matches!(&parts[1],AssistantPart::ToolCall{call_id} if call_id=="call"));
    }
}
