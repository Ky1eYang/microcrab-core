//! Protocol-neutral replay projection. Hints supplement semantics; they never supply calls.
use crate::Error;
use microcrab_protocol::{AssistantPart, AssistantProviderMetadata, AssistantReplayHint, ToolCall};
use serde_json::{Value, json};

pub(crate) fn normalized(
    content: &str,
    calls: &[ToolCall],
    metadata: &Option<AssistantProviderMetadata>,
) -> Result<AssistantProviderMetadata, Error> {
    if let Some(metadata) = metadata {
        if matches!(metadata, AssistantProviderMetadata::Google { .. }) {
            super::google::replay::validate_projection(content, calls, Some(metadata))?;
        }
        let mut result = metadata
            .normalized(content, calls)
            .map_err(|reason| Error::Protocol(reason.into()))?;
        if matches!(metadata, AssistantProviderMetadata::Google { .. }) {
            seal(&mut result, calls)?;
        }
        return Ok(result);
    }
    let mut parts = Vec::new();
    if !content.is_empty() {
        parts.push(AssistantPart::Text {
            text: content.into(),
        });
    }
    let mut replay_hints = Vec::new();
    for call in calls {
        if let Some(microcrab_protocol::ToolCallProviderMetadata::Google { thought_signature }) =
            &call.provider_metadata
        {
            replay_hints.push(AssistantReplayHint{part_index:parts.len(),scope:"google".into(),kind:"shape".into(),data:json!({"thoughtSignature":thought_signature,"id_present":true,"args_present":true})});
        }
        parts.push(AssistantPart::ToolCall {
            call_id: call.id.clone(),
        });
    }
    let mut metadata = AssistantProviderMetadata::Neutral {
        parts,
        replay_hints,
    };
    seal(&mut metadata, calls)?;
    Ok(metadata)
}

pub(crate) fn hint<'a>(
    hints: &'a [AssistantReplayHint],
    index: usize,
    scope: &str,
    kind: &str,
) -> Option<&'a Value> {
    hints
        .iter()
        .find(|hint| hint.part_index == index && hint.scope == scope && hint.kind == kind)
        .map(|hint| &hint.data)
}

pub(crate) fn fallback_reasoning(text: &str) -> String {
    format!("[Reasoning]\n{text}\n[/Reasoning]\n")
}

pub(crate) fn native_parts(
    content: &str,
    calls: &[ToolCall],
    metadata: &Option<AssistantProviderMetadata>,
    scope: &str,
) -> Result<Vec<Value>, Error> {
    let metadata = normalized(content, calls, metadata)?;
    validate_bindings(&metadata, calls, scope)?;
    let AssistantProviderMetadata::Neutral {
        parts,
        replay_hints,
    } = metadata
    else {
        unreachable!("normalized replay is neutral")
    };
    let mut result = Vec::new();
    for (index, part) in parts.iter().enumerate() {
        let mut value = match (scope, part) {
            ("google", AssistantPart::Text { text }) => json!({"text":text}),
            ("google", AssistantPart::Reasoning { text }) => json!({"text":text,"thought":true}),
            ("anthropic", AssistantPart::Text { text }) => json!({"type":"text","text":text}),
            ("anthropic", AssistantPart::Reasoning { text }) => {
                if let Some(signature) =
                    hint(&replay_hints, index, scope, "signature").and_then(Value::as_str)
                {
                    json!({"type":"thinking","thinking":text,"signature":signature})
                } else {
                    json!({"type":"text","text":fallback_reasoning(text)})
                }
            }
            ("responses", AssistantPart::Text { text }) => {
                json!({"role":"assistant","content":text})
            }
            ("responses", AssistantPart::Reasoning { text }) => {
                if let Some(evidence) = hint(&replay_hints, index, scope, "reasoning") {
                    let mut item =
                        json!({"type":"reasoning","summary":[{"type":"summary_text","text":text}]});
                    for key in ["id", "encrypted_content"] {
                        if let Some(value) = evidence.get(key) {
                            item[key] = value.clone();
                        }
                    }
                    item
                } else {
                    json!({"role":"assistant","content":fallback_reasoning(text)})
                }
            }
            (_, AssistantPart::ToolCall { call_id }) => {
                let call = calls
                    .iter()
                    .find(|call| &call.id == call_id)
                    .ok_or_else(|| Error::protocol("assistant replay call missing"))?;
                match scope {
                    "google" => {
                        json!({"functionCall":{"id":call.id,"name":call.name,"args":call.arguments}})
                    }
                    "anthropic" => {
                        json!({"type":"tool_use","id":call.id,"name":call.name,"input":call.arguments})
                    }
                    "responses" => {
                        json!({"type":"function_call","call_id":call.id,"name":call.name,"arguments":call.arguments.to_string()})
                    }
                    _ => return Err(Error::protocol("unsupported replay target")),
                }
            }
            ("anthropic", AssistantPart::RedactedReasoning) => {
                if let Some(data) =
                    hint(&replay_hints, index, scope, "redacted").and_then(Value::as_str)
                {
                    json!({"type":"redacted_thinking","data":data})
                } else {
                    json!({"type":"text","text":"[Reasoning unavailable: provider withheld plaintext]"})
                }
            }
            ("responses", AssistantPart::RedactedReasoning) => {
                if let Some(evidence) = hint(&replay_hints, index, scope, "reasoning") {
                    let mut item = json!({"type":"reasoning","summary":[]});
                    for key in ["id", "encrypted_content"] {
                        if let Some(value) = evidence.get(key) {
                            item[key] = value.clone();
                        }
                    }
                    item
                } else {
                    json!({"role":"assistant","content":"[Reasoning unavailable: provider withheld plaintext]"})
                }
            }
            ("google", AssistantPart::RedactedReasoning) => json!({}),
            _ => return Err(Error::protocol("unsupported replay target")),
        };
        if scope == "google" {
            if let Some(shape) = hint(&replay_hints, index, scope, "shape") {
                for key in ["thought", "thoughtSignature"] {
                    if let Some(v) = shape.get(key) {
                        value[key] = v.clone();
                    }
                }
                if let Some(function) = value.get_mut("functionCall").and_then(Value::as_object_mut)
                {
                    if shape.get("id_present").and_then(Value::as_bool) == Some(false) {
                        function.remove("id");
                    }
                    if shape.get("args_present").and_then(Value::as_bool) == Some(false) {
                        function.remove("args");
                    }
                }
            } else if matches!(part, AssistantPart::RedactedReasoning) {
                value = json!({"text":"[Reasoning unavailable: provider withheld plaintext]"});
            }
            super::google::replay::validate_part(&value)?;
        }
        result.push(value);
    }
    Ok(result)
}

pub(crate) fn from_google(
    parts: Vec<Value>,
    content: &str,
    calls: &[ToolCall],
) -> Result<AssistantProviderMetadata, Error> {
    let mut metadata = AssistantProviderMetadata::Google { parts }
        .normalized(content, calls)
        .map_err(|reason| Error::Protocol(reason.into()))?;
    seal(&mut metadata, calls)?;
    Ok(metadata)
}

fn binding(
    parts: &[AssistantPart],
    hints: &[AssistantReplayHint],
    calls: &[ToolCall],
    index: usize,
    scope: &str,
) -> Result<String, Error> {
    use sha2::{Digest, Sha256};
    let part = parts
        .get(index)
        .ok_or_else(|| Error::protocol("replay binding part missing"))?;
    let call = if let AssistantPart::ToolCall { call_id } = part {
        let call = calls
            .iter()
            .find(|call| &call.id == call_id)
            .ok_or_else(|| Error::protocol("replay binding call missing"))?;
        json!({"id":call.id,"name":call.name,"arguments":call.arguments})
    } else {
        Value::Null
    };
    let hints: Vec<_> = hints
        .iter()
        .filter(|h| h.part_index == index && h.scope == scope && h.kind != "semantic_binding")
        .collect();
    let mut canonical = json!([part, call, hints]);
    canonical.sort_all_objects();
    let bytes = serde_json::to_vec(&canonical)?;
    let digest = Sha256::digest(bytes);
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}
/// A digest is a stale-evidence witness, never an authority or a secret authenticator.
pub(crate) fn seal(
    metadata: &mut AssistantProviderMetadata,
    calls: &[ToolCall],
) -> Result<(), Error> {
    let AssistantProviderMetadata::Neutral {
        parts,
        replay_hints,
    } = metadata
    else {
        return Err(Error::protocol("cannot seal native history"));
    };
    replay_hints.retain(|h| h.kind != "semantic_binding");
    let associations: std::collections::BTreeSet<_> = replay_hints
        .iter()
        .filter(|h| h.scope != "chat")
        .map(|h| (h.part_index, h.scope.clone()))
        .collect();
    for (part_index, scope) in associations {
        let data = json!(binding(parts, replay_hints, calls, part_index, &scope)?);
        replay_hints.push(AssistantReplayHint {
            part_index,
            scope,
            kind: "semantic_binding".into(),
            data,
        });
    }
    Ok(())
}
pub(crate) fn validate_bindings(
    metadata: &AssistantProviderMetadata,
    calls: &[ToolCall],
    scope: &str,
) -> Result<(), Error> {
    let AssistantProviderMetadata::Neutral {
        parts,
        replay_hints,
    } = metadata
    else {
        return Ok(());
    };
    if scope == "chat" {
        return Ok(());
    }
    for h in replay_hints
        .iter()
        .filter(|h| h.scope == scope && h.kind != "semantic_binding")
    {
        let expected = binding(parts, replay_hints, calls, h.part_index, scope)?;
        if hint(replay_hints, h.part_index, scope, "semantic_binding").and_then(Value::as_str)
            != Some(expected.as_str())
        {
            return Err(Error::protocol(
                "native replay hint no longer matches canonical assistant semantics",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn calls() -> Vec<ToolCall> {
        vec![ToolCall {
            id: "call".into(),
            name: "echo".into(),
            arguments: json!({"b":2,"a":1}),
            provider_metadata: None,
        }]
    }
    fn metadata(scope: &str) -> AssistantProviderMetadata {
        let mut hints = match scope {
            "google" => vec![
                AssistantReplayHint {
                    part_index: 0,
                    scope: scope.into(),
                    kind: "shape".into(),
                    data: json!({"thought":true,"thoughtSignature":"opaque-origin-google"}),
                },
                AssistantReplayHint {
                    part_index: 2,
                    scope: scope.into(),
                    kind: "shape".into(),
                    data: json!({"thoughtSignature":"opaque-origin-google-call","id_present":true,"args_present":true}),
                },
            ],
            "anthropic" => vec![AssistantReplayHint {
                part_index: 0,
                scope: scope.into(),
                kind: "signature".into(),
                data: json!("opaque-origin-anthropic"),
            }],
            "responses" => vec![AssistantReplayHint {
                part_index: 0,
                scope: scope.into(),
                kind: "reasoning".into(),
                data: json!({"id":"item","encrypted_content":"opaque-origin-responses"}),
            }],
            "chat" => vec![AssistantReplayHint {
                part_index: 0,
                scope: scope.into(),
                kind: "reasoning_field".into(),
                data: json!("reasoning_content"),
            }],
            _ => vec![],
        };
        let mut metadata = AssistantProviderMetadata::Neutral {
            parts: vec![
                AssistantPart::Reasoning {
                    text: "portable thought".into(),
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
            ],
            replay_hints: std::mem::take(&mut hints),
        };
        seal(&mut metadata, &calls()).unwrap();
        metadata
    }
    #[test]
    fn all_origin_target_pairs_replay_semantics_after_persistence_without_foreign_opaque_bytes() {
        for origin in ["google", "anthropic", "responses", "chat"] {
            let metadata = metadata(origin);
            let stored = serde_json::to_vec(&metadata).unwrap();
            let restored: AssistantProviderMetadata = serde_json::from_slice(&stored).unwrap();
            for target in ["google", "anthropic", "responses", "chat"] {
                let projected = if target == "chat" {
                    crate::openai_compatible::chat_replay(
                        "beforeafter",
                        &calls(),
                        &Some(restored.clone()),
                    )
                    .unwrap()
                } else {
                    json!(
                        native_parts("beforeafter", &calls(), &Some(restored.clone()), target)
                            .unwrap()
                    )
                };
                let wire = projected.to_string();
                assert!(wire.contains("portable thought"), "{origin}->{target}");
                assert!(wire.contains("echo"));
                assert!(wire.contains("before"));
                assert!(wire.contains("after"));
                if origin != target {
                    assert!(
                        !wire.contains("opaque-origin"),
                        "foreign opaque bytes crossed {origin}->{target}"
                    );
                }
            }
        }
    }
    #[test]
    fn target_hint_binding_detects_changed_call_semantics_and_can_be_explicitly_cleared() {
        let mut metadata = metadata("google");
        let mut calls = calls();
        calls[0].arguments = json!({"a":9});
        assert!(native_parts("beforeafter", &calls, &Some(metadata.clone()), "google").is_err());
        assert!(native_parts("beforeafter", &calls, &Some(metadata.clone()), "anthropic").is_ok());
        metadata.clear_replay_hints();
        assert!(native_parts("beforeafter", &calls, &Some(metadata), "google").is_ok());
    }
    #[test]
    fn equivalent_json_key_order_preserves_hint_binding() {
        let metadata = metadata("google");
        let mut calls = calls();
        calls[0].arguments = serde_json::from_str("{\"a\":1,\"b\":2}").unwrap();
        assert!(native_parts("beforeafter", &calls, &Some(metadata), "google").is_ok());
    }
    #[test]
    fn old_native_json_is_normalized_before_cross_protocol_projection() {
        let historical = AssistantProviderMetadata::Google {
            parts: vec![
                json!({"text":"old thought","thought":true}),
                json!({"text":"answer"}),
            ],
        };
        let projected = native_parts("answer", &[], &Some(historical), "anthropic").unwrap();
        assert!(
            projected[0]["text"]
                .as_str()
                .unwrap()
                .contains("old thought")
        );
    }
}
