//! Native Responses lifecycle/status/usage and neutral reasoning replay tests.
use super::*;
use microcrab_protocol::{AssistantPart, AssistantProviderMetadata, ToolDefinition};
fn request() -> Request {
    Request {
        epoch: 1,
        messages: vec![],
        tools: vec![ToolDefinition {
            name: "echo".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        }],
        max_output_tokens: 100,
    }
}
fn push(d: &mut OpenAiResponsesSseDecoder, v: Value) -> Result<(), Error> {
    d.push_frame(crate::sse::SseFrame {
        event: None,
        data: v.to_string(),
    })
    .map(|_| ())
}
fn tool(arguments: &str) -> OpenAiResponsesSseDecoder {
    let mut d = OpenAiResponsesSseDecoder::default();
    push(&mut d,json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"item","call_id":"call","name":"echo","arguments":arguments}})).unwrap();
    d
}
fn completed(d: &mut OpenAiResponsesSseDecoder) {
    push(
        d,
        json!({"type":"response.completed","response":{"status":"completed"}}),
    )
    .unwrap();
}
#[test]
fn interleaved_output_indices_and_components_keep_canonical_semantics() {
    for terminal_snapshot in [false, true] {
        let mut d = OpenAiResponsesSseDecoder::default();
        for (index, kind, id) in [
            (3, "function_call", "second"),
            (2, "message", "message"),
            (1, "function_call", "first"),
            (0, "reasoning", "reasoning"),
        ] {
            let item = match kind {
                "function_call" => {
                    json!({"type":kind,"id":id,"call_id":id,"name":"echo","arguments":""})
                }
                "message" => json!({"type":kind,"id":id,"content":[]}),
                _ => json!({"type":kind,"id":id,"summary":[]}),
            };
            push(
                &mut d,
                json!({"type":"response.output_item.added","output_index":index,"item":item}),
            )
            .unwrap();
        }
        // Text component 1 arrives before 0, as do reasoning summary components.
        for (kind, index, key, component, text) in [
            ("response.output_text.delta", 2, "content_index", 1, "after"),
            (
                "response.reasoning_summary_text.delta",
                0,
                "summary_index",
                1,
                "thought two",
            ),
            (
                "response.output_text.delta",
                2,
                "content_index",
                0,
                "before",
            ),
            (
                "response.reasoning_summary_text.delta",
                0,
                "summary_index",
                0,
                "thought one",
            ),
        ] {
            let mut value = json!({"type":kind,"output_index":index,"delta":text});
            value[key] = json!(component);
            push(&mut d, value).unwrap();
        }
        for (index, id, arguments) in [(3, "second", "{\"b\":2}"), (1, "first", "{\"a\":1}")] {
            push(&mut d,json!({"type":"response.function_call_arguments.delta","output_index":index,"item_id":id,"delta":arguments})).unwrap();
            push(&mut d,json!({"type":"response.function_call_arguments.done","output_index":index,"item_id":id,"arguments":arguments})).unwrap();
        }
        let output = vec![
            json!({"type":"reasoning","id":"reasoning","summary":[{"type":"summary_text","text":"thought one"},{"type":"summary_text","text":"thought two"}]}),
            json!({"type":"function_call","id":"first","call_id":"first","name":"echo","arguments":"{\"a\":1}"}),
            json!({"type":"message","id":"message","content":[{"type":"output_text","text":"before"},{"type":"output_text","text":"after"}]}),
            json!({"type":"function_call","id":"second","call_id":"second","name":"echo","arguments":"{\"b\":2}"}),
        ];
        for index in [3, 2, 1, 0] {
            push(&mut d,json!({"type":"response.output_item.done","output_index":index,"item":output[index].clone()})).unwrap();
        }
        if terminal_snapshot {
            push(&mut d,json!({"type":"response.completed","response":{"status":"completed","output":output}})).unwrap();
        } else {
            completed(&mut d);
        }
        let response = d.finish(&request()).unwrap();
        assert_eq!(response.content, "beforeafter");
        assert_eq!(
            reasoning_text(&response.provider_metadata),
            "thought onethought two"
        );
        assert_eq!(
            response
                .tool_calls
                .iter()
                .map(|call| call.id.as_str())
                .collect::<Vec<_>>(),
            vec!["first", "second"]
        );
        response
            .provider_metadata
            .as_ref()
            .unwrap()
            .validate_projection(&response.content, &response.tool_calls)
            .unwrap();
    }
}

#[test]
fn json_explicit_nonterminal_status_never_admits_text_or_calls() {
    for status in [
        json!("failed"),
        json!("incomplete"),
        json!("in_progress"),
        Value::Null,
        json!(0),
    ] {
        for nested in [false, true] {
            let mut value = json!({"status":status,"output":[{"type":"message","content":[{"type":"output_text","text":"partial"}]},{"type":"function_call","call_id":"call","name":"echo","arguments":"{}"}]});
            if nested {
                value = json!({"response":value});
            }
            assert!(decode_json_response(value, &request()).is_err());
        }
    }
    let old = json!({"output":[{"type":"message","content":"answer"}]});
    assert!(decode_json_response(old, &request()).is_ok());
}
#[test]
fn call_initial_arguments_survive_and_done_snapshots_must_agree() {
    let mut d = tool("{\"x\":1}");
    push(&mut d,json!({"type":"response.function_call_arguments.done","output_index":0,"item_id":"item","arguments":"{\"x\":1}"})).unwrap();
    completed(&mut d);
    assert_eq!(
        d.finish(&request()).unwrap().tool_calls[0].arguments,
        json!({"x":1})
    );
    let mut d = tool("{\"x\":1}");
    assert!(push(&mut d,json!({"type":"response.function_call_arguments.done","output_index":0,"arguments":"{\"x\":2}"})).is_err());
    let mut d = tool("{}");
    completed(&mut d);
    assert!(
        d.finish(&request()).is_err(),
        "global completion cannot hide missing per-call completion"
    );
    let mut d = tool("{\"x\":");
    assert!(push(&mut d,json!({"type":"response.function_call_arguments.done","output_index":0,"arguments":"{\"x\":1}"})).is_err(),"done must not conceal lost fragments");
}
#[test]
fn duplicate_unknown_closed_and_late_function_events_are_rejected() {
    let mut d = tool("");
    assert!(push(&mut d,json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":"other","name":"echo","arguments":"{}"}})).is_err());
    for index in [1, u64::MAX] {
        assert!(push(&mut d,json!({"type":"response.function_call_arguments.delta","output_index":index,"delta":"{}"})).is_err());
    }
    assert!(push(&mut d,json!({"type":"response.function_call_arguments.delta","output_index":0,"item_id":"other","delta":"{}"})).is_err());
    push(
        &mut d,
        json!({"type":"response.function_call_arguments.done","output_index":0,"arguments":"{}"}),
    )
    .unwrap();
    assert!(
        push(
            &mut d,
            json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":"{}"})
        )
        .is_err()
    );
    push(&mut d,json!({"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","id":"item","call_id":"call","name":"echo","arguments":"{}"}})).unwrap();
    assert!(
        push(
            &mut d,
            json!({"type":"response.output_item.done","output_index":0,"item":{}})
        )
        .is_err()
    );
    completed(&mut d);
    assert!(
        push(
            &mut d,
            json!({"type":"response.output_text.delta","delta":"late"})
        )
        .is_err()
    );
}
#[test]
fn terminal_output_reconciles_id_arguments_and_text() {
    let mut d = tool("{}");
    assert!(push(&mut d,json!({"type":"response.completed","response":{"status":"completed","output":[{"type":"function_call","id":"item","call_id":"other","name":"echo","arguments":"{}"}]}})).is_err());
    let mut d = tool("{}");
    push(&mut d,json!({"type":"response.completed","response":{"status":"completed","output":[{"type":"function_call","id":"item","call_id":"call","name":"echo","arguments":"{}"}]}})).unwrap();
    assert!(d.finish(&request()).is_ok());
    let mut d = OpenAiResponsesSseDecoder::default();
    push(
        &mut d,
        json!({"type":"response.output_text.delta","delta":"before"}),
    )
    .unwrap();
    push(&mut d,json!({"type":"response.completed","response":{"status":"completed","output":[{"type":"message","content":"after"}]}})).unwrap();
    assert!(d.finish(&request()).is_err());
}
#[test]
fn reasoning_plaintext_survives_encrypted_evidence_clear_and_foreign_projection() {
    let response=decode_json_response(json!({"status":"completed","output":[{"type":"reasoning","id":"reasoning-item","summary":[{"type":"summary_text","text":"think"}],"encrypted_content":"private-encrypted"},{"type":"message","content":"answer"}]}),&request()).unwrap();
    let encoded = serde_json::to_vec(&response).unwrap();
    let mut restored: Response = serde_json::from_slice(&encoded).unwrap();
    let native = crate::replay::native_parts(
        &restored.content,
        &restored.tool_calls,
        &restored.provider_metadata,
        "responses",
    )
    .unwrap();
    assert_eq!(native[0]["encrypted_content"], "private-encrypted");
    let foreign = crate::replay::native_parts(
        &restored.content,
        &restored.tool_calls,
        &restored.provider_metadata,
        "anthropic",
    )
    .unwrap();
    assert!(foreign[0]["text"].as_str().unwrap().contains("think"));
    assert!(
        !serde_json::to_string(&foreign)
            .unwrap()
            .contains("private-encrypted")
    );
    restored
        .provider_metadata
        .as_mut()
        .unwrap()
        .clear_replay_hints();
    let Some(AssistantProviderMetadata::Neutral {
        parts,
        replay_hints,
    }) = &restored.provider_metadata
    else {
        panic!("neutral")
    };
    assert!(replay_hints.is_empty());
    assert!(matches!(&parts[0],AssistantPart::Reasoning{text} if text=="think"));
    let replay = crate::replay::native_parts(
        &restored.content,
        &restored.tool_calls,
        &restored.provider_metadata,
        "responses",
    )
    .unwrap();
    assert!(replay[0]["content"].as_str().unwrap().contains("think"));
    assert!(!format!("{response:?}").contains("private-encrypted"));
}
#[test]
fn usage_preserves_reported_total_rejects_bad_components_and_counts_samples() {
    let usage=extract_usage(&json!({"input_tokens":10,"output_tokens":7,"total_tokens":99,"input_tokens_details":{"cached_tokens":3,"cache_write_tokens":2},"output_tokens_details":{"reasoning_tokens":4}})).unwrap();
    assert_eq!(usage.total_tokens, Some(99));
    assert_eq!(usage.uncached_input_tokens, Some(5));
    assert_eq!(usage.non_reasoning_output_tokens, Some(3));
    for value in [
        json!({"input_tokens":-1}),
        json!({"output_tokens":1,"output_tokens_details":{"reasoning_tokens":2}}),
        json!({"input_tokens":1,"input_tokens_details":{"cached_tokens":2}}),
        json!({"input_tokens":u64::MAX,"output_tokens":1}),
        json!({"input_tokens_details":true}),
    ] {
        assert!(extract_usage(&value).is_err(), "{value}");
    }
    let response = decode_json_response(
        json!({"output":[{"type":"message","content":"answer"}]}),
        &request(),
    )
    .unwrap();
    assert_eq!(response.usage.samples, 1);
}
#[test]
fn every_byte_split_keeps_reasoning_tool_arguments_and_terminal_snapshot() {
    let frames = [
        json!({"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning","id":"reason","summary":[]}}),
        json!({"type":"response.reasoning_summary_text.delta","output_index":0,"delta":"思考"}),
        json!({"type":"response.output_item.done","output_index":0,"item":{"type":"reasoning","id":"reason","summary":[{"type":"summary_text","text":"思考"}],"encrypted_content":"opaque"}}),
        json!({"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","id":"item","call_id":"call","name":"echo","arguments":""}}),
        json!({"type":"response.function_call_arguments.delta","output_index":1,"item_id":"item","delta":"{}"}),
        json!({"type":"response.function_call_arguments.done","output_index":1,"item_id":"item","arguments":"{}"}),
        json!({"type":"response.completed","response":{"status":"completed"}}),
    ];
    let wire = frames
        .iter()
        .map(|v| format!("data: {v}\r\n\r\n"))
        .collect::<String>();
    for split in 0..=wire.len() {
        let mut reader = SseFrameReader::default();
        let mut d = OpenAiResponsesSseDecoder::default();
        let mut frames = reader.push(&wire.as_bytes()[..split], false).unwrap();
        frames.extend(reader.push(&wire.as_bytes()[split..], true).unwrap());
        for frame in frames {
            d.push_frame(frame).unwrap();
        }
        let response = d.finish(&request()).unwrap();
        assert_eq!(response.tool_calls[0].arguments, json!({}));
    }
}
