//! Native Messages admission and replay regressions; no provider network calls.
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
fn push(decoder: &mut AnthropicSseDecoder, value: Value) -> Result<(), Error> {
    decoder
        .push_frame(crate::sse::SseFrame {
            event: None,
            data: value.to_string(),
        })
        .map(|_| ())
}
fn tool(initial: Value) -> AnthropicSseDecoder {
    let mut d = AnthropicSseDecoder::default();
    push(&mut d, json!({"type":"message_start","message":{}})).unwrap();
    push(&mut d,json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call","name":"echo","input":initial}})).unwrap();
    d
}
#[test]
fn initial_tool_input_is_not_discarded_and_truncation_cannot_dispatch() {
    let mut d = tool(json!({"x":1}));
    push(&mut d, json!({"type":"content_block_stop","index":0})).unwrap();
    push(&mut d, json!({"type":"message_stop"})).unwrap();
    assert_eq!(
        d.finish(&request()).unwrap().tool_calls[0].arguments,
        json!({"x":1})
    );
    for input in [json!({}), json!({"x":1})] {
        assert!(decode_json_response(json!({"content":[{"type":"tool_use","id":"call","name":"echo","input":input}],"stop_reason":"max_tokens"}),&request()).is_err());
        let mut d = tool(input);
        push(&mut d, json!({"type":"content_block_stop","index":0})).unwrap();
        push(
            &mut d,
            json!({"type":"message_delta","delta":{"stop_reason":"max_tokens"}}),
        )
        .unwrap();
        push(&mut d, json!({"type":"message_stop"})).unwrap();
        assert!(d.finish(&request()).is_err());
    }
}
#[test]
fn block_lifecycle_rejects_duplicate_unknown_closed_and_late_events() {
    let mut d = tool(json!({}));
    assert!(push(&mut d,json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}})).is_err());
    for index in [1, u64::MAX] {
        assert!(push(&mut d,json!({"type":"content_block_delta","index":index,"delta":{"type":"input_json_delta","partial_json":"{}"}})).is_err());
    }
    assert!(push(&mut d, json!({"type":"message_stop"})).is_err());
    push(&mut d, json!({"type":"content_block_stop","index":0})).unwrap();
    assert!(push(&mut d,json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{}"}})).is_err());
    assert!(push(&mut d, json!({"type":"content_block_stop","index":0})).is_err());
    push(&mut d, json!({"type":"message_stop"})).unwrap();
    assert!(push(&mut d, json!({"type":"message_stop"})).is_err());
}
#[test]
fn fragmented_arguments_reconcile_against_nonempty_initial_input() {
    let mut d = tool(json!({"x":1}));
    push(&mut d,json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"x\":2}"}})).unwrap();
    assert!(push(&mut d, json!({"type":"content_block_stop","index":0})).is_err());
}
#[test]
fn inclusive_cache_usage_json_and_fragmented_stream_agree() {
    let raw = json!({"input_tokens":2,"cache_read_input_tokens":3,"cache_creation_input_tokens":5,"output_tokens":7});
    let json = decode_json_response(
        json!({"content":[{"type":"text","text":"answer"}],"usage":raw}),
        &request(),
    )
    .unwrap();
    let mut d = AnthropicSseDecoder::default();
    push(&mut d,json!({"type":"message_start","message":{"usage":{"input_tokens":2,"cache_read_input_tokens":3,"cache_creation_input_tokens":5}}})).unwrap();
    push(&mut d,json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":"answer"}})).unwrap();
    push(&mut d, json!({"type":"content_block_stop","index":0})).unwrap();
    push(&mut d,json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":7}})).unwrap();
    push(&mut d, json!({"type":"message_stop"})).unwrap();
    let stream = d.finish(&request()).unwrap();
    assert_eq!(json.usage.input_tokens, Some(10));
    assert_eq!(stream.usage.input_tokens, Some(10));
    assert_eq!(stream.usage.total_tokens, Some(17));
    assert_eq!(stream.usage.uncached_input_tokens, Some(2));
    for usage in [
        json!({"input_tokens":-1}),
        json!({"input_tokens":1,"cache_read_input_tokens":"0"}),
        json!({"input_tokens":u64::MAX,"cache_read_input_tokens":1}),
    ] {
        assert!(anthropic_usage(&usage).is_err());
    }
    assert_eq!(anthropic_usage(&Value::Null).unwrap().samples, 1);
    assert_eq!(
        anthropic_usage(&json!({"input_tokens":0}))
            .unwrap()
            .input_tokens,
        Some(0)
    );
    assert_eq!(
        anthropic_usage(&json!({"input_tokens":2,"cache_read_input_tokens":null}))
            .unwrap()
            .input_tokens,
        None
    );
}
#[test]
fn reasoning_is_neutral_ordered_and_signatures_are_scoped_and_bound() {
    let response=decode_json_response(json!({"content":[{"type":"thinking","thinking":"consider carefully","signature":"secret-signature"},{"type":"text","text":"answer"},{"type":"redacted_thinking","data":"opaque-bytes"}]}),&request()).unwrap();
    let restored: Response =
        serde_json::from_slice(&serde_json::to_vec(&response).unwrap()).unwrap();
    let Some(AssistantProviderMetadata::Neutral { parts, .. }) = &restored.provider_metadata else {
        panic!("neutral")
    };
    assert!(matches!(&parts[0],AssistantPart::Reasoning{text} if text=="consider carefully"));
    let native = crate::replay::native_parts(
        &restored.content,
        &restored.tool_calls,
        &restored.provider_metadata,
        "anthropic",
    )
    .unwrap();
    assert_eq!(native[0]["signature"], "secret-signature");
    let foreign = crate::replay::native_parts(
        &restored.content,
        &restored.tool_calls,
        &restored.provider_metadata,
        "responses",
    )
    .unwrap();
    let foreign = serde_json::to_string(&foreign).unwrap();
    assert!(foreign.contains("consider carefully"));
    assert!(!foreign.contains("secret-signature"));
    assert!(!foreign.contains("opaque-bytes"));
    let mut changed = restored.clone();
    if let Some(AssistantProviderMetadata::Neutral { parts, .. }) = &mut changed.provider_metadata {
        parts[0] = AssistantPart::Reasoning {
            text: "changed".into(),
        };
    }
    assert!(
        crate::replay::native_parts(
            &changed.content,
            &changed.tool_calls,
            &changed.provider_metadata,
            "anthropic"
        )
        .is_err()
    );
    assert!(
        crate::replay::native_parts(
            &changed.content,
            &changed.tool_calls,
            &changed.provider_metadata,
            "responses"
        )
        .is_ok()
    );
}
#[test]
fn fragmented_text_and_signature_keep_initial_content_and_signature_fallback() {
    for signature in [
        None,
        Some(Value::Null),
        Some(json!(42)),
        Some(json!("pre-")),
    ] {
        let mut d = AnthropicSseDecoder::default();
        push(&mut d, json!({"type":"message_start","message":{}})).unwrap();
        let mut block = json!({"type":"thinking","thinking":"initial-"});
        if let Some(signature) = &signature {
            block["signature"] = signature.clone();
        }
        push(
            &mut d,
            json!({"type":"content_block_start","index":0,"content_block":block}),
        )
        .unwrap();
        for _ in 0..32 {
            push(&mut d, json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"思"}})).unwrap();
            push(&mut d, json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"s"}})).unwrap();
        }
        push(&mut d, json!({"type":"content_block_stop","index":0})).unwrap();
        push(&mut d, json!({"type":"content_block_start","index":1,"content_block":{"type":"text","text":"answer-"}})).unwrap();
        for _ in 0..32 {
            push(&mut d, json!({"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"字"}})).unwrap();
        }
        push(&mut d, json!({"type":"content_block_stop","index":1})).unwrap();
        push(&mut d, json!({"type":"message_stop"})).unwrap();
        let response = d.finish(&request()).unwrap();
        assert_eq!(response.content, format!("answer-{}", "字".repeat(32)));
        let native = crate::replay::native_parts(
            &response.content,
            &response.tool_calls,
            &response.provider_metadata,
            "anthropic",
        )
        .unwrap();
        assert_eq!(
            native[0]["thinking"],
            format!("initial-{}", "思".repeat(32))
        );
        let prefix = signature.as_ref().and_then(Value::as_str).unwrap_or("");
        assert_eq!(
            native[0]["signature"],
            format!("{prefix}{}", "s".repeat(32))
        );
    }
}

#[test]
fn every_byte_split_preserves_native_thinking_and_signature() {
    let frames = [
        json!({"type":"message_start","message":{}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"思考"}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"signed"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"content_block_start","index":1,"content_block":{"type":"text","text":"answer"}}),
        json!({"type":"content_block_stop","index":1}),
        json!({"type":"message_stop"}),
    ];
    let wire = frames
        .iter()
        .map(|v| format!("data: {v}\r\n\r\n"))
        .collect::<String>();
    for split in 0..=wire.len() {
        let mut reader = SseFrameReader::default();
        let mut d = AnthropicSseDecoder::default();
        for frame in reader
            .push(&wire.as_bytes()[..split], false)
            .unwrap()
            .into_iter()
            .chain(reader.push(&wire.as_bytes()[split..], true).unwrap())
        {
            d.push_frame(frame).unwrap();
        }
        let response = d.finish(&request()).unwrap();
        assert_eq!(response.content, "answer");
    }
}
