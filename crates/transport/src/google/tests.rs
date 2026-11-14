use super::*;
use microcrab_protocol::ToolDefinition;

fn request() -> Request {
    Request {
        epoch: 0,
        messages: vec![Message::User {
            parts: vec![InputPart::Text {
                text: "hello".into(),
            }],
        }],
        tools: vec![ToolDefinition {
            name: "lookup".into(),
            description: "Lookup a value".into(),
            parameters: json!({"type":"object"}),
        }],
        max_output_tokens: 32,
    }
}

#[test]
fn adaptation_preserves_job_id_union_schema() {
    let schema = json!({
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["list", "wait", "cancel"]},
            "id": {"oneOf": [
                {"type": "string", "minLength": 1},
                {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 16,
                    "uniqueItems": true,
                    "items": {"type": "string", "minLength": 1}
                }
            ]}
        },
        "required": ["id", "action"]
    });
    let mut request = request();
    request.tools = vec![ToolDefinition {
        name: "job".into(),
        description: "Control executions".into(),
        parameters: schema.clone(),
    }];
    let client = Google::new(GoogleConfig {
        endpoint: "http://127.0.0.1:1/v1beta".into(),
        model: "gemini-test".into(),
        api_key: None,
        timeout_ms: 1000,
        max_response_bytes: 65536,
    })
    .unwrap();
    let body = client.adapt(&request).unwrap();
    let declaration = &body["tools"][0]["functionDeclarations"][0];
    assert_eq!(declaration["parametersJsonSchema"], schema);
    assert!(declaration.get("parameters").is_none());
}

#[test]
fn native_endpoint_switches_action_without_duplicate_alt_or_model_prefix() {
    for model in ["gemini-test", "models/gemini-test"] {
        let base = "https://generativelanguage.googleapis.com/v1beta/";
        let url = resolve_endpoint(base, model, true).unwrap();
        assert_eq!(
            url.path(),
            "/v1beta/models/gemini-test:streamGenerateContent"
        );
        assert_eq!(url.query(), Some("alt=sse"));
    }
    let url = resolve_endpoint("https://example.invalid/v1beta/models/gemini-test:generateContent?route=local&alt=json&alt=sse", "gemini-test", true).unwrap();
    assert_eq!(
        url.path(),
        "/v1beta/models/gemini-test:streamGenerateContent"
    );
    assert_eq!(url.query_pairs().filter(|(key, _)| key == "alt").count(), 1);
    assert!(
        url.query_pairs()
            .any(|(key, value)| key == "route" && value == "local")
    );
    let plain = resolve_endpoint(url.as_str(), "gemini-test", false).unwrap();
    assert_eq!(plain.path(), "/v1beta/models/gemini-test:generateContent");
    assert_eq!(plain.query(), Some("route=local"));
    for (endpoint, model) in [
        ("https://example.invalid/v1beta", "other/gemini-test"),
        ("https://example.invalid/v1beta", "../escape"),
        (
            "https://example.invalid/v1beta?key=synthetic",
            "gemini-test",
        ),
        (
            "https://example.invalid/v1beta/models/wrong:generateContent",
            "gemini-test",
        ),
    ] {
        assert!(resolve_endpoint(endpoint, model, false).is_err());
    }
}

#[test]
fn native_rewrite_preserves_frozen_fields_and_rejects_alias_pairs() {
    let old = json!({"systemInstruction":{"parts":[{"text":"frozen"}]},"contents":[{"role":"user","parts":[{"text":"hello"}]}],"tools":[],"generationConfig":{"maxOutputTokens":32}});
    let mut valid = old.clone();
    valid["generationConfig"]["temperature"] = json!(0.1);
    valid["generationConfig"]["maxOutputTokens"] = json!(16);
    valid["contents"][0]["parts"][0]["text"] = json!("explicit rewrite");
    crate::validate_request_rewrite(&old.to_string(), &valid.to_string(), 65536).unwrap();
    let mut cases = vec![];
    for (alias, field) in [
        ("system_instruction", "systemInstruction"),
        ("generation_config", "generationConfig"),
    ] {
        let mut value = old.clone();
        value[alias] = old[field].clone();
        cases.push(value);
    }
    let mut value = old.clone();
    value["systemInstruction"]["parts"][0]["text"] = json!("changed");
    cases.push(value);
    let mut value = old.clone();
    value["tools"] = json!([{"functionDeclarations":[]}]);
    cases.push(value);
    let mut value = old.clone();
    value["generationConfig"]["max_output_tokens"] = json!(8);
    cases.push(value);
    let mut value = old.clone();
    value["generationConfig"]["maxOutputTokens"] = json!(33);
    cases.push(value);
    let mut value = old.clone();
    value["generationConfig"]
        .as_object_mut()
        .unwrap()
        .remove("maxOutputTokens");
    cases.push(value);
    let mut value = old.clone();
    value["contents"][0]["parts"][0]["function_call"] = json!({"name":"lookup"});
    cases.push(value);
    for value in cases {
        assert!(
            crate::validate_request_rewrite(&old.to_string(), &value.to_string(), 65536).is_err()
        );
    }
}

#[test]
fn decoder_distinguishes_reasoning_and_requires_a_terminal_candidate() {
    let mut decoder = GoogleSseDecoder::default();
    let events = decoder.push_value(&json!({"candidates":[{"index":0,"content":{"role":"model","parts":[{"thought":true,"text":"reason"},{"text":"answer"}]}}]})).unwrap();
    assert!(matches!(&events[0], ModelEvent::ReasoningDelta { text } if text == "reason"));
    assert!(decoder.finish(&request()).is_err());
    let mut decoder = GoogleSseDecoder::default();
    decoder.push_value(&json!({"candidates":[{"content":{"parts":[{"thought":true,"text":"reason"},{"text":"answer"}]},"finishReason":"STOP"}]})).unwrap();
    assert_eq!(decoder.finish(&request()).unwrap().content, "answer");
    for value in [
        json!({"candidates":[{"finishReason":"SAFETY"}]}),
        json!({"candidates":[{"finishReason":"MALFORMED_FUNCTION_CALL"}]}),
        json!({"candidates":[{"index":1,"content":{"parts":[{"text":"other candidate"}]}}]}),
        json!({"candidates":[{},{}]}),
        json!({"error":{"message":"must not leak upstream diagnostic"}}),
        json!({"promptFeedback":{"blockReason":"SAFETY"}}),
    ] {
        assert!(GoogleSseDecoder::default().push_value(&value).is_err());
    }
}

#[test]
fn decoder_preserves_signed_parallel_calls_on_exact_parts_and_rejects_truncation() {
    let value = json!({"candidates":[{"content":{"parts":[
        {"functionCall":{"id":"signed-first","name":"lookup","args":{}},"thoughtSignature":"b3BhcXVlLWZpcnN0"},
        {"functionCall":{"id":"unsigned","name":"lookup","args":{}}},
        {"functionCall":{"id":"signed-last","name":"lookup","args":{}},"thoughtSignature":"b3BhcXVlLWxhc3Q="}
    ]},"finishReason":"STOP"}]});
    let response = decode_json_response(value, &request()).unwrap();
    assert_eq!(response.tool_calls.len(), 3);
    assert_eq!(response.tool_calls[1].provider_metadata, None);
    let response: Response =
        serde_json::from_slice(&serde_json::to_vec(&response).unwrap()).unwrap();
    let mut history = request();
    history.messages.push(Message::Assistant {
        content: response.content,
        tool_calls: response.tool_calls,
        provider_metadata: response.provider_metadata,
    });
    let client = Google::new(GoogleConfig {
        endpoint: "http://127.0.0.1:1/v1beta".into(),
        model: "gemini-test".into(),
        api_key: None,
        timeout_ms: 1000,
        max_response_bytes: 65536,
    })
    .unwrap();
    let body = client.adapt(&history).unwrap();
    let parts = body["contents"][1]["parts"].as_array().unwrap();
    assert_eq!(parts[0]["thoughtSignature"], "b3BhcXVlLWZpcnN0");
    assert!(parts[1].get("thoughtSignature").is_none());
    assert_eq!(parts[2]["thoughtSignature"], "b3BhcXVlLWxhc3Q=");
    assert_eq!(parts[0]["functionCall"]["id"], "signed-first");
    assert_eq!(parts[1]["functionCall"]["id"], "unsigned");
    assert_eq!(parts[2]["functionCall"]["id"], "signed-last");
    assert!(
        parts
            .iter()
            .all(|part| part["functionCall"]["args"] == json!({}))
    );
    let authored = body.to_string();
    assert!(validate_request_rewrite(&authored, &authored, 65536).is_ok());
    for action in ["replace", "remove", "arguments"] {
        let mut changed = body.clone();
        let signed = &mut changed["contents"][1]["parts"][0];
        match action {
            "replace" => signed["thoughtSignature"] = json!("forged"),
            "remove" => {
                signed.as_object_mut().unwrap().remove("thoughtSignature");
            }
            _ => signed["functionCall"]["args"] = json!({"changed":true}),
        }
        assert!(validate_request_rewrite(&authored, &changed.to_string(), 65536).is_err());
    }
    for signature in [None, Some("b3BhcXVlLWZpcnN0")] {
        let mut part = json!({"functionCall":{"id":"call","name":"lookup","args":{}}});
        if let Some(signature) = signature {
            part["thoughtSignature"] = json!(signature);
        }
        assert!(
            decode_json_response(
                json!({"candidates":[{"content":{"parts":[part]},"finishReason":"MAX_TOKENS"}]}),
                &request()
            )
            .is_err()
        );
    }
}

#[test]
fn adaptation_wraps_scalar_results_and_rejects_orphan_tool_identity() {
    let client = Google::new(GoogleConfig {
        endpoint: "http://127.0.0.1:1/v1beta".into(),
        model: "gemini-test".into(),
        api_key: None,
        timeout_ms: 1000,
        max_response_bytes: 65536,
    })
    .unwrap();
    let mut request = request();
    request.messages.push(Message::Assistant {
        content: String::new(),
        provider_metadata: None,
        tool_calls: vec![ToolCall {
            id: "call".into(),
            name: "lookup".into(),
            arguments: json!({}),
            provider_metadata: None,
        }],
    });
    request.messages.push(Message::Tool {
        call_id: "call".into(),
        content: "[1,2]".into(),
    });
    let body = client.adapt(&request).unwrap();
    let result = &body["contents"][2]["parts"][0]["functionResponse"];
    assert_eq!(result["id"], "call");
    assert_eq!(result["name"], "lookup");
    assert_eq!(result["response"], json!({"output":[1,2]}));
    request.messages.push(Message::Tool {
        call_id: "orphan".into(),
        content: "done".into(),
    });
    assert!(client.adapt(&request).is_err());
}

#[test]
fn successful_response_without_usage_metadata_counts_one_unknown_attempt() {
    let response = decode_json_response(
        json!({"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"answer"}]},"finishReason":"STOP"}]}),
        &request(),
    )
    .unwrap();
    assert_eq!(response.content, "answer");
    assert_eq!(
        response.usage,
        Usage {
            samples: 1,
            ..Usage::default()
        }
    );
}

#[test]
fn reported_input_and_output_do_not_require_cache_metadata() {
    let usage = extract_usage(
        &json!({"promptTokenCount":10,"candidatesTokenCount":4,"totalTokenCount":14}),
    )
    .unwrap();
    assert_eq!(usage.samples, 1);
    assert_eq!(usage.input_tokens, Some(10));
    assert_eq!(usage.output_tokens, Some(4));
    assert_eq!(usage.total_tokens, Some(14));
    assert_eq!(usage.cache_read_tokens, Some(0));
    assert_eq!(usage.cache_write_tokens, None);
    assert_eq!(usage.uncached_input_tokens, Some(10));
}

#[test]
fn absent_cache_needs_reported_prompt_and_malformed_native_counts_fail() {
    for metadata in [
        json!({}),
        Value::Null,
        json!({"candidatesTokenCount":4}),
        json!({"promptTokenCount":null}),
        json!({"promptTokenCount":10,"cachedContentTokenCount":null}),
    ] {
        let usage = extract_usage(&metadata).unwrap();
        assert_eq!(usage.cache_read_tokens, None);
        assert_eq!(usage.uncached_input_tokens, None);
    }
    for metadata in [
        json!({"promptTokenCount":-1}),
        json!({"promptTokenCount":10,"cachedContentTokenCount":-1}),
        json!({"promptTokenCount":10,"cachedContentTokenCount":"0"}),
        json!({"promptTokenCount":10,"cachedContentTokenCount":11}),
        json!({"promptTokenCount":u64::MAX,"candidatesTokenCount":1}),
    ] {
        assert!(extract_usage(&metadata).is_err(), "{metadata}");
    }
    let zero = extract_usage(&json!({"promptTokenCount":0})).unwrap();
    assert_eq!(zero.cache_read_tokens, Some(0));
    assert_eq!(zero.uncached_input_tokens, Some(0));
}

#[test]
fn cold_stream_uses_merged_final_usage_without_erasing_late_output() {
    let mut decoder = GoogleSseDecoder::default();
    decoder.push_value(&json!({"candidates":[{"content":{"parts":[{"text":"answer"}]},"finishReason":"STOP"}]})).unwrap();
    for metadata in [
        json!({"promptTokenCount":100}),
        json!({"candidatesTokenCount":4}),
        json!({}),
        Value::Null,
    ] {
        decoder
            .push_value(&json!({"usageMetadata":metadata}))
            .unwrap();
    }
    let usage = decoder.finish(&request()).unwrap().usage;
    assert_eq!(usage.input_tokens, Some(100));
    assert_eq!(usage.output_tokens, Some(4));
    assert_eq!(usage.cache_read_tokens, Some(0));
    assert_eq!(usage.uncached_input_tokens, Some(100));
}

#[test]
fn explicit_cache_counts_preserve_reported_zero_and_known_uncached_input() {
    for cached in [0, 3] {
        let usage = extract_usage(
            &json!({"promptTokenCount":10,"cachedContentTokenCount":cached,"candidatesTokenCount":4,"totalTokenCount":14}),
        ).unwrap();
        assert_eq!(usage.samples, 1);
        assert_eq!(usage.input_tokens, Some(10));
        assert_eq!(usage.cache_read_tokens, Some(cached));
        assert_eq!(usage.uncached_input_tokens, Some(10 - cached));
        assert_eq!(usage.output_tokens, Some(4));
        assert_eq!(usage.total_tokens, Some(14));
        assert_eq!(usage.cache_write_tokens, None);
    }
}

#[test]
fn usage_includes_thoughts_without_guessing_missing_visible_counts() {
    let usage = extract_usage(
        &json!({"promptTokenCount":10,"cachedContentTokenCount":3,"candidatesTokenCount":4,"thoughtsTokenCount":2,"totalTokenCount":16}),
    ).unwrap();
    assert_eq!(usage.input_tokens, Some(10));
    assert_eq!(usage.uncached_input_tokens, Some(7));
    assert_eq!(usage.output_tokens, Some(6));
    assert_eq!(usage.reasoning_tokens, Some(2));
    assert_eq!(usage.non_reasoning_output_tokens, Some(4));
    assert_eq!(usage.total_tokens, Some(16));
    assert_eq!(
        extract_usage(&json!({"thoughtsTokenCount":2}))
            .unwrap()
            .output_tokens,
        None
    );
}

fn replay_client() -> Google {
    Google::new(GoogleConfig {
        endpoint: "http://127.0.0.1:1/v1beta".into(),
        model: "gemini-test".into(),
        api_key: None,
        timeout_ms: 1000,
        max_response_bytes: 65_536,
    })
    .unwrap()
}

fn append_native(history: &mut Request, response: Response) {
    history.messages.push(Message::Assistant {
        content: response.content,
        tool_calls: response.tool_calls,
        provider_metadata: response.provider_metadata,
    });
}

#[test]
fn json_and_sse_preserve_hidden_thoughts_empty_signed_tail_and_signature_only_parts() {
    let parts = json!([
        {"thought":true,"text":"private reason","thoughtSignature":"reason-signature"},
        {"text":"an"},
        {"text":"swer"},
        {"text":"","thoughtSignature":"signed-empty-tail"},
        {"thoughtSignature":"signature-only"}
    ]);
    let full = json!({"candidates":[{"content":{"parts":parts},"finishReason":"STOP"}]});
    let json_response = decode_json_response(full, &request()).unwrap();
    let mut decoder = GoogleSseDecoder::default();
    let mut events = Vec::new();
    for (index, part) in parts.as_array().unwrap().iter().enumerate() {
        let mut frame = json!({"candidates":[{"content":{"parts":[part]}}]});
        if index + 1 == parts.as_array().unwrap().len() {
            frame["candidates"][0]["finishReason"] = json!("STOP");
        }
        events.extend(
            decoder
                .push_frame(crate::sse::SseFrame {
                    event: None,
                    data: frame.to_string(),
                })
                .unwrap(),
        );
    }
    let streamed = decoder.finish(&request()).unwrap();
    assert_eq!(json_response, streamed);
    assert_eq!(streamed.content, "answer");
    let visible = events
        .iter()
        .filter_map(|event| match event {
            ModelEvent::TextDelta { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<String>();
    assert_eq!(visible, "answer");
    let mut history = request();
    append_native(&mut history, streamed);
    let body = replay_client().adapt(&history).unwrap();
    assert_eq!(body["contents"][1]["parts"], parts);
    assert!(validate_request_rewrite(&body.to_string(), &body.to_string(), 65_536).is_ok());
}

#[test]
fn ordered_parallel_calls_replay_original_optional_ids_and_arguments() {
    let parts = json!([
        {"text":"before"},
        {"functionCall":{"name":"lookup"},"thoughtSignature":"first"},
        {"text":"between"},
        {"functionCall":{"id":"wire-second","name":"lookup","args":{"key":1}}},
        {"text":"after"}
    ]);
    let response = decode_json_response(
        json!({"candidates":[{"content":{"parts":parts},"finishReason":"STOP"}]}),
        &request(),
    )
    .unwrap();
    let runtime_id = response.tool_calls[0].id.clone();
    assert!(!runtime_id.is_empty());
    assert_eq!(response.tool_calls[0].arguments, json!({}));
    assert_eq!(response.content, "beforebetweenafter");
    let mut history = request();
    append_native(&mut history, response);
    history.messages.push(Message::Tool {
        call_id: runtime_id.clone(),
        content: "one".into(),
    });
    history.messages.push(Message::Tool {
        call_id: "wire-second".into(),
        content: "two".into(),
    });
    let body = replay_client().adapt(&history).unwrap();
    assert_eq!(body["contents"][1]["parts"], parts);
    assert!(
        body["contents"][2]["parts"][0]["functionResponse"]
            .get("id")
            .is_none()
    );
    assert_eq!(
        body["contents"][2]["parts"][0]["functionResponse"]["name"],
        "lookup"
    );
    assert_eq!(
        body["contents"][2]["parts"][1]["functionResponse"]["id"],
        "wire-second"
    );
    let Message::Assistant { tool_calls, .. } = &history.messages[1] else {
        panic!("assistant")
    };
    assert_eq!(tool_calls[0].id, runtime_id);
}

#[test]
fn evidence_model_contents_keep_group_boundaries_and_signed_rewrites_keep_positions() {
    let first = decode_json_response(json!({"candidates":[{"content":{"parts":[{"text":"first","thoughtSignature":"sig"},{"text":"tail"}]},"finishReason":"STOP"}]}), &request()).unwrap();
    let second = decode_json_response(
        json!({"candidates":[{"content":{"parts":[{"text":"second"}]},"finishReason":"STOP"}]}),
        &request(),
    )
    .unwrap();
    let mut history = request();
    append_native(&mut history, first);
    append_native(&mut history, second);
    history.messages.push(Message::Assistant {
        content: "legacy".into(),
        tool_calls: vec![],
        provider_metadata: None,
    });
    let body = replay_client().adapt(&history).unwrap();
    assert_eq!(body["contents"].as_array().unwrap().len(), 4);
    let authored = body.to_string();
    for action in ["part-order", "group-order", "role", "split", "merge"] {
        let mut changed = body.clone();
        match action {
            "part-order" => changed["contents"][1]["parts"]
                .as_array_mut()
                .unwrap()
                .swap(0, 1),
            "group-order" => changed["contents"].as_array_mut().unwrap().swap(1, 2),
            "role" => changed["contents"][1]["role"] = json!("user"),
            "split" => {
                let tail = changed["contents"][1]["parts"]
                    .as_array_mut()
                    .unwrap()
                    .pop()
                    .unwrap();
                changed["contents"]
                    .as_array_mut()
                    .unwrap()
                    .insert(2, json!({"role":"model","parts":[tail]}));
            }
            _ => {
                let other = changed["contents"].as_array_mut().unwrap().remove(2);
                changed["contents"][1]["parts"]
                    .as_array_mut()
                    .unwrap()
                    .extend(other["parts"].as_array().unwrap().clone());
            }
        }
        assert!(
            validate_request_rewrite(&authored, &changed.to_string(), 65_536).is_err(),
            "{action}"
        );
    }
}

#[test]
fn forged_native_evidence_cannot_override_neutral_content_calls_or_signatures() {
    let response = decode_json_response(json!({"candidates":[{"content":{"parts":[{"text":"answer"},{"functionCall":{"id":"call","name":"lookup","args":{}},"thoughtSignature":"sig"}]},"finishReason":"STOP"}]}), &request()).unwrap();
    for action in [
        "text",
        "name",
        "arguments",
        "id",
        "signature",
        "extra-call",
        "invalid-part",
    ] {
        let mut changed = response.clone();
        match action {
            "text" => changed.content = "forged".into(),
            "name" => changed.tool_calls[0].name = "other".into(),
            "arguments" => changed.tool_calls[0].arguments = json!({"extra":true}),
            "id" => changed.tool_calls[0].id = "other".into(),
            "signature" => {
                let Some(AssistantProviderMetadata::Neutral { replay_hints, .. }) =
                    &mut changed.provider_metadata
                else {
                    panic!("neutral")
                };
                replay_hints
                    .iter_mut()
                    .find(|h| h.kind == "shape" && h.part_index == 1)
                    .unwrap()
                    .data["thoughtSignature"] = json!("forged");
            }
            "extra-call" => changed.tool_calls.push(changed.tool_calls[0].clone()),
            _ => {
                let Some(AssistantProviderMetadata::Neutral { replay_hints, .. }) =
                    &mut changed.provider_metadata
                else {
                    panic!("metadata")
                };
                replay_hints.push(microcrab_protocol::AssistantReplayHint {
                    part_index: 999,
                    scope: "google".into(),
                    kind: "shape".into(),
                    data: json!({}),
                });
            }
        }
        assert!(validate_replay(&changed).is_err(), "{action}");
        let mut history = request();
        append_native(&mut history, changed);
        assert!(replay_client().adapt(&history).is_err(), "{action}");
    }
}

#[test]
fn partial_usage_snapshots_merge_present_fields_keep_zero_and_all_raw_details() {
    let snapshots = [
        json!({"promptTokenCount":100,"cachedContentTokenCount":80,"thoughtsTokenCount":3}),
        json!({"candidatesTokenCount":4}),
        json!({"cachedContentTokenCount":0}),
        json!({}),
        Value::Null,
    ];
    let mut decoder = GoogleSseDecoder::default();
    decoder.push_value(&json!({"candidates":[{"content":{"parts":[{"text":"answer"}]},"finishReason":"STOP"}]})).unwrap();
    for snapshot in &snapshots {
        decoder
            .push_value(&json!({"usageMetadata":snapshot}))
            .unwrap();
    }
    let usage = decoder.finish(&request()).unwrap().usage;
    assert_eq!(usage.input_tokens, Some(100));
    assert_eq!(usage.cache_read_tokens, Some(0));
    assert_eq!(usage.uncached_input_tokens, Some(100));
    assert_eq!(usage.reasoning_tokens, Some(3));
    assert_eq!(usage.non_reasoning_output_tokens, Some(4));
    assert_eq!(usage.output_tokens, Some(7));
    assert_eq!(usage.total_tokens, Some(107));
    assert_eq!(usage.samples, 1);
    assert_eq!(usage.provider_details, snapshots.to_vec());
}

#[test]
fn terminal_candidate_allows_only_late_signed_metadata_and_then_usage() {
    let terminal =
        json!({"candidates":[{"content":{"parts":[{"text":"answer"}]},"finishReason":"STOP"}]});
    let mut decoder = GoogleSseDecoder::default();
    decoder.push_value(&terminal).unwrap();
    for part in [
        json!({"text":"","thoughtSignature":"late-empty"}),
        json!({"thoughtSignature":"late-only"}),
    ] {
        decoder
            .push_frame(crate::sse::SseFrame {
                event: None,
                data: json!({"candidates":[{"content":{"parts":[part]}}]}).to_string(),
            })
            .unwrap();
    }
    decoder.push_value(&json!({"usageMetadata":{"promptTokenCount":100,"cachedContentTokenCount":80,"candidatesTokenCount":2}})).unwrap();
    let response = decoder.finish(&request()).unwrap();
    assert_eq!(response.content, "answer");
    assert_eq!(response.usage.cache_read_tokens, Some(80));
    let mut history = request();
    append_native(&mut history, response);
    assert_eq!(
        replay_client().adapt(&history).unwrap()["contents"][1]["parts"],
        json!([
            {"text":"answer"},
            {"text":"","thoughtSignature":"late-empty"},
            {"thoughtSignature":"late-only"}
        ])
    );
    for part in [
        json!({"text":"new visible","thoughtSignature":"sig"}),
        json!({"text":"new hidden","thought":true,"thoughtSignature":"sig"}),
        json!({"functionCall":{"name":"lookup"},"thoughtSignature":"sig"}),
        json!({"text":""}),
    ] {
        let mut decoder = GoogleSseDecoder::default();
        decoder.push_value(&terminal).unwrap();
        assert!(
            decoder
                .push_value(&json!({"candidates":[{"content":{"parts":[part]}}]}))
                .is_err()
        );
    }
}

#[test]
fn native_envelopes_reject_aliases_but_function_args_keep_underscore_keys() {
    let valid = json!({"functionCall":{"name":"lookup","args":{"snake_case":{"nested_key":1}}},"thoughtSignature":"sig"});
    assert!(replay::validate_part(&valid).is_ok());
    for part in [
        json!({"function_call":{"name":"lookup"},"thoughtSignature":"sig"}),
        json!({"text":"answer","thought_signature":"sig"}),
        json!({"functionCall":{"name":"lookup","thought_signature":"sig"}}),
    ] {
        assert!(replay::validate_part(&part).is_err());
    }
}
