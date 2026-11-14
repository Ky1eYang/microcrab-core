//! Real HTTP compatibility coverage, including exact raw auditing on rejection.
use super::serve;
use microcrab_auditor::{AuditOutcome, ChannelAuditor, EmptyVariableView};
use microcrab_protocol::ToolDefinition;
use microcrab_transport::{
    Client, Content, Error, ModelEvent, OpenAiCompatible, OpenAiCompatibleConfig,
};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

async fn roundtrip(
    media_type: &str,
    body: &str,
    expect_ok: bool,
) -> (Option<microcrab_protocol::Response>, Vec<ModelEvent>) {
    let (endpoint, worker) = serve(media_type, body);
    let client = OpenAiCompatible::new(OpenAiCompatibleConfig {
        endpoint,
        model: "test".into(),
        api_key: None,
        timeout_ms: 5000,
        max_response_bytes: 65536,
    })
    .unwrap();
    let content = Content {
        epoch: 1,
        subagent: None,
        system: "system".into(),
        items: vec![],
        tools: vec![ToolDefinition {
            name: "lookup".into(),
            description: "fixture".into(),
            parameters: json!({"type":"object","properties":{"q":{"type":"string"}},"required":["q"],"additionalProperties":false}),
        }],
        max_output_tokens: 32,
        max_bytes: 65536,
        turn_id: Some(1),
        round: Some(1),
    };
    let (audit_tx, mut audit_rx) = tokio::sync::mpsc::unbounded_channel();
    let auditor = ChannelAuditor::from_sender(audit_tx);
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(32);
    let result = client
        .stream(
            &content,
            &auditor,
            &EmptyVariableView,
            CancellationToken::new(),
            Some(event_tx),
        )
        .await;
    worker.join().unwrap();
    if expect_ok {
        assert!(result.is_ok(), "{result:?}");
    } else {
        assert!(
            matches!(&result, Err(Error::Provider(_)) | Err(Error::Transport)),
            "{result:?}"
        );
        if let Err(Error::Provider(detail)) = &result {
            assert!(detail.public_protocol_reason().is_some(), "{detail:?}");
        }
    }
    let _prepared = audit_rx.try_recv().unwrap();
    let terminal = audit_rx.try_recv().unwrap();
    assert_eq!(
        terminal.outcome,
        Some(if expect_ok {
            AuditOutcome::Accepted
        } else {
            AuditOutcome::Unavailable
        })
    );
    assert_eq!(terminal.response.as_deref(), Some(body));
    assert_eq!(terminal.response_complete, Some(true));
    assert!(audit_rx.try_recv().is_err());
    let mut events = Vec::new();
    while let Ok(event) = event_rx.try_recv() {
        events.push(event);
    }
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, ModelEvent::Finished { .. }))
            .count(),
        usize::from(expect_ok)
    );
    (result.ok(), events)
}

fn frame(choice: Value) -> String {
    format!("data: {}\n\n", json!({"choices":[choice]}))
}
fn call(arguments: &str) -> Value {
    json!({"id":"call_1","type":"function","function":{"name":"lookup","arguments":arguments}})
}

#[tokio::test]
async fn json_and_sse_accept_only_approved_finish_aliases() {
    for finish in ["stop", "end_turn", "stop_sequence", "length"] {
        let body = json!({"error":null,"choices":[{"message":{"role":"assistant","content":"a"},"finish_reason":finish}]}).to_string();
        assert_eq!(
            roundtrip("application/json", &body, true)
                .await
                .0
                .unwrap()
                .content,
            "a"
        );
        let body =
            frame(json!({"delta":{"content":"a"},"finish_reason":finish})) + "data: [DONE]\n\n";
        assert_eq!(
            roundtrip("text/event-stream", &body, true)
                .await
                .0
                .unwrap()
                .content,
            "a"
        );
    }
    for finish in ["tool_calls", "function_call", "stop"] {
        let body = json!({"choices":[{"message":{"role":"assistant","tool_calls":[call("{\"q\":\"x\"}")]},"finish_reason":finish}]}).to_string();
        assert_eq!(
            roundtrip("application/json", &body, true)
                .await
                .0
                .unwrap()
                .tool_calls
                .len(),
            1
        );
        let mut delta_call = call("{\"q\":\"x\"}");
        delta_call["index"] = json!(0);
        let body = frame(json!({"delta":{"tool_calls":[delta_call]},"finish_reason":finish}))
            + "data: [DONE]\n\n";
        assert_eq!(
            roundtrip("text/event-stream", &body, true)
                .await
                .0
                .unwrap()
                .tool_calls
                .len(),
            1
        );
    }
}

#[tokio::test]
async fn http_rejects_incomplete_late_content_bad_indices_and_malformed_arguments() {
    for finish in [
        Value::Null,
        json!("content_filter"),
        json!("unknown"),
        json!("tool_calls"),
    ] {
        let body = json!({"choices":[{"message":{"role":"assistant","content":"a"},"finish_reason":finish}]}).to_string();
        roundtrip("application/json", &body, false).await;
        let body =
            frame(json!({"delta":{"content":"a"},"finish_reason":finish})) + "data: [DONE]\n\n";
        roundtrip("text/event-stream", &body, false).await;
    }
    for choice in [
        json!({"delta":{"content":"late"}}),
        json!({"delta":{"reasoning":"late"}}),
        json!({"delta":{"reasoning_content":"late"}}),
        json!({"delta":{"tool_calls":[{"index":0}]}}),
        json!({"delta":{},"finish_reason":"length"}),
        json!({"delta":null}),
        json!({"delta":{"reasoning":false}}),
        json!({"delta":{"tool_calls":{}}}),
        json!({"index":null,"delta":{}}),
        json!({"index":1,"delta":{}}),
        json!({"index":"0","delta":{}}),
        json!({"index":0.5,"delta":{}}),
    ] {
        let body = frame(json!({"delta":{"content":"a"},"finish_reason":"stop"}))
            + &frame(choice)
            + "data: [DONE]\n\n";
        let (_, events) = roundtrip("text/event-stream", &body, false).await;
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, ModelEvent::TextDelta { text } if text == "late"))
        );
    }
    for arguments in ["{", "[]", "null"] {
        let body = json!({"choices":[{"message":{"role":"assistant","tool_calls":[call(arguments)]},"finish_reason":"stop"}]}).to_string();
        roundtrip("application/json", &body, false).await;
        let mut delta_call = call(arguments);
        delta_call["index"] = json!(0);
        let body = frame(json!({"delta":{"tool_calls":[delta_call]},"finish_reason":"stop"}))
            + "data: [DONE]\n\n";
        roundtrip("text/event-stream", &body, false).await;
    }
    for invalid_call in [
        json!({"id":"call_1","type":"function","function":null}),
        json!({"id":"call_1","type":"function","function":{"name":"lookup","arguments":{}}}),
        json!({"id":"call_1","type":"other","function":{"name":"lookup","arguments":"{}"}}),
        json!({"id":"","type":"function","function":{"name":"lookup","arguments":"{}"}}),
        json!({"id":"call_1","type":"function","function":{"name":"unknown","arguments":"{}"}}),
        json!({"type":"function","function":{"name":"lookup","arguments":"{}"}}),
    ] {
        let body = json!({"choices":[{"message":{"role":"assistant","tool_calls":[invalid_call.clone()]},"finish_reason":"stop"}]}).to_string();
        roundtrip("application/json", &body, false).await;
        let mut delta_call = invalid_call;
        delta_call["index"] = json!(0);
        let body = frame(json!({"delta":{"tool_calls":[delta_call]},"finish_reason":"stop"}))
            + "data: [DONE]\n\n";
        roundtrip("text/event-stream", &body, false).await;
    }
    let calls = vec![call("{\"q\":\"x\"}"), call("{\"q\":\"y\"}")];
    let body = json!({"choices":[{"message":{"role":"assistant","tool_calls":calls},"finish_reason":"stop"}]}).to_string();
    roundtrip("application/json", &body, false).await;
    let mut delta_calls = calls;
    for (index, call) in delta_calls.iter_mut().enumerate() {
        call["index"] = json!(index);
    }
    let body = frame(json!({"delta":{"tool_calls":delta_calls},"finish_reason":"stop"}))
        + "data: [DONE]\n\n";
    roundtrip("text/event-stream", &body, false).await;
    for finish in [
        "length",
        "end_turn",
        "stop_sequence",
        "content_filter",
        "unknown",
    ] {
        let body = json!({"choices":[{"message":{"role":"assistant","tool_calls":[call("{\"q\":\"x\"}")]},"finish_reason":finish}]}).to_string();
        roundtrip("application/json", &body, false).await;
        let mut delta_call = call("{\"q\":\"x\"}");
        delta_call["index"] = json!(0);
        let body = frame(json!({"delta":{"tool_calls":[delta_call]},"finish_reason":finish}))
            + "data: [DONE]\n\n";
        roundtrip("text/event-stream", &body, false).await;
    }
    let body = json!({"choices":[{"message":{"role":"assistant","content":"a"}}]}).to_string();
    roundtrip("application/json", &body, false).await;
    let body = frame(json!({"delta":{"content":"a"}})) + "data: [DONE]\n\n";
    roundtrip("text/event-stream", &body, false).await;
    let body = frame(json!({"delta":{"content":"a"},"finish_reason":"stop"}));
    roundtrip("text/event-stream", &body, false).await;
}
