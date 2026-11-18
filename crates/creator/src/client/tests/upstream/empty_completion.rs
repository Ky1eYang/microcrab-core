//! Real HTTP fixtures for semantic empty completions and exact explicit retry.
use super::*;
use microcrab_auditor::{AuditOutcome, ChannelAuditor};
use microcrab_transport::{EmptyCompletionFacts, EmptyCompletionFinish, ModelEvent};
use serde_json::json;

fn json_completion(protocol: Protocol, reasoning: bool) -> serde_json::Value {
    match protocol {
        Protocol::OpenaiCompatible => {
            json!({"choices":[{"message":{"role":"assistant","content":"","reasoning_content":if reasoning {"fixture reasoning"} else {""}},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5}})
        }
        Protocol::OpenaiResponses => {
            json!({"status":"completed","output":if reasoning {vec![json!({"type":"reasoning","summary":[{"type":"summary_text","text":"fixture reasoning"}]})]} else {vec![]},"usage":{"input_tokens":2,"output_tokens":3,"total_tokens":5}})
        }
        Protocol::Anthropic => {
            json!({"content":if reasoning {vec![json!({"type":"thinking","thinking":"fixture reasoning"})]} else {vec![]},"stop_reason":"end_turn","usage":{"input_tokens":2,"output_tokens":3}})
        }
        Protocol::Google => {
            json!({"candidates":[{"content":{"parts":if reasoning {vec![json!({"thought":true,"text":"fixture reasoning"})]} else {vec![]}},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":2,"candidatesTokenCount":3,"totalTokenCount":5}})
        }
    }
}

fn wire_json(value: &serde_json::Value) -> String {
    let body = value.to_string();
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn assert_empty(
    result: Result<Response, Error>,
    reasoning_present: bool,
    finish: EmptyCompletionFinish,
) {
    let Err(Error::Provider(cause)) = result else {
        panic!("expected typed empty completion: {result:?}")
    };
    assert_eq!(
        cause.empty_completion_details(),
        Some(EmptyCompletionFacts {
            reasoning_present,
            finish
        })
    );
    assert!(cause.protocol_error().is_none());
    assert!(!cause.public_reason().unwrap().contains("fixture reasoning"));
}

#[tokio::test]
async fn completed_empty_json_is_one_semantic_attempt_for_every_protocol() {
    for protocol in PROTOCOLS {
        for reasoning in [false, true] {
            for streaming in [false, true] {
                let response = wire_json(&json_completion(protocol, reasoning));
                let (endpoint, worker, calls) = response_server_many_counted(vec![response]).await;
                let client = protocol_client(endpoint, protocol);
                let (sender, mut records) = mpsc::unbounded_channel();
                let auditor = ChannelAuditor::from_sender(sender);
                let result = if streaming {
                    client
                        .stream(
                            &request(),
                            &auditor,
                            &EmptyVariableView,
                            CancellationToken::new(),
                            None,
                        )
                        .await
                } else {
                    client
                        .complete(
                            &request(),
                            &auditor,
                            &EmptyVariableView,
                            CancellationToken::new(),
                        )
                        .await
                };
                assert_empty(result, reasoning, EmptyCompletionFinish::Stop);
                worker.await.unwrap();
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                let prepared = records.try_recv().unwrap();
                let terminal = records.try_recv().unwrap();
                assert_eq!(prepared.call_id, terminal.call_id);
                assert_eq!(terminal.response_complete, Some(true));
                assert_eq!(terminal.outcome, Some(AuditOutcome::Unavailable));
                assert_eq!(
                    terminal.response.unwrap(),
                    json_completion(protocol, reasoning).to_string()
                );
                assert!(records.try_recv().is_err());
            }
        }
    }
}

fn reasoning_sse(protocol: Protocol) -> String {
    match protocol {
        Protocol::OpenaiCompatible => concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":null,\"reasoning_content\":\"fixture reasoning\"}}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3,\"total_tokens\":5}}\n\n",
            "data: [DONE]\n\n"
        ).into(),
        Protocol::OpenaiResponses => format!("data: {{\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"fixture reasoning\"}}\n\ndata: {{\"type\":\"response.completed\",\"response\":{}}}\n\n", json_completion(protocol, true)),
        Protocol::Anthropic => concat!(
            "data: {\"type\":\"message_start\",\"message\":{}}\n\n",
            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\"}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"fixture reasoning\"}}\n\n",
            "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":3}}\n\n",
            "data: {\"type\":\"message_stop\"}\n\n"
        ).into(),
        Protocol::Google => format!("data: {}\n\n", json_completion(protocol, true)),
    }
}

#[tokio::test]
async fn published_reasoning_empty_sse_is_not_replayed_and_manual_retry_keeps_request() {
    for protocol in PROTOCOLS {
        let wire = reasoning_sse(protocol);
        let (endpoint, worker, calls) = response_server_many_counted(vec![sse(&wire); 2]).await;
        let client = protocol_client(endpoint, protocol);
        let content = request();
        let before = serde_json::to_vec(&content).unwrap();
        for _ in 0..2 {
            let (sender, mut events) = mpsc::channel(16);
            let result = client
                .stream(
                    &content,
                    &NoopAuditor,
                    &EmptyVariableView,
                    CancellationToken::new(),
                    Some(sender),
                )
                .await;
            assert_empty(result, true, EmptyCompletionFinish::Stop);
            assert!(
                matches!(events.try_recv().unwrap(), ModelEvent::ReasoningDelta { text } if text == "fixture reasoning")
            );
            assert_eq!(serde_json::to_vec(&content).unwrap(), before);
        }
        worker.await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn published_reasoning_without_terminal_is_incomplete_and_never_replayed() {
    for protocol in PROTOCOLS {
        let wire = match protocol {
            Protocol::OpenaiCompatible => "data: {\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"fixture reasoning\"}}]}\n\n".to_owned(),
            Protocol::OpenaiResponses => "data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"fixture reasoning\"}\n\n".to_owned(),
            Protocol::Anthropic => reasoning_sse(protocol).split("data: {\"type\":\"content_block_stop\"").next().unwrap().to_owned(),
            Protocol::Google => {
                let mut body = json_completion(protocol, true);
                body["candidates"][0].as_object_mut().unwrap().remove("finishReason");
                format!("data: {body}\n\n")
            }
        };
        let (endpoint, worker, calls) = response_server_many_counted(vec![sse(&wire)]).await;
        let (sender, mut events) = mpsc::channel(16);
        let result = protocol_client(endpoint, protocol)
            .stream(
                &request(),
                &NoopAuditor,
                &EmptyVariableView,
                CancellationToken::new(),
                Some(sender),
            )
            .await;
        assert!(
            matches!(events.try_recv().unwrap(), ModelEvent::ReasoningDelta { text } if text == "fixture reasoning")
        );
        match result {
            Err(Error::Provider(cause)) => assert!(cause.empty_completion_details().is_none()),
            Err(_) => {}
            Ok(response) => panic!("incomplete response accepted: {response:?}"),
        }
        worker.await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn responses_completed_without_optional_metadata_is_typed_empty_not_malformed() {
    let (endpoint, worker, calls) = response_server_many_counted(vec![sse(
        "data: {\"type\":\"response.completed\",\"response\":{}}\n\n",
    )])
    .await;
    assert_empty(
        protocol_client(endpoint, Protocol::OpenaiResponses)
            .stream(
                &request(),
                &NoopAuditor,
                &EmptyVariableView,
                CancellationToken::new(),
                None,
            )
            .await,
        false,
        EmptyCompletionFinish::Stop,
    );
    worker.await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn explicit_sse_refusal_is_distinct_from_empty_completion() {
    for refusal in [json!("PRIVATE_REFUSAL"), json!({"invalid":true})] {
        let wire = format!(
            "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
            json!({"choices":[{"index":0,"delta":{"refusal":refusal}}]}),
            json!({"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}),
        );
        let (endpoint, worker) = response_server(sse(&wire)).await;
        let result = protocol_client(endpoint, Protocol::OpenaiCompatible)
            .stream(
                &request(),
                &NoopAuditor,
                &EmptyVariableView,
                CancellationToken::new(),
                None,
            )
            .await;
        let Err(Error::Provider(cause)) = result else {
            panic!("refusal must fail: {result:?}")
        };
        assert!(cause.empty_completion_details().is_none());
        assert!(!cause.public_reason().unwrap().contains("PRIVATE_REFUSAL"));
        worker.await.unwrap();
    }
}

#[tokio::test]
async fn incomplete_or_refused_json_never_acquires_empty_completion_facts() {
    let cases = [
        (
            Protocol::OpenaiCompatible,
            json!({"choices":[{"message":{"role":"assistant","reasoning_content":"reason"},"finish_reason":null}]}),
        ),
        (
            Protocol::OpenaiCompatible,
            json!({"choices":[{"message":{"role":"assistant","refusal":"PRIVATE_REFUSAL"},"finish_reason":"stop"}]}),
        ),
        (
            Protocol::OpenaiCompatible,
            json!({"choices":[{"message":{"role":"assistant","refusal":{}},"finish_reason":"stop"}]}),
        ),
        (
            Protocol::OpenaiCompatible,
            json!({"choices":[{"message":{"role":"assistant"},"finish_reason":"content_filter"}]}),
        ),
        (
            Protocol::OpenaiResponses,
            json!({"status":"incomplete","output":[]}),
        ),
        (Protocol::OpenaiResponses, json!({"output":[]})),
        (
            Protocol::Anthropic,
            json!({"content":[],"stop_reason":"refusal"}),
        ),
        (Protocol::Anthropic, json!({"content":[]})),
        (
            Protocol::Google,
            json!({"candidates":[{"content":{"parts":[]},"finishReason":"SAFETY"}]}),
        ),
    ];
    for (protocol, body) in cases {
        let (endpoint, worker) = response_server(wire_json(&body)).await;
        let result = protocol_client(endpoint, protocol)
            .complete(
                &request(),
                &NoopAuditor,
                &EmptyVariableView,
                CancellationToken::new(),
            )
            .await;
        match result {
            Err(Error::Provider(cause)) => {
                assert!(cause.empty_completion_details().is_none());
                assert!(!cause.public_reason().unwrap().contains("PRIVATE_REFUSAL"));
            }
            Err(_) => {}
            Ok(response) => panic!("unexpected answer: {response:?}"),
        }
        worker.await.unwrap();
    }
}

#[tokio::test]
async fn explicit_output_limit_empty_completion_has_length_not_inferred_stop() {
    for protocol in [
        Protocol::OpenaiCompatible,
        Protocol::Anthropic,
        Protocol::Google,
    ] {
        let mut body = json_completion(protocol, true);
        match protocol {
            Protocol::OpenaiCompatible => body["choices"][0]["finish_reason"] = json!("length"),
            Protocol::Anthropic => body["stop_reason"] = json!("max_tokens"),
            Protocol::Google => body["candidates"][0]["finishReason"] = json!("MAX_TOKENS"),
            _ => unreachable!("tested protocols"),
        }
        let (endpoint, worker) = response_server(wire_json(&body)).await;
        assert_empty(
            protocol_client(endpoint, protocol)
                .complete(
                    &request(),
                    &NoopAuditor,
                    &EmptyVariableView,
                    CancellationToken::new(),
                )
                .await,
            true,
            EmptyCompletionFinish::Length,
        );
        worker.await.unwrap();
    }
}
