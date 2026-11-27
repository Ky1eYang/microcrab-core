use microcrab_auditor::{AuditOutcome, ChannelAuditor};
use microcrab_protocol::ToolDefinition;
use microcrab_session::context::Context;
use microcrab_transport::Error;
use microcrab_transport::{Client, ModelEvent};
use microcrab_transport::{OpenAiCompatible, OpenAiCompatibleConfig};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

fn content() -> microcrab_session::context::Content {
    let mut context = Context::new(
        "system".into(),
        vec![ToolDefinition {
            name: "echo".into(),
            description: "Echo".into(),
            parameters: json!({"type":"object"}),
        }],
        65536,
    )
    .unwrap();
    context.admit_user("task", &json!({})).unwrap();
    context.content(100).unwrap()
}
fn config(endpoint: String) -> OpenAiCompatibleConfig {
    OpenAiCompatibleConfig {
        endpoint,
        model: "test".into(),
        api_key: Some("private-key".into()),
        timeout_ms: 2000,
        max_response_bytes: 65536,
    }
}
fn event(value: Value) -> String {
    format!("data: {value}\r\n\r\n")
}
fn chunk(delta: Value, finish: Value) -> String {
    event(json!({"choices":[{"index":0,"delta":delta,"finish_reason":finish}]}))
}

async fn server(body: String, media_type: &str) -> (String, tokio::task::JoinHandle<Vec<u8>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/chat/completions", listener.local_addr().unwrap());
    let media_type = media_type.to_owned();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let (start, length) = loop {
            let mut buffer = [0; 1024];
            let n = socket.read(&mut buffer).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&buffer[..n]);
            if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..end]);
                let length = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|s| s.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                break (end + 4, length);
            }
        };
        while bytes.len() < start + length {
            let mut buffer = [0; 1024];
            let n = socket.read(&mut buffer).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&buffer[..n]);
        }
        let headers = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: {media_type}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
        );
        socket.write_all(headers.as_bytes()).await.unwrap();
        // Deliberately fragment both UTF-8 and SSE delimiters at the wire level.
        for byte in body.bytes() {
            if socket
                .write_all(&[b'1', b'\r', b'\n', byte, b'\r', b'\n'])
                .await
                .is_err()
            {
                break;
            }
        }
        let _ = socket.write_all(b"0\r\n\r\n").await;
        bytes[start..start + length].to_vec()
    });
    (endpoint, task)
}

#[tokio::test]
async fn fragmented_interleaved_sse_returns_validated_tools_trailing_usage_and_exact_audit() {
    let body = ": heartbeat\r\n\r\n".to_owned()
        + chunk(json!({"role":"assistant","content":"你"}), Value::Null).as_str()
        + chunk(
            json!({"tool_calls":[{"index":1,"id":"two","type":"function","function":{"name":"echo","arguments":"{\"b\":"}},{"index":0,"id":"one","type":"function","function":{"name":"echo","arguments":"{\"a\":"}}]}),
            Value::Null,
        )
        .as_str()
        + chunk(
            json!({"content":"好","tool_calls":[{"index":0,"id":"","function":{"name":"","arguments":"1}"}},{"index":1,"id":"","function":{"name":"","arguments":"2}"}}]}),
            Value::Null,
        )
        .as_str()
        + chunk(json!({}), json!("tool_calls")).as_str()
        + event(
            json!({"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":5,"earlier_only":{"retained":true}}}),
        )
        .as_str()
        + event(
            json!({"choices":[],"usage":{"prompt_tokens":7,"completion_tokens":11,"prompt_tokens_details":{"cached_tokens":2,"cache_write_tokens":1},"completion_tokens_details":{"reasoning_tokens":4},"vendor_only":9}}),
        )
        .as_str()
        + "data: [DONE]\r\n\r\n";
    let (endpoint, task) = server(body.clone(), "text/event-stream; charset=utf-8").await;
    let client = OpenAiCompatible::new(config(endpoint)).unwrap();
    let (audit_sender, mut audit) = tokio::sync::mpsc::unbounded_channel();
    let auditor = ChannelAuditor::from_sender(audit_sender);
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let content = content();
    let run = client.stream(
        &content,
        &auditor,
        &microcrab_auditor::EmptyVariableView,
        CancellationToken::new(),
        Some(sender),
    );
    let drain = async {
        let mut events = Vec::new();
        while let Some(event) = receiver.recv().await {
            events.push(event);
        }
        events
    };
    let (response, events) = tokio::join!(run, drain);
    let response = response.unwrap();
    assert_eq!(response.content, "你好");
    assert_eq!(response.tool_calls[0].id, "one");
    assert_eq!(response.tool_calls[0].arguments, json!({"a":1}));
    assert_eq!(response.tool_calls[1].arguments, json!({"b":2}));
    assert_eq!(response.usage.input_tokens, Some(7));
    assert_eq!(response.usage.output_tokens, Some(11));
    assert_eq!(response.usage.cache_read_tokens, Some(2));
    assert_eq!(response.usage.cache_write_tokens, Some(1));
    assert_eq!(response.usage.uncached_input_tokens, Some(4));
    assert_eq!(response.usage.reasoning_tokens, Some(4));
    assert_eq!(response.usage.non_reasoning_output_tokens, Some(7));
    assert_eq!(response.usage.total_tokens, Some(18));
    assert_eq!(response.usage.samples, 1);
    assert_eq!(response.usage.provider_details.len(), 2);
    assert_eq!(
        response.usage.provider_details[0]["earlier_only"]["retained"],
        true
    );
    assert_eq!(response.usage.provider_details[1]["vendor_only"], 9);
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, ModelEvent::Finished { .. }))
            .count(),
        1
    );
    assert!(
        matches!(events.last(), Some(ModelEvent::Finished { usage }) if usage == &response.usage)
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, ModelEvent::ToolCallDelta { .. }))
            .count(),
        4
    );
    assert_eq!(
        events
            .iter()
            .filter_map(|event| match event {
                ModelEvent::ToolCallDelta { index, id, .. } => Some((*index, id.as_deref())),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![(1, Some("two")), (0, Some("one")), (0, None), (1, None)]
    );
    let request = task.await.unwrap();
    let start = audit.try_recv().unwrap();
    let end = audit.try_recv().unwrap();
    assert_eq!(start.sent_request.as_ref().unwrap().as_bytes(), request);
    assert!(
        serde_json::from_slice::<Value>(&request).unwrap()["stream"]
            .as_bool()
            .unwrap()
    );
    assert!(!start.sent_request.unwrap().contains("private-key"));
    assert_eq!(end.response.as_deref(), Some(body.as_str()));
    assert_eq!(end.response_complete, Some(true));
    assert_eq!(end.outcome, Some(AuditOutcome::Accepted));
    assert_eq!(start.call_id, end.call_id);
    assert!(audit.try_recv().is_err());
}

#[tokio::test]
async fn gateway_text_done_with_usage_completes_without_inventing_finish_reason() {
    let body = chunk(json!({"role":"assistant","content":"你好"}), Value::Null)
        + event(
            json!({"choices":[],"usage":{"prompt_tokens":46588,"completion_tokens":1488,"total_tokens":48076}}),
        )
        .as_str()
        + "data: [DONE]\r\n\r\n";
    let (endpoint, task) = server(body.clone(), "text/event-stream").await;
    let client = OpenAiCompatible::new(config(endpoint)).unwrap();
    let (audit_sender, mut audit) = tokio::sync::mpsc::unbounded_channel();
    let auditor = ChannelAuditor::from_sender(audit_sender);
    let (sender, mut receiver) = tokio::sync::mpsc::channel(16);
    let response = client
        .stream(
            &content(),
            &auditor,
            &microcrab_auditor::EmptyVariableView,
            CancellationToken::new(),
            Some(sender),
        )
        .await
        .unwrap();
    assert_eq!(response.content, "你好");
    assert!(response.tool_calls.is_empty());
    assert_eq!(response.usage.input_tokens, Some(46588));
    assert_eq!(response.usage.output_tokens, Some(1488));
    let mut events = Vec::new();
    while let Some(event) = receiver.recv().await {
        events.push(event);
    }
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, ModelEvent::TextDelta { .. }))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, ModelEvent::Finished { .. }))
            .count(),
        1
    );
    task.await.unwrap();
    let start = audit.try_recv().unwrap();
    let end = audit.try_recv().unwrap();
    assert_eq!(start.call_id, end.call_id);
    assert_eq!(end.response.as_deref(), Some(body.as_str()));
    assert_eq!(end.response_complete, Some(true));
    assert_eq!(end.outcome, Some(AuditOutcome::Accepted));
    assert!(!body.contains("\"finish_reason\":\"stop\""));
}

#[tokio::test]
async fn stream_rejects_terminal_and_tool_errors_without_finished_event() {
    let stop = chunk(json!({"content":"partial"}), json!("stop"));
    let tool = |name: &str, arguments: &str| {
        chunk(
            json!({"tool_calls":[{"index":0,"id":"call","type":"function","function":{"name":name,"arguments":arguments}}]}),
            json!("tool_calls"),
        )
    };
    let cases = [
        stop.clone(),
        chunk(json!({"content":"partial"}), json!("content_filter")) + "data: [DONE]\n\n",
        stop.clone() + "data: [DONE]\n\ndata: [DONE]\n\n",
        stop.clone()
            + event(json!({"choices":[],"usage":{"prompt_tokens":-1}})).as_str()
            + event(json!({"choices":[],"usage":{"prompt_tokens":3}})).as_str()
            + "data: [DONE]\n\n",
        tool("echo", "{") + "data: [DONE]\n\n",
        tool("unknown", "{}") + "data: [DONE]\n\n",
        stop + chunk(json!({"content":"late"}), Value::Null).as_str() + "data: [DONE]\n\n",
    ];
    for (index, body) in cases.into_iter().enumerate() {
        let (endpoint, task) = server(body.clone(), "text/event-stream").await;
        let client = OpenAiCompatible::new(config(endpoint)).unwrap();
        let (audit_sender, mut audit) = tokio::sync::mpsc::unbounded_channel();
        let auditor = ChannelAuditor::from_sender(audit_sender);
        let (sender, mut receiver) = tokio::sync::mpsc::channel(32);
        let error = client
            .stream(
                &content(),
                &auditor,
                &microcrab_auditor::EmptyVariableView,
                CancellationToken::new(),
                Some(sender),
            )
            .await
            .unwrap_err();
        if index == 0 {
            let Error::Provider(cause) = &error else {
                panic!("expected typed provider decode cause: {error:?}")
            };
            let protocol = cause
                .protocol_error()
                .expect("incomplete SSE keeps protocol identity");
            assert!(protocol.is_retryable_decode());
            assert_eq!(
                protocol.public_reason(),
                Some("provider SSE ended without its DONE terminal marker")
            );
        } else {
            assert!(matches!(error, Error::Provider(_)));
        }
        while let Ok(event) = receiver.try_recv() {
            assert!(!matches!(event, ModelEvent::Finished { .. }));
        }
        task.await.unwrap();
        audit.try_recv().unwrap();
        let terminal = audit.try_recv().unwrap();
        assert_eq!(terminal.outcome, Some(AuditOutcome::Unavailable));
        assert_eq!(terminal.response_complete, Some(true));
        assert_eq!(terminal.response, Some(body));
        assert!(audit.try_recv().is_err());
    }
}

#[tokio::test]
async fn json_fallback_is_explicit_and_still_sends_stream_true() {
    let body = json!({"choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"json"}}]}).to_string();
    let (endpoint, task) = server(body, "application/json").await;
    let client = OpenAiCompatible::new(config(endpoint)).unwrap();
    let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
    client
        .stream(
            &content(),
            &ChannelAuditor::disconnected(),
            &microcrab_auditor::EmptyVariableView,
            CancellationToken::new(),
            Some(sender),
        )
        .await
        .unwrap();
    assert_eq!(
        receiver.recv().await,
        Some(ModelEvent::TextDelta {
            text: "json".into()
        })
    );
    assert!(matches!(
        receiver.recv().await,
        Some(ModelEvent::Finished { usage }) if usage.samples == 1
            && usage.input_tokens.is_none()
            && usage.output_tokens.is_none()
            && usage.provider_details.is_empty()
    ));
    assert!(receiver.recv().await.is_none());
    let request: Value = serde_json::from_slice(&task.await.unwrap()).unwrap();
    assert_eq!(request["stream"], true);
}

#[tokio::test]
async fn cancellation_and_dropping_backpressured_stream_emit_one_terminal_audit() {
    for drop_future in [false, true] {
        let body = chunk(json!({"content":"one"}), Value::Null)
            + chunk(json!({"content":"two"}), Value::Null).as_str()
            + chunk(json!({}), json!("stop")).as_str()
            + "data: [DONE]\n\n";
        let (endpoint, task) = server(body, "text/event-stream").await;
        let client = OpenAiCompatible::new(config(endpoint)).unwrap();
        let (audit_sender, mut audit) = tokio::sync::mpsc::unbounded_channel();
        let auditor = ChannelAuditor::from_sender(audit_sender);
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let cancellation = CancellationToken::new();
        let content = content();
        let mut run = Box::pin(client.stream(
            &content,
            &auditor,
            &microcrab_auditor::EmptyVariableView,
            cancellation.clone(),
            Some(sender),
        ));
        // The first item proves streaming work was polled. Fill the queue ourselves
        // after receiving it so cancellation also covers waiting on bounded sends.
        tokio::select! {
            result = &mut run => panic!("unexpected completion: {result:?}"),
            item = receiver.recv() => assert!(matches!(item, Some(ModelEvent::TextDelta { .. }))),
        }
        if drop_future {
            drop(run);
        } else {
            cancellation.cancel();
            assert!(matches!(run.await, Err(Error::Cancelled)));
        }
        task.await.unwrap();
        let start = audit.try_recv().unwrap();
        let end = audit.try_recv().unwrap();
        assert_eq!(start.call_id, end.call_id);
        assert_eq!(end.outcome, Some(AuditOutcome::Cancelled));
        assert!(audit.try_recv().is_err());
        while let Ok(event) = receiver.try_recv() {
            assert!(!matches!(event, ModelEvent::Finished { .. }));
        }
    }
}

#[tokio::test]
async fn oversized_sse_audits_only_bounded_prefix() {
    let body = chunk(json!({"content":"x".repeat(1000)}), Value::Null);
    let (endpoint, task) = server(body.clone(), "text/event-stream").await;
    let mut configuration = config(endpoint);
    configuration.max_response_bytes = 32;
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let auditor = ChannelAuditor::from_sender(sender);
    assert!(matches!(
        OpenAiCompatible::new(configuration)
            .unwrap()
            .complete(
                &content(),
                &auditor,
                &microcrab_auditor::EmptyVariableView,
                CancellationToken::new()
            )
            .await,
        Err(Error::Limit(_))
    ));
    task.await.unwrap();
    receiver.try_recv().unwrap();
    let end = receiver.try_recv().unwrap();
    assert_eq!(end.response.as_deref(), Some(&body[..32]));
    assert_eq!(end.response_complete, Some(false));
    assert_eq!(end.outcome, Some(AuditOutcome::LimitExceeded));
}
