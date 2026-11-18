//! Real loopback Gemini REST regression. The server validates protobuf JSON
//! envelopes rather than accepting both aliases or treating IDs as function names.
use microcrab_auditor::{
    AuditBody, Auditor, AuditorError, BoxFuture, EmptyVariableView, HookContext, Injection, Moment,
    NoopAuditor,
};
use microcrab_protocol::{InputPart, ToolDefinition};
use microcrab_transport::{Client, Content, ContentItem, Google, GoogleConfig, ModelEvent};
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

fn content() -> Content {
    Content {
        epoch: 1,
        subagent: None,
        system: "frozen system".into(),
        items: vec![ContentItem::User {
            envelope: None,
            source: None,
            parts: vec![InputPart::Text {
                text: "lookup".into(),
            }],
        }],
        tools: vec![ToolDefinition {
            name: "lookup".into(),
            description: "Lookup a value".into(),
            parameters: json!({"type":"object","properties":{"city":{"type":"string"},"system_instruction":{"type":"string"}},"required":["city"],"additionalProperties":false}),
        }],
        max_output_tokens: 32,
        max_bytes: 65536,
        turn_id: Some(1),
        round: Some(1),
    }
}

fn read_request(socket: &mut TcpStream) -> (String, String, Value) {
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut received = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        let read = socket.read(&mut buffer).unwrap();
        assert!(read > 0, "HTTP request incomplete");
        received.extend_from_slice(&buffer[..read]);
        if let Some(end) = received.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            let headers = std::str::from_utf8(&received[..end]).unwrap();
            let length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            if received.len() >= end + 4 + length {
                let target = headers
                    .lines()
                    .next()
                    .unwrap()
                    .split_whitespace()
                    .nth(1)
                    .unwrap()
                    .to_owned();
                let headers = headers.to_ascii_lowercase();
                let body = serde_json::from_slice(&received[end + 4..end + 4 + length]).unwrap();
                return (target, headers, body);
            }
        }
        assert!(
            received.len() <= 131072,
            "HTTP fixture request budget exceeded"
        );
    }
}

fn strict_native_envelope(body: &Value) {
    for key in [
        "system_instruction",
        "generation_config",
        "tool_config",
        "messages",
        "stream",
    ] {
        assert!(body.get(key).is_none(), "noncanonical request envelope");
    }
    assert_eq!(
        body["systemInstruction"]["parts"][0]["text"],
        "frozen system"
    );
    assert!(
        body["generationConfig"]["maxOutputTokens"]
            .as_u64()
            .is_some()
    );
    assert!(body["generationConfig"].get("max_output_tokens").is_none());
    let tools = body["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 1);
    assert!(tools[0].get("function_declarations").is_none());
    let declaration = &tools[0]["functionDeclarations"][0];
    assert_eq!(declaration["name"], "lookup");
    assert_eq!(
        declaration["parametersJsonSchema"],
        content().tools[0].parameters
    );
    assert!(declaration.get("parameters").is_none());
    for message in body["contents"].as_array().unwrap() {
        for part in message["parts"].as_array().unwrap() {
            for key in ["function_call", "function_response", "inline_data"] {
                assert!(part.get(key).is_none(), "noncanonical part envelope");
            }
        }
    }
}

fn respond(socket: &mut TcpStream, media_type: &str, body: &str) {
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {media_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(header.as_bytes()).unwrap();
    for chunk in body.as_bytes().chunks(3) {
        socket.write_all(chunk).unwrap();
    }
}

struct Rewrite {
    alias: bool,
}
impl Auditor for Rewrite {
    fn is_enabled(&self) -> bool {
        true
    }
    fn publish(&self, _: Moment, _: AuditBody) {}
    fn intervene<'a>(
        &'a self,
        _: Moment,
        context: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
        Box::pin(async move {
            let mut value: Value = serde_json::from_str(context.request_body.unwrap()).unwrap();
            if self.alias {
                value["system_instruction"] = value["systemInstruction"].clone();
            } else {
                value["generationConfig"]["temperature"] = json!(0.2);
                value["generationConfig"]["maxOutputTokens"] = json!(16);
            }
            Ok(Injection {
                request_body: Some(value.to_string()),
                ..Default::default()
            })
        })
    }
}

#[tokio::test]
async fn real_google_round_trip_has_single_canonical_system_tools_config_and_function_linkage() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!(
        "http://{}/v1beta/models/gemini-test:generateContent?route=local&alt=json",
        listener.local_addr().unwrap()
    );
    let worker = std::thread::spawn(move || {
        let mut frozen = None;
        for round in 0..2 {
            let (mut socket, _) = listener.accept().unwrap();
            let (target, headers, body) = read_request(&mut socket);
            assert!(headers.contains("x-goog-api-key:"));
            assert!(!headers.contains("authorization:"));
            assert!(!target.contains("key="));
            strict_native_envelope(&body);
            if round == 0 {
                assert_eq!(
                    target,
                    "/v1beta/models/gemini-test:streamGenerateContent?route=local&alt=sse"
                );
                assert_eq!(body["generationConfig"]["maxOutputTokens"], 16);
                assert_eq!(body["generationConfig"]["temperature"], 0.2);
                frozen = Some((body["systemInstruction"].clone(), body["tools"].clone()));
                let frames = [
                    json!({"candidates":[{"index":0,"content":{"role":"model","parts":[{"thought":true,"text":"private synthetic reasoning 界"}]}}]}),
                    json!({"candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"id":"provider-call","name":"lookup","args":{"city":"Tokyo"}},"thoughtSignature":"b3BhcXVlLXNpZ25lZC1jYWxs"}]}}]}),
                    json!({"candidates":[{"index":0,"finishReason":"STOP"}]}),
                    json!({"usageMetadata":{"promptTokenCount":3,"cachedContentTokenCount":1,"candidatesTokenCount":4,"thoughtsTokenCount":2,"totalTokenCount":9}}),
                ];
                let body = frames
                    .iter()
                    .map(|frame| format!("data: {frame}\n\n"))
                    .collect::<String>();
                respond(&mut socket, "text/event-stream; charset=utf-8", &body);
            } else {
                assert_eq!(
                    target,
                    "/v1beta/models/gemini-test:generateContent?route=local"
                );
                let (system, tools) = frozen.as_ref().unwrap();
                assert_eq!(&body["systemInstruction"], system);
                assert_eq!(&body["tools"], tools);
                assert_eq!(
                    body["contents"][1]["parts"],
                    json!([
                        {"thought":true,"text":"private synthetic reasoning 界"},
                        {"functionCall":{"id":"provider-call","name":"lookup","args":{"city":"Tokyo"}},"thoughtSignature":"b3BhcXVlLXNpZ25lZC1jYWxs"}
                    ])
                );
                let call_part = &body["contents"][1]["parts"][1];
                assert_eq!(call_part["thoughtSignature"], "b3BhcXVlLXNpZ25lZC1jYWxs");
                let call = &call_part["functionCall"];
                assert!(call.get("thoughtSignature").is_none());
                assert_eq!(call["args"], json!({"city":"Tokyo"}));
                let result_part = &body["contents"][2]["parts"][0];
                assert!(result_part.get("thoughtSignature").is_none());
                let result = &result_part["functionResponse"];
                assert_eq!(call["id"], "provider-call");
                assert_eq!(call["name"], "lookup");
                assert_eq!(result["id"], call["id"]);
                assert_eq!(result["name"], call["name"]);
                assert_eq!(result["response"], json!({"output":[1,2]}));
                let response = json!({"candidates":[{"index":0,"content":{"role":"model","parts":[{"thought":true,"text":"hidden"},{"text":"visible answer"}]},"finishReason":"STOP"}]}).to_string();
                respond(&mut socket, "application/json", &response);
            }
        }
    });
    let client = Google::new(GoogleConfig {
        endpoint,
        model: "models/gemini-test".into(),
        api_key: Some("synthetic-key".into()),
        timeout_ms: 5000,
        max_response_bytes: 65536,
    })
    .unwrap();
    let mut content = content();
    let (sender, mut receiver) = tokio::sync::mpsc::channel(32);
    let response = client
        .stream(
            &content,
            &Rewrite { alias: false },
            &EmptyVariableView,
            CancellationToken::new(),
            Some(sender),
        )
        .await
        .unwrap();
    assert!(response.content.is_empty());
    assert_eq!(response.tool_calls.len(), 1);
    assert_eq!(response.tool_calls[0].id, "provider-call");
    assert_eq!(response.tool_calls[0].name, "lookup");
    assert_eq!(response.usage.output_tokens, Some(6));
    assert_eq!(response.usage.uncached_input_tokens, Some(2));
    let mut saw_reasoning = false;
    let mut saw_finished = false;
    while let Ok(event) = receiver.try_recv() {
        if matches!(event, ModelEvent::ReasoningDelta { .. }) {
            saw_reasoning = true;
        }
        if matches!(event, ModelEvent::Finished { .. }) {
            saw_finished = true;
        }
        assert!(!matches!(event, ModelEvent::TextDelta { .. }));
    }
    assert!(saw_reasoning && saw_finished);
    content.items.push(ContentItem::Assistant {
        provider_metadata: response.provider_metadata,
        content: response.content,
        tool_calls: response.tool_calls,
    });
    content.items.push(ContentItem::Tool {
        parts: vec![],
        call_id: "provider-call".into(),
        content: "[1,2]".into(),
    });
    content.round = Some(2);
    // The continuation uses restored history, not the decoder's live objects or
    // a transport-local cache. Opaque evidence survives the same serde contract.
    content.items = serde_json::from_slice(&serde_json::to_vec(&content.items).unwrap()).unwrap();
    let response = client
        .complete(
            &content,
            &NoopAuditor,
            &EmptyVariableView,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(response.content, "visible answer");
    worker.join().unwrap();
}

#[tokio::test]
async fn full_sse_audit_and_native_signed_tail_survive_a_restored_continuation() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/v1beta", listener.local_addr().unwrap());
    let native_parts = vec![
        json!({"thought":true,"text":"hidden synthetic reasoning"}),
        json!({"text":"visible answer 界"}),
        json!({"text":"","thoughtSignature":"opaque-synthetic-text-tail"}),
    ];
    let frames = [
        json!({"candidates":[{"index":0,"content":{"role":"model","parts":[native_parts[0].clone()]}}],"usageMetadata":{"promptTokenCount":100,"cachedContentTokenCount":40}}),
        json!({"candidates":[{"index":0,"content":{"role":"model","parts":[native_parts[1].clone()]}}]}),
        json!({"candidates":[{"index":0,"content":{"role":"model","parts":[native_parts[2].clone()]},"finishReason":"STOP"}]}),
        json!({"usageMetadata":{"promptTokenCount":100,"candidatesTokenCount":2,"thoughtsTokenCount":3,"totalTokenCount":105}}),
    ];
    let raw = format!(
        ": synthetic raw framing must survive\r\n\r\n{}",
        frames
            .iter()
            .map(|frame| format!("data: {frame}\r\n\r\n"))
            .collect::<String>()
    );
    let server_raw = raw.clone();
    let server_parts = native_parts.clone();
    let worker = std::thread::spawn(move || {
        let mut first = None;
        for round in 0..2 {
            let (mut socket, _) = listener.accept().unwrap();
            let (_, _, body) = read_request(&mut socket);
            strict_native_envelope(&body);
            if round == 0 {
                first = Some(body);
                // Three-byte writes split UTF-8 and SSE framing independently.
                respond(&mut socket, "text/event-stream; charset=utf-8", &server_raw);
            } else {
                let first = first.as_ref().unwrap();
                for key in ["systemInstruction", "tools", "generationConfig"] {
                    assert_eq!(body[key], first[key], "frozen request field changed");
                }
                assert_eq!(body["contents"][0], first["contents"][0]);
                assert_eq!(body["contents"][1]["role"], "model");
                assert_eq!(body["contents"][1]["parts"], json!(server_parts));
                assert_eq!(body["contents"][2]["role"], "user");
                respond(&mut socket, "application/json", &json!({
                    "candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"continued"}]},"finishReason":"STOP"}]
                }).to_string());
            }
        }
    });
    let client = Google::new(GoogleConfig {
        endpoint,
        model: "gemini-test".into(),
        api_key: None,
        timeout_ms: 5000,
        max_response_bytes: 65536,
    })
    .unwrap();
    let (audit_sender, mut audit_receiver) = tokio::sync::mpsc::unbounded_channel();
    let auditor = microcrab_auditor::ChannelAuditor::from_sender(audit_sender);
    let (event_sender, mut event_receiver) = tokio::sync::mpsc::channel(32);
    let mut history = content();
    let response = client
        .stream(
            &history,
            &auditor,
            &EmptyVariableView,
            CancellationToken::new(),
            Some(event_sender),
        )
        .await
        .unwrap();
    assert_eq!(response.content, "visible answer 界");
    assert!(response.tool_calls.is_empty());
    assert_eq!(response.usage.input_tokens, Some(100));
    assert_eq!(response.usage.cache_read_tokens, Some(40));
    assert_eq!(response.usage.uncached_input_tokens, Some(60));
    assert_eq!(response.usage.output_tokens, Some(5));
    assert_eq!(response.usage.total_tokens, Some(105));
    let microcrab_protocol::AssistantProviderMetadata::Neutral { parts, .. } =
        response.provider_metadata.as_ref().unwrap()
    else {
        panic!("new Google responses must record neutral semantics")
    };
    assert!(
        matches!(&parts[0],microcrab_protocol::AssistantPart::Reasoning{text} if !text.is_empty())
    );
    let start = audit_receiver.try_recv().unwrap();
    let terminal = audit_receiver.try_recv().unwrap();
    assert_eq!(start.event, Moment::RequestPrepared);
    assert_eq!(terminal.event, Moment::RoundCompleted);
    assert_eq!(terminal.call_id, start.call_id);
    assert_eq!(terminal.response.as_deref(), Some(raw.as_str()));
    assert!(terminal.response_bytes.is_none());
    assert_eq!(terminal.response_complete, Some(true));
    assert_eq!(terminal.response_status, Some(200));
    assert_eq!(
        terminal.response_content_type.as_deref(),
        Some("text/event-stream")
    );
    assert_eq!(
        terminal.outcome,
        Some(microcrab_auditor::AuditOutcome::Accepted)
    );
    assert_eq!(terminal.usage.as_ref().unwrap().cache_read_tokens, Some(40));
    assert!(audit_receiver.try_recv().is_err());
    let mut displayed = String::new();
    let mut saw_reasoning = false;
    while let Ok(event) = event_receiver.try_recv() {
        match event {
            ModelEvent::TextDelta { text } => displayed.push_str(&text),
            ModelEvent::ReasoningDelta { .. } => saw_reasoning = true,
            _ => {}
        }
    }
    assert_eq!(displayed, "visible answer 界");
    assert!(saw_reasoning);
    history.items.push(ContentItem::Assistant {
        content: response.content,
        tool_calls: response.tool_calls,
        provider_metadata: response.provider_metadata,
    });
    history.items.push(ContentItem::User {
        envelope: None,
        source: None,
        parts: vec![InputPart::Text {
            text: "continue".into(),
        }],
    });
    // Exercise the durable serde boundary, not decoder-local continuation state.
    history.items = serde_json::from_slice(&serde_json::to_vec(&history.items).unwrap()).unwrap();
    history.round = Some(2);
    let continued = client
        .complete(
            &history,
            &NoopAuditor,
            &EmptyVariableView,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(continued.content, "continued");
    worker.join().unwrap();
}

#[tokio::test]
async fn interrupted_sse_audit_keeps_the_received_prefix_and_marks_it_partial() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/v1beta", listener.local_addr().unwrap());
    let prefix = format!(
        "data: {}\r\n\r\n",
        json!({
            "candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"partial synthetic output"}]}}],
            "usageMetadata":{"promptTokenCount":100,"cachedContentTokenCount":40}
        })
    );
    let sent_prefix = prefix.clone();
    let (release, released) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        read_request(&mut socket);
        write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", sent_prefix.len() + 64).unwrap();
        socket.write_all(sent_prefix.as_bytes()).unwrap();
        released.recv_timeout(Duration::from_secs(5)).unwrap();
        // Closing before the declared length is a real HTTP read failure, not EOF.
    });
    let client = Google::new(GoogleConfig {
        endpoint,
        model: "gemini-test".into(),
        api_key: None,
        timeout_ms: 5000,
        max_response_bytes: 65536,
    })
    .unwrap();
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let auditor = microcrab_auditor::ChannelAuditor::from_sender(sender);
    let (events, mut deltas) = tokio::sync::mpsc::channel(8);
    let input = content();
    let pending = client.stream(
        &input,
        &auditor,
        &EmptyVariableView,
        CancellationToken::new(),
        Some(events),
    );
    tokio::pin!(pending);
    tokio::select! {
        event = deltas.recv() => assert!(matches!(event, Some(ModelEvent::TextDelta { .. }))),
        result = &mut pending => panic!("stream settled before the fixture released its connection: {result:?}"),
    }
    release.send(()).unwrap();
    assert!(pending.await.is_err());
    let start = receiver.try_recv().unwrap();
    let terminal = receiver.try_recv().unwrap();
    assert_eq!(terminal.call_id, start.call_id);
    assert_eq!(terminal.event, Moment::RoundCompleted);
    assert_eq!(terminal.response.as_deref(), Some(prefix.as_str()));
    assert_eq!(terminal.response_complete, Some(false));
    assert_eq!(terminal.response_status, Some(200));
    assert_eq!(
        terminal.response_content_type.as_deref(),
        Some("text/event-stream")
    );
    assert_ne!(
        terminal.outcome,
        Some(microcrab_auditor::AuditOutcome::Accepted)
    );
    assert!(receiver.try_recv().is_err());
    worker.join().unwrap();
}

#[tokio::test]
async fn duplicate_protobuf_alias_rewrite_is_rejected_before_any_http_effect() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let client = Google::new(GoogleConfig {
        endpoint: format!("http://{}/v1beta", listener.local_addr().unwrap()),
        model: "gemini-test".into(),
        api_key: None,
        timeout_ms: 1000,
        max_response_bytes: 65536,
    })
    .unwrap();
    let result = client
        .complete(
            &content(),
            &Rewrite { alias: true },
            &EmptyVariableView,
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(
        result,
        Err(microcrab_transport::Error::Protocol(_))
    ));
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}
