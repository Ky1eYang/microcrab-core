use crate::hooks;
use microcrab_auditor::{AuditOutcome, AuditRecord, ChannelAuditor as Auditor};
use microcrab_protocol::Moment;
use microcrab_protocol::{InputPart, Message, Response, ToolCall, ToolDefinition, Usage};
use microcrab_session::context::{ContentItem, Context};
use microcrab_transport::Error;
use microcrab_transport::StateFrame;
use microcrab_transport::{Client, render_ast};
use microcrab_transport::{OpenAiCompatible, OpenAiCompatibleConfig};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

fn tools() -> Vec<ToolDefinition> {
    vec![ToolDefinition {
        name: "echo".into(),
        description: "Echo text".into(),
        parameters: json!({"type":"object","properties":{"text":{"type":"string"}}}),
    }]
}
fn context() -> Context {
    Context::new("Static\n  code remains".into(), tools(), 16_384).unwrap()
}
fn response(calls: Vec<ToolCall>) -> Response {
    Response {
        provider_metadata: None,
        content: "".into(),
        tool_calls: calls,
        usage: Usage::default(),
    }
}
fn call(id: &str) -> ToolCall {
    ToolCall {
        provider_metadata: None,
        id: id.into(),
        name: "echo".into(),
        arguments: json!({"text":"hello"}),
    }
}

#[test]
fn state_epochs_keep_system_and_tools_and_preserve_text() {
    let mut context = context();
    context
        .admit_user("hello\n  world", &json!({"cwd":"a","remove_me":true}))
        .unwrap();
    let first = context.content(100).unwrap();
    context
        .append_response(&Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: vec![],
            usage: Usage::default(),
        })
        .unwrap();
    context.admit_user("next", &json!({"cwd":"b"})).unwrap();
    let content = context.content(100).unwrap();
    assert_eq!(content.system, first.system);
    assert_eq!(content.tools, first.tools);
    assert!(matches!(
        &content.items[0],
        ContentItem::State(StateFrame::StateSnapshot { .. })
    ));
    assert_eq!(
        content.items[1],
        ContentItem::User {
            source: None,
            parts: vec![InputPart::Text {
                text: "hello\n  world".into()
            }]
        }
    );
    let ContentItem::State(StateFrame::StatePatch { set, remove, .. }) = &content.items[3] else {
        panic!("state patch");
    };
    assert_eq!(set["cwd"], "b");
    assert_eq!(remove, &vec!["remove_me".to_owned()]);
    let rendered = render_ast(&content).unwrap();
    assert_eq!(rendered.messages.len(), 4);
    let Message::User { parts } = &rendered.messages[3] else {
        panic!("rendered patch and next user share their carrier");
    };
    assert_eq!(parts, &vec![InputPart::Text {
        text: "<state>\n{\"type\":\"state_patch\",\"set\":{\"cwd\":\"b\"},\"remove\":[\"remove_me\"]}\n</state>\n<user>\nnext\n</user>\n".into()
    }]);
    let Message::User { parts } = &rendered.messages[1] else {
        panic!("initial framed user carrier");
    };
    assert_eq!(parts, &vec![InputPart::Text {
        text: "<state>\n{\"type\":\"state_snapshot\",\"state\":{\"cwd\":\"a\",\"remove_me\":true}}\n</state>\n<user>\nhello\n  world\n</user>\n".into()
    }]);
    context
        .compact("summary".into(), &json!({"cwd":"b"}))
        .unwrap();
    let compacted = context.content(100).unwrap();
    assert_eq!(compacted.epoch, 0);
    assert!(context.pending_epoch());
    assert_eq!(compacted.items.len(), 2);
    assert_eq!(compacted.system, first.system);
    assert_eq!(compacted.tools, first.tools);
    assert!(matches!(
        &compacted.items[0],
        ContentItem::State(StateFrame::StateSnapshot { .. })
    ));
}

#[test]
fn restored_framework_notice_keeps_source_and_state_first_batch() {
    let mut context = context();
    context
        .admit_user("notice </framework> literal", &json!({"cwd":"a"}))
        .unwrap();
    let mut snapshot = context.snapshot().unwrap();
    let ContentItem::User { source, .. } = &mut snapshot.items[1] else {
        panic!("notice user item")
    };
    *source = Some(microcrab_transport::UserSource::Framework);
    let mut restored = Context::from_snapshot(snapshot).unwrap();
    restored.synchronize_state(&json!({"cwd":"b"})).unwrap();
    let content = restored.content(100).unwrap();
    let ast = render_ast(&content).unwrap();
    assert_eq!(ast.messages.len(), 2);
    assert_eq!(ast.messages[1], Message::User { parts: vec![InputPart::Text {
        text: "<state>\n{\"type\":\"state_snapshot\",\"state\":{\"cwd\":\"a\"}}\n</state>\n<state>\n{\"type\":\"state_patch\",\"set\":{\"cwd\":\"b\"},\"remove\":[]}\n</state>\n<framework>\nnotice </framework> literal\n</framework>\n".into()
    }] });
    assert!(matches!(
        &content.items[1],
        ContentItem::User {
            source: Some(microcrab_transport::UserSource::Framework),
            ..
        }
    ));
}

#[test]
fn failed_mutations_are_atomic_and_pair_tool_results() {
    let mut context = context();
    context.admit_user("task", &json!({})).unwrap();
    let before = context.items().to_vec();
    assert!(context.admit_user("bad", &Value::Null).is_err());
    assert!(
        context
            .admit_user("x".repeat(20_000), &json!({"changed":true}))
            .is_err()
    );
    assert!(context.compact("x".repeat(20_000), &json!({})).is_err());
    assert_eq!(context.items(), before);
    assert!(
        context
            .append_response(&response(vec![call("same"), call("same")]))
            .is_err()
    );
    assert_eq!(context.items(), before);
    context
        .append_response(&response(vec![call("one"), call("two")]))
        .unwrap();
    assert!(context.content(10).is_err());
    assert!(context.compact("summary".into(), &json!({})).is_err());
    assert!(
        context
            .append_tool_result("missing".into(), "oops".into())
            .is_err()
    );
    let pending = context.items().to_vec();
    assert!(
        context
            .append_tool_result("one".into(), "x".repeat(20_000))
            .is_err()
    );
    assert_eq!(context.items(), pending);
    context
        .append_tool_result("two".into(), "ok".into())
        .unwrap();
    assert!(context.content(10).is_err());
    context
        .append_tool_result("one".into(), "ok".into())
        .unwrap();
    context.content(10).unwrap();
    assert!(
        context
            .append_tool_result("one".into(), "duplicate".into())
            .is_err()
    );
    assert!(
        context
            .append_response(&response(vec![call("one")]))
            .is_err()
    );
}

#[test]
fn tool_and_system_bytes_consume_budget() {
    assert!(Context::new("s".repeat(200), tools(), 100).is_err());
    let mut definitions = tools();
    definitions[0].description = "x".repeat(2000);
    assert!(Context::new("system".into(), definitions, 1000).is_err());
}

async fn mock_server(
    status: &str,
    body: String,
    extra_headers: &str,
) -> (String, tokio::task::JoinHandle<Value>) {
    let (endpoint, server) = mock_server_bytes(status, body.into_bytes(), extra_headers).await;
    let server =
        tokio::spawn(async move { serde_json::from_slice(&server.await.unwrap()).unwrap() });
    (endpoint, server)
}

// Fixtures need complete headers before replying or cancelling, but must not wait
// for an arbitrary buffer to fill while the client is waiting for a response.
async fn read_request_headers(socket: &mut tokio::net::TcpStream) -> (Vec<u8>, usize) {
    const MAX_HEADER_BYTES: usize = 16 * 1024;
    let mut buffer = Vec::new();
    loop {
        let remaining = MAX_HEADER_BYTES - buffer.len();
        assert!(remaining > 0, "fixture request headers exceed the bound");
        let mut chunk = [0; 2048];
        let capacity = remaining.min(chunk.len());
        let count = socket.read(&mut chunk[..capacity]).await.unwrap();
        assert!(count > 0, "fixture request ended before complete headers");
        buffer.extend_from_slice(&chunk[..count]);
        if let Some(end) = buffer.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            return (buffer, end + 4);
        }
    }
}
async fn mock_server_bytes(
    status: &str,
    body: Vec<u8>,
    extra_headers: &str,
) -> (String, tokio::task::JoinHandle<Vec<u8>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let status = status.to_owned();
    let extra_headers = extra_headers.to_owned();
    let handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let (mut buffer, start) = read_request_headers(&mut socket).await;
        let headers = String::from_utf8_lossy(&buffer[..start - 4]);
        let length = headers
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(|value| value.trim().parse::<usize>().unwrap())
            })
            .unwrap();
        while buffer.len() < start + length {
            let mut chunk = [0; 2048];
            let n = socket.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            buffer.extend_from_slice(&chunk[..n]);
        }
        let request = buffer[start..start + length].to_vec();
        let headers = format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n{extra_headers}\r\n",
            body.len()
        );
        socket.write_all(headers.as_bytes()).await.unwrap();
        let _ = socket.write_all(&body).await;
        request
    });
    (format!("http://{address}/v1/chat/completions"), handle)
}
fn config(endpoint: String) -> OpenAiCompatibleConfig {
    OpenAiCompatibleConfig {
        endpoint,
        model: "test-model".into(),
        api_key: Some("secret-credential".into()),
        timeout_ms: 1000,
        max_response_bytes: 4096,
    }
}
fn good_body() -> Value {
    json!({"choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"hello\n  code"}}],"usage":{"prompt_tokens":3,"completion_tokens":4}})
}

#[tokio::test]
async fn provider_materializes_wire_ast_and_validates_calls() {
    let mut context = context();
    context.admit_user("task", &json!({})).unwrap();
    context
        .append_response(&response(vec![call("call")]))
        .unwrap();
    context
        .append_tool_result("call".into(), "output".into())
        .unwrap();
    let (endpoint, server) = mock_server("200 OK", good_body().to_string(), "").await;
    let client = OpenAiCompatible::new(config(endpoint)).unwrap();
    let response = client
        .complete(
            &context.content(50).unwrap(),
            &Auditor::disconnected(),
            &microcrab_auditor::EmptyVariableView,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(response.content, "hello\n  code");
    assert_eq!(response.usage.input_tokens, Some(3));
    let wire = server.await.unwrap();
    assert_eq!(wire["model"], "test-model");
    assert_eq!(wire["stream"], true);
    assert_eq!(wire["stream_options"]["include_usage"], true);
    assert_eq!(wire["messages"].as_array().unwrap().len(), 4);
    assert_eq!(wire["messages"][1]["role"], "user");
    assert_eq!(
        wire["messages"][2]["tool_calls"][0]["function"]["arguments"],
        "{\"text\":\"hello\"}"
    );
    assert_eq!(wire["messages"][3]["tool_call_id"], "call");
    assert_eq!(wire["tools"][0]["function"]["name"], "echo");
    assert!(wire.get("session_id").is_none());

    let body = json!({"choices":[{"finish_reason":"tool_calls","message":{"role":"assistant","content":null,"tool_calls":[{"id":"next","type":"function","function":{"name":"echo","arguments":"{\"text\":\"yes\"}"}}]}}]});
    let (endpoint, server) = mock_server("200 OK", body.to_string(), "").await;
    let response = OpenAiCompatible::new(config(endpoint))
        .unwrap()
        .complete(
            &context.content(50).unwrap(),
            &Auditor::disconnected(),
            &microcrab_auditor::EmptyVariableView,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(response.tool_calls[0].arguments, json!({"text":"yes"}));
    server.await.unwrap();
}

#[tokio::test]
async fn provider_bounds_body_refuses_redirects_and_redacts_secrets() {
    let (endpoint, server) = mock_server("200 OK", "x".repeat(5000), "").await;
    let client = OpenAiCompatible::new(config(endpoint)).unwrap();
    assert!(!format!("{client:?}").contains("secret-credential"));
    assert!(matches!(
        client
            .complete(
                &context().content(10).unwrap(),
                &Auditor::disconnected(),
                &microcrab_auditor::EmptyVariableView,
                CancellationToken::new()
            )
            .await,
        Err(Error::Limit(_))
    ));
    server.await.unwrap();
    let (endpoint, server) = mock_server(
        "302 Found",
        "secret-credential".into(),
        "Location: http://127.0.0.1:1/steal\r\n",
    )
    .await;
    let error = OpenAiCompatible::new(config(endpoint))
        .unwrap()
        .complete(
            &context().content(10).unwrap(),
            &Auditor::disconnected(),
            &microcrab_auditor::EmptyVariableView,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::ProviderStatus { status: 302 }));
    assert!(!error.to_string().contains("secret-credential"));
    server.await.unwrap();
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let client =
        OpenAiCompatible::new(config("http://127.0.0.1:1/chat/completions".into())).unwrap();
    assert!(matches!(
        client
            .complete(
                &context().content(10).unwrap(),
                &Auditor::disconnected(),
                &microcrab_auditor::EmptyVariableView,
                cancelled
            )
            .await,
        Err(Error::Cancelled)
    ));
    assert!(
        OpenAiCompatible::new(config("http://example.org/v1/chat/completions".into())).is_err()
    );
}

#[tokio::test]
async fn provider_limits_chunked_body_and_cancels_inflight_request() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/chat/completions", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request_headers(&mut socket).await;
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
            .await
            .unwrap();
        let body = "x".repeat(5000);
        let wire = format!("{:x}\r\n{}\r\n0\r\n\r\n", body.len(), body);
        let _ = socket.write_all(wire.as_bytes()).await;
    });
    let client = OpenAiCompatible::new(config(endpoint)).unwrap();
    assert!(matches!(
        client
            .complete(
                &context().content(10).unwrap(),
                &Auditor::disconnected(),
                &microcrab_auditor::EmptyVariableView,
                CancellationToken::new()
            )
            .await,
        Err(Error::Limit(_))
    ));
    server.await.unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/chat/completions", listener.local_addr().unwrap());
    let cancellation = CancellationToken::new();
    let token = cancellation.clone();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request_headers(&mut socket).await;
        token.cancel();
        std::future::pending::<()>().await;
    });
    let client = OpenAiCompatible::new(config(endpoint)).unwrap();
    assert!(matches!(
        client
            .complete(
                &context().content(10).unwrap(),
                &Auditor::disconnected(),
                &microcrab_auditor::EmptyVariableView,
                cancellation
            )
            .await,
        Err(Error::Cancelled)
    ));
    server.abort();
    let _ = server.await;
}

fn assert_audit_pair(start: &AuditRecord, end: &AuditRecord, outcome: AuditOutcome) {
    assert_eq!(start.event, Moment::RequestPrepared);
    assert_eq!(end.event, Moment::RoundCompleted);
    assert!(start.call_id.as_ref().is_some_and(|id| !id.is_empty()));
    assert_eq!(start.call_id, end.call_id);
    assert_eq!(start.epoch, end.epoch);
    assert_eq!(start.turn_id, end.turn_id);
    assert_eq!(start.round, end.round);
    assert!(start.started_at_unix_ms.is_some());
    assert_eq!(start.started_at_unix_ms, end.started_at_unix_ms);
    assert!(start.elapsed_ms.is_none());
    assert!(end.elapsed_ms.is_some());
    assert_eq!(end.outcome, Some(outcome));
    assert!(start.response.is_none());
    assert!(end.sent_request.is_none());
    assert_eq!(end.sequence, start.sequence + 1);
}

#[tokio::test]
async fn provider_audits_exact_wire_bodies_and_correlates_each_logical_call() {
    let body = "{\n  \"id\":\"provider-id\", \"choices\":[{\"finish_reason\":\"stop\",\"message\":{\"role\":\"assistant\",\"content\":\"hello\"}}],\n  \"usage\":{\"prompt_tokens\":3,\"completion_tokens\":4,\"provider_only\":17}, \"extra\":true\n}";
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let auditor = Auditor::from_sender(sender);
    let mut content_context = context();
    content_context
        .admit_user("  exact task\n", &json!({}))
        .unwrap();
    let content = content_context.content(50).unwrap();
    let mut previous_call_id: Option<String> = None;
    for _ in 0..2 {
        let (endpoint, server) = mock_server_bytes("200 OK", body.as_bytes().to_vec(), "").await;
        let result = OpenAiCompatible::new(config(endpoint))
            .unwrap()
            .complete(
                &content,
                &auditor,
                &microcrab_auditor::EmptyVariableView,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(result.content, "hello");
        let wire = server.await.unwrap();
        let start = receiver.try_recv().unwrap();
        let end = receiver.try_recv().unwrap();
        assert_audit_pair(&start, &end, AuditOutcome::Accepted);
        assert_eq!(
            start.sent_request.as_ref().unwrap().as_bytes(),
            wire.as_slice()
        );
        assert_eq!(end.response.as_deref(), Some(body));
        assert_eq!(end.response_complete, Some(true));
        assert_eq!(end.response_status, Some(200));
        assert!(end.response_bytes.is_none());
        assert_eq!(end.usage, Some(result.usage.clone()));
        assert_eq!(result.usage.input_tokens, Some(3));
        assert_eq!(result.usage.output_tokens, Some(4));
        assert_eq!(result.usage.total_tokens, Some(7));
        assert_eq!(result.usage.samples, 1);
        assert_eq!(result.usage.provider_details[0]["provider_only"], 17);
        assert!(
            !start
                .sent_request
                .as_ref()
                .unwrap()
                .contains("secret-credential")
        );
        if let Some(previous) = previous_call_id {
            assert_ne!(start.call_id.as_deref(), Some(previous.as_str()));
            assert_eq!(start.request_diff.as_deref(), Some(""));
        }
        previous_call_id = start.call_id;
        assert!(receiver.try_recv().is_err());
    }
}

#[tokio::test]
async fn provider_audits_malformed_and_http_error_bodies_before_decoding() {
    let cases = [
        (
            "200 OK",
            200,
            b"{ malformed JSON\n".to_vec(),
            AuditOutcome::Unavailable,
        ),
        (
            "200 OK",
            200,
            br#"{"choices":[],"extra":"retained"}"#.to_vec(),
            AuditOutcome::Unavailable,
        ),
        (
            "429 Too Many Requests",
            429,
            b"{\n\"error\":{\"message\":\"private-provider-error\"}\n}".to_vec(),
            AuditOutcome::Unavailable,
        ),
        (
            "200 OK",
            200,
            vec![b'{', 0xff, b'}'],
            AuditOutcome::Unavailable,
        ),
    ];
    for (status, status_code, body, outcome) in cases {
        let (endpoint, server) = mock_server_bytes(status, body.clone(), "").await;
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let auditor = Auditor::from_sender(sender);
        let error = OpenAiCompatible::new(config(endpoint))
            .unwrap()
            .complete(
                &context().content(10).unwrap(),
                &auditor,
                &microcrab_auditor::EmptyVariableView,
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert!(!error.to_string().contains("private-provider-error"));
        assert!(!error.to_string().contains("secret-credential"));
        server.await.unwrap();
        let start = receiver.try_recv().unwrap();
        let end = receiver.try_recv().unwrap();
        assert_audit_pair(&start, &end, outcome);
        assert_eq!(end.response_complete, Some(true));
        assert_eq!(end.response_status, Some(status_code));
        match String::from_utf8(body.clone()) {
            Ok(text) => {
                assert_eq!(end.response, Some(text));
                assert!(end.response_bytes.is_none());
            }
            Err(_) => {
                assert!(end.response.is_none());
                assert_eq!(end.response_bytes, Some(body));
            }
        }
        assert!(receiver.try_recv().is_err());
    }
}

#[tokio::test]
async fn provider_audit_marks_bounded_response_prefix_incomplete() {
    let body = "x".repeat(5000);
    let (endpoint, server) = mock_server("200 OK", body.clone(), "").await;
    let mut config = config(endpoint);
    config.max_response_bytes = 32;
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let auditor = Auditor::from_sender(sender);
    assert!(matches!(
        OpenAiCompatible::new(config)
            .unwrap()
            .complete(
                &context().content(10).unwrap(),
                &auditor,
                &microcrab_auditor::EmptyVariableView,
                CancellationToken::new()
            )
            .await,
        Err(Error::Limit(_))
    ));
    server.await.unwrap();
    let start = receiver.try_recv().unwrap();
    let end = receiver.try_recv().unwrap();
    assert_audit_pair(&start, &end, AuditOutcome::LimitExceeded);
    assert_eq!(end.response.as_deref(), Some(&body[..32]));
    assert_eq!(end.response_complete, Some(false));
    assert_eq!(end.response_status, Some(200));
    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn dropping_inflight_provider_future_synchronously_emits_terminal_audit() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/chat/completions", listener.local_addr().unwrap());
    let (ready, received) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        assert!(socket.read(&mut request).await.unwrap() > 0);
        ready.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let auditor = Auditor::from_sender(sender);
    let client = OpenAiCompatible::new(config(endpoint)).unwrap();
    let content = context().content(10).unwrap();
    let mut pending = Box::pin(client.complete(
        &content,
        &auditor,
        &microcrab_auditor::EmptyVariableView,
        CancellationToken::new(),
    ));
    tokio::select! {
        result = &mut pending => panic!("provider finished unexpectedly: {result:?}"),
        result = received => result.unwrap(),
    }
    let start = receiver.try_recv().unwrap();
    assert!(receiver.try_recv().is_err());
    drop(pending);
    let end = receiver.try_recv().unwrap();
    assert_audit_pair(&start, &end, AuditOutcome::Cancelled);
    assert!(end.response.is_none());
    assert!(end.response_complete.is_none());
    assert!(receiver.try_recv().is_err());
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn precancelled_call_and_transport_failure_have_distinct_terminal_outcomes() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/chat/completions", listener.local_addr().unwrap());
    // Refused connections can time out on Windows. A live loopback peer returning
    // an invalid HTTP status line deterministically exercises a non-timeout failure.
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        assert!(socket.read(&mut request).await.unwrap() > 0);
        socket
            .write_all(b"invalid HTTP response\r\n\r\n")
            .await
            .unwrap();
        socket.shutdown().await.unwrap();
    });
    for cancelled in [true, false] {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let auditor = Auditor::from_sender(sender);
        let cancellation = CancellationToken::new();
        if cancelled {
            cancellation.cancel();
        }
        let error = OpenAiCompatible::new(config(endpoint.clone()))
            .unwrap()
            .complete(
                &context().content(10).unwrap(),
                &auditor,
                &microcrab_auditor::EmptyVariableView,
                cancellation,
            )
            .await
            .unwrap_err();
        if cancelled {
            assert!(matches!(error, Error::Cancelled));
            assert!(
                receiver.try_recv().is_err(),
                "pre-cancelled calls have no sent request to audit"
            );
            continue;
        }
        let Error::TransportFailure(cause) = &error else {
            panic!("expected cause-bearing network failure: {error:?}")
        };
        let reason = cause.public_reason().expect("network cause must be public");
        assert!(reason.contains("invalid HTTP version"), "{reason}");
        let outcome = AuditOutcome::Unavailable;
        let start = receiver.try_recv().unwrap();
        let end = receiver.try_recv().unwrap();
        assert_audit_pair(&start, &end, outcome);
        assert!(end.response.is_none());
        assert!(end.response_bytes.is_none());
        assert!(end.response_status.is_none());
        assert!(end.response_complete.is_none());
        assert!(receiver.try_recv().is_err());
    }
    server.await.unwrap();
}

#[tokio::test]
async fn disconnecting_audit_consumer_does_not_fail_provider_call() {
    let (endpoint, server) = mock_server("200 OK", good_body().to_string(), "").await;
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let auditor = Auditor::from_sender(sender);
    drop(receiver);
    let response = OpenAiCompatible::new(config(endpoint))
        .unwrap()
        .complete(
            &context().content(10).unwrap(),
            &auditor,
            &microcrab_auditor::EmptyVariableView,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(response.content, "hello\n  code");
    server.await.unwrap();
}

#[tokio::test]
async fn provider_timeout_bounds_a_stalled_body() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/chat/completions", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request_headers(&mut socket).await;
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n{partial")
            .await
            .unwrap();
        std::future::pending::<()>().await;
    });
    let mut config = config(endpoint);
    config.timeout_ms = 100;
    let client = OpenAiCompatible::new(config).unwrap();
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let auditor = Auditor::from_sender(sender);
    assert!(matches!(
        client
            .complete(
                &context().content(10).unwrap(),
                &auditor,
                &microcrab_auditor::EmptyVariableView,
                CancellationToken::new()
            )
            .await,
        Err(Error::ProviderIdleTimeout(details)) if details.timeout_ms == 100 && details.streamed_bytes == 8 && details.streamed_chars == 0 && details.first_token_ms.is_none()
    ));
    let start = receiver.try_recv().unwrap();
    let end = receiver.try_recv().unwrap();
    assert_audit_pair(&start, &end, AuditOutcome::TimedOut);
    assert_eq!(end.response.as_deref(), Some("{partial"));
    assert_eq!(end.response_complete, Some(false));
    assert_eq!(end.response_status, Some(200));
    assert!(receiver.try_recv().is_err());
    server.abort();
    let _ = server.await;
}

struct RewriteHook {
    mode: &'static str,
    sink: Auditor,
    prepared: std::sync::Mutex<Vec<String>>,
    completed: std::sync::Mutex<Vec<(Option<String>, Option<u16>)>>,
}
impl microcrab_auditor::Auditor for RewriteHook {
    fn is_enabled(&self) -> bool {
        true
    }
    fn publish(&self, moment: Moment, body: microcrab_auditor::AuditBody) {
        self.sink.publish(moment, body);
    }
    fn intervene<'a>(
        &'a self,
        moment: Moment,
        ctx: microcrab_auditor::HookContext<'a>,
    ) -> microcrab_auditor::BoxFuture<
        'a,
        Result<microcrab_auditor::Injection, microcrab_auditor::AuditorError>,
    > {
        use microcrab_auditor::{HookPayload, Injection};
        Box::pin(async move {
            if moment == Moment::RoundCompleted {
                let HookPayload::RoundCompleted {
                    response,
                    status,
                    usage,
                } = ctx.payload
                else {
                    panic!("logical round payload required")
                };
                assert!(response.is_some());
                assert!(usage.is_some());
                self.completed.lock().unwrap().push((ctx.call_id, status));
            }
            if moment != Moment::RequestPrepared {
                return Ok(Injection::default());
            }
            let authored = ctx
                .request_body
                .expect("prepared hook receives authored wire body");
            self.prepared.lock().unwrap().push(authored.into());
            let mut body: Value = serde_json::from_str(authored).unwrap();
            match self.mode {
                "valid" => body["temperature"] = json!(0.125),
                "system" => body["messages"][0]["content"] = json!("forged system"),
                "tools" => {
                    body["tools"] = json!([{"type":"function","function":{"name":"forged"}}])
                }
                "shape" => body["messages"] = json!({}),
                _ => unreachable!(),
            }
            Ok(Injection {
                variables: microcrab_protocol::VariablesChange {
                    scoped_set: vec![],
                    set: json!({"from_prepared_hook":true})
                        .as_object()
                        .unwrap()
                        .clone(),
                    remove: vec![],
                },
                request_body: Some(body.to_string()),
                ..Default::default()
            })
        })
    }
}

#[tokio::test]
async fn rename_transport_keeps_functional_identity_in_hooks_and_both_wire_records() {
    use microcrab_session::{
        Session,
        runtime::{Runtime, tool::DenyAll},
    };
    use std::sync::Arc;
    let mut body = good_body();
    body["choices"][0]["message"]["content"] = json!("Session title");
    let (endpoint, server) = mock_server_bytes("200 OK", body.to_string().into_bytes(), "").await;
    let hook = Arc::new(hooks::Recording::default());
    let client = OpenAiCompatible::new(config(endpoint)).unwrap();
    let session = Session::builder(client, Runtime::new(vec![], Arc::new(DenyAll)).unwrap())
        .auditor(hook.clone())
        .build()
        .unwrap();
    session.start().await.unwrap();
    let before = session.checkpoint().await.unwrap();
    hook.clear();
    session.rename_with_model("Name the session").await.unwrap();
    let wire = server.await.unwrap();
    let sent: Value = serde_json::from_slice(&wire).unwrap();
    assert!(sent.get("subagent").is_none());
    {
        let observed = hook.seen.lock().unwrap();
        assert_eq!(
            observed.iter().map(|seen| seen.moment).collect::<Vec<_>>(),
            vec![
                Moment::SessionCreated,
                Moment::RequestPrepared,
                Moment::RoundCompleted,
                Moment::SessionTerminated
            ]
        );
        for seen in observed.iter() {
            assert_eq!(seen.subagent.as_deref(), Some("rename"));
            assert_eq!(
                (seen.epoch, seen.turn, seen.round),
                (before.state.epoch_id, None, None)
            );
        }
        assert_eq!(observed[1].call_id, observed[2].call_id);
    }
    {
        let published = hook.published.lock().unwrap();
        assert_eq!(published.len(), 4);
        assert_eq!(
            published[1].1.sent_request.as_ref().unwrap().as_bytes(),
            wire
        );
        for (_, body) in published.iter() {
            assert_eq!(body.subagent.as_deref(), Some("rename"));
            assert_eq!((body.turn_id, body.round), (None, None));
        }
    }
    let after = session.checkpoint().await.unwrap();
    assert_eq!(
        serde_json::to_value(before).unwrap(),
        serde_json::to_value(after).unwrap()
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn prepared_rewrite_audits_only_sent_body_and_round_hook_is_once_with_correlated_status() {
    use microcrab_session::{
        Session,
        runtime::{Runtime, tool::DenyAll},
    };
    use std::sync::{Arc, Mutex};
    let (endpoint, server) =
        mock_server_bytes("200 OK", good_body().to_string().into_bytes(), "").await;
    let (sender, mut records) = tokio::sync::mpsc::unbounded_channel();
    let hook = Arc::new(RewriteHook {
        mode: "valid",
        sink: Auditor::from_sender(sender),
        prepared: Mutex::new(vec![]),
        completed: Mutex::new(vec![]),
    });
    let client = OpenAiCompatible::new(config(endpoint)).unwrap();
    let session = Session::builder(client, Runtime::new(vec![], Arc::new(DenyAll)).unwrap())
        .auditor(hook.clone())
        .build()
        .unwrap();
    session.submit("request rewrite").await.unwrap();
    let wire = server.await.unwrap();
    let sent: Value = serde_json::from_slice(&wire).unwrap();
    assert_eq!(sent["temperature"], 0.125);
    assert_eq!(
        session.state_handle().extract("from_prepared_hook"),
        Some(json!(true))
    );
    // Returning variables does not implicitly mutate this already-authored request.
    assert!(!String::from_utf8_lossy(&wire).contains("from_prepared_hook"));
    {
        let authored = hook.prepared.lock().unwrap();
        assert_eq!(authored.len(), 1);
        assert!(
            serde_json::from_str::<Value>(&authored[0])
                .unwrap()
                .get("temperature")
                .is_none()
        );
    }
    let records = std::iter::from_fn(|| records.try_recv().ok()).collect::<Vec<_>>();
    let starts = records
        .iter()
        .filter(|record| record.event == Moment::RequestPrepared)
        .collect::<Vec<_>>();
    assert_eq!(starts.len(), 1);
    assert_eq!(starts[0].sent_request.as_ref().unwrap().as_bytes(), wire);
    {
        let completed = hook.completed.lock().unwrap();
        assert_eq!(
            completed.as_slice(),
            &[(starts[0].call_id.clone(), Some(200))]
        );
    }
    session.close().await.unwrap();
}

#[tokio::test]
async fn rejected_request_rewrite_never_commits_returned_variables_or_sends_http() {
    use microcrab_session::{
        Session,
        runtime::{Runtime, tool::DenyAll},
    };
    use std::sync::{Arc, Mutex};
    for mode in ["system", "tools", "shape"] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/chat/completions", listener.local_addr().unwrap());
        let (sender, _records) = tokio::sync::mpsc::unbounded_channel();
        let hook = Arc::new(RewriteHook {
            mode,
            sink: Auditor::from_sender(sender),
            prepared: Mutex::new(vec![]),
            completed: Mutex::new(vec![]),
        });
        let client = OpenAiCompatible::new(config(endpoint)).unwrap();
        let session = Session::builder(client, Runtime::new(vec![], Arc::new(DenyAll)).unwrap())
            .auditor(hook.clone())
            .build()
            .unwrap();
        session
            .state_handle()
            .inject("retained", json!(7))
            .await
            .unwrap();
        assert!(
            matches!(
                session.submit("must reject rewrite").await,
                Err(microcrab_session::Error::Lifecycle(_))
            ),
            "{mode}"
        );
        assert_eq!(hook.prepared.lock().unwrap().len(), 1);
        assert_eq!(session.state_handle().variables(), json!({"retained":7}));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
        session.close().await.unwrap();
    }
}
