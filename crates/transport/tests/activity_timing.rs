//! Real HTTP adapter regression: SSE clocks must come from decoded wire deltas,
//! while aggregate JSON previews must not manufacture streaming measurements.
use microcrab_auditor::{AuditOutcome, ChannelAuditor, EmptyVariableView, Moment};
use microcrab_transport::{Client, Content, Protocol, TransportClient, TransportConfig};
use std::{
    io::{Read, Write},
    net::TcpListener,
    time::Duration,
};
use tokio_util::sync::CancellationToken;

#[path = "activity_timing/idle.rs"]
mod idle;
#[path = "activity_timing/openai_compatibility.rs"]
mod openai_compatibility;

struct ActivityObserver;
impl microcrab_transport::ModelObserver for ActivityObserver {
    fn publish(&self, _: microcrab_transport::ModelObservation) {}
}

fn serve(media_type: &str, body: &str) -> (String, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {media_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let worker = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut received = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            let read = socket.read(&mut buffer).unwrap();
            assert!(read > 0, "request ended before body");
            received.extend_from_slice(&buffer[..read]);
            if let Some(header_end) = received.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                let headers = std::str::from_utf8(&received[..header_end]).unwrap();
                let length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                if received.len() >= header_end + 4 + length {
                    break;
                }
            }
        }
        socket.write_all(response.as_bytes()).unwrap();
    });
    (endpoint, worker)
}

#[tokio::test]
async fn actual_adapter_sse_events_reach_terminal_audit_without_preview_subscribers() {
    let fixtures = [
        (
            Protocol::OpenaiCompatible,
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"a\"},\"finish_reason\":null}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n",
        ),
        (
            Protocol::OpenaiResponses,
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"a\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n",
        ),
        (
            Protocol::Anthropic,
            "data: {\"type\":\"message_start\",\"message\":{}}\n\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"a\"}}\n\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\ndata: {\"type\":\"message_stop\"}\n\n",
        ),
        (
            Protocol::Google,
            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"a\"}]},\"finishReason\":\"STOP\"}]}\n\n",
        ),
    ];
    for (protocol, body) in fixtures {
        let terminal = complete(protocol, "text/event-stream", body, true).await;
        assert_eq!(
            terminal.outcome,
            Some(AuditOutcome::Accepted),
            "{protocol:?}"
        );
        assert_eq!(
            terminal.timing_source.as_deref(),
            Some("transport_stream"),
            "{protocol:?}"
        );
        let first = terminal.first_token_ms.unwrap();
        let generation = terminal.generation_ms.unwrap();
        assert!(first + generation <= terminal.elapsed_ms.unwrap());
        assert_eq!(terminal.usage.unwrap().output_tokens, None);
    }
}

#[tokio::test]
async fn json_fallback_previews_do_not_create_first_content_or_generation_clocks() {
    let fixtures = [
        (
            Protocol::OpenaiCompatible,
            r#"{"choices":[{"message":{"role":"assistant","content":"a"},"finish_reason":"stop"}]}"#,
        ),
        (
            Protocol::OpenaiResponses,
            r#"{"output":[{"type":"message","content":[{"type":"output_text","text":"a"}]}]}"#,
        ),
        (
            Protocol::Anthropic,
            r#"{"content":[{"type":"text","text":"a"}]}"#,
        ),
        (
            Protocol::Google,
            r#"{"candidates":[{"content":{"parts":[{"text":"a"}]},"finishReason":"STOP"}]}"#,
        ),
    ];
    for (protocol, body) in fixtures {
        for observer in [false, true] {
            let terminal = complete(protocol, "application/json", body, observer).await;
            assert_eq!(
                terminal.outcome,
                Some(AuditOutcome::Accepted),
                "{protocol:?}"
            );
            assert!(terminal.first_token_ms.is_none());
            assert!(terminal.generation_ms.is_none());
            assert!(terminal.timing_source.is_none());
            assert_eq!(terminal.usage.unwrap().output_tokens, None);
        }
    }
}

async fn complete(
    protocol: Protocol,
    media_type: &str,
    body: &str,
    observer: bool,
) -> microcrab_auditor::AuditRecord {
    let (endpoint, worker) = serve(media_type, body);
    let client = TransportClient::new(TransportConfig {
        auth: Default::default(),
        protocol,
        endpoint,
        model: "test".into(),
        api_key: None,
        timeout_ms: 5000,
        max_response_bytes: 65536,
    })
    .unwrap();
    let client = if observer {
        client.with_observer(std::sync::Arc::new(ActivityObserver))
    } else {
        client
    };
    let content = Content {
        epoch: 1,
        subagent: None,
        system: "system".into(),
        items: if protocol == Protocol::Google {
            vec![microcrab_transport::ContentItem::User {
                envelope: None,
                source: None,
                parts: vec![microcrab_protocol::InputPart::Text {
                    text: "hello".into(),
                }],
            }]
        } else {
            vec![]
        },
        tools: vec![],
        max_output_tokens: 32,
        max_bytes: 65536,
        turn_id: Some(1),
        round: Some(1),
    };
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let auditor = ChannelAuditor::from_sender(sender);
    let response = client
        .stream_admitted_outcome(
            &content,
            &auditor,
            &EmptyVariableView,
            CancellationToken::new(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(response.response.content, "a", "{protocol:?}");
    let attempt = response.attempt.unwrap();
    worker.join().unwrap();
    let prepared = receiver.try_recv().unwrap();
    let terminal = receiver.try_recv().unwrap();
    assert_eq!(prepared.event, Moment::RequestPrepared);
    assert_eq!(terminal.event, Moment::RoundCompleted);
    assert_eq!(prepared.call_id, terminal.call_id);
    assert!(prepared.response_completed_at_ms.is_none());
    assert_eq!(terminal.call_id.as_deref(), Some(attempt.call_id.as_str()));
    let sample = attempt.response_completed_at_ms.unwrap().get();
    assert_eq!(terminal.response_completed_at_ms, Some(sample));
    assert_eq!(
        terminal.started_at_unix_ms.unwrap() + terminal.elapsed_ms.unwrap(),
        sample
    );
    assert!(receiver.try_recv().is_err());
    terminal
}
