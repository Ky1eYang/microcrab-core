//! The display recording port must not change real provider request bytes,
//! including the next request and Google's existing native thought metadata.
use microcrab_auditor::{EmptyVariableView, NoopAuditor};
use microcrab_protocol::InputPart;
use microcrab_transport::{
    Client, Content, ContentItem, ModelObservation, ModelRecording, ObservationState, Protocol,
    TransportClient, TransportConfig,
};
use serde_json::json;
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_util::sync::CancellationToken;
#[path = "display_wire_invariance/cancellation.rs"]
mod cancellation;

#[derive(Default)]
struct Recorder(Mutex<Vec<ModelObservation>>);
#[async_trait::async_trait]
impl ModelRecording for Recorder {
    async fn record(&self, event: ModelObservation) -> Result<(), String> {
        self.0.lock().unwrap().push(event);
        Ok(())
    }
    fn dropped(&self, event: ModelObservation) {
        self.0.lock().unwrap().push(event);
    }
}
fn fixture(protocol: Protocol, thought: &str) -> String {
    match protocol {
        Protocol::OpenaiCompatible => format!(
            "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
            json!({"choices":[{"index":0,"delta":{"reasoning_content":thought,"content":"answer"},"finish_reason":null}]}),
            json!({"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]})
        ),
        Protocol::OpenaiResponses => format!(
            "data: {}\n\ndata: {}\n\ndata: {}\n\n",
            json!({"type":"response.reasoning_summary_text.delta","delta":thought}),
            json!({"type":"response.output_text.delta","delta":"answer"}),
            json!({"type":"response.completed","response":{"status":"completed"}})
        ),
        Protocol::Anthropic => [
            json!({"type":"message_start","message":{}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":thought}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}),
            json!({"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"answer"}}),
            json!({"type":"content_block_stop","index":1}),
            json!({"type":"message_stop"})
        ].iter().map(|event|format!("data: {event}\n\n")).collect(),
        Protocol::Google => format!(
            "data: {}\n\ndata: {}\n\n",
            json!({"candidates":[{"index":0,"content":{"role":"model","parts":[{"thought":true,"text":thought},{"text":"answer"}]}}]}),
            json!({"candidates":[{"index":0,"finishReason":"STOP"}]})
        ),
    }
}
fn json_fixture(protocol: Protocol, thought: &str, answer: &str) -> String {
    let split = thought
        .char_indices()
        .nth(thought.chars().count() / 2)
        .map_or(thought.len(), |(index, _)| index);
    match protocol {
        Protocol::OpenaiCompatible => json!({"choices":[{"message":{"role":"assistant","content":answer,"reasoning_content":thought},"finish_reason":"stop"}]}),
        Protocol::OpenaiResponses => json!({"status":"completed","output":[{"type":"reasoning","summary":[{"type":"summary_text","text":&thought[..split]}],"content":[{"type":"reasoning_text","text":&thought[split..]}],"encrypted_content":"opaque-encrypted-marker"},{"type":"message","role":"assistant","content":[{"type":"output_text","text":answer}]}]}),
        Protocol::Anthropic => json!({"type":"message","role":"assistant","content":[{"type":"thinking","thinking":thought,"signature":"opaque-signature-marker"},{"type":"redacted_thinking","data":"opaque-redacted-marker"},{"type":"text","text":answer}],"stop_reason":"end_turn"}),
        Protocol::Google => json!({"candidates":[{"index":0,"content":{"role":"model","parts":[{"thought":true,"text":thought},{"text":answer}]},"finishReason":"STOP"}]}),
    }.to_string()
}
fn read_body(socket: &mut std::net::TcpStream) -> Vec<u8> {
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let read = socket.read(&mut buffer).unwrap();
        assert!(read > 0, "request ended before complete body");
        bytes.extend_from_slice(&buffer[..read]);
        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            let length = std::str::from_utf8(&bytes[..end])
                .unwrap()
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            if bytes.len() >= end + 4 + length {
                return bytes[end + 4..end + 4 + length].to_vec();
            }
        }
    }
}

async fn two_rounds(
    protocol: Protocol,
    thought: &str,
    recording: Option<Arc<Recorder>>,
    json_response: bool,
) -> (Vec<Vec<u8>>, Vec<serde_json::Value>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
    let body = if json_response {
        json_fixture(protocol, thought, "answer")
    } else {
        fixture(protocol, thought)
    };
    let content_type = if json_response {
        "application/json"
    } else {
        "text/event-stream"
    };
    let worker = std::thread::spawn(move || {
        let mut requests = Vec::new();
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().unwrap();
            requests.push(read_body(&mut socket));
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            ).unwrap();
        }
        requests
    });
    let mut client = TransportClient::new(TransportConfig {
        auth: Default::default(),
        protocol,
        endpoint,
        model: "synthetic-model".into(),
        api_key: None,
        timeout_ms: 10_000,
        max_response_bytes: 4 * 1024 * 1024,
    })
    .unwrap();
    if let Some(recording) = recording {
        client = client.with_recording(recording);
    }
    let mut content = Content {
        epoch: 3,
        subagent: None,
        system: "Frozen synthetic system".into(),
        items: vec![ContentItem::User {
            envelope: None,
            source: None,
            parts: vec![InputPart::Text {
                text: "First question".into(),
            }],
        }],
        tools: vec![],
        max_output_tokens: 128,
        max_bytes: 4 * 1024 * 1024,
        turn_id: Some(1),
        round: Some(1),
    };
    let mut responses = Vec::new();
    for round in 1..=2 {
        content.round = Some(round);
        let response = client
            .complete(
                &content,
                &NoopAuditor,
                &EmptyVariableView,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(response.content, "answer", "{protocol:?}");
        responses.push(serde_json::to_value(&response).unwrap());
        content.items.push(ContentItem::Assistant {
            content: response.content,
            tool_calls: response.tool_calls,
            provider_metadata: response.provider_metadata,
        });
        content.items.push(ContentItem::User {
            envelope: None,
            source: None,
            parts: vec![InputPart::Text {
                text: "Next question".into(),
            }],
        });
    }
    (worker.join().unwrap(), responses)
}

#[tokio::test]
async fn complete_display_thinking_cannot_change_initial_or_next_actual_wire_request() {
    let thought = "synthetic private thought 界🦀\n".repeat(12_000);
    assert!(thought.len() > 256 * 1024);
    for protocol in [
        Protocol::OpenaiCompatible,
        Protocol::OpenaiResponses,
        Protocol::Anthropic,
        Protocol::Google,
    ] {
        let baseline = two_rounds(protocol, &thought, None, false).await;
        let recording = Arc::new(Recorder::default());
        let recorded = two_rounds(protocol, &thought, Some(recording.clone()), false).await;
        assert_eq!(
            baseline.0, recorded.0,
            "actual HTTP body bytes changed: {protocol:?}"
        );
        assert_eq!(
            baseline.1, recorded.1,
            "native response metadata changed: {protocol:?}"
        );
        let events = recording.0.lock().unwrap();
        let reasoning = events
            .iter()
            .filter_map(|event| match &event.state {
                ObservationState::ReasoningDelta { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<String>();
        assert_eq!(
            reasoning,
            thought.repeat(2),
            "full display stream: {protocol:?}"
        );
        let next: serde_json::Value = serde_json::from_slice(&recorded.0[1]).unwrap();
        assert!(
            next.to_string().contains("synthetic private thought"),
            "neutral semantic reasoning must survive next request: {protocol:?}"
        );
    }
}

#[tokio::test]
async fn json_fallback_display_thinking_cannot_change_initial_or_next_actual_wire_request() {
    let thought = "synthetic private thought 界🦀\n".repeat(12_000);
    assert!(thought.len() > 256 * 1024);
    for protocol in [
        Protocol::OpenaiCompatible,
        Protocol::OpenaiResponses,
        Protocol::Anthropic,
        Protocol::Google,
    ] {
        let baseline = two_rounds(protocol, &thought, None, true).await;
        let recording = Arc::new(Recorder::default());
        let recorded = two_rounds(protocol, &thought, Some(recording.clone()), true).await;
        assert_eq!(
            baseline.0, recorded.0,
            "actual HTTP body bytes changed: {protocol:?}"
        );
        assert_eq!(
            baseline.1, recorded.1,
            "native response metadata changed: {protocol:?}"
        );
        let events = recording.0.lock().unwrap();
        let reasoning = events
            .iter()
            .filter_map(|event| match &event.state {
                ObservationState::ReasoningDelta { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<String>();
        assert_eq!(
            reasoning,
            thought.repeat(2),
            "full display stream: {protocol:?}"
        );
        let next: serde_json::Value = serde_json::from_slice(&recorded.0[1]).unwrap();
        assert!(
            next.to_string().contains("synthetic private thought"),
            "neutral semantic reasoning must survive next request: {protocol:?}"
        );
    }
}

#[tokio::test]
async fn rejected_json_response_never_publishes_its_reasoning_as_display_evidence() {
    for protocol in [
        Protocol::OpenaiCompatible,
        Protocol::OpenaiResponses,
        Protocol::Anthropic,
        Protocol::Google,
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
        // Complete syntax, invalid model result: no answer and no valid tool call.
        let body = json_fixture(protocol, "must not publish rejected reasoning", "");
        let worker = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let _ = read_body(&mut socket);
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        });
        let recorder = Arc::new(Recorder::default());
        let client = TransportClient::new(TransportConfig {
            auth: Default::default(),
            protocol,
            endpoint,
            model: "synthetic-model".into(),
            api_key: None,
            timeout_ms: 10_000,
            max_response_bytes: 4 * 1024 * 1024,
        })
        .unwrap()
        .with_recording(recorder.clone());
        let content = Content {
            epoch: 1,
            subagent: None,
            system: "synthetic".into(),
            items: vec![ContentItem::User {
                envelope: None,
                source: None,
                parts: vec![InputPart::Text {
                    text: "question".into(),
                }],
            }],
            tools: vec![],
            max_output_tokens: 128,
            max_bytes: 4 * 1024 * 1024,
            turn_id: Some(1),
            round: Some(1),
        };
        assert!(
            client
                .complete(
                    &content,
                    &NoopAuditor,
                    &EmptyVariableView,
                    CancellationToken::new()
                )
                .await
                .is_err(),
            "{protocol:?}"
        );
        worker.join().unwrap();
        let events = recorder.0.lock().unwrap();
        assert!(
            !events
                .iter()
                .any(|event| matches!(&event.state, ObservationState::ReasoningDelta { .. })),
            "rejected JSON reasoning leaked: {protocol:?}"
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(&event.state, ObservationState::Invalidated { .. })),
            "missing invalidation: {protocol:?}"
        );
    }
}
