//! Controlled HTTP waits exercise real adapters, including heartbeat-only progress.
use super::*;
use microcrab_transport::{ModelObservation, ModelObserver, ObservationState};
use std::sync::{Arc, Mutex, mpsc};

fn fixtures() -> [(Protocol, &'static str, &'static str); 4] {
    [
        (
            Protocol::OpenaiCompatible,
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"a\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n",
            r#"{"choices":[{"message":{"role":"assistant","content":"a"},"finish_reason":"stop"}]}"#,
        ),
        (
            Protocol::OpenaiResponses,
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"a\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n",
            r#"{"output":[{"type":"message","content":[{"type":"output_text","text":"a"}]}]}"#,
        ),
        (
            Protocol::Anthropic,
            "data: {\"type\":\"message_start\",\"message\":{}}\n\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"a\"}}\n\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\ndata: {\"type\":\"message_stop\"}\n\n",
            r#"{"content":[{"type":"text","text":"a"}]}"#,
        ),
        (
            Protocol::Google,
            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"a\"}]},\"finishReason\":\"STOP\"}]}\n\n",
            r#"{"candidates":[{"content":{"parts":[{"text":"a"}]},"finishReason":"STOP"}]}"#,
        ),
    ]
}

#[derive(Default)]
struct Observed {
    records: Mutex<Vec<ModelObservation>>,
    progress: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}
impl ModelObserver for Observed {
    fn publish(&self, observation: ModelObservation) {
        if matches!(observation.state, ObservationState::TextDelta { .. })
            && let Some(progress) = self.progress.lock().unwrap().take()
        {
            let _ = progress.send(());
        }
        self.records.lock().unwrap().push(observation);
    }
}

fn content(protocol: Protocol) -> Content {
    Content {
        epoch: 1,
        subagent: None,
        system: "system".into(),
        items: if protocol == Protocol::Google {
            vec![microcrab_transport::ContentItem::User {
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
    }
}

/// `hold` waits after writing chunks; `headers` false simulates no initial response.
/// A release channel prevents the server thread from outliving a completed call.
fn chunks_server(
    media_type: &str,
    chunks: Vec<String>,
    spacing: Duration,
    hold: bool,
    headers: bool,
) -> (String, mpsc::Sender<()>, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
    let media_type = media_type.to_owned();
    let (release, released) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket.set_nodelay(true).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut received = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            let count = socket.read(&mut buffer).unwrap();
            assert!(count > 0);
            received.extend_from_slice(&buffer[..count]);
            if let Some(end) = received.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                let headers = std::str::from_utf8(&received[..end]).unwrap();
                let length = headers
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                if received.len() >= end + 4 + length {
                    break;
                }
            }
        }
        if headers {
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: {media_type}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").unwrap();
            for (index, chunk) in chunks.into_iter().enumerate() {
                if index > 0 {
                    std::thread::sleep(spacing);
                }
                if write!(socket, "{:x}\r\n{chunk}\r\n", chunk.len()).is_err() {
                    return;
                }
                if socket.flush().is_err() {
                    return;
                }
            }
        }
        if hold || !headers {
            let _ = released.recv_timeout(Duration::from_secs(5));
        } else {
            let _ = socket.write_all(b"0\r\n\r\n");
        }
    });
    (endpoint, release, worker)
}

fn client(
    protocol: Protocol,
    endpoint: String,
    idle_ms: u64,
    observer: Arc<Observed>,
) -> TransportClient {
    TransportClient::new(TransportConfig {
        auth: Default::default(),
        protocol,
        endpoint,
        model: "test".into(),
        api_key: None,
        timeout_ms: idle_ms,
        max_response_bytes: 65536,
    })
    .unwrap()
    .with_observer(observer)
}

#[tokio::test]
async fn all_protocols_renew_idle_on_heartbeat_and_nonstream_body_chunks_without_total_cap() {
    for (protocol, sse, json) in fixtures() {
        for streaming in [true, false] {
            let chunks = if streaming {
                let mut chunks = vec![sse.to_owned()];
                chunks.extend((0..8).map(|_| ": heartbeat\n\n".to_owned()));
                chunks
            } else {
                json.as_bytes()
                    .chunks(json.len().div_ceil(10))
                    .map(|chunk| std::str::from_utf8(chunk).unwrap().to_owned())
                    .collect()
            };
            let expected_bytes: usize = chunks.iter().map(String::len).sum();
            let (endpoint, release, worker) = chunks_server(
                if streaming {
                    "text/event-stream"
                } else {
                    "application/json"
                },
                chunks,
                Duration::from_millis(100),
                false,
                true,
            );
            let observer = Arc::new(Observed::default());
            let client = client(protocol, endpoint, 500, observer.clone());
            let (sender, mut records) = tokio::sync::mpsc::unbounded_channel();
            let auditor = ChannelAuditor::from_sender(sender);
            let response = client
                .complete(
                    &content(protocol),
                    &auditor,
                    &EmptyVariableView,
                    CancellationToken::new(),
                )
                .await
                .unwrap();
            assert_eq!(response.content, "a", "{protocol:?}, streaming={streaming}");
            let _ = release.send(());
            worker.join().unwrap();
            let prepared = records.try_recv().unwrap();
            let completed = records.try_recv().unwrap();
            assert_eq!(prepared.call_id, completed.call_id);
            assert_eq!(completed.outcome, Some(AuditOutcome::Accepted));
            assert_eq!(completed.response.unwrap().len(), expected_bytes);
            assert!(records.try_recv().is_err());
            assert!(matches!(
                observer.records.lock().unwrap().last().unwrap().state,
                ObservationState::Validated { .. }
            ));
        }
    }
}

#[tokio::test]
async fn all_protocols_headers_or_body_silence_finish_timed_out_audit_and_observation() {
    for (protocol, sse, _) in fixtures() {
        for headers in [false, true] {
            let (endpoint, release, worker) = chunks_server(
                "text/event-stream",
                vec![sse.into()],
                Duration::ZERO,
                true,
                headers,
            );
            let observer = Arc::new(Observed::default());
            let client = client(protocol, endpoint, 400, observer.clone());
            let (sender, mut records) = tokio::sync::mpsc::unbounded_channel();
            let auditor = ChannelAuditor::from_sender(sender);
            let result = client
                .complete(
                    &content(protocol),
                    &auditor,
                    &EmptyVariableView,
                    CancellationToken::new(),
                )
                .await;
            let Err(microcrab_transport::Error::ProviderIdleTimeout(facts)) = result else {
                panic!("{protocol:?} headers={headers}: {result:?}");
            };
            assert_eq!(facts.timeout_ms, 400);
            assert!(facts.elapsed_ms >= 350);
            assert_eq!(facts.first_token_ms.is_some(), headers);
            assert_eq!(
                facts.streamed_bytes,
                if headers { sse.len() as u64 } else { 0 }
            );
            assert_eq!(facts.streamed_chars, u64::from(headers));
            let _ = release.send(());
            worker.join().unwrap();
            let prepared = records.try_recv().unwrap();
            let completed = records.try_recv().unwrap();
            assert_eq!(prepared.call_id, completed.call_id);
            assert_eq!(completed.outcome, Some(AuditOutcome::TimedOut));
            assert_eq!(completed.response_status, headers.then_some(200));
            assert!(!completed.response_complete.unwrap_or(false));
            assert!(records.try_recv().is_err());
            assert!(
                matches!(&observer.records.lock().unwrap().last().unwrap().state,
                ObservationState::Invalidated { reason } if reason == "timed_out")
            );
        }
    }
}

#[tokio::test]
async fn all_protocols_caller_cancellation_settles_once_as_cancelled_after_progress() {
    for (protocol, sse, _) in fixtures() {
        let (endpoint, release, worker) = chunks_server(
            "text/event-stream",
            vec![sse.into()],
            Duration::ZERO,
            true,
            true,
        );
        let (progress, received) = tokio::sync::oneshot::channel();
        let observer = Arc::new(Observed {
            records: Mutex::new(vec![]),
            progress: Mutex::new(Some(progress)),
        });
        let client = client(protocol, endpoint, 400, observer.clone());
        let cancellation = CancellationToken::new();
        let (sender, mut records) = tokio::sync::mpsc::unbounded_channel();
        let auditor = ChannelAuditor::from_sender(sender);
        let content = content(protocol);
        let (result, ()) = tokio::join!(
            client.complete(&content, &auditor, &EmptyVariableView, cancellation.clone()),
            async {
                received.await.unwrap();
                cancellation.cancel();
            }
        );
        assert!(
            matches!(result, Err(microcrab_transport::Error::Cancelled)),
            "{protocol:?}: {result:?}"
        );
        let _ = release.send(());
        worker.join().unwrap();
        let prepared = records.try_recv().unwrap();
        let completed = records.try_recv().unwrap();
        assert_eq!(prepared.call_id, completed.call_id);
        assert_eq!(completed.outcome, Some(AuditOutcome::Cancelled));
        assert!(records.try_recv().is_err());
        assert!(
            matches!(&observer.records.lock().unwrap().last().unwrap().state,
            ObservationState::Invalidated { reason } if reason == "cancelled")
        );
    }
}
