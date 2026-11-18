//! Real HTTP acceptance for upstream failures, including incomplete error bodies.
mod empty_completion;
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy, Debug)]
enum BodyFault {
    Stalled,
    Truncated,
}

struct IncompleteResponse {
    endpoint: String,
    calls: Arc<AtomicUsize>,
    headers: tokio::sync::oneshot::Receiver<()>,
    stop: CancellationToken,
    worker: tokio::task::JoinHandle<()>,
}

impl IncompleteResponse {
    async fn serve(status: u16, fault: BodyFault) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let stop = CancellationToken::new();
        let stopped = stop.clone();
        let (headers, received) = tokio::sync::oneshot::channel();
        let worker = tokio::spawn(async move {
            let mut headers = Some(headers);
            loop {
                let mut socket = tokio::select! {
                    biased;
                    _ = stopped.cancelled() => break,
                    accepted = listener.accept() => accepted.unwrap().0,
                };
                super::idle::read_request(&mut socket).await;
                seen.fetch_add(1, Ordering::SeqCst);
                socket.write_all(format!(
                    "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n7\r\npartial\r\n"
                ).as_bytes()).await.unwrap();
                if let Some(headers) = headers.take() {
                    let _ = headers.send(());
                }
                match fault {
                    BodyFault::Stalled => {
                        // Peer EOF acknowledges that this attempt released its response.
                        let mut byte = [0u8; 1];
                        tokio::select! {
                            biased;
                            _ = stopped.cancelled() => break,
                            _ = socket.read(&mut byte) => {},
                        }
                    }
                    BodyFault::Truncated => {
                        socket.write_all(b"8\r\nx").await.unwrap();
                        socket.shutdown().await.unwrap();
                    }
                }
            }
        });
        Self {
            endpoint,
            calls,
            headers: received,
            stop,
            worker,
        }
    }
}

fn incomplete_client(endpoint: String, protocol: Protocol) -> RetryingClient {
    RetryingClient::new_transport(
        TransportConfig {
            auth: Default::default(),
            protocol,
            endpoint,
            model: "fixture".into(),
            api_key: None,
            timeout_ms: 200,
            max_response_bytes: 4096,
        },
        RetryPolicy {
            attempts: 6,
            backoff_ms: 0,
            deadline_ms: 200,
        },
    )
    .unwrap()
}

#[tokio::test]
async fn incomplete_http_error_bodies_preserve_status_and_retry_counts_for_all_protocols() {
    use microcrab_auditor::{AuditOutcome, ChannelAuditor};

    for protocol in PROTOCOLS {
        for fault in [BodyFault::Stalled, BodyFault::Truncated] {
            for (status, attempts) in [(400, 1), (401, 1), (429, 6), (503, 6)] {
                for streaming in [false, true] {
                    let fixture = IncompleteResponse::serve(status, fault).await;
                    let client = incomplete_client(fixture.endpoint.clone(), protocol);
                    let (sender, mut records) = mpsc::unbounded_channel();
                    let auditor = ChannelAuditor::from_sender(sender);
                    let content = request();
                    let result = tokio::time::timeout(Duration::from_secs(10), async {
                        if streaming {
                            client
                                .stream(
                                    &content,
                                    &auditor,
                                    &EmptyVariableView,
                                    CancellationToken::new(),
                                    None,
                                )
                                .await
                        } else {
                            client
                                .complete(
                                    &content,
                                    &auditor,
                                    &EmptyVariableView,
                                    CancellationToken::new(),
                                )
                                .await
                        }
                    })
                    .await
                    .expect("incomplete response must settle");
                    fixture.stop.cancel();
                    fixture.worker.await.unwrap();
                    assert!(
                        matches!(result, Err(Error::ProviderStatus { status: actual }) if actual == status),
                        "{protocol} {fault:?} status={status} streaming={streaming}: {result:?}"
                    );
                    assert_eq!(
                        fixture.calls.load(Ordering::SeqCst),
                        attempts,
                        "{protocol} {fault:?} status={status}"
                    );
                    for _ in 0..attempts {
                        let prepared = records.try_recv().unwrap();
                        let terminal = records.try_recv().unwrap();
                        assert_eq!(prepared.call_id, terminal.call_id);
                        assert_eq!(terminal.response_status, Some(status));
                        assert_eq!(terminal.response_complete, Some(false));
                        assert_eq!(terminal.outcome, Some(AuditOutcome::Unavailable));
                        if matches!(fault, BodyFault::Stalled) {
                            assert_eq!(terminal.response.as_deref(), Some("partial"));
                        }
                    }
                    assert!(records.try_recv().is_err());
                }
            }
        }
    }
}

#[tokio::test]
async fn incomplete_success_bodies_keep_network_errors_for_all_protocols() {
    for protocol in PROTOCOLS {
        for fault in [BodyFault::Stalled, BodyFault::Truncated] {
            let fixture = IncompleteResponse::serve(200, fault).await;
            let client = TransportClient::new(TransportConfig {
                auth: Default::default(),
                protocol,
                endpoint: fixture.endpoint.clone(),
                model: "fixture".into(),
                api_key: None,
                timeout_ms: 200,
                max_response_bytes: 4096,
            })
            .unwrap();
            let result = client
                .complete(
                    &request(),
                    &NoopAuditor,
                    &EmptyVariableView,
                    CancellationToken::new(),
                )
                .await;
            fixture.stop.cancel();
            fixture.worker.await.unwrap();
            match fault {
                BodyFault::Stalled => assert!(
                    matches!(result, Err(Error::ProviderIdleTimeout(_))),
                    "{protocol}: {result:?}"
                ),
                BodyFault::Truncated => assert!(
                    matches!(&result, Err(Error::TransportFailure(_))),
                    "{protocol}: {result:?}"
                ),
            }
            assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn cancelling_incomplete_http_rejection_stops_without_retry_for_all_protocols() {
    use microcrab_auditor::{AuditOutcome, ChannelAuditor};

    for protocol in PROTOCOLS {
        let fixture = IncompleteResponse::serve(401, BodyFault::Stalled).await;
        let client = protocol_client(fixture.endpoint, protocol);
        let cancellation = CancellationToken::new();
        let (sender, mut records) = mpsc::unbounded_channel();
        let auditor = ChannelAuditor::from_sender(sender);
        let content = request();
        let (result, ()) = tokio::join!(
            client.stream(
                &content,
                &auditor,
                &EmptyVariableView,
                cancellation.clone(),
                None
            ),
            async {
                fixture.headers.await.unwrap();
                cancellation.cancel();
            }
        );
        fixture.stop.cancel();
        fixture.worker.await.unwrap();
        assert!(
            matches!(result, Err(Error::Cancelled)),
            "{protocol}: {result:?}"
        );
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        let prepared = records.try_recv().unwrap();
        let terminal = records.try_recv().unwrap();
        assert_eq!(prepared.call_id, terminal.call_id);
        assert_eq!(terminal.outcome, Some(AuditOutcome::Cancelled));
        assert_ne!(terminal.response_complete, Some(true));
        assert!(records.try_recv().is_err());
    }
}

const PROTOCOLS: [Protocol; 4] = [
    Protocol::OpenaiCompatible,
    Protocol::OpenaiResponses,
    Protocol::Anthropic,
    Protocol::Google,
];

fn protocol_client(endpoint: String, protocol: Protocol) -> RetryingClient {
    RetryingClient::new_transport(
        TransportConfig {
            auth: Default::default(),
            protocol,
            endpoint,
            model: "fixture".into(),
            api_key: None,
            timeout_ms: 1000,
            max_response_bytes: 4096,
        },
        RetryPolicy {
            attempts: 6,
            backoff_ms: 0,
            deadline_ms: 1000,
        },
    )
    .unwrap()
}

fn request() -> Content {
    let mut request = content();
    request.items.push(microcrab_transport::ContentItem::User {
        envelope: None,
        source: None,
        parts: vec![microcrab_protocol::InputPart::Text {
            text: "fixture input".into(),
        }],
    });
    request
}

fn sse(body: &str) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

#[tokio::test]
async fn transient_in_band_error_retries_six_attempts_for_every_protocol() {
    for (protocol, body) in [
        (
            Protocol::OpenaiCompatible,
            "data: {\"error\":{\"type\":\"server_error\"}}\n\n",
        ),
        (
            Protocol::OpenaiResponses,
            "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_error\"}}}\n\n",
        ),
        (
            Protocol::Anthropic,
            "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\"}}\n\n",
        ),
        (
            Protocol::Google,
            "data: {\"error\":{\"code\":503,\"status\":\"UNAVAILABLE\"}}\n\n",
        ),
    ] {
        for streaming in [false, true] {
            let (endpoint, server) = response_server_many(vec![sse(body); 6]).await;
            let client = protocol_client(endpoint, protocol);
            let result = if streaming {
                client
                    .stream(
                        &request(),
                        &NoopAuditor,
                        &EmptyVariableView,
                        CancellationToken::new(),
                        None,
                    )
                    .await
            } else {
                client
                    .complete(
                        &request(),
                        &NoopAuditor,
                        &EmptyVariableView,
                        CancellationToken::new(),
                    )
                    .await
            };
            assert!(
                matches!(result, Err(Error::ProviderUnavailable(_))),
                "{protocol}: {result:?}"
            );
            server.await.unwrap();
        }
    }
}

async fn six_decode_attempts(protocol: Protocol, response: String) -> Error {
    let (endpoint, mut server, calls) = response_server_many_counted(vec![response; 6]).await;
    let exchange = tokio::time::timeout(Duration::from_secs(3), async {
        let result = protocol_client(endpoint, protocol)
            .stream(
                &request(),
                &NoopAuditor,
                &EmptyVariableView,
                CancellationToken::new(),
                None,
            )
            .await;
        (&mut server).await.unwrap();
        result
    })
    .await;
    if exchange.is_err() {
        server.abort();
        let _ = server.await;
    }
    let result = exchange.unwrap_or_else(|_| {
        panic!(
            "{protocol}: retry exchange timed out after {} of 6 requests",
            calls.load(Ordering::SeqCst)
        )
    });
    assert_eq!(calls.load(Ordering::SeqCst), 6, "{protocol}");
    let error = result.expect_err("malformed response must fail after six attempts");
    assert!(
        matches!(&error, Error::Protocol(_) | Error::Provider(_)),
        "{protocol}: {error:?}"
    );
    error
}

#[tokio::test]
async fn clean_eof_without_terminal_or_with_an_unfinished_frame_retries_six_attempts() {
    for protocol in PROTOCOLS {
        for body in [": keepalive\n\n", "data: {"] {
            six_decode_attempts(protocol, sse(body)).await;
        }
    }
}

#[tokio::test]
async fn clean_eof_after_published_text_never_replays_for_any_protocol() {
    for (protocol, body) in [
        (
            Protocol::OpenaiCompatible,
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"},\"finish_reason\":null}]}\n\n",
        ),
        (
            Protocol::OpenaiResponses,
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n",
        ),
        (
            Protocol::Anthropic,
            concat!(
                "data: {\"type\":\"message_start\",\"message\":{}}\n\n",
                "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
                "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\n"
            ),
        ),
        (
            Protocol::Google,
            "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"partial\"}]}}]}\n\n",
        ),
    ] {
        let (endpoint, server) = response_server(sse(body)).await;
        let (sender, mut receiver) = mpsc::channel(16);
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
            matches!(&result, Err(Error::Protocol(_) | Error::Provider(_))),
            "{protocol}: {result:?}"
        );
        assert!(
            matches!(receiver.recv().await, Some(ModelEvent::TextDelta { text }) if text == "partial")
        );
        assert!(receiver.recv().await.is_none());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn permanent_in_band_errors_are_not_retried() {
    for protocol in PROTOCOLS {
        let body = "data: {\"error\":{\"code\":400,\"type\":\"invalid_request_error\"}}\n\n";
        let (endpoint, server) = response_server(sse(body)).await;
        let result = protocol_client(endpoint, protocol)
            .stream(
                &request(),
                &NoopAuditor,
                &EmptyVariableView,
                CancellationToken::new(),
                None,
            )
            .await;
        assert!(
            matches!(result, Err(Error::Provider(_))),
            "{protocol}: {result:?}"
        );
        server.await.unwrap();
    }
}

#[tokio::test]
async fn malformed_upstream_json_bytes_retry_six_attempts_in_sse_and_json_fallback() {
    for protocol in PROTOCOLS {
        for content_type in ["text/event-stream", "application/json"] {
            let body = if content_type == "text/event-stream" {
                "data: {\"raw_body\":\"PRIVATE_DECODER_SENTINEL\",\n\n"
            } else {
                "{\"raw_body\":\"PRIVATE_DECODER_SENTINEL\","
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let error = six_decode_attempts(protocol, response).await;
            let cause = error.diagnostic_reason();
            assert!(
                cause.contains("EOF while parsing a value (line 1, column 39)"),
                "{protocol} {content_type}: {cause}"
            );
            assert!(
                cause.contains("line 1"),
                "{protocol} {content_type}: {cause}"
            );
            assert!(
                cause.contains("column"),
                "{protocol} {content_type}: {cause}"
            );
            assert!(!cause.contains("PRIVATE_DECODER_SENTINEL"));
        }
    }
}

#[tokio::test]
async fn semantic_response_errors_remain_manual_only_without_quarantining_local_protocol() {
    for (protocol, body) in [
        (
            Protocol::OpenaiCompatible,
            "data: {\"choices\":\"invalid\"}\n\n",
        ),
        (
            Protocol::OpenaiResponses,
            // A completed event with no optional metadata is valid; make the
            // output structure explicitly malformed instead of testing emptiness.
            "data: {\"type\":\"response.completed\",\"response\":{\"output\":\"invalid\"}}\n\n",
        ),
        (Protocol::Anthropic, "data: {\"type\":\"message_stop\"}\n\n"),
        (Protocol::Google, "data: {\"candidates\":[{},{}]}\n\n"),
    ] {
        let (endpoint, server) = response_server(sse(body)).await;
        let result = protocol_client(endpoint, protocol)
            .stream(
                &request(),
                &NoopAuditor,
                &EmptyVariableView,
                CancellationToken::new(),
                None,
            )
            .await;
        assert!(
            matches!(result, Err(Error::Provider(_))),
            "{protocol}: {result:?}"
        );
        assert!(
            result
                .as_ref()
                .unwrap_err()
                .public_protocol_reason()
                .is_some(),
            "{protocol}: semantic provider failure lost its safe cause"
        );
        server.await.unwrap();
    }
}

#[tokio::test]
async fn oversized_http_error_body_does_not_hide_retryable_or_permanent_status() {
    for protocol in PROTOCOLS {
        for (status, attempts) in [(503, 6), (401, 1)] {
            let body = "x".repeat(5000);
            let response = format!(
                "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let (endpoint, server) = response_server_many(vec![response; attempts]).await;
            let result = protocol_client(endpoint, protocol)
                .stream(
                    &request(),
                    &NoopAuditor,
                    &EmptyVariableView,
                    CancellationToken::new(),
                    None,
                )
                .await;
            assert!(
                matches!(result, Err(Error::ProviderStatus { status: actual }) if actual == status),
                "{protocol}: {result:?}"
            );
            server.await.unwrap();
        }
    }
}
