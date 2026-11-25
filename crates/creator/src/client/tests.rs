//! Retry fixtures use synthetic requests only; no configured provider or audit storage.
mod idle;
mod media;
mod upstream;

use super::*;
use microcrab_auditor::{
    AuditBody, BoxFuture, EmptyVariableView, HookContext, Injection, Moment, NoopAuditor,
};
use std::sync::Mutex;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::Instant,
};

fn client(endpoint: String, backoff_ms: u64, deadline_ms: u64) -> RetryingClient {
    RetryingClient::new(
        OpenAiCompatibleConfig {
            endpoint,
            model: "fixture".into(),
            api_key: None,
            timeout_ms: deadline_ms,
            max_response_bytes: 4096,
        },
        RetryPolicy {
            attempts: 6,
            backoff_ms,
            deadline_ms,
        },
    )
    .unwrap()
}

fn content() -> Content {
    Content {
        epoch: 1,
        subagent: None,
        system: "Fixture".into(),
        items: vec![],
        tools: vec![],
        max_output_tokens: 16,
        max_bytes: 4096,
        turn_id: Some(1),
        round: Some(1),
    }
}

#[derive(Default)]
struct HeldPreparation(Mutex<Vec<Instant>>);
impl Auditor for HeldPreparation {
    fn is_enabled(&self) -> bool {
        false
    }
    fn publish(&self, _: Moment, _: AuditBody) {}
    fn intervene<'a>(
        &'a self,
        _: Moment,
        _: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, microcrab_auditor::AuditorError>> {
        self.0.lock().unwrap().push(Instant::now());
        Box::pin(std::future::pending())
    }
}

#[tokio::test(start_paused = true)]
async fn five_backoffs_keep_the_exact_exponential_schedule() {
    let client = client("http://127.0.0.1:1/chat/completions".into(), 1000, 100);
    let start = Instant::now();
    for (attempt, elapsed_ms) in [1000, 3000, 7000, 15000, 31000].into_iter().enumerate() {
        client
            .delay(attempt as u32, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(start.elapsed(), Duration::from_millis(elapsed_ms));
    }
}

#[tokio::test(start_paused = true)]
async fn preparation_has_no_absolute_cap_and_remains_caller_cancellable() {
    for streaming in [false, true] {
        let client = client("http://127.0.0.1:1/chat/completions".into(), 1000, 100);
        let auditor = HeldPreparation::default();
        let token = CancellationToken::new();
        let cancel = token.clone();
        let content = content();
        let request = async {
            if streaming {
                client
                    .stream(&content, &auditor, &EmptyVariableView, token, None)
                    .await
            } else {
                client
                    .complete(&content, &auditor, &EmptyVariableView, token)
                    .await
            }
        };
        tokio::pin!(request);
        tokio::select! {
            biased;
            result = &mut request => panic!("preparation unexpectedly ended: {result:?}"),
            _ = sleep(Duration::from_millis(10_000)) => {}
        }
        assert_eq!(auditor.0.lock().unwrap().len(), 1);
        cancel.cancel();
        assert!(matches!(request.await, Err(Error::Cancelled)));
    }
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_backoff_stops_before_the_next_attempt() {
    let client = client("http://127.0.0.1:1/chat/completions".into(), 1000, 100);
    let token = CancellationToken::new();
    let cancel = token.clone();
    let start = Instant::now();
    let cancellation = async {
        sleep(Duration::from_millis(200)).await;
        cancel.cancel();
    };
    let (result, ()) = tokio::join!(client.delay(0, &token), cancellation);
    assert!(matches!(result, Err(Error::Cancelled)));
    assert_eq!(start.elapsed(), Duration::from_millis(200));
}

async fn response_server(response: String) -> (String, tokio::task::JoinHandle<()>) {
    response_server_many(vec![response]).await
}

async fn response_server_many(responses: Vec<String>) -> (String, tokio::task::JoinHandle<()>) {
    let (endpoint, worker, _) = response_server_many_counted(responses).await;
    (endpoint, worker)
}

async fn response_server_many_counted(
    responses: Vec<String>,
) -> (
    String,
    tokio::task::JoinHandle<()>,
    Arc<std::sync::atomic::AtomicUsize>,
) {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = calls.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/chat/completions", listener.local_addr().unwrap());
    let worker = tokio::spawn(async move {
        let mut previous = None;
        for response in responses {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0u8; 2048];
            loop {
                let read = socket.read(&mut buffer).await.unwrap();
                assert_ne!(read, 0);
                request.extend_from_slice(&buffer[..read]);
                if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&request[..end]);
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    if request.len() >= end + 4 + length {
                        break;
                    }
                }
            }
            if let Some(previous) = &previous {
                assert_eq!(&request, previous);
            }
            seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            previous = Some(request);
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
        }
        // A retry must not open another request after the terminal condition.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    });
    (endpoint, worker, calls)
}

#[tokio::test]
async fn fifth_retry_can_succeed_without_changing_the_request() {
    for streaming in [false, true] {
        let mut responses = [408, 429, 500, 502, 503]
            .into_iter()
            .map(|status| {
                format!(
                    "HTTP/1.1 {status} Fixture\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
            })
            .collect::<Vec<_>>();
        let body = r#"{"choices":[{"message":{"role":"assistant","content":"recovered"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#;
        responses.push(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()));
        let (endpoint, server) = response_server_many(responses).await;
        let client = client(endpoint, 0, 1000);
        let content = content();
        let (sender, mut records) = mpsc::unbounded_channel();
        let auditor = microcrab_auditor::ChannelAuditor::from_sender(sender);
        let response = if streaming {
            let outcome = client
                .stream_admitted_outcome(
                    &content,
                    &auditor,
                    &EmptyVariableView,
                    CancellationToken::new(),
                    None,
                )
                .await
                .unwrap();
            for _ in 0..5 {
                let prepared = records.try_recv().unwrap();
                let failed = records.try_recv().unwrap();
                assert_eq!(prepared.call_id, failed.call_id);
                assert!(failed.response_completed_at_ms.is_none());
                assert_ne!(
                    failed.call_id.as_deref(),
                    outcome.attempt.as_ref().map(|a| a.call_id.as_str())
                );
            }
            let prepared = records.try_recv().unwrap();
            let terminal = records.try_recv().unwrap();
            let attempt = outcome.attempt.unwrap();
            assert_eq!(prepared.call_id, terminal.call_id);
            assert_eq!(terminal.call_id.as_deref(), Some(attempt.call_id.as_str()));
            assert_eq!(
                terminal.response_completed_at_ms,
                Some(attempt.response_completed_at_ms.unwrap().get())
            );
            assert!(records.try_recv().is_err());
            outcome.response
        } else {
            client
                .complete(
                    &content,
                    &NoopAuditor,
                    &EmptyVariableView,
                    CancellationToken::new(),
                )
                .await
                .unwrap()
        };
        assert_eq!(response.content, "recovered");
        server.await.unwrap();
    }
}

#[tokio::test]
async fn permanent_http_failure_is_not_retried() {
    let (endpoint, server) = response_server(
        "HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
    )
    .await;
    let result = client(endpoint, 0, 1000)
        .complete(
            &content(),
            &NoopAuditor,
            &EmptyVariableView,
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(result, Err(Error::ProviderStatus { status: 401 })));
    server.await.unwrap();
}

#[tokio::test]
async fn partial_stream_failure_does_not_replay_with_or_without_an_event_consumer() {
    for forwarding in [false, true] {
        let delta = "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"},\"finish_reason\":null}]}\n\n";
        // A valid first chunk followed by EOF without a chunked terminator is a transport failure.
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n{}\r\n",
            delta.len(),
            delta
        );
        let (endpoint, server) = response_server(response).await;
        let (sender, mut receiver) = mpsc::channel(16);
        let result = client(endpoint, 0, 1000)
            .stream(
                &content(),
                &NoopAuditor,
                &EmptyVariableView,
                CancellationToken::new(),
                forwarding.then_some(sender),
            )
            .await;
        assert!(matches!(result, Err(Error::TransportFailure(_))));
        if forwarding {
            assert!(
                matches!(receiver.recv().await, Some(ModelEvent::TextDelta { text }) if text == "partial")
            );
            assert!(receiver.recv().await.is_none());
        }
        server.await.unwrap();
    }
}
