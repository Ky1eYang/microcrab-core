//! HTTP fixtures exercise transport-owned inactivity through the retry wrapper.
use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

pub(super) async fn read_request(socket: &mut tokio::net::TcpStream) {
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
                return;
            }
        }
    }
}

async fn fixture(
    attempts: usize,
    steady: bool,
    publish: bool,
) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/chat/completions", listener.local_addr().unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let worker = tokio::spawn(async move {
        for _ in 0..attempts {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            seen.fetch_add(1, Ordering::SeqCst);
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await.unwrap();
            if steady || publish {
                let delta = "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"},\"finish_reason\":null}]}\n\n";
                for _ in 0..if steady { 16 } else { 1 } {
                    let chunk = format!("{:x}\r\n{delta}\r\n", delta.len());
                    socket.write_all(chunk.as_bytes()).await.unwrap();
                    if steady {
                        sleep(Duration::from_millis(25)).await;
                    }
                }
            }
            if steady {
                let done = "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
                socket
                    .write_all(format!("{:x}\r\n{done}\r\n0\r\n\r\n", done.len()).as_bytes())
                    .await
                    .unwrap();
            } else {
                // The client closes this socket only after its own idle expiry.
                let mut byte = [0u8; 1];
                let _ = socket.read(&mut byte).await;
            }
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    });
    (endpoint, calls, worker)
}

#[tokio::test]
async fn steady_stream_outlives_the_legacy_deadline_through_retrying_client() {
    let (endpoint, calls, server) = fixture(1, true, true).await;
    // A deliberately different transport config proves the public legacy field
    // still configures the idle window rather than an outer absolute cap.
    let client = RetryingClient::new(
        OpenAiCompatibleConfig {
            endpoint,
            model: "fixture".into(),
            api_key: None,
            timeout_ms: 1,
            max_response_bytes: 4096,
        },
        RetryPolicy {
            attempts: 6,
            backoff_ms: 0,
            deadline_ms: 200,
        },
    )
    .unwrap();
    let (sender, mut receiver) = mpsc::channel(32);
    let start = Instant::now();
    let response = client
        .stream(
            &content(),
            &NoopAuditor,
            &EmptyVariableView,
            CancellationToken::new(),
            Some(sender),
        )
        .await
        .unwrap();
    assert_eq!(response.content, "x".repeat(16));
    assert!(start.elapsed() > Duration::from_millis(200));
    let mut count = 0;
    let mut finished = false;
    while let Some(event) = receiver.recv().await {
        match event {
            ModelEvent::TextDelta { .. } => count += 1,
            ModelEvent::Finished { .. } => finished = true,
            _ => {}
        }
    }
    assert_eq!(count, 16);
    assert!(finished);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    server.await.unwrap();
}

#[tokio::test]
async fn blocked_event_forwarding_has_no_absolute_cap_and_accepts_cancellation() {
    let (endpoint, calls, server) = fixture(1, true, true).await;
    let client = client(endpoint, 0, 200);
    let content = content();
    let (sender, _receiver) = mpsc::channel(1);
    let token = CancellationToken::new();
    let request = client.stream(
        &content,
        &NoopAuditor,
        &EmptyVariableView,
        token.clone(),
        Some(sender),
    );
    tokio::pin!(request);
    tokio::select! {
        biased;
        result = &mut request => panic!("forwarding unexpectedly ended: {result:?}"),
        _ = sleep(Duration::from_millis(700)) => {}
    }
    token.cancel();
    assert!(matches!(request.await, Err(Error::Cancelled)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    server.await.unwrap();
}

#[tokio::test]
async fn unpublished_idle_timeout_retries_all_six_attempts() {
    for streaming in [false, true] {
        let (endpoint, calls, server) = fixture(6, false, false).await;
        let client = client(endpoint, 0, 200);
        let content = content();
        let result = if streaming {
            client
                .stream(
                    &content,
                    &NoopAuditor,
                    &EmptyVariableView,
                    CancellationToken::new(),
                    None,
                )
                .await
        } else {
            client
                .complete(
                    &content,
                    &NoopAuditor,
                    &EmptyVariableView,
                    CancellationToken::new(),
                )
                .await
        };
        assert!(
            matches!(result, Err(Error::ProviderIdleTimeout(_))),
            "{result:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 6);
        server.await.unwrap();
    }
}

#[tokio::test]
async fn published_idle_timeout_never_automatically_replays() {
    for forwarding in [false, true] {
        let (endpoint, calls, server) = fixture(1, false, true).await;
        let (sender, mut receiver) = mpsc::channel(16);
        let result = client(endpoint, 0, 200)
            .stream(
                &content(),
                &NoopAuditor,
                &EmptyVariableView,
                CancellationToken::new(),
                forwarding.then_some(sender),
            )
            .await;
        assert!(
            matches!(result, Err(Error::ProviderIdleTimeout(_))),
            "{result:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        if forwarding {
            assert!(
                matches!(receiver.recv().await, Some(ModelEvent::TextDelta { text }) if text == "x")
            );
            assert!(receiver.recv().await.is_none());
        }
        server.await.unwrap();
    }
}
