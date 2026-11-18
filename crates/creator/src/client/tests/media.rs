//! HTTP-only remote-image fallback fixtures. No provider credentials or uploads.
use super::*;
use microcrab_session::protocol::{ImageSource, InputPart};
use microcrab_transport::{ContentItem, ImageUrlResolver, MediaProjection};
use serde_json::{Value, json};

#[tokio::test]
async fn media_fallback_consumes_the_configured_total_attempt_budget() {
    for attempts in [1, 2] {
        for streaming in [false, true] {
            let rejection = r#"{"error":{"code":"failed_to_download_image"}}"#;
            let (endpoint, server) =
                serve((0..attempts).map(|_| http(400, rejection, false)).collect()).await;
            let mut client = client(endpoint, 0, 1000)
                .with_media_projection(Arc::new(MediaProjection::new(Arc::new(UrlHint), true)));
            client.policy.attempts = attempts;
            let canonical = media_content();
            let result = if streaming {
                client
                    .stream(
                        &canonical,
                        &NoopAuditor,
                        &EmptyVariableView,
                        CancellationToken::new(),
                        None,
                    )
                    .await
            } else {
                client
                    .complete(
                        &canonical,
                        &NoopAuditor,
                        &EmptyVariableView,
                        CancellationToken::new(),
                    )
                    .await
            };
            assert!(result.unwrap_err().is_remote_image_fetch_rejection());
            let requests = server.await.unwrap();
            assert_eq!(requests.len(), attempts as usize);
            if attempts == 2 {
                assert_eq!(
                    requests[1]["messages"][2]["content"][1]["image_url"]["url"],
                    "data:image/png;base64,AQID"
                );
            }
        }
    }
}

struct UrlHint;
#[async_trait::async_trait]
impl ImageUrlResolver for UrlHint {
    async fn upload(
        &self,
        _: &str,
        _: &[u8],
        _: CancellationToken,
    ) -> Result<Option<String>, Error> {
        Ok(Some("https://example.invalid/image?private=secret".into()))
    }
}
fn media_content() -> Content {
    let mut content = content();
    content.max_bytes = 16384;
    content.items.push(ContentItem::Tool {
        call_id: "fixture-call".into(),
        content: "image".into(),
        parts: vec![InputPart::Image {
            source: ImageSource::Data {
                media_type: "image/png".into(),
                data: vec![1, 2, 3],
            },
        }],
    });
    content
}
fn http(status: u16, body: &str, sse: bool) -> String {
    let kind = if sse {
        "text/event-stream"
    } else {
        "application/json"
    };
    format!(
        "HTTP/1.1 {status} Fixture\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}
async fn serve(responses: Vec<String>) -> (String, tokio::task::JoinHandle<Vec<Value>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/chat/completions", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        for response in responses {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut buf = [0u8; 2048];
            loop {
                let n = socket.read(&mut buf).await.unwrap();
                assert_ne!(n, 0);
                bytes.extend_from_slice(&buf[..n]);
                if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..end]);
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (key, value) = line.split_once(':')?;
                            key.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    if bytes.len() >= end + 4 + length {
                        requests.push(
                            serde_json::from_slice(&bytes[end + 4..end + 4 + length]).unwrap(),
                        );
                        break;
                    }
                }
            }
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
        requests
    });
    (endpoint, server)
}
#[tokio::test]
async fn typed_http_rejection_retries_once_with_canonical_data_and_actual_sent_audits() {
    use microcrab_auditor::ChannelAuditor;
    for streaming in [false, true] {
        let rejection =
            r#"{"error":{"code":"failed_to_download_image","message":"private upstream body"}}"#;
        let success = r#"{"choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}"#;
        let (endpoint, server) =
            serve(vec![http(400, rejection, false), http(200, success, false)]).await;
        let client = client(endpoint, 0, 1000)
            .with_media_projection(Arc::new(MediaProjection::new(Arc::new(UrlHint), true)));
        let (sender, mut records) = mpsc::unbounded_channel();
        let auditor = ChannelAuditor::from_sender(sender);
        let canonical = media_content();
        let response = if streaming {
            client
                .stream(
                    &canonical,
                    &auditor,
                    &EmptyVariableView,
                    CancellationToken::new(),
                    None,
                )
                .await
        } else {
            client
                .complete(
                    &canonical,
                    &auditor,
                    &EmptyVariableView,
                    CancellationToken::new(),
                )
                .await
        };
        assert_eq!(response.unwrap().content, "ok");
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0]["messages"][2]["content"][1]["image_url"]["url"],
            "https://example.invalid/image?private=secret"
        );
        assert_eq!(
            requests[1]["messages"][2]["content"][1]["image_url"]["url"],
            "data:image/png;base64,AQID"
        );
        assert_eq!(requests[0]["messages"][0], requests[1]["messages"][0]);
        for request in &requests {
            let prepared = records.try_recv().unwrap();
            let terminal = records.try_recv().unwrap();
            assert_eq!(prepared.call_id, terminal.call_id);
            assert_eq!(
                serde_json::from_str::<Value>(prepared.sent_request.as_deref().unwrap()).unwrap(),
                *request
            );
        }
        assert!(records.try_recv().is_err());
        assert!(
            matches!(&canonical.items[0], ContentItem::Tool { parts, .. } if matches!(&parts[0], InputPart::Image { source: ImageSource::Data { .. } }))
        );
    }
}
#[tokio::test]
async fn generic_errors_and_repeated_typed_rejection_never_trigger_more_media_retries() {
    for (status, body, count) in [
        (
            400,
            json!({"error":{"message":"failed to download image"}}).to_string(),
            1,
        ),
        (
            403,
            json!({"error":{"code":"failed_to_download_image"}}).to_string(),
            1,
        ),
        (
            400,
            json!({"error":{"code":"failed_to_download_image"}}).to_string(),
            2,
        ),
    ] {
        let (endpoint, server) =
            serve((0..count).map(|_| http(status, &body, false)).collect()).await;
        let client = client(endpoint, 0, 1000)
            .with_media_projection(Arc::new(MediaProjection::new(Arc::new(UrlHint), true)));
        let result = client
            .stream(
                &media_content(),
                &NoopAuditor,
                &EmptyVariableView,
                CancellationToken::new(),
                None,
            )
            .await;
        assert!(result.is_err());
        assert_eq!(server.await.unwrap().len(), count);
    }
}
#[tokio::test]
async fn in_band_fetch_error_after_a_published_delta_is_not_replayed() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/chat/completions", listener.local_addr().unwrap());
    let (published, observed) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        super::idle::read_request(&mut socket).await;
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await.unwrap();
        let delta = "data: {\"choices\":[{\"delta\":{\"content\":\"visible\"}}]}\n\n";
        socket
            .write_all(format!("{:x}\r\n{delta}\r\n", delta.len()).as_bytes())
            .await
            .unwrap();
        // The error cannot share an input chunk with an unpublished first delta.
        observed.await.unwrap();
        let error = "data: {\"error\":{\"code\":\"failed_to_download_image\"}}\n\n";
        socket
            .write_all(format!("{:x}\r\n{error}\r\n0\r\n\r\n", error.len()).as_bytes())
            .await
            .unwrap();
        socket.shutdown().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    });
    let client = client(endpoint, 0, 1000)
        .with_media_projection(Arc::new(MediaProjection::new(Arc::new(UrlHint), true)));
    let (sender, mut receiver) = mpsc::channel(16);
    let canonical = media_content();
    let request = client.stream(
        &canonical,
        &NoopAuditor,
        &EmptyVariableView,
        CancellationToken::new(),
        Some(sender),
    );
    let consumer = async move {
        assert!(receiver.recv().await.is_some());
        published.send(()).unwrap();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(request, consumer)
    })
    .await
    .unwrap();
    assert!(result.is_err());
    server.await.unwrap();
}
