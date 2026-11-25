//! HTTP-only remote-image fallback fixtures. No provider credentials or uploads.
use super::*;
use microcrab_session::protocol::{ImageSource, InputPart};
use microcrab_transport::{
    ContentItem, HostedImageHint, ImageHintRevocation, ImageUpload, ImageUploadError,
};
use serde_json::{Value, json};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::SystemTime,
};

const FRESH: ToolImagePosition = ToolImagePosition { item: 0, part: 0 };
const INLINE: &str = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAAAAAAAA";
const HINT: &str = "https://loopback-media.invalid/image?private=0";
fn scope() -> ImageHostingScope {
    ImageHostingScope {
        route: "loopback/model".into(),
        authority: "owner".into(),
    }
}
fn config() -> ImageHostingConfig {
    ImageHostingConfig {
        scope: scope(),
        allowed_hosts: vec!["loopback-media.invalid".into()],
        max_hint_ttl: Duration::from_secs(60),
    }
}
fn enable(client: RetryingClient) -> (RetryingClient, Arc<UrlHint>) {
    let uploader = Arc::new(UrlHint::default());
    (
        client
            .with_image_hosting(config(), uploader.clone())
            .unwrap(),
        uploader,
    )
}
fn grant<'a>(client: &'a RetryingClient, canonical: &'a Content) -> ImageHostingGrant<'a> {
    client
        .authorize_image_hosting(&scope(), canonical, &[FRESH])
        .unwrap()
}

const SUCCESS: &str =
    r#"{"choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}"#;
const REJECTION: &str =
    r#"{"error":{"code":"failed_to_download_image","message":"private upstream body"}}"#;

#[tokio::test]
async fn configured_route_without_grant_never_uploads_historical_data() {
    let (endpoint, server) =
        serve(vec![http(200, SUCCESS, false), http(200, SUCCESS, false)]).await;
    let (client, uploader) = enable(client(endpoint, 0, 1000));
    let canonical = media_content();
    let mut invalid = media_content();
    let ContentItem::Tool { parts, .. } = &mut invalid.items[0] else {
        unreachable!()
    };
    parts[0] = InputPart::Image {
        source: ImageSource::Data {
            media_type: "image/png".into(),
            data: vec![1, 2, 3],
        },
    };
    assert!(
        client
            .authorize_image_hosting(&scope(), &invalid, &[FRESH])
            .is_err()
    );
    client
        .complete(
            &canonical,
            &NoopAuditor,
            &EmptyVariableView,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    client
        .stream(
            &canonical,
            &NoopAuditor,
            &EmptyVariableView,
            CancellationToken::new(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(uploader.0.load(Ordering::SeqCst), 0);
    for request in server.await.unwrap() {
        assert_eq!(
            request["messages"][2]["content"][1]["image_url"]["url"],
            INLINE
        );
    }
}

#[tokio::test]
async fn media_rejection_without_remaining_attempt_still_invalidates_the_hint() {
    let (endpoint, server) =
        serve(vec![http(400, REJECTION, false), http(200, SUCCESS, false)]).await;
    let (mut client, uploader) = enable(client(endpoint, 0, 1000));
    client.policy.attempts = 1;
    let canonical = media_content();
    for expected_success in [false, true] {
        let result = client
            .complete_with_image_hosting(
                grant(&client, &canonical),
                &NoopAuditor,
                &EmptyVariableView,
                CancellationToken::new(),
            )
            .await;
        assert_eq!(result.is_ok(), expected_success);
    }
    let requests = server.await.unwrap();
    assert_eq!(
        requests[0]["messages"][2]["content"][1]["image_url"]["url"],
        HINT
    );
    assert_eq!(
        requests[1]["messages"][2]["content"][1]["image_url"]["url"],
        "https://loopback-media.invalid/image?private=1"
    );
    assert_eq!(uploader.0.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn media_in_band_rejection_before_first_event_retries_inline() {
    let error_event = format!("data: {REJECTION}\n\n");
    let (endpoint, server) = serve(vec![
        http(200, &error_event, true),
        http(200, SUCCESS, false),
    ])
    .await;
    let (client, uploader) = enable(client(endpoint, 0, 1000));
    let canonical = media_content();
    let (sender, mut received) = mpsc::channel(4);
    let response = client
        .stream_with_image_hosting(
            grant(&client, &canonical),
            &NoopAuditor,
            &EmptyVariableView,
            CancellationToken::new(),
            Some(sender),
        )
        .await
        .unwrap();
    assert_eq!(response.content, "ok");
    assert!(matches!(received.try_recv(), Ok(ModelEvent::TextDelta { text }) if text == "ok"));
    assert!(matches!(
        received.try_recv(),
        Ok(ModelEvent::Finished { .. })
    ));
    assert!(received.try_recv().is_err());
    let requests = server.await.unwrap();
    assert_eq!(
        requests[0]["messages"][2]["content"][1]["image_url"]["url"],
        HINT
    );
    assert_eq!(
        requests[1]["messages"][2]["content"][1]["image_url"]["url"],
        INLINE
    );
    assert_eq!(uploader.0.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn media_fallback_consumes_the_configured_total_attempt_budget() {
    for attempts in [1, 2] {
        for streaming in [false, true] {
            let rejection = r#"{"error":{"code":"failed_to_download_image"}}"#;
            let (endpoint, server) =
                serve((0..attempts).map(|_| http(400, rejection, false)).collect()).await;
            let (mut client, _) = enable(client(endpoint, 0, 1000));
            client.policy.attempts = attempts;
            let canonical = media_content();
            let result = if streaming {
                client
                    .stream_with_image_hosting(
                        grant(&client, &canonical),
                        &NoopAuditor,
                        &EmptyVariableView,
                        CancellationToken::new(),
                        None,
                    )
                    .await
            } else {
                client
                    .complete_with_image_hosting(
                        grant(&client, &canonical),
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
                    INLINE
                );
            }
        }
    }
}

#[derive(Default)]
struct UrlHint(AtomicUsize);
#[async_trait::async_trait]
impl ImageUrlResolver for UrlHint {
    async fn upload(
        &self,
        upload_scope: &ImageHostingScope,
        image: ImageUpload,
        _: CancellationToken,
    ) -> Result<Option<HostedImageHint>, ImageUploadError> {
        assert!(upload_scope == &scope());
        assert_eq!(image.media_type, "image/png");
        assert_eq!(image.data, png());
        let generation = self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Some(HostedImageHint {
            url: format!("https://loopback-media.invalid/image?private={generation}"),
            expires_at: SystemTime::now() + Duration::from_secs(30),
            revocation: ImageHintRevocation::ExpiresOnly,
        }))
    }
}
fn png() -> Vec<u8> {
    let mut bytes = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
    bytes.extend_from_slice(&1u32.to_be_bytes());
    bytes.extend_from_slice(&1u32.to_be_bytes());
    bytes.extend_from_slice(&[8, 2, 0, 0, 0, 0, 0, 0, 0]);
    bytes
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
                data: png(),
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
async fn typed_http_rejection_retries_once_with_canonical_data_and_metadata_only_audits() {
    use microcrab_auditor::{AuditRecord, ChannelAuditor};
    struct MetadataOnly(ChannelAuditor);
    impl Auditor for MetadataOnly {
        fn is_enabled(&self) -> bool {
            self.0.is_enabled()
        }
        fn publish(&self, moment: Moment, body: AuditBody) {
            self.0.publish(moment, body);
        }
        fn intervene<'a>(
            &'a self,
            _: Moment,
            _: HookContext<'a>,
        ) -> BoxFuture<'a, Result<Injection, microcrab_auditor::AuditorError>> {
            panic!("hosted attempts must bypass the wire-body hook");
        }
    }
    for streaming in [false, true] {
        let (endpoint, server) =
            serve(vec![http(400, REJECTION, false), http(200, SUCCESS, false)]).await;
        let (client, _) = enable(client(endpoint, 0, 1000));
        let (sender, mut records) = mpsc::unbounded_channel::<AuditRecord>();
        let auditor = MetadataOnly(ChannelAuditor::from_sender(sender));
        let canonical = media_content();
        let attempt = if streaming {
            let outcome = client
                .stream_with_image_hosting_outcome(
                    grant(&client, &canonical),
                    &auditor,
                    &EmptyVariableView,
                    CancellationToken::new(),
                    None,
                )
                .await
                .unwrap();
            assert_eq!(outcome.response.content, "ok");
            Some(outcome.attempt.unwrap())
        } else {
            let response = client
                .complete_with_image_hosting(
                    grant(&client, &canonical),
                    &auditor,
                    &EmptyVariableView,
                    CancellationToken::new(),
                )
                .await
                .unwrap();
            assert_eq!(response.content, "ok");
            None
        };
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0]["messages"][2]["content"][1]["image_url"]["url"],
            HINT
        );
        assert_eq!(
            requests[1]["messages"][2]["content"][1]["image_url"]["url"],
            INLINE
        );
        assert_eq!(requests[0]["messages"][0], requests[1]["messages"][0]);
        for number in 0..requests.len() {
            let prepared = records.try_recv().unwrap();
            let terminal = records.try_recv().unwrap();
            assert_eq!(prepared.call_id, terminal.call_id);
            assert!(prepared.sent_request.is_none());
            assert!(terminal.response.is_none() && terminal.response_bytes.is_none());
            assert!(matches!(
                terminal.payload,
                microcrab_auditor::HookPayload::None
            ));
            assert!(terminal.response_status.is_some());
            assert!(terminal.outcome.is_some());
            if number == 0 {
                assert!(terminal.response_completed_at_ms.is_none());
                if let Some(attempt) = &attempt {
                    assert_ne!(terminal.call_id.as_deref(), Some(attempt.call_id.as_str()));
                }
            } else if let Some(attempt) = &attempt {
                let completed = attempt.response_completed_at_ms.unwrap().get();
                assert_eq!(terminal.call_id.as_deref(), Some(attempt.call_id.as_str()));
                assert_eq!(terminal.response_completed_at_ms, Some(completed));
            }
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
        let (client, _) = enable(client(endpoint, 0, 1000));
        let result = client
            .stream_with_image_hosting(
                grant(&client, &media_content()),
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
    let (client, _) = enable(client(endpoint, 0, 1000));
    let (sender, mut receiver) = mpsc::channel(16);
    let canonical = media_content();
    let request = client.stream_with_image_hosting(
        grant(&client, &canonical),
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
    assert!(result.unwrap_err().is_remote_image_fetch_rejection());
    server.await.unwrap();
}
