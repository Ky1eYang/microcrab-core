use super::*;
use microcrab_session::admission::image::{ResolvedAttachment, UploadedAttachment};
use microcrab_session::protocol::{ImageSource, InputPart};
use microcrab_transport::{Content, ContentItem};
use std::sync::atomic::{AtomicUsize, Ordering};

fn png() -> Vec<u8> {
    let mut bytes = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
    bytes.extend_from_slice(&1u32.to_be_bytes());
    bytes.extend_from_slice(&1u32.to_be_bytes());
    bytes.extend_from_slice(&[8, 2, 0, 0, 0, 0, 0, 0, 0]);
    bytes
}
fn content() -> Content {
    Content {
        epoch: 1,
        subagent: None,
        system: "frozen".into(),
        items: vec![ContentItem::Tool {
            call_id: "call".into(),
            content: "caption".into(),
            parts: vec![InputPart::Image {
                source: ImageSource::Data {
                    media_type: "image/png".into(),
                    data: png(),
                },
            }],
        }],
        tools: vec![],
        max_output_tokens: 32,
        max_bytes: 65536,
        turn_id: Some(1),
        round: Some(1),
    }
}
fn client() -> super::super::RetryingClient {
    super::super::RetryingClient::new(
        microcrab_transport::OpenAiCompatibleConfig {
            endpoint: "http://127.0.0.1:1/chat/completions".into(),
            model: "fixture".into(),
            api_key: None,
            timeout_ms: 1000,
            max_response_bytes: 4096,
        },
        super::super::RetryPolicy {
            attempts: 2,
            backoff_ms: 0,
            deadline_ms: 1000,
        },
    )
    .unwrap()
}
struct Uploader {
    calls: AtomicUsize,
    mode: &'static str,
}
#[async_trait::async_trait]
impl AttachmentResolver for Uploader {
    async fn resolve(
        &self,
        _: &str,
        _: CancellationToken,
    ) -> Result<ResolvedAttachment, microcrab_session::Error> {
        panic!("upload bridge must not resolve historical bytes")
    }
    async fn upload_attachment(
        &self,
        media_type: &str,
        data: &[u8],
        _: CancellationToken,
    ) -> Result<Option<UploadedAttachment>, microcrab_session::Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(media_type, "image/png");
        assert_eq!(data, png());
        match self.mode {
            "url" => Ok(Some(UploadedAttachment {
                attachment_id: "route-local-id".into(),
                public_url: Some("https://example.invalid/image?signature=private".into()),
            })),
            "no-url" => Ok(Some(UploadedAttachment {
                attachment_id: "route-local-id".into(),
                public_url: None,
            })),
            "cancel" => Err(microcrab_session::Error::Cancelled),
            "pending" => std::future::pending().await,
            _ => Err(microcrab_session::Error::tool(
                "unknown upload outcome https://example.invalid/?token=secret body=private",
            )),
        }
    }
}
struct NoUpload;
#[async_trait::async_trait]
impl AttachmentResolver for NoUpload {
    async fn resolve(
        &self,
        _: &str,
        _: CancellationToken,
    ) -> Result<ResolvedAttachment, microcrab_session::Error> {
        panic!("no implicit resolve")
    }
}
#[tokio::test]
async fn default_client_and_disabled_capability_do_not_call_the_uploader() {
    assert!(client().media_projection.is_none());
    let resolver = Arc::new(Uploader {
        calls: AtomicUsize::new(0),
        mode: "url",
    });
    let configured = client().with_attachment_uploader(resolver.clone(), false);
    assert!(
        configured
            .media_projection
            .as_ref()
            .unwrap()
            .project_tool_images(&content(), CancellationToken::new())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn bridge_projects_stable_hints_without_mutating_canonical_data() {
    let resolver = Arc::new(Uploader {
        calls: AtomicUsize::new(0),
        mode: "url",
    });
    let configured = client().with_attachment_uploader(resolver.clone(), true);
    let canonical = content();
    let before = serde_json::to_vec(&canonical).unwrap();
    for _ in 0..2 {
        let projected = configured
            .media_projection
            .as_ref()
            .unwrap()
            .project_tool_images(&canonical, CancellationToken::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(projected.system, canonical.system);
        assert!(
            matches!(&projected.items[0],ContentItem::Tool {parts,..} if matches!(&parts[0],InputPart::Image {source:ImageSource::Url {url}} if url=="https://example.invalid/image?signature=private"))
        );
    }
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    assert_eq!(serde_json::to_vec(&canonical).unwrap(), before);
}
#[tokio::test]
async fn optional_uploader_noop_and_absent_public_url_preserve_data() {
    for resolver in [
        Arc::new(NoUpload) as Arc<dyn AttachmentResolver>,
        Arc::new(Uploader {
            calls: AtomicUsize::new(0),
            mode: "no-url",
        }),
    ] {
        let configured = client().with_attachment_uploader(resolver, true);
        assert!(
            configured
                .media_projection
                .as_ref()
                .unwrap()
                .project_tool_images(&content(), CancellationToken::new())
                .await
                .unwrap()
                .is_none()
        );
    }
}
#[tokio::test]
async fn cancellation_is_exact_for_callback_and_inflight_upload() {
    let resolver = Arc::new(Uploader {
        calls: AtomicUsize::new(0),
        mode: "cancel",
    });
    let bridge = AttachmentImageUrlResolver(resolver);
    assert!(matches!(
        bridge
            .upload("image/png", &png(), CancellationToken::new())
            .await,
        Err(Error::Cancelled)
    ));
    let token = CancellationToken::new();
    token.cancel();
    let resolver = Arc::new(Uploader {
        calls: AtomicUsize::new(0),
        mode: "url",
    });
    let bridge = AttachmentImageUrlResolver(resolver.clone());
    assert!(matches!(
        bridge.upload("image/png", &png(), token).await,
        Err(Error::Cancelled)
    ));
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
    let resolver = Arc::new(Uploader {
        calls: AtomicUsize::new(0),
        mode: "pending",
    });
    let bridge = AttachmentImageUrlResolver(resolver);
    let token = CancellationToken::new();
    let bytes = png();
    let mut upload = Box::pin(bridge.upload("image/png", &bytes, token.clone()));
    assert!(futures_util::poll!(&mut upload).is_pending());
    token.cancel();
    assert!(matches!(upload.await, Err(Error::Cancelled)));
}
#[tokio::test]
async fn validation_precedes_upload_and_unknown_outcomes_are_not_cached_or_leaked() {
    let resolver = Arc::new(Uploader {
        calls: AtomicUsize::new(0),
        mode: "failure",
    });
    let bridge = AttachmentImageUrlResolver(resolver.clone());
    assert!(
        bridge
            .upload("image/png", &[1, 2, 3], CancellationToken::new())
            .await
            .is_err()
    );
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
    let configured = client().with_attachment_uploader(resolver.clone(), true);
    for _ in 0..2 {
        let error = configured
            .media_projection
            .as_ref()
            .unwrap()
            .project_tool_images(&content(), CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(error.diagnostic_reason(), "attachment URL upload failed");
        let private = error.to_string();
        assert!(private.contains("tool_failed"));
        assert!(!private.contains("secret"));
        assert!(!private.contains("example.invalid"));
        assert!(!private.contains("body=private"));
    }
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 2);
}
#[test]
fn upload_failures_retain_typed_numeric_limit_and_io_facts_only() {
    let error = upload_error(microcrab_session::Error::Limit(
        microcrab_session::LimitError::new(microcrab_session::LimitKind::AttachmentBytes)
            .bytes(100, 64)
            .with_label("https://private.invalid/?token=secret"),
    ));
    assert!(error.to_string().contains("attachment_bytes"));
    assert!(error.to_string().contains("100"));
    assert!(!error.to_string().contains("secret"));
    let error = upload_error(microcrab_session::Error::Io(std::io::Error::from(
        std::io::ErrorKind::NotFound,
    )));
    assert!(error.to_string().contains("NotFound"));
}
