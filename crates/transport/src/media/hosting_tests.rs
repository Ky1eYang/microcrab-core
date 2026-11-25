use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

const AUTHORIZED: ToolImagePosition = ToolImagePosition { item: 2, part: 0 };

fn scope() -> ImageHostingScope {
    ImageHostingScope {
        route: "fake/openai/model".into(),
        authority: "authorized-owner".into(),
    }
}

fn config() -> ImageHostingConfig {
    ImageHostingConfig {
        scope: scope(),
        allowed_hosts: vec!["objects.invalid".into()],
        max_hint_ttl: Duration::from_secs(90),
    }
}

fn content() -> Content {
    let tool = ContentItem::Tool {
        call_id: "retained".into(),
        content: "result".into(),
        parts: vec![image()],
    };
    Content {
        epoch: 1,
        subagent: None,
        system: "frozen".into(),
        items: vec![
            ContentItem::User {
                parts: vec![image()],
                source: None,
                envelope: None,
            },
            tool.clone(),
            ContentItem::Tool {
                call_id: "fresh".into(),
                content: "result".into(),
                parts: vec![image(), image()],
            },
        ],
        tools: vec![],
        max_output_tokens: 32,
        max_bytes: 65536,
        turn_id: Some(1),
        round: Some(1),
    }
}

struct Resolver {
    calls: AtomicUsize,
    outcome: Outcome,
}
enum Outcome {
    Live,
    Revocable(CancellationToken),
    Expired,
    OverlongExpiry,
    ForeignHost,
    NoUpload,
    Cancel,
    Pending,
    Unknown,
}
#[async_trait::async_trait]
impl ImageUrlResolver for Resolver {
    async fn upload(
        &self,
        scope: &ImageHostingScope,
        image: ImageUpload,
        _: CancellationToken,
    ) -> Result<Option<HostedImageHint>, ImageUploadError> {
        assert_eq!(scope.route, "fake/openai/model");
        assert_eq!(image.media_type, "image/png");
        let ticket = self.calls.fetch_add(1, Ordering::SeqCst);
        let now = SystemTime::now();
        let mut hint = HostedImageHint {
            url: format!("https://objects.invalid/image?private={ticket}"),
            expires_at: now + Duration::from_secs(30),
            revocation: ImageHintRevocation::ExpiresOnly,
        };
        match &self.outcome {
            Outcome::Live => {}
            Outcome::Revocable(token) => {
                hint.revocation = ImageHintRevocation::Revocable(token.clone())
            }
            Outcome::Expired => hint.expires_at = now - Duration::from_secs(1),
            Outcome::OverlongExpiry => hint.expires_at = now + Duration::from_secs(3600),
            Outcome::ForeignHost => hint.url = "https://other.invalid/image".into(),
            Outcome::NoUpload => return Ok(None),
            Outcome::Cancel => return Err(ImageUploadError::Cancelled),
            Outcome::Pending => return std::future::pending().await,
            Outcome::Unknown => return Err(ImageUploadError::OutcomeUnknown),
        }
        Ok(Some(hint))
    }
}

fn projection(outcome: Outcome) -> (MediaProjection, Arc<Resolver>) {
    let resolver = Arc::new(Resolver {
        calls: AtomicUsize::new(0),
        outcome,
    });
    (
        MediaProjection::new(config(), resolver.clone()).unwrap(),
        resolver,
    )
}

async fn project(
    projection: &MediaProjection,
    content: &Content,
) -> Result<Option<ProjectedMedia>, Error> {
    let grant = projection.authorize(&scope(), content, &[AUTHORIZED])?;
    projection
        .project_tool_images(grant, CancellationToken::new())
        .await
}

#[tokio::test]
async fn explicit_position_never_projects_identical_retained_or_user_images() {
    let (projection, resolver) = projection(Outcome::Live);
    let canonical = content();
    let before = serde_json::to_vec(&canonical).unwrap();
    for _ in 0..2 {
        let result = project(&projection, &canonical).await.unwrap().unwrap();
        let projected = result.content_if_live().unwrap();
        assert_eq!(projected.items[..2], canonical.items[..2]);
        assert_eq!(projected.system, canonical.system);
        assert!(
            matches!(&projected.items[2], ContentItem::Tool { parts, .. }
            if matches!(&parts[0], InputPart::Image { source: ImageSource::Url { url } } if url.ends_with("private=0"))
                && parts[1] == image())
        );
    }
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    assert_eq!(serde_json::to_vec(&canonical).unwrap(), before);
}

#[tokio::test]
async fn grants_are_bound_to_the_exact_configured_route_and_authority() {
    let (first, resolver) = projection(Outcome::Live);
    let mut other = config();
    other.scope.authority = "different-authority".into();
    let second = MediaProjection::new(other, resolver.clone()).unwrap();
    let content = content();
    assert!(second.authorize(&scope(), &content, &[AUTHORIZED]).is_err());
    assert!(
        first
            .authorize(
                &scope(),
                &content,
                &[ToolImagePosition { item: 0, part: 0 }]
            )
            .is_err()
    );
    assert!(
        first
            .authorize(&scope(), &content, &[AUTHORIZED, AUTHORIZED])
            .is_err()
    );
    let grant = first.authorize(&scope(), &content, &[AUTHORIZED]).unwrap();
    assert!(
        second
            .project_tool_images(grant, CancellationToken::new())
            .await
            .is_err()
    );
    let grant = second
        .authorize(&second.config.scope, &content, &[AUTHORIZED])
        .unwrap();
    assert!(
        second
            .project_tool_images(grant, CancellationToken::new())
            .await
            .unwrap()
            .is_some()
    );
    assert!(project(&first, &content).await.unwrap().is_some());
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn rejected_generation_does_not_invalidate_replacement() {
    let (projection, resolver) = projection(Outcome::Live);
    let content = content();
    let first = project(&projection, &content).await.unwrap().unwrap();
    let concurrent = project(&projection, &content).await.unwrap().unwrap();
    first.invalidate();
    assert!(concurrent.content_if_live().is_none());
    let next = project(&projection, &content).await.unwrap().unwrap();
    concurrent.invalidate();
    assert!(next.content_if_live().is_some());
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn invalid_expired_revoked_and_declined_hints_fall_back_to_canonical_data() {
    for outcome in [
        Outcome::Expired,
        Outcome::OverlongExpiry,
        Outcome::ForeignHost,
        Outcome::NoUpload,
    ] {
        let (projection, resolver) = projection(outcome);
        assert!(project(&projection, &content()).await.unwrap().is_none());
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
        assert!(projection.cache.lock().await.is_empty());
    }
    let revoke = CancellationToken::new();
    let (projection, _) = projection(Outcome::Revocable(revoke.clone()));
    let before = project(&projection, &content()).await.unwrap().unwrap();
    assert!(before.content_if_live().is_some());
    revoke.cancel();
    assert!(before.content_if_live().is_none());
    assert!(project(&projection, &content()).await.unwrap().is_none());
    assert!(projection.cache.lock().await.is_empty());
}

#[tokio::test]
async fn cancellation_remains_exact_and_unknown_mutation_is_not_retried() {
    let (cancelled, _) = projection(Outcome::Cancel);
    assert!(matches!(
        project(&cancelled, &content()).await,
        Err(Error::Cancelled)
    ));
    let (pending, resolver) = projection(Outcome::Pending);
    let content = content();
    let grant = pending
        .authorize(&scope(), &content, &[AUTHORIZED])
        .unwrap();
    let cancellation = CancellationToken::new();
    let mut future = Box::pin(pending.project_tool_images(grant, cancellation.clone()));
    assert!(futures_util::poll!(&mut future).is_pending());
    cancellation.cancel();
    assert!(matches!(future.await, Err(Error::Cancelled)));
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    assert!(pending.cache.lock().await.is_empty());
    let (unknown, resolver) = projection(Outcome::Unknown);
    let result = project(&unknown, &content).await;
    let Err(error) = result else {
        panic!("expected unknown outcome error");
    };
    assert_eq!(
        error.diagnostic_reason(),
        "image hosting upload outcome is unknown"
    );
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn hint_admission_requires_exact_bounded_https_without_credentials_or_server_routes() {
    let allowed_hosts = config().allowed_hosts;
    for url in [
        "http://objects.invalid/a",
        "https://objects.invalid:444/a",
        "https://objects.invalid:443/a",
        "https://user:secret@objects.invalid/a",
        "https://objects.invalid/a#private",
        "https://other.invalid/a",
        "https://sub.objects.invalid/a",
        "https://objects.invalid.other/a",
        "https://objects.invalid./a",
        "https://objects.invalid/a%2fb",
        "https://objects.invalid/api/v1/sessions/s/images/i/0",
        "https://objects.invalid/%61pi/v1/sessions/s/attachments/a",
        "data:image/png;base64,AQID",
        "not a URL",
    ] {
        assert!(!valid_hint(url, &allowed_hosts), "accepted {url}");
    }
    assert!(!valid_hint(
        &format!("https://objects.invalid/{}", "x".repeat(8192)),
        &allowed_hosts
    ));
    assert!(!valid_hint(
        &format!("https://objects.invalid/a?{}", "x".repeat(4097)),
        &allowed_hosts
    ));
    assert!(valid_hint(
        "https://objects.invalid/a?signature=private",
        &allowed_hosts
    ));
    for host in [
        "*",
        "*.objects.invalid",
        "objects.invalid:443",
        "127.0.0.1",
        "OBJECTS.INVALID",
        "objects.invalid/",
    ] {
        assert!(!valid_host(host));
    }
}

#[tokio::test]
async fn full_cache_uses_data_without_evicting_live_stable_hints() {
    let (projection, resolver) = projection(Outcome::Live);
    for n in 0..=MAX_HINTS {
        let mut canonical = content();
        if let ContentItem::Tool { parts, .. } = &mut canonical.items[AUTHORIZED.item] {
            parts[0] = InputPart::Image {
                source: ImageSource::Data {
                    media_type: "image/png".into(),
                    data: n.to_be_bytes().to_vec(),
                },
            };
        }
        assert_eq!(
            project(&projection, &canonical).await.unwrap().is_some(),
            n < MAX_HINTS
        );
    }
    assert_eq!(resolver.calls.load(Ordering::SeqCst), MAX_HINTS);
    assert_eq!(projection.cache.lock().await.len(), MAX_HINTS);
}
