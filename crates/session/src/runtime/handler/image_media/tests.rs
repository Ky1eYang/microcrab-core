use super::*;
use async_trait::async_trait;
use microcrab_auditor::{Auditor, NoopAuditor, VariableView};
use microcrab_protocol::Response;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

// The actual dispatch port is 'static; keep the fixture's owned counter.
struct Fixture {
    count: Arc<AtomicUsize>,
    status: Option<u16>,
    partial: bool,
}
#[async_trait]
impl Client for Fixture {
    fn public_https_image_attempts(&self) -> u8 {
        2
    }
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        panic!("must use single attempt port")
    }
    async fn stream_public_https_image_attempt(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
        events: Option<mpsc::Sender<ModelEvent>>,
    ) -> Result<ResponseOutcome, microcrab_transport::Error> {
        assert_eq!(self.count.fetch_add(1, Ordering::SeqCst), 0);
        assert!(
            matches!(content.items.last(), Some(ContentItem::User { parts, .. }) if matches!(parts.last(), Some(InputPart::Image { source: ImageSource::Url { .. } })))
        );
        if self.partial {
            events
                .unwrap()
                .send(ModelEvent::TextDelta {
                    text: "partial".into(),
                })
                .await
                .unwrap();
        }
        match self.status {
            None => Ok(Response {
                content: "ok".into(),
                tool_calls: Vec::new(),
                usage: Default::default(),
                provider_metadata: None,
            }
            .into()),
            Some(status) => Err(microcrab_transport::Error::ProviderStatus { status }),
        }
    }
}

#[tokio::test]
async fn provider_url_attempt_always_precedes_the_single_archive() {
    let preview = Content {
        epoch: 0,
        subagent: None,
        system: "system".into(),
        items: vec![media_carrier(
            "view-a",
            InputPart::Image {
                source: ImageSource::Url {
                    url: "https://example.invalid/raster.png?q=opaque".into(),
                },
            },
            None,
        )],
        tools: Vec::new(),
        max_output_tokens: 255,
        max_bytes: 1024,
        turn_id: Some(1),
        round: Some(2),
    };
    for (status, partial) in [(None, false), (Some(403), false), (Some(400), true)] {
        let count = Arc::new(AtomicUsize::new(0));
        let model = Fixture {
            count: count.clone(),
            status,
            partial,
        };
        let result = first_attempt_and_archive(
            &model,
            &preview,
            &NoopAuditor,
            &State::from_snapshot(crate::SessionState::new(Default::default())),
            &CancellationToken::new(),
            async || {
                assert_eq!(count.fetch_add(1, Ordering::SeqCst), 1);
                Ok(42)
            },
        )
        .await
        .unwrap();
        assert_eq!(result.0.is_ok(), status.is_none());
        assert_eq!(result.1, partial);
        assert_eq!(result.2, 42);
        assert_eq!(count.load(Ordering::SeqCst), 2);
        if let Err(error) = result.0 {
            assert!(!should_fallback(&error, partial, 2));
        }
    }
}
