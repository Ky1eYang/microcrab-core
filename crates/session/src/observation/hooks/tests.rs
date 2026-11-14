//! Hook projection privacy and cancellation retain the existing owner contract.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn audit_projection_excludes_private_payloads_and_preserves_failure_position() {
    let body = AuditBody {
        epoch: 8,
        turn_id: Some(5),
        round: Some(2),
        sent_request: Some("token=secret /private/request".into()),
        response: Some("password=secret /private/response".into()),
        call_id: Some("private-call-id".into()),
        outcome: Some(microcrab_auditor::AuditOutcome::Cancelled),
        ..Default::default()
    };
    let observation = audit_observation(&body);
    assert_eq!(observation.kind, ObservationKind::Audit);
    assert_eq!(
        observation.position,
        Position {
            epoch: 8,
            turn: Some(5),
            round: Some(2)
        }
    );
    assert_eq!(observation.failure, Some(FailureClass::Cancelled));
    let projection = format!("{observation:?}");
    for private in ["secret", "/private", "private-call-id"] {
        assert!(!projection.contains(private));
    }
}

struct PendingHook {
    polls: AtomicUsize,
    publications: AtomicUsize,
}
impl Auditor for PendingHook {
    fn is_enabled(&self) -> bool {
        true
    }
    fn publish(&self, _: Moment, _: AuditBody) {
        self.publications.fetch_add(1, Ordering::SeqCst);
    }
    fn intervene<'a>(
        &'a self,
        _: Moment,
        _: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
        Box::pin(async move {
            self.polls.fetch_add(1, Ordering::SeqCst);
            std::future::pending().await
        })
    }
}

#[tokio::test]
async fn cancelled_hook_does_not_poll_embedder_or_publish_a_success() {
    let hook = PendingHook {
        polls: AtomicUsize::new(0),
        publications: AtomicUsize::new(0),
    };
    let state = State::new(crate::Limits::default());
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let result = intervene(
        &hook,
        &state,
        Moment::SessionCreated,
        HookPayload::None,
        &cancellation,
    )
    .await;
    assert!(matches!(result, Err(Error::Cancelled)));
    assert_eq!(hook.polls.load(Ordering::SeqCst), 0);
    assert_eq!(hook.publications.load(Ordering::SeqCst), 0);
}
