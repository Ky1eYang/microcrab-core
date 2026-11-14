//! Whole-chain invocation policy regressions and review routing coverage.
use super::*;
use serde_json::json;
mod admission;
mod concurrency;
struct WaitingAsk;
#[async_trait]
impl SessionPermission for WaitingAsk {
    async fn evaluate(
        &self,
        policy: &PolicySnapshot,
        request: &PermissionRequest,
    ) -> Result<Decision, PermissionError> {
        evaluate(policy, request).map_err(|e| PermissionError::InvalidConfiguration(e.to_string()))
    }
    fn features(&self) -> PermissionFeatures {
        PermissionFeatures {
            interactive_ask: true,
            ..Default::default()
        }
    }
    async fn ask(&self, _: &PermissionRequest) -> Result<AskReply, PermissionError> {
        std::future::pending().await
    }
}
use admission::{batch_rule, invocation, issuer, request};
struct ReviewProbe {
    action: Action,
    calls: std::sync::atomic::AtomicUsize,
}
#[async_trait]
impl crate::Client for ReviewProbe {
    async fn complete(
        &self,
        content: &crate::context::Content,
        _: &dyn crate::Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<microcrab_protocol::Response, microcrab_transport::Error> {
        assert_eq!(content.subagent.as_deref(), Some("action_review"));
        assert!(content.tools.is_empty());
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(microcrab_protocol::Response {
            content: serde_json::to_string(&AskReply {
                action: self.action,
                reason: "review fixture".into(),
            })
            .unwrap(),
            tool_calls: vec![],
            usage: Default::default(),
            provider_metadata: None,
        })
    }
}
fn review_probe(action: Action) -> Arc<ReviewProbe> {
    Arc::new(ReviewProbe {
        action,
        calls: std::sync::atomic::AtomicUsize::new(0),
    })
}
struct DelayedReview(Arc<ReviewProbe>);
#[async_trait]
impl crate::Client for DelayedReview {
    async fn complete(
        &self,
        content: &crate::context::Content,
        auditor: &dyn crate::Auditor,
        state: &dyn microcrab_auditor::VariableView,
        cancellation: CancellationToken,
    ) -> Result<microcrab_protocol::Response, microcrab_transport::Error> {
        tokio::time::sleep(std::time::Duration::from_secs(6)).await;
        crate::Client::complete(self.0.as_ref(), content, auditor, state, cancellation).await
    }
}

#[tokio::test(start_paused = true)]
async fn review_wait_exceeds_tool_budget_and_preserves_complete_user_grace() {
    let controller = Arc::new(PermissionController::new(Arc::new(WaitingAsk)));
    let before = controller.snapshot().unwrap();
    let mut config = before.config;
    config.review = true;
    config.overrides = vec![batch_rule(Action::Ask)];
    controller.replace(before.revision, config).unwrap();
    let probe = review_probe(Action::Allow);
    controller
        .bind(
            Arc::new(DelayedReview(probe.clone())),
            Arc::new(crate::NoopAuditor),
        )
        .unwrap();
    controller
        .bind_one_shot_executor(crate::Session::one_shot_executor())
        .unwrap();
    let context = invocation();
    let started = tokio::time::Instant::now();
    let request = request(&controller, &context, json!({}));
    controller
        .admit(&request, &context, &issuer(), None)
        .await
        .unwrap();
    assert!(started.elapsed() >= std::time::Duration::from_secs(11));
    assert_eq!(probe.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

fn review_selection() -> ReviewModelSelection {
    ReviewModelSelection {
        provider_id: "review-provider".into(),
        model: "review-model".into(),
        variant: None,
    }
}
#[tokio::test(start_paused = true)]
async fn dedicated_review_is_revision_pinned_and_null_returns_to_main_without_mutating_context() {
    let controller = PermissionController::new(Arc::new(WaitingAsk));
    let main = review_probe(Action::Deny);
    let dedicated = review_probe(Action::Allow);
    controller
        .bind(
            main.clone(),
            Arc::new(microcrab_auditor::ChannelAuditor::disconnected()),
        )
        .unwrap();
    controller
        .bind_one_shot_executor(crate::Session::one_shot_executor())
        .unwrap();
    let before = controller.snapshot().unwrap();
    let mut config = before.config.clone();
    config.overrides = vec![batch_rule(Action::Ask)];
    config.review_model = Some(review_selection());
    assert!(
        controller
            .replace_user(before.revision, config.clone())
            .is_err()
    );
    assert!(
        controller
            .replace_user_with_review(before.revision, config.clone(), None)
            .is_err()
    );
    assert_eq!(controller.snapshot().unwrap(), before);
    let policy = controller
        .replace_user_with_review(before.revision, config, Some(dedicated.clone()))
        .unwrap();
    let request = PermissionRequest {
        subject: Default::default(),
        session_id: "s".into(),
        call_id: "c".into(),
        agent: String::new(),
        mode: String::new(),
        tool: "job".into(),
        arguments: json!({}),
        revision: policy.revision,
    };
    let state = crate::State::new(Default::default());
    let issuer = crate::context::Content {
        epoch: 0,
        subagent: None,
        system: "review fixture".into(),
        items: vec![],
        tools: vec![],
        max_output_tokens: 1024,
        max_bytes: 65536,
        turn_id: Some(1),
        round: Some(1),
    };
    let original = issuer.clone();
    let cancellation = CancellationToken::new();
    assert_eq!(
        controller
            .resolve_ask(&policy, &request, &state, &issuer, &cancellation)
            .await
            .unwrap()
            .0,
        Action::Allow
    );
    assert_eq!(dedicated.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(main.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    let mut follow = policy.config.clone();
    follow.review_model = None;
    let next = controller
        .replace_user_with_review(policy.revision, follow, None)
        .unwrap();
    assert!(matches!(
        controller
            .resolve_ask(&policy, &request, &state, &issuer, &cancellation)
            .await,
        Err(PermissionError::StalePolicy)
    ));
    let next_request = PermissionRequest {
        revision: next.revision,
        ..request.clone()
    };
    assert_eq!(
        controller
            .resolve_ask(&next, &next_request, &state, &issuer, &cancellation)
            .await
            .unwrap()
            .0,
        Action::Deny
    );
    assert_eq!(main.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(issuer.items, original.items);
    assert_eq!(issuer.system, original.system);
    let (snapshot, bound) = controller.review_binding().unwrap();
    assert_eq!(snapshot, next);
    assert!(bound.is_none());
}

#[test]
fn review_model_identity_is_typed_and_absence_keeps_legacy_permission_bytes() {
    let config = PermissionConfig::default();
    let bytes = serde_json::to_value(&config).unwrap();
    assert!(bytes.get("review_model").is_none());
    assert_eq!(
        serde_json::from_value::<PermissionConfig>(bytes).unwrap(),
        config
    );
    let mut selected = config;
    selected.review_model = Some(review_selection());
    let value = serde_json::to_value(&selected).unwrap();
    assert_eq!(value["review_model"]["provider_id"], "review-provider");
    assert_eq!(
        serde_json::from_value::<PermissionConfig>(value).unwrap(),
        selected
    );
    selected.review_model.as_mut().unwrap().provider_id.clear();
    assert!(validate(&selected).is_err());
}
