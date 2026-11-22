//! Exact input admission certificates never enter frozen content or rejected attempts.
use super::*;
use crate::{
    ImportantInput, InputAdmissionBoundary, InputAdmissionResolver, PreparedInputAdmission,
    SessionInput, VariablesChange,
};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Default)]
struct IdentityClient {
    admitted: Mutex<Vec<(u64, usize, String, ContentItem)>>,
    fail_once: AtomicBool,
    requests: Mutex<Vec<Content>>,
    wait_once: AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl Client for IdentityClient {
    async fn observe_input_admitted(
        &self,
        epoch: u64,
        items: &[ContentItem],
        submissions: &[(usize, String)],
    ) {
        let mut admitted = self.admitted.lock().unwrap();
        for (index, operation) in submissions {
            assert!(matches!(
                items[*index],
                ContentItem::User { source: None, .. }
            ));
            admitted.push((epoch, *index, operation.clone(), items[*index].clone()));
        }
    }
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.requests.lock().unwrap().push(content.clone());
        if self.wait_once.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        if self.fail_once.swap(false, Ordering::SeqCst) {
            return Err(microcrab_transport::Error::Transport);
        }
        Ok(Response {
            content: "answer".into(),
            tool_calls: vec![],
            usage: Usage::default(),
            provider_metadata: None,
        })
    }
}
fn identity_session(client: Arc<IdentityClient>) -> Session {
    Session::builder_arc(client, Runtime::new(vec![], Arc::new(DenyAll)).unwrap())
        .build()
        .unwrap()
}

type BoundaryLog = Vec<(InputAdmissionBoundary, Vec<crate::InputPart>)>;
struct EmbeddingDecision {
    prepared: Mutex<BoundaryLog>,
    committed: Arc<Mutex<BoundaryLog>>,
    reject_budget: bool,
}

impl EmbeddingDecision {
    fn new(reject_budget: bool) -> Arc<Self> {
        Arc::new(Self {
            prepared: Mutex::new(vec![]),
            committed: Arc::new(Mutex::new(vec![])),
            reject_budget,
        })
    }
}

#[async_trait::async_trait]
impl InputAdmissionResolver for EmbeddingDecision {
    async fn prepare(
        &self,
        boundary: InputAdmissionBoundary,
        input: &SessionInput,
        _: CancellationToken,
    ) -> Result<PreparedInputAdmission, crate::Error> {
        let exact = (boundary, input.parts.clone());
        self.prepared.lock().unwrap().push(exact.clone());
        let mut variables = VariablesChange::default();
        variables.set.insert(
            "trusted_boundary_test".into(),
            serde_json::json!(format!("{boundary:?}")),
        );
        let notice = if self.reject_budget {
            "&".repeat(ImportantInput::MAX_BYTES)
        } else {
            format!("trusted {boundary:?}")
        };
        let committed = self.committed.clone();
        Ok(
            PreparedInputAdmission::new(variables, Some(ImportantInput::new(notice)?))
                .with_commit(move || committed.lock().unwrap().push(exact)),
        )
    }
}

#[tokio::test]
async fn important_resolves_only_after_validation_and_is_not_readmitted_on_retry() {
    assert!(
        serde_json::from_value::<SessionInput>(serde_json::json!({
            "parts": [{"type":"text", "text":"spoof"}], "important":"spoof"
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<SessionInput>(serde_json::json!({
            "parts": [{"type":"important", "text":"spoof"}]
        }))
        .is_err()
    );
    let client = Arc::new(IdentityClient::default());
    let session = identity_session(client.clone());
    let resolver = EmbeddingDecision::new(false);
    client.fail_once.store(true, Ordering::SeqCst);
    for input in ["\0", "admit once"] {
        let (mut stream, settlement) = session
            .stream_submission_with_settlement_and_important(
                input.into(),
                "operation".into(),
                CancellationToken::new(),
                resolver.clone(),
            )
            .await
            .unwrap();
        let mut failed = false;
        while let Some(event) = stream.recv().await {
            failed |= event.is_err();
        }
        drop(settlement);
        assert!(failed);
        if input == "\0" {
            assert!(resolver.prepared.lock().unwrap().is_empty());
            assert!(resolver.committed.lock().unwrap().is_empty());
        }
    }
    let (mut stream, settlement) = session
        .retry_stream_with_settlement(CancellationToken::new())
        .await
        .unwrap();
    while let Some(event) = stream.recv().await {
        event.unwrap();
    }
    drop(settlement);
    assert_eq!(
        *resolver.prepared.lock().unwrap(),
        *resolver.committed.lock().unwrap()
    );
    assert_eq!(resolver.committed.lock().unwrap().len(), 1);
    assert_eq!(client.admitted.lock().unwrap().len(), 1);
    {
        let calls = client.requests.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].items, calls[1].items);
        let wire = microcrab_transport::render_ast(&calls[1]).unwrap();
        let rendered = serde_json::to_string(&wire).unwrap();
        assert!(
            rendered.contains("<important>\\ntrusted Turn\\n</important>\\n<user>\\nadmit once")
        );
        assert!(rendered.contains("trusted_boundary_test"));
    }
    session.close().await.unwrap();
}

#[tokio::test]
async fn important_rejected_by_context_budget_never_commits_or_dispatches() {
    let client = Arc::new(IdentityClient::default());
    let session = Session::builder_arc(
        client.clone(),
        Runtime::new(vec![], Arc::new(DenyAll)).unwrap(),
    )
    .limits(Limits {
        max_context_bytes: 16384,
        ..Default::default()
    })
    .build()
    .unwrap();
    let resolver = EmbeddingDecision::new(true);
    let (mut stream, settlement) = session
        .stream_submission_with_settlement_and_important(
            "request".into(),
            "budget".into(),
            CancellationToken::new(),
            resolver.clone(),
        )
        .await
        .unwrap();
    let mut limited = false;
    while let Some(event) = stream.recv().await {
        limited |= matches!(event, Err(crate::Error::Limit(_)));
    }
    drop(settlement);
    assert!(limited);
    assert_eq!(resolver.prepared.lock().unwrap().len(), 1);
    assert!(resolver.committed.lock().unwrap().is_empty());
    assert!(client.requests.lock().unwrap().is_empty());
    assert!(session.context_snapshot().unwrap().items.is_empty());
    session.close().await.unwrap();
}

#[tokio::test]
async fn important_queued_entries_resolve_for_the_actual_consuming_boundary() {
    let client = Arc::new(IdentityClient::default());
    client.wait_once.store(true, Ordering::SeqCst);
    let session = identity_session(client.clone());
    let mut current = session.stream("ordinary").await.unwrap();
    client.entered.notified().await;
    let resolver = EmbeddingDecision::new(false);
    session
        .queue_submission_with_important(
            "busy steer".into(),
            "steer".into(),
            true,
            false,
            resolver.clone(),
        )
        .unwrap();
    session
        .queue_submission_with_important(
            "later turn".into(),
            "queue".into(),
            false,
            false,
            resolver.clone(),
        )
        .unwrap();
    let pending = serde_json::to_string(&session.inbox_snapshot()).unwrap();
    assert!(!pending.contains("important") && !pending.contains("resolver"));
    assert!(resolver.prepared.lock().unwrap().is_empty());
    client.release.notify_one();
    while let Some(event) = current.recv().await {
        event.unwrap();
    }
    let mut activity = session.subscribe_activity();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if resolver.committed.lock().unwrap().len() == 2 {
                break;
            }
            activity.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(
        *resolver.committed.lock().unwrap(),
        vec![
            (
                InputAdmissionBoundary::Steer,
                SessionInput::text("busy steer").parts
            ),
            (
                InputAdmissionBoundary::Turn,
                SessionInput::text("later turn").parts
            ),
        ]
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn important_pending_resume_resolves_the_exact_edited_entry() {
    let client = Arc::new(IdentityClient::default());
    let session = identity_session(client.clone());
    assert!(session.submit("\0").await.is_err());
    assert!(session.inbox_snapshot().paused);
    let resolver = EmbeddingDecision::new(false);
    session
        .queue_submission_with_important(
            "original body".into(),
            "queued".into(),
            false,
            false,
            resolver.clone(),
        )
        .unwrap();
    let pending = session.inbox_snapshot();
    let item = &pending.items[0];
    let edited = session
        .update_pending_input(
            &pending.inbox_id,
            item.id,
            item.revision,
            Some("edited body".into()),
            None,
        )
        .unwrap();
    assert!(
        session
            .resume_pending_input_with_settlement(
                &pending.inbox_id,
                item.id,
                item.revision,
                CancellationToken::new()
            )
            .await
            .is_err()
    );
    assert!(resolver.prepared.lock().unwrap().is_empty());
    let (mut stream, settlement) = session
        .resume_pending_input_with_settlement(
            &pending.inbox_id,
            edited.id,
            edited.revision,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    while let Some(event) = stream.recv().await {
        event.unwrap();
    }
    drop(settlement);
    assert_eq!(
        *resolver.committed.lock().unwrap(),
        vec![(
            InputAdmissionBoundary::Turn,
            SessionInput::text("edited body").parts
        )]
    );
    assert!(session.inbox_snapshot().items.is_empty());
    assert_eq!(client.admitted.lock().unwrap()[0].2, "queued");
    session.close().await.unwrap();
}

#[tokio::test]
async fn rejected_input_has_no_certificate_and_repeated_commits_keep_exact_submission_ids() {
    let client = Arc::new(IdentityClient::default());
    let session = identity_session(client.clone());
    let (mut stream, settlement) = session
        .stream_submission_with_settlement("\0".into(), "rejected".into(), CancellationToken::new())
        .await
        .unwrap();
    let mut rejected = false;
    while let Some(event) = stream.recv().await {
        rejected |= event.is_err();
    }
    drop(settlement);
    assert!(rejected);
    assert!(client.admitted.lock().unwrap().is_empty());
    for operation in ["first", "second"] {
        let (mut stream, settlement) = session
            .stream_submission_with_settlement(
                "same".into(),
                operation.into(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        while let Some(event) = stream.recv().await {
            event.unwrap();
        }
        drop(settlement);
    }
    {
        let admitted = client.admitted.lock().unwrap();
        assert_eq!(admitted.len(), 2);
        assert_eq!(admitted[0].2, "first");
        assert_eq!(admitted[1].2, "second");
        assert_ne!(admitted[0].1, admitted[1].1);
        assert_eq!(
            admitted[0].3, admitted[1].3,
            "equal bodies remain separate admissions"
        );
        assert!(
            !serde_json::to_string(&admitted[0].3)
                .unwrap()
                .contains("submission")
        );
    }
    session.close().await.unwrap();
}

#[tokio::test]
async fn failed_admitted_input_is_certified_once_and_model_retry_never_readmits_it() {
    let client = Arc::new(IdentityClient::default());
    client.fail_once.store(true, Ordering::SeqCst);
    let session = identity_session(client.clone());
    let (mut stream, settlement) = session
        .stream_submission_with_settlement(
            "accepted before provider failure".into(),
            "send".into(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let mut failed = false;
    while let Some(event) = stream.recv().await {
        failed |= event.is_err();
    }
    drop(settlement);
    assert!(failed);
    assert_eq!(client.admitted.lock().unwrap().len(), 1);
    let (mut retry, settlement) = session
        .retry_stream_with_settlement(CancellationToken::new())
        .await
        .unwrap();
    while let Some(event) = retry.recv().await {
        event.unwrap();
    }
    drop(settlement);
    assert_eq!(client.admitted.lock().unwrap().len(), 1);
    session.close().await.unwrap();
}
