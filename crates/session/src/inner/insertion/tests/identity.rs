//! Exact input admission certificates never enter frozen content or rejected attempts.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Default)]
struct IdentityClient {
    admitted: Mutex<Vec<(u64, usize, String, ContentItem)>>,
    fail_once: AtomicBool,
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
        _: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
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
