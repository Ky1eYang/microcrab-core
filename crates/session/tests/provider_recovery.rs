//! Typed model failures preserve a coherent session for the next user input.
use microcrab_protocol::{Response, Usage};
use microcrab_session::context::Content;
use microcrab_session::runtime::{Runtime, journal::MemoryJournal, tool::DenyAll};
use microcrab_session::{Auditor, Client, Error, Session};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

struct OnceFailed(
    Mutex<Option<microcrab_transport::Error>>,
    Arc<Mutex<Vec<serde_json::Value>>>,
);
#[async_trait::async_trait]
impl Client for OnceFailed {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.1
            .lock()
            .unwrap()
            .push(serde_json::to_value(content).unwrap());
        if let Some(error) = self.0.lock().unwrap().take() {
            return Err(error);
        }
        Ok(Response {
            provider_metadata: None,
            content: "next input completed".into(),
            tool_calls: vec![],
            usage: Usage::default(),
        })
    }
}
#[tokio::test]
async fn typed_provider_failure_settles_input_and_allows_the_next_user_turn() {
    for failure in [
        microcrab_transport::Error::ProviderStatus { status: 502 },
        microcrab_transport::Error::Transport,
        microcrab_transport::Error::TransportFailure("DNS lookup failed; token=secret".into()),
        microcrab_transport::Error::Protocol("invalid SSE UTF-8 at offset 8".into()),
        microcrab_transport::Error::ProviderTimeout,
        microcrab_transport::Error::Provider("provider unavailable".into()),
    ] {
        let journal = Arc::new(MemoryJournal::new());
        let session = Session::builder(
            OnceFailed(Mutex::new(Some(failure)), Arc::new(Mutex::new(vec![]))),
            Runtime::new(vec![], Arc::new(DenyAll)).unwrap(),
        )
        .journal_arc(journal.clone())
        .build()
        .unwrap();
        assert!(matches!(
            session.submit("first input").await,
            Err(Error::ProviderStatus { .. }
                | Error::Transport
                | Error::TransportFailure(_)
                | Error::Protocol(_)
                | Error::ProviderTimeout
                | Error::Provider(_))
        ));
        let settled = journal.latest().unwrap();
        assert_eq!(
            serde_json::to_value(&settled.state).unwrap()["last_turn_id"],
            1
        );
        assert_eq!(settled.state.turn_id, Some(1));
        assert!(settled.continuation.as_ref().unwrap().request_prepared);
        assert!(session.retry_ready());
        assert_eq!(
            session.submit("second input").await.unwrap().content,
            "next input completed"
        );
        assert_eq!(
            serde_json::to_value(session.state()).unwrap()["last_turn_id"],
            2
        );
        session.close().await.unwrap();
    }
}
#[tokio::test]
async fn retry_and_restore_reuse_the_exact_first_request() {
    let seen = Arc::new(Mutex::new(vec![]));
    let journal = Arc::new(MemoryJournal::new());
    let session = Session::builder(
        OnceFailed(
            Mutex::new(Some(microcrab_transport::Error::Protocol(
                "response JSON expected colon at line 1 column 8".into(),
            ))),
            seen.clone(),
        ),
        Runtime::new(vec![], Arc::new(DenyAll)).unwrap(),
    )
    .journal_arc(journal.clone())
    .build()
    .unwrap();
    assert!(session.submit("original input").await.is_err());
    let saved = journal.latest().unwrap();
    assert_eq!(saved.continuation.as_ref().unwrap().next_round, 1);
    assert_eq!(saved.continuation.as_ref().unwrap().unattended_rounds, 0);
    let mut stream = session.retry_stream().await.unwrap();
    while let Some(event) = stream.recv().await {
        if matches!(event, Ok(microcrab_session::SessionEvent::Finished { .. })) {
            break;
        }
        assert!(event.is_ok());
    }
    assert!(!session.retry_ready());
    {
        let requests = seen.lock().unwrap();
        assert_eq!(requests[0], requests[1]);
    }
    assert_eq!(
        serde_json::to_value(session.state()).unwrap()["last_turn_id"],
        1
    );
    assert!(session.retry_stream().await.is_err());
    session.close().await.unwrap();

    let restored = Session::restore(
        Session::builder(
            OnceFailed(Mutex::new(None), seen.clone()),
            Runtime::new(vec![], Arc::new(DenyAll)).unwrap(),
        ),
        saved,
    )
    .await
    .unwrap();
    restored.start().await.unwrap();
    assert!(restored.retry_ready());
    restored.resume().await.unwrap();
    {
        let requests = seen.lock().unwrap();
        assert_eq!(requests[0], requests[2]);
    }
    restored.close().await.unwrap();
}

struct CancelledRetry {
    calls: std::sync::atomic::AtomicUsize,
    entered: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl Client for CancelledRetry {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        token: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if call >= 2 {
            return Ok(Response {
                provider_metadata: None,
                content: "retry completed".into(),
                tool_calls: vec![],
                usage: Usage::default(),
            });
        }
        if call == 0 {
            return Err(microcrab_transport::Error::Transport);
        }
        self.entered.notify_one();
        token.cancelled().await;
        Err(microcrab_transport::Error::Cancelled)
    }
}
#[tokio::test]
async fn cancelling_unadmitted_retry_retains_continuation_without_readding_input() {
    let client = Arc::new(CancelledRetry {
        calls: std::sync::atomic::AtomicUsize::new(0),
        entered: tokio::sync::Notify::new(),
    });
    let session = Session::builder_arc(
        client.clone(),
        Runtime::new(vec![], Arc::new(DenyAll)).unwrap(),
    )
    .build()
    .unwrap();
    assert!(session.submit("original").await.is_err());
    let running = session.clone();
    let retry = tokio::spawn(async move { running.resume().await });
    tokio::time::timeout(std::time::Duration::from_secs(5), client.entered.notified())
        .await
        .unwrap();
    session.interrupt();
    assert!(matches!(retry.await.unwrap(), Err(Error::Cancelled)));
    assert!(session.has_continuation());
    assert!(session.retry_ready());
    assert_eq!(session.resume().await.unwrap().content, "retry completed");
    assert!(!session.retry_ready());
    session.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn cancelling_retry_before_worker_start_settles_without_a_provider_call() {
    let seen = Arc::new(Mutex::new(vec![]));
    let journal = Arc::new(MemoryJournal::new());
    let session = Session::builder(
        OnceFailed(
            Mutex::new(Some(microcrab_transport::Error::Protocol(
                "response JSON expected colon at line 1 column 8".into(),
            ))),
            seen.clone(),
        ),
        Runtime::new(vec![], Arc::new(DenyAll)).unwrap(),
    )
    .journal_arc(journal.clone())
    .build()
    .unwrap();
    assert!(session.submit("original").await.is_err());
    assert!(session.retry_ready());
    let original_messages = session.messages();
    let token = CancellationToken::new();
    let mut stream = session
        .retry_stream_with_cancellation(token.clone())
        .await
        .unwrap();
    // Current-thread admission does not yield before spawning the worker. Cancel
    // synchronously, before polling the stream or allowing that worker to run.
    token.cancel();
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(5), stream.recv())
            .await
            .unwrap(),
        Some(Err(Error::Cancelled))
    ));
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert_eq!(session.messages(), original_messages);
    assert!(session.has_continuation());
    assert!(session.retry_ready());
    assert!(
        journal
            .latest()
            .unwrap()
            .continuation
            .as_ref()
            .unwrap()
            .request_prepared
    );
    // Terminal delivery must release foreground admission as well as settling
    // persistence, so the next user turn can start immediately.
    session.submit("next input").await.unwrap();
    assert_eq!(seen.lock().unwrap().len(), 2);
    session.close().await.unwrap();
}

#[tokio::test]
async fn already_cancelled_submission_does_not_call_the_provider_or_append_input() {
    let seen = Arc::new(Mutex::new(vec![]));
    let session = Session::builder(
        OnceFailed(Mutex::new(None), seen.clone()),
        Runtime::new(vec![], Arc::new(DenyAll)).unwrap(),
    )
    .build()
    .unwrap();
    session.start().await.unwrap();
    let original_messages = session.messages();
    let token = CancellationToken::new();
    token.cancel();
    let mut stream = session
        .stream_with_cancellation("must not be admitted", token)
        .await
        .unwrap();
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(5), stream.recv())
            .await
            .unwrap(),
        Some(Err(Error::Cancelled))
    ));
    assert!(seen.lock().unwrap().is_empty());
    assert_eq!(session.messages(), original_messages);
    assert!(!session.has_continuation());
    session.submit("next input").await.unwrap();
    assert_eq!(seen.lock().unwrap().len(), 1);
    session.close().await.unwrap();
}

struct SettlementClient {
    calls: std::sync::atomic::AtomicUsize,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl Client for SettlementClient {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if call >= 2 {
            return Ok(Response {
                provider_metadata: None,
                content: "retry completed".into(),
                tool_calls: vec![],
                usage: Usage::default(),
            });
        }
        if call == 0 {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(Response {
            provider_metadata: None,
            content: "settled".into(),
            tool_calls: vec![],
            usage: Usage::default(),
        })
    }
}

#[tokio::test]
async fn explicit_stream_settlement_fence_delays_next_inbox_turn() {
    let client = Arc::new(SettlementClient {
        calls: std::sync::atomic::AtomicUsize::new(0),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let session = Session::builder_arc(
        client.clone(),
        Runtime::new(vec![], Arc::new(DenyAll)).unwrap(),
    )
    .build()
    .unwrap();
    let mut turns = session.subscribe_turns();
    let (mut stream, settlement) = session
        .stream_with_settlement("foreground", CancellationToken::new())
        .await
        .unwrap();
    client.entered.notified().await;
    session.queue("next input").unwrap();
    client.release.notify_one();
    loop {
        let event = tokio::time::timeout(std::time::Duration::from_secs(5), stream.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if matches!(event, microcrab_session::SessionEvent::Finished { .. }) {
            break;
        }
    }
    assert_eq!(turns.recv().await.unwrap().sequence, 1);
    assert_eq!(client.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(session.pending_inputs().len(), 1);
    assert!(matches!(
        session.submit("competing input").await,
        Err(Error::Busy)
    ));
    drop(settlement);
    let next = tokio::time::timeout(std::time::Duration::from_secs(5), turns.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.sequence, 2);
    assert_eq!(client.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert!(session.pending_inputs().is_empty());
    drop(stream);
    session.close().await.unwrap();
}

#[tokio::test]
async fn client_protocol_failure_retries_the_exact_unadmitted_request() {
    let seen = Arc::new(Mutex::new(vec![]));
    let journal = Arc::new(MemoryJournal::new());
    let session = Session::builder(
        OnceFailed(
            Mutex::new(Some(microcrab_transport::Error::Protocol(
                "invalid response".into(),
            ))),
            seen.clone(),
        ),
        Runtime::new(vec![], Arc::new(DenyAll)).unwrap(),
    )
    .journal_arc(journal.clone())
    .build()
    .unwrap();
    assert!(matches!(
        session.submit("first input").await,
        Err(Error::Protocol(_))
    ));
    assert!(session.retry_ready());
    let saved = journal.latest().unwrap();
    assert!(saved.continuation.as_ref().unwrap().request_prepared);
    let boundary = serde_json::to_value(session.context_snapshot().unwrap()).unwrap();
    assert!(!boundary.to_string().contains("next input completed"));
    assert!(session.execution_snapshots().is_empty());
    assert_eq!(
        session.resume().await.unwrap().content,
        "next input completed"
    );
    let requests = seen.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0], requests[1]);
    drop(requests);
    assert!(!session.retry_ready());
    session.close().await.unwrap();
}

struct FailingRoundCompleted(Arc<std::sync::atomic::AtomicUsize>);
impl Auditor for FailingRoundCompleted {
    // This fixture tests the intervention barrier; observations are intentionally ignored.
    fn publish(&self, _: microcrab_auditor::Moment, _: microcrab_auditor::AuditBody) {}

    fn is_enabled(&self) -> bool {
        true
    }
    fn intervene<'a>(
        &'a self,
        moment: microcrab_auditor::Moment,
        _: microcrab_auditor::HookContext<'a>,
    ) -> microcrab_auditor::BoxFuture<
        'a,
        Result<microcrab_auditor::Injection, microcrab_auditor::AuditorError>,
    > {
        Box::pin(async move {
            if moment == microcrab_auditor::Moment::RoundCompleted {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return Err(microcrab_auditor::AuditorError(
                    "round audit write failed; token=private-audit-secret".into(),
                ));
            }
            Ok(microcrab_auditor::Injection::default())
        })
    }
}

#[tokio::test]
async fn effectful_round_auditor_failure_still_quarantines_without_retry() {
    let hooks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(vec![]));
    let session = Session::builder(
        OnceFailed(Mutex::new(None), seen.clone()),
        Runtime::new(vec![], Arc::new(DenyAll)).unwrap(),
    )
    .auditor(FailingRoundCompleted(hooks.clone()))
    .build()
    .unwrap();
    let error = session.submit("first input").await.unwrap_err();
    assert!(matches!(&error, Error::Lifecycle(_)));
    assert!(
        error
            .diagnostic_reason()
            .contains("round audit write failed")
    );
    assert!(!error.diagnostic_reason().contains("private-audit-secret"));
    assert_eq!(hooks.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert!(!session.retry_ready());
    assert!(
        matches!(session.submit("second input").await, Err(Error::Config(message)) if message.contains("faulted"))
    );
    assert_eq!(hooks.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(seen.lock().unwrap().len(), 1);
    session.close().await.unwrap();
}
