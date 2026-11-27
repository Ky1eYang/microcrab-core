//! Pending reference callbacks cannot retain Session admission past cancellation/deadlines.
use microcrab_protocol::{Response, Usage};
use microcrab_session::context::Content;
use microcrab_session::runtime::{
    Runtime,
    journal::{Journal, MemoryJournal, Recover},
    tool::DenyAll,
};
use microcrab_session::{
    Auditor, Client, DynReference, Error, Limits, Reference, ReferenceCapabilities,
    ReferenceContent, ReferenceEntry, ReferenceError, ReferenceProjection, Session,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Pending {
    blocked: AtomicBool,
    entered: Notify,
}
struct PendingReference(Arc<Pending>);
#[async_trait::async_trait]
impl Reference for PendingReference {
    fn kind(&self) -> &str {
        "pending_reference"
    }
    fn capabilities(&self) -> ReferenceCapabilities {
        ReferenceCapabilities::read_only()
    }
    async fn load(&self, _cancel: CancellationToken) -> Result<(), ReferenceError> {
        if self.0.blocked.load(Ordering::SeqCst) {
            self.0.entered.notify_one();
            std::future::pending::<()>().await;
        }
        Ok(())
    }
    fn list(&self) -> Result<Vec<ReferenceEntry>, ReferenceError> {
        Ok(vec![])
    }
    fn projections(&self) -> Result<ReferenceProjection, ReferenceError> {
        Ok(ReferenceProjection::empty())
    }
    async fn expand(
        &self,
        _: &str,
        _cancel: CancellationToken,
    ) -> Result<Option<ReferenceContent>, ReferenceError> {
        Ok(None)
    }
}
struct Answer;
#[async_trait::async_trait]
impl Client for Answer {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        Ok(Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: vec![],
            usage: Usage::default(),
        })
    }
}
fn make_session(pending: &Arc<Pending>, timeout: u64, journal: Arc<dyn Journal>) -> Session {
    Session::builder(Answer, Runtime::new(vec![], Arc::new(DenyAll)).unwrap())
        .references(vec![
            DynReference::new(PendingReference(pending.clone())).unwrap(),
        ])
        .limits(Limits {
            lifecycle_timeout_ms: timeout,
            ..Limits::default()
        })
        .journal_arc(journal)
        .build()
        .unwrap()
}

#[tokio::test]
async fn main_submit_reference_load_obeys_interrupt_and_close_before_admission() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for close in [false, true] {
            let pending = Arc::new(Pending::default());
            pending.blocked.store(true, Ordering::SeqCst);
            let session = make_session(&pending, 30_000, Arc::new(MemoryJournal::new()));
            let running = session.clone();
            let submitted = tokio::spawn(async move { running.submit("first input").await });
            pending.entered.notified().await;
            if close {
                session.close().await.unwrap();
            } else {
                session.interrupt();
            }
            assert!(matches!(submitted.await.unwrap(), Err(Error::Cancelled)));
            assert_eq!(session.state().turn_id, None);
            assert!(session.messages().is_empty());
            if !close {
                // Cancellation before lifecycle admission must remain retryable.
                pending.blocked.store(false, Ordering::SeqCst);
                assert_eq!(session.submit("retry").await.unwrap().content, "done");
                session.close().await.unwrap();
            }
        }
    })
    .await
    .expect("pending initial references must not block interrupt or close");
}

#[tokio::test]
async fn direct_start_reference_load_has_a_deadline_and_can_be_closed() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let pending = Arc::new(Pending::default());
        pending.blocked.store(true, Ordering::SeqCst);
        let session = make_session(&pending, 30, Arc::new(MemoryJournal::new()));
        assert!(matches!(session.start().await, Err(Error::Lifecycle(message)) if message.contains("reference loading deadline")));
        pending.blocked.store(false, Ordering::SeqCst);
        session.start().await.unwrap();
        session.close().await.unwrap();

        let pending = Arc::new(Pending::default());
        pending.blocked.store(true, Ordering::SeqCst);
        let session = make_session(&pending, 30_000, Arc::new(MemoryJournal::new()));
        let running = session.clone();
        let started = tokio::spawn(async move { running.start().await });
        pending.entered.notified().await;
        session.close().await.unwrap();
        assert!(matches!(started.await.unwrap(), Err(Error::Cancelled)));
    }).await.expect("direct start needs bounded and registered initialization cancellation");
}

#[derive(Default)]
struct Publications {
    memory: MemoryJournal,
    recovered: AtomicUsize,
}
#[async_trait::async_trait]
impl Journal for Publications {
    async fn commit(&self, recover: Recover) -> Result<(), Error> {
        self.memory.commit(recover).await
    }
    async fn recovered(&self, _: Recover) -> Result<(), Error> {
        self.recovered.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
#[tokio::test]
async fn published_recovery_reference_load_is_bounded_and_faults_on_incomplete_refresh() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for action in ["interrupt", "close", "deadline"] {
            let pending = Arc::new(Pending::default());
            let journal = Arc::new(Publications::default());
            let session = make_session(&pending, if action == "deadline" { 30 } else { 30_000 }, journal.clone());
            session.start().await.unwrap();
            let recover = session.checkpoint().await.unwrap();
            pending.blocked.store(true, Ordering::SeqCst);
            let running = session.clone();
            let recovery = tokio::spawn(async move { running.recover(recover).await });
            pending.entered.notified().await;
            if action == "close" { session.close().await.unwrap(); }
            if action == "interrupt" { session.interrupt(); }
            let result = recovery.await.unwrap();
            if action == "deadline" {
                assert!(matches!(result, Err(Error::Lifecycle(message)) if message.contains("reference loading deadline")));
            } else {
                assert!(matches!(result, Err(Error::Cancelled)));
            }
            assert_eq!(journal.recovered.load(Ordering::SeqCst), 1);
            if action != "close" {
                assert!(matches!(session.submit("cannot use partial recovery").await, Err(Error::Config(message)) if message.contains("faulted")));
                session.close().await.unwrap();
            }
        }
    }).await.expect("published recovery must release admission and quarantine incomplete refresh");
}
