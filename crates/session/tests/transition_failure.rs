//! Initialization and admitted model-hook failures cannot reuse partial transitions.
use microcrab_auditor::{
    AuditBody, Auditor, AuditorError, BoxFuture, HookContext, Injection, Moment, VariableView,
};
use microcrab_protocol::Response;
use microcrab_session::context::Content;
use microcrab_session::runtime::{Runtime, tool::DenyAll};
use microcrab_session::{Client, Error, Session};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Answer(AtomicUsize);
#[async_trait::async_trait]
impl Client for Answer {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: vec![],
            usage: Default::default(),
        })
    }
}
struct Failure {
    target: Moment,
    pending: bool,
    enabled: AtomicBool,
    entered: Notify,
}
impl Auditor for Failure {
    fn is_enabled(&self) -> bool {
        true
    }
    fn publish(&self, _: Moment, _: AuditBody) {}
    fn intervene<'a>(
        &'a self,
        moment: Moment,
        _: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
        Box::pin(async move {
            if moment == self.target && self.enabled.load(Ordering::SeqCst) {
                self.entered.notify_one();
                if self.pending {
                    std::future::pending::<()>().await;
                }
                return Err(AuditorError("injected hook failure".into()));
            }
            Ok(Injection::default())
        })
    }
}
fn runtime() -> Runtime {
    Runtime::new(vec![], Arc::new(DenyAll)).unwrap()
}
fn failure(target: Moment, pending: bool) -> Arc<Failure> {
    Arc::new(Failure {
        target,
        pending,
        enabled: AtomicBool::new(true),
        entered: Notify::new(),
    })
}

#[tokio::test]
async fn failed_round_completed_hook_faults_before_any_future_provider_request() {
    let client = Arc::new(Answer::default());
    let hook = failure(Moment::RoundCompleted, false);
    let session = Session::builder_arc(client.clone(), runtime())
        .auditor(hook)
        .build()
        .unwrap();
    assert!(matches!(
        session.submit("work").await,
        Err(Error::Lifecycle(_))
    ));
    assert_eq!(client.0.load(Ordering::SeqCst), 1);
    assert!(!session.messages().iter().any(|item| matches!(
        item,
        microcrab_session::context::ContentItem::Assistant { .. }
    )));
    assert!(matches!(
        session.submit("again").await,
        Err(Error::Config(_))
    ));
    assert_eq!(client.0.load(Ordering::SeqCst), 1);
    session.close().await.unwrap();
}

#[tokio::test]
async fn dropped_initialization_or_restoration_hook_faults_instead_of_reusing_a_partial_start() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        for moment in [Moment::SessionCreated, Moment::SessionRestored] {
            let client = Arc::new(Answer::default());
            let hook = failure(moment, true);
            let session = if moment == Moment::SessionRestored {
                let original = Session::builder(Answer::default(), runtime())
                    .build()
                    .unwrap();
                original.start().await.unwrap();
                let saved = original.checkpoint().await.unwrap();
                original.close().await.unwrap();
                Session::restore(
                    Session::builder_arc(client.clone(), runtime()).auditor(hook.clone()),
                    saved,
                )
                .await
                .unwrap()
            } else {
                Session::builder_arc(client.clone(), runtime())
                    .auditor(hook.clone())
                    .build()
                    .unwrap()
            };
            let running = session.clone();
            let start = tokio::spawn(async move { running.start().await });
            hook.entered.notified().await;
            start.abort();
            assert!(start.await.unwrap_err().is_cancelled());
            assert!(matches!(
                session.submit("must not reach provider").await,
                Err(Error::Config(_))
            ));
            assert_eq!(client.0.load(Ordering::SeqCst), 0);
            session.close().await.unwrap();
        }
    })
    .await
    .expect("dropped start hooks must release admission and fault partial initialization");
}

#[tokio::test]
async fn failed_model_hook_after_pending_epoch_materialization_cannot_reuse_the_new_epoch() {
    let client = Arc::new(Answer::default());
    let hook = failure(Moment::RoundCompleted, false);
    hook.enabled.store(false, Ordering::SeqCst);
    let session = Session::builder_arc(client.clone(), runtime())
        .auditor(hook.clone())
        .build()
        .unwrap();
    session.start().await.unwrap();
    session.compact("summary", json!({})).await.unwrap();
    hook.enabled.store(true, Ordering::SeqCst);
    assert!(matches!(
        session.submit("materialize epoch").await,
        Err(Error::Lifecycle(_))
    ));
    assert_eq!(session.state().epoch_id, 1);
    assert_eq!(client.0.load(Ordering::SeqCst), 1);
    assert!(matches!(
        session.submit("again").await,
        Err(Error::Config(_))
    ));
    session.close().await.unwrap();
}
