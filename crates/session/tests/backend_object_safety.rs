//! Runtime-selected journals and clients do not change Session's concrete type.
use microcrab_auditor::VariableView;
use microcrab_session::context::Content;
use microcrab_session::protocol::Response;
use microcrab_session::runtime::{
    Runtime,
    journal::{Journal, MemoryJournal, Recover},
    tool::DenyAll,
};
use microcrab_session::{Auditor, Client, Error, Session};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

struct Answer;
#[async_trait::async_trait]
impl Client for Answer {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        Ok(Response {
            provider_metadata: None,
            content: "answer".into(),
            tool_calls: vec![],
            usage: Default::default(),
        })
    }
}
struct Recording(Arc<Mutex<Vec<&'static str>>>);
#[async_trait::async_trait]
impl Journal for Recording {
    async fn commit(&self, recover: Recover) -> Result<(), Error> {
        recover.validate()?;
        self.0.lock().unwrap().push("commit");
        Ok(())
    }
    async fn close(&self) -> Result<(), Error> {
        self.0.lock().unwrap().push("close");
        Ok(())
    }
}
fn assemble(journal: Arc<dyn Journal>) -> Session {
    Session::builder(Answer, Runtime::new(vec![], Arc::new(DenyAll)).unwrap())
        .journal_arc(journal)
        .build()
        .unwrap()
}
#[tokio::test]
async fn dynamically_selected_journals_keep_session_type_and_commit_before_close() {
    let memory = MemoryJournal::new();
    let events = Arc::new(Mutex::new(vec![]));
    let backends: Vec<Arc<dyn Journal>> = vec![
        Arc::new(memory.clone()),
        Arc::new(Recording(events.clone())),
    ];
    let sessions: Vec<Session> = backends.into_iter().map(assemble).collect();
    for session in &sessions {
        assert_eq!(session.submit("hello").await.unwrap().content, "answer");
        session.close().await.unwrap();
    }
    assert!(memory.latest().is_some());
    let events = events.lock().unwrap();
    assert!(events.len() > 1);
    assert_eq!(events.last(), Some(&"close"));
    assert!(
        events[..events.len() - 1]
            .iter()
            .all(|event| *event == "commit")
    );
}

struct View;
impl VariableView for View {
    fn get(&self, name: &str) -> Option<serde_json::Value> {
        (name == "live").then(|| serde_json::json!(42))
    }
}
struct Probe {
    entered: Arc<tokio::sync::Notify>,
}
#[async_trait::async_trait]
impl Client for Probe {
    async fn complete(
        &self,
        content: &Content,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        cancellation: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        assert_eq!(content.system, "unchanged system");
        assert_eq!(variables.get("live"), Some(serde_json::json!(42)));
        auditor.publish(
            microcrab_auditor::Moment::RequestPrepared,
            microcrab_auditor::AuditBody::default(),
        );
        self.entered.notify_one();
        cancellation.cancelled().await;
        Err(microcrab_transport::Error::Cancelled)
    }
}
#[tokio::test]
async fn dyn_default_stream_passes_auditor_live_variables_and_cancellation_to_complete() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let backend: Arc<dyn Client> = Arc::new(Probe {
        entered: entered.clone(),
    });
    let (audit_sender, mut audits) = tokio::sync::mpsc::unbounded_channel();
    let auditor = microcrab_auditor::ChannelAuditor::from_sender(audit_sender);
    let mut context =
        microcrab_session::context::Context::new("unchanged system".into(), vec![], 65536).unwrap();
    context.admit_user("hello", &serde_json::json!({})).unwrap();
    let content = context.content(1024).unwrap();
    let token = CancellationToken::new();
    let cancelling = token.clone();
    let signal = tokio::spawn(async move {
        entered.notified().await;
        cancelling.cancel();
    });
    let (events, mut received) = tokio::sync::mpsc::channel(1);
    let result = backend
        .stream(&content, &auditor, &View, token, Some(events))
        .await;
    signal.await.unwrap();
    assert!(matches!(result, Err(microcrab_transport::Error::Cancelled)));
    assert!(audits.try_recv().is_ok());
    assert!(received.try_recv().is_err());
}
