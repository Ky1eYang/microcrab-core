//! Navigation provenance is certified by Session transitions, not inferred from counters.
use async_trait::async_trait;
use microcrab_protocol::Response;
use microcrab_session::context::{Content, ContentItem};
use microcrab_session::runtime::{Runtime, tool::DenyAll};
use microcrab_session::{
    Auditor, Client, Error, Journal, NavigationBoundary, Recover, Session, SessionInput,
};
use serde_json::json;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Boundaries {
    records: Mutex<Vec<(Option<NavigationBoundary>, Recover)>>,
    reject_initial: AtomicBool,
}
#[async_trait]
impl Journal for Boundaries {
    async fn commit(&self, recover: Recover) -> Result<(), Error> {
        recover.validate()?;
        self.records.lock().unwrap().push((None, recover));
        Ok(())
    }
    async fn commit_boundary(
        &self,
        recover: Recover,
        boundary: NavigationBoundary,
    ) -> Result<(), Error> {
        recover.validate()?;
        if boundary == NavigationBoundary::EpochInitial
            && self.reject_initial.load(Ordering::SeqCst)
        {
            return Err(Error::config("injected epoch initial publication failure"));
        }
        self.records.lock().unwrap().push((Some(boundary), recover));
        Ok(())
    }
    async fn has_selected_history(&self) -> bool {
        !self.records.lock().unwrap().is_empty()
    }
}
#[derive(Default)]
struct Model {
    requests: AtomicUsize,
    fail: AtomicBool,
}
#[async_trait]
impl Client for Model {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            return Err(microcrab_transport::Error::Transport);
        }
        Ok(Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: vec![],
            usage: Default::default(),
        })
    }
}
fn runtime() -> Runtime {
    Runtime::new(vec![], Arc::new(DenyAll)).unwrap()
}
fn session(model: Arc<Model>, journal: Arc<Boundaries>) -> Session {
    Session::builder_arc(model, runtime())
        .journal_arc(journal)
        .build()
        .unwrap()
}

#[tokio::test]
async fn initial_and_inter_turn_callbacks_preserve_internal_commits_and_reuse_manual_boundary() {
    let journal = Arc::new(Boundaries::default());
    let model = Arc::new(Model::default());
    let session = session(model.clone(), journal.clone());
    session.submit("first").await.unwrap();
    {
        let records = journal.records.lock().unwrap();
        let initial = records
            .iter()
            .find(|(kind, _)| *kind == Some(NavigationBoundary::EpochInitial))
            .unwrap();
        assert!(initial.1.context.items.is_empty());
        assert!(initial.1.continuation.is_none());
        assert!(records.iter().any(|(kind, _)| kind.is_none()));
        assert_eq!(
            records.last().unwrap().0,
            Some(NavigationBoundary::InterTurn)
        );
    }
    let count = journal.records.lock().unwrap().len();
    let first = session.checkpoint_navigation().await.unwrap();
    let second = session.checkpoint_navigation().await.unwrap();
    assert_eq!(
        serde_json::to_value(first).unwrap(),
        serde_json::to_value(second).unwrap()
    );
    assert_eq!(journal.records.lock().unwrap().len(), count);
    assert_eq!(model.requests.load(Ordering::SeqCst), 1);
    session.close().await.unwrap();
}

#[tokio::test]
async fn compacted_epoch_initial_precedes_input_variables_and_retains_summary() {
    let journal = Arc::new(Boundaries::default());
    let session = session(Arc::new(Model::default()), journal.clone());
    session.submit("inherited").await.unwrap();
    session
        .compact("certified summary", json!({"baseline":true}))
        .await
        .unwrap();
    let mut input = SessionInput::text("first new epoch input");
    input.variables.set.insert("input_only".into(), json!(7));
    session.submit(input).await.unwrap();
    {
        let records = journal.records.lock().unwrap();
        let initials = records
            .iter()
            .filter(|(kind, _)| *kind == Some(NavigationBoundary::EpochInitial))
            .collect::<Vec<_>>();
        assert_eq!(initials.len(), 2);
        let initial = &initials[1].1;
        assert_eq!(initial.context.epoch, 1);
        assert!(!initial.context.pending_epoch);
        assert!(
            initial
                .state
                .variables
                .as_value()
                .get("input_only")
                .is_none()
        );
        assert_eq!(initial.state.variables.as_value()["baseline"], true);
        assert!(initial.context.items.iter().any(|item| matches!(item, ContentItem::User { parts, .. } if parts == &SessionInput::text("certified summary").parts)));
        assert!(!initial.context.items.iter().any(|item| matches!(item, ContentItem::User { parts, .. } if parts == &SessionInput::text("first new epoch input").parts)));
    }
    session.close().await.unwrap();
}

#[tokio::test]
async fn failed_epoch_initial_publication_prevents_provider_effect_and_input_variables() {
    let journal = Arc::new(Boundaries::default());
    let model = Arc::new(Model::default());
    let session = session(model.clone(), journal.clone());
    session.start().await.unwrap();
    session.compact("summary", json!({})).await.unwrap();
    journal.reject_initial.store(true, Ordering::SeqCst);
    let mut input = SessionInput::text("must not transmit");
    input.variables.set.insert("input_only".into(), json!(7));
    assert!(session.submit(input).await.is_err());
    assert_eq!(model.requests.load(Ordering::SeqCst), 0);
    assert!(session.state_handle().extract("input_only").is_none());
    assert_eq!(
        journal
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|(kind, _)| *kind == Some(NavigationBoundary::EpochInitial))
            .count(),
        1
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn prepared_continuation_is_internal_and_cannot_become_manual_checkpoint() {
    let journal = Arc::new(Boundaries::default());
    let model = Arc::new(Model::default());
    model.fail.store(true, Ordering::SeqCst);
    let session = session(model, journal.clone());
    assert!(session.submit("retained request").await.is_err());
    assert!(session.retry_ready());
    let before = journal.records.lock().unwrap().len();
    let error = session.checkpoint_navigation().await.unwrap_err();
    assert!(error.to_string().contains("settled Turn"));
    assert_eq!(journal.records.lock().unwrap().len(), before);
    {
        let records = journal.records.lock().unwrap();
        assert!(records.last().unwrap().1.continuation.is_some());
        assert!(
            !records
                .iter()
                .any(|(kind, _)| *kind == Some(NavigationBoundary::InterTurn))
        );
    }
    session.close().await.unwrap();
}

#[tokio::test]
async fn witnessed_attachment_does_not_invent_another_initial_boundary() {
    let journal = Arc::new(Boundaries::default());
    let original = session(Arc::new(Model::default()), journal.clone());
    original.submit("inherited").await.unwrap();
    let saved = original.checkpoint_navigation().await.unwrap();
    original.close().await.unwrap();
    let restored = Session::restore(
        Session::builder_arc(Arc::new(Model::default()), runtime()).journal_arc(journal.clone()),
        saved,
    )
    .await
    .unwrap();
    restored.submit("same epoch next input").await.unwrap();
    assert_eq!(
        journal
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|(kind, _)| *kind == Some(NavigationBoundary::EpochInitial))
            .count(),
        1
    );
    restored.close().await.unwrap();
}

#[tokio::test]
async fn explicit_pre_first_input_checkpoint_preserves_state_as_initial_and_reuses_identity() {
    let journal = Arc::new(Boundaries::default());
    let model = Arc::new(Model::default());
    let session = session(model.clone(), journal.clone());
    session.start().await.unwrap();
    session
        .state_handle()
        .inject("manual_initial", json!(7))
        .await
        .unwrap();
    let before = session.checkpoint_navigation().await.unwrap();
    assert_eq!(before.state.variables.as_value()["manual_initial"], 7);
    assert!(before.context.items.is_empty());
    let count = journal.records.lock().unwrap().len();
    assert_eq!(
        journal.records.lock().unwrap().last().unwrap().0,
        Some(NavigationBoundary::EpochInitial)
    );
    let again = session.checkpoint_navigation().await.unwrap();
    assert_eq!(
        serde_json::to_value(before).unwrap(),
        serde_json::to_value(again).unwrap()
    );
    assert_eq!(journal.records.lock().unwrap().len(), count);
    assert_eq!(model.requests.load(Ordering::SeqCst), 0);
    session.close().await.unwrap();
}
