use async_trait::async_trait;
use microcrab_session::context::Content;
use microcrab_session::protocol::{Response, ToolDefinition};
use microcrab_session::runtime::{
    AdmittedDialogueReply, DialogueReplyAdapter, DialogueReplyPosition, DialogueReplyReceipt,
    DialogueReplyRegistration, DialogueReplyRequest, Runtime, ToolRegistration,
    journal::MemoryJournal,
    tool::{DenyAll, Tool, ToolContext, ToolOutput},
};
use microcrab_session::{
    Auditor, Client, Error, ModelAdmission, PreparedModelAdmission, Session, SessionBuilder,
    SessionInput, VariableView,
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const REPLY: &str = "fixture_dialogue_append";
const RETIRED: &str = "fixture_retired_tool";

fn definition(name: &str) -> ToolDefinition {
    ToolDefinition {
        name: name.into(),
        description: "fixture".into(),
        parameters: json!({"type":"object"}),
    }
}

struct NoReply;
#[async_trait]
impl DialogueReplyAdapter for NoReply {
    async fn execute(
        &self,
        _: AdmittedDialogueReply,
        _: DialogueReplyRequest,
    ) -> Result<DialogueReplyReceipt, Error> {
        panic!("the fixture only observes the advertised catalog")
    }
    fn format_receipt(&self, _: &DialogueReplyPosition) -> String {
        unreachable!()
    }
}

struct External(&'static str);
#[async_trait]
impl Tool for External {
    fn definition(&self) -> ToolDefinition {
        definition(self.0)
    }
    async fn execute(&self, _: serde_json::Value, _: ToolContext) -> Result<ToolOutput, Error> {
        unreachable!()
    }
}

struct Owner {
    registration: DialogueReplyRegistration,
    enabled: AtomicBool,
    blocked: AtomicBool,
    prepares: AtomicUsize,
    markers: Arc<AtomicUsize>,
}
struct Marker(Arc<AtomicUsize>);
impl Drop for Marker {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl Owner {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            registration: DialogueReplyRegistration::new(
                definition(REPLY),
                vec![],
                Arc::new(NoReply),
            ),
            enabled: AtomicBool::new(false),
            blocked: AtomicBool::new(false),
            prepares: AtomicUsize::new(0),
            markers: Arc::new(AtomicUsize::new(0)),
        })
    }
}
#[async_trait]
impl ModelAdmission for Owner {
    fn dialogue_reply(&self) -> &DialogueReplyRegistration {
        &self.registration
    }
    fn retired_tool_names(&self) -> &[&str] {
        &[RETIRED]
    }
    async fn prepare(&self) -> Result<PreparedModelAdmission, Error> {
        self.prepares.fetch_add(1, Ordering::SeqCst);
        if self.blocked.load(Ordering::SeqCst) {
            return Err(Error::Busy);
        }
        self.markers.fetch_add(1, Ordering::SeqCst);
        Ok(PreparedModelAdmission::new(
            Box::new(Marker(self.markers.clone())),
            self.enabled
                .load(Ordering::SeqCst)
                .then(|| self.registration.clone()),
        ))
    }
}

type Request = (u64, Vec<ToolDefinition>, tokio::sync::oneshot::Sender<bool>);
struct Provider {
    calls: mpsc::UnboundedSender<Request>,
    markers: Arc<AtomicUsize>,
}
#[async_trait]
impl Client for Provider {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        assert_eq!(self.markers.load(Ordering::SeqCst), 1);
        let (respond, response) = tokio::sync::oneshot::channel();
        self.calls
            .send((content.epoch, content.tools.clone(), respond))
            .unwrap();
        if response.await.unwrap() {
            Ok(Response {
                content: "done".into(),
                tool_calls: vec![],
                provider_metadata: None,
                usage: Default::default(),
            })
        } else {
            Err(microcrab_transport::Error::ProviderTimeout)
        }
    }
}

fn fixture(
    owner: &Arc<Owner>,
    journal: MemoryJournal,
) -> (SessionBuilder, mpsc::UnboundedReceiver<Request>) {
    let (calls, requests) = mpsc::unbounded_channel();
    let session = Session::builder(
        Provider {
            calls,
            markers: owner.markers.clone(),
        },
        Runtime::new(
            vec![ToolRegistration::External(Arc::new(External(
                "browser_old",
            )))],
            Arc::new(DenyAll),
        )
        .unwrap(),
    )
    .journal(journal)
    .model_admission(owner.clone());
    (session, requests)
}

async fn request(requests: &mut mpsc::UnboundedReceiver<Request>) -> Request {
    tokio::time::timeout(Duration::from_secs(2), requests.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn explicit_publication_and_each_provider_turn_share_the_owned_witness() {
    let owner = Owner::new();
    let journal = MemoryJournal::new();
    let (builder, mut requests) = fixture(&owner, journal.clone());
    let session = Arc::new(builder.build().unwrap());
    let direct = tokio::spawn({
        let s = session.clone();
        async move { s.submit(SessionInput::text("off")).await }
    });
    let (off_epoch, tools, respond) = request(&mut requests).await;
    assert!(!tools.iter().any(|tool| tool.name == REPLY));
    assert!(matches!(
        session.reconcile_model_catalog().await,
        Err(Error::Busy)
    ));
    respond.send(true).unwrap();
    direct.await.unwrap().unwrap();
    assert_eq!(owner.markers.load(Ordering::SeqCst), 0);

    // The reserved effect and retired names cannot masquerade as browser tools.
    for name in [REPLY, RETIRED] {
        assert!(matches!(
            session
                .replace_external_tools(vec![Arc::new(External(name))])
                .await,
            Err(Error::IncompatibleModelCatalog)
        ));
    }
    session
        .replace_external_tools(vec![Arc::new(External("browser_new"))])
        .await
        .unwrap();
    owner.enabled.store(true, Ordering::SeqCst);
    session.reconcile_model_catalog().await.unwrap();
    let pending = journal.latest().unwrap().context;
    assert_eq!(pending.epoch, off_epoch);
    assert!(pending.pending_epoch);
    assert!(pending.tools.contains(&definition(REPLY)));
    assert!(pending.tools.contains(&definition("browser_old")));
    session.reconcile_model_catalog().await.unwrap();
    assert_eq!(
        serde_json::to_vec(&pending).unwrap(),
        serde_json::to_vec(&journal.latest().unwrap().context).unwrap()
    );

    let (mut stream, settlement) = session
        .stream_with_settlement(SessionInput::text("on"), CancellationToken::new())
        .await
        .unwrap();
    let (on_epoch, tools, respond) = request(&mut requests).await;
    assert_eq!(on_epoch, off_epoch + 1);
    assert!(tools.contains(&definition(REPLY)));
    assert!(tools.contains(&definition("browser_new")));
    assert!(!tools.contains(&definition("browser_old")));
    respond.send(true).unwrap();
    while !matches!(
        stream.recv().await.unwrap().unwrap(),
        microcrab_session::SessionEvent::Finished { .. }
    ) {}
    // The embedding can publish its own terminal receipt before releasing the marker.
    assert_eq!(owner.markers.load(Ordering::SeqCst), 1);
    drop(settlement);
    assert_eq!(owner.markers.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn autonomous_rejection_leaves_the_exact_entry_unclaimed_and_reconciliation_wakes_it() {
    let owner = Owner::new();
    owner.blocked.store(true, Ordering::SeqCst);
    let (builder, mut requests) = fixture(&owner, MemoryJournal::new());
    let session = builder.build().unwrap();
    let mut activity = session.subscribe_activity();
    session.queue(SessionInput::text("queued")).unwrap();
    tokio::time::timeout(Duration::from_secs(2), activity.wait_for(|row| row.paused))
        .await
        .unwrap()
        .unwrap();
    let snapshot = session.inbox_snapshot();
    assert_eq!(snapshot.items.len(), 1);
    assert!(!snapshot.items[0].claimed);
    assert!(requests.try_recv().is_err());
    assert_eq!(owner.prepares.load(Ordering::SeqCst), 1);

    owner.blocked.store(false, Ordering::SeqCst);
    owner.enabled.store(true, Ordering::SeqCst);
    session.reconcile_model_catalog().await.unwrap();
    let (_, tools, respond) = request(&mut requests).await;
    assert!(tools.contains(&definition(REPLY)));
    assert!(session.inbox_snapshot().items.is_empty());
    respond.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), activity.wait_for(|row| !row.active))
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn mismatched_continuation_cannot_retry_or_be_replaced_by_new_input() {
    let owner = Owner::new();
    let journal = MemoryJournal::new();
    let (builder, mut requests) = fixture(&owner, journal.clone());
    let session = Arc::new(builder.build().unwrap());
    let direct = tokio::spawn({
        let s = session.clone();
        async move { s.submit(SessionInput::text("retry me")).await }
    });
    let (_, _, respond) = request(&mut requests).await;
    respond.send(false).unwrap();
    assert!(matches!(direct.await.unwrap(), Err(Error::ProviderTimeout)));
    assert!(session.retry_ready());
    let checkpoint = journal.latest().unwrap();
    owner.enabled.store(true, Ordering::SeqCst);
    assert!(matches!(
        session.retry_stream().await,
        Err(Error::ModelCatalogContinuationMismatch)
    ));
    assert!(matches!(
        session.submit(SessionInput::text("do not replace")).await,
        Err(Error::ModelCatalogContinuationMismatch)
    ));
    assert!(matches!(
        session.reconcile_model_catalog().await,
        Err(Error::ModelCatalogContinuationMismatch)
    ));
    let (restoring, _) = fixture(&owner, journal.clone());
    assert!(matches!(
        Session::restore(restoring, checkpoint.clone()).await,
        Err(Error::ModelCatalogContinuationMismatch)
    ));
    assert!(requests.try_recv().is_err());
    assert_eq!(
        serde_json::to_vec(&checkpoint).unwrap(),
        serde_json::to_vec(&journal.latest().unwrap()).unwrap()
    );
    assert!(session.has_continuation());
}

#[tokio::test]
async fn restore_and_travel_accept_only_exact_managed_presence_delta() {
    let owner = Owner::new();
    let journal = MemoryJournal::new();
    let (builder, _) = fixture(&owner, journal.clone());
    let session = builder.build().unwrap();
    let original = session.checkpoint().await.unwrap();
    owner.enabled.store(true, Ordering::SeqCst);
    let (restoring, _) = fixture(&owner, journal.clone());
    let restored = Session::restore(restoring, original.clone()).await.unwrap();
    assert!(
        journal
            .latest()
            .unwrap()
            .context
            .tools
            .contains(&definition(REPLY))
    );
    assert!(journal.latest().unwrap().context.pending_epoch);
    assert!(!original.context.tools.contains(&definition(REPLY)));
    owner.enabled.store(false, Ordering::SeqCst);
    let selected = journal.latest().unwrap();
    restored.recover(selected.clone()).await.unwrap();
    assert!(
        !journal
            .latest()
            .unwrap()
            .context
            .tools
            .contains(&definition(REPLY))
    );
    assert!(selected.context.tools.contains(&definition(REPLY)));

    for name in [RETIRED, REPLY, "browser_old"] {
        let mut bad = original.clone();
        if name == "browser_old" {
            bad.context
                .tools
                .iter_mut()
                .find(|t| t.name == name)
                .unwrap()
                .description = "not exact".into();
        } else if name == REPLY {
            let mut wrong = definition(REPLY);
            wrong.description = "not exact".into();
            bad.context.tools.push(wrong);
        } else {
            bad.context.tools.push(definition(RETIRED));
        }
        bad.context.tools.sort_by(|a, b| a.name.cmp(&b.name));
        let before = serde_json::to_vec(&journal.latest().unwrap()).unwrap();
        let result = restored.recover(bad).await;
        if name == "browser_old" {
            assert!(matches!(result, Err(Error::Config(_))));
        } else {
            assert!(matches!(result, Err(Error::IncompatibleModelCatalog)));
        }
        assert_eq!(
            serde_json::to_vec(&journal.latest().unwrap()).unwrap(),
            before
        );
    }
}
