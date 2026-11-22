//! Command admission and close meet at token registration, before model effects.
use super::*;
use crate::context::Content;
use crate::runtime::{Runtime, journal::Recover, tool::DenyAll};
use crate::{NoopAuditor, VariableView};
use microcrab_protocol::Response;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use tokio::sync::Notify;
mod interrupt;

/// Only test builds can pause a command after reservation and before registration.
pub(crate) struct RegistrationGate {
    entered: Notify,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}
impl RegistrationGate {
    pub(super) fn wait(&self) {
        self.entered.notify_one();
        self.release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(5))
            .expect("test releases command registration");
    }
}

#[derive(Default)]
struct PendingModel {
    calls: AtomicUsize,
    dropped: AtomicUsize,
    entered: Notify,
}
struct ModelCall<'a>(&'a AtomicUsize);
impl Drop for ModelCall<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait::async_trait]
impl Client for PendingModel {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let _call = ModelCall(&self.dropped);
        self.entered.notify_one();
        std::future::pending().await
    }
}
#[derive(Default)]
struct JournalLog(Mutex<Vec<Recover>>);
#[async_trait::async_trait]
impl crate::runtime::journal::Journal for JournalLog {
    async fn commit(&self, recover: Recover) -> Result<(), Error> {
        self.0.lock().unwrap().push(recover);
        Ok(())
    }
}
fn session(
    client: Arc<PendingModel>,
    journal: Arc<dyn crate::runtime::journal::Journal>,
) -> Arc<SessionInner> {
    SessionInner::new(SessionParts {
        dialogue_reply_store: None,
        kind: crate::SessionKind::Interactive,
        communication: None,
        client,
        route: None,
        auto_compaction: None,
        round_limit_callback: None,
        attachment_resolver: None,
        runtime: Runtime::new(vec![], Arc::new(DenyAll)).unwrap(),
        prompt_config: Default::default(),
        materialized_system: None,
        limits: Limits::default(),
        agent: None,
        auditor: Arc::new(NoopAuditor),
        journal,
        host: None,
        references: vec![],
        session_id: "command-close".into(),
        processes_parent: None,
        catalog: vec![],
        is_main: true,
        permissions: None,
        review_client: None,
        permission_parent: None,
    })
    .unwrap()
}
#[derive(Clone, Copy, Debug)]
enum Command {
    Start,
    Refresh,
    Compact,
    ModelCompact,
}
impl Command {
    async fn run(self, inner: Arc<SessionInner>) -> Result<(), Error> {
        match self {
            Self::Start => inner.start().await,
            Self::Refresh => inner.refresh_references().await,
            Self::Compact => {
                inner
                    .compact("must not install".into(), serde_json::json!({}))
                    .await
            }
            Self::ModelCompact => inner
                .compact_with_model(
                    "must not reach model".into(),
                    serde_json::json!({}),
                    CancellationToken::new(),
                )
                .await
                .map(|_| ()),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_before_command_registration_rejects_without_model_or_compaction_publication() {
    for command in [
        Command::Start,
        Command::Refresh,
        Command::Compact,
        Command::ModelCompact,
    ] {
        let model = Arc::new(PendingModel::default());
        let journal = Arc::new(JournalLog::default());
        let inner = session(model.clone(), journal.clone());
        inner.start().await.unwrap();
        let before = serde_json::to_value(inner.context_snapshot().unwrap()).unwrap();
        let (release, receiver) = std::sync::mpsc::channel();
        let gate = Arc::new(RegistrationGate {
            entered: Notify::new(),
            release: Mutex::new(receiver),
        });
        *inner.command_registration_gate.lock().unwrap() = Some(gate.clone());
        let running = inner.clone();
        let operation = tokio::spawn(async move { command.run(running).await });
        tokio::time::timeout(Duration::from_secs(3), gate.entered.notified())
            .await
            .unwrap();
        let mut activity = inner.subscribe_activity();
        activity.borrow_and_update();
        let stopping = inner.clone();
        let closed = tokio::spawn(async move { stopping.close().await });
        // Close publishes activity after sealing and interrupting. The held command
        // has not registered a token, so this observes close winning that boundary.
        tokio::time::timeout(Duration::from_secs(3), activity.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            inner.submit("closed probe".into(), None).await,
            Err(Error::Closed)
        ));
        release.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(3), operation)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(result, Err(Error::Closed | Error::Cancelled)),
            "{command:?}: {result:?}"
        );
        tokio::time::timeout(Duration::from_secs(3), closed)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(model.calls.load(Ordering::SeqCst), 0, "{command:?}");
        assert_eq!(
            serde_json::to_value(inner.context_snapshot().unwrap()).unwrap(),
            before,
            "{command:?}"
        );
        assert!(
            journal
                .0
                .lock()
                .unwrap()
                .iter()
                .all(|recover| { serde_json::to_value(&recover.context).unwrap() == before }),
            "{command:?}: no compaction checkpoint may be published"
        );
    }
}

#[tokio::test]
async fn close_after_model_compaction_registration_cancels_the_owned_call() {
    let model = Arc::new(PendingModel::default());
    let journal = Arc::new(JournalLog::default());
    let inner = session(model.clone(), journal.clone());
    inner.start().await.unwrap();
    let before = serde_json::to_value(inner.context_snapshot().unwrap()).unwrap();
    let running = inner.clone();
    let operation = tokio::spawn(async move { Command::ModelCompact.run(running).await });
    tokio::time::timeout(Duration::from_secs(3), model.entered.notified())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), inner.close())
        .await
        .unwrap()
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(3), operation)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(Error::Cancelled)));
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(model.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        serde_json::to_value(inner.context_snapshot().unwrap()).unwrap(),
        before
    );
    assert!(
        journal
            .0
            .lock()
            .unwrap()
            .iter()
            .all(|recover| { serde_json::to_value(&recover.context).unwrap() == before })
    );
}

#[derive(Default)]
struct FailedJournalClose {
    commits: AtomicUsize,
    closes: AtomicUsize,
}
#[async_trait::async_trait]
impl crate::runtime::journal::Journal for FailedJournalClose {
    async fn commit(&self, _: Recover) -> Result<(), Error> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn close(&self) -> Result<(), Error> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        Err(Error::Lifecycle("injected journal close failure".into()))
    }
}
#[tokio::test]
async fn explicit_close_reconciliation_never_replays_or_erases_terminal_failure() {
    let journal = Arc::new(FailedJournalClose::default());
    let inner = session(Arc::new(PendingModel::default()), journal.clone());
    assert!(inner.close().await.is_err());
    let commits = journal.commits.load(Ordering::SeqCst);
    for _ in 0..2 {
        assert!(inner.reconcile_close().await.is_err());
    }
    assert_eq!(journal.closes.load(Ordering::SeqCst), 1);
    assert_eq!(journal.commits.load(Ordering::SeqCst), commits);
    assert!(inner.closed.load(Ordering::SeqCst));
}

#[derive(Default)]
struct FailedFinalSnapshot {
    commits: AtomicUsize,
    closes: AtomicUsize,
    panic: bool,
}
#[async_trait::async_trait]
impl crate::runtime::journal::Journal for FailedFinalSnapshot {
    async fn commit(&self, _: Recover) -> Result<(), Error> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        assert!(!self.panic, "injected final snapshot panic");
        Err(Error::Protocol("injected final snapshot failure".into()))
    }
    async fn close(&self) -> Result<(), Error> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
#[tokio::test]
async fn destructive_settlement_discards_only_failed_final_snapshot() {
    let journal = Arc::new(FailedFinalSnapshot::default());
    let inner = session(Arc::new(PendingModel::default()), journal.clone());
    assert!(inner.close().await.is_err());
    assert!(inner.settle_for_delete(true).await.is_ok());
    assert!(
        inner.close().await.is_err(),
        "ordinary close preserves its failure"
    );
    assert!(inner.reconcile_close().await.is_err());
    assert_eq!(journal.commits.load(Ordering::SeqCst), 1);
    assert_eq!(journal.closes.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn destructive_settlement_never_tolerates_close_panic_or_journal_release_failure() {
    // Delete never writes a terminal snapshot, so a panicking final-snapshot
    // writer is not reached; the Journal is still released exactly once.
    let panic = Arc::new(FailedFinalSnapshot {
        panic: true,
        ..Default::default()
    });
    let inner = session(Arc::new(PendingModel::default()), panic.clone());
    assert!(inner.settle_for_delete(false).await.is_ok());
    assert!(inner.settle_for_delete(true).await.is_ok());
    assert_eq!(panic.commits.load(Ordering::SeqCst), 0);
    assert_eq!(panic.closes.load(Ordering::SeqCst), 1);
    let journal = Arc::new(FailedJournalClose::default());
    let inner = session(Arc::new(PendingModel::default()), journal.clone());
    assert!(inner.settle_for_delete(false).await.is_err());
    assert!(inner.settle_for_delete(true).await.is_err());
    assert_eq!(journal.closes.load(Ordering::SeqCst), 1);
}

#[derive(Default)]
struct CoherentInputModel(Mutex<Vec<Content>>);
#[async_trait::async_trait]
impl Client for CoherentInputModel {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.0.lock().unwrap().push(content.clone());
        Ok(Response {
            content: "done".into(),
            tool_calls: vec![],
            provider_metadata: None,
            usage: Default::default(),
        })
    }
}
struct ModePublicationGap {
    controller: Arc<crate::policy::PermissionController>,
    armed: Arc<AtomicBool>,
}
impl crate::Reference for ModePublicationGap {
    type Error = std::io::Error;
    fn kind(&self) -> &str {
        "skills"
    }
    async fn load(&self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn list(&self) -> Result<Vec<crate::ReferenceInfo>, Self::Error> {
        Ok(vec![])
    }
    fn read(&self, _: &str) -> Result<Option<crate::ReferenceContent>, Self::Error> {
        Ok(None)
    }
    fn change(&self) -> Result<Option<Value>, Self::Error> {
        if self.armed.swap(false, Ordering::SeqCst) {
            // Pause a real mode publication after policy commit but before its
            // State name commit, exactly while candidate reference refresh runs.
            self.controller
                .configure_mode("input-fixture", "build", &[])
                .map_err(|error| std::io::Error::other(error.to_string()))?;
        }
        Ok(Some(serde_json::json!([])))
    }
}

#[tokio::test]
async fn stale_post_hook_input_candidate_neither_faults_nor_allocates_a_turn_and_can_be_retried() {
    use serde_json::json;
    let runtime = Runtime::new(vec![], Arc::new(DenyAll)).unwrap();
    let controller = runtime.permission_controller();
    let armed = Arc::new(AtomicBool::new(false));
    let model = Arc::new(CoherentInputModel::default());
    let agent = serde_json::from_value(json!({"name":"input-fixture","modes":[
        {"name":"plan","role":"both","prompt":"Inspect only","permissions":[]},
        {"name":"build","role":"both","prompt":"Implement changes","permissions":[]}
    ],"tools":[],"variables":{}}))
    .unwrap();
    let session = crate::Session::builder_arc(model.clone(), runtime)
        .agent(agent)
        .references(vec![
            crate::DynReference::new(ModePublicationGap {
                controller,
                armed: armed.clone(),
            })
            .unwrap(),
        ])
        .build()
        .unwrap();
    session.start().await.unwrap();
    let before = serde_json::to_value(session.context_snapshot().unwrap()).unwrap();
    armed.store(true, Ordering::SeqCst);
    let mut input = crate::SessionInput::text("staged input");
    input.variables.set.insert("input_only".into(), json!(7));
    let error = session.submit(input).await.unwrap_err();
    assert!(
        matches!(error, Error::StateObservationConflict),
        "{error:?}"
    );
    assert!(!session.inner.is_faulted());
    assert_eq!(session.state().last_turn_id(), 0);
    assert!(session.state_handle().extract("input_only").is_none());
    assert!(model.0.lock().unwrap().is_empty());
    assert_eq!(
        serde_json::to_value(session.context_snapshot().unwrap()).unwrap(),
        before
    );
    session.select_mode("build").await.unwrap();
    session.submit("retry complete mode").await.unwrap();
    assert_eq!(session.state().last_turn_id(), 1);
    assert!(!session.inner.is_faulted());
    let requests = model.0.lock().unwrap().clone();
    let mut admitted = json!({});
    for item in &requests[0].items {
        if let crate::context::ContentItem::State(frame) = item {
            microcrab_transport::apply_state_patch(&mut admitted, frame).unwrap();
        }
    }
    assert_eq!(admitted["mode"], "build");
    assert_eq!(admitted["mode_prompt"], "Implement changes");
    session.close().await.unwrap();
}

struct SettledToolModel {
    requests: Mutex<Vec<Content>>,
}
#[async_trait::async_trait]
impl Client for SettledToolModel {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let first = {
            let mut requests = self.requests.lock().unwrap();
            let first = requests.is_empty();
            requests.push(content.clone());
            first
        };
        Ok(Response {
            content: if first { String::new() } else { "done".into() },
            tool_calls: if first {
                vec![microcrab_protocol::ToolCall {
                    id: "settled-once".into(),
                    name: "once".into(),
                    arguments: serde_json::json!({}),
                    provider_metadata: None,
                }]
            } else {
                vec![]
            },
            provider_metadata: None,
            usage: microcrab_protocol::Usage {
                input_tokens: Some(11),
                output_tokens: Some(3),
                samples: 1,
                ..Default::default()
            },
        })
    }
}
struct OnceTool(Arc<AtomicUsize>, Arc<AtomicBool>);
#[async_trait::async_trait]
impl crate::runtime::tool::Tool for OnceTool {
    fn definition(&self) -> microcrab_protocol::ToolDefinition {
        microcrab_protocol::ToolDefinition {
            name: "once".into(),
            description: "Count committed effects".into(),
            parameters: serde_json::json!({"type":"object","additionalProperties":false}),
        }
    }
    async fn execute(
        &self,
        _: Value,
        _: crate::runtime::tool::ToolContext,
    ) -> Result<crate::runtime::execution::ToolOutput, Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        self.1.store(true, Ordering::SeqCst);
        Ok(crate::runtime::execution::ToolOutput::text(
            "committed once",
        ))
    }
}

#[tokio::test]
async fn admitted_state_conflict_persists_exact_retry_boundary_without_replaying_input_or_tools() {
    use serde_json::json;
    let effects = Arc::new(AtomicUsize::new(0));
    let armed = Arc::new(AtomicBool::new(false));
    let runtime = Runtime::new(
        vec![crate::runtime::ToolRegistration::Wasm(Arc::new(OnceTool(
            effects.clone(),
            armed.clone(),
        )))],
        Arc::new(crate::runtime::tool::AllowTools::new(["once"])),
    )
    .unwrap();
    let controller = runtime.permission_controller();
    let model = Arc::new(SettledToolModel {
        requests: Mutex::new(vec![]),
    });
    let journal = crate::runtime::journal::MemoryJournal::new();
    let agent = serde_json::from_value(json!({"name":"input-fixture","modes":[
        {"name":"plan","role":"both","prompt":"Inspect only","permissions":[]},
        {"name":"build","role":"both","prompt":"Implement changes","permissions":[]}
    ],"tools":[],"variables":{}}))
    .unwrap();
    let session = crate::Session::builder_arc(model.clone(), runtime)
        .agent(agent)
        .references(vec![
            crate::DynReference::new(ModePublicationGap { controller, armed }).unwrap(),
        ])
        .journal(journal.clone())
        .build()
        .unwrap();
    let error = session
        .submit("one admitted instruction")
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::StateObservationConflict),
        "{error:?}"
    );
    assert!(!session.inner.is_faulted());
    assert!(session.retry_ready());
    assert!(session.inbox_snapshot().paused);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(model.requests.lock().unwrap().len(), 1);
    let recover = journal.latest().unwrap();
    recover.validate().unwrap();
    let continuation = recover.continuation.as_ref().unwrap();
    assert!(!continuation.request_prepared);
    assert!(continuation.state_observation_pending);
    assert_eq!(continuation.next_round, 2);
    assert_eq!(continuation.unattended_rounds, 1);
    assert_eq!(continuation.usage.input_tokens, Some(11));
    assert_eq!(continuation.usage.output_tokens, Some(3));
    assert_eq!(continuation.usage.samples, 1);
    let admitted_prefix = serde_json::to_value(&recover.context.items).unwrap();
    session.select_mode("build").await.unwrap();
    session.resume().await.unwrap();
    assert!(!session.retry_ready());
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    {
        let requests = model.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].turn_id, requests[0].turn_id);
        assert_eq!(requests[1].round, Some(2));
        assert_eq!(
            serde_json::to_value(&requests[1].items[..recover.context.items.len()]).unwrap(),
            admitted_prefix
        );
        assert_eq!(
            requests[1]
                .items
                .iter()
                .filter(|item| matches!(item, crate::context::ContentItem::User { .. }))
                .count(),
            1
        );
        assert_eq!(requests[1].items.iter().filter(|item| matches!(item,crate::context::ContentItem::Tool{call_id,..} if call_id == "settled-once")).count(),1);
    }
    session.close().await.unwrap();
}
