//! Persistence and admitted work remain Session-owned after their callers drop.
use microcrab_protocol::Response;
use microcrab_session::context::Content;
use microcrab_session::runtime::Runtime;
use microcrab_session::runtime::execution::ToolOutput;
use microcrab_session::runtime::journal::{Journal, MemoryJournal, Recover};
use microcrab_session::runtime::tool::DenyAll;
use microcrab_session::{Auditor, Client, Error, Session};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

struct UnusedClient;
#[async_trait::async_trait]
impl Client for UnusedClient {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        panic!("persistence transaction test must not request a model");
    }
}

#[derive(Default)]
struct SuspendedCommit {
    journal: MemoryJournal,
    suspend: AtomicBool,
    entered: Notify,
    release: Notify,
    committed: Notify,
}
#[async_trait::async_trait]
impl Journal for SuspendedCommit {
    async fn commit(&self, recover: Recover) -> Result<(), Error> {
        if self.suspend.load(Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.journal.commit(recover).await?;
        self.committed.notify_one();
        Ok(())
    }
}

#[tokio::test]
async fn cancelling_checkpoint_waiter_keeps_commit_owned_and_blocks_new_admission() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let journal = Arc::new(SuspendedCommit::default());
        let runtime = Runtime::new(vec![], Arc::new(DenyAll)).unwrap();
        let session = Session::builder(UnusedClient, runtime)
            .journal_arc(journal.clone())
            .build()
            .unwrap();
        session.start().await.unwrap();
        // Consume the initialization commit notification before suspending the next one.
        journal.committed.notified().await;
        // An unchanged boundary is reused; mutate State so this case exercises
        // an actual durable publication whose caller can be cancelled.
        session
            .state_handle()
            .inject("checkpoint_change", serde_json::json!(true))
            .await
            .unwrap();
        journal.suspend.store(true, Ordering::SeqCst);
        let waiting = session.clone();
        let checkpoint = tokio::spawn(async move { waiting.checkpoint().await });
        journal.entered.notified().await;
        checkpoint.abort();
        assert!(checkpoint.await.unwrap_err().is_cancelled());
        assert!(matches!(session.start().await, Err(Error::Busy)));
        journal.release.notify_one();
        journal.committed.notified().await;
        loop {
            match session.start().await {
                Err(Error::Busy) => tokio::task::yield_now().await,
                result => {
                    result.unwrap();
                    break;
                }
            }
        }
        assert!(journal.journal.latest().is_some());
        session
            .state_handle()
            .inject("after_completed_commit", serde_json::json!(true))
            .await
            .unwrap();
        journal.suspend.store(false, Ordering::SeqCst);
        session.close().await.unwrap();
        assert_eq!(
            journal.journal.latest().unwrap().state.variables.as_value()["after_completed_commit"],
            true
        );
    })
    .await
    .expect("unknown-commit cancellation must settle within deadline");
}

struct BlockingClient {
    entered: Notify,
    release: tokio::sync::Semaphore,
}
#[async_trait::async_trait]
impl Client for BlockingClient {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        token: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.entered.notify_one();
        tokio::select! {
            biased;
            _ = token.cancelled() => Err(microcrab_transport::Error::Cancelled),
            permit = self.release.acquire() => {
                permit.expect("release semaphore remains open").forget();
                Ok(text_response("independent turn completed"))
            }
        }
    }
}
fn text_response(text: &str) -> Response {
    Response {
        provider_metadata: None,
        content: text.into(),
        tool_calls: vec![],
        usage: Default::default(),
    }
}

#[tokio::test]
async fn stream_reserves_admission_and_its_drop_does_not_cancel_the_next_turn() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let client = Arc::new(BlockingClient {
            entered: Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        });
        let session = Session::builder_arc(
            client.clone(),
            Runtime::new(vec![], Arc::new(DenyAll)).unwrap(),
        )
        .build()
        .unwrap();
        let stream = session.stream("streamed turn").await.unwrap();
        // Reject before the spawned stream worker is first polled, not merely
        // after its model future has started.
        assert!(matches!(
            session.submit("must not steal admission").await,
            Err(Error::Busy)
        ));
        client.entered.notified().await;
        drop(stream);
        loop {
            match session.start().await {
                Err(Error::Busy) => tokio::task::yield_now().await,
                result => {
                    result.unwrap();
                    break;
                }
            }
        }
        let next = session.clone();
        let next_turn = tokio::spawn(async move { next.submit("independent new turn").await });
        client.entered.notified().await;
        client.release.add_permits(1);
        assert_eq!(
            next_turn.await.unwrap().unwrap().content,
            "independent turn completed"
        );
        session.close().await.unwrap();
    })
    .await
    .expect("stream cleanup and independent admission must settle");
}

#[tokio::test]
async fn finished_stream_releases_admission_before_the_next_turn() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let client = Arc::new(BlockingClient {
            entered: Notify::new(),
            release: tokio::sync::Semaphore::new(1),
        });
        let session = Session::builder_arc(
            client.clone(),
            Runtime::new(vec![], Arc::new(DenyAll)).unwrap(),
        )
        .build()
        .unwrap();
        let mut first = session.stream("first user input").await.unwrap();
        while let Some(event) = first.recv().await {
            if matches!(
                event.unwrap(),
                microcrab_session::SessionEvent::Finished { .. }
            ) {
                break;
            }
        }
        assert_eq!(session.state().turn_id, None);
        // Keep the completed receiver alive while reserving the next operation.
        // Its later Drop may cancel only the first stream's token.
        let mut second = session.stream("second user input").await.unwrap();
        drop(first);
        client.release.add_permits(1);
        let mut finished = false;
        while let Some(event) = second.recv().await {
            if let microcrab_session::SessionEvent::Finished { outcome } = event.unwrap() {
                assert_eq!(outcome.content, "independent turn completed");
                finished = true;
                break;
            }
        }
        assert!(finished);
        assert_eq!(
            serde_json::to_value(session.state()).unwrap()["last_turn_id"],
            2
        );
        session.close().await.unwrap();
    })
    .await
    .expect("terminal delivery must release admission and isolate stream cancellation");
}

struct ScriptedClient {
    responses:
        std::sync::Mutex<std::collections::VecDeque<Result<Response, microcrab_transport::Error>>>,
}
impl ScriptedClient {
    fn new(responses: Vec<Result<Response, microcrab_transport::Error>>) -> Self {
        Self {
            responses: std::sync::Mutex::new(responses.into()),
        }
    }
}
#[async_trait::async_trait]
impl Client for ScriptedClient {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("script contains each expected model round")
    }
}
struct CountTool(Arc<std::sync::atomic::AtomicUsize>);
#[async_trait::async_trait]
impl microcrab_session::runtime::tool::Tool for CountTool {
    fn definition(&self) -> microcrab_protocol::ToolDefinition {
        microcrab_protocol::ToolDefinition {
            name: "count_effect".into(),
            description: "Count a completed effect".into(),
            parameters: serde_json::json!({"type":"object"}),
        }
    }
    async fn execute(
        &self,
        _: serde_json::Value,
        _: microcrab_session::runtime::tool::ToolContext,
    ) -> Result<ToolOutput, Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutput::text("counted"))
    }
}
fn counter_runtime(counter: Arc<std::sync::atomic::AtomicUsize>) -> Runtime {
    Runtime::new(
        vec![microcrab_session::runtime::ToolRegistration::External(
            Arc::new(CountTool(counter)),
        )],
        Arc::new(microcrab_session::runtime::tool::AllowTools::new([
            "count_effect",
        ])),
    )
    .unwrap()
}
fn request_counter(id: &str) -> Response {
    Response {
        provider_metadata: None,
        content: "working".into(),
        tool_calls: vec![microcrab_protocol::ToolCall {
            provider_metadata: None,
            id: id.into(),
            name: "count_effect".into(),
            arguments: serde_json::json!({}),
        }],
        usage: Default::default(),
    }
}

#[tokio::test]
async fn rejected_submit_preserves_restored_turn_identity_for_another_settled_tool_round() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let journal = Arc::new(MemoryJournal::default());
        let effects = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let original = Session::builder(
            ScriptedClient::new(vec![
                Ok(request_counter("before-recovery")),
                Err(microcrab_transport::Error::Protocol(
                    "stop after first settled tool round".into(),
                )),
            ]),
            counter_runtime(effects.clone()),
        )
        .journal_arc(journal.clone())
        .build()
        .unwrap();
        assert!(matches!(
            original.submit("do the work").await,
            Err(Error::Protocol(_))
        ));
        assert_eq!(effects.load(Ordering::SeqCst), 1);
        let saved = journal.latest().unwrap();
        let saved_turn = saved.state.turn_id;
        assert!(saved_turn.is_some());
        assert_eq!(saved.continuation.as_ref().unwrap().next_round, 2);
        original.close().await.unwrap();
        let restored = Session::restore(
            Session::builder(
                ScriptedClient::new(vec![
                    Ok(request_counter("after-recovery")),
                    Ok(text_response("finished resumed work")),
                ]),
                counter_runtime(effects.clone()),
            )
            .journal_arc(journal),
            saved,
        )
        .await
        .unwrap();
        assert!(matches!(restored.submit("").await, Err(Error::Config(_))));
        assert_eq!(restored.state().turn_id, saved_turn);
        assert!(restored.has_continuation());
        let outcome = restored.resume().await.unwrap();
        assert_eq!(outcome.content, "finished resumed work");
        assert_eq!(outcome.rounds, 3);
        assert_eq!(effects.load(Ordering::SeqCst), 2);
        assert!(!restored.has_continuation());
        assert_eq!(restored.state().turn_id, None);
        restored.close().await.unwrap();
    })
    .await
    .expect("recovered continuation must survive rejected replacement input");
}
