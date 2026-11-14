//! Recovery waits for live producer cleanup acknowledgement before Journal selection.
use async_trait::async_trait;
use microcrab_protocol::{Response, ToolCall, ToolDefinition};
use microcrab_session::{
    Auditor, Client, Error, Session, VariableView,
    context::Content,
    runtime::{
        Runtime, ToolRegistration,
        execution::{
            ExecutionCapabilities, ExecutionController, OutputWriter, ToolFailure, ToolFailureKind,
            ToolOutput,
        },
        journal::{Journal, MemoryJournal, Recover},
        tool::{AllowTools, Tool, ToolContext},
    },
};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio_util::sync::CancellationToken;

struct Once(AtomicUsize);
#[async_trait]
impl Client for Once {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let first = self.0.fetch_add(1, Ordering::SeqCst) == 0;
        Ok(Response {
            provider_metadata: None,
            content: "foreground settled".into(),
            tool_calls: if first {
                vec![ToolCall {
                    provider_metadata: None,
                    id: "recover-producer".into(),
                    name: "observe_catalog".into(),
                    arguments: json!({}),
                }]
            } else {
                vec![]
            },
            usage: Default::default(),
        })
    }
}
// A real native input-driven producer owns its writer until explicit cleanup.
// Keeping the owner controllable lets the test distinguish Busy from partial restore.
struct Owner {
    writer: Mutex<Option<OutputWriter>>,
    cleaned: Arc<AtomicBool>,
    fail_cleanup: AtomicBool,
    cancel_entered: tokio::sync::Notify,
    release_ack: tokio::sync::Semaphore,
}
#[async_trait]
impl ExecutionController for Owner {
    fn capabilities(&self) -> ExecutionCapabilities {
        ExecutionCapabilities { input: true }
    }
    async fn write(&self, data: String, _: bool, _: CancellationToken) -> Result<(), Error> {
        let writer = self.writer.lock().unwrap();
        writer
            .as_ref()
            .ok_or_else(|| Error::Tool("producer already released".into()))?
            .append(None, &data)
    }
    async fn cancel(&self, _: CancellationToken) -> Result<(), Error> {
        if self.fail_cleanup.load(Ordering::SeqCst) {
            return Err(Error::tool("native cleanup not acknowledged"));
        }
        if self.writer.lock().unwrap().is_none() {
            return Ok(());
        }
        self.cancel_entered.notify_one();
        self.release_ack
            .acquire()
            .await
            .map_err(|_| Error::Closed)?
            .forget();
        let writer = self.writer.lock().unwrap().take();
        self.cleaned.store(true, Ordering::SeqCst);
        if let Some(writer) = writer {
            writer.finish(Err(ToolFailure {
                kind: ToolFailureKind::Cancelled,
                code: "producer_cancelled".into(),
                message: "Native producer cleanup acknowledged".into(),
                retryable: false,
            }))?;
        }
        Ok(())
    }
    async fn release(&self, token: CancellationToken) -> Result<(), Error> {
        self.cancel(token).await
    }
}
struct Producer(Arc<Owner>);
#[async_trait]
impl Tool for Producer {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "observe_catalog".into(),
            description: "Input-driven native producer".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn execute(&self, _: Value, context: ToolContext) -> Result<ToolOutput, Error> {
        let (writer, output) = context.open_output(Default::default(), self.0.clone())?;
        writer.append(None, "before recover;")?;
        *self.0.writer.lock().unwrap() = Some(writer);
        Ok(ToolOutput::Buffered(output.with_initial_output(true)))
    }
}
#[derive(Clone)]
struct RecordingJournal {
    memory: MemoryJournal,
    cleaned: Arc<AtomicBool>,
    published_cleanup: Arc<Mutex<Vec<bool>>>,
}
#[async_trait]
impl Journal for RecordingJournal {
    async fn commit(&self, recover: Recover) -> Result<(), Error> {
        self.memory.commit(recover).await
    }
    async fn recovered(&self, recover: Recover) -> Result<(), Error> {
        self.published_cleanup
            .lock()
            .unwrap()
            .push(self.cleaned.load(Ordering::SeqCst));
        self.memory.recovered(recover).await
    }
}
#[tokio::test]
async fn pending_buffered_recovery_retains_failed_cleanup_then_waits_for_actual_ack() {
    let cleaned = Arc::new(AtomicBool::new(false));
    let owner = Arc::new(Owner {
        writer: Mutex::new(None),
        cleaned: cleaned.clone(),
        fail_cleanup: AtomicBool::new(true),
        cancel_entered: tokio::sync::Notify::new(),
        release_ack: tokio::sync::Semaphore::new(0),
    });
    let journal = RecordingJournal {
        memory: MemoryJournal::new(),
        cleaned: cleaned.clone(),
        published_cleanup: Arc::new(Mutex::new(vec![])),
    };
    let runtime = Runtime::new(
        vec![ToolRegistration::External(Arc::new(Producer(
            owner.clone(),
        )))],
        Arc::new(AllowTools::new(["observe_catalog"])),
    )
    .unwrap();
    let session = Session::builder(Once(AtomicUsize::new(0)), runtime)
        .session_id("recover-live-producer")
        .journal(journal.clone())
        .build()
        .unwrap();
    // submit returns to foreground Idle while the Session still owns live output.
    session.submit("start real pending output").await.unwrap();
    let checkpoint = session.checkpoint().await.unwrap();
    assert_eq!(checkpoint.state.executions.len(), 1);
    assert!(checkpoint.state.executions[0].status.is_pending());
    let id = checkpoint.state.executions[0].execution_id;
    assert!(session.executions().has_pending());
    let before = session.messages();
    assert!(session.recover(checkpoint.clone()).await.is_err());
    assert!(
        journal.published_cleanup.lock().unwrap().is_empty(),
        "cleanup failure must precede Journal.recovered"
    );
    assert!(!cleaned.load(Ordering::SeqCst));
    assert!(
        owner.writer.lock().unwrap().is_some(),
        "failed cleanup keeps its actual producer owner"
    );
    assert_eq!(
        serde_json::to_value(session.messages()).unwrap(),
        serde_json::to_value(before).unwrap()
    );
    assert!(
        session.executions().get(id).is_ok(),
        "failed cleanup does not erase the invocation"
    );
    owner.fail_cleanup.store(false, Ordering::SeqCst);
    let recovering = session.clone();
    let replacement = tokio::spawn(async move { recovering.recover(checkpoint).await });
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        owner.cancel_entered.notified(),
    )
    .await
    .unwrap();
    assert!(
        !cleaned.load(Ordering::SeqCst),
        "cancellation request is not cleanup acknowledgement"
    );
    assert!(journal.published_cleanup.lock().unwrap().is_empty());
    owner.release_ack.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(3), replacement)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(cleaned.load(Ordering::SeqCst));
    assert!(owner.writer.lock().unwrap().is_none());
    assert_eq!(
        *journal.published_cleanup.lock().unwrap(),
        vec![true],
        "successful recovery publishes only after acknowledged cleanup"
    );
    assert!(
        !session.executions().has_pending(),
        "restoring a pending durable observation cannot revive its old producer"
    );
    session
        .submit("recovered Session remains operational")
        .await
        .unwrap();
    session.close().await.unwrap();
}
