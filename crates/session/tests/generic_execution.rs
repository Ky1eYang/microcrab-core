//! Generic output acceptance uses actual Runtime buffers and owned nonprocess producers.
use async_trait::async_trait;
use microcrab_protocol::{Response, ToolCall, ToolDefinition};
use microcrab_session::{
    Auditor, Client, Error, Limits, Session, VariableView,
    context::{Content, ContentItem},
    runtime::{
        Runtime, ToolRegistration,
        execution::{
            ExecutionController, ExecutionId, ExecutionSnapshot, ExecutionStatus, OutputOptions,
            OutputPage, ToolFailure, ToolFailureKind, ToolOutput,
        },
        tool::{AllowTools, Tool, ToolContext},
    },
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

struct Model {
    calls: AtomicUsize,
    requests: Mutex<Vec<Content>>,
}
#[async_trait]
impl Client for Model {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.requests.lock().unwrap().push(content.clone());
        let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
        Ok(Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: if first {
                vec![ToolCall {
                    provider_metadata: None,
                    id: "producer-call".into(),
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
fn model() -> Arc<Model> {
    Arc::new(Model {
        calls: AtomicUsize::new(0),
        requests: Mutex::new(vec![]),
    })
}
fn definition() -> ToolDefinition {
    ToolDefinition {
        name: "observe_catalog".into(),
        description: "Nonprocess producer fixture".into(),
        parameters: json!({"type":"object","additionalProperties":false}),
    }
}
fn session(tool: Arc<dyn Tool>, limits: Limits) -> (Session, Arc<Model>) {
    let client = model();
    let runtime = Runtime::new(
        vec![ToolRegistration::External(tool)],
        Arc::new(AllowTools::new(["observe_catalog"])),
    )
    .unwrap();
    (
        Session::builder_arc(client.clone(), runtime)
            .session_id("generic-output")
            .limits(limits)
            .build()
            .unwrap(),
        client,
    )
}
fn text(page: &OutputPage) -> String {
    page.chunks.iter().map(|c| c.content.as_str()).collect()
}
fn result(session: &Session) -> String {
    session
        .messages()
        .iter()
        .find_map(|item| match item {
            ContentItem::Tool { call_id, content, .. } if call_id == "producer-call" => {
                Some(content.clone())
            }
            _ => None,
        })
        .unwrap()
}
async fn status(session: &Session, wanted: ExecutionStatus) -> ExecutionSnapshot {
    let mut changed = session.executions().subscribe();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(row) = session
                .execution_snapshots()
                .into_iter()
                .find(|r| r.status == wanted)
            {
                return row;
            }
            changed.changed().await.unwrap();
        }
    })
    .await
    .expect("producer must report its actual terminal transition")
}

#[derive(Debug)]
enum Command {
    Append(&'static str),
    Finish(Result<(), ToolFailure>),
}
struct Producer {
    initial: &'static str,
    commands: Mutex<Option<mpsc::UnboundedReceiver<Command>>>,
    cleaned: Arc<AtomicBool>,
}
struct Owner {
    token: CancellationToken,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.token.cancel();
        if let Some(worker) = self.worker.get_mut().unwrap().take() {
            worker.abort();
        }
    }
}
#[async_trait]
impl ExecutionController for Owner {
    async fn cancel(&self, _: CancellationToken) -> Result<(), Error> {
        self.token.cancel();
        let worker = self.worker.lock().unwrap().take();
        if let Some(worker) = worker {
            worker
                .await
                .map_err(|e| Error::Tool(e.to_string().into()))?;
        }
        Ok(())
    }
    async fn release(&self, token: CancellationToken) -> Result<(), Error> {
        self.cancel(token).await
    }
}
#[async_trait]
impl Tool for Producer {
    fn definition(&self) -> ToolDefinition {
        definition()
    }
    async fn execute(&self, _: Value, context: ToolContext) -> Result<ToolOutput, Error> {
        let mut commands = self.commands.lock().unwrap().take().unwrap();
        let owner = Arc::new(Owner {
            token: CancellationToken::new(),
            worker: Mutex::new(None),
        });
        let (writer, output) = context.open_output(
            OutputOptions {
                channels: vec!["records".into()],
            },
            owner.clone(),
        )?;
        writer.append(Some("records"), self.initial)?;
        let token = owner.token.clone();
        let cleaned = self.cleaned.clone();
        let worker = tokio::spawn(async move {
            let outcome = loop {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => break Err(ToolFailure {
                        kind: ToolFailureKind::Cancelled, code: "producer_cancelled".into(),
                        message: "Producer cleanup acknowledged".into(), retryable: false,
                    }),
                    command = commands.recv() => match command {
                        Some(Command::Append(content)) => writer.append(Some("records"), content).unwrap(),
                        Some(Command::Finish(outcome)) => break outcome,
                        None => break Err(ToolFailure::failed("producer_disconnected", "Producer channel disconnected")),
                    },
                }
            };
            drop(commands);
            cleaned.store(true, Ordering::SeqCst);
            writer.finish(outcome).unwrap();
        });
        *owner.worker.lock().unwrap() = Some(worker);
        Ok(ToolOutput::Buffered(output.with_initial_output(true)))
    }
}
fn producer(
    initial: &'static str,
) -> (
    Arc<Producer>,
    mpsc::UnboundedSender<Command>,
    Arc<AtomicBool>,
) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let cleaned = Arc::new(AtomicBool::new(false));
    (
        Arc::new(Producer {
            initial,
            commands: Mutex::new(Some(receiver)),
            cleaned: cleaned.clone(),
        }),
        sender,
        cleaned,
    )
}

struct PendingBackgroundCommit {
    journal: microcrab_session::runtime::journal::MemoryJournal,
    suspend: AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[async_trait]
impl microcrab_session::Journal for PendingBackgroundCommit {
    async fn commit(&self, recover: microcrab_session::Recover) -> Result<(), Error> {
        if self.suspend.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        microcrab_session::Journal::commit(&self.journal, recover).await
    }
}

#[tokio::test]
async fn cancelled_stream_admission_releases_foreground_before_background_commit_settles() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (producer, commands, _) = producer("initial");
        let client = model();
        let journal = Arc::new(PendingBackgroundCommit {
            journal: Default::default(),
            suspend: AtomicBool::new(false),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let runtime = Runtime::new(
            vec![ToolRegistration::External(producer)],
            Arc::new(AllowTools::new(["observe_catalog"])),
        )
        .unwrap();
        let session = Session::builder_arc(client.clone(), runtime)
            .journal_arc(journal.clone())
            .build()
            .unwrap();
        session.submit("start producer").await.unwrap();
        let original = session.messages();
        let requests = client.requests.lock().unwrap().len();
        journal.suspend.store(true, Ordering::SeqCst);
        commands.send(Command::Append("late output")).unwrap();
        journal.entered.notified().await;

        // Poll through acquisition of foreground and into the actual Journal
        // reservation wait before cancelling. The unrelated commit stays blocked.
        let cancellation = CancellationToken::new();
        let mut admission = Box::pin(
            session.stream_with_cancellation("must not be admitted", cancellation.clone()),
        );
        assert!(futures_util::poll!(&mut admission).is_pending());
        assert!(matches!(
            session.stream("competing input").await,
            Err(Error::Busy)
        ));
        cancellation.cancel();
        assert!(matches!(admission.await, Err(Error::Cancelled)));

        // Retry shares this admission path. Reaching Pending proves the cancelled
        // waiter released foreground before the Journal owner was allowed to finish.
        let cancellation = CancellationToken::new();
        let mut retry = Box::pin(session.retry_stream_with_cancellation(cancellation.clone()));
        assert!(futures_util::poll!(&mut retry).is_pending());
        cancellation.cancel();
        assert!(matches!(retry.await, Err(Error::Cancelled)));
        assert_eq!(session.messages(), original);
        assert_eq!(client.requests.lock().unwrap().len(), requests);

        journal.release.notify_one();
        session.submit("after cancelled admission").await.unwrap();
        session.close().await.unwrap();
    })
    .await
    .expect("cancelled admission must not wait for the unrelated Journal commit");
}

#[tokio::test]
async fn nonprocess_producer_has_one_execution_and_initial_and_subsequent_cursors() {
    let (producer, commands, _) = producer("initial-你好");
    let (session, _) = session(producer, Limits::default());
    session.submit("observe catalog updates").await.unwrap();
    let receipt: Value = serde_json::from_str(&result(&session)).unwrap();
    let id = ExecutionId(receipt["execution_id"].as_u64().unwrap());
    assert_eq!(receipt["tool"], "observe_catalog");
    assert!(receipt.get("pid").is_none() && receipt.get("job_id").is_none());
    let initial: OutputPage = serde_json::from_value(receipt["initial"].clone()).unwrap();
    assert_eq!(text(&initial), "initial-你好");
    assert_eq!(initial.execution_id, id);
    commands.send(Command::Append("-later")).unwrap();
    commands.send(Command::Finish(Ok(()))).unwrap();
    let terminal = status(&session, ExecutionStatus::Succeeded).await;
    assert_eq!(terminal.execution_id, id);
    assert_eq!(session.execution_snapshots().len(), 1);
    let next = session
        .read_execution_output(id, initial.next_cursor, 4096)
        .unwrap();
    assert_eq!(text(&next), "-later");
    assert!(next.closed);
    let ui = session.read_execution_output(id, 0, 4096).unwrap();
    assert_eq!(text(&ui), "initial-你好-later");
    let llm = session.executions().read(id, 0, 4096).unwrap();
    assert_eq!(text(&llm), text(&ui), "UI reads cannot consume LLM output");
    session
        .executions()
        .release(id, CancellationToken::new())
        .await
        .unwrap();
    let prefix = session.messages();
    session.submit("observe terminal once").await.unwrap();
    let boundary = session.messages().len();
    session
        .submit("unchanged terminal must not repeat")
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(&session.messages()[..prefix.len()]).unwrap(),
        serde_json::to_value(prefix).unwrap()
    );
    assert!(
        session.messages()[boundary..]
            .iter()
            .filter_map(|item| match item {
                ContentItem::State(frame) => Some(serde_json::to_value(frame).unwrap()),
                _ => None,
            })
            .all(|frame| frame["set"].get("executions").is_none()),
        "unchanged terminal metadata must not be duplicated"
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn immediate_empty_buffer_is_valid_and_cancel_acknowledges_actual_resource_cleanup() {
    let (producer, commands, cleaned) = producer("");
    let (session, _) = session(producer, Limits::default());
    session.submit("start empty producer").await.unwrap();
    let row = status(&session, ExecutionStatus::Running).await;
    let initial: Value = serde_json::from_str(&result(&session)).unwrap();
    assert_eq!(initial["initial"]["chunks"], json!([]));
    assert!(!initial["initial"]["closed"].as_bool().unwrap());
    session.cancel_execution(row.execution_id).await.unwrap();
    status(&session, ExecutionStatus::Cancelled).await;
    assert!(cleaned.load(Ordering::SeqCst));
    assert!(commands.send(Command::Append("must not run")).is_err());
    assert!(
        session
            .read_execution_output(row.execution_id, 0, 4096)
            .unwrap()
            .closed
    );
    session.close().await.unwrap();
}

struct Final {
    content: String,
    failure: Option<ToolFailure>,
}
#[async_trait]
impl Tool for Final {
    fn definition(&self) -> ToolDefinition {
        definition()
    }
    async fn execute(&self, _: Value, _: ToolContext) -> Result<ToolOutput, Error> {
        Ok(match &self.failure {
            Some(failure) => ToolOutput::failed(self.content.clone(), failure.clone()),
            None => ToolOutput::text(self.content.clone()),
        })
    }
}
#[tokio::test]
async fn final_and_buffered_failures_preserve_partial_content_and_tool_reported_cause() {
    let failure = ToolFailure::failed("partial_fixture", "Producer reported failure");
    let (foreground, _) = session(
        Arc::new(Final {
            content: "partial-final".into(),
            failure: Some(failure.clone()),
        }),
        Limits::default(),
    );
    foreground.submit("partial Final").await.unwrap();
    let row = status(&foreground, ExecutionStatus::Failed).await;
    assert_eq!(row.failure, Some(failure.clone()));
    let delivered: Value = serde_json::from_str(&result(&foreground)).unwrap();
    assert_eq!(delivered["content"], "partial-final");
    assert_eq!(delivered["failure"], json!(failure));
    assert!(foreground.executions().model_projection().is_empty());
    assert_eq!(
        text(
            &foreground
                .read_execution_output(row.execution_id, 0, 4096)
                .unwrap()
        ),
        "partial-final"
    );
    foreground.close().await.unwrap();
    let (producer, commands, _) = producer("partial-buffered");
    let (buffered, _) = session(producer, Limits::default());
    buffered.submit("partial Buffered").await.unwrap();
    commands
        .send(Command::Finish(Err(failure.clone())))
        .unwrap();
    let row = status(&buffered, ExecutionStatus::Failed).await;
    assert_eq!(row.failure, Some(failure));
    assert_eq!(
        text(
            &buffered
                .read_execution_output(row.execution_id, 0, 4096)
                .unwrap()
        ),
        "partial-buffered"
    );
    buffered.close().await.unwrap();
}

#[tokio::test]
async fn opaque_arbitrary_json_and_malformed_json_never_derive_exit_or_background_semantics() {
    for content in [
        r#"{"exit_code":9,"run_in_background":true,"pid":876,"job_id":7}"#,
        "{malformed",
        r#"{"truncated":false,"state":"running","error":"ordinary data"}"#,
    ] {
        let (session, _) = session(
            Arc::new(Final {
                content: content.into(),
                failure: None,
            }),
            Limits::default(),
        );
        session.submit("opaque result").await.unwrap();
        assert_eq!(result(&session), content);
        assert_eq!(session.execution_snapshots().len(), 1);
        assert_eq!(
            session.execution_snapshots()[0].status,
            ExecutionStatus::Succeeded
        );
        assert!(session.execution_snapshots()[0].failure.is_none());
        assert!(session.executions().model_projection().is_empty());
        session.close().await.unwrap();
    }
}

struct Startup {
    release: Mutex<Option<oneshot::Receiver<()>>>,
    entered: tokio::sync::Notify,
}
#[async_trait]
impl Tool for Startup {
    fn definition(&self) -> ToolDefinition {
        definition()
    }
    async fn execute(&self, _: Value, context: ToolContext) -> Result<ToolOutput, Error> {
        let release = self.release.lock().unwrap().take().unwrap();
        self.entered.notify_one();
        tokio::select! {
            biased;
            _ = context.cancellation.cancelled() => return Err(Error::Cancelled),
            result = release => result.map_err(|_| Error::Cancelled)?,
        }
        Ok(ToolOutput::failed(
            "startup-partial",
            ToolFailure {
                kind: ToolFailureKind::Timeout,
                code: "startup_timeout".into(),
                message: "Tool startup deadline expired".into(),
                retryable: true,
            },
        ))
    }
}
#[tokio::test]
async fn startup_timeout_is_reported_by_tool_after_its_own_wait() {
    let (release, receiver) = oneshot::channel();
    let tool = Arc::new(Startup {
        release: Mutex::new(Some(receiver)),
        entered: tokio::sync::Notify::new(),
    });
    let (session, model) = session(
        tool.clone(),
        Limits {
            interaction_timeout_ms: 1,
            ..Limits::default()
        },
    );
    let running = session.clone();
    let worker = tokio::spawn(async move { running.submit("tool owns startup timeout").await });
    tool.entered.notified().await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !worker.is_finished(),
        "Runtime cannot synthesize an elapsed tool timeout"
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert!(session.execution_snapshots()[0].status.is_pending());
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let row = status(&session, ExecutionStatus::Failed).await;
    let failure = row.failure.unwrap();
    assert_eq!(failure.kind, ToolFailureKind::Timeout);
    assert_eq!(failure.code, "startup_timeout");
    assert!(result(&session).contains("startup-partial"));
    session.close().await.unwrap();
}

struct FragmentedOutput;
#[async_trait]
impl Tool for FragmentedOutput {
    fn definition(&self) -> ToolDefinition {
        definition()
    }
    async fn execute(&self, _: Value, context: ToolContext) -> Result<ToolOutput, Error> {
        let channel = "c".repeat(64);
        let other = "d".repeat(64);
        let (writer, output) = context.open_output(
            OutputOptions {
                channels: vec![channel.clone(), other.clone()],
            },
            Arc::new(
                microcrab_session::runtime::execution::CancellationController::new(
                    CancellationToken::new(),
                ),
            ),
        )?;
        // Every retained byte has its own channel/cursor envelope. Budget the
        // complete response while retaining every byte across nonconsuming page reads.
        for index in 0..1024 {
            let character = char::from(b'a' + (index % 26) as u8).to_string();
            writer.append(
                Some(if index % 2 == 0 { &channel } else { &other }),
                &character,
            )?;
        }
        writer.finish(Ok(()))?;
        Ok(ToolOutput::Buffered(output))
    }
}

struct ReadPagesModel {
    rounds: AtomicUsize,
    requests: Mutex<Vec<Content>>,
}
#[async_trait]
impl Client for ReadPagesModel {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.requests.lock().unwrap().push(content.clone());
        let round = self.rounds.fetch_add(1, Ordering::SeqCst);
        let calls = if round == 0 {
            vec![ToolCall {
                provider_metadata: None,
                id: "producer-call".into(),
                name: "observe_catalog".into(),
                arguments: json!({}),
            }]
        } else {
            let receipt: Value = content
                .items
                .iter()
                .find_map(|item| match item {
                    ContentItem::Tool { call_id, content, .. } if call_id == "producer-call" => {
                        Some(serde_json::from_str(content).unwrap())
                    }
                    _ => None,
                })
                .expect("producer receipt must be delivered to the model");
            let latest = content.items.iter().rev().find_map(|item| match item {
                ContentItem::Tool { call_id, content, .. } if call_id.starts_with("read-page-") => {
                    Some(
                        serde_json::from_str::<OutputPage>(content)
                            .expect("every successful read page must reach Context as output"),
                    )
                }
                _ => None,
            });
            if latest
                .as_ref()
                .is_some_and(|page| page.closed && page.next_cursor == page.end_cursor)
            {
                vec![]
            } else {
                vec![ToolCall {
                    provider_metadata: None,
                    id: format!("read-page-{round}"),
                    name: "job".into(),
                    arguments: json!({"id":receipt["id"],"action":"read","args":{"cursor":latest.as_ref().map_or(0, |page| page.next_cursor)}}),
                }]
            }
        };
        Ok(Response {
            provider_metadata: None,
            content: "read retained output".into(),
            tool_calls: calls,
            usage: Default::default(),
        })
    }
}

#[tokio::test]
async fn builtin_read_budgets_alternating_chunk_envelopes_and_retains_until_explicit_release() {
    let limits = Limits {
        output_buffer_bytes: 1024,
        max_tool_output_bytes: 128 * 1024,
        max_rounds: 64,
        ..Limits::default()
    };
    let accepted_page_bytes = limits.output_buffer_bytes * 6 + 16 * 1024;
    let client = Arc::new(ReadPagesModel {
        rounds: AtomicUsize::new(0),
        requests: Mutex::new(vec![]),
    });
    let runtime = Runtime::new(
        vec![
            ToolRegistration::External(Arc::new(FragmentedOutput)),
            ToolRegistration::Builtin(microcrab_session::runtime::BuiltinTool::Job),
        ],
        Arc::new(AllowTools::new(["observe_catalog", "job"])),
    )
    .unwrap();
    let session = Session::builder_arc(client.clone(), runtime)
        .session_id("fragmented-output")
        .limits(limits)
        .build()
        .unwrap();
    session
        .submit("read all fragmented output with builtin job")
        .await
        .expect("bounded reads must reach Context without losing retained output");

    let receipt: Value = serde_json::from_str(&result(&session)).unwrap();
    let id = ExecutionId(receipt["execution_id"].as_u64().unwrap());
    assert_eq!(receipt["id"], format!("fragmented-output/{}", id.0));
    assert_eq!(
        receipt["output"]["end_cursor"].as_u64().unwrap()
            - receipt["output"]["start_cursor"].as_u64().unwrap(),
        1024
    );
    let messages = session.messages();
    let pages: Vec<_> = messages
        .iter()
        .filter_map(|item| match item {
            ContentItem::Tool { call_id, content, .. } if call_id.starts_with("read-page-") => {
                assert!(content.len() <= accepted_page_bytes);
                Some(
                    serde_json::from_str::<OutputPage>(content)
                        .expect("read output must remain a page in committed Context"),
                )
            }
            _ => None,
        })
        .collect();
    assert!(
        pages.len() > 1,
        "chunk envelopes must force actual builtin pagination"
    );
    let channel = "c".repeat(64);
    let mut cursor = 0;
    let mut actual = String::new();
    for page in &pages {
        assert_eq!(page.execution_id, id);
        assert!(page.closed);
        assert_eq!(page.cursor, cursor);
        assert!(page.next_cursor > cursor);
        for chunk in &page.chunks {
            assert_eq!(
                chunk.channel.as_deref(),
                Some(if cursor % 2 == 0 {
                    channel.as_str()
                } else {
                    "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                })
            );
            assert_eq!(chunk.cursor, cursor);
            cursor = chunk.next_cursor;
            actual.push_str(&chunk.content);
        }
        assert_eq!(page.next_cursor, cursor);
    }
    let expected: String = (0..1024)
        .map(|index| char::from(b'a' + (index % 26) as u8))
        .collect();
    assert_eq!(actual, expected);
    assert_eq!(cursor, 1024);
    assert_eq!(
        pages.last().unwrap().next_cursor,
        pages.last().unwrap().end_cursor
    );
    {
        let requests = client.requests.lock().unwrap();
        let last_request = requests.last().unwrap();
        let observed_pages = last_request
            .items
            .iter()
            .filter(|item| {
                matches!(item,
                    ContentItem::Tool { call_id, .. } if call_id.starts_with("read-page-")
                )
            })
            .count();
        assert_eq!(
            observed_pages,
            pages.len(),
            "the model must observe every committed page"
        );
    }

    let retained = session.executions().get(id).unwrap();
    assert_eq!(
        retained.output_availability,
        Some(microcrab_session::runtime::execution::OutputAvailability::Retained)
    );
    assert_eq!(
        text(&session.read_execution_output(id, 0, 256 * 1024).unwrap()),
        expected
    );
    session
        .executions()
        .release(id, CancellationToken::new())
        .await
        .unwrap();
    let mut changed = session.executions().subscribe();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if !session
                .executions()
                .model_projection()
                .iter()
                .any(|row| row.execution_id == id)
            {
                break;
            }
            changed.changed().await.unwrap();
        }
    })
    .await
    .expect("explicit release must finish after controller acknowledgment");
    assert_eq!(
        session
            .executions()
            .get(id)
            .unwrap()
            .output
            .unwrap()
            .start_cursor,
        1024
    );
    session.close().await.unwrap();
}
