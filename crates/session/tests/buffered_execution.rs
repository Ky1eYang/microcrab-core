//! Tool-owned buffered producers keep one execution, with bounded live
//! State, idle durable completion, explicit cancellation and no recovery replay.
use async_trait::async_trait;
use microcrab_auditor::Auditor;
use microcrab_protocol::{Response, ToolCall, ToolDefinition};
use microcrab_session::runtime::execution::ToolOutput;
use microcrab_session::{
    Client, Error, Limits, Session,
    context::Content,
    runtime::{
        Runtime, ToolRegistration,
        execution::{ExecutionSnapshot, ExecutionStatus},
        journal::{Journal, MemoryJournal, Recover},
        tool::{AllowTools, Tool, ToolContext},
    },
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{oneshot, watch};
use tokio_util::sync::CancellationToken;

struct Script {
    calls: Arc<AtomicUsize>,
}
#[async_trait]
impl Client for Script {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
        Ok(Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: if first {
                vec![ToolCall {
                    provider_metadata: None,
                    id: "owned-call".into(),
                    name: "held".into(),
                    arguments: json!({}),
                }]
            } else {
                vec![]
            },
            usage: Default::default(),
        })
    }
}
// This fixture alone chooses whether execute waits or returns its owned buffer.
#[derive(Clone, Copy, PartialEq, Eq)]
enum HeldMode {
    Normal,
    Interaction { deferred: bool },
}
struct Held {
    effect_before_wait: bool,
    started: tokio::sync::Notify,
    policy: HeldMode,
    release: Mutex<Option<oneshot::Receiver<()>>>,
    starts: Arc<AtomicUsize>,
    effects: Arc<AtomicUsize>,
}
#[async_trait]
impl Tool for Held {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "held".into(),
            description: "Fixture with explicit one-shot lifetime".into(),
            parameters: json!({"type":"object","additionalProperties":false}),
        }
    }
    async fn execute(&self, _: Value, context: ToolContext) -> Result<ToolOutput, Error> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        if self.effect_before_wait {
            self.effects.fetch_add(1, Ordering::SeqCst);
        }
        self.started.notify_one();
        let release = self.release.lock().unwrap().take().unwrap();
        if self.policy == (HeldMode::Interaction { deferred: true }) {
            let controller = Arc::new(ProducerController {
                token: CancellationToken::new(),
                worker: Mutex::new(None),
            });
            let (writer, output) = context.open_output(Default::default(), controller.clone())?;
            let token = controller.token.clone();
            let effects = self.effects.clone();
            let effect_before_wait = self.effect_before_wait;
            let worker = tokio::spawn(async move {
                let outcome = tokio::select! {
                    biased;
                    _ = token.cancelled() => Err(microcrab_session::runtime::execution::ToolFailure {
                        kind: microcrab_session::runtime::execution::ToolFailureKind::Cancelled,
                        code: "fixture_cancelled".into(), message: "Producer cancelled".into(), retryable: false,
                    }),
                    result = release => match result {
                        Ok(()) => {
                            if !effect_before_wait { effects.fetch_add(1, Ordering::SeqCst); }
                            writer.append(None, "actual final result").unwrap();
                            Ok(())
                        }
                        Err(_) => Err(microcrab_session::runtime::execution::ToolFailure::failed("fixture_closed", "Producer closed")),
                    },
                };
                writer.finish(outcome).unwrap();
            });
            *controller.worker.lock().unwrap() = Some(worker);
            return Ok(ToolOutput::Buffered(output));
        }
        if let Some(progress) = &context.execution_progress
            && matches!(self.policy, HeldMode::Interaction { .. })
        {
            progress.mark_waiting();
        }
        tokio::select! {
            biased;
            _ = context.cancellation.cancelled() => return Err(Error::Cancelled),
            result = release => result.map_err(|_| Error::Cancelled)?,
        }
        if !self.effect_before_wait {
            self.effects.fetch_add(1, Ordering::SeqCst);
        }
        Ok(ToolOutput::text("actual final result"))
    }
}
struct ProducerController {
    token: CancellationToken,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl Drop for ProducerController {
    fn drop(&mut self) {
        self.token.cancel();
        if let Some(worker) = self.worker.get_mut().unwrap().take() {
            worker.abort();
        }
    }
}
#[async_trait]
impl microcrab_session::runtime::execution::ExecutionController for ProducerController {
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
    async fn release(&self, cancellation: CancellationToken) -> Result<(), Error> {
        self.cancel(cancellation).await
    }
}
fn output_text(page: microcrab_session::runtime::execution::OutputPage) -> String {
    page.chunks.into_iter().map(|chunk| chunk.content).collect()
}
#[derive(Clone)]
struct RecordingJournal {
    memory: MemoryJournal,
    changed: watch::Sender<u64>,
}
impl RecordingJournal {
    fn new() -> Self {
        let (changed, _) = watch::channel(0);
        Self {
            memory: MemoryJournal::new(),
            changed,
        }
    }
    async fn terminal(&self) -> Recover {
        let mut changes = self.changed.subscribe();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(recover) = self.memory.latest()
                    && recover
                        .state
                        .executions
                        .iter()
                        .any(|row| !row.status.is_pending())
                {
                    return recover;
                }
                changes.changed().await.unwrap();
            }
        })
        .await
        .expect("late result must persist without another user turn")
    }
}
#[async_trait]
impl Journal for RecordingJournal {
    async fn commit(&self, recover: Recover) -> Result<(), Error> {
        self.memory.commit(recover).await?;
        self.changed.send_modify(|value| *value += 1);
        Ok(())
    }
}
fn runtime(tool: Arc<Held>) -> Runtime {
    Runtime::new(
        vec![ToolRegistration::External(tool)],
        Arc::new(AllowTools::new(["held"])),
    )
    .unwrap()
}
fn held(policy: HeldMode) -> (Arc<Held>, oneshot::Sender<()>) {
    let (release, received) = oneshot::channel();
    (
        Arc::new(Held {
            effect_before_wait: false,
            started: tokio::sync::Notify::new(),
            policy,
            release: Mutex::new(Some(received)),
            starts: Arc::new(AtomicUsize::new(0)),
            effects: Arc::new(AtomicUsize::new(0)),
        }),
        release,
    )
}
fn limits() -> Limits {
    Limits {
        ..Limits::default()
    }
}
async fn row(session: &Session, wanted: ExecutionStatus) -> ExecutionSnapshot {
    let mut changes = session.executions().subscribe();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(row) = session
                .executions()
                .snapshot()
                .into_iter()
                .find(|row| row.status == wanted)
            {
                return row;
            }
            changes.changed().await.unwrap();
        }
    })
    .await
    .expect("expected actual execution transition")
}
#[tokio::test]
async fn explicit_buffered_owner_completes_once_and_idle_completion_is_durable() {
    let (tool, release) = held(HeldMode::Interaction { deferred: true });
    let journal = RecordingJournal::new();
    let session = Session::builder(
        Script {
            calls: Arc::new(AtomicUsize::new(0)),
        },
        runtime(tool.clone()),
    )
    .session_id("buffered-execution")
    .limits(limits())
    .journal(journal.clone())
    .build()
    .unwrap();
    session.submit("run once").await.unwrap();
    let pending = row(&session, ExecutionStatus::Running).await;
    assert_eq!(pending.call_id, "owned-call");
    assert!(
        pending.started_at_unix_ms.is_some(),
        "admission records the actual execution start"
    );
    assert_eq!(tool.starts.load(Ordering::SeqCst), 1);
    assert_eq!(tool.effects.load(Ordering::SeqCst), 0);
    assert!(session.messages().iter().any(|item| matches!(item, microcrab_session::context::ContentItem::Tool { content, .. } if content.contains("execution_id"))));
    session.interrupt(); // an already transferred future no longer belongs to this turn
    release.send(()).unwrap();
    let complete = row(&session, ExecutionStatus::Succeeded).await;
    assert!(complete.result.is_none());
    assert_eq!(
        output_text(
            session
                .read_execution_output(complete.execution_id, 0, 65536)
                .unwrap()
        ),
        "actual final result"
    );
    assert_eq!(tool.effects.load(Ordering::SeqCst), 1);
    let durable = journal.terminal().await;
    let recovered_output =
        microcrab_session::runtime::execution::ExecutionRegistry::new(durable.state.limits.clone());
    recovered_output.restore(&durable.state.executions).unwrap();
    assert_eq!(
        output_text(
            recovered_output
                .read(complete.execution_id, 0, 65536)
                .unwrap()
        ),
        "actual final result"
    );
    let (replacement, _release) = held(HeldMode::Normal);
    let restored = Session::restore(
        Session::builder(
            Script {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            runtime(replacement.clone()),
        )
        .session_id("buffered-execution")
        .limits(limits()),
        durable,
    )
    .await
    .unwrap();
    assert_eq!(
        restored.executions().snapshot()[0].status,
        ExecutionStatus::Succeeded
    );
    assert_eq!(replacement.starts.load(Ordering::SeqCst), 0);
    restored.close().await.unwrap();
    session.close().await.unwrap();
}
#[tokio::test]
async fn pending_recovery_is_interrupted_and_never_replayed() {
    let (tool, _release) = held(HeldMode::Interaction { deferred: true });
    let journal = RecordingJournal::new();
    let session = Session::builder(
        Script {
            calls: Arc::new(AtomicUsize::new(0)),
        },
        runtime(tool),
    )
    .limits(limits())
    .journal(journal.clone())
    .build()
    .unwrap();
    session.submit("run once").await.unwrap();
    let durable = journal.memory.latest().unwrap();
    assert!(durable.state.executions[0].status.is_pending());
    let (replacement, _release) = held(HeldMode::Normal);
    let restored = Session::restore(
        Session::builder(
            Script {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            runtime(replacement.clone()),
        )
        .limits(limits()),
        durable,
    )
    .await
    .unwrap();
    let observation = restored.executions().snapshot();
    assert_eq!(observation[0].status, ExecutionStatus::Interrupted);
    assert!(observation[0].result.is_none());
    assert_eq!(replacement.starts.load(Ordering::SeqCst), 0);
    restored.close().await.unwrap();
    session.close().await.unwrap();
}
#[tokio::test]
async fn deferred_receipt_has_real_owner_and_explicit_cancel_settles_it() {
    let (tool, _release) = held(HeldMode::Interaction { deferred: true });
    let session = Session::builder(
        Script {
            calls: Arc::new(AtomicUsize::new(0)),
        },
        runtime(tool.clone()),
    )
    .limits(limits())
    .build()
    .unwrap();
    session.submit("ask in background").await.unwrap();
    let pending = row(&session, ExecutionStatus::Running).await;
    assert!(
        session.state_view().unwrap()["executions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value["execution_id"] == json!(pending.execution_id))
    );
    session
        .cancel_execution(pending.execution_id)
        .await
        .unwrap();
    row(&session, ExecutionStatus::Cancelled).await;
    assert_eq!(tool.effects.load(Ordering::SeqCst), 0);
    session.close().await.unwrap();
}
#[tokio::test]
async fn blocking_wait_has_no_automatic_elapsed_tool_timeout() {
    let (tool, release) = held(HeldMode::Interaction { deferred: false });
    let calls = Arc::new(AtomicUsize::new(0));
    let session = Session::builder(
        Script {
            calls: calls.clone(),
        },
        runtime(tool.clone()),
    )
    .limits(Limits {
        interaction_timeout_ms: 5,
        ..Limits::default()
    })
    .build()
    .unwrap();
    let worker_session = session.clone();
    let worker = tokio::spawn(async move { worker_session.submit("ask and wait").await });
    row(&session, ExecutionStatus::Waiting).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        !worker.is_finished(),
        "elapsed tool/interaction limits cannot finish a tool-owned question"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "unresolved question must not call model again"
    );
    assert_eq!(tool.effects.load(Ordering::SeqCst), 0);
    release.send(()).unwrap();
    worker.await.unwrap().unwrap();
    assert_eq!(tool.effects.load(Ordering::SeqCst), 1);
    session.close().await.unwrap();
}
#[tokio::test]
async fn close_cancels_and_joins_background_work_before_acknowledgement() {
    let (tool, release) = held(HeldMode::Interaction { deferred: true });
    let session = Session::builder(
        Script {
            calls: Arc::new(AtomicUsize::new(0)),
        },
        runtime(tool.clone()),
    )
    .build()
    .unwrap();
    session.submit("pending").await.unwrap();
    session.close().await.unwrap();
    assert_eq!(
        session.executions().snapshot()[0].status,
        ExecutionStatus::Cancelled
    );
    assert!(
        release.send(()).is_err(),
        "execution future must be dropped before close acknowledgement"
    );
    assert_eq!(tool.effects.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dropping_last_session_aborts_pending_future_without_registry_cycle() {
    let (tool, mut release) = held(HeldMode::Interaction { deferred: true });
    let session = Session::builder(
        Script {
            calls: Arc::new(AtomicUsize::new(0)),
        },
        runtime(tool.clone()),
    )
    .build()
    .unwrap();
    session.submit("pending").await.unwrap();
    tool.started.notified().await;
    drop(session);
    tokio::time::timeout(Duration::from_secs(5), release.closed())
        .await
        .expect("last Session drop must abort owned future despite ToolContext State references");
    assert_eq!(tool.effects.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn admission_recovery_interrupts_a_partly_effectful_call_without_resuming_it() {
    let (mut tool, _release) = held(HeldMode::Interaction { deferred: false });
    Arc::get_mut(&mut tool).unwrap().effect_before_wait = true;
    let journal = RecordingJournal::new();
    let session = Session::builder(
        Script {
            calls: Arc::new(AtomicUsize::new(0)),
        },
        runtime(tool.clone()),
    )
    .journal(journal.clone())
    .build()
    .unwrap();
    let running_session = session.clone();
    let running = tokio::spawn(async move { running_session.submit("effect then wait").await });
    tool.started.notified().await;
    assert_eq!(tool.effects.load(Ordering::SeqCst), 1);
    let admission = journal.memory.latest().unwrap();
    assert!(
        admission.continuation.is_none(),
        "restart cannot resume the pre-effect model continuation"
    );
    assert!(admission.state.executions[0].status.is_pending());
    assert!(
        admission
            .state
            .executions
            .iter()
            .any(|row| row.call_id == "owned-call")
    );
    let (mut replacement, _release) = held(HeldMode::Interaction { deferred: false });
    {
        let replacement = Arc::get_mut(&mut replacement).unwrap();
        replacement.effect_before_wait = true;
        replacement.effects = tool.effects.clone();
    }
    let restored = Session::restore(
        Session::builder(
            Script {
                calls: Arc::new(AtomicUsize::new(1)),
            },
            runtime(replacement.clone()),
        ),
        admission,
    )
    .await
    .unwrap();
    assert_eq!(
        restored.executions().snapshot()[0].status,
        ExecutionStatus::Interrupted
    );
    assert!(restored.resume().await.is_err());
    restored
        .submit("report the interrupted execution only")
        .await
        .unwrap();
    assert_eq!(replacement.starts.load(Ordering::SeqCst), 0);
    assert_eq!(tool.effects.load(Ordering::SeqCst), 1);
    restored.close().await.unwrap();
    session.close().await.unwrap();
    assert!(matches!(running.await.unwrap(), Err(Error::Cancelled)));
    assert_eq!(tool.effects.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn caller_cancellation_releases_model_wait_after_a_settled_tool_without_a_turn_deadline() {
    struct RequestGuard(Arc<AtomicUsize>);
    impl Drop for RequestGuard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct ProviderBudget {
        script: Script,
        dropped: Arc<AtomicUsize>,
        entered: Arc<tokio::sync::Notify>,
    }
    #[async_trait]
    impl Client for ProviderBudget {
        async fn complete(
            &self,
            content: &Content,
            auditor: &dyn Auditor,
            variables: &dyn microcrab_auditor::VariableView,
            cancellation: CancellationToken,
        ) -> Result<Response, microcrab_transport::Error> {
            if self.script.calls.load(Ordering::SeqCst) == 0 {
                return self
                    .script
                    .complete(content, auditor, variables, cancellation)
                    .await;
            }
            let _request = RequestGuard(self.dropped.clone());
            self.entered.notify_one();
            // Session cancellation must drop even a custom client that ignores
            // its token. No historical turn deadline may terminate this wait.
            std::future::pending().await
        }
    }
    let (tool, release) = held(HeldMode::Normal);
    release.send(()).unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let session = Session::builder(
        ProviderBudget {
            script: Script {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            dropped: dropped.clone(),
            entered: entered.clone(),
        },
        runtime(tool.clone()),
    )
    .limits(Limits {
        ..Limits::default()
    })
    .build()
    .unwrap();
    let running = session.clone();
    let worker = tokio::spawn(async move {
        running
            .submit("settle the tool then wait for provider work")
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .expect("provider must be entered after tool settlement");
    tokio::time::advance(Duration::from_secs(600)).await;
    assert!(!worker.is_finished());
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    session.interrupt();
    let outcome = tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("caller cancellation must release the model wait")
        .unwrap();
    assert!(matches!(outcome, Err(Error::Cancelled)), "{outcome:?}");
    row(&session, ExecutionStatus::Succeeded).await;
    assert_eq!(tool.effects.load(Ordering::SeqCst), 1);
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        1,
        "caller cancellation releases the actual provider request future"
    );
    assert!(
        !session.executions().has_pending(),
        "model cancellation cannot resurrect or cancel the settled tool"
    );
    session.close().await.unwrap();
}

#[derive(Clone)]
struct HeldLateCommit {
    memory: MemoryJournal,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
    commits: Arc<AtomicUsize>,
    boundaries: Arc<Mutex<Vec<microcrab_session::NavigationBoundary>>>,
}
#[async_trait]
impl Journal for HeldLateCommit {
    async fn commit(&self, recover: Recover) -> Result<(), Error> {
        if recover
            .state
            .executions
            .iter()
            .any(|row| row.status == ExecutionStatus::Succeeded)
            && self.release.available_permits() == 0
        {
            self.entered.notify_one();
            let permit = self.release.acquire().await.map_err(|_| Error::Cancelled)?;
            permit.forget();
            self.release.add_permits(100);
        }
        self.memory.commit(recover).await?;
        self.commits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn commit_boundary(
        &self,
        recover: Recover,
        boundary: microcrab_session::NavigationBoundary,
    ) -> Result<(), Error> {
        self.commit(recover).await?;
        self.boundaries.lock().unwrap().push(boundary);
        Ok(())
    }
    async fn recovered(&self, recover: Recover) -> Result<(), Error> {
        self.memory.recovered(recover).await
    }
}
#[tokio::test]
async fn sequential_public_operations_await_internal_checkpoint_but_competing_public_work_is_busy()
{
    tokio::time::timeout(Duration::from_secs(10), async {
        for operation in [
            "submit",
            "mode",
            "compact",
            "recover",
            "checkpoint",
            "start",
        ] {
            let (tool, release) = held(HeldMode::Interaction { deferred: true });
            let journal = HeldLateCommit {
                memory: MemoryJournal::new(),
                entered: Arc::new(tokio::sync::Notify::new()),
                release: Arc::new(tokio::sync::Semaphore::new(0)),
                commits: Arc::new(AtomicUsize::new(0)),
                boundaries: Arc::new(Mutex::new(vec![])),
            };
            let agent = microcrab_agent_types::AgentInfo {
                name: "fixture".into(),
                modes: ["general", "alternate"]
                    .into_iter()
                    .map(|name| microcrab_agent_types::AgentMode {
                        description: String::new(),
                        prompt: String::new(),
                        name: name.into(),
                        role: microcrab_agent_types::AgentRole::MainAgent,
                        permissions: vec![],
                    })
                    .collect(),
                tools: vec![],
                variables: Default::default(),
            };
            let session = Session::builder(
                Script {
                    calls: Arc::new(AtomicUsize::new(0)),
                },
                runtime(tool),
            )
            .agent(agent)
            .journal(journal.clone())
            .build()
            .unwrap();
            session
                .submit("start real pending execution")
                .await
                .unwrap();
            let historical = journal.memory.latest().unwrap();
            // This case isolates storage-only checkpoint contention. Pause idle
            // model admission before terminal publication so the held commit
            // cannot instead belong to a real automatic Wake turn. The producer
            // and unclaimed notice stay owned; genuine public Busy is unchanged.
            session.interrupt();
            release.send(()).unwrap();
            journal.entered.notified().await;
            if operation == "mode" {
                tokio::time::timeout(Duration::from_secs(1), session.select_mode("alternate"))
                    .await
                    .expect("mode admission must not wait for the held storage checkpoint")
                    .unwrap();
                assert_eq!(session.state().selected_mode.as_deref(), Some("alternate"));
                journal.release.add_permits(1);
                session.start().await.unwrap();
                session.close().await.unwrap();
                continue;
            }
            let public = async {
                match operation {
                    "submit" => session.submit("next input").await.map(|_| ()),
                    "mode" => session.select_mode("alternate").await,
                    "compact" => session.compact("summary", json!({})).await,
                    "recover" => session.recover(historical).await,
                    "checkpoint" => session.checkpoint().await.map(|_| ()),
                    "start" => session.start().await,
                    _ => unreachable!(),
                }
            };
            tokio::pin!(public);
            assert!(
                matches!(futures_util::poll!(&mut public), std::task::Poll::Pending),
                "{operation} must await the internal writer rather than return Busy"
            );
            assert!(
                matches!(
                    session.submit("genuinely concurrent public call").await,
                    Err(Error::Busy)
                ),
                "waiting public admission owns foreground exclusion"
            );
            journal.release.add_permits(1);
            public
                .await
                .unwrap_or_else(|error| panic!("sequential {operation} failed: {error}"));
            session.close().await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn acknowledged_foreground_generations_do_not_checkpoint_again_or_move_selected_history() {
    let (tool, release) = held(HeldMode::Normal);
    release.send(()).unwrap();
    let journal = HeldLateCommit {
        memory: MemoryJournal::new(),
        entered: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Semaphore::new(100)),
        commits: Arc::new(AtomicUsize::new(0)),
        boundaries: Arc::new(Mutex::new(vec![])),
    };
    let session = Session::builder(
        Script {
            calls: Arc::new(AtomicUsize::new(0)),
        },
        runtime(tool),
    )
    .journal(journal.clone())
    .build()
    .unwrap();
    session.submit("synchronous tool turn").await.unwrap();
    // Fair operation admission orders behind the already-queued watcher. It must
    // observe that the final foreground commit covered its generation and skip.
    session.start().await.unwrap();
    {
        let boundaries = journal.boundaries.lock().unwrap();
        assert_eq!(
            boundaries.as_slice(),
            &[
                microcrab_session::NavigationBoundary::EpochInitial,
                microcrab_session::NavigationBoundary::InterTurn,
            ],
            "one initialized baseline and one settled Turn boundary are certified"
        );
        assert_eq!(
            journal.commits.load(Ordering::SeqCst) - boundaries.len(),
            4,
            "startup intent + execution admission + two round commits; no duplicate generation checkpoint"
        );
    }
    let selected = journal.memory.latest().unwrap();
    session.submit("a later settled turn").await.unwrap();
    let committed = journal.commits.load(Ordering::SeqCst);
    session.recover(selected.clone()).await.unwrap();
    session.start().await.unwrap();
    assert_eq!(
        journal.commits.load(Ordering::SeqCst),
        committed,
        "travel must not manufacture a checkpoint from old execution notifications"
    );
    assert_eq!(
        serde_json::to_value(journal.memory.latest().unwrap()).unwrap(),
        serde_json::to_value(selected).unwrap()
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn mode_admitted_during_recovery_io_survives_historical_state_publication() {
    #[derive(Clone)]
    struct HeldRestore {
        memory: MemoryJournal,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Semaphore>,
    }
    #[async_trait]
    impl Journal for HeldRestore {
        async fn commit(&self, recover: Recover) -> Result<(), Error> {
            self.memory.commit(recover).await
        }
        async fn recovered(&self, recover: Recover) -> Result<(), Error> {
            self.entered.notify_one();
            self.release
                .acquire()
                .await
                .map_err(|_| Error::Closed)?
                .forget();
            self.memory.recovered(recover).await
        }
    }
    let journal = HeldRestore {
        memory: MemoryJournal::new(),
        entered: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Semaphore::new(0)),
    };
    let agent = microcrab_agent_types::AgentInfo {
        name:"mode-fixture".into(), modes:["general", "alternate"].into_iter().map(|name| microcrab_agent_types::AgentMode {
            description: String::new(), prompt: String::new(), name:name.into(), role:microcrab_agent_types::AgentRole::MainAgent, permissions:vec![serde_json::from_value(json!({"tool":{"kind":"exact","value":"held"},"action":if name == "general" {"deny"} else {"ask"}})).unwrap()],
        }).collect(), tools:vec![], variables:Default::default(),
    };
    let (tool, _unused_release) = held(HeldMode::Normal);
    let session = Session::builder(
        Script {
            calls: Arc::new(AtomicUsize::new(1)),
        },
        Runtime::new(
            vec![ToolRegistration::Wasm(tool)],
            Arc::new(AllowTools::new(["held"])),
        )
        .unwrap(),
    )
    .agent(agent)
    .journal(journal.clone())
    .build()
    .unwrap();
    session.start().await.unwrap();
    let historical = session.checkpoint().await.unwrap();
    assert_eq!(historical.state.selected_mode.as_deref(), Some("general"));
    let original_policy =
        serde_json::to_value(session.permission_controller().snapshot().unwrap()).unwrap();
    assert!(
        original_policy["config"]["provider_defaults"]
            .as_array()
            .unwrap()
            .iter()
            .any(|rule| rule["tool"]["value"] == "held" && rule["action"] == "deny")
    );
    let frozen = serde_json::to_value(&historical).unwrap();
    let owner = session.clone();
    let recovery =
        tokio::spawn(async move { owner.recover_with_external_tools(historical, vec![]).await });
    tokio::time::timeout(Duration::from_secs(3), journal.entered.notified())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), session.select_mode("alternate"))
        .await
        .expect("mode admission must not wait for restore Journal I/O")
        .unwrap();
    assert_eq!(session.state().selected_mode.as_deref(), Some("alternate"));
    journal.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(3), recovery)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(session.state().selected_mode.as_deref(), Some("alternate"));
    let policy = serde_json::to_value(session.permission_controller().snapshot().unwrap()).unwrap();
    assert_eq!(policy["config"]["mode"], "alternate");
    assert!(
        policy["config"]["provider_defaults"]
            .as_array()
            .unwrap()
            .iter()
            .any(|rule| rule["tool"]["value"] == "held" && rule["action"] == "ask")
    );
    assert!(
        !policy["config"]["provider_defaults"]
            .as_array()
            .unwrap()
            .iter()
            .any(|rule| rule["tool"]["value"] == "held" && rule["action"] == "deny")
    );
    assert_eq!(
        serde_json::to_value(journal.memory.latest().unwrap()).unwrap(),
        frozen,
        "live intent must not rewrite selected historical bytes"
    );
    session.close().await.unwrap();
}
