use crate::delegate::{NativeChildFactory, NativeDelegateConfig};
use crate::{Children, DeclarativePermission, UsageLedger, assemble};
use async_trait::async_trait;
use microcrab_agent_types::{AgentInfo, AgentMode, AgentRole, Variable};
use microcrab_auditor::{AuditBody, Auditor, Moment, VariableView};
use microcrab_host_local::{LocalHost, ShellLimits};
use microcrab_protocol::{Response, Usage};
use microcrab_session::runtime::{Runtime, tool::ToolContext};
use microcrab_session::{Client, Error, Limits, Session, SessionState};
use microcrab_transport::Content;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio_util::sync::CancellationToken;

struct Answer;
#[async_trait]
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
            content: "native child answer".into(),
            tool_calls: vec![],
            usage: Usage::default(),
        })
    }
}

fn child_config(
    root: &std::path::Path,
    client: Arc<dyn Client>,
    children: Children,
) -> NativeDelegateConfig {
    let parent = Runtime::new(vec![], Arc::new(DeclarativePermission)).unwrap();
    let host = Arc::new(LocalHost::new(root, ShellLimits::default()).unwrap());
    NativeDelegateConfig {
        children,
        aggregate: UsageLedger::open(None).unwrap(),
        client,
        host,
        wasm: vec![],
        external: vec![],
        catalog: vec![AgentInfo {
            name: "reviewer".into(),
            modes: vec![AgentMode {
                description: String::new(),
                prompt: String::new(),
                name: "review".into(),
                role: AgentRole::SubAgent,
                permissions: vec![],
            }],
            tools: vec![],
            variables: Variable::default(),
        }],
        permissions: parent.permission_controller(),
        limits: Limits::default(),
        directory: root.join("sessions"),
        disk: true,
        system: None,
        auditors: None,
        journals: None,
    }
}

#[tokio::test]
async fn delegated_prompt_snapshot_uses_child_agent_and_rejects_before_allocation() {
    use crate::delegate::ChildFactory;
    use microcrab_session::context::system::PromptConfig;
    let root = tempfile::tempdir().unwrap();
    let parent = Session::builder(
        Answer,
        Runtime::new(vec![], Arc::new(DeclarativePermission)).unwrap(),
    )
    .build()
    .unwrap();
    let mut config = child_config(root.path(), Arc::new(Answer), Children::default());
    config.catalog[0]
        .variables
        .inject("description", "selected child")
        .unwrap();
    let invalid = NativeChildFactory::new(config.clone()).prompt_config(PromptConfig {
        system_template: "{{ agent.required }}".into(),
        ..Default::default()
    });
    assert!(
        invalid
            .create(None, "review", None, "clean", parent.process_control())
            .await
            .is_err()
    );
    assert!(!root.path().join("sessions/subsessions").exists());
    let prompts = PromptConfig {
        system_template: "{{ agent.description }}|{{ client_prompt }}".into(),
        client_prompt: "  {{ literal }} ".into(),
        rename_template: "CHILD NAME {{ user_input }}".into(),
        ..Default::default()
    };
    let factory = NativeChildFactory::new(config).prompt_config(prompts.clone());
    let child = factory
        .create(None, "review", None, "clean", parent.process_control())
        .await
        .unwrap();
    assert_eq!(child.session.prompt_config(), &prompts);
    assert_eq!(
        child.session.context_snapshot().unwrap().system,
        "selected child|  {{ literal }} "
    );
    child.session.close().await.unwrap();
    parent.close().await.unwrap();
}

#[tokio::test]
async fn native_delegate_assembles_without_python_and_settles_before_close() {
    let directory = tempfile::tempdir().unwrap();
    let children = Children::default();
    let executor = native_test_delegate(child_config(
        directory.path(),
        Arc::new(Answer),
        children.clone(),
    ));
    let context = ToolContext::detached(
        SessionState::new(Limits::default()),
        CancellationToken::new(),
    );
    let receipt = executor
        .execute("review", "inspect", None, "clean", None, context)
        .await
        .unwrap();
    let receipt: serde_json::Value = serde_json::from_str(&receipt).unwrap();
    assert_eq!(receipt["outcome"]["content"], "native child answer");
    assert_eq!(receipt["subagent"], "reviewer");
    let id = receipt["child_id"].as_str().unwrap();
    let metadata = directory
        .path()
        .join("sessions/subsessions")
        .join(id)
        .join("creator.json");
    assert!(metadata.is_file());
    children.close().await.unwrap();
    let context = ToolContext::detached(
        SessionState::new(Limits::default()),
        CancellationToken::new(),
    );
    assert!(matches!(
        executor
            .execute("review", "late", None, "clean", None, context)
            .await,
        Err(Error::Closed)
    ));
}

struct Wait(Arc<tokio::sync::Notify>);
#[async_trait]
impl Client for Wait {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        cancellation: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.0.notify_one();
        cancellation.cancelled().await;
        Err(microcrab_transport::Error::Cancelled)
    }
}

#[tokio::test]
async fn native_delegate_turn_cancellation_still_acknowledges_child_cleanup() {
    let directory = tempfile::tempdir().unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let children = Children::default();
    let executor = native_test_delegate(child_config(
        directory.path(),
        Arc::new(Wait(entered.clone())),
        children.clone(),
    ));
    let cancellation = CancellationToken::new();
    let context = ToolContext::detached(SessionState::new(Limits::default()), cancellation.clone());
    let running = tokio::spawn(async move {
        executor
            .execute("review", "inspect", None, "clean", None, context)
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    cancellation.cancel();
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(5), running)
            .await
            .unwrap()
            .unwrap(),
        Err(Error::Cancelled)
    ));
    tokio::time::timeout(std::time::Duration::from_secs(5), children.close())
        .await
        .unwrap()
        .unwrap();
}

struct ObserveClose(Arc<AtomicUsize>);
impl Auditor for ObserveClose {
    fn is_enabled(&self) -> bool {
        true
    }
    fn publish(&self, moment: Moment, _: AuditBody) {
        if moment == Moment::SessionTerminated {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[tokio::test]
async fn failed_requested_mode_closes_the_started_session() {
    let closed = Arc::new(AtomicUsize::new(0));
    let runtime = Runtime::new(vec![], Arc::new(DeclarativePermission)).unwrap();
    let builder = Session::builder(Answer, runtime).auditor(ObserveClose(closed.clone()));
    assert!(
        assemble(builder, None, Some("missing".into()))
            .await
            .is_err()
    );
    assert_eq!(closed.load(Ordering::SeqCst), 1);
}

#[test]
fn select_child_uses_declared_roles_without_magic_caller_or_target_names() {
    use crate::delegate::select_child;
    let catalog = vec![AgentInfo {
        name: "execute".into(),
        modes: vec![
            AgentMode {
                description: String::new(),
                prompt: String::new(),
                name: "execute".into(),
                role: AgentRole::SubAgent,
                permissions: vec![],
            },
            AgentMode {
                description: String::new(),
                prompt: String::new(),
                name: "private-main".into(),
                role: AgentRole::MainAgent,
                permissions: vec![],
            },
            AgentMode {
                description: String::new(),
                prompt: String::new(),
                name: "plan".into(),
                role: AgentRole::Both,
                permissions: vec![],
            },
        ],
        tools: vec![],
        variables: Variable::default(),
    }];
    for caller in [None, Some("plan"), Some("general"), Some("opaque-caller")] {
        assert_eq!(
            select_child(&catalog, caller, "execute", None).unwrap().1,
            "execute"
        );
        assert_eq!(
            select_child(&catalog, caller, "execute", Some("plan"))
                .unwrap()
                .1,
            "plan"
        );
        assert!(matches!(
            select_child(&catalog, caller, "execute", Some("private-main")),
            Err(Error::Denied(_))
        ));
        assert!(matches!(
            select_child(&catalog, caller, "private-main", None),
            Err(Error::Denied(_))
        ));
        assert!(matches!(
            select_child(&catalog, caller, "missing", None),
            Err(Error::Denied(_))
        ));
    }
}

#[tokio::test]
async fn native_delegate_timeout_interrupts_child_and_returns_error() {
    let directory = tempfile::tempdir().unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let children = Children::default();
    let executor = native_test_delegate(child_config(
        directory.path(),
        Arc::new(Wait(entered.clone())),
        children.clone(),
    ));
    let context = ToolContext::detached(
        SessionState::new(Limits::default()),
        CancellationToken::new(),
    );
    let result = executor
        .execute("review", "inspect", None, "clean", Some(100), context)
        .await;
    assert!(matches!(result, Err(Error::ToolFailure(failure))
        if failure.kind == microcrab_session::runtime::execution::ToolFailureKind::Timeout
            && failure.code == "delegate_timeout"
            && failure.message == "delegate timed out after 100ms"
            && !failure.retryable));
    children.close().await.unwrap();
}

struct AssemblyResource(Arc<AtomicUsize>);
impl Drop for AssemblyResource {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct PendingAssembly {
    entered: Arc<tokio::sync::Notify>,
    released: Arc<AtomicUsize>,
}
#[async_trait]
impl crate::delegate::ChildFactory for PendingAssembly {
    async fn create(
        &self,
        _: Option<&str>,
        _: &str,
        _: Option<&str>,
        _: &str,
        _: Arc<dyn microcrab_session::execution::process::ProcessControl>,
    ) -> Result<crate::delegate::ChildSession, Error> {
        // Stand in for a factory that owns partial journal/fork resources while
        // waiting on an asynchronous callback. No live Session is published.
        let _resource = AssemblyResource(self.released.clone());
        self.entered.notify_one();
        std::future::pending().await
    }
}

#[async_trait]
impl crate::delegate::ChildAuditorFactory for PendingAssembly {
    async fn create(
        &self,
        _: &std::path::Path,
        _: &microcrab_storage::CreatorJournal,
    ) -> Result<Arc<dyn Auditor>, Error> {
        let _resource = AssemblyResource(self.released.clone());
        self.entered.notify_one();
        std::future::pending().await
    }
}

#[tokio::test]
async fn native_assembly_cancellation_releases_the_open_child_journal() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let directory = tempfile::tempdir().unwrap();
        let children = Children::default();
        let entered = Arc::new(tokio::sync::Notify::new());
        let released = Arc::new(AtomicUsize::new(0));
        let mut config = child_config(directory.path(), Arc::new(Answer), children.clone());
        config.auditors = Some(Arc::new(PendingAssembly {
            entered: entered.clone(),
            released: released.clone(),
        }));
        let executor = native_test_delegate(config);
        let cancellation = CancellationToken::new();
        let context =
            ToolContext::detached(SessionState::new(Limits::default()), cancellation.clone());
        let running = tokio::spawn(async move {
            executor
                .execute("review", "inspect", None, "clean", None, context)
                .await
        });
        entered.notified().await;
        cancellation.cancel();
        assert!(matches!(running.await.unwrap(), Err(Error::Cancelled)));
        children.close().await.unwrap();
        assert_eq!(released.load(Ordering::SeqCst), 1);
        let child_path = std::fs::read_dir(directory.path().join("sessions/subsessions"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let id = child_path.file_name().unwrap().to_str().unwrap().to_owned();
        // FileJournal::open requires an exclusive lock. A reopened journal proves
        // cancelling the later auditor factory released the partial child owner.
        let (journal, _) =
            microcrab_storage::CreatorJournal::open(child_path.to_str().unwrap(), id, true, false)
                .await
                .unwrap();
        microcrab_session::Journal::close(&journal).await.unwrap();
    })
    .await
    .expect("native partial assembly must release its journal before acknowledging cleanup");
}

async fn pending_assembly_settles(stop: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let entered = Arc::new(tokio::sync::Notify::new());
        let released = Arc::new(AtomicUsize::new(0));
        let children = Children::default();
        let executor = test_delegate_with_factory(
            children.clone(),
            Arc::new(PendingAssembly {
                entered: entered.clone(),
                released: released.clone(),
            }),
        );
        let cancellation = CancellationToken::new();
        let context =
            ToolContext::detached(SessionState::new(Limits::default()), cancellation.clone());
        let timeout = (stop == "timeout").then_some(100);
        let running = tokio::spawn(async move {
            executor
                .execute("review", "inspect", None, "clean", timeout, context)
                .await
        });
        entered.notified().await;
        assert_eq!(released.load(Ordering::SeqCst), 0);
        match stop {
            "turn" => cancellation.cancel(),
            "close" => children.close().await.unwrap(),
            "timeout" => {}
            _ => unreachable!("test selects a known stop condition"),
        }
        let result = running.await.unwrap();
        if stop == "timeout" {
            assert!(matches!(result, Err(Error::ToolFailure(failure))
                if failure.kind == microcrab_session::runtime::execution::ToolFailureKind::Timeout
                    && failure.code == "delegate_timeout"
                    && failure.message == "delegate timed out after 100ms"
                    && !failure.retryable));
        } else {
            assert!(matches!(result, Err(Error::Cancelled)));
        }
        // Completion acknowledges partial-resource destruction, so parent close
        // neither hangs nor races with a detached factory creating a late child.
        assert_eq!(released.load(Ordering::SeqCst), 1);
        children.close().await.unwrap();
    })
    .await
    .expect("pending assembly must release its resources and acknowledge cleanup");
}

#[tokio::test]
async fn delegate_timeout_includes_pending_assembly() {
    pending_assembly_settles("timeout").await;
}

#[tokio::test]
async fn delegate_turn_cancellation_drops_pending_assembly() {
    pending_assembly_settles("turn").await;
}

#[tokio::test]
async fn parent_close_cancels_pending_assembly_and_joins_cleanup() {
    pending_assembly_settles("close").await;
}

struct PendingLoad(Arc<tokio::sync::Notify>);
impl microcrab_session::state::reference::Reference for PendingLoad {
    type Error = std::io::Error;
    fn kind(&self) -> &str {
        "pending_load"
    }
    async fn load(&self) -> Result<(), Self::Error> {
        self.0.notify_one();
        std::future::pending().await
    }
    fn list(&self) -> Result<Vec<microcrab_session::state::reference::ReferenceInfo>, Self::Error> {
        Ok(vec![])
    }
    fn read(
        &self,
        _: &str,
    ) -> Result<Option<microcrab_session::state::reference::ReferenceContent>, Self::Error> {
        Ok(None)
    }
    fn change(&self) -> Result<Option<serde_json::Value>, Self::Error> {
        Ok(None)
    }
}
struct PendingStart {
    entered: Arc<tokio::sync::Notify>,
    closed: Arc<AtomicUsize>,
}
#[async_trait]
impl crate::delegate::ChildFactory for PendingStart {
    async fn create(
        &self,
        _: Option<&str>,
        _: &str,
        _: Option<&str>,
        _: &str,
        processes: Arc<dyn microcrab_session::execution::process::ProcessControl>,
    ) -> Result<crate::delegate::ChildSession, Error> {
        let auditor = Arc::new(ObserveClose(self.closed.clone()));
        let runtime = Runtime::new(vec![], Arc::new(DeclarativePermission))?;
        let session = Session::builder(Answer, runtime)
            .session_id("pending-start")
            .agent(test_agent())
            .is_main(false)
            .processes_parent(processes)
            .auditor(auditor.clone())
            .references(vec![microcrab_session::DynReference::new(PendingLoad(
                self.entered.clone(),
            ))?])
            .build()?;
        Ok(crate::delegate::ChildSession {
            id: "pending-start".into(),
            session,
            mode: "review".into(),
            ledger: Arc::new(UsageLedger::open(None)?),
            auditor,
        })
    }
}

#[tokio::test]
async fn delegate_cancellation_during_start_closes_the_published_child() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let children = Children::default();
        let entered = Arc::new(tokio::sync::Notify::new());
        let closed = Arc::new(AtomicUsize::new(0));
        let executor = test_delegate_with_factory(
            children.clone(),
            Arc::new(PendingStart {
                entered: entered.clone(),
                closed: closed.clone(),
            }),
        );
        let cancellation = CancellationToken::new();
        let context =
            ToolContext::detached(SessionState::new(Limits::default()), cancellation.clone());
        let running = tokio::spawn(async move {
            executor
                .execute("review", "inspect", None, "clean", None, context)
                .await
        });
        entered.notified().await;
        cancellation.cancel();
        assert!(matches!(running.await.unwrap(), Err(Error::Cancelled)));
        assert_eq!(closed.load(Ordering::SeqCst), 1);
        children.close().await.unwrap();
    })
    .await
    .expect("startup cancellation must close the child without awaiting its pending load");
}

struct QuestionCleanupAck {
    entered: tokio::sync::Notify,
    acknowledged: tokio::sync::Semaphore,
    completed: std::sync::atomic::AtomicBool,
}
struct GatedQuestionController {
    cancellation: CancellationToken,
    ack: Arc<QuestionCleanupAck>,
}
#[async_trait]
impl microcrab_session::runtime::execution::ExecutionController for GatedQuestionController {
    async fn cancel(&self, _: CancellationToken) -> Result<(), Error> {
        if self.ack.completed.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.cancellation.cancel();
        self.ack.entered.notify_one();
        self.ack
            .acknowledged
            .acquire()
            .await
            .map_err(|_| Error::Closed)?
            .forget();
        self.ack.completed.store(true, Ordering::SeqCst);
        Ok(())
    }
    async fn release(&self, token: CancellationToken) -> Result<(), Error> {
        self.cancel(token).await
    }
}
struct ChildTaskQuestion {
    answer: String,
    entered: Arc<tokio::sync::Notify>,
    released: Arc<tokio::sync::Semaphore>,
    deferred: bool,
    cleanup_ack: Option<Arc<QuestionCleanupAck>>,
}
#[async_trait]
impl microcrab_session::runtime::tool::Tool for ChildTaskQuestion {
    fn definition(&self) -> microcrab_protocol::ToolDefinition {
        microcrab_protocol::ToolDefinition {
            name: "child_question".into(),
            description: "Owned fixture interaction".into(),
            parameters: serde_json::json!({"type":"object"}),
        }
    }
    async fn execute(
        &self,
        _: serde_json::Value,
        context: ToolContext,
    ) -> Result<microcrab_session::runtime::execution::ToolOutput, Error> {
        use microcrab_session::runtime::execution::{
            CancellationController, ToolFailure, ToolFailureKind, ToolOutput,
        };
        self.entered.notify_one();
        if self.deferred {
            let cancellation = CancellationToken::new();
            let controller: Arc<dyn microcrab_session::runtime::execution::ExecutionController> =
                match &self.cleanup_ack {
                    Some(ack) => Arc::new(GatedQuestionController {
                        cancellation: cancellation.clone(),
                        ack: ack.clone(),
                    }),
                    None => Arc::new(CancellationController::new(cancellation.clone())),
                };
            let (writer, output) = context.open_output(Default::default(), controller)?;
            if self.cleanup_ack.is_some() {
                writer.append(None, &self.answer)?;
            }
            let released = self.released.clone();
            let answer = self.answer.clone();
            let prefilled = self.cleanup_ack.is_some();
            tokio::spawn(async move {
                let outcome = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => Err(ToolFailure { kind: ToolFailureKind::Cancelled,
                        code: "question_cancelled".into(), message: "Question cancelled".into(), retryable: false }),
                    permit = released.acquire() => match permit {
                        Ok(permit) => { permit.forget(); if !prefilled { writer.append(None, &answer).unwrap(); } Ok(()) },
                        Err(_) => Err(ToolFailure::failed("question_closed", "Question closed")),
                    },
                };
                writer.finish(outcome).unwrap();
            });
            return Ok(ToolOutput::Buffered(output));
        }
        let permit = tokio::select! {
            biased;
            _ = context.cancellation.cancelled() => return Err(Error::Cancelled),
            permit = self.released.acquire() => permit.map_err(|_| Error::Cancelled)?,
        };
        permit.forget();
        Ok(ToolOutput::text(self.answer.clone()))
    }
}
struct ChildAsking(AtomicUsize);
#[async_trait]
impl Client for ChildAsking {
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
            content: "child turn done".into(),
            tool_calls: if first {
                vec![microcrab_protocol::ToolCall {
                    provider_metadata: None,
                    id: "child-ask".into(),
                    name: "child_question".into(),
                    arguments: serde_json::json!({}),
                }]
            } else {
                vec![]
            },
            usage: Usage::default(),
        })
    }
}
#[tokio::test]
async fn delegate_retains_blocking_and_deferred_child_tasks_until_real_results_settle() {
    for deferred in [false, true] {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let directory = tempfile::tempdir().unwrap();
            let children = Children::default();
            let entered = Arc::new(tokio::sync::Notify::new());
            let released = Arc::new(tokio::sync::Semaphore::new(0));
            let question = Arc::new(ChildTaskQuestion {
                answer: "actual settled child answer".into(),
                entered: entered.clone(),
                released: released.clone(),
                deferred,
                cleanup_ack: None,
            });
            let parent = Runtime::new(
                vec![microcrab_session::runtime::ToolRegistration::External(
                    question.clone(),
                )],
                Arc::new(microcrab_session::runtime::tool::AllowTools::new([
                    "child_question",
                ])),
            )
            .unwrap();
            let client = Arc::new(ChildAsking(AtomicUsize::new(0)));
            let mut config = child_config(directory.path(), client.clone(), children.clone());
            config.permissions = parent.permission_controller();
            config.external = vec![question];
            let executor = native_test_delegate(config);
            let context = ToolContext::detached(
                SessionState::new(Limits::default()),
                CancellationToken::new(),
            );
            let worker = tokio::spawn(async move {
                executor
                    .execute("review", "ask", None, "clean", None, context)
                    .await
            });
            entered.notified().await;
            assert!(
                !worker.is_finished(),
                "child owner must retain actual pending interaction"
            );
            released.add_permits(1);
            let receipt: serde_json::Value =
                serde_json::from_str(&worker.await.unwrap().unwrap()).unwrap();
            assert_eq!(receipt["executions"][0]["status"], "succeeded");
            let row = &receipt["executions"][0];
            assert!(row.get("result").is_none());
            let child_id = receipt["child_id"].as_str().unwrap();
            assert_eq!(row["session_id"], child_id);
            assert_eq!(row["execution_id"], 1);
            if deferred {
                assert_eq!(row["output"]["bytes"], "actual settled child answer".len());
                assert_eq!(
                    row["output"]["end_cursor"].as_u64().unwrap()
                        - row["output"]["start_cursor"].as_u64().unwrap(),
                    "actual settled child answer".len() as u64
                );
                assert_eq!(row["output"]["closed"], true);
            } else {
                assert!(row.get("output").is_none());
            }
            let child_directory = directory.path().join("sessions/subsessions").join(child_id);
            let (journal, _) = microcrab_storage::CreatorJournal::open(
                child_directory.to_str().unwrap(),
                child_id.into(),
                true,
                true,
            )
            .await
            .unwrap();
            let recovered = journal.head_recover().await.unwrap().unwrap();
            assert_eq!(
                recovered_text(&recovered.state.executions[0]),
                "actual settled child answer"
            );
            microcrab_session::Journal::close(&journal).await.unwrap();
            assert_eq!(
                client.0.load(Ordering::SeqCst),
                2,
                "waiting for tasks never replays child model/effects"
            );
            children.close().await.unwrap();
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn delegate_large_child_result_stays_durable_with_metadata_only_bounded_receipt() {
    let directory = tempfile::tempdir().unwrap();
    let children = Children::default();
    let entered = Arc::new(tokio::sync::Notify::new());
    let released = Arc::new(tokio::sync::Semaphore::new(1));
    let question = Arc::new(ChildTaskQuestion {
        answer: "界".repeat(2000),
        entered,
        released,
        deferred: true,
        cleanup_ack: None,
    });
    let parent = Runtime::new(
        vec![microcrab_session::runtime::ToolRegistration::External(
            question.clone(),
        )],
        Arc::new(microcrab_session::runtime::tool::AllowTools::new([
            "child_question",
        ])),
    )
    .unwrap();
    let mut config = child_config(
        directory.path(),
        Arc::new(ChildAsking(AtomicUsize::new(0))),
        children.clone(),
    );
    config.permissions = parent.permission_controller();
    config.external = vec![question];
    let context = ToolContext::detached(
        SessionState::new(Limits {
            max_tool_output_bytes: 1024,
            ..Limits::default()
        }),
        CancellationToken::new(),
    );
    let output = native_test_delegate(config)
        .execute("review", "ask", None, "clean", None, context)
        .await
        .unwrap();
    assert!(output.len() <= 1024);
    let value: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert!(value["child_id"].is_string());
    assert_eq!(value["executions"][0]["status"], "succeeded");
    let row = &value["executions"][0];
    assert!(row.get("result").is_none());
    assert!(row.get("arguments").is_none());
    assert!(row.get("invocation_result").is_none());
    assert!(row.get("output_availability").is_none());
    assert!(row.get("arguments_available").is_none());
    assert!(row.get("result_available").is_none());
    let child_id = value["child_id"].as_str().unwrap();
    assert_eq!(row["session_id"], child_id);
    assert_eq!(row["execution_id"], 1);
    assert_eq!(row["output"]["bytes"], "界".repeat(2000).len());
    assert_eq!(
        row["output"]["end_cursor"].as_u64().unwrap()
            - row["output"]["start_cursor"].as_u64().unwrap(),
        "界".repeat(2000).len() as u64
    );
    assert!(row["output"].get("truncated").is_none());
    assert_eq!(row["output"]["closed"], true);
    let child_directory = directory.path().join("sessions/subsessions").join(child_id);
    let (journal, _) = microcrab_storage::CreatorJournal::open(
        child_directory.to_str().unwrap(),
        child_id.into(),
        true,
        true,
    )
    .await
    .unwrap();
    let recovered = journal.head_recover().await.unwrap().unwrap();
    assert_eq!(
        recovered.state.executions[0].arguments.as_deref(),
        Some("{}")
    );
    assert!(recovered.state.executions[0].invocation_result.is_some());
    assert_eq!(
        recovered_text(&recovered.state.executions[0]),
        "界".repeat(2000)
    );
    microcrab_session::Journal::close(&journal).await.unwrap();
    children.close().await.unwrap();
}

#[tokio::test]
async fn file_journal_checkpoint_after_travel_preserves_identity_redo_and_live_task_facts() {
    use microcrab_storage::{CreatorJournal, Travel};
    for deferred in [false, true] {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let directory = tempfile::tempdir().unwrap();
            let session_id = "0123456789abcdef";
            let (journal, _) = CreatorJournal::open(
                directory.path().to_str().unwrap(),
                session_id.into(),
                true,
                false,
            )
            .await
            .unwrap();
            let released = Arc::new(tokio::sync::Semaphore::new(if deferred { 0 } else { 1 }));
            let cleanup_ack = Arc::new(QuestionCleanupAck {
                entered:tokio::sync::Notify::new(), acknowledged:tokio::sync::Semaphore::new(0),
                completed:std::sync::atomic::AtomicBool::new(false),
            });
            let question = Arc::new(ChildTaskQuestion {
                answer: "actual live observation".into(),
                entered: Arc::new(tokio::sync::Notify::new()),
                released: released.clone(),
                deferred,
                cleanup_ack: deferred.then(|| cleanup_ack.clone()),
            });
            let runtime = Runtime::new(
                vec![microcrab_session::runtime::ToolRegistration::External(
                    question,
                )],
                Arc::new(microcrab_session::runtime::tool::AllowTools::new([
                    "child_question",
                ])),
            )
            .unwrap();
            let client = Arc::new(ChildAsking(AtomicUsize::new(0)));
            let session = Session::builder_arc(client.clone(), runtime)
                .session_id(session_id)
                .journal(journal.clone())
                .build()
                .unwrap();
            session.start().await.unwrap();
            let root_checkpoint = session.checkpoint().await.unwrap();
            let before = journal.cursor().await.unwrap();
            session.submit("one real effect").await.unwrap();
            let original_id = session.execution_snapshots()[0].execution_id;
            if deferred {
                let selected = journal.cursor().await.unwrap();
                let recover = journal.prepare_travel(Travel::UndoTo { checkpoint_id:before.checkpoint_id.clone() }).await.unwrap();
                assert_eq!(serde_json::to_value(&recover).unwrap(), serde_json::to_value(&root_checkpoint).unwrap());
                let recovering = session.clone();
                let mut replacement = tokio::spawn(async move { recovering.recover(recover).await });
                cleanup_ack.entered.notified().await;
                assert!(!cleanup_ack.completed.load(Ordering::SeqCst));
                assert_eq!(journal.cursor().await.unwrap(), selected, "cancellation request alone cannot publish Journal selection");
                assert!(tokio::time::timeout(std::time::Duration::from_millis(20), &mut replacement).await.is_err());
                cleanup_ack.acknowledged.add_permits(1);
                replacement.await.unwrap().unwrap();
                assert!(cleanup_ack.completed.load(Ordering::SeqCst));
                assert_eq!(journal.cursor().await.unwrap(), before);
                assert!(session.execution_snapshots().is_empty());
                let redo = journal.prepare_travel(Travel::Redo).await.unwrap();
                session.recover(redo).await.unwrap();
                assert_eq!(journal.cursor().await.unwrap(), selected, "acknowledged recovery preserves the exact redo tail");
                assert!(!session.executions().has_pending(), "historical pending facts cannot revive a cancelled producer");
            }
            session
                .compact("settled conversation", serde_json::json!({}))
                .await
                .unwrap();
            session.checkpoint().await.unwrap();
            let after = journal.cursor().await.unwrap();
            assert_eq!(
                recovered_text(
                    &journal
                        .head_recover()
                        .await
                        .unwrap()
                        .unwrap()
                        .state
                        .executions[0]
                ),
                "actual live observation"
            );
            let mut steps = 0;
            while journal.cursor().await.unwrap() != before {
                let recover = journal.prepare_travel(Travel::Undo).await.unwrap();
                session.recover(recover).await.unwrap();
                steps += 1;
                assert!(steps < 20);
            }
            for _ in 0..2 {
                let returned = session.checkpoint().await.unwrap();
                assert!(
                    returned.state.executions.is_empty(),
                    "checkpoint returns immutable selected bytes"
                );
                assert_eq!(
                    journal.cursor().await.unwrap(),
                    before,
                    "live observation overlay must not create a history branch"
                );
                assert_eq!(
                    session.execution_snapshots().len(),
                    0,
                    "recovery selects the exact historical execution observations"
                );
            }
            for _ in 0..steps {
                let recover = journal.prepare_travel(Travel::Redo).await.unwrap();
                session.recover(recover).await.unwrap();
            }
            assert_eq!(
                journal.cursor().await.unwrap(),
                after,
                "no-op checkpoint preserves redo tail"
            );
            let recover = journal
                .prepare_travel(Travel::To {
                    checkpoint_id: before.checkpoint_id.clone(),
                })
                .await
                .unwrap();
            session.recover(recover).await.unwrap();
            session.checkpoint().await.unwrap();
            assert_eq!(journal.cursor().await.unwrap(), before);
            if deferred {
                client.0.store(0, Ordering::SeqCst);
                session
                    .submit("a newly admitted effect after selected history")
                    .await
                    .unwrap();
                let fresh = session.execution_snapshots();
                assert_eq!(fresh.len(), 1);
                assert!(
                    fresh[0].execution_id.0 > original_id.0,
                    "history selection never reuses live execution identity"
                );
                let pending = journal.cursor().await.unwrap();
                let registry = session.executions();
                let mut changed = registry.subscribe();
                // The explicit submit above rearms automatic admission. Pause
                // again before this second terminal publication to isolate the
                // fresh-generation persistence assertion from a real Wake turn.
                session.interrupt();
                released.add_permits(1);
                while registry.has_pending() {
                    changed.changed().await.unwrap();
                }
                session.checkpoint().await.unwrap();
                assert_ne!(
                    journal.cursor().await.unwrap(),
                    pending,
                    "a real newly completed generation must persist"
                );
                assert_eq!(
                    recovered_text(
                        &journal
                            .head_recover()
                            .await
                            .unwrap()
                            .unwrap()
                            .state
                            .executions[0]
                    ),
                    "actual live observation"
                );
            } else {
                session
                    .state_handle()
                    .inject("changed", serde_json::json!(true))
                    .await
                    .unwrap();
                session.checkpoint().await.unwrap();
                assert_ne!(
                    journal.cursor().await.unwrap(),
                    before,
                    "custom State changes must persist"
                );
                let saved =
                    serde_json::to_value(journal.head_recover().await.unwrap().unwrap()).unwrap();
                assert_eq!(saved["state"]["variables"]["changed"], true);
                assert!(session.execution_snapshots().is_empty(),
                    "new custom State does not promote observations from discarded future history");
                assert!(saved["state"].get("executions").is_none_or(|rows|
                    rows.as_array().is_some_and(Vec::is_empty)),
                    "durable State either omits empty execution observations or stores the empty array");
                assert!(journal.head_recover().await.unwrap().unwrap().state.executions.is_empty());
            }
            session.close().await.unwrap();
        })
        .await
        .unwrap();
    }
}

fn recovered_text(row: &microcrab_session::runtime::execution::ExecutionSnapshot) -> String {
    let result = row.result.as_deref().unwrap();
    if row.output.is_some() {
        let registry = microcrab_session::runtime::execution::ExecutionRegistry::new(
            microcrab_session::Limits::default(),
        );
        registry.restore(std::slice::from_ref(row)).unwrap();
        registry
            .read(row.execution_id, 0, 16 * 1024 * 1024)
            .unwrap()
            .chunks
            .into_iter()
            .map(|chunk| chunk.content)
            .collect()
    } else {
        result.into()
    }
}

struct ExactChildDisplayFactory {
    seen: Arc<std::sync::Mutex<Vec<(String, std::path::PathBuf)>>>,
}
#[async_trait]
impl crate::delegate::ChildDisplayFactory for ExactChildDisplayFactory {
    async fn create(
        &self,
        directory: &std::path::Path,
        session_id: &str,
        journal: &microcrab_storage::CreatorJournal,
        auditor: Option<Arc<crate::audit::NativeAuditor>>,
    ) -> Result<Arc<dyn Client>, Error> {
        assert!(auditor.is_some());
        assert_eq!(directory.file_name().unwrap().to_str().unwrap(), session_id);
        // Factory receives the already-created exact child's Journal; no parent
        // client identity or body hash is used to select the archive namespace.
        let _ = journal.cursor().await;
        self.seen
            .lock()
            .unwrap()
            .push((session_id.into(), directory.into()));
        Ok(Arc::new(Answer))
    }
}
struct ExactChildNativeAuditorFactory;
#[async_trait]
impl crate::delegate::ChildAuditorFactory for ExactChildNativeAuditorFactory {
    async fn create_native(
        &self,
        directory: &std::path::Path,
        journal: &microcrab_storage::CreatorJournal,
    ) -> Result<Option<Arc<crate::audit::NativeAuditor>>, Error> {
        crate::audit::NativeAuditor::open(
            directory,
            journal.history_anchor(),
            crate::audit::RecorderOptions::default(),
        )
        .await
        .map(Some)
        .map_err(|error| Error::Audit(error.to_string().into()))
    }
    async fn create(
        &self,
        _: &std::path::Path,
        _: &microcrab_storage::CreatorJournal,
    ) -> Result<Arc<dyn Auditor>, Error> {
        panic!("typed child auditor must bypass erased factory creation");
    }
}
struct ParentClientMustNotBeUsed;
#[async_trait]
impl Client for ParentClientMustNotBeUsed {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        panic!("child used parent-configured provider client");
    }
}
#[tokio::test]
async fn child_display_factory_uses_actual_child_identity_and_scoped_client() {
    let directory = tempfile::tempdir().unwrap();
    let children = Children::default();
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut config = child_config(
        directory.path(),
        Arc::new(ParentClientMustNotBeUsed),
        children.clone(),
    );
    config.auditors = Some(Arc::new(ExactChildNativeAuditorFactory));
    let executor = native_test_delegate_with(config, |factory| {
        factory.with_display_factory(Arc::new(ExactChildDisplayFactory { seen: seen.clone() }))
    });
    let context = ToolContext::detached(
        SessionState::new(Limits::default()),
        CancellationToken::new(),
    );
    let receipt = executor
        .execute("review", "inspect", None, "clean", None, context)
        .await
        .unwrap();
    let receipt: serde_json::Value = serde_json::from_str(&receipt).unwrap();
    assert_eq!(receipt["outcome"]["content"], "native child answer");
    let child_id = receipt["child_id"].as_str().unwrap();
    assert_eq!(
        seen.lock().unwrap().as_slice(),
        &[(
            child_id.to_owned(),
            directory
                .path()
                .join("sessions/subsessions")
                .join(child_id)
                .canonicalize()
                .unwrap()
        )]
    );
    children.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
struct FailedChildDisplayState {
    id: String,
    directory: std::path::PathBuf,
    journal: microcrab_storage::CreatorJournal,
    source: microcrab_storage::display::DisplaySource,
    recorder: Arc<crate::display::DisplayRecorder>,
    call_id: String,
    finalized: bool,
}
#[cfg(feature = "storage-file")]
struct FailedChildDisplayFactory(Arc<tokio::sync::Mutex<Option<FailedChildDisplayState>>>);
#[cfg(feature = "storage-file")]
struct FailedChildRecordedClient {
    recorder: Arc<crate::display::DisplayRecorder>,
    call_id: String,
}
#[cfg(feature = "storage-file")]
#[async_trait]
impl Client for FailedChildRecordedClient {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        use microcrab_transport::{ModelObservation, ModelRecording, ObservationState};
        assert!(
            content.subagent.is_none(),
            "actual child is not an internal helper"
        );
        for state in [
            ObservationState::Started,
            ObservationState::ReasoningDelta {
                text: "failed child display survives restart".into(),
            },
            ObservationState::Invalidated {
                reason: "provider_call_failed".into(),
            },
        ] {
            self.recorder
                .record(ModelObservation {
                    call_id: self.call_id.clone(),
                    epoch: content.epoch,
                    turn: content.turn_id,
                    round: content.round,
                    subagent: None,
                    state,
                })
                .await
                .unwrap();
        }
        Err(microcrab_transport::Error::Transport)
    }
}
#[cfg(feature = "storage-file")]
#[async_trait]
impl crate::delegate::ChildDisplayFactory for FailedChildDisplayFactory {
    async fn create(
        &self,
        directory: &std::path::Path,
        id: &str,
        journal: &microcrab_storage::CreatorJournal,
        _: Option<Arc<crate::audit::NativeAuditor>>,
    ) -> Result<Arc<dyn Client>, Error> {
        use microcrab_storage::display::{DisplayScope, DisplaySource};
        let source = DisplaySource::File {
            directory: directory.join("display"),
            scope: DisplayScope::new("test-owner", "test-root", id),
        };
        let actual_child = id.to_owned();
        let recorder = crate::display::DisplayRecorder::open(
            source.clone(),
            Arc::new(move |_| serde_json::json!({"child_session_id": actual_child})),
            Arc::new(|_| {}),
            Arc::new(|error| panic!("{error}")),
        )
        .await?;
        let call_id = uuid::Uuid::new_v4().to_string();
        *self.0.lock().await = Some(FailedChildDisplayState {
            id: id.into(),
            directory: directory.into(),
            journal: journal.clone(),
            source,
            recorder: recorder.clone(),
            call_id: call_id.clone(),
            finalized: false,
        });
        Ok(Arc::new(FailedChildRecordedClient { recorder, call_id }))
    }
    async fn finalize(&self, id: &str, _: &Session, close_succeeded: bool) -> Result<(), Error> {
        let mut slot = self.0.lock().await;
        let state = slot.as_mut().unwrap();
        assert_eq!(id, state.id);
        assert!(
            close_succeeded,
            "failed model request still has an acknowledged close"
        );
        let cursor = state.journal.cursor().await.unwrap();
        state.recorder.append(state.call_id.clone(), serde_json::json!({"phase":"admission_binding","child_session_id":id,"selected_checkpoint":cursor,"admitted":false})).await.map_err(|error| Error::Audit(error.into()))?;
        state.recorder.close().await?;
        state.finalized = true;
        Ok(())
    }
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn failed_child_finalization_preserves_exact_attempt_and_checkpoint_on_restart() {
    let directory = tempfile::tempdir().unwrap();
    let children = Children::default();
    let state = Arc::new(tokio::sync::Mutex::new(None));
    let executor = native_test_delegate_with(
        child_config(
            directory.path(),
            Arc::new(ParentClientMustNotBeUsed),
            children.clone(),
        ),
        |factory| factory.with_display_factory(Arc::new(FailedChildDisplayFactory(state.clone()))),
    );
    let context = ToolContext::detached(
        SessionState::new(Limits::default()),
        CancellationToken::new(),
    );
    assert!(
        executor
            .execute("review", "inspect", None, "clean", None, context)
            .await
            .is_err()
    );
    children.close().await.unwrap();
    let state = state.lock().await.take().unwrap();
    assert!(state.finalized);
    let final_cursor = state.journal.cursor().await.unwrap();
    let (reopened, recover) = microcrab_storage::CreatorJournal::open(
        state.directory.to_str().unwrap(),
        state.id.clone(),
        true,
        true,
    )
    .await
    .unwrap();
    assert!(recover.is_some());
    assert_eq!(reopened.cursor().await.unwrap(), final_cursor);
    let page = tokio::task::spawn_blocking(move || state.source.query(0, None, 32))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(page.records.len(), 4);
    assert!(
        page.records
            .iter()
            .all(|record| record.attempt_id == state.call_id)
    );
    assert_eq!(
        page.records[1].event["text"],
        "failed child display survives restart"
    );
    assert_eq!(
        page.records[3].event["selected_checkpoint"],
        serde_json::to_value(final_cursor).unwrap()
    );
    assert_eq!(page.records[3].event["child_session_id"], state.id);
    assert_eq!(page.records[3].event["admitted"], false);
}

// Migration fixture drives the real parent Session and its Buffered output. It
// intentionally has no alternate production executor or child lifecycle loop.
struct DelegateCommand(std::sync::Mutex<Option<serde_json::Value>>);
#[async_trait]
impl Client for DelegateCommand {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let call = self.0.lock().unwrap().take();
        Ok(Response {
            provider_metadata: None,
            content: "parent settled".into(),
            tool_calls: call
                .into_iter()
                .map(|arguments| microcrab_protocol::ToolCall {
                    provider_metadata: None,
                    id: "fixture-delegate".into(),
                    name: "delegate".into(),
                    arguments,
                })
                .collect(),
            usage: Usage::default(),
        })
    }
}
struct TestDelegate {
    parent: Session,
    command: Arc<DelegateCommand>,
}
fn test_agent() -> AgentInfo {
    AgentInfo {
        name: "reviewer".into(),
        modes: vec![AgentMode {
            name: "review".into(),
            description: String::new(),
            prompt: String::new(),
            role: AgentRole::SubAgent,
            permissions: vec![],
        }],
        tools: vec![],
        variables: Variable::default(),
    }
}
fn test_parent_runtime(external: Vec<Arc<dyn microcrab_session::runtime::tool::Tool>>) -> Runtime {
    use microcrab_session::runtime::{BuiltinTool, ToolRegistration};
    let mut names = vec!["delegate".to_owned()];
    names.extend(external.iter().map(|tool| tool.definition().name));
    let mut tools = vec![ToolRegistration::Builtin(BuiltinTool::Delegate)];
    tools.extend(external.into_iter().map(ToolRegistration::External));
    Runtime::new(
        tools,
        Arc::new(microcrab_session::runtime::tool::AllowTools::new(names)),
    )
    .unwrap()
}
fn native_test_delegate(config: NativeDelegateConfig) -> TestDelegate {
    native_test_delegate_with(config, |factory| factory)
}
fn native_test_delegate_with(
    mut config: NativeDelegateConfig,
    finish: impl FnOnce(NativeChildFactory) -> NativeChildFactory,
) -> TestDelegate {
    let runtime = test_parent_runtime(config.external.clone());
    config.permissions = runtime.permission_controller();
    let host = config.host.clone();
    let catalog = config.catalog.clone();
    let limits = config.limits.clone();
    let children = config.children.clone();
    let executor = crate::delegate::OwnedDelegateExecutor::new(
        children,
        Arc::new(finish(NativeChildFactory::new(config))),
    );
    let command = Arc::new(DelegateCommand(std::sync::Mutex::new(None)));
    let parent = Session::builder_arc(
        command.clone(),
        runtime.with_delegate_executor(Arc::new(executor)),
    )
    .catalog(catalog)
    .host(host)
    .limits(limits)
    .build()
    .unwrap();
    TestDelegate { parent, command }
}
fn test_delegate_with_factory(
    children: Children,
    factory: Arc<dyn crate::delegate::ChildFactory>,
) -> TestDelegate {
    let runtime = test_parent_runtime(vec![]).with_delegate_executor(Arc::new(
        crate::delegate::OwnedDelegateExecutor::new(children, factory),
    ));
    let command = Arc::new(DelegateCommand(std::sync::Mutex::new(None)));
    let parent = Session::builder_arc(command.clone(), runtime)
        .catalog(vec![test_agent()])
        .build()
        .unwrap();
    TestDelegate { parent, command }
}
impl TestDelegate {
    async fn execute(
        &self,
        target: &str,
        input: &str,
        mode: Option<&str>,
        context_mode: &str,
        timeout_ms: Option<u64>,
        invocation: ToolContext,
    ) -> Result<String, Error> {
        let mut args =
            serde_json::json!({"target":target,"input":input,"context_mode":context_mode});
        if let Some(mode) = mode {
            args["mode"] = mode.into();
        }
        if let Some(timeout) = timeout_ms {
            args["timeout_ms"] = timeout.into();
        }
        *self.command.0.lock().unwrap() = Some(args);
        self.parent.submit("delegate fixture").await?;
        let registry = self.parent.executions();
        let id = registry
            .snapshot()
            .into_iter()
            .find(|row| row.tool == "delegate")
            .ok_or_else(|| Error::tool("fixture delegate was not admitted"))?
            .execution_id;
        let mut changed = registry.subscribe();
        let outcome = async {
            loop {
                let row = registry.get(id)?;
                let page = self.parent.read_execution_output(id, 0, 16 * 1024 * 1024)?;
                for chunk in page.chunks {
                    for line in chunk.content.lines() {
                        let Ok(mut receipt) = serde_json::from_str::<serde_json::Value>(line)
                        else {
                            continue;
                        };
                        if receipt.get("turn_error").is_some() {
                            return Err(Error::Tool(receipt["turn_error"].to_string().into()));
                        }
                        if receipt.get("outcome").is_some() {
                            let child_id = receipt["child_id"].as_str().unwrap().to_owned();
                            let target = receipt["subagent"].as_str().unwrap().to_owned();
                            let child = self.parent.inspect_child(&child_id, &target)?;
                            let tasks = child.executions();
                            let mut task_changed = tasks.subscribe();
                            while tasks.has_pending() {
                                task_changed.changed().await.map_err(|_| Error::Closed)?;
                            }
                            receipt["executions"] = serde_json::to_value(
                                child
                                    .execution_snapshots()
                                    .into_iter()
                                    .map(|mut row| {
                                        row.output_availability = None;
                                        row.arguments_available = None;
                                        row.result_available = None;
                                        row
                                    })
                                    .collect::<Vec<_>>(),
                            )?;
                            self.parent.close_child(&child_id, &target).await?;
                            return Ok(receipt.to_string());
                        }
                    }
                }
                if !row.status.is_pending() {
                    if row.status
                        == microcrab_session::runtime::execution::ExecutionStatus::Cancelled
                    {
                        return Err(Error::Cancelled);
                    }
                    if let Some(failure) = row.failure {
                        return Err(Error::ToolFailure(failure));
                    }
                    return Err(Error::Tool(
                        row.diagnostic
                            .map(|error| error.summary().to_owned())
                            .unwrap_or_else(|| "delegate ended without reply".into())
                            .into(),
                    ));
                }
                changed.changed().await.map_err(|_| Error::Closed)?;
            }
        };
        let result = tokio::select! {biased;_=invocation.cancellation.cancelled()=>Err(Error::Cancelled),result=outcome=>result};
        self.parent.close().await?;
        result
    }
}
