use super::super::*;
use microcrab_session::runtime::execution::{
    CancellationController, OutputOptions, OutputWriter, ToolOutput,
};
use microcrab_session::runtime::tool::{AllowTools, ToolContext};
use microcrab_session::{
    Client, VariableView,
    context::Content,
    protocol::{Response, ToolCall, Usage},
};
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

struct Script {
    first: Mutex<bool>,
    child: bool,
}
#[async_trait]
impl Client for Script {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let mut first = self.first.lock().unwrap();
        let calls = if std::mem::take(&mut *first) {
            vec![ToolCall {
                id: if self.child { "producer" } else { "delegate" }.into(),
                name: if self.child { "source" } else { "delegate" }.into(),
                arguments: if self.child {
                    json!({})
                } else {
                    json!({"target":"review","input":"work"})
                },
                provider_metadata: None,
            }]
        } else {
            vec![]
        };
        Ok(Response {
            provider_metadata: None,
            content: if self.child {
                "committed child answer"
            } else {
                "parent answer"
            }
            .into(),
            tool_calls: calls,
            usage: Usage::default(),
        })
    }
}
struct Source(mpsc::UnboundedSender<OutputWriter>);
#[async_trait]
impl Tool for Source {
    fn definition(&self) -> microcrab_session::protocol::ToolDefinition {
        microcrab_session::protocol::ToolDefinition {
            name: "source".into(),
            description: "source fixture".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn execute(
        &self,
        _: serde_json::Value,
        context: ToolContext,
    ) -> Result<ToolOutput, Error> {
        let (writer, output) = context.open_output(
            OutputOptions::default(),
            Arc::new(CancellationController::new(context.cancellation.clone())),
        )?;
        self.0.send(writer).map_err(|_| Error::Closed)?;
        Ok(ToolOutput::Buffered(output))
    }
}
struct Factory {
    child: mpsc::UnboundedSender<Session>,
    writer: mpsc::UnboundedSender<OutputWriter>,
}
fn agent() -> AgentInfo {
    AgentInfo {
        name: "review".into(),
        modes: vec![microcrab_agent_types::AgentMode {
            description: String::new(),
            prompt: String::new(),
            name: "review".into(),
            role: microcrab_agent_types::AgentRole::Both,
            permissions: vec![],
        }],
        tools: vec![],
        variables: Default::default(),
    }
}
#[async_trait]
impl ChildFactory for Factory {
    async fn create(
        &self,
        _: Option<&str>,
        _: &str,
        _: Option<&str>,
        _: &str,
        processes: Arc<dyn ProcessControl>,
    ) -> Result<ChildSession, Error> {
        let runtime = Runtime::new(
            vec![ToolRegistration::External(Arc::new(Source(
                self.writer.clone(),
            )))],
            Arc::new(AllowTools::new(["source"])),
        )?;
        let session = Session::builder(
            Script {
                first: Mutex::new(true),
                child: true,
            },
            runtime,
        )
        .session_id("child-fixture")
        .agent(agent())
        .is_main(false)
        .processes_parent(processes)
        .build()?;
        self.child
            .send(session.clone())
            .map_err(|_| Error::Closed)?;
        Ok(ChildSession {
            id: "child-fixture".into(),
            session,
            mode: "review".into(),
            ledger: Arc::new(UsageLedger::open(None)?),
            auditor: Arc::new(NoopAuditor),
        })
    }
}

#[tokio::test]
async fn explicit_delegate_cancel_and_parent_close_join_child_cleanup() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        for explicit_cancel in [true, false] {
            let (children_tx, mut children_rx) = mpsc::unbounded_channel();
            let (writers_tx, mut writers_rx) = mpsc::unbounded_channel();
            let children = Children::default();
            let executor = Arc::new(OwnedDelegateExecutor::new(
                children.clone(),
                Arc::new(Factory {
                    child: children_tx,
                    writer: writers_tx.clone(),
                }),
            ));
            let runtime = Runtime::new(
                vec![
                    ToolRegistration::Builtin(BuiltinTool::Delegate),
                    ToolRegistration::External(Arc::new(Source(writers_tx.clone()))),
                ],
                Arc::new(AllowTools::new(["delegate", "source"])),
            )
            .unwrap()
            .with_delegate_executor(executor);
            let parent = Session::builder(
                Script {
                    first: Mutex::new(true),
                    child: false,
                },
                runtime,
            )
            .session_id("cancel-parent")
            .catalog(vec![agent()])
            .build()
            .unwrap();
            parent.submit("delegate").await.unwrap();
            let child = children_rx.recv().await.unwrap();
            let writer = writers_rx.recv().await.unwrap();
            let id = parent
                .execution_snapshots()
                .into_iter()
                .find(|row| row.tool == "delegate")
                .unwrap()
                .execution_id;
            if explicit_cancel {
                parent.cancel_execution(id).await.unwrap();
            } else {
                parent.close().await.unwrap();
            }
            assert!(
                !child.executions().has_pending(),
                "cancel acknowledgement includes child execution cleanup"
            );
            assert!(
                writer
                    .append(None, "must not survive explicit cancellation")
                    .is_err()
            );
            let registry = parent.executions();
            let mut changed = registry.subscribe();
            while registry.get(id).unwrap().status.is_pending() {
                changed.changed().await.unwrap();
            }
            if explicit_cancel {
                assert_eq!(
                    registry.get(id).unwrap().status,
                    microcrab_session::runtime::execution::ExecutionStatus::Cancelled
                );
            }
            parent.close().await.unwrap();
            children.close().await.unwrap();
        }
    })
    .await
    .expect("explicit cancellation must acknowledge retained child cleanup");
}

#[tokio::test]
async fn buffered_delegate_keeps_one_parent_identity_and_forwards_retired_original_writer() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (children_tx, mut children_rx) = mpsc::unbounded_channel();
        let (writers_tx, mut writers_rx) = mpsc::unbounded_channel();
        let children = Children::default();
        let executor = Arc::new(OwnedDelegateExecutor::new(
            children.clone(),
            Arc::new(Factory {
                child: children_tx,
                writer: writers_tx.clone(),
            }),
        ));
        let runtime = Runtime::new(
            vec![
                ToolRegistration::Builtin(BuiltinTool::Delegate),
                ToolRegistration::Builtin(BuiltinTool::Job),
                ToolRegistration::External(Arc::new(Source(writers_tx.clone()))),
            ],
            Arc::new(AllowTools::new(["delegate", "job", "source"])),
        )
        .unwrap()
        .with_delegate_executor(executor);
        let parent = Session::builder(
            Script {
                first: Mutex::new(true),
                child: false,
            },
            runtime,
        )
        .session_id("parent-fixture")
        .catalog(vec![agent()])
        .build()
        .unwrap();
        parent.submit("delegate").await.unwrap();
        let child = children_rx.recv().await.unwrap();
        let writer = writers_rx.recv().await.unwrap();
        let delegates: Vec<_> = parent
            .execution_snapshots()
            .into_iter()
            .filter(|row| row.tool == "delegate")
            .collect();
        assert_eq!(delegates.len(), 1);
        let id = delegates[0].execution_id;
        let binding = delegates[0]
            .delegate_child
            .as_ref()
            .expect("actual assembled child is bound before its producer runs");
        assert_eq!(binding.session_id, "child-fixture");
        assert_eq!(binding.target, "review");
        let receipt: serde_json::Value =
            serde_json::from_str(&parent.executions().receipt(id, false).unwrap()).unwrap();
        assert_eq!(receipt["delegate_child"]["session_id"], "child-fixture");
        assert_eq!(receipt["delegate_child"]["target"], "review");
        assert!(
            delegates[0].status.is_pending(),
            "parent returned before the child producer settles"
        );
        // Ordinary turn interrupt must not revoke a transferred Buffered owner.
        parent.interrupt();
        let mut activity = child.subscribe_activity();
        while activity.borrow().outcome_sequence == 0 {
            activity.changed().await.unwrap();
        }
        child.retire_conversation().await.unwrap();
        writer
            .append(None, "after-retirement-original-writer")
            .unwrap();
        writer.finish(Ok(())).unwrap();
        let registry = parent.executions();
        let mut changes = registry.subscribe();
        while registry.get(id).unwrap().status.is_pending() {
            changes.changed().await.unwrap();
        }
        let output = parent.read_execution_output(id, 0, 16384).unwrap();
        let text: String = output
            .chunks
            .into_iter()
            .map(|chunk| chunk.content)
            .collect();
        assert!(text.contains("committed child answer"));
        assert!(text.contains("after-retirement-original-writer"));
        assert_eq!(
            parent
                .execution_snapshots()
                .into_iter()
                .filter(|row| row.tool == "delegate")
                .count(),
            1
        );
        // Forwarding uses the parent buffer; foreign child job handles remain denied.
        let mut context = ToolContext::detached(parent.state(), CancellationToken::new());
        context.session_id = "parent-fixture".into();
        let job = microcrab_session::execution::job::Job;
        assert!(
            job.execute(json!({"id":"child-fixture/1","action":"status"}), context)
                .await
                .is_err()
        );
        parent.close().await.unwrap();
        children.close().await.unwrap();
    })
    .await
    .expect("background delegate ownership must settle");
}

// Child assembly can yield through filesystem/audit setup while the main Session
// advances its catalog epoch. The explicit issuer argument must survive that wait.
struct EpochGateFactory {
    entered: mpsc::UnboundedSender<u64>,
    resumed: mpsc::UnboundedSender<u64>,
    proceed: tokio::sync::Semaphore,
}
#[async_trait]
impl ChildFactory for EpochGateFactory {
    async fn create(
        &self,
        _: Option<&str>,
        _: &str,
        _: Option<&str>,
        _: &str,
        _: Arc<dyn ProcessControl>,
    ) -> Result<ChildSession, Error> {
        Err(Error::tool(
            "background assembly used the unpinned factory path",
        ))
    }
    async fn create_for_epoch(
        &self,
        _: Option<&str>,
        _: &str,
        _: Option<&str>,
        _: &str,
        _: Arc<dyn ProcessControl>,
        issuer_epoch: u64,
    ) -> Result<ChildSession, Error> {
        self.entered.send(issuer_epoch).map_err(|_| Error::Closed)?;
        self.proceed
            .acquire()
            .await
            .map_err(|_| Error::Closed)?
            .forget();
        self.resumed.send(issuer_epoch).map_err(|_| Error::Closed)?;
        // No child was published; deterministic assembly refusal ends this proof
        // without starting unrelated child producers or acquiring filesystem state.
        Err(Error::tool(
            "controlled assembly refusal after epoch witness",
        ))
    }
}
#[tokio::test]
async fn background_child_assembly_keeps_issuer_epoch_across_later_main_epoch() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
        let (resumed_tx, mut resumed_rx) = mpsc::unbounded_channel();
        let factory = Arc::new(EpochGateFactory {
            entered: entered_tx,
            resumed: resumed_tx,
            proceed: tokio::sync::Semaphore::new(0),
        });
        let children = Children::default();
        let executor = Arc::new(OwnedDelegateExecutor::new(
            children.clone(),
            factory.clone(),
        ));
        let runtime = Runtime::new(
            vec![ToolRegistration::Builtin(BuiltinTool::Delegate)],
            Arc::new(AllowTools::new(["delegate", "source"])),
        )
        .unwrap()
        .with_delegate_executor(executor);
        let parent = Session::builder(
            Script {
                first: Mutex::new(true),
                child: false,
            },
            runtime,
        )
        .session_id("epoch-pinned-background")
        .catalog(vec![agent()])
        .build()
        .unwrap();
        parent
            .submit("delegate before catalog change")
            .await
            .unwrap();
        let issuer_epoch = entered_rx.recv().await.unwrap();
        assert_eq!(issuer_epoch, parent.state().epoch_id);
        let (writers_tx, _writers_rx) = mpsc::unbounded_channel();
        parent
            .replace_external_tools(vec![Arc::new(Source(writers_tx))])
            .await
            .unwrap();
        parent
            .submit("admit a later main catalog epoch")
            .await
            .unwrap();
        assert!(parent.state().epoch_id > issuer_epoch);
        factory.proceed.add_permits(1);
        assert_eq!(resumed_rx.recv().await.unwrap(), issuer_epoch);
        let registry = parent.executions();
        let mut changes = registry.subscribe();
        while registry
            .snapshot()
            .iter()
            .any(|row| row.tool == "delegate" && row.status.is_pending())
        {
            changes.changed().await.unwrap();
        }
        parent.close().await.unwrap();
        children.close().await.unwrap();
    })
    .await
    .expect("background epoch witness and controlled assembly cleanup must settle");
}
