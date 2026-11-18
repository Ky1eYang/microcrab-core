use super::*;
use microcrab_session::{
    VariableView,
    context::Content,
    protocol::{Response, ToolCall, Usage},
};
use serde_json::json;
use std::sync::Mutex;
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
            content: "child answer".into(),
            tool_calls: vec![],
            provider_metadata: None,
            usage: Usage::default(),
        })
    }
}
struct DelegateOnce(Mutex<bool>);
#[async_trait]
impl Client for DelegateOnce {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let first = std::mem::take(&mut *self.0.lock().unwrap());
        Ok(Response {
            content: "parent answer".into(),
            tool_calls: if first {
                vec![ToolCall {
                    id: "delegate".into(),
                    name: "delegate".into(),
                    arguments: json!({"target":"worker","mode":"both","input":"focused task"}),
                    provider_metadata: None,
                }]
            } else {
                vec![]
            },
            provider_metadata: None,
            usage: Usage::default(),
        })
    }
}
fn agent() -> AgentInfo {
    serde_json::from_value(
        json!({"name":"worker","tools":[],"variables":{},"modes":[{"name":"both","role":"both"}]}),
    )
    .unwrap()
}
fn make_factory(
    root: &Path,
    host: Arc<dyn Host>,
    runtime: &Runtime,
    children: Children,
) -> Arc<NativeChildFactory> {
    Arc::new(NativeChildFactory::new(NativeDelegateConfig {
        children,
        aggregate: UsageLedger::open(None).unwrap(),
        client: Arc::new(Answer),
        host,
        wasm: vec![],
        external: vec![],
        catalog: vec![agent()],
        permissions: runtime.permission_controller(),
        limits: Limits::default(),
        directory: root.to_owned(),
        disk: true,
        system: None,
        auditors: None,
        journals: None,
    }))
}
#[tokio::test]
async fn native_factory_restores_exact_selected_child_and_both_input_after_parent_restart() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let root = tempfile::tempdir().unwrap();
        let host: Arc<dyn Host> = Arc::new(
            microcrab_host_local::LocalHost::new(root.path(), Default::default()).unwrap(),
        );
        let children = Children::default();
        let runtime = Runtime::new(
            vec![ToolRegistration::Builtin(BuiltinTool::Delegate)],
            Arc::new(microcrab_session::runtime::tool::AllowTools::new([
                "delegate",
            ])),
        )
        .unwrap();
        let factory = make_factory(root.path(), host.clone(), &runtime, children.clone());
        let parent_journal=microcrab_session::runtime::journal::MemoryJournal::new();
        let parent = Session::builder(
            DelegateOnce(Mutex::new(true)),
            runtime.with_delegate_executor(Arc::new(OwnedDelegateExecutor::new(
                children.clone(),
                factory.clone(),
            ))),
        )
        .session_id("root-owner")
        .journal(parent_journal.clone())
        .host(host.clone())
        .catalog(vec![agent()])
        .build()
        .unwrap();
        parent.submit("start child").await.unwrap();
        let mut registry_changes = parent.executions().subscribe();
        let (child_id, child) = loop {
            let row = parent
                .execution_snapshots()
                .into_iter()
                .find(|row| row.tool == "delegate")
                .expect("delegate execution admitted");
            if let Some(binding) = &row.delegate_child
                && let Ok(child) = parent.inspect_child(&binding.session_id, "worker")
            {
                break (binding.session_id.clone(), child);
            }
            assert!(
                row.status.is_pending(),
                "delegate settled before child publication: {:?}",
                row.failure
            );
            registry_changes.changed().await.unwrap();
        };
        let mut activity = child.subscribe_activity();
        while activity.borrow().outcome_sequence < 1 {
            activity.changed().await.unwrap();
        }
        let old_identity = child.communication_identity().unwrap();
        children.close().await.unwrap();
        parent.close().await.unwrap();
        let recover=parent_journal.latest().unwrap();
        assert!(recover.state.communication.as_ref().unwrap().correspondents.iter().any(|row|row.agent_id==old_identity.agent_id),"normal child-first shutdown retains exact persistent lineage in the final parent checkpoint");
        assert!(parent.state_view().unwrap()["correspondents"].as_array().unwrap().is_empty(),"closed endpoints do not remain advertised as live addresses");
        drop(child);
        drop(parent);
        drop(factory);
        let children = Children::default();
        let runtime = Runtime::new(
            vec![ToolRegistration::Builtin(BuiltinTool::Delegate)],
            Arc::new(microcrab_session::runtime::tool::AllowTools::new([
                "delegate",
            ])),
        )
        .unwrap();
        let restore_gate = Arc::new(RestoreGate {
            entered: Default::default(),
            release: tokio::sync::Semaphore::new(0),
        });
        let restored_factory = Arc::new(
            make_factory(root.path(), host.clone(), &runtime, children.clone())
                .as_ref()
                .clone()
                .with_child_configuration(restore_gate.clone()),
        );
        let parent = Session::restore(
            Session::builder(
                Answer,
                runtime.with_delegate_executor(Arc::new(OwnedDelegateExecutor::new(
                    children.clone(),
                    restored_factory.clone(),
                ))),
            )
            .session_id("root-owner")
            .host(host)
            .catalog(vec![agent()]),
            recover,
        )
        .await
        .unwrap();
        let directory = root.path().join("subsessions").join(&child_id);
        let (journal, _) =
            CreatorJournal::open(directory.to_str().unwrap(), child_id.clone(), true, true)
                .await
                .unwrap();
        let frozen = journal.head_recover().await.unwrap().unwrap().context;
        let initial = journal.clone();
        let restoring_factory = restored_factory.clone();
        let restoring_parent = parent.clone();
        let restoring_directory = directory.clone();
        let restoring = tokio::spawn(async move {
            restoring_factory
                .restore_child(&restoring_parent, &restoring_directory, initial)
                .await
        });
        restore_gate.entered.notified().await;
        assert!(
            restored_factory
                .restore_child(&parent, &directory, journal.clone())
                .await
                .is_err(),
            "concurrent restoration is rejected before sharing process and Journal owners"
        );
        assert!(
            journal.head_recover().await.unwrap().is_some(),
            "rejected concurrent restore cannot close the active Journal"
        );
        restore_gate.release.add_permits(1);
        let child = restoring.await.unwrap().unwrap();
        assert!(
            restored_factory
                .restore_child(&parent, &directory, journal.clone())
                .await
                .is_err(),
            "already-bound child cannot be constructed twice"
        );
        assert_eq!(
            child.session.communication_identity().unwrap(),
            old_identity
        );
        assert_eq!(
            serde_json::to_value(child.session.context_snapshot().unwrap()).unwrap(),
            serde_json::to_value(frozen).unwrap()
        );
        assert!(parent.child_accepts_user_input(&child_id).unwrap());
        assert!(
            !parent.executions().has_pending(),
            "the former delegate Job is interrupted, not revived"
        );
        parent
            .submit_child_user(&child_id, "explicit targeted user")
            .await
            .unwrap();
        assert_eq!(
            child.session.subscribe_activity().borrow().outcome_sequence,
            1
        );
        let processes = child.session.process_control();
        let handle = processes
            .launch_shell(
                "native_restore_test",
                "owner_after_duplicate",
                microcrab_host_api::ShellRequest {
                    command: "echo restored-owner".into(),
                    cwd: None,
                    output_bytes: 1024,
                },
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert!(processes.owns_process(handle));
        processes
            .release_process(handle, CancellationToken::new())
            .await
            .unwrap();
        child.session.checkpoint().await.unwrap();
        assert!(
            journal.head_recover().await.unwrap().is_some(),
            "original child Journal remains writable after duplicate rejection"
        );
        parent.close().await.unwrap();
        children.close().await.unwrap();
    })
    .await
    .unwrap();
}

struct RestoreGate {
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}
#[async_trait]
impl ChildConfiguration for RestoreGate {
    async fn apply(&self, _: &Session) -> Result<(), Error> {
        self.entered.notify_one();
        self.release
            .acquire()
            .await
            .map_err(|_| Error::Closed)?
            .forget();
        Ok(())
    }
}
