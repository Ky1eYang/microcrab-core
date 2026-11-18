//! Delegate state exposure and Session execution isolation on a shared Host.
use async_trait::async_trait;
use microcrab_agent_types::{AgentInfo, AgentMode, AgentRole, Variable};
use microcrab_auditor::Auditor;
use microcrab_auditor::VariableView;
use microcrab_host_local::{LocalHost, ShellLimits};
use microcrab_protocol::{Response, Usage};
use microcrab_session::context::Content;
use microcrab_session::runtime::{BuiltinTool, Runtime, ToolRegistration, tool::DenyAll};
use microcrab_session::{Client, Session};
use serde_json::Value;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

struct DummyClient(AtomicUsize);
#[async_trait]
impl Client for DummyClient {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        Ok(Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                vec![microcrab_protocol::ToolCall {
                    provider_metadata: None,
                    id: "explicit-delegate-denial".into(),
                    name: "delegate".into(),
                    arguments: serde_json::json!({"target":"execute","input":"inspect"}),
                }]
            } else {
                vec![]
            },
            usage: Usage::default(),
        })
    }
}

fn test_catalog() -> Vec<AgentInfo> {
    let mut rev_vars = Variable::default();
    rev_vars
        .inject("description", "Code and design reviewer")
        .unwrap();
    let mut exec_vars = Variable::default();
    exec_vars.inject("description", "Direct executor").unwrap();

    vec![
        AgentInfo {
            tool_media_output_bytes: Default::default(),
            name: "planner".into(),
            modes: vec![
                AgentMode {
                    description: String::new(),
                    prompt: String::new(),
                    name: "general".into(),
                    role: AgentRole::MainAgent,
                    permissions: vec![],
                },
                AgentMode {
                    description: String::new(),
                    prompt: String::new(),
                    name: "plan".into(),
                    role: AgentRole::MainAgent,
                    permissions: vec![],
                },
            ],
            tools: vec![],
            variables: Variable::default(),
        },
        AgentInfo {
            tool_media_output_bytes: Default::default(),
            name: "reviewer".into(),
            modes: vec![AgentMode {
                description: String::new(),
                prompt: String::new(),
                name: "review".into(),
                role: AgentRole::SubAgent,
                permissions: vec![],
            }],
            tools: vec![],
            variables: rev_vars,
        },
        AgentInfo {
            tool_media_output_bytes: Default::default(),
            name: "execute".into(),
            modes: vec![AgentMode {
                description: String::new(),
                prompt: String::new(),
                name: "execute".into(),
                role: AgentRole::SubAgent,
                permissions: vec![],
            }],
            tools: vec![],
            variables: exec_vars,
        },
    ]
}

struct AvailableDelegate(Arc<AtomicUsize>);
#[async_trait]
impl microcrab_session::runtime::tool::delegate::DelegateExecutor for AvailableDelegate {
    async fn execute(
        &self,
        _: &str,
        _: &str,
        _: Option<&str>,
        _: &str,
        _: Option<u64>,
        _: microcrab_session::runtime::tool::ToolContext,
    ) -> Result<String, microcrab_session::Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok("fixture executor".into())
    }
}

#[tokio::test]
async fn subagent_roles_are_visible_and_explicit_mode_policy_denies_dispatch() {
    let dispatches = Arc::new(AtomicUsize::new(0));
    let runtime = Runtime::new(
        vec![
            ToolRegistration::Builtin(BuiltinTool::Delegate),
            ToolRegistration::Builtin(BuiltinTool::Job),
        ],
        Arc::new(DenyAll),
    )
    .unwrap()
    .with_delegate_executor(Arc::new(AvailableDelegate(dispatches.clone())));

    let session = Session::builder(DummyClient(AtomicUsize::new(0)), runtime)
        .catalog(test_catalog())
        .agent(test_catalog()[0].clone())
        .build()
        .unwrap();

    // Default mode: both reviewer and execute subagents should be visible in state
    let subagents = session
        .state_handle()
        .get("delegatable_agents")
        .expect("subagents should be exposed in VariableView");
    let arr = subagents.as_array().expect("subagents is an array");
    let names: Vec<&str> = arr
        .iter()
        .filter_map(|s| s.get("name").and_then(Value::as_str))
        .collect();
    assert!(names.contains(&"reviewer"));
    assert!(names.contains(&"execute"));

    // Opaque mode names preserve role eligibility; declared policy controls dispatch.
    session.select_mode("plan").await.unwrap();
    let subagents = session
        .state_handle()
        .get("delegatable_agents")
        .expect("subagents should be exposed in VariableView");
    let arr = subagents.as_array().expect("subagents is an array");
    let names: Vec<&str> = arr
        .iter()
        .filter_map(|s| s.get("name").and_then(Value::as_str))
        .collect();
    assert!(names.contains(&"reviewer"));
    assert!(
        names.contains(&"execute"),
        "opaque mode/agent names cannot remove a declared SubAgent role"
    );
    assert!(
        !names.contains(&"planner"),
        "MainAgent-only roles are not eligible children"
    );
    let policy = session.permission_controller().snapshot().unwrap();
    let mut config = policy.config;
    config.overrides = vec![microcrab_permission::Permission {
        agent: None,
        mode: Some("plan".into()),
        tool: Some(microcrab_permission::Matcher::Exact("delegate".into())),
        args: vec![],
        action: microcrab_permission::Action::Deny,
    }];
    session
        .replace_permissions(policy.revision, config)
        .unwrap();
    assert!(
        session
            .state_view()
            .unwrap()
            .get("delegatable_agents")
            .is_none(),
        "explicit denial removes authorized delegate discovery"
    );
    session
        .submit("explicit mode rule denies delegation")
        .await
        .unwrap();
    assert_eq!(dispatches.load(Ordering::SeqCst), 0);
    let row = session
        .execution_snapshots()
        .into_iter()
        .find(|row| row.tool == "delegate")
        .unwrap();
    assert_eq!(
        row.status,
        microcrab_session::runtime::execution::ExecutionStatus::Failed
    );

    session.close().await.unwrap();
}

fn host(root: &TempDir) -> Arc<LocalHost> {
    Arc::new(LocalHost::new(root.path(), ShellLimits::default()).unwrap())
}

use crate::execution_fixture;

#[tokio::test]
async fn session_owned_executions_are_isolated_even_when_host_is_shared() {
    use execution_fixture::{Fixture, id, sleeping};
    use microcrab_session::{Limits, runtime::execution::ExecutionStatus};
    let root = TempDir::new().unwrap();
    let host = host(&root);
    let main = Fixture::new("main-session", host.clone(), Limits::default());
    let child = Fixture::new("sub-session-1", host.clone(), Limits::default());
    let peer = Fixture::new("sub-session-2", host, Limits::default());
    let own = main.start(sleeping(), 0).await.unwrap();
    let theirs = child.start(sleeping(), 0).await.unwrap();
    let peer_receipt = peer.start(sleeping(), 0).await.unwrap();
    assert_eq!(
        own["execution_id"], theirs["execution_id"],
        "public IDs are Session scoped"
    );
    assert_ne!(id(&own), id(&theirs));
    for fixture in [&main, &child, &peer] {
        let rows = fixture.session.execution_snapshots();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].session_id, fixture.owner);
    }
    assert!(
        child
            .action(id(&own), "cancel", serde_json::json!({}))
            .await
            .is_err()
    );
    assert!(
        child
            .action(id(&peer_receipt), "cancel", serde_json::json!({}))
            .await
            .is_err()
    );
    assert!(
        main.action(id(&theirs), "cancel", serde_json::json!({}))
            .await
            .is_err()
    );
    assert!(child.read(id(&theirs), 0, 65536).unwrap().chunks.is_empty());
    child
        .session
        .cancel_execution(child.id(id(&theirs)).unwrap())
        .await
        .unwrap();
    assert_eq!(
        child.terminal(id(&theirs)).await.status,
        ExecutionStatus::Cancelled
    );
    peer.close().await.unwrap();
    assert!(
        !peer.session.executions().has_pending(),
        "close acknowledges its own execution cleanup"
    );
    assert!(
        main.session.executions().has_pending(),
        "another Session keeps its independent producer"
    );
    main.close().await.unwrap();
    child.close().await.unwrap();
}
