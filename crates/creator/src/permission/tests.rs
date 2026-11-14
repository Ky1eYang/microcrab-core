//! Real shared-adapter permissions inspect and control generic owned executions.
use super::*;
use microcrab_auditor::{Auditor, VariableView};
use microcrab_host_local::{LocalHost, ShellLimits};
use microcrab_protocol::{Response, ToolCall, Usage};
use microcrab_session::{
    Client, Session,
    runtime::{BuiltinTool, Runtime, ToolRegistration},
};
use microcrab_transport::Content;
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

struct Script {
    responses: Mutex<VecDeque<Response>>,
    requests: Mutex<Vec<Content>>,
}
impl Script {
    fn new() -> Self {
        Self {
            responses: Mutex::new(VecDeque::new()),
            requests: Mutex::new(vec![]),
        }
    }
    fn turn(&self, calls: Vec<ToolCall>) {
        self.responses.lock().unwrap().extend([
            Response {
                provider_metadata: None,
                content: "inspect jobs".into(),
                tool_calls: calls,
                usage: Usage::default(),
            },
            Response {
                provider_metadata: None,
                content: "done".into(),
                tool_calls: vec![],
                usage: Usage::default(),
            },
        ]);
    }
    fn output(&self, id: &str) -> String {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find_map(|request| {
                request.items.iter().find_map(|item| match item {
                    microcrab_session::context::ContentItem::Tool { call_id, content }
                        if call_id == id =>
                    {
                        Some(content.clone())
                    }
                    _ => None,
                })
            })
            .expect("next model request contains the tool result")
    }
}
#[async_trait]
impl Client for Script {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.requests.lock().unwrap().push(content.clone());
        Ok(self
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("script covers each round"))
    }
}
struct FixtureProducer;
struct FixtureOwner(std::sync::Mutex<Option<microcrab_session::runtime::execution::OutputWriter>>);
#[async_trait]
impl microcrab_session::runtime::execution::ExecutionController for FixtureOwner {
    fn capabilities(&self) -> microcrab_session::runtime::execution::ExecutionCapabilities {
        microcrab_session::runtime::execution::ExecutionCapabilities { input: true }
    }
    async fn write(
        &self,
        data: String,
        _: bool,
        _: CancellationToken,
    ) -> Result<(), microcrab_session::Error> {
        self.0.lock().unwrap().as_ref().unwrap().append(None, &data)
    }
    async fn cancel(&self, _: CancellationToken) -> Result<(), microcrab_session::Error> {
        if let Some(writer) = self.0.lock().unwrap().take() {
            writer.finish(Err(microcrab_session::runtime::execution::ToolFailure {
                kind: microcrab_session::runtime::execution::ToolFailureKind::Cancelled,
                code: "fixture_cancelled".into(),
                message: "Fixture cancelled".into(),
                retryable: false,
            }))?;
        }
        Ok(())
    }
    async fn release(&self, token: CancellationToken) -> Result<(), microcrab_session::Error> {
        self.cancel(token).await
    }
}
#[async_trait]
impl microcrab_session::runtime::tool::Tool for FixtureProducer {
    fn definition(&self) -> microcrab_protocol::ToolDefinition {
        microcrab_protocol::ToolDefinition {
            name: "fixture_producer".into(),
            description: "Owned nonprocess fixture".into(),
            parameters: json!({"type":"object"}),
        }
    }
    fn permission_defaults(&self) -> Vec<microcrab_permission::Permission> {
        serde_json::from_value(
            json!([{"tool":{"kind":"exact","value":"fixture_producer"},"action":"allow"}]),
        )
        .unwrap()
    }
    async fn execute(
        &self,
        _: Value,
        context: microcrab_session::runtime::tool::ToolContext,
    ) -> Result<microcrab_session::runtime::execution::ToolOutput, microcrab_session::Error> {
        let owner = Arc::new(FixtureOwner(std::sync::Mutex::new(None)));
        let (writer, output) = context.open_output(Default::default(), owner.clone())?;
        *owner.0.lock().unwrap() = Some(writer);
        Ok(microcrab_session::runtime::execution::ToolOutput::Buffered(
            output,
        ))
    }
}
fn job_call(id: &str, address: &str, action: &str) -> ToolCall {
    let mut arguments = json!({"id":address,"action":action});
    if action == "write" {
        arguments["args"] = json!({"data":"forbidden\n"});
    }
    ToolCall {
        provider_metadata: None,
        id: id.into(),
        name: "job".into(),
        arguments,
    }
}
fn assert_permission_denial(output: &str) {
    let result: Value = serde_json::from_str(output).unwrap();
    assert_eq!(result["diagnostic"]["code"], "permission_denied");
    assert_eq!(result["diagnostic"]["layer"], "runtime");
    assert_eq!(result["diagnostic"]["phase"], "authorization");
    let reason = result["diagnostic"]["reason"].as_str().unwrap();
    assert!(
        reason.contains("permission denied by controller"),
        "{result}"
    );
    assert!(reason.contains("revision"), "{result}");
    assert_eq!(result["error"], result["diagnostic"]["reason"]);
}

fn inspect_rules() -> Vec<microcrab_permission::Permission> {
    // Explicit restrictions narrow the native builtin Allow baseline.
    serde_json::from_value(json!([
        {"tool":{"kind":"exact","value":"job"},"action":"deny"},
        {"tool":{"kind":"exact","value":"job"},"args":[{"pointer":"/action","matcher":{"kind":"exact","value":"status"}}],"action":"allow"}
    ])).unwrap()
}
fn runtime() -> Runtime {
    Runtime::new(
        vec![
            ToolRegistration::Builtin(BuiltinTool::Job),
            ToolRegistration::Builtin(BuiltinTool::Sleep),
            ToolRegistration::External(Arc::new(FixtureProducer)),
        ],
        Arc::new(DeclarativePermission),
    )
    .unwrap()
}
async fn start_job(session: &Session, client: &Script) -> String {
    client.turn(vec![ToolCall {
        provider_metadata: None,
        id: "fixture-start".into(),
        name: "fixture_producer".into(),
        arguments: json!({}),
    }]);
    session.submit("start generic producer").await.unwrap();
    serde_json::from_str::<Value>(&client.output("fixture-start")).unwrap()["id"]
        .as_str()
        .unwrap()
        .into()
}

#[tokio::test]
async fn declarative_inspection_grant_does_not_allow_job_input_or_stop() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let root = tempfile::tempdir().unwrap();
        let host = Arc::new(LocalHost::new(root.path(), ShellLimits::default()).unwrap());
        let runtime = runtime();
        let controller = runtime.permission_controller();
        let policy = controller.snapshot().unwrap();
        let mut config = policy.config;
        config.review = false;
        config.overrides = inspect_rules();
        let client = Arc::new(Script::new());
        let session = Session::builder_arc(client.clone(), runtime)
            .permissions(config)
            .host(host)
            .build()
            .unwrap();
        let address = start_job(&session, &client).await;
        client.turn(vec![
            job_call("inspect", &address, "status"),
            job_call("write-denied", &address, "write"),
            job_call("kill-denied", &address, "cancel"),
        ]);
        session.submit("inspect only").await.unwrap();
        let inspected: Value = serde_json::from_str(&client.output("inspect")).unwrap();
        assert_eq!(inspected["status"], "running");
        for id in ["write-denied", "kill-denied"] {
            assert_permission_denial(&client.output(id));
        }
        assert!(
            session.executions().has_pending(),
            "denied cancellation must preserve the owned producer"
        );
        let hints = session.state_handle().get("permission").unwrap();
        let job_hint = hints
            .as_array()
            .unwrap()
            .iter()
            .find(|hint| {
                hint["tools"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|value| value == "job")
            })
            .unwrap();
        let mut compiled = job_hint.clone();
        compiled.as_object_mut().unwrap().remove("tools");
        compiled.as_object_mut().unwrap().remove("owners");
        let compiled: microcrab_permission::CompiledToolPolicy =
            serde_json::from_value(compiled).unwrap();
        assert_eq!(
            microcrab_permission::evaluate_compiled(&compiled, &json!({"action":"status"}))
                .unwrap(),
            microcrab_permission::Action::Allow
        );
        assert_eq!(
            microcrab_permission::evaluate_compiled(&compiled, &json!({"action":"cancel"}))
                .unwrap(),
            microcrab_permission::Action::Deny
        );
        session.close().await.unwrap();
    })
    .await
    .expect("job permission regression must settle");
}

#[tokio::test]
async fn declarative_explicit_builtin_deny_overrides_mode_allow() {
    let runtime = runtime();
    let controller = runtime.permission_controller();
    let policy = controller.snapshot().unwrap();
    let mut config = policy.config;
    config.review = false;
    config.overrides = serde_json::from_value(json!([
        {"tool":{"kind":"exact","value":"sleep"},"action":"deny"}
    ]))
    .unwrap();
    let client = Arc::new(Script::new());
    client.turn(vec![ToolCall {
        provider_metadata: None,
        id: "explicit-deny".into(),
        name: "sleep".into(),
        arguments: json!({"time_ms":1}),
    }]);
    let session = Session::builder_arc(client.clone(), runtime)
        .permissions(config)
        .build()
        .unwrap();
    session.submit("must honor explicit deny").await.unwrap();
    assert_permission_denial(&client.output("explicit-deny"));
    let hints = session.state_handle().get("permission").unwrap();
    assert!(hints.as_array().unwrap().iter().any(|hint| {
        hint["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value == "sleep")
            && hint["action"] == "deny"
    }));
    session.close().await.unwrap();
}

#[tokio::test]
async fn declarative_child_job_permissions_keep_the_live_parent_ceiling() {
    tokio::time::timeout(Duration::from_secs(30), async {
        use crate::delegate::{ChildFactory, ChildSession, OwnedDelegateExecutor};
        use microcrab_session::{AgentInfo, AgentMode, AgentRole, Error};
        use microcrab_session::runtime::tool::{Tool, ToolContext};
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountedProducer(Arc<AtomicUsize>);
        #[async_trait]
        impl Tool for CountedProducer {
            fn definition(&self) -> microcrab_protocol::ToolDefinition {
                FixtureProducer.definition()
            }
            fn permission_defaults(&self) -> Vec<microcrab_permission::Permission> {
                FixtureProducer.permission_defaults()
            }
            async fn execute(&self, arguments: Value, context: ToolContext) -> Result<microcrab_session::runtime::execution::ToolOutput, Error> {
                self.0.fetch_add(1, Ordering::SeqCst);
                FixtureProducer.execute(arguments, context).await
            }
        }
        struct Factory {
            client: Arc<Script>,
            parent_policy: Arc<microcrab_session::policy::PermissionController>,
            host: Arc<dyn microcrab_host_api::Host>,
            agent: AgentInfo,
            count: Arc<AtomicUsize>,
            prepared: tokio::sync::mpsc::UnboundedSender<Session>,
        }
        #[async_trait]
        impl ChildFactory for Factory {
            async fn create(&self, _: Option<&str>, _: &str, _: Option<&str>, _: &str, processes: Arc<dyn microcrab_session::execution::process::ProcessControl>) -> Result<ChildSession, Error> {
                let runtime = Runtime::new(vec![
                    ToolRegistration::Builtin(BuiltinTool::Job),
                    ToolRegistration::Builtin(BuiltinTool::Sleep),
                    ToolRegistration::External(Arc::new(CountedProducer(self.count.clone()))),
                ], Arc::new(DeclarativePermission))?;
                let builder = crate::delegate::inherit_policy(
                    Session::builder_arc(self.client.clone(), runtime), &self.parent_policy,
                )?;
                let child = builder.session_id("policy-child").agent(self.agent.clone())
                    .processes_parent(processes).is_main(false).host(self.host.clone()).build()?;
                // The child deliberately grants more than its parent. The live
                // ancestor must continue to restrict every later job invocation.
                let policy = child.permission_controller().snapshot().unwrap();
                let mut config = policy.config;
                config.overrides.push(serde_json::from_value(json!({"tool":{"kind":"exact","value":"job"},"action":"allow"})).unwrap());
                child.replace_permissions(policy.revision, config).unwrap();
                self.prepared.send(child.clone()).map_err(|_| Error::Closed)?;
                Ok(ChildSession { id: "policy-child".into(), session: child, mode: "execute".into(),
                    ledger: Arc::new(crate::UsageLedger::open(None)?), auditor: Arc::new(microcrab_auditor::NoopAuditor) })
            }
        }

        let root = tempfile::tempdir().unwrap();
        let host: Arc<dyn microcrab_host_api::Host> = Arc::new(LocalHost::new(root.path(), ShellLimits::default()).unwrap());
        let agent = AgentInfo { name: "policy-worker".into(), modes: vec![AgentMode {
            name: "execute".into(), description: "Inspect the owned producer.".into(),
            prompt: "Follow parent assignments without exceeding the parent permission ceiling.".into(),
            role: AgentRole::SubAgent, permissions: vec![],
        }], tools: vec![], variables: Default::default() };
        let parent_runtime = Runtime::new(vec![
            ToolRegistration::Builtin(BuiltinTool::Delegate),
            ToolRegistration::Builtin(BuiltinTool::Job),
            ToolRegistration::Builtin(BuiltinTool::Sleep),
            ToolRegistration::External(Arc::new(FixtureProducer)),
        ], Arc::new(DeclarativePermission)).unwrap();
        let parent_policy = parent_runtime.permission_controller();
        let mut config = parent_policy.snapshot().unwrap().config;
        config.review = false;
        config.overrides = inspect_rules();
        config.overrides.push(serde_json::from_value(json!({"tool":{"kind":"exact","value":"delegate"},"action":"allow"})).unwrap());
        let client = Arc::new(Script::new());
        client.turn(vec![ToolCall { provider_metadata: None, id: "fixture-start".into(), name: "fixture_producer".into(), arguments: json!({}) }]);
        let parent_client = Arc::new(Script::new());
        parent_client.turn(vec![ToolCall { provider_metadata: None, id: "delegate-policy-child".into(), name: "delegate".into(), arguments: json!({"target":"policy-worker","mode":"execute","input":"start generic producer"}) }]);
        let count = Arc::new(AtomicUsize::new(0));
        let children = crate::Children::default();
        let (prepared, mut received) = tokio::sync::mpsc::unbounded_channel();
        let factory = Factory {client: client.clone(), parent_policy: parent_policy.clone(), host: host.clone(), agent: agent.clone(), count: count.clone(), prepared};
        let parent = Session::builder_arc(parent_client, parent_runtime.with_delegate_executor(Arc::new(OwnedDelegateExecutor::new(children.clone(), Arc::new(factory)))))
            .session_id("policy-parent").permissions(config).host(host).catalog(vec![agent]).build().unwrap();
        assert!(Arc::ptr_eq(&parent_policy, &parent.permission_controller()));
        parent.submit("delegate producer ownership").await.unwrap();
        let child = received.recv().await.unwrap();
        let mut activity = child.subscribe_activity();
        while activity.borrow().outcome_sequence < 1 {activity.changed().await.unwrap();}
        assert_eq!(child.kind(), microcrab_session::SessionKind::Delegated);
        assert_eq!(child.parent_relationship().unwrap().parent_session_id, "policy-parent");
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let address = serde_json::from_str::<Value>(&client.output("fixture-start")).unwrap()["id"].as_str().unwrap().to_owned();

        client.turn(vec![job_call("child-inspect", &address, "status"), job_call("ceiling-kill", &address, "cancel")]);
        parent.submit_child("policy-child", "policy-worker", "parent grants inspect only").await.unwrap();
        assert_eq!(serde_json::from_str::<Value>(&client.output("child-inspect")).unwrap()["status"], "running");
        assert_permission_denial(&client.output("ceiling-kill"));
        assert!(child.executions().has_pending(), "denied cancellation preserves the actual owned producer");
        let policy = parent_policy.snapshot().unwrap();
        let mut config = policy.config;
        config.overrides.push(serde_json::from_value(json!({"tool":{"kind":"exact","value":"job"},"action":"deny"})).unwrap());
        parent.replace_permissions(policy.revision, config).unwrap();
        client.turn(vec![job_call("live-ceiling", &address, "status")]);
        parent.submit_child("policy-child", "policy-worker", "parent has revoked inspect").await.unwrap();
        assert_permission_denial(&client.output("live-ceiling"));
        assert_eq!(count.load(Ordering::SeqCst), 1, "follow-up control does not replay the producer effect");
        parent.close_child("policy-child", "policy-worker").await.unwrap();
        assert!(!child.executions().has_pending(), "parent close acknowledges child producer cleanup");
        parent.close().await.unwrap();
        children.close().await.unwrap();
    }).await.expect("live parent ceiling must remain authoritative");
}
