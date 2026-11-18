//! v0.1.8 authorization layers, native builtins and explicit catalog epochs.
use async_trait::async_trait;
use microcrab_agent_types::{AgentInfo, AgentMode, AgentRole, Variable};
use microcrab_permission::{Action, Permission};
use microcrab_protocol::{Response, ToolCall, ToolDefinition};
use microcrab_session::context::{Content, ContentItem};
use microcrab_session::runtime::execution::ToolOutput;
use microcrab_session::runtime::tool::{DenyAll, Tool, ToolContext};
use microcrab_session::runtime::{BuiltinTool, Runtime, ToolRegistration, builtin_tools};
use microcrab_session::{
    Auditor, Client, DelegateExecutor, DynReference, Error, Reference, ReferenceContent,
    ReferenceInfo, Session, VariableView,
};
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Model {
    responses: Mutex<VecDeque<Response>>,
    requests: Mutex<Vec<Content>>,
}
impl Model {
    fn call(&self, id: &str, name: &str, arguments: Value) {
        self.responses.lock().unwrap().extend([
            Response {
                provider_metadata: None,
                content: String::new(),
                tool_calls: vec![ToolCall {
                    provider_metadata: None,
                    id: id.into(),
                    name: name.into(),
                    arguments,
                }],
                usage: Default::default(),
            },
            Response {
                provider_metadata: None,
                content: "done".into(),
                tool_calls: vec![],
                usage: Default::default(),
            },
        ]);
    }
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
        Ok(self
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Response {
                provider_metadata: None,
                content: "ready".into(),
                tool_calls: vec![],
                usage: Default::default(),
            }))
    }
}
fn permission(tool: &str, action: Action) -> Permission {
    serde_json::from_value(json!({"tool":{"kind":"exact","value":tool},"action":action})).unwrap()
}
fn row(view: &Value, name: &str) -> Value {
    view["permission"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| {
            row["tools"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == name)
        })
        .unwrap()
        .clone()
}
fn edit(session: &Session, rules: Vec<Permission>) {
    let policy = session.permission_controller().snapshot().unwrap();
    let mut config = policy.config;
    config.overrides = rules;
    session
        .replace_permissions(policy.revision, config)
        .unwrap();
}
struct Provider {
    name: &'static str,
    action: Option<Action>,
}
#[async_trait]
impl Tool for Provider {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name.into(),
            description: "provider fixture".into(),
            parameters: json!({"type":"object"}),
        }
    }
    fn permission_defaults(&self) -> Vec<Permission> {
        self.action
            .map(|action| permission(self.name, action))
            .into_iter()
            .collect()
    }
    async fn execute(&self, _: Value, _: ToolContext) -> Result<ToolOutput, Error> {
        Ok(ToolOutput::text("provider executed"))
    }
}
struct Executor;
#[async_trait]
impl DelegateExecutor for Executor {
    async fn execute(
        &self,
        target: &str,
        _: &str,
        mode: Option<&str>,
        _: &str,
        timeout_ms: Option<u64>,
        _: ToolContext,
    ) -> Result<String, Error> {
        Ok(json!({"target":target,"mode":mode,"timeout_ms":timeout_ms}).to_string())
    }
}
struct Catalog;
impl Reference for Catalog {
    type Error = std::io::Error;
    fn kind(&self) -> &str {
        "skills"
    }
    async fn load(&self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn list(&self) -> Result<Vec<ReferenceInfo>, Self::Error> {
        Ok(vec![])
    }
    fn read(&self, id: &str) -> Result<Option<ReferenceContent>, Self::Error> {
        Ok((id == "sample").then(|| ReferenceContent {
            id: id.into(),
            body: "actual reference body".into(),
        }))
    }
    fn change(&self) -> Result<Option<Value>, Self::Error> {
        Ok(Some(json!([{"id":"sample"}])))
    }
}
#[tokio::test]
async fn native_builtins_override_guest_defaults_but_user_rules_win_and_expand_reads_catalog() {
    let model = Arc::new(Model::default());
    let runtime = Runtime::new(builtin_tools().into(), Arc::new(DenyAll)).unwrap();
    let agent = AgentInfo {
        tool_media_output_bytes: Default::default(),
        name: "custom".into(),
        modes: vec![AgentMode {
            description: String::new(),
            prompt: String::new(),
            name: "work".into(),
            role: AgentRole::Both,
            permissions: vec![permission("sleep", Action::Deny)],
        }],
        tools: vec![],
        variables: Variable::default(),
    };
    let session = Session::builder_arc(model.clone(), runtime)
        .agent(agent)
        .references(vec![DynReference::new(Catalog).unwrap()])
        .build()
        .unwrap();
    let view = session.state_view().unwrap();
    for name in ["delegate", "expand", "job", "sleep"] {
        assert_eq!(row(&view, name)["action"], "allow");
    }
    assert!(BuiltinTool::from_name("mcp").is_none());
    assert!(
        session
            .permission_controller()
            .snapshot()
            .unwrap()
            .config
            .provider_defaults
            .is_empty(),
        "guest cannot declare defaults for native tool identities"
    );
    model.call("expand-1", "expand", json!({"reference":"sample"}));
    session.submit("expand installed reference").await.unwrap();
    assert!(session.messages().iter().any(|item| matches!(item, ContentItem::Tool { call_id, content, .. } if call_id == "expand-1" && content == "actual reference body")));
    edit(&session, vec![permission("sleep", Action::Deny)]);
    assert_eq!(
        row(&session.state_view().unwrap(), "sleep")["action"],
        "deny"
    );
    let before = session.permission_controller().snapshot().unwrap();
    let mut attempted = before.config.clone();
    attempted.defaults.clear();
    assert!(
        session
            .replace_permissions(before.revision, attempted)
            .is_err()
    );
    assert_eq!(session.permission_controller().snapshot().unwrap(), before);
    session.close().await.unwrap();
}
#[tokio::test]
async fn provider_defaults_are_scoped_low_priority_and_unknown_is_denied() {
    let model = Arc::new(Model::default());
    let runtime = Runtime::new(
        vec![
            ToolRegistration::External(Arc::new(Provider {
                name: "declared",
                action: Some(Action::Allow),
            })),
            ToolRegistration::External(Arc::new(Provider {
                name: "unknown",
                action: None,
            })),
        ],
        Arc::new(DenyAll),
    )
    .unwrap();
    let session = Session::builder_arc(model.clone(), runtime)
        .build()
        .unwrap();
    assert_eq!(
        row(&session.state_view().unwrap(), "declared")["action"],
        "allow"
    );
    assert_eq!(
        row(&session.state_view().unwrap(), "unknown")["action"],
        "deny"
    );
    model.call("provider-1", "declared", json!({}));
    session.submit("call declared provider").await.unwrap();
    assert!(session.messages().iter().any(
        |item| matches!(item, ContentItem::Tool { content, .. } if content == "provider executed")
    ));
    edit(&session, vec![permission("declared", Action::Deny)]);
    assert_eq!(
        row(&session.state_view().unwrap(), "declared")["action"],
        "deny"
    );
    let policy = session.permission_controller().snapshot().unwrap();
    let mut attempted = policy.config.clone();
    attempted.provider_defaults.clear();
    assert!(
        session
            .replace_permissions(policy.revision, attempted)
            .is_err()
    );
    session.close().await.unwrap();
}
#[tokio::test]
async fn opaque_guest_name_uses_declared_mode_grants_and_user_can_override_it() {
    let runtime = Runtime::new(
        vec![ToolRegistration::Wasm(Arc::new(Provider {
            name: "write",
            action: None,
        }))],
        Arc::new(DenyAll),
    )
    .unwrap();
    let agent = AgentInfo {
        tool_media_output_bytes: Default::default(),
        name: "coding".into(),
        modes: vec![AgentMode {
            description: String::new(),
            prompt: String::new(),
            name: "plan".into(),
            role: AgentRole::Both,
            permissions: vec![permission("write", Action::Allow)],
        }],
        tools: vec![],
        variables: Variable::default(),
    };
    let session = Session::builder_arc(Arc::new(Model::default()), runtime)
        .agent(agent)
        .build()
        .unwrap();
    assert_eq!(
        row(&session.state_view().unwrap(), "write")["action"],
        "allow"
    );
    edit(&session, vec![permission("write", Action::Deny)]);
    assert_eq!(
        row(&session.state_view().unwrap(), "write")["action"],
        "deny"
    );
    session.close().await.unwrap();
}
#[tokio::test]
async fn eligibility_requires_executor_and_live_authorization_and_uses_declared_mode_order() {
    let catalog = vec![AgentInfo {
        tool_media_output_bytes: Default::default(),
        name: "worker".into(),
        modes: vec![
            AgentMode {
                description: String::new(),
                prompt: String::new(),
                name: "execute".into(),
                role: AgentRole::Both,
                permissions: vec![],
            },
            AgentMode {
                description: String::new(),
                prompt: String::new(),
                name: "safe".into(),
                role: AgentRole::Both,
                permissions: vec![],
            },
        ],
        tools: vec![],
        variables: Variable::default(),
    }];
    let selected = microcrab_session::runtime::tool::delegate::select_child(
        &catalog,
        Some("plan"),
        "worker",
        None,
    )
    .unwrap();
    assert_eq!(
        selected.1, "execute",
        "opaque mode names do not override declared eligible order"
    );
    let unavailable = Session::builder_arc(
        Arc::new(Model::default()),
        Runtime::new(
            vec![ToolRegistration::Builtin(BuiltinTool::Delegate)],
            Arc::new(DenyAll),
        )
        .unwrap(),
    )
    .catalog(catalog.clone())
    .build()
    .unwrap();
    assert!(
        unavailable
            .state_view()
            .unwrap()
            .get("delegatable_agents")
            .is_none()
    );
    let runtime = Runtime::new(
        vec![ToolRegistration::Builtin(BuiltinTool::Delegate)],
        Arc::new(DenyAll),
    )
    .unwrap()
    .with_delegate_executor(Arc::new(Executor));
    let model = Arc::new(Model::default());
    let session = Session::builder_arc(model.clone(), runtime)
        .catalog(catalog)
        .build()
        .unwrap();
    assert_eq!(
        session.state_view().unwrap()["delegatable_agents"][0]["name"],
        "worker"
    );
    let descriptor = session.state_view().unwrap()["delegatable_agents"][0].clone();
    assert!(descriptor.get("authorization").is_none());
    assert!(descriptor.get("policies").is_none());
    assert!(session.state_view().unwrap().get("subagents").is_none());
    edit(&session, vec![permission("delegate", Action::Deny)]);
    assert!(
        session
            .state_view()
            .unwrap()
            .get("delegatable_agents")
            .is_none()
    );
    let mut conditional = permission("delegate", Action::Allow);
    conditional.args = serde_json::from_value(json!([
        {"pointer":"/input","matcher":{"kind":"exact","value":"safe task"}}
    ]))
    .unwrap();
    edit(
        &session,
        vec![permission("delegate", Action::Deny), conditional],
    );
    assert_eq!(
        session.state_view().unwrap()["delegatable_agents"][0]["modes"]
            .as_array()
            .unwrap()
            .len(),
        2,
        "unknown invocation input cannot prove every mode denied"
    );
    let mut selective = permission("delegate", Action::Allow);
    selective.args = serde_json::from_value(
        json!([{"pointer":"/mode","matcher":{"kind":"exact","value":"safe"}}]),
    )
    .unwrap();
    edit(
        &session,
        vec![permission("delegate", Action::Deny), selective],
    );
    assert_eq!(
        session.state_view().unwrap()["delegatable_agents"][0]["modes"],
        json!([{"name":"safe","description":""}])
    );
    model.call(
        "canonical-delegate",
        "delegate",
        json!({"target":"worker","input":"perform safe work"}),
    );
    session
        .submit("delegate without explicit mode")
        .await
        .unwrap();
    let actual: Value = session
        .messages()
        .iter()
        .find_map(|item| match item {
            ContentItem::Tool { call_id, content, .. } if call_id == "canonical-delegate" => {
                serde_json::from_str(content).ok()
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(actual["target"], "worker");
    assert_eq!(
        actual["mode"], "safe",
        "omitted mode must use the same authorized modes as State"
    );
    assert!(
        actual["timeout_ms"].is_null(),
        "Runtime must not invent a child deadline"
    );
    session.close().await.unwrap();
    unavailable.close().await.unwrap();
}
#[tokio::test]
async fn external_catalog_change_is_explicit_preserves_history_and_restores_historical_calls() {
    let model = Arc::new(Model::default());
    let runtime = Runtime::new(
        vec![ToolRegistration::External(Arc::new(Provider {
            name: "old_tool",
            action: Some(Action::Allow),
        }))],
        Arc::new(DenyAll),
    )
    .unwrap();
    let session = Session::builder_arc(model.clone(), runtime)
        .build()
        .unwrap();
    model.call("old-call", "old_tool", json!({}));
    session.submit("old epoch").await.unwrap();
    let old = session.checkpoint().await.unwrap();
    session
        .replace_external_tools(vec![Arc::new(Provider {
            name: "new_tool",
            action: Some(Action::Allow),
        })])
        .await
        .unwrap();
    model.call("new-call", "new_tool", json!({}));
    session.submit("new epoch").await.unwrap();
    let current = session.checkpoint().await.unwrap();
    assert!(current.state.epoch_id > old.state.epoch_id);
    assert!(
        current
            .context
            .items
            .iter()
            .any(|item| matches!(item, ContentItem::Tool { call_id, .. } if call_id == "old-call"))
    );
    assert!(
        current
            .context
            .tools
            .iter()
            .any(|tool| tool.name == "new_tool")
    );
    assert!(
        !current
            .context
            .tools
            .iter()
            .any(|tool| tool.name == "old_tool")
    );
    let restored_runtime = Runtime::new(
        vec![ToolRegistration::External(Arc::new(Provider {
            name: "new_tool",
            action: Some(Action::Allow),
        }))],
        Arc::new(DenyAll),
    )
    .unwrap();
    let restored = Session::restore(
        Session::builder_arc(Arc::new(Model::default()), restored_runtime),
        current,
    )
    .await
    .unwrap();
    restored.submit("continue restored history").await.unwrap();
    assert!(
        session.recover(old.clone()).await.is_err(),
        "ordinary recovery remains exact for external catalog changes"
    );
    session
        .recover_with_external_tools(
            old.clone(),
            vec![Arc::new(Provider {
                name: "old_tool",
                action: Some(Action::Allow),
            })],
        )
        .await
        .unwrap();
    assert_eq!(session.context_snapshot().unwrap().tools, old.context.tools);
    assert_eq!(
        session.context_snapshot().unwrap().epoch,
        old.context.epoch,
        "history recovery does not invent an epoch"
    );
    let before = session.checkpoint().await.unwrap();
    assert!(
        session
            .replace_external_tools(vec![Arc::new(Provider {
                name: "sleep",
                action: Some(Action::Allow)
            })])
            .await
            .is_err()
    );
    assert_eq!(
        session.checkpoint().await.unwrap().context.tools,
        before.context.tools
    );
    restored.close().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test]
async fn native_job_keeps_all_six_actions_allowed_in_every_mode_and_args_override_narrows_calls() {
    use microcrab_permission::{ArgumentPredicate, CompiledToolPolicy, Matcher, evaluate_compiled};
    let session = Session::builder_arc(
        Arc::new(Model::default()),
        Runtime::new(builtin_tools().into(), Arc::new(DenyAll)).unwrap(),
    )
    .agent(AgentInfo {
        tool_media_output_bytes: Default::default(),
        name: "coding".into(),
        modes: ["plan", "build"]
            .into_iter()
            .map(|name| AgentMode {
                description: String::new(),
                prompt: String::new(),
                name: name.into(),
                role: AgentRole::Both,
                permissions: vec![],
            })
            .collect(),
        tools: vec![],
        variables: Variable::default(),
    })
    .build()
    .unwrap();
    for mode in ["plan", "build"] {
        session.select_mode(mode).await.unwrap();
        let mut projected = row(&session.state_view().unwrap(), "job");
        projected.as_object_mut().unwrap().remove("tools");
        projected.as_object_mut().unwrap().remove("owners");
        let policy: CompiledToolPolicy = serde_json::from_value(projected).unwrap();
        for action in ["status", "read", "wait", "write", "cancel", "release"] {
            assert_eq!(
                evaluate_compiled(&policy, &json!({"action":action})).unwrap(),
                Action::Allow
            );
        }
    }
    let mut deny_write = permission("job", Action::Deny);
    deny_write.args = vec![ArgumentPredicate {
        pointer: "/action".into(),
        matcher: Matcher::Exact("write".into()),
    }];
    edit(&session, vec![deny_write]);
    let mut projected = row(&session.state_view().unwrap(), "job");
    assert_eq!(projected["kind"], "conditional");
    projected.as_object_mut().unwrap().remove("tools");
    projected.as_object_mut().unwrap().remove("owners");
    let policy: CompiledToolPolicy = serde_json::from_value(projected).unwrap();
    assert_eq!(
        evaluate_compiled(&policy, &json!({"action":"write"})).unwrap(),
        Action::Deny
    );
    assert_eq!(
        evaluate_compiled(&policy, &json!({"action":"cancel"})).unwrap(),
        Action::Allow
    );
    assert_eq!(
        microcrab_permission::PermissionConfig::default().initialization,
        Action::Allow
    );
    session.close().await.unwrap();
}

#[test]
fn delegate_canonical_names_override_aliases_and_ambiguous_aliases_are_rejected() {
    use microcrab_session::runtime::tool::delegate::select_child;
    let catalog: Vec<AgentInfo> = serde_json::from_value(json!([
        {"name":"one","modes":[{"name":"review","role":"sub_agent","permissions":[]}],"tools":[],"variables":{}},
        {"name":"two","modes":[{"name":"review","role":"sub_agent","permissions":[]}],"tools":[],"variables":{}},
        {"name":"review","modes":[{"name":"canonical","role":"sub_agent","permissions":[]}],"tools":[],"variables":{}}
    ])).unwrap();
    assert!(select_child(&catalog[..2], None, "review", None).is_err());
    let (agent, mode) = select_child(&catalog, None, "review", None).unwrap();
    assert_eq!(agent.name, "review");
    assert_eq!(mode, "canonical");
    assert_eq!(
        select_child(&catalog, None, "one", Some("review"))
            .unwrap()
            .0
            .name,
        "one"
    );
}
