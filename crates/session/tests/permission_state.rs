//! Concrete tool hints follow Runtime policy without exposing its configuration hierarchy.
use async_trait::async_trait;
use microcrab_agent_types::{AgentInfo, AgentMode, AgentRole, Variable};
use microcrab_permission::*;
use microcrab_protocol::{Response, ToolDefinition, Usage};
use microcrab_session::context::{Content, ContentItem};
use microcrab_session::policy::{PermissionController, PermissionError, SessionPermission};
use microcrab_session::runtime::Runtime;
use microcrab_session::runtime::execution::ToolOutput;
use microcrab_session::runtime::tool::{BuiltinTool, Tool, ToolContext, ToolRegistration};
use microcrab_session::{Auditor, Client, Session, VariableView};
use microcrab_transport::StateFrame;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

struct Policy;
#[async_trait]
impl SessionPermission for Policy {
    fn declarative_policy(&self) -> bool {
        true
    }
    async fn evaluate(
        &self,
        policy: &PolicySnapshot,
        request: &PermissionRequest,
    ) -> Result<Decision, PermissionError> {
        evaluate(policy, request)
            .map_err(|error| PermissionError::InvalidConfiguration(error.to_string()))
    }
}
struct Unknown;
#[async_trait]
impl SessionPermission for Unknown {
    async fn evaluate(
        &self,
        _: &PolicySnapshot,
        _: &PermissionRequest,
    ) -> Result<Decision, PermissionError> {
        panic!("model projection must not execute the async adapter")
    }
}
struct Export(&'static str);
#[async_trait]
impl Tool for Export {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.0.into(),
            description: "fixture export".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn execute(
        &self,
        _: Value,
        _: ToolContext,
    ) -> Result<ToolOutput, microcrab_session::Error> {
        panic!("answer-only fixture never dispatches")
    }
}
#[derive(Default)]
struct Model(Mutex<Vec<Content>>);
#[async_trait]
impl Client for Model {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.0.lock().unwrap().push(content.clone());
        Ok(Response {
            provider_metadata: None,
            content: "ready".into(),
            tool_calls: vec![],
            usage: Usage::default(),
        })
    }
}
fn rule(action: Action) -> Permission {
    Permission {
        agent: None,
        mode: None,
        tool: None,
        args: vec![],
        action,
    }
}
fn patch_rule(action: Action) -> Permission {
    Permission {
        tool: Some(Matcher::Exact("patch".into())),
        ..rule(action)
    }
}
fn setup_with_parent(
    adapter: Arc<dyn SessionPermission>,
    parent: Option<Arc<PermissionController>>,
) -> (Session, Arc<PermissionController>, Arc<Model>) {
    let runtime = Runtime::new(
        vec![
            ToolRegistration::Wasm(Arc::new(Export("read"))),
            ToolRegistration::Wasm(Arc::new(Export("patch"))),
            ToolRegistration::Builtin(BuiltinTool::Sleep),
            ToolRegistration::Builtin(BuiltinTool::Job),
        ],
        adapter,
    )
    .unwrap();
    let controller = runtime.permission_controller();
    let model = Arc::new(Model::default());
    let defaults = |action| vec![rule(Action::Allow), patch_rule(action)];
    let agent = AgentInfo {
        tool_media_output_bytes: Default::default(),
        name: "coding".into(),
        modes: vec![
            AgentMode {
                description: String::new(),
                prompt: String::new(),
                name: "plan".into(),
                role: AgentRole::Both,
                permissions: defaults(Action::Deny),
            },
            AgentMode {
                description: String::new(),
                prompt: String::new(),
                name: "build".into(),
                role: AgentRole::Both,
                permissions: defaults(Action::Ask),
            },
        ],
        tools: vec![],
        variables: Variable::default(),
    };
    let mut builder = Session::builder_arc(model.clone(), runtime).agent(agent);
    if let Some(parent) = parent {
        builder = builder.permission_parent(parent);
    }
    let session = builder.build().unwrap();
    (session, controller, model)
}
fn setup(adapter: Arc<dyn SessionPermission>) -> (Session, Arc<PermissionController>, Arc<Model>) {
    setup_with_parent(adapter, None)
}
fn edit(
    session: &Session,
    change: impl FnOnce(&mut PermissionConfig),
) -> Result<PolicySnapshot, PermissionError> {
    let snapshot = session.permission_controller().snapshot()?;
    let mut config = snapshot.config;
    change(&mut config);
    session.replace_permissions(snapshot.revision, config)
}
fn permission(content: &Content) -> &Value {
    content
        .items
        .iter()
        .rev()
        .find_map(|item| match item {
            ContentItem::State(StateFrame::StateSnapshot { state }) => state.get("permission"),
            ContentItem::State(StateFrame::StatePatch { set, .. }) => set.get("permission"),
            _ => None,
        })
        .expect("initial or changed top-level permission")
}
fn row<'a>(content: &'a Content, tool: &str) -> &'a Value {
    permission(content)
        .as_array()
        .unwrap()
        .iter()
        .find(|row| {
            row["tools"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == tool)
        })
        .unwrap()
}
fn has_permission_patch_after(previous: &Content, next: &Content) -> bool {
    next.items[previous.items.len()..].iter().any(|item| matches!(item, ContentItem::State(StateFrame::StatePatch { set, .. }) if set.contains_key("permission")))
}
fn compiled(content: &Content, tool: &str) -> CompiledToolPolicy {
    let mut value = row(content, tool).clone();
    value.as_object_mut().unwrap().remove("tools");
    value.as_object_mut().unwrap().remove("owners");
    serde_json::from_value(value).unwrap()
}
#[tokio::test]
async fn concrete_tool_snapshot_mode_override_constraints_and_recovery_preserve_frozen_epoch() {
    let (session, _, model) = setup(Arc::new(Policy));
    session.submit("plan").await.unwrap();
    let checkpoint = session.checkpoint().await.unwrap();
    session.select_mode("build").await.unwrap();
    session.submit("build").await.unwrap();
    edit(&session, |config| {
        config.overrides = vec![patch_rule(Action::Allow)]
    })
    .unwrap();
    session.submit("override").await.unwrap();
    edit(&session, |_| {}).unwrap();
    session.submit("same policy").await.unwrap();
    let mut conditional = patch_rule(Action::Allow);
    conditional.args = vec![
        ArgumentPredicate {
            pointer: "/path".into(),
            matcher: Matcher::Glob("src/*".into()),
        },
        ArgumentPredicate {
            pointer: "/content".into(),
            matcher: Matcher::Exact("approved".into()),
        },
    ];
    edit(&session, |config| {
        config.overrides = vec![conditional.clone()]
    })
    .unwrap();
    session.submit("conditional").await.unwrap();
    session.recover(checkpoint).await.unwrap();
    session.submit("restored").await.unwrap();
    {
        let requests = model.0.lock().unwrap();
        assert_eq!(row(&requests[0], "patch")["kind"], "static");
        assert_eq!(row(&requests[0], "patch")["action"], "deny");
        for tool in ["read", "sleep", "job"] {
            assert_eq!(row(&requests[0], tool)["action"], "allow");
        }
        assert_eq!(row(&requests[1], "patch")["action"], "ask");
        assert_eq!(row(&requests[2], "patch")["action"], "allow");
        assert!(!has_permission_patch_after(&requests[2], &requests[3]));
        let complex = row(&requests[4], "patch");
        assert_eq!(complex["kind"], "conditional");
        assert_eq!(complex["default_action"], "ask");
        assert_eq!(complex["branches"][0]["action"], "allow");
        assert_eq!(
            complex["branches"][0]["args"],
            serde_json::to_value(&conditional.args).unwrap()
        );
        for (request, fallback) in [(&requests[4], Action::Ask), (&requests[5], Action::Deny)] {
            let policy = compiled(request, "patch");
            assert_eq!(
                evaluate_compiled(&policy, &json!({"path":"src/a","content":"approved"})).unwrap(),
                Action::Allow
            );
            assert_eq!(
                evaluate_compiled(&policy, &json!({"path":"src/a","content":false})).unwrap(),
                fallback
            );
        }
        for request in requests.iter() {
            assert_eq!(request.system, requests[0].system);
            assert_eq!(request.tools, requests[0].tools);
            assert_eq!(
                request
                    .items
                    .iter()
                    .filter(|item| matches!(
                        item,
                        ContentItem::State(StateFrame::StateSnapshot { .. })
                    ))
                    .count(),
                1
            );
            for item in &request.items {
                if let ContentItem::State(StateFrame::StateSnapshot { state }) = item {
                    assert!(state["session"].get("agent").is_none());
                }
            }
        }
    }
    assert!(session.state_handle().extract("permission").is_none());
    session.close().await.unwrap();
}
#[tokio::test]
async fn parent_denial_and_constrained_ceiling_cannot_advertise_unconditional_allow() {
    let parent = Session::builder_arc(
        Arc::new(Model::default()),
        Runtime::new(vec![], Arc::new(Policy)).unwrap(),
    )
    .permissions(PermissionConfig {
        overrides: vec![rule(Action::Allow)],
        ..Default::default()
    })
    .build()
    .unwrap();
    let (session, _, model) =
        setup_with_parent(Arc::new(Policy), Some(parent.permission_controller()));
    session.select_mode("build").await.unwrap();
    edit(&session, |config| {
        config.overrides = vec![patch_rule(Action::Allow)]
    })
    .unwrap();
    session.submit("allowed").await.unwrap();
    edit(&parent, |config| {
        config.overrides = vec![patch_rule(Action::Deny)]
    })
    .unwrap();
    session.submit("revoked").await.unwrap();
    let mut constrained = patch_rule(Action::Deny);
    constrained.args = vec![ArgumentPredicate {
        pointer: "/path".into(),
        matcher: Matcher::Glob("private/*".into()),
    }];
    edit(&parent, |config| {
        config.overrides = vec![rule(Action::Allow), constrained]
    })
    .unwrap();
    session.submit("argument ceiling").await.unwrap();
    let mut child_guard = patch_rule(Action::Ask);
    child_guard.args = vec![ArgumentPredicate {
        pointer: "/content".into(),
        matcher: Matcher::Exact("review".into()),
    }];
    edit(&session, |config| {
        config.overrides = vec![patch_rule(Action::Allow), child_guard]
    })
    .unwrap();
    session.submit("composed conditional owners").await.unwrap();
    {
        let requests = model.0.lock().unwrap();
        assert_eq!(row(&requests[0], "patch")["action"], "allow");
        assert_eq!(row(&requests[1], "patch")["action"], "deny");
        let policy = compiled(&requests[2], "patch");
        assert_eq!(
            evaluate_compiled(&policy, &json!({"path":"private/a"})).unwrap(),
            Action::Deny
        );
        assert_eq!(
            evaluate_compiled(&policy, &json!({"path":"src/a"})).unwrap(),
            Action::Allow
        );
        assert!(row(&requests[2], "patch").get("owners").is_none());
        assert_eq!(row(&requests[3], "patch")["kind"], "all_of");
        let composed = compiled(&requests[3], "patch");
        for (arguments, expected) in [
            (json!({"path":"private/a","content":"review"}), Action::Deny),
            (json!({"path":"src/a","content":"review"}), Action::Ask),
            (json!({"path":"src/a","content":"ordinary"}), Action::Allow),
        ] {
            assert_eq!(evaluate_compiled(&composed, &arguments).unwrap(), expected);
        }
    }
    session.close().await.unwrap();
    parent.close().await.unwrap();
}
#[tokio::test]
async fn arbitrary_async_adapter_is_not_evaluated_by_model_projection() {
    let (session, _, model) = setup(Arc::new(Unknown));
    session.select_mode("build").await.unwrap();
    session.submit("hint only").await.unwrap();
    assert_eq!(row(&model.0.lock().unwrap()[0], "read")["action"], "allow");
    session.close().await.unwrap();
}

#[tokio::test]
async fn model_groups_equivalent_tools_while_management_rows_keep_exact_owner_witnesses() {
    let (session, _, model) = setup(Arc::new(Policy));
    session.submit("inspect grouped projection").await.unwrap();
    {
        let requests = model.0.lock().unwrap();
        let groups = permission(&requests[0]).as_array().unwrap();
        let allowed = groups
            .iter()
            .find(|row| row["kind"] == "static" && row["action"] == "allow")
            .unwrap();
        assert_eq!(allowed["tools"], json!(["job", "read", "sleep"]));
        let communication = groups
            .iter()
            .filter(|group| {
                group["tools"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|tool| tool == "send_message")
            })
            .collect::<Vec<_>>();
        assert_eq!(communication.len(), 1);
        assert_eq!(communication[0]["tools"], json!(["send_message"]));
        let policy = compiled(&requests[0], "send_message");
        assert_eq!(
            evaluate_compiled(&policy, &json!({"intent":"send"})).unwrap(),
            Action::Ask
        );
        assert_eq!(
            evaluate_compiled(&policy, &json!({"intent":"reply"})).unwrap(),
            Action::Allow
        );
        assert!(
            groups
                .iter()
                .all(|row| row.get("owners").is_none() && row.get("tool").is_none())
        );
    }
    let inventory = session.permission_inventory().unwrap();
    for mode in inventory["modes"].as_array().unwrap() {
        let rows = mode["compiled"].as_array().unwrap();
        let fixture_tools = ["job", "patch", "read", "sleep"];
        let fixture_rows = rows
            .iter()
            .filter(|row| fixture_tools.contains(&row["tool"]["value"].as_str().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(fixture_rows.len(), fixture_tools.len());
        for name in fixture_tools {
            assert_eq!(
                fixture_rows
                    .iter()
                    .filter(|row| row["tool"]["value"] == name)
                    .count(),
                1
            );
        }
        assert_eq!(
            rows.iter()
                .filter(|row| row["tool"]["value"] == "send_message")
                .count(),
            1
        );
        assert_eq!(rows.len(), fixture_rows.len() + 1);
        for row in rows {
            assert_eq!(row["tool"]["kind"], "exact");
            assert!(
                row["owners"]
                    .as_array()
                    .is_some_and(|owners| !owners.is_empty())
            );
            assert!(row.get("tools").is_none());
        }
    }
    session.close().await.unwrap();
}
