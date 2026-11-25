//! Restoring old epochs may install new native builtins without silently
//! advertising them. Provider/WASM definitions still match the frozen catalog.
use async_trait::async_trait;
use microcrab_auditor::{Auditor, VariableView};
use microcrab_protocol::{Response, ToolCall};
use microcrab_session::{
    Client, Limits, Session,
    context::Content,
    runtime::{BuiltinTool, Runtime, ToolRegistration, journal::MemoryJournal, tool::DenyAll},
};
use serde_json::json;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
struct Answer {
    hidden: bool,
}
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
            content: "answer".into(),
            tool_calls: if self.hidden {
                vec![ToolCall {
                    provider_metadata: None,
                    id: "hidden".into(),
                    name: "expand".into(),
                    arguments: json!({"reference":"not-installed-in-this-epoch"}),
                }]
            } else {
                vec![]
            },
            usage: Default::default(),
        })
    }
}
struct Available;
#[async_trait]
impl microcrab_session::DelegateExecutor for Available {
    async fn execute(
        &self,
        _: &str,
        _: &str,
        _: Option<&str>,
        _: &str,
        _: Option<u64>,
        _: microcrab_session::runtime::tool::ToolContext,
    ) -> Result<String, microcrab_session::Error> {
        Ok("child".into())
    }
}
fn catalog() -> Vec<microcrab_agent_types::AgentInfo> {
    vec![microcrab_agent_types::AgentInfo {
        tool_media_output_bytes: Default::default(),
        name: "child".into(),
        modes: vec![microcrab_agent_types::AgentMode {
            description: String::new(),
            prompt: String::new(),
            name: "review".into(),
            role: microcrab_agent_types::AgentRole::SubAgent,
            permissions: vec![],
        }],
        tools: vec![],
        variables: Default::default(),
    }]
}
fn has_model_permission(session: &Session, name: &str) -> bool {
    let mut state = json!({});
    for item in session.messages() {
        if let microcrab_session::context::ContentItem::State(frame) = item {
            microcrab_transport::apply_state_patch(&mut state, &frame).unwrap();
        }
    }
    state["permission"].as_array().unwrap().iter().any(|row| {
        row["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value == name)
    })
}
fn job_runtime() -> Runtime {
    Runtime::new(
        vec![ToolRegistration::Builtin(BuiltinTool::Job)],
        Arc::new(DenyAll),
    )
    .unwrap()
}

#[tokio::test]
async fn unsupported_job_catalog_is_rejected_without_rewriting_frozen_context() {
    let journal = MemoryJournal::new();
    let source = Session::builder(Answer { hidden: false }, job_runtime())
        .journal(journal.clone())
        .build()
        .unwrap();
    source.submit("current epoch").await.unwrap();
    let original = journal.latest().unwrap();
    let before = serde_json::to_value(source.context_snapshot().unwrap()).unwrap();
    let current = BuiltinTool::Job.definition();
    // Reconstruct the previous six-action declaration: it understood bounded
    // public reads, but no typed private redirect or selector contract.
    let mut old_shape = current.clone();
    old_shape.parameters["properties"]["action"]["enum"] =
        json!(["status", "read", "wait", "write", "cancel", "release"]);
    let rules = old_shape.parameters.as_object_mut().unwrap();
    rules.remove("if");
    rules.remove("then");
    let properties = rules.get_mut("properties").unwrap()["args"]["properties"]
        .as_object_mut()
        .unwrap();
    for name in ["channel", "expected_mime", "variable"] {
        properties.remove(name);
    }
    // Restore the exact image-only redirect description and MIME enum.
    let mut raster_redirect = current.clone();
    let action = &mut raster_redirect.parameters["properties"]["action"]["description"];
    *action = json!(action.as_str().unwrap().replace(
        "shares complete successful captured bytes",
        "shares a complete successful captured image"
    ));
    raster_redirect.parameters["properties"]["args"]["properties"]["expected_mime"] = json!({
        "type":"string", "enum":["image/png","image/jpeg","image/gif","image/webp"],
        "description":"Redirect-only exact declared MIME. The image is validated separately."
    });
    let mut changed_description = current;
    changed_description
        .description
        .push_str(" unsupported catalog");
    for unsupported in [old_shape, raster_redirect, changed_description] {
        let mut recover = original.clone();
        recover.context.tools = vec![unsupported];
        let frozen = serde_json::to_value(&recover.context).unwrap();
        let result = Session::restore(
            Session::builder(Answer { hidden: false }, job_runtime()),
            recover.clone(),
        )
        .await;
        assert!(
            matches!(result, Err(microcrab_session::Error::Config(message))
            if message.as_str() == "restored job catalog is unsupported; create a new Session with current tool definitions")
        );
        assert_eq!(serde_json::to_value(&recover.context).unwrap(), frozen);
        assert_eq!(
            serde_json::to_value(source.context_snapshot().unwrap()).unwrap(),
            before
        );
    }
    source.close().await.unwrap();
}

#[test]
fn current_job_descriptor_has_one_complete_bounded_output_contract() {
    let definition = BuiltinTool::Job.definition();
    assert!(
        definition.parameters["properties"]["id"]["description"]
            .as_str()
            .unwrap()
            .contains("decoded once")
    );
    assert!(
        definition.parameters["properties"]["args"]["properties"]["max_bytes"]["description"]
            .as_str()
            .unwrap()
            .contains("reads never consume or release")
    );
    assert_eq!(
        definition.parameters["properties"]["action"]["enum"],
        json!([
            "status", "read", "wait", "write", "cancel", "release", "redirect"
        ])
    );
    assert!(
        definition.parameters["properties"]["action"]["description"]
            .as_str()
            .unwrap()
            .contains("After collecting needed output, explicitly release a terminal execution")
    );
    assert_eq!(
        definition.parameters["properties"]["args"]["properties"]["max_bytes"]["minimum"],
        json!(1)
    );
    assert_eq!(
        definition.parameters["properties"]["args"]["properties"]["max_bytes"]["maximum"],
        json!(16777216)
    );
}

#[tokio::test]
async fn current_job_descriptor_restores_exactly() {
    let journal = MemoryJournal::new();
    let source = Session::builder(Answer { hidden: false }, job_runtime())
        .journal(journal.clone())
        .build()
        .unwrap();
    source.submit("current descriptor").await.unwrap();
    let recover = journal.latest().unwrap();
    let frozen_tools = source.context_snapshot().unwrap().tools;
    assert_eq!(recover.context.tools, frozen_tools);
    assert_eq!(
        frozen_tools
            .iter()
            .filter(|tool| tool.name == "job")
            .collect::<Vec<_>>(),
        vec![&BuiltinTool::Job.definition()]
    );
    assert_eq!(
        frozen_tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        ["job", "send_message"]
    );
    let frozen = serde_json::to_value(&recover.context).unwrap();
    let restored = Session::restore(
        Session::builder(Answer { hidden: false }, job_runtime()),
        recover,
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::to_value(restored.context_snapshot().unwrap()).unwrap(),
        frozen
    );
    assert_eq!(restored.context_snapshot().unwrap().tools, frozen_tools);
    restored.close().await.unwrap();
    source.close().await.unwrap();
}

fn runtime(expand: bool) -> Runtime {
    let mut tools = vec![ToolRegistration::Builtin(BuiltinTool::Sleep)];
    if expand {
        tools.push(ToolRegistration::Builtin(BuiltinTool::Expand));
        tools.push(ToolRegistration::Builtin(BuiltinTool::Delegate));
    }
    Runtime::new(tools, Arc::new(DenyAll))
        .unwrap()
        .with_delegate_executor(Arc::new(Available))
}
#[tokio::test]
async fn old_frozen_catalog_restores_and_adopts_new_builtin_only_on_explicit_update() {
    let journal = MemoryJournal::new();
    let old = Session::builder(Answer { hidden: false }, runtime(false))
        .journal(journal.clone())
        .build()
        .unwrap();
    old.submit("old epoch").await.unwrap();
    let recover = journal.latest().unwrap();
    let epoch = recover.context.epoch;
    let restored = Session::restore(
        Session::builder(Answer { hidden: false }, runtime(true))
            .catalog(catalog())
            .limits(Limits {
                max_tool_output_bytes: 64 * 1024,
                ..Limits::default()
            }),
        recover.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        restored.context_snapshot().unwrap().tools,
        recover.context.tools
    );
    assert_eq!(restored.context_snapshot().unwrap().epoch, epoch);
    assert_eq!(restored.state().limits.max_tool_output_bytes, 16 * 1024);
    assert!(restored.state_view().unwrap().get("permission").is_none());
    let list = restored
        .permission_list(microcrab_session::policy::PermissionListView::Current)
        .unwrap();
    let microcrab_session::policy::PermissionList::Current { current } = list else {
        unreachable!("the requested view was Current");
    };
    assert!(current.tools.iter().any(|tool| tool.name == "expand"));
    assert!(!has_model_permission(&restored, "expand"));
    assert!(!has_model_permission(&restored, "delegate"));
    assert!(
        restored
            .state_view()
            .unwrap()
            .get("delegatable_agents")
            .is_none()
    );
    restored.replace_external_tools(vec![]).await.unwrap();
    assert_eq!(
        restored.context_snapshot().unwrap().tools,
        recover.context.tools
    );
    assert_eq!(restored.context_snapshot().unwrap().epoch, epoch);
    assert!(!has_model_permission(&restored, "expand"));
    assert!(!has_model_permission(&restored, "delegate"));
    restored
        .submit("materialize explicitly updated epoch")
        .await
        .unwrap();
    let next = restored.context_snapshot().unwrap();
    assert!(next.tools.iter().any(|tool| tool.name == "expand"));
    assert!(!next.pending_epoch);
    assert_eq!(next.epoch, epoch + 1);
    assert!(has_model_permission(&restored, "expand"));
    assert!(has_model_permission(&restored, "delegate"));
    assert_eq!(
        restored.state_view().unwrap()["delegatable_agents"][0]["name"],
        "child"
    );
    assert_eq!(recover.context.epoch, epoch);
    assert!(
        !recover
            .context
            .tools
            .iter()
            .any(|tool| tool.name == "expand")
    );
    restored.close().await.unwrap();
    old.close().await.unwrap();
}
#[tokio::test]
async fn installed_but_unadvertised_builtin_cannot_be_called_by_the_model() {
    let journal = MemoryJournal::new();
    let old = Session::builder(Answer { hidden: false }, runtime(false))
        .journal(journal.clone())
        .build()
        .unwrap();
    old.submit("old epoch").await.unwrap();
    let restored = Session::restore(
        Session::builder(Answer { hidden: true }, runtime(true)),
        journal.latest().unwrap(),
    )
    .await
    .unwrap();
    assert!(matches!(
        restored.submit("attempt hidden builtin").await,
        Err(microcrab_session::Error::Protocol(_))
    ));
    assert!(
        restored.executions().snapshot().is_empty(),
        "hidden implementation was never dispatched"
    );
    restored.close().await.unwrap();
    old.close().await.unwrap();
}
