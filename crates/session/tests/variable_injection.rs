//! Visible injection changes the permission view without rewriting model history.
use async_trait::async_trait;
use microcrab_auditor::{Auditor, VariableView};
use microcrab_protocol::{Response, ToolCall, ToolDefinition};
use microcrab_session::context::ContentItem;
use microcrab_session::{Client, Error, Session};
use microcrab_session::{
    policy::SessionPermission,
    runtime::{
        Runtime, ToolRegistration,
        tool::{Tool, ToolContext},
    },
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

struct Script {
    calls: Vec<ToolCall>,
    attempts: std::sync::atomic::AtomicUsize,
}
#[async_trait]
impl Client for Script {
    async fn complete(
        &self,
        _: &microcrab_session::context::Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let attempt = self
            .attempts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: if attempt == 0 {
                self.calls.clone()
            } else {
                vec![]
            },
            usage: Default::default(),
        })
    }
}
struct Effects {
    permission: Mutex<Vec<Value>>,
    execution: Mutex<Vec<Value>>,
}
struct Inspect(Arc<Effects>);
#[async_trait]
impl SessionPermission for Inspect {
    async fn evaluate(
        &self,
        _: &microcrab_permission::PolicySnapshot,
        _: &microcrab_permission::PermissionRequest,
    ) -> Result<microcrab_permission::Decision, microcrab_session::policy::PermissionError> {
        panic!("adapter evaluate is not authorization authority")
    }
    fn features(&self) -> microcrab_permission::PermissionFeatures {
        microcrab_permission::PermissionFeatures {
            interactive_ask: true,
            ..Default::default()
        }
    }
    async fn ask(
        &self,
        request: &microcrab_permission::PermissionRequest,
    ) -> Result<microcrab_permission::AskReply, microcrab_session::policy::PermissionError> {
        self.0
            .permission
            .lock()
            .unwrap()
            .push(request.arguments.clone());
        Ok(microcrab_permission::AskReply {
            action: microcrab_permission::Action::Allow,
            reason: "synthetic fixture approval".into(),
        })
    }
}
struct Effect(Arc<Effects>);
#[async_trait]
impl Tool for Effect {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "synthetic_effect".into(),
            description: "Record an explicitly synthetic execution fixture".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn execute(
        &self,
        arguments: Value,
        _: ToolContext,
    ) -> Result<microcrab_session::runtime::execution::ToolOutput, Error> {
        self.0.execution.lock().unwrap().push(arguments);
        Ok(microcrab_session::runtime::execution::ToolOutput::text(
            "done",
        ))
    }
}
fn call(arguments: Value) -> ToolCall {
    ToolCall {
        provider_metadata: None,
        id: "model-call-var".into(),
        name: "synthetic_effect".into(),
        arguments,
    }
}
fn permissions() -> microcrab_permission::PermissionConfig {
    microcrab_permission::PermissionConfig {
        review: false,
        overrides: vec![
            serde_json::from_value(
                json!({"tool":{"kind":"exact","value":"synthetic_effect"},"action":"ask"}),
            )
            .unwrap(),
        ],
        ..Default::default()
    }
}

#[tokio::test]
async fn visible_expansion_reaches_permission_and_execution_without_rewriting_history() {
    let authored = json!({"who":"{{var:profile:/name}}","all":"{{var:profile}}"});
    let effects = Arc::new(Effects {
        permission: Mutex::new(vec![]),
        execution: Mutex::new(vec![]),
    });
    let runtime = Runtime::new(
        vec![ToolRegistration::External(Arc::new(Effect(
            effects.clone(),
        )))],
        Arc::new(Inspect(effects.clone())),
    )
    .unwrap();
    let session = Session::builder_arc(
        Arc::new(Script {
            calls: vec![call(authored.clone())],
            attempts: std::sync::atomic::AtomicUsize::new(0),
        }),
        runtime,
    )
    .permissions(permissions())
    .build()
    .unwrap();
    session
        .state_handle()
        .inject("profile", json!({"name":"ada","n":2}))
        .await
        .unwrap();
    assert_eq!(session.submit("inject").await.unwrap().content, "done");
    assert_eq!(
        effects.permission.lock().unwrap().as_slice(),
        &[json!({"who":"ada","all":{"name":"ada","n":2}})]
    );
    assert_eq!(
        effects.execution.lock().unwrap().as_slice(),
        &[json!({"who":"ada","all":{"name":"ada","n":2}})]
    );
    let messages = session.messages();
    let stored = messages.iter().find_map(|item| match item {
        ContentItem::Assistant { tool_calls, .. } if !tool_calls.is_empty() => Some(tool_calls),
        _ => None,
    });
    assert_eq!(stored, Some(&vec![call(authored)]));
}

#[tokio::test]
async fn ordinary_arguments_over_one_mib_reach_permission_and_execution_unchanged() {
    let authored = json!({"payload": "x".repeat(1024 * 1024 + 16)});
    assert!(serde_json::to_vec(&authored).unwrap().len() > 1024 * 1024);
    let effects = Arc::new(Effects {
        permission: Mutex::new(vec![]),
        execution: Mutex::new(vec![]),
    });
    let runtime = Runtime::new(
        vec![ToolRegistration::External(Arc::new(Effect(
            effects.clone(),
        )))],
        Arc::new(Inspect(effects.clone())),
    )
    .unwrap();
    let session = Session::builder_arc(
        Arc::new(Script {
            calls: vec![call(authored.clone())],
            attempts: std::sync::atomic::AtomicUsize::new(0),
        }),
        runtime,
    )
    .permissions(permissions())
    .build()
    .unwrap();
    assert_eq!(session.state().limits.max_context_bytes, 64 * 1024 * 1024);
    assert_eq!(
        session
            .submit("pass ordinary arguments")
            .await
            .unwrap()
            .content,
        "done"
    );
    assert_eq!(
        effects.permission.lock().unwrap().as_slice(),
        std::slice::from_ref(&authored)
    );
    assert_eq!(effects.execution.lock().unwrap().len(), 1);
    assert_eq!(
        effects.execution.lock().unwrap().as_slice(),
        std::slice::from_ref(&authored)
    );
    let messages = session.messages();
    let stored = messages.iter().find_map(|item| match item {
        ContentItem::Assistant { tool_calls, .. } if !tool_calls.is_empty() => Some(tool_calls),
        _ => None,
    });
    assert_eq!(stored, Some(&vec![call(authored)]));
    session.close().await.unwrap();
}

async fn assert_visible_secret_crossing_stays_literal(fragment: &str, crossing: &str) {
    let authored = json!({"crossing": crossing, "direct": "{{secret:TOKEN}}"});
    let effects = Arc::new(Effects {
        permission: Mutex::new(vec![]),
        execution: Mutex::new(vec![]),
    });
    let runtime = Runtime::new(
        vec![ToolRegistration::External(Arc::new(Effect(
            effects.clone(),
        )))],
        Arc::new(Inspect(effects.clone())),
    )
    .unwrap();
    let session = Session::builder_arc(
        Arc::new(Script {
            calls: vec![call(authored.clone())],
            attempts: std::sync::atomic::AtomicUsize::new(0),
        }),
        runtime,
    )
    .permissions(permissions())
    .build()
    .unwrap();
    let state = session.state_handle();
    state.inject("fragment", json!(fragment)).await.unwrap();
    state
        .prepare_secret_write(std::collections::BTreeMap::from([(
            "TOKEN".into(),
            "fixture-credential".into(),
        )]))
        .await
        .unwrap()
        .commit()
        .unwrap();
    assert_eq!(
        session
            .submit("cross insertion boundary")
            .await
            .unwrap()
            .content,
        "done"
    );
    assert_eq!(
        effects.permission.lock().unwrap().as_slice(),
        &[json!({"crossing": "{{secret:TOKEN}}", "direct": "{{secret:TOKEN}}"})]
    );
    assert_eq!(effects.execution.lock().unwrap().len(), 1);
    assert_eq!(
        effects.execution.lock().unwrap().as_slice(),
        &[json!({"crossing": "{{secret:TOKEN}}", "direct": "fixture-credential"})]
    );
    let messages = session.messages();
    let stored = messages.iter().find_map(|item| match item {
        ContentItem::Assistant { tool_calls, .. } if !tool_calls.is_empty() => Some(tool_calls),
        _ => None,
    });
    assert_eq!(stored, Some(&vec![call(authored)]));
    session.close().await.unwrap();
}

#[tokio::test]
async fn nonempty_visible_insertion_cannot_form_an_active_secret_placeholder() {
    assert_visible_secret_crossing_stays_literal("cret", "{{se{{var:string:fragment}}:TOKEN}}")
        .await;
}

#[tokio::test]
async fn empty_visible_insertion_cannot_form_an_active_secret_placeholder() {
    assert_visible_secret_crossing_stays_literal("", "{{se{{var:string:fragment}}cret:TOKEN}}")
        .await;
}

#[tokio::test]
async fn embedded_native_placeholder_is_a_tool_error_before_permission() {
    let effects = Arc::new(Effects {
        permission: Mutex::new(vec![]),
        execution: Mutex::new(vec![]),
    });
    let runtime = Runtime::new(
        vec![ToolRegistration::External(Arc::new(Effect(
            effects.clone(),
        )))],
        Arc::new(Inspect(effects.clone())),
    )
    .unwrap();
    let session = Session::builder_arc(
        Arc::new(Script {
            calls: vec![call(json!({"text":"id {{var:name}}"}))],
            attempts: std::sync::atomic::AtomicUsize::new(0),
        }),
        runtime,
    )
    .permissions(permissions())
    .build()
    .unwrap();
    session
        .state_handle()
        .inject("name", json!("ada"))
        .await
        .unwrap();
    session.submit("reject").await.unwrap();
    let messages = session.messages();
    let tool_result = messages.iter().find_map(|item| match item {
        ContentItem::Tool { content, .. } => Some(content.clone()),
        _ => None,
    });
    assert!(
        tool_result
            .as_deref()
            .unwrap_or("")
            .contains("entire value"),
        "{tool_result:?}"
    );
    assert!(effects.permission.lock().unwrap().is_empty());
    assert!(effects.execution.lock().unwrap().is_empty());
}
