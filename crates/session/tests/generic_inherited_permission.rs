//! Inherited tool admission questions stay owned and cancellable without elapsed tool deadlines.
use async_trait::async_trait;
use microcrab_permission::{
    Action, AskReply, Decision, Permission, PermissionConfig, PermissionFeatures,
    PermissionRequest, PolicySnapshot,
};
use microcrab_protocol::{Response, ToolCall, ToolDefinition};
use microcrab_session::{
    Auditor, Client, Error, Limits, Session, VariableView,
    context::Content,
    policy::{PermissionError, SessionPermission},
    runtime::{
        Runtime, ToolRegistration,
        execution::ToolOutput,
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
use tokio::sync::{Notify, oneshot};
use tokio_util::sync::CancellationToken;

struct Once(AtomicUsize);
#[async_trait]
impl Client for Once {
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
            content: "done".into(),
            tool_calls: if first {
                vec![ToolCall {
                    provider_metadata: None,
                    id: "actual-operation".into(),
                    name: "scoped_write".into(),
                    arguments: json!({}),
                }]
            } else {
                vec![]
            },
            usage: Default::default(),
        })
    }
}
struct HeldAsk {
    entered: Notify,
    requests: Mutex<Vec<PermissionRequest>>,
    answer: Mutex<Option<oneshot::Receiver<Action>>>,
}
#[async_trait]
impl SessionPermission for HeldAsk {
    fn features(&self) -> PermissionFeatures {
        PermissionFeatures {
            interactive_ask: true,
            remembered_grants: false,
            live_edit: true,
        }
    }
    async fn evaluate(
        &self,
        policy: &PolicySnapshot,
        request: &PermissionRequest,
    ) -> Result<Decision, PermissionError> {
        microcrab_permission::evaluate(policy, request)
            .map_err(|error| PermissionError::InvalidConfiguration(error.to_string()))
    }
    async fn ask(&self, request: &PermissionRequest) -> Result<AskReply, PermissionError> {
        self.requests.lock().unwrap().push(request.clone());
        let answer = self
            .answer
            .lock()
            .unwrap()
            .take()
            .expect("one actual inherited question");
        self.entered.notify_one();
        let action = answer.await.map_err(|_| PermissionError::Cancelled)?;
        Ok(AskReply {
            action,
            reason: "explicit parent owner answer".into(),
        })
    }
}
struct Effect(Arc<AtomicUsize>);
#[async_trait]
impl Tool for Effect {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "scoped_write".into(),
            description: "One tool admission before effect".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn execute(&self, _: Value, context: ToolContext) -> Result<ToolOutput, Error> {
        context.ensure_admitted(None).map_err(|error| match error {
            PermissionError::Cancelled => Error::Cancelled,
            _ => Error::Denied("scoped_write".into()),
        })?;
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutput::text("authorized effect"))
    }
}
fn rule(action: Action) -> Permission {
    Permission {
        agent: None,
        mode: None,
        tool: Some(microcrab_permission::Matcher::Exact("scoped_write".into())),
        args: vec![],
        action,
    }
}
fn fixture() -> (
    Session,
    Session,
    Arc<HeldAsk>,
    oneshot::Sender<Action>,
    Arc<AtomicUsize>,
) {
    let (answer, receive) = oneshot::channel();
    let policy = Arc::new(HeldAsk {
        entered: Notify::new(),
        requests: Mutex::new(vec![]),
        answer: Mutex::new(Some(receive)),
    });
    let parent = Session::builder(
        Once(AtomicUsize::new(0)),
        Runtime::new(vec![], policy.clone()).unwrap(),
    )
    .session_id("operation-question-parent")
    .permissions(PermissionConfig {
        review: false,
        overrides: vec![rule(Action::Ask)],
        ..Default::default()
    })
    .build()
    .unwrap();
    let effects = Arc::new(AtomicUsize::new(0));
    let runtime = Runtime::new(
        vec![ToolRegistration::External(Arc::new(Effect(
            effects.clone(),
        )))],
        Arc::new(AllowTools::new(["scoped_write"])),
    )
    .unwrap();
    let child = Session::builder(Once(AtomicUsize::new(0)), runtime)
        .session_id("operation-question-child")
        .permission_parent(parent.permission_controller())
        .permissions(PermissionConfig {
            overrides: vec![rule(Action::Allow)],
            ..Default::default()
        })
        .limits(Limits {
            ..Default::default()
        })
        .build()
        .unwrap();
    (parent, child, policy, answer, effects)
}
#[tokio::test]
async fn inherited_tool_admission_question_can_outlive_tool_budget_then_apply_the_owner_answer() {
    let (parent, child, policy, answer, effects) = fixture();
    let worker_session = child.clone();
    let worker = tokio::spawn(async move {
        worker_session
            .submit("perform the authorized operation")
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), policy.entered.notified())
        .await
        .expect("actual parent operation Ask must be reached");
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !worker.is_finished(),
        "the inherited permission question has no elapsed tool or outer-turn deadline"
    );
    assert_eq!(
        effects.load(Ordering::SeqCst),
        0,
        "waiting for the owner never bypasses the parent ceiling"
    );
    answer.send(Action::Allow).unwrap();
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    {
        let requests = policy.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].tool, "scoped_write");
        assert_eq!(requests[0].call_id, "actual-operation");
        assert_eq!(requests[0].arguments, json!({}));
        assert_eq!(requests[0].session_id, "operation-question-child");
    }
    child.close().await.unwrap();
    parent.close().await.unwrap();
}
#[tokio::test]
async fn cancelling_inherited_tool_question_releases_the_question_without_an_effect() {
    let (parent, child, policy, answer, effects) = fixture();
    let worker_session = child.clone();
    let worker =
        tokio::spawn(async move { worker_session.submit("wait for parent authority").await });
    tokio::time::timeout(Duration::from_secs(5), policy.entered.notified())
        .await
        .unwrap();
    child.interrupt();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap()
            .unwrap(),
        Err(Error::Cancelled)
    ));
    assert!(
        answer.send(Action::Allow).is_err(),
        "owner cancellation releases the actual pending question future"
    );
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert_eq!(policy.requests.lock().unwrap().len(), 1);
    child.close().await.unwrap();
    parent.close().await.unwrap();
}

#[tokio::test]
async fn parent_policy_revision_during_its_ask_discards_the_stale_allow() {
    let (parent, child, policy, answer, effects) = fixture();
    let running = child.clone();
    let worker = tokio::spawn(async move { running.submit("pending parent decision").await });
    tokio::time::timeout(Duration::from_secs(5), policy.entered.notified())
        .await
        .unwrap();
    let before = parent.permission_controller().snapshot().unwrap();
    let mut config = before.config;
    config.overrides = vec![rule(Action::Deny)];
    parent.replace_permissions(before.revision, config).unwrap();
    answer.send(Action::Allow).unwrap();
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert_eq!(policy.requests.lock().unwrap().len(), 1);
    child.close().await.unwrap();
    parent.close().await.unwrap();
}
