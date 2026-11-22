//! Whole-tool admission, controller-owned approvals and revision fences.
use async_trait::async_trait;
use microcrab_permission::*;
use microcrab_protocol::{Response, ToolCall, ToolDefinition, Usage};
use microcrab_session::context::{Content, ContentItem};
use microcrab_session::policy::{PermissionController, PermissionError, SessionPermission};
use microcrab_session::runtime::execution::ToolOutput;
use microcrab_session::runtime::tool::{Tool, ToolContext};
use microcrab_session::runtime::{Runtime, ToolRegistration};
use microcrab_session::{Auditor, Client, Error, Session, VariableView};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

struct Policy {
    asks: AtomicUsize,
    reply: Option<Action>,
    unavailable: std::sync::atomic::AtomicBool,
    reply_delay_ms: std::sync::atomic::AtomicU64,
}
#[async_trait]
impl SessionPermission for Policy {
    fn features(&self) -> PermissionFeatures {
        PermissionFeatures {
            interactive_ask: !self.unavailable.load(Ordering::SeqCst),
            remembered_grants: false,
            live_edit: true,
        }
    }
    async fn evaluate(
        &self,
        policy: &PolicySnapshot,
        request: &PermissionRequest,
    ) -> Result<Decision, PermissionError> {
        evaluate(policy, request).map_err(|e| PermissionError::InvalidConfiguration(e.to_string()))
    }
    async fn ask(&self, _: &PermissionRequest) -> Result<AskReply, PermissionError> {
        self.asks.fetch_add(1, Ordering::SeqCst);
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(PermissionError::Unavailable);
        }
        let delay = self.reply_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        }
        match self.reply {
            Some(action) => Ok(AskReply {
                action,
                reason: "fixture".into(),
            }),
            None => std::future::pending().await,
        }
    }
}
struct Model {
    review: String,
    requests: Mutex<Vec<Content>>,
    pause: bool,
    entered: Notify,
    release: Notify,
}
#[async_trait]
impl Client for Model {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        cancel: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.requests.lock().unwrap().push(content.clone());
        if content.subagent.as_deref() == Some("action_review") {
            if self.pause {
                self.entered.notify_one();
                tokio::select! { _=cancel.cancelled()=>return Err(microcrab_transport::Error::Cancelled),_=self.release.notified()=>{} }
            }
            return Ok(response(&self.review));
        }
        if content
            .items
            .iter()
            .rev()
            .take_while(|v| !matches!(v, ContentItem::User { .. }))
            .any(|v| matches!(v, ContentItem::Tool { .. }))
        {
            return Ok(response("finished"));
        }
        let mut answer = response("");
        answer.tool_calls.push(ToolCall {
            provider_metadata: None,
            id: format!(
                "operation-{}",
                1 + content
                    .items
                    .iter()
                    .filter(|item| matches!(item, ContentItem::Tool { .. }))
                    .count()
            ),
            name: "write".into(),
            arguments: json!({"path":"/root/a","content":"new"}),
        });
        Ok(answer)
    }
}
fn response(text: &str) -> Response {
    Response {
        provider_metadata: None,
        content: text.into(),
        tool_calls: vec![],
        usage: Usage::default(),
    }
}
struct Effect {
    count: Arc<AtomicUsize>,
    pause: Option<Arc<(Notify, Notify)>>,
}
#[async_trait]
impl Tool for Effect {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "write".into(),
            description: "scoped effect fixture".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn execute(&self, arguments: Value, context: ToolContext) -> Result<ToolOutput, Error> {
        context
            .ensure_admitted_arguments(&arguments)
            .map_err(|_| Error::Denied("write".into()))?;
        if let Some(pause) = &self.pause {
            pause.0.notify_one();
            pause.1.notified().await;
        }
        // A continuation retains one admitted invocation across policy edits.
        context
            .ensure_admitted(None)
            .map_err(|_| Error::Denied("write".into()))?;
        self.count.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutput::text("written"))
    }
}
fn rule(tool: &str, action: Action) -> Permission {
    Permission {
        agent: None,
        mode: None,
        tool: Some(Matcher::Exact(tool.into())),
        args: vec![],
        action,
    }
}
fn setup_with_parent(
    parent: Option<Arc<PermissionController>>,
    review: &str,
    pause: bool,
    reply: Option<Action>,
    pause_effect: bool,
) -> (
    Session,
    Arc<PermissionController>,
    Arc<Model>,
    Arc<Policy>,
    Arc<AtomicUsize>,
) {
    let adapter = Arc::new(Policy {
        asks: AtomicUsize::new(0),
        unavailable: std::sync::atomic::AtomicBool::new(false),
        reply_delay_ms: std::sync::atomic::AtomicU64::new(0),
        reply,
    });
    let count = Arc::new(AtomicUsize::new(0));
    let runtime = Runtime::new(
        vec![ToolRegistration::Wasm(Arc::new(Effect {
            count: count.clone(),
            pause: pause_effect.then(|| Arc::new((Notify::new(), Notify::new()))),
        }))],
        adapter.clone(),
    )
    .unwrap();
    let controller = runtime.permission_controller();
    let config = PermissionConfig {
        overrides: vec![rule("write", Action::Ask)],
        ..Default::default()
    };
    let model = Arc::new(Model {
        review: review.into(),
        requests: Mutex::new(vec![]),
        pause,
        entered: Notify::new(),
        release: Notify::new(),
    });
    let mut builder = Session::builder_arc(model.clone(), runtime).permissions(config);
    if let Some(parent) = parent {
        builder = builder.permission_parent(parent);
    }
    let session = builder.build().unwrap();
    (session, controller, model, adapter, count)
}
fn setup(
    review: &str,
    pause: bool,
    reply: Option<Action>,
    pause_effect: bool,
) -> (
    Session,
    Arc<PermissionController>,
    Arc<Model>,
    Arc<Policy>,
    Arc<AtomicUsize>,
) {
    setup_with_parent(None, review, pause, reply, pause_effect)
}
fn edit(session: &Session, change: impl FnOnce(&mut PermissionConfig)) {
    let snapshot = session.permission_controller().snapshot().unwrap();
    let mut config = snapshot.config;
    change(&mut config);
    session
        .replace_permissions(snapshot.revision, config)
        .unwrap();
}
fn parent_session(adapter: Arc<dyn SessionPermission>, config: PermissionConfig) -> Session {
    let model = Arc::new(Model {
        review: String::new(),
        requests: Mutex::new(vec![]),
        pause: false,
        entered: Notify::new(),
        release: Notify::new(),
    });
    Session::builder_arc(model, Runtime::new(vec![], adapter).unwrap())
        .permissions(config)
        .build()
        .unwrap()
}
#[tokio::test(start_paused = true)]
async fn review_allow_is_once_per_tool_invocation_and_never_enters_conversation() {
    let (session, _, model, adapter, count) = setup(
        r#"{"action":"allow","reason":"bounded fixture"}"#,
        false,
        None,
        false,
    );
    let started = tokio::time::Instant::now();
    session.submit("private issuer history").await.unwrap();
    assert!(started.elapsed() >= std::time::Duration::from_secs(5));
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(adapter.asks.load(Ordering::SeqCst), 1);
    let requests = model.requests.lock().unwrap();
    let reviews = requests
        .iter()
        .filter(|r| r.subagent.as_deref() == Some("action_review"))
        .collect::<Vec<_>>();
    assert_eq!(reviews.len(), 1);
    let review = reviews[0];
    assert!(review.tools.is_empty());
    assert_eq!((review.turn_id, review.round), (None, None));
    assert_eq!(review.items.len(), 1);
    assert!(
        review
            .system
            .contains("<untrusted_user_input>\nprivate issuer history\n</untrusted_user_input>")
    );
    assert!(matches!(&review.items[0], ContentItem::User { parts, .. }
        if matches!(parts.as_slice(), [microcrab_protocol::InputPart::Text { text }]
            if text == "begin")));
    assert!(
        !serde_json::to_string(&session.messages())
            .unwrap()
            .contains("bounded fixture")
    );
    assert_eq!(session.state().turn_id, None);
}
#[tokio::test(start_paused = true)]
async fn conditional_review_runs_only_after_failed_predicates_with_ask_fallback() {
    for (expected, review_enabled, effect, reviews, asks) in [
        ("new", true, 1, 0, 0),
        ("other", true, 1, 1, 1),
        ("other", false, 0, 0, 1),
    ] {
        let (session, _, model, adapter, count) = setup(
            r#"{"action":"allow","reason":"exact conditional fallback"}"#,
            false,
            if review_enabled {
                None
            } else {
                Some(Action::Deny)
            },
            false,
        );
        let mut conditional = rule("write", Action::Allow);
        conditional.args = vec![ArgumentPredicate {
            pointer: "/content".into(),
            matcher: Matcher::Exact(expected.into()),
        }];
        edit(&session, |config| {
            config.review = review_enabled;
            config.overrides.push(conditional);
        });
        session.submit("run conditional operation").await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), effect);
        assert_eq!(adapter.asks.load(Ordering::SeqCst), asks);
        let requests = model.requests.lock().unwrap();
        let actual = requests
            .iter()
            .filter(|request| request.subagent.as_deref() == Some("action_review"))
            .collect::<Vec<_>>();
        assert_eq!(actual.len(), reviews);
        if let Some(review) = actual.first() {
            let [ContentItem::User { parts, .. }] = review.items.as_slice() else {
                panic!("review must contain only one request item");
            };
            let [microcrab_protocol::InputPart::Text { text }] = parts.as_slice() else {
                panic!("review request must be text");
            };
            assert_eq!(text, "begin");
            assert!(review.system.contains(
                "<untrusted_user_input>\nrun conditional operation\n</untrusted_user_input>"
            ));
            let rendered = microcrab_transport::render_ast(review).unwrap();
            assert_eq!(rendered.messages.len(), 2);
            assert!(
                matches!(&rendered.messages[1], microcrab_protocol::Message::User { parts } if parts.as_slice() == [microcrab_protocol::InputPart::Text { text: text.clone() }])
            );
            assert!(review.system.contains("Operation kind: tool"));
            assert!(review.system.contains("<untrusted_tool_name>\nwrite\n"));
            let (_, args) = review
                .system
                .split_once("<untrusted_tool_arguments>\n")
                .unwrap();
            let (args, _) = args.split_once("\n</untrusted_tool_arguments>").unwrap();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(args).unwrap(),
                json!({"path":"/root/a","content":"new"})
            );
            assert!(!review.system.contains("operation-1"));
            assert!(review.tools.is_empty());
        }
    }
}

#[tokio::test(start_paused = true)]
async fn deny_ask_malformed_and_disabled_review_fail_closed_or_use_explicit_adapter() {
    for (review, reply, allowed, asks) in [
        (r#"{"action":"deny","reason":"fixture"}"#, None, false, 1),
        (
            r#"{"action":"ask","reason":"fixture"}"#,
            Some(Action::Deny),
            false,
            1,
        ),
        (
            r#"{"action":"conditional","reason":"fixture"}"#,
            Some(Action::Deny),
            false,
            1,
        ),
        ("not JSON", Some(Action::Deny), false, 1),
        (
            r#"{"action":"allow","reason":"fixture","extra":true}"#,
            Some(Action::Deny),
            false,
            1,
        ),
        ("not JSON", Some(Action::Allow), true, 1),
        ("not JSON", Some(Action::Deny), false, 1),
    ] {
        let (session, _, model, adapter, count) = setup(review, false, reply, false);
        adapter.reply_delay_ms.store(1000, Ordering::SeqCst);
        session.submit("run").await.unwrap();
        assert_eq!(
            model
                .requests
                .lock()
                .unwrap()
                .iter()
                .filter(|request| request.subagent.as_deref() == Some("action_review"))
                .count(),
            1
        );
        assert_eq!(count.load(Ordering::SeqCst), usize::from(allowed));
        assert_eq!(adapter.asks.load(Ordering::SeqCst), asks);
    }
    let (session, controller, model, adapter, count) =
        setup("must not be called", false, None, false);
    let _snapshot = controller.snapshot().unwrap();
    adapter.unavailable.store(true, Ordering::SeqCst);
    edit(&session, |config| config.review = false);
    session.submit("run").await.unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert_eq!(adapter.asks.load(Ordering::SeqCst), 0);
    assert!(
        model
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|r| r.subagent.is_none())
    );
}
#[tokio::test(start_paused = true)]
async fn policy_revoke_during_review_cannot_execute_under_old_allow() {
    let (session, controller, model, _, count) = setup(
        r#"{"action":"allow","reason":"fixture"}"#,
        true,
        None,
        false,
    );
    let running = session.clone();
    let worker = tokio::spawn(async move { running.submit("run").await });
    tokio::time::timeout(std::time::Duration::from_secs(5), model.entered.notified())
        .await
        .expect("review must start while the user interaction remains unanswered");
    assert_eq!(count.load(Ordering::SeqCst), 0);
    let _snapshot = controller.snapshot().unwrap();
    edit(&session, |config| {
        config.overrides.push(rule("write", Action::Deny))
    });
    model.release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(5), worker)
        .await
        .expect("released or revoked work must settle")
        .unwrap()
        .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 0);
}
#[tokio::test(start_paused = true)]
async fn cancellation_during_review_stays_cancelled_without_dispatch() {
    let (session, _, model, _, count) = setup(
        r#"{"action":"allow","reason":"fixture"}"#,
        true,
        None,
        false,
    );
    let running = session.clone();
    let worker = tokio::spawn(async move { running.submit("run").await });
    tokio::time::timeout(std::time::Duration::from_secs(5), model.entered.notified())
        .await
        .expect("review must start while the user interaction remains unanswered");
    assert_eq!(count.load(Ordering::SeqCst), 0);
    session.interrupt();
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(5), worker)
            .await
            .expect("cancelled review must acknowledge cleanup")
            .unwrap(),
        Err(Error::Cancelled)
    ));
    assert_eq!(count.load(Ordering::SeqCst), 0);
}
#[tokio::test(start_paused = true)]
async fn explicit_user_deny_has_priority_over_an_automatic_reviewer_allow() {
    let (session, _, model, adapter, count) = setup(
        r#"{"action":"allow","reason":"must lose to explicit user"}"#,
        false,
        Some(Action::Deny),
        false,
    );
    session.submit("user refuses").await.unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert_eq!(adapter.asks.load(Ordering::SeqCst), 1);
    assert!(
        model
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|request| request.subagent.is_none()),
        "an immediate accepted answer cancels review before model dispatch"
    );
    session.close().await.unwrap();
}
#[tokio::test(start_paused = true)]
async fn unavailable_executor_denies_even_when_action_review_is_enabled() {
    let (session, _, model, adapter, count) = setup(
        r#"{"action":"allow","reason":"no executor cannot be bypassed"}"#,
        false,
        None,
        false,
    );
    adapter.unavailable.store(true, Ordering::SeqCst);
    session.submit("unavailable executor").await.unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert_eq!(adapter.asks.load(Ordering::SeqCst), 0);
    assert!(
        model
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|request| request.subagent.is_none())
    );
    session.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn parent_ask_ceiling_requires_its_own_single_call_approval() {
    let adapter = Arc::new(Policy {
        asks: AtomicUsize::new(0),
        unavailable: std::sync::atomic::AtomicBool::new(false),
        reply_delay_ms: std::sync::atomic::AtomicU64::new(0),
        reply: Some(Action::Allow),
    });
    let parent = parent_session(
        adapter.clone(),
        PermissionConfig {
            review: false,
            overrides: vec![rule("write", Action::Ask)],
            ..Default::default()
        },
    );
    let (session, _child, _, _, count) = setup_with_parent(
        Some(parent.permission_controller()),
        r#"{"action":"allow","reason":"fixture"}"#,
        false,
        None,
        false,
    );
    session.submit("run").await.unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(adapter.asks.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn child_uses_live_parent_ceiling_and_cannot_keep_revoked_approval() {
    let parent = parent_session(
        Arc::new(Policy {
            asks: AtomicUsize::new(0),
            unavailable: std::sync::atomic::AtomicBool::new(false),
            reply_delay_ms: std::sync::atomic::AtomicU64::new(0),
            reply: None,
        }),
        PermissionConfig {
            overrides: vec![rule("write", Action::Allow)],
            ..Default::default()
        },
    );
    let (session, _child, model, _, count) = setup_with_parent(
        Some(parent.permission_controller()),
        r#"{"action":"allow","reason":"fixture"}"#,
        true,
        None,
        false,
    );
    let running = session.clone();
    let worker = tokio::spawn(async move { running.submit("run").await });
    tokio::time::timeout(std::time::Duration::from_secs(5), model.entered.notified())
        .await
        .expect("review must start while the user interaction remains unanswered");
    assert_eq!(count.load(Ordering::SeqCst), 0);
    let _snapshot = parent.permission_controller().snapshot().unwrap();
    edit(&parent, |config| {
        config.overrides.push(rule("write", Action::Deny))
    });
    model.release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(5), worker)
        .await
        .expect("released or revoked work must settle")
        .unwrap()
        .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn selecting_and_recovering_modes_preserves_catalogs_and_current_overrides() {
    use microcrab_agent_types::{AgentInfo, AgentMode, AgentRole, Variable};
    let adapter = Arc::new(Policy {
        asks: AtomicUsize::new(0),
        unavailable: std::sync::atomic::AtomicBool::new(false),
        reply_delay_ms: std::sync::atomic::AtomicU64::new(0),
        reply: None,
    });
    let runtime = Runtime::new(vec![], adapter).unwrap();
    let controller = runtime.permission_controller();
    let model = Arc::new(Model {
        review: String::new(),
        requests: Mutex::new(vec![]),
        pause: false,
        entered: Notify::new(),
        release: Notify::new(),
    });
    let agent = AgentInfo {
        tool_media_output_bytes: Default::default(),
        name: "coding".into(),
        modes: vec![
            AgentMode {
                description: String::new(),
                prompt: String::new(),
                name: "plan".into(),
                role: AgentRole::Both,
                permissions: vec![rule("write", Action::Deny)],
            },
            AgentMode {
                description: String::new(),
                prompt: String::new(),
                name: "build".into(),
                role: AgentRole::Both,
                permissions: vec![rule("write", Action::Ask)],
            },
            AgentMode {
                description: String::new(),
                prompt: String::new(),
                name: "child-only".into(),
                role: AgentRole::SubAgent,
                permissions: vec![rule("write", Action::Allow)],
            },
        ],
        tools: vec![],
        variables: Variable::default(),
    };
    let session = Session::builder_arc(model, runtime)
        .agent(agent)
        .build()
        .unwrap();
    assert_eq!(session.state().selected_mode.as_deref(), Some("plan"));
    // New Sessions include native communication defaults alongside mode policy.
    let initial_defaults = controller.snapshot().unwrap().config.defaults;
    let before = session.checkpoint().await.unwrap();
    session.select_mode("build").await.unwrap();
    assert_eq!(session.state().selected_mode.as_deref(), Some("build"));
    let after = session.checkpoint().await.unwrap();
    assert_eq!(
        serde_json::to_value(&before.context).unwrap(),
        serde_json::to_value(&after.context).unwrap()
    );
    let _snapshot = controller.snapshot().unwrap();
    edit(&session, |config| {
        config.overrides.push(rule("write", Action::Deny))
    });
    session.recover(before).await.unwrap();
    assert_eq!(session.state().selected_mode.as_deref(), Some("plan"));
    let snapshot = controller.snapshot().unwrap();
    assert_eq!(snapshot.config.mode, "plan");
    assert_eq!(snapshot.config.defaults, initial_defaults);
    assert!(snapshot.config.provider_defaults.is_empty());
    assert_eq!(snapshot.config.overrides, vec![rule("write", Action::Deny)]);
    assert!(session.select_mode("unknown").await.is_err());
    assert!(session.select_mode("child-only").await.is_err());
    assert_eq!(session.state().selected_mode.as_deref(), Some("plan"));
    assert_eq!(controller.snapshot().unwrap(), snapshot);
}

#[test]
fn invalid_edit_is_atomic_and_revision_compare_and_swap_rejects_stale_writer() {
    let session = parent_session(
        Arc::new(Policy {
            asks: AtomicUsize::new(0),
            unavailable: std::sync::atomic::AtomicBool::new(false),
            reply_delay_ms: std::sync::atomic::AtomicU64::new(0),
            reply: None,
        }),
        PermissionConfig::default(),
    );
    let controller = session.permission_controller();
    let before = controller.snapshot().unwrap();
    let mut invalid = before.config.clone();
    invalid.overrides = vec![rule("write", Action::Allow); 257];
    assert!(
        session
            .replace_permissions(before.revision, invalid)
            .is_err()
    );
    assert_eq!(controller.snapshot().unwrap(), before);
    edit(&session, |config| config.review = false);
    assert!(matches!(
        session.replace_permissions(before.revision, before.config),
        Err(PermissionError::StalePolicy)
    ));
}

#[tokio::test(start_paused = true)]
async fn deterministic_parent_deny_prevents_child_review_and_effects() {
    let parent = parent_session(
        Arc::new(Policy {
            asks: AtomicUsize::new(0),
            unavailable: std::sync::atomic::AtomicBool::new(false),
            reply_delay_ms: std::sync::atomic::AtomicU64::new(0),
            reply: Some(Action::Allow),
        }),
        PermissionConfig {
            overrides: vec![rule("write", Action::Deny)],
            ..Default::default()
        },
    );
    let (session, _, model, adapter, effects) = setup_with_parent(
        Some(parent.permission_controller()),
        r#"{"action":"allow","reason":"must not run"}"#,
        false,
        Some(Action::Allow),
        false,
    );
    session.submit("denied before review").await.unwrap();
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert_eq!(adapter.asks.load(Ordering::SeqCst), 0);
    assert!(
        model
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|request| request.subagent.is_none())
    );
    session.close().await.unwrap();
    parent.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn child_review_allow_cannot_resolve_a_parent_owned_ask() {
    let owner = Arc::new(Policy {
        asks: AtomicUsize::new(0),
        unavailable: std::sync::atomic::AtomicBool::new(false),
        reply_delay_ms: std::sync::atomic::AtomicU64::new(0),
        reply: Some(Action::Deny),
    });
    let parent = parent_session(
        owner.clone(),
        PermissionConfig {
            review: false,
            overrides: vec![rule("write", Action::Ask)],
            ..Default::default()
        },
    );
    let (session, _, model, _, effects) = setup_with_parent(
        Some(parent.permission_controller()),
        r#"{"action":"allow","reason":"child only"}"#,
        false,
        None,
        false,
    );
    session.submit("parent must decide").await.unwrap();
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert_eq!(owner.asks.load(Ordering::SeqCst), 1);
    assert_eq!(
        model
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.subagent.as_deref() == Some("action_review"))
            .count(),
        1
    );
    session.close().await.unwrap();
    parent.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn policy_edits_only_affect_future_calls_while_admitted_continuation_is_pinned() {
    let pause = Arc::new((Notify::new(), Notify::new()));
    let effects = Arc::new(AtomicUsize::new(0));
    let runtime = Runtime::new(
        vec![ToolRegistration::Wasm(Arc::new(Effect {
            count: effects.clone(),
            pause: Some(pause.clone()),
        }))],
        Arc::new(Policy {
            asks: AtomicUsize::new(0),
            unavailable: std::sync::atomic::AtomicBool::new(false),
            reply_delay_ms: std::sync::atomic::AtomicU64::new(0),
            reply: None,
        }),
    )
    .unwrap();
    let model = Arc::new(Model {
        review: String::new(),
        requests: Mutex::new(vec![]),
        pause: false,
        entered: Notify::new(),
        release: Notify::new(),
    });
    let session = Session::builder_arc(model, runtime)
        .permissions(PermissionConfig {
            overrides: vec![rule("write", Action::Allow)],
            ..Default::default()
        })
        .build()
        .unwrap();
    let running = session.clone();
    let worker = tokio::spawn(async move { running.submit("start admitted work").await });
    tokio::time::timeout(std::time::Duration::from_secs(5), pause.0.notified())
        .await
        .unwrap();
    edit(&session, |config| {
        config.overrides = vec![rule("write", Action::Deny)]
    });
    pause.1.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(5), worker)
        .await
        .expect("released or revoked work must settle")
        .unwrap()
        .unwrap();
    assert_eq!(
        effects.load(Ordering::SeqCst),
        1,
        "policy edits cannot revoke admitted work"
    );
    session.submit("future invocation is denied").await.unwrap();
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    let rows = session.execution_snapshots();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[1].diagnostic.as_ref().unwrap().code,
        microcrab_session::observation::diagnostics::DiagnosticCode::PermissionDenied
    );
    session.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn parent_allow_cannot_override_a_child_tool_deny() {
    let parent = parent_session(
        Arc::new(Policy {
            asks: AtomicUsize::new(0),
            unavailable: std::sync::atomic::AtomicBool::new(false),
            reply_delay_ms: std::sync::atomic::AtomicU64::new(0),
            reply: Some(Action::Allow),
        }),
        PermissionConfig {
            overrides: vec![rule("write", Action::Allow)],
            ..Default::default()
        },
    );
    let (session, _, model, _, effects) = setup_with_parent(
        Some(parent.permission_controller()),
        r#"{"action":"allow","reason":"must not run"}"#,
        false,
        None,
        false,
    );
    edit(&session, |config| {
        config.overrides = vec![rule("write", Action::Deny)]
    });
    session.submit("child refusal").await.unwrap();
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert!(
        model
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|request| request.subagent.is_none())
    );
    session.close().await.unwrap();
    parent.close().await.unwrap();
}
