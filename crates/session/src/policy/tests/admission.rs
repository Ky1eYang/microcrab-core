//! Whole-call admission replaces the former per-resource batch policy checks.
use super::*;
use crate::runtime::tool::ToolContext;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

pub(super) struct CompatibilityAllow;
#[async_trait]
impl SessionPermission for CompatibilityAllow {
    async fn evaluate(
        &self,
        _: &PolicySnapshot,
        _: &PermissionRequest,
    ) -> Result<Decision, PermissionError> {
        Ok(Decision::new(
            Action::Allow,
            "adapter cannot override declarations",
            "adapter",
        ))
    }
}
pub(super) fn batch_rule(action: Action) -> Permission {
    Permission {
        agent: None,
        mode: None,
        tool: Some(Matcher::Exact("job".into())),
        args: vec![],
        action,
    }
}
fn configure(owner: &PermissionController, action: Action) {
    let snapshot = owner.snapshot().unwrap();
    owner
        .replace(
            snapshot.revision,
            PermissionConfig {
                overrides: vec![batch_rule(action)],
                review: false,
                ..Default::default()
            },
        )
        .unwrap();
}
pub(super) fn invocation() -> ToolContext {
    let mut context = ToolContext::detached(
        crate::SessionState::new(Default::default()),
        CancellationToken::new(),
    );
    context.call_id = "batch".into();
    context.tool = "job".into();
    reserve(&mut context);
    context
}
fn reserve(context: &mut ToolContext) {
    let mut row = crate::runtime::execution::ExecutionSnapshot::new(
        context.session_id.clone(),
        context.call_id.clone(),
        context.tool.clone(),
        context.agent.clone(),
        context.mode.clone(),
    );
    row.subject = context.subject.clone();
    context.execution_id = Some(
        context
            .state
            .executions()
            .admit_detached(row, context.cancellation.clone())
            .unwrap(),
    );
}
pub(super) fn request(
    owner: &PermissionController,
    context: &ToolContext,
    arguments: serde_json::Value,
) -> PermissionRequest {
    let policy = owner.snapshot().unwrap();
    PermissionRequest {
        subject: context.subject.clone(),
        session_id: context.session_id.clone(),
        call_id: context.call_id.clone(),
        agent: policy.config.agent,
        mode: policy.config.mode,
        tool: context.tool.clone(),
        arguments,
        revision: policy.revision,
    }
}
pub(super) fn issuer() -> crate::context::Content {
    crate::context::Content {
        epoch: 0,
        subagent: None,
        system: "Permission fixture".into(),
        items: vec![],
        tools: vec![],
        max_output_tokens: 1024,
        max_bytes: 65536,
        turn_id: None,
        round: None,
    }
}
#[derive(Default)]
pub(super) struct BatchAsk {
    pub(super) requests: Mutex<Vec<PermissionRequest>>,
    change: Mutex<Option<Change>>,
    calls: AtomicUsize,
}
enum Change {
    Churn(std::sync::Weak<PermissionController>),
    Revision(std::sync::Weak<PermissionController>, bool),
    Parent(
        std::sync::Weak<PermissionController>,
        Arc<PermissionController>,
    ),
}
#[async_trait]
impl SessionPermission for BatchAsk {
    async fn evaluate(
        &self,
        policy: &PolicySnapshot,
        request: &PermissionRequest,
    ) -> Result<Decision, PermissionError> {
        microcrab_permission::evaluate(policy, request)
            .map_err(|e| PermissionError::InvalidConfiguration(e.to_string()))
    }
    fn features(&self) -> PermissionFeatures {
        PermissionFeatures {
            interactive_ask: true,
            ..Default::default()
        }
    }
    async fn ask(&self, request: &PermissionRequest) -> Result<AskReply, PermissionError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request.clone());
        let change = self.change.lock().unwrap().take();
        if let Some(change) = change {
            match change {
                Change::Churn(owner) => {
                    let live = owner.upgrade().unwrap();
                    let snapshot = live.snapshot()?;
                    live.replace(snapshot.revision, snapshot.config)?;
                    *self.change.lock().unwrap() = Some(Change::Churn(owner));
                }
                Change::Revision(owner, deny) => {
                    let owner = owner.upgrade().unwrap();
                    let snapshot = owner.snapshot()?;
                    let mut config = snapshot.config;
                    if deny {
                        config.overrides = vec![batch_rule(Action::Deny)];
                    }
                    owner.replace(snapshot.revision, config)?;
                }
                Change::Parent(child, parent) => child.upgrade().unwrap().set_parent(parent)?,
            }
        }
        Ok(AskReply {
            action: Action::Allow,
            reason: "whole invocation".into(),
        })
    }
}

#[tokio::test]
async fn every_builtin_explicit_deny_beats_adapter_allow() {
    for tool in ["job", "delegate", "sleep", "expand"] {
        let owner = Arc::new(PermissionController::new(Arc::new(CompatibilityAllow)));
        let before = owner.snapshot().unwrap();
        let mut config = before.config;
        let mut rule = batch_rule(Action::Deny);
        rule.tool = Some(Matcher::Exact(tool.into()));
        config.overrides.push(rule);
        let policy = owner.replace(before.revision, config).unwrap();
        let mut context = invocation();
        context.tool = tool.into();
        assert_eq!(
            microcrab_permission::evaluate(&policy, &request(&owner, &context, json!({})))
                .unwrap()
                .action,
            Action::Deny
        );
    }
}
#[test]
fn child_ceiling_cannot_be_replaced_or_cycled() {
    let parent = Arc::new(PermissionController::new(Arc::new(CompatibilityAllow)));
    let child = Arc::new(PermissionController::new(Arc::new(CompatibilityAllow)));
    child.set_parent(parent.clone()).unwrap();
    assert!(parent.set_parent(child.clone()).is_err());
    assert!(child.set_parent(parent).is_err());
}
#[tokio::test]
async fn deterministic_deny_precedes_every_owner_review() {
    for parent_deny in [false, true] {
        let adapter = Arc::new(BatchAsk::default());
        let local = Arc::new(PermissionController::new(adapter.clone()));
        let parent = Arc::new(PermissionController::new(adapter.clone()));
        configure(
            &local,
            if parent_deny {
                Action::Ask
            } else {
                Action::Deny
            },
        );
        configure(
            &parent,
            if parent_deny {
                Action::Deny
            } else {
                Action::Ask
            },
        );
        local.set_parent(parent).unwrap();
        let context = invocation();
        assert!(matches!(
            local
                .admit(
                    &request(&local, &context, json!({})),
                    &context,
                    &issuer(),
                    None
                )
                .await,
            Err(PermissionError::PolicyDenied { .. })
        ));
        assert_eq!(adapter.calls.load(Ordering::SeqCst), 0);
    }
}
#[tokio::test]
async fn each_owner_asks_once_with_own_agent_mode_and_original_batch_args() {
    let adapter = Arc::new(BatchAsk::default());
    let parent_adapter = Arc::new(BatchAsk::default());
    let local = Arc::new(PermissionController::new(adapter.clone()));
    let parent = Arc::new(PermissionController::new(parent_adapter.clone()));
    for (owner, agent) in [(&local, "child"), (&parent, "parent")] {
        let snapshot = owner.snapshot().unwrap();
        let mut config = snapshot.config;
        config.agent = agent.into();
        config.mode = agent.into();
        config.review = false;
        config.overrides = vec![batch_rule(Action::Ask)];
        owner.replace(snapshot.revision, config).unwrap();
    }
    local.set_parent(parent).unwrap();
    let context = invocation();
    let args = json!({"id":["s/1","s/2"],"action":"release"});
    local
        .admit(
            &request(&local, &context, args.clone()),
            &context,
            &issuer(),
            None,
        )
        .await
        .unwrap();
    for (adapter, name) in [(adapter, "child"), (parent_adapter, "parent")] {
        let requests = adapter.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].agent, name);
        assert_eq!(requests[0].mode, name);
        assert_eq!(requests[0].arguments, args);
    }
}
#[tokio::test]
async fn local_or_ancestor_change_discards_approval_before_minting() {
    for ancestor in [false, true] {
        let adapter = Arc::new(BatchAsk::default());
        let local = Arc::new(PermissionController::new(adapter.clone()));
        let parent = Arc::new(PermissionController::new(Arc::new(CompatibilityAllow)));
        configure(&local, Action::Ask);
        configure(&parent, Action::Allow);
        local.set_parent(parent.clone()).unwrap();
        *adapter.change.lock().unwrap() = Some(Change::Revision(
            Arc::downgrade(if ancestor { &parent } else { &local }),
            true,
        ));
        let context = invocation();
        assert!(matches!(
            local
                .admit(
                    &request(&local, &context, json!({})),
                    &context,
                    &issuer(),
                    None
                )
                .await,
            Err(PermissionError::PolicyDenied { .. })
        ));
        assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn newly_added_ancestor_is_rechecked_after_await() {
    let adapter = Arc::new(BatchAsk::default());
    let local = Arc::new(PermissionController::new(adapter.clone()));
    let parent = Arc::new(PermissionController::new(Arc::new(CompatibilityAllow)));
    configure(&local, Action::Ask);
    configure(&parent, Action::Deny);
    *adapter.change.lock().unwrap() = Some(Change::Parent(Arc::downgrade(&local), parent));
    let context = invocation();
    assert!(matches!(
        local
            .admit(
                &request(&local, &context, json!({})),
                &context,
                &issuer(),
                None
            )
            .await,
        Err(PermissionError::PolicyDenied { .. })
    ));
}
#[tokio::test]
async fn clearing_context_proof_cannot_remint_for_the_same_execution() {
    let owner = Arc::new(PermissionController::new(Arc::new(CompatibilityAllow)));
    configure(&owner, Action::Allow);
    let mut context = invocation();
    let original = request(&owner, &context, json!({"action":"write"}));
    context.admission = Some(
        owner
            .admit(&original, &context, &issuer(), None)
            .await
            .unwrap(),
    );
    context.begin_dispatch(None).unwrap();
    context.admission = None;
    assert!(matches!(
        owner.admit(&original, &context, &issuer(), None).await,
        Err(PermissionError::AlreadyDispatched)
    ));
}

#[tokio::test]
async fn admitted_authority_is_pinned_and_scope_cannot_be_reused() {
    let owner = Arc::new(PermissionController::new(Arc::new(CompatibilityAllow)));
    configure(&owner, Action::Allow);
    let mut context = invocation();
    let args = json!({"action":"write"});
    context.admission = Some(
        owner
            .admit(
                &request(&owner, &context, args.clone()),
                &context,
                &issuer(),
                Some("component".into()),
            )
            .await
            .unwrap(),
    );
    assert!(context.begin_dispatch(Some("other")).is_err());
    context.begin_dispatch(Some("component")).unwrap();
    configure(&owner, Action::Deny);
    // A dispatched effect keeps its invocation-scoped authority. Separate
    // pre-dispatch revocation coverage rejects an unentered old proof.
    context.ensure_admitted(Some("component")).unwrap();
    context.ensure_admitted_arguments(&args).unwrap();
    assert!(context.ensure_admitted(Some("other")).is_err());
    assert!(
        context
            .ensure_admitted_arguments(&json!({"action":"release"}))
            .is_err()
    );
    let mut other = context.clone();
    other.call_id = "other".into();
    assert!(other.ensure_admitted(None).is_err());
    let mut other = context.clone();
    other.state = crate::State::new(Default::default());
    assert!(other.ensure_admitted(None).is_err());
    let mut other = context.clone();
    other.processes = invocation().processes;
    assert!(other.ensure_admitted(None).is_err());
    context.ensure_admitted(None).unwrap();
    let mut other = context.clone();
    other.agent = "changed".into();
    assert!(other.ensure_admitted(None).is_err());
    let mut other = context.clone();
    other.subject = InvocationSubject::AgentInit {
        component: "component".into(),
        generation: 1,
        attempt: 1,
    };
    assert!(other.ensure_admitted(None).is_err());
    let mut other = context.clone();
    other.execution_id = Some(crate::runtime::execution::ExecutionId(999));
    assert!(other.ensure_admitted(None).is_err());
    let mut other = context.clone();
    other.session_id = "another-session".into();
    assert!(other.ensure_admitted(None).is_err());
    assert!(context.begin_dispatch(Some("other")).is_err());
    assert!(matches!(
        context.clone().begin_dispatch(Some("component")),
        Err(PermissionError::AlreadyDispatched)
    ));
    context.ensure_admitted(Some("component")).unwrap();
    context.ensure_admitted_arguments(&args).unwrap();
    context.cancellation.cancel();
    assert!(matches!(
        context.ensure_admitted(None),
        Err(PermissionError::Cancelled)
    ));
    assert!(invocation().ensure_admitted(None).is_err());
}
#[tokio::test]
async fn lifecycle_is_independent_of_wildcard_tool_allow_and_inherits_parent() {
    let adapter = Arc::new(BatchAsk::default());
    let local = Arc::new(PermissionController::new(adapter.clone()));
    let before = local.snapshot().unwrap();
    let mut config = before.config;
    config.review = false;
    config.overrides = vec![Permission {
        tool: None,
        ..batch_rule(Action::Allow)
    }];
    assert_eq!(config.initialization, Action::Allow);
    config.initialization = Action::Ask;
    local.replace(before.revision, config).unwrap();
    let mut context = invocation();
    context.tool.clear();
    context.subject = InvocationSubject::AgentInit {
        component: "component".into(),
        generation: 1,
        attempt: 1,
    };
    reserve(&mut context);
    local
        .admit(
            &request(
                &local,
                &context,
                json!({"component":"component","imports":["file-write"]}),
            ),
            &context,
            &issuer(),
            Some("component".into()),
        )
        .await
        .unwrap();
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
    let parent = Arc::new(PermissionController::new(Arc::new(CompatibilityAllow)));
    let before = parent.snapshot().unwrap();
    let mut config = before.config;
    config.initialization = Action::Deny;
    parent.replace(before.revision, config).unwrap();
    local.set_parent(parent).unwrap();
    assert!(matches!(
        local
            .admit(
                &request(&local, &context, json!({})),
                &context,
                &issuer(),
                Some("component".into())
            )
            .await,
        Err(PermissionError::PolicyDenied { .. })
    ));
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
}
#[test]
fn native_job_six_actions_allow_in_every_mode() {
    for mode in ["plan", "build", ""] {
        let owner = Arc::new(PermissionController::new(Arc::new(CompatibilityAllow)));
        let tools = vec![("job".into(), crate::runtime::tool::ToolOrigin::Builtin)];
        let snapshot = owner
            .tool_config_for_mode(&tools, &[], &[], "coding", mode)
            .unwrap();
        let policy = compile(&snapshot, "job").unwrap();
        for action in ["status", "read", "wait", "write", "cancel", "release"] {
            assert_eq!(
                evaluate_compiled(&policy, &json!({"action":action})).unwrap(),
                Action::Allow
            );
        }
    }
}
#[test]
fn projection_matches_enforcement_and_retains_ancestor_witness() {
    let parent = Arc::new(PermissionController::new(Arc::new(CompatibilityAllow)));
    let local = PermissionController::new(Arc::new(CompatibilityAllow));
    configure(&parent, Action::Allow);
    configure(&local, Action::Deny);
    local.set_parent(parent.clone()).unwrap();
    let tools = vec![("job".into(), crate::runtime::tool::ToolOrigin::Builtin)];
    let value = local
        .compiled_inventory(&local.snapshot().unwrap(), &tools)
        .unwrap();
    assert_eq!(value[0]["action"], "deny");
    assert_eq!(value[0]["owners"].as_array().unwrap().len(), 2);
    let mut node = value[0].clone();
    node.as_object_mut().unwrap().remove("tool");
    node.as_object_mut().unwrap().remove("owners");
    let compiled: CompiledToolPolicy = serde_json::from_value(node).unwrap();
    assert_eq!(
        evaluate_compiled(&compiled, &json!({})).unwrap(),
        Action::Deny
    );
    let before = value[0]["owners"][1]["revision"].clone();
    configure(&parent, Action::Ask);
    let after = local
        .compiled_inventory(&local.snapshot().unwrap(), &tools)
        .unwrap();
    assert_ne!(before, after[0]["owners"][1]["revision"]);
}

#[tokio::test]
async fn repeated_policy_churn_is_bounded_and_never_mints_a_proof() {
    let adapter = Arc::new(BatchAsk::default());
    let owner = Arc::new(PermissionController::new(adapter.clone()));
    configure(&owner, Action::Ask);
    *adapter.change.lock().unwrap() = Some(Change::Churn(Arc::downgrade(&owner)));
    let context = invocation();
    assert!(matches!(
        owner
            .admit(
                &request(&owner, &context, json!({})),
                &context,
                &issuer(),
                None
            )
            .await,
        Err(PermissionError::StalePolicy)
    ));
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 3);
    assert!(context.admission.is_none());
}

#[test]
fn delegate_partial_evaluation_enforces_aggregate_matcher_budget() {
    let owner = Arc::new(PermissionController::new(Arc::new(CompatibilityAllow)));
    let before = owner.snapshot().unwrap();
    let mut config = before.config;
    config.overrides = vec![Permission {
        tool: Some(Matcher::Exact("delegate".into())),
        args: vec![
            ArgumentPredicate {
                pointer: "/target".into(),
                matcher: Matcher::Glob("*".repeat(4096))
            };
            2
        ],
        ..batch_rule(Action::Allow)
    }];
    owner.replace(before.revision, config).unwrap();
    let observation = owner.observe(vec![(
        "delegate".into(),
        crate::runtime::tool::ToolOrigin::Builtin,
    )]);
    assert!(
        matches!(observation.delegate_policy(&"w".repeat(128),"build"),Err(PermissionError::InvalidConfiguration(reason)) if reason.contains("matcher work budget"))
    );
}

#[test]
fn proof_mismatch_keeps_its_safe_reason_and_permission_classification() {
    use crate::observation::diagnostics::{DiagnosticCode, DiagnosticPhase};
    let error = PermissionError::ProofMismatch("original arguments differ");
    assert_eq!(error.public_reason(), "original arguments differ");
    let diagnostic = error.into_runtime_error().tool_diagnostic();
    assert_eq!(diagnostic.code, DiagnosticCode::PermissionDenied);
    assert_eq!(diagnostic.phase, DiagnosticPhase::Authorization);
    assert_eq!(
        diagnostic.reason.as_ref().unwrap().as_str(),
        "invocation admission proof mismatch: original arguments differ"
    );
}

#[test]
fn delegate_partial_evaluation_keeps_unknown_guards_and_never_asks() {
    let adapter = Arc::new(BatchAsk::default());
    let owner = Arc::new(PermissionController::new(adapter.clone()));
    let before = owner.snapshot().unwrap();
    let mut config = before.config;
    config.review = false;
    config.overrides = serde_json::from_value(json!([
        {"tool":{"kind":"exact","value":"delegate"},"action":"deny"},
        {"tool":{"kind":"exact","value":"delegate"},"args":[{"pointer":"/target","matcher":{"kind":"exact","value":"worker"}},{"pointer":"/input","matcher":{"kind":"exact","value":"safe"}}],"action":"allow"}
    ])).unwrap();
    owner.replace(before.revision, config).unwrap();
    let observation = owner.observe(vec![(
        "delegate".into(),
        crate::runtime::tool::ToolOrigin::Builtin,
    )]);
    assert_eq!(
        observation.delegate_policy("other", "build").unwrap().0,
        Action::Deny
    );
    let (summary, residual) = observation.delegate_policy("worker", "build").unwrap();
    assert_eq!(summary, Action::Ask);
    assert_eq!(residual["arguments_unresolved"], true);
    let mut node = residual.clone();
    for field in ["owners", "action_bounds", "arguments_unresolved"] {
        node.as_object_mut().unwrap().remove(field);
    }
    let policy: CompiledToolPolicy = serde_json::from_value(node).unwrap();
    assert_eq!(
        evaluate_compiled(&policy, &json!({"input":"safe"})).unwrap(),
        Action::Allow
    );
    assert_eq!(
        evaluate_compiled(&policy, &json!({})).unwrap(),
        Action::Deny
    );
    assert_eq!(
        evaluate_compiled(&policy, &json!({"input":false})).unwrap(),
        Action::Deny
    );
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 0);
    let projected = observation.project().unwrap();
    let snapshot = owner.snapshot().unwrap();
    owner.replace(snapshot.revision, snapshot.config).unwrap();
    assert_eq!(projected, observation.project().unwrap());
}
