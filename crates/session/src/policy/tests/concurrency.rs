//! Concurrency, dispatch and durable adoption behavior; these tests are not run
//! in the implementation worktree.
use super::*;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::time::Instant;
struct TimedInteraction {
    deadline: Instant,
    user_after: Duration,
    user_action: Action,
    publication_delay: Duration,
    review_calls: AtomicUsize,
    settled: AtomicBool,
}
#[async_trait]
impl PermissionInteraction for TimedInteraction {
    fn deadline(&self) -> Instant {
        self.deadline
    }
    async fn user_reply(&self) -> Result<AskReply, PermissionError> {
        tokio::time::sleep(self.user_after).await;
        Ok(AskReply {
            action: self.user_action,
            reason: "user_once".into(),
        })
    }
    async fn review(&self, _: &ReviewResult, _: Instant) -> Result<Instant, PermissionError> {
        self.review_calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.publication_delay).await;
        Ok(Instant::now() + Duration::from_secs(5))
    }
    async fn settle(&self, decision: &Decision) -> Result<AskReply, PermissionError> {
        self.settled.store(true, Ordering::SeqCst);
        Ok(AskReply {
            action: decision.action,
            reason: if decision.source == "review" {
                "review_auto".into()
            } else {
                "timeout".into()
            },
        })
    }
}
struct TimedAdapter(Option<Arc<TimedInteraction>>);
#[async_trait]
impl SessionPermission for TimedAdapter {
    async fn evaluate(
        &self,
        p: &PolicySnapshot,
        r: &PermissionRequest,
    ) -> Result<Decision, PermissionError> {
        evaluate(p, r).map_err(|e| PermissionError::InvalidConfiguration(e.to_string()))
    }
    fn features(&self) -> PermissionFeatures {
        PermissionFeatures {
            interactive_ask: true,
            ..Default::default()
        }
    }
    async fn open_interaction(
        &self,
        _: &PermissionRequest,
        _: &AskAttribution,
    ) -> Result<Option<Arc<dyn PermissionInteraction>>, PermissionError> {
        self.0
            .clone()
            .map(|p| Some(p as Arc<dyn PermissionInteraction>))
            .ok_or(PermissionError::Unavailable)
    }
}
fn timed(user_after: u64, user_action: Action, publication_delay: u64) -> Arc<TimedInteraction> {
    Arc::new(TimedInteraction {
        deadline: Instant::now() + Duration::from_secs(120),
        user_after: Duration::from_secs(user_after),
        user_action,
        publication_delay: Duration::from_secs(publication_delay),
        review_calls: AtomicUsize::new(0),
        settled: AtomicBool::new(false),
    })
}
async fn resolve(adapter: Arc<dyn SessionPermission>, action: Action) -> (Action, Duration, Option<interaction::Denial>) {
    let owner = PermissionController::new(adapter);
    let probe = review_probe(action);
    owner.bind(probe, Arc::new(crate::NoopAuditor)).unwrap();
    owner
        .bind_one_shot_executor(crate::Session::one_shot_executor())
        .unwrap();
    let policy = owner.snapshot().unwrap();
    let state = crate::State::new(Default::default());
    let context = invocation();
    let request = request(&owner, &context, json!({}));
    let start = Instant::now();
    let result = owner
        .resolve_ask(
            &policy,
            &request,
            None,
            &state,
            &issuer(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    (result.0, start.elapsed(), result.3)
}
#[tokio::test(start_paused = true)]
async fn explicit_user_deny_during_grace_overrides_review_allow() {
    let port = timed(2, Action::Deny, 0);
    let (action, elapsed, denial) =
        resolve(Arc::new(TimedAdapter(Some(port.clone()))), Action::Allow).await;
    assert_eq!(action, Action::Deny);
    let denial = denial.unwrap();
    assert_eq!(denial.source, crate::observation::diagnostics::PermissionDecisionSource::User);
    assert!(!denial.reason.as_str().contains("review fixture"));
    assert_eq!(elapsed, Duration::from_secs(2));
    assert!(!port.settled.load(Ordering::SeqCst));
}
#[tokio::test(start_paused = true)]
async fn delayed_review_publication_still_gets_full_five_seconds() {
    let port = timed(100, Action::Deny, 3);
    let (action, elapsed, denial) =
        resolve(Arc::new(TimedAdapter(Some(port.clone()))), Action::Allow).await;
    assert_eq!(action, Action::Allow);
    assert!(denial.is_none());
    assert!(elapsed >= Duration::from_secs(8));
    assert_eq!(port.review_calls.load(Ordering::SeqCst), 1);
}
#[tokio::test(start_paused = true)]
async fn settled_review_denial_reaches_model_diagnostic() {
    use crate::observation::diagnostics::{EffectKnowledge, PermissionDecisionSource};
    let port = timed(200, Action::Allow, 0);
    let owner = Arc::new(PermissionController::new(Arc::new(TimedAdapter(Some(port)))));
    owner.bind(review_probe(Action::Deny), Arc::new(crate::NoopAuditor)).unwrap();
    owner.bind_one_shot_executor(crate::Session::one_shot_executor()).unwrap();
    let before = owner.snapshot().unwrap();
    let mut config = before.config;
    config.review = true;
    config.overrides.push(Permission { agent: None, mode: None, tool: Some(Matcher::Exact("job".into())), args: vec![], action: Action::Ask });
    owner.replace(before.revision, config).unwrap();
    let context = invocation();
    let error = owner.admit(&request(&owner, &context, json!({})), &context, &issuer(), None).await.err().unwrap();
    let result = error.into_runtime_error().tool_diagnostic();
    assert_eq!(result.decision_source, Some(PermissionDecisionSource::ActionReview));
    assert_eq!(result.effects, EffectKnowledge::NotStarted);
    assert_eq!(result.reason.unwrap().as_str(), "Action Review denied the operation: review fixture");
}

#[tokio::test(start_paused = true)]
async fn redacted_image_review_cannot_auto_allow() {
    let port = timed(200, Action::Allow, 0);
    let owner = PermissionController::new(Arc::new(TimedAdapter(Some(port.clone()))));
    let probe = review_probe(Action::Allow);
    owner
        .bind(probe.clone(), Arc::new(crate::NoopAuditor))
        .unwrap();
    owner
        .bind_one_shot_executor(crate::Session::one_shot_executor())
        .unwrap();
    let snapshot = owner.snapshot().unwrap();
    let context = invocation();
    let mut request = request(
        &owner,
        &context,
        json!({"image_url":"file:private-witness"}),
    );
    request.tool = "view_image".into();
    let preview = crate::runtime::tool::view_image::ApprovalPreview {
        owner: json!({"image_url":"file:///workspace/exact.png"}),
        reviewer: json!({"image_url":"[local file URI redacted]"}),
    };
    let result = owner
        .resolve_ask(
            &snapshot,
            &request,
            Some(&preview),
            &context.state,
            &issuer(),
            &context.cancellation,
        )
        .await
        .unwrap();
    assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
    assert_eq!(result.0, Action::Deny);
    assert_eq!(port.review_calls.load(Ordering::SeqCst), 0);
    assert!(port.settled.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn review_ask_keeps_waiting_for_user_without_automatic_allow() {
    let port = timed(9, Action::Deny, 0);
    let (action, elapsed, denial) = resolve(Arc::new(TimedAdapter(Some(port.clone()))), Action::Ask).await;
    assert_eq!(denial.unwrap().source, crate::observation::diagnostics::PermissionDecisionSource::User);
    assert_eq!(action, Action::Deny);
    assert_eq!(elapsed, Duration::from_secs(9));
    assert_eq!(port.review_calls.load(Ordering::SeqCst), 0);
}
#[tokio::test(start_paused = true)]
async fn no_final_reply_identifies_timeout() {
    let port = timed(200, Action::Deny, 0);
    let (action, elapsed, denial) = resolve(Arc::new(TimedAdapter(Some(port))), Action::Ask).await;
    assert_eq!(action, Action::Deny);
    assert_eq!(elapsed, Duration::from_secs(120));
    assert_eq!(denial.unwrap().source, crate::observation::diagnostics::PermissionDecisionSource::Timeout);
}

#[tokio::test]
async fn missing_executor_prevents_even_valid_reviewer_from_starting() {
    let owner = PermissionController::new(Arc::new(TimedAdapter(None)));
    let probe = review_probe(Action::Allow);
    owner
        .bind(probe.clone(), Arc::new(crate::NoopAuditor))
        .unwrap();
    let context = invocation();
    let p = owner.snapshot().unwrap();
    assert!(matches!(
        owner
            .resolve_ask(
                &p,
                &request(&owner, &context, json!({})),
                None,
                &context.state,
                &issuer(),
                &context.cancellation
            )
            .await,
        Err(PermissionError::Unavailable)
    ));
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
}
struct Durable {
    writes: Mutex<Vec<(String, PolicySnapshot)>>,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    block: bool,
    uncertain: bool,
}
#[async_trait]
impl FutureGrantPersistence for Durable {
    async fn persist(
        &self,
        session: &str,
        _: u64,
        expected: &PolicySnapshot,
        next: &PolicySnapshot,
    ) -> Result<(), PermissionError> {
        assert_eq!(next.revision, expected.revision + 1);
        self.entered.notify_one();
        if self.block {
            self.release.notified().await;
        }
        if self.uncertain {
            return Err(PermissionError::PersistenceUncertain(
                "fixture storage acknowledgement lost".into(),
            ));
        }
        self.writes
            .lock()
            .unwrap()
            .push((session.into(), next.clone()));
        Ok(())
    }
}
fn grant_fixture(
    parent: bool,
    block: bool,
    uncertain: bool,
) -> (Arc<super::super::grants::StagedGrant>, Arc<Durable>) {
    let root = Arc::new(PermissionController::new(Arc::new(WaitingAsk)));
    root.bind_session_id("root").unwrap();
    let owner = if parent {
        let owner = Arc::new(PermissionController::new(Arc::new(WaitingAsk)));
        owner.bind_session_id("ancestor").unwrap();
        root.set_parent(owner.clone()).unwrap();
        owner
    } else {
        root.clone()
    };
    owner
        .configure_tools(
            vec![("job".into(), crate::runtime::tool::ToolOrigin::Builtin)],
            vec![],
        )
        .unwrap();
    let durable = Arc::new(Durable {
        writes: Mutex::new(vec![]),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        block,
        uncertain,
    });
    let existing = owner.snapshot().unwrap();
    let mut config = existing.config;
    config.overrides.push(Permission {
        agent: None,
        mode: None,
        tool: Some(Matcher::Exact("unrelated".into())),
        args: vec![],
        action: Action::Deny,
    });
    owner.replace(existing.revision, config).unwrap();
    owner.set_future_grant_persistence(durable.clone()).unwrap();
    let context = invocation();
    let mut request = request(&owner, &context, json!({"path":"src/main.rs"}));
    request.session_id = "root".into();
    let expected = owner.snapshot().unwrap();
    let lineage = root.lineage().unwrap();
    let grant = super::super::grants::StagedGrant {
        owner,
        expected,
        request,
        grant: FutureGrant {
            args: vec![ArgumentPredicate {
                pointer: "/path".into(),
                matcher: Matcher::Exact("src/main.rs".into()),
            }],
        },
        root: root.clone(),
        root_revision: root.snapshot().unwrap().revision,
        lineage,
        cancellation: context.cancellation.clone(),
        terminal: context
            .state
            .executions()
            .observe_terminal(context.execution_id.unwrap())
            .unwrap(),
    };
    (Arc::new(grant), durable)
}
#[tokio::test]
async fn future_grant_persists_exact_ask_owner_without_clobbering_overrides() {
    let (grant, durable) = grant_fixture(true, false, false);
    let root_before = grant.root.snapshot().unwrap();
    let next = grant.adopt().await.unwrap();
    assert_eq!(grant.root.snapshot().unwrap(), root_before);
    assert_eq!(durable.writes.lock().unwrap()[0].0, "ancestor");
    assert_eq!(grant.owner.snapshot().unwrap(), next);
    assert_eq!(
        &next.config.overrides[..next.config.overrides.len() - 1],
        grant.expected.config.overrides.as_slice()
    );
    let rule = next.config.overrides.last().unwrap();
    assert_eq!(rule.tool, Some(Matcher::Exact("job".into())));
    assert_eq!(rule.args, grant.grant.args);
    assert!(matches!(
        grant.adopt().await,
        Err(PermissionError::StalePolicy)
    ));
    assert_eq!(durable.writes.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn durable_adoption_reserves_ancestors_and_survives_waiter_cancellation() {
    let (grant, durable) = grant_fixture(true, true, false);
    let cloned = grant.clone();
    let waiter = tokio::spawn(async move { cloned.adopt().await });
    durable.entered.notified().await;
    for owner in [&grant.root, &grant.owner] {
        let p = owner.snapshot().unwrap();
        assert!(matches!(
            owner.replace(p.revision, p.config),
            Err(PermissionError::StalePolicy)
        ));
    }
    waiter.abort();
    let _ = waiter.await;
    durable.release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if grant.owner.snapshot().unwrap().revision > grant.expected.revision {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(durable.writes.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn uncertain_persistence_keeps_live_policy_and_fences_new_writes() {
    let (grant, durable) = grant_fixture(false, false, true);
    assert!(matches!(
        grant.adopt().await,
        Err(PermissionError::PersistenceUncertain(_))
    ));
    assert_eq!(grant.owner.snapshot().unwrap(), grant.expected);
    assert!(durable.writes.lock().unwrap().is_empty());
    assert!(matches!(
        grant
            .owner
            .replace(grant.expected.revision, grant.expected.config.clone()),
        Err(PermissionError::StalePolicy)
    ));
}
#[tokio::test]
async fn stale_ancestor_discards_proposal_without_storage_write() {
    let (grant, durable) = grant_fixture(true, false, false);
    let before = grant.root.snapshot().unwrap();
    grant.root.replace(before.revision, before.config).unwrap();
    assert!(matches!(
        grant.adopt().await,
        Err(PermissionError::StalePolicy)
    ));
    assert!(durable.writes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn successful_terminal_outcome_survives_release_and_history_replacement() {
    use crate::runtime::execution::{ExecutionStatus, FinalOutput};
    let context = invocation();
    let registry = context.state.executions();
    let id = context.execution_id.unwrap();
    let observed = registry.observe_terminal(id).unwrap();
    registry
        .complete_detached(
            id,
            FinalOutput {
                content: "done".into(),
                failure: None,
                attachments: Vec::new(),
            },
        )
        .await
        .unwrap();
    assert_eq!(*observed.borrow(), Some(ExecutionStatus::Succeeded));
    registry.restore(&[]).unwrap();
    assert!(registry.get(id).is_err());
    assert_eq!(*observed.borrow(), Some(ExecutionStatus::Succeeded));
    let (grant, durable) = grant_fixture(false, false, false);
    let mut retained = (*grant).clone();
    retained.terminal = observed;
    retained.cancellation.cancel();
    retained.adopt().await.unwrap();
    assert_eq!(durable.writes.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn lineage_revision_watches_wake_without_periodic_polling() {
    let (grant, _) = grant_fixture(true, false, false);
    let changed = grant
        .root
        .lineage_changed(grant.root_revision, &grant.lineage);
    tokio::pin!(changed);
    tokio::select! {biased; _=&mut changed=>panic!("unchanged lineage must wait"),_=tokio::task::yield_now()=>{}}
    let before = grant.owner.snapshot().unwrap();
    grant.owner.replace(before.revision, before.config).unwrap();
    changed.await.unwrap();
    assert!(matches!(
        grant
            .root
            .ensure_witness(grant.root_revision, &grant.lineage),
        Err(PermissionError::StalePolicy)
    ));
}
#[tokio::test]
async fn closed_proposal_lifetime_rejects_confirmations_without_writing() {
    let (grant, durable) = grant_fixture(false, false, false);
    grant.root.close_interactions();
    assert!(matches!(
        grant.adopt().await,
        Err(PermissionError::Cancelled)
    ));
    assert!(durable.writes.lock().unwrap().is_empty());
}

struct ProposalAdapter {
    port: Arc<TimedInteraction>,
    proposals: Mutex<Vec<FutureGrantProposal>>,
}
#[async_trait]
impl SessionPermission for ProposalAdapter {
    async fn evaluate(
        &self,
        p: &PolicySnapshot,
        r: &PermissionRequest,
    ) -> Result<Decision, PermissionError> {
        evaluate(p, r).map_err(|e| PermissionError::InvalidConfiguration(e.to_string()))
    }
    fn features(&self) -> PermissionFeatures {
        PermissionFeatures {
            interactive_ask: true,
            remembered_grants: true,
            live_edit: true,
        }
    }
    async fn open_interaction(
        &self,
        _: &PermissionRequest,
        _: &AskAttribution,
    ) -> Result<Option<Arc<dyn PermissionInteraction>>, PermissionError> {
        Ok(Some(self.port.clone()))
    }
    async fn propose_future_grant(
        &self,
        proposal: FutureGrantProposal,
        _: Arc<dyn FutureGrantAdoption>,
    ) -> Result<(), PermissionError> {
        self.proposals.lock().unwrap().push(proposal);
        Ok(())
    }
}
struct GrantReviewer;
#[async_trait]
impl crate::Client for GrantReviewer {
    async fn complete(
        &self,
        _: &crate::context::Content,
        _: &dyn crate::Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<microcrab_protocol::Response, microcrab_transport::Error> {
        Ok(microcrab_protocol::Response {
            content: serde_json::to_string(&ReviewResult {
                action: Action::Allow,
                reason: "Read exact source".into(),
                future_grant: Some(FutureGrant {
                    args: vec![ArgumentPredicate {
                        pointer: "/path".into(),
                        matcher: Matcher::Exact("src/main.rs".into()),
                    }],
                }),
            })
            .unwrap(),
            tool_calls: vec![],
            usage: Default::default(),
            provider_metadata: None,
        })
    }
}
#[tokio::test(start_paused = true)]
async fn current_admission_never_publishes_proposal_until_exact_dispatch_ack() {
    let adapter = Arc::new(ProposalAdapter {
        port: timed(100, Action::Deny, 0),
        proposals: Mutex::new(vec![]),
    });
    let owner = Arc::new(PermissionController::new(adapter.clone()));
    owner.bind_session_id("owning-session").unwrap();
    owner
        .configure_tools(
            vec![("job".into(), crate::runtime::tool::ToolOrigin::Builtin)],
            vec![],
        )
        .unwrap();
    owner
        .bind(Arc::new(GrantReviewer), Arc::new(crate::NoopAuditor))
        .unwrap();
    owner
        .bind_one_shot_executor(crate::Session::one_shot_executor())
        .unwrap();
    let policy = owner.snapshot().unwrap();
    let mut config = policy.config;
    config.overrides = vec![batch_rule(Action::Ask)];
    owner.replace(policy.revision, config).unwrap();
    let durable = Arc::new(Durable {
        writes: Mutex::new(vec![]),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        block: false,
        uncertain: false,
    });
    owner.set_future_grant_persistence(durable.clone()).unwrap();
    let context = invocation();
    let proof = owner
        .admit(
            &request(&owner, &context, json!({"path":"src/main.rs"})),
            &context,
            &issuer(),
            None,
        )
        .await
        .unwrap();
    assert!(adapter.proposals.lock().unwrap().is_empty());
    assert!(durable.writes.lock().unwrap().is_empty());
    proof.begin_dispatch(&context, None).unwrap();
    tokio::task::yield_now().await;
    assert_eq!(adapter.proposals.lock().unwrap().len(), 1);
    assert_eq!(
        adapter.proposals.lock().unwrap()[0].session_id,
        "owning-session"
    );
    assert!(durable.writes.lock().unwrap().is_empty());
    assert!(matches!(
        proof.begin_dispatch(&context, None),
        Err(PermissionError::AlreadyDispatched)
    ));
}
#[tokio::test]
async fn revoke_after_once_acceptance_before_dispatch_denies_old_proof() {
    let owner = Arc::new(PermissionController::new(Arc::new(
        super::admission::BatchAsk::default(),
    )));
    let policy = owner.snapshot().unwrap();
    let mut config = policy.config;
    config.review = false;
    config.overrides = vec![batch_rule(Action::Ask)];
    owner.replace(policy.revision, config).unwrap();
    let context = invocation();
    let proof = owner
        .admit(
            &request(&owner, &context, json!({})),
            &context,
            &issuer(),
            None,
        )
        .await
        .unwrap();
    let before = owner.snapshot().unwrap();
    let mut config = before.config;
    config.overrides = vec![batch_rule(Action::Deny)];
    owner.replace(before.revision, config).unwrap();
    assert!(matches!(
        proof.begin_dispatch(&context, None),
        Err(PermissionError::StalePolicy)
    ));
}
