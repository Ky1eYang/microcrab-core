//! Session-owned mutation ordering and atomic returned hook injections.
use microcrab_auditor::{
    AuditBody, Auditor, AuditorError, BoxFuture, HookContext, Injection, Moment, VariableView,
};
use microcrab_protocol::{Response, Usage, VariablesChange};
use microcrab_session::context::Content;
use microcrab_session::runtime::{Runtime, tool::DenyAll};
use microcrab_session::{Client, Error, Session};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

struct Answer {
    entered: Notify,
    release: Notify,
    requests: std::sync::Mutex<Vec<Content>>,
}
#[async_trait::async_trait]
impl Client for Answer {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        token: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.requests.lock().unwrap().push(content.clone());
        self.entered.notify_one();
        tokio::select! {_=token.cancelled()=>return Err(microcrab_transport::Error::Cancelled),_=self.release.notified()=>{}}
        Ok(Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: vec![],
            usage: Usage::default(),
        })
    }
}
fn runtime() -> Runtime {
    Runtime::new(vec![], Arc::new(DenyAll)).unwrap()
}
fn client() -> Arc<Answer> {
    Arc::new(Answer {
        entered: Notify::new(),
        release: Notify::new(),
        requests: std::sync::Mutex::new(vec![]),
    })
}
fn session() -> (Session, Arc<Answer>) {
    let client = client();
    (
        Session::builder_arc(client.clone(), runtime())
            .build()
            .unwrap(),
        client,
    )
}
fn set(key: &str, value: i32) -> VariablesChange {
    VariablesChange {
        scoped_set: vec![],
        set: serde_json::Map::from_iter([(key.into(), json!(value))]),
        remove: vec![],
    }
}

#[tokio::test]
async fn non_mut_state_commands_work_before_start_and_during_an_active_model_call() {
    let (session, client) = session();
    let state = session.state_handle();
    state.inject("old", json!(1)).await.unwrap();
    let running = session.clone();
    let worker = tokio::spawn(async move { running.submit("hello").await });
    client.entered.notified().await;
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        state.change(VariablesChange {
            scoped_set: vec![],
            set: set("new", 2).set,
            remove: vec!["old".into()],
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(state.variables(), json!({"new":2}));
    let replay = |content: &Content| {
        let mut values = json!({});
        for item in &content.items {
            if let microcrab_session::context::ContentItem::State(frame) = item {
                microcrab_transport::apply_state_patch(&mut values, frame).unwrap();
            }
        }
        values
    };
    let admitted = client.requests.lock().unwrap()[0].clone();
    let admitted_state = replay(&admitted);
    assert_eq!(admitted_state["old"], 1);
    assert!(admitted_state.get("new").is_none());
    let current = session.state_view().unwrap();
    assert_eq!(current["new"], 2);
    assert!(current.get("old").is_none());
    assert!(
        current["session"]["variables_revision"].as_u64().unwrap()
            > admitted_state["session"]["variables_revision"]
                .as_u64()
                .unwrap()
    );
    // The ACK commits current State; it cannot replace a transmitted request.
    assert_eq!(
        serde_json::to_value(&client.requests.lock().unwrap()[0]).unwrap(),
        serde_json::to_value(&admitted).unwrap()
    );
    client.release.notify_one();
    worker.await.unwrap().unwrap();
    client.release.notify_one();
    session.submit("next boundary").await.unwrap();
    let requests = client.requests.lock().unwrap().clone();
    let next = replay(&requests[1]);
    assert_eq!(next["new"], 2);
    assert!(next.get("old").is_none());
    assert_eq!(
        serde_json::to_value(&requests[1].items[..admitted.items.len()]).unwrap(),
        serde_json::to_value(&admitted.items).unwrap()
    );
    session.close().await.unwrap();
    assert!(matches!(
        state.inject("closed", json!(3)).await,
        Err(Error::Closed)
    ));
}

struct ChangeHook {
    change: VariablesChange,
}
impl Auditor for ChangeHook {
    fn is_enabled(&self) -> bool {
        true
    }
    fn publish(&self, _: Moment, _: AuditBody) {}
    fn intervene<'a>(
        &'a self,
        moment: Moment,
        _: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
        Box::pin(async move {
            Ok(if moment == Moment::SessionCreated {
                Injection {
                    variables: self.change.clone(),
                    ..Default::default()
                }
            } else {
                Injection::default()
            })
        })
    }
}

#[tokio::test]
async fn invalid_returned_variable_changes_fail_atomically_without_partial_writes() {
    for change in [
        VariablesChange {
            scoped_set: vec![],
            set: json!({"safe":1,"time":"forged"})
                .as_object()
                .unwrap()
                .clone(),
            remove: vec![],
        },
        VariablesChange {
            scoped_set: vec![],
            set: set("overlap", 1).set,
            remove: vec!["overlap".into()],
        },
        VariablesChange {
            scoped_set: vec![],
            set: set("safe", 1).set,
            remove: vec!["session".into()],
        },
    ] {
        let session = Session::builder_arc(client(), runtime())
            .auditor(Arc::new(ChangeHook { change }))
            .build()
            .unwrap();
        session
            .state_handle()
            .inject("retained", json!(7))
            .await
            .unwrap();
        assert!(session.start().await.is_err());
        assert_eq!(session.state_handle().variables(), json!({"retained":7}));
        assert!(matches!(
            session.submit("must not reuse a failed hook").await,
            Err(Error::Config(_))
        ));
        session.close().await.unwrap();
    }
}

struct Gate {
    entered: Notify,
    release: Notify,
    returns: AtomicUsize,
}
impl Auditor for Gate {
    fn is_enabled(&self) -> bool {
        true
    }
    fn publish(&self, _: Moment, _: AuditBody) {}
    fn intervene<'a>(
        &'a self,
        moment: Moment,
        _: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
        Box::pin(async move {
            if moment != Moment::SessionCreated {
                return Ok(Injection::default());
            }
            self.entered.notify_one();
            self.release.notified().await;
            self.returns.fetch_add(1, Ordering::SeqCst);
            Ok(Injection {
                variables: set("late", 9),
                ..Default::default()
            })
        })
    }
}

#[tokio::test]
async fn timed_out_intervention_cannot_return_late_writes_after_its_future_is_dropped() {
    let gate = Arc::new(Gate {
        entered: Notify::new(),
        release: Notify::new(),
        returns: AtomicUsize::new(0),
    });
    let session = Session::builder_arc(client(), runtime())
        .auditor(gate.clone())
        .limits(microcrab_session::Limits {
            lifecycle_timeout_ms: 25,
            ..Default::default()
        })
        .build()
        .unwrap();
    let running = session.clone();
    let start = tokio::spawn(async move { running.start().await });
    gate.entered.notified().await;
    assert!(session.state_handle().extract("late").is_none());
    assert!(matches!(start.await.unwrap(), Err(Error::Lifecycle(_))));
    gate.release.notify_one();
    tokio::task::yield_now().await;
    assert_eq!(gate.returns.load(Ordering::SeqCst), 0);
    assert!(session.state_handle().extract("late").is_none());
    session.close().await.unwrap();
}

struct Sequenced;
impl Auditor for Sequenced {
    fn is_enabled(&self) -> bool {
        true
    }
    fn publish(&self, _: Moment, _: AuditBody) {}
    fn intervene<'a>(
        &'a self,
        moment: Moment,
        ctx: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
        Box::pin(async move {
            let variables = match moment {
                Moment::SessionCreated => set("shared", 1),
                Moment::InputAdmitted => {
                    assert_eq!(ctx.variables.get("shared"), Some(json!(1)));
                    VariablesChange {
                        scoped_set: vec![],
                        set: set("shared", 2).set,
                        remove: vec!["old".into()],
                    }
                }
                Moment::RoundCompleted => {
                    assert_eq!(ctx.variables.get("shared"), Some(json!(2)));
                    set("other", 3)
                }
                _ => VariablesChange::default(),
            };
            Ok(Injection {
                variables,
                ..Default::default()
            })
        })
    }
}

#[tokio::test]
async fn ordered_hooks_can_update_the_same_key_and_remove_other_keys_without_consumer_claims() {
    let client = client();
    client.release.notify_one();
    let session = Session::builder_arc(client.clone(), runtime())
        .auditor(Arc::new(Sequenced))
        .build()
        .unwrap();
    session
        .state_handle()
        .inject("old", json!(true))
        .await
        .unwrap();
    session.submit("complete").await.unwrap();
    assert_eq!(
        session.state_handle().variables(),
        json!({"shared":2,"other":3})
    );
    let mut admitted = json!({});
    for item in &client.requests.lock().unwrap()[0].items {
        if let microcrab_session::context::ContentItem::State(frame) = item {
            microcrab_transport::apply_state_patch(&mut admitted, frame).unwrap();
        }
    }
    assert_eq!(admitted["shared"], 2);
    assert!(admitted.get("old").is_none());
    assert!(
        admitted.get("other").is_none(),
        "RoundCompleted writes belong to a later model boundary"
    );
    assert!(
        session.state_view().unwrap()["session"]["variables_revision"]
            .as_u64()
            .unwrap()
            > admitted["session"]["variables_revision"].as_u64().unwrap()
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn rejected_compaction_variable_candidate_does_not_fault_or_modify_session() {
    let (session, client) = session();
    session.start().await.unwrap();
    session
        .state_handle()
        .inject("retained", json!(7))
        .await
        .unwrap();
    let context = serde_json::to_value(session.context_snapshot().unwrap()).unwrap();
    assert!(
        session
            .compact("summary", json!({"oversized":"x".repeat(2 * 1024 * 1024)}))
            .await
            .is_err()
    );
    assert_eq!(session.state_handle().variables(), json!({"retained":7}));
    assert_eq!(
        serde_json::to_value(session.context_snapshot().unwrap()).unwrap(),
        context
    );
    client.release.notify_one();
    session
        .submit("still usable after rejected candidate")
        .await
        .unwrap();
    session.close().await.unwrap();
}

fn invisible(name: &str, value: serde_json::Value, ephemeral: bool) -> VariablesChange {
    VariablesChange {
        scoped_set: vec![microcrab_protocol::ScopedVariableSet {
            name: name.into(),
            editors: None,
            value,
            visibility: microcrab_protocol::VariableVisibility::Invisible,
            persistence: if ephemeral {
                microcrab_protocol::VariablePersistence::Ephemeral
            } else {
                microcrab_protocol::VariablePersistence::Persisted
            },
        }],
        ..Default::default()
    }
}

struct ScopedInputHook {
    calls: AtomicUsize,
    invalid: bool,
    records: std::sync::Mutex<Vec<(Moment, AuditBody)>>,
}
impl Auditor for ScopedInputHook {
    fn is_enabled(&self) -> bool {
        true
    }
    fn publish(&self, moment: Moment, body: AuditBody) {
        self.records.lock().unwrap().push((moment, body));
    }
    fn intervene<'a>(
        &'a self,
        moment: Moment,
        context: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
        Box::pin(async move {
            if moment != Moment::InputAdmitted {
                return Ok(Injection::default());
            }
            self.calls.fetch_add(1, Ordering::SeqCst);
            let attached = context
                .input()
                .expect("only this exact admission carries its input");
            assert_eq!(
                attached.variables.scoped_set[0].value,
                json!("input-ephemeral-fixture")
            );
            assert!(context.variables.get("private").is_none());
            assert_eq!(
                context.variables.get_all("private"),
                Some(json!("input-ephemeral-fixture"))
            );
            let mut variables = invisible("collision", json!("server-private-fixture"), false);
            if self.invalid {
                variables.scoped_set[0].name = "session".into();
            }
            Ok(Injection {
                variables,
                ..Default::default()
            })
        })
    }
}
fn input_hook(invalid: bool) -> Arc<ScopedInputHook> {
    Arc::new(ScopedInputHook {
        calls: AtomicUsize::new(0),
        invalid,
        records: std::sync::Mutex::new(vec![]),
    })
}
fn scoped_input() -> microcrab_protocol::SessionInput {
    let mut input = microcrab_protocol::SessionInput::text("opaque input");
    input.variables = invisible("private", json!("input-ephemeral-fixture"), true);
    input
        .variables
        .set
        .insert("collision".into(), json!("client-visible-fixture"));
    input
}

#[tokio::test]
async fn input_hook_observes_exact_scoped_input_and_server_precedence_without_secret_audit() {
    let client = client();
    client.release.notify_one();
    let hook = input_hook(false);
    let session = Session::builder_arc(client.clone(), runtime())
        .auditor(hook.clone())
        .build()
        .unwrap();
    session.submit(scoped_input()).await.unwrap();
    assert_eq!(
        session.state_handle().view_all().unwrap()["private"],
        "input-ephemeral-fixture"
    );
    assert_eq!(
        session.state_handle().view_all().unwrap()["collision"],
        "server-private-fixture"
    );
    assert!(session.state_view().unwrap().get("collision").is_none());
    let requests = serde_json::to_string(&*client.requests.lock().unwrap()).unwrap();
    assert!(!requests.contains("input-ephemeral-fixture"));
    assert!(!requests.contains("server-private-fixture"));
    assert!(!requests.contains("client-visible-fixture"));
    let (sender, mut records) = tokio::sync::mpsc::unbounded_channel();
    let producer = microcrab_auditor::ChannelAuditor::from_sender(sender);
    for (moment, body) in hook.records.lock().unwrap().iter() {
        producer.publish(*moment, body.clone());
    }
    let mut serialized = Vec::new();
    while let Ok(record) = records.try_recv() {
        serialized.push(record);
    }
    assert!(
        serialized
            .iter()
            .any(|record| record.event == Moment::InputAdmitted)
    );
    let audit = serde_json::to_string(&serialized).unwrap();
    assert!(!audit.contains("input-ephemeral-fixture"));
    assert!(!audit.contains("server-private-fixture"));
    let recover = session.checkpoint().await.unwrap();
    assert!(recover.state.variables.get_all("private").is_none());
    assert_eq!(
        recover.state.variables.get_all("collision"),
        Some(&json!("server-private-fixture"))
    );
    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    session.close().await.unwrap();
    assert!(
        session
            .state_handle()
            .view_all()
            .unwrap()
            .get("private")
            .is_none()
    );
}

#[tokio::test]
async fn invalid_input_hook_injection_does_not_commit_client_or_hook_variables() {
    let hook = input_hook(true);
    let session = Session::builder_arc(client(), runtime())
        .auditor(hook.clone())
        .build()
        .unwrap();
    session
        .state_handle()
        .inject("retained", json!(7))
        .await
        .unwrap();
    assert!(session.submit(scoped_input()).await.is_err());
    let values = session.state_handle().view_all().unwrap();
    assert_eq!(values["retained"], 7);
    assert!(values.get("collision").is_none());
    assert!(values.get("private").is_none());
    assert_eq!(session.state().last_turn_id(), 0);
    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    session.close().await.unwrap();
}

#[tokio::test]
async fn retry_keeps_live_ephemeral_without_reauditing_or_reapplying_admitted_input() {
    struct Once(AtomicUsize);
    #[async_trait::async_trait]
    impl Client for Once {
        async fn complete(
            &self,
            _: &Content,
            _: &dyn Auditor,
            _: &dyn VariableView,
            _: CancellationToken,
        ) -> Result<Response, microcrab_transport::Error> {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(microcrab_transport::Error::ProviderTimeout);
            }
            Ok(Response {
                provider_metadata: None,
                content: "retried".into(),
                tool_calls: vec![],
                usage: Default::default(),
            })
        }
    }
    let hook = input_hook(false);
    let session = Session::builder(Once(AtomicUsize::new(0)), runtime())
        .auditor(hook.clone())
        .build()
        .unwrap();
    assert!(session.submit(scoped_input()).await.is_err());
    session
        .state_handle()
        .change(invisible("private", json!("updated-live-fixture"), true))
        .await
        .unwrap();
    assert_eq!(session.resume().await.unwrap().content, "retried");
    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        session.state_handle().view_all().unwrap()["private"],
        "updated-live-fixture"
    );
    assert!(
        session
            .checkpoint()
            .await
            .unwrap()
            .state
            .variables
            .get_all("private")
            .is_none()
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn fork_has_persisted_invisible_state_but_no_live_ephemeral_and_no_cross_session_writes() {
    let answer = client();
    answer.release.notify_one();
    let source = Session::builder_arc(answer, runtime())
        .session_id("scope-source")
        .build()
        .unwrap();
    source
        .state_handle()
        .change(invisible("persisted", json!(1), false))
        .await
        .unwrap();
    source
        .state_handle()
        .change(invisible("temporary", json!(2), true))
        .await
        .unwrap();
    source.submit("settled source").await.unwrap();
    let before = serde_json::to_value(source.context_snapshot().unwrap()).unwrap();
    let target = source
        .fork(Session::builder_arc(client(), runtime()).session_id("scope-fork"))
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(target.context_snapshot().unwrap()).unwrap(),
        before
    );
    assert_eq!(target.state_handle().view_all().unwrap()["persisted"], 1);
    assert!(
        target
            .state_handle()
            .view_all()
            .unwrap()
            .get("temporary")
            .is_none()
    );
    target
        .state_handle()
        .change(invisible("persisted", json!(3), true))
        .await
        .unwrap();
    assert_eq!(source.state_handle().view_all().unwrap()["persisted"], 1);
    assert_eq!(source.state_handle().view_all().unwrap()["temporary"], 2);
    target.close().await.unwrap();
    source.close().await.unwrap();
}

#[tokio::test]
async fn public_compaction_and_fork_preserve_agent_ownership_without_runtime_bypass() {
    let (source, _) = session();
    source.start().await.unwrap();
    let state = source.state_handle();
    let invocation = microcrab_session::runtime::tool::ToolContext::new(
        state.clone(),
        state.processes(),
        CancellationToken::new(),
        "fixture-call".into(),
        "fixture-native".into(),
        "fixture-session".into(),
        "fixture-agent".into(),
        None,
    );
    invocation
        .state
        .inject("agent_owned", json!(1))
        .await
        .unwrap();
    let original = serde_json::to_value(source.context_snapshot().unwrap()).unwrap();
    assert!(matches!(
        source
            .compact("denied", json!({"agent_owned":2,"partial":true}))
            .await,
        Err(Error::VariableForbidden(_))
    ));
    assert_eq!(
        serde_json::to_value(source.context_snapshot().unwrap()).unwrap(),
        original
    );
    assert!(state.extract("partial").is_none());
    source
        .compact("allowed", json!({"client_owned":3}))
        .await
        .unwrap();
    assert_eq!(state.extract("agent_owned"), Some(json!(1)));
    assert_eq!(
        state.variable_metadata().unwrap()["agent_owned"]["owner"],
        "agent"
    );
    let target = source
        .fork(Session::builder_arc(client(), runtime()).session_id("governance-fork"))
        .await
        .unwrap();
    let target_state = target.state_handle();
    assert_eq!(
        target_state.variable_metadata().unwrap()["agent_owned"],
        state.variable_metadata().unwrap()["agent_owned"]
    );
    assert!(matches!(
        target_state.inject("agent_owned", json!(9)).await,
        Err(Error::VariableForbidden(_))
    ));
    assert_ne!(
        target_state
            .inspection()
            .unwrap()
            .witness
            .runtime_instance_id,
        state.inspection().unwrap().witness.runtime_instance_id
    );
    let recovered = source.checkpoint().await.unwrap();
    let frozen = serde_json::to_vec(&recovered).unwrap();
    let decoded: microcrab_session::Recover = serde_json::from_slice(&frozen).unwrap();
    assert_eq!(serde_json::to_vec(&decoded).unwrap(), frozen);
    assert_eq!(
        decoded.state.variables.metadata()["agent_owned"].owner,
        microcrab_protocol::VariableRole::Agent
    );
    target.close().await.unwrap();
    source.close().await.unwrap();
}

#[tokio::test]
async fn pending_scoped_input_idle_steer_normalizes_to_queue_and_keeps_changes_attached() {
    let hook = input_hook(false);
    let session = Session::builder_arc(client(), runtime())
        .auditor(hook.clone())
        .build()
        .unwrap();
    session.queue(scoped_input()).unwrap();
    let initial = session.inbox_snapshot();
    let entry = &initial.items[0];
    let mut replacement = scoped_input();
    replacement.variables.scoped_set[0].value = json!("replacement-pending-fixture");
    let edited = session
        .update_pending_input(
            &initial.inbox_id,
            entry.id,
            entry.revision,
            Some(replacement.clone()),
            Some(microcrab_session::PendingInputMode::Steer),
        )
        .unwrap();
    assert_eq!(edited.input, replacement);
    // Without an active turn, the Inbox owner normalizes steer into the next-turn queue.
    assert_eq!(edited.mode, microcrab_session::PendingInputMode::Queue);
    assert!(
        session
            .state_handle()
            .view_all()
            .unwrap()
            .get("private")
            .is_none()
    );
    assert!(
        session
            .update_pending_input(
                &initial.inbox_id,
                entry.id,
                entry.revision,
                Some(scoped_input()),
                None
            )
            .is_err()
    );
    let removed = session
        .remove_pending_input(&initial.inbox_id, edited.id, edited.revision)
        .unwrap();
    assert_eq!(removed.input, replacement);
    assert!(session.pending_inputs().is_empty());
    session.steer(scoped_input()).unwrap();
    let steered = session.inbox_snapshot();
    session
        .remove_pending_input(
            &steered.inbox_id,
            steered.items[0].id,
            steered.items[0].revision,
        )
        .unwrap();
    assert!(
        session
            .state_handle()
            .view_all()
            .unwrap()
            .get("private")
            .is_none()
    );
    assert_eq!(hook.calls.load(Ordering::SeqCst), 0);
    session.close().await.unwrap();
}

#[tokio::test]
async fn queued_scoped_input_uses_the_same_atomic_hook_admission() {
    let answer = client();
    let hook = input_hook(false);
    let session = Session::builder_arc(answer.clone(), runtime())
        .auditor(hook.clone())
        .build()
        .unwrap();
    session.queue_and_activate(scoped_input()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), answer.entered.notified())
        .await
        .unwrap();
    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        session.state_handle().view_all().unwrap()["private"],
        "input-ephemeral-fixture"
    );
    assert!(session.pending_inputs().is_empty());
    session.close().await.unwrap();
}

#[tokio::test]
async fn live_ephemeral_survives_turn_round_compaction_and_environment_boundaries() {
    struct LifecycleClient {
        calls: AtomicUsize,
        requests: std::sync::Mutex<Vec<Content>>,
    }
    #[async_trait::async_trait]
    impl Client for LifecycleClient {
        async fn complete(
            &self,
            content: &Content,
            _: &dyn Auditor,
            variables: &dyn VariableView,
            _: CancellationToken,
        ) -> Result<Response, microcrab_transport::Error> {
            assert!(variables.get("lifecycle_private").is_none());
            assert_eq!(
                variables.get_all("lifecycle_private"),
                Some(json!("live-lifecycle-fixture"))
            );
            self.requests.lock().unwrap().push(content.clone());
            let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
            Ok(Response {
                provider_metadata: None,
                content: if first { "inspect" } else { "done" }.into(),
                tool_calls: if first {
                    vec![microcrab_protocol::ToolCall {
                        provider_metadata: None,
                        id: "lifecycle-probe-call".into(),
                        name: "lifecycle_probe".into(),
                        arguments: json!({}),
                    }]
                } else {
                    vec![]
                },
                usage: Default::default(),
            })
        }
    }
    struct LifecycleTool(&'static str);
    #[async_trait::async_trait]
    impl microcrab_session::runtime::tool::Tool for LifecycleTool {
        fn definition(&self) -> microcrab_protocol::ToolDefinition {
            microcrab_protocol::ToolDefinition {
                name: "lifecycle_probe".into(),
                description: self.0.into(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn execute(
            &self,
            _: serde_json::Value,
            context: microcrab_session::runtime::tool::ToolContext,
        ) -> Result<microcrab_session::runtime::execution::ToolOutput, Error> {
            assert_eq!(
                context.state.view_all()?["lifecycle_private"],
                "live-lifecycle-fixture"
            );
            Ok(microcrab_session::runtime::execution::ToolOutput::text(
                "fixture checked",
            ))
        }
    }
    let client = Arc::new(LifecycleClient {
        calls: AtomicUsize::new(0),
        requests: std::sync::Mutex::new(vec![]),
    });
    let runtime = Runtime::new(
        vec![microcrab_session::runtime::ToolRegistration::External(
            Arc::new(LifecycleTool("before environment update")),
        )],
        Arc::new(microcrab_session::runtime::tool::AllowTools::new([
            "lifecycle_probe",
        ])),
    )
    .unwrap();
    let session = Session::builder_arc(client.clone(), runtime)
        .build()
        .unwrap();
    session
        .state_handle()
        .change(invisible(
            "lifecycle_private",
            json!("live-lifecycle-fixture"),
            true,
        ))
        .await
        .unwrap();
    let first = session.submit("exercise two rounds").await.unwrap();
    assert_eq!(first.rounds, 2);
    assert_eq!(
        session.state_handle().view_all().unwrap()["lifecycle_private"],
        "live-lifecycle-fixture"
    );
    let original_epoch = session.context_snapshot().unwrap().epoch;
    session
        .compact("lifecycle summary", json!({"compacted":true}))
        .await
        .unwrap();
    assert_eq!(
        session.state_handle().view_all().unwrap()["lifecycle_private"],
        "live-lifecycle-fixture"
    );
    session
        .replace_external_tools(vec![Arc::new(LifecycleTool("after environment update"))])
        .await
        .unwrap();
    session
        .submit("materialize the environment boundary")
        .await
        .unwrap();
    session.submit("ordinary next turn").await.unwrap();
    assert!(session.context_snapshot().unwrap().epoch > original_epoch);
    assert_eq!(
        session.state_handle().view_all().unwrap()["lifecycle_private"],
        "live-lifecycle-fixture"
    );
    assert_eq!(
        session.state_handle().variable_metadata().unwrap()["lifecycle_private"],
        json!({"visibility":"invisible","persistence":"ephemeral","owner":"client","editors":["client"]})
    );
    assert!(
        session
            .state_view()
            .unwrap()
            .get("lifecycle_private")
            .is_none()
    );
    let requests = client.requests.lock().unwrap();
    assert_eq!(
        requests.last().unwrap().tools[0].description,
        "after environment update"
    );
    assert!(
        !serde_json::to_string(&*requests)
            .unwrap()
            .contains("live-lifecycle-fixture")
    );
    drop(requests);
    assert!(
        session
            .checkpoint()
            .await
            .unwrap()
            .state
            .variables
            .get_all("lifecycle_private")
            .is_none()
    );
    session.close().await.unwrap();
}
