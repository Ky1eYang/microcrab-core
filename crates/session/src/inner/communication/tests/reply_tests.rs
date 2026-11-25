use super::*;
use crate::policy::{Action, Matcher, Permission};
use crate::runtime::journal::MemoryJournal;
use crate::runtime::tool::{
    AdmittedDialogueReply, DialogueReplyAdapter, DialogueReplyPosition, DialogueReplyReceipt,
    DialogueReplyRegistration, DialogueReplyRequest, ToolRegistration,
};
use crate::state::communication::{
    ExternalSpeaker,
    inbox::{ExternalCredentialIncarnation, ExternalDialogueReceipt},
    reply::{
        DialogueAdmissionKey, DialogueReplyMutation, DialogueReplyStore, DialogueTerminalReason,
        DurableDialogueReply, MemoryDialogueReplies, ReplyWrite,
    },
};
use microcrab_protocol::{Response, ToolCall, ToolDefinition};
use std::{collections::VecDeque, time::Duration};

const NAME: &str = "fixture_dialogue_append";

fn definition() -> ToolDefinition {
    ToolDefinition {
        name: NAME.into(),
        description: "Test fixture append".into(),
        parameters: json!({
            "type":"object", "additionalProperties":false, "required":["dialogue_id", "message"],
            "properties":{
                "dialogue_id":{"type":"string", "pattern":"^[0-9a-f]{8}$"},
                "message":{"type":"string", "minLength":1, "maxLength":8192},
                "is_final":{"type":"boolean"},
            }
        }),
    }
}

fn rule(action: Action) -> Permission {
    Permission {
        agent: None,
        mode: None,
        tool: Some(Matcher::Exact(NAME.into())),
        args: vec![],
        action,
    }
}

#[derive(Default)]
struct Adapter {
    mutate_request: bool,
    receipt_size: Option<usize>,
    authority: Arc<tokio::sync::Mutex<()>>,
    before_guard: Option<Arc<tokio::sync::Semaphore>>,
    calls: std::sync::atomic::AtomicUsize,
    error: Mutex<Option<crate::observation::diagnostics::ToolDiagnostic>>,
}
#[async_trait::async_trait]
impl DialogueReplyAdapter for Adapter {
    async fn execute(
        &self,
        admitted: AdmittedDialogueReply,
        mut request: DialogueReplyRequest,
    ) -> Result<DialogueReplyReceipt, Error> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.mutate_request {
            request.is_final = Some(false);
        }
        if let Some(entered) = &self.before_guard {
            entered.add_permits(1);
        }
        let result = admitted
            .commit(request, self.authority.clone().lock_owned().await)
            .await;
        if let Err(error) = &result {
            *self.error.lock().unwrap() = Some(error.tool_diagnostic());
        }
        result
    }

    fn format_receipt(&self, receipt: &DialogueReplyPosition) -> String {
        if let Some(size) = self.receipt_size {
            return "x".repeat(size);
        }
        json!({"status":"committed", "dialogue_id":receipt.dialogue_id(),
            "chunk_index":receipt.chunk_index(), "is_final":receipt.is_final()})
        .to_string()
    }
}

fn registration(adapter: Arc<Adapter>) -> ToolRegistration {
    ToolRegistration::DialogueReply(DialogueReplyRegistration::new(
        definition(),
        vec![rule(Action::Allow)],
        adapter,
    ))
}

#[derive(Default)]
struct Script {
    calls: Mutex<VecDeque<Result<Response, microcrab_transport::Error>>>,
}
impl Script {
    fn batch(&self, calls: Vec<ToolCall>) {
        self.calls.lock().unwrap().push_back(Ok(Response {
            content: "unrelated assistant text".into(),
            tool_calls: calls,
            provider_metadata: None,
            usage: Default::default(),
        }));
    }
    fn timeout(&self) {
        self.calls
            .lock()
            .unwrap()
            .push_back(Err(microcrab_transport::Error::ProviderTimeout));
    }
}
#[async_trait::async_trait]
impl crate::Client for Script {
    async fn complete(
        &self,
        _: &crate::context::Content,
        _: &dyn microcrab_auditor::Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.calls
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected model round")
    }
}

struct GatedStore {
    memory: MemoryDialogueReplies,
    enter: tokio::sync::Semaphore,
    permit: tokio::sync::Semaphore,
}
#[async_trait::async_trait]
impl DialogueReplyStore for GatedStore {
    async fn load(
        &self,
        agent: &crate::state::communication::AgentId,
        key: &DialogueAdmissionKey,
    ) -> Result<Option<DurableDialogueReply>, Error> {
        self.memory.load(agent, key).await
    }
    async fn apply(&self, mutation: DialogueReplyMutation) -> Result<ReplyWrite, Error> {
        if matches!(&mutation, DialogueReplyMutation::Append { .. }) {
            self.enter.add_permits(1);
            self.permit.acquire().await.unwrap().forget();
        }
        self.memory.apply(mutation).await
    }
}

fn with_adapter(
    model: Arc<Script>,
    journal: MemoryJournal,
    adapter: Arc<Adapter>,
) -> crate::SessionBuilder {
    crate::Session::builder_arc(
        model,
        Runtime::new(vec![registration(adapter)], Arc::new(DenyAll)).unwrap(),
    )
    .system("fixed")
    .journal(journal)
}
fn session_builder(model: Arc<Script>, journal: MemoryJournal) -> crate::SessionBuilder {
    with_adapter(model, journal, Arc::new(Adapter::default()))
}
fn with_memory(model: Arc<Script>, journal: MemoryJournal) -> crate::Session {
    session_builder(model, journal)
        .dialogue_reply_store_with(|id| Ok(Arc::new(MemoryDialogueReplies::new(id.clone()))))
        .build()
        .unwrap()
}
async fn prepare(session: &crate::Session) {
    session.inner.inbox.state.lock().unwrap().paused = true;
    session
        .replace_external_speakers(vec![ExternalSpeaker {
            identity: "speaker".into(),
        }])
        .await
        .unwrap();
}
async fn enqueue(
    session: &crate::Session,
    operation: &str,
) -> (ExternalDialogueReceipt, DialogueAdmissionKey) {
    let proof = ExternalCredentialIncarnation::new([5; 32]);
    let receipt = session
        .enqueue_authenticated_external(
            operation.into(),
            "speaker".into(),
            "external request".into(),
            proof.clone(),
        )
        .await
        .unwrap();
    let (_, key) = session
        .state()
        .communication
        .unwrap()
        .inbox
        .authenticated_external_dialogue(&receipt.dialogue_id, "speaker", &proof)
        .unwrap()
        .unwrap();
    (receipt, key)
}
fn call(call_id: &str, alias: &str, message: &str, is_final: bool) -> ToolCall {
    ToolCall {
        id: call_id.into(),
        name: NAME.into(),
        arguments: json!({"dialogue_id": alias, "message": message, "is_final": is_final}),
        provider_metadata: None,
    }
}
async fn record(session: &crate::Session, key: &DialogueAdmissionKey) -> DurableDialogueReply {
    session
        .observe_external_dialogue_reply(key)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn only_exact_current_turn_chunks_relay_and_final_does_not_end_the_turn() {
    let model = Arc::new(Script::default());
    let session = with_memory(model.clone(), MemoryJournal::new());
    prepare(&session).await;
    let (one, key) = enqueue(&session, "one").await;
    let (_, no_reply) = enqueue(&session, "no-reply").await;
    model.batch(vec![
        call("progress", &one.dialogue_id, "explicit progress", false),
        call("final", &one.dialogue_id, "explicit final", true),
        call("late", &one.dialogue_id, "never relayed", false),
    ]);
    model.timeout();
    assert!(matches!(
        session.resume().await,
        Err(Error::ProviderTimeout)
    ));
    assert!(session.retry_ready());
    let open_final = record(&session, &key).await;
    assert_eq!(open_final.binding().turn().number(), 1);
    assert_eq!(
        open_final
            .chunks()
            .iter()
            .map(|chunk| chunk.message())
            .collect::<Vec<_>>(),
        ["explicit progress", "explicit final"]
    );
    assert!(open_final.chunks()[1].is_final());
    assert!(open_final.terminal().is_none());
    assert!(record(&session, &no_reply).await.terminal().is_none());
    model.batch(vec![]);
    session.resume().await.unwrap();
    let completed = record(&session, &key).await;
    assert_eq!(
        completed.terminal().unwrap().reason(),
        DialogueTerminalReason::Completed
    );
    let completed_empty = record(&session, &no_reply).await;
    assert_eq!(
        completed_empty.terminal().unwrap().reason(),
        DialogueTerminalReason::Completed
    );
    assert!(completed_empty.chunks().is_empty());
    assert!(!completed.is_no_reply() && completed_empty.is_no_reply());

    model.batch(vec![call("stale", &one.dialogue_id, "wrong Turn", false)]);
    model.batch(vec![]);
    session.submit("next turn").await.unwrap();
    assert!(record(&session, &key).await == completed);
}

#[tokio::test]
async fn restore_ends_exact_unfinished_reply_without_replaying_and_close_releases_store() {
    let model = Arc::new(Script::default());
    let journal = MemoryJournal::new();
    let identity_slot = Arc::new(Mutex::new(None));
    let slot = identity_slot.clone();
    let source = session_builder(model.clone(), journal.clone())
        .dialogue_reply_store_with(move |id| {
            let store = Arc::new(MemoryDialogueReplies::new(id.clone()));
            *slot.lock().unwrap() = Some((id.clone(), store.clone()));
            Ok(store)
        })
        .build()
        .unwrap();
    prepare(&source).await;
    let (receipt, key) = enqueue(&source, "recover").await;
    model.batch(vec![call(
        "progress",
        &receipt.dialogue_id,
        "persisted chunk",
        false,
    )]);
    model.timeout();
    assert!(matches!(source.resume().await, Err(Error::ProviderTimeout)));
    let history = journal.latest().unwrap();
    let (agent, store) = identity_slot.lock().unwrap().take().unwrap();
    // Simulate process loss without running the live close settlement.
    drop(source);
    let weak = Arc::downgrade(&store);
    let restored_model = Arc::new(Script::default());
    let copy = store.clone();
    let restored = crate::Session::restore(
        session_builder(restored_model.clone(), journal).dialogue_reply_store_with(move |id| {
            assert_eq!(id, &agent);
            Ok(copy)
        }),
        history,
    )
    .await
    .unwrap();
    let interrupted = record(&restored, &key).await;
    assert_eq!(
        interrupted.terminal().unwrap().reason(),
        DialogueTerminalReason::Interrupted
    );
    assert_eq!(interrupted.chunks().len(), 1);
    restored_model.batch(vec![call(
        "replayed",
        &receipt.dialogue_id,
        "must never replay",
        false,
    )]);
    restored_model.batch(vec![]);
    restored.resume().await.unwrap();
    assert!(record(&restored, &key).await == interrupted);
    drop(store);
    restored.close().await.unwrap();
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn cancellation_joins_effect_acknowledgement_before_settlement() {
    let model = Arc::new(Script::default());
    let slot = Arc::new(Mutex::new(None));
    let put = slot.clone();
    let adapter = Arc::new(Adapter::default());
    let session = with_adapter(model.clone(), MemoryJournal::new(), adapter.clone())
        .limits(crate::Limits {
            lifecycle_timeout_ms: 20,
            ..Default::default()
        })
        .dialogue_reply_store_with(move |id| {
            let store = Arc::new(GatedStore {
                memory: MemoryDialogueReplies::new(id.clone()),
                enter: tokio::sync::Semaphore::new(0),
                permit: tokio::sync::Semaphore::new(0),
            });
            *put.lock().unwrap() = Some(store.clone());
            Ok(store)
        })
        .build()
        .unwrap();
    let store = slot.lock().unwrap().take().unwrap();
    let agent = session.communication_identity().unwrap().agent_id;
    prepare(&session).await;
    let (receipt, key) = enqueue(&session, "cancel").await;
    model.batch(vec![call(
        "gated",
        &receipt.dialogue_id,
        "explicit and durable",
        false,
    )]);
    let runner = session.clone();
    let mut turn = tokio::spawn(async move { runner.resume().await });
    store.enter.acquire().await.unwrap().forget();
    assert!(
        store
            .memory
            .load(&agent, &key)
            .await
            .unwrap()
            .unwrap()
            .chunks()
            .is_empty()
    );
    session.interrupt();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut turn)
            .await
            .is_err()
    );
    assert!(adapter.authority.clone().try_lock_owned().is_err());
    // The generic tool worker has already dropped its waiter on timeout; the
    // still-blocked Turn is waiting on Core's reply fence, not the adapter.
    assert!(
        session
            .state_handle()
            .executions()
            .snapshot()
            .iter()
            .any(|row| { row.call_id == "gated" && !row.status.is_pending() })
    );
    store.permit.add_permits(1);
    assert!(matches!(turn.await.unwrap(), Err(Error::Cancelled)));
    assert!(adapter.authority.clone().try_lock_owned().is_ok());
    let ended = record(&session, &key).await;
    assert_eq!(
        ended.terminal().unwrap().reason(),
        DialogueTerminalReason::Cancelled
    );
    assert_eq!(ended.chunks()[0].message(), "explicit and durable");
}

#[tokio::test]
async fn reply_store_without_registration_still_settles_without_advertising_or_dispatch() {
    let model = Arc::new(Script::default());
    let session = crate::Session::builder_arc(
        model.clone(),
        Runtime::new(vec![], Arc::new(DenyAll)).unwrap(),
    )
    .system("fixed")
    .journal(MemoryJournal::new())
    .dialogue_reply_store_with(|id| Ok(Arc::new(MemoryDialogueReplies::new(id.clone()))))
    .build()
    .unwrap();
    prepare(&session).await;
    let (receipt, key) = enqueue(&session, "unadvertised").await;
    assert!(
        !session
            .inner
            .context_snapshot()
            .unwrap()
            .tools
            .iter()
            .any(|tool| tool.name == NAME)
    );
    model.batch(vec![]);
    session.resume().await.unwrap();
    assert!(record(&session, &key).await.is_no_reply());
    model.batch(vec![call(
        "hallucinated",
        &receipt.dialogue_id,
        "never relayed",
        true,
    )]);
    assert!(matches!(
        session.submit("try without the effect").await,
        Err(Error::Protocol(_))
    ));
    assert!(record(&session, &key).await.is_no_reply());
}

#[test]
fn typed_registration_survives_browser_replacement_and_is_filtered_from_non_roots() {
    let model = Arc::new(Script::default());
    let root = session_builder(model.clone(), MemoryJournal::new())
        .build()
        .unwrap();
    assert_eq!(
        root.inner.runtime.origin(NAME),
        Some(crate::runtime::ToolOrigin::DialogueReply)
    );
    assert!(
        root.inner
            .runtime
            .prepare_external_tools(vec![])
            .unwrap()
            .definitions()
            .iter()
            .any(|tool| tool.name == NAME)
    );
    let child = session_builder(model.clone(), MemoryJournal::new())
        .kind(crate::SessionKind::Delegated)
        .build()
        .unwrap();
    assert_eq!(child.inner.runtime.origin(NAME), None);
    assert!(
        session_builder(model, MemoryJournal::new())
            .kind(crate::SessionKind::OneShot)
            .build()
            .is_err()
    );
}

#[tokio::test]
async fn admitted_arguments_are_literal_closed_and_keep_optional_final_exact() {
    let model = Arc::new(Script::default());
    let session = with_memory(model.clone(), MemoryJournal::new());
    prepare(&session).await;
    let state = session.state_handle();
    state.inject("greeting", json!("Hello")).await.unwrap();
    state
        .prepare_secret_write(
            [("token".into(), "secret-plaintext".into())]
                .into_iter()
                .collect(),
        )
        .await
        .unwrap()
        .commit()
        .unwrap();
    let (receipt, key) = enqueue(&session, "literal").await;
    let mut literal = call(
        "literal",
        &receipt.dialogue_id,
        "{{var:string:greeting}} {{secret:token}}",
        false,
    );
    literal
        .arguments
        .as_object_mut()
        .unwrap()
        .remove("is_final");
    let mut null = call("null", &receipt.dialogue_id, "null is not omitted", false);
    null.arguments["is_final"] = Value::Null;
    let mut extra = call("extra", &receipt.dialogue_id, "unknown key", false);
    extra.arguments["sender"] = json!("forged");
    model.batch(vec![
        null,
        extra,
        call("wrong-alias", "abcdef00", "not admitted", false),
        call("blank", &receipt.dialogue_id, "   ", false),
        call("bytes", &receipt.dialogue_id, &"界".repeat(2731), false),
        literal,
    ]);
    model.batch(vec![]);
    session.resume().await.unwrap();
    let record = record(&session, &key).await;
    assert_eq!(record.chunks().len(), 1);
    assert_eq!(record.chunks()[0].message(), "Hello {{secret:token}}");
    assert!(!record.chunks()[0].is_final());
}

#[tokio::test]
async fn adapter_cannot_change_admitted_payload_or_exceed_receipt_budget() {
    for (mutate_request, receipt_size) in [(true, None), (false, Some(129))] {
        let model = Arc::new(Script::default());
        let adapter = Arc::new(Adapter {
            mutate_request,
            receipt_size,
            ..Default::default()
        });
        let session = with_adapter(model.clone(), MemoryJournal::new(), adapter.clone())
            .dialogue_reply_store_with(|id| Ok(Arc::new(MemoryDialogueReplies::new(id.clone()))))
            .build()
            .unwrap();
        prepare(&session).await;
        let (receipt, key) = enqueue(&session, "no-effect").await;
        let mut request = call("append", &receipt.dialogue_id, "must not commit", false);
        request
            .arguments
            .as_object_mut()
            .unwrap()
            .remove("is_final");
        model.batch(vec![request]);
        model.batch(vec![]);
        let turn = session.resume().await;
        let reply = record(&session, &key).await;
        if mutate_request {
            turn.unwrap();
            assert!(reply.is_no_reply());
        } else {
            assert!(matches!(turn, Err(Error::Limit(_))));
            assert!(reply.chunks().is_empty());
        }
        assert!(adapter.error.lock().unwrap().is_some());
        assert!(!session.inner.faulted.load(Ordering::Relaxed));
    }
}

#[tokio::test]
async fn compiled_owner_deny_and_unavailable_ask_override_exact_provider_allow() {
    for action in [Action::Deny, Action::Ask] {
        let model = Arc::new(Script::default());
        let adapter = Arc::new(Adapter::default());
        let session = with_adapter(model.clone(), MemoryJournal::new(), adapter.clone())
            .permissions(crate::policy::PermissionConfig {
                overrides: vec![rule(action)],
                ..Default::default()
            })
            .dialogue_reply_store_with(|id| Ok(Arc::new(MemoryDialogueReplies::new(id.clone()))))
            .build()
            .unwrap();
        prepare(&session).await;
        let (receipt, key) = enqueue(&session, "policy").await;
        model.batch(vec![call(
            "blocked",
            &receipt.dialogue_id,
            "must not commit",
            false,
        )]);
        model.batch(vec![]);
        session.resume().await.unwrap();
        assert!(record(&session, &key).await.is_no_reply());
        assert_eq!(adapter.calls.load(Ordering::Relaxed), 0);
    }
}

#[tokio::test]
async fn policy_change_while_adapter_waits_is_rechecked_at_append() {
    let model = Arc::new(Script::default());
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let adapter = Arc::new(Adapter {
        before_guard: Some(entered.clone()),
        ..Default::default()
    });
    let fence = adapter.authority.clone().lock_owned().await;
    let session = with_adapter(model.clone(), MemoryJournal::new(), adapter.clone())
        .dialogue_reply_store_with(|id| Ok(Arc::new(MemoryDialogueReplies::new(id.clone()))))
        .build()
        .unwrap();
    prepare(&session).await;
    let (receipt, key) = enqueue(&session, "stale").await;
    model.batch(vec![call(
        "stale",
        &receipt.dialogue_id,
        "must not commit",
        false,
    )]);
    model.batch(vec![]);
    let runner = session.clone();
    let turn = tokio::spawn(async move { runner.resume().await });
    entered.acquire().await.unwrap().forget();
    let controller = session.inner.runtime.permission_controller();
    let snapshot = controller.snapshot().unwrap();
    let mut config = snapshot.config;
    config.overrides = vec![rule(Action::Deny)];
    controller.replace_user(snapshot.revision, config).unwrap();
    drop(fence);
    turn.await.unwrap().unwrap();
    assert!(record(&session, &key).await.is_no_reply());
    assert!(adapter.error.lock().unwrap().is_some());
}

#[tokio::test]
async fn definite_failure_is_no_reply_but_ambiguous_ack_quarantines_the_session() {
    use crate::observation::diagnostics::EffectKnowledge;
    struct AmbiguousStore {
        inner: MemoryDialogueReplies,
        commit_then_lose_ack: bool,
    }
    #[async_trait::async_trait]
    impl DialogueReplyStore for AmbiguousStore {
        async fn load(
            &self,
            agent: &crate::state::communication::AgentId,
            key: &DialogueAdmissionKey,
        ) -> Result<Option<DurableDialogueReply>, Error> {
            self.inner.load(agent, key).await
        }
        async fn apply(&self, mutation: DialogueReplyMutation) -> Result<ReplyWrite, Error> {
            if let DialogueReplyMutation::Append { binding, .. } = &mutation {
                if self.commit_then_lose_ack {
                    let old = self
                        .inner
                        .load(binding.agent_id(), binding.admission())
                        .await?
                        .unwrap();
                    self.inner.apply(mutation).await?;
                    return Ok(ReplyWrite::AlreadyApplied(old));
                }
                return Err(Error::StateObservationConflict);
            }
            self.inner.apply(mutation).await
        }
    }
    for commit_then_lose_ack in [false, true] {
        let model = Arc::new(Script::default());
        let adapter = Arc::new(Adapter::default());
        let session = with_adapter(model.clone(), MemoryJournal::new(), adapter.clone())
            .dialogue_reply_store_with(move |id| {
                Ok(Arc::new(AmbiguousStore {
                    inner: MemoryDialogueReplies::new(id.clone()),
                    commit_then_lose_ack,
                }))
            })
            .build()
            .unwrap();
        prepare(&session).await;
        let (receipt, key) = enqueue(&session, "ack").await;
        model.batch(vec![call(
            "append",
            &receipt.dialogue_id,
            "only after CAS",
            false,
        )]);
        model.batch(vec![]);
        let result = session.resume().await;
        assert_eq!(result.is_err(), commit_then_lose_ack);
        assert_eq!(session.is_faulted(), commit_then_lose_ack);
        let effect = adapter.error.lock().unwrap().as_ref().unwrap().effects;
        assert_eq!(
            effect,
            if commit_then_lose_ack {
                EffectKnowledge::MayHaveOccurred
            } else {
                EffectKnowledge::NotStarted
            }
        );
        let outcome = record(&session, &key).await;
        assert_eq!(outcome.chunks().len(), usize::from(commit_then_lose_ack));
        if commit_then_lose_ack {
            assert_eq!(
                outcome.terminal().unwrap().reason(),
                DialogueTerminalReason::Interrupted
            );
        } else {
            assert!(outcome.is_no_reply());
        }
    }
}

#[tokio::test]
async fn aggregate_limit_is_checked_before_the_store_append() {
    use crate::state::communication::reply::{MAX_CHUNK_BYTES, MAX_DIALOGUE_TEXT_BYTES};
    let model = Arc::new(Script::default());
    let session = with_memory(model.clone(), MemoryJournal::new());
    prepare(&session).await;
    let (receipt, key) = enqueue(&session, "aggregate").await;
    let message = "m".repeat(MAX_CHUNK_BYTES);
    let count = MAX_DIALOGUE_TEXT_BYTES / MAX_CHUNK_BYTES;
    model.batch(
        (0..count)
            .map(|i| call(&format!("chunk-{i}"), &receipt.dialogue_id, &message, false))
            .collect(),
    );
    model.batch(vec![call(
        "over",
        &receipt.dialogue_id,
        "must not commit",
        false,
    )]);
    model.batch(vec![]);
    let _ = session.resume().await;
    let outcome = record(&session, &key).await;
    assert_eq!(outcome.chunks().len(), count);
    assert_eq!(
        outcome
            .chunks()
            .iter()
            .map(|chunk| chunk.message().len())
            .sum::<usize>(),
        MAX_DIALOGUE_TEXT_BYTES
    );
}
