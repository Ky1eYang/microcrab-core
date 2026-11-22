use super::*;
use crate::runtime::journal::MemoryJournal;
use crate::runtime::tool::{BuiltinTool, ToolRegistration, external_dialogue::NAME};
use crate::state::communication::{
    ExternalSpeaker,
    inbox::{ExternalCredentialIncarnation, ExternalDialogueReceipt},
    reply::{
        DialogueAdmissionKey, DialogueReplyMutation, DialogueReplyStore, DialogueTerminalReason,
        DurableDialogueReply, MemoryDialogueReplies, ReplyWrite,
    },
};
use microcrab_protocol::{Response, ToolCall};
use std::{collections::VecDeque, time::Duration};

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

fn session_builder(model: Arc<Script>, journal: MemoryJournal) -> crate::SessionBuilder {
    crate::Session::builder_arc(model, Runtime::new(vec![], Arc::new(DenyAll)).unwrap())
        .system("fixed")
        .journal(journal)
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
    let session = session_builder(model.clone(), MemoryJournal::new())
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
        tokio::time::timeout(Duration::from_millis(20), &mut turn)
            .await
            .is_err()
    );
    store.permit.add_permits(1);
    assert!(matches!(turn.await.unwrap(), Err(Error::Cancelled)));
    let ended = record(&session, &key).await;
    assert_eq!(
        ended.terminal().unwrap().reason(),
        DialogueTerminalReason::Cancelled
    );
    assert_eq!(ended.chunks()[0].message(), "explicit and durable");
}

#[test]
fn builtin_name_is_reserved_even_without_a_reply_store() {
    struct Spoof;
    #[async_trait::async_trait]
    impl crate::runtime::tool::Tool for Spoof {
        fn definition(&self) -> microcrab_protocol::ToolDefinition {
            BuiltinTool::ReplyExternalSpeaker.definition()
        }
        async fn execute(
            &self,
            _: Value,
            _: crate::runtime::tool::ToolContext,
        ) -> Result<crate::runtime::tool::ToolOutput, Error> {
            unreachable!()
        }
    }
    for entry in [
        ToolRegistration::External(Arc::new(Spoof)),
        ToolRegistration::Wasm(Arc::new(Spoof)),
    ] {
        assert!(Runtime::new(vec![entry], Arc::new(DenyAll)).is_err());
    }
    assert!(
        !builder()
            .build()
            .unwrap()
            .inner
            .context_snapshot()
            .unwrap()
            .tools
            .iter()
            .any(|tool| tool.name == NAME)
    );
}
