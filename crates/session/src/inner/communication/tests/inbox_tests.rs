use super::*;
use crate::runtime::journal::{Journal, MemoryJournal};
use crate::state::communication::inbox::{DeliveryStatus, ExternalCredentialIncarnation};
use crate::state::communication::{ExternalSpeaker, SessionInteraction};
use microcrab_transport::UserSource;

#[tokio::test]
async fn accepted_receipt_survives_restart_deduplicates_and_keeps_revoked_witness() {
    let journal = MemoryJournal::new();
    let session = builder().journal(journal.clone()).build().unwrap();
    session.inner.inbox.state.lock().unwrap().paused = true;
    let before = session.checkpoint().await.unwrap();
    session
        .replace_external_speakers(vec![ExternalSpeaker {
            identity: "speaker".into(),
        }])
        .await
        .unwrap();
    let receipt = session
        .enqueue_external("operation-a".into(), "speaker".into(), "accepted".into())
        .await
        .unwrap();
    assert_eq!(receipt.status, DeliveryStatus::Queued);
    assert_eq!(
        session
            .enqueue_external("operation-a".into(), "speaker".into(), "accepted".into())
            .await
            .unwrap(),
        receipt
    );
    assert!(
        session
            .enqueue_external("operation-a".into(), "speaker".into(), "different".into())
            .await
            .is_err()
    );
    session.replace_external_speakers(vec![]).await.unwrap();
    assert!(
        session
            .enqueue_external("new-operation".into(), "speaker".into(), "rejected".into())
            .await
            .is_err()
    );
    assert_eq!(
        session.state_view().unwrap()["external_speakers"],
        json!([{"identity":"speaker"}])
    );
    let restored = crate::Session::restore(builder().journal(journal.clone()), before)
        .await
        .unwrap();
    restored.inner.inbox.state.lock().unwrap().paused = true;
    restored.inner.restore_communication_inbox().await.unwrap();
    assert_eq!(
        restored.delivery_receipt("operation-a").await.unwrap(),
        Some(receipt)
    );
    let communication = restored.state().communication.unwrap();
    assert_eq!(communication.inbox.pending.len(), 1);
    assert!(
        matches!(&communication.inbox.pending[0].source,UserSource::External{identity} if identity=="speaker")
    );
    assert_eq!(
        restored.state_view().unwrap()["external_speakers"],
        json!([{"identity":"speaker"}])
    );
}

#[tokio::test]
async fn replay_legacy_external_requires_exact_unbound_admission_and_stays_read_only() {
    let journal = MemoryJournal::new();
    let session = builder().journal(journal.clone()).build().unwrap();
    session.inner.inbox.state.lock().unwrap().paused = true;
    let before = session.checkpoint().await.unwrap();
    session
        .replace_external_speakers(vec![ExternalSpeaker {
            identity: "speaker".into(),
        }])
        .await
        .unwrap();
    let queued = session
        .enqueue_external("legacy".into(), "speaker".into(), "body".into())
        .await
        .unwrap();
    session
        .enqueue_authenticated_external(
            "bound".into(),
            "speaker".into(),
            "body".into(),
            ExternalCredentialIncarnation::new([7; 32]),
        )
        .await
        .unwrap();
    let mut checkpoint = session.checkpoint().await.unwrap();
    let id = session.communication_identity().unwrap().agent_id;
    let original = journal.load_communication(&id).await.unwrap().unwrap();
    let prior = before.state.communication.as_ref().unwrap().inbox.clone();
    let cold = crate::Session::restore(builder().journal(journal.clone()), before)
        .await
        .unwrap();
    let replay = |session: crate::Session,
                  operation: &'static str,
                  identity: &'static str,
                  body: &'static str| async move {
        session
            .replay_legacy_external(operation.into(), identity.into(), body.into())
            .await
    };
    assert_eq!(
        replay(cold.clone(), "legacy", "speaker", "body")
            .await
            .unwrap(),
        queued
    );
    for (operation, identity, body) in [
        ("legacy", "speaker", "changed"),
        ("legacy", "other", "body"),
        ("bound", "speaker", "body"),
        ("new", "speaker", "body"),
    ] {
        assert!(
            replay(cold.clone(), operation, identity, body)
                .await
                .is_err()
        );
    }
    assert!(
        !cold
            .inner
            .inbox_worker_started
            .load(std::sync::atomic::Ordering::Acquire)
    );
    assert!(
        !cold
            .inner
            .communication_loaded
            .load(std::sync::atomic::Ordering::Acquire)
    );
    assert_eq!(cold.state().communication.unwrap().inbox, prior);

    // Model the crash window where consumption is in the selected checkpoint
    // but not in the sidecar. A replay observes it without repairing the sidecar.
    let inbox = &mut checkpoint.state.communication.as_mut().unwrap().inbox;
    inbox.pending.retain(|row| row.operation_id != "legacy");
    inbox
        .receipts
        .iter_mut()
        .find(|row| row.receipt.operation_id == "legacy")
        .unwrap()
        .receipt
        .status = DeliveryStatus::Consumed;
    let cold = crate::Session::restore(builder().journal(journal.clone()), checkpoint)
        .await
        .unwrap();
    let receipt = replay(cold.clone(), "legacy", "speaker", "body")
        .await
        .unwrap();
    assert_eq!(receipt.ordinal, queued.ordinal);
    assert_eq!(receipt.status, DeliveryStatus::Consumed);
    assert_eq!(
        journal.load_communication(&id).await.unwrap(),
        Some(original)
    );
    assert!(
        !cold
            .inner
            .inbox_worker_started
            .load(std::sync::atomic::Ordering::Acquire)
    );
}

#[tokio::test]
async fn authenticated_dialogue_replays_exactly_and_observes_only_its_incarnation() {
    let journal = MemoryJournal::new();
    let session = builder().journal(journal.clone()).build().unwrap();
    session.inner.inbox.state.lock().unwrap().paused = true;
    let before = session.checkpoint().await.unwrap();
    session
        .replace_external_speakers(vec![ExternalSpeaker {
            identity: "speaker".into(),
        }])
        .await
        .unwrap();
    let proof = ExternalCredentialIncarnation::new([7; 32]);
    let replacement = ExternalCredentialIncarnation::new([8; 32]);
    let enqueue = |session: crate::Session,
                   operation: &'static str,
                   identity: &'static str,
                   body: &'static str,
                   proof| async move {
        session
            .enqueue_authenticated_external(operation.into(), identity.into(), body.into(), proof)
            .await
    };
    let (first, retry) = tokio::join!(
        enqueue(session.clone(), "exact", "speaker", "body", proof.clone()),
        enqueue(session.clone(), "exact", "speaker", "body", proof.clone()),
    );
    let first = first.unwrap();
    assert_eq!(retry.unwrap(), first);
    assert_eq!(first.dialogue_id.len(), 8);
    assert!(
        first
            .dialogue_id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    );
    assert!(
        enqueue(
            session.clone(),
            "exact",
            "speaker",
            "body",
            replacement.clone()
        )
        .await
        .is_err()
    );
    assert!(
        enqueue(
            session.clone(),
            "exact",
            "speaker",
            "changed",
            proof.clone()
        )
        .await
        .is_err()
    );
    assert!(
        enqueue(session.clone(), "exact", "other", "body", proof.clone())
            .await
            .is_err()
    );
    assert!(
        session
            .enqueue_external("exact".into(), "speaker".into(), "body".into())
            .await
            .is_err()
    );
    assert!(session.delivery_receipt("exact").await.unwrap().is_none());
    let legacy = session
        .enqueue_external("legacy".into(), "speaker".into(), "body".into())
        .await
        .unwrap();
    assert!(
        enqueue(session.clone(), "legacy", "speaker", "body", proof.clone())
            .await
            .is_err()
    );
    assert_eq!(
        session.delivery_receipt("legacy").await.unwrap(),
        Some(legacy)
    );
    assert!(
        session
            .authenticated_external_receipt("legacy", "speaker", &proof)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        session.state().communication.unwrap().inbox.pending.len(),
        2
    );
    let context = session.inner.context_snapshot().unwrap();
    assert_eq!(
        serde_json::to_vec(&context).unwrap(),
        serde_json::to_vec(&before.context).unwrap()
    );
    assert_eq!(
        session.state_view().unwrap()["external_speakers"],
        json!([{"identity":"speaker"}])
    );

    // Simulate acknowledged consumption before restart; the alias is permanent.
    {
        let _guard = session.inner.communication_transaction.lock().await;
        session
            .inner
            .acknowledge_carrier_run_locked(&["exact".into()])
            .await
            .unwrap();
    }
    let restored = crate::Session::restore(builder().journal(journal), before)
        .await
        .unwrap();
    restored.inner.inbox.state.lock().unwrap().paused = true;
    assert!(
        restored
            .authenticated_external_receipt("exact", "speaker", &replacement)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        restored
            .authenticated_external_receipt("exact", "other", &proof)
            .await
            .unwrap()
            .is_none()
    );
    assert!(restored.delivery_receipt("exact").await.unwrap().is_none());
    let consumed = restored
        .authenticated_external_receipt("exact", "speaker", &proof)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(consumed.dialogue_id, first.dialogue_id);
    assert_eq!(consumed.receipt.ordinal, first.receipt.ordinal);
    assert_eq!(consumed.receipt.status, DeliveryStatus::Consumed);
    assert_eq!(
        enqueue(restored, "exact", "speaker", "body", proof)
            .await
            .unwrap(),
        consumed
    );
}

#[tokio::test]
async fn full_inbox_rejects_append_without_replacing_accepted_receipts() {
    let session = builder().build().unwrap();
    session.inner.inbox.state.lock().unwrap().paused = true;
    session
        .replace_external_speakers(vec![ExternalSpeaker {
            identity: "speaker".into(),
        }])
        .await
        .unwrap();
    for ordinal in 0..128 {
        session
            .enqueue_external(format!("op{ordinal}"), "speaker".into(), "body".into())
            .await
            .unwrap();
    }
    let before = session.state().communication.unwrap().inbox;
    assert!(
        session
            .enqueue_external("overflow".into(), "speaker".into(), "body".into())
            .await
            .is_err()
    );
    assert_eq!(session.state().communication.unwrap().inbox, before);
    assert_eq!(session.delivery_receipt("overflow").await.unwrap(), None);
}

#[tokio::test]
async fn initial_catalog_is_fixed_by_lineage_and_return_interaction() {
    let root = builder().build().unwrap();
    let root_tools = root.inner.context_snapshot().unwrap().tools;
    assert!(root_tools.iter().any(|tool| tool.name == "send_message"));
    assert!(!root_tools.iter().any(|tool| tool.name == "escalate"));
    let child = builder()
        .agent(modes())
        .kind(crate::SessionKind::Delegated)
        .build()
        .unwrap();
    root.inner.bind_child_communication(&child).unwrap();
    let child_tools = child.inner.context_snapshot().unwrap().tools;
    assert!(child_tools.iter().any(|tool| tool.name == "send_message"));
    assert!(child_tools.iter().any(|tool| tool.name == "escalate"));
    assert!(!child_tools.iter().any(|tool| tool.name == "delegate"));
    let returning = builder()
        .agent(modes())
        .kind(crate::SessionKind::Delegated)
        .interaction(SessionInteraction::Return)
        .build()
        .unwrap();
    let tools = returning.inner.context_snapshot().unwrap().tools;
    assert!(
        !tools
            .iter()
            .any(|tool| matches!(tool.name.as_str(), "send_message" | "escalate" | "delegate"))
    );
    assert_eq!(root.inner.context_snapshot().unwrap().tools, root_tools);
}

#[tokio::test]
async fn cancelling_enqueue_caller_does_not_abandon_owned_durable_commit() {
    struct Gated {
        memory: MemoryJournal,
        entered: tokio::sync::Notify,
        release: tokio::sync::Semaphore,
    }
    #[async_trait::async_trait]
    impl Journal for Gated {
        fn supports_communication(&self) -> bool {
            true
        }
        async fn commit(&self, recover: crate::runtime::journal::Recover) -> Result<(), Error> {
            self.memory.commit(recover).await
        }
        async fn commit_communication(
            &self,
            id: &crate::state::communication::AgentId,
            inbox: crate::state::communication::inbox::DurableInbox,
        ) -> Result<(), Error> {
            self.entered.notify_one();
            self.release
                .acquire()
                .await
                .map_err(|_| Error::Closed)?
                .forget();
            self.memory.commit_communication(id, inbox).await
        }
        async fn load_communication(
            &self,
            id: &crate::state::communication::AgentId,
        ) -> Result<Option<crate::state::communication::inbox::DurableInbox>, Error> {
            self.memory.load_communication(id).await
        }
    }
    let journal = Arc::new(Gated {
        memory: MemoryJournal::new(),
        entered: Default::default(),
        release: tokio::sync::Semaphore::new(0),
    });
    let session = builder().journal_arc(journal.clone()).build().unwrap();
    session.inner.inbox.state.lock().unwrap().paused = true;
    session
        .replace_external_speakers(vec![ExternalSpeaker {
            identity: "speaker".into(),
        }])
        .await
        .unwrap();
    let caller = session.clone();
    let task = tokio::spawn(async move {
        caller
            .enqueue_external("cancelled-caller".into(), "speaker".into(), "keep".into())
            .await
    });
    journal.entered.notified().await;
    task.abort();
    journal.release.add_permits(1);
    let receipt = session
        .delivery_receipt("cancelled-caller")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.status, DeliveryStatus::Queued);
    assert_eq!(
        session.state().communication.unwrap().inbox.pending.len(),
        1
    );
}
