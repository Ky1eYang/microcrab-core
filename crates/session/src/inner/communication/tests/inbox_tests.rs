use super::*;
use crate::runtime::journal::{Journal, MemoryJournal};
use crate::state::communication::inbox::DeliveryStatus;
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
