use super::*;
use microcrab_session::state::communication::{
    AgentId,
    inbox::{AcceptedCarrier, DeliveryStatus, DurableInbox, OperationReceipt, QueuedReceipt},
};
fn inbox() -> DurableInbox {
    let carrier:AcceptedCarrier=serde_json::from_value(serde_json::json!({"operation_id":"accepted","ordinal":1,"body":"body","source":{"external":{"identity":"speaker"}},"speaker":{"identity":"speaker"}})).unwrap();
    use sha2::{Digest, Sha256};
    let digest = format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&(
                &carrier.source,
                &carrier.body,
                &carrier.correspondent,
                &carrier.speaker
            ))
            .unwrap()
        )
    );
    DurableInbox {
        revision: 1,
        next_ordinal: 1,
        pending: vec![carrier],
        receipts: vec![OperationReceipt {
            receipt: QueuedReceipt {
                operation_id: "accepted".into(),
                ordinal: 1,
                status: DeliveryStatus::Queued,
            },
            digest,
        }],
    }
}
fn selected_consumption(inbox: DurableInbox, agent_id: &AgentId) -> Recover {
    use microcrab_session::{Limits, SessionState, context::Context};
    let limits = Limits::default();
    let context = Context::new("system".into(), vec![], limits.max_context_bytes).unwrap();
    let mut state = SessionState::new(limits);
    state.communication = Some(
        serde_json::from_value(serde_json::json!({
            "identity": {
                "agent_id": agent_id,
                "depth": 0,
                "interaction": "persistent",
                "lineage": "lineage"
            },
            "inbox": inbox
        }))
        .unwrap(),
    );
    Recover {
        state,
        context: context.snapshot().unwrap(),
        continuation: None,
        workspace: None,
        cache_report: None,
        agent: None,
        prompt_config: None,
    }
}
#[test]
fn inspected_consumption_requires_exact_digest_and_ordinal() {
    let id = AgentId::try_from("agt_0123456789abcdef".to_owned()).unwrap();
    let stored = StoredInbox {
        session_id: "session".into(),
        agent_id: id.clone(),
        inbox: inbox(),
    };
    let mut consumed = stored.inbox.clone();
    consumed.pending.clear();
    consumed.receipts[0].receipt.status = DeliveryStatus::Consumed;
    let selected = selected_consumption(consumed.clone(), &id);
    let inspected = reconcile_inspected(stored.clone(), "session", &id, Some(&selected)).unwrap();
    assert!(inspected.pending.is_empty());
    assert_eq!(
        inspected.receipts[0].receipt.status,
        DeliveryStatus::Consumed
    );

    let mut wrong_digest = consumed.clone();
    wrong_digest.receipts[0].digest = "0".repeat(64);
    let mut wrong_ordinal = consumed;
    wrong_ordinal.next_ordinal = 2;
    wrong_ordinal.receipts[0].receipt.ordinal = 2;
    for conflicting in [wrong_digest, wrong_ordinal] {
        conflicting.validate().unwrap();
        let selected = selected_consumption(conflicting, &id);
        let error = reconcile_inspected(stored.clone(), "session", &id, Some(&selected))
            .expect_err("conflicting selected consumption must not report a receipt");
        assert!(error.to_string().contains("consumption witness mismatch"));
    }
}
async fn exercise(journal: &CreatorJournal) {
    let id = AgentId::try_from("agt_0123456789abcdef".to_owned()).unwrap();
    let wrong = AgentId::try_from("agt_1123456789abcdef".to_owned()).unwrap();
    let original = inbox();
    let busy = journal.selection.lock().await;
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        journal.commit_communication(&id, original.clone()),
    )
    .await
    .expect("busy conversation selection must not block accepted inbox persistence")
    .unwrap();
    drop(busy);
    journal
        .commit_communication(&id, original.clone())
        .await
        .unwrap();
    journal
        .commit_communication(&id, original.clone())
        .await
        .unwrap();
    assert_eq!(
        journal.load_communication(&id).await.unwrap(),
        Some(original.clone())
    );
    assert!(journal.load_communication(&wrong).await.is_err());
    assert!(
        journal
            .commit_communication(&wrong, original.clone())
            .await
            .is_err()
    );
    let mut bad = original.clone();
    bad.revision = 3;
    assert!(journal.commit_communication(&id, bad).await.is_err());
    let mut consumed = original.clone();
    consumed.revision = 2;
    consumed.pending.clear();
    consumed.receipts[0].receipt.status = DeliveryStatus::Consumed;
    journal
        .commit_communication(&id, consumed.clone())
        .await
        .unwrap();
    let mut replay = original;
    replay.revision = 3;
    assert!(journal.commit_communication(&id, replay).await.is_err());
    assert_eq!(
        journal.load_communication(&id).await.unwrap(),
        Some(consumed)
    );
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_sidecar_restart_exact_identity_revision_and_no_replay() {
    let directory =
        std::env::temp_dir().join(format!("microcrab-communication-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let (journal, _) =
        CreatorJournal::open(directory.to_str().unwrap(), "session".into(), true, false)
            .await
            .unwrap();
    exercise(&journal).await;
    Journal::close(&journal).await.unwrap();
    drop(journal);
    let (reopened, _) =
        CreatorJournal::open(directory.to_str().unwrap(), "session".into(), true, false)
            .await
            .unwrap();
    let id = AgentId::try_from("agt_0123456789abcdef".to_owned()).unwrap();
    let inspected =
        super::super::FileJournal::inspect_communication(&directory, "session".into(), id.clone())
            .await
            .unwrap()
            .unwrap();
    let restored = reopened.load_communication(&id).await.unwrap().unwrap();
    assert_eq!(inspected, restored);
    assert_eq!(
        restored.receipts[0].receipt.status,
        DeliveryStatus::Consumed
    );
    assert!(restored.pending.is_empty());
    Journal::close(&reopened).await.unwrap();
}
#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_sidecar_restart_exact_identity_revision_and_no_replay() {
    let directory =
        std::env::temp_dir().join(format!("microcrab-communication-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join("history.db");
    let (journal, _) = CreatorJournal::open_sqlite(&path, "session".into(), false)
        .await
        .unwrap();
    exercise(&journal).await;
    Journal::close(&journal).await.unwrap();
    drop(journal);
    let (reopened, _) = CreatorJournal::open_sqlite(&path, "session".into(), false)
        .await
        .unwrap();
    let id = AgentId::try_from("agt_0123456789abcdef".to_owned()).unwrap();
    assert_eq!(
        reopened
            .load_communication(&id)
            .await
            .unwrap()
            .unwrap()
            .receipts[0]
            .receipt
            .status,
        DeliveryStatus::Consumed
    );
    let inspected =
        super::super::SqliteJournal::inspect_communication(&path, "session".into(), id.clone())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(
        inspected.receipts[0].receipt.status,
        DeliveryStatus::Consumed
    );
    let (other, _) = CreatorJournal::open_sqlite(&path, "other-session".into(), false)
        .await
        .unwrap();
    assert!(other.load_communication(&id).await.unwrap().is_none());
    Journal::close(&other).await.unwrap();
    Journal::close(&reopened).await.unwrap();
}
