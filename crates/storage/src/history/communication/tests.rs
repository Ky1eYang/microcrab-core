use super::*;
use microcrab_session::state::communication::{
    AgentId,
    inbox::{AcceptedCarrier, DeliveryStatus, DurableInbox, ExternalCredentialIncarnation},
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
    serde_json::from_value(serde_json::json!({
        "revision": 1,
        "next_ordinal": 1,
        "pending": [carrier],
        "receipts": [{
            "receipt": {"operation_id":"accepted", "ordinal":1, "status":"queued"},
            "digest": digest
        }]
    }))
    .unwrap()
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
const DIALOGUE_ID: &str = "0123abcd-0000-4000-8000-000000000001";
const DIALOGUE_ALIAS: &str = "0123abcd";

fn dialogue_inbox() -> DurableInbox {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"microcrab/external-dialogue-incarnation/v1\0");
    digest.update(uuid::Uuid::parse_str(DIALOGUE_ID).unwrap().as_bytes());
    digest.update([7; 32]);
    let mut value = serde_json::to_value(inbox()).unwrap();
    value["receipts"][0]["dialogue"] = serde_json::json!({
        "canonical_id": DIALOGUE_ID,
        "alias": DIALOGUE_ALIAS,
        "identity": "speaker",
        "incarnation_witness": format!("{:x}", digest.finalize()),
    });
    let result: DurableInbox = serde_json::from_value(value).unwrap();
    result.validate().unwrap();
    result
}

fn assert_dialogue(inbox: &DurableInbox, status: DeliveryStatus) {
    assert!(inbox.receipt("accepted").is_none());
    let proof = ExternalCredentialIncarnation::new([7; 32]);
    let public = inbox
        .authenticated_external_receipt("accepted", "speaker", &proof)
        .unwrap()
        .unwrap();
    assert_eq!(public.dialogue_id, DIALOGUE_ALIAS);
    assert_eq!(public.receipt.status, status);
    assert!(
        inbox
            .authenticated_external_receipt("accepted", "other", &proof)
            .unwrap()
            .is_none()
    );
    assert!(
        inbox
            .authenticated_external_receipt(
                "accepted",
                "speaker",
                &ExternalCredentialIncarnation::new([8; 32])
            )
            .unwrap()
            .is_none()
    );
}

#[test]
fn dialogue_inspection_rejects_conflicting_selected_admission() {
    let id = AgentId::try_from("agt_0123456789abcdef".to_owned()).unwrap();
    let stored = StoredInbox {
        session_id: "session".into(),
        agent_id: id.clone(),
        inbox: dialogue_inbox(),
    };
    let mut consumed = stored.inbox.clone();
    consumed.pending.clear();
    consumed.receipts[0].receipt.status = DeliveryStatus::Consumed;
    let selected = selected_consumption(consumed.clone(), &id);
    let reconciled = reconcile_inspected(stored.clone(), "session", &id, Some(&selected)).unwrap();
    assert!(reconciled.pending.is_empty());
    assert_dialogue(&reconciled, DeliveryStatus::Consumed);
    let mut legacy = inbox();
    legacy.pending.clear();
    legacy.receipts[0].receipt.status = DeliveryStatus::Consumed;
    let selected = selected_consumption(legacy, &id);
    assert!(reconcile_inspected(stored, "session", &id, Some(&selected)).is_err());
}

#[test]
fn dialogue_records_fit_existing_sidecar_budget_at_retained_limits() {
    use microcrab_session::state::communication::inbox::{MAX_CARRIERS, MAX_RECEIPTS};
    use sha2::{Digest, Sha256};
    let id = AgentId::try_from("agt_0123456789abcdef".to_owned()).unwrap();
    let identity = "a".repeat(64);
    let body = format!("a{}", "\0".repeat(1023));
    let source = serde_json::json!({"external":{"identity":identity}});
    let speaker = serde_json::json!({"identity":identity});
    let digest = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&(&source, &body, None::<()>, &speaker)).unwrap())
    );
    let mut pending = Vec::new();
    let mut receipts = Vec::new();
    for ordinal in 1..=MAX_RECEIPTS {
        let operation = format!("{}{ordinal:04x}", "\"".repeat(124));
        let queued = ordinal > MAX_RECEIPTS - MAX_CARRIERS;
        if queued {
            pending.push(serde_json::json!({
                "operation_id": operation, "ordinal": ordinal, "body": body,
                "source": source, "speaker": speaker,
            }));
        }
        receipts.push(serde_json::json!({
            "receipt": {"operation_id":operation, "ordinal": ordinal,
                "status": if queued {"queued"} else {"consumed"}},
            "digest": digest,
            "dialogue": {
                "canonical_id": format!("{ordinal:08x}-0000-4000-8000-000000000000"),
                "alias": format!("{ordinal:08x}"),
                "identity": identity,
                "incarnation_witness": "a".repeat(64),
            }
        }));
    }
    let stored = StoredInbox {
        session_id: "session".into(),
        agent_id: id.clone(),
        inbox: serde_json::from_value(serde_json::json!({
            "revision": MAX_RECEIPTS, "next_ordinal": MAX_RECEIPTS,
            "pending": pending, "receipts": receipts,
        }))
        .unwrap(),
    };
    stored.validate("session", &id).unwrap();
}

async fn exercise(journal: &CreatorJournal) {
    let id = AgentId::try_from("agt_0123456789abcdef".to_owned()).unwrap();
    let wrong = AgentId::try_from("agt_1123456789abcdef".to_owned()).unwrap();
    let original = dialogue_inbox();
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
    let mut changed = serde_json::to_value(&original).unwrap();
    changed["revision"] = 2.into();
    changed["receipts"][0]["dialogue"]["incarnation_witness"] = "0".repeat(64).into();
    assert!(
        journal
            .commit_communication(&id, serde_json::from_value(changed).unwrap())
            .await
            .is_err()
    );
    let mut legacy = inbox();
    legacy.revision = 2;
    assert!(journal.commit_communication(&id, legacy).await.is_err());
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
async fn file_dialogue_sidecar_restart_exact_identity_revision_and_no_replay() {
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
    assert_dialogue(&restored, DeliveryStatus::Consumed);
    assert!(restored.pending.is_empty());
    Journal::close(&reopened).await.unwrap();
}
#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_dialogue_sidecar_restart_exact_identity_revision_and_no_replay() {
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
    let restored = reopened.load_communication(&id).await.unwrap().unwrap();
    assert_dialogue(&restored, DeliveryStatus::Consumed);
    let inspected =
        super::super::SqliteJournal::inspect_communication(&path, "session".into(), id.clone())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(inspected, restored);
    let (other, _) = CreatorJournal::open_sqlite(&path, "other-session".into(), false)
        .await
        .unwrap();
    assert!(other.load_communication(&id).await.unwrap().is_none());
    Journal::close(&other).await.unwrap();
    Journal::close(&reopened).await.unwrap();
}
