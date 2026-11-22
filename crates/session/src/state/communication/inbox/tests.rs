use super::*;
fn queued() -> DurableInbox {
    let carrier = AcceptedCarrier {
        operation_id: "operation".into(),
        ordinal: 1,
        body: "accepted".into(),
        source: UserSource::External {
            identity: "speaker".into(),
        },
        correspondent: None,
        speaker: Some(ExternalSpeaker {
            identity: "speaker".into(),
        }),
    };
    DurableInbox {
        revision: 1,
        next_ordinal: 1,
        pending: vec![carrier.clone()],
        receipts: vec![OperationReceipt {
            receipt: QueuedReceipt {
                operation_id: "operation".into(),
                ordinal: 1,
                status: DeliveryStatus::Queued,
            },
            digest: DurableInbox::digest(&carrier).unwrap(),
            dialogue: None,
        }],
    }
}
#[test]
fn legacy_dialogue_absence_preserves_exact_bytes_and_digest() {
    let bytes = concat!(
        r#"{"revision":1,"next_ordinal":1,"pending":[{"operation_id":"operation","ordinal":1,"body":"accepted","source":{"external":{"identity":"speaker"}},"speaker":{"identity":"speaker"}}],"receipts":[{"receipt":{"operation_id":"operation","ordinal":1,"status":"queued"},"digest":"#,
        "\"87a88ec893341c98ccc947971e6a5d539e91ca79f1b9958da3688faf1813e047\"}]}"
    );
    let restored: DurableInbox = serde_json::from_str(bytes).unwrap();
    restored.validate().unwrap();
    assert_eq!(restored, queued());
    assert_eq!(serde_json::to_string(&restored).unwrap(), bytes);
    assert!(restored.receipt("operation").is_some());
    assert!(
        restored
            .authenticated_external_receipt(
                "operation",
                "speaker",
                &ExternalCredentialIncarnation::new([7; 32])
            )
            .unwrap()
            .is_none()
    );
}

#[test]
fn dialogue_alias_collision_checks_consumed_receipts_and_never_guesses() {
    let mut inbox = queued();
    let mut carrier = inbox.pending.pop().unwrap();
    let proof = ExternalCredentialIncarnation::new([7; 32]);
    let first = uuid::Uuid::parse_str("0112abcd-0000-4000-8000-000000000000").unwrap();
    let collision = uuid::Uuid::parse_str("0112abcd-0000-4000-8000-000000000001").unwrap();
    let fresh = uuid::Uuid::parse_str("0212abcd-0000-4000-8000-000000000002").unwrap();
    inbox.receipts[0].dialogue =
        Some(DialogueAdmission::allocate(&inbox, &carrier, &proof, || first).unwrap());
    inbox.receipts[0].receipt.status = DeliveryStatus::Consumed;
    carrier.operation_id = "second".into();
    carrier.ordinal = 2;
    let mut ids = [first, collision, fresh].into_iter();
    let next =
        DialogueAdmission::allocate(&inbox, &carrier, &proof, || ids.next().unwrap()).unwrap();
    assert_eq!(next.canonical_id, fresh);
    assert_eq!(next.alias, "0212abcd");
    assert!(DialogueAdmission::allocate(&inbox, &carrier, &proof, || collision).is_err());
    assert!(DialogueAdmission::allocate(&inbox, &carrier, &proof, || first).is_err());
    carrier.operation_id = fresh.to_string();
    assert!(DialogueAdmission::allocate(&inbox, &carrier, &proof, || fresh).is_err());
}

#[test]
fn dialogue_reply_key_matches_only_exact_admission_and_credential() {
    use super::super::{
        AgentId, reply::DialogueReplyBinding, turn::AdmittedDialogueTurn,
    };
    let mut inbox = queued();
    let proof = ExternalCredentialIncarnation::new([7; 32]);
    let id = uuid::Uuid::parse_str("0112abcd-0000-4000-8000-000000000000").unwrap();
    assert!(
        inbox
            .authenticated_external_dialogue("0112abcd", "speaker", &proof)
            .unwrap()
            .is_none()
    );
    inbox.receipts[0].dialogue =
        Some(DialogueAdmission::allocate(&inbox, &inbox.pending[0], &proof, || id).unwrap());
    let (_, key) = inbox
        .authenticated_external_dialogue("0112abcd", "speaker", &proof)
        .unwrap()
        .unwrap();
    assert!(
        inbox
            .authenticated_external_dialogue("0112abcd", "other", &proof)
            .unwrap()
            .is_none()
    );
    assert!(
        inbox
            .authenticated_external_dialogue(
                "0112abcd",
                "speaker",
                &ExternalCredentialIncarnation::new([8; 32])
            )
            .unwrap()
            .is_none()
    );
    let agent = AgentId::try_from("agt_0123456789abcdef".to_owned()).unwrap();
    let turn = AdmittedDialogueTurn::fresh(1).unwrap();
    assert!(
        DialogueReplyBinding::from_admission(agent.clone(), &inbox.receipts[0], turn.clone())
            .is_err()
    );
    inbox.receipts[0].receipt.status = DeliveryStatus::Consumed;
    inbox.pending.clear();
    let binding =
        DialogueReplyBinding::from_admission(agent.clone(), &inbox.receipts[0], turn.clone())
            .unwrap()
            .unwrap();
    assert!(binding.admission() == &key);
    inbox.receipts[0].receipt.operation_id = "different-operation".into();
    let different = DialogueReplyBinding::from_admission(agent, &inbox.receipts[0], turn)
        .unwrap()
        .unwrap();
    assert!(different.admission() != &key);
}

#[test]
fn queued_receipts_require_retained_bodies_and_unique_arrival_ordinals() {
    let valid = queued();
    valid.validate().unwrap();
    let mut missing = valid.clone();
    missing.pending.clear();
    assert!(missing.validate().is_err());
    let mut duplicate = valid;
    let mut receipt = duplicate.receipts[0].clone();
    receipt.receipt.operation_id = "another".into();
    receipt.receipt.status = DeliveryStatus::Consumed;
    duplicate.receipts.push(receipt);
    assert!(duplicate.validate().is_err());
}
#[test]
fn new_receipts_cannot_reuse_gaps_before_the_durable_arrival_watermark() {
    let mut original = queued();
    // Other inbox lanes can leave gaps between accepted communication ordinals.
    original.next_ordinal = 3;
    original.validate().unwrap();
    for status in [DeliveryStatus::Queued, DeliveryStatus::Consumed] {
        for ordinal in [2, 3, 4] {
            let mut next = original.clone();
            next.revision += 1;
            next.next_ordinal = original.next_ordinal.max(ordinal);
            let mut carrier = original.pending[0].clone();
            carrier.operation_id = "later-operation".into();
            carrier.ordinal = ordinal;
            next.receipts.push(OperationReceipt {
                receipt: QueuedReceipt {
                    operation_id: carrier.operation_id.clone(),
                    ordinal,
                    status: status.clone(),
                },
                digest: DurableInbox::digest(&carrier).unwrap(),
                dialogue: None,
            });
            if status == DeliveryStatus::Queued {
                next.pending.push(carrier);
            }
            next.validate().unwrap();
            assert_eq!(
                next.check_successor(Some(&original)).is_ok(),
                ordinal > original.next_ordinal,
                "new {status:?} receipt at ordinal {ordinal} must follow the prior watermark"
            );
        }
    }
}
#[tokio::test]
async fn memory_journal_and_storage_share_irreversible_consumption_cas() {
    use crate::runtime::journal::{Journal, MemoryJournal};
    let journal = MemoryJournal::new();
    let id = super::super::AgentId::try_from("agt_0123456789abcdef".to_owned()).unwrap();
    let original = queued();
    journal
        .commit_communication(&id, original.clone())
        .await
        .unwrap();
    journal
        .commit_communication(&id, original.clone())
        .await
        .unwrap();
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
