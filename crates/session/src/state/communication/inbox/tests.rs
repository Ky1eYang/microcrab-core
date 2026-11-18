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
        }],
    }
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
