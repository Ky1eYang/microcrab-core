//! Real journal hash validation must tolerate only quarantined legacy calibration.
use super::*;
use microcrab_session::context::{Content, TokenEstimate, TokenEstimator};
use sha2::{Digest, Sha256};

struct Capacity;
impl TokenEstimator for Capacity {
    fn estimate(&self, _: &Content) -> Result<Option<TokenEstimate>, Error> {
        Ok(Some(TokenEstimate {
            route: "fixture:model".into(),
            provenance: "fixture_capacity".into(),
            input_tokens: 1,
            context_window_tokens: 1_000,
        }))
    }
}

fn measured_recover(version: u8) -> Recover {
    let mut recover = super::recover(true);
    // Independently author an insertion-ordered frozen schema to reproduce the
    // historical hash bug even when current admission canonicalizes new tools.
    recover.context.tools[0].parameters = serde_json::from_str(
        r#"{"type":"object","properties":{"z":{"enum":[{"z":2,"a":1}]},"a":{"type":"string"}}}"#,
    )
    .unwrap();
    let prefix_items = recover.context.items.len();
    let material = (
        recover.context.epoch,
        recover.context.baseline_generation,
        &recover.context.system,
        &recover.context.tools,
        &recover.context.items,
    );
    let bytes = if version == 0 {
        serde_json::to_vec(&material).unwrap()
    } else {
        canonical_bytes(&material).unwrap()
    };
    let fingerprint = format!("{:x}", Sha256::digest(bytes));
    let mut context = Context::from_snapshot(recover.context).unwrap();
    context
        .append_response(
            &serde_json::from_value(json!({
                "content":"Measured final answer", "tool_calls":[], "usage":{}
            }))
            .unwrap(),
        )
        .unwrap();
    recover.context = context.snapshot().unwrap();
    let mut evidence = json!({
        "route":"fixture:model", "epoch":0, "baseline_generation":0,
        "prefix_items":prefix_items, "fingerprint":fingerprint, "input_tokens":900
    });
    if version != 0 {
        evidence["fingerprint_version"] = json!(version);
    }
    recover.context.usage_evidence = Some(serde_json::from_value(evidence).unwrap());
    recover.validate().unwrap();
    recover
}

fn assert_measurement(restored: &Recover, version: u8) {
    let context = Context::from_snapshot(restored.context.clone()).unwrap();
    let estimate = context.estimate_tokens(&Capacity, 64).unwrap();
    if version == 0 {
        assert!(estimate.is_none());
        assert!(context.snapshot().unwrap().usage_evidence.is_none());
    } else {
        assert_eq!(estimate.unwrap().input_tokens, 900);
    }
    let mut expected = serde_json::to_value(&restored.context).unwrap();
    let mut actual = serde_json::to_value(context.snapshot().unwrap()).unwrap();
    expected.as_object_mut().unwrap().remove("usage_evidence");
    actual.as_object_mut().unwrap().remove("usage_evidence");
    assert_eq!(expected, actual);
}

async fn durable_roundtrip(journal: CreatorJournal, original: Recover) -> JournalCursor {
    journal.commit(original).await.unwrap();
    let cursor = journal.cursor().await.unwrap();
    journal.close().await.unwrap();
    cursor
}

async fn assert_reopened(
    journal: CreatorJournal,
    restored: Recover,
    cursor: JournalCursor,
    expected: &Value,
    version: u8,
) {
    assert_eq!(journal.cursor().await.unwrap(), cursor);
    restored.validate().unwrap();
    assert_eq!(serde_json::to_value(&restored).unwrap(), *expected);
    // Hash verification happens on every backend load. Reconstruct the exact new
    // internal envelope: usage-version defaults must not alter its frozen bytes.
    let checkpoint = CreatorEnvelope {
        session_id: SESSION_ID.into(),
        recover: restored.clone(),
        request_head: None,
        model_selection: None,
        route_witness: None,
        navigation_boundary: Some(microcrab_session::NavigationBoundary::Internal),
    };
    let (node, _) = crate::history::dedup::split_v5(None, &checkpoint).unwrap();
    node.validate_header(&cursor.checkpoint_id).unwrap();
    assert_measurement(&restored, version);
    assert_eq!(serde_json::to_value(&restored).unwrap(), *expected);
    journal.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn usage_evidence_file_close_reopen_keeps_canonical_and_quarantines_legacy() {
    for version in [0, 1] {
        let directory =
            std::env::temp_dir().join(format!("microcrab-usage-file-{}", uuid::Uuid::new_v4()));
        let original = measured_recover(version);
        let expected = serde_json::to_value(&original).unwrap();
        let (journal, _) =
            CreatorJournal::open(directory.to_str().unwrap(), SESSION_ID.into(), true, false)
                .await
                .unwrap();
        let cursor = durable_roundtrip(journal, original).await;
        let (journal, restored) =
            CreatorJournal::open(directory.to_str().unwrap(), SESSION_ID.into(), true, true)
                .await
                .unwrap();
        assert_reopened(journal, restored.unwrap(), cursor, &expected, version).await;
    }
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn usage_evidence_sqlite_close_reopen_keeps_canonical_and_quarantines_legacy() {
    for version in [0, 1] {
        let database =
            std::env::temp_dir().join(format!("microcrab-usage-{}.sqlite", uuid::Uuid::new_v4()));
        let original = measured_recover(version);
        let expected = serde_json::to_value(&original).unwrap();
        let (journal, _) = CreatorJournal::open_sqlite(&database, SESSION_ID.into(), false)
            .await
            .unwrap();
        let cursor = durable_roundtrip(journal, original).await;
        let (journal, restored) = CreatorJournal::open_sqlite(&database, SESSION_ID.into(), true)
            .await
            .unwrap();
        assert_reopened(journal, restored.unwrap(), cursor, &expected, version).await;
    }
}
