//! Core persistence stores runtime values; history and files belong to creators.
use microcrab_host_api::{WorkspaceFile, WorkspaceSnapshot};
use microcrab_session::{
    Limits, SessionState,
    context::Context,
    protocol::Usage,
    runtime::journal::{Journal, MemoryJournal, Recover, RoundContinuation},
};
use serde_json::json;

fn recover(epoch: u64, label: &str) -> Recover {
    let limits = Limits::default();
    let mut context = Context::new("system".into(), vec![], limits.max_context_bytes).unwrap();
    context.admit_user(label, &json!({})).unwrap();
    let mut snapshot = context.snapshot().unwrap();
    snapshot.epoch = epoch;
    let mut state = SessionState::new(limits);
    state.epoch_id = epoch;
    Recover {
        state,
        context: snapshot,
        continuation: None,
        workspace: None,
        cache_report: None,
        agent: None,
        prompt_config: None,
    }
}

fn assert_recover(actual: &Recover, expected: &Recover) {
    assert_eq!(
        serde_json::to_value(actual).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
}

#[tokio::test]
async fn memory_clones_share_latest_and_return_independent_snapshots() {
    let journal = MemoryJournal::new();
    assert!(journal.latest().is_none());
    let clone = journal.clone();
    let mut expected = recover(0, "settled round");
    expected.state.turn_id = Some(9);
    expected.state.round_id = Some(1);
    expected.continuation = Some(RoundContinuation {
        next_round: 2,
        unattended_rounds: 0,
        usage: Usage::default(),
        request_prepared: false,
        state_observation_pending: false,
    });
    expected.workspace = Some(WorkspaceSnapshot {
        files: vec![WorkspaceFile {
            path: "note.txt".into(),
            data: b"captured".to_vec(),
        }],
    });
    journal.commit(expected.clone()).await.unwrap();
    let mut snapshot = clone.latest().unwrap();
    assert_recover(&snapshot, &expected);
    snapshot.workspace.as_mut().unwrap().files[0].data.clear();
    assert_recover(&journal.latest().unwrap(), &expected);
    let replacement = recover(1, "selected by creator");
    clone.recovered(replacement.clone()).await.unwrap();
    assert_recover(&journal.latest().unwrap(), &replacement);
}

#[tokio::test]
async fn invalid_values_do_not_replace_the_latest_recovery() {
    let journal = MemoryJournal::default();
    let expected = recover(0, "valid");
    journal.commit(expected.clone()).await.unwrap();
    let mut invalid = recover(1, "invalid epoch");
    invalid.state.epoch_id = 0;
    assert!(journal.commit(invalid).await.is_err());
    let mut invalid = expected.clone();
    invalid.context.epoch = 1;
    assert!(journal.recovered(invalid).await.is_err());
    assert_recover(&journal.latest().unwrap(), &expected);
}

#[test]
fn recover_validates_context_continuation_and_rejects_creator_metadata() {
    let expected = recover(0, "value");
    expected.validate().unwrap();
    let encoded = serde_json::to_value(&expected).unwrap();
    assert!(encoded.get("session_id").is_none());
    assert!(encoded["state"].get("session_id").is_none());
    assert!(encoded["context"].get("session_id").is_none());
    for field in ["session_id", "cursor", "history", "files", "high_water"] {
        let mut invalid = encoded.clone();
        invalid[field] = json!("creator metadata");
        assert!(serde_json::from_value::<Recover>(invalid).is_err());
    }
    let mut invalid = expected.clone();
    invalid.continuation = Some(RoundContinuation {
        next_round: 2,
        unattended_rounds: 0,
        usage: Usage::default(),
        request_prepared: false,
        state_observation_pending: false,
    });
    assert!(invalid.validate().is_err());
    invalid.state.turn_id = Some(1);
    invalid.continuation.as_mut().unwrap().next_round = 0;
    assert!(invalid.validate().is_err());
    let mut invalid = expected;
    invalid.context.used_call_ids.insert("orphan-call".into());
    assert!(invalid.validate().is_err());
}

#[test]
fn state_pending_continuation_preserves_legacy_absence_and_rejects_prepared_overlap() {
    let legacy = json!({"next_round":2,"unattended_rounds":1,"usage":{}});
    let decoded: RoundContinuation = serde_json::from_value(legacy.clone()).unwrap();
    assert!(!decoded.state_observation_pending);
    let encoded = serde_json::to_value(&decoded).unwrap();
    assert!(encoded.get("state_observation_pending").is_none());
    assert!(encoded.get("request_prepared").is_none());
    let mut checkpoint = recover(0, "admitted boundary");
    checkpoint.state.turn_id = Some(1);
    checkpoint.continuation = Some(RoundContinuation {
        next_round: 2,
        unattended_rounds: 1,
        usage: Usage::default(),
        request_prepared: false,
        state_observation_pending: true,
    });
    checkpoint.validate().unwrap();
    let bytes = serde_json::to_value(&checkpoint).unwrap();
    assert_eq!(bytes["continuation"]["state_observation_pending"], true);
    let restored: Recover = serde_json::from_value(bytes.clone()).unwrap();
    assert_eq!(serde_json::to_value(restored).unwrap(), bytes);
    checkpoint.continuation.as_mut().unwrap().request_prepared = true;
    checkpoint.state.round_id = Some(2);
    let error = checkpoint.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("both a prepared request and pending State observation")
    );
}
