//! Secret State regressions: live privacy, governance and staged publication.
use super::*;
use crate::{Limits, VariablesChange};
use microcrab_protocol::{
    ScopedVariableSet, VariablePersistence, VariableRole, VariableVisibility,
};
use serde_json::{Map, Value, json};
use std::sync::atomic::AtomicBool;

fn bound() -> (State, Arc<VariableEndpoint>) {
    State::new(Limits::default()).bind_writer(
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    )
}
fn values(key: &str, value: &str) -> BTreeMap<String, String> {
    BTreeMap::from([(key.into(), value.into())])
}
fn secret_change(value: Value) -> VariablesChange {
    VariablesChange {
        scoped_set: vec![ScopedVariableSet {
            name: "token".into(),
            value,
            visibility: VariableVisibility::Secret,
            persistence: VariablePersistence::Ephemeral,
            editors: Some(vec![VariableRole::Client]),
        }],
        ..Default::default()
    }
}

#[tokio::test]
async fn prepared_secret_is_invisible_until_atomic_commit_and_drop_aborts() {
    let (state, _endpoint) = bound();
    let before = state.inspection().unwrap().witness;
    let prepared = state
        .prepare_secret_write(values("token", "PRIVATE_FIRST"))
        .await
        .unwrap();
    assert!(state.extract("token").is_none());
    assert!(!format!("{prepared:?}").contains("PRIVATE_FIRST"));
    drop(prepared);
    assert_eq!(state.inspection().unwrap().witness, before);
    let committed = state
        .prepare_secret_write(values("token", "PRIVATE_SECOND"))
        .await
        .unwrap()
        .commit()
        .unwrap();
    assert_eq!(committed.before, before);
    assert!(committed.changed);
    assert_eq!(state.extract("token"), Some(json!("{{secret:token}}")));
    assert!(
        !serde_json::to_string(&committed)
            .unwrap()
            .contains("PRIVATE")
    );
}

#[tokio::test]
async fn all_public_views_and_snapshot_getters_mask_secrets_but_private_snapshot_is_stable() {
    let (state, _endpoint) = bound();
    state
        .prepare_secret_write(values("token", "PRIVATE_INITIAL"))
        .await
        .unwrap()
        .commit()
        .unwrap();
    let captured = state.secret_snapshot();
    let inspection = state.inspection().unwrap();
    assert_eq!(inspection.values["token"], "{{secret:token}}");
    assert_eq!(inspection.metadata["token"]["visibility"], "secret");
    assert!(!inspection.descriptors["token"].editable);
    assert!(!inspection.descriptors["token"].owner_editable);
    assert!(inspection.descriptors["token"].deletable);
    assert_eq!(state.view().unwrap()["token"], "{{secret:token}}");
    assert_eq!(state.view_all().unwrap()["token"], "{{secret:token}}");
    assert_eq!(state.variables()["token"], "{{secret:token}}");
    assert_eq!(
        state.wasm_extract("token").unwrap(),
        Some(json!("{{secret:token}}"))
    );
    let snapshot = state.snapshot();
    assert_eq!(
        snapshot.variables.get("token"),
        Some(&json!("{{secret:token}}"))
    );
    assert_eq!(
        snapshot.variables.get_all("token"),
        Some(&json!("{{secret:token}}"))
    );
    assert_eq!(snapshot.variables.all_values()["token"], "{{secret:token}}");
    assert!(!format!("{snapshot:?} {captured:?}").contains("PRIVATE_INITIAL"));
    state
        .prepare_secret_write(values("token", "PRIVATE_REPLACED"))
        .await
        .unwrap()
        .commit()
        .unwrap();
    assert_eq!(captured.lookup("token"), Some("PRIVATE_INITIAL"));
    assert_eq!(
        captured.entries().collect::<Vec<_>>(),
        vec![("token", "PRIVATE_INITIAL")]
    );
    assert_eq!(
        state.secret_snapshot().lookup("token"),
        Some("PRIVATE_REPLACED")
    );
    assert_eq!(state.view().unwrap()["token"], "{{secret:token}}");
}

#[tokio::test]
async fn checkpoints_replacement_and_shutdown_drop_values_without_changing_visible_history() {
    let (state, endpoint) = bound();
    state.inject("visible", json!("kept")).await.unwrap();
    state
        .change(VariablesChange {
            scoped_set: vec![ScopedVariableSet {
                name: "hidden".into(),
                value: json!("hidden_kept"),
                visibility: VariableVisibility::Invisible,
                persistence: VariablePersistence::Persisted,
                editors: Some(vec![VariableRole::Client]),
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    let before = serde_json::to_value(state.snapshot()).unwrap();
    state
        .prepare_secret_write(values("token", "PRIVATE_DURABLE_SCAN"))
        .await
        .unwrap()
        .commit()
        .unwrap();
    let encoded = serde_json::to_value(state.snapshot()).unwrap();
    assert_eq!(encoded, before);
    assert!(
        !serde_json::to_string(&encoded)
            .unwrap()
            .contains("PRIVATE_DURABLE_SCAN")
    );
    assert!(
        state
            .recovery_snapshot()
            .variables
            .get_all("token")
            .is_none()
    );
    let snapshot = state.snapshot();
    state.replace(snapshot);
    assert!(state.extract("token").is_none());
    assert_eq!(state.view_all().unwrap()["hidden"], "hidden_kept");
    state
        .prepare_secret_write(values("token", "PRIVATE_CLOSE"))
        .await
        .unwrap()
        .commit()
        .unwrap();
    endpoint.shutdown();
    assert!(state.snapshot().variables.get_all("token").is_none());
}

#[tokio::test]
async fn sideband_collisions_and_invalid_batches_have_no_partial_writes() {
    let (state, _endpoint) = bound();
    state.inject("existing", json!(1)).await.unwrap();
    let original = state.inspection().unwrap().witness;
    for invalid in [
        BTreeMap::new(),
        values("1bad", "value"),
        values("token", ""),
        values("token", &"a".repeat(8193)),
        values("time", "value"),
        BTreeMap::from([
            ("token".into(), "PRIVATE".into()),
            ("existing".into(), "value".into()),
        ]),
        (0..9)
            .map(|index| (format!("key{index}"), "value".into()))
            .collect(),
    ] {
        assert!(state.prepare_secret_write(invalid).await.is_err());
        assert_eq!(state.inspection().unwrap().witness, original);
        assert!(state.extract("token").is_none());
    }
    let accepted = state
        .prepare_secret_write(values("token", &"é".repeat(4096)))
        .await
        .unwrap()
        .commit()
        .unwrap();
    assert!(accepted.changed);
}

#[tokio::test]
async fn generic_creation_is_rejected_and_existing_values_remain_client_governed() {
    let (state, _endpoint) = bound();
    assert!(state.change(secret_change(json!("PRIVATE"))).await.is_err());
    assert!(
        state
            .for_agent()
            .prepare_secret_write(values("token", "PRIVATE"))
            .await
            .is_err()
    );
    state
        .prepare_secret_write(values("token", "PRIVATE"))
        .await
        .unwrap()
        .commit()
        .unwrap();
    let before = state.inspection().unwrap().witness;
    assert!(state.inject("token", json!(7)).await.is_err());
    assert!(
        state
            .for_agent()
            .inject("token", json!("attacker"))
            .await
            .is_err()
    );
    let mut downgrade = secret_change(json!("PRIVATE"));
    downgrade.scoped_set[0].visibility = VariableVisibility::Visible;
    downgrade.scoped_set[0].persistence = VariablePersistence::Persisted;
    assert!(state.change(downgrade).await.is_err());
    assert_eq!(state.inspection().unwrap().witness, before);
    state
        .inject("token", json!("PRIVATE_CHANGED"))
        .await
        .unwrap();
    assert_eq!(
        state.secret_snapshot().lookup("token"),
        Some("PRIVATE_CHANGED")
    );
    let receipt = state.receipt_change(&VariablesChange {
        set: Map::from_iter([("token".into(), json!("PRIVATE_CHANGED"))]),
        ..Default::default()
    });
    assert!(receipt.set.is_empty());
    state.remove("token").await.unwrap();
    assert!(state.secret_snapshot().lookup("token").is_none());
}

#[tokio::test]
async fn held_candidate_rejects_shutdown_and_concurrent_hook_changes_without_clobbering() {
    let (state, endpoint) = bound();
    let prepared = state
        .prepare_secret_write(values("token", "PRIVATE"))
        .await
        .unwrap();
    state
        .apply_change(VariablesChange {
            set: Map::from_iter([("hook".into(), json!(1))]),
            ..Default::default()
        })
        .unwrap();
    assert!(matches!(prepared.commit(), Err(Error::VariableConflict)));
    assert_eq!(state.extract("hook"), Some(json!(1)));
    assert!(state.extract("token").is_none());
    let prepared = state
        .prepare_secret_write(values("token", "PRIVATE"))
        .await
        .unwrap();
    endpoint.shutdown();
    assert!(matches!(prepared.commit(), Err(Error::Closed)));
    assert!(state.extract("token").is_none());
}
