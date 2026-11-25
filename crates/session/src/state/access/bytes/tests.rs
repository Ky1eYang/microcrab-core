use super::*;
use crate::{Limits, SessionKind};
use serde_json::json;
use std::sync::atomic::AtomicBool;

fn root(limits: Limits) -> (State, Arc<super::super::VariableEndpoint>) {
    let (state, endpoint) = State::new(limits).bind_writer(
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    state.update(|snapshot| snapshot.session_kind = Some(SessionKind::Interactive));
    (state, endpoint)
}

// Minimal admitted image header. These tests don't certify a raster decoder.
pub(crate) const PNG: &[u8] =
    b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR\0\0\0\x01\0\0\0\x01\x08\x02\0\0\0\0\0\0\0";

fn witness(state: &State) -> VariableWitness {
    state.inspection().unwrap().witness
}

fn ticket(state: &State, name: &str) -> PreparedBytesWrite {
    let mut pending = state
        .prepare_bytes_write(name, "image/png", PNG.len() as u64, witness(state))
        .unwrap();
    for chunk in PNG.chunks(8) {
        pending.append(chunk).unwrap();
    }
    pending
}

#[tokio::test]
async fn guest_image_commit_is_private_create_only_and_cancellable() {
    let (state, _owner) = root(Limits::default());
    let before = serde_json::to_value(state.recovery_snapshot()).unwrap();
    let destination = state.prepare_redirect_destination("capture").unwrap();
    let cancelled = tokio_util::sync::CancellationToken::new();
    cancelled.cancel();
    assert!(matches!(state.commit_private_image(destination.clone(), "image/png", PNG.to_vec(), &cancelled).await, Err(Error::Cancelled)));
    assert!(state.image_bytes_selector_for_name("capture").is_err());
    let receipt = state.commit_private_image(destination, "image/png", PNG.to_vec(), &Default::default()).await.unwrap();
    assert_eq!(receipt.metadata.name, "capture");
    let selector = state.image_bytes_selector_for_name("capture").unwrap();
    state.with_image_bytes(&selector, |mime, bytes| {
        assert_eq!(mime, "image/png");
        assert_eq!(bytes, PNG);
        Ok(())
    }).unwrap();
    assert!(state.prepare_redirect_destination("capture").is_err());
    assert!(state.wasm_extract("capture").unwrap().is_none());
    assert_eq!(serde_json::to_value(state.recovery_snapshot()).unwrap(), before);
}

#[tokio::test]
async fn typed_receipts_are_the_only_projection_and_json_writes_cannot_collide() {
    let (state, _owner) = root(Limits::default());
    let original = serde_json::to_value(state.recovery_snapshot()).unwrap();
    assert!(
        serde_json::to_value(state.inspection().unwrap())
            .unwrap()
            .get("bytes")
            .is_none()
    );
    let pending = ticket(&state, "raster");
    let pointer = pending.data.as_ptr();
    assert_eq!(format!("{pending:?}"), "PreparedBytesWrite { .. }");
    let receipt = state.commit_bytes(pending).await.unwrap();
    let metadata = &receipt.metadata;
    assert!(receipt.commit.changed);
    assert_eq!(receipt.commit.after.variables_revision, metadata.revision);
    let observed = state.inspection().unwrap();
    assert_eq!(&observed.bytes["raster"], metadata);
    assert!(observed.values.get("raster").is_none());
    assert!(observed.metadata.get("raster").is_none());
    assert_eq!(
        serde_json::to_value(metadata).unwrap(),
        json!({
            "name":"raster", "kind":"bytes", "media_type":"image/png", "byte_length":33,
            "lifecycle":"ephemeral", "availability":"available", "revision":"1", "selector":metadata.selector
        })
    );
    assert!(state.for_agent().inspection().unwrap().bytes.is_empty());
    assert!(state.view().unwrap().get("raster").is_none());
    assert_eq!(state.extract("raster"), None);
    assert_eq!(state.wasm_extract("raster").unwrap(), None);
    assert_eq!(state.snapshot().variables.all_values(), json!({}));
    assert_eq!(
        serde_json::to_value(state.recovery_snapshot()).unwrap(),
        original
    );
    assert!(matches!(
        state.inject("raster", json!(0)).await,
        Err(Error::VariableForbidden(_))
    ));
    assert!(matches!(
        state.remove("raster").await,
        Err(Error::VariableForbidden(_))
    ));
    assert!(matches!(
        state
            .prepare_secret_write(BTreeMap::from([("raster".into(), "hidden".into())]))
            .await,
        Err(Error::VariableForbidden(_))
    ));
    state
        .with_image_bytes(&metadata.selector, |mime, data| {
            assert_eq!(mime, "image/png");
            assert_eq!(data, PNG);
            assert_eq!(data.as_ptr(), pointer);
            Ok(())
        })
        .unwrap();
}

#[tokio::test]
async fn witnesses_and_exact_selectors_do_not_retarget() {
    let (state, owner) = root(Limits::default());
    let (foreign, _foreign_owner) = root(Limits::default());
    let stale = ticket(&state, "raster");
    let first = state.commit_bytes(ticket(&state, "raster")).await.unwrap();
    assert!(matches!(
        state.commit_bytes(stale).await,
        Err(Error::VariableConflict)
    ));
    assert!(matches!(
        foreign.commit_bytes(ticket(&state, "raster")).await,
        Err(Error::VariableConflict)
    ));
    assert!(
        foreign
            .with_image_bytes(&first.metadata.selector, |_, _| Ok(()))
            .is_err()
    );
    let second = state.commit_bytes(ticket(&state, "raster")).await.unwrap();
    assert_ne!(first.metadata.selector, second.metadata.selector);
    assert!(
        state
            .with_image_bytes(&first.metadata.selector, |_, _| Ok(()))
            .is_err()
    );
    assert!(
        state
            .remove_bytes_checked("raster", first.commit.after)
            .await
            .is_err()
    );
    assert!(
        state
            .remove_bytes_checked("raster", second.commit.after)
            .await
            .unwrap()
            .changed
    );
    assert!(state.inspection().unwrap().bytes.is_empty());
    let third = state.commit_bytes(ticket(&state, "raster")).await.unwrap();
    let pending = ticket(&state, "raster");
    let held = owner.fence().await.unwrap();
    state.replace(state.recovery_snapshot());
    drop(held);
    assert!(state.inspection().unwrap().bytes.is_empty());
    assert!(matches!(
        state.commit_bytes(pending).await,
        Err(Error::VariableConflict)
    ));
    assert!(
        state
            .with_image_bytes(&third.metadata.selector, |_, _| Ok(()))
            .is_err()
    );
    state.commit_bytes(ticket(&state, "raster")).await.unwrap();
    owner.shutdown();
    assert!(state.inspection().unwrap().bytes.is_empty());
    assert!(matches!(
        state.prepare_bytes_write("late", "image/png", 33, witness(&state)),
        Err(Error::Closed)
    ));
}

#[tokio::test]
async fn name_binding_requires_a_live_root_and_returns_only_the_exact_current_selector() {
    let (state, owner) = root(Limits::default());
    let (other, _other_owner) = root(Limits::default());
    state.inject("json", json!(true)).await.unwrap();
    let first = state.commit_bytes(ticket(&state, "Preview")).await.unwrap();
    let exact = state.image_bytes_selector_for_name("Preview").unwrap();
    assert_eq!(exact, first.metadata.selector);
    for nonmatch in ["preview", "json", "missing", exact.as_str()] {
        assert!(state.image_bytes_selector_for_name(nonmatch).is_err());
    }
    assert!(other.image_bytes_selector_for_name("Preview").is_err());
    let second = state.commit_bytes(ticket(&state, "Preview")).await.unwrap();
    assert_ne!(
        exact,
        state.image_bytes_selector_for_name("Preview").unwrap()
    );
    assert!(state.with_image_bytes(&exact, |_, _| Ok(())).is_err());

    state.update(|snapshot| snapshot.session_kind = Some(SessionKind::Delegated));
    assert!(matches!(
        state.image_bytes_selector_for_name("Preview"),
        Err(Error::Denied(_))
    ));
    state.update(|snapshot| snapshot.session_kind = Some(SessionKind::Interactive));
    state
        .remove_bytes_checked("Preview", second.commit.after)
        .await
        .unwrap();
    assert!(state.image_bytes_selector_for_name("Preview").is_err());

    let third = state.commit_bytes(ticket(&state, "Preview")).await.unwrap();
    let fence = owner.fence().await.unwrap();
    state.replace(state.recovery_snapshot());
    drop(fence);
    assert!(state.image_bytes_selector_for_name("Preview").is_err());
    assert!(
        state
            .with_image_bytes(&third.metadata.selector, |_, _| Ok(()))
            .is_err()
    );

    owner.shutdown();
    assert!(matches!(
        state.image_bytes_selector_for_name("Preview"),
        Err(Error::Closed)
    ));
}

#[tokio::test]
async fn reservations_bound_abandoned_and_failed_writes_and_follow_the_last_lease() {
    let (state, _owner) = root(Limits {
        max_tool_image_bytes: MAX_IMAGE_BYTES,
        ..Limits::default()
    });
    let expected = witness(&state);
    let stages = (0..3)
        .map(|_| {
            state
                .prepare_bytes_write(
                    "large",
                    "image/png",
                    MAX_IMAGE_BYTES as u64,
                    expected.clone(),
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert!(
        matches!(state.prepare_bytes_write("large", "image/png", MAX_IMAGE_BYTES as u64, expected.clone()), Err(Error::Limit(limit)) if limit.kind == LimitKind::PrivateVariableBytes)
    );
    drop(stages);
    let stages = (0..MAX_PRIVATE_ENTRIES)
        .map(|_| {
            state
                .prepare_bytes_write("tiny", "image/png", 1, expected.clone())
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert!(
        matches!(state.prepare_bytes_write("tiny", "image/png", 1, expected.clone()), Err(Error::Limit(limit)) if limit.kind == LimitKind::PrivateVariableEntries)
    );
    drop(stages);
    let mut overflowed = ticket(&state, "bad");
    assert!(overflowed.append(&[0]).is_err());
    assert!(state.commit_bytes(overflowed).await.is_err());
    let mut invalid = state
        .prepare_bytes_write("bad", "image/jpeg", PNG.len() as u64, expected.clone())
        .unwrap();
    invalid.append(PNG).unwrap();
    assert!(state.commit_bytes(invalid).await.is_err());
    let incomplete = state
        .prepare_bytes_write("short", "image/png", PNG.len() as u64, expected)
        .unwrap();
    assert!(state.commit_bytes(incomplete).await.is_err());
    let budget = state.inner.lock().unwrap().bytes.budget.clone();
    assert_eq!(budget.0.lock().unwrap().bytes, 0);
    let version = state.commit_bytes(ticket(&state, "raster")).await.unwrap();
    // The guarded map is closed before invoking any reader. Simulate replacement
    // while a read holds the old allocation and verify it is charged until drop.
    state
        .with_image_bytes(&version.metadata.selector, |_, data| {
            state.inner.lock().unwrap().bytes.clear();
            assert_eq!(budget.0.lock().unwrap().bytes, data.len());
            Ok(())
        })
        .unwrap();
    assert_eq!(budget.0.lock().unwrap().bytes, 0);
    assert_eq!(budget.0.lock().unwrap().entries, 0);
}

#[tokio::test]
async fn dropping_a_commit_waiter_before_fence_ack_cannot_publish_or_replay() {
    let (state, owner) = root(Limits::default());
    let held = owner.fence().await.unwrap();
    let pending = ticket(&state, "raster");
    let commit_state = state.clone();
    let mut commit = Box::pin(commit_state.commit_bytes(pending));
    assert!(futures_util::poll!(&mut commit).is_pending());
    drop(commit);
    drop(held);
    let _drained = owner.fence().await.unwrap();
    assert!(state.inspection().unwrap().bytes.is_empty());
    let budget = state.inner.lock().unwrap().bytes.budget.clone();
    assert_eq!(budget.0.lock().unwrap().bytes, 0);
}

#[tokio::test]
async fn invalid_client_roles_and_reserved_names_never_admit_staging() {
    let (state, _owner) = root(Limits::default());
    state.inject("json", json!(true)).await.unwrap();
    let expected = witness(&state);
    assert!(
        state
            .prepare_bytes_write("binary", "application/octet-stream", 33, expected.clone())
            .is_err()
    );
    for name in ["json", "execution_capacity", "", "invalid name"] {
        assert!(
            state
                .prepare_bytes_write(name, "image/png", 33, expected.clone())
                .is_err()
        );
    }
    assert!(
        state
            .for_agent()
            .prepare_bytes_write("image", "image/png", 33, expected.clone())
            .is_err()
    );
    state.update(|snapshot| snapshot.session_kind = Some(SessionKind::Delegated));
    assert!(
        state
            .prepare_bytes_write("image", "image/png", 33, expected)
            .is_err()
    );
}
