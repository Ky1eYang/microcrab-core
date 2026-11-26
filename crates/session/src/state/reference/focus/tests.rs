use super::*;
use serde_json::json;
fn snapshot(id: &str, body: &str) -> FocusSnapshot {
    FocusSnapshot::capture(
        ReferenceInfo {
            id: id.into(),
            title: "selected".into(),
            location: "source.md".into(),
            description: "section".into(),
        },
        body.into(),
    )
}
#[test]
fn recorded_body_and_digest_restore_without_reopening_source() {
    let frozen = FocusedReferences(vec![snapshot("section-a", "original\n完整")]);
    let encoded = serde_json::to_vec(&frozen.0).unwrap();
    let restored = FocusedReferences(serde_json::from_slice(&encoded).unwrap());
    validate_snapshots(&restored.0).unwrap();
    assert_eq!(
        restored.read("section-a").unwrap().unwrap().as_text().unwrap(),
        "original\n完整"
    );
    assert!(restored.read("unselected").unwrap().is_none());
    assert_eq!(restored.list().unwrap().len(), 1);
    let mut tampered = restored.0;
    tampered[0].body = "changed".into();
    assert!(validate_snapshots(&tampered).is_err());
}
#[test]
fn focus_capacity_and_selector_ambiguity_fail_closed() {
    assert!(
        serde_json::from_value::<FocusSelection>(json!({"section_id":"id","path":"secret"}))
            .is_err()
    );
    assert!(serde_json::from_value::<FocusSelection>(json!({"path":"file","extra":true})).is_err());
    assert!(validate_snapshots(&vec![snapshot("duplicate", "body"); 2]).is_err());
    assert!(
        validate_snapshots(
            &(0..33)
                .map(|index| snapshot(&format!("id{index}"), "a"))
                .collect::<Vec<_>>()
        )
        .is_err()
    );
    assert!(
        validate_snapshots(&[snapshot("oversized", &"x".repeat(MAX_FOCUS_BYTES + 1))]).is_err()
    );
}
#[test]
fn child_catalog_contains_only_snapshot_and_recovery_reinstalls_it() {
    let state = crate::State::new(crate::Limits::default());
    state
        .install_focused_references(vec![snapshot("chosen", "frozen")])
        .unwrap();
    assert_eq!(
        state
            .list_references()
            .unwrap()
            .iter()
            .map(|row| row.info.id.as_str())
            .collect::<Vec<_>>(),
        vec!["chosen"]
    );
    let mut communication = crate::state::communication::CommunicationState::fresh(
        1,
        crate::state::communication::SessionInteraction::Persistent,
    );
    communication.focus = vec![snapshot("chosen", "frozen")];
    let encoded = serde_json::to_vec(&communication).unwrap();
    let recovered: crate::state::communication::CommunicationState =
        serde_json::from_slice(&encoded).unwrap();
    let fresh = crate::State::new(crate::Limits::default());
    fresh.install_focused_references(recovered.focus).unwrap();
    assert_eq!(
        fresh.read_reference("chosen").unwrap().as_text(),
        Some("frozen")
    );
    assert!(fresh.read_reference("other").is_err());
}
