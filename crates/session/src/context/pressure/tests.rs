//! Usage calibration survives storage canonicalization without changing frozen input.
use super::*;
use crate::context::{Context, ContextSnapshot};
use microcrab_protocol::{Response, ToolDefinition, Usage};
use serde_json::{Value, json};

const ROUTE: &str = "fixture:model";

struct Capacity;
impl TokenEstimator for Capacity {
    fn estimate(&self, _: &Content) -> Result<Option<TokenEstimate>, Error> {
        Ok(Some(TokenEstimate {
            route: ROUTE.into(),
            provenance: "fixture_capacity".into(),
            input_tokens: 999_999,
            context_window_tokens: 1_000,
        }))
    }
}

fn response(text: &str) -> Response {
    Response {
        content: text.into(),
        tool_calls: vec![],
        provider_metadata: None,
        usage: Usage {
            input_tokens: Some(900),
            output_tokens: Some(3),
            samples: 1,
            ..Default::default()
        },
    }
}

fn fixture(version: u8) -> Context {
    // Historical frozen schemas and metadata can retain arbitrary key order.
    let nested: Value =
        serde_json::from_str(r#"{"z":[{"z":2,"a":1},[{"z":false,"a":null}]],"a":"unchanged"}"#)
            .unwrap();
    let mut context = Context::with_frozen_tools(
        "Frozen SYSTEM\n  unchanged".into(),
        vec![ToolDefinition {
            name: "echo".into(),
            description: "Frozen tool".into(),
            parameters: nested.clone(),
        }],
        65_536,
    )
    .unwrap();
    context
        .admit_user("retained user", &json!({"nested": nested}))
        .unwrap();
    let mut prior = response("retained answer");
    prior.provider_metadata = Some(microcrab_protocol::AssistantProviderMetadata::Neutral {
        parts: vec![
            microcrab_protocol::AssistantPart::Reasoning {
                text: "retained reasoning".into(),
            },
            microcrab_protocol::AssistantPart::Text {
                text: "retained answer".into(),
            },
        ],
        replay_hints: vec![microcrab_protocol::AssistantReplayHint {
            part_index: 0,
            scope: "fixture".into(),
            kind: "opaque".into(),
            data: nested,
        }],
    });
    context.append_response(&prior).unwrap();
    let mut evidence = context
        .capture_usage_request(Some(ROUTE.into()))
        .unwrap()
        .unwrap();
    evidence.fingerprint_version = version;
    evidence.fingerprint = context
        .usage_fingerprint(evidence.prefix_items, version)
        .unwrap()
        .unwrap();
    let accepted = response("new answer");
    context.append_response(&accepted).unwrap();
    context
        .accept_usage_request(Some(evidence), Some(ROUTE.into()), &accepted.usage)
        .unwrap();
    assert!(context.usage_evidence.is_some());
    context
}

fn canonical_roundtrip(snapshot: &ContextSnapshot) -> ContextSnapshot {
    let mut value = serde_json::to_value(snapshot).unwrap();
    value.sort_all_objects();
    serde_json::from_slice(&serde_json::to_vec(&value).unwrap()).unwrap()
}

#[test]
fn canonical_usage_roundtrip_preserves_nested_objects_and_live_frozen_bytes() {
    let context = fixture(1);
    let before = serde_json::to_vec(&context.content(64).unwrap()).unwrap();
    let snapshot = context.snapshot().unwrap();
    let canonical = canonical_roundtrip(&snapshot);
    canonical.validate().unwrap();
    let restored = Context::from_snapshot(canonical).unwrap();
    let estimate = restored.estimate_tokens(&Capacity, 64).unwrap().unwrap();
    assert_eq!(estimate.input_tokens, 900);
    assert_eq!(
        estimate.provenance,
        "verified_accepted_main_request_input_tokens_v1"
    );
    assert!(estimate.pressure(64, 0).unwrap() > 0.95);
    assert_eq!(
        context.usage_request_fingerprint(),
        restored.usage_request_fingerprint()
    );
    assert_eq!(
        before,
        serde_json::to_vec(&context.content(64).unwrap()).unwrap()
    );
    assert_eq!(
        context.usage_fingerprint(2, 1).unwrap(),
        restored.usage_fingerprint(2, 1).unwrap()
    );
    // The legacy hash demonstrably depends on the order that storage changed.
    assert_ne!(
        context.usage_fingerprint(2, 0).unwrap(),
        restored.usage_fingerprint(2, 0).unwrap()
    );
}

#[test]
fn changed_material_never_authorizes_measured_tokens_or_new_snapshot() {
    for mutation in 0..4 {
        let mut context = fixture(1);
        match mutation {
            0 => context.system.push_str(" changed"),
            1 => context.tools[0].parameters["a"] = json!("changed"),
            2 => {
                let crate::context::ContentItem::Assistant {
                    provider_metadata, ..
                } = &mut context.items[2]
                else {
                    panic!("fixture assistant")
                };
                let microcrab_protocol::AssistantProviderMetadata::Neutral { replay_hints, .. } =
                    provider_metadata.as_mut().unwrap()
                else {
                    panic!("neutral replay fixture")
                };
                replay_hints[0].data["z"][0]["a"] = json!(7);
            }
            _ => context.tools[0].parameters["z"]
                .as_array_mut()
                .unwrap()
                .reverse(),
        }
        assert!(context.estimate_tokens(&Capacity, 64).unwrap().is_none());
        assert!(context.validate_usage_evidence().is_err());
        let snapshot = context.snapshot().unwrap();
        assert!(snapshot.validate().is_err());
        assert!(Context::from_snapshot(snapshot).is_err());
    }
}

#[test]
fn changed_prefix_between_capture_and_accept_does_not_gain_evidence() {
    let mut context = fixture(1);
    let captured = context.capture_usage_request(Some(ROUTE.into())).unwrap();
    context.system.push_str(" changed");
    let accepted = response("answer");
    context.append_response(&accepted).unwrap();
    context
        .accept_usage_request(captured, Some(ROUTE.into()), &accepted.usage)
        .unwrap();
    assert!(context.usage_evidence.is_none());
    assert!(context.estimate_tokens(&Capacity, 64).unwrap().is_none());
}

#[test]
fn legacy_exact_match_retains_measurement_without_upgrading_frozen_bytes() {
    let context = fixture(0);
    let snapshot = context.snapshot().unwrap();
    let bytes = serde_json::to_vec(&snapshot).unwrap();
    let restored = Context::from_snapshot(snapshot).unwrap();
    assert_eq!(
        restored
            .estimate_tokens(&Capacity, 64)
            .unwrap()
            .unwrap()
            .input_tokens,
        900
    );
    assert_eq!(
        bytes,
        serde_json::to_vec(&restored.snapshot().unwrap()).unwrap()
    );
}

#[test]
fn legacy_storage_mismatch_retires_only_calibration_preserving_source_and_history() {
    let context = fixture(0);
    let snapshot = canonical_roundtrip(&context.snapshot().unwrap());
    let source = serde_json::to_vec(&snapshot).unwrap();
    snapshot.validate().unwrap();
    assert_eq!(source, serde_json::to_vec(&snapshot).unwrap());
    let restored = Context::from_snapshot(snapshot.clone()).unwrap();
    assert!(restored.usage_evidence.is_none());
    assert!(restored.usage_request_fingerprint().is_none());
    assert!(restored.estimate_tokens(&Capacity, 64).unwrap().is_none());
    let mut expected = serde_json::to_value(&snapshot).unwrap();
    expected.as_object_mut().unwrap().remove("usage_evidence");
    assert_eq!(
        expected,
        serde_json::to_value(restored.snapshot().unwrap()).unwrap()
    );
    assert_eq!(source, serde_json::to_vec(&snapshot).unwrap());
}

#[test]
fn legacy_quarantine_cannot_bypass_structure_or_identity_checks() {
    for mutation in 0..9 {
        let mut snapshot = canonical_roundtrip(&fixture(0).snapshot().unwrap());
        let mut evidence = serde_json::to_value(snapshot.usage_evidence.as_ref().unwrap()).unwrap();
        match mutation {
            0 => evidence["route"] = json!(""),
            1 => evidence["epoch"] = json!(9),
            2 => evidence["baseline_generation"] = json!(9),
            3 => evidence["prefix_items"] = json!(usize::MAX),
            4 => evidence["prefix_items"] = json!(1),
            5 => evidence["fingerprint"] = json!("invalid hash"),
            6 => evidence["fingerprint_version"] = json!(2),
            7 => snapshot.pending_epoch = true,
            _ => {
                snapshot.used_call_ids.insert("forged-call".into());
            }
        }
        snapshot.usage_evidence = Some(serde_json::from_value(evidence).unwrap());
        assert!(snapshot.validate().is_err(), "mutation {mutation}");
        assert!(
            Context::from_snapshot(snapshot).is_err(),
            "mutation {mutation}"
        );
    }
}
