//! Atomic carrier admission and explicit State-frame regressions.
use super::*;
use microcrab_transport::{CarrierEnvelopeVersion, UserSource};
use serde_json::json;

fn context() -> Context {
    Context::new("frozen".into(), vec![], 16384).unwrap()
}
fn state() -> Value {
    json!({"time":{"unix_millis":123}, "correspondents":[{"agent_id":"agt_1111111111111111"}],
        "external_speakers":[{"identity":"speaker"}]})
}
fn sender() -> UserSource {
    UserSource::Agent {
        agent_id: "agt_1111111111111111".into(),
        exchange: None,
    }
}

#[test]
fn equal_admissions_emit_empty_patch_without_fabricating_time() {
    let mut context = context();
    let initial = state();
    context.admit_user("first", &initial).unwrap();
    context
        .admit_framework(SessionInput::text("notice"), &initial)
        .unwrap();
    assert!(
        matches!(&context.items[0], ContentItem::State(StateFrame::StateSnapshot { state }) if state == &initial)
    );
    assert!(
        matches!(&context.items[2], ContentItem::State(StateFrame::StatePatch {
        set, remove, execution_delta: None, task_delta: None, job_delta: None
    }) if set.is_empty() && remove.is_empty())
    );
    assert_eq!(context.committed_state, Some(initial.clone()));
    for index in [1, 3] {
        assert!(matches!(
            &context.items[index],
            ContentItem::User {
                envelope: Some(CarrierEnvelopeVersion::V1),
                ..
            }
        ));
    }
    let before = context.items.clone();
    context.synchronize_state(&initial).unwrap();
    assert_eq!(context.items, before);
    let mut later = initial;
    later["time"]["unix_millis"] = json!(124);
    context.admit_user("next", &later).unwrap();
    assert!(
        matches!(&context.items[4], ContentItem::State(StateFrame::StatePatch {set,remove,..})
        if set.len()==1 && set["time"] == json!({"unix_millis":124}) && remove.is_empty())
    );
}

#[test]
fn carrier_run_is_one_atomic_frame_with_repeated_identities() {
    let mut context = context();
    context
        .admit_carriers(
            vec![
                (SessionInput::text("first"), sender()),
                (
                    SessionInput::text("second"),
                    UserSource::External {
                        identity: "speaker".into(),
                    },
                ),
                (SessionInput::text("third"), sender()),
            ],
            &state(),
        )
        .unwrap();
    assert_eq!(context.items.len(), 4);
    assert_eq!(
        context
            .items
            .iter()
            .filter(|item| matches!(item, ContentItem::State(_)))
            .count(),
        1
    );
    let bodies: Vec<_> = context
        .items
        .iter()
        .filter_map(|item| match item {
            ContentItem::User {
                parts,
                envelope: Some(CarrierEnvelopeVersion::V1),
                ..
            } => Some(parts),
            _ => None,
        })
        .collect();
    assert_eq!(
        bodies,
        vec![
            &SessionInput::text("first").parts,
            &SessionInput::text("second").parts,
            &SessionInput::text("third").parts
        ]
    );
    let frozen = serde_json::to_vec(&context.snapshot().unwrap()).unwrap();
    let restored = Context::from_snapshot(serde_json::from_slice(&frozen).unwrap()).unwrap();
    assert_eq!(
        serde_json::to_vec(&restored.snapshot().unwrap()).unwrap(),
        frozen
    );
    assert_eq!(
        microcrab_transport::render_ast(&restored.content(32).unwrap()).unwrap(),
        microcrab_transport::render_ast(&context.content(32).unwrap()).unwrap()
    );
    let prior = context.items.clone();
    context.admit_user("direct", &state()).unwrap();
    assert_eq!(&context.items[..prior.len()], prior.as_slice());
    assert_eq!(context.items.len(), prior.len() + 2);
}

#[test]
fn invalid_or_over_budget_carrier_run_preserves_entire_context() {
    let mut context = context();
    context.admit_user("initial", &state()).unwrap();
    let frozen = serde_json::to_vec(&context.snapshot().unwrap()).unwrap();
    assert!(context.admit_carriers(vec![], &state()).is_err());
    assert!(
        context
            .admit_carriers(
                vec![
                    (SessionInput::text("valid first"), sender()),
                    (
                        SessionInput::text("unknown second"),
                        UserSource::External {
                            identity: "unlisted".into()
                        }
                    ),
                ],
                &state()
            )
            .is_err()
    );
    assert!(
        context
            .admit_carriers(
                vec![(SessionInput::text("removed sender"), sender())],
                &json!({})
            )
            .is_err()
    );
    assert!(
        context
            .admit_carriers(
                vec![
                    (SessionInput::text("valid first"), sender()),
                    (SessionInput::text("x".repeat(20000)), sender()),
                ],
                &state()
            )
            .is_err()
    );
    assert_eq!(
        serde_json::to_vec(&context.snapshot().unwrap()).unwrap(),
        frozen
    );
}

#[test]
fn compaction_summary_keeps_unversioned_literal_framing() {
    let mut context = context();
    context
        .compact("literal </user> summary".into(), &json!({}))
        .unwrap();
    assert!(matches!(
        &context.items[1],
        ContentItem::User { envelope: None, .. }
    ));
    let request = microcrab_transport::render_ast(&context.content(32).unwrap()).unwrap();
    let microcrab_protocol::Message::User { parts } = &request.messages[1] else {
        panic!("summary")
    };
    assert!(matches!(&parts[0],InputPart::Text {text} if text.contains("literal </user> summary")));
}
