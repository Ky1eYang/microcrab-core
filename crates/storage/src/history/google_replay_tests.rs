//! Durable replay evidence and legacy history bytes use the real CreatorJournal.
#[path = "google_replay_tests/usage.rs"]
mod usage;
use super::*;
use microcrab_session::{Limits, SessionState, context::Context};
use serde_json::{Value, json};

const SESSION_ID: &str = "0123456789abcdef";

fn parts() -> Vec<Value> {
    vec![
        json!({"thought":true,"text":"Synthetic private thought","thoughtSignature":"dGhvdWdodA=="}),
        json!({"text":"Before.","thoughtSignature":"dGV4dA=="}),
        json!({"functionCall":{"name":"lookup","args":{}},"thoughtSignature":"Y2FsbA=="}),
        json!({"text":"After."}),
        json!({"text":"","thoughtSignature":"dGFpbA=="}),
    ]
}

fn recover(with_metadata: bool) -> Recover {
    let limits = Limits::default();
    let mut context = Context::new(
        "Frozen Google system".into(),
        serde_json::from_value(json!([
            {"name":"lookup","description":"Frozen lookup tool","parameters":{"type":"object"}}
        ]))
        .unwrap(),
        limits.max_context_bytes,
    )
    .unwrap();
    context.admit_user("Look up Tokyo", &json!({})).unwrap();
    let mut response = json!({
        "content":"Before.After.",
        "tool_calls":[{"id":"runtime-only-call","name":"lookup","arguments":{}}],
        "usage":{}
    });
    if with_metadata {
        response["provider_metadata"] = json!({"protocol":"google","parts":parts()});
        response["tool_calls"][0]["provider_metadata"] =
            json!({"protocol":"google","thought_signature":"Y2FsbA=="});
    }
    context
        .append_response(&serde_json::from_value(response).unwrap())
        .unwrap();
    context
        .append_tool_result("runtime-only-call".into(), "{\"temperature\":20}".into())
        .unwrap();
    Recover {
        state: SessionState::new(limits),
        context: context.snapshot().unwrap(),
        continuation: None,
        workspace: None,
        cache_report: None,
        agent: None,
    }
}

fn assert_replay_restored(restored: Recover, expected: &Value) {
    restored.validate().unwrap();
    assert_eq!(serde_json::to_value(&restored).unwrap(), *expected);
    let context = Context::from_snapshot(restored.context).unwrap();
    let content = context.content(128).unwrap();
    assert_eq!(content.system, "Frozen Google system");
    assert_eq!(content.tools[0].name, "lookup");
    let items = serde_json::to_value(&content.items).unwrap();
    let assistant = items
        .as_array()
        .unwrap()
        .iter()
        .find_map(|item| item.get("Assistant"))
        .unwrap();
    assert_eq!(assistant["content"], "Before.After.");
    assert_eq!(assistant["provider_metadata"]["parts"], json!(parts()));
    assert!(
        assistant["provider_metadata"]["parts"][2]["functionCall"]
            .get("id")
            .is_none()
    );
    assert_eq!(assistant["tool_calls"][0]["id"], "runtime-only-call");
    assert_eq!(
        assistant["tool_calls"][0]["provider_metadata"]["thought_signature"],
        "Y2FsbA=="
    );
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn full_google_replay_survives_file_creator_journal_close_and_reopen() {
    let directory = std::env::temp_dir().join(format!(
        "microcrab-google-replay-file-{}",
        uuid::Uuid::new_v4()
    ));
    let original = recover(true);
    let expected = serde_json::to_value(&original).unwrap();
    let (journal, restored) =
        CreatorJournal::open(directory.to_str().unwrap(), SESSION_ID.into(), true, false)
            .await
            .unwrap();
    assert!(restored.is_none());
    journal.commit(original).await.unwrap();
    let cursor = journal.cursor().await.unwrap();
    journal.close().await.unwrap();
    drop(journal);
    let (reopened, restored) =
        CreatorJournal::open(directory.to_str().unwrap(), SESSION_ID.into(), true, true)
            .await
            .unwrap();
    assert_eq!(reopened.cursor().await.unwrap(), cursor);
    assert_replay_restored(restored.unwrap(), &expected);
    reopened.close().await.unwrap();
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn full_google_replay_survives_sqlite_creator_journal_close_and_reopen() {
    let database = std::env::temp_dir().join(format!(
        "microcrab-google-replay-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let original = recover(true);
    let expected = serde_json::to_value(&original).unwrap();
    let (journal, restored) = CreatorJournal::open_sqlite(&database, SESSION_ID.into(), false)
        .await
        .unwrap();
    assert!(restored.is_none());
    journal.commit(original).await.unwrap();
    let cursor = journal.cursor().await.unwrap();
    journal.close().await.unwrap();
    drop(journal);
    let (reopened, restored) = CreatorJournal::open_sqlite(&database, SESSION_ID.into(), true)
        .await
        .unwrap();
    assert_eq!(reopened.cursor().await.unwrap(), cursor);
    assert_replay_restored(restored.unwrap(), &expected);
    reopened.close().await.unwrap();
}

#[test]
fn absent_assistant_replay_metadata_keeps_legacy_history_bytes_and_hash() {
    let checkpoint = CreatorEnvelope {
        session_id: SESSION_ID.into(),
        recover: recover(false),
        request_head: None,
        model_selection: None,
        route_witness: None,
        navigation_boundary: None,
    };
    let mut historical = serde_json::to_value(HistoryNode::new(None, checkpoint)).unwrap();
    let assistant = historical["checkpoint"]["recover"]["context"]["items"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find_map(|item| item.get_mut("Assistant"))
        .unwrap();
    // Author the pre-metadata shape independently of the current serializer.
    *assistant = json!({
        "content":"Before.After.",
        "tool_calls":[{"id":"runtime-only-call","name":"lookup","arguments":{}}]
    });
    let bytes = canonical_bytes(&historical).unwrap();
    let hash = content_hash(&historical).unwrap();
    let decoded: HistoryNode = serde_json::from_slice(&bytes).unwrap();
    decoded.validate(&hash).unwrap();
    assert_eq!(canonical_bytes(&decoded).unwrap(), bytes);
    assert_eq!(decoded.id().unwrap(), hash);
    assert!(
        !String::from_utf8(bytes)
            .unwrap()
            .contains("provider_metadata")
    );
}
