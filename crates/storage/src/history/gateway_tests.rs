//! Neutral semantic replay and route identity survive real durable carriers.
use super::*;
use microcrab_protocol::{AssistantPart, AssistantProviderMetadata, Response, Usage};
use microcrab_session::{Limits, SessionState, context::Context};
use serde_json::json;

fn model() -> crate::directory::ModelSelection {
    crate::directory::ModelSelection {
        provider_id: "mixed".into(),
        model: "shared-model".into(),
        variant: None,
    }
}
fn neutral_recover() -> Recover {
    let limits = Limits::default();
    let mut context = Context::new("system".into(), vec![], limits.max_context_bytes).unwrap();
    context
        .admit_user("input", &json!({"session":{"epoch":0}}))
        .unwrap();
    context
        .append_response(&Response {
            content: "answer".into(),
            tool_calls: vec![],
            usage: Usage {
                samples: 1,
                ..Default::default()
            },
            provider_metadata: Some(AssistantProviderMetadata::Neutral {
                parts: vec![
                    AssistantPart::Reasoning {
                        text: "intermediate thought".into(),
                    },
                    AssistantPart::Text {
                        text: "answer".into(),
                    },
                ],
                replay_hints: vec![],
            }),
        })
        .unwrap();
    Recover {
        state: SessionState::new(limits),
        context: context.snapshot().unwrap(),
        continuation: None,
        workspace: None,
        cache_report: None,
        agent: None,
        prompt_config: None,
    }
}
async fn commit_neutral(journal: &CreatorJournal) -> JournalCursor {
    journal
        .prepare_model_route(model(), Some("a".repeat(64)))
        .await;
    Journal::commit(journal, neutral_recover()).await.unwrap();
    let first = journal.cursor().await.unwrap();
    // Identical semantic content/model with another effective wire route must
    // have a distinct immutable envelope, before any provider effect.
    journal
        .prepare_model_route(model(), Some("b".repeat(64)))
        .await;
    Journal::commit(journal, neutral_recover()).await.unwrap();
    let second = journal.cursor().await.unwrap();
    assert_ne!(first.checkpoint_id, second.checkpoint_id);
    let original = journal
        .prepare_travel(Travel::To {
            checkpoint_id: first.checkpoint_id.clone(),
        })
        .await
        .unwrap();
    assert_eq!(
        journal.restore_model_route().await.unwrap(),
        (first.clone(), Some(model()), Some("a".repeat(64)))
    );
    Journal::recovered(journal, original).await.unwrap();
    first
}
async fn assert_reopened(journal: CreatorJournal, restored: Recover, expected: JournalCursor) {
    assert_eq!(
        journal.restore_model_route().await.unwrap(),
        (expected, Some(model()), Some("a".repeat(64)))
    );
    assert_eq!(
        serde_json::to_value(&restored).unwrap(),
        serde_json::to_value(neutral_recover()).unwrap()
    );
    let assistant = restored
        .context
        .items
        .iter()
        .find_map(|item| match item {
            microcrab_session::context::ContentItem::Assistant {
                provider_metadata: Some(metadata),
                ..
            } => Some(metadata),
            _ => None,
        })
        .unwrap();
    let AssistantProviderMetadata::Neutral {
        parts,
        replay_hints,
    } = assistant
    else {
        panic!("neutral semantic history")
    };
    assert!(
        matches!(&parts[0], AssistantPart::Reasoning { text } if text == "intermediate thought")
    );
    assert!(replay_hints.is_empty());
    Journal::close(&journal).await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn neutral_reasoning_and_route_witness_survive_file_reopen_and_travel() {
    let directory =
        std::env::temp_dir().join(format!("microcrab-neutral-file-{}", uuid::Uuid::new_v4()));
    let (journal, _) = CreatorJournal::open(
        directory.to_str().unwrap(),
        "neutral-file".into(),
        true,
        false,
    )
    .await
    .unwrap();
    let first = commit_neutral(&journal).await;
    Journal::close(&journal).await.unwrap();
    let (journal, restored) = CreatorJournal::open(
        directory.to_str().unwrap(),
        "neutral-file".into(),
        true,
        true,
    )
    .await
    .unwrap();
    assert_reopened(journal, restored.unwrap(), first).await;
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn neutral_reasoning_and_route_witness_survive_sqlite_reopen_and_travel() {
    let database =
        std::env::temp_dir().join(format!("microcrab-neutral-{}.sqlite", uuid::Uuid::new_v4()));
    let (journal, _) = CreatorJournal::open_sqlite(&database, "neutral-sqlite".into(), false)
        .await
        .unwrap();
    let first = commit_neutral(&journal).await;
    Journal::close(&journal).await.unwrap();
    let (journal, restored) = CreatorJournal::open_sqlite(&database, "neutral-sqlite".into(), true)
        .await
        .unwrap();
    assert_reopened(journal, restored.unwrap(), first).await;
}
