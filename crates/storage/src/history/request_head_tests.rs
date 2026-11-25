use super::*;
use microcrab_session::{Limits, SessionState, context::Context};

fn checkpoint(epoch: u64, label: &str) -> Recover {
    let limits = Limits::default();
    let mut context = Context::new("system".into(), vec![], limits.max_context_bytes).unwrap();
    context.admit_user(label, &serde_json::json!({})).unwrap();
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

async fn publish(
    journal: &CreatorJournal,
    epoch: u64,
    label: &str,
    request: char,
) -> JournalCursor {
    journal.history_anchor().lock().unwrap().request_head = Some(request.to_string().repeat(64));
    journal.commit(checkpoint(epoch, label)).await.unwrap();
    journal.cursor().await.unwrap()
}

async fn selected_request_heads(journal: &CreatorJournal) {
    let root = publish(journal, 0, "root", 'a').await;
    publish(journal, 1, "old branch", 'b').await;
    publish(journal, 1, "latest checkpoint in old branch", 'c').await;
    assert_eq!(
        journal.display_request_heads(&[0, 1, 1, 2]).await.unwrap(),
        vec![
            (1, Some("c".repeat(64))),
            (0, Some("a".repeat(64))),
            (2, None)
        ]
    );
    let recovered = journal
        .prepare_travel(Travel::To {
            checkpoint_id: root.checkpoint_id,
        })
        .await
        .unwrap();
    journal.recovered(recovered).await.unwrap();
    publish(journal, 1, "sibling branch", 'd').await;
    assert_eq!(
        journal.display_request_heads(&[0, 1, 1, 2]).await.unwrap(),
        vec![
            (1, Some("d".repeat(64))),
            (0, Some("a".repeat(64))),
            (2, None)
        ]
    );
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn live_file_display_heads_follow_the_exact_branch() {
    let root = std::env::temp_dir().join(format!(
        "microcrab-live-request-heads-{}",
        uuid::Uuid::new_v4()
    ));
    let (journal, _) = CreatorJournal::open(root.to_str().unwrap(), "session".into(), true, false)
        .await
        .unwrap();
    selected_request_heads(&journal).await;
    journal.close().await.unwrap();
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn live_sqlite_display_heads_follow_the_exact_branch() {
    let root = std::env::temp_dir().join(format!(
        "microcrab-live-request-heads-{}",
        uuid::Uuid::new_v4()
    ));
    let (journal, _) =
        CreatorJournal::open_sqlite(root.join("store.sqlite"), "session".into(), false)
            .await
            .unwrap();
    selected_request_heads(&journal).await;
    journal.close().await.unwrap();
}
