use super::*;
use microcrab_session::state::communication::reply::{
    DialogueReplyBinding, DialogueReplyMutation, DialogueReplyStore, DialogueTerminalReason,
    MAX_CHUNK_BYTES, ReplyWrite,
};
use uuid::Uuid;

// Storage operates on already-certified values; the Session owns minting.
fn binding() -> DialogueReplyBinding {
    serde_json::from_value(serde_json::json!({
        "agent_id": "agt_000000000000001a",
        "admission": { "id": Uuid::new_v4(), "witness": "1".repeat(64) },
        "turn": { "id": Uuid::new_v4(), "number": 1 }
    }))
    .unwrap()
}
fn scope(
    binding: &DialogueReplyBinding,
    owner: &str,
    root: &str,
    session: &str,
) -> DialogueReplyScope {
    DialogueReplyScope::new(owner, root, session, binding.agent_id().clone()).unwrap()
}
fn dir() -> std::path::PathBuf {
    let directory = std::env::temp_dir().join(format!("microcrab-dialogue-{}", Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    directory
}
fn open(binding: &DialogueReplyBinding) -> DialogueReplyMutation {
    DialogueReplyMutation::Open {
        binding: binding.clone(),
    }
}
fn settle(binding: &DialogueReplyBinding, expected_revision: u64) -> DialogueReplyMutation {
    DialogueReplyMutation::Settle {
        binding: binding.clone(),
        expected_revision,
        reason: DialogueTerminalReason::Completed,
        at_ms: 9876,
    }
}
async fn exercise(store: &dyn DialogueReplyStore, binding: &DialogueReplyBinding) {
    assert!(matches!(
        store.apply(open(binding)).await.unwrap(),
        ReplyWrite::Applied(_)
    ));
    assert!(matches!(
        store.apply(open(binding)).await.unwrap(),
        ReplyWrite::AlreadyApplied(_)
    ));
    let effect_id = Uuid::new_v4();
    let append =
        |message: String, expected_revision, effect_id, is_final| DialogueReplyMutation::Append {
            binding: binding.clone(),
            expected_revision,
            effect_id,
            message,
            is_final,
            at_ms: 1000,
        };
    assert!(
        store
            .apply(append("x".repeat(MAX_CHUNK_BYTES + 1), 0, effect_id, true))
            .await
            .is_err()
    );
    assert!(
        store
            .apply(append("must not send".into(), 1, Uuid::new_v4(), false))
            .await
            .is_err()
    );
    assert!(matches!(
        store
            .apply(append("private payload".into(), 0, effect_id, true))
            .await
            .unwrap(),
        ReplyWrite::Applied(_)
    ));
    assert!(matches!(
        store
            .apply(append("private payload".into(), 0, effect_id, true))
            .await
            .unwrap(),
        ReplyWrite::AlreadyApplied(_)
    ));
    assert!(
        store
            .apply(append("changed".into(), 0, effect_id, true))
            .await
            .is_err()
    );
    assert!(
        store
            .apply(append("after final".into(), 1, Uuid::new_v4(), false))
            .await
            .is_err()
    );
    let record = store
        .load(binding.agent_id(), binding.admission())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.chunks()[0].message(), "private payload");
    assert!(record.terminal().is_none());
    assert!(store.apply(settle(binding, 0)).await.is_err());
    assert!(matches!(
        store.apply(settle(binding, 1)).await.unwrap(),
        ReplyWrite::Applied(_)
    ));
    assert!(matches!(
        store.apply(settle(binding, 1)).await.unwrap(),
        ReplyWrite::AlreadyApplied(_)
    ));

    let mut value = serde_json::to_value(binding).unwrap();
    value["turn"]["id"] = serde_json::json!(Uuid::new_v4());
    let other_turn = serde_json::from_value(value.clone()).unwrap();
    assert!(store.apply(open(&other_turn)).await.is_err());
    value["admission"]["witness"] = serde_json::json!("2".repeat(64));
    let wrong_credential: DialogueReplyBinding = serde_json::from_value(value).unwrap();
    assert!(
        store
            .load(binding.agent_id(), wrong_credential.admission())
            .await
            .is_err()
    );
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_exact_reopen_and_owner_purge() {
    let directory = dir();
    let binding = binding();
    let scope = scope(&binding, "owner", "root", "session");
    assert!(
        FileDialogueReplies::inspect(&directory, scope.clone(), binding.admission())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 0);
    let writer = FileDialogueReplies::open(&directory, scope.clone())
        .await
        .unwrap();
    assert!(
        FileDialogueReplies::open(&directory, scope.clone())
            .await
            .is_err()
    );
    exercise(&writer, &binding).await;
    let record = FileDialogueReplies::inspect(&directory, scope.clone(), binding.admission())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        record.terminal().unwrap().reason(),
        DialogueTerminalReason::Completed
    );
    assert!(
        FileDialogueReplies::purge(&directory, scope.clone())
            .await
            .is_err()
    );
    drop(writer);
    let reopened = FileDialogueReplies::open(&directory, scope.clone())
        .await
        .unwrap();
    assert_eq!(
        reopened
            .load(binding.agent_id(), binding.admission())
            .await
            .unwrap()
            .unwrap()
            .revision(),
        2
    );
    drop(reopened);
    let mut wrong = scope.clone();
    wrong.owner = "other".into();
    assert!(FileDialogueReplies::purge(&directory, wrong).await.is_err());
    FileDialogueReplies::purge(&directory, scope.clone())
        .await
        .unwrap();
    FileDialogueReplies::purge(&directory, scope.clone())
        .await
        .unwrap();
    assert!(
        FileDialogueReplies::inspect(&directory, scope, binding.admission())
            .await
            .unwrap()
            .is_none()
    );
    std::fs::remove_dir(directory).unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_uncertain_terminal_quarantines_detached_reads_until_writer_reopens() {
    use microcrab_session::observation::diagnostics::EffectKnowledge;
    let directory = dir();
    let binding = binding();
    let scope = scope(&binding, "owner", "root", "session");
    let writer = FileDialogueReplies::open(&directory, scope.clone())
        .await
        .unwrap();
    writer.apply(open(&binding)).await.unwrap();
    super::file::fail_after_rename(&directory, binding.admission().id());
    let error = writer.apply(settle(&binding, 0)).await.err().unwrap();
    let diagnostic = error.tool_diagnostic();
    assert_eq!(diagnostic.effects, EffectKnowledge::MayHaveOccurred);
    assert_eq!(diagnostic.retryable, Some(false));
    assert!(
        FileDialogueReplies::inspect(&directory, scope.clone(), binding.admission())
            .await
            .is_err()
    );
    drop(writer);
    // Reopening verifies the surviving image and syncs the directory before
    // returning success. Exact retry observes it but cannot publish twice.
    let reopened = FileDialogueReplies::open(&directory, scope.clone())
        .await
        .unwrap();
    assert!(matches!(
        reopened.apply(settle(&binding, 0)).await.unwrap(),
        ReplyWrite::AlreadyApplied(_)
    ));
    let record = FileDialogueReplies::inspect(&directory, scope.clone(), binding.admission())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        record.terminal().unwrap().reason(),
        DialogueTerminalReason::Completed
    );
    drop(reopened);
    FileDialogueReplies::purge(&directory, scope).await.unwrap();
    std::fs::remove_dir(directory).unwrap();
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_uncertain_terminal_is_quarantined_until_explicit_reopen() {
    use microcrab_session::observation::diagnostics::EffectKnowledge;
    let directory = dir();
    let database = directory.join("test.sqlite3");
    let binding = binding();
    let scope = scope(&binding, "owner", "root", "session");
    let writer = SqliteDialogueReplies::open(&database, scope.clone())
        .await
        .unwrap();
    writer.apply(open(&binding)).await.unwrap();
    super::sqlite::fail_after_commit(binding.admission().id());
    let error = writer.apply(settle(&binding, 0)).await.err().unwrap();
    let diagnostic = error.tool_diagnostic();
    assert_eq!(diagnostic.effects, EffectKnowledge::MayHaveOccurred);
    assert_eq!(diagnostic.retryable, Some(false));
    assert!(
        writer
            .load(binding.agent_id(), binding.admission())
            .await
            .is_err()
    );
    drop(writer);
    assert!(
        SqliteDialogueReplies::inspect(&database, scope.clone(), binding.admission())
            .await
            .is_err()
    );
    let reopened = SqliteDialogueReplies::open(&database, scope.clone())
        .await
        .unwrap();
    let record = SqliteDialogueReplies::inspect(&database, scope, binding.admission())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        record.terminal().unwrap().reason(),
        DialogueTerminalReason::Completed
    );
    assert_eq!(record.revision(), 1);
    drop(reopened);
    std::fs::remove_dir_all(directory).unwrap();
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_root_purge_fences_live_writers_and_exact_descendants() {
    use crate::audit::SqliteAuditStore;
    let directory = dir();
    let database = directory.join("test.sqlite3");
    let child = binding();
    let child_scope = scope(&child, "owner", "root", "child");
    assert!(
        SqliteDialogueReplies::inspect(&database, child_scope.clone(), child.admission())
            .await
            .unwrap()
            .is_none()
    );
    assert!(!database.exists());
    let writer = SqliteDialogueReplies::open(&database, child_scope.clone())
        .await
        .unwrap();
    assert!(
        SqliteDialogueReplies::open(&database, child_scope.clone())
            .await
            .is_err()
    );
    exercise(&writer, &child).await;
    let root = binding();
    let root_scope = scope(&root, "owner", "root", "root");
    let root_writer = SqliteDialogueReplies::open(&database, root_scope.clone())
        .await
        .unwrap();
    root_writer.apply(open(&root)).await.unwrap();
    let other = binding();
    let other_scope = scope(&other, "other owner", "root", "other");
    let other_writer = SqliteDialogueReplies::open(&database, other_scope.clone())
        .await
        .unwrap();
    other_writer.apply(open(&other)).await.unwrap();
    assert!(SqliteAuditStore::purge_root(&database, "owner", "root").is_err());
    drop(writer);
    drop(root_writer);
    let record = SqliteDialogueReplies::inspect(&database, child_scope.clone(), child.admission())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.terminal().unwrap().at_ms(), 9876);
    SqliteAuditStore::purge_root(&database, "owner", "root").unwrap();
    assert!(
        SqliteDialogueReplies::inspect(&database, child_scope, child.admission())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        SqliteDialogueReplies::inspect(&database, root_scope, root.admission())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        SqliteDialogueReplies::inspect(&database, other_scope, other.admission())
            .await
            .unwrap()
            .is_some()
    );
    drop(other_writer);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn memory_budget_full_keeps_terminal_and_empty_dialogue_admission_available() {
    use microcrab_session::state::communication::reply::{MAX_CHUNKS, MemoryDialogueReplies};
    let first = binding();
    let store = MemoryDialogueReplies::new(first.agent_id().clone());
    let mut hit_limit = false;
    for _ in 0..32 {
        let binding = binding();
        store.apply(open(&binding)).await.unwrap();
        let mut revision = 0;
        for _ in 0..8 {
            // Contrived escaped UTF-8 charges encoded bytes, not only raw text.
            let mut message = "a".to_owned();
            message.push_str(&"\u{0001}".repeat(MAX_CHUNK_BYTES - 1));
            let result = store
                .apply(DialogueReplyMutation::Append {
                    binding: binding.clone(),
                    expected_revision: revision,
                    effect_id: Uuid::new_v4(),
                    message,
                    is_final: false,
                    at_ms: 1000,
                })
                .await;
            if matches!(result, Err(Error::Limit(_))) {
                hit_limit = true;
                break;
            }
            result.unwrap();
            revision += 1;
        }
        let settled = store.apply(settle(&binding, revision)).await.unwrap();
        assert!(settled.record().terminal().is_some());
        assert!(settled.record().chunks().len() <= MAX_CHUNKS);
        if hit_limit {
            break;
        }
    }
    assert!(hit_limit);
    store.apply(open(&first)).await.unwrap();
    assert!(
        store
            .apply(settle(&first, 0))
            .await
            .unwrap()
            .record()
            .chunks()
            .is_empty()
    );
}
