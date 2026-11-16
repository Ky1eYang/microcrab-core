use super::*;
use microcrab_session::{Limits, SessionState, context::Context};
use serde_json::json;

#[tokio::test]
async fn sqlite_request_heads_retain_compaction_epochs_and_exclude_siblings() {
    let directory = root();
    let path = directory.join("store.sqlite");
    let journal = SqliteJournal::open(&path, "session-1".into())
        .await
        .unwrap();
    let mut first = checkpoint(0, "a");
    first.request_head = Some("a".repeat(64));
    let a = journal.commit(None, first).await.unwrap();
    let mut second = checkpoint(1, "b");
    second.request_head = Some("b".repeat(64));
    let b = journal.commit(Some(a.clone()), second).await.unwrap();
    journal
        .move_head(b.clone(), a.clone(), Travel::Undo)
        .await
        .unwrap();
    let c = journal
        .commit(Some(a.clone()), checkpoint(2, "c"))
        .await
        .unwrap();
    assert_eq!(
        SqliteJournal::inspect_request_heads(&path, "session-1".into(), c.checkpoint_id.clone())
            .await
            .unwrap(),
        vec![
            (c.checkpoint_id.clone(), None),
            (a.checkpoint_id.clone(), Some("a".repeat(64)))
        ]
    );
    assert_eq!(
        SqliteJournal::inspect_request_heads(&path, "session-1".into(), b.checkpoint_id.clone())
            .await
            .unwrap(),
        vec![
            (b.checkpoint_id, Some("b".repeat(64))),
            (a.checkpoint_id, Some("a".repeat(64)))
        ]
    );
    assert_eq!(
        journal.restore(RestoreTarget::Head).await.unwrap().cursor,
        c
    );
    assert!(
        SqliteJournal::inspect_request_heads(&path, "foreign".into(), c.checkpoint_id)
            .await
            .is_err()
    );
    assert!(
        SqliteJournal::inspect_request_heads(&path, "session-1".into(), "f".repeat(64))
            .await
            .is_err()
    );
}

fn root() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("microcrab-sqlite-history-{}", uuid::Uuid::new_v4()))
}
fn checkpoint(epoch: u64, label: &str) -> CreatorEnvelope {
    let limits = Limits::default();
    let context = Context::new(label.into(), vec![], limits.max_context_bytes).unwrap();
    let mut snapshot = context.snapshot().unwrap();
    snapshot.epoch = epoch;
    let mut state = SessionState::new(limits);
    state.epoch_id = epoch;
    CreatorEnvelope {
        session_id: "session-1".into(),
        recover: Recover {
            state,
            context: snapshot,
            continuation: None,
            workspace: None,
            cache_report: None,
            agent: None,
            prompt_config: None,
        },
        request_head: None,
        model_selection: None,
        route_witness: None,
        navigation_boundary: None,
    }
}
#[tokio::test]
async fn sqlite_ancestry_inspection_is_read_only_and_rejects_missing_or_foreign_parents() {
    let root = root();
    let database = root.join("store.sqlite");
    let journal = SqliteJournal::open(&database, "session-1".into())
        .await
        .unwrap();
    let a = journal.commit(None, checkpoint(0, "a")).await.unwrap();
    let b = journal
        .commit(Some(a.clone()), checkpoint(0, "b"))
        .await
        .unwrap();
    journal
        .move_head(b.clone(), a.clone(), Travel::Undo)
        .await
        .unwrap();
    let c = journal
        .commit(Some(a.clone()), checkpoint(0, "c"))
        .await
        .unwrap();
    assert_eq!(
        SqliteJournal::inspect_ancestry(&database, "session-1".into(), c.checkpoint_id.clone())
            .await
            .unwrap(),
        vec![c.checkpoint_id.clone(), a.checkpoint_id.clone()]
    );
    assert_eq!(
        journal.restore(RestoreTarget::Head).await.unwrap().cursor,
        c
    );
    assert!(
        SqliteJournal::inspect_ancestry(
            &database,
            "foreign-session".into(),
            c.checkpoint_id.clone()
        )
        .await
        .is_err()
    );
    journal
        .move_head(
            c.clone(),
            b.clone(),
            Travel::To {
                checkpoint_id: b.checkpoint_id.clone(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        SqliteJournal::inspect_ancestry(&database, "session-1".into(), b.checkpoint_id.clone())
            .await
            .unwrap(),
        vec![b.checkpoint_id.clone(), a.checkpoint_id.clone()]
    );
    // A valid own node can still reference a retained node from another Session.
    let mut foreign = checkpoint(0, "foreign");
    foreign.session_id = "foreign-session".into();
    let foreign = HistoryNode::new(None, foreign);
    let foreign_id = foreign.id().unwrap();
    let child = HistoryNode::new(Some(foreign_id.clone()), checkpoint(0, "foreign-parent"));
    let child_id = child.id().unwrap();
    let connection = rusqlite::Connection::open(&database).unwrap();
    for (id, node) in [(&foreign_id, &foreign), (&child_id, &child)] {
        connection
            .execute(
                "INSERT INTO journal_nodes(session_id,id,body) VALUES(?1,?2,?3)",
                rusqlite::params!["session-1", id, canonical_bytes(node).unwrap()],
            )
            .unwrap();
    }
    assert!(
        SqliteJournal::inspect_ancestry(&database, "session-1".into(), child_id)
            .await
            .is_err()
    );
    connection
        .execute(
            "DELETE FROM journal_nodes WHERE session_id=?1 AND id=?2",
            rusqlite::params!["session-1", a.checkpoint_id],
        )
        .unwrap();
    assert!(
        SqliteJournal::inspect_ancestry(&database, "session-1".into(), b.checkpoint_id.clone())
            .await
            .is_err()
    );
    assert_eq!(
        journal.restore(RestoreTarget::Head).await.unwrap().cursor,
        b
    );
    journal.close().await.unwrap();
}

#[tokio::test]
async fn sqlite_nodes_match_memory_hashes_and_branch_travel_survives_reopen() {
    let root = root();
    let database = root.join("store.sqlite");
    let sqlite = SqliteJournal::open(&database, "session-1".into())
        .await
        .unwrap();
    let memory = MemoryJournal::default();
    let a = sqlite.commit(None, checkpoint(0, "a")).await.unwrap();
    assert_eq!(a, memory.commit(None, checkpoint(0, "a")).await.unwrap());
    let b = sqlite
        .commit(Some(a.clone()), checkpoint(1, "b"))
        .await
        .unwrap();
    assert_eq!(
        b,
        memory
            .commit(Some(a.clone()), checkpoint(1, "b"))
            .await
            .unwrap()
    );
    assert!(
        sqlite
            .commit(Some(a.clone()), checkpoint(2, "stale"))
            .await
            .is_err()
    );
    let target = sqlite.travel(b.clone(), Travel::Undo).await.unwrap();
    assert_eq!(target.cursor, a);
    assert_eq!(sqlite.restore(RestoreTarget::Head).await.unwrap().cursor, b);
    sqlite
        .move_head(b.clone(), a.clone(), Travel::Undo)
        .await
        .unwrap();
    assert_eq!(
        sqlite.travel(a.clone(), Travel::Redo).await.unwrap().cursor,
        b
    );
    let c = sqlite.commit(Some(a), checkpoint(1, "c")).await.unwrap();
    assert_ne!(b.checkpoint_id, c.checkpoint_id);
    assert!(sqlite.travel(c.clone(), Travel::Redo).await.is_err());
    sqlite.close().await.unwrap();
    assert!(sqlite.restore(RestoreTarget::Head).await.is_err());
    let reopened = SqliteJournal::open(&database, "session-1".into())
        .await
        .unwrap();
    assert_eq!(
        reopened.restore(RestoreTarget::Head).await.unwrap().cursor,
        c
    );
    assert_eq!(
        reopened
            .restore(RestoreTarget::Checkpoint {
                checkpoint_id: b.checkpoint_id
            })
            .await
            .unwrap()
            .checkpoint
            .recover
            .state
            .epoch_id,
        1
    );
    reopened.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
async fn sqlite_connections_enforce_cas_and_isolate_session_nodes() {
    let root = root();
    let database = root.join("store.sqlite");
    let left = SqliteJournal::open(&database, "session-1".into())
        .await
        .unwrap();
    let right = SqliteJournal::open(&database, "session-1".into())
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        left.commit(None, checkpoint(0, "left")),
        right.commit(None, checkpoint(0, "right"))
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let selected = left.restore(RestoreTarget::Head).await.unwrap();
    let other = SqliteJournal::open(&database, "session-2".into())
        .await
        .unwrap();
    assert!(
        other
            .restore(RestoreTarget::Checkpoint {
                checkpoint_id: selected.cursor.checkpoint_id
            })
            .await
            .is_err()
    );
    assert!(
        other
            .commit(None, checkpoint(0, "wrong owner namespace"))
            .await
            .is_err()
    );
    left.close().await.unwrap();
    right.close().await.unwrap();
    other.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
async fn sqlite_coordinator_waits_for_recovered_ack_and_retains_request_anchor() {
    let root = root();
    let database = root.join("store.sqlite");
    let (journal, _) = CreatorJournal::open_sqlite(&database, "session-1".into(), false)
        .await
        .unwrap();
    journal.commit(checkpoint(0, "a").recover).await.unwrap();
    let a = journal.cursor().await.unwrap();
    journal.history_anchor().lock().unwrap().request_head = Some("a".repeat(64));
    journal.commit(checkpoint(1, "b").recover).await.unwrap();
    let b = journal.cursor().await.unwrap();
    let recover = journal.prepare_travel(Travel::Undo).await.unwrap();
    assert_eq!(journal.cursor().await, Some(b));
    journal.recovered(recover).await.unwrap();
    assert_eq!(journal.cursor().await, Some(a));
    assert_eq!(journal.history_anchor().lock().unwrap().request_head, None);
    journal.close().await.unwrap();
    let (restored, recover) = CreatorJournal::open_sqlite(&database, "session-1".into(), true)
        .await
        .unwrap();
    assert_eq!(recover.unwrap().state.epoch_id, 0);
    restored.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn shared_canonical_hash_has_fixed_cross_language_encoding() {
    assert_eq!(
        canonical_bytes(&json!({"z":[2,1],"a":{"y":"é","b":true}})).unwrap(),
        "{\"a\":{\"b\":true,\"y\":\"é\"},\"z\":[2,1]}".as_bytes()
    );
    assert_eq!(
        content_hash(&json!({})).unwrap(),
        "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
    );
}
