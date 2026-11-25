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
    assert_eq!(
        a.checkpoint_id,
        crate::history::dedup::split_v5(None, &checkpoint(0, "a"))
            .unwrap()
            .0
            .id()
            .unwrap()
    );
    let b = sqlite
        .commit(Some(a.clone()), checkpoint(1, "b"))
        .await
        .unwrap();
    assert_eq!(
        b.checkpoint_id,
        crate::history::dedup::split_v5(Some(a.checkpoint_id.clone()), &checkpoint(1, "b"))
            .unwrap()
            .0
            .id()
            .unwrap()
    );
    let _ = memory;
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

#[cfg(unix)]
#[tokio::test]
async fn sqlite_ancestry_reuses_verified_prefix_and_rechecks_snapshot_version() {
    let root = root();
    let path = root.join("store.sqlite");
    let journal = SqliteJournal::open(&path, "session-1".into())
        .await
        .unwrap();
    let a = journal.commit(None, checkpoint(0, "a")).await.unwrap();
    let b = journal
        .commit(Some(a.clone()), checkpoint(1, "b"))
        .await
        .unwrap();
    let outside = rusqlite::Connection::open(&path).unwrap();
    // Simulate legacy unindexed nodes so the byte-budgeted walk and prefix cache are exercised.
    outside
        .execute("DELETE FROM journal_ancestry_index WHERE session_id='session-1'", [])
        .unwrap();
    let size = |id: &str| -> usize {
        outside
            .query_row(
                "SELECT length(body) FROM journal_nodes WHERE session_id='session-1' AND id=?1",
                [id],
                |row| row.get::<_, u32>(0),
            )
            .unwrap() as usize
    };
    let read = |id: String, budget| {
        SqliteJournal::inspect_metadata(&path, "session-1".into(), id, budget, |_| false)
    };
    assert!(matches!(
        read(b.checkpoint_id.clone(), size(&b.checkpoint_id)).await,
        Err(Error::Limit(_))
    ));
    assert_eq!(
        read(b.checkpoint_id.clone(), size(&a.checkpoint_id))
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(read(b.checkpoint_id.clone(), 0).await.unwrap().len(), 2);
    let c = journal
        .commit(Some(b.clone()), checkpoint(2, "c"))
        .await
        .unwrap();
    // Re-index a and b via warm_ancestry, then delete only c from the index so
    // read(c, size(&c)) exercises prefix reuse + unindexed suffix c.
    SqliteJournal::warm_ancestry(&path, "session-1".into(), b.checkpoint_id.clone())
        .await
        .unwrap();
    outside
        .execute("DELETE FROM journal_ancestry_index WHERE session_id='session-1' AND id=?1", [&c.checkpoint_id])
        .unwrap();
    assert!(matches!(
        read(c.checkpoint_id.clone(), 0).await,
        Err(Error::Limit(_))
    ));
    assert_eq!(
        read(c.checkpoint_id.clone(), size(&c.checkpoint_id))
            .await
            .unwrap()
            .len(),
        3
    );
    journal
        .move_head(
            c,
            a.clone(),
            Travel::To {
                checkpoint_id: a.checkpoint_id.clone(),
            },
        )
        .await
        .unwrap();
    let d = journal
        .commit(Some(a.clone()), checkpoint(3, "d"))
        .await
        .unwrap();
    let selected = read(d.checkpoint_id.clone(), size(&d.checkpoint_id))
        .await
        .unwrap();
    assert_eq!(
        selected.into_iter().map(|node| node.id).collect::<Vec<_>>(),
        vec![d.checkpoint_id.clone(), a.checkpoint_id.clone()]
    );
    assert_eq!(read(d.checkpoint_id.clone(), 0).await.unwrap().len(), 2);
    let mut body: Vec<u8> = outside
        .query_row(
            "SELECT body FROM journal_nodes WHERE session_id='session-1' AND id=?1",
            [&a.checkpoint_id],
            |row| row.get(0),
        )
        .unwrap();
    let text = b"checkpoint";
    let index = body
        .windows(text.len())
        .position(|part| part == text)
        .unwrap();
    body[index] = b'x';
    outside
        .execute(
            "UPDATE journal_nodes SET body=?2 WHERE session_id='session-1' AND id=?1",
            rusqlite::params![a.checkpoint_id, body],
        )
        .unwrap();
    assert!(
        SqliteJournal::inspect_ancestry(&path, "session-1".into(), d.checkpoint_id.clone())
            .await
            .is_err()
    );
    outside
        .execute(
            "DELETE FROM journal_nodes WHERE session_id='session-1' AND id=?1",
            [&a.checkpoint_id],
        )
        .unwrap();
    assert!(
        SqliteJournal::inspect_ancestry(&path, "session-1".into(), d.checkpoint_id)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn sqlite_ancestry_index_survives_cross_session_writes_and_warm_ancestry_indexes_v4() {
    let root = root();
    let path = root.join("store.sqlite");
    let journal = SqliteJournal::open(&path, "session-1".into())
        .await
        .unwrap();
    let mut prev: Option<JournalCursor> = None;
    for turn in 0..4u64 {
        let cursor = journal
            .commit(prev.clone(), checkpoint(turn, &format!("t{turn}")))
            .await
            .unwrap();
        prev = Some(cursor);
    }
    let head = prev.unwrap().checkpoint_id;
    // Simulate legacy unindexed chain + out-of-band writer bumping data_version.
    let outside = rusqlite::Connection::open(&path).unwrap();
    outside
        .execute("DELETE FROM journal_ancestry_index WHERE session_id='session-1'", [])
        .unwrap();
    let indexed = SqliteJournal::warm_ancestry(&path, "session-1".into(), head.clone())
        .await
        .unwrap();
    assert_eq!(indexed, 4);
    // Another session writes to the same SQLite DB, advancing data_version.
    let other = SqliteJournal::open(&path, "session-2".into())
        .await
        .unwrap();
    let mut other_env = checkpoint(0, "other");
    other_env.session_id = "session-2".into();
    other.commit(None, other_env).await.unwrap();
    // Even with a 0-byte hydration budget after data_version changed, the derived index serves all 4 nodes.
    let verified = SqliteJournal::inspect_metadata(&path, "session-1".into(), head, 0, |_| false)
        .await
        .unwrap();
    assert_eq!(verified.len(), 4);
    other.close().await.unwrap();
    journal.close().await.unwrap();
}

#[tokio::test]
async fn sqlite_v5_deduplicates_static_and_item_chunks_across_checkpoints() {
    let root = root();
    let path = root.join("store.sqlite");
    let journal = SqliteJournal::open(&path, "session-dedup".into())
        .await
        .unwrap();
    let mut items = Vec::new();
    let mut prev: Option<JournalCursor> = None;
    for i in 0..20usize {
        items.push(microcrab_session::context::ContentItem::User {
            parts: vec![microcrab_session::protocol::InputPart::Text {
                text: format!("msg-{i}"),
            }],
            envelope: None,
            source: None,
        });
        let mut env = checkpoint(i as u64, &format!("msg-{i}"));
        env.session_id = "session-dedup".into();
        env.recover.context.system = "shared system prompt".into();
        env.recover.context.items = items.clone();
        prev = Some(journal.commit(prev, env).await.unwrap());
    }
    let head_cursor = prev.unwrap();
    let restored = journal.restore(RestoreTarget::Head).await.unwrap();
    assert_eq!(restored.cursor, head_cursor);
    assert_eq!(restored.checkpoint.recover.context.items.len(), 20);
    let conn = rusqlite::Connection::open(&path).unwrap();
    let max_node_bytes: i64 = conn
        .query_row(
            "SELECT MAX(length(body)) FROM journal_nodes WHERE session_id='session-dedup'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        max_node_bytes < 2048,
        "v5 header node should be compact, got {max_node_bytes} bytes"
    );
    let blob_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM journal_blobs WHERE session_id='session-dedup'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    // 1 shared static blob + 1 full 16-item chunk + 20 tail/partial chunks (< 20 * 3 = 60).
    assert!(
        blob_count < 45,
        "expected deduplicated blobs < 45, got {blob_count}"
    );
    journal.close().await.unwrap();
}
