use super::*;
use microcrab_session::{Limits, SessionState, context::Context};
use serde_json::json;

fn recover(epoch: u64, label: &str) -> Recover {
    let limits = Limits::default();
    let mut context = Context::new("system".into(), vec![], limits.max_context_bytes).unwrap();
    context.admit_user(label, &json!({})).unwrap();
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
#[tokio::test]
async fn display_lineage_proves_appends_and_invalidates_same_head_travel() {
    let root = directory();
    let path = root.to_str().unwrap();
    let (journal, _) = CreatorJournal::open(path, "display-lineage".into(), true, false)
        .await
        .unwrap();
    let first = recover(0, "one");
    journal.commit(first.clone()).await.unwrap();
    let initial = journal.display_witness().await;
    let mut appended = first;
    appended
        .context
        .items
        .push(microcrab_session::context::ContentItem::User {
            parts: vec![microcrab_session::protocol::InputPart::Text { text: "two".into() }],
            source: None,
        });
    journal.commit(appended).await.unwrap();
    assert_eq!(journal.display_witness().await.lineage, initial.lineage);
    assert_ne!(journal.display_witness().await.cursor, initial.cursor);
    let head = journal.cursor().await.unwrap();
    let prepared = journal
        .prepare_travel(Travel::To {
            checkpoint_id: head.checkpoint_id,
        })
        .await
        .unwrap();
    journal.recovered(prepared).await.unwrap();
    let traveled = journal.display_witness().await;
    assert_ne!(traveled.lineage, initial.lineage);
    assert_eq!(traveled.cursor, journal.cursor().await);
    journal.commit(recover(0, "replacement")).await.unwrap();
    let replaced = journal.display_witness().await;
    assert_ne!(replaced.lineage, traveled.lineage);
    journal.commit(recover(1, "epoch")).await.unwrap();
    let changed_epoch = journal.display_witness().await;
    assert_ne!(changed_epoch.lineage, replaced.lineage);
    assert_eq!(
        journal.history_anchor().lock().unwrap().display_lineage,
        changed_epoch.lineage
    );
    journal.close().await.unwrap();
    let (reopened, _) = CreatorJournal::open(path, "display-lineage".into(), true, true)
        .await
        .unwrap();
    assert_ne!(
        reopened.display_witness().await.lineage,
        changed_epoch.lineage
    );
    assert_eq!(reopened.cursor().await, changed_epoch.cursor);
}

#[tokio::test]
async fn compacted_display_cache_is_branch_scoped_and_survives_linear_commits() {
    use microcrab_session::context::CompactionMarker;
    fn compacted(epoch: u64, label: &str, marker: &CompactionMarker) -> Recover {
        let mut checkpoint = recover(epoch, &marker.summary);
        checkpoint.context.system = format!("system {label}");
        checkpoint.context.compaction = Some(marker.clone());
        checkpoint
    }
    fn marker(summary: &str) -> CompactionMarker {
        let mut context =
            Context::new("system".into(), vec![], Limits::default().max_context_bytes).unwrap();
        context.compact(summary.into(), &json!({})).unwrap();
        context.compaction().unwrap().clone()
    }
    let marker = marker("summary");
    let (journal, _) = CreatorJournal::open("unused-memory", "cache-branch".into(), false, false)
        .await
        .unwrap();
    journal.history_anchor().lock().unwrap().request_head = Some("a".repeat(64));
    journal.commit(recover(0, "original-a")).await.unwrap();
    let root = journal.cursor().await.unwrap();
    journal
        .commit(compacted(1, "summary", &marker))
        .await
        .unwrap();
    let selected = journal.cursor().await.unwrap();
    let initial = journal.compacted_history(Some(&marker)).await.unwrap();
    assert_eq!(initial.len(), 1);
    assert_eq!(
        journal
            .compacted_history_with_evidence(Some(&marker))
            .await
            .unwrap()[0]
            .request_head,
        Some("a".repeat(64))
    );
    assert_eq!(journal.cursor().await.unwrap(), selected);
    // More than the cold ancestry budget proves the immutable prefix is reused.
    for index in 0..1025 {
        journal
            .commit(compacted(1, &format!("after-a-{index}"), &marker))
            .await
            .unwrap();
    }
    let cached = journal.compacted_history(Some(&marker)).await.unwrap();
    assert_eq!(
        journal
            .compacted_history_with_evidence(Some(&marker))
            .await
            .unwrap()[0]
            .request_head,
        Some("a".repeat(64))
    );
    assert_eq!(
        serde_json::to_value(cached).unwrap(),
        serde_json::to_value(&initial).unwrap()
    );
    let mut context =
        Context::new("system".into(), vec![], Limits::default().max_context_bytes).unwrap();
    context
        .compact("second summary".into(), &json!({}))
        .unwrap();
    let second_marker = context.compaction().unwrap().clone();
    journal
        .commit(compacted(2, "second compact", &second_marker))
        .await
        .unwrap();
    let twice = journal
        .compacted_history(Some(&second_marker))
        .await
        .unwrap();
    assert_eq!(twice.len(), 2);
    assert!(
        serde_json::to_string(&twice[0])
            .unwrap()
            .contains("original-a")
    );
    assert!(
        serde_json::to_string(&twice[1])
            .unwrap()
            .contains("after-a-1024")
    );
    let target = journal
        .prepare_travel(Travel::To {
            checkpoint_id: root.checkpoint_id,
        })
        .await
        .unwrap();
    journal.recovered(target).await.unwrap();
    assert!(journal.selection.lock().await.compacted.is_none());
    journal.history_anchor().lock().unwrap().request_head = Some("b".repeat(64));
    journal.commit(recover(0, "sibling-b")).await.unwrap();
    // Equal marker IDs on sibling branches cannot reuse a stale prefix.
    journal
        .commit(compacted(1, "summary", &marker))
        .await
        .unwrap();
    let sibling = journal.compacted_history(Some(&marker)).await.unwrap();
    assert_eq!(sibling.len(), 1);
    assert_eq!(
        journal
            .compacted_history_with_evidence(Some(&marker))
            .await
            .unwrap()[0]
            .request_head,
        Some("b".repeat(64))
    );
    assert!(
        serde_json::to_string(&sibling[0])
            .unwrap()
            .contains("sibling-b")
    );
    assert!(
        !serde_json::to_string(&sibling[0])
            .unwrap()
            .contains("original-a")
    );
    // A fork starts with the materialized Context and its own root, no old archive.
    let (fork, _) = CreatorJournal::open("unused-memory", "cache-fork".into(), false, false)
        .await
        .unwrap();
    fork.commit(compacted(1, "summary", &marker)).await.unwrap();
    assert!(
        fork.compacted_history(Some(&marker))
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn file_request_heads_retain_compaction_epochs_and_exclude_siblings() {
    let root = directory();
    let journal = FileJournal::open(&root).await.unwrap();
    let mut first = envelope(0);
    first.request_head = Some("a".repeat(64));
    let a = journal.commit(None, first).await.unwrap();
    let mut second = envelope(1);
    second.request_head = Some("b".repeat(64));
    let b = journal.commit(Some(a.clone()), second).await.unwrap();
    journal
        .move_head(b.clone(), a.clone(), Travel::Undo)
        .await
        .unwrap();
    let c = journal.commit(Some(a.clone()), envelope(2)).await.unwrap();
    let witnesses = FileJournal::inspect_request_heads(
        &root,
        "0123456789abcdef".into(),
        c.checkpoint_id.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        witnesses,
        vec![
            (c.checkpoint_id.clone(), None),
            (a.checkpoint_id.clone(), Some("a".repeat(64)))
        ]
    );
    assert_eq!(
        FileJournal::inspect_request_heads(
            &root,
            "0123456789abcdef".into(),
            b.checkpoint_id.clone()
        )
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
        FileJournal::inspect_request_heads(&root, "foreign".into(), c.checkpoint_id)
            .await
            .is_err()
    );
    assert!(
        FileJournal::inspect_request_heads(&root, "0123456789abcdef".into(), "f".repeat(64))
            .await
            .is_err()
    );
}

fn directory() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "microcrab-creator-journal-{}",
        uuid::Uuid::new_v4()
    ))
}
fn envelope(epoch: u64) -> CreatorEnvelope {
    CreatorEnvelope {
        session_id: "0123456789abcdef".into(),
        recover: recover(epoch, "checkpoint"),
        request_head: None,
        model_selection: None,
        route_witness: None,
        navigation_boundary: None,
    }
}
fn turn_checkpoint(turn: u64, label: &str) -> CreatorEnvelope {
    let mut checkpoint = envelope(0);
    checkpoint.recover = recover(0, label);
    // SessionState's turn position is intentionally private to the core runtime.
    let mut state = serde_json::to_value(&checkpoint.recover.state).unwrap();
    state["last_turn_id"] = json!(turn);
    checkpoint.recover.state = serde_json::from_value(state).unwrap();
    checkpoint
}

#[test]
fn legacy_limits_keep_original_checkpoint_hash_and_new_limits_serialize_current_names() {
    let mut original = serde_json::to_value(HistoryNode::new(None, envelope(0))).unwrap();
    let limits = original["checkpoint"]["recover"]["state"]["limits"]
        .as_object_mut()
        .unwrap();
    for (current, legacy) in [
        ("max_pending_executions", "max_tool_tasks"),
        ("max_retained_executions", "max_closed_tool_tasks"),
        ("max_retained_processes", "max_closed_jobs"),
        ("output_buffer_bytes", "job_output_bytes"),
    ] {
        let value = limits.remove(current).unwrap();
        limits.insert(legacy.into(), value);
    }
    limits.remove("max_processes");
    limits.remove("max_execution_buffers");
    limits.insert("max_jobs".into(), json!(7));
    // A historically omitted default must remain omitted in the immutable node.
    limits.remove("interaction_timeout_ms");
    let original_id = content_hash(&original).unwrap();
    let original_bytes = canonical_bytes(&original).unwrap();
    let mut decoded: HistoryNode = serde_json::from_slice(&original_bytes).unwrap();
    decoded.validate(&original_id).unwrap();
    assert_eq!(decoded.id().unwrap(), original_id);
    assert_eq!(canonical_bytes(&decoded).unwrap(), original_bytes);
    assert_eq!(decoded.checkpoint.recover.state.limits.max_processes, 7);
    assert_eq!(
        decoded
            .checkpoint
            .recover
            .state
            .limits
            .max_execution_buffers,
        7
    );

    let standalone = serde_json::to_value(&decoded.checkpoint.recover.state.limits).unwrap();
    assert_eq!(standalone["max_processes"], 7);
    assert_eq!(standalone["max_execution_buffers"], 7);
    assert!(standalone.get("max_jobs").is_none());
    let fresh = SessionState::new(decoded.checkpoint.recover.state.limits.clone());
    assert!(
        serde_json::to_value(fresh).unwrap()["limits"]
            .get("max_jobs")
            .is_none()
    );

    decoded
        .checkpoint
        .recover
        .state
        .limits
        .max_execution_buffers = 3;
    let changed = serde_json::to_value(&decoded).unwrap();
    let changed_limits = &changed["checkpoint"]["recover"]["state"]["limits"];
    assert_eq!(changed_limits["max_processes"], 7);
    assert_eq!(changed_limits["max_execution_buffers"], 3);
    assert!(changed_limits.get("max_jobs").is_none());
    assert_ne!(decoded.id().unwrap(), original_id);
}

#[test]
fn legacy_background_task_keeps_original_checkpoint_hash_on_decode() {
    // Hash the original JSON before typed decoding, as a v0.1.8 writer did.
    // A serde alias that rewrites background to foreground_expired breaks this.
    let mut original = serde_json::to_value(HistoryNode::new(None, envelope(0))).unwrap();
    original["checkpoint"]["recover"]["state"]["tool_tasks"] = json!([{
        "task_id": 1,
        "session_id": "0123456789abcdef",
        "call_id": "old-invocation",
        "tool": "slow",
        "agent": "fixture",
        "mode": null,
        "status": "background"
    }]);
    let original_id = content_hash(&original).unwrap();
    let original_bytes = canonical_bytes(&original).unwrap();
    let decoded: HistoryNode = serde_json::from_slice(&original_bytes).unwrap();
    decoded.validate(&original_id).unwrap();
    assert_eq!(decoded.id().unwrap(), original_id);
    assert_eq!(canonical_bytes(&decoded).unwrap(), original_bytes);
    let roundtrip = serde_json::to_value(&decoded).unwrap();
    assert_eq!(
        roundtrip["checkpoint"]["recover"]["state"]["tool_tasks"][0]["status"],
        "background"
    );
    assert!(decoded.checkpoint.recover.state.executions.is_empty());
}

#[tokio::test]
async fn availability_reports_real_root_and_redo_without_staging_or_moving_head() {
    let (journal, _) =
        CreatorJournal::open("unused-memory", "0123456789abcdef".into(), false, false)
            .await
            .unwrap();
    assert_eq!(
        journal.history_available().await.unwrap(),
        HistoryAvailable {
            undo: false,
            redo: false
        }
    );
    journal.commit(recover(0, "root")).await.unwrap();
    let root = journal.cursor().await;
    assert_eq!(
        journal.history_available().await.unwrap(),
        HistoryAvailable {
            undo: false,
            redo: false
        }
    );
    journal.commit(recover(1, "next")).await.unwrap();
    let next = journal.cursor().await;
    assert_eq!(
        journal.history_available().await.unwrap(),
        HistoryAvailable {
            undo: true,
            redo: false
        }
    );
    assert_eq!(journal.cursor().await, next);
    let selected = journal.prepare_travel(Travel::Undo).await.unwrap();
    journal.recovered(selected).await.unwrap();
    assert_eq!(journal.cursor().await, root);
    assert_eq!(
        journal.history_available().await.unwrap(),
        HistoryAvailable {
            undo: false,
            redo: true
        }
    );
    // Identical commit preserves the actual redo path and its cached availability.
    let unchanged = journal.head_recover().await.unwrap().unwrap();
    journal.commit(unchanged).await.unwrap();
    assert_eq!(journal.cursor().await, root);
    assert_eq!(
        journal.history_available().await.unwrap(),
        HistoryAvailable {
            undo: false,
            redo: true
        }
    );
    // To(current) keeps the same cursor but clears redo, so cursor-only cache keys fail.
    let selected = journal
        .prepare_travel(Travel::To {
            checkpoint_id: root.as_ref().unwrap().checkpoint_id.clone(),
        })
        .await
        .unwrap();
    journal.recovered(selected).await.unwrap();
    assert_eq!(journal.cursor().await, root);
    assert_eq!(
        journal.history_available().await.unwrap(),
        HistoryAvailable {
            undo: false,
            redo: false
        }
    );
    // Availability did not stage a travel transaction; a branch can still commit.
    journal.commit(recover(2, "branch")).await.unwrap();
    assert_eq!(
        journal.history_available().await.unwrap(),
        HistoryAvailable {
            undo: true,
            redo: false
        }
    );
    journal.close().await.unwrap();
}

#[tokio::test]
async fn staged_travel_only_recovered_publishes_exact_head_and_request_anchor() {
    let (journal, _) =
        CreatorJournal::open("unused-memory", "0123456789abcdef".into(), false, false)
            .await
            .unwrap();
    journal.commit(recover(0, "first")).await.unwrap();
    let first = journal.cursor().await.unwrap();
    journal.history_anchor().lock().unwrap().request_head = Some("a".repeat(64));
    journal.commit(recover(1, "second")).await.unwrap();
    let second = journal.cursor().await.unwrap();
    let target = journal.prepare_travel(Travel::Undo).await.unwrap();
    assert_eq!(target.state.epoch_id, 0);
    assert_eq!(journal.cursor().await.unwrap(), second);
    assert!(matches!(
        journal.commit(recover(2, "blocked")).await,
        Err(Error::Busy)
    ));
    assert!(matches!(
        journal.prepare_travel(Travel::Undo).await,
        Err(Error::Busy)
    ));
    journal.cancel_pending().await;
    assert_eq!(journal.cursor().await.unwrap(), second);
    let mut wrong_target = journal.prepare_travel(Travel::Undo).await.unwrap();
    wrong_target.context.system = "different valid system".into();
    assert!(journal.recovered(wrong_target).await.is_err());
    assert_eq!(journal.cursor().await.unwrap(), second);
    let target = journal.prepare_travel(Travel::Undo).await.unwrap();
    journal.recovered(target).await.unwrap();
    assert_eq!(journal.cursor().await.unwrap(), first);
    assert_eq!(journal.history_anchor().lock().unwrap().request_head, None);
    let target = journal.prepare_travel(Travel::Redo).await.unwrap();
    journal.recovered(target).await.unwrap();
    assert_eq!(journal.cursor().await.unwrap(), second);
    assert_eq!(
        journal.history_anchor().lock().unwrap().request_head,
        Some("a".repeat(64))
    );
}

#[tokio::test]
async fn disk_cas_lock_close_schema_and_lazy_node_loading() {
    let directory = directory();
    let journal = FileJournal::open(&directory).await.unwrap();
    let first = journal.commit(None, envelope(0)).await.unwrap();
    assert!(journal.commit(None, envelope(0)).await.is_err());
    let second = journal
        .commit(Some(first.clone()), envelope(1))
        .await
        .unwrap();
    assert!(FileJournal::open(&directory).await.is_err());
    journal.close().await.unwrap();
    assert!(journal.restore(RestoreTarget::Head).await.is_err());
    // Unselected branch corruption remains lazy; selecting it verifies its body.
    let old_path = directory
        .join("journal/nodes")
        .join(format!("{}.json", first.checkpoint_id));
    std::fs::write(&old_path, b"invalid\n").unwrap();
    let reopened = FileJournal::open(&directory).await.unwrap();
    assert_eq!(
        reopened.restore(RestoreTarget::Head).await.unwrap().cursor,
        second
    );
    assert!(
        reopened
            .restore(RestoreTarget::Checkpoint {
                checkpoint_id: first.checkpoint_id
            })
            .await
            .is_err()
    );
    reopened.close().await.unwrap();
    let manifest_path = directory.join("journal/manifest.json");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    manifest["version"] = json!(3);
    let legacy = serde_json::to_vec(&manifest).unwrap();
    std::fs::write(&manifest_path, &legacy).unwrap();
    assert!(FileJournal::open(&directory).await.is_err());
    assert_eq!(std::fs::read(&manifest_path).unwrap(), legacy);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn disk_branch_hashes_retain_both_turn_two_nodes_and_exact_reopen_position() {
    let directory = directory();
    let journal = FileJournal::open(&directory).await.unwrap();
    let a = journal.commit(None, turn_checkpoint(1, "A")).await.unwrap();
    let b = journal
        .commit(Some(a.clone()), turn_checkpoint(2, "B"))
        .await
        .unwrap();
    valid_hash(&a.checkpoint_id).unwrap();
    valid_hash(&b.checkpoint_id).unwrap();
    let target = journal.travel(b.clone(), Travel::Undo).await.unwrap();
    let selected_a = journal
        .move_head(b.clone(), target.cursor, Travel::Undo)
        .await
        .unwrap();
    assert_eq!(selected_a, a);
    let target = journal.travel(a.clone(), Travel::Redo).await.unwrap();
    assert_eq!(target.cursor, b);
    let selected_b = journal
        .move_head(a.clone(), target.cursor, Travel::Redo)
        .await
        .unwrap();
    let target = journal
        .travel(selected_b.clone(), Travel::Undo)
        .await
        .unwrap();
    let selected_a = journal
        .move_head(selected_b, target.cursor, Travel::Undo)
        .await
        .unwrap();
    // A repeated checkpoint is a no-op and preserves the selected redo path.
    assert_eq!(
        journal
            .commit(Some(selected_a.clone()), turn_checkpoint(1, "A"))
            .await
            .unwrap(),
        a
    );
    assert!(journal.travel(a.clone(), Travel::Redo).await.is_ok());
    let c = journal
        .commit(Some(selected_a), turn_checkpoint(2, "C"))
        .await
        .unwrap();
    assert_ne!(b.checkpoint_id, c.checkpoint_id);
    assert!(journal.travel(c.clone(), Travel::Redo).await.is_err());
    for node in [&b, &c] {
        let path = directory
            .join("journal/nodes")
            .join(format!("{}.json", node.checkpoint_id));
        let payload: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(payload["parent"], a.checkpoint_id);
        assert_eq!(payload["checkpoint"]["recover"]["state"]["last_turn_id"], 2);
    }
    journal.close().await.unwrap();
    let reopened = FileJournal::open(&directory).await.unwrap();
    let point = reopened.restore(RestoreTarget::Head).await.unwrap();
    assert_eq!(point.cursor, c);
    assert_eq!(point.checkpoint.recover.state.epoch_id, 0);
    assert!(!point.checkpoint.recover.context.pending_epoch);
    assert_eq!(
        serde_json::to_value(point.checkpoint.recover.state).unwrap()["last_turn_id"],
        2
    );
    assert!(
        reopened
            .restore(RestoreTarget::Checkpoint {
                checkpoint_id: b.checkpoint_id
            })
            .await
            .is_ok()
    );
    reopened.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn disk_rejects_body_tampering_and_incomplete_files_without_changing_head() {
    let directory = directory();
    let journal = FileJournal::open(&directory).await.unwrap();
    let first = journal.commit(None, envelope(0)).await.unwrap();
    let manifest_path = directory.join("journal/manifest.json");
    let manifest = std::fs::read(&manifest_path).unwrap();
    journal.close().await.unwrap();
    let path = directory
        .join("journal/nodes")
        .join(format!("{}.json", first.checkpoint_id));
    let original = std::fs::read(&path).unwrap();
    let mut node: serde_json::Value = serde_json::from_slice(&original).unwrap();
    node["checkpoint"]["recover"]["context"]["system"] = json!("tampered");
    std::fs::write(&path, serde_json::to_vec(&node).unwrap()).unwrap();
    assert!(FileJournal::open(&directory).await.is_err());
    assert_eq!(std::fs::read(&manifest_path).unwrap(), manifest);
    std::fs::write(&path, &original[..original.len() / 2]).unwrap();
    assert!(FileJournal::open(&directory).await.is_err());
    assert_eq!(std::fs::read(&manifest_path).unwrap(), manifest);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn disk_checkpoint_encoding_is_bounded_before_any_head_publication() {
    let directory = directory();
    let journal = FileJournal::open_with_options(
        &directory,
        JournalOptions {
            max_checkpoint_bytes: 128,
        },
    )
    .await
    .unwrap();
    assert!(journal.commit(None, envelope(0)).await.is_err());
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(directory.join("journal/manifest.json")).unwrap())
            .unwrap();
    assert!(manifest["head"].is_null());
    assert_eq!(
        std::fs::read_dir(directory.join("journal/nodes"))
            .unwrap()
            .count(),
        0
    );
    journal.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn failed_manifest_publication_quarantines_writer_and_preserves_old_selection() {
    let directory = directory();
    let journal = FileJournal::open(&directory).await.unwrap();
    let first = journal.commit(None, envelope(0)).await.unwrap();
    let manifest = directory.join("journal/manifest.json");
    let saved = directory.join("journal/saved-manifest.json");
    std::fs::rename(&manifest, &saved).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    assert!(
        journal
            .commit(Some(first.clone()), envelope(1))
            .await
            .is_err()
    );
    assert_eq!(
        journal.restore(RestoreTarget::Head).await.unwrap().cursor,
        first
    );
    assert!(
        journal
            .commit(Some(first.clone()), envelope(2))
            .await
            .is_err()
    );
    journal.close().await.unwrap();
    std::fs::remove_dir(&manifest).unwrap();
    std::fs::rename(&saved, &manifest).unwrap();
    let reopened = FileJournal::open(&directory).await.unwrap();
    assert_eq!(
        reopened.restore(RestoreTarget::Head).await.unwrap().cursor,
        first
    );
    // The durable but unpublished node can be reused without overwriting it.
    let next = reopened.commit(Some(first), envelope(1)).await.unwrap();
    assert_eq!(
        reopened.restore(RestoreTarget::Head).await.unwrap().cursor,
        next
    );
    reopened.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn canonical_hash_ignores_object_insertion_order_and_validates_full_identifiers() {
    assert_eq!(
        content_hash(&json!({"b": [1, 2], "a": {"z": 3, "y": 4}})).unwrap(),
        content_hash(&json!({"a": {"y": 4, "z": 3}, "b": [1, 2]})).unwrap()
    );
    assert_ne!(
        content_hash(&json!([1, 2])).unwrap(),
        content_hash(&json!([2, 1])).unwrap()
    );
    assert!(valid_hash(&uuid::Uuid::new_v4().to_string()).is_err());
    assert!(valid_hash(&"a".repeat(63)).is_err());
    assert!(valid_hash(&"A".repeat(64)).is_err());
    valid_hash(&"a".repeat(64)).unwrap();
}

#[tokio::test]
async fn identical_neutral_turns_on_different_routes_have_distinct_durable_checkpoint_identity() {
    let root = directory();
    let (journal, _) =
        CreatorJournal::open(root.to_str().unwrap(), "route-owner".into(), false, false)
            .await
            .unwrap();
    let mut initial = envelope(0).recover;
    initial.state.epoch_id = 0;
    let route = |name: &str| crate::directory::ModelSelection {
        provider_id: name.into(),
        model: "same-model-name".into(),
        variant: None,
    };
    journal
        .prepare_model_route(route("A"), Some("a".repeat(64)))
        .await;
    Journal::commit(&journal, initial.clone()).await.unwrap();
    let original = journal.cursor().await.unwrap();
    let mut same_body = initial.clone();
    same_body.context.epoch = 1;
    same_body.state.epoch_id = 1;
    journal
        .prepare_model_route(route("B"), Some("b".repeat(64)))
        .await;
    Journal::commit(&journal, same_body.clone()).await.unwrap();
    let b = journal.cursor().await.unwrap();
    let restored = journal
        .prepare_travel(Travel::To {
            checkpoint_id: original.checkpoint_id.clone(),
        })
        .await
        .unwrap();
    Journal::recovered(&journal, restored).await.unwrap();
    assert_eq!(journal.model_selection().await, Some(route("A")));
    journal
        .prepare_model_route(route("C"), Some("c".repeat(64)))
        .await;
    Journal::commit(&journal, same_body.clone()).await.unwrap();
    let c = journal.cursor().await.unwrap();
    assert_ne!(
        b.checkpoint_id, c.checkpoint_id,
        "route is part of the immutable envelope before any provider call"
    );
    assert_eq!(b.epoch, c.epoch);
    let selected_b = journal
        .prepare_travel(Travel::To {
            checkpoint_id: b.checkpoint_id.clone(),
        })
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(selected_b.clone()).unwrap(),
        serde_json::to_value(&same_body).unwrap()
    );
    assert_eq!(
        journal.restore_model_route().await.unwrap(),
        (b, Some(route("B")), Some("b".repeat(64)))
    );
    Journal::recovered(&journal, selected_b).await.unwrap();
    assert_eq!(journal.model_selection().await, Some(route("B")));
}

#[test]
fn absent_creator_route_roundtrips_identical_historical_envelope_bytes() {
    let legacy = envelope(0);
    let old = serde_json::to_vec(&legacy).unwrap();
    assert!(
        !serde_json::to_value(&legacy)
            .unwrap()
            .as_object()
            .unwrap()
            .contains_key("model_selection")
    );
    let restored: CreatorEnvelope = serde_json::from_slice(&old).unwrap();
    assert!(restored.model_selection.is_none());
    assert_eq!(serde_json::to_vec(&restored).unwrap(), old);
}

fn admission_context() -> Context {
    use microcrab_session::protocol::{Response, ToolCall, ToolDefinition};
    let mut context = Context::new(
        "system".into(),
        vec![ToolDefinition {
            name: "inspect".into(),
            description: "inspect".into(),
            parameters: json!({"type":"object"}),
        }],
        Limits::default().max_context_bytes,
    )
    .unwrap();
    context.admit_user("inspect siblings", &json!({})).unwrap();
    context
        .append_response(&Response {
            content: "".into(),
            usage: Default::default(),
            provider_metadata: None,
            tool_calls: ["b", "a"]
                .into_iter()
                .map(|id| ToolCall {
                    id: id.into(),
                    name: "inspect".into(),
                    arguments: json!({"id":id}),
                    provider_metadata: None,
                })
                .collect(),
        })
        .unwrap();
    context
}

fn admission_recover(
    context: &Context,
    receipts: &[(&str, &str)],
) -> (Recover, AdmissionCheckpoint) {
    let proof = AdmissionCheckpoint::from_context(context).unwrap();
    let mut completed = context.clone();
    for (id, text) in receipts {
        completed
            .append_tool_result((*id).into(), (*text).into())
            .unwrap();
    }
    let mut value = recover(0, "unused");
    value.context = completed.snapshot().unwrap();
    (value, proof)
}

#[tokio::test]
async fn admission_display_prefix_survives_receipts_and_partial_sibling_settlement() {
    let (journal, _) = CreatorJournal::open("unused", "admission-display".into(), false, false)
        .await
        .unwrap();
    let context = admission_context();
    let (first, proof) = admission_recover(&context, &[("a", "waiting"), ("b", "not executed")]);
    assert_eq!(
        proof.admitted_prefix_hash,
        content_hash(&context.items()).unwrap()
    );
    journal
        .commit_admission(first.clone(), proof.clone())
        .await
        .unwrap();
    let start = journal.display_witness().await;
    let (_, projected) = journal
        .display_projection(start.cursor.as_ref().unwrap())
        .await
        .unwrap();
    assert_eq!(projected, Some(proof.clone()));
    let (changed, proof) = admission_recover(
        &context,
        &[("a", "different receipt"), ("b", "denied receipt")],
    );
    journal.commit_admission(changed, proof).await.unwrap();
    assert_eq!(journal.display_witness().await.lineage, start.lineage);

    let mut partial = context.clone();
    // A real tool may legitimately return exactly the receipt-shaped JSON. It
    // becomes immutable immediately, before the other sibling has settled.
    let mimic =
        r#"{"status":"waiting","error":"not executed; interrupted before invocation admission"}"#;
    partial
        .append_tool_result("a".into(), mimic.into())
        .unwrap();
    let (checkpoint, proof) = admission_recover(&partial, &[("b", "interrupted")]);
    journal
        .commit_admission(checkpoint.clone(), proof.clone())
        .await
        .unwrap();
    assert_eq!(journal.display_witness().await.lineage, start.lineage);
    assert_eq!(proof.admitted_prefix_len, context.items().len() + 1);

    let before = journal.cursor().await.unwrap();
    let (weaker, old_proof) = admission_recover(&context, &[("a", "waiting"), ("b", "waiting")]);
    let error = journal
        .commit_admission(weaker, old_proof)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("retracts previously certified"));
    assert_eq!(journal.cursor().await, Some(before.clone()));
    let mut malformed = proof.clone();
    malformed.pending[0].call_index = 99;
    assert!(
        journal
            .commit_admission(checkpoint.clone(), malformed)
            .await
            .is_err()
    );
    assert_eq!(journal.cursor().await, Some(before));

    let mut settled = partial;
    settled
        .append_tool_result("b".into(), "actual final result".into())
        .unwrap();
    let mut final_recover = first;
    final_recover.context = settled.snapshot().unwrap();
    journal.commit(final_recover.clone()).await.unwrap();
    assert_eq!(journal.display_witness().await.lineage, start.lineage);
    let head = journal.cursor().await.unwrap();
    assert!(journal.display_projection(&head).await.unwrap().1.is_none());
    // Once certified, even receipt-looking real content cannot change silently.
    if let microcrab_session::context::ContentItem::Tool { content, .. } =
        &mut final_recover.context.items[context.items().len()]
    {
        *content = "rewritten real result".into();
    } else {
        panic!("expected real sibling result");
    }
    journal.commit(final_recover).await.unwrap();
    assert_ne!(journal.display_witness().await.lineage, start.lineage);
    assert!(journal.display_projection(&head).await.is_none());
}

#[tokio::test]
async fn admission_proof_rejects_wrong_suffix_and_is_not_retrofitted_on_restore() {
    let root = directory();
    let path = root.to_str().unwrap();
    let (journal, _) = CreatorJournal::open(path, "admission-restore".into(), true, false)
        .await
        .unwrap();
    let context = admission_context();
    let (checkpoint, proof) = admission_recover(&context, &[("a", "receipt"), ("b", "receipt")]);
    let mut malformed = checkpoint.clone();
    malformed
        .context
        .items
        .swap(proof.admitted_prefix_len, proof.admitted_prefix_len + 1);
    assert!(
        journal
            .commit_admission(malformed, proof.clone())
            .await
            .is_err()
    );
    assert!(journal.cursor().await.is_none());
    let mut malformed = proof.clone();
    malformed.admitted_prefix_hash = "0".repeat(64);
    assert!(
        journal
            .commit_admission(checkpoint.clone(), malformed)
            .await
            .is_err()
    );
    assert!(journal.cursor().await.is_none());
    journal
        .commit_admission(checkpoint.clone(), proof.clone())
        .await
        .unwrap();
    let head = journal.cursor().await.unwrap();
    let bytes = canonical_bytes(
        &journal
            .backend
            .restore(RestoreTarget::Head)
            .await
            .unwrap()
            .checkpoint,
    )
    .unwrap();
    let before = journal.display_witness().await;
    let selected = journal
        .prepare_travel(Travel::To {
            checkpoint_id: head.checkpoint_id.clone(),
        })
        .await
        .unwrap();
    journal.recovered(selected).await.unwrap();
    assert!(journal.display_projection(&head).await.unwrap().1.is_none());
    assert_ne!(journal.display_witness().await.lineage, before.lineage);
    journal.close().await.unwrap();
    let (reopened, _) = CreatorJournal::open(path, "admission-restore".into(), true, true)
        .await
        .unwrap();
    assert_eq!(
        canonical_bytes(
            &reopened
                .backend
                .restore(RestoreTarget::Head)
                .await
                .unwrap()
                .checkpoint
        )
        .unwrap(),
        bytes
    );
    assert!(
        reopened
            .display_projection(&head)
            .await
            .unwrap()
            .1
            .is_none()
    );
    assert_ne!(reopened.display_witness().await.lineage, before.lineage);
    // Stored receipts without runtime provenance are conservative full-prefix
    // evidence; reopening cannot silently downgrade them to a synthetic suffix.
    assert!(
        reopened
            .commit_admission(checkpoint.clone(), proof)
            .await
            .is_err()
    );
    assert_eq!(reopened.cursor().await, Some(head.clone()));
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn unwitnessed_admission_is_conservative_and_baseline_change_resets() {
    let (journal, _) = CreatorJournal::open("unused", "unwitnessed-admission".into(), false, false)
        .await
        .unwrap();
    let context = admission_context();
    let (first, _) = admission_recover(
        &context,
        &[("a", r#"{"status":"waiting"}"#), ("b", "receipt")],
    );
    journal.commit(first).await.unwrap();
    let original = journal.display_witness().await.lineage;
    let (changed, _) = admission_recover(&context, &[("a", "real result"), ("b", "receipt")]);
    journal.commit(changed).await.unwrap();
    assert_ne!(journal.display_witness().await.lineage, original);
    let mut next = recover(0, "new baseline");
    journal.commit(next.clone()).await.unwrap();
    let before = journal.display_witness().await.lineage;
    next.context.baseline_generation += 1;
    journal.commit(next).await.unwrap();
    assert_ne!(journal.display_witness().await.lineage, before);
}
