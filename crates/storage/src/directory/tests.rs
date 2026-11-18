use super::*;
#[path = "tests/owner_lookup.rs"]
mod owner_lookup;
use std::{path::PathBuf, sync::Arc};

fn root() -> PathBuf {
    std::env::temp_dir().join(format!("microcrab-directory-{}", uuid::Uuid::new_v4()))
}
fn session() -> SessionRecord {
    SessionRecord {
        version: RECORD_VERSION,
        id: "session-1".into(),
        owner: "owner-1".into(),
        title: "initial".into(),
        project_id: Some("project-1".into()),
        project_root: "workspace".into(),
        agent: Some("coding".into()),
        mode: Some("plan".into()),
        model_selection: None,
        auto_compaction: None,
        prompt_config: None,
        permissions: vec![],
        initialization: microcrab_permission::Action::Ask,
        review: true,
        review_model: None,
        revision: 0,
        archived: false,
        created_at_ms: 1,
        updated_at_ms: 1,
    }
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn incompatible_file_record_is_explicit_and_survives_healthy_writes() {
    let root = root();
    std::fs::create_dir_all(&root).unwrap();
    let mut legacy = serde_json::to_value(session()).unwrap();
    legacy["id"] = serde_json::json!("legacy");
    legacy["permissions"] = serde_json::json!([{
        "capability": "file_write", "action": "deny", "resource": null
    }]);
    let raw_legacy = serde_json::to_string_pretty(&legacy).unwrap();
    let mut old_healthy = serde_json::to_value(session()).unwrap();
    old_healthy
        .as_object_mut()
        .unwrap()
        .remove("initialization");
    old_healthy
        .as_object_mut()
        .unwrap()
        .remove("model_selection");
    let raw_healthy = serde_json::to_string_pretty(&old_healthy).unwrap();
    let source = format!(
        "{{\"version\":1,\"sessions\":{{\"unowned\":null,\"legacy\":{raw_legacy},\"session-1\":{raw_healthy}}},\"operations\":{{\"bad-receipt\":{{\"owner\":\"owner-2\"}}}}}}"
    );
    std::fs::write(root.join("directory.json"), &source).unwrap();
    let directory = FileDirectory::open(&root).await.unwrap();
    assert_eq!(
        std::fs::read_to_string(root.join("directory.json")).unwrap(),
        source
    );
    assert_eq!(directory.unavailable_unowned_count().await.unwrap(), 1);
    let entries = directory.list_entries("owner-1").await.unwrap();
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().any(
        |entry| matches!(entry, SessionEntry::Unavailable { id, reason }
        if id == "legacy" && reason.contains("capability"))
    ));
    assert!(
        entries
            .iter()
            .any(|entry| matches!(entry, SessionEntry::Available { record }
        if record.id == "session-1"))
    );
    assert!(directory.list("owner-1").await.is_err());
    assert!(directory.list_entries("owner-2").await.unwrap().is_empty());
    assert!(directory.get("owner-1", "legacy").await.is_err());
    assert!(directory.get("owner-2", "legacy").await.unwrap().is_none());
    let mut healthy = session();
    healthy.id = "healthy-2".into();
    directory.create(healthy).await.unwrap();
    let published = std::fs::read_to_string(root.join("directory.json")).unwrap();
    assert!(
        published.contains(&raw_legacy),
        "unavailable record retains exact source bytes"
    );
    assert!(published.contains(&raw_healthy));
    assert!(published.contains("\"bad-receipt\":{\"owner\":\"owner-2\"}"));
    directory.close().await.unwrap();
    let reopened = FileDirectory::open(&root).await.unwrap();
    assert_eq!(reopened.list_entries("owner-1").await.unwrap().len(), 3);
    reopened.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_remove_cleans_exact_owned_receipts_and_preserves_unattributed_raw_bytes() {
    let root = root();
    std::fs::create_dir_all(&root).unwrap();
    let other_session = SessionRecord {
        id: "session-2".into(),
        ..session()
    };
    let owned = r#"{"owner":"owner-1","session_id":"session-1","incompatible":true}"#;
    let preserved = [
        (
            "other-owner",
            r#"{ "owner" : "owner-2", "session_id" : "session-1" }"#,
        ),
        ("unowned", r#"{ "session_id" : "session-1" }"#),
        (
            "other-session",
            r#"{ "owner" : "owner-1", "session_id" : "session-2" }"#,
        ),
        ("missing-session", r#"{ "owner" : "owner-1" }"#),
    ];
    let retained = preserved
        .iter()
        .map(|(id, raw)| format!("\"{id}\":{raw}"))
        .collect::<Vec<_>>()
        .join(",");
    let source = format!(
        "{{\"version\":1,\"sessions\":{{\"session-1\":{},\"session-2\":{}}},\"operations\":{{\"operation-1\":{},\"owned-bad\":{owned},{retained}}}}}",
        serde_json::to_string(&session()).unwrap(),
        serde_json::to_string(&other_session).unwrap(),
        serde_json::to_string(&receipt()).unwrap(),
    );
    std::fs::write(root.join("directory.json"), &source).unwrap();
    let directory = FileDirectory::open(&root).await.unwrap();
    assert!(
        directory
            .list_operations("owner-1", "session-1")
            .await
            .is_err()
    );
    assert!(directory.remove("owner-2", "session-1", 0).await.is_err());
    assert!(directory.remove("owner-1", "session-1", 1).await.is_err());
    assert_eq!(
        std::fs::read_to_string(root.join("directory.json")).unwrap(),
        source,
        "a rejected revision publishes neither Session nor receipt removal"
    );
    assert_eq!(
        directory.remove("owner-1", "session-1", 0).await.unwrap(),
        2
    );
    assert!(
        directory
            .get("owner-1", "session-1")
            .await
            .unwrap()
            .is_none()
    );
    for id in ["operation-1", "owned-bad"] {
        assert!(
            directory
                .get_operation("owner-1", id)
                .await
                .unwrap()
                .is_none()
        );
    }
    assert!(
        directory
            .list_operations("owner-1", "session-1")
            .await
            .unwrap()
            .is_empty()
    );
    let published = std::fs::read_to_string(root.join("directory.json")).unwrap();
    let value: serde_json::Value = serde_json::from_str(&published).unwrap();
    assert!(value["sessions"].get("session-1").is_none());
    assert!(value["operations"].get("operation-1").is_none());
    assert!(value["operations"].get("owned-bad").is_none());
    for (id, raw) in &preserved {
        assert!(value["operations"].get(*id).is_some());
        assert!(
            published.contains(*raw),
            "retained receipt {id} must keep its exact bytes"
        );
    }
    directory.close().await.unwrap();
    let reopened = FileDirectory::open(&root).await.unwrap();
    assert!(
        reopened
            .list_operations("owner-1", "session-1")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        reopened
            .list_operations("owner-1", "session-2")
            .await
            .is_err()
    );
    assert!(
        reopened
            .get_operation("owner-2", "other-owner")
            .await
            .is_err()
    );
    assert_eq!(reopened.unavailable_unowned_count().await.unwrap(), 1);
    reopened.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn incompatible_sqlite_record_isolated_from_healthy_records_and_other_owners() {
    let root = root();
    let path = root.join("storage.sqlite");
    let directory = SqliteDirectory::open(&path).await.unwrap();
    directory.create(session()).await.unwrap();
    let mut legacy = serde_json::to_value(session()).unwrap();
    legacy["id"] = serde_json::json!("legacy");
    legacy["permissions"] = serde_json::json!([{"capability":"file_write", "action":"deny"}]);
    let bytes = serde_json::to_vec(&legacy).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute("INSERT INTO directory_sessions(id,owner,revision,body) VALUES('legacy','owner-1',0,?1)", [&bytes]).unwrap();
    let entries = directory.list_entries("owner-1").await.unwrap();
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().any(
        |entry| matches!(entry, SessionEntry::Unavailable { id, reason }
        if id == "legacy" && reason.contains("capability"))
    ));
    assert!(directory.list("owner-1").await.is_err());
    assert!(directory.list_entries("owner-2").await.unwrap().is_empty());
    assert!(
        directory
            .get("owner-1", "session-1")
            .await
            .unwrap()
            .is_some()
    );
    let stored: Vec<u8> = connection
        .query_row(
            "SELECT body FROM directory_sessions WHERE id='legacy'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored, bytes);
    drop(connection);
    directory.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn legacy_permission_diagnostic_preserves_known_field_and_index_without_values() {
    for field in [
        "capability",
        "resource",
        "arguments",
        "operation_arguments",
        "conditions",
    ] {
        let mut value = serde_json::to_value(session()).unwrap();
        value["permissions"] = serde_json::json!([{"action": "allow"}]);
        value["permissions"][0][field] = serde_json::json!("private-policy-value");
        let error = decode_session(&serde_json::to_vec(&value).unwrap()).unwrap_err();
        let reason = error.diagnostic_reason();
        assert!(
            reason.contains("session.permissions rule index 0"),
            "{reason}"
        );
        assert!(reason.contains(field), "{reason}");
        assert!(!reason.contains("private-policy-value"), "{reason}");
    }
}

#[test]
fn legacy_initialization_defaults_to_ask() {
    let mut value = serde_json::to_value(session()).unwrap();
    value.as_object_mut().unwrap().remove("initialization");
    let decoded: SessionRecord = serde_json::from_value(value).unwrap();
    assert_eq!(decoded.initialization, microcrab_permission::Action::Ask);
}

#[test]
fn legacy_record_accepts_missing_selection_and_rejects_invalid_identifiers() {
    let mut legacy = serde_json::to_value(session()).unwrap();
    legacy.as_object_mut().unwrap().remove("model_selection");
    let decoded: SessionRecord = serde_json::from_value(legacy).unwrap();
    assert_eq!(decoded.model_selection, None);
    decoded.validate().unwrap();
    let mut invalid = decoded;
    invalid.model_selection = Some(ModelSelection {
        provider_id: String::new(),
        model: "m".into(),
        variant: None,
    });
    assert!(invalid.validate().is_err());
    invalid.model_selection.as_mut().unwrap().provider_id = "p".into();
    invalid.model_selection.as_mut().unwrap().variant = Some(String::new());
    assert!(invalid.validate().is_err());
}

fn receipt() -> OperationRecord {
    OperationRecord {
        version: RECORD_VERSION,
        id: "operation-1".into(),
        session_id: "session-1".into(),
        owner: "owner-1".into(),
        method: "turn.submit".into(),
        payload_hash: "a".repeat(64),
        status: "accepted".into(),
        result: None,
        revision: 0,
        created_at_ms: 2,
        updated_at_ms: 2,
    }
}
async fn contract(directory: Arc<dyn Directory>) {
    let first = directory.create(session()).await.unwrap();
    assert!(directory.create(session()).await.is_err());
    assert!(directory.get("owner-2", &first.id).await.unwrap().is_none());
    assert!(directory.list("owner-2").await.unwrap().is_empty());
    let mut next = first.clone();
    next.title = "renamed".into();
    next.model_selection = Some(ModelSelection {
        provider_id: "provider-b".into(),
        model: "model-b".into(),
        variant: Some("high".into()),
    });
    next.revision = 1;
    next.updated_at_ms = 3;
    assert!(directory.replace("owner-2", 0, next.clone()).await.is_err());
    let updated = directory.replace("owner-1", 0, next.clone()).await.unwrap();
    assert!(directory.replace("owner-1", 0, next).await.is_err());
    let mut illegal = updated.clone();
    illegal.revision += 1;
    illegal.project_root = "different root".into();
    assert!(directory.replace("owner-1", 1, illegal).await.is_err());
    assert_eq!(
        directory.get("owner-1", &first.id).await.unwrap(),
        Some(updated)
    );
    let mut invalid = receipt();
    invalid.owner = "owner-2".into();
    assert!(directory.create_operation(invalid).await.is_err());
    let created = directory.create_operation(receipt()).await.unwrap();
    assert!(directory.create_operation(receipt()).await.is_err());
    assert!(
        directory
            .get_operation("owner-2", &created.id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        directory
            .list_operations("owner-2", &created.session_id)
            .await
            .unwrap()
            .is_empty()
    );
    let mut done = created.clone();
    done.status = "completed".into();
    done.result = Some(serde_json::json!({"text":"answer"}));
    done.revision = 1;
    done.updated_at_ms = 4;
    let done = directory
        .replace_operation("owner-1", 0, done)
        .await
        .unwrap();
    let mut replaced = done.clone();
    replaced.revision = 2;
    replaced.result = None;
    assert!(
        directory
            .replace_operation("owner-1", 1, replaced)
            .await
            .is_err()
    );
    assert_eq!(
        directory
            .list_operations("owner-1", &first.id)
            .await
            .unwrap(),
        vec![done]
    );
    directory.close().await.unwrap();
    assert!(directory.get("owner-1", &first.id).await.is_err());
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_directory_owner_cas_receipts_and_reopen() {
    let root = root();
    let directory = Arc::new(FileDirectory::open(&root).await.unwrap());
    assert!(FileDirectory::open(&root).await.is_err());
    contract(directory.clone()).await;
    let reopened = FileDirectory::open(&root).await.unwrap();
    assert_eq!(
        reopened
            .get("owner-1", "session-1")
            .await
            .unwrap()
            .unwrap()
            .title,
        "renamed"
    );
    assert_eq!(
        reopened
            .get_operation("owner-1", "operation-1")
            .await
            .unwrap()
            .unwrap()
            .status,
        "completed"
    );
    reopened.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_directory_owner_cas_receipts_and_reopen() {
    let root = root();
    let database = root.join("storage.sqlite");
    contract(Arc::new(SqliteDirectory::open(&database).await.unwrap())).await;
    let reopened = SqliteDirectory::open(&database).await.unwrap();
    assert_eq!(
        reopened
            .get("owner-1", "session-1")
            .await
            .unwrap()
            .unwrap()
            .title,
        "renamed"
    );
    assert_eq!(
        reopened
            .get_operation("owner-1", "operation-1")
            .await
            .unwrap()
            .unwrap()
            .status,
        "completed"
    );
    reopened.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_directory_ids_never_become_paths_and_future_schema_is_preserved() {
    let root = root();
    let directory = FileDirectory::open(&root).await.unwrap();
    let mut record = session();
    record.id = "../../untrusted".into();
    directory.create(record).await.unwrap();
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 2); // lock and one manifest
    directory.close().await.unwrap();
    let path = root.join("directory.json");
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    value["version"] = serde_json::json!(99);
    let bytes = serde_json::to_vec(&value).unwrap();
    std::fs::write(&path, &bytes).unwrap();
    assert!(FileDirectory::open(&root).await.is_err());
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    std::fs::remove_dir_all(root).unwrap();
}

fn settlement(cursor: Option<crate::history::JournalCursor>) -> OperationSettlement {
    let mut next_session = session();
    next_session.revision = 1;
    next_session.updated_at_ms = 9;
    let mut next_operation = receipt();
    next_operation.status = "completed".into();
    next_operation.revision = 1;
    next_operation.updated_at_ms = 9;
    next_operation.result = Some(serde_json::json!({"content":"answer","journal_cursor":cursor}));
    OperationSettlement {
        expected_session_revision: 0,
        session: next_session,
        expected_operation_revision: 0,
        operation: next_operation,
        journal_cursor: cursor,
    }
}
async fn unchanged(directory: &dyn Directory) {
    assert_eq!(
        directory.get("owner-1", "session-1").await.unwrap(),
        Some(session())
    );
    assert_eq!(
        directory
            .get_operation("owner-1", "operation-1")
            .await
            .unwrap(),
        Some(receipt())
    );
}
async fn settlement_cas(directory: Arc<dyn Directory>) {
    directory.create(session()).await.unwrap();
    directory.create_operation(receipt()).await.unwrap();
    let mut invalid = settlement(None);
    invalid.expected_session_revision = 7;
    invalid.session.revision = 8;
    assert!(
        directory
            .settle_operation("owner-1", invalid)
            .await
            .is_err()
    );
    unchanged(directory.as_ref()).await;
    let mut invalid = settlement(None);
    invalid.expected_operation_revision = 7;
    invalid.operation.revision = 8;
    assert!(
        directory
            .settle_operation("owner-1", invalid)
            .await
            .is_err()
    );
    unchanged(directory.as_ref()).await;
    assert!(
        directory
            .settle_operation("owner-2", settlement(None))
            .await
            .is_err()
    );
    unchanged(directory.as_ref()).await;
    let expected = settlement(None);
    let actual = directory
        .settle_operation("owner-1", expected.clone())
        .await
        .unwrap();
    assert_eq!(
        actual,
        (expected.session.clone(), expected.operation.clone())
    );
    assert!(
        directory
            .settle_operation("owner-1", expected.clone())
            .await
            .is_err()
    );
    assert_eq!(
        directory.get("owner-1", "session-1").await.unwrap(),
        Some(expected.session)
    );
    assert_eq!(
        directory
            .get_operation("owner-1", "operation-1")
            .await
            .unwrap(),
        Some(expected.operation)
    );
    directory.close().await.unwrap();
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_settlement_atomically_updates_both_records_and_rejects_stale_cas() {
    let root = root();
    settlement_cas(Arc::new(FileDirectory::open(&root).await.unwrap())).await;
    let reopened = FileDirectory::open(&root).await.unwrap();
    assert_eq!(
        reopened.get("owner-1", "session-1").await.unwrap(),
        Some(settlement(None).session)
    );
    assert_eq!(
        reopened
            .get_operation("owner-1", "operation-1")
            .await
            .unwrap(),
        Some(settlement(None).operation)
    );
    reopened.close().await.unwrap();
}
#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_settlement_atomically_updates_both_records_and_rejects_stale_cas() {
    let database = root().join("storage.sqlite");
    settlement_cas(Arc::new(SqliteDirectory::open(&database).await.unwrap())).await;
    let reopened = SqliteDirectory::open(&database).await.unwrap();
    assert_eq!(
        reopened.get("owner-1", "session-1").await.unwrap(),
        Some(settlement(None).session)
    );
    assert_eq!(
        reopened
            .get_operation("owner-1", "operation-1")
            .await
            .unwrap(),
        Some(settlement(None).operation)
    );
    reopened.close().await.unwrap();
}
fn recover(epoch: u64, label: &str) -> microcrab_session::Recover {
    use microcrab_session::{Limits, SessionState, context::Context};
    let limits = Limits::default();
    let context = Context::new(label.into(), vec![], limits.max_context_bytes).unwrap();
    let mut snapshot = context.snapshot().unwrap();
    snapshot.epoch = epoch;
    let mut state = SessionState::new(limits);
    state.epoch_id = epoch;
    microcrab_session::Recover {
        state,
        context: snapshot,
        continuation: None,
        workspace: None,
        cache_report: None,
        agent: None,
        prompt_config: None,
    }
}
async fn settlement_head_fence(
    directory: Arc<dyn Directory>,
    journal: crate::history::CreatorJournal,
) {
    use microcrab_session::Journal;
    directory.create(session()).await.unwrap();
    directory.create_operation(receipt()).await.unwrap();
    journal.commit(recover(0, "first")).await.unwrap();
    let stale = journal.cursor().await.unwrap();
    journal.commit(recover(1, "second")).await.unwrap();
    let selected = journal.cursor().await.unwrap();
    assert_ne!(stale, selected);
    assert!(
        directory
            .settle_operation("owner-1", settlement(Some(stale)))
            .await
            .is_err()
    );
    unchanged(directory.as_ref()).await;
    let expected = settlement(Some(selected.clone()));
    let actual = directory
        .settle_operation("owner-1", expected.clone())
        .await
        .unwrap();
    assert_eq!(actual, (expected.session, expected.operation));
    assert_eq!(journal.cursor().await, Some(selected));
    journal.close().await.unwrap();
    directory.close().await.unwrap();
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_settlement_fences_actual_acknowledged_creator_history() {
    let root = root();
    let histories = root.join("sessions");
    let session_dir = histories.join("session-1");
    let directory = FileDirectory::open_with_history(root.join("directory"), &histories)
        .await
        .unwrap();
    let journal = crate::history::CreatorJournal::open(
        session_dir.to_str().unwrap(),
        "session-1".into(),
        true,
        false,
    )
    .await
    .unwrap()
    .0;
    settlement_head_fence(Arc::new(directory), journal).await;
}
#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_settlement_fences_actual_acknowledged_creator_history_in_same_database() {
    let database = root().join("storage.sqlite");
    let directory = SqliteDirectory::open(&database).await.unwrap();
    let journal = crate::history::CreatorJournal::open_sqlite(&database, "session-1".into(), false)
        .await
        .unwrap()
        .0;
    settlement_head_fence(Arc::new(directory), journal).await;
}
#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_receipt_cas_conflict_rolls_back_the_already_updated_session_row() {
    let database = root().join("storage.sqlite");
    let directory = SqliteDirectory::open(&database).await.unwrap();
    directory.create(session()).await.unwrap();
    directory.create_operation(receipt()).await.unwrap();
    let inspector = rusqlite::Connection::open(&database).unwrap();
    // Body revision0 passes validation; session UPDATE succeeds before the
    // deliberately conflicting indexed operation CAS forces a full rollback.
    inspector
        .execute(
            "UPDATE directory_operations SET revision=99 WHERE id='operation-1'",
            [],
        )
        .unwrap();
    assert!(
        directory
            .settle_operation("owner-1", settlement(None))
            .await
            .is_err()
    );
    unchanged(&directory).await;
    let revision: i64 = inspector
        .query_row(
            "SELECT revision FROM directory_sessions WHERE id='session-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(revision, 0);
    inspector
        .execute(
            "UPDATE directory_operations SET revision=0 WHERE id='operation-1'",
            [],
        )
        .unwrap();
    directory
        .settle_operation("owner-1", settlement(None))
        .await
        .unwrap();
    directory.close().await.unwrap();
}

#[test]
fn projectless_identity_is_explicit_and_legacy_project_strings_stay_readable() {
    let original = session();
    let frozen = serde_json::to_vec(&original).unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&frozen).unwrap();
    assert_eq!(value["project_id"], "project-1");
    let legacy: SessionRecord = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(legacy.project_id.as_deref(), Some("project-1"));
    value["project_id"] = serde_json::Value::Null;
    let mut projectless: SessionRecord = serde_json::from_value(value).unwrap();
    projectless.validate().unwrap();
    projectless.revision += 1;
    assert!(
        original
            .replacement("owner-1", original.revision, &projectless)
            .is_err()
    );
    projectless.project_id = Some(String::new());
    assert!(projectless.validate().is_err());
    assert_eq!(serde_json::to_vec(&original).unwrap(), frozen);
}
