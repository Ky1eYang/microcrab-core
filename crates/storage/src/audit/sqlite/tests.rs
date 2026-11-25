use super::*;
fn root() -> PathBuf {
    std::env::temp_dir().join(format!("microcrab-audit-{}", uuid::Uuid::new_v4()))
}
fn record(
    store: &mut SqliteAuditStore,
    seq: u64,
    previous: Option<&str>,
    hash: &str,
) -> Result<(), Error> {
    let bytes = serde_json::to_vec(&serde_json::json!({"sequence":seq,"previous":previous,"sha256":hash,"data":{"kind":"format","format":"test"}})).unwrap();
    let mut line = bytes;
    line.push(b'\n');
    store.append(seq - 1, previous, seq, hash, &line)
}
#[test]
fn expected_head_conflict_does_not_commit_record_or_notify_new_head() {
    let directory = root();
    let path = directory.join("storage.sqlite");
    let scope = AuditScope::new("owner", "root", "root");
    let mut store = SqliteAuditStore::open(&path, scope.clone()).unwrap();
    record(&mut store, 1, None, "first").unwrap();
    assert!(record(&mut store, 2, Some("stale"), "second").is_err());
    let mut reader = SqliteAuditStore::inspect(&path, &scope).unwrap().unwrap();
    let mut bytes = String::new();
    reader.read_to_string(&mut bytes).unwrap();
    assert_eq!(bytes.lines().count(), 1);
    record(&mut store, 2, Some("first"), "second").unwrap();
    assert!(SqliteAuditStore::open(&path, scope.clone()).is_err());
    drop(store);
    let mut reopened = SqliteAuditStore::open(&path, scope).unwrap();
    record(&mut reopened, 3, Some("second"), "third").unwrap();
}
#[test]
fn root_purge_fences_live_writers_and_removes_only_authorized_descendants() {
    let directory = root();
    let path = directory.join("storage.sqlite");
    for scope in [
        AuditScope::new("owner", "root", "root"),
        AuditScope::new("owner", "root", "child"),
        AuditScope::new("owner", "other-root", "other-child"),
        AuditScope::new("other-owner", "private", "private-child"),
    ] {
        let mut store = SqliteAuditStore::open(&path, scope).unwrap();
        record(&mut store, 1, None, "first").unwrap();
    }
    let connection = Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TABLE journal_nodes(session_id TEXT,id TEXT,body BLOB); CREATE TABLE journal_heads(session_id TEXT,cursor BLOB,redo BLOB);
        INSERT INTO journal_heads VALUES('root',NULL,X'00'),('child',NULL,X'00'),('other-child',NULL,X'00'),('private-child',NULL,X'00');
        INSERT INTO journal_nodes VALUES('root','node',X'00'),('child','node',X'00'),('other-child','node',X'00'),('private-child','node',X'00');
        CREATE TABLE journal_file_versions(session_id TEXT PRIMARY KEY,body BLOB NOT NULL);
        INSERT INTO journal_file_versions VALUES('root',X'00'),('child',X'00'),('other-child',X'00'),('private-child',X'00');").unwrap();
    let mut ledger = crate::usage::SqliteUsageStore::open(
        &path,
        AuditScope::new("owner", "root", "root"),
        "usage.jsonl",
    )
    .unwrap();
    ledger.append(b"{}\n").unwrap();
    for scope in [
        crate::display::DisplayScope::new("owner", "root", "root"),
        crate::display::DisplayScope::new("owner", "root", "child"),
        crate::display::DisplayScope::new("owner", "other-root", "other-child"),
        crate::display::DisplayScope::new("other-owner", "root", "foreign-child"),
    ] {
        let source = crate::display::DisplaySource::Sqlite {
            database: path.clone(),
            scope,
        };
        let mut display = source.open().unwrap();
        display
            .append(
                0,
                &crate::display::ArchiveRecord {
                    sequence: 1,
                    attempt_id: "attempt".into(),
                    event: serde_json::json!({"text":"complete thought"}),
                },
            )
            .unwrap();
    }
    let active = SqliteAuditStore::open(&path, AuditScope::new("owner", "root", "child")).unwrap();
    assert!(SqliteAuditStore::purge_root(&path, "owner", "root").is_err());
    drop(active);
    SqliteAuditStore::purge_root(&path, "owner", "root").unwrap();
    SqliteAuditStore::purge_root(&path, "owner", "root").unwrap();
    let count: u64 = connection
        .query_row("SELECT count(*) FROM journal_nodes", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 2);
    let versions = connection
        .prepare("SELECT session_id FROM journal_file_versions ORDER BY session_id")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(versions, ["other-child", "private-child"]);
    let count: u64 = connection
        .query_row("SELECT count(*) FROM audit_streams", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 2);
    for table in ["display_streams", "display_records"] {
        let count: u64 = connection
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 2);
        let unauthorized: u64 = connection
            .query_row(
                &format!(
                    "SELECT count(*) FROM {table} WHERE owner='other-owner' AND root_session='root'"
                ),
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(unauthorized, 1);
        let deleted: u64 = connection
            .query_row(
                &format!(
                    "SELECT count(*) FROM {table} WHERE owner='owner' AND root_session='root'"
                ),
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(deleted, 0);
    }
    assert!(
        SqliteAuditStore::inspect(&path, &AuditScope::new("owner", "root", "child"))
            .unwrap()
            .is_none()
    );
    assert!(
        crate::usage::SqliteUsageStore::inspect(
            &path,
            &AuditScope::new("owner", "root", "root"),
            "usage.jsonl"
        )
        .unwrap()
        .is_empty()
    );
}
#[test]
fn root_purge_fences_independent_display_writer_until_it_closes() {
    use crate::display::{ArchiveRecord, DisplayScope, DisplaySource};
    let path = root().join("storage.sqlite");
    let audit_scope = AuditScope::new("owner", "root", "root");
    let mut audit = SqliteAuditStore::open(&path, audit_scope.clone()).unwrap();
    record(&mut audit, 1, None, "first").unwrap();
    drop(audit);
    let source = DisplaySource::Sqlite {
        database: path.clone(),
        scope: DisplayScope::new("owner", "root", "child"),
    };
    let mut display = source.open().unwrap();
    let archived = ArchiveRecord {
        sequence: 1,
        attempt_id: "attempt".into(),
        event: serde_json::json!({"text":"retained display body"}),
    };
    display.append(0, &archived).unwrap();
    let error = SqliteAuditStore::purge_root(&path, "owner", "root").unwrap_err();
    assert!(
        error
            .to_string()
            .contains("display scope still has a live writer")
    );
    assert_eq!(
        SqliteAuditStore::revision(&path, &audit_scope).unwrap(),
        Some(1)
    );
    assert_eq!(display.revision().unwrap(), 1);
    assert_eq!(
        source.query(0, None, 10).unwrap().unwrap().records,
        vec![archived]
    );
    drop(display);

    // A live display writer in another owner's identical root remains unrelated.
    let foreign = DisplaySource::Sqlite {
        database: path.clone(),
        scope: DisplayScope::new("other-owner", "root", "foreign-child"),
    };
    let mut foreign_writer = foreign.open().unwrap();
    SqliteAuditStore::purge_root(&path, "owner", "root").unwrap();
    assert_eq!(
        SqliteAuditStore::revision(&path, &audit_scope).unwrap(),
        None
    );
    assert!(source.query(0, None, 10).unwrap().is_none());
    assert_eq!(foreign_writer.revision().unwrap(), 0);
    let mut fresh = source.open().unwrap();
    assert_eq!(fresh.revision().unwrap(), 0);
    fresh
        .append(
            0,
            &ArchiveRecord {
                sequence: 1,
                attempt_id: "fresh-attempt".into(),
                event: serde_json::json!({"text":"fresh stream"}),
            },
        )
        .unwrap();
    assert_eq!(fresh.revision().unwrap(), 1);
}

#[test]
fn root_purge_does_not_create_display_schema() {
    let directory = root();
    let path = directory.join("storage.sqlite");
    let mut audit =
        SqliteAuditStore::open(&path, AuditScope::new("owner", "root", "root")).unwrap();
    record(&mut audit, 1, None, "first").unwrap();
    drop(audit);
    SqliteAuditStore::purge_root(&path, "owner", "root").unwrap();
    let connection = Connection::open(&path).unwrap();
    let count: u64 = connection.query_row("SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('display_streams','display_records')",[],|row|row.get(0)).unwrap();
    assert_eq!(count, 0);
}

#[test]
fn missing_inspection_never_creates_a_database_or_parent_directory() {
    let directory = root();
    assert!(
        SqliteAuditStore::inspect(
            &directory.join("storage.sqlite"),
            &AuditScope::new("owner", "root", "root")
        )
        .unwrap()
        .is_none()
    );
    assert!(!directory.exists());
}
