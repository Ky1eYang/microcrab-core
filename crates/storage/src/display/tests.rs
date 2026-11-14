use super::*;
use serde_json::json;
use std::fs;
#[path = "graph_tests.rs"]
mod graph_tests;
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("microcrab-display-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn record(sequence: u64, attempt: &str, body: &str) -> ArchiveRecord {
    ArchiveRecord {
        sequence,
        attempt_id: attempt.into(),
        event: json!({"phase":"reasoning_delta","text":body}),
    }
}
fn contract(source: DisplaySource) {
    assert_eq!(source.revision().unwrap(), None);
    assert!(source.query(0, None, 1).unwrap().is_none());
    let text = format!(
        "{}middle:{}:完整尾部",
        "思考🙂".repeat(90000),
        "x".repeat(300000)
    );
    let first = record(1, "attempt-a", &text);
    let second = record(2, "attempt-b", "other thought");
    let third = record(3, "attempt-a", "final");
    {
        let mut writer = source.open().unwrap();
        assert_eq!(writer.revision().unwrap(), 0);
        assert!(source.open().is_err());
        writer.append(0, &first).unwrap();
        assert!(writer.append(0, &second).is_err());
        writer.append(1, &second).unwrap();
        writer.append(2, &third).unwrap();
        writer.flush().unwrap();
    }
    assert_eq!(source.revision().unwrap(), Some(3));
    let page = source.query(0, Some(3), 1).unwrap().unwrap();
    assert_eq!(page.records, vec![first]);
    assert_eq!(page.next_cursor, Some(1));
    assert_eq!(page.high_water, 3);
    let page = source.query(1, Some(3), 1).unwrap().unwrap();
    assert_eq!(page.records, vec![second]);
    assert_eq!(page.next_cursor, Some(2));
    let page = source.query(2, Some(3), 1).unwrap().unwrap();
    assert_eq!(page.records, vec![third]);
    assert_eq!(page.next_cursor, None);
    assert!(source.query(4, None, 1).is_err());
    assert!(source.query(0, Some(4), 1).is_err());
    assert!(source.query(0, None, 0).is_err());
    let mut reopened = source.open().unwrap();
    assert_eq!(reopened.revision().unwrap(), 3);
    reopened
        .append(3, &record(4, "attempt-c", "after restart"))
        .unwrap();
    // A pinned page never expands to include newly appended observations.
    assert_eq!(
        source.query(3, Some(3), 1).unwrap().unwrap().records.len(),
        0
    );
}
#[cfg(feature = "storage-file")]
#[test]
fn file_complete_large_utf8_pages_restart_and_cas() {
    let directory = Directory::new();
    contract(DisplaySource::File {
        directory: directory.0.join("display"),
        scope: DisplayScope::new("owner", "root", "session"),
    });
}
#[cfg(feature = "storage-sqlite")]
#[test]
fn sqlite_complete_large_utf8_pages_restart_and_cas() {
    let directory = Directory::new();
    contract(DisplaySource::Sqlite {
        database: directory.0.join("display.db"),
        scope: DisplayScope::new("owner", "root", "session"),
    });
}
#[cfg(feature = "storage-file")]
#[test]
fn file_readonly_absence_foreign_scope_corruption_and_uncommitted_tail() {
    let directory = Directory::new();
    let path = directory.0.join("absent");
    let source = DisplaySource::File {
        directory: path.clone(),
        scope: DisplayScope::new("a", "root", "session"),
    };
    assert_eq!(source.revision().unwrap(), None);
    assert!(!path.exists());
    let mut writer = source.open().unwrap();
    writer.append(0, &record(1, "a", "committed")).unwrap();
    drop(writer);
    let foreign = DisplaySource::File {
        directory: path.clone(),
        scope: DisplayScope::new("b", "root", "session"),
    };
    assert!(foreign.revision().is_err());
    assert!(foreign.open().is_err());
    fs::write(path.join("00000000000000000002.json"), b"{partial").unwrap();
    assert_eq!(source.revision().unwrap(), Some(1));
    let mut writer = source.open().unwrap();
    writer
        .append(1, &record(2, "a", "resumed after crash"))
        .unwrap();
    let orphans: Vec<_> = fs::read_dir(&path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|entry| {
            entry
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with(".orphan-")
        })
        .collect();
    assert_eq!(orphans.len(), 1);
    assert_eq!(fs::read(&orphans[0]).unwrap(), b"{partial");
    assert_eq!(source.revision().unwrap(), Some(2));
    assert_eq!(
        source.query(1, None, 1).unwrap().unwrap().records[0].event["text"],
        "resumed after crash"
    );
    drop(writer);
    fs::write(path.join("00000000000000000001.json"), b"{corrupt").unwrap();
    assert!(source.query(0, None, 1).is_err());
    assert!(source.open().is_err());
}
#[cfg(feature = "storage-sqlite")]
#[test]
fn sqlite_readonly_absence_foreign_scope_and_corruption() {
    let directory = Directory::new();
    let path = directory.0.join("absent.db");
    let scope = DisplayScope::new("a", "root", "session");
    let source = DisplaySource::Sqlite {
        database: path.clone(),
        scope,
    };
    assert_eq!(source.revision().unwrap(), None);
    assert!(!path.exists());
    let mut writer = source.open().unwrap();
    writer.append(0, &record(1, "a", "committed")).unwrap();
    drop(writer);
    let foreign = DisplaySource::Sqlite {
        database: path.clone(),
        scope: DisplayScope::new("b", "root", "session"),
    };
    assert_eq!(foreign.revision().unwrap(), None);
    assert!(foreign.open().is_err());
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute(
            "UPDATE display_records SET record=?1",
            [b"{corrupt".as_slice()],
        )
        .unwrap();
    assert!(source.query(0, None, 1).is_err());
    assert!(source.open().is_err());
}
#[cfg(feature = "storage-sqlite")]
#[test]
fn sqlite_inspection_does_not_create_schema_in_existing_database() {
    let directory = Directory::new();
    let path = directory.0.join("existing.db");
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch("CREATE TABLE unrelated(value TEXT);")
        .unwrap();
    let source = DisplaySource::Sqlite {
        database: path,
        scope: DisplayScope::new("a", "root", "session"),
    };
    assert_eq!(source.revision().unwrap(), None);
    let count: u64 = connection
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE name LIKE 'display_%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn rejected_display_scope_keeps_closed_cause_without_private_identifiers() {
    let error = DisplayScope::new("private-storage-secret", "", "session")
        .validate()
        .unwrap_err();
    assert_eq!(error.public_reason(), Some("invalid display scope"));
    assert!(!format!("{error:?}").contains("private-storage-secret"));
    let head = Head::empty(DisplayScope::new("owner", "root", "session"));
    let error = page_bounds(&head, 0, None, 0).unwrap_err();
    assert_eq!(error.public_reason(), Some("invalid display page cursor"));
}
