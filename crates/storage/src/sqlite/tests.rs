use super::open;
use rusqlite::TransactionBehavior;

#[test]
fn system_sqlite_preserves_durable_connection_policy_and_transactions() {
    let directory =
        std::env::temp_dir().join(format!("microcrab-system-sqlite-{}", uuid::Uuid::new_v4()));
    let database = directory.join("storage.sqlite");
    {
        let mut connection = open(&database, "system-linkage-regression").unwrap();
        let version = rusqlite::version_number();
        assert!(
            version >= 3_024_000,
            "storage UPSERT requires SQLite >= 3.24.0; linked {} ({version})",
            rusqlite::version()
        );
        assert!(
            rusqlite::ffi::SQLITE_VERSION_NUMBER <= version,
            "SQLite runtime is older than the generated API bindings"
        );
        let journal: String = connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        let synchronous: i64 = connection
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .unwrap();
        let foreign_keys: i64 = connection
            .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
            .unwrap();
        assert_eq!(journal, "wal");
        assert_eq!(synchronous, 2);
        assert_eq!(foreign_keys, 1);
        connection
            .execute_batch(
                "CREATE TABLE parent(id INTEGER PRIMARY KEY, body BLOB NOT NULL);
                 CREATE TABLE child(parent_id INTEGER REFERENCES parent(id));",
            )
            .unwrap();
        let body = "system SQLite archive: 思考🙂".repeat(4096).into_bytes();
        {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            transaction
                .execute("INSERT INTO parent(id,body) VALUES(1,?1)", [&body])
                .unwrap();
            transaction
                .execute("INSERT INTO child(parent_id) VALUES(1)", [])
                .unwrap();
            transaction.commit().unwrap();
        }
        {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            transaction
                .execute(
                    "INSERT INTO parent(id,body) VALUES(1,?1)
                     ON CONFLICT(id) DO UPDATE SET body=excluded.body",
                    [b"rolled back".as_slice()],
                )
                .unwrap();
            // Dropping an uncommitted transaction must retain the original bytes.
        }
        assert!(
            connection
                .execute("INSERT INTO child(parent_id) VALUES(999)", [])
                .is_err()
        );
        drop(connection);
        let reopened = open(&database, "system-linkage-regression").unwrap();
        let restored: Vec<u8> = reopened
            .query_row("SELECT body FROM parent WHERE id=1", [], |row| row.get(0))
            .unwrap();
        assert_eq!(restored, body);
        let result: String = reopened
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(result, "ok");
    }
    std::fs::remove_dir_all(directory).unwrap();
}
