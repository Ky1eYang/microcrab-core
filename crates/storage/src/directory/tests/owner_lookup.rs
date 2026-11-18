//! Owner lookup is an exact routing fact, never an authentication shortcut.
use super::*;

async fn exact(directory: &dyn Directory) {
    assert_eq!(directory.session_owner("missing").await.unwrap(), None);
    assert!(directory.session_owner("").await.is_err());
    let first = directory.create(session()).await.unwrap();
    let second = SessionRecord {
        id: "other-session".into(),
        owner: "other-owner".into(),
        ..session()
    };
    directory.create(second).await.unwrap();
    assert_eq!(
        directory.session_owner(&first.id).await.unwrap().as_deref(),
        Some("owner-1")
    );
    assert_eq!(
        directory
            .session_owner("other-session")
            .await
            .unwrap()
            .as_deref(),
        Some("other-owner")
    );
    assert!(
        directory
            .get("other-owner", &first.id)
            .await
            .unwrap()
            .is_none()
    );
    let mut collision = session();
    collision.owner = "other-owner".into();
    assert!(directory.create(collision).await.is_err());
    directory
        .remove("owner-1", &first.id, first.revision)
        .await
        .unwrap();
    assert_eq!(directory.session_owner(&first.id).await.unwrap(), None);
    directory.close().await.unwrap();
    assert!(directory.session_owner("other-session").await.is_err());
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn exact_owner_lookup_file_reopen_and_quarantine() {
    let root = root();
    let directory = FileDirectory::open(&root).await.unwrap();
    exact(&directory).await;
    let reopened = FileDirectory::open(&root).await.unwrap();
    assert_eq!(
        reopened
            .session_owner("other-session")
            .await
            .unwrap()
            .as_deref(),
        Some("other-owner")
    );
    reopened.close().await.unwrap();
    let path = root.join("directory.json");
    let mut value: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    value["sessions"]["other-session"]["version"] = serde_json::json!(999);
    let original = serde_json::to_vec(&value).unwrap();
    std::fs::write(&path, &original).unwrap();
    let quarantined = FileDirectory::open(&root).await.unwrap();
    assert!(quarantined.session_owner("other-session").await.is_err());
    quarantined.close().await.unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), original);
    // The temporary path is constructed by the shared directory test fixture.
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn exact_owner_lookup_sqlite_reopen_and_column_disagreement() {
    let root = root();
    let path = root.join("storage.sqlite");
    let directory = SqliteDirectory::open(&path).await.unwrap();
    exact(&directory).await;
    let reopened = SqliteDirectory::open(&path).await.unwrap();
    assert_eq!(
        reopened
            .session_owner("other-session")
            .await
            .unwrap()
            .as_deref(),
        Some("other-owner")
    );
    reopened.close().await.unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute(
            "UPDATE directory_sessions SET owner='forged-owner' WHERE id='other-session'",
            [],
        )
        .unwrap();
    drop(connection);
    let corrupted = SqliteDirectory::open(&path).await.unwrap();
    assert!(corrupted.session_owner("other-session").await.is_err());
    corrupted.close().await.unwrap();
    // The temporary path is constructed by the shared directory test fixture.
    std::fs::remove_dir_all(root).unwrap();
}
