use super::*;
use super::types::{LocalBlobOrigin, ShortcutTarget};

fn png_bytes() -> Vec<u8> {
    let mut data = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
    data.extend_from_slice(&2u32.to_be_bytes());
    data.extend_from_slice(&3u32.to_be_bytes());
    data.extend_from_slice(&[8, 2, 0, 0, 0, 0, 0, 0, 0]);
    data
}

async fn exercise_backend(lib: &dyn PersonalLibrary) {
    let limits = LibraryLimits {
        max_user_bytes: 120,
        max_user_entries: 4,
        max_object_bytes: 100,
    };
    let png = png_bytes();
    let created = lib
        .create(
            "alice",
            limits,
            CreateLibraryObject::LocalBlob {
                id: None,
                title: "diagram".into(),
                media_type: "image/png".into(),
                data: png.clone(),
                origin: LocalBlobOrigin::Upload,
                origin_execution_id: None,
            },
            Some("s1"),
            None,
        )
        .await
        .unwrap();
    assert_eq!(created.summary.width, Some(2));
    assert_eq!(created.summary.height, Some(3));
    assert_eq!(created.mounted_sessions, vec!["s1"]);
    assert_eq!(
        lib.read_blob("alice", &created.summary.id)
            .await
            .unwrap()
            .unwrap(),
        png
    );
    // Owner isolation: Bob cannot see, read, mount, or delete Alice's object.
    assert!(lib.get("bob", &created.summary.id).await.unwrap().is_none());
    assert!(
        lib.read_blob("bob", &created.summary.id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !lib.mount_session("bob", &created.summary.id, "s-bob")
            .await
            .unwrap()
    );
    assert!(!lib.delete("bob", &created.summary.id).await.unwrap());

    // Shortcuts consume 0 byte quota.
    let shortcut = lib
        .create(
            "alice",
            limits,
            CreateLibraryObject::Shortcut {
                title: "guide".into(),
                target: ShortcutTarget::HostPath {
                    path: "docs/GUIDE.md".into(),
                    project_id: Some("p1".into()),
                    host_id: None,
                    workspace_id: None,
                },
            },
            None,
            Some("p1"),
        )
        .await
        .unwrap();
    let quota = lib.quota("alice", limits).await.unwrap();
    assert_eq!(quota.used_bytes, png.len() as u64);
    assert_eq!(quota.object_count, 2);

    // Project mount propagation + session mount sharing.
    lib.mount_project_to_session("alice", "p1", "s2")
        .await
        .unwrap();
    let s2_mounts = lib.list_session_mounts("alice", "s2").await.unwrap();
    assert_eq!(s2_mounts.len(), 1);
    assert_eq!(s2_mounts[0].id, shortcut.summary.id);
    assert!(
        lib.mount_session("alice", &created.summary.id, "s2")
            .await
            .unwrap()
    );
    assert_eq!(lib.list_session_mounts("alice", "s2").await.unwrap().len(), 2);

    // Strict byte quota rejection (no LRU eviction).
    let overflow = lib
        .create(
            "alice",
            limits,
            CreateLibraryObject::LocalBlob {
                id: None,
                title: "too-big".into(),
                media_type: "text/plain".into(),
                data: vec![b'a'; 100],
                origin: LocalBlobOrigin::Upload,
                origin_execution_id: None,
            },
            None,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        overflow,
        Error::Limit(limit) if limit.kind == LimitKind::LibraryStorageBytes
    ));
    assert!(lib.get("alice", &created.summary.id).await.unwrap().is_some());

    // Delete frees quota and removes mounts.
    assert!(lib.delete("alice", &created.summary.id).await.unwrap());
    assert_eq!(lib.quota("alice", limits).await.unwrap().used_bytes, 0);
    assert_eq!(lib.list_session_mounts("alice", "s2").await.unwrap().len(), 1);
}

struct TestTempDir(std::path::PathBuf);

impl TestTempDir {
    fn new(prefix: &str) -> Self {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let id = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("{prefix}-{}-{id}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TestTempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_personal_library_roundtrips_and_enforces_isolation_and_quota() {
    let dir = TestTempDir::new("microcrab-lib-file");
    let lib = FilePersonalLibrary::open(dir.path()).await.unwrap();
    exercise_backend(&lib).await;
    drop(lib);
    let reopened = FilePersonalLibrary::open(dir.path()).await.unwrap();
    assert_eq!(reopened.list("alice").await.unwrap().len(), 1);
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_personal_library_roundtrips_and_enforces_isolation_and_quota() {
    let dir = TestTempDir::new("microcrab-lib-sqlite");
    let db = dir.path().join("library.sqlite");
    let lib = SqlitePersonalLibrary::open(&db).await.unwrap();
    exercise_backend(&lib).await;
    drop(lib);
    let reopened = SqlitePersonalLibrary::open(&db).await.unwrap();
    assert_eq!(reopened.list("alice").await.unwrap().len(), 1);
}
