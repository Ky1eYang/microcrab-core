//! Behavioral storage tests authored for the parent verification gate; not run here.
use super::*;
use serde_json::json;
async fn contract(store: &dyn Configuration) {
    let first = store
        .compare_exchange(
            "alice",
            "provider",
            "user:local",
            0,
            Some(json!({"api_key":"private"})),
        )
        .await
        .unwrap();
    assert_eq!(first.revision, 1);
    assert!(
        store
            .get("bob", "provider", "user:local")
            .await
            .unwrap()
            .is_none()
    );
    assert!(store.list("bob", "provider").await.unwrap().is_empty());
    assert!(
        store
            .compare_exchange("alice", "provider", "user:local", 0, None)
            .await
            .is_err()
    );
    let other = store
        .compare_exchange(
            "alice",
            "settings",
            "user",
            0,
            Some(json!({"review":false})),
        )
        .await
        .unwrap();
    assert_eq!(other.revision, 1);
    let removed = store
        .compare_exchange("alice", "provider", "user:local", 1, None)
        .await
        .unwrap();
    assert_eq!(removed.revision, 2);
    assert!(store.list("alice", "provider").await.unwrap().is_empty());
    assert_eq!(
        store
            .get("alice", "provider", "user:local")
            .await
            .unwrap()
            .unwrap()
            .revision,
        2
    );
    assert!(
        store
            .compare_exchange("alice", "provider", "user:local", 0, Some(json!({})))
            .await
            .is_err()
    );
    assert_eq!(
        store
            .get("alice", "settings", "user")
            .await
            .unwrap()
            .unwrap()
            .value,
        Some(json!({"review":false}))
    );
    assert!(
        store
            .compare_exchange(
                "alice",
                "settings",
                "oversize",
                0,
                Some(json!("x".repeat(MAX_CONFIGURATION_BYTES)))
            )
            .await
            .is_err()
    );
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_owner_cas_reset_and_restart() {
    let root =
        std::env::temp_dir().join(format!("microcrab-configuration-{}", uuid::Uuid::new_v4()));
    {
        let store = FileConfiguration::open(&root).await.unwrap();
        contract(&store).await;
    }
    let store = FileConfiguration::open(&root).await.unwrap();
    assert_eq!(
        store
            .get("alice", "provider", "user:local")
            .await
            .unwrap()
            .unwrap()
            .revision,
        2
    );
}
#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_owner_cas_reset_and_restart() {
    let root =
        std::env::temp_dir().join(format!("microcrab-configuration-{}", uuid::Uuid::new_v4()));
    let path = &root.join("config.sqlite");
    {
        let store = SqliteConfiguration::open(&path).await.unwrap();
        contract(&store).await;
    }
    let store = SqliteConfiguration::open(&path).await.unwrap();
    assert_eq!(
        store
            .get("alice", "provider", "user:local")
            .await
            .unwrap()
            .unwrap()
            .revision,
        2
    );
}
