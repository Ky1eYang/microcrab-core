//! Cache points are evidence for selected paths, not implicit workspace snapshots.
use microcrab_host_api::{CachePointRequest, Host, MAX_CACHE_POINT_BYTES, ToolError};
use microcrab_host_local::{LocalHost, ShellLimits};
use tokio_util::sync::CancellationToken;

async fn point(
    host: &LocalHost,
    path: &str,
    max_bytes: usize,
) -> Result<microcrab_host_api::FileCachePoint, ToolError> {
    host.cache_point(
        CachePointRequest {
            path: path.into(),
            max_bytes,
        },
        CancellationToken::new(),
    )
    .await
}

#[tokio::test]
async fn missing_empty_and_fresh_same_metadata_content_are_distinct_without_traversal() {
    let root = tempfile::tempdir().unwrap();
    let host = LocalHost::new_filesystem(root.path(), ShellLimits::default()).unwrap();
    // An unrelated file exceeds even the legacy whole-workspace byte limit.
    // Selected-path capture must remain available and must never read it.
    let unrelated = std::fs::File::create(root.path().join("unrelated")).unwrap();
    unrelated.set_len(128 * 1024 * 1024).unwrap();
    let missing = point(&host, "chosen", 16).await.unwrap();
    assert_eq!(missing.path, "chosen");
    assert_eq!((missing.data, missing.version), (None, None));
    std::fs::write(root.path().join("chosen"), b"").unwrap();
    let empty = point(&host, "chosen", 16).await.unwrap();
    assert_eq!(empty.data, Some(vec![]));
    assert_eq!(
        empty.version.as_deref(),
        Some("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
    );
    std::fs::write(root.path().join("chosen"), b"one").unwrap();
    let modified = std::fs::metadata(root.path().join("chosen"))
        .unwrap()
        .modified()
        .unwrap();
    let before = point(&host, "chosen", 16).await.unwrap();
    std::fs::write(root.path().join("chosen"), b"two").unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(root.path().join("chosen"))
        .unwrap()
        .set_times(std::fs::FileTimes::new().set_modified(modified))
        .unwrap();
    let after = point(&host, "chosen", 16).await.unwrap();
    assert_eq!(after.data, Some(b"two".to_vec()));
    assert_ne!(before.version, after.version);
}

#[tokio::test]
async fn bounds_admission_and_cancellation_fail_explicitly() {
    let root = tempfile::tempdir().unwrap();
    let host = LocalHost::new_filesystem(root.path(), ShellLimits::default()).unwrap();
    std::fs::write(root.path().join("large"), b"12345").unwrap();
    assert!(matches!(
        point(&host, "large", 4).await,
        Err(ToolError::FileLimit)
    ));
    assert!(matches!(
        point(&host, "large", 0).await,
        Err(ToolError::InvalidRequest(_))
    ));
    assert!(matches!(
        point(&host, "large", MAX_CACHE_POINT_BYTES + 1).await,
        Err(ToolError::InvalidRequest(_))
    ));
    assert!(matches!(
        point(&host, "../outside", 16).await,
        Err(ToolError::PathOutsideRoot)
    ));
    assert!(point(&host, ".", 16).await.is_err());
    std::fs::create_dir(root.path().join("private")).unwrap();
    host.protect_workspace_path("private".into()).unwrap();
    assert!(matches!(
        point(&host, "private/new", 16).await,
        Err(ToolError::Host(ref error)) if error.public_reason.as_deref() == Some("target path is protected by Host private-directory policy")
    ));
    let token = CancellationToken::new();
    token.cancel();
    assert!(matches!(
        host.cache_point(
            CachePointRequest {
                path: "large".into(),
                max_bytes: 16
            },
            token
        )
        .await,
        Err(ToolError::Cancelled)
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn cache_points_reject_symlinks_and_hardlinks() {
    let root = tempfile::tempdir().unwrap();
    let host = LocalHost::new_filesystem(root.path(), ShellLimits::default()).unwrap();
    std::fs::write(root.path().join("source"), b"content").unwrap();
    std::os::unix::fs::symlink(root.path().join("source"), root.path().join("link")).unwrap();
    assert!(point(&host, "link", 16).await.is_err());
    std::fs::hard_link(root.path().join("source"), root.path().join("hard")).unwrap();
    assert!(point(&host, "hard", 16).await.is_err());
}
