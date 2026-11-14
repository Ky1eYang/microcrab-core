//! Rooted file capability admission, limits, and cancellation.
use microcrab_host_api::{Host, ToolError};
use microcrab_host_local::{LocalHost, ShellLimits};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio_util::sync::CancellationToken;

struct Root(PathBuf);
impl Root {
    fn new() -> Self {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "microcrab-files-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        // Host owns a canonical root; system Temp may be reached through a Windows profile junction.
        Self(std::fs::canonicalize(path).unwrap())
    }
    fn host(&self) -> LocalHost {
        LocalHost::new(&self.0, ShellLimits::default()).unwrap()
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn reads_exact_bound_and_rejects_one_byte_over() {
    let root = Root::new();
    std::fs::write(root.0.join("input.txt"), b"hello").unwrap();
    let host = root.host();
    assert_eq!(
        host.read_file("input.txt".into(), 5, CancellationToken::new())
            .await
            .unwrap(),
        b"hello"
    );
    assert_eq!(
        host.read_file(root.0.join("input.txt"), 5, CancellationToken::new())
            .await
            .unwrap(),
        b"hello"
    );
    assert!(matches!(
        host.read_file("input.txt".into(), 4, CancellationToken::new())
            .await,
        Err(ToolError::FileLimit)
    ));
    std::fs::write(root.0.join("empty.txt"), b"").unwrap();
    assert!(
        host.read_file("empty.txt".into(), 1, CancellationToken::new())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn rejects_directory_missing_nul_and_zero_budget() {
    let root = Root::new();
    let host = root.host();
    for (path, limit) in [(".", 10), ("missing", 10), ("bad\0path", 10), (".", 0)] {
        assert!(
            host.read_file(path.into(), limit, CancellationToken::new())
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn rejects_absolute_parent_and_symlink_escape() {
    let root = Root::new();
    let outside = Root::new();
    std::fs::write(outside.0.join("secret.txt"), b"secret").unwrap();
    let host = root.host();
    assert!(matches!(
        host.read_file(outside.0.join("secret.txt"), 100, CancellationToken::new())
            .await,
        Err(ToolError::PathOutsideRoot)
    ));
    let relative_escape = PathBuf::from("..")
        .join(outside.0.file_name().unwrap())
        .join("secret.txt");
    assert!(matches!(
        host.read_file(relative_escape, 100, CancellationToken::new())
            .await,
        Err(ToolError::PathOutsideRoot)
    ));
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(outside.0.join("secret.txt"), root.0.join("escape.txt"))
            .unwrap();
        assert!(matches!(
            host.read_file("escape.txt".into(), 100, CancellationToken::new())
                .await,
            Err(ToolError::Host(ref error)) if error.public_reason.as_deref() == Some("target path contains a disallowed symlink or reparse point")
        ));
    }
}

#[tokio::test]
async fn cancellation_wins_before_filesystem_admission() {
    let root = Root::new();
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(matches!(
        root.host()
            .read_file("missing".into(), 100, cancellation)
            .await,
        Err(ToolError::Cancelled)
    ));
}

#[cfg(windows)]
#[tokio::test]
async fn rejects_alternate_data_streams_and_device_namespace() {
    let root = Root::new();
    let host = root.host();
    for path in [r"input.txt:stream", r"\\.\NUL"] {
        assert!(matches!(
            host.read_file(path.into(), 100, CancellationToken::new())
                .await,
            Err(ToolError::InvalidRequest(_))
        ));
    }
}
