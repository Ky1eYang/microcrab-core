//! Primitive acceptance boundaries independent of an installed shell or network.
use bytes::Bytes;
use microcrab_host_api::{
    ErrorCategory, FsApi, HandleId, Host, HostAPI, OpContext, ProcessApi, ShellApi, ShellRequest,
    ToolError, WritePrecondition, WriteRequest,
    filesystem::{FileHandle, FileKind, ListOptions, OpenOptions},
    process::ProcessSpec,
};
use microcrab_host_local::{LocalHost, ShellLimits};
use std::{
    path::Path,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

fn context() -> OpContext {
    OpContext {
        principal: "test-owner".into(),
        ..OpContext::default()
    }
}
fn read_options() -> OpenOptions {
    OpenOptions {
        read: true,
        ..OpenOptions::default()
    }
}
fn host(root: &Path) -> LocalHost {
    LocalHost::new_filesystem(root, ShellLimits::default()).unwrap()
}
fn category<T>(result: Result<T, microcrab_host_api::HostError>, expected: ErrorCategory) {
    match result {
        Ok(_) => panic!("expected {expected:?}"),
        Err(error) => assert_eq!(error.category, expected, "{error}"),
    }
}

#[tokio::test]
async fn filesystem_only_host_reports_precise_support_and_keeps_other_ports_unsupported() {
    let root = tempfile::tempdir().unwrap();
    let host = host(root.path());
    let api: &dyn HostAPI = &host;
    assert!(api.supports("filesystem.open"));
    assert!(api.supports("filesystem.atomic-write"));
    assert!(!api.supports("filesystem.write"));
    assert!(!api.supports("process.spawn"));
    assert!(!api.capabilities().process_kill_tree);
    assert!(!api.capabilities().process_pty);
    assert!(!api.capabilities().fs_watch);
    assert!(!api.capabilities().fs_symlink);
    assert!(!api.capabilities().net_listen);
    assert!(!api.capabilities().net_udp);
    assert!(
        !host
            .tool_capabilities()
            .iter()
            .any(|name| name == "shell" || name == "start_job")
    );
    assert!(host.info().unwrap().shell.is_empty());
    category(
        ProcessApi::spawn(
            api,
            &context(),
            ProcessSpec {
                program: "never-run".into(),
                ..ProcessSpec::default()
            },
        )
        .await,
        ErrorCategory::Unsupported,
    );
    category(
        FsApi::open(
            api,
            &context(),
            Path::new("never-create"),
            OpenOptions {
                write: true,
                create: true,
                ..OpenOptions::default()
            },
        )
        .await,
        ErrorCategory::Unsupported,
    );
    assert!(!root.path().join("never-create").exists());
    category(
        ShellApi::launch(
            &host,
            &context(),
            ShellRequest {
                command: "never-run".into(),
                cwd: None,
                output_bytes: 1024,
            },
        )
        .await,
        ErrorCategory::Unsupported,
    );
    host.disable_process_execution();
    assert!(!HostAPI::capabilities(&host).supports("process.spawn"));
}

#[tokio::test]
async fn typed_handles_share_clones_but_reject_foreign_stale_forged_and_other_principals() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("file"), b"abcdef").unwrap();
    let one = host(root.path());
    let clone = one.clone();
    let two = host(root.path());
    let ctx = context();
    let first = FsApi::open(&one, &ctx, Path::new("file"), read_options())
        .await
        .unwrap();
    let second = FsApi::open(&two, &ctx, Path::new("file"), read_options())
        .await
        .unwrap();
    assert_ne!(first, second);
    assert_eq!(
        FsApi::read(&clone, &ctx, first, 2, 20).await.unwrap(),
        &b"cdef"[..]
    );
    assert!(
        FsApi::read(&clone, &ctx, first, 100, 4)
            .await
            .unwrap()
            .is_empty()
    );
    category(
        FsApi::read(&two, &ctx, first, 0, 1).await,
        ErrorCategory::NotFound,
    );
    category(
        FsApi::read(&one, &ctx, FileHandle(HandleId(0)), 0, 1).await,
        ErrorCategory::NotFound,
    );
    let other = OpContext {
        principal: "other".into(),
        ..context()
    };
    category(
        FsApi::read(&one, &other, first, 0, 1).await,
        ErrorCategory::Permission,
    );
    category(
        FsApi::close(&one, &other, first).await,
        ErrorCategory::Permission,
    );
    assert_eq!(
        FsApi::read(&one, &ctx, first, 0, 1).await.unwrap(),
        &b"a"[..]
    );
    FsApi::close(&clone, &ctx, first).await.unwrap();
    category(
        FsApi::read(&one, &ctx, first, 0, 1).await,
        ErrorCategory::NotFound,
    );
    category(
        FsApi::close(&one, &ctx, first).await,
        ErrorCategory::NotFound,
    );
    let next = FsApi::open(&one, &ctx, Path::new("file"), read_options())
        .await
        .unwrap();
    assert_ne!(first, next);
    FsApi::close(&one, &ctx, next).await.unwrap();
    FsApi::close(&two, &ctx, second).await.unwrap();
}

#[tokio::test]
async fn private_policy_covers_open_handles_list_metadata_and_atomic_publication() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("private")).unwrap();
    std::fs::write(root.path().join("private/secret"), b"secret").unwrap();
    std::fs::write(root.path().join("public"), b"public").unwrap();
    let host = host(root.path());
    let ctx = context();
    let handle = FsApi::open(&host, &ctx, Path::new("private/secret"), read_options())
        .await
        .unwrap();
    host.protect_workspace_path("private".into()).unwrap();
    let clone = host.clone();
    category(
        FsApi::read(&clone, &ctx, handle, 0, 100).await,
        ErrorCategory::Permission,
    );
    category(
        FsApi::stat(&clone, &ctx, Path::new("private/secret")).await,
        ErrorCategory::Permission,
    );
    category(
        FsApi::open(&clone, &ctx, Path::new("private/secret"), read_options()).await,
        ErrorCategory::Permission,
    );
    category(
        FsApi::list(&clone, &ctx, Path::new("private"), ListOptions::default()).await,
        ErrorCategory::Permission,
    );
    category(
        FsApi::atomic_write(
            &clone,
            &ctx,
            Path::new("private/secret"),
            Bytes::from_static(b"overwrite"),
        )
        .await,
        ErrorCategory::Permission,
    );
    category(
        FsApi::atomic_write(&clone, &ctx, Path::new("private/new"), Bytes::new()).await,
        ErrorCategory::Permission,
    );
    let listing = FsApi::list(
        &clone,
        &ctx,
        Path::new("."),
        ListOptions {
            include_hidden: true,
            ..ListOptions::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(listing.len(), 1);
    assert_eq!(listing[0].name, "public");
    assert_eq!(listing[0].kind, FileKind::File);
    FsApi::close(&host, &ctx, handle).await.unwrap();
    assert_eq!(
        std::fs::read(root.path().join("private/secret")).unwrap(),
        b"secret"
    );
    assert!(!root.path().join("private/new").exists());
}

#[tokio::test]
async fn contexts_bound_handle_lifetime_and_reject_expired_or_cancelled_admission() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("file"), b"abc").unwrap();
    let host = host(root.path());
    let ctx = context();
    let handle = FsApi::open(&host, &ctx, Path::new("file"), read_options())
        .await
        .unwrap();
    ctx.cancel.cancel();
    let error = FsApi::read(&host, &context(), handle, 0, 1)
        .await
        .unwrap_err();
    assert!(matches!(
        error.category,
        ErrorCategory::Cancelled | ErrorCategory::NotFound
    ));
    category(
        FsApi::stat(&host, &ctx, Path::new("file")).await,
        ErrorCategory::Cancelled,
    );
    category(
        FsApi::atomic_write(
            &host,
            &ctx,
            Path::new("cancelled"),
            Bytes::from_static(b"x"),
        )
        .await,
        ErrorCategory::Cancelled,
    );
    let expired = OpContext {
        deadline: Some(Instant::now() - Duration::from_secs(1)),
        ..context()
    };
    category(
        FsApi::open(&host, &expired, Path::new("file"), read_options()).await,
        ErrorCategory::Timeout,
    );
    assert!(!root.path().join("cancelled").exists());
    // A fresh principal-matching cleanup context can release an expired handle;
    // the cancellation watcher may already have completed the same cleanup.
    if let Err(error) = FsApi::close(&host, &context(), handle).await {
        assert_eq!(error.category, ErrorCategory::NotFound);
    }
}

#[tokio::test]
async fn byte_publication_preserves_versions() {
    let root = tempfile::tempdir().unwrap();
    let host = host(root.path());
    let ctx = context();
    let original = Host::write(
        &host,
        WriteRequest {
            path: "file".into(),
            content: "before".into(),
            precondition: WritePrecondition::Missing,
        },
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let bytes = Bytes::from_static(&[0, 0xff, b'a', b'\n']);
    FsApi::atomic_write(&host, &ctx, Path::new("file"), bytes.clone())
        .await
        .unwrap();
    assert!(matches!(
        Host::write(
            &host,
            WriteRequest {
                path: "file".into(),
                content: "stale".into(),
                precondition: WritePrecondition::Version(original.version)
            },
            CancellationToken::new()
        )
        .await,
        Err(ToolError::FileConflict)
    ));
    assert_eq!(
        host.read_file("file".into(), 1024, CancellationToken::new())
            .await
            .unwrap(),
        bytes.as_ref()
    );
    FsApi::atomic_write(&host, &ctx, Path::new("new"), Bytes::new())
        .await
        .unwrap();
    assert_eq!(
        FsApi::stat(&host, &ctx, Path::new("new"))
            .await
            .unwrap()
            .size,
        0
    );
    assert!(std::fs::read_dir(root.path()).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".microcrab-write-")
    }));
}

#[tokio::test]
async fn confinement_limits_and_unsupported_refinements_have_no_side_effects() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("file"), b"abc").unwrap();
    std::fs::write(root.path().join(".hidden"), b"hidden").unwrap();
    let host = host(root.path());
    let ctx = context();
    for path in ["../escape", "NUL", "file:stream", "file.", "missing/child"] {
        assert!(
            FsApi::atomic_write(&host, &ctx, Path::new(path), Bytes::from_static(b"x"))
                .await
                .is_err()
        );
    }
    let listing = FsApi::list(&host, &ctx, Path::new("."), ListOptions::default())
        .await
        .unwrap();
    assert_eq!(listing.len(), 1);
    assert_eq!(listing[0].name, "file");
    category(
        FsApi::list(
            &host,
            &ctx,
            Path::new("."),
            ListOptions {
                recursive: true,
                ..ListOptions::default()
            },
        )
        .await,
        ErrorCategory::Unsupported,
    );
    category(
        FsApi::mkdir(&host, &ctx, Path::new("new-directory"), false).await,
        ErrorCategory::Unsupported,
    );
    let handle = FsApi::open(&host, &ctx, Path::new("file"), read_options())
        .await
        .unwrap();
    category(
        FsApi::read(&host, &ctx, handle, 0, 1024 * 1024 + 1).await,
        ErrorCategory::Io,
    );
    category(
        FsApi::read(&host, &ctx, handle, u64::MAX, 1).await,
        ErrorCategory::Io,
    );
    category(
        FsApi::write(&host, &ctx, handle, 0, Bytes::from_static(b"x")).await,
        ErrorCategory::Unsupported,
    );
    FsApi::close(&host, &ctx, handle).await.unwrap();
    assert_eq!(std::fs::read(root.path().join("file")).unwrap(), b"abc");
    assert!(!root.path().join("new-directory").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn filesystem_primitives_reject_symlinks_and_hardlinks() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("secret"), b"secret").unwrap();
    std::os::unix::fs::symlink(outside.path(), root.path().join("link")).unwrap();
    std::fs::hard_link(outside.path().join("secret"), root.path().join("hardlink")).unwrap();
    let host = host(root.path());
    let ctx = context();
    category(
        FsApi::stat(&host, &ctx, Path::new("link/secret")).await,
        ErrorCategory::Permission,
    );
    category(
        FsApi::open(&host, &ctx, Path::new("link/secret"), read_options()).await,
        ErrorCategory::Permission,
    );
    assert!(
        FsApi::open(&host, &ctx, Path::new("hardlink"), read_options())
            .await
            .is_err()
    );
    assert!(
        FsApi::atomic_write(
            &host,
            &ctx,
            Path::new("hardlink"),
            Bytes::from_static(b"overwrite")
        )
        .await
        .is_err()
    );
    assert_eq!(
        std::fs::read(outside.path().join("secret")).unwrap(),
        b"secret"
    );
}
