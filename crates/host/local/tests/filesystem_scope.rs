//! Explicit scope changes must agree across the facade and primitive ports.
use bytes::Bytes;
use microcrab_host_api::{
    CachePointRequest, DeleteRequest, ErrorCategory, FileMutation, FsApi, GlobRequest, GrepRequest,
    Host, HostAPI, MoveRequest, OpContext, ReadRequest, ToolError, WritePrecondition, WriteRequest,
    filesystem::{ListOptions, OpenOptions},
};
use microcrab_host_local::{LocalHost, ShellLimits};
use std::{
    fs,
    path::{Path, PathBuf},
};
use tokio_util::sync::CancellationToken;

fn token() -> CancellationToken {
    CancellationToken::new()
}
fn host(root: &Path, unrestricted: bool) -> LocalHost {
    LocalHost::new_filesystem_with_scope(root, ShellLimits::default(), unrestricted).unwrap()
}
fn read(path: PathBuf) -> ReadRequest {
    ReadRequest {
        path,
        offset_line: 1,
        max_lines: 10,
        max_bytes: 1024,
    }
}
fn write(path: PathBuf) -> WriteRequest {
    WriteRequest {
        path,
        content: "needle".into(),
        precondition: WritePrecondition::Missing,
    }
}
fn glob(root: PathBuf) -> GlobRequest {
    GlobRequest {
        root,
        pattern: "**/*".into(),
        include_hidden: true,
        include_ignored: true,
        include_directories: false,
        max_results: 100,
    }
}
fn grep(root: PathBuf) -> GrepRequest {
    GrepRequest {
        root,
        pattern: "needle".into(),
        include_glob: None,
        case_sensitive: true,
        literal: true,
        max_matches: 100,
        max_bytes: 1024,
    }
}
fn cache(path: PathBuf) -> CachePointRequest {
    CachePointRequest {
        path,
        max_bytes: 1024,
    }
}
fn denied<T>(result: Result<T, ToolError>) {
    assert_eq!(
        result
            .err()
            .expect("private access must fail")
            .into_host_error()
            .category,
        ErrorCategory::Permission
    );
}

#[tokio::test]
async fn default_and_unrestricted_agree_across_every_path_consumer() {
    let fixture = tempfile::tempdir().unwrap();
    let base = fixture.path().canonicalize().unwrap();
    let root = base.join("workspace");
    let outside = base.join("outside");
    fs::create_dir(&root).unwrap();
    fs::create_dir(&outside).unwrap();
    fs::write(outside.join("source"), "needle").unwrap();
    let confined = LocalHost::new_filesystem(&root, ShellLimits::default()).unwrap();
    let unrestricted = host(&root, true);
    assert_eq!(confined.info().unwrap().description, None);
    assert!(
        unrestricted
            .info()
            .unwrap()
            .description
            .unwrap()
            .contains("unrestricted")
    );
    assert_eq!(
        confined.tool_capabilities(),
        unrestricted.tool_capabilities()
    );
    assert!(!unrestricted.supports("process.spawn"));
    let ctx = OpContext::default();
    for (index, directory) in [outside.clone(), PathBuf::from("../outside")]
        .into_iter()
        .enumerate()
    {
        let source = directory.join("source");
        assert!(matches!(
            confined.read_file(source.clone(), 1024, token()).await,
            Err(ToolError::PathOutsideRoot)
        ));
        assert!(
            confined
                .read_text(read(source.clone()), token())
                .await
                .is_err()
        );
        assert!(
            confined
                .normalize_resource(source.clone(), token())
                .await
                .is_err()
        );
        assert!(
            confined
                .cache_point(cache(source.clone()), token())
                .await
                .is_err()
        );
        assert!(
            confined
                .glob(glob(directory.clone()), token())
                .await
                .is_err()
        );
        assert!(
            confined
                .grep(grep(directory.clone()), token())
                .await
                .is_err()
        );
        assert!(FsApi::stat(&confined, &ctx, &source).await.is_err());
        assert!(
            FsApi::list(&confined, &ctx, &directory, ListOptions::default())
                .await
                .is_err()
        );
        assert!(
            FsApi::open(
                &confined,
                &ctx,
                &source,
                OpenOptions {
                    read: true,
                    ..OpenOptions::default()
                }
            )
            .await
            .is_err()
        );
        let destination = directory.join(format!("new-{index}"));
        assert!(
            Host::write(&confined, write(destination.clone()), token())
                .await
                .is_err()
        );
        assert!(
            confined
                .preflight_mutations(
                    vec![FileMutation::Write(write(destination.clone()))],
                    token()
                )
                .await
                .is_err()
        );
        assert!(
            confined
                .mutate(FileMutation::Write(write(destination.clone())), token())
                .await
                .is_err()
        );
        assert!(
            FsApi::atomic_write(
                &confined,
                &ctx,
                &destination,
                Bytes::from_static(b"blocked")
            )
            .await
            .is_err()
        );
        assert!(!outside.join(format!("new-{index}")).exists());

        assert_eq!(
            unrestricted
                .read_file(source.clone(), 1024, token())
                .await
                .unwrap(),
            b"needle"
        );
        assert_eq!(
            unrestricted
                .read_text(read(source.clone()), token())
                .await
                .unwrap()
                .content,
            "needle"
        );
        let identity = unrestricted
            .normalize_resource(source.clone(), token())
            .await
            .unwrap();
        assert!(Path::new(&identity).is_absolute());
        assert_eq!(
            Path::new(&identity).canonicalize().unwrap(),
            outside.join("source")
        );
        let point = unrestricted
            .cache_point(cache(source.clone()), token())
            .await
            .unwrap();
        assert_eq!(
            Path::new(&point.path).canonicalize().unwrap(),
            outside.join("source")
        );
        assert_eq!(point.data.unwrap(), b"needle");
        assert!(
            unrestricted
                .glob(glob(directory.clone()), token())
                .await
                .unwrap()
                .entries
                .iter()
                .all(|entry| entry.path.is_absolute())
        );
        assert!(
            unrestricted
                .grep(grep(directory.clone()), token())
                .await
                .unwrap()
                .matches
                .iter()
                .all(|entry| entry.path.is_absolute())
        );
        // A file root uses the same return semantics as recursive traversal.
        assert_eq!(
            unrestricted
                .glob(glob(source.clone()), token())
                .await
                .unwrap()
                .entries[0]
                .path,
            outside.join("source")
        );
        FsApi::stat(&unrestricted, &ctx, &source).await.unwrap();
        assert!(
            !FsApi::list(&unrestricted, &ctx, &directory, ListOptions::default())
                .await
                .unwrap()
                .is_empty()
        );
        let handle = FsApi::open(
            &unrestricted,
            &ctx,
            &source,
            OpenOptions {
                read: true,
                ..OpenOptions::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            FsApi::read(&unrestricted, &ctx, handle, 0, 1024)
                .await
                .unwrap(),
            &b"needle"[..]
        );
        FsApi::close(&unrestricted, &ctx, handle).await.unwrap();
        let plan = unrestricted
            .preflight_mutations(
                vec![FileMutation::Write(write(destination.clone()))],
                token(),
            )
            .await
            .unwrap();
        assert!(matches!(&plan[0], FileMutation::Write(request) if request.path.is_absolute()));
        let created = unrestricted.mutate(plan[0].clone(), token()).await.unwrap();
        let moved = directory.join(format!("moved-{index}"));
        let receipt = unrestricted
            .mutate(
                FileMutation::Move(MoveRequest {
                    path: destination,
                    destination: moved.clone(),
                    expected_version: created.version.unwrap(),
                    content: "needle".into(),
                }),
                token(),
            )
            .await
            .unwrap();
        assert!(receipt.source_removed);
        unrestricted
            .mutate(
                FileMutation::Delete(DeleteRequest {
                    path: moved,
                    expected_version: receipt.version.unwrap(),
                }),
                token(),
            )
            .await
            .unwrap();
        Host::write(
            &unrestricted,
            write(directory.join(format!("text-{index}"))),
            token(),
        )
        .await
        .unwrap();
        FsApi::atomic_write(
            &unrestricted,
            &ctx,
            &directory.join(format!("bytes-{index}")),
            Bytes::from_static(b"bytes"),
        )
        .await
        .unwrap();
    }
    fs::write(root.join("inside"), "needle").unwrap();
    assert_eq!(
        unrestricted
            .normalize_resource("inside".into(), token())
            .await
            .unwrap(),
        "inside"
    );
    assert_eq!(
        unrestricted
            .glob(glob(PathBuf::from(".")), token())
            .await
            .unwrap()
            .entries[0]
            .path,
        PathBuf::from("inside")
    );
}

#[tokio::test]
async fn unrestricted_private_roots_handles_and_cancelled_publication_remain_protected() {
    let fixture = tempfile::tempdir().unwrap();
    let base = fixture.path().canonicalize().unwrap();
    let root = base.join("workspace");
    let private = base.join("private");
    fs::create_dir(&root).unwrap();
    fs::create_dir(&private).unwrap();
    fs::write(private.join("secret"), "needle secret").unwrap();
    let host = host(&root, true);
    let clone = host.clone();
    let ctx = OpContext::default();
    let handle = FsApi::open(
        &host,
        &ctx,
        &private.join("secret"),
        OpenOptions {
            read: true,
            ..OpenOptions::default()
        },
    )
    .await
    .unwrap();
    host.protect_workspace_path(private.clone()).unwrap();
    for path in [private.join("secret"), PathBuf::from("../private/secret")] {
        denied(clone.read_file(path.clone(), 1024, token()).await);
        denied(clone.read_text(read(path.clone()), token()).await);
        denied(clone.normalize_resource(path.clone(), token()).await);
        denied(clone.cache_point(cache(path.clone()), token()).await);
        denied(Host::write(&clone, write(path.clone()), token()).await);
        denied(
            clone
                .preflight_mutations(vec![FileMutation::Write(write(path.clone()))], token())
                .await,
        );
        denied(
            clone
                .mutate(FileMutation::Write(write(path.clone())), token())
                .await,
        );
        assert_eq!(
            FsApi::stat(&clone, &ctx, &path).await.unwrap_err().category,
            ErrorCategory::Permission
        );
        assert_eq!(
            FsApi::atomic_write(&clone, &ctx, &path, Bytes::from_static(b"changed"))
                .await
                .unwrap_err()
                .category,
            ErrorCategory::Permission
        );
    }
    denied(clone.glob(glob(private.clone()), token()).await);
    denied(clone.grep(grep(private.clone()), token()).await);
    assert_eq!(
        FsApi::read(&clone, &ctx, handle, 0, 1024)
            .await
            .unwrap_err()
            .category,
        ErrorCategory::Permission
    );
    let expired = OpContext {
        cancel: token(),
        ..ctx.clone()
    };
    expired.cancel.cancel();
    FsApi::close(&clone, &expired, handle).await.unwrap();
    assert!(
        clone
            .glob(glob(base.clone()), token())
            .await
            .unwrap()
            .entries
            .is_empty()
    );
    assert!(
        clone
            .grep(grep(base.clone()), token())
            .await
            .unwrap()
            .matches
            .is_empty()
    );
    // Reserved private state outside all registered workspaces must also stay private.
    for name in [".microcrab", ".micro-crab"] {
        fs::create_dir(base.join(name)).unwrap();
        fs::write(base.join(name).join("secret"), "needle").unwrap();
        denied(
            clone
                .read_file(base.join(name).join("secret"), 1024, token())
                .await,
        );
    }
    let destination = base.join("cancelled");
    let cancelled = token();
    cancelled.cancel();
    assert!(matches!(
        Host::write(&clone, write(destination.clone()), cancelled.clone()).await,
        Err(ToolError::Cancelled)
    ));
    assert!(
        FsApi::atomic_write(&clone, &expired, &destination, Bytes::from_static(b"never"))
            .await
            .is_err()
    );
    assert!(
        clone
            .mutate(FileMutation::Write(write(destination.clone())), cancelled)
            .await
            .is_err()
    );
    assert!(!destination.exists());
    assert!(!fs::read_dir(&base).unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".microcrab-write-")
    }));
    assert_eq!(fs::read(private.join("secret")).unwrap(), b"needle secret");
}

#[tokio::test]
async fn unrestricted_rejects_devices_ads_links_and_hardlinks() {
    let fixture = tempfile::tempdir().unwrap();
    let base = fixture.path().canonicalize().unwrap();
    let root = base.join("workspace");
    let outside = base.join("outside");
    fs::create_dir(&root).unwrap();
    fs::create_dir(&outside).unwrap();
    fs::write(outside.join("file"), "needle").unwrap();
    let host = host(&root, true);
    for name in [
        "NUL",
        "con.txt",
        "COM1",
        "file:stream",
        "trailing.",
        "NUL/../outside/file",
        "C:relative",
        r"\\.\PhysicalDrive0",
    ] {
        assert!(
            host.read_file(PathBuf::from(name), 1024, token())
                .await
                .is_err(),
            "{name}"
        );
    }
    fs::hard_link(outside.join("file"), outside.join("hard")).unwrap();
    assert!(
        host.read_file(outside.join("hard"), 1024, token())
            .await
            .is_err()
    );
    fs::remove_file(outside.join("hard")).unwrap();
    let link = base.join("link");
    #[cfg(unix)]
    std::os::unix::fs::symlink(&outside, &link).unwrap();
    #[cfg(windows)]
    assert!(
        std::process::Command::new("cmd.exe")
            .args(["/d", "/c", "mklink", "/J"])
            .arg(&link)
            .arg(&outside)
            .status()
            .unwrap()
            .success()
    );
    assert!(
        host.read_file(link.join("file"), 1024, token())
            .await
            .is_err()
    );
    // PathBuf::join on a Windows verbatim base can eliminate link/.. before
    // Host sees it. Send a relative path and prove both segments survive.
    let request = PathBuf::from("../link/../outside/file");
    assert_eq!(
        request.components().collect::<Vec<_>>(),
        vec![
            std::path::Component::ParentDir,
            std::path::Component::Normal(std::ffi::OsStr::new("link")),
            std::path::Component::ParentDir,
            std::path::Component::Normal(std::ffi::OsStr::new("outside")),
            std::path::Component::Normal(std::ffi::OsStr::new("file")),
        ]
    );
    let error = host.read_file(request, 1024, token()).await.unwrap_err();
    assert_eq!(
        error.public_reason(),
        Some("target path contains a disallowed symlink or reparse point")
    );
    assert!(
        FsApi::stat(&host, &OpContext::default(), &link.join("file"))
            .await
            .is_err()
    );
}
