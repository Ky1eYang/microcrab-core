//! Acceptance tests for typed ordinary-file operations and their failure boundaries.
use microcrab_host_api::{
    GlobEntry, GlobEntryKind, GlobRequest, GrepRequest, Host, ReadRequest, ToolError,
    WritePrecondition, WriteRequest,
};
use microcrab_host_local::{LocalHost, ShellLimits};
use std::{
    fs,
    path::{Path, PathBuf},
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

fn host() -> (TempDir, LocalHost) {
    let root = tempfile::tempdir().unwrap();
    let host = LocalHost::new(root.path(), ShellLimits::default()).unwrap();
    (root, host)
}
fn read(path: &str, offset: usize, lines: usize, bytes: usize) -> ReadRequest {
    ReadRequest {
        path: path.into(),
        offset_line: offset,
        max_lines: lines,
        max_bytes: bytes,
    }
}
fn create(path: &str, content: &str) -> WriteRequest {
    WriteRequest {
        path: path.into(),
        content: content.into(),
        precondition: WritePrecondition::Missing,
    }
}
fn glob(pattern: &str, max_results: usize) -> GlobRequest {
    GlobRequest {
        root: ".".into(),
        pattern: pattern.into(),
        include_hidden: false,
        include_ignored: false,
        include_directories: false,
        max_results,
    }
}
fn grep(pattern: &str) -> GrepRequest {
    GrepRequest {
        root: ".".into(),
        pattern: pattern.into(),
        include_glob: Some("**/*.txt".into()),
        case_sensitive: true,
        literal: false,
        max_matches: 100,
        max_bytes: 1024,
    }
}
fn no_staging(root: &Path) {
    assert!(fs::read_dir(root).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".microcrab-write-")
    }));
}

#[tokio::test]
async fn page_cursor_preserves_complete_lines_and_full_file_version() {
    let (root, host) = host();
    fs::write(root.path().join("page.txt"), "alpha\nβeta\nlast").unwrap();
    let first = host
        .read_text(read("page.txt", 1, 1, 64), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(first.content, "alpha\n");
    assert_eq!(first.next_line, Some(2));
    assert!(first.truncated);
    let second = host
        .read_text(read("page.txt", 2, 10, 6), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(second.content, "βeta\n");
    assert_eq!(second.next_line, Some(3));
    assert_eq!(first.version, second.version);
    let last = host
        .read_text(read("page.txt", 3, 10, 64), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(last.content, "last");
    assert_eq!(last.next_line, None);
    assert!(!last.truncated);
    assert_eq!(first.version, last.version);
    assert_eq!(first.version.len(), 64);
    let past = host
        .read_text(read("page.txt", 20, 10, 64), CancellationToken::new())
        .await
        .unwrap();
    assert!(past.content.is_empty());
    assert_eq!(past.start_line, 20);
    assert_eq!(past.next_line, None);
    assert!(matches!(
        host.read_text(read("page.txt", 1, 1, 2), CancellationToken::new())
            .await,
        Err(ToolError::FileLimit)
    ));
}

#[tokio::test]
async fn oversized_file_rejected_before_full_read_and_large_skipped_line_is_streamed() {
    let (root, host) = host();
    let file = fs::File::create(root.path().join("oversized.txt")).unwrap();
    file.set_len(64 * 1024 * 1024 + 1).unwrap();
    assert!(matches!(
        host.read_text(read("oversized.txt", 1, 1, 20), CancellationToken::new())
            .await,
        Err(ToolError::FileLimit)
    ));
    let mut bytes = vec![b'x'; 2 * 1024 * 1024];
    bytes.extend_from_slice(b"\nsmall\n");
    fs::write(root.path().join("skip.txt"), bytes).unwrap();
    let page = host
        .read_text(read("skip.txt", 2, 1, 6), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(page.content, "small\n");
    assert!(!page.truncated);
}

#[tokio::test]
async fn create_replace_conflict_and_permissions_are_atomic() {
    let (root, host) = host();
    let output = host
        .write(create("file.txt", "initial"), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(output.bytes, 7);
    let page = host
        .read_text(read("file.txt", 1, 2, 64), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(output.version, page.version);
    assert!(matches!(
        host.write(create("file.txt", "bad"), CancellationToken::new())
            .await,
        Err(ToolError::FileConflict)
    ));
    let stale = WriteRequest {
        path: "file.txt".into(),
        content: "bad".into(),
        precondition: WritePrecondition::Version("0".repeat(64)),
    };
    assert!(matches!(
        host.write(stale, CancellationToken::new()).await,
        Err(ToolError::FileConflict)
    ));
    assert_eq!(fs::read(root.path().join("file.txt")).unwrap(), b"initial");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(
            root.path().join("file.txt"),
            fs::Permissions::from_mode(0o640),
        )
        .unwrap();
    }
    let replace = WriteRequest {
        path: "file.txt".into(),
        content: "replacement".into(),
        precondition: WritePrecondition::Version(output.version),
    };
    host.write(replace, CancellationToken::new()).await.unwrap();
    assert_eq!(
        fs::read(root.path().join("file.txt")).unwrap(),
        b"replacement"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(root.path().join("file.txt"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o640
        );
    }
    no_staging(root.path());
}

#[tokio::test]
async fn competing_conditional_writers_have_exactly_one_winner() {
    let (root, host) = host();
    let original = host
        .write(create("race.txt", "original"), CancellationToken::new())
        .await
        .unwrap();
    let one = WriteRequest {
        path: "race.txt".into(),
        content: "one".into(),
        precondition: WritePrecondition::Version(original.version.clone()),
    };
    let two = WriteRequest {
        path: "./race.txt".into(),
        content: "two".into(),
        precondition: WritePrecondition::Version(original.version),
    };
    let clone = host.clone();
    let (one, two) = tokio::join!(
        host.write(one, CancellationToken::new()),
        clone.write(two, CancellationToken::new())
    );
    assert!(matches!(
        (&one, &two),
        (Ok(_), Err(ToolError::FileConflict)) | (Err(ToolError::FileConflict), Ok(_))
    ));
    let data = fs::read(root.path().join("race.txt")).unwrap();
    assert!(data == b"one" || data == b"two");
    no_staging(root.path());
}

#[tokio::test]
async fn dropping_a_write_caller_finishes_cleanup_before_next_same_path_write() {
    let (root, host) = host();
    let writer = host.clone();
    let content = "x".repeat(8 * 1024 * 1024);
    let task = tokio::spawn(async move {
        writer
            .write(
                WriteRequest {
                    path: "drop.txt".into(),
                    content,
                    precondition: WritePrecondition::Missing,
                },
                CancellationToken::new(),
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let staging = fs::read_dir(root.path()).unwrap().any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".microcrab-write-")
            });
            if staging || task.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
    let _ = task.await;
    // Same-path publication serializes behind the detached worker's cleanup.
    let result = host
        .write(create("drop.txt", "replacement"), CancellationToken::new())
        .await;
    match result {
        Ok(_) => assert_eq!(
            fs::read(root.path().join("drop.txt")).unwrap(),
            b"replacement"
        ),
        Err(ToolError::FileConflict) => {
            let data = host
                .read_file("drop.txt".into(), 8 * 1024 * 1024, CancellationToken::new())
                .await
                .unwrap();
            assert_eq!(data.len(), 8 * 1024 * 1024);
            assert!(data.iter().all(|byte| *byte == b'x'));
        }
        other => panic!("unexpected publication result: {other:?}"),
    }
    no_staging(root.path());
}

#[tokio::test]
async fn cancellation_and_invalid_paths_have_no_mutation_side_effects() {
    let (root, host) = host();
    let token = CancellationToken::new();
    token.cancel();
    assert!(matches!(
        host.write(create("new.txt", "data"), token.clone()).await,
        Err(ToolError::Cancelled)
    ));
    assert!(matches!(
        host.glob(glob("**", 1), token.clone()).await,
        Err(ToolError::Cancelled)
    ));
    assert!(matches!(
        host.grep(grep("."), token.clone()).await,
        Err(ToolError::Cancelled)
    ));
    assert!(matches!(
        host.read_text(read("missing", 1, 1, 20), token).await,
        Err(ToolError::Cancelled)
    ));
    for path in [
        "../escape.txt",
        "NUL",
        "CON.txt",
        "file.txt:stream",
        "bad.",
        "missing/child.txt",
    ] {
        assert!(
            host.write(create(path, "data"), CancellationToken::new())
                .await
                .is_err(),
            "{path}"
        );
    }
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn glob_honors_hidden_ignored_and_result_limits() {
    let (root, host) = host();
    fs::create_dir(root.path().join("sub")).unwrap();
    fs::write(root.path().join("a.txt"), "first").unwrap();
    fs::write(root.path().join("sub/b.txt"), "second").unwrap();
    fs::write(root.path().join(".hidden.txt"), "hidden").unwrap();
    fs::write(root.path().join("ignored.txt"), "ignored").unwrap();
    fs::write(root.path().join(".gitignore"), "ignored.txt\n").unwrap();
    let output = host
        .glob(glob("**/*.txt", 10), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        output.entries,
        vec![
            GlobEntry {
                path: PathBuf::from("a.txt"),
                kind: GlobEntryKind::File
            },
            GlobEntry {
                path: PathBuf::from("sub/b.txt"),
                kind: GlobEntryKind::File
            },
        ]
    );
    assert!(!output.truncated);
    let output = host
        .glob(glob("**/*.txt", 1), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(output.entries.len(), 1);
    assert!(output.truncated);
    let mut request = glob("**/*.txt", 10);
    request.include_hidden = true;
    request.include_ignored = true;
    let output = host.glob(request, CancellationToken::new()).await.unwrap();
    assert_eq!(output.entries.len(), 4);
}

#[tokio::test]
async fn glob_directory_opt_in_preserves_patterns_roots_filters_and_shared_limits() {
    let (root, host) = host();
    for directory in ["sub/empty", ".hidden", "ignored"] {
        fs::create_dir_all(root.path().join(directory)).unwrap();
    }
    fs::write(root.path().join("a.txt"), "a").unwrap();
    fs::write(root.path().join("sub/b.txt"), "b").unwrap();
    fs::write(root.path().join("ignored/secret.txt"), "hidden by ignore").unwrap();
    fs::write(root.path().join(".gitignore"), "ignored/\n").unwrap();
    let files = host
        .glob(glob("*", 20), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        files.entries,
        vec![GlobEntry {
            path: "a.txt".into(),
            kind: GlobEntryKind::File
        }]
    );
    let mut request = glob("*", 20);
    request.include_directories = true;
    let top = host
        .glob(request.clone(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        top.entries,
        vec![
            GlobEntry {
                path: "a.txt".into(),
                kind: GlobEntryKind::File
            },
            GlobEntry {
                path: "sub".into(),
                kind: GlobEntryKind::Directory
            },
        ]
    );
    assert!(!top.truncated);
    request.root = "sub".into();
    let nested = host
        .glob(request.clone(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        nested.entries,
        vec![
            GlobEntry {
                path: "sub/b.txt".into(),
                kind: GlobEntryKind::File
            },
            GlobEntry {
                path: "sub/empty".into(),
                kind: GlobEntryKind::Directory
            },
        ]
    );
    request.root = "a.txt".into();
    request.pattern = "*.txt".into();
    assert_eq!(
        host.glob(request.clone(), CancellationToken::new())
            .await
            .unwrap()
            .entries,
        files.entries
    );
    request.root = ".".into();
    request.pattern = "**/*".into();
    request.include_hidden = true;
    request.include_ignored = true;
    let all = host
        .glob(request.clone(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(all.entries.len(), 8);
    assert!(all.entries.contains(&GlobEntry {
        path: ".hidden".into(),
        kind: GlobEntryKind::Directory
    }));
    assert!(all.entries.contains(&GlobEntry {
        path: "ignored/secret.txt".into(),
        kind: GlobEntryKind::File
    }));
    request.max_results = 1;
    let limited = host
        .glob(request.clone(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(limited.entries.len(), 1);
    assert!(limited.truncated);
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(matches!(
        host.glob(request, cancelled).await,
        Err(ToolError::Cancelled)
    ));
}

#[tokio::test]
async fn glob_directory_depth_limit_reports_incomplete_metadata() {
    let (root, host) = host();
    let mut path = root.path().to_path_buf();
    for _ in 0..65 {
        path.push("d");
        fs::create_dir(&path).unwrap();
    }
    let mut request = glob("**/*", 4096);
    request.include_directories = true;
    let output = host.glob(request, CancellationToken::new()).await.unwrap();
    assert!(output.truncated);
    assert_eq!(output.entries.len(), 65);
    assert!(
        output
            .entries
            .iter()
            .all(|entry| entry.kind == GlobEntryKind::Directory)
    );
}

#[cfg(any(unix, windows))]
fn directory_link(target: &Path, link: &Path) {
    #[cfg(unix)]
    std::os::unix::fs::symlink(target, link).unwrap();
    #[cfg(windows)]
    {
        // Junctions exercise reparse nodes without requiring symbolic-link privileges.
        let output = std::process::Command::new("cmd")
            .args(["/d", "/c", "mklink", "/J"])
            .arg(link)
            .arg(target)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "junction fixture failed: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[cfg(any(unix, windows))]
#[tokio::test]
async fn glob_lists_link_nodes_without_target_access_or_traversal() {
    let (root, host) = host();
    let outside = tempfile::tempdir().unwrap();
    fs::write(outside.path().join("secret.txt"), "needle outside").unwrap();
    // Traversing this target would fail ignore decoding before returning any names.
    fs::write(outside.path().join(".gitignore"), [0xff, 0xfe]).unwrap();
    fs::create_dir(root.path().join("private")).unwrap();
    fs::write(root.path().join("private/secret.txt"), "needle private").unwrap();
    host.protect_workspace_path("private".into()).unwrap();
    fs::create_dir(root.path().join("protected-link")).unwrap();
    host.protect_workspace_path("protected-link".into())
        .unwrap();
    fs::remove_dir(root.path().join("protected-link")).unwrap();
    for name in ["out-link", ".hidden-link", "ignored-link", "protected-link"] {
        directory_link(outside.path(), &root.path().join(name));
    }
    directory_link(
        &root.path().join("missing"),
        &root.path().join("dangling-link"),
    );
    directory_link(root.path(), &root.path().join("cycle-link"));
    directory_link(
        &root.path().join("private"),
        &root.path().join("to-private"),
    );
    fs::write(root.path().join(".gitignore"), "ignored-link\n").unwrap();
    assert!(
        host.glob(glob("**/*", 20), CancellationToken::new())
            .await
            .unwrap()
            .entries
            .is_empty()
    );
    let mut request = glob("**/*", 20);
    request.include_directories = true;
    let output = host
        .glob(request.clone(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        output.entries,
        ["cycle-link", "dangling-link", "out-link", "to-private"]
            .into_iter()
            .map(|path| GlobEntry {
                path: path.into(),
                kind: GlobEntryKind::Symlink
            })
            .collect::<Vec<_>>()
    );
    assert!(!output.truncated);
    request.include_hidden = true;
    request.include_ignored = true;
    let all = host
        .glob(request.clone(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(all.entries.len(), 7);
    assert!(
        !all.entries
            .iter()
            .any(|entry| entry.path.starts_with("protected-link")
                || entry.path.starts_with("private"))
    );
    assert!(
        host.grep(grep("needle"), CancellationToken::new())
            .await
            .unwrap()
            .matches
            .is_empty()
    );
    assert!(
        host.read_text(
            read("out-link/secret.txt", 1, 1, 64),
            CancellationToken::new()
        )
        .await
        .is_err()
    );
    assert!(
        host.write(create("out-link/new.txt", "bad"), CancellationToken::new())
            .await
            .is_err()
    );
    request.root = "out-link".into();
    assert!(host.glob(request, CancellationToken::new()).await.is_err());
    assert!(!outside.path().join("new.txt").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn glob_file_symlinks_describe_the_node_and_never_follow_its_contents() {
    let (root, host) = host();
    fs::write(root.path().join("ordinary.txt"), "needle").unwrap();
    std::os::unix::fs::symlink("ordinary.txt", root.path().join("linked.txt")).unwrap();
    std::os::unix::fs::symlink("missing.txt", root.path().join("dangling.txt")).unwrap();
    let mut request = glob("*.txt", 10);
    request.include_directories = true;
    let output = host.glob(request, CancellationToken::new()).await.unwrap();
    assert_eq!(
        output.entries,
        vec![
            GlobEntry {
                path: "dangling.txt".into(),
                kind: GlobEntryKind::Symlink
            },
            GlobEntry {
                path: "linked.txt".into(),
                kind: GlobEntryKind::Symlink
            },
            GlobEntry {
                path: "ordinary.txt".into(),
                kind: GlobEntryKind::File
            },
        ]
    );
    let matches = host
        .grep(grep("needle"), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(matches.matches.len(), 1);
    assert_eq!(matches.matches[0].path, PathBuf::from("ordinary.txt"));
}

#[tokio::test]
async fn grep_structures_regex_literal_case_and_byte_truncation() {
    let (root, host) = host();
    fs::write(root.path().join("a.txt"), "Foo\r\nfoo.\nfood\n").unwrap();
    fs::write(root.path().join("binary.txt"), b"\0foo\n").unwrap();
    fs::write(root.path().join("other.rs"), "foo\n").unwrap();
    let mut request = grep("^foo\\.$");
    let output = host
        .grep(request.clone(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(output.matches.len(), 1);
    assert_eq!(output.matches[0].path, PathBuf::from("a.txt"));
    assert_eq!(output.matches[0].line, 2);
    assert_eq!(output.matches[0].text, "foo.");
    request.pattern = "FOO.".into();
    request.literal = true;
    request.case_sensitive = false;
    assert_eq!(
        host.grep(request, CancellationToken::new())
            .await
            .unwrap()
            .matches
            .len(),
        1
    );
    let mut request = grep("foo");
    request.max_matches = 1;
    let output = host.grep(request, CancellationToken::new()).await.unwrap();
    assert_eq!(output.matches.len(), 1);
    assert!(output.truncated);
    let mut request = grep("foo");
    request.max_bytes = 1;
    let output = host.grep(request, CancellationToken::new()).await.unwrap();
    assert!(output.matches.is_empty());
    assert!(output.truncated);
    assert!(
        host.grep(grep("["), CancellationToken::new())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn traversal_budget_counts_nonmatching_entries() {
    let (root, host) = host();
    for n in 0..16_385 {
        fs::write(root.path().join(format!("item-{n}")), b"").unwrap();
    }
    let output = host
        .glob(glob("*.absent", 10), CancellationToken::new())
        .await
        .unwrap();
    assert!(output.entries.is_empty());
    assert!(output.truncated);
}

#[cfg(unix)]
#[tokio::test]
async fn links_never_expose_or_replace_their_targets() {
    let (root, host) = host();
    let outside = tempfile::tempdir().unwrap();
    fs::write(outside.path().join("secret.txt"), "secret").unwrap();
    std::os::unix::fs::symlink(outside.path(), root.path().join("link")).unwrap();
    assert!(matches!(
        host.read_text(read("link/secret.txt", 1, 1, 64), CancellationToken::new())
            .await,
        Err(ToolError::Host(ref error)) if error.public_reason.as_deref() == Some("target path contains a disallowed symlink or reparse point")
    ));
    assert!(
        host.write(create("link/new.txt", "bad"), CancellationToken::new())
            .await
            .is_err()
    );
    assert!(
        host.glob(glob("**/*.txt", 10), CancellationToken::new())
            .await
            .unwrap()
            .entries
            .is_empty()
    );
    assert_eq!(
        fs::read(outside.path().join("secret.txt")).unwrap(),
        b"secret"
    );
    assert!(!outside.path().join("new.txt").exists());
}

#[tokio::test]
async fn normalized_resources_are_root_relative_and_allow_missing_write_destinations() {
    let (root, host) = host();
    fs::create_dir(root.path().join("sub")).unwrap();
    assert_eq!(
        host.normalize_resource(".".into(), CancellationToken::new())
            .await
            .unwrap(),
        "."
    );
    assert_eq!(
        host.normalize_resource("./sub/new.txt".into(), CancellationToken::new())
            .await
            .unwrap(),
        "sub/new.txt"
    );
    assert_eq!(
        host.normalize_resource(
            root.path().canonicalize().unwrap().join("sub/new.txt"),
            CancellationToken::new()
        )
        .await
        .unwrap(),
        "sub/new.txt"
    );
    assert!(matches!(
        host.normalize_resource("../outside".into(), CancellationToken::new())
            .await,
        Err(ToolError::PathOutsideRoot)
    ));
}

#[tokio::test]
async fn availability_is_independent_of_guest_linked_imports() {
    let (_root, host) = host();
    assert!(host.info().unwrap().capabilities.is_empty());
    for capability in ["read", "write", "glob", "grep"] {
        assert!(
            host.tool_capabilities()
                .iter()
                .any(|value| value == capability)
        );
    }
    for operation in [
        "process.spawn",
        "process.status",
        "process.wait",
        "process.release",
        "shell.launch",
        "shell.read",
        "shell.drain",
    ] {
        assert!(
            microcrab_host_api::HostAPI::supports(&host, operation),
            "{operation}"
        );
    }
    assert!(!microcrab_host_api::HostAPI::supports(
        &host,
        "process.resize-pty"
    ));
}
