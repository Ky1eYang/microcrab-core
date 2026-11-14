//! Conditional multi-file contracts; these tests intentionally do not claim an
//! OS CAS against external writers or a transaction across several mutations.
use microcrab_host_api::{
    DeleteRequest, FileMutation, Host, MoveRequest, ToolError, WritePrecondition, WriteRequest,
};
use microcrab_host_local::{LocalHost, ShellLimits};
use sha2::{Digest, Sha256};
use std::{fs, path::PathBuf};
use tokio_util::sync::CancellationToken;

fn host() -> (tempfile::TempDir, LocalHost) {
    let root = tempfile::tempdir().unwrap();
    let host = LocalHost::new_filesystem(root.path(), ShellLimits::default()).unwrap();
    (root, host)
}
fn digest(content: &str) -> String {
    format!("{:x}", Sha256::digest(content.as_bytes()))
}
fn add(path: &str, content: &str) -> FileMutation {
    FileMutation::Write(WriteRequest {
        path: path.into(),
        content: content.into(),
        precondition: WritePrecondition::Missing,
    })
}
fn delete(path: &str, content: &str) -> FileMutation {
    FileMutation::Delete(DeleteRequest {
        path: path.into(),
        expected_version: digest(content),
    })
}
fn moving(path: &str, destination: &str, before: &str, after: &str) -> FileMutation {
    FileMutation::Move(MoveRequest {
        path: path.into(),
        destination: destination.into(),
        expected_version: digest(before),
        content: after.into(),
    })
}
fn no_staging(root: &std::path::Path) {
    assert!(fs::read_dir(root).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".microcrab-")
    }));
}

#[tokio::test]
async fn operation_and_complete_source_bounds_fail_before_effects() {
    let (root, host) = host();
    let requests = (0..=microcrab_host_api::MAX_FILE_MUTATIONS)
        .map(|index| add(&format!("new-{index}"), ""))
        .collect();
    assert!(matches!(
        Host::preflight_mutations(&host, requests, CancellationToken::new()).await,
        Err(ToolError::InvalidRequest(_))
    ));
    let file = fs::File::create(root.path().join("large")).unwrap();
    file.set_len(microcrab_host_api::MAX_MUTATION_BYTES as u64 + 1)
        .unwrap();
    drop(file);
    assert!(matches!(
        Host::preflight_mutations(
            &host,
            vec![add("first", "x"), delete("large", "wrong")],
            CancellationToken::new()
        )
        .await,
        Err(ToolError::FileLimit)
    ));
    assert!(!root.path().join("first").exists());
    no_staging(root.path());
}

#[tokio::test]
async fn whole_list_preflight_has_no_effect_even_when_last_version_fails() {
    let (root, host) = host();
    fs::write(root.path().join("source"), "before\r\n").unwrap();
    let requests = vec![add("new", "created"), delete("source", "stale")];
    assert!(matches!(
        Host::preflight_mutations(&host, requests, CancellationToken::new()).await,
        Err(ToolError::FileConflict)
    ));
    assert!(!root.path().join("new").exists());
    assert_eq!(fs::read(root.path().join("source")).unwrap(), b"before\r\n");
    no_staging(root.path());
}

#[tokio::test]
async fn normalized_preflight_then_sequential_write_move_delete_report_exact_phases() {
    let (root, host) = host();
    fs::write(root.path().join("source"), "before\r\n").unwrap();
    let requests = Host::preflight_mutations(
        &host,
        vec![
            add("./new", "new\r\n"),
            moving("./source", "./destination", "before\r\n", "after\r\n"),
        ],
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(
        matches!(&requests[0], FileMutation::Write(request) if request.path == PathBuf::from("new"))
    );
    assert!(!root.path().join("new").exists());
    let first = Host::mutate(&host, requests[0].clone(), CancellationToken::new())
        .await
        .unwrap();
    assert!(first.destination_written && !first.source_removed);
    let second = Host::mutate(&host, requests[1].clone(), CancellationToken::new())
        .await
        .unwrap();
    assert!(second.destination_written && second.source_removed && second.diagnostic.is_none());
    assert_eq!(second.version, Some(digest("after\r\n")));
    assert_eq!(second.bytes, 7);
    assert!(!root.path().join("source").exists());
    assert_eq!(
        fs::read(root.path().join("destination")).unwrap(),
        b"after\r\n"
    );
    let removed = Host::mutate(
        &host,
        delete("destination", "after\r\n"),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(removed.source_removed && !removed.destination_written);
    assert_eq!(removed.version, None);
    assert_eq!(removed.bytes, 0);
    no_staging(root.path());
}

#[tokio::test]
async fn preflight_rejects_aliases_cycles_and_missing_parents() {
    let (root, host) = host();
    fs::write(root.path().join("source"), "before").unwrap();
    for requests in [
        vec![add("new", "a"), add("./new", "b")],
        vec![moving("source", "source", "before", "after")],
        vec![
            moving("source", "new", "before", "after"),
            add("new", "collision"),
        ],
        vec![add("missing-parent/child", "content")],
    ] {
        assert!(
            Host::preflight_mutations(&host, requests, CancellationToken::new())
                .await
                .is_err()
        );
    }
    assert_eq!(
        fs::read_to_string(root.path().join("source")).unwrap(),
        "before"
    );
    assert!(!root.path().join("new").exists());
    assert!(!root.path().join("missing-parent").exists());
    no_staging(root.path());
}

#[tokio::test]
async fn commit_revalidates_source_and_destination_and_preserves_earlier_prefix() {
    let (root, host) = host();
    fs::write(root.path().join("source"), "before").unwrap();
    let requests = Host::preflight_mutations(
        &host,
        vec![
            add("first", "committed"),
            moving("source", "destination", "before", "after"),
        ],
        CancellationToken::new(),
    )
    .await
    .unwrap();
    Host::mutate(&host, requests[0].clone(), CancellationToken::new())
        .await
        .unwrap();
    fs::write(root.path().join("destination"), "unrelated").unwrap();
    assert!(matches!(
        Host::mutate(&host, requests[1].clone(), CancellationToken::new()).await,
        Err(ToolError::FileConflict)
    ));
    assert_eq!(
        fs::read_to_string(root.path().join("first")).unwrap(),
        "committed"
    );
    assert_eq!(
        fs::read_to_string(root.path().join("destination")).unwrap(),
        "unrelated"
    );
    assert_eq!(
        fs::read_to_string(root.path().join("source")).unwrap(),
        "before"
    );
    fs::write(root.path().join("source"), "external edit").unwrap();
    assert!(matches!(
        Host::mutate(&host, delete("source", "before"), CancellationToken::new()).await,
        Err(ToolError::FileConflict)
    ));
    no_staging(root.path());
}

#[tokio::test]
async fn private_paths_escape_binary_and_cancellation_fail_before_effect() {
    let (root, host) = host();
    fs::create_dir(root.path().join("private")).unwrap();
    fs::write(root.path().join("private/secret"), "secret").unwrap();
    host.protect_workspace_path("private".into()).unwrap();
    fs::write(root.path().join("source"), "before").unwrap();
    fs::write(root.path().join("binary"), [0xff, 0xfe]).unwrap();
    for request in [
        add("../outside", "x"),
        add("private/new", "x"),
        delete("private/secret", "secret"),
        moving("source", "private/new", "before", "x"),
    ] {
        assert!(
            Host::mutate(&host, request, CancellationToken::new())
                .await
                .is_err()
        );
    }
    let binary_digest = format!("{:x}", Sha256::digest([0xff, 0xfe]));
    assert!(matches!(
        Host::preflight_mutations(
            &host,
            vec![FileMutation::Delete(DeleteRequest {
                path: "binary".into(),
                expected_version: binary_digest
            })],
            CancellationToken::new()
        )
        .await,
        Err(ToolError::InvalidRequest(_))
    ));
    let token = CancellationToken::new();
    token.cancel();
    assert!(matches!(
        Host::mutate(
            &host,
            moving("source", "destination", "before", "after"),
            token
        )
        .await,
        Err(ToolError::Cancelled)
    ));
    assert!(!root.path().join("destination").exists());
    assert_eq!(
        fs::read_to_string(root.path().join("source")).unwrap(),
        "before"
    );
    no_staging(root.path());
}

#[tokio::test]
async fn racing_move_and_write_share_source_lock_and_only_one_version_wins() {
    let (root, host) = host();
    fs::write(root.path().join("source"), "before").unwrap();
    let write = FileMutation::Write(WriteRequest {
        path: "source".into(),
        content: "changed".into(),
        precondition: WritePrecondition::Version(digest("before")),
    });
    let (moved, written) = tokio::join!(
        Host::mutate(
            &host,
            moving("source", "destination", "before", "after"),
            CancellationToken::new()
        ),
        Host::mutate(&host, write, CancellationToken::new())
    );
    assert_ne!(moved.is_ok(), written.is_ok());
    if moved.is_ok() {
        assert!(!root.path().join("source").exists());
    } else {
        assert_eq!(
            fs::read_to_string(root.path().join("source")).unwrap(),
            "changed"
        );
        assert!(!root.path().join("destination").exists());
    }
    no_staging(root.path());
}

#[cfg(windows)]
#[tokio::test]
async fn source_unlink_sharing_violation_keeps_committed_destination_in_receipt() {
    use std::os::windows::fs::OpenOptionsExt;

    let (root, host) = host();
    let source = root.path().join("source");
    fs::write(&source, "before").unwrap();
    // Keep ordinary source reads available through preflight and publication,
    // while the OS rejects unlink because this handle omits FILE_SHARE_DELETE.
    // A read-only attribute alone does not prevent modern Windows remove_file.
    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    let source_handle = fs::OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .open(&source)
        .unwrap();
    let output = Host::mutate(
        &host,
        moving("source", "destination", "before", "after"),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(output.destination_written && !output.source_removed);
    assert_eq!(output.version, Some(digest("after")));
    assert_eq!(output.bytes, 5);
    assert!(
        output
            .diagnostic
            .as_ref()
            .is_some_and(|reason| reason.contains("source removal failed") && reason.len() <= 1024)
    );
    assert_eq!(fs::read_to_string(&source).unwrap(), "before");
    assert_eq!(
        fs::read_to_string(root.path().join("destination")).unwrap(),
        "after"
    );
    drop(source_handle);
    no_staging(root.path());
}

#[cfg(unix)]
#[tokio::test]
async fn symlink_and_hardlink_mutations_are_rejected() {
    let (root, host) = host();
    fs::write(root.path().join("source"), "before").unwrap();
    std::os::unix::fs::symlink(root.path().join("source"), root.path().join("link")).unwrap();
    assert!(
        Host::mutate(&host, delete("link", "before"), CancellationToken::new())
            .await
            .is_err()
    );
    fs::hard_link(root.path().join("source"), root.path().join("hard")).unwrap();
    assert!(
        Host::mutate(
            &host,
            moving("source", "destination", "before", "after"),
            CancellationToken::new()
        )
        .await
        .is_err()
    );
    assert!(!root.path().join("destination").exists());
}
