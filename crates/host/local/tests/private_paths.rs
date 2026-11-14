//! Private server storage is denied by Host admission; process execution must be
//! disabled separately because arbitrary OS processes do not use this file policy.
use microcrab_host_api::{
    GlobRequest, GrepRequest, Host, HostAPI, OpContext, ProcessApi, ReadRequest, ShellRequest,
    ToolError, WritePrecondition, WriteRequest,
};
use microcrab_host_local::{LocalHost, ShellLimits};
use std::{fs, path::PathBuf};
use tokio_util::sync::CancellationToken;

fn read(path: &str) -> ReadRequest {
    ReadRequest {
        path: path.into(),
        offset_line: 1,
        max_lines: 100,
        max_bytes: 4096,
    }
}
fn write(path: &str, content: &str) -> WriteRequest {
    WriteRequest {
        path: path.into(),
        content: content.into(),
        precondition: WritePrecondition::Missing,
    }
}
fn glob(root: &str) -> GlobRequest {
    GlobRequest {
        root: root.into(),
        pattern: "**/*".into(),
        include_hidden: true,
        include_ignored: false,
        include_directories: false,
        max_results: 100,
    }
}
fn grep(root: &str) -> GrepRequest {
    GrepRequest {
        root: root.into(),
        pattern: "needle".into(),
        include_glob: None,
        case_sensitive: true,
        literal: true,
        max_matches: 100,
        max_bytes: 4096,
    }
}
fn denied<T>(result: Result<T, ToolError>) {
    let error = result.err().expect("protected operation must fail");
    assert!(matches!(
        &error,
        ToolError::Host(cause) if cause.category == microcrab_host_api::ErrorCategory::Permission
    ));
    assert_eq!(
        error.public_reason(),
        Some("target path is protected by Host private-directory policy")
    );
    for private in ["private bytes", "private configuration", "changed"] {
        assert!(!error.to_string().contains(private));
    }
}
fn token() -> CancellationToken {
    CancellationToken::new()
}

#[tokio::test]
async fn protected_reads_writes_and_normalization_share_policy_across_clones() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("private")).unwrap();
    fs::write(root.path().join("private/state.txt"), "private bytes").unwrap();
    fs::write(root.path().join("server.json"), "private configuration").unwrap();
    fs::write(root.path().join("public.txt"), "public").unwrap();
    let host = LocalHost::new(root.path(), ShellLimits::default()).unwrap();
    let clone = host.clone();
    // Default behavior remains available until the embedder registers protection.
    assert_eq!(
        host.read_text(read("server.json"), token())
            .await
            .unwrap()
            .content,
        "private configuration"
    );
    assert!(host.supports("shell.launch"));
    let version = host
        .read_text(read("private/state.txt"), token())
        .await
        .unwrap()
        .version;
    host.protect_workspace_path("private".into()).unwrap();
    host.protect_workspace_path(root.path().join("server.json"))
        .unwrap();
    for path in ["private/state.txt", "private/missing.txt", "server.json"] {
        denied(clone.normalize_resource(path.into(), token()).await);
        denied(clone.read_text(read(path), token()).await);
        denied(clone.read_file(path.into(), 4096, token()).await);
        denied(clone.write(write(path, "changed"), token()).await);
    }
    denied(
        host.read_file(host.root().join("server.json"), 4096, token())
            .await,
    );
    denied(
        clone
            .write(
                WriteRequest {
                    path: "private/state.txt".into(),
                    content: "changed".into(),
                    precondition: WritePrecondition::Version(version),
                },
                token(),
            )
            .await,
    );
    assert_eq!(
        fs::read_to_string(root.path().join("private/state.txt")).unwrap(),
        "private bytes"
    );
    assert!(!root.path().join("private/missing.txt").exists());
    let escape = host
        .read_file("../outside-private.txt".into(), 4096, token())
        .await
        .unwrap_err();
    assert!(matches!(&escape, ToolError::PathOutsideRoot));
    assert_eq!(
        escape.public_reason(),
        Some("file path is outside the configured root")
    );
    assert_eq!(
        clone
            .read_text(read("public.txt"), token())
            .await
            .unwrap()
            .content,
        "public"
    );
}

#[tokio::test]
async fn searches_prune_private_paths_and_do_not_read_private_ignore_rules() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir_all(root.path().join("private/nested")).unwrap();
    fs::write(
        root.path().join("private/nested/secret.txt"),
        "needle private",
    )
    .unwrap();
    fs::write(root.path().join("server.json"), "needle configuration").unwrap();
    fs::write(root.path().join("public.txt"), "needle public").unwrap();
    // An unprotected ignore file with these bytes would make rule parsing fail.
    fs::write(root.path().join(".gitignore"), [0xff, 0xfe]).unwrap();
    let host = LocalHost::new(root.path(), ShellLimits::default()).unwrap();
    for path in ["private", "server.json", ".gitignore"] {
        host.protect_workspace_path(path.into()).unwrap();
    }
    let listed = host.glob(glob("."), token()).await.unwrap();
    assert_eq!(
        listed.entries,
        vec![microcrab_host_api::GlobEntry {
            path: PathBuf::from("public.txt"),
            kind: microcrab_host_api::GlobEntryKind::File,
        }]
    );
    assert!(!listed.truncated);
    let mut directories = glob(".");
    directories.include_directories = true;
    assert_eq!(
        host.glob(directories, token()).await.unwrap().entries,
        listed.entries
    );
    let mut include_ignored = glob(".");
    include_ignored.include_ignored = true;
    assert_eq!(
        host.glob(include_ignored, token()).await.unwrap().entries,
        listed.entries
    );
    let found = host.grep(grep("."), token()).await.unwrap();
    assert_eq!(found.matches.len(), 1);
    assert_eq!(found.matches[0].path, PathBuf::from("public.txt"));
    assert_eq!(found.matches[0].text, "needle public");
    for path in ["private", "private/nested/secret.txt", "server.json"] {
        denied(host.glob(glob(path), token()).await);
        denied(host.grep(grep(path), token()).await);
    }
}

#[tokio::test]
async fn private_ancestor_denies_whole_root_and_unrelated_private_paths_are_noop() {
    let base = tempfile::tempdir().unwrap();
    fs::create_dir(base.path().join("project")).unwrap();
    fs::write(base.path().join("project/public.txt"), "public").unwrap();
    fs::write(base.path().join("outside.json"), "private").unwrap();
    let host = LocalHost::new(base.path().join("project"), ShellLimits::default()).unwrap();
    host.protect_workspace_path(base.path().join("outside.json"))
        .unwrap();
    assert_eq!(
        host.read_text(read("public.txt"), token())
            .await
            .unwrap()
            .content,
        "public"
    );
    host.protect_workspace_path(base.path().to_path_buf())
        .unwrap();
    denied(host.normalize_resource(".".into(), token()).await);
    denied(host.read_text(read("public.txt"), token()).await);
}

#[cfg(windows)]
#[tokio::test]
async fn deleted_private_file_stays_protected_under_case_aliases() {
    let root = tempfile::tempdir().unwrap();
    fs::write(root.path().join("server.json"), "private").unwrap();
    let host = LocalHost::new(root.path(), ShellLimits::default()).unwrap();
    host.protect_workspace_path("server.json".into()).unwrap();
    fs::remove_file(root.path().join("server.json")).unwrap();
    denied(
        host.write(write("SERVER.JSON", "replacement"), token())
            .await,
    );
    denied(host.normalize_resource("SERVER.JSON".into(), token()).await);
}

#[tokio::test]
async fn disabled_process_capabilities_deny_both_execution_ports() {
    let root = tempfile::tempdir().unwrap();
    let host = LocalHost::new(root.path(), ShellLimits::default()).unwrap();
    let clone = host.clone();
    assert!(clone.supports("process.spawn"));
    assert!(clone.supports("shell.launch"));
    host.disable_process_execution();
    assert!(!clone.supports("process.spawn"));
    assert!(!clone.supports("shell.launch"));
    #[cfg(windows)]
    let command = "Set-Content -LiteralPath 'process-marker.txt' -Value 'executed'";
    #[cfg(not(windows))]
    let command = "printf executed > process-marker.txt";
    let error = microcrab_host_api::ShellApi::launch(
        &clone,
        &OpContext::default(),
        ShellRequest {
            command: command.into(),
            cwd: None,
            output_bytes: 1024,
        },
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.category,
        microcrab_host_api::ErrorCategory::Unsupported
    );
    let error = ProcessApi::spawn(
        &clone,
        &OpContext::default(),
        microcrab_host_api::process::ProcessSpec {
            program: "never-run".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.category,
        microcrab_host_api::ErrorCategory::Unsupported
    );
    assert!(!root.path().join("process-marker.txt").exists());
    assert!(!ProcessApi::has_running_processes(&clone));
}
