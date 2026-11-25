use super::*;
use crate::{LocalHost, ShellLimits};
use microcrab_host_api::{GlobRequest, Host, OpContext};

fn put(root: &Path, path: &str, bytes: impl AsRef<[u8]>) {
    let path = root.join(path);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, bytes).unwrap();
}

fn request(root: &str, pattern: &str) -> GlobRequest {
    GlobRequest {
        root: root.into(),
        pattern: pattern.into(),
        include_hidden: false,
        include_ignored: false,
        include_directories: false,
        max_results: 100,
    }
}

fn host(root: &Path) -> LocalHost {
    LocalHost::new(root, ShellLimits::default()).unwrap()
}

fn rule(path: &str, content: &str) -> GlobIgnoreRuleFile {
    GlobIgnoreRuleFile {
        path: path.into(),
        content: content.into(),
    }
}

#[tokio::test]
async fn snapshot_prunes_ignored_tree_without_claiming_its_coverage() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    put(root, ".gitignore", "/build/\n");
    put(root, "build/.ignore", "*.txt\n");
    put(root, "build/wanted.txt", "file");
    put(root, ".hidden/.gitignore", "*.bin\n");
    // A deep default-ignored tree exceeds the scan depth if traversed.
    put(root, &format!("build/{}overflow", "d/".repeat(65)), "file");
    let host = host(root);
    let ctx = OpContext::default();
    let snapshot = host.glob_ignore_snapshot(&ctx).await.unwrap();
    assert!(snapshot.pruned);
    assert!(!snapshot.truncated);
    assert_eq!(
        snapshot.rules,
        [
            rule(".gitignore", "/build/\n"),
            rule(".hidden/.gitignore", "*.bin\n")
        ]
    );
    assert!(
        host.glob_with_ignore_rules(
            &ctx,
            request("build/wanted.txt", "*"),
            snapshot.rules.clone()
        )
        .await
        .unwrap()
        .entries
        .is_empty()
    );
    // Rules are State data; reopening a directory does not reread its .ignore.
    let mut amended = snapshot.rules;
    amended[0].content.clear();
    let out = host
        .glob_with_ignore_rules(&ctx, request("build/wanted.txt", "*"), amended)
        .await
        .unwrap();
    assert_eq!(out.entries[0].path, Path::new("build/wanted.txt"));
}

#[tokio::test]
async fn scoped_patterns_and_precedence_filter_before_result_limit() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    // Deliberately provide the higher-precedence source first in the DTO.
    let rules = vec![
        rule(".ignore", "!overridden.tmp\n"),
        rule(
            "nested/.gitignore",
            "\u{feff}!keep.tmp\r\n/anchored.txt\n\\#comment.txt\n\\!negation.txt\ntrail\\ \n",
        ),
        rule(".gitignore", "*.tmp\nblocked/\nignored/**\n"),
        rule("blocked/.ignore", "!rescued.txt\n"),
    ];
    for path in [
        "overridden.tmp",
        "nested/keep.tmp",
        "nested/skip.tmp",
        "nested/anchored.txt",
        "nested/sub/anchored.txt",
        "nested/#comment.txt",
        "nested/!negation.txt",
        "blocked/rescued.txt",
        "ignored/many.txt",
        "visible.txt",
    ] {
        put(root, path, "file");
    }
    // Even a conflicting directory-local .ignore on disk has no authority.
    put(root, "nested/.ignore", "*\n");
    let host = host(root);
    let ctx = OpContext::default();
    let output = host
        .glob_with_ignore_rules(&ctx, request(".", "**"), rules.clone())
        .await
        .unwrap();
    assert_eq!(
        output
            .entries
            .into_iter()
            .map(|e| e.path)
            .collect::<Vec<_>>(),
        [
            "nested/keep.tmp",
            "nested/sub/anchored.txt",
            "overridden.tmp",
            "visible.txt"
        ]
        .map(PathBuf::from)
    );
    let mut capped = request(".", "**/*.txt");
    capped.max_results = 2;
    let result = host
        .glob_with_ignore_rules(&ctx, capped, rules.clone())
        .await
        .unwrap();
    assert_eq!(result.entries.len(), 2);
    assert!(!result.truncated);
    let output = host
        .glob_with_ignore_rules(&ctx, request("nested", "**/*.tmp"), rules)
        .await
        .unwrap();
    assert_eq!(output.entries[0].path, Path::new("nested/keep.tmp"));
    assert_eq!(output.entries.len(), 1);
    let output = host
        .glob_with_ignore_rules(&ctx, request("nested", "**/*.tmp"), vec![])
        .await
        .unwrap();
    assert_eq!(output.entries.len(), 2);
}

#[tokio::test]
async fn protected_paths_and_links_are_not_observed_even_when_bypassing_rules() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    put(root, "private/.ignore", "content");
    put(root, "visible.txt", "file");
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(root.join("private"), root.join("alias")).unwrap();
        std::os::unix::fs::symlink(root.join("private/.ignore"), root.join(".ignore")).unwrap();
    }
    let host = host(root);
    host.protect_workspace_path(root.join("private")).unwrap();
    let ctx = OpContext::default();
    assert!(
        host.glob_ignore_snapshot(&ctx)
            .await
            .unwrap()
            .rules
            .is_empty()
    );
    let mut bypass = request(".", "**");
    bypass.include_ignored = true;
    assert_eq!(
        host.glob_with_ignore_rules(&ctx, bypass, vec![rule("../.gitignore", "*")])
            .await
            .unwrap()
            .entries[0]
            .path,
        Path::new("visible.txt")
    );
    assert!(
        host.glob_with_ignore_rules(&ctx, request(".", "**"), vec![rule("../.gitignore", "*")])
            .await
            .is_err()
    );
    assert!(
        host.glob_with_ignore_rules(&ctx, request(".", "**"), vec![rule("private/.ignore", "*")])
            .await
            .is_err()
    );
}

#[tokio::test]
async fn budget_and_invalid_evidence_are_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let host = host(root);
    let ctx = OpContext::default();
    put(root, ".ignore", "\\".repeat(MAX_GLOB_IGNORE_RULE_BYTES / 2));
    assert!(host.glob_ignore_snapshot(&ctx).await.unwrap().truncated);
    put(root, ".ignore", [255]);
    assert!(matches!(
        host.glob_ignore_snapshot(&ctx).await,
        Err(ToolError::InvalidRequest(_))
    ));
    put(root, ".ignore", "[z-a]\n");
    assert!(matches!(
        host.glob_ignore_snapshot(&ctx).await,
        Err(ToolError::InvalidRequest(_))
    ));
    let cancelled = OpContext::default();
    cancelled.cancel.cancel();
    assert!(
        matches!(host.glob_ignore_snapshot(&cancelled).await, Err(ToolError::Host(error)) if error.category == microcrab_host_api::ErrorCategory::Cancelled)
    );
}
