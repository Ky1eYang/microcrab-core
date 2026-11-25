use super::*;
use crate::search::glob_ignored_paths;
use std::path::PathBuf;

fn temp_root() -> (tempfile::TempDir, PathBuf) {
    let owner = tempfile::tempdir().unwrap();
    let root = owner.path().canonicalize().unwrap();
    (owner, root)
}

fn put(root: &Path, path: &str, content: &str) {
    let path = root.join(path);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, content).unwrap();
}

fn request(pattern: &str) -> GlobRequest {
    GlobRequest {
        root: ".".into(),
        pattern: pattern.into(),
        include_hidden: false,
        include_ignored: false,
        include_directories: false,
        max_results: 64,
    }
}

fn selected(out: &ResolvedGlobOutput) -> Vec<&Path> {
    out.entries
        .iter()
        .map(|entry| entry.path.as_path())
        .collect()
}

#[test]
fn resolves_order_and_only_existing_nodes_without_opening_pruned_parents() {
    let (_owner, root) = temp_root();
    put(
        &root,
        ".gitignore",
        "*.bin\nclosed/\n!closed/reopened.bin\nfolder-name\nnever-born\n",
    );
    put(&root, ".ignore", "!root.bin\nignore-wins\n");
    put(&root, "root.bin", "");
    put(&root, "ignore-wins", "");
    put(&root, "closed/.ignore", "!reopened.bin\n");
    put(&root, "closed/reopened.bin", "");
    put(&root, "folder-name", "");
    put(&root, "nested/.gitignore", "!retained.bin\nfile-only\n");
    put(&root, "nested/.ignore", "reexcluded.bin\n");
    put(&root, "nested/retained.bin", "");
    put(&root, "nested/reexcluded.bin", "");
    put(&root, "nested/file-only", "");
    put(&root, "private/would-match.bin", "");
    let policy = Policy {
        private: [root.join("private")].into_iter().collect(),
        ..Policy::default()
    };
    let token = CancellationToken::new();
    let list = glob_ignored_paths(&root, &policy, &token).unwrap();
    assert_eq!(
        list,
        [
            "closed/",
            "folder-name",
            "ignore-wins",
            "nested/file-only",
            "nested/reexcluded.bin"
        ]
    );
    put(&root, "nested/new.bin", "");
    put(&root, "closed/new.bin", "");
    put(&root, "closed-elsewhere/new.bin", "");
    fs::remove_file(root.join("folder-name")).unwrap();
    put(&root, "folder-name/new.bin", "");
    let out = search(&root, &policy, request("**"), list.clone(), &token).unwrap();
    let got = selected(&out);
    for path in [
        "root.bin",
        "nested/retained.bin",
        "closed-elsewhere/new.bin",
        "nested/new.bin",
        "folder-name/new.bin",
    ] {
        assert!(got.contains(&Path::new(path)), "missing {path}");
    }
    assert!(!got.contains(&Path::new("closed/new.bin")));
    assert!(!got.contains(&Path::new("private/would-match.bin")));
    let mut named_root = request("**");
    named_root.root = "closed".into();
    assert!(
        search(&root, &policy, named_root.clone(), list, &token)
            .unwrap()
            .entries
            .is_empty()
    );
    assert!(
        !search(&root, &policy, named_root, vec![], &token)
            .unwrap()
            .entries
            .is_empty()
    );
}

#[test]
fn exact_and_prefix_prune_large_irrelevant_tree_and_keep_original_match_root() {
    let (_owner, root) = temp_root();
    put(&root, "needle/inner/match.txt", "");
    put(&root, "needle/peer.txt", "");
    put(&root, "needle-copy/inner/match.txt", "");
    fs::create_dir(root.join("irrelevant")).unwrap();
    for i in 0..ENTRIES {
        fs::File::create(root.join("irrelevant").join(i.to_string())).unwrap();
    }
    let policy = Policy::default();
    let token = CancellationToken::new();
    for pattern in [
        "needle/inner/match.txt",
        "needle/**/match.txt",
        "needle/inner/*.txt",
    ] {
        let out = search(&root, &policy, request(pattern), vec![], &token).unwrap();
        assert_eq!(
            selected(&out),
            [Path::new("needle/inner/match.txt")],
            "{pattern}"
        );
        assert!(!out.truncated);
        assert_eq!(out.scan_incomplete, None);
    }
    let out = search(&root, &policy, request("**/*.absent"), vec![], &token).unwrap();
    assert!(out.entries.is_empty() && !out.truncated);
    assert_eq!(
        out.scan_incomplete,
        Some(GlobScanIncomplete {
            reason: GlobScanReason::Entries,
            limit: ENTRIES,
            observed: ENTRIES + 1
        })
    );
    assert!(glob_ignored_paths(&root, &policy, &token).is_err());
}

#[test]
fn limit_reports_distinguish_omitted_output_from_holes_in_the_scan() {
    let (_owner, root) = temp_root();
    put(&root, "a.hit", "");
    put(&root, "b.hit", "");
    let deep = format!("{}end", "d/".repeat(DEPTH + 1));
    put(&root, &deep, "");
    let policy = Policy::default();
    let token = CancellationToken::new();
    let mut req = request("*.hit");
    req.max_results = 1;
    let out = search(&root, &policy, req, vec!["d/".into()], &token).unwrap();
    assert_eq!(out.entries.len(), 1);
    assert!(out.truncated);
    assert_eq!(out.scan_incomplete, None);
    let out = search(&root, &policy, request("**/*.missing"), vec![], &token).unwrap();
    assert!(!out.truncated);
    assert_eq!(
        out.scan_incomplete,
        Some(GlobScanIncomplete {
            reason: GlobScanReason::Depth,
            limit: DEPTH,
            observed: DEPTH + 1
        })
    );
    for prefix in [
        "../host", "a//", "/root", "a/../b", "**", "a\\b", "C:/b", "a\0b",
    ] {
        assert!(search(&root, &policy, request("**"), vec![prefix.into()], &token).is_err());
    }
}

#[cfg(unix)]
#[test]
fn literal_plan_preserves_hidden_links_and_protected_nodes() {
    let (_owner, root) = temp_root();
    put(&root, "private/secret", "");
    put(&root, ".hidden/file", "");
    std::os::unix::fs::symlink("private", root.join("link")).unwrap();
    let policy = Policy {
        private: [root.join("private")].into_iter().collect(),
        ..Policy::default()
    };
    let token = CancellationToken::new();
    for pattern in ["link/**", "private/secret", ".hidden/file"] {
        assert!(
            search(&root, &policy, request(pattern), vec![], &token)
                .unwrap()
                .entries
                .is_empty()
        );
    }
    let mut req = request("link");
    req.include_directories = true;
    let out = search(&root, &policy, req.clone(), vec!["link/".into()], &token).unwrap();
    assert_eq!(out.entries[0].kind, GlobEntryKind::Symlink);
    assert!(
        search(&root, &policy, req, vec!["link".into()], &token)
            .unwrap()
            .entries
            .is_empty()
    );
    token.cancel();
    assert!(matches!(
        search(&root, &policy, request("link"), vec![], &token),
        Err(ToolError::Cancelled)
    ));
}
