//! Typed, bounded file operations and non-following entry discovery. Paths are
//! interpreted by the executing Host relative to its configured root. Default
//! scope confines absolute and relative paths to that root. An explicit Host
//! unrestricted filesystem policy permits external ordinary paths while retaining
//! private-path and platform safety admission.
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadRequest {
    pub path: PathBuf,
    /// One-based first line. Pages contain complete lines, including line endings.
    pub offset_line: usize,
    pub max_lines: usize,
    pub max_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadOutput {
    pub content: String,
    pub start_line: usize,
    /// First unreturned line, or None at EOF. A line larger than the byte budget
    /// fails with FileLimit rather than returning a cursor that cannot advance.
    pub next_line: Option<usize>,
    pub truncated: bool,
    /// Lowercase SHA-256 of the entire file, including bytes outside this page.
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WritePrecondition {
    Missing,
    Version(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteRequest {
    pub path: PathBuf,
    pub content: String,
    pub precondition: WritePrecondition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteOutput {
    pub version: String,
    pub bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GlobRequest {
    pub root: PathBuf,
    /// Globset syntax, matched against slash-separated paths relative to root.
    pub pattern: String,
    pub include_hidden: bool,
    /// When false, honor bounded .gitignore/.ignore rules beneath the search root.
    pub include_ignored: bool,
    /// Include directory and symlink/junction nodes without following links.
    /// Omission preserves ordinary-files-only selection.
    #[serde(default)]
    pub include_directories: bool,
    pub max_results: usize,
}

/// A State-supplied ignore source. `path` is a portable, slash-separated
/// workspace-relative `.gitignore` or `.ignore` file identity, not an IO request.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GlobIgnoreRuleFile {
    pub path: PathBuf,
    pub content: String,
}

impl std::fmt::Debug for GlobIgnoreRuleFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlobIgnoreRuleFile").finish_non_exhaustive()
    }
}

/// A bounded discovery result in the executing Host's workspace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GlobIgnoreSnapshot {
    pub rules: Vec<GlobIgnoreRuleFile>,
    /// The scan hit a resource ceiling; callers must not treat it as complete.
    pub truncated: bool,
    /// At least one ignored directory was pruned. Its unobserved rules cannot
    /// affect this snapshot's interpretation, but reopening it by editing State
    /// does not implicitly recover those rules from disk.
    pub pruned: bool,
}

/// Compact JSON list budget, including escaped text, paths and record syntax.
/// This leaves room for the surrounding State document and native RPC envelope.
pub const MAX_GLOB_IGNORE_RULE_BYTES: usize = 24 * 1024;
pub const MAX_GLOB_IGNORE_RULE_FILES: usize = 256;
pub const MAX_GLOB_IGNORE_RULE_LINES_PER_FILE: usize = 1024;

/// Structural and size validation for untrusted State / carrier evidence.
/// Matching syntax and filesystem policy are enforced by the executing Host.
pub fn validate_glob_ignore_rules(rules: &[GlobIgnoreRuleFile]) -> Result<(), crate::ToolError> {
    use crate::ToolError;
    if rules.len() > MAX_GLOB_IGNORE_RULE_FILES {
        return Err(ToolError::FileLimit);
    }
    let mut paths = std::collections::BTreeSet::new();
    let mut bytes = 2;
    for rule in rules {
        let path = rule.path.to_str().ok_or_else(invalid_rule_path)?;
        if !crate::is_normalized_file_path(path, false)
            || !matches!(path.rsplit('/').next(), Some(".gitignore" | ".ignore"))
        {
            return Err(invalid_rule_path());
        }
        #[cfg(windows)]
        let path_key = path.to_lowercase();
        #[cfg(not(windows))]
        let path_key = path;
        if !paths.insert(path_key) {
            return Err(ToolError::invalid_request(
                "duplicate glob ignore rule file",
            ));
        }
        if rule.content.len() > MAX_GLOB_IGNORE_RULE_BYTES
            || rule.content.lines().count() > MAX_GLOB_IGNORE_RULE_LINES_PER_FILE
        {
            return Err(ToolError::FileLimit);
        }
        if rule.content.contains('\0') {
            return Err(ToolError::invalid_request(
                "ignore rules must not contain NUL",
            ));
        }
        bytes += glob_ignore_rule_json_bytes(path, &rule.content) + usize::from(paths.len() > 1);
        if bytes > MAX_GLOB_IGNORE_RULE_BYTES {
            return Err(ToolError::FileLimit);
        }
    }
    Ok(())
}

fn invalid_rule_path() -> crate::ToolError {
    crate::ToolError::invalid_request(
        "ignore rule path must name a normalized workspace-relative .gitignore or .ignore",
    )
}

#[cfg(test)]
#[test]
fn ignore_rule_byte_accounting_matches_compact_json() {
    let path = "unicode-λ/.ignore";
    let text = "*\n!match\r\n\t\u{1f}\\\"é";
    let rule = GlobIgnoreRuleFile {
        path: path.into(),
        content: text.into(),
    };
    assert_eq!(
        glob_ignore_rule_json_bytes(path, text),
        serde_json::to_vec(&rule).unwrap().len()
    );
}

/// JSON accounting for a single compact rule record (without list separators).
#[doc(hidden)]
pub fn glob_ignore_rule_json_bytes(path: &str, content: &str) -> usize {
    r#"{"path":"","content":""}"#.len()
        + json_string_body_bytes(path)
        + json_string_body_bytes(content)
}

fn json_string_body_bytes(text: &str) -> usize {
    text.bytes()
        .map(|b| match b {
            b'"' | b'\\' | 8 | b'\t' | b'\n' | 12 | b'\r' => 2,
            0..=31 => 6,
            _ => 1,
        })
        .sum()
}

/// Bound the resolved list independently of the rule inputs used at init.
pub const MAX_GLOB_IGNORED_PATHS: usize = 4096;
pub const MAX_GLOB_IGNORED_PATH_BYTES: usize = 24 * 1024;

/// Size of a compact JSON path string, excluding list separators.
#[doc(hidden)]
pub fn glob_ignored_path_json_bytes(path: &str) -> usize {
    2 + json_string_body_bytes(path)
}

/// Validate portable workspace-relative *data* identities, never IO authority.
/// A single trailing slash distinguishes subtree exclusions from exact nodes.
pub fn validate_glob_ignored_paths(paths: &[String]) -> Result<(), crate::ToolError> {
    use crate::ToolError;
    if paths.len() > MAX_GLOB_IGNORED_PATHS {
        return Err(ToolError::operation_limit(
            "glob ignore path list exceeds 4096 paths",
        ));
    }
    let mut distinct = std::collections::BTreeSet::new();
    let mut bytes = 2;
    for path in paths {
        if path.len() > MAX_GLOB_IGNORED_PATH_BYTES - bytes {
            return Err(ToolError::operation_limit(
                "glob ignore path list exceeds 24576 JSON bytes",
            ));
        }
        if !crate::is_normalized_file_path(path.strip_suffix('/').unwrap_or(path), false) {
            return Err(ToolError::invalid_request(
                "ignore path must be a portable workspace-relative path with an optional trailing slash",
            ));
        }
        if !distinct.insert(path) {
            return Err(ToolError::invalid_request("duplicate glob ignore path"));
        }
        bytes += glob_ignored_path_json_bytes(path) + usize::from(distinct.len() > 1);
        if bytes > MAX_GLOB_IGNORED_PATH_BYTES {
            return Err(ToolError::operation_limit(
                "glob ignore path list exceeds 24576 JSON bytes",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
#[test]
fn resolved_ignore_path_limits_count_compact_json_and_reject_invalid_segments() {
    use crate::ToolError;
    assert!(validate_glob_ignored_paths(&[]).is_ok());
    let paths = vec!["dir/".to_owned(), "λ space".to_owned()];
    assert!(validate_glob_ignored_paths(&paths).is_ok());
    assert_eq!(
        2 + paths
            .iter()
            .map(|path| glob_ignored_path_json_bytes(path))
            .sum::<usize>()
            + 1,
        serde_json::to_vec(&paths).unwrap().len()
    );
    assert!(validate_glob_ignored_paths(&["a".repeat(MAX_GLOB_IGNORED_PATH_BYTES - 4)]).is_ok());
    assert!(matches!(
        validate_glob_ignored_paths(&["a".repeat(MAX_GLOB_IGNORED_PATH_BYTES - 3)]),
        Err(ToolError::OperationLimit(_))
    ));
    assert!(matches!(
        validate_glob_ignored_paths(&vec!["x".to_owned(); MAX_GLOB_IGNORED_PATHS + 1]),
        Err(ToolError::OperationLimit(_))
    ));
    for path in [
        "", "../host", "a//", "/root", "a/../b", "**", "a\\b", "C:/b", "a\0b", "x/NUL",
    ] {
        assert!(
            matches!(
                validate_glob_ignored_paths(&[path.to_owned()]),
                Err(ToolError::InvalidRequest(_))
            ),
            "{path:?}"
        );
    }
    assert!(matches!(
        validate_glob_ignored_paths(&["dir/".to_owned(), "dir/".to_owned()]),
        Err(ToolError::InvalidRequest(_))
    ));
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GlobEntryKind {
    File,
    Directory,
    Symlink,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobEntry {
    /// Path relative to the configured Host root, not the search root. When an
    /// explicit unrestricted Host policy admits an external path, it is absolute.
    pub path: PathBuf,
    /// Describes this node; symlink targets are never inspected or traversed.
    pub kind: GlobEntryKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobOutput {
    pub entries: Vec<GlobEntry>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GlobScanReason {
    Entries,
    Depth,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GlobScanIncomplete {
    pub reason: GlobScanReason,
    /// Maximum entry count or maximum descent depth, as identified by `reason`.
    /// The original search root has depth zero.
    pub limit: usize,
    /// The excess observation, including the entry or depth not scanned.
    pub observed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedGlobOutput {
    pub entries: Vec<GlobEntry>,
    /// At least one observed match could not fit in the result budget.
    pub truncated: bool,
    /// The search hit a defensive scan ceiling; absence is not established.
    pub scan_incomplete: Option<GlobScanIncomplete>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrepRequest {
    pub root: PathBuf,
    /// Rust regex syntax unless literal is true; bounded compiled program size.
    pub pattern: String,
    pub include_glob: Option<String>,
    pub case_sensitive: bool,
    pub literal: bool,
    pub max_matches: usize,
    /// Combined UTF-8 match text and path bytes in the result.
    pub max_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrepMatch {
    pub path: PathBuf,
    pub line: usize,
    /// Complete line without CR/LF; binary/non-UTF-8 files are skipped.
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrepOutput {
    pub matches: Vec<GrepMatch>,
    pub truncated: bool,
}
