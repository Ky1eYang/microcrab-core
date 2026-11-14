//! Typed, bounded file operations and non-following entry discovery. Paths are
//! interpreted by the executing Host, relative to its configured root (absolute
//! paths must remain within it).
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GlobEntryKind {
    File,
    Directory,
    Symlink,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobEntry {
    /// Path relative to the configured Host root, not the search root.
    pub path: PathBuf,
    /// Describes this node; symlink targets are never inspected or traversed.
    pub kind: GlobEntryKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobOutput {
    pub entries: Vec<GlobEntry>,
    pub truncated: bool,
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
