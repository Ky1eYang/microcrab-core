//! Conditional ordinary-file mutations. A preflight is validation, never a reservation
//! against external writers or a transaction spanning several files. Destinations
//! require existing admitted parent directories; mutations never create directories.
use crate::WriteRequest;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub const MAX_FILE_MUTATIONS: usize = 128;
pub const MAX_MUTATION_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileMutation {
    Write(WriteRequest),
    Delete(DeleteRequest),
    Move(MoveRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteRequest {
    pub path: PathBuf,
    pub expected_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MoveRequest {
    pub path: PathBuf,
    pub destination: PathBuf,
    pub expected_version: String,
    /// Final UTF-8 destination content. The destination must be missing.
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationOutput {
    /// Destination version for write/move; absent after delete.
    pub version: Option<String>,
    /// Published destination bytes, or zero for delete.
    pub bytes: usize,
    pub source_removed: bool,
    pub destination_written: bool,
    /// Sanitized failure after destination publication, at most 1024 UTF-8 bytes.
    /// A partial move retains both files; callers must not replay automatically.
    pub diagnostic: Option<String>,
}
