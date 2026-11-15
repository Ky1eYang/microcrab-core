//! Explicit per-path evidence for controlled writes; never a workspace scan.
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Callers select exactly one path and a byte bound before and after a write.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CachePointRequest {
    pub path: PathBuf,
    pub max_bytes: usize,
}

/// Missing files have no data or version; empty files have bytes and a digest.
/// Session owns scope, before/after correlation, retention and round summaries.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileCachePoint {
    /// Normalized portable file identity: workspace-relative by default, or
    /// absolute outside the workspace under explicit unrestricted Host scope.
    /// Evidence never grants authority to reopen or restore this file.
    pub path: String,
    pub data: Option<Vec<u8>>,
    pub version: Option<String>,
}

/// Bound individual captures independently of a caller-provided budget.
pub const MAX_CACHE_POINT_BYTES: usize = 8 * 1024 * 1024;
