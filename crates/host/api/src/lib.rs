//! Composable execution contracts; authorization and auditing belong to Runtime.
mod cache;
mod facade;
mod files;
pub mod filesystem;
pub mod host_reference;
pub mod markdown;
mod mutation;
pub mod net;
mod paths;
pub mod process;
pub mod reference;
pub use paths::{
    UNRESTRICTED_FILESYSTEM_DESCRIPTION, is_normalized_cache_path, is_normalized_file_path,
    is_portable_file_name,
};
pub mod shell;
mod tool;
pub mod tool_api;
pub mod types;
pub mod web_transport;

pub use bytes::Bytes;
pub use cache::{CachePointRequest, FileCachePoint, MAX_CACHE_POINT_BYTES};
pub use facade::HostAPI;
pub use host_reference::{HostMarkdownReference, HostReferenceSpec, HostSkillsReference};
pub use microcrab_protocol::{
    ReferenceCapabilities, ReferenceContent, ReferenceEntry, ReferenceError, ReferenceErrorKind,
    ReferenceInfo, ReferenceLimitDetails, ReferenceNodeKind, ReferencePatch, ReferencePayload,
    ReferenceProjection, ReferenceQuota, ReferenceWrite,
};
pub use reference::{CompositeReference, DynReference, Reference};
pub use files::{
    GlobEntry, GlobEntryKind, GlobIgnoreRuleFile, GlobIgnoreSnapshot, GlobOutput, GlobRequest,
    GlobScanIncomplete, GlobScanReason, GrepMatch, GrepOutput, GrepRequest,
    MAX_GLOB_IGNORE_RULE_BYTES, MAX_GLOB_IGNORE_RULE_FILES, MAX_GLOB_IGNORE_RULE_LINES_PER_FILE,
    MAX_GLOB_IGNORED_PATH_BYTES, MAX_GLOB_IGNORED_PATHS, ReadOutput, ReadRequest,
    ResolvedGlobOutput, WriteOutput, WritePrecondition, WriteRequest, glob_ignore_rule_json_bytes,
    glob_ignored_path_json_bytes, validate_glob_ignore_rules, validate_glob_ignored_paths,
};
pub use filesystem::FsApi;
pub use futures_util::{Stream, stream};
pub use mutation::{
    DeleteRequest, FileMutation, MAX_FILE_MUTATIONS, MAX_MUTATION_BYTES, MoveRequest,
    MutationOutput,
};
pub use net::NetApi;
pub use process::ProcessApi;
pub use shell::{
    ExpiredRange, PrivateShellRequest, ShellApi, ShellCaptureStatus, ShellOutput, ShellPosition,
    ShellRequest,
};
pub use tool::{
    FailureReason, Host, HostInfo, OsFamily, PrivateFileBuffer, ToolError, WorkspaceFile,
    WorkspaceSnapshot,
};
pub use tool_api::FileMutationApi;
pub use types::{
    BoxStream, CancelToken, Capabilities, ErrorCategory, HandleId, HostError, OpContext, Platform,
};
