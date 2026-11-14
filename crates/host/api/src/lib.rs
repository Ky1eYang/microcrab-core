//! Composable execution contracts; authorization and auditing belong to Runtime.
mod cache;
mod facade;
mod files;
pub mod filesystem;
mod mutation;
pub mod net;
pub mod process;
pub mod shell;
mod tool;
pub mod tool_api;
pub mod types;

pub use bytes::Bytes;
pub use cache::{CachePointRequest, FileCachePoint, MAX_CACHE_POINT_BYTES};
pub use facade::HostAPI;
pub use files::{
    GlobEntry, GlobEntryKind, GlobOutput, GlobRequest, GrepMatch, GrepOutput, GrepRequest,
    ReadOutput, ReadRequest, WriteOutput, WritePrecondition, WriteRequest,
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
    ExpiredRange, ShellApi, ShellCaptureStatus, ShellOutput, ShellPosition, ShellRequest,
};
pub use tool::{
    FailureReason, Host, HostInfo, OsFamily, ToolError, WorkspaceFile, WorkspaceSnapshot,
};
pub use tool_api::FileMutationApi;
pub use types::{
    BoxStream, CancelToken, Capabilities, ErrorCategory, HandleId, HostError, OpContext, Platform,
};
