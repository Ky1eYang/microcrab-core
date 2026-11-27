//! File and workspace convenience facade; execution belongs to general owner ports.
use crate::{CachePointRequest, FileCachePoint, HostAPI, files::*};
use async_trait::async_trait;
pub use microcrab_agent_types::{HostInfo, OsFamily};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

/// Historical full-workspace bytes retained only for durable recovery decoding.
/// Current Hosts expose bounded cache points; these bytes do not authorize restoration.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkspaceSnapshot {
    pub files: Vec<WorkspaceFile>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkspaceFile {
    pub path: PathBuf,
    pub data: Vec<u8>,
}

/// Producer feedback with a bounded sanitized public description.
/// Producers must exclude bodies before constructing diagnostic text.
#[derive(Clone)]
pub struct FailureReason {
    message: String,
    public_reason: Option<String>,
}
impl FailureReason {
    pub fn known(reason: &'static str) -> Self {
        Self {
            message: reason.into(),
            public_reason: Some(microcrab_protocol::diagnostic::sanitize(reason, 2048)),
        }
    }
    pub fn with_public_reason(mut self, reason: &str) -> Self {
        self.public_reason = Some(microcrab_protocol::diagnostic::sanitize(reason, 2048));
        self
    }
    pub fn public_reason(&self) -> Option<&str> {
        self.public_reason.as_deref()
    }
}
impl From<String> for FailureReason {
    fn from(message: String) -> Self {
        Self {
            public_reason: Some(microcrab_protocol::diagnostic::sanitize(&message, 2048)),
            message,
        }
    }
}
impl From<&str> for FailureReason {
    fn from(message: &str) -> Self {
        message.to_owned().into()
    }
}
impl std::fmt::Debug for FailureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FailureReason")
            .field("public_reason", &self.public_reason())
            .finish_non_exhaustive()
    }
}
impl std::fmt::Display for FailureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

#[derive(Debug, Error)]
pub enum ToolError {
    #[error(transparent)]
    Host(#[from] crate::HostError),
    #[error("host capability is unsupported: {0}")]
    Unsupported(FailureReason),
    #[error("file path is outside the configured root")]
    PathOutsideRoot,
    #[error("file byte limit exceeded")]
    FileLimit,
    #[error("Host operation limit exceeded: {0}")]
    OperationLimit(FailureReason),
    #[error("invalid host request: {0}")]
    InvalidRequest(FailureReason),
    #[error("shell working directory is outside the configured root")]
    CwdOutsideRoot,
    #[error("host operation cancelled")]
    Cancelled,
    #[error("Host operation context deadline exceeded")]
    OperationDeadline,
    #[error("host deadline exceeded")]
    Timeout,
    #[error("host output byte limit exceeded")]
    OutputLimit,
    #[error("Host operation conflicts with current owner state")]
    WorkspaceConflict,
    #[error("host I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("file changed since the expected version")]
    FileConflict,
    #[error("host execution worker failed: {0}")]
    Worker(FailureReason),
}

impl ToolError {
    pub fn invalid_request(reason: &'static str) -> Self {
        Self::InvalidRequest(FailureReason::known(reason))
    }
    pub fn unsupported(reason: &'static str) -> Self {
        Self::Unsupported(FailureReason::from(reason).with_public_reason(known_unsupported(reason)))
    }
    pub fn operation_limit(reason: &'static str) -> Self {
        Self::OperationLimit(FailureReason::known(reason))
    }
    pub fn worker(reason: &'static str) -> Self {
        Self::Worker(FailureReason::known(reason))
    }
    /// Only closed variants and explicitly trusted producer facts cross diagnostics.
    pub fn public_reason(&self) -> Option<&str> {
        match self {
            Self::Host(error) => error
                .public_reason
                .as_deref()
                .filter(|reason| reason.len() <= 2048),
            Self::Unsupported(reason)
            | Self::OperationLimit(reason)
            | Self::InvalidRequest(reason)
            | Self::Worker(reason) => reason.public_reason(),
            Self::PathOutsideRoot => Some("file path is outside the configured root"),
            Self::CwdOutsideRoot => Some("shell working directory is outside the configured root"),
            Self::FileLimit => Some("file byte limit exceeded"),
            Self::Cancelled => Some("host operation cancelled"),
            Self::OperationDeadline => Some("Host operation context deadline exceeded"),
            Self::Timeout => Some("host deadline exceeded"),
            Self::OutputLimit => Some("host output byte limit exceeded"),
            Self::WorkspaceConflict => Some("Host operation conflicts with current owner state"),
            Self::FileConflict => Some("file changed since the expected version"),
            Self::Io(_) => None,
        }
    }
    /// Native adapters retain category and private feedback independently of cause.
    pub fn into_host_error(self) -> crate::HostError {
        use crate::{ErrorCategory, HostError};
        let category = match &self {
            Self::Host(_) | Self::Io(_) => {
                return match self {
                    Self::Host(error) => error,
                    Self::Io(error) => error.into(),
                    _ => unreachable!("matched native Host errors"),
                };
            }
            Self::Unsupported(_) => ErrorCategory::Unsupported,
            Self::Cancelled => ErrorCategory::Cancelled,
            Self::Timeout | Self::OperationDeadline => ErrorCategory::Timeout,
            Self::PathOutsideRoot | Self::CwdOutsideRoot => ErrorCategory::Permission,
            Self::WorkspaceConflict | Self::FileConflict | Self::InvalidRequest(_) => {
                ErrorCategory::Conflict
            }
            Self::FileLimit | Self::OutputLimit | Self::OperationLimit(_) | Self::Worker(_) => {
                ErrorCategory::Io
            }
        };
        let public_reason = self.public_reason().map(str::to_owned);
        let mut error = HostError::new(category, self.to_string());
        error.public_reason = public_reason;
        error
    }
}

/// Compatibility assembly interface for existing tool implementations. New
/// consumers should depend on owner-specific ports in `tool_api`; primitives
/// and metadata come from the foundational HostAPI supertrait.
#[async_trait]
pub trait Host: HostAPI {
    /// Stable, exact workspace authority for the SHA-256 typed text APIs. A
    /// carrier that cannot authenticate this identity must leave it unavailable.
    fn file_version_authority(&self) -> Option<String> {
        None
    }

    /// Operations implemented by this Host, independent of linked guest imports.
    /// This is availability discovery, never an authorization decision.
    fn tool_capabilities(&self) -> Vec<String> {
        Vec::new()
    }

    /// Normalize a root/cwd/file resource in the executing Host's namespace.
    /// Root is "."; other results are root-relative slash paths. This performs
    /// path admission only and never authorizes an operation.
    async fn normalize_resource(
        &self,
        _path: PathBuf,
        cancellation: CancellationToken,
    ) -> Result<String, ToolError> {
        if cancellation.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        Err(ToolError::unsupported("normalize_resource"))
    }

    async fn read_text(
        &self,
        _request: ReadRequest,
        cancellation: CancellationToken,
    ) -> Result<ReadOutput, ToolError> {
        if cancellation.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        Err(ToolError::unsupported("read"))
    }

    /// Implementations serialize conditional publication. Cancellation before the
    /// commit aborts; after commit they finish with a known outcome even on drop.
    async fn write(
        &self,
        _request: WriteRequest,
        cancellation: CancellationToken,
    ) -> Result<WriteOutput, ToolError> {
        if cancellation.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        Err(ToolError::unsupported("write"))
    }

    /// Validate the complete bounded list without effects and return admitted,
    /// normalized paths. This does not reserve versions for subsequent commits.
    async fn preflight_mutations(
        &self,
        _mutations: Vec<crate::FileMutation>,
        cancellation: CancellationToken,
    ) -> Result<Vec<crate::FileMutation>, ToolError> {
        if cancellation.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        Err(ToolError::unsupported("file_mutation"))
    }

    /// Revalidate and commit one conditional mutation under the Host's path locks.
    /// After any destination publication return its known outcome, including a
    /// partial move, even if cancellation or source removal subsequently fails.
    async fn mutate(
        &self,
        _mutation: crate::FileMutation,
        cancellation: CancellationToken,
    ) -> Result<crate::MutationOutput, ToolError> {
        if cancellation.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        Err(ToolError::unsupported("file_mutation"))
    }

    async fn glob(
        &self,
        _request: GlobRequest,
        cancellation: CancellationToken,
    ) -> Result<GlobOutput, ToolError> {
        if cancellation.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        Err(ToolError::unsupported("glob"))
    }

    /// Discover bounded ignore sources under the configured Host root. A Host
    /// without this operation must fail explicitly, not return an empty snapshot.
    async fn glob_ignore_snapshot(
        &self,
        ctx: &crate::OpContext,
    ) -> Result<GlobIgnoreSnapshot, ToolError> {
        ctx.check()?;
        Err(ToolError::unsupported("glob_ignore_snapshot"))
    }

    /// Filter using only the supplied workspace-scoped rules, before result
    /// admission. An empty list never consults disk; `include_ignored` bypasses
    /// only filtering and does not bypass path admission.
    async fn glob_with_ignore_rules(
        &self,
        ctx: &crate::OpContext,
        _request: GlobRequest,
        _rules: Vec<GlobIgnoreRuleFile>,
    ) -> Result<GlobOutput, ToolError> {
        ctx.check()?;
        Err(ToolError::unsupported("glob_with_ignore_rules"))
    }

    /// Materialize concrete workspace-relative exclusions or report why the list is incomplete.
    async fn glob_ignored_paths(&self, ctx: &crate::OpContext) -> Result<Vec<String>, ToolError> {
        ctx.check()?;
        Err(ToolError::unsupported("glob_ignored_paths"))
    }

    /// The supplied concrete paths are the only ignore filter; a search-work limit
    /// is reported separately from omitted matching output.
    async fn glob_with_ignored_paths(
        &self,
        ctx: &crate::OpContext,
        _request: GlobRequest,
        _paths: Vec<String>,
    ) -> Result<ResolvedGlobOutput, ToolError> {
        ctx.check()?;
        Err(ToolError::unsupported("glob_with_ignored_paths"))
    }

    async fn grep(
        &self,
        _request: GrepRequest,
        cancellation: CancellationToken,
    ) -> Result<GrepOutput, ToolError> {
        if cancellation.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        Err(ToolError::unsupported("grep"))
    }

    /// Actual execution environment facts. Discovery must not execute capabilities.
    fn info(&self) -> Result<HostInfo, ToolError> {
        Err(ToolError::unsupported("info"))
    }

    /// Capture one explicitly selected regular file, including missing state.
    /// Never traverse siblings or infer content from metadata. Runtime/Session
    /// correlates before/after points; this interface retains no global state.
    async fn cache_point(
        &self,
        _request: CachePointRequest,
        cancellation: CancellationToken,
    ) -> Result<FileCachePoint, ToolError> {
        if cancellation.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        Err(ToolError::unsupported("cache_point"))
    }

    /// Authenticated native Project binding for an active, explicitly
    /// negotiated web transport route. Inspection and unproven routes return None.
    fn web_transport_binding(&self) -> Option<crate::web_transport::ProjectScope> {
        None
    }

    /// Availability only. Implementations revalidate capability and lease at dispatch.
    fn supports_web_transport(&self) -> bool {
        self.web_transport_binding().is_some()
    }

    /// One non-redirecting, credential-free GET at the invocation-admitted URL.
    async fn http_get(
        &self,
        ctx: &crate::OpContext,
        _scope: &crate::web_transport::ConnectionScope,
        _url: &str,
        _max_bytes: u32,
    ) -> crate::web_transport::WebTransportResult<crate::web_transport::HttpResponse> {
        crate::web_transport::unsupported(ctx)
    }

    /// Never replay an open. The scope is allocated by Runtime, not the peer.
    async fn ws_open(
        &self,
        ctx: &crate::OpContext,
        _scope: &crate::web_transport::ConnectionScope,
        _url: &str,
    ) -> crate::web_transport::WebTransportResult<()> {
        crate::web_transport::unsupported(ctx)
    }

    /// Observe locally known ownership; never send a ping or consume a frame.
    async fn ws_status(
        &self,
        ctx: &crate::OpContext,
        _scope: &crate::web_transport::ConnectionScope,
    ) -> crate::web_transport::WebTransportResult<()> {
        crate::web_transport::unsupported(ctx)
    }

    /// Send exactly one complete UTF-8 message, once; never reconnect.
    async fn ws_send(
        &self,
        ctx: &crate::OpContext,
        _scope: &crate::web_transport::ConnectionScope,
        _text: &str,
    ) -> crate::web_transport::WebTransportResult<()> {
        crate::web_transport::unsupported(ctx)
    }

    /// Consume at most one complete message. Never return a truncated message.
    async fn ws_receive(
        &self,
        ctx: &crate::OpContext,
        _scope: &crate::web_transport::ConnectionScope,
        _max_bytes: u32,
    ) -> crate::web_transport::WebTransportResult<crate::web_transport::WsReceive> {
        crate::web_transport::unsupported(ctx)
    }

    /// Consume at most one complete message, with a soft wait up to
    /// [`crate::web_transport::WEB_MAX_WS_SOFT_WAIT_MS`]. Zero checks
    /// immediately. `Empty` preserves the scope; a hard deadline or cancellation
    /// is not `Empty`. Fence an unconfirmed consume, never replay.
    async fn ws_pull(
        &self,
        ctx: &crate::OpContext,
        _scope: &crate::web_transport::ConnectionScope,
        _max_bytes: u32,
        _soft_wait_ms: u32,
    ) -> crate::web_transport::WebTransportResult<crate::web_transport::WsPull> {
        crate::web_transport::unsupported(ctx)
    }

    /// Retire this exact scoped identity, including any racing pending open.
    async fn ws_close(
        &self,
        ctx: &crate::OpContext,
        _scope: &crate::web_transport::ConnectionScope,
    ) -> crate::web_transport::WebTransportResult<()> {
        crate::web_transport::unsupported(ctx)
    }

    /// Runtime-only teardown for a retired Session epoch. Fence pending opens
    /// from this owner and release its sockets; never affect another owner.
    async fn ws_release_scope(
        &self,
        ctx: &crate::OpContext,
        _owner: &crate::web_transport::ConnectionOwner,
    ) -> crate::web_transport::WebTransportResult<()> {
        crate::web_transport::unsupported(ctx)
    }

    /// Whether this Host can currently use a confined private binary read lane.
    /// Capability is not permission; implementations revalidate on dispatch.
    fn supports_private_file_read(&self) -> bool {
        false
    }

    /// Read a confined regular file directly into a caller-owned, precharged
    /// binary allocation. Relative paths resolve against the bound Host root;
    /// parent traversal (`..`) is rejected. No generic JSON/text carrier, retained
    /// source, or duplicate raster is allowed. The Host owns the buffer until its
    /// actual read worker settles, including when the caller drops its future.
    async fn read_private_file(
        &self,
        _path: PathBuf,
        _buffer: PrivateFileBuffer,
        context: &crate::OpContext,
    ) -> Result<PrivateFileBuffer, ToolError> {
        context.check().map_err(ToolError::Host)?;
        Err(ToolError::unsupported("read_private_file"))
    }

    /// Read a regular file under the Host's configured root. Relative paths resolve
    /// against that root. Implementations must enforce the caller's byte bound.
    async fn read_file(
        &self,
        _path: PathBuf,
        _max_bytes: usize,
        cancellation: CancellationToken,
    ) -> Result<Vec<u8>, ToolError> {
        if cancellation.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        Err(ToolError::unsupported("read_file"))
    }
}

/// Bounded caller-owned raster read space. The opaque owner keeps its allocation
/// reservation until the actual Host worker drops or returns the buffer.
pub struct PrivateFileBuffer {
    data: Vec<u8>,
    max_bytes: usize,
    _owner: Box<dyn Send>,
}

impl PrivateFileBuffer {
    pub fn new(max_bytes: usize, owner: impl Send + 'static) -> Self {
        Self {
            data: Vec::new(),
            max_bytes,
            _owner: Box::new(owner),
        }
    }

    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    /// Allocate the exact observed length once. A growing or shrinking source
    /// must be rejected rather than extending or partially returning this buffer.
    pub fn allocate(&mut self, length: usize) -> Result<&mut [u8], ToolError> {
        if length > self.max_bytes {
            return Err(ToolError::FileLimit);
        }
        if self.data.capacity() != 0 {
            return Err(ToolError::invalid_request(
                "private file buffer is already allocated",
            ));
        }
        self.data
            .try_reserve_exact(length)
            .map_err(|_| ToolError::worker("private file allocation failed"))?;
        self.data.resize(length, 0);
        Ok(&mut self.data)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Fill the exact-size, previously allocated buffer without reallocating.
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// Transfer to the already-reserved admitted image output owner without copying.
    pub fn into_bytes(self) -> Vec<u8> {
        self.data
    }
}

/// Operations selected by trusted native producers, never arbitrary capability input.
pub(crate) fn known_unsupported(operation: &'static str) -> &'static str {
    match operation {
        "file_mutation" => "host capability is unsupported: file_mutation",
        "cache_point" => "host capability is unsupported: cache_point",
        "filesystem.close" => "host capability is unsupported: filesystem.close",
        "filesystem.create_temp" => "host capability is unsupported: filesystem.create_temp",
        "filesystem.fsync" => "host capability is unsupported: filesystem.fsync",
        "filesystem.list" => "host capability is unsupported: filesystem.list",
        "filesystem.list.recursive" => "host capability is unsupported: filesystem.list.recursive",
        "filesystem.mkdir" => "host capability is unsupported: filesystem.mkdir",
        "filesystem.open" => "host capability is unsupported: filesystem.open",
        "filesystem.open.mutable" => "host capability is unsupported: filesystem.open.mutable",
        "filesystem.read" => "host capability is unsupported: filesystem.read",
        "filesystem.remove" => "host capability is unsupported: filesystem.remove",
        "filesystem.rename" => "host capability is unsupported: filesystem.rename",
        "filesystem.special-file" => "host capability is unsupported: filesystem.special-file",
        "filesystem.stat" => "host capability is unsupported: filesystem.stat",
        "filesystem.write" => "host capability is unsupported: filesystem.write",
        "glob" => "host capability is unsupported: glob",
        "glob_ignore_snapshot" => "host capability is unsupported: glob_ignore_snapshot",
        "glob_ignored_paths" => "host capability is unsupported: glob_ignored_paths",
        "glob_with_ignored_paths" => "host capability is unsupported: glob_with_ignored_paths",
        "glob_with_ignore_rules" => "host capability is unsupported: glob_with_ignore_rules",
        "grep" => "host capability is unsupported: grep",
        "info" => "host capability is unsupported: info",
        "net.accept" => "host capability is unsupported: net.accept",
        "net.close_listener" => "host capability is unsupported: net.close_listener",
        "net.close_socket" => "host capability is unsupported: net.close_socket",
        "net.connect_tcp" => "host capability is unsupported: net.connect_tcp",
        "net.listen_tcp" => "host capability is unsupported: net.listen_tcp",
        "net.read" => "host capability is unsupported: net.read",
        "net.resolve" => "host capability is unsupported: net.resolve",
        "net.write" => "host capability is unsupported: net.write",
        "normalize_resource" => "host capability is unsupported: normalize_resource",
        "process.close_stdin" => "host capability is unsupported: process.close_stdin",
        "process.exec" => "host capability is unsupported: process.exec",
        "process.kill_tree" => "host capability is unsupported: process.kill_tree",
        "process.pid" => "host capability is unsupported: process.pid",
        "process.pty" => "host capability is unsupported: process.pty",
        "process.release" => "host capability is unsupported: process.release",
        "process.resize_pty" => "host capability is unsupported: process.resize_pty",
        "process.shell-spec" => "host capability is unsupported: process.shell-spec",
        "process.signal" => "host capability is unsupported: process.signal",
        "process.spawn" => "host capability is unsupported: process.spawn",
        "process.status" => "host capability is unsupported: process.status",
        "process.stderr" => "host capability is unsupported: process.stderr",
        "process.stdout" => "host capability is unsupported: process.stdout",
        "process.wait" => "host capability is unsupported: process.wait",
        "process.watch" => "host capability is unsupported: process.watch",
        "process.write_stdin" => "host capability is unsupported: process.write_stdin",
        "read" => "host capability is unsupported: read",
        "read_file" => "host capability is unsupported: read_file",
        "read_private_file" => "Host does not support confined private binary reads",
        "shell.capture-status" => "host capability is unsupported: shell.capture-status",
        "shell.drain" => "host capability is unsupported: shell.drain",
        "shell.launch" => "host capability is unsupported: shell.launch",
        "shell.read" => "host capability is unsupported: shell.read",
        "shell.write" => "host capability is unsupported: shell.write",
        "write" => "host capability is unsupported: write",
        _ => operation,
    }
}
