//! Owned operation records permit native adapters without a WASM engine or OS access.
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationErrorCode {
    Denied,
    InvalidRequest,
    Unsupported,
    Conflict,
    Limit,
    Cancelled,
    Timeout,
    Io,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OperationError {
    pub code: OperationErrorCode,
    pub message: String,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadRequest {
    pub path: String,
    pub offset_line: u64,
    pub max_lines: u64,
    pub max_bytes: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReadOutput {
    pub content: String,
    pub start_line: u64,
    pub next_line: Option<u64>,
    pub truncated: bool,
    pub version: String,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WritePrecondition {
    Missing,
    Version(String),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteRequest {
    pub path: String,
    pub content: String,
    pub precondition: WritePrecondition,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WriteOutput {
    pub version: String,
    pub bytes: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobRequest {
    pub root: String,
    pub pattern: String,
    pub include_hidden: bool,
    pub include_ignored: bool,
    pub include_directories: bool,
    pub max_results: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GlobEntryKind {
    File,
    Directory,
    Symlink,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GlobEntry {
    pub path: String,
    pub kind: GlobEntryKind,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GlobOutput {
    pub entries: Vec<GlobEntry>,
    pub truncated: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrepRequest {
    pub root: String,
    pub pattern: String,
    pub include_glob: Option<String>,
    pub case_sensitive: bool,
    pub literal: bool,
    pub max_matches: u64,
    pub max_bytes: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GrepMatch {
    pub path: String,
    pub line: u64,
    pub text: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GrepOutput {
    pub matches: Vec<GrepMatch>,
    pub truncated: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShellRequest {
    pub command: String,
    pub cwd: Option<String>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessHandle {
    pub id: u64,
    pub pid: Option<u32>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessState {
    Running,
    Exited { code: Option<i32> },
    Failed(String),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShellOutput {
    pub stdout: String,
    pub stderr: String,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileMutation {
    Write(WriteRequest),
    Delete(DeleteRequest),
    Move(MoveRequest),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRequest {
    pub path: String,
    pub expected_version: String,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveRequest {
    pub path: String,
    pub destination: String,
    pub expected_version: String,
    pub content: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MutationOutput {
    pub version: Option<String>,
    pub bytes: u64,
    pub source_removed: bool,
    pub destination_written: bool,
    pub diagnostic: Option<String>,
}
impl FileMutation {
    pub fn paths(&self) -> Vec<&str> {
        match self {
            Self::Write(r) => vec![&r.path],
            Self::Delete(r) => vec![&r.path],
            Self::Move(r) => vec![&r.path, &r.destination],
        }
    }
}

/// Binary media is copied through the typed execution ABI, never JSON tool text.
#[derive(Clone, PartialEq, Eq)]
pub struct MediaAttachment {
    pub media_type: String,
    pub data: Vec<u8>,
}
impl std::fmt::Debug for MediaAttachment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MediaAttachment")
            .field("media_type", &self.media_type)
            .field("byte_len", &self.data.len())
            .finish()
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvocationOutput {
    pub content: String,
    pub attachments: Vec<MediaAttachment>,
}

/// Adapters preserve structured failures. Runtime authorizes the complete tool call.
/// Conditional operations serialize cooperating Host writers; unrelated external writers
/// are not covered by an OS-level compare-and-swap guarantee.
pub trait Host {
    fn read_image(&mut self, _path: &str) -> Result<MediaAttachment, OperationError> {
        Err(OperationError {
            code: OperationErrorCode::Unsupported,
            message: "Host image reads are unavailable".into(),
        })
    }
    /// Validate and normalize every operation before any mutation; reserve Runtime
    /// output, operation-call and selected evidence budgets for the complete call.
    fn preflight_mutations(
        &mut self,
        requests: Vec<FileMutation>,
    ) -> Result<Vec<FileMutation>, OperationError> {
        let _ = requests;
        Err(OperationError {
            code: OperationErrorCode::Unsupported,
            message: "Host file mutation preflight is unavailable".into(),
        })
    }
    fn mutate(&mut self, request: FileMutation) -> Result<MutationOutput, OperationError> {
        let _ = request;
        Err(OperationError {
            code: OperationErrorCode::Unsupported,
            message: "Host conditional file mutations are unavailable".into(),
        })
    }
    fn capture(&mut self, path: &str, max_bytes: u64) -> Result<(), OperationError>;
    fn read(&mut self, request: ReadRequest) -> Result<ReadOutput, OperationError>;
    fn write(&mut self, request: WriteRequest) -> Result<WriteOutput, OperationError>;
    fn glob(&mut self, request: GlobRequest) -> Result<GlobOutput, OperationError>;
    fn grep(&mut self, request: GrepRequest) -> Result<GrepOutput, OperationError>;
    fn shell_launch(&mut self, request: ShellRequest) -> Result<ProcessHandle, OperationError>;
    /// A bounded observation wait never imposes an execution deadline.
    fn process_wait_for_state(
        &mut self,
        handle: ProcessHandle,
        yield_ms: u64,
    ) -> Result<ProcessState, OperationError>;
    /// Read without consuming output; Runtime may clamp the requested maximum.
    fn shell_read(
        &mut self,
        handle: ProcessHandle,
        max_bytes: u64,
    ) -> Result<ShellOutput, OperationError>;
}
