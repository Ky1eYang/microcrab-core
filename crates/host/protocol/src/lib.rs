//! Versioned Host contract shared by direct native and virtual browser transports.
mod carrier;
mod codec;
pub use carrier::{decode_transfer_chunk, encode_transfer_chunk};
pub use codec::pb;
#[cfg(feature = "grpc")]
pub use codec::{HostBridge, HostBridgeClient, HostBridgeServer};
pub use codec::{decode_frame, encode_frame, frame_from_proto, frame_to_proto};
mod cause;
mod digest;
pub use cause::HostCause;
pub use digest::{descriptor_fingerprint, descriptor_matches_pin, payload_digest, sha256};
use microcrab_host_api::{GlobRequest, GrepRequest, HostInfo, ReadRequest, WriteRequest};
use serde::{Deserialize, Serialize};
use serde_json::Value;
pub const PROTOCOL_VERSION: u32 = 2;
pub const MAX_FRAME_BYTES: usize = 131072;
pub const MAX_CHUNK_BYTES: usize = 16384;
pub const MAX_TRANSFER_BYTES: usize = 67108864;
pub const MAX_PENDING_REQUESTS: usize = 32;
/// Per-Control capability returned only after Pair/Resume authentication. Execute
/// must present it; a workspace's public lease counter is never authentication.
pub const GRPC_EXECUTE_TOKEN_HEADER: &str = "x-microcrab-execute-token";
mod scope;
pub use scope::{
    BindingScope, ConversationScope, ConversationScopeKind, ConversationScopeVersion, ProjectScope,
};
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostDescriptor {
    pub protocol_version: u32,
    pub host_id: String,
    pub workspace_id: String,
    pub bridge_instance_id: String,
    pub workspace_root: String,
    pub platform: String,
    pub info: HostInfo,
    pub tool_capabilities: Vec<String>,
    pub supported_primitives: Vec<String>,
    pub descriptor_digest: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    Unsupported,
    Denied,
    Timeout,
    Cancelled,
    Conflict,
    InvalidRequest,
    Limit,
    Io,
    Disconnected,
    LeaseExpired,
    OutcomeUnknown,
    OperationConflict,
    NotFound,
    PairingDenied,
    WorkspaceDenied,
    BindingMismatch,
}
/// Safe bridge evidence: identifiers and closed vocabulary only, never request content.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BridgeDiagnostic {
    pub event_id: String,
    pub timestamp_ms: u64,
    pub bridge_instance_id: String,
    pub request_id: Option<String>,
    pub operation_id: Option<String>,
    pub lease_generation: String,
    pub method: Option<String>,
    pub phase: BridgeDiagnosticPhase,
    pub error_code: Option<ErrorCode>,
    pub elapsed_ms: Option<u64>,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BridgeDiagnosticPhase {
    OperationFailed,
    CancelReceived,
    ConnectionClosed,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WireError {
    pub code: ErrorCode,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<HostCause>,
}
impl WireError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: microcrab_protocol::diagnostic::sanitize(&message.into(), 4096),
            cause: None,
        }
    }
    pub fn with_cause(mut self, cause: Option<HostCause>) -> Self {
        self.cause = cause;
        self
    }
}
impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for WireError {}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "method",
    content = "params",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum HostRequest {
    CachePoint(microcrab_host_api::CachePointRequest),
    Read(ReadRequest),
    Write(WriteRequest),
    PreflightMutations {
        requests: Vec<microcrab_host_api::FileMutation>,
    },
    Mutate(microcrab_host_api::FileMutation),
    Glob(GlobRequest),
    Grep(GrepRequest),
    Normalize {
        path: String,
    },
    ReadFile {
        path: String,
        max_bytes: u32,
    },
    Stat {
        path: String,
    },
    ProcessSpawn(microcrab_host_api::process::ProcessSpec),
    ShellLaunch(microcrab_host_api::ShellRequest),
    ProcessInventory,
    ProcessStatus {
        handle: microcrab_host_api::process::ProcessHandle,
    },
    ProcessPid {
        handle: microcrab_host_api::process::ProcessHandle,
    },
    ProcessWait {
        handle: microcrab_host_api::process::ProcessHandle,
    },
    ProcessSignal {
        handle: microcrab_host_api::process::ProcessHandle,
        signal: microcrab_host_api::process::Signal,
    },
    ProcessKill {
        handle: microcrab_host_api::process::ProcessHandle,
        force: bool,
    },
    ProcessRelease {
        handle: microcrab_host_api::process::ProcessHandle,
    },
    ProcessWrite {
        handle: microcrab_host_api::process::ProcessHandle,
        data: Vec<u8>,
    },
    ProcessCloseStdin {
        handle: microcrab_host_api::process::ProcessHandle,
    },
    ProcessResize {
        handle: microcrab_host_api::process::ProcessHandle,
        rows: u16,
        cols: u16,
    },
    /// Taking a raw pipe is exclusive. Reads consume at most MAX_CHUNK_BYTES.
    ProcessStreamOpen {
        handle: microcrab_host_api::process::ProcessHandle,
        stderr: bool,
    },
    ProcessStreamRead {
        handle: microcrab_host_api::process::ProcessHandle,
        stderr: bool,
        max_bytes: u32,
    },
    ShellCaptureStatus {
        handle: microcrab_host_api::process::ProcessHandle,
    },
    ShellRead {
        handle: microcrab_host_api::process::ProcessHandle,
        from: Option<microcrab_host_api::ShellPosition>,
    },
    ShellDrain {
        handle: microcrab_host_api::process::ProcessHandle,
        max_bytes: u32,
    },
    ShellWrite {
        handle: microcrab_host_api::process::ProcessHandle,
        data: Vec<u8>,
        eof: bool,
    },
    Recovery,
    /// Settle one fenced original intent without replaying its payload.
    SettleUnknown {
        operation_id: String,
        payload_digest: String,
        scope: BindingScope,
    },
    Unsupported {
        owner: String,
        operation: String,
    },
}
impl HostRequest {
    /// Closed protocol vocabulary; safe to record without serializing parameters.
    pub fn method_name(&self) -> &'static str {
        match self {
            Self::CachePoint(_) => "cache_point",
            Self::Read(_) => "read",
            Self::Write(_) => "write",
            Self::PreflightMutations { .. } => "preflight_mutations",
            Self::Mutate(_) => "mutate",
            Self::Glob(_) => "glob",
            Self::Grep(_) => "grep",
            Self::Normalize { .. } => "normalize",
            Self::ReadFile { .. } => "read_file",
            Self::Stat { .. } => "stat",
            Self::ProcessSpawn(_) => "process_spawn",
            Self::ShellLaunch(_) => "shell_launch",
            Self::ProcessInventory => "process_inventory",
            Self::ProcessStatus { .. } => "process_status",
            Self::ProcessPid { .. } => "process_pid",
            Self::ProcessWait { .. } => "process_wait",
            Self::ProcessSignal { .. } => "process_signal",
            Self::ProcessKill { .. } => "process_kill",
            Self::ProcessRelease { .. } => "process_release",
            Self::ProcessWrite { .. } => "process_write",
            Self::ProcessCloseStdin { .. } => "process_close_stdin",
            Self::ProcessResize { .. } => "process_resize",
            Self::ProcessStreamOpen { .. } => "process_stream_open",
            Self::ProcessStreamRead { .. } => "process_stream_read",
            Self::ShellCaptureStatus { .. } => "shell_capture_status",
            Self::ShellRead { .. } => "shell_read",
            Self::ShellDrain { .. } => "shell_drain",
            Self::ShellWrite { .. } => "shell_write",
            Self::Recovery => "recovery",
            Self::SettleUnknown { .. } => "settle_unknown",
            Self::Unsupported { .. } => "unsupported",
        }
    }
    /// Reserved bounded admission keeps cancellation/cleanup and ownership
    /// observation reachable when ordinary requests occupy every data slot.
    pub fn is_control(&self) -> bool {
        matches!(
            self,
            Self::ProcessKill { .. }
                | Self::ProcessRelease { .. }
                | Self::ProcessSignal { .. }
                | Self::ProcessStatus { .. }
                | Self::ProcessPid { .. }
                | Self::ShellCaptureStatus { .. }
                | Self::ProcessInventory
                | Self::Recovery
                | Self::SettleUnknown { .. }
        )
    }
    pub fn is_process_operation(&self) -> bool {
        matches!(
            self,
            Self::ProcessStatus { .. }
                | Self::ProcessPid { .. }
                | Self::ProcessWait { .. }
                | Self::ProcessSignal { .. }
                | Self::ProcessKill { .. }
                | Self::ProcessRelease { .. }
                | Self::ProcessWrite { .. }
                | Self::ProcessCloseStdin { .. }
                | Self::ProcessResize { .. }
                | Self::ProcessStreamOpen { .. }
                | Self::ProcessStreamRead { .. }
                | Self::ShellCaptureStatus { .. }
                | Self::ShellRead { .. }
                | Self::ShellDrain { .. }
                | Self::ShellWrite { .. }
        )
    }
    /// Consuming output keeps immutable intent/deduplication, but uncertainty
    /// belongs only to this output cursor/pipe. It cannot fence other processes,
    /// workspace writes, or positively owned kill/release cleanup.
    pub fn consumption_key(&self) -> Option<String> {
        match self {
            Self::ShellDrain { handle, .. } => Some(format!("shell:{}", handle.0.0)),
            Self::ProcessStreamOpen { handle, stderr }
            | Self::ProcessStreamRead { handle, stderr, .. } => {
                Some(format!("pipe:{}:{}", handle.0.0, stderr))
            }
            _ => None,
        }
    }
    pub fn is_start(&self) -> bool {
        matches!(self, Self::ProcessSpawn(_) | Self::ShellLaunch(_))
    }
    /// Whether this operation needs an immutable intent and no-replay receipt.
    /// Consuming output is an effect even when no persistent file is changed:
    /// retrying a drain or raw pipe read could otherwise consume different bytes.
    pub fn is_mutation(&self) -> bool {
        self.is_start()
            || matches!(
                self,
                Self::Write(_)
                    | Self::Mutate(_)
                    | Self::ProcessSignal { .. }
                    | Self::ProcessKill { .. }
                    | Self::ProcessRelease { .. }
                    | Self::ProcessWrite { .. }
                    | Self::ProcessCloseStdin { .. }
                    | Self::ProcessResize { .. }
                    | Self::ProcessStreamOpen { .. }
                    | Self::ProcessStreamRead { .. }
                    | Self::ShellDrain { .. }
                    | Self::ShellWrite { .. }
            )
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransferRef {
    pub transfer_id: String,
    pub byte_length: u64,
    pub sha256: String,
    pub kind: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "state", content = "result", rename_all = "snake_case")]
pub enum ReceiptState {
    Intent,
    Completed(Result<Value, WireError>),
    Indeterminate,
    FailedUnknown {
        reason: WireError,
        effects_may_have_occurred: bool,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationReceipt {
    pub operation_id: String,
    pub payload_digest: String,
    pub scope: BindingScope,
    pub state: ReceiptState,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessStarted {
    pub handle: microcrab_host_api::process::ProcessHandle,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    pub revision: u64,
}
pub type HostReply = Result<Value, WireError>;
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WireFrame {
    Inspect {
        request: InspectRequest,
    },
    Inspected {
        request_id: String,
        result: Result<HostDescriptor, WireError>,
    },
    HandshakeRejected {
        error: WireError,
    },
    Pair {
        token: String,
        scope: BindingScope,
        #[serde(default)]
        directory: Option<String>,
        #[serde(default)]
        expected_host_id: Option<String>,
        #[serde(default)]
        expected_workspace_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_descriptor_digest: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_workspace_root: Option<String>,
    },
    Resume {
        credential: String,
        scope: BindingScope,
        #[serde(default)]
        directory: Option<String>,
        #[serde(default)]
        expected_host_id: Option<String>,
        #[serde(default)]
        expected_workspace_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_descriptor_digest: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_workspace_root: Option<String>,
    },
    WorkspaceBound {
        descriptor: HostDescriptor,
        project_id: String,
        backend_id: String,
    },
    Hello {
        descriptor: HostDescriptor,
        credential: String,
        lease_generation: String,
    },
    Request {
        request_id: String,
        operation_id: String,
        lease_generation: String,
        remaining_budget_ms: u32,
        method: HostRequest,
    },
    Response {
        request_id: String,
        operation_id: String,
        result: HostReply,
    },
    Cancel {
        request_id: String,
        lease_generation: String,
    },
    Status {
        request_id: String,
        operation_id: String,
        lease_generation: String,
    },
    Receipt {
        request_id: String,
        receipt: Option<OperationReceipt>,
    },
    Diagnostic {
        event: BridgeDiagnostic,
    },
    ProcessChanged {
        lease_generation: String,
        handle: microcrab_host_api::process::ProcessHandle,
        revision: u64,
    },
    Ping {
        lease_generation: String,
    },
    Pong {
        lease_generation: String,
    },
    TransferChunk {
        transfer_id: String,
        sequence: u32,
        data: Vec<u8>,
        eof: bool,
    },
    Detach {
        lease_generation: String,
    },
}
pub use WireFrame as HostFrame;

/// One bounded raw stream read, with EOF distinct from an empty data chunk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessChunk {
    pub data: Vec<u8>,
    pub eof: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use microcrab_host_api::{HandleId, process::ProcessHandle};
    #[test]
    fn shared_process_identity_round_trips_and_pipe_reads_are_effects() {
        let handle = ProcessHandle(HandleId((1_u64 << 53) - 1));
        let request = HostRequest::ShellWrite {
            handle,
            data: vec![0, 255],
            eof: true,
        };
        let decoded: HostRequest =
            serde_json::from_slice(&serde_json::to_vec(&request).unwrap()).unwrap();
        assert!(
            matches!(decoded, HostRequest::ShellWrite { handle: actual, .. } if actual == handle)
        );
        assert!(request.is_process_operation());
        assert!(request.is_mutation());
        let read = HostRequest::ProcessStreamRead {
            handle,
            stderr: false,
            max_bytes: MAX_CHUNK_BYTES as u32,
        };
        assert!(
            read.is_mutation(),
            "consuming raw reads must retain no-replay receipts"
        );
        assert!(
            HostRequest::ShellDrain {
                handle,
                max_bytes: MAX_CHUNK_BYTES as u32,
            }
            .is_mutation(),
            "consuming shell drains must retain no-replay receipts"
        );
        assert!(!HostRequest::ShellRead { handle, from: None }.is_mutation());
        assert!(!HostRequest::ProcessStatus { handle }.is_mutation());
        assert!(HostRequest::ProcessSpawn(Default::default()).is_start());
        assert!(
            HostRequest::ShellLaunch(microcrab_host_api::ShellRequest {
                command: "echo".into(),
                cwd: None,
                output_bytes: 16
            })
            .is_start()
        );
    }
    #[test]
    fn retired_shell_job_wire_operations_are_rejected() {
        for method in ["shell", "start_job", "job_status", "job_release"] {
            let payload = serde_json::json!({"method": method, "params": {}});
            assert!(serde_json::from_value::<HostRequest>(payload).is_err());
        }
    }
}

/// Authentication for descriptor inspection grants no execution authority.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum InspectAuthentication {
    Pair { token: String },
    Resume { credential: String },
}
impl std::fmt::Debug for InspectAuthentication {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Pair { .. } => "Pair { token: [redacted] }",
            Self::Resume { .. } => "Resume { credential: [redacted] }",
        })
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectRequest {
    pub request_id: String,
    pub authentication: InspectAuthentication,
    pub scope: BindingScope,
    pub directory: Option<String>,
    pub expected_host_id: Option<String>,
    pub expected_workspace_id: Option<String>,
}
impl InspectRequest {
    pub fn new(request_id: String, handshake: &WireFrame) -> Result<Self, WireError> {
        let (authentication, scope, directory, expected_host_id, expected_workspace_id) =
            match handshake {
                WireFrame::Pair {
                    token,
                    scope,
                    directory,
                    expected_host_id,
                    expected_workspace_id,
                    ..
                } => (
                    InspectAuthentication::Pair {
                        token: token.clone(),
                    },
                    scope,
                    directory,
                    expected_host_id,
                    expected_workspace_id,
                ),
                WireFrame::Resume {
                    credential,
                    scope,
                    directory,
                    expected_host_id,
                    expected_workspace_id,
                    ..
                } => (
                    InspectAuthentication::Resume {
                        credential: credential.clone(),
                    },
                    scope,
                    directory,
                    expected_host_id,
                    expected_workspace_id,
                ),
                _ => {
                    return Err(WireError::new(
                        ErrorCode::InvalidRequest,
                        "inspection requires Pair or Resume intent",
                    ));
                }
            };
        let request = Self {
            request_id,
            authentication,
            scope: scope.clone(),
            directory: directory.clone(),
            expected_host_id: expected_host_id.clone(),
            expected_workspace_id: expected_workspace_id.clone(),
        };
        request.validate()?;
        Ok(request)
    }
    pub fn validate(&self) -> Result<(), WireError> {
        if self.request_id.is_empty()
            || self.request_id.len() > 128
            || !self
                .request_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            || !self.scope.valid()
            || !self.scope.is_project()
        {
            return Err(WireError::new(
                ErrorCode::InvalidRequest,
                "invalid inspection identity or scope",
            ));
        }
        Ok(())
    }
    /// Reconstruct only the intended authentication, never an inspected execution grant.
    pub fn handshake(&self) -> WireFrame {
        match &self.authentication {
            InspectAuthentication::Pair { token } => WireFrame::Pair {
                token: token.clone(),
                scope: self.scope.clone(),
                directory: self.directory.clone(),
                expected_host_id: self.expected_host_id.clone(),
                expected_workspace_id: self.expected_workspace_id.clone(),
                expected_descriptor_digest: None,
                expected_workspace_root: None,
            },
            InspectAuthentication::Resume { credential } => WireFrame::Resume {
                credential: credential.clone(),
                scope: self.scope.clone(),
                directory: self.directory.clone(),
                expected_host_id: self.expected_host_id.clone(),
                expected_workspace_id: self.expected_workspace_id.clone(),
                expected_descriptor_digest: None,
                expected_workspace_root: None,
            },
        }
    }
}
