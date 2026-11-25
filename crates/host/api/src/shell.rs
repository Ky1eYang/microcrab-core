//! General shell capture over the Host's process owner. Observation and execution
//! policy belong to callers; launch never installs an execution deadline.
use crate::{
    process::ProcessHandle,
    types::{HostError, OpContext, Result},
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShellRequest {
    pub command: String,
    /// Relative paths resolve against the configured Host root.
    #[serde(default)]
    pub cwd: Option<PathBuf>,
    /// Shared retained UTF-8 byte budget for stdout and stderr.
    pub output_bytes: usize,
}
/// Select exclusive Runtime-owned raw pipe capture before launch. The Host must
/// not start its text capture or expose a text fallback for this request. MIME
/// and retention belong to the Runtime's selected channel, not the Host.
/// This native contract is not carried by the JSON Shell protocol.
pub struct PrivateShellRequest {
    pub command: String,
    pub cwd: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShellPosition {
    pub line: u64,
    pub column: usize,
}
impl Default for ShellPosition {
    fn default() -> Self {
        Self { line: 1, column: 0 }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpiredRange {
    pub through_line: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShellOutput {
    pub stdout: String,
    pub stderr: String,
    pub next: ShellPosition,
    pub cursor: ShellPosition,
    pub expired: Option<ExpiredRange>,
    pub truncated: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShellCaptureStatus {
    pub closed: bool,
    pub cursor: ShellPosition,
    pub stdout_bytes: usize,
    pub stderr_bytes: usize,
    pub truncated: bool,
}

/// Capture owns both output pipes. Raw ProcessApi pipe acquisition therefore
/// conflicts for shell-launched processes. Kill retains capture until release.
#[async_trait]
pub trait ShellApi: Send + Sync {
    /// Optional capability; never fall back to text Shell or a carrier that does
    /// not explicitly support private raw capture. Raw pipes follow ProcessApi.
    async fn launch_private(
        &self,
        _ctx: &OpContext,
        _request: PrivateShellRequest,
    ) -> Result<ProcessHandle> {
        Err(HostError::unsupported_known("shell.launch-private"))
    }
    async fn launch(&self, _ctx: &OpContext, _request: ShellRequest) -> Result<ProcessHandle> {
        Err(HostError::unsupported_known("shell.launch"))
    }
    async fn capture_status(
        &self,
        _ctx: &OpContext,
        _handle: ProcessHandle,
    ) -> Result<ShellCaptureStatus> {
        Err(HostError::unsupported_known("shell.capture-status"))
    }
    /// Immediate non-consuming read; None starts at the shared drain cursor.
    async fn read(
        &self,
        _ctx: &OpContext,
        _handle: ProcessHandle,
        _from: Option<ShellPosition>,
    ) -> Result<ShellOutput> {
        Err(HostError::unsupported_known("shell.read"))
    }
    /// Atomically consume all retained output only if its combined UTF-8 byte
    /// count fits max_bytes. Otherwise return Conflict without changing capture,
    /// its cursor or closure state, including while the process is still running.
    async fn drain(
        &self,
        _ctx: &OpContext,
        _handle: ProcessHandle,
        _max_bytes: usize,
    ) -> Result<ShellOutput> {
        Err(HostError::unsupported_known("shell.drain"))
    }
    async fn write(
        &self,
        _ctx: &OpContext,
        _handle: ProcessHandle,
        _data: Vec<u8>,
        _eof: bool,
    ) -> Result<()> {
        Err(HostError::unsupported_known("shell.write"))
    }
}
