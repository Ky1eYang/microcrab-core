//! Structured process execution. Implementations own process trees and reaping.
//! Handles are globally unique, table-validated identities, never operating-system pids.
use crate::types::{BoxStream, ErrorCategory, HandleId, HostError, OpContext, Result};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProcessHandle(pub HandleId);
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Signal {
    Term,
    Kill,
    Int,
    Hup,
    Usr1,
    Usr2,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PtySize {
    pub rows: u16,
    pub cols: u16,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProcessSpec {
    pub program: String,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: BTreeMap<String, String>,
    pub pty: Option<PtySize>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExitStatus {
    /// The actual operating-system exit code, when available.
    pub code: Option<i32>,
    /// Native termination signal when reported. Kill also records a known Host
    /// force-termination operation selected before terminal publication, including
    /// on Windows; it does not claim a Windows OS signal or prove the leader's
    /// death was caused by that operation when natural exit raced termination.
    pub signal: Option<Signal>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecOutput {
    pub stdout: Bytes,
    pub stderr: Bytes,
    pub status: ExitStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "lowercase")]
pub enum ProcessState {
    Running,
    Exited { status: ExitStatus },
    Failed { reason: String },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessStatus {
    /// Present only when the backend owns a real operating-system process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(flatten)]
    pub state: ProcessState,
    pub elapsed_s: u64,
}

/// A conservative bound for each captured pipe in the convenience helper.
/// A backend with a smaller resource budget must enforce its own tighter bound.
pub const EXEC_PIPE_LIMIT: usize = 16 * 1024 * 1024;

#[async_trait]
pub trait ProcessApi: Send + Sync {
    /// Cancellation covers admission only. After receipt the Host owns lifetime;
    /// dropping or cancelling observations never terminates the process.
    async fn spawn(&self, _ctx: &OpContext, _spec: ProcessSpec) -> Result<ProcessHandle> {
        Err(HostError::unsupported_known("process.spawn"))
    }
    fn has_running_processes(&self) -> bool {
        false
    }
    fn has_unresolved_cleanup(&self) -> bool {
        false
    }
    /// State/capture closure notifications, independent of admission context.
    fn watch(&self, _handle: ProcessHandle) -> Result<tokio::sync::watch::Receiver<u64>> {
        Err(HostError::unsupported_known("process.watch"))
    }
    async fn status(&self, _ctx: &OpContext, _handle: ProcessHandle) -> Result<ProcessStatus> {
        Err(HostError::unsupported_known("process.status"))
    }
    async fn pid(&self, ctx: &OpContext, handle: ProcessHandle) -> Result<u32> {
        self.status(ctx, handle)
            .await?
            .pid
            .ok_or_else(|| HostError::unsupported_known("process.pid"))
    }
    /// Repeated waits return the retained terminal result until release.
    async fn wait(&self, _ctx: &OpContext, _handle: ProcessHandle) -> Result<ExitStatus> {
        Err(HostError::unsupported_known("process.wait"))
    }
    async fn signal(
        &self,
        _ctx: &OpContext,
        _handle: ProcessHandle,
        _signal: Signal,
    ) -> Result<()> {
        Err(HostError::unsupported_known("process.signal"))
    }
    /// Success means the complete owned tree has terminated and been reaped.
    async fn kill_tree(
        &self,
        _ctx: &OpContext,
        _handle: ProcessHandle,
        _force: bool,
    ) -> Result<()> {
        Err(HostError::unsupported_known("process.kill_tree"))
    }
    /// One consumer per pipe; a second take or shell capture conflicts. Streams
    /// remain independent of the admission context after publication.
    fn stdout(&self, _handle: ProcessHandle) -> Result<BoxStream<Bytes>> {
        Err(HostError::unsupported_known("process.stdout"))
    }
    fn stderr(&self, _handle: ProcessHandle) -> Result<BoxStream<Bytes>> {
        Err(HostError::unsupported_known("process.stderr"))
    }
    /// Write all bytes, or return an error; partial progress is backend-owned.
    async fn write_stdin(
        &self,
        _ctx: &OpContext,
        _handle: ProcessHandle,
        _data: Bytes,
    ) -> Result<()> {
        Err(HostError::unsupported_known("process.write_stdin"))
    }
    async fn close_stdin(&self, _ctx: &OpContext, _handle: ProcessHandle) -> Result<()> {
        Err(HostError::unsupported_known("process.close_stdin"))
    }
    async fn resize_pty(
        &self,
        _ctx: &OpContext,
        _handle: ProcessHandle,
        _rows: u16,
        _cols: u16,
    ) -> Result<()> {
        Err(HostError::unsupported_known("process.resize_pty"))
    }
    /// Kill/reap a running process and release all retained state. Cleanup remains
    /// owned even if the release waiter is cancelled or dropped.
    async fn release(&self, _ctx: &OpContext, _handle: ProcessHandle) -> Result<()> {
        Err(HostError::unsupported_known("process.release"))
    }
    /// The executing backend selects its trusted interpreter; the API cannot infer
    /// a remote platform or replace an embedding application's interpreter choice.
    fn shell_spec(&self, _command: &str) -> Result<ProcessSpec> {
        Err(HostError::unsupported_known("process.exec"))
    }
    /// Drain both pipes concurrently with wait. Backends advertising exec must
    /// retain a cleanup owner when the caller drops this convenience operation.
    async fn exec(&self, ctx: &OpContext, command: &str) -> Result<ExecOutput> {
        exec(self, ctx, command).await
    }
}

/// Compose execution over primitive process operations without calling the trait
/// exec method recursively. A backend advertising exec must run this helper in
/// an owned worker and cancel its context on caller drop, so kill/release always
/// finish even when the observing future disappears.
pub async fn exec<P: ProcessApi + ?Sized>(
    api: &P,
    ctx: &OpContext,
    command: &str,
) -> Result<ExecOutput> {
    ctx.check()?;
    let spec = api.shell_spec(command)?;
    let scoped = ctx.child();
    let handle = api.spawn(&scoped, spec).await?;
    let result = scoped
        .run(async {
            let stdout = api.stdout(handle)?;
            let stderr = api.stderr(handle)?;
            api.close_stdin(&scoped, handle).await?;
            let (stdout, stderr, status) = futures_util::try_join!(
                collect(stdout),
                collect(stderr),
                api.wait(&scoped, handle)
            )?;
            Ok(ExecOutput {
                stdout,
                stderr,
                status,
            })
        })
        .await;
    let cleanup = ctx.cleanup();
    if result.is_err() {
        scoped.cancel.cancel();
        let _ = api.kill_tree(&cleanup, handle, true).await;
    }
    let released = api.release(&cleanup, handle).await;
    match result {
        Ok(output) => {
            released?;
            Ok(output)
        }
        Err(error) => Err(error),
    }
}

async fn collect(mut stream: BoxStream<Bytes>) -> Result<Bytes> {
    let mut output = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if chunk.len() > EXEC_PIPE_LIMIT.saturating_sub(output.len()) {
            return Err(HostError::known(
                ErrorCategory::Io,
                "process output byte limit exceeded",
            ));
        }
        output.extend_from_slice(&chunk);
    }
    Ok(output.freeze())
}
