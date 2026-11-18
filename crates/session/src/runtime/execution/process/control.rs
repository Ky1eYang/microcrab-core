//! Narrow object-safe capability passed to tools; no Session owner is retained.
use super::ProcessRegistry;
use crate::Error;
use async_trait::async_trait;
use microcrab_host_api::{
    ShellRequest,
    process::{ProcessHandle, ProcessSpec},
};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait ProcessControl: Send + Sync {
    fn session_id(&self) -> &str;
    fn wakeup(&self) -> Arc<Notify>;
    fn generation(&self) -> u64;
    fn has_running(&self) -> bool;
    fn has_unclosed(&self) -> bool;
    fn projection(&self) -> Option<Value>;
    fn register_child(&self, child: Arc<ProcessRegistry>) -> Result<(), Error>;
    fn registry(&self) -> Arc<ProcessRegistry>;
    /// Built-in process owners capture this invocation's opaque output witness.
    /// Custom process capabilities may implement the same non-authority hook.
    fn for_invocation(
        &self,
        _redaction: super::super::InvocationRedaction,
    ) -> Option<Arc<dyn ProcessControl>> {
        None
    }
    fn scoped(&self, session_id: String, agent: String, is_main: bool) -> Arc<dyn ProcessControl>;
    /// Reserve Session capacity and own every launched process before publication.
    async fn launch_shell(
        &self,
        tool: &str,
        call_id: &str,
        request: ShellRequest,
        cancellation: CancellationToken,
    ) -> Result<ProcessHandle, Error>;
    async fn launch_process(
        &self,
        tool: &str,
        call_id: &str,
        spec: ProcessSpec,
        cancellation: CancellationToken,
    ) -> Result<ProcessHandle, Error>;
    /// Raw guest handles are authorized only against this exact Session owner.
    fn owns_process(&self, handle: ProcessHandle) -> bool;
    /// Outer None means absent ownership; Some(None) is an owned virtual process.
    fn process_pid(&self, handle: ProcessHandle) -> Option<Option<u32>>;
    /// Accepted release finishes independently of the original waiter.
    async fn release_process(
        &self,
        handle: ProcessHandle,
        cancellation: CancellationToken,
    ) -> Result<(), Error>;
    /// One retained consumer per raw pipe; reads preserve byte tails across calls.
    async fn read_process_pipe(
        &self,
        handle: ProcessHandle,
        stderr: bool,
        max: usize,
        cancellation: CancellationToken,
    ) -> Result<Option<Vec<u8>>, Error>;
}
