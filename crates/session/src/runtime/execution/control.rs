//! Optional input and cleanup callbacks. Output bytes are owned only by Runtime.
use super::ExecutionCapabilities;
use crate::Error;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
#[async_trait]
pub trait ExecutionController: Send + Sync {
    fn capabilities(&self) -> ExecutionCapabilities {
        ExecutionCapabilities::default()
    }
    async fn write(
        &self,
        _data: String,
        _eof: bool,
        _cancellation: CancellationToken,
    ) -> Result<(), Error> {
        Err(Error::tool("execution does not support input"))
    }
    /// Success acknowledges a request; producer completion confirms the outcome.
    async fn cancel(&self, cancellation: CancellationToken) -> Result<(), Error>;
    async fn release(&self, cancellation: CancellationToken) -> Result<(), Error>;
}
/// Cooperative native/WASM writers can share this token without retaining ToolContext.
pub struct CancellationController {
    token: CancellationToken,
}
impl CancellationController {
    pub fn new(token: CancellationToken) -> Self {
        Self { token }
    }
}
#[async_trait]
impl ExecutionController for CancellationController {
    async fn cancel(&self, _: CancellationToken) -> Result<(), Error> {
        self.token.cancel();
        Ok(())
    }
    async fn release(&self, _: CancellationToken) -> Result<(), Error> {
        self.token.cancel();
        Ok(())
    }
}
