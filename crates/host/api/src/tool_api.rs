//! Owner-specific extension ports for tool semantics not represented by primitives.
//! Host remains the compatibility implementation entry. New callers supply the
//! Runtime-owned operation context; legacy implementations keep their signatures.
use crate::{
    CachePointRequest, CancelToken, ErrorCategory, FileCachePoint, GlobOutput, GlobRequest,
    GrepOutput, GrepRequest, Host, HostError, HostInfo, OpContext, ReadOutput, ReadRequest,
    ToolError, WriteOutput, WriteRequest,
};
use async_trait::async_trait;
use std::{future::Future, path::PathBuf};

fn context_error(error: HostError) -> ToolError {
    match error.category {
        ErrorCategory::Cancelled => ToolError::Cancelled,
        ErrorCategory::Timeout => ToolError::OperationDeadline,
        _ => ToolError::Host(error),
    }
}

/// A derived token makes dropping this future observable to the legacy backend.
/// Only non-publication operations race the context; cleanup remains backend-owned.
async fn invoke<T, F, Fut>(ctx: &OpContext, operation: F) -> Result<T, ToolError>
where
    T: Send,
    F: FnOnce(CancelToken) -> Fut + Send,
    Fut: Future<Output = Result<T, ToolError>> + Send,
{
    let scoped = ctx.child();
    let _cancel_on_drop = scoped.cancel.clone().drop_guard();
    scoped
        .run(async { Ok(operation(scoped.cancel.clone()).await) })
        .await
        .map_err(context_error)?
}

/// A committed write or admitted background job must return its known outcome.
/// Check the deadline before admission, propagate caller/drop cancellation, then
/// let the owner finish or compensate. Do not race publication against a timer.
/// Existing legacy write deadlines remain backend-owned after this precheck.
async fn publish<T, F, Fut>(ctx: &OpContext, operation: F) -> Result<T, ToolError>
where
    T: Send,
    F: FnOnce(CancelToken) -> Fut + Send,
    Fut: Future<Output = Result<T, ToolError>> + Send,
{
    ctx.check().map_err(context_error)?;
    let scoped = ctx.child();
    let _cancel_on_drop = scoped.cancel.clone().drop_guard();
    operation(scoped.cancel.clone()).await
}

#[async_trait]
pub trait TextReadApi: Send + Sync {
    async fn read_text(
        &self,
        ctx: &OpContext,
        request: ReadRequest,
    ) -> Result<ReadOutput, ToolError>;
}
#[async_trait]
impl<T: Host + ?Sized> TextReadApi for T {
    async fn read_text(
        &self,
        ctx: &OpContext,
        request: ReadRequest,
    ) -> Result<ReadOutput, ToolError> {
        invoke(ctx, |cancel| Host::read_text(self, request, cancel)).await
    }
}
#[async_trait]
pub trait TextWriteApi: Send + Sync {
    async fn write(&self, ctx: &OpContext, request: WriteRequest)
    -> Result<WriteOutput, ToolError>;
}
#[async_trait]
impl<T: Host + ?Sized> TextWriteApi for T {
    async fn write(
        &self,
        ctx: &OpContext,
        request: WriteRequest,
    ) -> Result<WriteOutput, ToolError> {
        publish(ctx, |cancel| Host::write(self, request, cancel)).await
    }
}
/// Conditional file publication through the real Host facade. Preflight is
/// cancellable; mutation awaits the owner's final receipt after admission.
#[async_trait]
pub trait FileMutationApi: Send + Sync {
    async fn preflight_mutations(
        &self,
        ctx: &OpContext,
        mutations: Vec<crate::FileMutation>,
    ) -> Result<Vec<crate::FileMutation>, ToolError>;
    async fn mutate(
        &self,
        ctx: &OpContext,
        mutation: crate::FileMutation,
    ) -> Result<crate::MutationOutput, ToolError>;
}
#[async_trait]
impl<T: Host + ?Sized> FileMutationApi for T {
    async fn preflight_mutations(
        &self,
        ctx: &OpContext,
        mutations: Vec<crate::FileMutation>,
    ) -> Result<Vec<crate::FileMutation>, ToolError> {
        invoke(ctx, |cancel| {
            Host::preflight_mutations(self, mutations, cancel)
        })
        .await
    }
    async fn mutate(
        &self,
        ctx: &OpContext,
        mutation: crate::FileMutation,
    ) -> Result<crate::MutationOutput, ToolError> {
        publish(ctx, |cancel| Host::mutate(self, mutation, cancel)).await
    }
}

#[async_trait]
pub trait GlobApi: Send + Sync {
    async fn glob(&self, ctx: &OpContext, request: GlobRequest) -> Result<GlobOutput, ToolError>;
}
#[async_trait]
impl<T: Host + ?Sized> GlobApi for T {
    async fn glob(&self, ctx: &OpContext, request: GlobRequest) -> Result<GlobOutput, ToolError> {
        invoke(ctx, |cancel| Host::glob(self, request, cancel)).await
    }
}
#[async_trait]
pub trait GrepApi: Send + Sync {
    async fn grep(&self, ctx: &OpContext, request: GrepRequest) -> Result<GrepOutput, ToolError>;
}
#[async_trait]
impl<T: Host + ?Sized> GrepApi for T {
    async fn grep(&self, ctx: &OpContext, request: GrepRequest) -> Result<GrepOutput, ToolError> {
        invoke(ctx, |cancel| Host::grep(self, request, cancel)).await
    }
}
/// Host-owned path admission and bounded attachment bytes, independent of policy.
#[async_trait]
pub trait ResourceApi: Send + Sync {
    async fn normalize_resource(&self, ctx: &OpContext, path: PathBuf)
    -> Result<String, ToolError>;
    async fn read_file(
        &self,
        ctx: &OpContext,
        path: PathBuf,
        max_bytes: usize,
    ) -> Result<Vec<u8>, ToolError>;
}
#[async_trait]
impl<T: Host + ?Sized> ResourceApi for T {
    async fn normalize_resource(
        &self,
        ctx: &OpContext,
        path: PathBuf,
    ) -> Result<String, ToolError> {
        invoke(ctx, |cancel| Host::normalize_resource(self, path, cancel)).await
    }
    async fn read_file(
        &self,
        ctx: &OpContext,
        path: PathBuf,
        max_bytes: usize,
    ) -> Result<Vec<u8>, ToolError> {
        invoke(ctx, |cancel| Host::read_file(self, path, max_bytes, cancel)).await
    }
}
/// Stateless evidence capture; authorization and round correlation belong to Runtime.
#[async_trait]
pub trait CachePointApi: Send + Sync {
    async fn cache_point(
        &self,
        ctx: &OpContext,
        request: CachePointRequest,
    ) -> Result<FileCachePoint, ToolError>;
}
#[async_trait]
impl<T: Host + ?Sized> CachePointApi for T {
    async fn cache_point(
        &self,
        ctx: &OpContext,
        request: CachePointRequest,
    ) -> Result<FileCachePoint, ToolError> {
        invoke(ctx, |cancel| Host::cache_point(self, request, cancel)).await
    }
}

pub trait HostInfoApi: Send + Sync {
    fn info(&self) -> Result<HostInfo, ToolError>;
    fn tool_capabilities(&self) -> Vec<String>;
}
impl<T: Host + ?Sized> HostInfoApi for T {
    fn info(&self) -> Result<HostInfo, ToolError> {
        Host::info(self)
    }
    fn tool_capabilities(&self) -> Vec<String> {
        Host::tool_capabilities(self)
    }
}
