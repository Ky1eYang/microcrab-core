//! Filesystem primitives. Paths belong to the executing backend's namespace.
use crate::types::{ErrorCategory, HandleId, HostError, OpContext, Result};
use async_trait::async_trait;
use bytes::Bytes;
use std::{
    path::{Path, PathBuf},
    time::SystemTime,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileHandle(pub HandleId);
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenOptions {
    pub read: bool,
    pub write: bool,
    pub create: bool,
    pub truncate: bool,
    pub append: bool,
    pub exclusive: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    File,
    Directory,
    Symlink,
    Other,
}
#[derive(Debug, Clone)]
pub struct FileMeta {
    pub kind: FileKind,
    pub size: u64,
    pub modified: Option<SystemTime>,
}
#[derive(Debug, Clone)]
pub struct DirEntry {
    pub name: String,
    pub kind: FileKind,
}
#[derive(Debug, Clone, Copy, Default)]
pub struct ListOptions {
    pub recursive: bool,
    pub include_hidden: bool,
}

#[async_trait]
pub trait FsApi: Send + Sync {
    async fn stat(&self, _ctx: &OpContext, _path: &Path) -> Result<FileMeta> {
        Err(HostError::unsupported_known("filesystem.stat"))
    }
    async fn list(
        &self,
        _ctx: &OpContext,
        _path: &Path,
        _options: ListOptions,
    ) -> Result<Vec<DirEntry>> {
        Err(HostError::unsupported_known("filesystem.list"))
    }
    /// Open handles inherit context cancellation; the backend closes them even if
    /// an owning operation future is dropped. Owner/kind/liveness are validated.
    async fn open(
        &self,
        _ctx: &OpContext,
        _path: &Path,
        _options: OpenOptions,
    ) -> Result<FileHandle> {
        Err(HostError::unsupported_known("filesystem.open"))
    }
    async fn read(
        &self,
        _ctx: &OpContext,
        _handle: FileHandle,
        _offset: u64,
        _len: usize,
    ) -> Result<Bytes> {
        Err(HostError::unsupported_known("filesystem.read"))
    }
    /// May return a short write; a nonempty zero-progress write is an error for
    /// composed helpers. Offsets and append behavior must follow open options.
    async fn write(
        &self,
        _ctx: &OpContext,
        _handle: FileHandle,
        _offset: u64,
        _data: Bytes,
    ) -> Result<usize> {
        Err(HostError::unsupported_known("filesystem.write"))
    }
    async fn fsync(&self, _ctx: &OpContext, _handle: FileHandle) -> Result<()> {
        Err(HostError::unsupported_known("filesystem.fsync"))
    }
    async fn close(&self, _ctx: &OpContext, _handle: FileHandle) -> Result<()> {
        Err(HostError::unsupported_known("filesystem.close"))
    }
    async fn mkdir(&self, _ctx: &OpContext, _path: &Path, _recursive: bool) -> Result<()> {
        Err(HostError::unsupported_known("filesystem.mkdir"))
    }
    /// Atomically replace an existing ordinary-file destination on the same
    /// filesystem, or return Unsupported when the platform cannot guarantee it.
    /// Once committed, report success despite late cancellation. No CAS or
    /// durable directory fsync guarantee is implied.
    async fn rename(&self, _ctx: &OpContext, _from: &Path, _to: &Path) -> Result<()> {
        Err(HostError::unsupported_known("filesystem.rename"))
    }
    async fn remove(&self, _ctx: &OpContext, _path: &Path, _recursive: bool) -> Result<()> {
        Err(HostError::unsupported_known("filesystem.remove"))
    }
    /// Exclusively create an unpredictable ordinary file in exactly dir. Context
    /// cancellation closes/removes this temporary even after close, until rename
    /// commits. This backend ownership rule covers dropped helper futures.
    async fn create_temp(&self, _ctx: &OpContext, _dir: &Path) -> Result<(PathBuf, FileHandle)> {
        Err(HostError::unsupported_known("filesystem.create_temp"))
    }
    /// Publish only complete bytes. Close before rename, retry short writes, and
    /// attempt close/remove after every pre-publication failure without replacing
    /// its error. Successful rename is the publication boundary.
    async fn atomic_write(&self, ctx: &OpContext, path: &Path, data: Bytes) -> Result<()> {
        ctx.check()?;
        let dir = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let scoped = ctx.child();
        let _cancel_on_drop = scoped.cancel.clone().drop_guard();
        let (temporary, handle) = scoped.run(self.create_temp(&scoped, dir)).await?;
        let cleanup = ctx.cleanup();
        let mut closed = false;
        let result = async {
            let mut offset = 0;
            while offset < data.len() {
                scoped.check()?;
                let written = scoped
                    .run(self.write(&scoped, handle, offset as u64, data.slice(offset..)))
                    .await?;
                if written == 0 || written > data.len() - offset {
                    return Err(HostError::known(
                        ErrorCategory::Io,
                        "invalid atomic_write progress",
                    ));
                }
                offset += written;
            }
            scoped.run(self.fsync(&scoped, handle)).await?;
            self.close(&cleanup, handle).await?;
            closed = true;
            scoped.check()?;
            // Do not wrap publication in a cancellation race: rename reports the
            // committed outcome, including cancellation arriving after commit.
            self.rename(&scoped, &temporary, path).await
        }
        .await;
        if result.is_err() {
            if !closed {
                let _ = self.close(&cleanup, handle).await;
            }
            let _ = self.remove(&cleanup, &temporary, false).await;
        }
        result
    }
}
