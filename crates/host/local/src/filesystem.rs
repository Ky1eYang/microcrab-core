//! Bounded ordinary-file primitives. Each blocking worker retains admission and
//! private-path policy until I/O settles, even after its caller is dropped. Byte
//! publication takes the workspace write lock, coordinating with tool CAS writes
//! and recovery; raw mutable handles are deliberately unsupported.
use crate::LocalHost;
use async_trait::async_trait;
use bytes::Bytes;
use microcrab_host_api::{
    FsApi, ToolError,
    filesystem::{DirEntry, FileHandle, FileKind, FileMeta, ListOptions, OpenOptions},
    types::{ErrorCategory, HostError, OpContext, Result},
};
use std::{
    fs,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const CHUNK_BYTES: usize = 1024 * 1024;
const LIST_ENTRIES: usize = 4096;
const LIST_BYTES: usize = 1024 * 1024;

fn failure(message: &'static str) -> HostError {
    HostError::known(ErrorCategory::Io, message)
}
fn tool_error(error: ToolError) -> HostError {
    error.into_host_error()
}
fn admitted(host: &LocalHost, protected: &crate::paths::Policy, path: &Path) -> Result<PathBuf> {
    crate::paths::admit(&host.root, path, protected).map_err(tool_error)
}
fn metadata(path: &Path) -> Result<FileMeta> {
    let metadata = fs::symlink_metadata(path)?;
    let kind = if metadata.is_dir() {
        FileKind::Directory
    } else if metadata.is_file() {
        // Reuse the ordinary-file/hard-link checks, including Win32 handle checks.
        crate::workspace::open_regular(path).map_err(tool_error)?;
        FileKind::File
    } else {
        return Err(HostError::unsupported_known("filesystem.special-file"));
    };
    Ok(FileMeta {
        kind,
        size: metadata.len(),
        modified: metadata.modified().ok(),
    })
}

impl LocalHost {
    pub(crate) async fn read_private_file_owned(
        &self,
        path: PathBuf,
        mut buffer: microcrab_host_api::PrivateFileBuffer,
        ctx: &OpContext,
    ) -> std::result::Result<microcrab_host_api::PrivateFileBuffer, ToolError> {
        self.filesystem_work(ctx, false, move |host, ctx, protected| {
            if buffer.max_bytes() == 0 || buffer.max_bytes() > crate::text::HARD_BYTES {
                return Err(ToolError::FileLimit.into_host_error());
            }
            // Unlike ordinary FsApi calls, a private read never inherits the
            // unrestricted filesystem setting. Keep the same protected policy.
            let path = crate::paths::resolve(&host.root, &path).map_err(tool_error)?;
            if crate::paths::protected(&path, protected) {
                return Err(HostError::known(
                    ErrorCategory::Permission,
                    "target path is protected by Host private-directory policy",
                ));
            }
            let mut file = crate::workspace::open_regular(&path).map_err(tool_error)?;
            let initial_length = file.metadata()?.len();
            let length = usize::try_from(initial_length)
                .map_err(|_| ToolError::FileLimit.into_host_error())?;
            let data = buffer.allocate(length).map_err(tool_error)?;
            // Read directly into the one precharged exact-size allocation. If the
            // length changes, reject instead of delivering a prefix or reallocating.
            for chunk in data.chunks_mut(8192) {
                ctx.check()?;
                file.read_exact(chunk)?;
            }
            if file.metadata()?.len() != initial_length {
                return Err(ToolError::FileConflict.into_host_error());
            }
            ctx.check()?;
            Ok(buffer)
        })
        .await
        .map_err(ToolError::Host)
    }

    async fn filesystem_work<T: Send + 'static>(
        &self,
        ctx: &OpContext,
        exclusive: bool,
        work: impl FnOnce(&LocalHost, &OpContext, &crate::paths::Policy) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        ctx.check()?;
        let mut scoped = ctx.clone();
        scoped.cancel = ctx.cancel.child_token();
        let cancel_on_drop = scoped.cancel.clone().drop_guard();
        let (permit, shared, write) = scoped
            .run(async {
                let permit = self
                    .permits
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| failure("filesystem admission closed"))?;
                let (shared, write) = if exclusive {
                    (None, Some(self.workspace.clone().write_owned().await))
                } else {
                    (Some(self.workspace.clone().read_owned().await), None)
                };
                Ok((permit, shared, write))
            })
            .await?;
        let host = self.clone();
        let result = tokio::task::spawn_blocking(move || {
            let (_permit, _shared, _write) = (permit, shared, write);
            let protected = host
                .protected_paths
                .read()
                .map_err(|_| failure("private path policy lock poisoned"))?;
            scoped.check()?;
            work(&host, &scoped, &protected)
        })
        .await
        .map_err(|error| {
            HostError::new(
                ErrorCategory::Io,
                format!("filesystem worker failed: {error}"),
            )
            .with_public_reason("filesystem worker failed")
        })?;
        // Successful opens retain their context until explicit close, owner
        // cancellation/deadline, or the last Host clone is dropped.
        if result.is_ok() {
            cancel_on_drop.disarm();
        }
        result
    }
}

#[async_trait]
impl FsApi for LocalHost {
    async fn stat(&self, ctx: &OpContext, path: &Path) -> Result<FileMeta> {
        let path = path.to_path_buf();
        self.filesystem_work(ctx, false, move |host, ctx, protected| {
            let path = admitted(host, protected, &path)?;
            let output = metadata(&path)?;
            ctx.check()?;
            Ok(output)
        })
        .await
    }

    async fn list(
        &self,
        ctx: &OpContext,
        path: &Path,
        options: ListOptions,
    ) -> Result<Vec<DirEntry>> {
        ctx.check()?;
        if options.recursive {
            return Err(HostError::unsupported_known("filesystem.list.recursive"));
        }
        let path = path.to_path_buf();
        self.filesystem_work(ctx, false, move |host, ctx, protected| {
            let path = admitted(host, protected, &path)?;
            let mut output = Vec::new();
            let mut bytes = 0usize;
            for (observed, entry) in fs::read_dir(path)?.enumerate() {
                ctx.check()?;
                if observed >= 16_384 {
                    return Err(failure("directory scan entry limit exceeded"));
                }
                let entry = entry?;
                let path = entry.path();
                if crate::paths::protected(&path, protected) {
                    continue;
                }
                let name = entry
                    .file_name()
                    .into_string()
                    .map_err(|_| failure("directory names must be Unicode"))?;
                if !options.include_hidden && name.starts_with('.') {
                    continue;
                }
                let info = fs::symlink_metadata(&path)?;
                if crate::paths::reparse(&info) || (!info.is_dir() && !info.is_file()) {
                    continue;
                }
                let path = admitted(host, protected, &path)?;
                let kind = metadata(&path)?.kind;
                if output.len() == LIST_ENTRIES || name.len() > LIST_BYTES.saturating_sub(bytes) {
                    return Err(failure("directory result limit exceeded"));
                }
                bytes += name.len();
                output.push(DirEntry { name, kind });
            }
            output.sort_by(|left, right| left.name.cmp(&right.name));
            ctx.check()?;
            Ok(output)
        })
        .await
    }

    async fn open(&self, ctx: &OpContext, path: &Path, options: OpenOptions) -> Result<FileHandle> {
        ctx.check()?;
        if !options.read
            || options.write
            || options.create
            || options.truncate
            || options.append
            || options.exclusive
        {
            return Err(HostError::unsupported_known("filesystem.open.mutable"));
        }
        let path = path.to_path_buf();
        self.filesystem_work(ctx, false, move |host, ctx, protected| {
            let path = admitted(host, protected, &path)?;
            let file = crate::workspace::open_regular(&path).map_err(tool_error)?;
            if file.metadata()?.len() > crate::text::HARD_BYTES as u64 {
                return Err(failure("file byte limit exceeded"));
            }
            host.resources.insert_file(file, path, ctx.clone())
        })
        .await
    }

    async fn read(
        &self,
        ctx: &OpContext,
        handle: FileHandle,
        offset: u64,
        len: usize,
    ) -> Result<Bytes> {
        ctx.check()?;
        if len > CHUNK_BYTES
            || offset > crate::text::HARD_BYTES as u64
            || len as u64 > (crate::text::HARD_BYTES as u64).saturating_sub(offset)
        {
            return Err(failure("file read range exceeds backend limit"));
        }
        self.filesystem_work(ctx, false, move |host, ctx, protected| {
            let entry = host.resources.file(ctx, handle)?;
            let mut resource = entry
                .lock()
                .map_err(|_| failure("file resource lock poisoned"))?;
            // Registration applies to already-open handles too. Policy stays held
            // until the worker finishes; protected bytes cannot escape via clones.
            admitted(host, protected, &resource.path)?;
            let owner = resource.owner.clone();
            owner.check()?;
            let file = resource
                .file
                .as_mut()
                .ok_or_else(|| HostError::known(ErrorCategory::NotFound, "closed file handle"))?;
            if file.metadata()?.len() > crate::text::HARD_BYTES as u64 {
                return Err(failure("file byte limit exceeded"));
            }
            file.seek(SeekFrom::Start(offset))?;
            let mut output = vec![0u8; len];
            let mut used = 0;
            while used < len {
                ctx.check()?;
                owner.check()?;
                let end = (used + 8192).min(len);
                let count = file.read(&mut output[used..end])?;
                if count == 0 {
                    break;
                }
                used += count;
            }
            ctx.check()?;
            owner.check()?;
            output.truncate(used);
            Ok(Bytes::from(output))
        })
        .await
    }

    async fn close(&self, ctx: &OpContext, handle: FileHandle) -> Result<()> {
        // Resource release remains available after cancellation/deadline. It does
        // not admit new filesystem access and still validates principal ownership.
        let resources = self.resources.clone();
        let ctx = ctx.clone();
        tokio::task::spawn_blocking(move || resources.close_file(&ctx, handle))
            .await
            .map_err(|error| {
                HostError::new(
                    ErrorCategory::Io,
                    format!("file close worker failed: {error}"),
                )
                .with_public_reason("file close worker failed")
            })?
    }

    async fn atomic_write(&self, ctx: &OpContext, path: &Path, data: Bytes) -> Result<()> {
        ctx.check()?;
        if data.len() > crate::text::HARD_BYTES {
            return Err(failure("file byte limit exceeded"));
        }
        let path = path.to_path_buf();
        self.filesystem_work(ctx, true, move |host, ctx, protected| {
            let destination = admitted(host, protected, &path)?;
            if destination == host.root {
                return Err(HostError::known(
                    ErrorCategory::Permission,
                    "cannot replace workspace root",
                ));
            }
            let permissions = match fs::symlink_metadata(&destination) {
                Ok(_) => Some(
                    crate::workspace::open_regular(&destination)
                        .map_err(tool_error)?
                        .metadata()?
                        .permissions(),
                ),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.into()),
            };
            #[cfg(windows)]
            if permissions.as_ref().is_some_and(fs::Permissions::readonly) {
                return Err(HostError::known(
                    ErrorCategory::Permission,
                    "destination is read-only",
                ));
            }
            let parent = destination
                .parent()
                .ok_or_else(|| failure("destination has no parent"))?;
            let mut staged = tempfile::Builder::new()
                .prefix(".microcrab-write-")
                .tempfile_in(parent)?;
            for chunk in data.chunks(8192) {
                ctx.check()?;
                staged.write_all(chunk)?;
            }
            if let Some(permissions) = permissions {
                staged.as_file().set_permissions(permissions)?;
            }
            staged.as_file().sync_all()?;
            if admitted(host, protected, &path)? != destination {
                return Err(HostError::known(
                    ErrorCategory::Permission,
                    "destination admission changed",
                ));
            }
            // Close before publication. TempPath owns removal on all failures,
            // including dropped callers; persist is the sole commit boundary.
            let temporary = staged.into_temp_path();
            ctx.check()?;
            temporary
                .persist(&destination)
                .map_err(|error| HostError::from(error.error))?;
            Ok(())
        })
        .await
    }
}
