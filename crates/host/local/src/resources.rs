//! Typed ownership for primitive resources. IDs are never reused across hosts;
//! the table is shared only by clones of one LocalHost. No public raw lookup or
//! downcast exists. Removing an entry closes its file after admitted I/O settles.
use microcrab_host_api::{
    filesystem::FileHandle,
    types::{ErrorCategory, HandleId, HostError, OpContext, Result},
};
use std::{
    collections::HashMap,
    fs::File,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio_util::sync::CancellationToken;

#[cfg(test)]
mod tests;

const MAX_HANDLES: usize = 256;
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Default)]
pub(crate) struct HandleTable {
    files: Mutex<HashMap<FileHandle, Arc<Mutex<FileResource>>>>,
}

pub(crate) struct FileResource {
    pub(crate) file: Option<File>,
    pub(crate) path: PathBuf,
    pub(crate) owner: OpContext,
    closed: CancellationToken,
}
impl Drop for FileResource {
    fn drop(&mut self) {
        self.closed.cancel();
    }
}

fn lock_error() -> HostError {
    HostError::known(ErrorCategory::Io, "primitive file table lock poisoned")
}
fn missing() -> HostError {
    HostError::known(ErrorCategory::NotFound, "unknown or closed file handle")
}

impl HandleTable {
    pub(crate) fn insert_file(
        self: &Arc<Self>,
        file: File,
        path: PathBuf,
        owner: OpContext,
    ) -> Result<FileHandle> {
        owner.check()?;
        let closed = CancellationToken::new();
        let handle = {
            let mut entries = self.files.lock().map_err(|_| lock_error())?;
            if entries.len() >= MAX_HANDLES {
                return Err(HostError::known(
                    ErrorCategory::Io,
                    "primitive file handle limit exceeded",
                ));
            }
            let id = NEXT_ID
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
                .map_err(|_| {
                    HostError::known(ErrorCategory::Io, "primitive resource IDs exhausted")
                })?;
            let handle = FileHandle(HandleId(id));
            entries.insert(
                handle,
                Arc::new(Mutex::new(FileResource {
                    file: Some(file),
                    path,
                    owner: owner.clone(),
                    closed: closed.clone(),
                })),
            );
            handle
        };
        let table = Arc::downgrade(self);
        tokio::spawn(async move {
            let deadline = async {
                match owner.deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                _ = closed.cancelled() => return,
                _ = owner.cancel.cancelled() => {},
                _ = deadline => {},
            }
            if let Some(table) = table.upgrade() {
                let _ = tokio::task::spawn_blocking(move || table.retire_file(handle)).await;
            }
        });
        Ok(handle)
    }

    pub(crate) fn file(
        &self,
        ctx: &OpContext,
        handle: FileHandle,
    ) -> Result<Arc<Mutex<FileResource>>> {
        let entry = self
            .files
            .lock()
            .map_err(|_| lock_error())?
            .get(&handle)
            .cloned()
            .ok_or_else(missing)?;
        {
            let resource = entry.lock().map_err(|_| lock_error())?;
            if resource.owner.principal != ctx.principal {
                return Err(HostError::known(
                    ErrorCategory::Permission,
                    "file handle belongs to another principal",
                ));
            }
            resource.owner.check()?;
        }
        Ok(entry)
    }

    pub(crate) fn close_file(&self, ctx: &OpContext, handle: FileHandle) -> Result<()> {
        let resource = self
            .files
            .lock()
            .map_err(|_| lock_error())?
            .get(&handle)
            .cloned()
            .ok_or_else(missing)?;
        if resource.lock().map_err(|_| lock_error())?.owner.principal != ctx.principal {
            return Err(HostError::known(
                ErrorCategory::Permission,
                "file handle belongs to another principal",
            ));
        }
        self.retire_file(handle)
    }

    fn retire_file(&self, handle: FileHandle) -> Result<()> {
        let resource = self
            .files
            .lock()
            .map_err(|_| lock_error())?
            .remove(&handle)
            .ok_or_else(missing)?;
        let mut resource = resource.lock().map_err(|_| lock_error())?;
        resource.file.take();
        resource.closed.cancel();
        Ok(())
    }
}
