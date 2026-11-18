//! Read-only handles are subordinate resources retained by canonical execution.
use super::*;
use crate::runtime::execution::ExecutionController;
use std::{collections::HashMap, sync::Arc};
use tokio_util::sync::CancellationToken;
const CHUNK_BYTES: usize = 16 * 1024;
const READ_CALLS: usize = 4096;
const CLOSE_CALLS: usize = 8;
const HANDLE_COUNT: usize = 1;
const READ_BYTES: usize = crate::admission::image::MAX_IMAGE_BYTES + 1;

pub(in crate::runtime::wasm) struct Reads {
    owner: Option<Arc<ReadOwner>>,
    next: u64,
    remaining_bytes: usize,
    remaining_calls: usize,
    close_calls_remaining: usize,
}
impl Default for Reads {
    fn default() -> Self {
        Self {
            owner: None,
            next: 1,
            remaining_bytes: READ_BYTES,
            remaining_calls: READ_CALLS,
            close_calls_remaining: CLOSE_CALLS,
        }
    }
}
impl Reads {
    pub(in crate::runtime::wasm) fn detach(&mut self) {
        if let Some(owner) = self.owner.take() {
            owner.cancellation.cancel();
        }
        // Execution retains cleanup ownership independently of the Store lifetime.
    }
    pub(in crate::runtime::wasm) fn reset(&mut self) {
        self.detach();
        self.remaining_bytes = READ_BYTES;
        self.remaining_calls = READ_CALLS;
        self.close_calls_remaining = CLOSE_CALLS;
    }
    fn reserve_read(
        &mut self,
        offset: u64,
        len: usize,
    ) -> Result<(), ImportFailure<types::HostError>> {
        if len == 0
            || len > CHUNK_BYTES
            || offset > crate::admission::image::MAX_IMAGE_BYTES as u64
            || offset.saturating_add(len as u64) > READ_BYTES as u64
        {
            return Err(error(
                types::ErrorCategory::Io,
                "file read range exceeds media bounds",
            ));
        }
        crate::budget::check(crate::LimitKind::HostOperations, 1, self.remaining_calls)
            .and_then(|()| {
                crate::budget::check(crate::LimitKind::AttachmentBytes, len, self.remaining_bytes)
            })
            .map_err(|_| error(types::ErrorCategory::Io, "file read media budget exceeded"))?;
        self.remaining_calls -= 1;
        self.remaining_bytes -= len;
        Ok(())
    }
}
impl Drop for Reads {
    fn drop(&mut self) {
        self.detach();
    }
}
struct Entry {
    handle: host::filesystem::FileHandle,
    context: host::OpContext,
    backend: Arc<dyn host::Host>,
}
struct ReadOwner {
    cancellation: CancellationToken,
    // Gate joins in-flight file operations; failed closes keep their exact entry.
    entries: tokio::sync::Mutex<HashMap<u64, Entry>>,
}
impl ReadOwner {
    async fn close_id(&self, id: u64) -> Result<(), host::HostError> {
        let mut entries = self.entries.lock().await;
        let entry = entries.get(&id).ok_or_else(|| {
            host::HostError::new(
                host::ErrorCategory::Permission,
                "file handle is foreign, expired or closed",
            )
        })?;
        FsApi::close(
            entry.backend.as_ref(),
            &cleanup_context(&entry.context),
            entry.handle,
        )
        .await?;
        entry.context.cancel.cancel();
        entries.remove(&id);
        Ok(())
    }
}
#[async_trait::async_trait]
impl ExecutionController for ReadOwner {
    async fn cancel(&self, _: CancellationToken) -> Result<(), crate::Error> {
        self.cancellation.cancel();
        Ok(())
    }
    async fn release(&self, _: CancellationToken) -> Result<(), crate::Error> {
        self.cancellation.cancel();
        let mut entries = self.entries.lock().await;
        let ids: Vec<_> = entries.keys().copied().collect();
        let mut first = None;
        for id in ids {
            let entry = entries
                .get(&id)
                .expect("identity selected under the same owner gate");
            match FsApi::close(
                entry.backend.as_ref(),
                &cleanup_context(&entry.context),
                entry.handle,
            )
            .await
            {
                Ok(()) => {
                    entry.context.cancel.cancel();
                    entries.remove(&id);
                }
                Err(source) => {
                    first.get_or_insert_with(|| {
                        crate::Error::from_host_error(host::ToolError::Host(source))
                    });
                }
            }
        }
        first.map_or(Ok(()), Err)
    }
}
impl Drop for ReadOwner {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}
fn cleanup_context(context: &host::OpContext) -> host::OpContext {
    host::OpContext {
        deadline: None,
        cancel: CancellationToken::new(),
        ..context.clone()
    }
}
fn read_only(flags: fs::OpenFlags) -> Result<(), ImportFailure<types::HostError>> {
    if flags != fs::OpenFlags::READ {
        return Err(error(
            types::ErrorCategory::Permission,
            "only read-only file opens are supported",
        ));
    }
    Ok(())
}
fn owner(state: &mut RuntimeState) -> Result<Arc<ReadOwner>, ImportFailure<types::HostError>> {
    if let Some(owner) = &state.file_reads.owner {
        return Ok(owner.clone());
    }
    let context = state.tool_context.as_ref().ok_or_else(|| {
        error(
            types::ErrorCategory::Permission,
            "file cleanup requires an admitted invocation",
        )
    })?;
    let owner = Arc::new(ReadOwner {
        cancellation: state.cancellation.child_token(),
        entries: Default::default(),
    });
    // Registration precedes file effects. Store destruction cannot erase retries.
    context
        .retain_cleanup(owner.clone())
        .map_err(|source| private_error(types::ErrorCategory::Io, &source.to_string()))?;
    state.file_reads.owner = Some(owner.clone());
    Ok(owner)
}
pub(in crate::runtime::wasm) fn open(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (path, flags): (String, fs::OpenFlags),
) -> wasmtime::Result<(Result<fs::FileHandle, types::HostError>,)> {
    let state = caller.data_mut();
    let result = (|| {
        let mut context = begin(state, path.len().saturating_add(16))?;
        read_only(flags)?;
        super::super::operations::resource(&path).map_err(|source| {
            private_error(
                types::ErrorCategory::Permission,
                &super::super::encode_diagnostic(&super::super::operations::diagnostic(&source)),
            )
        })?;
        crate::budget::check(
            crate::LimitKind::ToolOutput,
            8,
            state.output_bytes_remaining,
        )
        .map_err(|_| error(types::ErrorCategory::Io, "Host response budget exceeded"))?;
        let id = state.file_reads.next;
        let next = id
            .checked_add(1)
            .ok_or_else(|| error(types::ErrorCategory::Io, "file handle identities exhausted"))?;
        let backend = state
            .host
            .clone()
            .ok_or_else(|| error(types::ErrorCategory::Unsupported, "Host unavailable"))?;
        let runtime = state.runtime.clone().ok_or_else(|| {
            error(
                types::ErrorCategory::Unsupported,
                "Host runtime unavailable",
            )
        })?;
        let owner = owner(state)?;
        // The resource lifetime is closed by the execution owner after active
        // operations join. Cancelling the invocation must not independently retire
        // the native handle before its exact close receipt is acknowledged.
        context.cancel = CancellationToken::new();
        let epoch = state.epoch_cancellation.clone();
        let guard = context.cancel.clone().drop_guard();
        let started = Instant::now();
        let mut normalization_diagnostic = None;
        let opened = runtime.block_on(async {
            let mut entries = owner.entries.lock().await;
            crate::budget::check(crate::LimitKind::AttachmentCount, entries.len().saturating_add(1), HANDLE_COUNT)
                .map_err(|_| error(types::ErrorCategory::Io, "file read handle capacity exceeded"))?;
            let handle = tokio::select! { biased;
                _ = epoch.cancelled() => Err(error(types::ErrorCategory::Cancelled, "Host file open cancelled")),
                _ = owner.cancellation.cancelled() => Err(error(types::ErrorCategory::Cancelled, "Host file open cancelled")),
                result = context.run(async {
                    let actual = backend.normalize_resource(path.into(), context.cancel.clone()).await
                        .map_err(|source| normalization_error(source, &mut normalization_diagnostic))?;
                    FsApi::open(backend.as_ref(), &context, Path::new(&actual), host::filesystem::OpenOptions { read: true, ..Default::default() }).await
                }) => result.map_err(host_error),
            }?;
            entries.insert(id, Entry { handle, context: context.clone(), backend });
            Ok(())
        });
        state.operation_diagnostic = normalization_diagnostic;
        exclude_host_wait(&mut state.deadline, started.elapsed(), opened)?;
        let _ = guard.disarm();
        state.file_reads.next = next;
        state.output_bytes_remaining -= 8;
        Ok(fs::FileHandle { id })
    })();
    finish(state, result)
}
pub(in crate::runtime::wasm) fn read(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (handle, offset, len): (fs::FileHandle, u64, u32),
) -> wasmtime::Result<(Result<Vec<u8>, types::HostError>,)> {
    let state = caller.data_mut();
    let result = (|| {
        let context = begin_scope(state)?;
        crate::budget::check(
            crate::LimitKind::ToolArguments,
            24,
            state.input_bytes_remaining,
        )
        .map_err(|_| {
            error(
                types::ErrorCategory::Io,
                "Host invocation input budget exceeded",
            )
        })?;
        let owner = state.file_reads.owner.clone().ok_or_else(|| {
            error(
                types::ErrorCategory::Permission,
                "file handle is foreign, expired or closed",
            )
        })?;
        let runtime = state.runtime.clone().ok_or_else(|| {
            error(
                types::ErrorCategory::Unsupported,
                "Host runtime unavailable",
            )
        })?;
        state.file_reads.reserve_read(offset, len as usize)?;
        state.input_bytes_remaining -= 24;
        let epoch = state.epoch_cancellation.clone();
        let _guard = context.cancel.clone().drop_guard();
        let started = Instant::now();
        let result = runtime.block_on(async {
            let entries = owner.entries.lock().await;
            let entry = entries.get(&handle.id).ok_or_else(|| error(types::ErrorCategory::Permission, "file handle is foreign, expired or closed"))?;
            tokio::select! { biased;
                _ = epoch.cancelled() => Err(error(types::ErrorCategory::Cancelled, "Host file read cancelled")),
                _ = owner.cancellation.cancelled() => Err(error(types::ErrorCategory::Cancelled, "Host file read cancelled")),
                result = context.run(FsApi::read(entry.backend.as_ref(), &context, entry.handle, offset, len as usize)) => result.map_err(host_error),
            }
        });
        let bytes = exclude_host_wait(&mut state.deadline, started.elapsed(), result)?;
        if bytes.len() > len as usize {
            return Err(error(
                types::ErrorCategory::Io,
                "Host file read exceeded requested byte count",
            ));
        }
        Ok(bytes.to_vec())
    })();
    finish(state, result)
}
pub(in crate::runtime::wasm) fn close(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (handle,): (fs::FileHandle,),
) -> wasmtime::Result<(Result<(), types::HostError>,)> {
    let state = caller.data_mut();
    let result = (|| {
        let owner = state.file_reads.owner.clone().ok_or_else(|| {
            error(
                types::ErrorCategory::Permission,
                "file handle is foreign, expired or closed",
            )
        })?;
        let runtime = state.runtime.clone().ok_or_else(|| {
            error(
                types::ErrorCategory::Unsupported,
                "Host runtime unavailable",
            )
        })?;
        crate::budget::check(
            crate::LimitKind::HostOperations,
            1,
            state.file_reads.close_calls_remaining,
        )
        .map_err(|_| {
            error(
                types::ErrorCategory::Io,
                "file close attempt budget exceeded; execution retains cleanup ownership",
            )
        })?;
        state.file_reads.close_calls_remaining -= 1;
        let started = Instant::now();
        let closed = runtime
            .block_on(owner.close_id(handle.id))
            .map_err(host_error);
        exclude_host_wait(&mut state.deadline, started.elapsed(), closed)
    })();
    finish(state, result)
}
pub(in crate::runtime::wasm) fn cleanup(state: &mut RuntimeState) -> Result<(), crate::Error> {
    let Some(owner) = state.file_reads.owner.clone() else {
        return Ok(());
    };
    let runtime = state
        .runtime
        .clone()
        .ok_or_else(|| crate::Error::tool("file cleanup runtime unavailable"))?;
    let started = Instant::now();
    let result = runtime.block_on(owner.release(CancellationToken::new()));
    state.deadline += started.elapsed();
    result
}
#[cfg(test)]
mod tests;
