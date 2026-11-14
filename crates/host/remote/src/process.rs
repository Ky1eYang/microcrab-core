//! Native owner ports over scoped process RPC. Watches wake observations; effects
//! are never retried after route loss. Release installs cleanup ownership first.
use super::*;
use microcrab_host_api::process::{
    ExitStatus, ProcessHandle, ProcessSpec, ProcessState, ProcessStatus, Signal,
};
use microcrab_host_api::{
    BoxStream, Bytes, ErrorCategory, HostError, OpContext, ShellCaptureStatus, ShellOutput,
    ShellPosition, ShellRequest,
};
use microcrab_host_protocol::{HostRequest, MAX_CHUNK_BYTES, ProcessChunk};

fn native_error(error: ToolError) -> HostError {
    error.into_host_error()
}
impl RemoteHost {
    async fn process_call<T: serde::de::DeserializeOwned>(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
        request: HostRequest,
    ) -> Result<T, HostError> {
        ctx.check()?;
        self.check_process(handle).map_err(native_error)?;
        ctx.run(async {
            self.typed(request, ctx.cancel.clone())
                .await
                .map_err(native_error)
        })
        .await
    }
    fn pipe(&self, handle: ProcessHandle, stderr: bool) -> Result<BoxStream<Bytes>, HostError> {
        self.check_process(handle).map_err(native_error)?;
        let mut entries = self
            .processes
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let entry = entries
            .get_mut(&handle)
            .ok_or_else(|| HostError::known(ErrorCategory::NotFound, "unknown Host process"))?;
        if entry.streams_taken[usize::from(stderr)] {
            return Err(HostError::known(
                ErrorCategory::Conflict,
                "process pipe already taken",
            ));
        }
        entry.streams_taken[usize::from(stderr)] = true;
        drop(entries);
        let host = self
            .weak_self
            .upgrade()
            .ok_or_else(|| HostError::known(ErrorCategory::Io, "Host owner unavailable"))?;
        // At most one chunk lives in this stream. No background output pump or
        // unbounded queue exists: polling the consumer admits the next RPC.
        Ok(Box::pin(microcrab_host_api::stream::unfold(
            (host, false, false),
            move |(host, opened, done)| async move {
                if done {
                    return None;
                }
                let ctx = OpContext::default();
                let result = async {
                    if !opened {
                        host.process_call::<()>(
                            &ctx,
                            handle,
                            HostRequest::ProcessStreamOpen { handle, stderr },
                        )
                        .await?;
                    }
                    let chunk: ProcessChunk = host
                        .process_call(
                            &ctx,
                            handle,
                            HostRequest::ProcessStreamRead {
                                handle,
                                stderr,
                                max_bytes: MAX_CHUNK_BYTES as u32,
                            },
                        )
                        .await?;
                    if chunk.data.len() > MAX_CHUNK_BYTES {
                        return Err(HostError::known(
                            ErrorCategory::Io,
                            "Host pipe chunk exceeds bound",
                        ));
                    }
                    Ok(chunk)
                }
                .await;
                match result {
                    Ok(chunk) if chunk.eof && chunk.data.is_empty() => None,
                    Ok(chunk) => Some((Ok(Bytes::from(chunk.data)), (host, true, chunk.eof))),
                    Err(error) => Some((Err(error), (host, true, true))),
                }
            },
        )))
    }
}
#[async_trait::async_trait]
impl ProcessApi for RemoteHost {
    fn has_running_processes(&self) -> bool {
        !self.is_connected()
            || self
                .processes
                .starts
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .values()
                .any(|start| start.cancelled)
            || !self
                .unresolved
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_empty()
            || self
                .processes
                .entries
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .values()
                .any(|entry| !entry.terminal || entry.release_pending)
    }
    fn has_unresolved_cleanup(&self) -> bool {
        self.processes
            .starts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .any(|start| start.cancelled)
            || self
                .processes
                .entries
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .values()
                .any(|entry| entry.release_pending)
    }
    async fn spawn(&self, ctx: &OpContext, spec: ProcessSpec) -> Result<ProcessHandle, HostError> {
        ctx.check()?;
        ctx.run(async {
            self.start_remote_process(HostRequest::ProcessSpawn(spec), ctx.cancel.clone())
                .await
                .map_err(native_error)
        })
        .await
    }
    fn watch(&self, handle: ProcessHandle) -> Result<tokio::sync::watch::Receiver<u64>, HostError> {
        self.check_process(handle).map_err(native_error)?;
        self.processes
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&handle)
            .map(|entry| {
                let mut receiver = entry.changes.subscribe();
                if *entry.changes.borrow() != 0 {
                    receiver.mark_changed();
                }
                receiver
            })
            .ok_or_else(|| HostError::known(ErrorCategory::NotFound, "unknown Host process"))
    }
    async fn status(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
    ) -> Result<ProcessStatus, HostError> {
        let status: ProcessStatus = self
            .process_call(ctx, handle, HostRequest::ProcessStatus { handle })
            .await?;
        let pid = self
            .processes
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&handle)
            .map(|entry| entry.pid);
        if pid != Some(status.pid) {
            return Err(HostError::known(
                ErrorCategory::Io,
                "Host process PID changed",
            ));
        }
        self.note_process_state(handle, &status.state);
        Ok(status)
    }
    async fn pid(&self, ctx: &OpContext, handle: ProcessHandle) -> Result<u32, HostError> {
        ctx.check()?;
        self.check_process(handle).map_err(native_error)?;
        let expected = self
            .processes
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&handle)
            .ok_or_else(|| HostError::known(ErrorCategory::NotFound, "unknown Host process"))?
            .pid
            .ok_or_else(|| HostError::unsupported_known("process.pid"))?;
        // A virtual handle is authoritative without an OS PID. Do not issue an
        // unsupported RPC or fabricate an identity for that process.
        let pid: u32 = self
            .process_call(ctx, handle, HostRequest::ProcessPid { handle })
            .await?;
        let current = self
            .processes
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&handle)
            .map(|entry| entry.pid);
        if expected != pid || current != Some(Some(pid)) {
            return Err(HostError::known(
                ErrorCategory::Io,
                "Host process PID changed",
            ));
        }
        Ok(pid)
    }
    async fn wait(&self, ctx: &OpContext, handle: ProcessHandle) -> Result<ExitStatus, HostError> {
        let mut changes = self.watch(handle)?;
        loop {
            changes.borrow_and_update();
            match ProcessApi::status(self, ctx, handle).await?.state {
                ProcessState::Exited { status } => return Ok(status),
                ProcessState::Failed { reason } => {
                    return Err(HostError::new(ErrorCategory::Io, reason)
                        .with_public_reason("Host process supervisor reported failure"));
                }
                ProcessState::Running => {}
            }
            ctx.run(async {
                changes
                    .changed()
                    .await
                    .map_err(|_| HostError::known(ErrorCategory::Io, "Host process watch closed"))
            })
            .await?;
        }
    }
    async fn signal(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
        signal: Signal,
    ) -> Result<(), HostError> {
        self.process_call(ctx, handle, HostRequest::ProcessSignal { handle, signal })
            .await
    }
    async fn kill_tree(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
        force: bool,
    ) -> Result<(), HostError> {
        ctx.check()?;
        self.check_process(handle).map_err(native_error)?;
        let host = self
            .weak_self
            .upgrade()
            .ok_or_else(|| HostError::known(ErrorCategory::Io, "Host owner unavailable"))?;
        // Publication remains owned after the observation future is dropped.
        let owned = tokio::spawn(async move {
            host.typed::<()>(
                HostRequest::ProcessKill { handle, force },
                CancellationToken::new(),
            )
            .await
            .map_err(native_error)
        });
        ctx.run(async {
            owned
                .await
                .map_err(|_| HostError::known(ErrorCategory::Io, "Host kill owner stopped"))?
        })
        .await
    }
    fn stdout(&self, handle: ProcessHandle) -> Result<BoxStream<Bytes>, HostError> {
        self.pipe(handle, false)
    }
    fn stderr(&self, handle: ProcessHandle) -> Result<BoxStream<Bytes>, HostError> {
        self.pipe(handle, true)
    }
    async fn write_stdin(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
        data: Bytes,
    ) -> Result<(), HostError> {
        // Sequential chunks preserve stdin order and each effect's no-replay fence.
        for chunk in data.chunks(MAX_CHUNK_BYTES) {
            self.process_call::<()>(
                ctx,
                handle,
                HostRequest::ProcessWrite {
                    handle,
                    data: chunk.to_vec(),
                },
            )
            .await?;
        }
        Ok(())
    }
    async fn close_stdin(&self, ctx: &OpContext, handle: ProcessHandle) -> Result<(), HostError> {
        self.process_call(ctx, handle, HostRequest::ProcessCloseStdin { handle })
            .await
    }
    async fn resize_pty(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
        rows: u16,
        cols: u16,
    ) -> Result<(), HostError> {
        self.process_call(
            ctx,
            handle,
            HostRequest::ProcessResize { handle, rows, cols },
        )
        .await
    }
    async fn release(&self, ctx: &OpContext, handle: ProcessHandle) -> Result<(), HostError> {
        // Install intent synchronously before any await, including ctx cancellation.
        self.release_remote_process(handle);
        let host = self
            .weak_self
            .upgrade()
            .ok_or_else(|| HostError::known(ErrorCategory::Io, "Host owner unavailable"))?;
        let owned =
            tokio::spawn(async move { host.cleanup_releases().await.map_err(native_error) });
        ctx.run(async {
            owned
                .await
                .map_err(|_| HostError::known(ErrorCategory::Io, "Host release owner stopped"))?
        })
        .await
    }
}
#[async_trait::async_trait]
impl ShellApi for RemoteHost {
    async fn launch(
        &self,
        ctx: &OpContext,
        request: ShellRequest,
    ) -> Result<ProcessHandle, HostError> {
        ctx.check()?;
        ctx.run(async {
            self.start_remote_process(HostRequest::ShellLaunch(request), ctx.cancel.clone())
                .await
                .map_err(native_error)
        })
        .await
    }
    async fn capture_status(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
    ) -> Result<ShellCaptureStatus, HostError> {
        self.process_call(ctx, handle, HostRequest::ShellCaptureStatus { handle })
            .await
    }
    async fn read(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
        from: Option<ShellPosition>,
    ) -> Result<ShellOutput, HostError> {
        self.process_call(ctx, handle, HostRequest::ShellRead { handle, from })
            .await
    }
    async fn drain(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
        max_bytes: usize,
    ) -> Result<ShellOutput, HostError> {
        if max_bytes > microcrab_host_protocol::MAX_TRANSFER_BYTES {
            return Err(HostError::known(
                ErrorCategory::Conflict,
                "shell drain byte budget exceeds relay limit",
            ));
        }
        self.process_call(
            ctx,
            handle,
            HostRequest::ShellDrain {
                handle,
                max_bytes: max_bytes as u32,
            },
        )
        .await
    }
    async fn write(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
        data: Vec<u8>,
        eof: bool,
    ) -> Result<(), HostError> {
        for chunk in data.chunks(MAX_CHUNK_BYTES) {
            self.process_call::<()>(
                ctx,
                handle,
                HostRequest::ShellWrite {
                    handle,
                    data: chunk.to_vec(),
                    eof: false,
                },
            )
            .await?;
        }
        if eof {
            self.process_call::<()>(
                ctx,
                handle,
                HostRequest::ShellWrite {
                    handle,
                    data: Vec::new(),
                    eof: true,
                },
            )
            .await?;
        }
        Ok(())
    }
}
