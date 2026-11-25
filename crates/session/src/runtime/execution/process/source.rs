//! Host processes are private output sources; public identity belongs to execution.
#[cfg(test)]
mod tests;
use super::registry::ProcessRegistry;
use crate::{
    Error,
    runtime::execution::{ExecutionCapabilities, ExecutionController, OutputWriter},
};
use async_trait::async_trait;
use microcrab_host_api::{
    ProcessApi, ShellApi,
    process::{ProcessHandle, ProcessState},
};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

pub(super) struct ProcessController {
    pub registry: Arc<ProcessRegistry>,
    pub session: String,
    pub handle: ProcessHandle,
    pub captured: bool,
    pub private: bool,
    pub stopping: CancellationToken,
    cancel_state: Mutex<CancelState>,
    cancel_changed: tokio::sync::Notify,
}

#[derive(Default)]
struct CancelState {
    pending: usize,
    confirmed: bool,
}

// The caller owns each attempt, including synchronous settlement when its future
// is dropped. An error or lost acknowledgement cannot confirm cancellation or
// erase an acknowledgement already received by another attempt.
struct CancelAttempt<'a> {
    controller: &'a ProcessController,
    acknowledged: bool,
}
impl<'a> CancelAttempt<'a> {
    fn new(controller: &'a ProcessController) -> Self {
        controller
            .cancel_state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .pending += 1;
        Self {
            controller,
            acknowledged: false,
        }
    }
}
impl Drop for CancelAttempt<'_> {
    fn drop(&mut self) {
        {
            let mut state = self
                .controller
                .cancel_state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.pending -= 1;
            state.confirmed |= self.acknowledged;
        }
        self.controller.cancel_changed.notify_waiters();
    }
}
impl ProcessController {
    pub fn new(
        registry: Arc<ProcessRegistry>,
        session: String,
        handle: ProcessHandle,
        captured: bool,
    ) -> Self {
        Self {
            registry,
            session,
            handle,
            captured,
            private: false,
            stopping: CancellationToken::new(),
            cancel_state: Mutex::new(CancelState::default()),
            cancel_changed: tokio::sync::Notify::new(),
        }
    }

    async fn cancellation_confirmed(&self) -> Result<bool, Error> {
        loop {
            // Register before inspecting state: notify_waiters retains no permit.
            let changed = self.cancel_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.stopping.is_cancelled() {
                return Err(Error::Cancelled);
            }
            {
                let state = self
                    .cancel_state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if state.confirmed || state.pending == 0 {
                    return Ok(state.confirmed);
                }
            }
            tokio::select! {
                biased;
                _ = self.stopping.cancelled() => return Err(Error::Cancelled),
                _ = changed => {}
            }
        }
    }
}
#[async_trait]
impl ExecutionController for ProcessController {
    fn capabilities(&self) -> ExecutionCapabilities {
        ExecutionCapabilities {
            input: !self.private,
        }
    }
    async fn write(
        &self,
        data: String,
        eof: bool,
        cancellation: CancellationToken,
    ) -> Result<(), Error> {
        if self.private {
            return Err(Error::denied("private shell does not accept input"));
        }
        self.registry
            .source_write(
                &self.session,
                self.handle,
                self.captured,
                data,
                eof,
                cancellation,
            )
            .await
    }
    async fn cancel(&self, cancellation: CancellationToken) -> Result<(), Error> {
        let mut attempt = CancelAttempt::new(self);
        let result = self
            .registry
            .source_cancel(&self.session, self.handle, cancellation)
            .await;
        attempt.acknowledged = result.is_ok();
        result
    }
    async fn release(&self, cancellation: CancellationToken) -> Result<(), Error> {
        self.stopping.cancel();
        self.registry
            .source_release(&self.session, self.handle, cancellation)
            .await
    }
}

pub(super) async fn pump(
    controller: Arc<ProcessController>,
    channels: Vec<String>,
    writer: OutputWriter,
) -> Result<(), Error> {
    let registry = &controller.registry;
    let host = registry.source_host()?;
    if controller.captured {
        let mut changes = ProcessApi::watch(host.as_ref(), controller.handle)
            .map_err(|error| Error::from_host_error(error.into()))?;
        let mut loss_seen = false;
        let mut expired_through = 0;
        loop {
            let ctx = registry.source_context(CancellationToken::new());
            let status = ProcessApi::status(host.as_ref(), &ctx, controller.handle)
                .await
                .map_err(|error| Error::from_host_error(error.into()))?;
            let output = ShellApi::drain(
                host.as_ref(),
                &ctx,
                controller.handle,
                registry.source_output_limit(),
            )
            .await
            .map_err(|error| Error::from_host_error(error.into()))?;
            // Host truncation is sticky; only a first flag or a fresh explicit
            // gap witnesses loss. Aggregate capture cannot locate per-pipe bytes.
            let gap = output
                .expired
                .as_ref()
                .map_or(0, |range| range.through_line);
            if (output.truncated && !loss_seen) || gap > expired_through {
                writer.mark_truncated()?;
                loss_seen = true;
            }
            expired_through = expired_through.max(gap);
            writer.append(Some(&channels[0]), &output.stdout)?;
            writer.append(Some(&channels[1]), &output.stderr)?;
            if !matches!(status.state, ProcessState::Running) {
                // Process terminal publication includes pipe closure in Host implementations.
                let final_capture =
                    ShellApi::capture_status(host.as_ref(), &ctx, controller.handle)
                        .await
                        .map_err(|error| Error::from_host_error(error.into()))?;
                if final_capture.stdout_bytes == 0 && final_capture.stderr_bytes == 0 {
                    break;
                }
                continue;
            }
            tokio::select! {biased;_=controller.stopping.cancelled()=>return Err(Error::Cancelled),changed=changes.changed()=>changed.map_err(|_|Error::tool("Host process observation closed"))?}
        }
    } else {
        let a = read_pipe(
            controller.clone(),
            false,
            channels[0].clone(),
            writer.clone(),
        );
        let b = read_pipe(
            controller.clone(),
            true,
            channels[1].clone(),
            writer.clone(),
        );
        tokio::try_join!(a, b)?;
        let ctx = registry.source_context(CancellationToken::new());
        let _ = ProcessApi::wait(host.as_ref(), &ctx, controller.handle)
            .await
            .map_err(|error| Error::from_host_error(error.into()))?;
    }
    if controller.cancellation_confirmed().await? {
        writer.confirm_cancelled()?;
    } else {
        writer.finish(Ok(()))?;
    }
    Ok(())
}
/// These streams are taken directly from the Host by the single owner; unlike
/// guest pipe imports they do not allocate a Vec per Host frame before capture.
/// Local Host bounds each frame; no raw payload enters Host/Shell diagnostic text.
pub(super) async fn pump_private(
    controller: Arc<ProcessController>,
    writer: OutputWriter,
) -> Result<(), Error> {
    let host = controller.registry.source_host()?;
    let context = controller
        .registry
        .source_context(controller.stopping.clone());
    ProcessApi::close_stdin(host.as_ref(), &context, controller.handle)
        .await
        .map_err(|_| Error::tool("private shell could not close stdin"))?;
    let owner = &controller;
    let writer_ref = &writer;
    let read = |stderr: bool| async move {
        let controller = owner;
        let writer = writer_ref;
        while let Some(frame) = controller
            .registry
            .source_read_pipe(
                &controller.session,
                controller.handle,
                stderr,
                controller.stopping.clone(),
            )
            .await
            .map_err(|_| Error::tool("private output pipe capture failed"))?
        {
            if !stderr {
                writer.append_private(&frame)?;
            }
        }
        Ok::<_, Error>(())
    };
    let wait = async {
        ProcessApi::wait(host.as_ref(), &context, controller.handle)
            .await
            .map_err(|_| Error::tool("private shell wait failed"))
    };
    let (_, _, status) = tokio::select! {
        biased;
        _ = controller.stopping.cancelled() => return Err(Error::Cancelled),
        result = async { tokio::try_join!(read(false), read(true), wait) } => result?,
    };
    if controller.cancellation_confirmed().await? {
        writer.confirm_cancelled()?;
    } else if status.code == Some(0) && status.signal.is_none() {
        writer.finish(Ok(()))?;
    } else {
        writer.finish(Err(super::super::ToolFailure::failed(
            "process_exit",
            "Private shell did not exit successfully",
        )))?;
    }
    Ok(())
}

async fn read_pipe(
    controller: Arc<ProcessController>,
    stderr: bool,
    channel: String,
    writer: OutputWriter,
) -> Result<(), Error> {
    let mut tail = Vec::new();
    loop {
        let bytes = controller
            .registry
            .source_read_pipe(
                &controller.session,
                controller.handle,
                stderr,
                controller.stopping.clone(),
            )
            .await?;
        let Some(bytes) = bytes else { break };
        tail.extend_from_slice(&bytes);
        loop {
            match std::str::from_utf8(&tail) {
                Ok(text) => {
                    writer.append(Some(&channel), text)?;
                    tail.clear();
                    break;
                }
                Err(error) => {
                    let valid = error.valid_up_to();
                    if valid > 0 {
                        let text = std::str::from_utf8(&tail[..valid])
                            .map_err(|_| Error::tool("invalid UTF-8 decoder state"))?;
                        writer.append(Some(&channel), text)?;
                        tail.drain(..valid);
                    }
                    if let Some(length) = error.error_len() {
                        writer.append(Some(&channel), "�")?;
                        tail.drain(..length);
                    } else {
                        break;
                    }
                }
            }
        }
    }
    if !tail.is_empty() {
        writer.append(Some(&channel), "�")?;
    }
    Ok(())
}
