//! Typed Runtime storage handles. No guest pointer, JSON field or tool name is authority.
use super::{RuntimeState, microcrab::agent::execution as abi};
use crate::{
    Error,
    runtime::execution::{
        BufferedOutput, ExecutionCapabilities, ExecutionController, FinalOutput, OutputOptions,
        OutputWriter, ToolFailure, ToolFailureKind, ToolOutput,
    },
};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;
use wasmtime::{
    StoreContextMut,
    component::{Linker, Resource, ResourceType},
};

pub enum BufferResource {}
pub(super) struct Entry {
    pub writer: OutputWriter,
    pub output: Option<BufferedOutput>,
    pub controller: Arc<Controller>,
    pub channels: Vec<String>,
    pub cleanup: Option<(
        tokio::runtime::Handle,
        Arc<dyn crate::runtime::execution::process::ProcessControl>,
    )>,
    pub retained: std::collections::HashMap<u64, (Vec<String>, serde_json::Value)>,
    pub driver: Option<Vec<u8>>,
    pub attached: bool,
}
impl Drop for Entry {
    fn drop(&mut self) {
        let Some((runtime, processes)) = self.cleanup.take() else {
            return;
        };
        let handles: Vec<_> = self.retained.keys().copied().collect();
        if handles.is_empty() {
            return;
        }
        runtime.spawn(async move {
            for id in handles {
                let handle =
                    microcrab_host_api::process::ProcessHandle(microcrab_host_api::HandleId(id));
                if processes.owns_process(handle)
                    && processes
                        .release_process(handle, CancellationToken::new())
                        .await
                        .is_err()
                {
                    tracing::warn!("Retained guest source cleanup failed");
                }
            }
        });
    }
}
pub(super) struct Controller {
    pub cancellation: CancellationToken,
    delegate: Mutex<Option<Arc<dyn ExecutionController>>>,
}
impl Controller {
    fn new() -> Self {
        Self {
            cancellation: CancellationToken::new(),
            delegate: Mutex::new(None),
        }
    }
    fn delegate(&self) -> Option<Arc<dyn ExecutionController>> {
        self.delegate
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}
#[async_trait]
impl ExecutionController for Controller {
    fn capabilities(&self) -> ExecutionCapabilities {
        self.delegate()
            .map_or_else(ExecutionCapabilities::default, |d| d.capabilities())
    }
    async fn write(&self, data: String, eof: bool, cancel: CancellationToken) -> Result<(), Error> {
        let delegate = self
            .delegate()
            .ok_or_else(|| Error::tool("execution does not support input"))?;
        delegate.write(data, eof, cancel).await
    }
    async fn cancel(&self, cancel: CancellationToken) -> Result<(), Error> {
        self.cancellation.cancel();
        if let Some(delegate) = self.delegate() {
            delegate.cancel(cancel).await?;
        }
        Ok(())
    }
    async fn release(&self, cancel: CancellationToken) -> Result<(), Error> {
        self.cancellation.cancel();
        if let Some(delegate) = self.delegate() {
            delegate.release(cancel).await?;
        }
        Ok(())
    }
}
fn failure(code: &str, message: impl Into<String>) -> abi::ToolFailure {
    abi::ToolFailure {
        kind: abi::FailureKind::Failed,
        code: code.into(),
        message: message.into(),
        retryable: false,
    }
}
pub(super) fn native_failure(value: abi::ToolFailure) -> Result<ToolFailure, Error> {
    let result = ToolFailure {
        kind: match value.kind {
            abi::FailureKind::Failed => ToolFailureKind::Failed,
            abi::FailureKind::Timeout => ToolFailureKind::Timeout,
            abi::FailureKind::Cancelled => ToolFailureKind::Cancelled,
        },
        code: value.code,
        message: value.message,
        retryable: value.retryable,
    };
    result.validate()?;
    Ok(result)
}
fn context(state: &RuntimeState) -> Result<super::ToolContext, abi::ToolFailure> {
    if state.cancellation.is_cancelled() || state.epoch_cancellation.is_cancelled() {
        return Err(failure("cancelled", "Execution cancelled"));
    }
    if state
        .tool_context
        .as_ref()
        .is_some_and(|context| !context.subject.is_tool())
    {
        return Err(failure(
            "unsupported",
            "Initialization has scope-only output and processes",
        ));
    }
    let context = state
        .tool_context
        .clone()
        .ok_or_else(|| failure("denied", "Output requires an authorized invocation"))?;
    context
        .ensure_admitted(None)
        .map_err(|error| failure("denied", error.public_reason()))?;
    Ok(context)
}
fn entry<'a>(
    state: &'a RuntimeState,
    resource: &Resource<BufferResource>,
) -> Result<&'a Entry, abi::ToolFailure> {
    context(state)?;
    state.outputs.get(&resource.rep()).ok_or_else(|| {
        failure(
            "invalid_resource",
            "Output resource is foreign, expired or already transferred",
        )
    })
}
fn spend(state: &mut RuntimeState, bytes: usize) -> Result<(), abi::ToolFailure> {
    crate::budget::check(
        crate::LimitKind::HostOperations,
        1,
        usize::from(state.operation_calls_remaining),
    )
    .and_then(|()| {
        crate::budget::check(
            crate::LimitKind::ToolArguments,
            bytes,
            state.input_bytes_remaining,
        )
    })
    .map_err(|_| failure("limit", "Runtime output invocation budget exceeded"))?;
    state.operation_calls_remaining -= 1;
    state.input_bytes_remaining -= bytes;
    Ok(())
}
pub(super) fn insert(
    state: &mut RuntimeState,
    entry: Entry,
) -> Result<Resource<BufferResource>, Error> {
    let id = state.next_output;
    state.next_output = state.next_output.checked_add(1).ok_or_else(|| {
        crate::budget::reject(
            crate::LimitKind::Arithmetic,
            "WASM output resource identities exhausted",
        )
    })?;
    state.outputs.insert(id, entry);
    Ok(Resource::new_own(id))
}
pub(super) fn add_to_linker(linker: &mut Linker<RuntimeState>) -> wasmtime::Result<()> {
    let mut interface = linker.instance("microcrab:agent/execution@0.7.0")?;
    interface.resource(
        "buffer",
        ResourceType::host::<BufferResource>(),
        |mut caller, id| {
            // Dropping a prepared untransferred output abandons its Runtime token.
            caller.data_mut().outputs.remove(&id);
            Ok(())
        },
    )?;
    interface.func_wrap("create", create)?;
    interface.func_wrap("append", append)?;
    interface.func_wrap("mark-truncated", mark_truncated)?;
    interface.func_wrap("finish", finish)?;
    interface.func_wrap("attach-process", attach_process)?;
    interface.func_wrap("retain-process", retain_process)?;
    interface.func_wrap("attach-guest", attach_guest)?;
    Ok(())
}
fn create(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (channels,): (Vec<String>,),
) -> wasmtime::Result<(Result<Resource<BufferResource>, abi::ToolFailure>,)> {
    let state = caller.data_mut();
    let result = (|| {
        let context = context(state)?;
        spend(state, channels.iter().map(String::len).sum())?;
        let controller = Arc::new(Controller::new());
        let (writer, output) = context
            .open_output(
                OutputOptions {
                    channels: channels.clone(),
                },
                controller.clone(),
            )
            .map_err(|e| failure("output", e.to_string()))?;
        let source = Entry {
            writer,
            output: Some(output),
            controller,
            channels,
            driver: None,
            cleanup: state
                .runtime
                .clone()
                .map(|runtime| (runtime, context.processes.clone())),
            retained: Default::default(),
            attached: false,
        };
        insert(state, source).map_err(|e| failure("limit", e.to_string()))
    })();
    Ok((result,))
}
fn append(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (resource, channel, content): (Resource<BufferResource>, Option<String>, String),
) -> wasmtime::Result<(Result<(), abi::ToolFailure>,)> {
    let state = caller.data_mut();
    let result = (|| {
        spend(
            state,
            content.len() + channel.as_ref().map_or(0, String::len),
        )?;
        entry(state, &resource)?
            .writer
            .append(channel.as_deref(), &content)
            .map_err(|e| failure("output", e.to_string()))
    })();
    Ok((result,))
}
fn mark_truncated(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (resource,): (Resource<BufferResource>,),
) -> wasmtime::Result<(Result<(), abi::ToolFailure>,)> {
    let state = caller.data_mut();
    let result = (|| {
        spend(state, 0)?;
        entry(state, &resource)?
            .writer
            .mark_truncated()
            .map_err(|e| failure("output", e.to_string()))
    })();
    Ok((result,))
}
fn finish(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (resource, reported): (Resource<BufferResource>, Option<abi::ToolFailure>),
) -> wasmtime::Result<(Result<(), abi::ToolFailure>,)> {
    let state = caller.data_mut();
    let result = (|| {
        spend(
            state,
            reported
                .as_ref()
                .map_or(0, |f| f.message.len() + f.code.len()),
        )?;
        let outcome = reported
            .map(native_failure)
            .transpose()
            .map_err(|e| failure("invalid_failure", e.to_string()))?;
        entry(state, &resource)?
            .writer
            .finish(outcome.map_or(Ok(()), Err))
            .map_err(|e| failure("output", e.to_string()))
    })();
    Ok((result,))
}
fn attach_process(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (resource, process, captured): (
        Resource<BufferResource>,
        super::microcrab::host::process::ProcessHandle,
        bool,
    ),
) -> wasmtime::Result<(Result<(), abi::ToolFailure>,)> {
    let state = caller.data_mut();
    let result = (|| {
        let context = context(state)?;
        spend(state, 0)?;
        if state.borrowed_processes.contains(&process.id)
            || !state.process_grants.contains_key(&process.id)
        {
            return Err(failure(
                "denied",
                "Host source was not created by this invocation",
            ));
        }
        let source = entry(state, &resource)?;
        if source.attached || !source.retained.is_empty() {
            return Err(failure("conflict", "Output already has a producer"));
        }
        let handle =
            microcrab_host_api::process::ProcessHandle(microcrab_host_api::HandleId(process.id));
        if context.processes.process_pid(handle) != Some(process.pid) {
            return Err(failure("denied", "Host source identity differs"));
        }
        let runtime = state
            .runtime
            .clone()
            .ok_or_else(|| failure("unsupported", "Runtime executor absent"))?;
        let controller = runtime
            .block_on(context.attach_process_output(
                source.writer.clone(),
                handle,
                source.channels.clone(),
                captured,
            ))
            .map_err(|e| failure("host_source", e.to_string()))?;
        *source
            .controller
            .delegate
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(controller);
        state
            .outputs
            .get_mut(&resource.rep())
            .ok_or_else(|| failure("invalid_resource", "Output removed"))?
            .attached = true;
        state.process_grants.remove(&process.id);
        Ok(())
    })();
    Ok((result,))
}
fn retain_process(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (resource, process, captured): (
        Resource<BufferResource>,
        super::microcrab::host::process::ProcessHandle,
        bool,
    ),
) -> wasmtime::Result<(Result<(), abi::ToolFailure>,)> {
    let state = caller.data_mut();
    let result = (|| {
        let context = context(state)?;
        spend(state, 0)?;
        let source = entry(state, &resource)?;
        if state.borrowed_processes.contains(&process.id)
            || !state.process_grants.contains_key(&process.id)
            || !source.retained.is_empty()
            || (source.attached && source.driver.is_none())
        {
            return Err(failure(
                "denied",
                "Host source ownership cannot be transferred",
            ));
        }
        let handle =
            microcrab_host_api::process::ProcessHandle(microcrab_host_api::HandleId(process.id));
        if context.processes.process_pid(handle) != Some(process.pid) {
            return Err(failure("denied", "Host source identity differs"));
        }
        let controller = context
            .processes
            .registry()
            .output_controller(&context.session_id, handle, captured)
            .map_err(|e| failure("host_source", e.to_string()))?;
        *source
            .controller
            .delegate
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(controller);
        let grant = state
            .process_grants
            .remove(&process.id)
            .ok_or_else(|| failure("denied", "Host source was not created by this invocation"))?;
        state
            .outputs
            .get_mut(&resource.rep())
            .ok_or_else(|| failure("invalid_resource", "Output removed"))?
            .retained
            .insert(process.id, grant);
        Ok(())
    })();
    Ok((result,))
}
fn attach_guest(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (resource, bytes): (Resource<BufferResource>, Vec<u8>),
) -> wasmtime::Result<(Result<(), abi::ToolFailure>,)> {
    let state = caller.data_mut();
    let result = (|| {
        spend(state, bytes.len())?;
        entry(state, &resource)?;
        let source = state
            .outputs
            .get_mut(&resource.rep())
            .ok_or_else(|| failure("invalid_resource", "Output removed"))?;
        if source.attached {
            return Err(failure("conflict", "Output already has a producer"));
        }
        source.driver = Some(bytes);
        source.attached = true;
        Ok(())
    })();
    Ok((result,))
}
pub(super) struct Prepared {
    pub outcome: ToolOutput,
    pub driver: Option<Entry>,
}
pub(super) fn lift(
    state: &mut RuntimeState,
    value: Result<abi::ToolOutput, abi::ToolFailure>,
) -> Result<Prepared, Error> {
    match value {
        Err(failure) => Ok(Prepared {
            outcome: ToolOutput::failed(String::new(), native_failure(failure)?),
            driver: None,
        }),
        Ok(abi::ToolOutput::Final(value)) => {
            crate::budget::check(
                crate::LimitKind::ToolOutput,
                value.content.len(),
                state.max_output_bytes,
            )
            .map_err(|error| match error {
                Error::Limit(limit) => Error::Limit(limit.with_label("WASM result bytes")),
                error => error,
            })?;
            Ok(Prepared {
                outcome: ToolOutput::Final(FinalOutput {
                    content: value.content,
                    failure: value.failure.map(native_failure).transpose()?,
                }),
                driver: None,
            })
        }
        Ok(abi::ToolOutput::Buffered(value)) => {
            if !value.output.owned() {
                return Err(Error::denied(
                    "Buffered result must transfer an owned output resource",
                ));
            }
            let mut source = state.outputs.remove(&value.output.rep()).ok_or_else(|| {
                Error::denied("Buffered resource is foreign, expired or transferred")
            })?;
            let output = source
                .output
                .take()
                .ok_or_else(|| Error::denied("Output already transferred"))?
                .with_initial_output(value.include_initial);
            let driver = if source.driver.is_some() {
                Some(source)
            } else {
                None
            };
            Ok(Prepared {
                outcome: ToolOutput::Buffered(output),
                driver,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn typed_tool_failure_preserves_startup_timeout_and_retryability() {
        let reported = abi::ToolFailure {
            kind: abi::FailureKind::Timeout,
            code: "startup_timeout".into(),
            message: "Tool initialization exceeded its own deadline".into(),
            retryable: true,
        };
        let native = native_failure(reported).unwrap();
        assert_eq!(native.kind, ToolFailureKind::Timeout);
        assert_eq!(native.code, "startup_timeout");
        assert!(native.retryable);
    }
    #[test]
    fn guest_failure_bounds_are_validated_before_native_adoption() {
        let mut reported = failure("custom_code", "retained output is still meaningful");
        reported.code = "x".repeat(129);
        assert!(native_failure(reported).is_err());
        let reported = failure("custom_code", "x".repeat(4097));
        assert!(native_failure(reported).is_err());
        let reported = failure("custom_code", "x".repeat(4096));
        assert!(native_failure(reported).is_ok());
    }
}
