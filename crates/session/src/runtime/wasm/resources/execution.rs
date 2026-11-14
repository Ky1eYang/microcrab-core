//! General process/shell imports. Runtime supplies policy and Session ownership;
//! Host interfaces contain only process resources, pipe bytes and capture facts.
use super::super::microcrab::host::shell;
#[cfg(test)]
mod tests;

use super::*;
use host::process::{ProcessHandle, ProcessState, ProcessStatus};
use host::shell::ShellApi;
use std::{future::Future, pin::Pin, sync::Arc};

type Work<T> = Pin<Box<dyn Future<Output = Result<T, ImportFailure<host::HostError>>> + Send>>;
pub(super) const SESSION_DIAGNOSTIC: &str = "Runtime process diagnostic";
fn session_error(source: crate::Error) -> ImportFailure<host::HostError> {
    // Process-operation Diagnostic receipts remain guest-owned; admission failures
    // retain their actual Tool/Closed/Limit variant until this adapter.
    let authoritative = !matches!(&source, crate::Error::Diagnostic(_));
    use crate::observation::diagnostics::DiagnosticCode as C;
    let diagnostic = source.tool_diagnostic();
    let code = match diagnostic.code {
        C::Cancelled => host::ErrorCategory::Cancelled,
        C::Timeout => host::ErrorCategory::Timeout,
        C::PermissionDenied => host::ErrorCategory::Permission,
        C::NotFound => host::ErrorCategory::NotFound,
        C::Unsupported => host::ErrorCategory::Unsupported,
        C::Conflict | C::InvalidArgument | C::PathOutsideRoot => host::ErrorCategory::Conflict,
        _ => host::ErrorCategory::Io,
    };
    let mut error = host::HostError::new(code, super::super::encode_diagnostic(&diagnostic));
    error.public_reason = Some(SESSION_DIAGNOSTIC.into());
    error.retryable = diagnostic.retryable.unwrap_or(false);
    ImportFailure {
        wire: error,
        authoritative,
    }
}
fn operation<T>(
    state: &mut RuntimeState,
    bytes: usize,
    creating: bool,
    resources: Vec<String>,
    arguments: Value,
    work: impl FnOnce(Arc<dyn host::Host>, ToolContext, host::OpContext) -> Work<T>,
) -> Result<T, ImportFailure<types::HostError>> {
    let mut context = begin(state, bytes)?;
    // The native operation cancellation bounds admission/observation. Process
    // lifetime is owned independently after admission, never by this deadline.
    context.deadline = None;
    let backend = state
        .host
        .clone()
        .ok_or_else(|| error(types::ErrorCategory::Unsupported, "Host unavailable"))?;
    let invocation = state
        .tool_context
        .clone()
        .ok_or_else(|| error(types::ErrorCategory::Permission, "Invocation absent"))?;
    let runtime = state
        .runtime
        .clone()
        .ok_or_else(|| error(types::ErrorCategory::Unsupported, "Host runtime absent"))?;
    let epoch = state.epoch_cancellation.clone();
    let started = Instant::now();
    let mut authoritative = false;
    let mut normalization_diagnostic = None;
    let mut admitted_scope = None;
    let result = runtime.block_on(async {
        let _cancel_on_drop = context.cancel.clone().drop_guard();
        tokio::select! { biased;
            _ = epoch.cancelled() => Err(host::HostError::new(host::ErrorCategory::Cancelled, "Host operation cancelled")),
            result = context.run(async {
                let resources = if creating {
                    let mut normalized = Vec::with_capacity(resources.len());
                    for resource in resources {
                        normalized.push(backend.normalize_resource(resource.into(), context.cancel.clone()).await
                            .map_err(|source| normalization_error(source, &mut normalization_diagnostic))?);
                    }
                    normalized
                } else { resources };
                if creating { admitted_scope=Some((resources,arguments)); }
                work(backend.clone(), invocation, context.clone()).await.map_err(|failure| { authoritative = failure.authoritative; failure.wire })
            }) => result,
        }
    }).map_err(|source| { let mut failure = host_error(source); failure.authoritative = authoritative; failure });
    if creating {
        state.admitted_process_scope = if result.is_ok() { admitted_scope } else { None };
    }
    state.operation_diagnostic = normalization_diagnostic;
    exclude_host_wait(&mut state.deadline, started.elapsed(), result)
}
fn checked(
    state: &RuntimeState,
    handle: &process::ProcessHandle,
) -> Result<ProcessHandle, ImportFailure<types::HostError>> {
    let handle_id = ProcessHandle(host::HandleId(handle.id));
    let invocation = state
        .tool_context
        .as_ref()
        .ok_or_else(|| error(types::ErrorCategory::Permission, "Invocation absent"))?;
    if !invocation.processes.owns_process(handle_id)
        || invocation.processes.process_pid(handle_id) != Some(handle.pid)
    {
        return Err(error(
            types::ErrorCategory::Permission,
            "Process handle does not belong to this Session",
        ));
    }
    Ok(handle_id)
}
fn address(state: &RuntimeState, id: u64) -> String {
    format!(
        "{}/{}",
        state
            .tool_context
            .as_ref()
            .map_or("", |c| c.session_id.as_str()),
        id
    )
}
fn signal_from(value: process::ProcessSignal) -> host::process::Signal {
    use host::process::Signal as N;
    match value {
        process::ProcessSignal::Term => N::Term,
        process::ProcessSignal::Kill => N::Kill,
        process::ProcessSignal::Int => N::Int,
        process::ProcessSignal::Hup => N::Hup,
        process::ProcessSignal::Usr1 => N::Usr1,
        process::ProcessSignal::Usr2 => N::Usr2,
    }
}
fn exit_status(value: host::process::ExitStatus) -> process::ExitStatus {
    use host::process::Signal as N;
    process::ExitStatus {
        code: value.code,
        signal: value.signal.map(|s| match s {
            N::Term => process::ProcessSignal::Term,
            N::Kill => process::ProcessSignal::Kill,
            N::Int => process::ProcessSignal::Int,
            N::Hup => process::ProcessSignal::Hup,
            N::Usr1 => process::ProcessSignal::Usr1,
            N::Usr2 => process::ProcessSignal::Usr2,
        }),
    }
}
fn status_value(value: ProcessStatus) -> process::ProcessStatus {
    process::ProcessStatus {
        pid: value.pid,
        elapsed_s: value.elapsed_s as f64,
        state: match value.state {
            ProcessState::Running => process::ProcessState::Running,
            ProcessState::Exited { status } => process::ProcessState::Exited(exit_status(status)),
            ProcessState::Failed { reason } => process::ProcessState::Failed(
                microcrab_protocol::diagnostic::sanitize(&reason, 2048),
            ),
        },
    }
}
fn launch_scope(
    _state: &RuntimeState,
    command: &str,
    cwd: &str,
) -> Result<(), ImportFailure<types::HostError>> {
    let reason = if command.is_empty() {
        Some("Shell command must not be empty")
    } else if command.contains('\0') {
        Some("Shell command must not contain NUL")
    } else if cwd.is_empty() {
        Some("Shell cwd must not be empty")
    } else if cwd.contains('\0') {
        Some("Shell cwd must not contain NUL")
    } else {
        None
    };
    if let Some(reason) = reason {
        return Err(error(types::ErrorCategory::Conflict, reason));
    }
    Ok(())
}
pub(super) fn launch(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (request,): (shell::ShellRequest,),
) -> wasmtime::Result<(Result<process::ProcessHandle, types::HostError>,)> {
    let state = caller.data_mut();
    let result = (|| {
        let cwd = request.cwd.as_deref().unwrap_or(".").to_owned();
        launch_scope(state, &request.command, &cwd)?;
        let output_bytes = state
            .tool_context
            .as_ref()
            .ok_or_else(|| error(types::ErrorCategory::Permission, "Invocation absent"))?
            .state
            .snapshot()
            .limits
            .output_buffer_bytes
            .min(state.max_output_bytes.saturating_sub(256) / 2);
        let bytes = request
            .command
            .len()
            .saturating_add(cwd.len())
            .saturating_add(32);
        let args = json!({"operation":"shell.launch","command":request.command,"cwd":cwd});
        let handle = operation(
            state,
            bytes,
            true,
            vec![cwd.clone()],
            args.clone(),
            move |_backend, invocation, context| {
                Box::pin(async move {
                    invocation
                        .processes
                        .launch_shell(
                            &invocation.tool,
                            &invocation.call_id,
                            host::shell::ShellRequest {
                                command: request.command,
                                cwd: request.cwd.map(Into::into),
                                output_bytes,
                            },
                            context.cancel,
                        )
                        .await
                        .map_err(session_error)
                })
            },
        )?;
        let pid = state
            .tool_context
            .as_ref()
            .and_then(|c| c.processes.process_pid(handle))
            .ok_or_else(|| error(types::ErrorCategory::Io, "Process receipt absent"))?;
        let grant = state.admitted_process_scope.take().ok_or_else(|| {
            error(
                types::ErrorCategory::Permission,
                "Canonical process creation authority absent",
            )
        })?;
        state.process_grants.insert(handle.0.0, grant);
        Ok(process::ProcessHandle {
            id: handle.0.0,
            pid,
        })
    })();
    finish(state, result)
}
pub(super) fn spawn(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (spec,): (process::ProcessSpec,),
) -> wasmtime::Result<(Result<process::ProcessHandle, types::HostError>,)> {
    let state = caller.data_mut();
    let result = (|| {
        let bytes = process_spec_bytes(&spec);
        let cwd = spec.cwd.as_deref().unwrap_or(".").to_owned();
        let args = json!({"operation":"process.spawn","program":spec.program,"args":spec.args,"cwd":cwd,"env":spec.env,"pty":spec.pty.as_ref().map(|p|json!({"rows":p.rows,"cols":p.cols}))});
        let native = host::process::ProcessSpec {
            program: spec.program,
            args: spec.args,
            cwd: spec.cwd.map(Into::into),
            env: spec.env.into_iter().collect(),
            pty: spec.pty.map(|s| host::process::PtySize {
                rows: s.rows,
                cols: s.cols,
            }),
        };
        let handle = operation(
            state,
            bytes,
            true,
            vec![cwd.clone()],
            args.clone(),
            move |_backend, invocation, context| {
                Box::pin(async move {
                    invocation
                        .processes
                        .launch_process(
                            &invocation.tool,
                            &invocation.call_id,
                            native,
                            context.cancel,
                        )
                        .await
                        .map_err(session_error)
                })
            },
        )?;
        let pid = state
            .tool_context
            .as_ref()
            .and_then(|c| c.processes.process_pid(handle))
            .ok_or_else(|| error(types::ErrorCategory::Io, "Process receipt absent"))?;
        let grant = state.admitted_process_scope.take().ok_or_else(|| {
            error(
                types::ErrorCategory::Permission,
                "Canonical process creation authority absent",
            )
        })?;
        state.process_grants.insert(handle.0.0, grant);
        Ok(process::ProcessHandle {
            id: handle.0.0,
            pid,
        })
    })();
    finish(state, result)
}
fn observe(
    state: &mut RuntimeState,
    handle: process::ProcessHandle,
    duration: Option<u64>,
) -> Result<process::ProcessStatus, ImportFailure<types::HostError>> {
    let native = checked(state, &handle)?;
    if duration.is_some_and(|ms| ms > 60_000) {
        return Err(error(
            types::ErrorCategory::Conflict,
            "yield_ms must be 0..=60000",
        ));
    }
    let resource = address(state, handle.id);
    let status = operation(
        state,
        24,
        false,
        vec![resource],
        json!({"operation":"process.status","id":handle.id}),
        move |backend, _invocation, context| {
            Box::pin(async move {
                let mut changes = if duration.is_some() {
                    Some(backend.watch(native)?)
                } else {
                    None
                };
                let mut value = backend.status(&context, native).await?;
                if let Some(duration) = duration {
                    let end = tokio::time::Instant::now() + Duration::from_millis(duration);
                    while matches!(value.state, ProcessState::Running) {
                        tokio::select! { biased;
                            _ = context.cancel.cancelled() => return Err(ImportFailure::local(host::HostError::new(host::ErrorCategory::Cancelled,"Process observation cancelled"))),
                            _ = tokio::time::sleep_until(end) => break,
                            changed = changes.as_mut().expect("bounded observation subscribed before status").changed() => { changed.map_err(|_| host::HostError::new(host::ErrorCategory::Io,"Process observation closed"))?; },
                        }
                        value = backend.status(&context, native).await?;
                    }
                }
                Ok(value)
            })
        },
    )?;
    Ok(status_value(status))
}
pub(super) fn status(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (handle,): (process::ProcessHandle,),
) -> wasmtime::Result<(Result<process::ProcessStatus, types::HostError>,)> {
    let state = caller.data_mut();
    let result = observe(state, handle, None);
    finish(state, result)
}
pub(super) fn wait_for_state(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (handle, yield_ms): (process::ProcessHandle, u64),
) -> wasmtime::Result<(Result<process::ProcessStatus, types::HostError>,)> {
    let state = caller.data_mut();
    let result = observe(state, handle, Some(yield_ms));
    finish(state, result)
}
pub(super) fn wait(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (handle,): (process::ProcessHandle,),
) -> wasmtime::Result<(Result<process::ExitStatus, types::HostError>,)> {
    let state = caller.data_mut();
    let result = (|| {
        let native = checked(state, &handle)?;
        let resource = address(state, handle.id);
        operation(
            state,
            16,
            false,
            vec![resource],
            json!({"operation":"process.wait","id":handle.id}),
            move |backend, _, context| {
                Box::pin(async move {
                    backend
                        .wait(&context, native)
                        .await
                        .map_err(ImportFailure::backend)
                })
            },
        )
        .map(exit_status)
    })();
    finish(state, result)
}
fn control(
    state: &mut RuntimeState,
    handle: process::ProcessHandle,
    signal: Option<process::ProcessSignal>,
    force: bool,
    release: bool,
) -> Result<(), ImportFailure<types::HostError>> {
    let native = checked(state, &handle)?;
    let resource = address(state, handle.id);
    operation(
        state,
        24,
        false,
        vec![resource],
        json!({"operation":if release {"process.release"} else {"process.kill"},"id":handle.id}),
        move |backend, invocation, context| {
            Box::pin(async move {
                if release {
                    invocation
                        .processes
                        .release_process(native, context.cancel)
                        .await
                        .map_err(session_error)
                } else if let Some(signal) = signal {
                    backend
                        .signal(&context, native, signal_from(signal))
                        .await
                        .map_err(ImportFailure::backend)
                } else {
                    backend
                        .kill_tree(&context, native, force)
                        .await
                        .map_err(ImportFailure::backend)
                }
            })
        },
    )
}
pub(super) fn signal(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (handle, sig): (process::ProcessHandle, process::ProcessSignal),
) -> wasmtime::Result<(Result<(), types::HostError>,)> {
    let state = caller.data_mut();
    let result = control(state, handle, Some(sig), false, false);
    finish(state, result)
}
pub(super) fn kill_tree(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (handle, force): (process::ProcessHandle, bool),
) -> wasmtime::Result<(Result<(), types::HostError>,)> {
    let state = caller.data_mut();
    let result = control(state, handle, None, force, false);
    finish(state, result)
}
pub(super) fn release(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (handle,): (process::ProcessHandle,),
) -> wasmtime::Result<(Result<(), types::HostError>,)> {
    let state = caller.data_mut();
    let result = control(state, handle, None, true, true);
    finish(state, result)
}
fn output_limit(state: &RuntimeState, requested: u64) -> usize {
    let limit = state
        .tool_context
        .as_ref()
        .map_or(0, |c| c.state.snapshot().limits.output_buffer_bytes);
    usize::try_from(requested)
        .unwrap_or(usize::MAX)
        // Typed Host resource bytes are intermediate data, not an LLM JSON receipt.
        .min(limit)
        .min(state.output_bytes_remaining.saturating_sub(128))
}
fn capture(
    state: &mut RuntimeState,
    handle: process::ProcessHandle,
    max: u64,
    drain: bool,
) -> Result<shell::ShellOutput, ImportFailure<types::HostError>> {
    let native = checked(state, &handle)?;
    let limit = output_limit(state, max);
    if limit == 0 {
        return Err(error(
            types::ErrorCategory::Io,
            "Capture response budget exhausted",
        ));
    }
    let resource = address(state, handle.id);
    let mut output = operation(
        state,
        24,
        false,
        vec![resource],
        json!({"operation":if drain {"shell.drain"} else {"shell.read"},"id":handle.id}),
        move |backend, _, context| {
            Box::pin(async move {
                if drain {
                    backend
                        .drain(&context, native, limit)
                        .await
                        .map_err(ImportFailure::backend)
                } else {
                    ShellApi::read(backend.as_ref(), &context, native, None)
                        .await
                        .map_err(ImportFailure::backend)
                }
            })
        },
    )?;
    let mut remaining = limit;
    for text in [&mut output.stdout, &mut output.stderr] {
        let mut end = remaining.min(text.len());
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        output.truncated |= end < text.len();
        text.truncate(end);
        remaining -= end;
    }
    state.output_bytes_remaining = state
        .output_bytes_remaining
        .saturating_sub(output.stdout.len() + output.stderr.len() + 64);
    Ok(shell::ShellOutput {
        stdout: output.stdout,
        stderr: output.stderr,
        truncated: output.truncated,
    })
}
pub(super) fn read(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (handle, max): (process::ProcessHandle, u64),
) -> wasmtime::Result<(Result<shell::ShellOutput, types::HostError>,)> {
    let state = caller.data_mut();
    let result = capture(state, handle, max, false);
    finish(state, result)
}
pub(super) fn drain(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (handle, max): (process::ProcessHandle, u64),
) -> wasmtime::Result<(Result<shell::ShellOutput, types::HostError>,)> {
    let state = caller.data_mut();
    let result = capture(state, handle, max, true);
    finish(state, result)
}
fn input(
    state: &mut RuntimeState,
    handle: process::ProcessHandle,
    data: Vec<u8>,
    eof: bool,
    captured: bool,
) -> Result<u32, ImportFailure<types::HostError>> {
    let native = checked(state, &handle)?;
    let resource = address(state, handle.id);
    let size = data.len();
    operation(
        state,
        size.saturating_add(24),
        false,
        vec![resource],
        json!({"operation":"process.stdin","id":handle.id,"bytes":size,"data":data,"eof":eof}),
        move |backend, _, context| {
            Box::pin(async move {
                if captured {
                    ShellApi::write(backend.as_ref(), &context, native, data, eof).await?;
                } else {
                    backend
                        .write_stdin(&context, native, host::Bytes::from(data))
                        .await?;
                    if eof {
                        backend.close_stdin(&context, native).await?;
                    }
                }
                u32::try_from(size).map_err(|_| {
                    ImportFailure::local(host::HostError::new(
                        host::ErrorCategory::Conflict,
                        "stdin size exceeds wire range",
                    ))
                })
            })
        },
    )
}
pub(super) fn write(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (handle, data, eof): (process::ProcessHandle, Vec<u8>, bool),
) -> wasmtime::Result<(Result<(), types::HostError>,)> {
    let state = caller.data_mut();
    let result = input(state, handle, data, eof, true).map(|_| ());
    finish(state, result)
}
pub(super) fn write_stdin(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (handle, data): (process::ProcessHandle, Vec<u8>),
) -> wasmtime::Result<(Result<u32, types::HostError>,)> {
    let state = caller.data_mut();
    let result = input(state, handle, data, false, false);
    finish(state, result)
}
pub(super) fn close_stdin(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (handle,): (process::ProcessHandle,),
) -> wasmtime::Result<(Result<(), types::HostError>,)> {
    let state = caller.data_mut();
    let result = input(state, handle, Vec::new(), true, false).map(|_| ());
    finish(state, result)
}
fn pipe(
    state: &mut RuntimeState,
    handle: process::ProcessHandle,
    max: u32,
    stderr: bool,
) -> Result<Option<Vec<u8>>, ImportFailure<types::HostError>> {
    let native = checked(state, &handle)?;
    let resource = address(state, handle.id);
    let limit = output_limit(state, u64::from(max)).min(65536);
    if limit == 0 {
        return Err(error(
            types::ErrorCategory::Io,
            "Pipe response budget exhausted",
        ));
    }
    let output = operation(
        state,
        24,
        false,
        vec![resource],
        json!({"operation":"process.pipe","id":handle.id,"stderr":stderr}),
        move |_, invocation, context| {
            Box::pin(async move {
                invocation
                    .processes
                    .read_process_pipe(native, stderr, limit, context.cancel)
                    .await
                    .map_err(session_error)
            })
        },
    )?;
    state.output_bytes_remaining = state
        .output_bytes_remaining
        .saturating_sub(output.as_ref().map_or(0, Vec::len));
    Ok(output)
}
pub(super) fn read_stdout(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (handle, max): (process::ProcessHandle, u32),
) -> wasmtime::Result<(Result<Option<Vec<u8>>, types::HostError>,)> {
    let state = caller.data_mut();
    let result = pipe(state, handle, max, false);
    finish(state, result)
}
pub(super) fn read_stderr(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (handle, max): (process::ProcessHandle, u32),
) -> wasmtime::Result<(Result<Option<Vec<u8>>, types::HostError>,)> {
    let state = caller.data_mut();
    let result = pipe(state, handle, max, true);
    finish(state, result)
}
pub(super) fn resize_pty(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (handle, rows, cols): (process::ProcessHandle, u16, u16),
) -> wasmtime::Result<(Result<(), types::HostError>,)> {
    let state = caller.data_mut();
    let result = (|| {
        let native = checked(state, &handle)?;
        let resource = address(state, handle.id);
        operation(
            state,
            24,
            false,
            vec![resource],
            json!({"operation":"process.resize-pty","id":handle.id,"rows":rows,"cols":cols}),
            move |backend, _, context| {
                Box::pin(async move {
                    backend
                        .resize_pty(&context, native, rows, cols)
                        .await
                        .map_err(ImportFailure::backend)
                })
            },
        )
    })();
    finish(state, result)
}
