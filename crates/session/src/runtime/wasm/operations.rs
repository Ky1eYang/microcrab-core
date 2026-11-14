//! Typed imports run on the WASM worker. Runtime validates admitted invocation proof;
//! the Host owns path confinement, atomic publication and cancellation cleanup.
#[cfg(test)]
mod review_tests;
#[cfg(test)]
mod tests;

pub(super) mod mutations;
mod abi {
    pub(super) use super::super::microcrab::host::glob::{
        GlobEntry, GlobEntryKind, GlobOutput, GlobRequest,
    };
    pub(super) use super::super::microcrab::host::grep::{GrepMatch, GrepOutput, GrepRequest};
    pub(super) use super::super::microcrab::host::text_read::{ReadOutput, ReadRequest};
    pub(super) use super::super::microcrab::host::text_write::{
        WriteOutput, WritePrecondition, WriteRequest,
    };
    pub(super) use super::super::microcrab::host::tool_types::{
        OperationError, OperationErrorCode,
    };
}
use super::{CancelOnDrop, ImportFailure, RuntimeState};
use microcrab_host_api as host;
use serde_json::{Value, json};
use std::{future::Future, path::PathBuf, time::Instant};
use wasmtime::StoreContextMut;

fn private_failure(
    code: abi::OperationErrorCode,
    message: &str,
) -> ImportFailure<abi::OperationError> {
    ImportFailure::local(abi::OperationError {
        code,
        message: message.into(),
    })
}
fn failure(
    code: abi::OperationErrorCode,
    reason: &'static str,
) -> ImportFailure<abi::OperationError> {
    let mut error = private_failure(code, reason);
    let mut safe = diagnostic(&error);
    safe.reason = Some(crate::observation::diagnostics::SafeDiagnosticReason::new(
        reason,
    ));
    error.message = super::encode_diagnostic(&safe);
    error
}
fn finish<T>(
    state: &mut RuntimeState,
    result: Result<T, ImportFailure<abi::OperationError>>,
) -> wasmtime::Result<(Result<T, abi::OperationError>,)> {
    if let Err(error) = &result {
        state.operation_diagnostic_authoritative = error.authoritative;
        let mut safe = diagnostic(error);
        if state.known_write_committed {
            safe.effects = crate::observation::diagnostics::EffectKnowledge::Committed;
        }

        if safe.code == crate::observation::diagnostics::DiagnosticCode::PathOutsideRoot {
            state
                .confinement_diagnostic
                .get_or_insert_with(|| safe.clone());
        }
        state.operation_diagnostic = Some((safe, error.message.clone()));
        match error.code {
            abi::OperationErrorCode::Cancelled => state.operation_cancelled = true,
            _ => {}
        }
    } else {
        state.operation_diagnostic = None;
        state.operation_diagnostic_authoritative = false;
    }
    Ok((result.map_err(|failure| failure.wire),))
}

pub(super) fn diagnostic(
    error: &abi::OperationError,
) -> crate::observation::diagnostics::ToolDiagnostic {
    if let Some(safe) = super::decode_diagnostic(&error.message) {
        return safe;
    }
    use crate::observation::diagnostics::{
        DiagnosticCode as C, DiagnosticLayer as L, DiagnosticPhase as P, ToolDiagnostic,
    };
    use abi::OperationErrorCode as A;
    let code = match error.code {
        A::InvalidRequest => C::InvalidArgument,
        A::Timeout => C::Timeout,
        A::Cancelled => C::Cancelled,
        A::Denied => C::PermissionDenied,
        A::Unsupported => C::Unsupported,
        A::Conflict => C::Conflict,
        A::Limit => C::OutputLimit,
        _ => C::Io,
    };
    ToolDiagnostic::new(code, L::Host, P::HostOperation)
}

fn invalid(reason: &'static str) -> ImportFailure<abi::OperationError> {
    failure(abi::OperationErrorCode::InvalidRequest, reason)
}
fn host_error(error: host::ToolError) -> ImportFailure<abi::OperationError> {
    use abi::OperationErrorCode as C;
    let detail = match &error {
        host::ToolError::Host(source) => source.message.clone(),
        _ => error.to_string(),
    };
    let code = match &error {
        host::ToolError::Host(source) => match source.category {
            host::ErrorCategory::Permission => C::Denied,
            host::ErrorCategory::Timeout => C::Timeout,
            host::ErrorCategory::Cancelled => C::Cancelled,
            host::ErrorCategory::Conflict => C::Conflict,
            host::ErrorCategory::Unsupported => C::Unsupported,
            _ => C::Io,
        },
        host::ToolError::Io(source) => match source.kind() {
            std::io::ErrorKind::PermissionDenied => C::Denied,
            std::io::ErrorKind::TimedOut => C::Timeout,
            _ => C::Io,
        },
        host::ToolError::Unsupported(_) => C::Unsupported,
        host::ToolError::Cancelled => C::Cancelled,
        host::ToolError::OperationDeadline | host::ToolError::Timeout => C::Timeout,
        host::ToolError::FileConflict | host::ToolError::WorkspaceConflict => C::Conflict,
        host::ToolError::OperationLimit(_)
        | host::ToolError::FileLimit
        | host::ToolError::OutputLimit => C::Limit,
        host::ToolError::PathOutsideRoot
        | host::ToolError::CwdOutsideRoot
        | host::ToolError::InvalidRequest(_) => C::InvalidRequest,
        _ => C::Io,
    };
    // Worker text belongs to the authorized guest result, while its Session
    // fallback is deliberately body-free. Do not replace that concrete result
    // with the generic fallback merely because projection supplies a safe reason.
    let preserve_worker_detail =
        matches!(&error, host::ToolError::Worker(_)) && error.public_reason().is_none();
    let safe = crate::Error::from_host_error(error).tool_diagnostic();
    if (!preserve_worker_detail && safe.reason.is_some())
        || safe.code == crate::observation::diagnostics::DiagnosticCode::PathOutsideRoot
    {
        return ImportFailure::backend(
            private_failure(code, &super::encode_diagnostic(&safe)).wire,
        );
    }
    // Native bodies are guest-visible only. They cannot impersonate Runtime metadata.
    let detail = if detail.starts_with("tool_diagnostic:") {
        format!("Host: {detail}")
    } else {
        detail
    };
    ImportFailure::backend(private_failure(code, &detail).wire)
}
/// Keep the wire path bounded; only the executing Host can normalize aliases,
/// absolute paths, platform separators, symlinks and device names correctly.
pub(super) fn resource(path: &str) -> Result<String, ImportFailure<abi::OperationError>> {
    if path.is_empty() {
        return Err(invalid("Host resource path must not be empty"));
    }
    if path.len() > 4096 {
        return Err(invalid("Host resource path exceeds 4096 bytes"));
    }
    if path.contains('\0') {
        return Err(invalid("Host resource path contains a NUL byte"));
    }
    Ok(path.to_owned())
}
fn number(
    value: u64,
    maximum: usize,
    reason: &'static str,
) -> Result<usize, ImportFailure<abi::OperationError>> {
    let value = usize::try_from(value).map_err(|_| invalid(reason))?;
    if value == 0 || value > maximum {
        return Err(invalid(reason));
    }
    Ok(value)
}

fn bridge<T>(
    state: &mut RuntimeState,
    resources: Vec<String>,
    arguments: Value,
    execute: impl FnOnce(
        std::sync::Arc<dyn host::Host>,
        host::OpContext,
        Option<super::ToolContext>,
    ) -> std::pin::Pin<
        Box<dyn Future<Output = Result<(T, usize), ImportFailure<abi::OperationError>>> + Send>,
    >,
) -> Result<T, ImportFailure<abi::OperationError>> {
    if state.operation_cancelled {
        return Err(failure(
            abi::OperationErrorCode::Cancelled,
            "Host operation cancelled",
        ));
    }
    let context = state.tool_context.clone().ok_or_else(|| {
        failure(
            abi::OperationErrorCode::Denied,
            "Host operations require an authorized invocation",
        )
    })?;
    context.ensure_admitted(None).map_err(|error| {
        private_failure(
            if matches!(error, crate::policy::PermissionError::Cancelled) {
                abi::OperationErrorCode::Cancelled
            } else {
                abi::OperationErrorCode::Denied
            },
            error.public_reason(),
        )
    })?;
    let input = serde_json::to_vec(&arguments)
        .map_err(|_| invalid("Host request arguments could not be serialized"))?
        .len();
    crate::budget::check(
        crate::LimitKind::HostOperations,
        1,
        usize::from(state.operation_calls_remaining),
    )
    .and_then(|()| {
        crate::budget::check(
            crate::LimitKind::ToolArguments,
            input,
            state.input_bytes_remaining,
        )
    })
    .map_err(|_| {
        failure(
            abi::OperationErrorCode::Limit,
            "Host invocation budget exceeded",
        )
    })?;
    state.operation_calls_remaining -= 1;
    state.input_bytes_remaining -= input;
    let host = state
        .host
        .clone()
        .ok_or_else(|| failure(abi::OperationErrorCode::Unsupported, "Host unavailable"))?;
    let runtime = state.runtime.clone().ok_or_else(|| {
        failure(
            abi::OperationErrorCode::Unsupported,
            "Host runtime unavailable",
        )
    })?;
    let cancellation = state.cancellation.child_token();
    let _guard = CancelOnDrop(cancellation.clone());
    let epoch = state.epoch_cancellation.clone();
    let started = Instant::now();
    let publication = arguments.get("_file_mutation").and_then(Value::as_bool) == Some(true);
    let result=runtime.block_on(async {
        let work=async {
            for path in resources {
                host.normalize_resource(PathBuf::from(path),cancellation.clone()).await.map_err(host_error)?;
            }
            if cancellation.is_cancelled() || epoch.is_cancelled() { return Err(failure(abi::OperationErrorCode::Cancelled,"Host operation cancelled")); }
            let op=host::OpContext {deadline:None,cancel:cancellation.clone(),trace_id:context.call_id.clone(),principal:context.processes.session_id().to_owned()};
            execute(host,op,Some(context)).await
        };
        if publication { work.await } else {
            tokio::select! { biased;
                _=cancellation.cancelled()=>Err(failure(abi::OperationErrorCode::Cancelled,"Host operation cancelled")),
                _=epoch.cancelled()=>Err(failure(abi::OperationErrorCode::Cancelled,"Host operation cancelled")),
                result=work=>result,
            }
        }
    });
    // Awaiting Host work does not consume guest CPU wall time or fuel.
    state.deadline += started.elapsed();
    let (output, bytes) = result?;
    crate::budget::check(
        crate::LimitKind::ToolOutput,
        bytes,
        state.output_bytes_remaining,
    )
    .map_err(|_| {
        failure(
            abi::OperationErrorCode::Limit,
            "Host aggregate response budget exceeded",
        )
    })?;
    state.output_bytes_remaining -= bytes;
    Ok(output)
}

/// Capture is an explicit guest primitive, but bytes stay inside Session. The
/// trusted invocation supplies attribution and rejects late or over-budget points.
pub(super) fn capture(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (path, max_bytes): (String, u64),
) -> wasmtime::Result<(Result<(), abi::OperationError>,)> {
    let state = caller.data_mut();
    let result = (|| {
        let path = resource(&path)?;
        let max_bytes = number(
            max_bytes,
            state.max_output_bytes.min(1_048_576),
            "cache-point max_bytes must be positive and at most 1048576 bytes within the output bound",
        )?;
        crate::budget::check(
            crate::LimitKind::CachePoints,
            1,
            usize::from(state.cache_points_remaining),
        )
        .map_err(|_| {
            failure(
                abi::OperationErrorCode::Limit,
                "cache-point invocation budget exceeded",
            )
        })?;
        state.cache_points_remaining -= 1;
        let request = host::CachePointRequest {
            path: PathBuf::from(&path),
            max_bytes,
        };
        bridge(
            state,
            vec![path.clone()],
            json!({"path":path,"max_bytes":max_bytes,"_cache_point":true}),
            move |host, token, invocation| {
                Box::pin(async move {
                    let invocation = invocation.ok_or_else(|| {
                        failure(
                            abi::OperationErrorCode::Denied,
                            "cache-point invocation unavailable",
                        )
                    })?;
                    let expected_path = host::tool_api::ResourceApi::normalize_resource(
                        host.as_ref(),
                        &token,
                        request.path.clone(),
                    )
                    .await
                    .map_err(host_error)?;
                    let point =
                        host::tool_api::CachePointApi::cache_point(host.as_ref(), &token, request)
                            .await
                            .map_err(host_error)?;
                    // Host normalization preserves platform case and alias semantics;
                    // raw request strings need not match root-relative evidence paths.
                    let observed_path = host::tool_api::ResourceApi::normalize_resource(
                        host.as_ref(),
                        &token,
                        PathBuf::from(&point.path),
                    )
                    .await
                    .map_err(host_error)?;
                    if observed_path != expected_path {
                        return Err(failure(
                            abi::OperationErrorCode::Io,
                            "Host cache-point response differs from requested path",
                        ));
                    }
                    crate::budget::check(
                        crate::LimitKind::CacheBytes,
                        point.data.as_ref().map_or(0, Vec::len),
                        max_bytes,
                    )
                    .map_err(|_| {
                        failure(
                            abi::OperationErrorCode::Limit,
                            "Host cache-point response exceeds requested bound",
                        )
                    })?;
                    invocation
                        .record_cache_point(point)
                        .map_err(|error| match error {
                            crate::Error::Limit(_) => failure(
                                abi::OperationErrorCode::Limit,
                                "Session cache-point budget exceeded",
                            ),
                            crate::Error::Cancelled => failure(
                                abi::OperationErrorCode::Cancelled,
                                "cache-point invocation cancelled",
                            ),
                            _ => failure(
                                abi::OperationErrorCode::Io,
                                "Session rejected cache-point retention",
                            ),
                        })?;
                    Ok(((), 0))
                })
            },
        )
    })();
    let completed = finish(state, result);
    if let Some((diagnostic, _)) = state.operation_diagnostic.as_mut() {
        diagnostic.operation =
            Some(crate::observation::diagnostics::DiagnosticOperation::CacheCapture);
    }
    completed
}

pub(super) fn read(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (request,): (abi::ReadRequest,),
) -> wasmtime::Result<(Result<abi::ReadOutput, abi::OperationError>,)> {
    let state = caller.data_mut();
    let result = (|| {
        let path = resource(&request.path)?;
        let req = host::ReadRequest {
            path: PathBuf::from(&path),
            offset_line: number(
                request.offset_line,
                usize::MAX,
                "Host offset_line must be positive and fit the native line-number range",
            )?,
            max_lines: number(
                request.max_lines,
                100_000,
                "Host max_lines must be in 1..=100000",
            )?,
            max_bytes: number(
                request.max_bytes,
                state.output_bytes_remaining,
                "Host max_bytes must be positive and within its configured bound",
            )?,
        };
        let args = serde_json::to_value(&req)
            .map_err(|_| invalid("Host request arguments could not be serialized"))?;
        bridge(state, vec![path], args, move |host, token, _| {
            Box::pin(async move {
                let out = host::tool_api::TextReadApi::read_text(host.as_ref(), &token, req)
                    .await
                    .map_err(host_error)?;
                let size = out.content.len() + out.version.len() + 64;
                Ok((
                    abi::ReadOutput {
                        content: out.content,
                        start_line: out.start_line as u64,
                        next_line: out.next_line.map(|v| v as u64),
                        truncated: out.truncated,
                        version: out.version,
                    },
                    size,
                ))
            })
        })
    })();
    finish(state, result)
}
pub(super) fn write(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (request,): (abi::WriteRequest,),
) -> wasmtime::Result<(Result<abi::WriteOutput, abi::OperationError>,)> {
    let state = caller.data_mut();
    let result = (|| {
        let path = resource(&request.path)?;
        let precondition = match request.precondition {
            abi::WritePrecondition::Missing => host::WritePrecondition::Missing,
            abi::WritePrecondition::Version(v) => host::WritePrecondition::Version(v),
        };
        let req = host::WriteRequest {
            path: PathBuf::from(&path),
            content: request.content,
            precondition,
        };
        let args = serde_json::to_value(&req)
            .map_err(|_| invalid("Host request arguments could not be serialized"))?;
        bridge(state, vec![path], args, move |host, token, _| {
            Box::pin(async move {
                let out = host::tool_api::TextWriteApi::write(host.as_ref(), &token, req)
                    .await
                    .map_err(host_error)?;
                let size = out.version.len() + 8;
                Ok((
                    abi::WriteOutput {
                        version: out.version,
                        bytes: out.bytes as u64,
                    },
                    size,
                ))
            })
        })
    })();
    if result.is_ok() {
        state.known_write_committed = true;
    }
    finish(state, result)
}
pub(super) fn glob(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (request,): (abi::GlobRequest,),
) -> wasmtime::Result<(Result<abi::GlobOutput, abi::OperationError>,)> {
    let state = caller.data_mut();
    let result = (|| {
        let root = resource(&request.root)?;
        let req = host::GlobRequest {
            root: PathBuf::from(&root),
            pattern: request.pattern,
            include_hidden: request.include_hidden,
            include_ignored: request.include_ignored,
            include_directories: request.include_directories,
            max_results: number(
                request.max_results,
                10_000,
                "Host max_results must be in 1..=10000",
            )?,
        };
        let args = serde_json::to_value(&req)
            .map_err(|_| invalid("Host request arguments could not be serialized"))?;
        bridge(state, vec![root], args, move |host, token, _| {
            Box::pin(async move {
                let out = host::tool_api::GlobApi::glob(host.as_ref(), &token, req)
                    .await
                    .map_err(host_error)?;
                let entries: Vec<abi::GlobEntry> = out
                    .entries
                    .into_iter()
                    .map(|entry| abi::GlobEntry {
                        path: entry.path.to_string_lossy().into_owned(),
                        kind: match entry.kind {
                            host::GlobEntryKind::File => abi::GlobEntryKind::File,
                            host::GlobEntryKind::Directory => abi::GlobEntryKind::Directory,
                            host::GlobEntryKind::Symlink => abi::GlobEntryKind::Symlink,
                        },
                    })
                    .collect();
                let size = entries.iter().map(|entry| entry.path.len() + 16).sum();
                Ok((
                    abi::GlobOutput {
                        entries,
                        truncated: out.truncated,
                    },
                    size,
                ))
            })
        })
    })();
    finish(state, result)
}
pub(super) fn grep(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (request,): (abi::GrepRequest,),
) -> wasmtime::Result<(Result<abi::GrepOutput, abi::OperationError>,)> {
    let state = caller.data_mut();
    let result = (|| {
        let root = resource(&request.root)?;
        let req = host::GrepRequest {
            root: PathBuf::from(&root),
            pattern: request.pattern,
            include_glob: request.include_glob,
            case_sensitive: request.case_sensitive,
            literal: request.literal,
            max_matches: number(
                request.max_matches,
                10_000,
                "Host max_matches must be in 1..=10000",
            )?,
            max_bytes: number(
                request.max_bytes,
                state.output_bytes_remaining,
                "Host max_bytes must be positive and within its configured bound",
            )?,
        };
        let args = serde_json::to_value(&req)
            .map_err(|_| invalid("Host request arguments could not be serialized"))?;
        bridge(state, vec![root], args, move |host, token, _| {
            Box::pin(async move {
                let out = host::tool_api::GrepApi::grep(host.as_ref(), &token, req)
                    .await
                    .map_err(host_error)?;
                let matches: Vec<_> = out
                    .matches
                    .into_iter()
                    .map(|m| abi::GrepMatch {
                        path: m.path.to_string_lossy().into_owned(),
                        line: m.line as u64,
                        text: m.text,
                    })
                    .collect();
                let size = matches
                    .iter()
                    .map(|m| m.path.len() + m.text.len() + 16)
                    .sum();
                Ok((
                    abi::GrepOutput {
                        matches,
                        truncated: out.truncated,
                    },
                    size,
                ))
            })
        })
    })();
    finish(state, result)
}
