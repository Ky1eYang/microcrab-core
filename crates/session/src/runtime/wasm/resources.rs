//! Independent Host interfaces retain their supported Runtime mappings. Raw file
//! reads and mutation primitives have no implemented execution adapter.
//! Unmapped file/network operations remain typed Unsupported. General process
//! and shell imports share one admitted Session-owned resource mapping.
mod execution;
use super::microcrab::host::{filesystem as fs, net, process, types};
use super::{ImportFailure, RuntimeState, ToolContext};
use microcrab_host_api::{self as host, FsApi};
use serde_json::{Value, json};
use std::{
    path::Path,
    time::{Duration, Instant, UNIX_EPOCH},
};
use wasmtime::{StoreContextMut, component::Linker};

/// Availability describes Runtime mappings, independently of backend support.
pub(super) fn supported_operation(name: &str) -> bool {
    matches!(
        name,
        "filesystem.stat"
            | "process.spawn"
            | "process.status"
            | "process.wait"
            | "process.wait-for-state"
            | "process.signal"
            | "process.kill-tree"
            | "process.stdout"
            | "process.stderr"
            | "process.write-stdin"
            | "process.close-stdin"
            | "process.resize-pty"
            | "process.release"
            | "shell.launch"
            | "shell.read"
            | "shell.drain"
            | "shell.write"
    )
}

fn private_error(category: types::ErrorCategory, message: &str) -> ImportFailure<types::HostError> {
    ImportFailure::local(types::HostError {
        category,
        retryable: false,
        message: message.into(),
    })
}

fn error(category: types::ErrorCategory, reason: &'static str) -> ImportFailure<types::HostError> {
    let mut failure = private_error(category, reason);
    let mut diagnostic = diagnostic(&failure);
    diagnostic.reason = Some(crate::observation::diagnostics::SafeDiagnosticReason::new(
        reason,
    ));
    failure.message = super::encode_diagnostic(&diagnostic);
    failure
}

fn diagnostic(failure: &types::HostError) -> crate::observation::diagnostics::ToolDiagnostic {
    use crate::observation::diagnostics::{
        DiagnosticCode as C, DiagnosticLayer as L, DiagnosticPhase as P, ToolDiagnostic,
    };
    if let Some(safe) = super::decode_diagnostic(&failure.message) {
        return safe;
    }
    let code = match failure.category {
        types::ErrorCategory::NotFound => C::NotFound,
        types::ErrorCategory::Permission => C::PermissionDenied,
        types::ErrorCategory::Timeout => C::Timeout,
        types::ErrorCategory::Cancelled => C::Cancelled,
        types::ErrorCategory::Conflict => C::Conflict,
        types::ErrorCategory::Unsupported => C::Unsupported,
        types::ErrorCategory::Io => C::Io,
    };
    ToolDiagnostic::new(code, L::Host, P::HostOperation)
}

fn host_error(source: host::HostError) -> ImportFailure<types::HostError> {
    use host::ErrorCategory as H;
    use types::ErrorCategory as W;
    let category = match source.category {
        H::NotFound => W::NotFound,
        H::Permission => W::Permission,
        H::Timeout => W::Timeout,
        H::Cancelled => W::Cancelled,
        H::Conflict => W::Conflict,
        H::Unsupported => W::Unsupported,
        H::Io => W::Io,
    };
    // This is the authorized tool-result path, not telemetry. The executing
    // Host owns its failure description; discarding it prevents correction of
    // concrete paths, process failures and caller-context cancellation.
    let mut result = private_error(category, &source.message);
    if source.public_reason.as_deref() == Some(execution::SESSION_DIAGNOSTIC)
        && source.message.len() <= super::MAX_DIAGNOSTIC_CARRIER_BYTES
        && source.message.starts_with("tool_diagnostic:")
    {
        // This marker is set only by the Runtime's typed Session-error adapter.
        // Unmarked native text below never becomes diagnostic metadata.
    } else if let Some(reason) = source.public_reason.as_deref() {
        let mut safe = diagnostic(&private_error(category, ""));
        safe.reason = Some(crate::observation::diagnostics::SafeDiagnosticReason::new(
            reason,
        ));
        result.message = super::encode_diagnostic(&safe);
    } else if result.message.starts_with("tool_diagnostic:") {
        result.message.insert_str(0, "Host: ");
    }
    result.retryable = source.retryable;
    result.authoritative = false;
    result
}

fn tool_error(source: host::ToolError) -> host::HostError {
    use host::ErrorCategory as C;
    let message = source.to_string();
    let reason = source.public_reason().map(str::to_owned);
    let category = match source {
        host::ToolError::Host(source) => return source,
        host::ToolError::Io(source) => return source.into(),
        host::ToolError::Unsupported(_) => C::Unsupported,
        host::ToolError::Cancelled => C::Cancelled,
        host::ToolError::Timeout | host::ToolError::OperationDeadline => C::Timeout,
        host::ToolError::PathOutsideRoot | host::ToolError::CwdOutsideRoot => C::Conflict,
        host::ToolError::FileConflict | host::ToolError::WorkspaceConflict => C::Conflict,
        _ => C::Io,
    };
    let mut error = host::HostError::new(category, message);
    error.public_reason = reason;
    error
}

/// The frozen Host ABI has no invalid-path category. Preserve the concrete
/// typed normalization fact beside its ABI projection, before it is erased.
fn normalization_error(
    source: host::ToolError,
    diagnostic: &mut Option<(crate::observation::diagnostics::ToolDiagnostic, String)>,
) -> host::HostError {
    if matches!(
        source,
        host::ToolError::PathOutsideRoot | host::ToolError::CwdOutsideRoot
    ) {
        let safe = crate::Error::from_host_error(source).tool_diagnostic();
        let message = safe.summary().to_owned();
        *diagnostic = Some((safe, message.clone()));
        return host::HostError::new(host::ErrorCategory::Conflict, message);
    }
    tool_error(source)
}

fn finish<T>(
    state: &mut RuntimeState,
    result: Result<T, ImportFailure<types::HostError>>,
) -> wasmtime::Result<(Result<T, types::HostError>,)> {
    if let Err(failure) = &result {
        state.operation_diagnostic_authoritative = failure.authoritative;
        use crate::observation::diagnostics::{DiagnosticCode as C, DiagnosticOperation};
        let mut diagnostic = state
            .operation_diagnostic
            .take()
            .map(|(diagnostic, _)| diagnostic)
            .filter(|diagnostic| diagnostic.code == C::PathOutsideRoot)
            .unwrap_or_else(|| diagnostic(failure));
        diagnostic.operation = Some(DiagnosticOperation::Resource);
        if state.known_write_committed {
            diagnostic.effects = crate::observation::diagnostics::EffectKnowledge::Committed;
        }
        diagnostic.retryable = Some(failure.retryable);
        if diagnostic.code == crate::observation::diagnostics::DiagnosticCode::PathOutsideRoot {
            state
                .confinement_diagnostic
                .get_or_insert_with(|| diagnostic.clone());
        }
        state.operation_diagnostic = Some((diagnostic, failure.message.clone()));
        match failure.category {
            types::ErrorCategory::Cancelled => state.operation_cancelled = true,
            _ => {}
        }
    } else {
        state.operation_diagnostic = None;
        state.operation_diagnostic_authoritative = false;
    }
    Ok((result.map_err(|failure| failure.wire),))
}

/// All context facts come from Runtime. There are no context fields on the wire.
fn invocation_context(
    invocation: Option<&ToolContext>,
    _started: Instant,
    cancellation: tokio_util::sync::CancellationToken,
) -> Result<host::OpContext, ImportFailure<types::HostError>> {
    let invocation = invocation.ok_or_else(|| {
        error(
            types::ErrorCategory::Permission,
            "Host resources require an authorized invocation",
        )
    })?;
    invocation.ensure_admitted(None).map_err(|source| {
        private_error(
            if matches!(source, crate::policy::PermissionError::Cancelled) {
                types::ErrorCategory::Cancelled
            } else {
                types::ErrorCategory::Permission
            },
            source.public_reason(),
        )
    })?;
    let snapshot = invocation.state.snapshot();
    Ok(host::OpContext {
        deadline: None,
        cancel: cancellation,
        trace_id: format!(
            "{}:{}:{}:{}",
            snapshot.epoch_id,
            snapshot
                .turn_id
                .map_or_else(|| "none".into(), |id| id.to_string()),
            snapshot
                .round_id
                .map_or_else(|| "none".into(), |id| id.to_string()),
            invocation.call_id
        ),
        principal: invocation.processes.session_id().into(),
    })
}

fn begin(
    state: &mut RuntimeState,
    input_bytes: usize,
) -> Result<host::OpContext, ImportFailure<types::HostError>> {
    state.operation_diagnostic = None;
    state.operation_diagnostic_authoritative = false;
    use types::ErrorCategory as C;
    if state.operation_cancelled
        || state.cancellation.is_cancelled()
        || state.epoch_cancellation.is_cancelled()
    {
        return Err(error(C::Cancelled, "Host resource operation cancelled"));
    }
    let started = Instant::now();
    if started >= state.deadline {
        return Err(error(C::Timeout, "WASM wall deadline exceeded"));
    }
    let context = invocation_context(
        state.tool_context.as_ref(),
        started,
        state.cancellation.child_token(),
    )?;
    if state.operation_scope.is_none() {
        return Err(error(C::Permission, "Host invocation scope absent"));
    }
    context.check().map_err(host_error)?;
    crate::budget::check(
        crate::LimitKind::HostOperations,
        1,
        usize::from(state.operation_calls_remaining),
    )
    .and_then(|()| {
        crate::budget::check(
            crate::LimitKind::ToolArguments,
            input_bytes,
            state.input_bytes_remaining,
        )
    })
    .map_err(|_| error(C::Io, "Host invocation budget exceeded"))?;
    state.operation_calls_remaining -= 1;
    state.input_bytes_remaining -= input_bytes;
    Ok(context)
}

fn unavailable<T>(
    state: &mut RuntimeState,
    bytes: usize,
) -> wasmtime::Result<(Result<T, types::HostError>,)> {
    let result = begin(state, bytes).and_then(|_| {
        Err(error(
            types::ErrorCategory::Unsupported,
            "Host operation unavailable: no scope-preserving Runtime mapping",
        ))
    });
    finish(state, result)
}

fn file_kind(kind: host::filesystem::FileKind) -> fs::FileKind {
    match kind {
        host::filesystem::FileKind::File => fs::FileKind::File,
        host::filesystem::FileKind::Directory => fs::FileKind::Directory,
        host::filesystem::FileKind::Symlink => fs::FileKind::Symlink,
        host::filesystem::FileKind::Other => fs::FileKind::Other,
    }
}

/// Preserve the guest CPU allowance on both success and typed Host failure.
fn exclude_host_wait<T>(
    deadline: &mut Instant,
    elapsed: Duration,
    result: Result<T, ImportFailure<types::HostError>>,
) -> Result<T, ImportFailure<types::HostError>> {
    *deadline += elapsed;
    result
}

fn stat(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (path,): (String,),
) -> wasmtime::Result<(Result<fs::FileMeta, types::HostError>,)> {
    let state = caller.data_mut();
    let result = (|| {
        let context = begin(state, path.len().saturating_add(16))?;
        if let Err(failure) = super::operations::resource(&path) {
            let mut safe = super::operations::diagnostic(&failure);
            // This frozen resource ABI classifies malformed targets as Permission.
            // Keep that classification while preserving the closed validation cause.
            safe.code = crate::observation::diagnostics::DiagnosticCode::PermissionDenied;
            return Err(private_error(
                types::ErrorCategory::Permission,
                &super::encode_diagnostic(&safe),
            ));
        }
        // Reserve the bounded metadata response before any backend call.
        const RESPONSE_BYTES: usize = 32;
        crate::budget::check(
            crate::LimitKind::ToolOutput,
            RESPONSE_BYTES,
            state.output_bytes_remaining,
        )
        .map_err(|_| error(types::ErrorCategory::Io, "Host response budget exceeded"))?;
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
        let epoch = state.epoch_cancellation.clone();
        let _cancel_on_drop = context.cancel.clone().drop_guard();
        // This synchronous import runs only on Runtime's blocking WASM worker.
        // Host I/O waiting does not consume the guest CPU budget.
        let started = Instant::now();
        let mut normalization_diagnostic = None;
        let metadata = runtime.block_on(async {
            tokio::select! { biased;
                _ = epoch.cancelled() => Err(error(types::ErrorCategory::Cancelled,
                    "Host resource operation cancelled")),
                result = context.run(async {
                    let actual = backend.normalize_resource(path.into(), context.cancel.clone())
                        .await.map_err(|source| normalization_error(source, &mut normalization_diagnostic))?;
                    FsApi::stat(backend.as_ref(), &context, Path::new(&actual)).await
                }) => result.map_err(host_error),
            }
        });
        state.operation_diagnostic = normalization_diagnostic;
        let metadata = exclude_host_wait(&mut state.deadline, started.elapsed(), metadata)?;
        state.output_bytes_remaining -= RESPONSE_BYTES;
        Ok(fs::FileMeta {
            kind: file_kind(metadata.kind),
            size: metadata.size,
            modified_ms: metadata
                .modified
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .and_then(|duration| u64::try_from(duration.as_millis()).ok()),
        })
    })();
    finish(state, result)
}

// Register every declared WIT function even when no execution adapter exists. Unsupported
// is a normal typed result, never a trap or an unknown-import loader rejection.
// Rejected payloads are charged for input and calls; they cannot invoke a backend.
macro_rules! unsupported {
    ($name:ident, ($($arg:ident: $ty:ty),*), $output:ty, $bytes:expr) => {
        fn $name(mut caller: StoreContextMut<'_, RuntimeState>,
            ($($arg,)*): ($($ty,)*))
            -> wasmtime::Result<(Result<$output, types::HostError>,)>
        {
            $(let _ = &$arg;)*
            unavailable(caller.data_mut(), $bytes)
        }
    };
}

fn process_spec_bytes(spec: &process::ProcessSpec) -> usize {
    let arguments = spec.args.iter().fold(0usize, |bytes, argument| {
        bytes.saturating_add(argument.len()).saturating_add(8)
    });
    let environment = spec.env.iter().fold(0usize, |bytes, (key, value)| {
        bytes
            .saturating_add(key.len())
            .saturating_add(value.len())
            .saturating_add(16)
    });
    spec.program
        .len()
        .saturating_add(arguments)
        .saturating_add(environment)
        .saturating_add(spec.cwd.as_ref().map_or(0, String::len))
        .saturating_add(32)
}

unsupported!(list, (path: String, opts: fs::ListOptions), Vec<fs::DirEntry>, path.len().saturating_add(16));
unsupported!(open, (path: String, flags: fs::OpenFlags), fs::FileHandle, path.len().saturating_add(16));
unsupported!(file_read, (h: fs::FileHandle, offset: u64, len: u32), Vec<u8>, 24);
unsupported!(file_write, (h: fs::FileHandle, offset: u64, data: Vec<u8>), u32, data.len().saturating_add(24));
unsupported!(fsync, (h: fs::FileHandle), (), 8);
unsupported!(close, (h: fs::FileHandle), (), 8);
unsupported!(mkdir, (path: String, recursive: bool), (), path.len().saturating_add(16));
unsupported!(rename, (from: String, to: String), (), from.len().saturating_add(to.len()).saturating_add(16));
unsupported!(remove, (path: String, recursive: bool), (), path.len().saturating_add(16));
unsupported!(atomic_write, (path: String, data: Vec<u8>), (), path.len().saturating_add(data.len()).saturating_add(16));
unsupported!(resolve, (name: String), Vec<String>, name.len().saturating_add(8));
unsupported!(connect_tcp, (addr: net::SocketAddr), net::SocketHandle, addr.host.len().saturating_add(16));
unsupported!(listen_tcp, (addr: net::SocketAddr), net::ListenerHandle, addr.host.len().saturating_add(16));
unsupported!(accept, (l: net::ListenerHandle), (net::SocketHandle, net::SocketAddr), 8);
unsupported!(net_read, (s: net::SocketHandle, max: u32), Vec<u8>, 16);
unsupported!(net_write, (s: net::SocketHandle, data: Vec<u8>), u32, data.len().saturating_add(16));
unsupported!(close_socket, (s: net::SocketHandle), (), 8);
unsupported!(close_listener, (l: net::ListenerHandle), (), 8);
pub(super) fn add_to_linker(linker: &mut Linker<RuntimeState>) -> wasmtime::Result<()> {
    let mut interface = linker.instance("microcrab:host/process@0.1.0")?;
    interface.func_wrap("spawn", execution::spawn)?;
    interface.func_wrap("status", execution::status)?;
    interface.func_wrap("wait-for-state", execution::wait_for_state)?;
    interface.func_wrap("release", execution::release)?;
    interface.func_wrap("wait", execution::wait)?;
    interface.func_wrap("signal", execution::signal)?;
    interface.func_wrap("kill-tree", execution::kill_tree)?;
    interface.func_wrap("read-stdout", execution::read_stdout)?;
    interface.func_wrap("read-stderr", execution::read_stderr)?;
    interface.func_wrap("write-stdin", execution::write_stdin)?;
    interface.func_wrap("close-stdin", execution::close_stdin)?;
    interface.func_wrap("resize-pty", execution::resize_pty)?;
    let mut interface = linker.instance("microcrab:host/filesystem@0.1.0")?;
    interface.func_wrap("stat", stat)?;
    interface.func_wrap("list", list)?;
    interface.func_wrap("open", open)?;
    interface.func_wrap("read", file_read)?;
    interface.func_wrap("write", file_write)?;
    interface.func_wrap("fsync", fsync)?;
    interface.func_wrap("close", close)?;
    interface.func_wrap("mkdir", mkdir)?;
    interface.func_wrap("rename", rename)?;
    interface.func_wrap("remove", remove)?;
    interface.func_wrap("atomic-write", atomic_write)?;
    let mut interface = linker.instance("microcrab:host/net@0.1.0")?;
    interface.func_wrap("resolve", resolve)?;
    interface.func_wrap("connect-tcp", connect_tcp)?;
    interface.func_wrap("listen-tcp", listen_tcp)?;
    interface.func_wrap("accept", accept)?;
    interface.func_wrap("read", net_read)?;
    interface.func_wrap("write", net_write)?;
    interface.func_wrap("close-socket", close_socket)?;
    interface.func_wrap("close-listener", close_listener)?;
    let mut interface = linker.instance("microcrab:host/shell@0.1.0")?;
    interface.func_wrap("launch", execution::launch)?;
    interface.func_wrap("read", execution::read)?;
    interface.func_wrap("drain", execution::drain)?;
    interface.func_wrap("write", execution::write)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn runtime_state(invoked: bool) -> RuntimeState {
        let mut snapshot = crate::SessionState::new(crate::Limits::default());
        snapshot.epoch_id = 7;
        snapshot.turn_id = Some(8);
        snapshot.round_id = Some(9);
        let mut invocation = ToolContext::detached(snapshot, Default::default());
        invocation.call_id = "host-call".into();
        crate::runtime::wasm::test_admission::admit(&mut invocation, None);
        RuntimeState {
            limits: wasmtime::StoreLimitsBuilder::new().build(),
            host: None,
            outputs: Default::default(),
            next_output: 1,
            borrowed_processes: Default::default(),
            admitted_process_scope: None,
            process_grants: Default::default(),
            binding: None,
            tool_context: invoked.then_some(invocation),
            operation_scope: invoked.then(|| ("read".into(), json!({"path": "allowed"}))),
            confinement_diagnostic: None,
            operation_cancelled: false,
            operation_diagnostic: None,
            operation_diagnostic_authoritative: false,
            known_write_committed: false,
            host_info_json: Err("unavailable".into()),
            runtime: None,
            cancellation: Default::default(),
            cooperative_driver_cancellation: false,
            epoch_cancellation: Default::default(),
            deadline: Instant::now() + std::time::Duration::from_secs(10),
            max_input_bytes: 1024,
            max_output_bytes: 1024,
            operation_calls_remaining: if invoked { 16 } else { 0 },
            cache_points_remaining: if invoked { 2 } else { 0 },
            patch_plan: None,
            patch_receipts: Vec::new(),
            input_bytes_remaining: 1024,
            output_bytes_remaining: 1024,
        }
    }

    #[test]
    fn unmapped_operations_return_typed_unsupported_without_backend() {
        let mut state = runtime_state(true);
        let (result,) = unavailable::<()>(&mut state, 24).expect("typed errors do not trap");
        assert!(matches!(
            result,
            Err(types::HostError {
                category: types::ErrorCategory::Unsupported,
                ..
            })
        ));
        assert!(!state.operation_cancelled);
        assert_eq!(state.operation_calls_remaining, 15);
        assert_eq!(state.input_bytes_remaining, 1000);
        assert!(supported_operation("filesystem.stat"));
        for operation in [
            "filesystem.read",
            "filesystem.open",
            "filesystem.atomic-write",
            "filesystem.write",
            "net.connect-tcp",
        ] {
            assert!(!supported_operation(operation));
        }
    }

    #[test]
    fn host_permission_is_guest_owned_and_does_not_block_later_imports() {
        let mut state = runtime_state(true);
        let permission = host_error(host::HostError::new(
            host::ErrorCategory::Permission,
            "Host OS denied this file",
        ));
        let (failure,) = finish::<()>(&mut state, Err(permission)).unwrap();
        assert!(failure.is_err());
        let diagnostic = &state.operation_diagnostic.as_ref().unwrap().0;
        assert_eq!(
            diagnostic.layer,
            crate::observation::diagnostics::DiagnosticLayer::Host
        );
        assert!(!state.operation_diagnostic_authoritative);
        assert!(begin(&mut state, 0).is_ok());
        assert!(finish(&mut state, Ok(())).unwrap().0.is_ok());
        assert!(state.operation_diagnostic.is_none());
        assert!(state.confinement_diagnostic.is_none());
    }

    #[test]
    fn discovery_and_epoch_cancellation_are_authoritative() {
        let mut state = runtime_state(false);
        let (result,) = unavailable::<()>(&mut state, 8).expect("discovery failure does not trap");
        assert!(matches!(
            result,
            Err(types::HostError {
                category: types::ErrorCategory::Permission,
                ..
            })
        ));
        let mut state = runtime_state(true);
        state.epoch_cancellation.cancel();
        let (result,) = unavailable::<()>(&mut state, 8).expect("cancellation does not trap");
        assert!(matches!(
            result,
            Err(types::HostError {
                category: types::ErrorCategory::Cancelled,
                ..
            })
        ));
        assert!(state.operation_cancelled);
        // Clearing the token cannot erase the authoritative outcome.
        state.epoch_cancellation = Default::default();
        assert!(matches!(
            begin(&mut state, 8).map_err(|failure| failure.wire),
            Err(types::HostError {
                category: types::ErrorCategory::Cancelled,
                ..
            })
        ));
    }

    #[test]
    fn host_context_uses_actual_invocation_snapshot() {
        let mut state = runtime_state(true);
        let context = begin(&mut state, 8).expect("invoked context exists");
        assert_eq!(context.trace_id, "7:8:9:host-call");
        assert_eq!(
            context.principal,
            state
                .tool_context
                .as_ref()
                .expect("invoked")
                .processes
                .session_id()
        );
        assert!(context.deadline.is_none());
        state.cancellation.cancel();
        assert!(matches!(
            context.check(),
            Err(host::HostError {
                category: host::ErrorCategory::Cancelled,
                ..
            })
        ));
    }

    #[test]
    fn host_wait_has_no_generic_deadline_and_does_not_consume_guest_budget() {
        let state = runtime_state(true);
        let invocation = state.tool_context.as_ref().expect("invoked");
        let started = Instant::now();
        let context = invocation_context(Some(invocation), started, Default::default())
            .expect("invoked context exists");
        assert!(context.deadline.is_none());
        let guest_deadline = started + Duration::from_millis(20);
        let wait = Duration::from_secs(5);
        let mut deadline = guest_deadline;
        assert_eq!(
            exclude_host_wait(&mut deadline, wait, Ok(42)).ok(),
            Some(42)
        );
        assert_eq!(deadline, guest_deadline + wait);
        for category in [
            types::ErrorCategory::Permission,
            types::ErrorCategory::Cancelled,
            types::ErrorCategory::Timeout,
            types::ErrorCategory::Unsupported,
        ] {
            let mut deadline = guest_deadline;
            let failure = error(category, "Host rejected after waiting");
            let result = exclude_host_wait::<()>(&mut deadline, wait, Err(failure));
            assert!(result.is_err());
            assert_eq!(deadline, guest_deadline + wait);
        }
    }

    #[test]
    fn expired_deadline_and_exhausted_budget_fail_before_backend() {
        let mut state = runtime_state(true);
        state.deadline = Instant::now();
        assert!(matches!(
            begin(&mut state, 8).map_err(|failure| failure.wire),
            Err(types::HostError {
                category: types::ErrorCategory::Timeout,
                ..
            })
        ));
        let mut state = runtime_state(true);
        state.input_bytes_remaining = 7;
        assert!(begin(&mut state, 8).is_err());
        assert_eq!(state.operation_calls_remaining, 16);
        assert_eq!(state.input_bytes_remaining, 7);
        state.input_bytes_remaining = 1024;
        state.operation_calls_remaining = 0;
        assert!(begin(&mut state, 8).is_err());
    }

    #[test]
    fn discovery_has_no_resource_context() {
        let result = invocation_context(None, Instant::now(), Default::default())
            .map_err(|failure| failure.wire);
        assert!(matches!(
            result,
            Err(types::HostError {
                category: types::ErrorCategory::Permission,
                ..
            })
        ));
    }

    #[test]
    fn backend_errors_preserve_actionable_details_on_the_authorized_result_path() {
        let detail =
            "shell.drain process_handle=7: caller deadline elapsed; output remains available";
        for (category, expected) in [
            (
                host::ErrorCategory::NotFound,
                types::ErrorCategory::NotFound,
            ),
            (
                host::ErrorCategory::Permission,
                types::ErrorCategory::Permission,
            ),
            (host::ErrorCategory::Timeout, types::ErrorCategory::Timeout),
            (
                host::ErrorCategory::Cancelled,
                types::ErrorCategory::Cancelled,
            ),
            (
                host::ErrorCategory::Conflict,
                types::ErrorCategory::Conflict,
            ),
            (
                host::ErrorCategory::Unsupported,
                types::ErrorCategory::Unsupported,
            ),
            (host::ErrorCategory::Io, types::ErrorCategory::Io),
        ] {
            let mut source = host::HostError::new(
                category,
                format!(
                    "{detail}; token=private-backend-token; response_body=private-backend-body"
                ),
            );
            source.retryable = true;
            let wire = host_error(source);
            assert_eq!(
                std::mem::discriminant(&wire.category),
                std::mem::discriminant(&expected)
            );
            assert!(wire.retryable);
            let safe = diagnostic(&wire);
            assert_eq!(
                super::super::decode_diagnostic(&wire.message),
                Some(safe.clone())
            );
            assert_eq!(
                safe.layer,
                crate::observation::diagnostics::DiagnosticLayer::Host
            );
            assert_eq!(
                safe.phase,
                crate::observation::diagnostics::DiagnosticPhase::HostOperation
            );
            assert!(safe.summary().contains(detail));
            let public = safe.result_json();
            assert!(!public.contains("private-backend-token"));
            assert!(!public.contains("private-backend-body"));
            assert!(!public.contains("tool_diagnostic:"));
        }
        let missing = tool_error(host::ToolError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "requested executable does not exist",
        )));
        assert_eq!(missing.category, host::ErrorCategory::NotFound);
        assert!(missing.message.contains("requested executable"));
    }
}
