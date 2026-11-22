//! Communication imports share a frozen HostInfo view and one invocation binder.
//! Sync imports run on the WASM blocking worker. Only writes await the Session
//! command pump through the captured runtime; no Session lock spans that await.

mod typed;
pub(super) use typed::{TYPED_VARIABLES_INTERFACE, add_typed_to_linker};

use super::microcrab::agent::host_info::{CommunicationError, CommunicationErrorCode};
use super::{CancelOnDrop, RuntimeState};
use crate::state::wasm::SessionBinding;
use crate::{Error, VariablesChange};
use std::time::Instant;
use wasmtime::StoreContextMut;

fn failure(code: CommunicationErrorCode, message: impl Into<String>) -> CommunicationError {
    CommunicationError {
        code,
        message: message.into(),
    }
}

fn bounded<T>(
    result: Result<T, CommunicationError>,
    output_bytes: impl FnOnce(&T) -> usize,
    limit: usize,
) -> wasmtime::Result<(Result<T, CommunicationError>,)> {
    let result = match result {
        Ok(value) => {
            if crate::budget::check(crate::LimitKind::WasmBytes, output_bytes(&value), limit)
                .is_err()
            {
                Err(failure(
                    CommunicationErrorCode::Rejected,
                    "host communication response byte limit exceeded",
                ))
            } else {
                Ok(value)
            }
        }
        Err(error) => Err(error),
    };
    let result = result.map_err(|mut error| {
        // Preserve semantic error codes while keeping every diagnostic within
        // the string budget, including arbitrarily small configured budgets.
        if crate::budget::check(crate::LimitKind::WasmBytes, error.message.len(), limit).is_err() {
            error.message.clear();
        }
        error
    });
    Ok((result,))
}

pub(super) fn host_info_import(
    caller: StoreContextMut<'_, RuntimeState>,
    (): (),
) -> wasmtime::Result<(Result<String, CommunicationError>,)> {
    let state = caller.data();
    let result = state
        .host_info_json
        .clone()
        .map_err(|message| failure(CommunicationErrorCode::Rejected, message));
    // This value is also an invoke/describe argument, so both budgets apply.
    let limit = state.max_input_bytes.min(state.max_output_bytes);
    bounded(result, String::len, limit)
}

fn variable_get_import(
    caller: StoreContextMut<'_, RuntimeState>,
    (name,): (String,),
) -> wasmtime::Result<(Result<String, CommunicationError>,)> {
    let state = caller.data();
    if crate::budget::check(
        crate::LimitKind::ToolArguments,
        name.len(),
        state.max_input_bytes,
    )
    .is_err()
    {
        return bounded(
            Err(failure(
                CommunicationErrorCode::Rejected,
                "host variable name byte limit exceeded",
            )),
            String::len,
            state.max_output_bytes,
        );
    }
    let result = match &state.binding {
        None => Err(failure(
            CommunicationErrorCode::Unavailable,
            "no invocation Session binding",
        )),
        Some(binding) => match binding.get(&name) {
            Ok(Some(value)) => serde_json::to_string(&value)
                .map_err(|error| failure(CommunicationErrorCode::Rejected, error.to_string())),
            Ok(None) => Err(failure(
                CommunicationErrorCode::Absent,
                "variable is absent",
            )),
            Err(error) => Err(failure(
                CommunicationErrorCode::Unavailable,
                error.to_string(),
            )),
        },
    };
    bounded(result, String::len, state.max_output_bytes)
}

fn session_binding(state: &RuntimeState) -> Result<&SessionBinding, CommunicationError> {
    state.binding.as_ref().ok_or_else(|| {
        failure(
            CommunicationErrorCode::Unavailable,
            "no invocation Session binding",
        )
    })
}

fn apply_change(
    mut caller: StoreContextMut<'_, RuntimeState>,
    decode: impl FnOnce() -> Result<VariablesChange, CommunicationError>,
) -> wasmtime::Result<(Result<(), CommunicationError>,)> {
    let state = caller.data();
    let mut waited = None;
    let result = (|| {
        let binding = session_binding(state)?;
        let runtime = state.runtime.clone().ok_or_else(|| {
            failure(
                CommunicationErrorCode::Unavailable,
                "host writes require a Tokio runtime",
            )
        })?;
        let change = decode()?;
        let cancellation = state.cancellation.child_token();
        let _guard = CancelOnDrop(cancellation.clone());
        // Session input/command waits are not guest CPU execution time.
        let started = Instant::now();
        let result = runtime.block_on(async {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    Err(failure(CommunicationErrorCode::Cancelled, "variable change cancelled"))
                }
                _ = state.epoch_cancellation.cancelled() => {
                    Err(failure(CommunicationErrorCode::Cancelled, "variable change cancelled"))
                }
                result = binding.change(change, cancellation.clone()) => {
                    match result {
                        Ok(()) => Ok(()),
                        Err(Error::Cancelled) => Err(failure(CommunicationErrorCode::Cancelled, "variable change cancelled")),
                        Err(error) => Err(failure(CommunicationErrorCode::Rejected, error.to_string())),
                    }
                }
            }
        });
        waited = Some(started.elapsed());
        result
    })();
    let limit = state.max_output_bytes;
    if let Some(elapsed) = waited {
        caller.data_mut().deadline += elapsed;
    }
    bounded(result, |()| 0, limit)
}
