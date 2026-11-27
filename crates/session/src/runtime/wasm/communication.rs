//! Communication imports share a frozen HostInfo view.
use super::RuntimeState;
use super::microcrab::agent::host_info::{CommunicationError, CommunicationErrorCode};
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
