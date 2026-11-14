//! Communication imports share a frozen HostInfo view and one invocation binder.
//! Sync imports run on the WASM blocking worker. Only writes await the Session
//! command pump through the captured runtime; no Session lock spans that await.

use super::microcrab::agent::host_info::{CommunicationError, CommunicationErrorCode};
use super::{CancelOnDrop, RuntimeState};
use crate::Error;
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

pub(super) fn variable_get_import(
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

pub(super) fn variable_change_import(
    caller: StoreContextMut<'_, RuntimeState>,
    (change,): (String,),
) -> wasmtime::Result<(Result<(), CommunicationError>,)> {
    variable_change(caller, change, false)
}

pub(super) fn scoped_variable_change_import(
    caller: StoreContextMut<'_, RuntimeState>,
    (change,): (String,),
) -> wasmtime::Result<(Result<(), CommunicationError>,)> {
    variable_change(caller, change, true)
}

fn variable_change(
    caller: StoreContextMut<'_, RuntimeState>,
    change: String,
    scoped: bool,
) -> wasmtime::Result<(Result<(), CommunicationError>,)> {
    let mut caller = caller;
    let started = Instant::now();
    let state = caller.data();
    if crate::budget::check(
        crate::LimitKind::ToolArguments,
        change.len(),
        state.max_input_bytes,
    )
    .is_err()
    {
        return bounded(
            Err(failure(
                CommunicationErrorCode::Rejected,
                "host variable change byte limit exceeded",
            )),
            |()| 0,
            state.max_output_bytes,
        );
    }
    let result = (|| {
        let binding = state.binding.clone().ok_or_else(|| {
            failure(
                CommunicationErrorCode::Unavailable,
                "no invocation Session binding",
            )
        })?;
        let runtime = state.runtime.clone().ok_or_else(|| {
            failure(
                CommunicationErrorCode::Unavailable,
                "host writes require a Tokio runtime",
            )
        })?;
        let change = decode_change(&change, scoped)?;
        let cancellation = state.cancellation.child_token();
        let _guard = CancelOnDrop(cancellation.clone());
        // Session input/command waits are not guest CPU execution time.
        runtime.block_on(async {
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
        })
    })();
    let limit = state.max_output_bytes;
    caller.data_mut().deadline += started.elapsed();
    bounded(result, |()| 0, limit)
}

pub(super) fn variable_get_all_import(
    caller: StoreContextMut<'_, RuntimeState>,
    (): (),
) -> wasmtime::Result<(Result<String, CommunicationError>,)> {
    inspect_scoped(caller, false)
}

pub(super) fn variable_metadata_import(
    caller: StoreContextMut<'_, RuntimeState>,
    (): (),
) -> wasmtime::Result<(Result<String, CommunicationError>,)> {
    inspect_scoped(caller, true)
}

fn inspect_scoped(
    caller: StoreContextMut<'_, RuntimeState>,
    metadata: bool,
) -> wasmtime::Result<(Result<String, CommunicationError>,)> {
    let state = caller.data();
    let result = state
        .binding
        .as_ref()
        .ok_or_else(|| {
            failure(
                CommunicationErrorCode::Unavailable,
                "no invocation Session binding",
            )
        })
        .and_then(|binding| {
            let value = if metadata {
                binding.metadata()
            } else {
                binding.get_all()
            }
            .map_err(|error| failure(CommunicationErrorCode::Unavailable, error.to_string()))?;
            serde_json::to_string(&value)
                .map_err(|error| failure(CommunicationErrorCode::Rejected, error.to_string()))
        });
    bounded(result, String::len, state.max_output_bytes)
}

fn decode_change(change: &str, scoped: bool) -> Result<crate::VariablesChange, CommunicationError> {
    // Struct deserializers may accept JSON sequences; the wire shape is
    // explicitly an object, with canonical field/key checks below.
    if !change.trim_start().starts_with('{') {
        return Err(failure(
            CommunicationErrorCode::Rejected,
            "variable change must be a JSON object",
        ));
    }
    // Deserialize directly into the canonical deny_unknown_fields DTO and
    // reuse the command endpoint's reserved/overlap/duplicate-key validator.
    // Version 0.1.0 must reject even an empty scoped_set field explicitly.
    if !scoped {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct LegacyChange {
            #[serde(default)]
            set: serde_json::Map<String, serde_json::Value>,
            #[serde(default)]
            remove: Vec<String>,
        }
        let legacy: LegacyChange = serde_json::from_str(change)
            .map_err(|error| failure(CommunicationErrorCode::Rejected, error.to_string()))?;
        let _ = (legacy.set, legacy.remove);
    }
    let change: crate::VariablesChange = serde_json::from_str(change)
        .map_err(|error| failure(CommunicationErrorCode::Rejected, error.to_string()))?;
    crate::state::access::validate_change(&change)
        .map_err(|error| failure(CommunicationErrorCode::Rejected, error.to_string()))?;
    Ok(change)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_variable_capability_rejects_scoped_json_and_new_capability_validates_it() {
        assert!(decode_change(r#"{"set":{"ordinary":1},"remove":[]}"#, false).is_ok());
        assert!(decode_change(r#"{"scoped_set":[]}"#, false).is_err());
        let scoped = r#"{"scoped_set":[{"name":"private","value":"synthetic-fixture","visibility":"invisible","persistence":"ephemeral"}]}"#;
        assert!(decode_change(scoped, false).is_err());
        assert_eq!(decode_change(scoped, true).unwrap().scoped_set.len(), 1);
        for forged in [
            r#"{"set":{"ordinary":1},"role":"runtime"}"#,
            r#"{"set":{"ordinary":1},"owner":"client"}"#,
            r#"{"scoped_set":[{"name":"private","value":1,"visibility":"invisible","persistence":"persisted","owner":"client"}]}"#,
        ] {
            assert!(decode_change(forged, false).is_err());
            assert!(decode_change(forged, true).is_err());
        }
        assert!(decode_change(r#"{"scoped_set":[{"name":"delegated","value":1,"visibility":"visible","persistence":"persisted","editors":["client","agent"]}]}"#, true).is_ok());
        for rejected in [
            r#"{"scoped_set":[{"name":"private","value":1,"visibility":"visible","persistence":"ephemeral"}]}"#,
            r#"{"scoped_set":[{"name":"session","value":1,"visibility":"invisible","persistence":"persisted"}]}"#,
            r#"{"set":{"same":1},"scoped_set":[{"name":"same","value":2,"visibility":"invisible","persistence":"persisted"}]}"#,
        ] {
            assert!(decode_change(rejected, true).is_err());
        }
    }
}
