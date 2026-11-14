//! Invocation-bound file mutation plan. Reservations happen before the first
//! capture; publication awaits the actual Host owner even after cancellation.
use super::super::microcrab::host::file_mutation as wire;
use super::super::microcrab::host::text_write as write;
use super::*;

fn decode(
    request: wire::FileMutation,
) -> Result<host::FileMutation, ImportFailure<abi::OperationError>> {
    Ok(match request {
        wire::FileMutation::Write(r) => host::FileMutation::Write(host::WriteRequest {
            path: resource(&r.path)?.into(),
            content: r.content,
            precondition: match r.precondition {
                write::WritePrecondition::Missing => host::WritePrecondition::Missing,
                write::WritePrecondition::Version(v) => host::WritePrecondition::Version(v),
            },
        }),
        wire::FileMutation::Delete(r) => host::FileMutation::Delete(host::DeleteRequest {
            path: resource(&r.path)?.into(),
            expected_version: r.expected_version,
        }),
        wire::FileMutation::Move(r) => host::FileMutation::Move(host::MoveRequest {
            path: resource(&r.path)?.into(),
            destination: resource(&r.destination)?.into(),
            expected_version: r.expected_version,
            content: r.content,
        }),
    })
}
fn encode(request: host::FileMutation) -> wire::FileMutation {
    match request {
        host::FileMutation::Write(r) => wire::FileMutation::Write(write::WriteRequest {
            path: r.path.to_string_lossy().into(),
            content: r.content,
            precondition: match r.precondition {
                host::WritePrecondition::Missing => write::WritePrecondition::Missing,
                host::WritePrecondition::Version(v) => write::WritePrecondition::Version(v),
            },
        }),
        host::FileMutation::Delete(r) => wire::FileMutation::Delete(wire::DeleteRequest {
            path: r.path.to_string_lossy().into(),
            expected_version: r.expected_version,
        }),
        host::FileMutation::Move(r) => wire::FileMutation::Move(wire::MoveRequest {
            path: r.path.to_string_lossy().into(),
            destination: r.destination.to_string_lossy().into(),
            expected_version: r.expected_version,
            content: r.content,
        }),
    }
}
fn paths(request: &host::FileMutation) -> Vec<PathBuf> {
    match request {
        host::FileMutation::Write(r) => vec![r.path.clone()],
        host::FileMutation::Delete(r) => vec![r.path.clone()],
        host::FileMutation::Move(r) => vec![r.path.clone(), r.destination.clone()],
    }
}

fn receipt_bound(requests: &[host::FileMutation]) -> usize {
    requests
        .iter()
        .flat_map(paths)
        .map(|path| path.to_string_lossy().len() * 6 + 256)
        .sum::<usize>()
        + requests.len() * 16_384
        + 256
}

fn invalid_preflight(index: usize, detail: &str) -> ImportFailure<abi::OperationError> {
    private_failure(
        abi::OperationErrorCode::InvalidRequest,
        &format!(
            "invalid Host file-mutation preflight response at operation {}: {detail}",
            index + 1
        ),
    )
}

/// Paths are the Host's authority. Compare their Host-supplied canonical resource
/// identities, while every non-path field must remain byte-for-byte unchanged.
/// This detects reordering even when two operations have identical payloads.
async fn validate_preflight(
    host: &dyn host::Host,
    token: &host::OpContext,
    original: &[host::FileMutation],
    normalized: &[host::FileMutation],
) -> Result<(), ImportFailure<abi::OperationError>> {
    if original.len() != normalized.len() {
        return Err(private_failure(
            abi::OperationErrorCode::InvalidRequest,
            &format!(
                "invalid Host file-mutation preflight response: operation count changed from {} to {}",
                original.len(),
                normalized.len()
            ),
        ));
    }
    for (index, (before, after)) in original.iter().zip(normalized).enumerate() {
        let changed = match (before, after) {
            (host::FileMutation::Write(before), host::FileMutation::Write(after)) => {
                if before.content != after.content {
                    Some("write content changed")
                } else if before.precondition != after.precondition {
                    Some("write precondition changed")
                } else {
                    None
                }
            }
            (host::FileMutation::Delete(before), host::FileMutation::Delete(after)) => {
                (before.expected_version != after.expected_version)
                    .then_some("delete source version changed")
            }
            (host::FileMutation::Move(before), host::FileMutation::Move(after)) => {
                if before.content != after.content {
                    Some("move destination content changed")
                } else if before.expected_version != after.expected_version {
                    Some("move source version changed")
                } else {
                    None
                }
            }
            _ => Some("operation kind or order changed"),
        };
        if let Some(detail) = changed {
            return Err(invalid_preflight(index, detail));
        }
        for (before, after) in paths(before).into_iter().zip(paths(after)) {
            let returned = after
                .to_str()
                .ok_or_else(|| invalid_preflight(index, "normalized path is not UTF-8"))?;
            resource(returned).map_err(|_| {
                invalid_preflight(index, "normalized path exceeds the bounded path contract")
            })?;
            let before = host::tool_api::ResourceApi::normalize_resource(host, token, before)
                .await
                .map_err(host_error)?;
            let after = host::tool_api::ResourceApi::normalize_resource(host, token, after)
                .await
                .map_err(host_error)?;
            if before != after {
                return Err(invalid_preflight(
                    index,
                    "source/destination resource identity or operation order changed",
                ));
            }
        }
    }
    Ok(())
}

pub(in crate::runtime::wasm) fn preflight(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (requests,): (Vec<wire::FileMutation>,),
) -> wasmtime::Result<(Result<Vec<wire::FileMutation>, abi::OperationError>,)> {
    let state = caller.data_mut();
    let result = (|| {
        if state.patch_plan.is_some() {
            return Err(invalid(
                "one file-mutation preflight is allowed per invocation",
            ));
        }
        if requests.is_empty() || requests.len() > 32 {
            return Err(invalid(
                "file-mutation preflight requires 1..=32 operations",
            ));
        }
        state
            .tool_context
            .as_ref()
            .ok_or_else(|| invalid("file-mutation requires an admitted tool invocation"))?;
        let requests: Vec<_> = requests.into_iter().map(decode).collect::<Result<_, _>>()?;
        let args = serde_json::to_value(&requests)
            .map_err(|_| invalid("mutation request serialization failed"))?;
        let path_count = requests.iter().map(|r| paths(r).len()).sum::<usize>();
        let operation_count = requests.len();
        // Each operation has at most two bounded paths and one bounded diagnostic.
        // Reserve both import outcomes and the complete JSON receipt before effects.
        let receipt_bytes = receipt_bound(&requests);
        let required_calls = operation_count + path_count * 2 + 1;
        if required_calls > usize::from(state.operation_calls_remaining)
            || receipt_bytes > state.output_bytes_remaining
        {
            return Err(failure(
                abi::OperationErrorCode::Limit,
                "whole patch operation/output reservation exceeds remaining invocation limits",
            ));
        }
        let input_reserve = serde_json::to_vec(&args)
            .map_err(|_| invalid("mutation request serialization failed"))?
            .len();
        if input_reserve.saturating_mul(2) > state.input_bytes_remaining {
            return Err(failure(
                abi::OperationErrorCode::Limit,
                "whole patch input reservation exceeds remaining invocation limits",
            ));
        }
        let output_budget = state.output_bytes_remaining;
        let (normalized, evidence_bytes, points, active, receipt_bytes) = bridge(
            state,
            Vec::new(),
            args,
            move |host, token, invocation| {
                Box::pin(async move {
                    if !host
                        .tool_capabilities()
                        .iter()
                        .any(|c| c == "file_mutation")
                    {
                        return Err(failure(
                            abi::OperationErrorCode::Unsupported,
                            "Host does not support conditional file mutations",
                        ));
                    }
                    let normalized = host::tool_api::FileMutationApi::preflight_mutations(
                        host.as_ref(),
                        &token,
                        requests.clone(),
                    )
                    .await
                    .map_err(host_error)?;
                    validate_preflight(host.as_ref(), &token, &requests, &normalized).await?;
                    let invocation =
                        invocation.ok_or_else(|| invalid("mutation invocation is absent"))?;
                    let response_size = serde_json::to_vec(&normalized)
                        .map_err(|_| invalid("normalized mutation serialization failed"))?
                        .len();
                    let receipt_bytes = receipt_bound(&normalized);
                    if receipt_bytes > output_budget.saturating_sub(response_size) {
                        return Err(failure(
                            abi::OperationErrorCode::Limit,
                            "normalized Host patch response leaves insufficient receipt capacity",
                        ));
                    }
                    if let Some(progress) = &invocation.execution_progress {
                        progress.reserve_mutation_receipt(receipt_bytes).map_err(|_|failure(abi::OperationErrorCode::Limit,"normalized mutation receipt exceeds execution inspection capacity"))?;
                    }
                    let mut evidence_bytes = 0usize;
                    let mut points = 0usize;
                    let mut active = Vec::new();
                    for request in &normalized {
                        let mut noop = false;
                        let request_paths = paths(request);
                        let content = match request {
                            host::FileMutation::Write(r) => Some(&r.content),
                            host::FileMutation::Move(r) => Some(&r.content),
                            _ => None,
                        };
                        for (index, path) in request_paths.iter().enumerate() {
                            let source_exists = match request {
                                host::FileMutation::Write(r) => {
                                    !matches!(r.precondition, host::WritePrecondition::Missing)
                                }
                                _ => index == 0,
                            };
                            let before = if source_exists {
                                let read = host::tool_api::TextReadApi::read_text(
                                    host.as_ref(),
                                    &token,
                                    host::ReadRequest {
                                        path: path.clone(),
                                        offset_line: 1,
                                        max_lines: 10000,
                                        max_bytes: 1_048_576,
                                    },
                                )
                                .await
                                .map_err(host_error)?;
                                if read.truncated
                                    || read.next_line.is_some()
                                    || read.start_line != 1
                                {
                                    return Err(invalid(
                                        "patch evidence requires complete bounded UTF-8 sources",
                                    ));
                                }
                                if matches!(request, host::FileMutation::Write(_))
                                    && content.is_some_and(|content| *content == read.content)
                                {
                                    noop = true;
                                }
                                read.content.len()
                            } else {
                                0
                            };
                            let after = match request {
                                host::FileMutation::Delete(_) => 0,
                                host::FileMutation::Move(_) if index == 0 => before,
                                _ => content.map_or(0, String::len),
                            };
                            // Metadata includes hash, canonical path, invocation and tool.
                            evidence_bytes = evidence_bytes
                                .saturating_add(before)
                                .saturating_add(after)
                                .saturating_add(
                                    2 * (path.to_string_lossy().len()
                                        + invocation.call_id.len()
                                        + invocation.tool.len()
                                        + 64),
                                );
                            points += 2;
                        }
                        if !noop {
                            active.push(serde_json::to_value(request).map_err(|_| {
                                invalid("normalized mutation serialization failed")
                            })?);
                        }
                    }
                    Ok((
                        (normalized, evidence_bytes, points, active, receipt_bytes),
                        response_size,
                    ))
                })
            },
        )?;
        let remaining_input = normalized.iter().try_fold(0usize, |sum, request| {
            let mutation = serde_json::to_vec(&json!({"request":request,"_file_mutation":true}))
                .map_err(|_| invalid("mutation request serialization failed"))?
                .len();
            let captures = paths(request).into_iter().try_fold(0usize, |sum, path| {
                let bytes = serde_json::to_vec(
                    &json!({"path":path,"max_bytes":1_048_576,"_cache_point":true}),
                )
                .map_err(|_| invalid("capture request serialization failed"))?
                .len();
                Ok::<usize, ImportFailure<abi::OperationError>>(sum.saturating_add(bytes * 2))
            })?;
            Ok::<usize, ImportFailure<abi::OperationError>>(
                sum.saturating_add(mutation).saturating_add(captures),
            )
        })?;
        if remaining_input > state.input_bytes_remaining {
            return Err(failure(
                abi::OperationErrorCode::Limit,
                "normalized whole patch input exceeds remaining invocation limits",
            ));
        }
        let cache_points_remaining =
            u8::try_from(path_count * 2).map_err(|_| invalid("patch evidence count overflow"))?;
        let patch_receipts=normalized.iter().enumerate().map(|(index,request)| {
            let value=serde_json::to_value(request).map_err(|_|invalid("normalized mutation serialization failed"))?;
            Ok(json!({"operation":index+1,"paths":paths(request).into_iter().map(|path|path.to_string_lossy().into_owned()).collect::<Vec<_>>(),"status":if active.contains(&value){"not_started"}else{"unchanged"}}))
        }).collect::<Result<_,ImportFailure<abi::OperationError>>>()?;
        // Receipt space is kept out of subsequent import output consumption.
        if receipt_bytes > state.output_bytes_remaining {
            return Err(failure(
                abi::OperationErrorCode::Limit,
                "normalized patch response leaves insufficient receipt capacity",
            ));
        }
        let output = normalized.into_iter().map(encode).collect();
        let invocation = state
            .tool_context
            .as_ref()
            .ok_or_else(|| invalid("mutation invocation is absent"))?;
        // This collector-owned reservation is the last fallible step. No candidate
        // plan is exposed before it succeeds, and nothing can fail between its
        // success and publication below. InvocationScope owns normal release.
        invocation
            .reserve_cache_points(points, evidence_bytes)
            .map_err(|_| {
                failure(
                    abi::OperationErrorCode::Limit,
                    "whole patch selected evidence exceeds remaining round capacity",
                )
            })?;
        state.output_bytes_remaining -= receipt_bytes;
        state.cache_points_remaining = cache_points_remaining;
        state.patch_receipts = patch_receipts;
        state.patch_plan = Some(active);
        state.cooperative_driver_cancellation = true;
        Ok(output)
    })();
    finish(state, result)
}

pub(in crate::runtime::wasm) fn mutate(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (request,): (wire::FileMutation,),
) -> wasmtime::Result<(Result<wire::MutationOutput, abi::OperationError>,)> {
    let state = caller.data_mut();
    let mut receipt_index = None;
    let result = (|| {
        let request = decode(request)?;
        let value = serde_json::to_value(&request)
            .map_err(|_| invalid("mutation request serialization failed"))?;
        let plan = state
            .patch_plan
            .as_mut()
            .ok_or_else(|| invalid("mutation requires whole-call preflight"))?;
        let Some(index) = plan.iter().position(|expected| *expected == value) else {
            return Err(invalid(
                "mutation was not part of the validated whole-call patch",
            ));
        };
        // Earlier unchanged operations may be skipped. Reordering real mutations
        // is rejected by consuming only a prefix whose remaining entries are no-ops.
        if index != 0 {
            return Err(invalid(
                "mutation does not match the next preflight operation",
            ));
        }
        plan.remove(0);
        receipt_index = state
            .patch_receipts
            .iter()
            .position(|receipt| receipt["status"] == "not_started");
        let resources = paths(&request)
            .into_iter()
            .map(|p| p.to_string_lossy().into())
            .collect();
        let args = json!({"request":value,"_file_mutation":true});
        bridge(state, resources, args, move |host, token, _| {
            Box::pin(async move {
                let out = host::tool_api::FileMutationApi::mutate(host.as_ref(), &token, request)
                    .await
                    .map_err(host_error)?;
                // Capacity was reserved before publication. A violating Host diagnostic
                // is truncated without losing the committed outcome.
                let diagnostic = out.diagnostic.map(|s| s.chars().take(256).collect());
                Ok((
                    wire::MutationOutput {
                        version: out.version,
                        bytes: out.bytes as u64,
                        source_removed: out.source_removed,
                        destination_written: out.destination_written,
                        diagnostic,
                    },
                    0,
                ))
            })
        })
    })();
    if let Some(index) = receipt_index {
        let receipt = &mut state.patch_receipts[index];
        match &result {
            Ok(out) => {
                receipt["status"] = json!("committed");
                receipt["outcome"] = json!({"version":out.version,"bytes":out.bytes,"source_removed":out.source_removed,"destination_written":out.destination_written,"diagnostic":out.diagnostic});
            }
            Err(error) => {
                receipt["status"] = json!(match error.code {
                    abi::OperationErrorCode::Conflict
                    | abi::OperationErrorCode::Denied
                    | abi::OperationErrorCode::InvalidRequest
                    | abi::OperationErrorCode::Unsupported
                    | abi::OperationErrorCode::Limit => "failed",
                    _ => "uncertain",
                });
                receipt["error"] = json!({"code":format!("{:?}",error.code),"message":error.message.chars().take(1024).collect::<String>()});
            }
        }
        if let Some(progress) = state
            .tool_context
            .as_ref()
            .and_then(|context| context.execution_progress.as_ref())
        {
            let content=json!({"source":"host_file_mutation","receipts":state.patch_receipts,"replay_safe":false}).to_string();
            // The preflight reserved this bounded retained envelope. Retention is
            // synchronous and independent of guest cancellation/fuel/linear memory.
            if let Err(error) = progress.retain_mutation_receipt(&content) {
                tracing::error!(reason=%error.diagnostic_reason(),"Reserved Host mutation receipt could not be retained");
            }
        }
    }
    if receipt_index.is_some() && result.as_ref().map_or(true, |out| out.diagnostic.is_some()) {
        if let Some(plan) = state.patch_plan.as_mut() {
            plan.clear();
        }
    }
    if result
        .as_ref()
        .is_ok_and(|out| out.source_removed || out.destination_written)
    {
        state.known_write_committed = true;
    }
    finish(state, result)
}
