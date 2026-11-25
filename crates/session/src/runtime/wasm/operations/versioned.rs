//! The negotiated ABI projects typed Host facts, never JSON output from guest code.
use super::*;
use crate::runtime::wasm::{
    file_versions as refs, microcrab::session0_2_0::file_versions::VersionedRead,
};
use sha2::{Digest, Sha256};

pub(super) fn ledger_error(error: crate::Error) -> ImportFailure<abi::OperationError> {
    let detail = error.diagnostic_reason();
    let code = if matches!(error, crate::Error::Cancelled) {
        abi::OperationErrorCode::Cancelled
    } else {
        abi::OperationErrorCode::InvalidRequest
    };
    let mut out = private_failure(code, &detail);
    let mut safe = diagnostic(&out);
    safe.reason = Some(crate::observation::diagnostics::SafeDiagnosticReason::new(
        &detail,
    ));
    out.message = super::super::encode_diagnostic(&safe);
    out
}

pub(super) fn sha(content: &str) -> String {
    format!("{:x}", Sha256::digest(content.as_bytes()))
}

pub(in crate::runtime::wasm) fn read(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (request,): (abi::ReadRequest,),
) -> wasmtime::Result<(Result<VersionedRead, abi::OperationError>,)> {
    let state = caller.data_mut();
    let result = (|| {
        let name = resource(&request.path)?;
        let req = host::ReadRequest {
            path: PathBuf::from(&name),
            offset_line: number(request.offset_line, usize::MAX, "invalid read line offset")?,
            max_lines: number(request.max_lines, 100_000, "invalid read line bound")?,
            max_bytes: number(
                request.max_bytes,
                state.output_bytes_remaining,
                "invalid read byte bound",
            )?,
        };
        let budget = state.output_bytes_remaining;
        let args =
            serde_json::to_value(&req).map_err(|_| invalid("read request serialization failed"))?;
        bridge(state, vec![], args, move |host, op, invocation| {
            Box::pin(async move {
                let invocation =
                    invocation.ok_or_else(|| invalid("file version invocation absent"))?;
                let owner = refs::owner(&invocation).map_err(ledger_error)?;
                let mut ledger = owner
                    .ledger(host.as_ref(), &invocation)
                    .await
                    .map_err(ledger_error)?;
                let path = refs::path(host.as_ref(), &op, &name)
                    .await
                    .map_err(host_error)?;
                let req = host::ReadRequest {
                    path: PathBuf::from(&path),
                    ..req
                };
                let out = host::tool_api::TextReadApi::read_text(host.as_ref(), &op, req)
                    .await
                    .map_err(host_error)?;
                let size = out.content.len().saturating_add(160);
                if size > budget {
                    return Err(failure(
                        abi::OperationErrorCode::Limit,
                        "Host read exceeds remaining output budget",
                    ));
                }
                let reference = ledger
                    .issue(&path, &out.version, true)
                    .map_err(ledger_error)?;
                ledger.commit().await.map_err(ledger_error)?;
                Ok((
                    VersionedRead {
                        reference,
                        page: abi::ReadOutput {
                            content: out.content,
                            start_line: out.start_line as u64,
                            next_line: out.next_line.map(|i| i as u64),
                            truncated: out.truncated,
                            version: out.version,
                        },
                    },
                    size,
                ))
            })
        })
    })();
    finish(state, result)
}

fn retain(progress: &crate::runtime::execution::ExecutionProgress, value: &Value) {
    if let Err(error) = progress.retain_mutation_receipt(&value.to_string()) {
        tracing::error!(reason=%error.diagnostic_reason(), "Reserved Host write receipt could not be retained");
    }
}

pub(in crate::runtime::wasm) fn write(
    mut caller: StoreContextMut<'_, RuntimeState>,
    (request,): (abi::WriteRequest,),
) -> wasmtime::Result<(Result<abi::WriteOutput, abi::OperationError>,)> {
    let state = caller.data_mut();
    let mut committed = false;
    let mut uncertain = false;
    let result = (|| {
        let name = resource(&request.path)?;
        if state.output_bytes_remaining < 128 {
            return Err(failure(
                abi::OperationErrorCode::Limit,
                "insufficient file write receipt capacity",
            ));
        }
        let precondition = match request.precondition {
            abi::WritePrecondition::Missing => host::WritePrecondition::Missing,
            abi::WritePrecondition::Version(v) => host::WritePrecondition::Version(v),
        };
        let content = request.content;
        let args = json!({"path":name,"content":content,"precondition":precondition,"_file_mutation":true});
        let result = bridge(state, vec![], args, move |host, op, invocation| {
            Box::pin(async move {
                let invocation =
                    invocation.ok_or_else(|| invalid("file version invocation absent"))?;
                let owner = refs::owner(&invocation).map_err(ledger_error)?;
                let mut ledger = owner
                    .ledger(host.as_ref(), &invocation)
                    .await
                    .map_err(ledger_error)?;
                let path = refs::path(host.as_ref(), &op, &name)
                    .await
                    .map_err(host_error)?;
                let progress = invocation.execution_progress.as_ref().ok_or_else(|| {
                    invalid("file version write requires a retained execution receipt")
                })?;
                let receipt_bound = path.len() * 6 + 512;
                progress
                    .reserve_mutation_receipt(receipt_bound)
                    .map_err(ledger_error)?;
                let planned = sha(&content);
                let alias = ledger.issue(&path, &planned, false).map_err(ledger_error)?;
                ledger.commit().await.map_err(ledger_error)?;
                let request = host::WriteRequest {
                    path: PathBuf::from(&path),
                    content,
                    precondition,
                };
                let out = host::tool_api::TextWriteApi::write(host.as_ref(), &op, request).await;
                let out = match out {
                    Ok(out) => out,
                    Err(error) => {
                        let error = host_error(error);
                        let uncertain = matches!(
                            error.code,
                            abi::OperationErrorCode::Io
                                | abi::OperationErrorCode::Cancelled
                                | abi::OperationErrorCode::Timeout
                        );
                        let status = if uncertain { "uncertain" } else { "failed" };
                        let content = json!({"source":"host_file_mutation","receipts":[{"operation":1,"paths":[path],"status":status}],"replay_safe":false});
                        retain(progress, &content);
                        if uncertain {
                            let mut safe = diagnostic(&error);
                            safe.effects =
                                crate::observation::diagnostics::EffectKnowledge::MayHaveOccurred;
                            safe.retryable = Some(false);
                            let mut error = error;
                            error.message = super::super::encode_diagnostic(&safe);
                            return Ok((Err(error), 128));
                        }
                        return Err(error);
                    }
                };
                let mut retained = json!({"source":"host_file_mutation","receipts":[{"operation":1,"paths":[path],"status":"committed","outcome":{"version":out.version,"bytes":out.bytes},"version_ref":null}],"replay_safe":false});
                retain(progress, &retained);
                let projection = async {
                    ledger.confirm(&path, &out.version, &alias)?;
                    ledger.commit().await?;
                    Ok::<_, crate::Error>(abi::WriteOutput {
                        version: alias,
                        bytes: out.bytes as u64,
                    })
                }
                .await;
                if let Ok(projected) = &projection {
                    retained["receipts"][0]["version_ref"] = json!(projected.version);
                    retain(progress, &retained);
                }
                Ok((Ok(projection), 128))
            })
        })?;
        match result {
            Ok(projected) => {
                committed = true;
                projected.map_err(|_| failure(abi::OperationErrorCode::Io, "write committed; version reference publication failed; reread the file and do not replay the write"))
            }
            Err(error) => {
                uncertain = true;
                Err(error)
            }
        }
    })();
    if committed {
        state.known_write_committed = true;
    }
    if (committed || uncertain) && state.patch_receipts.is_empty() {
        // Preserve existing run/cleanup behavior when the guest waiter is cancelled.
        state
            .patch_receipts
            .push(json!({"status": if committed { "committed" } else { "uncertain" }}));
    }
    finish(state, result)
}
