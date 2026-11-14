//! Per-target outcomes stay bounded before any consuming operation starts.
use super::{ToolContext, ToolOutput, arguments::Arguments, output_budget, resolve};
use crate::{
    Error,
    runtime::execution::{ExecutionId, ExecutionRegistry, ExecutionSnapshot},
};
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

const CONCURRENCY: usize = 8;
// Closed enums, u64 counters and no caller/producer text. Unit tests construct
// their widest projection; these bounds must not grow silently with new fields.
const METADATA_BYTES: usize = 768;
const ERROR_BYTES: usize = 256;
const MIN_PAGE_BYTES: usize = 512;

fn compact_diagnostic(diagnostic: crate::observation::diagnostics::ToolDiagnostic) -> Value {
    let mut projection = json!({"code":diagnostic.code,"phase":diagnostic.phase,
        "effects":diagnostic.effects,"operation":"job"});
    if let Some(cause) = diagnostic.reason {
        // Keep the existing per-target envelope reservation. Truncation is an
        // explicit fact; scalar Job status still exposes the complete diagnostic.
        let mut reason = String::new();
        let mut encoded_bytes = 2;
        for character in cause.as_str().chars() {
            let width = serde_json::json!(character.to_string()).to_string().len() - 2;
            if encoded_bytes + width > 96 {
                break;
            }
            reason.push(character);
            encoded_bytes += width;
        }
        let truncated = reason.len() < cause.as_str().len();
        projection["reason"] = json!(reason);
        if truncated {
            projection["reason_truncated"] = json!(true);
        }
    }
    projection
}
fn failure(error: &Error) -> Value {
    compact_diagnostic(error.tool_diagnostic())
}
fn metadata(row: ExecutionSnapshot) -> Value {
    let output = row.output.map(|output| {
        json!({
            "bytes":output.bytes,"start_cursor":output.start_cursor,
            "end_cursor":output.end_cursor,
            "closed":output.closed
        })
    });
    json!({"execution_id":row.execution_id,"status":row.status,
        "capabilities":row.capabilities,"output_availability":row.output_availability,
        "output":output,"failure":row.failure.map(|failure| json!({"kind":failure.kind})),
        "diagnostic":row.diagnostic.map(|diagnostic| json!({"code":diagnostic.code,
            "phase":diagnostic.phase,"effects":diagnostic.effects,"operation":"job"}))})
}
fn row_result(row: ExecutionSnapshot, compact: bool) -> Result<Value, Error> {
    if compact {
        Ok(metadata(row))
    } else {
        Ok(serde_json::to_value(row)?)
    }
}
pub(super) async fn operate<const COMPACT: bool>(
    registry: &Arc<ExecutionRegistry>,
    id: ExecutionId,
    action: &str,
    args: &Value,
    maximum: usize,
    revision: Option<u64>,
    cancellation: CancellationToken,
) -> Result<Value, Error> {
    match action {
        "status" => row_result(registry.get(id)?, COMPACT),
        "read" => Ok(serde_json::to_value(
            registry.read_capped(
                id,
                args.get("cursor").and_then(Value::as_u64).unwrap_or(0),
                maximum,
                args.get("max_bytes")
                    .and_then(Value::as_u64)
                    .map(|value| value as usize),
            )?,
        )?),
        "write" => {
            registry
                .write(
                    id,
                    args["data"].as_str().unwrap_or_default().into(),
                    args.get("eof").and_then(Value::as_bool).unwrap_or(false),
                    cancellation,
                )
                .await?;
            Ok(json!({"accepted":true}))
        }
        "cancel" => {
            registry.cancel(id).await?;
            row_result(registry.get(id)?, COMPACT)
        }
        "release" => {
            registry.release(id, cancellation).await?;
            row_result(registry.get(id)?, COMPACT)
        }
        "wait" => {
            let (revision, row) = registry
                .wait_for_execution_change(id, revision, cancellation)
                .await?;
            Ok(json!({"revision":revision,"execution":row_result(row, COMPACT)?}))
        }
        _ => Err(Error::tool("unsupported execution action")),
    }
}

pub(super) async fn execute(
    parsed: Arguments<'_>,
    context: ToolContext,
) -> Result<ToolOutput, Error> {
    context
        .ensure_admitted(None)
        .map_err(|error| Error::denied_with_private(error.public_reason(), error.to_string()))?;
    let registry = context.state.executions();
    let mut ids = Vec::with_capacity(parsed.ids.len());
    let mut handles = Vec::with_capacity(parsed.ids.len());
    for handle in parsed.ids {
        let id = resolve(&handle, &context.session_id)?;
        if ids.contains(&id) {
            return Err(Error::tool("duplicate execution id in job batch"));
        }
        // Resolve every owner before state mutation. A targeted
        // lookup avoids cloning the full registry once per member.
        registry.get(id)?;
        handles.push(format!("{}/{}", context.session_id, id.0));
        ids.push(id);
    }
    let maximum = output_budget(&context.state.snapshot().limits);
    let skeleton = json!({"action":parsed.action,"results":handles.iter()
        .map(|handle| json!({"id":handle,"ok":true,"result":null})).collect::<Vec<_>>()});
    // success and failure wrapper widths agree: true/result vs false/error.
    let overhead = serde_json::to_vec(&skeleton)?.len() - 4 * ids.len();
    let slot = maximum.saturating_sub(overhead) / ids.len();
    let required = if parsed.action == "read" {
        MIN_PAGE_BYTES.max(ERROR_BYTES)
    } else {
        METADATA_BYTES.max(ERROR_BYTES)
    };
    crate::budget::check(
        crate::LimitKind::ToolOutput,
        overhead.saturating_add(required.saturating_mul(ids.len())),
        maximum,
    )?;
    if context.cancellation.is_cancelled() {
        return Err(Error::Cancelled);
    }
    // Capture every target's baseline together, before the bounded scheduler can
    // delay later waits behind earlier targets. No per-target revision is invented.
    let revisions = if parsed.action == "wait" {
        match parsed.args.get("revision").and_then(Value::as_u64) {
            Some(revision) => vec![revision; ids.len()],
            None => registry.execution_revisions(&ids)?,
        }
    } else {
        vec![0; ids.len()]
    };
    let entries = ids.into_iter().zip(handles).zip(revisions).enumerate();
    let results = async {
        if matches!(parsed.action, "read" | "status") {
            // These futures complete in one poll. In particular, there is no pending
            // await between consuming a page and retaining its deliverable result.
            let mut results = Vec::new();
            for (index, ((id, handle), revision)) in entries {
                let result = operate::<true>(
                    &registry,
                    id,
                    parsed.action,
                    &parsed.args,
                    slot,
                    Some(revision),
                    context.cancellation.clone(),
                )
                .await;
                results.push((index, outcome(handle, result)));
            }
            results
        } else {
            stream::iter(entries.map(|(index, ((id, handle), revision))| {
                let registry = registry.clone();
                let args = &parsed.args;
                let action = parsed.action;
                let cancellation = context.cancellation.clone();
                async move {
                    let result = operate::<true>(
                        &registry,
                        id,
                        action,
                        args,
                        slot,
                        Some(revision),
                        cancellation,
                    )
                    .await;
                    (index, outcome(handle, result))
                }
            }))
            .buffer_unordered(CONCURRENCY)
            .collect::<Vec<(usize, Value)>>()
            .await
        }
    };
    let signal = context
        .observation_interrupt
        .as_ref()
        .filter(|_| parsed.action == "wait");
    let Some(mut results) = super::observe(signal, results).await else {
        return Ok(super::interrupted());
    };
    results.sort_by_key(|(index, _)| *index);
    let result = json!({"action":parsed.action,"results":results.into_iter()
        .map(|(_, value)| value).collect::<Vec<_>>()});
    let text = serde_json::to_string(&result)?;
    // Per-slot bounds were admitted before effects; exceeding this is an
    // implementation invariant violation, never a post-consumption truncation.
    debug_assert!(text.len() <= maximum);
    Ok(ToolOutput::text(text))
}
fn outcome(id: String, result: Result<Value, Error>) -> Value {
    match result {
        Ok(result) => json!({"id":id,"ok":true,"result":result}),
        Err(error) => json!({"id":id,"ok":false,"error":failure(&error)}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{observation::diagnostics::*, runtime::execution::*};
    #[test]
    fn compact_control_bounds_cover_widest_counters_and_safe_diagnostics() {
        let mut row = ExecutionSnapshot::new(
            "s".repeat(65536),
            "c".repeat(65536),
            "t".repeat(65536),
            "a".repeat(65536),
            Some("m".repeat(65536)),
        );
        row.execution_id = ExecutionId(u64::MAX);
        row.status = ExecutionStatus::Interrupted;
        row.output = Some(OutputMetadata {
            channels: vec!["x".repeat(65536)],
            bytes: u64::MAX,
            start_cursor: u64::MAX,
            end_cursor: u64::MAX,
            closed: false,
        });
        row.output_availability = Some(OutputAvailability::Released);
        row.failure = Some(ToolFailure {
            kind: ToolFailureKind::Cancelled,
            code: "x".repeat(128),
            message: "x".repeat(4096),
            retryable: false,
        });
        row.diagnostic = Some(ToolDiagnostic::new(
            DiagnosticCode::PermissionDenied,
            DiagnosticLayer::Runtime,
            DiagnosticPhase::OutputValidation,
        ));
        row.diagnostic.as_mut().unwrap().effects = EffectKnowledge::MayHaveOccurred;
        let projected = metadata(row);
        assert!(
            serde_json::to_vec(&json!({"revision":u64::MAX,"execution":projected}))
                .unwrap()
                .len()
                <= METADATA_BYTES
        );
        assert!(
            serde_json::to_vec(&failure(&Error::Denied("private body".into())))
                .unwrap()
                .len()
                <= ERROR_BYTES
        );
        let page = OutputPage {
            execution_id: ExecutionId(u64::MAX),
            chunks: vec![],
            cursor: u64::MAX,
            next_cursor: u64::MAX,
            end_cursor: u64::MAX,
            start_cursor: u64::MAX,
            closed: false,
            revision: u64::MAX,
        };
        assert!(serde_json::to_vec(&page).unwrap().len() <= MIN_PAGE_BYTES);
    }
}
