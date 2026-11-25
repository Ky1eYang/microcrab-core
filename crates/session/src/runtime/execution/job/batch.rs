//! Per-target outcomes stay bounded before any consuming operation starts.
use super::{
    ToolContext, ToolOutput,
    arguments::Arguments,
    output_budget, resolve,
    result::{Batch, Diagnostic, Outcome, ResultBody, Row, Target},
};
use crate::{
    Error,
    runtime::execution::{ExecutionId, ExecutionRegistry},
};
use futures_util::{StreamExt, stream};
use serde_json::Value;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

const CONCURRENCY: usize = 8;
// Closed enums, u64 counters and no caller/producer text. Unit tests construct
// their widest projection; these bounds must not grow silently with new fields.
const METADATA_BYTES: usize = 768;
const ERROR_BYTES: usize = 256;
const MIN_TEXT_PAGE_BYTES: usize = 512;
// A private page never has chunks, but retains the bounded MIME/cursor metadata.
const MIN_PRIVATE_PAGE_BYTES: usize = 576;

pub(super) async fn operate<const COMPACT: bool>(
    registry: &Arc<ExecutionRegistry>,
    id: ExecutionId,
    action: &str,
    args: &Value,
    maximum: usize,
    revision: Option<u64>,
    cancellation: CancellationToken,
) -> Result<ResultBody, Error> {
    match action {
        "status" => Ok(ResultBody::Execution(Row::new(registry.get(id)?, COMPACT))),
        "read" => Ok(ResultBody::Read(
            registry.read_capped(
                id,
                args.get("cursor").and_then(Value::as_u64).unwrap_or(0),
                maximum,
                args.get("max_bytes")
                    .and_then(Value::as_u64)
                    .map(|value| value as usize),
            )?,
        )),
        "write" => {
            registry
                .write(
                    id,
                    args["data"].as_str().unwrap_or_default().into(),
                    args.get("eof").and_then(Value::as_bool).unwrap_or(false),
                    cancellation,
                )
                .await?;
            Ok(ResultBody::Write { accepted: true })
        }
        "cancel" => {
            registry.cancel(id).await?;
            Ok(ResultBody::Execution(Row::new(registry.get(id)?, COMPACT)))
        }
        "release" => {
            registry.release(id, cancellation).await?;
            Ok(ResultBody::Execution(Row::new(registry.get(id)?, COMPACT)))
        }
        "wait" => {
            let (revision, row) = registry
                .wait_for_execution_change(id, revision, cancellation)
                .await?;
            Ok(ResultBody::Wait {
                revision,
                execution: Row::new(row, COMPACT),
            })
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
    let mut private_page = false;
    for handle in parsed.ids {
        let id = resolve(&handle, &context.session_id)?;
        if ids.contains(&id) {
            return Err(Error::tool("duplicate execution id in job batch"));
        }
        // Resolve every owner before state mutation. A targeted
        // lookup avoids cloning the full registry once per member.
        let row = registry.get(id)?;
        private_page |= row
            .output
            .is_some_and(|output| output.private_stdout.is_some());
        handles.push(format!("{}/{}", context.session_id, id.0));
        ids.push(id);
    }
    let maximum = output_budget(&context.state.snapshot().limits);
    let skeleton = Batch {
        action: parsed.action,
        results: handles
            .iter()
            .map(|id| Target {
                id: id.clone(),
                ok: true,
                outcome: Outcome::Result(()),
            })
            .collect(),
    };
    // success and failure wrapper widths agree: true/result vs false/error.
    let overhead = serde_json::to_vec(&skeleton)?.len() - 4 * ids.len();
    let slot = maximum.saturating_sub(overhead) / ids.len();
    let required = if parsed.action == "read" {
        // Targets keep equal slots. A mixed batch needs each slot to be large
        // enough for the largest envelope; the whole-call cap does not increase.
        let minimum = if private_page {
            MIN_PRIVATE_PAGE_BYTES
        } else {
            MIN_TEXT_PAGE_BYTES
        };
        minimum.max(ERROR_BYTES)
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
    if parsed.action == "write" {
        let redaction = context.redaction_handle();
        for id in &ids {
            registry.merge_redaction(*id, &redaction)?;
        }
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
            .collect::<Vec<_>>()
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
    let result = Batch {
        action: parsed.action,
        results: results.into_iter().map(|(_, value)| value).collect(),
    };
    let text = serde_json::to_string(&result)?;
    // Per-slot bounds were admitted before effects; exceeding this is an
    // implementation invariant violation, never a post-consumption truncation.
    debug_assert!(text.len() <= maximum);
    Ok(ToolOutput::text(text))
}
fn outcome(id: String, result: Result<ResultBody, Error>) -> Target<ResultBody> {
    let ok = result.is_ok();
    Target {
        id,
        ok,
        outcome: match result {
            Ok(result) => Outcome::Result(result),
            Err(error) => Outcome::Error(Diagnostic::new(error.tool_diagnostic(), true)),
        },
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
            private_stdout: Some(PrivateOutputMetadata {
                kind: PrivateOutputKind::Bytes,
                channel: "stdout".into(),
                media_type: format!("application/{}", "x".repeat(115)),
                start_cursor: u64::MAX,
                end_cursor: u64::MAX,
                closed: false,
                truncated: false,
                available: false,
            }),
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
        let projected = ResultBody::Wait {
            revision: u64::MAX,
            execution: Row::new(row, true),
        };
        assert!(serde_json::to_vec(&projected).unwrap().len() <= METADATA_BYTES);
        assert!(
            serde_json::to_vec(&Diagnostic::new(
                Error::Denied("private body".into()).tool_diagnostic(),
                true
            ))
            .unwrap()
            .len()
                <= ERROR_BYTES
        );
        let mut page = OutputPage {
            execution_id: ExecutionId(u64::MAX),
            chunks: vec![],
            cursor: u64::MAX,
            next_cursor: u64::MAX,
            end_cursor: u64::MAX,
            start_cursor: u64::MAX,
            closed: false,
            revision: u64::MAX,
            private_stdout: Some(PrivateOutputMetadata {
                kind: PrivateOutputKind::Bytes,
                channel: "stdout".into(),
                media_type: format!("application/{}", "x".repeat(115)),
                start_cursor: u64::MAX,
                end_cursor: u64::MAX,
                closed: false,
                truncated: false,
                available: false,
            }),
        };
        let length = serde_json::to_vec(&page).unwrap().len();
        assert!(length > MIN_TEXT_PAGE_BYTES);
        assert!(length <= MIN_PRIVATE_PAGE_BYTES);
        page.private_stdout = None;
        assert!(serde_json::to_vec(&page).unwrap().len() <= MIN_TEXT_PAGE_BYTES);
    }
}
