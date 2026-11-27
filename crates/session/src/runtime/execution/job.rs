//! One control invocation may address a bounded set of existing execution owners.
use crate::runtime::tool::{Tool, ToolContext, ToolOutput};
use crate::{Error, runtime::execution::ExecutionId};
use async_trait::async_trait;
use microcrab_protocol::ToolDefinition;
use serde_json::{Value, json};
mod arguments;
mod batch;
#[cfg(test)]
mod batch_tests;
#[cfg(test)]
mod max_bytes_tests;
mod redirect;
mod result;
pub(crate) use arguments::{canonical, is_redirect, validate};
pub(crate) const REDIRECT: &str = "redirect";

pub(crate) fn bind_redirect_destination(
    arguments: &Value,
    context: &mut ToolContext,
) -> Result<(), Error> {
    if is_redirect(arguments) {
        let parsed = validate(arguments)?;
        let destination = parsed
            .redirect
            .ok_or_else(|| Error::tool("missing Job redirect destination"))?;
        resolve(parsed.ids[0].as_ref(), &context.session_id)?;
        context.redirect_destination = Some(
            context
                .state
                .prepare_library_redirect(destination.expected_mime, destination.title)?,
        );
    }
    Ok(())
}
#[derive(Debug, Default, Clone, Copy)]
pub struct Job;
impl Job {
    /// The sole current descriptor. Old Session catalogs are unsupported rather
    /// than silently upgraded inside their frozen Context epoch.
    pub fn definition() -> ToolDefinition {
        ToolDefinition {
            name: "job".into(),
            description: "Control existing buffered executions by one handle or 1..16 unique same-Session handles; never starts work. Batches share one output budget. Handles and aggregate output capacity are checked before dispatch; later per-target failures do not undo successful siblings. Cancellation may leave partial effects: inspect original handles before retrying.".into(),
            parameters: json!({
                "type":"object", "additionalProperties":false,
                "required":["id","action"],
                "properties":{
                    "id":{
                        "oneOf":[{"type":"string","minLength":1},{"type":"array","minItems":1,"maxItems":16,"uniqueItems":true,"items":{"type":"string","minLength":1,"description":"One exact execution handle in the batch."}}],
                        "description":"Exact session_id/execution_id handle or batch. Prefer an actual JSON array; a string containing one complete JSON array is decoded once. No PID, tool name, child Session handle, nested encoding or mixed types; null is invalid. Scalar returns one result; batch returns ordered {action,results:[{id,ok,result|error}]} with at most 8 asynchronous targets in flight. Batch metadata is compact; scalar status gives full metadata."
                    },
                    "action":{
                        "type":"string", "enum":["status","read","wait","write","cancel","release", REDIRECT],
                        "description":"status reports metadata: terminal means production ended, not all output delivered. read is nonconsuming and never frees buffer capacity. After collecting needed output, explicitly release a terminal execution; release discards retained output only after cleanup succeeds. wait returns changed metadata, not output; user steer may return {action:wait,interrupted:true,reason:user_steer,targets_cancelled:false}, leaving targets live. write sends text stdin; cancel requests stop, confirmed by status; release frees terminal resources and discards remaining output. redirect saves complete successful captured bytes into the Personal Content Library and mounts a ref_ ID in this Session, never reads private bytes into Job results, and does not release its source. Inspect the exact redirect execution and state.references metadata if its receipt is lost; do not replay blindly. No default; null is invalid."
                    },
                    "args":{
                        "type":"object", "additionalProperties":false,
                        "description":"Arguments shared by all targets for one action. Read accepts cursor/max_bytes; wait only revision; write requires data and accepts eof; redirect requires literal channel and expected_mime and accepts optional title. Other actions accept no fields. Omission means {}; null is invalid.",
                        "properties":{
                            "cursor":{"type":"integer","minimum":0,"description":"Read-only absolute UTF-8 byte boundary, applied per target. Omission starts at zero; null is invalid. Continue from next_cursor until closed=true and next_cursor=end_cursor. start_cursor marks the earliest retained byte; older requests clamp to it after overwrite. Inline [upstream lost] marks upstream capture loss at an observed batch boundary. Read all pages before explicit terminal release."},
                            "max_bytes":{"type":"integer","minimum":1,"maximum":crate::runtime::execution::buffer::MAX_PAGE_BYTES,"description":"Read UTF-8 content byte cap per target, excluding JSON envelopes. Omission adds no content cap. Read takes a forward prefix from cursor; UTF-8 boundaries and the shared serialized whole-call budget may reduce pages. Continue with next_cursor; reads never consume or release. null is invalid."},
                            "revision":{"type":"integer","minimum":0,"description":"Wait-only observed revision, applied per target. Omission captures each target's initial revision; null is invalid. Batch wait waits for every target to change or become terminal."},
                            "data":{"type":"string","description":"Write-only input text broadcast to all targets; empty is allowed with eof. Omission is invalid for write; null is invalid."},
                            "eof":{"type":"boolean","description":"Write-only flag to close input after writing, broadcast to all targets. Omission defaults to false; null is invalid."},
                            "channel":{"const":"stdout","description":"Redirect-only exact private channel."},
                            "expected_mime":{"type":"string","minLength":3,"maxLength":127,"description":"Redirect-only exact MIME declared at capture: lowercase type/subtype, no parameters or wildcards (e.g. application/octet-stream or image/png). The consumer validates the byte format."},
                            "title":{"type":"string","minLength":1,"maxLength":256,"description":"Redirect-only optional literal title for the created Personal Content Library entry; defaults to 'job stdout'."}
                        }
                    }
                },
                "if":{"properties":{"action":{"const":REDIRECT}},"required":["action"]},
                "then":{"properties":{"id":{"type":"string"},"args":{"required":["channel","expected_mime"]}},"required":["args"]}
            }),
        }
    }
}
/// Reject an incompatible frozen Job contract without changing the Context.
pub(crate) fn require_current_definition(definitions: &[ToolDefinition]) -> Result<(), Error> {
    if let Some(frozen) = definitions
        .iter()
        .find(|definition| definition.name == "job")
        && frozen != &Job::definition()
    {
        return Err(Error::config(
            "restored job catalog is unsupported; create a new Session with current tool definitions",
        ));
    }
    Ok(())
}

pub(crate) fn uncertain_redirect(operation: ExecutionId) -> Error {
    use crate::observation::diagnostics::{
        DiagnosticCode, DiagnosticLayer, DiagnosticOperation, DiagnosticPhase, EffectKnowledge,
        SafeDiagnosticReason, ToolDiagnostic,
    };
    let mut diagnostic = ToolDiagnostic::new(
        DiagnosticCode::Io,
        DiagnosticLayer::Runtime,
        DiagnosticPhase::OutputValidation,
    );
    diagnostic.operation = Some(DiagnosticOperation::Job);
    diagnostic.effects = EffectKnowledge::MayHaveOccurred;
    diagnostic.operation_effects = Some(EffectKnowledge::MayHaveOccurred);
    diagnostic.retryable = Some(false);
    diagnostic.reason = Some(SafeDiagnosticReason::new(&format!(
        "Job redirect acknowledgement unavailable; inspect execution {} and Bytes variable metadata in this runtime instance; do not replay",
        operation.0
    )));
    Error::Diagnostic(diagnostic)
}

// A batch shares this budget; per-target content caps never multiply it.
pub(crate) fn output_budget(limits: &crate::Limits) -> usize {
    limits.max_tool_output_bytes.min(
        limits
            .output_buffer_bytes
            .saturating_mul(6)
            .saturating_add(16 * 1024),
    )
}
fn resolve(id: &str, session_id: &str) -> Result<ExecutionId, Error> {
    let (session, number) = arguments::parse_handle(id)?;
    if session != session_id {
        return Err(Error::denied("execution belongs to another Session"));
    }
    Ok(ExecutionId(number))
}
pub(super) async fn observe<F: std::future::Future>(
    signal: Option<&tokio_util::sync::CancellationToken>,
    operation: F,
) -> Option<F::Output> {
    tokio::select! { biased;
        result = operation => Some(result),
        _ = async {
            match signal {
                Some(signal) => signal.cancelled().await,
                None => std::future::pending().await,
            }
        } => None,
    }
}
pub(super) fn interrupted() -> ToolOutput {
    ToolOutput::text(
        serde_json::to_string(&result::WaitInterrupted {
            action: "wait",
            interrupted: true,
            reason: result::Interruption::UserSteer,
            targets_cancelled: false,
        })
        .expect("wait interruption has only fixed strings and booleans"),
    )
}
#[async_trait]
impl Tool for Job {
    fn definition(&self) -> ToolDefinition {
        Self::definition()
    }
    async fn execute(&self, arguments: Value, context: ToolContext) -> Result<ToolOutput, Error> {
        if !context.subject.is_tool() || context.tool != "job" {
            return Err(Error::denied(
                "job requires its own admitted tool invocation",
            ));
        }
        context.ensure_admitted(None).map_err(|error| {
            if matches!(error, crate::policy::PermissionError::Cancelled) {
                Error::Cancelled
            } else {
                Error::denied_with_private(error.public_reason(), error.to_string())
            }
        })?;
        context
            .ensure_admitted_arguments(&arguments)
            .map_err(|error| {
                if matches!(error, crate::policy::PermissionError::Cancelled) {
                    Error::Cancelled
                } else {
                    Error::denied_with_private(error.public_reason(), error.to_string())
                }
            })?;
        let parsed = validate(&arguments)?;
        context.begin_dispatch(None).map_err(|error| {
            if matches!(error, crate::policy::PermissionError::Cancelled) {
                Error::Cancelled
            } else {
                Error::denied_with_private(error.public_reason(), error.to_string())
            }
        })?;
        execute_parsed(parsed, context).await
    }
}

pub(crate) async fn execute_admitted(
    arguments: Value,
    context: ToolContext,
) -> Result<ToolOutput, Error> {
    let parsed = validate(&arguments)?;
    if parsed.redirect.is_some() {
        return Err(Error::denied(
            "job redirect requires its own admitted tool call; use refs::create directly",
        ));
    }
    execute_parsed(parsed, context).await
}

async fn execute_parsed(
    parsed: arguments::Arguments<'_>,
    context: ToolContext,
) -> Result<ToolOutput, Error> {
        if let Some(redirect) = parsed.redirect {
            let source = resolve(parsed.ids[0].as_ref(), &context.session_id)?;
            return redirect::execute(source, redirect, context).await;
        }
        if parsed.batch {
            return batch::execute(parsed, context).await;
        }
        let id = parsed.ids[0].as_ref();
        let action = parsed.action;
        let args = parsed.args;
        let execution_id = resolve(id, &context.session_id)?;
        let registry = context.state.executions();
        let maximum = output_budget(&context.state.snapshot().limits);
        if action == "write" {
            registry.merge_redaction(execution_id, &context.redaction_handle())?;
        }
        let operation = batch::operate::<false>(
            &registry,
            execution_id,
            action,
            &args,
            maximum,
            args.get("revision").and_then(Value::as_u64),
            context.cancellation.clone(),
        );
        let signal = context
            .observation_interrupt
            .as_ref()
            .filter(|_| action == "wait");
        let Some(result) = observe(signal, operation).await else {
            return Ok(interrupted());
        };
        let result = result?;
        let result = if context.state.short_model_refs() {
            result.with_model_metadata()
        } else {
            result
        };
        Ok(ToolOutput::text(serde_json::to_string(&result)?))
}
