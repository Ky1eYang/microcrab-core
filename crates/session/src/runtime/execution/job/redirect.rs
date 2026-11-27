//! Transfer only a certified registry lease into the session's default writable Reference.
use super::{ExecutionId, REDIRECT, ToolContext, ToolOutput, arguments::Redirect};
use crate::{Error, LimitKind, state::reference::ReferenceEntry};
use serde::Serialize;

#[derive(Serialize)]
struct Receipt {
    action: &'static str,
    reference: ReferenceEntry,
    /// This operation never releases the source; another admission may do so.
    source_released: bool,
}

// Receipt contains bounded IDs, MIME (<= 127), title (<= 256), and summary fields.
const MAX_RECEIPT_BYTES: usize = 2048;

pub(super) async fn execute(
    source: ExecutionId,
    args: Redirect<'_>,
    context: ToolContext,
) -> Result<ToolOutput, Error> {
    let destination = context.redirect_destination.ok_or_else(|| {
        Error::denied("job redirect requires a destination witnessed before permission")
    })?;
    if destination.expected_mime != args.expected_mime {
        return Err(Error::denied(
            "job redirect destination differs from admission",
        ));
    }
    let operation = context.execution_id.ok_or_else(|| {
        Error::denied("job redirect requires its exact execution operation identity")
    })?;
    crate::budget::check(
        LimitKind::ToolOutput,
        MAX_RECEIPT_BYTES,
        super::output_budget(&context.state.snapshot().limits),
    )?;
    let lease = context.state.executions().private_output(
        source,
        &context.session_id,
        args.channel,
        args.expected_mime,
    )?;
    let reference = context
        .state
        .commit_library_redirect(destination, lease, operation, &context.cancellation)
        .await?;
    let text = serde_json::to_string(&Receipt {
        action: REDIRECT,
        reference,
        source_released: false,
    })
    .map_err(|_| super::uncertain_redirect(operation))?;
    Ok(ToolOutput::text(text))
}

#[cfg(test)]
mod tests;
