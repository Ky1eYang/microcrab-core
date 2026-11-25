//! Transfer only a certified registry lease into the live variable owner.
use super::{ExecutionId, REDIRECT, ToolContext, ToolOutput, arguments::Redirect};
use crate::{Error, LimitKind, state::access::BytesVariableMetadata};
use serde::Serialize;

#[derive(Serialize)]
struct Receipt {
    action: &'static str,
    variable: BytesVariableMetadata,
    /// This operation never releases the source; another admission may do so.
    source_released: bool,
}

// Name and MIME are at most 128 and 127 ASCII characters without JSON escapes;
// the remaining fields are fixed strings or bounded identifiers. Check the
// bound before an effect so a tiny output budget cannot hide an applied write.
const MAX_RECEIPT_BYTES: usize = 590;

pub(super) async fn execute(
    source: ExecutionId,
    args: Redirect<'_>,
    context: ToolContext,
) -> Result<ToolOutput, Error> {
    let destination = context.redirect_destination.ok_or_else(|| {
        Error::denied("job redirect requires a destination witnessed before permission")
    })?;
    if destination.name() != args.variable {
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
    let write = context
        .state
        .commit_redirect(destination, lease, operation, &context.cancellation)
        .await?;
    let text = serde_json::to_string(&Receipt {
        action: REDIRECT,
        variable: write.metadata,
        source_released: false,
    })
    .map_err(|_| super::uncertain_redirect(operation))?;
    debug_assert!(text.len() <= MAX_RECEIPT_BYTES);
    Ok(ToolOutput::text(text))
}

#[cfg(test)]
mod tests;
