//! Internal clean-context review; never allocates a Session command or mutates Context.
use super::{Builtin, ContextMode, Source};
use crate::context::Content;
use crate::policy::{PermissionError, PermissionRequest, ReviewResult};
use crate::{Client, Error, State};
use microcrab_auditor::Auditor;
use tokio_util::sync::CancellationToken;

pub(super) const BUILTIN: Builtin = Builtin {
    name: "action_review",
    source: Source::System,
    context_mode: ContextMode::Clean,
    prompt: include_str!("../../../templates/subagents/ACTION_REVIEW.tmpl"),
};

pub(crate) async fn run(
    executor: &dyn super::OneShotExecutor,
    client: std::sync::Arc<dyn Client>,
    auditor: std::sync::Arc<dyn Auditor>,
    state: &State,
    issuer: &Content,
    permission: &PermissionRequest,
    cancellation: &CancellationToken,
) -> Result<ReviewResult, PermissionError> {
    let input = serde_json::to_string(permission).map_err(|_| PermissionError::Review)?;
    if input.len() > 65536 {
        return Err(PermissionError::Review);
    }
    let tool_args =
        serde_json::to_string(&permission.arguments).map_err(|_| PermissionError::Review)?;
    let request = super::prepare_configured(
        BUILTIN.name,
        Source::System,
        issuer,
        &[
            ("user_input", input.as_str()),
            ("tool", permission.tool.as_str()),
            ("tool_name", permission.tool.as_str()),
            ("tool_args", tool_args.as_str()),
        ],
        &executor.prompt_config(),
    )
    .map_err(|_| PermissionError::Review)?;
    let response = executor
        .execute(
            client,
            auditor.clone(),
            std::sync::Arc::new(state.clone()),
            &request,
            cancellation,
        )
        .await
        .map_err(|error| match error {
            Error::Cancelled => PermissionError::Cancelled,
            _ => PermissionError::Review,
        })?;
    if cancellation.is_cancelled() {
        return Err(PermissionError::Cancelled);
    }
    // The one-shot executor owns RoundCompleted, usage acceptance and hook
    // injection exactly once in its isolated child State. The parent never
    // synthesizes another completion for equal request bytes.
    if !response.tool_calls.is_empty() || response.content.len() > 16 * 1024 {
        return Err(PermissionError::Review);
    }
    let reply: ReviewResult = serde_json::from_str(&response.content).map_err(|error| {
        PermissionError::InvalidConfiguration(format!("invalid action review JSON: {error}"))
    })?;
    crate::policy::validate_review_result(&reply, permission).map_err(|error| {
        PermissionError::InvalidConfiguration(format!("invalid action review result: {error}"))
    })?;
    Ok(reply)
}
