//! Compact owns its prompt, descriptor, model call and summary validation.
use super::{Builtin, ContextMode, Source};
use crate::Client;
use crate::Error;
use crate::context::Content;
use microcrab_auditor::Auditor;
use microcrab_auditor::VariableView;
use microcrab_protocol::Response;
use tokio_util::sync::CancellationToken;

pub(super) const BUILTIN: Builtin = Builtin {
    name: "compact",
    source: Source::User,
    context_mode: ContextMode::Inherit,
    prompt: include_str!("../../../templates/subagents/COMPACT.tmpl"),
};

/// Uses a frozen request without admitting a turn or dispatching inherited tools.
/// Session admission and baseline publication remain with the command owner.
pub(crate) async fn run(
    executor: &dyn super::OneShotExecutor,
    client: std::sync::Arc<dyn Client>,
    auditor: std::sync::Arc<dyn Auditor>,
    variables: std::sync::Arc<dyn VariableView>,
    issuer: &Content,
    user_input: &str,
    cancellation: &CancellationToken,
) -> Result<Response, Error> {
    run_builtin(
        executor,
        client,
        auditor,
        variables,
        issuer,
        &[("user_input", user_input)],
        cancellation,
        BUILTIN.name,
        BUILTIN.source,
    )
    .await
}

#[expect(
    clippy::too_many_arguments,
    reason = "Functional calls retain separate model/audit/context owners"
)]
pub(super) async fn run_builtin(
    executor: &dyn super::OneShotExecutor,
    client: std::sync::Arc<dyn Client>,
    auditor: std::sync::Arc<dyn Auditor>,
    variables: std::sync::Arc<dyn VariableView>,
    issuer: &Content,
    bindings: &[(&str, &str)],
    cancellation: &CancellationToken,
    name: &str,
    source: Source,
) -> Result<Response, Error> {
    let request =
        super::prepare_configured(name, source, issuer, bindings, &executor.prompt_config())?;
    let response = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(Error::Cancelled),
        result = executor.execute(client, auditor.clone(), variables.clone(), &request, cancellation) => result?,
    };
    // Audit has observed usage even when the returned response is rejected.
    if !response.tool_calls.is_empty() {
        return Err(Error::protocol("functional subagent returned tool calls"));
    }
    if response.content.trim().is_empty() {
        return Err(Error::protocol("functional subagent returned empty text"));
    }
    if cancellation.is_cancelled() {
        return Err(Error::Cancelled);
    }
    Ok(response)
}
