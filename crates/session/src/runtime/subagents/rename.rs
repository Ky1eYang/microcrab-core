//! Rename owns its prompt, model call and title validation; scheduling is external.
use super::{Builtin, ContextMode, Source};
use crate::Client;
use crate::Error;
use crate::context::Content;
use microcrab_auditor::Auditor;
use microcrab_auditor::VariableView;
use microcrab_protocol::Response;
use tokio_util::sync::CancellationToken;

pub(super) const BUILTIN: Builtin = Builtin {
    name: "rename",
    source: Source::System,
    context_mode: ContextMode::Clean,
    prompt: include_str!("../../../templates/subagents/RENAME.tmpl"),
};

/// Generate a title from an independent clean snapshot without installing it.
pub(crate) async fn run(
    executor: &dyn super::OneShotExecutor,
    client: std::sync::Arc<dyn Client>,
    auditor: std::sync::Arc<dyn Auditor>,
    variables: std::sync::Arc<dyn VariableView>,
    issuer: &Content,
    user_input: &str,
    cancellation: &CancellationToken,
) -> Result<Response, Error> {
    let request = super::prepare_configured(
        BUILTIN.name,
        BUILTIN.source,
        issuer,
        &[("user_input", user_input)],
        &executor.prompt_config(),
    )?;
    let mut response = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(Error::Cancelled),
        result = executor.execute(client, auditor.clone(), variables.clone(), &request, cancellation) => result?,
    };
    // Transport audit observes usage even when title validation rejects the result.
    if !response.tool_calls.is_empty() {
        return Err(Error::protocol("rename subagent returned tool calls"));
    }
    // Inspect before trimming so trailing line breaks cannot hide multiline output.
    if response.content.chars().any(char::is_control)
        || response.content.contains(['\u{2028}', '\u{2029}'])
    {
        return Err(Error::protocol("rename title must be a single line"));
    }
    let title = response.content.trim();
    if title.is_empty() {
        return Err(Error::protocol("rename subagent returned empty title"));
    }
    if title.chars().count() > 160 {
        return Err(crate::budget::reject(
            crate::LimitKind::ProviderResponse,
            "rename title character budget",
        ));
    }
    response.content = title.to_owned();
    if cancellation.is_cancelled() {
        return Err(Error::Cancelled);
    }
    Ok(response)
}
