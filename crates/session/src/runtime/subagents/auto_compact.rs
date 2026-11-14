//! System helper retains the issuer prefix and never dispatches inherited tools.
use super::{Builtin, ContextMode, Source};
use crate::context::Content;
use crate::{Client, Error};
use microcrab_auditor::{Auditor, VariableView};
use microcrab_protocol::Response;
use tokio_util::sync::CancellationToken;

pub(super) const BUILTIN: Builtin = Builtin {
    name: "auto_compact",
    source: Source::System,
    context_mode: ContextMode::Inherit,
    prompt: include_str!("../../../templates/subagents/AUTO_COMPACT.tmpl"),
};
pub(crate) async fn run(
    executor: &dyn super::OneShotExecutor,
    client: std::sync::Arc<dyn Client>,
    auditor: std::sync::Arc<dyn Auditor>,
    variables: std::sync::Arc<dyn VariableView>,
    issuer: &Content,
    cancellation: &CancellationToken,
) -> Result<Response, Error> {
    let response = super::compact::run_builtin(
        executor,
        client,
        auditor,
        variables,
        issuer,
        &[],
        cancellation,
        BUILTIN.name,
        BUILTIN.source,
    )
    .await?;
    let headings = [
        "## Request and Intent",
        "## Technical Concepts",
        "## Files and Code",
        "## Errors and Fixes",
        "## Pending Jobs",
        "## Current Work",
        "## Next Step",
        "## Critical Context",
    ];
    let mut section = 0usize;
    let mut body = false;
    for line in response.content.lines().map(str::trim) {
        if line.starts_with("## ") {
            if section > 0 && !body {
                return Err(Error::protocol(
                    "automatic compaction summary has an empty section",
                ));
            }
            if headings.get(section).copied() != Some(line) {
                return Err(Error::protocol(
                    "automatic compaction summary has invalid section order",
                ));
            }
            section += 1;
            body = false;
        } else if !line.is_empty() {
            if section == 0 {
                return Err(Error::protocol(
                    "automatic compaction summary has text before its first section",
                ));
            }
            body = true;
        }
    }
    let truncated = response.usage.provider_details.iter().any(|value| {
        value.get("stop_reason").and_then(serde_json::Value::as_str) == Some("max_tokens")
            || value
                .get("finishReason")
                .and_then(serde_json::Value::as_str)
                == Some("MAX_TOKENS")
    });
    if section != headings.len()
        || !body
        || truncated
        || response
            .usage
            .output_tokens
            .is_some_and(|tokens| tokens >= u64::from(issuer.max_output_tokens))
    {
        return Err(Error::protocol(
            "automatic compaction summary is incomplete or output-limited",
        ));
    }
    Ok(response)
}
