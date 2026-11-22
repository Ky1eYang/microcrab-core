//! Functional definitions and immutable request snapshots; no model or tool dispatch.
use crate::Error;
use crate::context::Content;
use serde::{Deserialize, Serialize};

/// Composition supplies a one-shot Session; Runtime never calls composition internals.
/// The child owns RoundCompleted hooks; callers validate the functional result.
#[async_trait::async_trait]
pub trait OneShotExecutor: Send + Sync {
    /// The issuing Session supplies a frozen helper configuration.
    fn prompt_config(&self) -> crate::context::system::PromptConfig {
        Default::default()
    }
    /// Seal operation admission, cancel outstanding work and await cleanup ACKs.
    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
    async fn execute(
        &self,
        client: std::sync::Arc<dyn crate::Client>,
        auditor: std::sync::Arc<dyn microcrab_auditor::Auditor>,
        variables: std::sync::Arc<dyn microcrab_auditor::VariableView>,
        request: &Content,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> Result<microcrab_protocol::Response, Error>;
}

pub(crate) mod action_review;
pub(crate) mod auto_compact;
pub(crate) mod compact;
mod prompt;
pub(crate) mod rename;

pub(crate) fn validate_template(
    template: &str,
    names: &[&str],
    maximum: usize,
) -> Result<(), Error> {
    let bindings: Vec<_> = names.iter().map(|name| (*name, "binding")).collect();
    let rendered = prompt::interpolate(template, &bindings, maximum)?;
    if rendered.trim().is_empty() {
        return Err(Error::config("functional prompt must be nonempty"));
    }
    Ok(())
}

pub(crate) fn prepare_configured(
    name: &str,
    source: Source,
    issuer: &Content,
    bindings: &[(&str, &str)],
    config: &crate::context::system::PromptConfig,
) -> Result<Content, Error> {
    let entry = BUILTINS
        .iter()
        .find(|entry| entry.name == name && entry.source == source)
        .ok_or_else(|| Error::config("unknown functional subagent for this source"))?;
    let template = match name {
        "compact" => &config.compact_template,
        "auto_compact" => &config.auto_compact_template,
        "rename" => &config.rename_template,
        "action_review" => &config.action_review_template,
        _ => return Err(Error::config("unknown configured helper")),
    };
    if name == "action_review" {
        return action_review::prepare_review(issuer, template, bindings);
    }
    entry.definition().prepare_markdown(
        if template.is_empty() {
            entry.prompt
        } else {
            template
        },
        issuer,
        bindings,
    )
}

const BUILTINS: &[Builtin] = &[
    compact::BUILTIN,
    rename::BUILTIN,
    action_review::BUILTIN,
    auto_compact::BUILTIN,
];

/// Static registration keeps each subagent's metadata and prompt together.
struct Builtin {
    name: &'static str,
    source: Source,
    context_mode: ContextMode,
    prompt: &'static str,
}

impl Builtin {
    fn definition(&self) -> Definition {
        Definition {
            name: self.name.into(),
            source: self.source,
            context_mode: self.context_mode,
        }
    }
}

/// The only discovery and execution path authorized for a definition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    System,
    Agent,
    User,
}

/// Which frozen issuer prefix is retained in the functional request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextMode {
    Inherit,
    Clean,
    System,
}

/// Metadata is mandatory: missing source or context mode cannot acquire defaults.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Definition {
    pub name: String,
    pub source: Source,
    pub context_mode: ContextMode,
}

pub fn discover(source: Source) -> Vec<Definition> {
    BUILTINS
        .iter()
        .filter(|entry| entry.source == source)
        .map(Builtin::definition)
        .collect()
}

/// Resolve only within the caller's source; unknown names never fall back.
pub fn prepare(
    name: &str,
    source: Source,
    issuer: &Content,
    bindings: &[(&str, &str)],
) -> Result<Content, Error> {
    let entry = BUILTINS
        .iter()
        .find(|entry| entry.name == name && entry.source == source)
        .ok_or_else(|| Error::config("unknown functional subagent for this source"))?;
    if name == "action_review" {
        return action_review::prepare_review(issuer, entry.prompt, bindings);
    }
    entry
        .definition()
        .prepare_markdown(entry.prompt, issuer, bindings)
}
