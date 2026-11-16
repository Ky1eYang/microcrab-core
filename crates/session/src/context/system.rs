//! Render SYSTEM with Agent-owned structured values and frozen literal client text.
use crate::Error;
use microcrab_agent_types::AgentInfo;

pub const DEFAULT_SYSTEM: &str = include_str!("../../templates/SYSTEM.tmpl");
mod config;
mod syntax;
pub use config::*;

/// Loading is deliberately separate from rendering. Session Variables are never
/// inserted here: only guest-produced values may populate the agent namespace.
pub fn render(template: &str, agent: Option<&AgentInfo>) -> Result<String, Error> {
    render_with_client(template, agent, None)
}

pub fn render_with_client(
    template: &str,
    agent: Option<&AgentInfo>,
    client_prompt: Option<&str>,
) -> Result<String, Error> {
    let template = if template.is_empty() {
        DEFAULT_SYSTEM
    } else {
        template
    };
    if template.len() > MAX_PROMPT_BYTES
        || client_prompt.is_some_and(|value| value.len() > MAX_PROMPT_BYTES)
    {
        return Err(Error::config("prompt configuration byte limit exceeded"));
    }
    if template.trim().is_empty() {
        return Err(Error::config("SYSTEM template must be nonempty"));
    }
    syntax::validate(template)?.validate_data(agent, client_prompt)?;
    let mut context = tera::Context::new();
    if let Some(agent) = agent {
        context.insert("agent", &agent.variables);
    }
    if let Some(prompt) = client_prompt {
        context.insert("client_prompt", prompt);
    }
    // Tera 2 no longer provides json_encode. Keep the existing SYSTEM filter
    // contract locally. Tera's preserve_order feature must retain incoming JSON
    // object order during Context::insert; serde_json cannot restore lost order.
    let mut engine = tera::Tera::new();
    engine.register_filter("json_encode", json_encode);
    struct BoundedOutput(Vec<u8>);
    impl std::io::Write for BoundedOutput {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > MAX_PROMPT_CONFIG_BYTES.saturating_sub(self.0.len()) {
                return Err(std::io::Error::other("rendered SYSTEM byte limit exceeded"));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut output = BoundedOutput(Vec::new());
    engine
        .render_str_to(template, &context, false, &mut output)
        .map_err(|error| {
            Error::config_with_private(
                "SYSTEM template rendering failed",
                format!("SYSTEM template rendering failed: {error}"),
            )
        })?;
    let rendered = String::from_utf8(output.0).map_err(|error| {
        Error::config_with_private("SYSTEM template output is not UTF-8", error.to_string())
    })?;
    if rendered.trim().is_empty() {
        return Err(Error::config("rendered SYSTEM must be nonempty"));
    }
    Ok(rendered)
}

fn json_encode(
    value: &tera::Value,
    kwargs: tera::Kwargs,
    _: &tera::State,
) -> tera::TeraResult<String> {
    if value.is_undefined() {
        return Err(tera::Error::message("json_encode requires a defined value"));
    }
    let pretty = kwargs
        .get::<tera::Value>("pretty")?
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let json = serde_json::to_value(value)
        .map_err(|error| tera::Error::message(format!("json_encode failed: {error}")))?;
    let encoded = if pretty {
        serde_json::to_string_pretty(&json)
    } else {
        serde_json::to_string(&json)
    };
    encoded.map_err(|error| tera::Error::message(format!("json_encode failed: {error}")))
}
