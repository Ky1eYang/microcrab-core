//! Render the loaded SYSTEM string with the agent's own structured values only.
use crate::Error;
use microcrab_agent_types::AgentInfo;

pub const DEFAULT_SYSTEM: &str = include_str!("../../templates/SYSTEM.tmpl");

/// Loading is deliberately separate from rendering. Session Variables are never
/// inserted here: only guest-produced values may populate the agent namespace.
pub fn render(template: &str, agent: Option<&AgentInfo>) -> Result<String, Error> {
    if template.trim().is_empty() {
        return Err(Error::config("SYSTEM template must be nonempty"));
    }
    let mut context = tera::Context::new();
    if let Some(agent) = agent {
        context.insert("agent", &agent.variables);
    }
    // Tera 2 no longer provides json_encode. Keep the existing SYSTEM filter
    // contract locally. Tera's preserve_order feature must retain incoming JSON
    // object order during Context::insert; serde_json cannot restore lost order.
    let mut engine = tera::Tera::new();
    engine.register_filter("json_encode", json_encode);
    let rendered = engine
        .render_str(template, &context, false)
        .map_err(|error| {
            Error::config_with_private(
                "SYSTEM template rendering failed",
                format!("SYSTEM template rendering failed: {error}"),
            )
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
