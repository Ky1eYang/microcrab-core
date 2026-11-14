//! Origin-tagged dispatch, schema validation and provider-default registration.
use super::*;
use microcrab_protocol::ToolCall;
use std::collections::BTreeMap;
struct Entry {
    origin: ToolOrigin,
    tool: Arc<dyn Tool>,
    definition: ToolDefinition,
    schema: jsonschema::Validator,
}
pub(crate) struct ToolRegistry {
    entries: BTreeMap<String, Entry>,
}
impl ToolRegistry {
    pub(crate) fn new(tools: Vec<ToolRegistration>) -> Result<Self, Error> {
        let mut entries = BTreeMap::new();
        for registration in tools {
            let origin = registration.origin();
            let tool = registration.tool();
            let definition = tool.definition();
            if origin != ToolOrigin::Builtin && BuiltinTool::from_name(&definition.name).is_some() {
                return Err(Error::config_with_private(
                    "builtin tool name is reserved",
                    format!("builtin tool name is reserved: {}", definition.name),
                ));
            }
            if definition.name.is_empty()
                || definition.name.len() > 64
                || !definition
                    .name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            {
                return Err(Error::config("tool names must match [a-zA-Z0-9_-]{1,64}"));
            }
            if definition.parameters.get("type").and_then(Value::as_str) != Some("object") {
                return Err(Error::config("tool parameters must declare type object"));
            }
            reject_external_refs(&definition.parameters)?;
            let schema = jsonschema::validator_for(&definition.parameters).map_err(|e| {
                Error::Config(format!("invalid tool schema: {}", schema_reason(&e)).into())
            })?;
            let name = definition.name.clone();
            if entries
                .insert(
                    name.clone(),
                    Entry {
                        origin,
                        tool,
                        definition,
                        schema,
                    },
                )
                .is_some()
            {
                return Err(Error::config_with_private(
                    "duplicate tool name",
                    format!("duplicate tool: {name}"),
                ));
            }
        }
        Ok(Self { entries })
    }
    pub(crate) fn replacing_external(&self, external: Vec<Arc<dyn Tool>>) -> Result<Self, Error> {
        let mut registrations = self
            .entries
            .iter()
            .filter_map(|(name, entry)| match entry.origin {
                ToolOrigin::Builtin => BuiltinTool::from_name(name).map(ToolRegistration::Builtin),
                ToolOrigin::Wasm => Some(ToolRegistration::Wasm(entry.tool.clone())),
                ToolOrigin::External => None,
            })
            .collect::<Vec<_>>();
        registrations.extend(external.into_iter().map(ToolRegistration::External));
        Self::new(registrations)
    }
    pub(crate) fn provider_defaults(&self) -> Result<Vec<crate::policy::Permission>, Error> {
        let mut rules = Vec::new();
        for (name, entry) in &self.entries {
            if entry.origin == ToolOrigin::Builtin {
                continue;
            }
            for mut rule in entry.tool.permission_defaults() {
                if rule
                    .tool
                    .as_ref()
                    .is_some_and(|selector| !crate::policy::matches(selector, name))
                {
                    return Err(Error::config(
                        "provider permission does not match its registered tool",
                    ));
                }
                rule.tool = Some(crate::policy::Matcher::Exact(name.clone()));
                rules.push(rule);
            }
        }
        Ok(rules)
    }
    pub(crate) fn origins(&self) -> Vec<(String, ToolOrigin)> {
        self.entries
            .iter()
            .map(|(name, entry)| (name.clone(), entry.origin))
            .collect()
    }
    pub(crate) fn component(&self, name: &str) -> Option<String> {
        self.entries
            .get(name)?
            .tool
            .component_identity()
            .map(str::to_owned)
    }
    pub(crate) fn origin(&self, name: &str) -> Option<ToolOrigin> {
        self.entries.get(name).map(|entry| entry.origin)
    }
    #[cfg(feature = "wasm")]
    pub(crate) fn wasm_lifecycles(&self) -> Vec<super::super::wasm::AgentLifecycle> {
        let mut seen = std::collections::HashSet::new();
        self.entries
            .values()
            .filter_map(|entry| {
                (entry.origin == ToolOrigin::Wasm)
                    .then(|| entry.tool.wasm_lifecycle())
                    .flatten()
            })
            .filter(|lifecycle| seen.insert(lifecycle.identity()))
            .collect()
    }
    pub(crate) fn definitions(&self) -> Vec<ToolDefinition> {
        self.entries
            .values()
            .map(|e| e.definition.clone())
            .collect()
    }
    pub(crate) fn validate(&self, call: &ToolCall) -> Result<(), Error> {
        let entry = self
            .entries
            .get(&call.name)
            .ok_or_else(|| Error::tool("unknown tool"))?;
        if let Err(error) = entry.schema.validate(&call.arguments) {
            let mut diagnostic = crate::observation::diagnostics::ToolDiagnostic::new(
                crate::observation::diagnostics::DiagnosticCode::InvalidArgument,
                crate::observation::diagnostics::DiagnosticLayer::Runtime,
                crate::observation::diagnostics::DiagnosticPhase::Validation,
            );
            diagnostic.reason = Some(crate::observation::diagnostics::SafeDiagnosticReason::new(
                &format!("tool arguments violate schema: {}", schema_reason(&error)),
            ));
            diagnostic.effects = crate::observation::diagnostics::EffectKnowledge::NotStarted;
            return Err(Error::Diagnostic(diagnostic));
        }
        if entry.origin == ToolOrigin::Builtin && call.name == "expand" {
            super::expand::validate_selectors(&call.arguments)?;
        }
        Ok(())
    }
    pub(crate) async fn execute(
        &self,
        call: &ToolCall,
        invocation: ToolContext,
    ) -> Result<ToolOutput, Error> {
        let entry = self
            .entries
            .get(&call.name)
            .ok_or_else(|| Error::tool("unknown tool"))?;
        invocation
            .ensure_admitted(entry.tool.component_identity())
            .and_then(|()| invocation.ensure_admitted_arguments(&call.arguments))
            .map_err(|error| error.into_runtime_error())?;
        if entry.origin != ToolOrigin::Wasm && call.name != "job" {
            invocation
                .begin_dispatch(entry.tool.component_identity())
                .map_err(|error| error.into_runtime_error())?;
        }
        entry.tool.execute(call.arguments.clone(), invocation).await
    }
}
fn reject_external_refs(value: &Value) -> Result<(), Error> {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                if ["$ref", "$dynamicRef", "$recursiveRef"].contains(&key.as_str())
                    && !value.as_str().is_some_and(|s| s.starts_with('#'))
                {
                    return Err(Error::config("external schema references are disabled"));
                }
                reject_external_refs(value)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                reject_external_refs(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Schema facts come from the registered public tool contract. Instance values,
/// unknown user keys, embedded documents and recursive error contexts stay excluded.
fn schema_reason(error: &jsonschema::error::ValidationError<'_>) -> String {
    use jsonschema::error::ValidationErrorKind as K;
    use microcrab_protocol::diagnostic::{describe, sanitize};
    let cause = match &error.kind {
        K::Type { kind } => format!("expected type {kind:?}"),
        K::Required { property } => format!(
            "missing required field {}",
            sanitize(property.as_str().unwrap_or("field"), 128)
        ),
        K::MinLength { limit } => format!("string requires at least {limit} characters"),
        K::MaxLength { limit } => format!("string permits at most {limit} characters"),
        K::MinItems { limit } => format!("array requires at least {limit} items"),
        K::MaxItems { limit } => format!("array permits at most {limit} items"),
        K::AdditionalProperties { unexpected } => {
            format!("object contains {} unexpected fields", unexpected.len())
        }
        K::UnevaluatedProperties { unexpected } => {
            format!("object contains {} unevaluated fields", unexpected.len())
        }
        _ => describe(&error.masked(), 1400),
    };
    format!("{cause}; schema rule {}", describe(&error.schema_path, 256))
}
