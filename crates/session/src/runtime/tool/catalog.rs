//! Origin-tagged dispatch, schema validation and provider-default registration.
#[cfg(test)]
mod media_tests;
#[cfg(test)]
mod schema_tests;
mod text_arguments;
use super::*;
use microcrab_protocol::ToolCall;
use std::collections::BTreeMap;
enum Implementation {
    Builtin(BuiltinTool, Arc<dyn Tool>),
    Wasm(Arc<dyn Tool>),
    External(Arc<dyn Tool>),
    DialogueReply(DialogueReplyRegistration),
}
impl Implementation {
    fn registration(&self) -> ToolRegistration {
        match self {
            Self::Builtin(kind, _) => ToolRegistration::Builtin(*kind),
            Self::Wasm(tool) => ToolRegistration::Wasm(tool.clone()),
            Self::External(tool) => ToolRegistration::External(tool.clone()),
            Self::DialogueReply(effect) => ToolRegistration::DialogueReply(effect.clone()),
        }
    }
    fn origin(&self) -> ToolOrigin {
        match self {
            Self::Builtin(..) => ToolOrigin::Builtin,
            Self::Wasm(_) => ToolOrigin::Wasm,
            Self::External(_) => ToolOrigin::External,
            Self::DialogueReply(_) => ToolOrigin::DialogueReply,
        }
    }
    fn tool(&self) -> Option<&dyn Tool> {
        match self {
            Self::Builtin(_, tool) | Self::Wasm(tool) | Self::External(tool) => Some(tool.as_ref()),
            Self::DialogueReply(_) => None,
        }
    }
}
struct Entry {
    implementation: Implementation,
    definition: ToolDefinition,
    schema: jsonschema::Validator,
    media_output_bytes: usize,
    secret_delivery: SecretDelivery,
}
pub(crate) struct ToolRegistry {
    entries: BTreeMap<String, Entry>,
}
impl ToolRegistry {
    pub(crate) fn new(tools: Vec<ToolRegistration>) -> Result<Self, Error> {
        let mut entries = BTreeMap::new();
        for registration in tools {
            let implementation = match registration {
                ToolRegistration::Builtin(kind) => {
                    Implementation::Builtin(kind, kind.implementation())
                }
                ToolRegistration::Wasm(tool) => Implementation::Wasm(tool),
                ToolRegistration::External(tool) => Implementation::External(tool),
                ToolRegistration::DialogueReply(effect) => Implementation::DialogueReply(effect),
            };
            let (definition, media_output_bytes, secret_delivery) = match &implementation {
                Implementation::DialogueReply(effect) => (
                    effect.definition.clone(),
                    0,
                    SecretDelivery::ForwardPlaceholders,
                ),
                Implementation::Builtin(kind, tool) => (
                    tool.definition(),
                    tool.media_output_bytes(),
                    if *kind == BuiltinTool::Job {
                        SecretDelivery::Resolve
                    } else {
                        SecretDelivery::ForwardPlaceholders
                    },
                ),
                Implementation::Wasm(tool) => (
                    tool.definition(),
                    tool.media_output_bytes(),
                    SecretDelivery::Resolve,
                ),
                Implementation::External(tool) => (
                    tool.definition(),
                    tool.media_output_bytes(),
                    tool.secret_delivery(),
                ),
            };
            crate::budget::check(
                crate::LimitKind::AttachmentBytes,
                media_output_bytes,
                crate::admission::image::MAX_IMAGE_BYTES,
            )?;
            if implementation.origin() != ToolOrigin::Builtin
                && BuiltinTool::from_name(&definition.name).is_some()
            {
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
                        implementation,
                        definition,
                        schema,
                        media_output_bytes,
                        secret_delivery,
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
    pub(crate) fn for_conversation(
        &self,
        identity: Option<&crate::state::communication::ConversationIdentity>,
        interactive_root: bool,
    ) -> Result<Self, Error> {
        use crate::state::communication::SessionInteraction;
        let persistent =
            identity.filter(|identity| identity.interaction == SessionInteraction::Persistent);
        let reply_eligible =
            interactive_root && persistent.is_some_and(|identity| identity.depth == 0);
        let mut registrations = self
            .entries
            .values()
            .filter_map(|entry| match &entry.implementation {
                Implementation::Builtin(BuiltinTool::SendMessage | BuiltinTool::Escalate, _) => {
                    None
                }
                Implementation::Builtin(BuiltinTool::Delegate, _)
                    if identity.is_some_and(|id| id.depth != 0) =>
                {
                    None
                }
                Implementation::DialogueReply(_) if !reply_eligible => None,
                implementation => Some(implementation.registration()),
            })
            .collect::<Vec<_>>();
        if let Some(identity) = persistent {
            registrations.push(ToolRegistration::Builtin(BuiltinTool::SendMessage));
            if identity.depth == 1 {
                registrations.push(ToolRegistration::Builtin(BuiltinTool::Escalate));
            }
        }
        Self::new(registrations)
    }
    pub(crate) fn replacing_external(&self, external: Vec<Arc<dyn Tool>>) -> Result<Self, Error> {
        let mut registrations = self
            .entries
            .values()
            .filter_map(|entry| match &entry.implementation {
                Implementation::External(_) => None,
                implementation => Some(implementation.registration()),
            })
            .collect::<Vec<_>>();
        registrations.extend(external.into_iter().map(ToolRegistration::External));
        Self::new(registrations)
    }
    pub(crate) fn validate_model_owner(
        &self,
        owner: &dyn crate::ModelAdmission,
    ) -> Result<(), Error> {
        let canonical = owner.dialogue_reply();
        if owner
            .retired_tool_names()
            .contains(&canonical.definition.name.as_str())
            || self.entries.iter().any(|(name, entry)| {
                owner.retired_tool_names().contains(&name.as_str())
                    || match &entry.implementation {
                        Implementation::DialogueReply(effect) => {
                            effect.admission_identity() != canonical.admission_identity()
                        }
                        _ => *name == canonical.definition.name,
                    }
            })
        {
            return Err(Error::IncompatibleModelCatalog);
        }
        Ok(())
    }

    pub(crate) fn replacing_dialogue_reply(
        &self,
        owner: &dyn crate::ModelAdmission,
        effect: Option<&DialogueReplyRegistration>,
    ) -> Result<Self, Error> {
        self.validate_model_owner(owner)?;
        if effect.is_some_and(|effect| {
            effect.admission_identity() != owner.dialogue_reply().admission_identity()
        }) {
            return Err(Error::IncompatibleModelCatalog);
        }
        let mut registrations = self
            .entries
            .values()
            .filter_map(|entry| match &entry.implementation {
                Implementation::DialogueReply(_) => None,
                implementation => Some(implementation.registration()),
            })
            .collect::<Vec<_>>();
        registrations.extend(effect.cloned().map(ToolRegistration::DialogueReply));
        Self::new(registrations)
    }

    pub(crate) fn provider_defaults(&self) -> Result<Vec<crate::policy::Permission>, Error> {
        let mut rules = Vec::new();
        for (name, entry) in &self.entries {
            if entry.implementation.origin() == ToolOrigin::Builtin {
                continue;
            }
            let defaults = match &entry.implementation {
                Implementation::DialogueReply(effect) => effect.defaults.clone(),
                implementation => implementation
                    .tool()
                    .map_or_else(Vec::new, Tool::permission_defaults),
            };
            for mut rule in defaults {
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
            .map(|(name, entry)| (name.clone(), entry.implementation.origin()))
            .collect()
    }
    pub(crate) fn interaction_presentation(
        &self,
        name: &str,
    ) -> Option<microcrab_auditor::ToolPresentation> {
        use microcrab_auditor::ToolPresentation;
        let entry = self.entries.get(name)?;
        match &entry.implementation {
            Implementation::Builtin(BuiltinTool::Reference, _) => {
                Some(ToolPresentation::Reference)
            }
            Implementation::Builtin(BuiltinTool::SendMessage | BuiltinTool::Escalate, _) => {
                Some(ToolPresentation::MessageReceipt)
            }
            #[cfg(feature = "wasm")]
            Implementation::Wasm(tool) if tool.file_version_host().is_some() => {
                const FILE_VERSION_PATTERN: &str =
                    "^(fv_([a-f0-9]{16}|[a-f0-9]{20}|[a-f0-9]{24})|[a-fA-F0-9]{64})$";
                let schema = &entry.definition.parameters;
                let known = match name {
                    "read" => super::short_model_refs(std::slice::from_ref(&entry.definition)),
                    "write" | "edit" | "patch" => schema
                        .pointer("/properties/expected_version/pattern")
                        .and_then(Value::as_str) == Some(FILE_VERSION_PATTERN),
                    "apply_patch" => schema
                        .pointer("/properties/preconditions/items/properties/expected/oneOf/1/properties/version/pattern")
                        .and_then(Value::as_str) == Some(FILE_VERSION_PATTERN),
                    _ => false,
                };
                known.then_some(ToolPresentation::CodingFile)
            }
            _ => None,
        }
    }

    pub(crate) fn admission_identity(&self, name: &str) -> Option<String> {
        match &self.entries.get(name)?.implementation {
            Implementation::DialogueReply(effect) => Some(effect.admission_identity().to_owned()),
            implementation => implementation
                .tool()?
                .component_identity()
                .map(str::to_owned),
        }
    }
    pub(crate) fn dialogue_reply_is_registered(&self, name: &str, identity: &str) -> bool {
        self.entries.get(name).is_some_and(|entry| {
            matches!(&entry.implementation, Implementation::DialogueReply(effect) if effect.admission_identity() == identity)
        })
    }
    /// Snapshot captured at registration, never derived from tool names or arguments.
    pub(crate) fn media_output_bytes(&self, name: &str) -> usize {
        self.entries
            .get(name)
            .map_or(0, |entry| entry.media_output_bytes)
    }

    pub(crate) fn origin(&self, name: &str) -> Option<ToolOrigin> {
        self.entries
            .get(name)
            .map(|entry| entry.implementation.origin())
    }
    #[cfg(feature = "wasm")]
    pub(crate) fn wasm_lifecycles(&self) -> Vec<super::super::wasm::AgentLifecycle> {
        let mut seen = std::collections::HashSet::new();
        self.entries
            .values()
            .filter_map(|entry| match &entry.implementation {
                Implementation::Wasm(tool) => tool.wasm_lifecycle(),
                _ => None,
            })
            .filter(|lifecycle| seen.insert(lifecycle.identity()))
            .collect()
    }
    pub(crate) fn tool_names(&self) -> impl ExactSizeIterator<Item = &str> {
        self.entries.keys().map(String::as_str)
    }

    pub(crate) fn tool_descriptions(&self) -> impl Iterator<Item = (&str, &str)> {
        self.entries.values().map(|entry| {
            (
                entry.definition.name.as_str(),
                entry.definition.description.as_str(),
            )
        })
    }

    pub(crate) fn definitions(&self) -> Vec<ToolDefinition> {
        self.entries
            .values()
            .map(|e| e.definition.clone())
            .collect()
    }
    #[cfg(feature = "wasm")]
    pub(crate) async fn bind_file_versions(
        &self,
        call: &ToolCall,
        invocation: &mut ToolContext,
    ) -> Result<(), Error> {
        if let Some(Entry {
            implementation: Implementation::Wasm(tool),
            ..
        }) = self.entries.get(&call.name)
            && let Some(host) = tool.file_version_host()
        {
            invocation.file_version_policy_arguments =
                super::super::wasm::bind_file_version_arguments(host, invocation, &call.arguments)
                    .await?
                    .map(Arc::new);
        }
        Ok(())
    }
    pub(crate) fn resolve_call(
        &self,
        call: &ToolCall,
        invocation: &mut ToolContext,
    ) -> Result<ToolCall, Error> {
        let entry = self
            .entries
            .get(&call.name)
            .ok_or_else(|| Error::tool("unknown tool"))?;
        let delivery = if self.uses_literal_sources(call) {
            SecretDelivery::ForwardPlaceholders
        } else {
            entry.secret_delivery
        };
        secret::resolve(call, invocation, delivery, |resolved| {
            self.validate_inner(resolved, false)
        })
    }
    #[cfg(feature = "wasm")]
    pub(crate) fn apply_glob_path_default(&self, call: &mut ToolCall) {
        let Some(entry) = self.entries.get(&call.name) else {
            return;
        };
        if let Implementation::Wasm(tool) = &entry.implementation
            && tool.uses_glob_path_default()
            && let Some(args) = call.arguments.as_object_mut()
        {
            args.entry("ignored_list")
                .or_insert_with(|| Value::String(GLOB_PATH_DEFAULT.into()));
        }
    }

    pub(crate) fn uses_literal_sources(&self, call: &ToolCall) -> bool {
        #[cfg(feature = "wasm")]
        if let Some(entry) = self.entries.get(&call.name)
            && let Implementation::Wasm(tool) = &entry.implementation
            && tool.uses_literal_sources()
        {
            return true;
        }
        self.origin(&call.name) == Some(ToolOrigin::Builtin)
            && call.name == "job"
            && crate::runtime::execution::job::is_redirect(&call.arguments)
    }
    pub(crate) fn convert_text_arguments(&self, call: &mut ToolCall) -> usize {
        self.entries.get(&call.name).map_or(0, |entry| {
            text_arguments::convert(&entry.definition.parameters, &mut call.arguments)
        })
    }

    pub(crate) fn validate(&self, call: &ToolCall) -> Result<(), Error> {
        self.validate_inner(call, true)
    }
    fn validate_inner(&self, call: &ToolCall, permit_placeholders: bool) -> Result<(), Error> {
        let entry = self
            .entries
            .get(&call.name)
            .ok_or_else(|| Error::tool("unknown tool"))?;
        let defer = permit_placeholders
            && entry.secret_delivery == SecretDelivery::Resolve
            && !self.uses_literal_sources(call);
        if let Some(error) = entry
            .schema
            .iter_errors(&call.arguments)
            .find(|error| !defer || !deferred_string_constraint(error))
        {
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
        match &entry.implementation {
            Implementation::Builtin(BuiltinTool::Reference, _) => {
                super::reference::validate(&call.arguments)?
            }
            Implementation::DialogueReply(_) => {
                DialogueReplyRequest::decode(&call.arguments)?;
            }
            _ => {}
        }
        Ok(())
    }
    pub(crate) async fn execute(
        &self,
        call: &ToolCall,
        invocation: ToolContext,
        reply_owner: Option<Arc<dyn DialogueReplyOwner>>,
    ) -> Result<ToolOutput, Error> {
        let entry = self
            .entries
            .get(&call.name)
            .ok_or_else(|| Error::tool("unknown tool"))?;
        let tool = match &entry.implementation {
            Implementation::DialogueReply(effect) => {
                return effect.dispatch(call, invocation, reply_owner).await;
            }
            implementation => implementation
                .tool()
                .ok_or_else(|| Error::config("missing tool implementation"))?,
        };
        invocation
            .ensure_admitted(tool.component_identity())
            .and_then(|()| invocation.ensure_admitted_arguments(&call.arguments))
            .map_err(|error| error.into_runtime_error())?;
        if entry.implementation.origin() != ToolOrigin::Wasm && call.name != "job" {
            invocation
                .begin_dispatch(tool.component_identity())
                .map_err(|error| error.into_runtime_error())?;
        }
        tool.execute(call.arguments.clone(), invocation).await
    }
}
fn deferred_string_constraint(error: &jsonschema::error::ValidationError<'_>) -> bool {
    use jsonschema::error::ValidationErrorKind as K;
    // Object shape, unknown fields, branches and types are never bypassed.
    // A constrained string is checked in full against its real resolved input.
    error
        .instance
        .as_str()
        .is_some_and(secret::contains_placeholder)
        && matches!(
            &error.kind,
            K::Pattern { .. } | K::Format { .. } | K::MinLength { .. } | K::MaxLength { .. }
        )
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
        K::Type { kind } => {
            let received = match error.instance.as_ref() {
                Value::Null => "null",
                Value::Bool(_) => "boolean",
                Value::Number(_) => "number",
                Value::String(_) => "string",
                Value::Array(_) => "array",
                Value::Object(_) => "object",
            };
            let mut cause = format!("expected type {kind:?}, received JSON {received}");
            if matches!(error.instance.as_ref(), Value::Number(_))
                && matches!(
                    kind,
                    jsonschema::error::TypeKind::Single(jsonschema::JsonType::String)
                )
            {
                cause.push_str("; pass a JSON string, not a number. Copy any opaque reference exactly from its source; do not convert numeric notation or guess a value");
            }
            if error.instance.is_null() {
                cause.push_str(
                    "; explicit null is invalid here; provide the required non-null value, or omit an unused optional field",
                );
            }
            cause
        }
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
