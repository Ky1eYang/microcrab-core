//! Invocation proof adapters; policy decisions stay with the controller owner.
use super::*;
use crate::policy::{
    Action, Decision, PermissionError, PermissionRequest, PolicySnapshot, SessionPermission,
};

impl ToolContext {
    pub(crate) fn has_resolution_for(&self, proof: &crate::policy::AdmittedInvocation) -> bool {
        self.resolved_arguments
            .as_ref()
            .is_some_and(|resolved| resolved.belongs_to(proof))
    }
    /// Claim tool/lifecycle entry once. Internal Host operations and background
    /// continuations validate the retained proof without claiming another entry.
    pub fn begin_dispatch(&self, component: Option<&str>) -> Result<(), PermissionError> {
        self.admission
            .as_ref()
            .ok_or(PermissionError::Denied)?
            .begin_dispatch(self, component)
    }
    pub(crate) fn admitted_arguments(&self) -> Result<&Value, PermissionError> {
        self.recorded_arguments()
    }
    /// Original admitted arguments for audit, descriptors and durable dispatch
    /// records. Resolved secret values must never be copied into those owners.
    pub fn recorded_arguments(&self) -> Result<&Value, PermissionError> {
        self.ensure_admitted(None)?;
        Ok(self
            .admission
            .as_ref()
            .ok_or(PermissionError::Denied)?
            .original_arguments())
    }
    /// Opaque per-invocation literal masking for trusted retention adapters.
    pub fn redaction_handle(&self) -> super::super::execution::InvocationRedaction {
        self.redaction.clone()
    }
    pub fn ensure_admitted_arguments(&self, arguments: &Value) -> Result<(), PermissionError> {
        self.ensure_admitted(None)?;
        if let Some(resolved) = &self.resolved_arguments {
            return resolved.validate(self, arguments);
        }
        self.admission
            .as_ref()
            .ok_or(PermissionError::Denied)?
            .validate_arguments(arguments)
    }
    /// Validate invocation-pinned authority. Policy edits affect later admissions;
    /// cancellation still revokes this invocation and every cloned continuation.
    pub fn ensure_admitted(&self, component: Option<&str>) -> Result<(), PermissionError> {
        self.admission
            .as_ref()
            .ok_or(PermissionError::Denied)?
            .validate(self, component)
    }
}

#[derive(Default)]
pub struct DenyAll;
#[async_trait]
impl SessionPermission for DenyAll {
    async fn evaluate(
        &self,
        _: &PolicySnapshot,
        _: &PermissionRequest,
    ) -> Result<Decision, PermissionError> {
        Ok(Decision::new(Action::Deny, "deny-all adapter", "adapter"))
    }
}

/// Explicit override factory. Allowing a WASM export trusts its registered
/// component's linked Host behavior throughout that admitted invocation.
pub struct AllowTools {
    names: std::collections::BTreeSet<String>,
}
impl AllowTools {
    pub fn new(names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            names: names.into_iter().map(Into::into).collect(),
        }
    }
}
#[async_trait]
impl SessionPermission for AllowTools {
    fn initial_overrides(&self) -> Vec<crate::policy::Permission> {
        use crate::policy::{ArgumentPredicate, Matcher, Permission};
        let mut rules = Vec::new();
        for declared in &self.names {
            let (name, actions): (&str, &[&str]) = match declared.as_str() {
                "job" => ("job", &["status", "read", "wait"]),
                "job:write" => ("job", &["write"]),
                "job:cancel" => ("job", &["cancel", "release"]),
                name => (name, &[]),
            };
            let base = Permission {
                agent: None,
                mode: None,
                tool: Some(Matcher::Exact(name.into())),
                args: vec![],
                action: Action::Allow,
            };
            if actions.is_empty() {
                rules.push(base);
            } else {
                for action in actions {
                    let mut rule = base.clone();
                    rule.args.push(ArgumentPredicate {
                        pointer: "/action".into(),
                        matcher: Matcher::Exact((*action).into()),
                    });
                    rules.push(rule);
                }
            }
        }
        rules
    }
    async fn evaluate(
        &self,
        policy: &PolicySnapshot,
        request: &PermissionRequest,
    ) -> Result<Decision, PermissionError> {
        microcrab_permission::evaluate(policy, request)
            .map_err(|e| PermissionError::InvalidConfiguration(e.to_string()))
    }
}
