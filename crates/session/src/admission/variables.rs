//! Pure custom-variable policy; State retains reference ownership and commit locks.
use crate::{Error, VariablesChange};
use std::collections::HashSet;

/// Protected model namespaces. `host` and `agent` remain compatibility guards:
/// HostInfo and SYSTEM's agent namespace carry those facts, not derived State.
pub(crate) fn is_reserved_key(key: &str) -> bool {
    matches!(
        key,
        "time"
            | "notices"
            | "session"
            | "host"
            | "agent"
            | "agent_init"
            | "agent_initialization_pending"
            | "mode"
            | "jobs"
            | "permission"
            | "subagents"
            | "delegatable_agents"
            | "mode_prompt"
            | "executions"
            | "tool_tasks"
    )
}

pub(crate) fn validate_custom_key(key: &str) -> Result<(), Error> {
    if key.is_empty() || key.contains('\0') || is_reserved_key(key) {
        return Err(Error::config("empty, invalid, or reserved variable name"));
    }
    Ok(())
}

pub(crate) fn validate_change(change: &VariablesChange) -> Result<(), Error> {
    let mut seen = HashSet::new();
    for key in change
        .set
        .keys()
        .chain(change.remove.iter())
        .chain(change.scoped_set.iter().map(|entry| &entry.name))
    {
        validate_custom_key(key)?;
        if !seen.insert(key) {
            return Err(Error::config("duplicate or overlapping variable changes"));
        }
    }
    for entry in &change.scoped_set {
        if entry.visibility == microcrab_protocol::VariableVisibility::Visible
            && entry.persistence == microcrab_protocol::VariablePersistence::Ephemeral
        {
            return Err(Error::config(
                "model-visible ephemeral variables are not supported",
            ));
        }
    }
    Ok(())
}

/// Admit the candidate value before State installs it under its existing lock.
pub(crate) fn admit(
    value: &serde_json::Value,
    context_budget: usize,
) -> Result<super::Admission, Error> {
    const MAX_CUSTOM_VARIABLE_BYTES: usize = 2 * 1024 * 1024;
    super::Admission::serialized(
        super::AdmissionSource::CustomVariables,
        crate::LimitKind::CustomVariableBytes,
        value,
        MAX_CUSTOM_VARIABLE_BYTES.min(context_budget),
    )
}
