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
            | "correspondents"
            | "external_speakers"
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
        if entry.visibility == microcrab_protocol::VariableVisibility::Secret {
            validate_secret(&entry.name, &entry.value)?;
            if entry.persistence != microcrab_protocol::VariablePersistence::Ephemeral
                || entry.editors.as_ref().is_some_and(|editors| {
                    editors
                        .iter()
                        .any(|role| *role != microcrab_protocol::VariableRole::Client)
                })
            {
                return Err(Error::config(
                    "Secret variables require Ephemeral persistence and Client-only editors",
                ));
            }
        }
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

pub(crate) fn validate_secret(key: &str, value: &serde_json::Value) -> Result<(), Error> {
    if !microcrab_protocol::valid_secret_key(key) {
        return Err(Error::config("invalid Secret variable key"));
    }
    let Some(value) = value.as_str() else {
        return Err(Error::config("Secret variable value must be a string"));
    };
    let bytes = value.len();
    if !(1..=8192).contains(&bytes) {
        return Err(Error::config("Secret variable value must be 1..8192 bytes"));
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
