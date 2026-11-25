//! Current Runtime permission facts and the separately marked management view.
use super::{CompiledToolPolicy, Matcher, PermissionConfig, PermissionError, PolicySnapshot};
use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PermissionListView {
    Current,
    Management,
}

#[derive(Debug, thiserror::Error)]
pub enum PermissionListError {
    #[error("current Runtime is busy publishing permissions")]
    Busy,
    #[error("current Runtime permissions are unavailable")]
    Unavailable,
    #[error("permission list exceeds its projection budget")]
    Budget,
    #[error(transparent)]
    Policy(#[from] PermissionError),
}

#[derive(Debug, Serialize)]
#[serde(tag = "view", rename_all = "snake_case")]
pub enum PermissionList {
    Current {
        current: CurrentPermissionList,
    },
    Management {
        current: CurrentPermissionList,
        #[serde(flatten)]
        policy: PolicySnapshot,
        tool_inventory: PermissionInventory,
    },
}

#[derive(Debug, Serialize)]
pub struct CurrentPermissionList {
    pub runtime_instance_id: String,
    pub catalog_revision: String,
    pub policy_revision: String,
    pub ancestor_witness: String,
    pub agent: String,
    pub mode: String,
    pub tools: Vec<CurrentToolPermission>,
}

#[derive(Debug, Serialize)]
pub struct CurrentToolPermission {
    pub name: String,
    pub policy: CompiledToolPolicy,
}

#[derive(Debug, Serialize)]
pub struct PermissionInventory {
    pub agent: String,
    pub modes: Vec<PermissionInventoryMode>,
    pub initialization: PermissionInitialization,
}

#[derive(Debug, Serialize)]
pub struct PermissionInventoryMode {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
    pub current: bool,
    pub tools: Vec<PermissionInventoryTool>,
    pub policy: PermissionConfig,
    pub compiled: Vec<PermissionInventoryRule>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PermissionInventoryTool {
    pub name: String,
    pub description: String,
}

#[derive(Debug, Serialize)]
pub struct PermissionInventoryRule {
    #[serde(flatten)]
    pub policy: CompiledToolPolicy,
    pub tool: Matcher,
    pub owners: Vec<PermissionOwner>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PermissionOwner {
    pub controller: u64,
    pub revision: u64,
    pub agent: String,
    pub mode: String,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PermissionInitialization {
    AllOf { policies: Vec<InitializationPolicy> },
}

#[derive(Debug, Serialize)]
pub struct InitializationPolicy {
    pub revision: u64,
    pub agent: String,
    pub mode: String,
    pub action: super::Action,
}

impl From<&PolicySnapshot> for InitializationPolicy {
    fn from(snapshot: &PolicySnapshot) -> Self {
        Self {
            revision: snapshot.revision,
            agent: snapshot.config.agent.clone(),
            mode: snapshot.config.mode.clone(),
            action: snapshot.config.initialization,
        }
    }
}

/// Enforce a response-wide bound while accumulating rows and alternate modes.
/// Counting serialization does not allocate a second copy of the payload.
pub(crate) struct ListBudget(usize);

impl ListBudget {
    pub(crate) fn new() -> Self {
        Self(8 * 1024 * 1024)
    }

    pub(crate) fn charge(&mut self, value: &impl Serialize) -> Result<(), PermissionListError> {
        serde_json::to_writer(self, value).map_err(|error| {
            if error.is_io() {
                PermissionListError::Budget
            } else {
                PermissionError::InvalidConfiguration(format!(
                    "permission list serialization: {error}"
                ))
                .into()
            }
        })
    }
}

impl std::io::Write for ListBudget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_sub(bytes.len())
            .ok_or_else(|| std::io::Error::other("permission list byte budget"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::Action;

    #[test]
    fn management_keeps_edit_revision_and_legacy_inventory_shape() {
        let policy = PolicySnapshot {
            revision: 7,
            config: PermissionConfig::default(),
        };
        let owner = PermissionOwner {
            controller: 1,
            revision: 7,
            agent: String::new(),
            mode: String::new(),
        };
        let compiled = CompiledToolPolicy::Static {
            action: Action::Ask,
        };
        let current = CurrentPermissionList {
            runtime_instance_id: "runtime".into(),
            catalog_revision: "8".into(),
            policy_revision: "7".into(),
            ancestor_witness: "witness".into(),
            agent: String::new(),
            mode: String::new(),
            tools: vec![CurrentToolPermission {
                name: "external".into(),
                policy: compiled.clone(),
            }],
        };
        let mode = PermissionInventoryMode {
            name: String::new(),
            description: None,
            prompt: None,
            current: true,
            tools: vec![],
            policy: policy.config.clone(),
            compiled: vec![PermissionInventoryRule {
                tool: Matcher::Exact("external".into()),
                policy: compiled,
                owners: vec![owner],
            }],
        };
        let tool_inventory = PermissionInventory {
            agent: String::new(),
            modes: vec![mode],
            initialization: PermissionInitialization::AllOf {
                policies: vec![(&policy).into()],
            },
        };
        let response = PermissionList::Management {
            current,
            policy,
            tool_inventory,
        };
        ListBudget::new().charge(&response).unwrap();
        let json = serde_json::to_value(response).unwrap();
        assert_eq!(json["revision"], 7);
        assert_eq!(json["current"]["policy_revision"], "7");
        assert_eq!(json["current"]["tools"][0]["policy"]["action"], "ask");
        let mode = &json["tool_inventory"]["modes"][0];
        assert_eq!(mode["compiled"][0]["action"], "ask");
        assert_eq!(mode["compiled"][0]["tool"]["value"], "external");
        assert_eq!(mode["compiled"][0]["owners"][0]["revision"], 7);
    }
}
