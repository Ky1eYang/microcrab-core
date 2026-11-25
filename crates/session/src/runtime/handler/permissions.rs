//! One Runtime publication of the exact registered implementations and their defaults.
use super::{Runtime, ToolRegistry};
use crate::policy::{
    CurrentToolPermission, ListBudget, Permission, PermissionController, PermissionError,
    PermissionListError, PermissionObservation, PolicySnapshot,
};
use std::sync::{Arc, RwLock, TryLockError};

#[derive(Clone)]
pub(crate) struct AppliedCatalog {
    pub(super) registry: Arc<ToolRegistry>,
    revision: u64,
}

impl AppliedCatalog {
    pub(super) fn new(registry: ToolRegistry) -> Self {
        Self {
            registry: Arc::new(registry),
            revision: 1,
        }
    }

    pub(crate) fn revision(&self) -> u64 {
        self.revision
    }

    pub(crate) fn registry(&self) -> &ToolRegistry {
        &self.registry
    }

    pub(crate) fn matches(&self, other: &Self) -> bool {
        self.revision == other.revision && Arc::ptr_eq(&self.registry, &other.registry)
    }

    pub(crate) fn observe(
        tools: &RwLock<Self>,
        controller: &PermissionController,
    ) -> Result<(Self, PolicySnapshot), PermissionListError> {
        let catalog = tools.try_read().map_err(|error| match error {
            TryLockError::WouldBlock => PermissionListError::Busy,
            TryLockError::Poisoned(_) => PermissionListError::Unavailable,
        })?;
        // Read policy while pinning the catalog, never in the interval between
        // installing controller defaults and swapping the applied catalog.
        let policy = controller.snapshot()?;
        Ok((catalog.clone(), policy))
    }

    pub(crate) fn policy_rows(
        &self,
        policy: &PolicySnapshot,
        lineage: &[(Arc<PermissionController>, PolicySnapshot)],
        budget: &mut ListBudget,
    ) -> Result<Vec<CurrentToolPermission>, PermissionListError> {
        let tools = self.registry.tool_names();
        if tools.len() > 4096 {
            return Err(PermissionListError::Budget);
        }
        tools
            .map(|name| {
                let policy = PermissionController::compile_witness(policy, lineage, name)?;
                let row = CurrentToolPermission {
                    name: name.to_owned(),
                    policy,
                };
                budget.charge(&row)?;
                Ok(row)
            })
            .collect()
    }
}

impl Runtime {
    pub(crate) fn observe_applied_policy(
        &self,
    ) -> Result<(AppliedCatalog, PolicySnapshot), PermissionListError> {
        AppliedCatalog::observe(&self.tools, &self.permission)
    }

    pub(crate) fn permission_observation(&self) -> PermissionObservation {
        PermissionObservation::runtime(&self.permission, &self.tools)
    }

    /// No async work under the lock; read-side snapshots are nonblocking and
    /// user edits still compare-and-publish at the controller's own revision.
    pub(crate) fn publish_catalog(
        &self,
        registry: Arc<ToolRegistry>,
        provider: Vec<Permission>,
        snapshot: PolicySnapshot,
    ) -> Result<(), PermissionError> {
        let mut applied = self
            .tools
            .write()
            .map_err(|_| PermissionError::Unavailable)?;
        let revision = applied.revision.checked_add(1).ok_or_else(|| {
            PermissionError::InvalidConfiguration("applied catalog revision exhausted".into())
        })?;
        self.permission
            .publish_tools(registry.origins(), provider, snapshot)?;
        *applied = AppliedCatalog { registry, revision };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use crate::runtime::tool::{DenyAll, Tool, ToolContext, ToolOutput, ToolRegistration};
    use microcrab_protocol::ToolDefinition;
    use serde_json::{Value, json};

    struct External;

    #[async_trait::async_trait]
    impl Tool for External {
        fn definition(&self) -> ToolDefinition {
            ToolDefinition {
                name: "fixture_external".into(),
                description: "same schema, different registration".into(),
                parameters: json!({"type": "object"}),
            }
        }

        async fn execute(&self, _: Value, _: ToolContext) -> Result<ToolOutput, Error> {
            Ok(ToolOutput::text("fixture"))
        }
    }

    #[test]
    fn applied_policy_read_fences_even_same_definition_catalog_replacement() {
        let runtime = Runtime::new(
            vec![ToolRegistration::External(Arc::new(External))],
            Arc::new(DenyAll),
        )
        .unwrap();
        let (before, policy) = runtime.observe_applied_policy().unwrap();
        let candidate = runtime
            .prepare_external_tools(vec![Arc::new(External)])
            .unwrap();
        assert_eq!(before.registry().definitions(), candidate.definitions());
        assert!(before.matches(&runtime.observe_applied_policy().unwrap().0));
        let publication = runtime.tools.write().unwrap();
        assert!(matches!(
            runtime.observe_applied_policy(),
            Err(PermissionListError::Busy)
        ));
        drop(publication);

        runtime
            .publish_catalog(candidate.clone(), vec![], policy.clone())
            .unwrap();
        let (after, replacement_policy) = runtime.observe_applied_policy().unwrap();
        assert!(!before.matches(&after));
        assert_eq!(after.revision(), before.revision() + 1);
        assert_eq!(replacement_policy.revision, policy.revision + 1);
        let rows = after
            .policy_rows(&replacement_policy, &[], &mut ListBudget::new())
            .unwrap();
        assert_eq!(rows[0].name, "fixture_external");
        assert!(matches!(
            runtime.publish_catalog(candidate, vec![], policy),
            Err(PermissionError::StalePolicy)
        ));
        assert!(after.matches(&runtime.observe_applied_policy().unwrap().0));
    }
}
