//! Shared-rule evaluation is explicit; no network client gains an interactive Ask adapter.
use async_trait::async_trait;
use microcrab_permission::{Decision, PermissionFeatures, PermissionRequest, PolicySnapshot};
use microcrab_session::policy::{PermissionError, SessionPermission};

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Default)]
pub struct DeclarativePermission;

#[async_trait]
impl SessionPermission for DeclarativePermission {
    fn declarative_policy(&self) -> bool {
        true
    }

    async fn evaluate(
        &self,
        policy: &PolicySnapshot,
        request: &PermissionRequest,
    ) -> Result<Decision, PermissionError> {
        // Native builtin defaults, provider declarations, and user overrides all
        // use the shared evaluator. Explicit rules can narrow every job operation.
        microcrab_permission::evaluate(policy, request)
            .map_err(|error| PermissionError::InvalidConfiguration(error.to_string()))
    }

    fn features(&self) -> PermissionFeatures {
        PermissionFeatures {
            interactive_ask: false,
            remembered_grants: false,
            live_edit: true,
        }
    }
}
