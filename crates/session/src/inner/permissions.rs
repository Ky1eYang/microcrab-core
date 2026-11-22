//! User policy edits and explicit external-catalog epoch admission.
use super::*;
use crate::policy::{PermissionConfig, PermissionError, PolicySnapshot};

impl SessionInner {
    pub(crate) fn replace_permissions_with_review(
        &self,
        expected_revision: u64,
        config: PermissionConfig,
        review_client: Option<Arc<dyn Client>>,
    ) -> Result<PolicySnapshot, PermissionError> {
        self.ensure_open()
            .map_err(|error| PermissionError::InvalidConfiguration(error.to_string()))?;
        self.runtime
            .permission_controller()
            .replace_user_with_review(expected_revision, config, review_client)
    }
    /// Inventory comes from this Session's immutable pinned agent and actual registry.
    pub(crate) fn permission_inventory(&self) -> Result<serde_json::Value, PermissionError> {
        let controller = self.runtime.permission_controller();
        let current = controller.snapshot()?;
        let lineage = controller.lineage()?;
        let definitions = self.runtime.definitions();
        let origins = definitions
            .iter()
            .filter_map(|tool| {
                self.runtime
                    .origin(&tool.name)
                    .map(|origin| (tool.name.clone(), origin))
            })
            .collect::<Vec<_>>();
        let tools = definitions
            .iter()
            .map(|tool| serde_json::json!({"name":tool.name,"description":tool.description}))
            .collect::<Vec<_>>();
        let mut modes = Vec::new();
        if let Some(agent) = &self.agent {
            for mode in &agent.modes {
                if matches!(
                    (self.is_main, mode.role),
                    (true, microcrab_agent_types::AgentRole::SubAgent)
                        | (false, microcrab_agent_types::AgentRole::MainAgent)
                ) {
                    continue;
                }
                let policy = controller.inventory_mode(
                    &origins,
                    &mode.permissions,
                    &agent.name,
                    &mode.name,
                )?;
                let compiled = controller.compiled_inventory(&policy, &origins)?;
                modes.push(serde_json::json!({"name":mode.name,"description":mode.description,"prompt":mode.prompt,"current":current.config.mode == mode.name,"tools":tools,"policy":policy.config,"compiled":compiled}));
            }
        } else {
            let compiled = controller.compiled_inventory(&current, &origins)?;
            modes.push(serde_json::json!({"name":current.config.mode,"current":true,"tools":tools,"policy":current.config,"compiled":compiled}));
        }
        controller.ensure_witness(current.revision, &lineage)?;
        let mut initialization = vec![
            serde_json::json!({"revision":current.revision,"agent":current.config.agent,"mode":current.config.mode,"action":current.config.initialization}),
        ];
        initialization.extend(lineage.iter().map(|(_, policy)| serde_json::json!({"revision":policy.revision,"agent":policy.config.agent,"mode":policy.config.mode,"action":policy.config.initialization})));
        Ok(
            serde_json::json!({"agent":current.config.agent,"modes":modes,"initialization":{"kind":"all_of","policies":initialization}}),
        )
    }
    pub(crate) fn replace_permissions(
        &self,
        expected_revision: u64,
        config: PermissionConfig,
    ) -> Result<PolicySnapshot, PermissionError> {
        self.ensure_open()
            .map_err(|error| PermissionError::InvalidConfiguration(error.to_string()))?;
        self.runtime
            .permission_controller()
            .replace_user(expected_revision, config)
    }

    /// Admission is serialized with turns and variable writes. Validate the next
    /// Context and policy before durable commit; a failed commit faults this owner
    /// because its durable effect may be unknown. No pending invocation is replayed.
    pub(crate) async fn replace_external_tools(
        self: &Arc<Self>,
        tools: Vec<Arc<dyn crate::runtime::tool::Tool>>,
    ) -> Result<(), Error> {
        self.ensure_open()?;
        // Validate without replacing the applied registry or an active lease.
        self.runtime.prepare_external_tools(tools.clone())?;
        let mut configured = self.pending_tools.lock().unwrap_or_else(|e| e.into_inner());
        let generation = configured
            .as_ref()
            .map_or(Some(1), |(generation, _)| generation.checked_add(1))
            .ok_or_else(|| Error::config("external catalog configuration generation overflow"))?;
        *configured = Some((generation, tools));
        Ok(())
    }
    pub(super) async fn apply_external_tools(
        &self,
        tools: Vec<Arc<dyn crate::runtime::tool::Tool>>,
    ) -> Result<(), Error> {
        let _fence = self.endpoint.fence().await?;
        let registry = self.runtime.prepare_external_tools(tools)?;
        let mut context = self
            .context
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        context.replace_tools_for_environment(registry.definitions())?;
        let controller = self.runtime.permission_controller();
        let provider = registry.provider_defaults()?;
        let mut state = self.state.recovery_snapshot();
        state.turn_id = None;
        state.round_id = None;
        let guest = self
            .agent
            .as_ref()
            .and_then(|agent| {
                agent
                    .modes
                    .iter()
                    .find(|mode| Some(mode.name.as_str()) == state.selected_mode.as_deref())
            })
            .map(|mode| mode.permissions.as_slice())
            .unwrap_or(&[]);
        controller
            .tool_config(&registry.origins(), &provider, guest)
            .map_err(|error| {
                Error::config_with_private(error.public_reason(), error.to_string())
            })?;
        let task_generation = self.state.executions().generation();
        let recover = crate::runtime::journal::Recover {
            state,
            context: context.snapshot()?,
            continuation: None,
            workspace: None,
            cache_report: None,
            agent: self.agent.clone().map(Into::into),
            prompt_config: Some(self.prompt_config.clone()),
        };
        recover.validate()?;
        let mut guard = FaultOnDrop {
            faulted: &self.faulted,
            armed: true,
        };
        self.commit_recover(recover).await?;
        self.acknowledge_checkpoint(task_generation);
        // User overrides can change while storage commits. Rebase the immutable
        // catalog on the latest user revision without losing a concurrent revoke.
        let mut published = false;
        for _ in 0..3 {
            let policy = controller
                .tool_config(&registry.origins(), &provider, guest)
                .map_err(|error| {
                    Error::config_with_private(error.public_reason(), error.to_string())
                })?;
            match controller.publish_tools(registry.origins(), provider.clone(), policy) {
                Ok(()) => {
                    published = true;
                    break;
                }
                Err(PermissionError::StalePolicy) => continue,
                Err(error) => {
                    return Err(Error::config_with_private(
                        error.public_reason(),
                        error.to_string(),
                    ));
                }
            }
        }
        if !published {
            return Err(Error::config(
                "permission policy changed during tool admission",
            ));
        }
        self.state
            .install_permission(controller.observe(registry.origins()));
        self.runtime.publish_registry(registry);
        self.state
            .install_delegate_availability(self.runtime.delegate_available());
        self.state
            .install_switch_mode_availability(switch_mode_available(
                self.kind,
                context.frozen_tools(),
            ));
        *self
            .context
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = context;
        self.state.end_wasm_epoch();
        self.state.refresh_preview(Boundary::SessionRestored)?;
        guard.armed = false;
        Ok(())
    }
}

impl SessionInner {
    /// Late completions serialize behind active turns. A weak observer cannot keep
    /// the Session alive; close cancels waits but never drops an in-flight commit.
    pub(super) fn start_task_persistence(&self) {
        if self.task_persistence_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let mut changed = self.state.executions().subscribe();
        let weak = self.self_weak.clone();
        let cancellation = self.task_persistence_cancel.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! { biased; _ = cancellation.cancelled() => return, update = changed.changed() => if update.is_err() { return; } }
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                let operation = inner.operation.clone();
                drop(inner);
                let _operation = tokio::select! { biased; _ = cancellation.cancelled() => return, operation = operation.lock_owned() => operation };
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                if inner.closed.load(Ordering::SeqCst) {
                    return;
                }
                // Consume coalesced notifications after admission has settled.
                let generation = *changed.borrow_and_update();
                if generation <= inner.task_persisted_generation.load(Ordering::SeqCst) {
                    continue;
                }
                let context = inner
                    .context
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .clone();
                let continuation = inner
                    .continuation
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .clone();
                if inner.persist(&context, continuation).await.is_err() {
                    inner.faulted.store(true, Ordering::SeqCst);
                    tracing::warn!("tool task result persistence failed; Session faulted");
                    return;
                }
            }
        });
    }
}
