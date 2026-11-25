//! User policy edits and explicit external-catalog epoch admission.
use super::*;
use crate::policy::{
    CurrentPermissionList, CurrentToolPermission, ListBudget, Matcher, PermissionConfig,
    PermissionController, PermissionError, PermissionInitialization, PermissionInventory,
    PermissionInventoryMode, PermissionInventoryRule, PermissionInventoryTool, PermissionList,
    PermissionListError, PermissionListView, PolicySnapshot,
};
use crate::runtime::handler::permissions::AppliedCatalog;

impl SessionInner {
    pub(crate) fn permission_list(
        &self,
        view: PermissionListView,
    ) -> Result<PermissionList, PermissionListError> {
        let controller = self.runtime.permission_controller();
        let (generation, position, catalog, policy) = self.permission_list_position()?;
        let revision = policy.revision;
        let lineage = controller.lineage()?;
        controller.ensure_witness(revision, &lineage)?;
        let mut budget = ListBudget::new();
        let tools = catalog.policy_rows(&policy, &lineage, &mut budget)?;
        let inventory = match view {
            PermissionListView::Current => None,
            PermissionListView::Management => Some(self.permission_management(
                &controller,
                &catalog,
                &policy,
                &lineage,
                &tools,
                &mut budget,
            )?),
        };
        let current = CurrentPermissionList {
            runtime_instance_id: position.0.clone(),
            catalog_revision: catalog.revision().to_string(),
            policy_revision: policy.revision.to_string(),
            ancestor_witness: PermissionController::ancestor_witness(&lineage),
            agent: policy.config.agent.clone(),
            mode: policy.config.mode.clone(),
            tools,
        };
        let list = match inventory {
            Some(tool_inventory) => PermissionList::Management {
                current,
                policy,
                tool_inventory,
            },
            None => PermissionList::Current { current },
        };
        // Include envelope, config, collection delimiters and initialization in
        // the exact byte bound. No response allocation is needed for counting.
        ListBudget::new().charge(&list)?;
        let (latest_generation, latest_position, latest_catalog, latest_policy) =
            self.permission_list_position()?;
        if latest_generation != generation
            || latest_position != position
            || !catalog.matches(&latest_catalog)
            || latest_policy.revision != revision
        {
            return Err(PermissionError::StalePolicy.into());
        }
        controller.ensure_witness(revision, &lineage)?;
        self.ensure_open()
            .map_err(|_| PermissionListError::Unavailable)?;
        if self.replacing.load(Ordering::Acquire) {
            return Err(PermissionListError::Busy);
        }
        Ok(list)
    }

    /// `select_mode_now` can publish outside the turn operation latch. Its
    /// existing mode guard keeps selected State and applied policy coherent.
    /// A regular active turn does not itself exclude a current-list read.
    fn permission_list_position(
        &self,
    ) -> Result<
        (
            u64,
            (String, Option<String>),
            AppliedCatalog,
            PolicySnapshot,
        ),
        PermissionListError,
    > {
        self.ensure_open()
            .map_err(|_| PermissionListError::Unavailable)?;
        let mode = self
            .configured_mode
            .try_lock()
            .map_err(|error| match error {
                std::sync::TryLockError::WouldBlock => PermissionListError::Busy,
                std::sync::TryLockError::Poisoned(_) => PermissionListError::Unavailable,
            })?;
        if self.replacing.load(Ordering::Acquire) {
            return Err(PermissionListError::Busy);
        }
        let position = self.state.permission_position();
        let (catalog, policy) = self.runtime.observe_applied_policy()?;
        if position.1.as_deref().unwrap_or_default() != policy.config.mode {
            return Err(PermissionError::StalePolicy.into());
        }
        self.ensure_open()
            .map_err(|_| PermissionListError::Unavailable)?;
        Ok((mode.0, position, catalog, policy))
    }

    fn permission_management(
        &self,
        controller: &PermissionController,
        catalog: &AppliedCatalog,
        policy: &PolicySnapshot,
        lineage: &[(Arc<PermissionController>, PolicySnapshot)],
        current_rows: &[CurrentToolPermission],
        budget: &mut ListBudget,
    ) -> Result<PermissionInventory, PermissionListError> {
        let mut initialization = Vec::with_capacity(lineage.len() + 1);
        initialization.push(policy.into());
        initialization.extend(lineage.iter().map(|(_, snapshot)| snapshot.into()));
        let origins = catalog.registry().origins();
        let mut modes = Vec::new();
        let mut add_mode = |name: &str,
                            metadata: Option<&microcrab_agent_types::AgentMode>|
         -> Result<(), PermissionListError> {
            let current = name == policy.config.mode;
            let projection = match metadata {
                Some(mode) if !current => controller.inventory_mode(
                    &origins,
                    &mode.permissions,
                    &policy.config.agent,
                    name,
                )?,
                _ => policy.clone(),
            };
            if projection.revision != policy.revision {
                return Err(PermissionError::StalePolicy.into());
            }
            budget.charge(&projection.config)?;
            let mut tools = Vec::new();
            let mut compiled = Vec::new();
            let owners: Vec<_> = std::iter::once(controller.provenance(&projection))
                .chain(
                    lineage
                        .iter()
                        .map(|(owner, snapshot)| owner.provenance(snapshot)),
                )
                .collect();
            for ((name, description), row) in
                catalog.registry().tool_descriptions().zip(current_rows)
            {
                let tool = PermissionInventoryTool {
                    name: name.to_owned(),
                    description: description.to_owned(),
                };
                budget.charge(&tool)?;
                tools.push(tool);
                let compiled_policy = if current {
                    row.policy.clone()
                } else {
                    PermissionController::compile_witness(&projection, lineage, name)?
                };
                let rule = PermissionInventoryRule {
                    tool: Matcher::Exact(name.to_owned()),
                    policy: compiled_policy,
                    owners: owners.clone(),
                };
                budget.charge(&rule)?;
                compiled.push(rule);
            }
            let description = metadata.map(|mode| mode.description.clone());
            let prompt = metadata.map(|mode| mode.prompt.clone());
            budget.charge(&description)?;
            budget.charge(&prompt)?;
            modes.push(PermissionInventoryMode {
                name: name.to_owned(),
                description,
                prompt,
                current,
                tools,
                policy: projection.config,
                compiled,
            });
            Ok(())
        };
        let mut found_current = false;
        if let Some(agent) = &self.agent {
            for mode in &agent.modes {
                if crate::session::mode_role_eligible(self.is_main, mode.role) {
                    found_current |= policy.config.mode == mode.name;
                    add_mode(&mode.name, Some(mode))?;
                }
            }
        }
        if !found_current {
            add_mode(&policy.config.mode, None)?;
        }
        Ok(PermissionInventory {
            agent: policy.config.agent.clone(),
            modes,
            initialization: PermissionInitialization::AllOf {
                policies: initialization,
            },
        })
    }

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
        let registry = self.runtime.prepare_external_tools(tools.clone())?;
        if let Some(owner) = &self.model_admission {
            registry.validate_model_owner(owner.as_ref())?;
        }
        let mut configured = self.pending_tools.lock().unwrap_or_else(|e| e.into_inner());
        let generation = configured
            .as_ref()
            .map_or(Some(1), |(generation, _)| generation.checked_add(1))
            .ok_or_else(|| Error::config("external catalog configuration generation overflow"))?;
        *configured = Some((generation, tools));
        Ok(())
    }
    pub(super) async fn apply_catalog(
        &self,
        registry: Arc<crate::runtime::tool::ToolRegistry>,
        context: Context,
    ) -> Result<(), Error> {
        let _fence = self.endpoint.fence().await?;
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
            match self
                .runtime
                .publish_catalog(registry.clone(), provider.clone(), policy)
            {
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
            .set_model_permission_names(context.frozen_tools());
        self.state
            .set_short_model_refs(crate::runtime::tool::short_model_refs(
                context.frozen_tools(),
            ));
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
