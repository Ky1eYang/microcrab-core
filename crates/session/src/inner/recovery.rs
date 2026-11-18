//! Settled conversation recovery and explicit guest-selected round cache evidence.
use super::*;
use crate::runtime::handler::RoundRecorder;
use crate::runtime::journal::{
    AdmissionCheckpoint, NavigationBoundary, Recover, RoundContinuation,
};

impl SessionInner {
    /// Route evidence is written before the Journal effect and acknowledged only
    /// after its durable cursor. Interrupted publication is quarantined by callers.
    pub(super) async fn commit_recover(&self, recover: Recover) -> Result<(), Error> {
        self.commit_recover_with_boundary(recover, None).await
    }
    async fn commit_recover_with_boundary(
        &self,
        recover: Recover,
        boundary: Option<NavigationBoundary>,
    ) -> Result<(), Error> {
        self.commit_recover_provenance(recover, boundary, None)
            .await
    }
    async fn commit_recover_provenance(
        &self,
        recover: Recover,
        boundary: Option<NavigationBoundary>,
        admission: Option<AdmissionCheckpoint>,
    ) -> Result<(), Error> {
        if let Some(route) = &self.route {
            route.before_checkpoint(&recover).await?;
        }
        match (boundary, admission) {
            (None, Some(admission)) => {
                self.journal
                    .commit_admission(recover.clone(), admission)
                    .await?
            }
            (Some(boundary), None) => {
                self.journal
                    .commit_boundary(recover.clone(), boundary)
                    .await?
            }
            (None, None) => self.journal.commit(recover.clone()).await?,
            (Some(_), Some(_)) => {
                return Err(Error::protocol("admission cannot certify navigation"));
            }
        }
        self.remember_durable_carrier_consumption(&recover)?;
        if let Some(route) = &self.route {
            route.checkpoint_committed(&recover).await?;
        }
        Ok(())
    }
    pub(super) fn acknowledge_checkpoint(&self, generation: u64) {
        self.selected_checkpoint
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        self.task_persisted_generation
            .fetch_max(generation, Ordering::SeqCst);
    }

    fn acknowledge_recovered_checkpoint(&self, recover: &Recover) {
        let generation = self.state.executions().generation();
        self.task_persisted_generation
            .fetch_max(generation, Ordering::SeqCst);
        *self
            .selected_checkpoint
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some((generation, recover.clone()));
    }

    pub(super) async fn persist_terminal(&self) -> Result<(), Error> {
        if self.kind == crate::SessionKind::OneShot {
            return Ok(());
        }
        if self.faulted.load(Ordering::SeqCst) {
            return Ok(());
        }
        let _fence = self.endpoint.terminal_fence().await?;
        let context = self
            .context
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let continuation = self
            .continuation
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let task_generation = self.state.executions().generation();
        let mut state = self.state.recovery_snapshot();
        if continuation.is_none() {
            state.turn_id = None;
            state.round_id = None;
        }
        let recover = Recover {
            state,
            context: context.snapshot()?,
            continuation,
            workspace: None,
            cache_report: self
                .cache_report
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone(),
            agent: self.agent.clone().map(Into::into),
            prompt_config: Some(self.prompt_config.clone()),
        };
        recover.validate()?;
        self.commit_recover(recover).await?;
        self.acknowledge_checkpoint(task_generation);
        Ok(())
    }

    pub(crate) fn retry_ready(&self) -> bool {
        !self.faulted.load(Ordering::SeqCst)
            && !self.closed.load(Ordering::SeqCst)
            && self
                .continuation
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .as_ref()
                .is_some_and(|value| value.request_prepared || value.state_observation_pending)
    }

    pub(crate) fn has_continuation(&self) -> bool {
        self.continuation
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_some()
    }

    pub(super) async fn persist_initial(&self, context: &Context) -> Result<Recover, Error> {
        let _fence = self.endpoint.fence().await?;
        let mut state = self.state.recovery_snapshot();
        state.turn_id = None;
        state.round_id = None;
        let recover = Recover {
            state,
            context: context.snapshot()?,
            continuation: None,
            workspace: None,
            cache_report: None,
            agent: self.agent.clone().map(Into::into),
            prompt_config: Some(self.prompt_config.clone()),
        };
        recover.validate()?;
        let mut commit_guard = FaultOnDrop {
            faulted: &self.faulted,
            armed: true,
        };
        self.commit_recover_with_boundary(recover.clone(), Some(NavigationBoundary::EpochInitial))
            .await?;
        self.acknowledge_recovered_checkpoint(&recover);
        *self
            .navigation_epoch
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(context.epoch());
        commit_guard.armed = false;
        Ok(recover)
    }

    pub(super) async fn persist(
        &self,
        context: &Context,
        continuation: Option<RoundContinuation>,
    ) -> Result<Recover, Error> {
        self.persist_checkpoint(context, continuation, false, false, None)
            .await
    }

    pub(super) async fn persist_inter_turn(&self, context: &Context) -> Result<Recover, Error> {
        self.persist_checkpoint(
            context,
            None,
            true,
            false,
            Some(NavigationBoundary::InterTurn),
        )
        .await
    }

    pub(super) async fn persist_agent_initialization_complete(
        &self,
        context: &Context,
        continuation: Option<RoundContinuation>,
    ) -> Result<(), Error> {
        self.persist_checkpoint(context, continuation, false, true, None)
            .await?;
        Ok(())
    }

    async fn persist_checkpoint(
        &self,
        context: &Context,
        continuation: Option<RoundContinuation>,
        preserve_selection: bool,
        complete_agent_initialization: bool,
        boundary: Option<NavigationBoundary>,
    ) -> Result<Recover, Error> {
        let started = std::time::Instant::now();
        let _fence = self.endpoint.fence().await?;
        // Only explicitly selected observations are summarized. Checkpoint/close
        // never enumerate the workspace or infer files changed by shell commands.
        if let Some(report) = self.state.cache_points().finish() {
            *self
                .cache_report
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = Some(report);
        }
        let cache_report = self
            .cache_report
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let point_count = cache_report
            .as_ref()
            .map_or(0, |report| report.points.len());
        let cache_bytes: usize = cache_report.as_ref().map_or(0, |report| {
            report
                .points
                .iter()
                .map(|point| point.point.data.as_ref().map_or(0, Vec::len))
                .sum()
        });
        let task_generation = self.state.executions().generation();
        let mut state = self.state.recovery_snapshot();
        if complete_agent_initialization {
            state.agent_initialization_pending = false;
        }
        if continuation.is_none() {
            state.turn_id = None;
            state.round_id = None;
        }
        let recover = Recover {
            state,
            context: context.snapshot()?,
            continuation: continuation.clone(),
            workspace: None,
            cache_report,
            agent: self.agent.clone().map(Into::into),
            prompt_config: Some(self.prompt_config.clone()),
        };
        recover.validate()?;
        if preserve_selection {
            let selected = self
                .selected_checkpoint
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some((generation, historical)) = selected.as_ref()
                && *generation == task_generation
            {
                let mut comparable = serde_json::to_value(&recover)?;
                let mut selected_bytes = serde_json::to_value(historical)?;
                // Only the retained live observation overlay may differ.
                // All conversation, custom State and cache bytes must match.
                comparable["state"]
                    .as_object_mut()
                    .ok_or_else(|| Error::protocol("invalid checkpoint State"))?
                    .remove("executions");
                selected_bytes["state"]
                    .as_object_mut()
                    .ok_or_else(|| Error::protocol("invalid selected State"))?
                    .remove("executions");
                if comparable == selected_bytes {
                    return Ok(historical.clone());
                }
            }
        }
        // A dropped adapter future may have committed; quarantine unknown outcomes.
        let mut commit_guard = FaultOnDrop {
            faulted: &self.faulted,
            armed: true,
        };
        let result = self
            .commit_recover_with_boundary(recover.clone(), boundary)
            .await;
        tracing::info!(
            cache_point_count = point_count,
            cache_bytes,
            elapsed_ms = started.elapsed().as_millis() as u64,
            succeeded = result.is_ok(),
            "session round persistence settled"
        );
        result?;
        if boundary.is_some() {
            self.acknowledge_recovered_checkpoint(&recover);
        } else {
            self.acknowledge_checkpoint(task_generation);
        }
        *self
            .continuation
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = continuation;
        if complete_agent_initialization {
            self.state
                .update(|state| state.agent_initialization_pending = false);
        }
        commit_guard.armed = false;
        Ok(recover)
    }

    pub(crate) async fn checkpoint(self: &Arc<Self>) -> Result<Recover, Error> {
        self.checkpoint_impl(false).await
    }

    pub(crate) async fn navigation_checkpoint(self: &Arc<Self>) -> Result<Recover, Error> {
        self.checkpoint_impl(true).await
    }

    async fn checkpoint_impl(self: &Arc<Self>, navigation: bool) -> Result<Recover, Error> {
        let operation = self.reserve().await?;
        let inner = self.clone();
        tokio::spawn(async move {
            let _operation = operation;
            inner.ensure_open()?;
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
            if !navigation {
                return inner
                    .persist_checkpoint(&context, continuation, true, false, None)
                    .await;
            }
            if continuation.is_some() || inner.state().turn_id.is_some() || context.pending_epoch()
            {
                return Err(Error::config(
                    "conversation checkpoint requires a settled Turn and materialized epoch",
                ));
            }
            let initial_required = *inner
                .navigation_epoch
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                != Some(context.epoch());
            if initial_required {
                return inner.persist_initial(&context).await;
            }
            if inner.state().last_turn_id() == 0 {
                return inner
                    .persist_checkpoint(
                        &context,
                        None,
                        true,
                        false,
                        Some(NavigationBoundary::EpochInitial),
                    )
                    .await;
            }
            inner.persist_inter_turn(&context).await
        })
        .await
        .map_err(|_| Error::tool("checkpoint worker failed; recover from durable state"))?
    }

    fn validate_recover(&self, recover: &Recover) -> Result<Context, Error> {
        self.validate_recover_catalog(recover, &self.runtime.definitions())
    }
    fn validate_recover_catalog(
        &self,
        recover: &Recover,
        definitions: &[microcrab_protocol::ToolDefinition],
    ) -> Result<Context, Error> {
        recover.validate()?;
        if recover.prompt_config.clone().unwrap_or_default().resolved() != self.prompt_config {
            return Err(Error::config(
                "recovery prompt configuration differs from the frozen Session",
            ));
        }
        if recover
            .state
            .session_kind
            .is_some_and(|kind| kind != self.kind)
        {
            return Err(Error::config(
                "recovery Session kind differs from the configured lifecycle",
            ));
        }
        if let Some(relation) = &recover.state.parent_relationship
            && (self.kind != crate::SessionKind::Delegated
                || self.process_parent_session_id.as_deref()
                    != Some(relation.parent_session_id.as_str()))
        {
            return Err(Error::config("invalid recovered parent relationship"));
        }
        let configured = self
            .configured_limits
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let restored = &recover.state.limits;
        if restored.max_processes != configured.max_processes
            || restored.max_execution_buffers != configured.max_execution_buffers
            || restored.max_retained_processes != configured.max_retained_processes
            || restored.output_buffer_bytes != configured.output_buffer_bytes
            || restored.sleep_max_ms != configured.sleep_max_ms
            || restored.max_pending_executions != configured.max_pending_executions
            || restored.max_retained_executions != configured.max_retained_executions
            || restored.interaction_timeout_ms != configured.interaction_timeout_ms
        {
            return Err(Error::config(
                "recovery requires compatible configured process, execution, output and sleep budgets",
            ));
        }
        self.state.executions().validate_recovery_limits(restored)?;
        self.state()
            .references
            .validate_variables(&recover.state.variables)?;
        if recover.context.tools.iter().any(|frozen| {
            frozen.name == "job" && !definitions.iter().any(|installed| installed == frozen)
        }) {
            return Err(Error::config(
                "restored job catalog is unsupported; create a new Session with current tool definitions",
            ));
        }
        let exact_frozen = recover.context.tools.iter().all(|frozen| {
            definitions.iter().any(|installed| {
                installed == frozen
                    || (frozen.name == "expand"
                        && self.runtime.origin(&frozen.name)
                            == Some(crate::runtime::tool::ToolOrigin::Builtin)
                        && Self::prior_expand_definition(frozen, installed))
                    || (frozen.name == "delegate"
                        && self.runtime.origin(&frozen.name)
                            == Some(crate::runtime::tool::ToolOrigin::Builtin)
                        && Self::prior_delegate_definition(frozen, installed))
            })
        });
        let extras_are_native = definitions
            .iter()
            .filter(|installed| {
                !recover
                    .context
                    .tools
                    .iter()
                    .any(|frozen| frozen.name == installed.name)
            })
            .all(|extra| {
                self.runtime.origin(&extra.name) == Some(crate::runtime::tool::ToolOrigin::Builtin)
            });
        if !exact_frozen || !extras_are_native {
            return Err(Error::config(
                "restored epoch requires different tool implementations",
            ));
        }
        let same_agent = match (&self.agent, &recover.agent) {
            (None, None) => true,
            (Some(expected), Some(actual)) => {
                let actual = actual.current()?;
                expected.name == actual.name
                    && expected.modes == actual.modes
                    && expected.tools == actual.tools
                    && expected.variables == actual.variables
            }
            _ => false,
        };
        if !same_agent {
            return Err(Error::config("recovery requires compatible agent metadata"));
        }
        crate::budget::check(
            crate::LimitKind::RecoveryBytes,
            recover.state.limits.max_context_bytes,
            self.state().limits.max_context_bytes,
        )?;
        if let Some(mode) = recover.state.selected_mode.as_deref()
            && self
                .agent
                .as_ref()
                .is_none_or(|agent| !agent.modes.iter().any(|entry| entry.name == mode))
        {
            return Err(Error::config("unknown recovered agent mode"));
        }
        if recover.workspace.is_some() {
            tracing::warn!("legacy full-workspace snapshot is inert; restoring conversation only");
        }
        if let Some(mode) = recover.state.selected_mode.as_deref() {
            self.validate_mode(mode)?;
        }
        if self
            .state()
            .communication
            .as_ref()
            .map(|value| &value.identity)
            != recover
                .state
                .communication
                .as_ref()
                .map(|value| &value.identity)
        {
            return Err(Error::config(
                "recovery cannot replace or upgrade the Session communication identity",
            ));
        }
        Context::from_snapshot(recover.context.clone())
    }

    pub(crate) async fn install_initial(&self, recover: Recover) -> Result<(), Error> {
        let context = self.validate_recover(&recover)?;
        if let Some(route) = &self.route {
            route.validate_restore(&recover).await?;
        }
        let refresh = self.refresh_required(&recover);
        let selected_history = self.journal.has_selected_history().await;
        let (recover, context) = if refresh {
            self.refresh_live_recover(&recover, context)?
        } else {
            (recover, context)
        };
        *self
            .navigation_epoch
            .lock()
            .unwrap_or_else(|error| error.into_inner()) =
            (selected_history && !refresh).then_some(context.epoch());
        self.install_policy_mode(recover.state.selected_mode.as_deref())?;
        self.bind_frozen_tools(&recover.context.tools);
        self.state.executions().release_for_restore().await?;
        self.state.executions().restore(&recover.state.executions)?;
        if let Some(route) = &self.route {
            route.restore(&recover).await?;
        }
        // Initial attachment has not published a Journal selection. A fresh fork
        // still needs its first checkpoint; only suppress restoration's own
        // execution notification here, without a selected-checkpoint witness.
        self.task_persisted_generation
            .fetch_max(self.state.executions().generation(), Ordering::SeqCst);
        *self
            .parent_relationship
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = recover.state.parent_relationship.clone();
        let focus = recover
            .state
            .communication
            .as_ref()
            .filter(|value| value.identity.depth == 1)
            .map(|value| value.focus.clone());
        self.remember_durable_carrier_consumption(&recover)?;
        self.state.replace(recover.state);
        if let Some(focus) = focus {
            self.state.install_focused_references(focus)?;
        }
        self.reset_communication_endpoints()?;
        *self
            .context
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = context;
        *self
            .continuation
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = recover.continuation;
        *self
            .cache_report
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = recover.cache_report;
        self.restored.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn prior_delegate_definition(
        frozen: &microcrab_protocol::ToolDefinition,
        installed: &microcrab_protocol::ToolDefinition,
    ) -> bool {
        let mut previous = installed.clone();
        if let Some(properties) = previous.parameters["properties"].as_object_mut() {
            properties.remove("interaction");
            properties.remove("references");
            if let Some(input) = properties.get_mut("input") {
                input["description"]=Value::String("Task instructions including all context the child needs. Required, nonempty and not whitespace-only; null is invalid.".into());
            }
        }
        if &previous == frozen {
            return true;
        }
        previous.parameters["required"] = serde_json::json!(["target", "input"]);
        if let Some(parameters) = previous.parameters.as_object_mut() {
            parameters.remove("allOf");
        }
        if let Some(properties) = previous.parameters["properties"].as_object_mut() {
            properties.remove("action");
            properties.remove("child_id");
        }
        if let Some(description) =
            previous.parameters["properties"]["target"]["description"].as_str()
        {
            previous.parameters["properties"]["target"]["description"] =
                Value::String(description.replace("<delegatable_agents>", "<subagents>"));
        }
        &previous == frozen
    }

    fn prior_expand_definition(
        frozen: &microcrab_protocol::ToolDefinition,
        installed: &microcrab_protocol::ToolDefinition,
    ) -> bool {
        let mut previous = installed.clone();
        previous.parameters["oneOf"] = serde_json::json!([
            {"required":["ids"]}, {"required":["reference"]}
        ]);
        frozen == &previous
    }

    fn refresh_required(&self, recover: &Recover) -> bool {
        self.route
            .as_ref()
            .is_some_and(|route| route.refresh_context_on_restore())
            || recover
                .context
                .tools
                .iter()
                .any(|tool| tool.name == "expand" && tool.parameters.get("oneOf").is_some())
    }

    fn refresh_live_recover(
        &self,
        original: &Recover,
        mut context: Context,
    ) -> Result<(Recover, Context), Error> {
        // Upgrade only installed builtin names already present in the frozen
        // catalog. External/client tool definitions retain their exact authority;
        // the Context owner archives the old catalog for historical call checks.
        let installed = self.runtime.definitions();
        let tools = original
            .context
            .tools
            .iter()
            .map(|frozen| {
                if self.runtime.origin(&frozen.name)
                    == Some(crate::runtime::tool::ToolOrigin::Builtin)
                {
                    let mut tool = installed
                        .iter()
                        .find(|tool| tool.name == frozen.name)
                        .cloned()
                        .unwrap_or_else(|| frozen.clone());
                    tool.parameters.sort_all_objects();
                    tool
                } else {
                    frozen.clone()
                }
            })
            .collect();
        context.replace_tools_for_recovery(tools)?;
        context.refresh_recovery_epoch()?;
        let mut live = original.clone();
        live.context = context.snapshot()?;
        live.state.epoch_id = context.epoch();
        live.validate()?;
        Ok((live, context))
    }

    pub(crate) async fn recover(self: &Arc<Self>, recover: Recover) -> Result<(), Error> {
        self.recover_impl(recover, None).await
    }
    pub(crate) async fn recover_with_external_tools(
        self: &Arc<Self>,
        recover: Recover,
        tools: Vec<Arc<dyn crate::runtime::tool::Tool>>,
    ) -> Result<(), Error> {
        self.recover_impl(recover, Some(tools)).await
    }
    async fn recover_impl(
        self: &Arc<Self>,
        recover: Recover,
        tools: Option<Vec<Arc<dyn crate::runtime::tool::Tool>>>,
    ) -> Result<(), Error> {
        // Reject malformed/incompatible requests before interrupting live work.
        recover.validate()?;
        if let Some(tools) = &tools {
            let registry = self.runtime.prepare_external_tools(tools.clone())?;
            self.validate_recover_catalog(&recover, &registry.definitions())?;
        } else {
            self.validate_recover(&recover)?;
        }
        let issued_mode_generation = self
            .configured_mode
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .0;
        if let Some(route) = &self.route {
            route.validate_restore(&recover).await?;
        }
        if self.refresh_required(&recover) {
            let context = Context::from_snapshot(recover.context.clone())?;
            self.refresh_live_recover(&recover, context)?;
        }
        let operation = self.reserve_replacement().await?;
        let inner = self.clone();
        tokio::spawn(async move {
            let _operation = operation;
            inner.ensure_open()?;
            // Producers outlive foreground turns. Await their actual cleanup
            // acknowledgement before publishing any historical selection or policy;
            // failed cleanup retains its owner and prevents Journal publication.
            inner.state.executions().quiesce_for_restore().await?;
            inner
                .processes
                .quiesce_session_for_restore(inner.state.processes().session_id())
                .await?;
            let token = CancellationToken::new();
            inner.register_replacement_token(&token)?;
            let _active = TurnGuard {
                inner: inner.clone(),
                clear: false,
            };
            let _cancel = CancelOnDrop(token.clone());
            inner.ensure_open()?;
            let registry = tools
                .map(|tools| inner.runtime.prepare_external_tools(tools))
                .transpose()?;
            let context = match &registry {
                Some(registry) => {
                    inner.validate_recover_catalog(&recover, &registry.definitions())?
                }
                None => inner.validate_recover(&recover)?,
            };
            let selected_mode = recover.state.selected_mode.as_deref();
            let guest = inner
                .agent
                .as_ref()
                .and_then(|agent| {
                    agent
                        .modes
                        .iter()
                        .find(|mode| Some(mode.name.as_str()) == selected_mode)
                })
                .map(|mode| mode.permissions.as_slice())
                .unwrap_or(&[]);
            let agent_name = inner
                .agent
                .as_ref()
                .map(|agent| agent.name.as_str())
                .unwrap_or("");
            let provider = registry
                .as_ref()
                .map(|registry| registry.provider_defaults())
                .transpose()?;
            if let (Some(registry), Some(provider)) = (&registry, &provider) {
                inner
                    .runtime
                    .permission_controller()
                    .tool_config_for_mode(
                        &registry.origins(),
                        provider,
                        guest,
                        agent_name,
                        selected_mode.unwrap_or(""),
                    )
                    .map_err(|error| {
                        Error::config_with_private(error.public_reason(), error.to_string())
                    })?;
            }
            let _communication = inner.communication_transaction.lock().await;
            let _fence = inner.endpoint.fence().await?;
            inner.state.executions().release_for_restore().await?;
            // History selection restores conversation, never filesystem bytes.
            // The creator's selected node stays byte-for-byte intact; subsequent
            // new checkpoints omit its inert legacy snapshot.
            let mut transition = FaultOnDrop {
                faulted: &inner.faulted,
                armed: true,
            };
            // A history selection is authoritative even if its task observations
            // are historical. A queued old notification must not create a new
            // branch checkpoint after that explicit selection. Later real task
            // transitions have a newer generation and still persist normally.
            if let Some(route) = &inner.route {
                route.validate_restore(&recover).await?;
            }
            let (live_recover, context) = if inner.refresh_required(&recover) {
                inner.refresh_live_recover(&recover, context)?
            } else {
                (recover.clone(), context)
            };
            inner.flush_durable_carrier_consumption_locked().await?;
            inner.journal.recovered(recover.clone()).await?;
            inner.remember_durable_carrier_consumption(&recover)?;
            let selected_recover = recover;
            let recover = live_recover;
            if let Some(route) = &inner.route {
                route.restore(&recover).await?;
            }
            {
                let mut configured_mode = inner
                    .configured_mode
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                let live_mode = (configured_mode.0 != issued_mode_generation)
                    .then(|| configured_mode.1.clone())
                    .flatten();
                let effective_mode = live_mode
                    .as_deref()
                    .or(recover.state.selected_mode.as_deref());
                let effective_guest = inner
                    .agent
                    .as_ref()
                    .and_then(|agent| {
                        agent
                            .modes
                            .iter()
                            .find(|mode| Some(mode.name.as_str()) == effective_mode)
                    })
                    .map(|mode| mode.permissions.as_slice())
                    .unwrap_or(&[]);
                if let (Some(registry), Some(provider)) = (registry, provider) {
                    let controller = inner.runtime.permission_controller();
                    let mut published = false;
                    for _ in 0..3 {
                        let snapshot = controller
                            .tool_config_for_mode(
                                &registry.origins(),
                                &provider,
                                effective_guest,
                                agent_name,
                                effective_mode.unwrap_or(""),
                            )
                            .map_err(|error| {
                                Error::config_with_private(error.public_reason(), error.to_string())
                            })?;
                        match controller.publish_tools(
                            registry.origins(),
                            provider.clone(),
                            snapshot,
                        ) {
                            Ok(()) => {
                                published = true;
                                break;
                            }
                            Err(crate::policy::PermissionError::StalePolicy) => continue,
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
                            "permission changed during recovered tool publication",
                        ));
                    }
                    inner
                        .state
                        .install_permission(controller.observe(registry.origins()));
                    inner.runtime.publish_registry(registry);
                } else {
                    inner.install_policy_mode(effective_mode)?;
                }
                inner.bind_frozen_tools(&recover.context.tools);
                inner
                    .state
                    .executions()
                    .restore(&recover.state.executions)?;
                // Registry restoration publishes an observation change. It belongs
                // to this selection, not a new execution checkpoint or undo branch.
                // The endpoint fence prevents persistence before this acknowledgment.
                inner.acknowledge_recovered_checkpoint(&selected_recover);
                *inner
                    .navigation_epoch
                    .lock()
                    .unwrap_or_else(|error| error.into_inner()) = (recover.context.epoch
                    == selected_recover.context.epoch)
                    .then_some(recover.context.epoch);
                *inner
                    .parent_relationship
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()) = recover.state.parent_relationship.clone();
                let focus = recover
                    .state
                    .communication
                    .as_ref()
                    .filter(|value| value.identity.depth == 1)
                    .map(|value| value.focus.clone());
                inner.state.replace(recover.state);
                if let Some(focus) = focus {
                    inner.state.install_focused_references(focus)?;
                }
                inner.reset_communication_endpoints()?;
                if let Some(mode) = live_mode.as_ref() {
                    inner
                        .state
                        .update(|state| state.selected_mode = Some(mode.clone()));
                } else {
                    configured_mode.1 = None;
                }
            }
            inner.restore_communication_inbox_locked().await?;
            inner.applied_tools.store(0, Ordering::Release);
            *inner
                .context
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = context;
            *inner
                .continuation
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = recover.continuation;
            *inner
                .cache_report
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = recover.cache_report;
            inner.restored.store(true, Ordering::SeqCst);
            drop(_fence);
            drop(_communication);
            inner.load_references(&token).await?;
            inner
                .boundary(Boundary::SessionRestored, None, &token)
                .await?;
            inner.started.store(true, Ordering::SeqCst);
            transition.armed = false;
            Ok(())
        })
        .await
        .map_err(|_| Error::tool("recovery worker failed; recover from durable state"))?
    }
}

impl RoundRecorder for SessionInner {
    fn set_model_retry_ready(&self, ready: bool) {
        self.model_retry_ready.store(ready, Ordering::Release);
    }
    async fn wait_for_steer(&self) {
        self.inbox.wait_for_steer().await;
    }
    fn request_route(&self, content: &crate::context::Content) -> Result<Option<String>, Error> {
        let configured = self
            .auto_compaction
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let Some((policy, estimator)) = configured else {
            return Ok(None);
        };
        if !policy.enabled {
            return Ok(None);
        }
        // Route resolution does not depend on whether the request contains an
        // image whose cost the text heuristic cannot price.
        let mut probe = content.clone();
        probe.items.clear();
        Ok(estimator.estimate(&probe)?.map(|estimate| estimate.route))
    }
    async fn compact_boundary(
        &self,
        context: &mut Context,
        continuation: Option<RoundContinuation>,
        final_round: bool,
        cancellation: &CancellationToken,
    ) -> Result<(), Error> {
        self.auto_compact_settled(context, continuation, final_round, cancellation)
            .await
    }
    fn round_exhaustion(&self) -> crate::budget::Exhaustion {
        self.round_limit_callback
            .as_ref()
            .map_or(crate::budget::Exhaustion::Stop, |callback| {
                crate::budget::Exhaustion::Ask(callback.clone())
            })
    }
    fn stop_at_round_limit(&self) {
        self.inbox
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .downgrade_framework();
    }
    async fn admit_step(
        &self,
        _: &State,
        context: &mut Context,
        allow_framework: bool,
        sending: bool,
        round: usize,
        _: &dyn Auditor,
        cancellation: &CancellationToken,
    ) -> Result<crate::runtime::handler::StepAdmission, Error> {
        self.admit_inbox_step(context, allow_framework, sending, round, cancellation)
            .await
    }
    fn observe_context(&self, context: &Context) -> Result<(), Error> {
        let measurement = context.measure()?;
        {
            // The foreground owns mutation of its local Context. Publish only
            // admitted material, together with its measurement, for live reads.
            // Match with_context_measurement's lock order; never hold either
            // owner across IO or publish a synthetic recovery/task receipt here.
            let mut live = self
                .live_context_measurement
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let mut visible = self
                .context
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            *visible = context.clone();
            *live = Some(measurement);
        }
        self.context_measurement_changes.send_replace(());
        Ok(())
    }
    async fn record_task_admission(&self, context: &Context) -> Result<(), Error> {
        // Foreground owns the operation reservation. Preserve the generated
        // response with real task receipts, and explicitly mark not-yet-admitted
        // siblings as unexecuted. Restart has no automatic model continuation or
        // tool replay, even if the previous settled round had a continuation.
        let _fence = self.endpoint.fence().await?;
        let tasks = self.state.executions();
        let rows = tasks.snapshot();
        let admission = AdmissionCheckpoint::from_context(context)?;
        let mut context = context.clone();
        for call_id in context.pending_call_ids() {
            let result = rows
                .iter()
                .find(|row| row.call_id == call_id)
                .map(|row| {
                    tasks.receipt(row.execution_id, false).unwrap_or_else(|_| {
                        "{\"error\":\"execution admitted; inspect its history before retrying\"}".into()
                    })
                })
                .unwrap_or_else(|| {
                    "{\"error\":\"not executed; interrupted before invocation admission\"}".into()
                });
            context.append_tool_result(call_id, result)?;
        }
        let task_generation = self.state.executions().generation();
        let mut state = self.state.recovery_snapshot();
        state.turn_id = None;
        state.round_id = None;
        let recover = Recover {
            state,
            context: context.snapshot()?,
            continuation: None,
            workspace: None,
            cache_report: self
                .cache_report
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone(),
            agent: self.agent.clone().map(Into::into),
            prompt_config: Some(self.prompt_config.clone()),
        };
        recover.validate()?;
        admission.validate(&recover)?;
        let mut guard = FaultOnDrop {
            faulted: &self.faulted,
            armed: true,
        };
        self.commit_recover_provenance(recover, None, Some(admission))
            .await?;
        self.acknowledge_checkpoint(task_generation);
        guard.armed = false;
        Ok(())
    }
    async fn record(
        &self,
        context: &Context,
        continuation: Option<RoundContinuation>,
    ) -> Result<(), Error> {
        self.persist(context, continuation).await.map(|_| ())
    }
}
