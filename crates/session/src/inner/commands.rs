//! Serialize user commands while the independent Session variable pump serves plugin replies.
#[cfg(test)]
pub(super) mod tests;
use super::*;

/// One operation owns its token and its first explicit interrupt cause. A caller
/// token cancelled without an explicit Session interrupt has no attributed cause.
#[derive(Clone)]
pub(super) struct ActiveOperation {
    pub(super) token: CancellationToken,
    pub(super) reason: Arc<Mutex<Option<crate::InterruptReason>>>,
}
impl ActiveOperation {
    pub(super) fn command(token: &CancellationToken) -> Self {
        Self {
            token: token.clone(),
            reason: Arc::new(Mutex::new(None)),
        }
    }
    fn interrupt(&self, reason: crate::InterruptReason) {
        let mut first = self.reason.lock().unwrap_or_else(|e| e.into_inner());
        // Cancellation already observed from the caller/consumer remains unknown.
        if !self.token.is_cancelled() && first.is_none() {
            *first = Some(reason);
        }
        self.token.cancel();
    }
}

impl SessionInner {
    /// Close seals before acquiring `active` in interrupt. Check that seal while
    /// holding the same mutex so it either rejects registration or cancels its token.
    pub(super) fn register_command_token(&self, token: &CancellationToken) -> Result<(), Error> {
        self.register_token(token, false)
    }
    fn register_token(&self, token: &CancellationToken, replacement: bool) -> Result<(), Error> {
        #[cfg(test)]
        {
            let gate = self
                .command_registration_gate
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone();
            if let Some(gate) = gate {
                gate.wait();
            }
        }
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.ensure_open()?;
        if !replacement && self.replacing.load(Ordering::Acquire) {
            token.cancel();
            return Err(Error::Cancelled);
        }
        *active = Some(ActiveOperation::command(token));
        Ok(())
    }
    pub(super) fn register_replacement_token(
        &self,
        token: &CancellationToken,
    ) -> Result<(), Error> {
        self.register_token(token, true)
    }
    pub(crate) async fn start(self: &Arc<Self>) -> Result<(), Error> {
        self.ensure_open()?;
        let _operation = self.reserve().await?;
        self.ensure_open()?;
        let token = CancellationToken::new();
        self.register_command_token(&token)?;
        let _active = TurnGuard {
            inner: self.clone(),
            clear: false,
        };
        let _cancel = CancelOnDrop(token.clone());
        self.initialize(&token).await
    }

    pub(crate) async fn refresh_references(self: &Arc<Self>) -> Result<(), Error> {
        self.ensure_open()?;
        let _operation = self.reserve().await?;
        self.ensure_open()?;
        let token = CancellationToken::new();
        self.register_command_token(&token)?;
        let _active = TurnGuard {
            inner: self.clone(),
            clear: false,
        };
        let _cancel = CancelOnDrop(token.clone());
        if self.started.load(Ordering::SeqCst) {
            self.load_references(&token).await?;
            self.state.refresh_reference_projection()
        } else {
            self.initialize(&token).await
        }
    }

    /// Reference callbacks may await arbitrary embedder IO. Bound the entire load
    /// batch and drop its future on cancellation; failed pre-admission loading is
    /// retryable, while a caller that already published recovery keeps its fault guard.
    pub(super) async fn load_references(&self, token: &CancellationToken) -> Result<(), Error> {
        self.ensure_open()?;
        let deadline = std::time::Duration::from_millis(self.state().limits.lifecycle_timeout_ms);
        tokio::select! {
            biased;
            _ = token.cancelled() => Err(Error::Cancelled),
            result = tokio::time::timeout(deadline, self.state.load_references()) => {
                result.map_err(|_| Error::lifecycle("reference loading deadline"))?
            }
        }
    }
    pub(super) async fn initialize(&self, token: &CancellationToken) -> Result<(), Error> {
        self.initialize_with_retry(token, false).await
    }
    async fn initialize_with_retry(
        &self,
        token: &CancellationToken,
        retry: bool,
    ) -> Result<(), Error> {
        self.start_task_persistence();
        if self.started.load(Ordering::SeqCst) {
            return if retry {
                self.ensure_agent_ready_impl(token, true).await
            } else {
                self.agent_initialization_status()
            };
        }
        self.load_references(token).await?;
        let mut transition = FaultOnDrop {
            faulted: &self.faulted,
            armed: true,
        };
        let restored = self.restored.load(Ordering::SeqCst);
        self.boundary(
            if restored {
                Boundary::SessionRestored
            } else {
                Boundary::SessionCreated
            },
            None,
            token,
        )
        .await?;
        if !restored {
            self.boundary(Boundary::EpochStarted, None, token).await?;
        }
        let context = self
            .context
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if !self.restored.load(Ordering::SeqCst) && self.kind != crate::SessionKind::OneShot {
            // Startup intent is durable before guest initialization effects, but
            // cannot certify a navigation baseline until all setup is ready.
            self.persist(&context, None).await?;
        }
        self.started.store(true, Ordering::SeqCst);
        transition.armed = false;
        #[cfg(feature = "wasm")]
        let fresh_initialization_required =
            self.state.wasm_instances().fresh_initialization_required();
        #[cfg(not(feature = "wasm"))]
        let fresh_initialization_required = false;
        if restored && !fresh_initialization_required {
            let status = self.saved_initialization_status();
            if status.is_ok() {
                #[cfg(feature = "wasm")]
                self.state.wasm_instances().permit_initialized_session();
                return Ok(());
            }
            if !retry {
                return status;
            }
        }
        self.ensure_agent_ready_impl(token, retry).await?;
        if !restored && self.kind != crate::SessionKind::OneShot {
            let context = self
                .context
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone();
            self.persist_initial(&context).await?;
        }
        Ok(())
    }
    fn saved_initialization_status(&self) -> Result<(), Error> {
        use crate::runtime::journal::AgentInitStatus;
        let snapshot = self.state();
        for record in snapshot.agent_init {
            match record.status {
                AgentInitStatus::Intent => {
                    return Err(Error::ToolFailure(
                        crate::runtime::execution::ToolFailure::failed(
                            "init_outcome_unknown",
                            "Initialization was interrupted; inspect effects and explicitly retry initialization.",
                        ),
                    ));
                }
                AgentInitStatus::Failed { failure } => return Err(Error::ToolFailure(failure)),
                AgentInitStatus::Ready => {}
            }
        }
        if snapshot.agent_initialization_pending {
            return Err(Error::ToolFailure(
                crate::runtime::execution::ToolFailure::failed(
                    "init_outcome_unknown",
                    "Initial Session setup did not finish; inspect effects and explicitly retry initialization.",
                ),
            ));
        }
        Ok(())
    }
    pub(crate) fn agent_initialization_complete(&self) -> bool {
        #[cfg(feature = "wasm")]
        {
            self.runtime.wasm_lifecycles().is_empty()
                || self.state.wasm_instances().initialization_complete()
        }
        #[cfg(not(feature = "wasm"))]
        {
            true
        }
    }
    fn agent_initialization_status(&self) -> Result<(), Error> {
        #[cfg(feature = "wasm")]
        if !self.state.wasm_instances().initialization_complete() {
            self.saved_initialization_status()?;
            return Err(Error::lifecycle(
                "initial Session setup is incomplete; explicitly retry initialization",
            ));
        }
        Ok(())
    }

    async fn ensure_agent_ready_impl(
        &self,
        token: &CancellationToken,
        retry: bool,
    ) -> Result<(), Error> {
        #[cfg(feature = "wasm")]
        {
            use crate::policy::{InvocationSubject, PermissionRequest};
            use crate::runtime::execution::{ExecutionSnapshot, FinalOutput, ToolFailure};
            use crate::runtime::journal::{AgentInitRecord, AgentInitStatus};
            if self.state.wasm_instances().initialization_complete() {
                return Ok(());
            }
            let context = self
                .context
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone();
            for lifecycle in self.runtime.wasm_lifecycles() {
                if token.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                let previous = self
                    .state()
                    .agent_init
                    .into_iter()
                    .find(|record| record.component == lifecycle.component());
                if let Some(record) = &previous {
                    let blocked = match &record.status {
                        AgentInitStatus::Intent => Some(ToolFailure::failed(
                            "init_outcome_unknown",
                            "Initialization was interrupted; effects may have occurred. Inspect the workspace and explicitly retry initialization.",
                        )),
                        AgentInitStatus::Failed { failure } => Some(failure.clone()),
                        AgentInitStatus::Ready => None,
                    };
                    if let Some(failure) = blocked {
                        if !retry {
                            return Err(Error::ToolFailure(failure));
                        }
                        lifecycle.reset(&self.state);
                    }
                }
                if previous
                    .as_ref()
                    .is_some_and(|record| matches!(record.status, AgentInitStatus::Ready))
                {
                    continue;
                }
                if lifecycle.ready(&self.state, token).await? {
                    continue;
                }
                if previous.is_none() {
                    crate::budget::check(
                        crate::LimitKind::ExecutionHistory,
                        self.state().agent_init.len().saturating_add(1),
                        128,
                    )?;
                }
                let attempt = previous
                    .as_ref()
                    .map_or(Some(1), |record| record.attempt.checked_add(1))
                    .ok_or_else(|| Error::lifecycle("initialization attempt counter exhausted"))?;
                let generation = lifecycle.generation(&self.state)?;
                let subject = InvocationSubject::AgentInit {
                    component: lifecycle.component().into(),
                    generation,
                    attempt,
                };
                let policy = self.runtime.permission_controller();
                let snapshot = policy.snapshot().map_err(|error| {
                    Error::config_with_private(error.public_reason(), error.to_string())
                })?;
                let call_id = format!(
                    "agent-init:{generation}:{attempt}:{}",
                    lifecycle.component()
                );
                let cancellation = token.child_token();
                let mut invocation = crate::runtime::tool::ToolContext::new(
                    self.state.clone(),
                    self.state.processes(),
                    cancellation.clone(),
                    call_id.clone(),
                    String::new(),
                    self.state.processes().session_id().into(),
                    snapshot.config.agent.clone(),
                    Some(snapshot.config.mode.clone()),
                );
                invocation.subject = subject.clone();
                invocation.begin_initialization_cache()?;
                invocation.turn_id = None;
                invocation.round_id = None;
                let request = PermissionRequest {
                    subject: subject.clone(),
                    session_id: invocation.session_id.clone(),
                    call_id: call_id.clone(),
                    agent: snapshot.config.agent.clone(),
                    mode: snapshot.config.mode.clone(),
                    tool: String::new(),
                    arguments: serde_json::json!({"component":lifecycle.component(),"imports":lifecycle.import_scope(),"trust":"Approval trusts this component to use its linked Host imports during this initialization attempt."}),
                    revision: snapshot.revision,
                };
                let issuer = context.content(self.state().limits.max_output_tokens)?;
                let mut row = ExecutionSnapshot::new(
                    invocation.session_id.clone(),
                    call_id,
                    String::new(),
                    invocation.agent.clone(),
                    invocation.mode.clone(),
                );
                row.subject = subject;
                let executions = self.state.executions();
                // This is a Session-owned lifecycle invocation, with no fabricated
                // model ToolCall. It shares the existing admission/output owner.
                let execution_id = executions.admit_detached(row, cancellation)?;
                invocation.execution_id = Some(execution_id);
                let record = AgentInitRecord {
                    version: 1,
                    component: lifecycle.component().into(),
                    generation,
                    attempt,
                    status: AgentInitStatus::Intent,
                    cache_points: Vec::new(),
                };
                self.state.update(|state| {
                    state
                        .agent_init
                        .retain(|old| old.component != record.component);
                    state.agent_init.push(record);
                });
                // Acknowledged durable intent precedes every guest/Host effect.
                self.persist_init_evidence(false).await?;
                let cache_context = invocation.clone();
                let result = match policy
                    .admit(
                        &request,
                        &invocation,
                        &issuer,
                        Some(lifecycle.admission_identity().into()),
                    )
                    .await
                {
                    Ok(proof) => {
                        invocation.admission = Some(proof);
                        lifecycle.initialize(invocation).await
                    }
                    Err(error) => Err(error.into_runtime_error()),
                };
                let cache_points = cache_context.finish_initialization_cache();
                let status = match &result {
                    Ok(()) => AgentInitStatus::Ready,
                    Err(Error::ToolFailure(failure)) => AgentInitStatus::Failed {
                        failure: failure.clone(),
                    },
                    Err(error) => {
                        let diagnostic = error.tool_diagnostic();
                        let code = serde_json::to_value(diagnostic.code)?
                            .as_str()
                            .unwrap_or("init_failed")
                            .to_owned();
                        AgentInitStatus::Failed {
                            failure: ToolFailure::failed(code, diagnostic.summary()),
                        }
                    }
                };
                match &result {
                    Ok(()) => {
                        executions
                            .complete_detached(
                                execution_id,
                                FinalOutput {
                                    content: "Agent initialization ready".into(),
                                    failure: None,
                                },
                            )
                            .await?
                    }
                    Err(error) => executions.finish_error(execution_id, error).await,
                }
                self.state.update(|state| {
                    if let Some(record) = state.agent_init.iter_mut().find(|record| {
                        record.component == lifecycle.component() && record.attempt == attempt
                    }) {
                        record.status = status;
                        record.cache_points = cache_points;
                    }
                });
                self.persist_init_evidence(false).await?;
                result?;
            }
            if self.state().agent_initialization_pending {
                self.persist_init_evidence(true).await?;
            }
            self.state.wasm_instances().permit_initialized_session();
        }
        #[cfg(not(feature = "wasm"))]
        let _ = (token, retry);
        Ok(())
    }

    async fn persist_init_evidence(&self, complete: bool) -> Result<(), Error> {
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
        if complete {
            self.persist_agent_initialization_complete(&context, continuation)
                .await?;
        } else {
            self.persist(&context, continuation).await?;
        }
        Ok(())
    }

    /// Explicit retry is a new admitted attempt, never automatic recovery replay.
    pub(crate) async fn retry_agent_init(self: &Arc<Self>) -> Result<(), Error> {
        let _operation = self.reserve().await?;
        self.ensure_open()?;
        if self.state.executions().has_pending() {
            return Err(Error::Busy);
        }
        let token = CancellationToken::new();
        self.register_command_token(&token)?;
        let _active = TurnGuard {
            inner: self.clone(),
            clear: false,
        };
        let _cancel = CancelOnDrop(token.clone());
        self.initialize_with_retry(&token, true).await
    }

    pub(crate) fn interrupt(&self) {
        self.interrupt_with_reason(crate::InterruptReason::UserRequested);
    }
    pub(crate) fn interrupt_with_reason(&self, reason: crate::InterruptReason) {
        self.inbox
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .paused = true;
        if let Some(operation) = &*self.active.lock().unwrap_or_else(|e| e.into_inner()) {
            operation.interrupt(reason);
        }
        // Watch readers cannot postpone cancellation of the admitted worker.
        self.publish_activity();
    }
    pub(crate) async fn compact(
        self: &Arc<Self>,
        summary: String,
        variables: Value,
    ) -> Result<(), Error> {
        self.ensure_open()?;
        crate::state::Variables::new(variables.clone())?;
        let _operation = self.reserve_replacement().await?;
        self.ensure_open()?;
        let token = CancellationToken::new();
        self.register_replacement_token(&token)?;
        let _active = TurnGuard {
            inner: self.clone(),
            clear: false,
        };
        let _cancel = CancelOnDrop(token.clone());
        self.initialize(&token).await?;
        self.compact_locked(summary, variables).await
    }
    /// Caller owns the operation reservation; functional generation never recurses
    /// through command admission or changes this baseline transition protocol.
    pub(super) async fn compact_locked(
        &self,
        summary: String,
        variables: Value,
    ) -> Result<(), Error> {
        let proposed = crate::state::Variables::new(variables)?;
        let Value::Object(set) = proposed.as_value() else {
            unreachable!("Variables is an object")
        };
        let change = crate::VariablesChange {
            scoped_set: vec![],
            set,
            remove: vec![],
        };
        let previous = self
            .context
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        // Reject invalid variable/reference writes and the complete candidate
        // before persistence or a transition fault can affect this Session.
        let preview = self.state.preview();
        preview.apply_change(change.clone())?;
        let mut replacement = previous.clone();
        replacement.compact(summary.clone(), &preview.model_variables()?)?;
        self.persist(&previous, None).await?;
        let fence = self.endpoint.fence().await?;
        // Earlier queued writes have committed. Rebuild under the fence so a
        // candidate never overwrites a concurrent variable commit with old data.
        let preview = self.state.preview();
        preview.apply_change(change.clone())?;
        let mut replacement = previous;
        replacement.compact(summary, &preview.model_variables()?)?;
        let mut transition = FaultOnDrop {
            faulted: &self.faulted,
            armed: true,
        };
        self.state.apply_change(change)?;
        *self.context.lock().unwrap_or_else(|e| e.into_inner()) = replacement;
        drop(fence);
        let current = self
            .context
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        self.persist(&current, None).await?;
        transition.armed = false;
        Ok(())
    }
    pub(crate) async fn close(self: &Arc<Self>) -> Result<(), Error> {
        self.close_owned(false, false).await
    }
    pub(crate) async fn reconcile_close(self: &Arc<Self>) -> Result<(), Error> {
        self.close_owned(true, false).await
    }
    pub(crate) async fn settle_for_delete(self: &Arc<Self>, reconcile: bool) -> Result<(), Error> {
        self.close_owned(reconcile, true).await
    }
    fn close_owned(
        self: &Arc<Self>,
        reconcile: bool,
        discard_snapshot: bool,
    ) -> futures_util::future::BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let mut receiver = {
                let mut slot = self.close_result.lock().unwrap_or_else(|e| e.into_inner());
                let previous = slot
                    .as_ref()
                    .and_then(|completion| completion.sender.borrow().clone());
                if let Some(completion) = slot.as_ref()
                    && !previous
                        .as_ref()
                        .is_some_and(|outcome| reconcile && outcome.cleanup_pending)
                {
                    completion.sender.subscribe()
                } else {
                    if self.is_main {
                        self.processes.seal();
                    } else {
                        self.processes
                            .seal_session(self.state.processes().session_id());
                    }
                    self.task_persistence_cancel.cancel();
                    self.closed.store(true, Ordering::SeqCst);
                    self.permission_controller().close_interactions();
                    self.interrupt_with_reason(crate::InterruptReason::SessionClosed);
                    let (sender, receiver) = tokio::sync::watch::channel(None);
                    *slot = Some(super::SessionClose {
                        sender: sender.clone(),
                    });
                    let inner = self.clone();
                    tokio::spawn(async move {
                        let retry = previous.is_some();
                        let result = AssertUnwindSafe(async {
                            inner.close_rename().await;
                            let helpers = crate::runtime::subagents::OneShotExecutor::close(
                                inner.one_shots.as_ref(),
                            )
                            .await;
                            let _operation = inner.operation.lock().await;
                            let children = inner.close_owned_children().await;
                            let tasks = inner.state.executions().close().await;
                            let processes = if inner.is_main {
                                if retry {
                                    inner.processes.reconcile_close().await
                                } else {
                                    inner.processes.close().await
                                }
                            } else {
                                inner
                                    .processes
                                    .close_session(inner.state.processes().session_id())
                                    .await
                            };
                            let cleanup_pending = tasks.is_err()
                                || processes.is_err()
                                || helpers.is_err()
                                || children.is_err();
                            if let Some(previous) = previous {
                                return super::SessionCloseOutcome {
                                    cleanup_pending,
                                    terminal_failed: previous.terminal_failed,
                                    persistence_failed: previous.persistence_failed,
                                };
                            }
                            inner.state.close_wasm();
                            let boundary = inner
                                .boundary(
                                    Boundary::SessionTerminated,
                                    None,
                                    &CancellationToken::new(),
                                )
                                .await;
                            // A destructive owner removes the Journal right after this
                            // close, so it skips the full terminal checkpoint instead of
                            // writing bytes that are deleted immediately. Hooks, audit
                            // and Journal release still fail closed.
                            let persistence_failed = if boundary.is_ok() && !discard_snapshot {
                                inner.persist_terminal().await.is_err()
                            } else {
                                false
                            };
                            let audit = inner.auditor.flush().await.map_err(|error| {
                                Error::Audit(
                                    format!(
                                        "auditor flush failed: {}",
                                        microcrab_protocol::diagnostic::describe(&error, 1800)
                                    )
                                    .into(),
                                )
                            });
                            super::SessionCloseOutcome {
                                cleanup_pending,
                                terminal_failed: boundary.and(audit).is_err(),
                                persistence_failed,
                            }
                        })
                        .catch_unwind()
                        .await;
                        let released = if retry {
                            Ok(Ok(()))
                        } else {
                            AssertUnwindSafe(inner.journal.close()).catch_unwind().await
                        };
                        inner.endpoint.shutdown();
                        let outcome = match result {
                            Ok(mut outcome) => {
                                outcome.terminal_failed |= !matches!(released, Ok(Ok(())));
                                outcome
                            }
                            Err(_) => super::SessionCloseOutcome {
                                // A panic can precede the cleanup acknowledgement.
                                cleanup_pending: true,
                                terminal_failed: true,
                                persistence_failed: false,
                            },
                        };
                        sender.send_replace(Some(outcome));
                    });
                    receiver
                }
            };
            loop {
                let result = receiver.borrow().clone();
                if let Some(outcome) = result {
                    return if outcome.cleanup_pending
                        || outcome.terminal_failed
                        || (outcome.persistence_failed && !discard_snapshot)
                    {
                        Err(Error::lifecycle("session termination cleanup failed"))
                    } else {
                        Ok(())
                    };
                }
                receiver.changed().await.map_err(|_| Error::Closed)?;
            }
        })
    }
}
