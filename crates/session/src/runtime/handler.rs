//! Admitted-turn execution over live State. Permission gates every tool dispatch.
use super::journal::RoundContinuation;
use super::tool::{
    DialogueReplyOwner, DialogueReplyRegistration, ToolContext, ToolRegistration, ToolRegistry,
};
pub(crate) mod permissions;
use crate::ModelEvent;
use crate::boundary::Boundary;
use crate::context::Context;
use crate::policy::SessionPermission;
use crate::utils::cancel::CancelOnDrop;
use crate::{Client, Error, SessionState, State};
use microcrab_auditor::Auditor;
use microcrab_auditor::{
    AuditBody, AuditorError, BoxFuture, HookContext, HookPayload, Injection, Moment,
};
use microcrab_protocol::{Response, ToolDefinition, Usage};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// One main call owns this witness across all transport retries. Publication and
/// intervention still delegate even when audit publication is disabled.
struct UsageRequestAuditor<'a> {
    inner: &'a dyn Auditor,
    rewritten: std::sync::atomic::AtomicBool,
}
impl Auditor for UsageRequestAuditor<'_> {
    fn is_enabled(&self) -> bool {
        self.inner.is_enabled()
    }
    fn publish(&self, moment: Moment, body: AuditBody) {
        self.inner.publish(moment, body);
    }
    fn intervene<'a>(
        &'a self,
        moment: Moment,
        context: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
        Box::pin(async move {
            let injection = self.inner.intervene(moment, context).await?;
            if injection.request_body.is_some() {
                self.rewritten
                    .store(true, std::sync::atomic::Ordering::Release);
            }
            Ok(injection)
        })
    }
    fn flush(&self) -> BoxFuture<'_, Result<(), AuditorError>> {
        self.inner.flush()
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnStopReason {
    #[default]
    ModelCompleted,
    RoundLimit,
    RepeatedInvalidToolArguments,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnOutcome {
    pub content: String,
    pub rounds: usize,
    pub usage: Usage,
    #[serde(default)]
    pub stop_reason: TurnStopReason,
}

/// Executes admitted work; Session owns admission and the bound auditor port.
pub trait RuntimeHandler: Send + Sync {
    fn execute_turn(
        &self,
        state: &SessionState,
        context: &mut Context,
        client: &dyn Client,
        auditor: &dyn Auditor,
        cancellation: CancellationToken,
    ) -> impl std::future::Future<Output = Result<TurnOutcome, Error>> + Send;
}

pub struct Runtime {
    tools: Arc<std::sync::RwLock<permissions::AppliedCatalog>>,
    controls:
        std::sync::RwLock<Option<std::sync::Weak<dyn super::tool::switch_mode::SessionControl>>>,
    reply_owner: std::sync::OnceLock<std::sync::Weak<dyn DialogueReplyOwner>>,
    permission: Arc<crate::policy::PermissionController>,
    delegate: Option<Arc<dyn super::tool::delegate::DelegateExecutor>>,
}

impl Runtime {
    pub fn new(
        tools: Vec<ToolRegistration>,
        permission: Arc<dyn SessionPermission>,
    ) -> Result<Self, Error> {
        let tools = ToolRegistry::new(tools)?;
        let initial_overrides = permission.initial_overrides();
        let controller = Arc::new(crate::policy::PermissionController::new(permission));
        controller
            .configure_tools(tools.origins(), tools.provider_defaults()?)
            .map_err(|error| {
                Error::config_with_private(error.public_reason(), error.to_string())
            })?;
        if !initial_overrides.is_empty() {
            let snapshot = controller.snapshot().map_err(|error| {
                Error::config_with_private(error.public_reason(), error.to_string())
            })?;
            let mut config = snapshot.config;
            config.overrides = initial_overrides;
            controller
                .replace_user(snapshot.revision, config)
                .map_err(|error| {
                    Error::config_with_private(error.public_reason(), error.to_string())
                })?;
        }
        Ok(Self {
            tools: Arc::new(std::sync::RwLock::new(permissions::AppliedCatalog::new(
                tools,
            ))),
            controls: std::sync::RwLock::new(None),
            reply_owner: std::sync::OnceLock::new(),
            permission: controller,
            delegate: None,
        })
    }
    /// Admit a direct invocation already reserved in this State's execution owner.
    /// Callers must pass the same original arguments to the registered tool; WASM
    /// validates these against the proof before entering the component.
    pub async fn admit_invocation(
        &self,
        call: &microcrab_protocol::ToolCall,
        invocation: &mut ToolContext,
        issuer: &crate::context::Content,
    ) -> Result<(), crate::policy::PermissionError> {
        use crate::policy::{PermissionError, PermissionRequest};
        let registry = self.registry();
        registry
            .validate(call)
            .map_err(|e| PermissionError::InvalidConfiguration(e.to_string()))?;
        if registry.origin(&call.name) == Some(super::tool::ToolOrigin::Builtin) {
            if call.name == "job" {
                crate::runtime::execution::job::validate(&call.arguments)
                    .and_then(|_| {
                        crate::runtime::execution::job::bind_redirect_destination(
                            &call.arguments,
                            invocation,
                        )
                    })
                    .map_err(|e| PermissionError::InvalidConfiguration(e.to_string()))?;
                if let Some(action) = call
                    .arguments
                    .get("action")
                    .and_then(serde_json::Value::as_str)
                    && crate::runtime::execution::job::canonical(action)
                        .is_some_and(|canonical| canonical != action)
                {
                    return Err(PermissionError::InvalidConfiguration("direct job admission requires the canonical action; normalize the action before admission and execution".into()));
                }
            } else if call.name == "delegate"
                && call
                    .arguments
                    .get("action")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("start")
                    == "start"
            {
                let target = call
                    .arguments
                    .get("target")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                let mode = call
                    .arguments
                    .get("mode")
                    .and_then(serde_json::Value::as_str);
                let (canonical_target, canonical_mode) = invocation
                    .state
                    .resolve_delegate(target, mode)
                    .map_err(|e| PermissionError::InvalidConfiguration(e.to_string()))?;
                if target != canonical_target || mode != Some(canonical_mode.as_str()) {
                    return Err(PermissionError::InvalidConfiguration(format!(
                        "direct delegate admission requires canonical target {canonical_target:?} and explicit mode {canonical_mode:?} before admission and execution"
                    )));
                }
            }
        }
        let id = invocation.execution_id.ok_or_else(|| {
            PermissionError::InvalidConfiguration(
                "direct admission requires a reserved execution identity".into(),
            )
        })?;
        let row = invocation
            .state
            .executions()
            .get(id)
            .map_err(|e| PermissionError::InvalidConfiguration(e.to_string()))?;
        if invocation.call_id != call.id
            || invocation.tool != call.name
            || !invocation.subject.is_tool()
            || row.session_id != invocation.session_id
            || row.call_id != invocation.call_id
            || row.agent != invocation.agent
            || row.mode != invocation.mode
            || row.tool != invocation.tool
            || row.subject != invocation.subject
            || !row.status.is_pending()
        {
            return Err(PermissionError::InvalidConfiguration(
                "direct admission execution identity mismatch".into(),
            ));
        }
        #[cfg(feature = "wasm")]
        registry
            .bind_file_versions(call, invocation)
            .await
            .map_err(|e| PermissionError::InvalidConfiguration(e.diagnostic_reason()))?;
        let snapshot = self.permission.snapshot()?;
        invocation.tool_description = registry.tool_description(&call.name).map(str::to_owned);
        let request = PermissionRequest {
            subject: invocation.subject.clone(),
            session_id: invocation.session_id.clone(),
            call_id: call.id.clone(),
            tool: call.name.clone(),
            arguments: call.arguments.clone(),
            agent: snapshot.config.agent.clone(),
            mode: snapshot.config.mode.clone(),
            revision: snapshot.revision,
        };
        let proof = self
            .permission
            .admit(
                &request,
                invocation,
                issuer,
                registry.admission_identity(&call.name),
            )
            .await?;
        if !Arc::ptr_eq(&registry, &self.registry()) {
            return Err(PermissionError::StalePolicy);
        }
        invocation.admission = Some(proof);
        Ok(())
    }
    /// Admit a direct lifecycle attempt for an exact registered component. The
    /// embedder retains responsibility for acknowledged intent and settlement.
    #[cfg(feature = "wasm")]
    pub async fn admit_initialization(
        &self,
        lifecycle: &super::wasm::AgentLifecycle,
        invocation: &mut ToolContext,
        issuer: &crate::context::Content,
    ) -> Result<(), crate::policy::PermissionError> {
        use crate::policy::{InvocationSubject, PermissionError, PermissionRequest};
        let registry = self.registry();
        let registered = registry
            .wasm_lifecycles()
            .into_iter()
            .find(|candidate| candidate.identity() == lifecycle.identity())
            .ok_or_else(|| {
                PermissionError::InvalidConfiguration(
                    "initialization component is not registered in this Runtime".into(),
                )
            })?;
        let InvocationSubject::AgentInit {
            component,
            generation,
            attempt,
        } = &invocation.subject
        else {
            return Err(PermissionError::InvalidConfiguration(
                "initialization requires an AgentInit subject".into(),
            ));
        };
        if component != registered.component()
            || *attempt == 0
            || !invocation.tool.is_empty()
            || *generation
                != registered
                    .generation(&invocation.state)
                    .map_err(|e| PermissionError::InvalidConfiguration(e.to_string()))?
        {
            return Err(PermissionError::InvalidConfiguration("initialization component, generation or attempt does not match registered lifecycle".into()));
        }
        let id = invocation.execution_id.ok_or_else(|| {
            PermissionError::InvalidConfiguration(
                "initialization admission requires a reserved execution identity".into(),
            )
        })?;
        let row = invocation
            .state
            .executions()
            .get(id)
            .map_err(|e| PermissionError::InvalidConfiguration(e.to_string()))?;
        if row.session_id != invocation.session_id
            || row.call_id != invocation.call_id
            || row.agent != invocation.agent
            || row.mode != invocation.mode
            || row.subject != invocation.subject
            || !row.tool.is_empty()
            || !row.status.is_pending()
        {
            return Err(PermissionError::InvalidConfiguration(
                "initialization admission execution identity mismatch".into(),
            ));
        }
        let snapshot = self.permission.snapshot()?;
        invocation.tool_description = Some(
            "Initialize the registered WASM agent component within its declared host import scope."
                .into(),
        );
        let request = PermissionRequest {
            subject: invocation.subject.clone(),
            session_id: invocation.session_id.clone(),
            call_id: invocation.call_id.clone(),
            agent: snapshot.config.agent,
            mode: snapshot.config.mode,
            tool: String::new(),
            revision: snapshot.revision,
            arguments: serde_json::json!({"component":registered.component(),"imports":registered.import_scope(),"trust":"Approval trusts this component to use its linked Host imports during this initialization attempt."}),
        };
        let proof = self
            .permission
            .admit(
                &request,
                invocation,
                issuer,
                Some(registered.admission_identity().into()),
            )
            .await?;
        if !Arc::ptr_eq(&registry, &self.registry()) {
            return Err(PermissionError::StalePolicy);
        }
        invocation.admission = Some(proof);
        Ok(())
    }
    /// Replace only editable user policy; native/catalog defaults and current
    /// agent/mode retain the controller's existing read-only validation.
    pub fn replace_permissions(
        &self,
        expected_revision: u64,
        config: crate::policy::PermissionConfig,
    ) -> Result<crate::policy::PolicySnapshot, crate::policy::PermissionError> {
        self.permission.replace_user(expected_revision, config)
    }
    pub fn permission_controller(&self) -> Arc<crate::policy::PermissionController> {
        self.permission.clone()
    }
    pub(crate) fn bind_delegate_parent(
        &self,
        parent: std::sync::Weak<dyn super::tool::delegate::DelegateParent>,
    ) {
        if let Some(delegate) = &self.delegate {
            delegate.bind_parent(parent);
        }
    }
    pub fn with_delegate_executor(
        mut self,
        executor: Arc<dyn super::tool::delegate::DelegateExecutor>,
    ) -> Self {
        self.delegate = Some(executor);
        self
    }
    pub(crate) fn bind_controls(
        &self,
        control: std::sync::Weak<dyn super::tool::switch_mode::SessionControl>,
    ) {
        *self.controls.write().unwrap_or_else(|e| e.into_inner()) = Some(control);
    }
    pub(crate) fn delegate_executor(
        &self,
    ) -> Option<Arc<dyn super::tool::delegate::DelegateExecutor>> {
        self.delegate.clone()
    }
    pub(crate) fn bind_dialogue_reply_owner(&self, owner: std::sync::Weak<dyn DialogueReplyOwner>) {
        // Runtime is moved into a single Session; there is no public rebinding path.
        let _ = self.reply_owner.set(owner);
    }
    pub(crate) fn dialogue_reply_is_registered(&self, name: &str, identity: &str) -> bool {
        self.registry().dialogue_reply_is_registered(name, identity)
    }
    fn controls(&self) -> Option<Arc<dyn super::tool::switch_mode::SessionControl>> {
        self.controls
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
    }
    fn registry(&self) -> Arc<ToolRegistry> {
        self.tools
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .registry
            .clone()
    }
    /// Initial Session assembly only; never changes an already frozen epoch.
    pub(crate) fn configure_conversation_tools(
        &self,
        identity: Option<&crate::state::communication::ConversationIdentity>,
        interactive_root: bool,
    ) -> Result<(), Error> {
        let registry = Arc::new(
            self.registry()
                .for_conversation(identity, interactive_root)?,
        );
        let provider = registry.provider_defaults()?;
        let snapshot = self
            .permission
            .tool_config(&registry.origins(), &provider, &[])
            .map_err(|error| {
                Error::config_with_private(error.public_reason(), error.to_string())
            })?;
        self.publish_catalog(registry, provider, snapshot)
            .map_err(|error| {
                Error::config_with_private(error.public_reason(), error.to_string())
            })?;
        Ok(())
    }
    pub(crate) fn prepare_external_tools(
        &self,
        tools: Vec<Arc<dyn super::tool::Tool>>,
    ) -> Result<Arc<ToolRegistry>, Error> {
        Ok(Arc::new(self.registry().replacing_external(tools)?))
    }
    pub(crate) fn prepare_model_catalog(
        &self,
        owner: &dyn crate::ModelAdmission,
        effect: Option<&DialogueReplyRegistration>,
        external: Option<Vec<Arc<dyn super::tool::Tool>>>,
    ) -> Result<Arc<ToolRegistry>, Error> {
        let current = self.registry();
        let registry = current.replacing_dialogue_reply(owner, effect)?;
        let registry = match external {
            Some(external) => registry.replacing_external(external)?,
            None => registry,
        };
        registry.validate_model_owner(owner)?;
        Ok(Arc::new(registry))
    }

    pub(crate) fn delegate_available(&self) -> bool {
        self.delegate.is_some() && self.origin("delegate").is_some()
    }
    #[cfg(feature = "wasm")]
    pub(crate) fn wasm_lifecycles(&self) -> Vec<super::wasm::AgentLifecycle> {
        self.registry().wasm_lifecycles()
    }
    pub fn definitions(&self) -> Vec<ToolDefinition> {
        self.registry().definitions()
    }
    pub fn origin(&self, name: &str) -> Option<super::tool::ToolOrigin> {
        self.registry().origin(name)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Turn execution borrows distinct Context, model, audit and recording owners with separate lifetimes"
    )]
    pub(crate) async fn execute_live<R: RoundRecorder>(
        &self,
        state: &State,
        context: &mut Context,
        client: &dyn Client,
        auditor: &dyn Auditor,
        cancellation: CancellationToken,
        recorder: &R,
        continuation: Option<RoundContinuation>,
        events: Option<mpsc::Sender<ModelEvent>>,
    ) -> Result<TurnOutcome, Error> {
        let limits = state.snapshot().limits;
        limits.validate()?;
        super::execution::job::require_current_definition(context.frozen_tools())?;
        let turn = cancellation.child_token();
        let _cancel = CancelOnDrop(turn.clone());
        state.refresh_writable_references(turn.clone()).await?;
        self.run_turn(
            state,
            context,
            client,
            auditor,
            turn,
            recorder,
            continuation,
            events,
        )
        .await
    }

    /// Build one consumption-checkpoint question. No content, path or secret is
    /// carried; the numbers are the facts the caller already measured.
    fn confirmation_request(
        &self,
        state: &State,
        kind: crate::budget::ConfirmationKind,
        used: usize,
        block: usize,
    ) -> Result<crate::budget::RoundLimitRequest, Error> {
        let snapshot = state.snapshot();
        Ok(crate::budget::RoundLimitRequest {
            session_id: state.processes().session_id().into(),
            epoch: snapshot.epoch_id,
            turn_id: snapshot
                .turn_id
                .ok_or_else(|| Error::config("budget confirmation requires an active turn"))?,
            kind,
            used,
            block,
        })
    }

    /// Ask the embedding owner for one more block. `Ok(false)` means the owner
    /// declined or the embedding has no callback; the caller then keeps its
    /// existing recoverable-continuation path instead of dispatching effects.
    async fn confirm_checkpoint<R: RoundRecorder>(
        &self,
        recorder: &R,
        state: &State,
        kind: crate::budget::ConfirmationKind,
        used: usize,
        block: usize,
        cancellation: &CancellationToken,
    ) -> Result<bool, Error> {
        let exhaustion = recorder.round_exhaustion();
        if matches!(exhaustion, crate::budget::Exhaustion::Stop) {
            return Ok(false);
        }
        let request = self.confirmation_request(state, kind, used, block)?;
        let decision = exhaustion.resolve(request, cancellation.clone()).await?;
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        Ok(decision == crate::budget::RoundLimitDecision::Continue)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "The turn loop forwards independently borrowed Context, model, audit, recorder and cancellation owners"
    )]
    async fn run_turn<R: RoundRecorder>(
        &self,
        state: &State,
        context: &mut Context,
        client: &dyn Client,
        auditor: &dyn Auditor,
        cancellation: CancellationToken,
        recorder: &R,
        continuation: Option<RoundContinuation>,
        events: Option<mpsc::Sender<ModelEvent>>,
    ) -> Result<TurnOutcome, Error> {
        // Renewable consumption checkpoints may raise the Context budget mid-turn,
        // so this copy is refreshed after an explicit grant.
        recorder.set_model_retry_ready(false);
        let mut limits = state.snapshot().limits;
        if let Some(settled) = continuation
            .as_ref()
            .filter(|value| !value.request_prepared && !value.state_observation_pending)
        {
            recorder
                .compact_boundary(context, Some(settled.clone()), false, &cancellation)
                .await?;
        }
        let mut prepared = continuation
            .as_ref()
            .is_some_and(|value| value.request_prepared);
        let (mut round, mut unattended, mut usage) = continuation
            .map(|value| (value.next_round, value.unattended_rounds, value.usage))
            .unwrap_or((1, 0, Usage::default()));
        let mut sending = true;
        let mut last_content = String::new();
        loop {
            if cancellation.is_cancelled() {
                if prepared {
                    recorder
                        .record(
                            context,
                            Some(RoundContinuation {
                                next_round: round,
                                unattended_rounds: unattended,
                                usage: usage.clone(),
                                request_prepared: true,
                                state_observation_pending: false,
                            }),
                        )
                        .await?;
                    recorder.set_model_retry_ready(true);
                }
                return Err(Error::Cancelled);
            }
            let allowed = unattended.checked_add(1).is_some_and(|required| {
                crate::budget::check(crate::LimitKind::Rounds, required, limits.max_rounds).is_ok()
            });
            let prepared_request = std::mem::take(&mut prepared);
            let admission = if prepared_request {
                StepAdmission {
                    user: false,
                    send: true,
                    blocked: false,
                }
            } else {
                match recorder
                    .admit_step(
                        state,
                        context,
                        allowed,
                        sending,
                        round,
                        auditor,
                        &cancellation,
                    )
                    .await
                {
                    Ok(admission) => admission,
                    Err(Error::StateObservationConflict) => {
                        // Preserve the exact admitted suffix boundary. Unlike a
                        // prepared provider retry, this request still needs fresh
                        // State materialization after the update settles.
                        recorder
                            .record(
                                context,
                                Some(RoundContinuation {
                                    next_round: round,
                                    unattended_rounds: unattended,
                                    usage: usage.clone(),
                                    request_prepared: false,
                                    state_observation_pending: true,
                                }),
                            )
                            .await?;
                        recorder.set_model_retry_ready(true);
                        return Err(Error::StateObservationConflict);
                    }
                    Err(error) => return Err(error),
                }
            };
            if admission.user {
                unattended = 0;
            }
            let mut stop_reason = TurnStopReason::ModelCompleted;
            let exhaustion = recorder.round_exhaustion();
            if admission.blocked && !matches!(&exhaustion, crate::budget::Exhaustion::Stop) {
                // Persist the settled boundary, never a human answer or live request.
                // Recovery must ask again before sending another automatic request.
                recorder
                    .record(
                        context,
                        Some(RoundContinuation {
                            next_round: round,
                            unattended_rounds: unattended,
                            usage: usage.clone(),
                            request_prepared: false,
                            state_observation_pending: false,
                        }),
                    )
                    .await?;
                let request = self.confirmation_request(
                    state,
                    crate::budget::ConfirmationKind::Rounds,
                    round - 1,
                    limits.max_rounds,
                )?;
                let decision = exhaustion.resolve(request, cancellation.clone()).await?;
                if cancellation.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                if decision == crate::budget::RoundLimitDecision::Continue {
                    unattended = 0;
                    // Admit arrivals during the wait at this same, not-yet-started round.
                    continue;
                }
            }
            if admission.blocked {
                recorder.stop_at_round_limit();
                stop_reason = TurnStopReason::RoundLimit;
            }
            if !admission.send {
                recorder.record(context, None).await?;
                return Ok(TurnOutcome {
                    content: last_content,
                    rounds: round - 1,
                    usage,
                    stop_reason,
                });
            }
            if !prepared_request && let Err(error) = recorder.check_request(context) {
                recorder
                    .record(
                        context,
                        Some(RoundContinuation {
                            next_round: round,
                            unattended_rounds: unattended,
                            usage: usage.clone(),
                            request_prepared: false,
                            state_observation_pending: false,
                        }),
                    )
                    .await?;
                return Err(error);
            }
            unattended = unattended.checked_add(1).ok_or_else(|| {
                crate::budget::reject(
                    crate::LimitKind::Arithmetic,
                    "unattended round counter overflow",
                )
            })?;
            recorder.observe_context(context)?;
            let (epoch, items) = context.admitted_items();
            client.observe_admitted(epoch, items, None).await;
            let mut content = context.content(limits.max_output_tokens)?;
            content.turn_id = state.snapshot().turn_id;
            content.round = Some(round as u64);
            let usage_request = context.capture_usage_request(recorder.request_route(&content)?)?;
            let usage_auditor = UsageRequestAuditor {
                inner: auditor,
                rewritten: std::sync::atomic::AtomicBool::new(false),
            };
            // Transport owns the renewable inactivity window. Session imposes no
            // elapsed-turn ceiling, but caller cancellation must interrupt a model
            // wait even when an embedding Client does not observe its token.
            // Only this model wait and pure response validation certify no admitted batch.
            let candidate: Result<_, Error> = async {
                let outcome = tokio::select! { biased;
                    _ = cancellation.cancelled() => Err(microcrab_transport::Error::Cancelled),
                    result = client.stream_admitted_outcome(&content, &usage_auditor, state, cancellation.clone(), events.clone()) => result,
                }.map_err(Error::from)?;
                let response = outcome.response;
                if cancellation.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                for call in &response.tool_calls {
                    if !content
                        .tools
                        .iter()
                        .any(|definition| definition.name == call.name)
                    {
                        return Err(Error::protocol(
                            "model called a tool absent from the frozen Context catalog",
                        ));
                    }
                }
                // Validate and measure the unadmitted response against a probe budget
                // first, so a protocol violation keeps its quarantine and the exact
                // required bytes are known before any grant or Context mutation.
                let mut probe = context.clone();
                probe.grant_bytes(usize::MAX)?;
                probe.append_response(&response)?;
                let measured = probe.measure()?;
                let mut required = measured.bytes;
                for call in &response.tool_calls {
                    let framing = serde_json::to_vec(&crate::context::ContentItem::Tool {
                        parts: vec![],
                        call_id: call.id.clone(),
                        content: String::new(),
                    })?
                    .len()
                    .saturating_add(1);
                    required = required
                        .saturating_add(tool_result_reservation_bytes(
                            call,
                            &limits,
                            self.registry().media_output_bytes(&call.name),
                        ))
                        .saturating_add(framing);
                }
                // Renewable consumption checkpoints: crossing one asks the embedding
                // owner instead of failing an otherwise valid model response. Both
                // questions are pre-effect, so a Stop keeps the existing recoverable
                // continuation and a Continue never replays a dispatched call.
                if response.tool_calls.len() > limits.max_tool_calls_per_round {
                    let granted = self
                        .confirm_checkpoint(
                            recorder,
                            state,
                            crate::budget::ConfirmationKind::ToolCalls,
                            response.tool_calls.len(),
                            limits.max_tool_calls_per_round,
                            &cancellation,
                        )
                        .await?;
                    if !granted {
                        return Err(Error::Limit(
                            crate::LimitError::new(crate::LimitKind::ToolCalls)
                                .counts(response.tool_calls.len(), limits.max_tool_calls_per_round)
                                .calls(response.tool_calls.len())
                                .before_effects(),
                        ));
                    }
                }
                let available = context.max_bytes();
                if required > available {
                    // One block is at least the deficit and at least a Mebibyte, so a
                    // grant buys usable headroom instead of asking again for a few bytes.
                    let block = required.saturating_sub(available).max(1024 * 1024);
                    let granted = self
                        .confirm_checkpoint(
                            recorder,
                            state,
                            crate::budget::ConfirmationKind::ContextBytes,
                            required,
                            block,
                            &cancellation,
                        )
                        .await?;
                    if !granted {
                        return Err(Error::Limit(
                            crate::LimitError::new(crate::LimitKind::ToolResultReservation)
                                .bytes(required, available)
                                .calls(response.tool_calls.len())
                                .before_effects(),
                        ));
                    }
                    // One block is at least the deficit and at least a Mebibyte, so a
                    // grant buys usable headroom instead of asking again for a few bytes.
                    let next = available.saturating_add(block);
                    context.grant_bytes(next)?;
                    // One agreed budget in both witnesses, without rewriting history.
                    state.update(|snapshot| snapshot.limits.max_context_bytes = next);
                    limits = state.snapshot().limits;
                }
                Ok((response, outcome.attempt))
            }
            .await;
            let (response, attempt) = match candidate {
                Ok(outcome) => outcome,
                Err(error) => {
                    if matches!(
                        &error,
                        Error::Cancelled
                            | Error::ProviderStatus { .. }
                            | Error::Transport
                            | Error::TransportFailure(_)
                            | Error::ProviderTimeout
                            | Error::ProviderIdleTimeout(_)
                            | Error::Provider(_)
                            | Error::Protocol(_)
                            | Error::Json(_)
                    ) || matches!(&error, Error::Limit(limit) if limit.is_pre_effect())
                    {
                        recorder
                            .record(
                                context,
                                Some(RoundContinuation {
                                    next_round: round,
                                    unattended_rounds: unattended - 1,
                                    usage: usage.clone(),
                                    request_prepared: true,
                                    state_observation_pending: false,
                                }),
                            )
                            .await?;
                        recorder.set_model_retry_ready(true);
                    }
                    return Err(error);
                }
            };
            if cancellation.is_cancelled() {
                recorder
                    .record(
                        context,
                        Some(RoundContinuation {
                            next_round: round,
                            unattended_rounds: unattended - 1,
                            usage: usage.clone(),
                            request_prepared: true,
                            state_observation_pending: false,
                        }),
                    )
                    .await?;
                recorder.set_model_retry_ready(true);
                return Err(Error::Cancelled);
            }
            // Effectful interventions are outside the safe retry region.
            recorder.set_model_retry_ready(false);
            crate::observation::hooks::intervene(
                auditor,
                state,
                Moment::RoundCompleted,
                HookPayload::RoundCompleted {
                    response: Some(response.clone()),
                    status: None,
                    usage: Some(response.usage.clone()),
                },
                &cancellation,
            )
            .await?;
            if cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let assistant_index = context.admitted_items().1.len();
            context.append_response(&response)?;
            recorder.set_model_retry_ready(false);
            let (epoch, items) = context.admitted_items();
            client
                .observe_response_admitted(epoch, items, assistant_index, attempt.as_ref())
                .await;
            let usage_request = if usage_auditor
                .rewritten
                .load(std::sync::atomic::Ordering::Acquire)
            {
                None
            } else {
                usage_request
            };
            context.accept_usage_request(
                usage_request,
                recorder.request_route(&content)?,
                &response.usage,
            )?;
            recorder.observe_context(context)?;
            usage.accumulate(&response.usage);
            last_content = response.content.clone();
            sending = !response.tool_calls.is_empty();
            struct ModeBatch(Option<Arc<dyn super::tool::switch_mode::SessionControl>>);
            impl Drop for ModeBatch {
                fn drop(&mut self) {
                    if let Some(control) = &self.0 {
                        control.discard_pending_mode();
                    }
                }
            }
            let _mode_batch = ModeBatch(self.controls());
            if !sending {
                crate::observation::hooks::boundary(
                    auditor,
                    state,
                    Boundary::RoundEnded,
                    &cancellation,
                )
                .await?;
            } else {
                crate::observation::hooks::boundary(
                    auditor,
                    state,
                    Boundary::RoundSuspended,
                    &cancellation,
                )
                .await?;
                self.execute_calls(
                    state,
                    context,
                    &response,
                    auditor,
                    &cancellation,
                    recorder,
                    client,
                )
                .await?;
                crate::observation::hooks::boundary(
                    auditor,
                    state,
                    Boundary::RoundResumed,
                    &cancellation,
                )
                .await?;
                crate::observation::hooks::boundary(
                    auditor,
                    state,
                    Boundary::RoundEnded,
                    &cancellation,
                )
                .await?;
            }
            if let Some(control) = self.controls() {
                control.apply_pending_mode()?;
            }
            let next_round = round.checked_add(1).ok_or_else(|| {
                crate::budget::reject(crate::LimitKind::Arithmetic, "round ordinal overflow")
            })?;
            if sending {
                recorder
                    .record(
                        context,
                        Some(RoundContinuation {
                            next_round,
                            unattended_rounds: unattended,
                            usage: usage.clone(),
                            request_prepared: false,
                            state_observation_pending: false,
                        }),
                    )
                    .await?;
            }
            recorder
                .compact_boundary(
                    context,
                    sending.then(|| RoundContinuation {
                        next_round,
                        unattended_rounds: unattended,
                        usage: usage.clone(),
                        request_prepared: false,
                        state_observation_pending: false,
                    }),
                    !sending,
                    &cancellation,
                )
                .await?;
            round = next_round;
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Execution borrows the real client admission observation port"
    )]
    async fn execute_calls(
        &self,
        state: &State,
        context: &mut Context,
        response: &Response,
        auditor: &dyn Auditor,
        cancellation: &CancellationToken,
        recorder: &impl RoundRecorder,
        client: &dyn Client,
    ) -> Result<(), Error> {
        let limits = state.snapshot().limits;
        let tool_registry = self.registry();
        // The assistant was appended before entering this batch; execution order
        // can differ from the canonical order when pending-work waits are deferred.
        let assistant_index = context.admitted_items().1.len().checked_sub(1);
        let mut ordered = response
            .tool_calls
            .iter()
            .cloned()
            .enumerate()
            .collect::<Vec<_>>();
        ordered.sort_by_key(|(_, call)| is_pending_work_wait(call));
        let mut pending = ordered
            .into_iter()
            .map(|(slot, call)| {
                (
                    slot,
                    call,
                    None,
                    super::tool::injection::LiteralMask::default(),
                    0usize,
                )
            })
            .collect::<std::collections::VecDeque<_>>();
        while let Some((
            call_index,
            mut normalized,
            prepared,
            mut literal_mask,
            mut text_conversions,
        )) = pending.pop_front()
        {
            if cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }
            // One admission clock survives a deferred pending-work wait. It measures this
            // foreground invocation through its returned result/receipt, not the
            // lifetime of background work represented by that receipt.
            let clock = prepared.unwrap_or_else(|| {
                (
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .ok()
                        .and_then(|duration| u64::try_from(duration.as_millis()).ok()),
                    Instant::now(),
                )
            });
            let mut expansion_error = None;
            if prepared.is_none() {
                let injection = crate::observation::hooks::intervene_recorded(
                    auditor,
                    state,
                    Moment::ToolCall,
                    HookPayload::ToolCall {
                        name: normalized.name.clone(),
                        arguments: normalized.arguments.clone(),
                    },
                    cancellation,
                    AuditBody {
                        tool_call_id: Some(normalized.id.clone()),
                        tool_presentation: tool_registry.interaction_presentation(&normalized.name),
                        started_at_unix_ms: clock.0,
                        ..Default::default()
                    },
                )
                .await?;
                let rewritten = injection.tool_arguments.is_some();
                if let Some(arguments) = injection.tool_arguments {
                    normalized.arguments = arguments;
                }
                text_conversions = tool_registry.convert_text_arguments(&mut normalized);
                #[cfg(feature = "wasm")]
                tool_registry.apply_glob_path_default(&mut normalized);
                // Visible expansion is a derived copy. Context and the ToolCall audit
                // retain the authored or hook-replaced placeholders.
                if !tool_registry.uses_literal_sources(&normalized) {
                    match super::tool::injection::expand_dispatch(
                        &normalized.arguments,
                        state,
                        limits.max_context_bytes,
                        cancellation,
                    )
                    .await
                    {
                        Ok((arguments, mask)) => {
                            if normalized.name == "job"
                                && super::execution::job::is_redirect(&arguments)
                            {
                                expansion_error = Some(Error::tool(
                                    "job redirect action and selectors must be authored literally",
                                ));
                            } else {
                                normalized.arguments = arguments;
                                literal_mask = mask;
                            }
                        }
                        Err(error) => expansion_error = Some(error),
                    }
                }
                if expansion_error.is_none() {
                    if rewritten || text_conversions != 0 {
                        // A rewrite may turn a small status response into a destructive
                        // drain. Re-admit it together with remaining calls so it cannot
                        // spend capacity already reserved for their results.
                        let mut reserved = context.clone();
                        reserve_tool_result(
                            &mut reserved,
                            &normalized,
                            &limits,
                            tool_registry.media_output_bytes(&normalized.name),
                        )?;
                        for (_, remaining, _, _, _) in &pending {
                            reserve_tool_result(
                                &mut reserved,
                                remaining,
                                &limits,
                                tool_registry.media_output_bytes(&remaining.name),
                            )?;
                        }
                    }
                    // Restoring or replacing arguments may create a cross-plane wait. Defer
                    // it without repeating the hook or its committed variables.
                    if is_pending_work_wait(&normalized)
                        && pending
                            .iter()
                            .any(|(_, call, _, _, _)| !is_pending_work_wait(call))
                    {
                        pending.push_back((
                            call_index,
                            normalized,
                            Some(clock),
                            literal_mask,
                            text_conversions,
                        ));
                        continue;
                    }
                }
            }
            if normalized.name == "job"
                && let Some(action) = normalized
                    .arguments
                    .get("action")
                    .and_then(serde_json::Value::as_str)
                    .and_then(crate::runtime::execution::job::canonical)
            {
                normalized.arguments["action"] = serde_json::json!(action);
            }
            // Start discovery resolves new Agent/mode eligibility. Existing-child
            // controls retain their exact target and are checked by the parent's
            // relationship owner after ordinary current-policy admission.
            let delegate_error = if normalized.name == "delegate"
                && normalized
                    .arguments
                    .get("action")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("start")
                    == "start"
            {
                let target = normalized
                    .arguments
                    .get("target")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                let requested_mode = normalized
                    .arguments
                    .get("mode")
                    .and_then(serde_json::Value::as_str);
                match state.resolve_delegate(target, requested_mode) {
                    Ok((target, mode)) => {
                        normalized.arguments["target"] = serde_json::json!(target);
                        normalized.arguments["mode"] = serde_json::json!(mode);
                        None
                    }
                    Err(error) => Some(error),
                }
            } else {
                None
            };
            let call = &normalized;
            // A registration may have changed since model response admission.
            // Pin its actual declaration and reserve this entire remaining batch
            // before any invocation can begin Host effects.
            let mut result_probe = context.clone();
            reserve_tool_result(
                &mut result_probe,
                call,
                &limits,
                tool_registry.media_output_bytes(&call.name),
            )?;
            for (_, remaining, _, _, _) in &pending {
                reserve_tool_result(
                    &mut result_probe,
                    remaining,
                    &limits,
                    tool_registry.media_output_bytes(&remaining.name),
                )?;
            }
            // Registry ownership replaces call/turn CancelOnDrop after transfer.
            let tool_token = CancellationToken::new();
            let snapshot_config = self
                .permission
                .snapshot()
                .map(|s| s.config)
                .unwrap_or_default();
            let session_id = state.processes().session_id().to_owned();
            let agent = if snapshot_config.agent.is_empty() {
                "default".to_owned()
            } else {
                snapshot_config.agent.clone()
            };
            let mode = if snapshot_config.mode.is_empty() {
                state.snapshot().selected_mode
            } else {
                Some(snapshot_config.mode.clone())
            };
            let invocation = ToolContext::new(
                state.clone(),
                state.processes(),
                tool_token,
                call.id.clone(),
                call.name.clone(),
                session_id,
                agent,
                mode,
            );
            let mut invocation = invocation;
            let action_error = expansion_error
                .or_else(|| tool_registry.clone().validate(call).err())
                .or_else(|| {
                    if call.name == "job" {
                        crate::runtime::execution::job::validate(&call.arguments)
                            .and_then(|_| {
                                crate::runtime::execution::job::bind_redirect_destination(
                                    &call.arguments,
                                    &mut invocation,
                                )
                            })
                            .err()
                    } else {
                        delegate_error
                    }
                });
            use crate::policy::{PermissionError, PermissionRequest};
            invocation.literal_mask = literal_mask;
            let observation_interrupt = (tool_registry.clone().origin(&call.name)
                == Some(super::tool::ToolOrigin::Builtin)
                && call.name == "job"
                && call
                    .arguments
                    .get("action")
                    .and_then(serde_json::Value::as_str)
                    == Some("wait"))
            .then(CancellationToken::new);
            invocation.observation_interrupt = observation_interrupt.clone();
            let executions = state.executions();
            let mut descriptor = super::execution::ExecutionSnapshot::new(
                invocation.session_id.clone(),
                call.id.clone(),
                call.name.clone(),
                invocation.agent.clone(),
                invocation.mode.clone(),
            );
            // The invocation source survives UI projection truncation. Serialization
            // failure stays explicitly unavailable instead of hiding the call.
            descriptor.arguments = serde_json::to_string(&call.arguments).ok();
            let reserved =
                executions.admit_detached(descriptor, invocation.cancellation.clone())?;
            invocation.execution_id = Some(reserved);
            executions.mark(reserved, super::execution::ExecutionStatus::Waiting);
            recorder.record_task_admission(context).await?;
            let invocation_cancellation = invocation.cancellation.clone();
            let issuer = context.review_issuer(1024);
            let dispatch_call = call;
            #[cfg(feature = "wasm")]
            let action_error = match action_error {
                Some(error) => Some(error),
                None => tool_registry
                    .bind_file_versions(dispatch_call, &mut invocation)
                    .await
                    .err(),
            };
            let mut admission_error = None;
            let mut invocation_cancelled = false;
            if action_error.is_none() {
                let snapshot = self
                    .permission
                    .snapshot()
                    .map_err(|e| Error::config_with_private(e.public_reason(), e.to_string()))?;
                invocation.tool_description =
                    tool_registry.tool_description(&call.name).map(str::to_owned);
                let request = PermissionRequest {
                    subject: invocation.subject.clone(),
                    session_id: invocation.session_id.clone(),
                    call_id: invocation.call_id.clone(),
                    agent: snapshot.config.agent.clone(),
                    mode: snapshot.config.mode.clone(),
                    tool: call.name.clone(),
                    arguments: dispatch_call.arguments.clone(),
                    revision: snapshot.revision,
                };
                let component = tool_registry.admission_identity(&call.name);
                let result = tokio::select! { biased;
                    _ = cancellation.cancelled() => { executions.finish_error(reserved, &Error::Cancelled).await; return Err(Error::Cancelled); },
                    result = self.permission.admit(&request, &invocation, &issuer, component) => result,
                };
                match result {
                    Ok(proof) => invocation.admission = Some(proof),
                    Err(PermissionError::Cancelled) => invocation_cancelled = true,
                    Err(error) => admission_error = Some(error),
                }
            }
            let allowed = invocation.admission.is_some();
            let result = if invocation_cancelled || invocation_cancellation.is_cancelled() {
                Err(Error::Cancelled)
            } else if let Some(error) = action_error {
                Err(error)
            } else if allowed {
                // Secret placeholders remain until the sealed resolver. Image
                // dispatch additionally checks its exact bound policy witness.
                let prepared = tool_registry
                    .resolve_call(dispatch_call, &mut invocation)
                    .and_then(|resolved| {
                        executions.install_redaction(reserved, invocation.redaction_handle())?;
                        Ok(resolved)
                    });
                match prepared {
                    Err(error) => Err(error),
                    Ok(owned_call) => {
                        let mut descriptor = super::execution::ExecutionSnapshot::new(
                            invocation.session_id.clone(),
                            call.id.clone(),
                            call.name.clone(),
                            invocation.agent.clone(),
                            invocation.mode.clone(),
                        );
                        descriptor.execution_id = reserved;
                        let registry = tool_registry.clone();
                        let delegate = self.delegate.clone();
                        let controls = self.controls();
                        let reply_owner = self.reply_owner.get().cloned();
                        let execution_cancellation = invocation.cancellation.clone();
                        match executions.start_with_media_output(
                            descriptor,
                            execution_cancellation,
                            tool_registry.media_output_bytes(&call.name),
                            move |progress| {
                                invocation.execution_id = Some(progress.execution_id());
                                invocation.execution_progress = Some(progress);
                                async move {
                                    if registry.origin(&owned_call.name)
                                        == Some(super::tool::ToolOrigin::Builtin)
                                        && owned_call.name == "delegate"
                                    {
                                        super::tool::delegate::execute(
                                            delegate.as_deref(),
                                            &owned_call.arguments,
                                            invocation,
                                        )
                                        .await
                                    } else if registry.origin(&owned_call.name)
                                        == Some(super::tool::ToolOrigin::Builtin)
                                        && owned_call.name == "switch_mode"
                                    {
                                        super::tool::switch_mode::execute(
                                            controls.as_deref(),
                                            &owned_call.arguments,
                                            invocation,
                                        )
                                    } else {
                                        registry
                                            .execute(
                                                &owned_call,
                                                invocation,
                                                reply_owner.and_then(|owner| owner.upgrade()),
                                            )
                                            .await
                                    }
                                }
                            },
                        ) {
                            Err(error) => Err(error),
                            Ok(mut execution) => {
                                execution.activate()?;
                                // Tools own observation/yield and self-reported timeouts. Waiting
                                // for a question or a long tool is not Runtime timeout evidence.
                                let result = loop {
                                    tokio::select! {
                                        biased;
                                        _ = cancellation.cancelled() => {
                                            if call.name == "job" && super::execution::job::is_redirect(&call.arguments) {
                                                // Preserve a returned commit receipt even if the Turn waiter is cancelled.
                                                // A worker lost after dispatch is not evidence of no mutation.
                                                let _ = executions.cancel(execution.id).await;
                                                break match execution.wait_output().await {
                                                    Err(Error::Closed | Error::ExecutionInterrupted) =>
                                                        Err(super::execution::job::uncertain_redirect(reserved)),
                                                    result => result,
                                                };
                                            }
                                            executions.cancel(execution.id).await?;
                                            let _ = execution.wait_output().await;
                                            break Err(Error::Cancelled);
                                        },
                                        result = execution.wait_output() => {
                                            break match result {
                                                Err(Error::Closed | Error::ExecutionInterrupted)
                                                    if call.name == "job" && super::execution::job::is_redirect(&call.arguments) =>
                                                {
                                                    Err(super::execution::job::uncertain_redirect(reserved))
                                                }
                                                result => result,
                                            };
                                        },
                                        _ = recorder.wait_for_steer(), if observation_interrupt
                                            .as_ref().is_some_and(|signal| !signal.is_cancelled()) => {
                                            // Only the read-only observation is interrupted. Job
                                            // returns its own final result through normal recording.
                                            if let Some(signal) = &observation_interrupt {
                                                signal.cancel();
                                            }
                                        },
                                    }
                                };
                                // A Buffered return transfers lifetime to its Session owner.
                                execution.detach();
                                result
                            }
                        }
                    }
                }
            } else {
                Err(admission_error
                    .unwrap_or(PermissionError::Denied)
                    .into_runtime_error())
            };
            if let Err(error) = &result {
                executions.finish_error(reserved, error).await;
            }
            let result = result.and_then(|output| {
                let budget = if call.name == "job" {
                    crate::runtime::execution::job::output_budget(&limits)
                } else {
                    limits.max_tool_output_bytes
                };
                crate::budget::check(crate::LimitKind::ToolOutput, output.content.len(), budget)
                    .map_err(|error| match error {
                        Error::Limit(limit) => Error::Limit(limit.with_label("tool output budget")),
                        other => other,
                    })?;
                Ok(output)
            });
            // Some valid output receipts carry a failed terminal execution while
            // the invocation still returns a body to Context. Read the exact
            // reservation, never infer its outcome from that body.
            use super::execution::ExecutionStatus;
            let terminal_status = match executions.get(reserved).ok().map(|row| row.status) {
                Some(ExecutionStatus::Failed) => Some("failed"),
                Some(ExecutionStatus::Cancelled) => Some("cancelled"),
                Some(ExecutionStatus::Interrupted) => Some("interrupted"),
                _ if matches!(&result, Err(Error::Cancelled)) => Some("cancelled"),
                _ if result.is_err() => Some("failed"),
                _ => None,
            };
            // Use one canonical safe error projection for both the admitted
            // model feedback and ToolResult audit (media retains metadata only).
            let error_feedback = result.as_ref().err().map(|error| {
                let content = match error {
                    Error::ToolFailure(failure) => {
                        serde_json::json!({"content":"","failure":failure}).to_string()
                    }
                    _ => executions.error_result_with_effects(reserved, error),
                };
                if content.len() > error_result_reservation_bytes()
                    || serde_json::json!(&content).to_string().len() - 2
                        > error_result_context_reservation_bytes()
                {
                    error.tool_diagnostic().result_json()
                } else {
                    content
                }
            });
            let note = (text_conversions != 0).then_some(TEXT_ARGUMENT_CONVERSION_NOTE);
            let with_note = |mut content: String| {
                if let Some(note) = note {
                    content.push_str(note);
                }
                content
            };
            // Every returned outcome, including terminal errors, is observable.
            // ToolResult never enters the intervention lane.
            let snapshot = state.snapshot();
            let payload = match &result {
                Ok(output) => HookPayload::ToolResult {
                    result: Some(super::execution::ResolvedOutput::observation_text(
                        &with_note(output.content.clone()),
                        &output.attachments,
                    )?),
                    error: None,
                },
                Err(_) => HookPayload::ToolResult {
                    result: None,
                    error: error_feedback.clone().map(with_note),
                },
            };
            if let HookPayload::ToolResult { result, error } = &payload
                && let Some(content) = result.as_ref().or(error.as_ref())
            {
                executions.retain_invocation_result(reserved, content)?;
            }
            auditor.publish(
                Moment::ToolResult,
                AuditBody {
                    epoch: snapshot.epoch_id,
                    turn_id: snapshot.turn_id,
                    round: snapshot.round_id,
                    tool_call_id: Some(call.id.clone()),
                    tool_execution_id: Some(reserved.0),
                    tool_presentation: tool_registry.interaction_presentation(&call.name),
                    started_at_unix_ms: clock.0,
                    elapsed_ms: u64::try_from(clock.1.elapsed().as_millis()).ok(),
                    payload,
                    ..Default::default()
                },
            );
            let output = match result {
                Ok(output) => output,
                Err(Error::Cancelled) if cancellation.is_cancelled() => {
                    return Err(Error::Cancelled);
                }
                Err(Error::Cancelled) => super::execution::ResolvedOutput::text(
                    error_feedback.unwrap_or_else(|| Error::Cancelled.tool_diagnostic().result_json()),
                ),
                Err(Error::Limit(message)) => return Err(Error::Limit(message)),
                Err(error)
                    if tool_registry.origin(&call.name) == Some(super::tool::ToolOrigin::DialogueReply)
                        && error.tool_diagnostic().effects
                            == crate::observation::diagnostics::EffectKnowledge::MayHaveOccurred =>
                {
                    // Do not continue this Turn under an unacknowledged append.
                    return Err(error);
                }
                Err(error) => {
                    super::execution::ResolvedOutput::text(
                        error_feedback.unwrap_or_else(|| error.tool_diagnostic().result_json()),
                    )
                }
            };
            let parts = output
                .attachments
                .into_iter()
                .map(|attachment| microcrab_protocol::InputPart::Image {
                    source: microcrab_protocol::ImageSource::Data {
                        media_type: attachment.media_type,
                        data: attachment.data,
                    },
                })
                .collect();
            context.append_tool_result_with_parts(
                call.id.clone(),
                with_note(output.content),
                parts,
            )?;
            let (epoch, items) = context.admitted_items();
            if let Some(assistant_index) = assistant_index {
                client
                    .observe_tool_result_admitted(
                        epoch,
                        items,
                        assistant_index,
                        call_index,
                        reserved.0,
                        terminal_status,
                    )
                    .await;
            } else {
                client.observe_admitted(epoch, items, None).await;
            }
            executions.acknowledge_result_delivered(reserved);
            recorder.observe_context(context)?;
        }
        Ok(())
    }
}

fn is_pending_work_wait(call: &microcrab_protocol::ToolCall) -> bool {
    call.name == "sleep"
        && call
            .arguments
            .get("time_ms")
            .and_then(serde_json::Value::as_i64)
            == Some(-1)
}
const TEXT_ARGUMENT_CONVERSION_NOTE: &str = "\n[Runtime: schema-opted-in textual argument conversion applied on the execution copy. Original tool-call evidence is unchanged.]";

fn error_result_reservation_bytes() -> usize {
    crate::observation::diagnostics::ToolDiagnostic::max_result_bytes()
        .max(super::execution::ToolFailure::max_result_bytes())
}
fn error_result_context_reservation_bytes() -> usize {
    crate::observation::diagnostics::ToolDiagnostic::max_context_result_bytes()
        .max(super::execution::ToolFailure::max_context_result_bytes())
}
fn tool_result_reservation_bytes(
    call: &microcrab_protocol::ToolCall,
    limits: &crate::Limits,
    declared_media_bytes: usize,
) -> usize {
    let output = if call.name == "job"
        && call
            .arguments
            .get("action")
            .and_then(serde_json::Value::as_str)
            .and_then(crate::runtime::execution::job::canonical)
            .is_some_and(|action| action == "read")
    {
        crate::runtime::execution::job::output_budget(limits)
    } else {
        limits.max_tool_output_bytes
    };
    // Success text can escape sixfold. Error encoders already account for their
    // own JSON and outer string escaping; multiplying that whole JSON by six
    // again rejects valid small Context budgets unnecessarily.
    output
        .saturating_mul(6)
        .max(error_result_context_reservation_bytes())
        .saturating_add(TEXT_ARGUMENT_CONVERSION_NOTE.len() + 1)
        .saturating_add(crate::admission::image::tool_context_reservation(
            declared_media_bytes.min(limits.max_tool_image_bytes),
        ))
}
fn reserve_tool_result(
    context: &mut Context,
    call: &microcrab_protocol::ToolCall,
    limits: &crate::Limits,
    declared_media_bytes: usize,
) -> Result<(), Error> {
    let bytes = tool_result_reservation_bytes(call, limits, declared_media_bytes);
    crate::budget::check(
        crate::LimitKind::ToolResultReservation,
        bytes,
        limits.max_context_bytes,
    )
    .map_err(|error| match error {
        Error::Limit(limit) => Error::Limit(limit.calls(1)),
        other => other,
    })?;
    context
        .append_tool_result(call.id.clone(), "x".repeat(bytes))
        .map_err(|error| match error {
            Error::Limit(limit) => Error::Limit(limit.before_effects()),
            other => other,
        })
}

impl RuntimeHandler for Runtime {
    async fn execute_turn(
        &self,
        state: &SessionState,
        context: &mut Context,
        client: &dyn Client,
        auditor: &dyn Auditor,
        cancellation: CancellationToken,
    ) -> Result<TurnOutcome, Error> {
        self.execute_live(
            &State::from_snapshot(state.clone()),
            context,
            client,
            auditor,
            cancellation,
            &NoopRecorder,
            None,
            None,
        )
        .await
    }
}

/// One settled boundary chooses whether there is real input for another request.
#[derive(Default)]
pub(crate) struct StepAdmission {
    pub user: bool,
    pub send: bool,
    pub blocked: bool,
}

pub(crate) async fn start_round(
    state: &State,
    round: usize,
    auditor: &dyn Auditor,
    cancellation: &CancellationToken,
) -> Result<(), Error> {
    state.update(|snapshot| snapshot.round_id = Some(round as u64));
    let scope = state.snapshot();
    state
        .cache_points()
        .begin(scope.epoch_id, scope.turn_id.unwrap_or(0), round as u64)?;
    crate::observation::hooks::boundary(auditor, state, Boundary::RoundStarted, cancellation).await
}

/// Commits only settled model/tool batches; no partial generated call is recoverable.
pub(crate) trait RoundRecorder: Send + Sync {
    fn wait_for_steer(&self) -> impl std::future::Future<Output = ()> + Send {
        std::future::pending()
    }
    fn request_route(&self, _: &crate::context::Content) -> Result<Option<String>, Error> {
        Ok(None)
    }
    fn check_request(&self, _: &Context) -> Result<(), Error> {
        Ok(())
    }
    fn compact_boundary(
        &self,
        _: &mut Context,
        _: Option<RoundContinuation>,
        _: bool,
        _: &CancellationToken,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send {
        async { Ok(()) }
    }
    fn round_exhaustion(&self) -> crate::budget::Exhaustion {
        crate::budget::Exhaustion::Stop
    }
    fn stop_at_round_limit(&self) {}
    /// Transient evidence, set only after durable recording of an unadmitted attempt.
    fn set_model_retry_ready(&self, _ready: bool) {}
    #[expect(
        clippy::too_many_arguments,
        reason = "One boundary borrows the existing execution owners without a second loop"
    )]
    fn admit_step(
        &self,
        state: &State,
        context: &mut Context,
        allow_framework: bool,
        sending: bool,
        round: usize,
        auditor: &dyn Auditor,
        cancellation: &CancellationToken,
    ) -> impl std::future::Future<Output = Result<StepAdmission, Error>> + Send {
        async move {
            if sending && !allow_framework {
                return Ok(StepAdmission {
                    blocked: true,
                    ..Default::default()
                });
            }
            if sending {
                start_round(state, round, auditor, cancellation).await?;
                context.synchronize_state(&state.model_variables()?)?;
            }
            Ok(StepAdmission {
                user: false,
                send: sending,
                blocked: false,
            })
        }
    }

    fn observe_context(&self, _: &Context) -> Result<(), Error> {
        Ok(())
    }
    fn record_task_admission(
        &self,
        _: &Context,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send {
        async { Ok(()) }
    }
    fn record(
        &self,
        context: &Context,
        continuation: Option<RoundContinuation>,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;
}
struct NoopRecorder;
impl RoundRecorder for NoopRecorder {
    async fn record(&self, _: &Context, _: Option<RoundContinuation>) -> Result<(), Error> {
        Ok(())
    }
}
