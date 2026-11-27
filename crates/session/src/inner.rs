//! Session-owned resources. Command execution and transitions live in child modules.
mod activity;
mod builder;
mod commands;
pub(crate) mod delegation;
mod execution;
pub(crate) use execution::OperationReservation;
mod communication;
mod configuration;
mod fork;
pub(crate) mod functional;
mod insertion;
mod model_admission;
mod permissions;
mod recovery;
use crate::boundary::Boundary;
use crate::context::Context;
use crate::runtime::{Runtime, TurnOutcome};
use crate::utils::cancel::CancelOnDrop;
use crate::{Client, Error, Limits, SessionState, State};
use futures_util::FutureExt;
use microcrab_agent_types::AgentInfo;
use microcrab_auditor::{AuditOutcome, Auditor};
use serde_json::Value;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

fn switch_mode_available(
    kind: crate::SessionKind,
    frozen_tools: &[microcrab_protocol::ToolDefinition],
) -> bool {
    use crate::runtime::BuiltinTool;
    // The registry reserves this name for the native builtin; recovery validates
    // the frozen definition against its installed implementation.
    matches!(
        kind,
        crate::SessionKind::Interactive | crate::SessionKind::Delegated
    ) && frozen_tools
        .iter()
        .any(|definition| BuiltinTool::from_name(&definition.name) == Some(BuiltinTool::SwitchMode))
}

// Close callers share one owned worker. Only execution/process cleanup may be
// reconciled explicitly; terminal hooks, persistence and journal errors stay sticky.
#[derive(Clone)]
struct SessionCloseOutcome {
    cleanup_pending: bool,
    terminal_failed: bool,
    persistence_failed: bool,
}
struct SessionClose {
    sender: tokio::sync::watch::Sender<Option<SessionCloseOutcome>>,
}

pub(crate) struct SessionInner {
    pub(crate) prompt_config: crate::context::system::PromptConfig,
    pub(crate) kind: crate::SessionKind,
    directory: std::sync::RwLock<Arc<crate::runtime::messaging::LineageDirectory>>,
    communication_transaction: Arc<tokio::sync::Mutex<()>>,
    communication_loaded: AtomicBool,
    // Also the reply mutation fence: terminal settlement joins a started write.
    dialogue_reply_store:
        tokio::sync::Mutex<Option<Arc<dyn crate::state::communication::reply::DialogueReplyStore>>>,
    durable_carrier_consumption: Mutex<
        std::collections::BTreeMap<String, crate::state::communication::inbox::OperationReceipt>,
    >,
    recovered_child_reservations:
        Mutex<std::collections::BTreeMap<String, crate::state::communication::AgentId>>,
    pub(crate) one_shot_claimed: AtomicBool,
    #[expect(
        clippy::type_complexity,
        reason = "The optional watched cleanup receipt distinguishes absent, pending and acknowledged outcomes"
    )]
    delegation_completion: Mutex<
        Option<
            tokio::sync::watch::Receiver<
                Option<Result<(), crate::observation::diagnostics::ToolDiagnostic>>,
            >,
        >,
    >,
    inbox: Arc<crate::admission::inbox::Inbox>,
    _notice_sink: Arc<dyn crate::runtime::execution::ExecutionNoticeSink>,
    observation: crate::observation::activity::ActivityPublisher,
    inbox_worker_started: AtomicBool,
    explicit_activation: AtomicBool,
    model_retry_ready: AtomicBool,
    replacing: AtomicBool,
    replacement: Arc<tokio::sync::Mutex<()>>,
    #[expect(
        clippy::type_complexity,
        reason = "Pending tool publication retains its exact generation beside its owned tool definitions"
    )]
    pending_tools: Mutex<Option<(u64, Vec<Arc<dyn crate::runtime::tool::Tool>>)>>,
    applied_tools: std::sync::atomic::AtomicU64,
    route: Option<Arc<dyn crate::session::TurnRouting>>,
    model_admission: Option<Arc<dyn crate::ModelAdmission>>,
    inbox_validation_failed: AtomicBool,
    client: Arc<dyn Client>,
    auto_compaction: Mutex<
        Option<(
            crate::context::AutoCompactionPolicy,
            Arc<dyn crate::context::TokenEstimator>,
        )>,
    >,
    auto_compaction_interrupted: AtomicBool,
    round_limit_callback: Option<Arc<dyn crate::budget::RoundLimitCallback>>,
    pub(crate) attachment_resolver: Option<Arc<dyn crate::admission::image::AttachmentResolver>>,
    pub(crate) processes: Arc<crate::runtime::execution::process::ProcessRegistry>,
    pub(crate) is_main: bool,
    process_parent_session_id: Option<String>,
    state: State,
    configured_limits: Mutex<Limits>,
    // Explicit acknowledged mode intent overrides later historical selection.
    // None preserves core recovery's historical-mode behavior; not a policy copy.
    configured_mode: Mutex<(u64, Option<String>)>,
    pending_mode: Mutex<Option<PendingMode>>,
    delegated_children: Mutex<std::collections::BTreeMap<String, delegation::ChildRecord>>,
    #[expect(
        clippy::type_complexity,
        reason = "Closed child history retains exact child identity, target and acknowledged cleanup result"
    )]
    closed_children: Mutex<
        std::collections::VecDeque<(
            String,
            String,
            Result<(), crate::observation::diagnostics::ToolDiagnostic>,
        )>,
    >,
    pub(crate) parent_relationship: Mutex<Option<crate::DelegationRelationship>>,
    context: Mutex<Context>,
    live_context_measurement: Mutex<Option<crate::context::ContextMeasurement>>,
    context_measurement_changes: tokio::sync::watch::Sender<()>,
    runtime: Runtime,
    auditor: Arc<crate::observation::hooks::BoundAuditor>,
    rename: Mutex<Option<functional::RenameTask>>,
    one_shots: Arc<functional::one_shot::SessionOneShot>,
    active: Mutex<Option<commands::ActiveOperation>>,
    #[cfg(test)]
    command_registration_gate: Mutex<Option<Arc<commands::tests::RegistrationGate>>>,
    operation: Arc<tokio::sync::Mutex<()>>,
    // Public work excludes public work; internal checkpoints only share storage
    // serialization and must never surface as a competing foreground operation.
    foreground: Arc<tokio::sync::Mutex<()>>,
    task_persisted_generation: std::sync::atomic::AtomicU64,
    // Derived witness of the last explicitly selected immutable Journal bytes.
    // Live task observations remain authoritative in the registry.
    selected_checkpoint: Mutex<Option<(u64, crate::runtime::journal::Recover)>>,
    // Foreground/restore owns transitions. A refreshed recovery defers its entry
    // baseline until the first new input, preserving any original continuation.
    navigation_epoch: Mutex<Option<u64>>,
    started: AtomicBool,
    closed: Arc<AtomicBool>,
    faulted: Arc<AtomicBool>,
    host: Option<Arc<dyn microcrab_host_api::Host>>,
    endpoint: Arc<crate::state::endpoint::VariableEndpoint>,
    journal: Arc<dyn crate::runtime::journal::Journal>,
    agent: Option<AgentInfo>,
    catalog: Vec<AgentInfo>,
    continuation: Mutex<Option<crate::runtime::journal::RoundContinuation>>,
    cache_report: Mutex<Option<crate::runtime::journal::RoundCacheReport>>,
    restored: AtomicBool,
    self_weak: std::sync::Weak<SessionInner>,
    task_persistence_started: AtomicBool,
    task_persistence_cancel: CancellationToken,
    close_result: Mutex<Option<SessionClose>>,
}

pub(crate) struct SessionParts {
    pub kind: crate::SessionKind,
    pub communication: Option<crate::state::communication::CommunicationState>,
    pub dialogue_reply_store:
        Option<Arc<dyn crate::state::communication::reply::DialogueReplyStore>>,
    pub client: Arc<dyn Client>,
    pub route: Option<Arc<dyn crate::session::TurnRouting>>,
    pub model_admission: Option<Arc<dyn crate::ModelAdmission>>,
    pub auto_compaction: Option<(
        crate::context::AutoCompactionPolicy,
        Arc<dyn crate::context::TokenEstimator>,
    )>,
    pub round_limit_callback: Option<Arc<dyn crate::budget::RoundLimitCallback>>,
    pub attachment_resolver: Option<Arc<dyn crate::admission::image::AttachmentResolver>>,
    pub runtime: Runtime,
    pub prompt_config: crate::context::system::PromptConfig,
    pub materialized_system: Option<String>,
    pub limits: Limits,
    pub agent: Option<AgentInfo>,
    pub auditor: Arc<dyn Auditor>,
    pub journal: Arc<dyn crate::runtime::journal::Journal>,
    pub host: Option<Arc<dyn microcrab_host_api::Host>>,
    pub references: Vec<crate::DynReference>,
    pub default_writable_reference: Option<String>,
    pub session_id: String,
    pub processes_parent: Option<Arc<dyn crate::runtime::execution::process::ProcessControl>>,
    pub catalog: Vec<AgentInfo>,
    pub is_main: bool,
    pub permissions: Option<crate::policy::PermissionConfig>,
    pub review_client: Option<Arc<dyn Client>>,
    pub permission_parent: Option<Arc<crate::policy::PermissionController>>,
}

impl SessionInner {
    pub(crate) fn is_faulted(&self) -> bool {
        self.faulted.load(Ordering::Acquire)
    }
    async fn boundary(
        &self,
        event: Boundary,
        _outcome: Option<AuditOutcome>,
        cancellation: &CancellationToken,
    ) -> Result<(), Error> {
        let epoch_release =
            (event == Boundary::EpochEnded).then(|| EndWasmEpoch(self.state.clone()));
        let result = crate::observation::hooks::boundary(
            self.auditor.as_ref(),
            &self.state,
            event,
            cancellation,
        )
        .await;
        drop(epoch_release);
        result
    }

    pub(crate) fn validate_mode(&self, mode: &str) -> Result<(), Error> {
        self.ensure_open()?;
        let selected = self
            .agent
            .as_ref()
            .and_then(|agent| agent.modes.iter().find(|entry| entry.name == mode))
            .ok_or_else(|| Error::config("unknown agent mode"))?;
        if !crate::session::mode_role_eligible(self.is_main, selected.role) {
            return Err(Error::config(
                "agent mode is not eligible for this session role",
            ));
        }
        Ok(())
    }
    pub(crate) async fn select_mode(self: &Arc<Self>, mode: String) -> Result<(), Error> {
        self.select_mode_now(mode, None)
    }
    fn select_mode_now(&self, mode: String, expected: Option<&PendingMode>) -> Result<(), Error> {
        self.validate_mode(&mode)?;
        self.ensure_open()?;
        let mut configured = self
            .configured_mode
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(expected) = expected
            && (expected.generation != configured.0 || expected.epoch != self.state().epoch_id)
        {
            return Err(Error::tool(
                "pending mode transition rejected because its epoch or mode generation changed",
            ));
        }
        let generation = configured
            .0
            .checked_add(1)
            .ok_or_else(|| Error::config("mode admission generation overflow"))?;
        if let Some(expected) = expected {
            let agent = self
                .agent
                .as_ref()
                .ok_or_else(|| Error::config("Session has no Agent modes"))?;
            let selected = agent
                .modes
                .iter()
                .find(|entry| entry.name == mode)
                .ok_or_else(|| Error::config("unknown Agent mode"))?;
            self.permission_controller()
                .configure_mode_at(
                    expected.revision,
                    &agent.name,
                    &selected.name,
                    &selected.permissions,
                )
                .map_err(|e| Error::config_with_private(e.public_reason(), e.to_string()))?;
        } else {
            self.install_policy_mode(Some(&mode))?;
        }

        self.state
            .update(|state| state.selected_mode = Some(mode.clone()));
        if let Err(error) = self.state.refresh_preview(Boundary::SessionRestored) {
            self.faulted.store(true, Ordering::Release);
            return Err(error);
        }
        *configured = (generation, Some(mode));
        Ok(())
    }
    fn install_policy_mode(&self, mode: Option<&str>) -> Result<(), Error> {
        let Some(agent) = &self.agent else {
            if mode.is_some() {
                return Err(Error::config("session has no agent modes"));
            }
            return Ok(());
        };
        let selected = match mode {
            Some(name) => agent.modes.iter().find(|m| m.name == name),
            None => agent.modes.first(),
        };
        let Some(selected) = selected else {
            if mode.is_none() {
                return Ok(());
            }
            return Err(Error::config("unknown agent mode"));
        };
        self.runtime
            .permission_controller()
            .configure_mode(&agent.name, &selected.name, &selected.permissions)
            .map_err(|error| Error::config_with_private(error.public_reason(), error.to_string()))
    }
    pub(crate) fn permission_controller(&self) -> Arc<crate::policy::PermissionController> {
        self.runtime.permission_controller()
    }
    fn bind_frozen_tools(&self, definitions: &[microcrab_protocol::ToolDefinition]) {
        self.state.set_model_permission_names(definitions);
        self.state
            .set_short_model_refs(crate::runtime::tool::short_model_refs(definitions));
        self.state.install_delegate_availability(
            self.runtime.delegate_available()
                && definitions
                    .iter()
                    .any(|definition| definition.name == "delegate"),
        );
        self.state
            .install_switch_mode_availability(switch_mode_available(self.kind, definitions));
    }
    pub(crate) fn state(&self) -> SessionState {
        self.state.snapshot()
    }
    pub(crate) fn state_handle(&self) -> State {
        self.state.clone()
    }
    pub(crate) fn auto_compaction_retry_available(&self) -> bool {
        self.auto_compaction
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_some_and(|(policy, _)| policy.enabled)
            && !self.retry_ready()
            && self
                .context
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .auto_compaction_retry_available()
    }

    pub(crate) fn compaction(&self) -> Option<crate::context::CompactionMarker> {
        self.context
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .compaction()
            .cloned()
    }

    pub(crate) fn context_snapshot(&self) -> Result<crate::context::ContextSnapshot, Error> {
        self.context
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .snapshot()
    }
    pub(crate) fn context_measurement_changes(&self) -> tokio::sync::watch::Receiver<()> {
        self.context_measurement_changes.subscribe()
    }
    pub(crate) fn with_context_measurement<R>(
        &self,
        project: impl FnOnce(&crate::context::ContextMeasurement) -> R,
    ) -> Result<R, Error> {
        let live = self
            .live_context_measurement
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let context = self
            .context
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(measurement) = live.as_ref() {
            return Ok(project(measurement));
        }
        Ok(project(&context.measure()?))
    }
    pub(crate) fn context_measurement(&self) -> Result<crate::context::ContextMeasurement, Error> {
        if let Some(measurement) = self
            .live_context_measurement
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
        {
            return Ok(measurement);
        }
        self.context
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .measure()
    }
    pub(crate) fn messages(&self) -> Vec<crate::context::ContentItem> {
        self.context
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .items()
            .to_vec()
    }

    fn ensure_open(&self) -> Result<(), Error> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        if self.faulted.load(Ordering::SeqCst) {
            return Err(Error::config("session faulted; create a new session"));
        }
        Ok(())
    }

    fn finish_turn(&self) {
        if !self.has_continuation() {
            self.state.update(|state| {
                state.turn_id = None;
                state.round_id = None;
                state.dialogue_turn_live = false;
            });
        }
        *self.active.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }
}

impl Drop for SessionInner {
    fn drop(&mut self) {
        if self.is_main {
            self.processes.abort();
        } else {
            self.processes
                .seal_session(self.state.processes().session_id());
        }
        self.task_persistence_cancel.cancel();
        if let Some(rename) = self
            .rename
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            rename.abort();
        }
        self.state.executions().abort();
        self.state.close_wasm();
        self.endpoint.shutdown();
    }
}

/// Epoch resources must be released even when lifecycle delivery is interrupted.
struct EndWasmEpoch(State);
impl Drop for EndWasmEpoch {
    fn drop(&mut self) {
        self.0.end_wasm_epoch();
    }
}

/// A delivered boundary may outlive a dropped future. Fault until a complete
/// transition has committed both State and Context.
struct FaultOnDrop<'a> {
    faulted: &'a AtomicBool,
    armed: bool,
}
impl Drop for FaultOnDrop<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.faulted.store(true, Ordering::SeqCst);
        }
    }
}

struct TurnGuard {
    inner: Arc<SessionInner>,
    clear: bool,
}
impl Drop for TurnGuard {
    fn drop(&mut self) {
        if self.clear {
            self.inner.finish_turn();
        } else {
            *self.inner.active.lock().unwrap_or_else(|e| e.into_inner()) = None;
        }
    }
}

struct PendingMode {
    target: String,
    epoch: u64,
    revision: u64,
    generation: u64,
}
impl crate::runtime::tool::switch_mode::SessionControl for SessionInner {
    fn request_mode(
        &self,
        mode: &str,
        invocation: &crate::runtime::tool::ToolContext,
    ) -> Result<Value, Error> {
        self.validate_mode(mode)?;
        if self.kind == crate::SessionKind::OneShot {
            return Err(Error::denied("one-shot Sessions cannot switch modes"));
        }
        let configured = self
            .configured_mode
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let snapshot = self.state();
        if invocation.session_id != self.state.processes().session_id()
            || invocation.mode != snapshot.selected_mode
        {
            return Err(Error::tool("mode request has a stale Session/mode witness"));
        }
        let revision = self
            .permission_controller()
            .snapshot()
            .map_err(|e| Error::config_with_private(e.public_reason(), e.to_string()))?
            .revision;
        let mut pending = self.pending_mode.lock().unwrap_or_else(|e| e.into_inner());
        if pending.is_some() {
            return Err(Error::tool(
                "a mode transition is already pending for this batch",
            ));
        }
        *pending = Some(PendingMode {
            target: mode.into(),
            epoch: snapshot.epoch_id,
            revision,
            generation: configured.0,
        });
        Ok(
            serde_json::json!({"requested":mode,"pending":true,"applied":false,"apply_point":"after_tool_batch"}),
        )
    }
    fn apply_pending_mode(&self) -> Result<(), Error> {
        let Some(pending) = self
            .pending_mode
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        else {
            return Ok(());
        };
        self.select_mode_now(pending.target.clone(), Some(&pending))
    }
    fn discard_pending_mode(&self) {
        self.pending_mode
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
    }
}
