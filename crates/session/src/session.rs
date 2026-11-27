//! Session admission, dynamic backend assembly and caller-visible stream ownership.
use crate::ModelEvent;
use crate::context::{ContentItem, system::PromptConfig};
use crate::inner::{SessionInner, SessionParts};
use crate::runtime::{
    Runtime, TurnOutcome,
    journal::{Journal, MemoryJournal, Recover},
};
use crate::{Client, DynReference, Error, Limits, SessionState, State};
use microcrab_auditor::{AuditRecord, Auditor, ChannelAuditor};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Parent-owned exclusivity for one exact restored child, held before native
/// Session/process registration until the new endpoint is published or rejected.
#[must_use]
pub struct RecoveredChildReservation {
    pub(crate) parent: Arc<SessionInner>,
    pub(crate) session_id: String,
    pub(crate) agent_id: crate::state::communication::AgentId,
}
impl Drop for RecoveredChildReservation {
    fn drop(&mut self) {
        self.parent
            .release_recovered_child_reservation(&self.session_id, &self.agent_id);
    }
}
/// Safe interruption facts supplied by the embedding owner. This vocabulary
/// carries no provider bodies, Host messages, or caller-controlled detail text.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum InterruptReason {
    UserRequested,
    HostDetached { code: HostDetachCode },
    SessionClosed,
    StateReplaced,
    ClientToolDisconnected,
}

/// Closed Host route causes; an unrecognized cause maps to Unavailable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostDetachCode {
    Disconnected,
    Replaced,
    LeaseExpired,
    BindingMismatch,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionKind {
    #[default]
    Interactive,
    Delegated,
    OneShot,
}

pub(crate) fn mode_role_eligible(is_main: bool, role: microcrab_agent_types::AgentRole) -> bool {
    use microcrab_agent_types::AgentRole;
    !matches!(
        (is_main, role),
        (true, AgentRole::SubAgent) | (false, AgentRole::MainAgent)
    )
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegateRequest {
    pub target: String,
    pub mode: Option<String>,
    pub issuer_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegationRelationship {
    pub parent_session_id: String,
    pub parent_execution_id: crate::runtime::execution::ExecutionId,
    pub issuer_epoch: u64,
    pub target: String,
    pub mode: String,
}

pub struct Session {
    pub(crate) inner: Arc<SessionInner>,
}
impl Clone for Session {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
/// Creator-owned routing participates in actual new-Turn admission and durable
/// checkpoints. Provider identity and credentials never enter frozen Recover bytes.
#[async_trait::async_trait]
pub trait TurnRouting: Send + Sync {
    async fn prepare(&self) -> Result<Option<crate::ConfigurationEffect>, Error>;
    fn apply(&self, epoch: u64) -> Result<(), Error>;
    fn client(&self, epoch: u64) -> Arc<dyn Client>;
    async fn before_checkpoint(&self, recover: &Recover) -> Result<(), Error>;
    async fn checkpoint_committed(&self, recover: &Recover) -> Result<(), Error>;
    async fn validate_restore(&self, _recover: &Recover) -> Result<(), Error> {
        Ok(())
    }
    /// The route owner requests a fresh live epoch when the selected route has
    /// changed or lacks a historical witness. The original Recover is still sent
    /// unchanged to Journal::recovered; restore receives the validated live copy.
    fn refresh_context_on_restore(&self) -> bool {
        false
    }
    async fn restore(&self, recover: &Recover) -> Result<(), Error>;
}

use crate::state::communication::{
    AgentId,
    reply::{DialogueAdmissionKey, DialogueReplyStore, DurableDialogueReply},
};

type DialogueReplyStoreFactory =
    Box<dyn FnOnce(&AgentId) -> Result<Arc<dyn DialogueReplyStore>, Error> + Send>;

pub struct SessionBuilder {
    kind: Option<SessionKind>,
    communication: Option<Option<crate::state::communication::CommunicationState>>,
    interaction: crate::state::communication::SessionInteraction,
    dialogue_reply_store: Option<DialogueReplyStoreFactory>,
    route: Option<Arc<dyn TurnRouting>>,
    model_admission: Option<Arc<dyn crate::ModelAdmission>>,
    client: Arc<dyn Client>,
    auto_compaction: Option<(
        crate::context::AutoCompactionPolicy,
        Arc<dyn crate::context::TokenEstimator>,
    )>,
    review_client: Option<Arc<dyn Client>>,
    runtime: Runtime,
    prompt_config: PromptConfig,
    materialized_system: Option<String>,
    limits: Limits,
    agent: Option<microcrab_agent_types::AgentInfo>,
    auditor: Arc<dyn Auditor>,
    journal: Arc<dyn Journal>,
    host: Option<Arc<dyn microcrab_host_api::Host>>,
    references: Vec<DynReference>,
    default_writable_reference: Option<String>,
    session_id: String,
    processes_parent: Option<Arc<dyn crate::runtime::execution::process::ProcessControl>>,
    catalog: Vec<microcrab_agent_types::AgentInfo>,
    is_main: Option<bool>,
    permissions: Option<crate::policy::PermissionConfig>,
    permission_parent: Option<Arc<crate::policy::PermissionController>>,
    round_limit_callback: Option<Arc<dyn crate::budget::RoundLimitCallback>>,
    attachment_resolver: Option<Arc<dyn crate::admission::image::AttachmentResolver>>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionEvent {
    Model { event: ModelEvent },
    Finished { outcome: TurnOutcome },
}
/// Keeps foreground/storage admission reserved while an embedding owner records
/// a terminal stream receipt. Drop before checkpointing or admitting another turn.
/// Dropping it before worker settlement is safe: the worker retains its own owner.
#[must_use = "retain until the terminal stream receipt has settled"]
pub struct SettlementGuard {
    _reservation: Arc<crate::inner::OperationReservation>,
}

/// Dropping a stream interrupts its owned worker; it never abandons tool cleanup.
pub struct SessionStream {
    receiver: mpsc::Receiver<Result<SessionEvent, Error>>,
    cancellation: CancellationToken,
    interrupt_reason: Arc<std::sync::Mutex<Option<InterruptReason>>>,
}
impl SessionStream {
    /// The first explicit cause for this operation. Caller-token or consumer
    /// cancellation remains None; later Session operations never change this fact.
    pub fn interrupt_reason(&self) -> Option<InterruptReason> {
        *self
            .interrupt_reason
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }
    pub async fn recv(&mut self) -> Option<Result<SessionEvent, Error>> {
        self.receiver.recv().await
    }
}
impl Drop for SessionStream {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}
static NEXT_SESSION_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

impl Session {
    pub fn builder<C: Client>(client: C, runtime: Runtime) -> SessionBuilder {
        Self::builder_arc(Arc::new(client), runtime)
    }
    pub fn builder_arc(client: Arc<dyn Client>, runtime: Runtime) -> SessionBuilder {
        SessionBuilder {
            kind: None,
            communication: None,
            dialogue_reply_store: None,
            interaction: crate::state::communication::SessionInteraction::Persistent,
            route: None,
            model_admission: None,
            client,
            auto_compaction: None,
            review_client: None,
            runtime,
            prompt_config: PromptConfig::default(),
            materialized_system: None,
            limits: Limits::default(),
            agent: None,
            auditor: Arc::new(ChannelAuditor::disconnected()),
            journal: Arc::new(MemoryJournal::new()),
            host: None,
            references: Vec::new(),
            default_writable_reference: None,
            session_id: format!(
                "session-{}",
                NEXT_SESSION_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ),
            processes_parent: None,
            catalog: Vec::new(),
            is_main: None,
            permissions: None,
            permission_parent: None,
            round_limit_callback: None,
            attachment_resolver: None,
        }
    }
}
impl Session {
    pub fn prompt_config(&self) -> &PromptConfig {
        &self.inner.prompt_config
    }
    pub fn kind(&self) -> SessionKind {
        self.inner.kind
    }
    pub fn parent_relationship(&self) -> Option<DelegationRelationship> {
        self.inner
            .parent_relationship
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    /// Accept prepared external dependencies without starting any child effects.
    /// Admission proof binds this construction to the parent's real invocation.
    pub async fn delegate(
        &self,
        request: &DelegateRequest,
        prepared: Session,
        invocation: &crate::runtime::tool::ToolContext,
    ) -> Result<Session, Error> {
        use crate::runtime::tool::delegate::DelegateParent;
        let parent = self.inner.clone();
        let request = request.clone();
        let invocation = invocation.clone();
        tokio::spawn(async move {
            if let Err(error) = parent.adopt_child(
                &request.target,
                request.mode.as_deref(),
                request.issuer_epoch,
                &prepared,
                &invocation,
            ) {
                prepared.close().await?;
                return Err(error);
            }
            Ok(prepared)
        })
        .await
        .map_err(|_| {
            Error::lifecycle("delegate construction owner failed before cleanup acknowledgement")
        })?
    }
    pub async fn submit_child(
        &self,
        child_id: &str,
        target: &str,
        input: impl Into<String>,
    ) -> Result<TurnOutcome, Error> {
        use crate::runtime::tool::delegate::DelegateParent;
        self.inner
            .submit_child(child_id, target, input.into())
            .await
    }
    pub async fn resume_child(&self, child_id: &str, target: &str) -> Result<TurnOutcome, Error> {
        use crate::runtime::tool::delegate::DelegateParent;
        self.inner.resume_child(child_id, target).await
    }
    pub fn inspect_child(&self, child_id: &str, target: &str) -> Result<Session, Error> {
        self.inner.owned_child(child_id, target)
    }
    pub async fn close_child(&self, child_id: &str, target: &str) -> Result<(), Error> {
        use crate::runtime::tool::delegate::DelegateParent;
        self.inner.close_child(child_id, target).await
    }
    pub fn one_shot_executor() -> Arc<dyn crate::runtime::subagents::OneShotExecutor> {
        Arc::new(crate::inner::functional::one_shot::SessionOneShot::default())
    }
    fn require_interactive(&self) -> Result<(), Error> {
        self.inner.require_user_input(false)
    }
    pub fn communication_identity(
        &self,
    ) -> Option<crate::state::communication::ConversationIdentity> {
        self.state()
            .communication
            .map(|communication| communication.identity)
    }
    /// Observe the configured reply store without activating the model. The
    /// embedding owner must authenticate and resolve the exact admission first.
    pub async fn observe_external_dialogue_reply(
        &self,
        key: &DialogueAdmissionKey,
    ) -> Result<Option<DurableDialogueReply>, Error> {
        self.inner.observe_external_dialogue_reply(key).await
    }
    /// The caller must authenticate access to this exact parent. Inspection grants
    /// no persistent unlock; every targeted submission checks the child's current mode.
    pub async fn submit_child_user(
        &self,
        child_id: &str,
        input: impl Into<crate::SessionInput>,
    ) -> Result<TurnOutcome, Error> {
        let child = self.inner.owned_child_by_id(child_id)?;
        child.inner.require_user_input(true)?;
        child.inner.submit_user(input.into(), true).await
    }
    /// Authenticated parent-scoped streaming input; the reservation carries this
    /// single submission's authority through asynchronous preparation.
    pub async fn stream_child_user_with_settlement(
        &self,
        child_id: &str,
        input: crate::SessionInput,
        operation_id: Option<String>,
        cancellation: CancellationToken,
    ) -> Result<(SessionStream, SettlementGuard), Error> {
        let child = self.inner.owned_child_by_id(child_id)?;
        child.inner.require_user_input(true)?;
        let mut reservation = tokio::select! { biased;
            result = child.inner.reserve() => result?,
            _ = cancellation.cancelled() => return Err(Error::Cancelled),
        };
        reservation.user_target = Some(true);
        reservation.submission_operation_id = operation_id;
        Ok(child.stream_reserved(Some(input), Arc::new(reservation), cancellation))
    }
    pub fn child_accepts_user_input(&self, child_id: &str) -> Result<bool, Error> {
        Ok(self
            .inner
            .owned_child_by_id(child_id)?
            .inner
            .require_user_input(true)
            .is_ok())
    }
    /// Resolve the selected Agent mode's current role under this Session's input gate.
    pub fn current_agent_role(&self) -> Result<microcrab_agent_types::AgentRole, Error> {
        self.inner.current_agent_role()
    }
    /// Normal root input availability. Child input uses its exact parent's gate.
    pub fn accepts_user_input(&self) -> bool {
        self.inner.require_user_input(false).is_ok()
    }
    pub fn reserve_recovered_child(
        &self,
        session_id: &str,
        agent_id: &crate::state::communication::AgentId,
    ) -> Result<RecoveredChildReservation, Error> {
        self.inner.reserve_recovered_child(session_id, agent_id)
    }
    pub async fn attach_reserved_child(
        &self,
        child: &Session,
        reservation: &RecoveredChildReservation,
    ) -> Result<(), Error> {
        if !Arc::ptr_eq(&self.inner, &reservation.parent)
            || child.process_control().session_id() != reservation.session_id
            || child
                .communication_identity()
                .as_ref()
                .is_none_or(|identity| identity.agent_id != reservation.agent_id)
        {
            return Err(Error::denied(
                "recovered child reservation does not match this parent and child",
            ));
        }
        self.inner.attach_recovered_child(child).await
    }
    /// Explicitly bind a restored persistent child after validating its recorded
    /// parent identities and current native ownership and permission ceilings.
    /// Does not restore the former delegate Job or any exchange approval.
    pub async fn attach_recovered_child(&self, child: &Session) -> Result<(), Error> {
        let identity = child
            .communication_identity()
            .ok_or_else(|| Error::denied("legacy child has no reattachment identity"))?;
        let reservation =
            self.reserve_recovered_child(child.process_control().session_id(), &identity.agent_id)?;
        self.attach_reserved_child(child, &reservation).await
    }
    pub async fn replace_external_speakers(
        &self,
        speakers: Vec<crate::state::communication::ExternalSpeaker>,
    ) -> Result<(), Error> {
        self.inner.replace_external_speakers(speakers).await
    }
    pub async fn enqueue_external(
        &self,
        operation_id: String,
        identity: String,
        body: String,
    ) -> Result<crate::state::communication::inbox::QueuedReceipt, Error> {
        self.inner
            .enqueue_external(operation_id, identity, body)
            .await
    }
    /// Observe an exact historical external admission without enqueueing or
    /// installing the inbox. The embedder authenticates the external identity;
    /// the historical receipt contains no credential-incarnation binding.
    pub async fn replay_legacy_external(
        &self,
        operation_id: String,
        identity: String,
        body: String,
    ) -> Result<crate::state::communication::inbox::QueuedReceipt, Error> {
        self.inner
            .replay_legacy_external(operation_id, identity, body)
            .await
    }
    /// Admit a request after the embedder authenticates the exact credential.
    /// Success means the operation and its dialogue alias are durably committed.
    pub async fn enqueue_authenticated_external(
        &self,
        operation_id: String,
        identity: String,
        body: String,
        incarnation: crate::state::communication::inbox::ExternalCredentialIncarnation,
    ) -> Result<crate::state::communication::inbox::ExternalDialogueReceipt, Error> {
        self.inner
            .enqueue_authenticated_external(operation_id, identity, body, incarnation)
            .await
    }
    /// Observe an authenticated operation without replaying an uncertain enqueue.
    pub async fn authenticated_external_receipt(
        &self,
        operation_id: &str,
        identity: &str,
        incarnation: &crate::state::communication::inbox::ExternalCredentialIncarnation,
    ) -> Result<Option<crate::state::communication::inbox::ExternalDialogueReceipt>, Error> {
        self.inner
            .authenticated_external_receipt(operation_id, identity, incarnation)
            .await
    }
    /// Legacy/unbound receipt metadata. Authenticated rows require exact observation.
    pub async fn delivery_receipt(
        &self,
        operation_id: &str,
    ) -> Result<Option<crate::state::communication::inbox::QueuedReceipt>, Error> {
        self.inner.delivery_receipt(operation_id).await
    }
    /// User edits affect only explicit overrides/review, never native/provider declarations.
    /// Creator supplies the prepared review client; None deliberately follows main.
    pub fn replace_permissions_with_review(
        &self,
        expected_revision: u64,
        config: crate::policy::PermissionConfig,
        review_client: Option<Arc<dyn Client>>,
    ) -> Result<crate::policy::PolicySnapshot, crate::policy::PermissionError> {
        self.inner
            .replace_permissions_with_review(expected_revision, config, review_client)
    }
    pub async fn apply_limits(&self, limits: Limits) -> Result<(), Error> {
        self.inner.apply_limits(limits).await
    }
    /// Current applied policy, independently of desired or persisted Server defaults.
    /// None means no compaction estimator/policy was assembled for this Session.
    pub fn auto_compaction_policy(&self) -> Option<crate::context::AutoCompactionPolicy> {
        self.inner.auto_compaction_policy()
    }
    pub async fn set_auto_compaction(
        &self,
        policy: crate::context::AutoCompactionPolicy,
    ) -> Result<(), Error> {
        self.inner.set_auto_compaction(policy).await
    }
    /// Observe actually applied Runtime permissions without starting model admission.
    /// A successful list is a presentation snapshot, never an admission token.
    pub fn permission_list(
        &self,
        view: crate::policy::PermissionListView,
    ) -> Result<crate::policy::PermissionList, crate::policy::PermissionListError> {
        self.inner.permission_list(view)
    }

    pub fn replace_permissions(
        &self,
        expected_revision: u64,
        config: crate::policy::PermissionConfig,
    ) -> Result<crate::policy::PolicySnapshot, crate::policy::PermissionError> {
        self.inner.replace_permissions(expected_revision, config)
    }
    pub fn subscribe_activity(&self) -> tokio::sync::watch::Receiver<crate::SessionActivity> {
        self.inner.subscribe_activity()
    }
    /// Bounded, loss-detecting committed-turn delivery. Handle broadcast Lagged
    /// explicitly; the latest activity snapshot cannot reconstruct missed replies.
    pub fn subscribe_turns(&self) -> tokio::sync::broadcast::Receiver<crate::CompletedTurn> {
        self.inner.subscribe_turns()
    }
    pub fn subscribe_notices(&self) -> tokio::sync::broadcast::Receiver<crate::StateNotice> {
        self.inner.subscribe_notices()
    }
    pub async fn retire_conversation(&self) -> Result<(), Error> {
        self.inner.retire_conversation().await
    }
    pub async fn retire_if_quiescent(&self) -> Result<bool, Error> {
        self.inner.retire_if_quiescent().await
    }
    pub fn executions(&self) -> Arc<crate::runtime::execution::ExecutionRegistry> {
        self.inner.state_handle().executions()
    }
    pub fn execution_snapshots(&self) -> Vec<crate::runtime::execution::ExecutionSnapshot> {
        self.executions().snapshot()
    }
    pub async fn cancel_execution(
        &self,
        id: crate::runtime::execution::ExecutionId,
    ) -> Result<(), Error> {
        self.executions().cancel(id).await
    }
    pub fn read_execution_output(
        &self,
        id: crate::runtime::execution::ExecutionId,
        cursor: u64,
        max_bytes: usize,
    ) -> Result<crate::runtime::execution::OutputPage, Error> {
        self.executions().read(id, cursor, max_bytes)
    }
    pub fn state_view(&self) -> Result<Value, Error> {
        self.inner.state_handle().view()
    }
    pub async fn replace_external_tools(
        &self,
        tools: Vec<Arc<dyn crate::runtime::tool::Tool>>,
    ) -> Result<(), Error> {
        self.inner.replace_external_tools(tools).await
    }
    pub fn permission_controller(&self) -> Arc<crate::policy::PermissionController> {
        self.inner.permission_controller()
    }
    /// Pure mode admission check for durable-first creator configuration.
    pub fn validate_mode(&self, mode: &str) -> Result<(), Error> {
        self.inner.validate_mode(mode)
    }
    pub async fn select_mode(&self, mode: impl Into<String>) -> Result<(), Error> {
        self.inner.select_mode(mode.into()).await
    }
    pub fn process_control(&self) -> Arc<dyn crate::runtime::execution::process::ProcessControl> {
        self.inner.state_handle().processes()
    }
    pub fn state(&self) -> SessionState {
        self.inner.state()
    }
    /// Applied runtime mode, without projecting Execution rows or invoking controllers.
    pub fn selected_mode(&self) -> Option<String> {
        self.inner.state_handle().selected_mode()
    }
    pub fn state_handle(&self) -> State {
        self.inner.state_handle()
    }
    pub async fn start(&self) -> Result<(), Error> {
        self.inner.start().await
    }
    /// Provenance for the currently retained compaction summary, if any.
    pub fn compaction(&self) -> Option<crate::context::CompactionMarker> {
        self.inner.compaction()
    }

    /// Capture a recovery-compatible Context; pending tool calls can prevent capture.
    /// For live admitted conversation, use `messages` instead.
    pub fn context_snapshot(&self) -> Result<crate::context::ContextSnapshot, Error> {
        self.inner.context_snapshot()
    }
    /// Notify observers at semantic Context mutation boundaries, never model deltas.
    pub fn context_measurement_changes(&self) -> tokio::sync::watch::Receiver<()> {
        self.inner.context_measurement_changes()
    }
    /// Read exact current Context occupancy without requiring a settled checkpoint.
    /// Run a synchronous read-only projection while semantic Context publication
    /// is fenced. The callback must not await or reenter Session methods.
    pub fn with_context_measurement<R>(
        &self,
        project: impl FnOnce(&crate::context::ContextMeasurement) -> R,
    ) -> Result<R, Error> {
        self.inner.with_context_measurement(project)
    }
    pub fn context_measurement(&self) -> Result<crate::context::ContextMeasurement, Error> {
        self.inner.context_measurement()
    }
    /// Read admitted conversation at the latest semantic publication boundary,
    /// including an active turn. Unvalidated stream previews are excluded; this
    /// does not claim a durable recovery checkpoint has been committed.
    pub fn messages(&self) -> Vec<ContentItem> {
        self.inner.messages()
    }
    pub async fn submit(
        &self,
        input: impl Into<crate::SessionInput>,
    ) -> Result<TurnOutcome, Error> {
        self.require_interactive()?;
        self.inner.submit_user(input.into(), false).await
    }
    /// Ordered pending user input and effective lane from the shared insertion owner.
    pub fn inbox_snapshot(&self) -> crate::InboxSnapshot {
        self.inner.inbox_snapshot()
    }
    /// Unclaimed user input in admission order, including paused or retired input.
    pub fn pending_inputs(&self) -> Vec<crate::SessionInput> {
        self.inner.pending_inputs()
    }
    /// Queue a later user turn without competing for foreground admission.
    /// Run a synchronous read while the inbox owner is locked. Publication owners
    /// may use this to install current facts without an intervening async race.
    pub fn with_inbox_snapshot<T>(&self, action: impl FnOnce(&crate::InboxSnapshot) -> T) -> T {
        self.inner.with_inbox_snapshot(action)
    }
    /// Atomically replace/convert an unclaimed pending input at its CAS revision.
    /// Successful no-op writes increment revision; IDs/order and pause are retained.
    pub fn update_pending_input(
        &self,
        expected_inbox_id: &str,
        item_id: u64,
        expected_revision: u64,
        input: Option<crate::SessionInput>,
        mode: Option<crate::PendingInputMode>,
    ) -> Result<crate::PendingInput, Error> {
        self.inner
            .update_pending_input(expected_inbox_id, item_id, expected_revision, input, mode)
    }
    /// Remove only the exact unclaimed entry in this inbox incarnation.
    pub fn remove_pending_input(
        &self,
        expected_inbox_id: &str,
        item_id: u64,
        expected_revision: u64,
    ) -> Result<crate::PendingInput, Error> {
        self.inner
            .remove_pending_input(expected_inbox_id, item_id, expected_revision)
    }
    /// Cancellation/errors pause automatic draining until submit or resume.
    pub fn queue(&self, input: impl Into<crate::SessionInput>) -> Result<(), Error> {
        self.require_interactive()?;
        self.inner.enqueue(input.into(), false, None, None)
    }
    /// Enqueue an explicit send, activating the existing dispatcher only if idle.
    /// Retained continuations reject before admission and require explicit retry;
    /// busy sends remain queued without changing the running worker or pause.
    pub fn queue_and_activate(&self, input: impl Into<crate::SessionInput>) -> Result<(), Error> {
        self.require_interactive()?;
        self.inner.enqueue_and_activate(input.into(), None, None)
    }
    /// Insert at the next settled model boundary. A builtin `job` wait yields
    /// its observation so this boundary can be reached without stopping targets.
    /// Idle steer becomes queued NextTurn input and does not wake a silent Session.
    pub fn steer(&self, input: impl Into<crate::SessionInput>) -> Result<(), Error> {
        self.require_interactive()?;
        self.inner.enqueue(input.into(), true, None, None)
    }
    /// Trusted embedding admission identity; not part of model-facing or frozen input bytes.
    pub fn queue_submission(
        &self,
        input: crate::SessionInput,
        operation_id: String,
        steer: bool,
        activate_if_idle: bool,
    ) -> Result<(), Error> {
        self.queue_submission_with_resolver(input, operation_id, steer, activate_if_idle, None)
    }
    /// Queue a trusted embedding decision with this exact input. It is resolved
    /// on consumption, not enqueue, and is not exposed in pending-input DTOs.
    pub fn queue_submission_with_important(
        &self,
        input: crate::SessionInput,
        operation_id: String,
        steer: bool,
        activate_if_idle: bool,
        resolver: Arc<dyn crate::InputAdmissionResolver>,
    ) -> Result<(), Error> {
        self.queue_submission_with_resolver(
            input,
            operation_id,
            steer,
            activate_if_idle,
            Some(resolver),
        )
    }
    fn queue_submission_with_resolver(
        &self,
        input: crate::SessionInput,
        operation_id: String,
        steer: bool,
        activate_if_idle: bool,
        resolver: Option<Arc<dyn crate::InputAdmissionResolver>>,
    ) -> Result<(), Error> {
        self.require_interactive()?;
        if activate_if_idle {
            self.inner
                .enqueue_and_activate(input, Some(operation_id), resolver)
        } else {
            self.inner
                .enqueue(input, steer, Some(operation_id), resolver)
        }
    }
    pub async fn stream(
        &self,
        input: impl Into<crate::SessionInput>,
    ) -> Result<SessionStream, Error> {
        self.stream_with_cancellation(input, CancellationToken::new())
            .await
    }
    /// Stream a turn with cancellation retained across admission and execution.
    /// The terminal event follows owned worker settlement, including persistence.
    pub async fn stream_with_cancellation(
        &self,
        input: impl Into<crate::SessionInput>,
        cancellation: CancellationToken,
    ) -> Result<SessionStream, Error> {
        self.stream_turn(Some(input.into()), cancellation, None, None)
            .await
            .map(|(stream, _)| stream)
    }
    /// Stream with an explicit embedding settlement fence. Retain the guard
    /// through durable receipt publication, then drop it before checkpointing.
    /// Autonomous inbox turns wait for that same foreground reservation.
    pub async fn stream_with_settlement(
        &self,
        input: impl Into<crate::SessionInput>,
        cancellation: CancellationToken,
    ) -> Result<(SessionStream, SettlementGuard), Error> {
        self.stream_turn(Some(input.into()), cancellation, None, None)
            .await
    }
    /// Retry the current failed model request without appending user input.
    pub async fn retry_stream(&self) -> Result<SessionStream, Error> {
        self.retry_stream_with_cancellation(CancellationToken::new())
            .await
    }
    /// Retry with cancellation retained even before the owned worker starts.
    pub async fn retry_stream_with_cancellation(
        &self,
        cancellation: CancellationToken,
    ) -> Result<SessionStream, Error> {
        self.stream_turn(None, cancellation, None, None)
            .await
            .map(|(stream, _)| stream)
    }
    /// Retry while retaining foreground admission until the embedding owner has
    /// published the terminal receipt. Drop the guard before checkpointing.
    pub async fn retry_stream_with_settlement(
        &self,
        cancellation: CancellationToken,
    ) -> Result<(SessionStream, SettlementGuard), Error> {
        self.stream_turn(None, cancellation, None, None).await
    }
    /// Explicitly resume the exact oldest pending user input. Retained round
    /// continuations require their own retry/resume action; failed CAS never unpauses.
    pub async fn resume_pending_input_with_settlement(
        &self,
        expected_inbox_id: &str,
        item_id: u64,
        expected_revision: u64,
        cancellation: CancellationToken,
    ) -> Result<(SessionStream, SettlementGuard), Error> {
        let reservation = Arc::new(
            self.inner
                .reserve_pending_input(expected_inbox_id, item_id, expected_revision, &cancellation)
                .await?,
        );
        Ok(self.stream_reserved(None, reservation, cancellation))
    }
    /// Stream a trusted submission while retaining its exact admission identity.
    pub async fn stream_submission_with_settlement(
        &self,
        input: crate::SessionInput,
        operation_id: String,
        cancellation: CancellationToken,
    ) -> Result<(SessionStream, SettlementGuard), Error> {
        self.stream_turn(Some(input), cancellation, Some(operation_id), None)
            .await
    }
    /// Stream with a trusted embedding decision for this exact input. A retry
    /// cannot set a new decision: it uses the already-frozen admitted envelope.
    pub async fn stream_submission_with_settlement_and_important(
        &self,
        input: crate::SessionInput,
        operation_id: String,
        cancellation: CancellationToken,
        resolver: Arc<dyn crate::InputAdmissionResolver>,
    ) -> Result<(SessionStream, SettlementGuard), Error> {
        self.stream_turn(
            Some(input),
            cancellation,
            Some(operation_id),
            Some(resolver),
        )
        .await
    }
    async fn stream_turn(
        &self,
        input: Option<crate::SessionInput>,
        cancellation: CancellationToken,
        submission_operation_id: Option<String>,
        admission_resolver: Option<Arc<dyn crate::InputAdmissionResolver>>,
    ) -> Result<(SessionStream, SettlementGuard), Error> {
        self.require_interactive()?;
        // Prefer immediate admission even for an already-cancelled token, so the
        // owned stream still reports its terminal event. Once reservation waits
        // behind an unrelated commit, cancellation must release foreground promptly.
        let mut reservation = tokio::select! {
            biased;
            result = self.inner.reserve_model() => result?,
            _ = cancellation.cancelled() => return Err(Error::Cancelled),
        };
        reservation.submission_operation_id = submission_operation_id;
        reservation.admission_resolver = admission_resolver;
        reservation.user_target = input.as_ref().map(|_| false);
        let reservation = Arc::new(reservation);
        if input.is_none() && !self.inner.retry_ready() {
            return Err(Error::config("no failed model request to retry"));
        }
        Ok(self.stream_reserved(input, reservation, cancellation))
    }
    fn stream_reserved(
        &self,
        input: Option<crate::SessionInput>,
        reservation: Arc<crate::inner::OperationReservation>,
        cancellation: CancellationToken,
    ) -> (SessionStream, SettlementGuard) {
        let cancellation = cancellation.child_token();
        let interrupt_reason = reservation.interrupt_reason.clone();
        let settlement = SettlementGuard {
            _reservation: reservation.clone(),
        };
        let (sender, receiver) = mpsc::channel(32);
        let token = cancellation.clone();
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let (events, mut received) = mpsc::channel(16);
            let forwarding = sender.clone();
            let forwarding_token = token.clone();
            let forward = tokio::spawn(async move {
                while let Some(event) = received.recv().await {
                    tokio::select! {biased;_=forwarding_token.cancelled()=>break,result=forwarding.send(Ok(SessionEvent::Model{event}))=>{if result.is_err(){forwarding_token.cancel();break;}}}
                }
            });
            // Cancellation belongs to the owned worker. Dropping its future here
            // could publish a terminal event before its durable cleanup completes.
            let result = inner
                .submit_reserved(input, Some(events), reservation, token)
                .await;
            let _ = forward.await;
            let message = result.map(|outcome| SessionEvent::Finished { outcome });
            let _ = sender.send(message).await;
        });
        (
            SessionStream {
                receiver,
                cancellation,
                interrupt_reason,
            },
            settlement,
        )
    }
    /// Whether an uncertain durable/lifecycle publication quarantined this owner.
    pub fn is_faulted(&self) -> bool {
        self.inner.is_faulted()
    }
    pub fn interrupt(&self) {
        self.inner.interrupt()
    }
    pub fn interrupt_with_reason(&self, reason: InterruptReason) {
        self.inner.interrupt_with_reason(reason)
    }
    pub async fn compact(&self, summary: impl Into<String>, variables: Value) -> Result<(), Error> {
        self.inner.compact(summary.into(), variables).await
    }
    /// Generate a text-only summary from an immutable inherited request, then
    /// install it through the existing deferred-epoch compaction operation.
    pub async fn compact_with_model(
        &self,
        user_input: impl Into<String>,
        variables: Value,
    ) -> Result<microcrab_protocol::Response, Error> {
        self.inner
            .compact_with_model(user_input.into(), variables, CancellationToken::new())
            .await
    }
    /// Generate a summary with caller-owned cooperative cancellation. Token
    /// cancellation rejects generation before installation and does not interrupt
    /// an admitted Journal publication. The embedding caller must keep polling
    /// this future through settlement; dropping it retains the existing guarded
    /// publication fault-containment behavior, not detached completion.
    pub async fn compact_with_model_cancellation(
        &self,
        user_input: impl Into<String>,
        variables: Value,
        cancellation: CancellationToken,
    ) -> Result<microcrab_protocol::Response, Error> {
        self.inner
            .compact_with_model(user_input.into(), variables, cancellation)
            .await
    }
    /// Generate a validated title without changing conversation or epoch state.
    pub async fn rename_with_model(
        &self,
        user_input: impl Into<String>,
    ) -> Result<microcrab_protocol::Response, Error> {
        self.inner.rename_with_model(user_input.into()).await
    }
    pub async fn close(&self) -> Result<(), Error> {
        self.inner.close().await
    }
    /// Explicitly reconcile retained execution/process cleanup after a failed close.
    /// Admission stays sealed; terminal hooks and durable writes are never replayed.
    /// Success requires every retained owner to acknowledge cleanup. Non-cleanup
    /// termination failures remain failures even after containment is established.
    pub async fn reconcile_close(&self) -> Result<(), Error> {
        self.inner.reconcile_close().await
    }
    /// Settle a runtime for an owner-authorized irreversible deletion.
    /// The caller must retain its ownership/revision and filesystem authority fences.
    /// Only failure to persist the final snapshot is tolerated: execution/process
    /// cleanup, terminal hooks, audit flush and Journal release must all complete.
    /// `reconcile` explicitly retries retained cleanup without replaying terminal writes.
    /// This does not authorize deleting storage or clearing any admission fence.
    pub async fn settle_for_delete(&self, reconcile: bool) -> Result<(), Error> {
        self.inner.settle_for_delete(reconcile).await
    }
    /// Persist settled recovery state, or return the unchanged selected historical
    /// value without a new commit when only its retained live task overlay differs.
    /// The returned value does not imply that a new history node was created.
    pub async fn checkpoint(&self) -> Result<Recover, Error> {
        self.inner.checkpoint().await
    }
    /// Persist a conversation-navigation checkpoint only between settled Turns.
    /// Generic recovery snapshots remain available independently through `checkpoint`.
    pub async fn checkpoint_navigation(&self) -> Result<Recover, Error> {
        self.inner.navigation_checkpoint().await
    }
    /// Explicitly acknowledge the current embedding catalog without admitting input.
    /// Busy and unfinished mismatched Turns are not interrupted or discarded.
    pub async fn reconcile_model_catalog(&self) -> Result<(), Error> {
        self.inner.reconcile_model_catalog().await
    }

    pub async fn recover_with_external_tools(
        &self,
        recover: crate::runtime::journal::Recover,
        tools: Vec<Arc<dyn crate::runtime::tool::Tool>>,
    ) -> Result<(), Error> {
        self.inner.recover_with_external_tools(recover, tools).await
    }
    /// Install a recovery value into this live Session. The configured Journal
    /// publishes the selection before State/Context installation; creator journals
    /// require a matching prepared travel target. Files are never restored.
    pub async fn recover(&self, recover: Recover) -> Result<(), Error> {
        self.inner.recover(recover).await
    }
    /// Reload configured reference catalogs under Session command admission. New
    /// catalogs appear through the next normal State snapshot/patch; frozen model
    /// messages and SYSTEM are never rewritten. No ordinary snapshot scans sources.
    pub async fn refresh_references(&self) -> Result<(), Error> {
        self.inner.refresh_references().await
    }

    /// Whether this live Session has completed its one initial Agent setup.
    /// Historical State observations do not revoke completion. True without WASM agents.
    pub fn agent_initialization_complete(&self) -> bool {
        self.inner.agent_initialization_complete()
    }

    /// Retry a failed or unresolved Agent initialization through Session admission.
    /// Earlier external effects remain recorded and are not rolled back.
    pub async fn retry_agent_init(&self) -> Result<(), Error> {
        self.inner.retry_agent_init().await
    }

    /// A prior retained attempt permits an explicit retry; successful replacement clears it.
    pub fn auto_compaction_retry_available(&self) -> bool {
        self.inner.auto_compaction_retry_available()
    }

    /// Explicitly allow one additional compaction attempt at a settled boundary.
    /// Returns true only when a smaller durable baseline was installed.
    pub async fn retry_auto_compaction(&self) -> Result<bool, Error> {
        self.inner.retry_auto_compaction().await
    }

    /// Whether the current Context contains a safely retryable failed model request.
    pub fn retry_ready(&self) -> bool {
        self.inner.retry_ready()
    }
    pub fn has_continuation(&self) -> bool {
        self.inner.has_continuation()
    }
    pub async fn resume(&self) -> Result<TurnOutcome, Error> {
        self.require_interactive()?;
        self.inner.resume(None).await
    }
    /// Construct an independent Session with this settled, materialized Context.
    /// Rendered SYSTEM, frozen tools, messages and historical schema witnesses are
    /// copied exactly; source history selection and live execution are not copied.
    /// The caller supplies fresh destination identity, Journal and runtime wiring.
    /// If destination references are omitted, live catalog kinds are Arc-shared;
    /// supply fresh kinds for independently bound Host/source-writer callbacks.
    /// As with restore, call start before use; creators must commit a destination
    /// checkpoint to establish its independent history root before publication.
    pub async fn fork(&self, mut builder: SessionBuilder) -> Result<Self, Error> {
        if builder.session_id == self.process_control().session_id()
            || builder.processes_parent.is_some()
            || builder.permission_parent.is_some()
            || self.inner.shares_journal(&builder.journal)
        {
            return Err(Error::config(
                "fork requires fresh identity, Journal and independent execution wiring",
            ));
        }
        let mut recover = self.inner.fork_recover().await?;
        // A fork is a new Session invocation owner, not a replay of source init.
        recover.state.agent_init.clear();
        recover.state.parent_relationship = None;
        recover.state.communication = Some(crate::state::communication::CommunicationState::fresh(
            0,
            crate::state::communication::SessionInteraction::Persistent,
        ));
        recover.state.session_kind = Some(builder.kind.unwrap_or(SessionKind::Interactive));
        #[cfg(feature = "wasm")]
        {
            recover.state.agent_initialization_pending =
                !builder.runtime.wasm_lifecycles().is_empty();
        }
        #[cfg(not(feature = "wasm"))]
        {
            recover.state.agent_initialization_pending = false;
        }
        if builder.references.is_empty() {
            builder.references = recover.state.references.iter().cloned().collect();
            if builder.default_writable_reference.is_none() {
                builder.default_writable_reference =
                    self.inner.state_handle().default_writable_kind();
            }
        }
        let session = Self::restore(builder, recover).await?;
        #[cfg(feature = "wasm")]
        session
            .inner
            .state_handle()
            .wasm_instances()
            .require_fresh_initialization();
        Ok(session)
    }

    /// Construct a Session from a recovery value selected by the caller.
    /// This validates and installs runtime state without publishing a Journal head
    /// move. Initialization loads live references and emits SessionRestored when
    /// the Session starts; legacy workspace bytes remain inert.
    pub async fn restore(mut builder: SessionBuilder, recover: Recover) -> Result<Self, Error> {
        if let Some(owner) = &builder.model_admission {
            crate::admission::model::reject_retired(owner.as_ref(), &recover)?;
        }
        // Frozen Context is installed below; creation must never render a mutable
        // replacement SYSTEM or require Agent values no longer present in it.
        builder.prompt_config = recover.prompt_config.clone().unwrap_or_default();
        builder.materialized_system = Some(recover.context.system.clone());
        builder.communication = Some(recover.state.communication.clone());
        if let Some(kind) = recover.state.session_kind {
            if builder.kind.is_some_and(|configured| configured != kind) {
                return Err(Error::config(
                    "recovery lifecycle kind differs from explicit configuration",
                ));
            }
            builder.kind = Some(kind);
        }
        tokio::spawn(async move {
            let session = builder.build()?;
            session.inner.install_initial(recover).await?;
            Ok(session)
        })
        .await
        .map_err(|_| Error::protocol("restore worker failed"))?
    }
}
impl SessionBuilder {
    /// Install the trusted admission owner for this persistent Interactive root.
    pub fn model_admission(mut self, owner: Arc<dyn crate::ModelAdmission>) -> Self {
        self.model_admission = Some(owner);
        self
    }

    pub fn interaction(
        mut self,
        interaction: crate::state::communication::SessionInteraction,
    ) -> Self {
        self.interaction = interaction;
        self
    }
    pub fn kind(mut self, kind: SessionKind) -> Self {
        self.kind = Some(kind);
        self
    }
    /// Explicit opt-in policy and route-aware estimator supplied by the embedding.
    pub fn auto_compaction(
        mut self,
        policy: crate::context::AutoCompactionPolicy,
        estimator: Arc<dyn crate::context::TokenEstimator>,
    ) -> Self {
        self.auto_compaction = Some((policy, estimator));
        self
    }

    /// Confirm another max_rounds requests without changing other limits.
    pub fn round_limit_callback(
        mut self,
        callback: Arc<dyn crate::budget::RoundLimitCallback>,
    ) -> Self {
        self.round_limit_callback = Some(callback);
        self
    }
    /// Resolve opaque attachment identities through the embedding owner.
    pub fn attachment_resolver(
        mut self,
        resolver: Arc<dyn crate::admission::image::AttachmentResolver>,
    ) -> Self {
        self.attachment_resolver = Some(resolver);
        self
    }
    /// Initial user overrides/review. Native/provider fields are ignored at assembly.
    pub fn review_client(mut self, client: Arc<dyn Client>) -> Self {
        self.review_client = Some(client);
        self
    }
    pub fn permissions(mut self, config: crate::policy::PermissionConfig) -> Self {
        self.permissions = Some(config);
        self
    }
    /// Native child assembly retains a live, immutable parent authorization ceiling.
    pub fn permission_parent(mut self, parent: Arc<crate::policy::PermissionController>) -> Self {
        self.permission_parent = Some(parent);
        self
    }
    pub fn auditor<B: Auditor + 'static>(mut self, auditor: B) -> Self {
        self.auditor = Arc::new(auditor);
        self
    }
    /// Install the reply adapter for this exact Interactive Session. An adapter
    /// must not be shared across independent Session/Agent identities.
    pub fn dialogue_reply_store(self, store: Arc<dyn DialogueReplyStore>) -> Self {
        self.dialogue_reply_store_with(|_| Ok(store))
    }
    /// Bind a scoped adapter after Core has created or restored the canonical
    /// communication AgentId. Legacy/other Session kinds do not get a reply owner.
    /// The factory is synchronous: adapters needing I/O may open lazily.
    pub fn dialogue_reply_store_with(
        mut self,
        factory: impl FnOnce(&AgentId) -> Result<Arc<dyn DialogueReplyStore>, Error> + Send + 'static,
    ) -> Self {
        self.dialogue_reply_store = Some(Box::new(factory));
        self
    }
    pub fn journal<J: Journal>(self, journal: J) -> Self {
        self.journal_arc(Arc::new(journal))
    }
    pub fn journal_arc(mut self, journal: Arc<dyn Journal>) -> Self {
        self.journal = journal;
        self
    }
    pub fn session_id(mut self, id: impl Into<String>) -> Self {
        self.session_id = id.into();
        self
    }
    pub fn processes_parent(
        mut self,
        parent: Arc<dyn crate::runtime::execution::process::ProcessControl>,
    ) -> Self {
        self.processes_parent = Some(parent);
        self
    }
    pub fn host(mut self, host: Arc<dyn microcrab_host_api::Host>) -> Self {
        self.host = Some(host);
        self
    }
    pub(crate) fn materialized_system(mut self, system: String) -> Self {
        self.materialized_system = Some(system);
        self
    }
    pub fn prompt_config(mut self, config: PromptConfig) -> Self {
        self.prompt_config = config;
        self
    }
    pub fn client_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.prompt_config.client_prompt = prompt.into();
        self
    }
    pub fn system(mut self, system: impl Into<String>) -> Self {
        self.prompt_config.system_template = system.into();
        self
    }
    /// Bind a creator's provider routing without adding creator metadata to core State.
    pub fn turn_routing(mut self, route: Arc<dyn TurnRouting>) -> Self {
        self.route = Some(route);
        self
    }
    pub fn limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }
    pub fn agent(mut self, agent: microcrab_agent_types::AgentInfo) -> Self {
        self.agent = Some(agent);
        self
    }
    /// Install reference kinds loaded during session initialization.
    pub fn references(mut self, references: Vec<DynReference>) -> Self {
        self.references = references;
        self
    }
    /// Designate the default writable reference `kind` (e.g. `references`) for writes without an explicit target kind.
    pub fn default_writable_reference(mut self, kind: impl Into<String>) -> Self {
        self.default_writable_reference = Some(kind.into());
        self
    }
    pub fn catalog(mut self, catalog: Vec<microcrab_agent_types::AgentInfo>) -> Self {
        self.catalog = catalog;
        self
    }
    pub fn is_main(mut self, is_main: bool) -> Self {
        self.is_main = Some(is_main);
        self
    }
    pub fn build(self) -> Result<Session, Error> {
        let is_main =
            self.is_main
                .unwrap_or(self.kind.map_or(self.processes_parent.is_none(), |kind| {
                    kind == SessionKind::Interactive
                }));
        if self.kind == Some(SessionKind::OneShot)
            && (!self.runtime.definitions().is_empty() || self.agent.is_some())
        {
            return Err(Error::config(
                "one-shot functional Sessions cannot install Agent/tool dispatch",
            ));
        }
        let kind = self.kind.unwrap_or(if is_main {
            SessionKind::Interactive
        } else {
            SessionKind::Delegated
        });
        if (kind == SessionKind::Interactive && !is_main)
            || (kind == SessionKind::Delegated && is_main)
        {
            return Err(Error::config(
                "Session kind and Agent lineage role disagree",
            ));
        }
        let communication = self.communication.unwrap_or_else(|| {
            (kind != SessionKind::OneShot).then(|| {
                crate::state::communication::CommunicationState::fresh(
                    if is_main { 0 } else { 1 },
                    self.interaction,
                )
            })
        });
        if let Some(communication) = &communication {
            communication.validate()?;
        }
        if self.model_admission.is_some()
            && (kind != SessionKind::Interactive
                || self.processes_parent.is_some()
                || !communication.as_ref().is_some_and(|owner| {
                    owner.identity.depth == 0
                        && owner.identity.interaction
                            == crate::state::communication::SessionInteraction::Persistent
                }))
        {
            return Err(Error::config(
                "model admission owner requires a persistent Interactive root",
            ));
        }
        let dialogue_reply_store = self
            .dialogue_reply_store
            .zip(communication.as_ref().filter(|owner| {
                kind == SessionKind::Interactive
                    && owner.identity.depth == 0
                    && owner.identity.interaction
                        == crate::state::communication::SessionInteraction::Persistent
            }))
            .map(|(factory, owner)| factory(&owner.identity.agent_id))
            .transpose()?;
        Ok(Session {
            inner: SessionInner::new(SessionParts {
                communication,
                dialogue_reply_store,
                kind: self.kind.unwrap_or(if is_main {
                    SessionKind::Interactive
                } else {
                    SessionKind::Delegated
                }),
                client: self.client,
                route: self.route,
                model_admission: self.model_admission,
                auto_compaction: self.auto_compaction,
                round_limit_callback: self.round_limit_callback,
                attachment_resolver: self.attachment_resolver,
                runtime: self.runtime,
                prompt_config: self.prompt_config.resolved(),
                materialized_system: self.materialized_system,
                limits: self.limits,
                agent: self.agent,
                auditor: self.auditor,
                journal: self.journal,
                host: self.host,
                references: self.references,
                default_writable_reference: self.default_writable_reference,
                session_id: self.session_id,
                processes_parent: self.processes_parent,
                catalog: self.catalog,
                is_main,
                permissions: self.permissions,
                review_client: self.review_client,
                permission_parent: self.permission_parent,
            })?,
        })
    }
}
impl SessionBuilder {
    pub fn audit_sender(mut self, sender: mpsc::UnboundedSender<AuditRecord>) -> Self {
        self.auditor = Arc::new(ChannelAuditor::from_sender(sender));
        self
    }
}
