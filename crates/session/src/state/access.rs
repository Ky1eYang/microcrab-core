//! Shared snapshots, queued custom writes, and atomic Session hook commits.
mod bytes;
mod secrets;
pub(crate) use bytes::RedirectDestination;
pub use bytes::{
    BytesVariableAvailability, BytesVariableKind, BytesVariableLifecycle, BytesVariableMetadata,
    BytesWriteReceipt, PreparedBytesWrite,
};
pub use secrets::PreparedSecretWrite;
#[cfg(test)]
mod tests;
use super::endpoint::VariableEndpoint;
use super::reference::{self, ListedReference, ReferenceContent, References};
use super::{Limits, SessionState, Variables};
use crate::admission::variables::is_reserved_key;
pub(crate) use crate::admission::variables::{validate_change, validate_custom_key};
use crate::boundary::Boundary;
use crate::{Error, VariablesChange};
use microcrab_protocol::{VariableMetadata, VariableRole};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VariableWitness {
    pub runtime_instance_id: String,
    pub variables_revision: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VariableDescriptor {
    pub kind: String,
    pub editable: bool,
    pub owner_editable: bool,
    pub deletable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VariableInspection {
    pub values: Value,
    pub metadata: Value,
    pub descriptors: BTreeMap<String, VariableDescriptor>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub bytes: BTreeMap<String, BytesVariableMetadata>,
    pub witness: VariableWitness,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VariableCommit {
    pub before: VariableWitness,
    pub after: VariableWitness,
    pub changed: bool,
}

#[derive(Clone)]
pub struct State {
    inner: Arc<Mutex<LiveState>>,
    writer: Writer,
    actor: VariableRole,
    // Independent of LiveState so registry callbacks never reenter its lock.
    notice_inbox: Arc<Mutex<Weak<crate::admission::inbox::Inbox>>>,
    messaging: Arc<Mutex<Option<Weak<dyn crate::runtime::messaging::AgentPeer>>>>,
}
#[derive(Clone)]
enum Writer {
    Detached,
    Session(Weak<VariableEndpoint>),
}
#[derive(Clone)]
struct LiveState {
    snapshot: SessionState,
    bytes: bytes::ByteVariables,
    image_host: Option<Arc<dyn microcrab_host_api::Host>>,
    #[cfg(feature = "wasm")]
    file_versions: Option<Arc<crate::runtime::wasm::FileVersionOwner>>,
    short_model_refs: bool,
    /// Live-owner commit counter, separate from frozen Recover serialization.
    variables_revision: u64,
    runtime_instance_id: String,
    processes: Arc<dyn crate::runtime::execution::process::ProcessControl>,
    executions: Arc<crate::runtime::execution::ExecutionRegistry>,
    permission: Option<crate::policy::PermissionObservation>,
    permission_model_names: Option<Arc<std::collections::BTreeSet<String>>>,
    builtins: Map<String, Value>,
    references_installed: bool,
    references_revision: u64,
    references_loaded: bool,
    catalog: Vec<microcrab_agent_types::AgentInfo>,
    agent: Option<microcrab_agent_types::AgentInfo>,
    is_main: bool,
    delegate_available: bool,
    switch_mode_available: bool,
    active_correspondents: std::collections::BTreeSet<crate::state::communication::AgentId>,
    cache_points: Arc<crate::runtime::cache::Collector>,
    /// Live epoch resources: shared by handles of this Session, never serialized.
    #[cfg(feature = "wasm")]
    wasm: Arc<crate::runtime::wasm::EpochInstances>,
    /// Last projection returned by each reference kind. Not a custom variable.
    reference_projection: Vec<(String, super::reference::ReferenceProjection)>,
}
fn inspection_metadata(live: &LiveState) -> Result<Value, Error> {
    let mut metadata = live.snapshot.variables.metadata();
    // Historical custom metadata must not describe the live framework object.
    metadata.remove(crate::admission::variables::EXECUTION_CAPACITY_KEY);
    metadata.remove("permission");
    for name in ["agent_init", "agent_initialization_pending"] {
        metadata.insert(
            name.into(),
            VariableMetadata {
                visibility: microcrab_protocol::VariableVisibility::Invisible,
                persistence: microcrab_protocol::VariablePersistence::Persisted,
                owner: VariableRole::Runtime,
                editors: vec![VariableRole::Runtime],
            },
        );
    }
    serde_json::to_value(metadata).map_err(Error::from)
}
impl LiveState {
    fn witness(&self) -> VariableWitness {
        VariableWitness {
            runtime_instance_id: self.runtime_instance_id.clone(),
            variables_revision: self.variables_revision.to_string(),
        }
    }
    fn with_all_variables(mut self) -> Self {
        // This detached capture is only an explicit owner projection.
        self.snapshot.variables.expose_for_inspection();
        self.builtins
            .insert("agent_init".into(), json!(self.snapshot.agent_init));
        self.builtins.insert(
            "agent_initialization_pending".into(),
            json!(self.snapshot.agent_initialization_pending),
        );
        self
    }
}
impl State {
    pub(crate) fn for_agent(&self) -> Self {
        let mut state = self.clone();
        state.actor = VariableRole::Agent;
        state
    }
    pub(crate) fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
    pub fn new(limits: Limits) -> Self {
        Self::from_snapshot(SessionState::new(limits))
    }
    pub(crate) fn from_snapshot(snapshot: SessionState) -> Self {
        Self {
            inner: Arc::new(Mutex::new(LiveState {
                processes: crate::runtime::execution::process::ProcessRegistry::detached(
                    snapshot.limits.clone(),
                ),
                executions: crate::runtime::execution::ExecutionRegistry::new(
                    snapshot.limits.clone(),
                ),
                permission: None,
                permission_model_names: None,
                snapshot,
                bytes: Default::default(),
                image_host: None,
                #[cfg(feature = "wasm")]
                file_versions: None,
                short_model_refs: false,
                variables_revision: 0,
                runtime_instance_id: uuid::Uuid::new_v4().to_string(),
                builtins: Map::new(),
                references_installed: false,
                references_revision: 0,
                references_loaded: false,
                catalog: Vec::new(),
                agent: None,
                is_main: true,
                delegate_available: false,
                switch_mode_available: false,
                active_correspondents: Default::default(),
                cache_points: Arc::new(crate::runtime::cache::Collector::default()),
                #[cfg(feature = "wasm")]
                wasm: Arc::new(crate::runtime::wasm::EpochInstances::default()),
                reference_projection: Default::default(),
            })),
            writer: Writer::Detached,
            actor: VariableRole::Client,
            messaging: Default::default(),
            notice_inbox: Arc::new(Mutex::new(Weak::new())),
        }
    }
    /// Admission may reject this state. Reuse the last committed projection;
    /// invoking a kind here would call user code before load or consume updates.
    pub(crate) fn preview(&self) -> Self {
        let live = self.inner.lock().unwrap_or_else(|e| e.into_inner()).clone();
        Self {
            inner: Arc::new(Mutex::new(live)),
            writer: Writer::Detached,
            actor: self.actor,
            messaging: Default::default(),
            notice_inbox: Arc::new(Mutex::new(Weak::new())),
        }
    }

    #[cfg(feature = "wasm")]
    pub(crate) fn install_file_versions(&self, owner: Arc<crate::runtime::wasm::FileVersionOwner>) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .file_versions = Some(owner);
    }

    #[cfg(feature = "wasm")]
    pub(crate) fn file_versions(&self) -> Option<Arc<crate::runtime::wasm::FileVersionOwner>> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .file_versions
            .clone()
    }

    pub(crate) fn set_short_model_refs(&self, enabled: bool) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .short_model_refs = enabled;
    }

    pub(crate) fn short_model_refs(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .short_model_refs
    }

    pub(crate) fn install_image_host(&self, host: Option<Arc<dyn microcrab_host_api::Host>>) {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .image_host = host;
    }

    pub(crate) fn install_inbox(&self, inbox: Weak<crate::admission::inbox::Inbox>) {
        *self.notice_inbox.lock().unwrap_or_else(|e| e.into_inner()) = inbox;
    }
    /// Framework-only port. No execution producer is classified here: registry
    /// visibility is not evidence that the invocation result was delivered.
    /// This port only locks the independent weak-inbox binding and the inbox;
    /// registry callbacks may use it without reentering LiveState or a registry.
    pub(crate) fn post_notice(
        &self,
        notice: crate::admission::inbox::StateNotice,
    ) -> Result<(), Error> {
        let inbox = self
            .notice_inbox
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .upgrade()
            .ok_or(Error::Closed)?;
        {
            let mut pending = inbox.state.lock().unwrap_or_else(|e| e.into_inner());
            pending.deliver_notice(notice, &inbox.notices_changed)?;
        }
        inbox.changed.notify_one();
        Ok(())
    }

    pub(crate) fn cache_points(&self) -> Arc<crate::runtime::cache::Collector> {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .cache_points
            .clone()
    }

    #[cfg(feature = "wasm")]
    pub(crate) fn wasm_instances(&self) -> Arc<crate::runtime::wasm::EpochInstances> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .wasm
            .clone()
    }

    pub(crate) fn end_wasm_epoch(&self) {
        #[cfg(feature = "wasm")]
        self.wasm_instances().end();
    }

    pub(crate) fn close_wasm(&self) {
        #[cfg(feature = "wasm")]
        self.wasm_instances().close();
    }

    /// Share this store with a nongeneric writer without retaining the Session owner.
    pub(crate) fn bind_writer(
        &self,
        closed: Arc<AtomicBool>,
        faulted: Arc<AtomicBool>,
    ) -> (Self, Arc<VariableEndpoint>) {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .snapshot
            .variables
            .activate();
        let detached = Self {
            inner: self.inner.clone(),
            writer: Writer::Detached,
            actor: self.actor,
            messaging: self.messaging.clone(),
            notice_inbox: self.notice_inbox.clone(),
        };
        let endpoint = VariableEndpoint::new(detached, closed, faulted);
        let attached = Self {
            inner: self.inner.clone(),
            writer: Writer::Session(Arc::downgrade(&endpoint)),
            actor: self.actor,
            messaging: self.messaging.clone(),
            notice_inbox: self.notice_inbox.clone(),
        };
        (attached, endpoint)
    }
    /// Install kinds before the session starts. Recovery never reloads them from a snapshot.
    pub(crate) fn set_correspondent_active(
        &self,
        agent_id: crate::state::communication::AgentId,
        active: bool,
    ) {
        let mut live = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if active {
            live.active_correspondents.insert(agent_id);
        } else {
            live.active_correspondents.remove(&agent_id);
        }
    }
    pub(crate) fn clear_active_correspondents(&self) {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .active_correspondents
            .clear();
    }
    pub(crate) fn install_messaging_peer(
        &self,
        peer: Weak<dyn crate::runtime::messaging::AgentPeer>,
    ) {
        *self
            .messaging
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(peer);
    }
    pub(crate) fn messaging_peer(
        &self,
    ) -> Result<Arc<dyn crate::runtime::messaging::AgentPeer>, Error> {
        self.messaging
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(|| Error::denied("Session communication endpoint is unavailable"))
    }
    pub(crate) fn install_references(&self, references: References) -> Result<(), Error> {
        let mut state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if state.references_installed {
            return Err(Error::config("references are already installed"));
        }
        let revision = state.references_revision.checked_add(1).ok_or_else(|| {
            crate::budget::reject(
                crate::LimitKind::Arithmetic,
                "reference generation overflow",
            )
        })?;
        state.snapshot.references = references;
        state.references_revision = revision;
        state.references_installed = true;
        Ok(())
    }

    /// Initial delegation/recovery installs only the immutable selected material.
    pub(crate) fn install_focused_references(
        &self,
        snapshots: Vec<super::reference::focus::FocusSnapshot>,
    ) -> Result<(), Error> {
        super::reference::focus::validate_snapshots(&snapshots)?;
        let references = References::new(vec![super::reference::DynReference::new(
            super::reference::focus::FocusedReferences(snapshots),
        )?])?;
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        references.validate_variables(&state.snapshot.variables)?;
        state.snapshot.references = references;
        state.reference_projection.clear();
        state.references_loaded = false;
        state.references_revision = state.references_revision.checked_add(1).ok_or_else(|| {
            crate::budget::reject(
                crate::LimitKind::Arithmetic,
                "reference generation overflow",
            )
        })?;
        state.catalog.clear();
        Ok(())
    }
    /// Load every installed kind. The state lock is not held across the await.
    pub(crate) async fn load_references(&self) -> Result<(), Error> {
        let references = self.snapshot().references.clone();
        for reference in references.iter() {
            reference.load().await?;
        }
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .references_loaded = true;
        Ok(())
    }

    pub fn list_references(&self) -> Result<Vec<ListedReference>, Error> {
        reference::list_all(&self.snapshot().references)
    }

    pub fn read_reference(&self, id: &str) -> Result<ReferenceContent, Error> {
        reference::read_id(&self.snapshot().references, id)
    }

    /// Replace counters and variables while the caller holds the variable fence.
    /// Keep the init reference list and drop the derived projection.
    pub(crate) fn replace(&self, mut snapshot: SessionState) {
        // WASM memory is live execution state, not a historical checkpoint.
        // Even a same-number recovery starts a new instance generation.
        self.end_wasm_epoch();
        self.cache_points().clear();
        let mut state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        snapshot.references = state.snapshot.references.clone();
        // Frozen Recover values retain their encoding for hash validation; newly
        // active state and future checkpoints use the current limit vocabulary.
        snapshot.clear_historical_limits();
        // Recover can retain a historical certificate, but selecting history or
        // restarting alone does not certify a currently owned dialogue Turn.
        snapshot.dialogue_turn_live = false;
        snapshot.variables.clear_ephemeral();
        snapshot.variables.activate();
        state.runtime_instance_id = uuid::Uuid::new_v4().to_string();
        if let Some(revision) = state.variables_revision.checked_add(1) {
            state.variables_revision = revision;
        } else {
            // Exhausting a counter starts a new incarnation, never an ABA witness.
            state.runtime_instance_id = uuid::Uuid::new_v4().to_string();
            state.variables_revision = 0;
        }
        state.snapshot = snapshot;
        state.bytes.clear();
        state.builtins.clear();
        state.reference_projection.clear();
        state.references_loaded = false;
    }
    /// Only numeric facts are read here: do not clone payloads or invoke
    /// execution-source capability callbacks under the observation fence.
    pub(crate) fn observation_position(&self) -> crate::observation::Position {
        let live = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        crate::observation::Position {
            epoch: live.snapshot.epoch_id,
            turn: live.snapshot.turn_id,
            round: live.snapshot.round_id,
        }
    }
    pub(crate) fn completion_position(&self) -> crate::observation::Position {
        let live = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        crate::observation::Position {
            epoch: live.snapshot.epoch_id,
            turn: Some(live.snapshot.last_turn_id),
            round: None,
        }
    }
    pub(crate) fn selected_mode(&self) -> Option<String> {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .snapshot
            .selected_mode
            .clone()
    }
    /// Exact live identity and selected mode, without projecting other State facts.
    pub(crate) fn permission_position(&self) -> (String, Option<String>) {
        let live = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        (
            live.runtime_instance_id.clone(),
            live.snapshot.selected_mode.clone(),
        )
    }

    /// Issuer mode and its literal instructions, observed under one State lock.
    pub(crate) fn selected_mode_instructions(&self) -> (String, String) {
        let live = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let Some(mode) = &live.snapshot.selected_mode else {
            return (String::new(), String::new());
        };
        let prompt = live
            .agent
            .as_ref()
            .and_then(|agent| agent.modes.iter().find(|metadata| metadata.name == *mode));
        (
            mode.clone(),
            prompt.map_or_else(String::new, |metadata| metadata.prompt.clone()),
        )
    }
    pub fn snapshot(&self) -> SessionState {
        let (mut snapshot, executions) = {
            let live = self.inner.lock().unwrap_or_else(|error| error.into_inner());
            (live.snapshot.clone(), live.executions.clone())
        };
        snapshot.executions = executions.model_projection();
        snapshot.dialogue_turn = None;
        snapshot.dialogue_turn_live = false;
        snapshot
    }
    /// The live Turn claim is deliberately independent of serializable Recover.
    pub(crate) fn live_dialogue_turn(
        &self,
    ) -> Option<super::communication::turn::DialogueTurnWitness> {
        let live = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let witness = live.snapshot.dialogue_turn.as_ref()?;
        (live.snapshot.dialogue_turn_live
            && live.snapshot.epoch_id == witness.exact_parts().1
            && live.snapshot.turn_id == Some(witness.exact_parts().2.number()))
        .then(|| witness.clone())
    }

    /// Checkpoint-only payload projection. Public/model State never exposes the
    /// owner-specific restoration payloads retained by this separate port.
    pub(crate) fn recovery_snapshot(&self) -> SessionState {
        let (mut snapshot, executions) = {
            let live = self.inner.lock().unwrap_or_else(|error| error.into_inner());
            (live.snapshot.clone(), live.executions.clone())
        };
        snapshot.variables.clear_ephemeral();
        snapshot.dialogue_turn_live = false;
        snapshot.executions = executions.recovery_snapshot();
        snapshot
    }
    pub(crate) fn executions(&self) -> Arc<crate::runtime::execution::ExecutionRegistry> {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .executions
            .clone()
    }
    pub(crate) fn is_attached(&self) -> bool {
        matches!(self.writer, Writer::Session(_))
    }
    pub(crate) fn install_permission(&self, permission: crate::policy::PermissionObservation) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .permission = Some(permission);
    }
    pub(crate) fn set_model_permission_names(
        &self,
        definitions: &[microcrab_protocol::ToolDefinition],
    ) {
        let names = definitions.iter().map(|tool| tool.name.clone()).collect();
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .permission_model_names = Some(Arc::new(names));
    }

    pub(crate) fn install_processes(
        &self,
        processes: Arc<dyn crate::runtime::execution::process::ProcessControl>,
    ) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .processes = processes;
    }
    /// Immutable selected Agent metadata; mode names and instructions are derived
    /// from the same selected_mode observation, including after restore.
    pub(crate) fn install_agent_metadata(&self, agent: Option<microcrab_agent_types::AgentInfo>) {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .agent = agent;
    }
    pub(crate) fn install_catalog(
        &self,
        catalog: Vec<microcrab_agent_types::AgentInfo>,
        is_main: bool,
    ) {
        let mut state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        state.catalog = catalog;
        state.is_main = is_main;
    }
    pub(crate) fn resolve_delegate(
        &self,
        target: &str,
        mode: Option<&str>,
    ) -> Result<(String, String), Error> {
        let live = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if !live.is_main || !live.delegate_available {
            return Err(Error::tool(
                "delegate executor unavailable for this Session",
            ));
        }
        crate::runtime::tool::delegate::select_child(
            &live.catalog,
            live.snapshot.selected_mode.as_deref(),
            target,
            mode,
        )?;
        let eligible = authorized_delegatable_agents(
            &live.catalog,
            live.snapshot.selected_mode.as_deref(),
            true,
            live.permission.as_ref(),
        )
        .map_err(projection_error)?;
        let mut catalog = live.catalog.clone();
        catalog.retain_mut(|agent| {
            if let Some(descriptor) = eligible
                .iter()
                .find(|descriptor| descriptor.name == agent.name)
            {
                agent.modes.retain(|mode| {
                    descriptor
                        .modes
                        .iter()
                        .any(|candidate| candidate.name == mode.name)
                });
                !agent.modes.is_empty()
            } else {
                false
            }
        });
        if live.catalog.iter().any(|agent| agent.name == target)
            && !catalog.iter().any(|agent| agent.name == target)
        {
            return Err(Error::denied(
                "delegate target has no authorized eligible modes",
            ));
        }
        let (agent, mode) = crate::runtime::tool::delegate::select_child(
            &catalog,
            live.snapshot.selected_mode.as_deref(),
            target,
            mode,
        )
        .map_err(|error| match error.public_reason() {
            Some(reason) => Error::Denied(crate::error::ErrorReason::with_private(
                reason,
                error.to_string(),
            )),
            None => Error::Denied(crate::error::ErrorReason::with_private(
                &error.diagnostic_reason(),
                error.to_string(),
            )),
        })?;
        Ok((agent.name, mode))
    }
    pub(crate) fn install_delegate_availability(&self, available: bool) {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .delegate_available = available;
    }
    pub(crate) fn install_switch_mode_availability(&self, available: bool) {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .switch_mode_available = available;
    }
    /// Public read-only facts use the same derivation as model State. Handles and
    /// custom-variable mutation remain separate from this ephemeral view.
    pub fn view(&self) -> Result<Value, Error> {
        current_view(false, || {
            self.inner
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone()
        })
    }
    /// Explicit owner inspection includes all custom values; automatic projections stay visible-only.
    pub fn view_all(&self) -> Result<Value, Error> {
        current_view(false, || {
            self.inner
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone()
                .with_all_variables()
        })
    }

    /// One live capture binds every displayed fact to the same custom revision.
    pub fn inspection(&self) -> Result<VariableInspection, Error> {
        let live = self
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let values = current_projection(&live.clone().with_all_variables(), false)
            .map_err(projection_error)?;
        let metadata = inspection_metadata(&live)?;
        let custom = live.snapshot.variables.metadata();
        let mut descriptors = BTreeMap::new();
        if let Some(values) = values.as_object() {
            for name in values.keys() {
                let descriptor = if let Some(metadata) = custom
                    .get(name)
                    .filter(|_| !is_reserved_key(name) && !live.snapshot.references.owns(name))
                {
                    let owner = metadata.owner == VariableRole::Client;
                    let secret =
                        metadata.visibility == microcrab_protocol::VariableVisibility::Secret;
                    let editable =
                        !secret && (owner || metadata.editors.contains(&VariableRole::Client));
                    VariableDescriptor {
                        kind: "custom".into(),
                        editable,
                        owner_editable: owner && !secret,
                        deletable: owner,
                        reason: if secret {
                            Some("Secret values are write-only".into())
                        } else if editable {
                            None
                        } else {
                            Some("Client is not an editor of this variable".into())
                        },
                    }
                } else {
                    let reference = live.snapshot.references.owns(name);
                    VariableDescriptor {
                        kind: if reference { "reference" } else { "framework" }.into(),
                        editable: false,
                        owner_editable: false,
                        deletable: false,
                        reason: Some(
                            if reference {
                                "Reference facts are read-only"
                            } else {
                                "Framework facts are read-only"
                            }
                            .into(),
                        ),
                    }
                };
                descriptors.insert(name.clone(), descriptor);
            }
        }
        Ok(VariableInspection {
            values,
            metadata,
            descriptors,
            bytes: if self.actor == VariableRole::Client {
                live.bytes.metadata()
            } else {
                BTreeMap::new()
            },
            witness: live.witness(),
        })
    }
    /// Safe durable projection; authority digests must use the original input.
    pub fn receipt_change(&self, change: &VariablesChange) -> VariablesChange {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .snapshot
            .variables
            .receipt_change(change)
    }

    /// Attributes are a separate pure map and never wrap returned values.
    pub fn variable_metadata(&self) -> Result<Value, Error> {
        let live = self
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        inspection_metadata(&live)
    }

    pub fn processes(&self) -> Arc<dyn crate::runtime::execution::process::ProcessControl> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .processes
            .clone()
    }
    pub fn variables(&self) -> Value {
        let mut values = self
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .snapshot
            .variables
            .as_value();
        if let Value::Object(values) = &mut values {
            values.remove(crate::admission::variables::EXECUTION_CAPACITY_KEY);
            values.remove("permission");
        }
        values
    }
    pub fn extract(&self, key: &str) -> Option<Value> {
        if matches!(
            key,
            crate::admission::variables::EXECUTION_CAPACITY_KEY | "permission"
        ) {
            return None;
        }
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .snapshot
            .variables
            .get(key)
            .cloned()
    }
    /// Private guest/auditor read path. Public extract remains custom-only;
    /// detached snapshots cannot impersonate an attached Session variable view.
    pub(super) fn ensure_variable_open(&self) -> Result<(), Error> {
        let Writer::Session(owner) = &self.writer else {
            return Err(Error::Closed);
        };
        owner.upgrade().ok_or(Error::Closed)?.ensure_open()
    }
    pub(crate) fn clear_ephemeral_variables(&self) {
        let mut live = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        live.snapshot.variables.clear_ephemeral();
        live.bytes.clear();
    }
    pub(super) fn wasm_extract(&self, key: &str) -> Result<Option<Value>, Error> {
        let Writer::Session(owner) = &self.writer else {
            return Err(Error::Closed);
        };
        let owner = owner.upgrade().ok_or(Error::Closed)?;
        owner.ensure_open()?;
        if is_reserved_key(key)
            || self
                .inner
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .snapshot
                .references
                .owns(key)
        {
            Ok(self.view()?.get(key).cloned())
        } else {
            Ok(self
                .inner
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .snapshot
                .variables
                .get(key)
                .cloned())
        }
    }
    pub async fn inject(&self, key: impl Into<String>, value: Value) -> Result<(), Error> {
        self.change(VariablesChange {
            scoped_set: vec![],
            set: Map::from_iter([(key.into(), value)]),
            remove: vec![],
        })
        .await
    }
    pub async fn patch(&self, value: Value) -> Result<(), Error> {
        let patch = Variables::new(value)?;
        let Value::Object(set) = patch.as_value() else {
            unreachable!("Variables is an object")
        };
        self.change(VariablesChange {
            scoped_set: vec![],
            set,
            remove: vec![],
        })
        .await
    }
    pub async fn remove(&self, key: &str) -> Result<(), Error> {
        self.change(VariablesChange {
            scoped_set: vec![],
            set: Map::new(),
            remove: vec![key.into()],
        })
        .await
    }
    pub async fn change(&self, change: VariablesChange) -> Result<(), Error> {
        validate_change(&change)?;
        self.reject_reference_keys(&change)?;
        match &self.writer {
            Writer::Session(owner) => {
                let owner = owner.upgrade().ok_or(Error::Closed)?;
                owner.change(change, self.actor, None).await.map(|_| ())
            }
            Writer::Detached => Err(Error::config(
                "State is not attached to a Session command channel",
            )),
        }
    }
    pub async fn change_checked(
        &self,
        change: VariablesChange,
        expected: VariableWitness,
    ) -> Result<VariableCommit, Error> {
        validate_change(&change)?;
        self.reject_reference_keys(&change)?;
        match &self.writer {
            Writer::Session(owner) => {
                owner
                    .upgrade()
                    .ok_or(Error::Closed)?
                    .change(change, self.actor, Some(expected))
                    .await
            }
            Writer::Detached => Err(Error::config(
                "State is not attached to a Session command channel",
            )),
        }
    }
    /// Atomic commit used by the variable pump and the Session hook wrapper.
    /// The wrapper owns hook admission, deadlines, and terminal-write policy;
    /// validation and mutation remain one lock-protected transaction here.
    pub(crate) fn apply_change(&self, change: VariablesChange) -> Result<(), Error> {
        self.apply_change_checked(change, self.actor, None)
            .map(|_| ())
    }
    pub(super) fn apply_change_checked(
        &self,
        change: VariablesChange,
        actor: VariableRole,
        expected: Option<VariableWitness>,
    ) -> Result<VariableCommit, Error> {
        validate_change(&change)?;
        let mut state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let before = state.witness();
        if expected
            .as_ref()
            .is_some_and(|expected| expected != &before)
        {
            return Err(Error::VariableConflict);
        }
        reject_owned_keys(&state.snapshot.references, &change)?;
        if change
            .set
            .keys()
            .chain(&change.remove)
            .chain(change.scoped_set.iter().map(|entry| &entry.name))
            .any(|name| state.bytes.owns(name))
        {
            return Err(Error::VariableForbidden(
                "Bytes variables require the typed Client endpoint".into(),
            ));
        }
        let mut variables = state.snapshot.variables.clone();
        variables.change(&change, actor)?;
        crate::admission::variables::admit(
            &variables.admission_value(),
            state.snapshot.limits.max_context_bytes,
        )?;
        let changed = variables != state.snapshot.variables;
        if changed {
            state.variables_revision =
                state.variables_revision.checked_add(1).ok_or_else(|| {
                    crate::budget::reject(
                        crate::LimitKind::Arithmetic,
                        "variable revision overflow",
                    )
                })?;
            state.snapshot.variables = variables;
        }
        Ok(VariableCommit {
            before,
            after: state.witness(),
            changed,
        })
    }
    /// Publish a validated input candidate under the caller's variable fence.
    /// Context validation and reference callbacks already finished on the preview;
    /// a concurrent mode/configuration change rejects this candidate before commit.
    pub(crate) fn commit_input(&self, original: &State, candidate: &State) -> bool {
        let original = original
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let candidate = candidate
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let mut live = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if live.runtime_instance_id != original.runtime_instance_id
            || live.variables_revision != original.variables_revision
            || live.references_revision != original.references_revision
            || live.active_correspondents != original.active_correspondents
            || live.snapshot.communication != original.snapshot.communication
            || live.snapshot.dialogue_turn != original.snapshot.dialogue_turn
            || live.snapshot.dialogue_turn_live != original.snapshot.dialogue_turn_live
            || live.snapshot.selected_mode != original.snapshot.selected_mode
            || live.snapshot.epoch_id != original.snapshot.epoch_id
            || live.snapshot.last_turn_id != original.snapshot.last_turn_id
            || live.snapshot.limits != original.snapshot.limits
            || live.short_model_refs != original.short_model_refs
            || live.reference_projection != original.reference_projection
        {
            return false;
        }
        live.snapshot.communication = candidate.snapshot.communication;
        live.snapshot.dialogue_turn = candidate.snapshot.dialogue_turn;
        live.snapshot.dialogue_turn_live = candidate.snapshot.dialogue_turn_live;
        live.snapshot.variables = candidate.snapshot.variables;
        live.variables_revision = candidate.variables_revision;
        live.snapshot.turn_id = candidate.snapshot.turn_id;
        live.snapshot.round_id = candidate.snapshot.round_id;
        live.snapshot.last_turn_id = candidate.snapshot.last_turn_id;
        live.builtins = candidate.builtins;
        live.reference_projection = candidate.reference_projection;
        true
    }

    pub(crate) fn update(&self, change: impl FnOnce(&mut SessionState)) {
        change(
            &mut self
                .inner
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .snapshot,
        );
    }
    pub(crate) fn refresh(&self, event: Boundary) -> Result<(), Error> {
        // Closing an unstarted Session or a failed initialization still refreshes
        // termination, but must not call a projection that requires load first.
        let read_references = event != Boundary::SessionTerminated
            || self
                .inner
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .references_loaded;
        self.refresh_inner(event, read_references)
    }

    /// Replace one preloaded kind at an idle Session configuration boundary.
    /// The caller owns foreground and the variable fence. User reference code
    /// runs before the State lock; validation failure preserves both wiring and
    /// the committed projection, and never changes historical Context frames.
    pub(crate) fn replace_reference(
        &self,
        reference: super::reference::DynReference,
    ) -> Result<(), Error> {
        validate_custom_key(reference.kind())?;
        let projection = reference.projections()?;
        let original = self
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let mut candidate = original.clone();
        let mut references: Vec<_> = original.snapshot.references.iter().cloned().collect();
        if let Some(existing) = references
            .iter_mut()
            .find(|existing| existing.kind() == reference.kind())
        {
            *existing = reference.clone();
        } else {
            references.push(reference.clone());
        }
        candidate.snapshot.references = References::new(references)?;
        if original.bytes.owns(reference.kind()) {
            return Err(Error::config(
                "reference kind conflicts with a live Bytes variable",
            ));
        }
        candidate
            .snapshot
            .references
            .validate_variables(&candidate.snapshot.variables)?;
        let previous = candidate
            .reference_projection
            .iter()
            .position(|(kind, _)| kind == reference.kind());
        match (
            previous,
            projection.exact.is_some() || projection.short_model.is_some(),
        ) {
            (Some(index), true) => candidate.reference_projection[index].1 = projection,
            (Some(index), false) => {
                candidate.reference_projection.remove(index);
            }
            (None, true) => candidate
                .reference_projection
                .push((reference.kind().to_owned(), projection)),
            (None, false) => {}
        }
        let values = current_projection(&candidate, true).map_err(projection_error)?;
        crate::admission::Admission::serialized(
            crate::admission::AdmissionSource::StateDelivery,
            crate::LimitKind::ContextBytes,
            &values,
            candidate.snapshot.limits.max_context_bytes,
        )?;
        let mut live = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if live.runtime_instance_id != original.runtime_instance_id
            || live.variables_revision != original.variables_revision
            || live.references_revision != original.references_revision
            || live.short_model_refs != original.short_model_refs
            || live.reference_projection != original.reference_projection
            || live.active_correspondents != original.active_correspondents
            || live.snapshot.communication != original.snapshot.communication
            || live.snapshot.selected_mode != original.snapshot.selected_mode
        {
            return Err(Error::config(
                "State changed while replacing references; retry at an idle boundary",
            ));
        }
        let revision = live.references_revision.checked_add(1).ok_or_else(|| {
            crate::budget::reject(
                crate::LimitKind::Arithmetic,
                "reference generation overflow",
            )
        })?;
        live.snapshot.references = candidate.snapshot.references;
        live.references_revision = revision;
        live.reference_projection = candidate.reference_projection;
        Ok(())
    }

    pub(crate) fn refresh_reference_projection(&self) -> Result<(), Error> {
        let references = self.snapshot().references.clone();
        let projection = capture_reference_projections(&references)?;
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .reference_projection = projection;
        Ok(())
    }

    /// Refresh framework counters for validation without running reference code.
    pub(crate) fn refresh_preview(&self, event: Boundary) -> Result<(), Error> {
        self.refresh_inner(event, false)
    }

    fn refresh_inner(&self, event: Boundary, read_references: bool) -> Result<(), Error> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::config("system clock precedes Unix epoch"))?
            .as_millis();
        let timestamp = u64::try_from(timestamp).map_err(|_| {
            crate::budget::reject(crate::LimitKind::Arithmetic, "system time overflow")
        })?;
        let projection = if read_references {
            let references = self.snapshot().references.clone();
            Some(capture_reference_projections(&references)?)
        } else {
            None
        };
        let mut state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        // This string is a State projection, not part of the shared lifecycle enum.
        let boundary = match event {
            Boundary::SessionCreated => "session_created",
            Boundary::SessionRestored => "session_restored",
            Boundary::EpochStarted => "epoch_started",
            Boundary::EpochEnded => "epoch_ended",
            Boundary::TurnStarted => "turn_started",
            Boundary::TurnEnded => "turn_ended",
            Boundary::RoundStarted => "round_started",
            Boundary::RoundSuspended => "round_suspended",
            Boundary::RoundResumed => "round_resumed",
            Boundary::RoundEnded => "round_ended",
            Boundary::SessionTerminated => "session_terminated",
        };
        let snapshot = &state.snapshot;
        let session = json!({"epoch":snapshot.epoch_id,"turn":snapshot.turn_id,"round":snapshot.round_id,"boundary":boundary});
        state.builtins.insert("session".into(), session);
        state
            .builtins
            .insert("time".into(), json!({"unix_millis":timestamp}));
        if let Some(projection) = projection {
            state.reference_projection = projection;
        }
        Ok(())
    }
    /// Capture current facts at the legal model boundary. This does not mutate
    /// already admitted Context frames or an in-flight provider request.
    pub(crate) fn model_projection_attempt(&self) -> Result<Value, crate::policy::PermissionError> {
        let live = self
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        current_projection(&live, true)
    }

    pub(crate) fn model_variables(&self) -> Result<Value, Error> {
        current_view(true, || {
            self.inner
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone()
        })
    }

    fn reject_reference_keys(&self, change: &VariablesChange) -> Result<(), Error> {
        let state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        reject_owned_keys(&state.snapshot.references, change)
    }
}

fn capture_reference_projections(
    references: &References,
) -> Result<Vec<(String, super::reference::ReferenceProjection)>, Error> {
    references
        .iter()
        .filter_map(|reference| match reference.projections() {
            Ok(projection) if projection.exact.is_none() && projection.short_model.is_none() => {
                None
            }
            Ok(projection) => Some(Ok((reference.kind().to_owned(), projection))),
            Err(error) => Some(Err(error)),
        })
        .collect()
}

fn reject_owned_keys(references: &References, change: &VariablesChange) -> Result<(), Error> {
    for key in change
        .set
        .keys()
        .chain(change.remove.iter())
        .chain(change.scoped_set.iter().map(|entry| &entry.name))
    {
        if references.owns(key) {
            return Err(Error::config_with_private(
                "reference kind is not a custom variable",
                format!("reference kind `{key}` is not a custom variable"),
            ));
        }
    }
    Ok(())
}
/// One current projection serves both public readers and model admission.
/// Reference callbacks run only in refresh; readers reuse their committed JSON.
// Keep transient policy races typed until the bounded reader has retried a fresh
// State capture. Invalid configuration and persistence uncertainty never retry.
pub(crate) fn projection_error(error: crate::policy::PermissionError) -> Error {
    match error {
        crate::policy::PermissionError::StalePolicy => Error::StateObservationConflict,
        error => Error::config_with_private(error.public_reason(), error.to_string()),
    }
}
fn current_view(model: bool, mut capture: impl FnMut() -> LiveState) -> Result<Value, Error> {
    for _ in 0..3 {
        match current_projection(&capture(), model) {
            Ok(value) => return Ok(value),
            Err(crate::policy::PermissionError::StalePolicy) => continue,
            Err(error) => return Err(projection_error(error)),
        }
    }
    Err(Error::StateObservationConflict)
}
fn current_projection(
    live: &LiveState,
    model: bool,
) -> Result<Value, crate::policy::PermissionError> {
    let witness = live
        .permission
        .as_ref()
        .map(|permission| {
            permission.capture_witness(
                live.agent
                    .as_ref()
                    .zip(live.snapshot.selected_mode.as_deref())
                    .map(|(agent, mode)| (agent.name.as_str(), mode)),
            )
        })
        .transpose()?;
    let Value::Object(mut values) = live.snapshot.variables.as_value() else {
        unreachable!("Variables is an object")
    };
    // Initialization evidence belongs to Session, never the model-facing variable map.
    // Explicit owner captures add its authoritative values through builtins below.
    for key in ["agent_init", "agent_initialization_pending"] {
        values.remove(key);
    }
    values.extend(live.builtins.clone());
    let short_model = model && live.short_model_refs;
    values.extend(
        live.reference_projection
            .iter()
            .filter_map(|(kind, projection)| {
                projection
                    .value(short_model)
                    .map(|value| (kind.clone(), value.clone()))
            }),
    );
    for key in ["tool_tasks", "jobs", "subagents"] {
        values.remove(key);
    }
    let (executions, capacity) = live.executions.model_observation();
    if executions.is_empty() {
        values.remove("executions");
    } else {
        let rows = if short_model {
            let metadata: Vec<_> = executions
                .into_iter()
                .map(crate::runtime::execution::ModelExecutionMetadata::from)
                .collect();
            json!(metadata)
        } else {
            json!(executions)
        };
        values.insert("executions".into(), rows);
    }
    values.insert(
        crate::admission::variables::EXECUTION_CAPACITY_KEY.into(),
        json!(capacity),
    );
    // Do not let a legacy custom variable with this name stand in for a live hint.
    values.remove("switchable_modes");
    if let Some(mode) = &live.snapshot.selected_mode {
        values.insert("mode".into(), json!(mode));
        if let Some((agent, metadata)) = live.agent.as_ref().and_then(|agent| {
            agent
                .modes
                .iter()
                .find(|candidate| candidate.name == *mode)
                .map(|metadata| (agent, metadata))
        }) {
            values.insert("mode_prompt".into(), json!(metadata.prompt));
            if live.switch_mode_available {
                // Discovery is role/catalog eligibility, not permission to switch.
                let targets = agent
                    .modes
                    .iter()
                    .filter(|target| {
                        target.name != *mode
                            && crate::session::mode_role_eligible(live.is_main, target.role)
                    })
                    .map(|target| target.name.as_str())
                    .collect::<Vec<_>>();
                if !targets.is_empty() {
                    values.insert("switchable_modes".into(), json!(targets));
                }
            }
        } else {
            values.remove("mode_prompt");
        }
    } else {
        values.remove("mode");
        values.remove("mode_prompt");
    }
    // Never expose the model hint (or a legacy value with the same key) in
    // generic State reads. Keeping this key reserved is independent of visibility.
    values.remove("permission");
    if model && let Some(permission) = &live.permission {
        values.insert(
            "permission".into(),
            permission.project(live.permission_model_names.as_deref())?,
        );
    }
    let eligible = authorized_delegatable_agents(
        &live.catalog,
        live.snapshot.selected_mode.as_deref(),
        live.is_main && live.delegate_available,
        live.permission.as_ref(),
    )?;
    if eligible.is_empty() {
        values.remove("delegatable_agents");
    } else {
        values.insert("delegatable_agents".into(), json!(eligible));
    }
    // Counters and variable commits can advance between lifecycle refreshes.
    let session = values.entry("session").or_insert_with(|| json!({}));
    let session = session
        .as_object_mut()
        .expect("framework session facts are an object");
    session.insert("epoch".into(), json!(live.snapshot.epoch_id));
    session.insert("turn".into(), json!(live.snapshot.turn_id));
    session.insert("round".into(), json!(live.snapshot.round_id));
    session.insert("variables_revision".into(), json!(live.variables_revision));
    if let Some(kind) = &live.snapshot.session_kind {
        session.insert("kind".into(), json!(kind));
    } else {
        session.remove("kind");
    }
    if let Some(communication) = &live.snapshot.communication {
        session.insert("agent_id".into(), json!(communication.identity.agent_id));
        session.insert("depth".into(), json!(communication.identity.depth));
        session.insert(
            "interaction".into(),
            json!(communication.identity.interaction),
        );
        let mut correspondents = communication
            .correspondents
            .iter()
            .filter(|row| live.active_correspondents.contains(&row.agent_id))
            .cloned()
            .collect::<Vec<_>>();
        let mut speakers = communication.external_speakers.clone();
        for carrier in &communication.inbox.pending {
            if let Some(row) = &carrier.correspondent
                && !correspondents
                    .iter()
                    .any(|prior| prior.agent_id == row.agent_id)
            {
                correspondents.push(row.clone());
            }
            if let Some(row) = &carrier.speaker
                && !speakers.iter().any(|prior| prior.identity == row.identity)
            {
                speakers.push(row.clone());
            }
        }
        values.insert("correspondents".into(), json!(correspondents));
        values.insert("external_speakers".into(), json!(speakers));
    }
    if let Some(witness) = witness {
        witness()?;
    }
    Ok(Value::Object(values))
}

#[derive(serde::Serialize, Clone, Debug, PartialEq)]
pub(crate) struct DelegatableAgentDescriptor {
    pub name: String,
    pub description: String,
    pub modes: Vec<DelegatableModeDescriptor>,
}
#[derive(serde::Serialize, Clone, Debug, PartialEq)]
pub(crate) struct DelegatableModeDescriptor {
    pub name: String,
    pub description: String,
}

fn authorized_delegatable_agents(
    catalog: &[microcrab_agent_types::AgentInfo],
    caller_mode: Option<&str>,
    available: bool,
    permission: Option<&crate::policy::PermissionObservation>,
) -> Result<Vec<DelegatableAgentDescriptor>, crate::policy::PermissionError> {
    if !available {
        return Ok(Vec::new());
    }
    let Some(permission) = permission else {
        return Ok(Vec::new());
    };
    let mut eligible = Vec::new();
    for agent in catalog {
        let mut modes = Vec::new();
        for mode in crate::runtime::tool::delegate::eligible_modes(agent, caller_mode) {
            let (action, _) = permission.delegate_policy(&agent.name, &mode)?;
            if action == crate::policy::Action::Deny {
                continue;
            }
            let description = agent
                .modes
                .iter()
                .find(|candidate| candidate.name == mode)
                .map(|metadata| metadata.description.clone())
                .unwrap_or_default();
            modes.push(DelegatableModeDescriptor {
                name: mode,
                description,
            });
        }
        if !modes.is_empty() {
            eligible.push(DelegatableAgentDescriptor {
                name: agent.name.clone(),
                description: agent
                    .variables
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or(&agent.name)
                    .to_owned(),
                modes,
            });
        }
    }
    Ok(eligible)
}
impl microcrab_auditor::VariableView for State {
    fn get(&self, name: &str) -> Option<Value> {
        self.wasm_extract(name).ok().flatten()
    }
    fn get_all(&self, name: &str) -> Option<Value> {
        self.view_all().ok()?.get(name).cloned()
    }
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State").finish_non_exhaustive()
    }
}
