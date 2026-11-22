//! Runtime execution metadata; model-facing context is owned separately.
use super::reference::References;
use super::{Limits, Variables};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Runtime-owned state exposed immutably to lifecycle plugins.
///
/// Mutation is serialized by the session owner. Cloning captures runtime state,
/// including variables, but does not snapshot the model-facing context.
/// Reference kinds are init wiring: clones share them, and recovery does not
/// persist them.
#[derive(Debug, Clone)]
pub struct SessionState {
    pub epoch_id: u64,
    pub turn_id: Option<u64>,
    pub round_id: Option<u64>,
    pub selected_mode: Option<String>,
    /// None preserves the absence in frozen historical checkpoints.
    pub session_kind: Option<crate::SessionKind>,
    pub parent_relationship: Option<crate::DelegationRelationship>,
    pub communication: Option<super::communication::CommunicationState>,
    pub(crate) dialogue_turn: Option<super::communication::turn::DialogueTurnWitness>,
    pub(crate) dialogue_turn_live: bool,
    pub limits: Limits,
    pub variables: Variables,
    /// Bounded observations only. Pending rows restore as interrupted, never replayed futures.
    pub executions: Vec<crate::runtime::execution::ExecutionSnapshot>,
    /// Durable init attempt evidence; absent for unchanged historical snapshots.
    pub agent_init: Vec<crate::runtime::journal::AgentInitRecord>,
    /// True only while a fresh Session's initial setup batch is unfinished.
    pub agent_initialization_pending: bool,
    /// Historical observations are inert bytes, never executable handles. Keeping
    /// this field separate preserves old checkpoints without inventing an ID map.
    pub(crate) tool_tasks: Option<serde_json::Value>,
    /// Last allocated turn within the selected epoch; restored with that history.
    pub(crate) last_turn_id: u64,
    pub(crate) references: References,
    // Immutable decoded history keeps its original limit keys for content hashes.
    // Live activation clears this marker; changing effective limits also retires it.
    historical_limits: Option<(serde_json::Value, Limits)>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum LimitsEncoding<'a> {
    Historical(&'a serde_json::Value),
    Current(&'a Limits),
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl Serialize for SessionState {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Wire<'a> {
            epoch_id: u64,
            turn_id: Option<u64>,
            round_id: Option<u64>,
            selected_mode: &'a Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            session_kind: &'a Option<crate::SessionKind>,
            #[serde(skip_serializing_if = "Option::is_none")]
            parent_relationship: &'a Option<crate::DelegationRelationship>,
            #[serde(skip_serializing_if = "Option::is_none")]
            communication: &'a Option<super::communication::CommunicationState>,
            #[serde(skip_serializing_if = "Option::is_none")]
            dialogue_turn: &'a Option<super::communication::turn::DialogueTurnWitness>,
            limits: LimitsEncoding<'a>,
            variables: &'a Variables,
            #[serde(skip_serializing_if = "serde_json::Map::is_empty")]
            invisible_variables: serde_json::Map<String, serde_json::Value>,
            #[serde(skip_serializing_if = "std::collections::BTreeMap::is_empty")]
            variable_metadata: std::collections::BTreeMap<String, serde_json::Value>,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            executions: &'a Vec<crate::runtime::execution::ExecutionSnapshot>,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            agent_init: &'a Vec<crate::runtime::journal::AgentInitRecord>,
            #[serde(skip_serializing_if = "is_false")]
            agent_initialization_pending: bool,
            #[serde(skip_serializing_if = "Option::is_none")]
            tool_tasks: &'a Option<serde_json::Value>,
            last_turn_id: u64,
        }
        let limits = match &self.historical_limits {
            Some((raw, decoded)) if decoded == &self.limits => LimitsEncoding::Historical(raw),
            _ => LimitsEncoding::Current(&self.limits),
        };
        Wire {
            epoch_id: self.epoch_id,
            turn_id: self.turn_id,
            round_id: self.round_id,
            selected_mode: &self.selected_mode,
            session_kind: &self.session_kind,
            parent_relationship: &self.parent_relationship,
            communication: &self.communication,
            dialogue_turn: &self.dialogue_turn,
            limits,
            variables: &self.variables,
            invisible_variables: self.variables.persisted_invisible(),
            variable_metadata: self.variables.persisted_metadata(),
            executions: &self.executions,
            agent_init: &self.agent_init,
            agent_initialization_pending: self.agent_initialization_pending,
            tool_tasks: &self.tool_tasks,
            last_turn_id: self.last_turn_id,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SessionState {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Wire {
            epoch_id: u64,
            turn_id: Option<u64>,
            round_id: Option<u64>,
            #[serde(default)]
            selected_mode: Option<String>,
            #[serde(default)]
            session_kind: Option<crate::SessionKind>,
            #[serde(default)]
            parent_relationship: Option<crate::DelegationRelationship>,
            #[serde(default)]
            communication: Option<super::communication::CommunicationState>,
            #[serde(default)]
            dialogue_turn: Option<super::communication::turn::DialogueTurnWitness>,
            limits: serde_json::Value,
            variables: Variables,
            #[serde(default)]
            invisible_variables: serde_json::Map<String, serde_json::Value>,
            #[serde(default)]
            variable_metadata: std::collections::BTreeMap<String, serde_json::Value>,
            #[serde(default)]
            executions: Vec<crate::runtime::execution::ExecutionSnapshot>,
            #[serde(default)]
            agent_init: Vec<crate::runtime::journal::AgentInitRecord>,
            #[serde(default)]
            agent_initialization_pending: bool,
            #[serde(default)]
            tool_tasks: Option<serde_json::Value>,
            last_turn_id: u64,
        }
        let mut wire = Wire::deserialize(deserializer)?;
        wire.variables
            .restore_metadata(wire.variable_metadata, wire.invisible_variables)
            .map_err(serde::de::Error::custom)?;
        let limits: Limits =
            serde_json::from_value(wire.limits.clone()).map_err(serde::de::Error::custom)?;
        let legacy = [
            "max_tool_tasks",
            "max_closed_tool_tasks",
            "max_jobs",
            "max_closed_jobs",
            "job_output_bytes",
        ]
        .iter()
        .any(|key| wire.limits.get(key).is_some());
        let historical_limits = legacy.then(|| (wire.limits, limits.clone()));
        Ok(Self {
            epoch_id: wire.epoch_id,
            turn_id: wire.turn_id,
            round_id: wire.round_id,
            selected_mode: wire.selected_mode,
            session_kind: wire.session_kind,
            parent_relationship: wire.parent_relationship,
            communication: wire.communication,
            dialogue_turn: wire.dialogue_turn,
            dialogue_turn_live: false,
            limits,
            variables: wire.variables,
            executions: wire.executions,
            agent_init: wire.agent_init,
            agent_initialization_pending: wire.agent_initialization_pending,
            tool_tasks: wire.tool_tasks,
            last_turn_id: wire.last_turn_id,
            references: References::default(),
            historical_limits,
        })
    }
}

impl SessionState {
    /// Last allocated Turn within this selected epoch, restored with its history.
    /// This counter is not a globally unique request or invocation identity.
    pub fn last_turn_id(&self) -> u64 {
        self.last_turn_id
    }

    pub(crate) fn clear_historical_limits(&mut self) {
        self.historical_limits = None;
    }

    pub fn new(limits: Limits) -> Self {
        Self {
            epoch_id: 0,
            turn_id: None,
            round_id: None,
            selected_mode: None,
            session_kind: None,
            parent_relationship: None,
            communication: None,
            dialogue_turn: None,
            dialogue_turn_live: false,
            limits,
            variables: Variables::default(),
            executions: Vec::new(),
            agent_init: Vec::new(),
            agent_initialization_pending: false,
            tool_tasks: None,
            last_turn_id: 0,
            references: References::default(),
            historical_limits: None,
        }
    }
}
