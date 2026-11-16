//! Runtime recovery values contain no creator identity, history, or storage metadata.
use microcrab_agent_types::AgentInfo;
use microcrab_protocol::Usage;
use serde::{Deserialize, Serialize};

use crate::{Error, SessionState, context::ContextSnapshot};

/// Trusted invocation identity disambiguates interleaved captures of one path.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordedCachePoint {
    pub call_id: String,
    pub tool: String,
    pub sequence: u32,
    #[serde(flatten)]
    pub point: microcrab_host_api::FileCachePoint,
}

/// Explicit tool-selected observations persisted with a settled round. These
/// records do not establish filesystem provenance or authorize restoration.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoundCacheReport {
    pub epoch_id: u64,
    pub turn_id: u64,
    pub round_id: u64,
    pub points: Vec<RecordedCachePoint>,
}

/// Versioned lifecycle evidence. Intent is durable before effects; a missing
/// settlement blocks replay after recovery. Ready never snapshots guest memory.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentInitRecord {
    pub version: u8,
    pub component: String,
    pub generation: u64,
    pub attempt: u64,
    pub status: AgentInitStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cache_points: Vec<RecordedCachePoint>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentInitStatus {
    Intent,
    Ready,
    Failed {
        failure: crate::runtime::execution::ToolFailure,
    },
}
impl AgentInitRecord {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.version != 1
            || self.component.len() != 64
            || !self.component.bytes().all(|byte| byte.is_ascii_hexdigit())
            || self.generation == 0
            || self.attempt == 0
        {
            return Err(Error::config("invalid initialization attempt evidence"));
        }
        if let AgentInitStatus::Failed { failure } = &self.status {
            failure.validate()?;
        }
        crate::runtime::cache::validate_points(&self.cache_points)?;
        Ok(())
    }
}

/// Frozen metadata is evidence, never live permission authority. Retain its JSON
/// representation so old rules cannot be normalized through the current DTO.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HistoricalAgent(serde_json::Value);

impl HistoricalAgent {
    /// Descriptive history metadata only; this does not admit permissions or a
    /// replacement component and cannot establish activation compatibility.
    pub fn name(&self) -> Option<&str> {
        self.0.get("name").and_then(serde_json::Value::as_str)
    }

    /// Activation must explicitly admit the complete strict current descriptor.
    /// Failure leaves the frozen evidence intact and must block only its Session.
    pub fn current(&self) -> Result<AgentInfo, Error> {
        if let Some(modes) = self.0.get("modes").and_then(serde_json::Value::as_array) {
            for (mode_index, mode) in modes.iter().enumerate() {
                if let Some(rules) = mode
                    .get("permissions")
                    .and_then(serde_json::Value::as_array)
                {
                    for (rule_index, rule) in rules.iter().enumerate() {
                        for field in [
                            "capability",
                            "resource",
                            "arguments",
                            "operation_arguments",
                            "conditions",
                        ] {
                            if rule
                                .as_object()
                                .is_some_and(|object| object.contains_key(field))
                            {
                                return Err(Error::Config(format!(
                                    "historical agent unavailable for current activation: agent.modes[{mode_index}].permissions[{rule_index}] contains unsupported legacy field {field}; current rules use tool, args and action"
                                ).into()));
                            }
                        }
                        serde_json::from_value::<microcrab_permission::Permission>(rule.clone())
                            .map_err(|error| Error::Config(format!(
                                "historical agent unavailable for current activation: agent.modes[{mode_index}].permissions[{rule_index}]: {error}"
                            ).into()))?;
                    }
                }
            }
        }
        serde_json::from_value(self.0.clone()).map_err(|error| {
            Error::Config(
                format!("historical agent is unavailable for current activation: {error}").into(),
            )
        })
    }
}
impl From<AgentInfo> for HistoricalAgent {
    fn from(agent: AgentInfo) -> Self {
        // AgentInfo contains only JSON-serializable descriptor fields.
        Self(serde_json::to_value(agent).expect("AgentInfo serializes as JSON"))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Recover {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_config: Option<crate::context::system::PromptConfig>,
    pub state: SessionState,
    pub context: ContextSnapshot,
    pub continuation: Option<RoundContinuation>,
    /// Legacy whole-workspace bytes remain readable but are inert during execution.
    pub workspace: Option<microcrab_host_api::WorkspaceSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_report: Option<super::RoundCacheReport>,
    pub agent: Option<HistoricalAgent>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoundContinuation {
    pub next_round: usize,
    /// Consecutive requests since the last user intervention. Legacy records
    /// default to zero; total round ordinal remains independently monotonic.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub unattended_rounds: usize,
    pub usage: Usage,
    /// The failed model request is already materialized. Resume must send it
    /// before admitting inbox changes or refreshing boundary State. An explicit
    /// route recovery refresh may rebuild its live Context in a fresh epoch while
    /// retaining these Turn/round identities and all admitted semantic work.
    #[serde(default, skip_serializing_if = "is_false")]
    pub request_prepared: bool,
    /// The next request has not materialized because its current State raced a
    /// permission/mode update. Explicit retry refreshes that boundary only; all
    /// acknowledged input and settled tool batches remain admitted exactly once.
    #[serde(default, skip_serializing_if = "is_false")]
    pub state_observation_pending: bool,
}

impl Recover {
    pub fn validate(&self) -> Result<(), Error> {
        self.context.validate()?;
        if let Some(config) = &self.prompt_config {
            config.validate()?;
        }
        if let Some(report) = &self.cache_report {
            report.validate()?;
        }
        self.state.limits.validate()?;
        if self.state.agent_init.len() > 128 {
            return Err(Error::config("initialization evidence capacity exceeded"));
        }
        let mut components = std::collections::HashSet::new();
        for record in &self.state.agent_init {
            record.validate()?;
            if !components.insert(&record.component) {
                return Err(Error::config("duplicate initialization evidence"));
            }
        }
        crate::runtime::execution::ExecutionRegistry::validate_rows(
            &self.state.executions,
            &self.state.limits,
        )?;
        crate::budget::check(
            crate::LimitKind::RecoveryBytes,
            self.context.max_bytes,
            self.state.limits.max_context_bytes,
        )?;
        if self.state.epoch_id != self.context.epoch {
            return Err(Error::config("recovery state/context epoch mismatch"));
        }
        if let Some(continuation) = &self.continuation {
            if continuation.request_prepared && continuation.state_observation_pending {
                return Err(Error::config(
                    "continuation cannot have both a prepared request and pending State observation",
                ));
            }
            if self.context.pending_epoch {
                return Err(Error::config("continuation cannot contain a pending epoch"));
            }
            if continuation.request_prepared
                && self.state.round_id != Some(continuation.next_round as u64)
            {
                return Err(Error::config(
                    "prepared request requires its original round identity",
                ));
            }
            if self.state.turn_id.is_none() || continuation.next_round == 0 {
                return Err(Error::config(
                    "continuation requires an active turn and next round",
                ));
            }
        } else if self.state.turn_id.is_some() || self.state.round_id.is_some() {
            return Err(Error::config("settled recovery contains an active turn"));
        }
        Ok(())
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn is_zero(value: &usize) -> bool {
    *value == 0
}
