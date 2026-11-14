//! Owner-specific admission limits and shared per-source output budgets.
//!
//! Field removal is deliberate: historical State snapshots still carry the retired
//! keys, and `Limits` does not deny unknown keys, so those snapshots load and the
//! retired keys are dropped. No constraint that only rejected ordinary content is
//! retained here; the Context consumption checkpoint owns that decision instead.
use crate::Error;
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Limits {
    /// Consecutive model requests per unattended segment; another segment needs
    /// user input or explicit round-limit confirmation from the embedding owner.
    pub max_rounds: usize,
    /// Per-round tool-call count. Crossing it asks the embedding owner instead of
    /// rejecting the already produced response.
    pub max_tool_calls_per_round: usize,
    /// Model-facing conversation budget. Crossing it asks for another byte block
    /// instead of failing the turn.
    pub max_context_bytes: usize,
    pub max_tool_output_bytes: usize,
    pub max_output_tokens: u32,
    pub interaction_timeout_ms: u64,
    pub max_pending_executions: usize,
    pub max_retained_executions: usize,
    pub lifecycle_timeout_ms: u64,
    /// Host process admission is independent of generic output-source admission.
    pub max_processes: usize,
    pub max_execution_buffers: usize,
    pub max_retained_processes: usize,
    /// Maximum bytes retained by each process capture or execution output source.
    pub output_buffer_bytes: usize,
    pub sleep_max_ms: u64,
}

// Decode retired limit keys only at the persistence/configuration boundary.
// New serialization exposes independent owners; old max_jobs initializes both
// capacities so restoring a historical non-default budget does not weaken it.
impl<'de> Deserialize<'de> for Limits {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Wire {
            max_rounds: usize,
            max_tool_calls_per_round: usize,
            max_context_bytes: usize,
            max_tool_output_bytes: usize,
            max_output_tokens: u32,
            #[serde(default = "default_interaction_timeout_ms")]
            interaction_timeout_ms: u64,
            #[serde(default = "default_execution_count", alias = "max_tool_tasks")]
            max_pending_executions: usize,
            #[serde(default = "default_execution_count", alias = "max_closed_tool_tasks")]
            max_retained_executions: usize,
            lifecycle_timeout_ms: u64,
            #[serde(default)]
            max_processes: Option<usize>,
            #[serde(default)]
            max_execution_buffers: Option<usize>,
            #[serde(default)]
            max_jobs: Option<usize>,
            #[serde(default = "default_execution_count", alias = "max_closed_jobs")]
            max_retained_processes: usize,
            #[serde(default = "default_output_buffer_bytes", alias = "job_output_bytes")]
            output_buffer_bytes: usize,
            #[serde(default = "default_sleep_max_ms")]
            sleep_max_ms: u64,
        }
        let wire = Wire::deserialize(deserializer)?;
        let legacy_capacity = wire.max_jobs.unwrap_or(8);
        Ok(Self {
            max_rounds: wire.max_rounds,
            max_tool_calls_per_round: wire.max_tool_calls_per_round,
            max_context_bytes: wire.max_context_bytes,
            max_tool_output_bytes: wire.max_tool_output_bytes,
            max_output_tokens: wire.max_output_tokens,
            interaction_timeout_ms: wire.interaction_timeout_ms,
            max_pending_executions: wire.max_pending_executions,
            max_retained_executions: wire.max_retained_executions,
            lifecycle_timeout_ms: wire.lifecycle_timeout_ms,
            max_processes: wire.max_processes.unwrap_or(legacy_capacity),
            max_execution_buffers: wire.max_execution_buffers.unwrap_or(legacy_capacity),
            max_retained_processes: wire.max_retained_processes,
            output_buffer_bytes: wire.output_buffer_bytes,
            sleep_max_ms: wire.sleep_max_ms,
        })
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_rounds: 64,
            max_tool_calls_per_round: 16,
            max_context_bytes: 64 * 1024 * 1024,
            max_tool_output_bytes: 16 * 1024,
            max_output_tokens: 4096,
            interaction_timeout_ms: default_interaction_timeout_ms(),
            max_pending_executions: default_execution_count(),
            max_retained_executions: default_execution_count(),
            lifecycle_timeout_ms: 10_000,
            max_processes: 8,
            max_execution_buffers: 8,
            max_retained_processes: default_execution_count(),
            output_buffer_bytes: default_output_buffer_bytes(),
            sleep_max_ms: default_sleep_max_ms(),
        }
    }
}

fn default_interaction_timeout_ms() -> u64 {
    300_000
}
fn default_execution_count() -> usize {
    16
}
fn default_output_buffer_bytes() -> usize {
    4 * 1024 * 1024
}
fn default_sleep_max_ms() -> u64 {
    3_600_000
}

impl Limits {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if [
            self.max_rounds,
            self.max_tool_calls_per_round,
            self.max_context_bytes,
            self.max_tool_output_bytes,
            self.max_processes,
            self.max_execution_buffers,
            self.max_pending_executions,
            self.max_retained_executions,
            self.output_buffer_bytes,
        ]
        .contains(&0)
            || self.max_output_tokens == 0
            || self.interaction_timeout_ms == 0
            || self.lifecycle_timeout_ms == 0
            || self.sleep_max_ms == 0
            || self.sleep_max_ms > 3_600_000
        {
            return Err(Error::config("all execution limits must be positive"));
        }
        Ok(())
    }
}
