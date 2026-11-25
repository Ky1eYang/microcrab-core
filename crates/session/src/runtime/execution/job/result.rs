//! Typed Job projections. Serialization is the model-text boundary only.
use crate::observation::diagnostics::{
    DiagnosticCode, DiagnosticOperation, DiagnosticPhase, EffectKnowledge, ToolDiagnostic,
};
use crate::runtime::execution::{
    ExecutionCapabilities, ExecutionId, ExecutionSnapshot, ExecutionStatus, ModelExecutionMetadata,
    OutputAvailability, OutputPage, PrivateOutputMetadata, ToolFailureKind,
};
use serde::Serialize;

#[derive(Serialize)]
#[serde(untagged)]
pub(super) enum ResultBody {
    Execution(Row),
    Read(OutputPage),
    Write { accepted: bool },
    Wait { revision: u64, execution: Row },
}

#[derive(Serialize)]
#[serde(untagged)]
pub(super) enum Row {
    Full(Box<ExecutionSnapshot>),
    Compact(CompactRow),
    Model(Box<ModelExecutionMetadata>),
}

impl ResultBody {
    pub(super) fn with_model_metadata(self) -> Self {
        match self {
            Self::Execution(Row::Full(row)) => Self::Execution(Row::Model(Box::new((*row).into()))),
            Self::Wait {
                revision,
                execution: Row::Full(row),
            } => Self::Wait {
                revision,
                execution: Row::Model(Box::new((*row).into())),
            },
            other => other,
        }
    }
}

impl Row {
    pub(super) fn new(row: ExecutionSnapshot, compact: bool) -> Self {
        if !compact {
            return Self::Full(Box::new(row));
        }
        Self::Compact(CompactRow {
            execution_id: row.execution_id,
            status: row.status,
            capabilities: row.capabilities,
            output_availability: row.output_availability,
            output: row.output.map(|output| CompactOutput {
                bytes: output.bytes,
                start_cursor: output.start_cursor,
                end_cursor: output.end_cursor,
                closed: output.closed,
                private_stdout: output.private_stdout,
            }),
            failure: row
                .failure
                .map(|failure| CompactFailure { kind: failure.kind }),
            diagnostic: row
                .diagnostic
                .map(|diagnostic| Diagnostic::new(diagnostic, false)),
        })
    }
}

#[derive(Serialize)]
pub(super) struct CompactRow {
    execution_id: ExecutionId,
    status: ExecutionStatus,
    capabilities: ExecutionCapabilities,
    output_availability: Option<OutputAvailability>,
    output: Option<CompactOutput>,
    failure: Option<CompactFailure>,
    diagnostic: Option<Diagnostic>,
}

#[derive(Serialize)]
struct CompactOutput {
    bytes: u64,
    start_cursor: u64,
    end_cursor: u64,
    closed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    private_stdout: Option<PrivateOutputMetadata>,
}

#[derive(Serialize)]
struct CompactFailure {
    kind: ToolFailureKind,
}

#[derive(Serialize)]
pub(super) struct Diagnostic {
    code: DiagnosticCode,
    phase: DiagnosticPhase,
    effects: EffectKnowledge,
    operation: DiagnosticOperation,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    #[serde(skip_serializing_if = "is_false")]
    reason_truncated: bool,
}

fn is_false(value: &bool) -> bool {
    !value
}

impl Diagnostic {
    pub(super) fn new(diagnostic: ToolDiagnostic, include_reason: bool) -> Self {
        let mut projection = Self {
            code: diagnostic.code,
            phase: diagnostic.phase,
            effects: diagnostic.effects,
            operation: DiagnosticOperation::Job,
            reason: None,
            reason_truncated: false,
        };
        if let Some(cause) = diagnostic.reason.filter(|_| include_reason) {
            // Keep the existing per-target envelope reservation.
            let mut reason = String::new();
            let mut encoded_bytes = 2;
            for character in cause.as_str().chars() {
                let width = serde_json::json!(character.to_string()).to_string().len() - 2;
                if encoded_bytes + width > 96 {
                    break;
                }
                reason.push(character);
                encoded_bytes += width;
            }
            projection.reason_truncated = reason.len() < cause.as_str().len();
            projection.reason = Some(reason);
        }
        projection
    }
}

#[derive(Serialize)]
pub(super) struct Batch<T> {
    pub(super) action: &'static str,
    pub(super) results: Vec<T>,
}

#[derive(Serialize)]
pub(super) struct Target<T> {
    pub(super) id: String,
    pub(super) ok: bool,
    #[serde(flatten)]
    pub(super) outcome: Outcome<T>,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Outcome<T> {
    Result(T),
    Error(Diagnostic),
}

#[derive(Serialize)]
pub(super) struct WaitInterrupted {
    pub(super) action: &'static str,
    pub(super) interrupted: bool,
    pub(super) reason: Interruption,
    pub(super) targets_cancelled: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn recovered_init_state_and_scalar_job_share_model_metadata_but_owner_stays_exact() {
        let state = crate::State::new(Default::default());
        let component = "7".repeat(64);
        let call_id = format!("agent-init:3:2:{component}");
        let mut row = ExecutionSnapshot::new(
            "session".into(),
            call_id.clone(),
            String::new(),
            "coding".into(),
            None,
        );
        let id = ExecutionId(1);
        row.execution_id = id;
        row.subject = crate::policy::InvocationSubject::AgentInit {
            component: component.clone(),
            generation: 3,
            attempt: 2,
        };
        let executions = state.executions();
        executions.restore(&[row]).unwrap();
        let restored = executions.get(id).unwrap();
        let legacy =
            serde_json::to_value(ResultBody::Execution(Row::new(restored.clone(), false))).unwrap();
        assert_eq!(legacy["call_id"], call_id);
        assert_eq!(legacy["subject"]["component"], component);
        assert_eq!(state.model_variables().unwrap()["executions"][0], legacy);
        state.set_short_model_refs(true);
        let model = serde_json::to_value(
            ResultBody::Execution(Row::new(restored.clone(), false)).with_model_metadata(),
        )
        .unwrap();
        assert_eq!(model, state.model_variables().unwrap()["executions"][0]);
        assert_eq!(model["subject"], json!({"kind": "agent_init"}));
        assert!(model.get("call_id").is_none());
        assert_eq!(model["status"], "interrupted");
        assert_eq!(model["diagnostic"], legacy["diagnostic"]);
        assert_eq!(model["execution_id"], 1);
        assert_eq!(state.view().unwrap()["executions"][0], legacy);
        assert_eq!(state.snapshot().executions[0].call_id, call_id);
        let wait = serde_json::to_value(
            ResultBody::Wait {
                revision: 5,
                execution: Row::new(restored, false),
            }
            .with_model_metadata(),
        )
        .unwrap();
        assert_eq!(wait, json!({"revision": 5, "execution": model}));
    }
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Interruption {
    UserSteer,
}
