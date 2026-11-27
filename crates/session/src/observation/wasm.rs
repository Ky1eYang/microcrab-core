//! Safe boundary evidence. Correlation facts are observations, never authorization.
use super::{FailureClass, ObservationKind, Position};
use crate::runtime::tool::ToolContext;
use crate::{
    Error,
    observation::diagnostics::{
        DiagnosticCode, DiagnosticLayer, DiagnosticPhase, ToolDiagnostic, WasmFailure,
    },
};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    Compile,
    Instantiate,
    Budget,
    Describe,
    Init,
    Invoke,
    Drive,
}
impl Phase {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Compile => "compile",
            Self::Instantiate => "instantiate",
            Self::Budget => "budget",
            Self::Describe => "describe",
            Self::Init => "init",
            Self::Invoke => "invoke",
            Self::Drive => "drive",
        }
    }
}

/// Callback errors retain typed provenance without parsing Wasmtime/native text.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Stop {
    #[error("WASM operation cancelled")]
    Cancelled,
    #[error("WASM wall deadline exceeded")]
    Deadline,
}

pub(crate) fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(crate) struct EvidenceBoundary {
    component: String,
    session: Option<String>,
    tool: Option<String>,
    call: Option<String>,
    position: Option<Position>,
    execution: Option<u64>,
}
impl EvidenceBoundary {
    pub(crate) fn new(component: &str, context: Option<&ToolContext>) -> Self {
        Self {
            component: component.into(),
            session: context.map(|c| digest(c.session_id.as_bytes())),
            tool: context.map(|c| digest(c.tool.as_bytes())),
            call: context.map(|c| digest(c.call_id.as_bytes())),
            position: context.map(|c| Position {
                epoch: c.state.snapshot().epoch_id,
                turn: c.turn_id,
                round: c.round_id,
            }),
            execution: context.and_then(|c| c.execution_id.map(|id| id.0)),
        }
    }
    pub(crate) fn failure(&self, error: wasmtime::Error, phase: Phase) -> Error {
        self.classify_failure(error, phase, None, None)
    }

    /// Call only at an engine execution boundary after the runtime has installed
    /// these allowances. Link/compile/budget setup errors use `failure` instead.
    pub(crate) fn execution_failure(
        &self,
        error: wasmtime::Error,
        phase: Phase,
        fuel_limit: u64,
        timeout_ms: u64,
    ) -> Error {
        self.classify_failure(error, phase, Some(fuel_limit), Some(timeout_ms))
    }

    fn classify_failure(
        &self,
        error: wasmtime::Error,
        phase: Phase,
        fuel_limit: Option<u64>,
        wall_timeout_ms: Option<u64>,
    ) -> Error {
        // IDs are process-scoped; they are neither durable global IDs nor handles.
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let evidence_id = NEXT.fetch_add(1, Ordering::Relaxed);
        let failure = match error.downcast_ref::<Stop>() {
            Some(Stop::Cancelled) => FailureClass::Cancelled,
            Some(Stop::Deadline) => FailureClass::Timeout,
            None => FailureClass::WasmTrap,
        };
        let trap = error.downcast_ref::<wasmtime::Trap>();
        let wasm_failure = match error.downcast_ref::<Stop>() {
            Some(Stop::Cancelled) => Some(WasmFailure::Cancelled),
            Some(Stop::Deadline) => Some(WasmFailure::WallDeadline { wall_timeout_ms }),
            None => match trap {
                Some(wasmtime::Trap::UnreachableCodeReached) => Some(WasmFailure::Unreachable),
                Some(wasmtime::Trap::OutOfFuel) => Some(WasmFailure::OutOfFuel { fuel_limit }),
                Some(wasmtime::Trap::StackOverflow) => Some(WasmFailure::StackOverflow),
                Some(wasmtime::Trap::MemoryOutOfBounds) => Some(WasmFailure::MemoryOutOfBounds),
                Some(wasmtime::Trap::TableOutOfBounds) => Some(WasmFailure::TableOutOfBounds),
                Some(wasmtime::Trap::IntegerDivisionByZero) => {
                    Some(WasmFailure::IntegerDivisionByZero)
                }
                Some(wasmtime::Trap::IntegerOverflow) => Some(WasmFailure::IntegerOverflow),
                Some(wasmtime::Trap::BadConversionToInteger) => {
                    Some(WasmFailure::BadConversionToInteger)
                }
                Some(wasmtime::Trap::IndirectCallToNull) => Some(WasmFailure::IndirectCallToNull),
                Some(wasmtime::Trap::BadSignature) => Some(WasmFailure::BadSignature),
                Some(wasmtime::Trap::Interrupt) => Some(WasmFailure::Interrupt),
                _ => None,
            },
        };
        let cause = wasm_failure.map_or(
            if trap.is_some() {
                "other_trap"
            } else {
                "wasmtime_error"
            },
            WasmFailure::label,
        );
        // Only module-local function indices and offsets are retained. Never ask
        // Wasmtime to format errors/frames: that can disclose names and source paths.
        let trace = error.downcast_ref::<wasmtime::WasmBacktrace>();
        let frames: Vec<(u32, Option<usize>, Option<usize>)> = trace
            .into_iter()
            .flat_map(|trace| trace.frames())
            .take(8)
            .map(|frame| {
                (
                    frame.func_index(),
                    frame.module_offset(),
                    frame.func_offset(),
                )
            })
            .collect();
        let frames_truncated = trace.is_some_and(|trace| trace.frames().len() > frames.len());
        macro_rules! emit {
            ($level:ident) => { tracing::$level!(target: "microcrab_session::runtime::wasm",
                evidence_id, observation = ObservationKind::WasmEvidence.as_str(),
                failure = failure.as_str(), phase = phase.label(), cause,
                component_sha256 = %self.component,
                session_sha256 = ?self.session, tool_sha256 = ?self.tool, call_sha256 = ?self.call,
                epoch = ?self.position.map(|p| p.epoch), execution_id = ?self.execution,
                turn = ?self.position.and_then(|p| p.turn), round = ?self.position.and_then(|p| p.round),
                wasm_frames = ?frames, frames_truncated,
                "WASM boundary failure"); };
        }
        if failure == FailureClass::Cancelled {
            emit!(debug);
        } else {
            emit!(error);
        }
        let (layer, public_phase) = match phase {
            Phase::Compile | Phase::Budget => {
                (DiagnosticLayer::Runtime, DiagnosticPhase::Validation)
            }
            Phase::Instantiate => (DiagnosticLayer::Runtime, DiagnosticPhase::Unknown),
            _ => (DiagnosticLayer::Guest, DiagnosticPhase::GuestExecution),
        };
        let mut diagnostic = ToolDiagnostic::new(DiagnosticCode::WasmTrap, layer, public_phase);
        diagnostic.evidence_id = Some(evidence_id);
        diagnostic.wasm_failure = wasm_failure;
        Error::Diagnostic(diagnostic)
    }
}

impl WasmFailure {
    fn label(self) -> &'static str {
        match self {
            Self::Unreachable => "unreachable",
            Self::OutOfFuel { .. } => "out_of_fuel",
            Self::StackOverflow => "stack_overflow",
            Self::MemoryOutOfBounds => "memory_out_of_bounds",
            Self::TableOutOfBounds => "table_out_of_bounds",
            Self::IntegerDivisionByZero => "integer_division_by_zero",
            Self::IntegerOverflow => "integer_overflow",
            Self::BadConversionToInteger => "bad_conversion_to_integer",
            Self::IndirectCallToNull => "indirect_call_to_null",
            Self::BadSignature => "bad_signature",
            Self::Interrupt => "interrupt",
            Self::Cancelled => "cancelled",
            Self::WallDeadline { .. } => "wall_deadline",
        }
    }
}

/// The only facts retained across calls on a poisoned slot. Nothing from the
/// native error, arguments, or earlier effect outcome is kept here.
#[derive(Clone, Copy)]
pub(crate) struct FailureEvidence {
    pub(crate) id: u64,
    pub(crate) cause: Option<WasmFailure>,
}
impl FailureEvidence {
    pub(crate) fn from_error(error: &Error) -> Option<Self> {
        match error {
            Error::Diagnostic(d) if d.code == DiagnosticCode::WasmTrap => Some(Self {
                id: d.evidence_id?,
                cause: d.wasm_failure,
            }),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_typed_allowlisted_causes_get_budget_facts() {
        let boundary = EvidenceBoundary::new("test-component", None);
        let native = wasmtime::format_err!("out_of_fuel at /private/source.wat");
        for error in [native, wasmtime::Trap::HeapMisaligned.into()] {
            let diagnostic = boundary
                .execution_failure(error, Phase::Invoke, 123, 456)
                .tool_diagnostic();
            assert_eq!(diagnostic.wasm_failure, None);
            let public = diagnostic.result_json();
            assert!(!public.contains("/private/source.wat"));
            assert!(!public.contains("fuel_limit"));
            assert!(!public.contains("wall_timeout_ms"));
        }
        let deadline = boundary
            .execution_failure(Stop::Deadline.into(), Phase::Invoke, 123, 456)
            .tool_diagnostic();
        assert_eq!(
            deadline.wasm_failure,
            Some(WasmFailure::WallDeadline {
                wall_timeout_ms: Some(456)
            })
        );
        assert!(!deadline.result_json().contains("fuel_limit"));
    }
}
