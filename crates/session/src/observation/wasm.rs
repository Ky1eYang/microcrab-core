//! Safe boundary evidence. Correlation facts are observations, never authorization.
use super::{FailureClass, ObservationKind, Position};
use crate::runtime::tool::ToolContext;
use crate::{
    Error,
    observation::diagnostics::{DiagnosticCode, DiagnosticLayer, DiagnosticPhase, ToolDiagnostic},
};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy)]
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
    fn label(self) -> &'static str {
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
        // IDs are process-scoped; they are neither durable global IDs nor handles.
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let evidence_id = NEXT.fetch_add(1, Ordering::Relaxed);
        let failure = match error.downcast_ref::<Stop>() {
            Some(Stop::Cancelled) => FailureClass::Cancelled,
            Some(Stop::Deadline) => FailureClass::Timeout,
            None => FailureClass::WasmTrap,
        };
        let cause = match error.downcast_ref::<Stop>() {
            Some(Stop::Cancelled) => "cancelled",
            Some(Stop::Deadline) => "wall_deadline",
            None => match error.downcast_ref::<wasmtime::Trap>() {
                Some(wasmtime::Trap::UnreachableCodeReached) => "unreachable",
                Some(wasmtime::Trap::OutOfFuel) => "out_of_fuel",
                Some(wasmtime::Trap::StackOverflow) => "stack_overflow",
                Some(wasmtime::Trap::MemoryOutOfBounds) => "memory_out_of_bounds",
                Some(wasmtime::Trap::TableOutOfBounds) => "table_out_of_bounds",
                Some(wasmtime::Trap::IntegerDivisionByZero) => "integer_division_by_zero",
                Some(wasmtime::Trap::IntegerOverflow) => "integer_overflow",
                Some(wasmtime::Trap::BadConversionToInteger) => "bad_conversion_to_integer",
                Some(wasmtime::Trap::IndirectCallToNull) => "indirect_call_to_null",
                Some(wasmtime::Trap::BadSignature) => "bad_signature",
                Some(wasmtime::Trap::Interrupt) => "interrupt",
                Some(_) => "other_trap",
                None => "wasmtime_error",
            },
        };
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
        Error::Diagnostic(diagnostic)
    }
}

pub(crate) fn failure_id(error: &Error) -> Option<u64> {
    match error {
        Error::Diagnostic(d) if d.code == DiagnosticCode::WasmTrap => d.evidence_id,
        _ => None,
    }
}
