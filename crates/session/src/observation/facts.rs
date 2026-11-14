//! Shared closed, body-free vocabulary for owner-specific projections.
use crate::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObservationKind {
    Activity,
    TurnCompleted,
    InboxFailure,
    Audit,
    ToolDiagnostic,
    WasmEvidence,
}
impl ObservationKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Activity => "activity",
            Self::TurnCompleted => "turn_completed",
            Self::InboxFailure => "inbox_failure",
            Self::Audit => "audit",
            Self::ToolDiagnostic => "tool_diagnostic",
            Self::WasmEvidence => "wasm_evidence",
        }
    }
}

/// Numeric owner position only: never a request, path, tool argument, or body.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Position {
    pub epoch: u64,
    pub turn: Option<u64>,
    pub round: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureClass {
    ProviderStatus,
    Transport,
    Timeout,
    Provider,
    Protocol,
    Limit,
    Cancelled,
    InvalidArgument,
    PermissionDenied,
    Unsupported,
    Conflict,
    NotFound,
    Io,
    WasmTrap,
    WasmPoisoned,
    Tool,
    Other,
}
impl FailureClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProviderStatus => "provider_status",
            Self::Transport => "transport",
            Self::Timeout => "timeout",
            Self::Provider => "provider",
            Self::Protocol => "protocol",
            Self::Limit => "limit",
            Self::Cancelled => "cancelled",
            Self::InvalidArgument => "invalid_argument",
            Self::PermissionDenied => "permission_denied",
            Self::Unsupported => "unsupported",
            Self::Conflict => "conflict",
            Self::NotFound => "not_found",
            Self::Io => "io",
            Self::WasmTrap => "wasm_trap",
            Self::WasmPoisoned => "wasm_poisoned",
            Self::Tool => "tool",
            Self::Other => "other",
        }
    }
}

/// Borrowed typed facts. No formatting of native/provider/guest error strings.
/// Limit labels are deliberately unavailable; publication clones sanitized facts.
pub(crate) struct FailureFacts<'a> {
    pub class: FailureClass,
    pub provider_status: Option<u16>,
    pub idle_timeout: Option<&'a crate::ProviderTimeoutDetails>,
    limit: Option<&'a crate::LimitError>,
}
impl<'a> FailureFacts<'a> {
    pub(crate) fn from_error(error: &'a Error) -> Self {
        use FailureClass as C;
        let class = match error {
            Error::ProviderStatus { .. } => C::ProviderStatus,
            Error::Transport | Error::TransportFailure(_) => C::Transport,
            Error::ProviderTimeout | Error::ProviderIdleTimeout(_) => C::Timeout,
            Error::Provider(_) => C::Provider,
            Error::Protocol(_) => C::Protocol,
            Error::Limit(_) => C::Limit,
            Error::Cancelled => C::Cancelled,
            Error::Denied(_) | Error::VariableForbidden(_) => C::PermissionDenied,
            Error::Json(_) => C::InvalidArgument,
            Error::Wasm(_) => C::WasmTrap,
            Error::Diagnostic(d) => d.failure_class(),
            Error::ToolFailure(f) => match f.kind {
                crate::runtime::execution::ToolFailureKind::Failed => C::Tool,
                crate::runtime::execution::ToolFailureKind::Timeout => C::Timeout,
                crate::runtime::execution::ToolFailureKind::Cancelled => C::Cancelled,
            },
            Error::Io(e) => match e.kind() {
                std::io::ErrorKind::NotFound => C::NotFound,
                std::io::ErrorKind::PermissionDenied => C::PermissionDenied,
                std::io::ErrorKind::TimedOut => C::Timeout,
                _ => C::Io,
            },
            Error::Tool(_) => C::Tool,
            _ => C::Other,
        };
        Self {
            class,
            provider_status: match error {
                Error::ProviderStatus { status } => Some(*status),
                _ => None,
            },
            idle_timeout: match error {
                Error::ProviderIdleTimeout(d) => Some(d),
                _ => None,
            },
            limit: match error {
                Error::Limit(limit) => Some(limit),
                _ => None,
            },
        }
    }
    pub(crate) fn sanitized_limit(&self) -> Option<crate::LimitError> {
        self.limit.map(crate::LimitError::sanitized)
    }
}

/// Shared metadata for leaf adapters. Each leaf keeps its own payload/privacy port.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Observation {
    pub kind: ObservationKind,
    pub position: Position,
    pub failure: Option<FailureClass>,
}
impl Observation {
    pub(crate) fn trace(self) {
        tracing::trace!(
            observation = self.kind.as_str(), epoch = self.position.epoch,
            turn = ?self.position.turn, round = ?self.position.round,
            failure = ?self.failure.map(FailureClass::as_str),
            "Session observation projected"
        );
    }
}
