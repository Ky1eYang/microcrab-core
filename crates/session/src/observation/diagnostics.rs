//! Bounded public tool failure facts. Never serialize native error text here.
use super::{FailureClass, FailureFacts, Observation, ObservationKind, Position};
use serde::{Deserialize, Serialize};

/// Project only typed owner facts; never inspect or format a native error body.
/// Existing Error::tool_diagnostic delegates here through its compatibility API.
pub(crate) fn project_error(error: &crate::Error) -> ToolDiagnostic {
    use crate::Error;
    use DiagnosticCode as C;
    use DiagnosticLayer as L;
    use DiagnosticPhase as P;
    let facts = FailureFacts::from_error(error);
    let (code, layer, phase) = match error {
        Error::Diagnostic(diagnostic) => {
            let mut diagnostic = diagnostic.clone();
            diagnostic.reason = diagnostic
                .reason
                .as_ref()
                .map(|reason| SafeDiagnosticReason::new(reason.as_str()));
            return diagnostic;
        }
        Error::VariableConflict | Error::VariableForbidden(_) => {
            let forbidden = matches!(error, Error::VariableForbidden(_));
            let mut diagnostic = ToolDiagnostic::new(
                if forbidden {
                    C::PermissionDenied
                } else {
                    C::Conflict
                },
                L::Runtime,
                if forbidden {
                    P::Authorization
                } else {
                    P::Validation
                },
            );
            diagnostic.effects = EffectKnowledge::NotStarted;
            diagnostic.retryable = Some(false);
            diagnostic.reason = Some(SafeDiagnosticReason::new(&error.diagnostic_reason()));
            return diagnostic;
        }
        Error::StateObservationConflict => {
            let mut diagnostic = ToolDiagnostic::new(C::Conflict, L::Runtime, P::Validation);
            diagnostic.effects = EffectKnowledge::NotStarted;
            diagnostic.retryable = Some(true);
            diagnostic.reason = error.public_reason().map(SafeDiagnosticReason::new);
            return diagnostic;
        }
        Error::ToolFailure(_) | Error::Cancelled | Error::Io(_) | Error::Json(_) => (
            match facts.class {
                FailureClass::Cancelled => C::Cancelled,
                FailureClass::Timeout => C::Timeout,
                FailureClass::InvalidArgument => C::InvalidArgument,
                FailureClass::NotFound => C::NotFound,
                FailureClass::PermissionDenied => C::PermissionDenied,
                FailureClass::Io => C::Io,
                _ => C::ToolFailed,
            },
            L::Runtime,
            if matches!(error, Error::Json(_)) {
                P::Validation
            } else {
                P::Unknown
            },
        ),
        Error::Denied(_) => (C::PermissionDenied, L::Runtime, P::Authorization),
        Error::Limit(_) => (C::OutputLimit, L::Runtime, P::OutputValidation),
        Error::Wasm(_) => (C::WasmTrap, L::Guest, P::GuestExecution),
        _ => (C::ToolFailed, L::Runtime, P::Unknown),
    };
    let mut diagnostic = ToolDiagnostic::new(code, layer, phase);
    diagnostic.reason = error.public_reason().map(SafeDiagnosticReason::new);
    if let Error::Json(source) = error {
        diagnostic.reason = Some(SafeDiagnosticReason::new(
            &microcrab_protocol::diagnostic::json_reason(source),
        ));
        diagnostic.json_line = Some(source.line() as u64);
        diagnostic.json_column = Some(source.column() as u64);
    }
    if diagnostic.reason.is_none() {
        diagnostic.reason = Some(SafeDiagnosticReason::new(&match error {
            Error::Io(source) => format!(
                "{}; kind={:?}; os_error={:?}",
                source,
                source.kind(),
                source.raw_os_error()
            ),
            Error::ToolFailure(failure) => format!("{}: {}", failure.code, failure.message),
            _ => error.diagnostic_reason(),
        }));
    }
    diagnostic
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticCode {
    InvalidArgument,
    PathOutsideRoot,
    Timeout,
    Cancelled,
    PermissionDenied,
    Unsupported,
    Conflict,
    OutputLimit,
    NotFound,
    Io,
    WasmTrap,
    WasmPoisoned,
    ToolFailed,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticLayer {
    Runtime,
    Host,
    Guest,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticPhase {
    Unknown,
    Validation,
    Authorization,
    HostOperation,
    GuestExecution,
    OutputValidation,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectKnowledge {
    Unknown,
    NotStarted,
    MayHaveOccurred,
    Committed,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticOperation {
    Shell,
    Read,
    Write,
    Glob,
    Grep,
    CacheCapture,
    Resource,
    Job,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LimitOwner {
    RuntimeHostOperation,
    HostShell,
    ExecutionBuffers,
}

/// Allowlisted engine failures and typed runtime stops. A limit is the configured
/// allowance for the failed operation, not measured fuel used or elapsed time.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "cause", rename_all = "snake_case", deny_unknown_fields)]
pub enum WasmFailure {
    Unreachable,
    OutOfFuel {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        fuel_limit: Option<u64>,
    },
    StackOverflow,
    MemoryOutOfBounds,
    TableOutOfBounds,
    IntegerDivisionByZero,
    IntegerOverflow,
    BadConversionToInteger,
    IndirectCallToNull,
    BadSignature,
    Interrupt,
    Cancelled,
    WallDeadline {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        wall_timeout_ms: Option<u64>,
    },
}

/// Only producer-authored static text enters live diagnostics. Deserialization
/// accepts bounded historical/ABI facts without granting native strings this port.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct SafeDiagnosticReason(String);
impl SafeDiagnosticReason {
    pub const MAX_BYTES: usize = 2048;
    pub fn new(reason: &str) -> Self {
        Self(microcrab_protocol::diagnostic::sanitize(
            reason,
            Self::MAX_BYTES,
        ))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl<'de> Deserialize<'de> for SafeDiagnosticReason {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let reason = String::deserialize(deserializer)?;
        if reason.len() > Self::MAX_BYTES {
            return Err(serde::de::Error::custom(
                "diagnostic reason exceeds 2048 bytes",
            ));
        }
        Ok(Self(reason))
    }
}

/// Effects describe the whole invocation. operation_effects is narrower: rejecting
/// a later Host operation never proves earlier guest imports had no effects.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolDiagnostic {
    pub code: DiagnosticCode,
    pub layer: DiagnosticLayer,
    pub phase: DiagnosticPhase,
    pub effects: EffectKnowledge,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<SafeDiagnosticReason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub json_line: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub json_column: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_effects: Option<EffectKnowledge>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<DiagnosticOperation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit_owner: Option<LimitOwner>,
    /// Correlates safe WASM failure evidence within one embedding process run.
    /// Absent on historical records and failures without captured boundary evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_id: Option<u64>,
    /// Explicit typed Host retryability; absent means the owner supplied no fact.
    /// This is descriptive metadata, never authorization to replay an operation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retryable: Option<bool>,
    /// Safe, allowlisted Wasmtime/Session cause; raw native error text is never retained.
    /// On WasmPoisoned this describes the earlier failure, not the skipped call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wasm_failure: Option<WasmFailure>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_source: Option<PermissionDecisionSource>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionDecisionSource {
    ActionReview,
    User,
    Timeout,
    Unavailable,
    Interaction,
}

impl ToolDiagnostic {
    /// Common classification is a projection, not another serialized DTO field.
    pub fn failure_class(&self) -> FailureClass {
        match self.code {
            DiagnosticCode::InvalidArgument | DiagnosticCode::PathOutsideRoot => {
                FailureClass::InvalidArgument
            }
            DiagnosticCode::Timeout => FailureClass::Timeout,
            DiagnosticCode::Cancelled => FailureClass::Cancelled,
            DiagnosticCode::PermissionDenied => FailureClass::PermissionDenied,
            DiagnosticCode::Unsupported => FailureClass::Unsupported,
            DiagnosticCode::Conflict => FailureClass::Conflict,
            DiagnosticCode::OutputLimit => FailureClass::Limit,
            DiagnosticCode::NotFound => FailureClass::NotFound,
            DiagnosticCode::Io => FailureClass::Io,
            DiagnosticCode::WasmTrap => FailureClass::WasmTrap,
            DiagnosticCode::WasmPoisoned => FailureClass::WasmPoisoned,
            DiagnosticCode::ToolFailed => FailureClass::Tool,
        }
    }
    pub fn new(code: DiagnosticCode, layer: DiagnosticLayer, phase: DiagnosticPhase) -> Self {
        Self {
            code,
            layer,
            phase,
            effects: EffectKnowledge::Unknown,
            reason: None,
            json_line: None,
            json_column: None,
            operation_effects: None,
            operation: None,
            requested_timeout_ms: None,
            effective_timeout_ms: None,
            limit_owner: None,
            evidence_id: None,
            retryable: None,
            wasm_failure: None,
            decision_source: None,
        }
    }
    pub fn summary(&self) -> &str {
        if let Some(reason) = &self.reason {
            return reason.as_str();
        }
        match self.code {
            DiagnosticCode::InvalidArgument if self.limit_owner == Some(LimitOwner::HostShell) => {
                "requested shell timeout is outside the Host limit; this Host operation was not started"
            }
            DiagnosticCode::InvalidArgument if self.operation == Some(DiagnosticOperation::Job) => {
                "invalid job handle; use session_id/execution_id, or 1–16 exact handles as a JSON array or JSON-encoded array string; nested arrays and non-string members are invalid"
            }
            DiagnosticCode::InvalidArgument => "invalid tool or Host argument",
            DiagnosticCode::Timeout => {
                "execution deadline exceeded; inspect the effective timeout and limit owner before retrying"
            }
            DiagnosticCode::Cancelled => "operation cancelled; effects are not ruled out",
            DiagnosticCode::PathOutsideRoot => {
                "working directory or target path is outside the configured workspace root; use relative paths or paths inside the workspace root"
            }
            DiagnosticCode::PermissionDenied => "permission denied",
            DiagnosticCode::Unsupported => "Host operation is unsupported",
            DiagnosticCode::Conflict => "resource version or execution state conflict",
            DiagnosticCode::OutputLimit
                if self.limit_owner == Some(LimitOwner::ExecutionBuffers) =>
            {
                "execution buffer capacity (max_execution_buffers) exhausted; inspect state.executions and use job with id=session_id/execution_id. Collect needed output with read. Reading never frees a buffer slot. Use job.release explicitly on terminal executions after needed output is collected; release discards retained output after cleanup succeeds. Shortening command output does not free buffer slots; earlier invocation effects are not ruled out"
            }
            DiagnosticCode::OutputLimit => {
                "output or resource budget exceeded; execution may already have occurred"
            }
            DiagnosticCode::NotFound => "requested resource was not found",
            DiagnosticCode::Io => "Host I/O operation failed",
            DiagnosticCode::WasmTrap if self.wasm_failure.is_some() => {
                "WASM execution stopped for the cause in wasm_failure. After a recoverable failure, a new admitted call can use a fresh instance once old resource cleanup is confirmed. The failed call is not replayed; inspect existing jobs and external state before repeating it."
            }
            DiagnosticCode::WasmTrap => {
                "WASM guest execution trapped or failed; the original cause is unavailable in this diagnostic. After a recoverable failure, a new admitted call can use a fresh instance once cleanup is confirmed. The failed call is not replayed; its effects may be unknown."
            }
            DiagnosticCode::WasmPoisoned => {
                "The previous WASM instance is unavailable and this call did not enter the guest. Any wasm_failure describes the earlier call, whose effects may be unknown. If recovery is waiting for cleanup, inspect the existing jobs and external state. Do not replay the earlier operation automatically."
            }
            DiagnosticCode::ToolFailed => {
                "tool failed; the producer supplied no safe structured cause"
            }
        }
    }
    /// Conservative JSON bound for every public diagnostic. Enum strings are
    /// selected independently at their largest encoded width, optional fields are
    /// all present, and both timeout counters use their widest u64 representation.
    pub(crate) fn max_result_bytes() -> usize {
        Self::result_byte_bounds().0
    }
    pub(crate) fn max_context_result_bytes() -> usize {
        Self::result_byte_bounds().1
    }
    fn result_byte_bounds() -> (usize, usize) {
        static MAX: std::sync::OnceLock<(usize, usize)> = std::sync::OnceLock::new();
        *MAX.get_or_init(|| {
            use DiagnosticCode as C;
            use DiagnosticLayer as L;
            use DiagnosticOperation as O;
            use DiagnosticPhase as P;
            use EffectKnowledge as E;
            use LimitOwner as W;
            let codes = [C::InvalidArgument, C::PathOutsideRoot, C::Timeout, C::Cancelled, C::PermissionDenied,
                C::Unsupported, C::Conflict, C::OutputLimit, C::NotFound, C::Io,
                C::WasmTrap, C::WasmPoisoned, C::ToolFailed];
            let owners = [W::RuntimeHostOperation, W::HostShell, W::ExecutionBuffers];
            fn widest<T: Serialize>(values: &[T]) -> serde_json::Value {
                values.iter().map(|value| serde_json::json!(value))
                    .max_by_key(|value| value.to_string().len())
                    .expect("closed diagnostic enum has at least one variant")
            }
            let summary = codes.iter().flat_map(|code| owners.iter().map(move |owner| {
                let mut diagnostic = Self::new(*code, L::Runtime, P::Unknown);
                diagnostic.limit_owner = Some(*owner);
                diagnostic.summary().to_owned()
            })).max_by_key(|summary| serde_json::json!(summary).to_string().len())
                .expect("closed diagnostic summaries are nonempty");
            // A control byte takes six JSON bytes, then its escape backslash is
            // escaped again when the tool result becomes Context text. Reserve
            // both occurrences (summary and reason) at that worst-case width.
            let reason = "\u{1}".repeat(SafeDiagnosticReason::MAX_BYTES);
            let summary = if serde_json::json!(&reason).to_string().len() > serde_json::json!(&summary).to_string().len() { reason.clone() } else { summary };
            let envelope = serde_json::json!({"error":summary,"diagnostic":{
                "code":widest(&codes), "layer":widest(&[L::Runtime,L::Host,L::Guest]),
                "phase":widest(&[P::Unknown,P::Validation,P::Authorization,P::HostOperation,P::GuestExecution,P::OutputValidation]),
                "effects":widest(&[E::Unknown,E::NotStarted,E::MayHaveOccurred,E::Committed]),
                "operation_effects":widest(&[E::Unknown,E::NotStarted,E::MayHaveOccurred,E::Committed]),
                "operation":widest(&[O::Shell,O::Read,O::Write,O::Glob,O::Grep,O::CacheCapture,O::Resource,O::Job]),
                "requested_timeout_ms":u64::MAX, "effective_timeout_ms":u64::MAX,
                "limit_owner":widest(&owners), "evidence_id":u64::MAX, "retryable":false,
                "reason":reason, "json_line":u64::MAX, "json_column":u64::MAX,
                "decision_source":widest(&[PermissionDecisionSource::ActionReview, PermissionDecisionSource::User,
                    PermissionDecisionSource::Timeout, PermissionDecisionSource::Unavailable, PermissionDecisionSource::Interaction]),
                // Reserve the widest cause name and widest budget field independently.
                "wasm_failure":{
                    "cause":serde_json::json!(WasmFailure::BadConversionToInteger)["cause"],
                    "wall_timeout_ms":u64::MAX
                }
            }}).to_string();
            let context_bytes = serde_json::json!(&envelope).to_string().len() - 2;
            (envelope.len(), context_bytes)
        })
    }
    pub fn result_json(&self) -> String {
        Observation {
            kind: ObservationKind::ToolDiagnostic,
            position: Position::default(),
            failure: Some(self.failure_class()),
        }
        .trace();
        serde_json::json!({"error":self.summary(),"diagnostic":self}).to_string()
    }
}
impl std::fmt::Display for ToolDiagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.summary())
    }
}
impl std::error::Error for ToolDiagnostic {}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_reasons_reserve_both_json_escape_layers_and_keep_old_bytes() {
        let legacy =
            r#"{"code":"tool_failed","layer":"runtime","phase":"unknown","effects":"unknown"}"#;
        let old: ToolDiagnostic = serde_json::from_str(legacy).unwrap();
        assert_eq!(serde_json::to_string(&old).unwrap(), legacy);
        let mut diagnostic = old;
        for reason in [
            "\u{1}".repeat(SafeDiagnosticReason::MAX_BYTES),
            "\"".repeat(SafeDiagnosticReason::MAX_BYTES),
            "é".repeat(SafeDiagnosticReason::MAX_BYTES / 2),
        ] {
            diagnostic.reason = Some(serde_json::from_value(serde_json::json!(reason)).unwrap());
            diagnostic.json_line = Some(u64::MAX);
            diagnostic.json_column = Some(u64::MAX);
            assert_eq!(diagnostic.summary(), reason);
            let result = diagnostic.result_json();
            assert!(result.len() <= ToolDiagnostic::max_result_bytes());
            assert!(
                serde_json::json!(result).to_string().len() - 2
                    <= ToolDiagnostic::max_context_result_bytes()
            );
        }
        assert!(
            serde_json::from_value::<SafeDiagnosticReason>(serde_json::json!("x".repeat(2049)))
                .is_err()
        );
        assert!(
            serde_json::from_value::<SafeDiagnosticReason>(serde_json::json!("é".repeat(1025)))
                .is_err()
        );
    }

    #[test]
    fn json_category_and_position_are_safe_without_native_display() {
        for source in ["{\"private-secret\":", "{\"private-secret\" 1}"] {
            let error =
                crate::Error::Json(serde_json::from_str::<serde_json::Value>(source).unwrap_err());
            let diagnostic = error.tool_diagnostic();
            assert!(diagnostic.reason.is_some());
            assert!(diagnostic.json_line.is_some());
            assert!(diagnostic.json_column.is_some());
            assert_eq!(diagnostic.phase, DiagnosticPhase::Validation);
            assert!(!diagnostic.result_json().contains("private-secret"));
        }
        let source = serde_json::from_str::<u64>("\"private-secret\"").unwrap_err();
        let diagnostic = crate::Error::Json(source).tool_diagnostic();
        assert!(diagnostic.summary().contains("invalid type"));
        assert!(!diagnostic.result_json().contains("private-secret"));
    }

    #[test]
    fn diagnostic_result_reservation_covers_all_summary_branches_and_optional_fields() {
        use DiagnosticCode as C;
        let codes = [
            C::InvalidArgument,
            C::PathOutsideRoot,
            C::Timeout,
            C::Cancelled,
            C::PermissionDenied,
            C::Unsupported,
            C::Conflict,
            C::OutputLimit,
            C::NotFound,
            C::Io,
            C::WasmTrap,
            C::WasmPoisoned,
            C::ToolFailed,
        ];
        for code in codes {
            for owner in [
                None,
                Some(LimitOwner::RuntimeHostOperation),
                Some(LimitOwner::HostShell),
                Some(LimitOwner::ExecutionBuffers),
            ] {
                let mut diagnostic = ToolDiagnostic::new(
                    code,
                    DiagnosticLayer::Runtime,
                    DiagnosticPhase::OutputValidation,
                );
                diagnostic.effects = EffectKnowledge::MayHaveOccurred;
                diagnostic.operation_effects = Some(EffectKnowledge::MayHaveOccurred);
                diagnostic.operation = Some(DiagnosticOperation::CacheCapture);
                diagnostic.requested_timeout_ms = Some(u64::MAX);
                diagnostic.effective_timeout_ms = Some(u64::MAX);
                diagnostic.limit_owner = owner;
                diagnostic.evidence_id = Some(u64::MAX);
                diagnostic.retryable = Some(false);
                for operation in [DiagnosticOperation::CacheCapture, DiagnosticOperation::Job] {
                    diagnostic.operation = Some(operation);
                    for wasm_failure in [
                        None,
                        Some(WasmFailure::BadConversionToInteger),
                        Some(WasmFailure::OutOfFuel {
                            fuel_limit: Some(u64::MAX),
                        }),
                        Some(WasmFailure::WallDeadline {
                            wall_timeout_ms: Some(u64::MAX),
                        }),
                    ] {
                        diagnostic.wasm_failure = wasm_failure;
                        let result = diagnostic.result_json();
                        assert!(result.len() <= ToolDiagnostic::max_result_bytes());
                        assert!(
                            serde_json::json!(result).to_string().len() - 2
                                <= ToolDiagnostic::max_context_result_bytes()
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn typed_host_retryability_survives_redaction_without_inventing_other_facts() {
        use microcrab_host_api::{ErrorCategory as H, HostError, ToolError};
        for (category, code) in [
            (H::NotFound, DiagnosticCode::NotFound),
            (H::Permission, DiagnosticCode::PermissionDenied),
            (H::Timeout, DiagnosticCode::Timeout),
            (H::Cancelled, DiagnosticCode::Cancelled),
            (H::Conflict, DiagnosticCode::Conflict),
            (H::Unsupported, DiagnosticCode::Unsupported),
            (H::Io, DiagnosticCode::Io),
        ] {
            for retryable in [false, true] {
                let mut source = HostError::new(category, "token=private-host-secret");
                source.retryable = retryable;
                let diagnostic =
                    crate::Error::from_host_error(ToolError::Host(source)).tool_diagnostic();
                assert_eq!(diagnostic.code, code);
                assert_eq!(diagnostic.retryable, Some(retryable));
                assert_eq!(diagnostic.effects, EffectKnowledge::Unknown);
                let value = serde_json::to_value(&diagnostic).unwrap();
                assert_eq!(value["retryable"], retryable);
                assert!(!value.to_string().contains("private-host-secret"));
                assert_eq!(
                    serde_json::from_value::<ToolDiagnostic>(value).unwrap(),
                    diagnostic
                );
            }
        }
        let historical = serde_json::json!({
            "code":"io", "layer":"host", "phase":"host_operation", "effects":"unknown"
        });
        let untyped = crate::Error::from_host_error(ToolError::Io(std::io::Error::other(
            "token=private-host-secret",
        )))
        .tool_diagnostic();
        assert_eq!(untyped.retryable, None);
        assert!(untyped.reason.is_some());
        assert!(!untyped.summary().contains("private-host-secret"));
        let restored: ToolDiagnostic = serde_json::from_value(historical.clone()).unwrap();
        assert_eq!(restored.retryable, None);
        assert_eq!(serde_json::to_value(restored).unwrap(), historical);
    }

    #[test]
    fn host_boundary_mapping_is_validation_and_real_permission_denials_stay_permissions() {
        use microcrab_host_api::{ErrorCategory, HostError, ToolError};
        for source in [ToolError::PathOutsideRoot, ToolError::CwdOutsideRoot] {
            let diagnostic = crate::Error::from_host_error(source).tool_diagnostic();
            assert_eq!(diagnostic.code, DiagnosticCode::PathOutsideRoot);
            assert_eq!(diagnostic.failure_class(), FailureClass::InvalidArgument);
            assert_eq!(diagnostic.layer, DiagnosticLayer::Host);
            assert_eq!(diagnostic.phase, DiagnosticPhase::Validation);
            assert_eq!(diagnostic.effects, EffectKnowledge::Unknown);
            assert_eq!(
                diagnostic.operation_effects,
                Some(EffectKnowledge::NotStarted)
            );
            assert!(matches!(
                diagnostic.summary(),
                "file path is outside the configured root"
                    | "shell working directory is outside the configured root"
            ));
            assert_eq!(
                serde_json::to_value(&diagnostic).unwrap()["code"],
                "path_outside_root"
            );
        }
        for source in [
            ToolError::Host(HostError::new(
                ErrorCategory::Permission,
                "secret=/private/path",
            )),
            ToolError::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "secret=/private/path",
            )),
        ] {
            let diagnostic = crate::Error::from_host_error(source).tool_diagnostic();
            assert_eq!(diagnostic.code, DiagnosticCode::PermissionDenied);
            assert_eq!(diagnostic.failure_class(), FailureClass::PermissionDenied);
            assert_eq!(diagnostic.phase, DiagnosticPhase::HostOperation);
            assert!(!diagnostic.result_json().contains("/private/path"));
        }
        // Historical records remain byte-for-byte canonical with their original classification.
        let historical = r#"{"code":"permission_denied","layer":"runtime","phase":"authorization","effects":"unknown"}"#;
        let restored: ToolDiagnostic = serde_json::from_str(historical).unwrap();
        assert_eq!(serde_json::to_string(&restored).unwrap(), historical);
        assert_eq!(restored.code, DiagnosticCode::PermissionDenied);
        assert_eq!(restored.summary(), "permission denied");
    }

    #[test]
    fn fallback_preserves_dynamic_native_and_guest_causes_without_credentials() {
        let error = crate::Error::Tool(
            "native cache unavailable; password=secret /private/native/path".into(),
        );
        let projection = error.tool_diagnostic().result_json();
        assert!(projection.contains("native cache unavailable"));
        assert!(!projection.contains("secret"));
        assert!(!projection.contains("/private"));
        assert_eq!(error.tool_diagnostic().effects, EffectKnowledge::Unknown);
        let host_error = crate::Error::from_host_error(microcrab_host_api::ToolError::Worker(
            "token=secret /private/host".into(),
        ));
        assert!(
            !host_error
                .tool_diagnostic()
                .result_json()
                .contains("secret")
        );
        assert_eq!(host_error.tool_diagnostic().code, DiagnosticCode::Io);
        for (message, expected) in [
            (
                "guest parser failed; password=secret /private/guest/path",
                "guest parser failed",
            ),
            (
                "WASM instance is poisoned until the next epoch",
                "WASM instance is poisoned until the next epoch",
            ),
        ] {
            let wasm_error = crate::Error::Wasm(message.into());
            let diagnostic = wasm_error.tool_diagnostic();
            assert_eq!(diagnostic.code, DiagnosticCode::WasmTrap);
            assert_eq!(diagnostic.effects, EffectKnowledge::Unknown);
            let projection = diagnostic.result_json();
            assert!(projection.contains(expected));
            assert!(!projection.contains("secret"));
            assert!(!projection.contains("/private"));
        }
    }
    #[test]
    fn historical_wasm_trap_diagnostic_remains_readable_without_inventing_a_cause() {
        let historical = serde_json::json!({
            "code": "wasm_trap", "layer": "guest", "phase": "guest_execution", "effects": "unknown"
        });
        let diagnostic: ToolDiagnostic = serde_json::from_value(historical.clone()).unwrap();
        assert_eq!(serde_json::to_value(&diagnostic).unwrap(), historical);
        assert!(
            diagnostic
                .summary()
                .contains("original cause is unavailable")
        );
        assert!(diagnostic.summary().contains("effects may be unknown"));
        assert!(diagnostic.summary().contains("once cleanup is confirmed"));
        assert!(diagnostic.summary().contains("not replayed"));
    }
    #[test]
    fn buffer_capacity_has_safe_recovery_guidance_without_changing_old_diagnostics() {
        let mut diagnostic = ToolDiagnostic::new(
            DiagnosticCode::OutputLimit,
            DiagnosticLayer::Runtime,
            DiagnosticPhase::OutputValidation,
        );
        let historical = serde_json::to_value(&diagnostic).unwrap();
        assert!(historical.get("limit_owner").is_none());
        let previous_summary = diagnostic.summary().to_owned();
        diagnostic.limit_owner = Some(LimitOwner::ExecutionBuffers);
        let encoded = diagnostic.result_json();
        let decoded: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded["diagnostic"]["limit_owner"], "execution_buffers");
        assert_eq!(decoded["diagnostic"]["effects"], "unknown");
        assert!(
            decoded["error"]
                .as_str()
                .unwrap()
                .contains("max_execution_buffers")
        );
        let advice = decoded["error"].as_str().unwrap();
        assert!(advice.contains("Collect needed output with read"));
        assert!(advice.contains("Reading never frees a buffer slot"));
        assert!(advice.contains("job.release explicitly on terminal executions"));
        assert!(advice.contains("release discards retained output after cleanup succeeds"));
        let restored: ToolDiagnostic = serde_json::from_value(historical.clone()).unwrap();
        assert_eq!(restored.summary(), previous_summary);
        assert_eq!(serde_json::to_value(restored).unwrap(), historical);
    }

    #[test]
    fn operation_rejection_is_not_whole_invocation_no_effects() {
        let mut d = ToolDiagnostic::new(
            DiagnosticCode::InvalidArgument,
            DiagnosticLayer::Host,
            DiagnosticPhase::Validation,
        );
        d.operation_effects = Some(EffectKnowledge::NotStarted);
        d.requested_timeout_ms = Some(120_000);
        d.effective_timeout_ms = Some(30_000);
        d.limit_owner = Some(LimitOwner::HostShell);
        let roundtrip: ToolDiagnostic =
            serde_json::from_value(serde_json::to_value(&d).unwrap()).unwrap();
        assert_eq!(roundtrip, d);
        assert_eq!(roundtrip.effects, EffectKnowledge::Unknown);
        assert!(roundtrip.result_json().contains("120000"));
    }
}
