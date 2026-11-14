//! Explicit failures across session boundaries.
/// Producer-authored public cause beside a private legacy display message.
/// Opaque conversions never authorize publication of native or guest text.
#[derive(Clone, PartialEq, Eq)]
pub struct ErrorReason {
    private: String,
    public: Option<String>,
}
impl ErrorReason {
    pub fn known(reason: &'static str) -> Self {
        Self::with_private(reason, reason)
    }
    pub fn with_private(reason: &str, private: impl Into<String>) -> Self {
        Self {
            private: private.into(),
            public: Some(microcrab_protocol::diagnostic::sanitize(reason, 2048)),
        }
    }
    pub fn public_reason(&self) -> Option<&str> {
        self.public.as_deref()
    }
    pub fn as_str(&self) -> &str {
        &self.private
    }
    pub fn contains(&self, pattern: &str) -> bool {
        self.private.contains(pattern)
    }
}
impl From<String> for ErrorReason {
    fn from(private: String) -> Self {
        Self {
            public: Some(microcrab_protocol::diagnostic::sanitize(&private, 2048)),
            private,
        }
    }
}
impl From<&str> for ErrorReason {
    fn from(private: &str) -> Self {
        private.to_owned().into()
    }
}
impl std::fmt::Debug for ErrorReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErrorReason")
            .field("public", &self.public)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Display for ErrorReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.private)
    }
}

/// Numeric facts from the transport's renewable model inactivity window.
/// No request, response, credential or provider body is retained here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderTimeoutDetails {
    pub timeout_ms: u64,
    pub elapsed_ms: u64,
    pub first_token_ms: Option<u64>,
    pub streamed_bytes: u64,
    pub streamed_chars: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    PendingInput(#[from] crate::PendingInputError),
    #[error("invalid configuration: {0}")]
    Config(ErrorReason),
    #[error("session is busy")]
    Busy,
    #[error(
        "current State changed during observation; retry after the permission or mode update settles"
    )]
    StateObservationConflict,
    #[error("variable inspection is stale; inspect current variables and explicitly save again")]
    VariableConflict,
    #[error("variable change forbidden: {0}")]
    VariableForbidden(ErrorReason),
    #[error("session is closed")]
    Closed,
    #[error("operation cancelled")]
    Cancelled,
    #[error("execution interrupted; cleanup outcome is unknown")]
    ExecutionInterrupted,
    #[error("execution limit exceeded: {0}")]
    Limit(crate::LimitError),
    #[error("provider HTTP status {status}")]
    ProviderStatus { status: u16 },
    #[error("provider transport failed")]
    Transport,
    #[error("provider transport failed: {0}")]
    TransportFailure(ErrorReason),
    #[error("provider request timed out")]
    ProviderTimeout,
    #[error("provider response became inactive")]
    ProviderIdleTimeout(ProviderTimeoutDetails),
    #[error("provider error: {0}")]
    Provider(microcrab_transport::ProviderError),
    #[error("invalid model response: {0}")]
    Protocol(microcrab_transport::ProtocolError),
    #[error("{0}")]
    Diagnostic(crate::observation::diagnostics::ToolDiagnostic),
    #[error("{0}")]
    ToolFailure(crate::runtime::execution::ToolFailure),
    #[error("tool error: {0}")]
    Tool(ErrorReason),
    #[error("permission denied: {0}")]
    Denied(ErrorReason),
    #[error("lifecycle failed: {0}")]
    Lifecycle(ErrorReason),
    #[error("audit failed: {0}")]
    Audit(ErrorReason),
    #[error("WASM error: {0}")]
    Wasm(ErrorReason),
    #[error("reference failed: {0}")]
    Reference(ErrorReason),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

impl Error {
    /// Bounded public description; private legacy Display fields are never a fallback.
    pub fn diagnostic_reason(&self) -> String {
        use microcrab_protocol::diagnostic::{describe, json_reason, sanitize};
        if let Some(reason) = self.public_reason() {
            return sanitize(reason, 2048);
        }
        match self {
            Self::Json(source) => json_reason(source),
            Self::Io(source) => format!(
                "{}; kind={:?}; os_error={:?}",
                describe(source, 1800),
                source.kind(),
                source.raw_os_error()
            ),
            Self::ToolFailure(failure) => sanitize(
                &format!(
                    "{}: {}",
                    sanitize(&failure.code, 128),
                    sanitize(&failure.message, 1800)
                ),
                2048,
            ),
            Self::Diagnostic(diagnostic) => sanitize(diagnostic.summary(), 2048),
            Self::Limit(limit) => limit.safe_message().to_owned(),
            Self::Config(_)
            | Self::Tool(_)
            | Self::Denied(_)
            | Self::Lifecycle(_)
            | Self::Audit(_)
            | Self::Wasm(_)
            | Self::Reference(_)
            | Self::Provider(_)
            | Self::Protocol(_)
            | Self::TransportFailure(_) => "failure description omitted".to_owned(),
            _ => describe(self, 2048),
        }
    }
    /// Sanitized owned descriptions and canonical protocol facts are public.
    pub fn public_reason(&self) -> Option<&str> {
        match self {
            Self::VariableConflict => Some(
                "variable inspection is stale; inspect current variables and explicitly save again",
            ),
            Self::StateObservationConflict => Some(
                "current State changed during observation; retry after the permission or mode update settles",
            ),
            Self::TransportFailure(cause)
            | Self::VariableForbidden(cause)
            | Self::Config(cause)
            | Self::Denied(cause)
            | Self::Tool(cause)
            | Self::Lifecycle(cause)
            | Self::Audit(cause)
            | Self::Wasm(cause)
            | Self::Reference(cause) => cause.public_reason(),
            Self::Provider(cause) => cause.public_reason(),
            _ => self.public_protocol_reason(),
        }
    }

    pub fn config(reason: &'static str) -> Self {
        Self::Config(ErrorReason::known(reason))
    }
    pub fn config_with_private(reason: &'static str, private: impl Into<String>) -> Self {
        Self::Config(ErrorReason::with_private(reason, private))
    }
    pub fn denied(reason: &'static str) -> Self {
        Self::Denied(ErrorReason::known(reason))
    }
    pub fn denied_with_private(reason: &'static str, private: impl Into<String>) -> Self {
        Self::Denied(ErrorReason::with_private(reason, private))
    }
    pub fn tool(reason: &'static str) -> Self {
        Self::Tool(ErrorReason::known(reason))
    }
    pub fn tool_with_private(reason: &'static str, private: impl Into<String>) -> Self {
        Self::Tool(ErrorReason::with_private(reason, private))
    }
    pub fn lifecycle(reason: &'static str) -> Self {
        Self::Lifecycle(ErrorReason::known(reason))
    }
    pub fn lifecycle_with_private(reason: &'static str, private: impl Into<String>) -> Self {
        Self::Lifecycle(ErrorReason::with_private(reason, private))
    }
    pub fn audit(reason: &'static str) -> Self {
        Self::Audit(ErrorReason::known(reason))
    }
    pub fn audit_with_private(reason: &'static str, private: impl Into<String>) -> Self {
        Self::Audit(ErrorReason::with_private(reason, private))
    }
    pub fn wasm(reason: &'static str) -> Self {
        Self::Wasm(ErrorReason::known(reason))
    }
    pub fn wasm_with_private(reason: &'static str, private: impl Into<String>) -> Self {
        Self::Wasm(ErrorReason::with_private(reason, private))
    }
    pub fn reference(reason: &'static str) -> Self {
        Self::Reference(ErrorReason::known(reason))
    }
    pub fn reference_with_private(reason: &'static str, private: impl Into<String>) -> Self {
        Self::Reference(ErrorReason::with_private(reason, private))
    }

    pub fn public_protocol_reason(&self) -> Option<&str> {
        match self {
            Self::Protocol(cause) => cause.public_reason(),
            Self::Provider(cause) => cause.public_protocol_reason(),
            _ => None,
        }
    }

    pub fn protocol_with_private_reason(reason: &'static str, private_reason: String) -> Self {
        Self::Protocol(microcrab_transport::ProtocolError::with_private_reason(
            reason,
            private_reason,
        ))
    }

    pub fn protocol(reason: &'static str) -> Self {
        Self::Protocol(microcrab_transport::ProtocolError::known(reason))
    }

    pub fn from_host_error(error: microcrab_host_api::ToolError) -> Self {
        use crate::observation::diagnostics::{
            DiagnosticCode as C, DiagnosticLayer as L, DiagnosticPhase as P, ToolDiagnostic,
        };
        use microcrab_host_api::ToolError as H;
        let code = match &error {
            H::Unsupported(_) => C::Unsupported,
            H::PathOutsideRoot | H::CwdOutsideRoot => C::PathOutsideRoot,
            H::InvalidRequest(_) => C::InvalidArgument,
            H::Timeout | H::OperationDeadline => C::Timeout,
            H::Cancelled => C::Cancelled,
            H::FileLimit | H::OutputLimit | H::OperationLimit(_) => C::OutputLimit,
            H::WorkspaceConflict | H::FileConflict => C::Conflict,
            H::Host(source) => match source.category {
                microcrab_host_api::ErrorCategory::NotFound => C::NotFound,
                microcrab_host_api::ErrorCategory::Permission => C::PermissionDenied,
                microcrab_host_api::ErrorCategory::Timeout => C::Timeout,
                microcrab_host_api::ErrorCategory::Cancelled => C::Cancelled,
                microcrab_host_api::ErrorCategory::Conflict => C::Conflict,
                microcrab_host_api::ErrorCategory::Unsupported => C::Unsupported,
                microcrab_host_api::ErrorCategory::Io => C::Io,
            },
            H::Io(source) => match source.kind() {
                std::io::ErrorKind::NotFound => C::NotFound,
                std::io::ErrorKind::PermissionDenied => C::PermissionDenied,
                std::io::ErrorKind::TimedOut => C::Timeout,
                _ => C::Io,
            },
            _ => C::Io,
        };
        let mut d = ToolDiagnostic::new(
            code,
            L::Host,
            if code == C::PathOutsideRoot {
                P::Validation
            } else {
                P::HostOperation
            },
        );
        d.reason = error
            .public_reason()
            .map(crate::observation::diagnostics::SafeDiagnosticReason::new);
        if let H::Io(source) = &error {
            d.reason = Some(crate::observation::diagnostics::SafeDiagnosticReason::new(
                &format!(
                    "{source}; kind={:?}; os_error={:?}",
                    source.kind(),
                    source.raw_os_error()
                ),
            ));
        }
        if d.reason.is_none() {
            d.reason = Some(crate::observation::diagnostics::SafeDiagnosticReason::new(
                &microcrab_protocol::diagnostic::sanitize(&error.to_string(), 2048),
            ));
        }
        if code == C::PathOutsideRoot {
            d.operation_effects =
                Some(crate::observation::diagnostics::EffectKnowledge::NotStarted);
        }
        if let H::Host(source) = &error {
            d.retryable = Some(source.retryable);
        }
        Self::Diagnostic(d)
    }
    pub fn tool_diagnostic(&self) -> crate::observation::diagnostics::ToolDiagnostic {
        crate::observation::diagnostics::project_error(self)
    }
}

impl From<microcrab_transport::Error> for Error {
    fn from(error: microcrab_transport::Error) -> Self {
        use microcrab_transport::Error as TransportError;
        match error {
            TransportError::Config(cause) => Self::Config(match cause.public_reason() {
                Some(reason) => ErrorReason::with_private(reason, cause.to_string()),
                None => cause.to_string().into(),
            }),
            TransportError::Cancelled => Self::Cancelled,
            TransportError::Limit(message) => {
                let kind = match message.as_str() {
                    "provider request byte budget" => crate::LimitKind::ProviderRequest,
                    "provider response byte budget" => crate::LimitKind::ProviderResponse,
                    "rendered context byte budget" => crate::LimitKind::ContextBytes,
                    "tool index budget" => crate::LimitKind::ToolCalls,
                    "request rewrite output token budget" => crate::LimitKind::PluginRewrite,
                    _ => crate::LimitKind::Other,
                };
                crate::budget::reject(kind, message)
            }
            TransportError::ProviderStatus { status } => Self::ProviderStatus { status },
            TransportError::Transport => Self::Transport,
            TransportError::TransportFailure(cause) => {
                Self::TransportFailure(cause.public_reason().unwrap_or("network failure").into())
            }
            TransportError::ProviderTimeout => Self::ProviderTimeout,
            TransportError::ProviderIdleTimeout(details) => {
                Self::ProviderIdleTimeout(ProviderTimeoutDetails {
                    timeout_ms: details.timeout_ms,
                    elapsed_ms: details.elapsed_ms,
                    first_token_ms: details.first_token_ms,
                    streamed_bytes: details.streamed_bytes,
                    streamed_chars: details.streamed_chars,
                })
            }
            TransportError::Provider(message) => Self::Provider(message),
            TransportError::ProviderUnavailable(failure) => {
                Self::Provider(microcrab_transport::ProviderError::failure(failure))
            }
            TransportError::Protocol(message) => Self::Protocol(message),
            TransportError::Audit(cause) => Self::Lifecycle(match cause.public_reason() {
                Some(reason) => ErrorReason::with_private(reason, cause.to_string()),
                None => cause.to_string().into(),
            }),
            TransportError::Json(error) => Self::Json(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_explicit_producer_causes_are_public_and_debug_is_redacted() {
        let constructors: [fn(&'static str, String) -> Error; 7] = [
            Error::config_with_private,
            Error::denied_with_private,
            Error::tool_with_private,
            Error::lifecycle_with_private,
            Error::audit_with_private,
            Error::wasm_with_private,
            Error::reference_with_private,
        ];
        for make in constructors {
            let error = make(
                "closed framework cause",
                "token=private-secret /private/path".to_owned(),
            );
            assert_eq!(error.public_reason(), Some("closed framework cause"));
            assert!(error.to_string().contains("private-secret"));
            assert!(!format!("{error:?}").contains("private-secret"));
            assert_eq!(
                error.tool_diagnostic().reason.as_ref().unwrap().as_str(),
                "closed framework cause"
            );
        }
        for error in [
            Error::Config("concrete dynamic failure; token=private-secret".into()),
            Error::Denied("concrete dynamic failure; token=private-secret".into()),
            Error::Tool("concrete dynamic failure; token=private-secret".into()),
            Error::Lifecycle("concrete dynamic failure; token=private-secret".into()),
            Error::Audit("concrete dynamic failure; token=private-secret".into()),
            Error::Wasm("concrete dynamic failure; token=private-secret".into()),
            Error::Reference("concrete dynamic failure; token=private-secret".into()),
        ] {
            assert!(
                error
                    .public_reason()
                    .unwrap()
                    .contains("concrete dynamic failure")
            );
            assert!(!format!("{error:?}").contains("private-secret"));
            assert!(
                !error
                    .tool_diagnostic()
                    .result_json()
                    .contains("private-secret")
            );
        }
    }
}
