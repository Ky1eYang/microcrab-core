//! Provider and intervention failures, independent of Session ownership.
mod provider;
mod reason;
pub use provider::ProviderFailure;
pub use reason::FailureReason;

/// A protocol failure keeps private diagnostics separate from a producer-marked,
/// bounded sanitized cause. Producers must exclude bodies from descriptions.
#[derive(Clone, PartialEq, Eq)]
pub struct ProtocolError {
    private_reason: String,
    public_reason: Option<String>,
    retryable_decode: bool,
}

impl ProtocolError {
    /// Mark a framework-owned static reason as safe for public diagnostics.
    pub fn known(reason: &'static str) -> Self {
        Self::with_private_reason(reason, reason.to_owned())
    }

    /// Retain private context while publishing only the safe static cause.
    pub fn with_private_reason(reason: &'static str, private_reason: String) -> Self {
        Self {
            private_reason,
            public_reason: Some(microcrab_protocol::diagnostic::sanitize(reason, 2048)),
            retryable_decode: false,
        }
    }

    pub fn is_retryable_decode(&self) -> bool {
        self.retryable_decode
    }
    pub fn public_reason(&self) -> Option<&str> {
        self.public_reason.as_deref()
    }
}

impl From<String> for ProtocolError {
    fn from(private_reason: String) -> Self {
        Self {
            public_reason: Some(microcrab_protocol::diagnostic::sanitize(
                &private_reason,
                2048,
            )),
            private_reason,
            retryable_decode: false,
        }
    }
}

impl From<&str> for ProtocolError {
    fn from(private_reason: &str) -> Self {
        private_reason.to_owned().into()
    }
}

impl std::fmt::Debug for ProtocolError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProtocolError")
            .field("public_reason", &self.public_reason)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.private_reason)
    }
}
/// Provider failures preserve their recovery classification while retaining an
/// optional protocol cause. Dynamic descriptions use the canonical sanitizer.
#[derive(Clone, PartialEq, Eq)]
pub struct ProviderError {
    private_reason: String,
    protocol: Option<ProtocolError>,
    failure: Option<ProviderFailure>,
}

impl ProviderError {
    pub fn protocol(cause: ProtocolError) -> Self {
        Self {
            private_reason: "provider returned an invalid response".into(),
            protocol: Some(cause),
            failure: None,
        }
    }

    pub fn failure(failure: ProviderFailure) -> Self {
        Self {
            private_reason: failure.public_reason().into(),
            protocol: None,
            failure: Some(failure),
        }
    }

    pub fn failure_details(&self) -> Option<&ProviderFailure> {
        self.failure.as_ref()
    }

    pub fn public_reason(&self) -> Option<&str> {
        self.public_protocol_reason()
            .or_else(|| self.failure.as_ref().map(ProviderFailure::public_reason))
            .or(Some(self.private_reason.as_str()))
    }

    pub fn protocol_error(&self) -> Option<&ProtocolError> {
        self.protocol.as_ref()
    }

    pub fn public_protocol_reason(&self) -> Option<&str> {
        self.protocol_error().and_then(ProtocolError::public_reason)
    }
}

impl From<String> for ProviderError {
    fn from(private_reason: String) -> Self {
        Self {
            private_reason: microcrab_protocol::diagnostic::sanitize(&private_reason, 2048),
            protocol: None,
            failure: None,
        }
    }
}

impl From<&str> for ProviderError {
    fn from(private_reason: &str) -> Self {
        private_reason.to_owned().into()
    }
}

impl std::fmt::Debug for ProviderError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProviderError")
            .field("protocol", &self.protocol)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for ProviderError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.private_reason)
    }
}

/// Bounded measured facts for a transport-owned inactivity expiry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdleTimeout {
    pub timeout_ms: u64,
    pub elapsed_ms: u64,
    pub first_token_ms: Option<u64>,
    pub streamed_bytes: u64,
    pub streamed_chars: u64,
}
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid configuration: {0}")]
    Config(FailureReason),
    #[error("operation cancelled")]
    Cancelled,
    #[error("execution limit exceeded: {0}")]
    Limit(String),
    #[error("provider HTTP status {status}")]
    ProviderStatus { status: u16 },
    #[error("provider transport failed")]
    Transport,
    #[error("provider transport failed: {0}")]
    TransportFailure(FailureReason),
    #[error("provider request timed out")]
    ProviderTimeout,
    #[error("provider network inactivity timed out")]
    ProviderIdleTimeout(IdleTimeout),
    #[error("provider temporarily unavailable")]
    ProviderUnavailable(ProviderFailure),
    #[error("provider error: {0}")]
    Provider(ProviderError),
    #[error("invalid model response: {0}")]
    Protocol(ProtocolError),
    #[error("audit failed: {0}")]
    Audit(FailureReason),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
impl Error {
    pub fn diagnostic_reason(&self) -> String {
        use microcrab_protocol::diagnostic::{describe, json_reason};
        match self {
            Self::Config(cause) | Self::Audit(cause) | Self::TransportFailure(cause) => cause
                .public_reason()
                .unwrap_or("failure description omitted")
                .to_owned(),
            Self::Protocol(cause) => cause
                .public_reason()
                .unwrap_or("protocol description omitted")
                .to_owned(),
            Self::Provider(cause) => cause
                .public_reason()
                .unwrap_or("provider description omitted")
                .to_owned(),
            Self::Json(source) => json_reason(source),
            _ => describe(self, 2048),
        }
    }
    pub fn config(reason: &'static str) -> Self {
        Self::Config(FailureReason::known(reason))
    }
    pub fn config_with_private(reason: &'static str, private: impl Into<String>) -> Self {
        Self::Config(FailureReason::with_private(reason, private))
    }
    pub fn audit(reason: &'static str) -> Self {
        Self::Audit(FailureReason::known(reason))
    }

    pub fn public_protocol_reason(&self) -> Option<&str> {
        match self {
            Self::Protocol(cause) => cause.public_reason(),
            Self::Provider(cause) => cause.public_protocol_reason(),
            _ => None,
        }
    }

    pub fn decode(error: serde_json::Error) -> Self {
        Self::decode_reason(microcrab_protocol::diagnostic::json_reason(&error))
    }
    pub(crate) fn decode_reason(reason: impl Into<String>) -> Self {
        let mut cause: ProtocolError = reason.into().into();
        cause.retryable_decode = true;
        Self::Protocol(cause)
    }
    pub fn protocol(reason: &'static str) -> Self {
        Self::Protocol(ProtocolError::known(reason))
    }
}

impl From<microcrab_auditor::AuditorError> for Error {
    fn from(error: microcrab_auditor::AuditorError) -> Self {
        Self::Audit(
            format!(
                "model audit intervention failed: {}",
                microcrab_protocol::diagnostic::describe(&error, 1800)
            )
            .into(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_retains_safe_causes_and_redacts_all_private_context() {
        let private = "response_body=private-call-id storage-body secret";
        let opaque = Error::Protocol(private.into());
        let custom_provider = Error::Provider(private.into());
        let protocol =
            ProtocolError::with_private_reason("invalid tool arguments JSON", private.into());
        let known = Error::Protocol(protocol.clone());
        let provider = Error::Provider(ProviderError::protocol(protocol));
        for error in [&opaque, &custom_provider, &known, &provider] {
            assert!(!format!("{error:?}").contains(private));
        }
        assert!(format!("{known:?}").contains("invalid tool arguments JSON"));
        assert!(format!("{provider:?}").contains("invalid tool arguments JSON"));
        assert!(opaque.to_string().contains(private));
    }

    #[test]
    fn arbitrary_strings_remain_opaque_even_when_they_match_a_known_reason() {
        for cause in [
            ProtocolError::from("concrete protocol mismatch; token=private-secret"),
            ProtocolError::from("empty assistant response".to_owned()),
        ] {
            assert!(cause.public_reason().is_some());
            assert!(!cause.public_reason().unwrap().contains("private-secret"));
        }
        let cause = ProtocolError::with_private_reason(
            "invalid tool arguments JSON",
            "invalid tool arguments JSON for call private-call-id".into(),
        );
        assert_eq!(cause.public_reason(), Some("invalid tool arguments JSON"));
        assert!(cause.to_string().contains("private-call-id"));
        let provider = super::super::failure::response_error(Error::Protocol(cause));
        assert!(matches!(&provider, Error::Provider(_)));
        assert_eq!(
            provider.public_protocol_reason(),
            Some("invalid tool arguments JSON")
        );
    }
}
