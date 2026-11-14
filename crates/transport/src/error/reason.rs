//! Producer-marked Transport configuration and audit causes.
#[derive(Clone, PartialEq, Eq)]
pub struct FailureReason {
    private_reason: String,
    public_reason: Option<String>,
}
impl FailureReason {
    pub fn known(reason: &'static str) -> Self {
        Self::with_private(reason, reason)
    }
    pub fn with_private(reason: &'static str, private: impl Into<String>) -> Self {
        Self {
            private_reason: private.into(),
            public_reason: Some(microcrab_protocol::diagnostic::sanitize(reason, 2048)),
        }
    }
    pub fn public_reason(&self) -> Option<&str> {
        self.public_reason.as_deref()
    }
}
impl From<String> for FailureReason {
    fn from(private_reason: String) -> Self {
        Self {
            public_reason: Some(microcrab_protocol::diagnostic::sanitize(
                &private_reason,
                2048,
            )),
            private_reason,
        }
    }
}
impl From<&str> for FailureReason {
    fn from(private_reason: &str) -> Self {
        private_reason.to_owned().into()
    }
}
impl std::fmt::Debug for FailureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FailureReason")
            .field("public_reason", &self.public_reason)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Display for FailureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.private_reason)
    }
}
