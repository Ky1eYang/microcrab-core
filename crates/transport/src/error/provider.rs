//! Closed in-band rejection facts; provider messages and unknown codes stay private.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderFailure {
    kind: Option<&'static str>,
    status: Option<u16>,
}
impl ProviderFailure {
    pub fn http_status(status: u16) -> Option<Self> {
        (100..=599).contains(&status).then_some(Self {
            kind: None,
            status: Some(status),
        })
    }

    pub(crate) fn from_value(error: &serde_json::Value) -> Self {
        let mut kind = None;
        let mut status = None;
        for field in ["code", "status", "type"] {
            let Some(value) = error.get(field) else {
                continue;
            };
            if status.is_none() {
                status = value
                    .as_u64()
                    .or_else(|| value.as_str()?.parse().ok())
                    .filter(|status| (100..=599).contains(status))
                    .map(|status| status as u16);
            }
            if kind.is_none() {
                kind = value.as_str().and_then(Self::known_kind);
            }
        }
        Self { kind, status }
    }
    fn known_kind(value: &str) -> Option<&'static str> {
        // Never echo a provider-owned identifier, even when its shape looks safe.
        Some(match value.to_ascii_lowercase().as_str() {
            "invalid_request_error" => "invalid_request_error",
            "authentication_error" => "authentication_error",
            "permission_error" => "permission_error",
            "not_found_error" => "not_found_error",
            "context_length_exceeded" => "context_length_exceeded",
            "insufficient_quota" => "insufficient_quota",
            "billing_error" => "billing_error",
            "content_filter" => "content_filter",
            "server_error" => "server_error",
            "internal_error" => "internal_error",
            "internal_server_error" => "internal_server_error",
            "overloaded_error" => "overloaded_error",
            "rate_limit_error" => "rate_limit_error",
            "rate_limit_exceeded" => "rate_limit_exceeded",
            "unavailable" => "unavailable",
            "internal" => "internal",
            "resource_exhausted" => "resource_exhausted",
            "deadline_exceeded" => "deadline_exceeded",
            _ => return None,
        })
    }
    pub fn kind(&self) -> Option<&'static str> {
        self.kind
    }
    pub fn status(&self) -> Option<u16> {
        self.status
    }
    pub fn public_reason(&self) -> &'static str {
        match self.kind {
            Some("invalid_request_error") => "Model provider rejected invalid request parameters",
            Some("authentication_error") => "Model provider authentication failed",
            Some("permission_error") => "Model provider denied access",
            Some("not_found_error") => "Model provider could not find the requested resource",
            Some("context_length_exceeded") => "Model provider context length was exceeded",
            Some("insufficient_quota" | "billing_error") => {
                "Model provider quota or billing rejected the request"
            }
            Some("content_filter") => "Model provider content filtering rejected the response",
            Some("rate_limit_error" | "rate_limit_exceeded" | "resource_exhausted") => {
                "Model provider rate or resource limit was exceeded"
            }
            Some("deadline_exceeded") => "Model provider reported a deadline exceeded",
            Some(_) => "Model provider reported a temporary service failure",
            None if self.status.is_some() => "Model provider reported an in-band HTTP error status",
            None => "Model provider reported an in-band failure without a recognized code or type",
        }
    }
}
