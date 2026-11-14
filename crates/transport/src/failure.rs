//! In-band provider failures keep typed retry policy without retaining private bodies.
use crate::Error;
use serde_json::Value;

/// A failed error-body drain must not replace an already known HTTP rejection.
/// Successful responses retain their network error and retry classification.
/// Timeout stays distinct from other transport failures. Protocol adapters log
/// through this one classifier instead of copying the branch.
pub(crate) fn transport_error(
    error: reqwest::Error,
    protocol: &'static str,
    context: &'static str,
) -> Error {
    if error.is_timeout() {
        Error::ProviderTimeout
    } else {
        tracing::warn!(
            protocol,
            context,
            connect = error.is_connect(),
            request = error.is_request(),
            body = error.is_body(),
            decode = error.is_decode(),
            redirect = error.is_redirect(),
            status = error.status().map(|status| status.as_u16()),
            "provider transport failure"
        );
        let error = error.without_url();
        let mut message = format!(
            "{protocol} {context}: {}",
            microcrab_protocol::diagnostic::describe(&error, 2048)
        );
        let mut source = std::error::Error::source(&error);
        for _ in 0..4 {
            let Some(cause) = source else {
                break;
            };
            if message.len() >= 4096 {
                break;
            }
            message.push_str(": ");
            message.push_str(&microcrab_protocol::diagnostic::describe(cause, 1024));
            source = cause.source();
        }
        Error::TransportFailure(message.into())
    }
}

pub(crate) fn body_error(status: reqwest::StatusCode, error: Error) -> Error {
    if status.is_success() {
        error
    } else {
        Error::ProviderStatus {
            status: status.as_u16(),
        }
    }
}

/// One warning per provider call when the received body passes the large-response
/// threshold. The configured ceiling remains the only rejection, and this record
/// carries sizes only, never body bytes.
pub(crate) fn warn_large_response(received: usize, ceiling: usize, warned: &mut bool) {
    let threshold = (ceiling / 2).min(32 * 1024 * 1024);
    if !*warned && received >= threshold {
        *warned = true;
        tracing::warn!(
            received_bytes = received,
            ceiling_bytes = ceiling,
            "provider response passed the large-response warning threshold; reception continues"
        );
    }
}

pub(crate) fn response_error(error: Error) -> Error {
    match error {
        Error::Json(error) => Error::decode(error),
        Error::Protocol(cause) => Error::Provider(crate::ProviderError::protocol(cause)),
        error => error,
    }
}

pub(crate) fn provider_failure(value: &Value) -> Error {
    let error = value
        .get("error")
        .filter(|error| !error.is_null())
        .or_else(|| {
            value
                .pointer("/response/error")
                .filter(|error| !error.is_null())
        })
        .unwrap_or(value);
    let transient = ["code", "status", "type"].iter().any(|field| {
        let Some(value) = error.get(*field) else {
            return false;
        };
        if let Some(status) = value.as_u64().or_else(|| value.as_str()?.parse().ok()) {
            return matches!(status, 408 | 429 | 500..=599);
        }
        value.as_str().is_some_and(|kind| {
            matches!(
                kind.to_ascii_lowercase().as_str(),
                "server_error"
                    | "internal_error"
                    | "internal_server_error"
                    | "overloaded_error"
                    | "rate_limit_error"
                    | "rate_limit_exceeded"
                    | "unavailable"
                    | "internal"
                    | "resource_exhausted"
                    | "deadline_exceeded"
            )
        })
    });
    let facts = crate::ProviderFailure::from_value(error);
    if transient {
        Error::ProviderUnavailable(facts)
    } else {
        Error::Provider(crate::ProviderError::failure(facts))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn in_band_failure_policy_uses_codes_not_private_messages() {
        for value in [
            json!({"error":{"code":503}}),
            json!({"error":{"type":"overloaded_error"}}),
            json!({"response":{"error":{"code":"server_error"}}}),
            json!({"error":{"status":"RESOURCE_EXHAUSTED"}}),
        ] {
            assert!(matches!(
                provider_failure(&value),
                Error::ProviderUnavailable(_)
            ));
        }
        for value in [
            json!({"error":{"code":401,"message":"private"}}),
            json!({"error":{"type":"invalid_request_error"}}),
            json!({"error":{"message":"server_error private text"}}),
            json!({"type":"response.incomplete"}),
        ] {
            let error = provider_failure(&value);
            assert!(matches!(error, Error::Provider(_)));
            assert!(!error.to_string().contains("private"));
        }
    }
}
