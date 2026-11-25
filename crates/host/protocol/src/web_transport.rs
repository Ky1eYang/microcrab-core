//! Ephemeral, non-replayable native requests. These are not mutation receipts:
//! identities and text must never be written to receipt storage. Carriers must
//! retire an ambiguously affected connection, not retry it or transfer ownership.
use crate::{ErrorCode, HostCause, ProjectScope, WireError};
use microcrab_host_api::web_transport::{
    ConnectionOwner, ConnectionScope, EndpointKind, WEB_MAX_BUDGET_MS, WEB_MAX_HTTP_BODY_BYTES,
    WEB_MAX_WS_RECEIVE_BYTES, WEB_MAX_WS_SEND_BYTES, WEB_MAX_WS_SOFT_WAIT_MS, WebTransportError,
    WebTransportResult, validate_endpoint_shape, validate_limit,
};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Version 1 is an opt-in, lease-local proof, never a descriptor pin.
pub const WEB_TRANSPORT_V1: u32 = 1;

/// Accept an exact version only where this route is configured to support it.
/// The returned value is only a candidate until the same control route has
/// authenticated successfully. Never grant it on Inspect/WorkspaceBound.
pub fn web_transport_v1_negotiated(offer: Option<u32>, acknowledgment: Option<u32>) -> bool {
    offer == Some(WEB_TRANSPORT_V1) && acknowledgment == offer
}

/// Protobuf-only Control handshake features. The pull bit advertises method
/// compatibility; it grants nothing independently of a proven v1 Project route.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WebTransportFeatures {
    pub version: Option<u32>,
    pub ws_pull: bool,
}

impl WebTransportFeatures {
    pub const V1: Self = Self { version: Some(WEB_TRANSPORT_V1), ws_pull: true };
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum WebTransportRequest {
    HttpGet { scope: ConnectionScope, url: String, max_bytes: u32 },
    WsOpen { scope: ConnectionScope, url: String },
    WsStatus { scope: ConnectionScope },
    WsSend { scope: ConnectionScope, text: String },
    WsReceive { scope: ConnectionScope, max_bytes: u32 },
    WsPull { scope: ConnectionScope, max_bytes: u32, soft_wait_ms: u32 },
    WsClose { scope: ConnectionScope },
    ReleaseScope { owner: ConnectionOwner },
}
impl WebTransportRequest {
    /// Closed vocabulary, suitable for metrics or redacted Debug output.
    pub fn operation(&self) -> &'static str {
        match self {
            Self::HttpGet { .. } => "http_get",
            Self::WsOpen { .. } => "ws_open",
            Self::WsStatus { .. } => "ws_status",
            Self::WsSend { .. } => "ws_send",
            Self::WsReceive { .. } => "ws_receive",
            Self::WsPull { .. } => "ws_pull",
            Self::WsClose { .. } => "ws_close",
            Self::ReleaseScope { .. } => "release_scope",
        }
    }

    pub fn binding(&self) -> &ProjectScope {
        match self {
            Self::HttpGet { scope, .. }
            | Self::WsOpen { scope, .. }
            | Self::WsStatus { scope }
            | Self::WsSend { scope, .. }
            | Self::WsReceive { scope, .. }
            | Self::WsPull { scope, .. }
            | Self::WsClose { scope } => &scope.binding,
            Self::ReleaseScope { owner } => &owner.binding,
        }
    }

    /// Maximum serialized JSON reply, before any transfer allocation. Callers
    /// must enforce this per-operation ceiling on `TransferRef` and validate the
    /// decoded body/message against its raw byte limit. Only GET, receive and
    /// pull may transfer payloads; successful unit operations return null.
    pub fn max_reply_json_bytes(&self) -> Option<usize> {
        let bound = match self {
            Self::HttpGet { max_bytes, .. } => (*max_bytes).min(WEB_MAX_HTTP_BODY_BYTES),
            Self::WsReceive { max_bytes, .. } | Self::WsPull { max_bytes, .. } => (*max_bytes).min(WEB_MAX_WS_RECEIVE_BYTES),
            _ => return None,
        };
        // A UTF-8 byte can expand to six JSON bytes (e.g. a control character).
        Some(128 + 6 * bound as usize)
    }

    /// No I/O. Bridge must additionally check the exact authenticated control
    /// lease and the full resource scope, and parse URLs before dispatch.
    pub fn validate(&self, remaining_budget_ms: u32) -> WebTransportResult<()> {
        if remaining_budget_ms == 0 {
            return Err(WebTransportError::Timeout);
        }
        validate_limit(remaining_budget_ms, WEB_MAX_BUDGET_MS)?;
        match self {
            Self::ReleaseScope { owner } if !owner.valid() => return Err(WebTransportError::InvalidRequest),
            Self::HttpGet { scope, .. }
            | Self::WsOpen { scope, .. }
            | Self::WsStatus { scope }
            | Self::WsSend { scope, .. }
            | Self::WsReceive { scope, .. }
            | Self::WsPull { scope, .. }
            | Self::WsClose { scope } if !scope.valid() => return Err(WebTransportError::InvalidRequest),
            _ => {}
        }
        match self {
            Self::HttpGet { url, max_bytes, .. } => {
                validate_endpoint_shape(url, EndpointKind::Http)?;
                validate_limit(*max_bytes, WEB_MAX_HTTP_BODY_BYTES)
            }
            Self::WsOpen { url, .. } => validate_endpoint_shape(url, EndpointKind::WebSocket),
            Self::WsSend { text, .. } if text.len() > WEB_MAX_WS_SEND_BYTES => Err(WebTransportError::Limit),
            Self::WsReceive { max_bytes, .. } => validate_limit(*max_bytes, WEB_MAX_WS_RECEIVE_BYTES),
            Self::WsPull { max_bytes, soft_wait_ms, .. } => {
                if *soft_wait_ms > WEB_MAX_WS_SOFT_WAIT_MS {
                    return Err(WebTransportError::Limit);
                }
                validate_limit(*max_bytes, WEB_MAX_WS_RECEIVE_BYTES)
            }
            _ => Ok(()),
        }
    }

    /// Caller supplies facts from authenticated controller ownership, not from
    /// a header or untrusted frame. This is independent of endpoint authorization
    /// (which Runtime binds to the admitted invocation) and connection ownership
    /// (which the socket owner validates under the same lease).
    pub fn validate_for_lease(
        &self,
        authenticated_binding: &crate::BindingScope,
        negotiated: bool,
        remaining_budget_ms: u32,
    ) -> WebTransportResult<()> {
        if !negotiated {
            return Err(WebTransportError::Unsupported);
        }
        if !matches!(authenticated_binding, crate::BindingScope::Project(binding) if binding == self.binding()) {
            return Err(WebTransportError::Denied);
        }
        self.validate(remaining_budget_ms)
    }
}
impl fmt::Debug for WebTransportRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("WebTransportRequest").field(&self.operation()).finish()
    }
}

impl From<WebTransportError> for WireError {
    fn from(error: WebTransportError) -> Self {
        use WebTransportError as E;
        let (code, cause) = match error {
            E::Unsupported => (ErrorCode::Unsupported, None),
            E::Denied => (ErrorCode::Denied, None),
            E::InvalidRequest => (ErrorCode::InvalidRequest, None),
            E::Limit => (ErrorCode::Limit, None),
            E::Cancelled => (ErrorCode::Cancelled, None),
            E::Timeout => (ErrorCode::Timeout, None),
            E::Stale => (ErrorCode::NotFound, Some(HostCause::WebTransportStale)),
            E::Closed => (ErrorCode::NotFound, Some(HostCause::WebTransportClosed)),
            E::OutcomeUnknown => (ErrorCode::OutcomeUnknown, None),
            E::Io => (ErrorCode::Io, None),
        };
        WireError::new(code, error.to_string()).with_cause(cause)
    }
}
impl From<WireError> for WebTransportError {
    fn from(error: WireError) -> Self {
        // Never promote arbitrary remote messages to component or log output.
        match (error.code, error.cause) {
            (ErrorCode::Unsupported, _) => Self::Unsupported,
            (ErrorCode::Denied | ErrorCode::BindingMismatch, _) => Self::Denied,
            (ErrorCode::InvalidRequest, _) => Self::InvalidRequest,
            (ErrorCode::Limit, _) => Self::Limit,
            (ErrorCode::Cancelled, _) => Self::Cancelled,
            (ErrorCode::Timeout, _) => Self::Timeout,
            (ErrorCode::NotFound, Some(HostCause::WebTransportClosed)) => Self::Closed,
            (ErrorCode::NotFound | ErrorCode::LeaseExpired, _) => Self::Stale,
            (ErrorCode::OutcomeUnknown | ErrorCode::Disconnected, _) => Self::OutcomeUnknown,
            _ => Self::Io,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BindingScope;
    use microcrab_host_api::web_transport::{HttpResponse, WsPull, WsReceive};

    fn scope() -> ConnectionScope {
        ConnectionScope {
            binding: ProjectScope { binding_id: "binding".into(), backend_id: "backend".into(), project_id: "project".into() },
            session_id: "session".into(),
            epoch: 4,
            connection_id: "unique-id".into(),
        }
    }

    #[test]
    fn validation_rejects_wrong_binding_unproven_routes_and_bad_scopes() {
        let scope = scope();
        let binding = BindingScope::Project(scope.binding.clone());
        let valid = WebTransportRequest::WsStatus { scope: scope.clone() };
        assert_eq!(valid.validate_for_lease(&binding, true, 1), Ok(()));
        assert_eq!(valid.validate_for_lease(&binding, false, 1), Err(WebTransportError::Unsupported));
        let mismatched = BindingScope::project("backend".into(), "other".into(), "project".into());
        let conversation = BindingScope::conversation("backend".into(), "binding".into(), "project".into());
        for binding in [mismatched, conversation] {
            assert_eq!(valid.validate_for_lease(&binding, true, 1), Err(WebTransportError::Denied));
        }
        assert_eq!(valid.validate(0), Err(WebTransportError::Timeout));
        assert_eq!(valid.validate(WEB_MAX_BUDGET_MS + 1), Err(WebTransportError::Limit));
        let invalid = WebTransportRequest::WsStatus { scope: ConnectionScope { connection_id: "wrong/path".into(), ..scope.clone() } };
        assert_eq!(invalid.validate_for_lease(&binding, true, 1), Err(WebTransportError::InvalidRequest));
        let invalid = WebTransportRequest::ReleaseScope { owner: ConnectionOwner { session_id: "\n".into(), ..scope.owner() } };
        assert_eq!(invalid.validate_for_lease(&binding, true, 1), Err(WebTransportError::InvalidRequest));
    }

    #[test]
    fn endpoint_shape_accepts_absolute_urls_and_rejects_ambiguous_authority() {
        for url in ["http://example.test:9222/json/version", "HTTP://[::1]:9222/path?x=1", "https://example.test"] {
            assert_eq!(validate_endpoint_shape(url, EndpointKind::Http), Ok(()));
        }
        for url in ["wss://[2001:db8::1]/path", "ws://example.test:9222/path"] {
            assert_eq!(validate_endpoint_shape(url, EndpointKind::WebSocket), Ok(()));
        }
        for url in [
            "example.test:9222", "ws://example.test", "http:///path", "http://host:0/path",
            "http://host:65536/path", "http://host:/path", "http://host:+80/path",
            "http://::1/path", "http://[::1/path", "http://[example.test]/path", "http://[::1]bad/path",
            "http://user@host/path", "http://ho%73t/path", "http://host/#fragment", "http://host\\path", "http://host/white space",
        ] {
            assert_eq!(validate_endpoint_shape(url, EndpointKind::Http), Err(WebTransportError::InvalidRequest), "{url}");
        }
        let url = format!("http://host/{}", "x".repeat(4096));
        assert_eq!(validate_endpoint_shape(&url, EndpointKind::Http), Err(WebTransportError::Limit));
    }

    #[test]
    fn raw_limits_and_json_transfer_bounds_are_distinct() {
        let scope = scope();
        let get = WebTransportRequest::HttpGet { scope: scope.clone(), url: "https://host/path".into(), max_bytes: WEB_MAX_HTTP_BODY_BYTES };
        let receive = WebTransportRequest::WsReceive { scope: scope.clone(), max_bytes: WEB_MAX_WS_RECEIVE_BYTES };
        let send = WebTransportRequest::WsSend { scope: scope.clone(), text: "é".repeat(WEB_MAX_WS_SEND_BYTES / 2) };
        for request in [&get, &receive, &send] {
            assert_eq!(request.validate(1), Ok(()));
        }
        assert_eq!(send.max_reply_json_bytes(), None);
        let send = WebTransportRequest::WsSend { scope: scope.clone(), text: "é".repeat(WEB_MAX_WS_SEND_BYTES / 2 + 1) };
        assert_eq!(send.validate(1), Err(WebTransportError::Limit));
        let invalid = WebTransportRequest::WsReceive { scope, max_bytes: WEB_MAX_WS_RECEIVE_BYTES + 1 };
        assert_eq!(invalid.validate(1), Err(WebTransportError::Limit));
        let http = HttpResponse { status: 200, body: "\u{0001}".repeat(WEB_MAX_HTTP_BODY_BYTES as usize) };
        let ws = WsReceive::Message("\u{0001}".repeat(WEB_MAX_WS_RECEIVE_BYTES as usize));
        assert_eq!(http.validate(WEB_MAX_HTTP_BODY_BYTES), Ok(()));
        assert_eq!(ws.validate(WEB_MAX_WS_RECEIVE_BYTES), Ok(()));
        assert_eq!(http.validate(WEB_MAX_HTTP_BODY_BYTES - 1), Err(WebTransportError::Limit));
        assert_eq!(ws.validate(WEB_MAX_WS_RECEIVE_BYTES - 1), Err(WebTransportError::Limit));
        assert!(serde_json::to_vec(&http).unwrap().len() <= get.max_reply_json_bytes().unwrap());
        let wire = serde_json::to_vec(&ws).unwrap();
        assert!(wire.len() > crate::MAX_FRAME_BYTES);
        assert!(wire.len() <= receive.max_reply_json_bytes().unwrap());
        assert_eq!(receive.max_reply_json_bytes(), Some(128 + 6 * WEB_MAX_WS_RECEIVE_BYTES as usize));
        assert_eq!(serde_json::from_slice::<WsReceive>(&wire).unwrap(), ws);
    }

    #[test]
    fn pull_wait_and_reply_limits_are_independent_of_hard_budget() {
        let pull = |max_bytes, soft_wait_ms| WebTransportRequest::WsPull {
            scope: scope(),
            max_bytes,
            soft_wait_ms,
        };
        let immediate = pull(1, 0);
        assert_eq!(immediate.validate(1), Ok(()));
        assert_eq!(pull(1, WEB_MAX_WS_SOFT_WAIT_MS).validate(1), Ok(()));
        assert_eq!(pull(1, WEB_MAX_WS_SOFT_WAIT_MS + 1).validate(1), Err(WebTransportError::Limit));
        assert_eq!(pull(0, 0).validate(1), Err(WebTransportError::InvalidRequest));
        assert_eq!(pull(WEB_MAX_WS_RECEIVE_BYTES + 1, 0).validate(1), Err(WebTransportError::Limit));
        assert_eq!(immediate.validate(0), Err(WebTransportError::Timeout));
        assert_eq!(immediate.max_reply_json_bytes(), Some(134));
        for (reply, kind) in [(WsPull::Empty, "empty"), (WsPull::Closed, "closed")] {
            let value = serde_json::to_value(&reply).unwrap();
            assert_eq!(value, serde_json::json!({"kind": kind}));
            assert_eq!(serde_json::from_value::<WsPull>(value).unwrap(), reply);
        }
        assert!(serde_json::from_value::<WsPull>(serde_json::json!({"kind": "timeout"})).is_err());
        assert_eq!(WsPull::Message(String::new()).validate(1), Ok(()));
        assert_eq!(WsPull::Message("é".into()).validate(1), Err(WebTransportError::Limit));
        assert!(!format!("{:?}", WsPull::Message("secret".into())).contains("secret"));
    }

    #[test]
    fn operations_and_errors_do_not_expose_payloads() {
        let operation = WebTransportRequest::WsSend { scope: scope(), text: "do-not-log".into() };
        let request = crate::HostRequest::WebTransport(operation);
        assert!(request.is_ephemeral());
        assert!(!request.is_mutation());
        assert!(!format!("{request:?}").contains("do-not-log"));
        assert!(!format!("{:?}", WsReceive::Message("do-not-log".into())).contains("do-not-log"));
        for error in [WebTransportError::Stale, WebTransportError::Closed, WebTransportError::OutcomeUnknown] {
            assert_eq!(WebTransportError::from(WireError::from(error)), error);
        }
    }
}
