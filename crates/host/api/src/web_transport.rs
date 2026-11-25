//! Bounded text transport for explicitly admitted Project endpoints. Neither
//! a linked import nor one of these data types grants network authority.
use crate::{ErrorCategory, OpContext};
use serde::{Deserialize, Serialize};
use std::fmt;

pub const WEB_MAX_ENDPOINT_BYTES: usize = 4096;
pub const WEB_MAX_HTTP_BODY_BYTES: u32 = 16 * 1024;
pub const WEB_MAX_WS_SEND_BYTES: usize = 16 * 1024;
pub const WEB_MAX_WS_RECEIVE_BYTES: u32 = 256 * 1024;
pub const WEB_MAX_WS_SOFT_WAIT_MS: u32 = 1000;
pub const WEB_MAX_BUDGET_MS: u32 = 10_000;

/// Exact native Project binding (unchanged legacy wire shape). This value
/// must originate in the authenticated attachment, not in component data.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields)]
pub struct ProjectScope {
    pub binding_id: String,
    pub backend_id: String,
    pub project_id: String,
}
impl ProjectScope {
    pub fn valid(&self) -> bool {
        [&self.binding_id, &self.backend_id, &self.project_id]
            .into_iter()
            .all(|value| !value.trim().is_empty() && value.len() <= 256 && !value.contains('\0'))
    }
}

/// Runtime-issued owner, never minted from a guest or restored State. A native
/// Host additionally binds this to the authenticated carrier lease; the epoch
/// does not stand in for that lease or its authentication.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields)]
pub struct ConnectionOwner {
    pub binding: ProjectScope,
    pub session_id: String,
    pub epoch: u64,
}
impl ConnectionOwner {
    pub fn valid(&self) -> bool {
        self.binding.valid()
            && !self.session_id.trim().is_empty()
            && self.session_id.len() <= 256
            && !self.session_id.chars().any(char::is_control)
    }
}

/// Full identity checked on every operation. Runtime allocates a fresh identity
/// for every explicit connect attempt (including failures) and may expose just
/// the opaque id to the guest. Never persist this identity as an authority.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields)]
pub struct ConnectionScope {
    pub binding: ProjectScope,
    pub session_id: String,
    pub epoch: u64,
    pub connection_id: String,
}
impl ConnectionScope {
    pub fn owner(&self) -> ConnectionOwner {
        ConnectionOwner {
            binding: self.binding.clone(),
            session_id: self.session_id.clone(),
            epoch: self.epoch,
        }
    }

    pub fn valid(&self) -> bool {
        self.owner().valid()
            && !self.connection_id.is_empty()
            && self.connection_id.len() <= 128
            && self
                .connection_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    }
}

/// UTF-8 response body, returned whole or rejected. No ambient headers or
/// response headers (including redirect targets) cross this interface.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HttpResponse {
    pub status: u16,
    pub body: String,
}
impl HttpResponse {
    pub fn validate(&self, max_bytes: u32) -> WebTransportResult<()> {
        if !(100..=599).contains(&self.status) {
            return Err(WebTransportError::Io);
        }
        validate_limit(max_bytes, WEB_MAX_HTTP_BODY_BYTES)?;
        if self.body.len() > max_bytes as usize {
            return Err(WebTransportError::Limit);
        }
        Ok(())
    }
}
impl fmt::Debug for HttpResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpResponse")
            .field("status", &self.status)
            .field("body_bytes", &self.body.len())
            .finish()
    }
}

/// One complete UTF-8 message (which may be empty), idle deadline, or known
/// closure. A receive timeout reports no message delivered; a late external
/// response can still arrive. An ambiguous consume must fail OutcomeUnknown.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "text", rename_all = "snake_case", deny_unknown_fields)]
pub enum WsReceive {
    Message(String),
    Timeout,
    Closed,
}
impl WsReceive {
    pub fn validate(&self, max_bytes: u32) -> WebTransportResult<()> {
        validate_limit(max_bytes, WEB_MAX_WS_RECEIVE_BYTES)?;
        if matches!(self, Self::Message(text) if text.len() > max_bytes as usize) {
            return Err(WebTransportError::Limit);
        }
        Ok(())
    }
}
impl fmt::Debug for WsReceive {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message(text) => f.debug_tuple("MessageBytes").field(&text.len()).finish(),
            Self::Timeout => f.write_str("Timeout"),
            Self::Closed => f.write_str("Closed"),
        }
    }
}

/// One complete UTF-8 message (which may itself be empty), a healthy stream
/// with no message consumed, or known closure. `Empty` is not a hard deadline.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "text", rename_all = "snake_case", deny_unknown_fields)]
pub enum WsPull {
    Message(String),
    Empty,
    Closed,
}
impl WsPull {
    pub fn validate(&self, max_bytes: u32) -> WebTransportResult<()> {
        validate_limit(max_bytes, WEB_MAX_WS_RECEIVE_BYTES)?;
        if matches!(self, Self::Message(text) if text.len() > max_bytes as usize) {
            return Err(WebTransportError::Limit);
        }
        Ok(())
    }
}
impl fmt::Debug for WsPull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message(text) => f.debug_tuple("MessageBytes").field(&text.len()).finish(),
            Self::Empty => f.write_str("Empty"),
            Self::Closed => f.write_str("Closed"),
        }
    }
}

/// Closed vocabulary: never carry raw frames, URL secrets, library or upstream
/// error strings in diagnostics, receipts or ordinary logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WebTransportError {
    #[error("the authenticated Host route does not support web transport")]
    Unsupported,
    #[error("web transport operation is not authorized for this invocation or endpoint")]
    Denied,
    #[error("web transport request is malformed")]
    InvalidRequest,
    #[error("web transport request or complete response exceeds its byte limit")]
    Limit,
    #[error("web transport operation cancelled")]
    Cancelled,
    #[error("web transport operation deadline exceeded")]
    Timeout,
    #[error("web transport connection identity is stale; connect explicitly")]
    Stale,
    #[error("web transport connection is closed; connect explicitly")]
    Closed,
    #[error("web transport operation outcome is unknown; do not replay, connect explicitly")]
    OutcomeUnknown,
    #[error("web transport I/O failed")]
    Io,
}
pub type WebTransportResult<T> = Result<T, WebTransportError>;

/// A zero bound cannot authorize a get/receive. A deadline above the shared
/// cap must be clamped by the trusted caller before wire admission, never by
/// extending the invocation or carrier deadline.
pub fn validate_limit(requested: u32, ceiling: u32) -> WebTransportResult<()> {
    if requested == 0 {
        Err(WebTransportError::InvalidRequest)
    } else if requested > ceiling {
        Err(WebTransportError::Limit)
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointKind {
    Http,
    WebSocket,
}

/// Early wire-shape check only. Runtime still parses the admitted exact URL and
/// Bridge independently parses an absolute URL before dialing; neither may
/// rewrite authority or target, include userinfo, follow redirects, use proxy
/// authority/ambient credentials, or downgrade TLS. This check deliberately
/// performs no DNS resolution, normalization or grant.
pub fn validate_endpoint_shape(url: &str, kind: EndpointKind) -> WebTransportResult<()> {
    if url.len() > WEB_MAX_ENDPOINT_BYTES {
        return Err(WebTransportError::Limit);
    }
    if url.chars().any(|c| c.is_control() || c.is_whitespace() || matches!(c, '#' | '\\')) {
        return Err(WebTransportError::InvalidRequest);
    }
    let Some((scheme, tail)) = url.split_once("://") else {
        return Err(WebTransportError::InvalidRequest);
    };
    let allowed = match kind {
        EndpointKind::Http => ["http", "https"],
        EndpointKind::WebSocket => ["ws", "wss"],
    };
    let authority = tail.split(['/', '?']).next().unwrap_or_default();
    if !allowed.iter().any(|allowed| scheme.eq_ignore_ascii_case(allowed))
        || authority.is_empty()
        || authority.contains(['@', '%'])
    {
        return Err(WebTransportError::InvalidRequest);
    }
    // Reject ambiguous bracket/port forms before passing the original URL to
    // the carrier's full parser. Standard implicit ports remain valid for URLs.
    let port = if let Some(ip_and_suffix) = authority.strip_prefix('[') {
        let Some((ip, suffix)) = ip_and_suffix.split_once(']') else {
            return Err(WebTransportError::InvalidRequest);
        };
        ip.parse::<std::net::Ipv6Addr>().map_err(|_| WebTransportError::InvalidRequest)?;
        if suffix.is_empty() {
            None
        } else {
            Some(suffix.strip_prefix(':').ok_or(WebTransportError::InvalidRequest)?)
        }
    } else {
        let (host, port) = authority.split_once(':').map_or((authority, None), |(host, port)| (host, Some(port)));
        if host.is_empty() || host.contains(['[', ']']) {
            return Err(WebTransportError::InvalidRequest);
        }
        port
    };
    if let Some(port) = port
        && (!port.bytes().all(|b| b.is_ascii_digit()) || !matches!(port.parse::<u16>(), Ok(1..)))
    {
        return Err(WebTransportError::InvalidRequest);
    }
    Ok(())
}

pub(crate) fn unsupported<T>(ctx: &OpContext) -> WebTransportResult<T> {
    ctx.check().map_err(|error| match error.category {
        ErrorCategory::Cancelled => WebTransportError::Cancelled,
        ErrorCategory::Timeout => WebTransportError::Timeout,
        _ => WebTransportError::Io,
    })?;
    Err(WebTransportError::Unsupported)
}
