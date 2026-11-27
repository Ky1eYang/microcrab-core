//! Universal provider transport configuration.
use crate::error::Error;
use crate::protocol::Protocol;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

pub const MAX_CUSTOM_HEADERS: usize = 64;
pub const MAX_CUSTOM_HEADER_VALUE_BYTES: usize = 8192;

fn is_reserved_header(name: &reqwest::header::HeaderName) -> bool {
    matches!(
        name.as_str(),
        "host"
            | "content-length"
            | "transfer-encoding"
            | "connection"
            | "content-type"
            | "accept"
            | "authorization"
            | "x-api-key"
            | "x-goog-api-key"
            | "anthropic-version"
    )
}

pub fn parse_custom_headers(
    headers: &BTreeMap<String, String>,
) -> Result<Vec<(reqwest::header::HeaderName, reqwest::header::HeaderValue)>, Error> {
    if headers.len() > MAX_CUSTOM_HEADERS {
        return Err(Error::config("provider custom header count exceeds 64"));
    }
    let mut parsed = Vec::with_capacity(headers.len());
    let mut seen = HashSet::with_capacity(headers.len());
    for (raw_name, raw_value) in headers {
        if raw_name.is_empty() || raw_name != raw_name.trim() {
            return Err(Error::config("invalid custom provider header name"));
        }
        let name = reqwest::header::HeaderName::from_bytes(raw_name.as_bytes())
            .map_err(|_| Error::config("invalid custom provider header name"))?;
        if is_reserved_header(&name) {
            return Err(Error::config(
                "custom provider headers cannot override reserved protocol or auth headers",
            ));
        }
        if !seen.insert(name.clone()) {
            return Err(Error::config(
                "duplicate custom provider header name (header names are case-insensitive)",
            ));
        }
        if raw_value.is_empty() || raw_value.len() > MAX_CUSTOM_HEADER_VALUE_BYTES {
            return Err(Error::config("invalid custom provider header value"));
        }
        let value = reqwest::header::HeaderValue::from_str(raw_value)
            .map_err(|_| Error::config("invalid custom provider header value"))?;
        parsed.push((name, value));
    }
    Ok(parsed)
}

pub fn apply_custom_headers(
    mut builder: reqwest::RequestBuilder,
    headers: &[(reqwest::header::HeaderName, reqwest::header::HeaderValue)],
) -> reqwest::RequestBuilder {
    for (name, value) in headers {
        builder = builder.header(name, value);
    }
    builder
}

/// Credential placement is independent of the gateway's wire protocol.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AuthMode {
    #[default]
    Native,
    Bearer,
}

impl AuthMode {
    /// Apply only known authentication headers; callers retain endpoint/body ownership.
    pub fn apply(
        self,
        mut builder: reqwest::RequestBuilder,
        protocol: Protocol,
        api_key: Option<&str>,
    ) -> reqwest::RequestBuilder {
        if protocol == Protocol::Anthropic {
            builder = builder.header("anthropic-version", "2023-06-01");
        }
        let Some(key) = api_key else { return builder };
        match (self, protocol) {
            (Self::Native, Protocol::Anthropic) => builder.header("x-api-key", key),
            (Self::Native, Protocol::Google) => builder.header("x-goog-api-key", key),
            _ => builder.bearer_auth(key),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TransportConfig {
    #[serde(default)]
    pub auth: AuthMode,
    #[serde(default = "default_protocol")]
    pub protocol: Protocol,
    pub endpoint: String,
    pub model: String,
    #[serde(default)]
    pub api_key: Option<String>,
    /// Maximum network inactivity, renewed by every received chunk; no total cap.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Absolute safety ceiling for one provider response. It exists only to stop an
    /// unbounded body from exhausting memory; a response that approaches it is
    /// warned, not rejected. Ordinary large responses are accepted.
    #[serde(default = "default_max_response_bytes")]
    pub max_response_bytes: usize,
    /// Optional validated custom HTTP request headers applied before protocol/auth headers.
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}

fn default_protocol() -> Protocol {
    Protocol::OpenaiCompatible
}

fn default_timeout_ms() -> u64 {
    60_000
}

fn default_max_response_bytes() -> usize {
    1024 * 1024 * 1024
}

impl std::fmt::Debug for TransportConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransportConfig")
            .field("protocol", &self.protocol)
            .field("auth", &self.auth)
            .field("endpoint", &"<configured>")
            .field("model", &self.model)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field("timeout_ms", &self.timeout_ms)
            .field("max_response_bytes", &self.max_response_bytes)
            .field(
                "headers",
                &self.headers.keys().map(String::as_str).collect::<Vec<_>>(),
            )
            .finish()
    }
}

/// Append a native resource suffix to the URL path without changing route queries.
/// Endpoint admission remains with the calling configuration/adapter validator.
pub(crate) fn resolve_resource_endpoint(endpoint: &str, suffix: &str) -> Result<String, Error> {
    let mut url = reqwest::Url::parse(endpoint.trim())
        .map_err(|_| Error::config("invalid provider endpoint"))?;
    let path = url.path().trim_end_matches('/');
    let path = if path.ends_with(suffix) {
        path.to_owned()
    } else {
        format!("{path}{suffix}")
    };
    url.set_path(&path);
    Ok(url.into())
}

impl TransportConfig {
    /// Complete streaming request endpoint. It can contain private route data;
    /// Creator hashes this identity and never exposes it in public model choices.
    pub fn request_endpoint(&self) -> Result<reqwest::Url, Error> {
        let endpoint = match self.protocol {
            Protocol::OpenaiCompatible => self.endpoint.clone(),
            Protocol::OpenaiResponses => crate::openai_response::resolve_endpoint(&self.endpoint)?,
            Protocol::Anthropic => crate::anthropic::resolve_endpoint(&self.endpoint)?,
            Protocol::Google => {
                return crate::google::resolve_endpoint(&self.endpoint, &self.model, true);
            }
        };
        reqwest::Url::parse(&endpoint).map_err(|_| Error::config("invalid provider endpoint"))
    }

    pub fn validate(&self) -> Result<(), Error> {
        if let Some(key) = &self.api_key {
            let header = if self.auth == AuthMode::Bearer
                || matches!(
                    self.protocol,
                    Protocol::OpenaiCompatible | Protocol::OpenaiResponses
                ) {
                format!("Bearer {key}")
            } else {
                key.clone()
            };
            reqwest::header::HeaderValue::from_str(&header)
                .map_err(|_| Error::config("invalid API credential header"))?;
        }
        let url = reqwest::Url::parse(&self.endpoint)
            .map_err(|_| Error::config("invalid provider endpoint"))?;
        let local = matches!(
            url.host_str(),
            Some("127.0.0.1" | "localhost" | "[::1]" | "::1")
        ) || url
            .host_str()
            .and_then(|h| h.parse::<std::net::IpAddr>().ok())
            .is_some_and(|ip| match ip {
                std::net::IpAddr::V4(ipv4) => ipv4.is_loopback() || ipv4.is_private(),
                std::net::IpAddr::V6(ipv6) => ipv6.is_loopback(),
            });
        if (!matches!(url.scheme(), "https") && !(url.scheme() == "http" && local))
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || url.host_str().is_none()
        {
            return Err(Error::config(
                "provider endpoint requires HTTPS (or loopback HTTP), without credentials or fragment",
            ));
        }
        if self.model.trim().is_empty() {
            return Err(Error::config("provider model must be nonempty"));
        }
        if self.timeout_ms == 0 {
            return Err(Error::config(
                "provider inactivity timeout must be positive",
            ));
        }
        if self.max_response_bytes == 0 {
            return Err(Error::config(
                "provider response byte budget must be positive",
            ));
        }
        parse_custom_headers(&self.headers)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(protocol: Protocol, endpoint: &str) -> TransportConfig {
        TransportConfig {
            auth: AuthMode::Native,
            protocol,
            endpoint: endpoint.into(),
            model: "test-model".into(),
            api_key: None,
            timeout_ms: 1000,
            max_response_bytes: 1024,
            headers: BTreeMap::new(),
        }
    }

    #[test]
    fn custom_headers_validate_reserved_case_duplicates_and_redacted_debug() {
        let mut ok = config(Protocol::OpenaiCompatible, "https://gateway.invalid/v1");
        ok.headers.insert("User-Agent".into(), "pi".into());
        ok.headers.insert("X-Custom-Tag".into(), "alpha".into());
        ok.validate().unwrap();
        let debug = format!("{ok:?}");
        assert!(debug.contains("User-Agent"));
        assert!(!debug.contains("alpha"));

        for (name, value) in [
            ("Authorization", "Bearer leaked"),
            ("host", "evil.invalid"),
            ("Content-Type", "text/plain"),
            ("x-api-key", "k"),
            ("x-goog-api-key", "k"),
            ("anthropic-version", "2024-01-01"),
            ("Bad Header", "v"),
            ("X-Test", "line\r\ninjection"),
            ("X-Empty", ""),
        ] {
            let mut bad = config(Protocol::OpenaiCompatible, "https://gateway.invalid/v1");
            bad.headers.insert(name.into(), value.into());
            assert!(bad.validate().is_err(), "{name}");
        }

        let mut dup = config(Protocol::OpenaiCompatible, "https://gateway.invalid/v1");
        dup.headers.insert("User-Agent".into(), "pi".into());
        dup.headers.insert("user-agent".into(), "other".into());
        assert!(dup.validate().is_err());
    }

    #[test]
    fn native_resource_endpoints_preserve_queries_for_base_and_complete_paths() {
        for (protocol, suffix) in [
            (Protocol::Anthropic, "/messages"),
            (Protocol::OpenaiResponses, "/responses"),
        ] {
            for path in ["/v1".to_owned(), format!("/v1{suffix}")] {
                for trailing in ["", "/"] {
                    let endpoint = format!(
                        "https://gateway.invalid{path}{trailing}?api-version=2025&route=a%2Fb&route=c"
                    );
                    let config = config(protocol, &endpoint);
                    config.validate().unwrap();
                    let resolved = config.request_endpoint().unwrap();
                    assert_eq!(resolved.path(), format!("/v1{suffix}"));
                    assert_eq!(
                        resolved.query(),
                        Some("api-version=2025&route=a%2Fb&route=c")
                    );
                }
            }
        }
    }

    #[test]
    fn native_resource_resolution_does_not_relax_endpoint_admission() {
        for protocol in [Protocol::Anthropic, Protocol::OpenaiResponses] {
            for endpoint in [
                "https://user:secret@gateway.invalid/v1?route=a",
                "https://gateway.invalid/v1?route=a#fragment",
                "http://gateway.invalid/v1?route=a",
            ] {
                let config = config(protocol, endpoint);
                assert!(config.validate().is_err());
                assert!(crate::TransportClient::new(config).is_err());
            }
        }
    }
}
