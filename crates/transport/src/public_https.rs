//! Public HTTPS image transfer, separate from provider clients and image hosting.
//!
//! Session owns authorization, ordering, one-shot effect settlement, and canonical
//! raster admission. A target is a destination pin, not an authorization grant.
use reqwest::{StatusCode, Url, header, redirect, retry};
use std::{fmt, future::Future, net::SocketAddr, time::Duration};
use tokio::{sync::Semaphore, time::timeout};
use tokio_util::sync::CancellationToken;

mod destination;
use destination::{PinnedAddresses, resolve};
#[cfg(test)]
mod tests;

const MAX_URL_BYTES: usize = 8192;
const MAX_QUERY_BYTES: usize = 4096;
const MAX_TRANSFER_BYTES: usize = 20 * 1024 * 1024;
const DNS_TIMEOUT: Duration = Duration::from_secs(4);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(4);
const FETCH_TIMEOUT: Duration = Duration::from_secs(15);
static FETCH_SLOTS: Semaphore = Semaphore::const_new(8);

/// Safe to report; never embeds URLs, response values/bodies, or DNS diagnostics.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PublicHttpsError {
    #[error("image URL exceeds its length limit")]
    UrlLimit,
    #[error("image URL must be an absolute HTTPS URL without credentials or a fragment")]
    InvalidUrl,
    #[error("image URL destination is not public")]
    NonPublic,
    #[error("image URL destination resolution failed")]
    Resolution,
    #[error("image URL destination resolved to too many addresses")]
    AddressLimit,
    #[error("image URL destination resolution timed out")]
    ResolutionTimeout,
    #[error("image URL destination addresses changed after admission")]
    DestinationChanged,
    #[error("image fetch is at its concurrency limit")]
    Busy,
    #[error("image fetch was cancelled")]
    Cancelled,
    #[error("image fetch timed out")]
    Timeout,
    #[error("image fetch could not initialize HTTPS")]
    Client,
    #[error("image fetch could not establish a verified HTTPS connection")]
    Connect,
    #[error("image fetch failed before receiving response headers")]
    Response,
    #[error("image fetch connection did not match the public destination pin")]
    PeerMismatch,
    #[error("image fetch redirects are not allowed")]
    Redirect,
    #[error("image fetch returned HTTP {0}")]
    HttpStatus(u16),
    #[error("image fetch requires a valid image Content-Type")]
    ContentType,
    #[error("image fetch does not accept HTTP content encoding")]
    ContentEncoding,
    #[error("image fetch returned unsupported or ambiguous HTTP transfer framing")]
    TransferEncoding,
    #[error("image fetch body exceeds the transfer byte limit")]
    BodyLimit,
    #[error("image fetch body could not be read completely")]
    BodyRead,
    #[error("image fetch returned an empty body")]
    EmptyBody,
    #[error("image fetch could not reserve its bounded body buffer")]
    Buffer,
}

/// A normalized, bounded HTTPS source. Only explicit `as_url()` exposes the
/// sensitive path and query. Do not log or persist that value.
#[derive(Clone, PartialEq, Eq)]
pub struct PublicHttpsUrl(Url);

impl fmt::Debug for PublicHttpsUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("PublicHttpsUrl")
            .field(&self.destination())
            .finish()
    }
}

impl PublicHttpsUrl {
    /// Validate URL shape without DNS or network traffic.
    pub fn parse(value: &str) -> Result<Self, PublicHttpsError> {
        if value.len() > MAX_URL_BYTES {
            return Err(PublicHttpsError::UrlLimit);
        }
        let Some((scheme, rest)) = value.split_once("://") else {
            return Err(PublicHttpsError::InvalidUrl);
        };
        let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
        // Reject ambiguous input before WHATWG URL parsing can normalize it.
        if !scheme.eq_ignore_ascii_case("https")
            || authority.is_empty()
            || authority.contains('@')
            || value.bytes().any(|b| b <= b' ' || b == 0x7f || b == b'\\')
            || value.as_bytes().iter().enumerate().any(|(i, &byte)| {
                byte == b'%'
                    && !value
                        .as_bytes()
                        .get(i + 1..i + 3)
                        .is_some_and(|escape| escape.iter().all(u8::is_ascii_hexdigit))
            })
        {
            return Err(PublicHttpsError::InvalidUrl);
        }
        let url = Url::parse(value).map_err(|_| PublicHttpsError::InvalidUrl)?;
        if url.scheme() != "https"
            || url.host().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || url.port() == Some(0)
        {
            return Err(PublicHttpsError::InvalidUrl);
        }
        if url.as_str().len() > MAX_URL_BYTES
            || url
                .query()
                .is_some_and(|query| query.len() > MAX_QUERY_BYTES)
        {
            return Err(PublicHttpsError::UrlLimit);
        }
        destination::check_host(&url)?;
        Ok(Self(url))
    }

    /// Origin only, for approval and diagnostics. Never includes path or query.
    pub fn destination(&self) -> String {
        self.0.origin().ascii_serialization()
    }

    /// Full source for the authorized provider disclosure; sensitive.
    pub fn as_url(&self) -> &Url {
        &self.0
    }

    /// Validate the full DNS answer set before provider disclosure. This does not
    /// perform a GET. For a literal address, admission requires no DNS.
    pub async fn admit(
        &self,
        cancel: &CancellationToken,
    ) -> Result<PublicHttpsTarget, PublicHttpsError> {
        let addresses = resolve(&self.0, cancel).await?;
        Ok(PublicHttpsTarget {
            url: self.clone(),
            addresses,
        })
    }
}

/// Public DNS admission witness. It is consumed by one fetch and is not serializable.
#[derive(Debug)]
pub struct PublicHttpsTarget {
    url: PublicHttpsUrl,
    addresses: PinnedAddresses,
}

/// Bounded encoded bytes and normalized HTTP media type, **not** a certified
/// raster. The caller must validate MIME, signature and dimensions before use.
pub struct PublicHttpsImage {
    pub mime_type: String,
    pub bytes: Vec<u8>,
}

impl PublicHttpsTarget {
    pub fn url(&self) -> &PublicHttpsUrl {
        &self.url
    }

    /// Make at most one GET, after provider submission and under the caller's live
    /// grant. Recheck DNS; never follow a newly resolved or redirected destination.
    pub async fn fetch(
        self,
        max_bytes: usize,
        cancel: &CancellationToken,
    ) -> Result<PublicHttpsImage, PublicHttpsError> {
        bounded(cancel, FETCH_TIMEOUT, PublicHttpsError::Timeout, async {
            let max_bytes = max_bytes.min(MAX_TRANSFER_BYTES);
            if max_bytes == 0 {
                return Err(PublicHttpsError::BodyLimit);
            }
            let _permit = FETCH_SLOTS
                .try_acquire()
                .map_err(|_| PublicHttpsError::Busy)?;
            let fresh = resolve(&self.url.0, cancel).await?;
            self.addresses.check_unchanged(&fresh)?;
            let client = guarded_client()
                .dns_resolver(std::sync::Arc::new(
                    self.addresses.resolver_for(&self.url.0),
                ))
                .build()
                .map_err(|_| PublicHttpsError::Client)?;
            if cancel.is_cancelled() {
                return Err(PublicHttpsError::Cancelled);
            }
            transfer(&client, self.url.0, max_bytes, |peer| {
                self.addresses.contains(peer)
            })
            .await
        })
        .await
    }
}

fn guarded_client() -> reqwest::ClientBuilder {
    crate::http::client_builder()
        .https_only(true)
        .http1_only()
        .redirect(redirect::Policy::none())
        .retry(retry::never())
        .no_proxy()
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .no_zstd()
        .pool_max_idle_per_host(0)
        .connect_timeout(CONNECT_TIMEOUT)
}

async fn bounded<T>(
    cancel: &CancellationToken,
    deadline: Duration,
    elapsed_error: PublicHttpsError,
    work: impl Future<Output = Result<T, PublicHttpsError>>,
) -> Result<T, PublicHttpsError> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(PublicHttpsError::Cancelled),
        result = timeout(deadline, work) => result.map_err(|_| elapsed_error)?,
    }
}

async fn transfer(
    client: &reqwest::Client,
    url: Url,
    max_bytes: usize,
    peer_allowed: impl FnOnce(SocketAddr) -> bool,
) -> Result<PublicHttpsImage, PublicHttpsError> {
    // This is a fresh client with one GET and no cookie store, authentication,
    // proxy, redirects, or ambient/user-provided headers.
    let mut response = client
        .get(url)
        .header(header::ACCEPT, "image/*")
        .header(header::ACCEPT_ENCODING, "identity")
        .send()
        .await
        .map_err(|error| {
            if error.is_timeout() {
                PublicHttpsError::Timeout
            } else if error.is_connect() {
                PublicHttpsError::Connect
            } else {
                PublicHttpsError::Response
            }
        })?;
    if !response.remote_addr().is_some_and(peer_allowed) {
        return Err(PublicHttpsError::PeerMismatch);
    }
    let status = response.status();
    if status.is_redirection() {
        return Err(PublicHttpsError::Redirect);
    }
    if status != StatusCode::OK {
        return Err(PublicHttpsError::HttpStatus(status.as_u16()));
    }
    let mime_type = response_content_type(response.headers())?;
    if response
        .headers()
        .get_all(header::CONTENT_ENCODING)
        .iter()
        .any(|value| !value.as_bytes().eq_ignore_ascii_case(b"identity"))
    {
        return Err(PublicHttpsError::ContentEncoding);
    }
    let encodings = response.headers().get_all(header::TRANSFER_ENCODING);
    let mut encodings = encodings.iter();
    if let Some(value) = encodings.next()
        && (!value.as_bytes().eq_ignore_ascii_case(b"chunked")
            || encodings.next().is_some()
            || response.headers().contains_key(header::CONTENT_LENGTH))
    {
        return Err(PublicHttpsError::TransferEncoding);
    }
    if response
        .content_length()
        .is_some_and(|len| len > max_bytes as u64)
    {
        return Err(PublicHttpsError::BodyLimit);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| PublicHttpsError::BodyRead)?
    {
        let required = bytes.len() + chunk.len();
        if required > max_bytes {
            return Err(PublicHttpsError::BodyLimit);
        }
        if required > bytes.capacity() {
            let next = required.max(bytes.capacity().saturating_mul(2).min(max_bytes));
            bytes
                .try_reserve_exact(next - bytes.len())
                .map_err(|_| PublicHttpsError::Buffer)?;
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.is_empty() {
        return Err(PublicHttpsError::EmptyBody);
    }
    Ok(PublicHttpsImage { mime_type, bytes })
}

fn response_content_type(headers: &header::HeaderMap) -> Result<String, PublicHttpsError> {
    let values = headers.get_all(header::CONTENT_TYPE);
    let mut values = values.iter();
    let value = values
        .next()
        .and_then(|value| value.to_str().ok())
        .ok_or(PublicHttpsError::ContentType)?;
    if values.next().is_some() {
        return Err(PublicHttpsError::ContentType);
    }
    let value = value.split(';').next().unwrap_or_default().trim();
    let (kind, subtype) = value.split_once('/').ok_or(PublicHttpsError::ContentType)?;
    if !kind.eq_ignore_ascii_case("image")
        || subtype.is_empty()
        || subtype.len() > 127
        || !subtype
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$&^_.+-".contains(&b))
    {
        return Err(PublicHttpsError::ContentType);
    }
    Ok(value.to_ascii_lowercase())
}
