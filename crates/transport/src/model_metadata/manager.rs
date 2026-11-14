use super::{MetadataError, MetadataSnapshot, ModelMatch, ModelMetadata};
use async_trait::async_trait;
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;

/// No authentication or provider preferences: this is the entire public market.
pub const OPENROUTER_MODELS_ENDPOINT: &str =
    "https://openrouter.ai/api/v1/models?output_modalities=all";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_BODY_BYTES: usize = 16 * 1024 * 1024;
const MAX_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

/// Reusable and object-safe. Scheduling, durable storage, and stale-data policy
/// belong to the embedding application, not to the transport.
#[async_trait]
pub trait ModelMetadataManager: Send + Sync {
    fn snapshot(&self) -> Arc<MetadataSnapshot>;
    fn find(&self, provider: &str, model: &str) -> Option<ModelMatch>;
    async fn refresh(&self) -> Result<Arc<MetadataSnapshot>, MetadataError>;
}

struct RefreshState {
    last_result: Option<Result<Arc<MetadataSnapshot>, MetadataError>>,
}

/// Readers only clone an immutable Arc. The asynchronous refresh gate coalesces
/// callers that observed the same generation, including failed refreshes. A
/// cancelled leader releases the gate without publishing a partial snapshot;
/// the next waiting caller may retry. Network I/O never holds the read lock.
pub struct OpenRouterModelManager {
    client: reqwest::Client,
    endpoint: reqwest::Url,
    timeout: Duration,
    max_body_bytes: usize,
    snapshot: RwLock<Arc<MetadataSnapshot>>,
    generation: AtomicU64,
    refresh: Mutex<RefreshState>,
}

impl OpenRouterModelManager {
    pub fn new() -> Result<Self, MetadataError> {
        Self::with_endpoint(
            OPENROUTER_MODELS_ENDPOINT,
            DEFAULT_TIMEOUT,
            DEFAULT_MAX_BODY_BYTES,
        )
    }

    /// Explicit endpoint injection permits HTTPS and numeric loopback HTTP only.
    /// Redirects, proxies, userinfo, fragments, and authentication query keys
    /// are rejected/disabled; the client never receives inference credentials.
    pub fn with_endpoint(
        endpoint: &str,
        timeout: Duration,
        max_body_bytes: usize,
    ) -> Result<Self, MetadataError> {
        let endpoint = validate_endpoint(endpoint)?;
        if timeout.is_zero()
            || timeout > MAX_TIMEOUT
            || max_body_bytes == 0
            || max_body_bytes > MAX_BODY_BYTES
        {
            return Err(MetadataError::InvalidConfiguration);
        }
        let client = crate::http::client_builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .timeout(timeout)
            .build()
            .map_err(|_| MetadataError::InvalidConfiguration)?;
        let snapshot = Arc::new(MetadataSnapshot {
            version: 1,
            source: endpoint.as_str().to_owned(),
            fetched_at_ms: 0,
            models: Vec::new(),
            extra: BTreeMap::new(),
        });
        Ok(Self {
            client,
            endpoint,
            timeout,
            max_body_bytes,
            snapshot: RwLock::new(snapshot),
            generation: AtomicU64::new(0),
            refresh: Mutex::new(RefreshState { last_result: None }),
        })
    }

    /// Restore only a validated snapshot of this public all-market source.
    /// Persistence remains private to the embedding server.
    pub fn with_cached_snapshot(snapshot: MetadataSnapshot) -> Result<Self, MetadataError> {
        snapshot.validate()?;
        if snapshot.source != OPENROUTER_MODELS_ENDPOINT {
            return Err(MetadataError::InvalidSnapshot);
        }
        let mut manager = Self::new()?;
        manager.snapshot = RwLock::new(Arc::new(snapshot));
        Ok(manager)
    }

    async fn fetch(&self) -> Result<Arc<MetadataSnapshot>, MetadataError> {
        let response = self
            .client
            .get(self.endpoint.clone())
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await
            .map_err(request_error)?;
        if !response.status().is_success() {
            return Err(MetadataError::HttpStatus);
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.max_body_bytes as u64)
        {
            return Err(MetadataError::BodyTooLarge);
        }
        let mut bytes = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(request_error)?;
            if chunk.len() > self.max_body_bytes.saturating_sub(bytes.len()) {
                return Err(MetadataError::BodyTooLarge);
            }
            bytes.extend_from_slice(&chunk);
        }
        let envelope: Envelope =
            serde_json::from_slice(&bytes).map_err(|_| MetadataError::InvalidResponse)?;
        // A partial paginated response cannot be published as a full market.
        if envelope
            .extra
            .get("links")
            .and_then(|links| links.get("next"))
            .is_some_and(|next| !next.is_null())
            || envelope
                .extra
                .get("total_count")
                .is_some_and(|count| count.as_u64() != Some(envelope.data.len() as u64))
        {
            return Err(MetadataError::InvalidResponse);
        }
        let fetched_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| MetadataError::InvalidResponse)?
            .as_millis();
        let fetched_at_ms =
            u64::try_from(fetched_at_ms).map_err(|_| MetadataError::InvalidResponse)?;
        let snapshot = MetadataSnapshot {
            version: 1,
            source: self.endpoint.as_str().to_owned(),
            fetched_at_ms,
            models: envelope.data,
            extra: envelope.extra,
        };
        snapshot
            .validate()
            .map_err(|_| MetadataError::InvalidResponse)?;
        Ok(Arc::new(snapshot))
    }
}

#[async_trait]
impl ModelMetadataManager for OpenRouterModelManager {
    fn snapshot(&self) -> Arc<MetadataSnapshot> {
        // Only whole-Arc replacement occurs under this lock, so recovering a
        // poisoned guard cannot expose an incompletely mutated snapshot.
        self.snapshot
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn find(&self, provider: &str, model: &str) -> Option<ModelMatch> {
        self.snapshot().find(provider, model)
    }

    async fn refresh(&self) -> Result<Arc<MetadataSnapshot>, MetadataError> {
        let generation = self.generation.load(Ordering::Acquire);
        let mut refresh = self.refresh.lock().await;
        if self.generation.load(Ordering::Acquire) != generation
            && let Some(result) = &refresh.last_result
        {
            return result.clone();
        }
        let result = tokio::time::timeout(self.timeout, self.fetch())
            .await
            .map_err(|_| MetadataError::TimedOut)
            .and_then(|result| result);
        if let Ok(snapshot) = &result {
            *self
                .snapshot
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = snapshot.clone();
        }
        refresh.last_result = Some(result.clone());
        self.generation.fetch_add(1, Ordering::Release);
        result
    }
}

#[derive(Deserialize)]
pub(super) struct Envelope {
    pub(super) data: Vec<ModelMetadata>,
    #[serde(flatten)]
    pub(super) extra: BTreeMap<String, Value>,
}

fn request_error(error: reqwest::Error) -> MetadataError {
    if error.is_timeout() {
        MetadataError::TimedOut
    } else {
        MetadataError::RequestFailed
    }
}

pub(super) fn validate_endpoint(endpoint: &str) -> Result<reqwest::Url, MetadataError> {
    let url = reqwest::Url::parse(endpoint).map_err(|_| MetadataError::InvalidConfiguration)?;
    let loopback = url
        .host_str()
        .and_then(|host| {
            host.trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .ok()
        })
        .is_some_and(|ip| ip.is_loopback());
    if url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || !(url.scheme() == "https" || (url.scheme() == "http" && loopback))
        || url
            .query_pairs()
            .any(|(key, value)| key != "output_modalities" || value != "all")
    {
        return Err(MetadataError::InvalidConfiguration);
    }
    Ok(url)
}
