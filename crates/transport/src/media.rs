//! Ephemeral, explicitly authorized URL projections of canonical tool Data.
use crate::{Content, ContentItem, Error};
use microcrab_protocol::{ImageSource, InputPart};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime},
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

const MAX_HINTS: usize = 128;
const MAX_URL_BYTES: usize = 8192;
const MAX_QUERY_BYTES: usize = 4096;
const MAX_GRANT_IMAGES: usize = 20;

/// Trusted embedding identity for one exact route/model and authorization domain.
/// These opaque labels must not contain credentials and are never sent or logged.
#[derive(Clone, PartialEq, Eq)]
pub struct ImageHostingScope {
    pub route: String,
    pub authority: String,
}

/// Explicit opt-in; use only for a provider/model verified to accept tool image URLs.
pub struct ImageHostingConfig {
    pub scope: ImageHostingScope,
    /// Dedicated hosting DNS names, not authenticated browser/server origins.
    pub allowed_hosts: Vec<String>,
    /// Longest acceptable remaining URL lifetime at upload return, up to one hour.
    pub max_hint_ttl: Duration,
}

/// The uploader owns a copy of the validated bytes, not the Context's backing.
pub struct ImageUpload {
    pub media_type: String,
    pub data: Vec<u8>,
}

pub struct HostedImageHint {
    /// A potentially sensitive bearer URL; never print or persist it.
    pub url: String,
    pub expires_at: SystemTime,
    pub revocation: ImageHintRevocation,
}

pub enum ImageHintRevocation {
    ExpiresOnly,
    /// The uploader signals revocation by cancelling this token. This stops Core
    /// from reusing the hint; the uploader owns external revocation and deletion.
    Revocable(CancellationToken),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImageUploadError {
    Cancelled,
    Failed,
    /// The uploader must reconcile a possible side effect before resubmission.
    OutcomeUnknown,
}

/// Embedder-supplied uploader. `Ok(None)` means no URL projection is available.
/// Cancellation can drop this future and is not evidence of external cleanup.
#[async_trait::async_trait]
pub trait ImageUrlResolver: Send + Sync {
    async fn upload(
        &self,
        scope: &ImageHostingScope,
        image: ImageUpload,
        cancellation: CancellationToken,
    ) -> Result<Option<HostedImageHint>, ImageUploadError>;
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ToolImagePosition {
    pub item: usize,
    pub part: usize,
}
impl ToolImagePosition {
    pub fn data_in(self, content: &Content) -> Option<(&str, &[u8])> {
        let ContentItem::Tool { parts, .. } = content.items.get(self.item)? else {
            return None;
        };
        match parts.get(self.part)? {
            InputPart::Image {
                source: ImageSource::Data { media_type, data },
            } => Some((media_type, data)),
            _ => None,
        }
    }
}

/// Single-use, non-serializable authorization pinned to an immutable request and
/// exact projection instance. A trusted caller must establish fresh provenance
/// before constructing this grant. Coordinates alone are not that provenance.
pub struct ImageHostingGrant<'a> {
    projection: &'a MediaProjection,
    content: &'a Content,
    positions: BTreeSet<ToolImagePosition>,
}
impl<'a> ImageHostingGrant<'a> {
    pub fn content(&self) -> &'a Content {
        self.content
    }
}

/// Bounded, instance-local cache (max 128 metadata records, no image bytes).
/// Clones of a single configured client can share it; separately configured
/// routes or authorization domains never share it.
pub struct MediaProjection {
    resolver: Arc<dyn ImageUrlResolver>,
    config: ImageHostingConfig,
    cache: Mutex<BTreeMap<[u8; 32], Arc<CachedHint>>>,
}

struct CachedHint {
    offer: HostedImageHint,
    invalidated: AtomicBool,
}
impl CachedHint {
    fn live(&self, now: SystemTime) -> bool {
        !self.invalidated.load(Ordering::Acquire)
            && self.offer.expires_at > now
            && match &self.offer.revocation {
                ImageHintRevocation::ExpiresOnly => true,
                ImageHintRevocation::Revocable(token) => !token.is_cancelled(),
            }
    }
}

/// A provider-only copy. Avoid logging even its Debug (it contains bearer URLs).
pub struct ProjectedMedia {
    content: Content,
    hints: Vec<Arc<CachedHint>>,
}
impl ProjectedMedia {
    /// Select before each attempt, including after a general retry backoff. Do
    /// not re-upload or switch to a newer hint within the same logical call.
    pub fn content_if_live(&self) -> Option<&Content> {
        let now = SystemTime::now();
        self.hints
            .iter()
            .all(|hint| hint.live(now))
            .then_some(&self.content)
    }

    /// Providers need not identify which image failed. Invalidate only the
    /// actual hint generations sent, never a newer entry for the same digest.
    pub fn invalidate(&self) {
        for hint in &self.hints {
            hint.invalidated.store(true, Ordering::Release);
        }
    }
}

impl MediaProjection {
    pub fn new(
        config: ImageHostingConfig,
        resolver: Arc<dyn ImageUrlResolver>,
    ) -> Result<Self, Error> {
        let valid_id = |id: &str| {
            !id.trim().is_empty() && id.len() <= 256 && !id.chars().any(char::is_control)
        };
        if !valid_id(&config.scope.route) || !valid_id(&config.scope.authority) {
            return Err(Error::config(
                "image hosting requires bounded route and authority identities",
            ));
        }
        if config.allowed_hosts.is_empty()
            || config.allowed_hosts.len() > 32
            || !config.allowed_hosts.iter().all(|host| valid_host(host))
        {
            return Err(Error::config(
                "image hosting requires 1..=32 exact DNS hostnames",
            ));
        }
        if config.max_hint_ttl.is_zero() || config.max_hint_ttl > Duration::from_secs(3600) {
            return Err(Error::config(
                "image hosting hint lifetime must be positive and at most one hour",
            ));
        }
        Ok(Self {
            resolver,
            config,
            cache: Mutex::new(BTreeMap::new()),
        })
    }

    pub fn authorize<'a>(
        &'a self,
        scope: &ImageHostingScope,
        content: &'a Content,
        positions: &[ToolImagePosition],
    ) -> Result<ImageHostingGrant<'a>, Error> {
        if *scope != self.config.scope {
            return Err(Error::config("image hosting route or authority mismatch"));
        }
        if positions.is_empty() || positions.len() > MAX_GRANT_IMAGES {
            return Err(Error::config(
                "image hosting requires 1..=20 explicit tool image positions",
            ));
        }
        let mut total = 0usize;
        for position in positions {
            let (_, data) = position.data_in(content).ok_or_else(|| {
                Error::config("image hosting position must identify canonical tool image Data")
            })?;
            total = total.saturating_add(data.len());
        }
        if total > content.max_bytes {
            return Err(Error::Limit("image hosting input byte budget".into()));
        }
        let unique: BTreeSet<_> = positions.iter().copied().collect();
        if unique.len() != positions.len() {
            return Err(Error::config(
                "image hosting grant contains duplicate tool image positions",
            ));
        }
        Ok(ImageHostingGrant {
            projection: self,
            content,
            positions: unique,
        })
    }

    /// None means the caller should use its original Content without changes.
    pub async fn project_tool_images(
        &self,
        grant: ImageHostingGrant<'_>,
        cancellation: CancellationToken,
    ) -> Result<Option<ProjectedMedia>, Error> {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if !std::ptr::eq(self, grant.projection) {
            return Err(Error::config(
                "image hosting grant belongs to another configured client",
            ));
        }
        let mut replacements = BTreeMap::new();
        let mut attempted = BTreeMap::new();
        for position in grant.positions {
            let Some((media_type, data)) = position.data_in(grant.content) else {
                return Err(Error::config(
                    "image hosting grant lost its canonical image",
                ));
            };
            let mut digest = Sha256::new();
            digest.update((media_type.len() as u64).to_be_bytes());
            digest.update(media_type.as_bytes());
            digest.update(data);
            let key = digest.finalize().into();
            let hint = if let Some(previous) = attempted.get(&key) {
                Option::<Arc<CachedHint>>::clone(previous)
            } else {
                let hint = self.hint(key, media_type, data, &cancellation).await?;
                attempted.insert(key, hint.clone());
                hint
            };
            if let Some(hint) = hint {
                replacements.insert(position, hint);
            }
        }
        if replacements.is_empty() {
            return Ok(None);
        }
        // Clone only for an actual URL projection, and never copy the bytes of
        // the images being replaced into that projection.
        let items = grant
            .content
            .items
            .iter()
            .enumerate()
            .map(|(item, value)| match value {
                ContentItem::Tool {
                    call_id,
                    content,
                    parts,
                } => ContentItem::Tool {
                    call_id: call_id.clone(),
                    content: content.clone(),
                    parts: parts
                        .iter()
                        .enumerate()
                        .map(|(part, value)| {
                            replacements
                                .get(&ToolImagePosition { item, part })
                                .map_or_else(
                                    || value.clone(),
                                    |hint| InputPart::Image {
                                        source: ImageSource::Url {
                                            url: hint.offer.url.clone(),
                                        },
                                    },
                                )
                        })
                        .collect(),
                },
                value => value.clone(),
            })
            .collect();
        let Content {
            epoch,
            subagent,
            system,
            tools,
            max_output_tokens,
            max_bytes,
            turn_id,
            round,
            ..
        } = grant.content;
        let content = Content {
            epoch: *epoch,
            subagent: subagent.clone(),
            system: system.clone(),
            items,
            tools: tools.clone(),
            max_output_tokens: *max_output_tokens,
            max_bytes: *max_bytes,
            turn_id: *turn_id,
            round: *round,
        };
        Ok(Some(ProjectedMedia {
            content,
            hints: replacements.into_values().collect(),
        }))
    }

    async fn hint(
        &self,
        key: [u8; 32],
        media_type: &str,
        data: &[u8],
        cancellation: &CancellationToken,
    ) -> Result<Option<Arc<CachedHint>>, Error> {
        // One cancellation-aware upload at a time per scope. Rejection can
        // invalidate atomically without waiting behind another upload.
        let mut cache = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(Error::Cancelled),
            cache = self.cache.lock() => cache,
        };
        let now = SystemTime::now();
        cache.retain(|_, hint| hint.live(now));
        if let Some(hint) = cache.get(&key) {
            return Ok(Some(hint.clone()));
        }
        if cache.len() >= MAX_HINTS {
            return Ok(None);
        }
        let upload = ImageUpload {
            media_type: media_type.to_owned(),
            data: data.to_vec(),
        };
        let uploaded = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(Error::Cancelled),
            result = self.resolver.upload(&self.config.scope, upload, cancellation.clone()) => result.map_err(|error| match error {
                ImageUploadError::Cancelled => Error::Cancelled,
                ImageUploadError::Failed => Error::protocol("image hosting upload failed"),
                ImageUploadError::OutcomeUnknown => Error::protocol("image hosting upload outcome is unknown"),
            })?,
        };
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let now = SystemTime::now();
        let hint = uploaded
            .filter(|hint| {
                valid_hint(&hint.url, &self.config.allowed_hosts)
                    && hint
                        .expires_at
                        .duration_since(now)
                        .is_ok_and(|ttl| ttl <= self.config.max_hint_ttl)
            })
            .map(|offer| {
                Arc::new(CachedHint {
                    offer,
                    invalidated: AtomicBool::new(false),
                })
            })
            .filter(|hint| hint.live(now));
        if let Some(hint) = &hint {
            cache.insert(key, hint.clone());
        }
        Ok(hint)
    }
}

fn valid_host(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 253
        && !value.ends_with('.')
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
        })
        && reqwest::Url::parse(&format!("https://{value}/"))
            .is_ok_and(|url| url.domain() == Some(value))
}

fn valid_hint(value: &str, allowed_hosts: &[String]) -> bool {
    value.len() <= MAX_URL_BYTES
        && reqwest::Url::parse(value).is_ok_and(|url| {
            url.as_str() == value
                && url.scheme() == "https"
                && url
                    .domain()
                    .is_some_and(|host| allowed_hosts.iter().any(|allowed| allowed == host))
                && url.port().is_none()
                && url.username().is_empty()
                && url.password().is_none()
                && url.fragment().is_none()
                && !url.path().contains('%')
                && !url.path().to_ascii_lowercase().starts_with("/api/v1/")
                && !url.path().eq_ignore_ascii_case("/api/v1")
                && url
                    .query()
                    .is_none_or(|query| query.len() <= MAX_QUERY_BYTES)
        })
}

/// Only an explicit machine-readable fetch rejection is retry evidence. Body text,
/// generic statuses, invalid URLs and authentication failures never qualify.
pub(crate) fn remote_image_fetch_rejection(
    status: reqwest::StatusCode,
    body: &[u8],
) -> Option<Error> {
    if !matches!(status.as_u16(), 400 | 422) {
        return None;
    }
    let value: serde_json::Value = serde_json::from_slice(body).ok()?;
    fetch_rejection_at(&value, "/error")
}

/// OpenAI SSE callers pass only recognized error envelopes, never arbitrary
/// response messages or incomplete events.
pub(crate) fn in_band_fetch_rejection(value: &serde_json::Value) -> Option<Error> {
    fetch_rejection_at(value, "/error").or_else(|| fetch_rejection_at(value, "/response/error"))
}

fn fetch_rejection_at(value: &serde_json::Value, at: &str) -> Option<Error> {
    let error = value.pointer(at)?;
    let code = error.get("code")?.as_str()?;
    let permission_error = error
        .get("type")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|kind| matches!(kind, "authentication_error" | "permission_error"));
    let other_status = error
        .get("status")
        .and_then(|status| status.as_u64().or_else(|| status.as_str()?.parse().ok()))
        .is_some_and(|status| !matches!(status, 400 | 422));
    (code == "failed_to_download_image" && !permission_error && !other_status)
        .then(Error::remote_image_fetch_rejected)
}

#[cfg(test)]
mod tests;
