//! Replaceable route hints for canonical tool image bytes. Never mutate Context.
use crate::{Content, ContentItem, Error};
use microcrab_protocol::{ImageSource, InputPart};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// Upload/hint authority supplied by the embedder, scoped to one selected route/model.
/// An unknown upload outcome must return an error; cancellation must remain Cancelled.
#[async_trait::async_trait]
pub trait ImageUrlResolver: Send + Sync {
    async fn upload(
        &self,
        media_type: &str,
        data: &[u8],
        cancellation: CancellationToken,
    ) -> Result<Option<String>, Error>;
}

/// One route/model's bounded stable hint cache. Create a new instance on route change.
/// Up to 128 digests and URLs of at most 8192 bytes are retained (about 1 MiB).
/// At capacity new images use Data, preserving stability of admitted hint entries.
/// No bytes, URLs or callback errors are logged; URLs may contain private signatures.
pub struct MediaProjection {
    resolver: Arc<dyn ImageUrlResolver>,
    supports_external_image_urls: bool,
    cache: Mutex<BTreeMap<[u8; 32], Option<String>>>,
}
impl MediaProjection {
    pub fn new(resolver: Arc<dyn ImageUrlResolver>, supports_external_image_urls: bool) -> Self {
        Self {
            resolver,
            supports_external_image_urls,
            cache: Mutex::new(BTreeMap::new()),
        }
    }

    /// Only tool Data is eligible. Existing user-image behavior stays unchanged.
    /// None means no hint was used; the caller should send its original Content.
    pub async fn project_tool_images(
        &self,
        canonical: &Content,
        cancellation: CancellationToken,
    ) -> Result<Option<Content>, Error> {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if !self.supports_external_image_urls {
            return Ok(None);
        }
        let mut projected = canonical.clone();
        let mut changed = false;
        for item in &mut projected.items {
            let ContentItem::Tool { parts, .. } = item else {
                continue;
            };
            for part in parts {
                let InputPart::Image {
                    source: ImageSource::Data { media_type, data },
                } = part
                else {
                    continue;
                };
                let mut digest = Sha256::new();
                digest.update((media_type.len() as u64).to_be_bytes());
                digest.update(media_type.as_bytes());
                digest.update(data.as_slice());
                let key: [u8; 32] = digest.finalize().into();
                // Hold the cancellation-aware lock through one upload to prevent
                // duplicate concurrent uploads. An interrupted/failed upload is not cached.
                let mut cache = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Err(Error::Cancelled),
                    cache = self.cache.lock() => cache,
                };
                let hint = if let Some(hint) = cache.get(&key) {
                    hint.clone()
                } else if cache.len() >= 128 {
                    None
                } else {
                    let uploaded = tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => return Err(Error::Cancelled),
                        result = self.resolver.upload(media_type, data, cancellation.clone()) => result?,
                    };
                    if cancellation.is_cancelled() {
                        return Err(Error::Cancelled);
                    }
                    let hint = uploaded.filter(|url| valid_hint(url));
                    cache.insert(key, hint.clone());
                    hint
                };
                if let Some(url) = hint {
                    *part = InputPart::Image {
                        source: ImageSource::Url { url },
                    };
                    changed = true;
                }
            }
        }
        Ok(changed.then_some(projected))
    }
}

fn valid_hint(value: &str) -> bool {
    value.len() <= 8192
        && reqwest::Url::parse(value).is_ok_and(|url| {
            url.scheme() == "https"
                && url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.fragment().is_none()
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
    (value
        .pointer("/error/code")
        .and_then(serde_json::Value::as_str)
        == Some("failed_to_download_image"))
    .then(Error::remote_image_fetch_rejected)
}

#[cfg(test)]
mod tests;
