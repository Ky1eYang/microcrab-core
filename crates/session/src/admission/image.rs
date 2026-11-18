//! Dedicated image admission policy, independent of text and control-message budgets.
use crate::Error;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
mod inspect;

pub const MAX_IMAGE_BYTES: usize = 20 * 1024 * 1024;
pub const MAX_IMAGES_PER_INPUT: usize = 20;
pub const MAX_IMAGES_PER_TOOL_OUTPUT: usize = 1;

/// Context stores JSON numeric byte vectors: each byte and comma uses at most four
/// bytes. One image's fixed framing/MIME overhead is bounded by admitted MIME types.
pub(crate) fn tool_context_reservation(media_bytes: usize) -> usize {
    if media_bytes == 0 {
        0
    } else {
        media_bytes.saturating_mul(4).saturating_add(256)
    }
}

/// Canonical tool history admits self-contained Data only, never transport hints.
pub(crate) fn validate_tool_parts(parts: &[microcrab_protocol::InputPart]) -> Result<(), Error> {
    use microcrab_protocol::{ImageSource, InputPart};
    crate::budget::check(
        crate::LimitKind::AttachmentCount,
        parts
            .iter()
            .filter(|part| matches!(part, InputPart::Image { .. }))
            .count(),
        MAX_IMAGES_PER_TOOL_OUTPUT,
    )?;
    for part in parts {
        match part {
            InputPart::Text { .. } => {}
            InputPart::Image {
                source: ImageSource::Data { media_type, data },
            } => {
                validate_image(media_type, data)?;
            }
            InputPart::Image { .. } => {
                return Err(Error::protocol(
                    "tool history requires self-contained image Data",
                ));
            }
        }
    }
    Ok(())
}
pub const MAX_INPUT_IMAGE_BYTES: usize = 200 * 1024 * 1024;
pub const MAX_IMAGE_PIXELS: u64 = 64_000_000;
pub const MAX_IMAGE_DIMENSION: u32 = 8192;

/// The only admission allow-list for encoded images. Storage extensions and
/// provider path fallbacks are separate owners and must not copy this match.
pub fn image_type(media_type: &str) -> Option<&'static str> {
    Some(match media_type {
        "image/png" => "image/png",
        "image/jpeg" => "image/jpeg",
        "image/gif" => "image/gif",
        "image/webp" => "image/webp",
        _ => return None,
    })
}

/// Raw image bytes loaded from a Session-authorized attachment identity.
pub struct ResolvedAttachment {
    pub media_type: String,
    pub data: Vec<u8>,
}

/// Optional stable public route hint. Canonical Context always retains Data.
/// The embedder owns identity scope and must not disclose private carrier URLs.
pub struct UploadedAttachment {
    pub attachment_id: String,
    pub public_url: Option<String>,
}

/// The embedding application owns identity scope, durable bytes and cancellation.
#[async_trait]
pub trait AttachmentResolver: Send + Sync {
    async fn resolve(
        &self,
        id: &str,
        cancellation: CancellationToken,
    ) -> Result<ResolvedAttachment, Error>;

    /// Optional upload authority; no upload occurs unless an embedder overrides it.
    async fn upload_attachment(
        &self,
        _media_type: &str,
        _data: &[u8],
        _cancellation: CancellationToken,
    ) -> Result<Option<UploadedAttachment>, Error> {
        Ok(None)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ImageMetadata {
    pub width: u32,
    pub height: u32,
}

/// Inspect bounded encoded metadata without decoding or transforming the raster.
/// The format must agree with the declared MIME; dimension admission is not a
/// complete image decoder and does not claim to verify compressed pixel data.
pub fn validate_image(media_type: &str, data: &[u8]) -> Result<ImageMetadata, Error> {
    if data.is_empty() {
        return Err(Error::config("image data must be nonempty"));
    }
    super::Admission::check(
        super::AdmissionSource::Image,
        crate::LimitKind::AttachmentBytes,
        data.len(),
        MAX_IMAGE_BYTES,
    )
    // Image admission historically exposes source-policy failures as Config.
    // Keep that public taxonomy while sharing the bounded decision owner.
    .map_err(|_| Error::config("image exceeds the 20 MiB attachment byte limit"))?;
    let metadata = inspect::inspect(media_type, data)?;
    check_dimensions(metadata.width, metadata.height)?;
    Ok(metadata)
}

fn check_dimensions(width: u32, height: u32) -> Result<(), Error> {
    if width == 0 || height == 0 {
        return Err(Error::config("image dimensions must be positive"));
    }
    crate::budget::check(
        crate::LimitKind::ImageSide,
        width.max(height) as usize,
        MAX_IMAGE_DIMENSION as usize,
    )
    .map_err(|_| Error::config("image side exceeds the 8192 pixel attachment limit"))?;
    crate::budget::check(
        crate::LimitKind::ImagePixels,
        (u64::from(width) * u64::from(height)) as usize,
        MAX_IMAGE_PIXELS as usize,
    )
    .map_err(|_| Error::config("image exceeds the 64000000 pixel attachment limit"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn png(width: u32, height: u32) -> Vec<u8> {
        let mut bytes = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
        bytes.extend_from_slice(&width.to_be_bytes());
        bytes.extend_from_slice(&height.to_be_bytes());
        bytes.extend_from_slice(&[8, 2, 0, 0, 0, 0, 0, 0, 0]);
        bytes
    }

    #[test]
    fn source_limits_are_independent_and_inclusive() {
        assert!(validate_image("image/png", &png(8000, 8000)).is_ok());
        assert!(validate_image("image/png", &png(8192, 1)).is_ok());
        assert!(validate_image("image/png", &png(8193, 1)).is_err());
        assert!(validate_image("image/png", &png(8001, 8000)).is_err());
        assert!(validate_image("image/png", &png(0, 1)).is_err());
        let mut source = png(1, 1);
        source.resize(MAX_IMAGE_BYTES, 0);
        assert!(validate_image("image/png", &source).is_ok());
        source.push(0);
        assert!(validate_image("image/png", &source).is_err());
        assert!(validate_image("image/jpeg", &png(1, 1)).is_err());
        assert!(validate_image("image/png", &[]).is_err());
    }
}
