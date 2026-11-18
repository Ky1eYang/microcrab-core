//! Explicit route-scoped bridge; Session image bytes remain canonical history.
use microcrab_session::admission::image::AttachmentResolver;
use microcrab_transport::{Error, ImageUrlResolver};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub(super) struct AttachmentImageUrlResolver(pub(super) Arc<dyn AttachmentResolver>);
#[async_trait::async_trait]
impl ImageUrlResolver for AttachmentImageUrlResolver {
    async fn upload(
        &self,
        media_type: &str,
        data: &[u8],
        cancellation: CancellationToken,
    ) -> Result<Option<String>, Error> {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        microcrab_session::admission::image::validate_image(media_type, data)
            .map_err(upload_error)?;
        let uploaded = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(Error::Cancelled),
            result = self.0.upload_attachment(media_type, data, cancellation.clone()) => result.map_err(upload_error)?,
        };
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        // Attachment IDs never replace the already validated Data in Context.
        // MediaProjection owns URL admission/cache; private Server carrier URLs
        // are not made public by installing this optional callback.
        Ok(uploaded.and_then(|attachment| attachment.public_url))
    }
}

fn upload_error(error: microcrab_session::Error) -> Error {
    use microcrab_session::Error as SessionError;
    if matches!(error, SessionError::Cancelled) {
        return Error::Cancelled;
    }
    // Retain typed causes while excluding callback-owned free-form strings,
    // including reasons that may contain resource URLs, query secrets or bodies.
    let mut diagnostic = error.tool_diagnostic();
    diagnostic.reason = None;
    let mut facts = serde_json::to_value(diagnostic).unwrap_or(serde_json::Value::Null);
    if let SessionError::Limit(limit) = &error {
        facts["limit"] = serde_json::json!({
            "kind":limit.kind.as_str(), "required_bytes":limit.required_bytes,
            "budget_bytes":limit.budget_bytes, "required_count":limit.required_count,
            "budget_count":limit.budget_count,
        });
    }
    if let SessionError::Io(source) = &error {
        facts["io_kind"] = serde_json::json!(format!("{:?}", source.kind()));
        facts["os_error"] = serde_json::json!(source.raw_os_error());
    }
    Error::Protocol(microcrab_transport::ProtocolError::with_private_reason(
        "attachment URL upload failed",
        format!("attachment URL upload failed: {facts}"),
    ))
}

#[cfg(test)]
mod tests;
