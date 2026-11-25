//! Root builtin: disclose exactly one admitted raster as typed image Data.
use super::{FinalOutput, Tool, ToolContext, ToolOutput};
use crate::admission::image::{MAX_IMAGE_BYTES, image_type, validate_image};
use crate::{Error, LimitKind};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use microcrab_protocol::ToolDefinition;
use serde::Serialize;
use serde_json::{Value, json};

mod source;
#[cfg(test)]
mod tests;

pub(crate) use source::Source;
pub(crate) const NAME: &str = "view_image";
pub(super) struct ViewImage;

#[derive(Serialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
enum SourceKind {
    File,
    Bytes,
    Inline,
    Https,
}

#[derive(Serialize)]
struct Metadata {
    byte_length: u64,
    media_type: &'static str,
    width: u32,
    height: u32,
    source_kind: SourceKind,
}

fn checked_metadata(
    expected_mime: Option<&str>,
    data: &[u8],
    source_kind: SourceKind,
) -> Result<Metadata, Error> {
    let media_type = crate::admission::image::signature_type(data)?;
    if expected_mime.is_some_and(|expected| expected != media_type) {
        return Err(Error::tool("image MIME does not match the encoded raster"));
    }
    let image = validate_image(media_type, data)?;
    let byte_length = u64::try_from(data.len())
        .map_err(|_| crate::budget::reject(LimitKind::Arithmetic, "image byte length overflow"))?;
    Ok(Metadata {
        byte_length,
        media_type,
        width: image.width,
        height: image.height,
        source_kind,
    })
}

fn output(metadata: Metadata, data: Vec<u8>) -> Result<ToolOutput, Error> {
    Ok(ToolOutput::Final(FinalOutput {
        content: serde_json::to_string(&metadata)?,
        failure: None,
        attachments: vec![crate::execution::MediaAttachment {
            media_type: metadata.media_type.into(),
            data,
        }],
    }))
}

fn image_limit(state: &crate::State) -> usize {
    state
        .snapshot()
        .limits
        .max_tool_image_bytes
        .min(MAX_IMAGE_BYTES)
}

pub(crate) fn require_current_definition(definitions: &[ToolDefinition]) -> Result<(), Error> {
    if let Some(definition) = definitions.iter().find(|tool| tool.name == NAME)
        && definition != &ViewImage.definition()
    {
        return Err(Error::config(
            "restored view_image catalog is unsupported; update the Session environment or create a new Session",
        ));
    }
    Ok(())
}

pub(crate) fn bind(arguments: &Value, state: &crate::State) -> Result<Source, Error> {
    state.ensure_root_bytes()?;
    let object = arguments
        .as_object()
        .ok_or_else(|| invalid("view_image expects an object containing only image_url"))?;
    let value = object
        .get("image_url")
        .ok_or_else(|| invalid("view_image requires the image_url string"))?;
    if object.len() != 1 {
        return Err(invalid(
            "view_image only accepts image_url; do not pass extra arguments",
        ));
    }
    let text = value.as_str().ok_or_else(|| {
        if value.is_boolean() {
            invalid("image_url must be a string; received a boolean")
        } else {
            invalid("image_url must be a string; for example file:///workspace/image.png")
        }
    })?;
    Source::parse(text, state).map_err(|error| {
        let reason = error.to_string();
        if reason.contains("selector is stale") || reason.contains("variable is unavailable") {
            return error;
        }
        let mut diagnostic = invalid("invalid image_url source").tool_diagnostic();
        diagnostic.reason = Some(crate::observation::diagnostics::SafeDiagnosticReason::new(
            &reason,
        ));
        Error::Diagnostic(diagnostic)
    })
}

pub(crate) fn invalid(reason: &'static str) -> Error {
    use crate::observation::diagnostics::*;
    let mut diagnostic = ToolDiagnostic::new(
        DiagnosticCode::InvalidArgument,
        DiagnosticLayer::Runtime,
        DiagnosticPhase::Validation,
    );
    diagnostic.reason = Some(SafeDiagnosticReason::new(reason));
    diagnostic.effects = EffectKnowledge::NotStarted;
    Error::Diagnostic(diagnostic)
}

/// The sole image argument is resolved on a dispatch copy. The special Bytes
/// conversion is handled by the image source binder, not generic JSON expansion.
pub(crate) fn bind_dispatch(
    arguments: &Value,
    state: &crate::State,
    max_bytes: usize,
) -> Result<Source, Error> {
    if arguments
        .get("image_url")
        .and_then(Value::as_str)
        .and_then(source::bytes_reference)
        .is_some()
    {
        return bind(arguments, state);
    }
    let (effective, _) =
        super::injection::expand_visible(arguments, &state.injection_view(), max_bytes)?;
    bind(&effective, state)
}

pub(crate) fn is_https_replay_reference(source: &str) -> bool {
    // v0.2.3 wrote a policy digest; v0.2.4 wrote a presentation placeholder.
    source == "[HTTPS image URL redacted]"
        || source
            .get(..6)
            .is_some_and(|s| s.eq_ignore_ascii_case("https:"))
        || source.contains("{{var:")
}

#[derive(Clone)]
pub(crate) struct ApprovalPreview {
    pub(crate) owner: Value,
    pub(crate) reviewer: Value,
}

pub(crate) fn approval_preview(authored: &Value, source: &Source) -> ApprovalPreview {
    let mut owner = authored.clone();
    // A separately routed reviewer has no grant to receive the image source.
    let mut reviewer = json!({"image_url":"[source withheld from separate reviewer]"});
    if let Source::Https(url) = source {
        let destination = json!(url.destination());
        let effects = json!(source::HTTPS_EFFECTS);
        owner["destination"] = destination.clone();
        owner["effects"] = effects.clone();
        reviewer["destination"] = destination;
        reviewer["effects"] = effects;
    }
    ApprovalPreview { owner, reviewer }
}

async fn read_path(
    path: &std::path::Path,
    limit: usize,
    context: &ToolContext,
) -> Result<ToolOutput, Error> {
    let host = context.state.bound_image_host()?;
    if !host.supports_private_file_read() {
        return Err(Error::tool(
            "the bound Runtime does not support confined file reads; use inline image data or Bytes",
        ));
    }
    let charge = context.state.reserve_image_transfer(limit)?;
    let buffer = microcrab_host_api::PrivateFileBuffer::new(limit, charge);
    let op = microcrab_host_api::OpContext {
        cancel: context.cancellation.clone(),
        principal: context.session_id.clone(),
        trace_id: context.call_id.clone(),
        deadline: None,
    };
    let result = host.read_private_file(path.into(), buffer, &op).await;
    if context.cancellation.is_cancelled() {
        return Err(Error::Cancelled);
    }
    let buffer = result.map_err(Error::from_host_error)?;
    crate::budget::check(LimitKind::AttachmentBytes, buffer.as_bytes().len(), limit)?;
    let metadata = checked_metadata(None, buffer.as_bytes(), SourceKind::File)?;
    output(metadata, buffer.into_bytes())
}

#[async_trait]
impl Tool for ViewImage {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: NAME.into(),
            description: "View one image. Pass image_url as a quoted string: file:///workspace/image.png, https://example.org/image.png, data:image/png;base64,..., standard Base64, or {{var:base64:preview}} for an exact private Bytes value. Use file URIs only for allowed files in a Runtime with confined file reads.".into(),
            parameters: json!({
                "type":"object","additionalProperties":false,"required":["image_url"],
                "properties":{"image_url":{"type":"string","minLength":1,"maxLength":source::MAX_ENCODED_BYTES + 64}}
            }),
        }
    }

    fn media_output_bytes(&self) -> usize {
        MAX_IMAGE_BYTES
    }

    async fn execute(&self, _arguments: Value, context: ToolContext) -> Result<ToolOutput, Error> {
        context.state.ensure_root_bytes()?;
        let source = context
            .image_source
            .as_deref()
            .ok_or_else(|| invalid("image_url has no admitted source"))?;
        if context.cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let limit = image_limit(&context.state);
        match source {
            Source::File(path) => read_path(path, limit, &context).await,
            Source::Bytes(selector) => context.state.with_image_bytes(selector, |mime, data| {
                crate::budget::check(LimitKind::AttachmentBytes, data.len(), limit)?;
                let metadata = checked_metadata(Some(mime), data, SourceKind::Bytes)?;
                let _charge = context.state.reserve_image_transfer(data.len())?;
                let mut bytes = Vec::new();
                bytes.try_reserve_exact(data.len()).map_err(|_| {
                    crate::budget::reject(
                        LimitKind::PrivateVariableBytes,
                        "could not allocate image attachment",
                    )
                })?;
                bytes.extend_from_slice(data);
                if context.cancellation.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                output(metadata, bytes)
            }),
            Source::Inline { mime, encoded } => {
                let padding = encoded.len() - encoded.trim_end_matches('=').len();
                let decoded = encoded.len() / 4 * 3 - padding;
                crate::budget::check(LimitKind::AttachmentBytes, decoded, limit)?;
                let _charge = context.state.reserve_image_transfer(decoded)?;
                let bytes = STANDARD
                    .decode(encoded)
                    .map_err(|_| invalid("image_url contains invalid standard Base64"))?;
                if context.cancellation.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                let metadata = checked_metadata(*mime, &bytes, SourceKind::Inline)?;
                output(metadata, bytes)
            }
            Source::Https(url) => execute_https(url.clone(), &context).await,
        }
    }
}

pub(crate) struct PendingHttps {
    pub(crate) target: microcrab_transport::public_https::PublicHttpsTarget,
    pub(crate) call_id: String,
    pub(crate) limit: usize,
    pub(crate) model: Option<std::sync::Arc<dyn crate::Client>>,
}

async fn execute_https(
    url: microcrab_transport::public_https::PublicHttpsUrl,
    context: &ToolContext,
) -> Result<ToolOutput, Error> {
    let slot = context.image_https.as_ref().ok_or_else(|| Error::tool("selected model route does not support public HTTPS tool images; use inline data or Bytes"))?;
    let target = url.admit(&context.cancellation).await.map_err(|error| {
        if context.cancellation.is_cancelled() {
            Error::Cancelled
        } else {
            Error::tool_dynamic(error.to_string())
        }
    })?;
    let mut slot = slot.lock().unwrap_or_else(|error| error.into_inner());
    if slot.is_some() {
        return Err(Error::tool(
            "only one fresh HTTPS image can be viewed in each round",
        ));
    }
    *slot = Some(PendingHttps {
        target,
        call_id: context.call_id.clone(),
        limit: image_limit(&context.state),
        model: context.image_model.clone(),
    });
    Ok(ToolOutput::Final(FinalOutput {
        content: json!({"source_kind":"https","status":"awaiting_provider_and_archive"})
            .to_string(),
        failure: None,
        attachments: vec![],
    }))
}

pub(crate) fn https_attachment(
    image: microcrab_transport::public_https::PublicHttpsImage,
) -> Result<(String, microcrab_protocol::InputPart), Error> {
    let mime = image_type(&image.mime_type)
        .ok_or_else(|| Error::tool("unsupported HTTPS image media type"))?;
    let metadata = checked_metadata(Some(mime), &image.bytes, SourceKind::Https)?;
    Ok((
        serde_json::to_string(&metadata)?,
        microcrab_protocol::InputPart::Image {
            source: microcrab_protocol::ImageSource::Data {
                media_type: metadata.media_type.into(),
                data: image.bytes,
            },
        },
    ))
}
