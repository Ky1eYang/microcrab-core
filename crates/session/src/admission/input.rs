//! Admission validates text metadata separately from encoded attachment bytes.
//! Only injected owners resolve paths and attachment IDs; Context stores resolved bytes.
use super::image::{
    AttachmentResolver, MAX_IMAGE_BYTES, MAX_IMAGES_PER_INPUT, MAX_INPUT_IMAGE_BYTES, image_type,
    validate_image,
};
use crate::error::Error;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use microcrab_host_api::{Host, ToolError};
use microcrab_protocol::{ImageSource, InputPart, SessionInput, VariablesChange};
use serde::Serialize;
use std::path::Path;
use tokio_util::sync::CancellationToken;

pub(crate) async fn prepare(
    mut input: SessionInput,
    host: Option<&dyn Host>,
    attachments: Option<&dyn AttachmentResolver>,
    max_bytes: usize,
    cancellation: CancellationToken,
) -> Result<SessionInput, Error> {
    if cancellation.is_cancelled() {
        return Err(Error::Cancelled);
    }
    check_budget(&input, max_bytes)?;
    check_image_count(&input)?;
    let mut total = 0;
    for part in &mut input.parts {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let InputPart::Image { source } = part else {
            continue;
        };
        let resolved = match source {
            ImageSource::Path { path } => {
                validate_path(path)?;
                let media_type = image_media_type(path)?.to_owned();
                let host =
                    host.ok_or_else(|| Error::config("local image input requires a Host"))?;
                let data = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Err(Error::Cancelled),
                    result = host.read_file(path.clone(), MAX_IMAGE_BYTES, cancellation.child_token()) => result.map_err(host_error)?,
                };
                Some(ImageSource::Data { media_type, data })
            }
            ImageSource::Attachment { attachment_id } => {
                let resolver = attachments.ok_or_else(|| {
                    Error::config("attachment input requires a Session attachment resolver")
                })?;
                let image = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Err(Error::Cancelled),
                    result = resolver.resolve(attachment_id, cancellation.child_token()) => result?,
                };
                Some(ImageSource::Data {
                    media_type: image.media_type,
                    data: image.data,
                })
            }
            _ => None,
        };
        if let Some(resolved) = resolved {
            *source = resolved;
        }
        add_image_bytes(&mut total, validate_source(source, true)?)?;
    }
    if cancellation.is_cancelled() {
        return Err(Error::Cancelled);
    }
    validate_resolved(&input, max_bytes)?;
    Ok(input)
}

/// Stage a settled content batch on detached candidates. The Session caller owns
/// the variable fence, cancellation, claims and final atomic publication; no real
/// State or Context is mutated when this content decision fails.
pub(crate) fn stage_step(
    context: &crate::context::Context,
    preview: &crate::State,
    prepared: &[SessionInput],
    notices: Option<serde_json::Value>,
    max_output_tokens: u32,
) -> Result<(crate::context::Context, VariablesChange), Error> {
    let mut merged = VariablesChange::default();
    for input in prepared {
        preview.apply_change(input.variables.clone())?;
        merged.overlay(&input.variables);
    }
    let mut values = preview.model_variables()?;
    if let Some(notices) = notices {
        values["notices"] = notices;
    }
    let mut candidate = context.clone();
    // One State suffix precedes all user parts admitted at this boundary.
    candidate.synchronize_state(&values)?;
    for input in prepared.iter().cloned() {
        candidate.admit_user(input, &values)?;
    }
    candidate.content(max_output_tokens)?;
    Ok((candidate, merged))
}

fn check_image_count(input: &SessionInput) -> Result<(), Error> {
    crate::budget::check(
        crate::LimitKind::AttachmentCount,
        input
            .parts
            .iter()
            .filter(|part| matches!(part, InputPart::Image { .. }))
            .count(),
        MAX_IMAGES_PER_INPUT,
    )
    .map_err(|_| Error::config("input exceeds the 20 image attachment count limit"))
}
fn add_image_bytes(total: &mut usize, bytes: usize) -> Result<(), Error> {
    let required = total.checked_add(bytes).ok_or_else(|| {
        crate::budget::reject(
            crate::LimitKind::Arithmetic,
            "attachment byte count overflow",
        )
    })?;
    super::Admission::check(
        super::AdmissionSource::Image,
        crate::LimitKind::AttachmentBytes,
        required,
        MAX_INPUT_IMAGE_BYTES,
    )
    .map_err(|_| {
        Error::config("input images exceed the 200 MiB aggregate attachment byte limit")
    })?;
    *total = required;
    Ok(())
}

/// New direct Context callers must supply already resolved, admitted images.
pub(crate) fn validate_resolved(input: &SessionInput, max_bytes: usize) -> Result<(), Error> {
    admit(input, max_bytes, true).map(|_| ())
}

/// Frozen historical Data remains readable under its original validation rules.
/// References were never historical Context content and remain unresolved errors.
pub(crate) fn validate_historical(input: &SessionInput, max_bytes: usize) -> Result<(), Error> {
    admit(input, max_bytes, false).map(|_| ())
}
pub(super) fn admit(
    input: &SessionInput,
    max_bytes: usize,
    source_policy: bool,
) -> Result<super::Admission, Error> {
    let text_metadata_bytes = encoded_size(input, max_bytes)?;
    if source_policy {
        check_image_count(input)?;
    }
    let mut meaningful = false;
    let mut total = 0;
    let mut image_count = 0;
    for part in &input.parts {
        match part {
            InputPart::Text { text } => {
                if text.contains('\0') {
                    return Err(Error::config("input text contains NUL"));
                }
                meaningful |= !text.trim().is_empty();
            }
            InputPart::Image { source } => {
                let bytes = validate_source(source, source_policy)?;
                if source_policy {
                    add_image_bytes(&mut total, bytes)?;
                }
                image_count += 1;
                meaningful = true;
            }
        }
    }
    if !meaningful {
        return Err(Error::config(
            "input must contain meaningful text or an image",
        ));
    }
    Ok(super::Admission {
        source: if source_policy {
            super::AdmissionSource::Input
        } else {
            super::AdmissionSource::HistoricalInput
        },
        measurement: super::Measurement {
            text_metadata_bytes,
            image_bytes: total,
            image_count,
            ..Default::default()
        },
    })
}

fn validate_source(source: &ImageSource, source_policy: bool) -> Result<usize, Error> {
    match source {
        ImageSource::Url { url } => validate_url(url, source_policy),
        ImageSource::Data { media_type, data } => {
            validate_media_type(media_type)?;
            if data.is_empty() {
                return Err(Error::config("image data must be nonempty"));
            }
            if source_policy {
                validate_image(media_type, data)?;
            }
            Ok(data.len())
        }
        ImageSource::Path { .. } => Err(Error::config(
            "image paths must be resolved by a Host before Context admission",
        )),
        ImageSource::Attachment { .. } => Err(Error::config(
            "attachment references must be resolved before Context admission",
        )),
    }
}

fn check_budget(input: &SessionInput, max_bytes: usize) -> Result<(), Error> {
    encoded_size(input, max_bytes).map(|_| ())
}

/// The inbox retains complete submitted values and keeps its separate memory cap.
pub(crate) fn retained_size(input: &SessionInput, max_bytes: usize) -> Result<usize, Error> {
    super::Admission::serialized(
        super::AdmissionSource::Inbox,
        crate::LimitKind::InboxBytes,
        input,
        max_bytes,
    )
    .map(|admission| {
        admission
            .measurement
            .retained_bytes
            .expect("Inbox serialization measures retained storage")
    })
}

pub(crate) fn encoded_size(input: &SessionInput, max_bytes: usize) -> Result<usize, Error> {
    // Preserve text, variables and image metadata accounting, without serializing
    // raw bytes or the payload portion of inline data URLs into the text budget.
    #[derive(Serialize)]
    struct MetadataInput<'a> {
        parts: Vec<MetadataPart<'a>>,
        variables: &'a VariablesChange,
    }
    #[derive(Serialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    enum MetadataPart<'a> {
        Text { text: &'a str },
        Image { source: MetadataSource<'a> },
    }
    #[derive(Serialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    enum MetadataSource<'a> {
        Url { url: &'a str },
        Data { media_type: &'a str },
        Path { path: &'a Path },
        Attachment { attachment_id: &'a str },
    }
    let parts = input
        .parts
        .iter()
        .map(|part| match part {
            InputPart::Text { text } => MetadataPart::Text { text },
            InputPart::Image { source } => MetadataPart::Image {
                source: match source {
                    ImageSource::Data { media_type, .. } => MetadataSource::Data { media_type },
                    ImageSource::Url { url } => MetadataSource::Url {
                        url: if url.starts_with("data:") {
                            url.split_once(',')
                                .map_or(url.as_str(), |(header, _)| header)
                        } else {
                            url
                        },
                    },
                    ImageSource::Path { path } => MetadataSource::Path { path },
                    ImageSource::Attachment { attachment_id } => {
                        MetadataSource::Attachment { attachment_id }
                    }
                },
            },
        })
        .collect();
    super::Admission::serialized(
        super::AdmissionSource::Input,
        crate::LimitKind::InputBytes,
        &MetadataInput {
            parts,
            variables: &input.variables,
        },
        max_bytes,
    )
    .map(|admission| admission.measurement.text_metadata_bytes)
}

fn validate_url(url: &str, source_policy: bool) -> Result<usize, Error> {
    if url.contains('\0') || url.chars().any(char::is_whitespace) {
        return Err(Error::config("invalid image URL"));
    }
    if let Some(data_url) = url.strip_prefix("data:") {
        let (header, encoded) = data_url
            .split_once(',')
            .ok_or_else(|| Error::config("invalid image data URL"))?;
        let media_type = header
            .strip_suffix(";base64")
            .ok_or_else(|| Error::config("image data URL requires base64 encoding"))?;
        validate_media_type(media_type)?;
        if encoded.is_empty() || (source_policy && encoded.len() > MAX_IMAGE_BYTES.div_ceil(3) * 4)
        {
            return Err(Error::config("empty or oversized image data URL"));
        }
        let data = STANDARD
            .decode(encoded)
            .map_err(|_| Error::config("invalid base64 image data URL"))?;
        if source_policy {
            validate_image(media_type, &data)?;
        }
        return Ok(data.len());
    }
    let parsed = reqwest::Url::parse(url).map_err(|_| Error::config("invalid image URL"))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
    {
        return Err(Error::config(
            "image URL requires HTTP or HTTPS without credentials",
        ));
    }
    // Remote content is provider-owned; no remote fetch is introduced by admission.
    Ok(0)
}
fn validate_media_type(media_type: &str) -> Result<(), Error> {
    if image_type(media_type).is_some() {
        Ok(())
    } else {
        Err(Error::config("unsupported image media type"))
    }
}
fn validate_path(path: &Path) -> Result<(), Error> {
    let text = path
        .to_str()
        .ok_or_else(|| Error::config("image path must be valid Unicode"))?;
    if text.is_empty() || text.contains('\0') {
        return Err(Error::config(
            "image path must be nonempty and contain no NUL",
        ));
    }
    #[cfg(windows)]
    if text.starts_with(r"\\.\") || path.components().any(|component| {
        matches!(component, std::path::Component::Normal(value) if value.to_string_lossy().contains(':'))
    }) { return Err(Error::config("unsupported image path encoding")); }
    Ok(())
}
fn image_media_type(path: &Path) -> Result<&'static str, Error> {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    match extension.as_str() {
        "png" => Ok("image/png"),
        "jpg" | "jpeg" => Ok("image/jpeg"),
        "gif" => Ok("image/gif"),
        "webp" => Ok("image/webp"),
        _ => Err(Error::config("unsupported local image extension")),
    }
}
fn host_error(error: ToolError) -> Error {
    match error {
        ToolError::Cancelled => Error::Cancelled,
        ToolError::FileLimit => {
            Error::config("local image exceeds the 20 MiB attachment byte limit")
        }
        error => Error::from_host_error(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admission::image::ResolvedAttachment;
    use std::sync::atomic::{AtomicUsize, Ordering};
    fn png(bytes: usize) -> Vec<u8> {
        let mut data = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
        data.extend_from_slice(&1u32.to_be_bytes());
        data.extend_from_slice(&1u32.to_be_bytes());
        data.extend_from_slice(&[8, 2, 0, 0, 0, 0, 0, 0, 0]);
        data.resize(bytes.max(data.len()), 0);
        data
    }
    struct Resolver(AtomicUsize);
    #[async_trait::async_trait]
    impl AttachmentResolver for Resolver {
        async fn resolve(
            &self,
            id: &str,
            _: CancellationToken,
        ) -> Result<ResolvedAttachment, Error> {
            self.0.fetch_add(1, Ordering::SeqCst);
            if id != "image" {
                return Err(Error::Config("unavailable".into()));
            }
            Ok(ResolvedAttachment {
                media_type: "image/png".into(),
                data: png(128 * 1024),
            })
        }
    }
    #[test]
    fn shared_admission_keeps_text_image_and_retention_dimensions_independent() {
        let input = SessionInput {
            parts: vec![InputPart::Image {
                source: ImageSource::Data {
                    media_type: "image/png".into(),
                    data: png(128 * 1024),
                },
            }],
            variables: Default::default(),
        };
        let admitted = crate::admission::Admission::input(&input, 256).unwrap();
        assert_eq!(admitted.source, crate::admission::AdmissionSource::Input);
        assert_eq!(admitted.measurement.image_bytes, 128 * 1024);
        assert_eq!(admitted.measurement.image_count, 1);
        assert!(admitted.measurement.text_metadata_bytes < 256);
        assert_eq!(admitted.measurement.retained_bytes, None);
        let error = retained_size(&input, 256).unwrap_err();
        let Error::Limit(limit) = error else {
            panic!("expected inbox byte limit")
        };
        assert_eq!(limit.kind, crate::LimitKind::InboxBytes);
        assert!(!limit.is_pre_effect());
        let exact = admitted.measurement.text_metadata_bytes;
        assert!(crate::admission::Admission::input(&input, exact).is_ok());
        assert!(crate::admission::Admission::input(&input, exact - 1).is_err());
    }

    #[tokio::test]
    async fn reference_resolves_before_context_without_using_text_budget() {
        let resolver = Resolver(AtomicUsize::new(0));
        let input = SessionInput {
            parts: vec![InputPart::Image {
                source: ImageSource::Attachment {
                    attachment_id: "image".into(),
                },
            }],
            variables: Default::default(),
        };
        let prepared = prepare(
            input.clone(),
            None,
            Some(&resolver),
            256,
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(
            matches!(&prepared.parts[0], InputPart::Image { source: ImageSource::Data { data, .. } } if data.len() == 128 * 1024)
        );
        assert!(encoded_size(&prepared, 256).is_ok());
        assert!(validate_resolved(&input, 256).is_err());
        assert!(
            prepare(input, None, None, 256, CancellationToken::new())
                .await
                .is_err()
        );
        assert_eq!(resolver.0.load(Ordering::SeqCst), 1);
        assert!(encoded_size(&SessionInput::text("x".repeat(300)), 256).is_err());
    }
    #[tokio::test]
    async fn image_count_rejects_before_resolver_and_cancelled_input_does_not_resolve() {
        let resolver = Resolver(AtomicUsize::new(0));
        let part = InputPart::Image {
            source: ImageSource::Attachment {
                attachment_id: "image".into(),
            },
        };
        let input = SessionInput {
            parts: vec![part; MAX_IMAGES_PER_INPUT + 1],
            variables: Default::default(),
        };
        assert!(
            prepare(
                input,
                None,
                Some(&resolver),
                64 * 1024,
                CancellationToken::new()
            )
            .await
            .is_err()
        );
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        assert!(matches!(
            prepare(
                SessionInput::text("hello"),
                None,
                Some(&resolver),
                256,
                cancellation
            )
            .await,
            Err(Error::Cancelled)
        ));
        assert_eq!(resolver.0.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn direct_data_and_data_urls_enforce_source_policy_while_history_remains_readable() {
        let legacy = SessionInput {
            parts: vec![InputPart::Image {
                source: ImageSource::Data {
                    media_type: "image/png".into(),
                    data: vec![1],
                },
            }],
            variables: Default::default(),
        };
        assert!(validate_resolved(&legacy, 256).is_err());
        assert!(validate_historical(&legacy, 256).is_ok());
        let url = format!("data:image/png;base64,{}", STANDARD.encode(png(128 * 1024)));
        let input = SessionInput {
            parts: vec![InputPart::Image {
                source: ImageSource::Url { url },
            }],
            variables: Default::default(),
        };
        assert!(validate_resolved(&input, 256).is_ok());
        let mut total = MAX_INPUT_IMAGE_BYTES - 1;
        assert!(add_image_bytes(&mut total, 1).is_ok());
        assert!(add_image_bytes(&mut total, 1).is_err());
    }
}
