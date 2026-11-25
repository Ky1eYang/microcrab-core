//! Fresh public image projection and one retained Server snapshot per invocation.
use super::{RoundRecorder, view_image};
use crate::{Client, Error, State, context::Context};
use microcrab_auditor::Auditor;
use microcrab_protocol::{ImageSource, InputPart};
use microcrab_transport::{Content, ContentItem, ModelEvent, ResponseOutcome, UserSource};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[cfg(test)]
mod tests;

// Stop only when the model actually repeats the same invalid call, not when
// different attempts happen to fail the same validation rule.
pub(super) fn malformed_key(arguments: &Value, error: &Error) -> Option<String> {
    use crate::observation::diagnostics::{DiagnosticCode, DiagnosticPhase};
    let diagnostic = error.tool_diagnostic();
    if diagnostic.code != DiagnosticCode::InvalidArgument
        || diagnostic.phase != DiagnosticPhase::Validation
    {
        return None;
    }
    let mut canonical = arguments.clone();
    canonical.sort_all_objects();
    let bytes = serde_json::to_vec(&canonical).ok()?;
    Some(format!("{:x}", Sha256::digest(bytes)))
}

#[expect(
    clippy::too_many_arguments,
    reason = "The media attempt uses separate session, recording, provider and cancellation owners"
)]
pub(super) async fn run<R: RoundRecorder>(
    context: &mut Context,
    content: &mut Content,
    pending: view_image::PendingHttps,
    state: &State,
    client: &dyn Client,
    auditor: &dyn Auditor,
    recorder: &R,
    cancellation: &CancellationToken,
    events: Option<mpsc::Sender<ModelEvent>>,
    continuation: super::RoundContinuation,
) -> Result<ResponseOutcome, Error> {
    let model = pending.model.as_deref().unwrap_or(client);
    let attempts = model.public_https_image_attempts();
    if attempts == 0 {
        return Err(Error::tool(
            "selected model route does not support public HTTPS tool images",
        ));
    }
    let mut preview = content.clone();
    preview.items.push(media_carrier(
        &pending.call_id,
        InputPart::Image {
            source: ImageSource::Url {
                url: pending.target.url().as_url().as_str().into(),
            },
        },
        None,
    ));
    let charge = state.reserve_image_transfer(pending.limit)?;
    let limit = pending.limit;
    let call_id = pending.call_id;
    let (outcome, emitted, image) =
        first_attempt_and_archive(model, &preview, auditor, state, cancellation, || {
            fetch(pending.target, limit, cancellation)
        })
        .await?;
    let (metadata, part) = view_image::https_attachment(image)?;
    context.append_retained_tool_image(&call_id, media_carrier(&call_id, part, Some(metadata)))?;
    drop(charge);
    recorder.record(context, Some(continuation)).await?;
    let (epoch, items) = context.admitted_items();
    client.observe_admitted(epoch, items, None).await;
    let mut archived = context.content(content.max_output_tokens)?;
    archived.turn_id = content.turn_id;
    archived.round = content.round;
    *content = archived;
    match outcome {
        Err(error) if should_fallback(&error, emitted, attempts) => {
            if cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }
            model
                .stream_public_https_image_attempt(
                    content,
                    auditor,
                    state,
                    cancellation.clone(),
                    events,
                )
                .await
                .map_err(Error::from)
        }
        result => result.map_err(Error::from),
    }
}

fn should_fallback(error: &microcrab_transport::Error, emitted: bool, attempts: u8) -> bool {
    !emitted && attempts > 1 && error.is_remote_image_fetch_rejection()
}

async fn first_attempt_and_archive<T>(
    model: &dyn Client,
    preview: &Content,
    auditor: &dyn Auditor,
    state: &State,
    cancellation: &CancellationToken,
    archive: impl AsyncFnOnce() -> Result<T, Error>,
) -> Result<(Result<ResponseOutcome, microcrab_transport::Error>, bool, T), Error> {
    let (send, mut receive) = mpsc::channel(16);
    // No output is forwarded until retention. Draining the temporary stream
    // keeps provider progress alive and detects even an event queued on failure.
    let attempt = model.stream_public_https_image_attempt(
        &preview,
        auditor,
        state,
        cancellation.clone(),
        Some(send),
    );
    tokio::pin!(attempt);
    let mut emitted = false;
    let mut receiving = true;
    let outcome = loop {
        tokio::select! { biased;
            _ = cancellation.cancelled() => return Err(Error::Cancelled),
            event = receive.recv(), if receiving => match event {
                Some(_) => emitted = true,
                None => receiving = false,
            },
            outcome = &mut attempt => break outcome,
        }
    };
    emitted |= receive.try_recv().is_ok();
    if cancellation.is_cancelled() {
        return Err(Error::Cancelled);
    }
    let image = archive().await?;
    Ok((outcome, emitted, image))
}

async fn fetch(
    target: microcrab_transport::public_https::PublicHttpsTarget,
    limit: usize,
    cancellation: &CancellationToken,
) -> Result<microcrab_transport::public_https::PublicHttpsImage, Error> {
    target.fetch(limit, cancellation).await.map_err(|error| {
        if cancellation.is_cancelled() {
            Error::Cancelled
        } else {
            Error::tool_dynamic(error.to_string())
        }
    })
}

fn media_carrier(call: &str, image: InputPart, metadata: Option<String>) -> ContentItem {
    let metadata = metadata
        .map(|metadata| format!(" Retained image metadata: {metadata}"))
        .unwrap_or_default();
    ContentItem::User {
        parts: vec![
            InputPart::Text {
                text: format!("[Image output for tool call {call}].{metadata}"),
            },
            image,
        ],
        source: Some(UserSource::Framework),
        envelope: Some(
            microcrab_transport::CarrierEnvelopeVersion::RetainedToolImage {
                call_id: call.into(),
            },
        ),
    }
}
