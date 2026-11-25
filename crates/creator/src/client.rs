//! Retries one provider request only; a published stream event forbids replay.
use microcrab_auditor::{Auditor, VariableView};
use microcrab_session::protocol::Response;
use microcrab_transport::Content;
use microcrab_transport::{
    Client, Error, ImageHostingConfig, ImageHostingGrant, ImageHostingScope, ImageUrlResolver,
    MediaProjection, ModelEvent, OpenAiCompatibleConfig, ProjectedMedia, Protocol, ResponseOutcome,
    ToolImagePosition, TransportClient, TransportConfig,
};
use std::{sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::sleep};
use tokio_util::sync::CancellationToken;

mod media;

#[cfg(test)]
mod tests;

#[derive(Clone)]
pub struct RetryingClient {
    inner: Arc<TransportClient>,
    policy: RetryPolicy,
    media_projection: Option<Arc<MediaProjection>>,
}
#[derive(Clone, Copy)]
pub struct RetryPolicy {
    /// Total attempts, including the initial request and any single media fallback.
    /// One configured attempt disables all retries, including URL-to-Data fallback.
    pub attempts: u32,
    /// Initial exponential backoff between retryable failures.
    pub backoff_ms: u64,
    /// Legacy name for the transport's resettable network inactivity window.
    /// This is not an absolute attempt or turn deadline; preparation and event
    /// forwarding remain controlled by the caller's cancellation token.
    pub deadline_ms: u64,
}
impl RetryingClient {
    pub fn new(config: OpenAiCompatibleConfig, policy: RetryPolicy) -> Result<Self, Error> {
        Self::new_transport(
            TransportConfig {
                auth: Default::default(),
                protocol: Protocol::OpenaiCompatible,
                endpoint: config.endpoint,
                model: config.model,
                api_key: config.api_key,
                timeout_ms: config.timeout_ms,
                max_response_bytes: config.max_response_bytes,
            },
            policy,
        )
    }
    pub fn new_transport(mut config: TransportConfig, policy: RetryPolicy) -> Result<Self, Error> {
        if policy.attempts == 0
            || policy.attempts > 10
            || policy.deadline_ms == 0
            || policy.backoff_ms > 60_000
        {
            return Err(Error::config(
                "retry attempts must be 1..=10 with a positive idle window and backoff <= 60000 ms",
            ));
        }
        // Preserve the public policy field as an idle-window alias for callers.
        config.timeout_ms = policy.deadline_ms;
        Ok(Self {
            inner: Arc::new(TransportClient::new(config)?),
            policy,
            media_projection: None,
        })
    }
    /// Explicitly configure hosting for this exact selected provider/model and
    /// authority. Ordinary Client calls still do not upload any images.
    pub fn with_image_hosting(
        mut self,
        config: ImageHostingConfig,
        resolver: Arc<dyn ImageUrlResolver>,
    ) -> Result<Self, Error> {
        if !matches!(
            self.inner.as_ref(),
            TransportClient::OpenAiCompatible(_) | TransportClient::OpenAiResponses(_)
        ) {
            return Err(Error::config(
                "route protocol has no verified hosted-image fallback",
            ));
        }
        self.media_projection = Some(Arc::new(MediaProjection::new(config, resolver)?));
        Ok(self)
    }

    /// Only trusted request construction with fresh attachment provenance can
    /// issue a grant. Do not derive positions by scanning retained history.
    pub fn authorize_image_hosting<'a>(
        &'a self,
        scope: &ImageHostingScope,
        content: &'a Content,
        positions: &[ToolImagePosition],
    ) -> Result<ImageHostingGrant<'a>, Error> {
        let projection = self.hosting_projection()?;
        let grant = projection.authorize(scope, content, positions)?;
        for position in positions {
            let (media_type, data) = position.data_in(content).ok_or_else(|| {
                Error::config("image hosting position must identify canonical tool image Data")
            })?;
            microcrab_session::admission::image::validate_image(media_type, data)
                .map_err(|_| Error::config("image hosting image failed admission"))?;
        }
        Ok(grant)
    }

    fn hosting_projection(&self) -> Result<&MediaProjection, Error> {
        self.media_projection
            .as_deref()
            .ok_or_else(|| Error::config("image hosting is not configured on this route"))
    }

    pub async fn complete_with_image_hosting(
        &self,
        grant: ImageHostingGrant<'_>,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        cancellation: CancellationToken,
    ) -> Result<Response, Error> {
        self.stream_with_image_hosting(grant, auditor, variables, cancellation, None)
            .await
    }

    pub async fn stream_with_image_hosting(
        &self,
        grant: ImageHostingGrant<'_>,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        cancellation: CancellationToken,
        events: Option<mpsc::Sender<ModelEvent>>,
    ) -> Result<Response, Error> {
        self.stream_with_image_hosting_outcome(grant, auditor, variables, cancellation, events)
            .await
            .map(|outcome| outcome.response)
    }

    /// Preserve exact terminal attempt evidence for trusted hosted requests.
    pub async fn stream_with_image_hosting_outcome(
        &self,
        grant: ImageHostingGrant<'_>,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        cancellation: CancellationToken,
        events: Option<mpsc::Sender<ModelEvent>>,
    ) -> Result<ResponseOutcome, Error> {
        let content = grant.content();
        let projected = self
            .hosting_projection()?
            .project_tool_images(grant, cancellation.clone())
            .await?;
        self.stream_attempts(
            content,
            projected.as_ref(),
            &media::MetadataAudit(auditor),
            variables,
            cancellation,
            events,
        )
        .await
    }

    pub fn with_function_response_media(mut self, enabled: bool) -> Self {
        self.inner = Arc::new(match self.inner.as_ref().clone() {
            TransportClient::Google(client) => {
                TransportClient::Google(client.with_function_response_media(enabled))
            }
            client => client,
        });
        self
    }

    pub fn with_request_options(
        mut self,
        options: microcrab_transport::OpenAiCompatibleRequestOptions,
    ) -> Result<Self, Error> {
        self.inner = Arc::new(match self.inner.as_ref().clone() {
            TransportClient::OpenAiCompatible(client) => {
                TransportClient::OpenAiCompatible(client.with_request_options(options)?)
            }
            TransportClient::OpenAiResponses(client) => {
                TransportClient::OpenAiResponses(client.with_request_options(
                    microcrab_transport::OpenAiResponseRequestOptions {
                        reasoning_effort: options.reasoning_effort,
                        prompt_cache_key: None,
                    },
                )?)
            }
            client if options.reasoning_effort.is_none() => client,
            _ => {
                return Err(Error::config(
                    "reasoning options are unavailable for this provider protocol",
                ));
            }
        });
        Ok(self)
    }

    pub fn with_recording(
        mut self,
        recording: Arc<dyn microcrab_transport::ModelRecording>,
    ) -> Self {
        self.inner = Arc::new(self.inner.as_ref().clone().with_recording(recording));
        self
    }

    pub fn with_observer(mut self, observer: Arc<dyn microcrab_transport::ModelObserver>) -> Self {
        self.inner = Arc::new(self.inner.as_ref().clone().with_observer(observer));
        self
    }

    async fn delay(&self, attempt: u32, cancellation: &CancellationToken) -> Result<(), Error> {
        let delay = Duration::from_millis(
            self.policy
                .backoff_ms
                .saturating_mul(1u64 << attempt.min(16)),
        );
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(Error::Cancelled),
            _ = sleep(delay) => Ok(()),
        }
    }
}
fn retryable(error: &Error) -> bool {
    let former_transport_decode = match error {
        Error::Protocol(cause) => cause.is_retryable_decode(),
        Error::Provider(cause) => cause
            .protocol_error()
            .is_some_and(|cause| cause.is_retryable_decode()),
        _ => false,
    };
    former_transport_decode
        || matches!(
            error,
            Error::Transport
                | Error::TransportFailure(_)
                | Error::ProviderUnavailable(_)
                | Error::ProviderTimeout
                | Error::ProviderIdleTimeout(_)
                | Error::ProviderStatus {
                    status: 408 | 429 | 500..=599
                }
        )
}
#[async_trait::async_trait]
impl Client for RetryingClient {
    fn public_https_image_attempts(&self) -> u8 {
        self.inner
            .public_https_image_attempts()
            .min(self.policy.attempts as u8)
    }

    async fn stream_public_https_image_attempt(
        &self,
        content: &Content,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        cancellation: CancellationToken,
        events: Option<mpsc::Sender<ModelEvent>>,
    ) -> Result<ResponseOutcome, Error> {
        self.inner
            .stream_public_https_image_attempt(content, auditor, variables, cancellation, events)
            .await
    }

    async fn complete(
        &self,
        content: &Content,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        cancellation: CancellationToken,
    ) -> Result<Response, Error> {
        let mut attempt = 0;
        loop {
            let result = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(Error::Cancelled),
                result = self.inner.complete(content, auditor, variables, cancellation.clone()) => result,
            };
            match result {
                Err(error) if retryable(&error) && attempt + 1 < self.policy.attempts => {
                    self.delay(attempt, &cancellation).await?;
                    attempt += 1;
                }
                result => return result,
            }
        }
    }
    async fn stream(
        &self,
        content: &Content,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        cancellation: CancellationToken,
        events: Option<mpsc::Sender<ModelEvent>>,
    ) -> Result<Response, Error> {
        self.stream_admitted_outcome(content, auditor, variables, cancellation, events)
            .await
            .map(|outcome| outcome.response)
    }

    async fn stream_admitted_outcome(
        &self,
        content: &Content,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        cancellation: CancellationToken,
        events: Option<mpsc::Sender<ModelEvent>>,
    ) -> Result<ResponseOutcome, Error> {
        self.stream_attempts(content, None, auditor, variables, cancellation, events)
            .await
    }
}

impl RetryingClient {
    async fn stream_attempts(
        &self,
        content: &Content,
        mut projected: Option<&ProjectedMedia>,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        cancellation: CancellationToken,
        events: Option<mpsc::Sender<ModelEvent>>,
    ) -> Result<ResponseOutcome, Error> {
        let mut attempt = 0;
        loop {
            let (attempt_content, hosted) = media::attempt_content(content, projected);
            if !hosted {
                projected = None;
            }
            let (sender, mut receiver) = mpsc::channel(16);
            let future = self.inner.stream_admitted_outcome(
                attempt_content,
                auditor,
                variables,
                cancellation.clone(),
                Some(sender),
            );
            tokio::pin!(future);
            let mut published = false;
            let mut receiving = true;
            let result = loop {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => break Err(Error::Cancelled),
                    event = receiver.recv(), if receiving => match event {
                        Some(event) => {
                            published = true;
                            if let Some(events) = &events {
                                tokio::select! {
                                    biased;
                                    _ = cancellation.cancelled() => return Err(Error::Cancelled),
                                    sent = events.send(event) => { sent.map_err(|_| Error::Cancelled)?; }
                                }
                            }
                        }
                        None => receiving = false,
                    },
                    result = &mut future => break result,
                }
            };
            let rejected_hosted = hosted
                && result
                    .as_ref()
                    .is_err_and(|error| error.is_remote_image_fetch_rejection());
            if rejected_hosted {
                if let Some(media) = projected.take() {
                    media.invalidate();
                }
            }
            // A provider can enqueue its final deltas and return Ready during the
            // same poll. Drain those events before observing success or retrying.
            while let Ok(event) = receiver.try_recv() {
                published = true;
                if let Some(events) = &events {
                    tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => return Err(Error::Cancelled),
                        sent = events.send(event) => { sent.map_err(|_| Error::Cancelled)?; }
                    }
                }
            }
            match result {
                Err(error) if rejected_hosted => {
                    if published || attempt + 1 >= self.policy.attempts {
                        return Err(error);
                    }
                    attempt += 1;
                }
                Err(error)
                    if !published && retryable(&error) && attempt + 1 < self.policy.attempts =>
                {
                    self.delay(attempt, &cancellation).await?;
                    attempt += 1;
                }
                result => return result,
            }
        }
    }
}
