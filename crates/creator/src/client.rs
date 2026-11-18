//! Retries one provider request only; a published stream event forbids replay.
use microcrab_auditor::{Auditor, VariableView};
use microcrab_session::protocol::Response;
use microcrab_transport::Content;
use microcrab_transport::{
    Client, Error, ModelEvent, OpenAiCompatibleConfig, Protocol, TransportClient, TransportConfig,
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
    media_projection: Option<Arc<microcrab_transport::MediaProjection>>,
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
    /// The projection instance belongs only to this client's selected route/model.
    pub fn with_media_projection(
        mut self,
        projection: Arc<microcrab_transport::MediaProjection>,
    ) -> Self {
        self.media_projection = Some(projection);
        self
    }

    /// Opt into a public URL uploader for this exact selected route/model.
    /// Pass false for unverified routes (including Google without a MIME witness).
    /// The same resolver may separately be installed on SessionBuilder for input
    /// attachment resolution. This builder never changes frozen canonical Data.
    pub fn with_attachment_uploader(
        self,
        resolver: Arc<dyn microcrab_session::admission::image::AttachmentResolver>,
        supports_external_image_urls: bool,
    ) -> Self {
        self.with_media_projection(Arc::new(microcrab_transport::MediaProjection::new(
            Arc::new(media::AttachmentImageUrlResolver(resolver)),
            supports_external_image_urls,
        )))
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
    async fn complete(
        &self,
        content: &Content,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        cancellation: CancellationToken,
    ) -> Result<Response, Error> {
        let projected = match &self.media_projection {
            Some(projection) => {
                projection
                    .project_tool_images(content, cancellation.clone())
                    .await?
            }
            None => None,
        };
        let mut use_data = false;
        let mut attempt = 0;
        loop {
            let attempt_content = if use_data {
                content
            } else {
                projected.as_ref().unwrap_or(content)
            };
            let result = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(Error::Cancelled),
                result = self.inner.complete(attempt_content, auditor, variables, cancellation.clone()) => result,
            };
            match result {
                Err(error)
                    if error.is_remote_image_fetch_rejection()
                        && projected.is_some()
                        && !use_data
                        && attempt + 1 < self.policy.attempts =>
                {
                    use_data = true;
                    attempt += 1;
                }
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
        let projected = match &self.media_projection {
            Some(projection) => {
                projection
                    .project_tool_images(content, cancellation.clone())
                    .await?
            }
            None => None,
        };
        let mut use_data = false;
        let mut attempt = 0;
        loop {
            let attempt_content = if use_data {
                content
            } else {
                projected.as_ref().unwrap_or(content)
            };
            let (sender, mut receiver) = mpsc::channel(16);
            let future = self.inner.stream(
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
                Err(error)
                    if error.is_remote_image_fetch_rejection()
                        && !published
                        && projected.is_some()
                        && !use_data
                        && attempt + 1 < self.policy.attempts =>
                {
                    use_data = true;
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
