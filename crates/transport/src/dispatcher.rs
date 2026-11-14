//! Dynamic provider dispatch based on configured protocol.
use crate::anthropic::{Anthropic, AnthropicConfig};
use crate::client::{Client, ModelEvent};
use crate::config::TransportConfig;
use crate::content::Content;
use crate::error::Error;
use crate::google::{Google, GoogleConfig};
use crate::openai_compatible::{OpenAiCompatible, OpenAiCompatibleConfig};
use crate::openai_response::{OpenAiResponse, OpenAiResponseConfig};
use crate::protocol::Protocol;
use microcrab_auditor::{Auditor, VariableView};
use microcrab_protocol::Response;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub enum TransportClient {
    OpenAiCompatible(OpenAiCompatible),
    OpenAiResponses(OpenAiResponse),
    Anthropic(Anthropic),
    Google(Google),
}

impl TransportClient {
    pub fn new(config: TransportConfig) -> Result<Self, Error> {
        config.validate()?;
        let auth = config.auth;
        match config.protocol {
            Protocol::OpenaiCompatible => {
                let client = OpenAiCompatible::new(OpenAiCompatibleConfig {
                    endpoint: config.endpoint,
                    model: config.model,
                    api_key: config.api_key,
                    timeout_ms: config.timeout_ms,
                    max_response_bytes: config.max_response_bytes,
                })?
                .with_auth_mode(auth);
                Ok(Self::OpenAiCompatible(client))
            }
            Protocol::OpenaiResponses => {
                let client = OpenAiResponse::new(OpenAiResponseConfig {
                    endpoint: config.endpoint,
                    model: config.model,
                    api_key: config.api_key,
                    timeout_ms: config.timeout_ms,
                    max_response_bytes: config.max_response_bytes,
                })?
                .with_auth_mode(auth);
                Ok(Self::OpenAiResponses(client))
            }
            Protocol::Anthropic => {
                let client = Anthropic::new(AnthropicConfig {
                    endpoint: config.endpoint,
                    model: config.model,
                    api_key: config.api_key,
                    timeout_ms: config.timeout_ms,
                    max_response_bytes: config.max_response_bytes,
                })?
                .with_auth_mode(auth);
                Ok(Self::Anthropic(client))
            }
            Protocol::Google => {
                let client = Google::new(GoogleConfig {
                    endpoint: config.endpoint,
                    model: config.model,
                    api_key: config.api_key,
                    timeout_ms: config.timeout_ms,
                    max_response_bytes: config.max_response_bytes,
                })?
                .with_auth_mode(auth);
                Ok(Self::Google(client))
            }
        }
    }

    pub fn with_recording(self, recording: Arc<dyn crate::ModelRecording>) -> Self {
        match self {
            Self::OpenAiCompatible(c) => Self::OpenAiCompatible(c.with_recording(recording)),
            Self::OpenAiResponses(c) => Self::OpenAiResponses(c.with_recording(recording)),
            Self::Anthropic(c) => Self::Anthropic(c.with_recording(recording)),
            Self::Google(c) => Self::Google(c.with_recording(recording)),
        }
    }

    pub fn with_observer(self, observer: Arc<dyn crate::ModelObserver>) -> Self {
        match self {
            Self::OpenAiCompatible(c) => Self::OpenAiCompatible(c.with_observer(observer)),
            Self::OpenAiResponses(c) => Self::OpenAiResponses(c.with_observer(observer)),
            Self::Anthropic(c) => Self::Anthropic(c.with_observer(observer)),
            Self::Google(c) => Self::Google(c.with_observer(observer)),
        }
    }
}

#[async_trait::async_trait]
impl Client for TransportClient {
    async fn complete(
        &self,
        content: &Content,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        cancellation: CancellationToken,
    ) -> Result<Response, Error> {
        match self {
            Self::OpenAiCompatible(c) => {
                c.complete(content, auditor, variables, cancellation).await
            }
            Self::OpenAiResponses(c) => c.complete(content, auditor, variables, cancellation).await,
            Self::Anthropic(c) => c.complete(content, auditor, variables, cancellation).await,
            Self::Google(c) => c.complete(content, auditor, variables, cancellation).await,
        }
    }

    async fn stream(
        &self,
        content: &Content,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        cancellation: CancellationToken,
        events: Option<Sender<ModelEvent>>,
    ) -> Result<Response, Error> {
        match self {
            Self::OpenAiCompatible(c) => {
                c.stream(content, auditor, variables, cancellation, events)
                    .await
            }
            Self::OpenAiResponses(c) => {
                c.stream(content, auditor, variables, cancellation, events)
                    .await
            }
            Self::Anthropic(c) => {
                c.stream(content, auditor, variables, cancellation, events)
                    .await
            }
            Self::Google(c) => {
                c.stream(content, auditor, variables, cancellation, events)
                    .await
            }
        }
    }
}

pub fn create_client(config: TransportConfig) -> Result<Arc<dyn Client>, Error> {
    Ok(Arc::new(TransportClient::new(config)?))
}
