//! Provider transport and request DTOs. Session/journal own conversation history.
pub mod anthropic;
mod call;
mod client;
pub mod config;
mod content;
pub mod dispatcher;
pub mod error;
mod failure;
pub mod google;
pub mod http;
mod idle;
pub mod media;
pub use media::{ImageUrlResolver, MediaProjection};
pub mod model_metadata;
mod native_usage;
mod observation;
pub mod openai_compatible;
pub mod openai_response;
pub mod protocol;
mod render;
mod replay;
mod state_delta;
pub use state_delta::{apply_state_patch, keyed_state_delta};
mod rewrite;
pub(crate) mod sse;

pub use anthropic::{Anthropic, AnthropicConfig};
pub use client::{Client, ModelEvent};
pub use config::{AuthMode, TransportConfig};
pub use content::{
    AgentExchange, CarrierEnvelopeVersion, Content, ContentItem, ExchangeIntent, ImportantInput,
    StateFrame, UserSource,
};
pub use dispatcher::{TransportClient, create_client};
pub use error::{
    EmptyCompletionFacts, EmptyCompletionFinish, Error, IdleTimeout, ProtocolError, ProviderError,
    ProviderFailure,
};
pub use google::{Google, GoogleConfig};
use microcrab_protocol::{Response, ToolDefinition};
pub use model_metadata::{
    MetadataError, MetadataSnapshot, ModelArchitecture, ModelDefaultParameters, ModelMatch,
    ModelMatchKind, ModelMetadata, ModelMetadataManager, ModelPerRequestLimits, ModelPricing,
    ModelTopProvider, OPENROUTER_MODELS_ENDPOINT, OpenRouterModelManager,
};
pub use observation::{ModelObservation, ModelObserver, ModelRecording, ObservationState};
pub use openai_compatible::{
    OpenAiCompatible, OpenAiCompatibleConfig, OpenAiCompatibleRequestOptions,
};
pub use openai_response::{OpenAiResponse, OpenAiResponseConfig};
pub use protocol::Protocol;
pub use render::render_ast;
pub use rewrite::{MAX_REQUEST_REWRITE_BYTES, validate_request_rewrite};
use std::collections::HashSet;

#[cfg(test)]
mod empty_completion_tests;

/// Shared decoded-response validation; history-dependent call-id reuse remains
/// a Session concern, while each provider response validates on its own here.
pub fn validate_response(response: &Response, tools: &[ToolDefinition]) -> Result<(), Error> {
    validate_response_shape(response, tools)?;
    if response.content.trim().is_empty() && response.tool_calls.is_empty() {
        return Err(Error::protocol("empty assistant response"));
    }
    Ok(())
}

pub(crate) fn validate_response_shape(
    response: &Response,
    tools: &[ToolDefinition],
) -> Result<(), Error> {
    let mut ids = HashSet::new();
    for call in &response.tool_calls {
        if call.id.trim().is_empty() {
            return Err(Error::protocol("tool call ID is empty"));
        }
        if !ids.insert(call.id.as_str()) {
            return Err(Error::protocol("duplicate tool call ID"));
        }
        if !call.arguments.is_object() {
            return Err(Error::protocol("tool call arguments must be an object"));
        }
        if !tools.iter().any(|tool| tool.name == call.name) {
            return Err(Error::protocol("tool call names an unknown tool"));
        }
    }
    crate::google::validate_replay(response)?;
    if let Some(metadata) = &response.provider_metadata {
        metadata
            .validate_projection(&response.content, &response.tool_calls)
            .map_err(|reason| Error::Protocol(reason.into()))?;
    }
    Ok(())
}

pub(crate) fn validate_response_with_finish(
    response: &Response,
    tools: &[ToolDefinition],
    finish: EmptyCompletionFinish,
) -> Result<(), Error> {
    validate_response_shape(response, tools)?;
    if response.content.trim().is_empty() && response.tool_calls.is_empty() {
        use microcrab_protocol::{AssistantPart, AssistantProviderMetadata};
        let reasoning_present = match &response.provider_metadata {
            Some(metadata) => {
                let normalized = metadata
                    .normalized(&response.content, &response.tool_calls)
                    .map_err(|reason| Error::Protocol(reason.into()))?;
                match normalized {
                    AssistantProviderMetadata::Neutral { parts, .. } => parts.iter().any(|part| {
                        matches!(part, AssistantPart::Reasoning { text } if !text.is_empty())
                            || matches!(part, AssistantPart::RedactedReasoning)
                    }),
                    AssistantProviderMetadata::Google { .. } => unreachable!("normalized metadata"),
                }
            }
            None => false,
        };
        return Err(Error::Provider(ProviderError::empty_completion(
            EmptyCompletionFacts {
                reasoning_present,
                finish,
            },
        )));
    }
    Ok(())
}
