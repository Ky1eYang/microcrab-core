//! Bounded, cancellable OpenAI-compatible Chat Completions transport.
mod sse;
use crate::Content;
use crate::call::ProviderCallAudit;
use crate::error::Error;
use crate::observation::emit;
use crate::{Client, ModelEvent, render_ast};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::StreamExt;
use microcrab_auditor::{Auditor, VariableView};
use microcrab_protocol::{ImageSource, InputPart, Message, Request, Response, ToolCall, Usage};
use serde_json::{Value, json};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct OpenAiCompatibleConfig {
    /// Complete Chat Completions endpoint, including `/chat/completions`.
    pub endpoint: String,
    pub model: String,
    pub api_key: Option<String>,
    /// Maximum network inactivity; received chunks renew it. No total call limit.
    pub timeout_ms: u64,
    pub max_response_bytes: usize,
}

impl std::fmt::Debug for OpenAiCompatibleConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenAiCompatibleConfig")
            .field("endpoint", &"<configured>")
            .field("model", &self.model)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field("timeout_ms", &self.timeout_ms)
            .field("max_response_bytes", &self.max_response_bytes)
            .finish()
    }
}

/// Optional wire settings preserve existing connection configuration literals.
pub(crate) fn validate_reasoning_effort(effort: Option<&str>) -> Result<(), Error> {
    if effort.is_some_and(|effort| {
        effort.is_empty()
            || effort.len() > 64
            || effort == "default"
            || !effort
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    }) {
        return Err(Error::config("invalid reasoning effort"));
    }
    Ok(())
}

/// Callers authorize capability support; transport validates only the token shape.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OpenAiCompatibleRequestOptions {
    pub reasoning_effort: Option<String>,
}
impl OpenAiCompatibleRequestOptions {
    pub fn validate(&self) -> Result<(), Error> {
        validate_reasoning_effort(self.reasoning_effort.as_deref())
    }
}

#[derive(Clone)]
pub struct OpenAiCompatible {
    auth: crate::AuthMode,
    headers: Vec<(reqwest::header::HeaderName, reqwest::header::HeaderValue)>,
    config: OpenAiCompatibleConfig,
    options: OpenAiCompatibleRequestOptions,
    http: reqwest::Client,
    observer: Option<std::sync::Arc<dyn crate::ModelObserver>>,
    recording: Option<std::sync::Arc<dyn crate::ModelRecording>>,
}

impl std::fmt::Debug for OpenAiCompatible {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenAiCompatible")
            .field("config", &self.config)
            .finish()
    }
}

impl OpenAiCompatible {
    pub fn new(config: OpenAiCompatibleConfig) -> Result<Self, Error> {
        let url = reqwest::Url::parse(&config.endpoint)
            .map_err(|_| Error::config("invalid provider endpoint"))?;
        let local = matches!(
            url.host_str(),
            Some("127.0.0.1" | "localhost" | "[::1]" | "::1")
        ) || url
            .host_str()
            .and_then(|h| h.parse::<std::net::IpAddr>().ok())
            .is_some_and(|ip| match ip {
                std::net::IpAddr::V4(ipv4) => ipv4.is_loopback() || ipv4.is_private(),
                std::net::IpAddr::V6(ipv6) => ipv6.is_loopback(),
            });
        if (!matches!(url.scheme(), "https") && !(url.scheme() == "http" && local))
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || url.query().is_some()
            || url.host_str().is_none()
        {
            return Err(Error::config(
                "provider endpoint requires HTTPS (or loopback HTTP), without credentials, query or fragment",
            ));
        }
        if config.model.trim().is_empty() {
            return Err(Error::config("provider model must be nonempty"));
        }
        if config.timeout_ms == 0 {
            return Err(Error::config(
                "provider inactivity timeout must be positive",
            ));
        }
        if config.max_response_bytes == 0 {
            return Err(Error::config(
                "provider response byte budget must be positive",
            ));
        }
        if let Some(key) = &config.api_key {
            reqwest::header::HeaderValue::from_str(&format!("Bearer {key}"))
                .map_err(|_| Error::config("invalid API credential"))?;
        }
        let mut builder = crate::http::client_builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_millis(config.timeout_ms.min(20_000)));
        if local {
            builder = builder.no_proxy();
        }
        let http = builder
            .build()
            .map_err(|_| Error::config("cannot construct provider HTTP client"))?;
        Ok(Self {
            auth: crate::AuthMode::default(),
            headers: Vec::new(),
            config,
            options: OpenAiCompatibleRequestOptions::default(),
            http,
            observer: None,
            recording: None,
        })
    }

    pub fn with_request_options(
        mut self,
        options: OpenAiCompatibleRequestOptions,
    ) -> Result<Self, Error> {
        options.validate()?;
        self.options = options;
        Ok(self)
    }

    pub fn with_auth_mode(mut self, auth: crate::AuthMode) -> Self {
        self.auth = auth;
        self
    }

    pub fn with_headers(
        mut self,
        headers: std::collections::BTreeMap<String, String>,
    ) -> Result<Self, Error> {
        self.headers = crate::config::parse_custom_headers(&headers)?;
        Ok(self)
    }

    pub fn with_recording(mut self, recording: std::sync::Arc<dyn crate::ModelRecording>) -> Self {
        self.recording = Some(recording);
        self
    }

    pub fn with_observer(mut self, observer: std::sync::Arc<dyn crate::ModelObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    pub(crate) fn adapt(&self, request: &Request) -> Result<Value, Error> {
        let mut messages = Vec::new();
        let mut carriers = Vec::new();
        for message in &request.messages {
            // A complete contiguous tool-result batch must precede every synthetic
            // user image carrier, including when the last tool result has no media.
            if !matches!(message, Message::Tool { .. }) {
                messages.append(&mut carriers);
            }
            match message {
                Message::System { content } => {
                    messages.push(json!({"role":"system", "content":content}))
                }
                Message::User { parts } => {
                    messages.push(json!({"role":"user", "content":user_content(parts)?}))
                }
                Message::Assistant {
                    content,
                    tool_calls,
                    provider_metadata,
                } => {
                    messages.push(chat_replay(content, tool_calls, provider_metadata)?);
                }
                Message::Tool {
                    call_id,
                    content,
                    parts,
                } => {
                    let mut text = content.clone();
                    let mut images = Vec::new();
                    for part in parts {
                        match part {
                            InputPart::Text { text: extra } => {
                                text.push('\n');
                                text.push_str(extra);
                            }
                            InputPart::Image {
                                source: ImageSource::Data { .. } | ImageSource::Url { .. },
                            } => images.push(part.clone()),
                            InputPart::Image { .. } => {
                                return Err(Error::protocol(
                                    "tool images require resolved Data or route URL hints",
                                ));
                            }
                        }
                    }
                    messages.push(json!({"role":"tool", "tool_call_id":call_id,"content":text}));
                    if !images.is_empty() {
                        images.insert(
                            0,
                            InputPart::Text {
                                text: format!("[Image output for tool call {call_id}]"),
                            },
                        );
                        carriers.push(json!({"role":"user", "content":user_content(&images)?}));
                    }
                }
            }
        }
        messages.append(&mut carriers);
        let mut body = json!({"model":self.config.model,"messages":messages,"stream":true,"stream_options":{"include_usage":true},"max_tokens":request.max_output_tokens});
        if !request.tools.is_empty() {
            body["tools"] = Value::Array(request.tools.iter().map(|tool| json!({"type":"function","function":{"name":tool.name,"description":tool.description,"parameters":tool.parameters}})).collect());
        }
        if let Some(effort) = &self.options.reasoning_effort {
            body["reasoning_effort"] = json!(effort);
        }
        Ok(body)
    }

    async fn complete_inner(
        &self,
        request: &Request,
        wire_request: String,
        audit: &mut ProviderCallAudit<'_>,
        observation: &crate::observation::CallObservation,
        events: Option<tokio::sync::mpsc::Sender<ModelEvent>>,
    ) -> Result<Response, Error> {
        // Serialize once: the audited string is exactly the body the transport sends.
        let mut builder = crate::config::apply_custom_headers(
            self.http.post(&self.config.endpoint),
            &self.headers,
        )
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .header(
            reqwest::header::ACCEPT,
            "text/event-stream, application/json",
        )
        .body(wire_request);
        builder = self.auth.apply(
            builder,
            crate::Protocol::OpenaiCompatible,
            self.config.api_key.as_deref(),
        );
        let response = crate::idle::wait(
            Duration::from_millis(self.config.timeout_ms),
            builder.send(),
        )
        .await?
        .map_err(|error| {
            crate::failure::transport_error(error, "openai_compatible", "HTTP request")
        })?;
        let status = response.status();
        audit.received_status(status.as_u16());
        let media_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        audit.received_content_type(media_type.clone());
        let is_sse = media_type == "text/event-stream";
        let is_json = media_type == "application/json" || media_type.ends_with("+json");
        let mut decoder = sse::Decoder::default();
        // Retain parser failures until EOF so complete malformed bodies are audited
        // exactly. The same byte/time budget bounds draining an invalid response.
        let mut decode_error = None;
        let limit = self.config.max_response_bytes;
        let mut warned_large = false;
        let mut chunks = response.bytes_stream();
        while let Some(chunk) =
            crate::idle::wait(Duration::from_millis(self.config.timeout_ms), chunks.next())
                .await
                .map_err(|error| crate::failure::body_error(status, error))?
        {
            let chunk = chunk.map_err(|error| {
                crate::failure::body_error(
                    status,
                    crate::failure::transport_error(error, "openai_compatible", "response body"),
                )
            })?;
            let remaining = limit.saturating_sub(audit.received.len());
            audit
                .received
                .extend_from_slice(&chunk[..chunk.len().min(remaining)]);
            crate::failure::warn_large_response(audit.received.len(), limit, &mut warned_large);
            if chunk.len() > remaining {
                if !status.is_success() {
                    return Err(Error::ProviderStatus {
                        status: status.as_u16(),
                    });
                }
                return Err(Error::Limit("provider response byte budget".into()));
            }
            if status.is_success() && is_sse && decode_error.is_none() {
                match decoder.push(&chunk, false) {
                    Ok(deltas) => {
                        for delta in deltas {
                            audit.stream_delta(&delta);
                            emit(&events, observation, delta).await?;
                        }
                    }
                    Err(error) => decode_error = Some(error),
                }
            }
        }
        audit.received_complete();
        if !status.is_success() {
            if let Some(error) = crate::media::remote_image_fetch_rejection(status, &audit.received)
            {
                return Err(error);
            }
            return Err(Error::ProviderStatus {
                status: status.as_u16(),
            });
        }
        if let Some(error) = decode_error {
            return Err(error);
        }
        let response = if is_sse {
            for delta in decoder.push(&[], true)? {
                audit.stream_delta(&delta);
                emit(&events, observation, delta).await?;
            }
            audit.stream_complete();
            let (value, usage_details, gateway_text_complete) = decoder.into_value()?;
            if gateway_text_complete {
                tracing::info!(
                    completion_evidence = "sse_done_with_usage",
                    finish_reason = "missing",
                    "accepted gateway text completion without provider finish reason"
                );
            }
            let response =
                decode_response(value, request, Some(usage_details), gateway_text_complete)?;
            audit.normalized();
            response
        } else if is_json {
            // Some compatible providers explicitly return JSON despite stream:true.
            // One aggregate text delta describes that fallback without implying SSE.
            let value: Value = serde_json::from_slice(&audit.received).map_err(Error::decode)?;
            // Only explicit plaintext is display evidence. Never expose signatures,
            // encrypted/redacted blocks, or data from a response rejected below.
            let reasoning = value
                .pointer("/choices/0/message")
                .and_then(|message| {
                    message
                        .get("reasoning_content")
                        .or_else(|| message.get("reasoning"))
                })
                .and_then(Value::as_str)
                .map(str::to_owned)
                .into_iter()
                .collect::<Vec<_>>();
            let response = decode_response(value, request, None, false)?;
            audit.normalized();
            for text in reasoning {
                emit(&events, observation, ModelEvent::ReasoningDelta { text }).await?;
            }
            if !response.content.is_empty() {
                emit(
                    &events,
                    observation,
                    ModelEvent::TextDelta {
                        text: response.content.clone(),
                    },
                )
                .await?;
            }
            for (index, call) in response.tool_calls.iter().enumerate() {
                emit(
                    &events,
                    observation,
                    ModelEvent::ToolCallDelta {
                        index: u32::try_from(index)
                            .map_err(|_| Error::Limit("tool index budget".into()))?,
                        id: Some(call.id.clone()),
                        name: Some(call.name.clone()),
                        arguments: call.arguments.to_string(),
                    },
                )
                .await?;
            }
            response
        } else {
            return Err(Error::protocol(
                "unsupported provider response content type",
            ));
        };
        emit(
            &events,
            observation,
            ModelEvent::Finished {
                usage: response.usage.clone(),
            },
        )
        .await?;
        Ok(response)
    }
}

#[async_trait::async_trait]
impl Client for OpenAiCompatible {
    async fn complete(
        &self,
        content: &Content,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        cancellation: CancellationToken,
    ) -> Result<Response, Error> {
        self.stream(content, auditor, variables, cancellation, None)
            .await
    }

    async fn stream(
        &self,
        content: &Content,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        cancellation: CancellationToken,
        events: Option<tokio::sync::mpsc::Sender<ModelEvent>>,
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
        events: Option<tokio::sync::mpsc::Sender<ModelEvent>>,
    ) -> Result<crate::ResponseOutcome, Error> {
        let ast = render_ast(content)?;
        let request = serde_json::to_string(&self.adapt(&ast)?)?;
        if request.len() > content.max_bytes {
            return Err(Error::Limit("provider request byte budget".into()));
        }
        let call_id = uuid::Uuid::new_v4().to_string();
        // This hook runs for every HTTP attempt, including retries, with the same
        // call identity later observed by the sent-body and terminal records.
        let request = crate::call::intervene_request(
            auditor,
            content,
            variables,
            &cancellation,
            &call_id,
            request,
        )
        .await?;

        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let mut observation = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(Error::Cancelled),
            observation = crate::observation::CallObservation::start(
            self.observer.clone(),
            self.recording.clone(),
            content,
            call_id.clone(),
            &cancellation,
        ) => observation,
        };
        let mut audit = ProviderCallAudit::start(auditor, content, &request, call_id);
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(Error::Cancelled),
            result = self.complete_inner(&ast, request, &mut audit, &observation, events) => {
                result.map_err(crate::failure::response_error)
            },
        };
        let result = result.map_err(|error| audit.measured_error(error));
        let attempt = audit.finish(&result);
        let finishing = observation.finish(&result);
        tokio::select! {
            biased;
            _ = finishing => {},
            _ = cancellation.cancelled() => {},
        }

        result.map(|response| crate::ResponseOutcome { response, attempt })
    }
}

fn user_content(parts: &[InputPart]) -> Result<Value, Error> {
    if parts
        .iter()
        .all(|part| matches!(part, InputPart::Text { .. }))
    {
        let mut content = String::new();
        for part in parts {
            if let InputPart::Text { text } = part {
                content.push_str(text);
            }
        }
        return Ok(Value::String(content));
    }
    let mut content = Vec::with_capacity(parts.len());
    for part in parts {
        content.push(match part {
            InputPart::Text { text } => json!({"type":"text", "text":text}),
            InputPart::Image { source } => {
                let url = match source {
                    ImageSource::Url { url } => url.clone(),
                    ImageSource::Data { media_type, data } => {
                        format!("data:{media_type};base64,{}", STANDARD.encode(data))
                    }
                    ImageSource::Attachment { .. } => {
                        return Err(Error::config(
                            "attachments must be resolved before provider rendering",
                        ));
                    }
                    ImageSource::Path { .. } => {
                        return Err(Error::config(
                            "image paths must be resolved before provider rendering",
                        ));
                    }
                };
                json!({"type":"image_url", "image_url":{"url":url}})
            }
        });
    }
    Ok(Value::Array(content))
}

fn decode_response(
    value: Value,
    request: &Request,
    usage_details: Option<Vec<Value>>,
    gateway_text_complete: bool,
) -> Result<Response, Error> {
    if value.get("error").is_some_and(|error| !error.is_null()) {
        return Err(crate::failure::provider_failure(&value));
    }
    let choices = value
        .get("choices")
        .and_then(Value::as_array)
        .filter(|choices| choices.len() == 1)
        .ok_or_else(|| Error::protocol("expected one provider choice"))?;
    let choice = &choices[0];
    let message = choice
        .get("message")
        .filter(|message| message.get("role").and_then(Value::as_str) == Some("assistant"))
        .ok_or_else(|| Error::protocol("missing assistant message"))?;
    let content = match message.get("content") {
        Some(Value::String(content)) => content.clone(),
        None | Some(Value::Null) => String::new(),
        _ => return Err(Error::protocol("unsupported assistant content")),
    };
    let mut calls = Vec::new();
    if let Some(value) = message.get("tool_calls").filter(|value| !value.is_null()) {
        let values = value
            .as_array()
            .ok_or_else(|| Error::protocol("invalid tool calls"))?;
        for call in values {
            if call.get("type").and_then(Value::as_str) != Some("function") {
                return Err(Error::protocol("unsupported tool call type"));
            }
            let text = |value: &Value, key: &str, reason: &'static str| -> Result<String, Error> {
                value
                    .get(key)
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .ok_or_else(|| Error::protocol(reason))
            };
            let function = call
                .get("function")
                .filter(|function| function.is_object())
                .ok_or_else(|| Error::protocol("missing or invalid tool function object"))?;
            let arguments = serde_json::from_str(&text(
                function,
                "arguments",
                "missing or invalid tool function arguments string",
            )?)
            .map_err(|_| Error::protocol("invalid tool argument JSON"))?;
            calls.push(ToolCall {
                provider_metadata: None,
                id: text(call, "id", "missing or invalid tool call id string")?,
                name: text(
                    function,
                    "name",
                    "missing or invalid tool function name string",
                )?,
                arguments,
            });
        }
    }
    let mut usage = decode_usage(value.get("usage"))?;
    let finish = match choice.get("finish_reason") {
        None | Some(Value::Null)
            if gateway_text_complete
                && usage_details.is_some()
                && !content.is_empty()
                && calls.is_empty()
                && usage.input_tokens.is_some()
                && usage.output_tokens.is_some() =>
        {
            None
        }
        None | Some(Value::Null) => {
            return Err(Error::protocol(
                "provider response is incomplete: missing finish reason",
            ));
        }
        Some(Value::String(reason)) => Some(reason.as_str()),
        Some(_) => return Err(Error::protocol("invalid provider finish reason type")),
    };
    if calls.is_empty() {
        if finish.is_some_and(|finish| {
            !matches!(finish, "stop" | "end_turn" | "stop_sequence" | "length")
        }) {
            return Err(Error::protocol(
                "unsupported finish reason for text response",
            ));
        }
    } else if !matches!(finish, Some("tool_calls" | "function_call" | "stop")) {
        return Err(Error::protocol("incompatible finish reason for tool calls"));
    }
    let usage_details_were_streamed = usage_details.is_some();
    if let Some(details) = usage_details {
        usage.provider_details = details;
    }
    use microcrab_protocol::{AssistantPart, AssistantProviderMetadata, AssistantReplayHint};
    let mut parts = Vec::new();
    if usage_details_were_streamed {
        if let Some(native) = message.get("__microcrab_parts") {
            parts = serde_json::from_value(native.clone()).map_err(Error::decode)?;
        }
    } else {
        for key in ["reasoning_content", "reasoning"] {
            if message
                .get(key)
                .is_some_and(|v| !v.is_null() && !v.is_string())
            {
                return Err(Error::protocol("unsupported assistant reasoning content"));
            }
        }
        if let Some(text) = message
            .get("reasoning_content")
            .filter(|v| !v.is_null())
            .or_else(|| message.get("reasoning"))
            .and_then(Value::as_str)
            && !text.is_empty()
        {
            parts.push(AssistantPart::Reasoning { text: text.into() });
        }
        if !content.is_empty() {
            parts.push(AssistantPart::Text {
                text: content.clone(),
            });
        }
        parts.extend(calls.iter().map(|call| AssistantPart::ToolCall {
            call_id: call.id.clone(),
        }));
    }
    let replay_hints = parts
        .iter()
        .enumerate()
        .filter(|(_, p)| matches!(p, AssistantPart::Reasoning { .. }))
        .map(|(part_index, _)| AssistantReplayHint {
            part_index,
            scope: "chat".into(),
            kind: "reasoning_field".into(),
            data: json!("reasoning_content"),
        })
        .collect();
    let response = Response {
        provider_metadata: Some(AssistantProviderMetadata::Neutral {
            parts,
            replay_hints,
        }),
        content,
        tool_calls: calls,
        usage,
    };
    if let Some(refusal) = message.get("refusal").filter(|value| !value.is_null()) {
        let refusal = refusal
            .as_str()
            .ok_or_else(|| Error::protocol("invalid assistant refusal field"))?;
        if !refusal.is_empty()
            && response.content.trim().is_empty()
            && response.tool_calls.is_empty()
        {
            crate::validate_response_shape(&response, &request.tools)?;
            return Err(Error::Provider(
                "provider refused the assistant response".into(),
            ));
        }
    }
    crate::validate_response_with_finish(
        &response,
        &request.tools,
        match finish {
            Some("stop" | "end_turn" | "stop_sequence") => crate::EmptyCompletionFinish::Stop,
            Some("length") => crate::EmptyCompletionFinish::Length,
            _ => crate::EmptyCompletionFinish::Unknown,
        },
    )?;
    Ok(response)
}

pub(crate) fn chat_replay(
    content: &str,
    calls: &[ToolCall],
    metadata: &Option<microcrab_protocol::AssistantProviderMetadata>,
) -> Result<Value, Error> {
    use microcrab_protocol::{AssistantPart, AssistantProviderMetadata};
    let AssistantProviderMetadata::Neutral {
        parts,
        replay_hints,
    } = crate::replay::normalized(content, calls, metadata)?
    else {
        unreachable!("normalized replay")
    };
    let mut text = String::new();
    let mut reasoning = String::new();
    for (index, part) in parts.iter().enumerate() {
        match part {
            AssistantPart::Text { text: value } => text.push_str(value),
            AssistantPart::Reasoning { text: value } => {
                if crate::replay::hint(&replay_hints, index, "chat", "reasoning_field").is_some() {
                    reasoning.push_str(value);
                } else {
                    text.push_str(&crate::replay::fallback_reasoning(value));
                }
            }
            AssistantPart::RedactedReasoning => {
                text.push_str("[Reasoning unavailable: provider withheld plaintext]")
            }
            _ => {}
        }
    }
    let mut message = json!({"role":"assistant","content":text});
    if !reasoning.is_empty() {
        message["reasoning_content"] = json!(reasoning);
    }
    if !calls.is_empty() {
        message["tool_calls"]=json!(calls.iter().map(|call|json!({"id":call.id,"type":"function","function":{"name":call.name,"arguments":call.arguments.to_string()}})).collect::<Vec<_>>());
    }
    Ok(message)
}

/// Select one accounting schema before normalizing counters. Gateway provenance
/// (`usage_source`/billing metadata) does not override the wire's usage semantics.
/// Foreign counters stay in provider_details, including contradictory zeros.
fn decode_usage(value: Option<&Value>) -> Result<Usage, Error> {
    let mut usage = Usage {
        samples: 1,
        ..Usage::default()
    };
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(usage);
    };
    if !value.is_object() {
        return Err(Error::protocol("invalid usage object"));
    }
    usage.provider_details.push(value.clone());

    enum Semantic {
        OpenAi,
        InputAlias,
        Anthropic,
    }
    let present = |key: &str| value.get(key).is_some_and(|value| !value.is_null());
    let anthropic_cache = [
        "cache_read_input_tokens",
        "cache_creation_input_tokens",
        "cache_write_input_tokens",
    ]
    .iter()
    .any(|key| present(key));
    let exclusive_input = present("input_tokens") && anthropic_cache;
    let semantic = match value.get("usage_semantic").and_then(Value::as_str) {
        Some("openai") => Semantic::OpenAi,
        Some("anthropic") => Semantic::Anthropic,
        _ if present("completion_tokens") || (present("prompt_tokens") && !exclusive_input) => {
            Semantic::OpenAi
        }
        _ if anthropic_cache => Semantic::Anthropic,
        _ if present("input_tokens") || present("output_tokens") => Semantic::InputAlias,
        _ if present("completion_tokens_details") || present("prompt_tokens_details") => {
            Semantic::OpenAi
        }
        _ => Semantic::InputAlias,
    };
    // Only aliases inside the selected schema must agree. Null remains unknown;
    // malformed selected counters, overflow and component bounds still fail.
    let count = |paths: &[&[&str]]| -> Result<Option<u64>, Error> {
        let mut result = None;
        for path in paths {
            let mut current = value;
            for key in *path {
                if current.is_null() {
                    break;
                }
                if !current.is_object() {
                    return Err(Error::protocol("invalid usage details object"));
                }
                current = current.get(*key).unwrap_or(&Value::Null);
            }
            if current.is_null() {
                continue;
            }
            let number = current
                .as_u64()
                .ok_or_else(|| Error::protocol("invalid usage count"))?;
            if result.is_some_and(|old| old != number) {
                return Err(Error::protocol("conflicting usage aliases"));
            }
            result = Some(number);
        }
        Ok(result)
    };
    let reported_total = match semantic {
        Semantic::OpenAi => {
            usage.input_tokens = count(&[&["prompt_tokens"]])?;
            usage.output_tokens = count(&[&["completion_tokens"]])?;
            usage.cache_read_tokens = count(&[
                &["prompt_tokens_details", "cached_tokens"],
                &["prompt_cache_hit_tokens"],
                &["cache_read_tokens"],
            ])?;
            usage.cache_write_tokens = count(&[
                &["prompt_tokens_details", "cache_write_tokens"],
                &["prompt_tokens_details", "cache_creation_tokens"],
                &["cache_write_tokens"],
            ])?;
            usage.reasoning_tokens = count(&[
                &["completion_tokens_details", "reasoning_tokens"],
                &["reasoning_tokens"],
            ])?;
            count(&[&["total_tokens"]])?
        }
        Semantic::InputAlias => {
            usage.input_tokens = count(&[&["input_tokens"]])?;
            usage.output_tokens = count(&[&["output_tokens"]])?;
            usage.cache_read_tokens = count(&[
                &["input_tokens_details", "cached_tokens"],
                &["cache_read_tokens"],
            ])?;
            usage.cache_write_tokens = count(&[
                &["input_tokens_details", "cache_write_tokens"],
                &["input_tokens_details", "cache_creation_tokens"],
                &["cache_write_tokens"],
            ])?;
            usage.reasoning_tokens = count(&[
                &["output_tokens_details", "reasoning_tokens"],
                &["reasoning_tokens"],
            ])?;
            count(&[&["total_tokens"]])?
        }
        Semantic::Anthropic => {
            usage.uncached_input_tokens = count(&[&["input_tokens"], &["uncached_input_tokens"]])?;
            usage.output_tokens = count(&[&["output_tokens"]])?;
            usage.cache_read_tokens =
                count(&[&["cache_read_input_tokens"], &["cache_read_tokens"]])?;
            usage.cache_write_tokens = count(&[
                &["cache_creation_input_tokens"],
                &["cache_write_input_tokens"],
                &["cache_write_tokens"],
            ])?;
            usage.reasoning_tokens = count(&[&["reasoning_tokens"]])?;
            usage.input_tokens = add_counts(
                add_counts(usage.uncached_input_tokens, usage.cache_read_tokens)?,
                usage.cache_write_tokens,
            )?;
            // OpenAI total_tokens belongs to a different accounting schema.
            None
        }
    };
    if !matches!(semantic, Semantic::Anthropic) {
        let reported_uncached = match semantic {
            Semantic::OpenAi => {
                count(&[&["prompt_cache_miss_tokens"], &["uncached_input_tokens"]])?
            }
            _ => count(&[&["uncached_input_tokens"]])?,
        };
        let derived = subtract_counts(
            subtract_counts(usage.input_tokens, usage.cache_read_tokens)?,
            usage.cache_write_tokens,
        )?;
        usage.uncached_input_tokens = agree(reported_uncached, derived)?;
    }
    // Preserve the existing compatible visible+reasoning convention only when
    // the selected schema's explicit total proves that accounting exactly.
    let exclusive_reasoning = match (
        usage.input_tokens,
        usage.output_tokens,
        usage.reasoning_tokens,
        reported_total,
    ) {
        (Some(input), Some(output), Some(reasoning), Some(total)) if reasoning > 0 => output
            .checked_add(reasoning)
            .is_some_and(|combined| total.checked_sub(input) == Some(combined)),
        _ => false,
    };
    if exclusive_reasoning {
        usage.non_reasoning_output_tokens = usage.output_tokens;
        usage.output_tokens = add_counts(usage.output_tokens, usage.reasoning_tokens)?;
    } else {
        usage.non_reasoning_output_tokens =
            subtract_counts(usage.output_tokens, usage.reasoning_tokens)?;
    }
    usage.total_tokens = match reported_total {
        Some(total) => Some(total),
        None => add_counts(usage.input_tokens, usage.output_tokens)?,
    };
    Ok(usage)
}

fn agree(left: Option<u64>, right: Option<u64>) -> Result<Option<u64>, Error> {
    match (left, right) {
        (Some(left), Some(right)) if left != right => {
            Err(Error::protocol("conflicting usage counts"))
        }
        _ => Ok(left.or(right)),
    }
}

fn add_counts(left: Option<u64>, right: Option<u64>) -> Result<Option<u64>, Error> {
    left.zip(right)
        .map(|(left, right)| {
            left.checked_add(right)
                .ok_or_else(|| Error::protocol("usage count overflow"))
        })
        .transpose()
}

fn subtract_counts(left: Option<u64>, right: Option<u64>) -> Result<Option<u64>, Error> {
    left.zip(right)
        .map(|(left, right)| {
            left.checked_sub(right)
                .ok_or_else(|| Error::protocol("usage component exceeds total"))
        })
        .transpose()
}

#[cfg(test)]
mod request_options_tests {
    use super::*;
    use microcrab_protocol::ToolDefinition;

    #[test]
    fn effort_changes_only_the_declared_wire_field() {
        let client = OpenAiCompatible::new(OpenAiCompatibleConfig {
            endpoint: "http://127.0.0.1:1/chat/completions".into(),
            model: "fixture".into(),
            api_key: None,
            timeout_ms: 1000,
            max_response_bytes: 1024,
        })
        .unwrap();
        let schema = json!({
            "type": "object",
            "properties": {
                "action": {"type": "string", "enum": ["list", "wait", "cancel"]},
                "id": {"oneOf": [
                    {"type": "string", "minLength": 1},
                    {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 16,
                        "uniqueItems": true,
                        "items": {"type": "string", "minLength": 1}
                    }
                ]}
            },
            "required": ["id", "action"]
        });
        let request = Request {
            epoch: 1,
            messages: vec![Message::System {
                content: "frozen".into(),
            }],
            tools: vec![ToolDefinition {
                name: "job".into(),
                description: "Control executions".into(),
                parameters: schema.clone(),
            }],
            max_output_tokens: 16,
        };
        let baseline = client.adapt(&request).unwrap();
        assert_eq!(baseline["tools"][0]["function"]["parameters"], schema);
        assert!(baseline.get("reasoning_effort").is_none());
        for effort in ["low", "medium", "high", "xhigh", "max"] {
            let configured = client
                .clone()
                .with_request_options(OpenAiCompatibleRequestOptions {
                    reasoning_effort: Some(effort.into()),
                })
                .unwrap();
            let mut body = configured.adapt(&request).unwrap();
            assert_eq!(
                body.as_object_mut().unwrap().remove("reasoning_effort"),
                Some(json!(effort))
            );
            assert_eq!(body, baseline);
        }
        for effort in ["", "default", "has space", "newline\n", "\u{2603}"] {
            assert!(
                client
                    .clone()
                    .with_request_options(OpenAiCompatibleRequestOptions {
                        reasoning_effort: Some(effort.into())
                    })
                    .is_err()
            );
        }
    }
}
