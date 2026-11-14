//! Anthropic Messages API wire protocol implementation.
use crate::call::ProviderCallAudit;
use crate::error::Error;
use crate::observation::emit;
use crate::sse::SseFrameReader;
use crate::{Client, Content, ModelEvent, render_ast};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::StreamExt;
use microcrab_auditor::{Auditor, HookContext, HookPayload, Moment, VariableView};
use microcrab_protocol::{ImageSource, InputPart, Message, Request, Response, ToolCall, Usage};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct AnthropicConfig {
    pub endpoint: String,
    pub model: String,
    pub api_key: Option<String>,
    /// Maximum network inactivity; received chunks renew it. No total call limit.
    pub timeout_ms: u64,
    pub max_response_bytes: usize,
}

impl std::fmt::Debug for AnthropicConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnthropicConfig")
            .field("endpoint", &"<configured>")
            .field("model", &self.model)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field("timeout_ms", &self.timeout_ms)
            .field("max_response_bytes", &self.max_response_bytes)
            .finish()
    }
}

#[derive(Clone)]
pub struct Anthropic {
    auth: crate::AuthMode,
    config: AnthropicConfig,
    resolved_endpoint: String,
    http: reqwest::Client,
    observer: Option<std::sync::Arc<dyn crate::ModelObserver>>,
    recording: Option<std::sync::Arc<dyn crate::ModelRecording>>,
}

impl std::fmt::Debug for Anthropic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Anthropic")
            .field("config", &self.config)
            .finish()
    }
}

pub(crate) fn resolve_endpoint(endpoint: &str) -> Result<String, Error> {
    crate::config::resolve_resource_endpoint(endpoint, "/messages")
}

impl Anthropic {
    pub fn new(config: AnthropicConfig) -> Result<Self, Error> {
        let endpoint = resolve_endpoint(&config.endpoint)?;
        let url = reqwest::Url::parse(&endpoint)
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
            || url.host_str().is_none()
        {
            return Err(Error::config(
                "provider endpoint requires HTTPS (or loopback HTTP), without credentials or fragment",
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
        let http = crate::http::client_builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_millis(config.timeout_ms.min(20_000)))
            .build()
            .map_err(|_| Error::config("cannot construct provider HTTP client"))?;
        Ok(Self {
            auth: crate::AuthMode::default(),
            config,
            resolved_endpoint: endpoint,
            http,
            observer: None,
            recording: None,
        })
    }

    pub fn with_auth_mode(mut self, auth: crate::AuthMode) -> Self {
        self.auth = auth;
        self
    }

    pub fn with_recording(mut self, recording: std::sync::Arc<dyn crate::ModelRecording>) -> Self {
        self.recording = Some(recording);
        self
    }

    pub fn with_observer(mut self, observer: std::sync::Arc<dyn crate::ModelObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    pub fn adapt(&self, request: &Request) -> Result<Value, Error> {
        let mut system = String::new();
        let mut messages: Vec<Value> = Vec::new();

        fn push_role_block(messages: &mut Vec<Value>, role: &'static str, block: Value) {
            if let Some(last) = messages.last_mut().filter(|m| m["role"] == role)
                && let Some(content) = last["content"].as_array_mut()
            {
                content.push(block);
                return;
            }
            messages.push(json!({
                "role": role,
                "content": [block]
            }));
        }

        for msg in &request.messages {
            match msg {
                Message::System { content } => {
                    if !system.is_empty() {
                        system.push('\n');
                    }
                    system.push_str(content);
                }
                Message::User { parts } => {
                    for part in parts {
                        match part {
                            InputPart::Text { text } => {
                                push_role_block(
                                    &mut messages,
                                    "user",
                                    json!({"type": "text", "text": text}),
                                );
                            }
                            InputPart::Image { source } => match source {
                                ImageSource::Url { url } => {
                                    push_role_block(
                                        &mut messages,
                                        "user",
                                        json!({
                                            "type": "image",
                                            "source": { "type": "url", "url": url }
                                        }),
                                    );
                                }
                                ImageSource::Data { media_type, data } => {
                                    push_role_block(
                                        &mut messages,
                                        "user",
                                        json!({
                                            "type": "image",
                                            "source": {
                                                "type": "base64",
                                                "media_type": media_type,
                                                "data": STANDARD.encode(data)
                                            }
                                        }),
                                    );
                                }
                                ImageSource::Attachment { .. } => {
                                    return Err(Error::protocol(
                                        "attachments must be resolved before provider rendering",
                                    ));
                                }
                                ImageSource::Path { path } => {
                                    let (media_type, bytes) =
                                        crate::content::read_local_image(path)?;
                                    push_role_block(
                                        &mut messages,
                                        "user",
                                        json!({
                                            "type": "image",
                                            "source": {
                                                "type": "base64",
                                                "media_type": media_type,
                                                "data": STANDARD.encode(bytes)
                                            }
                                        }),
                                    );
                                }
                            },
                        }
                    }
                }
                Message::Assistant {
                    content,
                    tool_calls,
                    provider_metadata,
                } => {
                    for block in crate::replay::native_parts(
                        content,
                        tool_calls,
                        provider_metadata,
                        "anthropic",
                    )? {
                        push_role_block(&mut messages, "assistant", block);
                    }
                }
                Message::Tool { call_id, content } => {
                    push_role_block(
                        &mut messages,
                        "user",
                        json!({
                            "type": "tool_result",
                            "tool_use_id": call_id,
                            "content": content
                        }),
                    );
                }
            }
        }

        let mut body = json!({
            "model": self.config.model,
            "max_tokens": request.max_output_tokens,
            "messages": messages,
            "stream": true,
        });

        if !system.is_empty() {
            body["system"] = json!(system);
        }

        if !request.tools.is_empty() {
            body["tools"] = Value::Array(
                request
                    .tools
                    .iter()
                    .map(|tool| {
                        json!({
                            "name": tool.name,
                            "description": tool.description,
                            "input_schema": tool.parameters,
                        })
                    })
                    .collect(),
            );
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
        let mut builder = self
            .http
            .post(&self.resolved_endpoint)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(
                reqwest::header::ACCEPT,
                "text/event-stream, application/json",
            )
            .body(wire_request);

        builder = self.auth.apply(
            builder,
            crate::Protocol::Anthropic,
            self.config.api_key.as_deref(),
        );

        let response = crate::idle::wait(
            Duration::from_millis(self.config.timeout_ms),
            builder.send(),
        )
        .await?
        .map_err(|error| crate::failure::transport_error(error, "anthropic", "HTTP request"))?;

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

        let mut sse_reader = SseFrameReader::default();
        let mut sse_decoder = AnthropicSseDecoder::default();
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
                    crate::failure::transport_error(error, "anthropic", "response body"),
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
                match sse_reader.push(&chunk, false) {
                    Ok(frames) => {
                        for frame in frames {
                            match sse_decoder.push_frame(frame) {
                                Ok(deltas) => {
                                    for delta in deltas {
                                        audit.stream_delta(&delta);
                                        emit(&events, observation, delta).await?;
                                    }
                                }
                                Err(err) => {
                                    decode_error = Some(err);
                                    break;
                                }
                            }
                        }
                    }
                    Err(err) => decode_error = Some(err),
                }
            }
        }

        audit.received_complete();
        if !status.is_success() {
            return Err(Error::ProviderStatus {
                status: status.as_u16(),
            });
        }
        if let Some(err) = decode_error {
            return Err(err);
        }

        let response = if is_sse {
            for frame in sse_reader.push(&[], true)? {
                for delta in sse_decoder.push_frame(frame)? {
                    audit.stream_delta(&delta);
                    emit(&events, observation, delta).await?;
                }
            }
            audit.stream_complete();
            sse_decoder.finish(request)?
        } else if is_json {
            let value: Value = serde_json::from_slice(&audit.received).map_err(Error::decode)?;
            // Only explicit plaintext is display evidence. Never expose signatures,
            // encrypted/redacted blocks, or data from a response rejected below.
            let reasoning = value
                .get("content")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter(|part| part["type"] == "thinking")
                .filter_map(|part| {
                    part.get("thinking")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .collect::<Vec<_>>();
            let response = decode_json_response(value, request)?;
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
impl Client for Anthropic {
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
        let ast = render_ast(content)?;
        let request = serde_json::to_string(&self.adapt(&ast)?)?;
        if request.len() > content.max_bytes {
            return Err(Error::Limit("provider request byte budget".into()));
        }
        let call_id = uuid::Uuid::new_v4().to_string();

        let injection = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(Error::Cancelled),
            result = auditor.intervene(
                Moment::RequestPrepared,
                HookContext {
                    epoch: content.epoch,
                    subagent: content.subagent.clone(),
                    turn: content.turn_id,
                    round: content.round,
                    call_id: Some(call_id.clone()),
                    request_body: Some(&request),
                    max_request_bytes: Some(content.max_bytes),
                    variables,
                    payload: HookPayload::None,
                },
            ) => result?,
        };

        let request = match injection.request_body {
            Some(replacement) => {
                crate::validate_request_rewrite(&request, &replacement, content.max_bytes)?;
                replacement
            }
            None => request,
        };

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
        audit.finish(&result);
        let finishing = observation.finish(&result);
        tokio::select! {
            biased;
            _ = finishing => {},
            _ = cancellation.cancelled() => {},
        }

        result
    }
}

#[derive(Default)]
struct AnthropicSseDecoder {
    started: bool,
    completed: bool,
    stopped: bool,
    blocks: BTreeMap<u32, AnthropicBlock>,
    usage: serde_json::Map<String, Value>,
    raw_details: Vec<Value>,
    stop_reason: Option<String>,
}
struct AnthropicBlock {
    value: Value,
    arguments: String,
    closed: bool,
}

impl AnthropicSseDecoder {
    fn merge_usage(&mut self, usage: &Value) -> Result<(), Error> {
        let fields = usage
            .as_object()
            .ok_or_else(|| Error::protocol("invalid Anthropic usage object"))?;
        self.usage.extend(fields.clone());
        self.raw_details.push(usage.clone());
        // Validate active counters on each fragment; later updates cannot hide malformed data.
        anthropic_usage(&Value::Object(self.usage.clone()))?;
        Ok(())
    }
    fn push_frame(&mut self, frame: crate::sse::SseFrame) -> Result<Vec<ModelEvent>, Error> {
        let value: Value = serde_json::from_str(&frame.data).map_err(Error::decode)?;
        if value.get("error").is_some_and(|v| !v.is_null()) {
            return Err(crate::failure::provider_failure(&value));
        }
        let kind = value
            .get("type")
            .and_then(Value::as_str)
            .or(frame.event.as_deref())
            .unwrap_or("");
        if let (Some(a), Some(b)) = (
            value.get("type").and_then(Value::as_str),
            frame.event.as_deref(),
        ) {
            if a != b {
                return Err(Error::protocol(
                    "Anthropic event name differs from payload type",
                ));
            }
        }
        if kind == "ping" {
            return Ok(vec![]);
        }
        if self.completed {
            return Err(Error::protocol(
                "Anthropic event received after message_stop",
            ));
        }
        if !self.started && kind != "message_start" {
            return Err(Error::protocol(
                "Anthropic content event before message_start",
            ));
        }
        let index = || {
            value
                .get("index")
                .and_then(Value::as_u64)
                .and_then(|n| u32::try_from(n).ok())
                .ok_or_else(|| {
                    Error::protocol("Anthropic content block index missing or out of range")
                })
        };
        let mut events = vec![];
        match kind {
            "message_start" => {
                if self.started || !self.blocks.is_empty() {
                    return Err(Error::protocol("duplicate or late Anthropic message_start"));
                }
                self.started = true;
                if let Some(usage) = value.pointer("/message/usage") {
                    self.merge_usage(usage)?;
                }
            }
            "content_block_start" => {
                if self.stopped {
                    return Err(Error::protocol("Anthropic content after stop reason"));
                }
                let index = index()?;
                if self.blocks.contains_key(&index) {
                    return Err(Error::protocol("duplicate Anthropic content block start"));
                }
                if usize::try_from(index).ok() != Some(self.blocks.len()) {
                    return Err(Error::protocol(
                        "noncontiguous Anthropic content block start index",
                    ));
                }
                let block = value
                    .get("content_block")
                    .filter(|v| v.is_object())
                    .ok_or_else(|| Error::protocol("Anthropic content block missing"))?
                    .clone();
                match block.get("type").and_then(Value::as_str) {
                    Some("text") => {
                        let text = block
                            .get("text")
                            .and_then(Value::as_str)
                            .ok_or_else(|| Error::protocol("invalid Anthropic initial text"))?;
                        if !text.is_empty() {
                            events.push(ModelEvent::TextDelta { text: text.into() });
                        }
                    }
                    Some("thinking") => {
                        let text = block
                            .get("thinking")
                            .and_then(Value::as_str)
                            .ok_or_else(|| Error::protocol("invalid Anthropic initial thinking"))?;
                        if !text.is_empty() {
                            events.push(ModelEvent::ReasoningDelta { text: text.into() });
                        }
                    }
                    Some("redacted_thinking") => {
                        if !block.get("data").is_some_and(Value::is_string) {
                            return Err(Error::protocol("invalid Anthropic redacted thinking"));
                        }
                    }
                    Some("tool_use") => {
                        if !block.get("input").is_some_and(Value::is_object) {
                            return Err(Error::protocol("invalid Anthropic initial tool input"));
                        }
                        let id = block
                            .get("id")
                            .and_then(Value::as_str)
                            .filter(|v| !v.is_empty())
                            .ok_or_else(|| Error::protocol("Anthropic tool id missing"))?;
                        let name = block
                            .get("name")
                            .and_then(Value::as_str)
                            .filter(|v| !v.is_empty())
                            .ok_or_else(|| Error::protocol("Anthropic tool name missing"))?;
                        events.push(ModelEvent::ToolCallDelta {
                            index,
                            id: Some(id.into()),
                            name: Some(name.into()),
                            arguments: if block["input"]
                                .as_object()
                                .is_some_and(serde_json::Map::is_empty)
                            {
                                String::new()
                            } else {
                                block["input"].to_string()
                            },
                        });
                    }
                    _ => return Err(Error::protocol("unsupported Anthropic content block type")),
                }
                self.blocks.insert(
                    index,
                    AnthropicBlock {
                        value: block,
                        arguments: String::new(),
                        closed: false,
                    },
                );
            }
            "content_block_delta" => {
                if self.stopped {
                    return Err(Error::protocol("Anthropic delta after stop reason"));
                }
                let index = index()?;
                let block = self
                    .blocks
                    .get_mut(&index)
                    .ok_or_else(|| Error::protocol("Anthropic delta references unknown block"))?;
                if block.closed {
                    return Err(Error::protocol("Anthropic delta after content_block_stop"));
                }
                let delta = value
                    .get("delta")
                    .ok_or_else(|| Error::protocol("Anthropic block delta missing"))?;
                let (field, expected, event) = match delta.get("type").and_then(Value::as_str) {
                    Some("text_delta") => ("text", "text", 0),
                    Some("thinking_delta") => ("thinking", "thinking", 1),
                    Some("signature_delta") => ("signature", "thinking", 2),
                    Some("input_json_delta") => ("partial_json", "tool_use", 3),
                    _ => return Err(Error::protocol("unsupported Anthropic content delta")),
                };
                if block.value["type"] != expected {
                    return Err(Error::protocol(
                        "Anthropic delta type differs from block type",
                    ));
                }
                let text = delta
                    .get(field)
                    .and_then(Value::as_str)
                    .ok_or_else(|| Error::protocol("invalid Anthropic delta text"))?;
                if event == 3 {
                    block.arguments.push_str(text);
                    events.push(ModelEvent::ToolCallDelta {
                        index,
                        id: None,
                        name: None,
                        arguments: text.into(),
                    });
                } else {
                    match &mut block.value[field] {
                        Value::String(accumulated) => accumulated.push_str(text),
                        value => *value = Value::String(text.to_owned()),
                    }
                    if event == 0 {
                        events.push(ModelEvent::TextDelta { text: text.into() });
                    }
                    if event == 1 {
                        events.push(ModelEvent::ReasoningDelta { text: text.into() });
                    }
                }
            }
            "content_block_stop" => {
                let index = index()?;
                let block = self
                    .blocks
                    .get_mut(&index)
                    .ok_or_else(|| Error::protocol("Anthropic stop references unknown block"))?;
                if block.closed {
                    return Err(Error::protocol("duplicate Anthropic content_block_stop"));
                }
                if block.value["type"] == "tool_use" && !block.arguments.is_empty() {
                    let arguments: Value = serde_json::from_str(&block.arguments)
                        .map_err(|_| Error::protocol("invalid tool arguments JSON"))?;
                    if !arguments.is_object() {
                        return Err(Error::protocol("tool call arguments must be an object"));
                    }
                    let initial = &block.value["input"];
                    if initial.as_object().is_some_and(|v| !v.is_empty()) && initial != &arguments {
                        return Err(Error::protocol(
                            "Anthropic initial tool input differs from streamed arguments",
                        ));
                    }
                    block.value["input"] = arguments;
                }
                block.closed = true;
            }
            "message_delta" => {
                if self.blocks.values().any(|b| !b.closed) {
                    return Err(Error::protocol(
                        "Anthropic message_delta before content block completion",
                    ));
                }
                if let Some(reason) = value.pointer("/delta/stop_reason").filter(|v| !v.is_null()) {
                    let reason = reason
                        .as_str()
                        .ok_or_else(|| Error::protocol("invalid Anthropic stop reason"))?;
                    if self.stopped {
                        return Err(Error::protocol("duplicate Anthropic terminal stop reason"));
                    }
                    self.stop_reason = Some(reason.into());
                    self.stopped = true;
                }
                if let Some(usage) = value.get("usage") {
                    self.merge_usage(usage)?;
                }
            }
            "message_stop" => {
                if self.blocks.values().any(|b| !b.closed) {
                    return Err(Error::protocol(
                        "Anthropic message_stop before content block completion",
                    ));
                }
                self.completed = true;
            }
            _ => return Err(Error::protocol("unsupported Anthropic stream event")),
        }
        Ok(events)
    }
    fn finish(self, request: &Request) -> Result<Response, Error> {
        if !self.completed {
            return Err(Error::decode_reason(
                "provider SSE ended before its terminal frame",
            ));
        }
        let blocks: Vec<_> = self.blocks.into_values().map(|b| b.value).collect();
        let mut response = decode_json_response(
            json!({"content":blocks,"usage":self.usage,"stop_reason":self.stop_reason}),
            request,
        )?;
        response.usage.provider_details = self.raw_details;
        if self.stop_reason.as_deref() == Some("max_tokens") {
            response
                .usage
                .provider_details
                .push(json!({"stop_reason":"max_tokens"}));
        }
        Ok(response)
    }
}

fn anthropic_usage(value: &Value) -> Result<Usage, Error> {
    let mut usage = Usage {
        samples: 1,
        ..Usage::default()
    };
    if value.is_null() {
        return Ok(usage);
    }
    if !value.is_object() {
        return Err(Error::protocol("invalid Anthropic usage object"));
    }
    usage.provider_details.push(value.clone());
    usage.uncached_input_tokens = crate::native_usage::count(value, "input_tokens")?;
    usage.output_tokens = crate::native_usage::count(value, "output_tokens")?;
    // Native Messages omits optional cache categories for a cold prompt. Explicit null stays unknown.
    let cache = |key| -> Result<Option<u64>, Error> {
        if value.get(key).is_none() {
            Ok(usage.uncached_input_tokens.map(|_| 0))
        } else {
            crate::native_usage::count(value, key)
        }
    };
    usage.cache_read_tokens = cache("cache_read_input_tokens")?;
    usage.cache_write_tokens = cache("cache_creation_input_tokens")?;
    usage.input_tokens = crate::native_usage::add(
        crate::native_usage::add(usage.uncached_input_tokens, usage.cache_read_tokens)?,
        usage.cache_write_tokens,
    )?;
    usage.total_tokens = crate::native_usage::add(usage.input_tokens, usage.output_tokens)?;
    Ok(usage)
}

fn decode_json_response(value: Value, request: &Request) -> Result<Response, Error> {
    use microcrab_protocol::{AssistantPart, AssistantProviderMetadata, AssistantReplayHint};
    if value.get("error").is_some_and(|v| !v.is_null()) {
        return Err(crate::failure::provider_failure(&value));
    }
    let blocks = value
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::protocol("Anthropic response content missing"))?;
    let mut content = String::new();
    let mut tool_calls = vec![];
    let mut parts = vec![];
    let mut replay_hints = vec![];
    for block in blocks {
        let index = parts.len();
        match block.get("type").and_then(Value::as_str) {
            Some("text") => {
                let text = block
                    .get("text")
                    .and_then(Value::as_str)
                    .ok_or_else(|| Error::protocol("invalid Anthropic text block"))?;
                content.push_str(text);
                parts.push(AssistantPart::Text { text: text.into() });
            }
            Some("thinking") => {
                let text = block
                    .get("thinking")
                    .and_then(Value::as_str)
                    .ok_or_else(|| Error::protocol("invalid Anthropic thinking block"))?;
                parts.push(AssistantPart::Reasoning { text: text.into() });
                if let Some(signature) = block.get("signature") {
                    if !signature.is_string() {
                        return Err(Error::protocol("invalid Anthropic thinking signature"));
                    }
                    replay_hints.push(AssistantReplayHint {
                        part_index: index,
                        scope: "anthropic".into(),
                        kind: "signature".into(),
                        data: signature.clone(),
                    });
                }
            }
            Some("redacted_thinking") => {
                let data = block
                    .get("data")
                    .filter(|v| v.is_string())
                    .ok_or_else(|| Error::protocol("invalid Anthropic redacted thinking"))?;
                parts.push(AssistantPart::RedactedReasoning);
                replay_hints.push(AssistantReplayHint {
                    part_index: index,
                    scope: "anthropic".into(),
                    kind: "redacted".into(),
                    data: data.clone(),
                });
            }
            Some("tool_use") => {
                let id = block
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| Error::protocol("Anthropic tool id missing"))?
                    .to_owned();
                let name = block
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| Error::protocol("Anthropic tool name missing"))?
                    .to_owned();
                let arguments = block
                    .get("input")
                    .filter(|v| v.is_object())
                    .ok_or_else(|| Error::protocol("Anthropic tool input missing or invalid"))?
                    .clone();
                parts.push(AssistantPart::ToolCall {
                    call_id: id.clone(),
                });
                tool_calls.push(ToolCall {
                    id,
                    name,
                    arguments,
                    provider_metadata: None,
                });
            }
            _ => return Err(Error::protocol("unsupported Anthropic response block")),
        }
    }
    let mut usage = anthropic_usage(value.get("usage").unwrap_or(&Value::Null))?;
    if value.get("stop_reason").and_then(Value::as_str) == Some("max_tokens") {
        if !tool_calls.is_empty() {
            return Err(Error::protocol(
                "Anthropic tool calls stopped at the output limit",
            ));
        }
        usage
            .provider_details
            .push(json!({"stop_reason":"max_tokens"}));
    }
    let mut metadata = AssistantProviderMetadata::Neutral {
        parts,
        replay_hints,
    };
    crate::replay::seal(&mut metadata, &tool_calls)?;
    let response = Response {
        content,
        tool_calls,
        usage,
        provider_metadata: Some(metadata),
    };
    crate::validate_response(&response, &request.tools)?;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use microcrab_protocol::ToolDefinition;

    fn request() -> Request {
        Request {
            epoch: 1,
            messages: vec![],
            tools: vec![ToolDefinition {
                name: "echo".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }],
            max_output_tokens: 1024,
        }
    }

    #[test]
    fn adaptation_preserves_job_id_union_schema() {
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
        let mut request = request();
        request.tools = vec![ToolDefinition {
            name: "job".into(),
            description: "Control executions".into(),
            parameters: schema.clone(),
        }];
        let client = Anthropic::new(AnthropicConfig {
            endpoint: "http://127.0.0.1:1/messages".into(),
            model: "fixture".into(),
            api_key: None,
            timeout_ms: 1000,
            max_response_bytes: 65536,
        })
        .unwrap();
        let body = client.adapt(&request).unwrap();
        assert_eq!(body["tools"][0]["input_schema"], schema);
    }

    fn push(decoder: &mut AnthropicSseDecoder, value: Value) -> Result<(), Error> {
        decoder
            .push_frame(crate::sse::SseFrame {
                event: None,
                data: value.to_string(),
            })
            .map(|_| ())
    }

    fn text_stream() -> AnthropicSseDecoder {
        let mut decoder = AnthropicSseDecoder::default();
        push(
            &mut decoder,
            json!({"type":"message_start","message":{
            "usage":{"input_tokens":2}}}),
        )
        .expect("message start");
        push(
            &mut decoder,
            json!({"type":"content_block_start","index":0,
            "content_block":{"type":"text","text":""}}),
        )
        .expect("text start");
        push(
            &mut decoder,
            json!({"type":"content_block_delta","index":0,
            "delta":{"type":"text_delta","text":"hello"}}),
        )
        .expect("text delta");
        push(&mut decoder, json!({"type":"content_block_stop","index":0})).expect("text stop");
        decoder
    }

    #[test]
    fn sse_requires_message_stop_even_with_complete_text_or_tool_json() {
        assert!(matches!(
            text_stream().finish(&request()),
            Err(Error::Protocol(_))
        ));
        let mut decoder = AnthropicSseDecoder::default();
        push(&mut decoder, json!({"type":"message_start","message":{}})).unwrap();
        push(
            &mut decoder,
            json!({"type":"content_block_start","index":0,
            "content_block":{"type":"tool_use","id":"call","name":"echo","input":{}}}),
        )
        .expect("tool start");
        push(
            &mut decoder,
            json!({"type":"content_block_delta","index":0,
            "delta":{"type":"input_json_delta","partial_json":"{}"}}),
        )
        .expect("tool arguments");
        assert!(matches!(
            decoder.finish(&request()),
            Err(Error::Protocol(_))
        ));
    }

    #[test]
    fn sse_message_stop_preserves_text_usage_and_stop_reasons() {
        for reason in ["end_turn", "stop_sequence", "max_tokens"] {
            let mut decoder = text_stream();
            push(
                &mut decoder,
                json!({"type":"message_delta","delta":{"stop_reason":reason},
                "usage":{"output_tokens":3}}),
            )
            .expect("message delta");
            push(&mut decoder, json!({"type":"message_stop"})).expect("message stop");
            let response = decoder.finish(&request()).expect("complete stream");
            assert_eq!(response.content, "hello", "stop reason {reason}");
            assert_eq!(response.usage.total_tokens, Some(5));
        }
    }

    #[test]
    fn sse_message_stop_preserves_tool_arguments() {
        let mut decoder = AnthropicSseDecoder::default();
        push(&mut decoder, json!({"type":"message_start","message":{}})).expect("message start");
        push(
            &mut decoder,
            json!({"type":"content_block_start","index":0,
            "content_block":{"type":"tool_use","id":"call","name":"echo","input":{}}}),
        )
        .expect("tool start");
        push(
            &mut decoder,
            json!({"type":"content_block_delta","index":0,
            "delta":{"type":"input_json_delta","partial_json":"{\"text\":\"hello\"}"}}),
        )
        .expect("tool arguments");
        push(&mut decoder, json!({"type":"content_block_stop","index":0})).expect("tool stop");
        push(
            &mut decoder,
            json!({"type":"message_delta","delta":{"stop_reason":"tool_use"}}),
        )
        .expect("message delta");
        push(&mut decoder, json!({"type":"message_stop"})).expect("message stop");
        let response = decoder.finish(&request()).expect("complete tool stream");
        assert_eq!(response.tool_calls[0].arguments, json!({"text":"hello"}));
    }

    #[test]
    fn sse_provider_error_is_not_a_successful_terminal() {
        let mut decoder = text_stream();
        assert!(matches!(
            push(
                &mut decoder,
                json!({"type":"error","error":{
            "type":"overloaded_error","message":"overloaded"}})
            ),
            Err(Error::ProviderUnavailable(_))
        ));
    }
}

#[cfg(test)]
#[path = "anthropic/integrity_tests.rs"]
mod integrity_tests;
