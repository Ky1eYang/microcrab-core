//! OpenAI Responses API wire protocol implementation (`/v1/responses`).
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
pub struct OpenAiResponseConfig {
    pub endpoint: String,
    pub model: String,
    pub api_key: Option<String>,
    /// Maximum network inactivity; received chunks renew it. No total call limit.
    pub timeout_ms: u64,
    pub max_response_bytes: usize,
}

impl std::fmt::Debug for OpenAiResponseConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenAiResponseConfig")
            .field("endpoint", &"<configured>")
            .field("model", &self.model)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field("timeout_ms", &self.timeout_ms)
            .field("max_response_bytes", &self.max_response_bytes)
            .finish()
    }
}

#[derive(Clone)]
pub struct OpenAiResponse {
    auth: crate::AuthMode,
    config: OpenAiResponseConfig,
    resolved_endpoint: String,
    http: reqwest::Client,
    observer: Option<std::sync::Arc<dyn crate::ModelObserver>>,
    recording: Option<std::sync::Arc<dyn crate::ModelRecording>>,
}

impl std::fmt::Debug for OpenAiResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenAiResponse")
            .field("config", &self.config)
            .finish()
    }
}

pub(crate) fn resolve_endpoint(endpoint: &str) -> Result<String, Error> {
    crate::config::resolve_resource_endpoint(endpoint, "/responses")
}

impl OpenAiResponse {
    pub fn new(config: OpenAiResponseConfig) -> Result<Self, Error> {
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
        let mut instructions = String::new();
        let mut input: Vec<Value> = Vec::new();

        for msg in &request.messages {
            match msg {
                Message::System { content } => {
                    if !instructions.is_empty() {
                        instructions.push('\n');
                    }
                    instructions.push_str(content);
                }
                Message::User { parts } => {
                    let mut content_parts = Vec::new();
                    for part in parts {
                        match part {
                            InputPart::Text { text } => {
                                content_parts.push(json!({
                                    "type": "input_text",
                                    "text": text,
                                }));
                            }
                            InputPart::Image { source } => match source {
                                ImageSource::Attachment { .. } => {
                                    return Err(Error::protocol(
                                        "attachments must be resolved before provider rendering",
                                    ));
                                }
                                ImageSource::Url { url } => {
                                    content_parts.push(json!({
                                        "type": "input_image",
                                        "image_url": url,
                                    }));
                                }
                                ImageSource::Data { media_type, data } => {
                                    let b64 = STANDARD.encode(data);
                                    content_parts.push(json!({
                                        "type": "input_image",
                                        "image_url": format!("data:{media_type};base64,{b64}"),
                                    }));
                                }
                                ImageSource::Path { path } => {
                                    let (media_type, bytes) =
                                        crate::content::read_local_image(path)?;
                                    let b64 = STANDARD.encode(bytes);
                                    content_parts.push(json!({
                                        "type": "input_image",
                                        "image_url": format!("data:{media_type};base64,{b64}"),
                                    }));
                                }
                            },
                        }
                    }
                    input.push(json!({
                        "role": "user",
                        "content": content_parts,
                    }));
                }
                Message::Assistant {
                    content,
                    tool_calls,
                    provider_metadata,
                } => {
                    input.extend(crate::replay::native_parts(
                        content,
                        tool_calls,
                        provider_metadata,
                        "responses",
                    )?);
                }
                Message::Tool {
                    call_id,
                    content,
                    parts,
                } => {
                    let output = if parts.is_empty() {
                        json!(content)
                    } else {
                        let mut blocks = vec![json!({"type": "input_text", "text": content})];
                        for part in parts {
                            blocks.push(match part {
                                InputPart::Text { text } => json!({"type": "input_text", "text": text}),
                                InputPart::Image { source } => {
                                    let url = match source {
                                        ImageSource::Data { media_type, data } => format!("data:{media_type};base64,{}", STANDARD.encode(data)),
                                        ImageSource::Url { url } => url.clone(),
                                        _ => return Err(Error::protocol("tool images require resolved Data or route URL hints")),
                                    };
                                    json!({"type": "input_image", "image_url": url})
                                }
                            });
                        }
                        Value::Array(blocks)
                    };
                    input.push(json!({
                        "type": "function_call_output",
                        "call_id": call_id,
                        "output": output,
                    }));
                }
            }
        }

        let mut body = json!({
            "model": self.config.model,
            "input": input,
            "stream": true,
            "store": false,
            "include": ["reasoning.encrypted_content"],
            "max_output_tokens": request.max_output_tokens,
        });

        if !instructions.is_empty() {
            body["instructions"] = json!(instructions);
        }

        if !request.tools.is_empty() {
            body["tools"] = Value::Array(
                request
                    .tools
                    .iter()
                    .map(|tool| {
                        json!({
                            "type": "function",
                            "name": tool.name,
                            "description": tool.description,
                            "parameters": tool.parameters,
                            "strict": false,
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
            crate::Protocol::OpenaiResponses,
            self.config.api_key.as_deref(),
        );

        let response = crate::idle::wait(
            Duration::from_millis(self.config.timeout_ms),
            builder.send(),
        )
        .await?
        .map_err(|error| {
            crate::failure::transport_error(error, "openai_response", "HTTP request")
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

        let mut sse_reader = SseFrameReader::default();
        let mut sse_decoder = OpenAiResponsesSseDecoder::default();
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
                    crate::failure::transport_error(error, "openai_response", "response body"),
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
            if let Some(error) = crate::media::remote_image_fetch_rejection(status, &audit.received)
            {
                return Err(error);
            }
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
                .get("output")
                .or_else(|| value.pointer("/response/output"))
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter(|item| item["type"] == "reasoning")
                .flat_map(|item| {
                    ["summary", "content"].into_iter().flat_map(move |key| {
                        item.get(key)
                            .and_then(Value::as_array)
                            .into_iter()
                            .flatten()
                            .filter_map(|part| {
                                part.get("text").and_then(Value::as_str).map(str::to_owned)
                            })
                    })
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
impl Client for OpenAiResponse {
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
struct OpenAiResponsesSseDecoder {
    completed: bool,
    framing_done: bool,
    lifecycle_done: std::collections::HashSet<(String, u32, u64)>,
    content_text: String,
    reasoning_text: String,
    observed_text: BTreeMap<u32, String>,
    observed_reasoning: BTreeMap<u32, String>,
    text_components: BTreeMap<(u32, u64), String>,
    reasoning_components: BTreeMap<(u32, String, u64), String>,
    tool_calls: BTreeMap<u32, PartialToolCall>,
    items: BTreeMap<u32, Value>,
    closed_items: std::collections::HashSet<u32>,
    usage: Option<Usage>,
    terminal: Option<Value>,
}
#[derive(Default)]
struct PartialToolCall {
    id: String,
    name: String,
    item_id: Option<String>,
    arguments: String,
    arguments_done: bool,
}
fn response_index(value: &Value) -> Result<u32, Error> {
    value
        .get("output_index")
        .and_then(Value::as_u64)
        .and_then(|n| u32::try_from(n).ok())
        .ok_or_else(|| Error::protocol("Responses output index missing or out of range"))
}
fn reconcile_arguments(current: &str, final_text: &str) -> Result<(), Error> {
    if current.is_empty() {
        return Ok(());
    }
    let current: Value = serde_json::from_str(current)
        .map_err(|_| Error::protocol("Responses streamed tool arguments are incomplete"))?;
    let final_value: Value = serde_json::from_str(final_text)
        .map_err(|_| Error::protocol("invalid tool arguments JSON"))?;
    if current != final_value {
        return Err(Error::protocol(
            "Responses final arguments differ from streamed arguments",
        ));
    }
    Ok(())
}
fn item_plaintext(item: &Value) -> Result<String, Error> {
    let mut text = String::new();
    let keys: &[&str] = if item["type"] == "reasoning" {
        &["summary", "content"]
    } else {
        &["content"]
    };
    for key in keys {
        if let Some(value) = item.get(*key) {
            if let Some(value) = value.as_str() {
                text.push_str(value);
                continue;
            }
            let blocks = value
                .as_array()
                .ok_or_else(|| Error::protocol("Responses item content is not an array"))?;
            for block in blocks {
                if let Some(value) = block.get("text") {
                    text.push_str(
                        value
                            .as_str()
                            .ok_or_else(|| Error::protocol("invalid Responses item text"))?,
                    );
                }
            }
        }
    }
    Ok(text)
}
impl OpenAiResponsesSseDecoder {
    // Output/component indices own semantic order; network arrival only owns
    // incremental display timing. Preserve legacy unindexed text only when no
    // indexed evidence exists, and never hide a mixed unassociated suffix.
    fn ordered_observed(&self, index: u32, reasoning: bool) -> String {
        if reasoning {
            let mut result = String::new();
            for kind in [
                "response.reasoning_summary_text.delta",
                "response.reasoning_text.delta",
            ] {
                for ((item, component_kind, _), text) in &self.reasoning_components {
                    if *item == index && component_kind == kind {
                        result.push_str(text);
                    }
                }
            }
            if self
                .reasoning_components
                .keys()
                .any(|(item, _, _)| *item == index)
            {
                result
            } else {
                self.observed_reasoning
                    .get(&index)
                    .cloned()
                    .unwrap_or_default()
            }
        } else {
            let result = self
                .text_components
                .iter()
                .filter(|((item, _), _)| *item == index)
                .map(|(_, text)| text.as_str())
                .collect::<String>();
            if self.text_components.keys().any(|(item, _)| *item == index) {
                result
            } else {
                self.observed_text.get(&index).cloned().unwrap_or_default()
            }
        }
    }
    fn ordered_stream(&self, reasoning: bool) -> Result<String, Error> {
        let (indexed, received) = if reasoning {
            (&self.observed_reasoning, &self.reasoning_text)
        } else {
            (&self.observed_text, &self.content_text)
        };
        if indexed.is_empty() {
            return Ok(received.clone());
        }
        let ordered = indexed
            .keys()
            .map(|index| self.ordered_observed(*index, reasoning))
            .collect::<String>();
        if ordered.len() != received.len() {
            return Err(Error::protocol(
                "Responses indexed content does not cover all streamed text",
            ));
        }
        Ok(ordered)
    }
    fn reconcile_item(&mut self, index: u32, item: &Value) -> Result<(), Error> {
        let started = self
            .items
            .get(&index)
            .ok_or_else(|| Error::protocol("Responses done references unknown item"))?;
        if started.get("type") != item.get("type") {
            return Err(Error::protocol("Responses final item type changed"));
        }
        for key in ["id", "call_id", "name"] {
            if let Some(old) = started.get(key).filter(|v| !v.is_null())
                && item.get(key) != Some(old)
            {
                return Err(Error::protocol("Responses final item identity changed"));
            }
        }
        let observed = if item["type"] == "message" {
            Some(self.ordered_observed(index, false))
        } else if item["type"] == "reasoning" {
            Some(self.ordered_observed(index, true))
        } else {
            None
        };
        if let Some(observed) = observed.filter(|text| !text.is_empty())
            && observed != item_plaintext(item)?
        {
            return Err(Error::protocol(
                "Responses final item text differs from accumulated text",
            ));
        }
        if let Some(call) = self.tool_calls.get_mut(&index) {
            let arguments = item
                .get("arguments")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::protocol("Responses final function arguments missing"))?;
            reconcile_arguments(&call.arguments, arguments)?;
            call.arguments = arguments.into();
            call.arguments_done = true;
        }
        self.items.insert(index, item.clone());
        Ok(())
    }
    fn push_frame(&mut self, frame: crate::sse::SseFrame) -> Result<Vec<ModelEvent>, Error> {
        if frame.data == "[DONE]" {
            if self.framing_done {
                return Err(Error::protocol("duplicate Responses SSE framing sentinel"));
            }
            self.framing_done = true;
            return Ok(vec![]);
        }
        if self.framing_done {
            return Err(Error::protocol("Responses data after SSE framing sentinel"));
        }
        let value: Value = serde_json::from_str(&frame.data).map_err(Error::decode)?;
        if value
            .get("error")
            .filter(|v| !v.is_null())
            .or_else(|| value.pointer("/response/error").filter(|v| !v.is_null()))
            .is_some()
        {
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
        ) && a != b
        {
            return Err(Error::protocol(
                "Responses event name differs from payload type",
            ));
        }
        if self.completed {
            return Err(Error::protocol("Responses event received after completion"));
        }
        let mut events = vec![];
        match kind {
            "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => {
                let delta = value
                    .get("delta")
                    .and_then(Value::as_str)
                    .ok_or_else(|| Error::protocol("invalid Responses reasoning delta"))?;
                if value.get("output_index").is_none() && !self.items.is_empty() {
                    return Err(Error::protocol(
                        "Responses text delta omitted its output item identity",
                    ));
                }
                if let Some(index) = value.get("output_index") {
                    let index = index
                        .as_u64()
                        .and_then(|n| u32::try_from(n).ok())
                        .ok_or_else(|| Error::protocol("invalid Responses output index"))?;
                    if self.closed_items.contains(&index)
                        || self
                            .items
                            .get(&index)
                            .is_none_or(|item| item["type"] != "reasoning")
                    {
                        return Err(Error::protocol(
                            "Responses reasoning delta references unknown or closed item",
                        ));
                    }
                }
                if value.get("output_index").is_some() {
                    self.observed_reasoning
                        .entry(response_index(&value)?)
                        .or_default()
                        .push_str(delta);
                    let component = value
                        .get("summary_index")
                        .or_else(|| value.get("content_index"))
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    self.reasoning_components
                        .entry((response_index(&value)?, kind.to_owned(), component))
                        .or_default()
                        .push_str(delta);
                }
                if value.get("output_index").is_some() {
                    let component = value
                        .get("summary_index")
                        .or_else(|| value.get("content_index"))
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    let done = kind
                        .strip_suffix(".delta")
                        .map(|prefix| format!("{prefix}.done"))
                        .unwrap_or_default();
                    if self
                        .lifecycle_done
                        .contains(&(done, response_index(&value)?, component))
                    {
                        return Err(Error::protocol(
                            "Responses reasoning delta after text completion",
                        ));
                    }
                }
                self.reasoning_text.push_str(delta);
                events.push(ModelEvent::ReasoningDelta { text: delta.into() });
            }
            "response.output_text.delta" => {
                let delta = value
                    .get("delta")
                    .and_then(Value::as_str)
                    .ok_or_else(|| Error::protocol("invalid Responses text delta"))?;
                if value.get("output_index").is_none() && !self.items.is_empty() {
                    return Err(Error::protocol(
                        "Responses text delta omitted its output item identity",
                    ));
                }
                if let Some(index) = value.get("output_index") {
                    let index = index
                        .as_u64()
                        .and_then(|n| u32::try_from(n).ok())
                        .ok_or_else(|| Error::protocol("invalid Responses output index"))?;
                    if self.closed_items.contains(&index)
                        || self
                            .items
                            .get(&index)
                            .is_none_or(|item| item["type"] != "message")
                    {
                        return Err(Error::protocol(
                            "Responses text delta references unknown or closed item",
                        ));
                    }
                }
                if value.get("output_index").is_some() {
                    self.observed_text
                        .entry(response_index(&value)?)
                        .or_default()
                        .push_str(delta);
                    let component = value
                        .get("content_index")
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    self.text_components
                        .entry((response_index(&value)?, component))
                        .or_default()
                        .push_str(delta);
                }
                if value.get("output_index").is_some() {
                    let component = value
                        .get("content_index")
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    if self.lifecycle_done.contains(&(
                        "response.output_text.done".into(),
                        response_index(&value)?,
                        component,
                    )) {
                        return Err(Error::protocol(
                            "Responses text delta after text completion",
                        ));
                    }
                }
                self.content_text.push_str(delta);
                events.push(ModelEvent::TextDelta { text: delta.into() });
            }
            "response.output_item.added" => {
                let index = response_index(&value)?;
                if self.items.contains_key(&index) {
                    return Err(Error::protocol("duplicate Responses output item start"));
                }
                let item = value
                    .get("item")
                    .filter(|v| v.is_object())
                    .ok_or_else(|| Error::protocol("Responses output item missing"))?;
                match item.get("type").and_then(Value::as_str) {
                    Some("function_call") => {
                        let id = item
                            .get("call_id")
                            .and_then(Value::as_str)
                            .filter(|v| !v.is_empty())
                            .ok_or_else(|| Error::protocol("Responses call id missing"))?
                            .to_owned();
                        let name = item
                            .get("name")
                            .and_then(Value::as_str)
                            .filter(|v| !v.is_empty())
                            .ok_or_else(|| Error::protocol("Responses function name missing"))?
                            .to_owned();
                        let arguments = match item.get("arguments") {
                            None => String::new(),
                            Some(v) => v
                                .as_str()
                                .ok_or_else(|| {
                                    Error::protocol("invalid initial Responses arguments")
                                })?
                                .to_owned(),
                        };
                        self.tool_calls.insert(
                            index,
                            PartialToolCall {
                                id: id.clone(),
                                name: name.clone(),
                                item_id: item.get("id").and_then(Value::as_str).map(str::to_owned),
                                arguments: arguments.clone(),
                                arguments_done: false,
                            },
                        );
                        events.push(ModelEvent::ToolCallDelta {
                            index,
                            id: Some(id),
                            name: Some(name),
                            arguments,
                        });
                    }
                    Some("message" | "reasoning") => {
                        let text = item_plaintext(item)?;
                        for key in ["summary", "content"] {
                            if let Some(blocks) = item.get(key).and_then(Value::as_array) {
                                for (component, block) in blocks.iter().enumerate() {
                                    let initial = block
                                        .get("text")
                                        .and_then(Value::as_str)
                                        .unwrap_or("")
                                        .to_owned();
                                    if item["type"] == "message" {
                                        self.text_components
                                            .insert((index, component as u64), initial);
                                    } else {
                                        let delta = if key == "summary" {
                                            "response.reasoning_summary_text.delta"
                                        } else {
                                            "response.reasoning_text.delta"
                                        };
                                        self.reasoning_components.insert(
                                            (index, delta.into(), component as u64),
                                            initial,
                                        );
                                    }
                                }
                            }
                        }
                        if item["type"] == "message" {
                            self.content_text.push_str(&text);
                            self.observed_text.insert(index, text.clone());
                            if !text.is_empty() {
                                events.push(ModelEvent::TextDelta { text });
                            }
                        } else {
                            self.reasoning_text.push_str(&text);
                            self.observed_reasoning.insert(index, text.clone());
                            if !text.is_empty() {
                                events.push(ModelEvent::ReasoningDelta { text });
                            }
                        }
                    }
                    _ => return Err(Error::protocol("unsupported Responses output item type")),
                }
                self.items.insert(index, item.clone());
            }
            "response.function_call_arguments.delta" | "response.function_call_arguments.done" => {
                let index = response_index(&value)?;
                if self.closed_items.contains(&index) {
                    return Err(Error::protocol(
                        "Responses argument event after item completion",
                    ));
                }
                let call = self.tool_calls.get_mut(&index).ok_or_else(|| {
                    Error::protocol("Responses argument event references unknown call")
                })?;
                if call.arguments_done {
                    return Err(Error::protocol(
                        "Responses argument event after arguments.done",
                    ));
                }
                if let Some(id) = value.get("item_id")
                    && id.as_str() != call.item_id.as_deref()
                {
                    return Err(Error::protocol(
                        "Responses argument item identity differs from start",
                    ));
                }
                if kind.ends_with(".delta") {
                    let delta = value
                        .get("delta")
                        .and_then(Value::as_str)
                        .ok_or_else(|| Error::protocol("invalid Responses arguments delta"))?;
                    call.arguments.push_str(delta);
                    events.push(ModelEvent::ToolCallDelta {
                        index,
                        id: None,
                        name: None,
                        arguments: delta.into(),
                    });
                } else {
                    let arguments = value
                        .get("arguments")
                        .and_then(Value::as_str)
                        .ok_or_else(|| Error::protocol("Responses final arguments missing"))?;
                    reconcile_arguments(&call.arguments, arguments)?;
                    call.arguments = arguments.into();
                    call.arguments_done = true;
                }
            }
            "response.output_item.done" => {
                let index = response_index(&value)?;
                if !self.closed_items.insert(index) {
                    return Err(Error::protocol("duplicate Responses output_item.done"));
                }
                let item = value
                    .get("item")
                    .ok_or_else(|| Error::protocol("Responses final item missing"))?;
                self.reconcile_item(index, item)?;
            }
            "response.completed" => {
                let response = value
                    .get("response")
                    .ok_or_else(|| Error::protocol("Responses completion response missing"))?;
                validate_response_status(response)?;
                if let Some(output) = response.get("output") {
                    let output = output.as_array().ok_or_else(|| {
                        Error::protocol("Responses terminal output is not an array")
                    })?;
                    if output.len() != self.items.len() && !self.items.is_empty() {
                        return Err(Error::protocol(
                            "Responses terminal output item count differs from stream",
                        ));
                    }
                    for (index, item) in output.iter().enumerate() {
                        let index = u32::try_from(index)
                            .map_err(|_| Error::Limit("tool index budget".into()))?;
                        if self.items.contains_key(&index) {
                            self.reconcile_item(index, item)?;
                        } else if !self.items.is_empty() || item["type"] == "function_call" {
                            return Err(Error::protocol(
                                "Responses terminal item has no matching output-item start",
                            ));
                        }
                    }
                    self.terminal = Some(response.clone());
                }
                if let Some(usage) = response.get("usage").filter(|v| !v.is_null()) {
                    self.usage = Some(extract_usage(usage)?);
                }
                self.completed = true;
            }
            "response.failed" | "response.incomplete" | "error" => {
                return Err(crate::failure::provider_failure(&value));
            }
            "response.created"
            | "response.in_progress"
            | "response.content_part.added"
            | "response.content_part.done"
            | "response.output_text.done"
            | "response.reasoning_summary_part.added"
            | "response.reasoning_summary_part.done"
            | "response.reasoning_summary_text.done"
            | "response.reasoning_text.done" => {
                if value.get("output_index").is_some() {
                    let index = response_index(&value)?;
                    if !self.items.contains_key(&index) || self.closed_items.contains(&index) {
                        return Err(Error::protocol(
                            "Responses content lifecycle references unknown or closed item",
                        ));
                    }
                    if kind.ends_with(".done") {
                        let component = value
                            .get("content_index")
                            .or_else(|| value.get("summary_index"))
                            .and_then(Value::as_u64)
                            .unwrap_or(0);
                        if !self
                            .lifecycle_done
                            .insert((kind.to_owned(), index, component))
                        {
                            return Err(Error::protocol(
                                "duplicate Responses content completion event",
                            ));
                        }
                    }
                    if matches!(
                        kind,
                        "response.reasoning_summary_text.done" | "response.reasoning_text.done"
                    ) {
                        let component = value
                            .get("summary_index")
                            .or_else(|| value.get("content_index"))
                            .and_then(Value::as_u64)
                            .unwrap_or(0);
                        let delta = kind
                            .strip_suffix(".done")
                            .map(|prefix| format!("{prefix}.delta"))
                            .unwrap_or_default();
                        let text = value.get("text").and_then(Value::as_str).ok_or_else(|| {
                            Error::protocol("Responses done reasoning text missing")
                        })?;
                        if self
                            .reasoning_components
                            .get(&(index, delta, component))
                            .is_none_or(|old| old != text)
                        {
                            return Err(Error::protocol(
                                "Responses done reasoning text differs from accumulated text",
                            ));
                        }
                    }
                    if kind == "response.output_text.done" {
                        let text = value
                            .get("text")
                            .and_then(Value::as_str)
                            .ok_or_else(|| Error::protocol("Responses done text missing"))?;
                        let component = value
                            .get("content_index")
                            .and_then(Value::as_u64)
                            .unwrap_or(0);
                        if self
                            .text_components
                            .get(&(index, component))
                            .is_none_or(|old| old != text)
                        {
                            return Err(Error::protocol(
                                "Responses done text differs from accumulated text",
                            ));
                        }
                    }
                }
            }
            _ => return Err(Error::protocol("unsupported Responses stream event")),
        }
        Ok(events)
    }
    fn finish(self, request: &Request) -> Result<Response, Error> {
        let ordered_text = self.ordered_stream(false)?;
        let ordered_reasoning = self.ordered_stream(true)?;
        if !self.completed {
            return Err(Error::decode_reason(
                "provider SSE ended before its terminal frame",
            ));
        }
        for call in self.tool_calls.values() {
            if !call.arguments_done {
                return Err(Error::protocol(
                    "Responses stream ended before function arguments completion",
                ));
            }
        }
        if self.terminal.is_none()
            && self.items.iter().any(|(index, item)| {
                item["type"] != "function_call" && !self.closed_items.contains(index)
            })
        {
            return Err(Error::protocol(
                "Responses stream ended before output item completion",
            ));
        }
        if let Some(terminal) = self.terminal {
            let response = decode_json_response_inner(terminal, request, true)?;
            if !ordered_text.is_empty() && ordered_text != response.content {
                return Err(Error::protocol(
                    "Responses terminal text differs from streamed text",
                ));
            }
            let final_reasoning = reasoning_text(&response.provider_metadata);
            if !ordered_reasoning.is_empty() && ordered_reasoning != final_reasoning {
                return Err(Error::protocol(
                    "Responses terminal reasoning differs from streamed reasoning",
                ));
            }
            crate::validate_response_with_finish(
                &response,
                &request.tools,
                crate::EmptyCompletionFinish::Stop,
            )?;
            return Ok(response);
        }
        let mut output = Vec::new();
        let finalized_content = self
            .items
            .iter()
            .filter(|(i, item)| self.closed_items.contains(i) && item["type"] == "message")
            .map(|(_, item)| item.clone())
            .collect::<Vec<_>>();
        let finalized_reasoning = self
            .items
            .iter()
            .filter(|(i, item)| self.closed_items.contains(i) && item["type"] == "reasoning")
            .map(|(_, item)| item.clone())
            .collect::<Vec<_>>();
        let has_text = !finalized_content.is_empty();
        let has_reasoning = !finalized_reasoning.is_empty();
        for (index, item) in self.items {
            if let Some(call) = self.tool_calls.get(&index) {
                output.push(json!({"type":"function_call","call_id":call.id,"name":call.name,"arguments":call.arguments}));
            } else if self.closed_items.contains(&index) {
                output.push(item);
            }
        }
        if !has_reasoning && !self.reasoning_text.is_empty() {
            output.insert(0,json!({"type":"reasoning","summary":[{"type":"summary_text","text":self.reasoning_text}]}));
        }
        if !has_text && !self.content_text.is_empty() {
            output.push(json!({"type":"message","content":[{"type":"output_text","text":self.content_text}]}));
        }
        let mut response = decode_json_response_inner(
            json!({"status":"completed","output":output}),
            request,
            true,
        )?;
        if has_text && !ordered_text.is_empty() && response.content != ordered_text {
            return Err(Error::protocol(
                "Responses done text differs from streamed text",
            ));
        }
        if has_reasoning
            && !ordered_reasoning.is_empty()
            && reasoning_text(&response.provider_metadata) != ordered_reasoning
        {
            return Err(Error::protocol(
                "Responses done reasoning differs from streamed reasoning",
            ));
        }
        response.usage = self.usage.unwrap_or(Usage {
            samples: 1,
            ..Usage::default()
        });
        crate::validate_response_with_finish(
            &response,
            &request.tools,
            crate::EmptyCompletionFinish::Stop,
        )?;
        Ok(response)
    }
}
fn reasoning_text(metadata: &Option<microcrab_protocol::AssistantProviderMetadata>) -> String {
    match metadata {
        Some(microcrab_protocol::AssistantProviderMetadata::Neutral { parts, .. }) => parts
            .iter()
            .filter_map(|p| match p {
                microcrab_protocol::AssistantPart::Reasoning { text } => Some(text.as_str()),
                _ => None,
            })
            .collect(),
        _ => String::new(),
    }
}
fn validate_response_status(value: &Value) -> Result<(), Error> {
    // Missing status is the explicit gateway JSON compatibility policy. Present nonterminal statuses fail.
    if let Some(status) = value.get("status") {
        match status.as_str() {
            Some("completed") => {}
            Some("incomplete") => {
                return Err(Error::protocol("Responses response status is incomplete"));
            }
            Some("failed") => return Err(Error::protocol("Responses response status is failed")),
            Some("in_progress" | "queued") => {
                return Err(Error::protocol(
                    "Responses response has not reached terminal completion",
                ));
            }
            _ => {
                return Err(Error::protocol(
                    "Responses response status is unsupported or malformed",
                ));
            }
        }
    }
    Ok(())
}
fn extract_usage(value: &Value) -> Result<Usage, Error> {
    use crate::native_usage::{add, count, details, subtract};
    if !value.is_object() {
        return Err(Error::protocol("invalid Responses usage object"));
    }
    let input = details(value, "input_tokens_details")?;
    let output = details(value, "output_tokens_details")?;
    let mut usage = Usage {
        samples: 1,
        provider_details: vec![value.clone()],
        ..Usage::default()
    };
    usage.input_tokens = count(value, "input_tokens")?;
    usage.output_tokens = count(value, "output_tokens")?;
    usage.cache_read_tokens = count(input, "cached_tokens")?;
    usage.cache_write_tokens = count(input, "cache_write_tokens")?;
    usage.reasoning_tokens = count(output, "reasoning_tokens")?;
    usage.uncached_input_tokens = subtract(
        subtract(usage.input_tokens, usage.cache_read_tokens)?,
        usage.cache_write_tokens,
    )?;
    usage.non_reasoning_output_tokens = subtract(usage.output_tokens, usage.reasoning_tokens)?;
    usage.total_tokens =
        count(value, "total_tokens")?.or(add(usage.input_tokens, usage.output_tokens)?);
    Ok(usage)
}
fn decode_json_response(value: Value, request: &Request) -> Result<Response, Error> {
    decode_json_response_inner(value, request, false)
}

fn decode_json_response_inner(
    value: Value,
    request: &Request,
    defer_empty: bool,
) -> Result<Response, Error> {
    use microcrab_protocol::{AssistantPart, AssistantProviderMetadata, AssistantReplayHint};
    if value
        .get("error")
        .filter(|v| !v.is_null())
        .or_else(|| value.pointer("/response/error").filter(|v| !v.is_null()))
        .is_some()
    {
        return Err(crate::failure::provider_failure(&value));
    }
    validate_response_status(&value)?;
    if matches!(
        value.get("type").and_then(Value::as_str),
        Some("response.failed" | "response.incomplete")
    ) {
        return Err(Error::protocol(
            "Responses JSON wrapper reports failed or incomplete output",
        ));
    }
    let value = value.get("response").unwrap_or(&value);
    validate_response_status(value)?;
    let items = value
        .get("output")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::protocol("Responses output array missing"))?;
    let mut content = String::new();
    let mut tool_calls = vec![];
    let mut parts = vec![];
    let mut replay_hints = vec![];
    for item in items {
        if let Some(status) = item.get("status")
            && !matches!(status.as_str(), Some("completed"))
        {
            return Err(Error::protocol("Responses output item is not completed"));
        }
        match item.get("type").and_then(Value::as_str) {
            Some("message") => {
                if let Some(text) = item.get("content").and_then(Value::as_str) {
                    content.push_str(text);
                    parts.push(AssistantPart::Text { text: text.into() });
                } else {
                    let blocks = item
                        .get("content")
                        .and_then(Value::as_array)
                        .ok_or_else(|| Error::protocol("Responses message content missing"))?;
                    for block in blocks {
                        if block["type"] != "output_text" {
                            return Err(Error::protocol(
                                "unsupported Responses message content type",
                            ));
                        }
                        let text = block
                            .get("text")
                            .and_then(Value::as_str)
                            .ok_or_else(|| Error::protocol("invalid Responses output text"))?;
                        content.push_str(text);
                        parts.push(AssistantPart::Text { text: text.into() });
                    }
                }
            }
            Some("reasoning") => {
                let mut text = String::new();
                for key in ["summary", "content"] {
                    if let Some(blocks) = item.get(key) {
                        let blocks = blocks
                            .as_array()
                            .ok_or_else(|| Error::protocol("invalid Responses reasoning parts"))?;
                        for block in blocks {
                            let value =
                                block.get("text").and_then(Value::as_str).ok_or_else(|| {
                                    Error::protocol("invalid Responses reasoning text")
                                })?;
                            text.push_str(value);
                        }
                    }
                }
                let index = parts.len();
                parts.push(if text.is_empty() {
                    AssistantPart::RedactedReasoning
                } else {
                    AssistantPart::Reasoning { text }
                });
                let mut evidence = serde_json::Map::new();
                for key in ["id", "encrypted_content"] {
                    if let Some(v) = item.get(key).filter(|v| !v.is_null()) {
                        if !v.is_string() {
                            return Err(Error::protocol(
                                "invalid Responses reasoning replay evidence",
                            ));
                        }
                        evidence.insert(key.into(), v.clone());
                    }
                }
                if !evidence.is_empty() {
                    replay_hints.push(AssistantReplayHint {
                        part_index: index,
                        scope: "responses".into(),
                        kind: "reasoning".into(),
                        data: Value::Object(evidence),
                    });
                }
            }
            Some("function_call") => {
                let id = item
                    .get("call_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| Error::protocol("Responses call id missing"))?
                    .to_owned();
                let name = item
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| Error::protocol("Responses function name missing"))?
                    .to_owned();
                let arguments = match item.get("arguments") {
                    Some(Value::String(s)) => serde_json::from_str(s).map_err(|_| {
                        Error::Protocol(crate::ProtocolError::with_private_reason(
                            "invalid tool arguments JSON",
                            format!("invalid tool arguments JSON for call {id}"),
                        ))
                    })?,
                    Some(v) => v.clone(),
                    None => return Err(Error::protocol("Responses function arguments missing")),
                };
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
            _ => return Err(Error::protocol("unsupported Responses output item type")),
        }
    }
    let usage = value
        .get("usage")
        .filter(|v| !v.is_null())
        .map(extract_usage)
        .transpose()?
        .unwrap_or(Usage {
            samples: 1,
            ..Usage::default()
        });
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
    if defer_empty {
        crate::validate_response_shape(&response, &request.tools)?;
    } else if value.get("status").and_then(Value::as_str) == Some("completed") {
        crate::validate_response_with_finish(
            &response,
            &request.tools,
            crate::EmptyCompletionFinish::Stop,
        )?;
    } else {
        crate::validate_response(&response, &request.tools)?;
    }
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
    fn invalid_tool_arguments_preserve_safe_cause_without_publishing_call_id() {
        let error = decode_json_response(
            json!({"output":[{"type":"function_call","call_id":"private-call-id",
                "name":"echo","arguments":"private malformed JSON"}]}),
            &request(),
        )
        .unwrap_err();
        assert_eq!(
            error.public_protocol_reason(),
            Some("invalid tool arguments JSON")
        );
        assert!(error.to_string().contains("private-call-id"));
        let provider = crate::failure::response_error(error);
        assert!(matches!(&provider, Error::Provider(_)));
        assert_eq!(
            provider.public_protocol_reason(),
            Some("invalid tool arguments JSON")
        );
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
        let client = OpenAiResponse::new(OpenAiResponseConfig {
            endpoint: "http://127.0.0.1:1/responses".into(),
            model: "fixture".into(),
            api_key: None,
            timeout_ms: 1000,
            max_response_bytes: 65536,
        })
        .unwrap();
        let body = client.adapt(&request).unwrap();
        assert_eq!(body["tools"][0]["parameters"], schema);
    }

    fn push(decoder: &mut OpenAiResponsesSseDecoder, value: Value) -> Result<(), Error> {
        decoder
            .push_frame(crate::sse::SseFrame {
                event: None,
                data: value.to_string(),
            })
            .map(|_| ())
    }

    fn partial_tool_call(decoder: &mut OpenAiResponsesSseDecoder) {
        push(
            decoder,
            json!({"type":"response.output_item.added","output_index":0,
            "item":{"type":"function_call","call_id":"call","name":"echo"}}),
        )
        .expect("tool start");
        push(
            decoder,
            json!({"type":"response.function_call_arguments.delta",
            "output_index":0,"delta":"{\"text\":\"hello\"}"}),
        )
        .expect("tool arguments");
        push(decoder,json!({"type":"response.function_call_arguments.done","output_index":0,"arguments":"{\"text\":\"hello\"}"})).expect("arguments done");
    }

    #[test]
    fn json_tool_arguments_reject_malformed_strings_and_preserve_objects() {
        for arguments in ["{", "{\"text\":", "", "{\"text\":\"hello\"} trailing"] {
            let result = decode_json_response(
                json!({"output":[{
                    "type":"function_call","call_id":"call","name":"echo","arguments":arguments
                }]}),
                &request(),
            );
            assert!(
                matches!(result, Err(Error::Protocol(_))),
                "arguments {arguments:?}"
            );
        }
        for arguments in [
            json!("{\"text\":\"hello\"}"),
            json!({"text":"hello"}),
            json!("{}"),
            json!({}),
        ] {
            let response = decode_json_response(
                json!({"error":null,"output":[{
                    "type":"function_call","call_id":"call","name":"echo","arguments":arguments
                }]}),
                &request(),
            )
            .expect("valid tool arguments");
            assert!(response.tool_calls[0].arguments.is_object());
            if arguments != json!("{}") && arguments != json!({}) {
                assert_eq!(response.tool_calls[0].arguments, json!({"text":"hello"}));
            }
        }
    }

    #[test]
    fn sse_requires_completed_for_text_and_complete_tool_arguments() {
        let mut text = OpenAiResponsesSseDecoder::default();
        push(
            &mut text,
            json!({"type":"response.output_text.delta","delta":"partial"}),
        )
        .expect("text delta");
        assert!(matches!(text.finish(&request()), Err(Error::Protocol(_))));
        let mut tool = OpenAiResponsesSseDecoder::default();
        partial_tool_call(&mut tool);
        assert!(matches!(tool.finish(&request()), Err(Error::Protocol(_))));
    }

    #[test]
    fn sse_completed_preserves_text_tools_and_usage_with_null_error() {
        let mut decoder = OpenAiResponsesSseDecoder::default();
        push(
            &mut decoder,
            json!({"type":"response.output_text.delta","delta":"hello"}),
        )
        .expect("text delta");
        partial_tool_call(&mut decoder);
        push(
            &mut decoder,
            json!({"type":"response.completed","response":{
                "status":"completed","error":null,"usage":{"input_tokens":2,"output_tokens":3}
            }}),
        )
        .expect("completed response");
        let response = decoder.finish(&request()).expect("complete stream");
        assert_eq!(response.content, "hello");
        assert_eq!(response.tool_calls[0].arguments, json!({"text":"hello"}));
        assert_eq!(response.usage.total_tokens, Some(5));
    }

    #[test]
    fn sse_completed_without_usage_is_valid_but_done_alone_is_not() {
        let mut decoder = OpenAiResponsesSseDecoder::default();
        push(
            &mut decoder,
            json!({"type":"response.output_text.delta","delta":"hello"}),
        )
        .expect("text delta");
        decoder
            .push_frame(crate::sse::SseFrame {
                event: None,
                data: "[DONE]".into(),
            })
            .expect("optional framing sentinel");
        assert!(matches!(
            decoder.finish(&request()),
            Err(Error::Protocol(_))
        ));

        let mut decoder = OpenAiResponsesSseDecoder::default();
        push(
            &mut decoder,
            json!({"type":"response.output_text.delta","delta":"hello"}),
        )
        .expect("text delta");
        push(
            &mut decoder,
            json!({"type":"response.completed","response":{}}),
        )
        .expect("completion without optional metadata");
        assert_eq!(
            decoder.finish(&request()).expect("complete stream").content,
            "hello"
        );
    }

    #[test]
    fn sse_failure_and_incomplete_events_reject_partial_output() {
        for terminal in [
            json!({"type":"response.failed","response":{"status":"failed","error":null}}),
            json!({"type":"response.incomplete","response":{"status":"incomplete","error":null}}),
            json!({"type":"error","message":"provider failed"}),
            json!({"type":"response.completed","response":{"status":"incomplete"}}),
        ] {
            let mut decoder = OpenAiResponsesSseDecoder::default();
            push(
                &mut decoder,
                json!({"type":"response.output_text.delta","delta":"partial"}),
            )
            .expect("text delta");
            assert!(
                push(&mut decoder, terminal.clone()).is_err(),
                "terminal {terminal}"
            );
        }
    }
}

#[cfg(test)]
#[path = "openai_response/integrity_tests.rs"]
mod integrity_tests;
