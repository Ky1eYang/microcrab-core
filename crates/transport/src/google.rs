//! Google Gemini REST API wire protocol implementation.
use crate::call::ProviderCallAudit;
use crate::error::Error;
use crate::observation::emit;
use crate::sse::SseFrameReader;
use crate::{Client, Content, ModelEvent, render_ast};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::StreamExt;
use microcrab_auditor::{Auditor, HookContext, HookPayload, Moment, VariableView};
#[cfg(test)]
use microcrab_protocol::AssistantProviderMetadata;
use microcrab_protocol::{ImageSource, InputPart, Message, Request, Response, ToolCall, Usage};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub(crate) mod replay;
pub(crate) use replay::validate_replay;
mod wire;
pub(crate) use wire::resolve_endpoint;
pub(crate) use wire::validate_request_rewrite;

#[cfg(test)]
mod tests;

#[derive(Clone)]
pub struct GoogleConfig {
    pub endpoint: String,
    pub model: String,
    pub api_key: Option<String>,
    /// Maximum network inactivity; received chunks renew it. No total call limit.
    pub timeout_ms: u64,
    pub max_response_bytes: usize,
}

impl std::fmt::Debug for GoogleConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GoogleConfig")
            .field("endpoint", &"<configured>")
            .field("model", &self.model)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field("timeout_ms", &self.timeout_ms)
            .field("max_response_bytes", &self.max_response_bytes)
            .finish()
    }
}

#[derive(Clone)]
pub struct Google {
    auth: crate::AuthMode,
    config: GoogleConfig,
    http: reqwest::Client,
    observer: Option<std::sync::Arc<dyn crate::ModelObserver>>,
    recording: Option<std::sync::Arc<dyn crate::ModelRecording>>,
}

impl std::fmt::Debug for Google {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Google")
            .field("config", &self.config)
            .finish()
    }
}

impl Google {
    pub fn new(config: GoogleConfig) -> Result<Self, Error> {
        let url = resolve_endpoint(&config.endpoint, &config.model, false)?;
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
        let mut system_text = String::new();
        let mut contents: Vec<Value> = Vec::new();
        let mut calls = HashMap::new();
        let mut last_model_has_evidence = false;

        fn push_role_content(
            contents: &mut Vec<Value>,
            role: &'static str,
            parts: Vec<Value>,
            merge: bool,
        ) {
            if merge
                && let Some(last) = contents.last_mut().filter(|c| c["role"] == role)
                && let Some(existing_parts) = last["parts"].as_array_mut()
            {
                existing_parts.extend(parts);
                return;
            }
            contents.push(json!({
                "role": role,
                "parts": parts,
            }));
        }

        for msg in &request.messages {
            match msg {
                Message::System { content } => {
                    if !system_text.is_empty() {
                        system_text.push('\n');
                    }
                    system_text.push_str(content);
                }
                Message::User { parts } => {
                    let mut user_parts = Vec::new();
                    for part in parts {
                        match part {
                            InputPart::Text { text } => {
                                user_parts.push(json!({"text": text}));
                            }
                            InputPart::Image { source } => {
                                match source {
                                    ImageSource::Url { url } => {
                                        // Data URLs carry bytes; ordinary URLs retain native media
                                        // semantics through fileData instead of becoming text.
                                        if let Some((header, data)) = url
                                            .strip_prefix("data:")
                                            .and_then(|v| v.split_once(','))
                                        {
                                            let mime = header
                                                .split(';')
                                                .next()
                                                .unwrap_or("application/octet-stream");
                                            if !header.ends_with(";base64")
                                                || STANDARD.decode(data).is_err()
                                            {
                                                return Err(Error::protocol(
                                                    "Google image data URL must contain base64 bytes",
                                                ));
                                            }
                                            user_parts.push(json!({
                                                "inlineData": {
                                                    "mimeType": mime,
                                                    "data": data,
                                                }
                                            }));
                                        } else {
                                            user_parts.push(json!({"fileData": {"fileUri": url}}));
                                        }
                                    }
                                    ImageSource::Data { media_type, data } => {
                                        user_parts.push(json!({
                                            "inlineData": {
                                                "mimeType": media_type,
                                                "data": STANDARD.encode(data),
                                            }
                                        }));
                                    }
                                    ImageSource::Attachment { .. } => {
                                        return Err(Error::protocol(
                                            "attachments must be resolved before provider rendering",
                                        ));
                                    }
                                    ImageSource::Path { path } => {
                                        let (media_type, bytes) =
                                            crate::content::read_local_image(path)?;
                                        user_parts.push(json!({
                                            "inlineData": {
                                                "mimeType": media_type,
                                                "data": STANDARD.encode(bytes),
                                            }
                                        }));
                                    }
                                }
                            }
                        }
                    }
                    push_role_content(&mut contents, "user", user_parts, true);
                }
                Message::Assistant {
                    content,
                    tool_calls,
                    provider_metadata,
                } => {
                    let assistant_parts = crate::replay::native_parts(
                        content,
                        tool_calls,
                        provider_metadata,
                        "google",
                    )?;
                    let functions = assistant_parts
                        .iter()
                        .filter_map(|part| part.get("functionCall"));
                    for (call, function) in tool_calls.iter().zip(functions) {
                        if call.id.is_empty()
                            || calls
                                .insert(
                                    call.id.as_str(),
                                    (
                                        call.name.as_str(),
                                        function
                                            .get("id")
                                            .and_then(Value::as_str)
                                            .map(str::to_owned),
                                    ),
                                )
                                .is_some()
                        {
                            return Err(Error::protocol("ambiguous Google tool call identity"));
                        }
                    }
                    if !assistant_parts.is_empty() {
                        push_role_content(
                            &mut contents,
                            "model",
                            assistant_parts,
                            !last_model_has_evidence && provider_metadata.is_none(),
                        );
                        last_model_has_evidence = provider_metadata.is_some();
                    }
                }
                Message::Tool { call_id, content } => {
                    let (name, wire_id) = calls.get(call_id.as_str()).ok_or_else(|| {
                        Error::protocol("Google tool result has no matching function call")
                    })?;
                    let value: Value = serde_json::from_str(content)
                        .unwrap_or_else(|_| Value::String(content.clone()));
                    let response_obj = if value.is_object() {
                        value
                    } else {
                        json!({"output": value})
                    };
                    let mut part = json!({
                        "functionResponse": {
                            "name": name,
                            "response": response_obj,
                        }
                    });
                    if let Some(wire_id) = wire_id {
                        part["functionResponse"]["id"] = json!(wire_id);
                    }
                    push_role_content(&mut contents, "user", vec![part], true);
                }
            }
        }

        if contents.is_empty() {
            return Err(Error::protocol("Google requires conversation contents"));
        }
        let mut body = json!({
            "contents": contents,
            "generationConfig": {
                "maxOutputTokens": request.max_output_tokens,
            }
        });

        if !system_text.is_empty() {
            body["systemInstruction"] = json!({
                "parts": [{ "text": system_text }]
            });
        }

        if !request.tools.is_empty() {
            body["tools"] = json!([{
                "functionDeclarations": request.tools.iter().map(|tool| {
                    json!({
                        "name": tool.name,
                        "description": tool.description,
                        // Runtime schemas are JSON Schema, not Gemini's restricted
                        // OpenAPI Schema. Retain them without dropping constraints.
                        "parametersJsonSchema": tool.parameters,
                    })
                }).collect::<Vec<_>>()
            }]);
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
        stream: bool,
    ) -> Result<Response, Error> {
        let endpoint = resolve_endpoint(&self.config.endpoint, &self.config.model, stream)?;
        let mut builder = self
            .http
            .post(endpoint)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(
                reqwest::header::ACCEPT,
                "text/event-stream, application/json",
            )
            .body(wire_request);

        builder = self.auth.apply(
            builder,
            crate::Protocol::Google,
            self.config.api_key.as_deref(),
        );

        let response = crate::idle::wait(
            Duration::from_millis(self.config.timeout_ms),
            builder.send(),
        )
        .await?
        .map_err(|error| crate::failure::transport_error(error, "google", "HTTP request"))?;

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
        let mut sse_decoder = GoogleSseDecoder::default();
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
                    crate::failure::transport_error(error, "google", "response body"),
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
                .pointer("/candidates/0/content/parts")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter(|part| part.get("thought").and_then(Value::as_bool) == Some(true))
                .filter_map(|part| part.get("text").and_then(Value::as_str).map(str::to_owned))
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
impl Client for Google {
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
        let stream = events.is_some() || self.observer.is_some();
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(Error::Cancelled),
            result = self.complete_inner(&ast, request, &mut audit, &observation, events, stream) => {
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
struct GoogleSseDecoder {
    content_text: String,
    tool_calls: Vec<ToolCall>,
    parts: Vec<Value>,
    usage_metadata: serde_json::Map<String, Value>,
    usage_details: Vec<Value>,
    finish_reason: Option<String>,
}

impl GoogleSseDecoder {
    fn push_frame(&mut self, frame: crate::sse::SseFrame) -> Result<Vec<ModelEvent>, Error> {
        if frame.data.trim().is_empty() {
            return Ok(Vec::new());
        }
        let value: Value = serde_json::from_str(&frame.data).map_err(Error::decode)?;
        self.push_value(&value)
    }

    fn push_value(&mut self, value: &Value) -> Result<Vec<ModelEvent>, Error> {
        if !value.is_object()
            || value
                .get("candidates")
                .is_some_and(|candidates| !candidates.is_array())
        {
            return Err(Error::protocol(
                "invalid Google generation response envelope",
            ));
        }
        if value.get("error").is_some() {
            return Err(crate::failure::provider_failure(value));
        }
        if value
            .pointer("/promptFeedback/blockReason")
            .and_then(Value::as_str)
            .is_some_and(|reason| reason != "BLOCK_REASON_UNSPECIFIED")
        {
            let reason = value
                .pointer("/promptFeedback/blockReason")
                .and_then(Value::as_str)
                .unwrap_or("unspecified");
            return Err(Error::Provider(
                format!(
                    "Google Gemini blocked the prompt: {}",
                    microcrab_protocol::diagnostic::sanitize(reason, 128)
                )
                .into(),
            ));
        }
        let mut events = Vec::new();
        if let Some(candidates) = value.get("candidates").and_then(Value::as_array) {
            if candidates.len() > 1 {
                return Err(Error::protocol(
                    "Google adapter requires one response candidate",
                ));
            }
            if let Some(candidate) = candidates.first() {
                if candidate
                    .get("index")
                    .is_some_and(|index| index.as_u64() != Some(0))
                {
                    return Err(Error::protocol(
                        "unexpected Google response candidate index",
                    ));
                }
                if let Some(content) = candidate.get("content")
                    && (!content.is_object()
                        || content
                            .get("role")
                            .is_some_and(|role| role.as_str() != Some("model"))
                        || !content.get("parts").is_some_and(Value::is_array))
                {
                    return Err(Error::protocol("invalid Google model content"));
                }
                if let Some(parts) = candidate
                    .pointer("/content/parts")
                    .and_then(Value::as_array)
                {
                    for part in parts {
                        replay::validate_part(part)?;
                        if self.finish_reason.is_some()
                            && (!part.get("thoughtSignature").is_some_and(Value::is_string)
                                || part.get("functionCall").is_some()
                                || part
                                    .get("text")
                                    .and_then(Value::as_str)
                                    .is_some_and(|text| !text.is_empty()))
                        {
                            return Err(Error::protocol(
                                "Google content received after terminal candidate",
                            ));
                        }
                        self.parts.push(part.clone());
                        if let Some(text) = part.get("text").and_then(Value::as_str) {
                            if part.get("thought").and_then(Value::as_bool) == Some(true) {
                                events.push(ModelEvent::ReasoningDelta {
                                    text: text.to_owned(),
                                });
                            } else {
                                self.content_text.push_str(text);
                                events.push(ModelEvent::TextDelta {
                                    text: text.to_owned(),
                                });
                            }
                        }
                        if let Some(fc) = part.get("functionCall") {
                            let name = fc
                                .get("name")
                                .and_then(Value::as_str)
                                .filter(|name| !name.is_empty())
                                .ok_or_else(|| {
                                    Error::protocol("Google function call name missing")
                                })?
                                .to_owned();
                            let args = fc.get("args").cloned().unwrap_or_else(|| json!({}));
                            if !args.is_object() {
                                return Err(Error::protocol(
                                    "Google function arguments must be an object",
                                ));
                            }
                            let call_id = match fc.get("id") {
                                None => uuid::Uuid::new_v4().to_string(),
                                Some(value) => value
                                    .as_str()
                                    .filter(|id| !id.is_empty())
                                    .ok_or_else(|| {
                                        Error::protocol("invalid Google function call ID")
                                    })?
                                    .to_owned(),
                            };
                            let index = u32::try_from(self.tool_calls.len())
                                .map_err(|_| Error::Limit("tool index budget".into()))?;
                            let provider_metadata = None;
                            self.tool_calls.push(ToolCall {
                                id: call_id.clone(),
                                name: name.clone(),
                                arguments: args.clone(),
                                provider_metadata,
                            });
                            events.push(ModelEvent::ToolCallDelta {
                                index,
                                id: Some(call_id),
                                name: Some(name),
                                arguments: args.to_string(),
                            });
                        }
                    }
                }
                if candidate
                    .get("finishReason")
                    .is_some_and(|reason| !reason.is_string())
                {
                    return Err(Error::protocol("invalid Google finish reason"));
                }
                if let Some(reason) = candidate
                    .get("finishReason")
                    .and_then(Value::as_str)
                    .filter(|reason| !reason.is_empty() && *reason != "FINISH_REASON_UNSPECIFIED")
                {
                    if !matches!(reason, "STOP" | "MAX_TOKENS") {
                        return Err(Error::Provider(
                            format!(
                                "Google Gemini finish reason: {}",
                                microcrab_protocol::diagnostic::sanitize(reason, 128)
                            )
                            .into(),
                        ));
                    }
                    if self
                        .finish_reason
                        .as_deref()
                        .is_some_and(|previous| previous != reason)
                    {
                        return Err(Error::protocol("conflicting Google finish reasons"));
                    }
                    self.finish_reason = Some(reason.to_owned());
                }
            }
        }
        if let Some(meta) = value.get("usageMetadata") {
            extract_usage(meta)?;
            if let Some(fields) = meta.as_object() {
                self.usage_metadata.extend(fields.clone());
            }
            self.usage_details.push(meta.clone());
        }
        Ok(events)
    }

    fn finish(self, request: &Request) -> Result<Response, Error> {
        if self.finish_reason.is_none() {
            return Err(Error::decode_reason(
                "provider SSE ended before its terminal frame",
            ));
        }
        if !self.tool_calls.is_empty() && self.finish_reason.as_deref() == Some("MAX_TOKENS") {
            return Err(Error::protocol(
                "Google function calls stopped at the output limit",
            ));
        }
        let mut usage = extract_usage(&Value::Object(self.usage_metadata))?;
        usage.provider_details = self.usage_details;
        if self.finish_reason.as_deref() == Some("MAX_TOKENS") {
            usage
                .provider_details
                .push(json!({"finishReason":"MAX_TOKENS"}));
        }
        let provider_metadata = Some(crate::replay::from_google(
            self.parts,
            &self.content_text,
            &self.tool_calls,
        )?);
        let response = Response {
            content: self.content_text,
            tool_calls: self.tool_calls,
            usage,
            provider_metadata,
        };
        crate::validate_response(&response, &request.tools)?;
        Ok(response)
    }
}

fn extract_usage(value: &Value) -> Result<Usage, Error> {
    use crate::native_usage::{add, count, subtract};
    let mut usage = Usage {
        samples: 1,
        ..Usage::default()
    };
    if value.is_null() {
        return Ok(usage);
    }
    if !value.is_object() {
        return Err(Error::protocol("invalid Google usageMetadata object"));
    }
    usage.provider_details.push(value.clone());
    usage.input_tokens = count(value, "promptTokenCount")?;
    usage.non_reasoning_output_tokens = count(value, "candidatesTokenCount")?;
    usage.reasoning_tokens = count(value, "thoughtsTokenCount")?;
    // Google omits thoughts on non-thinking output. Explicit null remains unknown.
    let thoughts = if value.get("thoughtsTokenCount").is_none() {
        Some(0)
    } else {
        usage.reasoning_tokens
    };
    usage.output_tokens = add(usage.non_reasoning_output_tokens, thoughts)?;
    usage.cache_read_tokens = if value.get("cachedContentTokenCount").is_none() {
        usage.input_tokens.map(|_| 0)
    } else {
        count(value, "cachedContentTokenCount")?
    };
    usage.uncached_input_tokens = subtract(usage.input_tokens, usage.cache_read_tokens)?;
    usage.total_tokens =
        count(value, "totalTokenCount")?.or(add(usage.input_tokens, usage.output_tokens)?);
    Ok(usage)
}

fn decode_json_response(value: Value, request: &Request) -> Result<Response, Error> {
    let mut decoder = GoogleSseDecoder::default();
    decoder.push_value(&value)?;
    decoder.finish(request)
}
