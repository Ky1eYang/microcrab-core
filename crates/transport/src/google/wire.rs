//! Gemini URL construction and native rewrite validation. Provider payloads inside
//! tool arguments/results and JSON Schema remain opaque to alias checks.
use crate::Error;
use reqwest::Url;
use serde_json::Value;

pub(crate) fn resolve_endpoint(endpoint: &str, model: &str, stream: bool) -> Result<Url, Error> {
    let mut url =
        Url::parse(endpoint.trim()).map_err(|_| Error::config("invalid provider endpoint"))?;
    let model = model.strip_prefix("models/").unwrap_or(model);
    if model.is_empty()
        || !model
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    {
        return Err(Error::config("invalid Google model identifier"));
    }
    if url
        .query_pairs()
        .any(|(key, _)| matches!(key.as_ref(), "key" | "api_key" | "apiKey" | "access_token"))
    {
        return Err(Error::config(
            "Google credentials must use the API key header",
        ));
    }
    let action = if stream {
        "streamGenerateContent"
    } else {
        "generateContent"
    };
    let path = url.path().trim_end_matches('/');
    let path = if let Some(resource) = path
        .strip_suffix(":generateContent")
        .or_else(|| path.strip_suffix(":streamGenerateContent"))
    {
        if !resource.ends_with(&format!("/models/{model}")) {
            return Err(Error::config(
                "Google endpoint model differs from selected model",
            ));
        }
        format!("{resource}:{action}")
    } else {
        if path.contains("/models/") {
            return Err(Error::config(
                "Google endpoint must be a base or generation method",
            ));
        }
        format!("{path}/models/{model}:{action}")
    };
    url.set_path(&path);
    let pairs: Vec<_> = url
        .query_pairs()
        .filter(|(key, _)| key != "alt")
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    url.set_query(None);
    if !pairs.is_empty() || stream {
        let mut query = url.query_pairs_mut();
        query.extend_pairs(pairs);
        if stream {
            query.append_pair("alt", "sse");
        }
    }
    Ok(url)
}

/// Both the Session intervention boundary and the Google transport use this check
/// before state mutation/HTTP. Aliases are rejected, never silently overlaid onto
/// the epoch-frozen authored system and tools.
pub(crate) fn validate_request_rewrite(
    authored: &str,
    replacement: &str,
    max_bytes: usize,
) -> Result<(), Error> {
    if replacement.len() > max_bytes {
        return Err(Error::Limit("provider request byte budget".into()));
    }
    let original: Value = serde_json::from_str(authored)
        .map_err(|_| Error::protocol("invalid authored Google body"))?;
    let rewritten: Value = serde_json::from_str(replacement)
        .map_err(|_| Error::protocol("Google rewrite must be valid JSON"))?;
    validate_body(&rewritten)?;
    if original.get("systemInstruction") != rewritten.get("systemInstruction")
        || original.get("tools") != rewritten.get("tools")
    {
        return Err(Error::protocol(
            "request rewrite changed frozen system or tools",
        ));
    }
    if tool_call_groups(&original) != tool_call_groups(&rewritten) {
        return Err(Error::protocol(
            "request rewrite changed authored tool calls",
        ));
    }
    // A signature belongs to its model Content and exact ordered Parts, including
    // signed text and empty/signature-only tails. Keep its conversation position.
    let signed_groups = |body: &Value| -> Vec<(usize, Value)> {
        body.get("contents")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .enumerate()
            .filter(|(_, content)| {
                content.get("role").and_then(Value::as_str) == Some("model")
                    && content
                        .get("parts")
                        .and_then(Value::as_array)
                        .is_some_and(|parts| {
                            parts
                                .iter()
                                .any(|part| part.get("thoughtSignature").is_some())
                        })
            })
            .map(|(index, content)| (index, content.clone()))
            .collect()
    };
    if signed_groups(&original) != signed_groups(&rewritten) {
        return Err(Error::protocol(
            "request rewrite changed signed Google model content",
        ));
    }
    let maximum = original
        .pointer("/generationConfig/maxOutputTokens")
        .and_then(Value::as_u64)
        .ok_or_else(|| Error::protocol("authored Google output budget missing"))?;
    if rewritten
        .pointer("/generationConfig/maxOutputTokens")
        .and_then(Value::as_u64)
        .is_none_or(|value| value == 0 || value > maximum)
    {
        return Err(Error::Limit("request rewrite output token budget".into()));
    }
    Ok(())
}

fn tool_call_groups(body: &Value) -> Vec<(&Value, Vec<&Value>)> {
    body["contents"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|content| {
            let calls: Vec<_> = content["parts"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|part| part.get("functionCall"))
                .collect();
            (!calls.is_empty()).then_some((&content["role"], calls))
        })
        .collect()
}

pub(super) fn canonical_fields(value: &Value) -> Result<(), Error> {
    if !value.is_object()
        || value
            .as_object()
            .is_some_and(|object| object.keys().any(|key| key.contains('_')))
    {
        return Err(Error::protocol(
            "Google native fields require canonical camelCase",
        ));
    }
    Ok(())
}

fn validate_body(body: &Value) -> Result<(), Error> {
    canonical_fields(body)?;
    if body.get("model").is_some() || body.get("stream").is_some() || body.get("messages").is_some()
    {
        return Err(Error::protocol(
            "Google rewrite has incompatible request shape",
        ));
    }
    let config = body
        .get("generationConfig")
        .ok_or_else(|| Error::protocol("Google generation config missing"))?;
    canonical_fields(config)?;
    if config
        .get("candidateCount")
        .is_some_and(|count| count.as_u64() != Some(1))
    {
        return Err(Error::protocol(
            "Google adapter requires one response candidate",
        ));
    }
    if let Some(thinking) = config.get("thinkingConfig") {
        canonical_fields(thinking)?;
    }
    if let Some(tool_config) = body.get("toolConfig") {
        canonical_fields(tool_config)?;
        if let Some(function_config) = tool_config.get("functionCallingConfig") {
            canonical_fields(function_config)?;
        }
    }
    let contents = body
        .get("contents")
        .and_then(Value::as_array)
        .filter(|values| !values.is_empty())
        .ok_or_else(|| Error::protocol("Google contents missing"))?;
    for content in contents {
        canonical_fields(content)?;
        if !matches!(
            content.get("role").and_then(Value::as_str),
            Some("user" | "model")
        ) {
            return Err(Error::protocol("invalid Google content role"));
        }
        let parts = content
            .get("parts")
            .and_then(Value::as_array)
            .filter(|parts| !parts.is_empty())
            .ok_or_else(|| Error::protocol("Google content parts missing"))?;
        for part in parts {
            canonical_fields(part)?;
            if content.get("role").and_then(Value::as_str) == Some("model") {
                super::replay::validate_part(part)?;
                continue;
            }
            let members = [
                "text",
                "inlineData",
                "fileData",
                "functionCall",
                "functionResponse",
            ];
            if members
                .iter()
                .filter(|key| part.get(**key).is_some())
                .count()
                != 1
            {
                return Err(Error::protocol("Google Part must contain one data member"));
            }
            if part.get("text").is_some_and(|text| !text.is_string()) {
                return Err(Error::protocol("invalid Google text part"));
            }
            for key in ["inlineData", "fileData", "functionCall", "functionResponse"] {
                if let Some(value) = part.get(key) {
                    canonical_fields(value)?;
                }
            }
            if let Some(call) = part.get("functionCall")
                && (call
                    .get("name")
                    .and_then(Value::as_str)
                    .is_none_or(str::is_empty)
                    || call.get("args").is_some_and(|args| !args.is_object()))
            {
                return Err(Error::protocol("invalid Google function call"));
            }
            if let Some(parts) = part
                .get("functionResponse")
                .and_then(|result| result.get("parts"))
            {
                let media = parts
                    .as_array()
                    .filter(|parts| !parts.is_empty())
                    .ok_or_else(|| {
                        Error::protocol("Google function response media parts missing")
                    })?;
                for item in media {
                    canonical_fields(item)?;
                    let inline = item.get("inlineData");
                    let file = item.get("fileData");
                    if inline.is_some() == file.is_some()
                        || item.as_object().is_none_or(|object| {
                            object
                                .keys()
                                .any(|key| !matches!(key.as_str(), "inlineData" | "fileData"))
                        })
                    {
                        return Err(Error::protocol(
                            "Google function response part requires one media member",
                        ));
                    }
                    let value = inline
                        .or(file)
                        .ok_or_else(|| Error::protocol("Google function response media missing"))?;
                    canonical_fields(value)?;
                    let nonempty = |key| {
                        value
                            .get(key)
                            .and_then(Value::as_str)
                            .is_some_and(|s| !s.is_empty())
                    };
                    if !nonempty("mimeType")
                        || (inline.is_some()
                            && (!nonempty("data")
                                || value
                                    .get("data")
                                    .and_then(Value::as_str)
                                    .is_none_or(|data| {
                                        use base64::Engine as _;
                                        base64::engine::general_purpose::STANDARD
                                            .decode(data)
                                            .is_err()
                                    })))
                        || (file.is_some() && !nonempty("fileUri"))
                    {
                        return Err(Error::protocol(
                            "Google function response media requires MIME and source",
                        ));
                    }
                }
            }
            if let Some(result) = part.get("functionResponse")
                && (result
                    .get("name")
                    .and_then(Value::as_str)
                    .is_none_or(str::is_empty)
                    || !result.get("response").is_some_and(Value::is_object))
            {
                return Err(Error::protocol("invalid Google function response"));
            }
        }
    }
    Ok(())
}
