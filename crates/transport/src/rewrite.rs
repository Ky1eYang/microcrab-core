//! Validate explicit body replacements before state writes or HTTP effects.
use crate::Error;
use serde_json::Value;

/// Plugin-authored bodies have an independent cap; increasing conversation
/// capacity must not increase the plugin replacement allocation surface.
pub const MAX_REQUEST_REWRITE_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone, Copy)]
enum Shape {
    Chat,
    Responses,
    Anthropic,
    SharedMessages,
}

/// Replacements preserve the authored protocol, frozen prompt/tool values, stream
/// mode and output ceiling. Selection uses authored structure, never model names
/// or replacement-controlled fields; endpoint and authentication stay call-owned.
pub fn validate_request_rewrite(
    authored: &str,
    replacement: &str,
    max_bytes: usize,
) -> Result<(), Error> {
    let max_bytes = max_bytes.min(MAX_REQUEST_REWRITE_BYTES);
    let original: Value = serde_json::from_str(authored)
        .map_err(|_| Error::protocol("invalid authored provider body"))?;
    if original.get("contents").is_some() {
        return crate::google::validate_request_rewrite(authored, replacement, max_bytes);
    }
    if replacement.len() > max_bytes {
        return Err(Error::Limit("provider request byte budget".into()));
    }
    let rewritten: Value = serde_json::from_str(replacement)
        .map_err(|_| Error::protocol("request rewrite must be valid JSON"))?;
    let shape = authored_shape(&original)?;
    validate_shape(&original, shape)?;
    validate_shape(&rewritten, shape)?;
    // Compare decoded JSON values: whitespace/order outside strings are immaterial,
    // while frozen strings, block ordering and every tool-schema member are exact.
    if frozen_messages(&original) != frozen_messages(&rewritten)
        || ["system", "instructions", "tools"]
            .iter()
            .any(|key| original.get(*key) != rewritten.get(*key))
    {
        return Err(Error::protocol(
            "request rewrite changed frozen system, instructions or tools",
        ));
    }
    if original.get("stream") != rewritten.get("stream") {
        return Err(Error::protocol("request rewrite changed stream mode"));
    }
    let budget = match shape {
        Shape::Responses => "max_output_tokens",
        Shape::Chat if original.get("max_completion_tokens").is_some() => "max_completion_tokens",
        _ => "max_tokens",
    };
    let maximum = original
        .get(budget)
        .and_then(Value::as_u64)
        .filter(|count| *count > 0)
        .ok_or_else(|| Error::protocol("authored output budget must be positive"))?;
    if rewritten
        .get(budget)
        .and_then(Value::as_u64)
        .is_none_or(|count| count == 0 || count > maximum)
    {
        return Err(Error::Limit("request rewrite output token budget".into()));
    }
    Ok(())
}

fn authored_shape(body: &Value) -> Result<Shape, Error> {
    if body.get("input").is_some() {
        return Ok(Shape::Responses);
    }
    let chat = body.get("stream_options").is_some()
        || body.get("max_completion_tokens").is_some()
        || body
            .get("messages")
            .and_then(Value::as_array)
            .is_some_and(|messages| {
                messages.iter().any(|message| {
                    matches!(
                        message.get("role").and_then(Value::as_str),
                        Some("system" | "developer" | "tool")
                    ) || message.get("tool_calls").is_some()
                        || message
                            .get("content")
                            .and_then(Value::as_array)
                            .is_some_and(|blocks| {
                                blocks.iter().any(|block| {
                                    block.get("type").and_then(Value::as_str) == Some("image_url")
                                })
                            })
                })
            })
        || body
            .get("tools")
            .and_then(Value::as_array)
            .is_some_and(|tools| {
                tools
                    .iter()
                    .any(|tool| tool.get("type").and_then(Value::as_str) == Some("function"))
            });
    let anthropic = body.get("system").is_some()
        || body
            .get("tools")
            .and_then(Value::as_array)
            .is_some_and(|tools| tools.iter().any(|tool| tool.get("input_schema").is_some()))
        || body
            .get("messages")
            .and_then(Value::as_array)
            .is_some_and(|messages| {
                messages.iter().any(|message| {
                    message
                        .get("content")
                        .and_then(Value::as_array)
                        .is_some_and(|blocks| {
                            blocks.iter().any(|block| {
                                matches!(
                                    block.get("type").and_then(Value::as_str),
                                    Some(
                                        "image"
                                            | "tool_use"
                                            | "tool_result"
                                            | "thinking"
                                            | "redacted_thinking"
                                    )
                                )
                            })
                        })
                })
            });
    match (chat, anthropic) {
        (true, true) => Err(Error::protocol(
            "authored request mixes Chat and Anthropic fields",
        )),
        (true, false) => Ok(Shape::Chat),
        (false, true) => Ok(Shape::Anthropic),
        // Plain role/text messages overlap. Require both validators rather than
        // inventing protocol authority from a model name or an absent field.
        (false, false) => Ok(Shape::SharedMessages),
    }
}

fn frozen_messages(body: &Value) -> Vec<&Value> {
    ["messages", "input"]
        .into_iter()
        .filter_map(|key| body.get(key).and_then(Value::as_array))
        .flatten()
        .filter(|message| {
            matches!(
                message.get("role").and_then(Value::as_str),
                Some("system" | "developer")
            )
        })
        .collect()
}

fn nonempty(value: Option<&Value>) -> bool {
    value
        .and_then(Value::as_str)
        .is_some_and(|text| !text.trim().is_empty())
}

fn array(value: Option<&Value>, check: impl Fn(&Value) -> bool) -> bool {
    value
        .and_then(Value::as_array)
        .is_some_and(|values| !values.is_empty() && values.iter().all(check))
}

fn validate_shape(body: &Value, shape: Shape) -> Result<(), Error> {
    if !body.is_object()
        || !nonempty(body.get("model"))
        || body.get("stream").and_then(Value::as_bool).is_none()
        || body.get("contents").is_some()
        || body.get("tools").is_some_and(|tools| !tools.is_array())
    {
        return Err(Error::protocol(
            "request rewrite requires model, boolean stream and native tools array",
        ));
    }
    let valid = match shape {
        Shape::Chat => chat_body(body),
        Shape::Responses => responses_body(body),
        Shape::Anthropic => anthropic_body(body),
        Shape::SharedMessages => chat_body(body) && anthropic_body(body),
    };
    if !valid {
        return Err(Error::protocol(match shape {
            Shape::Chat => "request rewrite has invalid Chat Completions shape",
            Shape::Responses => "request rewrite has invalid Responses shape",
            Shape::Anthropic => "request rewrite has invalid Anthropic Messages shape",
            Shape::SharedMessages => "request rewrite has invalid shared message shape",
        }));
    }
    Ok(())
}

fn chat_body(body: &Value) -> bool {
    body.get("input").is_none()
        && body.get("instructions").is_none()
        && body.get("system").is_none()
        && body.get("max_output_tokens").is_none()
        && !(body.get("max_tokens").is_some() && body.get("max_completion_tokens").is_some())
        && array(body.get("messages"), |message| {
            if !message.is_object() {
                return false;
            }
            let content = message.get("content");
            match message.get("role").and_then(Value::as_str) {
                Some("system" | "developer" | "user") => chat_content(content),
                Some("assistant") => {
                    let calls = message.get("tool_calls");
                    (content.is_none_or(Value::is_null) || chat_content(content))
                        && calls.is_none_or(|calls| {
                            array(Some(calls), |call| {
                                nonempty(call.get("id"))
                                    && call.get("type").and_then(Value::as_str) == Some("function")
                                    && nonempty(call.pointer("/function/name"))
                                    && call
                                        .pointer("/function/arguments")
                                        .is_some_and(Value::is_string)
                            })
                        })
                        && (content.is_some_and(|content| !content.is_null()) || calls.is_some())
                }
                Some("tool") => {
                    nonempty(message.get("tool_call_id")) && content.is_some_and(Value::is_string)
                }
                _ => false,
            }
        })
        && body.get("tools").is_none_or(|tools| {
            tools.as_array().is_some_and(|tools| {
                tools.iter().all(|tool| {
                    tool.get("type").and_then(Value::as_str) == Some("function")
                        && nonempty(tool.pointer("/function/name"))
                        && tool
                            .pointer("/function/parameters")
                            .is_some_and(Value::is_object)
                })
            })
        })
}

fn chat_content(content: Option<&Value>) -> bool {
    content.is_some_and(Value::is_string)
        || array(content, |block| {
            match block.get("type").and_then(Value::as_str) {
                Some("text") => block.get("text").is_some_and(Value::is_string),
                Some("image_url") => nonempty(block.pointer("/image_url/url")),
                _ => false,
            }
        })
}

fn responses_body(body: &Value) -> bool {
    body.get("messages").is_none()
        && body.get("system").is_none()
        && body.get("max_tokens").is_none()
        && body.get("max_completion_tokens").is_none()
        && body.get("instructions").is_none_or(Value::is_string)
        && (body.get("input").is_some_and(Value::is_string)
            || array(body.get("input"), |item| {
                if !item.is_object() {
                    return false;
                }
                match item.get("type").and_then(Value::as_str) {
                    None | Some("message") => {
                        matches!(
                            item.get("role").and_then(Value::as_str),
                            Some("system" | "developer" | "user" | "assistant")
                        ) && (item.get("content").is_some_and(Value::is_string)
                            || array(item.get("content"), |block| {
                                match block.get("type").and_then(Value::as_str) {
                                    Some("input_text" | "output_text") => {
                                        block.get("text").is_some_and(Value::is_string)
                                    }
                                    Some("input_image") => {
                                        nonempty(block.get("image_url"))
                                            || nonempty(block.get("file_id"))
                                    }
                                    Some("refusal") => {
                                        block.get("refusal").is_some_and(Value::is_string)
                                    }
                                    _ => false,
                                }
                            }))
                    }
                    Some("function_call") => {
                        nonempty(item.get("call_id"))
                            && nonempty(item.get("name"))
                            && item.get("arguments").is_some_and(Value::is_string)
                    }
                    Some("function_call_output") => {
                        nonempty(item.get("call_id"))
                            && (item.get("output").is_some_and(Value::is_string)
                                || array(item.get("output"), |part| {
                                    match part.get("type").and_then(Value::as_str) {
                                        Some("input_text") => {
                                            part.get("text").is_some_and(Value::is_string)
                                        }
                                        Some("input_image") => {
                                            nonempty(part.get("image_url"))
                                                || nonempty(part.get("file_id"))
                                        }
                                        _ => false,
                                    }
                                }))
                    }
                    Some("reasoning") => {
                        item.get("summary")
                            .and_then(Value::as_array)
                            .is_some_and(|parts| {
                                parts.iter().all(|part| {
                                    part.get("type").and_then(Value::as_str) == Some("summary_text")
                                        && part.get("text").is_some_and(Value::is_string)
                                })
                            })
                            && item.get("encrypted_content").is_none_or(Value::is_string)
                    }
                    _ => false,
                }
            }))
        && body.get("tools").is_none_or(|tools| {
            tools.as_array().is_some_and(|tools| {
                tools.iter().all(|tool| {
                    tool.get("type").and_then(Value::as_str) == Some("function")
                        && nonempty(tool.get("name"))
                        && tool.get("parameters").is_some_and(Value::is_object)
                        && tool.get("strict").is_none_or(Value::is_boolean)
                })
            })
        })
}

fn anthropic_body(body: &Value) -> bool {
    body.get("input").is_none()
        && body.get("instructions").is_none()
        && body.get("max_output_tokens").is_none()
        && body.get("max_completion_tokens").is_none()
        && body.get("stream_options").is_none()
        && body.get("system").is_none_or(|system| {
            system.is_string()
                || array(Some(system), |block| {
                    block.get("type").and_then(Value::as_str) == Some("text")
                        && block.get("text").is_some_and(Value::is_string)
                })
        })
        && array(body.get("messages"), |message| {
            let role = message.get("role").and_then(Value::as_str);
            message.is_object()
                && matches!(role, Some("user" | "assistant"))
                && (message.get("content").is_some_and(Value::is_string)
                    || array(message.get("content"), |block| {
                        match block.get("type").and_then(Value::as_str) {
                            Some("text") => block.get("text").is_some_and(Value::is_string),
                            Some("image") if role == Some("user") => anthropic_image(block),
                            Some("tool_use") if role == Some("assistant") => {
                                nonempty(block.get("id"))
                                    && nonempty(block.get("name"))
                                    && block.get("input").is_some_and(Value::is_object)
                            }
                            Some("tool_result") if role == Some("user") => {
                                nonempty(block.get("tool_use_id"))
                                    && (block.get("content").is_some_and(Value::is_string)
                                        || array(block.get("content"), |part| {
                                            match part.get("type").and_then(Value::as_str) {
                                                Some("text") => {
                                                    part.get("text").is_some_and(Value::is_string)
                                                }
                                                Some("image") => anthropic_image(part),
                                                _ => false,
                                            }
                                        }))
                            }
                            Some("thinking") if role == Some("assistant") => {
                                block.get("thinking").is_some_and(Value::is_string)
                                    && nonempty(block.get("signature"))
                            }
                            Some("redacted_thinking") if role == Some("assistant") => {
                                nonempty(block.get("data"))
                            }
                            _ => false,
                        }
                    }))
        })
        && body.get("tools").is_none_or(|tools| {
            tools.as_array().is_some_and(|tools| {
                tools.iter().all(|tool| {
                    nonempty(tool.get("name"))
                        && tool.get("input_schema").is_some_and(Value::is_object)
                        && tool.get("type").is_none()
                })
            })
        })
}

fn anthropic_image(block: &Value) -> bool {
    match block.pointer("/source/type").and_then(Value::as_str) {
        Some("url") => nonempty(block.pointer("/source/url")),
        Some("base64") => {
            nonempty(block.pointer("/source/media_type")) && nonempty(block.pointer("/source/data"))
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests;
