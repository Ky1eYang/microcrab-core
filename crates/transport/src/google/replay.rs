//! Validate Google replay evidence against its neutral projections without
//! generating identities or granting tool authority to opaque provider bytes.
use crate::Error;
use microcrab_protocol::{AssistantProviderMetadata, Response, ToolCall, ToolCallProviderMetadata};
use serde_json::Value;

pub(crate) fn validate_part(part: &Value) -> Result<(), Error> {
    super::wire::canonical_fields(part)?;
    let members = [
        "text",
        "functionCall",
        "inlineData",
        "fileData",
        "functionResponse",
        "executableCode",
        "codeExecutionResult",
        "toolCall",
        "toolResponse",
    ];
    let count = members
        .iter()
        .filter(|key| part.get(**key).is_some())
        .count();
    if count > 1
        || (count == 0
            && (!part.get("thoughtSignature").is_some_and(Value::is_string)
                || part.as_object().is_some_and(|fields| {
                    fields
                        .keys()
                        .any(|key| !matches!(key.as_str(), "thought" | "thoughtSignature"))
                })))
    {
        return Err(Error::protocol(
            "Google Part requires one supported data member or a signature",
        ));
    }
    if members[2..].iter().any(|key| part.get(*key).is_some()) {
        return Err(Error::protocol("unsupported Google generated content part"));
    }
    if part.get("text").is_some_and(|text| !text.is_string())
        || part
            .get("thought")
            .is_some_and(|thought| !thought.is_boolean())
        || part
            .get("thoughtSignature")
            .is_some_and(|signature| !signature.is_string())
    {
        return Err(Error::protocol("invalid Google text, thought or signature"));
    }
    if let Some(call) = part.get("functionCall") {
        super::wire::canonical_fields(call)?;
        if !call.is_object()
            || call
                .get("name")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
            || call.get("args").is_some_and(|args| !args.is_object())
            || call
                .get("id")
                .is_some_and(|id| id.as_str().is_none_or(str::is_empty))
        {
            return Err(Error::protocol("invalid Google function call"));
        }
    }
    Ok(())
}

pub(crate) fn validate_projection(
    content: &str,
    tool_calls: &[ToolCall],
    metadata: Option<&AssistantProviderMetadata>,
) -> Result<(), Error> {
    if let Some(metadata @ AssistantProviderMetadata::Neutral { .. }) = metadata {
        metadata
            .validate_projection(content, tool_calls)
            .map_err(|reason| Error::Protocol(reason.into()))?;
        return crate::replay::validate_bindings(metadata, tool_calls, "google");
    }
    let Some(AssistantProviderMetadata::Google { parts }) = metadata else {
        return Ok(());
    };
    let mut visible = String::new();
    let mut calls = tool_calls.iter();
    for part in parts {
        validate_part(part)?;
        if part.get("thought").and_then(Value::as_bool) != Some(true)
            && let Some(text) = part.get("text").and_then(Value::as_str)
        {
            visible.push_str(text);
        }
        if let Some(function) = part.get("functionCall") {
            let call = calls
                .next()
                .ok_or_else(|| Error::protocol("Google replay call projection missing"))?;
            let signature = part.get("thoughtSignature").and_then(Value::as_str);
            let legacy_signature = call
                .provider_metadata
                .as_ref()
                .map(|metadata| match metadata {
                    ToolCallProviderMetadata::Google { thought_signature } => {
                        thought_signature.as_str()
                    }
                });
            if function.get("name").and_then(Value::as_str) != Some(call.name.as_str())
                || !function.get("args").map_or_else(
                    || {
                        call.arguments
                            .as_object()
                            .is_some_and(serde_json::Map::is_empty)
                    },
                    |args| args == &call.arguments,
                )
                || function
                    .get("id")
                    .is_some_and(|id| id.as_str() != Some(call.id.as_str()))
                || signature != legacy_signature
            {
                return Err(Error::protocol(
                    "Google replay differs from tool call projection",
                ));
            }
        }
    }
    if visible != content || calls.next().is_some() {
        return Err(Error::protocol(
            "Google replay differs from assistant projection",
        ));
    }
    Ok(())
}

pub(crate) fn validate_replay(response: &Response) -> Result<(), Error> {
    validate_projection(
        &response.content,
        &response.tool_calls,
        response.provider_metadata.as_ref(),
    )
}
