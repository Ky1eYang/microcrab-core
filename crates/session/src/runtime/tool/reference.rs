//! Multi-action builtin tool over the unified `Reference` trait:
//! `expand`, `add`, `update`, `remove`, `refresh` routed by `id` / `kind` / capabilities.
use crate::Error;
use crate::admission::image::{MAX_IMAGE_BYTES, MAX_IMAGES_PER_TOOL_OUTPUT, image_type, validate_image};
use crate::runtime::execution::MediaAttachment;
use crate::runtime::tool::{FinalOutput, Tool, ToolContext, ToolOutput};
use crate::state::reference::{
    ReferencePatch, ReferencePayload, ReferenceWrite, canonical_mime, detect_mime_from_bytes,
    inspect_binary_for_storage,
};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use microcrab_protocol::ToolDefinition;
use serde_json::{Value, json};
use std::collections::HashSet;

pub const NAME: &str = "reference";
pub const MAX_BATCH_IDS: usize = 128;

#[derive(Debug, Default, Clone, Copy)]
pub struct Reference;

impl Reference {
    pub fn definition() -> ToolDefinition {
        ToolDefinition {
            name: NAME.into(),
            description: "Inspect or manage session-visible references across installed kinds (`references`, `skills`, `media`, etc.) according to each kind's declared capabilities. Actions: expand (directory children, file text/images/binary metadata, or Markdown sections by 1..128 IDs via `ids` or `id`), add (write text/base64 or locator hint to a writable kind or the default writable reference), update (edit an owned writable entry by ID), remove (unmount/remove 1..128 writable IDs via `ids` or `id`), refresh (resync refreshable kinds)."
                .into(),
            parameters: json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["action"],
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["expand", "add", "update", "remove", "refresh"],
                        "description": "Operation to perform."
                    },
                    "kind": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Optional target reference kind for add (defaults to the Session's default writable kind)."
                    },
                    "id": {
                        "oneOf": [
                            { "type": "string", "minLength": 1 },
                            {
                                "type": "array",
                                "minItems": 1,
                                "maxItems": MAX_BATCH_IDS,
                                "uniqueItems": true,
                                "items": { "type": "string", "minLength": 1 }
                            }
                        ],
                        "description": "Reference ID or list of 1..128 unique IDs (for expand and remove; scalar ID for update). Mutually exclusive with `ids`."
                    },
                    "ids": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": MAX_BATCH_IDS,
                        "uniqueItems": true,
                        "items": { "type": "string", "minLength": 1 },
                        "description": "Batch selector of 1..128 unique reference IDs for expand or remove. Mutually exclusive with `id`."
                    },
                    "expected_revision": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Optional optimistic lock revision for update."
                    },
                    "title": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 256,
                        "description": "Human-readable title for add or update."
                    },
                    "media_type": {
                        "type": "string",
                        "minLength": 3,
                        "maxLength": 127,
                        "description": "Canonical lowercase MIME type without parameters (for add or update)."
                    },
                    "content": {
                        "type": "string",
                        "description": "Payload content string (UTF-8 or base64) for add or update."
                    },
                    "encoding": {
                        "type": "string",
                        "enum": ["utf8", "base64"],
                        "description": "Encoding of `content` (defaults to utf8)."
                    },
                    "locator": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Optional external locator hint interpreted by writable implementations that accept external pointers."
                    }
                }
            }),
        }
    }
}

fn invalid_argument(reason: impl AsRef<str>) -> Error {
    use crate::observation::diagnostics::{
        DiagnosticCode, DiagnosticLayer, DiagnosticPhase, EffectKnowledge, SafeDiagnosticReason,
        ToolDiagnostic,
    };
    let mut diagnostic = ToolDiagnostic::new(
        DiagnosticCode::InvalidArgument,
        DiagnosticLayer::Runtime,
        DiagnosticPhase::Validation,
    );
    diagnostic.reason = Some(SafeDiagnosticReason::new(reason.as_ref()));
    diagnostic.effects = EffectKnowledge::NotStarted;
    Error::Diagnostic(diagnostic)
}

pub(crate) fn validate(arguments: &Value) -> Result<(), Error> {
    validate_inner(arguments).map_err(|error| invalid_argument(error.to_string()))
}

fn validate_inner(arguments: &Value) -> Result<(), Error> {
    let object = arguments
        .as_object()
        .ok_or_else(|| Error::tool("reference arguments must be a JSON object"))?;
    let action = object
        .get("action")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::tool("reference action must be a string"))?;
    for key in object.keys() {
        if !matches!(
            key.as_str(),
            "action"
                | "kind"
                | "id"
                | "ids"
                | "expected_revision"
                | "title"
                | "media_type"
                | "content"
                | "encoding"
                | "locator"
        ) {
            return Err(Error::tool_dynamic(format!(
                "unsupported reference parameter `{key}`"
            )));
        }
    }
    match action {
        "expand" => {
            select_id_list(object, "expand")?;
            for key in [
                "kind",
                "title",
                "media_type",
                "content",
                "encoding",
                "locator",
                "expected_revision",
            ] {
                if object.contains_key(key) {
                    return Err(Error::tool_dynamic(format!(
                        "reference expand does not accept `{key}`"
                    )));
                }
            }
        }
        "remove" => {
            select_id_list(object, "remove")?;
            for key in [
                "kind",
                "title",
                "media_type",
                "content",
                "encoding",
                "locator",
                "expected_revision",
            ] {
                if object.contains_key(key) {
                    return Err(Error::tool_dynamic(format!(
                        "reference remove does not accept `{key}`"
                    )));
                }
            }
        }
        "add" => {
            if object.contains_key("id")
                || object.contains_key("ids")
                || object.contains_key("expected_revision")
            {
                return Err(Error::tool(
                    "reference add does not accept id, ids, or expected_revision",
                ));
            }
            if let Some(kind) = object.get("kind").and_then(Value::as_str)
                && (kind.is_empty() || kind.contains('\0'))
            {
                return Err(Error::tool("kind must be nonempty"));
            } else if object.contains_key("kind") && !object["kind"].is_string() {
                return Err(Error::tool("kind must be a string"));
            }
            if let Some(t) = object.get("title").and_then(Value::as_str) {
                validate_title(t)?;
            } else if object.contains_key("title") && !object["title"].is_string() {
                return Err(Error::tool("title must be a string"));
            }
            let has_content = object.contains_key("content");
            let has_locator = object.contains_key("locator");
            if !has_content && !has_locator {
                return Err(Error::tool(
                    "reference add requires `content` or `locator`",
                ));
            }
            if has_content {
                let _ = parse_payload_fields(object)?;
            } else if object.contains_key("encoding") {
                return Err(Error::tool("encoding requires content"));
            }
            if let Some(locator) = object.get("locator") {
                let text = locator
                    .as_str()
                    .ok_or_else(|| Error::tool("locator must be a string"))?;
                if text.trim().is_empty() || text.contains('\0') {
                    return Err(Error::tool("locator must be nonempty"));
                }
            }
            if let Some(mime) = object.get("media_type").and_then(Value::as_str) {
                canonical_mime(mime)?;
            } else if object.contains_key("media_type") && !object["media_type"].is_string() {
                return Err(Error::tool("media_type must be a string"));
            }
        }
        "update" => {
            if object.contains_key("kind") || object.contains_key("ids") {
                return Err(Error::tool(
                    "reference update does not accept `kind` or `ids`",
                ));
            }
            let id = object
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::tool("reference update requires scalar string id"))?;
            if id.is_empty() || id.contains('\0') {
                return Err(Error::tool("reference id must be nonempty"));
            }
            let has_title = if let Some(v) = object.get("title") {
                let t = v
                    .as_str()
                    .ok_or_else(|| Error::tool("title must be a string"))?;
                validate_title(t)?;
                true
            } else {
                false
            };
            let media_hint = if let Some(v) = object.get("media_type") {
                let mime = v
                    .as_str()
                    .ok_or_else(|| Error::tool("media_type must be a string"))?;
                canonical_mime(mime)?;
                Some(mime)
            } else {
                None
            };
            let has_content = if object.contains_key("content") {
                let _ = parse_payload_fields(object)?;
                true
            } else {
                if object.contains_key("encoding") {
                    return Err(Error::tool("encoding requires content"));
                }
                false
            };
            let has_locator = if let Some(v) = object.get("locator") {
                let text = v
                    .as_str()
                    .ok_or_else(|| Error::tool("locator must be a string"))?;
                if text.trim().is_empty() || text.contains('\0') {
                    return Err(Error::tool("locator must be nonempty"));
                }
                true
            } else {
                false
            };
            if !has_title && media_hint.is_none() && !has_content && !has_locator {
                return Err(Error::tool(
                    "reference update requires at least one of title, media_type, content, or locator",
                ));
            }
        }
        "refresh" => {
            for key in [
                "kind",
                "id",
                "ids",
                "title",
                "media_type",
                "content",
                "encoding",
                "locator",
                "expected_revision",
            ] {
                if object.contains_key(key) {
                    return Err(Error::tool_dynamic(format!(
                        "reference refresh does not accept `{key}`"
                    )));
                }
            }
        }
        other => {
            return Err(Error::tool_dynamic(format!(
                "unsupported reference action `{other}`"
            )));
        }
    }
    Ok(())
}

fn validate_title(title: &str) -> Result<String, Error> {
    let trimmed = title.trim();
    if trimmed.is_empty() || trimmed.chars().count() > 256 || trimmed.contains('\0') {
        return Err(Error::tool("title must be 1..=256 non-NUL characters"));
    }
    Ok(trimmed.to_owned())
}

fn select_id_list(
    object: &serde_json::Map<String, Value>,
    action: &str,
) -> Result<Vec<String>, Error> {
    match (object.get("id"), object.get("ids")) {
        (Some(_), Some(_)) => Err(Error::tool_dynamic(format!(
            "reference {action} accepts either `id` or `ids`, not both"
        ))),
        (Some(id), None) => parse_id_list(id),
        (None, Some(ids)) => {
            if !ids.is_array() {
                return Err(Error::tool("reference ids must be an array of strings"));
            }
            parse_id_list(ids)
        }
        (None, None) => Err(Error::tool_dynamic(format!(
            "reference {action} requires `id` or `ids`"
        ))),
    }
}

fn parse_id_list(value: &Value) -> Result<Vec<String>, Error> {
    let ids = match value {
        Value::String(id) => vec![id.clone()],
        Value::Array(items) => {
            if items.is_empty() || items.len() > MAX_BATCH_IDS {
                return Err(Error::tool(
                    "reference id array must contain 1..=128 entries",
                ));
            }
            items
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| Error::tool("reference id entries must be strings"))
                })
                .collect::<Result<Vec<_>, _>>()?
        }
        _ => return Err(Error::tool("reference id must be a string or array of strings")),
    };
    let mut seen = HashSet::new();
    for id in &ids {
        if id.is_empty() || id.contains('\0') {
            return Err(Error::tool("reference id must be nonempty"));
        }
        if !seen.insert(id.clone()) {
            return Err(Error::tool("reference id entries must be unique"));
        }
    }
    Ok(ids)
}

fn parse_payload_fields(
    obj: &serde_json::Map<String, Value>,
) -> Result<(String, ReferencePayload), Error> {
    let content = obj
        .get("content")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::tool("reference content must be a string"))?;
    let encoding = obj
        .get("encoding")
        .and_then(Value::as_str)
        .unwrap_or("utf8");
    let data = match encoding {
        "utf8" => content.as_bytes().to_vec(),
        "base64" => STANDARD
            .decode(content.trim())
            .map_err(|_| Error::tool("invalid base64 content"))?,
        other => {
            return Err(Error::tool_dynamic(format!(
                "unsupported encoding `{other}`"
            )));
        }
    };
    let hint = obj.get("media_type").and_then(Value::as_str);
    let mime = match hint {
        Some(raw) => inspect_binary_for_storage(raw, &data)?.0,
        None => detect_mime_from_bytes(None, &data),
    };
    let (canonical, width, height) = inspect_binary_for_storage(&mime, &data)?;
    let payload = if encoding == "utf8" && image_type(&canonical).is_none() {
        ReferencePayload::Text(content.to_owned())
    } else if canonical.starts_with("text/") || canonical == "application/json" {
        if let Ok(text) = String::from_utf8(data.clone()) {
            ReferencePayload::Text(text)
        } else {
            ReferencePayload::Binary {
                data,
                width,
                height,
            }
        }
    } else {
        ReferencePayload::Binary {
            data,
            width,
            height,
        }
    };
    Ok((canonical, payload))
}

#[async_trait]
impl Tool for Reference {
    fn definition(&self) -> ToolDefinition {
        Self::definition()
    }

    async fn execute(&self, arguments: Value, context: ToolContext) -> Result<ToolOutput, Error> {
        validate(&arguments)?;
        let obj = arguments
            .as_object()
            .expect("validate checked object arguments");
        let action = obj
            .get("action")
            .and_then(Value::as_str)
            .expect("validate checked action");

        match action {
            "expand" => execute_expand(obj, &context).await,
            "add" => execute_add(obj, &context).await,
            "update" => execute_update(obj, &context).await,
            "remove" => execute_remove(obj, &context).await,
            "refresh" => execute_refresh(&context).await,
            _ => unreachable!("validate restricted action"),
        }
    }
}

async fn execute_expand(
    obj: &serde_json::Map<String, Value>,
    context: &ToolContext,
) -> Result<ToolOutput, Error> {
    let ids = select_id_list(obj, "expand")?;
    let limits = context.state.snapshot().limits;
    let max_media_bytes = limits.max_tool_image_bytes.min(MAX_IMAGE_BYTES);
    let mut used_media_bytes: usize = 0;
    let mut attachments: Vec<MediaAttachment> = Vec::new();
    let mut items = Vec::with_capacity(ids.len());

    for id in ids {
        let content = context
            .state
            .read_reference_async(&id, context.cancellation.clone())
            .await?;
        match content.payload {
            ReferencePayload::Text(text) => {
                items.push(json!({
                    "id": content.id,
                    "media_type": content.media_type,
                    "kind": "text",
                    "content": text,
                }));
            }
            ReferencePayload::Binary {
                data,
                width,
                height,
            } => {
                if image_type(&content.media_type).is_some() {
                    if attachments.len() >= MAX_IMAGES_PER_TOOL_OUTPUT {
                        return Err(crate::budget::check(
                            crate::LimitKind::AttachmentCount,
                            attachments.len() + 1,
                            MAX_IMAGES_PER_TOOL_OUTPUT,
                        )
                        .unwrap_err());
                    }
                    let info = validate_image(&content.media_type, &data)?;
                    let next_media = used_media_bytes.saturating_add(data.len());
                    crate::budget::check(
                        crate::LimitKind::AttachmentBytes,
                        next_media,
                        max_media_bytes,
                    )?;
                    used_media_bytes = next_media;
                    let image_index = attachments.len();
                    items.push(json!({
                        "id": content.id,
                        "media_type": content.media_type,
                        "kind": "image",
                        "image_index": image_index,
                        "width": width.unwrap_or(info.width),
                        "height": height.unwrap_or(info.height),
                        "byte_len": data.len(),
                    }));
                    attachments.push(MediaAttachment {
                        media_type: content.media_type,
                        data,
                    });
                } else {
                    items.push(json!({
                        "id": content.id,
                        "media_type": content.media_type,
                        "kind": "binary",
                        "byte_len": data.len(),
                        "note": "binary reference; inject into tool arguments via {{ref:base64:<id>}} or {{ref:data_url:<id>}}",
                    }));
                }
            }
        }
    }

    let text = serde_json::to_string(&json!({
        "action": "expand",
        "items": items,
    }))?;
    crate::budget::check(crate::LimitKind::ToolOutput, text.len(), limits.max_tool_output_bytes)?;
    Ok(ToolOutput::Final(FinalOutput {
        content: text,
        failure: None,
        attachments,
    }))
}

async fn execute_add(
    obj: &serde_json::Map<String, Value>,
    context: &ToolContext,
) -> Result<ToolOutput, Error> {
    let explicit_kind = obj.get("kind").and_then(Value::as_str);
    let title = obj
        .get("title")
        .and_then(Value::as_str)
        .map(validate_title)
        .transpose()?;
    let locator_hint = obj
        .get("locator")
        .and_then(Value::as_str)
        .map(str::trim)
        .map(str::to_owned);
    let (media_type, payload) = if obj.contains_key("content") {
        parse_payload_fields(obj)?
    } else {
        let hint = obj
            .get("media_type")
            .and_then(Value::as_str)
            .map(canonical_mime)
            .transpose()?
            .unwrap_or_else(|| "text/plain".into());
        (hint, ReferencePayload::Text(String::new()))
    };
    let target_kind = explicit_kind.or_else(|| {
        if let ReferencePayload::Binary { .. } = &payload
            && image_type(&media_type).is_some()
            && context
                .state
                .snapshot()
                .references
                .get("media")
                .is_some_and(|r| r.capabilities().add)
        {
            Some("media")
        } else {
            None
        }
    });
    let write = ReferenceWrite {
        title,
        media_type,
        payload,
        locator_hint,
    };
    let entry = context
        .state
        .add_reference(target_kind, write, context.cancellation.clone())
        .await?;
    let quota = context
        .state
        .reference_quota(target_kind, context.cancellation.clone())
        .await?;
    Ok(ToolOutput::text(serde_json::to_string(&json!({
        "action": "add",
        "reference": entry,
        "quota": quota,
    }))?))
}

async fn execute_update(
    obj: &serde_json::Map<String, Value>,
    context: &ToolContext,
) -> Result<ToolOutput, Error> {
    let id = obj
        .get("id")
        .and_then(Value::as_str)
        .expect("validate checked scalar id");
    let title = obj
        .get("title")
        .and_then(Value::as_str)
        .map(validate_title)
        .transpose()?;
    let (media_type, payload) = if obj.contains_key("content") {
        let (mime, payload) = parse_payload_fields(obj)?;
        (Some(mime), Some(payload))
    } else {
        let mime = obj
            .get("media_type")
            .and_then(Value::as_str)
            .map(canonical_mime)
            .transpose()?;
        (mime, None)
    };
    let patch = ReferencePatch {
        expected_revision: obj
            .get("expected_revision")
            .and_then(Value::as_str)
            .map(str::to_owned),
        title,
        media_type,
        payload,
        locator_hint: obj
            .get("locator")
            .and_then(Value::as_str)
            .map(str::trim)
            .map(str::to_owned),
    };
    let entry = context
        .state
        .update_reference(id, patch, context.cancellation.clone())
        .await?;
    Ok(ToolOutput::text(serde_json::to_string(&json!({
        "action": "update",
        "reference": entry,
    }))?))
}

async fn execute_remove(
    obj: &serde_json::Map<String, Value>,
    context: &ToolContext,
) -> Result<ToolOutput, Error> {
    let ids = select_id_list(obj, "remove")?;
    let mut results = Vec::with_capacity(ids.len());
    for id in ids {
        let removed = context
            .state
            .remove_reference(&id, context.cancellation.clone())
            .await?;
        results.push(json!({ "id": id, "removed": removed }));
    }
    Ok(ToolOutput::text(serde_json::to_string(&json!({
        "action": "remove",
        "results": results,
    }))?))
}

async fn execute_refresh(context: &ToolContext) -> Result<ToolOutput, Error> {
    context
        .state
        .refresh_references_async(context.cancellation.clone())
        .await?;
    let quota = context
        .state
        .reference_quota(None, context.cancellation.clone())
        .await?;
    Ok(ToolOutput::text(serde_json::to_string(&json!({
        "action": "refresh",
        "quota": quota,
    }))?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expand_accepts_ids_array_id_selectors_and_up_to_128_entries() {
        assert!(validate(&json!({"action": "expand", "ids": ["a", "b"]})).is_ok());
        assert!(validate(&json!({"action": "expand", "id": "a"})).is_ok());
        assert!(validate(&json!({"action": "expand", "id": ["a", "b"]})).is_ok());
        assert!(validate(&json!({"action": "remove", "ids": ["a", "b"]})).is_ok());

        let ids_128: Vec<String> = (0..128).map(|i| format!("ref-{i}")).collect();
        assert!(validate(&json!({"action": "expand", "ids": ids_128})).is_ok());

        let ids_129: Vec<String> = (0..129).map(|i| format!("ref-{i}")).collect();
        assert!(validate(&json!({"action": "expand", "ids": ids_129})).is_err());

        assert!(validate(&json!({"action": "expand", "id": "a", "ids": ["b"]})).is_err());
        assert!(validate(&json!({"action": "expand", "ids": "scalar"})).is_err());
        assert!(validate(&json!({"action": "expand"})).is_err());
        assert!(validate(&json!({"action": "add", "ids": ["a"], "content": "x"})).is_err());
    }
}
