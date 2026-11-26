//! Multi-action builtin tool for dynamic Session mounts, Personal Content Library items, and unified reference expansion.
use crate::Error;
use crate::admission::image::{MAX_IMAGE_BYTES, MAX_IMAGES_PER_TOOL_OUTPUT, image_type, validate_image};
use crate::runtime::execution::MediaAttachment;
use crate::runtime::tool::{FinalOutput, Tool, ToolContext, ToolOutput};
use crate::state::reference::{
    CreateLibraryObject, LocalBlobOrigin, ReferencePayload, ShortcutTarget, UpdateLibraryObject,
    canonical_mime, detect_mime_from_bytes, inspect_binary_for_storage, normalize_shortcut,
    valid_library_id,
};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use microcrab_protocol::ToolDefinition;
use serde_json::{Value, json};
use std::collections::HashSet;

pub const NAME: &str = "reference";
pub const MAX_BATCH_IDS: usize = 16;

#[derive(Debug, Default, Clone, Copy)]
pub struct Reference;

impl Reference {
    pub fn definition() -> ToolDefinition {
        ToolDefinition {
            name: NAME.into(),
            description: "Inspect or manage session-visible references and Personal Content Library shortcuts. Actions: expand (text, images, or binary metadata by 1..16 IDs), add (create literal text/base64 or host/https shortcut and mount to this session), update (edit title, media_type, literal content, or shortcut target), remove (unmount 1..16 ref_ IDs from this session without deleting from global Personal Content Library), refresh (resync dynamic library metadata and projections)."
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
                        "description": "Reference ID or list of 1..16 unique IDs (for expand and remove; scalar ref_ ID for update)."
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
                        "description": "Canonical lowercase MIME type without parameters (for literal add or update)."
                    },
                    "source": {
                        "type": "object",
                        "description": "Source specification for add or update: {type: 'literal', content, encoding?: 'utf8'|'base64', media_type?} | {type: 'host_path', path} | {type: 'host_glob', root, pattern} | {type: 'https', url} | {type: 'directory_url', url}.",
                        "required": ["type"],
                        "properties": {
                            "type": {
                                "type": "string",
                                "enum": ["literal", "host_path", "host_glob", "https", "directory_url"]
                            },
                            "content": { "type": "string" },
                            "encoding": { "type": "string", "enum": ["utf8", "base64"] },
                            "media_type": { "type": "string", "minLength": 3, "maxLength": 127 },
                            "path": { "type": "string", "minLength": 1 },
                            "root": { "type": "string" },
                            "pattern": { "type": "string", "minLength": 1 },
                            "url": { "type": "string", "minLength": 1 }
                        }
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
    match action {
        "expand" => {
            parse_id_list(
                object
                    .get("id")
                    .ok_or_else(|| Error::tool("reference expand requires id"))?,
            )?;
            for key in ["title", "media_type", "source", "expected_revision"] {
                if object.contains_key(key) {
                    return Err(Error::tool_dynamic(format!(
                        "reference expand does not accept `{key}`"
                    )));
                }
            }
        }
        "remove" => {
            let ids = parse_id_list(
                object
                    .get("id")
                    .ok_or_else(|| Error::tool("reference remove requires id"))?,
            )?;
            for id in ids {
                if !valid_library_id(&id) {
                    return Err(Error::tool_dynamic(format!(
                        "reference remove requires dynamic library ref_ IDs, got `{id}`"
                    )));
                }
            }
            for key in ["title", "media_type", "source", "expected_revision"] {
                if object.contains_key(key) {
                    return Err(Error::tool_dynamic(format!(
                        "reference remove does not accept `{key}`"
                    )));
                }
            }
        }
        "add" => {
            if object.contains_key("id") || object.contains_key("expected_revision") {
                return Err(Error::tool(
                    "reference add does not accept id or expected_revision",
                ));
            }
            let source = object
                .get("source")
                .ok_or_else(|| Error::tool("reference add requires source"))?;
            let media_hint = object.get("media_type").and_then(Value::as_str);
            let title = object.get("title").and_then(Value::as_str);
            if let Some(t) = title {
                validate_title(t)?;
            }
            let _ = parse_source(source, media_hint)?;
        }
        "update" => {
            let id = object
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::tool("reference update requires scalar string id"))?;
            if !valid_library_id(id) {
                return Err(Error::tool(
                    "reference update requires a valid dynamic ref_ ID",
                ));
            }
            let has_title = object.get("title").and_then(Value::as_str).is_some();
            if let Some(t) = object.get("title").and_then(Value::as_str) {
                validate_title(t)?;
            }
            let media_hint = object.get("media_type").and_then(Value::as_str);
            if let Some(mime) = media_hint {
                canonical_mime(mime)?;
            }
            let has_source = if let Some(source) = object.get("source") {
                let _ = parse_source(source, media_hint)?;
                true
            } else {
                false
            };
            if !has_title && media_hint.is_none() && !has_source {
                return Err(Error::tool(
                    "reference update requires at least one of title, media_type, or source",
                ));
            }
        }
        "refresh" => {
            for key in ["id", "title", "media_type", "source", "expected_revision"] {
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

fn parse_id_list(value: &Value) -> Result<Vec<String>, Error> {
    let ids = match value {
        Value::String(id) => vec![id.clone()],
        Value::Array(items) => {
            if items.is_empty() || items.len() > MAX_BATCH_IDS {
                return Err(Error::tool("reference id array must contain 1..=16 entries"));
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
            return Err(Error::tool("reference id list must contain unique IDs"));
        }
    }
    Ok(ids)
}

enum ParsedSource {
    Literal {
        media_type: String,
        data: Vec<u8>,
    },
    Shortcut(ShortcutTarget),
}

fn parse_source(source: &Value, outer_media_type: Option<&str>) -> Result<ParsedSource, Error> {
    let obj = source
        .as_object()
        .ok_or_else(|| Error::tool("reference source must be an object"))?;
    let source_type = obj
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::tool("reference source requires type"))?;
    match source_type {
        "literal" => {
            let allowed = ["type", "content", "encoding", "media_type"];
            if obj.keys().any(|k| !allowed.contains(&k.as_str())) {
                return Err(Error::tool("unsupported field in literal source"));
            }
            let content = obj
                .get("content")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::tool("literal source requires string content"))?;
            let encoding = obj
                .get("encoding")
                .and_then(Value::as_str)
                .unwrap_or("utf8");
            let data = match encoding {
                "utf8" => content.as_bytes().to_vec(),
                "base64" => STANDARD
                    .decode(content.trim())
                    .map_err(|_| Error::tool("invalid base64 content in literal source"))?,
                other => {
                    return Err(Error::tool_dynamic(format!(
                        "unsupported literal encoding `{other}`"
                    )));
                }
            };
            let hint = obj
                .get("media_type")
                .and_then(Value::as_str)
                .or(outer_media_type);
            let mime = match hint {
                Some(raw) => inspect_binary_for_storage(raw, &data)?.0,
                None => detect_mime_from_bytes(None, &data),
            };
            if encoding == "base64" && image_type(&mime).is_some() {
                let _ = inspect_binary_for_storage(&mime, &data)?;
            }
            Ok(ParsedSource::Literal {
                media_type: mime,
                data,
            })
        }
        "host_path" => {
            let allowed = ["type", "path", "project_id", "host_id", "workspace_id"];
            if obj.keys().any(|k| !allowed.contains(&k.as_str())) {
                return Err(Error::tool("unsupported field in host_path source"));
            }
            let path = obj
                .get("path")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::tool("host_path source requires path"))?
                .to_owned();
            let target = ShortcutTarget::HostPath {
                path,
                project_id: obj.get("project_id").and_then(Value::as_str).map(str::to_owned),
                host_id: obj.get("host_id").and_then(Value::as_str).map(str::to_owned),
                workspace_id: obj
                    .get("workspace_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            };
            normalize_shortcut(&target)?;
            Ok(ParsedSource::Shortcut(target))
        }
        "host_glob" => {
            let allowed = [
                "type",
                "root",
                "pattern",
                "project_id",
                "host_id",
                "workspace_id",
            ];
            if obj.keys().any(|k| !allowed.contains(&k.as_str())) {
                return Err(Error::tool("unsupported field in host_glob source"));
            }
            let root = obj
                .get("root")
                .and_then(Value::as_str)
                .unwrap_or(".")
                .to_owned();
            let pattern = obj
                .get("pattern")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::tool("host_glob source requires pattern"))?
                .to_owned();
            let target = ShortcutTarget::HostGlob {
                root,
                pattern,
                project_id: obj.get("project_id").and_then(Value::as_str).map(str::to_owned),
                host_id: obj.get("host_id").and_then(Value::as_str).map(str::to_owned),
                workspace_id: obj
                    .get("workspace_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            };
            normalize_shortcut(&target)?;
            Ok(ParsedSource::Shortcut(target))
        }
        "https" => {
            let allowed = ["type", "url"];
            if obj.keys().any(|k| !allowed.contains(&k.as_str())) {
                return Err(Error::tool("unsupported field in https source"));
            }
            let url = obj
                .get("url")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::tool("https source requires url"))?
                .to_owned();
            let target = ShortcutTarget::PublicHttps { url };
            normalize_shortcut(&target)?;
            Ok(ParsedSource::Shortcut(target))
        }
        "directory_url" => {
            let allowed = ["type", "url"];
            if obj.keys().any(|k| !allowed.contains(&k.as_str())) {
                return Err(Error::tool("unsupported field in directory_url source"));
            }
            let url = obj
                .get("url")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::tool("directory_url source requires url"))?
                .to_owned();
            let target = ShortcutTarget::DirectoryUrl { url };
            normalize_shortcut(&target)?;
            Ok(ParsedSource::Shortcut(target))
        }
        other => Err(Error::tool_dynamic(format!(
            "unsupported reference source type `{other}`"
        ))),
    }
}

#[async_trait]
impl Tool for Reference {
    fn definition(&self) -> ToolDefinition {
        Self::definition()
    }

    fn media_output_bytes(&self) -> usize {
        MAX_IMAGE_BYTES
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
    let ids = parse_id_list(obj.get("id").expect("validate checked id"))?;
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
    let media_hint = obj.get("media_type").and_then(Value::as_str);
    let parsed = parse_source(obj.get("source").expect("validate checked source"), media_hint)?;
    let title = match obj.get("title").and_then(Value::as_str) {
        Some(raw) => validate_title(raw)?,
        None => match &parsed {
            ParsedSource::Literal { media_type, .. } => format!("literal ({media_type})"),
            ParsedSource::Shortcut(target) => {
                let loc = target.location();
                if loc.chars().count() > 256 {
                    loc.chars().take(256).collect()
                } else {
                    loc
                }
            }
        },
    };
    let request = match parsed {
        ParsedSource::Literal { media_type, data } => CreateLibraryObject::LocalBlob {
            id: None,
            title,
            media_type,
            data,
            origin: LocalBlobOrigin::AgentLiteral,
            origin_execution_id: context.execution_id.map(|id| id.0.to_string()),
        },
        ParsedSource::Shortcut(target) => CreateLibraryObject::Shortcut { title, target },
    };
    let library = context.state.reference_library();
    let summary = library.add(request, context.cancellation.clone()).await?;
    let quota = library.quota().await?;
    context
        .state
        .sync_library_projection(context.cancellation.clone())
        .await?;
    Ok(ToolOutput::text(serde_json::to_string(&json!({
        "action": "add",
        "reference": summary,
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
    let media_hint = obj.get("media_type").and_then(Value::as_str);
    let mut update = UpdateLibraryObject {
        expected_revision: obj
            .get("expected_revision")
            .and_then(Value::as_str)
            .map(str::to_owned),
        title,
        media_type: media_hint.map(canonical_mime).transpose()?,
        data: None,
        target: None,
    };
    if let Some(source) = obj.get("source") {
        match parse_source(source, media_hint)? {
            ParsedSource::Literal { media_type, data } => {
                update.media_type = Some(media_type);
                update.data = Some(data);
            }
            ParsedSource::Shortcut(target) => {
                update.target = Some(target);
            }
        }
    }
    let summary = context
        .state
        .reference_library()
        .update(id, update, context.cancellation.clone())
        .await?;
    context
        .state
        .sync_library_projection(context.cancellation.clone())
        .await?;
    Ok(ToolOutput::text(serde_json::to_string(&json!({
        "action": "update",
        "reference": summary,
    }))?))
}

async fn execute_remove(
    obj: &serde_json::Map<String, Value>,
    context: &ToolContext,
) -> Result<ToolOutput, Error> {
    let ids = parse_id_list(obj.get("id").expect("validate checked id"))?;
    let library = context.state.reference_library();
    let mut results = Vec::with_capacity(ids.len());
    for id in ids {
        let removed = library
            .remove_mount(&id, context.cancellation.clone())
            .await?;
        results.push(json!({ "id": id, "removed": removed }));
    }
    context
        .state
        .sync_library_projection(context.cancellation.clone())
        .await?;
    Ok(ToolOutput::text(serde_json::to_string(&json!({
        "action": "remove",
        "results": results,
    }))?))
}

async fn execute_refresh(context: &ToolContext) -> Result<ToolOutput, Error> {
    context
        .state
        .refresh_library(context.cancellation.clone())
        .await?;
    let quota = context.state.reference_library().quota().await?;
    Ok(ToolOutput::text(serde_json::to_string(&json!({
        "action": "refresh",
        "quota": quota,
    }))?))
}
