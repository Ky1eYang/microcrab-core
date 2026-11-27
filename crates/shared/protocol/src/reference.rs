//! Shared Reference trait data contracts, node hierarchy, and capability declarations.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

/// Logical hierarchy node kind, independent of any physical filesystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceNodeKind {
    /// Directory node enumerating valid child directories and files.
    Directory,
    /// File node carrying full content (text, image, or binary) and optional sections.
    File,
    /// Section node extracted from a structured document (such as Markdown / SKILL.md).
    Section,
}

/// Permission and capability flags declared by each `Reference` implementation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceCapabilities {
    pub list: bool,
    pub expand: bool,
    pub add: bool,
    pub update: bool,
    pub remove: bool,
    pub refresh: bool,
    pub quota: bool,
}

impl ReferenceCapabilities {
    pub const fn read_only() -> Self {
        Self {
            list: true,
            expand: true,
            add: false,
            update: false,
            remove: false,
            refresh: true,
            quota: false,
        }
    }

    pub const fn read_write() -> Self {
        Self {
            list: true,
            expand: true,
            add: true,
            update: true,
            remove: true,
            refresh: true,
            quota: true,
        }
    }
}

/// One listed entry in a `Reference` catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceEntry {
    pub id: String,
    pub node_kind: ReferenceNodeKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    pub revision: String,
    pub writable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

/// Compact summary retained for delegated focus snapshots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceInfo {
    pub id: String,
    pub title: String,
    pub location: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReferencePayload {
    Text(String),
    Binary {
        data: Vec<u8>,
        width: Option<u32>,
        height: Option<u32>,
    },
}

/// Body returned for one owned id. Absence is `Ok(None)`, not an error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceContent {
    pub id: String,
    pub media_type: String,
    pub payload: ReferencePayload,
}

impl ReferenceContent {
    pub fn text(id: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            media_type: "text/plain".into(),
            payload: ReferencePayload::Text(body.into()),
        }
    }

    pub fn text_with_mime(
        id: impl Into<String>,
        media_type: impl Into<String>,
        body: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            media_type: media_type.into(),
            payload: ReferencePayload::Text(body.into()),
        }
    }

    pub fn binary(
        id: impl Into<String>,
        media_type: impl Into<String>,
        data: Vec<u8>,
        width: Option<u32>,
        height: Option<u32>,
    ) -> Self {
        Self {
            id: id.into(),
            media_type: media_type.into(),
            payload: ReferencePayload::Binary {
                data,
                width,
                height,
            },
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match &self.payload {
            ReferencePayload::Text(text) => Some(text.as_str()),
            ReferencePayload::Binary { .. } => None,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        match &self.payload {
            ReferencePayload::Text(text) => text.as_bytes(),
            ReferencePayload::Binary { data, .. } => data.as_slice(),
        }
    }
}

/// Generic write payload accepted by writable `Reference` implementations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceWrite {
    pub title: Option<String>,
    pub media_type: String,
    pub payload: ReferencePayload,
    pub locator_hint: Option<String>,
}

/// Generic patch payload accepted by updatable `Reference` implementations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReferencePatch {
    pub expected_revision: Option<String>,
    pub title: Option<String>,
    pub media_type: Option<String>,
    pub payload: Option<ReferencePayload>,
    pub locator_hint: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceQuota {
    pub used_bytes: u64,
    pub max_bytes: u64,
    pub object_count: u64,
    pub max_entries: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReferenceProjection {
    pub exact: Option<Value>,
    pub short_model: Option<Value>,
}

impl ReferenceProjection {
    pub fn empty() -> Self {
        Self {
            exact: None,
            short_model: None,
        }
    }

    pub fn value(&self, short_model: bool) -> Option<&Value> {
        if short_model && let Some(value) = &self.short_model {
            return Some(value);
        }
        self.exact.as_ref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferenceErrorKind {
    Cancelled,
    Denied,
    NotFound,
    Conflict,
    Limit,
    Config,
    Unavailable,
    Failed,
}

/// Concrete error classification preserved across `Reference` implementations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceError {
    pub kind: ReferenceErrorKind,
    pub message: String,
    pub limit_details: Option<ReferenceLimitDetails>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceLimitDetails {
    pub kind: &'static str,
    pub required_bytes: u64,
    pub budget_bytes: u64,
    pub required_count: u64,
    pub budget_count: u64,
}

impl ReferenceError {
    pub fn new(kind: ReferenceErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            limit_details: None,
        }
    }

    pub fn cancelled() -> Self {
        Self::new(ReferenceErrorKind::Cancelled, "operation cancelled")
    }

    pub fn denied(message: impl Into<String>) -> Self {
        Self::new(ReferenceErrorKind::Denied, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ReferenceErrorKind::NotFound, message)
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(ReferenceErrorKind::Conflict, message)
    }

    pub fn config(message: impl Into<String>) -> Self {
        Self::new(ReferenceErrorKind::Config, message)
    }

    pub fn limit(message: impl Into<String>) -> Self {
        Self::new(ReferenceErrorKind::Limit, message)
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(ReferenceErrorKind::Unavailable, message)
    }

    pub fn failed(message: impl Into<String>) -> Self {
        Self::new(ReferenceErrorKind::Failed, message)
    }
}

impl fmt::Display for ReferenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ReferenceError {}

pub fn is_system_reserved_state_key(key: &str) -> bool {
    matches!(
        key,
        "time"
            | "notices"
            | "session"
            | "host"
            | "agent"
            | "agent_init"
            | "agent_initialization_pending"
            | "mode"
            | "jobs"
            | "permission"
            | "subagents"
            | "delegatable_agents"
            | "correspondents"
            | "external_speakers"
            | "mode_prompt"
            | "switchable_modes"
            | "executions"
            | "execution_capacity"
            | "tool_tasks"
    )
}

pub fn validate_reference_kind(kind: &str) -> Result<(), ReferenceError> {
    if kind.is_empty() || kind.contains('\0') || is_system_reserved_state_key(kind) {
        return Err(ReferenceError::config(
            "empty, invalid, or system-reserved reference kind",
        ));
    }
    Ok(())
}

pub fn valid_library_id(id: &str) -> bool {
    let Some(suffix) = id.strip_prefix("ref_") else {
        return false;
    };
    (16..=64).contains(&suffix.len())
        && suffix
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

pub fn canonical_mime(media_type: &str) -> Result<String, ReferenceError> {
    let trimmed = media_type.trim().to_ascii_lowercase();
    let Some((top, sub)) = trimmed.split_once('/') else {
        return Err(ReferenceError::config(
            "media_type must be a canonical lowercase type/subtype without parameters",
        ));
    };
    if trimmed.is_empty()
        || trimmed.len() > 127
        || trimmed.contains([';', '*', ' ', '\t', '\r', '\n', '\0'])
        || top.is_empty()
        || sub.is_empty()
        || sub.contains('/')
        || !top
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'+'))
        || !sub
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'+'))
    {
        return Err(ReferenceError::config(
            "media_type must be a canonical lowercase type/subtype without parameters",
        ));
    }
    Ok(trimmed)
}

pub fn detect_mime_from_bytes(path_or_mime: Option<&str>, data: &[u8]) -> String {
    if let Some(hint) = path_or_mime
        && let Ok(mime) = canonical_mime(hint)
        && matches!(
            mime.split('/').next().unwrap_or(""),
            "text" | "image" | "audio" | "video" | "application" | "font" | "model" | "multipart"
        )
    {
        return mime;
    }
    if data.starts_with(b"\x89PNG\r\n\x1a\n") {
        return "image/png".into();
    }
    if data.starts_with(&[0xff, 0xd8]) {
        return "image/jpeg".into();
    }
    if data.starts_with(b"GIF87a") || data.starts_with(b"GIF89a") {
        return "image/gif".into();
    }
    if data.starts_with(b"RIFF") && data.get(8..12) == Some(b"WEBP") {
        return "image/webp".into();
    }
    if let Some(path) = path_or_mime {
        let lower = path.to_ascii_lowercase();
        if lower.ends_with(".md") || lower.ends_with(".markdown") {
            return "text/markdown".into();
        }
        if lower.ends_with(".json") {
            return "application/json".into();
        }
        if lower.ends_with(".txt") || lower.ends_with(".log") {
            return "text/plain".into();
        }
        if lower.ends_with(".png") {
            return "image/png".into();
        }
        if lower.ends_with(".jpg") || lower.ends_with(".jpeg") {
            return "image/jpeg".into();
        }
        if lower.ends_with(".gif") {
            return "image/gif".into();
        }
        if lower.ends_with(".webp") {
            return "image/webp".into();
        }
    }
    if std::str::from_utf8(data).is_ok() {
        "text/plain".into()
    } else {
        "application/octet-stream".into()
    }
}
