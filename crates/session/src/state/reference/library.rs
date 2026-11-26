//! Dynamic user Personal Content Library contract and in-memory baseline.
//!
//! Server implements persistent owner-isolated LocalStorage backing and
//! cross-session mounts; Session owns mount visibility, shortcut resolution,
//! and strict quota errors without silent LRU eviction.
use super::{ReferenceContent, ReferenceProjection};
use crate::admission::image::{MAX_IMAGE_BYTES, image_type, signature_type, validate_image};
use crate::{Error, LimitKind};
use async_trait::async_trait;
use microcrab_host_api::{Host, OpContext, PrivateFileBuffer, ReadRequest};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

pub const DEFAULT_MAX_USER_BYTES: u64 = 256 * 1024 * 1024;
pub const DEFAULT_MAX_USER_ENTRIES: u64 = 2048;
pub const DEFAULT_MAX_OBJECT_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalBlobOrigin {
    Upload,
    JobRedirect,
    ToolCapture,
    AgentLiteral,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ShortcutTarget {
    HostPath {
        path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        project_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        host_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        workspace_id: Option<String>,
    },
    HostGlob {
        root: String,
        pattern: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        project_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        host_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        workspace_id: Option<String>,
    },
    PublicHttps {
        url: String,
    },
    DirectoryUrl {
        url: String,
    },
}

impl ShortcutTarget {
    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::HostPath { .. } => "shortcut_host_path",
            Self::HostGlob { .. } => "shortcut_host_glob",
            Self::PublicHttps { .. } => "shortcut_public_https",
            Self::DirectoryUrl { .. } => "shortcut_directory_url",
        }
    }

    pub fn location(&self) -> String {
        match self {
            Self::HostPath { path, .. } => path.clone(),
            Self::HostGlob { root, pattern, .. } => {
                let root = root.trim_end_matches('/');
                if root.is_empty() || root == "." {
                    pattern.clone()
                } else {
                    format!("{root}/{pattern}")
                }
            }
            Self::PublicHttps { url } | Self::DirectoryUrl { url } => url.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum LibrarySource {
    LocalBlob {
        origin: LocalBlobOrigin,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        origin_session_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        origin_execution_id: Option<String>,
    },
    Shortcut {
        target: ShortcutTarget,
    },
}

impl LibrarySource {
    pub fn source_kind(&self) -> &'static str {
        match self {
            Self::LocalBlob { origin, .. } => match origin {
                LocalBlobOrigin::Upload => "upload",
                LocalBlobOrigin::JobRedirect => "job_redirect",
                LocalBlobOrigin::ToolCapture => "tool_capture",
                LocalBlobOrigin::AgentLiteral => "agent_literal",
            },
            Self::Shortcut { target } => target.kind_name(),
        }
    }

    pub fn is_local_blob(&self) -> bool {
        matches!(self, Self::LocalBlob { .. })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LibraryQuota {
    pub used_bytes: u64,
    pub max_bytes: u64,
    pub object_count: u64,
    pub max_entries: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LibraryObjectSummary {
    pub id: String,
    pub title: String,
    pub source_kind: String,
    pub source: LibrarySource,
    pub media_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub byte_length: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub width: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,
    pub revision: String,
    pub read_only: bool,
    pub created_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subjects: Option<Vec<Value>>,
}

impl LibraryObjectSummary {
    pub fn projection_value(&self, short_model: bool) -> Value {
        let mut row = json!({
            "id": self.id,
            "title": self.title,
            "source_kind": self.source_kind,
            "media_type": self.media_type,
            "revision": if short_model && self.revision.len() > 16 {
                &self.revision[..16]
            } else {
                &self.revision
            },
            "read_only": self.read_only,
        });
        if let Some(bytes) = self.byte_length {
            row["byte_length"] = json!(bytes);
        }
        if let Some(width) = self.width {
            row["width"] = json!(width);
        }
        if let Some(height) = self.height {
            row["height"] = json!(height);
        }
        if let LibrarySource::Shortcut { target } = &self.source {
            row["location"] = json!(target.location());
        }
        if let Some(subjects) = &self.subjects {
            row["subjects"] = Value::Array(subjects.clone());
        }
        row
    }
}

#[derive(Clone, Debug)]
pub enum CreateLibraryObject {
    LocalBlob {
        id: Option<String>,
        title: String,
        media_type: String,
        data: Vec<u8>,
        origin: LocalBlobOrigin,
        origin_execution_id: Option<String>,
    },
    Shortcut {
        title: String,
        target: ShortcutTarget,
    },
}

#[derive(Clone, Debug, Default)]
pub struct UpdateLibraryObject {
    pub expected_revision: Option<String>,
    pub title: Option<String>,
    pub media_type: Option<String>,
    pub data: Option<Vec<u8>>,
    pub target: Option<ShortcutTarget>,
}

/// Literal redirect destination bound prior to permission/execution.
#[derive(Clone, Debug)]
pub struct LibraryRedirectDestination {
    pub expected_mime: String,
    pub title: String,
}

#[async_trait]
pub trait ReferenceLibrary: Send + Sync + 'static {
    async fn quota(&self) -> Result<LibraryQuota, Error>;

    async fn add(
        &self,
        request: CreateLibraryObject,
        cancel: CancellationToken,
    ) -> Result<LibraryObjectSummary, Error>;

    async fn update(
        &self,
        id: &str,
        update: UpdateLibraryObject,
        cancel: CancellationToken,
    ) -> Result<LibraryObjectSummary, Error>;

    async fn remove_mount(&self, id: &str, cancel: CancellationToken) -> Result<bool, Error>;

    async fn read(
        &self,
        id: &str,
        cancel: CancellationToken,
    ) -> Result<Option<ReferenceContent>, Error>;

    async fn projections(&self, cancel: CancellationToken) -> Result<ReferenceProjection, Error>;

    async fn refresh(&self, cancel: CancellationToken) -> Result<(), Error>;
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

pub fn canonical_mime(media_type: &str) -> Result<String, Error> {
    let trimmed = media_type.trim().to_ascii_lowercase();
    if trimmed.is_empty()
        || trimmed.len() > 127
        || trimmed.contains([';', '*', ' ', '\t', '\r', '\n', '\0'])
        || !trimmed.contains('/')
    {
        return Err(Error::config(
            "media_type must be a canonical lowercase type/subtype without parameters",
        ));
    }
    Ok(trimmed)
}

pub fn normalize_shortcut(target: &ShortcutTarget) -> Result<(), Error> {
    match target {
        ShortcutTarget::HostPath { path, .. } => validate_host_path(path),
        ShortcutTarget::HostGlob { root, pattern, .. } => {
            if !root.is_empty() && root != "." {
                validate_host_path(root)?;
            }
            if pattern.is_empty() || pattern.len() > 1024 || pattern.contains('\0') {
                return Err(Error::config("invalid host_glob pattern"));
            }
            Ok(())
        }
        ShortcutTarget::PublicHttps { url } => {
            microcrab_transport::public_https::PublicHttpsUrl::parse(url)
                .map(|_| ())
                .map_err(|error| Error::tool_dynamic(error.to_string()))
        }
        ShortcutTarget::DirectoryUrl { url } => {
            let parsed = reqwest::Url::parse(url)
                .map_err(|error| Error::Reference(error.to_string().into()))?;
            if parsed.scheme() != "https"
                || !parsed.username().is_empty()
                || parsed.password().is_some()
            {
                return Err(Error::reference(
                    "directory_url must be HTTPS without credentials",
                ));
            }
            Ok(())
        }
    }
}

fn validate_host_path(path: &str) -> Result<(), Error> {
    if path.is_empty() || path.len() > 4096 || path.contains('\0') {
        return Err(Error::config("host_path must be nonempty and bounded"));
    }
    Ok(())
}

pub fn inspect_binary_for_storage(
    declared_mime: &str,
    data: &[u8],
) -> Result<(String, Option<u32>, Option<u32>), Error> {
    let mime = canonical_mime(declared_mime)?;
    if data.is_empty() {
        return Err(Error::config("reference content must be nonempty"));
    }
    if image_type(&mime).is_some() {
        let detected = signature_type(data)?;
        if detected != mime {
            return Err(Error::config(
                "image data signature does not match declared MIME type",
            ));
        }
        let info = validate_image(&mime, data)?;
        return Ok((mime, Some(info.width), Some(info.height)));
    }
    Ok((mime, None, None))
}

pub fn detect_mime_from_bytes(path_or_mime: Option<&str>, data: &[u8]) -> String {
    if let Some(hint) = path_or_mime
        && let Ok(mime) = canonical_mime(hint)
    {
        return mime;
    }
    if let Ok(img) = signature_type(data) {
        return img.to_owned();
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

pub fn content_from_bytes(
    id: impl Into<String>,
    declared_or_path: Option<&str>,
    data: Vec<u8>,
) -> Result<ReferenceContent, Error> {
    let mime = detect_mime_from_bytes(declared_or_path, &data);
    if image_type(&mime).is_some() {
        let detected = signature_type(&data)?;
        let info = validate_image(detected, &data)?;
        return Ok(ReferenceContent::binary(
            id,
            detected,
            data,
            Some(info.width),
            Some(info.height),
        ));
    }
    if mime.starts_with("text/") || mime == "application/json" {
        if let Ok(text) = String::from_utf8(data.clone()) {
            return Ok(ReferenceContent {
                id: id.into(),
                media_type: mime,
                payload: super::ReferencePayload::Text(text),
            });
        }
    }
    Ok(ReferenceContent::binary(id, mime, data, None, None))
}

/// Shared Host/HTTPS shortcut content reader used by both Core in-memory
/// library and Server's persistent LocalStorage adapter.
pub async fn resolve_shortcut_content(
    id: &str,
    target: &ShortcutTarget,
    host: Option<&dyn Host>,
    max_bytes: usize,
    cancel: &CancellationToken,
) -> Result<ReferenceContent, Error> {
    match target {
        ShortcutTarget::HostPath { path, .. } => {
            let host = host.ok_or_else(|| {
                Error::tool("shortcut host_path requires a connected bound Host")
            })?;
            let path_buf = PathBuf::from(path);
            if host.supports_private_file_read() {
                let op = OpContext {
                    cancel: cancel.clone(),
                    principal: "library".into(),
                    trace_id: id.into(),
                    deadline: None,
                };
                let buffer = host
                    .read_private_file(path_buf, PrivateFileBuffer::new(max_bytes, ()), &op)
                    .await
                    .map_err(Error::from_host_error)?;
                let data = buffer.into_bytes();
                content_from_bytes(id, Some(path), data)
            } else {
                let output = host
                    .read_text(
                        ReadRequest {
                            path: path_buf,
                            offset_line: 1,
                            max_lines: usize::MAX,
                            max_bytes,
                        },
                        cancel.clone(),
                    )
                    .await
                    .map_err(Error::from_host_error)?;
                if output.truncated {
                    return Err(crate::budget::check(
                        LimitKind::ReferenceBytes,
                        max_bytes.saturating_add(1),
                        max_bytes,
                    )
                    .unwrap_err());
                }
                Ok(ReferenceContent {
                    id: id.into(),
                    media_type: detect_mime_from_bytes(Some(path), output.content.as_bytes()),
                    payload: super::ReferencePayload::Text(output.content),
                })
            }
        }
        ShortcutTarget::PublicHttps { url } => {
            let parsed = microcrab_transport::public_https::PublicHttpsUrl::parse(url)
                .map_err(|error| Error::tool_dynamic(error.to_string()))?;
            let target = parsed.admit(cancel).await.map_err(|error| {
                if cancel.is_cancelled() {
                    Error::Cancelled
                } else {
                    Error::tool_dynamic(error.to_string())
                }
            })?;
            let max_fetch = max_bytes.min(MAX_IMAGE_BYTES.max(8 * 1024 * 1024));
            let fetched = target.fetch(max_fetch, cancel).await.map_err(|error| {
                if cancel.is_cancelled() {
                    Error::Cancelled
                } else {
                    Error::tool_dynamic(error.to_string())
                }
            })?;
            content_from_bytes(id, Some(&fetched.mime_type), fetched.bytes)
        }
        ShortcutTarget::HostGlob { .. } | ShortcutTarget::DirectoryUrl { .. } => Err(
            Error::reference("collection shortcuts expose Markdown section IDs; expand those section IDs directly"),
        ),
    }
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
}

#[derive(Default)]
struct InMemoryState {
    objects: BTreeMap<String, InMemoryEntry>,
    mounted: BTreeSet<String>,
    used_bytes: u64,
    sequence: u64,
}

#[derive(Clone)]
struct InMemoryEntry {
    summary: LibraryObjectSummary,
    blob: Option<Vec<u8>>,
}

/// Default process-local in-memory ReferenceLibrary (256 MiB user cap) used for
/// embedded/detached sessions and Core test suites.
pub struct InMemoryReferenceLibrary {
    max_bytes: u64,
    max_entries: u64,
    max_object_bytes: usize,
    host: Mutex<Option<Arc<dyn Host>>>,
    inner: Mutex<InMemoryState>,
}

impl Default for InMemoryReferenceLibrary {
    fn default() -> Self {
        Self::new(
            DEFAULT_MAX_USER_BYTES,
            DEFAULT_MAX_USER_ENTRIES,
            DEFAULT_MAX_OBJECT_BYTES,
            None,
        )
    }
}

impl InMemoryReferenceLibrary {
    pub fn new(
        max_bytes: u64,
        max_entries: u64,
        max_object_bytes: usize,
        host: Option<Arc<dyn Host>>,
    ) -> Self {
        Self {
            max_bytes,
            max_entries,
            max_object_bytes,
            host: Mutex::new(host),
            inner: Mutex::new(InMemoryState::default()),
        }
    }

    pub fn set_host(&self, host: Option<Arc<dyn Host>>) {
        *self.host.lock().unwrap_or_else(|e| e.into_inner()) = host;
    }

    fn host(&self) -> Option<Arc<dyn Host>> {
        self.host.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

#[async_trait]
impl ReferenceLibrary for InMemoryReferenceLibrary {
    async fn quota(&self) -> Result<LibraryQuota, Error> {
        let state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(LibraryQuota {
            used_bytes: state.used_bytes,
            max_bytes: self.max_bytes,
            object_count: state.objects.len() as u64,
            max_entries: self.max_entries,
        })
    }

    async fn add(
        &self,
        request: CreateLibraryObject,
        cancel: CancellationToken,
    ) -> Result<LibraryObjectSummary, Error> {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let now = unix_ms();
        match request {
            CreateLibraryObject::LocalBlob {
                id,
                title,
                media_type,
                data,
                origin,
                origin_execution_id,
            } => {
                if data.len() > self.max_object_bytes {
                    crate::budget::check(
                        LimitKind::LibraryStorageBytes,
                        data.len(),
                        self.max_object_bytes,
                    )?;
                }
                let (mime, width, height) = inspect_binary_for_storage(&media_type, &data)?;
                let byte_length = u64::try_from(data.len()).map_err(|_| {
                    crate::budget::reject(LimitKind::Arithmetic, "library byte length overflow")
                })?;
                let sha256 = format!("{:x}", Sha256::digest(&data));
                let mut state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
                if (state.objects.len() as u64) >= self.max_entries {
                    crate::budget::check(
                        LimitKind::LibraryStorageEntries,
                        state.objects.len() + 1,
                        self.max_entries as usize,
                    )?;
                }
                let next_used = state.used_bytes.checked_add(byte_length).ok_or_else(|| {
                    crate::budget::reject(LimitKind::Arithmetic, "library quota overflow")
                })?;
                if next_used > self.max_bytes {
                    crate::budget::check(
                        LimitKind::LibraryStorageBytes,
                        usize::try_from(next_used).unwrap_or(usize::MAX),
                        usize::try_from(self.max_bytes).unwrap_or(usize::MAX),
                    )?;
                }
                state.sequence = state.sequence.saturating_add(1);
                let id = match id {
                    Some(id) => {
                        if !valid_library_id(&id) {
                            return Err(Error::config("invalid reference id"));
                        }
                        if state.objects.contains_key(&id) {
                            return Err(Error::config("reference id already exists"));
                        }
                        id
                    }
                    None => format!("ref_{}", uuid::Uuid::new_v4().simple()),
                };
                let summary = LibraryObjectSummary {
                    id: id.clone(),
                    title: if title.trim().is_empty() {
                        id.clone()
                    } else {
                        title
                    },
                    source_kind: match origin {
                        LocalBlobOrigin::Upload => "upload",
                        LocalBlobOrigin::JobRedirect => "job_redirect",
                        LocalBlobOrigin::ToolCapture => "tool_capture",
                        LocalBlobOrigin::AgentLiteral => "agent_literal",
                    }
                    .into(),
                    source: LibrarySource::LocalBlob {
                        origin,
                        origin_session_id: None,
                        origin_execution_id,
                    },
                    media_type: mime,
                    byte_length: Some(byte_length),
                    sha256: Some(sha256),
                    width,
                    height,
                    revision: format!("1_{}", &uuid::Uuid::new_v4().simple().to_string()[..12]),
                    read_only: false,
                    created_at_unix_ms: now,
                    updated_at_unix_ms: now,
                    subjects: None,
                };
                state.used_bytes = next_used;
                state.objects.insert(
                    id.clone(),
                    InMemoryEntry {
                        summary: summary.clone(),
                        blob: Some(data),
                    },
                );
                state.mounted.insert(id);
                Ok(summary)
            }
            CreateLibraryObject::Shortcut { title, target } => {
                normalize_shortcut(&target)?;
                let mut state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
                if (state.objects.len() as u64) >= self.max_entries {
                    crate::budget::check(
                        LimitKind::LibraryStorageEntries,
                        state.objects.len() + 1,
                        self.max_entries as usize,
                    )?;
                }
                let id = format!("ref_{}", uuid::Uuid::new_v4().simple());
                let location = target.location();
                let mime = detect_mime_from_bytes(Some(&location), b"");
                let summary = LibraryObjectSummary {
                    id: id.clone(),
                    title: if title.trim().is_empty() {
                        location
                    } else {
                        title
                    },
                    source_kind: target.kind_name().into(),
                    source: LibrarySource::Shortcut { target },
                    media_type: mime,
                    byte_length: None,
                    sha256: None,
                    width: None,
                    height: None,
                    revision: format!("1_{}", &uuid::Uuid::new_v4().simple().to_string()[..12]),
                    read_only: false,
                    created_at_unix_ms: now,
                    updated_at_unix_ms: now,
                    subjects: None,
                };
                state.objects.insert(
                    id.clone(),
                    InMemoryEntry {
                        summary: summary.clone(),
                        blob: None,
                    },
                );
                state.mounted.insert(id);
                Ok(summary)
            }
        }
    }

    async fn update(
        &self,
        id: &str,
        update: UpdateLibraryObject,
        cancel: CancellationToken,
    ) -> Result<LibraryObjectSummary, Error> {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if let Some(target) = &update.target {
            normalize_shortcut(target)?;
        }
        let validated_blob = match (&update.data, &update.media_type) {
            (Some(data), mime) => {
                if data.len() > self.max_object_bytes {
                    crate::budget::check(
                        LimitKind::LibraryStorageBytes,
                        data.len(),
                        self.max_object_bytes,
                    )?;
                }
                let mime = mime.as_deref().unwrap_or("application/octet-stream");
                Some((
                    inspect_binary_for_storage(mime, data)?,
                    data.clone(),
                ))
            }
            (None, _) => None,
        };
        let mut state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let old_used = state.used_bytes;
        if !state.mounted.contains(id) {
            return Err(Error::reference("reference is not mounted in this Session"));
        }
        let Some(entry) = state.objects.get_mut(id) else {
            return Err(Error::reference("requested reference was not found"));
        };
        if entry.summary.read_only {
            return Err(Error::denied("reference is read-only"));
        }
        if let Some(expected) = &update.expected_revision
            && &entry.summary.revision != expected
        {
            return Err(Error::Protocol(
                "reference revision conflict; refresh before updating".into(),
            ));
        }
        let mut next_used = old_used;
        if let Some(((mime, width, height), data)) = validated_blob {
            if !entry.summary.source.is_local_blob() {
                return Err(Error::config("cannot replace blob bytes on a shortcut reference"));
            }
            let old_len = entry.summary.byte_length.unwrap_or(0);
            let new_len = data.len() as u64;
            next_used = old_used
                .saturating_sub(old_len)
                .checked_add(new_len)
                .ok_or_else(|| {
                    crate::budget::reject(LimitKind::Arithmetic, "library quota overflow")
                })?;
            if next_used > self.max_bytes {
                crate::budget::check(
                    LimitKind::LibraryStorageBytes,
                    usize::try_from(next_used).unwrap_or(usize::MAX),
                    usize::try_from(self.max_bytes).unwrap_or(usize::MAX),
                )?;
            }
            entry.summary.media_type = mime;
            entry.summary.byte_length = Some(new_len);
            entry.summary.sha256 = Some(format!("{:x}", Sha256::digest(&data)));
            entry.summary.width = width;
            entry.summary.height = height;
            entry.blob = Some(data);
        } else if let Some(target) = update.target {
            if entry.summary.source.is_local_blob() {
                return Err(Error::config("cannot assign shortcut target to a blob reference"));
            }
            entry.summary.source_kind = target.kind_name().into();
            entry.summary.source = LibrarySource::Shortcut { target };
        }
        if let Some(title) = update.title
            && !title.trim().is_empty()
        {
            entry.summary.title = title;
        }
        entry.summary.updated_at_unix_ms = unix_ms();
        let next_rev_n = entry
            .summary
            .revision
            .split('_')
            .next()
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(1)
            .saturating_add(1);
        entry.summary.revision = format!(
            "{next_rev_n}_{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        );
        let updated = entry.summary.clone();
        state.used_bytes = next_used;
        Ok(updated)
    }

    async fn remove_mount(&self, id: &str, cancel: CancellationToken) -> Result<bool, Error> {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let mut state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.mounted.remove(id))
    }

    async fn read(
        &self,
        id: &str,
        cancel: CancellationToken,
    ) -> Result<Option<ReferenceContent>, Error> {
        let entry = {
            let state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if !state.mounted.contains(id) {
                return Ok(None);
            }
            state.objects.get(id).cloned()
        };
        let Some(entry) = entry else {
            return Ok(None);
        };
        match &entry.summary.source {
            LibrarySource::LocalBlob { .. } => {
                let data = entry.blob.unwrap_or_default();
                content_from_bytes(&entry.summary.id, Some(&entry.summary.media_type), data).map(Some)
            }
            LibrarySource::Shortcut { target } => {
                let host = self.host();
                resolve_shortcut_content(
                    &entry.summary.id,
                    target,
                    host.as_deref(),
                    self.max_object_bytes,
                    &cancel,
                )
                .await
                .map(Some)
            }
        }
    }

    async fn projections(&self, _cancel: CancellationToken) -> Result<ReferenceProjection, Error> {
        let state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mounted: Vec<_> = state
            .mounted
            .iter()
            .filter_map(|id| state.objects.get(id))
            .collect();
        if mounted.is_empty() {
            return Ok(ReferenceProjection {
                exact: None,
                short_model: None,
            });
        }
        let exact = Value::Array(
            mounted
                .iter()
                .map(|entry| entry.summary.projection_value(false))
                .collect(),
        );
        let short = Value::Array(
            mounted
                .iter()
                .map(|entry| entry.summary.projection_value(true))
                .collect(),
        );
        Ok(ReferenceProjection {
            exact: Some(exact),
            short_model: Some(short),
        })
    }

    async fn refresh(&self, _cancel: CancellationToken) -> Result<(), Error> {
        Ok(())
    }
}
