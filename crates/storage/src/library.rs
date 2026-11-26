//! Owner-isolated Personal Content Library persistence (metadata, blobs, and session/project mounts).
use async_trait::async_trait;
use microcrab_session::Error;
use microcrab_session::budget::{LimitKind, check, reject};
use microcrab_session::state::reference::library::{
    CreateLibraryObject, DEFAULT_MAX_OBJECT_BYTES, DEFAULT_MAX_USER_BYTES,
    DEFAULT_MAX_USER_ENTRIES, LibraryObjectSummary, LibraryQuota, LibrarySource, LocalBlobOrigin,
    UpdateLibraryObject, canonical_mime, inspect_binary_for_storage, normalize_shortcut,
    valid_library_id,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

#[cfg(feature = "storage-file")]
mod file;
#[cfg(feature = "storage-file")]
pub use file::FilePersonalLibrary;
#[cfg(feature = "storage-sqlite")]
mod sqlite;
#[cfg(feature = "storage-sqlite")]
pub use sqlite::SqlitePersonalLibrary;
#[cfg(test)]
mod tests;

pub const RECORD_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LibraryLimits {
    pub max_user_bytes: u64,
    pub max_user_entries: u64,
    pub max_object_bytes: usize,
}

impl Default for LibraryLimits {
    fn default() -> Self {
        Self {
            max_user_bytes: DEFAULT_MAX_USER_BYTES,
            max_user_entries: DEFAULT_MAX_USER_ENTRIES,
            max_object_bytes: DEFAULT_MAX_OBJECT_BYTES,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LibraryMountInfo {
    pub session_ids: Vec<String>,
    pub project_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LibraryObjectRecord {
    pub summary: LibraryObjectSummary,
    pub mounted_sessions: Vec<String>,
    pub mounted_projects: Vec<String>,
}

#[async_trait]
pub trait PersonalLibrary: Send + Sync + 'static {
    async fn quota(&self, owner: &str, limits: LibraryLimits) -> Result<LibraryQuota, Error>;

    async fn list(&self, owner: &str) -> Result<Vec<LibraryObjectRecord>, Error>;

    async fn get(&self, owner: &str, id: &str) -> Result<Option<LibraryObjectRecord>, Error>;

    async fn read_blob(&self, owner: &str, id: &str) -> Result<Option<Vec<u8>>, Error>;

    async fn create(
        &self,
        owner: &str,
        limits: LibraryLimits,
        request: CreateLibraryObject,
        session_mount: Option<&str>,
        project_mount: Option<&str>,
    ) -> Result<LibraryObjectRecord, Error>;

    async fn update(
        &self,
        owner: &str,
        limits: LibraryLimits,
        id: &str,
        update: UpdateLibraryObject,
    ) -> Result<LibraryObjectRecord, Error>;

    async fn delete(&self, owner: &str, id: &str) -> Result<bool, Error>;

    async fn mount_session(
        &self,
        owner: &str,
        id: &str,
        session_id: &str,
    ) -> Result<bool, Error>;

    async fn unmount_session(
        &self,
        owner: &str,
        id: &str,
        session_id: &str,
    ) -> Result<bool, Error>;

    async fn list_session_mounts(
        &self,
        owner: &str,
        session_id: &str,
    ) -> Result<Vec<LibraryObjectSummary>, Error>;

    async fn copy_session_mounts(
        &self,
        owner: &str,
        source_session_id: &str,
        target_session_id: &str,
    ) -> Result<(), Error>;

    async fn mount_project(
        &self,
        owner: &str,
        id: &str,
        project_id: &str,
    ) -> Result<bool, Error>;

    async fn unmount_project(
        &self,
        owner: &str,
        id: &str,
        project_id: &str,
    ) -> Result<bool, Error>;

    async fn list_project_mounts(
        &self,
        owner: &str,
        project_id: &str,
    ) -> Result<Vec<LibraryObjectSummary>, Error>;

    async fn mount_project_to_session(
        &self,
        owner: &str,
        project_id: &str,
        session_id: &str,
    ) -> Result<(), Error>;

    async fn update_shortcut_metadata(
        &self,
        owner: &str,
        id: &str,
        byte_length: Option<u64>,
        revision: String,
        subjects: Option<Vec<Value>>,
    ) -> Result<bool, Error>;

    async fn close(&self) -> Result<(), Error>;
}

pub(crate) fn validate_owner(owner: &str) -> Result<(), Error> {
    if owner.is_empty() || owner.len() > 1024 || owner.contains('\0') {
        return Err(Error::config("invalid library owner identity"));
    }
    Ok(())
}

pub(crate) fn validate_target_id(id: &str, label: &str) -> Result<(), Error> {
    if id.is_empty() || id.len() > 256 || id.contains('\0') {
        return Err(Error::config_with_private(
            "invalid library mount target",
            format!("invalid {label} mount target"),
        ));
    }
    Ok(())
}

pub(crate) fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

pub(crate) fn build_created_summary(
    request: CreateLibraryObject,
    limits: LibraryLimits,
    used_bytes: u64,
    object_count: u64,
) -> Result<(LibraryObjectSummary, Option<Vec<u8>>), Error> {
    let now = unix_ms();
    if object_count >= limits.max_user_entries {
        check(
            LimitKind::LibraryStorageEntries,
            usize::try_from(object_count.saturating_add(1)).unwrap_or(usize::MAX),
            usize::try_from(limits.max_user_entries).unwrap_or(usize::MAX),
        )?;
    }
    match request {
        CreateLibraryObject::LocalBlob {
            id,
            title,
            media_type,
            data,
            origin,
            origin_execution_id,
        } => {
            if data.len() > limits.max_object_bytes {
                check(
                    LimitKind::LibraryStorageBytes,
                    data.len(),
                    limits.max_object_bytes,
                )?;
            }
            let (mime, width, height) = inspect_binary_for_storage(&media_type, &data)?;
            let byte_length = u64::try_from(data.len())
                .map_err(|_| reject(LimitKind::Arithmetic, "library byte length overflow"))?;
            let next_used = used_bytes
                .checked_add(byte_length)
                .ok_or_else(|| reject(LimitKind::Arithmetic, "library quota overflow"))?;
            if next_used > limits.max_user_bytes {
                check(
                    LimitKind::LibraryStorageBytes,
                    usize::try_from(next_used).unwrap_or(usize::MAX),
                    usize::try_from(limits.max_user_bytes).unwrap_or(usize::MAX),
                )?;
            }
            let id = match id {
                Some(id) => {
                    if !valid_library_id(&id) {
                        return Err(Error::config("invalid reference id"));
                    }
                    id
                }
                None => format!("ref_{}", uuid::Uuid::new_v4().simple()),
            };
            let title = normalize_title(&title, &id)?;
            let sha256 = format!("{:x}", Sha256::digest(&data));
            let source_kind = match origin {
                LocalBlobOrigin::Upload => "upload",
                LocalBlobOrigin::JobRedirect => "job_redirect",
                LocalBlobOrigin::ToolCapture => "tool_capture",
                LocalBlobOrigin::AgentLiteral => "agent_literal",
            };
            let summary = LibraryObjectSummary {
                id,
                title,
                source_kind: source_kind.into(),
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
            Ok((summary, Some(data)))
        }
        CreateLibraryObject::Shortcut { title, target } => {
            normalize_shortcut(&target)?;
            let id = format!("ref_{}", uuid::Uuid::new_v4().simple());
            let title = normalize_title(&title, &id)?;
            let source_kind = target.kind_name().to_owned();
            let summary = LibraryObjectSummary {
                id,
                title,
                source_kind,
                source: LibrarySource::Shortcut { target },
                media_type: "text/markdown".into(),
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
            Ok((summary, None))
        }
    }
}

pub(crate) fn apply_object_update(
    summary: &mut LibraryObjectSummary,
    limits: LibraryLimits,
    used_bytes: u64,
    update: UpdateLibraryObject,
) -> Result<(u64, Option<Vec<u8>>), Error> {
    if summary.read_only {
        return Err(Error::denied("reference is read-only"));
    }
    if let Some(expected) = &update.expected_revision
        && &summary.revision != expected
    {
        return Err(Error::Protocol(
            "reference revision conflict; refresh before updating".into(),
        ));
    }
    let validated_blob = match (update.data, update.media_type.as_deref()) {
        (Some(data), mime_hint) => {
            if !summary.source.is_local_blob() {
                return Err(Error::config(
                    "cannot replace blob bytes on a shortcut reference",
                ));
            }
            if data.len() > limits.max_object_bytes {
                check(
                    LimitKind::LibraryStorageBytes,
                    data.len(),
                    limits.max_object_bytes,
                )?;
            }
            let mime = mime_hint.unwrap_or(&summary.media_type);
            Some((inspect_binary_for_storage(mime, &data)?, data))
        }
        (None, _) => None,
    };
    let mut next_used = used_bytes;
    let mut replacement_blob = None;
    if let Some(((mime, width, height), data)) = validated_blob {
        let old_len = summary.byte_length.unwrap_or(0);
        let new_len = u64::try_from(data.len())
            .map_err(|_| reject(LimitKind::Arithmetic, "library byte length overflow"))?;
        next_used = used_bytes
            .saturating_sub(old_len)
            .checked_add(new_len)
            .ok_or_else(|| reject(LimitKind::Arithmetic, "library quota overflow"))?;
        if next_used > limits.max_user_bytes {
            check(
                LimitKind::LibraryStorageBytes,
                usize::try_from(next_used).unwrap_or(usize::MAX),
                usize::try_from(limits.max_user_bytes).unwrap_or(usize::MAX),
            )?;
        }
        summary.media_type = mime;
        summary.byte_length = Some(new_len);
        summary.sha256 = Some(format!("{:x}", Sha256::digest(&data)));
        summary.width = width;
        summary.height = height;
        replacement_blob = Some(data);
    } else if let Some(mime) = update.media_type {
        summary.media_type = canonical_mime(&mime)?;
    }
    if let Some(target) = update.target {
        if summary.source.is_local_blob() {
            return Err(Error::config("cannot replace local blob with a shortcut"));
        }
        normalize_shortcut(&target)?;
        summary.source_kind = target.kind_name().to_owned();
        summary.source = LibrarySource::Shortcut { target };
    }
    if let Some(title) = update.title {
        summary.title = normalize_title(&title, &summary.id)?;
    }
    summary.updated_at_unix_ms = unix_ms();
    let next_rev_n = summary
        .revision
        .split('_')
        .next()
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(1)
        .saturating_add(1);
    summary.revision = format!(
        "{next_rev_n}_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    Ok((next_used, replacement_blob))
}

fn normalize_title(raw: &str, fallback: &str) -> Result<String, Error> {
    let trimmed = raw.trim();
    if trimmed.contains('\0') || trimmed.chars().count() > 256 {
        return Err(Error::config(
            "reference title must be <= 256 non-NUL characters",
        ));
    }
    if trimmed.is_empty() {
        Ok(fallback.to_owned())
    } else {
        Ok(trimmed.to_owned())
    }
}
