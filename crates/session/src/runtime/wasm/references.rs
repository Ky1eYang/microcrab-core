//! Staging and two-phase commit for Session references created or mounted
//! during WASM `init` and `invoke` script evaluations.
use crate::admission::image::{MAX_IMAGE_BYTES, image_type};
use crate::runtime::tool::ToolContext;
use crate::state::reference::{
    DynamicReferenceMount, ReferenceEntry, ReferenceQuota, ReferenceWrite,
    inspect_binary_for_storage,
};
use crate::{Error, LimitKind};
use microcrab_host_api::markdown::stable_reference_id;
use microcrab_protocol::{ReferencePayload, validate_reference_kind};
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

const MAX_STAGED_OBJECTS_PER_CALL: usize = 64;
const MAX_INLINE_CONTENT_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone)]
struct StagedCreateEntry {
    title: String,
    media_type: String,
    data: Vec<u8>,
}

#[derive(Clone)]
pub(super) struct Ingress {
    state: Arc<Mutex<IngressState>>,
}

struct IngressState {
    creates: Vec<StagedCreateEntry>,
    mounts: Vec<DynamicReferenceMount>,
    used_bytes: u64,
    used_entries: u64,
    max_bytes: u64,
    max_entries: u64,
}

pub(super) struct StagedReferences {
    creates: Vec<StagedCreateEntry>,
    mounts: Vec<DynamicReferenceMount>,
}

impl Ingress {
    pub(super) async fn new(context: &ToolContext, cancel: &CancellationToken) -> Result<Self, Error> {
        let quota = context
            .state
            .reference_quota(Some("media"), cancel.clone())
            .await?
            .unwrap_or(ReferenceQuota {
                used_bytes: 0,
                max_bytes: u64::MAX,
                object_count: 0,
                max_entries: u64::MAX,
            });
        Ok(Self {
            state: Arc::new(Mutex::new(IngressState {
                creates: Vec::new(),
                mounts: Vec::new(),
                used_bytes: quota.used_bytes,
                used_entries: quota.object_count,
                max_bytes: quota.max_bytes,
                max_entries: quota.max_entries,
            })),
        })
    }

    pub(super) fn stage_create(
        &self,
        title: String,
        media_type: String,
        data: Vec<u8>,
    ) -> Result<String, String> {
        let trimmed = title.trim();
        if trimmed.is_empty() || trimmed.chars().count() > 256 || trimmed.contains('\0') {
            return Err("title must be 1..=256 non-NUL characters".into());
        }
        if data.len() > MAX_IMAGE_BYTES {
            return Err("reference payload exceeds maximum object size".into());
        }
        let (canonical_mime, _, _) =
            inspect_binary_for_storage(&media_type, &data).map_err(|e| e.to_string())?;
        if canonical_mime.starts_with("text/") && std::str::from_utf8(&data).is_err() {
            return Err("text reference payload must be valid UTF-8".into());
        }
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if guard.creates.len() + guard.mounts.len() >= MAX_STAGED_OBJECTS_PER_CALL {
            return Err("exceeded per-invocation references limit".into());
        }
        let next_entries = guard.used_entries.saturating_add(1);
        if next_entries > guard.max_entries {
            return Err(
                "Personal Content Library entry quota exceeded; guide the user to Dashboard -> 个人内容库 to clean up entries"
                    .into(),
            );
        }
        let next_bytes = guard.used_bytes.saturating_add(data.len() as u64);
        if next_bytes > guard.max_bytes {
            return Err(
                "Personal Content Library storage quota exceeded (256 MiB default); guide the user to Dashboard -> 个人内容库 to clean up entries"
                    .into(),
            );
        }
        let id = format!("ref_{}", &uuid::Uuid::new_v4().simple().to_string()[..24]);
        guard.used_entries = next_entries;
        guard.used_bytes = next_bytes;
        guard.creates.push(StagedCreateEntry {
            title: trimmed.to_owned(),
            media_type: canonical_mime,
            data,
        });
        Ok(id)
    }

    pub(super) fn stage_host(
        &self,
        target_kind: String,
        normalized_path: String,
        title: Option<String>,
        plain_text: bool,
    ) -> Result<String, String> {
        validate_reference_kind(&target_kind).map_err(|e| e.to_string())?;
        if target_kind == "media" {
            return Err("cannot mount workspace markdown/text paths under `media`".into());
        }
        let trimmed_title = match title {
            Some(raw) => {
                let trimmed = raw.trim();
                if trimmed.is_empty() || trimmed.chars().count() > 256 || trimmed.contains('\0') {
                    return Err("title must be 1..=256 non-NUL characters".into());
                }
                Some(trimmed.to_owned())
            }
            None => None,
        };
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if guard.creates.len() + guard.mounts.len() >= MAX_STAGED_OBJECTS_PER_CALL {
            return Err("exceeded per-invocation references limit".into());
        }
        let mount = DynamicReferenceMount::Host {
            target_kind,
            path: normalized_path.clone(),
            title: trimmed_title,
            plain_text,
        };
        if !guard.mounts.contains(&mount) {
            guard.mounts.push(mount);
        }
        Ok(stable_reference_id("hf", &format!("host_markdown:{normalized_path}")))
    }

    pub(super) fn stage_inline(
        &self,
        target_kind: String,
        title: String,
        media_type: String,
        content: String,
        plain_text: bool,
    ) -> Result<String, String> {
        validate_reference_kind(&target_kind).map_err(|e| e.to_string())?;
        if target_kind == "media" {
            return Err("cannot mount inline text under `media`".into());
        }
        let trimmed = title.trim();
        if trimmed.is_empty() || trimmed.chars().count() > 256 || trimmed.contains('\0') {
            return Err("title must be 1..=256 non-NUL characters".into());
        }
        if content.len() > MAX_INLINE_CONTENT_BYTES {
            return Err("inline reference content exceeds 2 MiB limit".into());
        }
        let canonical_mime = microcrab_protocol::canonical_mime(&media_type).map_err(|e| e.to_string())?;
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if guard.creates.len() + guard.mounts.len() >= MAX_STAGED_OBJECTS_PER_CALL {
            return Err("exceeded per-invocation references limit".into());
        }
        let mut hasher = Sha256::new();
        hasher.update(trimmed.as_bytes());
        hasher.update(b"\0");
        hasher.update(canonical_mime.as_bytes());
        hasher.update(b"\0");
        hasher.update(content.as_bytes());
        let digest = format!("{:x}", hasher.finalize());
        let synthetic_path = format!("inline/{}", &digest[..16]);
        let mount = DynamicReferenceMount::Inline {
            target_kind,
            title: trimmed.to_owned(),
            media_type: canonical_mime,
            content,
            plain_text,
        };
        if !guard.mounts.contains(&mount) {
            guard.mounts.push(mount);
        }
        Ok(stable_reference_id("hf", &format!("session_inline:{synthetic_path}")))
    }

    pub(super) fn take(&mut self) -> Option<StagedReferences> {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if guard.creates.is_empty() && guard.mounts.is_empty() {
            None
        } else {
            Some(StagedReferences {
                creates: std::mem::take(&mut guard.creates),
                mounts: std::mem::take(&mut guard.mounts),
            })
        }
    }
}

impl StagedReferences {
    pub(super) async fn commit(
        self,
        context: &ToolContext,
        cancel: &CancellationToken,
    ) -> Result<Vec<ReferenceEntry>, Error> {
        let execution_id = context.execution_id.map(|id| id.0.to_string());
        let has_writable_media = context
            .state
            .snapshot()
            .references
            .get("media")
            .is_some_and(|r| r.capabilities().add);
        let mut committed = Vec::with_capacity(self.creates.len());
        for entry in self.creates {
            let (width, height) = inspect_binary_for_storage(&entry.media_type, &entry.data)
                .map(|(_, w, h)| (w, h))
                .unwrap_or((None, None));
            let is_image = image_type(&entry.media_type).is_some();
            let payload = if !is_image
                && (entry.media_type.starts_with("text/")
                    || entry.media_type == "application/json")
                && let Ok(text) = String::from_utf8(entry.data.clone())
            {
                ReferencePayload::Text(text)
            } else {
                ReferencePayload::Binary {
                    data: entry.data,
                    width,
                    height,
                }
            };
            let target_kind = (is_image && has_writable_media).then_some("media");
            let write = ReferenceWrite {
                title: Some(entry.title),
                media_type: entry.media_type,
                payload,
                locator_hint: execution_id
                    .as_ref()
                    .map(|id| format!("tool_capture:{id}")),
            };
            let summary = context
                .state
                .add_reference(target_kind, write, cancel.clone())
                .await?;
            committed.push(summary);
        }
        if !self.mounts.is_empty() {
            context
                .state
                .mount_dynamic_references(self.mounts, cancel.clone())
                .await?;
        }
        let _ = LimitKind::LibraryStorageBytes;
        Ok(committed)
    }
}
