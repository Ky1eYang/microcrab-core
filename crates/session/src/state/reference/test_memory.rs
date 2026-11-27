//! Test-only writable in-memory Reference fixture for `microcrab-session` unit tests.
use super::*;
use async_trait::async_trait;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Mutex;

struct Entry {
    entry: ReferenceEntry,
    content: ReferenceContent,
    byte_len: u64,
}

pub(crate) struct InMemoryTestReference {
    kind: String,
    max_bytes: u64,
    max_entries: u64,
    state: Mutex<Inner>,
}

struct Inner {
    entries: BTreeMap<String, Entry>,
    used_bytes: u64,
    seq: u64,
}

impl InMemoryTestReference {
    pub(crate) fn new(kind: impl Into<String>, max_bytes: u64, max_entries: u64) -> Self {
        Self {
            kind: kind.into(),
            max_bytes,
            max_entries,
            state: Mutex::new(Inner {
                entries: BTreeMap::new(),
                used_bytes: 0,
                seq: 0,
            }),
        }
    }
}

#[async_trait]
impl Reference for InMemoryTestReference {
    fn kind(&self) -> &str {
        &self.kind
    }

    fn capabilities(&self) -> ReferenceCapabilities {
        ReferenceCapabilities::read_write()
    }

    async fn load(&self, _cancel: CancellationToken) -> Result<(), ReferenceError> {
        Ok(())
    }

    fn list(&self) -> Result<Vec<ReferenceEntry>, ReferenceError> {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(guard.entries.values().map(|item| item.entry.clone()).collect())
    }

    fn projections(&self) -> Result<ReferenceProjection, ReferenceError> {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if guard.entries.is_empty() {
            return Ok(ReferenceProjection::empty());
        }
        let rows: Vec<Value> = guard
            .entries
            .values()
            .map(|item| {
                json!({
                    "id": item.entry.id,
                    "title": item.entry.title,
                    "media_type": item.entry.media_type,
                    "revision": item.entry.revision,
                    "writable": item.entry.writable,
                })
            })
            .collect();
        Ok(ReferenceProjection {
            exact: Some(Value::Array(rows.clone())),
            short_model: Some(Value::Array(rows)),
        })
    }

    async fn expand(
        &self,
        id: &str,
        _cancel: CancellationToken,
    ) -> Result<Option<ReferenceContent>, ReferenceError> {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(guard.entries.get(id).map(|item| item.content.clone()))
    }

    async fn add(
        &self,
        input: ReferenceWrite,
        _cancel: CancellationToken,
    ) -> Result<ReferenceEntry, ReferenceError> {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if (guard.entries.len() as u64) >= self.max_entries {
            let mut err = ReferenceError::limit("reference entry quota exceeded");
            err.limit_details = Some(ReferenceLimitDetails {
                kind: "library_storage_entries",
                required_bytes: 0,
                budget_bytes: 0,
                required_count: (guard.entries.len() as u64).saturating_add(1),
                budget_count: self.max_entries,
            });
            return Err(err);
        }
        let byte_len = match &input.payload {
            ReferencePayload::Text(t) => t.len() as u64,
            ReferencePayload::Binary { data, .. } => data.len() as u64,
        };
        let next_bytes = guard.used_bytes.saturating_add(byte_len);
        if next_bytes > self.max_bytes {
            let mut err = ReferenceError::limit("reference byte quota exceeded");
            err.limit_details = Some(ReferenceLimitDetails {
                kind: "library_storage_bytes",
                required_bytes: next_bytes,
                budget_bytes: self.max_bytes,
                required_count: 0,
                budget_count: 0,
            });
            return Err(err);
        }
        guard.seq += 1;
        let id = format!("ref_{:020}", guard.seq);
        let entry = ReferenceEntry {
            id: id.clone(),
            node_kind: ReferenceNodeKind::File,
            parent_id: None,
            title: input.title.unwrap_or_else(|| id.clone()),
            location: input.locator_hint,
            description: None,
            media_type: Some(input.media_type.clone()),
            revision: format!("rev_{}", guard.seq),
            writable: true,
            metadata: None,
        };
        let content = ReferenceContent {
            id: id.clone(),
            media_type: input.media_type,
            payload: input.payload,
        };
        guard.used_bytes = next_bytes;
        guard.entries.insert(
            id,
            Entry {
                entry: entry.clone(),
                content,
                byte_len,
            },
        );
        Ok(entry)
    }

    async fn update(
        &self,
        id: &str,
        patch: ReferencePatch,
        _cancel: CancellationToken,
    ) -> Result<ReferenceEntry, ReferenceError> {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let old_used = guard.used_bytes;
        let seq = guard.seq + 1;
        let Some(existing) = guard.entries.get_mut(id) else {
            return Err(ReferenceError::not_found("reference not found"));
        };
        if let Some(expected) = patch.expected_revision
            && existing.entry.revision != expected
        {
            return Err(ReferenceError::conflict("revision conflict"));
        }
        let mut next_used = old_used;
        if let Some(payload) = patch.payload {
            let new_len = match &payload {
                ReferencePayload::Text(t) => t.len() as u64,
                ReferencePayload::Binary { data, .. } => data.len() as u64,
            };
            next_used = old_used.saturating_sub(existing.byte_len).saturating_add(new_len);
            if next_used > self.max_bytes {
                return Err(ReferenceError::limit("reference byte quota exceeded"));
            }
            existing.byte_len = new_len;
            existing.content.payload = payload;
        }
        if let Some(mime) = patch.media_type {
            existing.entry.media_type = Some(mime.clone());
            existing.content.media_type = mime;
        }
        if let Some(title) = patch.title {
            existing.entry.title = title;
        }
        existing.entry.revision = format!("rev_{seq}");
        let out = existing.entry.clone();
        guard.seq = seq;
        guard.used_bytes = next_used;
        Ok(out)
    }

    async fn remove(&self, id: &str, _cancel: CancellationToken) -> Result<bool, ReferenceError> {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(removed) = guard.entries.remove(id) {
            guard.used_bytes = guard.used_bytes.saturating_sub(removed.byte_len);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn quota(&self, _cancel: CancellationToken) -> Result<Option<ReferenceQuota>, ReferenceError> {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(Some(ReferenceQuota {
            used_bytes: guard.used_bytes,
            max_bytes: self.max_bytes,
            object_count: guard.entries.len() as u64,
            max_entries: self.max_entries,
        }))
    }
}
