//! File-backed owner-isolated Personal Content Library with atomic index & blob commits.
use super::{
    CreateLibraryObject, LibraryLimits, LibraryObjectRecord, LibraryObjectSummary, LibraryQuota,
    PersonalLibrary, RECORD_VERSION, UpdateLibraryObject, apply_object_update,
    build_created_summary, validate_owner, validate_target_id,
};
use async_trait::async_trait;
use microcrab_session::Error;
use microcrab_session::state::reference::valid_library_id;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct FilePersonalLibrary {
    state: Arc<Mutex<State>>,
}

struct State {
    root: PathBuf,
    _lock: Option<File>,
    index: IndexData,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IndexData {
    version: u32,
    owners: BTreeMap<String, OwnerBucket>,
}

impl Default for IndexData {
    fn default() -> Self {
        Self {
            version: RECORD_VERSION,
            owners: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnerBucket {
    objects: BTreeMap<String, StoredObject>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredObject {
    summary: LibraryObjectSummary,
    #[serde(default)]
    sessions: BTreeSet<String>,
    #[serde(default)]
    projects: BTreeSet<String>,
}

struct TempFile(PathBuf);
impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn owner_dir_name(owner: &str) -> String {
    format!("{:x}", Sha256::digest(owner.as_bytes()))
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp_path = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4().simple()));
    let _guard = TempFile(tmp_path.clone());
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp_path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

impl FilePersonalLibrary {
    pub async fn open(root: impl AsRef<Path>) -> Result<Self, Error> {
        let root = root.as_ref().to_owned();
        let state = tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&root)?;
            std::fs::create_dir_all(root.join("blobs"))?;
            let lock = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(root.join("library.lock"))?;
            lock.try_lock().map_err(|error| {
                Error::protocol_with_private_reason(
                    "library writer lock unavailable",
                    error.to_string(),
                )
            })?;
            let index_path = root.join("index.json");
            let index: IndexData = if index_path.try_exists()? {
                let mut bytes = Vec::new();
                File::open(&index_path)?
                    .take(64 * 1024 * 1024 + 1)
                    .read_to_end(&mut bytes)?;
                let decoded: IndexData = serde_json::from_slice(&bytes)?;
                if decoded.version != RECORD_VERSION {
                    return Err(Error::config("unsupported personal library index version"));
                }
                decoded
            } else {
                IndexData::default()
            };
            Ok::<_, Error>(State {
                root,
                _lock: Some(lock),
                index,
            })
        })
        .await
        .map_err(|_| Error::config("library open worker failed"))??;
        Ok(Self {
            state: Arc::new(Mutex::new(state)),
        })
    }

    async fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut State) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let mut state = state
                .lock()
                .map_err(|_| Error::Protocol("library state poisoned".into()))?;
            if state._lock.is_none() {
                return Err(Error::Closed);
            }
            f(&mut state)
        })
        .await
        .map_err(|_| Error::config("library worker failed"))?
    }

    fn persist_index(state: &State) -> Result<(), Error> {
        let bytes = serde_json::to_vec(&state.index)?;
        atomic_write(&state.root.join("index.json"), &bytes)
    }

    fn blob_path(state: &State, owner: &str, id: &str) -> PathBuf {
        state
            .root
            .join("blobs")
            .join(owner_dir_name(owner))
            .join(format!("{id}.bin"))
    }
}

fn bucket_usage(bucket: &OwnerBucket) -> (u64, u64) {
    let used = bucket
        .objects
        .values()
        .filter(|entry| entry.summary.source.is_local_blob())
        .filter_map(|entry| entry.summary.byte_length)
        .fold(0u64, u64::saturating_add);
    (used, bucket.objects.len() as u64)
}

fn to_record(entry: &StoredObject) -> LibraryObjectRecord {
    LibraryObjectRecord {
        summary: entry.summary.clone(),
        mounted_sessions: entry.sessions.iter().cloned().collect(),
        mounted_projects: entry.projects.iter().cloned().collect(),
    }
}

#[async_trait]
impl PersonalLibrary for FilePersonalLibrary {
    async fn quota(&self, owner: &str, limits: LibraryLimits) -> Result<LibraryQuota, Error> {
        validate_owner(owner)?;
        let owner = owner.to_owned();
        self.run(move |state| {
            let (used_bytes, object_count) = state
                .index
                .owners
                .get(&owner)
                .map(bucket_usage)
                .unwrap_or((0, 0));
            Ok(LibraryQuota {
                used_bytes,
                max_bytes: limits.max_user_bytes,
                object_count,
                max_entries: limits.max_user_entries,
            })
        })
        .await
    }

    async fn list(&self, owner: &str) -> Result<Vec<LibraryObjectRecord>, Error> {
        validate_owner(owner)?;
        let owner = owner.to_owned();
        self.run(move |state| {
            let Some(bucket) = state.index.owners.get(&owner) else {
                return Ok(Vec::new());
            };
            let mut rows: Vec<_> = bucket.objects.values().map(to_record).collect();
            rows.sort_by(|a, b| {
                b.summary
                    .updated_at_unix_ms
                    .cmp(&a.summary.updated_at_unix_ms)
                    .then_with(|| a.summary.id.cmp(&b.summary.id))
            });
            Ok(rows)
        })
        .await
    }

    async fn get(&self, owner: &str, id: &str) -> Result<Option<LibraryObjectRecord>, Error> {
        validate_owner(owner)?;
        if !valid_library_id(id) {
            return Ok(None);
        }
        let owner = owner.to_owned();
        let id = id.to_owned();
        self.run(move |state| {
            Ok(state
                .index
                .owners
                .get(&owner)
                .and_then(|b| b.objects.get(&id))
                .map(to_record))
        })
        .await
    }

    async fn read_blob(&self, owner: &str, id: &str) -> Result<Option<Vec<u8>>, Error> {
        validate_owner(owner)?;
        if !valid_library_id(id) {
            return Ok(None);
        }
        let owner = owner.to_owned();
        let id = id.to_owned();
        self.run(move |state| {
            let Some(entry) = state.index.owners.get(&owner).and_then(|b| b.objects.get(&id))
            else {
                return Ok(None);
            };
            if !entry.summary.source.is_local_blob() {
                return Ok(None);
            }
            let path = Self::blob_path(state, &owner, &id);
            let mut data = Vec::new();
            File::open(path)?.read_to_end(&mut data)?;
            Ok(Some(data))
        })
        .await
    }

    async fn create(
        &self,
        owner: &str,
        limits: LibraryLimits,
        request: CreateLibraryObject,
        session_mount: Option<&str>,
        project_mount: Option<&str>,
    ) -> Result<LibraryObjectRecord, Error> {
        validate_owner(owner)?;
        if let Some(session_id) = session_mount {
            validate_target_id(session_id, "session")?;
        }
        if let Some(project_id) = project_mount {
            validate_target_id(project_id, "project")?;
        }
        let owner = owner.to_owned();
        let session_mount = session_mount.map(str::to_owned);
        let project_mount = project_mount.map(str::to_owned);
        self.run(move |state| {
            let (used_bytes, object_count) = state
                .index
                .owners
                .get(&owner)
                .map(bucket_usage)
                .unwrap_or((0, 0));
            let (summary, blob) =
                build_created_summary(request, limits, used_bytes, object_count)?;
            if state
                .index
                .owners
                .get(&owner)
                .is_some_and(|b| b.objects.contains_key(&summary.id))
            {
                return Err(Error::config("reference id already exists"));
            }
            if let Some(bytes) = blob {
                let path = Self::blob_path(state, &owner, &summary.id);
                atomic_write(&path, &bytes)?;
            }
            let mut sessions = BTreeSet::new();
            if let Some(session_id) = session_mount {
                sessions.insert(session_id);
            }
            let mut projects = BTreeSet::new();
            if let Some(project_id) = project_mount {
                projects.insert(project_id);
            }
            let stored = StoredObject {
                summary: summary.clone(),
                sessions,
                projects,
            };
            let record = to_record(&stored);
            let mut next_index = state.index.clone();
            next_index
                .owners
                .entry(owner.clone())
                .or_default()
                .objects
                .insert(summary.id.clone(), stored);
            let bytes = serde_json::to_vec(&next_index)?;
            if let Err(err) = atomic_write(&state.root.join("index.json"), &bytes) {
                let _ = std::fs::remove_file(Self::blob_path(state, &owner, &summary.id));
                return Err(err);
            }
            state.index = next_index;
            Ok(record)
        })
        .await
    }

    async fn update(
        &self,
        owner: &str,
        limits: LibraryLimits,
        id: &str,
        update: UpdateLibraryObject,
    ) -> Result<LibraryObjectRecord, Error> {
        validate_owner(owner)?;
        if !valid_library_id(id) {
            return Err(Error::reference("requested reference was not found"));
        }
        let owner = owner.to_owned();
        let id = id.to_owned();
        self.run(move |state| {
            let bucket = state
                .index
                .owners
                .get(&owner)
                .ok_or_else(|| Error::reference("requested reference was not found"))?;
            let (used_bytes, _) = bucket_usage(bucket);
            let existing = bucket
                .objects
                .get(&id)
                .ok_or_else(|| Error::reference("requested reference was not found"))?;
            let mut next_stored = existing.clone();
            let (_next_used, replacement_blob) =
                apply_object_update(&mut next_stored.summary, limits, used_bytes, update)?;
            if let Some(bytes) = &replacement_blob {
                let path = Self::blob_path(state, &owner, &id);
                atomic_write(&path, bytes)?;
            }
            let record = to_record(&next_stored);
            let mut next_index = state.index.clone();
            next_index
                .owners
                .get_mut(&owner)
                .expect("owner exists")
                .objects
                .insert(id, next_stored);
            let bytes = serde_json::to_vec(&next_index)?;
            atomic_write(&state.root.join("index.json"), &bytes)?;
            state.index = next_index;
            Ok(record)
        })
        .await
    }

    async fn delete(&self, owner: &str, id: &str) -> Result<bool, Error> {
        validate_owner(owner)?;
        if !valid_library_id(id) {
            return Ok(false);
        }
        let owner = owner.to_owned();
        let id = id.to_owned();
        self.run(move |state| {
            let mut next_index = state.index.clone();
            let Some(bucket) = next_index.owners.get_mut(&owner) else {
                return Ok(false);
            };
            let Some(removed) = bucket.objects.remove(&id) else {
                return Ok(false);
            };
            if bucket.objects.is_empty() {
                next_index.owners.remove(&owner);
            }
            let bytes = serde_json::to_vec(&next_index)?;
            atomic_write(&state.root.join("index.json"), &bytes)?;
            state.index = next_index;
            if removed.summary.source.is_local_blob() {
                let _ = std::fs::remove_file(Self::blob_path(state, &owner, &id));
            }
            Ok(true)
        })
        .await
    }

    async fn mount_session(
        &self,
        owner: &str,
        id: &str,
        session_id: &str,
    ) -> Result<bool, Error> {
        validate_owner(owner)?;
        validate_target_id(session_id, "session")?;
        if !valid_library_id(id) {
            return Ok(false);
        }
        let owner = owner.to_owned();
        let id = id.to_owned();
        let session_id = session_id.to_owned();
        self.run(move |state| {
            let Some(entry) = state
                .index
                .owners
                .get_mut(&owner)
                .and_then(|b| b.objects.get_mut(&id))
            else {
                return Ok(false);
            };
            if entry.sessions.insert(session_id) {
                Self::persist_index(state)?;
            }
            Ok(true)
        })
        .await
    }

    async fn unmount_session(
        &self,
        owner: &str,
        id: &str,
        session_id: &str,
    ) -> Result<bool, Error> {
        validate_owner(owner)?;
        validate_target_id(session_id, "session")?;
        if !valid_library_id(id) {
            return Ok(false);
        }
        let owner = owner.to_owned();
        let id = id.to_owned();
        let session_id = session_id.to_owned();
        self.run(move |state| {
            let Some(entry) = state
                .index
                .owners
                .get_mut(&owner)
                .and_then(|b| b.objects.get_mut(&id))
            else {
                return Ok(false);
            };
            let removed = entry.sessions.remove(&session_id);
            if removed {
                Self::persist_index(state)?;
            }
            Ok(removed)
        })
        .await
    }

    async fn list_session_mounts(
        &self,
        owner: &str,
        session_id: &str,
    ) -> Result<Vec<LibraryObjectSummary>, Error> {
        validate_owner(owner)?;
        validate_target_id(session_id, "session")?;
        let owner = owner.to_owned();
        let session_id = session_id.to_owned();
        self.run(move |state| {
            let Some(bucket) = state.index.owners.get(&owner) else {
                return Ok(Vec::new());
            };
            Ok(bucket
                .objects
                .values()
                .filter(|entry| entry.sessions.contains(&session_id))
                .map(|entry| entry.summary.clone())
                .collect())
        })
        .await
    }

    async fn copy_session_mounts(
        &self,
        owner: &str,
        source_session_id: &str,
        target_session_id: &str,
    ) -> Result<(), Error> {
        validate_owner(owner)?;
        validate_target_id(source_session_id, "source session")?;
        validate_target_id(target_session_id, "target session")?;
        let owner = owner.to_owned();
        let source_session_id = source_session_id.to_owned();
        let target_session_id = target_session_id.to_owned();
        self.run(move |state| {
            let Some(bucket) = state.index.owners.get_mut(&owner) else {
                return Ok(());
            };
            let mut changed = false;
            for entry in bucket.objects.values_mut() {
                if entry.sessions.contains(&source_session_id)
                    && entry.sessions.insert(target_session_id.clone())
                {
                    changed = true;
                }
            }
            if changed {
                Self::persist_index(state)?;
            }
            Ok(())
        })
        .await
    }

    async fn mount_project(
        &self,
        owner: &str,
        id: &str,
        project_id: &str,
    ) -> Result<bool, Error> {
        validate_owner(owner)?;
        validate_target_id(project_id, "project")?;
        if !valid_library_id(id) {
            return Ok(false);
        }
        let owner = owner.to_owned();
        let id = id.to_owned();
        let project_id = project_id.to_owned();
        self.run(move |state| {
            let Some(entry) = state
                .index
                .owners
                .get_mut(&owner)
                .and_then(|b| b.objects.get_mut(&id))
            else {
                return Ok(false);
            };
            if entry.projects.insert(project_id) {
                Self::persist_index(state)?;
            }
            Ok(true)
        })
        .await
    }

    async fn unmount_project(
        &self,
        owner: &str,
        id: &str,
        project_id: &str,
    ) -> Result<bool, Error> {
        validate_owner(owner)?;
        validate_target_id(project_id, "project")?;
        if !valid_library_id(id) {
            return Ok(false);
        }
        let owner = owner.to_owned();
        let id = id.to_owned();
        let project_id = project_id.to_owned();
        self.run(move |state| {
            let Some(entry) = state
                .index
                .owners
                .get_mut(&owner)
                .and_then(|b| b.objects.get_mut(&id))
            else {
                return Ok(false);
            };
            let removed = entry.projects.remove(&project_id);
            if removed {
                Self::persist_index(state)?;
            }
            Ok(removed)
        })
        .await
    }

    async fn list_project_mounts(
        &self,
        owner: &str,
        project_id: &str,
    ) -> Result<Vec<LibraryObjectSummary>, Error> {
        validate_owner(owner)?;
        validate_target_id(project_id, "project")?;
        let owner = owner.to_owned();
        let project_id = project_id.to_owned();
        self.run(move |state| {
            let Some(bucket) = state.index.owners.get(&owner) else {
                return Ok(Vec::new());
            };
            Ok(bucket
                .objects
                .values()
                .filter(|entry| entry.projects.contains(&project_id))
                .map(|entry| entry.summary.clone())
                .collect())
        })
        .await
    }

    async fn mount_project_to_session(
        &self,
        owner: &str,
        project_id: &str,
        session_id: &str,
    ) -> Result<(), Error> {
        validate_owner(owner)?;
        validate_target_id(project_id, "project")?;
        validate_target_id(session_id, "session")?;
        let owner = owner.to_owned();
        let project_id = project_id.to_owned();
        let session_id = session_id.to_owned();
        self.run(move |state| {
            let Some(bucket) = state.index.owners.get_mut(&owner) else {
                return Ok(());
            };
            let mut changed = false;
            for entry in bucket.objects.values_mut() {
                if entry.projects.contains(&project_id)
                    && entry.sessions.insert(session_id.clone())
                {
                    changed = true;
                }
            }
            if changed {
                Self::persist_index(state)?;
            }
            Ok(())
        })
        .await
    }

    async fn update_shortcut_metadata(
        &self,
        owner: &str,
        id: &str,
        byte_length: Option<u64>,
        revision: String,
        subjects: Option<Vec<Value>>,
    ) -> Result<bool, Error> {
        validate_owner(owner)?;
        if !valid_library_id(id) {
            return Ok(false);
        }
        let owner = owner.to_owned();
        let id = id.to_owned();
        self.run(move |state| {
            let Some(entry) = state
                .index
                .owners
                .get_mut(&owner)
                .and_then(|b| b.objects.get_mut(&id))
            else {
                return Ok(false);
            };
            if entry.summary.source.is_local_blob() {
                return Ok(false);
            }
            entry.summary.byte_length = byte_length;
            entry.summary.revision = revision;
            entry.summary.subjects = subjects;
            entry.summary.updated_at_unix_ms = super::unix_ms();
            Self::persist_index(state)?;
            Ok(true)
        })
        .await
    }

    async fn close(&self) -> Result<(), Error> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let mut state = state
                .lock()
                .map_err(|_| Error::Protocol("library state poisoned".into()))?;
            state._lock.take();
            state.index.owners.clear();
            Ok(())
        })
        .await
        .map_err(|_| Error::config("library close worker failed"))?
    }
}
