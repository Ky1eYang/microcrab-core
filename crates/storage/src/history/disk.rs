//! Immutable SHA256 checkpoint files and an atomically selected HEAD. Each
//! blocking operation owns the mutex and exclusive file lock until publication
//! settles, even when its async caller is cancelled. Only selected nodes load.
use super::ancestry::{BYTE_BUDGET, Cache, MAX_NODES, Scopes, VerifiedNode, cache_failure};
use super::dedup::{decode_restore_point, split_v5, verify_node_metadata};
use super::{
    CreatorEnvelope, HISTORY_VERSION, History, JournalCursor, JournalOptions, RestorePoint,
    RestoreTarget, Travel, canonical_bytes, content_hash, valid_hash,
};
use microcrab_session::Error;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    fs::{self, File, OpenOptions, TryLockError},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
};
const MAX_METADATA_BYTES: usize = 16 * 1024;
#[derive(Clone)]
pub struct FileJournal {
    state: Arc<Mutex<Disk>>,
}
struct Disk {
    root: PathBuf,
    lock: Option<File>,
    manifest: Manifest,
    options: JournalOptions,
    writable: bool,
    #[cfg(unix)]
    ancestry_incarnation: [(u64, u64); 3],
    /// The head point verified while opening, handed to the first head restore
    /// so it is not read, hashed and validated twice. Any other use drops it.
    opened: Option<RestorePoint>,
}
/// Redo is an immutable linked stack so HEAD publication stays constant-sized.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u32,
    head: Option<JournalCursor>,
    redo: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RedoNode {
    version: u32,
    kind: String,
    checkpoint_id: String,
    next: Option<String>,
}
fn invalid(message: &str) -> Error {
    io::Error::new(io::ErrorKind::InvalidData, message).into()
}
fn conflict() -> Error {
    Error::Protocol("journal cursor conflict".into())
}
fn missing() -> Error {
    Error::config("journal target does not exist")
}
const FILE_VERSIONS: &str = "file-versions.json";

impl FileJournal {
    pub(super) async fn file_versions(
        &self,
    ) -> Result<Option<super::file_versions::FileVersions>, Error> {
        self.run(|disk| disk.file_versions()).await
    }
    pub(super) async fn store_file_versions(
        &self,
        session: String,
        next: super::file_versions::FileVersions,
    ) -> Result<(), Error> {
        self.run(move |disk| {
            disk.ensure_writable()?;
            if next.check_successor(&session, disk.file_versions()?.as_ref())? {
                let bytes = encode_bounded(&next, super::file_versions::MAX_BYTES)?;
                if let Err(error) = atomic_bytes(&disk.root.join(FILE_VERSIONS), &bytes) {
                    disk.writable = false;
                    return Err(error);
                }
            }
            Ok(())
        })
        .await
    }
    /// Read exact sidecar receipts without acquiring a writer or activating a Session.
    pub async fn inspect_communication(
        session_dir: impl AsRef<Path>,
        session_id: String,
        agent_id: microcrab_session::state::communication::AgentId,
    ) -> Result<Option<microcrab_session::state::communication::inbox::DurableInbox>, Error> {
        let directory = session_dir.as_ref().to_owned();
        let read_directory = directory.clone();
        let stored = tokio::task::spawn_blocking(move || {
            let directory = read_directory.canonicalize()?;
            let root = directory.join("journal");
            if !root.try_exists()? {
                return Ok(None);
            }
            let actual = root.canonicalize()?;
            if actual.parent() != Some(directory.as_path()) {
                return Err(invalid("communication journal escapes session directory"));
            }
            let path = actual.join("communication.json");
            if !path.try_exists()? {
                return Ok(None);
            }
            let metadata = fs::symlink_metadata(&path)?;
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(invalid("communication sidecar is not an ordinary file"));
            }
            read_json::<super::communication::StoredInbox>(
                &path,
                super::communication::MAX_INBOX_STORAGE_BYTES,
            )
            .map(Some)
        })
        .await
        .map_err(|_| invalid("communication inspection worker failed"))??;
        let Some(stored) = stored else {
            return Ok(None);
        };
        let head = Self::inspect_head(directory).await?;
        super::communication::reconcile_inspected(
            stored,
            &session_id,
            &agent_id,
            head.as_ref().map(|point| &point.checkpoint.recover),
        )
        .map(Some)
    }
    pub(super) async fn read_inbox(
        &self,
    ) -> Result<Option<super::communication::StoredInbox>, Error> {
        self.run(|disk| disk.read_inbox()).await
    }
    pub(super) async fn store_inbox(
        &self,
        inbox: super::communication::StoredInbox,
    ) -> Result<(), Error> {
        self.run(move |disk| {
            disk.ensure_writable()?;
            if inbox.check_next(disk.read_inbox()?.as_ref())? {
                let bytes = encode_bounded(&inbox, super::communication::MAX_INBOX_STORAGE_BYTES)?;
                if let Err(error) = atomic_bytes(&disk.root.join("communication.json"), &bytes) {
                    disk.writable = false;
                    return Err(error);
                }
            }
            Ok(())
        })
        .await
    }
    /// Read one atomically published immutable checkpoint without opening a writer,
    /// creating storage, taking its exclusive lock, or moving the selected head.
    pub async fn inspect_head(
        session_dir: impl AsRef<Path>,
    ) -> Result<Option<RestorePoint>, Error> {
        let directory = session_dir.as_ref().to_owned();
        tokio::task::spawn_blocking(move || {
            let directory = directory.canonicalize()?;
            let root = directory.join("journal");
            if !root.try_exists()? {
                return Ok(None);
            }
            let actual = root.canonicalize()?;
            if actual.parent() != Some(directory.as_path()) {
                return Err(invalid("journal inspection escapes session directory"));
            }
            let manifest_path = root.join("manifest.json");
            if !manifest_path.try_exists()? {
                return Ok(None);
            }
            if fs::symlink_metadata(&manifest_path)?
                .file_type()
                .is_symlink()
            {
                return Err(invalid("journal head is a link"));
            }
            let manifest: Manifest = read_json(&manifest_path, MAX_METADATA_BYTES)?;
            if manifest.version != HISTORY_VERSION {
                return Err(invalid("journal manifest version mismatch"));
            }
            let Some(cursor) = manifest.head else {
                return Ok(None);
            };
            valid_hash(&cursor.checkpoint_id)?;
            let nodes = root.join("nodes").canonicalize()?;
            if nodes.parent() != Some(actual.as_path()) {
                return Err(invalid("journal nodes escape session directory"));
            }
            let path = nodes.join(format!("{}.json", cursor.checkpoint_id));
            if fs::symlink_metadata(&path)?.file_type().is_symlink() {
                return Err(invalid("journal node is a link"));
            }
            let max_bytes = JournalOptions::default().max_checkpoint_bytes;
            let bytes = read_bounded_bytes(&path, max_bytes)?;
            let blobs_dir = root.join("blobs");
            let point = decode_restore_point(&bytes, &cursor.checkpoint_id, &cursor.session_id, |blob_id| {
                read_blob_bytes(&blobs_dir, blob_id, max_bytes)
            })?;
            if point.cursor != cursor {
                return Err(invalid(
                    "journal manifest does not match its published head",
                ));
            }
            Ok(Some(point))
        })
        .await
        .map_err(|_| invalid("journal inspection worker failed"))?
    }

    /// Verify immutable parents of a server-selected head without opening a writer.
    /// Missing, foreign, linked or over-budget ancestry rejects the whole result.
    pub async fn inspect_ancestry(
        session_dir: impl AsRef<Path>,
        session_id: String,
        head: String,
    ) -> Result<Vec<String>, Error> {
        Self::inspect_metadata(
            session_dir.as_ref().to_owned(),
            session_id,
            head,
            BYTE_BUDGET,
            |_| false,
        )
        .await
        .map(|nodes| nodes.into_iter().map(|node| node.id).collect())
    }

    /// Incrementally verify and persist unindexed ancestry metadata in bounded
    /// `BYTE_BUDGET` chunks so future `inspect_ancestry` calls read zero full bodies.
    pub async fn warm_ancestry(
        session_dir: impl AsRef<Path>,
        session_id: String,
        head: String,
    ) -> Result<usize, Error> {
        let directory = session_dir.as_ref().to_owned();
        tokio::task::spawn_blocking(move || {
            let key = ancestry_scope(&directory, &session_id)?;
            let journal_root = directory.canonicalize()?.join("journal");
            let mut index = load_file_ancestry_index(&journal_root, &session_id);
            let mut indexed_count = 0usize;
            let mut seen = std::collections::HashSet::new();
            let mut next = Some(head);
            while next.is_some() {
                let mut remaining = BYTE_BUDGET;
                let mut chunk_added = 0usize;
                while let Some(id) = next.clone() {
                    valid_hash(&id)?;
                    if seen.len() >= MAX_NODES {
                        return Err(Error::Limit("journal ancestry node budget exceeded".into()));
                    }
                    if !seen.insert(id.clone()) {
                        return Err(Error::protocol("journal ancestry cycle"));
                    }
                    let path = key.nodes.join(format!("{id}.json"));
                    let meta = ancestry_file_metadata(&path)?;
                    if let Some((indexed_len, node)) = index.nodes.get(&id)
                        && *indexed_len == meta.len()
                    {
                        next = node.parent.clone();
                        continue;
                    }
                    let size = usize::try_from(meta.len()).map_err(|_| {
                        Error::Limit("journal ancestry hydration byte budget exceeded".into())
                    })?;
                    if remaining.checked_sub(size).is_none() && chunk_added > 0 {
                        seen.remove(&id);
                        break;
                    }
                    remaining = remaining.saturating_sub(size);
                    let bytes = read_bounded_bytes(&path, size)?;
                    let node = verify_node_metadata(&bytes, &id, &session_id)?;
                    next = node.parent.clone();
                    index.nodes.insert(id, (meta.len(), node));
                    chunk_added += 1;
                    indexed_count += 1;
                }
                if chunk_added > 0 {
                    let _ = save_file_ancestry_index(&journal_root, &index);
                }
                std::thread::yield_now();
            }
            Ok(indexed_count)
        })
        .await
        .map_err(|_| invalid("journal ancestry warm-up worker failed"))?
    }

    /// Read verified witnesses from the exact selected head back to its root.
    pub async fn inspect_request_heads(
        session_dir: impl AsRef<Path>,
        session_id: String,
        head: String,
    ) -> Result<Vec<(String, Option<String>)>, Error> {
        Self::inspect_metadata(
            session_dir.as_ref().to_owned(),
            session_id,
            head,
            BYTE_BUDGET,
            |_| false,
        )
        .await
        .map(|nodes| {
            nodes
                .into_iter()
                .map(|node| (node.id, node.request_head))
                .collect()
        })
    }

    pub(super) async fn inspect_metadata(
        directory: PathBuf,
        session_id: String,
        head: String,
        byte_budget: usize,
        stop: impl FnMut(&VerifiedNode) -> bool + Send + 'static,
    ) -> Result<Vec<VerifiedNode>, Error> {
        tokio::task::spawn_blocking(move || {
            inspect_metadata(&directory, &session_id, head, byte_budget, stop)
        })
        .await
        .map_err(|_| invalid("journal ancestry inspection worker failed"))?
    }

    pub(super) async fn request_metadata(
        &self,
        head: JournalCursor,
        stop: impl FnMut(&VerifiedNode) -> bool + Send + 'static,
    ) -> Result<Vec<VerifiedNode>, Error> {
        let session = head.session_id;
        let directory = self
            .run(move |disk| {
                disk.ensure_open()?;
                let directory = disk
                    .root
                    .parent()
                    .filter(|path| !path.as_os_str().is_empty())
                    .unwrap_or_else(|| Path::new("."));
                let key = ancestry_scope(directory, &session)?;
                #[cfg(unix)]
                if key.incarnation != disk.ancestry_incarnation {
                    return Err(conflict());
                }
                Ok((directory.to_owned(), key))
            })
            .await?;
        let (directory, key) = directory;
        tokio::task::spawn_blocking(move || {
            inspect_scoped_metadata(&directory, key, head.checkpoint_id, BYTE_BUDGET, stop)
        })
        .await
        .map_err(|_| invalid("journal ancestry inspection worker failed"))?
    }

    pub async fn open(session_dir: impl AsRef<Path>) -> Result<Self, Error> {
        Self::open_with_options(session_dir, JournalOptions::default()).await
    }
    pub async fn open_with_options(
        session_dir: impl AsRef<Path>,
        options: JournalOptions,
    ) -> Result<Self, Error> {
        let root = session_dir.as_ref().join("journal");
        let disk = tokio::task::spawn_blocking(move || Disk::open(root, options))
            .await
            .map_err(|error| {
                Error::protocol_with_private_reason(
                    "journal blocking task failed",
                    format!("journal blocking task failed: {error}"),
                )
            })??;
        Ok(Self {
            state: Arc::new(Mutex::new(disk)),
        })
    }
    async fn run<T, F>(&self, operation: F) -> Result<T, Error>
    where
        T: Send + 'static,
        F: FnOnce(&mut Disk) -> Result<T, Error> + Send + 'static,
    {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let mut disk = state
                .lock()
                .map_err(|_| invalid("journal mutex is poisoned"))?;
            operation(&mut disk)
        })
        .await
        .map_err(|error| {
            Error::protocol_with_private_reason(
                "journal blocking task failed",
                format!("journal blocking task failed: {error}"),
            )
        })?
    }
}
impl History for FileJournal {
    async fn close(&self) -> Result<(), Error> {
        self.run(|disk| {
            disk.opened = None;
            disk.writable = false;
            drop(disk.lock.take());
            Ok(())
        })
        .await
    }
    async fn commit(
        &self,
        expected: Option<JournalCursor>,
        checkpoint: CreatorEnvelope,
    ) -> Result<JournalCursor, Error> {
        self.run(move |disk| {
            disk.opened = None;
            disk.commit(expected, checkpoint)
        })
        .await
    }
    async fn restore(&self, target: RestoreTarget) -> Result<RestorePoint, Error> {
        self.run(move |disk| {
            disk.ensure_open()?;
            let opened = disk.opened.take();
            if matches!(target, RestoreTarget::Head)
                && let Some(point) = opened
                && disk.manifest.head.as_ref() == Some(&point.cursor)
            {
                return Ok(point);
            }
            let id = match target {
                RestoreTarget::Head => disk
                    .manifest
                    .head
                    .as_ref()
                    .ok_or_else(missing)?
                    .checkpoint_id
                    .clone(),
                RestoreTarget::Checkpoint { checkpoint_id } => checkpoint_id,
            };
            disk.point(&id)
        })
        .await
    }
    async fn travel(
        &self,
        expected: JournalCursor,
        direction: Travel,
    ) -> Result<RestorePoint, Error> {
        self.run(move |disk| {
            disk.opened = None;
            disk.check(&Some(expected))?;
            disk.point(&disk.target(&direction)?)
        })
        .await
    }
    async fn move_head(
        &self,
        expected: JournalCursor,
        target: JournalCursor,
        via: Travel,
    ) -> Result<JournalCursor, Error> {
        self.run(move |disk| {
            disk.opened = None;
            disk.move_head(expected, target, via)
        })
        .await
    }
}
impl Disk {
    fn file_versions(&self) -> Result<Option<super::file_versions::FileVersions>, Error> {
        let path = self.root.join(FILE_VERSIONS);
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Ok(metadata) if metadata.is_file() => (),
            Ok(_) => return Err(invalid("file version sidecar is not an ordinary file")),
            Err(error) => return Err(error.into()),
        }
        read_json(&path, super::file_versions::MAX_BYTES).map(Some)
    }
    fn read_inbox(&self) -> Result<Option<super::communication::StoredInbox>, Error> {
        let path = self.root.join("communication.json");
        if !path.try_exists()? {
            return Ok(None);
        }
        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(invalid("communication sidecar is not an ordinary file"));
        }
        read_json(&path, super::communication::MAX_INBOX_STORAGE_BYTES).map(Some)
    }
    fn open(root: PathBuf, options: JournalOptions) -> Result<Self, Error> {
        if options.max_checkpoint_bytes == 0 || options.max_checkpoint_bytes > 1024 * 1024 * 1024 {
            return Err(Error::config(
                "journal checkpoint budget must be between 1 byte and 1 GiB",
            ));
        }
        fs::create_dir_all(&root)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("lock"))?;
        lock.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => io::Error::new(
                io::ErrorKind::WouldBlock,
                "journal is already exclusively locked",
            ),
            TryLockError::Error(error) => error,
        })?;
        let path = root.join("manifest.json");
        let manifest = if path.try_exists()? {
            // Inspect version before strict decoding, rejecting schema 3 without
            // changing any of its artifacts or attempting an implicit migration.
            let value: serde_json::Value = read_json(&path, MAX_METADATA_BYTES)?;
            if value.get("version").and_then(serde_json::Value::as_u64)
                != Some(HISTORY_VERSION as u64)
            {
                return Err(invalid("unsupported journal schema; schema 4 is required"));
            }
            serde_json::from_value::<Manifest>(value)?
        } else {
            for entry in fs::read_dir(&root)? {
                if entry?.file_name() != "lock" {
                    return Err(invalid("journal data exists without a manifest"));
                }
            }
            let manifest = Manifest {
                version: HISTORY_VERSION,
                head: None,
                redo: None,
            };
            atomic_json(&path, &manifest)?;
            manifest
        };
        if let Some(redo) = &manifest.redo {
            valid_hash(redo)?;
        }
        if manifest.head.is_none() && manifest.redo.is_some() {
            return Err(invalid("empty journal has redo metadata"));
        }
        fs::create_dir_all(root.join("nodes"))?;
        fs::create_dir_all(root.join("blobs"))?;
        fs::create_dir_all(root.join("redo"))?;
        #[cfg(unix)]
        let ancestry_incarnation = {
            let root = root.canonicalize()?;
            let directory = root
                .parent()
                .ok_or_else(|| invalid("journal path has no parent"))?;
            directory_incarnation(directory, &root, &root.join("nodes"))?
        };
        let mut disk = Self {
            #[cfg(unix)]
            ancestry_incarnation,
            root,
            lock: Some(lock),
            manifest,
            options,
            writable: true,
            opened: None,
        };
        if let Some(head) = &disk.manifest.head {
            let point = disk.point(&head.checkpoint_id)?;
            if point.cursor != *head {
                return Err(invalid(
                    "journal manifest does not match its published head",
                ));
            }
            disk.opened = Some(point);
        }
        Ok(disk)
    }
    fn point(&self, id: &str) -> Result<RestorePoint, Error> {
        valid_hash(id)?;
        let path = self.root.join("nodes").join(format!("{id}.json"));
        let bytes = read_bounded_bytes(&path, self.options.max_checkpoint_bytes)?;
        let expected_session = self
            .manifest
            .head
            .as_ref()
            .map(|h| h.session_id.as_str());
        let blobs_dir = self.root.join("blobs");
        let point = if let Some(session) = expected_session {
            decode_restore_point(&bytes, id, session, |blob_id| {
                read_blob_bytes(&blobs_dir, blob_id, self.options.max_checkpoint_bytes)
            })?
        } else {
            #[derive(Deserialize)]
            struct SessionPeek {
                checkpoint: SessionField,
            }
            #[derive(Deserialize)]
            struct SessionField {
                session_id: String,
            }
            let peek: SessionPeek = serde_json::from_slice(&bytes)?;
            decode_restore_point(&bytes, id, &peek.checkpoint.session_id, |blob_id| {
                read_blob_bytes(&blobs_dir, blob_id, self.options.max_checkpoint_bytes)
            })?
        };
        Ok(point)
    }
    fn check(&self, expected: &Option<JournalCursor>) -> Result<(), Error> {
        self.ensure_open()?;
        if &self.manifest.head != expected {
            return Err(conflict());
        }
        Ok(())
    }
    fn redo_node(&self) -> Result<RedoNode, Error> {
        let id = self.manifest.redo.as_ref().ok_or_else(missing)?;
        valid_hash(id)?;
        let node: RedoNode = read_json(
            &self.root.join("redo").join(format!("{id}.json")),
            MAX_METADATA_BYTES,
        )?;
        valid_hash(&node.checkpoint_id)?;
        if let Some(next) = &node.next {
            valid_hash(next)?;
        }
        if node.version != HISTORY_VERSION || node.kind != "redo" || content_hash(&node)? != *id {
            return Err(invalid("redo content hash or schema mismatch"));
        }
        Ok(node)
    }
    fn target(&self, via: &Travel) -> Result<String, Error> {
        match via {
            Travel::Undo => {
                let head = self.manifest.head.as_ref().ok_or_else(missing)?;
                let path = self.root.join("nodes").join(format!("{}.json", head.checkpoint_id));
                let bytes = read_bounded_bytes(&path, self.options.max_checkpoint_bytes)?;
                let node = verify_node_metadata(&bytes, &head.checkpoint_id, &head.session_id)?;
                node.parent.ok_or_else(missing)
            }
            Travel::Redo => Ok(self.redo_node()?.checkpoint_id),
            Travel::To { checkpoint_id } | Travel::UndoTo { checkpoint_id } => {
                self.point(checkpoint_id)?;
                Ok(checkpoint_id.clone())
            }
        }
    }
    fn ensure_open(&self) -> Result<(), Error> {
        if self.lock.is_none() {
            return Err(Error::Closed);
        }
        Ok(())
    }
    fn ensure_writable(&self) -> Result<(), Error> {
        self.ensure_open()?;
        if !self.writable {
            return Err(invalid(
                "journal is no longer writable after a persistence failure",
            ));
        }
        Ok(())
    }
    fn publish(&mut self, manifest: Manifest) -> Result<(), Error> {
        let result = atomic_json(&self.root.join("manifest.json"), &manifest);
        if result.is_err() {
            self.writable = false;
        }
        result?;
        self.manifest = manifest;
        Ok(())
    }
    fn commit(
        &mut self,
        expected: Option<JournalCursor>,
        checkpoint: CreatorEnvelope,
    ) -> Result<JournalCursor, Error> {
        self.ensure_writable()?;
        self.check(&expected)?;
        checkpoint.validate()?;
        // Bound encoding before canonicalization can allocate the JSON tree.
        encode_bounded(&checkpoint, self.options.max_checkpoint_bytes)?;
        if let Some(head) = &expected {
            if head.session_id != checkpoint.session_id {
                return Err(conflict());
            }
            if canonical_bytes(&self.point(&head.checkpoint_id)?.checkpoint)?
                == canonical_bytes(&checkpoint)?
            {
                return Ok(head.clone());
            }
        }
        let parent = expected.map(|head| head.checkpoint_id);
        let (node, blobs) = split_v5(parent.clone(), &checkpoint)?;
        encode_bounded(&node, self.options.max_checkpoint_bytes)?;
        let bytes = canonical_bytes(&node)?;
        fs::create_dir_all(self.root.join("blobs"))?;
        for (blob_id, blob_bytes) in &blobs {
            if blob_bytes.len() > self.options.max_checkpoint_bytes {
                return Err(invalid("journal value exceeds byte budget"));
            }
            let result = immutable_bytes(
                &self.root.join("blobs").join(format!("{blob_id}.json")),
                blob_bytes,
            );
            if result.is_err() {
                self.writable = false;
            }
            result?;
        }
        let id = node.id()?;
        let cursor = node.cursor(id.clone());
        let result = immutable_bytes(&self.root.join("nodes").join(format!("{id}.json")), &bytes);
        if result.is_err() {
            self.writable = false;
        }
        result?;
        let mut index = load_file_ancestry_index(&self.root, &checkpoint.session_id);
        index.nodes.insert(
            id.clone(),
            (
                bytes.len() as u64,
                VerifiedNode {
                    id,
                    parent,
                    epoch: cursor.epoch,
                    request_head: checkpoint.request_head,
                },
            ),
        );
        let _ = save_file_ancestry_index(&self.root, &index);
        self.publish(Manifest {
            version: HISTORY_VERSION,
            head: Some(cursor.clone()),
            redo: None,
        })?;
        Ok(cursor)
    }
    fn move_head(
        &mut self,
        expected: JournalCursor,
        target: JournalCursor,
        via: Travel,
    ) -> Result<JournalCursor, Error> {
        self.ensure_writable()?;
        self.check(&Some(expected.clone()))?;
        if self.point(&self.target(&via)?)?.cursor != target {
            return Err(conflict());
        }
        let mut manifest = self.manifest.clone();
        match via {
            Travel::Undo | Travel::UndoTo { .. } => {
                let node = RedoNode {
                    version: HISTORY_VERSION,
                    kind: "redo".into(),
                    checkpoint_id: expected.checkpoint_id,
                    next: manifest.redo.clone(),
                };
                let id = content_hash(&node)?;
                let result = immutable_bytes(
                    &self.root.join("redo").join(format!("{id}.json")),
                    &canonical_bytes(&node)?,
                );
                if result.is_err() {
                    self.writable = false;
                }
                result?;
                manifest.redo = Some(id);
            }
            Travel::Redo => manifest.redo = self.redo_node()?.next,
            Travel::To { .. } => manifest.redo = None,
        }
        manifest.head = Some(target.clone());
        self.publish(manifest)?;
        Ok(target)
    }
}
#[cfg(unix)]
#[derive(Clone, Copy, PartialEq, Eq)]
struct FileStamp {
    identity: (u64, u64),
    len: u64,
    mtime: (i64, i64),
    ctime: (i64, i64),
}

#[cfg(not(unix))]
#[derive(PartialEq, Eq)]
struct FileStamp;

#[cfg(unix)]
fn identity(metadata: &fs::Metadata) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt;
    (metadata.dev(), metadata.ino())
}

fn stamp(metadata: &fs::Metadata) -> Option<FileStamp> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Some(FileStamp {
            identity: identity(metadata),
            len: metadata.len(),
            mtime: (metadata.mtime(), metadata.mtime_nsec()),
            ctime: (metadata.ctime(), metadata.ctime_nsec()),
        })
    }
    #[cfg(not(unix))]
    {
        // Do not use portable mtime/length alone as a replacement witness.
        let _ = metadata;
        None
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct FileScope {
    nodes: PathBuf,
    session: String,
    #[cfg(unix)]
    incarnation: [(u64, u64); 3],
}

static ANCESTRY: OnceLock<Scopes<FileScope, Cache<FileStamp>>> = OnceLock::new();

#[cfg(unix)]
fn directory_incarnation(
    directory: &Path,
    root: &Path,
    nodes: &Path,
) -> Result<[(u64, u64); 3], Error> {
    Ok([
        identity(&fs::metadata(directory)?),
        identity(&fs::metadata(root)?),
        identity(&fs::metadata(nodes)?),
    ])
}

fn ancestry_scope(directory: &Path, session: &str) -> Result<FileScope, Error> {
    let directory = directory.canonicalize()?;
    let root = directory.join("journal");
    let nodes = root.join("nodes");
    if root.canonicalize()? != root || nodes.canonicalize()? != nodes {
        return Err(invalid("journal ancestry escapes session directory"));
    }
    Ok(FileScope {
        #[cfg(unix)]
        incarnation: directory_incarnation(&directory, &root, &nodes)?,
        nodes,
        session: session.to_owned(),
    })
}

fn inspect_metadata(
    directory: &Path,
    session: &str,
    head: String,
    byte_budget: usize,
    stop: impl FnMut(&VerifiedNode) -> bool,
) -> Result<Vec<VerifiedNode>, Error> {
    inspect_scoped_metadata(
        directory,
        ancestry_scope(directory, session)?,
        head,
        byte_budget,
        stop,
    )
}

fn inspect_scoped_metadata(
    directory: &Path,
    key: FileScope,
    head: String,
    byte_budget: usize,
    stop: impl FnMut(&VerifiedNode) -> bool,
) -> Result<Vec<VerifiedNode>, Error> {
    if ancestry_scope(directory, &key.session)? != key {
        return Err(conflict());
    }
    let scope = ANCESTRY.get_or_init(Default::default).get(key.clone())?;
    let mut cache = scope.try_lock().map_err(|error| match error {
        std::sync::TryLockError::WouldBlock => Error::Busy,
        std::sync::TryLockError::Poisoned(_) => cache_failure(),
    })?;
    let journal_root = key.nodes.parent().unwrap_or(directory);
    let file_index = load_file_ancestry_index(journal_root, &key.session);
    let selected = cache.walk(
        head,
        byte_budget,
        |id| {
            let meta = ancestry_file_metadata(&key.nodes.join(format!("{id}.json")))?;
            if let Some((indexed_len, _)) = file_index.nodes.get(id)
                && *indexed_len == meta.len()
            {
                return Ok((stamp(&meta), 0));
            }
            let size = usize::try_from(meta.len()).map_err(|_| {
                Error::Limit("journal ancestry hydration byte budget exceeded".into())
            })?;
            Ok((stamp(&meta), size))
        },
        |id, size, expected_stamp| {
            let path = key.nodes.join(format!("{id}.json"));
            if let Some((indexed_len, node)) = file_index.nodes.get(id) {
                let meta = ancestry_file_metadata(&path)?;
                if *indexed_len == meta.len() && stamp(&meta).as_ref() == expected_stamp {
                    return Ok(node.clone());
                }
            }
            let file = File::open(&path)?;
            if stamp(&file.metadata()?).as_ref() != expected_stamp {
                return Err(conflict());
            }
            let bytes = read_file_bytes(file, size)?;
            let node = verify_node_metadata(&bytes, id, &key.session)?;
            if stamp(&ancestry_file_metadata(&path)?).as_ref() != expected_stamp {
                return Err(conflict());
            }
            Ok(node)
        },
        stop,
    )?;
    if ancestry_scope(directory, &key.session)? != key {
        return Err(conflict());
    }
    Ok(selected)
}

fn ancestry_file_metadata(path: &Path) -> Result<fs::Metadata, Error> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(invalid("journal ancestry node is not an ordinary file"));
    }
    Ok(metadata)
}

#[derive(Serialize, Deserialize, Default)]
struct FileAncestryIndex {
    session_id: String,
    nodes: std::collections::HashMap<String, (u64, VerifiedNode)>,
}

const ANCESTRY_INDEX_FILE: &str = "ancestry_index.json";
const MAX_ANCESTRY_INDEX_BYTES: usize = 8 * 1024 * 1024;

fn load_file_ancestry_index(journal_root: &Path, session_id: &str) -> FileAncestryIndex {
    let path = journal_root.join(ANCESTRY_INDEX_FILE);
    let Ok(index) = read_json::<FileAncestryIndex>(&path, MAX_ANCESTRY_INDEX_BYTES) else {
        return FileAncestryIndex {
            session_id: session_id.to_owned(),
            nodes: Default::default(),
        };
    };
    if index.session_id != session_id {
        return FileAncestryIndex {
            session_id: session_id.to_owned(),
            nodes: Default::default(),
        };
    }
    index
}

fn save_file_ancestry_index(journal_root: &Path, index: &FileAncestryIndex) -> Result<(), Error> {
    let bytes = encode_bounded(index, MAX_ANCESTRY_INDEX_BYTES)?;
    atomic_bytes(&journal_root.join(ANCESTRY_INDEX_FILE), &bytes)
}

fn read_blob_bytes(blobs_dir: &Path, id: &str, limit: usize) -> Result<Vec<u8>, Error> {
    valid_hash(id)?;
    let path = blobs_dir.join(format!("{id}.json"));
    if fs::symlink_metadata(&path)?.file_type().is_symlink() {
        return Err(invalid("journal blob is a link"));
    }
    read_bounded_bytes(&path, limit)
}

fn read_bounded_bytes(path: &Path, limit: usize) -> Result<Vec<u8>, Error> {
    read_file_bytes(File::open(path)?, limit)
}

fn read_file_bytes(file: File, limit: usize) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(invalid("journal value exceeds byte budget"));
    }
    Ok(bytes)
}

fn read_json<T: DeserializeOwned>(path: &Path, limit: usize) -> Result<T, Error> {
    read_value(File::open(path)?, limit)
}
fn read_value<T: DeserializeOwned>(file: File, limit: usize) -> Result<T, Error> {
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(invalid("journal value exceeds byte budget"));
    }
    Ok(serde_json::from_slice(&bytes)?)
}
fn encode_bounded<T: Serialize>(value: &T, limit: usize) -> Result<Vec<u8>, Error> {
    struct Bounded {
        bytes: Vec<u8>,
        limit: usize,
    }
    impl Write for Bounded {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "journal encoded value exceeds byte budget",
                ));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut output = Bounded {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut output, value)?;
    Ok(output.bytes)
}
/// An existing addressed file must have identical bytes; never overwrite it.
fn immutable_bytes(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    if path.try_exists()? {
        let mut existing = Vec::new();
        File::open(path)?
            .take(bytes.len() as u64 + 1)
            .read_to_end(&mut existing)?;
        if existing != bytes {
            return Err(invalid("immutable history file changed"));
        }
        return Ok(());
    }
    atomic_bytes(path, bytes)
}
fn atomic_json<T: Serialize>(path: &Path, value: &T) -> Result<(), Error> {
    atomic_bytes(path, &encode_bounded(value, MAX_METADATA_BYTES)?)
}
fn atomic_bytes(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid("journal path has no parent"))?;
    // Temporary identity is never stored in a record or manifest.
    let temporary = parent.join(format!(".tmp-{}", uuid::Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temporary, path)?;
    sync_directory(parent)
}
#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), Error> {
    File::open(path)?.sync_all()?;
    Ok(())
}
// Windows has no portable std directory fsync; files flush before atomic rename.
#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<(), Error> {
    Ok(())
}
