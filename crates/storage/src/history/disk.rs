//! Immutable SHA256 checkpoint files and an atomically selected HEAD. Each
//! blocking operation owns the mutex and exclusive file lock until publication
//! settles, even when its async caller is cancelled. Only selected nodes load.
use super::{
    CreatorEnvelope, HISTORY_VERSION, History, HistoryNode, JournalCursor, JournalOptions,
    RestorePoint, RestoreTarget, Travel, canonical_bytes, content_hash, valid_hash,
};
use microcrab_session::Error;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    fs::{self, File, OpenOptions, TryLockError},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
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
impl FileJournal {
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
            let node: HistoryNode =
                read_json(&path, JournalOptions::default().max_checkpoint_bytes)?;
            node.validate(&cursor.checkpoint_id)?;
            if node.cursor(cursor.checkpoint_id.clone()) != cursor {
                return Err(invalid(
                    "journal manifest does not match its published head",
                ));
            }
            Ok(Some(RestorePoint {
                cursor,
                parent: node.parent,
                checkpoint: node.checkpoint,
            }))
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
        let directory = session_dir.as_ref().to_owned();
        tokio::task::spawn_blocking(move || {
            let directory = directory.canonicalize()?;
            let root = directory.join("journal").canonicalize()?;
            let nodes = root.join("nodes").canonicalize()?;
            if root.parent() != Some(directory.as_path()) || nodes.parent() != Some(root.as_path())
            {
                return Err(invalid("journal ancestry escapes session directory"));
            }
            let mut next = Some(head);
            let mut ancestry = Vec::new();
            let mut remaining = 64 * 1024 * 1024usize;
            while let Some(id) = next {
                valid_hash(&id)?;
                if ancestry.len() >= 4096 || ancestry.contains(&id) {
                    return Err(Error::Limit(
                        "audit journal ancestry budget exceeded".into(),
                    ));
                }
                let path = nodes.join(format!("{id}.json"));
                let metadata = fs::symlink_metadata(&path)?;
                if !metadata.is_file() || metadata.file_type().is_symlink() {
                    return Err(invalid("journal ancestry node is not an ordinary file"));
                }
                let size = usize::try_from(metadata.len())
                    .map_err(|_| invalid("journal ancestry byte budget exceeded"))?;
                remaining = remaining
                    .checked_sub(size)
                    .ok_or_else(|| invalid("journal ancestry byte budget exceeded"))?;
                let node: HistoryNode = read_json(&path, size)?;
                node.validate(&id)?;
                if node.checkpoint.session_id != session_id {
                    return Err(invalid("journal ancestry belongs to another session"));
                }
                next = node.parent;
                ancestry.push(id);
            }
            Ok(ancestry)
        })
        .await
        .map_err(|_| invalid("journal ancestry inspection worker failed"))?
    }

    /// Read selected immutable request witnesses without opening a writer or
    /// applying audit response/body/count budgets. Every visited node is verified;
    /// results run from the selected head back to its root, including empty heads.
    pub async fn inspect_request_heads(
        session_dir: impl AsRef<Path>,
        session_id: String,
        head: String,
    ) -> Result<Vec<(String, Option<String>)>, Error> {
        let directory = session_dir.as_ref().to_owned();
        tokio::task::spawn_blocking(move || {
            let directory = directory.canonicalize()?;
            let root = directory.join("journal").canonicalize()?;
            let nodes = root.join("nodes").canonicalize()?;
            if root.parent() != Some(directory.as_path()) || nodes.parent() != Some(root.as_path())
            {
                return Err(invalid("journal ancestry escapes session directory"));
            }
            let mut next = Some(head);
            let mut seen = std::collections::HashSet::new();
            let mut witnesses = Vec::new();
            while let Some(id) = next {
                valid_hash(&id)?;
                if !seen.insert(id.clone()) {
                    return Err(invalid("journal ancestry cycle"));
                }
                let path = nodes.join(format!("{id}.json"));
                let metadata = fs::symlink_metadata(&path)?;
                if !metadata.is_file() || metadata.file_type().is_symlink() {
                    return Err(invalid("journal ancestry node is not an ordinary file"));
                }
                let size = usize::try_from(metadata.len())
                    .map_err(|_| invalid("journal node length cannot be represented"))?;
                let node: HistoryNode = read_json(&path, size)?;
                node.validate(&id)?;
                if node.checkpoint.session_id != session_id {
                    return Err(invalid("journal ancestry belongs to another session"));
                }
                next = node.parent;
                witnesses.push((id, node.checkpoint.request_head));
            }
            Ok(witnesses)
        })
        .await
        .map_err(|_| invalid("journal request-head inspection worker failed"))?
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
        fs::create_dir_all(root.join("redo"))?;
        let mut disk = Self {
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
    fn node(&self, id: &str) -> Result<HistoryNode, Error> {
        valid_hash(id)?;
        let node: HistoryNode = read_json(
            &self.root.join("nodes").join(format!("{id}.json")),
            self.options.max_checkpoint_bytes,
        )?;
        node.validate(id)?;
        if self
            .manifest
            .head
            .as_ref()
            .is_some_and(|head| head.session_id != node.checkpoint.session_id)
        {
            return Err(invalid("checkpoint belongs to another session"));
        }
        Ok(node)
    }
    fn point(&self, id: &str) -> Result<RestorePoint, Error> {
        let node = self.node(id)?;
        Ok(RestorePoint {
            cursor: node.cursor(id.into()),
            parent: node.parent,
            checkpoint: node.checkpoint,
        })
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
            Travel::Undo => self
                .node(
                    &self
                        .manifest
                        .head
                        .as_ref()
                        .ok_or_else(missing)?
                        .checkpoint_id,
                )?
                .parent
                .ok_or_else(missing),
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
        let node = HistoryNode::new(expected.map(|head| head.checkpoint_id), checkpoint);
        encode_bounded(&node, self.options.max_checkpoint_bytes)?;
        let bytes = canonical_bytes(&node)?;
        let id = node.id()?;
        let cursor = node.cursor(id.clone());
        let result = immutable_bytes(&self.root.join("nodes").join(format!("{id}.json")), &bytes);
        if result.is_err() {
            self.writable = false;
        }
        result?;
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
fn read_json<T: DeserializeOwned>(path: &Path, limit: usize) -> Result<T, Error> {
    let file = File::open(path)?;
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
