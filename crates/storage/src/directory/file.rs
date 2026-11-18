//! One authoritative file directory with an exclusive writer lock and atomic CAS
//! publication. A cancelled async waiter does not abandon a blocking disk writer.
use super::{
    Directory, MAX_DIRECTORY_BYTES, OperationRecord, RECORD_VERSION, SessionRecord, conflict,
    identity,
};
use async_trait::async_trait;
use microcrab_session::Error;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

#[derive(Clone)]
pub struct FileDirectory {
    state: Arc<Mutex<State>>,
}
#[derive(Clone)]
struct Records {
    version: u32,
    sessions: BTreeMap<String, SessionRecord>,
    operations: BTreeMap<String, OperationRecord>,
    unavailable_sessions: BTreeMap<String, Unavailable>,
    unavailable_operations: BTreeMap<String, Unavailable>,
    frozen_sessions: BTreeMap<String, (Box<serde_json::value::RawValue>, SessionRecord)>,
    frozen_operations: BTreeMap<String, (Box<serde_json::value::RawValue>, OperationRecord)>,
}
impl Default for Records {
    fn default() -> Self {
        Self {
            version: RECORD_VERSION,
            sessions: BTreeMap::new(),
            operations: BTreeMap::new(),
            unavailable_sessions: BTreeMap::new(),
            unavailable_operations: BTreeMap::new(),
            frozen_sessions: BTreeMap::new(),
            frozen_operations: BTreeMap::new(),
        }
    }
}

#[derive(Clone)]
struct Unavailable {
    raw: Box<serde_json::value::RawValue>,
    owner: Option<String>,
    session_id: Option<String>,
    reason: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRecords {
    version: u32,
    sessions: BTreeMap<String, Box<serde_json::value::RawValue>>,
    operations: BTreeMap<String, Box<serde_json::value::RawValue>>,
}
impl Unavailable {
    fn new(raw: Box<serde_json::value::RawValue>, error: Error) -> Self {
        // RawValue retains nested JSON beyond the typed decoder's limits.
        // Attribute only top-level identity fields, independently of bad policy
        // payloads; inability to attribute never grants access to another owner.
        let fields =
            serde_json::from_str::<BTreeMap<String, Box<serde_json::value::RawValue>>>(raw.get())
                .ok();
        let field = |name: &str| -> Option<String> {
            fields
                .as_ref()
                .and_then(|fields| fields.get(name))
                .and_then(|raw| serde_json::from_str(raw.get()).ok())
        };
        Self {
            owner: field("owner"),
            session_id: field("session_id"),
            raw,
            reason: error.diagnostic_reason(),
        }
    }
    fn error(&self) -> Error {
        Error::Config(format!("stored directory record unavailable: {}", self.reason).into())
    }
}
impl<'de> Deserialize<'de> for Records {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = RawRecords::deserialize(deserializer)?;
        if raw.version != RECORD_VERSION {
            return Err(serde::de::Error::custom(
                "unsupported creator directory schema",
            ));
        }
        let mut records = Self::default();
        for (id, raw) in raw.sessions {
            let decoded = (|| -> Result<SessionRecord, Error> {
                let record = super::decode_session(raw.get().as_bytes())?;
                record.validate()?;
                if record.id != id {
                    return Err(conflict());
                }
                Ok(record)
            })();
            match decoded {
                Ok(record) => {
                    records
                        .frozen_sessions
                        .insert(id.clone(), (raw, record.clone()));
                    records.sessions.insert(id, record);
                }
                Err(error) => {
                    records
                        .unavailable_sessions
                        .insert(id, Unavailable::new(raw, error));
                }
            }
        }
        for (id, raw) in raw.operations {
            let decoded = (|| -> Result<OperationRecord, Error> {
                let record: OperationRecord = serde_json::from_str(raw.get())?;
                record.validate()?;
                if record.id != id
                    || !records
                        .sessions
                        .get(&record.session_id)
                        .is_some_and(|session| session.owner == record.owner)
                {
                    return Err(conflict());
                }
                Ok(record)
            })();
            match decoded {
                Ok(record) => {
                    records
                        .frozen_operations
                        .insert(id.clone(), (raw, record.clone()));
                    records.operations.insert(id, record);
                }
                Err(error) => {
                    records
                        .unavailable_operations
                        .insert(id, Unavailable::new(raw, error));
                }
            }
        }
        Ok(records)
    }
}
impl Serialize for Records {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sessions: BTreeMap<_, _> = self
            .unavailable_sessions
            .iter()
            .map(|(id, value)| (id.clone(), value.raw.clone()))
            .collect();
        let mut operations: BTreeMap<_, _> = self
            .unavailable_operations
            .iter()
            .map(|(id, value)| (id.clone(), value.raw.clone()))
            .collect();
        for (id, record) in &self.sessions {
            let raw = match self.frozen_sessions.get(id) {
                Some((raw, original)) if original == record => raw.clone(),
                _ => serde_json::value::to_raw_value(record).map_err(serde::ser::Error::custom)?,
            };
            sessions.insert(id.clone(), raw);
        }
        for (id, record) in &self.operations {
            let raw = match self.frozen_operations.get(id) {
                Some((raw, original)) if original == record => raw.clone(),
                _ => serde_json::value::to_raw_value(record).map_err(serde::ser::Error::custom)?,
            };
            operations.insert(id.clone(), raw);
        }
        RawRecords {
            version: self.version,
            sessions,
            operations,
        }
        .serialize(serializer)
    }
}
struct State {
    root: PathBuf,
    history_root: Option<PathBuf>,
    lock: Option<File>,
    records: Records,
    writable: bool,
}
impl FileDirectory {
    pub async fn open(root: impl AsRef<Path>) -> Result<Self, Error> {
        Self::configured(root, None).await
    }
    /// The history root is trusted creator configuration, never a client path.
    /// This adds a HEAD precondition; it is not a cross-file atomic transaction.
    pub async fn open_with_history(
        root: impl AsRef<Path>,
        history_root: impl AsRef<Path>,
    ) -> Result<Self, Error> {
        Self::configured(root, Some(history_root.as_ref().to_owned())).await
    }
    async fn configured(
        root: impl AsRef<Path>,
        history_root: Option<PathBuf>,
    ) -> Result<Self, Error> {
        let root = root.as_ref().to_owned();
        let state = tokio::task::spawn_blocking(move || {
            let history_root = history_root
                .map(|path| -> Result<PathBuf, Error> {
                    std::fs::create_dir_all(&path)?;
                    Ok(path.canonicalize()?)
                })
                .transpose()?;
            std::fs::create_dir_all(&root)?;
            let lock = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(root.join("lock"))?;
            lock.try_lock().map_err(|error| {
                Error::protocol_with_private_reason(
                    "creator directory lock unavailable",
                    format!("creator directory lock unavailable: {error}"),
                )
            })?;
            let path = root.join("directory.json");
            let records = if path.try_exists()? {
                let mut bytes = Vec::new();
                File::open(path)?
                    .take(MAX_DIRECTORY_BYTES as u64 + 1)
                    .read_to_end(&mut bytes)?;
                if bytes.len() > MAX_DIRECTORY_BYTES {
                    return Err(Error::config("creator directory exceeds byte budget"));
                }
                serde_json::from_slice(&bytes)?
            } else {
                Records::default()
            };
            Ok::<_, Error>(State {
                root,
                history_root,
                lock: Some(lock),
                records,
                writable: true,
            })
        })
        .await
        .map_err(|error| {
            Error::protocol_with_private_reason(
                "creator directory open worker failed",
                format!("creator directory open worker: {error}"),
            )
        })??;
        Ok(Self {
            state: Arc::new(Mutex::new(state)),
        })
    }
    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut State) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let mut state = state.lock().map_err(|_| conflict())?;
            if state.lock.is_none() {
                return Err(Error::Closed);
            }
            operation(&mut state)
        })
        .await
        .map_err(|error| {
            Error::protocol_with_private_reason(
                "creator directory worker failed",
                format!("creator directory worker: {error}"),
            )
        })?
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectedFileHead {
    version: u32,
    head: Option<crate::history::JournalCursor>,
    redo: Option<String>,
}
impl State {
    fn fence(&self, expected: &crate::history::JournalCursor) -> Result<(), Error> {
        let root = self
            .history_root
            .as_ref()
            .ok_or_else(|| Error::config("file history fence root is not configured"))?;
        if expected.session_id.is_empty()
            || expected.session_id.len() > 256
            || !expected
                .session_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte))
        {
            return Err(conflict());
        }
        let path = root
            .join(&expected.session_id)
            .join("journal")
            .join("manifest.json");
        let mut bytes = Vec::new();
        File::open(path)?.take(16385).read_to_end(&mut bytes)?;
        if bytes.len() > 16384 {
            return Err(conflict());
        }
        let selected: SelectedFileHead = serde_json::from_slice(&bytes)?;
        if selected.version != crate::history::HISTORY_VERSION
            || selected.head.as_ref() != Some(expected)
        {
            return Err(conflict());
        }
        if let Some(redo) = selected.redo {
            crate::history::valid_hash(&redo)?;
        }
        Ok(())
    }
    fn publish(&mut self, next: Records) -> Result<(), Error> {
        if !self.writable {
            return Err(Error::Protocol(
                "creator directory quarantined after publication failure".into(),
            ));
        }
        let bytes = serde_json::to_vec(&next)?;
        if bytes.len() > MAX_DIRECTORY_BYTES {
            return Err(Error::config("creator directory exceeds byte budget"));
        }
        let path = self.root.join(format!(".tmp-{}", uuid::Uuid::new_v4()));
        let result = (|| {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);
            std::fs::rename(&path, self.root.join("directory.json"))?;
            #[cfg(unix)]
            File::open(&self.root)?.sync_all()?;
            Ok::<_, Error>(())
        })();
        if result.is_err() {
            self.writable = false;
        }
        result?;
        self.records = next;
        Ok(())
    }
}
#[async_trait]
impl Directory for FileDirectory {
    async fn unavailable_unowned_count(&self) -> Result<usize, Error> {
        self.run(|state| {
            Ok(state
                .records
                .unavailable_sessions
                .values()
                .chain(state.records.unavailable_operations.values())
                .filter(|record| {
                    record
                        .owner
                        .as_deref()
                        .is_none_or(|owner| identity(owner).is_err())
                })
                .count())
        })
        .await
    }

    async fn list_entries(&self, owner: &str) -> Result<Vec<super::SessionEntry>, Error> {
        identity(owner)?;
        let owner = owner.to_owned();
        self.run(move |state| {
            let mut entries: Vec<_> = state
                .records
                .sessions
                .values()
                .filter(|record| record.owner == owner)
                .cloned()
                .map(|record| super::SessionEntry::Available { record })
                .collect();
            entries.extend(
                state
                    .records
                    .unavailable_sessions
                    .iter()
                    .filter(|(_, record)| record.owner.as_deref() == Some(owner.as_str()))
                    .map(|(id, record)| super::SessionEntry::Unavailable {
                        id: id.clone(),
                        reason: record.reason.clone(),
                    }),
            );
            Ok(entries)
        })
        .await
    }
    async fn session_owner(&self, id: &str) -> Result<Option<String>, Error> {
        identity(id)?;
        let id = id.to_owned();
        self.run(move |state| {
            if let Some(record) = state.records.unavailable_sessions.get(&id) {
                return Err(record.error());
            }
            let Some(record) = state.records.sessions.get(&id) else {
                return Ok(None);
            };
            record.validate()?;
            if record.id != id {
                return Err(conflict());
            }
            Ok(Some(record.owner.clone()))
        })
        .await
    }
    async fn get(&self, owner: &str, id: &str) -> Result<Option<SessionRecord>, Error> {
        identity(owner)?;
        identity(id)?;
        let owner = owner.to_owned();
        let id = id.to_owned();
        self.run(move |state| {
            if let Some(record) = state
                .records
                .unavailable_sessions
                .get(&id)
                .filter(|record| record.owner.as_deref() == Some(owner.as_str()))
            {
                return Err(record.error());
            }
            Ok(state
                .records
                .sessions
                .get(&id)
                .filter(|record| record.owner == owner)
                .cloned())
        })
        .await
    }
    async fn list(&self, owner: &str) -> Result<Vec<SessionRecord>, Error> {
        identity(owner)?;
        let owner = owner.to_owned();
        self.run(move |state| {
            if let Some(record) = state
                .records
                .unavailable_sessions
                .values()
                .find(|record| record.owner.as_deref() == Some(owner.as_str()))
            {
                return Err(record.error());
            }
            Ok(state
                .records
                .sessions
                .values()
                .filter(|record| record.owner == owner)
                .cloned()
                .collect())
        })
        .await
    }
    async fn create(&self, record: SessionRecord) -> Result<SessionRecord, Error> {
        record.validate()?;
        self.run(move |state| {
            if record.revision != 0
                || state.records.sessions.contains_key(&record.id)
                || state.records.unavailable_sessions.contains_key(&record.id)
            {
                return Err(conflict());
            }
            let mut next = state.records.clone();
            next.sessions.insert(record.id.clone(), record.clone());
            state.publish(next)?;
            Ok(record)
        })
        .await
    }
    async fn replace(
        &self,
        owner: &str,
        expected_revision: u64,
        record: SessionRecord,
    ) -> Result<SessionRecord, Error> {
        identity(owner)?;
        let owner = owner.to_owned();
        self.run(move |state| {
            state
                .records
                .sessions
                .get(&record.id)
                .ok_or_else(conflict)?
                .replacement(&owner, expected_revision, &record)?;
            let mut next = state.records.clone();
            next.sessions.insert(record.id.clone(), record.clone());
            state.publish(next)?;
            Ok(record)
        })
        .await
    }
    async fn get_operation(&self, owner: &str, id: &str) -> Result<Option<OperationRecord>, Error> {
        identity(owner)?;
        identity(id)?;
        let owner = owner.to_owned();
        let id = id.to_owned();
        self.run(move |state| {
            if let Some(record) = state
                .records
                .unavailable_operations
                .get(&id)
                .filter(|record| record.owner.as_deref() == Some(owner.as_str()))
            {
                return Err(record.error());
            }
            Ok(state
                .records
                .operations
                .get(&id)
                .filter(|record| record.owner == owner)
                .cloned())
        })
        .await
    }
    async fn list_operations(
        &self,
        owner: &str,
        session_id: &str,
    ) -> Result<Vec<OperationRecord>, Error> {
        identity(owner)?;
        identity(session_id)?;
        let owner = owner.to_owned();
        let session_id = session_id.to_owned();
        self.run(move |state| {
            if let Some(record) = state
                .records
                .unavailable_operations
                .values()
                .find(|record| {
                    record.owner.as_deref() == Some(owner.as_str())
                        && record.session_id.as_deref() == Some(session_id.as_str())
                })
            {
                return Err(record.error());
            }
            Ok(state
                .records
                .operations
                .values()
                .filter(|record| record.owner == owner && record.session_id == session_id)
                .cloned()
                .collect())
        })
        .await
    }
    async fn create_operation(&self, record: OperationRecord) -> Result<OperationRecord, Error> {
        record.validate()?;
        self.run(move |state| {
            if record.revision != 0
                || state.records.operations.contains_key(&record.id)
                || state
                    .records
                    .unavailable_operations
                    .contains_key(&record.id)
                || !state
                    .records
                    .sessions
                    .get(&record.session_id)
                    .is_some_and(|session| session.owner == record.owner)
            {
                return Err(conflict());
            }
            let mut next = state.records.clone();
            next.operations.insert(record.id.clone(), record.clone());
            state.publish(next)?;
            Ok(record)
        })
        .await
    }
    async fn replace_operation(
        &self,
        owner: &str,
        expected_revision: u64,
        record: OperationRecord,
    ) -> Result<OperationRecord, Error> {
        identity(owner)?;
        let owner = owner.to_owned();
        self.run(move |state| {
            state
                .records
                .operations
                .get(&record.id)
                .ok_or_else(conflict)?
                .replacement(&owner, expected_revision, &record)?;
            let mut next = state.records.clone();
            next.operations.insert(record.id.clone(), record.clone());
            state.publish(next)?;
            Ok(record)
        })
        .await
    }
    async fn settle_operation(
        &self,
        owner: &str,
        settlement: super::OperationSettlement,
    ) -> Result<(SessionRecord, OperationRecord), Error> {
        settlement.validate(owner)?;
        let owner = owner.to_owned();
        self.run(move |state| {
            settlement.check(
                &owner,
                state
                    .records
                    .sessions
                    .get(&settlement.session.id)
                    .ok_or_else(conflict)?,
                state
                    .records
                    .operations
                    .get(&settlement.operation.id)
                    .ok_or_else(conflict)?,
            )?;
            if let Some(cursor) = &settlement.journal_cursor {
                state.fence(cursor)?;
            }
            let mut next = state.records.clone();
            next.sessions
                .insert(settlement.session.id.clone(), settlement.session.clone());
            next.operations.insert(
                settlement.operation.id.clone(),
                settlement.operation.clone(),
            );
            state.publish(next)?;
            Ok((settlement.session, settlement.operation))
        })
        .await
    }
    async fn remove(&self, owner: &str, id: &str, expected_revision: u64) -> Result<usize, Error> {
        identity(owner)?;
        identity(id)?;
        let owner = owner.to_owned();
        let id = id.to_owned();
        self.run(move |state| {
            let current = state.records.sessions.get(&id).ok_or_else(conflict)?;
            if current.owner != owner || current.revision != expected_revision {
                return Err(conflict());
            }
            // Receipts reference their session, so both leave in one publication.
            let receipts: Vec<String> = state
                .records
                .operations
                .values()
                .filter(|record| record.owner == owner && record.session_id == id)
                .map(|record| record.id.clone())
                .collect();
            let mut next = state.records.clone();
            next.sessions.remove(&id);
            next.frozen_sessions.remove(&id);
            for receipt in &receipts {
                next.operations.remove(receipt);
                next.frozen_operations.remove(receipt);
            }
            // Invalid bodies still belong to deletion when both independently
            // extracted identity fields match. Never infer missing ownership.
            let unavailable_before = next.unavailable_operations.len();
            next.unavailable_operations.retain(|_, record| {
                record.owner.as_deref() != Some(owner.as_str())
                    || record.session_id.as_deref() != Some(id.as_str())
            });
            let removed = receipts.len() + unavailable_before - next.unavailable_operations.len();
            state.publish(next)?;
            Ok(removed)
        })
        .await
    }
    async fn close(&self) -> Result<(), Error> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            drop(state.lock().map_err(|_| conflict())?.lock.take());
            Ok(())
        })
        .await
        .map_err(|_| conflict())?
    }
}
