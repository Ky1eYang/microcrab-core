//! SQLite adapter for the same v4 immutable nodes and expected-cursor contract.
//! Blocking workers retain their connection lock through publication after async
//! waiter cancellation. Host restoration still precedes coordinator move_head.
use super::ancestry::{BYTE_BUDGET, Cache, MAX_NODES, Scopes, VerifiedNode, cache_failure};
use super::dedup::{decode_restore_point, split_v5, verify_node_metadata};
use super::{
    CreatorEnvelope, History, JournalCursor, JournalOptions, RestorePoint, RestoreTarget, Travel,
    canonical_bytes, valid_hash,
};
use crate::sqlite::{failure, open};
use microcrab_session::Error;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, TryLockError, Weak},
};

#[derive(Clone)]
pub struct SqliteJournal {
    state: Arc<Mutex<Option<Database>>>,
}
struct Database {
    connection: Connection,
    session_id: String,
    options: JournalOptions,
    writable: bool,
    scope: AncestryScope,
    ancestry: SnapshotCache,
}
#[derive(Default)]
struct Head {
    cursor: Option<JournalCursor>,
    redo: Vec<String>,
}

fn conflict() -> Error {
    Error::Protocol("journal cursor conflict".into())
}
fn missing() -> Error {
    Error::config("journal target does not exist")
}

fn read_file_versions(
    connection: &Connection,
    session: &str,
) -> Result<Option<super::file_versions::FileVersions>, Error> {
    // One SELECT is also one snapshot; reject the stored length before copying a blob.
    let row: Option<(u64, Option<Vec<u8>>)> = connection.query_row(
        "SELECT length(body), CASE WHEN length(body)<=?2 THEN body END FROM journal_file_versions WHERE session_id=?1",
        params![session, super::file_versions::MAX_BYTES as i64], |row| Ok((row.get(0)?, row.get(1)?)),
    ).optional().map_err(failure)?;
    row.map(|(_, bytes)| {
        let bytes =
            bytes.ok_or_else(|| Error::config("file version sidecar exceeds its byte bound"))?;
        Ok(serde_json::from_slice(&bytes)?)
    })
    .transpose()
}

impl SqliteJournal {
    pub(super) async fn file_versions(
        &self,
    ) -> Result<Option<super::file_versions::FileVersions>, Error> {
        self.run(|db| read_file_versions(&db.connection, &db.session_id))
            .await
    }
    pub(super) async fn store_file_versions(
        &self,
        next: super::file_versions::FileVersions,
    ) -> Result<(), Error> {
        self.run(move |db| {
            if !db.writable { return Err(Error::config("SQLite journal is no longer writable")); }
            let tx = db.connection.transaction_with_behavior(TransactionBehavior::Immediate).map_err(failure)?;
            if next.check_successor(&db.session_id, read_file_versions(&tx, &db.session_id)?.as_ref())? {
                let body = serde_json::to_vec(&next)?;
                tx.execute("INSERT INTO journal_file_versions(session_id,body) VALUES(?1,?2) ON CONFLICT(session_id) DO UPDATE SET body=excluded.body", params![db.session_id, body]).map_err(failure)?;
                if let Err(error) = tx.commit() {
                    db.writable = false;
                    return Err(failure(error));
                }
            }
            Ok(())
        }).await
    }
    /// Read exact sidecar receipts without schema creation or Session activation.
    pub async fn inspect_communication(
        path: impl AsRef<Path>,
        session_id: String,
        agent_id: microcrab_session::state::communication::AgentId,
    ) -> Result<Option<microcrab_session::state::communication::inbox::DurableInbox>, Error> {
        let path = path.as_ref().to_owned();
        tokio::task::spawn_blocking(move||{
            if !path.try_exists()?{return Ok(None);}
            let connection=Connection::open_with_flags(path,rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY|rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX).map_err(failure)?;
            connection.execute_batch("PRAGMA query_only=ON; BEGIN DEFERRED;").map_err(failure)?;
            let exists:bool=connection.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='journal_communication')",[],|row|row.get(0)).map_err(failure)?;
            if !exists{return Ok(None);}
            let Some(stored)=read_inbox(&connection,&session_id)? else{return Ok(None);};
            let head=read_head(&connection,&session_id)?;
            let selected=head.cursor.map(|cursor|point(&connection,&session_id,&cursor.checkpoint_id,JournalOptions::default().max_checkpoint_bytes)).transpose()?;
            super::communication::reconcile_inspected(stored,&session_id,&agent_id,selected.as_ref().map(|point|&point.checkpoint.recover)).map(Some)
        }).await.map_err(|_|Error::protocol("SQLite communication inspection worker failed"))?
    }
    pub(super) async fn read_inbox(
        &self,
    ) -> Result<Option<super::communication::StoredInbox>, Error> {
        self.run(|db| {
            let session_id = db.session_id.clone();
            let transaction = db
                .connection
                .transaction_with_behavior(TransactionBehavior::Deferred)
                .map_err(failure)?;
            read_inbox(&transaction, &session_id)
        })
        .await
    }
    pub(super) async fn store_inbox(
        &self,
        inbox: super::communication::StoredInbox,
    ) -> Result<(), Error> {
        self.run(move|db|{
            if !db.writable{return Err(Error::config("SQLite journal is no longer writable"));}
            inbox.validate(&db.session_id,&inbox.agent_id)?;
            let transaction=db.connection.transaction_with_behavior(TransactionBehavior::Immediate).map_err(failure)?;
            let previous=read_inbox(&transaction,&db.session_id)?;
            if !inbox.check_next(previous.as_ref())?{return Ok(());}
            let body=serde_json::to_vec(&inbox)?;
            transaction.execute("INSERT INTO journal_communication(session_id,body) VALUES(?1,?2) ON CONFLICT(session_id) DO UPDATE SET body=excluded.body",params![db.session_id,body]).map_err(failure)?;
            if let Err(error)=transaction.commit(){db.writable=false;return Err(failure(error));}
            Ok(())
        }).await
    }
    /// Inspect the selected immutable node in a read transaction. No schema setup,
    /// writer registration, checkpoint movement, or Session assembly occurs.
    pub async fn inspect_head(
        path: impl AsRef<Path>,
        session_id: String,
    ) -> Result<Option<RestorePoint>, Error> {
        let path = path.as_ref().to_owned();
        tokio::task::spawn_blocking(move || {
            if !path.try_exists()? {
                return Ok(None);
            }
            let connection = Connection::open_with_flags(
                &path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .map_err(failure)?;
            connection
                .execute_batch("PRAGMA query_only=ON; BEGIN DEFERRED;")
                .map_err(failure)?;
            let head = read_head(&connection, &session_id)?;
            let Some(cursor) = head.cursor else {
                return Ok(None);
            };
            let selected = point(
                &connection,
                &session_id,
                &cursor.checkpoint_id,
                JournalOptions::default().max_checkpoint_bytes,
            )?;
            if selected.cursor != cursor {
                return Err(conflict());
            }
            Ok(Some(selected))
        })
        .await
        .map_err(|_| Error::Protocol("SQLite inspection worker failed".into()))?
    }

    /// Verify parents of an already selected head in one read-only transaction.
    pub async fn inspect_ancestry(
        path: impl AsRef<Path>,
        session_id: String,
        head: String,
    ) -> Result<Vec<String>, Error> {
        Self::inspect_metadata(path.as_ref(), session_id, head, BYTE_BUDGET, |_| false)
            .await
            .map(|nodes| nodes.into_iter().map(|node| node.id).collect())
    }

    /// Incrementally verify and persist unindexed ancestry metadata in bounded
    /// `BYTE_BUDGET` transactions so future `inspect_ancestry` calls read zero full bodies.
    pub async fn warm_ancestry(
        path: impl AsRef<Path>,
        session_id: String,
        head: String,
    ) -> Result<usize, Error> {
        let path = path.as_ref().to_owned();
        tokio::task::spawn_blocking(move || {
            valid_hash(&head)?;
            let mut connection = open(&path, "history")?;
            ensure_schema(&connection)?;
            let mut indexed_count = 0usize;
            let mut seen = std::collections::HashSet::new();
            let mut next = Some(head);
            while next.is_some() {
                let tx = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(failure)?;
                let mut remaining = BYTE_BUDGET;
                let mut chunk_verified = Vec::new();
                while let Some(id) = next.clone() {
                    valid_hash(&id)?;
                    if seen.len() >= MAX_NODES {
                        return Err(Error::Limit("journal ancestry node budget exceeded".into()));
                    }
                    if !seen.insert(id.clone()) {
                        return Err(Error::protocol("journal ancestry cycle"));
                    }
                    let size: i64 = tx
                        .query_row(
                            "SELECT length(body) FROM journal_nodes WHERE session_id=?1 AND id=?2",
                            params![&session_id, &id],
                            |row| row.get(0),
                        )
                        .optional()
                        .map_err(failure)?
                        .ok_or_else(missing)?;
                    if let Some(node) = read_indexed_node(&tx, &session_id, &id)? {
                        next = node.parent;
                        continue;
                    }
                    let size = usize::try_from(size).map_err(|_| {
                        Error::Limit("journal ancestry hydration byte budget exceeded".into())
                    })?;
                    if remaining.checked_sub(size).is_none() && !chunk_verified.is_empty() {
                        // Undo inserting `id` into `seen` so the next transaction visits it.
                        seen.remove(&id);
                        break;
                    }
                    remaining = remaining.saturating_sub(size);
                    let bytes: Vec<u8> = tx
                        .query_row(
                            "SELECT body FROM journal_nodes WHERE session_id=?1 AND id=?2",
                            params![&session_id, &id],
                            |row| row.get(0),
                        )
                        .map_err(failure)?;
                    let node = verify_node_metadata(&bytes, &id, &session_id)?;
                    next = node.parent.clone();
                    chunk_verified.push(node);
                }
                for node in &chunk_verified {
                    write_indexed_node(&tx, &session_id, node)?;
                    indexed_count += 1;
                }
                tx.commit().map_err(failure)?;
                std::thread::yield_now();
            }
            Ok(indexed_count)
        })
        .await
        .map_err(|_| Error::protocol("SQLite ancestry warm-up worker failed"))?
    }

    /// Read verified witnesses from the exact selected head back to its root.
    pub async fn inspect_request_heads(
        path: impl AsRef<Path>,
        session_id: String,
        head: String,
    ) -> Result<Vec<(String, Option<String>)>, Error> {
        Self::inspect_metadata(path.as_ref(), session_id, head, BYTE_BUDGET, |_| false)
            .await
            .map(|nodes| {
                nodes
                    .into_iter()
                    .map(|node| (node.id, node.request_head))
                    .collect()
            })
    }

    pub(super) async fn inspect_metadata(
        path: &Path,
        session: String,
        head: String,
        byte_budget: usize,
        stop: impl FnMut(&VerifiedNode) -> bool + Send + 'static,
    ) -> Result<Vec<VerifiedNode>, Error> {
        let path = path.to_owned();
        tokio::task::spawn_blocking(move || {
            let scope = AncestryScope::new(&path, &session)?;
            let slot = ANCESTRY.get_or_init(Default::default).get(scope.clone())?;
            let mut reader = slot.try_lock().map_err(lock_failure)?;
            // The active writer uses its own connection's data_version. Its own
            // inserts and head selections do not invalidate previously verified
            // immutable rows; all other connection commits do.
            let writer = if cfg!(unix) {
                reader.writer.as_ref().and_then(Weak::upgrade)
            } else {
                None
            };
            if let Some(writer) = writer {
                let mut writer = writer.try_lock().map_err(lock_failure)?;
                if let Some(db) = writer.as_mut() {
                    return db
                        .ancestry
                        .read(&scope, &db.connection, head, byte_budget, stop);
                }
            }
            let Reader {
                connection, cache, ..
            } = &mut *reader;
            if connection.is_none() || !cfg!(unix) {
                let read = Connection::open_with_flags(
                    &scope.path,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                        | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
                )
                .map_err(failure)?;
                read.execute_batch("PRAGMA query_only=ON;")
                    .map_err(failure)?;
                *connection = Some(read);
            }
            let connection = connection.as_ref().ok_or_else(cache_failure)?;
            let result = cache.read(&scope, connection, head, byte_budget, stop);
            if !cfg!(unix) {
                reader.connection = None;
            }
            result
        })
        .await
        .map_err(|_| Error::protocol("SQLite ancestry inspection worker failed"))?
    }

    pub(super) async fn request_metadata(
        &self,
        head: JournalCursor,
        stop: impl FnMut(&VerifiedNode) -> bool + Send + 'static,
    ) -> Result<Vec<VerifiedNode>, Error> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = state.try_lock().map_err(lock_failure)?;
            let db = guard.as_mut().ok_or(Error::Closed)?;
            if head.session_id != db.session_id {
                return Err(conflict());
            }
            db.ancestry.read(
                &db.scope,
                &db.connection,
                head.checkpoint_id,
                BYTE_BUDGET,
                stop,
            )
        })
        .await
        .map_err(|_| Error::protocol("SQLite ancestry inspection worker failed"))?
    }

    pub async fn open(path: impl AsRef<Path>, session_id: String) -> Result<Self, Error> {
        Self::open_with_options(path, session_id, JournalOptions::default()).await
    }
    pub async fn open_with_options(
        path: impl AsRef<Path>,
        session_id: String,
        options: JournalOptions,
    ) -> Result<Self, Error> {
        if session_id.trim().is_empty()
            || session_id.len() > 1024
            || options.max_checkpoint_bytes == 0
            || options.max_checkpoint_bytes > 1024 * 1024 * 1024
        {
            return Err(Error::config("invalid SQLite journal configuration"));
        }
        let path = path.as_ref().to_owned();
        let state = tokio::task::spawn_blocking(move || {
            let connection = open(&path, "history")?;
            ensure_schema(&connection)?;
            let scope = AncestryScope::new(&path, &session_id)?;
            let db = Database {
                connection,
                scope,
                ancestry: SnapshotCache::default(),
                session_id,
                options,
                writable: true,
            };
            let head = read_head(&db.connection, &db.session_id)?;
            if let Some(cursor) = head.cursor
                && point(
                    &db.connection,
                    &db.session_id,
                    &cursor.checkpoint_id,
                    options.max_checkpoint_bytes,
                )?
                .cursor
                    != cursor
            {
                return Err(conflict());
            }
            let scope = db.scope.clone();
            let state = Arc::new(Mutex::new(Some(db)));
            if let Ok(slot) = ANCESTRY.get_or_init(Default::default).get(scope)
                && let Ok(mut reader) = slot.try_lock()
            {
                reader.writer = Some(Arc::downgrade(&state));
            }
            Ok::<_, Error>(state)
        })
        .await
        .map_err(|error| {
            Error::protocol_with_private_reason(
                "SQLite open worker failed",
                format!("SQLite open worker: {error}"),
            )
        })??;
        Ok(Self { state })
    }
    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Database) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let mut db = state
                .lock()
                .map_err(|_| Error::Protocol("SQLite journal mutex poisoned".into()))?;
            operation(db.as_mut().ok_or(Error::Closed)?)
        })
        .await
        .map_err(|error| {
            Error::protocol_with_private_reason(
                "SQLite journal worker failed",
                format!("SQLite journal worker: {error}"),
            )
        })?
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct AncestryScope {
    path: PathBuf,
    session: String,
    #[cfg(unix)]
    identity: (u64, u64),
}

impl AncestryScope {
    fn new(path: &Path, session: &str) -> Result<Self, Error> {
        let path = path.canonicalize()?;
        #[cfg(unix)]
        let identity = {
            use std::os::unix::fs::MetadataExt;
            let metadata = path.metadata()?;
            (metadata.dev(), metadata.ino())
        };
        Ok(Self {
            path,
            session: session.to_owned(),
            #[cfg(unix)]
            identity,
        })
    }
}

#[derive(Default)]
struct Reader {
    connection: Option<Connection>,
    cache: SnapshotCache,
    writer: Option<Weak<Mutex<Option<Database>>>>,
}

static ANCESTRY: OnceLock<Scopes<AncestryScope, Reader>> = OnceLock::new();

fn lock_failure<T>(error: TryLockError<T>) -> Error {
    match error {
        TryLockError::WouldBlock => Error::Busy,
        TryLockError::Poisoned(_) => cache_failure(),
    }
}

#[derive(Default)]
struct SnapshotCache {
    version: Option<i64>,
    nodes: Cache<()>,
}

fn data_version(connection: &Connection) -> Result<i64, Error> {
    connection
        .query_row("PRAGMA main.data_version", [], |row| row.get(0))
        .map_err(failure)
}

fn ensure_schema(connection: &Connection) -> Result<(), Error> {
    connection
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS journal_heads(
                session_id TEXT PRIMARY KEY, cursor BLOB, redo BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS journal_nodes(
                session_id TEXT NOT NULL, id TEXT NOT NULL, body BLOB NOT NULL,
                PRIMARY KEY(session_id,id));
            CREATE TABLE IF NOT EXISTS journal_blobs(
                session_id TEXT NOT NULL, id TEXT NOT NULL, body BLOB NOT NULL,
                PRIMARY KEY(session_id,id));
            CREATE TABLE IF NOT EXISTS journal_ancestry_index(
                session_id TEXT NOT NULL, id TEXT NOT NULL, parent TEXT,
                epoch INTEGER NOT NULL, request_head TEXT,
                PRIMARY KEY(session_id,id));
            CREATE TRIGGER IF NOT EXISTS journal_nodes_au_index AFTER UPDATE ON journal_nodes
            BEGIN
                DELETE FROM journal_ancestry_index WHERE session_id=OLD.session_id AND id=OLD.id;
            END;
            CREATE TRIGGER IF NOT EXISTS journal_nodes_ad_index AFTER DELETE ON journal_nodes
            BEGIN
                DELETE FROM journal_ancestry_index WHERE session_id=OLD.session_id AND id=OLD.id;
            END;
            CREATE TABLE IF NOT EXISTS journal_communication(session_id TEXT PRIMARY KEY,body BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS journal_file_versions(session_id TEXT PRIMARY KEY,body BLOB NOT NULL);",
        )
        .map_err(failure)
}

fn has_ancestry_index_table(connection: &Connection) -> Result<bool, Error> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='journal_ancestry_index')",
            [],
            |row| row.get(0),
        )
        .map_err(failure)
}

fn read_indexed_node(
    connection: &Connection,
    session_id: &str,
    id: &str,
) -> Result<Option<VerifiedNode>, Error> {
    let row: Option<(Option<String>, i64, Option<String>)> = connection
        .query_row(
            "SELECT parent, epoch, request_head FROM journal_ancestry_index WHERE session_id=?1 AND id=?2",
            params![session_id, id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(failure)?;
    let Some((parent, epoch, request_head)) = row else {
        return Ok(None);
    };
    if let Some(p) = &parent {
        valid_hash(p)?;
    }
    if let Some(r) = &request_head {
        valid_hash(r)?;
    }
    let epoch = u64::try_from(epoch).map_err(|_| Error::Protocol("invalid indexed epoch".into()))?;
    Ok(Some(VerifiedNode {
        id: id.to_owned(),
        parent,
        epoch,
        request_head,
    }))
}

fn write_indexed_node(
    connection: &Connection,
    session_id: &str,
    node: &VerifiedNode,
) -> Result<(), Error> {
    let epoch = i64::try_from(node.epoch)
        .map_err(|_| Error::Protocol("epoch exceeds SQLite integer range".into()))?;
    connection
        .execute(
            "INSERT OR REPLACE INTO journal_ancestry_index(session_id,id,parent,epoch,request_head) VALUES(?1,?2,?3,?4,?5)",
            params![session_id, node.id, node.parent, epoch, node.request_head],
        )
        .map_err(failure)?;
    Ok(())
}

fn load_blob(
    connection: &Connection,
    session_id: &str,
    id: &str,
    limit: usize,
) -> Result<Vec<u8>, Error> {
    valid_hash(id)?;
    connection
        .query_row(
            "SELECT body FROM journal_blobs WHERE session_id=?1 AND id=?2 AND length(body)<=?3",
            params![session_id, id, limit as i64],
            |row| row.get(0),
        )
        .optional()
        .map_err(failure)?
        .ok_or_else(missing)
}

impl SnapshotCache {
    fn read(
        &mut self,
        scope: &AncestryScope,
        connection: &Connection,
        head: String,
        byte_budget: usize,
        stop: impl FnMut(&VerifiedNode) -> bool,
    ) -> Result<Vec<VerifiedNode>, Error> {
        if AncestryScope::new(&scope.path, &scope.session)? != *scope {
            self.nodes.clear();
            return Err(conflict());
        }
        let transaction = connection.unchecked_transaction().map_err(failure)?;
        // This must be the first read: SQLite executes data_version in a read
        // transaction, so its token and the rows below name the same snapshot.
        // A different connection can commit during this read; it will advance
        // the token on the next snapshot without invalidating this observation.
        let version = data_version(&transaction)?;
        if self.version != Some(version) {
            self.nodes.clear();
            self.version = Some(version);
        }
        let has_index = has_ancestry_index_table(&transaction)?;
        let mut size_statement = transaction
            .prepare_cached("SELECT length(body) FROM journal_nodes WHERE session_id=?1 AND id=?2")
            .map_err(failure)?;
        let result = self.nodes.walk(
            head,
            byte_budget,
            |id| {
                let size: i64 = size_statement
                    .query_row(params![scope.session, id], |row| row.get(0))
                    .optional()
                    .map_err(failure)?
                    .ok_or_else(missing)?;
                if has_index && read_indexed_node(&transaction, &scope.session, id)?.is_some() {
                    return Ok((cfg!(unix).then_some(()), 0));
                }
                let size = usize::try_from(size).map_err(|_| {
                    Error::Limit("journal ancestry hydration byte budget exceeded".into())
                })?;
                // On platforms without a filesystem identity witness, do not
                // carry any metadata across inspections.
                Ok((cfg!(unix).then_some(()), size))
            },
            |id, size, _| {
                if has_index && let Some(node) = read_indexed_node(&transaction, &scope.session, id)? {
                    return Ok(node);
                }
                let bytes: Vec<u8> = transaction
                    .query_row(
                        "SELECT body FROM journal_nodes WHERE session_id=?1 AND id=?2 AND length(body)<=?3",
                        params![scope.session, id, size as i64],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(failure)?
                    .ok_or_else(missing)?;
                verify_node_metadata(&bytes, id, &scope.session)
            },
            stop,
        );
        drop(size_statement);
        transaction.rollback().map_err(failure)?;
        if AncestryScope::new(&scope.path, &scope.session)? != *scope {
            self.nodes.clear();
            self.version = None;
            return Err(conflict());
        }
        result
    }
}

fn read_head(connection: &Connection, session_id: &str) -> Result<Head, Error> {
    // Metadata is bounded before allocating SQLite blobs into Rust.
    let data:Option<(Option<Vec<u8>>,Vec<u8>)> = connection.query_row(
        "SELECT cursor,redo FROM journal_heads WHERE session_id=?1 AND (cursor IS NULL OR length(cursor)<=16384) AND length(redo)<=1048576",
        [session_id], |row| Ok((row.get(0)?,row.get(1)?)),
    ).optional().map_err(failure)?;
    match data {
        Some((cursor, redo)) => {
            let cursor: Option<JournalCursor> = cursor
                .map(|bytes| serde_json::from_slice(&bytes))
                .transpose()?;
            let redo: Vec<String> = serde_json::from_slice(&redo)?;
            if let Some(cursor) = &cursor {
                valid_hash(&cursor.checkpoint_id)?;
                if cursor.session_id != session_id {
                    return Err(conflict());
                }
            }
            for id in &redo {
                valid_hash(id)?;
            }
            if cursor.is_none() && !redo.is_empty() {
                return Err(conflict());
            }
            Ok(Head { cursor, redo })
        }
        None => {
            let exists: bool = connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM journal_heads WHERE session_id=?1)",
                    [session_id],
                    |row| row.get(0),
                )
                .map_err(failure)?;
            if exists {
                Err(Error::Protocol(
                    "SQLite journal metadata exceeds byte budget".into(),
                ))
            } else {
                Ok(Head::default())
            }
        }
    }
}
fn point(
    connection: &Connection,
    session_id: &str,
    id: &str,
    limit: usize,
) -> Result<RestorePoint, Error> {
    valid_hash(id)?;
    let bytes: Vec<u8> = connection
        .query_row(
            "SELECT body FROM journal_nodes WHERE session_id=?1 AND id=?2 AND length(body)<=?3",
            params![session_id, id, limit as i64],
            |row| row.get(0),
        )
        .optional()
        .map_err(failure)?
        .ok_or_else(missing)?;
    decode_restore_point(&bytes, id, session_id, |blob_id| {
        load_blob(connection, session_id, blob_id, limit)
    })
}
fn target(
    connection: &Connection,
    session_id: &str,
    head: &Head,
    via: &Travel,
    limit: usize,
) -> Result<String, Error> {
    match via {
        Travel::Undo => {
            let cursor = head.cursor.as_ref().ok_or_else(missing)?;
            let bytes: Vec<u8> = connection
                .query_row(
                    "SELECT body FROM journal_nodes WHERE session_id=?1 AND id=?2 AND length(body)<=?3",
                    params![session_id, cursor.checkpoint_id, limit as i64],
                    |row| row.get(0),
                )
                .map_err(failure)?;
            let node = verify_node_metadata(&bytes, &cursor.checkpoint_id, session_id)?;
            node.parent.ok_or_else(missing)
        }
        Travel::Redo => head.redo.last().cloned().ok_or_else(missing),
        Travel::To { checkpoint_id } | Travel::UndoTo { checkpoint_id } => {
            point(connection, session_id, checkpoint_id, limit)?;
            Ok(checkpoint_id.clone())
        }
    }
}
fn write_head(connection: &Connection, session_id: &str, head: &Head) -> Result<(), Error> {
    let cursor = head.cursor.as_ref().map(serde_json::to_vec).transpose()?;
    let redo = serde_json::to_vec(&head.redo)?;
    if redo.len() > 1048576 {
        return Err(Error::Protocol(
            "SQLite redo metadata exceeds byte budget".into(),
        ));
    }
    connection
        .execute(
            "INSERT INTO journal_heads(session_id,cursor,redo) VALUES(?1,?2,?3)
        ON CONFLICT(session_id) DO UPDATE SET cursor=excluded.cursor,redo=excluded.redo",
            params![session_id, cursor, redo],
        )
        .map_err(failure)?;
    Ok(())
}
impl History for SqliteJournal {
    async fn close(&self) -> Result<(), Error> {
        // Closing twice is harmless; later journal calls reject admission.
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            drop(state.lock().map_err(|_| conflict())?.take());
            Ok(())
        })
        .await
        .map_err(|_| conflict())?
    }
    async fn commit(
        &self,
        expected: Option<JournalCursor>,
        checkpoint: CreatorEnvelope,
    ) -> Result<JournalCursor, Error> {
        self.run(move |db| {
            if !db.writable {
                return Err(Error::Protocol(
                    "SQLite journal quarantined after publication failure".into(),
                ));
            }
            checkpoint.validate()?;
            if checkpoint.session_id != db.session_id {
                return Err(conflict());
            }
            let transaction = db
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(failure)?;
            let head = read_head(&transaction, &db.session_id)?;
            if head.cursor != expected {
                return Err(conflict());
            }
            if let Some(cursor) = &head.cursor {
                let old = point(
                    &transaction,
                    &db.session_id,
                    &cursor.checkpoint_id,
                    db.options.max_checkpoint_bytes,
                )?;
                if canonical_bytes(&old.checkpoint)? == canonical_bytes(&checkpoint)? {
                    return Ok(cursor.clone());
                }
            }
            let parent = expected.map(|cursor| cursor.checkpoint_id);
            let (node, blobs) = split_v5(parent.clone(), &checkpoint)?;
            let bytes = canonical_bytes(&node)?;
            if bytes.len() > db.options.max_checkpoint_bytes {
                return Err(Error::Protocol(
                    "SQLite checkpoint exceeds byte budget".into(),
                ));
            }
            for (blob_id, blob_bytes) in &blobs {
                if blob_bytes.len() > db.options.max_checkpoint_bytes {
                    return Err(Error::Protocol(
                        "SQLite checkpoint exceeds byte budget".into(),
                    ));
                }
                transaction
                    .execute(
                        "INSERT OR IGNORE INTO journal_blobs(session_id,id,body) VALUES(?1,?2,?3)",
                        params![db.session_id, blob_id, blob_bytes],
                    )
                    .map_err(failure)?;
            }
            let id = node.id()?;
            let cursor = node.cursor(id.clone());
            transaction
                .execute(
                    "INSERT OR IGNORE INTO journal_nodes(session_id,id,body) VALUES(?1,?2,?3)",
                    params![db.session_id, id, bytes],
                )
                .map_err(failure)?;
            let existing = point(
                &transaction,
                &db.session_id,
                &id,
                db.options.max_checkpoint_bytes,
            )?;
            if canonical_bytes(&existing.checkpoint)? != canonical_bytes(&checkpoint)? {
                return Err(conflict());
            }
            write_indexed_node(
                &transaction,
                &db.session_id,
                &VerifiedNode {
                    id: id.clone(),
                    parent,
                    epoch: cursor.epoch,
                    request_head: checkpoint.request_head.clone(),
                },
            )?;
            write_head(
                &transaction,
                &db.session_id,
                &Head {
                    cursor: Some(cursor.clone()),
                    redo: vec![],
                },
            )?;
            if let Err(error) = transaction.commit() {
                db.writable = false;
                return Err(failure(error));
            }
            Ok(cursor)
        })
        .await
    }
    async fn restore(&self, target: RestoreTarget) -> Result<RestorePoint, Error> {
        self.run(move |db| {
            let id = match target {
                RestoreTarget::Head => {
                    read_head(&db.connection, &db.session_id)?
                        .cursor
                        .ok_or_else(missing)?
                        .checkpoint_id
                }
                RestoreTarget::Checkpoint { checkpoint_id } => checkpoint_id,
            };
            point(
                &db.connection,
                &db.session_id,
                &id,
                db.options.max_checkpoint_bytes,
            )
        })
        .await
    }
    async fn travel(&self, expected: JournalCursor, via: Travel) -> Result<RestorePoint, Error> {
        self.run(move |db| {
            let head = read_head(&db.connection, &db.session_id)?;
            if head.cursor != Some(expected) {
                return Err(conflict());
            }
            let id = target(
                &db.connection,
                &db.session_id,
                &head,
                &via,
                db.options.max_checkpoint_bytes,
            )?;
            point(
                &db.connection,
                &db.session_id,
                &id,
                db.options.max_checkpoint_bytes,
            )
        })
        .await
    }
    async fn move_head(
        &self,
        expected: JournalCursor,
        wanted: JournalCursor,
        via: Travel,
    ) -> Result<JournalCursor, Error> {
        self.run(move |db| {
            if !db.writable {
                return Err(Error::Protocol(
                    "SQLite journal quarantined after publication failure".into(),
                ));
            }
            let transaction = db
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(failure)?;
            let mut head = read_head(&transaction, &db.session_id)?;
            if head.cursor != Some(expected.clone()) {
                return Err(conflict());
            }
            let id = target(
                &transaction,
                &db.session_id,
                &head,
                &via,
                db.options.max_checkpoint_bytes,
            )?;
            if point(
                &transaction,
                &db.session_id,
                &id,
                db.options.max_checkpoint_bytes,
            )?
            .cursor
                != wanted
            {
                return Err(conflict());
            }
            match via {
                Travel::Undo | Travel::UndoTo { .. } => head.redo.push(expected.checkpoint_id),
                Travel::Redo => {
                    head.redo.pop();
                }
                Travel::To { .. } => head.redo.clear(),
            }
            head.cursor = Some(wanted.clone());
            write_head(&transaction, &db.session_id, &head)?;
            if let Err(error) = transaction.commit() {
                db.writable = false;
                return Err(failure(error));
            }
            Ok(wanted)
        })
        .await
    }
}
fn read_inbox(
    connection: &Connection,
    session_id: &str,
) -> Result<Option<super::communication::StoredInbox>, Error> {
    let length: Option<i64> = connection
        .query_row(
            "SELECT length(body) FROM journal_communication WHERE session_id=?1",
            [session_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(failure)?;
    let Some(length) = length else {
        return Ok(None);
    };
    if length < 0 || length as u64 > super::communication::MAX_INBOX_STORAGE_BYTES as u64 {
        return Err(Error::Limit(
            "communication sidecar byte budget exceeded".into(),
        ));
    }
    let body: Vec<u8> = connection
        .query_row(
            "SELECT body FROM journal_communication WHERE session_id=?1",
            [session_id],
            |row| row.get(0),
        )
        .map_err(failure)?;
    Ok(Some(serde_json::from_slice(&body)?))
}
