//! SQLite adapter for the same v4 immutable nodes and expected-cursor contract.
//! Blocking workers retain their connection lock through publication after async
//! waiter cancellation. Host restoration still precedes coordinator move_head.
use super::{
    CreatorEnvelope, History, HistoryNode, JournalCursor, JournalOptions, RestorePoint,
    RestoreTarget, Travel, canonical_bytes, valid_hash,
};
use crate::sqlite::{failure, open};
use microcrab_session::Error;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::{
    path::Path,
    sync::{Arc, Mutex},
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

impl SqliteJournal {
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
    /// Only identifiers survive each iteration; checkpoint bytes are bounded in total.
    pub async fn inspect_ancestry(
        path: impl AsRef<Path>,
        session_id: String,
        head: String,
    ) -> Result<Vec<String>, Error> {
        let path = path.as_ref().to_owned();
        tokio::task::spawn_blocking(move || {
            let connection = Connection::open_with_flags(
                &path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .map_err(failure)?;
            connection
                .execute_batch("PRAGMA query_only=ON; BEGIN DEFERRED;")
                .map_err(failure)?;
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
                let size: i64 = connection
                    .query_row(
                        "SELECT length(body) FROM journal_nodes WHERE session_id=?1 AND id=?2",
                        params![session_id, id],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(failure)?
                    .ok_or_else(missing)?;
                let size = usize::try_from(size).map_err(|_| {
                    Error::Limit("audit journal ancestry byte budget exceeded".into())
                })?;
                remaining = remaining.checked_sub(size).ok_or_else(|| {
                    Error::Limit("audit journal ancestry byte budget exceeded".into())
                })?;
                let selected = point(&connection, &session_id, &id, size)?;
                next = selected.parent;
                ancestry.push(id);
            }
            Ok(ancestry)
        })
        .await
        .map_err(|_| Error::Protocol("SQLite ancestry inspection worker failed".into()))?
    }

    /// Read all request-head witnesses on one verified immutable parent chain.
    /// No writer/schema/head mutation and no audit body/count retention budgets.
    pub async fn inspect_request_heads(
        path: impl AsRef<Path>,
        session_id: String,
        head: String,
    ) -> Result<Vec<(String, Option<String>)>, Error> {
        let path = path.as_ref().to_owned();
        tokio::task::spawn_blocking(move || {
            let connection = Connection::open_with_flags(
                &path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .map_err(failure)?;
            connection
                .busy_timeout(std::time::Duration::from_secs(5))
                .map_err(failure)?;
            connection
                .execute_batch("PRAGMA query_only=ON; BEGIN DEFERRED;")
                .map_err(failure)?;
            let mut next = Some(head);
            let mut seen = std::collections::HashSet::new();
            let mut witnesses = Vec::new();
            while let Some(id) = next {
                valid_hash(&id)?;
                if !seen.insert(id.clone()) {
                    return Err(Error::Protocol("journal ancestry cycle".into()));
                }
                let size: i64 = connection
                    .query_row(
                        "SELECT length(body) FROM journal_nodes WHERE session_id=?1 AND id=?2",
                        params![session_id, id],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(failure)?
                    .ok_or_else(missing)?;
                let size = usize::try_from(size).map_err(|_| {
                    Error::Protocol("journal node length cannot be represented".into())
                })?;
                let selected = point(&connection, &session_id, &id, size)?;
                next = selected.parent;
                witnesses.push((id, selected.checkpoint.request_head));
            }
            Ok(witnesses)
        })
        .await
        .map_err(|_| Error::Protocol("SQLite request-head inspection worker failed".into()))?
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
            connection
                .execute_batch(
                    "CREATE TABLE IF NOT EXISTS journal_heads(
                session_id TEXT PRIMARY KEY, cursor BLOB, redo BLOB NOT NULL);
                CREATE TABLE IF NOT EXISTS journal_nodes(
                session_id TEXT NOT NULL, id TEXT NOT NULL, body BLOB NOT NULL,
                PRIMARY KEY(session_id,id));",
                )
                .map_err(failure)?;
            let db = Database {
                connection,
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
            Ok::<_, Error>(db)
        })
        .await
        .map_err(|error| {
            Error::protocol_with_private_reason(
                "SQLite open worker failed",
                format!("SQLite open worker: {error}"),
            )
        })??;
        Ok(Self {
            state: Arc::new(Mutex::new(Some(state))),
        })
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
    let node: HistoryNode = serde_json::from_slice(&bytes)?;
    node.validate(id)?;
    if node.checkpoint.session_id != session_id {
        return Err(conflict());
    }
    Ok(RestorePoint {
        cursor: node.cursor(id.into()),
        parent: node.parent,
        checkpoint: node.checkpoint,
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
            let bytes:Vec<u8>=connection.query_row("SELECT body FROM journal_nodes WHERE session_id=?1 AND id=?2 AND length(body)<=?3",params![session_id,cursor.checkpoint_id,limit as i64],|row|row.get(0)).map_err(failure)?;
            let node: HistoryNode = serde_json::from_slice(&bytes)?;
            node.validate(&cursor.checkpoint_id)?;
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
            let node = HistoryNode::new(expected.map(|cursor| cursor.checkpoint_id), checkpoint);
            let bytes = canonical_bytes(&node)?;
            if bytes.len() > db.options.max_checkpoint_bytes {
                return Err(Error::Protocol(
                    "SQLite checkpoint exceeds byte budget".into(),
                ));
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
            if canonical_bytes(&existing.checkpoint)? != canonical_bytes(&node.checkpoint)? {
                return Err(conflict());
            }
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
