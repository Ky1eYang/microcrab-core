//! SQLite stores canonical bytes, not Creator types. Each append advances the
//! expected committed head in the same FULL-synchronous transaction as its row.
use super::{AuditScope, AuditStore};
use crate::sqlite::{failure, open};
use microcrab_session::Error;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::{
    collections::HashSet,
    io::{Cursor, Read},
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
};

static WRITERS: OnceLock<Mutex<HashSet<(PathBuf, AuditScope)>>> = OnceLock::new();
struct Lease {
    key: (PathBuf, AuditScope),
}
impl Lease {
    fn acquire(path: &Path, scope: &AuditScope) -> Result<Self, Error> {
        let key = (path.canonicalize()?, scope.clone());
        if !WRITERS
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key.clone())
        {
            return Err(Error::audit("audit already has an exclusive writer"));
        }
        Ok(Self { key })
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        WRITERS
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.key);
    }
}
pub struct SqliteAuditStore {
    connection: Connection,
    scope: AuditScope,
    _lease: Lease,
}
fn schema(connection: &Connection) -> Result<(), Error> {
    connection.execute_batch("CREATE TABLE IF NOT EXISTS audit_streams(
        owner TEXT NOT NULL, root_session TEXT NOT NULL, session TEXT NOT NULL,
        sequence INTEGER NOT NULL DEFAULT 0, hash TEXT, format TEXT,
        PRIMARY KEY(owner,root_session,session), UNIQUE(session));
        CREATE TABLE IF NOT EXISTS audit_records(
        owner TEXT NOT NULL, root_session TEXT NOT NULL, session TEXT NOT NULL,
        sequence INTEGER NOT NULL, hash TEXT NOT NULL, kind TEXT NOT NULL,
        epoch INTEGER, turn INTEGER, round INTEGER, call_id TEXT, request_hash TEXT,
        record BLOB NOT NULL, PRIMARY KEY(owner,root_session,session,sequence),
        FOREIGN KEY(owner,root_session,session) REFERENCES audit_streams(owner,root_session,session) ON DELETE CASCADE);
        CREATE INDEX IF NOT EXISTS audit_request ON audit_records(owner,root_session,session,request_hash);
        CREATE INDEX IF NOT EXISTS audit_position ON audit_records(owner,root_session,session,epoch,turn,round);")
        .map_err(failure)
}
fn read_connection(path: &Path) -> Result<Option<Connection>, Error> {
    if !path.try_exists()? {
        return Ok(None);
    }
    let connection = Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(failure)?;
    connection
        .busy_timeout(std::time::Duration::from_secs(5))
        .map_err(failure)?;
    connection
        .execute_batch("PRAGMA query_only=ON; BEGIN DEFERRED;")
        .map_err(failure)?;
    let exists: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='audit_streams')", [], |row| row.get(0)).map_err(failure)?;
    Ok(exists.then_some(connection))
}
/// Own one read transaction and one record buffer. Reopening a writer must not
/// allocate its complete history or inherit the smaller public query scan cap.
struct SnapshotReader {
    connection: Connection,
    scope: AuditScope,
    head_sequence: u64,
    head_hash: Option<String>,
    sequence: u64,
    record: Cursor<Vec<u8>>,
}
impl Read for SnapshotReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        let copied = self.record.read(buffer)?;
        if copied != 0 {
            return Ok(copied);
        }
        if self.sequence == self.head_sequence {
            return Ok(0);
        }
        let next = self.sequence + 1;
        let loaded = self.connection.query_row(
            "SELECT length(record),hash,record FROM audit_records WHERE owner=?1 AND root_session=?2 AND session=?3 AND sequence=?4",
            params![self.scope.owner,self.scope.root_session,self.scope.session,next],
            |row| {
                let size: u64 = row.get(0)?;
                // Check length before copying potentially hostile durable bytes.
                if size > 64 * 1024 * 1024 { return Err(rusqlite::Error::InvalidQuery); }
                Ok((row.get::<_,String>(1)?,row.get::<_,Vec<u8>>(2)?))
            },
        ).optional().map_err(|_| std::io::Error::other("SQLite audit snapshot read failed"))?;
        let (hash, record) =
            loaded.ok_or_else(|| std::io::Error::other("audit sequence mismatch"))?;
        if !record.ends_with(b"\n") || (next == self.head_sequence && Some(hash) != self.head_hash)
        {
            return Err(std::io::Error::other(
                "audit committed record/head mismatch",
            ));
        }
        self.sequence = next;
        self.record = Cursor::new(record);
        self.record.read(buffer)
    }
}
fn snapshot(
    connection: Connection,
    scope: &AuditScope,
    bounded: bool,
) -> Result<Option<Box<dyn Read + Send>>, Error> {
    let head: Option<(u64, Option<String>)> = connection.query_row("SELECT sequence,hash FROM audit_streams WHERE owner=?1 AND root_session=?2 AND session=?3", params![scope.owner,scope.root_session,scope.session], |row| Ok((row.get(0)?, row.get(1)?))).optional().map_err(failure)?;
    let Some((sequence, hash)) = head else {
        return Ok(None);
    };
    let (count,total): (u64,u64) = connection.query_row("SELECT count(*),coalesce(sum(length(record)),0) FROM audit_records WHERE owner=?1 AND root_session=?2 AND session=?3", params![scope.owner,scope.root_session,scope.session], |row| Ok((row.get(0)?,row.get(1)?))).map_err(failure)?;
    if bounded && total > 256 * 1024 * 1024 {
        return Err(Error::Limit("audit query scan budget exceeded".into()));
    }
    if count != sequence || (sequence == 0 && hash.is_some()) {
        return Err(Error::audit("audit committed head mismatch"));
    }
    Ok(Some(Box::new(SnapshotReader {
        connection,
        scope: scope.clone(),
        head_sequence: sequence,
        head_hash: hash,
        sequence: 0,
        record: Cursor::new(Vec::new()),
    })))
}

impl SqliteAuditStore {
    pub fn open(path: &Path, scope: AuditScope) -> Result<Self, Error> {
        scope.validate()?;
        let connection = open(path, "audit")?;
        schema(&connection)?;
        let lease = Lease::acquire(path, &scope)?;
        connection
            .execute(
                "INSERT OR IGNORE INTO audit_streams(owner,root_session,session) VALUES(?1,?2,?3)",
                params![scope.owner, scope.root_session, scope.session],
            )
            .map_err(failure)?;
        let owned: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM audit_streams WHERE owner=?1 AND root_session=?2 AND session=?3)",params![scope.owner,scope.root_session,scope.session], |row| row.get(0)).map_err(failure)?;
        if !owned {
            return Err(Error::audit("audit stream scope mismatch"));
        }
        Ok(Self {
            connection,
            scope,
            _lease: lease,
        })
    }
    pub fn revision(path: &Path, scope: &AuditScope) -> Result<Option<u64>, Error> {
        scope.validate()?;
        let Some(connection) = read_connection(path)? else {
            return Ok(None);
        };
        connection.query_row("SELECT sequence FROM audit_streams WHERE owner=?1 AND root_session=?2 AND session=?3", params![scope.owner,scope.root_session,scope.session], |row| row.get(0)).optional().map_err(failure)
    }
    pub fn inspect(path: &Path, scope: &AuditScope) -> Result<Option<Box<dyn Read + Send>>, Error> {
        scope.validate()?;
        let Some(connection) = read_connection(path)? else {
            return Ok(None);
        };
        snapshot(connection, scope, true)
    }
    /// Purge only the authorized root's registered descendants, including child
    /// Journals that have no physical directory in SQLite mode. Owners must close
    /// their writers first. Archive never calls this operation.
    pub fn purge_root(path: &Path, owner: &str, root: &str) -> Result<(), Error> {
        if !path.try_exists()? {
            return Ok(());
        }
        let mut connection = open(path, "audit")?;
        schema(&connection)?;
        let key_path = path.canonicalize()?;
        // Hold admission through commit so a same-process opener cannot acquire
        // the stream between the live-owner check and row deletion.
        let writers = WRITERS
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if writers.iter().any(|(path, scope)| {
            *path == key_path && scope.owner == owner && scope.root_session == root
        }) {
            return Err(Error::audit("audit scope still has a live writer"));
        }
        // Root deletion also owns display and private reply rows. Hold
        // admission through commit, in audit -> display -> reply order.
        let _display_writers =
            crate::display::DisplaySource::fence_sqlite_root_purge(&key_path, owner, root)?;
        let mut reply_writers =
            crate::dialogue_reply::RootPurgeGuard::acquire(&key_path, owner, root)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(failure)?;
        reply_writers.delete(&tx, owner, root)?;
        let has_history: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='journal_heads')",
                [],
                |row| row.get(0),
            )
            .map_err(failure)?;
        if has_history {
            for table in ["journal_nodes", "journal_heads"] {
                tx.execute(&format!("DELETE FROM {table} WHERE session_id=?3 OR session_id IN (SELECT session FROM audit_streams WHERE owner=?1 AND root_session=?2)"), params![owner,root,root]).map_err(failure)?;
            }
        }
        let has_usage: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='usage_records')",
                [],
                |row| row.get(0),
            )
            .map_err(failure)?;
        if has_usage {
            tx.execute(
                "DELETE FROM usage_records WHERE owner=?1 AND root_session=?2",
                params![owner, root],
            )
            .map_err(failure)?;
        }
        // Display storage may not exist in an older database. Do not create its
        // schema during deletion; remove only this owner's exact root scope.
        for table in ["display_records", "display_streams"] {
            let exists: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
                    [table],
                    |row| row.get(0),
                )
                .map_err(failure)?;
            if exists {
                tx.execute(
                    &format!("DELETE FROM {table} WHERE owner=?1 AND root_session=?2"),
                    params![owner, root],
                )
                .map_err(failure)?;
            }
        }
        tx.execute(
            "DELETE FROM audit_streams WHERE owner=?1 AND root_session=?2",
            params![owner, root],
        )
        .map_err(failure)?;
        tx.commit().map_err(failure)?;
        reply_writers.acknowledge_purge(&key_path, owner, root);
        Ok(())
    }
}
impl AuditStore for SqliteAuditStore {
    fn reader(&mut self) -> Result<Box<dyn Read + Send>, Error> {
        let connection = read_connection(&self._lease.key.0)?
            .ok_or_else(|| Error::audit("audit database disappeared"))?;
        snapshot(connection, &self.scope, false)?
            .ok_or_else(|| Error::audit("audit stream disappeared"))
    }
    fn append(
        &mut self,
        expected_sequence: u64,
        expected_hash: Option<&str>,
        sequence: u64,
        hash: &str,
        record: &[u8],
    ) -> Result<(), Error> {
        if expected_sequence.checked_add(1) != Some(sequence) || sequence > i64::MAX as u64 {
            return Err(Error::audit("audit sequence overflow"));
        }
        let row: serde_json::Value = serde_json::from_slice(record)?;
        let data = &row["data"];
        let metadata = if data["kind"] == "effective" {
            data
        } else {
            data["record"].get("body").unwrap_or(&data["record"])
        };
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(failure)?;
        let changed = tx.execute("UPDATE audit_streams SET sequence=?4,hash=?5,format=coalesce(format,?8) WHERE owner=?1 AND root_session=?2 AND session=?3 AND sequence=?6 AND hash IS ?7", params![self.scope.owner,self.scope.root_session,self.scope.session,sequence,hash,expected_sequence,expected_hash,data["format"].as_str()]).map_err(failure)?;
        if changed != 1 {
            return Err(Error::audit("audit committed head conflict"));
        }
        tx.execute("INSERT INTO audit_records(owner,root_session,session,sequence,hash,kind,epoch,turn,round,call_id,request_hash,record) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)", params![self.scope.owner,self.scope.root_session,self.scope.session,sequence,hash,data["kind"].as_str().unwrap_or(""),metadata["epoch"].as_i64(),metadata["turn"].as_i64().or_else(||metadata["turn_id"].as_i64()),metadata["round"].as_i64(),metadata["call_id"].as_str(),data["request_hash"].as_str(),record]).map_err(failure)?;
        tx.commit().map_err(failure)
    }
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
