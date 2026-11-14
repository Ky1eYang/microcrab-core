//! FULL-synchronous, scope-isolated immutable records and transactional head CAS.
use super::*;
use crate::sqlite::{failure as sql_failure, open};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::{
    collections::HashSet,
    path::Path,
    sync::{Mutex, OnceLock},
};
static WRITERS: OnceLock<Mutex<HashSet<(PathBuf, DisplayScope)>>> = OnceLock::new();
/// Retain display admission until the caller commits its root purge. Audit takes
/// its own writer lock first; display open/drop never acquire the audit lock.
pub(crate) struct RootPurgeGuard {
    _writers: std::sync::MutexGuard<'static, HashSet<(PathBuf, DisplayScope)>>,
}
impl DisplaySource {
    pub(crate) fn fence_sqlite_root_purge(
        canonical_path: &Path,
        owner: &str,
        root: &str,
    ) -> Result<RootPurgeGuard, Error> {
        let writers = WRITERS
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if writers.iter().any(|(path, scope)| {
            path == canonical_path && scope.owner == owner && scope.root_session == root
        }) {
            return Err(failure("display scope still has a live writer"));
        }
        Ok(RootPurgeGuard { _writers: writers })
    }
}
struct Lease((PathBuf, DisplayScope));
impl Lease {
    fn acquire(path: &Path, scope: &DisplayScope) -> Result<Self, Error> {
        let key = (path.canonicalize()?, scope.clone());
        if !WRITERS
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key.clone())
        {
            return Err(failure("display archive already has an exclusive writer"));
        }
        Ok(Self(key))
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        WRITERS
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.0);
    }
}
pub(super) struct Store {
    connection: Connection,
    scope: DisplayScope,
    graph: DisplayGraph,
    _lease: Lease,
}
fn schema(connection: &Connection) -> Result<(), Error> {
    connection.execute_batch("CREATE TABLE IF NOT EXISTS display_streams(
        owner TEXT NOT NULL,root_session TEXT NOT NULL,session TEXT NOT NULL UNIQUE,
        sequence INTEGER NOT NULL DEFAULT 0,hash TEXT,
        PRIMARY KEY(owner,root_session,session));
        CREATE TABLE IF NOT EXISTS display_records(
        owner TEXT NOT NULL,root_session TEXT NOT NULL,session TEXT NOT NULL,
        sequence INTEGER NOT NULL,record BLOB NOT NULL,
        PRIMARY KEY(owner,root_session,session,sequence),
        FOREIGN KEY(owner,root_session,session) REFERENCES display_streams(owner,root_session,session) ON DELETE CASCADE);")
        .map_err(sql_failure)
}
fn head(connection: &Connection, scope: &DisplayScope) -> Result<Option<Head>, Error> {
    let row: Option<(u64, Option<String>)> = connection.query_row(
        "SELECT sequence,hash FROM display_streams WHERE owner=?1 AND root_session=?2 AND session=?3",
        params![scope.owner,scope.root_session,scope.session], |row| Ok((row.get(0)?,row.get(1)?)))
        .optional().map_err(sql_failure)?;
    row.map(|(sequence, hash)| {
        let head = Head {
            sequence,
            hash,
            ..Head::empty(scope.clone())
        };
        head.validate(scope)?;
        Ok(head)
    })
    .transpose()
}
impl Store {
    pub(super) fn open(path: &Path, scope: &DisplayScope) -> Result<Self, Error> {
        scope.validate()?;
        let connection = open(path, "display")?;
        schema(&connection)?;
        let lease = Lease::acquire(path, scope)?;
        connection.execute("INSERT OR IGNORE INTO display_streams(owner,root_session,session) VALUES(?1,?2,?3)",params![scope.owner,scope.root_session,scope.session]).map_err(sql_failure)?;
        if head(&connection, scope)?.is_none() {
            return Err(failure("display stream scope mismatch"));
        }
        let mut store = Self {
            connection,
            scope: scope.clone(),
            graph: DisplayGraph::empty(scope.clone()),
            _lease: lease,
        };
        inspect(
            &store.connection,
            scope,
            0,
            None,
            1,
            true,
            Some(&mut store.graph),
        )?;
        Ok(store)
    }
}
impl DisplayStore for Store {
    fn append(&mut self, expected: u64, record: &ArchiveRecord) -> Result<(), Error> {
        self.graph.validate(record)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_failure)?;
        let head =
            head(&transaction, &self.scope)?.ok_or_else(|| failure("display stream missing"))?;
        validate_append(expected, &head, record)?;
        let envelope = Envelope {
            previous: head.hash.clone(),
            hash: hash(head.hash.as_deref(), record)?,
            record: record.clone(),
        };
        let next = i64::try_from(record.sequence)
            .map_err(|_| failure("display sequence exceeds SQLite integer"))?;
        transaction.execute("INSERT INTO display_records(owner,root_session,session,sequence,record) VALUES(?1,?2,?3,?4,?5)",params![self.scope.owner,self.scope.root_session,self.scope.session,next,serde_json::to_vec(&envelope)?]).map_err(sql_failure)?;
        let changed = transaction.execute("UPDATE display_streams SET sequence=?4,hash=?5 WHERE owner=?1 AND root_session=?2 AND session=?3 AND sequence=?6",params![self.scope.owner,self.scope.root_session,self.scope.session,next,envelope.hash,expected]).map_err(sql_failure)?;
        if changed != 1 {
            return Err(failure("display sequence conflict"));
        }
        transaction.commit().map_err(sql_failure)?;
        self.graph.apply(record)
    }
    fn revision(&mut self) -> Result<u64, Error> {
        Ok(head(&self.connection, &self.scope)?
            .ok_or_else(|| failure("display stream missing"))?
            .sequence)
    }
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}
fn inspect(
    connection: &Connection,
    scope: &DisplayScope,
    after: u64,
    high: Option<u64>,
    limit: usize,
    verify_all: bool,
    mut graph: Option<&mut DisplayGraph>,
) -> Result<Option<DisplayPage>, Error> {
    let Some(head) = head(connection, scope)? else {
        return Ok(None);
    };
    let high_water = page_bounds(&head, after, high, limit)?;
    if verify_all {
        let count: u64 = connection.query_row("SELECT count(*) FROM display_records WHERE owner=?1 AND root_session=?2 AND session=?3",params![scope.owner,scope.root_session,scope.session],|row|row.get(0)).map_err(sql_failure)?;
        if count != head.sequence {
            return Err(failure("display committed row count mismatch"));
        }
    }
    let end = if verify_all {
        head.sequence
    } else {
        high_water.min(after.saturating_add(u64::try_from(limit).unwrap_or(u64::MAX)))
    };
    let mut sequence = if verify_all { 0 } else { after };
    let mut previous = if sequence == 0 {
        None
    } else {
        let bytes: Vec<u8> = connection.query_row("SELECT record FROM display_records WHERE owner=?1 AND root_session=?2 AND session=?3 AND sequence=?4",params![scope.owner,scope.root_session,scope.session,sequence],|row|row.get(0)).map_err(sql_failure)?;
        let boundary: Envelope = serde_json::from_slice(&bytes)?;
        check(&boundary, sequence, boundary.previous.as_deref())?;
        Some(boundary.hash)
    };
    let mut statement = connection.prepare("SELECT sequence,record FROM display_records WHERE owner=?1 AND root_session=?2 AND session=?3 AND sequence>?4 AND sequence<=?5 ORDER BY sequence").map_err(sql_failure)?;
    let mut rows = statement
        .query(params![
            scope.owner,
            scope.root_session,
            scope.session,
            sequence,
            end
        ])
        .map_err(sql_failure)?;
    let mut records = Vec::new();
    while let Some(row) = rows.next().map_err(sql_failure)? {
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| failure("display sequence overflow"))?;
        let stored: u64 = row.get(0).map_err(sql_failure)?;
        let bytes: Vec<u8> = row.get(1).map_err(sql_failure)?;
        let envelope: Envelope = serde_json::from_slice(&bytes)?;
        if stored != sequence {
            return Err(failure("display committed sequence mismatch"));
        }
        check(&envelope, sequence, previous.as_deref())?;
        if let Some(graph) = graph.as_deref_mut() {
            graph.apply(&envelope.record)?;
        }
        previous = Some(envelope.hash);
        if sequence > after && sequence <= high_water && records.len() < limit {
            records.push(envelope.record);
        }
    }
    if sequence != end || (end == head.sequence && previous != head.hash) {
        return Err(failure("display committed head mismatch"));
    }
    let last = records.last().map_or(after, |record| record.sequence);
    Ok(Some(DisplayPage {
        records,
        high_water,
        next_cursor: (last < high_water).then_some(last),
    }))
}
fn read_connection(path: &Path, scope: &DisplayScope) -> Result<Option<Connection>, Error> {
    scope.validate()?;
    if !path.try_exists()? {
        return Ok(None);
    }
    let connection = Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(sql_failure)?;
    connection
        .busy_timeout(std::time::Duration::from_secs(5))
        .map_err(sql_failure)?;
    connection
        .execute_batch("PRAGMA query_only=ON; BEGIN DEFERRED;")
        .map_err(sql_failure)?;
    let exists: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='display_streams')",[],|row|row.get(0)).map_err(sql_failure)?;
    Ok(exists.then_some(connection))
}
pub(super) fn revision(path: &Path, scope: &DisplayScope) -> Result<Option<u64>, Error> {
    let Some(connection) = read_connection(path, scope)? else {
        return Ok(None);
    };
    Ok(head(&connection, scope)?.map(|head| head.sequence))
}
pub(super) fn query(
    path: &Path,
    scope: &DisplayScope,
    after: u64,
    high: Option<u64>,
    limit: usize,
) -> Result<Option<DisplayPage>, Error> {
    let Some(connection) = read_connection(path, scope)? else {
        return Ok(None);
    };
    inspect(&connection, scope, after, high, limit, false, None)
}
