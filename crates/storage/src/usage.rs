//! Backend-selected append-only consumption bytes. Creator owns Usage semantics;
//! this adapter never derives totals from selected conversation ancestry.
use crate::{
    audit::AuditScope,
    sqlite::{failure, open},
};
use microcrab_session::Error;
use rusqlite::{Connection, params};
use std::path::Path;

pub struct SqliteUsageStore {
    connection: Connection,
    scope: AuditScope,
    name: String,
}
fn schema(connection: &Connection) -> Result<(), Error> {
    connection.execute_batch("CREATE TABLE IF NOT EXISTS usage_records(
        id INTEGER PRIMARY KEY, owner TEXT NOT NULL, root_session TEXT NOT NULL,
        session TEXT NOT NULL, ledger TEXT NOT NULL, record BLOB NOT NULL);
        CREATE INDEX IF NOT EXISTS usage_scope ON usage_records(owner,root_session,session,ledger,id);") .map_err(failure)
}
fn rows(connection: &Connection, scope: &AuditScope, name: &str) -> Result<Vec<Vec<u8>>, Error> {
    let size: u64 = connection.query_row("SELECT coalesce(sum(length(record)),0) FROM usage_records WHERE owner=?1 AND root_session=?2 AND session=?3 AND ledger=?4", params![scope.owner,scope.root_session,scope.session,name], |row| row.get(0)).map_err(failure)?;
    if size > 64 * 1024 * 1024 {
        return Err(Error::Limit("usage read budget exceeded".into()));
    }
    let mut statement = connection.prepare("SELECT record FROM usage_records WHERE owner=?1 AND root_session=?2 AND session=?3 AND ledger=?4 ORDER BY id").map_err(failure)?;
    let mut rows = statement
        .query(params![
            scope.owner,
            scope.root_session,
            scope.session,
            name
        ])
        .map_err(failure)?;
    let mut records = Vec::new();
    let mut remaining = 64 * 1024 * 1024usize;
    while let Some(row) = rows.next().map_err(failure)? {
        let record: Vec<u8> = row.get(0).map_err(failure)?;
        remaining = remaining
            .checked_sub(record.len())
            .ok_or_else(|| Error::Limit("usage read budget exceeded".into()))?;
        records.push(record);
    }
    Ok(records)
}
impl SqliteUsageStore {
    pub fn open(path: &Path, scope: AuditScope, name: &str) -> Result<Self, Error> {
        scope.validate()?;
        if !matches!(name, "usage.jsonl" | "aggregate_usage.jsonl") {
            return Err(Error::config("invalid usage ledger"));
        }
        let connection = open(path, "usage")?;
        schema(&connection)?;
        Ok(Self {
            connection,
            scope,
            name: name.into(),
        })
    }
    pub fn read(&self) -> Result<Vec<Vec<u8>>, Error> {
        rows(&self.connection, &self.scope, &self.name)
    }
    pub fn append(&mut self, record: &[u8]) -> Result<(), Error> {
        if record.len() > 1024 * 1024 {
            return Err(Error::Limit("usage record budget exceeded".into()));
        }
        self.connection.execute("INSERT INTO usage_records(owner,root_session,session,ledger,record) VALUES(?1,?2,?3,?4,?5)", params![self.scope.owner,self.scope.root_session,self.scope.session,self.name,record]).map_err(failure)?;
        Ok(())
    }
    pub fn inspect(path: &Path, scope: &AuditScope, name: &str) -> Result<Vec<Vec<u8>>, Error> {
        scope.validate()?;
        if !path.try_exists()? {
            return Ok(vec![]);
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
        let exists: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='usage_records')",
                [],
                |row| row.get(0),
            )
            .map_err(failure)?;
        if !exists {
            return Ok(vec![]);
        }
        rows(&connection, scope, name)
    }
}
