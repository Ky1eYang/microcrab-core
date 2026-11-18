//! SQL metadata uses the same records/CAS contract as file mode. Ownership columns
//! are indexed lookup guards; serialized records are independently validated.
use super::{
    Directory, MAX_DIRECTORY_BYTES, MAX_RECORD_BYTES, OperationRecord, SessionRecord, conflict,
    identity,
};
use crate::sqlite::{failure, open};
use async_trait::async_trait;
use microcrab_session::Error;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
pub struct SqliteDirectory {
    state: Arc<Mutex<Option<Connection>>>,
}
impl SqliteDirectory {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref().to_owned();
        let connection=tokio::task::spawn_blocking(move|| {
            let connection=open(&path,"directory")?;
            connection.execute_batch("CREATE TABLE IF NOT EXISTS directory_sessions(
                id TEXT PRIMARY KEY,owner TEXT NOT NULL,revision INTEGER NOT NULL,body BLOB NOT NULL);
                CREATE INDEX IF NOT EXISTS directory_sessions_owner ON directory_sessions(owner,id);
                CREATE TABLE IF NOT EXISTS directory_operations(
                id TEXT PRIMARY KEY,session_id TEXT NOT NULL REFERENCES directory_sessions(id),owner TEXT NOT NULL,
                revision INTEGER NOT NULL,body BLOB NOT NULL);
                CREATE INDEX IF NOT EXISTS directory_operations_owner ON directory_operations(owner,session_id,id);").map_err(failure)?;
            Ok::<_,Error>(connection)
        }).await.map_err(|error|Error::protocol_with_private_reason("SQLite directory open worker failed", format!("SQLite directory open worker: {error}")))??;
        Ok(Self {
            state: Arc::new(Mutex::new(Some(connection))),
        })
    }
    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Connection) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let mut state = state.lock().map_err(|_| conflict())?;
            operation(state.as_mut().ok_or(Error::Closed)?)
        })
        .await
        .map_err(|error| {
            Error::protocol_with_private_reason(
                "SQLite directory worker failed",
                format!("SQLite directory worker: {error}"),
            )
        })?
    }
}
fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, Error> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(Error::config("directory record exceeds byte budget"));
    }
    Ok(bytes)
}
fn revision(value: u64) -> Result<i64, Error> {
    i64::try_from(value).map_err(|_| conflict())
}
fn decode<T: DeserializeOwned>(bytes: Option<Vec<u8>>) -> Result<T, Error> {
    let bytes =
        bytes.ok_or_else(|| Error::Protocol("directory record exceeds byte budget".into()))?;
    Ok(serde_json::from_slice(&bytes)?)
}
fn session(connection: &Connection, owner: &str, id: &str) -> Result<Option<SessionRecord>, Error> {
    let value:Option<Option<Vec<u8>>>=connection.query_row("SELECT CASE WHEN length(body)<=?3 THEN body ELSE NULL END FROM directory_sessions WHERE id=?1 AND owner=?2",params![id,owner,MAX_RECORD_BYTES as i64],|row|row.get(0)).optional().map_err(failure)?;
    let result: Option<SessionRecord> = value
        .map(|bytes| {
            let bytes = bytes
                .ok_or_else(|| Error::Protocol("directory record exceeds byte budget".into()))?;
            super::decode_session(&bytes)
        })
        .transpose()?;
    if let Some(record) = &result {
        record.validate()?;
        if record.owner != owner || record.id != id {
            return Err(conflict());
        }
    }
    Ok(result)
}
fn operation(
    connection: &Connection,
    owner: &str,
    id: &str,
) -> Result<Option<OperationRecord>, Error> {
    let value:Option<Option<Vec<u8>>>=connection.query_row("SELECT CASE WHEN length(body)<=?3 THEN body ELSE NULL END FROM directory_operations WHERE id=?1 AND owner=?2",params![id,owner,MAX_RECORD_BYTES as i64],|row|row.get(0)).optional().map_err(failure)?;
    let result: Option<OperationRecord> = value.map(decode).transpose()?;
    if let Some(record) = &result {
        record.validate()?;
        if record.owner != owner || record.id != id {
            return Err(conflict());
        }
    }
    Ok(result)
}
#[async_trait]
impl Directory for SqliteDirectory {
    async fn list_entries(&self, owner: &str) -> Result<Vec<super::SessionEntry>, Error> {
        identity(owner)?;
        let owner = owner.to_owned();
        self.run(move |connection| {
            let mut statement = connection
                .prepare(
                    "SELECT id, length(body) FROM directory_sessions WHERE owner=?1 ORDER BY id",
                )
                .map_err(failure)?;
            let mut rows = statement.query([&owner]).map_err(failure)?;
            let mut entries = Vec::new();
            let mut bytes = 0u64;
            while let Some(row) = rows.next().map_err(failure)? {
                let id: String = row.get(0).map_err(failure)?;
                let size: u64 = row.get(1).map_err(failure)?;
                if size > MAX_RECORD_BYTES as u64 {
                    entries.push(super::SessionEntry::Unavailable {
                        id,
                        reason: "directory record exceeds byte budget".into(),
                    });
                    continue;
                }
                bytes = bytes
                    .checked_add(size)
                    .ok_or_else(|| Error::config("directory listing exceeds byte budget"))?;
                if bytes > MAX_DIRECTORY_BYTES as u64 {
                    return Err(Error::config("directory listing exceeds byte budget"));
                }
                entries.push(match session(connection, &owner, &id) {
                    Ok(Some(record)) => super::SessionEntry::Available { record },
                    Ok(None) => return Err(conflict()),
                    Err(error) => super::SessionEntry::Unavailable {
                        id,
                        reason: error.diagnostic_reason(),
                    },
                });
            }
            Ok(entries)
        })
        .await
    }
    async fn session_owner(&self, id: &str) -> Result<Option<String>, Error> {
        identity(id)?;
        let id = id.to_owned();
        self.run(move |connection| {
            let owner: Option<Option<String>> = connection.query_row(
                "SELECT CASE WHEN length(owner)<=1024 THEN owner ELSE NULL END FROM directory_sessions WHERE id=?1",
                [&id],
                |row| row.get(0),
            ).optional().map_err(failure)?;
            let Some(owner) = owner else { return Ok(None) };
            let owner = owner.ok_or_else(|| Error::config("directory owner exceeds identity budget"))?;
            identity(&owner)?;
            let record = session(connection, &owner, &id)?.ok_or_else(conflict)?;
            Ok(Some(record.owner))
        })
        .await
    }
    async fn get(&self, owner: &str, id: &str) -> Result<Option<SessionRecord>, Error> {
        identity(owner)?;
        identity(id)?;
        let owner = owner.to_owned();
        let id = id.to_owned();
        self.run(move |connection| session(connection, &owner, &id))
            .await
    }
    async fn list(&self, owner: &str) -> Result<Vec<SessionRecord>, Error> {
        identity(owner)?;
        let owner = owner.to_owned();
        self.run(move |connection| {
            let mut statement = connection
                .prepare("SELECT id FROM directory_sessions WHERE owner=?1 ORDER BY id")
                .map_err(failure)?;
            let mut rows = statement.query([&owner]).map_err(failure)?;
            let mut records = vec![];
            let mut bytes = 0;
            while let Some(row) = rows.next().map_err(failure)? {
                let id: String = row.get(0).map_err(failure)?;
                let record = session(connection, &owner, &id)?.ok_or_else(conflict)?;
                bytes += encode(&record)?.len();
                if bytes > MAX_DIRECTORY_BYTES {
                    return Err(Error::config("directory listing exceeds byte budget"));
                }
                records.push(record);
            }
            Ok(records)
        })
        .await
    }
    async fn create(&self, record: SessionRecord) -> Result<SessionRecord, Error> {
        record.validate()?;
        if record.revision != 0 {
            return Err(conflict());
        }
        let body = encode(&record)?;
        self.run(move|connection| {
            let changed=connection.execute("INSERT OR IGNORE INTO directory_sessions(id,owner,revision,body) VALUES(?1,?2,0,?3)",params![record.id,record.owner,body]).map_err(failure)?;
            if changed!=1 {return Err(conflict());}Ok(record)
        }).await
    }
    async fn replace(
        &self,
        owner: &str,
        expected_revision: u64,
        record: SessionRecord,
    ) -> Result<SessionRecord, Error> {
        identity(owner)?;
        record.validate()?;
        let owner = owner.to_owned();
        self.run(move|connection| {
            let transaction=connection.transaction_with_behavior(TransactionBehavior::Immediate).map_err(failure)?;
            session(&transaction,&owner,&record.id)?.ok_or_else(conflict)?.replacement(&owner,expected_revision,&record)?;
            let changed=transaction.execute("UPDATE directory_sessions SET revision=?1,body=?2 WHERE id=?3 AND owner=?4 AND revision=?5",params![revision(record.revision)?,encode(&record)?,record.id,owner,revision(expected_revision)?]).map_err(failure)?;
            if changed!=1 {return Err(conflict());}transaction.commit().map_err(failure)?;Ok(record)
        }).await
    }
    async fn get_operation(&self, owner: &str, id: &str) -> Result<Option<OperationRecord>, Error> {
        identity(owner)?;
        identity(id)?;
        let owner = owner.to_owned();
        let id = id.to_owned();
        self.run(move |connection| operation(connection, &owner, &id))
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
        self.run(move|connection| {
            let mut statement=connection.prepare("SELECT id FROM directory_operations WHERE owner=?1 AND session_id=?2 ORDER BY id").map_err(failure)?;
            let mut rows=statement.query(params![owner,session_id]).map_err(failure)?;
            let mut records=vec![];let mut bytes=0;
            while let Some(row)=rows.next().map_err(failure)? {
                let id:String=row.get(0).map_err(failure)?;
                let record=operation(connection,&owner,&id)?.ok_or_else(conflict)?;
                if record.session_id!=session_id {return Err(conflict());}
                bytes+=encode(&record)?.len();if bytes>MAX_DIRECTORY_BYTES {return Err(Error::config("operation listing exceeds byte budget"));}
                records.push(record);
            }
            Ok(records)
        }).await
    }
    async fn create_operation(&self, record: OperationRecord) -> Result<OperationRecord, Error> {
        record.validate()?;
        if record.revision != 0 {
            return Err(conflict());
        }
        self.run(move|connection| {
            let transaction=connection.transaction_with_behavior(TransactionBehavior::Immediate).map_err(failure)?;
            session(&transaction,&record.owner,&record.session_id)?.ok_or_else(conflict)?;
            let changed=transaction.execute("INSERT OR IGNORE INTO directory_operations(id,session_id,owner,revision,body) VALUES(?1,?2,?3,0,?4)",params![record.id,record.session_id,record.owner,encode(&record)?]).map_err(failure)?;
            if changed!=1 {return Err(conflict());}transaction.commit().map_err(failure)?;Ok(record)
        }).await
    }
    async fn replace_operation(
        &self,
        owner: &str,
        expected_revision: u64,
        record: OperationRecord,
    ) -> Result<OperationRecord, Error> {
        identity(owner)?;
        record.validate()?;
        let owner = owner.to_owned();
        self.run(move|connection| {
            let transaction=connection.transaction_with_behavior(TransactionBehavior::Immediate).map_err(failure)?;
            operation(&transaction,&owner,&record.id)?.ok_or_else(conflict)?.replacement(&owner,expected_revision,&record)?;
            let changed=transaction.execute("UPDATE directory_operations SET revision=?1,body=?2 WHERE id=?3 AND owner=?4 AND revision=?5",params![revision(record.revision)?,encode(&record)?,record.id,owner,revision(expected_revision)?]).map_err(failure)?;
            if changed!=1 {return Err(conflict());}transaction.commit().map_err(failure)?;Ok(record)
        }).await
    }
    async fn settle_operation(
        &self,
        owner: &str,
        settlement: super::OperationSettlement,
    ) -> Result<(SessionRecord, OperationRecord), Error> {
        settlement.validate(owner)?;
        let owner = owner.to_owned();
        self.run(move |connection| {
            let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate).map_err(failure)?;
            let current_session = session(&transaction, &owner, &settlement.session.id)?.ok_or_else(conflict)?;
            let current_operation = operation(&transaction, &owner, &settlement.operation.id)?.ok_or_else(conflict)?;
            settlement.check(&owner, &current_session, &current_operation)?;
            if let Some(expected) = &settlement.journal_cursor {
                let selected: Option<Option<Vec<u8>>> = transaction.query_row(
                    "SELECT CASE WHEN length(cursor)<=16384 THEN cursor ELSE NULL END FROM journal_heads WHERE session_id=?1",
                    [&settlement.session.id], |row| row.get(0),
                ).optional().map_err(failure)?;
                let selected: crate::history::JournalCursor = serde_json::from_slice(&selected.flatten().ok_or_else(conflict)?)?;
                if &selected != expected { return Err(conflict()); }
            }
            let changed = transaction.execute(
                "UPDATE directory_sessions SET revision=?1,body=?2 WHERE id=?3 AND owner=?4 AND revision=?5",
                params![revision(settlement.session.revision)?, encode(&settlement.session)?, settlement.session.id, owner, revision(settlement.expected_session_revision)?],
            ).map_err(failure)?;
            if changed != 1 { return Err(conflict()); }
            let changed = transaction.execute(
                "UPDATE directory_operations SET revision=?1,body=?2 WHERE id=?3 AND session_id=?4 AND owner=?5 AND revision=?6",
                params![revision(settlement.operation.revision)?, encode(&settlement.operation)?, settlement.operation.id, settlement.session.id, owner, revision(settlement.expected_operation_revision)?],
            ).map_err(failure)?;
            if changed != 1 { return Err(conflict()); }
            transaction.commit().map_err(failure)?;
            Ok((settlement.session, settlement.operation))
        }).await
    }
    async fn remove(&self, owner: &str, id: &str, expected_revision: u64) -> Result<usize, Error> {
        identity(owner)?;
        identity(id)?;
        let owner = owner.to_owned();
        let id = id.to_owned();
        self.run(move |connection| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(failure)?;
            let current = session(&transaction, &owner, &id)?.ok_or_else(conflict)?;
            if current.revision != expected_revision {
                return Err(conflict());
            }
            // Receipts reference their session, so both leave in one transaction.
            let receipts = transaction
                .execute(
                    "DELETE FROM directory_operations WHERE owner=?1 AND session_id=?2",
                    params![&owner, &id],
                )
                .map_err(failure)?;
            let changed = transaction
                .execute(
                    "DELETE FROM directory_sessions WHERE id=?1 AND owner=?2 AND revision=?3",
                    params![&id, &owner, revision(expected_revision)?],
                )
                .map_err(failure)?;
            if changed != 1 {
                return Err(conflict());
            }
            transaction.commit().map_err(failure)?;
            Ok(receipts)
        })
        .await
    }
    async fn close(&self) -> Result<(), Error> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            drop(state.lock().map_err(|_| conflict())?.take());
            Ok(())
        })
        .await
        .map_err(|_| conflict())?
    }
}
