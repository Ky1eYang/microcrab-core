//! Exact scope-qualified rows and immediate transactions. The in-process owner
//! lease is held through writes and joined by the audit root purge fence.
use super::{DialogueReplyScope, invalid, uncertain_write};
use crate::sqlite::{failure, open};
use async_trait::async_trait;
use microcrab_session::{
    Error,
    state::communication::{
        AgentId,
        reply::{
            DialogueAdmissionKey, DialogueReplyMutation, DialogueReplyStore, DurableDialogueReply,
            MAX_REPLY_RECORD_BYTES, REPLY_METADATA_RESERVE, ReplyWrite, check_reply_budget,
            reply_extra_charge,
        },
    },
};
use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, OnceLock},
};
use uuid::Uuid;

#[derive(Default)]
struct Admission {
    writers: Vec<(PathBuf, DialogueReplyScope)>,
    uncertain: HashSet<(PathBuf, DialogueReplyScope)>,
}
static ADMISSION: OnceLock<Mutex<Admission>> = OnceLock::new();
fn admission() -> &'static Mutex<Admission> {
    ADMISSION.get_or_init(Mutex::default)
}

#[cfg(test)]
static FAIL_AFTER_COMMIT: OnceLock<Mutex<Option<Uuid>>> = OnceLock::new();
#[cfg(test)]
pub(super) fn fail_after_commit(id: Uuid) {
    *FAIL_AFTER_COMMIT
        .get_or_init(Mutex::default)
        .lock()
        .unwrap() = Some(id);
}

pub(crate) struct RootPurgeGuard {
    admission: MutexGuard<'static, Admission>,
}
impl RootPurgeGuard {
    pub(crate) fn acquire(path: &Path, owner: &str, root: &str) -> Result<Self, Error> {
        let held = admission().lock().map_err(|_| invalid())?;
        if held
            .writers
            .iter()
            .any(|(db, scope)| db == path && scope.owner == owner && scope.root_session == root)
        {
            return Err(Error::config(
                "private dialogue reply scope still has a writer",
            ));
        }
        Ok(Self { admission: held })
    }
    pub(crate) fn acknowledge_purge(&mut self, path: &Path, owner: &str, root: &str) {
        self.admission
            .uncertain
            .retain(|(db, scope)| db != path || scope.owner != owner || scope.root_session != root);
    }

    pub(crate) fn delete(
        &self,
        tx: &Transaction<'_>,
        owner: &str,
        root: &str,
    ) -> Result<(), Error> {
        if table_exists(tx, "dialogue_reply_scopes")? {
            tx.execute(
                "DELETE FROM dialogue_reply_scopes WHERE owner=?1 AND root_session=?2",
                params![owner, root],
            )
            .map_err(failure)?;
        }
        Ok(())
    }
}

struct Lease(PathBuf, DialogueReplyScope);
impl Drop for Lease {
    fn drop(&mut self) {
        admission()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .writers
            .retain(|(path, scope)| path != &self.0 || scope != &self.1);
    }
}
struct Writer {
    db: Connection,
    scope: DialogueReplyScope,
    lease: Lease,
}
pub struct SqliteDialogueReplies(Arc<Mutex<Writer>>);

impl SqliteDialogueReplies {
    pub async fn open(path: impl AsRef<Path>, scope: DialogueReplyScope) -> Result<Self, Error> {
        let path = path.as_ref().to_owned();
        worker(move || {
            let mut held = admission().lock().map_err(|_| invalid())?;
            let mut db = open(&path, "dialogue_replies")?;
            let path = path.canonicalize()?;
            if held.writers.iter().any(|(db, existing)| db == &path && existing.session == scope.session) {
                return Err(Error::config("private dialogue reply scope already has a writer"));
            }
            schema(&db)?;
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(failure)?;
            tx.execute("INSERT OR IGNORE INTO dialogue_reply_scopes(session,owner,root_session,agent_id) VALUES(?1,?2,?3,?4)", params![scope.session, scope.owner, scope.root_session, scope.agent_id.as_str()]).map_err(failure)?;
            if !verify_scope(&tx, &scope)? { return Err(invalid()); }
            let (count, charge) = budget(&tx, &scope)?;
            check_reply_budget(count, charge)?;
            tx.commit().map_err(failure)?;
            held.uncertain.remove(&(path.clone(), scope.clone()));
            held.writers.push((path.clone(), scope.clone()));
            Ok(Self(Arc::new(Mutex::new(Writer { db, lease: Lease(path, scope.clone()), scope }))))
        }).await
    }

    pub async fn inspect(
        path: impl AsRef<Path>,
        scope: DialogueReplyScope,
        key: &DialogueAdmissionKey,
    ) -> Result<Option<DurableDialogueReply>, Error> {
        let path = path.as_ref().to_owned();
        let key = key.clone();
        worker(move || {
            if !path.try_exists()? {
                return Ok(None);
            }
            let path = path.canonicalize()?;
            let held = admission().lock().map_err(|_| invalid())?;
            if held.uncertain.contains(&(path.clone(), scope.clone())) {
                return Err(invalid());
            }
            let mut db = Connection::open_with_flags(
                path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .map_err(failure)?;
            db.execute_batch("PRAGMA query_only=ON;").map_err(failure)?;
            let tx = db
                .transaction_with_behavior(TransactionBehavior::Deferred)
                .map_err(failure)?;
            if !table_exists(&tx, "dialogue_reply_scopes")? || !verify_scope(&tx, &scope)? {
                return Ok(None);
            }
            load_record(&tx, &scope, &key)
        })
        .await
    }
}

#[async_trait]
impl DialogueReplyStore for SqliteDialogueReplies {
    async fn load(
        &self,
        agent_id: &AgentId,
        key: &DialogueAdmissionKey,
    ) -> Result<Option<DurableDialogueReply>, Error> {
        let writer = self.0.clone();
        let key = key.clone();
        let agent_id = agent_id.clone();
        worker(move || {
            let mut writer = writer.lock().map_err(|_| invalid())?;
            let held = admission().lock().map_err(|_| invalid())?;
            if held
                .uncertain
                .contains(&(writer.lease.0.clone(), writer.scope.clone()))
                || writer.scope.agent_id() != &agent_id
            {
                return Err(invalid());
            }
            let scope = writer.scope.clone();
            let tx = writer
                .db
                .transaction_with_behavior(TransactionBehavior::Deferred)
                .map_err(failure)?;
            if !verify_scope(&tx, &scope)? {
                return Err(invalid());
            }
            load_record(&tx, &scope, &key)
        })
        .await
    }
    async fn apply(&self, mutation: DialogueReplyMutation) -> Result<ReplyWrite, Error> {
        let writer = self.0.clone();
        worker(move || {
            let mut writer = writer.lock().map_err(|_| invalid())?;
            // Scope inspection and purge cannot race the commit/uncertainty
            // publication. Reopen explicitly reconciles SQLite's atomic image.
            let mut held = admission().lock().map_err(|_| invalid())?;
            let address = (writer.lease.0.clone(), writer.scope.clone());
            if held.uncertain.contains(&address) || writer.scope.agent_id() != mutation.binding().agent_id() { return Err(invalid()); }
            let scope = writer.scope.clone();
            let tx = writer.db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(failure)?;
            if !verify_scope(&tx, &scope)? { return Err(invalid()); }
            let id = mutation.binding().admission().id();
            let previous = read(&tx, &scope, id)?;
            let outcome = mutation.apply(previous.as_ref().map(|(_, record)| record))?;
            if let ReplyWrite::Applied(record) = &outcome {
                let bytes = record.storage_bytes()?;
                let (count, charge) = budget(&tx, &scope)?;
                let previous_charge = previous.as_ref().map_or(0, |(charge, _)| *charge);
                let charge = charge.checked_sub(previous_charge).ok_or_else(invalid)? + reply_extra_charge(bytes.len())?;
                check_reply_budget(count + usize::from(previous.is_none()), charge)?;
                tx.execute("INSERT INTO dialogue_reply_records(session,id,body) VALUES(?1,?2,?3) ON CONFLICT(session,id) DO UPDATE SET body=excluded.body", params![scope.session, id.as_bytes().as_slice(), bytes]).map_err(failure)?;
                if let Err(error) = tx.commit() {
                    held.uncertain.insert(address.clone());
                    return Err(uncertain_write(failure(error)));
                }
                #[cfg(test)]
                if let Some(slot) = FAIL_AFTER_COMMIT.get() {
                    let mut target = slot.lock().map_err(|_| invalid())?;
                    if target.as_ref() == Some(&id) {
                        target.take();
                        held.uncertain.insert(address);
                        return Err(uncertain_write(Error::lifecycle("injected lost SQLite reply acknowledgement")));
                    }
                }
            }
            Ok(outcome)
        }).await
    }
}

fn schema(db: &Connection) -> Result<(), Error> {
    db.execute_batch("CREATE TABLE IF NOT EXISTS dialogue_reply_scopes(
        session TEXT PRIMARY KEY, owner TEXT NOT NULL, root_session TEXT NOT NULL, agent_id TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS dialogue_reply_scope_owner ON dialogue_reply_scopes(owner,root_session);
        CREATE TABLE IF NOT EXISTS dialogue_reply_records(
        session TEXT NOT NULL REFERENCES dialogue_reply_scopes(session) ON DELETE CASCADE,
        id BLOB NOT NULL CHECK(length(id)=16), body BLOB NOT NULL,
        PRIMARY KEY(session,id));").map_err(failure)
}
fn table_exists(db: &Connection, name: &str) -> Result<bool, Error> {
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
        [name],
        |row| row.get(0),
    )
    .map_err(failure)
}
fn verify_scope(db: &Connection, scope: &DialogueReplyScope) -> Result<bool, Error> {
    let matches: Option<bool> = db.query_row("SELECT owner=?2 AND root_session=?3 AND agent_id=?4 FROM dialogue_reply_scopes WHERE session=?1", params![scope.session, scope.owner, scope.root_session, scope.agent_id.as_str()], |row| row.get(0)).optional().map_err(failure)?;
    match matches {
        None => Ok(false),
        Some(true) => Ok(true),
        Some(false) => Err(invalid()),
    }
}
fn budget(db: &Connection, scope: &DialogueReplyScope) -> Result<(usize, usize), Error> {
    // Do not materialize or scan unrelated reply contents. An oversized or
    // corrupt BLOB fails closed even when the requested reply is small.
    let (count, charge, max): (usize, usize, usize) = db.query_row(
        "SELECT count(*),coalesce(sum(max(length(body)-?2,0)),0),coalesce(max(length(body)),0) FROM dialogue_reply_records WHERE session=?1",
        params![scope.session, REPLY_METADATA_RESERVE as i64], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))
    ).map_err(failure)?;
    reply_extra_charge(max)?;
    Ok((count, charge))
}
fn read(
    db: &Connection,
    scope: &DialogueReplyScope,
    id: Uuid,
) -> Result<Option<(usize, DurableDialogueReply)>, Error> {
    let row: Option<Option<Vec<u8>>> = db.query_row(
        "SELECT CASE WHEN typeof(body)='blob' AND length(body)<=?3 THEN body ELSE NULL END FROM dialogue_reply_records WHERE session=?1 AND id=?2",
        params![scope.session, id.as_bytes().as_slice(), MAX_REPLY_RECORD_BYTES as i64], |row| row.get(0)
    ).optional().map_err(failure)?;
    row.map(|bytes| {
        let bytes = bytes.ok_or_else(invalid)?;
        let record = DurableDialogueReply::from_storage_bytes(&bytes)?;
        Ok((reply_extra_charge(bytes.len())?, record))
    })
    .transpose()
}
fn load_record(
    db: &Connection,
    scope: &DialogueReplyScope,
    key: &DialogueAdmissionKey,
) -> Result<Option<DurableDialogueReply>, Error> {
    read(db, scope, key.id())?
        .map(|(_, record)| {
            record.validate_exact(scope.agent_id(), key)?;
            Ok(record)
        })
        .transpose()
}
async fn worker<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, Error> + Send + 'static,
) -> Result<T, Error> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|_| invalid())?
}
