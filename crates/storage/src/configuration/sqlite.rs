//! Targeted SQL CAS has the same owner isolation and tombstones as file storage.
use super::{
    Configuration, ConfigurationRecord, MAX_CONFIGURATION_BYTES, conflict, next, validate,
};
use crate::sqlite::{failure, open};
use async_trait::async_trait;
use microcrab_session::Error;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::Value;
use std::{
    path::Path,
    sync::{Arc, Mutex},
};
#[derive(Clone)]
pub struct SqliteConfiguration {
    state: Arc<Mutex<Option<Connection>>>,
}
impl SqliteConfiguration {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref().to_owned();
        let connection=tokio::task::spawn_blocking(move||{let connection=open(&path,"configuration")?;connection.execute_batch("CREATE TABLE IF NOT EXISTS owner_configuration(owner TEXT NOT NULL,kind TEXT NOT NULL,target TEXT NOT NULL,revision INTEGER NOT NULL,body BLOB NOT NULL,PRIMARY KEY(owner,kind,target));") .map_err(failure)?;Ok::<_,Error>(connection)}).await.map_err(|_|Error::config("configuration SQL open worker failed"))??;
        Ok(Self {
            state: Arc::new(Mutex::new(Some(connection))),
        })
    }
    async fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = state.lock().map_err(|_| conflict())?;
            f(connection.as_mut().ok_or(Error::Closed)?)
        })
        .await
        .map_err(|_| Error::config("configuration SQL worker failed"))?
    }
}
fn read(
    connection: &Connection,
    owner: &str,
    kind: &str,
    target: &str,
) -> Result<Option<ConfigurationRecord>, Error> {
    let bytes:Option<Option<Vec<u8>>>=connection.query_row("SELECT CASE WHEN length(body)<=?4 THEN body ELSE NULL END FROM owner_configuration WHERE owner=?1 AND kind=?2 AND target=?3",params![owner,kind,target,(MAX_CONFIGURATION_BYTES+8192) as i64],|r|r.get(0)).optional().map_err(failure)?;
    bytes
        .map(|bytes| {
            let bytes = bytes
                .ok_or_else(|| Error::config("configuration SQL record exceeds byte bound"))?;
            let record: ConfigurationRecord = serde_json::from_slice(&bytes)?;
            validate(&record.owner, &record.kind, &record.target, &record.value)?;
            if record.owner != owner
                || record.kind != kind
                || record.target != target
                || record.revision == 0
            {
                return Err(conflict());
            }
            Ok(record)
        })
        .transpose()
}
#[async_trait]
impl Configuration for SqliteConfiguration {
    async fn close(&self) -> Result<(), Error> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let mut state = state.lock().map_err(|_| conflict())?;
            state.take();
            Ok(())
        })
        .await
        .map_err(|_| Error::config("configuration SQL close worker failed"))?
    }

    async fn get(
        &self,
        owner: &str,
        kind: &str,
        target: &str,
    ) -> Result<Option<ConfigurationRecord>, Error> {
        validate(owner, kind, target, &None)?;
        let (owner, kind, target) = (owner.to_owned(), kind.to_owned(), target.to_owned());
        self.run(move |c| read(c, &owner, &kind, &target)).await
    }
    async fn list(&self, owner: &str, kind: &str) -> Result<Vec<ConfigurationRecord>, Error> {
        validate(owner, kind, "list", &None)?;
        let (owner, kind) = (owner.to_owned(), kind.to_owned());
        self.run(move|c|{let mut statement=c.prepare("SELECT target FROM owner_configuration WHERE owner=?1 AND kind=?2 ORDER BY target").map_err(failure)?;let ids=statement.query_map(params![owner,kind],|r|r.get::<_,String>(0)).map_err(failure)?.collect::<Result<Vec<_>,_>>().map_err(failure)?;let mut records=Vec::new();for id in ids{if let Some(record)=read(c,&owner,&kind,&id)?&&record.value.is_some(){records.push(record);}}Ok(records)}).await
    }
    async fn compare_exchange(
        &self,
        owner: &str,
        kind: &str,
        target: &str,
        expected_revision: u64,
        value: Option<Value>,
    ) -> Result<ConfigurationRecord, Error> {
        validate(owner, kind, target, &value)?;
        let (owner, kind, target) = (owner.to_owned(), kind.to_owned(), target.to_owned());
        self.run(move|c|{let tx=c.transaction_with_behavior(TransactionBehavior::Immediate).map_err(failure)?;let current=read(&tx,&owner,&kind,&target)?;let record=next(&owner,&kind,&target,expected_revision,current.as_ref(),value)?;let revision=i64::try_from(record.revision).map_err(|_|conflict())?;tx.execute("INSERT INTO owner_configuration(owner,kind,target,revision,body) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(owner,kind,target) DO UPDATE SET revision=excluded.revision,body=excluded.body",params![owner,kind,target,revision,serde_json::to_vec(&record)?]).map_err(failure)?;tx.commit().map_err(failure)?;Ok(record)}).await
    }
}
