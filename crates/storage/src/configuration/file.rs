//! Exclusive file writer; blocking publication survives cancellation of an HTTP waiter.
use super::{Configuration, ConfigurationRecord, conflict, next, validate};
use async_trait::async_trait;
use microcrab_session::Error;
use serde_json::Value;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
#[derive(Clone)]
pub struct FileConfiguration {
    state: Arc<Mutex<State>>,
}
struct Temporary(PathBuf);
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
struct State {
    root: PathBuf,
    _lock: Option<File>,
    records: Vec<ConfigurationRecord>,
    writable: bool,
}
impl FileConfiguration {
    pub async fn open(root: impl AsRef<Path>) -> Result<Self, Error> {
        let root = root.as_ref().to_owned();
        let state = tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&root)?;
            let lock = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(root.join("writer.lock"))?;
            lock.try_lock().map_err(|e| {
                Error::protocol_with_private_reason(
                    "configuration writer lock unavailable",
                    e.to_string(),
                )
            })?;
            let path = root.join("records.json");
            let records: Vec<ConfigurationRecord> = if path.try_exists()? {
                let mut bytes = Vec::new();
                File::open(path)?
                    .take(64 * 1024 * 1024 + 1)
                    .read_to_end(&mut bytes)?;
                if bytes.len() > 64 * 1024 * 1024 {
                    return Err(Error::config("configuration store exceeds 64 MiB"));
                }
                serde_json::from_slice(&bytes)?
            } else {
                Vec::new()
            };
            let mut keys = std::collections::BTreeSet::new();
            for r in &records {
                validate(&r.owner, &r.kind, &r.target, &r.value)?;
                if r.revision == 0 || !keys.insert((&r.owner, &r.kind, &r.target)) {
                    return Err(conflict());
                }
            }
            Ok::<_, Error>(State {
                root,
                _lock: Some(lock),
                records,
                writable: true,
            })
        })
        .await
        .map_err(|_| Error::config("configuration open worker failed"))??;
        Ok(Self {
            state: Arc::new(Mutex::new(state)),
        })
    }
    async fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut State) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let mut state = state.lock().map_err(|_| conflict())?;
            if state._lock.is_none() {
                return Err(Error::Closed);
            }
            f(&mut state)
        })
        .await
        .map_err(|_| Error::config("configuration worker failed"))?
    }
}
#[async_trait]
impl Configuration for FileConfiguration {
    async fn close(&self) -> Result<(), Error> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let mut state = state.lock().map_err(|_| conflict())?;
            state._lock.take();
            state.records.clear();
            Ok(())
        })
        .await
        .map_err(|_| Error::config("configuration close worker failed"))?
    }

    async fn get(
        &self,
        owner: &str,
        kind: &str,
        target: &str,
    ) -> Result<Option<ConfigurationRecord>, Error> {
        validate(owner, kind, target, &None)?;
        let (owner, kind, target) = (owner.to_owned(), kind.to_owned(), target.to_owned());
        self.run(move |s| {
            Ok(s.records
                .iter()
                .find(|r| r.owner == owner && r.kind == kind && r.target == target)
                .cloned())
        })
        .await
    }
    async fn list(&self, owner: &str, kind: &str) -> Result<Vec<ConfigurationRecord>, Error> {
        validate(owner, kind, "list", &None)?;
        let (owner, kind) = (owner.to_owned(), kind.to_owned());
        self.run(move |s| {
            Ok(s.records
                .iter()
                .filter(|r| r.owner == owner && r.kind == kind && r.value.is_some())
                .cloned()
                .collect())
        })
        .await
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
        self.run(move |s| {
            if !s.writable {return Err(Error::config("configuration store quarantined after ambiguous publication; reopen before writing"));}
            let index=s.records.iter().position(|r|r.owner==owner&&r.kind==kind&&r.target==target);
            let record=next(&owner,&kind,&target,expected_revision,index.map(|i|&s.records[i]),value)?;
            let mut records=s.records.clone();
            if let Some(i)=index {records[i]=record.clone();} else {records.push(record.clone());}
            let bytes=serde_json::to_vec(&records)?;
            if bytes.len()>64*1024*1024 {return Err(Error::config("configuration store exceeds 64 MiB"));}
            let temporary=Temporary(s.root.join(format!(".tmp-{}",uuid::Uuid::new_v4())));
            let mut options=OpenOptions::new();options.write(true).create_new(true);
            #[cfg(unix)] {use std::os::unix::fs::OpenOptionsExt;options.mode(0o600);}
            let mut file=options.open(&temporary.0)?;
            file.write_all(&bytes)?;file.sync_all()?;drop(file);
            // Before rename, errors are proven non-publications. Once replacement
            // starts, failures quarantine this writer rather than risk stale CAS.
            s.writable=false;
            std::fs::rename(&temporary.0,s.root.join("records.json"))?;
            #[cfg(unix)] File::open(&s.root)?.sync_all()?;
            s.records=records;s.writable=true;Ok(record)
        }).await
    }
}
