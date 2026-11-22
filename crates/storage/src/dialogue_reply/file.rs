//! One atomically published file per dialogue. The embedding process owns the
//! session directory exclusively, as with other scoped producer admissions.
use super::{DialogueReplyScope, invalid, uncertain_write};
use async_trait::async_trait;
use microcrab_session::{
    Error,
    state::communication::{
        AgentId,
        reply::{
            DialogueAdmissionKey, DialogueReplyMutation, DialogueReplyStore, DurableDialogueReply,
            MAX_REPLY_RECORD_BYTES, ReplyWrite, check_reply_budget, reply_extra_charge,
        },
    },
};
use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
};
use uuid::Uuid;

const SCOPE_FILE: &str = "scope.json";
const MAX_SCOPE_BYTES: usize = 20 * 1024;
#[derive(Default)]
struct Admission {
    writers: HashSet<PathBuf>,
    // An observer must not mistake visible-but-unacknowledged rename data for
    // an acknowledged mutation. Quarantine until an explicit writer reopen.
    uncertain: HashSet<PathBuf>,
}
static ADMISSION: OnceLock<Mutex<Admission>> = OnceLock::new();
#[cfg(test)]
static FAIL_AFTER_RENAME: OnceLock<Mutex<Option<PathBuf>>> = OnceLock::new();
#[cfg(test)]
pub(super) fn fail_after_rename(directory: &Path, id: Uuid) {
    let root = existing_root(directory).unwrap().unwrap();
    *FAIL_AFTER_RENAME
        .get_or_init(Mutex::default)
        .lock()
        .unwrap() = Some(record_path(&root, id));
}

pub struct FileDialogueReplies {
    root: PathBuf,
    scope: DialogueReplyScope,
    lease: Arc<Lease>,
}
struct Lease(PathBuf);
impl FileDialogueReplies {
    pub async fn open(
        session_directory: impl AsRef<Path>,
        scope: DialogueReplyScope,
    ) -> Result<Self, Error> {
        let directory = session_directory.as_ref().to_owned();
        worker(move || {
            let root = directory.canonicalize()?.join("dialogue-replies");
            let mut state = admission().lock().map_err(|_| invalid())?;
            if state.writers.contains(&root) {
                return Err(Error::config(
                    "private dialogue reply scope already has a writer",
                ));
            }
            let mut builder = fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            match builder.create(&root) {
                Ok(()) => sync_dir(root.parent().ok_or_else(invalid)?)?,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
            if !verify_directory(&root)? {
                return Err(invalid());
            }
            if !verify_scope(&root, &scope)? {
                // A crash can leave a not-yet-published scope with only a temp
                // file. Never claim a directory containing another scope's data.
                if account(&root, None, true)?.0 != 0 {
                    return Err(invalid());
                }
                publish(&root.join(SCOPE_FILE), &serde_json::to_vec(&scope)?)?;
            }
            let (count, charge, _) = account(&root, None, true)?;
            check_reply_budget(count, charge)?;
            // After a quarantined writer, revalidate/sync the surviving image
            // before any reader is allowed to report terminal evidence again.
            if state.uncertain.contains(&root) {
                for entry in fs::read_dir(&root)? {
                    let entry = entry?;
                    if let Some(id) = record_id(&entry.file_name().to_string_lossy())? {
                        let record = read_record(&entry.path())?.ok_or_else(invalid)?;
                        if record.binding().agent_id() != scope.agent_id()
                            || record.binding().admission().id() != id
                        {
                            return Err(invalid());
                        }
                    }
                }
                sync_dir(&root)?;
                state.uncertain.remove(&root);
            }
            state.writers.insert(root.clone());
            Ok(Self {
                lease: Arc::new(Lease(root.clone())),
                root,
                scope,
            })
        })
        .await
    }

    pub async fn inspect(
        session_directory: impl AsRef<Path>,
        scope: DialogueReplyScope,
        key: &DialogueAdmissionKey,
    ) -> Result<Option<DurableDialogueReply>, Error> {
        let directory = session_directory.as_ref().to_owned();
        let key = key.clone();
        worker(move || {
            let Some(root) = existing_root(&directory)? else {
                return Ok(None);
            };
            let state = admission().lock().map_err(|_| invalid())?;
            if state.uncertain.contains(&root) {
                return Err(invalid());
            }
            if !verify_directory(&root)? || !verify_scope(&root, &scope)? {
                return Ok(None);
            }
            load_record(&root, scope.agent_id(), &key)
        })
        .await
    }

    /// Purge one owner-authorized Session directory after activation has been
    /// fenced by the embedder. Root deletion must cover all its descendant dirs.
    pub async fn purge(
        session_directory: impl AsRef<Path>,
        scope: DialogueReplyScope,
    ) -> Result<(), Error> {
        let directory = session_directory.as_ref().to_owned();
        worker(move || {
            let Some(root) = existing_root(&directory)? else {
                return Ok(());
            };
            let mut state = admission().lock().map_err(|_| invalid())?;
            if state.writers.contains(&root) {
                return Err(Error::config(
                    "private dialogue reply scope still has a writer",
                ));
            }
            if !verify_directory(&root)? {
                return Ok(());
            }
            if verify_scope(&root, &scope)? {
                // Keep the scope witness until all data is gone, including on
                // partial failure. A retry can remove an empty scope directory.
                account(&root, None, true)?;
                for entry in fs::read_dir(&root)? {
                    let entry = entry?;
                    if record_id(entry.file_name().to_str().ok_or_else(invalid)?)?.is_some() {
                        fs::remove_file(entry.path())?;
                    }
                }
                sync_dir(&root)?;
                fs::remove_file(root.join(SCOPE_FILE))?;
            } else if account(&root, None, true)?.0 != 0 {
                return Err(invalid());
            }
            fs::remove_dir(&root)?;
            sync_dir(root.parent().ok_or_else(invalid)?)?;
            state.uncertain.remove(&root);
            Ok(())
        })
        .await
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        admission()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .writers
            .remove(&self.0);
    }
}

#[async_trait]
impl DialogueReplyStore for FileDialogueReplies {
    async fn load(
        &self,
        agent_id: &AgentId,
        key: &DialogueAdmissionKey,
    ) -> Result<Option<DurableDialogueReply>, Error> {
        if agent_id != self.scope.agent_id() {
            return Err(invalid());
        }
        let root = self.root.clone();
        let scope = self.scope.clone();
        let key = key.clone();
        worker(move || {
            let state = admission().lock().map_err(|_| invalid())?;
            if state.uncertain.contains(&root) || !verify_scope(&root, &scope)? {
                return Err(invalid());
            }
            load_record(&root, scope.agent_id(), &key)
        })
        .await
    }
    async fn apply(&self, mutation: DialogueReplyMutation) -> Result<ReplyWrite, Error> {
        if mutation.binding().agent_id() != self.scope.agent_id() {
            return Err(invalid());
        }
        let root = self.root.clone();
        let scope = self.scope.clone();
        // Keep writer ownership inside the blocking worker, even on async cancellation.
        let lease = self.lease.clone();
        worker(move || {
            let _lease = lease;
            let mut state = admission().lock().map_err(|_| invalid())?;
            if state.uncertain.contains(&root) || !verify_scope(&root, &scope)? {
                return Err(invalid());
            }
            let id = mutation.binding().admission().id();
            let path = record_path(&root, id);
            let previous = read_record(&path)?;
            let outcome = mutation.apply(previous.as_ref())?;
            if let ReplyWrite::Applied(record) = &outcome {
                let bytes = record.storage_bytes()?;
                let (count, charge, replaced_charge) = account(&root, Some(id), false)?;
                let charge = charge - replaced_charge + reply_extra_charge(bytes.len())?;
                check_reply_budget(count + usize::from(previous.is_none()), charge)?;
                if let Err(error) = publish(&path, &bytes) {
                    state.uncertain.insert(root);
                    return Err(uncertain_write(error));
                }
            }
            Ok(outcome)
        })
        .await
    }
}

fn admission() -> &'static Mutex<Admission> {
    ADMISSION.get_or_init(Mutex::default)
}
async fn worker<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, Error> + Send + 'static,
) -> Result<T, Error> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|_| invalid())?
}
fn existing_root(directory: &Path) -> Result<Option<PathBuf>, Error> {
    match directory.canonicalize() {
        Ok(dir) => Ok(Some(dir.join("dialogue-replies"))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}
fn verify_directory(path: &Path) -> Result<bool, Error> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => Ok(true),
        Ok(_) => Err(invalid()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}
fn verify_scope(root: &Path, scope: &DialogueReplyScope) -> Result<bool, Error> {
    let Some(data) = read_bounded(&root.join(SCOPE_FILE), MAX_SCOPE_BYTES)? else {
        return Ok(false);
    };
    let stored: DialogueReplyScope = serde_json::from_slice(&data).map_err(|_| invalid())?;
    if &stored != scope {
        return Err(invalid());
    }
    Ok(true)
}
fn read_bounded(path: &Path, max: usize) -> Result<Option<Vec<u8>>, Error> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && metadata.len() <= max as u64 => {}
        Ok(_) => return Err(invalid()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let mut bytes = Vec::new();
    File::open(path)?
        .take(max as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > max {
        return Err(invalid());
    }
    Ok(Some(bytes))
}
fn record_id(name: &str) -> Result<Option<Uuid>, Error> {
    if name == SCOPE_FILE || name.starts_with(".tmp-") {
        return Ok(None);
    }
    let id = name
        .strip_suffix(".reply")
        .and_then(|id| Uuid::parse_str(id).ok())
        .ok_or_else(invalid)?;
    if name != format!("{id}.reply") {
        return Err(invalid());
    }
    Ok(Some(id))
}
fn record_path(root: &Path, id: Uuid) -> PathBuf {
    root.join(format!("{id}.reply"))
}
fn read_record(path: &Path) -> Result<Option<DurableDialogueReply>, Error> {
    read_bounded(path, MAX_REPLY_RECORD_BYTES)?
        .map(|bytes| DurableDialogueReply::from_storage_bytes(&bytes))
        .transpose()
}
fn load_record(
    root: &Path,
    agent_id: &AgentId,
    key: &DialogueAdmissionKey,
) -> Result<Option<DurableDialogueReply>, Error> {
    read_record(&record_path(root, key.id()))?
        .map(|record| {
            record.validate_exact(agent_id, key)?;
            Ok(record)
        })
        .transpose()
}
fn account(
    root: &Path,
    replacing: Option<Uuid>,
    clean_temps: bool,
) -> Result<(usize, usize, usize), Error> {
    let (mut count, mut charge, mut old) = (0, 0, 0);
    for (entries, entry) in fs::read_dir(root)?.enumerate() {
        if entries > microcrab_session::state::communication::inbox::MAX_RECEIPTS + 2 {
            return Err(invalid());
        }
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_str().ok_or_else(invalid)?;
        if clean_temps && name.starts_with(".tmp-") {
            fs::remove_file(entry.path())?;
        }
        let Some(id) = record_id(name)? else {
            continue;
        };
        let metadata = fs::symlink_metadata(entry.path())?;
        if !metadata.is_file() {
            return Err(invalid());
        }
        let entry_charge =
            reply_extra_charge(usize::try_from(metadata.len()).map_err(|_| invalid())?)?;
        count += 1;
        charge += entry_charge;
        if Some(id) == replacing {
            old = entry_charge;
        }
        check_reply_budget(count, charge)?;
    }
    Ok((count, charge, old))
}
fn publish(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    let parent = path.parent().ok_or_else(invalid)?;
    let temp = parent.join(format!(".tmp-{}", Uuid::new_v4()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(temp, path)?;
    #[cfg(test)]
    if FAIL_AFTER_RENAME
        .get_or_init(Mutex::default)
        .lock()
        .map_err(|_| invalid())?
        .take_if(|target| target.as_path() == path)
        .is_some()
    {
        return Err(invalid());
    }
    sync_dir(parent)
}
#[cfg(unix)]
fn sync_dir(path: &Path) -> Result<(), Error> {
    Ok(File::open(path)?.sync_all()?)
}
// Match the existing file Journal's portable Windows durability contract.
#[cfg(not(unix))]
fn sync_dir(_: &Path) -> Result<(), Error> {
    Ok(())
}
