//! Explicit file alternative. Existing canonical lines and incomplete tails are
//! never repaired or truncated. One OS lock owns the append lane across reopen.
use super::AuditStore;
use microcrab_session::Error;
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

pub struct FileAuditStore {
    directory: PathBuf,
    file: File,
    _lock: File,
}
fn failure(message: &'static str) -> Error {
    Error::audit(message)
}
fn checked_path(directory: &Path, name: &str) -> Result<PathBuf, Error> {
    let path = directory.join(name);
    if path.try_exists()?
        && (fs::symlink_metadata(&path)?.file_type().is_symlink()
            || path.canonicalize()?.parent() != Some(directory))
    {
        return Err(failure("audit path escapes session directory"));
    }
    Ok(path)
}
impl FileAuditStore {
    pub fn open(directory: &Path) -> Result<Self, Error> {
        fs::create_dir_all(directory)?;
        let directory = directory.canonicalize()?;
        for name in [
            "request",
            "events.jsonl",
            "plugin-audit.jsonl",
            "plugin-audit",
        ] {
            if directory.join(name).try_exists()? {
                return Err(failure("legacy audit format; evidence is unchanged"));
            }
        }
        let path = checked_path(&directory, "audit.jsonl")?;
        let lock_path = checked_path(&directory, ".audit.lock")?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;
        lock.try_lock()
            .map_err(|_| failure("audit already has an exclusive writer"))?;
        if path.try_exists()? && fs::metadata(&path)?.len() == 0 {
            return Err(failure("empty audit file; evidence is unchanged"));
        }
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;
        Ok(Self {
            directory,
            file,
            _lock: lock,
        })
    }
    pub fn inspect(directory: &Path) -> Result<Option<Box<dyn Read + Send>>, Error> {
        let path = directory.join("audit.jsonl");
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        checked_path(&directory.canonicalize()?, "audit.jsonl")?;
        let length = file.metadata()?.len();
        if length > 256 * 1024 * 1024 {
            return Err(failure("audit query scan budget exceeded"));
        }
        Ok(Some(Box::new(file.take(length))))
    }
}
impl AuditStore for FileAuditStore {
    fn reader(&mut self) -> Result<Box<dyn Read + Send>, Error> {
        Ok(Box::new(File::open(self.directory.join("audit.jsonl"))?))
    }
    fn append(
        &mut self,
        _: u64,
        _: Option<&str>,
        _: u64,
        _: &str,
        record: &[u8],
    ) -> Result<(), Error> {
        self.file.write_all(record)?;
        self.file.flush()?;
        self.file.sync_all()?;
        Ok(())
    }
    fn flush(&mut self) -> Result<(), Error> {
        self.file.sync_all()?;
        Ok(())
    }
}
