//! Canonical audit bytes belong to Creator. This port owns persistence only;
//! synchronous calls run on its bounded blocking writer or a read-only worker.
use microcrab_session::Error;
use std::{
    io::Read,
    path::{Path, PathBuf},
};
#[cfg(feature = "storage-file")]
mod file;
#[cfg(feature = "storage-sqlite")]
mod sqlite;
#[cfg(feature = "storage-file")]
pub use file::FileAuditStore;
#[cfg(feature = "storage-sqlite")]
pub use sqlite::SqliteAuditStore;

/// Owner and root are checked on every read, including detached child inspection.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AuditScope {
    pub owner: String,
    pub root_session: String,
    pub session: String,
}
impl AuditScope {
    pub fn new(owner: &str, root_session: &str, session: &str) -> Self {
        Self {
            owner: owner.into(),
            root_session: root_session.into(),
            session: session.into(),
        }
    }
    // File authorization is path containment; these SQL key bounds are used
    // only by the SQLite audit and consumption adapters.
    #[cfg(feature = "storage-sqlite")]
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if [&self.owner, &self.root_session, &self.session]
            .iter()
            .any(|s| s.is_empty() || s.len() > 1024)
        {
            return Err(Error::config("invalid audit scope"));
        }
        Ok(())
    }
}

/// A read descriptor never registers a writer or creates database/schema/files.
#[derive(Clone, Debug)]
pub enum AuditSource {
    File(PathBuf),
    Sqlite {
        database: PathBuf,
        scope: AuditScope,
    },
}
impl From<PathBuf> for AuditSource {
    fn from(path: PathBuf) -> Self {
        Self::File(path)
    }
}
impl From<&PathBuf> for AuditSource {
    fn from(path: &PathBuf) -> Self {
        Self::File(path.clone())
    }
}
impl From<&Path> for AuditSource {
    fn from(path: &Path) -> Self {
        Self::File(path.into())
    }
}
impl From<&str> for AuditSource {
    fn from(path: &str) -> Self {
        Self::File(path.into())
    }
}
impl From<String> for AuditSource {
    fn from(path: String) -> Self {
        Self::File(path.into())
    }
}
impl From<&AuditSource> for AuditSource {
    fn from(source: &AuditSource) -> Self {
        source.clone()
    }
}

pub trait AuditStore: Send {
    fn reader(&mut self) -> Result<Box<dyn Read + Send>, Error>;
    /// Must durably commit exact bytes before returning. Head comparison fences
    /// stale owners; an error is sticky at the Creator writer and never replayed.
    fn append(
        &mut self,
        expected_sequence: u64,
        expected_hash: Option<&str>,
        sequence: u64,
        hash: &str,
        record: &[u8],
    ) -> Result<(), Error>;
    fn flush(&mut self) -> Result<(), Error>;
}
impl AuditSource {
    pub fn open(&self) -> Result<Box<dyn AuditStore>, Error> {
        match self {
            Self::File(path) => {
                #[cfg(feature = "storage-file")]
                {
                    Ok(Box::new(FileAuditStore::open(path)?))
                }
                #[cfg(not(feature = "storage-file"))]
                {
                    let _ = path;
                    Err(Error::config("File audit support is not compiled"))
                }
            }
            Self::Sqlite { database, scope } => {
                #[cfg(feature = "storage-sqlite")]
                {
                    Ok(Box::new(SqliteAuditStore::open(database, scope.clone())?))
                }
                #[cfg(not(feature = "storage-sqlite"))]
                {
                    let _ = (database, scope);
                    Err(Error::config("SQLite audit support is not compiled"))
                }
            }
        }
    }
    /// SQLite's transactionally advanced head is a committed cache witness.
    /// File reads are reverified rather than mistaking byte length for commit.
    pub fn revision(&self) -> Result<Option<u64>, Error> {
        match self {
            Self::File(_) => Ok(None),
            Self::Sqlite { database, scope } => {
                #[cfg(feature = "storage-sqlite")]
                {
                    SqliteAuditStore::revision(database, scope)
                }
                #[cfg(not(feature = "storage-sqlite"))]
                {
                    let _ = (database, scope);
                    Err(Error::config("SQLite audit support is not compiled"))
                }
            }
        }
    }
    pub fn reader(&self) -> Result<Option<Box<dyn Read + Send>>, Error> {
        match self {
            Self::File(path) => {
                #[cfg(feature = "storage-file")]
                {
                    FileAuditStore::inspect(path)
                }
                #[cfg(not(feature = "storage-file"))]
                {
                    let _ = path;
                    Err(Error::config("File audit support is not compiled"))
                }
            }
            Self::Sqlite { database, scope } => {
                #[cfg(feature = "storage-sqlite")]
                {
                    SqliteAuditStore::inspect(database, scope)
                }
                #[cfg(not(feature = "storage-sqlite"))]
                {
                    let _ = (database, scope);
                    Err(Error::config("SQLite audit support is not compiled"))
                }
            }
        }
    }
}
