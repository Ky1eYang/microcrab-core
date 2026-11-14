//! Complete display-only observation bytes. No model-facing history authority.
//! Page limits bound record counts, never the size or cumulative retained body.
use microcrab_session::Error;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
#[cfg(feature = "storage-file")]
mod file;
mod graph;
#[cfg(feature = "storage-sqlite")]
mod sqlite;
pub use graph::{DisplayBlock, DisplayChunk, DisplayGraph, DisplayHeadTransition};
#[cfg(test)]
mod tests;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DisplayScope {
    pub owner: String,
    pub root_session: String,
    pub session: String,
}
impl DisplayScope {
    pub fn new(owner: &str, root_session: &str, session: &str) -> Self {
        Self {
            owner: owner.into(),
            root_session: root_session.into(),
            session: session.into(),
        }
    }
    fn validate(&self) -> Result<(), Error> {
        if [&self.owner, &self.root_session, &self.session]
            .iter()
            .any(|s| s.is_empty() || s.contains('\0'))
        {
            return Err(failure("invalid display scope"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveRecord {
    pub sequence: u64,
    pub attempt_id: String,
    pub event: serde_json::Value,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DisplayPage {
    pub records: Vec<ArchiveRecord>,
    pub high_water: u64,
    pub next_cursor: Option<u64>,
}
#[derive(Clone, Debug)]
pub enum DisplaySource {
    File {
        directory: PathBuf,
        scope: DisplayScope,
    },
    Sqlite {
        database: PathBuf,
        scope: DisplayScope,
    },
}
pub trait DisplayStore: Send {
    /// Exact expected-head append. Success means the complete record is durable.
    fn append(&mut self, expected_sequence: u64, record: &ArchiveRecord) -> Result<(), Error>;
    fn revision(&mut self) -> Result<u64, Error>;
    fn flush(&mut self) -> Result<(), Error>;
}
fn failure(message: &'static str) -> Error {
    Error::audit(message)
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Head {
    version: u32,
    scope: DisplayScope,
    sequence: u64,
    hash: Option<String>,
}
impl Head {
    fn empty(scope: DisplayScope) -> Self {
        Self {
            version: 1,
            scope,
            sequence: 0,
            hash: None,
        }
    }
    fn validate(&self, scope: &DisplayScope) -> Result<(), Error> {
        if self.version != 1 || &self.scope != scope || (self.sequence == 0) != self.hash.is_none()
        {
            return Err(failure("display committed head/scope mismatch"));
        }
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    previous: Option<String>,
    hash: String,
    record: ArchiveRecord,
}
fn hash(previous: Option<&str>, record: &ArchiveRecord) -> Result<String, Error> {
    let bytes = serde_json::to_vec(&(previous, record))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}
fn validate_append(expected: u64, head: &Head, record: &ArchiveRecord) -> Result<(), Error> {
    if expected != head.sequence
        || expected.checked_add(1) != Some(record.sequence)
        || record.attempt_id.is_empty()
    {
        return Err(failure("display sequence conflict"));
    }
    Ok(())
}
fn check(envelope: &Envelope, sequence: u64, previous: Option<&str>) -> Result<(), Error> {
    if envelope.record.sequence != sequence
        || envelope.previous.as_deref() != previous
        || envelope.record.attempt_id.is_empty()
        || hash(previous, &envelope.record)? != envelope.hash
    {
        return Err(failure("display record integrity/order mismatch"));
    }
    Ok(())
}
fn page_bounds(head: &Head, after: u64, high: Option<u64>, limit: usize) -> Result<u64, Error> {
    let high = high.unwrap_or(head.sequence);
    if limit == 0 || after > high || high > head.sequence {
        return Err(failure("invalid display page cursor"));
    }
    Ok(high)
}
impl DisplaySource {
    /// Reconstruct metadata from bounded pages of the immutable archive, without Context.
    pub fn graph(&self) -> Result<Option<DisplayGraph>, Error> {
        self.graph_at(None)
    }
    pub fn graph_at(&self, high: Option<u64>) -> Result<Option<DisplayGraph>, Error> {
        let scope = match self {
            Self::File { scope, .. } | Self::Sqlite { scope, .. } => scope.clone(),
        };
        let mut graph = DisplayGraph::empty(scope);
        let mut after = 0;
        let mut bound = high;
        loop {
            let Some(page) = self.query(after, bound, 128)? else {
                return Ok(None);
            };
            bound = Some(page.high_water);
            for record in &page.records {
                graph.apply(record)?;
            }
            match page.next_cursor {
                Some(next) => after = next,
                None => return Ok(Some(graph)),
            }
        }
    }
    pub fn open(&self) -> Result<Box<dyn DisplayStore>, Error> {
        match self {
            Self::File { directory, scope } => {
                #[cfg(feature = "storage-file")]
                {
                    Ok(Box::new(file::Store::open(directory, scope)?))
                }
                #[cfg(not(feature = "storage-file"))]
                {
                    let _ = (directory, scope);
                    Err(failure("file display support is not compiled"))
                }
            }
            Self::Sqlite { database, scope } => {
                #[cfg(feature = "storage-sqlite")]
                {
                    Ok(Box::new(sqlite::Store::open(database, scope)?))
                }
                #[cfg(not(feature = "storage-sqlite"))]
                {
                    let _ = (database, scope);
                    Err(failure("SQLite display support is not compiled"))
                }
            }
        }
    }
    /// Read-only inspection does not create a writer, schema, directory or file.
    pub fn query(
        &self,
        after: u64,
        high_water: Option<u64>,
        limit: usize,
    ) -> Result<Option<DisplayPage>, Error> {
        match self {
            Self::File { directory, scope } => {
                #[cfg(feature = "storage-file")]
                {
                    file::query(directory, scope, after, high_water, limit)
                }
                #[cfg(not(feature = "storage-file"))]
                {
                    let _ = (directory, scope, after, high_water, limit);
                    Err(failure("file display support is not compiled"))
                }
            }
            Self::Sqlite { database, scope } => {
                #[cfg(feature = "storage-sqlite")]
                {
                    sqlite::query(database, scope, after, high_water, limit)
                }
                #[cfg(not(feature = "storage-sqlite"))]
                {
                    let _ = (database, scope, after, high_water, limit);
                    Err(failure("SQLite display support is not compiled"))
                }
            }
        }
    }
    /// Metadata only: this validates scope/head format, not every stored body.
    /// Body integrity is checked on requested pages and fully on writer reopen.
    pub fn revision(&self) -> Result<Option<u64>, Error> {
        match self {
            Self::File { directory, scope } => {
                #[cfg(feature = "storage-file")]
                {
                    file::revision(directory, scope)
                }
                #[cfg(not(feature = "storage-file"))]
                {
                    let _ = (directory, scope);
                    Err(failure("file display support is not compiled"))
                }
            }
            Self::Sqlite { database, scope } => {
                #[cfg(feature = "storage-sqlite")]
                {
                    sqlite::revision(database, scope)
                }
                #[cfg(not(feature = "storage-sqlite"))]
                {
                    let _ = (database, scope);
                    Err(failure("SQLite display support is not compiled"))
                }
            }
        }
    }
}
