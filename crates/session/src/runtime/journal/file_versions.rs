//! Private bounded owner for Coding version references. This is not a file index.
//! Only explicit Host observations and conditional mutation receipts can publish.
use crate::Error;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub const MAX_FILE_VERSION_BYTES: usize = 2 * 1024 * 1024;
const MAX_ENTRIES: usize = 4096;
const WIDTHS: [usize; 3] = [16, 20, 24];
pub(crate) const REREAD: &str = "file version reference is unknown, stale, or outside this scope; reread the exact file before trying again";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileVersions {
    session: String,
    revision: u64,
    generation: u64,
    salt: uuid::Uuid,
    authority: Option<Authority>,
    entries: BTreeMap<String, Entry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Authority {
    host: String,
    epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    generation: u64,
    path: String,
    version: String,
    observed: bool,
}

pub(crate) fn exact(value: &str) -> bool {
    value.len() == 64 && lower_hex(value)
}

fn lower_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn token(value: &str) -> bool {
    value
        .strip_prefix("fv_")
        .is_some_and(|hex| WIDTHS.contains(&hex.len()) && lower_hex(hex))
}

fn field(value: &str, max: usize) -> bool {
    !value.is_empty() && value.len() <= max && !value.contains('\0')
}

impl FileVersions {
    pub(crate) fn new(session: &str) -> Self {
        Self {
            session: session.into(),
            revision: 0,
            generation: 0,
            salt: uuid::Uuid::new_v4(),
            authority: None,
            entries: BTreeMap::new(),
        }
    }

    pub fn validate(&self, session: &str) -> Result<(), Error> {
        if self.session != session
            || !field(session, 4096)
            || self.salt.is_nil()
            || self.generation > self.revision
            || self.entries.len() > MAX_ENTRIES
            || self
                .authority
                .as_ref()
                .is_some_and(|authority| !field(&authority.host, 8192))
            || self.entries.iter().any(|(alias, entry)| {
                !token(alias)
                    || !field(&entry.path, 4096)
                    || !exact(&entry.version)
                    || entry.generation == 0
                    || entry.generation > self.generation
            })
            || serde_json::to_vec(self)?.len() > MAX_FILE_VERSION_BYTES
        {
            return Err(Error::config(
                "invalid file version sidecar or capacity exceeded",
            ));
        }
        Ok(())
    }

    /// Validate even when both sides have already checked their own storage bounds.
    /// The immutable bindings themselves are tombstones, including unobserved reservations.
    pub fn check_successor(&self, session: &str, prior: Option<&Self>) -> Result<bool, Error> {
        self.validate(session)?;
        if let Some(prior) = prior {
            prior.validate(session)?;
            if self == prior {
                return Ok(false);
            }
            let same_scope =
                self.generation == prior.generation && self.authority == prior.authority;
            let rotated = Some(self.generation) == prior.generation.checked_add(1);
            if self.salt != prior.salt
                || Some(self.revision) != prior.revision.checked_add(1)
                || !(same_scope || rotated)
                || prior.entries.iter().any(|(alias, before)| {
                    self.entries.get(alias).is_none_or(|after| {
                        before.generation != after.generation
                            || before.path != after.path
                            || before.version != after.version
                            || (before.observed && !after.observed)
                            || (before.observed != after.observed
                                && after.generation != self.generation)
                    })
                })
                || self.entries.iter().any(|(alias, entry)| {
                    !prior.entries.contains_key(alias) && entry.generation != self.generation
                })
            {
                return Err(Error::config("file version sidecar revision conflict"));
            }
        } else if self.revision != 1 || self.generation != 1 || self.authority.is_none() {
            return Err(Error::config("invalid initial file version sidecar"));
        }
        Ok(true)
    }

    pub(crate) fn advance(&mut self) -> Result<(), Error> {
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| Error::config("file version revision exhausted"))?;
        Ok(())
    }

    pub(crate) fn bind(&mut self, host: &str, epoch: u64) -> Result<(), Error> {
        if self.matches(host, epoch) {
            return Ok(());
        }
        self.rotate(Some(Authority {
            host: host.into(),
            epoch,
        }))
    }

    fn rotate(&mut self, authority: Option<Authority>) -> Result<(), Error> {
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| Error::config("file version scope exhausted"))?;
        self.authority = authority;
        Ok(())
    }

    pub(super) fn authority_bound(&self) -> bool {
        self.authority.is_some()
    }

    pub(super) fn retire(&mut self) -> Result<(), Error> {
        if self.authority.is_some() {
            self.rotate(None)?;
        }
        Ok(())
    }

    pub(crate) fn matches(&self, host: &str, epoch: u64) -> bool {
        self.authority
            .as_ref()
            .is_some_and(|authority| authority.host == host && authority.epoch == epoch)
    }

    pub(crate) fn resolve(&self, path: &str, alias: &str) -> Result<String, Error> {
        if !token(alias) {
            return Err(Error::tool(REREAD));
        }
        self.entries
            .get(alias)
            .filter(|entry| {
                entry.generation == self.generation && entry.observed && entry.path == path
            })
            .map(|entry| entry.version.clone())
            .ok_or_else(|| Error::tool(REREAD))
    }

    pub(crate) fn issue(
        &mut self,
        path: &str,
        version: &str,
        observed: bool,
    ) -> Result<String, Error> {
        let mut hash = Sha256::new();
        hash.update(b"microcrab.file-version.v1\0");
        hash.update(self.salt.as_bytes());
        hash.update(self.generation.to_be_bytes());
        hash.update(version.as_bytes());
        hash.update(path.as_bytes());
        self.allocate(path, version, observed, &format!("{:x}", hash.finalize()))
    }

    fn allocate(
        &mut self,
        path: &str,
        version: &str,
        observed: bool,
        digest: &str,
    ) -> Result<String, Error> {
        if self.authority.is_none() || !field(path, 4096) || !exact(version) {
            return Err(Error::tool(
                "Host did not return a supported canonical file version/path",
            ));
        }
        for width in WIDTHS {
            let alias = format!("fv_{}", &digest[..width]);
            if let Some(entry) = self.entries.get_mut(&alias) {
                if entry.generation == self.generation
                    && entry.path == path
                    && entry.version == version
                {
                    entry.observed |= observed;
                    return Ok(alias);
                }
                continue;
            }
            if self.entries.len() >= MAX_ENTRIES {
                return Err(Error::tool(
                    "file version ledger is full; start a new Session",
                ));
            }
            self.entries.insert(
                alias.clone(),
                Entry {
                    generation: self.generation,
                    path: path.into(),
                    version: version.into(),
                    observed,
                },
            );
            return Ok(alias);
        }
        Err(Error::tool(
            "file version reference collision exceeded its bound; start a new Session",
        ))
    }

    pub(crate) fn confirm(&mut self, path: &str, version: &str, alias: &str) -> Result<(), Error> {
        let entry = self
            .entries
            .get_mut(alias)
            .filter(|entry| {
                entry.generation == self.generation
                    && entry.path == path
                    && entry.version == version
            })
            .ok_or_else(|| Error::tool("Host result did not match the reserved file version"))?;
        entry.observed = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reservation_alone_is_never_evidence_even_after_reopen() {
        let mut ledger = FileVersions::new("session");
        ledger.bind("host", 1).unwrap();
        let exact = "b".repeat(64);
        let planned = ledger.issue("file", &exact, false).unwrap();
        ledger.advance().unwrap();
        let bytes = serde_json::to_vec(&ledger).unwrap();
        let mut recovered: FileVersions = serde_json::from_slice(&bytes).unwrap();
        assert!(recovered.resolve("file", &planned).is_err());
        assert!(
            recovered
                .confirm("file", &"c".repeat(64), &planned)
                .is_err()
        );
        assert!(recovered.resolve("file", &planned).is_err());
        assert_eq!(recovered.issue("file", &exact, true).unwrap(), planned);
        assert_eq!(recovered.resolve("file", &planned).unwrap(), exact);
    }

    #[test]
    fn collisions_never_retarget_even_after_rotation() {
        let mut ledger = FileVersions::new("session");
        ledger.bind("host", 1).unwrap();
        let collision = "a".repeat(64);
        let one = ledger.allocate("a", &collision, true, &collision).unwrap();
        let two = ledger.allocate("b", &collision, true, &collision).unwrap();
        assert_eq!(one.len(), 19);
        assert_eq!(two.len(), 23);
        assert!(ledger.resolve("b", &one).is_err());
        ledger.retire().unwrap();
        ledger.bind("host", 1).unwrap();
        assert!(ledger.resolve("a", &one).is_err());
        let three = ledger.allocate("a", &collision, false, &collision).unwrap();
        assert_eq!(three.len(), 27);
        assert!(ledger.resolve("a", &three).is_err());
        ledger.confirm("a", &collision, &three).unwrap();
        assert_eq!(ledger.resolve("a", &three).unwrap(), collision);
        assert!(ledger.allocate("c", &collision, true, &collision).is_err());
    }
}
