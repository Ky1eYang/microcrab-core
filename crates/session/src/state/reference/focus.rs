//! Immutable selected material captured once at delegation start.
use crate::Error;
use crate::state::reference::{Reference, ReferenceContent, ReferenceInfo};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub const MAX_FOCUS_REFERENCES: usize = 32;
pub const MAX_FOCUS_BYTES: usize = 1024 * 1024;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum FocusSelection {
    Section { section_id: String },
    File { path: String },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FocusSnapshot {
    pub info: ReferenceInfo,
    pub body: String,
    pub digest: String,
}
impl FocusSnapshot {
    pub(crate) fn capture(info: ReferenceInfo, body: String) -> Self {
        let digest = format!("{:x}", Sha256::digest(body.as_bytes()));
        Self { info, body, digest }
    }
    pub(crate) fn validate(&self) -> Result<(), Error> {
        crate::budget::check(
            crate::LimitKind::ReferenceBytes,
            self.body.len(),
            MAX_FOCUS_BYTES,
        )?;
        if self.info.id.is_empty()
            || self.info.id.len() > 256
            || self.info.id.contains('\0')
            || self.info.title.len() > 1024
            || self.info.location.len() > 4096
            || self.info.description.len() > 4096
            || format!("{:x}", Sha256::digest(self.body.as_bytes())) != self.digest
        {
            return Err(Error::config(
                "invalid delegated reference snapshot or digest",
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Default)]
pub(crate) struct FocusedReferences(pub Vec<FocusSnapshot>);
impl Reference for FocusedReferences {
    type Error = Error;
    fn kind(&self) -> &str {
        "focused_references"
    }
    async fn load(&self) -> Result<(), Error> {
        validate_snapshots(&self.0)
    }
    fn list(&self) -> Result<Vec<ReferenceInfo>, Error> {
        Ok(self.0.iter().map(|row| row.info.clone()).collect())
    }
    fn read(&self, id: &str) -> Result<Option<ReferenceContent>, Error> {
        Ok(self
            .0
            .iter()
            .find(|row| row.info.id == id)
            .map(|row| ReferenceContent {
                id: row.info.id.clone(),
                body: row.body.clone(),
            }))
    }
    fn change(&self) -> Result<Option<Value>, Error> {
        Ok(Some(json!(
            self.0.iter().map(|row| &row.info).collect::<Vec<_>>()
        )))
    }
}
pub(crate) fn validate_snapshots(snapshots: &[FocusSnapshot]) -> Result<(), Error> {
    crate::budget::check(
        crate::LimitKind::ReferenceCount,
        snapshots.len(),
        MAX_FOCUS_REFERENCES,
    )?;
    let mut ids = std::collections::BTreeSet::new();
    let mut bytes = 0usize;
    for snapshot in snapshots {
        snapshot.validate()?;
        if !ids.insert(&snapshot.info.id) {
            return Err(Error::config("duplicate delegated reference id"));
        }
        bytes = bytes.checked_add(snapshot.body.len()).ok_or_else(|| {
            crate::budget::reject(crate::LimitKind::Arithmetic, "focus byte overflow")
        })?;
        crate::budget::check(crate::LimitKind::ReferenceBytes, bytes, MAX_FOCUS_BYTES)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
