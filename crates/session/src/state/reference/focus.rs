//! Delegated focus material captured once at delegation start, implementing the unified Reference trait.
use crate::Error;
use async_trait::async_trait;
use microcrab_host_api::{
    Reference, ReferenceCapabilities, ReferenceContent, ReferenceEntry, ReferenceError,
    ReferenceInfo, ReferenceNodeKind, ReferenceProjection,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

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

#[async_trait]
impl Reference for FocusedReferences {
    fn kind(&self) -> &str {
        "focused_references"
    }

    fn capabilities(&self) -> ReferenceCapabilities {
        ReferenceCapabilities {
            list: true,
            expand: true,
            add: false,
            update: false,
            remove: false,
            refresh: false,
            quota: false,
        }
    }

    async fn load(&self, cancel: CancellationToken) -> Result<(), ReferenceError> {
        if cancel.is_cancelled() {
            return Err(ReferenceError::cancelled());
        }
        validate_snapshots(&self.0).map_err(|e| ReferenceError::config(e.to_string()))
    }

    fn list(&self) -> Result<Vec<ReferenceEntry>, ReferenceError> {
        Ok(self
            .0
            .iter()
            .map(|row| ReferenceEntry {
                id: row.info.id.clone(),
                node_kind: ReferenceNodeKind::Section,
                parent_id: None,
                title: row.info.title.clone(),
                location: (!row.info.location.is_empty()).then(|| row.info.location.clone()),
                description: (!row.info.description.is_empty())
                    .then(|| row.info.description.clone()),
                media_type: Some("text/plain".into()),
                revision: row.digest[..16.min(row.digest.len())].to_owned(),
                writable: false,
                metadata: None,
            })
            .collect())
    }

    fn projections(&self) -> Result<ReferenceProjection, ReferenceError> {
        let value = json!(self.0.iter().map(|row| &row.info).collect::<Vec<_>>());
        Ok(ReferenceProjection {
            exact: Some(value),
            short_model: None,
        })
    }

    async fn expand(
        &self,
        id: &str,
        cancel: CancellationToken,
    ) -> Result<Option<ReferenceContent>, ReferenceError> {
        if cancel.is_cancelled() {
            return Err(ReferenceError::cancelled());
        }
        Ok(self
            .0
            .iter()
            .find(|row| row.info.id == id)
            .map(|row| ReferenceContent::text(row.info.id.clone(), row.body.clone())))
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
