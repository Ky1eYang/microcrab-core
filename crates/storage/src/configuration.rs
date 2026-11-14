//! Revisioned owner-scoped configuration records. A reset is a tombstone, so an
//! old client cannot recreate a removed secret or default with a stale revision.
use async_trait::async_trait;
use microcrab_session::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(feature = "storage-file")]
mod file;
#[cfg(feature = "storage-file")]
pub use file::FileConfiguration;
#[cfg(feature = "storage-sqlite")]
mod sqlite;
#[cfg(feature = "storage-sqlite")]
pub use sqlite::SqliteConfiguration;
#[cfg(test)]
mod tests;

pub const MAX_CONFIGURATION_BYTES: usize = 1024 * 1024;
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigurationRecord {
    pub owner: String,
    pub kind: String,
    pub target: String,
    pub revision: u64,
    pub value: Option<Value>,
}
#[async_trait]
pub trait Configuration: Send + Sync {
    async fn close(&self) -> Result<(), Error>;
    async fn get(
        &self,
        owner: &str,
        kind: &str,
        target: &str,
    ) -> Result<Option<ConfigurationRecord>, Error>;
    async fn list(&self, owner: &str, kind: &str) -> Result<Vec<ConfigurationRecord>, Error>;
    /// Publication is durable before acknowledgement. The operation changes only
    /// this exact record; it never replaces the caller's complete configuration.
    async fn compare_exchange(
        &self,
        owner: &str,
        kind: &str,
        target: &str,
        expected_revision: u64,
        value: Option<Value>,
    ) -> Result<ConfigurationRecord, Error>;
}
pub(crate) fn validate(
    owner: &str,
    kind: &str,
    target: &str,
    value: &Option<Value>,
) -> Result<(), Error> {
    for (name, text, max) in [
        ("owner", owner, 1024),
        ("kind", kind, 64),
        ("target", target, 1024),
    ] {
        if text.is_empty() || text.len() > max || text.contains('\0') {
            return Err(Error::Config(
                format!("configuration {name} is empty or exceeds its identity bound").into(),
            ));
        }
    }
    if serde_json::to_vec(value)?.len() > MAX_CONFIGURATION_BYTES {
        return Err(Error::config("configuration record exceeds 1 MiB"));
    }
    Ok(())
}
pub(crate) fn conflict() -> Error {
    Error::Protocol("configuration revision conflict; reload before saving".into())
}
pub(crate) fn next(
    owner: &str,
    kind: &str,
    target: &str,
    expected: u64,
    current: Option<&ConfigurationRecord>,
    value: Option<Value>,
) -> Result<ConfigurationRecord, Error> {
    validate(owner, kind, target, &value)?;
    if current.map_or(0, |r| r.revision) != expected {
        return Err(conflict());
    }
    Ok(ConfigurationRecord {
        owner: owner.into(),
        kind: kind.into(),
        target: target.into(),
        revision: expected.checked_add(1).ok_or_else(conflict)?,
        value,
    })
}
