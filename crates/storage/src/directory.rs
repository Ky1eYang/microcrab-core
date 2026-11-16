//! Owner-scoped creator metadata, independent of Journal HEAD and raw audit data.
//! Caller identity is already verified; IDs are map/SQL keys, never filesystem paths.
use async_trait::async_trait;
use microcrab_permission::{Permission, PermissionConfig};
use microcrab_session::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[cfg(feature = "storage-file")]
mod file;
#[cfg(feature = "storage-file")]
pub use file::FileDirectory;
#[cfg(feature = "storage-sqlite")]
mod sqlite;
#[cfg(feature = "storage-sqlite")]
pub use sqlite::SqliteDirectory;
#[cfg(test)]
mod tests;

pub const RECORD_VERSION: u32 = 1;
// Safety ceiling for one session record, aligned with the directory ceiling. The
// former 2 MiB value rejected an ordinary grown record with a save failure that had
// no user action behind it; the record is now bounded only by this ceiling.
pub(crate) const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_DIRECTORY_BYTES: usize = 64 * 1024 * 1024;

/// Public provider catalog identity; connection settings remain creator configuration.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelSelection {
    pub provider_id: String,
    pub model: String,
    #[serde(default)]
    pub variant: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SessionRecord {
    pub version: u32,
    pub id: String,
    pub owner: String,
    pub title: String,
    /// `None` is the explicit projectless namespace; legacy strings deserialize as `Some`.
    #[serde(default)]
    pub project_id: Option<String>,
    pub project_root: String,
    pub agent: Option<String>,
    pub mode: Option<String>,
    #[serde(default)]
    pub model_selection: Option<ModelSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_compaction: Option<microcrab_session::context::AutoCompactionPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_config: Option<microcrab_session::context::system::PromptConfig>,
    pub permissions: Vec<Permission>,
    #[serde(default = "default_initialization")]
    pub initialization: microcrab_permission::Action,
    pub review: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_model: Option<microcrab_permission::ReviewModelSelection>,
    pub revision: u64,
    pub archived: bool,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OperationRecord {
    pub version: u32,
    pub id: String,
    pub session_id: String,
    pub owner: String,
    pub method: String,
    pub payload_hash: String,
    pub status: String,
    pub result: Option<Value>,
    pub revision: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

/// Final creator publication after Runtime has acknowledged its durable Journal
/// cursor. Backends atomically replace session metadata and the terminal receipt;
/// a supplied cursor additionally fences the selected history pointer.
#[derive(Clone, Debug)]
pub struct OperationSettlement {
    pub expected_session_revision: u64,
    pub session: SessionRecord,
    pub expected_operation_revision: u64,
    pub operation: OperationRecord,
    pub journal_cursor: Option<crate::history::JournalCursor>,
}
impl OperationSettlement {
    pub(crate) fn validate(&self, owner: &str) -> Result<(), Error> {
        identity(owner)?;
        self.session.validate()?;
        self.operation.validate()?;
        if self.session.owner != owner
            || self.operation.owner != owner
            || self.operation.session_id != self.session.id
            || matches!(self.operation.status.as_str(), "accepted" | "running")
        {
            return Err(conflict());
        }
        if let Some(cursor) = &self.journal_cursor {
            if cursor.session_id != self.session.id {
                return Err(conflict());
            }
            crate::history::valid_hash(&cursor.checkpoint_id)?;
        }
        Ok(())
    }
    pub(crate) fn check(
        &self,
        owner: &str,
        session: &SessionRecord,
        operation: &OperationRecord,
    ) -> Result<(), Error> {
        self.validate(owner)?;
        session.replacement(owner, self.expected_session_revision, &self.session)?;
        operation.replacement(owner, self.expected_operation_revision, &self.operation)
    }
}

/// Owner-scoped listing fact. Unavailable records remain durable and cannot be
/// mistaken for absent sessions or admitted as current configuration.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "availability", rename_all = "snake_case")]
pub enum SessionEntry {
    Available { record: SessionRecord },
    Unavailable { id: String, reason: String },
}

#[async_trait]
pub trait Directory: Send + Sync {
    /// Embedding applications may report this count at startup without exposing
    /// identities or bodies whose ownership cannot be established.
    async fn unavailable_unowned_count(&self) -> Result<usize, Error> {
        Ok(0)
    }

    /// Storage backends override this to isolate incompatible records. Other
    /// embedders retain their existing all-or-error contract by default.
    async fn list_entries(&self, owner: &str) -> Result<Vec<SessionEntry>, Error> {
        Ok(self
            .list(owner)
            .await?
            .into_iter()
            .map(|record| SessionEntry::Available { record })
            .collect())
    }
    async fn get(&self, owner: &str, id: &str) -> Result<Option<SessionRecord>, Error>;
    async fn list(&self, owner: &str) -> Result<Vec<SessionRecord>, Error>;
    async fn create(&self, record: SessionRecord) -> Result<SessionRecord, Error>;
    async fn replace(
        &self,
        owner: &str,
        expected_revision: u64,
        record: SessionRecord,
    ) -> Result<SessionRecord, Error>;
    async fn get_operation(&self, owner: &str, id: &str) -> Result<Option<OperationRecord>, Error>;
    async fn list_operations(
        &self,
        owner: &str,
        session_id: &str,
    ) -> Result<Vec<OperationRecord>, Error>;
    async fn create_operation(&self, record: OperationRecord) -> Result<OperationRecord, Error>;
    async fn replace_operation(
        &self,
        owner: &str,
        expected_revision: u64,
        record: OperationRecord,
    ) -> Result<OperationRecord, Error>;
    async fn settle_operation(
        &self,
        owner: &str,
        settlement: OperationSettlement,
    ) -> Result<(SessionRecord, OperationRecord), Error>;
    /// Irreversibly remove one session record and every operation receipt that
    /// belongs to it. Session directory bytes stay with the caller: this owns
    /// directory metadata only and returns the removed receipt count.
    async fn remove(&self, owner: &str, id: &str, expected_revision: u64) -> Result<usize, Error>;
    async fn close(&self) -> Result<(), Error>;
}
fn invalid() -> Error {
    Error::config("invalid creator directory record")
}
pub(crate) fn conflict() -> Error {
    Error::Protocol("creator directory revision or ownership conflict".into())
}
fn text(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max && !value.contains('\0')
}
pub(crate) fn identity(value: &str) -> Result<(), Error> {
    if text(value, 1024) {
        Ok(())
    } else {
        Err(invalid())
    }
}
pub(crate) fn decode_session(bytes: &[u8]) -> Result<SessionRecord, Error> {
    let value: Value = serde_json::from_slice(bytes)?;
    if let Some(rules) = value.get("permissions").and_then(Value::as_array) {
        for (index, rule) in rules.iter().enumerate() {
            // These schema identifiers are trusted labels, never rule values.
            // Report before serde's quoted unknown-field text is sanitized.
            for field in [
                "capability",
                "resource",
                "arguments",
                "operation_arguments",
                "conditions",
            ] {
                if rule
                    .as_object()
                    .is_some_and(|object| object.contains_key(field))
                {
                    return Err(Error::Config(format!(
                        "session.permissions rule index {index} contains unsupported legacy field {field}; current rules use tool, args and action"
                    ).into()));
                }
            }
            serde_json::from_value::<Permission>(rule.clone()).map_err(|error| {
                Error::Config(format!("session.permissions[{index}]: {error}").into())
            })?;
        }
    }
    Ok(serde_json::from_value(value)?)
}

fn default_initialization() -> microcrab_permission::Action {
    microcrab_permission::Action::Ask
}

impl SessionRecord {
    pub fn validate(&self) -> Result<(), Error> {
        if self.version != RECORD_VERSION
            || self.revision > i64::MAX as u64
            || !text(&self.id, 256)
            || !text(&self.owner, 1024)
            || self.title.len() > 4096
            || self.title.contains('\0')
            || self.project_id.as_ref().is_some_and(|id| !text(id, 256))
            || !text(&self.project_root, 32768)
            || self.updated_at_ms < self.created_at_ms
            || self.agent.as_ref().is_some_and(|value| !text(value, 256))
            || self.mode.as_ref().is_some_and(|value| !text(value, 256))
        {
            return Err(invalid());
        }
        if self.model_selection.as_ref().is_some_and(|selection| {
            !text(&selection.provider_id, 256)
                || !text(&selection.model, 1024)
                || selection
                    .variant
                    .as_ref()
                    .is_some_and(|value| !text(value, 64))
        }) {
            return Err(invalid());
        }
        if let Some(config) = &self.prompt_config {
            config.validate()?;
        }
        if let Some(policy) = &self.auto_compaction {
            policy.validate().map_err(|_| invalid())?;
        }
        microcrab_permission::validate(&PermissionConfig {
            agent: self.agent.clone().unwrap_or_default(),
            mode: self.mode.clone().unwrap_or_default(),
            overrides: self.permissions.clone(),
            initialization: self.initialization,
            review: self.review,
            review_model: self.review_model.clone(),
            ..Default::default()
        })
        .map_err(|error| Error::Config(format!("stored session permissions: {error}").into()))?;
        if serde_json::to_vec(self)?.len() > MAX_RECORD_BYTES {
            return Err(invalid());
        }
        Ok(())
    }
    pub(crate) fn replacement(&self, owner: &str, revision: u64, next: &Self) -> Result<(), Error> {
        next.validate()?;
        if self.owner != owner
            || next.owner != owner
            || self.id != next.id
            || self.revision != revision
            || revision.checked_add(1) != Some(next.revision)
            || self.project_id != next.project_id
            || self.project_root != next.project_root
            || self.prompt_config != next.prompt_config
            || self.created_at_ms != next.created_at_ms
            || next.updated_at_ms < self.updated_at_ms
        {
            return Err(conflict());
        }
        Ok(())
    }
}
impl OperationRecord {
    pub fn validate(&self) -> Result<(), Error> {
        if self.version != RECORD_VERSION
            || self.revision > i64::MAX as u64
            || !text(&self.id, 256)
            || !text(&self.session_id, 256)
            || !text(&self.owner, 1024)
            || !text(&self.method, 256)
            || self.updated_at_ms < self.created_at_ms
            || !matches!(
                self.status.as_str(),
                "accepted"
                    | "running"
                    | "completed"
                    | "cancelled"
                    | "failed"
                    | "interrupted"
                    | "unknown"
            )
        {
            return Err(invalid());
        }
        crate::history::valid_hash(&self.payload_hash)?;
        if serde_json::to_vec(self)?.len() > MAX_RECORD_BYTES {
            return Err(invalid());
        }
        Ok(())
    }
    pub(crate) fn replacement(&self, owner: &str, revision: u64, next: &Self) -> Result<(), Error> {
        next.validate()?;
        if self.owner != owner
            || next.owner != owner
            || self.id != next.id
            || self.session_id != next.session_id
            || self.method != next.method
            || self.payload_hash != next.payload_hash
            || self.revision != revision
            || revision.checked_add(1) != Some(next.revision)
            || self.created_at_ms != next.created_at_ms
            || next.updated_at_ms < self.updated_at_ms
        {
            return Err(conflict());
        }
        // Terminal receipts are immutable; clients retry by reading the receipt.
        if !matches!(self.status.as_str(), "accepted" | "running") {
            return Err(conflict());
        }
        if self.status == "running" && next.status == "accepted" {
            return Err(conflict());
        }
        Ok(())
    }
}
