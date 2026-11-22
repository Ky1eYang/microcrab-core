//! Private reply storage. The embedding owner must authorize the exact scope,
//! stop activations before purge, and prevent reactivation of deleted scopes.
//! Read descriptors do not acquire writer leases or create storage.
#[cfg(feature = "storage-file")]
mod file;
#[cfg(feature = "storage-sqlite")]
mod sqlite;
#[cfg(feature = "storage-file")]
pub use file::FileDialogueReplies;
#[cfg(feature = "storage-sqlite")]
pub(crate) use sqlite::RootPurgeGuard;
#[cfg(feature = "storage-sqlite")]
pub use sqlite::SqliteDialogueReplies;

use microcrab_session::{Error, state::communication::AgentId};
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DialogueReplyScope {
    owner: String,
    root_session: String,
    session: String,
    agent_id: AgentId,
}
impl DialogueReplyScope {
    pub fn new(
        owner: &str,
        root_session: &str,
        session: &str,
        agent_id: AgentId,
    ) -> Result<Self, Error> {
        if [owner, root_session, session]
            .iter()
            .any(|s| s.is_empty() || s.len() > 1024 || s.chars().any(char::is_control))
        {
            return Err(invalid());
        }
        Ok(Self {
            owner: owner.into(),
            root_session: root_session.into(),
            session: session.into(),
            agent_id,
        })
    }
    pub fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }
}
fn invalid() -> Error {
    Error::protocol("invalid private dialogue reply storage")
}

#[cfg(any(feature = "storage-file", feature = "storage-sqlite"))]
fn uncertain_write(error: Error) -> Error {
    let mut diagnostic = error.tool_diagnostic();
    diagnostic.effects =
        microcrab_session::observation::diagnostics::EffectKnowledge::MayHaveOccurred;
    diagnostic.retryable = Some(false);
    Error::Diagnostic(diagnostic)
}

#[cfg(all(test, any(feature = "storage-file", feature = "storage-sqlite")))]
mod tests;
