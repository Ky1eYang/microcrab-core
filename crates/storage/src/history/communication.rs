//! Mutable communication receipts are independent from immutable conversation travel.
use super::*;
use microcrab_session::state::communication::{AgentId, inbox::DurableInbox};
use serde::{Deserialize, Serialize};
pub(super) const MAX_INBOX_STORAGE_BYTES: usize = 4 * 1024 * 1024;
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredInbox {
    pub session_id: String,
    pub agent_id: AgentId,
    pub inbox: DurableInbox,
}
impl StoredInbox {
    pub fn validate(&self, session_id: &str, agent_id: &AgentId) -> Result<(), Error> {
        if self.session_id != session_id || &self.agent_id != agent_id {
            return Err(Error::config("communication sidecar identity mismatch"));
        }
        self.inbox.validate()?;
        if serde_json::to_vec(self)?.len() > MAX_INBOX_STORAGE_BYTES {
            return Err(Error::Limit(
                "communication sidecar byte budget exceeded".into(),
            ));
        }
        Ok(())
    }
    pub fn check_next(&self, previous: Option<&Self>) -> Result<bool, Error> {
        self.validate(&self.session_id, &self.agent_id)?;
        if let Some(prior) = previous {
            prior.validate(&self.session_id, &self.agent_id)?;
        }
        self.inbox
            .check_successor(previous.map(|prior| &prior.inbox))
    }
}
pub(super) fn reconcile_inspected(
    mut stored: StoredInbox,
    session_id: &str,
    agent_id: &AgentId,
    recover: Option<&Recover>,
) -> Result<DurableInbox, Error> {
    stored.validate(session_id, agent_id)?;
    if let Some(communication) = recover.and_then(|value| value.state.communication.as_ref()) {
        if &communication.identity.agent_id != agent_id {
            return Err(Error::config(
                "selected Recover communication identity mismatch",
            ));
        }
        for selected in &communication.inbox.receipts {
            if selected.receipt.status
                == microcrab_session::state::communication::inbox::DeliveryStatus::Consumed
                && let Some(receipt) = stored
                    .inbox
                    .receipts
                    .iter_mut()
                    .find(|row| row.receipt.operation_id == selected.receipt.operation_id)
            {
                if !receipt.same_admission(selected) {
                    return Err(Error::config(
                        "durable carrier consumption witness mismatch",
                    ));
                }
                receipt.receipt.status = selected.receipt.status.clone();
            }
        }
        stored.inbox.pending.retain(|entry| {
            stored.inbox.receipts.iter().any(|row| {
                row.receipt.operation_id == entry.operation_id
                    && row.receipt.status
                        == microcrab_session::state::communication::inbox::DeliveryStatus::Queued
            })
        });
    }
    stored.inbox.validate()?;
    Ok(stored.inbox)
}
impl CreatorJournal {
    pub(super) async fn store_inbox(
        &self,
        agent_id: &AgentId,
        inbox: DurableInbox,
    ) -> Result<(), Error> {
        let stored = StoredInbox {
            session_id: self.session_id.clone(),
            agent_id: agent_id.clone(),
            inbox,
        };
        stored.validate(&self.session_id, agent_id)?;
        match self.backend.as_ref() {
            Backend::Memory(backend) => backend.store_inbox(stored).await,
            #[cfg(feature = "storage-file")]
            Backend::Disk(backend) => backend.store_inbox(stored).await,
            #[cfg(feature = "storage-sqlite")]
            Backend::Sqlite(backend) => backend.store_inbox(stored).await,
        }
    }
    pub(super) async fn read_inbox(
        &self,
        agent_id: &AgentId,
    ) -> Result<Option<DurableInbox>, Error> {
        let stored = match self.backend.as_ref() {
            Backend::Memory(backend) => backend.read_inbox().await?,
            #[cfg(feature = "storage-file")]
            Backend::Disk(backend) => backend.read_inbox().await?,
            #[cfg(feature = "storage-sqlite")]
            Backend::Sqlite(backend) => backend.read_inbox().await?,
        };
        stored
            .map(|stored| {
                stored.validate(&self.session_id, agent_id)?;
                Ok(stored.inbox)
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests;
