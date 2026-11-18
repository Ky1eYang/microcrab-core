//! Shared in-process latest recovery value, without file or history semantics.
use std::sync::{Arc, Mutex};

use super::{Journal, Recover};
use crate::Error;

#[derive(Clone, Default)]
pub struct MemoryJournal {
    latest: Arc<Mutex<Option<Recover>>>,
    inboxes: Arc<
        Mutex<
            std::collections::BTreeMap<
                crate::state::communication::AgentId,
                crate::state::communication::inbox::DurableInbox,
            >,
        >,
    >,
}

impl MemoryJournal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn latest(&self) -> Option<Recover> {
        self.latest
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn store(&self, recover: Recover) -> Result<(), Error> {
        recover.validate()?;
        *self.latest.lock().unwrap_or_else(|e| e.into_inner()) = Some(recover);
        Ok(())
    }
}

#[async_trait::async_trait]
impl Journal for MemoryJournal {
    fn supports_communication(&self) -> bool {
        true
    }
    async fn commit_communication(
        &self,
        agent_id: &crate::state::communication::AgentId,
        inbox: crate::state::communication::inbox::DurableInbox,
    ) -> Result<(), Error> {
        inbox.validate()?;
        let mut inboxes = self
            .inboxes
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !inbox.check_successor(inboxes.get(agent_id))? {
            return Ok(());
        }
        inboxes.insert(agent_id.clone(), inbox);
        Ok(())
    }
    async fn load_communication(
        &self,
        agent_id: &crate::state::communication::AgentId,
    ) -> Result<Option<crate::state::communication::inbox::DurableInbox>, Error> {
        Ok(self
            .inboxes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(agent_id)
            .cloned())
    }
    async fn commit(&self, recover: Recover) -> Result<(), Error> {
        self.store(recover)
    }

    async fn has_selected_history(&self) -> bool {
        self.latest().is_some()
    }

    async fn recovered(&self, recover: Recover) -> Result<(), Error> {
        self.store(recover)
    }
}
