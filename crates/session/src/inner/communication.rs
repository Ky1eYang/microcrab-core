//! Session-owned lineage binding and per-submission current-mode authority.
mod exchange;
mod inbox;
mod reattach;
pub(super) mod reply;
use super::*;
use crate::runtime::messaging::AgentPeer;
use crate::state::communication::{
    ConversationIdentity, Correspondent, CorrespondentRelation, SessionInteraction,
};

#[async_trait::async_trait]
impl AgentPeer for SessionInner {
    async fn send_message(
        &self,
        arguments: Value,
        invocation: crate::runtime::tool::ToolContext,
    ) -> Result<crate::state::communication::inbox::QueuedReceipt, Error> {
        self.send_message_owned(arguments, invocation).await
    }
    async fn escalate(
        &self,
        arguments: Value,
        invocation: crate::runtime::tool::ToolContext,
    ) -> Result<crate::state::communication::inbox::QueuedReceipt, Error> {
        self.escalate_owned(arguments, invocation).await
    }
    async fn reply_external_speaker(
        &self,
        arguments: Value,
        invocation: crate::runtime::tool::ToolContext,
    ) -> Result<crate::runtime::tool::ToolOutput, Error> {
        self.reply_external_speaker_owned(arguments, invocation)
            .await
    }
    async fn enqueue_authenticated(
        &self,
        carrier: crate::state::communication::inbox::AcceptedCarrier,
    ) -> Result<crate::state::communication::inbox::QueuedReceipt, Error> {
        self.self_weak
            .upgrade()
            .ok_or(Error::Closed)?
            .enqueue_carrier(carrier)
            .await
    }
    fn communication_identity(&self) -> Option<ConversationIdentity> {
        self.state().communication.map(|value| value.identity)
    }
    fn session_id(&self) -> String {
        self.state.processes().session_id().to_owned()
    }
    fn agent_name(&self) -> String {
        self.agent
            .as_ref()
            .map(|agent| agent.name.clone())
            .unwrap_or_default()
    }
    fn is_available(&self) -> bool {
        self.ensure_open().is_ok()
    }
}
impl SessionInner {
    pub(crate) fn communication_directory(
        &self,
    ) -> Arc<crate::runtime::messaging::LineageDirectory> {
        self.directory
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
    pub(super) fn configure_unstarted_conversation_tools(
        &self,
        identity: Option<&ConversationIdentity>,
        mode: Option<&str>,
    ) -> Result<(), Error> {
        let old = self.context_snapshot()?;
        if self.started.load(Ordering::Acquire) || !old.items.is_empty() {
            return Err(Error::config("prepared child already started its Context"));
        }
        self.runtime.configure_conversation_tools(identity, false)?;
        *self
            .context
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Context::new(
            old.system,
            self.runtime.definitions(),
            self.state().limits.max_context_bytes,
        )?;
        self.install_policy_mode(mode)?;
        self.bind_frozen_tools(&self.runtime.definitions());
        Ok(())
    }
    pub(super) fn register_communication_self(&self) -> Result<(), Error> {
        if self.state().communication.is_some() {
            let peer: std::sync::Weak<dyn AgentPeer> = self.self_weak.clone();
            self.state.install_messaging_peer(peer.clone());
            self.communication_directory().register(peer)?;
        }
        Ok(())
    }
    pub(crate) fn current_agent_role(&self) -> Result<microcrab_agent_types::AgentRole, Error> {
        self.agent_role_for_snapshot(&self.state())
    }
    fn agent_role_for_snapshot(
        &self,
        snapshot: &SessionState,
    ) -> Result<microcrab_agent_types::AgentRole, Error> {
        match &self.agent {
            None => Ok(microcrab_agent_types::AgentRole::Both),
            Some(agent) => agent
                .modes
                .iter()
                .find(|mode| Some(mode.name.as_str()) == snapshot.selected_mode.as_deref())
                .map(|mode| mode.role)
                .ok_or_else(|| Error::config("current Agent mode is unavailable")),
        }
    }
    pub(crate) fn require_user_input(&self, targeted_child: bool) -> Result<(), Error> {
        self.require_user_input_snapshot(&self.state(), targeted_child)
    }
    pub(super) fn require_user_input_snapshot(
        &self,
        snapshot: &SessionState,
        targeted_child: bool,
    ) -> Result<(), Error> {
        self.ensure_open()?;
        if self.kind == crate::SessionKind::OneShot {
            return Err(Error::denied(
                "functional Sessions do not accept user submissions",
            ));
        }
        let Some(communication) = snapshot.communication.as_ref() else {
            return if self.kind == crate::SessionKind::Interactive {
                Ok(())
            } else {
                Err(Error::denied(
                    "legacy derived Session requires its parent control port",
                ))
            };
        };
        let role = self.agent_role_for_snapshot(snapshot)?;
        use microcrab_agent_types::AgentRole;
        let allowed = communication.identity.interaction == SessionInteraction::Persistent
            && match communication.identity.depth {
                0 => matches!(role, AgentRole::MainAgent | AgentRole::Both),
                1 => {
                    targeted_child
                        && role == AgentRole::Both
                        && communication.identity.parent_session_id.is_some()
                }
                _ => false,
            };
        if !allowed {
            return Err(Error::denied(
                "current mode and exact targeted-child authority do not allow user input",
            ));
        }
        Ok(())
    }
    pub(crate) fn owned_child_by_id(&self, id: &str) -> Result<crate::Session, Error> {
        self.ensure_open()?;
        let children = self
            .delegated_children
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let child = children
            .get(id)
            .ok_or_else(|| Error::denied("child is not owned by this exact parent Session"))?;
        if child
            .child
            .parent_relationship()
            .as_ref()
            .is_none_or(|relationship| {
                relationship.parent_session_id != self.state.processes().session_id()
            })
        {
            return Err(Error::denied("child parent identity no longer matches"));
        }
        Ok(child.child.clone())
    }
    pub(super) fn require_derivation(&self) -> Result<(), Error> {
        if self.kind == crate::SessionKind::OneShot
            || self
                .state()
                .communication
                .as_ref()
                .is_some_and(|communication| communication.identity.depth != 0)
            || !self.is_main
        {
            return Err(Error::denied(
                "only a root conversational Session may derive a child",
            ));
        }
        Ok(())
    }
    pub(super) fn bind_child_communication(&self, child: &crate::Session) -> Result<(), Error> {
        self.require_derivation()?;
        let Some(mut parent) = self.state().communication else {
            let mode = child.state().selected_mode;
            child
                .inner
                .configure_unstarted_conversation_tools(None, mode.as_deref())?;
            child
                .inner
                .state
                .update(|snapshot| snapshot.communication = None);
            *child
                .inner
                .directory
                .write()
                .unwrap_or_else(|error| error.into_inner()) =
                Arc::new(crate::runtime::messaging::LineageDirectory::default());
            return Ok(());
        };
        let mut derived = child.state().communication.ok_or_else(|| {
            Error::config("new parent requires a new child communication identity")
        })?;
        if derived.identity.agent_id == parent.identity.agent_id {
            return Err(Error::config(
                "parent and child communication identities collide",
            ));
        }
        if derived.identity.parent_session_id.is_some() {
            return Err(Error::config("prepared child is already bound to a parent"));
        }
        derived.identity.depth = 1;
        derived.identity.lineage = parent.identity.lineage.clone();
        derived.identity.parent_session_id = Some(self.state.processes().session_id().to_owned());
        derived.identity.parent_agent_id = Some(parent.identity.agent_id.clone());
        derived.correspondents = vec![Correspondent {
            agent_id: parent.identity.agent_id.clone(),
            agent: self.agent_name(),
            relation: CorrespondentRelation::Parent,
            interaction: parent.identity.interaction,
        }];
        parent.correspondents.push(Correspondent {
            agent_id: derived.identity.agent_id.clone(),
            agent: child.inner.agent_name(),
            relation: CorrespondentRelation::Child,
            interaction: derived.identity.interaction,
        });
        parent.validate()?;
        derived.validate()?;
        self.state
            .set_correspondent_active(derived.identity.agent_id.clone(), true);
        child
            .inner
            .state
            .set_correspondent_active(parent.identity.agent_id.clone(), true);
        child
            .inner
            .state
            .update(|snapshot| snapshot.communication = Some(derived));
        let peer: std::sync::Weak<dyn AgentPeer> = child.inner.self_weak.clone();
        self.communication_directory().register(peer)?;
        let peer: std::sync::Weak<dyn AgentPeer> = self.self_weak.clone();
        *child
            .inner
            .directory
            .write()
            .unwrap_or_else(|error| error.into_inner()) = self.communication_directory();
        child.inner.communication_directory().register(peer)?;
        self.state
            .update(|snapshot| snapshot.communication = Some(parent));
        Ok(())
    }
    pub(super) fn remove_child_communication(&self, child_id: &str) {
        // Accepted carriers retain their own authenticated disclosure witness.
        let child = self
            .delegated_children
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(child_id)
            .and_then(|entry| entry.child.communication_identity());
        if let Some(child) = child {
            self.communication_directory().unregister(&child.agent_id);
            self.state
                .set_correspondent_active(child.agent_id.clone(), false);
            self.state.update(|snapshot| {
                if let Some(communication) = &mut snapshot.communication {
                    communication.correspondents.retain(|row| {
                        row.agent_id != child.agent_id
                            || row.interaction == SessionInteraction::Persistent
                    });
                }
            });
        }
    }
    pub(super) async fn interrupt_communication(&self) {
        let directory = self.communication_directory();
        if let Some(identity) = self.communication_identity() {
            directory.unregister(&identity.agent_id);
            self.state.clear_active_correspondents();
            let mut ledger = directory.exchanges.lock().await;
            ledger.exchanges.retain(|row| {
                row.sender != identity.agent_id && row.recipient != identity.agent_id
            });
        }
        // An already-owned durable append settles before close can release storage.
        let _transaction = self.communication_transaction.lock().await;
    }
    pub(super) fn reset_communication_endpoints(&self) -> Result<(), Error> {
        self.communication_loaded.store(false, Ordering::Release);
        self.state.clear_active_correspondents();
        if let Some(identity) = self.communication_identity() {
            self.communication_directory()
                .unregister(&identity.agent_id);
        }
        *self
            .directory
            .write()
            .unwrap_or_else(|error| error.into_inner()) =
            Arc::new(crate::runtime::messaging::LineageDirectory::default());
        self.register_communication_self()
    }
}

#[cfg(test)]
mod tests;
