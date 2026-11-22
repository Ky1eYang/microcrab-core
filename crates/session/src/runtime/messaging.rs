//! Lineage-local runtime address resolution. Directory entries carry no authority from text.
use crate::Error;
use crate::state::communication::{AgentId, ConversationIdentity, SessionInteraction};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock, Weak};

/// Narrow live peer port. Session composition implements it without runtime importing inner.
#[async_trait::async_trait]
pub trait AgentPeer: Send + Sync {
    async fn send_message(
        &self,
        arguments: serde_json::Value,
        invocation: crate::runtime::tool::ToolContext,
    ) -> Result<crate::state::communication::inbox::QueuedReceipt, Error>;
    async fn escalate(
        &self,
        arguments: serde_json::Value,
        invocation: crate::runtime::tool::ToolContext,
    ) -> Result<crate::state::communication::inbox::QueuedReceipt, Error>;
    #[doc(hidden)]
    async fn reply_external_speaker(
        &self,
        _arguments: serde_json::Value,
        _invocation: crate::runtime::tool::ToolContext,
    ) -> Result<crate::runtime::tool::ToolOutput, Error> {
        Err(Error::denied(
            "external dialogue reply has no owning Session",
        ))
    }
    async fn enqueue_authenticated(
        &self,
        carrier: crate::state::communication::inbox::AcceptedCarrier,
    ) -> Result<crate::state::communication::inbox::QueuedReceipt, Error>;
    fn communication_identity(&self) -> Option<ConversationIdentity>;
    fn session_id(&self) -> String;
    fn agent_name(&self) -> String;
    fn is_available(&self) -> bool;
}
pub trait AgentDirectory: Send + Sync {
    fn resolve(&self, sender: &AgentId, target: &AgentId) -> Result<Arc<dyn AgentPeer>, Error>;
}
#[derive(Default)]
pub(crate) struct LineageDirectory {
    peers: RwLock<BTreeMap<AgentId, Weak<dyn AgentPeer>>>,
    pub(crate) exchanges: tokio::sync::Mutex<ExchangeLedger>,
}
impl LineageDirectory {
    pub(crate) fn register(&self, peer: Weak<dyn AgentPeer>) -> Result<(), Error> {
        let live = peer.upgrade().ok_or(Error::Closed)?;
        let identity = live
            .communication_identity()
            .ok_or_else(|| Error::denied("legacy Session has no communication identity"))?;
        identity.validate()?;
        let mut peers = self
            .peers
            .write()
            .unwrap_or_else(|error| error.into_inner());
        peers.retain(|_, peer| peer.upgrade().is_some_and(|peer| peer.is_available()));
        if let Some(prior) = peers.get(&identity.agent_id).and_then(Weak::upgrade)
            && !Arc::ptr_eq(&prior, &live)
        {
            return Err(Error::config("communication identity collision"));
        }
        peers.insert(identity.agent_id, peer);
        Ok(())
    }
    pub(crate) fn unregister(&self, agent_id: &AgentId) {
        self.peers
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .remove(agent_id);
    }
}
impl AgentDirectory for LineageDirectory {
    fn resolve(&self, sender: &AgentId, target: &AgentId) -> Result<Arc<dyn AgentPeer>, Error> {
        if sender == target {
            return Err(Error::denied("self messaging is forbidden"));
        }
        let peers = self.peers.read().unwrap_or_else(|error| error.into_inner());
        let source = peers
            .get(sender)
            .and_then(Weak::upgrade)
            .ok_or_else(|| Error::denied("sender endpoint is not registered"))?;
        let target = peers
            .get(target)
            .and_then(Weak::upgrade)
            .ok_or_else(|| Error::denied("target endpoint is not registered"))?;
        if !source.is_available() || !target.is_available() {
            return Err(Error::Closed);
        }
        let from = source
            .communication_identity()
            .ok_or_else(|| Error::denied("sender has no communication identity"))?;
        let to = target
            .communication_identity()
            .ok_or_else(|| Error::denied("target has no communication identity"))?;
        if from.interaction != SessionInteraction::Persistent
            || to.interaction != SessionInteraction::Persistent
        {
            return Err(Error::denied("messaging requires persistent Sessions"));
        }
        let direct = (from.parent_agent_id.as_ref() == Some(&to.agent_id)
            && from.parent_session_id.as_deref() == Some(target.session_id().as_str()))
            || (to.parent_agent_id.as_ref() == Some(&from.agent_id)
                && to.parent_session_id.as_deref() == Some(source.session_id().as_str()));
        if from.lineage != to.lineage || !direct {
            return Err(Error::denied(
                "messaging requires exact same-lineage parent and child",
            ));
        }
        Ok(target)
    }
}

#[cfg(test)]
mod tests;

#[derive(Clone)]
pub(crate) struct Exchange {
    pub id: String,
    pub sender: AgentId,
    pub recipient: AgentId,
    pub open: bool,
}
#[derive(Default)]
pub(crate) struct ExchangeLedger {
    pub exchanges: Vec<Exchange>,
}
