use super::*;
struct Peer {
    identity: ConversationIdentity,
    session: String,
    open: bool,
}
#[async_trait::async_trait]
impl AgentPeer for Peer {
    async fn send_message(
        &self,
        _: serde_json::Value,
        _: crate::runtime::tool::ToolContext,
    ) -> Result<crate::state::communication::inbox::QueuedReceipt, Error> {
        Err(Error::denied("read-only directory fixture"))
    }
    async fn escalate(
        &self,
        _: serde_json::Value,
        _: crate::runtime::tool::ToolContext,
    ) -> Result<crate::state::communication::inbox::QueuedReceipt, Error> {
        Err(Error::denied("read-only directory fixture"))
    }
    async fn enqueue_authenticated(
        &self,
        _: crate::state::communication::inbox::AcceptedCarrier,
    ) -> Result<crate::state::communication::inbox::QueuedReceipt, Error> {
        Err(Error::denied("read-only directory fixture"))
    }
    fn communication_identity(&self) -> Option<ConversationIdentity> {
        Some(self.identity.clone())
    }
    fn session_id(&self) -> String {
        self.session.clone()
    }
    fn agent_name(&self) -> String {
        "same-agent-name".into()
    }
    fn is_available(&self) -> bool {
        self.open
    }
}
fn peer(identity: ConversationIdentity, session: &str) -> Arc<dyn AgentPeer> {
    Arc::new(Peer {
        identity,
        session: session.into(),
        open: true,
    })
}
#[test]
fn directory_requires_exact_live_persistent_parent_child_addresses() {
    let parent = ConversationIdentity::fresh(0, SessionInteraction::Persistent);
    let mut child = ConversationIdentity::fresh(1, SessionInteraction::Persistent);
    child.lineage = parent.lineage.clone();
    child.parent_agent_id = Some(parent.agent_id.clone());
    child.parent_session_id = Some("parent-session".into());
    let parent_peer = peer(parent.clone(), "parent-session");
    let child_peer = peer(child.clone(), "child-session");
    let directory = LineageDirectory::default();
    directory.register(Arc::downgrade(&parent_peer)).unwrap();
    directory.register(Arc::downgrade(&child_peer)).unwrap();
    assert_eq!(
        directory
            .resolve(&parent.agent_id, &child.agent_id)
            .unwrap()
            .session_id(),
        "child-session"
    );
    assert_eq!(
        directory
            .resolve(&child.agent_id, &parent.agent_id)
            .unwrap()
            .session_id(),
        "parent-session"
    );
    assert!(
        directory
            .resolve(&parent.agent_id, &parent.agent_id)
            .is_err()
    );
    assert!(
        directory
            .resolve(&parent.agent_id, &AgentId::fresh())
            .is_err()
    );
    let mut sibling = child.clone();
    sibling.agent_id = AgentId::fresh();
    let sibling_peer = peer(sibling.clone(), "sibling-session");
    directory.register(Arc::downgrade(&sibling_peer)).unwrap();
    assert!(
        directory
            .resolve(&child.agent_id, &sibling.agent_id)
            .is_err()
    );
    drop(child_peer);
    assert!(
        directory
            .resolve(&parent.agent_id, &child.agent_id)
            .is_err()
    );
}
#[test]
fn wrong_lineage_wrong_parent_session_and_return_are_denied() {
    for mismatch in ["lineage", "session", "return"] {
        let parent = ConversationIdentity::fresh(0, SessionInteraction::Persistent);
        let mut child = ConversationIdentity::fresh(1, SessionInteraction::Persistent);
        child.lineage = parent.lineage.clone();
        child.parent_agent_id = Some(parent.agent_id.clone());
        child.parent_session_id = Some("parent".into());
        match mismatch {
            "lineage" => child.lineage = "different".into(),
            "session" => child.parent_session_id = Some("other".into()),
            _ => child.interaction = SessionInteraction::Return,
        }
        let parent_peer = peer(parent.clone(), "parent");
        let child_peer = peer(child.clone(), "child");
        let directory = LineageDirectory::default();
        directory.register(Arc::downgrade(&parent_peer)).unwrap();
        directory.register(Arc::downgrade(&child_peer)).unwrap();
        assert!(
            directory
                .resolve(&parent.agent_id, &child.agent_id)
                .is_err()
        );
    }
}
