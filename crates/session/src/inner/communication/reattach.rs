//! Explicit recovery binding creates endpoints only; no execution producer is resumed.
use super::*;
use crate::inner::delegation::ChildRecord;
impl SessionInner {
    pub(crate) fn release_recovered_child_reservation(
        &self,
        session_id: &str,
        agent_id: &crate::state::communication::AgentId,
    ) {
        let mut pending = self
            .recovered_child_reservations
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if pending.get(session_id) == Some(agent_id) {
            pending.remove(session_id);
        }
    }
    pub(crate) fn reserve_recovered_child(
        self: &Arc<Self>,
        session_id: &str,
        agent_id: &crate::state::communication::AgentId,
    ) -> Result<crate::RecoveredChildReservation, Error> {
        self.ensure_model_open()?;
        self.require_derivation()?;
        let parent = self
            .state()
            .communication
            .ok_or_else(|| Error::denied("legacy parent cannot reserve restored children"))?;
        if !parent.correspondents.iter().any(|row| {
            &row.agent_id == agent_id
                && row.relation == CorrespondentRelation::Child
                && row.interaction == SessionInteraction::Persistent
        }) {
            return Err(Error::denied(
                "restored child reservation requires its exact persisted correspondent",
            ));
        }
        if session_id.is_empty() || session_id == self.state.processes().session_id() {
            return Err(Error::denied("invalid restored child session identity"));
        }
        let mut pending = self
            .recovered_child_reservations
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if pending.contains_key(session_id) || pending.values().any(|value| value == agent_id) {
            return Err(Error::Busy);
        }
        let children = self
            .delegated_children
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if children.contains_key(session_id)
            || children.values().any(|row| {
                row.child
                    .communication_identity()
                    .is_some_and(|identity| &identity.agent_id == agent_id)
            })
        {
            return Err(Error::config(
                "restored child already has a live parent binding",
            ));
        }
        self.ensure_model_open()?;
        crate::budget::check(
            crate::LimitKind::PendingExecutions,
            pending
                .len()
                .saturating_add(children.len())
                .saturating_add(1),
            self.state().limits.max_pending_executions,
        )?;
        pending.insert(session_id.to_owned(), agent_id.clone());
        Ok(crate::RecoveredChildReservation {
            parent: self.clone(),
            session_id: session_id.to_owned(),
            agent_id: agent_id.clone(),
        })
    }
    pub(crate) async fn attach_recovered_child(
        self: &Arc<Self>,
        child: &crate::Session,
    ) -> Result<(), Error> {
        self.ensure_model_open()?;
        self.require_derivation()?;
        if Arc::ptr_eq(self, &child.inner) {
            return Err(Error::denied("a Session cannot attach itself"));
        }
        let _parent_operation = self.reserve().await?;
        let _child_operation = child.inner.reserve().await?;
        child.inner.ensure_model_open()?;
        if !child.inner.restored.load(Ordering::Acquire)
            || child.kind() != crate::SessionKind::Delegated
        {
            return Err(Error::config(
                "reattachment requires an explicitly restored delegated Session",
            ));
        }
        let parent = self
            .state()
            .communication
            .ok_or_else(|| Error::denied("legacy parent cannot attach communication children"))?;
        let derived = child
            .state()
            .communication
            .ok_or_else(|| Error::denied("legacy child has no persisted communication identity"))?;
        parent.validate()?;
        derived.validate()?;
        if derived.identity.depth != 1
            || derived.identity.interaction != SessionInteraction::Persistent
            || derived.identity.parent_session_id.as_deref()
                != Some(self.state.processes().session_id())
            || derived.identity.parent_agent_id.as_ref() != Some(&parent.identity.agent_id)
            || derived.identity.lineage != parent.identity.lineage
        {
            return Err(Error::denied(
                "recovered child persisted parent identity does not match this owner",
            ));
        }
        let child_id = child.process_control().session_id().to_owned();
        let relation = child
            .parent_relationship()
            .ok_or_else(|| Error::config("recovered child has no persisted parent relationship"))?;
        let child_agent = child
            .inner
            .agent
            .as_ref()
            .ok_or_else(|| Error::config("recovered child has no Agent metadata"))?;
        if relation.parent_session_id != self.state.processes().session_id()
            || relation.target != child_agent.name
        {
            return Err(Error::denied(
                "recovered child relationship names another parent or Agent",
            ));
        }
        use microcrab_agent_types::AgentRole;
        if !matches!(
            self.current_agent_role()?,
            AgentRole::MainAgent | AgentRole::Both
        ) || !matches!(
            child.current_agent_role()?,
            AgentRole::SubAgent | AgentRole::Both
        ) || self
            .catalog
            .iter()
            .find(|entry| entry.name == child_agent.name)
            != Some(child_agent)
        {
            return Err(Error::denied(
                "recovered child Agent and current modes are outside the current parent authority",
            ));
        }
        if !parent.correspondents.iter().any(|row| {
            row.agent_id == derived.identity.agent_id
                && row.agent == child_agent.name
                && row.relation == CorrespondentRelation::Child
                && row.interaction == SessionInteraction::Persistent
        }) || !derived.correspondents.iter().any(|row| {
            row.agent_id == parent.identity.agent_id
                && row.agent == self.agent_name()
                && row.relation == CorrespondentRelation::Parent
                && row.interaction == SessionInteraction::Persistent
        }) {
            return Err(Error::denied(
                "recovered child lacks the exact persisted two-sided correspondent witnesses",
            ));
        }
        if child.inner.host.is_some() != self.host.is_some()
            || child
                .inner
                .host
                .as_ref()
                .zip(self.host.as_ref())
                .is_some_and(|(left, right)| !Arc::ptr_eq(left, right))
            || !Arc::ptr_eq(&child.inner.processes, &self.processes)
            || child_id == self.state.processes().session_id()
        {
            return Err(Error::denied(
                "recovered child requires the current exact Host and process owner",
            ));
        }
        child
            .state()
            .limits
            .ensure_within_parent(&self.state().limits)?;
        child
            .permission_controller()
            .ensure_parent(self.permission_controller())
            .map_err(|error| error.into_runtime_error())?;
        let mut children = self
            .delegated_children
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.ensure_model_open()?;
        child.inner.ensure_model_open()?;
        if children.contains_key(&child_id) {
            return Err(Error::config("child already has an active owner binding"));
        }
        crate::budget::check(
            crate::LimitKind::PendingExecutions,
            children.len().saturating_add(1),
            self.state().limits.max_pending_executions,
        )?;
        let directory = self.communication_directory();
        let child_peer: std::sync::Weak<dyn AgentPeer> = child.inner.self_weak.clone();
        directory.register(child_peer)?;
        *child
            .inner
            .directory
            .write()
            .unwrap_or_else(|error| error.into_inner()) = directory;
        self.state
            .set_correspondent_active(derived.identity.agent_id.clone(), true);
        child
            .inner
            .state
            .set_correspondent_active(parent.identity.agent_id.clone(), true);
        children.insert(
            child_id,
            ChildRecord {
                target: child_agent.name.clone(),
                child: child.clone(),
            },
        );
        Ok(())
    }
}
