//! Exact parent relationship and prepared-child validation live in composition.
use crate::inner::*;
use crate::runtime::tool::ToolContext;
use crate::runtime::tool::delegate::DelegateParent;

pub(crate) struct ChildRecord {
    pub target: String,
    pub child: crate::Session,
}

#[async_trait::async_trait]
impl DelegateParent for SessionInner {
    async fn capture_focus(
        &self,
        selections: Vec<crate::state::reference::focus::FocusSelection>,
        invocation: &ToolContext,
    ) -> Result<Vec<crate::state::reference::focus::FocusSnapshot>, Error> {
        self.capture_delegation_focus(selections, invocation).await
    }
    fn adopt_child(
        &self,
        target: &str,
        mode: Option<&str>,
        issuer_epoch: u64,
        child: &crate::Session,
        invocation: &ToolContext,
    ) -> Result<(), Error> {
        self.ensure_open()?;
        self.require_derivation()?;
        invocation
            .ensure_admitted(None)
            .map_err(|e| e.into_runtime_error())?;
        if invocation.session_id != self.state.processes().session_id()
            || issuer_epoch != self.state().epoch_id
        {
            return Err(Error::tool(
                "prepared child has a stale parent epoch witness",
            ));
        }
        let (agent, selected_mode) = self.state.resolve_delegate(target, mode)?;
        if child.kind() != crate::SessionKind::Delegated
            || child.inner.started.load(Ordering::Acquire)
        {
            return Err(Error::config(
                "prepared child must be an unstarted Delegated Session",
            ));
        }
        let child_agent = child
            .inner
            .agent
            .as_ref()
            .ok_or_else(|| Error::config("prepared child has no Agent metadata"))?;
        let expected = self
            .catalog
            .iter()
            .find(|entry| entry.name == agent)
            .ok_or_else(|| {
                Error::denied("admitted delegate Agent is no longer in the issuing catalog")
            })?;
        if child_agent != expected {
            return Err(Error::config(
                "prepared child metadata differs from the admitted catalog definition",
            ));
        }
        if child_agent.name != agent
            || !child_agent
                .modes
                .iter()
                .any(|entry| entry.name == selected_mode)
        {
            return Err(Error::config(
                "prepared child Agent/mode does not match the admitted target",
            ));
        }
        if child
            .inner
            .host
            .as_ref()
            .zip(self.host.as_ref())
            .is_some_and(|(left, right)| !Arc::ptr_eq(left, right))
            || child.inner.host.is_some() != self.host.is_some()
        {
            return Err(Error::config(
                "prepared child Host binding does not match its parent",
            ));
        }
        if !Arc::ptr_eq(&child.inner.processes, &self.processes)
            || child.process_control().session_id() == self.state.processes().session_id()
        {
            return Err(Error::config(
                "prepared child requires fresh identity and the exact parent process scope",
            ));
        }
        if child.state().selected_mode.as_deref() != Some(selected_mode.as_str()) {
            child.inner.select_mode_now(selected_mode.clone(), None)?;
        }
        child
            .permission_controller()
            .ensure_parent(self.permission_controller())
            .map_err(|e| Error::config_with_private(e.public_reason(), e.to_string()))?;
        let parent_limits = self.state().limits;
        let child_limits = child.state().limits;
        child_limits.ensure_within_parent(&parent_limits)?;
        let execution_id = invocation
            .execution_id
            .ok_or_else(|| Error::tool("delegation requires its admitted execution identity"))?;
        let row = invocation.state.executions().get(execution_id)?;
        let arguments = invocation
            .admitted_arguments()
            .map_err(|e| e.into_runtime_error())?;
        if invocation.tool != "delegate"
            || !row.status.is_pending()
            || arguments.get("target").and_then(Value::as_str) != Some(agent.as_str())
            || arguments.get("mode").and_then(Value::as_str) != Some(selected_mode.as_str())
            || arguments
                .get("action")
                .and_then(Value::as_str)
                .is_some_and(|action| action != "start")
        {
            return Err(Error::tool(
                "prepared child does not match the original admitted delegate invocation",
            ));
        }
        let binding = row.delegate_child.ok_or_else(|| {
            Error::tool("prepared child requires an exact execution child binding")
        })?;
        if binding.session_id != child.process_control().session_id() || binding.target != agent {
            return Err(Error::tool("prepared child execution binding mismatch"));
        }
        let id = child.process_control().session_id().to_owned();
        let mut children = self
            .delegated_children
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        self.ensure_open()?;
        self.require_derivation()?;
        crate::budget::check(
            crate::LimitKind::PendingExecutions,
            children.len().saturating_add(1),
            self.state().limits.max_pending_executions,
        )?;
        if children.contains_key(&id)
            || self
                .closed_children
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .any(|(closed, _, _)| closed == &id)
        {
            return Err(Error::config(
                "prepared child identity is already registered",
            ));
        }
        *child
            .inner
            .parent_relationship
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(crate::DelegationRelationship {
            parent_session_id: self.state.processes().session_id().to_owned(),
            parent_execution_id: execution_id,
            issuer_epoch,
            target: agent.clone(),
            mode: selected_mode,
        });
        child
            .inner
            .state
            .update(|state| state.parent_relationship = child.parent_relationship());
        self.bind_child_communication(child)?;
        children.insert(
            id,
            ChildRecord {
                target: agent,
                child: child.clone(),
            },
        );
        Ok(())
    }
    fn child_settled(
        &self,
        child_id: &str,
        target: &str,
        result: Result<(), crate::observation::diagnostics::ToolDiagnostic>,
    ) {
        self.remove_child_communication(child_id);
        self.delegated_children
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(child_id);
        let mut receipts = self
            .closed_children
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        receipts.push_back((child_id.to_owned(), target.to_owned(), result));
        while receipts.len() > self.state().limits.max_retained_executions {
            receipts.pop_front();
        }
    }
    async fn submit_child(
        &self,
        child_id: &str,
        target: &str,
        input: String,
    ) -> Result<TurnOutcome, Error> {
        self.ensure_open()?;
        self.require_derivation()?;
        let child = self.owned_child(child_id, target)?;
        if child.communication_identity().is_some_and(|identity| {
            identity.interaction == crate::state::communication::SessionInteraction::Return
        }) {
            return Err(Error::denied("return child cannot receive follow-up input"));
        }
        child
            .inner
            .submit(crate::SessionInput::text(input), None)
            .await
    }
    async fn resume_child(&self, child_id: &str, target: &str) -> Result<TurnOutcome, Error> {
        self.ensure_open()?;
        self.require_derivation()?;
        let child = self.owned_child(child_id, target)?;
        if child.communication_identity().is_some_and(|identity| {
            identity.interaction == crate::state::communication::SessionInteraction::Return
        }) {
            return Err(Error::denied("return child cannot be resumed"));
        }
        child.inner.resume(None).await
    }
    async fn close_child(&self, child_id: &str, target: &str) -> Result<(), Error> {
        let receipt = self
            .closed_children
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .find(|(id, owner, _)| id == child_id && owner == target)
            .map(|(_, _, result)| result.clone());
        if let Some(receipt) = receipt {
            return receipt.map_err(Error::Diagnostic);
        }
        let child = self.owned_child(child_id, target)?;
        child.interrupt();
        child.close().await?;
        // The worker observes retirement then settles its buffer after audit and
        // Journal acknowledgement; close itself is sticky and safe to retry.
        child.inner.publish_retired();
        let completion = child
            .inner
            .delegation_completion
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(mut completion) = completion {
            loop {
                let outcome = completion.borrow_and_update().clone();
                if let Some(outcome) = outcome {
                    return outcome.map_err(Error::Diagnostic);
                }
                completion
                    .changed()
                    .await
                    .map_err(|_| Error::ExecutionInterrupted)?;
            }
        }
        self.child_settled(child_id, target, Ok(()));
        Ok(())
    }
}
impl SessionInner {
    pub(crate) fn owned_child(&self, id: &str, target: &str) -> Result<crate::Session, Error> {
        let children = self
            .delegated_children
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let child = children
            .get(id)
            .ok_or_else(|| Error::denied("child is not owned by this parent Session"))?;
        if child.target != target {
            return Err(Error::denied(
                "child target does not match its exact parent relationship",
            ));
        }
        Ok(child.child.clone())
    }
    pub(crate) async fn close_owned_children(&self) -> Result<(), Error> {
        let children = self
            .delegated_children
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .map(|entry| entry.child.clone())
            .collect::<Vec<_>>();
        let mut failure = None;
        for child in children {
            child.interrupt();
            match child.close().await {
                Ok(()) => child.inner.publish_retired(),
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        Ok(())
    }
    fn publish_retired(&self) {
        let mut inbox = self.inbox.state.lock().unwrap_or_else(|e| e.into_inner());
        inbox.retired = true;
        drop(inbox);
        self.publish_activity();
    }
}
