//! Whole-chain admission and invocation-pinned authority. No operation policy lives here.
use super::*;
use crate::runtime::tool::ToolContext;

/// Unforgeable, non-serializable authority for one approved invocation. Cloning
/// carries that same authority into its continuations, never into a new call.
pub struct AdmittedInvocation {
    // 0 original/unentered, 1 resolved/unentered, 2 original/entered,
    // 3 resolved/entered. A single CAS owns binding versus dispatch races.
    dispatch_state: std::sync::atomic::AtomicU8,
    processes: Arc<dyn crate::runtime::execution::process::ProcessControl>,
    state: crate::State,
    agent: String,
    mode: Option<String>,
    session_id: String,
    call_id: String,
    execution_id: Option<crate::runtime::execution::ExecutionId>,
    subject: InvocationSubject,
    tool: String,
    component: Option<String>,
    arguments: serde_json::Value,
    file_version_policy_arguments: Option<Arc<serde_json::Value>>,
    cancellation: CancellationToken,
    grants: std::sync::Mutex<Vec<super::grants::StagedGrant>>,
    interactions: Vec<Arc<dyn PermissionInteraction>>,
    witness: AdmissionWitness,
}
struct AdmissionWitness {
    root: Arc<PermissionController>,
    revision: u64,
    lineage: Vec<(Arc<PermissionController>, PolicySnapshot)>,
}
impl AdmittedInvocation {
    /// The sealed resolver is the sole producer of this non-serializable witness.
    /// Its private fields prevent arbitrary callers from authorizing rewritten input.
    pub(crate) fn certify_resolution(
        &self,
        context: &ToolContext,
        resolved: &crate::runtime::tool::secret::ResolvedArguments,
    ) -> Result<(), PermissionError> {
        self.validate(context, None)?;
        if !resolved.belongs_to(self) {
            return Err(PermissionError::ProofMismatch("resolver approval differs"));
        }
        self.witness
            .root
            .ensure_witness(self.witness.revision, &self.witness.lineage)?;
        self.dispatch_state
            .compare_exchange(
                0,
                1,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .map_err(|_| PermissionError::AlreadyDispatched)?;
        Ok(())
    }
    pub(crate) fn begin_dispatch(
        &self,
        context: &ToolContext,
        component: Option<&str>,
    ) -> Result<(), PermissionError> {
        self.validate(context, component)?;
        let state = self
            .dispatch_state
            .load(std::sync::atomic::Ordering::Acquire);
        if state >= 2 {
            return Err(PermissionError::AlreadyDispatched);
        }
        if state == 1 && !context.has_resolution_for(self) {
            return Err(PermissionError::ProofMismatch(
                "resolved argument witness required",
            ));
        }
        self.witness
            .root
            .ensure_witness(self.witness.revision, &self.witness.lineage)?;
        for interaction in &self.interactions {
            interaction.validate_dispatch()?;
        }
        self.dispatch_state
            .compare_exchange(
                state,
                state + 2,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .map_err(|_| PermissionError::AlreadyDispatched)?;
        let grants = std::mem::take(
            &mut *self
                .grants
                .lock()
                .map_err(|_| PermissionError::Unavailable)?,
        );
        for grant in grants {
            grant.dispatch();
        }
        Ok(())
    }
    pub(crate) fn original_arguments(&self) -> &serde_json::Value {
        &self.arguments
    }
    pub(crate) fn validate_arguments(
        &self,
        arguments: &serde_json::Value,
    ) -> Result<(), PermissionError> {
        if self
            .dispatch_state
            .load(std::sync::atomic::Ordering::Acquire)
            & 1
            != 0
        {
            return Err(PermissionError::ProofMismatch(
                "resolved argument witness required",
            ));
        }
        if &self.arguments != arguments {
            return Err(PermissionError::ProofMismatch("original arguments differ"));
        }
        Ok(())
    }
    pub(crate) fn validate(
        &self,
        context: &ToolContext,
        component: Option<&str>,
    ) -> Result<(), PermissionError> {
        if self.cancellation.is_cancelled() || context.cancellation.is_cancelled() {
            return Err(PermissionError::Cancelled);
        }
        if context.execution_progress.as_ref().is_some_and(|progress| {
            self.execution_id
                .is_none_or(|id| !progress.belongs_to(&self.state.executions(), id))
        }) {
            return Err(PermissionError::ProofMismatch(
                "execution progress owner or identity differs",
            ));
        }
        let mismatch = if !Arc::ptr_eq(&self.processes, &context.processes) {
            Some("process owner differs")
        } else if !self.state.same_owner(&context.state) {
            Some("state owner differs")
        } else if self.agent != context.agent {
            Some("agent identity differs")
        } else if self.mode != context.mode {
            Some("mode identity differs")
        } else if self.session_id != context.session_id {
            Some("session identity differs")
        } else if self.call_id != context.call_id {
            Some("call identity differs")
        } else if self.execution_id != context.execution_id {
            Some("execution identity differs")
        } else if self.subject != context.subject {
            Some("invocation subject differs")
        } else if self.tool != context.tool {
            Some("tool identity differs")
        } else if !match (
            &self.file_version_policy_arguments,
            &context.file_version_policy_arguments,
        ) {
            (None, None) => true,
            (Some(expected), Some(actual)) => Arc::ptr_eq(expected, actual),
            _ => false,
        } {
            Some("file version admission witness differs")
        } else if component.is_some_and(|actual| self.component.as_deref() != Some(actual)) {
            Some("component identity differs")
        } else {
            None
        };
        if let Some(reason) = mismatch {
            return Err(PermissionError::ProofMismatch(reason));
        }
        Ok(())
    }
}

impl PermissionController {
    /// Capture each owner once. The local snapshot may be a hypothetical mode for
    /// inventory; ancestors always retain their own actual agent/mode.
    pub(crate) fn compiled_chain(
        &self,
        local: &PolicySnapshot,
        tool: &str,
    ) -> Result<(CompiledToolPolicy, Vec<serde_json::Value>), PermissionError> {
        let lineage = self.lineage()?;
        let compiled = Self::compile_witness(local, &lineage, tool)?;
        let owners = std::iter::once(self.provenance(local))
            .chain(
                lineage
                    .iter()
                    .map(|(owner, snapshot)| owner.provenance(snapshot)),
            )
            .map(|owner| serde_json::to_value(owner).map_err(configuration))
            .collect::<Result<_, _>>()?;
        self.ensure_witness(local.revision, &lineage)?;
        Ok((compiled, owners))
    }

    /// Pure compilation from one captured chain; the reader validates the chain
    /// before and after projecting its entire observation, rather than per row.
    pub(crate) fn compile_witness(
        local: &PolicySnapshot,
        lineage: &[(Arc<PermissionController>, PolicySnapshot)],
        tool: &str,
    ) -> Result<CompiledToolPolicy, PermissionError> {
        let policies = std::iter::once(local)
            .chain(lineage.iter().map(|(_, snapshot)| snapshot))
            .map(|snapshot| compile(snapshot, tool).map_err(configuration))
            .collect::<Result<Vec<_>, _>>()?;
        combine(policies).map_err(configuration)
    }

    pub(crate) fn provenance(&self, snapshot: &PolicySnapshot) -> super::PermissionOwner {
        super::PermissionOwner {
            controller: self.identity,
            revision: snapshot.revision,
            agent: snapshot.config.agent.clone(),
            mode: snapshot.config.mode.clone(),
        }
    }

    pub(crate) fn ancestor_witness(
        lineage: &[(Arc<PermissionController>, PolicySnapshot)],
    ) -> String {
        use sha2::{Digest, Sha256};
        let mut digest = Sha256::new();
        for (owner, snapshot) in lineage {
            digest.update(owner.identity.to_be_bytes());
            digest.update(snapshot.revision.to_be_bytes());
        }
        format!("{:x}", digest.finalize())
    }

    pub(crate) fn ensure_witness(
        &self,
        revision: u64,
        expected: &[(Arc<PermissionController>, PolicySnapshot)],
    ) -> Result<(), PermissionError> {
        if self
            .grant_faulted
            .load(std::sync::atomic::Ordering::Acquire)
            || expected.iter().any(|(owner, _)| {
                owner
                    .grant_faulted
                    .load(std::sync::atomic::Ordering::Acquire)
            })
        {
            return Err(PermissionError::PersistenceUncertain(
                "controller lineage requires recovery after an unacknowledged policy write".into(),
            ));
        }
        let local = self
            .policy
            .read()
            .map_err(|_| PermissionError::StalePolicy)?;
        let edge = self
            .parent
            .read()
            .map_err(|_| PermissionError::StalePolicy)?;
        let mut policies = Vec::with_capacity(expected.len());
        let mut edges = Vec::with_capacity(expected.len());
        for (owner, _) in expected {
            policies.push(
                owner
                    .policy
                    .read()
                    .map_err(|_| PermissionError::StalePolicy)?,
            );
            edges.push(
                owner
                    .parent
                    .read()
                    .map_err(|_| PermissionError::StalePolicy)?,
            );
        }
        let mut actual = edge.as_ref();
        if local.revision != revision {
            return Err(PermissionError::StalePolicy);
        }
        for (index, (owner, snapshot)) in expected.iter().enumerate() {
            if actual.is_none_or(|current| !Arc::ptr_eq(current, owner))
                || policies[index].revision != snapshot.revision
            {
                return Err(PermissionError::StalePolicy);
            }
            actual = edges[index].as_ref();
        }
        if actual.is_some() {
            return Err(PermissionError::StalePolicy);
        }
        Ok(())
    }

    /// Only registry/lifecycle admission owners call this method. No proof is
    /// issued until all deterministic denies and all owner approvals settle under
    /// one complete witness. Retry discards every stale approval before effects.
    pub(crate) async fn admit(
        self: &Arc<Self>,
        request: &PermissionRequest,
        invocation: &ToolContext,
        issuer: &crate::context::Content,
        component: Option<String>,
    ) -> Result<Arc<AdmittedInvocation>, PermissionError> {
        if request.session_id != invocation.session_id
            || request.call_id != invocation.call_id
            || request.subject != invocation.subject
            || request.tool != invocation.tool
            || request.call_id.is_empty()
        {
            return Err(PermissionError::InvalidConfiguration(
                "invocation admission identity mismatch".into(),
            ));
        }
        let execution_id = invocation.execution_id.ok_or_else(|| {
            PermissionError::InvalidConfiguration(
                "invocation admission requires a reserved execution identity".into(),
            )
        })?;
        for _ in 0..3 {
            match self.admit_once(request, invocation, issuer).await {
                Err(PermissionError::StalePolicy) => continue,
                Err(error) => return Err(error),
                Ok((grants, interactions, witness)) => {
                    if invocation.cancellation.is_cancelled() {
                        return Err(PermissionError::Cancelled);
                    }
                    if !invocation
                        .state
                        .executions()
                        .claim_admission(execution_id)
                        .map_err(configuration)?
                    {
                        return Err(PermissionError::AlreadyDispatched);
                    }
                    return Ok(Arc::new(AdmittedInvocation {
                        dispatch_state: std::sync::atomic::AtomicU8::new(0),
                        processes: invocation.processes.clone(),
                        state: invocation.state.clone(),
                        agent: invocation.agent.clone(),
                        mode: invocation.mode.clone(),
                        session_id: invocation.session_id.clone(),
                        call_id: invocation.call_id.clone(),
                        execution_id: invocation.execution_id,
                        subject: invocation.subject.clone(),
                        tool: invocation.tool.clone(),
                        component,
                        arguments: request.arguments.clone(),
                        file_version_policy_arguments: invocation
                            .file_version_policy_arguments
                            .clone(),
                        cancellation: invocation.cancellation.clone(),
                        grants: std::sync::Mutex::new(grants),
                        interactions,
                        witness,
                    }));
                }
            }
        }
        Err(PermissionError::StalePolicy)
    }

    async fn admit_once(
        self: &Arc<Self>,
        request: &PermissionRequest,
        invocation: &ToolContext,
        issuer: &crate::context::Content,
    ) -> Result<
        (
            Vec<super::grants::StagedGrant>,
            Vec<Arc<dyn PermissionInteraction>>,
            AdmissionWitness,
        ),
        PermissionError,
    > {
        let cancellation = &invocation.cancellation;
        if cancellation.is_cancelled() {
            return Err(PermissionError::Cancelled);
        }
        let local = self.snapshot()?;
        let lineage = self.lineage()?;
        let mut chain = vec![(self.clone(), &local)];
        chain.extend(
            lineage
                .iter()
                .map(|(owner, policy)| (owner.clone(), policy)),
        );
        // Preserve owner order even when all members are Static. Each Ask belongs
        // to that owner, and matcher work is bounded across the complete chain.
        let mut actions = if request.subject.is_tool() {
            let policies = chain
                .iter()
                .map(|(_, snapshot)| compile(snapshot, &request.tool).map_err(configuration))
                .collect::<Result<Vec<_>, _>>()?;
            microcrab_permission::evaluate_compiled_members(
                &CompiledToolPolicy::AllOf { policies },
                invocation
                    .file_version_policy_arguments
                    .as_deref()
                    .unwrap_or(&request.arguments),
            )
            .map_err(configuration)?
        } else {
            chain
                .iter()
                .map(|(_, snapshot)| snapshot.config.initialization)
                .collect()
        };
        if request.subject.is_tool()
            && (request.tool == "escalate"
                || (request.tool == "send_message"
                    && request
                        .arguments
                        .get("intent")
                        .and_then(serde_json::Value::as_str)
                        == Some("send")))
            && let Some(action) = actions.first_mut()
        {
            *action = action.strictest(Action::Ask);
        }
        self.ensure_witness(local.revision, &lineage)?;
        if let Some(((owner, snapshot), _)) = chain
            .iter()
            .zip(&actions)
            .find(|(_, action)| **action == Action::Deny)
        {
            return Err(PermissionError::PolicyDenied {
                controller: owner.identity,
                revision: snapshot.revision,
                reason: format!(
                    "{} policy for agent {:?}, mode {:?}, tool {:?} resolved Deny",
                    if request.subject.is_tool() {
                        "compiled tool"
                    } else {
                        "initialization"
                    },
                    snapshot.config.agent,
                    snapshot.config.mode,
                    request.tool
                ),
            });
        }
        let mut grants = Vec::new();
        let mut interactions = Vec::new();
        for ((owner, snapshot), action) in chain.into_iter().zip(actions) {
            if action == Action::Ask {
                let mut owned = request.clone();
                owned.revision = snapshot.revision;
                owned.agent = snapshot.config.agent.clone();
                owned.mode = snapshot.config.mode.clone();
                let resolution = owner.resolve_ask(
                    snapshot,
                    &owned,
                    invocation.tool_description.as_deref(),
                    &invocation.state,
                    issuer,
                    cancellation,
                );
                tokio::pin!(resolution);
                let result = loop {
                    tokio::select! { biased;
                        _ = cancellation.cancelled() => return Err(PermissionError::Cancelled),
                        change = self.lineage_changed(local.revision,&lineage) => {change?;self.ensure_witness(local.revision,&lineage)?;},
                        result = &mut resolution => break result,
                    }
                };
                // A stale Deny or unavailable result also belongs to the old
                // witness. Discard it before interpreting that result.
                self.ensure_witness(local.revision, &lineage)?;
                let (action, grant, interaction, denial) = result?;
                if let Some(interaction) = interaction {
                    interactions.push(interaction);
                }
                if action != Action::Allow {
                    let denial = denial.unwrap_or_else(super::interaction::Denial::invalid_reply);
                    return Err(PermissionError::SettledDenied {
                        controller: owner.identity,
                        revision: snapshot.revision,
                        source_kind: denial.source,
                        reason: denial.reason,
                    });
                }
                // Exact predicates saw the canonical version, but this Ask/review
                // presented authored references. Never promote that view to a reusable grant.
                if let Some(grant) = grant
                    && invocation.file_version_policy_arguments.is_none()
                {
                    grants.push(super::grants::StagedGrant {
                        owner: owner.clone(),
                        expected: snapshot.clone(),
                        request: owned.clone(),
                        grant,
                        root: self.clone(),
                        root_revision: local.revision,
                        lineage: lineage.clone(),
                        cancellation: cancellation.clone(),
                        terminal: invocation
                            .state
                            .executions()
                            .observe_terminal(
                                invocation
                                    .execution_id
                                    .ok_or(PermissionError::Unavailable)?,
                            )
                            .map_err(configuration)?,
                    });
                }
            }
        }
        if cancellation.is_cancelled() {
            return Err(PermissionError::Cancelled);
        }
        self.ensure_witness(local.revision, &lineage)?;
        Ok((
            grants,
            interactions,
            AdmissionWitness {
                root: self.clone(),
                revision: local.revision,
                lineage,
            },
        ))
    }
}
fn configuration(error: impl std::fmt::Display) -> PermissionError {
    PermissionError::InvalidConfiguration(error.to_string())
}
