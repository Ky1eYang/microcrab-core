//! Admitted retained-instance initialization. Session owns durable intent/settlement;
//! this leaf owns the serialized guest call and scope-only producer cleanup.
use super::*;

/// Opaque shared component identity retained by each advertised tool.
#[derive(Clone)]
pub struct AgentLifecycle {
    pub(super) compiled: Arc<Compiled>,
}
impl AgentLifecycle {
    pub(crate) fn identity(&self) -> usize {
        Arc::as_ptr(&self.compiled) as usize
    }
    pub(crate) fn admission_identity(&self) -> &str {
        &self.compiled.admission_identity
    }
    pub fn component(&self) -> &str {
        &self.compiled.evidence_component
    }
    pub fn generation(&self, state: &crate::State) -> Result<u64, Error> {
        Ok(state.wasm_instances().slot(&self.compiled)?.generation)
    }
    pub(crate) async fn ready(
        &self,
        state: &crate::State,
        cancellation: &CancellationToken,
    ) -> Result<bool, Error> {
        let slot = state.wasm_instances().slot(&self.compiled)?;
        let worker = slot.lease(cancellation).await?;
        Ok(worker.ready())
    }
    pub(crate) fn reset(&self, state: &crate::State) {
        state.wasm_instances().reset(&self.compiled);
    }
    pub(crate) fn import_scope(&self) -> Vec<String> {
        self.compiled
            .host_info_json
            .as_ref()
            .ok()
            .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
            .and_then(|value| value.get("capabilities").cloned())
            .and_then(|value| serde_json::from_value(value).ok())
            .unwrap_or_default()
    }
    /// Direct embedding must supply a separately admitted AgentInit subject,
    /// typed admission proof and execution identity, and persist its own intent.
    /// Session supplies these duties automatically before ordinary model sending.
    pub async fn initialize(&self, mut invocation: ToolContext) -> Result<(), Error> {
        invocation
            .ensure_admitted(Some(self.admission_identity()))
            .map_err(admission_error)?;
        let crate::policy::InvocationSubject::AgentInit {
            component,
            generation,
            ..
        } = &invocation.subject
        else {
            return Err(Error::lifecycle(
                "initialization requires a trusted lifecycle subject",
            ));
        };
        if invocation.execution_id.is_none() || !invocation.tool.is_empty() {
            return Err(Error::lifecycle(
                "initialization requires its own admitted execution identity",
            ));
        }
        let slot = invocation.state.wasm_instances().slot(&self.compiled)?;
        if component != self.component() || *generation != slot.generation {
            return Err(Error::lifecycle(
                "initialization generation changed before admission",
            ));
        }
        invocation
            .begin_dispatch(Some(self.admission_identity()))
            .map_err(admission_error)?;
        invocation.begin_initialization_cache()?;
        let cancellation = invocation.cancellation.child_token();
        let _cancel = CancelOnDrop(cancellation.clone());
        let lease = slot.lease(&cancellation).await?;
        let compiled = self.compiled.clone();
        let runtime = tokio::runtime::Handle::current();
        let binding = invocation
            .state
            .is_attached()
            .then(|| SessionBinding::from_state(invocation.state.clone()));
        let ingress = if compiled.session_script {
            Some(references::Ingress::new(&invocation, &cancellation).await?)
        } else {
            None
        };
        let commit_context = ingress.as_ref().map(|_| invocation.clone());
        let commit_runtime = runtime.clone();
        let commit_cancel_root = cancellation.clone();
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        tokio::task::spawn_blocking(move || {
            tracing::dispatcher::with_default(&dispatch, || {
                let epoch = slot.cancellation.clone();
                let ready = lease.ready();
                slot.run(lease, |cached| {
                    if ready {
                        // A replacement inherits only the settled permission to
                        // invoke. It never repeats the initialization export.
                        return Ok(());
                    }
                    let mut staged_refs = None;
                    let result = compiled.run(
                        cached,
                        cancellation,
                        epoch.clone(),
                        Some(runtime),
                        binding,
                        Some(invocation),
                        Some((String::new(), serde_json::json!({}))),
                        |bindings, store, boundary| {
                            store.data_mut().guest_observation = compiled
                                .observation_scope(Phase::Init, None)
                                .map(|scope| (scope, Phase::Init));
                            store.data_mut().references = ingress.clone();
                            let result = bindings.call_init(&mut *store).map_err(|error| {
                                compiled.execution_failure(boundary, error, Phase::Init)
                            })?;
                            match result {
                                InitResult::Ready => {
                                    staged_refs = store
                                        .data_mut()
                                        .references
                                        .as_mut()
                                        .and_then(references::Ingress::take);
                                    Ok(())
                                }
                                InitResult::Failed(error) => {
                                    let failure = crate::runtime::execution::ToolFailure::failed(
                                        error.code,
                                        error.message,
                                    );
                                    failure.validate()?;
                                    Err(Error::ToolFailure(failure))
                                }
                            }
                        },
                    );
                    let result = match (result, staged_refs) {
                        (Ok(()), Some(staged)) => {
                            if let Some(context) = commit_context.as_ref() {
                                let commit_cancel = commit_cancel_root.child_token();
                                commit_runtime.block_on(async {
                                    let commit = staged.commit(context, &commit_cancel);
                                    tokio::pin!(commit);
                                    tokio::select! { biased;
                                        _ = epoch.cancelled() => {
                                            commit_cancel.cancel();
                                            commit.await.map(|_| ())
                                        }
                                        res = &mut commit => res.map(|_| ()),
                                    }
                                })
                            } else {
                                Err(Error::denied("references admission missing"))
                            }
                        }
                        (other, _) => other,
                    };
                    if let Some(instance) = cached.as_mut() {
                        instance.init_ready = result.is_ok();
                        // Guest-reported failure is terminal too. A later tool must
                        // never silently recreate a store and repeat external effects.
                        instance.poisoned |= result.is_err();
                    }
                    result
                })
            })
        })
        .await
        .map_err(|_| Error::lifecycle("initialization worker failed; effects may be unknown"))?
    }
}

pub(super) fn cleanup_init_processes(state: &mut RuntimeState) -> Result<(), Error> {
    let Some(context) = state
        .tool_context
        .as_ref()
        .filter(|context| !context.subject.is_tool())
        .cloned()
    else {
        return Ok(());
    };
    let Some(runtime) = state.runtime.clone() else {
        return Ok(());
    };
    let handles: Vec<_> = state.process_grants.keys().copied().collect();
    let started = Instant::now();
    let result = runtime.block_on(async {
        let mut failed = false;
        for id in handles {
            let handle =
                microcrab_host_api::process::ProcessHandle(microcrab_host_api::HandleId(id));
            if context.processes.owns_process(handle)
                && context
                    .processes
                    .release_process(handle, CancellationToken::new())
                    .await
                    .is_err()
            {
                failed = true;
            }
        }
        if failed {
            Err(Error::lifecycle(
                "initialization process cleanup failed; inspect execution history",
            ))
        } else {
            Ok(())
        }
    });
    state.deadline += started.elapsed();
    if result.is_ok() {
        state.process_grants.clear();
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn initialization_proof_binds_exact_component_generation_and_attempt() {
        let mut invocation = ToolContext::detached(
            crate::SessionState::new(Default::default()),
            Default::default(),
        );
        invocation.call_id = "initialization-attempt".into();
        invocation.execution_id = Some(crate::runtime::execution::ExecutionId(1));
        invocation.subject = crate::policy::InvocationSubject::AgentInit {
            component: "component-a".into(),
            generation: 3,
            attempt: 4,
        };
        super::super::test_admission::admit(&mut invocation, Some("component-a".into()));
        assert!(invocation.ensure_admitted(Some("component-a")).is_ok());
        assert!(invocation.ensure_admitted(Some("component-b")).is_err());
        for subject in [
            crate::policy::InvocationSubject::Tool,
            crate::policy::InvocationSubject::AgentInit {
                component: "component-a".into(),
                generation: 3,
                attempt: 5,
            },
            crate::policy::InvocationSubject::AgentInit {
                component: "component-a".into(),
                generation: 4,
                attempt: 4,
            },
        ] {
            let mut rebound = invocation.clone();
            rebound.subject = subject;
            assert!(rebound.ensure_admitted(Some("component-a")).is_err());
        }
    }
}
