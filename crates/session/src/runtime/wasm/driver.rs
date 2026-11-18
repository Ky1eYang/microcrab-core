//! Real epoch-bound guest continuations. Runtime storage never borrows linear memory.
use super::{Compiled, Instance, SessionBinding, ToolContext, epoch::Slot, output};
use crate::{Error, runtime::execution::OutputWriter};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use wasmtime::component::Resource;

/// Rebind the live counters and producer cancellation per step while retaining
/// the exact invocation conduit. Reconstructing through `ToolContext::new` would
/// mint a different process wrapper and lose the sealed argument/redaction binding.
pub(super) struct Scope {
    invocation: ToolContext,
}
impl Scope {
    pub fn capture(context: &ToolContext) -> Self {
        let mut invocation = context.clone();
        // Progress is supplied by the execution owner, not borrowed by a later
        // guest step. The original admission and execution identities remain.
        invocation.execution_progress = None;
        Self { invocation }
    }
    fn context(&self, cancel: CancellationToken) -> ToolContext {
        let mut context = self.invocation.clone();
        let snapshot = context.state.snapshot();
        context.cancellation = cancel;
        context.turn_id = snapshot.turn_id;
        context.round_id = snapshot.round_id;
        context
    }
}
fn interrupt(writer: &OutputWriter, error: impl std::fmt::Display) {
    let _ = writer.interrupt(format!("Guest producer interrupted: {error}"));
}

pub(super) fn start(
    compiled: Arc<Compiled>,
    slot: Arc<Slot>,
    scope: Scope,
    mut source: output::Entry,
) {
    let Some(mut state) = source.driver.take() else {
        return;
    };
    let writer = source.writer.clone();
    tokio::spawn(async move {
        loop {
            if slot.cancellation.is_cancelled() {
                interrupt(&writer, "Session epoch ended");
                break;
            }
            let is_cancelled = source.controller.cancellation.is_cancelled();
            // A fresh token allows one bounded cooperative cleanup step after an
            // explicit cancellation; the driver receives cancelled=true. Epoch
            // cancellation always interrupts rather than reentering a dead Store.
            let cancellation = if is_cancelled {
                CancellationToken::new()
            } else {
                source.controller.cancellation.child_token()
            };
            let lease = if is_cancelled {
                match slot.try_lease() {
                    Some(lease) => lease,
                    None => {
                        let _ = writer.confirm_cancelled();
                        break;
                    }
                }
            } else {
                match slot.lease(&cancellation).await {
                    Ok(lease) => lease,
                    Err(error) => {
                        if source.controller.cancellation.is_cancelled() {
                            let _ = writer.confirm_cancelled();
                        } else {
                            interrupt(&writer, error);
                        }
                        break;
                    }
                }
            };
            let worker_compiled = compiled.clone();
            let worker_slot = slot.clone();
            let context = scope.context(cancellation.clone());
            let binding = if context.state.is_attached() {
                Some(SessionBinding::from_state(context.state.clone()))
            } else {
                None
            };
            let operation_scope = Some((context.tool.clone(), serde_json::Value::Null));
            let retained = source.retained.clone();
            let entry = output::Entry {
                writer: writer.clone(),
                output: None,
                controller: source.controller.clone(),
                channels: source.channels.clone(),
                driver: None,
                retained: Default::default(),
                cleanup: None,
                attached: true,
            };
            let runtime = tokio::runtime::Handle::current();
            let dispatch = tracing::dispatcher::get_default(Clone::clone);
            let result = tokio::task::spawn_blocking(move || {
                tracing::dispatcher::with_default(&dispatch, || {
                    worker_slot.run(lease, |cached: &mut Option<Instance>| {
                        worker_compiled.run(
                            cached,
                            cancellation,
                            worker_slot.cancellation.clone(),
                            Some(runtime),
                            binding,
                            Some(context),
                            operation_scope,
                            |bindings, store, boundary| {
                                store.data_mut().borrowed_processes =
                                    retained.keys().copied().collect();
                                store.data_mut().process_grants = retained;
                                let resource = output::insert(store.data_mut(), entry)?;
                                let borrowed = Resource::new_borrow(resource.rep());
                                // Mid-step Job cancellation must reach Host imports as
                                // a typed error, not trap the shared Store on resume.
                                // Fuel, wall deadline and epoch invalidation still stop
                                // the guest; the invocation guard resets this policy.
                                store.data_mut().cooperative_driver_cancellation = true;
                                let result = bindings
                                    .call_drive(&mut *store, borrowed, &state, is_cancelled)
                                    .map_err(|error| {
                                        boundary
                                            .failure(error, crate::observation::wasm::Phase::Drive)
                                    })?;
                                match result {
                                    Ok(step) => {
                                        crate::budget::check(
                                            crate::LimitKind::WasmBytes,
                                            step.state.len(),
                                            worker_compiled.limits.max_input_bytes,
                                        )
                                        .map_err(
                                            |error| match error {
                                                Error::Limit(limit) => {
                                                    Error::Limit(limit.with_label(
                                                        "Guest continuation state bytes",
                                                    ))
                                                }
                                                error => error,
                                            },
                                        )?;
                                        Ok(Ok(step))
                                    }
                                    Err(failure) => Ok(Err(output::native_failure(failure)?)),
                                }
                            },
                        )
                    })
                })
            })
            .await;
            match result {
                Ok(Ok(Ok(step))) => {
                    if is_cancelled {
                        let _ = writer.confirm_cancelled();
                        break;
                    }
                    if step.finished {
                        let _ = writer.finish(Ok(()));
                        break;
                    }
                    state = step.state;
                    // An explicit wake request is time-sliced; zero never becomes
                    // an unbounded empty busy loop starving the executor.
                    let delay = Duration::from_millis(step.wake_after_ms.clamp(1, 60_000));
                    tokio::select! {biased;_=slot.cancellation.cancelled()=>{interrupt(&writer,"Session epoch ended");break},_=source.controller.cancellation.cancelled()=>{},_=tokio::time::sleep(delay)=>{}}
                }
                Ok(Ok(Err(failure))) => {
                    let _ = writer.finish(Err(failure));
                    break;
                }
                Ok(Err(error)) => {
                    if source.controller.cancellation.is_cancelled() {
                        let _ = writer.confirm_cancelled();
                    } else {
                        interrupt(&writer, error);
                    }
                    break;
                }
                Err(error) => {
                    if source.controller.cancellation.is_cancelled() {
                        let _ = writer.confirm_cancelled();
                    } else {
                        interrupt(&writer, error);
                    }
                    break;
                }
            }
        }
    });
}

#[cfg(test)]
#[path = "driver/cancellation_tests.rs"]
mod cancellation_tests;

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn guest_step_retains_shared_redaction_when_original_scope_rotates() {
        use crate::runtime::execution::InvocationRedaction;
        use std::collections::BTreeMap;
        use std::sync::atomic::AtomicBool;
        let (state, _endpoint) = crate::State::new(Default::default()).bind_writer(
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        );
        state
            .prepare_secret_write(BTreeMap::from([("token".into(), "PRIVATE_OLD".into())]))
            .await
            .unwrap()
            .commit()
            .unwrap();
        let original = ToolContext::new(
            state.clone(),
            state.processes(),
            CancellationToken::new(),
            "call".into(),
            "tool".into(),
            "session".into(),
            "agent".into(),
            None,
        );
        original
            .redaction_handle()
            .merge(&InvocationRedaction::from_snapshot(state.secret_snapshot()).unwrap())
            .unwrap();
        let scope = Scope::capture(&original);
        let continued = scope.context(CancellationToken::new());
        state
            .prepare_secret_write(BTreeMap::from([("token".into(), "PRIVATE_NEW".into())]))
            .await
            .unwrap()
            .commit()
            .unwrap();
        original
            .redaction_handle()
            .merge(&InvocationRedaction::from_snapshot(state.secret_snapshot()).unwrap())
            .unwrap();
        assert!(Arc::ptr_eq(&original.processes, &continued.processes));
        assert_eq!(
            continued
                .redaction_handle()
                .redact_text("PRIVATE_OLD PRIVATE_NEW"),
            "{{secret:token}} {{secret:token}}"
        );
    }
    #[test]
    fn guest_step_rebinds_live_state_and_producer_cancellation() {
        let invocation = ToolContext::detached(
            crate::SessionState::new(Default::default()),
            CancellationToken::new(),
        );
        let original = invocation.cancellation.clone();
        let scope = Scope::capture(&invocation);
        original.cancel();
        invocation.state.update(|snapshot| {
            snapshot.turn_id = Some(20);
            snapshot.round_id = Some(21);
        });
        let producer = CancellationToken::new();
        let step = scope.context(producer.child_token());
        assert!(!step.cancellation.is_cancelled());
        assert_eq!(step.turn_id, Some(20));
        assert_eq!(step.round_id, Some(21));
        assert_eq!(step.call_id, invocation.call_id);
        producer.cancel();
        assert!(step.cancellation.is_cancelled());
    }
}

#[cfg(test)]
mod proof_tests {
    use super::*;
    use crate::policy::InvocationSubject;
    #[test]
    fn continuation_retains_original_component_subject_and_exact_proof() {
        let mut original = ToolContext::detached(
            crate::SessionState::new(Default::default()),
            Default::default(),
        );
        original.call_id = "original-call".into();
        original.tool = "producer".into();
        super::super::test_admission::admit(&mut original, Some("component-a".into()));
        let scope = Scope::capture(&original);
        original.state.update(|snapshot| {
            snapshot.turn_id = Some(123);
        });
        let continued = scope.context(CancellationToken::new());
        assert!(Arc::ptr_eq(&original.processes, &continued.processes));
        assert!(Arc::ptr_eq(
            original.admission.as_ref().unwrap(),
            continued.admission.as_ref().unwrap()
        ));
        assert!(continued.ensure_admitted(Some("component-a")).is_ok());
        assert!(continued.ensure_admitted(Some("component-b")).is_err());
        let mut changed = continued.clone();
        changed.subject = InvocationSubject::AgentInit {
            component: "component-a".into(),
            generation: 1,
            attempt: 1,
        };
        assert!(changed.ensure_admitted(Some("component-a")).is_err());
        original.cancellation.cancel();
        assert!(matches!(
            continued.ensure_admitted(Some("component-a")),
            Err(crate::policy::PermissionError::Cancelled)
        ));
    }
}
