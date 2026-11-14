//! Session admission dispatcher and settled-boundary insertion. The waiting worker
//! retains only `Weak<SessionInner>`; its cloned synchronization ports cannot form
//! an Arc cycle, and close/drop cancels both notification and reservation waits.
#[cfg(test)]
mod tests;
use super::*;
use crate::admission::inbox::{Lane, StateDelivery};
use crate::runtime::handler::StepAdmission;

/// Selected copies stay immutable until append acknowledgement or guard release.
/// Its inbox port carries no Session owner; release wakes the existing observer.
pub(super) struct InputClaimGuard {
    inbox: Arc<crate::admission::inbox::Inbox>,
    ids: Vec<u64>,
}
impl Drop for InputClaimGuard {
    fn drop(&mut self) {
        self.inbox
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .release_claims(&self.ids);
        self.inbox.changed.notify_one();
    }
}
impl SessionInner {
    pub(super) fn input_claim(&self, ids: Vec<u64>) -> InputClaimGuard {
        InputClaimGuard {
            inbox: self.inbox.clone(),
            ids,
        }
    }
    pub(crate) fn with_inbox_snapshot<T>(
        &self,
        action: impl FnOnce(&crate::InboxSnapshot) -> T,
    ) -> T {
        let inbox = self
            .inbox
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        action(&inbox.snapshot())
    }
    pub(crate) fn update_pending_input(
        &self,
        inbox_id: &str,
        id: u64,
        revision: u64,
        input: Option<crate::SessionInput>,
        mode: Option<crate::PendingInputMode>,
    ) -> Result<crate::PendingInput, Error> {
        self.ensure_model_open()?;
        let item = self
            .inbox
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .update(inbox_id, id, revision, input, mode)?;
        self.inbox.steering_changed.notify_waiters();
        self.publish_activity();
        Ok(item)
    }
    pub(crate) fn remove_pending_input(
        &self,
        inbox_id: &str,
        id: u64,
        revision: u64,
    ) -> Result<crate::PendingInput, Error> {
        self.ensure_model_open()?;
        let item = self
            .inbox
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(inbox_id, id, revision)?;
        self.inbox.steering_changed.notify_waiters();
        self.publish_activity();
        Ok(item)
    }

    fn inbox_rejection(&self, error: Error) -> Error {
        if !matches!(error, Error::Cancelled) {
            self.inbox_validation_failed.store(true, Ordering::SeqCst);
        }
        error
    }

    pub(crate) fn inbox_snapshot(&self) -> crate::InboxSnapshot {
        self.inbox
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .snapshot()
    }
    pub(crate) fn pending_inputs(&self) -> Vec<crate::SessionInput> {
        self.inbox
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pending_inputs()
    }
    pub(crate) fn enqueue(
        self: &Arc<Self>,
        input: crate::SessionInput,
        steer: bool,
        submission_operation_id: Option<String>,
    ) -> Result<(), Error> {
        self.ensure_model_open()?;
        {
            let mut inbox = self.inbox.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(operation) = submission_operation_id {
                inbox.enqueue_submission(input, steer, Some(operation))?;
            } else {
                inbox.enqueue(input, steer)?;
            }
        }
        self.start_inbox_dispatcher();
        self.inbox.steering_changed.notify_waiters();
        self.publish_activity();
        if !steer {
            self.inbox.changed.notify_one();
        }
        Ok(())
    }
    pub(crate) fn enqueue_and_activate(
        self: &Arc<Self>,
        input: crate::SessionInput,
        submission_operation_id: Option<String>,
    ) -> Result<(), Error> {
        self.ensure_model_open()?;
        // Foreground is the execution/recovery owner, including pre-worker
        // reservations. Failure means busy: keep ordinary queue/pause semantics.
        if self.replacing.load(Ordering::Acquire) {
            return Err(Error::Busy);
        }
        let idle = self.foreground.clone().try_lock_owned().ok();
        {
            let mut inbox = self.inbox.state.lock().unwrap_or_else(|e| e.into_inner());
            if idle.is_none() && !inbox.active {
                return Err(Error::Busy);
            }
            // Admission must succeed before clearing pause, under the same lock.
            // Existing older entries retain FIFO precedence over this new input.
            inbox.enqueue_submission(input, false, submission_operation_id)?;
            if idle.is_some() {
                self.explicit_activation.store(true, Ordering::Release);
                inbox.paused = false;
            }
        }
        drop(idle);
        self.start_inbox_dispatcher();
        self.publish_activity();
        self.inbox.changed.notify_one();
        Ok(())
    }
    pub(super) fn start_inbox_dispatcher(self: &Arc<Self>) {
        if self.inbox_worker_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let weak = Arc::downgrade(self);
        let inbox = self.inbox.clone();
        let foreground = self.foreground.clone();
        let operation = self.operation.clone();
        let cancel = self.task_persistence_cancel.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! { biased;
                    _ = cancel.cancelled() => break,
                    _ = inbox.changed.notified() => {}
                }
                // Observe/empty/paused notifications only publish activity. They
                // never compete with a public operation for foreground ownership.
                let Some(inner) = weak.upgrade() else {
                    break;
                };
                if inner.ensure_open().is_err() {
                    break;
                }
                inner.publish_activity();
                let ready = inbox
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .has_next_turn();
                drop(inner);
                if !ready {
                    continue;
                }
                // An internal checkpoint may own storage. Wait without claiming
                // foreground so an explicit public reservation can wait normally.
                // Never await foreground while holding operation: public admission
                // takes the reverse order. A failed try gives that caller storage.
                let operation_guard = tokio::select! { biased;
                    _ = cancel.cancelled() => break,
                    guard = operation.clone().lock_owned() => guard,
                };
                let Ok(foreground_guard) = foreground.clone().try_lock_owned() else {
                    drop(operation_guard);
                    // Park behind the public owner with no storage reservation.
                    // No Session strong reference or inbox claim survives this
                    // cancelable wait. Release foreground immediately and retry
                    // the operation-first protocol after the owner's handoff.
                    let released = tokio::select! { biased;
                        _ = cancel.cancelled() => break,
                        guard = foreground.clone().lock_owned() => guard,
                    };
                    drop(released);
                    inbox.changed.notify_one();
                    continue;
                };
                let Some(inner) = weak.upgrade() else {
                    break;
                };
                if inner.ensure_open().is_err() {
                    break;
                }
                if inner.replacing.load(Ordering::Acquire) {
                    continue;
                }
                let explicit = inner.explicit_activation.swap(false, Ordering::AcqRel);
                if inner.has_continuation() && !explicit {
                    inbox.state.lock().unwrap_or_else(|e| e.into_inner()).paused = true;
                    inner.publish_activity();
                    continue;
                }
                // Recheck after both reservations; selection remains unconsumed
                // until execute's successful Context append acknowledges its id.
                let entry = inbox
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .next_turn();
                let Some(entry) = entry else {
                    continue;
                };
                let reservation = super::execution::OperationReservation::from_guards(
                    operation_guard,
                    foreground_guard,
                );
                // The id stays in the inbox throughout preparation and budget
                // validation; execute acknowledges it only after Context append.
                let _ = inner.execute_queued(entry, reservation).await;
            }
        });
    }
    pub(super) async fn admit_inbox_step(
        &self,
        context: &mut Context,
        allow_framework: bool,
        already_sending: bool,
        round: usize,
        cancellation: &CancellationToken,
    ) -> Result<StepAdmission, Error> {
        // Snapshot once. Arrivals during Host IO or hooks belong to the next
        // boundary; coalesced notice replacements have distinct ids and survive.
        let snapshot = self
            .inbox
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .step(true, already_sending);
        let _claim = self.input_claim(snapshot.users.iter().map(|entry| entry.id).collect());
        self.publish_activity();
        let mut prepared = Vec::new();
        let hook_state = self.state.preview();
        let context_bytes = {
            self.context
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .max_bytes()
        };
        for user in &snapshot.users {
            let mut input = crate::admission::input::prepare(
                user.input.clone(),
                self.host.as_deref(),
                self.attachment_resolver.as_deref(),
                context_bytes,
                cancellation.clone(),
            )
            .await
            .map_err(|error| self.inbox_rejection(error))?;
            let injection = crate::observation::hooks::intervene_input(
                self.auditor.as_ref(),
                &hook_state,
                &input,
                cancellation,
            )
            .await?;
            input.variables.overlay(&injection.variables);
            hook_state.apply_change(input.variables.clone())?;
            prepared.push(input);
        }
        let facts = serde_json::to_value(
            snapshot
                .notices
                .iter()
                .map(|entry| (&entry.notice.key, &entry.notice.fact))
                .collect::<Vec<_>>(),
        )?;
        let ids = snapshot
            .notices
            .iter()
            .map(|entry| match &entry.notice.key {
                crate::admission::inbox::NoticeKey::Execution { execution_id, .. } => *execution_id,
            })
            .collect::<Vec<_>>();
        let framework = snapshot.notices.iter().any(|entry| {
            entry.lane == Lane::NextStep
                && matches!(
                    entry.notice.delivery,
                    StateDelivery::Continue | StateDelivery::Wake
                )
        }) && (context.state_key_changed("notices", &facts)
            || context.execution_metadata_changed(&ids, &self.state.model_variables()?));
        if prepared.is_empty() && !already_sending && !framework {
            return Ok(StepAdmission::default());
        }
        if prepared.is_empty() && !allow_framework {
            // Preserve notices and the round identity until the user renews or stops.
            return Ok(StepAdmission {
                blocked: true,
                ..Default::default()
            });
        }
        crate::runtime::handler::start_round(
            &self.state,
            round,
            self.auditor.as_ref(),
            cancellation,
        )
        .await?;
        let _fence = self.endpoint.fence().await?;
        let original = self.state.preview();
        let preview = original.preview();
        let (candidate, _merged) = crate::admission::input::stage_step(
            context,
            &preview,
            &prepared,
            (!snapshot.notices.is_empty()).then_some(facts),
            self.state().limits.max_output_tokens,
        )
        .map_err(|error| self.inbox_rejection(error))?;
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if !self.state.commit_input(&original, &preview) {
            return Err(Error::StateObservationConflict);
        }
        let (epoch, items) = candidate.admitted_items();
        // stage_step appends one user item per claimed entry after its State suffix.
        let first = items.len() - snapshot.users.len();
        let submissions: Vec<_> = snapshot
            .users
            .iter()
            .enumerate()
            .filter_map(|(index, entry)| {
                entry
                    .submission_operation_id
                    .clone()
                    .map(|operation| (first + index, operation))
            })
            .collect();
        *context = candidate;
        self.inbox
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .acknowledge(
                &snapshot
                    .users
                    .iter()
                    .map(|entry| entry.id)
                    .collect::<Vec<_>>(),
                &snapshot
                    .notices
                    .iter()
                    .map(|entry| entry.id)
                    .collect::<Vec<_>>(),
            );
        // Claims are consumed before fallible external observation can run.
        if !submissions.is_empty() {
            self.client
                .observe_input_admitted(epoch, context.admitted_items().1, &submissions)
                .await;
        }
        Ok(StepAdmission {
            user: !prepared.is_empty(),
            send: true,
            blocked: false,
        })
    }
}

/// Registry callbacks use only State's independent notice port, never LiveState,
/// Context, activity publication, or any registry from this synchronous adapter.
pub(super) struct SessionNoticeSink {
    pub inbox: Arc<crate::admission::inbox::Inbox>,
    pub state: State,
}
impl crate::runtime::execution::ExecutionNoticeSink for SessionNoticeSink {
    fn changed(&self, event: crate::runtime::execution::ExecutionNotice) {
        use crate::admission::inbox::{NoticeFact, NoticeKey, NoticeKind, StateNotice};
        if !event.result_delivered {
            return;
        }
        let row = &event.row;
        let previous = event.previous.as_ref();
        let (delivery, change) = if !row.status.is_pending()
            && previous.is_none_or(|previous| previous.status.is_pending())
        {
            (StateDelivery::Wake, NoticeKind::Terminal)
        } else if matches!(
            row.status,
            crate::runtime::execution::ExecutionStatus::Running
                | crate::runtime::execution::ExecutionStatus::Waiting
        ) && previous.is_some_and(|previous| previous.status != row.status)
        {
            (StateDelivery::Wake, NoticeKind::Lifecycle)
        } else if previous.is_some_and(|previous| {
            let occupancy = |output: Option<&crate::runtime::execution::OutputMetadata>| {
                output
                    .map(|output| (output.start_cursor, output.end_cursor))
                    .unwrap_or((0, 0))
            };
            occupancy(previous.output.as_ref()) != occupancy(row.output.as_ref())
        }) {
            (StateDelivery::Continue, NoticeKind::Metadata)
        } else {
            (StateDelivery::Passive, NoticeKind::Metadata)
        };
        let notice = StateNotice {
            delivery,
            key: NoticeKey::Execution {
                session_id: row.session_id.clone(),
                execution_id: row.execution_id,
                change,
            },
            fact: NoticeFact {
                status: row.status,
                failure: row.failure.clone(),
                exit_code: None,
            },
        };
        if self.state.post_notice(notice).is_err() {
            // Refuse silent loss. The retained execution row remains inspectable
            // and automatic model admission pauses; no LiveState reentry.
            self.inbox
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .paused = true;
            self.inbox.changed.notify_one();
        }
    }
}
