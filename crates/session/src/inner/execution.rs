//! Own admitted work through caller cancellation, including Host-backed input preparation.
use super::*;
use crate::{SessionInput, state::communication::reply::DialogueTerminalReason};

struct LiveMeasurementGuard<'a>(&'a SessionInner);
impl Drop for LiveMeasurementGuard<'_> {
    fn drop(&mut self) {
        self.0
            .live_context_measurement
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        self.0.context_measurement_changes.send_replace(());
    }
}

pub(crate) struct OperationReservation {
    // Release storage before foreground admission. A public caller is never
    // mistaken for an internal worker, including while waiting for its commit.
    pub(crate) interrupt_reason: Arc<Mutex<Option<crate::InterruptReason>>>,
    pending: Mutex<Option<PendingResume>>,
    pub(crate) submission_operation_id: Option<String>,
    pub(crate) admission_resolver: Option<Arc<dyn crate::InputAdmissionResolver>>,
    resume_pending: bool,
    pub(crate) user_target: Option<bool>,
    pub(crate) carrier_run: Vec<crate::admission::inbox::IdleCarrier>,
    _operation: tokio::sync::OwnedMutexGuard<()>,
    _foreground: tokio::sync::OwnedMutexGuard<()>,
}
/// A reusable replacement owns admission until all old work has acknowledged
/// settlement. Dropping a cancelled caller releases only the fence, never a worker.
pub(super) struct ReplacementReservation {
    inner: Arc<SessionInner>,
    _operation: OperationReservation,
    _replacement: tokio::sync::OwnedMutexGuard<()>,
}
impl Drop for ReplacementReservation {
    fn drop(&mut self) {
        self.inner.replacing.store(false, Ordering::Release);
        self.inner.inbox.changed.notify_one();
    }
}
struct PendingResume {
    entry: crate::admission::inbox::UserEntry,
    _claim: super::insertion::InputClaimGuard,
}
impl OperationReservation {
    pub(super) fn from_guards(
        operation: tokio::sync::OwnedMutexGuard<()>,
        foreground: tokio::sync::OwnedMutexGuard<()>,
    ) -> Self {
        Self {
            _operation: operation,
            _foreground: foreground,
            interrupt_reason: Arc::new(Mutex::new(None)),
            pending: Mutex::new(None),
            submission_operation_id: None,
            admission_resolver: None,
            resume_pending: false,
            user_target: None,
            carrier_run: Vec::new(),
        }
    }
}
impl SessionInner {
    pub(crate) async fn submit_user(
        self: &Arc<Self>,
        input: SessionInput,
        targeted_child: bool,
    ) -> Result<TurnOutcome, Error> {
        self.require_user_input(targeted_child)?;
        // A retiring conversation must reject new input concretely even when
        // an earlier foreground reservation has not yet been released.
        self.ensure_model_open()?;
        let mut reservation = self.reserve().await?;
        reservation.user_target = Some(targeted_child);
        self.submit_reserved(
            Some(input),
            None,
            Arc::new(reservation),
            CancellationToken::new(),
        )
        .await
    }
    pub(crate) async fn submit(
        self: &Arc<Self>,
        input: SessionInput,
        events: Option<tokio::sync::mpsc::Sender<crate::ModelEvent>>,
    ) -> Result<TurnOutcome, Error> {
        crate::state::access::validate_change(&input.variables)?;
        self.execute(Some(input), events, None, None, false).await
    }
    pub(crate) async fn resume(
        self: &Arc<Self>,
        events: Option<tokio::sync::mpsc::Sender<crate::ModelEvent>>,
    ) -> Result<TurnOutcome, Error> {
        self.execute(None, events, None, None, false).await
    }
    pub(crate) async fn reserve(&self) -> Result<OperationReservation, Error> {
        self.ensure_open()?;
        if self.replacing.load(Ordering::Acquire) {
            return Err(Error::Busy);
        }
        let foreground = self
            .foreground
            .clone()
            .try_lock_owned()
            .map_err(|_| Error::Busy)?;
        let operation = self.operation.clone().lock_owned().await;
        self.ensure_open()?;
        if self.replacing.load(Ordering::Acquire) {
            return Err(Error::Cancelled);
        }
        Ok(OperationReservation {
            _operation: operation,
            _foreground: foreground,
            interrupt_reason: Arc::new(Mutex::new(None)),
            pending: Mutex::new(None),
            submission_operation_id: None,
            admission_resolver: None,
            resume_pending: false,
            user_target: None,
            carrier_run: Vec::new(),
        })
    }
    pub(super) async fn reserve_replacement(
        self: &Arc<Self>,
    ) -> Result<ReplacementReservation, Error> {
        self.ensure_open()?;
        let replacement = self.replacement.clone().lock_owned().await;
        self.replacing.store(true, Ordering::Release);
        // Arm cancellation before waiting for foreground, including a reservation
        // whose token has not registered yet. Registration rechecks replacing.
        self.interrupt_with_reason(crate::InterruptReason::StateReplaced);
        // Own the reset guard before any await so caller cancellation cannot seal
        // the Session forever. Empty guards are not manufactured.
        struct Reset(Option<Arc<SessionInner>>);
        impl Drop for Reset {
            fn drop(&mut self) {
                if let Some(inner) = &self.0 {
                    inner.replacing.store(false, Ordering::Release);
                    inner.inbox.changed.notify_one();
                }
            }
        }
        let mut reset = Reset(Some(self.clone()));
        self.close_rename().await;
        let foreground = self.foreground.clone().lock_owned().await;
        let operation = self.operation.clone().lock_owned().await;
        self.ensure_open()?;
        reset.0 = None;
        let reservation = ReplacementReservation {
            inner: self.clone(),
            _operation: OperationReservation::from_guards(operation, foreground),
            _replacement: replacement,
        };
        if self.state.live_dialogue_turn().is_none() {
            return Ok(reservation);
        }
        // A dropped replacement caller must not abandon the terminal write or
        // release the foreground fence while the reply adapter still owns it.
        tokio::spawn(async move {
            let mut guard = FaultOnDrop {
                faulted: &reservation.inner.faulted,
                armed: true,
            };
            reservation
                .inner
                .settle_live_external_dialogues(DialogueTerminalReason::Interrupted)
                .await?;
            guard.armed = false;
            drop(guard);
            Ok(reservation)
        })
        .await
        .map_err(|_| {
            Error::lifecycle("external dialogue replacement settlement stopped unexpectedly")
        })?
    }
    pub(crate) async fn reserve_pending_input(
        self: &Arc<Self>,
        inbox_id: &str,
        id: u64,
        revision: u64,
        cancellation: &CancellationToken,
    ) -> Result<OperationReservation, Error> {
        let mut reservation = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(Error::Cancelled),
            result = self.reserve() => result?,
        };
        self.ensure_model_open()?;
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let entry = self
            .inbox
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .claim_pending_resume(inbox_id, id, revision)?;
        let claim = self.input_claim(vec![entry.id]);
        *reservation
            .pending
            .get_mut()
            .unwrap_or_else(|e| e.into_inner()) = Some(PendingResume {
            entry,
            _claim: claim,
        });
        reservation.resume_pending = true;
        self.publish_activity();
        Ok(reservation)
    }
    pub(crate) async fn submit_reserved(
        self: &Arc<Self>,
        input: Option<SessionInput>,
        events: Option<tokio::sync::mpsc::Sender<crate::ModelEvent>>,
        reservation: Arc<OperationReservation>,
        cancellation: CancellationToken,
    ) -> Result<TurnOutcome, Error> {
        let pending = reservation
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(pending) = pending {
            let _claim = pending._claim;
            return self
                .execute(
                    Some(pending.entry.input),
                    events,
                    Some((reservation, cancellation)),
                    Some(pending.entry.id),
                    false,
                )
                .await;
        }
        if let Some(input) = &input {
            crate::state::access::validate_change(&input.variables)?;
        }
        self.execute(
            input,
            events,
            Some((reservation, cancellation)),
            None,
            false,
        )
        .await
    }
    pub(super) async fn execute_queued(
        self: &Arc<Self>,
        entry: crate::admission::inbox::TurnEntry,
        mut reservation: OperationReservation,
    ) -> Result<TurnOutcome, Error> {
        if let crate::admission::inbox::TurnEntry::Carriers(run) = &entry {
            reservation.carrier_run = run.clone();
        }
        self.execute(
            Some(entry.input()?),
            None,
            Some((Arc::new(reservation), CancellationToken::new())),
            Some(entry.id()),
            entry.framework(),
        )
        .await
    }
    async fn execute(
        self: &Arc<Self>,
        mut input: Option<SessionInput>,
        events: Option<tokio::sync::mpsc::Sender<crate::ModelEvent>>,
        reservation: Option<(Arc<OperationReservation>, CancellationToken)>,
        mut queued: Option<u64>,
        mut framework: bool,
    ) -> Result<TurnOutcome, Error> {
        let mut claim = queued
            .filter(|_| !framework)
            .map(|id| self.input_claim(vec![id]));
        self.ensure_model_open()?;
        self.start_inbox_dispatcher();
        let resume_pending = reservation
            .as_ref()
            .is_some_and(|(owner, _)| owner.resume_pending);
        let autonomous = queued.is_some() && !resume_pending;
        let explicit_retry = reservation.is_some() && input.is_none();
        let (operation, token) = match reservation {
            Some(owned) => owned,
            None => (Arc::new(self.reserve().await?), CancellationToken::new()),
        };
        let mut selected_carriers = operation.carrier_run.clone();
        {
            // Publish cancellation while sharing interrupt's active mutex. Close
            // can seal before this lock or cancel the installed token afterward;
            // there is no unobservable token-installation gap.
            let mut active = self.active.lock().unwrap_or_else(|e| e.into_inner());
            self.ensure_model_open()?;
            if self.replacing.load(Ordering::Acquire) {
                if !token.is_cancelled() {
                    *operation
                        .interrupt_reason
                        .lock()
                        .unwrap_or_else(|e| e.into_inner()) =
                        Some(crate::InterruptReason::StateReplaced);
                }
                token.cancel();
                return Err(Error::Cancelled);
            }
            if resume_pending && token.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let mut inbox = self.inbox.state.lock().unwrap_or_else(|e| e.into_inner());
            // [!important] Queue, steer and pending mutations preserve pause.
            // Explicit pending resume clears it only after claiming the exact entry.
            if queued.is_none() || resume_pending {
                inbox.paused = false;
            }
            if queued.is_none()
                && input.is_none()
                && (!self.has_continuation() || (!explicit_retry && self.retry_ready()))
                && let Some(entry) = inbox.next_turn()
            {
                if let crate::admission::inbox::TurnEntry::Carriers(run) = &entry {
                    selected_carriers = run.clone();
                }
                input = Some(entry.input()?);
                queued = Some(entry.id());
                framework = entry.framework();
                claim = (!framework).then(|| self.input_claim(vec![entry.id()]));
            }
            inbox.begin_turn();
            inbox.activate_notices();
            *active = Some(super::commands::ActiveOperation {
                token: token.clone(),
                reason: operation.interrupt_reason.clone(),
            });
        }
        self.publish_activity();
        if let Err(error) = self.ensure_model_open() {
            token.cancel();
            self.active.lock().unwrap_or_else(|e| e.into_inner()).take();
            self.inbox
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .end_turn(true);
            self.publish_activity();
            return Err(error);
        }
        self.inbox_validation_failed.store(false, Ordering::SeqCst);
        let (submission_operation_id, admission_resolver) =
            if let Some(id) = queued.filter(|_| !framework) {
                let inbox = self.inbox.state.lock().unwrap_or_else(|e| e.into_inner());
                inbox.claimed_user(id).map_or((None, None), |entry| {
                    (
                        entry.submission_operation_id.clone(),
                        entry.admission_resolver.clone(),
                    )
                })
            } else {
                (
                    operation.submission_operation_id.clone(),
                    operation.admission_resolver.clone(),
                )
            };
        let inner = self.clone();
        let worker_token = token.clone();
        let worker = tokio::spawn(async move {
            let user_target = operation
                .user_target
                .or_else(|| (queued.is_some() && !framework).then_some(false));
            let carrier_run = selected_carriers;
            let _operation = operation;
            let _claim = claim;
            let mut turn_guard = TurnGuard {
                inner: inner.clone(),
                clear: false,
            };
            let mut admitted = false;
            let result = AssertUnwindSafe(inner.run_turn(
                input,
                worker_token,
                &mut admitted,
                events,
                queued,
                framework,
                submission_operation_id,
                admission_resolver.as_deref(),
                user_target,
                carrier_run.clone(),
            ))
            .catch_unwind()
            .await;
            turn_guard.clear = admitted;
            let mut outcome = match result {
                Ok(result) => result,
                Err(_) => Err(Error::tool("session turn panicked")),
            };
            if admitted {
                let kind = match &outcome {
                    Ok(_) => AuditOutcome::Accepted,
                    Err(Error::Cancelled) => AuditOutcome::Cancelled,
                    Err(Error::Limit(_)) => AuditOutcome::LimitExceeded,
                    Err(Error::Protocol(_)) => AuditOutcome::InvalidResponse,
                    Err(_) => AuditOutcome::Unavailable,
                };
                if let Err(error) = inner
                    .boundary(Boundary::TurnEnded, Some(kind), &CancellationToken::new())
                    .await
                {
                    inner.faulted.store(true, Ordering::SeqCst);
                    if outcome.is_ok() {
                        outcome = Err(error);
                    }
                }
                // Provider failures occur before a response/tool batch is
                // admitted. Preserve the settled Context and allow another input;
                // uncertain tool, protocol and lifecycle failures stay quarantined.
                if !inner.inbox_validation_failed.load(Ordering::SeqCst)
                    && outcome.as_ref().is_err_and(|error| {
                        !(inner.model_retry_ready.load(Ordering::Acquire) && inner.retry_ready())
                            && !matches!(
                                error,
                                Error::Cancelled
                                    | Error::StateObservationConflict
                                    | Error::ProviderStatus { .. }
                                    | Error::TransportFailure(_)
                                    | Error::Transport
                                    | Error::ProviderTimeout
                                    | Error::ProviderIdleTimeout(_)
                                    | Error::Provider(_)
                            )
                            && !matches!(error, Error::Limit(limit) if limit.is_pre_effect())
                    })
                {
                    inner.faulted.store(true, Ordering::SeqCst);
                }
            }
            if admitted
                && !inner.faulted.load(Ordering::SeqCst)
                && !inner.closed.load(Ordering::SeqCst)
            {
                let needs_persist = inner
                    .continuation
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .is_some()
                    || outcome.is_err();
                if needs_persist {
                    let context = inner
                        .context
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .clone();
                    let continuation = if outcome.as_ref().is_err_and(|error| {
                        (inner.model_retry_ready.load(Ordering::Acquire) && inner.retry_ready())
                            || matches!(
                                error,
                                Error::ProviderStatus { .. }
                                    | Error::TransportFailure(_)
                                    | Error::Transport
                                    | Error::ProviderTimeout
                                    | Error::ProviderIdleTimeout(_)
                                    | Error::Provider(_)
                            )
                            || matches!(error, Error::Limit(limit) if limit.is_pre_effect())
                            || (matches!(error, Error::Cancelled)
                                && inner
                                    .auto_compaction_interrupted
                                    .swap(false, Ordering::SeqCst))
                    }) {
                        inner
                            .continuation
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .clone()
                    } else {
                        None
                    };
                    if let Err(error) = inner.persist(&context, continuation).await {
                        outcome = Err(error);
                    }
                }
            }
            if admitted
                && !inner.faulted.load(Ordering::SeqCst)
                && !inner.closed.load(Ordering::SeqCst)
                && !inner.has_continuation()
            {
                let context = inner
                    .context
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .clone();
                if let Err(error) = inner.persist_inter_turn(&context).await {
                    outcome = Err(error);
                }
            }
            if admitted {
                let can_resume = !inner.faulted.load(Ordering::Acquire)
                    && !inner.closed.load(Ordering::Acquire)
                    && !inner.replacing.load(Ordering::Acquire)
                    && !matches!(outcome, Err(Error::Cancelled))
                    && inner.has_continuation();
                if !can_resume {
                    let reason = match outcome {
                        Ok(_) => DialogueTerminalReason::Completed,
                        Err(Error::Cancelled) => DialogueTerminalReason::Cancelled,
                        Err(_) => DialogueTerminalReason::Interrupted,
                    };
                    if let Err(error) = inner.settle_live_external_dialogues(reason).await {
                        inner.faulted.store(true, Ordering::Release);
                        outcome = Err(error);
                    }
                }
            }
            if let Ok(result) = &outcome
                && let Err(error) = inner.publish_completed(result, autonomous)
            {
                outcome = Err(error);
            }
            if autonomous && let Err(error) = &outcome {
                let reason = *_operation
                    .interrupt_reason
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                inner.publish_inbox_failure(error, reason);
            }
            let pause = outcome.is_err() || inner.has_continuation();
            inner
                .inbox
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .end_turn(pause);
            // Publish quiescence before releasing the reservation; the dispatcher
            // waits for that exact foreground handoff before taking any item.
            inner.publish_activity();
            inner.inbox.changed.notify_one();
            outcome
        });
        let _cancel = CancelOnDrop(token);
        worker
            .await
            .map_err(|_| Error::tool("session worker stopped unexpectedly"))?
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "Turn orchestration carries distinct input, cancellation, routing and durable carrier admission facts"
    )]
    async fn run_turn(
        &self,
        input: Option<SessionInput>,
        cancellation: CancellationToken,
        admitted: &mut bool,
        events: Option<tokio::sync::mpsc::Sender<crate::ModelEvent>>,
        queued: Option<u64>,
        framework: bool,
        submission_operation_id: Option<String>,
        admission_resolver: Option<&dyn crate::InputAdmissionResolver>,
        user_target: Option<bool>,
        carrier_run: Vec<crate::admission::inbox::IdleCarrier>,
    ) -> Result<TurnOutcome, Error> {
        if input.is_some()
            && let Some(targeted) = user_target
        {
            self.require_user_input(targeted)?;
        }
        self.model_retry_ready.store(false, Ordering::Release);
        if input.is_none() && cancellation.is_cancelled() {
            // The retry never entered Runtime and has no new effects. Preserve
            // the previously prepared durable boundary without claiming a new one.
            return Err(Error::Cancelled);
        }
        self.auto_compaction_interrupted
            .store(false, Ordering::SeqCst);
        // Epoch overflow is an admission failure, before even Host-backed input resolution.
        if input.is_some() {
            self.context
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .next_model_epoch()?;
        }
        let input = match input {
            Some(input) => {
                let context_bytes = {
                    self.context
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .max_bytes()
                };
                Some(
                    crate::admission::input::prepare(
                        input,
                        self.host.as_deref(),
                        self.attachment_resolver.as_deref(),
                        context_bytes,
                        cancellation.clone(),
                    )
                    .await?,
                )
            }
            None => None,
        };
        let route_changed = if input.is_some() {
            match &self.route {
                Some(route) => match route.prepare().await? {
                    None => false,
                    Some(effect) if effect == crate::ConfigurationEffect::MODEL_SELECTION => true,
                    Some(_) => {
                        return Err(Error::config(
                            "route owner returned an unsupported admission effect",
                        ));
                    }
                },
                None => false,
            }
        } else {
            false
        };
        let catalog = if input.is_some() {
            self.pending_tools
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
                .filter(|(generation, _)| *generation != self.applied_tools.load(Ordering::Acquire))
        } else {
            None
        };
        if let Some(input) = &input {
            if self.has_continuation() && !self.retry_ready() && !route_changed && catalog.is_none()
            {
                return Err(Error::config(
                    "resume or recover before submitting a new turn",
                ));
            }
            let mut preview_context = self
                .context
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            if route_changed {
                preview_context.prepare_protocol_epoch()?;
            }
            if let Some((_, tools)) = &catalog {
                let registry = self.runtime.prepare_external_tools(tools.clone())?;
                preview_context.replace_tools_for_environment(registry.definitions())?;
            }
            let preview = self.state.preview();
            preview.install_switch_mode_availability(switch_mode_available(
                self.kind,
                preview_context.frozen_tools(),
            ));
            if preview_context.pending_epoch() {
                preview.refresh_preview(Boundary::EpochEnded)?;
                preview_context.materialize_epoch()?;
                preview.update(|state| {
                    state.epoch_id = preview_context.epoch();
                    state.turn_id = None;
                    state.round_id = None;
                    state.last_turn_id = 0;
                });
                preview.refresh_preview(Boundary::EpochStarted)?;
            }
            preview.apply_change(input.variables.clone())?;
            let turn = preview
                .snapshot()
                .last_turn_id
                .checked_add(1)
                .ok_or_else(|| {
                    crate::budget::reject(crate::LimitKind::Arithmetic, "turn counter overflow")
                })?;
            preview.update(|state| {
                state.turn_id = Some(turn);
                state.round_id = None;
                state.last_turn_id = turn;
            });
            preview.refresh_preview(Boundary::TurnStarted)?;
            if framework {
                preview_context.admit_framework(input.clone(), &preview.model_variables()?)?;
            } else {
                preview_context.admit_user(input.clone(), &preview.model_variables()?)?;
            }
            preview.update(|state| state.round_id = Some(1));
            preview.refresh_preview(Boundary::RoundStarted)?;
            preview_context.synchronize_state(&preview.model_variables()?)?;
            preview_context.content(preview.snapshot().limits.max_output_tokens)?;
        }
        self.initialize(&cancellation).await?;
        let continuation = self
            .continuation
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .filter(|value| {
                input.is_none()
                    || (!value.request_prepared
                        && !value.state_observation_pending
                        && !route_changed
                        && catalog.is_none())
            });
        let mut context = self
            .context
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let mut admission_transition = FaultOnDrop {
            faulted: &self.faulted,
            armed: false,
        };
        let mut communication_admission = None;
        let reply_store =
            !carrier_run.is_empty() && self.dialogue_reply_store.lock().await.is_some();
        if let Some(input) = input {
            if continuation.is_some() {
                return Err(Error::config(
                    "resume or recover before submitting a new turn",
                ));
            }
            if cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }
            // Epoch/catalog updates can discard the live Turn coordinates.
            // Secure an old dialogue before changing that baseline.
            if (catalog.is_some() || route_changed || context.pending_epoch())
                && let Err(error) = self
                    .settle_live_external_dialogues(DialogueTerminalReason::Interrupted)
                    .await
            {
                self.faulted.store(true, Ordering::Release);
                return Err(error);
            }
            if catalog.is_some() || route_changed || context.pending_epoch() {
                admission_transition.armed = true;
                *admitted = true;
            }
            if let Some((_, tools)) = &catalog {
                self.apply_external_tools(tools.clone()).await?;
                context = self
                    .context
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone();
            }
            if route_changed {
                context.prepare_protocol_epoch()?;
                self.route
                    .as_ref()
                    .expect("changed route has an admission owner")
                    .apply(context.next_model_epoch()?)?;
            }
            if context.pending_epoch() {
                // Validate before arming: rejection must leave the selected history intact.
                context.next_model_epoch()?;
                *admitted = true;
                let mut transition = FaultOnDrop {
                    faulted: &self.faulted,
                    armed: true,
                };
                self.boundary(Boundary::EpochEnded, None, &cancellation)
                    .await?;
                context.materialize_epoch()?;
                self.state.update(|state| {
                    state.epoch_id = context.epoch();
                    state.turn_id = None;
                    state.round_id = None;
                    state.last_turn_id = 0;
                });
                // Keep the owned Context consistent even if an acknowledged callback faults.
                *self.context.lock().unwrap_or_else(|e| e.into_inner()) = context.clone();
                self.boundary(Boundary::EpochStarted, None, &cancellation)
                    .await?;
                transition.armed = false;
            }
            // A route/environment epoch or recovery refresh needs its own entry
            // baseline. Resume never takes this path: existing semantic work and
            // invocation identity remain intact until the next new admission.
            let initial_required = *self
                .navigation_epoch
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                != Some(context.epoch());
            if initial_required {
                self.persist_initial(&context).await?;
            }
            // Any environment/route transition now has its acknowledged entry
            // baseline. New-input observation retries cannot fault that commit.
            admission_transition.armed = false;
            *admitted = false;
            let injection = crate::observation::hooks::intervene_input(
                self.auditor.as_ref(),
                &self.state,
                &input,
                &cancellation,
            )
            .await?;
            let mut changes = input.variables.clone();
            changes.overlay(&injection.variables);
            communication_admission = if carrier_run.is_empty() {
                None
            } else {
                Some(self.communication_transaction.lock().await)
            };
            let fence = self.endpoint.fence().await?;
            if cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let prepared = crate::admission::embedding::resolve(
                admission_resolver,
                crate::InputAdmissionBoundary::Turn,
                &input,
                &cancellation,
            )
            .await?;
            changes.overlay(&prepared.variables);
            let mut staged = None;
            for _ in 0..3 {
                let original = self.state.preview();
                if let Some(targeted) = user_target {
                    self.require_user_input_snapshot(&original.snapshot(), targeted)?;
                }
                let preview = original.preview();
                preview.apply_change(changes.clone())?;
                let turn_id = original
                    .snapshot()
                    .last_turn_id
                    .checked_add(1)
                    .ok_or_else(|| {
                        crate::budget::reject(crate::LimitKind::Arithmetic, "turn counter overflow")
                    })?;
                preview.update(|state| {
                    state.turn_id = Some(turn_id);
                    state.round_id = None;
                    state.last_turn_id = turn_id;
                    state.dialogue_turn = None;
                    state.dialogue_turn_live = false;
                });
                // TurnStarted owns reference refresh, but has no intervening hook.
                // Run its pure projection on the candidate; no State lock is held
                // across reference callbacks and failed admission commits nothing.
                preview.refresh(Boundary::TurnStarted)?;
                let values = match preview.model_projection_attempt() {
                    Ok(values) => values,
                    Err(crate::policy::PermissionError::StalePolicy) => continue,
                    Err(error) => return Err(crate::state::access::projection_error(error)),
                };
                let mut candidate = context.clone();
                if !carrier_run.is_empty() {
                    let carriers =
                        Self::stage_carrier_run(&preview, &carrier_run, turn_id, reply_store)?;
                    candidate.admit_carriers(carriers, &values)?;
                } else if framework {
                    candidate.admit_framework(input.clone(), &values)?;
                } else {
                    candidate.admit_user_with_important(
                        input.clone(),
                        &values,
                        prepared.important.clone(),
                    )?;
                }
                if prepared.important.is_some() || prepared.variables != Default::default() {
                    // The settled request includes the real round-start frame;
                    // count it together with the trusted overlay and escaped notice.
                    let mut request_preview = candidate.clone();
                    let round_preview = preview.preview();
                    round_preview.update(|state| state.round_id = Some(1));
                    round_preview.refresh_preview(Boundary::RoundStarted)?;
                    request_preview.synchronize_state(&round_preview.model_variables()?)?;
                    microcrab_transport::render_ast(
                        &request_preview
                            .content(round_preview.snapshot().limits.max_output_tokens)?,
                    )?;
                }
                if cancellation.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                // Only displace a resumable Turn after this input has a valid
                // candidate. Rebuild under the new authority after settlement.
                if self.state.live_dialogue_turn().is_some() {
                    if let Err(error) = self
                        .settle_live_external_dialogues(DialogueTerminalReason::Interrupted)
                        .await
                    {
                        self.faulted.store(true, Ordering::Release);
                        return Err(error);
                    }
                    continue;
                }
                if self.state.commit_input(&original, &preview) {
                    staged = Some(candidate);
                    break;
                }
            }
            let candidate = staged.ok_or(Error::StateObservationConflict)?;
            // From here the admitted State and input belong to one transition;
            // only failure of its durable acknowledgement faults the Session.
            admission_transition.armed = true;
            *admitted = true;
            self.continuation
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
            context = candidate;
            prepared.commit();
            drop(fence);
            if let Some(id) = queued {
                let mut inbox = self.inbox.state.lock().unwrap_or_else(|e| e.into_inner());
                if !carrier_run.is_empty() {
                    let notices = carrier_run
                        .iter()
                        .filter_map(|entry| match entry {
                            crate::admission::inbox::IdleCarrier::Framework(entry) => {
                                Some(entry.id)
                            }
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    inbox.acknowledge(&[], &notices);
                } else if framework {
                    inbox.acknowledge(&[], &[id]);
                } else {
                    inbox.acknowledge(&[id], &[]);
                }
            }
            if let Some(operation) = submission_operation_id.filter(|_| !framework) {
                let (epoch, items) = context.admitted_items();
                // The successful admit_user transaction appends exactly this final item.
                self.client
                    .observe_input_admitted(epoch, items, &[(items.len() - 1, operation)])
                    .await;
            }
        } else {
            if continuation.is_none() {
                return Err(Error::config("no suspended round continuation"));
            }
            *admitted = true;
        }
        if continuation.is_none() {
            // Pin route and admitted input durably before any provider effect.
            // No model request exists yet. Runtime owns the first prepared
            // continuation; a crash here must permit a fresh new Turn.
            if self.route.is_some() || !carrier_run.is_empty() {
                self.persist(&context, None).await?;
            }
            if let Some((generation, _)) = &catalog {
                self.applied_tools.store(*generation, Ordering::Release);
            }
        }
        if !carrier_run.is_empty() {
            let accepted = carrier_run
                .iter()
                .filter_map(|entry| match entry {
                    crate::admission::inbox::IdleCarrier::Accepted(entry) => {
                        Some(entry.operation_id.clone())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            self.acknowledge_carrier_run_locked(&accepted).await?;
        }
        if continuation.is_none() && !carrier_run.is_empty() {
            // Only the successfully acknowledged owning admission can mint live
            // reply authority; checkpoint recovery retains the evidence alone.
            self.state
                .update(|state| state.dialogue_turn_live = state.dialogue_turn.is_some());
        }
        drop(communication_admission);
        if continuation.is_none()
            && !carrier_run.is_empty()
            && let Err(error) = self.open_external_dialogues().await
        {
            // No acknowledged live reply authority after a failed Open. An
            // exact recovered admission can reconcile storage later.
            self.state.update(|state| state.dialogue_turn_live = false);
            return Err(error);
        }
        admission_transition.armed = false;
        if continuation
            .as_ref()
            .is_some_and(|value| value.request_prepared)
            && cancellation.is_cancelled()
        {
            *admitted = false;
            return Err(Error::Cancelled);
        }
        let _measurement = LiveMeasurementGuard(self);
        let outcome = self
            .runtime
            .execute_live(
                &self.state,
                &mut context,
                self.client.as_ref(),
                self.auditor.as_ref(),
                cancellation,
                self,
                continuation,
                events,
            )
            .await;
        for call_id in context.pending_call_ids() {
            context.append_tool_result(
                call_id,
                "{\"error\":\"interrupted; execution outcome may be unknown\"}".into(),
            )?;
        }
        *self.context.lock().unwrap_or_else(|e| e.into_inner()) = context;
        outcome
    }
}
