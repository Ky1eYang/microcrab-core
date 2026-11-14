//! Session adapter: hooks return one atomic variable mutation, never direct writes.
#[cfg(test)]
mod tests;
use super::{FailureClass, FailureFacts, Observation, ObservationKind, Position};
use crate::boundary::Boundary;
use crate::{Error, State};
use microcrab_auditor::{
    AuditBody, Auditor, AuditorError, BoxFuture, HookContext, HookPayload, Injection, Moment,
};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Borrow the envelope only. Audit payloads remain exclusively in the embedder's
/// existing auditor port, and are never copied into tracing or Activity.
fn audit_observation(body: &AuditBody) -> Observation {
    use microcrab_auditor::AuditOutcome;
    let failure = match body.outcome {
        Some(AuditOutcome::Unavailable) => Some(FailureClass::Provider),
        Some(AuditOutcome::InvalidResponse) => Some(FailureClass::Protocol),
        Some(AuditOutcome::LimitExceeded) => Some(FailureClass::Limit),
        Some(AuditOutcome::Cancelled) => Some(FailureClass::Cancelled),
        Some(AuditOutcome::TimedOut) => Some(FailureClass::Timeout),
        _ => body
            .response_status
            .filter(|status| *status >= 400)
            .map(|_| FailureClass::ProviderStatus),
    };
    Observation {
        kind: ObservationKind::Audit,
        position: Position {
            epoch: body.epoch,
            turn: body.turn_id,
            round: body.round,
        },
        failure,
    }
}

// The owned handle holds only State and the embedder auditor, never the Session owner.
pub(crate) struct BoundAuditor {
    pub inner: Arc<dyn Auditor>,
    pub state: State,
    pub completed: std::sync::Mutex<Option<(Option<String>, Option<u16>)>>,
}

impl Auditor for BoundAuditor {
    fn is_enabled(&self) -> bool {
        true
    }
    fn publish(&self, moment: Moment, body: AuditBody) {
        let observation = audit_observation(&body);
        observation.trace();
        if moment == Moment::RoundCompleted {
            *self
                .completed
                .lock()
                .unwrap_or_else(|error| error.into_inner()) =
                Some((body.call_id.clone(), body.response_status));
        }
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if self.inner.is_enabled() {
                self.inner.publish(moment, body);
            }
        }))
        .is_err()
        {
            tracing::warn!(observation = observation.kind.as_str(),
                epoch = observation.position.epoch, turn = ?observation.position.turn,
                round = ?observation.position.round,
                "audit observer panicked; observation dropped");
        }
    }
    fn intervene<'a>(
        &'a self,
        moment: Moment,
        mut context: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
        Box::pin(async move {
            if moment == Moment::ToolResult {
                return Err(AuditorError(
                    "ToolResult is observation-only; use publish".into(),
                ));
            }
            if matches!(
                moment,
                Moment::RequestPrepared
                    | Moment::SessionCreated
                    | Moment::SessionRestored
                    | Moment::InputAdmitted
            ) {
                self.completed
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .take();
            }
            if moment == Moment::RoundCompleted {
                let completed = self
                    .completed
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .take();
                if let Some((call_id, status)) = completed {
                    context.call_id = call_id;
                    if let HookPayload::RoundCompleted {
                        status: observed, ..
                    } = &mut context.payload
                    {
                        *observed = status;
                    }
                } else {
                    let usage = match &context.payload {
                        HookPayload::RoundCompleted { usage, .. } => usage.clone(),
                        _ => None,
                    };
                    self.publish(
                        moment,
                        AuditBody {
                            epoch: context.epoch,
                            subagent: context.subagent.clone(),
                            turn_id: context.turn,
                            round: context.round,
                            usage,
                            payload: context.payload.clone(),
                            outcome: Some(microcrab_auditor::AuditOutcome::Accepted),
                            ..Default::default()
                        },
                    );
                    self.completed
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .take();
                }
            }
            let authored = context.request_body;
            let request_budget = context
                .max_request_bytes
                .unwrap_or(self.state.snapshot().limits.max_context_bytes)
                .min(microcrab_transport::MAX_REQUEST_REWRITE_BYTES);
            let timeout = self.state.snapshot().limits.lifecycle_timeout_ms;
            let mut injection = tokio::time::timeout(
                Duration::from_millis(timeout),
                self.inner.intervene(moment, context),
            )
            .await
            .map_err(|_| AuditorError("hook completion deadline".into()))??;
            if moment == Moment::RequestPrepared {
                if let Some(replacement) = &injection.request_body {
                    microcrab_transport::validate_request_rewrite(
                        authored.ok_or_else(|| AuditorError("missing authored request".into()))?,
                        replacement,
                        request_budget.min(self.state.snapshot().limits.max_context_bytes),
                    )
                    .map_err(|error| AuditorError(error.to_string()))?;
                }
            } else {
                injection.request_body = None;
            }
            if let Some(arguments) = &injection.tool_arguments {
                if moment != Moment::ToolCall {
                    return Err(AuditorError(
                        "tool argument replacement requires ToolCall".into(),
                    ));
                }
                if !arguments.is_object() {
                    return Err(AuditorError(
                        "replacement tool arguments must be an object".into(),
                    ));
                }
            }
            // Input changes belong to its candidate transaction, never the precommit hook.
            if moment != Moment::InputAdmitted {
                self.state
                    .apply_change(injection.variables.clone())
                    .map_err(|error| AuditorError(error.to_string()))?;
            }
            Ok(injection)
        })
    }
    fn flush<'a>(&'a self) -> BoxFuture<'a, Result<(), AuditorError>> {
        Box::pin(async move {
            tokio::time::timeout(
                Duration::from_millis(self.state.snapshot().limits.lifecycle_timeout_ms),
                self.inner.flush(),
            )
            .await
            .map_err(|_| AuditorError("audit flush deadline".into()))?
        })
    }
}

pub(crate) async fn intervene(
    auditor: &dyn Auditor,
    state: &State,
    moment: Moment,
    payload: HookPayload,
    cancellation: &CancellationToken,
) -> Result<Injection, Error> {
    intervene_recorded(
        auditor,
        state,
        moment,
        payload,
        cancellation,
        AuditBody::default(),
    )
    .await
}

/// Preserve lifecycle semantics while carrying producer-owned correlation and clocks.
pub(crate) async fn intervene_recorded(
    auditor: &dyn Auditor,
    state: &State,
    moment: Moment,
    payload: HookPayload,
    cancellation: &CancellationToken,
    publication: AuditBody,
) -> Result<Injection, Error> {
    intervene_view(
        auditor,
        state,
        state,
        moment,
        payload,
        cancellation,
        publication,
    )
    .await
}

/// Attached input is available to the trusted hook without entering audit payloads.
pub(crate) async fn intervene_input(
    auditor: &dyn Auditor,
    state: &State,
    input: &crate::SessionInput,
    cancellation: &CancellationToken,
) -> Result<Injection, Error> {
    let preview = state.preview();
    preview.apply_change(input.variables.clone())?;
    struct InputView<'a> {
        state: State,
        input: &'a crate::SessionInput,
    }
    impl microcrab_auditor::VariableView for InputView<'_> {
        fn get(&self, name: &str) -> Option<serde_json::Value> {
            self.state.view().ok()?.get(name).cloned()
        }
        fn get_all(&self, name: &str) -> Option<serde_json::Value> {
            self.state.view_all().ok()?.get(name).cloned()
        }
        fn input(&self) -> Option<&crate::SessionInput> {
            Some(self.input)
        }
    }
    let view = InputView {
        state: preview,
        input,
    };
    intervene_view(
        auditor,
        state,
        &view,
        Moment::InputAdmitted,
        HookPayload::InputAdmitted {
            parts: input.parts.clone(),
        },
        cancellation,
        AuditBody::default(),
    )
    .await
}

async fn intervene_view(
    auditor: &dyn Auditor,
    state: &State,
    variables: &dyn microcrab_auditor::VariableView,
    moment: Moment,
    payload: HookPayload,
    cancellation: &CancellationToken,
    mut publication: AuditBody,
) -> Result<Injection, Error> {
    let snapshot = state.snapshot();
    let context = HookContext {
        epoch: snapshot.epoch_id,
        subagent: None,
        turn: snapshot.turn_id,
        round: snapshot.round_id,
        call_id: None,
        request_body: None,
        max_request_bytes: None,
        variables,
        payload: payload.clone(),
    };
    let result = tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(Error::Cancelled),
        result = auditor.intervene(moment, context) => result.map_err(|error| Error::Lifecycle(format!("auditor intervention failed: {}",microcrab_protocol::diagnostic::describe(&error,1800)).into())),
    };
    let injection = result.inspect_err(|error| {
        Observation {
            kind: ObservationKind::Audit,
            position: Position {
                epoch: snapshot.epoch_id,
                turn: snapshot.turn_id,
                round: snapshot.round_id,
            },
            failure: Some(FailureFacts::from_error(error).class),
        }
        .trace();
    })?;
    if moment != Moment::RoundCompleted {
        publication.epoch = snapshot.epoch_id;
        publication.turn_id = snapshot.turn_id;
        publication.round = snapshot.round_id;
        publication.payload = payload;
        auditor.publish(moment, publication);
    }
    Ok(injection)
}

pub(crate) async fn boundary(
    auditor: &dyn Auditor,
    state: &State,
    boundary: Boundary,
    cancellation: &CancellationToken,
) -> Result<(), Error> {
    state.refresh(boundary)?;
    let moment = match boundary {
        Boundary::SessionCreated => Some(Moment::SessionCreated),
        Boundary::SessionRestored => Some(Moment::SessionRestored),
        Boundary::SessionTerminated => Some(Moment::SessionTerminated),
        _ => None,
    };
    if let Some(moment) = moment {
        intervene(auditor, state, moment, HookPayload::None, cancellation).await?;
    }
    Ok(())
}
