//! Current Activity and bounded, loss-detecting completed-turn delivery.
//! Consumers must treat broadcast Lagged as an error, never skip replies.
use super::{FailureFacts, Observation, ObservationKind, Position};
use crate::{Error, runtime::TurnOutcome};
use std::sync::Mutex;
use tokio::sync::{broadcast, watch};

#[derive(Clone, Debug, Default)]
pub struct SessionActivity {
    pub active: bool,
    pub paused: bool,
    pub inbox_ready: bool,
    pub retired: bool,
    pub outcome_sequence: u64,
    pub latest_outcome: Option<TurnOutcome>,
    pub latest_inbox_failure: Option<InboxFailure>,
    /// Latest autonomous cap stop; later successful turns do not erase this fact.
    pub latest_round_limit_stop: Option<RoundLimitStop>,
}
/// A locator for an already published outcome, without duplicating its payload.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RoundLimitStop {
    pub sequence: u64,
    pub epoch: u64,
    pub turn: u64,
    pub rounds: usize,
}
#[derive(Clone, Debug)]
pub struct InboxFailure {
    pub sequence: u64,
    pub epoch: u64,
    pub turn: Option<u64>,
    pub round: Option<u64>,
    pub kind: InboxFailureKind,
    pub retry_ready: bool,
}
#[derive(Clone, Debug)]
pub enum InboxFailureKind {
    ProviderStatus {
        status: u16,
    },
    Transport,
    TransportFailure(String),
    ProviderTimeout,
    ProviderIdleTimeout(crate::ProviderTimeoutDetails),
    /// Only producer-marked safe reasons cross this observation boundary.
    Provider {
        protocol_reason: Option<String>,
        reason: String,
        failure: Option<microcrab_transport::ProviderFailure>,
    },
    Protocol(Option<String>),
    Limit(crate::LimitError),
    Cancelled {
        reason: Option<crate::InterruptReason>,
    },
    Config(Option<String>),
    Denied(Option<String>),
    Tool(Option<String>),
    Lifecycle(Option<String>),
    Audit(Option<String>),
    Wasm(Option<String>),
    Reference(Option<String>),
    Diagnostic(super::diagnostics::ToolDiagnostic),
    Json(super::diagnostics::ToolDiagnostic),
    Io {
        kind: std::io::ErrorKind,
        raw_os_error: Option<i32>,
        message: String,
    },
    ToolFailure {
        kind: crate::runtime::execution::ToolFailureKind,
        retryable: bool,
        code: String,
        message: String,
    },
    PendingInput(crate::PendingInputError),
    Busy,
    Closed,
    ExecutionInterrupted,
    Other,
}
#[derive(Clone, Debug)]
pub struct CompletedTurn {
    pub sequence: u64,
    pub epoch: u64,
    pub turn: u64,
    pub autonomous: bool,
    pub outcome: TurnOutcome,
}

/// Owner supplies this after acquiring the publication fence, releases its inbox
/// lock before returning, and never transfers inbox authority to observation.
pub(crate) struct ActivitySnapshot {
    pub position: Position,
    pub active: bool,
    pub paused: bool,
    pub ready: bool,
    pub retired: bool,
}
pub(crate) enum ActivityProjection<'a> {
    Snapshot(ActivitySnapshot),
    Completed {
        epoch: u64,
        turn: u64,
        outcome: &'a TurnOutcome,
        autonomous: bool,
    },
    InboxFailure {
        position: Position,
        error: &'a Error,
        interrupt_reason: Option<crate::InterruptReason>,
        retry_ready: bool,
    },
}
impl ActivityProjection<'_> {
    fn observation(&self) -> Observation {
        match self {
            Self::Snapshot(snapshot) => Observation {
                kind: ObservationKind::Activity,
                position: snapshot.position,
                failure: None,
            },
            Self::Completed { epoch, turn, .. } => Observation {
                kind: ObservationKind::TurnCompleted,
                position: Position {
                    epoch: *epoch,
                    turn: Some(*turn),
                    round: None,
                },
                failure: None,
            },
            Self::InboxFailure {
                position, error, ..
            } => Observation {
                kind: ObservationKind::InboxFailure,
                position: *position,
                failure: Some(FailureFacts::from_error(error).class),
            },
        }
    }
}

/// One publication owner: fence -> owner snapshot -> release owner locks -> watch
/// -> bounded reply broadcast. It retains no owner handle or request bodies.
pub(crate) struct ActivityPublisher {
    current: watch::Sender<SessionActivity>,
    publication: Mutex<()>,
    completed: broadcast::Sender<CompletedTurn>,
}
impl Default for ActivityPublisher {
    fn default() -> Self {
        Self {
            current: watch::channel(SessionActivity::default()).0,
            publication: Mutex::new(()),
            completed: broadcast::channel(128).0,
        }
    }
}
impl ActivityPublisher {
    pub(crate) fn subscribe_activity(&self) -> watch::Receiver<SessionActivity> {
        self.current.subscribe()
    }
    pub(crate) fn subscribe_turns(&self) -> broadcast::Receiver<CompletedTurn> {
        self.completed.subscribe()
    }
    /// Snapshot closure executes under the ordering fence. It must release all
    /// owner guards before returning: watch readers may inspect those owners.
    pub(crate) fn publish<'a>(
        &self,
        snapshot: impl FnOnce() -> ActivityProjection<'a>,
    ) -> Result<(), Error> {
        let _publication = self.publication.lock().unwrap_or_else(|e| e.into_inner());
        let projection = snapshot();
        let observation = projection.observation();
        match projection {
            ActivityProjection::Snapshot(snapshot) => self.current.send_modify(|activity| {
                activity.active = snapshot.active;
                activity.paused = snapshot.paused;
                activity.inbox_ready = snapshot.ready;
                activity.retired = snapshot.retired;
            }),
            ActivityProjection::Completed {
                epoch,
                turn,
                outcome,
                autonomous,
            } => {
                // Check before mutation. Watch and bounded replies use the same
                // sequence, and the fence prevents a later reply overtaking it.
                let mut sequence = None;
                self.current.send_modify(|activity| {
                    sequence = activity.outcome_sequence.checked_add(1);
                    if let Some(sequence) = sequence {
                        activity.outcome_sequence = sequence;
                        activity.latest_outcome = Some(outcome.clone());
                        if autonomous
                            && outcome.stop_reason == crate::runtime::TurnStopReason::RoundLimit
                        {
                            activity.latest_round_limit_stop = Some(RoundLimitStop {
                                sequence,
                                epoch,
                                turn,
                                rounds: outcome.rounds,
                            });
                        }
                    }
                });
                let sequence = sequence.ok_or_else(|| {
                    crate::budget::reject(
                        crate::LimitKind::Arithmetic,
                        "turn completion sequence overflow",
                    )
                })?;
                let _ = self.completed.send(CompletedTurn {
                    sequence,
                    epoch,
                    turn,
                    autonomous,
                    outcome: outcome.clone(),
                });
            }
            ActivityProjection::InboxFailure {
                position,
                error,
                interrupt_reason,
                retry_ready,
            } => {
                let kind = match inbox_failure(error) {
                    InboxFailureKind::Cancelled { .. } => InboxFailureKind::Cancelled {
                        reason: interrupt_reason,
                    },
                    other => other,
                };
                self.current.send_modify(|activity| {
                    let sequence = activity
                        .latest_inbox_failure
                        .as_ref()
                        .map_or(1, |failure| failure.sequence.saturating_add(1));
                    activity.latest_inbox_failure = Some(InboxFailure {
                        sequence,
                        epoch: position.epoch,
                        turn: position.turn,
                        round: position.round,
                        kind,
                        retry_ready,
                    });
                });
            }
        }
        drop(_publication);
        observation.trace();
        Ok(())
    }
}

fn inbox_failure(error: &Error) -> InboxFailureKind {
    let facts = FailureFacts::from_error(error);
    // Queue publication retains closed owner facts, never native/guest bodies.
    // Match every Error variant so new categories cannot silently become Other.
    match error {
        Error::ProviderStatus { .. } => InboxFailureKind::ProviderStatus {
            status: facts.provider_status.expect("typed provider status"),
        },
        Error::Transport => InboxFailureKind::Transport,
        Error::TransportFailure(cause) => InboxFailureKind::TransportFailure(
            cause
                .public_reason()
                .unwrap_or("network failure")
                .to_owned(),
        ),
        Error::ProviderTimeout => InboxFailureKind::ProviderTimeout,
        Error::ProviderIdleTimeout(_) => InboxFailureKind::ProviderIdleTimeout(
            facts.idle_timeout.expect("typed idle timeout").clone(),
        ),
        Error::Provider(cause) => InboxFailureKind::Provider {
            protocol_reason: cause.public_protocol_reason().map(str::to_owned),
            reason: cause
                .public_reason()
                .unwrap_or("provider failure")
                .to_owned(),
            failure: cause.failure_details().cloned(),
        },
        Error::Protocol(cause) => {
            InboxFailureKind::Protocol(cause.public_reason().map(str::to_owned))
        }
        Error::Limit(_) => InboxFailureKind::Limit(facts.sanitized_limit().expect("typed limit")),
        Error::Cancelled => InboxFailureKind::Cancelled { reason: None },
        Error::Config(cause) => InboxFailureKind::Config(cause.public_reason().map(str::to_owned)),
        Error::Denied(cause) => InboxFailureKind::Denied(cause.public_reason().map(str::to_owned)),
        Error::Tool(cause) => InboxFailureKind::Tool(cause.public_reason().map(str::to_owned)),
        Error::Lifecycle(cause) => {
            InboxFailureKind::Lifecycle(cause.public_reason().map(str::to_owned))
        }
        Error::Audit(cause) => InboxFailureKind::Audit(cause.public_reason().map(str::to_owned)),
        Error::Wasm(cause) => InboxFailureKind::Wasm(cause.public_reason().map(str::to_owned)),
        Error::Reference(cause) => {
            InboxFailureKind::Reference(cause.public_reason().map(str::to_owned))
        }
        Error::Diagnostic(diagnostic) => InboxFailureKind::Diagnostic(diagnostic.clone()),
        Error::Json(_) => InboxFailureKind::Json(error.tool_diagnostic()),
        Error::Io(source) => InboxFailureKind::Io {
            kind: source.kind(),
            raw_os_error: source.raw_os_error(),
            message: microcrab_protocol::diagnostic::sanitize(&source.to_string(), 2048),
        },
        Error::ToolFailure(failure) => InboxFailureKind::ToolFailure {
            kind: failure.kind,
            retryable: failure.retryable,
            code: microcrab_protocol::diagnostic::sanitize(&failure.code, 128),
            message: microcrab_protocol::diagnostic::sanitize(&failure.message, 4096),
        },
        Error::PendingInput(cause) => InboxFailureKind::PendingInput(*cause),
        Error::StateObservationConflict | Error::VariableConflict | Error::VariableForbidden(_) => {
            InboxFailureKind::Diagnostic(error.tool_diagnostic())
        }
        Error::Busy => InboxFailureKind::Busy,
        Error::Closed => InboxFailureKind::Closed,
        Error::ExecutionInterrupted => InboxFailureKind::ExecutionInterrupted,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inbox_projection_retains_safe_numeric_facts_without_error_labels() {
        let publisher = ActivityPublisher::default();
        let receiver = publisher.subscribe_activity();
        let error = crate::budget::reject(
            crate::LimitKind::ToolArguments,
            "token=secret /private/path",
        );
        publisher
            .publish(|| ActivityProjection::InboxFailure {
                position: Position {
                    epoch: 7,
                    turn: Some(3),
                    round: Some(2),
                },
                error: &error,
                interrupt_reason: None,
                retry_ready: true,
            })
            .unwrap();
        let activity = receiver.borrow();
        let failure = activity.latest_inbox_failure.as_ref().unwrap();
        assert_eq!(
            (failure.sequence, failure.epoch, failure.turn, failure.round),
            (1, 7, Some(3), Some(2))
        );
        let InboxFailureKind::Limit(limit) = &failure.kind else {
            panic!("typed limit required")
        };
        assert_eq!(limit.kind, crate::LimitKind::ToolArguments);
        assert!(!limit.to_string().contains("secret"));
        assert!(failure.retry_ready);
    }

    #[test]
    fn inbox_projection_preserves_known_protocol_causes_without_private_context() {
        let reason = "invalid tool arguments JSON";
        let private = "response_body=private-call-id";
        let failures = [
            Error::protocol_with_private_reason(reason, private.into()),
            Error::Provider(microcrab_transport::ProviderError::protocol(
                microcrab_transport::ProtocolError::with_private_reason(reason, private.into()),
            )),
            Error::Protocol(private.into()),
            Error::Provider(private.into()),
        ];
        for error in failures {
            let kind = inbox_failure(&error);
            match &kind {
                InboxFailureKind::Protocol(value) => {
                    assert_eq!(value.as_deref(), error.public_protocol_reason())
                }
                InboxFailureKind::Provider {
                    protocol_reason,
                    failure,
                    ..
                } => {
                    assert_eq!(protocol_reason.as_deref(), error.public_protocol_reason());
                    assert!(failure.is_none());
                }
                other => panic!("unexpected category: {other:?}"),
            }
            assert!(!format!("{kind:?}").contains(private));
        }
    }

    #[test]
    fn inbox_publication_preserves_each_marked_framework_category_without_private_context() {
        let private = "private-queue-body-secret";
        let errors = [
            Error::config_with_private("configuration root is empty", private),
            Error::denied_with_private("process belongs to another Session", private),
            Error::tool_with_private("unknown builtin tool", private),
            Error::lifecycle_with_private("initialization generation changed", private),
            Error::audit_with_private("audit publication failed", private),
            Error::wasm_with_private("unsupported component import", private),
            Error::reference_with_private("reference URL is not same-origin", private),
        ];
        let publisher = ActivityPublisher::default();
        let receiver = publisher.subscribe_activity();
        for error in errors {
            publisher
                .publish(|| ActivityProjection::InboxFailure {
                    position: Position::default(),
                    error: &error,
                    interrupt_reason: None,
                    retry_ready: false,
                })
                .unwrap();
            let activity = receiver.borrow();
            let kind = &activity.latest_inbox_failure.as_ref().unwrap().kind;
            let retained = match (kind, &error) {
                (InboxFailureKind::Config(reason), Error::Config(_))
                | (InboxFailureKind::Denied(reason), Error::Denied(_))
                | (InboxFailureKind::Tool(reason), Error::Tool(_))
                | (InboxFailureKind::Lifecycle(reason), Error::Lifecycle(_))
                | (InboxFailureKind::Audit(reason), Error::Audit(_))
                | (InboxFailureKind::Wasm(reason), Error::Wasm(_))
                | (InboxFailureKind::Reference(reason), Error::Reference(_)) => reason.as_deref(),
                other => panic!("category changed across queue: {other:?}"),
            };
            assert_eq!(retained, error.public_reason());
            assert!(!format!("{kind:?}").contains(private));
        }
        assert!(matches!(
            inbox_failure(&Error::Tool(private.into())),
            InboxFailureKind::Tool(Some(_))
        ));
    }

    #[test]
    fn inbox_projection_keeps_structured_json_io_and_tool_failure_facts_without_bodies() {
        use crate::observation::diagnostics::{
            DiagnosticCode, DiagnosticLayer, DiagnosticPhase, SafeDiagnosticReason, ToolDiagnostic,
        };
        let mut diagnostic = ToolDiagnostic::new(
            DiagnosticCode::PathOutsideRoot,
            DiagnosticLayer::Host,
            DiagnosticPhase::Validation,
        );
        diagnostic.reason = Some(SafeDiagnosticReason::new(
            "shell working directory is outside the configured root",
        ));
        diagnostic.effects = super::super::diagnostics::EffectKnowledge::Committed;
        match inbox_failure(&Error::Diagnostic(diagnostic.clone())) {
            InboxFailureKind::Diagnostic(retained) => assert_eq!(retained, diagnostic),
            other => panic!("structured diagnostic collapsed: {other:?}"),
        }
        let source = serde_json::from_str::<u64>("\"private-json-secret\"").unwrap_err();
        let (line, column) = (source.line() as u64, source.column() as u64);
        let kind = inbox_failure(&Error::Json(source));
        match &kind {
            InboxFailureKind::Json(retained) => {
                assert_eq!(retained.json_line, Some(line));
                assert_eq!(retained.json_column, Some(column));
                assert!(retained.summary().contains("invalid type"));
            }
            other => panic!("JSON category collapsed: {other:?}"),
        }
        assert!(!format!("{kind:?}").contains("private-json-secret"));
        let source = std::io::Error::from_raw_os_error(5);
        let expected = (source.kind(), source.raw_os_error());
        match inbox_failure(&Error::Io(source)) {
            InboxFailureKind::Io {
                kind, raw_os_error, ..
            } => {
                assert_eq!((kind, raw_os_error), expected)
            }
            other => panic!("I/O category collapsed: {other:?}"),
        }
        let mut failure = crate::runtime::execution::ToolFailure::failed(
            "token=private-failure-code",
            "response_body=private-failure-body",
        );
        failure.kind = crate::runtime::execution::ToolFailureKind::Timeout;
        failure.retryable = true;
        let kind = inbox_failure(&Error::ToolFailure(failure));
        assert!(matches!(
            kind,
            InboxFailureKind::ToolFailure {
                kind: crate::runtime::execution::ToolFailureKind::Timeout,
                retryable: true,
                ..
            }
        ));
        assert!(!format!("{kind:?}").contains("private-failure"));
        for (error, expected) in [
            (Error::Busy, "busy"),
            (Error::Closed, "closed"),
            (Error::ExecutionInterrupted, "interrupted"),
            (
                Error::PendingInput(crate::PendingInputError::RevisionConflict),
                "pending",
            ),
        ] {
            let actual = match inbox_failure(&error) {
                InboxFailureKind::Busy => "busy",
                InboxFailureKind::Closed => "closed",
                InboxFailureKind::ExecutionInterrupted => "interrupted",
                InboxFailureKind::PendingInput(crate::PendingInputError::RevisionConflict) => {
                    "pending"
                }
                other => panic!("closed state collapsed: {other:?}"),
            };
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn completed_replies_share_watch_sequence_and_round_limit_locator() {
        let publisher = ActivityPublisher::default();
        let current = publisher.subscribe_activity();
        let mut replies = publisher.subscribe_turns();
        let mut outcome = TurnOutcome {
            content: "private model body".into(),
            rounds: 64,
            usage: Default::default(),
            stop_reason: crate::runtime::TurnStopReason::RoundLimit,
        };
        publisher
            .publish(|| ActivityProjection::Completed {
                epoch: 4,
                turn: 9,
                outcome: &outcome,
                autonomous: true,
            })
            .unwrap();
        let reply = replies.try_recv().unwrap();
        assert_eq!(reply.sequence, current.borrow().outcome_sequence);
        assert_eq!(reply.outcome, outcome);
        assert_eq!(
            current
                .borrow()
                .latest_round_limit_stop
                .as_ref()
                .unwrap()
                .sequence,
            reply.sequence
        );
        outcome.stop_reason = crate::runtime::TurnStopReason::ModelCompleted;
        publisher
            .publish(|| ActivityProjection::Completed {
                epoch: 4,
                turn: 10,
                outcome: &outcome,
                autonomous: true,
            })
            .unwrap();
        assert_eq!(replies.try_recv().unwrap().sequence, 2);
        assert_eq!(
            current
                .borrow()
                .latest_round_limit_stop
                .as_ref()
                .unwrap()
                .turn,
            9
        );
        publisher
            .current
            .send_modify(|activity| activity.outcome_sequence = u64::MAX);
        let error = publisher
            .publish(|| ActivityProjection::Completed {
                epoch: 4,
                turn: 11,
                outcome: &outcome,
                autonomous: true,
            })
            .unwrap_err();
        let Error::Limit(limit) = error else {
            panic!("typed overflow required")
        };
        assert_eq!(limit.kind, crate::LimitKind::Arithmetic);
        assert_eq!(current.borrow().outcome_sequence, u64::MAX);
        assert!(matches!(
            replies.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn cancelled_inbox_projection_keeps_cancellation_distinct() {
        let facts = FailureFacts::from_error(&Error::Cancelled);
        assert_eq!(facts.class, super::super::FailureClass::Cancelled);
        assert!(matches!(
            inbox_failure(&Error::Cancelled),
            InboxFailureKind::Cancelled { reason: None }
        ));
    }
}
