//! Optional nonblocking model previews. They carry real provider call identity and
//! never replace a validated Response, history commit, or execution authorization.
use crate::{Content, ModelEvent, Response};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Instant};

/// Transport-owned metadata only. Synthetic JSON fallback previews never enter
/// this clock; callers stamp decoded SSE content before any bounded delivery await.
#[derive(Default)]
pub(crate) struct StreamTiming {
    first_content: Option<Instant>,
    completed: Option<Instant>,
}
impl StreamTiming {
    pub(crate) fn delta(&mut self, event: &ModelEvent, now: Instant) {
        let has_content = match event {
            ModelEvent::TextDelta { text } | ModelEvent::ReasoningDelta { text } => {
                !text.is_empty()
            }
            ModelEvent::ToolCallDelta {
                id,
                name,
                arguments,
                ..
            } => {
                id.as_ref().is_some_and(|value| !value.is_empty())
                    || name.as_ref().is_some_and(|value| !value.is_empty())
                    || !arguments.is_empty()
            }
            ModelEvent::Finished { .. } => false,
        };
        if has_content {
            self.first_content.get_or_insert(now);
        }
    }
    pub(crate) fn complete(&mut self, now: Instant) {
        self.completed = Some(now);
    }
    pub(crate) fn measured(&self, started: Instant) -> (Option<u64>, Option<u64>) {
        let millis = |end: Instant, start: Instant| {
            end.checked_duration_since(start)
                .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        };
        let first = self.first_content.and_then(|first| millis(first, started));
        let generation = self
            .first_content
            .zip(self.completed)
            .and_then(|(first, end)| millis(end, first));
        (first, generation)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelObservation {
    pub call_id: String,
    pub epoch: u64,
    pub turn: Option<u64>,
    pub round: Option<u64>,
    pub subagent: Option<String>,
    #[serde(flatten)]
    pub state: ObservationState,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum ObservationState {
    Started,
    ReasoningDelta {
        text: String,
    },
    TextDelta {
        text: String,
    },
    ToolCallDelta {
        index: u32,
        id: Option<String>,
        name: Option<String>,
        arguments: String,
    },
    Validated {
        text: String,
    },
    Invalidated {
        reason: String,
    },
}
/// Must not block. Embedders use bounded queues and require resync on loss.
pub trait ModelObserver: Send + Sync + 'static {
    fn publish(&self, observation: ModelObservation);
}
/// Creator-owned durable display delivery; never a model-input or admission port.
/// Errors concern display durability only and must not become provider retries.
#[async_trait::async_trait]
pub trait ModelRecording: Send + Sync + 'static {
    async fn record(&self, observation: ModelObservation) -> Result<(), String>;
    /// Nonblocking terminal registration; accepted writes outlive provider futures.
    fn dropped(&self, observation: ModelObservation);
}
pub(crate) struct CallObservation {
    observer: Option<Arc<dyn ModelObserver>>,
    recording: Option<Arc<dyn ModelRecording>>,
    identity: ModelObservation,
    cancellation: tokio_util::sync::CancellationToken,
    terminal: Option<ObservationState>,
    finished: bool,
}
impl CallObservation {
    pub(crate) async fn start(
        observer: Option<Arc<dyn ModelObserver>>,
        recording: Option<Arc<dyn ModelRecording>>,
        content: &Content,
        call_id: String,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> Self {
        let observation = Self {
            observer,
            recording,
            cancellation: cancellation.clone(),
            identity: ModelObservation {
                call_id,
                epoch: content.epoch,
                turn: content.turn_id,
                round: content.round,
                subagent: content.subagent.clone(),
                state: ObservationState::Started,
            },
            terminal: None,
            finished: false,
        };
        observation.send(ObservationState::Started).await;
        observation
    }
    async fn send(&self, state: ObservationState) {
        let mut observation = self.identity.clone();
        observation.state = state;
        if let Some(recording) = &self.recording {
            // Creator reports archive failures separately; never alter a response.
            let _ = recording.record(observation.clone()).await;
        }
        self.publish(observation);
    }
    fn publish(&self, observation: ModelObservation) {
        if let Some(observer) = &self.observer {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                observer.publish(observation)
            }));
        }
    }
    pub(crate) async fn delta(&self, event: &ModelEvent) {
        match event {
            ModelEvent::TextDelta { text } => {
                self.send(ObservationState::TextDelta { text: text.clone() })
                    .await;
            }
            ModelEvent::ReasoningDelta { text } => {
                self.send(ObservationState::ReasoningDelta { text: text.clone() })
                    .await;
            }
            ModelEvent::ToolCallDelta {
                index,
                id,
                name,
                arguments,
            } => {
                self.send(ObservationState::ToolCallDelta {
                    index: *index,
                    id: id.clone(),
                    name: name.clone(),
                    arguments: arguments.clone(),
                })
                .await;
            }
            ModelEvent::Finished { .. } => {}
        }
    }
    pub(crate) fn finish<'a>(
        &'a mut self,
        result: &Result<Response, crate::Error>,
    ) -> impl std::future::Future<Output = ()> + 'a {
        let state = match result {
            Ok(response) => ObservationState::Validated {
                text: response.content.clone(),
            },
            Err(error) => ObservationState::Invalidated {
                reason: match error {
                    crate::Error::ProviderTimeout | crate::Error::ProviderIdleTimeout(_) => {
                        "timed_out"
                    }
                    crate::Error::Cancelled => "cancelled",
                    _ => "provider_call_failed",
                }
                .into(),
            },
        };
        // This assignment happens when finish is called, before its returned
        // future can lose a cancellation race or be dropped without a first poll.
        self.terminal = Some(state.clone());
        async move {
            // Normal completion still awaits the recorder's durable acknowledgement.
            // A stalled recorder cannot hold a provider future forever; Drop registers
            // this exact terminal in its durable attempt slot, preserving accepted writes.
            if tokio::time::timeout(std::time::Duration::from_millis(250), self.send(state))
                .await
                .is_ok()
            {
                self.finished = true;
            }
        }
    }
}
impl Drop for CallObservation {
    fn drop(&mut self) {
        if !self.finished {
            let mut observation = self.identity.clone();
            observation.state =
                self.terminal
                    .clone()
                    .unwrap_or_else(|| ObservationState::Invalidated {
                        // An outer owner may select cancellation before polling
                        // the adapter's own cancellation branch. Its supplied
                        // token still proves this call was cancelled.
                        reason: if self.cancellation.is_cancelled() {
                            "cancelled"
                        } else {
                            "provider_call_dropped"
                        }
                        .into(),
                    });
            if let Some(recording) = &self.recording {
                recording.dropped(observation.clone());
            }
            // send publishes only after the recorder acknowledges. If that wait
            // is dropped or times out, the advisory observer still needs this
            // exact terminal; durable fallback has no observer delivery port.
            self.publish(observation);
        }
    }
}

/// Preview delivery is transport-owned. Adapters must not copy this send path.
pub(crate) async fn emit(
    events: &Option<tokio::sync::mpsc::Sender<ModelEvent>>,
    observation: &CallObservation,
    event: ModelEvent,
) -> Result<(), crate::Error> {
    observation.delta(&event).await;
    if let Some(sender) = events {
        sender
            .send(event)
            .await
            .map_err(|_| crate::Error::Cancelled)?;
    }
    Ok(())
}

#[cfg(test)]
mod terminal_tests;

#[cfg(test)]
mod timing_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn only_actual_nonempty_content_starts_the_clock_and_eof_ends_generation() {
        let start = Instant::now();
        let mut timing = StreamTiming::default();
        timing.delta(
            &ModelEvent::TextDelta {
                text: String::new(),
            },
            start + Duration::from_millis(2),
        );
        timing.delta(
            &ModelEvent::Finished {
                usage: Default::default(),
            },
            start + Duration::from_millis(3),
        );
        assert_eq!(timing.measured(start), (None, None));
        timing.delta(
            &ModelEvent::ReasoningDelta {
                text: "thinking".into(),
            },
            start + Duration::from_millis(40),
        );
        timing.delta(
            &ModelEvent::TextDelta {
                text: "answer".into(),
            },
            start + Duration::from_millis(90),
        );
        assert_eq!(timing.measured(start), (Some(40), None));
        timing.complete(start + Duration::from_millis(140));
        assert_eq!(timing.measured(start), (Some(40), Some(100)));
    }

    #[test]
    fn tool_only_stream_is_content_but_empty_and_cancelled_streams_stay_partial() {
        let start = Instant::now();
        let mut empty = StreamTiming::default();
        empty.complete(start + Duration::from_millis(10));
        assert_eq!(empty.measured(start), (None, None));
        let mut tool = StreamTiming::default();
        tool.delta(
            &ModelEvent::ToolCallDelta {
                index: 0,
                id: None,
                name: Some("search".into()),
                arguments: String::new(),
            },
            start + Duration::from_millis(5),
        );
        assert_eq!(tool.measured(start), (Some(5), None));
        tool.complete(start + Duration::from_millis(5));
        assert_eq!(tool.measured(start), (Some(5), Some(0)));
    }
}
#[cfg(test)]
mod recording_tests {
    use super::*;
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    };

    struct Recording {
        committed: Arc<AtomicBool>,
        dropped: Arc<Mutex<Vec<String>>>,
    }
    #[async_trait::async_trait]
    impl ModelRecording for Recording {
        async fn record(&self, _: ModelObservation) -> Result<(), String> {
            tokio::task::yield_now().await;
            self.committed.store(true, Ordering::SeqCst);
            Ok(())
        }
        fn dropped(&self, observation: ModelObservation) {
            self.dropped.lock().unwrap().push(observation.call_id);
        }
    }
    struct Observer {
        committed: Arc<AtomicBool>,
        received: Arc<Mutex<Vec<String>>>,
    }
    impl ModelObserver for Observer {
        fn publish(&self, observation: ModelObservation) {
            assert!(self.committed.load(Ordering::SeqCst));
            if let ObservationState::ReasoningDelta { text } = observation.state {
                self.received.lock().unwrap().push(text);
            }
        }
    }
    #[tokio::test]
    async fn observer_receives_exact_body_only_after_async_archive_acknowledgement() {
        let committed = Arc::new(AtomicBool::new(false));
        let received = Arc::new(Mutex::new(Vec::new()));
        let dropped = Arc::new(Mutex::new(Vec::new()));
        let call = CallObservation {
            cancellation: tokio_util::sync::CancellationToken::new(),
            observer: Some(Arc::new(Observer {
                committed: committed.clone(),
                received: received.clone(),
            })),
            recording: Some(Arc::new(Recording {
                committed,
                dropped: dropped.clone(),
            })),
            identity: ModelObservation {
                call_id: "independent-call".into(),
                epoch: 0,
                turn: Some(1),
                round: Some(2),
                subagent: None,
                state: ObservationState::Started,
            },
            terminal: None,
            finished: false,
        };
        let text = "unicode 思考\n".repeat(50_000);
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        emit(
            &Some(sender),
            &call,
            ModelEvent::ReasoningDelta { text: text.clone() },
        )
        .await
        .unwrap();
        assert!(
            matches!(receiver.recv().await, Some(ModelEvent::ReasoningDelta { text: body }) if body == text)
        );
        assert_eq!(*received.lock().unwrap(), vec![text]);
        drop(call);
        assert_eq!(*dropped.lock().unwrap(), vec!["independent-call"]);
    }
}
