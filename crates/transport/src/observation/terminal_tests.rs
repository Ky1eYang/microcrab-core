use super::*;
use std::sync::Mutex;

fn identity(state: ObservationState) -> ModelObservation {
    ModelObservation {
        call_id: "exact-call".into(),
        epoch: 1,
        turn: Some(1),
        round: Some(1),
        subagent: None,
        state,
    }
}
fn call(recording: Arc<dyn ModelRecording>) -> CallObservation {
    CallObservation {
        observer: None,
        recording: Some(recording),
        identity: identity(ObservationState::Started),
        cancellation: tokio_util::sync::CancellationToken::new(),
        terminal: None,
        finished: false,
    }
}
#[derive(Default)]
struct Blocked {
    accepted: Mutex<Vec<ModelObservation>>,
    fallback: Mutex<Vec<ModelObservation>>,
    acknowledged: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl ModelRecording for Blocked {
    async fn record(&self, observation: ModelObservation) -> Result<(), String> {
        self.accepted.lock().unwrap().push(observation);
        self.acknowledged.notified().await;
        Ok(())
    }
    fn dropped(&self, observation: ModelObservation) {
        self.fallback.lock().unwrap().push(observation);
    }
}
#[tokio::test]
async fn never_polled_finish_preserves_intended_cancelled_terminal() {
    let recording = Arc::new(Blocked::default());
    let mut observation = call(recording.clone());
    let result = Err(crate::Error::Cancelled);
    drop(observation.finish(&result));
    drop(observation);
    assert!(recording.accepted.lock().unwrap().is_empty());
    assert!(matches!(&recording.fallback.lock().unwrap()[0].state,
        ObservationState::Invalidated { reason } if reason == "cancelled"));
}
#[tokio::test]
async fn accepted_terminal_waits_for_recorder_ack_without_false_drop() {
    let recording = Arc::new(Blocked::default());
    let mut observation = call(recording.clone());
    let result = Err(crate::Error::Cancelled);
    let mut finish = Box::pin(observation.finish(&result));
    assert!(futures_util::poll!(finish.as_mut()).is_pending());
    assert_eq!(recording.accepted.lock().unwrap().len(), 1);
    recording.acknowledged.notify_one();
    finish.await;
    drop(observation);
    assert!(recording.fallback.lock().unwrap().is_empty());
}

struct FullQueue {
    sender: tokio::sync::mpsc::Sender<ModelObservation>,
    fallback: Mutex<Vec<ModelObservation>>,
}
#[async_trait::async_trait]
impl ModelRecording for FullQueue {
    async fn record(&self, observation: ModelObservation) -> Result<(), String> {
        self.sender
            .send(observation)
            .await
            .map_err(|e| e.to_string())
    }
    fn dropped(&self, observation: ModelObservation) {
        self.fallback.lock().unwrap().push(observation);
    }
}
#[tokio::test]
async fn full_terminal_receiver_is_bounded_and_retains_previously_accepted_body() {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let recording = Arc::new(FullQueue {
        sender,
        fallback: Mutex::new(vec![]),
    });
    recording
        .record(identity(ObservationState::TextDelta {
            text: "accepted body".into(),
        }))
        .await
        .unwrap();
    let mut observation = call(recording.clone());
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        observation.finish(&Err(crate::Error::Cancelled)),
    )
    .await
    .unwrap();
    drop(observation);
    assert!(matches!(receiver.recv().await.unwrap().state,
        ObservationState::TextDelta { text } if text == "accepted body"));
    assert_eq!(recording.fallback.lock().unwrap().len(), 1);
    assert!(matches!(&recording.fallback.lock().unwrap()[0].state,
        ObservationState::Invalidated { reason } if reason == "cancelled"));
}
#[derive(Default)]
struct TerminalObserver {
    received: Mutex<Vec<ModelObservation>>,
}
impl ModelObserver for TerminalObserver {
    fn publish(&self, observation: ModelObservation) {
        self.received.lock().unwrap().push(observation);
    }
}
fn observed_call(
    recording: Arc<dyn ModelRecording>,
    observer: Arc<TerminalObserver>,
) -> CallObservation {
    let mut observation = call(recording);
    observation.observer = Some(observer);
    observation
}
fn assert_cancelled_once(observer: &TerminalObserver) {
    let received = observer.received.lock().unwrap();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].call_id, "exact-call");
    assert!(matches!(&received[0].state,
        ObservationState::Invalidated { reason } if reason == "cancelled"));
}
#[tokio::test]
async fn never_polled_finish_notifies_observer_and_durable_fallback_once() {
    let recording = Arc::new(Blocked::default());
    let observer = Arc::new(TerminalObserver::default());
    let mut observation = observed_call(recording.clone(), observer.clone());
    drop(observation.finish(&Err(crate::Error::Cancelled)));
    drop(observation);
    assert_cancelled_once(&observer);
    assert!(recording.accepted.lock().unwrap().is_empty());
    assert_eq!(recording.fallback.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn cancelled_ack_wait_notifies_observer_and_preserves_accepted_terminal() {
    let recording = Arc::new(Blocked::default());
    let observer = Arc::new(TerminalObserver::default());
    let mut observation = observed_call(recording.clone(), observer.clone());
    let result = Err(crate::Error::Cancelled);
    let mut finish = Box::pin(observation.finish(&result));
    assert!(futures_util::poll!(finish.as_mut()).is_pending());
    assert!(observer.received.lock().unwrap().is_empty());
    drop(finish);
    drop(observation);
    assert_cancelled_once(&observer);
    assert_eq!(recording.accepted.lock().unwrap().len(), 1);
    assert_eq!(recording.fallback.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn timed_out_ack_notifies_observer_of_validated_response_once() {
    let recording = Arc::new(Blocked::default());
    let observer = Arc::new(TerminalObserver::default());
    let mut observation = observed_call(recording.clone(), observer.clone());
    let result = Ok(Response {
        content: "validated answer".into(),
        tool_calls: vec![],
        usage: Default::default(),
        provider_metadata: None,
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        observation.finish(&result),
    )
    .await
    .unwrap();
    drop(observation);
    let received = observer.received.lock().unwrap();
    assert_eq!(received.len(), 1);
    assert!(matches!(&received[0].state,
        ObservationState::Validated { text } if text == "validated answer"));
    assert_eq!(recording.accepted.lock().unwrap().len(), 1);
    assert_eq!(recording.fallback.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn full_queue_timeout_notifies_observer_before_terminal_acceptance() {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let recording = Arc::new(FullQueue {
        sender,
        fallback: Mutex::new(vec![]),
    });
    recording
        .record(identity(ObservationState::TextDelta {
            text: "accepted body".into(),
        }))
        .await
        .unwrap();
    let observer = Arc::new(TerminalObserver::default());
    let mut observation = observed_call(recording.clone(), observer.clone());
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        observation.finish(&Err(crate::Error::Cancelled)),
    )
    .await
    .unwrap();
    drop(observation);
    assert_cancelled_once(&observer);
    assert_eq!(recording.fallback.lock().unwrap().len(), 1);
    assert!(matches!(receiver.recv().await.unwrap().state,
        ObservationState::TextDelta { text } if text == "accepted body"));
    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn acknowledged_finish_publishes_once_after_ack_without_fallback() {
    let recording = Arc::new(Blocked::default());
    let observer = Arc::new(TerminalObserver::default());
    let mut observation = observed_call(recording.clone(), observer.clone());
    let result = Err(crate::Error::Cancelled);
    let mut finish = Box::pin(observation.finish(&result));
    assert!(futures_util::poll!(finish.as_mut()).is_pending());
    assert!(observer.received.lock().unwrap().is_empty());
    recording.acknowledged.notify_one();
    finish.await;
    assert_cancelled_once(&observer);
    drop(observation);
    assert_cancelled_once(&observer);
    assert!(recording.fallback.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancelled_owner_drop_without_finish_notifies_both_ports_as_cancelled() {
    let recording = Arc::new(Blocked::default());
    let observer = Arc::new(TerminalObserver::default());
    let observation = observed_call(recording.clone(), observer.clone());
    observation.cancellation.cancel();
    drop(observation);
    assert_cancelled_once(&observer);
    assert!(matches!(&recording.fallback.lock().unwrap()[0].state,
        ObservationState::Invalidated { reason } if reason == "cancelled"));
}
#[tokio::test]
async fn explicit_validated_terminal_wins_over_later_token_cancellation() {
    let recording = Arc::new(Blocked::default());
    let observer = Arc::new(TerminalObserver::default());
    let mut observation = observed_call(recording.clone(), observer.clone());
    drop(observation.finish(&Ok(Response {
        content: "validated answer".into(),
        tool_calls: vec![],
        usage: Default::default(),
        provider_metadata: None,
    })));
    observation.cancellation.cancel();
    drop(observation);
    for port in [&observer.received, &recording.fallback] {
        let received = port.lock().unwrap();
        assert_eq!(received.len(), 1);
        assert!(matches!(&received[0].state,
            ObservationState::Validated { text } if text == "validated answer"));
    }
}

#[tokio::test]
async fn genuinely_unfinished_call_alone_uses_provider_call_dropped() {
    let recording = Arc::new(Blocked::default());
    drop(call(recording.clone()));
    assert!(matches!(&recording.fallback.lock().unwrap()[0].state,
        ObservationState::Invalidated { reason } if reason == "provider_call_dropped"));
}
