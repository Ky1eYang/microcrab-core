use super::*;

struct InterruptedRecording {
    token: CancellationToken,
    cancel_on_body: bool,
    accepted: Mutex<Vec<ModelObservation>>,
    fallback: Mutex<Vec<ModelObservation>>,
}
#[async_trait::async_trait]
impl ModelRecording for InterruptedRecording {
    async fn record(&self, observation: ModelObservation) -> Result<(), String> {
        let body = matches!(observation.state, ObservationState::TextDelta { .. });
        let terminal = matches!(
            observation.state,
            ObservationState::Validated { .. } | ObservationState::Invalidated { .. }
        );
        self.accepted.lock().unwrap().push(observation);
        if body && self.cancel_on_body {
            // Acceptance survives the provider future losing its cancellation race.
            self.token.cancel();
            std::future::pending::<()>().await;
        }
        if terminal {
            // A terminal receiver can accept without acknowledging. The transport
            // must keep its terminal intent and finish within its bounded wait.
            std::future::pending::<()>().await;
        }
        Ok(())
    }
    fn dropped(&self, observation: ModelObservation) {
        self.fallback.lock().unwrap().push(observation);
    }
}
fn content() -> Content {
    Content {
        epoch: 1,
        subagent: None,
        system: "synthetic".into(),
        items: vec![ContentItem::User {
            source: None,
            parts: vec![InputPart::Text {
                text: "question".into(),
            }],
        }],
        tools: vec![],
        max_output_tokens: 128,
        max_bytes: 1024 * 1024,
        turn_id: Some(1),
        round: Some(1),
    }
}

#[tokio::test]
async fn all_real_adapters_preserve_cancelled_terminal_when_accepted_body_ack_is_cancelled() {
    for protocol in [
        Protocol::OpenaiCompatible,
        Protocol::OpenaiResponses,
        Protocol::Anthropic,
        Protocol::Google,
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
        let body = fixture(protocol, "reasoning");
        let worker = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let _ = read_body(&mut socket);
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        });
        let token = CancellationToken::new();
        let recorder = Arc::new(InterruptedRecording {
            token: token.clone(),
            cancel_on_body: true,
            accepted: Mutex::new(vec![]),
            fallback: Mutex::new(vec![]),
        });
        let client = TransportClient::new(TransportConfig {
            auth: Default::default(),
            protocol,
            endpoint,
            model: "synthetic".into(),
            api_key: None,
            timeout_ms: 10_000,
            max_response_bytes: 1024 * 1024,
        })
        .unwrap()
        .with_recording(recorder.clone());
        // An events port requests SSE for Google too. Capacity exceeds this fixture.
        let (events, _receiver) = tokio::sync::mpsc::channel(16);
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            client.stream(
                &content(),
                &NoopAuditor,
                &EmptyVariableView,
                token,
                Some(events),
            ),
        )
        .await
        .unwrap();
        assert!(
            matches!(result, Err(microcrab_transport::Error::Cancelled)),
            "{protocol:?}: {result:?}"
        );
        worker.join().unwrap();
        assert!(recorder.accepted.lock().unwrap().iter().any(|event|
            matches!(&event.state, ObservationState::TextDelta { text } if text == "answer")), "{protocol:?}");
        let fallback = recorder.fallback.lock().unwrap();
        assert_eq!(fallback.len(), 1, "{protocol:?}");
        assert!(
            matches!(&fallback[0].state, ObservationState::Invalidated { reason } if reason == "cancelled"),
            "{protocol:?}"
        );
    }
}

#[derive(Default)]
struct Observer(Mutex<Vec<ModelObservation>>);
impl microcrab_transport::ModelObserver for Observer {
    fn publish(&self, observation: ModelObservation) {
        self.0.lock().unwrap().push(observation);
    }
}

#[tokio::test]
async fn outer_biased_cancellation_preserves_cancelled_terminal_for_every_adapter() {
    for protocol in [
        Protocol::OpenaiCompatible,
        Protocol::OpenaiResponses,
        Protocol::Anthropic,
        Protocol::Google,
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
        let body = fixture(protocol, "reasoning");
        let worker = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let _ = read_body(&mut socket);
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        });
        let token = CancellationToken::new();
        let recorder = Arc::new(InterruptedRecording {
            token: token.clone(),
            cancel_on_body: true,
            accepted: Mutex::new(vec![]),
            fallback: Mutex::new(vec![]),
        });
        let observer = Arc::new(Observer::default());
        let client = TransportClient::new(TransportConfig {
            auth: Default::default(),
            protocol,
            endpoint,
            model: "synthetic".into(),
            api_key: None,
            timeout_ms: 10_000,
            max_response_bytes: 1024 * 1024,
        })
        .unwrap()
        .with_recording(recorder.clone())
        .with_observer(observer.clone());
        let (events, _receiver) = tokio::sync::mpsc::channel(16);
        let input = content();
        let result = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                biased;
                _ = token.cancelled() => Err(microcrab_transport::Error::Cancelled),
                result = client.stream_admitted(
                    &input,
                    &NoopAuditor,
                    &EmptyVariableView,
                    token.clone(),
                    Some(events),
                ) => result,
            }
        })
        .await
        .unwrap();
        assert!(
            matches!(result, Err(microcrab_transport::Error::Cancelled)),
            "{protocol:?}"
        );
        worker.join().unwrap();
        assert!(recorder.accepted.lock().unwrap().iter().any(|observation|
            matches!(&observation.state, ObservationState::TextDelta { text } if text == "answer")), "{protocol:?}");
        let fallback = recorder.fallback.lock().unwrap();
        assert_eq!(fallback.len(), 1, "{protocol:?}");
        assert!(
            matches!(&fallback[0].state,
            ObservationState::Invalidated { reason } if reason == "cancelled"),
            "{protocol:?}"
        );
        let observed = observer.0.lock().unwrap();
        let terminals: Vec<_> = observed
            .iter()
            .filter(|observation| {
                matches!(
                    observation.state,
                    ObservationState::Validated { .. } | ObservationState::Invalidated { .. }
                )
            })
            .collect();
        assert_eq!(terminals.len(), 1, "{protocol:?}");
        assert_eq!(terminals[0].call_id, fallback[0].call_id, "{protocol:?}");
        assert!(
            matches!(&terminals[0].state,
            ObservationState::Invalidated { reason } if reason == "cancelled"),
            "{protocol:?}"
        );
    }
}

#[tokio::test]
async fn all_real_adapters_retain_success_when_terminal_ack_receiver_stalls() {
    for protocol in [
        Protocol::OpenaiCompatible,
        Protocol::OpenaiResponses,
        Protocol::Anthropic,
        Protocol::Google,
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
        let body = json_fixture(protocol, "reasoning", "answer");
        let worker = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let _ = read_body(&mut socket);
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        });
        let token = CancellationToken::new();
        let recorder = Arc::new(InterruptedRecording {
            token: token.clone(),
            cancel_on_body: false,
            accepted: Mutex::new(vec![]),
            fallback: Mutex::new(vec![]),
        });
        let client = TransportClient::new(TransportConfig {
            auth: Default::default(),
            protocol,
            endpoint,
            model: "synthetic".into(),
            api_key: None,
            timeout_ms: 10_000,
            max_response_bytes: 1024 * 1024,
        })
        .unwrap()
        .with_recording(recorder.clone());
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            client.complete(&content(), &NoopAuditor, &EmptyVariableView, token),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result.content, "answer", "{protocol:?}");
        worker.join().unwrap();
        let fallback = recorder.fallback.lock().unwrap();
        assert_eq!(fallback.len(), 1, "{protocol:?}");
        assert!(
            matches!(&fallback[0].state, ObservationState::Validated { text } if text == "answer"),
            "{protocol:?}"
        );
    }
}
