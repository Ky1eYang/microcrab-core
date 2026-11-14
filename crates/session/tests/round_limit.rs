//! Round confirmation uses the existing turn, inbox and cancellation owners.
use microcrab_protocol::{InputPart, Response, ToolCall, ToolDefinition, Usage};
use microcrab_session::{
    Auditor, Client, Error, Limits, Session, SessionBuilder,
    budget::{RoundLimitCallback, RoundLimitDecision, RoundLimitRequest},
    context::{Content, ContentItem},
    runtime::{
        Runtime, ToolRegistration, TurnStopReason,
        journal::MemoryJournal,
        tool::{AllowTools, Tool, ToolContext, ToolOutput},
    },
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

type Question = (RoundLimitRequest, oneshot::Sender<RoundLimitDecision>);
struct Questions {
    sender: mpsc::UnboundedSender<Question>,
    retired: Arc<AtomicUsize>,
}
struct Retired(Arc<AtomicUsize>);
impl Drop for Retired {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait::async_trait]
impl RoundLimitCallback for Questions {
    async fn request(
        &self,
        request: RoundLimitRequest,
        _: CancellationToken,
    ) -> Result<RoundLimitDecision, Error> {
        let _retired = Retired(self.retired.clone());
        let (sender, receiver) = oneshot::channel();
        self.sender.send((request, sender)).unwrap();
        Ok(receiver.await.unwrap())
    }
}
fn questions() -> (Arc<Questions>, mpsc::UnboundedReceiver<Question>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    (
        Arc::new(Questions {
            sender,
            retired: Arc::new(AtomicUsize::new(0)),
        }),
        receiver,
    )
}
struct Model {
    seen: Mutex<Vec<Content>>,
    entered: Semaphore,
    release: Semaphore,
    final_round: usize,
}
impl Model {
    fn new(final_round: usize) -> Arc<Self> {
        Arc::new(Self {
            seen: Mutex::new(vec![]),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            final_round,
        })
    }
    async fn next(&self) {
        tokio::time::timeout(Duration::from_secs(3), self.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
    }
    fn answer(&self) {
        self.release.add_permits(1);
    }
}
#[async_trait::async_trait]
impl Client for Model {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        cancellation: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let index = {
            let mut seen = self.seen.lock().unwrap();
            seen.push(content.clone());
            seen.len()
        };
        self.entered.add_permits(1);
        tokio::select! { biased;
            _ = cancellation.cancelled() => return Err(microcrab_transport::Error::Cancelled),
            permit = self.release.acquire() => permit.unwrap().forget(),
        }
        Ok(Response {
            content: format!("answer-{index}"),
            tool_calls: if index == self.final_round {
                vec![]
            } else {
                vec![ToolCall {
                    id: format!("echo-{index}"),
                    name: "echo".into(),
                    arguments: json!({}),
                    provider_metadata: None,
                }]
            },
            usage: Usage::default(),
            provider_metadata: None,
        })
    }
}
struct Echo;
#[async_trait::async_trait]
impl Tool for Echo {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "echo".into(),
            description: "echo".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn execute(&self, _: Value, _: ToolContext) -> Result<ToolOutput, Error> {
        Ok(ToolOutput::text("settled"))
    }
}
fn builder(model: Arc<Model>, cap: usize, journal: Arc<MemoryJournal>) -> SessionBuilder {
    let runtime = Runtime::new(
        vec![ToolRegistration::External(Arc::new(Echo))],
        Arc::new(AllowTools::new(["echo"])),
    )
    .unwrap();
    Session::builder_arc(model, runtime)
        .session_id("round-limit-test")
        .limits(Limits {
            max_rounds: cap,
            ..Default::default()
        })
        .journal_arc(journal)
}
async fn question(receiver: &mut mpsc::UnboundedReceiver<Question>) -> Question {
    tokio::time::timeout(Duration::from_secs(3), receiver.recv())
        .await
        .unwrap()
        .unwrap()
}

#[test]
fn budget_count_facts_do_not_claim_tool_calls_or_bytes() {
    use microcrab_session::budget::{LimitKind, check};
    for kind in [
        LimitKind::Processes,
        LimitKind::AttachmentCount,
        LimitKind::Rounds,
    ] {
        assert!(check(kind, 2, 2).is_ok());
        let Error::Limit(limit) = check(kind, 3, 2).unwrap_err() else {
            panic!("expected typed count rejection")
        };
        assert_eq!(limit.required_count, Some(3));
        assert_eq!(limit.budget_count, Some(2));
        assert_eq!(limit.calls, None);
        assert_eq!(limit.required_bytes, None);
        assert_eq!(limit.budget_bytes, None);
        assert!(!limit.is_pre_effect());
        assert_eq!(
            limit.clone().with_label("private owner text").sanitized(),
            limit
        );
    }
    let Error::Limit(limit) = check(LimitKind::ToolCalls, 3, 2).unwrap_err() else {
        panic!("expected tool-call rejection")
    };
    assert_eq!(limit.calls, Some(3));
    assert_eq!(limit.required_count, Some(3));
    assert_eq!(limit.budget_count, Some(2));
    assert_eq!(limit.required_bytes, None);
    let Error::Limit(limit) = check(LimitKind::Arithmetic, 3, 2).unwrap_err() else {
        panic!("expected arithmetic rejection")
    };
    assert_eq!(limit.required_count, None);
    assert_eq!(limit.calls, None);
    assert_eq!(limit.required_bytes, None);
}

#[tokio::test]
async fn budget_exhaustion_decisions_preserve_wire_and_caller_cancellation() {
    use microcrab_session::budget::Exhaustion;
    let request = RoundLimitRequest {
        session_id: "policy".into(),
        epoch: 2,
        turn_id: 3,
        kind: microcrab_session::budget::ConfirmationKind::Rounds,
        used: 64,
        block: 64,
    };
    assert_eq!(
        serde_json::to_value(&request).unwrap(),
        json!({
            "session_id":"policy", "epoch":2, "turn_id":3,
            "kind":"rounds", "used":64, "block":64,
        })
    );
    assert_eq!(
        Exhaustion::Stop
            .resolve(request.clone(), CancellationToken::new())
            .await
            .unwrap(),
        RoundLimitDecision::Stop
    );
    assert_eq!(
        Exhaustion::Continue
            .resolve(request.clone(), CancellationToken::new())
            .await
            .unwrap(),
        RoundLimitDecision::Continue
    );
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(matches!(
        Exhaustion::Continue.resolve(request, cancelled).await,
        Err(Error::Cancelled)
    ));
}

#[tokio::test]
async fn each_continue_grants_one_more_segment_without_a_new_turn() {
    let model = Model::new(usize::MAX);
    let journal = Arc::new(MemoryJournal::new());
    let (callback, mut asks) = questions();
    let session = builder(model.clone(), 2, journal.clone())
        .round_limit_callback(callback)
        .build()
        .unwrap();
    let running = session.clone();
    let turn = tokio::spawn(async move { running.submit("work").await });
    for _ in 0..2 {
        model.next().await;
        model.answer();
    }
    let (first, reply) = question(&mut asks).await;
    assert_eq!(first.session_id, "round-limit-test");
    assert_eq!(
        first.kind,
        microcrab_session::budget::ConfirmationKind::Rounds
    );
    assert_eq!(first.used, 2);
    assert_eq!(first.block, 2);
    let saved = journal.latest().unwrap();
    let continuation = saved.continuation.as_ref().unwrap();
    assert_eq!(continuation.next_round, 3);
    assert_eq!(continuation.unattended_rounds, 2);
    assert!(!continuation.request_prepared);
    saved.validate().unwrap();
    reply.send(RoundLimitDecision::Continue).unwrap();
    for _ in 0..2 {
        model.next().await;
        model.answer();
    }
    let (second, reply) = question(&mut asks).await;
    assert_eq!(second.used, 4);
    assert_eq!(second.turn_id, first.turn_id);
    assert_eq!(second.epoch, first.epoch);
    reply.send(RoundLimitDecision::Stop).unwrap();
    let result = turn.await.unwrap().unwrap();
    assert_eq!(result.rounds, 4);
    assert_eq!(result.stop_reason, TurnStopReason::RoundLimit);
    assert!(!session.has_continuation());
    {
        let seen = model.seen.lock().unwrap();
        assert_eq!(
            seen.iter().map(|content| content.round).collect::<Vec<_>>(),
            [Some(1), Some(2), Some(3), Some(4)]
        );
        assert!(
            seen.iter()
                .all(|content| content.turn_id == Some(first.turn_id))
        );
    }
    session.close().await.unwrap();
}

#[tokio::test]
async fn natural_completion_at_the_cap_does_not_ask() {
    let model = Model::new(2);
    let (callback, mut asks) = questions();
    let session = builder(model.clone(), 2, Arc::new(MemoryJournal::new()))
        .round_limit_callback(callback)
        .build()
        .unwrap();
    let running = session.clone();
    let turn = tokio::spawn(async move { running.submit("work").await });
    for _ in 0..2 {
        model.next().await;
        model.answer();
    }
    let result = turn.await.unwrap().unwrap();
    assert_eq!(result.rounds, 2);
    assert_eq!(result.stop_reason, TurnStopReason::ModelCompleted);
    assert!(asks.try_recv().is_err());
    session.close().await.unwrap();
}

#[tokio::test]
async fn absent_callback_stops_explicitly_at_the_cap() {
    let model = Model::new(usize::MAX);
    let session = builder(model.clone(), 2, Arc::new(MemoryJournal::new()))
        .build()
        .unwrap();
    let running = session.clone();
    let turn = tokio::spawn(async move { running.submit("work").await });
    for _ in 0..2 {
        model.next().await;
        model.answer();
    }
    let result = turn.await.unwrap().unwrap();
    assert_eq!(result.rounds, 2);
    assert_eq!(result.stop_reason, TurnStopReason::RoundLimit);
    assert!(
        session
            .subscribe_activity()
            .borrow()
            .latest_round_limit_stop
            .is_none()
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn boundary_user_input_renews_before_any_question() {
    let model = Model::new(2);
    let (callback, mut asks) = questions();
    let session = builder(model.clone(), 1, Arc::new(MemoryJournal::new()))
        .round_limit_callback(callback)
        .build()
        .unwrap();
    let running = session.clone();
    let turn = tokio::spawn(async move { running.submit("work").await });
    model.next().await;
    session.steer("user intervention").unwrap();
    model.answer();
    model.next().await;
    model.answer();
    assert_eq!(
        turn.await.unwrap().unwrap().stop_reason,
        TurnStopReason::ModelCompleted
    );
    assert!(asks.try_recv().is_err());
    session.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn waiting_has_no_turn_deadline_and_admits_arrivals_after_continue() {
    let model = Model::new(3);
    let (callback, mut asks) = questions();
    let session = builder(model.clone(), 2, Arc::new(MemoryJournal::new()))
        .limits(Limits {
            max_rounds: 2,
            ..Default::default()
        })
        .round_limit_callback(callback)
        .build()
        .unwrap();
    let running = session.clone();
    let turn = tokio::spawn(async move { running.submit("work").await });
    for _ in 0..2 {
        model.next().await;
        model.answer();
    }
    let (_, reply) = question(&mut asks).await;
    tokio::time::advance(Duration::from_secs(600)).await;
    assert!(!turn.is_finished());
    session.steer("arrived while waiting").unwrap();
    reply.send(RoundLimitDecision::Continue).unwrap();
    model.next().await;
    {
        let seen = model.seen.lock().unwrap();
        assert!(seen[2].items.iter().any(|item| matches!(item,
            ContentItem::User { parts, .. } if parts.iter().any(|part| matches!(part,
                InputPart::Text { text } if text == "arrived while waiting")))));
    }
    model.answer();
    assert_eq!(
        turn.await.unwrap().unwrap().stop_reason,
        TurnStopReason::ModelCompleted
    );
    session.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn interrupt_drops_pending_callback_without_another_model_request() {
    let model = Model::new(usize::MAX);
    let (callback, mut asks) = questions();
    let retired = callback.retired.clone();
    let session = builder(model.clone(), 1, Arc::new(MemoryJournal::new()))
        .round_limit_callback(callback)
        .build()
        .unwrap();
    let running = session.clone();
    let turn = tokio::spawn(async move { running.submit("work").await });
    model.next().await;
    model.answer();
    let (_, reply) = question(&mut asks).await;
    tokio::time::advance(Duration::from_secs(86_400)).await;
    assert!(!turn.is_finished());
    session.interrupt();
    assert!(matches!(turn.await.unwrap(), Err(Error::Cancelled)));
    assert_eq!(retired.load(Ordering::SeqCst), 1);
    assert!(reply.send(RoundLimitDecision::Continue).is_err());
    assert_eq!(model.seen.lock().unwrap().len(), 1);
    session.close().await.unwrap();
}

#[tokio::test]
async fn restoring_a_pending_boundary_requires_a_fresh_confirmation() {
    let model = Model::new(usize::MAX);
    let journal = Arc::new(MemoryJournal::new());
    let (callback, mut asks) = questions();
    let session = builder(model.clone(), 1, journal.clone())
        .round_limit_callback(callback)
        .build()
        .unwrap();
    let running = session.clone();
    let turn = tokio::spawn(async move { running.submit("work").await });
    model.next().await;
    model.answer();
    let (old_request, old_reply) = question(&mut asks).await;
    let saved = journal.latest().unwrap();
    session.interrupt();
    assert!(matches!(turn.await.unwrap(), Err(Error::Cancelled)));
    assert!(old_reply.send(RoundLimitDecision::Continue).is_err());
    session.close().await.unwrap();

    let fresh_model = Model::new(usize::MAX);
    let (callback, mut fresh_asks) = questions();
    let restored = Session::restore(
        builder(fresh_model.clone(), 1, Arc::new(MemoryJournal::new()))
            .round_limit_callback(callback),
        saved,
    )
    .await
    .unwrap();
    let running = restored.clone();
    let turn = tokio::spawn(async move { running.resume().await });
    let (request, reply) = question(&mut fresh_asks).await;
    assert_eq!(request, old_request);
    assert!(fresh_model.seen.lock().unwrap().is_empty());
    reply.send(RoundLimitDecision::Stop).unwrap();
    assert_eq!(
        turn.await.unwrap().unwrap().stop_reason,
        TurnStopReason::RoundLimit
    );
    restored.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn renewed_rounds_outlast_legacy_turn_timeout_without_losing_confirmation() {
    let model = Model::new(4);
    let (callback, mut asks) = questions();
    let session = builder(model.clone(), 2, Arc::new(MemoryJournal::new()))
        .limits(Limits {
            max_rounds: 2,
            ..Default::default()
        })
        .round_limit_callback(callback)
        .build()
        .unwrap();
    let running = session.clone();
    let turn = tokio::spawn(async move { running.submit("work").await });
    for _ in 0..2 {
        model.next().await;
        tokio::time::advance(Duration::from_millis(400)).await;
        assert!(!turn.is_finished());
        model.answer();
    }
    let (_, reply) = question(&mut asks).await;
    tokio::time::advance(Duration::from_secs(600)).await;
    assert!(!turn.is_finished());
    reply.send(RoundLimitDecision::Continue).unwrap();
    for _ in 0..2 {
        model.next().await;
        tokio::time::advance(Duration::from_millis(400)).await;
        assert!(!turn.is_finished());
        model.answer();
    }
    let outcome = turn.await.unwrap().unwrap();
    assert_eq!(outcome.rounds, 4);
    assert_eq!(outcome.stop_reason, TurnStopReason::ModelCompleted);
    assert_eq!(model.seen.lock().unwrap().len(), 4);
    assert!(asks.try_recv().is_err());
    session.close().await.unwrap();
}

#[tokio::test]
async fn autonomous_stop_keeps_its_exact_locator_after_a_later_success() {
    let model = Model::new(2);
    let (callback, mut asks) = questions();
    let session = builder(model.clone(), 1, Arc::new(MemoryJournal::new()))
        .round_limit_callback(callback)
        .build()
        .unwrap();
    let activity = session.subscribe_activity();
    assert!(activity.borrow().latest_round_limit_stop.is_none());
    let mut completions = session.subscribe_turns();
    session.queue("queued work").unwrap();
    model.next().await;
    model.answer();
    let (request, reply) = question(&mut asks).await;
    reply.send(RoundLimitDecision::Stop).unwrap();
    let completed = tokio::time::timeout(Duration::from_secs(3), completions.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(completed.autonomous);
    assert_eq!(completed.epoch, request.epoch);
    assert_eq!(completed.turn, request.turn_id);
    assert_eq!(completed.outcome.stop_reason, TurnStopReason::RoundLimit);
    let stop = activity.borrow().latest_round_limit_stop.clone().unwrap();
    assert_eq!(stop.sequence, completed.sequence);
    assert_eq!(stop.epoch, request.epoch);
    assert_eq!(stop.turn, request.turn_id);
    assert_eq!(stop.rounds, completed.outcome.rounds);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match session.checkpoint().await {
                Ok(_) => break,
                Err(Error::Busy) => tokio::task::yield_now().await,
                Err(error) => panic!("checkpoint: {error}"),
            }
        }
    })
    .await
    .unwrap();
    let running = session.clone();
    let next = tokio::spawn(async move { running.submit("ordinary next turn").await });
    model.next().await;
    model.answer();
    assert_eq!(
        next.await.unwrap().unwrap().stop_reason,
        TurnStopReason::ModelCompleted
    );
    let completed = completions.recv().await.unwrap();
    assert!(!completed.autonomous);
    assert_eq!(completed.sequence, stop.sequence + 1);
    assert_eq!(completed.epoch, stop.epoch);
    assert_eq!(completed.turn, stop.turn + 1);
    let current = activity.borrow();
    assert_eq!(current.latest_round_limit_stop.as_ref(), Some(&stop));
    assert_eq!(
        current.latest_outcome.as_ref().unwrap().stop_reason,
        TurnStopReason::ModelCompleted
    );
    drop(current);
    session.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn stop_after_an_unanswered_day_preserves_the_settled_boundary() {
    let model = Model::new(usize::MAX);
    let (callback, mut asks) = questions();
    let session = builder(model.clone(), 1, Arc::new(MemoryJournal::new()))
        .limits(Limits {
            max_rounds: 1,
            ..Default::default()
        })
        .round_limit_callback(callback)
        .build()
        .unwrap();
    let running = session.clone();
    let turn = tokio::spawn(async move { running.submit("work").await });
    model.next().await;
    model.answer();
    let (_, reply) = question(&mut asks).await;
    tokio::time::advance(Duration::from_secs(86_400)).await;
    assert!(!turn.is_finished());
    assert_eq!(model.seen.lock().unwrap().len(), 1);
    reply.send(RoundLimitDecision::Stop).unwrap();
    let outcome = turn.await.unwrap().unwrap();
    assert_eq!(outcome.rounds, 1);
    assert_eq!(outcome.stop_reason, TurnStopReason::RoundLimit);
    assert!(!session.has_continuation());
    assert!(!session.executions().has_pending());
    session.close().await.unwrap();
}

struct SteadyModel;
#[async_trait::async_trait]
impl Client for SteadyModel {
    async fn complete(
        &self,
        content: &Content,
        auditor: &dyn Auditor,
        variables: &dyn microcrab_auditor::VariableView,
        cancellation: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.stream(content, auditor, variables, cancellation, None)
            .await
    }

    async fn stream(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        cancellation: CancellationToken,
        events: Option<mpsc::Sender<microcrab_transport::ModelEvent>>,
    ) -> Result<Response, microcrab_transport::Error> {
        for _ in 0..4 {
            tokio::select! { biased;
                _ = cancellation.cancelled() => return Err(microcrab_transport::Error::Cancelled),
                _ = tokio::time::sleep(Duration::from_millis(20)) => {},
            }
            if let Some(events) = &events {
                events
                    .send(microcrab_transport::ModelEvent::TextDelta {
                        text: "progress ".into(),
                    })
                    .await
                    .map_err(|_| microcrab_transport::Error::Cancelled)?;
            }
        }
        let round = content.round.unwrap();
        Ok(Response {
            content: "progress ".repeat(4),
            tool_calls: if round < 3 {
                vec![ToolCall {
                    id: format!("steady-{round}"),
                    name: "echo".into(),
                    arguments: json!({}),
                    provider_metadata: None,
                }]
            } else {
                vec![]
            },
            usage: Usage::default(),
            provider_metadata: None,
        })
    }
}

#[tokio::test(start_paused = true)]
async fn steady_stream_across_multiple_rounds_outlasts_the_legacy_turn_limit() {
    let runtime = Runtime::new(
        vec![ToolRegistration::External(Arc::new(Echo))],
        Arc::new(AllowTools::new(["echo"])),
    )
    .unwrap();
    let session = Session::builder(SteadyModel, runtime)
        .limits(Limits {
            ..Default::default()
        })
        .build()
        .unwrap();
    let before = tokio::time::Instant::now();
    let mut stream = session.stream("steady work").await.unwrap();
    let mut deltas = 0;
    let mut finished = None;
    while let Some(event) = stream.recv().await {
        match event.unwrap() {
            microcrab_session::SessionEvent::Model {
                event: microcrab_transport::ModelEvent::TextDelta { .. },
            } => deltas += 1,
            microcrab_session::SessionEvent::Finished { outcome } => finished = Some(outcome),
            _ => {}
        }
    }
    assert_eq!(deltas, 12);
    let outcome = finished.unwrap();
    assert_eq!(outcome.rounds, 3);
    assert_eq!(outcome.stop_reason, TurnStopReason::ModelCompleted);
    assert!(before.elapsed() >= Duration::from_millis(240));
    let executions = session.executions().snapshot();
    assert_eq!(executions.len(), 2);
    assert!(executions.iter().all(|execution| execution.status
        == microcrab_session::runtime::execution::ExecutionStatus::Succeeded));
    assert!(!session.executions().has_pending());
    drop(stream);
    session.close().await.unwrap();
}

#[test]
fn default_budget_and_legacy_outcome_decode_are_explicit() {
    assert_eq!(Limits::default().max_rounds, 64);
    let result: microcrab_session::runtime::TurnOutcome = serde_json::from_value(json!({
        "content":"done", "rounds":1, "usage":Usage::default()
    }))
    .unwrap();
    assert_eq!(result.stop_reason, TurnStopReason::ModelCompleted);
}

/// Answers one fixed response; used to drive a single consumption checkpoint.
struct Fixed(Response);
#[async_trait::async_trait]
impl Client for Fixed {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        Ok(self.0.clone())
    }
}
fn checkpoint_builder(model: Arc<dyn Client>, limits: Limits) -> SessionBuilder {
    let runtime = Runtime::new(
        vec![ToolRegistration::External(Arc::new(Echo))],
        Arc::new(AllowTools::new(["echo"])),
    )
    .unwrap();
    Session::builder_arc(model, runtime)
        .session_id("limit-checkpoint-test")
        .limits(limits)
        .journal_arc(Arc::new(MemoryJournal::new()))
}

#[tokio::test]
async fn context_checkpoint_asks_before_effects_and_continue_grants_one_block() {
    use microcrab_session::budget::ConfirmationKind;
    let response = Response {
        content: "x".repeat(200 * 1024),
        tool_calls: vec![],
        usage: Usage::default(),
        provider_metadata: None,
    };
    let budget = 64 * 1024;
    let (callback, mut asks) = questions();
    let session = checkpoint_builder(
        Arc::new(Fixed(response.clone())),
        Limits {
            max_context_bytes: budget,
            ..Default::default()
        },
    )
    .round_limit_callback(callback)
    .build()
    .unwrap();
    let running = session.clone();
    let turn = tokio::spawn(async move { running.submit("work").await });
    let (request, reply) = question(&mut asks).await;
    assert_eq!(request.kind, ConfirmationKind::ContextBytes);
    assert!(
        request.used > budget,
        "required bytes must exceed the budget"
    );
    assert_eq!(
        request.block,
        (request.used - budget).max(1024 * 1024),
        "one block covers the deficit and buys usable headroom"
    );
    reply.send(RoundLimitDecision::Continue).unwrap();
    let outcome = turn.await.unwrap().unwrap();
    assert_eq!(outcome.rounds, 1);
    assert_eq!(outcome.content.len(), response.content.len());
    // One agreed budget in both witnesses, raised by exactly the granted block.
    assert_eq!(
        session.state().limits.max_context_bytes,
        budget + request.block
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn context_checkpoint_stop_keeps_a_recoverable_continuation() {
    use microcrab_session::budget::ConfirmationKind;
    let budget = 64 * 1024;
    let (callback, mut asks) = questions();
    let session = checkpoint_builder(
        Arc::new(Fixed(Response {
            content: "y".repeat(200 * 1024),
            tool_calls: vec![],
            usage: Usage::default(),
            provider_metadata: None,
        })),
        Limits {
            max_context_bytes: budget,
            ..Default::default()
        },
    )
    .round_limit_callback(callback)
    .build()
    .unwrap();
    let running = session.clone();
    let turn = tokio::spawn(async move { running.submit("work").await });
    let (request, reply) = question(&mut asks).await;
    assert_eq!(request.kind, ConfirmationKind::ContextBytes);
    reply.send(RoundLimitDecision::Stop).unwrap();
    let Err(Error::Limit(limit)) = turn.await.unwrap() else {
        panic!("stop must keep the pre-effect limit rejection")
    };
    assert!(limit.is_pre_effect());
    assert_eq!(session.state().limits.max_context_bytes, budget);
    assert!(session.retry_ready());
    session.close().await.unwrap();
}

#[tokio::test]
async fn tool_call_checkpoint_asks_before_dispatch_and_continue_runs_the_batch() {
    use microcrab_session::budget::ConfirmationKind;
    // One batch response, then a plain answer so the turn terminates.
    struct Batch {
        first: Response,
        done: Response,
        sent: Mutex<bool>,
    }
    #[async_trait::async_trait]
    impl Client for Batch {
        async fn complete(
            &self,
            _: &Content,
            _: &dyn Auditor,
            _: &dyn microcrab_auditor::VariableView,
            _: CancellationToken,
        ) -> Result<Response, microcrab_transport::Error> {
            let mut sent = self.sent.lock().unwrap();
            if *sent {
                Ok(self.done.clone())
            } else {
                *sent = true;
                Ok(self.first.clone())
            }
        }
    }
    let (callback, mut asks) = questions();
    let session = checkpoint_builder(
        Arc::new(Batch {
            first: Response {
                content: "batch".into(),
                tool_calls: vec![
                    ToolCall {
                        id: "echo-1".into(),
                        name: "echo".into(),
                        arguments: json!({}),
                        provider_metadata: None,
                    },
                    ToolCall {
                        id: "echo-2".into(),
                        name: "echo".into(),
                        arguments: json!({}),
                        provider_metadata: None,
                    },
                ],
                usage: Usage::default(),
                provider_metadata: None,
            },
            done: Response {
                content: "finished".into(),
                tool_calls: vec![],
                usage: Usage::default(),
                provider_metadata: None,
            },
            sent: Mutex::new(false),
        }),
        Limits {
            max_tool_calls_per_round: 1,
            ..Default::default()
        },
    )
    .round_limit_callback(callback)
    .build()
    .unwrap();
    let running = session.clone();
    let turn = tokio::spawn(async move { running.submit("work").await });
    let (request, reply) = question(&mut asks).await;
    assert_eq!(request.kind, ConfirmationKind::ToolCalls);
    assert_eq!(request.used, 2);
    assert_eq!(request.block, 1);
    reply.send(RoundLimitDecision::Continue).unwrap();
    let outcome = turn.await.unwrap().unwrap();
    assert_eq!(outcome.content, "finished");
    assert_eq!(outcome.rounds, 2);
    let executions = session.executions().snapshot();
    assert_eq!(
        executions.len(),
        2,
        "both calls of the confirmed batch must run"
    );
    session.close().await.unwrap();
}
