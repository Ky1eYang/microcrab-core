//! Behavior at Session's queue/steer admission boundary. Semaphores establish the
//! boundary deterministically; timeouts only bound a missing wake or leaked owner.
use microcrab_protocol::{InputPart, Response, ToolCall, ToolDefinition, Usage};
use microcrab_session::context::{Content, ContentItem};
use microcrab_session::runtime::{
    Runtime, ToolRegistration,
    journal::MemoryJournal,
    tool::{AllowTools, Tool, ToolContext, ToolOutput},
};
use microcrab_session::{Auditor, Client, Error, Limits, Session};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

#[path = "blocked_job_steering.rs"]
mod blocked_job_steering;

struct Gate {
    seen: Mutex<Vec<Content>>,
    entered: Semaphore,
    release: Semaphore,
    call: bool,
    fail_first: bool,
}
impl Gate {
    fn new(call: bool, fail_first: bool) -> Arc<Self> {
        Arc::new(Self {
            seen: Mutex::new(vec![]),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            call,
            fail_first,
        })
    }
    async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(3), self.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
    }
}
#[async_trait::async_trait]
impl Client for Gate {
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
        if self.fail_first && index == 1 {
            return Err(microcrab_transport::Error::Transport);
        }
        Ok(Response {
            provider_metadata: None,
            content: format!("answer-{index}"),
            tool_calls: if self.call {
                vec![ToolCall {
                    id: format!("echo-{index}"),
                    name: "echo".into(),
                    arguments: json!({}),
                    provider_metadata: None,
                }]
            } else {
                vec![]
            },
            usage: Usage::default(),
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
fn build(client: Arc<Gate>, cap: usize, journal: Arc<MemoryJournal>) -> Session {
    let runtime = Runtime::new(
        vec![ToolRegistration::External(Arc::new(Echo))],
        Arc::new(AllowTools::new(["echo"])),
    )
    .unwrap();
    Session::builder_arc(client, runtime)
        .limits(Limits {
            max_rounds: cap,
            ..Default::default()
        })
        .journal_arc(journal)
        .build()
        .unwrap()
}
fn users(content: &Content) -> Vec<String> {
    content
        .items
        .iter()
        .filter_map(|item| match item {
            ContentItem::User { parts, .. } => Some(
                parts
                    .iter()
                    .filter_map(|part| match part {
                        InputPart::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}
async fn quiet(session: &Session) {
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
}

#[tokio::test]
async fn steer_batches_once_at_cap_then_queue_uses_a_new_turn() {
    let client = Gate::new(false, false);
    let session = build(client.clone(), 1, Arc::new(MemoryJournal::new()));
    let running = session.clone();
    let turn = tokio::spawn(async move { running.submit("first").await });
    client.entered().await;
    assert!(matches!(session.submit("busy").await, Err(Error::Busy)));
    session.queue("queued").unwrap();
    session.steer("steer-a").unwrap();
    session.steer("steer-b").unwrap();
    client.release.add_permits(1);
    client.entered().await;
    {
        let seen = client.seen.lock().unwrap();
        assert_eq!(users(&seen[1]), ["first", "steer-a", "steer-b"]);
        assert_eq!(seen[0].turn_id, seen[1].turn_id);
        assert_eq!(seen[1].round, Some(2));
        assert!(matches!(
            seen[1].items[seen[1].items.len() - 3],
            ContentItem::State(_)
        ));
        assert!(matches!(
            seen[1].items.last(),
            Some(ContentItem::User { source: None, .. })
        ));
    }
    client.release.add_permits(1);
    assert_eq!(turn.await.unwrap().unwrap().rounds, 2);
    client.entered().await;
    {
        let seen = client.seen.lock().unwrap();
        assert_ne!(seen[1].turn_id, seen[2].turn_id);
        assert_eq!(seen[2].round, Some(1));
        assert_eq!(users(&seen[2]).last().unwrap(), "queued");
    }
    client.release.add_permits(1);
    quiet(&session).await;
    session.close().await.unwrap();
}

#[tokio::test]
async fn idle_steer_waits_and_queue_wakes_in_fifo_order() {
    let client = Gate::new(false, false);
    let session = build(client.clone(), 2, Arc::new(MemoryJournal::new()));
    session.steer("idle-steer").unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), client.entered.acquire())
            .await
            .is_err()
    );
    session.queue("wake").unwrap();
    client.entered().await;
    assert_eq!(users(&client.seen.lock().unwrap()[0]), ["idle-steer"]);
    client.release.add_permits(1);
    client.entered().await;
    assert_eq!(
        users(&client.seen.lock().unwrap()[1]),
        ["idle-steer", "wake"]
    );
    client.release.add_permits(1);
    quiet(&session).await;
    session.close().await.unwrap();
}

#[tokio::test]
async fn cancel_and_provider_failure_retain_unclaimed_items_and_pause_drain() {
    for provider_failure in [false, true] {
        let client = Gate::new(false, provider_failure);
        let session = build(client.clone(), 4, Arc::new(MemoryJournal::new()));
        let running = session.clone();
        let turn = tokio::spawn(async move { running.submit("first").await });
        client.entered().await;
        session.queue("queued").unwrap();
        session.steer("retained-steer").unwrap();
        if provider_failure {
            client.release.add_permits(1);
        } else {
            session.interrupt();
        }
        assert!(turn.await.unwrap().is_err());
        session.queue("also-retained").unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), client.entered.acquire())
                .await
                .is_err()
        );
        let running = session.clone();
        let resumed = tokio::spawn(async move { running.resume().await });
        client.entered().await;
        assert_eq!(
            users(&client.seen.lock().unwrap()[1]).last().unwrap(),
            "queued"
        );
        client.release.add_permits(1);
        resumed.await.unwrap().unwrap();
        for expected in ["retained-steer", "also-retained"] {
            client.entered().await;
            assert_eq!(
                users(client.seen.lock().unwrap().last().unwrap())
                    .last()
                    .unwrap(),
                expected
            );
            client.release.add_permits(1);
        }
        quiet(&session).await;
        session.close().await.unwrap();
    }
}

#[tokio::test]
async fn cap_settles_owed_tool_results_and_returns_success_without_resend() {
    let client = Gate::new(true, false);
    let journal = Arc::new(MemoryJournal::new());
    let session = build(client.clone(), 1, journal.clone());
    let running = session.clone();
    let turn = tokio::spawn(async move { running.submit("first").await });
    client.entered().await;
    client.release.add_permits(1);
    assert_eq!(turn.await.unwrap().unwrap().rounds, 1);
    assert_eq!(client.seen.lock().unwrap().len(), 1);
    let settled = journal.latest().unwrap();
    assert!(settled.continuation.is_none());
    assert!(matches!(
        settled.context.items.last(),
        Some(ContentItem::Tool { .. })
    ));
    assert!(!session.has_continuation());
    session.close().await.unwrap();
}

#[tokio::test]
async fn idle_dispatcher_does_not_keep_the_session_alive() {
    let client = Gate::new(false, false);
    let session = build(client, 2, Arc::new(MemoryJournal::new()));
    let state = session.state_handle();
    session.steer("retained without wake").unwrap();
    drop(session);
    assert!(state.inject("after_drop", json!(true)).await.is_err());
}

#[tokio::test]
async fn continuation_preserves_total_ordinal_and_unattended_count() {
    use microcrab_session::context::Context;
    use microcrab_session::runtime::journal::Recover;
    use microcrab_session::{RoundContinuation, SessionState};
    let legacy: RoundContinuation =
        serde_json::from_value(json!({"next_round":8,"usage":Usage::default()})).unwrap();
    assert_eq!(legacy.unattended_rounds, 0);
    let client = Gate::new(true, false);
    let runtime = Runtime::new(
        vec![ToolRegistration::External(Arc::new(Echo))],
        Arc::new(AllowTools::new(["echo"])),
    )
    .unwrap();
    let limits = Limits {
        max_rounds: 2,
        ..Default::default()
    };
    let mut context = Context::new(
        "system".into(),
        runtime.definitions(),
        limits.max_context_bytes,
    )
    .unwrap();
    context.admit_user("restored", &json!({})).unwrap();
    let mut encoded_state = serde_json::to_value(SessionState::new(limits.clone())).unwrap();
    encoded_state["turn_id"] = json!(1);
    encoded_state["last_turn_id"] = json!(1);
    encoded_state["round_id"] = json!(7);
    let state: SessionState = serde_json::from_value(encoded_state).unwrap();
    let session = Session::restore(
        Session::builder_arc(client.clone(), runtime).limits(limits),
        Recover {
            state,
            context: context.snapshot().unwrap(),
            continuation: Some(RoundContinuation {
                next_round: 8,
                unattended_rounds: 1,
                usage: Usage::default(),
                request_prepared: false,
                state_observation_pending: false,
            }),
            workspace: None,
            cache_report: None,
            agent: None,
        },
    )
    .await
    .unwrap();
    let running = session.clone();
    let resumed = tokio::spawn(async move { running.resume().await });
    client.entered().await;
    assert_eq!(client.seen.lock().unwrap()[0].round, Some(8));
    client.release.add_permits(1);
    assert_eq!(resumed.await.unwrap().unwrap().rounds, 8);
    assert_eq!(client.seen.lock().unwrap().len(), 1);
    session.close().await.unwrap();
}

#[tokio::test]
async fn invalid_boundary_input_is_retained_without_partial_context_append() {
    let client = Gate::new(false, false);
    let session = build(client.clone(), 2, Arc::new(MemoryJournal::new()));
    let running = session.clone();
    let turn = tokio::spawn(async move { running.submit("first").await });
    client.entered().await;
    session.steer("valid-but-unclaimed").unwrap();
    session.steer("   ").unwrap();
    client.release.add_permits(1);
    assert!(turn.await.unwrap().is_err());
    assert_eq!(
        users(&Content {
            items: session.messages(),
            ..client.seen.lock().unwrap()[0].clone()
        }),
        ["first"]
    );
    session.queue("no-auto-restart").unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), client.entered.acquire())
            .await
            .is_err()
    );
    session.close().await.unwrap();
}

/// A held activity reader is a deterministic admission barrier: execute owns
/// foreground but cannot finish activity publication or spawn its turn worker.
/// Closing/retiring on another thread must fence the token behind that barrier.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_and_retire_fence_admission_before_worker_spawn() {
    for retire in [false, true] {
        let client = Gate::new(false, false);
        let session = build(client.clone(), 2, Arc::new(MemoryJournal::new()));
        session.start().await.unwrap();
        let activity = session.subscribe_activity();
        let held_reader = activity.borrow();
        // stream returns with the exact foreground reservation already held;
        // polling start in a yield loop could repeatedly win that reservation.
        let mut submitted = session.stream("must not reach model").await.unwrap();
        assert!(matches!(session.checkpoint().await, Err(Error::Busy)));
        let stopping = session.clone();
        let stopped = tokio::spawn(async move {
            if retire {
                stopping.retire_conversation().await
            } else {
                stopping.close().await
            }
        });
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match session.submit("probe rejected by held foreground").await {
                    Err(Error::Closed) if !retire => break,
                    Err(Error::Config(message)) if retire && message.contains("retir") => break,
                    Err(Error::Busy) => tokio::task::yield_now().await,
                    other => panic!("stop fence: {other:?}"),
                }
            }
        })
        .await
        .unwrap();
        drop(held_reader);
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(3), submitted.recv())
                .await
                .unwrap(),
            Some(Err(_))
        ));
        tokio::time::timeout(Duration::from_secs(3), stopped)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(client.seen.lock().unwrap().is_empty());
        if retire {
            session.close().await.unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_interrupt_and_completion_publish_a_settled_activity_snapshot() {
    let client = Gate::new(false, false);
    let session = build(client.clone(), 2, Arc::new(MemoryJournal::new()));
    let running = session.clone();
    let submitted = tokio::spawn(async move { running.submit("race completion").await });
    client.entered().await;
    let barrier = Arc::new(tokio::sync::Barrier::new(9));
    let mut publishers = Vec::new();
    for _ in 0..8 {
        let publisher = session.clone();
        let barrier = barrier.clone();
        publishers.push(tokio::spawn(async move {
            barrier.wait().await;
            publisher.interrupt();
        }));
    }
    barrier.wait().await;
    client.release.add_permits(1);
    let _ = submitted.await.unwrap();
    for publisher in publishers {
        publisher.await.unwrap();
    }
    quiet(&session).await;
    assert!(!session.subscribe_activity().borrow().active);
    session.close().await.unwrap();
}

#[tokio::test]
async fn explicit_pending_resume_preserves_fifo_and_public_settlement_fence() {
    let client = Gate::new(false, false);
    let session = build(client.clone(), 2, Arc::new(MemoryJournal::new()));
    session.interrupt();
    session.queue("first pending").unwrap();
    session.steer("second pending").unwrap();
    let before = session.inbox_snapshot();
    assert!(before.paused);
    assert!(!before.active);
    let first = &before.items[0];
    let mut turns = session.subscribe_turns();
    let (mut stream, settlement) = session
        .resume_pending_input_with_settlement(
            &before.inbox_id,
            first.id,
            first.revision,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    client.entered().await;
    assert_eq!(users(&client.seen.lock().unwrap()[0]), ["first pending"]);
    client.release.add_permits(1);
    while let Some(event) = stream.recv().await {
        if matches!(
            event.unwrap(),
            microcrab_session::SessionEvent::Finished { .. }
        ) {
            break;
        }
    }
    let completed = turns.recv().await.unwrap();
    assert!(!completed.autonomous);
    let waiting = session.inbox_snapshot();
    assert!(!waiting.active);
    assert!(!waiting.paused);
    assert_eq!(waiting.items.len(), 1);
    assert_eq!(waiting.items[0].id, before.items[1].id);
    assert_eq!(client.seen.lock().unwrap().len(), 1);
    assert!(matches!(session.checkpoint().await, Err(Error::Busy)));
    drop(settlement);
    client.entered().await;
    assert_eq!(
        users(&client.seen.lock().unwrap()[1]),
        ["first pending", "second pending"]
    );
    client.release.add_permits(1);
    quiet(&session).await;
    assert!(session.inbox_snapshot().items.is_empty());
    session.close().await.unwrap();
}

#[tokio::test]
async fn explicit_send_activates_paused_idle_fifo_only_after_successful_admission() {
    let client = Gate::new(false, false);
    let session = build(client.clone(), 2, Arc::new(MemoryJournal::new()));
    session.interrupt();
    session.queue("older queued").unwrap();
    session.steer("older steer").unwrap();
    let before = serde_json::to_value(session.inbox_snapshot()).unwrap();
    assert!(session.queue_and_activate("x".repeat(1024 * 1024)).is_err());
    assert_eq!(
        serde_json::to_value(session.inbox_snapshot()).unwrap(),
        before
    );
    session.queue_and_activate("explicit new send").unwrap();
    for expected in [
        vec!["older queued"],
        vec!["older queued", "older steer"],
        vec!["older queued", "older steer", "explicit new send"],
    ] {
        client.entered().await;
        assert_eq!(users(client.seen.lock().unwrap().last().unwrap()), expected);
        client.release.add_permits(1);
    }
    quiet(&session).await;
    assert!(!session.inbox_snapshot().paused);
    assert!(session.inbox_snapshot().items.is_empty());
    session.close().await.unwrap();
}

#[tokio::test]
async fn explicit_send_busy_queues_without_replacing_worker_or_clearing_pause() {
    for pause in [false, true] {
        let client = Gate::new(false, false);
        let session = build(client.clone(), 2, Arc::new(MemoryJournal::new()));
        let first = {
            let session = session.clone();
            tokio::spawn(async move { session.submit("running").await })
        };
        client.entered().await;
        let clock = session.inbox_snapshot().execution_started_at_ms;
        // No await separates pause, admission and observation on this current-
        // thread runtime: the cancelled worker has not settled its active slot.
        if pause {
            session.interrupt();
        }
        session.queue_and_activate("explicit while busy").unwrap();
        let queued = session.inbox_snapshot();
        assert!(queued.active);
        assert_eq!(queued.paused, pause);
        assert_eq!(queued.execution_started_at_ms, clock);
        assert_eq!(client.seen.lock().unwrap().len(), 1);
        assert_eq!(
            session.pending_inputs(),
            [microcrab_session::SessionInput::text("explicit while busy")]
        );
        client.release.add_permits(1);
        let result = first.await.unwrap();
        if pause {
            assert!(matches!(result, Err(Error::Cancelled)));
            quiet(&session).await;
            assert!(session.inbox_snapshot().paused);
            assert_eq!(session.pending_inputs().len(), 1);
            assert_eq!(client.seen.lock().unwrap().len(), 1);
        } else {
            result.unwrap();
            client.entered().await;
            assert_eq!(
                users(&client.seen.lock().unwrap()[1]),
                ["running", "explicit while busy"]
            );
            client.release.add_permits(1);
            quiet(&session).await;
        }
        session.close().await.unwrap();
    }
}

#[tokio::test]
async fn explicit_send_rejects_inactive_settlement_reservation_before_admission() {
    let client = Gate::new(false, false);
    let session = build(client.clone(), 2, Arc::new(MemoryJournal::new()));
    let (mut stream, settlement) = session
        .stream_with_settlement("owned foreground", CancellationToken::new())
        .await
        .unwrap();
    client.entered().await;
    client.release.add_permits(1);
    while let Some(event) = stream.recv().await {
        if matches!(
            event.unwrap(),
            microcrab_session::SessionEvent::Finished { .. }
        ) {
            break;
        }
    }
    session.interrupt();
    let before = serde_json::to_value(session.inbox_snapshot()).unwrap();
    assert!(matches!(
        session.queue_and_activate("must remain a draft"),
        Err(Error::Busy)
    ));
    assert_eq!(
        serde_json::to_value(session.inbox_snapshot()).unwrap(),
        before
    );
    drop(settlement);
    session.close().await.unwrap();
}

#[tokio::test]
async fn explicit_pending_resume_rejects_stale_non_fifo_and_continuation_without_unpausing() {
    use microcrab_session::PendingInputError;
    let client = Gate::new(false, false);
    let session = build(client.clone(), 2, Arc::new(MemoryJournal::new()));
    session.interrupt();
    session.queue("first pending").unwrap();
    session.queue("second pending").unwrap();
    let before = session.inbox_snapshot();
    for (inbox_id, id, revision, expected) in [
        (
            "stale",
            before.items[0].id,
            0,
            PendingInputError::InboxChanged,
        ),
        (
            before.inbox_id.as_str(),
            u64::MAX,
            0,
            PendingInputError::NotFound,
        ),
        (
            before.inbox_id.as_str(),
            before.items[0].id,
            1,
            PendingInputError::RevisionConflict,
        ),
        (
            before.inbox_id.as_str(),
            before.items[1].id,
            0,
            PendingInputError::NotFirst,
        ),
    ] {
        let result = session
            .resume_pending_input_with_settlement(inbox_id, id, revision, CancellationToken::new())
            .await;
        assert!(matches!(result, Err(Error::PendingInput(actual)) if actual == expected));
        assert_eq!(
            serde_json::to_value(session.inbox_snapshot()).unwrap(),
            serde_json::to_value(&before).unwrap()
        );
    }
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(matches!(
        session
            .resume_pending_input_with_settlement(
                &before.inbox_id,
                before.items[0].id,
                0,
                cancelled,
            )
            .await,
        Err(Error::Cancelled)
    ));
    assert_eq!(client.seen.lock().unwrap().len(), 0);
    session.close().await.unwrap();

    let client = Gate::new(false, true);
    let session = build(client.clone(), 2, Arc::new(MemoryJournal::new()));
    client.release.add_permits(1);
    assert!(session.submit("failed request").await.is_err());
    assert!(session.retry_ready());
    session.queue("must wait for explicit retry").unwrap();
    let before = session.inbox_snapshot();
    assert!(before.paused);
    assert_eq!(client.seen.lock().unwrap().len(), 1);
    tokio::task::yield_now().await;
    assert_eq!(
        serde_json::to_value(session.inbox_snapshot()).unwrap(),
        serde_json::to_value(&before).unwrap()
    );
    session
        .remove_pending_input(
            &before.inbox_id,
            before.items[0].id,
            before.items[0].revision,
        )
        .unwrap();
    assert!(session.inbox_snapshot().paused);
    client.release.add_permits(1);
    session.resume().await.unwrap();
    {
        let seen = client.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(
            serde_json::to_value(&seen[0]).unwrap(),
            serde_json::to_value(&seen[1]).unwrap(),
            "manual retry reuses only the exact prepared request"
        );
    }
    assert!(session.pending_inputs().is_empty());
    session.close().await.unwrap();
}

#[tokio::test]
async fn explicit_activation_admits_new_turn_after_failed_request_without_replaying_old_input() {
    for pending_resume in [false, true] {
        let client = Gate::new(false, true);
        let session = build(client.clone(), 2, Arc::new(MemoryJournal::new()));
        client.release.add_permits(1);
        assert!(session.submit("failed original").await.is_err());
        client.entered().await;
        assert!(session.retry_ready());
        session.queue("first pending").unwrap();
        let before = session.inbox_snapshot();
        assert!(before.paused);
        if pending_resume {
            let (mut stream, settlement) = session
                .resume_pending_input_with_settlement(
                    &before.inbox_id,
                    before.items[0].id,
                    before.items[0].revision,
                    CancellationToken::new(),
                )
                .await
                .unwrap();
            client.entered().await;
            {
                let seen = client.seen.lock().unwrap();
                assert_eq!(seen.len(), 2);
                assert_eq!(seen[1].turn_id, seen[0].turn_id.map(|id| id + 1));
                assert_eq!(users(&seen[1]), vec!["failed original", "first pending"]);
            }
            assert!(matches!(session.resume().await, Err(Error::Busy)));
            client.release.add_permits(1);
            while let Some(event) = stream.recv().await {
                event.unwrap();
            }
            drop(settlement);
        } else {
            let mut invalid = microcrab_session::SessionInput::text("invalid reserved variable");
            invalid
                .variables
                .set
                .insert("mode".into(), json!("forbidden"));
            assert!(matches!(
                session.queue_and_activate(invalid),
                Err(Error::Config(_))
            ));
            assert_eq!(
                serde_json::to_value(session.inbox_snapshot()).unwrap(),
                serde_json::to_value(&before).unwrap()
            );
            session.queue_and_activate("second pending").unwrap();
            client.entered().await;
            {
                let seen = client.seen.lock().unwrap();
                assert_eq!(seen.len(), 2);
                assert_eq!(seen[1].turn_id, seen[0].turn_id.map(|id| id + 1));
                assert_eq!(users(&seen[1]), vec!["failed original", "first pending"]);
            }
            client.release.add_permits(1);
            client.entered().await;
            {
                let seen = client.seen.lock().unwrap();
                assert_eq!(seen.len(), 3);
                assert_eq!(
                    users(&seen[2]),
                    vec!["failed original", "first pending", "second pending"]
                );
            }
            client.release.add_permits(1);
        }
        quiet(&session).await;
        assert!(!session.has_continuation());
        assert!(
            session.resume().await.is_err(),
            "the old prepared retry is superseded, never replayed"
        );
        session.close().await.unwrap();
    }
}
