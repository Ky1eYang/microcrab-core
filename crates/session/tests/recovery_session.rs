//! Recovery preserves settled model/tool work and restores state across epoch boundaries.
use crate::hooks;
use async_trait::async_trait;
use microcrab_protocol::Moment;
use microcrab_protocol::{Response, ToolCall, ToolDefinition, Usage};
use microcrab_session::context::{Content, ContentItem};
use microcrab_session::runtime::execution::ToolOutput;
use microcrab_session::runtime::journal::{Journal, MemoryJournal, Recover};
use microcrab_session::runtime::tool::{AllowTools, DenyAll, Tool, ToolContext};
use microcrab_session::runtime::{Runtime, ToolRegistration};
use microcrab_session::{Auditor, Client, Error, Session};
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio_util::sync::CancellationToken;

struct Script {
    responses: Mutex<VecDeque<Result<Response, microcrab_transport::Error>>>,
    seen: Mutex<Vec<Content>>,
}
impl Script {
    fn new(responses: Vec<Result<Response, microcrab_transport::Error>>) -> Self {
        Self {
            responses: Mutex::new(responses.into()),
            seen: Mutex::new(vec![]),
        }
    }
}
#[async_trait::async_trait]
impl Client for Script {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.seen.lock().unwrap().push(content.clone());
        self.responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted model request")
    }
}
fn answer(text: &str) -> Response {
    Response {
        provider_metadata: None,
        content: text.into(),
        tool_calls: vec![],
        usage: Usage::default(),
    }
}
fn empty_runtime() -> Runtime {
    Runtime::new(vec![], Arc::new(DenyAll)).unwrap()
}

#[tokio::test]
async fn restore_is_unstarted_and_announces_restoration_when_started() {
    let journal = Arc::new(MemoryJournal::new());
    let original = Session::builder(Script::new(vec![Ok(answer("saved"))]), empty_runtime())
        .journal_arc(journal.clone())
        .build()
        .unwrap();
    original.submit("remember this").await.unwrap();
    original
        .state_handle()
        .inject("saved", json!(7))
        .await
        .unwrap();
    let saved = original.checkpoint().await.unwrap();
    let messages = original.messages();
    let old_epoch = original.state().epoch_id;
    original.close().await.unwrap();

    let client = Arc::new(Script::new(vec![]));
    let hooks = Arc::new(hooks::Recording::default());
    let restored = Session::restore(
        Session::builder_arc(client.clone(), empty_runtime())
            .journal_arc(journal)
            .auditor(hooks.clone()),
        saved,
    )
    .await
    .unwrap();
    assert_eq!(restored.messages(), messages);
    assert_eq!(restored.state_handle().extract("saved"), Some(json!(7)));
    assert_eq!(restored.state().epoch_id, old_epoch);
    assert!(client.seen.lock().unwrap().is_empty());
    assert!(hooks.moments().is_empty());
    restored.start().await.unwrap();
    assert_eq!(hooks.moments(), vec![Moment::SessionRestored]);
    assert!(client.seen.lock().unwrap().is_empty());
    restored.close().await.unwrap();
}

struct Effect {
    calls: Arc<AtomicUsize>,
    path: Option<PathBuf>,
}
#[async_trait]
impl Tool for Effect {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "effect".into(),
            description: "Record an observable effect".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn execute(&self, _: Value, _: ToolContext) -> Result<ToolOutput, Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(path) = &self.path {
            std::fs::write(path, b"written by tool")
                .map_err(|error| Error::Tool(error.to_string().into()))?;
        }
        Ok(ToolOutput::text("effect recorded"))
    }
}
fn effect_runtime(calls: Arc<AtomicUsize>, path: Option<PathBuf>) -> Runtime {
    Runtime::new(
        vec![ToolRegistration::External(Arc::new(Effect { calls, path }))],
        Arc::new(AllowTools::new(["effect"])),
    )
    .unwrap()
}
fn request_effect() -> Response {
    Response {
        provider_metadata: None,
        content: "working".into(),
        tool_calls: vec![ToolCall {
            provider_metadata: None,
            id: "effect-1".into(),
            name: "effect".into(),
            arguments: json!({}),
        }],
        usage: Usage::default(),
    }
}

struct RecoveryRouting {
    refresh: bool,
    client: Arc<Script>,
    acknowledged: Arc<AtomicUsize>,
    expected_acknowledgements: usize,
    installed: Mutex<Option<Recover>>,
}
#[async_trait]
impl microcrab_session::TurnRouting for RecoveryRouting {
    async fn prepare(&self) -> Result<Option<microcrab_session::ConfigurationEffect>, Error> {
        Ok(None)
    }
    fn apply(&self, _: u64) -> Result<(), Error> {
        Ok(())
    }
    fn client(&self, _: u64) -> Arc<dyn Client> {
        self.client.clone()
    }
    async fn before_checkpoint(&self, _: &Recover) -> Result<(), Error> {
        Ok(())
    }
    async fn checkpoint_committed(&self, _: &Recover) -> Result<(), Error> {
        Ok(())
    }
    fn refresh_context_on_restore(&self) -> bool {
        self.refresh
    }
    async fn restore(&self, recover: &Recover) -> Result<(), Error> {
        assert_eq!(
            self.acknowledged.load(Ordering::SeqCst),
            self.expected_acknowledgements
        );
        *self.installed.lock().unwrap() = Some(recover.clone());
        Ok(())
    }
}
struct ExactRecoveryJournal {
    expected: Vec<u8>,
    acknowledged: Arc<AtomicUsize>,
}
#[async_trait]
impl Journal for ExactRecoveryJournal {
    async fn commit(&self, _: Recover) -> Result<(), Error> {
        Ok(())
    }
    async fn recovered(&self, recover: Recover) -> Result<(), Error> {
        assert_eq!(serde_json::to_vec(&recover).unwrap(), self.expected);
        self.acknowledged.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn recovery_rebuilds_old_expand_catalog_without_changing_external_tool_authority() {
    fn runtime_with_expand(effects: Arc<AtomicUsize>) -> Runtime {
        Runtime::new(
            vec![
                ToolRegistration::Builtin(microcrab_session::runtime::BuiltinTool::Expand),
                ToolRegistration::External(Arc::new(Effect {
                    calls: effects,
                    path: None,
                })),
            ],
            Arc::new(AllowTools::new(["effect", "expand"])),
        )
        .unwrap()
    }
    let effects = Arc::new(AtomicUsize::new(0));
    let source = Session::builder(
        Script::new(vec![Ok(request_effect()), Ok(answer("done"))]),
        runtime_with_expand(effects.clone()),
    )
    .build()
    .unwrap();
    source.submit("effect once").await.unwrap();
    let mut historical = source.checkpoint().await.unwrap();
    let expand = historical
        .context
        .tools
        .iter_mut()
        .find(|tool| tool.name == "expand")
        .unwrap();
    expand.parameters["oneOf"] = json!([{"required":["ids"]},{"required":["reference"]}]);
    historical.validate().unwrap();
    let frozen = serde_json::to_vec(&historical).unwrap();
    source.close().await.unwrap();
    for initial in [false, true] {
        let builder = Session::builder(Script::new(vec![]), runtime_with_expand(effects.clone()));
        let session = if initial {
            Session::restore(builder, historical.clone()).await.unwrap()
        } else {
            let session = builder.build().unwrap();
            session.recover(historical.clone()).await.unwrap();
            session
        };
        let live = session.context_snapshot().unwrap();
        assert_eq!(live.epoch, historical.context.epoch + 1);
        assert!(!live.pending_epoch);
        assert!(
            live.tools
                .iter()
                .find(|tool| tool.name == "expand")
                .unwrap()
                .parameters
                .get("oneOf")
                .is_none()
        );
        assert_eq!(
            live.historical_tools.get(&historical.context.epoch),
            Some(&historical.context.tools)
        );
        assert_eq!(
            live.tools.iter().find(|tool| tool.name == "effect"),
            historical
                .context
                .tools
                .iter()
                .find(|tool| tool.name == "effect")
        );
        live.validate().unwrap();
        assert_eq!(effects.load(Ordering::SeqCst), 1);
        assert_eq!(serde_json::to_vec(&historical).unwrap(), frozen);
        session.close().await.unwrap();
    }
    let mut unsupported = historical;
    unsupported
        .context
        .tools
        .iter_mut()
        .find(|tool| tool.name == "expand")
        .unwrap()
        .parameters["oneOf"] = json!([{"required":["ids"]}]);
    assert!(
        Session::restore(
            Session::builder(Script::new(vec![]), runtime_with_expand(effects)),
            unsupported
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn route_recovery_refreshes_only_live_epoch_and_never_replays_effects() {
    let effects = Arc::new(AtomicUsize::new(0));
    let source_journal = Arc::new(MemoryJournal::new());
    let source = Session::builder(
        Script::new(vec![
            Ok(request_effect()),
            Err(microcrab_transport::Error::Transport),
        ]),
        effect_runtime(effects.clone(), None),
    )
    .journal_arc(source_journal.clone())
    .build()
    .unwrap();
    assert!(source.submit("do it once").await.is_err());
    let original = source_journal.latest().unwrap();
    let frozen = serde_json::to_vec(&original).unwrap();
    source.close().await.unwrap();
    for (refresh, initial) in [(false, false), (true, false), (false, true), (true, true)] {
        let client = Arc::new(Script::new(vec![Ok(answer("continued"))]));
        let acknowledged = Arc::new(AtomicUsize::new(0));
        let route = Arc::new(RecoveryRouting {
            refresh,
            client: client.clone(),
            acknowledged: acknowledged.clone(),
            expected_acknowledgements: usize::from(!initial),
            installed: Mutex::new(None),
        });
        let journal = Arc::new(ExactRecoveryJournal {
            expected: frozen.clone(),
            acknowledged,
        });
        let builder = Session::builder_arc(client.clone(), effect_runtime(effects.clone(), None))
            .journal_arc(journal)
            .turn_routing(route.clone());
        let session = if initial {
            Session::restore(builder, original.clone()).await.unwrap()
        } else {
            let session = builder.build().unwrap();
            // This branch exercises replacement of an initialized live Session.
            // Startup attachment is covered separately by Session::restore.
            session.start().await.unwrap();
            session.recover(original.clone()).await.unwrap();
            session
        };
        let installed = route.installed.lock().unwrap().clone().unwrap();
        assert_eq!(
            installed.context.epoch,
            original.context.epoch + u64::from(refresh)
        );
        assert_eq!(session.state().epoch_id, installed.context.epoch);
        assert_eq!(
            serde_json::to_value(&installed.continuation).unwrap(),
            serde_json::to_value(&original.continuation).unwrap()
        );
        if !refresh {
            assert_eq!(serde_json::to_vec(&installed).unwrap(), frozen);
        }
        assert!(client.seen.lock().unwrap().is_empty());
        assert_eq!(effects.load(Ordering::SeqCst), 1);
        assert_eq!(session.resume().await.unwrap().content, "continued");
        assert_eq!(effects.load(Ordering::SeqCst), 1);
        let requests = client.seen.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0]
                .items
                .iter()
                .filter(|item| matches!(item, ContentItem::Tool { .. }))
                .count(),
            1
        );
        drop(requests);
        assert_eq!(serde_json::to_vec(&original).unwrap(), frozen);
        session.close().await.unwrap();
    }
}

#[tokio::test]
async fn restored_round_continuation_does_not_replay_settled_tools_or_user_input() {
    let journal = Arc::new(MemoryJournal::new());
    let effects = Arc::new(AtomicUsize::new(0));
    let original = Session::builder(
        Script::new(vec![
            Ok(request_effect()),
            Err(microcrab_transport::Error::Protocol(
                "injected second-round failure".into(),
            )),
        ]),
        effect_runtime(effects.clone(), None),
    )
    .journal_arc(journal.clone())
    .build()
    .unwrap();
    assert!(matches!(
        original.submit("do it once").await,
        Err(Error::Protocol(_))
    ));
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    let saved = journal.latest().unwrap();
    assert_eq!(saved.continuation.as_ref().unwrap().next_round, 2);
    let turn = saved.state.turn_id;
    original.close().await.unwrap();

    for replace in [false, true] {
        let client = Arc::new(Script::new(vec![Ok(answer("continued"))]));
        let restored = Session::restore(
            Session::builder_arc(client.clone(), effect_runtime(effects.clone(), None))
                .journal_arc(journal.clone()),
            saved.clone(),
        )
        .await
        .unwrap();
        assert!(restored.has_continuation());
        assert_eq!(restored.state().turn_id, turn);
        assert!(
            restored.submit("").await.is_err(),
            "invalid replacement input must preserve the prepared retry"
        );
        assert_eq!(restored.state().turn_id, turn);
        assert!(restored.has_continuation());
        assert!(client.seen.lock().unwrap().is_empty());
        let outcome = if replace {
            restored.submit("explicit fresh Turn").await.unwrap()
        } else {
            restored.resume().await.unwrap()
        };
        assert_eq!(outcome.content, "continued");
        assert!(!restored.has_continuation());
        assert!(
            restored.resume().await.is_err(),
            "a settled or superseded retry cannot replay"
        );
        assert_eq!(effects.load(Ordering::SeqCst), 1);
        {
            let requests = client.seen.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(
                requests[0].turn_id,
                if replace { turn.map(|id| id + 1) } else { turn }
            );
            assert_eq!(
                requests[0]
                    .items
                    .iter()
                    .filter(|item| matches!(item, ContentItem::User { .. }))
                    .count(),
                if replace { 2 } else { 1 }
            );
            assert_eq!(
                requests[0]
                    .items
                    .iter()
                    .filter(|item| matches!(item, ContentItem::Tool { .. }))
                    .count(),
                1
            );
        }
        restored.close().await.unwrap();
    }
}

#[tokio::test]
async fn model_failure_retry_after_tools_reuses_request_without_replaying_effects() {
    let effects = Arc::new(AtomicUsize::new(0));
    let client = Arc::new(Script::new(vec![
        Ok(request_effect()),
        Err(microcrab_transport::Error::Transport),
        Ok(answer("retried")),
    ]));
    let session = Session::builder_arc(client.clone(), effect_runtime(effects.clone(), None))
        .build()
        .unwrap();
    assert!(matches!(
        session.submit("once").await,
        Err(Error::Transport)
    ));
    assert!(session.retry_ready());
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(session.resume().await.unwrap().content, "retried");
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    {
        let requests = client.seen.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert_eq!(
            serde_json::to_value(&requests[1]).unwrap(),
            serde_json::to_value(&requests[2]).unwrap()
        );
    }
    session.close().await.unwrap();
}

struct IdleTimeoutThenAnswer {
    responses: Mutex<VecDeque<Option<Response>>>,
    seen: Mutex<Vec<serde_json::Value>>,
}
#[async_trait]
impl Client for IdleTimeoutThenAnswer {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.seen
            .lock()
            .unwrap()
            .push(serde_json::to_value(content).unwrap());
        let response = self
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted inactivity request");
        match response {
            Some(response) => Ok(response),
            None => {
                // Timeout belongs to the model client/transport. The historical
                // turn_timeout_ms no longer terminates an incomplete response.
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                Err(microcrab_transport::Error::ProviderIdleTimeout(
                    microcrab_transport::IdleTimeout {
                        timeout_ms: 25,
                        elapsed_ms: 25,
                        first_token_ms: Some(5),
                        streamed_bytes: 12,
                        streamed_chars: 10,
                    },
                ))
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn transport_idle_timeout_retains_first_request_for_exact_retry_after_restore() {
    let client = Arc::new(IdleTimeoutThenAnswer {
        responses: Mutex::new(vec![None, Some(answer("retried"))].into()),
        seen: Mutex::new(vec![]),
    });
    let journal = Arc::new(MemoryJournal::new());
    let limits = microcrab_session::Limits {
        ..Default::default()
    };
    let session = Session::builder_arc(client.clone(), empty_runtime())
        .limits(limits.clone())
        .journal_arc(journal.clone())
        .build()
        .unwrap();
    let before = tokio::time::Instant::now();
    assert!(matches!(
        session.submit("original input").await,
        Err(Error::ProviderIdleTimeout(details))
            if details.timeout_ms == 25
                && details.elapsed_ms == 25
                && details.first_token_ms == Some(5)
                && details.streamed_bytes == 12
                && details.streamed_chars == 10
    ));
    assert_eq!(before.elapsed(), std::time::Duration::from_millis(25));
    assert!(session.retry_ready());
    let saved = journal.latest().unwrap();
    let continuation = saved.continuation.as_ref().unwrap();
    assert!(continuation.request_prepared);
    assert_eq!(continuation.next_round, 1);
    assert_eq!(continuation.unattended_rounds, 0);
    session.close().await.unwrap();

    let restored = Session::restore(
        Session::builder_arc(client.clone(), empty_runtime()).limits(limits),
        saved,
    )
    .await
    .unwrap();
    restored.start().await.unwrap();
    assert!(restored.retry_ready());
    let mut stream = restored.retry_stream().await.unwrap();
    while let Some(event) = stream.recv().await {
        event.unwrap();
    }
    assert!(!restored.retry_ready());
    {
        let seen = client.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0], seen[1]);
    }
    assert_eq!(
        restored
            .messages()
            .iter()
            .filter(|item| matches!(item, ContentItem::User { .. }))
            .count(),
        1
    );
    restored.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn transport_idle_timeout_after_tools_retains_request_without_repeating_effects() {
    let effects = Arc::new(AtomicUsize::new(0));
    let client = Arc::new(IdleTimeoutThenAnswer {
        responses: Mutex::new(vec![Some(request_effect()), None, Some(answer("retried"))].into()),
        seen: Mutex::new(vec![]),
    });
    let journal = Arc::new(MemoryJournal::new());
    let session = Session::builder_arc(client.clone(), effect_runtime(effects.clone(), None))
        .limits(microcrab_session::Limits {
            ..Default::default()
        })
        .journal_arc(journal.clone())
        .build()
        .unwrap();
    assert!(matches!(
        session.submit("once").await,
        Err(Error::ProviderIdleTimeout(details))
            if details.timeout_ms == 25
                && details.elapsed_ms == 25
                && details.first_token_ms == Some(5)
                && details.streamed_bytes == 12
                && details.streamed_chars == 10
    ));
    assert!(session.retry_ready());
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    let saved = journal.latest().unwrap();
    let continuation = saved.continuation.as_ref().unwrap();
    assert!(continuation.request_prepared);
    assert_eq!(continuation.next_round, 2);
    assert_eq!(continuation.unattended_rounds, 1);
    let mut stream = session.retry_stream().await.unwrap();
    while let Some(event) = stream.recv().await {
        event.unwrap();
    }
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert!(!session.retry_ready());
    {
        let seen = client.seen.lock().unwrap();
        assert_eq!(seen.len(), 3);
        assert_eq!(seen[1], seen[2]);
    }
    session.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn autonomous_idle_timeout_keeps_numeric_failure_facts_and_retry_readiness() {
    let client = Arc::new(IdleTimeoutThenAnswer {
        responses: Mutex::new(vec![None].into()),
        seen: Mutex::new(vec![]),
    });
    let session = Session::builder_arc(client, empty_runtime())
        .limits(microcrab_session::Limits {
            ..Default::default()
        })
        .build()
        .unwrap();
    let mut activity = session.subscribe_activity();
    session.queue("queued work").unwrap();
    let failure = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if let Some(failure) = activity.borrow_and_update().latest_inbox_failure.clone() {
                break failure;
            }
            activity.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(matches!(failure.kind,
        microcrab_session::InboxFailureKind::ProviderIdleTimeout(details)
            if details.timeout_ms == 25
                && details.elapsed_ms == 25
                && details.first_token_ms == Some(5)
                && details.streamed_bytes == 12
                && details.streamed_chars == 10
    ));
    assert!(failure.retry_ready);
    assert!(session.retry_ready());
    session.close().await.unwrap();
}

#[tokio::test]
async fn selected_recovery_preserves_exact_compaction_positions() {
    let session = Session::builder(
        Script::new(vec![Ok(answer("before")), Ok(answer("after"))]),
        empty_runtime(),
    )
    .build()
    .unwrap();
    session.submit("original question").await.unwrap();
    session
        .state_handle()
        .inject("phase", json!("before"))
        .await
        .unwrap();
    let before = session.checkpoint().await.unwrap();
    let before_messages = session.messages();
    session
        .compact("retained summary", json!({"phase":"after"}))
        .await
        .unwrap();
    let after = session.checkpoint().await.unwrap();
    let after_messages = session.messages();
    assert_ne!(before_messages, after_messages);
    let epoch = session.state().epoch_id;
    session.recover(before).await.unwrap();
    assert_eq!(session.messages(), before_messages);
    assert_eq!(
        session.state_handle().extract("phase"),
        Some(json!("before"))
    );
    assert_eq!(session.state().epoch_id, epoch);
    let epoch = session.state().epoch_id;
    session.recover(after).await.unwrap();
    assert_eq!(session.messages(), after_messages);
    assert_eq!(
        session.state_handle().extract("phase"),
        Some(json!("after"))
    );
    assert_eq!(session.state().epoch_id, epoch);
    session.submit("next question").await.unwrap();
    assert_eq!(
        serde_json::to_value(session.state()).unwrap()["last_turn_id"],
        1
    );
    assert_eq!(session.state().epoch_id, 1);
    session.close().await.unwrap();
}

#[tokio::test]
async fn rollback_to_turn_two_reuses_turn_three_with_distinct_provider_call_ids() {
    let effects = Arc::new(AtomicUsize::new(0));
    let mut responses = vec![];
    for id in [
        "turn-one",
        "turn-two",
        "old-turn-three",
        "branch-turn-three",
    ] {
        let mut response = request_effect();
        response.tool_calls[0].id = id.into();
        responses.push(Ok(response));
        responses.push(Ok(answer(id)));
    }
    let client = Arc::new(Script::new(responses));
    let session = Session::builder_arc(client.clone(), effect_runtime(effects.clone(), None))
        .build()
        .unwrap();
    session.submit("first").await.unwrap();
    session.submit("second").await.unwrap();
    let turn_two = session.checkpoint().await.unwrap();
    let mut expected_state = serde_json::to_value(&turn_two.state).unwrap();
    // Recovery retains private invocation history, while readable State excludes
    // synchronous Final rows and never exposes private restoration payloads.
    expected_state.as_object_mut().unwrap().remove("executions");
    let expected_context = serde_json::to_value(&turn_two.context).unwrap();
    session.submit("original third").await.unwrap();
    assert_eq!(
        serde_json::to_value(session.state()).unwrap()["last_turn_id"],
        3
    );
    let live_tasks = session.executions().snapshot();
    assert_eq!(live_tasks.len(), 3);
    let discarded_id = live_tasks[2].execution_id;
    assert_eq!(turn_two.state.executions.len(), 2);
    session.recover(turn_two).await.unwrap();
    assert_eq!(
        serde_json::to_value(session.state()).unwrap(),
        expected_state
    );
    assert_eq!(
        serde_json::to_value(session.checkpoint().await.unwrap().context).unwrap(),
        expected_context
    );
    assert_eq!(session.state().epoch_id, 0);
    session.submit("branch third").await.unwrap();
    let branch_rows = session.execution_snapshots();
    assert_eq!(branch_rows.len(), 3);
    assert!(
        branch_rows
            .iter()
            .all(|row| row.call_id != "old-turn-three")
    );
    let branch = branch_rows
        .iter()
        .find(|row| row.call_id == "branch-turn-three")
        .unwrap();
    assert!(branch.execution_id.0 > discarded_id.0);
    assert_eq!(
        serde_json::to_value(session.state()).unwrap()["last_turn_id"],
        3
    );
    assert_eq!(effects.load(Ordering::SeqCst), 4);
    {
        let requests = client.seen.lock().unwrap();
        assert_eq!(requests[4].turn_id, Some(3));
        assert_eq!(requests[6].turn_id, Some(3));
        assert!(requests.iter().all(|request| request.epoch == 0));
    }
    let final_context = session.checkpoint().await.unwrap().context;
    assert!(final_context.used_call_ids.contains("branch-turn-three"));
    assert!(!final_context.used_call_ids.contains("old-turn-three"));
    let encoded = serde_json::to_vec(&final_context).unwrap();
    let roundtrip = microcrab_session::context::Context::from_snapshot(
        serde_json::from_slice(&encoded).unwrap(),
    )
    .unwrap()
    .snapshot()
    .unwrap();
    assert_eq!(encoded, serde_json::to_vec(&roundtrip).unwrap());
    session.close().await.unwrap();
}

#[tokio::test]
async fn selected_recovery_preserves_files_and_never_runs_the_tool_again() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("effect.txt");
    let host = Arc::new(
        microcrab_host_local::LocalHost::new(directory.path(), Default::default()).unwrap(),
    );
    let effects = Arc::new(AtomicUsize::new(0));
    let journal = Arc::new(MemoryJournal::default());
    let session = Session::builder(
        Script::new(vec![Ok(request_effect()), Ok(answer("done"))]),
        effect_runtime(effects.clone(), Some(path.clone())),
    )
    .host(host.clone())
    .journal_arc(journal.clone())
    .build()
    .unwrap();
    session.start().await.unwrap();
    let before = session.checkpoint().await.unwrap();
    session.submit("write the file").await.unwrap();
    let after = session.checkpoint().await.unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"written by tool");
    assert!(before.workspace.is_none());
    assert!(after.workspace.is_none());
    std::fs::write(&path, b"external edit after tool").unwrap();
    session.recover(before.clone()).await.unwrap();
    assert_eq!(session.messages(), before.context.items);
    assert_eq!(std::fs::read(&path).unwrap(), b"external edit after tool");
    session.recover(after.clone()).await.unwrap();
    assert_eq!(session.messages(), after.context.items);
    assert_eq!(std::fs::read(&path).unwrap(), b"external edit after tool");
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    session.close().await.unwrap();

    // Initial attachment accepts either conversation position without comparing
    // or rewriting files changed by tools or external processes.
    let restored_before = Session::restore(
        Session::builder(
            Script::new(vec![]),
            effect_runtime(effects.clone(), Some(path.clone())),
        )
        .host(host.clone())
        .journal_arc(journal.clone()),
        before.clone(),
    )
    .await
    .unwrap();
    assert_eq!(restored_before.messages(), before.context.items);
    assert_eq!(std::fs::read(&path).unwrap(), b"external edit after tool");
    restored_before.close().await.unwrap();
    let restored = Session::restore(
        Session::builder(
            Script::new(vec![]),
            effect_runtime(effects.clone(), Some(path.clone())),
        )
        .host(host)
        .journal_arc(journal.clone()),
        after.clone(),
    )
    .await
    .unwrap();
    assert_eq!(restored.messages(), after.context.items);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(std::fs::read(&path).unwrap(), b"external edit after tool");
    let successor = restored.checkpoint().await.unwrap();
    assert_eq!(
        serde_json::to_value(journal.latest().unwrap()).unwrap(),
        serde_json::to_value(successor).unwrap()
    );
    restored.close().await.unwrap();
}

#[derive(Default)]
struct RejectPublication {
    journal: MemoryJournal,
    publications: AtomicUsize,
}
#[async_trait::async_trait]
impl Journal for RejectPublication {
    async fn commit(&self, recover: Recover) -> Result<(), Error> {
        self.journal.commit(recover).await
    }
    async fn recovered(&self, _: Recover) -> Result<(), Error> {
        self.publications.fetch_add(1, Ordering::SeqCst);
        Err(Error::Provider(
            "injected recovery publication failure".into(),
        ))
    }
}

#[tokio::test]
async fn failed_recovery_publication_preserves_conversation_and_external_files() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("effect.txt");
    let host = Arc::new(
        microcrab_host_local::LocalHost::new(directory.path(), Default::default()).unwrap(),
    );
    let journal = Arc::new(RejectPublication::default());
    let session = Session::builder(Script::new(vec![]), empty_runtime())
        .host(host)
        .journal_arc(journal.clone())
        .build()
        .unwrap();
    session.start().await.unwrap();
    session
        .state_handle()
        .inject("phase", json!("before"))
        .await
        .unwrap();
    let before = session.checkpoint().await.unwrap();
    session
        .state_handle()
        .inject("phase", json!("current"))
        .await
        .unwrap();
    let after = session.checkpoint().await.unwrap();
    let state = serde_json::to_value(session.state()).unwrap();
    let messages = session.messages();

    std::fs::write(&path, b"unrecorded external edit").unwrap();
    assert!(matches!(
        session.recover(before).await,
        Err(Error::Provider(_))
    ));
    // External changes do not gate conversation history selection. Publication
    // failure must still preserve the previous runtime/journal selection, leave
    // files untouched and quarantine the ambiguous recovery transaction.
    assert_eq!(journal.publications.load(Ordering::SeqCst), 1);
    assert_eq!(std::fs::read(&path).unwrap(), b"unrecorded external edit");
    assert_eq!(serde_json::to_value(session.state()).unwrap(), state);
    assert_eq!(
        session.state_handle().extract("phase"),
        Some(json!("current"))
    );
    assert_eq!(session.messages(), messages);
    assert_eq!(
        serde_json::to_value(journal.journal.latest().unwrap()).unwrap(),
        serde_json::to_value(after).unwrap()
    );
    assert!(matches!(session.start().await, Err(Error::Config(_))));
    session.close().await.unwrap();
}
