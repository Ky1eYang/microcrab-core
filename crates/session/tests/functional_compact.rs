use crate::hooks;
use microcrab_protocol::{Response, ToolCall, ToolDefinition, Usage};
use microcrab_session::context::Content;
use microcrab_session::runtime::execution::ToolOutput;
use microcrab_session::runtime::journal::{Journal, Recover};
use microcrab_session::runtime::{
    Runtime, ToolRegistration,
    tool::{AllowTools, Tool, ToolContext},
};
use microcrab_session::{Auditor, Client, Error, Limits, Session};
use microcrab_transport::render_ast;
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

struct Script {
    responses: Mutex<VecDeque<Result<Response, microcrab_transport::Error>>>,
    seen: Mutex<Vec<Content>>,
    pause: bool,
    entered: Notify,
    release: Notify,
}
impl Script {
    fn new(responses: Vec<Result<Response, microcrab_transport::Error>>, pause: bool) -> Self {
        Self {
            responses: Mutex::new(responses.into()),
            seen: Mutex::new(vec![]),
            pause,
            entered: Notify::new(),
            release: Notify::new(),
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
        token: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let index = {
            let mut seen = self.seen.lock().unwrap();
            seen.push(content.clone());
            seen.len()
        };
        if self.pause && index == 2 {
            self.entered.notify_one();
            tokio::select! {
                _ = token.cancelled() => return Err(microcrab_transport::Error::Cancelled),
                _ = self.release.notified() => {},
            }
        }
        self.responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted response")
    }
}
fn answer(content: &str) -> Response {
    Response {
        provider_metadata: None,
        content: content.into(),
        tool_calls: vec![],
        usage: Usage::default(),
    }
}
#[derive(Default)]
struct JournalLog(Mutex<Vec<Recover>>);
#[async_trait::async_trait]
impl Journal for JournalLog {
    async fn commit(&self, value: Recover) -> Result<(), Error> {
        self.0.lock().unwrap().push(value);
        Ok(())
    }
}
struct Effect(Arc<AtomicUsize>);
#[async_trait::async_trait]
impl Tool for Effect {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "effect".into(),
            description: "Observable effect".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn execute(&self, _: Value, _: ToolContext) -> Result<ToolOutput, Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutput::text("effect"))
    }
}
fn runtime(effects: Arc<AtomicUsize>) -> Runtime {
    Runtime::new(
        vec![ToolRegistration::External(Arc::new(Effect(effects)))],
        Arc::new(AllowTools::new(["effect"])),
    )
    .unwrap()
}

struct ConcurrentRename {
    entered: Notify,
    release: tokio::sync::Semaphore,
    main_entered: Notify,
    main_release: tokio::sync::Semaphore,
    block_main: bool,
    fail: bool,
    dropped: Arc<AtomicUsize>,
    helper_views: Mutex<Vec<(Option<Value>, Option<Value>)>>,
}
impl ConcurrentRename {
    fn new(block_main: bool, fail: bool) -> Self {
        Self {
            entered: Notify::new(),
            release: tokio::sync::Semaphore::new(0),
            main_entered: Notify::new(),
            main_release: tokio::sync::Semaphore::new(0),
            block_main,
            fail,
            dropped: Arc::new(AtomicUsize::new(0)),
            helper_views: Mutex::new(vec![]),
        }
    }
}
struct ProviderDropped(Arc<AtomicUsize>);
impl Drop for ProviderDropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait::async_trait]
impl Client for ConcurrentRename {
    async fn complete(
        &self,
        content: &Content,
        auditor: &dyn Auditor,
        variables: &dyn microcrab_auditor::VariableView,
        token: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        use microcrab_auditor::{AuditBody, HookContext, Moment};
        let helper = content.subagent.as_deref() == Some("rename");
        let call = if helper { "rename-call" } else { "main-call" };
        auditor
            .intervene(
                Moment::RequestPrepared,
                HookContext {
                    epoch: content.epoch,
                    turn: content.turn_id,
                    round: content.round,
                    subagent: content.subagent.clone(),
                    call_id: Some(call.into()),
                    request_body: Some("{}"),
                    max_request_bytes: Some(1024),
                    variables,
                    payload: Default::default(),
                },
            )
            .await
            .map_err(|_| microcrab_transport::Error::Transport)?;
        if helper {
            let _dropped = ProviderDropped(self.dropped.clone());
            self.helper_views
                .lock()
                .unwrap()
                .push((variables.get("session"), variables.get("marker")));
            self.entered.notify_one();
            tokio::select! {
                _ = token.cancelled() => return Err(microcrab_transport::Error::Cancelled),
                permit = self.release.acquire() => permit.unwrap().forget(),
            }
            self.helper_views
                .lock()
                .unwrap()
                .push((variables.get("session"), variables.get("marker")));
            if self.fail {
                return Err(microcrab_transport::Error::Transport);
            }
        } else if self.block_main {
            // A real transport publishes before returning; keep that witness
            // pending while a concurrent helper publishes/completes its own.
            auditor.publish(
                Moment::RoundCompleted,
                AuditBody {
                    epoch: content.epoch,
                    turn_id: content.turn_id,
                    round: content.round,
                    call_id: Some(call.into()),
                    response_status: Some(200),
                    ..Default::default()
                },
            );
            self.main_entered.notify_one();
            tokio::select! {
                _ = token.cancelled() => return Err(microcrab_transport::Error::Cancelled),
                permit = self.main_release.acquire() => permit.unwrap().forget(),
            }
        }
        if helper || !self.block_main {
            auditor.publish(
                Moment::RoundCompleted,
                AuditBody {
                    epoch: content.epoch,
                    turn_id: content.turn_id,
                    round: content.round,
                    subagent: content.subagent.clone(),
                    call_id: Some(call.into()),
                    response_status: Some(200),
                    ..Default::default()
                },
            );
        }
        Ok(answer(if helper {
            "Concurrent title"
        } else {
            "Primary result"
        }))
    }
}

#[tokio::test]
async fn pending_rename_allows_primary_turn_and_retains_frozen_variables_and_call_identity() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let client = Arc::new(ConcurrentRename::new(false, false));
        let audit = Arc::new(hooks::Recording::default());
        let session = Session::builder_arc(client.clone(), runtime(Arc::new(AtomicUsize::new(0))))
            .auditor(audit.clone())
            .build()
            .unwrap();
        session.start().await.unwrap();
        session
            .state_handle()
            .patch(json!({"marker":"issued"}))
            .await
            .unwrap();
        session.compact("frozen", json!({})).await.unwrap();
        let epoch = session.state().epoch_id;
        let running = session.clone();
        let rename = tokio::spawn(async move { running.rename_with_model("name").await });
        client.entered.notified().await;
        assert!(matches!(
            session.rename_with_model("second").await,
            Err(Error::Busy)
        ));
        let primary = session.submit("primary while rename waits").await.unwrap();
        assert_eq!(primary.content, "Primary result");
        assert_eq!(session.state().epoch_id, epoch + 1);
        session
            .state_handle()
            .patch(json!({"marker":"later"}))
            .await
            .unwrap();
        let before = session.checkpoint().await.unwrap();
        client.release.add_permits(1);
        assert_eq!(rename.await.unwrap().unwrap().content, "Concurrent title");
        let after = session.checkpoint().await.unwrap();
        assert_eq!(
            serde_json::to_value(before).unwrap(),
            serde_json::to_value(after).unwrap()
        );
        {
            let views = client.helper_views.lock().unwrap();
            assert_eq!(views.len(), 2);
            assert_eq!(views[0], views[1]);
            assert!(views[0].0.is_some());
            assert_eq!(views[0].1, Some(json!("issued")));
        }
        {
            let seen = audit.seen.lock().unwrap();
            for row in seen
                .iter()
                .filter(|row| row.subagent.as_deref() == Some("rename"))
            {
                assert_eq!(row.epoch, epoch);
                assert_eq!((row.turn, row.round), (None, None));
                if matches!(
                    row.moment,
                    microcrab_protocol::Moment::RequestPrepared
                        | microcrab_protocol::Moment::RoundCompleted
                ) {
                    assert_eq!(row.call_id.as_deref(), Some("rename-call"));
                } else {
                    assert_eq!(row.call_id, None);
                }
            }
            assert!(seen.iter().any(|row| row.subagent.is_none()
                && row.moment == microcrab_protocol::Moment::RoundCompleted
                && row.call_id.as_deref() == Some("main-call")));
        }
        session.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn foreground_interrupt_does_not_cancel_pending_rename() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let client = Arc::new(ConcurrentRename::new(true, false));
        let session = Session::builder_arc(client.clone(), runtime(Arc::new(AtomicUsize::new(0))))
            .build()
            .unwrap();
        let running = session.clone();
        let rename =
            tokio::spawn(async move { running.rename_with_model("name before start").await });
        client.entered.notified().await;
        let running = session.clone();
        let primary = tokio::spawn(async move { running.submit("primary").await });
        client.main_entered.notified().await;
        session.interrupt();
        assert!(matches!(primary.await.unwrap(), Err(Error::Cancelled)));
        assert_eq!(client.dropped.load(Ordering::SeqCst), 0);
        assert!(!rename.is_finished());
        client.release.add_permits(1);
        assert_eq!(rename.await.unwrap().unwrap().content, "Concurrent title");
        session.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn rename_failure_isolated_from_concurrent_primary_and_next_admission() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let client = Arc::new(ConcurrentRename::new(false, true));
        let session = Session::builder_arc(client.clone(), runtime(Arc::new(AtomicUsize::new(0))))
            .build()
            .unwrap();
        let running = session.clone();
        let rename = tokio::spawn(async move { running.rename_with_model("fail").await });
        client.entered.notified().await;
        session.submit("concurrent primary").await.unwrap();
        let before = session.checkpoint().await.unwrap();
        client.release.add_permits(1);
        assert!(matches!(rename.await.unwrap(), Err(Error::Transport)));
        let after = session.checkpoint().await.unwrap();
        assert_eq!(
            serde_json::to_value(before).unwrap(),
            serde_json::to_value(after).unwrap()
        );
        session.submit("next primary").await.unwrap();
        session.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn close_joins_pending_rename_provider_before_returning() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let client = Arc::new(ConcurrentRename::new(false, false));
        let session = Session::builder_arc(client.clone(), runtime(Arc::new(AtomicUsize::new(0))))
            .build()
            .unwrap();
        let running = session.clone();
        let rename = tokio::spawn(async move { running.rename_with_model("close").await });
        client.entered.notified().await;
        session.close().await.unwrap();
        assert_eq!(client.dropped.load(Ordering::SeqCst), 1);
        assert!(matches!(
            rename.await.unwrap(),
            Err(Error::Cancelled) | Err(Error::Closed)
        ));
        assert!(matches!(
            session.rename_with_model("closed").await,
            Err(Error::Closed)
        ));
    })
    .await
    .unwrap();
}

#[derive(Default)]
struct RenameInjection(hooks::Recording);
impl Auditor for RenameInjection {
    fn is_enabled(&self) -> bool {
        true
    }
    fn publish(&self, moment: microcrab_protocol::Moment, body: microcrab_auditor::AuditBody) {
        self.0.publish(moment, body);
    }
    fn intervene<'a>(
        &'a self,
        moment: microcrab_protocol::Moment,
        ctx: microcrab_auditor::HookContext<'a>,
    ) -> microcrab_auditor::BoxFuture<
        'a,
        Result<microcrab_auditor::Injection, microcrab_auditor::AuditorError>,
    > {
        let helper = ctx.subagent.as_deref() == Some("rename");
        Box::pin(async move {
            let mut injection = self.0.intervene(moment, ctx).await?;
            if helper {
                injection
                    .variables
                    .set
                    .insert("helper_only".into(), json!(true));
            }
            Ok(injection)
        })
    }
}

#[tokio::test]
async fn rename_completion_does_not_consume_primary_transport_witness_or_write_live_variables() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let client = Arc::new(ConcurrentRename::new(true, false));
        let audit = Arc::new(RenameInjection::default());
        let session = Session::builder_arc(client.clone(), runtime(Arc::new(AtomicUsize::new(0))))
            .auditor(audit.clone())
            .build()
            .unwrap();
        let running = session.clone();
        let rename = tokio::spawn(async move { running.rename_with_model("name").await });
        client.entered.notified().await;
        let running = session.clone();
        let primary = tokio::spawn(async move { running.submit("primary").await });
        client.main_entered.notified().await;
        client.release.add_permits(1);
        rename.await.unwrap().unwrap();
        assert!(!primary.is_finished());
        assert!(session.state().variables.get("helper_only").is_none());
        client.main_release.add_permits(1);
        primary.await.unwrap().unwrap();
        {
            let seen = audit.0.seen.lock().unwrap();
            let completed: Vec<_> = seen
                .iter()
                .filter(|row| row.moment == microcrab_protocol::Moment::RoundCompleted)
                .collect();
            assert_eq!(completed.len(), 2);
            assert_eq!(completed[0].subagent.as_deref(), Some("rename"));
            assert_eq!(completed[0].call_id.as_deref(), Some("rename-call"));
            assert_eq!(completed[1].subagent, None);
            assert_eq!(completed[1].call_id.as_deref(), Some("main-call"));
        }
        session.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn rename_is_clean_audited_and_preserves_pending_epoch_and_context() {
    let mut named = answer("  Keep frozen context  ");
    named.usage.input_tokens = Some(11);
    named.usage.output_tokens = Some(4);
    let client = Arc::new(Script::new(
        vec![Ok(answer("first")), Ok(named), Ok(answer("next"))],
        false,
    ));
    let hooks = Arc::new(hooks::Recording::default());
    let journal = Arc::new(JournalLog::default());
    let effects = Arc::new(AtomicUsize::new(0));
    let session = Session::builder_arc(client.clone(), runtime(effects.clone()))
        .auditor(hooks.clone())
        .journal_arc(journal.clone())
        .build()
        .unwrap();
    session.submit("private prior conversation").await.unwrap();
    session
        .compact("frozen summary", json!({"retained": 7}))
        .await
        .unwrap();
    let before = session.checkpoint().await.unwrap();
    let commits = journal.0.lock().unwrap().len();
    hooks.clear();
    let response = session
        .rename_with_model("{{ literal input }}")
        .await
        .unwrap();
    assert_eq!(response.content, "Keep frozen context");
    assert_eq!(response.usage.input_tokens, Some(11));
    assert_eq!(response.usage.output_tokens, Some(4));
    assert_eq!(journal.0.lock().unwrap().len(), commits);
    let after = session.checkpoint().await.unwrap();
    assert_eq!(
        serde_json::to_value(&before).unwrap(),
        serde_json::to_value(&after).unwrap()
    );
    assert!(after.context.pending_epoch);
    let request = client.seen.lock().unwrap()[1].clone();
    assert_eq!(
        (request.epoch, request.turn_id, request.round),
        (before.state.epoch_id, None, None)
    );
    assert_eq!(request.subagent.as_deref(), Some("rename"));
    assert!(request.tools.is_empty());
    assert_eq!(request.items.len(), 1);
    assert_eq!(
        request.system,
        include_str!("../templates/subagents/RENAME.tmpl")
            .replace("{{ user_input }}", "{{ literal input }}")
    );
    assert!(matches!(
        request.items.as_slice(),
        [microcrab_session::context::ContentItem::User { parts, .. }]
            if matches!(parts.as_slice(), [microcrab_protocol::InputPart::Text { text }] if text == "begin")
    ));
    let rendered = serde_json::to_value(render_ast(&request).unwrap()).unwrap();
    assert!(!rendered.to_string().contains("private prior conversation"));
    assert!(!rendered.to_string().contains("frozen summary"));
    assert!(rendered.get("subagent").is_none());
    {
        let observed = hooks.seen.lock().unwrap();
        assert_eq!(
            observed.iter().map(|row| row.moment).collect::<Vec<_>>(),
            vec![
                microcrab_protocol::Moment::SessionCreated,
                microcrab_protocol::Moment::RoundCompleted,
                microcrab_protocol::Moment::SessionTerminated
            ]
        );
        assert!(
            observed
                .iter()
                .all(|row| row.subagent.as_deref() == Some("rename")
                    && (row.turn, row.round) == (None, None))
        );
        let published = hooks.published.lock().unwrap();
        let completed = published
            .iter()
            .filter(|(moment, _)| *moment == microcrab_protocol::Moment::RoundCompleted)
            .collect::<Vec<_>>();
        assert_eq!(
            completed.len(),
            1,
            "the one-shot Session publishes accepted usage exactly once"
        );
        assert_eq!(completed[0].1.subagent.as_deref(), Some("rename"));
        assert_eq!(
            completed[0].1.usage.as_ref().unwrap().input_tokens,
            Some(11)
        );
    }
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    session.submit("continue").await.unwrap();
    let request = client.seen.lock().unwrap()[2].clone();
    assert_eq!(request.epoch, before.state.epoch_id + 1);
    assert_eq!(request.subagent, None);
    session.close().await.unwrap();
}

#[tokio::test]
async fn invalid_rename_responses_preserve_context_and_never_dispatch_tools() {
    let mut tool = answer("Bad title");
    tool.tool_calls.push(ToolCall {
        provider_metadata: None,
        id: "rename-tool".into(),
        name: "effect".into(),
        arguments: json!({}),
    });
    for rejected in [
        Ok(tool),
        Ok(answer("   ")),
        Ok(answer("line one\nline two")),
        Ok(answer("trailing newline\n")),
        Ok(answer("control\tcharacter")),
        Ok(answer("line\u{2028}separator")),
        Ok(answer(&"x".repeat(161))),
        Ok(answer(&"x".repeat(1025))),
        Err(microcrab_transport::Error::Transport),
    ] {
        let client = Arc::new(Script::new(
            vec![Ok(answer("first")), rejected, Ok(answer("next"))],
            false,
        ));
        let effects = Arc::new(AtomicUsize::new(0));
        let session = Session::builder_arc(client, runtime(effects.clone()))
            .limits(Limits {
                ..Limits::default()
            })
            .build()
            .unwrap();
        session.submit("first").await.unwrap();
        let before = session.checkpoint().await.unwrap();
        assert!(session.rename_with_model("name this").await.is_err());
        let after = session.checkpoint().await.unwrap();
        assert_eq!(
            serde_json::to_value(before).unwrap(),
            serde_json::to_value(after).unwrap()
        );
        assert_eq!(effects.load(Ordering::SeqCst), 0);
        session.submit("continue").await.unwrap();
        session.close().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn rename_caller_drop_and_transport_timeout_cancel_only_the_helper() {
    for mode in ["drop", "transport_idle_timeout"] {
        let responses = if mode == "drop" {
            vec![Ok(answer("first")), Ok(answer("next"))]
        } else {
            vec![
                Ok(answer("first")),
                Err(microcrab_transport::Error::ProviderTimeout),
                Ok(answer("next")),
            ]
        };
        let client = Arc::new(Script::new(responses, true));
        let session = Session::builder_arc(client.clone(), runtime(Arc::new(AtomicUsize::new(0))))
            .limits(Limits {
                ..Limits::default()
            })
            .build()
            .unwrap();
        session.submit("first").await.unwrap();
        let before = session.checkpoint().await.unwrap();
        let running = session.clone();
        let worker = tokio::spawn(async move { running.rename_with_model("name this").await });
        tokio::time::timeout(Duration::from_secs(2), client.entered.notified())
            .await
            .unwrap();
        tokio::time::advance(Duration::from_secs(600)).await;
        assert!(
            !worker.is_finished(),
            "legacy turn timing must not end the helper"
        );
        assert!(matches!(
            session.rename_with_model("busy").await,
            Err(Error::Busy)
        ));
        if mode == "drop" {
            worker.abort();
            assert!(worker.await.unwrap_err().is_cancelled());
        } else {
            // The transport, rather than Session's legacy turn limit, reports
            // inactivity. Keep the original failure and unchanged-Context checks.
            client.release.notify_one();
            let error = tokio::time::timeout(Duration::from_secs(2), worker)
                .await
                .unwrap()
                .unwrap()
                .unwrap_err();
            assert!(matches!(error, Error::ProviderTimeout));
        }
        let after = session.checkpoint().await.unwrap();
        assert_eq!(
            serde_json::to_value(before).unwrap(),
            serde_json::to_value(after).unwrap()
        );
        session.submit("continue").await.unwrap();
        session.close().await.unwrap();
    }
}

#[tokio::test]
async fn inherited_compact_preserves_frozen_prefix_and_defers_epoch_turn_allocation() {
    let client = Arc::new(Script::new(
        vec![
            Ok(answer("first")),
            Ok(answer("continuation summary")),
            Ok(answer("next")),
        ],
        false,
    ));
    let journal = Arc::new(JournalLog::default());
    let effects = Arc::new(AtomicUsize::new(0));
    let hooks = Arc::new(hooks::Recording::default());
    let session = Session::builder_arc(client.clone(), runtime(effects.clone()))
        .auditor(hooks.clone())
        .journal_arc(journal)
        .build()
        .unwrap();
    session.submit("remember this").await.unwrap();
    let before = session.checkpoint().await.unwrap();
    hooks.clear();
    let response = session
        .compact_with_model("Keep exact paths.", json!({"retained":7}))
        .await
        .unwrap();
    assert_eq!(response.content, "continuation summary");
    assert!(
        hooks.moments()
            == vec![
                microcrab_protocol::Moment::SessionCreated,
                microcrab_protocol::Moment::RoundCompleted,
                microcrab_protocol::Moment::SessionTerminated
            ],
        "one-shot lifecycle is operation-local and does not admit a parent input, turn or epoch"
    );
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    let compacted = session.checkpoint().await.unwrap();
    let marker = compacted.context.compaction.as_ref().unwrap();
    assert_eq!(marker.summary, "continuation summary");
    let restored =
        microcrab_session::context::Context::from_snapshot(compacted.context.clone()).unwrap();
    assert_eq!(restored.compaction(), Some(marker));
    let mut forged = compacted.context.clone();
    forged.compaction.as_mut().unwrap().summary = "unrelated user message".into();
    assert!(microcrab_session::context::Context::from_snapshot(forged).is_err());
    let mut legacy = compacted.context.clone();
    legacy.compaction = None;
    assert!(
        microcrab_session::context::Context::from_snapshot(legacy)
            .unwrap()
            .compaction()
            .is_none()
    );
    assert_eq!(compacted.state.epoch_id, before.state.epoch_id);
    assert_eq!(
        serde_json::to_value(&compacted.state).unwrap()["last_turn_id"],
        1
    );
    assert!(compacted.context.pending_epoch);
    assert_eq!(session.state_handle().extract("retained"), Some(json!(7)));
    let inherited = client.seen.lock().unwrap()[1].clone();
    let mut issuer = microcrab_session::context::Context::from_snapshot(before.context.clone())
        .unwrap()
        .content(before.state.limits.max_output_tokens)
        .unwrap();
    // The functional request has no new position: its preserved context is the
    // settled issuer, not the next user turn's state projection.
    issuer.turn_id = inherited.turn_id;
    issuer.round = inherited.round;
    let original_ast = render_ast(&issuer).unwrap();
    let functional_ast = render_ast(&inherited).unwrap();
    assert_eq!(functional_ast.tools, original_ast.tools);
    assert_eq!(
        &functional_ast.messages[..original_ast.messages.len()],
        original_ast.messages.as_slice()
    );
    assert_eq!(
        functional_ast.messages.len(),
        original_ast.messages.len() + 1
    );
    assert_eq!(inherited.items.len(), issuer.items.len() + 1);
    assert_eq!(inherited.subagent.as_deref(), Some("compact"));
    assert_eq!(
        hooks.seen.lock().unwrap()[0].subagent.as_deref(),
        Some("compact")
    );
    assert_eq!(
        hooks.published.lock().unwrap()[0].1.subagent.as_deref(),
        Some("compact")
    );
    assert_eq!(
        (inherited.epoch, inherited.turn_id, inherited.round),
        (0, None, None)
    );
    session.submit("continue").await.unwrap();
    let next = client.seen.lock().unwrap()[2].clone();
    assert_eq!(
        (next.epoch, next.turn_id, next.round),
        (1, Some(1), Some(1))
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn rejected_results_do_not_change_context_variables_or_commit_history() {
    let mut tool = answer("not a summary");
    tool.tool_calls.push(ToolCall {
        provider_metadata: None,
        id: "functional-call".into(),
        name: "effect".into(),
        arguments: json!({}),
    });
    for rejected in [
        Ok(tool),
        Ok(answer(" \n")),
        Err(microcrab_transport::Error::Transport),
    ] {
        let client = Arc::new(Script::new(
            vec![Ok(answer("first")), rejected, Ok(answer("still usable"))],
            false,
        ));
        let journal = Arc::new(JournalLog::default());
        let effects = Arc::new(AtomicUsize::new(0));
        let limits = Limits {
            ..Limits::default()
        };
        let hooks = Arc::new(hooks::Recording::default());
        let session = Session::builder_arc(client, runtime(effects.clone()))
            .auditor(hooks.clone())
            .limits(limits)
            .journal_arc(journal.clone())
            .build()
            .unwrap();
        session.submit("first").await.unwrap();
        let before = session.checkpoint().await.unwrap();
        let commits = journal.0.lock().unwrap().len();
        hooks.clear();
        assert!(
            session
                .compact_with_model("summarize", json!({"must_not_apply":true}))
                .await
                .is_err()
        );
        assert_eq!(journal.0.lock().unwrap().len(), commits);
        assert!(hooks.moments().iter().all(|moment| matches!(
            moment,
            microcrab_protocol::Moment::SessionCreated
                | microcrab_protocol::Moment::RoundCompleted
                | microcrab_protocol::Moment::SessionTerminated
        )));
        let after = session.checkpoint().await.unwrap();
        assert_eq!(
            serde_json::to_value(&before).unwrap(),
            serde_json::to_value(after).unwrap()
        );
        assert_eq!(effects.load(Ordering::SeqCst), 0);
        session.submit("continue after rejection").await.unwrap();
        session.close().await.unwrap();
    }
}

#[tokio::test]
async fn issued_snapshot_is_immutable_while_variable_pump_remains_available() {
    let client = Arc::new(Script::new(
        vec![Ok(answer("first")), Ok(answer("summary"))],
        true,
    ));
    let session = Session::builder_arc(client.clone(), runtime(Arc::new(AtomicUsize::new(0))))
        .build()
        .unwrap();
    session.submit("first").await.unwrap();
    let running = session.clone();
    let worker =
        tokio::spawn(async move { running.compact_with_model("summarize", json!({})).await });
    tokio::time::timeout(Duration::from_secs(2), client.entered.notified())
        .await
        .unwrap();
    let issued = serde_json::to_value(&client.seen.lock().unwrap()[1]).unwrap();
    assert!(matches!(session.submit("busy").await, Err(Error::Busy)));
    session
        .state_handle()
        .inject("changed_after_issuance", json!(true))
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(&client.seen.lock().unwrap()[1]).unwrap(),
        issued
    );
    client.release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        session.state_handle().extract("changed_after_issuance"),
        Some(json!(true))
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn cancelled_functional_future_releases_admission_and_preserves_usable_context() {
    let client = Arc::new(Script::new(
        vec![Ok(answer("first")), Ok(answer("next main turn"))],
        true,
    ));
    let session = Session::builder_arc(client.clone(), runtime(Arc::new(AtomicUsize::new(0))))
        .build()
        .unwrap();
    session.submit("first").await.unwrap();
    let before = session.checkpoint().await.unwrap();
    let running = session.clone();
    let worker =
        tokio::spawn(async move { running.compact_with_model("summarize", json!({})).await });
    tokio::time::timeout(Duration::from_secs(2), client.entered.notified())
        .await
        .unwrap();
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    let after = session.checkpoint().await.unwrap();
    assert_eq!(
        serde_json::to_value(before).unwrap(),
        serde_json::to_value(after).unwrap()
    );
    session
        .submit("continue after caller cancellation")
        .await
        .unwrap();
    session.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn interrupt_and_transport_timeout_leave_functional_context_usable() {
    for interrupt in [true, false] {
        let responses = if interrupt {
            vec![Ok(answer("first")), Ok(answer("next"))]
        } else {
            vec![
                Ok(answer("first")),
                Err(microcrab_transport::Error::ProviderTimeout),
                Ok(answer("next")),
            ]
        };
        let client = Arc::new(Script::new(responses, true));
        let limits = Limits {
            ..Limits::default()
        };
        let session = Session::builder_arc(client.clone(), runtime(Arc::new(AtomicUsize::new(0))))
            .limits(limits)
            .build()
            .unwrap();
        session.submit("first").await.unwrap();
        let before = session.checkpoint().await.unwrap();
        let running = session.clone();
        let worker =
            tokio::spawn(async move { running.compact_with_model("summarize", json!({})).await });
        tokio::time::timeout(Duration::from_secs(2), client.entered.notified())
            .await
            .unwrap();
        tokio::time::advance(Duration::from_secs(600)).await;
        assert!(
            !worker.is_finished(),
            "legacy turn timing must not end compact"
        );
        if interrupt {
            session.interrupt();
        } else {
            // Client-owned inactivity, not elapsed turn time, ends the request.
            client.release.notify_one();
        }
        let error = tokio::time::timeout(Duration::from_secs(2), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(if interrupt {
            matches!(error, Error::Cancelled)
        } else {
            matches!(error, Error::ProviderTimeout)
        });
        assert_eq!(
            serde_json::to_value(before).unwrap(),
            serde_json::to_value(session.checkpoint().await.unwrap()).unwrap(),
        );
        session.submit("still available").await.unwrap();
        session.close().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn functional_helpers_can_complete_after_the_legacy_turn_limit() {
    for rename in [true, false] {
        let client = Arc::new(Script::new(
            vec![
                Ok(answer("first")),
                Ok(answer("helper result")),
                Ok(answer("next")),
            ],
            true,
        ));
        let effects = Arc::new(AtomicUsize::new(0));
        let session = Session::builder_arc(client.clone(), runtime(effects.clone()))
            .limits(Limits {
                ..Default::default()
            })
            .build()
            .unwrap();
        session.submit("first").await.unwrap();
        let before = session.checkpoint().await.unwrap();
        let running = session.clone();
        let worker = tokio::spawn(async move {
            if rename {
                running.rename_with_model("name this").await
            } else {
                running.compact_with_model("summarize", json!({})).await
            }
        });
        tokio::time::timeout(Duration::from_secs(2), client.entered.notified())
            .await
            .unwrap();
        tokio::time::advance(Duration::from_secs(600)).await;
        assert!(!worker.is_finished());
        client.release.notify_one();
        assert_eq!(worker.await.unwrap().unwrap().content, "helper result");
        let after = session.checkpoint().await.unwrap();
        if rename {
            assert_eq!(
                serde_json::to_value(before).unwrap(),
                serde_json::to_value(after).unwrap()
            );
        } else {
            assert_eq!(
                after.context.compaction.as_ref().unwrap().summary,
                "helper result"
            );
            assert_eq!(after.state.epoch_id, before.state.epoch_id);
        }
        assert_eq!(effects.load(Ordering::SeqCst), 0);
        session.submit("continue").await.unwrap();
        session.close().await.unwrap();
    }
}
