use microcrab_auditor::Auditor;
use microcrab_protocol::{Response, Usage};
use microcrab_session::context::Content;
use microcrab_session::runtime::Runtime;
use microcrab_session::runtime::tool::{DenyAll, Tool, ToolContext};
use microcrab_session::{Client, Error, Session, SessionState};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

struct Answer;
#[async_trait::async_trait]
impl Client for Answer {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        Ok(Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: vec![],
            usage: Usage::default(),
        })
    }
}

struct WaitThenAnswer {
    calls: std::sync::atomic::AtomicUsize,
    seen: std::sync::Mutex<Vec<serde_json::Value>>,
    entered: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl Client for WaitThenAnswer {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        cancellation: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.seen
            .lock()
            .unwrap()
            .push(serde_json::to_value(content).unwrap());
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            self.entered.notify_one();
            cancellation.cancelled().await;
            return Err(microcrab_transport::Error::Cancelled);
        }
        Ok(Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: vec![],
            usage: Usage::default(),
        })
    }
}

fn session<C: Client>(client: Arc<C>) -> Session {
    let runtime = Runtime::new(vec![], Arc::new(DenyAll)).unwrap();
    Session::builder_arc(client, runtime).build().unwrap()
}

#[tokio::test]
async fn submit_compact_and_close_are_user_commands() {
    let session = session(Arc::new(Answer));
    let mut input = microcrab_session::SessionInput::text("inspect");
    input.variables.set = json!({"workspace":"project"}).as_object().unwrap().clone();
    let outcome = session.submit(input).await.unwrap();
    assert_eq!(outcome.content, "done");
    assert_eq!(session.state().turn_id, None);
    assert_eq!(
        session.state().variables.as_value(),
        json!({"workspace": "project"})
    );
    session
        .compact("continue", json!({"workspace": "next"}))
        .await
        .unwrap();
    assert_eq!(session.state().epoch_id, 0);
    assert!(session.checkpoint().await.unwrap().context.pending_epoch);
    assert_eq!(
        session.state().variables.as_value(),
        json!({"workspace": "next"})
    );
    session.close().await.unwrap();
    assert!(matches!(session.submit("again").await, Err(Error::Closed)));
}

#[tokio::test]
async fn interrupt_cancels_the_admitted_turn_without_faulting_the_session() {
    let client = Arc::new(WaitThenAnswer {
        calls: std::sync::atomic::AtomicUsize::new(0),
        seen: std::sync::Mutex::new(vec![]),
        entered: tokio::sync::Notify::new(),
    });
    let session = session(client.clone());
    let running = session.clone();
    let submitted = tokio::spawn(async move { running.submit("inspect").await });
    tokio::time::timeout(Duration::from_secs(2), client.entered.notified())
        .await
        .unwrap();
    let turn = session.state().turn_id;
    assert_eq!(turn, Some(1));
    session.interrupt();
    assert!(matches!(submitted.await.unwrap(), Err(Error::Cancelled)));
    assert_eq!(session.state().turn_id, turn);
    assert!(session.retry_ready());
    assert!(session.has_continuation());
    assert_eq!(session.resume().await.unwrap().content, "done");
    {
        let requests = client.seen.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0], requests[1]);
    }
    assert_eq!(
        session
            .messages()
            .iter()
            .filter(|item| matches!(item, microcrab_session::context::ContentItem::User { .. }))
            .count(),
        1
    );
    assert_eq!(
        serde_json::to_value(session.state()).unwrap()["last_turn_id"],
        1
    );
    assert_eq!(session.state().turn_id, None);
    assert!(!session.retry_ready());
    session.close().await.unwrap();
}

struct DelegateClient {
    seen: std::sync::Mutex<Vec<Content>>,
}
#[async_trait::async_trait]
impl Client for DelegateClient {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let mut seen = self.seen.lock().unwrap();
        seen.push(content.clone());
        Ok(Response {
            provider_metadata: None,
            content: if seen.len() == 1 { "working" } else { "done" }.into(),
            tool_calls: if seen.len() == 1 {
                vec![microcrab_protocol::ToolCall {
                    provider_metadata: None,
                    id: "delegate-call".into(),
                    name: "delegate".into(),
                    arguments: json!({"mode":"delegate","target":"review","input":"look"}),
                }]
            } else {
                vec![]
            },
            usage: Usage::default(),
        })
    }
}

#[derive(Default)]
struct ChildExecutor {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    calls: std::sync::Mutex<Vec<(String, String, SessionState)>>,
}
#[async_trait::async_trait]
impl microcrab_session::runtime::tool::delegate::DelegateExecutor for ChildExecutor {
    async fn execute(
        &self,
        target: &str,
        input: &str,
        _mode: Option<&str>,
        _context_mode: &str,
        _timeout_ms: Option<u64>,
        invocation: ToolContext,
    ) -> Result<String, Error> {
        self.calls
            .lock()
            .unwrap()
            .push((target.into(), input.into(), invocation.state.snapshot()));
        self.entered.notify_one();
        tokio::select! {
            _ = invocation.cancellation.cancelled() => Err(Error::Cancelled),
            _ = self.release.notified() => Ok("actual child review output".into()),
        }
    }
}

fn delegate_catalog() -> Vec<microcrab_agent_types::AgentInfo> {
    vec![microcrab_agent_types::AgentInfo {
        name: "review".into(),
        modes: vec![microcrab_agent_types::AgentMode {
            description: String::new(),
            prompt: String::new(),
            name: "delegate".into(),
            role: microcrab_agent_types::AgentRole::SubAgent,
            permissions: vec![],
        }],
        tools: vec![],
        variables: Default::default(),
    }]
}
fn delegate_session(
    executor: Option<Arc<ChildExecutor>>,
    allowed: bool,
) -> (Session, Arc<DelegateClient>) {
    use microcrab_session::runtime::tool::AllowTools;
    let client = Arc::new(DelegateClient {
        seen: std::sync::Mutex::new(vec![]),
    });
    let mut runtime = Runtime::new(
        vec![microcrab_session::runtime::ToolRegistration::Builtin(
            microcrab_session::runtime::BuiltinTool::Delegate,
        )],
        Arc::new(AllowTools::new(if allowed {
            vec!["delegate"]
        } else {
            vec![]
        })),
    )
    .unwrap();
    if let Some(executor) = executor {
        runtime = runtime.with_delegate_executor(executor);
    }
    (
        Session::builder_arc(client.clone(), runtime)
            .catalog(delegate_catalog())
            .permissions(microcrab_permission::PermissionConfig {
                overrides: if allowed {
                    vec![]
                } else {
                    vec![
                        serde_json::from_value(
                            json!({"tool":{"kind":"exact","value":"delegate"},"action":"deny"}),
                        )
                        .unwrap(),
                    ]
                },
                ..Default::default()
            })
            .build()
            .unwrap(),
        client,
    )
}

#[tokio::test]
async fn delegate_awaits_the_configured_executor_and_returns_its_actual_output() {
    let executor = Arc::new(ChildExecutor::default());
    let (session, client) = delegate_session(Some(executor.clone()), true);
    let running = session.clone();
    let submitted = tokio::spawn(async move { running.submit("inspect").await });
    tokio::time::timeout(Duration::from_secs(2), executor.entered.notified())
        .await
        .unwrap();
    assert!(!submitted.is_finished());
    assert_eq!(client.seen.lock().unwrap().len(), 1);
    {
        let calls = executor.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!((&*calls[0].0, &*calls[0].1), ("review", "look"));
        assert_eq!(calls[0].2.round_id, Some(1));
        assert!(calls[0].2.turn_id.is_some());
    }
    executor.release.notify_one();
    let outcome = tokio::time::timeout(Duration::from_secs(2), submitted)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(outcome.content, "done");
    assert_eq!(outcome.rounds, 2);
    assert!(
        client.seen.lock().unwrap()[1]
            .items
            .iter()
            .any(|item| matches!(
                item,
                microcrab_session::context::ContentItem::Tool { content, .. }
                    if content == "actual child review output"
            ))
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn delegate_permission_denial_prevents_executor_dispatch() {
    let executor = Arc::new(ChildExecutor::default());
    let (session, client) = delegate_session(Some(executor.clone()), false);
    session.submit("inspect").await.unwrap();
    assert!(executor.calls.lock().unwrap().is_empty());
    let seen = client.seen.lock().unwrap();
    let result = seen[1]
        .items
        .iter()
        .find_map(|item| match item {
            microcrab_session::context::ContentItem::Tool { call_id, content }
                if call_id == "delegate-call" =>
            {
                Some(serde_json::from_str::<serde_json::Value>(content).unwrap())
            }
            _ => None,
        })
        .expect("next model request contains the delegate denial");
    // Denied delegate authority removes the target from the eligible catalog
    // before ordinary invocation admission, so retain that concrete cause.
    assert_eq!(
        result,
        json!({
            "error": "delegate target has no authorized eligible modes",
            "diagnostic": {
                "code": "permission_denied",
                "layer": "runtime",
                "phase": "authorization",
                "effects": "unknown",
                "reason": "delegate target has no authorized eligible modes",
            },
        })
    );
    drop(seen);
    session.close().await.unwrap();
}

#[tokio::test]
async fn delegate_without_an_executor_reports_failure_instead_of_an_assignment() {
    let tool = microcrab_session::runtime::tool::delegate::Delegate;
    let Err(error) = tool
        .execute(
            json!({"mode":"delegate","target":"review","input":"look"}),
            ToolContext::detached(
                SessionState::new(Default::default()),
                CancellationToken::new(),
            ),
        )
        .await
    else {
        panic!("delegate requires an executor");
    };
    assert!(matches!(error, Error::Tool(message) if message.contains("executor")));

    let (session, client) = delegate_session(None, true);
    session.submit("inspect").await.unwrap();
    {
        let seen = client.seen.lock().unwrap();
        let output = seen[1]
            .items
            .iter()
            .find_map(|item| match item {
                microcrab_session::context::ContentItem::Tool { content, .. } => Some(content),
                _ => None,
            })
            .unwrap();
        let failed: serde_json::Value = serde_json::from_str(output).unwrap();
        assert_eq!(failed["diagnostic"]["code"], "tool_failed");
        assert_eq!(failed["diagnostic"]["layer"], "runtime");
        assert_eq!(failed["diagnostic"]["phase"], "unknown");
        assert_eq!(failed["diagnostic"]["effects"], "unknown");
        assert!(!output.contains("assigned"));
    }
    session.close().await.unwrap();
}

struct DeadlineDelegateClient {
    timeout: Option<u64>,
    calls: std::sync::atomic::AtomicUsize,
    saw_settled_failure: std::sync::atomic::AtomicBool,
}
#[async_trait::async_trait]
impl Client for DeadlineDelegateClient {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_session::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let first = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0;
        if content.items.iter().any(|item| {
            let microcrab_session::context::ContentItem::Tool { content, .. } = item else {
                return false;
            };
            serde_json::from_str::<serde_json::Value>(content)
                .ok()
                .is_some_and(|value| {
                    value["diagnostic"]["code"] == "tool_failed"
                        && value["diagnostic"]["effects"] == "unknown"
                })
        }) {
            self.saw_settled_failure
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        let mut arguments = json!({"target":"review", "input":"inspect"});
        if let Some(timeout) = self.timeout {
            arguments["timeout_ms"] = json!(timeout);
        }
        Ok(Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: if first {
                vec![microcrab_protocol::ToolCall {
                    provider_metadata: None,
                    id: "bounded-child".into(),
                    name: "delegate".into(),
                    arguments,
                }]
            } else {
                vec![]
            },
            usage: Usage::default(),
        })
    }
}
struct DeadlineDelegate {
    requested: Option<u64>,
    observed: std::sync::atomic::AtomicU64,
    cleaned: std::sync::atomic::AtomicBool,
}
#[async_trait::async_trait]
impl microcrab_session::DelegateExecutor for DeadlineDelegate {
    async fn execute(
        &self,
        _: &str,
        _: &str,
        _: Option<&str>,
        _: &str,
        timeout: Option<u64>,
        invocation: ToolContext,
    ) -> Result<String, Error> {
        assert_eq!(
            timeout, self.requested,
            "only an explicit caller deadline belongs to the child owner"
        );
        let timeout = timeout.unwrap_or(0);
        self.observed
            .store(timeout, std::sync::atomic::Ordering::SeqCst);
        // Both paths outlast the ordinary 1ms tool budget in this regression.
        let result = tokio::select! {
            _ = invocation.cancellation.cancelled() => Err(Error::Cancelled),
            _ = tokio::time::sleep(Duration::from_millis(self.requested.unwrap_or(30))) => {
                if self.requested.is_some() {
                    Err(Error::Tool(format!("delegate timed out after {timeout}ms; token=private-delegate-secret").into()))
                } else {
                    Ok("settled child result".into())
                }
            }
        };
        // Cleanup is deliberately later than the requested execution deadline.
        // Runtime must retain the owner until its acknowledgement arrives.
        tokio::time::sleep(Duration::from_millis(20)).await;
        self.cleaned
            .store(true, std::sync::atomic::Ordering::SeqCst);
        result
    }
}

#[tokio::test]
async fn runtime_delegate_keeps_real_owner_and_only_explicit_child_deadlines() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for requested in [Some(40), None] {
            let client = Arc::new(DeadlineDelegateClient {
                timeout: requested,
                calls: std::sync::atomic::AtomicUsize::new(0),
                saw_settled_failure: std::sync::atomic::AtomicBool::new(false),
            });
            let executor = Arc::new(DeadlineDelegate {
                requested,
                observed: std::sync::atomic::AtomicU64::new(0),
                cleaned: std::sync::atomic::AtomicBool::new(false),
            });
            let runtime = Runtime::new(
                vec![microcrab_session::runtime::ToolRegistration::Builtin(
                    microcrab_session::runtime::BuiltinTool::Delegate,
                )],
                Arc::new(microcrab_session::runtime::tool::AllowTools::new([
                    "delegate",
                ])),
            )
            .unwrap()
            .with_delegate_executor(executor.clone());
            let limits = microcrab_session::Limits {
                ..Default::default()
            };
            let session = Session::builder_arc(client.clone(), runtime)
                .catalog(delegate_catalog())
                .limits(limits)
                .build()
                .unwrap();
            assert_eq!(session.submit("inspect").await.unwrap().content, "done");
            let mut changes = session.executions().subscribe();
            while session.executions().has_pending() {
                changes.changed().await.unwrap();
            }
            assert!(executor.cleaned.load(std::sync::atomic::Ordering::SeqCst));
            assert_eq!(
                executor.observed.load(std::sync::atomic::Ordering::SeqCst),
                requested.unwrap_or(0)
            );
            let task = session.executions().snapshot().pop().unwrap();
            if requested.is_some() {
                assert_eq!(
                    task.status,
                    microcrab_session::runtime::execution::ExecutionStatus::Failed
                );
                assert!(
                    task.failure.is_none(),
                    "untyped legacy adapter errors cannot invent a tool-reported failure"
                );
                let diagnostic = task.diagnostic.as_ref().expect("safe task diagnostic");
                assert_eq!(
                    diagnostic.code,
                    microcrab_session::observation::diagnostics::DiagnosticCode::ToolFailed
                );
                assert_eq!(
                    diagnostic.layer,
                    microcrab_session::observation::diagnostics::DiagnosticLayer::Runtime
                );
                assert_eq!(
                    diagnostic.phase,
                    microcrab_session::observation::diagnostics::DiagnosticPhase::Unknown
                );
                assert_eq!(
                    diagnostic.effects,
                    microcrab_session::observation::diagnostics::EffectKnowledge::Unknown
                );
                assert!(
                    diagnostic
                        .summary()
                        .contains("delegate timed out after 40ms")
                );
                assert!(
                    !serde_json::to_string(&session.messages())
                        .unwrap()
                        .contains("private-delegate-secret")
                );
            } else {
                assert!(task.result.is_none());
                assert_eq!(
                    session
                        .read_execution_output(task.execution_id, 0, 65536)
                        .unwrap()
                        .chunks
                        .into_iter()
                        .map(|chunk| chunk.content)
                        .collect::<String>(),
                    "settled child result"
                );
            }
            assert_eq!(
                session
                    .submit("continue after settled child")
                    .await
                    .unwrap()
                    .content,
                "done"
            );
            session.close().await.unwrap();
        }
    })
    .await
    .expect("delegate result and cleanup must leave the parent usable");
}
