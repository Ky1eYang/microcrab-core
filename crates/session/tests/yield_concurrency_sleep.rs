//! Session batch receipts and cancellable waits use actual LocalHost processes.
//! Stdin is the process lifetime barrier; watchdogs only bound broken tests.
use async_trait::async_trait;
use microcrab_auditor::{Auditor, VariableView};
use microcrab_host_api::ShellRequest;
use microcrab_host_local::{LocalHost, ShellLimits};
use microcrab_protocol::{Response, ToolCall, ToolDefinition};
use microcrab_session::runtime::execution::{ExecutionId, ToolOutput};
use microcrab_session::{
    Client, Error, Limits, Session,
    context::{Content, ContentItem},
    runtime::{
        BuiltinTool, Runtime, ToolRegistration,
        tool::{AllowTools, Tool, ToolContext, sleep::Sleep},
    },
};
use serde_json::{Value, json};
use std::{
    future::{Future, poll_fn},
    sync::{Arc, Mutex},
    task::Poll,
    time::Duration,
};
use tokio_util::sync::CancellationToken;

struct Script(Mutex<Option<Vec<ToolCall>>>);
#[async_trait]
impl Client for Script {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        Ok(Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: self.0.lock().unwrap().take().unwrap_or_default(),
            usage: Default::default(),
        })
    }
}
struct Launch;
#[async_trait]
impl Tool for Launch {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "launch_fixture".into(),
            description: "Launch a real process held on stdin".into(),
            parameters: json!({"type":"object","additionalProperties":false}),
        }
    }
    async fn execute(&self, _: Value, context: ToolContext) -> Result<ToolOutput, Error> {
        let handle = context
            .processes
            .launch_shell(
                &context.tool,
                &context.call_id,
                ShellRequest {
                    command: waiting().into(),
                    cwd: None,
                    output_bytes: context
                        .state
                        .snapshot()
                        .limits
                        .output_buffer_bytes
                        .min(ShellLimits::default().max_output_bytes),
                },
                context.cancellation.clone(),
            )
            .await?;
        Ok(ToolOutput::Buffered(
            context
                .adopt_process_output(handle, vec!["stdout".into(), "stderr".into()], true)
                .await?
                .with_initial_output(true),
        ))
    }
}
fn waiting() -> &'static str {
    if cfg!(windows) {
        "$line = [Console]::ReadLine(); [Console]::Out.WriteLine($line)"
    } else {
        "IFS= read -r line; printf '%s\\n' \"$line\""
    }
}
fn call(id: &str, name: &str, arguments: Value) -> ToolCall {
    ToolCall {
        provider_metadata: None,
        id: id.into(),
        name: name.into(),
        arguments,
    }
}
fn fixture(calls: Vec<ToolCall>, limits: Limits) -> (tempfile::TempDir, Session) {
    let root = tempfile::tempdir().unwrap();
    let host = Arc::new(LocalHost::new(root.path(), ShellLimits::default()).unwrap());
    let runtime = Runtime::new(
        vec![
            ToolRegistration::External(Arc::new(Launch)),
            ToolRegistration::Builtin(BuiltinTool::Sleep),
        ],
        Arc::new(AllowTools::new(["launch_fixture", "sleep"])),
    )
    .unwrap();
    let session = Session::builder(Script(Mutex::new(Some(calls))), runtime)
        .session_id("yield-concurrency")
        .host(host)
        .limits(limits)
        .build()
        .unwrap();
    (root, session)
}
fn sleep_context(session: &Session, token: CancellationToken) -> ToolContext {
    ToolContext::new(
        session.state_handle(),
        session.process_control(),
        token,
        "wait-fixture".into(),
        "sleep".into(),
        "yield-concurrency".into(),
        "fixture".into(),
        None,
    )
}
fn final_text(output: ToolOutput) -> String {
    match output {
        ToolOutput::Final(output) => {
            assert!(output.failure.is_none());
            output.content
        }
        ToolOutput::Buffered(_) => panic!("expected Final wait result"),
    }
}
fn channel(page: &Value, name: &str) -> String {
    page["chunks"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|chunk| chunk["channel"] == name)
        .map(|chunk| chunk["content"].as_str().unwrap())
        .collect()
}
async fn action(session: &Session, address: &str, action: &str, args: Value) -> Value {
    let (owner, id) = address
        .rsplit_once('/')
        .expect("canonical execution address");
    assert_eq!(
        owner,
        session.state_view().unwrap()["session"]["id"]
            .as_str()
            .unwrap_or(owner)
    );
    let id = ExecutionId(id.parse().unwrap());
    let registry = session.executions();
    match action {
        "status" => serde_json::to_value(registry.get(id).unwrap()).unwrap(),
        "read" => {
            serde_json::to_value(session.read_execution_output(id, 0, 65536).unwrap()).unwrap()
        }
        "write" => {
            registry
                .write(
                    id,
                    args["data"].as_str().unwrap().into(),
                    args["eof"].as_bool().unwrap_or(false),
                    CancellationToken::new(),
                )
                .await
                .unwrap();
            json!({"accepted":true})
        }
        "cancel" => {
            session.cancel_execution(id).await.unwrap();
            serde_json::to_value(registry.get(id).unwrap()).unwrap()
        }
        _ => panic!("unsupported fixture action"),
    }
}
/// Poll the exact wait once. No notification, time delay, or task scheduling is
/// allowed to stand in for proof that the wait subscribed and remains pending.
async fn pending<F: Future>(future: std::pin::Pin<&mut F>) {
    let mut future = future;
    poll_fn(|cx| {
        assert!(
            future.as_mut().poll(cx).is_pending(),
            "wait must remain pending at the barrier"
        );
        Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn batch_retains_two_shell_jobs_and_minus_one_wakes_without_consuming_output() {
    let (_root, session) = fixture(
        vec![
            call("launch-a", "launch_fixture", json!({})),
            call("launch-b", "launch_fixture", json!({})),
        ],
        Limits::default(),
    );
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        session.submit("launch both processes"),
    )
    .await
    .expect("batch receipts must yield")
    .unwrap();
    assert_eq!(outcome.content, "done");
    let receipts: Vec<Value> = session
        .messages()
        .iter()
        .filter_map(|item| match item {
            ContentItem::Tool { content, .. } => Some(serde_json::from_str(content).unwrap()),
            _ => None,
        })
        .collect();
    assert_eq!(receipts.len(), 2);
    for receipt in &receipts {
        assert_eq!(receipt["status"], "running");
        assert!(receipt["execution_id"].as_u64().is_some_and(|id| id > 0));
        assert!(receipt.get("pid").is_none());
        assert!(
            receipt.get("exit_code").is_none(),
            "running receipts never claim an exit code"
        );
        assert_eq!(
            action(
                &session,
                receipt["id"].as_str().unwrap(),
                "status",
                json!({})
            )
            .await["status"],
            "running"
        );
    }
    assert_ne!(receipts[0]["execution_id"], receipts[1]["execution_id"]);
    assert_ne!(receipts[0]["id"], receipts[1]["id"]);
    let first = receipts[0]["id"].as_str().unwrap();
    let second = receipts[1]["id"].as_str().unwrap();
    let context = sleep_context(&session, CancellationToken::new());
    let wait = Sleep.execute(
        json!({"time_ms":-1,"reason":"observe retained shell"}),
        context,
    );
    tokio::pin!(wait);
    pending(wait.as_mut()).await;
    action(
        &session,
        first,
        "write",
        json!({"data":"retained-output\n","eof":true}),
    )
    .await;
    let result: Value = serde_json::from_str(&final_text(
        tokio::time::timeout(Duration::from_secs(10), wait)
            .await
            .expect("a real execution change must wake -1")
            .unwrap(),
    ))
    .unwrap();
    assert_eq!(result["success"], true);
    assert_eq!(result["reason"], "observe retained shell");
    assert!(result.get("stdout").is_none());
    assert!(result.get("stderr").is_none());
    assert_eq!(
        action(&session, second, "status", json!({})).await["status"],
        "running"
    );
    let executions = session.executions();
    let mut changed = executions.subscribe();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if action(&session, first, "status", json!({})).await["status"] != "running" {
                break;
            }
            changed.changed().await.unwrap();
        }
    })
    .await
    .expect("released stdin process must finish its execution");
    let output = action(&session, first, "read", json!({})).await;
    assert!(channel(&output, "stdout").contains("retained-output"));
    let again = action(&session, first, "read", json!({})).await;
    assert_eq!(again["chunks"], output["chunks"]);
    assert_eq!(again["cursor"], output["cursor"]);
    session.close().await.unwrap();
}

#[tokio::test]
async fn sleep_first_batch_admits_both_real_jobs_before_waiting_and_interrupts_cleanly() {
    use microcrab_auditor::ChannelAuditor;
    use microcrab_protocol::Moment;
    let root = tempfile::tempdir().unwrap();
    let host = Arc::new(LocalHost::new(root.path(), ShellLimits::default()).unwrap());
    let (sender, mut records) = tokio::sync::mpsc::unbounded_channel();
    let runtime = Runtime::new(
        vec![
            ToolRegistration::External(Arc::new(Launch)),
            ToolRegistration::Builtin(BuiltinTool::Sleep),
        ],
        Arc::new(AllowTools::new(["launch_fixture", "sleep"])),
    )
    .unwrap();
    let session = Session::builder(
        Script(Mutex::new(Some(vec![
            call("batch-wait", "sleep", json!({"time_ms":-1})),
            call("batch-launch-a", "launch_fixture", json!({})),
            call("batch-launch-b", "launch_fixture", json!({})),
        ]))),
        runtime,
    )
    .host(host)
    .auditor(Arc::new(ChannelAuditor::from_sender(sender)))
    .build()
    .unwrap();
    let worker_session = session.clone();
    let worker = tokio::spawn(async move {
        worker_session
            .submit("launch siblings before waiting")
            .await
    });
    let preceding_results = tokio::time::timeout(Duration::from_secs(10), async {
        let mut launches = 0;
        while let Some(record) = records.recv().await {
            if record.event == Moment::ToolResult
                && record
                    .tool_call_id
                    .as_deref()
                    .is_some_and(|id| id.starts_with("batch-launch-"))
            {
                launches += 1;
            }
            if record.event == Moment::ToolCall
                && record.tool_call_id.as_deref() == Some("batch-wait")
            {
                return launches;
            }
        }
        panic!("audit closed before batch wait");
    })
    .await;
    let executions: Vec<_> = session
        .execution_snapshots()
        .into_iter()
        .filter(|row| row.tool == "launch_fixture")
        .collect();
    session.interrupt();
    let outcome = tokio::time::timeout(Duration::from_secs(5), worker).await;
    session.close().await.unwrap();
    assert_eq!(
        preceding_results.expect("sleep-first batch must reach deferred wait"),
        2
    );
    assert_eq!(executions.len(), 2);
    assert!(executions.iter().all(|row| row.status.is_pending()));
    assert!(matches!(
        outcome
            .expect("interrupt must finish waiting batch")
            .unwrap(),
        Err(Error::Cancelled)
    ));
    assert!(!session.process_control().has_running());
}

#[tokio::test]
async fn positive_session_sleep_uses_own_deadline_beyond_tool_and_turn_budgets() {
    let (_root, session) = fixture(
        vec![call(
            "finite-wait",
            "sleep",
            json!({"time_ms":100,"reason":"independent deadline"}),
        )],
        Limits {
            ..Limits::default()
        },
    );
    tokio::time::timeout(Duration::from_secs(10), session.submit("wait explicitly"))
        .await
        .expect("finite sleep must terminate")
        .unwrap();
    let result = session
        .messages()
        .iter()
        .find_map(|item| match item {
            ContentItem::Tool { content, .. } => {
                Some(serde_json::from_str::<Value>(content).unwrap())
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(result["success"], true);
    assert_eq!(result["cancelled"], false);
    assert_eq!(result["reason"], "independent deadline");
    assert!(result["elapsed_ms"].as_u64().unwrap() >= 100);
    session.close().await.unwrap();
}

#[tokio::test]
async fn positive_and_minus_one_waits_cancel_at_a_proven_pending_barrier() {
    let (_root, session) = fixture(
        vec![call("launch", "launch_fixture", json!({}))],
        Limits::default(),
    );
    session
        .submit("retain process for cancellation test")
        .await
        .unwrap();
    for time_ms in [60_000, -1] {
        let token = CancellationToken::new();
        let wait = Sleep.execute(
            json!({"time_ms":time_ms,"reason":"cancel wait only"}),
            sleep_context(&session, token.clone()),
        );
        tokio::pin!(wait);
        pending(wait.as_mut()).await;
        token.cancel();
        let result: Value = serde_json::from_str(&final_text(
            tokio::time::timeout(Duration::from_secs(5), wait)
                .await
                .expect("cancelled wait must finish promptly")
                .unwrap(),
        ))
        .unwrap();
        assert_eq!(result["success"], false);
        assert_eq!(result["cancelled"], true);
        assert_eq!(result["reason"], "cancel wait only");
        assert!(
            session.process_control().has_running(),
            "cancelling a wait does not kill its observed process"
        );
    }
    session.close().await.unwrap();
}

#[tokio::test]
async fn session_interrupt_cancels_an_admitted_positive_sleep() {
    use microcrab_auditor::ChannelAuditor;
    use microcrab_protocol::Moment;
    let (sender, mut records) = tokio::sync::mpsc::unbounded_channel();
    let runtime = Runtime::new(
        vec![ToolRegistration::Builtin(BuiltinTool::Sleep)],
        Arc::new(AllowTools::new(["sleep"])),
    )
    .unwrap();
    let session = Session::builder(
        Script(Mutex::new(Some(vec![call(
            "interruptible-sleep",
            "sleep",
            json!({"time_ms":60_000}),
        )]))),
        runtime,
    )
    .auditor(Arc::new(ChannelAuditor::from_sender(sender)))
    .build()
    .unwrap();
    let worker_session = session.clone();
    let worker = tokio::spawn(async move { worker_session.submit("wait until interrupted").await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(record) = records.recv().await {
            if record.event == Moment::ToolCall
                && record.tool_call_id.as_deref() == Some("interruptible-sleep")
            {
                return;
            }
        }
        panic!("audit channel closed before sleep admission");
    })
    .await
    .expect("sleep admission must be observable");
    session.interrupt();
    let outcome = tokio::time::timeout(Duration::from_secs(5), worker).await;
    session.close().await.unwrap();
    assert!(matches!(
        outcome.expect("interrupt must wake Session").unwrap(),
        Err(Error::Cancelled)
    ));
}

#[cfg(feature = "wasm")]
async fn actual_guest_yields_partial_output_without_process_lifetime_budget(environment: &str) {
    use microcrab_session::{
        policy::{Action, Matcher, Permission, PermissionConfig},
        runtime::wasm::{WasmAgent, WasmLimits},
    };
    let bytes =
        std::fs::read(std::env::var_os(environment).expect("set maintained guest component path"))
            .unwrap();
    let root = tempfile::tempdir().unwrap();
    let host = Arc::new(LocalHost::new(root.path(), ShellLimits::default()).unwrap());
    let agent = WasmAgent::load(&bytes, WasmLimits::default(), host.clone()).unwrap();
    let shell = if cfg!(windows) { "pwsh" } else { "bash" };
    let command = if cfg!(windows) {
        "[Console]::Out.WriteLine('guest-start'); [Console]::Error.WriteLine('guest-warning'); $line = [Console]::ReadLine(); [Console]::Out.WriteLine($line)"
    } else {
        "printf 'guest-start\\n'; printf 'guest-warning\\n' >&2; IFS= read -r line; printf '%s\\n' \"$line\""
    };
    let runtime = Runtime::new(
        agent
            .tools()
            .into_iter()
            .map(ToolRegistration::Wasm)
            .chain([ToolRegistration::Builtin(BuiltinTool::Sleep)])
            .collect(),
        Arc::new(AllowTools::new([shell, "sleep"])),
    )
    .unwrap();
    let session = Session::builder(
        Script(Mutex::new(Some(vec![
            call(
                "real-guest-shell",
                shell,
                json!({"command":command,"yield_ms":5000}),
            ),
            call(
                "real-guest-yield-zero",
                shell,
                json!({"command":waiting(),"yield_ms":0}),
            ),
            call("past-legacy-budget", "sleep", json!({"time_ms":100})),
        ]))),
        runtime,
    )
    .session_id("detached")
    .host(host)
    .agent(agent.agents()[0].clone())
    .permissions(PermissionConfig {
        initialization: Action::Allow,
        overrides: vec![Permission {
            agent: None,
            mode: None,
            tool: Some(Matcher::Exact(shell.into())),
            args: vec![],
            action: Action::Allow,
        }],
        ..PermissionConfig::default()
    })
    .limits(Limits {
        ..Limits::default()
    })
    .build()
    .unwrap();
    // Select the maintained executable mode while retaining its real catalog.
    session
        .select_mode(if environment == "MICROCRAB_CODING_WASM" {
            "build"
        } else {
            "execute"
        })
        .await
        .unwrap();
    let outcome = tokio::time::timeout(
        Duration::from_secs(20),
        session.submit("yield real guest process"),
    )
    .await;
    let messages = session.messages();
    let receipt = messages.iter().find_map(|item| match item {
        ContentItem::Tool { call_id, content } if call_id == "real-guest-shell" => {
            Some(serde_json::from_str::<Value>(content).unwrap())
        }
        _ => None,
    });
    let inspection = if let Some(receipt) = receipt.as_ref() {
        let id = receipt["id"].as_str().expect("canonical execution address");
        let status = action(&session, id, "status", json!({})).await;
        let cancelled = action(&session, id, "cancel", json!({})).await;
        Some((status, cancelled))
    } else {
        None
    };
    session.close().await.unwrap();
    outcome
        .expect("maintained guest must return a running receipt")
        .unwrap();
    let waited = messages
        .iter()
        .find_map(|item| match item {
            ContentItem::Tool { call_id, content } if call_id == "past-legacy-budget" => {
                Some(serde_json::from_str::<Value>(content).unwrap())
            }
            _ => None,
        })
        .expect("explicit sleep result");
    assert_eq!(waited["success"], true);
    assert!(waited["elapsed_ms"].as_u64().unwrap() >= 100);
    let zero: Value = messages
        .iter()
        .find_map(|item| match item {
            ContentItem::Tool { call_id, content } if call_id == "real-guest-yield-zero" => {
                Some(serde_json::from_str(content).unwrap())
            }
            _ => None,
        })
        .expect("zero-yield guest receipt");
    assert_eq!(zero["session_id"], "detached");
    assert_eq!(zero["status"], "running");
    assert!(zero["execution_id"].as_u64().is_some());
    assert!(zero.get("pid").is_none() && zero.get("job_id").is_none());
    let receipt = receipt.expect("real shell result");
    assert_ne!(zero["execution_id"], receipt["execution_id"]);
    assert_eq!(receipt["status"], "running", "{receipt}");
    assert!(
        receipt.get("pid").is_none(),
        "generic receipt does not expose process identity"
    );
    assert!(
        channel(&receipt["initial"], "stdout").contains("guest-start"),
        "{receipt}"
    );
    assert!(
        channel(&receipt["initial"], "stderr").contains("guest-warning"),
        "{receipt}"
    );
    let (status, cancelled) = inspection.expect("receipt identifies its execution");
    assert_eq!(
        status["status"], "running",
        "tool budgets must not kill an owned producer"
    );
    assert_ne!(cancelled["status"], "running");
    assert!(
        !session.process_control().has_running(),
        "kill and close must reap owned processes"
    );
}

#[cfg(feature = "wasm")]
#[tokio::test]
#[ignore = "parent gate builds maintained universal guest and sets MICROCRAB_UNIVERSAL_WASM"]
async fn maintained_universal_guest_yields_partial_output_and_outlives_legacy_budgets() {
    actual_guest_yields_partial_output_without_process_lifetime_budget("MICROCRAB_UNIVERSAL_WASM")
        .await;
}

#[cfg(feature = "wasm")]
#[tokio::test]
#[ignore = "parent gate builds maintained coding guest and sets MICROCRAB_CODING_WASM"]
async fn maintained_coding_guest_yields_partial_output_and_outlives_legacy_budgets() {
    actual_guest_yields_partial_output_without_process_lifetime_budget("MICROCRAB_CODING_WASM")
        .await;
}

#[tokio::test]
async fn minus_one_without_jobs_or_invocations_fails_immediately() {
    let (_root, session) = fixture(vec![], Limits::default());
    let result = Sleep
        .execute(
            json!({"time_ms":-1}),
            sleep_context(&session, CancellationToken::new()),
        )
        .await;
    assert!(
        matches!(result, Err(Error::Tool(message)) if message.contains("requires another pending execution"))
    );
    session.close().await.unwrap();
}
