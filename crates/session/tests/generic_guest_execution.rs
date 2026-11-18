//! Maintained guest continuations own post-yield output, failures and cleanup.
#![cfg(feature = "wasm")]
use async_trait::async_trait;
use microcrab_host_api::ProcessApi;
use microcrab_host_local::{LocalHost, ShellLimits};
use microcrab_protocol::{Response, ToolCall};
use microcrab_session::{
    Auditor, Client, Session, VariableView,
    context::{Content, ContentItem},
    runtime::{
        Runtime, ToolRegistration,
        execution::{ExecutionId, ExecutionSnapshot, ExecutionStatus, ToolFailureKind},
        tool::DenyAll,
        wasm::{WasmAgent, WasmLimits},
    },
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

struct Once(Mutex<Option<ToolCall>>);
#[async_trait]
impl Client for Once {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        Ok(Response {
            provider_metadata: None,
            content: "settled".into(),
            tool_calls: self.0.lock().unwrap().take().into_iter().collect(),
            usage: Default::default(),
        })
    }
}
async fn fixture(root: &tempfile::TempDir, command: &str) -> (Session, Arc<LocalHost>, Value) {
    let bytes = std::fs::read(
        std::env::var_os("MICROCRAB_UNIVERSAL_WASM")
            .expect("parent gate supplies the freshly built universal component"),
    )
    .unwrap();
    let host = Arc::new(LocalHost::new(root.path(), ShellLimits::default()).unwrap());
    let agent = WasmAgent::load(&bytes, WasmLimits::default(), host.clone()).unwrap();
    let shell = if cfg!(windows) { "pwsh" } else { "bash" };
    let runtime = Runtime::new(
        agent
            .tools()
            .into_iter()
            .map(ToolRegistration::Wasm)
            .collect(),
        Arc::new(DenyAll),
    )
    .unwrap();
    let permissions = serde_json::from_value(json!({"initialization":"allow","overrides":[
        {"tool":{"kind":"exact","value":shell},"action":"allow"}
    ]}))
    .unwrap();
    let session = Session::builder(
        Once(Mutex::new(Some(ToolCall {
            provider_metadata: None,
            id: "continued-guest".into(),
            name: shell.into(),
            arguments: json!({"command":command,"yield_ms":0}),
        }))),
        runtime,
    )
    .session_id("guest-continuation")
    .host(host.clone())
    .agent(agent.agents()[0].clone())
    .permissions(permissions)
    .build()
    .unwrap();
    session.select_mode("execute").await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(20),
        session.submit("start and yield immediately"),
    )
    .await
    .expect("yield zero must settle the first turn while the producer waits for input")
    .unwrap();
    let receipt = session
        .messages()
        .iter()
        .find_map(|item| match item {
            ContentItem::Tool { call_id, content, .. } if call_id == "continued-guest" => {
                Some(serde_json::from_str::<Value>(content).unwrap())
            }
            _ => None,
        })
        .expect("the actual guest returns a generic execution receipt");
    assert_eq!(receipt["status"], "running");
    assert_eq!(receipt["session_id"], "guest-continuation");
    assert!(receipt.get("pid").is_none() && receipt.get("job_id").is_none());
    assert!(
        receipt["initial"]["chunks"].is_array(),
        "initial output may be empty"
    );
    assert!(
        ProcessApi::has_running_processes(host.as_ref()),
        "first turn settled without killing the owned Host process"
    );
    (session, host, receipt)
}
fn id(receipt: &Value) -> ExecutionId {
    ExecutionId(receipt["execution_id"].as_u64().unwrap())
}
fn text(session: &Session, id: ExecutionId, channel: &str) -> String {
    session
        .read_execution_output(id, 0, 65536)
        .unwrap()
        .chunks
        .into_iter()
        .filter(|chunk| chunk.channel.as_deref() == Some(channel))
        .map(|chunk| chunk.content)
        .collect()
}
async fn terminal(session: &Session, id: ExecutionId) -> ExecutionSnapshot {
    let registry = session.executions();
    let mut changes = registry.subscribe();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let row = registry.get(id).unwrap();
            if !row.status.is_pending() {
                return row;
            }
            changes.changed().await.unwrap();
        }
    })
    .await
    .expect("the real guest continuation must settle its producer")
}
#[tokio::test]
async fn maintained_guest_yield_zero_continues_output_after_the_first_turn_settles() {
    let root = tempfile::tempdir().unwrap();
    let command = if cfg!(windows) {
        "$line = [Console]::ReadLine(); [Console]::Out.WriteLine('after-first-turn:' + $line)"
    } else {
        "IFS= read -r line; printf 'after-first-turn:%s\n' \"$line\""
    };
    let (session, host, receipt) = fixture(&root, command).await;
    let id = id(&receipt);
    assert!(!text(&session, id, "stdout").contains("after-first-turn"));
    session
        .executions()
        .write(
            id,
            "continued-input\n".into(),
            true,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        terminal(&session, id).await.status,
        ExecutionStatus::Succeeded
    );
    assert!(text(&session, id, "stdout").contains("after-first-turn:continued-input"));
    session.close().await.unwrap();
    assert!(!ProcessApi::has_running_processes(host.as_ref()));
}
#[tokio::test]
async fn maintained_guest_drive_self_reports_nonzero_exit_after_yield_with_output_retained() {
    let root = tempfile::tempdir().unwrap();
    let command = if cfg!(windows) {
        "$null = [Console]::ReadLine(); [Console]::Out.WriteLine('after-first-turn'); [Console]::Error.WriteLine('guest-failure-detail'); exit 7"
    } else {
        "IFS= read -r line; printf 'after-first-turn\n'; printf 'guest-failure-detail\n' >&2; exit 7"
    };
    let (session, host, receipt) = fixture(&root, command).await;
    let id = id(&receipt);
    assert!(session.executions().get(id).unwrap().failure.is_none());
    session
        .executions()
        .write(
            id,
            "report-failure\n".into(),
            true,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let row = terminal(&session, id).await;
    assert_eq!(row.status, ExecutionStatus::Failed);
    let failure = row
        .failure
        .expect("guest drive explicitly reports failure through the typed execution API");
    assert_eq!(failure.kind, ToolFailureKind::Failed);
    assert!(!failure.code.is_empty());
    assert!(text(&session, id, "stdout").contains("after-first-turn"));
    assert!(text(&session, id, "stderr").contains("guest-failure-detail"));
    session.close().await.unwrap();
    assert!(!ProcessApi::has_running_processes(host.as_ref()));
}
#[tokio::test]
async fn ending_guest_epoch_reaps_the_actual_process_and_preserves_already_delivered_bytes() {
    let root = tempfile::tempdir().unwrap();
    let command = if cfg!(windows) {
        "[Console]::Out.WriteLine('before-epoch'); $null = [Console]::ReadLine()"
    } else {
        "printf 'before-epoch\n'; IFS= read -r line"
    };
    let (session, host, receipt) = fixture(&root, command).await;
    let id = id(&receipt);
    let registry = session.executions();
    let mut changes = registry.subscribe();
    tokio::time::timeout(Duration::from_secs(20), async {
        while !text(&session, id, "stdout").contains("before-epoch") {
            changes.changed().await.unwrap();
        }
    })
    .await
    .expect("actual guest output must reach its Runtime buffer");
    let before = text(&session, id, "stdout");
    assert!(ProcessApi::has_running_processes(host.as_ref()));
    let old_epoch = session.state().epoch_id;
    session
        .compact("end the old guest execution epoch", json!({}))
        .await
        .unwrap();
    assert_eq!(
        session.state().epoch_id,
        old_epoch,
        "compaction defers epoch transition until the next valid input"
    );
    assert!(ProcessApi::has_running_processes(host.as_ref()));
    assert_eq!(
        session.executions().get(id).unwrap().status,
        ExecutionStatus::Running
    );
    session
        .submit("enter the compacted conversation epoch")
        .await
        .unwrap();
    assert_eq!(session.state().epoch_id, old_epoch + 1);
    let row = terminal(&session, id).await;
    assert!(matches!(
        row.status,
        ExecutionStatus::Cancelled | ExecutionStatus::Interrupted
    ));
    // Interrupted records uncertain completion before the asynchronous owner
    // cleanup has acknowledged the actual Host resource termination.
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut observations = tokio::time::interval(Duration::from_millis(10));
        while ProcessApi::has_running_processes(host.as_ref()) {
            observations.tick().await;
        }
    })
    .await
    .expect(
        "ending the epoch must eventually reap the actual Host process without a separate release",
    );
    assert!(
        !ProcessApi::has_running_processes(host.as_ref()),
        "epoch shutdown acknowledges cleanup of the actual Host process"
    );
    assert_eq!(text(&session, id, "stdout"), before);
    assert!(session.read_execution_output(id, 0, 65536).unwrap().closed);
    session.close().await.unwrap();
}
