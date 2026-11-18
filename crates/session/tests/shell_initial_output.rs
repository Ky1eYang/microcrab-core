//! Initial shell output must fit the tool budget without losing retained bytes.
use crate::execution_fixture;
use execution_fixture::{Fixture, channel, id};
use microcrab_host_local::{LocalHost, ShellLimits};
use microcrab_session::{
    Limits,
    runtime::execution::{ExecutionId, ExecutionStatus, OutputPage},
};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

fn large_output() -> &'static str {
    #[cfg(windows)]
    {
        "$text = ([string][char]1 + [string][char]0x4e2d + \"`n\") * 6000; [Console]::Out.Write($text); [Console]::Error.Write('tail'); exit 7"
    }
    #[cfg(unix)]
    {
        "i=0; while [ \"$i\" -lt 6000 ]; do printf '\\001\\344\\270\\255\\n'; i=$((i+1)); done; printf 'tail' >&2; exit 7"
    }
}

#[tokio::test]
async fn quick_large_terminal_output_yields_bounded_receipt_and_retains_complete_capture() {
    let root = tempfile::tempdir().unwrap();
    let host = Arc::new(LocalHost::new(root.path(), ShellLimits::default()).unwrap());
    let maximum = 1024;
    let fixture = Fixture::new(
        "large-quick-output",
        host,
        Limits {
            max_tool_output_bytes: maximum,
            output_buffer_bytes: 65536,
            ..Limits::default()
        },
    );
    let receipt = tokio::time::timeout(
        Duration::from_secs(20),
        fixture.start(large_output(), 60000),
    )
    .await
    .expect("tool-owned observation window must settle")
    .unwrap();
    assert!(serde_json::to_vec(&receipt).unwrap().len() <= maximum);
    assert_eq!(
        receipt["status"], "succeeded",
        "Host exit status is not a tool-reported failure"
    );
    assert!(receipt.get("pid").is_none() && receipt.get("job_id").is_none());
    assert!(
        receipt["initial"]["next_cursor"].as_u64().unwrap()
            < receipt["initial"]["end_cursor"].as_u64().unwrap()
    );
    let initial: OutputPage = serde_json::from_value(receipt["initial"].clone()).unwrap();
    let expected = "\u{1}中\n".repeat(6000);
    let prefix = channel(&initial, "stdout");
    assert!(!prefix.is_empty() && prefix.len() < expected.len());
    assert!(expected.starts_with(&prefix));
    let read = fixture.read(id(&receipt), 0, 256 * 1024).unwrap();
    let second = fixture.read(id(&receipt), 0, 256 * 1024).unwrap();
    assert_eq!(
        serde_json::to_value(&read).unwrap(),
        serde_json::to_value(second).unwrap()
    );
    assert_eq!(channel(&read, "stdout"), expected);
    assert_eq!(channel(&read, "stderr"), "tail");
    assert!(read.closed && read.start_cursor == 0);
    let drained = fixture
        .session
        .executions()
        .read(fixture.id(id(&receipt)).unwrap(), 0, 256 * 1024)
        .unwrap();
    assert_eq!(channel(&drained, "stdout"), expected);
    assert_eq!(channel(&drained, "stderr"), "tail");
    fixture.close().await.unwrap();
}

#[tokio::test]
async fn stdin_held_process_survives_observation_and_has_no_execution_deadline() {
    let root = tempfile::tempdir().unwrap();
    let host = Arc::new(LocalHost::new(root.path(), ShellLimits::default()).unwrap());
    let fixture = Fixture::new(
        "no-execution-deadline",
        host,
        Limits {
            ..Limits::default()
        },
    );
    let command = if cfg!(windows) {
        "$null = [Console]::ReadLine()"
    } else {
        "IFS= read -r line"
    };
    let receipt = fixture.start(command, 25).await.unwrap();
    assert_eq!(receipt["status"], "running");
    assert!(receipt.get("pid").is_none());
    tokio::time::sleep(Duration::from_millis(75)).await;
    assert_eq!(
        fixture
            .action(id(&receipt), "status", json!({}))
            .await
            .unwrap()["status"],
        "running"
    );
    fixture.close().await.unwrap();
}

#[cfg(feature = "wasm")]
mod maintained_guests {
    use super::*;
    use async_trait::async_trait;
    use microcrab_auditor::{Auditor, VariableView};
    use microcrab_protocol::{Response, ToolCall};
    use microcrab_session::{
        Client, Session,
        context::{Content, ContentItem},
        policy::{Action, Matcher, Permission, PermissionConfig},
        runtime::{
            Runtime, ToolRegistration,
            tool::AllowTools,
            wasm::{WasmAgent, WasmLimits},
        },
    };
    use std::sync::Mutex;

    struct Model(Mutex<Option<ToolCall>>);
    #[async_trait]
    impl Client for Model {
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
                tool_calls: self.0.lock().unwrap().take().into_iter().collect(),
                usage: Default::default(),
            })
        }
    }

    async fn retained_terminal_guest_receipt(environment: &str) {
        let bytes = std::fs::read(
            std::env::var_os(environment).expect("set current maintained guest component path"),
        )
        .unwrap();
        let root = tempfile::tempdir().unwrap();
        let host = Arc::new(LocalHost::new(root.path(), ShellLimits::default()).unwrap());
        let agent = WasmAgent::load(&bytes, WasmLimits::default(), host.clone()).unwrap();
        let shell = if cfg!(windows) { "pwsh" } else { "bash" };
        let runtime = Runtime::new(
            agent
                .tools()
                .into_iter()
                .map(ToolRegistration::Wasm)
                .collect(),
            Arc::new(AllowTools::new([shell])),
        )
        .unwrap();
        let maximum = 1024;
        let session = Session::builder(
            Model(Mutex::new(Some(ToolCall {
                provider_metadata: None,
                id: "guest-retained-terminal".into(),
                name: shell.into(),
                arguments: json!({"command":large_output(),"yield_ms":60000}),
            }))),
            runtime,
        )
        .session_id("guest-terminal-output")
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
            max_tool_output_bytes: maximum,
            output_buffer_bytes: 65536,
            ..Limits::default()
        })
        .build()
        .unwrap();
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
            session.submit("return the real retained terminal guest receipt"),
        )
        .await;
        let messages = session.messages();
        let receipt = messages.iter().find_map(|item| match item {
            ContentItem::Tool { call_id, content, .. } if call_id == "guest-retained-terminal" => {
                serde_json::from_str::<Value>(content)
                    .ok()
                    .map(|value| (content.len(), value))
            }
            _ => None,
        });
        let inspection = receipt
            .as_ref()
            .and_then(|(_, value)| value["execution_id"].as_u64())
            .map(|id| {
                let id = ExecutionId(id);
                let row = session.executions().get(id).unwrap();
                let read = session.read_execution_output(id, 0, 256 * 1024).unwrap();
                let again = session.read_execution_output(id, 0, 256 * 1024).unwrap();
                (row, read, again)
            });
        session.close().await.unwrap();
        outcome
            .expect("maintained guest must settle its terminal receipt")
            .unwrap();
        assert!(!session.executions().has_pending());
        let (bytes, receipt) = receipt.expect("maintained guest returns a typed Buffered receipt");
        assert!(
            bytes <= maximum,
            "escaped initial output is bounded by the tool budget"
        );
        assert_eq!(receipt["status"], "failed");
        assert!(receipt.get("pid").is_none() && receipt.get("job_id").is_none());
        assert_eq!(receipt["session_id"], "guest-terminal-output");
        assert!(
            receipt["initial"]["next_cursor"].as_u64().unwrap()
                < receipt["initial"]["end_cursor"].as_u64().unwrap()
        );
        let initial: OutputPage = serde_json::from_value(receipt["initial"].clone()).unwrap();
        let expected = "\u{1}中\n".repeat(6000);
        let prefix = channel(&initial, "stdout");
        assert!(!prefix.is_empty() && expected.starts_with(&prefix));
        let (row, read, again) = inspection.expect("canonical execution remains inspectable");
        assert_eq!(row.status, ExecutionStatus::Failed);
        assert!(
            row.failure.is_some(),
            "guest explicitly reports its process failure"
        );
        assert_eq!(
            serde_json::to_value(&read).unwrap(),
            serde_json::to_value(again).unwrap()
        );
        assert_eq!(channel(&read, "stdout"), expected);
        assert_eq!(channel(&read, "stderr"), "tail");
        assert!(read.start_cursor == 0 && read.closed);
    }

    #[tokio::test]
    #[ignore = "parent gate sets current MICROCRAB_UNIVERSAL_WASM component path"]
    async fn maintained_universal_guest_preserves_retained_terminal_receipt_fields() {
        retained_terminal_guest_receipt("MICROCRAB_UNIVERSAL_WASM").await;
    }

    #[tokio::test]
    #[ignore = "parent gate sets current MICROCRAB_CODING_WASM component path"]
    async fn maintained_coding_guest_preserves_retained_terminal_receipt_fields() {
        retained_terminal_guest_receipt("MICROCRAB_CODING_WASM").await;
    }
}
