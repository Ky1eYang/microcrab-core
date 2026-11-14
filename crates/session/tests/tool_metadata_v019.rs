//! Real Session output appears once in tool content, never in State patches.
use async_trait::async_trait;
use microcrab_auditor::{Auditor, VariableView};
use microcrab_protocol::{Response, ToolCall, ToolDefinition};
use microcrab_session::runtime::execution::ToolOutput;
use microcrab_session::{
    Client, Error, Session,
    context::{Content, ContentItem, Context},
    runtime::{
        Runtime, ToolRegistration,
        tool::{AllowTools, Tool, ToolContext},
    },
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio_util::sync::CancellationToken;

struct Model(AtomicUsize);
#[async_trait]
impl Client for Model {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let n = self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Response {
            provider_metadata: None,
            content: if n >= 2 { "done".into() } else { String::new() },
            tool_calls: if n < 2 {
                vec![ToolCall {
                    provider_metadata: None,
                    id: format!("echo-{n}"),
                    name: "echo".into(),
                    arguments: json!({"n":n}),
                }]
            } else {
                vec![]
            },
            usage: Default::default(),
        })
    }
}
struct Echo;
#[async_trait]
impl Tool for Echo {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "echo".into(),
            description: "bounded fixture output".into(),
            parameters: json!({"type":"object","required":["n"],"properties":{"n":{"type":"integer"}},"additionalProperties":false}),
        }
    }
    async fn execute(&self, args: Value, _: ToolContext) -> Result<ToolOutput, Error> {
        Ok(ToolOutput::text(format!(
            "BODY-{}-{}",
            args["n"],
            "中".repeat(2048)
        )))
    }
}
#[tokio::test]
async fn real_state_is_metadata_only_keyed_deltas_roundtrip_and_output_is_explicit() {
    let runtime = Runtime::new(
        vec![ToolRegistration::External(Arc::new(Echo))],
        Arc::new(AllowTools::new(["echo"])),
    )
    .unwrap();
    let session = Session::builder(Model(AtomicUsize::new(0)), runtime)
        .session_id("metadata-fixture")
        .build()
        .unwrap();
    session.submit("two sequential outputs").await.unwrap();
    let state = session.state_view().unwrap();
    assert!(state.get("executions").is_none());
    assert!(session.state().executions.is_empty());
    assert_eq!(session.execution_snapshots().len(), 2);
    assert!(!state.to_string().contains("BODY-"));
    assert!(
        session
            .execution_snapshots()
            .iter()
            .all(|row| row.result.is_none())
    );
    for item in session.messages() {
        if let ContentItem::State(frame) = item {
            let encoded = serde_json::to_value(&frame).unwrap();
            assert!(!encoded.to_string().contains("BODY-"));
            assert!(
                encoded["set"].get("executions").is_none(),
                "fully delivered foreground Final results must not add duplicate LLM execution rows"
            );
        }
    }
    let snapshot = session.context_snapshot().unwrap();
    let original = serde_json::to_value(&snapshot).unwrap();
    let restored = Context::from_snapshot(snapshot).unwrap();
    assert_eq!(
        serde_json::to_value(restored.snapshot().unwrap()).unwrap(),
        original
    );
    let page = session
        .read_execution_output(
            microcrab_session::runtime::execution::ExecutionId(1),
            0,
            65536,
        )
        .unwrap();
    assert!(
        page.chunks
            .iter()
            .map(|chunk| chunk.content.as_str())
            .collect::<String>()
            .starts_with("BODY-0-")
    );
    assert_eq!(
        serde_json::to_value(&page).unwrap(),
        serde_json::to_value(
            session
                .read_execution_output(
                    microcrab_session::runtime::execution::ExecutionId(1),
                    0,
                    65536
                )
                .unwrap()
        )
        .unwrap()
    );
    // Catalog publication commits a real Recover, not a public metadata snapshot.
    // It must retain completed output bodies while ordinary State stays body-free.
    session
        .replace_external_tools(vec![Arc::new(Echo)])
        .await
        .unwrap();
    let checkpoint = session.checkpoint().await.unwrap();
    assert!(
        checkpoint.state.executions[0]
            .result
            .as_deref()
            .unwrap()
            .starts_with("BODY-0-")
    );
    assert!(
        session
            .state()
            .executions
            .iter()
            .all(|row| row.result.is_none())
    );
    assert_eq!(
        serde_json::to_value(&page).unwrap(),
        serde_json::to_value(
            session
                .read_execution_output(
                    microcrab_session::runtime::execution::ExecutionId(1),
                    0,
                    65536
                )
                .unwrap()
        )
        .unwrap()
    );
    let mut incomplete = checkpoint;
    incomplete.state.executions[0].result = None;
    assert!(
        incomplete.validate().is_err(),
        "durable recovery must not silently accept metadata with missing retained bytes"
    );
    let prefix = session.messages();
    session
        .submit("continue without rereading output")
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(&session.messages()[..prefix.len()]).unwrap(),
        serde_json::to_value(prefix).unwrap()
    );
    assert!(
        session
            .messages()
            .iter()
            .filter_map(|item| if let ContentItem::State(frame) = item {
                Some(serde_json::to_string(frame).unwrap())
            } else {
                None
            })
            .all(|text| !text.contains("BODY-"))
    );
    session.close().await.unwrap();
}
