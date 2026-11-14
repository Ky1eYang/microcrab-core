use async_trait::async_trait;
use microcrab_auditor::{
    AuditBody, Auditor, AuditorError, BoxFuture, HookContext, Injection, Moment,
};
use microcrab_protocol::{Response, ToolCall, Usage};
use microcrab_session::context::{Content, ContentItem};
use microcrab_session::runtime::execution::ToolOutput;
use microcrab_session::runtime::tool::{AllowTools, Tool, ToolContext};
use microcrab_session::runtime::{Runtime, ToolRegistration};
use microcrab_session::{Client, Error, Session, VariablesChange};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

struct Script {
    seen: Arc<Mutex<Vec<Content>>>,
    responses: Mutex<Vec<Response>>,
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
        Ok(self.responses.lock().unwrap().remove(0))
    }
}
struct Echo;
#[async_trait]
impl Tool for Echo {
    fn definition(&self) -> microcrab_protocol::ToolDefinition {
        microcrab_protocol::ToolDefinition {
            name: "echo".into(),
            description: "Echo".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn execute(&self, _: Value, _: ToolContext) -> Result<ToolOutput, Error> {
        Ok(ToolOutput::text("ok"))
    }
}
struct InjectRounds;
impl Auditor for InjectRounds {
    fn is_enabled(&self) -> bool {
        true
    }
    fn publish(&self, _: Moment, _: AuditBody) {}
    fn intervene<'a>(
        &'a self,
        moment: Moment,
        ctx: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
        Box::pin(async move {
            let injected = match moment {
                Moment::InputAdmitted => 1,
                Moment::RoundCompleted if ctx.round == Some(1) => 2,
                _ => return Ok(Injection::default()),
            };
            assert!(ctx.variables.get("time").is_some());
            Ok(Injection {
                variables: VariablesChange {
                    scoped_set: vec![],
                    set: json!({"injected":injected}).as_object().unwrap().clone(),
                    remove: vec![],
                },
                ..Injection::default()
            })
        })
    }
}
#[tokio::test]
async fn custom_access_is_atomic_and_builtins_are_reserved() {
    let client = Arc::new(Script {
        seen: Arc::new(Mutex::new(vec![])),
        responses: Mutex::new(vec![]),
    });
    let runtime = Runtime::new(vec![], Arc::new(AllowTools::new(Vec::<String>::new()))).unwrap();
    let session = Session::builder_arc(client, runtime).build().unwrap();
    let state = session.state_handle();
    state.inject("custom", json!({"nested":1})).await.unwrap();
    let mut copy = state.variables();
    copy["custom"]["nested"] = json!(2);
    assert_eq!(state.extract("custom"), Some(json!({"nested":1})));
    assert!(state.patch(json!({"safe":1,"time":2})).await.is_err());
    assert!(state.extract("safe").is_none());
    assert!(state.remove("time").await.is_err());
    state.remove("custom").await.unwrap();
    assert!(state.variables().as_object().unwrap().is_empty());
    session.close().await.unwrap();
}

#[tokio::test]
async fn hook_injection_is_visible_in_each_round_without_repeating_snapshot() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let client = Arc::new(Script {
        seen: seen.clone(),
        responses: Mutex::new(vec![
            Response {
                provider_metadata: None,
                content: "working".into(),
                tool_calls: vec![ToolCall {
                    provider_metadata: None,
                    id: "call".into(),
                    name: "echo".into(),
                    arguments: json!({}),
                }],
                usage: Usage::default(),
            },
            Response {
                provider_metadata: None,
                content: "done".into(),
                tool_calls: vec![],
                usage: Usage::default(),
            },
        ]),
    });
    let runtime = Runtime::new(
        vec![ToolRegistration::External(Arc::new(Echo))],
        Arc::new(AllowTools::new(["echo"])),
    )
    .unwrap();
    let session = Session::builder_arc(client, runtime)
        .auditor(Arc::new(InjectRounds))
        .build()
        .unwrap();
    let mut input = microcrab_session::SessionInput::text("inspect");
    input.variables.set = json!({"user":true}).as_object().unwrap().clone();
    let result = session.submit(input).await.unwrap();
    assert_eq!(result.rounds, 2);
    session.close().await.unwrap();
    let requests = seen.lock().unwrap();
    assert_eq!(requests[0].system, requests[1].system);
    assert_eq!(requests[0].tools, requests[1].tools);
    for (index, content) in requests.iter().enumerate() {
        let items = serde_json::to_value(&content.items).unwrap();
        assert!(items.to_string().contains("unix_millis"));
        assert!(items.to_string().contains("injected"));
        let patches = content
            .items
            .iter()
            .filter(|item| {
                matches!(
                    item,
                    ContentItem::State(microcrab_transport::StateFrame::StatePatch { .. })
                )
            })
            .count();
        assert!(patches > index);
        assert_eq!(
            content
                .items
                .iter()
                .filter(|item| matches!(
                    item,
                    ContentItem::State(microcrab_transport::StateFrame::StateSnapshot { .. })
                ))
                .count(),
            1
        );
    }
    assert_eq!(session.state_handle().extract("injected"), Some(json!(2)));
    assert!(session.state_handle().extract("time").is_none());
}
