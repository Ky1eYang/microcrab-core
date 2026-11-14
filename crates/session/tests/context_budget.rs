//! Whole-response reservation rejects before effects and retains exact retry input.
use microcrab_protocol::{Response, ToolCall, ToolDefinition, Usage};
use microcrab_session::context::Content;
use microcrab_session::runtime::execution::ToolOutput;
use microcrab_session::runtime::tool::{AllowTools, Tool, ToolContext};
use microcrab_session::runtime::{Runtime, ToolRegistration, journal::MemoryJournal};
use microcrab_session::{Auditor, Client, Error, LimitKind, Limits, Session};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio_util::sync::CancellationToken;
struct Counted(Arc<AtomicUsize>);
#[async_trait::async_trait]
impl Tool for Counted {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "counted".into(),
            description: "Count effects".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn execute(&self, _: Value, _: ToolContext) -> Result<ToolOutput, Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutput::text("done"))
    }
}
struct Model {
    count: usize,
    duplicate: bool,
    fill_to_boundary: bool,
    seen: Arc<Mutex<Vec<Value>>>,
}
#[async_trait::async_trait]
impl Client for Model {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let mut seen = self.seen.lock().unwrap();
        seen.push(serde_json::to_value(content).unwrap());
        let mut response = Response {
            provider_metadata: None,
            content: "answer".into(),
            tool_calls: if seen.len() == 1 {
                (0..self.count)
                    .map(|n| ToolCall {
                        provider_metadata: None,
                        id: if self.duplicate {
                            "duplicate".into()
                        } else {
                            format!("call-{n}")
                        },
                        name: "counted".into(),
                        arguments: json!({}),
                    })
                    .collect()
            } else {
                vec![]
            },
            usage: Usage::default(),
        };
        if self.fill_to_boundary && seen.len() == 1 {
            let mut items = content.items.clone();
            items.push(microcrab_session::context::ContentItem::Assistant {
                content: response.content.clone(),
                provider_metadata: None,
                tool_calls: response.tool_calls.clone(),
            });
            let bytes = json!({"system":content.system,"items":items,"tools":content.tools})
                .to_string()
                .len();
            // The previous six-byte success reservation plus Tool framing fit,
            // while even the ordinary safe failure diagnostic cannot fit.
            response
                .content
                .push_str(&"x".repeat(content.max_bytes - bytes - 128));
        }
        Ok(response)
    }
}
fn runtime(effects: Arc<AtomicUsize>) -> Runtime {
    Runtime::new(
        vec![ToolRegistration::External(Arc::new(Counted(effects)))],
        Arc::new(AllowTools::new(["counted"])),
    )
    .unwrap()
}
#[tokio::test]
async fn six_eight_and_sixteen_calls_fit_declared_default_with_server_output_limit() {
    for count in [6, 8, 16] {
        let effects = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(vec![]));
        let session = Session::builder(
            Model {
                count,
                duplicate: false,
                fill_to_boundary: false,
                seen: seen.clone(),
            },
            runtime(effects.clone()),
        )
        .limits(Limits {
            max_tool_output_bytes: 64 * 1024,
            ..Limits::default()
        })
        .build()
        .unwrap();
        assert_eq!(session.submit("run batch").await.unwrap().content, "answer");
        assert_eq!(effects.load(Ordering::SeqCst), count);
        assert_eq!(seen.lock().unwrap().len(), 2);
        session.close().await.unwrap();
    }
}
#[tokio::test]
async fn rejected_batch_has_no_effects_and_manual_retry_reuses_exact_prepared_request() {
    for count in [6, 8, 16] {
        let effects = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(vec![]));
        let journal = Arc::new(MemoryJournal::new());
        let session = Session::builder(
            Model {
                count,
                duplicate: false,
                fill_to_boundary: false,
                seen: seen.clone(),
            },
            runtime(effects.clone()),
        )
        .limits(Limits {
            max_context_bytes: 1024 * 1024,
            max_tool_output_bytes: 64 * 1024,
            ..Limits::default()
        })
        .journal_arc(journal.clone())
        .build()
        .unwrap();
        let Error::Limit(limit) = session.submit("reject before effects").await.unwrap_err() else {
            panic!("expected typed reservation rejection")
        };
        assert_eq!(limit.kind, LimitKind::ToolResultReservation);
        assert!(limit.is_pre_effect());
        assert_eq!(limit.calls, Some(count));
        assert!(limit.required_bytes.unwrap() >= count * 64 * 1024 * 6);
        assert_eq!(limit.budget_bytes, Some(1024 * 1024));
        assert_eq!(effects.load(Ordering::SeqCst), 0);
        assert_eq!(seen.lock().unwrap().len(), 1);
        assert!(session.execution_snapshots().is_empty());
        assert!(session.retry_ready());
        let saved = journal.latest().unwrap();
        assert!(saved.continuation.as_ref().unwrap().request_prepared);
        assert!(
            !serde_json::to_string(&saved.context)
                .unwrap()
                .contains("call-0")
        );
        let mut stream = session.retry_stream().await.unwrap();
        while let Some(event) = stream.recv().await {
            event.unwrap();
        }
        {
            let requests = seen.lock().unwrap();
            assert_eq!(requests[0], requests[1]);
        }
        assert_eq!(effects.load(Ordering::SeqCst), 0);
        assert!(!session.retry_ready());
        assert!(session.submit("still usable").await.is_ok());
        session.close().await.unwrap();
    }
}

#[tokio::test]
async fn invalid_oversized_response_remains_protocol_failure_before_admission_and_can_retry() {
    let effects = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(vec![]));
    let journal = Arc::new(MemoryJournal::new());
    let session = Session::builder(
        Model {
            count: 17,
            duplicate: true,
            fill_to_boundary: false,
            seen: seen.clone(),
        },
        runtime(effects.clone()),
    )
    .journal_arc(journal.clone())
    .build()
    .unwrap();
    assert!(matches!(
        session.submit("reject invalid batch").await,
        Err(Error::Protocol(_))
    ));
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert!(session.execution_snapshots().is_empty());
    assert!(session.retry_ready());
    let saved = journal.latest().unwrap();
    assert!(saved.continuation.as_ref().unwrap().request_prepared);
    let boundary = serde_json::to_value(session.context_snapshot().unwrap()).unwrap();
    assert_eq!(boundary, serde_json::to_value(&saved.context).unwrap());
    assert!(!boundary.to_string().contains("duplicate"));
    assert_eq!(session.resume().await.unwrap().content, "answer");
    {
        let requests = seen.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0], requests[1]);
    }
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert!(session.execution_snapshots().is_empty());
    assert!(!session.retry_ready());
    assert!(session.submit("still usable").await.is_ok());
    session.close().await.unwrap();
}

struct FailingCounted(Arc<AtomicUsize>);
#[async_trait::async_trait]
impl Tool for FailingCounted {
    fn definition(&self) -> ToolDefinition {
        Counted(self.0.clone()).definition()
    }
    async fn execute(&self, _: Value, _: ToolContext) -> Result<ToolOutput, Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(Error::Tool("synthetic failing invocation".into()))
    }
}
#[tokio::test]
async fn tiny_success_output_cap_still_reserves_failed_tool_results_before_effects() {
    for fill_to_boundary in [false, true] {
        let effects = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(vec![]));
        let runtime = Runtime::new(
            vec![ToolRegistration::External(Arc::new(FailingCounted(
                effects.clone(),
            )))],
            Arc::new(AllowTools::new(["counted"])),
        )
        .unwrap();
        let session = Session::builder(
            Model {
                count: 1,
                duplicate: false,
                fill_to_boundary,
                seen: seen.clone(),
            },
            runtime,
        )
        .limits(Limits {
            max_context_bytes: 1024 * 1024,
            max_tool_output_bytes: 1,
            ..Limits::default()
        })
        .build()
        .unwrap();
        let result = session.submit("diagnostic boundary").await;
        if fill_to_boundary {
            let Err(Error::Limit(limit)) = result else {
                panic!("expected pre-effect diagnostic reservation rejection")
            };
            assert_eq!(limit.kind, LimitKind::ToolResultReservation);
            assert!(limit.is_pre_effect());
            assert_eq!(effects.load(Ordering::SeqCst), 0);
            assert_eq!(seen.lock().unwrap().len(), 1);
            assert!(session.execution_snapshots().is_empty());
            assert!(session.retry_ready());
        } else {
            assert_eq!(result.unwrap().content, "answer");
            assert_eq!(effects.load(Ordering::SeqCst), 1);
            assert_eq!(seen.lock().unwrap().len(), 2);
            assert!(!session.retry_ready());
            assert!(
                serde_json::to_string(&session.context_snapshot().unwrap())
                    .unwrap()
                    .contains("tool_failed")
            );
        }
        session.close().await.unwrap();
    }
}
