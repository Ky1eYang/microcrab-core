//! Public auditor hooks are object-safe, awaited interventions and observations.
use microcrab_auditor::{
    AuditBody, Auditor, AuditorError, BoxFuture, HookContext, Injection, VariableView,
};
use microcrab_protocol::{
    HookPayload, InputPart, Moment, Response, ToolCall, ToolDefinition, VariablesChange,
};
use microcrab_session::context::Content;
use microcrab_session::runtime::execution::ToolOutput;
use microcrab_session::runtime::{
    Runtime, ToolRegistration,
    tool::{Tool, ToolContext},
};
use microcrab_session::{Client, Error, Session};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio_util::sync::CancellationToken;

#[test]
fn public_moments_are_exactly_the_eight_intervention_and_observation_points() {
    let moments = [
        Moment::SessionCreated,
        Moment::SessionRestored,
        Moment::InputAdmitted,
        Moment::RequestPrepared,
        Moment::RoundCompleted,
        Moment::ToolCall,
        Moment::ToolResult,
        Moment::SessionTerminated,
    ];
    assert_eq!(
        serde_json::to_value(moments).unwrap(),
        json!([
            "session_created",
            "session_restored",
            "input_admitted",
            "request_prepared",
            "round_completed",
            "tool_call",
            "tool_result",
            "session_terminated",
        ])
    );
    for obsolete in [
        "epoch_started",
        "epoch_ended",
        "turn_started",
        "turn_ended",
        "round_started",
        "round_suspended",
        "round_resumed",
        "round_ended",
    ] {
        assert!(serde_json::from_value::<Moment>(json!(obsolete)).is_err());
    }
}

struct Calls {
    seen: Mutex<Vec<Content>>,
}
#[async_trait::async_trait]
impl Client for Calls {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        variables: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        assert_eq!(variables.get("admitted"), Some(json!(true)));
        let mut seen = self.seen.lock().unwrap();
        seen.push(content.clone());
        Ok(Response {
            provider_metadata: None,
            content: if seen.len() == 1 { "working" } else { "done" }.into(),
            tool_calls: if seen.len() == 1 {
                vec![ToolCall {
                    provider_metadata: None,
                    id: "typed-call".into(),
                    name: "effect".into(),
                    arguments: json!({"value":7}),
                }]
            } else {
                vec![]
            },
            usage: Default::default(),
        })
    }
}
struct Effect {
    fail: bool,
    calls: Arc<AtomicUsize>,
}
#[async_trait::async_trait]
impl Tool for Effect {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "effect".into(),
            description: "Record effect".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn execute(
        &self,
        arguments: Value,
        invocation: ToolContext,
    ) -> Result<ToolOutput, Error> {
        assert_eq!(arguments, json!({"value":7}));
        assert_eq!(invocation.state.variables()["policy_ready"], true);
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            Err(Error::Tool("effect storage write failed; token=private-tool-secret; response_body=private-tool-body".into()))
        } else {
            Ok(ToolOutput::text("actual effect"))
        }
    }
}
#[derive(Default)]
struct Typed {
    seen: Mutex<Vec<(Moment, HookPayload)>>,
    results: Mutex<Vec<HookPayload>>,
    published: Mutex<Vec<(Moment, AuditBody)>>,
}
impl Auditor for Typed {
    fn is_enabled(&self) -> bool {
        true
    }
    fn publish(&self, moment: Moment, body: AuditBody) {
        self.published.lock().unwrap().push((moment, body.clone()));
        if moment == Moment::ToolResult {
            self.results.lock().unwrap().push(body.payload);
        }
    }
    fn intervene<'a>(
        &'a self,
        moment: Moment,
        ctx: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
        assert_ne!(
            moment,
            Moment::ToolResult,
            "tool results are observation-only"
        );
        self.seen
            .lock()
            .unwrap()
            .push((moment, ctx.payload.clone()));
        Box::pin(async move {
            let variables = match moment {
                Moment::InputAdmitted => {
                    assert!(
                        matches!(ctx.payload, HookPayload::InputAdmitted { ref parts } if parts == &vec![InputPart::Text { text: "run effect".into() }])
                    );
                    VariablesChange {
                        scoped_set: vec![],
                        set: json!({"admitted":true}).as_object().unwrap().clone(),
                        remove: vec![],
                    }
                }
                Moment::ToolCall => {
                    assert!(
                        matches!(ctx.payload, HookPayload::ToolCall { ref name, ref arguments } if name == "effect" && arguments == &json!({"value":7}))
                    );
                    VariablesChange {
                        scoped_set: vec![],
                        set: json!({"policy_ready":true}).as_object().unwrap().clone(),
                        remove: vec![],
                    }
                }
                Moment::RoundCompleted => {
                    assert!(matches!(
                        ctx.payload,
                        HookPayload::RoundCompleted {
                            response: Some(_),
                            usage: Some(_),
                            ..
                        }
                    ));
                    VariablesChange::default()
                }
                _ => VariablesChange::default(),
            };
            Ok(Injection {
                variables,
                ..Default::default()
            })
        })
    }
}

#[tokio::test]
async fn typed_hooks_apply_before_permission_and_tool_results_are_sanitized_observations_only() {
    for fail in [false, true] {
        let auditor = Arc::new(Typed::default());
        let client = Arc::new(Calls {
            seen: Mutex::new(vec![]),
        });
        let effects = Arc::new(AtomicUsize::new(0));
        let runtime = Runtime::new(
            vec![ToolRegistration::External(Arc::new(Effect {
                fail,
                calls: effects.clone(),
            }))],
            Arc::new(microcrab_session::runtime::tool::AllowTools::new([
                "effect",
            ])),
        )
        .unwrap();
        let session = Session::builder_arc(client.clone(), runtime)
            .auditor(auditor.clone())
            .build()
            .unwrap();
        assert_eq!(session.submit("run effect").await.unwrap().content, "done");
        assert_eq!(effects.load(Ordering::SeqCst), 1);
        session.close().await.unwrap();
        let seen = auditor.seen.lock().unwrap();
        assert_eq!(
            seen.iter().map(|(moment, _)| *moment).collect::<Vec<_>>(),
            vec![
                Moment::SessionCreated,
                Moment::InputAdmitted,
                Moment::RoundCompleted,
                Moment::ToolCall,
                Moment::RoundCompleted,
                Moment::SessionTerminated,
            ]
        );
        let published = auditor.published.lock().unwrap();
        let tool_records: Vec<_> = published
            .iter()
            .filter(|(moment, _)| matches!(moment, Moment::ToolCall | Moment::ToolResult))
            .collect();
        assert_eq!(tool_records.len(), 2);
        let start = &tool_records[0].1;
        let end = &tool_records[1].1;
        assert_eq!(tool_records[0].0, Moment::ToolCall);
        assert_eq!(tool_records[1].0, Moment::ToolResult);
        assert_eq!(start.tool_call_id.as_deref(), Some("typed-call"));
        assert_eq!(end.tool_call_id, start.tool_call_id);
        assert!(start.call_id.is_none() && end.call_id.is_none());
        assert!(start.started_at_unix_ms.is_some());
        assert_eq!(start.started_at_unix_ms, end.started_at_unix_ms);
        assert!(start.elapsed_ms.is_none());
        assert!(end.elapsed_ms.is_some());
        assert_eq!(
            (start.epoch, start.turn_id, start.round),
            (end.epoch, end.turn_id, end.round)
        );
        let results = auditor.results.lock().unwrap();
        assert_eq!(results.len(), 1);
        let HookPayload::ToolResult { result, error } = &results[0] else {
            panic!("typed tool result required")
        };
        let output = format!(
            "{}{}",
            result.as_deref().unwrap_or_default(),
            error.as_deref().unwrap_or_default()
        );
        assert!(!output.contains("private-tool-secret"));
        assert!(!output.contains("private-tool-body"));
        if fail {
            assert!(output.contains("effect storage write failed"));
            let diagnostic: serde_json::Value =
                serde_json::from_str(error.as_deref().unwrap()).unwrap();
            assert_eq!(diagnostic["diagnostic"]["code"], "tool_failed");
            assert_eq!(diagnostic["diagnostic"]["layer"], "runtime");
            assert_eq!(diagnostic["diagnostic"]["phase"], "unknown");
            assert_eq!(diagnostic["diagnostic"]["effects"], "unknown");
            assert!(result.is_none());
        } else {
            assert!(output.contains("actual effect"));
        }
        let requests = client.seen.lock().unwrap();
        assert_eq!(requests[0].system, requests[1].system);
        assert_eq!(requests[0].tools, requests[1].tools);
    }
}

struct CauseEffect {
    make_error: fn() -> Error,
}
#[async_trait::async_trait]
impl Tool for CauseEffect {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "effect".into(),
            description: "Fail with a known cause".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn execute(&self, _: Value, _: ToolContext) -> Result<ToolOutput, Error> {
        Err((self.make_error)())
    }
}

#[tokio::test]
async fn producer_causes_survive_real_tool_context_and_audit_without_legacy_secrets() {
    let cases: [(fn() -> Error, &str, &str); 3] = [
        (
            || {
                Error::denied_with_private(
                    "process belongs to another Session",
                    "private-tool-secret",
                )
            },
            "process belongs to another Session",
            "permission_denied",
        ),
        (
            || Error::tool_with_private("unknown tool", "private-tool-secret"),
            "unknown tool",
            "tool_failed",
        ),
        (
            || Error::audit_with_private("auditor flush failed", "private-tool-secret"),
            "auditor flush failed",
            "tool_failed",
        ),
    ];
    for (make_error, reason, code) in cases {
        let auditor = Arc::new(Typed::default());
        let client = Arc::new(Calls {
            seen: Mutex::new(vec![]),
        });
        let runtime = Runtime::new(
            vec![ToolRegistration::External(Arc::new(CauseEffect {
                make_error,
            }))],
            Arc::new(microcrab_session::runtime::tool::AllowTools::new([
                "effect",
            ])),
        )
        .unwrap();
        let session = Session::builder_arc(client.clone(), runtime)
            .auditor(auditor.clone())
            .build()
            .unwrap();
        session.submit("run effect").await.unwrap();
        let content = session
            .messages()
            .into_iter()
            .find_map(|item| match item {
                microcrab_session::context::ContentItem::Tool { content, .. } => Some(content),
                _ => None,
            })
            .unwrap();
        let result: Value = serde_json::from_str(&content).unwrap();
        assert_eq!(result["error"], reason);
        assert_eq!(result["diagnostic"]["reason"], reason);
        assert_eq!(result["diagnostic"]["code"], code);
        assert_eq!(result["diagnostic"]["effects"], "unknown");
        assert!(!content.contains("private-tool-secret"));
        {
            let results = auditor.results.lock().unwrap();
            let HookPayload::ToolResult {
                error: Some(error),
                result: None,
            } = &results[0]
            else {
                panic!("tool error observation required")
            };
            assert_eq!(*error, content);
        }
        session.close().await.unwrap();
    }
}
