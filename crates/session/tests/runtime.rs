use async_trait::async_trait;
use microcrab_auditor::{Auditor, ChannelAuditor};
use microcrab_protocol::{Response, ToolCall, ToolDefinition, Usage};
use microcrab_session::context::Context;
use microcrab_session::context::{Content, ContentItem};
use microcrab_session::runtime::execution::ToolOutput;
use microcrab_session::runtime::tool::{AllowTools, Tool, ToolContext};
use microcrab_session::runtime::{
    BuiltinTool, Runtime, RuntimeHandler, ToolOrigin, ToolRegistration, builtin_tools,
};
use microcrab_session::{Client, Error, Limits, SessionState};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

struct Once {
    response: std::sync::Mutex<Vec<Response>>,
}
#[async_trait::async_trait]
impl Client for Once {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let mut responses = self.response.lock().unwrap();
        if responses.is_empty() {
            return Err(microcrab_transport::Error::Config(
                "unexpected extra model request".into(),
            ));
        }
        Ok(responses.remove(0))
    }
}

struct Echo;
#[async_trait]
impl Tool for Echo {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "echo".into(),
            description: "Echo".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn execute(&self, _: Value, _: ToolContext) -> Result<ToolOutput, Error> {
        Ok(ToolOutput::text("observed"))
    }
}

#[tokio::test]
async fn textual_conversion_preserves_original_call_and_matches_model_feedback() {
    use microcrab_auditor::{AuditBody, Moment};
    use microcrab_protocol::HookPayload;
    use std::sync::Mutex;

    struct Capture(Mutex<Vec<(Moment, HookPayload)>>);
    impl Auditor for Capture {
        fn is_enabled(&self) -> bool { true }
        fn publish(&self, moment: Moment, body: AuditBody) {
            if matches!(moment, Moment::ToolCall | Moment::ToolResult) {
                self.0.lock().unwrap().push((moment, body.payload));
            }
        }
    }
    struct Exact(Arc<Mutex<Vec<Value>>>);
    #[async_trait]
    impl Tool for Exact {
        fn definition(&self) -> ToolDefinition {
            ToolDefinition {name:"exact".into(), description:"accept typed input".into(), parameters:json!({
                "type":"object", "properties":{"count":{"type":"integer","x-microcrab-text-input":true},"ref":{"type":"string"}},
                "required":["count","ref"],"additionalProperties":false
            })}
        }
        async fn execute(&self, args: Value, _: ToolContext) -> Result<ToolOutput, Error> {
            self.0.lock().unwrap().push(args);
            Ok(ToolOutput::text("observed"))
        }
    }
    let valid = json!({"count":"42","ref":"literal"});
    let invalid = json!({"count":"42","ref":9.42e90});
    let seen = Arc::new(Mutex::new(Vec::new()));
    let runtime = Runtime::new(vec![ToolRegistration::External(Arc::new(Exact(seen.clone())))], Arc::new(AllowTools::new(["exact"]))).unwrap();
    let audit = Arc::new(Capture(Mutex::new(Vec::new())));
    let model = Once { response: Mutex::new(vec![
        Response {tool_calls: [(&valid,"valid"),(&invalid,"invalid")].into_iter().map(|(arguments,id)| ToolCall {
            id:id.into(), name:"exact".into(), arguments:arguments.clone(), provider_metadata:None
        }).collect(), content:String::new(),provider_metadata:None,usage:Usage::default()},
        Response {tool_calls:vec![], content:"done".into(),provider_metadata:None,usage:Usage::default()},
    ])};
    let session = microcrab_session::Session::builder(model,runtime).auditor(audit.clone()).build().unwrap();
    session.submit("exercise explicit conversion").await.unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![json!({"count":42,"ref":"literal"})]);
    let messages = session.messages();
    let feedback = messages.iter().filter_map(|item| match item {
        ContentItem::Tool {content,..} => Some(content.as_str()), _ => None
    }).collect::<Vec<_>>();
    let audited = audit.0.lock().unwrap();
    let authored = audited.iter().filter_map(|(_, payload)| match payload {
        HookPayload::ToolCall {arguments,..} => Some(arguments), _ => None
    }).collect::<Vec<_>>();
    assert_eq!(authored, vec![&valid,&invalid]);
    let results = audited.iter().filter_map(|(_,payload)| match payload {
        HookPayload::ToolResult {result,error} => result.as_deref().or(error.as_deref()), _ => None
    }).collect::<Vec<_>>();
    assert_eq!(results,feedback);
    assert!(feedback[0].contains("schema-opted-in"));
    assert!(feedback[1].contains("received JSON number"));
    assert!(feedback[1].contains("not_started"));
}

fn context_with(tools: Vec<ToolDefinition>) -> Context {
    let mut context = Context::new("system".into(), tools, 2 * 1024 * 1024).unwrap();
    context.admit_user("inspect", &json!({})).unwrap();
    context
}

#[test]
fn expand_advertises_portable_properties_without_root_selector_union() {
    let schema = BuiltinTool::Expand.definition().parameters;
    assert!(schema.get("oneOf").is_none());
    assert_eq!(schema["additionalProperties"], false);
    assert_eq!(schema["properties"]["ids"]["minItems"], 1);
    assert_eq!(schema["properties"]["ids"]["maxItems"], 128);
    assert_eq!(schema["properties"]["ids"]["uniqueItems"], true);
    assert_eq!(schema["properties"]["reference"]["minLength"], 1);
    let validator = jsonschema::validator_for(&schema).unwrap();
    for arguments in [json!({"ids":["section"]}), json!({"reference":"section"})] {
        assert!(validator.is_valid(&arguments));
    }
    for arguments in [
        json!({"ids":[]}),
        json!({"ids":["a","a"]}),
        json!({"reference":null}),
        json!({"reference":""}),
        json!({"extra":true}),
    ] {
        assert!(!validator.is_valid(&arguments));
    }
}

#[tokio::test]
async fn expand_selector_xor_is_a_concrete_pre_effect_diagnostic() {
    use microcrab_session::observation::diagnostics::{
        DiagnosticCode, DiagnosticLayer, DiagnosticPhase, EffectKnowledge,
    };
    for arguments in [
        json!({}),
        json!({"ids":["unknown"],"reference":"also-unknown"}),
    ] {
        let runtime = Runtime::new(
            vec![ToolRegistration::Builtin(BuiltinTool::Expand)],
            Arc::new(AllowTools::new(["expand"])),
        )
        .unwrap();
        let client = Arc::new(Once {
            response: std::sync::Mutex::new(vec![
                Response {
                    provider_metadata: None,
                    content: String::new(),
                    tool_calls: vec![ToolCall {
                        provider_metadata: None,
                        id: "expand-invalid".into(),
                        name: "expand".into(),
                        arguments,
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
        let session = microcrab_session::Session::builder_arc(client, runtime)
            .build()
            .unwrap();
        session.submit("expand selector validation").await.unwrap();
        let rows = session.execution_snapshots();
        assert_eq!(rows.len(), 1);
        let diagnostic = rows[0].diagnostic.as_ref().unwrap();
        assert_eq!(diagnostic.code, DiagnosticCode::InvalidArgument);
        assert_eq!(diagnostic.layer, DiagnosticLayer::Runtime);
        assert_eq!(diagnostic.phase, DiagnosticPhase::Validation);
        assert_eq!(diagnostic.effects, EffectKnowledge::NotStarted);
        assert_eq!(
            diagnostic.reason.as_ref().unwrap().as_str(),
            "expand requires exactly one of ids or reference"
        );
        assert!(rows[0].output.is_none());
        assert!(session.messages().iter().any(|item| matches!(item,
            ContentItem::Tool { call_id, content, .. } if call_id == "expand-invalid" && content == &diagnostic.result_json()
        )));
        session.close().await.unwrap();
    }
}

#[test]
fn builtin_catalog_is_only_the_defined_tools() {
    let runtime = Runtime::new(
        builtin_tools().into_iter().collect(),
        Arc::new(AllowTools::new(["delegate"])),
    )
    .unwrap();
    let names: Vec<_> = runtime
        .definitions()
        .into_iter()
        .map(|tool| tool.name)
        .collect();
    assert_eq!(names, ["delegate", "expand", "job", "sleep", "switch_mode"]);
    for name in &names {
        assert_eq!(runtime.origin(name), Some(ToolOrigin::Builtin));
        assert!(BuiltinTool::from_name(name).is_some());
    }
    assert!(BuiltinTool::from_name("echo").is_none());
}

#[test]
fn wasm_and_external_cannot_take_a_builtin_name() {
    let tool = Arc::new(Echo);
    let claimed = ToolDefinition {
        name: "sleep".into(),
        description: "not builtin".into(),
        parameters: json!({"type":"object"}),
    };
    struct Claimed(ToolDefinition);
    #[async_trait]
    impl Tool for Claimed {
        fn definition(&self) -> ToolDefinition {
            self.0.clone()
        }
        async fn execute(&self, _: Value, _: ToolContext) -> Result<ToolOutput, Error> {
            Ok(ToolOutput::text("no"))
        }
    }
    let claimed = Arc::new(Claimed(claimed));
    assert!(
        Runtime::new(
            vec![ToolRegistration::Wasm(claimed.clone())],
            Arc::new(AllowTools::new(["sleep"]))
        )
        .is_err()
    );
    assert!(
        Runtime::new(
            vec![ToolRegistration::External(claimed)],
            Arc::new(AllowTools::new(["sleep"]))
        )
        .is_err()
    );
    let runtime = Runtime::new(
        vec![ToolRegistration::External(tool)],
        Arc::new(AllowTools::new(["echo"])),
    )
    .unwrap();
    assert_eq!(runtime.origin("echo"), Some(ToolOrigin::External));
}

#[tokio::test]
async fn invalid_delegate_schema_records_failed_call_without_dispatch() {
    struct CountDelegates(std::sync::atomic::AtomicUsize);
    #[async_trait]
    impl microcrab_session::DelegateExecutor for CountDelegates {
        async fn execute(
            &self,
            _: &str,
            _: &str,
            _: Option<&str>,
            _: &str,
            _: Option<u64>,
            _: ToolContext,
        ) -> Result<String, Error> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok("unexpected delegation".into())
        }
    }
    let executor = Arc::new(CountDelegates(std::sync::atomic::AtomicUsize::new(0)));
    use microcrab_session::observation::diagnostics::{
        DiagnosticCode, DiagnosticLayer, DiagnosticPhase, EffectKnowledge, ToolDiagnostic,
    };
    let mut diagnostic = ToolDiagnostic {
        code: DiagnosticCode::InvalidArgument,
        layer: DiagnosticLayer::Runtime,
        phase: DiagnosticPhase::Validation,
        effects: EffectKnowledge::NotStarted,
        reason: Some(
            microcrab_session::observation::diagnostics::SafeDiagnosticReason::new(
                "tool arguments do not match the registered tool schema",
            ),
        ),
        json_line: None,
        json_column: None,
        operation_effects: None,
        operation: None,
        requested_timeout_ms: None,
        effective_timeout_ms: None,
        limit_owner: None,
        evidence_id: None,
        retryable: None,
        wasm_failure: None,
    };
    for arguments in [
        json!({"input":"look"}),
        json!({"target":"review"}),
        json!({"target":"","input":"look"}),
        json!({"target":"review","input":""}),
        json!({"target":"review","input":"look","timeout_ms":"PRIVATE_SCHEMA_VALUE_SENTINEL"}),
    ] {
        let runtime = Runtime::new(
            vec![ToolRegistration::Builtin(BuiltinTool::Delegate)],
            Arc::new(AllowTools::new(["delegate"])),
        )
        .unwrap()
        .with_delegate_executor(executor.clone());
        let call = ToolCall {
            provider_metadata: None,
            id: "invalid-delegate".into(),
            name: "delegate".into(),
            arguments,
        };
        let client = Arc::new(Once {
            response: std::sync::Mutex::new(vec![
                Response {
                    provider_metadata: None,
                    content: "working".into(),
                    tool_calls: vec![call.clone()],
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
        let session = microcrab_session::Session::builder_arc(client, runtime)
            .catalog(vec![microcrab_agent_types::AgentInfo {
                tool_media_output_bytes: Default::default(),
                name: "review".into(),
                modes: vec![microcrab_agent_types::AgentMode {
                    description: String::new(),
                    prompt: String::new(),
                    name: "review".into(),
                    role: microcrab_agent_types::AgentRole::Both,
                    permissions: vec![],
                }],
                tools: vec![],
                variables: Default::default(),
            }])
            .build()
            .unwrap();
        let before = session.context_snapshot().unwrap();
        let outcome = session.submit("invalid delegation").await.unwrap();
        assert_eq!(outcome.content, "done");
        assert_eq!(executor.0.load(std::sync::atomic::Ordering::SeqCst), 0);
        let after = session.context_snapshot().unwrap();
        assert_eq!(after.system, before.system);
        assert_eq!(after.tools, before.tools);
        assert_eq!(after.epoch, before.epoch);
        assert_eq!(
            serde_json::to_vec(&after.items[..before.items.len()]).unwrap(),
            serde_json::to_vec(&before.items).unwrap(),
        );
        let messages = session.messages();
        assert!(messages.iter().any(|item| matches!(
            item,
            ContentItem::Assistant { content, tool_calls, .. }
                if content == "working" && tool_calls == &vec![call.clone()]
        )));
        let results: Vec<_> = messages
            .iter()
            .filter_map(|item| match item {
                ContentItem::Tool { call_id, content, .. } if call_id == &call.id => Some(content),
                _ => None,
            })
            .collect();
        assert_eq!(results.len(), 1);
        assert!(results[0].contains("schema"));
        assert!(!results[0].contains("PRIVATE_SCHEMA_VALUE_SENTINEL"));
        let history = session.execution_snapshots();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].call_id, call.id);
        assert_eq!(history[0].tool, "delegate");
        assert_eq!(
            history[0].status,
            microcrab_session::runtime::execution::ExecutionStatus::Failed,
        );
        let actual = history[0].diagnostic.as_ref().unwrap();
        assert!(actual.reason.as_ref().unwrap().as_str().contains("schema"));
        diagnostic.reason = actual.reason.clone();
        assert_eq!(actual, &diagnostic);
        assert_eq!(results, vec![&diagnostic.result_json()]);
        assert!(history[0].output.is_none());
        session.close().await.unwrap();
    }
}

#[tokio::test]
async fn permission_denial_is_a_tool_result_and_does_not_execute_the_tool() {
    let runtime = Runtime::new(
        vec![ToolRegistration::External(Arc::new(Echo))],
        Arc::new(AllowTools::new(Vec::<String>::new())),
    )
    .unwrap();
    let state = SessionState::new(Limits::default());
    let mut context = context_with(runtime.definitions());
    let client = Once {
        response: std::sync::Mutex::new(vec![
            Response {
                provider_metadata: None,
                content: "working".into(),
                tool_calls: vec![ToolCall {
                    provider_metadata: None,
                    id: "call-one".into(),
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
    };
    let outcome = runtime
        .execute_turn(
            &state,
            &mut context,
            &client,
            &ChannelAuditor::disconnected(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(outcome.content, "done");
    let result = context
        .items()
        .iter()
        .find_map(|item| match item {
            ContentItem::Tool { call_id, content, .. } if call_id == "call-one" => {
                Some(serde_json::from_str::<Value>(content).unwrap())
            }
            _ => None,
        })
        .expect("denied invocation has a tool result");
    assert_eq!(result["diagnostic"]["code"], "permission_denied");
    assert_eq!(result["diagnostic"]["layer"], "runtime");
    assert_eq!(result["diagnostic"]["phase"], "authorization");
    assert!(
        result["diagnostic"]["reason"]
            .as_str()
            .is_some_and(|reason| !reason.is_empty())
    );
    assert!(result.get("error").and_then(Value::as_str).is_some());
}

#[tokio::test]
async fn allow_tools_job_conveniences_compile_to_whole_call_action_predicates() {
    use microcrab_session::policy::SessionPermission;

    use microcrab_permission::{Action, PermissionConfig, PermissionRequest, PolicySnapshot};
    let request = |call: ToolCall| PermissionRequest {
        subject: Default::default(),
        session_id: "s".into(),
        call_id: call.id,
        agent: String::new(),
        mode: String::new(),
        tool: call.name,
        arguments: call.arguments,
        revision: 1,
    };
    for (grants, expected) in [
        (vec!["job"], vec!["status", "read", "wait"]),
        (vec!["job:cancel"], vec!["cancel", "release"]),
        (vec!["job:write"], vec!["write"]),
        (
            vec!["job", "job:cancel", "job:write"],
            vec!["status", "read", "wait", "cancel", "release", "write"],
        ),
        (vec![], vec![]),
    ] {
        let policy = AllowTools::new(grants);
        let policy_snapshot = PolicySnapshot {
            revision: 1,
            config: PermissionConfig {
                overrides: policy.initial_overrides(),
                ..Default::default()
            },
        };
        for action in [
            "status", "read", "drain", "wait", "cancel", "release", "write", "kill", "unknown",
        ] {
            let call = ToolCall {
                provider_metadata: None,
                id: "permission-boundary".into(),
                name: "job".into(),
                arguments: json!({"id":"s/1", "action":action}),
            };
            assert_eq!(
                policy
                    .evaluate(&policy_snapshot, &request(call))
                    .await
                    .unwrap()
                    .action
                    == Action::Allow,
                expected.contains(&action),
                "unexpected grant for {action}",
            );
        }
        for arguments in [json!({"id":"s/1"}), json!({"id":"s/1", "action":true})] {
            let call = ToolCall {
                provider_metadata: None,
                id: "invalid-action".into(),
                name: "job".into(),
                arguments,
            };
            assert_eq!(
                policy
                    .evaluate(&policy_snapshot, &request(call))
                    .await
                    .unwrap()
                    .action,
                Action::Deny
            );
        }
    }
}

#[tokio::test]
async fn direct_tool_admission_requires_reserved_identity_and_binds_the_original_call() {
    use microcrab_session::policy::PermissionError;
    let runtime = Runtime::new(
        vec![ToolRegistration::External(Arc::new(Echo))],
        Arc::new(AllowTools::new(["echo"])),
    )
    .unwrap();
    let call = ToolCall {
        provider_metadata: None,
        id: "direct-proof".into(),
        name: "echo".into(),
        arguments: json!({"text":"original"}),
    };
    let issuer = Content {
        epoch: 0,
        subagent: None,
        system: "direct embedding".into(),
        items: vec![],
        tools: runtime.definitions(),
        max_output_tokens: 1024,
        max_bytes: 65536,
        turn_id: None,
        round: None,
    };
    let mut context = ToolContext::detached(
        SessionState::new(Limits::default()),
        CancellationToken::new(),
    );
    context.call_id = call.id.clone();
    context.tool = call.name.clone();
    assert!(matches!(
        context.ensure_admitted(None),
        Err(PermissionError::Denied)
    ));
    assert!(matches!(
        runtime.admit_invocation(&call, &mut context, &issuer).await,
        Err(PermissionError::InvalidConfiguration(_))
    ));
    context.ensure_detached_execution().unwrap();
    assert!(
        matches!(context.ensure_admitted(None), Err(PermissionError::Denied)),
        "reservation alone cannot grant authority"
    );
    runtime
        .admit_invocation(&call, &mut context, &issuer)
        .await
        .unwrap();
    context.ensure_admitted_arguments(&call.arguments).unwrap();
    assert!(matches!(
        context.ensure_admitted_arguments(&json!({"text":"changed"})),
        Err(PermissionError::ProofMismatch("original arguments differ"))
    ));
    let continuation = context.clone();
    continuation.ensure_admitted(None).unwrap();
    for (changed, reason) in [
        "session identity differs",
        "call identity differs",
        "tool identity differs",
        "execution identity differs",
        "agent identity differs",
        "mode identity differs",
        "invocation subject differs",
        "state owner differs",
    ]
    .into_iter()
    .enumerate()
    {
        let mut reused = continuation.clone();
        match changed {
            0 => reused.session_id.push_str("-other"),
            1 => reused.call_id.push_str("-other"),
            2 => reused.tool = "other".into(),
            3 => reused.execution_id = None,
            4 => reused.agent.push_str("-other"),
            5 => reused.mode = Some("other-mode".into()),
            6 => {
                reused.subject = microcrab_permission::InvocationSubject::AgentInit {
                    component: "other".into(),
                    generation: 1,
                    attempt: 1,
                }
            }
            _ => {
                reused.state = ToolContext::detached(
                    SessionState::new(Limits::default()),
                    CancellationToken::new(),
                )
                .state
            }
        }
        assert!(matches!(
            reused.ensure_admitted(None),
            Err(PermissionError::ProofMismatch(actual)) if actual == reason
        ));
    }
    assert!(matches!(
        context.ensure_admitted(Some("unregistered-component")),
        Err(PermissionError::ProofMismatch("component identity differs"))
    ));
    context
        .complete_detached_output(&ToolOutput::text("done"))
        .await
        .unwrap();
    context.cancellation.cancel();
    assert!(matches!(
        continuation.ensure_admitted(None),
        Err(PermissionError::Cancelled)
    ));
}
