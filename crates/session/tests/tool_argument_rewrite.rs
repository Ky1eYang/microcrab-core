//! Argument restoration is an execution boundary; model history retains authored fakes.
use async_trait::async_trait;
use microcrab_auditor::{
    AuditBody, Auditor, AuditorError, BoxFuture, HookContext, Injection, VariableView,
};
use microcrab_host_api::ShellRequest;
use microcrab_host_local::{LocalHost, ShellLimits};
use microcrab_protocol::{
    HookPayload, Moment, Response, ToolCall, ToolDefinition, VariablesChange,
};
use microcrab_session::context::{Content, ContentItem};
use microcrab_session::{Client, Error, Limits, Session};
use microcrab_session::{
    policy::SessionPermission,
    runtime::{
        BuiltinTool, Runtime, ToolRegistration,
        tool::{AllowTools, Tool, ToolContext},
    },
};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const FAKE: &str = "__sr_01020304__";
// Deliberately synthetic and not shaped like a usable credential.
const RESTORED: &str = "SYNTHETIC_RESTORE_FIXTURE_7E36";
const VARIABLE: &str = "rewrite_policy_ready";

struct ScriptClient {
    calls: Vec<ToolCall>,
    requests: Mutex<Vec<Content>>,
    attempts: AtomicUsize,
    request_prepared: bool,
    probe_tool_result: bool,
    backend_effects: AtomicUsize,
}
impl ScriptClient {
    fn new(calls: Vec<ToolCall>) -> Self {
        Self {
            calls,
            requests: Mutex::new(vec![]),
            attempts: AtomicUsize::new(0),
            request_prepared: false,
            probe_tool_result: false,
            backend_effects: AtomicUsize::new(0),
        }
    }
}
#[async_trait]
impl Client for ScriptClient {
    async fn complete(
        &self,
        content: &Content,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(content.clone());
        if self.request_prepared {
            // Exercise the exact Session-bound adapter handed to a transport,
            // without opening a socket or reaching the simulated send below.
            let authored = r#"{"messages":[{"role":"user","content":"fixture"}]}"#;
            auditor
                .intervene(
                    Moment::RequestPrepared,
                    HookContext {
                        epoch: content.epoch,
                        subagent: content.subagent.clone(),
                        turn: content.turn_id,
                        round: content.round,
                        call_id: Some("synthetic-request".into()),
                        request_body: Some(authored),
                        max_request_bytes: Some(content.max_bytes),
                        variables,
                        payload: HookPayload::None,
                    },
                )
                .await?;
        }
        if self.probe_tool_result {
            let rejected = auditor
                .intervene(
                    Moment::ToolResult,
                    HookContext {
                        epoch: content.epoch,
                        subagent: content.subagent.clone(),
                        turn: content.turn_id,
                        round: content.round,
                        call_id: None,
                        request_body: None,
                        max_request_bytes: None,
                        variables,
                        payload: HookPayload::ToolResult {
                            result: Some("done".into()),
                            error: None,
                        },
                    },
                )
                .await
                .unwrap_err();
            assert_eq!(rejected.0, "ToolResult is observation-only; use publish");
        }
        self.backend_effects.fetch_add(1, Ordering::SeqCst);
        Ok(Response {
            provider_metadata: None,
            content: if attempt == 0 { "prepare" } else { "done" }.into(),
            tool_calls: if attempt == 0 {
                self.calls.clone()
            } else {
                vec![]
            },
            usage: Default::default(),
        })
    }
}

struct Rewrite {
    moment: Moment,
    replacement: Value,
    interventions: Mutex<Vec<Moment>>,
    published: Mutex<Vec<(Moment, HookPayload)>>,
}
impl Rewrite {
    fn at(moment: Moment, replacement: Value) -> Self {
        Self {
            moment,
            replacement,
            interventions: Mutex::new(vec![]),
            published: Mutex::new(vec![]),
        }
    }
}
impl Auditor for Rewrite {
    fn is_enabled(&self) -> bool {
        true
    }
    fn publish(&self, moment: Moment, body: AuditBody) {
        self.published.lock().unwrap().push((moment, body.payload));
    }
    fn intervene<'a>(
        &'a self,
        moment: Moment,
        _: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
        self.interventions.lock().unwrap().push(moment);
        Box::pin(async move {
            if moment != self.moment {
                return Ok(Injection::default());
            }
            Ok(Injection {
                tool_arguments: Some(self.replacement.clone()),
                variables: VariablesChange {
                    scoped_set: vec![],
                    set: json!({"rewrite_policy_ready": true})
                        .as_object()
                        .unwrap()
                        .clone(),
                    remove: vec![],
                },
                ..Default::default()
            })
        })
    }
}

#[derive(Debug, PartialEq)]
struct ExecutionObservation {
    tool: String,
    call_id: String,
    arguments: Value,
    variable: Option<Value>,
}

#[derive(Default)]
struct Effects {
    permission: Mutex<Vec<(ToolCall, Option<Value>)>>,
    execution: Mutex<Vec<ExecutionObservation>>,
}
struct InspectPermission(Arc<Effects>);
#[async_trait]
impl SessionPermission for InspectPermission {
    async fn evaluate(
        &self,
        _: &microcrab_permission::PolicySnapshot,
        _: &microcrab_permission::PermissionRequest,
    ) -> Result<microcrab_permission::Decision, microcrab_session::policy::PermissionError> {
        panic!("adapter evaluate is not authorization authority")
    }
    fn features(&self) -> microcrab_permission::PermissionFeatures {
        microcrab_permission::PermissionFeatures {
            interactive_ask: true,
            ..Default::default()
        }
    }
    async fn ask(
        &self,
        request: &microcrab_permission::PermissionRequest,
    ) -> Result<microcrab_permission::AskReply, microcrab_session::policy::PermissionError> {
        self.0.permission.lock().unwrap().push((
            ToolCall {
                provider_metadata: None,
                id: request.call_id.clone(),
                name: request.tool.clone(),
                arguments: request.arguments.clone(),
            },
            None,
        ));
        Ok(microcrab_permission::AskReply {
            action: microcrab_permission::Action::Allow,
            reason: "synthetic fixture approval".into(),
        })
    }
}
fn rewrite_permissions() -> microcrab_permission::PermissionConfig {
    microcrab_permission::PermissionConfig {
        review: false,
        overrides: vec![
            serde_json::from_value(
                json!({"tool":{"kind":"exact","value":"synthetic_effect"},"action":"ask"}),
            )
            .unwrap(),
        ],
        ..Default::default()
    }
}
struct Effect(Arc<Effects>);
#[async_trait]
impl Tool for Effect {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "synthetic_effect".into(),
            description: "Record an explicitly synthetic execution fixture".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn execute(
        &self,
        arguments: Value,
        context: ToolContext,
    ) -> Result<microcrab_session::runtime::execution::ToolOutput, Error> {
        let variable = context.state.extract(VARIABLE);
        self.0.execution.lock().unwrap().push(ExecutionObservation {
            tool: context.tool,
            call_id: context.call_id,
            arguments,
            variable,
        });
        Ok(microcrab_session::runtime::execution::ToolOutput::text(
            "done",
        ))
    }
}
fn effect_call(arguments: Value) -> ToolCall {
    ToolCall {
        provider_metadata: None,
        id: "model-call-17".into(),
        name: "synthetic_effect".into(),
        arguments,
    }
}
fn effect_runtime(effects: Arc<Effects>) -> Runtime {
    Runtime::new(
        vec![ToolRegistration::External(Arc::new(Effect(
            effects.clone(),
        )))],
        Arc::new(InspectPermission(effects)),
    )
    .unwrap()
}
fn assert_no_effects(effects: &Effects) {
    assert!(
        effects.permission.lock().unwrap().is_empty(),
        "permission must not run"
    );
    assert!(
        effects.execution.lock().unwrap().is_empty(),
        "tool execution must not run"
    );
}

#[tokio::test]
async fn permission_and_execution_receive_restored_nested_arguments_while_history_and_audit_keep_fakes()
 {
    let authored = json!({"nested":{"token":FAKE,"list":[FAKE,{"unchanged":7}]},"name":"argument-not-tool-name","id":"argument-not-call-id"});
    let restored = json!({"nested":{"token":RESTORED,"list":[RESTORED,{"unchanged":7}]},"name":"argument-not-tool-name","id":"argument-not-call-id"});
    let call = effect_call(authored.clone());
    let client = Arc::new(ScriptClient::new(vec![call.clone()]));
    let auditor = Arc::new(Rewrite::at(Moment::ToolCall, restored.clone()));
    let effects = Arc::new(Effects::default());
    let session = Session::builder_arc(client.clone(), effect_runtime(effects.clone()))
        .permissions(rewrite_permissions())
        .auditor(auditor.clone())
        .build()
        .unwrap();

    assert_eq!(
        session
            .submit("run synthetic fixture")
            .await
            .unwrap()
            .content,
        "done"
    );
    {
        let seen = effects.permission.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0.id, call.id);
        assert_eq!(seen[0].0.name, call.name);
        assert_eq!(seen[0].0.arguments, restored);
        assert_eq!(seen[0].1, None);
    }
    {
        let seen = effects.execution.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(
            seen[0],
            ExecutionObservation {
                tool: call.name.clone(),
                call_id: call.id.clone(),
                arguments: restored,
                variable: Some(json!(true)),
            }
        );
    }
    let messages = session.messages();
    let stored = messages
        .iter()
        .find_map(|item| match item {
            ContentItem::Assistant { tool_calls, .. } if !tool_calls.is_empty() => Some(tool_calls),
            _ => None,
        })
        .unwrap();
    assert_eq!(stored, &vec![call.clone()]);
    let tool_results = messages
        .iter()
        .filter_map(|item| match item {
            ContentItem::Tool { call_id, content } => Some((call_id.as_str(), content.as_str())),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(tool_results, vec![(call.id.as_str(), "done")]);
    {
        let published = auditor.published.lock().unwrap();
        let audited = published
            .iter()
            .filter_map(|(moment, payload)| match (moment, payload) {
                (Moment::ToolCall, HookPayload::ToolCall { name, arguments }) => {
                    Some((name, arguments))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(audited, vec![(&call.name, &authored)]);
        assert!(
            !serde_json::to_string(&*published)
                .unwrap()
                .contains(RESTORED)
        );
    }
    assert!(!serde_json::to_string(&messages).unwrap().contains(RESTORED));
    {
        let requests = client.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(
            !serde_json::to_string(&*requests)
                .unwrap()
                .contains(RESTORED)
        );
        assert!(serde_json::to_string(&requests[1]).unwrap().contains(FAKE));
    }
    session.close().await.unwrap();
}

#[tokio::test]
async fn non_object_replacements_fail_before_permission_execution_or_variable_commit() {
    for malformed in [
        json!([{"token":RESTORED}]),
        json!(RESTORED),
        json!(23),
        json!(false),
        Value::Null,
    ] {
        let client = Arc::new(ScriptClient::new(vec![effect_call(json!({"token":FAKE}))]));
        let effects = Arc::new(Effects::default());
        let session = Session::builder_arc(client, effect_runtime(effects.clone()))
            .permissions(rewrite_permissions())
            .auditor(Rewrite::at(Moment::ToolCall, malformed))
            .build()
            .unwrap();
        let result = session.submit("reject malformed rewrite").await;
        assert!(result.is_err(), "invalid rewrite must fail closed");
        assert_no_effects(&effects);
        assert_eq!(session.state_handle().extract(VARIABLE), None);
        let _ = session.close().await;
    }
}

#[tokio::test]
async fn wrong_moment_rewrites_fail_closed_before_variables_and_the_next_backend_effect() {
    for moment in [
        Moment::SessionCreated,
        Moment::InputAdmitted,
        Moment::RequestPrepared,
        Moment::RoundCompleted,
    ] {
        let mut scripted = ScriptClient::new(vec![effect_call(json!({"token":FAKE}))]);
        scripted.request_prepared = moment == Moment::RequestPrepared;
        let client = Arc::new(scripted);
        let effects = Arc::new(Effects::default());
        let auditor = Arc::new(Rewrite::at(moment, json!({"token":RESTORED})));
        let session = Session::builder_arc(client.clone(), effect_runtime(effects.clone()))
            .permissions(rewrite_permissions())
            .auditor(auditor.clone())
            .build()
            .unwrap();
        assert!(session.submit("reject wrong moment").await.is_err());
        assert_no_effects(&effects);
        assert_eq!(session.state_handle().extract(VARIABLE), None);
        // RoundCompleted follows one model result, but must block tool admission.
        // RequestPrepared must fail before our simulated send, just as a real
        // transport must fail its hook before opening an HTTP request.
        let expected = usize::from(moment == Moment::RoundCompleted);
        assert_eq!(client.backend_effects.load(Ordering::SeqCst), expected);
        assert!(auditor.interventions.lock().unwrap().contains(&moment));
        let _ = session.close().await;
    }
}

#[tokio::test]
async fn tool_result_stays_observation_only_even_when_a_transport_probes_intervention() {
    let call = effect_call(json!({"marker":"ordinary-argument"}));
    let mut scripted = ScriptClient::new(vec![call.clone()]);
    scripted.probe_tool_result = true;
    let auditor = Arc::new(Rewrite::at(Moment::ToolResult, json!({"token":RESTORED})));
    let effects = Arc::new(Effects::default());
    let session = Session::builder(scripted, effect_runtime(effects.clone()))
        .permissions(rewrite_permissions())
        .auditor(auditor.clone())
        .build()
        .unwrap();
    assert_eq!(
        session.submit("observe result").await.unwrap().content,
        "done"
    );
    assert_eq!(effects.permission.lock().unwrap()[0], (call.clone(), None));
    assert_eq!(
        effects.execution.lock().unwrap()[0].arguments,
        call.arguments
    );
    assert_eq!(session.state_handle().extract(VARIABLE), None);
    assert!(
        !auditor
            .interventions
            .lock()
            .unwrap()
            .contains(&Moment::ToolResult)
    );
    assert!(auditor.published.lock().unwrap().iter().any(|(moment,payload)|
        *moment == Moment::ToolResult && matches!(payload,HookPayload::ToolResult { result:Some(value),error:None } if value=="done")
    ));
    session.close().await.unwrap();
}

#[tokio::test]
async fn rewritten_job_release_rechecks_context_capacity_before_discarding_capture() {
    struct Queue(Mutex<std::collections::VecDeque<Vec<ToolCall>>>);
    #[async_trait]
    impl Client for Queue {
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
                tool_calls: self.0.lock().unwrap().pop_front().unwrap_or_default(),
                usage: Default::default(),
            })
        }
    }
    struct Seed;
    struct Finished;
    #[async_trait]
    impl microcrab_session::runtime::execution::ExecutionController for Finished {
        async fn cancel(&self, _: CancellationToken) -> Result<(), Error> {
            Ok(())
        }
        async fn release(&self, _: CancellationToken) -> Result<(), Error> {
            Ok(())
        }
    }
    #[async_trait]
    impl Tool for Seed {
        fn definition(&self) -> ToolDefinition {
            ToolDefinition {
                name: "seed_output".into(),
                description: "Retained native output".into(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn execute(
            &self,
            _: Value,
            context: ToolContext,
        ) -> Result<microcrab_session::runtime::execution::ToolOutput, Error> {
            let (writer, output) = context.open_output(Default::default(), Arc::new(Finished))?;
            writer.append(None, "retained-synthetic-output")?;
            writer.finish(Ok(()))?;
            Ok(microcrab_session::runtime::execution::ToolOutput::Buffered(
                output,
            ))
        }
    }
    struct JobRewrite {
        arguments: Mutex<Option<Value>>,
        context_bytes: usize,
        restored: Mutex<Vec<HookPayload>>,
        published: Mutex<Vec<HookPayload>>,
    }
    impl Auditor for JobRewrite {
        fn is_enabled(&self) -> bool {
            true
        }
        fn publish(&self, moment: Moment, body: AuditBody) {
            if moment == Moment::ToolCall {
                self.published.lock().unwrap().push(body.payload);
            }
        }
        fn intervene<'a>(
            &'a self,
            moment: Moment,
            context: HookContext<'a>,
        ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
            Box::pin(async move {
                if moment != Moment::ToolCall
                    || !matches!(&context.payload, HookPayload::ToolCall {name,..} if name == "job")
                {
                    return Ok(Injection::default());
                }
                self.restored.lock().unwrap().push(context.payload.clone());
                Ok(Injection {
                    tool_arguments: self.arguments.lock().unwrap().clone(),
                    variables: VariablesChange {
                        scoped_set: vec![],
                        set: json!({"rewritten_context_pressure":"x".repeat(self.context_bytes)})
                            .as_object()
                            .unwrap()
                            .clone(),
                        remove: vec![],
                    },
                    ..Default::default()
                })
            })
        }
    }
    for (context_bytes, following_drain, output_bytes, batch_ids) in [
        (64 * 1024, false, 1024, false),
        (2 * 1024 * 1024, true, 16 * 1024, false),
        (64 * 1024, false, 1024, true),
        (2 * 1024 * 1024, true, 16 * 1024, true),
    ] {
        let client = Arc::new(Queue(Mutex::new(std::collections::VecDeque::from([
            vec![ToolCall {
                provider_metadata: None,
                id: "seed".into(),
                name: "seed_output".into(),
                arguments: json!({}),
            }],
            vec![],
        ]))));
        let auditor = Arc::new(JobRewrite {
            arguments: Mutex::new(None),
            context_bytes,
            restored: Mutex::new(vec![]),
            published: Mutex::new(vec![]),
        });
        let runtime = Runtime::new(
            vec![
                ToolRegistration::Builtin(BuiltinTool::Job),
                ToolRegistration::External(Arc::new(Seed)),
            ],
            Arc::new(AllowTools::new(["job", "job:cancel", "seed_output"])),
        )
        .unwrap();
        let session = Session::builder_arc(client.clone(), runtime)
            .session_id("rewrite-budget")
            .auditor(auditor.clone())
            .limits(Limits {
                max_context_bytes: context_bytes,
                max_tool_output_bytes: output_bytes,
                ..Limits::default()
            })
            .build()
            .unwrap();
        session.submit("create retained output").await.unwrap();
        let row = session
            .execution_snapshots()
            .into_iter()
            .find(|row| row.tool == "seed_output")
            .unwrap();
        let address = format!("{}/{}", row.session_id, row.execution_id.0);
        let rewritten_id = if batch_ids {
            json!([address])
        } else {
            json!(address)
        };
        *auditor.arguments.lock().unwrap() = Some(json!({"id":rewritten_id,"action":"release"}));
        let authored = ToolCall {
            provider_metadata: None,
            id: "status-before-restoration".into(),
            name: "job".into(),
            arguments: json!({"id":address,"action":"status"}),
        };
        let mut calls = vec![authored.clone()];
        if following_drain {
            calls.push(ToolCall {
                provider_metadata: None,
                id: "pending-authored-drain".into(),
                name: "job".into(),
                arguments: json!({"id":address,"action":"release"}),
            });
        }
        client.0.lock().unwrap().extend([calls, vec![]]);
        let before = session
            .read_execution_output(row.execution_id, 0, 65536)
            .unwrap();
        assert!(
            session
                .submit("restore drain with an oversized boundary patch")
                .await
                .is_err()
        );
        let after = session
            .read_execution_output(row.execution_id, 0, 65536)
            .unwrap();
        assert_eq!(
            after
                .chunks
                .iter()
                .map(|c| c.content.as_str())
                .collect::<String>(),
            before
                .chunks
                .iter()
                .map(|c| c.content.as_str())
                .collect::<String>()
        );
        assert_eq!(
            after.cursor, before.cursor,
            "rejected rewritten call cannot consume output"
        );
        assert!(auditor.restored.lock().unwrap().iter().any(|payload| matches!(payload, HookPayload::ToolCall {name,arguments} if name == "job" && arguments == &authored.arguments)), "the actual restoration hook must see the authored status call");
        assert!(
            !auditor
                .published
                .lock()
                .unwrap()
                .iter()
                .any(|payload| matches!(payload, HookPayload::ToolCall {name,..} if name == "job")),
            "an oversized boundary patch is rejected before publishing dispatch"
        );
        session.close().await.unwrap();
    }
}

#[tokio::test]
async fn rewriting_a_finite_sleep_to_job_wait_defers_it_once_until_a_sibling_launches() {
    struct WaitRewrite(Arc<AtomicUsize>);
    impl Auditor for WaitRewrite {
        fn is_enabled(&self) -> bool {
            true
        }
        fn publish(&self, _: Moment, _: AuditBody) {}
        fn intervene<'a>(
            &'a self,
            moment: Moment,
            context: HookContext<'a>,
        ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
            Box::pin(async move {
                if moment != Moment::ToolCall
                    || !matches!(&context.payload,HookPayload::ToolCall {name,..} if name=="sleep")
                {
                    return Ok(Injection::default());
                }
                self.0.fetch_add(1, Ordering::SeqCst);
                let previous = context
                    .variables
                    .get("synthetic_wait_preparations")
                    .and_then(|value| value.as_u64())
                    .unwrap_or(0);
                Ok(Injection {
                    tool_arguments: Some(json!({"time_ms":-1,"reason":"wait for sibling fixture"})),
                    variables: VariablesChange {
                        scoped_set: vec![],
                        set: json!({"synthetic_wait_preparations":previous+1})
                            .as_object()
                            .unwrap()
                            .clone(),
                        remove: vec![],
                    },
                    ..Default::default()
                })
            })
        }
    }
    struct Launch(Arc<AtomicUsize>);
    #[async_trait]
    impl Tool for Launch {
        fn definition(&self) -> ToolDefinition {
            ToolDefinition {
                name: "launch_fixture".into(),
                description: "Start a bounded synthetic local job".into(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn execute(
            &self,
            _: Value,
            context: ToolContext,
        ) -> Result<microcrab_session::runtime::execution::ToolOutput, Error> {
            assert_eq!(
                context.state.extract("synthetic_wait_preparations"),
                Some(json!(1))
            );
            self.0.fetch_add(1, Ordering::SeqCst);
            let command = if cfg!(windows) {
                "Start-Sleep -Milliseconds 300"
            } else {
                "sleep 0.3"
            };
            let handle = context
                .processes
                .launch_shell(
                    &context.tool,
                    &context.call_id,
                    ShellRequest {
                        command: command.into(),
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
            Ok(microcrab_session::runtime::execution::ToolOutput::Buffered(
                context
                    .adopt_process_output(handle, vec!["stdout".into(), "stderr".into()], true)
                    .await?,
            ))
        }
    }
    let root = tempfile::tempdir().unwrap();
    let host = Arc::new(LocalHost::new(root.path(), ShellLimits::default()).unwrap());
    let preparations = Arc::new(AtomicUsize::new(0));
    let launched = Arc::new(AtomicUsize::new(0));
    let runtime = Runtime::new(
        vec![
            ToolRegistration::Builtin(BuiltinTool::Sleep),
            ToolRegistration::External(Arc::new(Launch(launched.clone()))),
        ],
        Arc::new(AllowTools::new(["sleep", "launch_fixture"])),
    )
    .unwrap();
    // Authored order puts the finite sleep first. Only its hook reveals that it
    // needs a background job registered by the next sibling.
    let client = ScriptClient::new(vec![
        ToolCall {
            provider_metadata: None,
            id: "rewritten-wait".into(),
            name: "sleep".into(),
            arguments: json!({"time_ms":1000}),
        },
        ToolCall {
            provider_metadata: None,
            id: "sibling-launch".into(),
            name: "launch_fixture".into(),
            arguments: json!({}),
        },
    ]);
    let session = Session::builder(client, runtime)
        .host(host)
        .auditor(WaitRewrite(preparations.clone()))
        .build()
        .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        session.submit("start and await synthetic sibling job"),
    )
    .await;
    // Attempt process cleanup before assertions about the queue outcome.
    let messages = session.messages();
    let committed = session
        .state_handle()
        .extract("synthetic_wait_preparations");
    let closed = session.close().await;
    assert_eq!(
        result
            .expect("deferred wait must terminate")
            .unwrap()
            .content,
        "done"
    );
    closed.unwrap();
    assert_eq!(launched.load(Ordering::SeqCst), 1);
    assert_eq!(
        preparations.load(Ordering::SeqCst),
        1,
        "deferring a prepared call must not invoke its hook again"
    );
    assert_eq!(committed, Some(json!(1)));
    let wait_result = messages
        .iter()
        .find_map(|item| match item {
            ContentItem::Tool { call_id, content } if call_id == "rewritten-wait" => Some(content),
            _ => None,
        })
        .expect("rewritten wait must have a settled result");
    let wait_result: Value = serde_json::from_str(wait_result).unwrap();
    assert_eq!(
        wait_result["success"], true,
        "rewritten wait must observe the sibling's running job"
    );
    assert_eq!(wait_result["cancelled"], false);
}
