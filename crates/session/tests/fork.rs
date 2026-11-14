//! Fork preserves the materialized provider prefix but owns new execution wiring.
use async_trait::async_trait;
#[path = "support/images.rs"]
mod image_fixture;
use microcrab_auditor::{Auditor, VariableView};
use microcrab_protocol::{Response, ToolCall, ToolDefinition};
use microcrab_session::runtime::execution::ToolOutput;
use microcrab_session::{
    Client, Error, Limits, Session, SessionBuilder,
    context::{Content, Context},
    runtime::{
        Runtime, ToolRegistration,
        journal::{MemoryJournal, Recover},
        tool::{AllowTools, Tool, ToolContext},
    },
};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Default)]
struct Answers {
    calls: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<Content>>>,
    first_tool: bool,
}
#[async_trait]
impl Client for Answers {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.requests.lock().unwrap().push(content.clone());
        let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
        Ok(Response {
            provider_metadata: None,
            content: "answer ✓".into(),
            tool_calls: if self.first_tool && first {
                vec![ToolCall {
                    provider_metadata: None,
                    id: "original-call".into(),
                    name: "echo".into(),
                    arguments: json!({"value":"你好"}),
                }]
            } else {
                vec![]
            },
            usage: Default::default(),
        })
    }
}
struct Echo(&'static str);
#[async_trait]
impl Tool for Echo {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "echo".into(),
            description: self.0.into(),
            parameters: json!({"type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false}),
        }
    }
    async fn execute(&self, value: Value, _: ToolContext) -> Result<ToolOutput, Error> {
        Ok(ToolOutput::text(value.to_string()))
    }
}
fn builder(id: &str, client: Answers, description: &'static str) -> SessionBuilder {
    Session::builder(
        client,
        Runtime::new(
            vec![ToolRegistration::External(Arc::new(Echo(description)))],
            Arc::new(AllowTools::new(["echo"])),
        )
        .unwrap(),
    )
    .session_id(id)
}
fn snapshot_value(session: &Session) -> Value {
    serde_json::to_value(session.context_snapshot().unwrap()).unwrap()
}
fn request(snapshot: microcrab_session::context::ContextSnapshot) -> Value {
    let context = Context::from_snapshot(snapshot).unwrap();
    serde_json::to_value(microcrab_transport::render_ast(&context.content(1024).unwrap()).unwrap())
        .unwrap()
}

#[tokio::test]
async fn fork_preserves_complete_frozen_prefix_with_old_epoch_tool_witnesses() {
    let journal = MemoryJournal::new();
    let source = builder(
        "origin",
        Answers {
            first_tool: true,
            ..Default::default()
        },
        "old schema witness",
    )
    .system("Rendered SYSTEM must survive exactly: 你好 ✓")
    .journal(journal.clone())
    .build()
    .unwrap();
    source
        .state_handle()
        .inject("custom", json!({"nested":["值",1]}))
        .await
        .unwrap();
    source
        .submit(microcrab_protocol::SessionInput {
            parts: vec![
                microcrab_protocol::InputPart::Text {
                    text: "first user".into(),
                },
                microcrab_protocol::InputPart::Image {
                    source: microcrab_protocol::ImageSource::Data {
                        media_type: "image/png".into(),
                        data: image_fixture::png(),
                    },
                },
            ],
            variables: Default::default(),
        })
        .await
        .unwrap();
    source
        .replace_external_tools(vec![Arc::new(Echo("current frozen schema"))])
        .await
        .unwrap();
    source.submit("second epoch user").await.unwrap();
    let before = snapshot_value(&source);
    assert!(before["epoch"].as_u64().unwrap() > 0);
    assert!(
        !source
            .context_snapshot()
            .unwrap()
            .historical_tools
            .is_empty()
    );
    let source_checkpoint = serde_json::to_value(journal.latest().unwrap()).unwrap();
    let observer = Answers::default();
    let destination_journal = MemoryJournal::new();
    let target = source
        .fork(
            builder("fork", observer.clone(), "current frozen schema")
                .system("This replacement system must never enter the restored prefix")
                .journal(destination_journal.clone()),
        )
        .await
        .unwrap();
    assert_eq!(snapshot_value(&target), before);
    assert_eq!(
        request(target.context_snapshot().unwrap()),
        request(source.context_snapshot().unwrap())
    );
    assert_eq!(
        serde_json::to_value(journal.latest().unwrap()).unwrap(),
        source_checkpoint
    );
    assert!(destination_journal.latest().is_none());
    assert!(!Arc::ptr_eq(&source.executions(), &target.executions()));
    assert!(!Arc::ptr_eq(
        &source.permission_controller(),
        &target.permission_controller()
    ));
    assert!(!Arc::ptr_eq(
        &source.process_control(),
        &target.process_control()
    ));
    assert!(target.executions().snapshot().is_empty());
    target.start().await.unwrap();
    target.checkpoint().await.unwrap();
    let root = destination_journal.latest().unwrap();
    assert!(root.continuation.is_none() && root.workspace.is_none() && root.cache_report.is_none());
    assert!(root.state.executions.is_empty());
    assert_eq!(serde_json::to_value(&root.context).unwrap(), before);
    target
        .state_handle()
        .inject("custom", json!({"independent":true}))
        .await
        .unwrap();
    assert_eq!(
        source.state().variables.as_value()["custom"],
        json!({"nested":["值",1]})
    );
    target.submit("new branch input").await.unwrap();
    let sent = observer.requests.lock().unwrap()[0].clone();
    let baseline = request(source.context_snapshot().unwrap());
    let actual = serde_json::to_value(microcrab_transport::render_ast(&sent).unwrap()).unwrap();
    assert_eq!(actual["tools"], baseline["tools"]);
    let prefix = baseline["messages"].as_array().unwrap();
    assert_eq!(
        &actual["messages"].as_array().unwrap()[..prefix.len()],
        prefix.as_slice()
    );
    assert_eq!(snapshot_value(&source), before);
    target.close().await.unwrap();
    source.close().await.unwrap();
}

#[tokio::test]
async fn fork_preserves_actual_openai_request_body_with_identical_model_settings() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let source = builder(
        "wire-origin",
        Answers {
            first_tool: true,
            ..Default::default()
        },
        "frozen provider schema",
    )
    .system("Exact SYSTEM\n  你好")
    .build()
    .unwrap();
    source
        .submit("original structured tool conversation")
        .await
        .unwrap();
    let target = source
        .fork(builder(
            "wire-fork",
            Answers::default(),
            "frozen provider schema",
        ))
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let server = tokio::spawn(async move {
        let mut captured = Vec::new();
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = Vec::new();
            let (start, length) = loop {
                let mut chunk = [0; 2048];
                let count = socket.read(&mut chunk).await.unwrap();
                assert!(count > 0);
                buffer.extend_from_slice(&chunk[..count]);
                if let Some(end) = buffer.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                    let length = String::from_utf8_lossy(&buffer[..end])
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|length| length.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    break (end + 4, length);
                }
            };
            while buffer.len() < start + length {
                let mut chunk = [0; 2048];
                let count = socket.read(&mut chunk).await.unwrap();
                assert!(count > 0);
                buffer.extend_from_slice(&chunk[..count]);
            }
            captured.push(buffer[start..start + length].to_vec());
            let response = br#"{"choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"wire accepted"}}]}"#;
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", response.len()).as_bytes()).await.unwrap();
            socket.write_all(response).await.unwrap();
        }
        captured
    });
    // One transport instance fixes model/settings. These calls examine the exact
    // frozen prefix before new input; they do not admit another Session turn.
    let provider =
        microcrab_transport::OpenAiCompatible::new(microcrab_transport::OpenAiCompatibleConfig {
            endpoint,
            model: "same-fork-model".into(),
            api_key: None,
            timeout_ms: 5_000,
            max_response_bytes: 4096,
        })
        .unwrap();
    for session in [&source, &target] {
        let content = Context::from_snapshot(session.context_snapshot().unwrap())
            .unwrap()
            .content(1024)
            .unwrap();
        provider
            .complete(
                &content,
                &microcrab_auditor::ChannelAuditor::disconnected(),
                &microcrab_auditor::EmptyVariableView,
                CancellationToken::new(),
            )
            .await
            .unwrap();
    }
    let captured = tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        captured[0], captured[1],
        "fork must preserve provider request bytes; no cache-hit outcome is inferred"
    );
    let body: Value = serde_json::from_slice(&captured[0]).unwrap();
    assert_eq!(body["model"], "same-fork-model");
    assert_eq!(body["messages"][0]["content"], "Exact SYSTEM\n  你好");
    assert_eq!(body["tools"][0]["function"]["name"], "echo");
    target.close().await.unwrap();
    source.close().await.unwrap();
}

#[tokio::test]
async fn fork_retains_pending_compaction_epoch_and_does_not_copy_suspended_turn() {
    let source = builder("compact-source", Answers::default(), "echo")
        .build()
        .unwrap();
    source.submit("before compact").await.unwrap();
    source.compact("retained summary", json!({})).await.unwrap();
    assert!(source.context_snapshot().unwrap().pending_epoch);
    let target = source
        .fork(builder("compact-fork", Answers::default(), "echo"))
        .await
        .unwrap();
    assert_eq!(snapshot_value(&source), snapshot_value(&target));
    target.close().await.unwrap();
    source.close().await.unwrap();
}

#[tokio::test]
async fn fork_requires_independent_identity_journal_and_job_scope() {
    let journal: Arc<dyn microcrab_session::Journal> = Arc::new(MemoryJournal::new());
    let source = builder("identity", Answers::default(), "echo")
        .journal_arc(journal.clone())
        .build()
        .unwrap();
    source.start().await.unwrap();
    assert!(
        source
            .fork(builder("identity", Answers::default(), "echo"))
            .await
            .is_err()
    );
    assert!(
        source
            .fork(builder("new-identity", Answers::default(), "echo").journal_arc(journal))
            .await
            .is_err()
    );
    assert!(
        source
            .fork(
                builder("new-identity", Answers::default(), "echo")
                    .processes_parent(source.process_control())
            )
            .await
            .is_err()
    );
    assert!(
        source
            .fork(
                builder("new-identity", Answers::default(), "echo")
                    .permission_parent(source.permission_controller())
            )
            .await
            .is_err()
    );
    source.close().await.unwrap();
}

#[tokio::test]
async fn fork_has_an_empty_process_pool_without_stopping_source_process() {
    use microcrab_host_api::ShellRequest;
    let root = tempfile::tempdir().unwrap();
    let host =
        Arc::new(microcrab_host_local::LocalHost::new(root.path(), Default::default()).unwrap());
    let source = builder("process-origin", Answers::default(), "echo")
        .host(host.clone())
        .build()
        .unwrap();
    source.start().await.unwrap();
    let command = if cfg!(windows) {
        "$null = [Console]::ReadLine()"
    } else {
        "read line"
    };
    let handle = source
        .process_control()
        .launch_shell(
            "fixture",
            "source-only",
            ShellRequest {
                command: command.into(),
                cwd: None,
                output_bytes: microcrab_host_local::ShellLimits::default().max_output_bytes,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let target = source
        .fork(builder("process-fork", Answers::default(), "echo").host(host))
        .await
        .unwrap();
    assert!(!target.process_control().has_unclosed());
    assert!(source.process_control().has_running());
    assert!(
        !target.process_control().owns_process(handle),
        "fork must not inherit raw Host ownership"
    );
    assert!(source.process_control().owns_process(handle));
    target.close().await.unwrap();
    assert!(source.process_control().has_running());
    source.close().await.unwrap();
}

struct HeldModel(Arc<tokio::sync::Notify>);
#[async_trait]
impl Client for HeldModel {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        cancel: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.0.notify_one();
        cancel.cancelled().await;
        Err(microcrab_transport::Error::Cancelled)
    }
}
#[tokio::test]
async fn fork_rejects_an_admitted_foreground_turn() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let source = Session::builder(
        HeldModel(entered.clone()),
        Runtime::new(
            vec![],
            Arc::new(AllowTools::new(std::iter::empty::<&str>())),
        )
        .unwrap(),
    )
    .session_id("busy-source")
    .build()
    .unwrap();
    let stream = source.stream("held turn").await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    assert!(matches!(
        source
            .fork(
                Session::builder(
                    Answers::default(),
                    Runtime::new(
                        vec![],
                        Arc::new(AllowTools::new(std::iter::empty::<&str>()))
                    )
                    .unwrap()
                )
                .session_id("busy-fork")
            )
            .await,
        Err(Error::Busy)
    ));
    drop(stream);
    source.close().await.unwrap();
}

struct StaticSkills;
impl microcrab_session::Reference for StaticSkills {
    type Error = std::convert::Infallible;
    fn kind(&self) -> &str {
        "skills"
    }
    fn load(&self) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(Ok(()))
    }
    fn list(&self) -> Result<Vec<microcrab_session::ReferenceInfo>, Self::Error> {
        Ok(vec![microcrab_session::ReferenceInfo {
            id: "skill#fork".into(),
            title: "Fork skill".into(),
            location: "fixture/skill.md".into(),
            description: "inherited catalog fixture".into(),
        }])
    }
    fn read(&self, id: &str) -> Result<Option<microcrab_session::ReferenceContent>, Self::Error> {
        Ok(
            (id == "skill#fork").then(|| microcrab_session::ReferenceContent {
                id: id.into(),
                body: "Inherited reference body ✓".into(),
            }),
        )
    }
    fn change(&self) -> Result<Option<Value>, Self::Error> {
        Ok(Some(json!({"catalog": self.list()?})))
    }
}
struct ExpandInherited(AtomicUsize);
#[async_trait]
impl Client for ExpandInherited {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        Ok(Response {
            provider_metadata: None,
            content: "expanded".into(),
            tool_calls: if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                vec![ToolCall {
                    provider_metadata: None,
                    id: "fork-expand".into(),
                    name: "expand".into(),
                    arguments: json!({"reference":"skill#fork"}),
                }]
            } else {
                vec![]
            },
            usage: Default::default(),
        })
    }
}
#[tokio::test]
async fn fork_without_reference_override_keeps_inherited_ids_expandable() {
    let runtime = || {
        Runtime::new(
            vec![ToolRegistration::Builtin(
                microcrab_session::runtime::BuiltinTool::Expand,
            )],
            Arc::new(AllowTools::new(["expand"])),
        )
        .unwrap()
    };
    let source = Session::builder(Answers::default(), runtime())
        .session_id("reference-origin")
        .references(vec![
            microcrab_session::DynReference::new(StaticSkills).unwrap(),
        ])
        .build()
        .unwrap();
    source
        .submit("retain the reference catalog in materialized State")
        .await
        .unwrap();
    let before = snapshot_value(&source);
    let target = source
        .fork(
            Session::builder(ExpandInherited(AtomicUsize::new(0)), runtime())
                .session_id("reference-fork"),
        )
        .await
        .unwrap();
    assert_eq!(snapshot_value(&target), before);
    target.start().await.unwrap();
    assert_eq!(
        target
            .state_handle()
            .read_reference("skill#fork")
            .unwrap()
            .body,
        "Inherited reference body ✓"
    );
    assert_eq!(target.state_handle().list_references().unwrap().len(), 1);
    target.submit("expand the inherited skill").await.unwrap();
    assert!(target.messages().iter().any(|item| matches!(item, microcrab_session::context::ContentItem::Tool { call_id, content } if call_id == "fork-expand" && content.contains("Inherited reference body ✓"))));
    assert_eq!(snapshot_value(&source), before);
    assert!(!Arc::ptr_eq(
        &source.process_control(),
        &target.process_control()
    ));
    target.close().await.unwrap();
    source.close().await.unwrap();
}

struct PendingTool;
#[async_trait]
impl Tool for PendingTool {
    fn definition(&self) -> ToolDefinition {
        Echo("pending").definition()
    }
    async fn execute(&self, _: Value, context: ToolContext) -> Result<ToolOutput, Error> {
        let token = CancellationToken::new();
        let (writer, output) = context.open_output(
            Default::default(),
            Arc::new(
                microcrab_session::runtime::execution::CancellationController::new(token.clone()),
            ),
        )?;
        tokio::spawn(async move {
            token.cancelled().await;
            writer
                .finish(Err(microcrab_session::runtime::execution::ToolFailure {
                    kind: microcrab_session::runtime::execution::ToolFailureKind::Cancelled,
                    code: "fixture_cancelled".into(),
                    message: "Fixture cancelled".into(),
                    retryable: false,
                }))
                .unwrap();
        });
        Ok(ToolOutput::Buffered(output))
    }
}
#[tokio::test]
async fn fork_rejects_pending_invocations_even_after_foreground_turn_finished() {
    let source = Session::builder(
        Answers {
            first_tool: true,
            ..Default::default()
        },
        Runtime::new(
            vec![ToolRegistration::External(Arc::new(PendingTool))],
            Arc::new(AllowTools::new(["echo"])),
        )
        .unwrap(),
    )
    .session_id("pending-origin")
    .limits(Limits {
        ..Default::default()
    })
    .build()
    .unwrap();
    source.submit("leave future running").await.unwrap();
    assert!(source.executions().has_pending());
    assert!(
        source
            .fork(
                builder("pending-fork", Answers::default(), "pending").limits(Limits {
                    ..Default::default()
                })
            )
            .await
            .is_err()
    );
    source.close().await.unwrap();
}

#[tokio::test]
async fn fork_rejects_suspended_recovery_continuation() {
    let limits = Limits::default();
    let mut context = Context::new(
        "frozen".into(),
        vec![Echo("echo").definition()],
        limits.max_context_bytes,
    )
    .unwrap();
    context.admit_user("suspended", &json!({})).unwrap();
    let mut state = microcrab_session::SessionState::new(limits);
    state.turn_id = Some(1);
    let source = Session::restore(
        builder("suspended", Answers::default(), "echo"),
        Recover {
            state,
            context: context.snapshot().unwrap(),
            continuation: Some(microcrab_session::runtime::journal::RoundContinuation {
                next_round: 1,
                unattended_rounds: 0,
                usage: Default::default(),
                request_prepared: false,
                state_observation_pending: false,
            }),
            workspace: None,
            cache_report: None,
            agent: None,
        },
    )
    .await
    .unwrap();
    assert!(
        source
            .fork(builder("suspended-fork", Answers::default(), "echo"))
            .await
            .is_err()
    );
    source.close().await.unwrap();
}
