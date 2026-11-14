#![cfg(feature = "wasm")]
//! Current guest resource errors preserve trusted Host facts in Context and audit.
use microcrab_host_api::{
    Host, HostAPI, HostError, HostInfo, OpContext, ShellRequest, ToolError, process::ProcessHandle,
};
use microcrab_protocol::{Response, ToolCall, Usage};
use microcrab_session::{
    AuditBody, Auditor, Client, Moment, Session,
    context::{Content, ContentItem},
    runtime::{
        Runtime, ToolRegistration,
        tool::AllowTools,
        wasm::{WasmAgent, WasmLimits},
    },
};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio_util::sync::CancellationToken;

struct RejectLaunch {
    local: Arc<microcrab_host_local::LocalHost>,
    category: microcrab_host_api::ErrorCategory,
    retryable: bool,
}
#[async_trait::async_trait]
impl microcrab_host_api::ProcessApi for RejectLaunch {
    async fn status(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
    ) -> Result<microcrab_host_api::process::ProcessStatus, HostError> {
        microcrab_host_api::ProcessApi::status(self.local.as_ref(), ctx, handle).await
    }
    fn watch(&self, handle: ProcessHandle) -> Result<tokio::sync::watch::Receiver<u64>, HostError> {
        microcrab_host_api::ProcessApi::watch(self.local.as_ref(), handle)
    }
}
impl microcrab_host_api::FsApi for RejectLaunch {}
impl microcrab_host_api::NetApi for RejectLaunch {}
#[async_trait::async_trait]
impl microcrab_host_api::ShellApi for RejectLaunch {
    async fn read(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
        position: Option<microcrab_host_api::ShellPosition>,
    ) -> Result<microcrab_host_api::ShellOutput, HostError> {
        microcrab_host_api::ShellApi::read(self.local.as_ref(), ctx, handle, position).await
    }
    async fn drain(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
        max_bytes: usize,
    ) -> Result<microcrab_host_api::ShellOutput, HostError> {
        microcrab_host_api::ShellApi::drain(self.local.as_ref(), ctx, handle, max_bytes).await
    }
    async fn launch(&self, _: &OpContext, _: ShellRequest) -> Result<ProcessHandle, HostError> {
        let mut error = HostError::new(
            self.category,
            "fixture shell launch failed; token=private-host-fixture-secret; response_body=private-host-fixture-body",
        );
        error.retryable = self.retryable;
        Err(error)
    }
}
impl HostAPI for RejectLaunch {
    fn capabilities(&self) -> &microcrab_host_api::Capabilities {
        static CAPABILITIES: std::sync::LazyLock<microcrab_host_api::Capabilities> =
            std::sync::LazyLock::new(|| microcrab_host_api::Capabilities {
                supported_operations: [
                    "shell.launch",
                    "shell.read",
                    "shell.drain",
                    "process.status",
                    "process.watch",
                ]
                .into_iter()
                .map(str::to_owned)
                .collect(),
                ..Default::default()
            });
        &CAPABILITIES
    }
}
#[async_trait::async_trait]
impl Host for RejectLaunch {
    fn info(&self) -> Result<HostInfo, ToolError> {
        self.local.info()
    }
    async fn normalize_resource(
        &self,
        path: PathBuf,
        cancel: CancellationToken,
    ) -> Result<String, ToolError> {
        self.local.normalize_resource(path, cancel).await
    }
}
#[derive(Default)]
struct Audit(Mutex<Vec<AuditBody>>);
impl Auditor for Audit {
    fn is_enabled(&self) -> bool {
        true
    }
    fn publish(&self, moment: Moment, body: AuditBody) {
        if moment == Moment::ToolResult {
            self.0.lock().unwrap().push(body);
        }
    }
}
struct Model {
    name: String,
    once: Mutex<bool>,
}
#[async_trait::async_trait]
impl Client for Model {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_session::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let mut once = self.once.lock().unwrap();
        let calls = if !*once {
            *once = true;
            vec![ToolCall {
                provider_metadata: None,
                id: "denied-launch".into(),
                name: self.name.clone(),
                arguments: json!({"command":"echo must-not-run", "yield_ms":0}),
            }]
        } else {
            vec![]
        };
        Ok(Response {
            provider_metadata: None,
            content: if calls.is_empty() {
                "done".into()
            } else {
                String::new()
            },
            tool_calls: calls,
            usage: Usage::default(),
        })
    }
}
#[tokio::test]
#[ignore = "build universal.wasm and set MICROCRAB_UNIVERSAL_WASM first"]
async fn current_guest_launch_failure_retains_host_facts_in_context_and_audit() {
    let path =
        std::env::var_os("MICROCRAB_UNIVERSAL_WASM").expect("set current guest component path");
    let root = tempfile::tempdir().unwrap();
    let host = Arc::new(RejectLaunch {
        local: Arc::new(
            microcrab_host_local::LocalHost::new(root.path(), Default::default()).unwrap(),
        ),
        category: microcrab_host_api::ErrorCategory::Permission,
        retryable: false,
    });
    let agent = WasmAgent::load(
        &std::fs::read(path).unwrap(),
        WasmLimits::default(),
        host.clone(),
    )
    .unwrap();
    let name = if cfg!(windows) { "pwsh" } else { "bash" };
    let audit = Arc::new(Audit::default());
    let runtime = Runtime::new(
        agent
            .tools()
            .into_iter()
            .map(ToolRegistration::Wasm)
            .collect(),
        Arc::new(AllowTools::new([name])),
    )
    .unwrap();
    let session = Session::builder(
        Model {
            name: name.into(),
            once: Mutex::new(false),
        },
        runtime,
    )
    .host(host)
    .permissions(serde_json::from_value(json!({"initialization":"allow"})).unwrap())
    .agent(agent.agents()[0].clone())
    .auditor(audit.clone())
    .build()
    .unwrap();
    session.start().await.unwrap();
    session.submit("Execute the denied launch").await.unwrap();
    let output = session
        .messages()
        .into_iter()
        .find_map(|item| match item {
            ContentItem::Tool { call_id, content } if call_id == "denied-launch" => Some(content),
            _ => None,
        })
        .unwrap();
    let value: Value = serde_json::from_str(&output).unwrap();
    assert_eq!(value["failure"]["code"], "host_permission");
    assert_eq!(value["failure"]["kind"], "failed");
    assert_eq!(value["failure"]["retryable"], false);
    assert!(
        value["failure"]["message"]
            .as_str()
            .unwrap()
            .contains("fixture shell launch failed")
    );
    assert!(!output.contains("private-host-fixture-secret"));
    assert!(!output.contains("private-host-fixture-body"));
    {
        let records = audit.0.lock().unwrap();
        let record = records
            .iter()
            .find(|r| r.tool_call_id.as_deref() == Some("denied-launch"))
            .unwrap();
        let payload = serde_json::to_value(&record.payload).unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(payload["result"].as_str().unwrap()).unwrap(),
            value
        );
    }
    assert!(!session.process_control().has_running());
    session.close().await.unwrap();
}

#[tokio::test]
#[ignore = "build universal.wasm and set MICROCRAB_UNIVERSAL_WASM first"]
async fn current_guest_preserves_host_failure_categories_and_retryability() {
    use crate::typed_agent_fixture::InitializedTool as _;
    use microcrab_host_api::ErrorCategory;
    use microcrab_session::{
        SessionState,
        runtime::{
            execution::{ToolFailureKind, ToolOutput},
            tool::ToolContext,
        },
    };

    let path =
        std::env::var_os("MICROCRAB_UNIVERSAL_WASM").expect("set current guest component path");
    let bytes = std::fs::read(path).unwrap();
    let root = tempfile::tempdir().unwrap();
    for (category, code, kind) in [
        (
            ErrorCategory::NotFound,
            "host_not_found",
            ToolFailureKind::Failed,
        ),
        (
            ErrorCategory::Permission,
            "host_permission",
            ToolFailureKind::Failed,
        ),
        (
            ErrorCategory::Timeout,
            "startup_timeout",
            ToolFailureKind::Timeout,
        ),
        (
            ErrorCategory::Cancelled,
            "host_cancelled",
            ToolFailureKind::Cancelled,
        ),
        (
            ErrorCategory::Conflict,
            "host_conflict",
            ToolFailureKind::Failed,
        ),
        (
            ErrorCategory::Unsupported,
            "host_unsupported",
            ToolFailureKind::Failed,
        ),
        (ErrorCategory::Io, "host_io", ToolFailureKind::Failed),
    ] {
        for retryable in [false, true] {
            let host = Arc::new(RejectLaunch {
                local: Arc::new(
                    microcrab_host_local::LocalHost::new(root.path(), Default::default()).unwrap(),
                ),
                category,
                retryable,
            });
            let agent = WasmAgent::load(&bytes, WasmLimits::default(), host.clone()).unwrap();
            let tool = agent.tools().remove(0);
            let mut context = ToolContext::detached(
                SessionState::new(Default::default()),
                CancellationToken::new(),
            );
            context.processes =
                microcrab_session::runtime::execution::process::ProcessRegistry::new(
                    context.session_id.clone(),
                    Some(host.clone()),
                    context.state.snapshot().limits.clone(),
                );
            let result = tool
                .execute_initialized(json!({"command":"must-not-run","yield_ms":0}), context)
                .await;
            assert!(!microcrab_host_api::ProcessApi::has_running_processes(
                host.local.as_ref()
            ));
            // Cancellation remains Runtime-owned even when the guest returns a failure.
            if category == ErrorCategory::Cancelled {
                assert!(matches!(result, Err(microcrab_session::Error::Cancelled)));
                continue;
            }
            // Other Host failures retain the guest's typed failure semantics.
            let ToolOutput::Final(output) = result.unwrap() else {
                panic!("failed launch must return Final")
            };
            let failure = output
                .failure
                .expect("failed launch must retain typed failure");
            assert_eq!(failure.code, code, "category {category:?}");
            assert_eq!(failure.kind, kind, "category {category:?}");
            assert_eq!(failure.retryable, retryable, "category {category:?}");
            assert!(failure.message.contains("fixture shell launch failed"));
            assert!(!failure.message.contains("private-host-fixture-secret"));
            assert!(!failure.message.contains("private-host-fixture-body"));
            assert!(!microcrab_host_api::ProcessApi::has_running_processes(
                host.local.as_ref()
            ));
        }
    }
}
