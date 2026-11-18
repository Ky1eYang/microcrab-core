#![cfg(feature = "wasm")]

use crate::typed_agent_fixture;
use microcrab_session::{
    Error,
    runtime::{
        tool::ToolContext,
        wasm::{WasmAgent, WasmLimits, load_component_with_host},
    },
};
use serde_json::json;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use typed_agent_fixture::{InitializedTool as _, final_text};

fn load_component(
    bytes: &[u8],
    limits: WasmLimits,
) -> Result<Vec<std::sync::Arc<dyn microcrab_session::runtime::tool::Tool>>, Error> {
    microcrab_session::runtime::wasm::load_component(
        bytes,
        limits,
        std::sync::Arc::new(RecordingHost::default()),
    )
}

fn context_with_host(
    host: std::sync::Arc<dyn microcrab_host_api::Host>,
    cancel: CancellationToken,
) -> ToolContext {
    let mut context = context(cancel);
    context.processes = microcrab_session::execution::process::ProcessRegistry::new(
        "fixture".into(),
        Some(host),
        Default::default(),
    );
    context
}

fn context(cancel: CancellationToken) -> ToolContext {
    ToolContext::detached(
        microcrab_session::SessionState::new(Default::default()),
        cancel,
    )
}

fn component(invoke: &str, pages: u32) -> Vec<u8> {
    let catalog = r#"[{"name":"fixture","modes":[{"name":"execute","role":"both"}],"variables":{},"tools":[{"name":"echo","description":"fixture","parameters":{"type":"object"}}]}]"#;
    let escaped: String = catalog.bytes().map(|b| format!("\\{b:02x}")).collect();
    let text = format!(
        r#"(component
      (core module $m
        (memory (export "memory") {pages})
        (global $heap (mut i32) (i32.const 4096))
        (data (i32.const 128) "{escaped}")
        (data (i32.const 1024) "ok")
        (func (export "realloc") (param i32 i32 i32 i32) (result i32)
          (local $old i32)
          global.get $heap local.tee $old
          local.get 3 i32.add global.set $heap local.get $old)
        (func (export "describe") (param i32 i32) (result i32)
          i32.const 0 i32.const 0 i32.store
          i32.const 4 i32.const 128 i32.store
          i32.const 8 i32.const {length} i32.store i32.const 0)
        (func (export "invoke") (param i32 i32 i32 i32 i32 i32) (result i32)
          {invoke}
          i32.const 16 i32.const 0 i32.store
          i32.const 20 i32.const 1024 i32.store
          i32.const 24 i32.const 2 i32.store i32.const 16))
      (core instance $i (instantiate $m))
      (func (export "describe") (param "host-info" string) (result (result string (error string)))
        (canon lift (core func $i "describe") (memory (core memory $i "memory")) (realloc (core func $i "realloc"))))
      (func (export "invoke") (param "host-info" string) (param "name" string) (param "arguments" string)
        (result (result string (error string)))
        (canon lift (core func $i "invoke") (memory (core memory $i "memory")) (realloc (core func $i "realloc")))))"#,
        length = catalog.len()
    );
    typed_agent_fixture::component(&text)
}

#[tokio::test]
async fn discovers_and_executes_import_free_component() {
    let tools = load_component(&component("", 1), WasmLimits::default()).unwrap();
    let tool = tools
        .iter()
        .find(|t| t.definition().name == "echo")
        .unwrap();
    assert_eq!(
        final_text(
            tool.execute_initialized(json!({"text":"hello"}), context(CancellationToken::new()))
                .await
                .unwrap()
        ),
        "ok"
    );
    // A fresh Store is instantiated for each invocation.
    assert_eq!(
        final_text(
            tool.execute_initialized(json!({}), context(CancellationToken::new()))
                .await
                .unwrap()
        ),
        "ok"
    );
}

#[tokio::test]
async fn direct_component_requires_proof_bound_to_the_exact_invocation() {
    use microcrab_session::{policy::InvocationSubject, runtime::execution::ExecutionId};
    let tools = load_component(&component("", 1), WasmLimits::default()).unwrap();
    let tool = &tools[0];
    let arguments = json!({"text":"original"});
    let mut unadmitted = context(CancellationToken::new());
    unadmitted.call_id = "reserved-without-proof".into();
    unadmitted.tool = tool.definition().name;
    unadmitted.ensure_detached_execution().unwrap();
    let error = tool
        .execute(arguments.clone(), unadmitted)
        .await
        .unwrap_err();
    assert_eq!(
        error.tool_diagnostic().code,
        microcrab_session::observation::diagnostics::DiagnosticCode::PermissionDenied
    );

    let admitted = tool
        .admitted_context(arguments.clone(), context(CancellationToken::new()))
        .await
        .unwrap();
    assert_eq!(
        final_text(
            tool.execute(arguments.clone(), admitted.clone())
                .await
                .unwrap()
        ),
        "ok"
    );
    let mut variants = Vec::new();
    let mut rebound = admitted.clone();
    rebound.call_id.push_str("-other");
    variants.push(rebound);
    let mut rebound = admitted.clone();
    rebound.tool = "another-export".into();
    variants.push(rebound);
    let mut rebound = admitted.clone();
    rebound.session_id.push_str("-other");
    variants.push(rebound);
    let mut rebound = admitted.clone();
    rebound.agent.push_str("-other");
    variants.push(rebound);
    let mut rebound = admitted.clone();
    rebound.mode = Some("another-mode".into());
    variants.push(rebound);
    let mut rebound = admitted.clone();
    rebound.execution_id = Some(ExecutionId(u64::MAX));
    variants.push(rebound);
    let mut rebound = admitted.clone();
    rebound.state = context(CancellationToken::new()).state;
    variants.push(rebound);
    let mut rebound = admitted.clone();
    rebound.subject = InvocationSubject::AgentInit {
        component: tool.wasm_lifecycle().unwrap().component().into(),
        generation: 0,
        attempt: 1,
    };
    variants.push(rebound);
    let mut rebound = admitted.clone();
    rebound.admission = None;
    variants.push(rebound);
    for rebound in variants {
        let error = tool.execute(arguments.clone(), rebound).await.unwrap_err();
        assert_eq!(
            error.tool_diagnostic().code,
            microcrab_session::observation::diagnostics::DiagnosticCode::PermissionDenied
        );
    }
    let error = tool
        .execute(json!({"text":"changed"}), admitted.clone())
        .await
        .unwrap_err();
    assert_eq!(
        error.tool_diagnostic().code,
        microcrab_session::observation::diagnostics::DiagnosticCode::PermissionDenied
    );
    // Same export and catalog, different component bytes: no authority transfer.
    let other = load_component(&component("nop", 1), WasmLimits::default()).unwrap();
    let error = other[0]
        .execute(arguments.clone(), admitted.clone())
        .await
        .unwrap_err();
    assert_eq!(
        error.tool_diagnostic().code,
        microcrab_session::observation::diagnostics::DiagnosticCode::PermissionDenied
    );
    admitted.cancellation.cancel();
    assert!(matches!(
        tool.execute(arguments, admitted).await,
        Err(Error::Cancelled)
    ));
}

#[test]
fn rejects_imports_and_memory_over_budget() {
    let imported = wat::parse_str(r#"(component (import "host" (func)))"#).unwrap();
    assert!(load_component(&imported, WasmLimits::default()).is_err());
    let limits = WasmLimits {
        memory_bytes: 64 * 1024,
        ..Default::default()
    };
    assert!(load_component(&component("", 2), limits).is_err());
}

#[tokio::test]
async fn fuel_exhaustion_does_not_poison_future_calls() {
    let limits = WasmLimits {
        fuel: 100_000,
        ..Default::default()
    };
    let tools = load_component(&component("(loop $forever br $forever)", 1), limits).unwrap();
    let mut evidence_ids = std::collections::BTreeSet::new();
    for _ in 0..2 {
        let error = tools[0]
            .execute_initialized(json!({}), context(CancellationToken::new()))
            .await
            .expect_err("fuel exhaustion must fail");
        let Error::Diagnostic(diagnostic) = error else {
            panic!("fuel exhaustion must preserve structured boundary evidence");
        };
        assert_eq!(
            diagnostic.code,
            microcrab_session::observation::diagnostics::DiagnosticCode::WasmTrap
        );
        assert_eq!(
            diagnostic.layer,
            microcrab_session::observation::diagnostics::DiagnosticLayer::Guest
        );
        assert_eq!(
            diagnostic.phase,
            microcrab_session::observation::diagnostics::DiagnosticPhase::GuestExecution
        );
        assert_eq!(
            diagnostic.effects,
            microcrab_session::observation::diagnostics::EffectKnowledge::Unknown
        );
        assert!(
            evidence_ids.insert(
                diagnostic
                    .evidence_id
                    .expect("fuel trap has boundary evidence")
            ),
            "fresh invocation must have its own failure evidence"
        );
    }
}

#[tokio::test]
async fn cpu_loop_obeys_wall_deadline_and_cancellation() {
    let limits = WasmLimits {
        fuel: u64::MAX,
        timeout_ms: 30,
        ..Default::default()
    };
    let tool = load_component(&component("(loop $forever br $forever)", 1), limits)
        .unwrap()
        .remove(0);
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        tool.execute_initialized(json!({}), context(CancellationToken::new())),
    )
    .await
    .unwrap();
    assert!(
        matches!(result, Err(Error::Limit(_))),
        "unexpected tool outcome"
    );

    let limits = WasmLimits {
        fuel: u64::MAX,
        timeout_ms: 5_000,
        ..Default::default()
    };
    let tool = load_component(&component("(loop $forever br $forever)", 1), limits)
        .unwrap()
        .remove(0);
    let cancel = CancellationToken::new();
    let worker_cancel = cancel.clone();
    let running = tokio::spawn(async move {
        tool.execute_initialized(json!({}), context(worker_cancel))
            .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    cancel.cancel();
    let result = tokio::time::timeout(Duration::from_secs(2), running)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(result, Err(Error::Cancelled)),
        "unexpected tool outcome"
    );
}

#[tokio::test]
async fn arguments_and_return_allocations_are_bounded() {
    let limits = WasmLimits {
        max_input_bytes: 4096,
        ..Default::default()
    };
    let tools = load_component(&component("", 1), limits).unwrap();
    assert!(matches!(
        tools[0]
            .execute_initialized(
                json!({"large":"x".repeat(8192)}),
                context(CancellationToken::new())
            )
            .await,
        Err(Error::Limit(_))
    ));
    assert!(
        tools[0]
            .execute_initialized(json!("not an object"), context(CancellationToken::new()))
            .await
            .is_err()
    );
    // The valid catalog exceeds this budget, so discovery itself must fail.
    let limits = WasmLimits {
        max_output_bytes: 8,
        ..Default::default()
    };
    assert!(load_component(&component("", 1), limits).is_err());
}

fn shell_component(discovery_calls_host: bool) -> Vec<u8> {
    shell_component_named(discovery_calls_host, "bash")
}

fn shell_component_named(discovery_calls_host: bool, name: &str) -> Vec<u8> {
    let catalog = serde_json::to_string(
        &json!([{"name":"universal","modes":[{"name":"execute","role":"both"}],"variables":{"description":"fixture","rules":[],"hints":[]},"tools":[{"name":name,"description":"host fixture","parameters":{"type":"object"}}]}]),
    )
    .unwrap();
    shell_component_metadata(discovery_calls_host, &catalog)
}

fn shell_component_metadata(discovery_calls_host: bool, catalog: &str) -> Vec<u8> {
    shell_component_metadata_calls(discovery_calls_host, catalog, 0)
}

fn shell_component_metadata_calls(
    discovery_calls_host: bool,
    catalog: &str,
    repeat: usize,
) -> Vec<u8> {
    typed_agent_fixture::component(&shell_component_source(
        discovery_calls_host,
        catalog,
        repeat,
    ))
}

#[path = "support/host_shell_fixture.rs"]
mod host_shell_fixture;
use host_shell_fixture::shell_component_source;

#[tokio::test]
async fn initial_session_init_releases_its_real_shell_process_before_ready() {
    use microcrab_host_api::ProcessApi;
    use microcrab_session::{
        policy::InvocationSubject,
        runtime::{Runtime, ToolRegistration, execution::ExecutionStatus},
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct InitModel(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl microcrab_session::Client for InitModel {
        async fn complete(
            &self,
            _: &microcrab_session::context::Content,
            _: &dyn microcrab_session::Auditor,
            _: &dyn microcrab_auditor::VariableView,
            _: CancellationToken,
        ) -> Result<microcrab_protocol::Response, microcrab_transport::Error> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(microcrab_protocol::Response {
                provider_metadata: None,
                content: "done".into(),
                tool_calls: vec![],
                usage: Default::default(),
            })
        }
    }
    let catalog = r#"[{"name":"init-shell","modes":[{"name":"execute","role":"both"}],"variables":{},"tools":[{"name":"echo","description":"fixture","parameters":{"type":"object"}}]}]"#;
    let source =
        shell_component_source(false, catalog, 0).replace("typed-fixture", "echo init-ok ");
    let init = typed_agent_fixture::READY_INIT_BODY.replacen(
        "(local $result i32)",
        "(local $result i32)\n i32.const 3000 i32.const 13 i32.const 0 i32.const 0 i32.const 0 i32.const 0 call $invoke drop",
        1,
    );
    let bytes = typed_agent_fixture::component_with_init(&source, &init);
    let root = tempfile::tempdir().unwrap();
    let host =
        Arc::new(microcrab_host_local::LocalHost::new(root.path(), Default::default()).unwrap());
    let agent = WasmAgent::load(&bytes, WasmLimits::default(), host.clone()).unwrap();
    let runtime = Runtime::new(
        agent
            .tools()
            .into_iter()
            .map(ToolRegistration::Wasm)
            .collect(),
        Arc::new(microcrab_session::runtime::tool::DenyAll),
    )
    .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let session = microcrab_session::Session::builder(InitModel(calls.clone()), runtime)
        .host(host.clone())
        .permissions(serde_json::from_value(json!({"initialization":"allow"})).unwrap())
        .build()
        .unwrap();
    assert!(
        !ProcessApi::has_running_processes(host.as_ref()),
        "discovery cannot launch setup processes"
    );
    session.start().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(!ProcessApi::has_running_processes(host.as_ref()));
    assert!(
        !session.process_control().has_unclosed(),
        "init-owned resources settle before ready is published"
    );
    let rows = session.execution_snapshots();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, ExecutionStatus::Succeeded);
    assert!(rows[0].tool.is_empty());
    assert!(matches!(
        rows[0].subject,
        InvocationSubject::AgentInit { attempt: 1, .. }
    ));
    session.close().await.unwrap();
}

#[derive(Default)]
struct RecordingHost(std::sync::atomic::AtomicUsize);
#[async_trait::async_trait]
impl microcrab_host_api::ProcessApi for RecordingHost {
    fn watch(
        &self,
        _: microcrab_host_api::process::ProcessHandle,
    ) -> Result<tokio::sync::watch::Receiver<u64>, microcrab_host_api::HostError> {
        Ok(tokio::sync::watch::channel(0).1)
    }
    async fn status(
        &self,
        _: &microcrab_host_api::OpContext,
        _: microcrab_host_api::process::ProcessHandle,
    ) -> Result<microcrab_host_api::process::ProcessStatus, microcrab_host_api::HostError> {
        Ok(microcrab_host_api::process::ProcessStatus {
            pid: Some(42),
            state: microcrab_host_api::process::ProcessState::Exited {
                status: microcrab_host_api::process::ExitStatus {
                    code: Some(0),
                    signal: None,
                },
            },
            elapsed_s: 0,
        })
    }
    async fn release(
        &self,
        _: &microcrab_host_api::OpContext,
        _: microcrab_host_api::process::ProcessHandle,
    ) -> Result<(), microcrab_host_api::HostError> {
        Ok(())
    }
}
impl microcrab_host_api::filesystem::FsApi for RecordingHost {}
impl microcrab_host_api::net::NetApi for RecordingHost {}
#[async_trait::async_trait]
impl microcrab_host_api::ShellApi for RecordingHost {
    async fn launch(
        &self,
        _: &microcrab_host_api::OpContext,
        _: microcrab_host_api::shell::ShellRequest,
    ) -> Result<microcrab_host_api::process::ProcessHandle, microcrab_host_api::HostError> {
        let id = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        Ok(microcrab_host_api::process::ProcessHandle(
            microcrab_host_api::HandleId(id as u64),
        ))
    }
    async fn capture_status(
        &self,
        _: &microcrab_host_api::OpContext,
        _: microcrab_host_api::process::ProcessHandle,
    ) -> Result<microcrab_host_api::shell::ShellCaptureStatus, microcrab_host_api::HostError> {
        Ok(microcrab_host_api::shell::ShellCaptureStatus {
            closed: true,
            cursor: Default::default(),
            stdout_bytes: 0,
            stderr_bytes: 0,
            truncated: false,
        })
    }
}
impl microcrab_host_api::HostAPI for RecordingHost {
    fn supports(&self, operation: &str) -> bool {
        operation == "shell.launch"
    }
}
#[async_trait::async_trait]
impl microcrab_host_api::Host for RecordingHost {
    fn tool_capabilities(&self) -> Vec<String> {
        vec!["shell".into()]
    }
    async fn normalize_resource(
        &self,
        path: std::path::PathBuf,
        _: CancellationToken,
    ) -> Result<String, microcrab_host_api::ToolError> {
        Ok(path.to_string_lossy().replace('\\', "/"))
    }
    fn info(&self) -> Result<microcrab_host_api::HostInfo, microcrab_host_api::ToolError> {
        Ok(microcrab_host_api::HostInfo {
            os_family: microcrab_host_api::OsFamily::Unix,
            os: "linux".into(),
            arch: "x86_64".into(),
            shell: "/bin/bash".into(),
            path_separator: "/".into(),
            newline: "\n".into(),
            capabilities: vec![],
            resources: None,
            virtual_shell: None,
            description: None,
        })
    }
}

#[test]
fn rejects_invalid_agent_metadata() {
    let valid = json!([{"name":"universal","modes":[{"name":"execute","role":"both"}],"variables":{"rules":[],"hints":[]},"tools":[{"name":"bash","description":"fixture","parameters":{"type":"object"}}]}]);
    let mut cases = vec![json!([])];
    let mut invalid = valid.clone();
    invalid[0]["name"] = json!("");
    cases.push(invalid);
    let mut invalid = valid.clone();
    invalid[0]["modes"][0]["role"] = json!("main");
    cases.push(invalid);
    let mut invalid = valid.clone();
    invalid[0]["modes"] = json!([]);
    cases.push(invalid);
    let mut invalid = valid.clone();
    invalid[0]["variables"] = json!("stringified");
    cases.push(invalid);
    let mut invalid = valid.clone();
    invalid[0]["tools"][0]["parameters"] = json!({"type":"array"});
    cases.push(invalid);
    cases.push(json!([valid[0].clone(), valid[0].clone()]));
    for metadata in cases {
        let host = std::sync::Arc::new(RecordingHost::default());
        assert!(
            WasmAgent::load(
                &shell_component_metadata(false, &metadata.to_string()),
                WasmLimits::default(),
                host
            )
            .is_err()
        );
    }
}

#[test]
fn agent_mode_requires_metadata_and_bounded_host_info() {
    let host = std::sync::Arc::new(RecordingHost::default());
    assert!(
        WasmAgent::load(
            &wat::parse_str("(component)").unwrap(),
            WasmLimits::default(),
            host.clone()
        )
        .is_err()
    );
    let limits = WasmLimits {
        max_input_bytes: 8,
        ..Default::default()
    };
    assert!(matches!(
        WasmAgent::load(&shell_component(false), limits, host.clone()),
        Err(Error::Limit(_))
    ));
    let limits = WasmLimits {
        max_output_bytes: 8,
        ..Default::default()
    };
    assert!(WasmAgent::load(&shell_component(false), limits, host).is_err());
}

struct NoInfoHost;
impl microcrab_host_api::process::ProcessApi for NoInfoHost {}
impl microcrab_host_api::filesystem::FsApi for NoInfoHost {}
impl microcrab_host_api::net::NetApi for NoInfoHost {}
impl microcrab_host_api::ShellApi for NoInfoHost {}
impl microcrab_host_api::HostAPI for NoInfoHost {}
#[async_trait::async_trait]
impl microcrab_host_api::Host for NoInfoHost {}

#[test]
fn missing_host_info_is_explicitly_unsupported() {
    use microcrab_host_api::{Host, ToolError};
    assert!(matches!(NoInfoHost.info(), Err(ToolError::Unsupported(_))));
    assert!(
        WasmAgent::load(
            &shell_component(false),
            WasmLimits::default(),
            std::sync::Arc::new(NoInfoHost)
        )
        .is_err()
    );
}

struct WindowsHost(RecordingHost);
#[async_trait::async_trait]
impl microcrab_host_api::ProcessApi for WindowsHost {
    fn watch(
        &self,
        h: microcrab_host_api::process::ProcessHandle,
    ) -> Result<tokio::sync::watch::Receiver<u64>, microcrab_host_api::HostError> {
        microcrab_host_api::ProcessApi::watch(&self.0, h)
    }
    async fn status(
        &self,
        c: &microcrab_host_api::OpContext,
        h: microcrab_host_api::process::ProcessHandle,
    ) -> Result<microcrab_host_api::process::ProcessStatus, microcrab_host_api::HostError> {
        microcrab_host_api::ProcessApi::status(&self.0, c, h).await
    }
    async fn release(
        &self,
        c: &microcrab_host_api::OpContext,
        h: microcrab_host_api::process::ProcessHandle,
    ) -> Result<(), microcrab_host_api::HostError> {
        microcrab_host_api::ProcessApi::release(&self.0, c, h).await
    }
}
impl microcrab_host_api::filesystem::FsApi for WindowsHost {}
impl microcrab_host_api::net::NetApi for WindowsHost {}
#[async_trait::async_trait]
impl microcrab_host_api::ShellApi for WindowsHost {
    async fn launch(
        &self,
        c: &microcrab_host_api::OpContext,
        r: microcrab_host_api::shell::ShellRequest,
    ) -> Result<microcrab_host_api::process::ProcessHandle, microcrab_host_api::HostError> {
        microcrab_host_api::ShellApi::launch(&self.0, c, r).await
    }
    async fn capture_status(
        &self,
        c: &microcrab_host_api::OpContext,
        h: microcrab_host_api::process::ProcessHandle,
    ) -> Result<microcrab_host_api::shell::ShellCaptureStatus, microcrab_host_api::HostError> {
        microcrab_host_api::ShellApi::capture_status(&self.0, c, h).await
    }
}
impl microcrab_host_api::HostAPI for WindowsHost {
    fn supports(&self, operation: &str) -> bool {
        microcrab_host_api::HostAPI::supports(&self.0, operation)
    }
}
#[async_trait::async_trait]
impl microcrab_host_api::Host for WindowsHost {
    fn tool_capabilities(&self) -> Vec<String> {
        vec!["shell".into()]
    }
    async fn normalize_resource(
        &self,
        path: std::path::PathBuf,
        token: CancellationToken,
    ) -> Result<String, microcrab_host_api::ToolError> {
        microcrab_host_api::Host::normalize_resource(&self.0, path, token).await
    }
    fn info(&self) -> Result<microcrab_host_api::HostInfo, microcrab_host_api::ToolError> {
        Ok(microcrab_host_api::HostInfo {
            os_family: microcrab_host_api::OsFamily::Windows,
            os: "windows".into(),
            arch: "x86_64".into(),
            shell: "C:\\Program Files\\PowerShell\\7\\pwsh.exe".into(),
            path_separator: "\\".into(),
            newline: "\r\n".into(),
            capabilities: vec![],
            resources: None,
            virtual_shell: None,
            description: None,
        })
    }
}

#[tokio::test]
async fn shell_import_requires_runtime_admission_on_each_host() {
    use std::sync::{Arc, atomic::Ordering};
    let host = Arc::new(WindowsHost(RecordingHost::default()));
    let wrong =
        WasmAgent::load(&shell_component(false), WasmLimits::default(), host.clone()).unwrap();
    assert!(
        wrong.tools()[0]
            .execute(
                json!({"command":"blocked"}),
                ToolContext::detached(
                    microcrab_session::SessionState::new(Default::default()),
                    CancellationToken::new()
                )
            )
            .await
            .is_err()
    );
    assert_eq!(host.0.0.load(Ordering::SeqCst), 0);
    let agent = WasmAgent::load(
        &shell_component_named(false, "pwsh"),
        WasmLimits::default(),
        host.clone(),
    )
    .unwrap();
    for _ in 0..2 {
        let text = agent.tools()[0]
            .execute_initialized(
                json!({"command":"typed-fixture"}),
                context_with_host(host.clone(), CancellationToken::new()),
            )
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&final_text(text)).unwrap()["stdout"],
            "typed-fixture"
        );
    }
    assert_eq!(host.0.0.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn narrow_shell_import_uses_injected_host_and_is_disabled_for_discovery() {
    use std::sync::{Arc, atomic::Ordering};
    let host = Arc::new(RecordingHost::default());
    assert!(WasmAgent::load(&shell_component(true), WasmLimits::default(), host.clone()).is_err());
    assert_eq!(host.0.load(Ordering::SeqCst), 0);
    let agent =
        WasmAgent::load(&shell_component(false), WasmLimits::default(), host.clone()).unwrap();
    assert_eq!(agent.modes(), ["execute"]);
    let metadata = &agent.agents()[0];
    assert_eq!(metadata.name, "universal");
    assert_eq!(
        metadata.modes[0].role,
        microcrab_agent_types::AgentRole::Both
    );
    assert_eq!(metadata.tools[0].name, "bash");
    assert_eq!(metadata.variables.get("rules"), Some(&json!([])));
    assert_eq!(host.0.load(Ordering::SeqCst), 0);
    let text = agent.tools()[0]
        .execute_initialized(
            json!({"command":"typed-fixture"}),
            context_with_host(host.clone(), CancellationToken::new()),
        )
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(&final_text(text)).unwrap();
    assert_eq!(output["stdout"], "typed-fixture");
    assert_eq!(host.0.load(Ordering::SeqCst), 1);
    let tools =
        load_component_with_host(&shell_component(false), WasmLimits::default(), host).unwrap();
    assert_eq!(tools[0].definition().name, "bash");
}

#[tokio::test]
async fn renamed_export_requires_tool_admission_and_trusts_registered_component() {
    use std::sync::{Arc, atomic::Ordering};
    let host = Arc::new(RecordingHost::default());
    let agent = WasmAgent::load(
        &shell_component_named(false, "echo"),
        WasmLimits::default(),
        host.clone(),
    )
    .unwrap();
    assert!(
        agent.tools()[0]
            .execute(
                json!({"command":"must-not-run"}),
                ToolContext::detached(
                    microcrab_session::SessionState::new(Default::default()),
                    CancellationToken::new()
                )
            )
            .await
            .is_err()
    );
    assert_eq!(host.0.load(Ordering::SeqCst), 0);
    let output = agent.tools()[0]
        .execute_initialized(
            json!({"command":"typed-fixture"}),
            context_with_host(host.clone(), CancellationToken::new()),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&final_text(output)).unwrap()["stdout"],
        "typed-fixture"
    );
    assert_eq!(
        host.0.load(Ordering::SeqCst),
        1,
        "admitting the opaque export trusts its registered component Host behavior"
    );
}

#[tokio::test]
async fn typed_imports_share_a_bounded_reusable_invocation_budget() {
    use std::sync::{Arc, atomic::Ordering};
    let catalog = json!([{"name":"universal","modes":[{"name":"execute","role":"both"}],"variables":{"rules":[],"hints":[]},"tools":[{"name":"bash","description":"fixture","parameters":{"type":"object"}}]}]).to_string();
    let host = Arc::new(RecordingHost::default());
    use microcrab_session::runtime::execution::ToolOutput;
    let mut expected_calls = 0;
    for (repeat, failed, calls_per_invocation, invocations) in [
        (1, false, 2, 2),
        (239, false, 240, 2),
        (240, true, 240, 2),
        (1, false, 2, 1),
    ] {
        let agent = WasmAgent::load(
            &shell_component_metadata_calls(false, &catalog, repeat),
            WasmLimits::default(),
            host.clone(),
        )
        .unwrap();
        for _ in 0..invocations {
            // The fixture makes `repeat` launches plus one final launch. Cover
            // exactly 240 successful imports, rejection of import 241, and fresh
            // budgets on repeated invocations. Process capacity is independent
            // and must not reject a launch before the shared import boundary.
            let mut invocation = context_with_host(host.clone(), CancellationToken::new());
            invocation.processes = microcrab_session::execution::process::ProcessRegistry::new(
                "fixture".into(),
                Some(host.clone()),
                microcrab_session::Limits {
                    max_processes: 256,
                    ..Default::default()
                },
            );
            let output = agent.tools()[0]
                .execute_initialized(json!({"command":"typed-fixture"}), invocation)
                .await;
            if failed {
                let diagnostic = match output {
                    Err(error) => error.tool_diagnostic(),
                    Ok(_) => panic!(
                        "the 241st actual Host call must exhaust the shared invocation budget"
                    ),
                };
                assert_eq!(diagnostic.summary(), "Host invocation budget exceeded");
                assert_eq!(
                    diagnostic.operation,
                    Some(
                        microcrab_session::observation::diagnostics::DiagnosticOperation::Resource
                    )
                );
            } else {
                let ToolOutput::Final(output) = output.unwrap() else {
                    panic!("probe has no buffered producer")
                };
                assert!(output.failure.is_none());
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&output.content).unwrap()["stdout"],
                    "typed-fixture"
                );
            }
            expected_calls += calls_per_invocation;
            assert_eq!(
                host.0.load(Ordering::SeqCst),
                expected_calls,
                "exhausted calls never reach Host; each next invocation receives a fresh shared budget"
            );
        }
    }
}

#[tokio::test]
async fn cancelled_tool_admission_remains_cancellation() {
    let host = std::sync::Arc::new(RecordingHost::default());
    let agent =
        WasmAgent::load(&shell_component(false), WasmLimits::default(), host.clone()).unwrap();
    let cancel = CancellationToken::new();
    cancel.cancel();
    let result = agent.tools()[0]
        .execute_initialized(
            json!({"command":"typed-fixture"}),
            context_with_host(host.clone(), cancel),
        )
        .await;
    assert!(matches!(result, Err(Error::Cancelled)));
    assert_eq!(host.0.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn session_permission_denial_never_reaches_wasm_host() {
    use microcrab_auditor::Auditor;
    use microcrab_protocol::{Response, ToolCall, Usage};
    use microcrab_session::runtime::{Runtime, ToolRegistration, tool::DenyAll};
    use microcrab_session::{Client, Session, context::Content};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Calls(AtomicUsize);
    #[async_trait::async_trait]
    impl Client for Calls {
        async fn complete(
            &self,
            content: &Content,
            _: &dyn Auditor,
            _: &dyn microcrab_auditor::VariableView,
            _: CancellationToken,
        ) -> Result<Response, microcrab_transport::Error> {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                return Ok(Response {
                    provider_metadata: None,
                    content: String::new(),
                    tool_calls: vec![ToolCall {
                        provider_metadata: None,
                        id: "denied".into(),
                        name: "bash".into(),
                        arguments: json!({"command":"must-not-run"}),
                    }],
                    usage: Usage::default(),
                });
            }
            let result = content
                .items
                .iter()
                .find_map(|item| match item {
                    microcrab_session::context::ContentItem::Tool { call_id, content, .. }
                        if call_id == "denied" =>
                    {
                        Some(serde_json::from_str::<serde_json::Value>(content).unwrap())
                    }
                    _ => None,
                })
                .expect("next model request contains the WASM tool denial");
            assert_eq!(result["diagnostic"]["code"], "permission_denied");
            assert_eq!(result["diagnostic"]["layer"], "runtime");
            assert_eq!(result["diagnostic"]["phase"], "authorization");
            assert_eq!(result["diagnostic"]["effects"], "unknown");
            assert!(result["error"].as_str().unwrap().contains("denied"));
            assert_eq!(result["error"], result["diagnostic"]["reason"]);
            Ok(Response {
                provider_metadata: None,
                content: "done".into(),
                tool_calls: vec![],
                usage: Usage::default(),
            })
        }
    }
    let host = Arc::new(RecordingHost::default());
    let agent =
        WasmAgent::load(&shell_component(false), WasmLimits::default(), host.clone()).unwrap();
    let runtime = Runtime::new(
        agent
            .tools()
            .into_iter()
            .map(ToolRegistration::Wasm)
            .collect(),
        Arc::new(DenyAll),
    )
    .unwrap();
    let session = Session::builder_arc(Arc::new(Calls(AtomicUsize::new(0))), runtime)
        .permissions(serde_json::from_value(json!({"initialization":"allow"})).unwrap())
        .build()
        .unwrap();
    assert_eq!(session.submit("Denied").await.unwrap().content, "done");
    assert_eq!(host.0.load(Ordering::SeqCst), 0);
    session.close().await.unwrap();
}
