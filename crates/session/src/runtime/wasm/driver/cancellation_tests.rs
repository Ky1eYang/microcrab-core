//! Job cancellation during a real guest drive must not retire its shared store.
use super::super::{WasmAgent, WasmLimits};
use crate::{
    Auditor, Client, Session, VariableView,
    context::{Content, ContentItem},
    runtime::{
        Runtime, ToolRegistration,
        execution::{ExecutionId, ExecutionStatus},
        tool::DenyAll,
    },
};
use microcrab_host_api::{
    ErrorCategory, HandleId, Host, HostAPI, HostError, HostInfo, OpContext, OsFamily, ProcessApi,
    ShellApi, ShellCaptureStatus, ShellOutput, ShellRequest, ToolError,
    process::{ExitStatus, ProcessHandle, ProcessState, ProcessStatus},
};
use microcrab_protocol::{Response, ToolCall};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

/// An explicit gate override is authoritative. Without one, use maintained
/// component outputs rooted at the workspace (this crate is crates/session).
/// Missing or unreadable bytes remain a concrete prerequisite failure.
fn maintained_universal_component() -> Vec<u8> {
    let path = if let Some(path) = std::env::var_os("MICROCRAB_UNIVERSAL_WASM") {
        PathBuf::from(path)
    } else {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(std::path::Path::parent)
            .expect("session crate resides under crates in workspace");
        let candidates = [
            root.join(".microcrab/agents/universal.wasm"),
            root.join("target/universal.wasm"),
        ];
        candidates.iter().find(|path|path.is_file()).cloned().unwrap_or_else(||panic!("maintained universal component is missing; set MICROCRAB_UNIVERSAL_WASM or supply {} or {}",candidates[0].display(),candidates[1].display()))
    };
    std::fs::read(&path).unwrap_or_else(|error| {
        panic!(
            "cannot read maintained universal component {}: {error}",
            path.display()
        )
    })
}

struct BarrierHost {
    launches: AtomicUsize,
    drains: AtomicUsize,
    releases: AtomicUsize,
    killed: AtomicBool,
    entered_drive_drain: CancellationToken,
    observed_cancel: CancellationToken,
    released_first: CancellationToken,
    changes: tokio::sync::watch::Sender<u64>,
}
impl BarrierHost {
    fn new() -> Self {
        Self {
            launches: AtomicUsize::new(0),
            drains: AtomicUsize::new(0),
            releases: AtomicUsize::new(0),
            killed: AtomicBool::new(false),
            entered_drive_drain: CancellationToken::new(),
            observed_cancel: CancellationToken::new(),
            released_first: CancellationToken::new(),
            changes: tokio::sync::watch::channel(0).0,
        }
    }
    fn terminal(&self, handle: ProcessHandle) -> bool {
        handle.0.0 != 1 || self.killed.load(Ordering::SeqCst)
    }
    fn output(content: &str) -> ShellOutput {
        ShellOutput {
            stdout: content.into(),
            stderr: String::new(),
            next: Default::default(),
            cursor: Default::default(),
            expired: None,
            truncated: false,
        }
    }
}

// OpContext::run may drop the pending Host future when cancellation wins its
// select. Observe that path too, rather than requiring this future to be repolled.
struct ObserveCancellation {
    operation: CancellationToken,
    observed: CancellationToken,
}
impl Drop for ObserveCancellation {
    fn drop(&mut self) {
        if self.operation.is_cancelled() {
            self.observed.cancel();
        }
    }
}

#[async_trait::async_trait]
impl ProcessApi for BarrierHost {
    fn watch(&self, _: ProcessHandle) -> Result<tokio::sync::watch::Receiver<u64>, HostError> {
        Ok(self.changes.subscribe())
    }
    async fn status(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
    ) -> Result<ProcessStatus, HostError> {
        ctx.check()?;
        Ok(ProcessStatus {
            pid: None,
            state: if self.terminal(handle) {
                ProcessState::Exited {
                    status: ExitStatus {
                        code: Some(0),
                        signal: None,
                    },
                }
            } else {
                ProcessState::Running
            },
            elapsed_s: 0,
        })
    }
    async fn kill_tree(&self, ctx: &OpContext, _: ProcessHandle, _: bool) -> Result<(), HostError> {
        ctx.check()?;
        self.killed.store(true, Ordering::SeqCst);
        self.changes.send_modify(|version| *version += 1);
        Ok(())
    }
    async fn release(&self, ctx: &OpContext, handle: ProcessHandle) -> Result<(), HostError> {
        ctx.check()?;
        self.releases.fetch_add(1, Ordering::SeqCst);
        if handle.0.0 == 1 {
            self.released_first.cancel();
        }
        Ok(())
    }
}
impl microcrab_host_api::FsApi for BarrierHost {}
impl microcrab_host_api::NetApi for BarrierHost {}
#[async_trait::async_trait]
impl ShellApi for BarrierHost {
    async fn launch(&self, ctx: &OpContext, _: ShellRequest) -> Result<ProcessHandle, HostError> {
        ctx.check()?;
        let id = self.launches.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(ProcessHandle(HandleId(id as u64)))
    }
    async fn capture_status(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
    ) -> Result<ShellCaptureStatus, HostError> {
        ctx.check()?;
        Ok(ShellCaptureStatus {
            closed: self.terminal(handle),
            cursor: Default::default(),
            stdout_bytes: 0,
            stderr_bytes: 0,
            truncated: false,
        })
    }
    async fn read(
        &self,
        ctx: &OpContext,
        _: ProcessHandle,
        _: Option<microcrab_host_api::ShellPosition>,
    ) -> Result<ShellOutput, HostError> {
        ctx.check()?;
        Ok(Self::output(""))
    }
    async fn drain(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
        _: usize,
    ) -> Result<ShellOutput, HostError> {
        ctx.check()?;
        let ordinal = self.drains.fetch_add(1, Ordering::SeqCst) + 1;
        if handle.0.0 == 1 && ordinal == 2 {
            let _observe = ObserveCancellation {
                operation: ctx.cancel.clone(),
                observed: self.observed_cancel.clone(),
            };
            self.entered_drive_drain.cancel();
            ctx.cancel.cancelled().await;
            return Err(HostError::new(
                ErrorCategory::Cancelled,
                "fixture observation cancelled",
            ));
        }
        Ok(Self::output(if handle.0.0 == 1 {
            ""
        } else {
            "after-cancel"
        }))
    }
}
impl HostAPI for BarrierHost {
    fn supports(&self, operation: &str) -> bool {
        matches!(
            operation,
            "shell.launch"
                | "shell.read"
                | "shell.drain"
                | "shell.capture-status"
                | "process.status"
                | "process.watch"
                | "process.kill_tree"
                | "process.release"
        )
    }
}
#[async_trait::async_trait]
impl Host for BarrierHost {
    fn info(&self) -> Result<HostInfo, ToolError> {
        Ok(HostInfo {
            os_family: OsFamily::Unix,
            os: "fixture".into(),
            arch: "fixture".into(),
            shell: "bash".into(),
            path_separator: "/".into(),
            newline: "\n".into(),
            capabilities: vec![],
            resources: None,
            virtual_shell: None,
            description: None,
        })
    }
    async fn normalize_resource(
        &self,
        path: PathBuf,
        cancel: CancellationToken,
    ) -> Result<String, ToolError> {
        if cancel.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        Ok(path.to_string_lossy().replace('\\', "/"))
    }
}

struct Model(Arc<Mutex<Option<ToolCall>>>);
#[async_trait::async_trait]
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
            content: "settled".into(),
            tool_calls: self.0.lock().unwrap().take().into_iter().collect(),
            usage: Default::default(),
        })
    }
}
fn call(id: &str) -> ToolCall {
    ToolCall {
        provider_metadata: None,
        id: id.into(),
        name: "bash".into(),
        arguments: json!({"command":"simulated-by-test-host", "yield_ms":0}),
    }
}
fn result(session: &Session, id: &str) -> Value {
    session
        .messages()
        .iter()
        .find_map(|item| match item {
            ContentItem::Tool { call_id, content, .. } if call_id == id => {
                Some(serde_json::from_str(content).unwrap())
            }
            _ => None,
        })
        .expect("actual guest tool result")
}

#[derive(Clone, Default)]
struct Evidence(Arc<Mutex<Vec<std::collections::BTreeMap<String, String>>>>);
impl tracing::Subscriber for Evidence {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target() == "microcrab_session::runtime::wasm"
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Fields(std::collections::BTreeMap<String, String>);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.insert(field.name().into(), format!("{value:?}"));
            }
        }
        let mut fields = Fields(Default::default());
        event.record(&mut fields);
        self.0.lock().unwrap().push(fields.0);
    }
}

#[tokio::test]
async fn mid_drive_job_cancel_preserves_retained_guest_for_next_same_epoch_call() {
    let evidence = Evidence::default();
    // The default current-thread test runtime also polls Session-owned tasks.
    // Keep their dispatcher installed; the WASM worker captures it explicitly.
    let _subscriber = tracing::subscriber::set_default(evidence.clone());
    {
        let bytes = maintained_universal_component();
        let host = Arc::new(BarrierHost::new());
        let agent = WasmAgent::load(&bytes, WasmLimits::default(), host.clone()).unwrap();
        let compiled = agent.tools()[0].wasm_lifecycle().unwrap().compiled;
        let pending = Arc::new(Mutex::new(Some(call("before-cancel"))));
        let runtime = Runtime::new(
            agent
                .tools()
                .into_iter()
                .map(ToolRegistration::Wasm)
                .collect(),
            Arc::new(DenyAll),
        )
        .unwrap();
        let session = Session::builder(Model(pending.clone()), runtime)
            .session_id("driver-cancellation-regression")
            .host(host.clone())
            .agent(agent.agents()[0].clone())
            .permissions(
                serde_json::from_value(json!({"initialization":"allow","overrides":[
                    {"tool":{"kind":"exact","value":"bash"},"action":"allow"}
                ]}))
                .unwrap(),
            )
            .build()
            .unwrap();
        session.select_mode("execute").await.unwrap();
        session
            .submit("start the simulated producer")
            .await
            .unwrap();
        let receipt = result(&session, "before-cancel");
        assert_eq!(receipt["status"], "running");
        let id = ExecutionId(receipt["execution_id"].as_u64().unwrap());
        let epoch = session.state().epoch_id;
        let instances = session.state_handle().wasm_instances();
        let slot = instances.slot(&compiled).unwrap();
        let generation = slot.generation;
        tokio::time::timeout(
            Duration::from_secs(10),
            host.entered_drive_drain.cancelled(),
        )
        .await
        .expect("the real guest must enter its drive-only Host drain");
        assert_eq!(host.launches.load(Ordering::SeqCst), 1);
        assert_eq!(host.drains.load(Ordering::SeqCst), 2);

        // No Wasm can run while this import awaits its still-live token. Expire
        // the CPU epoch now, then cancel through the real Job controller. Its
        // first Wasm safepoint after the import will observe BOTH facts, without
        // depending on a sleep or racing the five-millisecond epoch ticker.
        compiled.engine.increment_epoch();
        session.cancel_execution(id).await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), host.observed_cancel.cancelled())
            .await
            .expect("Job cancellation must reach the in-flight Host operation");
        let registry = session.executions();
        let mut changed = registry.subscribe();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if !registry.get(id).unwrap().status.is_pending() {
                    break;
                }
                changed.changed().await.unwrap();
            }
        })
        .await
        .expect("the cancelled guest producer must settle");
        assert_eq!(registry.get(id).unwrap().status, ExecutionStatus::Cancelled);
        tokio::time::timeout(Duration::from_secs(10), host.released_first.cancelled())
            .await
            .expect("retained source cleanup must release its Host process");
        assert!(host.releases.load(Ordering::SeqCst) >= 1);

        // Inspect BEFORE another invocation: successful re-instantiation could
        // otherwise hide the loss of the original retained instance.
        let lease = slot.lease(&CancellationToken::new()).await.unwrap();
        let retained = slot.run(lease, |cached| {
            if let Some(instance) = cached.as_ref() {
                assert!(
                    !instance.store.data().cooperative_driver_cancellation,
                    "drive cancellation policy leaked into the idle store"
                );
            }
            Ok(cached.is_some())
        });
        let events = evidence.0.lock().unwrap().clone();
        assert!(
            matches!(retained, Ok(true)),
            "ordinary mid-drive Job cancellation retired the guest: {retained:?}; safe boundary evidence: {events:?}"
        );
        assert!(
            !events.iter().any(|event| event
                .get("phase")
                .is_some_and(|phase| phase == "\"drive\"")
                && event
                    .get("cause")
                    .is_some_and(|cause| cause == "\"cancelled\"")),
            "cooperative cancellation must not become a Wasmtime trap: {events:?}"
        );
        *pending.lock().unwrap() = Some(call("after-cancel"));
        session
            .submit("invoke the retained guest after cancellation")
            .await
            .unwrap();
        let next = result(&session, "after-cancel");
        assert!(
            next.get("diagnostic").is_none(),
            "next invocation was rejected: {next}"
        );
        assert!(
            next.to_string().contains("after-cancel"),
            "guest output missing: {next}"
        );
        assert_eq!(
            host.launches.load(Ordering::SeqCst),
            2,
            "next call must enter the guest exactly once"
        );
        assert_eq!(session.state().epoch_id, epoch);
        assert_eq!(instances.slot(&compiled).unwrap().generation, generation);
        session.close().await.unwrap();
    }
}

fn probe_compiled() -> Arc<super::super::Compiled> {
    let bytes = maintained_universal_component();
    let agent =
        WasmAgent::load(&bytes, WasmLimits::default(), Arc::new(BarrierHost::new())).unwrap();
    agent.tools()[0].wasm_lifecycle().unwrap().compiled
}

fn core_probe(
    store: &mut wasmtime::Store<super::super::RuntimeState>,
    body: &str,
) -> wasmtime::TypedFunc<(), ()> {
    // A memory-free core module uses the real retained Store's fuel, limiter and
    // epoch callback. This avoids duplicating the callback in a test substitute.
    let bytes = wat::parse_str(format!("(module (func (export \"probe\") {body}))")).unwrap();
    let module = wasmtime::Module::new(store.engine(), &bytes).unwrap();
    let instance = wasmtime::Instance::new(&mut *store, &module, &[]).unwrap();
    instance
        .get_typed_func::<(), ()>(&mut *store, "probe")
        .unwrap()
}

#[derive(Clone, Copy, Debug)]
enum HardStop {
    Unreachable,
    Fuel,
    Deadline,
    Epoch,
}

#[test]
fn cooperative_drive_cancel_preserves_real_traps_and_hard_bounds() {
    use crate::observation::wasm::Phase;
    let evidence = Evidence::default();
    let _subscriber = tracing::subscriber::set_default(evidence.clone());
    let compiled = probe_compiled();
    for case in [
        HardStop::Unreachable,
        HardStop::Fuel,
        HardStop::Deadline,
        HardStop::Epoch,
    ] {
        let mut cached = None;
        let caller = CancellationToken::new();
        let epoch = CancellationToken::new();
        let result = compiled.run(
            &mut cached,
            caller.clone(),
            epoch.clone(),
            None,
            None,
            None,
            None,
            |_, store, boundary| {
                let body = if matches!(case, HardStop::Unreachable) {
                    "unreachable"
                } else {
                    "(loop $forever br $forever)"
                };
                let probe = core_probe(store, body);
                store.data_mut().cooperative_driver_cancellation = true;
                caller.cancel();
                match case {
                    HardStop::Fuel => store.set_fuel(0).unwrap(),
                    HardStop::Deadline => {
                        store.data_mut().deadline =
                            std::time::Instant::now() - Duration::from_secs(1);
                    }
                    HardStop::Epoch => epoch.cancel(),
                    HardStop::Unreachable => {}
                }
                // Force the actual epoch callback rather than waiting for the
                // ticker. Cooperative caller cancellation must not suppress any
                // independent epoch/deadline stop or real Wasm instruction trap.
                store.engine().increment_epoch();
                probe
                    .call(&mut *store, ())
                    .map_err(|error| boundary.failure(error, Phase::Drive))
            },
        );
        assert!(
            matches!(result, Err(crate::Error::Cancelled)),
            "{case:?}: {result:?}"
        );
        let instance = cached
            .as_ref()
            .expect("Compiled::run retains the store for its slot owner");
        assert!(
            instance.poisoned,
            "{case:?}: genuine failure was cleared by caller cancellation"
        );
        let id = instance
            .failure_evidence
            .expect("actual boundary failure retains its evidence")
            .id;
        assert!(
            !instance.store.data().cooperative_driver_cancellation,
            "{case:?}: failed invocation leaked cooperative policy"
        );
        let events = evidence.0.lock().unwrap();
        let event = events
            .last()
            .expect("actual Wasmtime failure emits evidence");
        let cause = match case {
            HardStop::Unreachable => "unreachable",
            HardStop::Fuel => "out_of_fuel",
            HardStop::Deadline => "wall_deadline",
            HardStop::Epoch => "cancelled",
        };
        assert_eq!(event.get("phase").map(String::as_str), Some("\"drive\""));
        assert_eq!(
            event.get("cause"),
            Some(&format!("{cause:?}")),
            "{case:?}: {event:?}"
        );
        assert_eq!(event.get("evidence_id"), Some(&id.to_string()));
    }
    assert_eq!(
        evidence.0.lock().unwrap().len(),
        4,
        "each independent hard stop must produce exactly one boundary event"
    );
}

#[test]
fn cooperative_drive_policy_does_not_weaken_the_next_ordinary_call() {
    use crate::observation::wasm::Phase;
    let evidence = Evidence::default();
    let _subscriber = tracing::subscriber::set_default(evidence.clone());
    let compiled = probe_compiled();
    let mut cached = None;
    let first_caller = CancellationToken::new();
    let first = compiled.run(
        &mut cached,
        first_caller.clone(),
        CancellationToken::new(),
        None,
        None,
        None,
        None,
        |_, store, boundary| {
            let probe = core_probe(store, "nop");
            store.data_mut().cooperative_driver_cancellation = true;
            first_caller.cancel();
            store.engine().increment_epoch();
            probe
                .call(&mut *store, ())
                .map_err(|error| boundary.failure(error, Phase::Drive))
        },
    );
    assert!(matches!(first, Err(crate::Error::Cancelled)));
    let instance = cached.as_ref().unwrap();
    assert!(
        !instance.poisoned,
        "bounded cooperative return must retain the store"
    );
    assert!(instance.failure_evidence.is_none());
    assert!(!instance.store.data().cooperative_driver_cancellation);
    assert!(evidence.0.lock().unwrap().is_empty());

    let second_caller = CancellationToken::new();
    let second = compiled.run(
        &mut cached,
        second_caller.clone(),
        CancellationToken::new(),
        None,
        None,
        None,
        None,
        |_, store, boundary| {
            let probe = core_probe(store, "(loop $forever br $forever)");
            second_caller.cancel();
            store.engine().increment_epoch();
            probe
                .call(&mut *store, ())
                .map_err(|error| boundary.failure(error, Phase::Invoke))
        },
    );
    assert!(matches!(second, Err(crate::Error::Cancelled)));
    let instance = cached.as_ref().unwrap();
    assert!(
        instance.poisoned,
        "ordinary calls must still hard-stop on caller cancellation"
    );
    assert!(instance.failure_evidence.is_some());
    assert!(!instance.store.data().cooperative_driver_cancellation);
    let events = evidence.0.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].get("phase").map(String::as_str),
        Some("\"invoke\"")
    );
    assert_eq!(
        events[0].get("cause").map(String::as_str),
        Some("\"cancelled\"")
    );
}

#[tokio::test]
async fn direct_wasm_and_initialization_proofs_cannot_dispatch_identical_calls_twice() {
    use crate::runtime::{
        execution::process::{ProcessRegistry, ScopedProcessControl},
        tool::ToolContext,
    };
    let bytes = maintained_universal_component();
    let host = Arc::new(BarrierHost::new());
    // The second simulated process is immediately terminal, so no drive is pending.
    host.launches.store(1, Ordering::SeqCst);
    let agent = WasmAgent::load(&bytes, WasmLimits::default(), host.clone()).unwrap();
    let tool = agent
        .tools()
        .into_iter()
        .find(|tool| tool.definition().name == "bash")
        .unwrap();
    let lifecycle = tool.wasm_lifecycle().unwrap();
    let limits = crate::Limits::default();
    let state = crate::State::new(limits.clone());
    let processes = ProcessRegistry::new("direct-proof".into(), Some(host.clone()), limits);
    let port = Arc::new(ScopedProcessControl::new(
        processes.clone(),
        "direct-proof".into(),
        "universal".into(),
        true,
    ));
    let mut init = ToolContext::new(
        state.clone(),
        port.clone(),
        CancellationToken::new(),
        "init-proof".into(),
        String::new(),
        "direct-proof".into(),
        "universal".into(),
        Some("execute".into()),
    );
    init.execution_id = Some(ExecutionId(999));
    init.subject = crate::policy::InvocationSubject::AgentInit {
        component: lifecycle.component().into(),
        generation: lifecycle.generation(&state).unwrap(),
        attempt: 1,
    };
    super::super::test_admission::admit(&mut init, Some(lifecycle.admission_identity().into()));
    lifecycle.initialize(init.clone()).await.unwrap();
    assert!(lifecycle.initialize(init).await.is_err());
    let mut context = ToolContext::new(
        state.clone(),
        port,
        CancellationToken::new(),
        "direct-call".into(),
        "bash".into(),
        "direct-proof".into(),
        "universal".into(),
        Some("execute".into()),
    );
    context.ensure_detached_execution().unwrap();
    let arguments = call("direct-call").arguments;
    let admission_runtime = Runtime::new(
        agent
            .tools()
            .into_iter()
            .map(ToolRegistration::Wasm)
            .collect(),
        Arc::new(crate::runtime::tool::AllowTools::new(["bash"])),
    )
    .unwrap();
    let direct_call = microcrab_protocol::ToolCall {
        id: context.call_id.clone(),
        name: "bash".into(),
        arguments: arguments.clone(),
        provider_metadata: None,
    };
    admission_runtime
        .admit_invocation(
            &direct_call,
            &mut context,
            &super::super::test_admission::issuer(),
        )
        .await
        .unwrap();
    let mut cleared = context.clone();
    cleared.admission = None;
    assert!(
        admission_runtime
            .admit_invocation(
                &direct_call,
                &mut cleared,
                &super::super::test_admission::issuer()
            )
            .await
            .is_err()
    );
    assert_eq!(host.launches.load(Ordering::SeqCst), 1);
    assert_eq!(host.drains.load(Ordering::SeqCst), 0);
    let other_host = Arc::new(BarrierHost::new());
    let other_agent = WasmAgent::load(&bytes, WasmLimits::default(), other_host.clone()).unwrap();
    let other_tool = other_agent
        .tools()
        .into_iter()
        .find(|tool| tool.definition().name == "bash")
        .unwrap();
    assert_eq!(
        other_tool.wasm_lifecycle().unwrap().component(),
        lifecycle.component()
    );
    assert_ne!(other_tool.component_identity(), tool.component_identity());
    assert!(
        other_tool
            .execute(arguments.clone(), context.clone())
            .await
            .is_err()
    );
    assert_eq!(other_host.launches.load(Ordering::SeqCst), 0);
    assert_eq!(other_host.drains.load(Ordering::SeqCst), 0);
    let output = tool
        .execute(arguments.clone(), context.clone())
        .await
        .unwrap();
    context.complete_detached_output(&output).await.unwrap();
    assert_eq!(host.launches.load(Ordering::SeqCst), 2);
    let drains = host.drains.load(Ordering::SeqCst);
    assert!(tool.execute(arguments, context).await.is_err());
    assert_eq!(host.launches.load(Ordering::SeqCst), 2);
    assert_eq!(host.drains.load(Ordering::SeqCst), drains);
    state.executions().close().await.unwrap();
    processes.close().await.unwrap();
}


#[tokio::test]
async fn wasm_recovery_joins_exact_cleanup_and_publishes_only_one_successor() {
    use super::super::{EpochInstances, Phase};
    use crate::observation::diagnostics::{DiagnosticCode, EffectKnowledge};
    use crate::runtime::{execution::ExecutionController, tool::ToolContext};

    #[derive(Default)]
    struct Receipt {
        allowed: AtomicBool,
        releases: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl ExecutionController for Receipt {
        async fn cancel(&self, _: CancellationToken) -> Result<(), crate::Error> {
            Ok(())
        }
        async fn release(&self, _: CancellationToken) -> Result<(), crate::Error> {
            self.releases.fetch_add(1, Ordering::SeqCst);
            if self.allowed.load(Ordering::SeqCst) {
                Ok(())
            } else {
                Err(crate::Error::tool("exact release has not been acknowledged"))
            }
        }
    }
    let compiled = probe_compiled();
    let instances = Arc::new(EpochInstances::default());
    instances.permit_initialized_session();
    let old = instances.slot(&compiled).unwrap();
    let cancel = CancellationToken::new();
    let mut context = ToolContext::detached(crate::SessionState::new(Default::default()), cancel.clone());
    context.tool = "recovery_fixture".into();
    context.call_id = "recovery-fixture-call".into();
    super::super::test_admission::admit(&mut context, Some(compiled.admission_identity.clone()));
    let receipt = Arc::new(Receipt::default());
    context.retain_cleanup(receipt.clone()).unwrap();
    // The embedding Session owns the exact registry across Store replacement.
    let _owner = context.state.clone();
    let old_lease = old.tool_lease(&cancel).await.unwrap();
    let trap = tokio::task::spawn_blocking({
        let compiled = compiled.clone();
        let old = old.clone();
        let runtime = tokio::runtime::Handle::current();
        let cancel = cancel.clone();
        move || old.run(old_lease, |cached| {
            compiled.run(cached, cancel, old.cancellation.clone(), Some(runtime), None, Some(context), None,
                |_, store, boundary| core_probe(store, "unreachable").call(&mut *store, ())
                    .map_err(|error| compiled.execution_failure(boundary, error, Phase::Invoke)))
        })
    }).await.unwrap().unwrap_err().tool_diagnostic();
    assert_eq!(trap.code, DiagnosticCode::WasmTrap);
    assert_eq!(trap.effects, EffectKnowledge::Unknown);
    assert_eq!(receipt.releases.load(Ordering::SeqCst), 1);
    assert!(old.cancellation.is_cancelled());
    assert!(old.try_lease().is_none());
    let mut worker = old.tool_lease(&cancel).await.unwrap();
    let blocked = worker.poisoned().unwrap_err().tool_diagnostic();
    assert_eq!(blocked.code, DiagnosticCode::WasmPoisoned);
    assert_eq!(blocked.effects, EffectKnowledge::NotStarted);
    assert_eq!(blocked.evidence_id, trap.evidence_id);
    assert!(!worker.acknowledge_retirement(&old).await);
    assert!(Arc::ptr_eq(&old, &instances.slot(&compiled).unwrap()));
    receipt.allowed.store(true, Ordering::SeqCst);
    assert!(worker.acknowledge_retirement(&old).await);
    drop(worker);

    let select = || tokio::spawn({
        let instances = instances.clone();
        let compiled = compiled.clone();
        let old = old.clone();
        async move { instances.replace(&compiled, &old).unwrap().unwrap() }
    });
    let (a, b) = tokio::join!(select(), select());
    let (a, b) = (a.unwrap(), b.unwrap());
    assert!(Arc::ptr_eq(&a, &b));
    assert_eq!(a.generation, old.generation + 1);
    assert!(a.tool_lease(&cancel).await.unwrap().ready());
    instances.end();
    let next_epoch = instances.slot(&compiled).unwrap();
    assert!(instances.replace(&compiled, &old).unwrap().is_none());
    assert!(!Arc::ptr_eq(&a, &next_epoch));
    instances.close();
    assert!(instances.replace(&compiled, &old).is_err());
}
