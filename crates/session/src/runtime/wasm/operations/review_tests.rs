//! Imports consume the original admission proof; they never run policy again.
use super::*;
use crate::runtime::tool::ToolContext;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use wasmtime::{AsContextMut, Engine, Store, StoreLimitsBuilder};

struct ObservedHost {
    inner: microcrab_host_local::LocalHost,
    captures: AtomicUsize,
    writes: AtomicUsize,
    response_path: Option<String>,
    normalizations: AtomicUsize,
    slow_normalization_at: Option<usize>,
    cancel_on_slow: Option<tokio_util::sync::CancellationToken>,
}
impl host::ProcessApi for ObservedHost {}
impl host::FsApi for ObservedHost {}
impl host::NetApi for ObservedHost {}
impl host::shell::ShellApi for ObservedHost {}
impl host::HostAPI for ObservedHost {}
#[async_trait::async_trait]
impl host::Host for ObservedHost {
    async fn normalize_resource(
        &self,
        path: PathBuf,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<String, host::ToolError> {
        let call = self.normalizations.fetch_add(1, Ordering::SeqCst) + 1;
        if self.slow_normalization_at == Some(call) {
            if let Some(cancel) = &self.cancel_on_slow {
                cancel.cancel();
            }
            // Host waiting exceeds the guest's CPU budget; explicit cancellation remains live.
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        host::Host::normalize_resource(&self.inner, path, cancellation).await
    }
    async fn cache_point(
        &self,
        request: host::CachePointRequest,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<host::FileCachePoint, host::ToolError> {
        self.captures.fetch_add(1, Ordering::SeqCst);
        let mut point = host::Host::cache_point(&self.inner, request, cancellation).await?;
        if let Some(path) = &self.response_path {
            point.path = path.clone();
        }
        Ok(point)
    }
    async fn write(
        &self,
        request: host::WriteRequest,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<host::WriteOutput, host::ToolError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        host::Host::write(&self.inner, request, cancellation).await
    }
}

fn invocation(call_id: &str, _arguments: Value) -> ToolContext {
    let mut context = ToolContext::detached(
        crate::SessionState::new(Default::default()),
        Default::default(),
    );
    context.call_id = call_id.into();
    context.tool = "write".into();
    crate::runtime::wasm::test_admission::admit(&mut context, None);
    context
}
fn store(
    host: Arc<dyn host::Host>,
    runtime: tokio::runtime::Handle,
    context: ToolContext,
    arguments: Value,
) -> Store<RuntimeState> {
    Store::new(
        &Engine::default(),
        RuntimeState {
            limits: StoreLimitsBuilder::new().build(),
            host: Some(host),
            outputs: Default::default(),
            file_reads: Default::default(),
            next_output: 1,
            borrowed_processes: Default::default(),
            admitted_process_scope: None,
            process_launch_uncertain: false,
            process_grants: Default::default(),
            binding: None,
            tool_context: Some(context),
            operation_scope: Some(("write".into(), arguments)),
            web: None,
            references: None,
            script_profile: crate::runtime::wasm::script::ScriptProfile::Standard,
            guest_observation: None,
            confinement_diagnostic: None,
            operation_cancelled: false,
            operation_diagnostic: None,
            operation_diagnostic_authoritative: false,
            known_write_committed: false,
            host_info_json: Err("unused".into()),
            runtime: Some(runtime),
            cancellation: tokio_util::sync::CancellationToken::new(),
            cooperative_driver_cancellation: false,
            epoch_cancellation: tokio_util::sync::CancellationToken::new(),
            deadline: Instant::now() + std::time::Duration::from_secs(30),
            max_input_bytes: 1_048_576,
            max_output_bytes: 1_048_576,
            max_media_output_bytes: 0,
            operation_calls_remaining: 16,
            input_bytes_remaining: 1_048_576,
            output_bytes_remaining: 1_048_576,
            cache_points_remaining: 2,
            patch_plan: None,
            patch_receipts: Vec::new(),
            patch_version_refs: Vec::new(),
        },
    )
}
#[test]
fn target_boundary_after_committed_write_preserves_effects_and_skips_authorization() {
    use crate::observation::diagnostics::{
        DiagnosticCode, DiagnosticLayer, DiagnosticPhase, EffectKnowledge,
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let root = tempfile::tempdir().unwrap();
    let host = Arc::new(ObservedHost {
        inner: microcrab_host_local::LocalHost::new(root.path(), Default::default()).unwrap(),
        captures: AtomicUsize::new(0),
        writes: AtomicUsize::new(0),
        response_path: None,
        normalizations: AtomicUsize::new(0),
        slow_normalization_at: None,
        cancel_on_slow: None,
    });
    let arguments = json!({"path":"first.txt","content":"committed"});
    let context = invocation("boundary-after-write", arguments.clone());
    let mut store = store(host.clone(), runtime.handle().clone(), context, arguments);
    write(
        store.as_context_mut(),
        (abi::WriteRequest {
            path: "first.txt".into(),
            content: "committed".into(),
            precondition: abi::WritePrecondition::Missing,
        },),
    )
    .unwrap()
    .0
    .unwrap();
    assert!(store.data().known_write_committed);
    let rejected = write(
        store.as_context_mut(),
        (abi::WriteRequest {
            path: "../outside.txt".into(),
            content: "must-not-write".into(),
            precondition: abi::WritePrecondition::Missing,
        },),
    )
    .unwrap()
    .0
    .unwrap_err();
    assert!(matches!(
        rejected.code,
        abi::OperationErrorCode::InvalidRequest
    ));
    assert_eq!(host.writes.load(Ordering::SeqCst), 1);
    assert_eq!(
        std::fs::read_to_string(root.path().join("first.txt")).unwrap(),
        "committed"
    );
    let diagnostic = &store.data().operation_diagnostic.as_ref().unwrap().0;
    assert_eq!(diagnostic.code, DiagnosticCode::PathOutsideRoot);
    assert_eq!(diagnostic.layer, DiagnosticLayer::Host);
    assert_eq!(diagnostic.phase, DiagnosticPhase::Validation);
    assert_eq!(diagnostic.effects, EffectKnowledge::Committed);
    assert_eq!(
        diagnostic.operation_effects,
        Some(EffectKnowledge::NotStarted)
    );
    assert!(store.data().confinement_diagnostic.is_some());
    assert!(!diagnostic.result_json().contains("outside.txt"));
}

#[test]
fn capture_normalization_waits_without_generic_deadline_and_honors_cancellation() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let root = tempfile::tempdir().unwrap();
    // One admission normalization precedes the request and response checks.
    for slow_at in [2, 3] {
        for cancelled in [false, true] {
            let cancellation = tokio_util::sync::CancellationToken::new();
            let host = Arc::new(ObservedHost {
                inner: microcrab_host_local::LocalHost::new(root.path(), Default::default())
                    .unwrap(),
                captures: AtomicUsize::new(0),
                writes: AtomicUsize::new(0),
                response_path: None,
                normalizations: AtomicUsize::new(0),
                slow_normalization_at: Some(slow_at),
                cancel_on_slow: cancelled.then(|| cancellation.clone()),
            });
            let arguments = json!({"path":"first.txt","content":"unused"});
            let context = invocation("host-wait", arguments.clone());
            let collector = context.state.clone();
            let mut store = store(host.clone(), runtime.handle().clone(), context, arguments);
            store.data_mut().cancellation = cancellation;
            let result = capture(store.as_context_mut(), ("first.txt".into(), 1_048_576))
                .unwrap()
                .0;
            if cancelled {
                assert!(matches!(
                    result.unwrap_err().code,
                    abi::OperationErrorCode::Cancelled
                ));
                assert_eq!(host.normalizations.load(Ordering::SeqCst), slow_at);
                assert_eq!(
                    host.captures.load(Ordering::SeqCst),
                    usize::from(slow_at == 3)
                );
                assert!(collector.cache_points().finish().unwrap().points.is_empty());
            } else {
                result.unwrap();
                assert_eq!(host.normalizations.load(Ordering::SeqCst), 3);
                assert_eq!(host.captures.load(Ordering::SeqCst), 1);
                assert_eq!(collector.cache_points().finish().unwrap().points.len(), 1);
            }
            assert_eq!(host.writes.load(Ordering::SeqCst), 0);
        }
    }
}

#[test]
fn cache_capture_binds_response_path_using_host_normalization() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let root = tempfile::tempdir().unwrap();
    for (requested, returned, accepted) in [
        ("first.txt", "other.txt", false),
        ("./first.txt", "first.txt", true),
        ("first.txt", "FIRST.txt", cfg!(windows)),
    ] {
        let host = Arc::new(ObservedHost {
            inner: microcrab_host_local::LocalHost::new(root.path(), Default::default()).unwrap(),
            captures: AtomicUsize::new(0),
            writes: AtomicUsize::new(0),
            response_path: Some(returned.into()),
            normalizations: AtomicUsize::new(0),
            slow_normalization_at: None,
            cancel_on_slow: None,
        });
        let arguments = json!({"path":"first.txt","content":"unused"});
        let context = invocation("capture", arguments.clone());
        let collector = context.state.clone();
        let mut store = store(host.clone(), runtime.handle().clone(), context, arguments);
        let result = capture(store.as_context_mut(), (requested.into(), 1_048_576))
            .unwrap()
            .0;
        if accepted {
            result.unwrap();
        } else {
            let error = result.unwrap_err();
            assert!(matches!(error.code, abi::OperationErrorCode::Io));
            assert_eq!(
                diagnostic(&error).summary(),
                "Host cache-point response differs from requested path"
            );
        }
        let report = collector.cache_points().finish().unwrap();
        assert_eq!(report.points.len(), usize::from(accepted));
        assert_eq!(host.captures.load(Ordering::SeqCst), 1);
        assert_eq!(host.writes.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn one_ask_admission_allows_multiple_imports_and_policy_edits_only_affect_new_calls() {
    use crate::policy::*;
    use futures_util::FutureExt;
    struct AskOnce(AtomicUsize);
    #[async_trait::async_trait]
    impl SessionPermission for AskOnce {
        async fn evaluate(
            &self,
            policy: &PolicySnapshot,
            request: &PermissionRequest,
        ) -> Result<Decision, PermissionError> {
            microcrab_permission::evaluate(policy, request)
                .map_err(|error| PermissionError::InvalidConfiguration(error.to_string()))
        }
        fn features(&self) -> PermissionFeatures {
            PermissionFeatures {
                interactive_ask: true,
                ..Default::default()
            }
        }
        async fn ask(&self, _: &PermissionRequest) -> Result<AskReply, PermissionError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(AskReply {
                action: Action::Allow,
                reason: "one invocation".into(),
            })
        }
    }
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let root = tempfile::tempdir().unwrap();
    let host = Arc::new(ObservedHost {
        inner: microcrab_host_local::LocalHost::new(root.path(), Default::default()).unwrap(),
        captures: AtomicUsize::new(0),
        writes: AtomicUsize::new(0),
        response_path: None,
        normalizations: AtomicUsize::new(0),
        slow_normalization_at: None,
        cancel_on_slow: None,
    });
    let adapter = Arc::new(AskOnce(AtomicUsize::new(0)));
    let controller = Arc::new(PermissionController::new(adapter.clone()));
    let config: PermissionConfig = serde_json::from_value(json!({"review":false,
        "overrides":[{"tool":{"kind":"exact","value":"write"},"action":"ask"}]}))
    .unwrap();
    controller.replace(1, config.clone()).unwrap();
    let arguments = json!({"path":"first.txt","content":"approved"});
    let mut context = invocation("one-admission", arguments.clone());
    // This test exercises a new real Ask admission rather than the fixture's Allow proof.
    context.admission = None;
    context.execution_id = None;
    context.ensure_detached_execution().unwrap();
    let mut request = super::super::test_admission::request(&context);
    request.arguments = arguments.clone();
    context.admission = Some(
        runtime
            .block_on(controller.admit(
                &request,
                &context,
                &super::super::test_admission::issuer(),
                Some("component-a".into()),
            ))
            .unwrap(),
    );
    assert!(context.ensure_admitted(Some("component-a")).is_ok());
    assert!(context.ensure_admitted(Some("component-b")).is_err());
    assert!(
        context
            .ensure_admitted_arguments(&json!({"path":"other.txt"}))
            .is_err()
    );
    let mut denied = config;
    denied.overrides[0].action = Action::Deny;
    controller
        .replace(controller.snapshot().unwrap().revision, denied)
        .unwrap();
    let mut store = store(
        host.clone(),
        runtime.handle().clone(),
        context.clone(),
        arguments,
    );
    for name in ["first.txt", "second.txt"] {
        write(
            store.as_context_mut(),
            (abi::WriteRequest {
                path: name.into(),
                content: "approved".into(),
                precondition: abi::WritePrecondition::Missing,
            },),
        )
        .unwrap()
        .0
        .unwrap();
    }
    assert_eq!(host.writes.load(Ordering::SeqCst), 2);
    assert_eq!(adapter.0.load(Ordering::SeqCst), 1);
    let new = controller
        .admit(
            &request,
            &context,
            &super::super::test_admission::issuer(),
            Some("component-a".into()),
        )
        .now_or_never()
        .expect("deterministic Deny never waits");
    assert!(matches!(new, Err(PermissionError::PolicyDenied { .. })));
    assert_eq!(adapter.0.load(Ordering::SeqCst), 1);
}

#[test]
fn scope_presence_without_proof_cannot_write_and_confinement_survives_handled_success() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let root = tempfile::tempdir().unwrap();
    let host = Arc::new(ObservedHost {
        inner: microcrab_host_local::LocalHost::new(root.path(), Default::default()).unwrap(),
        captures: AtomicUsize::new(0),
        writes: AtomicUsize::new(0),
        response_path: None,
        normalizations: AtomicUsize::new(0),
        slow_normalization_at: None,
        cancel_on_slow: None,
    });
    let arguments = json!({});
    let mut context = invocation("scope-without-proof", arguments.clone());
    context.admission = None;
    let mut rejected = store(
        host.clone(),
        runtime.handle().clone(),
        context,
        arguments.clone(),
    );
    let request = |path: &str| abi::WriteRequest {
        path: path.into(),
        content: "value".into(),
        precondition: abi::WritePrecondition::Missing,
    };
    assert!(
        write(rejected.as_context_mut(), (request("first.txt"),))
            .unwrap()
            .0
            .is_err()
    );
    assert_eq!(host.writes.load(Ordering::SeqCst), 0);
    assert_eq!(host.normalizations.load(Ordering::SeqCst), 0);
    let context = invocation("confined", arguments.clone());
    let mut accepted = store(host, runtime.handle().clone(), context, arguments);
    assert!(
        write(accepted.as_context_mut(), (request("../outside.txt"),))
            .unwrap()
            .0
            .is_err()
    );
    write(accepted.as_context_mut(), (request("inside.txt"),))
        .unwrap()
        .0
        .unwrap();
    assert!(accepted.data().operation_diagnostic.is_none());
    let diagnostic = accepted.data().confinement_diagnostic.as_ref().unwrap();
    assert_eq!(
        diagnostic.code,
        crate::observation::diagnostics::DiagnosticCode::PathOutsideRoot
    );
    assert!(accepted.data().known_write_committed);
    let final_failure = accepted
        .data()
        .confinement_failure()
        .unwrap()
        .tool_diagnostic();
    assert_eq!(
        final_failure.code,
        crate::observation::diagnostics::DiagnosticCode::PathOutsideRoot
    );
    assert_eq!(
        final_failure.effects,
        crate::observation::diagnostics::EffectKnowledge::Committed
    );
}

#[test]
fn mutation_bridge_preflights_entire_call_and_rejects_unplanned_order() {
    use super::super::microcrab::host0_1_0::{file_mutation as wire, text_write as write};
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let root = tempfile::tempdir().unwrap();
    let host: Arc<dyn host::Host> =
        Arc::new(microcrab_host_local::LocalHost::new(root.path(), Default::default()).unwrap());
    let request = |path: &str| {
        wire::FileMutation::Write(write::WriteRequest {
            path: path.into(),
            content: "new\n".into(),
            precondition: write::WritePrecondition::Missing,
        })
    };
    let mut denied = store(
        host.clone(),
        runtime.handle().clone(),
        invocation("small", json!({})),
        json!({}),
    );
    denied.data_mut().output_bytes_remaining = 16;
    let (result,) =
        super::mutations::preflight(denied.as_context_mut(), (vec![request("a"), request("b")],))
            .unwrap();
    assert!(result.is_err());
    assert!(!root.path().join("a").exists());
    assert!(!root.path().join("b").exists());
    let mut allowed = store(
        host,
        runtime.handle().clone(),
        invocation("ordered", json!({})),
        json!({}),
    );
    let (result,) = super::mutations::preflight(
        allowed.as_context_mut(),
        (vec![request("a"), request("b")],),
    )
    .unwrap();
    let mut normalized = result.unwrap();
    let second = normalized.pop().unwrap();
    let first = normalized.pop().unwrap();
    let (wrong,) = super::mutations::mutate(allowed.as_context_mut(), (second.clone(),)).unwrap();
    assert!(wrong.is_err());
    assert!(!root.path().join("b").exists());
    let (committed,) = super::mutations::mutate(allowed.as_context_mut(), (first,)).unwrap();
    assert!(committed.unwrap().destination_written);
    assert_eq!(
        std::fs::read_to_string(root.path().join("a")).unwrap(),
        "new\n"
    );
    // A stale later operation keeps its known committed prefix. No import retries
    // the prior mutation or restores files after conflict.
    std::fs::write(root.path().join("b"), "external").unwrap();
    let (stale,) = super::mutations::mutate(allowed.as_context_mut(), (second,)).unwrap();
    assert!(stale.is_err());
    assert_eq!(allowed.data().patch_receipts[0]["status"], "committed");
    assert_eq!(allowed.data().patch_receipts[1]["status"], "failed");
    assert_eq!(
        std::fs::read_to_string(root.path().join("b")).unwrap(),
        "external"
    );
}

struct CommitCancelHost {
    inner: microcrab_host_local::LocalHost,
    cancel: tokio_util::sync::CancellationToken,
}
impl host::ProcessApi for CommitCancelHost {}
impl host::FsApi for CommitCancelHost {}
impl host::NetApi for CommitCancelHost {}
impl host::shell::ShellApi for CommitCancelHost {}
impl host::HostAPI for CommitCancelHost {}
#[async_trait::async_trait]
impl host::Host for CommitCancelHost {
    fn tool_capabilities(&self) -> Vec<String> {
        vec!["file_mutation".into()]
    }
    async fn normalize_resource(
        &self,
        path: PathBuf,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<String, host::ToolError> {
        host::Host::normalize_resource(&self.inner, path, cancel).await
    }
    async fn preflight_mutations(
        &self,
        requests: Vec<host::FileMutation>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<Vec<host::FileMutation>, host::ToolError> {
        host::Host::preflight_mutations(&self.inner, requests, cancel).await
    }
    async fn mutate(
        &self,
        request: host::FileMutation,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<host::MutationOutput, host::ToolError> {
        let out = host::Host::mutate(&self.inner, request, cancel).await?;
        self.cancel.cancel();
        Ok(out)
    }
}
#[test]
fn mutation_bridge_awaits_known_publication_when_caller_cancels_after_commit() {
    use super::super::microcrab::host0_1_0::{file_mutation as wire, text_write as write};
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let root = tempfile::tempdir().unwrap();
    let cancel = tokio_util::sync::CancellationToken::new();
    let host: Arc<dyn host::Host> = Arc::new(CommitCancelHost {
        inner: microcrab_host_local::LocalHost::new(root.path(), Default::default()).unwrap(),
        cancel: cancel.clone(),
    });
    let mut state = store(
        host,
        runtime.handle().clone(),
        invocation("after-commit", json!({})),
        json!({}),
    );
    state.data_mut().cancellation = cancel.clone();
    let (plan,) = super::mutations::preflight(
        state.as_context_mut(),
        (vec![wire::FileMutation::Write(write::WriteRequest {
            path: "a".into(),
            content: "committed".into(),
            precondition: write::WritePrecondition::Missing,
        })],),
    )
    .unwrap();
    let (result,) =
        super::mutations::mutate(state.as_context_mut(), (plan.unwrap().pop().unwrap(),)).unwrap();
    assert!(cancel.is_cancelled());
    assert!(result.unwrap().destination_written);
    assert!(state.data().known_write_committed);
    assert_eq!(state.data().patch_receipts[0]["status"], "committed");
    assert_eq!(
        std::fs::read_to_string(root.path().join("a")).unwrap(),
        "committed"
    );
}

#[derive(Clone, Copy)]
enum ChangedPreflight {
    PathSpelling,
    Count,
    Order,
    Kind,
    WriteContent,
    WritePrecondition,
    DeleteVersion,
    MoveVersion,
    MoveContent,
    MoveDestination,
}
struct AlteredPreflightHost {
    change: ChangedPreflight,
    reads: AtomicUsize,
    captures: AtomicUsize,
    mutations: AtomicUsize,
}
impl host::ProcessApi for AlteredPreflightHost {}
impl host::FsApi for AlteredPreflightHost {}
impl host::NetApi for AlteredPreflightHost {}
impl host::shell::ShellApi for AlteredPreflightHost {}
impl host::HostAPI for AlteredPreflightHost {}
#[async_trait::async_trait]
impl host::Host for AlteredPreflightHost {
    fn tool_capabilities(&self) -> Vec<String> {
        vec!["file_mutation".into()]
    }
    async fn normalize_resource(
        &self,
        path: PathBuf,
        _: tokio_util::sync::CancellationToken,
    ) -> Result<String, host::ToolError> {
        // This fake Host explicitly declares ./ spelling equivalence. The Session
        // must ask this owner instead of implementing platform path semantics.
        let canonical: PathBuf = path
            .components()
            .filter(|part| !matches!(part, std::path::Component::CurDir))
            .collect();
        Ok(canonical.to_string_lossy().into_owned())
    }
    async fn preflight_mutations(
        &self,
        mut requests: Vec<host::FileMutation>,
        _: tokio_util::sync::CancellationToken,
    ) -> Result<Vec<host::FileMutation>, host::ToolError> {
        match self.change {
            ChangedPreflight::PathSpelling => {
                for request in &mut requests {
                    match request {
                        host::FileMutation::Write(r) => r.path = PathBuf::from(".").join(&r.path),
                        host::FileMutation::Delete(r) => r.path = PathBuf::from(".").join(&r.path),
                        host::FileMutation::Move(r) => {
                            r.path = PathBuf::from(".").join(&r.path);
                            r.destination = PathBuf::from(".").join(&r.destination);
                        }
                    }
                }
            }
            ChangedPreflight::Count => {
                requests.pop();
            }
            ChangedPreflight::Order => requests.swap(0, 1),
            ChangedPreflight::Kind => {
                requests[0] = host::FileMutation::Delete(host::DeleteRequest {
                    path: "a".into(),
                    expected_version: "a".repeat(64),
                })
            }
            ChangedPreflight::WriteContent => {
                if let host::FileMutation::Write(r) = &mut requests[0] {
                    r.content = "changed".into();
                }
            }
            ChangedPreflight::WritePrecondition => {
                if let host::FileMutation::Write(r) = &mut requests[0] {
                    r.precondition = host::WritePrecondition::Version("c".repeat(64));
                }
            }
            ChangedPreflight::DeleteVersion => {
                if let host::FileMutation::Delete(r) = &mut requests[2] {
                    r.expected_version = "c".repeat(64);
                }
            }
            ChangedPreflight::MoveVersion => {
                if let host::FileMutation::Move(r) = &mut requests[3] {
                    r.expected_version = "c".repeat(64);
                }
            }
            ChangedPreflight::MoveContent => {
                if let host::FileMutation::Move(r) = &mut requests[3] {
                    r.content = "changed".into();
                }
            }
            ChangedPreflight::MoveDestination => {
                if let host::FileMutation::Move(r) = &mut requests[3] {
                    r.destination = "unrelated".into();
                }
            }
        }
        Ok(requests)
    }
    async fn read_text(
        &self,
        request: host::ReadRequest,
        _: tokio_util::sync::CancellationToken,
    ) -> Result<host::ReadOutput, host::ToolError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(host::ReadOutput {
            content: "old\n".into(),
            start_line: request.offset_line,
            next_line: None,
            truncated: false,
            version: "a".repeat(64),
        })
    }
    async fn cache_point(
        &self,
        _: host::CachePointRequest,
        _: tokio_util::sync::CancellationToken,
    ) -> Result<host::FileCachePoint, host::ToolError> {
        self.captures.fetch_add(1, Ordering::SeqCst);
        Err(host::ToolError::invalid_request("unexpected capture"))
    }
    async fn mutate(
        &self,
        _: host::FileMutation,
        _: tokio_util::sync::CancellationToken,
    ) -> Result<host::MutationOutput, host::ToolError> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        Err(host::ToolError::invalid_request("unexpected mutation"))
    }
}
fn mutation_preflight_requests() -> Vec<super::super::microcrab::host0_1_0::file_mutation::FileMutation>
{
    use super::super::microcrab::host0_1_0::{file_mutation as wire, text_write as write};
    vec![
        wire::FileMutation::Write(write::WriteRequest {
            path: "a".into(),
            content: "same".into(),
            precondition: write::WritePrecondition::Missing,
        }),
        wire::FileMutation::Write(write::WriteRequest {
            path: "b".into(),
            content: "same".into(),
            precondition: write::WritePrecondition::Missing,
        }),
        wire::FileMutation::Delete(wire::DeleteRequest {
            path: "deleted".into(),
            expected_version: "a".repeat(64),
        }),
        wire::FileMutation::Move(wire::MoveRequest {
            path: "moved".into(),
            destination: "destination".into(),
            expected_version: "b".repeat(64),
            content: "final".into(),
        }),
    ]
}
#[test]
fn mutation_preflight_rejects_host_changed_effects_before_reads_reservations_or_effects() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    for (change, reason) in [
        (ChangedPreflight::Count, "operation count changed"),
        (ChangedPreflight::Order, "operation order changed"),
        (ChangedPreflight::Kind, "operation kind or order changed"),
        (ChangedPreflight::WriteContent, "write content changed"),
        (
            ChangedPreflight::WritePrecondition,
            "write precondition changed",
        ),
        (
            ChangedPreflight::DeleteVersion,
            "delete source version changed",
        ),
        (ChangedPreflight::MoveVersion, "move source version changed"),
        (
            ChangedPreflight::MoveContent,
            "move destination content changed",
        ),
        (ChangedPreflight::MoveDestination, "resource identity"),
    ] {
        let host = Arc::new(AlteredPreflightHost {
            change,
            reads: AtomicUsize::new(0),
            captures: AtomicUsize::new(0),
            mutations: AtomicUsize::new(0),
        });
        let mut state = store(
            host.clone(),
            runtime.handle().clone(),
            invocation("altered-preflight", json!({})),
            json!({}),
        );
        state.data_mut().operation_calls_remaining = 240;
        let (result,) =
            super::mutations::preflight(state.as_context_mut(), (mutation_preflight_requests(),))
                .unwrap();
        let error = result.unwrap_err();
        assert!(matches!(
            error.code,
            abi::OperationErrorCode::InvalidRequest
        ));
        assert!(
            error
                .message
                .contains("invalid Host file-mutation preflight response")
        );
        assert!(error.message.contains(reason));
        assert_eq!(host.reads.load(Ordering::SeqCst), 0);
        assert_eq!(host.captures.load(Ordering::SeqCst), 0);
        assert_eq!(host.mutations.load(Ordering::SeqCst), 0);
        assert!(state.data().patch_plan.is_none());
        assert!(state.data().patch_receipts.is_empty());
        assert_eq!(state.data().cache_points_remaining, 2);
        // A rejected Host response has not occupied the round's evidence envelope.
        state
            .data()
            .tool_context
            .as_ref()
            .unwrap()
            .reserve_cache_points(128, 1)
            .unwrap();
    }
}
#[test]
fn mutation_preflight_accepts_only_host_equivalent_path_spelling_changes() {
    use super::super::microcrab::host0_1_0::file_mutation as wire;
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let host = Arc::new(AlteredPreflightHost {
        change: ChangedPreflight::PathSpelling,
        reads: AtomicUsize::new(0),
        captures: AtomicUsize::new(0),
        mutations: AtomicUsize::new(0),
    });
    let mut state = store(
        host.clone(),
        runtime.handle().clone(),
        invocation("path-spelling", json!({})),
        json!({}),
    );
    state.data_mut().operation_calls_remaining = 240;
    let (result,) =
        super::mutations::preflight(state.as_context_mut(), (mutation_preflight_requests(),))
            .unwrap();
    let normalized = result.unwrap();
    assert_eq!(normalized.len(), 4);
    assert!(
        matches!(&normalized[0],wire::FileMutation::Write(r) if r.path.replace('\\',"/")=="./a"&&r.content=="same")
    );
    assert!(
        matches!(&normalized[3],wire::FileMutation::Move(r) if r.path.replace('\\',"/")=="./moved"&&r.destination.replace('\\',"/")=="./destination"&&r.content=="final")
    );
    assert_eq!(host.reads.load(Ordering::SeqCst), 2);
    assert_eq!(host.captures.load(Ordering::SeqCst), 0);
    assert_eq!(host.mutations.load(Ordering::SeqCst), 0);
}

#[test]
fn failed_mutation_preflight_never_publishes_a_plan_or_leaks_evidence_capacity() {
    use super::super::microcrab::host0_1_0::{file_mutation as wire, text_write as write};
    let runtime = tokio::runtime::Runtime::new().unwrap();
    for exhaust_output in [true, false] {
        let host = Arc::new(AlteredPreflightHost {
            change: ChangedPreflight::PathSpelling,
            reads: AtomicUsize::new(0),
            captures: AtomicUsize::new(0),
            mutations: AtomicUsize::new(0),
        });
        let mut state = store(
            host.clone(),
            runtime.handle().clone(),
            invocation("failed-plan", json!({})),
            json!({}),
        );
        let native = vec![host::FileMutation::Write(host::WriteRequest {
            path: "a".into(),
            content: "same".into(),
            precondition: host::WritePrecondition::Missing,
        })];
        let input_bytes = serde_json::to_vec(&native).unwrap().len();
        let request = || {
            wire::FileMutation::Write(write::WriteRequest {
                path: "a".into(),
                content: "same".into(),
                precondition: write::WritePrecondition::Missing,
            })
        };
        if exhaust_output {
            // The original receipt alone fits, but the normalized response and
            // longer equivalent spelling leave insufficient receipt capacity.
            state.data_mut().output_bytes_remaining = 6 + 256 + 16_384 + 256;
        } else {
            // Original request/preflight fits. Its response is observed before
            // the later normalized future-import input envelope is rejected.
            state.data_mut().input_bytes_remaining = input_bytes * 2;
        }
        let before_output = state.data().output_bytes_remaining;
        let (result,) =
            super::mutations::preflight(state.as_context_mut(), (vec![request()],)).unwrap();
        let error = result.unwrap_err();
        assert!(matches!(error.code, abi::OperationErrorCode::Limit));
        if exhaust_output {
            assert!(error.message.contains("receipt capacity"));
        } else {
            assert!(error.message.contains("input"));
            assert!(state.data().output_bytes_remaining < before_output);
        }
        assert!(state.data().patch_plan.is_none());
        assert!(state.data().patch_receipts.is_empty());
        assert_eq!(state.data().cache_points_remaining, 2);
        assert!(!state.data().cooperative_driver_cancellation);
        // A custom Guest may ignore the failed import. It must still have no
        // executable plan, even with enough bytes for a subsequent mutate call.
        state.data_mut().input_bytes_remaining = 1_048_576;
        let (mutation,) = super::mutations::mutate(state.as_context_mut(), (request(),)).unwrap();
        let error = mutation.unwrap_err();
        assert!(error.message.contains("whole-call preflight"));
        assert_eq!(host.captures.load(Ordering::SeqCst), 0);
        assert_eq!(host.mutations.load(Ordering::SeqCst), 0);
        state
            .data()
            .tool_context
            .as_ref()
            .unwrap()
            .reserve_cache_points(128, 1)
            .unwrap();
    }
}
