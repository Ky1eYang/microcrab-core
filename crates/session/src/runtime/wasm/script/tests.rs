use super::*;
use crate::runtime::execution::ExecutionRegistry;
use crate::runtime::execution::process::ProcessRegistry;
use crate::{Limits, SessionState, State};
use microcrab_host_api::{Host, OpContext, ReadOutput, ReadRequest, ToolError};
use microcrab_host_local::LocalHost;
use serde_json::json;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

async fn make_ctx(
    phase: Phase,
    profile: ScriptProfile,
    host: Option<Arc<dyn Host>>,
) -> (
    State,
    Option<Arc<crate::state::endpoint::VariableEndpoint>>,
    ScriptRuntimeContext,
) {
    let limits = Limits::default();
    let (state, endpoint) = if phase == Phase::Init {
        let (state, ep) = State::new(limits.clone()).bind_writer(
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        );
        (state, Some(ep))
    } else {
        (State::new(limits.clone()), None)
    };
    if let Some(h) = host.clone() {
        state.install_host(Some(h));
    }
    let mut tc = super::super::ToolContext::new(
        state.clone(),
        ProcessRegistry::detached(limits.clone()),
        CancellationToken::new(),
        "call_script".into(),
        "test_tool".into(),
        "sess_script".into(),
        "agent".into(),
        Some("plan".into()),
    );
    if phase == Phase::Init {
        tc.subject = crate::policy::InvocationSubject::AgentInit {
            component: "comp".into(),
            generation: 1,
            attempt: 1,
        };
    } else {
        crate::runtime::wasm::test_admission::admit(&mut tc, None);
    }
    let ingress = references::Ingress::new(&tc, &CancellationToken::new())
        .await
        .unwrap();
    let ctx = ScriptRuntimeContext {
        phase,
        profile,
        binding: Some(SessionBinding::from_state(state.clone())),
        tool_context: Some(tc),
        references: Some(ingress),
        host,
        runtime: Some(tokio::runtime::Handle::current()),
        cancellation: CancellationToken::new(),
        epoch_cancellation: CancellationToken::new(),
        waited_ns: 0,
    };
    let _ = ExecutionRegistry::new(limits);
    let _ = OpContext::default();
    let _ = ReadRequest {
        path: std::path::PathBuf::from("AGENTS.md"),
        offset_line: 1,
        max_lines: 10,
        max_bytes: 1024,
    };
    let _ = ReadOutput {
        content: String::new(),
        start_line: 1,
        next_line: None,
        truncated: false,
        version: String::new(),
    };
    let _ = ToolError::unsupported("unused");
    (state, endpoint, ctx)
}

async fn run_eval(
    ctx: ScriptRuntimeContext,
    script: &'static str,
    args_json: Option<String>,
) -> Result<(String, u64), (SessionError, u64)> {
    tokio::task::spawn_blocking(move || evaluate_script(ctx, script, args_json.as_deref()))
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn phase_validation_denies_session_control_builtins_during_init() {
    let (_state, _ep, ctx) = make_ctx(Phase::Init, ScriptProfile::Standard, None).await;
    let err = run_eval(ctx, r#"session::switch_mode("build")"#, None)
        .await
        .unwrap_err();
    assert!(matches!(err.0.code, ErrorCode::Denied));

    let (_state2, _ep2, ctx2) = make_ctx(Phase::Init, ScriptProfile::Standard, None).await;
    let err2 = run_eval(ctx2, r#"vars::call_facts()"#, None)
        .await
        .unwrap_err();
    assert!(matches!(err2.0.code, ErrorCode::Unavailable));
}

#[tokio::test(flavor = "multi_thread")]
async fn web_transport_profile_restricts_variables_and_host_mounts() {
    let (_state, _ep, ctx) = make_ctx(Phase::Invoke, ScriptProfile::WebTransport, None).await;
    let err = run_eval(ctx, r#"vars::get("x")"#, None).await.unwrap_err();
    assert!(matches!(err.0.code, ErrorCode::Denied));

    let (_state2, _ep2, ctx2) = make_ctx(Phase::Invoke, ScriptProfile::WebTransport, None).await;
    let facts = run_eval(ctx2, r#"vars::call_facts()"#, None).await.unwrap();
    let parsed: Value = serde_json::from_str(&facts.0).unwrap();
    assert_eq!(parsed["session_id"], "sess_script");
    assert_eq!(parsed["admitted_mode"], "plan");
}

#[tokio::test(flavor = "multi_thread")]
async fn sandbox_limits_infinite_loops_and_blocks_eval() {
    let (_state, _ep, ctx) = make_ctx(Phase::Init, ScriptProfile::Standard, None).await;
    let err = run_eval(ctx, "loop {}", None).await.unwrap_err();
    assert!(matches!(err.0.code, ErrorCode::Limit));

    let (_state2, _ep2, ctx2) = make_ctx(Phase::Init, ScriptProfile::Standard, None).await;
    let err2 = run_eval(ctx2, r#"eval("1 + 1")"#, None).await.unwrap_err();
    assert!(matches!(err2.0.code, ErrorCode::ScriptError));
}

#[tokio::test(flavor = "multi_thread")]
async fn init_script_stages_variables_and_host_markdown_with_recovery() {
    let dir = TempDir::new().unwrap();
    std::fs::write(
        dir.path().join("AGENTS.md"),
        "# Root Rules\nFollow rules.\n## Sub Rule\nBe exact.\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("pkg")).unwrap();
    std::fs::write(
        dir.path().join("pkg/AGENTS.md"),
        "# Pkg Rules\nScoped instruction.\n",
    )
    .unwrap();
    let host: Arc<dyn Host> = Arc::new(
        LocalHost::new_filesystem(dir.path(), Default::default()).unwrap(),
    );
    let (state, _ep, ctx) = make_ctx(Phase::Init, ScriptProfile::Standard, Some(host.clone())).await;
    let script = r#"
        vars::set_scoped("glob_ignored", args.ignored, "visible", "persisted");
        let specs = args.files.map(|p| #{ path: p, format: "markdown", target_kind: "references" });
        refs::mount_host(specs)
    "#;
    let args = json!({
        "ignored": ["target/"],
        "files": ["AGENTS.md", "pkg/AGENTS.md"],
    })
    .to_string();
    let (out, _) = run_eval(ctx.clone(), script, Some(args)).await.unwrap();
    let ids: Vec<String> = serde_json::from_str(&out).unwrap();
    assert_eq!(ids.len(), 2);

    let mut ingress = ctx.references.clone().unwrap();
    let staged = ingress.take().unwrap();
    let tc = ctx.tool_context.clone().unwrap();
    staged.commit(&tc, &CancellationToken::new()).await.unwrap();

    assert_eq!(state.wasm_extract("glob_ignored").unwrap(), Some(json!(["target/"])));
    let listed = state.list_references().unwrap();
    assert!(listed.iter().any(|r| r.entry.location.as_deref() == Some("AGENTS.md")));
    assert!(listed.iter().any(|r| r.entry.location.as_deref() == Some("pkg/AGENTS.md")));
    assert!(listed.iter().any(|r| r.entry.title == "Sub Rule"));

    // Verify recovery snapshot preserves dynamic mounts and restores them cleanly.
    let serialized = serde_json::to_string(&state.snapshot()).unwrap();
    let recovered_snapshot: SessionState = serde_json::from_str(&serialized).unwrap();
    let fresh = State::from_snapshot(recovered_snapshot);
    fresh.install_host(Some(host));
    fresh.load_references(CancellationToken::new()).await.unwrap();
    let restored = fresh.list_references().unwrap();
    assert!(restored.iter().any(|r| r.entry.location.as_deref() == Some("pkg/AGENTS.md")));
}
