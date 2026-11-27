use super::*;
use crate::{
    Limits, SessionKind, State,
    context::Content,
    runtime::{
        BuiltinTool, Runtime, ToolRegistration,
        execution::{CancellationController, ExecutionSnapshot, process::ProcessRegistry},
        tool::{DenyAll, Tool},
    },
};
use serde_json::{Value, json};
use std::sync::{Arc, atomic::AtomicBool};
use tokio_util::sync::CancellationToken;

const RAW: &[u8] = b"\xff\0\x80PRIVATE_NOT_PUBLIC";
const MIME: &str = "application/octet-stream";

fn setup() -> (
    State,
    Arc<crate::state::endpoint::VariableEndpoint>,
    ToolContext,
) {
    let limits = Limits {
        max_execution_buffers: 1,
        max_tool_image_bytes: 1, // This is not a Personal Content Library quota.
        ..Default::default()
    };
    let (state, endpoint) = State::new(limits.clone()).bind_writer(
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    let mem = crate::state::reference::DynReference::new(
        crate::state::reference::test_memory::InMemoryTestReference::new(
            "references",
            1024 * 1024,
            64,
        ),
    )
    .unwrap();
    state
        .install_references_with_default(
            crate::state::reference::References::new(vec![mem]).unwrap(),
            Some("references".into()),
        )
        .unwrap();
    state.update(|live| live.session_kind = Some(SessionKind::Interactive));
    let context = ToolContext::new(
        state.clone(),
        ProcessRegistry::detached(limits),
        CancellationToken::new(),
        "redirect-call".into(),
        "job".into(),
        "owner".into(),
        "agent".into(),
        None,
    );
    (state, endpoint, context)
}

async fn source(state: &State) -> Result<ExecutionId, Error> {
    let owner = state.executions();
    let registry = owner.clone();
    let mut handle = owner.start(
        ExecutionSnapshot::new(
            "owner".into(),
            "producer-call".into(),
            "shell".into(),
            "agent".into(),
            None,
        ),
        CancellationToken::new(),
        move |progress| async move {
            let (writer, buffered) = registry.open_private_output(
                progress.execution_id(),
                MIME.into(),
                Arc::new(CancellationController::new(CancellationToken::new())),
            )?;
            writer.append_private(RAW)?;
            writer.finish(Ok(()))?;
            Ok(ToolOutput::Buffered(buffered))
        },
    )?;
    handle.activate()?;
    let result = handle.wait().await;
    handle.detach();
    result.map(|_| handle.id)
}

fn redirect(source: ExecutionId) -> Value {
    json!({"id":format!("owner/{}", source.0),"action":"redirect","args":{"channel":"stdout","expected_mime":MIME,"title":"captured binary"}})
}

async fn bind(context: &mut ToolContext, arguments: &Value) -> ExecutionId {
    use crate::policy::{Action, ArgumentPredicate, Matcher, Permission};
    let runtime = Runtime::new(
        vec![ToolRegistration::Builtin(BuiltinTool::Job)],
        Arc::new(DenyAll),
    )
    .unwrap();
    let controller = runtime.permission_controller();
    let snapshot = controller.snapshot().unwrap();
    let mut config = snapshot.config;
    config.overrides.push(Permission {
        agent: None,
        mode: None,
        tool: Some(Matcher::Exact("job".into())),
        args: vec![ArgumentPredicate {
            pointer: "/action".into(),
            matcher: Matcher::Exact("redirect".into()),
        }],
        action: Action::Allow,
    });
    controller.replace(snapshot.revision, config).unwrap();
    let descriptor = ExecutionSnapshot::new(
        context.session_id.clone(),
        context.call_id.clone(),
        context.tool.clone(),
        context.agent.clone(),
        context.mode.clone(),
    );
    let operation = context
        .state
        .executions()
        .admit_detached(descriptor, context.cancellation.clone())
        .unwrap();
    context.execution_id = Some(operation);
    let issuer = Content {
        epoch: 0,
        subagent: None,
        system: "Job redirect fixture".into(),
        items: vec![],
        tools: vec![],
        max_output_tokens: 1024,
        max_bytes: 65536,
        turn_id: None,
        round: None,
    };
    let call = microcrab_protocol::ToolCall {
        provider_metadata: None,
        id: context.call_id.clone(),
        name: context.tool.clone(),
        arguments: arguments.clone(),
    };
    runtime
        .admit_invocation(&call, context, &issuer)
        .await
        .unwrap();
    operation
}

#[tokio::test]
async fn admitted_redirect_mounts_library_reference_and_source_release_is_independent() {
    let (state, _endpoint, mut context) = setup();
    let id = source(&state).await.unwrap();
    let owner = state.executions();
    let lease = owner.private_output(id, "owner", "stdout", MIME).unwrap();
    drop(lease);
    assert!(owner.private_output(id, "other", "stdout", MIME).is_err());
    assert!(
        owner
            .private_output(id, "owner", "stdout", "image/png")
            .is_err()
    );
    let page = owner.read(id, 0, 1024).unwrap();
    assert!(page.chunks.is_empty());
    assert_eq!(page.private_stdout.unwrap().media_type, MIME);
    let arguments = redirect(id);
    let _operation = bind(&mut context, &arguments).await;
    let mut altered = arguments.clone();
    altered["args"]["expected_mime"] = json!("image/png");
    assert!(
        super::super::Job
            .execute(altered, context.clone())
            .await
            .is_err()
    );
    let ToolOutput::Final(output) = super::super::Job
        .execute(arguments.clone(), context.clone())
        .await
        .unwrap()
    else {
        panic!("metadata-only receipt")
    };
    assert!(output.attachments.is_empty());
    assert!(!output.content.contains("PRIVATE_NOT_PUBLIC"));
    let receipt: Value = serde_json::from_str(&output.content).unwrap();
    assert_eq!(receipt["source_released"], false);
    assert_eq!(receipt["reference"]["title"], "captured binary");
    assert_eq!(receipt["reference"]["media_type"], MIME);
    let ref_id = receipt["reference"]["id"].as_str().unwrap().to_owned();
    owner.release(id, CancellationToken::new()).await.unwrap();
    assert!(owner.private_output(id, "owner", "stdout", MIME).is_err());
    let stored = state
        .read_reference_async(&ref_id, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(stored.media_type, MIME);
    assert_eq!(stored.as_bytes(), RAW);
    owner.close().await.unwrap();
}

#[tokio::test]
async fn library_quota_limit_blocks_overflow_without_lru() {
    let limits = Limits {
        max_execution_buffers: 1,
        ..Default::default()
    };
    let state = State::new(limits);
    let mem = crate::state::reference::DynReference::new(
        crate::state::reference::test_memory::InMemoryTestReference::new("references", 1024, 2),
    )
    .unwrap();
    state
        .install_references_with_default(
            crate::state::reference::References::new(vec![mem]).unwrap(),
            Some("references".into()),
        )
        .unwrap();
    let registry = state.executions();
    let id = source(&state).await.unwrap();
    let lease = registry.private_output(id, "owner", "stdout", MIME).unwrap();
    let signal = CancellationToken::new();
    for index in 0..2 {
        let dest = state
            .prepare_library_redirect(MIME, Some(&format!("item_{index}")))
            .unwrap();
        state
            .commit_library_redirect(dest, lease.clone(), ExecutionId(index + 10), &signal)
            .await
            .unwrap();
    }
    let overflow = state
        .prepare_library_redirect(MIME, Some("overflow"))
        .unwrap();
    assert!(matches!(
        state
            .commit_library_redirect(overflow, lease, ExecutionId(100), &signal)
            .await,
        Err(Error::Limit(limit)) if limit.kind == LimitKind::LibraryStorageEntries
    ));
    registry.close().await.unwrap();
}

#[tokio::test]
async fn redirect_reserves_its_full_receipt_budget_before_publication() {
    let (state, _endpoint, mut context) = setup();
    state.update(|live| live.limits.max_tool_output_bytes = MAX_RECEIPT_BYTES - 1);
    let id = source(&state).await.unwrap();
    let arguments = redirect(id);
    bind(&mut context, &arguments).await;
    assert!(matches!(
        super::super::Job.execute(arguments, context).await,
        Err(Error::Limit(limit)) if limit.kind == LimitKind::ToolOutput
    ));
    assert!(
        state
            .list_references()
            .unwrap()
            .is_empty()
    );
    state.executions().close().await.unwrap();
}

#[test]
fn literal_scalar_only_and_uncertain_ack_is_not_retryable() {
    let good = redirect(ExecutionId(12));
    for (path, invalid) in [
        ("/id", json!(["owner/12"])),
        ("/id", json!("[\"owner/12\"]")),
        ("/id", json!("owner/012")),
        ("/args/channel", json!("stderr")),
        ("/args/expected_mime", json!("application/*")),
        ("/args/expected_mime", json!("Application/octet-stream")),
        ("/args/expected_mime", json!("text/plain; charset=utf-8")),
        ("/args/expected_mime", json!("application/octet-stream\n")),
        (
            "/args/expected_mime",
            json!(format!("a/{}", "a".repeat(126))),
        ),
        ("/args/title", json!("")),
        ("/args/title", json!("   ")),
    ] {
        let mut call = good.clone();
        *call.pointer_mut(path).unwrap() = invalid;
        assert!(super::super::validate(&call).is_err());
    }
    assert!(super::super::validate(&good).is_ok());
    let diagnostic = super::super::uncertain_redirect(ExecutionId(43)).tool_diagnostic();
    assert_eq!(
        diagnostic.effects,
        crate::observation::diagnostics::EffectKnowledge::MayHaveOccurred
    );
    assert_eq!(diagnostic.retryable, Some(false));
    assert!(diagnostic.summary().contains("execution 43"));
}
