//! Causal scope ownership at the actual NativeAuditor admission/publication boundary.
use super::*;
use microcrab_auditor::{AuditOutcome, EmptyVariableView, HookPayload};
use serde_json::Value;

const WIRE: &str = r#"{"messages":[{"role":"user","content":"fixture"}]}"#;

async fn prepare(auditor: &NativeAuditor, call: &str, epoch: u64, helper: Option<&str>) -> String {
    auditor
        .intervene(
            Moment::RequestPrepared,
            HookContext {
                epoch,
                turn: Some(1),
                round: Some(1),
                call_id: Some(call.into()),
                request_body: Some(WIRE),
                max_request_bytes: Some(65536),
                variables: &EmptyVariableView,
                payload: Default::default(),
                subagent: helper.map(str::to_owned),
            },
        )
        .await
        .unwrap();
    auditor.request_head(call).unwrap()
}
fn sent(auditor: &NativeAuditor, call: &str, epoch: u64, helper: Option<&str>) {
    auditor.publish(
        Moment::RequestPrepared,
        AuditBody {
            epoch,
            call_id: Some(call.into()),
            subagent: helper.map(str::to_owned),
            sent_request: Some(WIRE.into()),
            ..Default::default()
        },
    );
    auditor.publish(
        Moment::RoundCompleted,
        AuditBody {
            epoch,
            call_id: Some(call.into()),
            subagent: helper.map(str::to_owned),
            outcome: Some(AuditOutcome::Accepted),
            ..Default::default()
        },
    );
}
async fn request(auditor: &NativeAuditor, call: &str, epoch: u64, helper: Option<&str>) -> String {
    let identity = prepare(auditor, call, epoch, helper).await;
    sent(auditor, call, epoch, helper);
    identity
}
fn effective(directory: &Path) -> HashMap<String, Value> {
    let recorder = Recorder::open(directory, RecorderOptions::default()).unwrap();
    recorder
        .rows()
        .unwrap()
        .into_iter()
        .filter_map(|row| {
            (row["data"]["kind"] == "effective").then(|| {
                (
                    row["sha256"].as_str().unwrap().to_owned(),
                    row["data"].clone(),
                )
            })
        })
        .collect()
}
fn parent(rows: &HashMap<String, Value>, id: &str) -> Option<String> {
    rows[id]["parent_request"].as_str().map(str::to_owned)
}

#[tokio::test]
async fn main_and_each_helper_have_separate_sent_parent_chains() {
    let directory = tempfile::tempdir().unwrap();
    let anchor = Arc::new(Mutex::new(AuditAnchor::default()));
    let auditor = NativeAuditor::open(directory.path(), anchor.clone(), RecorderOptions::default())
        .await
        .unwrap();
    let a = request(&auditor, "main-a", 0, None).await;
    let r1 = request(&auditor, "rename-1", 0, Some("rename")).await;
    let review1 = request(&auditor, "review-1", 0, Some("action_review")).await;
    assert_eq!(anchor.lock().unwrap().request_head.as_ref(), Some(&a));
    let b = request(&auditor, "main-b", 0, None).await;
    let unsent = prepare(&auditor, "rename-never-sent", 0, Some("rename")).await;
    let r2 = request(&auditor, "rename-2", 0, Some("rename")).await;
    let review2 = request(&auditor, "review-2", 0, Some("action_review")).await;
    assert_eq!(anchor.lock().unwrap().request_head.as_ref(), Some(&b));
    auditor.close().await.unwrap();
    let rows = effective(directory.path());
    assert_eq!(parent(&rows, &a), None);
    assert_eq!(parent(&rows, &b), Some(a));
    assert_eq!(parent(&rows, &r1), None);
    assert_ne!(parent(&rows, &r2), Some(unsent));
    assert_eq!(parent(&rows, &r2), Some(r1));
    assert_eq!(parent(&rows, &review1), None);
    assert_eq!(parent(&rows, &review2), Some(review1));
}

#[tokio::test]
async fn epoch_and_checkpoint_movement_never_borrow_helper_or_main_baselines() {
    let directory = tempfile::tempdir().unwrap();
    let anchor = Arc::new(Mutex::new(AuditAnchor::default()));
    let auditor = NativeAuditor::open(directory.path(), anchor.clone(), RecorderOptions::default())
        .await
        .unwrap();
    let a = request(&auditor, "main-0", 0, None).await;
    let r0 = request(&auditor, "rename-0", 0, Some("rename")).await;
    let r1 = request(&auditor, "rename-epoch-1", 1, Some("rename")).await;
    let b = request(&auditor, "main-epoch-1", 1, None).await;
    anchor.lock().unwrap().history_head = Some("1".repeat(64));
    let r2 = request(&auditor, "rename-after-checkpoint", 1, Some("rename")).await;
    let c = request(&auditor, "main-after-checkpoint", 1, None).await;
    auditor.close().await.unwrap();
    let rows = effective(directory.path());
    assert_eq!(parent(&rows, &r0), None);
    assert_eq!(parent(&rows, &r1), None);
    assert_eq!(parent(&rows, &b), None);
    assert_eq!(parent(&rows, &r2), None);
    assert_eq!(parent(&rows, &c), Some(b));
    assert_ne!(a, c);
}

#[tokio::test]
async fn rollback_and_late_old_branch_sends_cannot_replace_selected_main_authority() {
    let directory = tempfile::tempdir().unwrap();
    let anchor = Arc::new(Mutex::new(AuditAnchor::default()));
    let auditor = NativeAuditor::open(directory.path(), anchor.clone(), RecorderOptions::default())
        .await
        .unwrap();
    let a = request(&auditor, "main-a", 0, None).await;
    let b = request(&auditor, "main-abandoned", 0, None).await;
    request(&auditor, "rename-abandoned", 0, Some("rename")).await;
    prepare(&auditor, "late-main", 0, None).await;
    prepare(&auditor, "late-helper", 0, Some("rename")).await;
    *anchor.lock().unwrap() = AuditAnchor {
        history_head: Some("2".repeat(64)),
        display_lineage: String::new(),
        request_head: Some(a.clone()),
    };
    sent(&auditor, "late-main", 0, None);
    sent(&auditor, "late-helper", 0, Some("rename"));
    assert_eq!(anchor.lock().unwrap().request_head.as_ref(), Some(&a));
    let r = request(&auditor, "rename-new-branch", 0, Some("rename")).await;
    let c = request(&auditor, "main-new-branch", 0, None).await;
    auditor.close().await.unwrap();
    let rows = effective(directory.path());
    assert_eq!(parent(&rows, &r), None);
    assert_eq!(parent(&rows, &c), Some(a));
    assert_ne!(parent(&rows, &c), Some(b));
}

#[tokio::test]
async fn reopen_uses_checkpoint_main_head_and_restoration_invalidates_helper_admissions() {
    let directory = tempfile::tempdir().unwrap();
    let anchor = Arc::new(Mutex::new(AuditAnchor::default()));
    let auditor = NativeAuditor::open(directory.path(), anchor.clone(), RecorderOptions::default())
        .await
        .unwrap();
    let a = request(&auditor, "main-a", 0, None).await;
    request(&auditor, "rename-before-close", 0, Some("rename")).await;
    request(&auditor, "main-later-branch", 0, None).await;
    auditor.close().await.unwrap();
    anchor.lock().unwrap().request_head = Some(a.clone());
    let auditor = NativeAuditor::open(directory.path(), anchor.clone(), RecorderOptions::default())
        .await
        .unwrap();
    let r = request(&auditor, "rename-reopened", 0, Some("rename")).await;
    let b = request(&auditor, "main-reopened", 0, None).await;
    prepare(&auditor, "rename-pending-restore", 0, Some("rename")).await;
    auditor.publish(Moment::SessionRestored, AuditBody::default());
    sent(&auditor, "rename-pending-restore", 0, Some("rename"));
    let next = request(&auditor, "rename-after-restore", 0, Some("rename")).await;
    assert_eq!(anchor.lock().unwrap().request_head.as_ref(), Some(&b));
    auditor.close().await.unwrap();
    let rows = effective(directory.path());
    assert_eq!(parent(&rows, &r), None);
    assert_eq!(parent(&rows, &b), Some(a));
    assert_eq!(parent(&rows, &next), None);
}

#[tokio::test]
async fn legacy_helper_checkpoint_head_is_not_reinterpreted_as_a_main_parent() {
    let directory = tempfile::tempdir().unwrap();
    let anchor = Arc::new(Mutex::new(AuditAnchor::default()));
    let auditor = NativeAuditor::open(directory.path(), anchor.clone(), RecorderOptions::default())
        .await
        .unwrap();
    request(&auditor, "main-old", 0, None).await;
    let helper = request(&auditor, "rename-old", 0, Some("rename")).await;
    auditor.close().await.unwrap();
    // Legacy checkpoints could contain a helper head. Do not rewrite that checkpoint
    // or fabricate an older main parent by walking the chronological audit log.
    anchor.lock().unwrap().request_head = Some(helper.clone());
    let auditor = NativeAuditor::open(directory.path(), anchor.clone(), RecorderOptions::default())
        .await
        .unwrap();
    let main = request(&auditor, "main-after-legacy", 0, None).await;
    auditor.close().await.unwrap();
    let rows = effective(directory.path());
    assert_eq!(parent(&rows, &main), None);
    assert_ne!(anchor.lock().unwrap().request_head.as_ref(), Some(&helper));
}

#[tokio::test]
async fn tool_stage_keeps_its_admission_branch_and_never_advances_provider_authority() {
    let directory = tempfile::tempdir().unwrap();
    let first = "a".repeat(64);
    let second = "b".repeat(64);
    let anchor = Arc::new(Mutex::new(AuditAnchor {
        history_head: Some(first.clone()),
        display_lineage: String::new(),
        request_head: None,
    }));
    let auditor = NativeAuditor::open(directory.path(), anchor.clone(), RecorderOptions::default())
        .await
        .unwrap();
    let main = request(&auditor, "same-id", 2, None).await;
    let tool = |start, elapsed| AuditBody {
        epoch: 2,
        turn_id: Some(1),
        round: Some(1),
        tool_call_id: Some("same-id".into()),
        started_at_unix_ms: Some(start),
        elapsed_ms: elapsed,
        payload: if elapsed.is_some() {
            HookPayload::ToolResult {
                result: Some("receipt".into()),
                error: None,
            }
        } else {
            HookPayload::ToolCall {
                name: "pwsh".into(),
                arguments: serde_json::json!({}),
            }
        },
        ..Default::default()
    };
    auditor.publish(Moment::ToolCall, tool(100, None));
    anchor.lock().unwrap().history_head = Some(second.clone());
    auditor.publish(Moment::ToolCall, tool(200, None));
    auditor.publish(Moment::ToolResult, tool(100, Some(30)));
    auditor.publish(Moment::ToolResult, tool(200, Some(5)));
    assert_eq!(anchor.lock().unwrap().request_head.as_ref(), Some(&main));
    auditor.close().await.unwrap();
    let result = crate::audit::query_internal(
        directory.path(),
        &crate::audit::AuditQuery {
            epoch: 2,
            turn: None,
            round: None,
            request_hash: None,
            history_head: None,
            subagent: None,
            call_id: None,
            tool_call_id: None,
        },
    )
    .unwrap();
    assert_eq!(result.stages.len(), 2);
    assert_eq!(result.stages[0].history_head.as_ref(), Some(&first));
    assert_eq!(result.stages[1].history_head.as_ref(), Some(&second));
    assert_eq!(
        (result.stages[0].started_at_ms, result.stages[0].ended_at_ms),
        (Some(100), Some(130))
    );
    assert_eq!(
        (result.stages[1].started_at_ms, result.stages[1].ended_at_ms),
        (Some(200), Some(205))
    );
}
