//! Behavioral coverage is registered by session_context, never a fifth suite.
use async_trait::async_trait;
use microcrab_protocol::{Response, ToolCall, ToolDefinition, Usage};
use microcrab_session::context::{
    AutoCompactionPolicy, Content, ContentItem, Context, TokenEstimate, TokenEstimator,
};
use microcrab_session::runtime::{
    Runtime, ToolRegistration,
    execution::ToolOutput,
    journal::{Journal, Recover},
    tool::{AllowTools, Tool, ToolContext},
};
use microcrab_session::{Auditor, Client, Error, Limits, Session};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio_util::sync::CancellationToken;

const SUMMARY: &str = "## Request and Intent\nContinue work.\n## Technical Concepts\nKeep ownership.\n## Files and Code\nnone\n## Errors and Fixes\nnone\n## Pending Jobs\nnone\n## Current Work\nTool completed.\n## Next Step\nAnswer.\n## Critical Context\nPreserve authorization.";
fn response(text: &str) -> Response {
    Response {
        content: text.into(),
        tool_calls: vec![],
        usage: Usage {
            input_tokens: (text.len() >= 35_000).then_some(34_000),
            output_tokens: Some(7),
            samples: 1,
            ..Default::default()
        },
        provider_metadata: None,
    }
}
struct Estimate {
    available: bool,
}
impl TokenEstimator for Estimate {
    fn estimate(&self, content: &Content) -> Result<Option<TokenEstimate>, Error> {
        if !self.available {
            return Ok(None);
        }
        let mut estimate =
            TokenEstimate::conservative_text(content, "fixture:model:default".into(), 40_000)?
                .unwrap();
        // Fixture measurements are deterministic boundary facts, separate from
        // production heuristic calibration. Tool pressure exceeds capacity.
        if content.items.iter().any(|item| matches!(item, ContentItem::Tool { .. })) { estimate.input_tokens = 39_500; }
        else if content.items.iter().any(|item| matches!(item, ContentItem::Assistant { content, .. } if content.len() >= 35_000)) { estimate.input_tokens = 34_000; }
        Ok(Some(estimate))
    }
}
#[derive(Default)]
struct Log {
    rows: Mutex<Vec<Recover>>,
    reject_summary: AtomicBool,
}
#[async_trait]
impl Journal for Log {
    async fn commit(&self, value: Recover) -> Result<(), Error> {
        value.validate()?;
        if value.context.baseline_generation > 0 && self.reject_summary.load(Ordering::SeqCst) {
            return Err(Error::Protocol("fixture storage failure".into()));
        }
        self.rows.lock().unwrap().push(value);
        Ok(())
    }
}
struct Model {
    replies: Mutex<VecDeque<Result<Response, microcrab_transport::Error>>>,
    seen: Mutex<Vec<Content>>,
    block: AtomicBool,
    entered: tokio::sync::Notify,
}
impl Model {
    fn new(replies: Vec<Result<Response, microcrab_transport::Error>>) -> Arc<Self> {
        Arc::new(Self {
            replies: Mutex::new(replies.into()),
            seen: Mutex::new(vec![]),
            block: AtomicBool::new(false),
            entered: tokio::sync::Notify::new(),
        })
    }
}
#[async_trait]
impl Client for Model {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        token: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.seen.lock().unwrap().push(content.clone());
        if content.subagent.as_deref() == Some("auto_compact") && self.block.load(Ordering::SeqCst)
        {
            self.entered.notify_one();
            token.cancelled().await;
            return Err(microcrab_transport::Error::Cancelled);
        }
        self.replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted response")
    }
}
struct Evidence;
#[async_trait]
impl Tool for Evidence {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "evidence".into(),
            description: "Settled tool evidence".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn execute(&self, _: Value, _: ToolContext) -> Result<ToolOutput, Error> {
        Ok(ToolOutput::text("x".repeat(38_000)))
    }
}
fn runtime() -> Runtime {
    Runtime::new(
        vec![ToolRegistration::External(Arc::new(Evidence))],
        Arc::new(AllowTools::new(["evidence"])),
    )
    .unwrap()
}
fn builder(
    model: Arc<Model>,
    log: Arc<Log>,
    enabled: bool,
    available: bool,
) -> microcrab_session::SessionBuilder {
    Session::builder_arc(model, runtime())
        .system("Frozen system")
        .limits(Limits {
            max_output_tokens: 1024,
            max_tool_output_bytes: 40_000,
            ..Default::default()
        })
        .journal_arc(log)
        .auto_compaction(
            AutoCompactionPolicy {
                enabled,
                turn_threshold: 0.8,
                round_threshold: 0.95,
                headroom_tokens: 0,
            },
            Arc::new(Estimate { available }),
        )
}
fn call() -> Response {
    let mut value = response("inspect");
    value.usage.input_tokens = Some(39_500);
    value.tool_calls.push(ToolCall {
        id: "exact-call".into(),
        name: "evidence".into(),
        arguments: json!({}),
        provider_metadata: None,
    });
    value
}

#[tokio::test]
async fn final_round_applies_turn_policy_once_and_preserves_prefix_and_usage() {
    let model = Model::new(vec![
        Ok(response(&"x".repeat(35_000))),
        Ok(response(SUMMARY)),
        Ok(response("next")),
    ]);
    let log = Arc::new(Log::default());
    let session = builder(model.clone(), log, true, true).build().unwrap();
    let outcome = session.submit("start").await.unwrap();
    assert_eq!(outcome.rounds, 1);
    assert_eq!(outcome.usage.output_tokens, Some(7));
    let compacted = session.context_snapshot().unwrap();
    assert_eq!(compacted.baseline_generation, 1);
    assert_eq!(compacted.epoch, 0);
    assert!(!compacted.pending_epoch);
    session.submit("next").await.unwrap();
    {
        let seen = model.seen.lock().unwrap();
        assert_eq!(seen.len(), 3);
        assert_eq!(seen[1].subagent.as_deref(), Some("auto_compact"));
        assert_eq!(seen[1].system, seen[0].system);
        assert_eq!(seen[1].tools, seen[0].tools);
        assert_eq!(&seen[1].items[..seen[0].items.len()], &seen[0].items);
        assert_eq!(seen[2].turn_id, Some(2));
    }
    session.close().await.unwrap();
}

#[tokio::test]
async fn round_compaction_follows_tool_checkpoint_and_keeps_turn_round_and_calls() {
    let model = Model::new(vec![
        Ok(call()),
        Ok(response(SUMMARY)),
        Ok(response("done")),
    ]);
    let log = Arc::new(Log::default());
    let session = builder(model.clone(), log.clone(), true, true)
        .build()
        .unwrap();
    let outcome = session.submit("start").await.unwrap();
    assert_eq!(outcome.rounds, 2);
    assert_eq!(outcome.usage.output_tokens, Some(14));
    {
        let seen = model.seen.lock().unwrap();
        assert_eq!(seen.len(), 3);
        assert_eq!(seen[2].turn_id, seen[0].turn_id);
        assert_eq!(seen[2].round, Some(2));
        assert_eq!(seen[2].epoch, seen[0].epoch);
        assert!(seen[1].items.iter().any(|item| matches!(item, ContentItem::Tool { call_id, content, .. } if call_id == "exact-call" && content.len() == 38_000)));
        let rows = log.rows.lock().unwrap();
        let settled = rows
            .iter()
            .find(|row| {
                row.context.baseline_generation == 0
                    && row.continuation.is_some()
                    && row
                        .context
                        .items
                        .iter()
                        .any(|item| matches!(item, ContentItem::Tool { .. }))
            })
            .unwrap();
        assert_eq!(
            &seen[1].items[..seen[1].items.len() - 1],
            &settled.context.items
        );
        let compacted = rows
            .iter()
            .find(|row| row.context.baseline_generation == 1)
            .unwrap();
        assert_eq!(compacted.continuation.as_ref().unwrap().next_round, 2);
        assert_eq!(
            compacted.continuation.as_ref().unwrap().unattended_rounds,
            1
        );
        assert!(compacted.context.retired_call_ids.contains("exact-call"));
    }
    session.close().await.unwrap();
}

#[tokio::test]
async fn disabled_unknown_and_invalid_thresholds_do_not_issue_helpers() {
    for (enabled, available) in [(false, true), (true, false)] {
        let model = Model::new(vec![Ok(response(&"x".repeat(35_000)))]);
        let session = builder(model.clone(), Arc::new(Log::default()), enabled, available)
            .build()
            .unwrap();
        session.submit("start").await.unwrap();
        assert_eq!(model.seen.lock().unwrap().len(), 1);
        assert_eq!(session.context_snapshot().unwrap().baseline_generation, 0);
        session.close().await.unwrap();
    }
    for (turn, round) in [(0.0, 0.95), (0.95, 0.8), (0.8, 1.0), (f64::NAN, 0.95)] {
        let policy = AutoCompactionPolicy {
            enabled: true,
            turn_threshold: turn,
            round_threshold: round,
            headroom_tokens: 0,
        };
        assert!(policy.validate().is_err());
    }
}

#[tokio::test]
async fn cancellation_retains_settled_continuation_and_recovery_never_repeats_helper() {
    let model = Model::new(vec![
        Ok(call()),
        Ok(response(SUMMARY)),
        Ok(response("resumed")),
    ]);
    model.block.store(true, Ordering::SeqCst);
    let log = Arc::new(Log::default());
    let session = builder(model.clone(), log.clone(), true, true)
        .build()
        .unwrap();
    let worker_session = session.clone();
    let worker = tokio::spawn(async move { worker_session.submit("start").await });
    model.entered.notified().await;
    session.interrupt();
    assert!(matches!(worker.await.unwrap(), Err(Error::Cancelled)));
    let checkpoint = session.checkpoint().await.unwrap();
    assert_eq!(checkpoint.context.baseline_generation, 0);
    assert!(checkpoint.context.auto_compaction_attempt.is_some());
    assert_eq!(checkpoint.continuation.as_ref().unwrap().next_round, 2);
    assert_eq!(
        checkpoint
            .continuation
            .as_ref()
            .unwrap()
            .usage
            .output_tokens,
        Some(7)
    );
    session.close().await.unwrap();
    let restored = Session::restore(
        builder(model.clone(), Arc::new(Log::default()), true, true),
        checkpoint,
    )
    .await
    .unwrap();
    restored.start().await.unwrap();
    // Pressure is still over capacity, so explicit resume must pause rather than
    // silently repeat the helper or dispatch another main request.
    assert!(restored.resume().await.is_err());
    assert_eq!(model.seen.lock().unwrap().len(), 2);
    assert!(restored.auto_compaction_retry_available());
    model.block.store(false, Ordering::SeqCst);
    assert!(restored.retry_auto_compaction().await.unwrap());
    assert!(!restored.auto_compaction_retry_available());
    let outcome = restored.resume().await.unwrap();
    assert_eq!(outcome.rounds, 2);
    assert_eq!(outcome.usage.output_tokens, Some(14));
    restored.close().await.unwrap();
}

#[tokio::test]
async fn nonshrinking_summary_retains_original_context_and_no_retry_loop() {
    let oversized = format!("{SUMMARY}\n{}", "y".repeat(40_000));
    let model = Model::new(vec![
        Ok(response(&"x".repeat(35_000))),
        Ok(response(&oversized)),
        Ok(response("next")),
    ]);
    let session = builder(model.clone(), Arc::new(Log::default()), true, true)
        .build()
        .unwrap();
    session.submit("start").await.unwrap();
    assert_eq!(session.context_snapshot().unwrap().baseline_generation, 0);
    for _ in 0..3 {
        session.context_measurement().unwrap();
    }
    assert_eq!(model.seen.lock().unwrap().len(), 2);
    assert_eq!(
        model
            .seen
            .lock()
            .unwrap()
            .iter()
            .filter(|row| row.subagent.is_some())
            .count(),
        1
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn storage_rejection_never_publishes_compacted_context() {
    let model = Model::new(vec![
        Ok(response(&"x".repeat(35_000))),
        Ok(response(SUMMARY)),
    ]);
    let log = Arc::new(Log::default());
    log.reject_summary.store(true, Ordering::SeqCst);
    let session = builder(model, log.clone(), true, true).build().unwrap();
    assert!(session.submit("start").await.is_err());
    assert_eq!(session.context_snapshot().unwrap().baseline_generation, 0);
    assert!(
        log.rows
            .lock()
            .unwrap()
            .iter()
            .all(|row| row.context.baseline_generation == 0)
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn prepared_failed_request_retry_keeps_exact_issued_content() {
    let model = Model::new(vec![
        Err(microcrab_transport::Error::Transport),
        Ok(response("recovered")),
    ]);
    let session = builder(model.clone(), Arc::new(Log::default()), true, true)
        .build()
        .unwrap();
    assert!(session.submit("start").await.is_err());
    session.resume().await.unwrap();
    {
        let seen = model.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(
            serde_json::to_value(&seen[0]).unwrap(),
            serde_json::to_value(&seen[1]).unwrap()
        );
    }
    session.close().await.unwrap();
}

#[test]
fn baseline_generation_preserves_legacy_bytes_and_retired_call_identity() {
    let mut context = Context::new("frozen".into(), vec![Evidence.definition()], 100_000).unwrap();
    let legacy = serde_json::to_value(context.snapshot().unwrap()).unwrap();
    assert!(legacy.get("baseline_generation").is_none());
    assert!(legacy.get("retired_call_ids").is_none());
    assert!(legacy.get("auto_compaction_attempt").is_none());
    context.admit_user("start", &json!({})).unwrap();
    context.append_response(&call()).unwrap();
    context
        .append_tool_result("exact-call".into(), "settled".into())
        .unwrap();
    context
        .compact_baseline(SUMMARY.into(), &json!({}))
        .unwrap();
    let mut restored = Context::from_snapshot(context.snapshot().unwrap()).unwrap();
    assert_eq!(restored.baseline_generation(), 1);
    assert_eq!(restored.epoch(), 0);
    assert!(!restored.pending_epoch());
    assert!(restored.append_response(&call()).is_err());
}

#[derive(Default)]
struct BufferedControl {
    writer: Mutex<Option<microcrab_session::execution::OutputWriter>>,
    cancelled: AtomicBool,
}
#[async_trait]
impl microcrab_session::execution::ExecutionController for BufferedControl {
    async fn cancel(&self, _: CancellationToken) -> Result<(), Error> {
        self.cancelled.store(true, Ordering::SeqCst);
        if let Some(writer) = self.writer.lock().unwrap().take() {
            writer.finish(Err(microcrab_session::execution::ToolFailure::failed(
                "cancelled",
                "explicit fixture cancellation",
            )))?;
        }
        Ok(())
    }
    async fn release(&self, token: CancellationToken) -> Result<(), Error> {
        self.cancel(token).await
    }
}
struct BufferedEvidence(Arc<BufferedControl>);
#[async_trait]
impl Tool for BufferedEvidence {
    fn definition(&self) -> ToolDefinition {
        Evidence.definition()
    }
    async fn execute(&self, _: Value, context: ToolContext) -> Result<ToolOutput, Error> {
        let (writer, output) = context.open_output(Default::default(), self.0.clone())?;
        writer.append(None, "before compaction")?;
        *self.0.writer.lock().unwrap() = Some(writer);
        Ok(ToolOutput::Buffered(output))
    }
}
#[tokio::test]
async fn live_buffered_execution_survives_in_turn_compaction_with_same_identity() {
    let controller = Arc::new(BufferedControl::default());
    let mut first = call();
    first.content = "x".repeat(35_000);
    let model = Model::new(vec![Ok(first), Ok(response(SUMMARY)), Ok(response("done"))]);
    let runtime = Runtime::new(
        vec![ToolRegistration::External(Arc::new(BufferedEvidence(
            controller.clone(),
        )))],
        Arc::new(AllowTools::new(["evidence"])),
    )
    .unwrap();
    let session = Session::builder_arc(model, runtime)
        .system("Frozen system")
        .limits(Limits {
            max_output_tokens: 1024,
            max_tool_output_bytes: 40_000,
            ..Default::default()
        })
        .auto_compaction(
            AutoCompactionPolicy {
                enabled: true,
                turn_threshold: 0.8,
                round_threshold: 0.95,
                headroom_tokens: 0,
            },
            Arc::new(Estimate { available: true }),
        )
        .build()
        .unwrap();
    let outcome = session.submit("start").await.unwrap();
    assert_eq!(outcome.rounds, 2);
    assert_eq!(session.context_snapshot().unwrap().baseline_generation, 1);
    assert!(!controller.cancelled.load(Ordering::SeqCst));
    let writer = controller.writer.lock().unwrap().as_ref().unwrap().clone();
    let id = writer.execution_id();
    let row = session
        .execution_snapshots()
        .into_iter()
        .find(|row| row.execution_id == id)
        .unwrap();
    assert!(row.status.is_pending());
    assert_eq!(row.call_id, "exact-call");
    writer.append(None, "after compaction").unwrap();
    let page = session.read_execution_output(id, 0, 1024).unwrap();
    let output: String = page.chunks.into_iter().map(|chunk| chunk.content).collect();
    assert!(output.contains("before compaction"));
    assert!(output.contains("after compaction"));
    writer.finish(Ok(())).unwrap();
    session.close().await.unwrap();
}

#[tokio::test]
async fn known_provider_truncation_and_invalid_sections_retain_original_and_allow_explicit_retry() {
    for helper in [
        {
            let mut value = response(SUMMARY);
            value.usage.output_tokens = None;
            value
                .usage
                .provider_details
                .push(json!({"finishReason":"MAX_TOKENS"}));
            value
        },
        {
            let mut value = response(SUMMARY);
            value.usage.output_tokens = None;
            value
                .usage
                .provider_details
                .push(json!({"stop_reason":"max_tokens"}));
            value
        },
        response(&SUMMARY.replacen("## Technical Concepts", "## Wrong", 1)),
        response(&SUMMARY.replacen("Keep ownership.", "", 1)),
        response(""),
        call(),
    ] {
        let model = Model::new(vec![
            Ok(response(&"x".repeat(35_000))),
            Ok(helper),
            Ok(response(SUMMARY)),
        ]);
        let session = builder(model.clone(), Arc::new(Log::default()), true, true)
            .build()
            .unwrap();
        session.submit("start").await.unwrap();
        assert_eq!(session.context_snapshot().unwrap().baseline_generation, 0);
        assert!(session.auto_compaction_retry_available());
        assert!(session.retry_auto_compaction().await.unwrap());
        assert_eq!(session.context_snapshot().unwrap().baseline_generation, 1);
        assert!(!session.auto_compaction_retry_available());
        assert_eq!(model.seen.lock().unwrap().len(), 3);
        session.close().await.unwrap();
    }
}

#[tokio::test]
async fn close_cancels_explicit_compaction_retry_and_retains_original_baseline() {
    let model = Model::new(vec![
        Ok(response(&"x".repeat(35_000))),
        Ok(response("invalid summary")),
    ]);
    let log = Arc::new(Log::default());
    let session = builder(model.clone(), log.clone(), true, true)
        .build()
        .unwrap();
    session.submit("start").await.unwrap();
    assert!(session.auto_compaction_retry_available());
    model.block.store(true, Ordering::SeqCst);
    let retry_session = session.clone();
    let retry = tokio::spawn(async move { retry_session.retry_auto_compaction().await });
    model.entered.notified().await;
    tokio::time::timeout(std::time::Duration::from_secs(2), session.close())
        .await
        .expect("close must interrupt the admitted helper")
        .unwrap();
    assert!(matches!(
        retry.await.unwrap(),
        Err(Error::Cancelled) | Err(Error::Closed)
    ));
    assert_eq!(session.context_snapshot().unwrap().baseline_generation, 0);
    assert!(
        log.rows
            .lock()
            .unwrap()
            .iter()
            .all(|row| row.context.baseline_generation == 0)
    );
}

#[tokio::test]
async fn disabled_policy_does_not_add_final_round_checkpoints() {
    let model = Model::new(vec![Ok(response("done"))]);
    let log = Arc::new(Log::default());
    let session = builder(model, log.clone(), false, true).build().unwrap();
    session.submit("start").await.unwrap();
    let baseline_log = Arc::new(Log::default());
    let baseline = Session::builder_arc(Model::new(vec![Ok(response("done"))]), runtime())
        .system("Frozen system")
        .limits(Limits {
            max_output_tokens: 1024,
            max_tool_output_bytes: 40_000,
            ..Default::default()
        })
        .journal_arc(baseline_log.clone())
        .build()
        .unwrap();
    baseline.submit("start").await.unwrap();
    assert_eq!(
        log.rows.lock().unwrap().len(),
        baseline_log.rows.lock().unwrap().len()
    );
    session.close().await.unwrap();
    baseline.close().await.unwrap();
}

// An inflated legacy estimate proves that only completed input usage triggers.
struct UsageEstimate {
    route: Mutex<String>,
}
impl UsageEstimate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            route: Mutex::new("fixture:usage:default".into()),
        })
    }
}
impl TokenEstimator for UsageEstimate {
    fn estimate(&self, content: &Content) -> Result<Option<TokenEstimate>, Error> {
        Ok(Some(TokenEstimate {
            route: self.route.lock().unwrap().clone(),
            provenance: "fixture_legacy_one_token_per_byte".into(),
            input_tokens: microcrab_session::context::ContextMeasurement::for_content(content)?
                .bytes as u64,
            context_window_tokens: 500_000,
        }))
    }
}
fn measured_response(text: &str, input: Option<u64>) -> Response {
    let mut value = response(text);
    value.usage.input_tokens = input;
    value
}
fn usage_builder(
    model: Arc<Model>,
    estimator: Arc<UsageEstimate>,
) -> microcrab_session::SessionBuilder {
    Session::builder_arc(model, runtime())
        .system("Frozen system")
        .limits(Limits {
            max_tool_output_bytes: 40_000,
            ..Default::default()
        })
        .auto_compaction(AutoCompactionPolicy::server_default(), estimator)
}

#[tokio::test]
async fn measured_large_context_avoids_false_compaction_and_survives_selected_recovery() {
    let model = Model::new(vec![
        Ok(measured_response("done", Some(121_700))),
        Ok(measured_response("next", Some(122_000))),
    ]);
    let estimator = UsageEstimate::new();
    let session = usage_builder(model.clone(), estimator.clone())
        .build()
        .unwrap();
    session.submit("x".repeat(400_000)).await.unwrap();
    assert_eq!(session.context_snapshot().unwrap().baseline_generation, 0);
    let checkpoint = session.checkpoint().await.unwrap();
    let context = Context::from_snapshot(checkpoint.context.clone()).unwrap();
    let estimate = context
        .estimate_tokens(estimator.as_ref(), 4096)
        .unwrap()
        .unwrap();
    assert_eq!(estimate.input_tokens, 121_700);
    session.close().await.unwrap();
    let restored = Session::restore(usage_builder(model.clone(), estimator), checkpoint)
        .await
        .unwrap();
    restored.submit("next").await.unwrap();
    assert_eq!(restored.context_snapshot().unwrap().baseline_generation, 0);
    assert_eq!(model.seen.lock().unwrap().len(), 2);
    restored.close().await.unwrap();
}

#[tokio::test]
async fn real_high_input_triggers_compaction_even_when_heuristic_is_lower() {
    let model = Model::new(vec![
        Ok(measured_response("done", Some(400_000))),
        Ok(response(SUMMARY)),
    ]);
    let session = usage_builder(model.clone(), UsageEstimate::new())
        .build()
        .unwrap();
    session.submit("x".repeat(20_000)).await.unwrap();
    assert_eq!(session.context_snapshot().unwrap().baseline_generation, 1);
    assert_eq!(model.seen.lock().unwrap().len(), 2);
    session.close().await.unwrap();
}

#[tokio::test]
async fn assistant_suffix_does_not_invent_unreported_input_pressure() {
    let model = Model::new(vec![Ok(measured_response(
        &"y".repeat(40_000),
        Some(390_000),
    ))]);
    let session = usage_builder(model.clone(), UsageEstimate::new())
        .build()
        .unwrap();
    session.submit("x".repeat(20_000)).await.unwrap();
    assert_eq!(session.context_snapshot().unwrap().baseline_generation, 0);
    assert_eq!(model.seen.lock().unwrap().len(), 1);
    session.close().await.unwrap();
}

#[tokio::test]
async fn measured_base_plus_tools_and_state_passes_round_boundary_and_send_capacity_gate() {
    let mut first = call();
    first.usage.input_tokens = Some(120_000);
    let model = Model::new(vec![
        Ok(first),
        Ok(measured_response("done", Some(130_000))),
    ]);
    let session = usage_builder(model.clone(), UsageEstimate::new())
        .build()
        .unwrap();
    // The old full estimate exceeds capacity after the result, but cannot
    // trigger compaction or block another request under the exact-usage policy.
    session.submit("x".repeat(460_000)).await.unwrap();
    assert_eq!(session.context_snapshot().unwrap().baseline_generation, 0);
    {
        let seen = model.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert!(seen.iter().all(|content| content.subagent.is_none()));
    }
    session.close().await.unwrap();
}

#[tokio::test]
async fn missing_and_aggregate_usage_skip_and_known_zero_remains_valid() {
    for input in [None, Some(0), Some(400_000)] {
        let mut reply = measured_response("done", input);
        if input == Some(400_000) {
            reply.usage.samples = 2;
        }
        let model = Model::new(vec![Ok(reply)]);
        let estimator = UsageEstimate::new();
        let session = usage_builder(model.clone(), estimator.clone())
            .build()
            .unwrap();
        session.submit("x".repeat(400_000)).await.unwrap();
        let context = Context::from_snapshot(session.context_snapshot().unwrap()).unwrap();
        let measured = context.estimate_tokens(estimator.as_ref(), 4096).unwrap();
        if input == Some(0) {
            assert_eq!(measured.unwrap().input_tokens, 0);
        } else {
            assert!(measured.is_none());
        }
        assert_eq!(session.context_snapshot().unwrap().baseline_generation, 0);
        assert_eq!(model.seen.lock().unwrap().len(), 1);
        session.close().await.unwrap();
    }
}
#[tokio::test]
async fn latest_missing_usage_cannot_reuse_previous_known_input() {
    let model = Model::new(vec![
        Ok(measured_response("first", Some(120_000))),
        Ok(measured_response("second", None)),
    ]);
    let session = usage_builder(model.clone(), UsageEstimate::new())
        .build()
        .unwrap();
    session.submit("x".repeat(400_000)).await.unwrap();
    session.submit("next").await.unwrap();
    assert_eq!(session.context_snapshot().unwrap().baseline_generation, 0);
    assert!(session.context_snapshot().unwrap().usage_evidence.is_none());
    assert_eq!(model.seen.lock().unwrap().len(), 2);
    session.close().await.unwrap();
}

#[tokio::test]
async fn changed_route_cannot_reuse_provider_input_for_unchanged_context() {
    let model = Model::new(vec![Ok(measured_response("done", Some(120_000)))]);
    let estimator = UsageEstimate::new();
    let session = usage_builder(model, estimator.clone()).build().unwrap();
    session.submit("x".repeat(400_000)).await.unwrap();
    let context = Context::from_snapshot(session.context_snapshot().unwrap()).unwrap();
    let original = context
        .estimate_tokens(estimator.as_ref(), 4096)
        .unwrap()
        .unwrap();
    assert!(original.input_tokens < 121_000);
    *estimator.route.lock().unwrap() = "fixture:other:default".into();
    assert!(
        context
            .estimate_tokens(estimator.as_ref(), 4096)
            .unwrap()
            .is_none()
    );
    session.close().await.unwrap();
}

#[test]
fn cold_context_has_no_reported_pressure_even_with_large_heuristic() {
    let mut context = Context::new("frozen".into(), vec![], 1_000_000).unwrap();
    context.admit_user("x".repeat(600_000), &json!({})).unwrap();
    let content = context.content(4096).unwrap();
    let heuristic = TokenEstimate::conservative_text(&content, "fixture:model".into(), 500_000)
        .unwrap()
        .unwrap();
    assert!(heuristic.pressure(4096, 0).unwrap() > 1.0);
    assert!(
        context
            .estimate_tokens(UsageEstimate::new().as_ref(), 4096)
            .unwrap()
            .is_none()
    );
}

struct FailingEstimate;
impl TokenEstimator for FailingEstimate {
    fn estimate(&self, _: &Content) -> Result<Option<TokenEstimate>, Error> {
        Err(Error::Config("disabled estimator must not run".into()))
    }
}
#[tokio::test]
async fn disabled_policy_never_calls_fallible_estimator() {
    let model = Model::new(vec![Ok(measured_response("done", Some(400_000)))]);
    let session = Session::builder_arc(model.clone(), runtime())
        .system("Frozen system")
        .auto_compaction(AutoCompactionPolicy::default(), Arc::new(FailingEstimate))
        .build()
        .unwrap();
    session.submit("start").await.unwrap();
    assert_eq!(model.seen.lock().unwrap().len(), 1);
    assert!(session.context_snapshot().unwrap().usage_evidence.is_none());
    session.close().await.unwrap();
}

struct TextOnlyEstimate;
impl TokenEstimator for TextOnlyEstimate {
    fn estimate(&self, content: &Content) -> Result<Option<TokenEstimate>, Error> {
        TokenEstimate::conservative_text(content, "fixture:image:model".into(), 500_000)
    }
}
#[tokio::test]
async fn exact_image_request_usage_can_publish_a_smaller_summary() {
    let model = Model::new(vec![
        Ok(measured_response("done", Some(400_000))),
        Ok(response(SUMMARY)),
    ]);
    let session = Session::builder_arc(model.clone(), runtime())
        .system("Frozen system")
        .auto_compaction(
            AutoCompactionPolicy::server_default(),
            Arc::new(TextOnlyEstimate),
        )
        .build()
        .unwrap();
    session
        .submit(microcrab_protocol::SessionInput {
            parts: vec![
                microcrab_protocol::InputPart::Text {
                    text: "x".repeat(20_000),
                },
                microcrab_protocol::InputPart::Image {
                    source: microcrab_protocol::ImageSource::Data {
                        media_type: "image/jpeg".into(),
                        data: vec![0xff, 0xd8, 0xff, 0xc0, 0, 11, 8, 0, 1, 0, 1, 1, 1, 0x11, 0],
                    },
                },
            ],
            variables: Default::default(),
        })
        .await
        .unwrap();
    assert_eq!(model.seen.lock().unwrap().len(), 2);
    assert_eq!(session.context_snapshot().unwrap().baseline_generation, 1);
    assert!(session.context_snapshot().unwrap().usage_evidence.is_none());
    session.close().await.unwrap();
}

#[tokio::test]
async fn usage_anchor_rejects_changed_prefix_and_baseline_and_compaction_retires_it() {
    let model = Model::new(vec![Ok(measured_response("done", Some(120_000)))]);
    let estimator = UsageEstimate::new();
    let session = usage_builder(model, estimator.clone()).build().unwrap();
    session.submit("x".repeat(20_000)).await.unwrap();
    let snapshot = session.context_snapshot().unwrap();
    let mut changed_prefix = snapshot.clone();
    changed_prefix.system.push_str(" changed");
    assert!(Context::from_snapshot(changed_prefix).is_err());
    let mut changed_baseline = snapshot.clone();
    changed_baseline.baseline_generation += 1;
    assert!(Context::from_snapshot(changed_baseline).is_err());
    let mut compacted = Context::from_snapshot(snapshot).unwrap();
    compacted
        .compact_baseline(SUMMARY.into(), &json!({}))
        .unwrap();
    assert!(compacted.snapshot().unwrap().usage_evidence.is_none());
    assert!(
        compacted
            .estimate_tokens(estimator.as_ref(), 4096)
            .unwrap()
            .is_none()
    );
    session.close().await.unwrap();
}
