//! Request rewrite coverage registered in the existing session_context suite.
use async_trait::async_trait;
use microcrab_auditor::{
    AuditBody, Auditor, AuditorError, BoxFuture, HookContext, HookPayload, Injection, Moment,
    VariableView,
};
use microcrab_protocol::{Response, Usage};
use microcrab_session::context::{
    AutoCompactionPolicy, Content, ContextMeasurement, TokenEstimate, TokenEstimator,
};
use microcrab_session::runtime::{
    Runtime,
    journal::{Journal, Recover},
    tool::AllowTools,
};
use microcrab_session::{Client, Error, Limits, Session};
use serde_json::json;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio_util::sync::CancellationToken;

const REWRITTEN_TEXT: &str = "private-request-rewrite-fixture-only";
const SUMMARY: &str = "## Request and Intent\nContinue work.\n## Technical Concepts\nKeep ownership.\n## Files and Code\nnone\n## Errors and Fixes\nnone\n## Pending Jobs\nnone\n## Current Work\nResponse completed.\n## Next Step\nAnswer.\n## Critical Context\nPreserve authorization.";

// Keep a high legacy byte estimate explicit: unavailable exact request evidence
// must skip compaction even when this heuristic would cross the Turn threshold.
struct LegacyEstimate;
impl TokenEstimator for LegacyEstimate {
    fn estimate(&self, content: &Content) -> Result<Option<TokenEstimate>, Error> {
        Ok(Some(TokenEstimate {
            route: "fixture:model:default".into(),
            provenance: "fixture_context_budget_bytes".into(),
            input_tokens: ContextMeasurement::for_content(content)?.bytes as u64,
            context_window_tokens: 500_000,
        }))
    }
}

#[derive(Default)]
struct Checkpoints(Mutex<Vec<Recover>>);
#[async_trait]
impl Journal for Checkpoints {
    async fn commit(&self, recover: Recover) -> Result<(), Error> {
        recover.validate()?;
        self.0.lock().unwrap().push(recover);
        Ok(())
    }
}

#[derive(Default)]
struct RewriteHook {
    prepared: AtomicUsize,
}
impl Auditor for RewriteHook {
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
            if moment != Moment::RequestPrepared {
                return Ok(Injection::default());
            }
            self.prepared.fetch_add(1, Ordering::SeqCst);
            let mut body: serde_json::Value =
                serde_json::from_str(context.request_body.expect("authored fixture body")).unwrap();
            body["messages"][1]["content"] = json!(REWRITTEN_TEXT);
            Ok(Injection {
                request_body: Some(body.to_string()),
                ..Default::default()
            })
        })
    }
}

#[derive(Default)]
struct RewritingModel {
    rewrites_received: AtomicUsize,
    helpers: AtomicUsize,
}
#[async_trait]
impl Client for RewritingModel {
    async fn complete(
        &self,
        content: &Content,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        if content.subagent.as_deref() == Some("auto_compact") {
            self.helpers.fetch_add(1, Ordering::SeqCst);
            return Ok(reply(SUMMARY));
        }
        let authored = json!({
            "model": "model",
            "messages": [
                {"role": "system", "content": content.system},
                {"role": "user", "content": "authored fixture prompt"}
            ],
            "tools": [],
            "stream": true,
            "max_tokens": content.max_output_tokens
        })
        .to_string();
        let injection = auditor
            .intervene(
                Moment::RequestPrepared,
                HookContext {
                    epoch: content.epoch,
                    subagent: content.subagent.clone(),
                    turn: content.turn_id,
                    round: content.round,
                    call_id: Some("fixture-physical-call".into()),
                    request_body: Some(&authored),
                    max_request_bytes: Some(content.max_bytes),
                    variables,
                    payload: HookPayload::None,
                },
            )
            .await?;
        let replacement = injection.request_body.expect("rewrite forwarded to client");
        assert!(replacement.contains(REWRITTEN_TEXT));
        self.rewrites_received.fetch_add(1, Ordering::SeqCst);
        Ok(reply("done"))
    }
}
fn reply(text: &str) -> Response {
    Response {
        content: text.into(),
        tool_calls: vec![],
        usage: Usage {
            input_tokens: Some(120_000),
            output_tokens: Some(7),
            samples: 1,
            ..Default::default()
        },
        provider_metadata: None,
    }
}

#[tokio::test]
async fn rewritten_request_usage_does_not_become_context_prefix_evidence() {
    let model = Arc::new(RewritingModel::default());
    let hook = Arc::new(RewriteHook::default());
    let checkpoints = Arc::new(Checkpoints::default());
    let runtime = Runtime::new(vec![], Arc::new(AllowTools::new(Vec::<String>::new()))).unwrap();
    let session = Session::builder_arc(model.clone(), runtime)
        .system("Frozen system")
        .auditor(hook.clone())
        .journal_arc(checkpoints.clone())
        .limits(Limits {
            max_output_tokens: 4096,
            max_context_bytes: 2 * 1024 * 1024,
            ..Default::default()
        })
        .auto_compaction(
            AutoCompactionPolicy {
                enabled: true,
                turn_threshold: 0.8,
                round_threshold: 0.95,
                headroom_tokens: 0,
            },
            Arc::new(LegacyEstimate),
        )
        .build()
        .unwrap();

    // The legacy estimate exceeds 80%, but rewritten usage cannot authorize
    // post-response compaction and must never become retained Context evidence.
    let outcome = session.submit("x".repeat(405_000)).await.unwrap();
    assert_eq!(outcome.usage.input_tokens, Some(120_000));
    assert_eq!(hook.prepared.load(Ordering::SeqCst), 1);
    assert_eq!(model.rewrites_received.load(Ordering::SeqCst), 1);
    assert_eq!(model.helpers.load(Ordering::SeqCst), 0);
    let snapshot = session.context_snapshot().unwrap();
    assert_eq!(snapshot.baseline_generation, 0);
    let encoded = serde_json::to_value(&snapshot).unwrap();
    assert!(encoded.get("usage_evidence").is_none());
    assert!(!encoded.to_string().contains(REWRITTEN_TEXT));
    {
        let rows = checkpoints.0.lock().unwrap();
        let settled = rows
            .iter()
            .rev()
            .find(|row| {
                row.continuation.is_none()
                    && row.context.items.iter().any(|item| {
                        matches!(
                            item,
                            microcrab_session::context::ContentItem::Assistant { .. }
                        )
                    })
            })
            .expect("final settled checkpoint retains the accepted answer");
        assert_eq!(settled.context.baseline_generation, 0);
        let encoded = serde_json::to_value(&settled.context).unwrap();
        assert!(encoded.get("usage_evidence").is_none());
        assert!(!encoded.to_string().contains(REWRITTEN_TEXT));
    }
    session.close().await.unwrap();
}
