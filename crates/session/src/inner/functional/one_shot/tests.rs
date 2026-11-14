use super::*;
use crate::{HookContext, Injection, Moment, VariablesChange};
use microcrab_protocol::{Response, Usage};
use serde_json::json;

struct ChildHook;
impl Auditor for ChildHook {
    fn is_enabled(&self) -> bool {
        false
    }
    fn publish(&self, _: Moment, _: microcrab_auditor::AuditBody) {}
    fn intervene<'a>(
        &'a self,
        moment: Moment,
        _: HookContext<'a>,
    ) -> microcrab_auditor::BoxFuture<'a, Result<Injection, microcrab_auditor::AuditorError>> {
        Box::pin(async move {
            Ok(if moment == Moment::RequestPrepared {
                Injection {
                    variables: VariablesChange {
                        scoped_set: vec![],
                        set: json!({"helper_write":"child"}).as_object().unwrap().clone(),
                        remove: vec![],
                    },
                    ..Default::default()
                }
            } else {
                Injection::default()
            })
        })
    }
}
struct InspectRequest {
    expected: crate::context::Content,
}
#[async_trait::async_trait]
impl Client for InspectRequest {
    async fn complete(
        &self,
        content: &crate::context::Content,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        assert_eq!(
            serde_json::to_value(content).unwrap(),
            serde_json::to_value(&self.expected).unwrap(),
            "functional Source/ContextMode materialization and route epoch remain exact"
        );
        assert_eq!(variables.get("issuer_value"), Some(json!(7)));
        auditor
            .intervene(
                Moment::RequestPrepared,
                HookContext {
                    epoch: content.epoch,
                    subagent: content.subagent.clone(),
                    turn: None,
                    round: None,
                    call_id: None,
                    request_body: None,
                    max_request_bytes: Some(content.max_bytes),
                    variables,
                    payload: crate::HookPayload::None,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            variables.get("helper_write"),
            Some(json!("child")),
            "hook commits belong to the real one-shot State"
        );
        Ok(Response {
            provider_metadata: None,
            content: "functional response".into(),
            tool_calls: vec![],
            usage: Usage::default(),
        })
    }
}
#[tokio::test]
async fn functional_request_owns_context_and_hook_state_without_mutating_the_parent() {
    let parent_session = crate::Session::builder_arc(
        Arc::new(CancelledOperation {
            entered: Arc::new(tokio::sync::Notify::new()),
            cancelled: Arc::new(tokio::sync::Notify::new()),
        }),
        Runtime::new(vec![], Arc::new(crate::runtime::tool::DenyAll)).unwrap(),
    )
    .build()
    .unwrap();
    let parent = parent_session.state_handle();
    parent.inject("issuer_value", json!(7)).await.unwrap();
    let issuer = crate::context::Content {
        epoch: 21,
        subagent: None,
        system: "frozen issuer".into(),
        items: vec![],
        tools: vec![],
        max_output_tokens: 1024,
        max_bytes: 1024 * 1024,
        turn_id: Some(9),
        round: Some(3),
    };
    for (name, source) in [
        ("rename", crate::runtime::subagents::Source::System),
        ("compact", crate::runtime::subagents::Source::User),
        ("action_review", crate::runtime::subagents::Source::System),
    ] {
        let request = crate::runtime::subagents::prepare(
            name,
            source,
            &issuer,
            &[
                ("user_input", "task"),
                ("tool", "echo"),
                ("tool_name", "echo"),
                ("tool_args", "{}"),
            ],
        )
        .unwrap();
        let result = crate::Session::one_shot_executor()
            .execute(
                Arc::new(InspectRequest {
                    expected: request.clone(),
                }),
                Arc::new(ChildHook),
                Arc::new(parent.clone()),
                &request,
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(result.content, "functional response");
        assert_eq!(
            parent.get("helper_write"),
            None,
            "one-shot hooks cannot mutate parent variables"
        );
    }
    parent_session.close().await.unwrap();
}

struct CancelledOperation {
    entered: Arc<tokio::sync::Notify>,
    cancelled: Arc<tokio::sync::Notify>,
}
#[async_trait::async_trait]
impl Client for CancelledOperation {
    async fn complete(
        &self,
        _: &crate::context::Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        token: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.entered.notify_one();
        token.cancelled().await;
        self.cancelled.notify_one();
        Err(microcrab_transport::Error::Cancelled)
    }
}
struct ClosedAudit(Arc<tokio::sync::Notify>);
impl Auditor for ClosedAudit {
    fn is_enabled(&self) -> bool {
        false
    }
    fn publish(&self, _: Moment, _: microcrab_auditor::AuditBody) {}
    fn flush(
        &self,
    ) -> microcrab_auditor::BoxFuture<'_, Result<(), microcrab_auditor::AuditorError>> {
        Box::pin(async move {
            self.0.notify_one();
            Ok(())
        })
    }
}
#[tokio::test]
async fn dropping_the_functional_waiter_cancels_and_retains_the_child_until_close_ack() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let entered = Arc::new(tokio::sync::Notify::new());
        let cancelled = Arc::new(tokio::sync::Notify::new());
        let closed = Arc::new(tokio::sync::Notify::new());
        let request = crate::context::Content {
            epoch: 3,
            subagent: Some("rename".into()),
            system: "one-shot".into(),
            items: vec![],
            tools: vec![],
            max_output_tokens: 1024,
            max_bytes: 1024 * 1024,
            turn_id: None,
            round: None,
        };
        let client = Arc::new(CancelledOperation {
            entered: entered.clone(),
            cancelled: cancelled.clone(),
        });
        let audit = Arc::new(ClosedAudit(closed.clone()));
        let waiter = tokio::spawn(async move {
            crate::Session::one_shot_executor()
                .execute(
                    client,
                    audit,
                    Arc::new(State::new(Limits::default())),
                    &request,
                    &CancellationToken::new(),
                )
                .await
        });
        entered.notified().await;
        waiter.abort();
        let _ = waiter.await;
        // The model future may be dropped when cancellation wins, but the owned
        // Session must still deliver its close/flush acknowledgement.
        closed.notified().await;
    })
    .await
    .unwrap();
}

struct TerminationGate {
    first: std::sync::atomic::AtomicBool,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
}
impl Auditor for TerminationGate {
    fn is_enabled(&self) -> bool {
        false
    }
    fn publish(&self, _: Moment, _: microcrab_auditor::AuditBody) {}
    fn intervene<'a>(
        &'a self,
        moment: Moment,
        _: HookContext<'a>,
    ) -> microcrab_auditor::BoxFuture<'a, Result<Injection, microcrab_auditor::AuditorError>> {
        Box::pin(async move {
            if moment == Moment::SessionTerminated && !self.first.swap(true, Ordering::AcqRel) {
                self.entered.notify_one();
                self.release
                    .acquire()
                    .await
                    .map_err(|_| microcrab_auditor::AuditorError("closed test gate".into()))?
                    .forget();
            }
            Ok(Injection::default())
        })
    }
}
#[tokio::test]
async fn parent_close_waits_for_functional_child_termination_before_audit_release() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let model_entered = Arc::new(tokio::sync::Notify::new());
        let close_entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let runtime = Runtime::new(vec![], Arc::new(crate::runtime::tool::DenyAll)).unwrap();
        let session = crate::Session::builder(
            CancelledOperation {
                entered: model_entered.clone(),
                cancelled: Arc::new(tokio::sync::Notify::new()),
            },
            runtime,
        )
        .auditor(TerminationGate {
            first: std::sync::atomic::AtomicBool::new(false),
            entered: close_entered.clone(),
            release: release.clone(),
        })
        .build()
        .unwrap();
        let renaming = session.clone();
        let rename = tokio::spawn(async move { renaming.rename_with_model("title").await });
        model_entered.notified().await;
        let closing = session.clone();
        let close = tokio::spawn(async move { closing.close().await });
        close_entered.notified().await;
        assert!(
            !close.is_finished(),
            "parent must retain audit ownership until the child's termination hook acknowledges"
        );
        release.add_permits(1);
        close.await.unwrap().unwrap();
        assert!(rename.await.unwrap().is_err());
    })
    .await
    .unwrap();
}
