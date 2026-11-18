use super::*;
use crate::policy::{
    Action, AskReply, Decision, PermissionError, PermissionFeatures, PermissionRequest,
    PolicySnapshot, SessionPermission,
};
use crate::runtime::tool::{ToolContext, ToolOutput};
use crate::state::communication::CorrespondentRelation;
use std::sync::atomic::{AtomicUsize, Ordering};
#[derive(Default)]
struct Approver(AtomicUsize);
#[async_trait::async_trait]
impl SessionPermission for Approver {
    async fn evaluate(
        &self,
        policy: &PolicySnapshot,
        request: &PermissionRequest,
    ) -> Result<Decision, PermissionError> {
        crate::policy::evaluate(policy, request)
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
            reason: "explicit fixture approval".into(),
        })
    }
}
fn endpoint(approver: Arc<Approver>, derived: bool) -> crate::Session {
    let runtime = Runtime::new(vec![], approver).unwrap();
    let builder = crate::Session::builder(NoCalls, runtime).system("fixed");
    let session = if derived {
        builder.agent(modes()).kind(crate::SessionKind::Delegated)
    } else {
        builder
    }
    .build()
    .unwrap();
    session.inner.inbox.state.lock().unwrap().paused = true;
    session
}
async fn call(
    session: &crate::Session,
    call_id: &str,
    tool: &str,
    args: serde_json::Value,
) -> Result<crate::state::communication::inbox::QueuedReceipt, Error> {
    let mut context = ToolContext::new(
        session.state_handle(),
        session.process_control(),
        CancellationToken::new(),
        call_id.into(),
        tool.into(),
        session.process_control().session_id().into(),
        session
            .inner
            .agent
            .as_ref()
            .map(|agent| agent.name.clone())
            .unwrap_or_default(),
        session.state().selected_mode,
    );
    let row = crate::runtime::execution::ExecutionSnapshot::new(
        context.session_id.clone(),
        context.call_id.clone(),
        context.tool.clone(),
        context.agent.clone(),
        context.mode.clone(),
    );
    context.execution_id = Some(
        context
            .state
            .executions()
            .admit_detached(row, context.cancellation.clone())?,
    );
    let issuer = session.inner.context.lock().unwrap().content(32)?;
    session
        .inner
        .runtime
        .admit_invocation(
            &microcrab_protocol::ToolCall {
                id: call_id.into(),
                name: tool.into(),
                arguments: args.clone(),
                provider_metadata: None,
            },
            &mut context,
            &issuer,
        )
        .await
        .map_err(|error| error.into_runtime_error())?;
    context
        .begin_dispatch(None)
        .map_err(|error| error.into_runtime_error())?;
    let result = if tool == "escalate" {
        session.inner.escalate_owned(args, context.clone()).await
    } else {
        session
            .inner
            .send_message_owned(args, context.clone())
            .await
    };
    let execution_id = context.execution_id.expect("test reserved execution");
    match &result {
        Ok(receipt) => {
            let ToolOutput::Final(output) = ToolOutput::text(serde_json::to_string(receipt)?)
            else {
                unreachable!()
            };
            context
                .state
                .executions()
                .complete_detached(execution_id, output)
                .await?;
        }
        Err(error) => {
            context
                .state
                .executions()
                .finish_error(execution_id, error)
                .await
        }
    }
    result
}
#[tokio::test]
async fn directed_exchanges_reply_once_reverse_pair_independent_and_caller_asks() {
    let root_asks = Arc::new(Approver::default());
    let child_asks = Arc::new(Approver::default());
    let root = endpoint(root_asks.clone(), false);
    let child = endpoint(child_asks.clone(), true);
    root.inner.bind_child_communication(&child).unwrap();
    let root_id = root.communication_identity().unwrap().agent_id;
    let child_id = child.communication_identity().unwrap().agent_id;
    call(
        &root,
        "send-one",
        "send_message",
        json!({"agent_id":child_id,"intent":"send","body":"first"}),
    )
    .await
    .unwrap();
    assert_eq!(root_asks.0.load(Ordering::SeqCst), 1);
    assert_eq!(child_asks.0.load(Ordering::SeqCst), 0);
    assert!(
        call(
            &root,
            "duplicate-pair",
            "send_message",
            json!({"agent_id":child_id,"intent":"send","body":"denied"})
        )
        .await
        .is_err()
    );
    call(
        &child,
        "reverse-independent",
        "send_message",
        json!({"agent_id":root_id,"intent":"send","body":"reverse"}),
    )
    .await
    .unwrap();
    assert_eq!(child_asks.0.load(Ordering::SeqCst), 1);
    let exchange = child.state().communication.unwrap().inbox.pending[0]
        .source
        .clone();
    let microcrab_transport::UserSource::Agent {
        exchange: Some(exchange),
        ..
    } = exchange
    else {
        panic!("trusted exchange witness")
    };
    call(&child,"reply-once","send_message",json!({"agent_id":root_id,"intent":"reply","exchange_id":exchange.exchange_id,"body":"answer"})).await.unwrap();
    assert_eq!(
        child_asks.0.load(Ordering::SeqCst),
        1,
        "reply needs no new proactive approval"
    );
    assert!(call(&child,"reply-again","send_message",json!({"agent_id":root_id,"intent":"reply","exchange_id":exchange.exchange_id,"body":"denied"})).await.is_err());
    assert!(
        call(
            &root,
            "self-send",
            "send_message",
            json!({"agent_id":root_id,"intent":"send","body":"denied"})
        )
        .await
        .is_err()
    );
    let source = &root.state().communication.unwrap().inbox.pending[1];
    assert_eq!(
        source.correspondent.as_ref().unwrap().relation,
        CorrespondentRelation::Child
    );
    call(&child, "escalation", "escalate", json!({"body":"one way"}))
        .await
        .unwrap();
    assert_eq!(child_asks.0.load(Ordering::SeqCst), 2);
    assert!(matches!(
        &root
            .state()
            .communication
            .unwrap()
            .inbox
            .pending
            .last()
            .unwrap()
            .source,
        microcrab_transport::UserSource::Agent { exchange: None, .. }
    ));
}
