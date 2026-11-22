use super::*;
use crate::runtime::tool::DenyAll;
use microcrab_agent_types::AgentInfo;
use serde_json::json;
struct NoCalls;
#[async_trait::async_trait]
impl crate::Client for NoCalls {
    async fn complete(
        &self,
        _: &crate::context::Content,
        _: &dyn microcrab_auditor::Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<microcrab_protocol::Response, microcrab_transport::Error> {
        Err(microcrab_transport::Error::protocol(
            "unexpected model call in identity authority test",
        ))
    }
}
fn builder() -> crate::SessionBuilder {
    crate::Session::builder(NoCalls, Runtime::new(vec![], Arc::new(DenyAll)).unwrap())
        .system("fixed")
}
fn modes() -> AgentInfo {
    serde_json::from_value(json!({"name":"agent","tools":[],"variables":{},"modes":[
        {"name":"main","role":"main_agent"}, {"name":"both","role":"both"}, {"name":"sub","role":"sub_agent"}
    ]})).unwrap()
}
#[test]
fn current_mode_targeted_child_and_return_gates_are_independent_of_inspection() {
    let root = builder().agent(modes()).build().unwrap();
    assert!(root.inner.require_user_input(false).is_ok());
    assert!(root.inner.select_mode_now("sub".into(), None).is_err());
    let child = builder()
        .agent(modes())
        .kind(crate::SessionKind::Delegated)
        .build()
        .unwrap();
    root.inner.bind_child_communication(&child).unwrap();
    assert!(child.inner.require_derivation().is_err());
    assert!(child.inner.require_user_input(false).is_err());
    assert!(child.inner.require_user_input(true).is_ok());
    *child.inner.parent_relationship.lock().unwrap() = Some(crate::DelegationRelationship {
        parent_session_id: root.process_control().session_id().into(),
        parent_execution_id: crate::runtime::execution::ExecutionId(1),
        issuer_epoch: 0,
        target: "agent".into(),
        mode: "both".into(),
    });
    root.inner.delegated_children.lock().unwrap().insert(
        child.process_control().session_id().into(),
        crate::inner::delegation::ChildRecord {
            target: "agent".into(),
            child: child.clone(),
        },
    );
    let _inspection = root
        .inspect_child(child.process_control().session_id(), "agent")
        .unwrap();
    assert!(child.inner.require_user_input(false).is_err());
    assert!(
        root.child_accepts_user_input(child.process_control().session_id())
            .unwrap()
    );
    child.inner.select_mode_now("sub".into(), None).unwrap();
    assert!(
        !root
            .child_accepts_user_input(child.process_control().session_id())
            .unwrap()
    );
    let before = child.inner.context_snapshot().unwrap();
    assert!(
        child
            .inner
            .require_user_input_snapshot(&child.state(), true)
            .is_err()
    );
    assert_eq!(
        serde_json::to_vec(&child.inner.context_snapshot().unwrap()).unwrap(),
        serde_json::to_vec(&before).unwrap()
    );
    let other = builder().build().unwrap();
    assert!(
        other
            .child_accepts_user_input(child.process_control().session_id())
            .is_err()
    );
    let returning = builder()
        .agent(modes())
        .kind(crate::SessionKind::Delegated)
        .interaction(SessionInteraction::Return)
        .build()
        .unwrap();
    root.inner.bind_child_communication(&returning).unwrap();
    assert!(returning.inner.require_user_input(true).is_err());
}

#[tokio::test]
async fn recovery_keeps_exact_identity_legacy_stays_absent_and_fork_is_fresh() {
    let source = builder().build().unwrap();
    let identity = source.communication_identity().unwrap();
    let recover = source.checkpoint().await.unwrap();
    let restored = crate::Session::restore(builder(), recover.clone())
        .await
        .unwrap();
    assert_eq!(restored.communication_identity().unwrap(), identity);
    let frozen = serde_json::to_vec(&recover.context).unwrap();
    let mut legacy = recover;
    legacy.state.communication = None;
    legacy
        .context
        .tools
        .retain(|tool| !matches!(tool.name.as_str(), "send_message" | "escalate"));
    let historical = serde_json::to_vec(&legacy.context).unwrap();
    let legacy = crate::Session::restore(builder(), legacy).await.unwrap();
    assert!(legacy.communication_identity().is_none());
    assert_eq!(
        serde_json::to_vec(&legacy.inner.context_snapshot().unwrap()).unwrap(),
        historical
    );
    let fork = source.fork(builder()).await.unwrap();
    let fork_identity = fork.communication_identity().unwrap();
    assert_ne!(fork_identity.agent_id, identity.agent_id);
    assert_eq!(fork_identity.depth, 0);
    assert!(fork_identity.parent_session_id.is_none());
    assert!(
        fork.state()
            .communication
            .unwrap()
            .correspondents
            .is_empty()
    );
    assert_eq!(
        serde_json::to_vec(&fork.inner.context_snapshot().unwrap()).unwrap(),
        frozen
    );
}

mod inbox_tests;

mod exchange_tests;

mod consumption_tests;

mod reattach_tests;
mod reply_tests;
