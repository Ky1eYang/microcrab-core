use super::*;
use crate::runtime::tool::AllowTools;
use crate::runtime::{BuiltinTool, ToolRegistration};
fn parent_builder() -> crate::SessionBuilder {
    crate::Session::builder(
        NoCalls,
        Runtime::new(
            vec![ToolRegistration::Builtin(BuiltinTool::Delegate)],
            Arc::new(AllowTools::new(["delegate"])),
        )
        .unwrap(),
    )
    .catalog(vec![modes()])
}
#[tokio::test]
async fn explicit_reattach_checks_persisted_owner_and_does_not_restore_jobs_or_exchanges() {
    let root = parent_builder().build().unwrap();
    let child = builder()
        .session_id("recorded-child")
        .agent(modes())
        .kind(crate::SessionKind::Delegated)
        .processes_parent(root.process_control())
        .build()
        .unwrap();
    root.inner.bind_child_communication(&child).unwrap();
    let relationship = crate::DelegationRelationship {
        parent_session_id: root.process_control().session_id().into(),
        parent_execution_id: crate::runtime::execution::ExecutionId(77),
        issuer_epoch: 0,
        target: "agent".into(),
        mode: "both".into(),
    };
    *child.inner.parent_relationship.lock().unwrap() = Some(relationship.clone());
    child
        .inner
        .state
        .update(|state| state.parent_relationship = Some(relationship));
    root.start().await.unwrap();
    child.start().await.unwrap();
    let parent_recover = root.checkpoint().await.unwrap();
    let child_recover = child.checkpoint().await.unwrap();
    drop(child);
    let root_id = root.process_control().session_id().to_owned();
    let root = crate::Session::restore(parent_builder().session_id(root_id), parent_recover)
        .await
        .unwrap();
    let child = crate::Session::restore(
        builder()
            .session_id("recorded-child")
            .agent(modes())
            .processes_parent(root.process_control()),
        child_recover.clone(),
    )
    .await
    .unwrap();
    assert!(
        !root
            .child_accepts_user_input("recorded-child")
            .unwrap_or(false)
    );
    assert!(!child.accepts_user_input());
    let foreign = parent_builder().build().unwrap();
    assert!(foreign.attach_recovered_child(&child).await.is_err());
    let jobs = root.execution_snapshots();
    let frozen = serde_json::to_vec(&child.inner.context_snapshot().unwrap()).unwrap();
    root.attach_recovered_child(&child).await.unwrap();
    assert!(root.child_accepts_user_input("recorded-child").unwrap());
    assert_eq!(
        serde_json::to_value(root.execution_snapshots()).unwrap(),
        serde_json::to_value(jobs).unwrap(),
        "no original delegate producer reconstructed"
    );
    assert!(
        root.inner
            .communication_directory()
            .exchanges
            .lock()
            .await
            .exchanges
            .is_empty()
    );
    assert_eq!(
        serde_json::to_vec(&child.inner.context_snapshot().unwrap()).unwrap(),
        frozen
    );
    assert!(
        root.attach_recovered_child(&child).await.is_err(),
        "active binding cannot be overwritten"
    );
    child.inner.select_mode_now("sub".into(), None).unwrap();
    assert_eq!(
        child.current_agent_role().unwrap(),
        microcrab_agent_types::AgentRole::SubAgent
    );
    assert!(!root.child_accepts_user_input("recorded-child").unwrap());
    child.inner.select_mode_now("both".into(), None).unwrap();
    assert_eq!(
        child.current_agent_role().unwrap(),
        microcrab_agent_types::AgentRole::Both
    );
    assert!(root.child_accepts_user_input("recorded-child").unwrap());
}
#[tokio::test]
async fn reattach_rejects_current_process_owner_and_limit_expansion() {
    let root = parent_builder().build().unwrap();
    let child = builder()
        .agent(modes())
        .kind(crate::SessionKind::Delegated)
        .build()
        .unwrap();
    root.inner.bind_child_communication(&child).unwrap();
    child.inner.restored.store(true, Ordering::Release);
    let relationship = crate::DelegationRelationship {
        parent_session_id: root.process_control().session_id().into(),
        parent_execution_id: crate::runtime::execution::ExecutionId(77),
        issuer_epoch: 0,
        target: "agent".into(),
        mode: "both".into(),
    };
    *child.inner.parent_relationship.lock().unwrap() = Some(relationship);
    assert!(
        root.attach_recovered_child(&child).await.is_err(),
        "a different process owner cannot be acquired"
    );
    assert!(
        !root.accepts_user_input()
            || root.current_agent_role().unwrap() == microcrab_agent_types::AgentRole::Both
    );
    let returning = builder()
        .agent(modes())
        .kind(crate::SessionKind::Delegated)
        .interaction(SessionInteraction::Return)
        .processes_parent(root.process_control())
        .build()
        .unwrap();
    root.inner.bind_child_communication(&returning).unwrap();
    returning.inner.restored.store(true, Ordering::Release);
    assert!(!returning.accepts_user_input());
    assert!(root.attach_recovered_child(&returning).await.is_err());
}

#[tokio::test]
async fn reattachment_applies_default_and_explicit_image_ceilings_before_binding() {
    for (parent_bytes, child_bytes, allowed) in [
        (256 * 1024, 1024 * 1024, false),
        (1024 * 1024, 512 * 1024, true),
    ] {
        let root = parent_builder()
            .limits(crate::Limits {
                max_tool_image_bytes: parent_bytes,
                ..Default::default()
            })
            .build()
            .unwrap();
        let child = builder()
            .agent(modes())
            .kind(crate::SessionKind::Delegated)
            .processes_parent(root.process_control())
            .limits(crate::Limits {
                max_tool_image_bytes: child_bytes,
                ..Default::default()
            })
            .build()
            .unwrap();
        root.inner.bind_child_communication(&child).unwrap();
        let relationship = crate::DelegationRelationship {
            parent_session_id: root.process_control().session_id().into(),
            parent_execution_id: crate::runtime::execution::ExecutionId(77),
            issuer_epoch: 0,
            target: "agent".into(),
            mode: "both".into(),
        };
        *child.inner.parent_relationship.lock().unwrap() = Some(relationship);
        child.inner.restored.store(true, Ordering::Release);
        let attached = root.attach_recovered_child(&child).await;
        assert_eq!(attached.is_ok(), allowed);
        assert_eq!(
            root.inner
                .delegated_children
                .lock()
                .unwrap()
                .contains_key(child.process_control().session_id()),
            allowed
        );
    }
}
