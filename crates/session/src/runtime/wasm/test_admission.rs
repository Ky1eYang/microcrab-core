//! Test fixtures pass through the real controller; no opaque proof constructor exists.
use super::*;
use crate::policy::{
    Action, Matcher, Permission, PermissionConfig, PermissionController, PermissionRequest,
};
use futures_util::FutureExt;

pub(crate) fn admit(context: &mut ToolContext, component: Option<String>) {
    admit_arguments(context, component, serde_json::json!({}));
}
pub(crate) fn admit_arguments(
    context: &mut ToolContext,
    component: Option<String>,
    arguments: serde_json::Value,
) {
    // Each fixture grant is a new invocation, including when a previous proof was cleared.
    context.admission = None;
    context.execution_id = None;
    context
        .ensure_detached_execution()
        .expect("fixture reserves its execution owner before admission");
    let controller = Arc::new(PermissionController::new(Arc::new(
        crate::runtime::tool::DenyAll,
    )));
    controller
        .replace(
            1,
            PermissionConfig {
                agent: context.agent.clone(),
                mode: context.mode.clone().unwrap_or_default(),
                initialization: Action::Allow,
                review: false,
                overrides: vec![Permission {
                    agent: None,
                    mode: None,
                    tool: Some(Matcher::Exact(context.tool.clone())),
                    args: vec![],
                    action: Action::Allow,
                }],
                ..Default::default()
            },
        )
        .unwrap();
    let mut request = request(context);
    request.arguments = arguments;
    context.admission = Some(
        controller
            .admit(&request, context, &issuer(), component)
            .now_or_never()
            .expect("unconditional Allow admission never waits")
            .unwrap(),
    );
}
pub(super) fn request(context: &ToolContext) -> PermissionRequest {
    PermissionRequest {
        subject: context.subject.clone(),
        session_id: context.session_id.clone(),
        call_id: context.call_id.clone(),
        agent: context.agent.clone(),
        mode: context.mode.clone().unwrap_or_default(),
        tool: context.tool.clone(),
        arguments: serde_json::json!({}),
        revision: 1,
    }
}
pub(super) fn issuer() -> crate::context::Content {
    crate::context::Content {
        epoch: 0,
        subagent: None,
        system: "Admission fixture".into(),
        items: vec![],
        tools: vec![],
        max_output_tokens: 1024,
        max_bytes: 65536,
        turn_id: None,
        round: None,
    }
}
