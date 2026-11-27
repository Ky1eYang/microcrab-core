//! Delegate work to a creator-provided child-session executor after permission checks.
use super::{Tool, ToolContext};
use crate::Error;
use async_trait::async_trait;
use microcrab_protocol::ToolDefinition;
use serde_json::{Value, json};

/// Own child assembly/execution through cancellation and deadline cleanup. Runtime
/// passes an explicit caller timeout only; omission does not synthesize a child
/// deadline. Implementations must observe cancellation, honor explicit deadlines and
/// acknowledge cleanup before returning a settled timeout as Error::Tool.
#[async_trait]
pub trait DelegateExecutor: Send + Sync {
    /// Session composition installs this weak parent control before publication.
    fn bind_parent(&self, _parent: std::sync::Weak<dyn DelegateParent>) {}
    async fn control(
        &self,
        _action: &str,
        _child_id: &str,
        _target: &str,
        _input: Option<&str>,
        _invocation: ToolContext,
    ) -> Result<super::ToolOutput, Error> {
        Err(Error::tool("delegate child control is unavailable"))
    }

    async fn execute(
        &self,
        target: &str,
        input: &str,
        mode: Option<&str>,
        context_mode: &str,
        timeout_ms: Option<u64>,
        invocation: ToolContext,
    ) -> Result<String, Error>;

    /// Focused production extension. An older embedding executor may serve the
    /// unchanged persistent/no-selection path, but must never silently drop focus.
    #[expect(
        clippy::too_many_arguments,
        reason = "Public focused delegation keeps compatible target, mode, timeout, interaction, references and invocation parameters"
    )]
    async fn execute_focused_output(
        &self,
        target: &str,
        input: &str,
        mode: Option<&str>,
        context_mode: &str,
        timeout_ms: Option<u64>,
        interaction: crate::state::communication::SessionInteraction,
        references: Vec<crate::state::reference::focus::FocusSelection>,
        invocation: ToolContext,
    ) -> Result<super::ToolOutput, Error> {
        if interaction != crate::state::communication::SessionInteraction::Persistent
            || !references.is_empty()
        {
            return Err(Error::tool(
                "delegate executor does not support focused interaction",
            ));
        }
        self.execute_output(target, input, mode, context_mode, timeout_ms, invocation)
            .await
    }
    /// Typed production path. Legacy embedding executors retain their inline
    /// result contract; assembled Creator executors override this with one
    /// Session-owned Buffered execution and acknowledged child cleanup.
    async fn execute_output(
        &self,
        target: &str,
        input: &str,
        mode: Option<&str>,
        context_mode: &str,
        timeout_ms: Option<u64>,
        invocation: ToolContext,
    ) -> Result<super::ToolOutput, Error> {
        self.execute(target, input, mode, context_mode, timeout_ms, invocation)
            .await
            .map(super::ToolOutput::text)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Delegate;
impl Delegate {
    pub fn definition() -> ToolDefinition {
        ToolDefinition {
            name: "delegate".into(),
            description: "Start an eligible sub-agent in an isolated child conversation sharing the authorized workspace without gaining permissions. The assembled executor returns a background execution handle; use job for committed replies and updates. State reports metadata, not reply bodies.".into(),
            parameters: json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["target"],
                "allOf": [{"if":{"properties":{"action":{"enum":["close","resume"]}},"required":["action"]},"then":{},"else":{"required":["input"]}}],
                "properties": {
                    "action": {"type":"string","enum":["start","follow_up","resume","close"],"default":"start","description":"start creates a long-lived child; follow_up submits another child Turn; close waits for acknowledged cleanup. job input never submits conversational Turns."},
                    "child_id": {"type":"string","minLength":1,"description":"Exact child identity owned by this Session; required for follow_up/close."},
                    "target": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Eligible agent or mode name listed in <state><delegatable_agents>. Required, nonempty and not whitespace-only; null is invalid."
                    },
                    "input": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Focused task wording, 1..8192 UTF-8 bytes; mount longer material using references."
                    },
                    "interaction": {"type":"string","enum":["persistent","return"],"default":"persistent"},
                    "references": {"type":"array","maxItems":32,"uniqueItems":true,"items":{"oneOf":[
                        {"type":"object","required":["section_id"],"additionalProperties":false,"properties":{"section_id":{"type":"string","minLength":1}}},
                        {"type":"object","required":["path"],"additionalProperties":false,"properties":{"path":{"type":"string","minLength":1}}}
                    ]}},
                    "mode": {
                        "type": "string",
                        "description": "Child mode. Omission selects the mode matching target, otherwise the first eligible mode in catalog order. Empty/whitespace-only strings also mean omission; null is invalid in the schema."
                    },
                    "context_mode": {
                        "type": "string",
                        "enum": ["clean"],
                        "default": "clean",
                        "description": "Initialize from input without copying the caller conversation. Omission defaults to 'clean', the only supported value; null is invalid."
                    },
                    "timeout_ms": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Positive child deadline in milliseconds (unsigned 64-bit). Omission sets no deadline; null is invalid. The child executor handles deadline and cancellation cleanup before returning a settled timeout."
                    }
                }
            }),
        }
    }
}

pub(crate) async fn execute(
    executor: Option<&dyn DelegateExecutor>,
    arguments: &Value,
    invocation: ToolContext,
) -> Result<super::ToolOutput, Error> {
    invocation
        .ensure_admitted(None)
        .and_then(|()| invocation.ensure_admitted_arguments(arguments))
        .map_err(|e| e.into_runtime_error())?;
    invocation
        .begin_dispatch(None)
        .map_err(|e| e.into_runtime_error())?;
    execute_inner(executor, arguments, invocation).await
}

pub(crate) async fn execute_admitted(
    executor: Option<&dyn DelegateExecutor>,
    arguments: &Value,
    invocation: ToolContext,
) -> Result<super::ToolOutput, Error> {
    execute_inner(executor, arguments, invocation).await
}

async fn execute_inner(
    executor: Option<&dyn DelegateExecutor>,
    arguments: &Value,
    invocation: ToolContext,
) -> Result<super::ToolOutput, Error> {
    let target = arguments
        .get("target")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| Error::tool("delegate requires target"))?;
    let snapshot = invocation.state.snapshot();
    if matches!(
        snapshot.session_kind,
        Some(crate::SessionKind::Delegated | crate::SessionKind::OneShot)
    ) || snapshot
        .communication
        .as_ref()
        .is_some_and(|communication| communication.identity.depth != 0)
    {
        return Err(Error::denied(
            "only a root conversational Session may delegate",
        ));
    }
    let action = arguments
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or("start");
    if action != "start" {
        let child_id = arguments
            .get("child_id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| Error::tool("delegate control requires child_id"))?;
        return executor
            .ok_or_else(|| Error::tool("no child-session executor was supplied"))?
            .control(
                action,
                child_id,
                target,
                arguments.get("input").and_then(Value::as_str),
                invocation,
            )
            .await;
    }
    let input = arguments
        .get("input")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| Error::tool("delegate requires input"))?;
    let mode = arguments
        .get("mode")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty());
    let context_mode = arguments
        .get("context_mode")
        .and_then(Value::as_str)
        .unwrap_or("clean");
    if context_mode != "clean" {
        return Err(Error::tool(
            "unsupported context_mode; currently only 'clean' is supported",
        ));
    }
    crate::budget::check(crate::LimitKind::InputBytes, input.len(), 8192)?;
    let interaction = arguments
        .get("interaction")
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()?
        .unwrap_or(crate::state::communication::SessionInteraction::Persistent);
    let references = arguments
        .get("references")
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()?
        .unwrap_or_default();
    let timeout_ms = arguments.get("timeout_ms").and_then(Value::as_u64);
    let executor = executor.ok_or_else(|| Error::tool("no child-session executor was supplied"))?;
    executor
        .execute_focused_output(
            target,
            input,
            mode,
            context_mode,
            timeout_ms,
            interaction,
            references,
            invocation,
        )
        .await
}
#[async_trait]
impl Tool for Delegate {
    fn definition(&self) -> ToolDefinition {
        Self::definition()
    }
    async fn execute(
        &self,
        _arguments: Value,
        _invocation: ToolContext,
    ) -> Result<super::ToolOutput, Error> {
        Err(Error::tool(
            "delegate requires a configured Runtime child-session executor",
        ))
    }
}

/// One resolver owns both discovery and execution, including the default mode.
pub fn eligible_modes(
    agent: &microcrab_agent_types::AgentInfo,
    _caller_mode: Option<&str>,
) -> Vec<String> {
    agent
        .modes
        .iter()
        .filter(|mode| {
            matches!(
                mode.role,
                microcrab_agent_types::AgentRole::SubAgent | microcrab_agent_types::AgentRole::Both
            )
        })
        .map(|mode| mode.name.clone())
        .collect()
}
pub fn select_child(
    catalog: &[microcrab_agent_types::AgentInfo],
    caller_mode: Option<&str>,
    target: &str,
    requested_mode: Option<&str>,
) -> Result<(microcrab_agent_types::AgentInfo, String), Error> {
    let canonical = catalog.iter().any(|agent| agent.name == target);
    let mut selected_child = None;
    for agent in catalog {
        if canonical && agent.name != target {
            continue;
        }
        let modes = eligible_modes(agent, caller_mode);
        if agent.name != target && !modes.iter().any(|mode| mode == target) {
            continue;
        }
        let selected = match requested_mode {
            Some(mode) => modes.iter().find(|candidate| candidate.as_str() == mode),
            None => modes
                .iter()
                .find(|mode| mode.as_str() == target)
                .or_else(|| modes.first()),
        };
        if let Some(mode) = selected {
            if selected_child.is_some() {
                return Err(Error::denied(
                    "delegate target/mode is ambiguous; use a unique canonical agent name and explicit mode",
                ));
            }
            selected_child = Some((agent.clone(), mode.clone()));
        }
    }
    selected_child.ok_or_else(|| Error::denied("delegate target/mode is not an eligible sub-agent"))
}

/// Prepared dependency validation and conversational controls owned by Session.
#[async_trait]
pub trait DelegateParent: Send + Sync {
    async fn capture_focus(
        &self,
        selections: Vec<crate::state::reference::focus::FocusSelection>,
        invocation: &ToolContext,
    ) -> Result<Vec<crate::state::reference::focus::FocusSnapshot>, Error>;
    fn adopt_child(
        &self,
        target: &str,
        mode: Option<&str>,
        issuer_epoch: u64,
        child: &crate::Session,
        invocation: &ToolContext,
    ) -> Result<(), Error>;
    fn child_settled(
        &self,
        child_id: &str,
        target: &str,
        result: Result<(), crate::observation::diagnostics::ToolDiagnostic>,
    );
    async fn submit_child(
        &self,
        child_id: &str,
        target: &str,
        input: String,
    ) -> Result<crate::runtime::TurnOutcome, Error>;
    async fn resume_child(
        &self,
        child_id: &str,
        target: &str,
    ) -> Result<crate::runtime::TurnOutcome, Error>;
    async fn close_child(&self, child_id: &str, target: &str) -> Result<(), Error>;
}
