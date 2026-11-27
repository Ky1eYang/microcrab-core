//! Reserved mode command; composition owns validation and the batch boundary.
use super::{Tool, ToolContext, ToolOutput};
use crate::Error;
use async_trait::async_trait;
use microcrab_protocol::ToolDefinition;
use serde_json::{Value, json};

/// Runtime receives this narrow command port, never an inner Session dependency.
pub(crate) trait SessionControl: Send + Sync {
    fn request_mode(&self, mode: &str, invocation: &ToolContext) -> Result<Value, Error>;
    fn apply_pending_mode(&self) -> Result<(), Error>;
    fn discard_pending_mode(&self);
}

pub struct SwitchMode;
impl SwitchMode {
    pub fn definition() -> ToolDefinition {
        ToolDefinition {
            name: "switch_mode".into(),
            description: "Request another mode of the current Agent. Permission defaults to Ask. The current tool batch settles under its original mode; the selected mode and its instructions apply together before the next model request. A pending receipt does not grant the target mode to this batch.".into(),
            parameters: json!({"type":"object","additionalProperties":false,"required":["mode"],"properties":{"mode":{"type":"string","minLength":1}}}),
        }
    }
}
#[async_trait]
impl Tool for SwitchMode {
    fn definition(&self) -> ToolDefinition {
        Self::definition()
    }
    async fn execute(&self, _: Value, _: ToolContext) -> Result<ToolOutput, Error> {
        Err(Error::tool(
            "switch_mode requires its owning Session command port",
        ))
    }
}
pub(crate) fn execute(
    control: Option<&dyn SessionControl>,
    arguments: &Value,
    invocation: ToolContext,
) -> Result<ToolOutput, Error> {
    invocation
        .ensure_admitted(None)
        .and_then(|()| invocation.ensure_admitted_arguments(arguments))
        .map_err(|e| e.into_runtime_error())?;
    invocation
        .begin_dispatch(None)
        .map_err(|e| e.into_runtime_error())?;
    execute_inner(control, arguments, &invocation)
}

pub(crate) fn execute_admitted(
    control: Option<&dyn SessionControl>,
    arguments: &Value,
    invocation: &ToolContext,
) -> Result<ToolOutput, Error> {
    execute_inner(control, arguments, invocation)
}

fn execute_inner(
    control: Option<&dyn SessionControl>,
    arguments: &Value,
    invocation: &ToolContext,
) -> Result<ToolOutput, Error> {
    let mode = arguments
        .get("mode")
        .and_then(Value::as_str)
        .filter(|name| !name.trim().is_empty())
        .ok_or_else(|| Error::tool("switch_mode requires a nonempty mode"))?;
    let receipt = control
        .ok_or_else(|| Error::tool("Session mode control is unavailable"))?
        .request_mode(mode, invocation)?;
    Ok(ToolOutput::text(receipt.to_string()))
}
