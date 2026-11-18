//! Messaging builtin schemas. Session owns delivery, atomic exchanges and durability.
use super::{Tool, ToolContext, ToolOutput};
use crate::Error;
use microcrab_protocol::ToolDefinition;
use serde_json::{Value, json};
#[derive(Clone, Copy)]
pub struct SendMessage;
#[derive(Clone, Copy)]
pub struct Escalate;
#[async_trait::async_trait]
impl Tool for SendMessage {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition{name:"send_message".into(),description:"Send to an exact persistent parent or direct child. send requires approval and opens one directed exchange; reply closes that accepted exchange once.".into(),parameters:json!({"type":"object","additionalProperties":false,"required":["agent_id","intent","body"],"properties":{"agent_id":{"type":"string","pattern":"^agt_[a-f0-9]{16}$"},"intent":{"type":"string","enum":["send","reply"]},"exchange_id":{"type":"string","minLength":1,"maxLength":128},"body":{"type":"string","minLength":1,"maxLength":8192}}})}
    }
    async fn execute(&self, arguments: Value, context: ToolContext) -> Result<ToolOutput, Error> {
        context
            .ensure_admitted(None)
            .and_then(|()| context.ensure_admitted_arguments(&arguments))
            .map_err(|error| error.into_runtime_error())?;
        let receipt = context
            .state
            .messaging_peer()?
            .send_message(arguments, context.clone())
            .await?;
        Ok(ToolOutput::text(serde_json::to_string(&receipt)?))
    }
}
#[async_trait::async_trait]
impl Tool for Escalate {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition{name:"escalate".into(),description:"Queue one approved one-way message to this persistent child Session's exact parent; does not await the parent's model.".into(),parameters:json!({"type":"object","additionalProperties":false,"required":["body"],"properties":{"body":{"type":"string","minLength":1,"maxLength":8192}}})}
    }
    async fn execute(&self, arguments: Value, context: ToolContext) -> Result<ToolOutput, Error> {
        context
            .ensure_admitted(None)
            .and_then(|()| context.ensure_admitted_arguments(&arguments))
            .map_err(|error| error.into_runtime_error())?;
        let receipt = context
            .state
            .messaging_peer()?
            .escalate(arguments, context.clone())
            .await?;
        Ok(ToolOutput::text(serde_json::to_string(&receipt)?))
    }
}
