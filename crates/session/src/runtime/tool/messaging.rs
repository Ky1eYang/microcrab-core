//! Messaging builtin schemas. Session owns delivery, atomic exchanges and durability.
use super::{Tool, ToolContext, ToolOutput};
use crate::Error;
use microcrab_protocol::ToolDefinition;
use serde_json::{Value, json};

fn result(
    context: &ToolContext,
    receipt: &crate::state::communication::inbox::QueuedReceipt,
) -> Result<ToolOutput, Error> {
    #[derive(serde::Serialize)]
    struct ModelReceipt<'a> {
        operation_ref: &'a str,
        ordinal: u64,
        status: &'a crate::state::communication::inbox::DeliveryStatus,
    }
    let content = if context.state.short_model_refs() {
        let raw = &receipt.operation_id;
        let operation_ref = raw
            .strip_prefix("msg_")
            .filter(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .and_then(|_| raw.get(..20))
            .ok_or_else(|| {
                Error::protocol("messaging producer returned an invalid operation receipt")
            })?;
        serde_json::to_string(&ModelReceipt {
            operation_ref,
            ordinal: receipt.ordinal,
            status: &receipt.status,
        })?
    } else {
        serde_json::to_string(receipt)?
    };
    Ok(ToolOutput::text(content))
}
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
        send_message_admitted(arguments, context).await
    }
}

pub(crate) async fn send_message_admitted(
    arguments: Value,
    context: ToolContext,
) -> Result<ToolOutput, Error> {
    let receipt = context
        .state
        .messaging_peer()?
        .send_message(arguments, context.clone())
        .await?;
    result(&context, &receipt)
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
        escalate_admitted(arguments, context).await
    }
}

pub(crate) async fn escalate_admitted(
    arguments: Value,
    context: ToolContext,
) -> Result<ToolOutput, Error> {
    let receipt = context
        .state
        .messaging_peer()?
        .escalate(arguments, context.clone())
        .await?;
    result(&context, &receipt)
}
