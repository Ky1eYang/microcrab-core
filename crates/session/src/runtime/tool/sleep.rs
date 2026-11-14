//! Cancellable duration or generic execution-change waits; output is never consumed.
use super::{Tool, ToolContext, ToolOutput};
use crate::Error;
use async_trait::async_trait;
use microcrab_protocol::ToolDefinition;
use serde_json::{Value, json};
use std::time::{Duration, Instant};
#[derive(Debug, Default, Clone, Copy)]
pub struct Sleep;
impl Sleep {
    pub fn definition() -> ToolDefinition {
        ToolDefinition {
            name: "sleep".into(),
            description: "Wait asynchronously and interruptibly for a duration or another execution's change, without consuming output. Returns success, cancelled, elapsed_ms and reason. Prefer job wait for a particular execution.".into(),
            parameters: json!({
                "type":"object", "additionalProperties":false, "required":["time_ms"],
                "properties":{
                    "time_ms":{"type":"integer","anyOf":[{"minimum":1,"maximum":3600000},{"const":-1}],"description":"Duration in milliseconds, 1..=3600000 bounded by Limits.sleep_max_ms. Required; null is invalid. -1 waits for another pending tool execution to change, failing immediately if none exists; a change does not prove completion."},
                    "reason":{"type":"string","description":"Optional explanation copied into the result. Omission returns reason=null; an empty string remains empty. null is invalid when supplied."}
                }
            }),
        }
    }
}
#[async_trait]
impl Tool for Sleep {
    fn definition(&self) -> ToolDefinition {
        Self::definition()
    }
    async fn execute(&self, args: Value, context: ToolContext) -> Result<ToolOutput, Error> {
        let time = args
            .get("time_ms")
            .and_then(Value::as_i64)
            .ok_or_else(|| Error::tool("sleep requires integer time_ms"))?;
        let started = Instant::now();
        let cancelled = if time == -1 {
            let executions = context.state.executions();
            let mut changes = executions.subscribe();
            let others_pending = || {
                executions.snapshot().iter().any(|row| {
                    Some(row.execution_id) != context.execution_id && row.status.is_pending()
                })
            };
            if !others_pending() {
                return Err(Error::tool(
                    "sleep time_ms=-1 requires another pending execution",
                ));
            }
            tokio::select! { biased; _=context.cancellation.cancelled()=>true, result=changes.changed()=>{result.map_err(|_|Error::Closed)?;false} }
        } else if time > 0
            && time as u64 <= context.state.snapshot().limits.sleep_max_ms
            && time <= 3_600_000
        {
            tokio::select! { biased; _=context.cancellation.cancelled()=>true, _=tokio::time::sleep(Duration::from_millis(time as u64))=>false }
        } else {
            return Err(Error::tool(
                "sleep time_ms must be -1 or 1..=sleep_max_ms (at most 3600000 milliseconds)",
            ));
        };
        Ok(ToolOutput::text(json!({"success":!cancelled,"cancelled":cancelled,"elapsed_ms":started.elapsed().as_millis(),"reason":args.get("reason").cloned().unwrap_or(Value::Null)}).to_string()))
    }
}
