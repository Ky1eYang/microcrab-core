//! Reserved model tool, not an external Host or transport API.
use super::{Tool, ToolContext, ToolOutput};
use crate::Error;
use crate::state::communication::reply::MAX_CHUNK_BYTES;
use microcrab_protocol::ToolDefinition;
use serde::Deserialize;
use serde_json::{Value, json};

pub(crate) const NAME: &str = "reply_external_speaker";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReplyArguments {
    pub dialogue_id: String,
    pub message: String,
    #[serde(default)]
    pub is_final: bool,
}

impl ReplyArguments {
    pub(crate) fn parse(arguments: Value) -> Result<Self, Error> {
        let result: Self = serde_json::from_value(arguments)
            .map_err(|_| Error::tool("invalid external dialogue reply arguments"))?;
        if result.dialogue_id.len() != 8
            || !result
                .dialogue_id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || result.message.trim().is_empty()
        {
            return Err(Error::tool(
                "reply requires the exact eight-digit lowercase dialogue id and nonblank message",
            ));
        }
        crate::budget::check(
            crate::LimitKind::DialogueReplyBytes,
            result.message.len(),
            MAX_CHUNK_BYTES,
        )?;
        Ok(result)
    }
}

pub(crate) struct ReplyExternalSpeaker;

#[async_trait::async_trait]
impl Tool for ReplyExternalSpeaker {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: NAME.into(),
            description: "Commit a message only to the external dialogue identified by the id attribute of an <external_...> input admitted in this Turn. Normal assistant output is not relayed. is_final prevents further messages to this dialogue; the dialogue settles when this Turn ends.".into(),
            parameters: json!({
                "type":"object", "additionalProperties":false,
                "required":["dialogue_id", "message"],
                "properties":{
                    "dialogue_id":{"type":"string", "pattern":"^[0-9a-f]{8}$", "minLength":8, "maxLength":8},
                    "message":{"type":"string", "minLength":1, "maxLength":MAX_CHUNK_BYTES},
                    "is_final":{"type":"boolean", "default":false}
                }
            }),
        }
    }

    async fn execute(
        &self,
        arguments: Value,
        invocation: ToolContext,
    ) -> Result<ToolOutput, Error> {
        invocation
            .state
            .messaging_peer()?
            .reply_external_speaker(arguments, invocation)
            .await
    }
}
