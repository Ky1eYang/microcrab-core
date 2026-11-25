//! Admitted-turn execution and its tool catalog.
pub(crate) mod cache;
pub mod execution;
pub(crate) mod handler;
pub mod journal;
pub mod messaging;
pub mod subagents;
pub mod tool;
#[cfg(feature = "wasm")]
pub mod wasm;

pub use handler::{Runtime, RuntimeHandler, TurnOutcome, TurnStopReason};
pub use tool::{
    AdmittedDialogueReply, BuiltinTool, DialogueReplyAdapter, DialogueReplyPosition,
    DialogueReplyReceipt, DialogueReplyRegistration, DialogueReplyRequest,
    MAX_DIALOGUE_REPLY_RECEIPT_BYTES, ToolOrigin, ToolRegistration, builtin_tools,
};
