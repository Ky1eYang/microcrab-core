//! One tool execution identity, bounded output and owned continuation resources.
mod buffer;
mod control;
mod invocation;
pub mod job;
mod model;
pub mod process;
mod redaction;
pub use redaction::{InvocationRedaction, InvocationRedactionStream};
mod registry;
mod retention;
mod status;
#[cfg(test)]
mod tests;
pub use buffer::{BufferedOutput, OutputWriter};
pub use control::{CancellationController, ExecutionController};
pub(crate) use model::ResolvedOutput;
pub use model::{
    DelegateChild, ExecutionCapabilities, ExecutionContentField, ExecutionContentPage,
    ExecutionDelivery, ExecutionId, ExecutionNotice, ExecutionNoticeSink, ExecutionSnapshot,
    ExecutionStatus, FinalOutput, MediaAttachment, OutputAvailability, OutputChunk, OutputMetadata,
    OutputOptions, OutputPage, ToolFailure, ToolFailureKind, ToolOutput,
};
pub use registry::{ExecutionHandle, ExecutionProgress, ExecutionRegistry};
pub use status::{ExecutionKind, StatusProjection};
