//! Tool contracts, origin-tagged registration, schema checks and deny-by-default authorization.
mod authorization;
mod catalog;
mod context;
pub mod delegate;
mod dialogue_reply;
pub(crate) mod injection;
pub(crate) mod messaging;
pub mod reference;
pub(crate) mod secret;
pub mod sleep;
pub(crate) mod switch_mode;
pub use authorization::{AllowTools, DenyAll};
pub(crate) use catalog::ToolRegistry;
pub use dialogue_reply::{
    AdmittedDialogueReply, DialogueReplyAdapter, DialogueReplyPosition, DialogueReplyReceipt,
    DialogueReplyRegistration, DialogueReplyRequest, MAX_DIALOGUE_REPLY_RECEIPT_BYTES,
};
pub(crate) use dialogue_reply::{DialogueReplyInvocation, DialogueReplyOwner};

pub use super::execution::{FinalOutput, ToolFailure, ToolFailureKind, ToolOutput};
use crate::Error;
use async_trait::async_trait;
use microcrab_protocol::ToolDefinition;
use serde_json::Value;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct ToolContext {
    pub subject: crate::policy::InvocationSubject,
    pub state: crate::State,
    pub processes: Arc<dyn super::execution::process::ProcessControl>,
    pub wakeup: Arc<tokio::sync::Notify>,
    pub cancellation: CancellationToken,
    /// Only Runtime's builtin wait observer receives this signal. It never owns
    /// cancellation of the executions being observed.
    pub(crate) observation_interrupt: Option<CancellationToken>,
    pub call_id: String,
    pub tool: String,
    pub(crate) tool_description: Option<String>,
    pub session_id: String,
    pub agent: String,
    pub mode: Option<String>,
    pub turn_id: Option<u64>,
    pub round_id: Option<u64>,
    pub admission: Option<Arc<crate::policy::AdmittedInvocation>>,
    pub interaction_timeout_ms: u64,
    pub execution_id: Option<super::execution::ExecutionId>,
    pub execution_progress: Option<super::execution::ExecutionProgress>,
    resolved_arguments: Option<Arc<secret::ResolvedArguments>>,
    pub(crate) literal_mask: injection::LiteralMask,
    pub(crate) redirect_destination: Option<crate::state::reference::ReferenceRedirectDestination>,
    pub(crate) file_version_policy_arguments: Option<Arc<Value>>,
    redaction: super::execution::InvocationRedaction,
    cache_scope: Option<super::cache::Scope>,
    cache_collector: Arc<super::cache::Collector>,
}

/// Argument delivery chosen by the trusted registered implementation, not by a
/// model-supplied name. Human interaction adapters may forward placeholders to UI;
/// ordinary native, WASM and plugin tools resolve them only after permission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SecretDelivery {
    #[default]
    Resolve,
    ForwardPlaceholders,
}

#[cfg(feature = "wasm")]
pub(crate) const GLOB_PATH_DEFAULT: &str = "{{var:glob_ignored}}";

/// A callable tool receives live state and invocation-scoped capabilities.
#[async_trait]
pub trait Tool: Send + Sync {
    fn definition(&self) -> ToolDefinition;
    /// Only a trusted registered adapter may declare model/UI-facing delivery.
    /// This is not a per-secret permission or a model-controlled tool-name rule.
    fn secret_delivery(&self) -> SecretDelivery {
        SecretDelivery::Resolve
    }
    /// Register literal arguments that cannot be sourced from visible variables
    /// or Secret interpolation. Chosen by the implementation, not the model.
    #[cfg(feature = "wasm")]
    fn uses_literal_sources(&self) -> bool {
        false
    }
    /// Trusted registered implementation's maximum encoded image output.
    /// Text tools keep zero. This declaration grants no Host or path permission.
    fn media_output_bytes(&self) -> usize {
        0
    }

    /// Registered component identity binds admission to the actual implementation.
    fn component_identity(&self) -> Option<&str> {
        None
    }
    /// Actual negotiated component/Host capability; never inferred from tool text.
    #[cfg(feature = "wasm")]
    fn file_version_host(&self) -> Option<&dyn microcrab_host_api::Host> {
        None
    }
    /// Component lifecycle identity is independent of any advertised tool name.
    #[cfg(feature = "wasm")]
    fn wasm_lifecycle(&self) -> Option<super::wasm::AgentLifecycle> {
        None
    }
    /// Trusted registered WASM adapter links the explicit resolved-path glob import.
    #[cfg(feature = "wasm")]
    fn uses_glob_path_default(&self) -> bool {
        false
    }
    /// Defaults are scoped to this registered tool and remain below native/user rules.
    fn permission_defaults(&self) -> Vec<crate::policy::Permission> {
        Vec::new()
    }
    async fn execute(&self, arguments: Value, context: ToolContext) -> Result<ToolOutput, Error>;
}

/// Where a registered tool came from. Builtin names cannot be supplied by another origin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolOrigin {
    Builtin,
    Wasm,
    External,
    DialogueReply,
}

/// Builtin tool identities reserved by the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuiltinTool {
    Delegate,
    Job,
    Sleep,
    Reference,
    SwitchMode,
    SendMessage,
    Escalate,
}

impl BuiltinTool {
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "delegate" => Some(Self::Delegate),
            "job" => Some(Self::Job),
            "sleep" => Some(Self::Sleep),
            reference::NAME => Some(Self::Reference),
            "switch_mode" => Some(Self::SwitchMode),
            "send_message" => Some(Self::SendMessage),
            "escalate" => Some(Self::Escalate),
            _ => None,
        }
    }

    pub fn definition(self) -> ToolDefinition {
        match self {
            Self::Delegate => delegate::Delegate.definition(),
            Self::Job => super::execution::job::Job.definition(),
            Self::Sleep => sleep::Sleep.definition(),
            Self::Reference => reference::Reference::definition(),
            Self::SwitchMode => switch_mode::SwitchMode::definition(),
            Self::SendMessage => messaging::SendMessage.definition(),
            Self::Escalate => messaging::Escalate.definition(),
        }
    }

    fn implementation(self) -> Arc<dyn Tool> {
        match self {
            Self::Delegate => Arc::new(delegate::Delegate),
            Self::Job => Arc::new(super::execution::job::Job),
            Self::Sleep => Arc::new(sleep::Sleep),
            Self::Reference => Arc::new(reference::Reference),
            Self::SwitchMode => Arc::new(switch_mode::SwitchMode),
            Self::SendMessage => Arc::new(messaging::SendMessage),
            Self::Escalate => Arc::new(messaging::Escalate),
        }
    }
}

/// One catalog entry. A native Session effect cannot be wrapped as an ordinary Tool.
#[derive(Clone)]
pub enum ToolRegistration {
    Builtin(BuiltinTool),
    Wasm(Arc<dyn Tool>),
    External(Arc<dyn Tool>),
    DialogueReply(DialogueReplyRegistration),
}

impl ToolRegistration {
    pub fn origin(&self) -> ToolOrigin {
        match self {
            Self::Builtin(_) => ToolOrigin::Builtin,
            Self::Wasm(_) => ToolOrigin::Wasm,
            Self::External(_) => ToolOrigin::External,
            Self::DialogueReply(_) => ToolOrigin::DialogueReply,
        }
    }
}

/// The Coding catalog explicitly advertises the new projection in its frozen
/// read definition. An older exact catalog never silently changes result shapes.
pub(crate) fn short_model_refs(definitions: &[ToolDefinition]) -> bool {
    definitions.iter().any(|tool| {
        tool.name == "read"
            && tool
                .description
                .starts_with("Read text with scoped file version references (fv_).")
    })
}

/// Runtime builtin tools, in stable name order.
pub fn builtin_tools() -> [ToolRegistration; 5] {
    [
        ToolRegistration::Builtin(BuiltinTool::Delegate),
        ToolRegistration::Builtin(BuiltinTool::Job),
        ToolRegistration::Builtin(BuiltinTool::Reference),
        ToolRegistration::Builtin(BuiltinTool::Sleep),
        ToolRegistration::Builtin(BuiltinTool::SwitchMode),
    ]
}
