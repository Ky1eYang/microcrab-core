//! Tool contracts, origin-tagged registration, schema checks and deny-by-default authorization.
mod authorization;
mod catalog;
mod context;
pub mod delegate;
pub mod expand;
pub(crate) mod external_dialogue;
pub(crate) mod injection;
mod messaging;
pub(crate) mod secret;
pub mod sleep;
pub(crate) mod switch_mode;
pub use authorization::{AllowTools, DenyAll};
pub(crate) use catalog::ToolRegistry;

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

/// A callable tool receives live state and invocation-scoped capabilities.
#[async_trait]
pub trait Tool: Send + Sync {
    fn definition(&self) -> ToolDefinition;
    /// Only a trusted registered adapter may declare model/UI-facing delivery.
    /// This is not a per-secret permission or a model-controlled tool-name rule.
    fn secret_delivery(&self) -> SecretDelivery {
        SecretDelivery::Resolve
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
    /// Component lifecycle identity is independent of any advertised tool name.
    #[cfg(feature = "wasm")]
    fn wasm_lifecycle(&self) -> Option<super::wasm::AgentLifecycle> {
        None
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
}

/// Builtin tool identities reserved by the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuiltinTool {
    Delegate,
    Job,
    Sleep,
    Expand,
    SwitchMode,
    SendMessage,
    Escalate,
    ReplyExternalSpeaker,
}

impl BuiltinTool {
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "delegate" => Some(Self::Delegate),
            "job" => Some(Self::Job),
            "sleep" => Some(Self::Sleep),
            "expand" => Some(Self::Expand),
            "switch_mode" => Some(Self::SwitchMode),
            "send_message" => Some(Self::SendMessage),
            "escalate" => Some(Self::Escalate),
            external_dialogue::NAME => Some(Self::ReplyExternalSpeaker),
            _ => None,
        }
    }

    pub fn definition(self) -> ToolDefinition {
        match self {
            Self::Delegate => delegate::Delegate.definition(),
            Self::Job => super::execution::job::Job.definition(),
            Self::Sleep => sleep::Sleep.definition(),
            Self::Expand => expand::Expand.definition(),
            Self::SwitchMode => switch_mode::SwitchMode::definition(),
            Self::SendMessage => messaging::SendMessage.definition(),
            Self::Escalate => messaging::Escalate.definition(),
            Self::ReplyExternalSpeaker => external_dialogue::ReplyExternalSpeaker.definition(),
        }
    }

    fn implementation(self) -> Arc<dyn Tool> {
        match self {
            Self::Delegate => Arc::new(delegate::Delegate),
            Self::Job => Arc::new(super::execution::job::Job),
            Self::Sleep => Arc::new(sleep::Sleep),
            Self::Expand => Arc::new(expand::Expand),
            Self::SwitchMode => Arc::new(switch_mode::SwitchMode),
            Self::SendMessage => Arc::new(messaging::SendMessage),
            Self::Escalate => Arc::new(messaging::Escalate),
            Self::ReplyExternalSpeaker => Arc::new(external_dialogue::ReplyExternalSpeaker),
        }
    }
}

/// One catalog entry. Builtin, WASM, and external tools share dispatch but not origin.
#[derive(Clone)]
pub enum ToolRegistration {
    Builtin(BuiltinTool),
    Wasm(Arc<dyn Tool>),
    External(Arc<dyn Tool>),
}

impl ToolRegistration {
    pub fn origin(&self) -> ToolOrigin {
        match self {
            Self::Builtin(_) => ToolOrigin::Builtin,
            Self::Wasm(_) => ToolOrigin::Wasm,
            Self::External(_) => ToolOrigin::External,
        }
    }

    fn tool(&self) -> Arc<dyn Tool> {
        match self {
            Self::Builtin(tool) => tool.implementation(),
            Self::Wasm(tool) | Self::External(tool) => Arc::clone(tool),
        }
    }
}

/// Runtime builtin tools, in stable name order.
pub fn builtin_tools() -> [ToolRegistration; 5] {
    [
        ToolRegistration::Builtin(BuiltinTool::Delegate),
        ToolRegistration::Builtin(BuiltinTool::Expand),
        ToolRegistration::Builtin(BuiltinTool::Job),
        ToolRegistration::Builtin(BuiltinTool::Sleep),
        ToolRegistration::Builtin(BuiltinTool::SwitchMode),
    ]
}
