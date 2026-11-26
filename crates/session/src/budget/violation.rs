//! Bounded, body-free limit facts. Recoverability is attached at the admission
//! boundary, never inferred from a kind which may also occur after effects.
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LimitKind {
    TurnDeadline,
    ToolResultReservation,
    ProviderResponse,
    ContextBytes,
    InputBytes,
    CustomVariableBytes,
    LibraryStorageBytes,
    LibraryStorageEntries,
    RecoveryBytes,
    ExecutionInspection,
    PluginRewrite,
    Rounds,
    ToolCalls,
    HostOperations,
    ToolArguments,
    ToolOutput,
    PendingExecutions,
    ExecutionHistory,
    ExecutionBuffers,
    Processes,
    CacheBytes,
    CachePoints,
    AttachmentBytes,
    AttachmentCount,
    ImagePixels,
    ImageSide,
    InboxBytes,
    InboxEntries,
    DialogueReplyBytes,
    DialogueReplyEntries,
    StateDeliveryBytes,
    ReferenceBytes,
    ReferenceCount,
    WasmBytes,
    OperationDeadline,
    ProviderRequest,
    Arithmetic,
    Other,
}
impl LimitKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TurnDeadline => "turn_deadline",
            Self::ToolResultReservation => "tool_result_reservation",
            Self::ProviderResponse => "provider_response",
            Self::ContextBytes => "context_bytes",
            Self::InputBytes => "input_bytes",
            Self::CustomVariableBytes => "custom_variable_bytes",
            Self::LibraryStorageBytes => "library_storage_bytes",
            Self::LibraryStorageEntries => "library_storage_entries",
            Self::RecoveryBytes => "recovery_bytes",
            Self::ExecutionInspection => "execution_inspection",
            Self::PluginRewrite => "plugin_rewrite",
            Self::Rounds => "rounds",
            Self::ToolCalls => "tool_calls",
            Self::HostOperations => "host_operations",
            Self::ToolArguments => "tool_arguments",
            Self::ToolOutput => "tool_output",
            Self::PendingExecutions => "pending_executions",
            Self::ExecutionHistory => "execution_history",
            Self::ExecutionBuffers => "execution_buffers",
            Self::Processes => "processes",
            Self::CacheBytes => "cache_bytes",
            Self::CachePoints => "cache_points",
            Self::AttachmentBytes => "attachment_bytes",
            Self::AttachmentCount => "attachment_count",
            Self::ImagePixels => "image_pixels",
            Self::ImageSide => "image_side",
            Self::InboxBytes => "inbox_bytes",
            Self::InboxEntries => "inbox_entries",
            Self::DialogueReplyBytes => "dialogue_reply_bytes",
            Self::DialogueReplyEntries => "dialogue_reply_entries",
            Self::StateDeliveryBytes => "state_delivery_bytes",
            Self::ReferenceBytes => "reference_bytes",
            Self::ReferenceCount => "reference_count",
            Self::WasmBytes => "wasm_bytes",
            Self::OperationDeadline => "operation_deadline",
            Self::ProviderRequest => "provider_request",
            Self::Arithmetic => "arithmetic",
            Self::Other => "other",
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LimitError {
    pub kind: LimitKind,
    pub calls: Option<usize>,
    pub required_bytes: Option<usize>,
    pub budget_bytes: Option<usize>,
    pub required_count: Option<usize>,
    pub budget_count: Option<usize>,
    label: String,
    pre_effect: bool,
}
impl LimitError {
    pub fn new(kind: LimitKind) -> Self {
        Self {
            kind,
            calls: None,
            required_bytes: None,
            budget_bytes: None,
            required_count: None,
            budget_count: None,
            label: String::new(),
            pre_effect: false,
        }
    }
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = label.into().chars().take(160).collect();
        self
    }
    pub fn bytes(mut self, required: usize, budget: usize) -> Self {
        self.required_bytes = Some(required);
        self.budget_bytes = Some(budget);
        self
    }
    pub fn counts(mut self, required: usize, budget: usize) -> Self {
        self.required_count = Some(required);
        self.budget_count = Some(budget);
        self
    }
    pub fn calls(mut self, calls: usize) -> Self {
        self.calls = Some(calls);
        self
    }
    pub fn is_pre_effect(&self) -> bool {
        self.pre_effect
    }
    pub fn sanitized(&self) -> Self {
        let mut safe = self.clone();
        safe.label.clear();
        safe
    }
    pub(crate) fn before_effects(mut self) -> Self {
        self.pre_effect = true;
        self
    }
    pub fn safe_message(&self) -> &'static str {
        match self.kind {
            LimitKind::TurnDeadline => "Turn deadline exceeded",
            LimitKind::ToolResultReservation => {
                "Tool result reservation exceeds Context byte budget"
            }
            LimitKind::ProviderResponse => "Provider response exceeds admission budget",
            LimitKind::ContextBytes => "Context byte budget exceeded",
            LimitKind::InputBytes => "Input byte budget exceeded",
            LimitKind::CustomVariableBytes => "Custom variable byte budget exceeded",
            LimitKind::LibraryStorageBytes => {
                "Personal Content Library storage quota exceeded; delete unused items in Dashboard -> 个人内容库"
            }
            LimitKind::LibraryStorageEntries => {
                "Personal Content Library entry capacity exceeded; delete unused items in Dashboard -> 个人内容库"
            }
            LimitKind::RecoveryBytes => "Recovery byte budget exceeded",
            LimitKind::ExecutionInspection => "Execution inspection byte budget exceeded",
            LimitKind::PluginRewrite => "Plugin request rewrite byte budget exceeded",
            LimitKind::Rounds => "Unattended round budget exhausted",
            LimitKind::ToolCalls => "Tool call count budget exceeded",
            LimitKind::HostOperations => "Host operation count budget exceeded",
            LimitKind::ToolArguments => "Tool argument byte budget exceeded",
            LimitKind::ToolOutput => "Tool output byte budget exceeded",
            LimitKind::PendingExecutions => "Pending execution capacity exceeded",
            LimitKind::ExecutionHistory => "Execution history capacity exceeded",
            LimitKind::ExecutionBuffers => "Execution buffer capacity exceeded",
            LimitKind::Processes => "Process capacity exceeded",
            LimitKind::CacheBytes => "Cache point byte budget exceeded",
            LimitKind::CachePoints => "Cache point count budget exceeded",
            LimitKind::AttachmentBytes => "Attachment byte budget exceeded",
            LimitKind::AttachmentCount => "Attachment count budget exceeded",
            LimitKind::ImagePixels => "Image pixel budget exceeded",
            LimitKind::ImageSide => "Image dimension budget exceeded",
            LimitKind::InboxBytes => "Inbox byte budget exceeded",
            LimitKind::InboxEntries => "Inbox entry capacity exceeded",
            LimitKind::DialogueReplyBytes => "Dialogue reply byte budget exceeded",
            LimitKind::DialogueReplyEntries => "Dialogue reply entry capacity exceeded",
            LimitKind::StateDeliveryBytes => "State delivery byte budget exceeded",
            LimitKind::ReferenceBytes => "Reference byte budget exceeded",
            LimitKind::ReferenceCount => "Reference count budget exceeded",
            LimitKind::WasmBytes => "WASM resource byte budget exceeded",
            LimitKind::OperationDeadline => "Operation deadline exceeded",
            LimitKind::ProviderRequest => "Provider request byte budget exceeded",
            LimitKind::Arithmetic => "Resource counter overflow",
            LimitKind::Other => "Operation limit exceeded",
        }
    }
    // Legacy internal diagnostic labels remain available for source compatibility;
    // wire consumers must use safe_message and the numeric fields exclusively.
    pub fn contains(&self, needle: &str) -> bool {
        self.to_string().contains(needle)
    }
}
impl fmt::Display for LimitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.label.is_empty() {
            f.write_str(self.safe_message())
        } else {
            f.write_str(&self.label)
        }
    }
}
impl From<String> for LimitError {
    fn from(label: String) -> Self {
        Self::new(LimitKind::Other).with_label(label)
    }
}
impl From<&str> for LimitError {
    fn from(label: &str) -> Self {
        label.to_owned().into()
    }
}
