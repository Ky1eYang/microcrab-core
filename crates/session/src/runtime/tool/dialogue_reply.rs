//! Native dialogue effect registration and its invocation-bound authority.
use super::{ToolContext, ToolOutput};
use crate::Error;
use crate::policy::Permission;
use crate::state::communication::{AgentId, reply::MAX_CHUNK_BYTES, turn::AdmittedDialogueTurn};
use async_trait::async_trait;
use microcrab_protocol::{ToolCall, ToolDefinition};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

/// A native adapter's receipt is bounded text, not a second effect or media result.
pub const MAX_DIALOGUE_REPLY_RECEIPT_BYTES: usize = 128;

/// Closed public argument shape. `None` means omitted, not explicit `false`.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DialogueReplyRequest {
    pub dialogue_id: String,
    pub message: String,
    #[serde(default, deserialize_with = "present_bool")]
    pub is_final: Option<bool>,
}

fn present_bool<'de, D: serde::Deserializer<'de>>(value: D) -> Result<Option<bool>, D::Error> {
    bool::deserialize(value).map(Some)
}

impl DialogueReplyRequest {
    pub(super) fn decode(arguments: &Value) -> Result<Self, Error> {
        let result = Self::deserialize(arguments)
            .map_err(|_| invalid_arguments("invalid external dialogue reply arguments"))?;
        if result.dialogue_id.len() != 8
            || !result
                .dialogue_id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || result.message.trim().is_empty()
        {
            return Err(invalid_arguments(
                "reply requires the exact eight-digit lowercase dialogue id and nonblank message",
            ));
        }
        crate::budget::check(
            crate::LimitKind::DialogueReplyBytes,
            result.message.len(),
            MAX_CHUNK_BYTES,
        )
        .map_err(|error| invalid_arguments(&error.to_string()))?;
        Ok(result)
    }
}

fn invalid_arguments(reason: &str) -> Error {
    use crate::observation::diagnostics::{
        DiagnosticCode, DiagnosticLayer, DiagnosticPhase, EffectKnowledge, SafeDiagnosticReason,
        ToolDiagnostic,
    };
    let mut diagnostic = ToolDiagnostic::new(
        DiagnosticCode::InvalidArgument,
        DiagnosticLayer::Runtime,
        DiagnosticPhase::Validation,
    );
    diagnostic.reason = Some(SafeDiagnosticReason::new(reason));
    diagnostic.effects = EffectKnowledge::NotStarted;
    Error::Diagnostic(diagnostic)
}

/// Bounded append coordinates, passed to the pure formatter for preflight.
pub struct DialogueReplyPosition {
    dialogue_id: String,
    chunk_index: u32,
    is_final: bool,
}

/// Acknowledged append, not delivery or Turn settlement. Only Core mints it.
pub struct DialogueReplyReceipt {
    position: DialogueReplyPosition,
    invocation: Arc<()>,
    presentation: String,
}

impl DialogueReplyReceipt {
    pub fn position(&self) -> &DialogueReplyPosition {
        &self.position
    }
}

impl DialogueReplyPosition {
    pub fn dialogue_id(&self) -> &str {
        &self.dialogue_id
    }
    pub fn chunk_index(&self) -> u32 {
        self.chunk_index
    }
    pub fn is_final(&self) -> bool {
        self.is_final
    }
}

/// Trusted embedding adapter. Obtain an owned authority guard, then hand it to
/// `admitted.commit`; do not acquire it in `format_receipt` or across a Turn.
#[async_trait]
pub trait DialogueReplyAdapter: Send + Sync {
    async fn execute(
        &self,
        admitted: AdmittedDialogueReply,
        request: DialogueReplyRequest,
    ) -> Result<DialogueReplyReceipt, Error>;

    /// Pure, synchronous rendering. Core preflights the actual text before append.
    fn format_receipt(&self, position: &DialogueReplyPosition) -> String;
}

/// An explicit native Session effect registration, separate from ordinary tools.
#[derive(Clone)]
pub struct DialogueReplyRegistration {
    pub(super) definition: ToolDefinition,
    pub(super) defaults: Vec<Permission>,
    adapter: Arc<dyn DialogueReplyAdapter>,
    identity: Arc<str>,
}

impl DialogueReplyRegistration {
    pub fn new(
        definition: ToolDefinition,
        defaults: Vec<Permission>,
        adapter: Arc<dyn DialogueReplyAdapter>,
    ) -> Self {
        Self {
            definition,
            defaults,
            adapter,
            identity: format!("session-dialogue-reply:{}", uuid::Uuid::new_v4()).into(),
        }
    }

    pub(crate) fn definition(&self) -> &ToolDefinition {
        &self.definition
    }

    pub(crate) fn admission_identity(&self) -> &str {
        &self.identity
    }

    pub(super) async fn dispatch(
        &self,
        call: &ToolCall,
        invocation: ToolContext,
        owner: Option<Arc<dyn DialogueReplyOwner>>,
    ) -> Result<ToolOutput, Error> {
        let owner =
            owner.ok_or_else(|| Error::denied("external dialogue has no owning Session"))?;
        invocation
            .ensure_admitted(Some(self.admission_identity()))
            .and_then(|()| invocation.ensure_admitted_arguments(&call.arguments))
            .map_err(|error| error.into_runtime_error())?;
        if invocation.call_id != call.id || invocation.tool != self.definition.name {
            return Err(Error::denied("external dialogue invocation differs"));
        }
        let state = invocation.state.snapshot();
        let agent = state
            .communication
            .map(|communication| communication.identity.agent_id)
            .ok_or_else(|| Error::denied("Session has no external dialogue identity"))?;
        let witness = invocation
            .state
            .live_dialogue_turn()
            .filter(|witness| witness.expects_reply_store())
            .ok_or_else(|| Error::denied("no admitted external dialogue in the current Turn"))?;
        let (admitted_agent, epoch, turn, _) = witness.exact_parts();
        if *admitted_agent != agent
            || epoch != state.epoch_id
            || Some(turn.number()) != invocation.turn_id
        {
            return Err(Error::denied(
                "external dialogue invocation differs from the admitted Turn",
            ));
        }
        let request = DialogueReplyRequest::decode(&call.arguments)?;
        let identity = Arc::new(());
        let admitted = AdmittedDialogueReply {
            owner,
            proof: DialogueReplyInvocation {
                registration: self.clone(),
                invocation,
                identity: identity.clone(),
                agent,
                epoch,
                turn: turn.clone(),
            },
        };
        let receipt = self.adapter.execute(admitted, request).await?;
        if !Arc::ptr_eq(&receipt.invocation, &identity) {
            return Err(Error::tool("external dialogue receipt invocation differs"));
        }
        Ok(ToolOutput::text(receipt.presentation))
    }
}

/// One non-cloneable permit. Neither an HTTP caller nor a generic ToolContext
/// can construct it. Its consuming method cannot be replayed.
pub struct AdmittedDialogueReply {
    owner: Arc<dyn DialogueReplyOwner>,
    proof: DialogueReplyInvocation,
}

impl AdmittedDialogueReply {
    /// Transfer the embedding's owned authority into Core's committing lifetime.
    /// Once polled into the owned write, dropping the waiter cannot release it.
    pub async fn commit(
        self,
        request: DialogueReplyRequest,
        authority: impl Send + 'static,
    ) -> Result<DialogueReplyReceipt, Error> {
        self.owner
            .commit_dialogue_reply(self.proof, request, Box::new(authority))
            .await
    }
}

#[async_trait]
pub(crate) trait DialogueReplyOwner: Send + Sync {
    async fn commit_dialogue_reply(
        &self,
        proof: DialogueReplyInvocation,
        request: DialogueReplyRequest,
        authority: Box<dyn Send>,
    ) -> Result<DialogueReplyReceipt, Error>;
}

/// Private handoff between the runtime mint and the Session write owner.
pub(crate) struct DialogueReplyInvocation {
    registration: DialogueReplyRegistration,
    invocation: ToolContext,
    identity: Arc<()>,
    agent: AgentId,
    epoch: u64,
    turn: AdmittedDialogueTurn,
}

impl DialogueReplyInvocation {
    pub(crate) fn context(&self) -> &ToolContext {
        &self.invocation
    }

    pub(crate) fn agent(&self) -> &AgentId {
        &self.agent
    }

    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }

    pub(crate) fn turn(&self) -> &AdmittedDialogueTurn {
        &self.turn
    }

    pub(crate) fn registration_identity(&self) -> &str {
        self.registration.admission_identity()
    }

    pub(crate) fn verify_request(&self, request: &DialogueReplyRequest) -> Result<(), Error> {
        let admitted = self
            .invocation
            .admitted_arguments()
            .map_err(|error| error.into_runtime_error())?;
        self.invocation
            .ensure_admitted(Some(self.registration.admission_identity()))
            .and_then(|()| self.invocation.ensure_admitted_arguments(admitted))
            .map_err(|error| error.into_runtime_error())?;
        if DialogueReplyRequest::decode(admitted)? != *request {
            return Err(Error::denied(
                "external dialogue request differs from admitted arguments",
            ));
        }
        Ok(())
    }

    pub(crate) fn prepare_receipt(
        &self,
        dialogue_id: String,
        chunk_index: u32,
        is_final: bool,
        limit: usize,
    ) -> Result<(DialogueReplyPosition, String), Error> {
        let position = DialogueReplyPosition {
            dialogue_id,
            chunk_index,
            is_final,
        };
        let rendered = self.registration.adapter.format_receipt(&position);
        crate::budget::check(
            crate::LimitKind::ToolOutput,
            rendered.len(),
            MAX_DIALOGUE_REPLY_RECEIPT_BYTES.min(limit),
        )?;
        // Execution will apply its usual masking to this exact rendering.
        let redacted = self.invocation.redaction_handle().redact_content(&rendered);
        crate::budget::check(crate::LimitKind::ToolOutput, redacted.len(), limit)?;
        Ok((position, rendered))
    }

    pub(crate) fn acknowledge(
        self,
        (position, presentation): (DialogueReplyPosition, String),
    ) -> DialogueReplyReceipt {
        DialogueReplyReceipt {
            position,
            presentation,
            invocation: self.identity,
        }
    }

    pub(crate) fn begin_dispatch(&self) -> Result<(), Error> {
        self.invocation
            .begin_dispatch(Some(self.registration.admission_identity()))
            .map_err(|error| error.into_runtime_error())
    }
}
