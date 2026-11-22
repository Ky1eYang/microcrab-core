//! Exact private dialogue reply effects. Neither a storage handle nor a model
//! alias establishes tool authority or substitutes for speaker authentication.
use super::{AgentId, inbox::OperationReceipt, turn::AdmittedDialogueTurn};
use crate::{Error, LimitKind, budget::check};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Mutex};
use uuid::Uuid;

pub const MAX_CHUNK_BYTES: usize = 8 * 1024;
pub const MAX_CHUNKS: usize = 16;
pub const MAX_DIALOGUE_TEXT_BYTES: usize = 64 * 1024;
pub const MAX_SESSION_REPLY_BYTES: usize = 4 * 1024 * 1024;
// Reserve room for a metadata-only terminal record for every retained inbox
// receipt, even when existing replies have used all available payload capacity.
pub const REPLY_METADATA_RESERVE: usize = 512;
const TERMINAL_RESERVE: usize = 128;
pub const MAX_REPLY_RECORD_BYTES: usize = 6 * MAX_DIALOGUE_TEXT_BYTES + 4096;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DialogueAdmissionKey {
    id: Uuid,
    witness: String,
}
impl DialogueAdmissionKey {
    pub(super) fn new(id: Uuid, witness: String) -> Self {
        Self { id, witness }
    }
    /// Storage address only; the full key must still match.
    pub fn id(&self) -> Uuid {
        self.id
    }
    fn validate(&self) -> Result<(), Error> {
        if !valid_nonce(self.id)
            || self.witness.len() != 64
            || !self
                .witness
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(invalid());
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DialogueReplyBinding {
    agent_id: AgentId,
    admission: DialogueAdmissionKey,
    turn: AdmittedDialogueTurn,
}
impl DialogueReplyBinding {
    pub(crate) fn from_admission(
        agent_id: AgentId,
        receipt: &OperationReceipt,
        turn: AdmittedDialogueTurn,
    ) -> Result<Option<Self>, Error> {
        use super::inbox::DeliveryStatus;
        if receipt.receipt.status != DeliveryStatus::Consumed {
            return Err(invalid());
        }
        Ok(receipt.dialogue_admission_key()?.map(|admission| Self {
            agent_id,
            admission,
            turn,
        }))
    }
    pub fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }
    pub fn admission(&self) -> &DialogueAdmissionKey {
        &self.admission
    }
    pub fn turn(&self) -> &AdmittedDialogueTurn {
        &self.turn
    }
    fn validate(&self) -> Result<(), Error> {
        self.admission.validate()?;
        self.turn.validate()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DialogueTerminalReason {
    Completed,
    Cancelled,
    Interrupted,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DialogueTerminal {
    reason: DialogueTerminalReason,
    at_ms: u64,
}
impl DialogueTerminal {
    pub fn reason(&self) -> DialogueTerminalReason {
        self.reason
    }
    pub fn at_ms(&self) -> u64 {
        self.at_ms
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DialogueReplyChunk {
    // Assignment by Session invocation (stable across an exact acknowledgement
    // retry), never directly supplied by model tool arguments.
    effect_id: Uuid,
    index: u32,
    message: String,
    is_final: bool,
    at_ms: u64,
}
impl DialogueReplyChunk {
    pub fn index(&self) -> u32 {
        self.index
    }
    pub fn message(&self) -> &str {
        &self.message
    }
    pub fn is_final(&self) -> bool {
        self.is_final
    }
    pub fn at_ms(&self) -> u64 {
        self.at_ms
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableDialogueReply {
    version: u8,
    binding: DialogueReplyBinding,
    revision: u64,
    chunks: Vec<DialogueReplyChunk>,
    terminal: Option<DialogueTerminal>,
}
impl DurableDialogueReply {
    pub fn binding(&self) -> &DialogueReplyBinding {
        &self.binding
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn chunks(&self) -> &[DialogueReplyChunk] {
        &self.chunks
    }
    pub fn terminal(&self) -> Option<&DialogueTerminal> {
        self.terminal.as_ref()
    }
    /// A completed Turn with no explicit replies. Interruption is not no-reply.
    pub fn is_no_reply(&self) -> bool {
        self.chunks.is_empty()
            && self
                .terminal()
                .is_some_and(|terminal| terminal.reason() == DialogueTerminalReason::Completed)
    }
    pub fn validate(&self) -> Result<(), Error> {
        self.binding.validate()?;
        check(
            LimitKind::DialogueReplyEntries,
            self.chunks.len(),
            MAX_CHUNKS,
        )?;
        if self.version != 1
            || self.revision != self.chunks.len() as u64 + u64::from(self.terminal.is_some())
        {
            return Err(invalid());
        }
        let mut total = 0;
        for (index, chunk) in self.chunks.iter().enumerate() {
            validate_chunk(chunk.effect_id, &chunk.message)?;
            total += chunk.message.len();
            if chunk.index as usize != index
                || (chunk.is_final && index + 1 != self.chunks.len())
                || self.chunks[..index]
                    .iter()
                    .any(|other| other.effect_id == chunk.effect_id)
            {
                return Err(invalid());
            }
        }
        check(
            LimitKind::DialogueReplyBytes,
            total,
            MAX_DIALOGUE_TEXT_BYTES,
        )
    }
    pub fn validate_exact(
        &self,
        agent_id: &AgentId,
        key: &DialogueAdmissionKey,
    ) -> Result<(), Error> {
        self.validate()?;
        if self.binding.agent_id() != agent_id || self.binding.admission() != key {
            return Err(conflict());
        }
        Ok(())
    }
    /// Encode one bounded record. Physical padding on open records reserves
    /// terminal growth, so backends can account from lengths without parsing
    /// unrelated payloads. The same encoding drives the memory adapter's budget.
    pub fn storage_bytes(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        let mut bytes = serde_json::to_vec(self)?;
        if self.terminal.is_none() {
            bytes.resize(bytes.len() + TERMINAL_RESERVE, b' ');
        }
        reply_extra_charge(bytes.len())?;
        Ok(bytes)
    }

    pub fn from_storage_bytes(bytes: &[u8]) -> Result<Self, Error> {
        reply_extra_charge(bytes.len())?;
        let record: Self = serde_json::from_slice(bytes).map_err(|_| invalid())?;
        if record.storage_bytes()? != bytes {
            return Err(invalid());
        }
        Ok(record)
    }
}

pub fn reply_extra_charge(encoded_len: usize) -> Result<usize, Error> {
    check(
        LimitKind::DialogueReplyBytes,
        encoded_len,
        MAX_REPLY_RECORD_BYTES,
    )?;
    Ok(encoded_len.saturating_sub(REPLY_METADATA_RESERVE))
}

pub fn check_reply_budget(count: usize, extra_charge: usize) -> Result<(), Error> {
    let slots = super::inbox::MAX_RECEIPTS;
    check(LimitKind::DialogueReplyEntries, count, slots)?;
    check(
        LimitKind::DialogueReplyBytes,
        extra_charge.saturating_add(slots * REPLY_METADATA_RESERVE),
        MAX_SESSION_REPLY_BYTES,
    )
}

#[derive(Clone)]
pub enum DialogueReplyMutation {
    Open {
        binding: DialogueReplyBinding,
    },
    Append {
        binding: DialogueReplyBinding,
        expected_revision: u64,
        effect_id: Uuid,
        message: String,
        is_final: bool,
        at_ms: u64,
    },
    Settle {
        binding: DialogueReplyBinding,
        expected_revision: u64,
        reason: DialogueTerminalReason,
        at_ms: u64,
    },
}
impl DialogueReplyMutation {
    pub fn binding(&self) -> &DialogueReplyBinding {
        match self {
            Self::Open { binding }
            | Self::Append { binding, .. }
            | Self::Settle { binding, .. } => binding,
        }
    }
    /// Pure transition; the adapter must execute it under its storage CAS and
    /// acknowledge Applied only after publication. AlreadyApplied is not an event.
    pub fn apply(self, previous: Option<&DurableDialogueReply>) -> Result<ReplyWrite, Error> {
        self.binding().validate()?;
        if let Some(record) = previous {
            record.validate()?;
            if self.binding() != record.binding() {
                return Err(conflict());
            }
        }
        let result = match self {
            Self::Open { binding } => {
                if let Some(record) = previous {
                    return Ok(ReplyWrite::AlreadyApplied(record.clone()));
                }
                DurableDialogueReply {
                    version: 1,
                    binding,
                    revision: 0,
                    chunks: Vec::new(),
                    terminal: None,
                }
            }
            Self::Append {
                expected_revision,
                effect_id,
                message,
                is_final,
                at_ms,
                ..
            } => {
                validate_chunk(effect_id, &message)?;
                let record = previous.ok_or_else(conflict)?;
                if let Some(chunk) = record
                    .chunks
                    .iter()
                    .find(|chunk| chunk.effect_id == effect_id)
                {
                    if chunk.index as u64 == expected_revision
                        && chunk.message == message
                        && chunk.is_final == is_final
                        && chunk.at_ms == at_ms
                    {
                        return Ok(ReplyWrite::AlreadyApplied(record.clone()));
                    }
                    return Err(conflict());
                }
                if record.revision != expected_revision
                    || record.terminal.is_some()
                    || record.chunks.last().is_some_and(|chunk| chunk.is_final)
                {
                    return Err(conflict());
                }
                check(
                    LimitKind::DialogueReplyEntries,
                    record.chunks.len() + 1,
                    MAX_CHUNKS,
                )?;
                check(
                    LimitKind::DialogueReplyBytes,
                    record
                        .chunks
                        .iter()
                        .map(|chunk| chunk.message.len())
                        .sum::<usize>()
                        + message.len(),
                    MAX_DIALOGUE_TEXT_BYTES,
                )?;
                let mut next = record.clone();
                next.chunks.push(DialogueReplyChunk {
                    effect_id,
                    index: next.chunks.len() as u32,
                    message,
                    is_final,
                    at_ms,
                });
                next.revision += 1;
                next
            }
            Self::Settle {
                expected_revision,
                reason,
                at_ms,
                ..
            } => {
                let record = previous.ok_or_else(conflict)?;
                let terminal = DialogueTerminal { reason, at_ms };
                if record.terminal.as_ref() == Some(&terminal)
                    && expected_revision.checked_add(1) == Some(record.revision)
                {
                    return Ok(ReplyWrite::AlreadyApplied(record.clone()));
                }
                if record.revision != expected_revision || record.terminal.is_some() {
                    return Err(conflict());
                }
                let mut next = record.clone();
                next.terminal = Some(terminal);
                next.revision += 1;
                next
            }
        };
        result.validate()?;
        Ok(ReplyWrite::Applied(result))
    }
}

pub enum ReplyWrite {
    Applied(DurableDialogueReply),
    AlreadyApplied(DurableDialogueReply),
}
impl ReplyWrite {
    pub(crate) fn into_record(self) -> DurableDialogueReply {
        match self {
            Self::Applied(record) | Self::AlreadyApplied(record) => record,
        }
    }
    pub fn record(&self) -> &DurableDialogueReply {
        match self {
            Self::Applied(record) | Self::AlreadyApplied(record) => record,
        }
    }
}

#[async_trait]
pub trait DialogueReplyStore: Send + Sync {
    async fn load(
        &self,
        agent_id: &AgentId,
        key: &DialogueAdmissionKey,
    ) -> Result<Option<DurableDialogueReply>, Error>;
    async fn apply(&self, mutation: DialogueReplyMutation) -> Result<ReplyWrite, Error>;
}

/// Volatile adapter for in-memory embeddings and tests. It does not claim restart
/// durability. The map is tied to one Session communication identity.
pub struct MemoryDialogueReplies {
    agent_id: AgentId,
    records: Mutex<BTreeMap<Uuid, (usize, DurableDialogueReply)>>,
}
impl MemoryDialogueReplies {
    pub fn new(agent_id: AgentId) -> Self {
        Self {
            agent_id,
            records: Mutex::default(),
        }
    }
}
#[async_trait]
impl DialogueReplyStore for MemoryDialogueReplies {
    async fn load(
        &self,
        agent_id: &AgentId,
        key: &DialogueAdmissionKey,
    ) -> Result<Option<DurableDialogueReply>, Error> {
        if agent_id != &self.agent_id {
            return Err(conflict());
        }
        let records = self.records.lock().map_err(|_| invalid())?;
        records
            .get(&key.id())
            .map(|(_, record)| {
                record.validate_exact(agent_id, key)?;
                Ok(record.clone())
            })
            .transpose()
    }
    async fn apply(&self, mutation: DialogueReplyMutation) -> Result<ReplyWrite, Error> {
        if mutation.binding().agent_id() != &self.agent_id {
            return Err(conflict());
        }
        let mut records = self.records.lock().map_err(|_| invalid())?;
        let id = mutation.binding().admission().id();
        let previous = records.get(&id);
        let outcome = mutation.apply(previous.map(|(_, record)| record))?;
        if let ReplyWrite::Applied(record) = &outcome {
            let extra = reply_extra_charge(record.storage_bytes()?.len())?;
            let total = records.values().map(|(charge, _)| *charge).sum::<usize>();
            check_reply_budget(
                records.len() + usize::from(previous.is_none()),
                total - previous.map_or(0, |(charge, _)| *charge) + extra,
            )?;
            records.insert(id, (extra, record.clone()));
        }
        Ok(outcome)
    }
}

fn valid_nonce(id: Uuid) -> bool {
    id.get_version() == Some(uuid::Version::Random) && id.get_variant() == uuid::Variant::RFC4122
}
fn validate_chunk(id: Uuid, message: &str) -> Result<(), Error> {
    if !valid_nonce(id) || message.trim().is_empty() {
        return Err(invalid());
    }
    check(
        LimitKind::DialogueReplyBytes,
        message.len(),
        MAX_CHUNK_BYTES,
    )
}
fn invalid() -> Error {
    Error::protocol("invalid private dialogue reply record")
}
fn conflict() -> Error {
    Error::StateObservationConflict
}
