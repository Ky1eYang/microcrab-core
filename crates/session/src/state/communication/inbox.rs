//! Durable accepted carrier and idempotency receipts. No authentication secrets.
use super::{Correspondent, ExternalSpeaker};
use crate::Error;
use microcrab_transport::UserSource;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

mod dialogue;
use dialogue::DialogueAdmission;
pub use dialogue::{ExternalCredentialIncarnation, ExternalDialogueReceipt};

pub const MAX_CARRIERS: usize = 128;
pub const MAX_CARRIER_BYTES: usize = 128 * 1024;
pub const MAX_BODY_BYTES: usize = 8192;
pub const MAX_RECEIPTS: usize = 4096;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryStatus {
    Queued,
    Consumed,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueuedReceipt {
    pub operation_id: String,
    pub ordinal: u64,
    pub status: DeliveryStatus,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationReceipt {
    pub receipt: QueuedReceipt,
    pub digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dialogue: Option<DialogueAdmission>,
}
impl OperationReceipt {
    pub(crate) fn new(
        inbox: &DurableInbox,
        carrier: &AcceptedCarrier,
        digest: String,
        incarnation: Option<&ExternalCredentialIncarnation>,
    ) -> Result<Self, Error> {
        let dialogue = incarnation
            .map(|proof| DialogueAdmission::allocate(inbox, carrier, proof, uuid::Uuid::new_v4))
            .transpose()?;
        Ok(Self {
            receipt: QueuedReceipt {
                operation_id: carrier.operation_id.clone(),
                ordinal: carrier.ordinal,
                status: DeliveryStatus::Queued,
            },
            digest,
            dialogue,
        })
    }

    /// An exact consumption witness may advance status, but no admission facts.
    pub fn same_admission(&self, other: &Self) -> bool {
        self.receipt.operation_id == other.receipt.operation_id
            && self.receipt.ordinal == other.receipt.ordinal
            && self.digest == other.digest
            && self.dialogue == other.dialogue
    }

    pub(crate) fn matches_enqueue(
        &self,
        digest: &str,
        identity: Option<&str>,
        incarnation: Option<&ExternalCredentialIncarnation>,
    ) -> bool {
        self.digest == digest
            && match (&self.dialogue, identity, incarnation) {
                (Some(dialogue), Some(identity), Some(proof)) => dialogue.matches(identity, proof),
                (None, _, None) => true,
                _ => false,
            }
    }

    pub(super) fn dialogue_admission_key(
        &self,
    ) -> Result<Option<super::reply::DialogueAdmissionKey>, Error> {
        self.dialogue
            .as_ref()
            .map(|admission| {
                admission.validate(&self.receipt.operation_id)?;
                // The exact commitment remains stable when a queued receipt is
                // advanced to consumed or history is selected on another branch.
                let fields = serde_json::to_vec(&(
                    &self.receipt.operation_id,
                    self.receipt.ordinal,
                    &self.digest,
                    admission,
                ))?;
                let mut digest = Sha256::new();
                digest.update(b"microcrab/private-dialogue-admission/v1\0");
                digest.update(fields);
                Ok(super::reply::DialogueAdmissionKey::new(
                    admission.canonical_id,
                    format!("{:x}", digest.finalize()),
                ))
            })
            .transpose()
    }

    pub(crate) fn dialogue_alias(&self) -> Option<&str> {
        self.dialogue
            .as_ref()
            .map(|dialogue| dialogue.alias.as_str())
    }

    pub(crate) fn legacy_receipt(&self) -> Option<QueuedReceipt> {
        self.dialogue.is_none().then(|| self.receipt.clone())
    }

    pub(crate) fn authenticated_external_receipt(
        &self,
        identity: &str,
        incarnation: &ExternalCredentialIncarnation,
    ) -> Option<ExternalDialogueReceipt> {
        self.dialogue
            .as_ref()
            .filter(|row| row.matches(identity, incarnation))
            .map(|row| ExternalDialogueReceipt {
                receipt: self.receipt.clone(),
                dialogue_id: row.alias.clone(),
            })
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedCarrier {
    pub operation_id: String,
    pub ordinal: u64,
    pub body: String,
    pub source: UserSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correspondent: Option<Correspondent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speaker: Option<ExternalSpeaker>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableInbox {
    pub revision: u64,
    pub next_ordinal: u64,
    pub pending: Vec<AcceptedCarrier>,
    pub receipts: Vec<OperationReceipt>,
}
impl DurableInbox {
    /// Idempotent full-sidecar publication with irreversible accepted receipts.
    pub fn check_successor(&self, previous: Option<&Self>) -> Result<bool, Error> {
        self.validate()?;
        if let Some(prior) = previous {
            prior.validate()?;
            if prior == self {
                return Ok(false);
            }
            for receipt in &prior.receipts {
                let next = self
                    .receipts
                    .iter()
                    .find(|row| row.receipt.operation_id == receipt.receipt.operation_id)
                    .ok_or_else(|| Error::config("communication receipt cannot be forgotten"))?;
                if !receipt.same_admission(next)
                    || (receipt.receipt.status == DeliveryStatus::Consumed
                        && next.receipt.status != receipt.receipt.status)
                {
                    return Err(Error::config(
                        "communication receipt cannot be replaced or replayed",
                    ));
                }
            }
            if self.next_ordinal < prior.next_ordinal {
                return Err(Error::config("communication ordinal cannot regress"));
            }
            for receipt in &self.receipts {
                if receipt.receipt.ordinal <= prior.next_ordinal
                    && !prior.receipts.iter().any(|previous| {
                        previous.receipt.operation_id == receipt.receipt.operation_id
                    })
                {
                    return Err(Error::config(
                        "new communication receipt must advance the arrival ordinal",
                    ));
                }
            }
        }
        let revision = previous
            .map_or(0, |value| value.revision)
            .checked_add(1)
            .ok_or_else(|| Error::config("communication revision overflow"))?;
        if self.revision != revision {
            return Err(Error::StateObservationConflict);
        }
        Ok(true)
    }
    pub(crate) fn operation(&self, operation_id: &str) -> Option<&OperationReceipt> {
        self.receipts
            .iter()
            .find(|row| row.receipt.operation_id == operation_id)
    }
    /// Compatibility observation for receipts without an exact credential binding.
    pub fn receipt(&self, operation_id: &str) -> Option<QueuedReceipt> {
        self.operation(operation_id)?.legacy_receipt()
    }
    /// Match exact operation and credential incarnation; never infer a legacy link.
    pub fn authenticated_external_receipt(
        &self,
        operation_id: &str,
        identity: &str,
        incarnation: &ExternalCredentialIncarnation,
    ) -> Result<Option<ExternalDialogueReceipt>, Error> {
        validate_operation(operation_id)?;
        Ok(self
            .operation(operation_id)
            .and_then(|row| row.authenticated_external_receipt(identity, incarnation)))
    }
    /// Resolve a presentation alias only with its exact authenticated credential
    /// incarnation. Legacy receipts are deliberately never upgraded here.
    pub fn authenticated_external_dialogue(
        &self,
        alias: &str,
        identity: &str,
        incarnation: &ExternalCredentialIncarnation,
    ) -> Result<Option<(ExternalDialogueReceipt, super::reply::DialogueAdmissionKey)>, Error> {
        let Some(row) = self.receipts.iter().find(|row| {
            row.dialogue
                .as_ref()
                .is_some_and(|admission| admission.alias == alias)
        }) else {
            return Ok(None);
        };
        let Some(receipt) = row.authenticated_external_receipt(identity, incarnation) else {
            return Ok(None);
        };
        Ok(row.dialogue_admission_key()?.map(|key| (receipt, key)))
    }

    pub(crate) fn digest(carrier: &AcceptedCarrier) -> Result<String, Error> {
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&(
                &carrier.source,
                &carrier.body,
                &carrier.correspondent,
                &carrier.speaker
            ))?)
        ))
    }
    pub fn validate(&self) -> Result<(), Error> {
        crate::budget::check(
            crate::LimitKind::InboxEntries,
            self.pending.len(),
            MAX_CARRIERS,
        )?;
        crate::budget::check(
            crate::LimitKind::InboxEntries,
            self.receipts.len(),
            MAX_RECEIPTS,
        )?;
        let mut bytes = 0usize;
        let mut ordinals = std::collections::BTreeSet::new();
        let mut operations = std::collections::BTreeSet::new();
        let mut receipt_ordinals = std::collections::BTreeSet::new();
        let mut dialogue_ids = std::collections::BTreeSet::new();
        let mut dialogue_aliases = std::collections::BTreeSet::new();
        for receipt in &self.receipts {
            validate_operation(&receipt.receipt.operation_id)?;
            if let Some(dialogue) = &receipt.dialogue {
                dialogue.validate(&receipt.receipt.operation_id)?;
                if !dialogue_ids.insert(dialogue.canonical_id)
                    || !dialogue_aliases.insert(&dialogue.alias)
                {
                    return Err(Error::config(
                        "duplicate external dialogue identity or alias",
                    ));
                }
            }
            if receipt.receipt.status == DeliveryStatus::Queued
                && !self
                    .pending
                    .iter()
                    .any(|carrier| carrier.operation_id == receipt.receipt.operation_id)
            {
                return Err(Error::config("queued receipt has no retained carrier"));
            }
            if receipt.receipt.ordinal == 0
                || receipt.receipt.ordinal > self.next_ordinal
                || !operations.insert(&receipt.receipt.operation_id)
                || !receipt_ordinals.insert(receipt.receipt.ordinal)
                || receipt.digest.len() != 64
                || !receipt.digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(Error::config("invalid durable inbox receipt"));
            }
        }
        for carrier in &self.pending {
            validate_body(&carrier.body)?;
            if !ordinals.insert(carrier.ordinal) {
                return Err(Error::config("duplicate carrier ordinal"));
            }
            let receipt = self
                .receipts
                .iter()
                .find(|row| row.receipt.operation_id == carrier.operation_id)
                .ok_or_else(|| Error::config("carrier has no durable receipt"))?;
            if receipt.receipt.ordinal != carrier.ordinal
                || receipt.receipt.status != DeliveryStatus::Queued
                || receipt.digest != Self::digest(carrier)?
                || receipt.dialogue.as_ref().is_some_and(|dialogue| {
                    !matches!(&carrier.source, UserSource::External { identity } if identity == &dialogue.identity)
                })
            {
                return Err(Error::config("carrier receipt witness mismatch"));
            }
            match &carrier.source {
                UserSource::Agent { agent_id, .. } => {
                    if carrier
                        .correspondent
                        .as_ref()
                        .is_none_or(|row| row.agent_id.as_str() != agent_id)
                        || carrier.speaker.is_some()
                    {
                        return Err(Error::config(
                            "agent carrier lacks authenticated correspondent witness",
                        ));
                    }
                }
                UserSource::External { identity } => {
                    let speaker = carrier.speaker.as_ref().ok_or_else(|| {
                        Error::config("external carrier lacks authenticated speaker witness")
                    })?;
                    speaker.validate()?;
                    if &speaker.identity != identity || carrier.correspondent.is_some() {
                        return Err(Error::config("external carrier speaker witness mismatch"));
                    }
                }
                UserSource::Framework => {
                    return Err(Error::config(
                        "framework notices retain their execution owner",
                    ));
                }
            }
            bytes = bytes.checked_add(carrier.body.len()).ok_or_else(|| {
                crate::budget::reject(crate::LimitKind::Arithmetic, "carrier byte overflow")
            })?;
            crate::budget::check(crate::LimitKind::InboxBytes, bytes, MAX_CARRIER_BYTES)?;
        }
        Ok(())
    }
}
pub(crate) fn validate_operation(id: &str) -> Result<(), Error> {
    if id.is_empty() || id.len() > 128 || id.chars().any(|character| character.is_control()) {
        return Err(Error::config(
            "carrier operation id must contain 1..128 non-control bytes",
        ));
    }
    Ok(())
}
pub(crate) fn validate_body(body: &str) -> Result<(), Error> {
    if body.trim().is_empty() {
        return Err(Error::config("carrier body must not be blank"));
    }
    crate::budget::check(crate::LimitKind::InputBytes, body.len(), MAX_BODY_BYTES)
}

#[cfg(test)]
mod tests;
