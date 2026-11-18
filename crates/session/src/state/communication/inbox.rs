//! Durable accepted carrier and idempotency receipts. No authentication secrets.
use super::{Correspondent, ExternalSpeaker};
use crate::Error;
use microcrab_transport::UserSource;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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
                if receipt.digest != next.digest
                    || receipt.receipt.ordinal != next.receipt.ordinal
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
    pub fn receipt(&self, operation_id: &str) -> Option<QueuedReceipt> {
        self.receipts
            .iter()
            .find(|row| row.receipt.operation_id == operation_id)
            .map(|row| row.receipt.clone())
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
        for receipt in &self.receipts {
            validate_operation(&receipt.receipt.operation_id)?;
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
