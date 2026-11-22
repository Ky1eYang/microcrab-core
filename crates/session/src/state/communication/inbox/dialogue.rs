//! Exact external admission identity, kept beside the canonical operation receipt.
use super::{AcceptedCarrier, DurableInbox, QueuedReceipt};
use crate::Error;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Caller-authenticated, opaque identifier for one credential incarnation.
/// The application must supply independent, stable random bytes, never a bearer
/// token, a display fingerprint, or an identifier reused on credential replacement.
#[derive(Clone)]
pub struct ExternalCredentialIncarnation([u8; 32]);

impl ExternalCredentialIncarnation {
    pub fn new(opaque_incarnation: [u8; 32]) -> Self {
        Self(opaque_incarnation)
    }

    fn witness(&self, canonical_id: Uuid) -> String {
        let mut digest = Sha256::new();
        digest.update(b"microcrab/external-dialogue-incarnation/v1\0");
        digest.update(canonical_id.as_bytes());
        digest.update(self.0);
        format!("{:x}", digest.finalize())
    }
}

/// Public admission metadata only. The eight-digit alias is not authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalDialogueReceipt {
    pub receipt: QueuedReceipt,
    pub dialogue_id: String,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DialogueAdmission {
    pub(super) canonical_id: Uuid,
    pub(super) alias: String,
    pub(super) identity: String,
    incarnation_witness: String,
}

impl std::fmt::Debug for DialogueAdmission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DialogueAdmission")
            .field("canonical_id", &self.canonical_id)
            .field("alias", &self.alias)
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

impl DialogueAdmission {
    pub(super) fn allocate(
        inbox: &DurableInbox,
        carrier: &AcceptedCarrier,
        incarnation: &ExternalCredentialIncarnation,
        mut next_id: impl FnMut() -> Uuid,
    ) -> Result<Self, Error> {
        let Some(speaker) = &carrier.speaker else {
            return Err(Error::config(
                "dialogue admission requires an external speaker",
            ));
        };
        // The caller holds the communication fence. Check all retained receipts,
        // including consumed ones; neither identity nor alias may be recycled.
        for _ in 0..32 {
            let canonical_id = next_id();
            let alias = format!("{:08x}", canonical_id.as_fields().0);
            if !inbox.receipts.iter().any(|row| {
                row.dialogue
                    .as_ref()
                    .is_some_and(|entry| entry.canonical_id == canonical_id || entry.alias == alias)
            }) && carrier.operation_id.parse::<Uuid>().ok() != Some(canonical_id)
            {
                return Ok(Self {
                    canonical_id,
                    alias,
                    identity: speaker.identity.clone(),
                    incarnation_witness: incarnation.witness(canonical_id),
                });
            }
        }
        Err(Error::config(
            "could not allocate a unique external dialogue identity",
        ))
    }

    pub(super) fn validate(&self, operation_id: &str) -> Result<(), Error> {
        use uuid::{Variant, Version};
        super::ExternalSpeaker {
            identity: self.identity.clone(),
        }
        .validate()?;
        if self.canonical_id.get_version() != Some(Version::Random)
            || self.canonical_id.get_variant() != Variant::RFC4122
            || operation_id.parse::<Uuid>().ok() == Some(self.canonical_id)
            || self.alias != format!("{:08x}", self.canonical_id.as_fields().0)
            || self.incarnation_witness.len() != 64
            || !lowercase_hex(&self.incarnation_witness)
        {
            return Err(Error::config("invalid external dialogue admission witness"));
        }
        Ok(())
    }

    pub(super) fn matches(
        &self,
        identity: &str,
        incarnation: &ExternalCredentialIncarnation,
    ) -> bool {
        self.identity == identity
            && self.incarnation_witness == incarnation.witness(self.canonical_id)
    }
}

fn lowercase_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
