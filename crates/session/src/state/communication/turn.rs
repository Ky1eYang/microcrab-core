//! Exact evidence of a Turn consuming authenticated external operations.
use super::inbox::{DeliveryStatus, OperationReceipt};
use super::{AgentId, CommunicationState, SessionInteraction};
use crate::Error;
use crate::admission::inbox::MAX_IDLE_CARRIERS;
use serde::{Deserialize, Serialize};
use uuid::{Uuid, Variant, Version};

/// Unique admitted Turn, independent of counters that history travel can reuse.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmittedDialogueTurn {
    id: Uuid,
    number: u64,
}

impl AdmittedDialogueTurn {
    pub(crate) fn fresh(number: u64) -> Result<Self, Error> {
        let turn = Self {
            id: Uuid::new_v4(),
            number,
        };
        turn.validate()?;
        Ok(turn)
    }

    pub fn number(&self) -> u64 {
        self.number
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.number == 0
            || self.id.get_version() != Some(Version::Random)
            || self.id.get_variant() != Variant::RFC4122
        {
            return Err(Error::config("invalid admitted external dialogue Turn"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DialogueTurnWitness {
    agent_id: AgentId,
    epoch: u64,
    turn: AdmittedDialogueTurn,
    admissions: Vec<OperationReceipt>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    reply_store: bool,
}

impl DialogueTurnWitness {
    pub(crate) fn new(
        agent_id: AgentId,
        epoch: u64,
        number: u64,
        admissions: Vec<OperationReceipt>,
        reply_store: bool,
    ) -> Result<Self, Error> {
        Ok(Self {
            agent_id,
            epoch,
            turn: AdmittedDialogueTurn::fresh(number)?,
            admissions,
            reply_store,
        })
    }

    pub(crate) fn expects_reply_store(&self) -> bool {
        self.reply_store
    }

    // Canonical private handoff for the Session reply/settlement owner.
    pub(crate) fn exact_parts(
        &self,
    ) -> (&AgentId, u64, &AdmittedDialogueTurn, &[OperationReceipt]) {
        (&self.agent_id, self.epoch, &self.turn, &self.admissions)
    }

    pub(crate) fn validate(
        &self,
        communication: Option<&CommunicationState>,
        epoch: u64,
        last_turn: u64,
    ) -> Result<(), Error> {
        self.turn.validate()?;
        if self.epoch > epoch || self.admissions.is_empty() {
            return Err(Error::config(
                "invalid external dialogue consumption certificate",
            ));
        }
        crate::budget::check(
            crate::LimitKind::InboxEntries,
            self.admissions.len(),
            MAX_IDLE_CARRIERS,
        )?;
        if self.epoch == epoch && self.turn.number() > last_turn {
            return Err(Error::config(
                "invalid external dialogue consumption certificate",
            ));
        }
        let Some(communication) = communication.filter(|owner| {
            owner.identity.agent_id == self.agent_id
                && owner.identity.interaction == SessionInteraction::Persistent
                && (!self.reply_store || owner.identity.depth == 0)
        }) else {
            return Err(Error::config(
                "external dialogue consumption owner mismatch",
            ));
        };
        let mut prior_ordinal = 0;
        for admitted in &self.admissions {
            if admitted.receipt.ordinal <= prior_ordinal
                || admitted.dialogue_alias().is_none()
                || admitted.receipt.status != DeliveryStatus::Consumed
                || !communication
                    .inbox
                    .operation(&admitted.receipt.operation_id)
                    .is_some_and(|row| {
                        row.receipt.status == DeliveryStatus::Consumed
                            && row.same_admission(admitted)
                    })
            {
                return Err(Error::config(
                    "external dialogue consumption receipt mismatch",
                ));
            }
            prior_ordinal = admitted.receipt.ordinal;
        }
        Ok(())
    }
}
