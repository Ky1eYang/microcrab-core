//! Durable source-authenticated enqueue and exact operation observation.
use super::*;
use crate::admission::inbox::IdleCarrier;
use crate::state::communication::inbox::{
    AcceptedCarrier, DeliveryStatus, DurableInbox, ExternalCredentialIncarnation,
    ExternalDialogueReceipt, MAX_RECEIPTS, OperationReceipt, QueuedReceipt,
};
use crate::state::communication::{ExternalSpeaker, SessionInteraction, turn::DialogueTurnWitness};
use microcrab_protocol::SessionInput;
use microcrab_transport::{CarrierEnvelopeVersion, UserSource};

impl SessionInner {
    /// Stage exact carrier evidence under the communication fence in the same
    /// State preview that owns input admission. Neither preview nor rendering
    /// publishes the certificate ahead of the Context checkpoint.
    pub(crate) fn stage_carrier_run(
        preview: &crate::State,
        run: &[IdleCarrier],
        turn_number: u64,
        reply_store: bool,
    ) -> Result<Vec<(SessionInput, UserSource, CarrierEnvelopeVersion)>, Error> {
        let mut snapshot = preview.snapshot();
        let mut communication = snapshot.communication.take();
        let mut admissions = Vec::new();
        let mut carriers = Vec::with_capacity(run.len());
        for carrier in run {
            let (input, source) = carrier.input()?;
            let mut envelope = CarrierEnvelopeVersion::V1;
            if let IdleCarrier::Accepted(entry) = carrier {
                let owner = communication
                    .as_mut()
                    .ok_or_else(|| Error::config("carrier admission has no communication owner"))?;
                let digest = DurableInbox::digest(entry)?;
                let receipt = owner
                    .inbox
                    .receipts
                    .iter_mut()
                    .find(|row| row.receipt.operation_id == entry.operation_id)
                    .filter(|row| {
                        row.receipt.ordinal == entry.ordinal
                            && row.receipt.status == DeliveryStatus::Queued
                            && row.digest == digest
                    })
                    .ok_or_else(|| {
                        Error::config("selected carrier differs from its exact queued admission")
                    })?;
                receipt.receipt.status = DeliveryStatus::Consumed;
                if let Some(alias) = receipt.dialogue_alias() {
                    envelope = CarrierEnvelopeVersion::ExternalDialogue {
                        alias: alias.into(),
                    };
                    admissions.push(receipt.clone());
                }
            }
            carriers.push((input, source, envelope));
        }
        let witness = if let Some(owner) = &mut communication {
            owner.inbox.pending.retain(|entry| {
                owner.inbox.receipts.iter().any(|row| {
                    row.receipt.operation_id == entry.operation_id
                        && row.receipt.status == DeliveryStatus::Queued
                })
            });
            if admissions.is_empty() {
                None
            } else {
                Some(DialogueTurnWitness::new(
                    owner.identity.agent_id.clone(),
                    snapshot.epoch_id,
                    turn_number,
                    admissions,
                    reply_store,
                )?)
            }
        } else {
            None
        };
        preview.update(|snapshot| {
            snapshot.communication = communication;
            snapshot.dialogue_turn = witness;
        });
        Ok(carriers)
    }
    pub(crate) fn remember_durable_carrier_consumption(
        &self,
        recover: &crate::runtime::journal::Recover,
    ) -> Result<(), Error> {
        let Some(communication) = &recover.state.communication else {
            return Ok(());
        };
        let mut known = self
            .durable_carrier_consumption
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        for row in communication
            .inbox
            .receipts
            .iter()
            .filter(|row| row.receipt.status == DeliveryStatus::Consumed)
        {
            if let Some(prior) = known.get(&row.receipt.operation_id) {
                if !prior.same_admission(row) {
                    return Err(Error::config(
                        "durable carrier consumption witness mismatch",
                    ));
                }
            } else {
                crate::budget::check(
                    crate::LimitKind::InboxEntries,
                    known.len().saturating_add(1),
                    MAX_RECEIPTS,
                )?;
                known.insert(row.receipt.operation_id.clone(), row.clone());
            }
        }
        Ok(())
    }
    fn apply_durable_carrier_consumption(&self, inbox: &mut DurableInbox) -> Result<bool, Error> {
        let known = self
            .durable_carrier_consumption
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut changed = false;
        for row in &mut inbox.receipts {
            if let Some(consumed) = known.get(&row.receipt.operation_id) {
                if !consumed.same_admission(row) {
                    return Err(Error::config(
                        "durable carrier consumption witness mismatch",
                    ));
                }
                if row.receipt.status != DeliveryStatus::Consumed {
                    row.receipt.status = DeliveryStatus::Consumed;
                    changed = true;
                }
            }
        }
        inbox.pending.retain(|entry| {
            inbox.receipts.iter().any(|row| {
                row.receipt.operation_id == entry.operation_id
                    && row.receipt.status == DeliveryStatus::Queued
            })
        });
        inbox.validate()?;
        Ok(changed)
    }
    async fn commit_carrier_sidecar(
        &self,
        identity: &crate::state::communication::AgentId,
        inbox: DurableInbox,
    ) -> Result<(), Error> {
        if let Err(error) = self.journal.commit_communication(identity, inbox).await {
            self.faulted.store(true, Ordering::Release);
            let mut diagnostic = error.tool_diagnostic();
            diagnostic.effects = crate::observation::diagnostics::EffectKnowledge::MayHaveOccurred;
            diagnostic.retryable = Some(false);
            return Err(Error::Diagnostic(diagnostic));
        }
        Ok(())
    }
    pub(crate) async fn flush_durable_carrier_consumption_locked(&self) -> Result<(), Error> {
        if self
            .durable_carrier_consumption
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_empty()
        {
            return Ok(());
        }
        let Some(communication) = self.state().communication else {
            return Ok(());
        };
        if !self.journal.supports_communication() {
            return Err(Error::config(
                "Journal cannot retain durable carrier consumption",
            ));
        }
        if let Some(mut durable) = self
            .journal
            .load_communication(&communication.identity.agent_id)
            .await?
            && self.apply_durable_carrier_consumption(&mut durable)?
        {
            durable.revision = durable
                .revision
                .checked_add(1)
                .ok_or_else(|| Error::config("communication revision overflow"))?;
            self.commit_carrier_sidecar(&communication.identity.agent_id, durable)
                .await?;
        }
        Ok(())
    }
    pub(crate) async fn replace_external_speakers(
        self: &Arc<Self>,
        speakers: Vec<ExternalSpeaker>,
    ) -> Result<(), Error> {
        self.ensure_model_open()?;
        self.restore_communication_inbox().await?;
        let _transaction = self.communication_transaction.lock().await;
        let mut communication = self
            .state()
            .communication
            .ok_or_else(|| Error::denied("legacy Session has no external inbox"))?;
        if communication.identity.interaction != SessionInteraction::Persistent {
            return Err(Error::denied("return Session has no inbox"));
        }
        communication.external_speakers = speakers;
        communication.validate()?;
        // Server-owned settings are the durable source of this live binding.
        self.state
            .update(|snapshot| snapshot.communication = Some(communication));
        Ok(())
    }
    pub(crate) async fn enqueue_external(
        self: &Arc<Self>,
        operation_id: String,
        identity: String,
        body: String,
    ) -> Result<QueuedReceipt, Error> {
        self.enqueue_carrier(external_carrier(operation_id, identity, body)?)
            .await
    }
    pub(crate) async fn replay_legacy_external(
        &self,
        operation_id: String,
        identity: String,
        body: String,
    ) -> Result<QueuedReceipt, Error> {
        use crate::state::communication::inbox::{validate_body, validate_operation};
        let carrier = external_carrier(operation_id, identity, body)?;
        validate_operation(&carrier.operation_id)?;
        validate_body(&carrier.body)?;
        let digest = DurableInbox::digest(&carrier)?;
        // Don't restore the inbox: that can publish reconciliation and install
        // queued carriers. Observe committed evidence without touching either.
        let _transaction = self.communication_transaction.lock().await;
        self.observe_operation_locked(&carrier.operation_id)
            .await?
            .filter(|row| row.matches_enqueue(&digest, None, None))
            .map(|row| row.receipt)
            .ok_or_else(|| Error::denied("no matching legacy external admission"))
    }
    pub(crate) async fn enqueue_authenticated_external(
        self: &Arc<Self>,
        operation_id: String,
        identity: String,
        body: String,
        incarnation: ExternalCredentialIncarnation,
    ) -> Result<ExternalDialogueReceipt, Error> {
        let carrier = external_carrier(operation_id, identity.clone(), body)?;
        self.commit_carrier(carrier, Some(incarnation.clone()))
            .await?
            .authenticated_external_receipt(&identity, &incarnation)
            .ok_or_else(|| Error::config("authenticated enqueue has no dialogue receipt"))
    }
    pub(crate) async fn enqueue_carrier(
        self: &Arc<Self>,
        carrier: AcceptedCarrier,
    ) -> Result<QueuedReceipt, Error> {
        Ok(self.commit_carrier(carrier, None).await?.receipt)
    }
    async fn commit_carrier(
        self: &Arc<Self>,
        mut carrier: AcceptedCarrier,
        incarnation: Option<ExternalCredentialIncarnation>,
    ) -> Result<OperationReceipt, Error> {
        crate::state::communication::inbox::validate_operation(&carrier.operation_id)?;
        crate::state::communication::inbox::validate_body(&carrier.body)?;
        self.ensure_model_open()?;
        if !self.journal.supports_communication() {
            return Err(Error::config(
                "Journal does not support durable communication inboxes",
            ));
        }
        self.restore_communication_inbox().await?;
        let transaction = self.communication_transaction.clone().lock_owned().await;
        let inner = self.clone();
        // Accepted/unknown outcome must outlive an HTTP caller cancellation.
        tokio::spawn(async move {
            let _transaction = transaction;
            inner.ensure_model_open()?;
            let mut communication = inner
                .state()
                .communication
                .ok_or_else(|| Error::denied("legacy Session has no communication inbox"))?;
            if communication.identity.interaction != SessionInteraction::Persistent {
                return Err(Error::denied("return Session has no communication inbox"));
            }
            let digest = DurableInbox::digest(&carrier)?;
            if let Some(prior) = communication.inbox.operation(&carrier.operation_id) {
                let identity = carrier
                    .speaker
                    .as_ref()
                    .map(|speaker| speaker.identity.as_str());
                if !prior.matches_enqueue(&digest, identity, incarnation.as_ref()) {
                    return Err(Error::config(
                        "operation id already names a different communication admission",
                    ));
                }
                return Ok(prior.clone());
            }
            match &carrier.source {
                UserSource::External { identity } => {
                    if !communication
                        .external_speakers
                        .iter()
                        .any(|row| &row.identity == identity)
                    {
                        return Err(Error::denied("external speaker is disabled or unbound"));
                    }
                }
                UserSource::Agent { agent_id, .. } => {
                    if carrier
                        .correspondent
                        .as_ref()
                        .is_none_or(|row| row.agent_id.as_str() != agent_id)
                        || !communication
                            .correspondents
                            .iter()
                            .any(|row| row.agent_id.as_str() == agent_id)
                    {
                        return Err(Error::denied(
                            "sender is not an authenticated live correspondent",
                        ));
                    }
                }
                UserSource::Framework => {
                    return Err(Error::config(
                        "framework notices cannot use external enqueue",
                    ));
                }
            }
            crate::budget::check(
                crate::LimitKind::InboxEntries,
                communication.inbox.receipts.len().saturating_add(1),
                MAX_RECEIPTS,
            )?;
            crate::budget::check(
                crate::LimitKind::InboxEntries,
                communication.inbox.pending.len().saturating_add(1),
                crate::state::communication::inbox::MAX_CARRIERS,
            )?;
            let bytes = communication
                .inbox
                .pending
                .iter()
                .map(|entry| entry.body.len())
                .sum::<usize>()
                .checked_add(carrier.body.len())
                .ok_or_else(|| {
                    crate::budget::reject(crate::LimitKind::Arithmetic, "carrier byte overflow")
                })?;
            crate::budget::check(
                crate::LimitKind::InboxBytes,
                bytes,
                crate::state::communication::inbox::MAX_CARRIER_BYTES,
            )?;
            carrier.ordinal = {
                inner
                    .inbox
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .id()?
            };
            let receipt = OperationReceipt::new(
                &communication.inbox,
                &carrier,
                digest,
                incarnation.as_ref(),
            )?;
            communication.inbox.next_ordinal =
                communication.inbox.next_ordinal.max(carrier.ordinal);
            communication.inbox.revision = communication
                .inbox
                .revision
                .checked_add(1)
                .ok_or_else(|| Error::config("communication revision overflow"))?;
            communication.inbox.pending.push(carrier);
            communication.inbox.receipts.push(receipt.clone());
            communication.validate()?;
            // Unknown outcomes fault this endpoint. Exact operation observation
            // consults durable storage; no failed mutation is replayed implicitly.
            let agent_id = communication.identity.agent_id.clone();
            inner
                .commit_carrier_sidecar(&agent_id, communication.inbox.clone())
                .await?;
            inner.state.update(|snapshot| {
                if let Some(live) = &mut snapshot.communication {
                    live.inbox = communication.inbox.clone();
                }
            });
            inner
                .inbox
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .install_carriers(
                    communication.inbox.pending,
                    communication.inbox.next_ordinal,
                );
            inner.start_inbox_dispatcher();
            inner.inbox.changed.notify_one();
            inner.publish_activity();
            Ok(receipt)
        })
        .await
        .map_err(|_| Error::tool("communication commit worker stopped unexpectedly"))?
    }
    pub(crate) async fn delivery_receipt(
        &self,
        operation_id: &str,
    ) -> Result<Option<QueuedReceipt>, Error> {
        Ok(self
            .observe_operation(operation_id)
            .await?
            .and_then(|row| row.legacy_receipt()))
    }
    pub(crate) async fn authenticated_external_receipt(
        &self,
        operation_id: &str,
        identity: &str,
        incarnation: &ExternalCredentialIncarnation,
    ) -> Result<Option<ExternalDialogueReceipt>, Error> {
        Ok(self
            .observe_operation(operation_id)
            .await?
            .and_then(|row| row.authenticated_external_receipt(identity, incarnation)))
    }
    async fn observe_operation(
        &self,
        operation_id: &str,
    ) -> Result<Option<OperationReceipt>, Error> {
        crate::state::communication::inbox::validate_operation(operation_id)?;
        self.restore_communication_inbox().await?;
        let _transaction = self.communication_transaction.lock().await;
        self.observe_operation_locked(operation_id).await
    }
    async fn observe_operation_locked(
        &self,
        operation_id: &str,
    ) -> Result<Option<OperationReceipt>, Error> {
        let identity = self
            .state()
            .communication
            .ok_or_else(|| Error::denied("legacy Session has no communication receipts"))?
            .identity;
        let durable = self.journal.load_communication(&identity.agent_id).await?;
        let stored = durable
            .as_ref()
            .and_then(|value| value.operation(operation_id));
        let selected = self
            .durable_carrier_consumption
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(operation_id)
            .cloned();
        if let Some(row) = selected.as_ref()
            && stored.is_some_and(|stored| !stored.same_admission(row))
        {
            return Err(Error::config(
                "durable carrier consumption witness mismatch",
            ));
        }
        Ok(selected.or_else(|| stored.cloned()))
    }
    pub(crate) async fn restore_communication_inbox(&self) -> Result<(), Error> {
        if self.communication_loaded.load(Ordering::Acquire) {
            return Ok(());
        }
        let _transaction = self.communication_transaction.lock().await;
        if self.communication_loaded.load(Ordering::Acquire) {
            return Ok(());
        }
        self.restore_communication_inbox_locked().await
    }
    pub(crate) async fn restore_communication_inbox_locked(&self) -> Result<(), Error> {
        let Some(mut communication) = self.state().communication else {
            return Ok(());
        };
        if !self.journal.supports_communication() {
            if !communication.inbox.pending.is_empty() {
                return Err(Error::config(
                    "Journal cannot restore accepted communication inbox",
                ));
            }
            return Ok(());
        }
        if let Some(mut durable) = self
            .journal
            .load_communication(&communication.identity.agent_id)
            .await?
        {
            durable.validate()?;
            if self.apply_durable_carrier_consumption(&mut durable)? {
                durable.revision = durable
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| Error::config("communication revision overflow"))?;
                self.commit_carrier_sidecar(&communication.identity.agent_id, durable.clone())
                    .await?;
            }
            communication.inbox = durable;
        }
        self.inbox
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .install_carriers(
                communication.inbox.pending.clone(),
                communication.inbox.next_ordinal,
            );
        self.state
            .update(|snapshot| snapshot.communication = Some(communication));
        self.communication_loaded.store(true, Ordering::Release);
        Ok(())
    }
    pub(crate) async fn acknowledge_carrier_run_locked(
        &self,
        accepted: &[String],
    ) -> Result<(), Error> {
        if accepted.is_empty() {
            return Ok(());
        }

        let mut communication = self
            .state()
            .communication
            .ok_or_else(|| Error::config("accepted carrier lost its communication owner"))?;
        for receipt in &mut communication.inbox.receipts {
            if accepted.contains(&receipt.receipt.operation_id) {
                receipt.receipt.status = DeliveryStatus::Consumed;
            }
        }
        communication
            .inbox
            .pending
            .retain(|carrier| !accepted.contains(&carrier.operation_id));
        communication.inbox.revision = communication
            .inbox
            .revision
            .checked_add(1)
            .ok_or_else(|| Error::config("communication revision overflow"))?;
        communication.inbox.validate()?;
        self.commit_carrier_sidecar(
            &communication.identity.agent_id,
            communication.inbox.clone(),
        )
        .await?;
        self.inbox
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .install_carriers(
                communication.inbox.pending.clone(),
                communication.inbox.next_ordinal,
            );
        self.state
            .update(|snapshot| snapshot.communication = Some(communication));
        Ok(())
    }
}

fn external_carrier(
    operation_id: String,
    identity: String,
    body: String,
) -> Result<AcceptedCarrier, Error> {
    let speaker = ExternalSpeaker {
        identity: identity.clone(),
    };
    speaker.validate()?;
    Ok(AcceptedCarrier {
        operation_id,
        ordinal: 0,
        body,
        source: UserSource::External { identity },
        correspondent: None,
        speaker: Some(speaker),
    })
}
