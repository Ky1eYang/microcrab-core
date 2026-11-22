//! External dialogue effects belong to the exact Session's acknowledged Turn.
use super::*;
use crate::runtime::tool::{
    ToolContext, ToolOutput,
    external_dialogue::{NAME, ReplyArguments},
};
use crate::state::communication::{
    reply::{
        DialogueAdmissionKey, DialogueReplyBinding, DialogueReplyMutation, DialogueReplyStore,
        DialogueTerminalReason, DurableDialogueReply,
    },
    turn::DialogueTurnWitness,
};
use serde_json::json;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

impl SessionInner {
    pub(crate) async fn observe_external_dialogue_reply(
        &self,
        key: &DialogueAdmissionKey,
    ) -> Result<Option<DurableDialogueReply>, Error> {
        let store = self.dialogue_reply_store.lock().await;
        let Some((store, identity)) = store.as_deref().zip(self.communication_identity()) else {
            return Ok(None);
        };
        let record = store.load(&identity.agent_id, key).await?;
        if let Some(record) = &record {
            record.validate_exact(&identity.agent_id, key)?;
        }
        Ok(record)
    }

    pub(super) async fn reply_external_speaker_owned(
        &self,
        arguments: Value,
        invocation: ToolContext,
    ) -> Result<ToolOutput, Error> {
        let owner = self.self_weak.upgrade().ok_or(Error::Closed)?;
        // The generic execution timeout may drop its waiter on cancellation.
        // A started durable mutation must finish under the reply fence anyway.
        match tokio::spawn(
            async move { owner.append_external_dialogue(arguments, invocation).await },
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                self.faulted.store(true, Ordering::Release);
                Err(uncertain(Error::lifecycle(
                    "external dialogue write owner stopped unexpectedly",
                )))
            }
        }
    }

    async fn append_external_dialogue(
        &self,
        arguments: Value,
        invocation: ToolContext,
    ) -> Result<ToolOutput, Error> {
        let store = self.dialogue_reply_store.lock().await;
        self.ensure_model_open()?;
        let store = store
            .as_deref()
            .ok_or_else(|| Error::denied("external dialogue reply storage is unavailable"))?;
        let identity = self
            .communication_identity()
            .ok_or_else(|| Error::denied("Session has no external dialogue identity"))?;
        let witness = self
            .state
            .live_dialogue_turn()
            .filter(DialogueTurnWitness::expects_reply_store)
            .ok_or_else(|| Error::denied("no admitted external dialogue in the current Turn"))?;
        let (agent, epoch, turn, admissions) = witness.exact_parts();
        let snapshot = self.state();
        if self.kind != crate::SessionKind::Interactive
            || identity.depth != 0
            || identity.interaction != SessionInteraction::Persistent
            || &identity.agent_id != agent
            || !self.state.same_owner(&invocation.state)
            || invocation.session_id != self.state.processes().session_id()
            || !invocation.subject.is_tool()
            || invocation.tool != NAME
            || invocation.turn_id != Some(turn.number())
            || snapshot.epoch_id != epoch
            || invocation.round_id.is_none()
            || invocation.round_id != snapshot.round_id
            || invocation.execution_id.is_none()
            || invocation.execution_progress.is_none()
        {
            return Err(Error::denied(
                "external dialogue reply invocation does not belong to this Turn",
            ));
        }
        invocation
            .ensure_admitted_arguments(&arguments)
            .map_err(|error| error.into_runtime_error())?;
        let args = ReplyArguments::parse(arguments)?;
        let mut candidates = admissions
            .iter()
            .filter(|row| row.dialogue_alias() == Some(args.dialogue_id.as_str()));
        let admission = candidates
            .next()
            .filter(|_| candidates.next().is_none())
            .ok_or_else(|| Error::denied("dialogue id was not uniquely admitted in this Turn"))?;
        let binding = DialogueReplyBinding::from_admission(agent.clone(), admission, turn.clone())?
            .ok_or_else(|| Error::protocol("external dialogue admission lacks an exact binding"))?;
        let record = load_exact(store, &binding)
            .await?
            .ok_or_else(|| Error::tool("external dialogue reply has not opened"))?;
        if record.terminal().is_some()
            || record.chunks().last().is_some_and(|chunk| chunk.is_final())
        {
            return Err(Error::tool(
                "external dialogue no longer accepts reply chunks",
            ));
        }
        let chunk_index = record.chunks().len();
        let output = json!({"status":"committed", "dialogue_id":args.dialogue_id, "chunk_index":chunk_index, "is_final":args.is_final}).to_string();
        crate::budget::check(
            crate::LimitKind::ToolOutput,
            output.len(),
            snapshot.limits.max_tool_output_bytes,
        )?;
        // Claim this exact invocation once, immediately before the durable effect.
        invocation
            .begin_dispatch(None)
            .map_err(|error| error.into_runtime_error())?;
        let mutation = DialogueReplyMutation::Append {
            binding,
            expected_revision: record.revision(),
            effect_id: Uuid::new_v4(),
            message: args.message,
            is_final: args.is_final,
            at_ms: now_ms()?,
        };
        if let Err(error) = apply_exact(store, mutation, Some(&record)).await {
            if error.tool_diagnostic().effects
                == crate::observation::diagnostics::EffectKnowledge::MayHaveOccurred
            {
                self.faulted.store(true, Ordering::Release);
            }
            return Err(error);
        }
        Ok(ToolOutput::text(output))
    }

    /// Called only after the Context and carrier consumption have acknowledged.
    pub(in crate::inner) async fn open_external_dialogues(&self) -> Result<(), Error> {
        let store = self.dialogue_reply_store.lock().await;
        let Some((store, witness)) = store.as_deref().zip(self.state.live_dialogue_turn()) else {
            return Ok(());
        };
        if !witness.expects_reply_store() {
            return Ok(());
        }
        for binding in bindings(&witness)? {
            open(store, binding).await?;
        }
        Ok(())
    }

    /// Deauthorize before settling. The exact persisted witness remains as
    /// recovery evidence, including when the terminal write is not acknowledged.
    pub(in crate::inner) async fn settle_live_external_dialogues(
        &self,
        reason: DialogueTerminalReason,
    ) -> Result<(), Error> {
        let store = self.dialogue_reply_store.lock().await;
        let witness = self.state.live_dialogue_turn();
        self.state.update(|state| state.dialogue_turn_live = false);
        if let Some((store, witness)) = store.as_deref().zip(witness.as_ref()) {
            settle(store, witness, reason, false).await?;
        }
        Ok(())
    }

    pub(in crate::inner) async fn reconcile_recovered_dialogues(
        &self,
        witness: Option<&DialogueTurnWitness>,
    ) -> Result<(), Error> {
        let store = self.dialogue_reply_store.lock().await;
        if let Some((store, witness)) = store.as_deref().zip(witness) {
            // Recover never recreates a live Turn, emits a chunk, or overwrites a
            // terminal result. Historical evidence without reply ownership is inert.
            settle(store, witness, DialogueTerminalReason::Interrupted, true).await?;
        }
        Ok(())
    }

    pub(in crate::inner) async fn release_dialogue_reply_store(&self) {
        self.dialogue_reply_store.lock().await.take();
    }
}

fn bindings(witness: &DialogueTurnWitness) -> Result<Vec<DialogueReplyBinding>, Error> {
    let (agent, _, turn, admissions) = witness.exact_parts();
    admissions
        .iter()
        .map(|admission| {
            DialogueReplyBinding::from_admission(agent.clone(), admission, turn.clone())?
                .ok_or_else(|| Error::protocol("external dialogue witness lacks an exact binding"))
        })
        .collect()
}

async fn load_exact(
    store: &dyn DialogueReplyStore,
    binding: &DialogueReplyBinding,
) -> Result<Option<DurableDialogueReply>, Error> {
    let record = store.load(binding.agent_id(), binding.admission()).await?;
    if let Some(record) = &record {
        record.validate_exact(binding.agent_id(), binding.admission())?;
        if record.binding() != binding {
            return Err(Error::StateObservationConflict);
        }
    }
    Ok(record)
}

async fn open(
    store: &dyn DialogueReplyStore,
    binding: DialogueReplyBinding,
) -> Result<DurableDialogueReply, Error> {
    if let Some(record) = load_exact(store, &binding).await? {
        return Ok(record);
    }
    apply_exact(store, DialogueReplyMutation::Open { binding }, None).await
}

async fn settle(
    store: &dyn DialogueReplyStore,
    witness: &DialogueTurnWitness,
    reason: DialogueTerminalReason,
    recovered: bool,
) -> Result<(), Error> {
    if !witness.expects_reply_store() {
        return Ok(());
    }
    let at_ms = now_ms()?;
    let mut failure = Ok(());
    for binding in bindings(witness)? {
        // Each independent admitted dialogue gets a settlement attempt even if
        // another record fails. No operation is blindly retried.
        let result = async {
            let previous = if recovered {
                // The exact, idempotent Open also activates lazy writer adapters
                // and gives them a chance to reconcile a quarantined durable image.
                let write = store
                    .apply(DialogueReplyMutation::Open {
                        binding: binding.clone(),
                    })
                    .await?;
                let record = write.into_record();
                record.validate_exact(binding.agent_id(), binding.admission())?;
                if record.binding() != &binding {
                    return Err(uncertain(Error::protocol(
                        "external dialogue recovery acknowledgment differs",
                    )));
                }
                record
            } else {
                open(store, binding.clone()).await?
            };
            if previous.terminal().is_none() {
                apply_exact(
                    store,
                    DialogueReplyMutation::Settle {
                        binding,
                        expected_revision: previous.revision(),
                        reason,
                        at_ms,
                    },
                    Some(&previous),
                )
                .await?;
            }
            Ok(())
        }
        .await;
        failure = failure.and(result);
    }
    failure
}

async fn apply_exact(
    store: &dyn DialogueReplyStore,
    mutation: DialogueReplyMutation,
    previous: Option<&DurableDialogueReply>,
) -> Result<DurableDialogueReply, Error> {
    // Same canonical transition as the backend; bound the request and verify
    // the full acknowledgment rather than granting authority to opaque output.
    let expected = mutation.clone().apply(previous)?;
    let written = store.apply(mutation).await?;
    if written.record() != expected.record() {
        return Err(uncertain(Error::protocol(
            "external dialogue write acknowledgment differs",
        )));
    }
    Ok(written.into_record())
}

fn uncertain(error: Error) -> Error {
    use crate::observation::diagnostics::EffectKnowledge;
    let mut diagnostic = error.tool_diagnostic();
    diagnostic.effects = EffectKnowledge::MayHaveOccurred;
    diagnostic.retryable = Some(false);
    Error::Diagnostic(diagnostic)
}

fn now_ms() -> Result<u64, Error> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| u64::try_from(elapsed.as_millis()).ok())
        .ok_or_else(|| Error::lifecycle("external dialogue timestamp is out of range"))
}
