//! Compaction policy belongs to Session composition. The caller owns foreground
//! admission and a settled checkpoint; Runtime supplies only boundary facts.
use super::*;
use crate::runtime::handler::RoundRecorder;
use crate::runtime::journal::RoundContinuation;

impl SessionInner {
    pub(crate) async fn retry_auto_compaction(self: &Arc<Self>) -> Result<bool, Error> {
        self.ensure_open()?;
        let _operation = self.reserve_model().await?;
        let continuation = self
            .continuation
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if continuation
            .as_ref()
            .is_some_and(|value| value.request_prepared)
        {
            return Err(Error::config(
                "retry the prepared model request before automatic compaction",
            ));
        }
        let token = CancellationToken::new();
        self.register_command_token(&token)?;
        let _active = TurnGuard {
            inner: self.clone(),
            clear: false,
        };
        let _cancel = CancelOnDrop(token.clone());
        self.initialize(&token).await?;
        let mut context = self
            .context
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let before = context.baseline_generation();
        context.clear_auto_compaction_attempt();
        self.auto_compact_settled(
            &mut context,
            continuation.clone(),
            continuation.is_none(),
            &token,
        )
        .await?;
        let changed = context.baseline_generation() > before;
        if changed && continuation.is_none() {
            // This command replaces a settled inter-Turn baseline without a new
            // epoch. Keep the new certified baseline eligible for the next input.
            self.persist_inter_turn(&context).await?;
        }
        Ok(changed)
    }

    pub(crate) async fn auto_compact_settled(
        &self,
        context: &mut Context,
        continuation: Option<RoundContinuation>,
        final_round: bool,
        cancellation: &CancellationToken,
    ) -> Result<(), Error> {
        let configured = self
            .auto_compaction
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let Some((policy, estimator)) = configured else {
            return Ok(());
        };
        if !policy.enabled || context.pending_epoch() {
            return Ok(());
        }
        let issued = context.content(self.state().limits.max_output_tokens)?;
        let Some(estimate) =
            context.estimate_tokens(estimator.as_ref(), issued.max_output_tokens)?
        else {
            tracing::debug!(
                reason = "token_measurement_or_model_window_unavailable",
                "automatic compaction skipped"
            );
            return Ok(());
        };
        let Some(pressure) = estimate.pressure(issued.max_output_tokens, policy.headroom_tokens)
        else {
            return Ok(());
        };
        let threshold = if final_round {
            policy.turn_threshold
        } else {
            policy.round_threshold
        };
        if pressure < threshold {
            return Ok(());
        }
        let measured = context.measure()?;
        let Some(request_fingerprint) = context.usage_request_fingerprint() else {
            return Ok(());
        };
        use sha2::{Digest, Sha256};
        let witness = format!(
            "usage:{:x}",
            Sha256::digest(serde_json::to_vec(&(&estimate.route, request_fingerprint))?)
        );
        if context.auto_compaction_attempt() == Some(witness.as_str()) {
            return if pressure >= 1.0 {
                Err(Error::Provider("automatic compaction already attempted; retained checkpoint; compact manually before continuing".into()))
            } else {
                Ok(())
            };
        }
        context.mark_auto_compaction_attempt(witness.clone());
        self.auto_compaction_interrupted
            .store(continuation.is_some(), Ordering::SeqCst);
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        // Preserve exact selection and continuation before the helper. This does
        // not advance rounds, renew budgets, reset execution or admit inbox input.
        self.persist(context, continuation.clone()).await?;
        self.observe_context(context)?;
        let helper_issuer = self.functional_issuer(issued.clone());
        let generated = subagents::auto_compact::run(
            self.one_shots.as_ref(),
            self.client.clone(),
            self.auditor.inner.clone(),
            Arc::new(self.state.clone()),
            &helper_issuer,
            cancellation,
        )
        .await;
        let response = match generated {
            Ok(response) => response,
            Err(Error::Cancelled) => {
                self.auto_compaction_interrupted
                    .store(continuation.is_some(), Ordering::SeqCst);
                return Err(Error::Cancelled);
            }
            Err(_) => {
                self.auto_compaction_interrupted
                    .store(false, Ordering::SeqCst);
                tracing::warn!(
                    reason = "helper_failed_or_invalid",
                    "automatic compaction retained original Context"
                );
                if pressure >= 1.0 && !final_round {
                    return Err(Error::Provider("automatic compaction failed; retained checkpoint; retry or compact manually before continuing".into()));
                }
                return Ok(());
            }
        };
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        self.ensure_open()?;
        // The foreground owns conversation mutation, but validate the exact local
        // candidate anyway; a stale selection must never publish a helper result.
        if context.measure()?.fingerprint != measured.fingerprint {
            return Err(Error::protocol("automatic compaction selection changed"));
        }
        let mut replacement = context.clone();
        if replacement
            .compact_baseline(response.content, &self.state.model_variables()?)
            .is_err()
        {
            self.auto_compaction_interrupted
                .store(false, Ordering::SeqCst);
            tracing::warn!(
                reason = "invalid_summary_baseline",
                "automatic compaction retained original Context"
            );
            if pressure >= 1.0 && !final_round {
                return Err(Error::Provider("automatic compaction baseline rejected; retained checkpoint; compact manually before continuing".into()));
            }
            return Ok(());
        }
        let replacement_measurement = replacement.measure()?;
        let replacement_issued = replacement.content(issued.max_output_tokens)?;
        // Shrink compares like-for-like full heuristics, never a calibrated
        // provider prefix against a summary priced by a different measurement.
        let before_heuristic = estimator.estimate(&issued)?;
        let after_heuristic = estimator.estimate(&replacement_issued)?;
        let same_capacity = |candidate: &crate::context::TokenEstimate| {
            candidate.route == estimate.route
                && candidate.context_window_tokens == estimate.context_window_tokens
        };
        let shrinking = match (&before_heuristic, &after_heuristic) {
            (Some(before), Some(after)) => {
                same_capacity(before)
                    && same_capacity(after)
                    && after.input_tokens < before.input_tokens
            }
            _ => {
                // Images have no text-token heuristic. Require exact byte shrink
                // below and revalidate current route/capacity without pricing
                // image items; do not claim a token reduction for unknown input.
                let mut probe = replacement_issued.clone();
                probe.items.clear();
                before_heuristic.as_ref().is_none_or(same_capacity)
                    && after_heuristic.as_ref().is_none_or(same_capacity)
                    && estimator
                        .estimate(&probe)?
                        .as_ref()
                        .is_some_and(same_capacity)
            }
        };
        if replacement_measurement.bytes >= measured.bytes || !shrinking {
            self.auto_compaction_interrupted
                .store(false, Ordering::SeqCst);
            tracing::warn!(
                reason = "summary_did_not_shrink",
                "automatic compaction retained original Context"
            );
            if pressure >= 1.0 && !final_round {
                return Err(Error::Provider("automatic compaction did not shrink; retained checkpoint; compact manually before continuing".into()));
            }
            return Ok(());
        }
        // persist owns uncertain-commit quarantine. Publish only acknowledged
        // bytes; cancellation during a commit is not a rollback guarantee.
        replacement.mark_auto_compaction_attempt(witness);
        replacement.acknowledge_auto_compaction();
        self.persist(&replacement, continuation).await?;
        *context = replacement;

        self.auto_compaction_interrupted
            .store(false, Ordering::SeqCst);
        self.observe_context(context)?;
        Ok(())
    }
}
