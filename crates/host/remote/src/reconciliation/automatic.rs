//! Bounded, owned receipt observation after local RPC abandonment. This worker
//! never settles an unknown effect or replays its payload; explicit reconciliation
//! retains that separate authority. Readiness keeps generation/pass/cause fences.
use super::*;

impl RemoteHost {
    pub(crate) fn recover_abandoned_call(&self, generation: u64) {
        let Some(host) = self.weak_self.upgrade() else {
            return;
        };
        let Some((pass, revision)) = self.request_call_recovery(generation) else {
            return;
        };
        tokio::spawn(async move {
            let _activity = Reconciling {
                host: host.as_ref(),
                generation,
                pass,
            };
            let result = match tokio::time::timeout(std::time::Duration::from_secs(15), async {
                let _gate = host.reconciliation_gate.lock().await;
                if !host.owns_reconciliation(generation, pass) {
                    return Err(unavailable(
                        "Host recovery pass was superseded before receipt observation",
                    ));
                }
                host.reconcile_outcomes(generation, Settlement::KnownOnly)
                    .await
            })
            .await
            {
                Ok(result) => result,
                Err(_) => Err(unavailable(
                    "Host automatic receipt recovery exceeded 15 seconds; effects remain unknown",
                )),
            };
            if host.finish_call_recovery(generation, pass, revision, &result) {
                host.processes.notify_route_change();
            }
            // The activity guard observes a coalesced follow-up request on drop.
            // A failure alone never reschedules itself or clears uncertainty.
        });
    }

    pub(super) async fn await_terminal_receipt(
        &self,
        id: &str,
        digest: &str,
        generation: u64,
    ) -> Result<(), ToolError> {
        loop {
            let Some(receipt) = self.status_for_generation(id, Some(generation)).await? else {
                return Err(unavailable(
                    "Original Host receipt is missing; explicit reconciliation required; no automatic replay",
                ));
            };
            if receipt.operation_id != id
                || receipt.payload_digest != digest
                || !self.valid_receipt_scope(&receipt)
            {
                return Err(self.reconciliation_validation_error(
                    generation,
                    ErrorCode::OperationConflict,
                    "Original Host receipt scope or payload differs during automatic recovery",
                    Some(id),
                ));
            }
            if matches!(
                receipt.state,
                ReceiptState::Completed(_) | ReceiptState::FailedUnknown { .. }
            ) {
                // Receipt delivery and future resumption can straddle route
                // replacement. Commit ownership only while this route is pinned;
                // the synchronous terminal handler never awaits transport.
                let route = self.route.lock().unwrap_or_else(|e| e.into_inner());
                if !route
                    .as_ref()
                    .is_some_and(|route| route.generation == generation)
                {
                    return Err(unavailable(
                        "Host route changed before terminal receipt observation",
                    ));
                }
                return self.forget_terminal(&receipt, generation);
            }
            // Cancellation can still be joining a bridge worker. Poll its receipt,
            // never reissue a consuming read or force an active intent terminal.
            // The caller owns the whole 15-second pass, including this cadence.
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
}
