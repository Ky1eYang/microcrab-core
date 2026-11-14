//! Reconciliation retains authenticated transport and settles original intents without replay.
mod automatic;
use super::*;
use microcrab_host_protocol::{ErrorCode, HostRequest, ReceiptState, WireError};
use serde_json::{Value, json};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Settlement {
    Explicit,
    KnownOnly,
}

struct Reconciling<'a> {
    host: &'a RemoteHost,
    generation: u64,
    pass: u64,
}
impl Drop for Reconciling<'_> {
    fn drop(&mut self) {
        self.host.abandon_reconciliation(self.generation, self.pass);
        if self.host.call_recovery_requested(self.generation) {
            self.host.recover_abandoned_call(self.generation);
        }
    }
}
impl RemoteHost {
    // Local validation still has an attributable protocol cause. Record it
    // before returning ToolError; never fabricate an unavailable request ID.
    pub(super) fn reconciliation_validation_error(
        &self,
        generation: u64,
        code: ErrorCode,
        message: &'static str,
        operation_id: Option<&str>,
    ) -> ToolError {
        self.mark_unready(
            generation,
            readiness::Cause::new(WireError::new(code, message), operation_id, None),
        );
        unavailable(message)
    }
    pub fn operations(&self, limit: usize) -> Vec<Value> {
        let maximum = limit.min(100);
        let mut items: Vec<Value> = self.unresolved.lock().unwrap_or_else(|e| e.into_inner())
            .iter().take(maximum).map(|(id,digest)| json!({"operation_id":id,"payload_digest":digest,"state":"indeterminate"})).collect();
        for (id, entry) in self
            .processes
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
        {
            if items.len() >= maximum {
                break;
            }
            if entry.release_pending {
                items.push(json!({"kind":"process_cleanup", "handle":id, "pid":entry.pid, "state":"cleanup_pending"}));
            }
        }
        for (operation_id, start) in self
            .processes
            .starts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
        {
            if items.len() >= maximum {
                break;
            }
            if start.cancelled
                && start.receipt.is_none()
                && !items
                    .iter()
                    .any(|item| item["operation_id"] == *operation_id)
            {
                items.push(json!({"operation_id":operation_id, "state":"cancelled_start_cleanup_pending", "effects_may_have_occurred":true}));
            }
        }
        let history = self
            .operation_history
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for item in history.iter().rev() {
            if items.len() >= maximum {
                break;
            }
            if !items
                .iter()
                .any(|current| current["operation_id"] == item["operation_id"])
            {
                items.push(item.clone());
            }
        }
        items
    }
    pub(super) fn remember_receipt(&self, receipt: &OperationReceipt) {
        let mut item =
            json!({"operation_id":receipt.operation_id,"payload_digest":receipt.payload_digest});
        match &receipt.state {
            ReceiptState::Intent => item["state"] = json!("intent"),
            ReceiptState::Indeterminate => item["state"] = json!("indeterminate"),
            ReceiptState::Completed(result) => {
                item["state"] = json!("completed");
                if let Err(reason) = result {
                    item["reason"] = json!(reason);
                }
            }
            ReceiptState::FailedUnknown {
                reason,
                effects_may_have_occurred,
            } => {
                item["state"] = json!("failed_unknown");
                item["reason"] = json!(reason);
                item["effects_may_have_occurred"] = json!(effects_may_have_occurred);
            }
        }
        let mut history = self
            .operation_history
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        history.retain(|old| old["operation_id"] != item["operation_id"]);
        history.push_back(item);
        while history.len() > 100 {
            history.pop_front();
        }
    }
    pub(super) fn forget_terminal(
        &self,
        receipt: &OperationReceipt,
        generation: u64,
    ) -> Result<(), ToolError> {
        if let ReceiptState::Completed(result) = &receipt.state {
            self.observe_start_reply(&receipt.operation_id, result);
        }
        self.remember_receipt(receipt);
        if !matches!(
            receipt.state,
            ReceiptState::Completed(_) | ReceiptState::FailedUnknown { .. }
        ) {
            return Err(self.reconciliation_validation_error(
                generation,
                ErrorCode::OutcomeUnknown,
                "Host original worker remains active; execution is fenced",
                Some(&receipt.operation_id),
            ));
        }
        let mut unresolved = self.unresolved.lock().unwrap_or_else(|e| e.into_inner());
        if unresolved.get(&receipt.operation_id) == Some(&receipt.payload_digest) {
            unresolved.remove(&receipt.operation_id);
            self.forget_consumption(&receipt.operation_id);
        }
        Ok(())
    }
    async fn settle(
        &self,
        operation_id: String,
        payload_digest: String,
        generation: u64,
    ) -> Result<(), ToolError> {
        let scope = self
            .scope
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or_else(|| {
                self.reconciliation_validation_error(
                    generation,
                    ErrorCode::BindingMismatch,
                    "Host scope is unavailable",
                    Some(&operation_id),
                )
            })?;
        let value = self
            .call_for_generation(
                HostRequest::SettleUnknown {
                    operation_id: operation_id.clone(),
                    payload_digest: payload_digest.clone(),
                    scope,
                },
                CancellationToken::new(),
                Some(generation),
            )
            .await?;
        let receipt: OperationReceipt = serde_json::from_value(value).map_err(|_| {
            self.reconciliation_validation_error(
                generation,
                ErrorCode::InvalidRequest,
                "invalid Host settlement receipt",
                Some(&operation_id),
            )
        })?;
        if receipt.operation_id != operation_id
            || receipt.payload_digest != payload_digest
            || !self.valid_receipt_scope(&receipt)
        {
            return Err(self.reconciliation_validation_error(
                generation,
                ErrorCode::OperationConflict,
                "Host settlement receipt does not match original operation",
                Some(&operation_id),
            ));
        }
        self.forget_terminal(&receipt, generation)
    }
    pub async fn reconcile(&self) -> Result<(), ToolError> {
        self.reconcile_for_generation(self.generation.load(Ordering::SeqCst))
            .await
    }
    pub async fn reconcile_for_generation(&self, generation: u64) -> Result<(), ToolError> {
        let _gate = self.reconciliation_gate.lock().await;
        let (pass, revision) = self.begin_reconciliation(generation)?;
        let _activity = Reconciling {
            host: self,
            generation,
            pass,
        };
        let result = match tokio::time::timeout(
            std::time::Duration::from_secs(15),
            self.reconcile_outcomes(generation, Settlement::Explicit),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                let message =
                    "Host reconciliation deadline exceeded; original ownership remains fenced";
                self.mark_unready(
                    generation,
                    readiness::Cause::phase(readiness::LocalCode::ReconciliationTimeout, message),
                );
                Err(unavailable(message))
            }
        };
        let published_ready = self.finish_reconciliation(generation, pass, revision, &result);
        if result.is_ok() && !published_ready {
            return Err(unavailable(
                "Host route or readiness cause changed during reconciliation",
            ));
        }
        if published_ready {
            self.processes.notify_route_change();
        }
        result
    }
    async fn reconcile_outcomes(
        &self,
        generation: u64,
        settlement: Settlement,
    ) -> Result<(), ToolError> {
        let operations = self
            .unresolved
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        for (operation_id, digest) in operations {
            if settlement == Settlement::KnownOnly {
                self.await_terminal_receipt(&operation_id, &digest, generation)
                    .await?;
                continue;
            }
            match self
                .status_for_generation(&operation_id, Some(generation))
                .await?
            {
                Some(receipt)
                    if receipt.operation_id == operation_id
                        && receipt.payload_digest == digest
                        && self.valid_receipt_scope(&receipt) =>
                {
                    if matches!(
                        receipt.state,
                        ReceiptState::Completed(_) | ReceiptState::FailedUnknown { .. }
                    ) {
                        self.forget_terminal(&receipt, generation)?;
                    } else {
                        self.settle(operation_id, digest, generation).await?;
                    }
                }
                None => self.settle(operation_id, digest, generation).await?,
                Some(foreign) => {
                    self.isolated_receipts
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .insert(foreign.operation_id.clone(), foreign.payload_digest.clone());
                    // Retire only our stale correlation; never alter the foreign durable record.
                    let scope = self
                        .scope
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .clone()
                        .ok_or_else(|| {
                            self.reconciliation_validation_error(
                                generation,
                                ErrorCode::BindingMismatch,
                                "Host scope is unavailable",
                                Some(&operation_id),
                            )
                        })?;
                    self.forget_terminal(&OperationReceipt { operation_id, payload_digest: digest, scope,
                        state: ReceiptState::FailedUnknown { reason: WireError::new(ErrorCode::OperationConflict,
                            "Original receipt scope or payload differs; foreign receipt was left unchanged"), effects_may_have_occurred: true } }, generation)?;
                }
            }
        }
        let value = self
            .call_for_generation(
                HostRequest::Recovery,
                CancellationToken::new(),
                Some(generation),
            )
            .await?;
        let receipts: Vec<OperationReceipt> = serde_json::from_value(value).map_err(|_| {
            self.reconciliation_validation_error(
                generation,
                ErrorCode::InvalidRequest,
                "invalid Host recovery receipts",
                None,
            )
        })?;
        for receipt in receipts {
            if !self.valid_receipt_scope(&receipt)
                || self
                    .isolated_receipts
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(&receipt.operation_id)
                    == Some(&receipt.payload_digest)
            {
                continue;
            }
            if matches!(
                receipt.state,
                ReceiptState::Completed(_) | ReceiptState::FailedUnknown { .. }
            ) {
                self.forget_terminal(&receipt, generation)?;
            } else if settlement == Settlement::KnownOnly {
                self.await_terminal_receipt(
                    &receipt.operation_id,
                    &receipt.payload_digest,
                    generation,
                )
                .await?;
            } else {
                self.settle(receipt.operation_id, receipt.payload_digest, generation)
                    .await?;
            }
        }
        self.reconcile_process_inventory(generation).await?;
        self.cleanup_releases_for_generation(Some(generation))
            .await?;
        Ok(())
    }
}
