//! Output consumption retains exact receipts without owning Host-wide readiness.
//! One bounded observer per abandoned operation never reissues its payload or
//! settles missing/nonterminal evidence. The same output stays fenced until proof.
use super::*;
use microcrab_host_protocol::ReceiptState;

impl RemoteHost {
    pub(super) fn is_consumption(&self, operation: &str) -> bool {
        self.consumption
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(operation)
    }
    pub(super) fn forget_consumption(&self, operation: &str) {
        self.consumption
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(operation);
    }
    pub(super) fn recover_consumption(&self, operation: String, generation: u64) {
        let Some(host) = self.weak_self.upgrade() else {
            return;
        };
        let Some(digest) = self
            .unresolved
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&operation)
            .cloned()
        else {
            return;
        };
        if !self
            .consumption_recovery
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(operation.clone())
        {
            return;
        }
        tokio::spawn(async move {
            let observed = tokio::time::timeout(std::time::Duration::from_secs(15), async {
                loop {
                    let receipt = host.status_for_generation(&operation, Some(generation)).await?.ok_or_else(|| unavailable("Original output receipt is missing; no automatic replay"))?;
                    if receipt.operation_id != operation || receipt.payload_digest != digest || !host.valid_receipt_scope(&receipt) {
                        return Err(unavailable("Original output receipt identity or scope differs; no automatic replay"));
                    }
                    if matches!(receipt.state, ReceiptState::Completed(_) | ReceiptState::FailedUnknown { .. }) {
                        let route = host.route.lock().unwrap_or_else(|e| e.into_inner());
                        if !route.as_ref().is_some_and(|route| route.generation == generation) {
                            return Err(unavailable("Host route changed before output receipt observation"));
                        }
                        return host.forget_terminal(&receipt, generation);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }).await;
            // Failure keeps the original operation/handle fence and cleanup owner.
            // A correlated late response or explicit refresh can observe it again.
            if !matches!(observed, Ok(Ok(()))) {
                tracing::debug!(
                    generation,
                    "Output receipt remains operation scoped and unresolved"
                );
            }
            host.consumption_recovery
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&operation);
        });
    }
}
