// Cleanup retains the controller in its execution until acknowledgment. A dropped
// waiter cannot cancel the cleanup worker or erase the only retryable owner.
impl ExecutionRegistry {
    fn request_cleanup(
        &self,
        id: ExecutionId,
    ) -> Result<watch::Receiver<Option<Result<(), crate::observation::diagnostics::ToolDiagnostic>>>, Error> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let entry = state
            .entries
            .get_mut(&id)
            .ok_or_else(|| Error::tool("unknown execution handle"))?;
        let Some(controller) = entry.controller.clone() else {
            let (_, receiver) = watch::channel(Some(Ok(())));
            return Ok(receiver);
        };
        // A runtime may drop a pending worker without sending an outcome. Only
        // a live acknowledgment channel still owns work that can be reused.
        if let Some(receiver) = &entry.cleanup
            && receiver.borrow().is_none()
            && receiver.has_changed().is_ok()
        {
            return Ok(receiver.clone());
        }
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| Error::lifecycle("execution cleanup needs the embedding async runtime"))?;
        let (sender, receiver) = watch::channel(None);
        entry.cleanup = Some(receiver.clone());
        let weak = self.self_ref.clone();
        let timeout = std::time::Duration::from_millis(self.limits.lifecycle_timeout_ms);
        runtime.spawn(async move {
            let result = tokio::time::timeout(
                timeout,
                AssertUnwindSafe(controller.release(CancellationToken::new())).catch_unwind(),
            )
            .await;
            let outcome = match result {
                Ok(Ok(Ok(()))) => Ok(()),
                Ok(Ok(Err(error))) => Err(error.tool_diagnostic()),
                Ok(Err(_)) => Err(Error::lifecycle("execution controller cleanup panicked").tool_diagnostic()),
                Err(_) => Err(Error::lifecycle("execution controller cleanup timed out before acknowledgment").tool_diagnostic()),
            };
            if let Some(registry) = weak.upgrade() {
                let mut state = registry
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if let Some(entry) = state.entries.get_mut(&id)
                    && entry
                        .controller
                        .as_ref()
                        .is_some_and(|current| Arc::ptr_eq(current, &controller))
                {
                    if outcome.is_ok() {
                        entry.controller = None;
                    } else {
                        entry.row.status = ExecutionStatus::Interrupted;
                        entry.row.failure = None;
                        entry.row.diagnostic = outcome.as_ref().err().cloned();
                        if let Some(buffer) = &mut entry.buffer {
                            buffer.closed = true;
                        }
                    }
                }
                drop(state);
                registry.notify();
            }
            let _ = sender.send(Some(outcome));
        });
        Ok(receiver)
    }
    async fn cleanup_controller(&self, id: ExecutionId) -> Result<(), Error> {
        Self::wait_for_cleanup(self.request_cleanup(id)?).await
    }
    async fn wait_for_cleanup(
        mut completion: watch::Receiver<Option<Result<(), crate::observation::diagnostics::ToolDiagnostic>>>,
    ) -> Result<(), Error> {
        loop {
            let observed = completion.borrow_and_update().clone();
            if let Some(result) = observed {
                return result.map_err(Error::Diagnostic);
            }
            completion
                .changed()
                .await
                .map_err(|_| Error::ExecutionInterrupted)?;
        }
    }
}
