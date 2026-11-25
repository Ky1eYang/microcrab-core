// Cleanup retains primary and subordinate owners until each exact receipt is
// acknowledged. Dropping a waiter never cancels or erases the cleanup worker.
impl ExecutionRegistry {
    fn request_cleanup(
        &self,
        id: ExecutionId,
    ) -> Result<
        watch::Receiver<Option<Result<(), crate::observation::diagnostics::ToolDiagnostic>>>,
        Error,
    > {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let entry = state
            .entries
            .get_mut(&id)
            .ok_or_else(|| Error::tool("unknown execution handle"))?;
        if let Some(receiver) = &entry.cleanup
            && receiver.borrow().is_none()
            && receiver.has_changed().is_ok()
        {
            return Ok(receiver.clone());
        }
        let primary = entry.controller.clone();
        let subordinate = entry.subordinate_cleanups.clone();
        if primary.is_none() && subordinate.is_empty() {
            if !entry.adopted
                && let Some(buffer) = &mut entry.buffer
                && buffer.is_private()
            {
                buffer.clear_private();
                entry.released = true;
            }
            let (_, receiver) = watch::channel(Some(Ok(())));
            return Ok(receiver);
        }
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| Error::lifecycle("execution cleanup needs the embedding async runtime"))?;
        let (sender, receiver) = watch::channel(None);
        entry.cleanup = Some(receiver.clone());
        let redaction = entry.redaction.clone();
        let weak = self.self_ref.clone();
        let timeout = std::time::Duration::from_millis(self.limits.lifecycle_timeout_ms);
        runtime.spawn(async move {
            let mut owners: Vec<Arc<dyn ExecutionController>> = Vec::new();
            for owner in primary.iter().chain(subordinate.iter()) {
                if !owners.iter().any(|current| Arc::ptr_eq(current, owner)) {
                    owners.push(owner.clone());
                }
            }
            let outcomes = futures_util::future::join_all(owners.iter().map(|owner| {
                let redaction = redaction.clone();
                async move {
                    match tokio::time::timeout(
                        timeout,
                        AssertUnwindSafe(owner.release(CancellationToken::new())).catch_unwind(),
                    )
                    .await
                    {
                        Ok(Ok(Ok(()))) => Ok(()),
                        Ok(Ok(Err(error))) => {
                            Err(redaction.redact_diagnostic(error.tool_diagnostic()))
                        }
                        Ok(Err(_)) => {
                            Err(Error::lifecycle("execution controller cleanup panicked")
                                .tool_diagnostic())
                        }
                        Err(_) => Err(Error::lifecycle(
                            "execution controller cleanup timed out before acknowledgment",
                        )
                        .tool_diagnostic()),
                    }
                }
            }))
            .await;
            let outcome = outcomes
                .iter()
                .find_map(|result| result.as_ref().err().cloned())
                .map_or(Ok(()), Err);
            if let Some(registry) = weak.upgrade() {
                let mut state = registry
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if let Some(entry) = state.entries.get_mut(&id) {
                    for (owner, result) in owners.iter().zip(&outcomes) {
                        if result.is_ok() {
                            if entry
                                .controller
                                .as_ref()
                                .is_some_and(|current| Arc::ptr_eq(current, owner))
                            {
                                entry.controller = None;
                            }
                            entry
                                .subordinate_cleanups
                                .retain(|current| !Arc::ptr_eq(current, owner));
                        }
                    }
                    if outcome.is_ok()
                        && !entry.adopted
                        && let Some(buffer) = &mut entry.buffer
                        && buffer.is_private()
                    {
                        buffer.clear_private();
                        entry.released = true;
                    }
                    if let Err(diagnostic) = &outcome {
                        entry.row.status = ExecutionStatus::Interrupted;
                        entry.row.failure = None;
                        if !entry.row.diagnostic.as_ref().is_some_and(|prior| {
                            prior.code == crate::observation::diagnostics::DiagnosticCode::WasmTrap
                        }) {
                            entry.row.diagnostic = Some(diagnostic.clone());
                        }
                        if let Some(buffer) = &mut entry.buffer {
                            if let Err(error) = buffer.flush_redaction() {
                                entry.row.diagnostic = Some(error.tool_diagnostic());
                            }
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
    pub(crate) async fn cleanup_controller(&self, id: ExecutionId) -> Result<(), Error> {
        Self::wait_for_cleanup(self.request_cleanup(id)?).await
    }
    async fn wait_for_cleanup(
        mut completion: watch::Receiver<
            Option<Result<(), crate::observation::diagnostics::ToolDiagnostic>>,
        >,
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
