//! Live configuration mutates only its actual owners at an idle boundary.
use super::*;
impl SessionInner {
    pub(crate) fn auto_compaction_policy(&self) -> Option<crate::context::AutoCompactionPolicy> {
        self.auto_compaction
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .map(|(policy, _)| policy.clone())
    }

    pub(crate) async fn apply_limits(self: &Arc<Self>, desired: Limits) -> Result<(), Error> {
        self.ensure_open()?;
        desired.validate()?;
        let _foreground = self
            .foreground
            .clone()
            .try_lock_owned()
            .map_err(|_| Error::Busy)?;
        let _operation = self
            .operation
            .clone()
            .try_lock_owned()
            .map_err(|_| Error::Busy)?;
        let _fence = self.endpoint.fence().await?;
        self.ensure_open()?;
        if self.has_continuation() || self.state.executions().has_pending() {
            return Err(Error::Busy);
        }
        let old = self.state().limits;
        let mut safe = old.clone();
        safe.max_rounds = desired.max_rounds;
        safe.max_tool_calls_per_round = desired.max_tool_calls_per_round;
        safe.max_output_tokens = desired.max_output_tokens;
        safe.interaction_timeout_ms = desired.interaction_timeout_ms;
        safe.lifecycle_timeout_ms = desired.lifecycle_timeout_ms;
        if safe != desired {
            return Err(Error::config(
                "context, tool-output, process, execution, retained-output and sleep limits require an idle Session rebuild; current owners retain their admitted capacities",
            ));
        }
        *self
            .configured_limits
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = desired.clone();
        self.state.update(|state| {
            state.limits = desired;
            state.clear_historical_limits();
        });
        self.state.refresh_preview(Boundary::SessionRestored)
    }
    pub(crate) async fn set_auto_compaction(
        self: &Arc<Self>,
        policy: crate::context::AutoCompactionPolicy,
    ) -> Result<(), Error> {
        self.ensure_open()?;
        policy.validate()?;
        let _foreground = self
            .foreground
            .clone()
            .try_lock_owned()
            .map_err(|_| Error::Busy)?;
        let _operation = self
            .operation
            .clone()
            .try_lock_owned()
            .map_err(|_| Error::Busy)?;
        self.ensure_open()?;
        if self.has_continuation() {
            return Err(Error::Busy);
        }
        let mut configured = self
            .auto_compaction
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        match configured.as_mut() {
            Some((current, _)) => {
                *current = policy;
                Ok(())
            }
            None if !policy.enabled => Ok(()),
            None => Err(Error::config(
                "automatic compaction requires an assembled route-aware token estimator; rebuild the idle Session",
            )),
        }
    }
    pub(crate) async fn replace_markdown_references(
        self: &Arc<Self>,
        kind: String,
        sources: Vec<crate::state::reference::ReferenceSource>,
        limits: crate::state::reference::ReferenceLimits,
    ) -> Result<(), Error> {
        self.ensure_open()?;
        let _foreground = self
            .foreground
            .clone()
            .try_lock_owned()
            .map_err(|_| Error::Busy)?;
        let _operation = self
            .operation
            .clone()
            .try_lock_owned()
            .map_err(|_| Error::Busy)?;
        self.ensure_open()?;
        let reference = crate::DynReference::new(
            crate::state::reference::MarkdownReferences::new(kind, sources, self.host.clone())
                .limits(limits),
        )?;
        let token = CancellationToken::new();
        self.register_command_token(&token)?;
        let _active = TurnGuard {
            inner: self.clone(),
            clear: false,
        };
        let _cancel = CancelOnDrop(token.clone());
        tokio::select! { biased;
            _=token.cancelled()=>return Err(Error::Cancelled),
            result=tokio::time::timeout(std::time::Duration::from_millis(self.state().limits.lifecycle_timeout_ms),reference.load())=>result.map_err(|_|Error::lifecycle("reference loading deadline"))??,
        }
        let _fence = self.endpoint.fence().await?;
        self.ensure_open()?;
        self.state.replace_reference(reference)
    }

    pub(crate) async fn refresh_reference_library(self: &Arc<Self>) -> Result<(), Error> {
        self.ensure_open()?;
        let _foreground = self
            .foreground
            .clone()
            .try_lock_owned()
            .map_err(|_| Error::Busy)?;
        let _operation = self
            .operation
            .clone()
            .try_lock_owned()
            .map_err(|_| Error::Busy)?;
        self.ensure_open()?;
        let token = CancellationToken::new();
        self.register_command_token(&token)?;
        let _active = TurnGuard {
            inner: self.clone(),
            clear: false,
        };
        let _cancel = CancelOnDrop(token.clone());
        tokio::select! { biased;
            _ = token.cancelled() => return Err(Error::Cancelled),
            result = tokio::time::timeout(
                std::time::Duration::from_millis(self.state().limits.lifecycle_timeout_ms),
                self.state.refresh_library(token.clone()),
            ) => result.map_err(|_| Error::lifecycle("reference loading deadline"))??,
        }
        let _fence = self.endpoint.fence().await?;
        self.ensure_open()?;
        Ok(())
    }
}
