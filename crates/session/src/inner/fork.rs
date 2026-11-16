//! A fork borrows a settled model prefix, never the source's execution ownership.
use super::*;
use crate::runtime::journal::Recover;

impl SessionInner {
    pub(crate) fn shares_journal(
        &self,
        journal: &Arc<dyn crate::runtime::journal::Journal>,
    ) -> bool {
        Arc::ptr_eq(&self.journal, journal)
    }

    /// Caller-owned destination assembly remains separate from this coherent read.
    /// In particular no checkpoint is committed to the source Journal here.
    pub(crate) async fn fork_recover(&self) -> Result<Recover, Error> {
        let _operation = self.reserve().await?;
        self.ensure_open()?;
        let _fence = self.endpoint.fence().await?;
        if self.has_continuation() || self.state.executions().has_pending() {
            return Err(Error::config(
                "fork requires settled invocation tasks and no suspended turn",
            ));
        }
        let context = self.context_snapshot()?;
        let mut state = self.state();
        // Preserve allocation positions and the complete frozen prefix. Old State
        // frames may describe source jobs; they are evidence, not target handles.
        state.epoch_id = context.epoch;
        state.turn_id = None;
        state.round_id = None;
        state.variables.clear_ephemeral();
        state.executions.clear();
        state.tool_tasks = None;
        let recover = Recover {
            state,
            context,
            continuation: None,
            workspace: None,
            cache_report: None,
            agent: self.agent.clone().map(Into::into),
            prompt_config: Some(self.prompt_config.clone()),
        };
        recover.validate()?;
        Ok(recover)
    }
}
