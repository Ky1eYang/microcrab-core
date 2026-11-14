//! ToolContext adapters into the authoritative invocation and producer owners.
use crate::{
    Error,
    runtime::tool::{ToolContext, ToolOutput},
};
use std::sync::Arc;
impl ToolContext {
    pub fn ensure_detached_execution(&mut self) -> Result<(), Error> {
        if self.execution_id.is_some() {
            return Ok(());
        }
        if self.state.is_attached() {
            return Err(Error::tool(
                "attached tool invocation requires an execution owner",
            ));
        }
        // Direct contexts may share an embedding caller token. Each execution
        // owns a child so cancelling it cannot cancel its caller or siblings.
        let cancellation = self.cancellation.child_token();
        let mut row = crate::runtime::execution::ExecutionSnapshot::new(
            self.session_id.clone(),
            self.call_id.clone(),
            self.tool.clone(),
            self.agent.clone(),
            self.mode.clone(),
        );
        row.subject = self.subject.clone();
        let execution_id = self
            .state
            .executions()
            .admit_detached(row, cancellation.clone())?;
        self.execution_id = Some(execution_id);
        self.cancellation = cancellation;
        Ok(())
    }
    pub async fn complete_detached_output(&self, output: &ToolOutput) -> Result<(), Error> {
        if self.state.is_attached() {
            return Err(Error::tool("attached executions are completed by Runtime"));
        }
        let id = self
            .execution_id
            .ok_or_else(|| Error::tool("tool invocation has no execution owner"))?;
        if let ToolOutput::Final(final_output) = output {
            self.state
                .executions()
                .complete_detached(id, final_output.clone())
                .await?;
        }
        Ok(())
    }
    pub async fn fail_detached_execution(&self, error: &Error) -> Result<(), Error> {
        if self.state.is_attached() {
            return Err(Error::tool("attached executions are completed by Runtime"));
        }
        let id = self
            .execution_id
            .ok_or_else(|| Error::tool("tool invocation has no execution owner"))?;
        self.state.executions().finish_error(id, error).await;
        Ok(())
    }
    pub fn open_output(
        &self,
        options: crate::runtime::execution::OutputOptions,
        controller: Arc<dyn crate::runtime::execution::ExecutionController>,
    ) -> Result<
        (
            crate::runtime::execution::OutputWriter,
            crate::runtime::execution::BufferedOutput,
        ),
        Error,
    > {
        let id = self
            .execution_id
            .ok_or_else(|| Error::tool("output requires an admitted execution"))?;
        self.state.executions().open_output(id, options, controller)
    }
    pub async fn attach_process_output(
        &self,
        writer: crate::runtime::execution::OutputWriter,
        handle: microcrab_host_api::process::ProcessHandle,
        channels: Vec<String>,
        captured: bool,
    ) -> Result<Arc<dyn crate::runtime::execution::ExecutionController>, Error> {
        if !self
            .execution_id
            .is_some_and(|id| writer.belongs_to(&self.state.executions(), id))
            || !self.processes.owns_process(handle)
        {
            return Err(Error::tool("process output is not owned by this execution"));
        }
        self.processes
            .registry()
            .attach_output(&self.session_id, handle, channels, captured, writer)
            .await
    }
    pub async fn adopt_process_output(
        &self,
        handle: microcrab_host_api::process::ProcessHandle,
        channels: Vec<String>,
        captured: bool,
    ) -> Result<crate::runtime::execution::BufferedOutput, Error> {
        if !self.processes.owns_process(handle) {
            return Err(Error::tool("process output is not owned by this execution"));
        }
        let id = self
            .execution_id
            .ok_or_else(|| Error::tool("output requires an admitted execution"))?;
        self.processes
            .registry()
            .adopt_output(
                &self.session_id,
                handle,
                channels,
                captured,
                self.state.executions(),
                id,
            )
            .await
    }
}
