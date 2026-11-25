// Private Host source implementation shares registry ownership and admission checks.
impl ProcessRegistry {
    pub(crate) fn attach_private_output(
        self: &Arc<Self>,
        session: &str,
        handle: ProcessHandle,
        writer: crate::runtime::execution::OutputWriter,
    ) -> Result<Arc<dyn crate::runtime::execution::ExecutionController>, Error> {
        let process = self
            .owned_process(session, handle)
            .filter(|process| process.private)
            .ok_or_else(|| Error::denied("private process source is absent"))?;
        if process.bridged.swap(true, Ordering::AcqRel) {
            return Err(Error::tool("private process source already adopted"));
        }
        let mut controller =
            super::source::ProcessController::new(self.clone(), session.into(), handle, false);
        controller.private = true;
        let controller = Arc::new(controller);
        let worker_controller = controller.clone();
        tokio::spawn(async move {
            if super::source::pump_private(worker_controller, writer.clone())
                .await
                .is_err()
            {
                let _ = writer.interrupt("private capture interrupted");
            }
        });
        Ok(controller)
    }
    pub(crate) fn output_controller(
        self: &Arc<Self>,
        session: &str,
        handle: ProcessHandle,
        captured: bool,
    ) -> Result<Arc<dyn crate::runtime::execution::ExecutionController>, Error> {
        let resource = self
            .owned_process(session, handle)
            .ok_or_else(|| Error::denied("process belongs to another Session"))?;
        if resource.private || resource.capture != captured {
            return Err(Error::tool("process capture kind mismatch"));
        }
        if resource.bridged.swap(true, Ordering::AcqRel) {
            return Err(Error::tool("process output already adopted"));
        }
        Ok(Arc::new(super::source::ProcessController::new(
            self.clone(),
            session.into(),
            handle,
            captured,
        )))
    }
    pub(crate) fn source_output_limit(&self) -> usize {
        self.limits.output_buffer_bytes
    }
    pub(crate) fn source_host(&self) -> Result<Arc<dyn Host>, Error> {
        self.host
            .clone()
            .ok_or_else(|| Error::tool("Host process capability unavailable"))
    }
    pub(crate) fn source_context(&self, cancellation: CancellationToken) -> OpContext {
        self.context(cancellation)
    }
    pub(crate) async fn source_read_pipe(
        &self,
        session: &str,
        handle: ProcessHandle,
        stderr: bool,
        cancellation: CancellationToken,
    ) -> Result<Option<Bytes>, Error> {
        self.read_pipe_scoped(
            session,
            handle,
            stderr,
            self.limits.max_tool_output_bytes.min(65_536),
            cancellation,
        )
        .await
    }
    pub(crate) async fn source_release(
        &self,
        session: &str,
        handle: ProcessHandle,
        cancellation: CancellationToken,
    ) -> Result<(), Error> {
        if self.owned_process(session, handle).is_none() {
            return Ok(());
        }
        self.release_process_scoped(session, handle, cancellation)
            .await
    }
    pub(crate) async fn source_cancel(
        &self,
        session: &str,
        handle: ProcessHandle,
        cancellation: CancellationToken,
    ) -> Result<(), Error> {
        if self.owned_process(session, handle).is_none() {
            return Err(Error::denied("process belongs to another Session"));
        }
        let host = self.source_host()?;
        ProcessApi::kill_tree(host.as_ref(), &self.context(cancellation), handle, true)
            .await
            .map_err(host_error)
    }
    pub(crate) async fn source_write(
        &self,
        session: &str,
        handle: ProcessHandle,
        captured: bool,
        data: String,
        eof: bool,
        cancellation: CancellationToken,
    ) -> Result<(), Error> {
        if self.owned_process(session, handle).is_none() {
            return Err(Error::denied("process belongs to another Session"));
        }
        let host = self.source_host()?;
        let ctx = self.context(cancellation);
        if captured {
            ShellApi::write(host.as_ref(), &ctx, handle, data.into_bytes(), eof)
                .await
                .map_err(host_error)
        } else {
            ProcessApi::write_stdin(host.as_ref(), &ctx, handle, data.into_bytes().into())
                .await
                .map_err(host_error)?;
            if eof {
                ProcessApi::close_stdin(host.as_ref(), &ctx, handle)
                    .await
                    .map_err(host_error)?;
            }
            Ok(())
        }
    }
    pub(crate) async fn adopt_output(
        self: &Arc<Self>,
        session: &str,
        handle: ProcessHandle,
        channels: Vec<String>,
        captured: bool,
        executions: Arc<crate::runtime::execution::ExecutionRegistry>,
        id: crate::runtime::execution::ExecutionId,
    ) -> Result<crate::runtime::execution::BufferedOutput, Error> {
        if channels.len() != 2 || channels.iter().any(|s| s.is_empty() || s.len() > 64) {
            return Err(Error::tool(
                "process source requires two valid channel names",
            ));
        }
        let process = self
            .owned_process(session, handle)
            .ok_or_else(|| Error::denied("process belongs to another Session"))?;
        if process.private || process.capture != captured || process.bridged.load(Ordering::Acquire)
        {
            return Err(Error::tool("process output is unavailable for adoption"));
        }
        if process.bridged.swap(true, Ordering::AcqRel) {
            return Err(Error::tool("process output already adopted"));
        }
        let controller = Arc::new(super::source::ProcessController::new(
            self.clone(),
            session.into(),
            handle,
            captured,
        ));
        let (writer, output) = match executions.open_output(
            id,
            crate::runtime::execution::OutputOptions {
                channels: channels.clone(),
            },
            controller.clone(),
        ) {
            Ok(output) => output,
            Err(error) => {
                // Host admission already owns the process. A failed Runtime
                // handoff must release it rather than strand an unbridged process.
                // Its Host cleanup worker retains ownership if this wait ends.
                let cleanup = crate::runtime::execution::ExecutionController::release(
                    controller.as_ref(),
                    CancellationToken::new(),
                );
                match tokio::time::timeout(
                    Duration::from_millis(self.limits.lifecycle_timeout_ms),
                    cleanup,
                )
                .await
                {
                    Ok(Ok(())) => return Err(error),
                    _ => return Err(Error::ExecutionInterrupted),
                }
            }
        };
        tokio::spawn(async move {
            if let Err(error) = super::source::pump(controller, channels, writer.clone()).await {
                let _ = writer.interrupt(error.tool_diagnostic().summary());
            }
        });
        Ok(output)
    }
    pub(crate) async fn attach_output(
        self: &Arc<Self>,
        session: &str,
        handle: ProcessHandle,
        channels: Vec<String>,
        captured: bool,
        writer: crate::runtime::execution::OutputWriter,
    ) -> Result<Arc<dyn crate::runtime::execution::ExecutionController>, Error> {
        if channels.len() != 2 || channels.iter().any(|s| s.is_empty() || s.len() > 64) {
            return Err(Error::tool(
                "process source requires two valid channel names",
            ));
        }
        let process = self
            .owned_process(session, handle)
            .ok_or_else(|| Error::denied("process belongs to another Session"))?;
        if process.private || process.capture != captured {
            return Err(Error::tool("process capture kind mismatch"));
        }
        if process.bridged.swap(true, Ordering::AcqRel) {
            return Err(Error::tool("process output already adopted"));
        }
        let controller = Arc::new(super::source::ProcessController::new(
            self.clone(),
            session.into(),
            handle,
            captured,
        ));
        let worker_controller = controller.clone();
        tokio::spawn(async move {
            if let Err(error) =
                super::source::pump(worker_controller, channels, writer.clone()).await
            {
                let _ = writer.interrupt(error.tool_diagnostic().summary());
            }
        });
        Ok(controller)
    }
}
