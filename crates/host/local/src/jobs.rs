//! One native process owner for raw execution and captured shell commands.
//! Admission is cancellable; after publication detached supervision owns cleanup.
//! Capture, when requested, consumes both pipes without a second process identity.
mod capture;
use crate::{LocalHost, process};
use capture::{Capture, Decoder, Stream};
use microcrab_host_api::{
    BoxStream, Bytes, ErrorCategory, HandleId, HostError, OpContext, PrivateShellRequest,
    ProcessApi, ShellApi, ShellCaptureStatus, ShellOutput, ShellPosition, ShellRequest,
    process::{ExitStatus, ProcessHandle, ProcessSpec, ProcessState, ProcessStatus, Signal},
    types::Result,
};
use std::{
    collections::HashMap,
    path::Path,
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command},
    sync::{OwnedSemaphorePermit, watch},
};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub(crate) struct ProcessStore {
    entries: Mutex<HashMap<ProcessHandle, Arc<Entry>>>,
    active: Arc<AtomicUsize>,
}
impl Drop for ProcessStore {
    fn drop(&mut self) {
        for entry in self
            .entries
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .values()
        {
            entry.cancel.cancel();
        }
    }
}
impl ProcessStore {
    pub(crate) fn running(&self) -> bool {
        self.active.load(Ordering::SeqCst) != 0
    }
    fn get(&self, handle: ProcessHandle) -> Result<Arc<Entry>> {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&handle)
            .cloned()
            .ok_or_else(|| HostError::known(ErrorCategory::NotFound, "unknown process handle"))
    }
    fn remove(&self, handle: ProcessHandle) {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&handle);
    }
}
struct Captured {
    output: Capture,
    drained: bool,
    closed: bool,
}
struct Data {
    state: ProcessState,
    ended: Option<tokio::time::Instant>,
    capture: Option<Captured>,
}
struct Pipes {
    stdout: Option<ChildStdout>,
    stderr: Option<ChildStderr>,
}
struct Entry {
    handle: ProcessHandle,
    pid: u32,
    started: tokio::time::Instant,
    data: Mutex<Data>,
    pipes: Mutex<Pipes>,
    stdin: tokio::sync::Mutex<Option<ChildStdin>>,
    cancel: CancellationToken,
    changes: watch::Sender<u64>,
}
impl Entry {
    fn native_failure(&self, phase: &'static str, error: &std::io::Error) {
        tracing::error!(
            operation = "process.supervise", phase,
            process_handle = self.handle.0.0, pid = self.pid,
            error_kind = ?error.kind(), raw_os_error = error.raw_os_error(),
            elapsed_ms = self.started.elapsed().as_millis() as u64,
            "local process supervisor native operation failed"
        );
    }
    fn notify(&self) {
        self.changes.send_modify(|n| *n = n.wrapping_add(1));
    }
    fn append(&self, stream: Stream, text: &str) {
        let mut data = self.data.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(capture) = &mut data.capture {
            capture.output.append(stream, text);
        }
        drop(data);
        // Byte counters/cursors are observable while a process is running. The
        // watch coalesces pipe chunks without a polling timer or an output body.
        if !text.is_empty() {
            self.notify();
        }
    }
    fn finish(&self, state: ProcessState) {
        let mut data = self.data.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(capture) = &mut data.capture {
            capture.output.finish();
            capture.closed = capture.drained && capture.output.is_empty();
        }
        data.state = state;
        data.ended = Some(tokio::time::Instant::now());
        drop(data);
        self.notify();
    }
}
/// Even unwinding supervisor futures publish failure and relinquish admission.
struct Completion {
    entry: Arc<Entry>,
    active: Option<Arc<AtomicUsize>>,
    finished: bool,
}
impl Drop for Completion {
    fn drop(&mut self) {
        if !self.finished {
            tracing::error!(
                operation = "process.supervise",
                phase = "completion_dropped",
                process_handle = self.entry.handle.0.0,
                pid = self.entry.pid,
                owner_cancelled = self.entry.cancel.is_cancelled(),
                elapsed_ms = self.entry.started.elapsed().as_millis() as u64,
                "local process supervisor stopped before cleanup completion"
            );
            self.entry.cancel.cancel();
            self.entry.finish(ProcessState::Failed {
                reason: "process supervisor stopped unexpectedly".into(),
            });
        }
        if let Some(active) = self.active.take() {
            active.fetch_sub(1, Ordering::SeqCst);
        }
    }
}
fn conflict(message: &'static str) -> HostError {
    HostError::known(ErrorCategory::Conflict, message)
}
fn tool_error(error: microcrab_host_api::ToolError) -> HostError {
    error.into_host_error()
}

impl LocalHost {
    async fn admit_process(
        &self,
        ctx: &OpContext,
        spec: ProcessSpec,
        output_bytes: Option<usize>,
    ) -> Result<ProcessHandle> {
        ctx.check()?;
        self.require_process_execution("process.spawn")
            .map_err(tool_error)?;
        if spec.pty.is_some() {
            return Err(HostError::unsupported_known("process.pty"));
        }
        if spec.program.is_empty()
            || spec.program.contains('\0')
            || spec.args.iter().any(|arg| arg.contains('\0'))
        {
            return Err(conflict(
                "process program must be nonempty and arguments must not contain NUL",
            ));
        }
        if output_bytes.is_some_and(|cap| cap == 0 || cap > self.limits.max_output_bytes) {
            return Err(conflict(
                "output_bytes must be positive and within the Host capture budget",
            ));
        }
        let admission = async {
            let permit = self.permits.clone().acquire_owned().await.map_err(|e| {
                HostError::new(ErrorCategory::Io, e.to_string())
                    .with_public_reason("Host process admission closed")
            })?;
            let cwd = tokio::fs::canonicalize(
                self.root
                    .join(spec.cwd.as_deref().unwrap_or(Path::new("."))),
            )
            .await?;
            if !cwd.starts_with(&self.root) {
                return Err(HostError::known(
                    ErrorCategory::Permission,
                    "shell working directory is outside the configured root",
                ));
            }
            if !tokio::fs::metadata(&cwd).await?.is_dir() {
                return Err(conflict("cwd must be a directory"));
            }
            let workspace = self.workspace.read().await;
            Ok((permit, cwd, workspace))
        };
        let (permit, cwd, workspace) = ctx.run(admission).await?;
        ctx.check()?;
        self.require_process_execution("process.spawn")
            .map_err(tool_error)?;
        // No await from spawning through publication: dropping admission cannot
        // interrupt the transfer of ownership to the detached supervisor.
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        const MAX_SAFE_ID: u64 = (1 << 53) - 1;
        let id = NEXT_ID
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| {
                (id <= MAX_SAFE_ID).then_some(id + 1)
            })
            .map_err(|_| HostError::known(ErrorCategory::Io, "process handle ids exhausted"))?;
        let handle = ProcessHandle(HandleId(id));
        let mut command = Command::new(&spec.program);
        command
            .args(&spec.args)
            .envs(&spec.env)
            .current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let (mut child, tree) = process::spawn(command).inspect_err(|error| {
            tracing::error!(
                operation = "process.spawn", phase = "native_spawn",
                process_handle = handle.0.0,
                error_kind = ?error.kind(), raw_os_error = error.raw_os_error(),
                "local process admission failed during native spawn"
            );
        })?;
        let pid = child
            .id()
            .ok_or_else(|| HostError::known(ErrorCategory::Io, "spawned process has no pid"))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| HostError::known(ErrorCategory::Io, "process stdin pipe missing"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| HostError::known(ErrorCategory::Io, "process stdout pipe missing"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| HostError::known(ErrorCategory::Io, "process stderr pipe missing"))?;
        let (captured, pipes) = if output_bytes.is_some() {
            (
                Some((stdout, stderr)),
                Pipes {
                    stdout: None,
                    stderr: None,
                },
            )
        } else {
            (
                None,
                Pipes {
                    stdout: Some(stdout),
                    stderr: Some(stderr),
                },
            )
        };
        let (changes, _) = watch::channel(0);
        let entry = Arc::new(Entry {
            handle,
            pid,
            started: tokio::time::Instant::now(),
            data: Mutex::new(Data {
                state: ProcessState::Running,
                ended: None,
                capture: output_bytes.map(|cap| Captured {
                    output: Capture::new(cap),
                    drained: false,
                    closed: false,
                }),
            }),
            pipes: Mutex::new(pipes),
            stdin: tokio::sync::Mutex::new(Some(stdin)),
            cancel: CancellationToken::new(),
            changes,
        });
        self.jobs.active.fetch_add(1, Ordering::SeqCst);
        self.jobs
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(handle, entry.clone());
        let completion = Completion {
            entry: entry.clone(),
            active: Some(self.jobs.active.clone()),
            finished: false,
        };
        tokio::spawn(supervise(child, tree, captured, permit, completion));
        drop(workspace);
        if let Err(error) = ctx.check() {
            entry.cancel.cancel();
            self.jobs.remove(handle);
            return Err(error);
        }
        Ok(handle)
    }
}

#[async_trait::async_trait]
impl ProcessApi for LocalHost {
    async fn exec(
        &self,
        ctx: &OpContext,
        command: &str,
    ) -> Result<microcrab_host_api::process::ExecOutput> {
        ctx.check()?;
        let scoped = OpContext {
            cancel: ctx.cancel.child_token(),
            ..ctx.clone()
        };
        let _cancel_on_drop = scoped.cancel.clone().drop_guard();
        let host = self.clone();
        let command = command.to_owned();
        // Dropping this JoinHandle detaches the worker. The caller's drop guard
        // cancels its observation context; the worker still kills and releases
        // the inaccessible convenience-process handle before relinquishing Host.
        tokio::spawn(
            async move { microcrab_host_api::process::exec(&host, &scoped, &command).await },
        )
        .await
        .map_err(|error| {
            HostError::new(ErrorCategory::Io, error.to_string())
                .with_public_reason("Host process execution worker failed")
        })?
    }
    async fn spawn(&self, ctx: &OpContext, spec: ProcessSpec) -> Result<ProcessHandle> {
        self.admit_process(ctx, spec, None).await
    }
    fn has_running_processes(&self) -> bool {
        self.jobs.running()
    }
    fn has_unresolved_cleanup(&self) -> bool {
        self.jobs.entries.lock().unwrap_or_else(|e| e.into_inner()).values().any(|entry| {
            matches!(&entry.data.lock().unwrap_or_else(|e| e.into_inner()).state,
                ProcessState::Failed { reason } if reason.starts_with("process cleanup failed:") || reason == "process supervisor stopped unexpectedly")
        })
    }
    fn watch(&self, handle: ProcessHandle) -> Result<watch::Receiver<u64>> {
        Ok(self.jobs.get(handle)?.changes.subscribe())
    }
    async fn status(&self, ctx: &OpContext, handle: ProcessHandle) -> Result<ProcessStatus> {
        ctx.check()?;
        let entry = self.jobs.get(handle)?;
        let data = entry.data.lock().unwrap_or_else(|e| e.into_inner());
        Ok(ProcessStatus {
            pid: Some(entry.pid),
            state: data.state.clone(),
            elapsed_s: data
                .ended
                .unwrap_or_else(tokio::time::Instant::now)
                .duration_since(entry.started)
                .as_secs(),
        })
    }
    async fn wait(&self, ctx: &OpContext, handle: ProcessHandle) -> Result<ExitStatus> {
        let entry = self.jobs.get(handle)?;
        ctx.run(wait_terminal(entry)).await
    }
    async fn kill_tree(&self, ctx: &OpContext, handle: ProcessHandle, _force: bool) -> Result<()> {
        ctx.check()?;
        let entry = self.jobs.get(handle)?;
        entry.cancel.cancel();
        ctx.run(wait_terminal(entry)).await.map(|_| ())
    }
    async fn signal(&self, ctx: &OpContext, handle: ProcessHandle, signal: Signal) -> Result<()> {
        ctx.check()?;
        let entry = self.jobs.get(handle)?;
        if signal == Signal::Kill {
            return ProcessApi::kill_tree(self, ctx, handle, true).await;
        }
        #[cfg(windows)]
        {
            let _ = entry;
            Err(HostError::unsupported_known("process.signal"))
        }
        #[cfg(unix)]
        {
            let data = entry.data.lock().unwrap_or_else(|e| e.into_inner());
            if !matches!(data.state, ProcessState::Running) {
                return Err(conflict("process is terminal"));
            }
            let native = match signal {
                Signal::Term => libc::SIGTERM,
                Signal::Kill => libc::SIGKILL,
                Signal::Int => libc::SIGINT,
                Signal::Hup => libc::SIGHUP,
                Signal::Usr1 => libc::SIGUSR1,
                Signal::Usr2 => libc::SIGUSR2,
            };
            // SAFETY: spawn creates this owned process group, and the data guard
            // prevents terminal publication and group-id reuse during this call.
            if unsafe { libc::kill(-(entry.pid as i32), native) } == -1 {
                return Err(std::io::Error::last_os_error().into());
            }
            Ok(())
        }
    }
    fn stdout(&self, handle: ProcessHandle) -> Result<BoxStream<Bytes>> {
        let entry = self.jobs.get(handle)?;
        let pipe = entry
            .pipes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .stdout
            .take()
            .ok_or_else(|| conflict("stdout is already taken or owned by shell capture"))?;
        Ok(raw_stream(pipe))
    }
    fn stderr(&self, handle: ProcessHandle) -> Result<BoxStream<Bytes>> {
        let entry = self.jobs.get(handle)?;
        let pipe = entry
            .pipes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .stderr
            .take()
            .ok_or_else(|| conflict("stderr is already taken or owned by shell capture"))?;
        Ok(raw_stream(pipe))
    }
    async fn write_stdin(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
        bytes: Bytes,
    ) -> Result<()> {
        let entry = self.jobs.get(handle)?;
        ctx.run(async {
            let mut stdin = entry.stdin.lock().await;
            let writer = stdin
                .as_mut()
                .ok_or_else(|| conflict("process stdin is closed"))?;
            writer.write_all(&bytes).await?;
            writer.flush().await?;
            Ok(())
        })
        .await
    }
    async fn close_stdin(&self, ctx: &OpContext, handle: ProcessHandle) -> Result<()> {
        let entry = self.jobs.get(handle)?;
        ctx.run(async {
            entry.stdin.lock().await.take();
            Ok(())
        })
        .await
    }
    async fn release(&self, ctx: &OpContext, handle: ProcessHandle) -> Result<()> {
        ctx.check()?;
        let entry = self.jobs.get(handle)?;
        entry.cancel.cancel();
        ctx.run(wait_terminal(entry)).await?;
        self.jobs.remove(handle);
        Ok(())
    }
    fn shell_spec(&self, command: &str) -> Result<ProcessSpec> {
        self.require_process_execution("process.shell-spec")
            .map_err(tool_error)?;
        if command.is_empty()
            || command.len() > self.limits.max_command_bytes
            || command.contains('\0')
        {
            return Err(conflict(
                "shell command must be nonempty, bounded, and without NUL",
            ));
        }
        #[cfg(windows)]
        let args = vec![
            "-NoLogo".into(),
            "-NoProfile".into(),
            "-NonInteractive".into(),
            "-Command".into(),
            crate::shell::utf8_script(command),
        ];
        #[cfg(unix)]
        let args = vec!["-c".into(), command.into()];
        Ok(ProcessSpec {
            program: self.interpreter.to_string_lossy().into_owned(),
            args,
            ..ProcessSpec::default()
        })
    }
}
fn raw_stream<R: AsyncRead + Unpin + Send + 'static>(reader: R) -> BoxStream<Bytes> {
    Box::pin(microcrab_host_api::stream::unfold(
        Some(reader),
        |reader| async move {
            let mut reader = reader?;
            let mut bytes = vec![0; 8192];
            match reader.read(&mut bytes).await {
                Ok(0) => None,
                Ok(count) => {
                    bytes.truncate(count);
                    Some((Ok(Bytes::from(bytes)), Some(reader)))
                }
                Err(error) => Some((Err(error.into()), None)),
            }
        },
    ))
}

// These synchronous capture operations cannot receive a producer Cancelled error.
// A rejected check establishes only caller-context token/deadline state, not who
// requested cancellation upstream; never infer that from a capture task failure.
fn check_capture_context(
    ctx: &OpContext,
    handle: ProcessHandle,
    operation: &'static str,
) -> Result<()> {
    ctx.check().map_err(|mut error| {
        tracing::warn!(
            operation, phase = "caller_context_check",
            process_handle = handle.0.0,
            category = ?error.category,
            caller_cancelled = ctx.cancel.is_cancelled(),
            caller_deadline_elapsed = ctx.deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline),
            "local shell observation rejected by caller context"
        );
        error.message = format!(
            "{operation} process_handle={}: {}; observation rejected before capture access; this observation did not cancel the process",
            handle.0.0, error.message
        );
        error
    })
}

#[async_trait::async_trait]
impl ShellApi for LocalHost {
    async fn launch_private(
        &self,
        ctx: &OpContext,
        request: PrivateShellRequest,
    ) -> Result<ProcessHandle> {
        let mut spec = ProcessApi::shell_spec(self, &request.command)?;
        spec.cwd = request.cwd;
        self.admit_process(ctx, spec, None).await
    }
    async fn launch(&self, ctx: &OpContext, request: ShellRequest) -> Result<ProcessHandle> {
        let mut spec = ProcessApi::shell_spec(self, &request.command)?;
        spec.cwd = request.cwd;
        self.admit_process(ctx, spec, Some(request.output_bytes))
            .await
    }
    async fn capture_status(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
    ) -> Result<ShellCaptureStatus> {
        check_capture_context(ctx, handle, "shell.capture_status")?;
        let entry = self.jobs.get(handle)?;
        let data = entry.data.lock().unwrap_or_else(|e| e.into_inner());
        let capture = data
            .capture
            .as_ref()
            .ok_or_else(|| conflict("process has no shell capture"))?;
        let (stdout_bytes, stderr_bytes) = capture.output.counts();
        Ok(ShellCaptureStatus {
            closed: capture.closed,
            cursor: capture.output.cursor(),
            stdout_bytes,
            stderr_bytes,
            truncated: capture.output.truncated(),
        })
    }
    async fn read(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
        from: Option<ShellPosition>,
    ) -> Result<ShellOutput> {
        check_capture_context(ctx, handle, "shell.read")?;
        let entry = self.jobs.get(handle)?;
        let data = entry.data.lock().unwrap_or_else(|e| e.into_inner());
        let capture = data
            .capture
            .as_ref()
            .ok_or_else(|| conflict("process has no shell capture"))?;
        capture.output.read(from)
    }
    async fn drain(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
        max_bytes: usize,
    ) -> Result<ShellOutput> {
        check_capture_context(ctx, handle, "shell.drain")?;
        let entry = self.jobs.get(handle)?;
        let mut data = entry.data.lock().unwrap_or_else(|e| e.into_inner());
        let terminal = !matches!(data.state, ProcessState::Running);
        let capture = data
            .capture
            .as_mut()
            .ok_or_else(|| conflict("process has no shell capture"))?;
        let (stdout_bytes, stderr_bytes) = capture.output.counts();
        // Appends take this same data lock. A budget rejection must not consume
        // bytes or advance the capture cursor between a pre-read and a drain.
        if stdout_bytes
            .checked_add(stderr_bytes)
            .is_none_or(|bytes| bytes > max_bytes)
        {
            return Err(conflict("retained shell output exceeds drain byte budget"));
        }
        let output = capture.output.drain()?;
        capture.drained = true;
        let close = terminal && !capture.closed;
        if close {
            capture.closed = true;
        }
        drop(data);
        if close {
            entry.notify();
        }
        Ok(output)
    }
    async fn write(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
        bytes: Vec<u8>,
        eof: bool,
    ) -> Result<()> {
        if bytes.len() > self.limits.max_command_bytes {
            return Err(conflict("shell stdin write exceeds Host byte budget"));
        }
        let entry = self.jobs.get(handle)?;
        if entry
            .data
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .capture
            .is_none()
        {
            return Err(conflict("process has no shell capture"));
        }
        // One lock keeps write+EOF atomic with respect to other stdin writers.
        ctx.run(async {
            let mut stdin = entry.stdin.lock().await;
            let writer = stdin
                .as_mut()
                .ok_or_else(|| conflict("process stdin is closed"))?;
            writer.write_all(&bytes).await?;
            writer.flush().await?;
            if eof {
                stdin.take();
            }
            Ok(())
        })
        .await
    }
}
async fn wait_terminal(entry: Arc<Entry>) -> Result<ExitStatus> {
    let mut changes = entry.changes.subscribe();
    loop {
        let state = entry
            .data
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .state
            .clone();
        match state {
            ProcessState::Running => {}
            ProcessState::Exited { status } => return Ok(status),
            ProcessState::Failed { reason } => {
                return Err(HostError::new(ErrorCategory::Io, reason)
                    .with_public_reason("Host process supervisor reported failure"));
            }
        }
        changes
            .changed()
            .await
            .map_err(|_| HostError::known(ErrorCategory::Io, "process notification closed"))?;
    }
}
async fn supervise(
    mut child: Child,
    tree: process::ProcessTree,
    captured: Option<(ChildStdout, ChildStderr)>,
    permit: OwnedSemaphorePermit,
    mut completion: Completion,
) {
    let entry = completion.entry.clone();
    // Capture readers run independently so cancellation/leader exit can always
    // kill the tree while both pipes continue draining to their stable end.
    let capture = captured.map(|(stdout, stderr)| {
        let entry = entry.clone();
        tokio::spawn(async move {
            tokio::try_join!(
                capture_pipe(stdout, entry.clone(), Stream::Stdout),
                capture_pipe(stderr, entry, Stream::Stderr)
            )
            .map(|_| ())
        })
    });
    let outcome = tokio::select! {
        biased;
        _ = entry.cancel.cancelled() => None,
        result = child.wait() => {
            if let Err(error) = &result {
                entry.native_failure("initial_wait", error);
            }
            Some(result)
        },
    };
    let owner_terminated = outcome.is_none();
    tracing::debug!(
        operation = "process.supervise",
        phase = "cleanup_begin",
        process_handle = entry.handle.0.0,
        pid = entry.pid,
        trigger = if owner_terminated {
            "owner_cancel_observed"
        } else {
            "child_wait_completed"
        },
        elapsed_ms = entry.started.elapsed().as_millis() as u64,
        "local process cleanup started"
    );
    let cleanup_started = tokio::time::Instant::now();
    let mut cleanup_error = tree.terminate().err().map(|error| {
        entry.native_failure("terminate_tree", &error);
        error.to_string()
    });
    let status = match outcome {
        Some(Ok(status)) => Some(status),
        Some(Err(error)) => {
            cleanup_error = Some(error.to_string());
            None
        }
        None => None,
    };
    let status = if status.is_none() {
        if let Err(error) = child.start_kill() {
            entry.native_failure("start_kill", &error);
        }
        let wait_started = tokio::time::Instant::now();
        // This wait is intentionally unbounded by the existing owner contract.
        tracing::debug!(
            operation = "process.supervise",
            phase = "cleanup_wait_begin",
            process_handle = entry.handle.0.0,
            pid = entry.pid,
            bounded = false,
            "local process cleanup waiting for child"
        );
        let result = child.wait().await;
        tracing::debug!(
            operation = "process.supervise",
            phase = "cleanup_wait_end",
            process_handle = entry.handle.0.0,
            pid = entry.pid,
            elapsed_ms = wait_started.elapsed().as_millis() as u64,
            succeeded = result.is_ok(),
            "local process cleanup child wait completed"
        );
        match result {
            Ok(status) => Some(status),
            Err(error) => {
                entry.native_failure("cleanup_wait", &error);
                cleanup_error = Some(error.to_string());
                None
            }
        }
    } else {
        status
    };
    #[cfg(windows)]
    if let Err(error) = tree.wait_empty().await {
        entry.native_failure("wait_job_empty", &error);
        cleanup_error = Some(error.to_string());
    }
    if let Some(mut capture) = capture {
        let pipes_started = tokio::time::Instant::now();
        match tokio::time::timeout(Duration::from_secs(2), &mut capture).await {
            Ok(Ok(Ok(()))) => {}
            // The native pipe failure is logged at the read before try_join!
            // drops the other reader; do not mistake that drop for caller cancel.
            Ok(Ok(Err(error))) => cleanup_error = Some(error.to_string()),
            Ok(Err(error)) => {
                tracing::error!(
                    operation = "shell.capture",
                    phase = "capture_task_join",
                    process_handle = entry.handle.0.0,
                    pid = entry.pid,
                    task_cancelled = error.is_cancelled(),
                    task_panicked = error.is_panic(),
                    elapsed_ms = pipes_started.elapsed().as_millis() as u64,
                    "local shell capture task failed"
                );
                cleanup_error = Some(error.to_string());
            }
            Err(_) => {
                tracing::error!(
                    operation = "shell.capture", phase = "wait_pipes_closed",
                    process_handle = entry.handle.0.0, pid = entry.pid,
                    error_kind = ?std::io::ErrorKind::TimedOut,
                    timeout_ms = 2000_u64,
                    elapsed_ms = pipes_started.elapsed().as_millis() as u64,
                    cleanup_step = "abort_capture_task",
                    "local shell capture pipes did not close before timeout"
                );
                capture.abort();
                let _ = capture.await;
                cleanup_error = Some("output pipes did not close within 2000 ms".into());
            }
        }
    }
    entry.stdin.lock().await.take();
    drop(child);
    drop(tree);
    drop(permit);
    tracing::debug!(
        operation = "process.supervise",
        phase = "cleanup_end",
        process_handle = entry.handle.0.0,
        pid = entry.pid,
        elapsed_ms = cleanup_started.elapsed().as_millis() as u64,
        succeeded = cleanup_error.is_none(),
        "local process cleanup completed"
    );
    if let Some(active) = completion.active.take() {
        active.fetch_sub(1, Ordering::SeqCst);
    }
    completion.finished = true;
    let state = match cleanup_error {
        Some(reason) => ProcessState::Failed {
            reason: format!("process cleanup failed: {reason}"),
        },
        None => {
            let mut status = native_status(status.expect("successful cleanup has an exit status"));
            if owner_terminated && status.signal.is_none() {
                status.signal = Some(Signal::Kill);
            }
            ProcessState::Exited { status }
        }
    };
    entry.finish(state);
}
async fn capture_pipe<R: AsyncRead + Unpin>(
    mut reader: R,
    entry: Arc<Entry>,
    stream: Stream,
) -> std::io::Result<()> {
    let mut decoder = Decoder::default();
    let mut bytes = [0; 8192];
    loop {
        let count = reader.read(&mut bytes).await.inspect_err(|error| {
            tracing::error!(
                operation = "shell.capture", phase = "read_pipe",
                process_handle = entry.handle.0.0, pid = entry.pid,
                stream = match stream { Stream::Stdout => "stdout", Stream::Stderr => "stderr" },
                error_kind = ?error.kind(), raw_os_error = error.raw_os_error(),
                owner_cancelled = entry.cancel.is_cancelled(),
                elapsed_ms = entry.started.elapsed().as_millis() as u64,
                "local shell capture producer pipe read failed"
            );
        })?;
        entry.append(stream, &decoder.push(&bytes[..count], count == 0));
        if count == 0 {
            return Ok(());
        }
    }
}
fn native_status(status: std::process::ExitStatus) -> ExitStatus {
    #[cfg(unix)]
    let signal = {
        use std::os::unix::process::ExitStatusExt;
        match status.signal() {
            Some(libc::SIGTERM) => Some(Signal::Term),
            Some(libc::SIGKILL) => Some(Signal::Kill),
            Some(libc::SIGINT) => Some(Signal::Int),
            Some(libc::SIGHUP) => Some(Signal::Hup),
            Some(libc::SIGUSR1) => Some(Signal::Usr1),
            Some(libc::SIGUSR2) => Some(Signal::Usr2),
            _ => None,
        }
    };
    #[cfg(not(unix))]
    let signal = None;
    ExitStatus {
        code: status.code(),
        signal,
    }
}

#[cfg(test)]
mod observation_error_tests {
    use super::*;

    #[test]
    fn capture_context_rejection_names_operation_without_claiming_process_failure() {
        let handle = ProcessHandle(HandleId(17));
        let cancelled = OpContext::default();
        cancelled.cancel.cancel();
        let error = check_capture_context(&cancelled, handle, "shell.drain").unwrap_err();
        assert_eq!(error.category, ErrorCategory::Cancelled);
        assert!(error.message.contains("shell.drain process_handle=17"));
        assert!(error.message.contains("did not cancel the process"));
        let expired = OpContext {
            deadline: Some(std::time::Instant::now()),
            ..Default::default()
        };
        let error = check_capture_context(&expired, handle, "shell.read").unwrap_err();
        assert_eq!(error.category, ErrorCategory::Timeout);
        assert!(error.message.contains("shell.read process_handle=17"));
        assert!(error.message.contains("deadline exceeded"));
    }
}
