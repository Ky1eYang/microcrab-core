//! Core contracts and composed helper failure behavior; no operating-system I/O.
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream;
use microcrab_host_api::{
    ErrorCategory, FsApi, HandleId, HostAPI, HostError, NetApi, OpContext, Platform, ProcessApi,
    ShellApi,
    filesystem::FileHandle,
    process::{ExitStatus, ProcessHandle, ProcessSpec},
    types::{BoxStream, Result},
};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Barrier;

struct Empty;
impl ProcessApi for Empty {}
impl FsApi for Empty {}
impl NetApi for Empty {}
impl ShellApi for Empty {}
impl HostAPI for Empty {}

impl microcrab_host_api::Host for Empty {}

#[tokio::test]
async fn web_transport_defaults_are_typed_unsupported_and_never_claim_authority() {
    use microcrab_host_api::web_transport::{ConnectionScope, ProjectScope, WebTransportError as E};
    let host: &dyn microcrab_host_api::Host = &Empty;
    let scope = ConnectionScope {
        binding: ProjectScope { binding_id: "binding".into(), backend_id: "backend".into(), project_id: "project".into() },
        session_id: "session".into(),
        epoch: 3,
        connection_id: "id".into(),
    };
    assert!(!host.supports_web_transport());
    assert!(host.web_transport_binding().is_none());
    let ctx = OpContext::default();
    let results = [
        host.http_get(&ctx, &scope, "http://example.test:80/path", 1).await.map(drop),
        host.ws_open(&ctx, &scope, "ws://example.test:80/path").await,
        host.ws_status(&ctx, &scope).await,
        host.ws_send(&ctx, &scope, "").await,
        host.ws_receive(&ctx, &scope, 1).await.map(drop),
        host.ws_pull(&ctx, &scope, 1, 0).await.map(drop),
        host.ws_close(&ctx, &scope).await,
        host.ws_release_scope(&ctx, &scope.owner()).await,
    ];
    assert!(results.into_iter().all(|err| err == Err(E::Unsupported)));
    ctx.cancel.cancel();
    assert_eq!(host.ws_status(&ctx, &scope).await, Err(E::Cancelled));
    let expired = OpContext { deadline: Some(Instant::now()), ..Default::default() };
    assert_eq!(host.ws_status(&expired, &scope).await, Err(E::Timeout));
    assert_eq!(host.ws_pull(&ctx, &scope, 1, 0).await, Err(E::Cancelled));
    assert_eq!(host.ws_pull(&expired, &scope, 1, 0).await, Err(E::Timeout));
}

#[tokio::test]
async fn tool_extension_ports_are_object_safe_and_map_context_errors() {
    use microcrab_host_api::{ReadRequest, ToolError, tool_api::TextReadApi};
    let host: &dyn TextReadApi = &Empty;
    let request = || ReadRequest {
        path: "file".into(),
        offset_line: 1,
        max_lines: 1,
        max_bytes: 10,
    };
    let ctx = OpContext::default();
    ctx.cancel.cancel();
    assert!(matches!(
        host.read_text(&ctx, request()).await,
        Err(ToolError::Cancelled)
    ));
    let ctx = OpContext {
        deadline: Some(Instant::now() - Duration::from_millis(1)),
        ..OpContext::default()
    };
    assert!(matches!(
        host.read_text(&ctx, request()).await,
        Err(ToolError::OperationDeadline)
    ));
    assert!(matches!(
        host.read_text(&OpContext::default(), request()).await,
        Err(ToolError::Unsupported(_))
    ));
}

struct CommittedWrite;
impl ProcessApi for CommittedWrite {}
impl FsApi for CommittedWrite {}
impl NetApi for CommittedWrite {}
impl ShellApi for CommittedWrite {}
impl HostAPI for CommittedWrite {}
#[async_trait]
impl microcrab_host_api::Host for CommittedWrite {
    async fn mutate(
        &self,
        _: microcrab_host_api::FileMutation,
        cancel: microcrab_host_api::CancelToken,
    ) -> std::result::Result<microcrab_host_api::MutationOutput, microcrab_host_api::ToolError>
    {
        cancel.cancel();
        Ok(microcrab_host_api::MutationOutput {
            version: Some("committed".into()),
            bytes: 1,
            source_removed: false,
            destination_written: true,
            diagnostic: Some("source removal failed: cancelled".into()),
        })
    }

    async fn write(
        &self,
        _: microcrab_host_api::WriteRequest,
        cancel: microcrab_host_api::CancelToken,
    ) -> std::result::Result<microcrab_host_api::WriteOutput, microcrab_host_api::ToolError> {
        // Simulate cancellation arriving immediately after successful publication.
        cancel.cancel();
        Ok(microcrab_host_api::WriteOutput {
            version: "committed".into(),
            bytes: 1,
        })
    }
}

#[tokio::test]
async fn tool_publication_adapter_preserves_known_commit_after_cancellation() {
    use microcrab_host_api::{WritePrecondition, WriteRequest, tool_api::TextWriteApi};
    let ctx = OpContext::default();
    let output = TextWriteApi::write(
        &CommittedWrite,
        &ctx,
        WriteRequest {
            path: "file".into(),
            content: "x".into(),
            precondition: WritePrecondition::Missing,
        },
    )
    .await
    .unwrap();
    assert_eq!(output.version, "committed");
    assert!(!ctx.cancel.is_cancelled());
}

#[tokio::test]
async fn facade_preserves_partial_mutation_receipt_after_cancellation() {
    use microcrab_host_api::{FileMutation, FileMutationApi, MoveRequest};
    let host: &dyn HostAPI = &CommittedWrite;
    let ctx = OpContext::default();
    let output = FileMutationApi::mutate(
        host,
        &ctx,
        FileMutation::Move(MoveRequest {
            path: "source".into(),
            destination: "destination".into(),
            expected_version: "version".into(),
            content: "x".into(),
        }),
    )
    .await
    .unwrap();
    assert!(output.destination_written && !output.source_removed);
    assert!(output.diagnostic.is_some());
    assert!(!ctx.cancel.is_cancelled());
}

#[tokio::test]
async fn object_safe_facade_defaults_are_unavailable() {
    let host: Arc<dyn HostAPI> = Arc::new(Empty);
    let ctx = OpContext::default();
    assert_eq!(host.platform(), Platform::Other);
    assert!(!host.supports("process.spawn"));
    let errors = [
        ProcessApi::spawn(host.as_ref(), &ctx, ProcessSpec::default())
            .await
            .unwrap_err(),
        FsApi::stat(host.as_ref(), &ctx, Path::new("missing"))
            .await
            .unwrap_err(),
        NetApi::resolve(host.as_ref(), &ctx, "localhost")
            .await
            .unwrap_err(),
        ShellApi::launch(
            host.as_ref(),
            &ctx,
            microcrab_host_api::ShellRequest {
                command: "echo hello".into(),
                cwd: None,
                output_bytes: 1024,
            },
        )
        .await
        .unwrap_err(),
        ProcessApi::exec(host.as_ref(), &ctx, "echo hello")
            .await
            .unwrap_err(),
    ];
    for error in errors {
        assert_eq!(error.category, ErrorCategory::Unsupported);
    }
}

#[tokio::test]
async fn context_keeps_cancellation_and_deadline_distinct() {
    let ctx = OpContext::default();
    ctx.cancel.cancel();
    assert_eq!(
        ctx.run(async { Ok(()) }).await.unwrap_err().category,
        ErrorCategory::Cancelled
    );
    let ctx = OpContext {
        deadline: Some(Instant::now() - Duration::from_millis(1)),
        ..OpContext::default()
    };
    assert_eq!(
        ctx.run(async { Ok(()) }).await.unwrap_err().category,
        ErrorCategory::Timeout
    );
    let ctx = OpContext {
        deadline: Some(Instant::now() + Duration::from_millis(1)),
        ..OpContext::default()
    };
    assert_eq!(
        ctx.run(std::future::pending::<Result<()>>())
            .await
            .unwrap_err()
            .category,
        ErrorCategory::Timeout
    );
}

#[test]
fn io_error_retains_category_and_source() {
    use std::error::Error;
    let error = HostError::from(std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "denied",
    ));
    assert_eq!(error.category, ErrorCategory::Permission);
    assert!(error.source().is_some());
    assert_eq!(error.to_string(), "denied");
    let facade = microcrab_host_api::ToolError::from(HostError::new(
        ErrorCategory::Conflict,
        "stdout already taken",
    ));
    assert!(
        matches!(facade, microcrab_host_api::ToolError::Host(error) if error.category == ErrorCategory::Conflict)
    );
}

struct ProcessMock {
    barrier: Arc<Barrier>,
    fail_wait: bool,
    events: Mutex<Vec<&'static str>>,
    context: Mutex<Option<OpContext>>,
}
impl ProcessMock {
    fn new(fail_wait: bool) -> Self {
        Self {
            barrier: Arc::new(Barrier::new(3)),
            fail_wait,
            events: Mutex::new(Vec::new()),
            context: Mutex::new(None),
        }
    }
    fn pipe(&self, bytes: &'static [u8]) -> BoxStream<Bytes> {
        let barrier = self.barrier.clone();
        Box::pin(stream::once(async move {
            barrier.wait().await;
            Ok(Bytes::from_static(bytes))
        }))
    }
}
#[async_trait]
impl ProcessApi for ProcessMock {
    fn shell_spec(&self, command: &str) -> Result<ProcessSpec> {
        Ok(ProcessSpec {
            program: "trusted-shell".into(),
            args: vec![command.into()],
            ..ProcessSpec::default()
        })
    }
    async fn spawn(&self, ctx: &OpContext, spec: ProcessSpec) -> Result<ProcessHandle> {
        assert_eq!(spec.program, "trusted-shell");
        *self.context.lock().unwrap() = Some(ctx.clone());
        Ok(ProcessHandle(HandleId(1)))
    }
    fn stdout(&self, _: ProcessHandle) -> Result<BoxStream<Bytes>> {
        Ok(self.pipe(b"stdout"))
    }
    fn stderr(&self, _: ProcessHandle) -> Result<BoxStream<Bytes>> {
        Ok(self.pipe(b"stderr"))
    }
    async fn close_stdin(&self, _: &OpContext, _: ProcessHandle) -> Result<()> {
        Ok(())
    }
    async fn wait(&self, _: &OpContext, _: ProcessHandle) -> Result<ExitStatus> {
        self.barrier.wait().await;
        if self.fail_wait {
            return Err(HostError::new(ErrorCategory::Io, "primary wait failure"));
        }
        Ok(ExitStatus {
            code: Some(0),
            signal: None,
        })
    }
    async fn kill_tree(&self, ctx: &OpContext, _: ProcessHandle, force: bool) -> Result<()> {
        ctx.check()?;
        assert!(force);
        self.events.lock().unwrap().push("kill");
        Err(HostError::new(ErrorCategory::Io, "cleanup kill failure"))
    }
    async fn release(&self, ctx: &OpContext, _: ProcessHandle) -> Result<()> {
        ctx.check()?;
        self.events.lock().unwrap().push("release");
        Ok(())
    }
}

#[tokio::test]
async fn exec_polls_both_pipes_and_wait_concurrently() {
    let process = ProcessMock::new(false);
    let output = tokio::time::timeout(
        Duration::from_secs(1),
        process.exec(&OpContext::default(), "command"),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(output.stdout, Bytes::from_static(b"stdout"));
    assert_eq!(output.stderr, Bytes::from_static(b"stderr"));
    assert_eq!(*process.events.lock().unwrap(), vec!["release"]);
}

#[tokio::test]
async fn exec_failure_kills_and_releases_without_masking_primary_error() {
    let process = ProcessMock::new(true);
    let error = process
        .exec(&OpContext::default(), "command")
        .await
        .unwrap_err();
    assert_eq!(error.message, "primary wait failure");
    assert_eq!(*process.events.lock().unwrap(), vec!["kill", "release"]);
}

#[tokio::test]
async fn dropped_exec_preserves_host_owned_process_context() {
    let process = ProcessMock::new(false);
    let ctx = OpContext::default();
    let mut execution = Box::pin(process.exec(&ctx, "command"));
    // One poll reaches the three-way barrier, whose wakeups cannot complete the
    // enclosing future within that same poll. Drop ends observation only.
    assert!(futures_util::poll!(execution.as_mut()).is_pending());
    drop(execution);
    assert!(
        !process
            .context
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .cancel
            .is_cancelled()
    );
    assert!(!ctx.cancel.is_cancelled());
}

#[derive(Clone, Copy)]
enum FsMode {
    Short,
    Zero,
    FlushFailure,
    CloseFailure,
    RenameFailure,
}
struct FsMock {
    mode: FsMode,
    bytes: Mutex<Vec<u8>>,
    events: Mutex<Vec<&'static str>>,
}
impl FsMock {
    fn new(mode: FsMode) -> Self {
        Self {
            mode,
            bytes: Mutex::new(Vec::new()),
            events: Mutex::new(Vec::new()),
        }
    }
}
#[async_trait]
impl FsApi for FsMock {
    async fn create_temp(&self, _: &OpContext, dir: &Path) -> Result<(PathBuf, FileHandle)> {
        assert_eq!(dir, Path::new("parent"));
        Ok((dir.join("exclusive-temp"), FileHandle(HandleId(2))))
    }
    async fn write(&self, _: &OpContext, _: FileHandle, offset: u64, data: Bytes) -> Result<usize> {
        if matches!(self.mode, FsMode::Zero) {
            return Ok(0);
        }
        let mut bytes = self.bytes.lock().unwrap();
        assert_eq!(offset, bytes.len() as u64);
        let count = data.len().min(2);
        bytes.extend_from_slice(&data[..count]);
        Ok(count)
    }
    async fn fsync(&self, _: &OpContext, _: FileHandle) -> Result<()> {
        self.events.lock().unwrap().push("fsync");
        if matches!(self.mode, FsMode::FlushFailure) {
            return Err(HostError::new(ErrorCategory::Io, "flush failure"));
        }
        Ok(())
    }
    async fn close(&self, ctx: &OpContext, _: FileHandle) -> Result<()> {
        ctx.check()?;
        self.events.lock().unwrap().push("close");
        if matches!(self.mode, FsMode::CloseFailure) {
            return Err(HostError::new(ErrorCategory::Io, "close failure"));
        }
        Ok(())
    }
    async fn rename(&self, _: &OpContext, _: &Path, _: &Path) -> Result<()> {
        self.events.lock().unwrap().push("rename");
        if matches!(self.mode, FsMode::RenameFailure) {
            return Err(HostError::new(ErrorCategory::Io, "rename failure"));
        }
        Ok(())
    }
    async fn remove(&self, ctx: &OpContext, _: &Path, recursive: bool) -> Result<()> {
        ctx.check()?;
        assert!(!recursive);
        self.events.lock().unwrap().push("remove");
        Err(HostError::new(ErrorCategory::Io, "cleanup remove failure"))
    }
}

#[tokio::test]
async fn atomic_write_retries_short_writes_and_closes_before_publication() {
    let fs = FsMock::new(FsMode::Short);
    fs.atomic_write(
        &OpContext::default(),
        Path::new("parent/file"),
        Bytes::from_static(b"12345"),
    )
    .await
    .unwrap();
    assert_eq!(*fs.bytes.lock().unwrap(), b"12345");
    assert_eq!(*fs.events.lock().unwrap(), vec!["fsync", "close", "rename"]);
}

#[tokio::test]
async fn atomic_write_zero_progress_and_failures_cleanup_preserving_primary_error() {
    for (mode, message, events) in [
        (
            FsMode::Zero,
            "invalid atomic_write progress",
            vec!["close", "remove"],
        ),
        (
            FsMode::FlushFailure,
            "flush failure",
            vec!["fsync", "close", "remove"],
        ),
        (
            FsMode::CloseFailure,
            "close failure",
            vec!["fsync", "close", "close", "remove"],
        ),
        (
            FsMode::RenameFailure,
            "rename failure",
            vec!["fsync", "close", "rename", "remove"],
        ),
    ] {
        let fs = FsMock::new(mode);
        let error = fs
            .atomic_write(
                &OpContext::default(),
                Path::new("parent/file"),
                Bytes::from_static(b"12345"),
            )
            .await
            .unwrap_err();
        assert_eq!(error.message, message);
        assert_eq!(*fs.events.lock().unwrap(), events);
    }
}

#[test]
fn host_reasons_require_explicit_trust_and_redact_private_debug() {
    use microcrab_host_api::{FailureReason, ToolError};
    for error in [
        ToolError::InvalidRequest("concrete worker failure; token=private-host-secret".into()),
        ToolError::Unsupported("concrete worker failure; token=private-host-secret".into()),
        ToolError::OperationLimit("concrete worker failure; token=private-host-secret".into()),
        ToolError::Worker("concrete worker failure; token=private-host-secret".into()),
    ] {
        assert!(
            error
                .public_reason()
                .unwrap()
                .contains("concrete worker failure")
        );
        assert!(!format!("{error:?}").contains("private-host-secret"));
        assert!(error.to_string().contains("private-host-secret"));
    }
    let known = ToolError::invalid_request("file byte budget must be positive");
    assert_eq!(
        known.public_reason(),
        Some("file byte budget must be positive")
    );
    let worker = ToolError::Worker(
        FailureReason::from("private-host-secret").with_public_reason("file worker stopped"),
    );
    assert_eq!(worker.public_reason(), Some("file worker stopped"));
    assert!(!format!("{worker:?}").contains("private-host-secret"));
    assert_ne!(
        ToolError::PathOutsideRoot.public_reason(),
        ToolError::CwdOutsideRoot.public_reason()
    );
    assert_ne!(
        ToolError::Timeout.public_reason(),
        ToolError::OperationDeadline.public_reason()
    );
    assert_ne!(
        ToolError::WorkspaceConflict.public_reason(),
        ToolError::FileConflict.public_reason()
    );
}
