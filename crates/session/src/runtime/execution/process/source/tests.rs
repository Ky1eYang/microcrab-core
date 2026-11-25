//! Deterministic process cancellation ownership and terminal-pump regressions.
use super::*;
use crate::{
    Limits,
    runtime::execution::{
        ExecutionRegistry, ExecutionSnapshot, ExecutionStatus, OutputOptions, ToolOutput,
        process::ProcessControl,
    },
};
use futures_util::{future::BoxFuture, poll};
use microcrab_host_api::{
    ErrorCategory, FsApi, HandleId, Host, HostAPI, HostError, NetApi, OpContext,
    ShellCaptureStatus, ShellOutput, ShellPosition, ShellRequest,
    process::{ExitStatus, ProcessStatus},
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{Semaphore, watch};

#[tokio::test]
async fn local_private_shell_pump_retains_exact_octets() {
    let workspace = tempfile::tempdir().unwrap();
    let host = Arc::new(
        microcrab_host_local::LocalHost::new(workspace.path(), Default::default()).unwrap(),
    );
    let processes = ProcessRegistry::new("owner".into(), Some(host), Limits::default());
    let executions = ExecutionRegistry::new(Limits::default());
    let owner = executions.clone();
    let process_owner = processes.clone();
    let mut task = executions.start(
        ExecutionSnapshot::new("owner".into(), "call".into(), "shell".into(), "agent".into(), None),
        CancellationToken::new(), move |progress| async move {
            // This native fixture keeps the exact scoped process controller until
            // the execution owner is ready; the production guest reserves first.
            let command = if cfg!(windows) {
                "[Console]::OpenStandardOutput().Write([byte[]](255,0,254,128),0,4); [Console]::Error.Write('not captured')"
            } else {
                "printf '\\377\\000\\376\\200'; printf 'not captured' >&2"
            };
            let handle = process_owner.launch_private_shell("shell", "call", microcrab_host_api::PrivateShellRequest { command: command.into(), cwd: None }, CancellationToken::new()).await?;
            assert!(!process_owner.owns_process(handle));
            assert!(process_owner.projection().is_none());
            assert!(process_owner.read_process_pipe(handle, false, 1024, CancellationToken::new()).await.is_err());
            let mut controller = ProcessController::new(process_owner, "owner".into(), handle, false);
            controller.private = true;
            let controller = Arc::new(controller);
            let (writer, output) = owner.open_private_output(progress.execution_id(), "application/octet-stream".into(), controller.clone())?;
            pump_private(controller, writer).await?;
            Ok(ToolOutput::Buffered(output))
        }
    ).unwrap();
    let id = task.id;
    task.activate().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(15), task.wait())
        .await
        .unwrap()
        .unwrap();
    task.detach();
    assert_eq!(
        executions
            .private_output(id, "owner", "stdout", "application/octet-stream")
            .unwrap()
            .as_bytes(),
        &[255, 0, 254, 128]
    );
    assert!(executions.read(id, 0, 1024).unwrap().chunks.is_empty());
    executions
        .release(id, CancellationToken::new())
        .await
        .unwrap();
    processes.close().await.unwrap();
}

struct ControlledHost {
    terminal: AtomicBool,
    fail_cancel: AtomicBool,
    cancel_allowed: Semaphore,
    cancel_calls: AtomicUsize,
    gate_release: AtomicBool,
    release_calls: AtomicUsize,
    release_entered: Semaphore,
    release_allowed: Semaphore,
    changes: watch::Sender<u64>,
    captures: std::sync::Mutex<std::collections::VecDeque<ShellOutput>>,
}
impl ControlledHost {
    fn finish(&self) {
        self.terminal.store(true, Ordering::Release);
        self.changes.send_modify(|revision| *revision += 1);
    }
}
#[async_trait]
impl ProcessApi for ControlledHost {
    fn watch(&self, _: ProcessHandle) -> Result<watch::Receiver<u64>, HostError> {
        Ok(self.changes.subscribe())
    }
    async fn status(&self, _: &OpContext, _: ProcessHandle) -> Result<ProcessStatus, HostError> {
        Ok(ProcessStatus {
            pid: Some(1),
            state: if self.terminal.load(Ordering::Acquire) {
                ProcessState::Exited {
                    status: ExitStatus {
                        code: Some(0),
                        signal: None,
                    },
                }
            } else {
                ProcessState::Running
            },
            elapsed_s: 0,
        })
    }
    async fn kill_tree(&self, _: &OpContext, _: ProcessHandle, _: bool) -> Result<(), HostError> {
        self.cancel_calls.fetch_add(1, Ordering::AcqRel);
        self.cancel_allowed.acquire().await.unwrap().forget();
        if self.fail_cancel.load(Ordering::Acquire) {
            Err(HostError::new(
                ErrorCategory::Io,
                "injected cancellation failure",
            ))
        } else {
            self.finish();
            Ok(())
        }
    }
    async fn release(&self, _: &OpContext, _: ProcessHandle) -> Result<(), HostError> {
        self.release_calls.fetch_add(1, Ordering::AcqRel);
        if self.gate_release.load(Ordering::Acquire) {
            self.release_entered.add_permits(1);
            self.release_allowed.acquire().await.unwrap().forget();
        }
        self.finish();
        Ok(())
    }
}
#[async_trait]
impl ShellApi for ControlledHost {
    async fn launch(&self, _: &OpContext, _: ShellRequest) -> Result<ProcessHandle, HostError> {
        Ok(ProcessHandle(HandleId(1)))
    }
    async fn capture_status(
        &self,
        _: &OpContext,
        _: ProcessHandle,
    ) -> Result<ShellCaptureStatus, HostError> {
        // Release the single capture guard before constructing the response;
        // separate field-expression guards live through the whole statement.
        let (stdout_bytes, stderr_bytes) = {
            let captures = self.captures.lock().unwrap();
            captures.iter().fold((0, 0), |(stdout, stderr), output| {
                (stdout + output.stdout.len(), stderr + output.stderr.len())
            })
        };
        Ok(ShellCaptureStatus {
            closed: self.terminal.load(Ordering::Acquire),
            cursor: ShellPosition::default(),
            stdout_bytes,
            stderr_bytes,
            truncated: false,
        })
    }
    async fn drain(
        &self,
        _: &OpContext,
        _: ProcessHandle,
        _: usize,
    ) -> Result<ShellOutput, HostError> {
        if let Some(output) = self.captures.lock().unwrap().pop_front() {
            return Ok(output);
        }
        Ok(ShellOutput {
            stdout: String::new(),
            stderr: String::new(),
            next: ShellPosition::default(),
            cursor: ShellPosition::default(),
            expired: None,
            truncated: false,
        })
    }
}
impl FsApi for ControlledHost {}
impl NetApi for ControlledHost {}
impl HostAPI for ControlledHost {}
impl Host for ControlledHost {}

struct Fixture {
    host: Arc<ControlledHost>,
    controller: Arc<ProcessController>,
    executions: Arc<ExecutionRegistry>,
    writer: OutputWriter,
}
impl Fixture {
    async fn new() -> Self {
        Self::with_limits(Limits::default()).await
    }
    async fn with_limits(limits: Limits) -> Self {
        let host = Arc::new(ControlledHost {
            terminal: AtomicBool::new(false),
            fail_cancel: AtomicBool::new(false),
            cancel_allowed: Semaphore::new(0),
            cancel_calls: AtomicUsize::new(0),
            gate_release: AtomicBool::new(false),
            release_calls: AtomicUsize::new(0),
            release_entered: Semaphore::new(0),
            release_allowed: Semaphore::new(0),
            changes: watch::channel(0).0,
            captures: Default::default(),
        });
        let registry = ProcessRegistry::new("owner".into(), Some(host.clone()), limits.clone());
        let handle = registry
            .launch_shell(
                "shell",
                "call",
                ShellRequest {
                    command: "controlled".into(),
                    cwd: None,
                    output_bytes: 1024,
                },
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let controller = Arc::new(ProcessController::new(
            registry,
            "owner".into(),
            handle,
            true,
        ));
        let executions = ExecutionRegistry::new(limits);
        let owner = executions.clone();
        let source = controller.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let mut task = executions
            .start(
                ExecutionSnapshot::new(
                    "owner".into(),
                    "call".into(),
                    "shell".into(),
                    "agent".into(),
                    None,
                ),
                CancellationToken::new(),
                move |progress| async move {
                    let (writer, output) = owner.open_output(
                        progress.execution_id(),
                        OutputOptions {
                            channels: vec!["stdout".into(), "stderr".into()],
                        },
                        source,
                    )?;
                    sender.send(writer).map_err(|_| Error::Closed)?;
                    Ok(ToolOutput::Buffered(output))
                },
            )
            .unwrap();
        task.activate().unwrap();
        task.wait().await.unwrap();
        task.detach();
        Self {
            host,
            controller,
            executions,
            writer: receiver.await.unwrap(),
        }
    }
    fn pump(&self) -> BoxFuture<'static, Result<(), Error>> {
        Box::pin(super::pump(
            self.controller.clone(),
            vec!["stdout".into(), "stderr".into()],
            self.writer.clone(),
        ))
    }
    fn status(&self) -> ExecutionStatus {
        self.executions
            .get(self.writer.execution_id())
            .unwrap()
            .status
    }
    async fn close(&self) {
        self.controller
            .release(CancellationToken::new())
            .await
            .unwrap();
        self.writer.interrupt("fixture closed").unwrap();
        self.executions.close().await.unwrap();
        self.controller.registry.close().await.unwrap();
    }
}

#[tokio::test]
async fn dropped_pending_cancel_before_natural_terminal_does_not_strand_pump() {
    let fixture = Fixture::new().await;
    let mut cancel = fixture.controller.cancel(CancellationToken::new());
    assert!(poll!(cancel.as_mut()).is_pending());
    drop(cancel);
    fixture.host.finish();
    let mut pump = fixture.pump();
    assert!(matches!(
        poll!(pump.as_mut()),
        std::task::Poll::Ready(Ok(()))
    ));
    assert_eq!(fixture.status(), ExecutionStatus::Succeeded);
    fixture.close().await;
}

#[tokio::test]
async fn dropped_pending_cancel_wakes_terminal_pump() {
    let fixture = Fixture::new().await;
    let mut cancel = fixture.controller.cancel(CancellationToken::new());
    assert!(poll!(cancel.as_mut()).is_pending());
    fixture.host.finish();
    let mut pump = fixture.pump();
    assert!(poll!(pump.as_mut()).is_pending());
    drop(cancel);
    assert!(matches!(
        poll!(pump.as_mut()),
        std::task::Poll::Ready(Ok(()))
    ));
    assert_eq!(fixture.status(), ExecutionStatus::Succeeded);
    fixture.close().await;
}

#[tokio::test]
async fn release_stops_terminal_pump_even_while_cancel_is_still_pending() {
    let fixture = Fixture::new().await;
    let mut cancel = fixture.controller.cancel(CancellationToken::new());
    assert!(poll!(cancel.as_mut()).is_pending());
    fixture.host.finish();
    let mut pump = fixture.pump();
    assert!(poll!(pump.as_mut()).is_pending());
    fixture
        .controller
        .release(CancellationToken::new())
        .await
        .unwrap();
    assert!(matches!(
        poll!(pump.as_mut()),
        std::task::Poll::Ready(Err(Error::Cancelled))
    ));
    fixture.writer.interrupt("source released").unwrap();
    assert_eq!(fixture.status(), ExecutionStatus::Interrupted);
    drop(cancel);
    fixture.close().await;
}

#[tokio::test]
async fn overlapping_cancel_drop_does_not_settle_another_pending_attempt() {
    let fixture = Fixture::new().await;
    let mut first = fixture.controller.cancel(CancellationToken::new());
    let mut second = fixture.controller.cancel(CancellationToken::new());
    assert!(poll!(first.as_mut()).is_pending());
    assert!(poll!(second.as_mut()).is_pending());
    fixture.host.finish();
    let mut pump = fixture.pump();
    assert!(poll!(pump.as_mut()).is_pending());
    drop(first);
    assert!(poll!(pump.as_mut()).is_pending());
    fixture.host.cancel_allowed.add_permits(1);
    second.await.unwrap();
    assert!(matches!(
        poll!(pump.as_mut()),
        std::task::Poll::Ready(Ok(()))
    ));
    assert_eq!(fixture.status(), ExecutionStatus::Cancelled);
    assert_eq!(fixture.host.cancel_calls.load(Ordering::Acquire), 2);
    fixture.close().await;
}

#[tokio::test]
async fn failed_cancel_can_retry_and_failed_duplicate_preserves_actual_acknowledgement() {
    let fixture = Fixture::new().await;
    fixture.host.fail_cancel.store(true, Ordering::Release);
    fixture.host.cancel_allowed.add_permits(1);
    assert!(
        fixture
            .controller
            .cancel(CancellationToken::new())
            .await
            .is_err()
    );
    assert!(!fixture.controller.cancellation_confirmed().await.unwrap());
    fixture.host.fail_cancel.store(false, Ordering::Release);
    fixture.host.cancel_allowed.add_permits(1);
    fixture
        .controller
        .cancel(CancellationToken::new())
        .await
        .unwrap();
    fixture.host.fail_cancel.store(true, Ordering::Release);
    fixture.host.cancel_allowed.add_permits(1);
    assert!(
        fixture
            .controller
            .cancel(CancellationToken::new())
            .await
            .is_err()
    );
    let mut pump = fixture.pump();
    assert!(matches!(
        poll!(pump.as_mut()),
        std::task::Poll::Ready(Ok(()))
    ));
    assert_eq!(fixture.status(), ExecutionStatus::Cancelled);
    assert_eq!(fixture.host.cancel_calls.load(Ordering::Acquire), 3);
    fixture.close().await;
}

#[tokio::test]
async fn dropping_attempt_notifies_all_registered_terminal_waiters() {
    use std::task::{Context, Wake, Waker};
    struct WakeCount(AtomicUsize);
    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }
    let fixture = Fixture::new().await;
    let mut cancel = fixture.controller.cancel(CancellationToken::new());
    assert!(poll!(cancel.as_mut()).is_pending());
    let wake_count = Arc::new(WakeCount(AtomicUsize::new(0)));
    let waker = Waker::from(wake_count.clone());
    let mut context = Context::from_waker(&waker);
    let mut first = Box::pin(fixture.controller.cancellation_confirmed());
    let mut second = Box::pin(fixture.controller.cancellation_confirmed());
    assert!(first.as_mut().poll(&mut context).is_pending());
    assert!(second.as_mut().poll(&mut context).is_pending());
    drop(cancel);
    assert_eq!(wake_count.0.load(Ordering::Acquire), 2);
    assert!(matches!(
        first.as_mut().poll(&mut context),
        std::task::Poll::Ready(Ok(false))
    ));
    assert!(matches!(
        second.as_mut().poll(&mut context),
        std::task::Poll::Ready(Ok(false))
    ));
    fixture.close().await;
}

async fn observe_execution(fixture: &Fixture, predicate: impl Fn() -> bool) {
    let mut changed = fixture.executions.subscribe();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            changed.borrow_and_update();
            if predicate() {
                break;
            }
            changed.changed().await.unwrap();
        }
    })
    .await
    .expect("real process cleanup must publish its settled ownership");
}
fn assert_process_cleanup_owned(fixture: &Fixture) {
    assert_eq!(fixture.executions.model_projection().len(), 1);
    assert_eq!(
        fixture
            .controller
            .registry
            .projection()
            .unwrap()
            .as_object()
            .unwrap()
            .len(),
        1,
        "Host acknowledgment must precede process removal"
    );
}

#[tokio::test]
async fn timed_out_real_process_release_retry_shares_pending_host_acknowledgment() {
    let fixture = Fixture::with_limits(Limits {
        lifecycle_timeout_ms: 10,
        ..Limits::default()
    })
    .await;
    fixture.host.gate_release.store(true, Ordering::Release);
    fixture.host.finish();
    fixture.writer.append(Some("stdout"), "final page").unwrap();
    fixture.writer.finish(Ok(())).unwrap();
    let id = fixture.writer.execution_id();
    let page = fixture.executions.read(id, 0, 4096).unwrap();
    assert_eq!(page.chunks[0].content, "final page");
    let mut release = Box::pin(fixture.executions.release(id, CancellationToken::new()));
    assert!(poll!(release.as_mut()).is_pending());
    fixture
        .host
        .release_entered
        .acquire()
        .await
        .unwrap()
        .forget();
    assert!(release.await.is_err());
    observe_execution(&fixture, || {
        fixture.status() == ExecutionStatus::Interrupted
    })
    .await;
    assert_process_cleanup_owned(&fixture);
    assert_eq!(fixture.host.release_calls.load(Ordering::Acquire), 1);

    // A new cleanup request must await the still-live Host worker. Polling it
    // deterministically catches the old controller's premature Ok shortcut.
    let mut retry = fixture.controller.release(CancellationToken::new());
    assert!(poll!(retry.as_mut()).is_pending());
    assert_process_cleanup_owned(&fixture);
    assert_eq!(fixture.host.release_calls.load(Ordering::Acquire), 1);
    fixture.host.release_allowed.add_permits(1);
    retry.await.unwrap();
    fixture
        .executions
        .release(id, CancellationToken::new())
        .await
        .unwrap();
    observe_execution(&fixture, || {
        fixture.executions.model_projection().is_empty()
    })
    .await;
    assert_eq!(fixture.host.release_calls.load(Ordering::Acquire), 1);
    fixture.close().await;
}

#[test]
fn real_process_cleanup_retries_after_runtime_shutdown_without_false_release() {
    let runtime_a = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let fixture = runtime_a.block_on(Fixture::with_limits(Limits {
        lifecycle_timeout_ms: 60_000,
        ..Limits::default()
    }));
    fixture.host.gate_release.store(true, Ordering::Release);
    fixture.host.finish();
    fixture.writer.append(Some("stdout"), "last page").unwrap();
    fixture.writer.finish(Ok(())).unwrap();
    let id = fixture.writer.execution_id();
    let owner = fixture.executions.clone();
    runtime_a.spawn(async move { owner.release(id, CancellationToken::new()).await });
    runtime_a.block_on(async {
        fixture
            .host
            .release_entered
            .acquire()
            .await
            .unwrap()
            .forget();
    });
    assert_process_cleanup_owned(&fixture);
    drop(runtime_a);
    assert_process_cleanup_owned(&fixture);
    let runtime_b = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime_b.block_on(async {
        let mut release = Box::pin(fixture.executions.release(id, CancellationToken::new()));
        assert!(poll!(release.as_mut()).is_pending());
        assert_eq!(
            fixture.executions.read(id, 0, 4096).unwrap().chunks[0].content,
            "last page"
        );
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            fixture.host.release_entered.acquire(),
        )
        .await
        .unwrap()
        .unwrap()
        .forget();
        assert_eq!(fixture.host.release_calls.load(Ordering::Acquire), 2);
        assert_process_cleanup_owned(&fixture);
        fixture.executions.read(id, 0, 4096).unwrap();
        assert_eq!(fixture.host.release_calls.load(Ordering::Acquire), 2);
        fixture.host.release_allowed.add_permits(1);
        release.await.unwrap();
        observe_execution(&fixture, || {
            fixture.executions.model_projection().is_empty()
        })
        .await;
        assert!(fixture.controller.registry.projection().is_none());
        assert_eq!(fixture.host.release_calls.load(Ordering::Acquire), 2);
        fixture.close().await;
    });
}

#[test]
fn interrupted_registry_close_retains_source_owner_and_retries_on_new_runtime() {
    let runtime_a = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let fixture = runtime_a.block_on(Fixture::new());
    fixture.host.gate_release.store(true, Ordering::Release);
    fixture.host.cancel_allowed.add_permits(2);
    let registry = fixture.controller.registry.clone();
    let close = runtime_a.spawn(async move { registry.close().await });
    runtime_a.block_on(async {
        fixture
            .host
            .release_entered
            .acquire()
            .await
            .unwrap()
            .forget();
        // close must retain the source mapping while its Host release is pending.
        let mut source_release = fixture.controller.release(CancellationToken::new());
        assert!(poll!(source_release.as_mut()).is_pending());
        assert_eq!(fixture.host.release_calls.load(Ordering::Acquire), 1);
        assert_process_cleanup_owned(&fixture);
    });
    drop(runtime_a);
    assert!(close.is_finished());
    assert_process_cleanup_owned(&fixture);
    let runtime_b = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime_b.block_on(async {
        let registry = fixture.controller.registry.clone();
        let close = tokio::spawn(async move { registry.close().await });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            fixture.host.release_entered.acquire(),
        )
        .await
        .unwrap()
        .unwrap()
        .forget();
        assert_process_cleanup_owned(&fixture);
        assert_eq!(fixture.host.release_calls.load(Ordering::Acquire), 2);
        let mut overlapping = Box::pin(fixture.controller.registry.close());
        assert!(poll!(overlapping.as_mut()).is_pending());
        fixture.host.release_allowed.add_permits(1);
        tokio::time::timeout(std::time::Duration::from_secs(5), close)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        overlapping.await.unwrap();
        fixture.controller.registry.close().await.unwrap();
        assert_eq!(fixture.host.release_calls.load(Ordering::Acquire), 2);
        fixture.writer.interrupt("fixture closed").unwrap();
        fixture.close().await;
    });
}

#[test]
fn dropping_real_process_registry_after_runtime_shutdown_does_not_panic() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let fixture = runtime.block_on(Fixture::new());
    let host = fixture.host.clone();
    drop(runtime);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(fixture))).is_ok());
    assert_eq!(
        host.release_calls.load(Ordering::Acquire),
        0,
        "synchronous destruction cannot acknowledge async Host cleanup"
    );
}

#[test]
fn abort_without_runtime_retains_real_process_for_explicit_close_on_new_runtime() {
    let runtime_a = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let fixture = runtime_a.block_on(Fixture::new());
    drop(runtime_a);
    fixture.controller.registry.abort();
    assert_process_cleanup_owned(&fixture);
    assert_eq!(fixture.host.release_calls.load(Ordering::Acquire), 0);
    fixture.host.cancel_allowed.add_permits(1);
    let runtime_b = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime_b.block_on(async {
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            fixture.controller.registry.close(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(fixture.host.release_calls.load(Ordering::Acquire), 1);
        assert!(fixture.controller.registry.projection().is_none());
        fixture.writer.interrupt("fixture closed").unwrap();
        fixture.close().await;
    });
}

#[test]
fn unpolled_abort_cleanup_keeps_retryable_handle_and_balances_reservation() {
    let runtime_a = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let fixture = runtime_a.block_on(Fixture::new());
    runtime_a.block_on(async {
        fixture.controller.registry.abort();
    });
    assert_eq!(fixture.host.cancel_calls.load(Ordering::Acquire), 0);
    drop(runtime_a);
    fixture.host.cancel_allowed.add_permits(1);
    let runtime_b = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime_b.block_on(async {
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            fixture.controller.registry.close(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(fixture.host.release_calls.load(Ordering::Acquire), 1);
        fixture.writer.interrupt("fixture closed").unwrap();
        fixture.close().await;
    });
}

#[tokio::test]
async fn capture_loss_marks_first_sticky_batch_and_fresh_native_gap_only() {
    let fixture = Fixture::new().await;
    for (stdout, gap) in [("first", 2), ("second", 0), ("third", 4)] {
        fixture
            .host
            .captures
            .lock()
            .unwrap()
            .push_back(ShellOutput {
                stdout: stdout.into(),
                stderr: String::new(),
                next: ShellPosition::default(),
                cursor: ShellPosition::default(),
                truncated: true,
                expired: (gap != 0)
                    .then_some(microcrab_host_api::ExpiredRange { through_line: gap }),
            });
    }
    fixture.host.finish();
    fixture.pump().await.unwrap();
    let page = fixture
        .executions
        .read(fixture.writer.execution_id(), 0, 4096)
        .unwrap();
    let text: String = page
        .chunks
        .iter()
        .map(|chunk| chunk.content.as_str())
        .collect();
    assert_eq!(text, "[upstream lost]\nfirstsecond\n[upstream lost]\nthird");
    assert_eq!(page.chunks[0].channel, None);
    assert_eq!(page.chunks[1].channel.as_deref(), Some("stdout"));
    assert_eq!(text.matches("[upstream lost]").count(), 2);
    fixture.close().await;
}
