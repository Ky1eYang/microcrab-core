//! Regression tests for restart retention and acknowledged cleanup ownership.
use crate::{
    Error, Limits, SessionState,
    runtime::{execution::*, tool::ToolContext},
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio_util::sync::CancellationToken;
fn descriptor() -> ExecutionSnapshot {
    ExecutionSnapshot::new(
        "owner".into(),
        "call".into(),
        "producer".into(),
        "agent".into(),
        None,
    )
}

#[tokio::test]
async fn execution_retention_uses_its_cap_independently_of_process_history() {
    for (execution_cap, process_cap) in [(2, 0), (0, 2)] {
        let registry = ExecutionRegistry::new(Limits {
            max_retained_executions: execution_cap,
            max_retained_processes: process_cap,
            ..Limits::default()
        });
        let mut newest = None;
        for index in 0..4 {
            let id = registry
                .admit_detached(descriptor(), CancellationToken::new())
                .unwrap();
            registry
                .complete_detached(
                    id,
                    FinalOutput {
                        content: format!("completed-{index}"),
                        failure: None,
                        attachments: Vec::new(),
                    },
                )
                .await
                .unwrap();
            newest = Some(id);
        }
        let rows = registry.snapshot();
        assert_eq!(rows.len(), execution_cap.max(1));
        let newest = newest.unwrap();
        let row = registry.get(newest).unwrap();
        let projection = row.status_projection();
        assert_eq!(projection.kind, ExecutionKind::Invocation);
        assert!(!projection.is_pending());
        assert!(projection.output_closed);
        assert!(row.result.is_none());
        assert_eq!(row.result_available, Some(true));
        let content = registry
            .read_content(newest, ExecutionContentField::Result, 0, 4096)
            .unwrap();
        assert!(content.available);
        assert_eq!(content.content, "completed-3");
        assert_eq!(content.next_cursor, content.end_cursor);
        // The shared observation carries no identity, and the existing public
        // recovery DTO retains only its established identity and status fields.
        let serialized = serde_json::to_value(row).unwrap();
        assert_eq!(serialized["status"], "succeeded");
        assert!(serialized.get("kind").is_none());
        assert!(serialized.get("pid").is_none());
        registry.close().await.unwrap();
    }
}

#[tokio::test]
async fn full_resource_quota_preserves_control_invocation_slots() {
    let owner = ExecutionRegistry::new(Limits {
        max_pending_executions: 1,
        max_retained_executions: 0,
        max_execution_buffers: 1,
        ..Limits::default()
    });
    let registry = owner.clone();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let mut producer = owner
        .start(
            descriptor(),
            CancellationToken::new(),
            move |progress| async move {
                let (writer, output) = registry.open_output(
                    progress.execution_id(),
                    OutputOptions::default(),
                    Arc::new(CancellationController::new(CancellationToken::new())),
                )?;
                sender.send(writer).map_err(|_| Error::Closed)?;
                Ok(ToolOutput::Buffered(output))
            },
        )
        .unwrap();
    let id = producer.id;
    producer.activate().unwrap();
    producer.wait().await.unwrap();
    producer.detach();
    let writer = receiver.await.unwrap();
    let registry = owner.clone();
    let mut control = owner
        .start(
            descriptor(),
            CancellationToken::new(),
            move |_| async move {
                assert_eq!(registry.get(id)?.status, ExecutionStatus::Running);
                registry.cancel(id).await?;
                Ok(ToolOutput::text("requested"))
            },
        )
        .unwrap();
    control.activate().unwrap();
    control.wait().await.unwrap();
    control.detach();
    writer.confirm_cancelled().unwrap();
    let registry = owner.clone();
    let mut release = owner
        .start(
            descriptor(),
            CancellationToken::new(),
            move |_| async move {
                registry.release(id, CancellationToken::new()).await?;
                Ok(ToolOutput::text("released"))
            },
        )
        .unwrap();
    release.activate().unwrap();
    release.wait().await.unwrap();
    release.detach();
    let registry = owner.clone();
    let mut next = owner
        .start(
            descriptor(),
            CancellationToken::new(),
            move |progress| async move {
                let (writer, output) = registry.open_output(
                    progress.execution_id(),
                    OutputOptions::default(),
                    Arc::new(CancellationController::new(CancellationToken::new())),
                )?;
                writer.finish(Ok(()))?;
                Ok(ToolOutput::Buffered(output))
            },
        )
        .unwrap();
    next.activate().unwrap();
    next.wait().await.unwrap();
    next.detach();
    owner.close().await.unwrap();
}

#[tokio::test]
async fn pending_recovery_receipt_gets_terminal_model_transition_without_output() {
    let owner = ExecutionRegistry::new(Limits::default());
    let mut row = descriptor();
    row.execution_id = ExecutionId(1);
    row.delivery = Some(ExecutionDelivery {
        exposed: false,
        released: true,
    });
    owner.restore(&[row]).unwrap();
    let projected = owner.model_projection();
    assert_eq!(projected.len(), 1);
    assert_eq!(projected[0].status, ExecutionStatus::Interrupted);
    assert!(projected[0].output.is_none());
    owner
        .release(ExecutionId(1), CancellationToken::new())
        .await
        .unwrap();
    assert!(owner.model_projection().is_empty());
    owner.close().await.unwrap();
}

#[tokio::test]
async fn restored_unread_buffer_remains_visible_and_survives_history_pruning() {
    let limits = Limits {
        max_execution_buffers: 1,
        max_retained_executions: 1,
        ..Default::default()
    };
    let owner = ExecutionRegistry::new(limits.clone());
    let registry = owner.clone();
    let mut task = owner
        .start(
            descriptor(),
            CancellationToken::new(),
            move |progress| async move {
                let (writer, output) = registry.open_output(
                    progress.execution_id(),
                    OutputOptions::default(),
                    Arc::new(CancellationController::new(CancellationToken::new())),
                )?;
                writer.append(None, "unread")?;
                writer.finish(Ok(()))?;
                Ok(ToolOutput::Buffered(output))
            },
        )
        .unwrap();
    let id = task.id;
    task.activate().unwrap();
    task.wait().await.unwrap();
    task.detach();
    let checkpoint = owner.recovery_snapshot();
    owner.close().await.unwrap();
    let restored = ExecutionRegistry::new(limits);
    restored.restore(&checkpoint).unwrap();
    let (rows, capacity) = restored.model_observation();
    assert_eq!(rows[0].execution_id, id);
    assert_eq!(
        serde_json::to_value(capacity).unwrap()["buffers"],
        serde_json::json!({"limit": 1, "used": 1, "remaining": 0})
    );
    for _ in 0..4 {
        let mut task = restored
            .start(descriptor(), CancellationToken::new(), |_| async {
                Ok(ToolOutput::text("final"))
            })
            .unwrap();
        task.activate().unwrap();
        task.wait().await.unwrap();
        task.detach();
    }
    assert_eq!(
        restored.read(id, 0, 4096).unwrap().chunks[0].content,
        "unread"
    );
    assert!(
        restored
            .model_projection()
            .iter()
            .any(|row| row.execution_id == id)
    );
    restored
        .release(id, CancellationToken::new())
        .await
        .unwrap();
    assert!(
        !restored
            .model_projection()
            .iter()
            .any(|row| row.execution_id == id)
    );
    restored.close().await.unwrap();
}

struct CleanupGate {
    entered: tokio::sync::Semaphore,
    allowed: tokio::sync::Semaphore,
    calls: AtomicUsize,
    fail_first: bool,
}
impl CleanupGate {
    fn new(fail_first: bool) -> Arc<Self> {
        Arc::new(Self {
            entered: tokio::sync::Semaphore::new(0),
            allowed: tokio::sync::Semaphore::new(0),
            calls: AtomicUsize::new(0),
            fail_first,
        })
    }
}
#[async_trait::async_trait]
impl ExecutionController for CleanupGate {
    async fn cancel(&self, _: CancellationToken) -> Result<(), Error> {
        Ok(())
    }
    async fn release(&self, _: CancellationToken) -> Result<(), Error> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.add_permits(1);
        self.allowed
            .acquire()
            .await
            .map_err(|_| Error::Closed)?
            .forget();
        if self.fail_first && call == 0 {
            Err(Error::Lifecycle("release rejected".into()))
        } else {
            Ok(())
        }
    }
}
#[tokio::test]
async fn actual_prepared_token_drop_waits_for_cleanup_before_final_success() {
    let owner = ExecutionRegistry::new(Limits::default());
    let registry = owner.clone();
    let gate = CleanupGate::new(false);
    let controller = gate.clone();
    let mut task = owner
        .start(
            descriptor(),
            CancellationToken::new(),
            move |progress| async move {
                let (writer, output) = registry.open_output(
                    progress.execution_id(),
                    OutputOptions::default(),
                    controller,
                )?;
                drop(output);
                drop(writer);
                Ok(ToolOutput::text("done"))
            },
        )
        .unwrap();
    let id = task.id;
    task.activate().unwrap();
    gate.entered.acquire().await.unwrap().forget();
    assert_eq!(owner.get(id).unwrap().status, ExecutionStatus::Running);
    gate.allowed.add_permits(1);
    assert_eq!(task.wait().await.unwrap(), "done");
    task.detach();
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
    owner.close().await.unwrap();
}
#[tokio::test]
async fn held_cleanup_expires_without_claiming_a_tool_timeout_and_can_retry() {
    let owner = ExecutionRegistry::new(Limits {
        lifecycle_timeout_ms: 10,
        ..Limits::default()
    });
    let registry = owner.clone();
    let gate = CleanupGate::new(false);
    let controller = gate.clone();
    let mut task = owner
        .start(
            descriptor(),
            CancellationToken::new(),
            move |progress| async move {
                let (writer, output) = registry.open_output(
                    progress.execution_id(),
                    OutputOptions::default(),
                    controller,
                )?;
                drop(output);
                drop(writer);
                Ok(ToolOutput::text("done"))
            },
        )
        .unwrap();
    let id = task.id;
    task.activate().unwrap();
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(2), task.wait())
            .await
            .unwrap(),
        Err(Error::Diagnostic(diagnostic)) if diagnostic.reason.is_some()
    ));
    task.detach();
    let row = owner.get(id).unwrap();
    assert_eq!(row.status, ExecutionStatus::Interrupted);
    assert!(row.failure.is_none());
    assert!(matches!(owner.restore(&[]), Err(Error::Busy)));
    gate.allowed.add_permits(1);
    owner.close().await.unwrap();
    assert_eq!(gate.calls.load(Ordering::SeqCst), 2);
}

struct HeldCancellation {
    released: std::sync::atomic::AtomicBool,
}
#[async_trait::async_trait]
impl ExecutionController for HeldCancellation {
    async fn cancel(&self, _: CancellationToken) -> Result<(), Error> {
        std::future::pending().await
    }
    async fn release(&self, _: CancellationToken) -> Result<(), Error> {
        self.released.store(true, Ordering::SeqCst);
        Ok(())
    }
}
#[tokio::test]
async fn held_cancellation_does_not_prevent_confirmed_close_cleanup() {
    let owner = ExecutionRegistry::new(Limits {
        lifecycle_timeout_ms: 10,
        ..Limits::default()
    });
    let registry = owner.clone();
    let controller = Arc::new(HeldCancellation {
        released: std::sync::atomic::AtomicBool::new(false),
    });
    let source = controller.clone();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let mut task = owner
        .start(
            descriptor(),
            CancellationToken::new(),
            move |progress| async move {
                let (writer, output) = registry.open_output(
                    progress.execution_id(),
                    OutputOptions::default(),
                    source,
                )?;
                sender.send(writer).map_err(|_| Error::Closed)?;
                Ok(ToolOutput::Buffered(output))
            },
        )
        .unwrap();
    let id = task.id;
    task.activate().unwrap();
    task.wait().await.unwrap();
    task.detach();
    let _writer = receiver.await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), owner.close())
        .await
        .unwrap()
        .unwrap();
    assert!(controller.released.load(Ordering::SeqCst));
    let row = owner.get(id).unwrap();
    assert_eq!(row.status, ExecutionStatus::Interrupted);
    assert!(row.failure.is_none());
}

#[tokio::test]
async fn failed_cleanup_keeps_resource_owner_for_close_retry() {
    let owner = ExecutionRegistry::new(Limits::default());
    let registry = owner.clone();
    let gate = CleanupGate::new(true);
    let controller = gate.clone();
    gate.allowed.add_permits(1);
    let mut task = owner
        .start(
            descriptor(),
            CancellationToken::new(),
            move |progress| async move {
                let (writer, output) = registry.open_output(
                    progress.execution_id(),
                    OutputOptions::default(),
                    controller,
                )?;
                drop(output);
                drop(writer);
                Ok(ToolOutput::text("done"))
            },
        )
        .unwrap();
    let id = task.id;
    task.activate().unwrap();
    assert!(matches!(
        task.wait().await,
        Err(Error::Diagnostic(diagnostic)) if diagnostic.reason.is_some()
    ));
    task.detach();
    assert_eq!(owner.get(id).unwrap().status, ExecutionStatus::Interrupted);
    assert!(matches!(owner.restore(&[]), Err(Error::Busy)));
    gate.allowed.add_permits(1);
    owner.close().await.unwrap();
    assert_eq!(gate.calls.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn unused_direct_prepared_output_does_not_cancel_the_shared_caller() {
    let cancellation = CancellationToken::new();
    let base = ToolContext::detached(SessionState::new(Default::default()), cancellation.clone());
    let mut first = base.clone();
    first.ensure_detached_execution().unwrap();
    let (writer, output) = first
        .open_output(
            OutputOptions::default(),
            Arc::new(CancellationController::new(CancellationToken::new())),
        )
        .unwrap();
    drop(output);
    drop(writer);
    first
        .complete_detached_output(&ToolOutput::text("first"))
        .await
        .unwrap();
    assert!(!cancellation.is_cancelled());
    let mut second = base.clone();
    second.ensure_detached_execution().unwrap();
    assert_ne!(first.execution_id, second.execution_id);
    second
        .complete_detached_output(&ToolOutput::text("second"))
        .await
        .unwrap();
    assert!(!cancellation.is_cancelled());
    base.state.executions().close().await.unwrap();
    assert!(!cancellation.is_cancelled());
}

#[tokio::test]
async fn direct_execution_cancellation_is_isolated_from_caller_and_siblings() {
    let cancellation = CancellationToken::new();
    let base = ToolContext::detached(SessionState::new(Default::default()), cancellation.clone());
    let mut first = base.clone();
    let mut second = base.clone();
    first.ensure_detached_execution().unwrap();
    second.ensure_detached_execution().unwrap();
    let first_id = first.execution_id.unwrap();
    let second_id = second.execution_id.unwrap();
    assert_ne!(first_id, second_id);

    base.state.executions().cancel(first_id).await.unwrap();
    assert!(first.cancellation.is_cancelled());
    assert!(!second.cancellation.is_cancelled());
    assert!(!cancellation.is_cancelled());
    assert_eq!(
        base.state.executions().get(second_id).unwrap().status,
        ExecutionStatus::Running
    );

    cancellation.cancel();
    assert!(second.cancellation.is_cancelled());
    first
        .fail_detached_execution(&Error::Cancelled)
        .await
        .unwrap();
    second
        .fail_detached_execution(&Error::Cancelled)
        .await
        .unwrap();
    base.state.executions().close().await.unwrap();
}

#[tokio::test]
async fn direct_buffer_owns_output_after_context_is_dropped() {
    let mut context = ToolContext::detached(
        SessionState::new(Default::default()),
        CancellationToken::new(),
    );
    context.ensure_detached_execution().unwrap();
    let (writer, output) = context
        .open_output(
            OutputOptions::default(),
            Arc::new(CancellationController::new(CancellationToken::new())),
        )
        .unwrap();
    drop(context);
    writer.append(None, "retained beyond invoke").unwrap();
    writer.finish(Ok(())).unwrap();
    drop(writer);
    assert_eq!(
        output.snapshot(4096).unwrap().chunks[0].content,
        "retained beyond invoke"
    );
}
