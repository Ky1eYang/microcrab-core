//! Reads retain output; explicit release waits for cleanup acknowledgment.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Semaphore;

struct CleanupGate {
    entered: Semaphore,
    allowed: Semaphore,
    calls: AtomicUsize,
    fail_first: bool,
}
impl CleanupGate {
    fn new(fail_first: bool) -> Arc<Self> {
        Arc::new(Self {
            entered: Semaphore::new(0),
            allowed: Semaphore::new(0),
            calls: AtomicUsize::new(0),
            fail_first,
        })
    }
    async fn entered(&self) {
        self.entered.acquire().await.unwrap().forget();
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
        self.allowed.acquire().await.unwrap().forget();
        if self.fail_first && call == 0 {
            Err(Error::Lifecycle("cleanup rejected".into()))
        } else {
            Ok(())
        }
    }
}
fn row() -> ExecutionSnapshot {
    ExecutionSnapshot::new(
        "owner".into(),
        "call".into(),
        "stream".into(),
        "agent".into(),
        None,
    )
}
async fn buffered(
    gate: Arc<CleanupGate>,
    content: &str,
    terminal: bool,
) -> (Arc<ExecutionRegistry>, ExecutionId, OutputWriter) {
    let registry = ExecutionRegistry::new(Limits {
        max_execution_buffers: 1,
        lifecycle_timeout_ms: 60_000,
        ..Limits::default()
    });
    let id = registry
        .admit_detached(row(), CancellationToken::new())
        .unwrap();
    let (writer, output) = registry
        .open_output(id, OutputOptions::default(), gate)
        .unwrap();
    writer.append(Some("stdout"), content).unwrap();
    if terminal {
        writer.finish(Ok(())).unwrap();
    }
    registry
        .resolve(id, Ok(ToolOutput::Buffered(output)))
        .await
        .unwrap();
    (registry, id, writer)
}
fn assert_owned(registry: &ExecutionRegistry, id: ExecutionId) {
    let (rows, capacity) = registry.model_observation();
    assert!(rows.iter().any(|row| row.execution_id == id));
    assert_eq!(capacity.buffers.used, 1);
    assert_eq!(capacity.buffers.remaining, capacity.buffers.limit - 1);
    let state = registry.state.lock().unwrap();
    let entry = &state.entries[&id];
    assert!(!entry.released);
    assert!(entry.controller.is_some());
}
fn assert_released(registry: &ExecutionRegistry, id: ExecutionId) {
    let (rows, capacity) = registry.model_observation();
    assert!(!rows.iter().any(|row| row.execution_id == id));
    assert_eq!(capacity.buffers.used, 0);
    assert_eq!(capacity.buffers.remaining, capacity.buffers.limit);
    let state = registry.state.lock().unwrap();
    let entry = &state.entries[&id];
    assert!(entry.released);
    assert!(entry.controller.is_none());
}

#[tokio::test]
async fn explicit_release_preserves_output_until_cleanup_ack() {
    let gate = CleanupGate::new(false);
    let (registry, id, _writer) = buffered(gate.clone(), "older中🙂z", true).await;
    let page = registry.read_capped(id, 0, 4096, Some(6)).unwrap();
    assert_eq!(page.chunks[0].content, "older");
    assert_eq!(page.start_cursor, 0);
    let owner = registry.clone();
    let task = tokio::spawn(async move { owner.release(id, CancellationToken::new()).await });
    gate.entered().await;
    assert_owned(&registry, id);
    assert_eq!(registry.read(id, 0, 4096).unwrap().end_cursor, 13);
    assert_eq!(
        registry.read(id, 0, 4096).unwrap().chunks[0].content,
        "older中🙂z"
    );
    gate.allowed.add_permits(1);
    task.await.unwrap().unwrap();
    assert_released(&registry, id);
    let page = registry.read(id, 0, 4096).unwrap();
    assert_eq!(page.start_cursor, page.end_cursor);
    assert!(page.chunks.is_empty());
    ExecutionRegistry::validate_rows(&registry.recovery_snapshot(), &Limits::default()).unwrap();
    registry.close().await.unwrap();
}
#[tokio::test]
async fn terminal_paginated_reads_keep_capacity_until_explicit_release_ack() {
    let gate = CleanupGate::new(false);
    let content = "中🙂\n\"\\".repeat(80);
    let (registry, id, writer) = buffered(gate.clone(), &content, true).await;
    let next = registry
        .admit_detached(row(), CancellationToken::new())
        .unwrap();
    let mut cursor = 0;
    let mut actual = String::new();
    while cursor < content.len() as u64 {
        let page = registry.read(id, cursor, 350).unwrap();
        assert!(page.next_cursor > cursor);
        assert!(serde_json::to_vec(&page).unwrap().len() <= 350);
        cursor = page.next_cursor;
        actual.extend(page.chunks.into_iter().map(|chunk| chunk.content));
    }
    assert_eq!(actual, content);
    assert_owned(&registry, id);
    assert_eq!(gate.calls.load(Ordering::SeqCst), 0);
    assert!(writer.append(None, "late").is_err());
    assert!(
        registry
            .open_output(next, OutputOptions::default(), gate.clone())
            .is_err()
    );
    let owner = registry.clone();
    let task = tokio::spawn(async move { owner.release(id, CancellationToken::new()).await });
    gate.entered().await;
    assert!(
        registry
            .open_output(next, OutputOptions::default(), gate.clone())
            .is_err()
    );
    gate.allowed.add_permits(1);
    task.await.unwrap().unwrap();
    assert_released(&registry, id);
    assert_eq!(
        registry.get(id).unwrap().output.unwrap().end_cursor,
        content.len() as u64
    );
    let (writer, output) = registry
        .open_output(next, OutputOptions::default(), gate.clone())
        .unwrap();
    writer.finish(Ok(())).unwrap();
    registry
        .resolve(next, Ok(ToolOutput::Buffered(output)))
        .await
        .unwrap();
    gate.allowed.add_permits(1);
    registry.close().await.unwrap();
}
#[tokio::test]
async fn terminal_read_and_initial_receipt_never_authorize_release() {
    let gate = CleanupGate::new(false);
    let (registry, id, _writer) = buffered(gate.clone(), "retained", true).await;
    let before = serde_json::to_vec(&registry.recovery_snapshot()).unwrap();
    assert_eq!(
        registry.read(id, 0, 4096).unwrap().chunks[0].content,
        "retained"
    );
    assert!(registry.receipt(id, true).unwrap().contains("retained"));
    assert_eq!(
        serde_json::to_vec(&registry.recovery_snapshot()).unwrap(),
        before
    );
    assert_owned(&registry, id);
    assert_eq!(gate.calls.load(Ordering::SeqCst), 0);
    gate.allowed.add_permits(1);
    registry.close().await.unwrap();
}
#[tokio::test]
async fn pending_and_terminal_empty_reads_never_release() {
    let gate = CleanupGate::new(false);
    let (registry, id, writer) = buffered(gate.clone(), "", false).await;
    assert!(!registry.read(id, 0, 4096).unwrap().closed);
    let (rows, capacity) = registry.model_observation();
    assert_eq!(rows[0].execution_id, id);
    assert_eq!(capacity.buffers.used, 1);
    // A running, adopted Buffered receipt charges the buffer quota only.
    assert_eq!(capacity.pending.used, 0);
    assert!(
        registry
            .release(id, CancellationToken::new())
            .await
            .is_err()
    );
    writer.finish(Ok(())).unwrap();
    assert!(registry.read(id, 0, 4096).unwrap().closed);
    assert_owned(&registry, id);
    assert_eq!(registry.model_observation().1.pending.used, 0);
    assert_eq!(gate.calls.load(Ordering::SeqCst), 0);
    gate.allowed.add_permits(1);
    registry
        .release(id, CancellationToken::new())
        .await
        .unwrap();
    assert_released(&registry, id);
    registry.close().await.unwrap();
}
#[tokio::test]
async fn metadata_only_read_keeps_producer_and_terminal_bytes() {
    let gate = CleanupGate::new(false);
    let (registry, id, writer) = buffered(gate.clone(), "first", false).await;
    let first = registry.read(id, 0, 4096).unwrap();
    writer.append(Some("stdout"), "last").unwrap();
    writer.finish(Ok(())).unwrap();
    let mut envelope = registry.read(id, first.next_cursor, 4096).unwrap();
    envelope.chunks.clear();
    envelope.next_cursor = envelope.cursor;
    let maximum = serde_json::to_vec(&envelope).unwrap().len();
    let empty = registry.read(id, first.next_cursor, maximum).unwrap();
    assert!(empty.chunks.is_empty());
    assert_eq!(empty.next_cursor, first.next_cursor);
    assert_eq!(
        registry.read(id, first.next_cursor, 4096).unwrap().chunks[0].content,
        "last"
    );
    assert_owned(&registry, id);
    assert_eq!(gate.calls.load(Ordering::SeqCst), 0);
    gate.allowed.add_permits(1);
    registry
        .release(id, CancellationToken::new())
        .await
        .unwrap();
}
#[tokio::test]
async fn failed_cleanup_retains_bytes_and_owner_until_explicit_retry() {
    let gate = CleanupGate::new(true);
    let (registry, id, writer) = buffered(gate.clone(), "last page", true).await;
    gate.allowed.add_permits(1);
    assert!(
        registry
            .release(id, CancellationToken::new())
            .await
            .is_err()
    );
    assert_owned(&registry, id);
    assert!(matches!(registry.restore(&[]), Err(Error::Busy)));
    let failed = registry.get(id).unwrap();
    assert_eq!(failed.status, ExecutionStatus::Interrupted);
    assert!(failed.diagnostic.is_some());
    assert_eq!(
        registry.read(id, 0, 4096).unwrap().chunks[0].content,
        "last page"
    );
    assert!(writer.append(None, "late").is_err());
    gate.allowed.add_permits(1);
    registry
        .release(id, CancellationToken::new())
        .await
        .unwrap();
    assert_released(&registry, id);
    assert_eq!(gate.calls.load(Ordering::SeqCst), 2);
    assert_eq!(registry.get(id).unwrap().diagnostic, failed.diagnostic);
}
#[tokio::test]
async fn explicit_release_reuses_inflight_restore_cleanup() {
    let gate = CleanupGate::new(false);
    let (registry, id, _writer) = buffered(gate.clone(), "final", true).await;
    let owner = registry.clone();
    let restoring = tokio::spawn(async move { owner.release_for_restore().await });
    gate.entered().await;
    let owner = registry.clone();
    let releasing = tokio::spawn(async move { owner.release(id, CancellationToken::new()).await });
    assert_eq!(
        registry.read(id, 0, 4096).unwrap().chunks[0].content,
        "final"
    );
    gate.allowed.add_permits(1);
    restoring.await.unwrap().unwrap();
    releasing.await.unwrap().unwrap();
    assert_released(&registry, id);
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn finish_error_cleanup_retains_output_until_explicit_release() {
    let gate = CleanupGate::new(false);
    let (registry, id, writer) = buffered(gate.clone(), "partial failure output", false).await;
    let owner = registry.clone();
    let finishing = tokio::spawn(async move {
        owner
            .finish_error(id, &Error::Tool("producer failed".into()))
            .await;
    });
    gate.entered().await;
    assert_eq!(
        registry.read(id, 0, 4096).unwrap().chunks[0].content,
        "partial failure output"
    );
    assert!(writer.append(None, "late").is_err());
    gate.allowed.add_permits(1);
    finishing.await.unwrap();
    assert!(!registry.state.lock().unwrap().entries[&id].released);
    registry
        .release(id, CancellationToken::new())
        .await
        .unwrap();
    assert_released(&registry, id);
}
#[tokio::test]
async fn dropped_release_waiter_does_not_cancel_or_duplicate_cleanup() {
    let gate = CleanupGate::new(false);
    let (registry, id, _writer) = buffered(gate.clone(), "final", true).await;
    let mut waiting = Box::pin(registry.release(id, CancellationToken::new()));
    assert!(futures_util::poll!(waiting.as_mut()).is_pending());
    gate.entered().await;
    drop(waiting);
    assert_owned(&registry, id);
    assert_eq!(
        registry.read(id, 0, 4096).unwrap().chunks[0].content,
        "final"
    );
    gate.allowed.add_permits(1);
    registry
        .release(id, CancellationToken::new())
        .await
        .unwrap();
    assert_released(&registry, id);
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
}
#[test]
fn stopped_cleanup_runtime_retries_on_new_runtime_without_duplicate_release() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let gate = CleanupGate::new(false);
    let (registry, id, _writer) = runtime.block_on(buffered(gate.clone(), "final", true));
    runtime.block_on(async {
        let mut waiting = Box::pin(registry.release(id, CancellationToken::new()));
        assert!(futures_util::poll!(waiting.as_mut()).is_pending());
        gate.entered().await;
    });
    drop(runtime);
    assert_owned(&registry, id);
    assert_eq!(
        registry.read(id, 0, 4096).unwrap().chunks[0].content,
        "final"
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        gate.allowed.add_permits(1);
        registry
            .release(id, CancellationToken::new())
            .await
            .unwrap();
        assert_released(&registry, id);
        assert_eq!(gate.calls.load(Ordering::SeqCst), 2);
    });
}
#[test]
fn read_without_runtime_preserves_final_page_and_cleanup_owner() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let gate = CleanupGate::new(false);
    let (registry, id, _writer) = runtime.block_on(buffered(gate.clone(), "last", true));
    drop(runtime);
    for _ in 0..2 {
        assert_eq!(
            registry.read(id, 0, 4096).unwrap().chunks[0].content,
            "last"
        );
    }
    assert_owned(&registry, id);
    assert_eq!(gate.calls.load(Ordering::SeqCst), 0);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        gate.allowed.add_permits(1);
        registry
            .release(id, CancellationToken::new())
            .await
            .unwrap();
    });
    assert_released(&registry, id);
}
#[test]
fn controller_free_empty_read_budgets_revision_without_releasing() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let gate = CleanupGate::new(false);
    let (registry, id, _writer) = runtime.block_on(buffered(gate.clone(), "", true));
    gate.allowed.add_permits(1);
    runtime.block_on(registry.release_for_restore()).unwrap();
    drop(runtime);
    let page = registry.read(id, 0, 4096).unwrap();
    let maximum = serde_json::to_vec(&page).unwrap().len();
    assert!(registry.read(id, 0, maximum - 1).is_err());
    assert_eq!(
        serde_json::to_vec(&registry.read(id, 0, maximum).unwrap())
            .unwrap()
            .len(),
        maximum
    );
    assert!(!registry.state.lock().unwrap().entries[&id].released);
}

#[tokio::test]
async fn final_source_and_producer_elapsed_are_visible_before_slow_cleanup_ack() {
    let gate = CleanupGate::new(false);
    let registry = ExecutionRegistry::new(Limits {
        max_execution_buffers: 1,
        max_pending_executions: 1,
        ..Limits::default()
    });
    let id = registry
        .admit_detached(row(), CancellationToken::new())
        .unwrap();
    let (writer, mut output) = registry
        .open_output(id, OutputOptions::default(), gate.clone())
        .unwrap();
    // A prepared writer can be owned by native code while Final resolves.
    output.adopted = true;
    let resolving = registry.clone();
    let task = tokio::spawn(async move {
        resolving
            .resolve(id, Ok(ToolOutput::text("final retained text")))
            .await
    });
    gate.entered().await;
    let pending = registry.get(id).unwrap();
    assert!(pending.status.is_pending());
    let elapsed = pending.elapsed_ms;
    assert!(elapsed.is_some());
    let page = registry
        .read_content(
            id,
            super::super::model::ExecutionContentField::Result,
            0,
            1024,
        )
        .unwrap();
    assert_eq!(page.content, "final retained text");
    assert!(page.available);
    // This hidden producer charges both admission quotas without exposing a handle.
    let (rows, capacity) = registry.model_observation();
    assert!(rows.is_empty());
    assert_eq!(capacity.buffers.used, 1);
    assert_eq!(capacity.pending.used, 1);
    assert_eq!(capacity.pending.remaining, 0);
    assert!(
        registry
            .admit_detached(row(), CancellationToken::new())
            .is_err()
    );
    assert!(
        registry
            .start(row(), CancellationToken::new(), |_| async {
                Ok(ToolOutput::text("must not run"))
            })
            .is_err()
    );
    {
        let state = registry.state.lock().unwrap();
        let entry = &state.entries[&id];
        assert!(!entry.exposed);
        assert!(!entry.released);
        assert!(entry.controller.is_some());
    }
    assert!(writer.append(None, "late").is_err());
    gate.allowed.add_permits(1);
    task.await.unwrap().unwrap();
    assert_eq!(registry.get(id).unwrap().elapsed_ms, elapsed);
    assert_eq!(registry.get(id).unwrap().status, ExecutionStatus::Succeeded);
    assert_released(&registry, id);
    assert_eq!(registry.model_observation().1.pending.remaining, 1);
    registry.close().await.unwrap();
}

#[tokio::test]
async fn dropped_replacement_waiter_retains_cleanup_and_output_without_sealing_registry() {
    let gate = CleanupGate::new(false);
    let (registry, id, _writer) = buffered(gate.clone(), "retained replacement bytes", false).await;
    let mut replacing = Box::pin(registry.quiesce_for_restore());
    assert!(futures_util::poll!(replacing.as_mut()).is_pending());
    gate.entered().await;
    drop(replacing);
    assert_eq!(
        registry.read(id, 0, 4096).unwrap().chunks[0].content,
        "retained replacement bytes"
    );
    assert!(matches!(registry.restore(&[]), Err(Error::Busy)));
    gate.allowed.add_permits(1);
    registry.quiesce_for_restore().await.unwrap();
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        registry.get(id).unwrap().output_availability,
        Some(super::super::OutputAvailability::Retained)
    );
    registry.restore(&[]).unwrap();
    registry
        .admit_detached(row(), CancellationToken::new())
        .unwrap();
    registry.close().await.unwrap();
}
#[tokio::test]
async fn replacement_cleanup_failure_preserves_owner_and_can_retry() {
    let gate = CleanupGate::new(true);
    let (registry, id, _writer) = buffered(gate.clone(), "retained", false).await;
    gate.allowed.add_permits(1);
    assert!(registry.quiesce_for_restore().await.is_err());
    assert_owned(&registry, id);
    assert_eq!(
        registry.read(id, 0, 4096).unwrap().chunks[0].content,
        "retained"
    );
    assert!(matches!(registry.restore(&[]), Err(Error::Busy)));
    gate.allowed.add_permits(1);
    registry.quiesce_for_restore().await.unwrap();
    assert_eq!(gate.calls.load(Ordering::SeqCst), 2);
    registry.restore(&[]).unwrap();
    registry.close().await.unwrap();
}
