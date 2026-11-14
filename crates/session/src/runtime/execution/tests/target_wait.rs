//! Target waits ignore unrelated history and output notifications.
use super::*;
use crate::{Error, Limits};
use std::{sync::Arc, time::Duration};
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
struct MutableInput(std::sync::atomic::AtomicBool);
#[async_trait::async_trait]
impl ExecutionController for MutableInput {
    async fn cancel(&self, _: CancellationToken) -> Result<(), Error> {
        Ok(())
    }
    async fn release(&self, _: CancellationToken) -> Result<(), Error> {
        Ok(())
    }
    fn capabilities(&self) -> ExecutionCapabilities {
        ExecutionCapabilities {
            input: self.0.load(std::sync::atomic::Ordering::SeqCst),
        }
    }
}
#[tokio::test]
async fn wait_metadata_matches_status_for_live_and_released_controller_capabilities() {
    let registry = ExecutionRegistry::new(Limits::default());
    let id = registry
        .admit_detached(descriptor(), CancellationToken::new())
        .unwrap();
    let controller = Arc::new(MutableInput(std::sync::atomic::AtomicBool::new(false)));
    let (writer, output) = registry
        .open_output(id, OutputOptions::default(), controller.clone())
        .unwrap();
    let page = output.snapshot(4096).unwrap();
    controller
        .0
        .store(true, std::sync::atomic::Ordering::SeqCst);
    writer.append(None, "capability now available").unwrap();
    let (_, row) = registry
        .wait_for_execution_change(id, Some(page.revision), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        row.capabilities.input,
        registry.get(id).unwrap().capabilities.input
    );
    assert!(row.capabilities.input);
    writer.finish(Ok(())).unwrap();
    registry
        .release(id, CancellationToken::new())
        .await
        .unwrap();
    let (_, row) = registry
        .wait_for_execution_change(id, None, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        row.capabilities.input,
        registry.get(id).unwrap().capabilities.input
    );
    assert!(!row.capabilities.input);
    drop(output);
    registry.close().await.unwrap();
}

#[tokio::test]
async fn read_revision_waits_for_next_target_event_without_mutation() {
    let registry = ExecutionRegistry::new(Limits::default());
    let (target, writer) = producer(&registry).await;
    let (other, other_writer) = producer(&registry).await;
    writer.append(None, "consumed").unwrap();
    let page = registry.read(target, 0, 4096).unwrap();
    assert_eq!(page.chunks[0].content, "consumed");
    assert_eq!(
        page.revision,
        registry.read(target, 0, 4096).unwrap().revision
    );
    let waiting =
        registry.wait_for_execution_change(target, Some(page.revision), CancellationToken::new());
    tokio::pin!(waiting);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut waiting)
            .await
            .is_err()
    );
    other_writer.append(None, "unrelated").unwrap();
    registry.read(other, 0, 4096).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut waiting)
            .await
            .is_err()
    );
    writer.append(None, "next").unwrap();
    assert_ne!(waiting.await.unwrap().0, page.revision);
    assert_eq!(
        registry
            .read(target, page.next_cursor, 4096)
            .unwrap()
            .chunks[0]
            .content,
        "next"
    );
    writer.finish(Ok(())).unwrap();
    other_writer.finish(Ok(())).unwrap();
    registry.close().await.unwrap();
}

async fn producer(owner: &Arc<ExecutionRegistry>) -> (ExecutionId, OutputWriter) {
    let registry = owner.clone();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let mut handle = owner
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
    let id = handle.id;
    handle.activate().unwrap();
    handle.wait().await.unwrap();
    handle.detach();
    (id, receiver.await.unwrap())
}
#[tokio::test]
async fn addressed_wait_ignores_other_invocations_and_reads_but_observes_target_changes() {
    let registry = ExecutionRegistry::new(Limits::default());
    let (target, writer) = producer(&registry).await;
    let (other, other_writer) = producer(&registry).await;
    let page = registry.read(target, 0, 4096).unwrap();
    let waiting =
        registry.wait_for_execution_change(target, Some(page.revision), CancellationToken::new());
    tokio::pin!(waiting);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut waiting)
            .await
            .is_err()
    );
    for _ in 0..3 {
        let owner = registry.clone();
        let mut invocation = registry
            .start(
                descriptor(),
                CancellationToken::new(),
                move |_| async move {
                    owner.get(target)?;
                    owner.read(target, 0, 4096)?;
                    Ok(ToolOutput::text("unrelated status/read"))
                },
            )
            .unwrap();
        invocation.activate().unwrap();
        invocation.wait().await.unwrap();
        invocation.detach();
    }
    other_writer.append(None, "other output").unwrap();
    registry.read(other, 0, 4096).unwrap();
    assert_eq!(
        registry.read(target, 0, 4096).unwrap().revision,
        page.revision
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut waiting)
            .await
            .is_err()
    );
    writer.append(None, "target output").unwrap();
    let (revision, row) = tokio::time::timeout(Duration::from_secs(1), &mut waiting)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(revision, page.revision);
    assert_eq!(row.execution_id, target);
    assert_eq!(
        registry.read(target, 0, 4096).unwrap().chunks[0].content,
        "target output"
    );
    let terminal =
        registry.wait_for_execution_change(target, Some(revision), CancellationToken::new());
    tokio::pin!(terminal);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut terminal)
            .await
            .is_err()
    );
    writer.finish(Ok(())).unwrap();
    assert_eq!(terminal.await.unwrap().1.status, ExecutionStatus::Succeeded);
    let cancellation = CancellationToken::new();
    let waiting = registry.wait_for_execution_change(other, None, cancellation.clone());
    tokio::pin!(waiting);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut waiting)
            .await
            .is_err()
    );
    cancellation.cancel();
    assert!(matches!(waiting.await, Err(Error::Cancelled)));
    other_writer.finish(Ok(())).unwrap();
    registry.close().await.unwrap();
}
#[tokio::test]
async fn buffered_handle_wait_uses_the_same_target_revision_as_its_page() {
    let registry = ExecutionRegistry::new(Limits::default());
    let id = registry
        .admit_detached(descriptor(), CancellationToken::new())
        .unwrap();
    let (writer, output) = registry
        .open_output(
            id,
            OutputOptions::default(),
            Arc::new(CancellationController::new(CancellationToken::new())),
        )
        .unwrap();
    let (other, other_writer) = producer(&registry).await;
    let page = output.snapshot(4096).unwrap();
    let waiting = output.wait_for_change(page.revision, CancellationToken::new());
    tokio::pin!(waiting);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut waiting)
            .await
            .is_err()
    );
    other_writer.append(None, "unrelated").unwrap();
    registry.read(other, 0, 4096).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut waiting)
            .await
            .is_err()
    );
    writer.append(None, "addressed").unwrap();
    let revision = waiting.await.unwrap();
    assert_eq!(revision, output.snapshot(4096).unwrap().revision);
    assert_ne!(revision, page.revision);
    writer.finish(Ok(())).unwrap();
    other_writer.finish(Ok(())).unwrap();
    registry.close().await.unwrap();
}

#[tokio::test]
async fn admission_progress_marks_publish_revisions_and_never_reopen_terminal_execution() {
    let registry = ExecutionRegistry::new(Limits::default());
    let mut handle = registry
        .start(descriptor(), CancellationToken::new(), |_| async {
            Ok(ToolOutput::text("done"))
        })
        .unwrap();
    let id = handle.id;
    let initial = registry.execution_revision(id).unwrap();
    registry.mark(id, ExecutionStatus::Waiting);
    let (waiting, row) = registry
        .wait_for_execution_change(id, Some(initial), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(row.status, ExecutionStatus::Waiting);
    assert_ne!(waiting, initial);
    registry.mark(id, ExecutionStatus::Running);
    let (running, row) = registry
        .wait_for_execution_change(id, Some(waiting), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(row.status, ExecutionStatus::Running);
    assert_ne!(running, waiting);
    handle.activate().unwrap();
    assert_eq!(handle.wait().await.unwrap(), "done");
    handle.detach();
    let terminal = registry.execution_revision(id).unwrap();
    registry.mark(id, ExecutionStatus::Waiting);
    assert_eq!(registry.get(id).unwrap().status, ExecutionStatus::Succeeded);
    assert_eq!(registry.execution_revision(id).unwrap(), terminal);
    registry.mark(ExecutionId(u64::MAX), ExecutionStatus::Waiting);
    assert_eq!(registry.execution_revision(id).unwrap(), terminal);
    registry.close().await.unwrap();
}
