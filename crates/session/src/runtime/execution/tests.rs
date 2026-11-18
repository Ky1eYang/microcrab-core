//! Contract tests for nonprocess producers, explicit failures and output ownership.
mod content;
mod media;
mod notices;
mod ownership;
mod target_wait;
use super::*;
use crate::{Error, Limits};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
fn row(name: &str) -> ExecutionSnapshot {
    ExecutionSnapshot::new(
        "owner".into(),
        "call".into(),
        name.into(),
        "agent".into(),
        None,
    )
}
#[test]
fn recovery_cannot_increase_execution_buffer_capacity() {
    let configured = Limits {
        max_processes: 9,
        max_execution_buffers: 2,
        ..Limits::default()
    };
    let registry = ExecutionRegistry::new(configured.clone());
    let mut restored = configured;
    registry.validate_recovery_limits(&restored).unwrap();
    restored.max_execution_buffers = 3;
    assert!(matches!(
        registry.validate_recovery_limits(&restored),
        Err(Error::Config(_))
    ));
}

#[tokio::test]
async fn final_after_prepared_buffer_closes_writer_and_preserves_final_failure() {
    let registry = ExecutionRegistry::new(Limits::default());
    let owner = registry.clone();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let mut handle = registry
        .start(
            row("question"),
            CancellationToken::new(),
            move |progress| async move {
                let (writer, mut output) = owner.open_output(
                    progress.execution_id(),
                    OutputOptions::default(),
                    Arc::new(CancellationController::new(CancellationToken::new())),
                )?;
                // A prepared capability stays owned until invocation resolution, even if its
                // holder is returned through another native scope rather than the final enum.
                output.adopted = true;
                sender
                    .send((writer, output))
                    .map_err(|_| Error::Tool("test transfer".into()))?;
                Ok(ToolOutput::failed(
                    "partial",
                    ToolFailure::failed("self_failure", "reported"),
                ))
            },
        )
        .unwrap();
    handle.activate().unwrap();
    let result = handle.wait().await.unwrap();
    handle.detach();
    assert!(result.contains("self_failure"));
    let (writer, _output) = receiver.await.unwrap();
    assert!(writer.append(None, "late").is_err());
    assert_eq!(registry.snapshot()[0].status, ExecutionStatus::Failed);
    registry.close().await.unwrap();
}
#[tokio::test]
async fn read_does_not_consume_and_never_overruns_serialized_budget() {
    let limits = Limits {
        output_buffer_bytes: 128,
        ..Limits::default()
    };
    let registry = ExecutionRegistry::new(limits);
    let owner = registry.clone();
    let mut handle = registry
        .start(
            row("stream"),
            CancellationToken::new(),
            move |progress| async move {
                let (writer, output) = owner.open_output(
                    progress.execution_id(),
                    OutputOptions::default(),
                    Arc::new(CancellationController::new(CancellationToken::new())),
                )?;
                writer.append(None, &"一\n\"".repeat(100))?;
                writer.finish(Ok(()))?;
                Ok(ToolOutput::Buffered(output))
            },
        )
        .unwrap();
    let id = handle.id;
    handle.activate().unwrap();
    handle.wait().await.unwrap();
    handle.detach();
    let page = registry.read(id, 0, 350).unwrap();
    assert!(page.start_cursor > 0);
    assert!(serde_json::to_vec(&page).unwrap().len() <= 350);
    let again = registry.read(id, 0, 350).unwrap();
    assert_eq!(page.next_cursor, again.next_cursor);
    assert_eq!(
        registry.get(id).unwrap().output.unwrap().start_cursor,
        page.start_cursor
    );
    registry.close().await.unwrap();
}
#[tokio::test]
async fn recovery_preserves_expired_cursor_and_rejects_missing_data() {
    let limits = Limits {
        output_buffer_bytes: 16,
        ..Limits::default()
    };
    let registry = ExecutionRegistry::new(limits.clone());
    let owner = registry.clone();
    let mut handle = registry
        .start(
            row("persisted"),
            CancellationToken::new(),
            move |progress| async move {
                let (writer, output) = owner.open_output(
                    progress.execution_id(),
                    OutputOptions::default(),
                    Arc::new(CancellationController::new(CancellationToken::new())),
                )?;
                writer.append(Some("data"), &"abcdef一".repeat(8))?;
                writer.finish(Ok(()))?;
                Ok(ToolOutput::Buffered(output))
            },
        )
        .unwrap();
    let id = handle.id;
    handle.activate().unwrap();
    handle.wait().await.unwrap();
    handle.detach();
    let before = registry.read(id, 0, 4096).unwrap();
    assert!(before.start_cursor > 0);
    let rows = registry.recovery_snapshot();
    let restored = ExecutionRegistry::new(limits);
    restored.restore(&rows).unwrap();
    let after = restored.read(id, 0, 4096).unwrap();
    assert_eq!(before.start_cursor, after.start_cursor);
    assert_eq!(before.end_cursor, after.end_cursor);
    assert_eq!(before.chunks[0].content, after.chunks[0].content);
    let mut invalid = rows;
    invalid[0].result = None;
    assert!(restored.restore(&invalid).is_err());
    assert_eq!(
        restored.read(id, 0, 4096).unwrap().end_cursor,
        after.end_cursor
    );
    registry.close().await.unwrap();
    restored.close().await.unwrap();
}
