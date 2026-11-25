use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

struct Cleanup(AtomicBool);

#[async_trait::async_trait]
impl ExecutionController for Cleanup {
    async fn cancel(&self, _: CancellationToken) -> Result<(), Error> {
        Ok(())
    }
    async fn release(&self, _: CancellationToken) -> Result<(), Error> {
        if self.0.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(Error::tool("injected cleanup failure"))
        }
    }
}

async fn capture(
    owner: &Arc<ExecutionRegistry>,
    controller: Arc<dyn ExecutionController>,
    bytes: &[u8],
    success: bool,
) -> Result<(ExecutionId, String), Error> {
    let registry = owner.clone();
    let bytes = bytes.to_vec();
    let mut handle = owner.start(
        row("shell"),
        CancellationToken::new(),
        move |progress| async move {
            let (writer, output) = registry.open_private_output(
                progress.execution_id(),
                "image/png".into(),
                controller,
            )?;
            assert!(writer.append(Some("stdout"), "not public").is_err());
            writer.append_private(&bytes)?;
            writer.finish(if success {
                Ok(())
            } else {
                Err(ToolFailure::failed("nonzero_exit", "producer failed"))
            })?;
            Ok(ToolOutput::Buffered(output.with_initial_output(true)))
        },
    )?;
    handle.activate()?;
    let result = handle.wait().await;
    let id = handle.id;
    handle.detach();
    result.map(|receipt| (id, receipt))
}

#[tokio::test]
async fn private_lease_preserves_bytes_through_release_and_keeps_allocation_charged() {
    let registry = ExecutionRegistry::new(Limits {
        max_execution_buffers: 1,
        ..Limits::default()
    });
    let cleanup = Arc::new(Cleanup(AtomicBool::new(true)));
    let bytes = b"\xff\xfe\0PRIVATE_NOT_PUBLIC\x80";
    let (id, receipt) = capture(&registry, cleanup.clone(), bytes, true)
        .await
        .unwrap();
    let receipt: serde_json::Value = serde_json::from_str(&receipt).unwrap();
    assert!(receipt.get("initial").is_none());
    assert_eq!(
        receipt["output"]["private_stdout"]["media_type"],
        "image/png"
    );
    assert!(registry.read(id, 0, 1024).unwrap().chunks.is_empty());
    let lease = registry
        .private_output(id, "owner", "stdout", "image/png")
        .unwrap();
    assert_eq!(lease.as_bytes(), bytes);
    assert!(
        registry
            .private_output(id, "owner", "stderr", "image/png")
            .is_err()
    );
    assert!(
        registry
            .private_output(id, "owner", "stdout", "image/jpeg")
            .is_err()
    );
    assert!(!format!("{lease:?}").contains("PRIVATE_NOT_PUBLIC"));

    let checkpoint = registry.recovery_snapshot();
    let snapshot = checkpoint
        .iter()
        .find(|row| row.execution_id == id)
        .unwrap();
    assert!(snapshot.result.is_none());
    assert!(
        !snapshot
            .output
            .as_ref()
            .unwrap()
            .private_stdout
            .as_ref()
            .unwrap()
            .available
    );
    assert!(
        !serde_json::to_string(&checkpoint)
            .unwrap()
            .contains("PRIVATE_NOT_PUBLIC")
    );
    let recovered = ExecutionRegistry::new(Limits::default());
    recovered.restore(&checkpoint).unwrap();
    assert!(
        recovered
            .private_output(id, "owner", "stdout", "image/png")
            .is_err()
    );

    registry
        .release(id, CancellationToken::new())
        .await
        .unwrap();
    assert!(
        registry
            .private_output(id, "owner", "stdout", "image/png")
            .is_err()
    );
    assert_eq!(lease.as_bytes(), bytes);
    assert!(
        capture(&registry, cleanup.clone(), b"next", true)
            .await
            .is_err()
    );
    drop(lease);
    capture(&registry, cleanup, b"", true).await.unwrap();
    registry.close().await.unwrap();
}

#[tokio::test]
async fn close_preserves_private_backing_until_cleanup_ack() {
    let registry = ExecutionRegistry::new(Limits::default());
    let cleanup = Arc::new(Cleanup(AtomicBool::new(false)));
    let (id, _) = capture(&registry, cleanup.clone(), b"data", true)
        .await
        .unwrap();
    let available = || {
        registry
            .get(id)
            .unwrap()
            .output
            .unwrap()
            .private_stdout
            .unwrap()
            .available
    };
    assert!(registry.close().await.is_err());
    assert!(available());
    assert!(
        registry
            .private_output(id, "owner", "stdout", "image/png")
            .is_err()
    );
    cleanup.0.store(true, Ordering::Release);
    registry.close().await.unwrap();
    assert!(!available());
}

#[tokio::test]
async fn overflow_bad_exit_and_cleanup_failure_never_release_or_project_raw_bytes() {
    let registry = ExecutionRegistry::new(Limits {
        output_buffer_bytes: 4,
        ..Limits::default()
    });
    let cleanup = Arc::new(Cleanup(AtomicBool::new(false)));
    let (id, _) = capture(&registry, cleanup.clone(), b"\x80\xff", true)
        .await
        .unwrap();
    assert!(
        registry
            .release(id, CancellationToken::new())
            .await
            .is_err()
    );
    let metadata = registry
        .get(id)
        .unwrap()
        .output
        .unwrap()
        .private_stdout
        .unwrap();
    assert_eq!(metadata.end_cursor, 2);
    assert!(
        metadata.available,
        "failed cleanup must retain source ownership"
    );
    cleanup.0.store(true, Ordering::Release);
    registry
        .release(id, CancellationToken::new())
        .await
        .unwrap();
    assert!(
        !registry
            .get(id)
            .unwrap()
            .output
            .unwrap()
            .private_stdout
            .unwrap()
            .available
    );

    let (id, _) = capture(&registry, cleanup.clone(), b"longer", true)
        .await
        .unwrap();
    let metadata = registry
        .get(id)
        .unwrap()
        .output
        .unwrap()
        .private_stdout
        .unwrap();
    assert!(metadata.truncated);
    assert_eq!(metadata.end_cursor, 6);
    assert!(
        registry
            .private_output(id, "owner", "stdout", "image/png")
            .is_err()
    );
    assert!(registry.read(id, 0, 1024).unwrap().chunks.is_empty());
    let (id, _) = capture(&registry, cleanup, b"data", false).await.unwrap();
    assert_eq!(registry.get(id).unwrap().status, ExecutionStatus::Failed);
    assert!(
        registry
            .private_output(id, "owner", "stdout", "image/png")
            .is_err()
    );
    registry.close().await.unwrap();
}
