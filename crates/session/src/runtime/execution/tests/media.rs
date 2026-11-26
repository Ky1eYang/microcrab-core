use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
fn png() -> Vec<u8> {
    let mut data = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
    data.extend_from_slice(&1u32.to_be_bytes());
    data.extend_from_slice(&1u32.to_be_bytes());
    data.extend_from_slice(&[8, 2, 0, 0, 0, 0, 0, 0, 0]);
    data
}
fn output(data: Vec<u8>, count: usize, content: &str) -> ToolOutput {
    ToolOutput::Final(FinalOutput {
        content: content.into(),
        failure: None,
        attachments: (0..count)
            .map(|_| MediaAttachment {
                media_type: "image/png".into(),
                data: data.clone(),
            })
            .collect(),
    })
}
#[tokio::test]
async fn native_media_validates_independent_caps_and_does_not_enter_inspection_bytes() {
    let owner = ExecutionRegistry::new(Limits {
        max_tool_output_bytes: 8,
        max_tool_image_bytes: 64,
        ..Limits::default()
    });
    let mut handle = owner
        .start_with_media_output(row("image"), CancellationToken::new(), 64, |_| async {
            Ok(output(png(), 1, "caption"))
        })
        .unwrap();
    handle.activate().unwrap();
    let result = handle.wait_output().await.unwrap();
    assert_eq!(result.content, "caption");
    assert_eq!(result.attachments[0].data, png());
    let rows = owner.recovery_snapshot();
    let projected: serde_json::Value =
        serde_json::from_str(rows[0].result.as_deref().unwrap()).unwrap();
    assert_eq!(projected["images"][0]["width"], 1);
    assert_eq!(projected["images"][0]["byte_len"], png().len());
    assert!(projected["images"][0].get("data").is_none());
    assert!(
        handle
            .wait()
            .await
            .unwrap_err()
            .to_string()
            .contains("typed Session admission")
    );
    handle.detach();
    owner.close().await.unwrap();
}
#[tokio::test]
async fn invalid_or_undeclared_native_media_and_excess_text_fail_explicitly() {
    for (declared, data, count, text) in [
        (0, png(), 1, "ok"),
        (32, png(), 1, "ok"),
        (64, vec![1, 2, 3], 1, "ok"),
        (1024, png(), crate::admission::image::MAX_IMAGES_PER_TOOL_OUTPUT + 1, "ok"),
        (64, png(), 1, "excess text"),
    ] {
        let owner = ExecutionRegistry::new(Limits {
            max_tool_output_bytes: 8,
            max_tool_image_bytes: 64,
            ..Limits::default()
        });
        let mut handle = owner
            .start_with_media_output(
                row("image"),
                CancellationToken::new(),
                declared,
                move |_| async move { Ok(output(data, count, text)) },
            )
            .unwrap();
        handle.activate().unwrap();
        assert!(handle.wait_output().await.is_err());
        assert_ne!(owner.snapshot()[0].status, ExecutionStatus::Succeeded);
        handle.detach();
        owner.close().await.unwrap();
    }
}
#[tokio::test]
async fn subordinate_owner_capacity_rejects_before_resource_effects() {
    let owner = ExecutionRegistry::new(Limits::default());
    let effects = Arc::new(AtomicUsize::new(0));
    let calls = effects.clone();
    let mut handle = owner
        .start(
            row("cleanup"),
            CancellationToken::new(),
            move |progress| async move {
                for _ in 0..9 {
                    progress.retain_cleanup(Arc::new(Cleanup {
                        calls: AtomicUsize::new(0),
                        fail_first: false,
                    }))?;
                }
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(ToolOutput::text("done"))
            },
        )
        .unwrap();
    handle.activate().unwrap();
    assert!(matches!(handle.wait().await, Err(Error::Limit(_))));
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    handle.detach();
    owner.close().await.unwrap();
}

struct GateCleanup {
    entered: tokio::sync::Notify,
    acknowledged: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl ExecutionController for GateCleanup {
    async fn cancel(&self, _: CancellationToken) -> Result<(), Error> {
        Ok(())
    }
    async fn release(&self, _: CancellationToken) -> Result<(), Error> {
        self.entered.notify_one();
        self.acknowledged.notified().await;
        Ok(())
    }
}
#[tokio::test]
async fn final_success_waits_for_subordinate_cleanup_acknowledgment() {
    let owner = ExecutionRegistry::new(Limits::default());
    let cleanup = Arc::new(GateCleanup {
        entered: tokio::sync::Notify::new(),
        acknowledged: tokio::sync::Notify::new(),
    });
    let retained = cleanup.clone();
    let mut handle = owner
        .start(
            row("cleanup"),
            CancellationToken::new(),
            move |progress| async move {
                progress.retain_cleanup(retained)?;
                Ok(ToolOutput::text("done"))
            },
        )
        .unwrap();
    handle.activate().unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        cleanup.entered.notified(),
    )
    .await
    .unwrap();
    assert!(owner.snapshot()[0].status.is_pending());
    cleanup.acknowledged.notify_one();
    assert_eq!(handle.wait().await.unwrap(), "done");
    assert_eq!(owner.snapshot()[0].status, ExecutionStatus::Succeeded);
    handle.detach();
    owner.close().await.unwrap();
}

struct Cleanup {
    calls: AtomicUsize,
    fail_first: bool,
}
#[async_trait::async_trait]
impl ExecutionController for Cleanup {
    async fn cancel(&self, _: CancellationToken) -> Result<(), Error> {
        Ok(())
    }
    async fn release(&self, _: CancellationToken) -> Result<(), Error> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 && self.fail_first {
            Err(Error::lifecycle("cleanup receipt not acknowledged"))
        } else {
            Ok(())
        }
    }
}
#[tokio::test]
async fn subordinate_cleanup_retains_failed_owner_and_retries_only_unacknowledged_members() {
    let owner = ExecutionRegistry::new(Limits::default());
    let okay = Arc::new(Cleanup {
        calls: AtomicUsize::new(0),
        fail_first: false,
    });
    let flaky = Arc::new(Cleanup {
        calls: AtomicUsize::new(0),
        fail_first: true,
    });
    let good = okay.clone();
    let bad = flaky.clone();
    let mut handle = owner
        .start(
            row("cleanup"),
            CancellationToken::new(),
            move |progress| async move {
                progress.retain_cleanup(good.clone())?;
                progress.retain_cleanup(good)?;
                progress.retain_cleanup(bad)?;
                Ok(ToolOutput::text("done"))
            },
        )
        .unwrap();
    handle.activate().unwrap();
    assert!(handle.wait().await.is_err());
    assert_eq!(owner.snapshot()[0].status, ExecutionStatus::Interrupted);
    assert_eq!(okay.calls.load(Ordering::SeqCst), 1);
    assert_eq!(flaky.calls.load(Ordering::SeqCst), 1);
    owner
        .release(handle.id, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(okay.calls.load(Ordering::SeqCst), 1);
    assert_eq!(flaky.calls.load(Ordering::SeqCst), 2);
    handle.detach();
    owner.close().await.unwrap();
}
#[test]
fn historical_empty_final_attachments_retain_exact_bytes() {
    let bytes = br#"{"content":"old","failure":null}"#;
    let output: FinalOutput = serde_json::from_slice(bytes).unwrap();
    assert!(output.attachments.is_empty());
    assert_eq!(serde_json::to_vec(&output).unwrap(), bytes);
}
