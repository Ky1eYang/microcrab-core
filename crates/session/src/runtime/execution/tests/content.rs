//! Authorized display sources stay exact and never consume the model output lane.
use super::*;

#[tokio::test]
async fn content_pages_preserve_full_utf8_source_and_recovery_without_metadata_bodies() {
    let registry = ExecutionRegistry::new(Limits::default());
    let arguments = serde_json::json!({"text": "中🙂\n\"\\".repeat(2000)}).to_string();
    let result = "result 中🙂\n\"\\ \n".repeat(1800);
    let mut descriptor = row("inspect");
    descriptor.arguments = Some(arguments.clone());
    let id = registry
        .admit_detached(descriptor, CancellationToken::new())
        .unwrap();
    registry.retain_invocation_result(id, &result).unwrap();
    for (field, expected) in [
        (ExecutionContentField::Arguments, arguments),
        (ExecutionContentField::Result, result),
    ] {
        let mut cursor = 0;
        let mut full = String::new();
        let first = registry.read_content(id, field, 0, 512).unwrap();
        assert!(first.available);
        assert_eq!(first.source_hash.as_ref().unwrap().len(), 64);
        loop {
            let page = registry.read_content(id, field, cursor, 512).unwrap();
            assert!(serde_json::to_vec(&page).unwrap().len() <= 512);
            assert_eq!(page.source_hash, first.source_hash);
            assert_eq!(page.cursor, cursor);
            assert_eq!(page.next_cursor - cursor, page.content.len() as u64);
            full.push_str(&page.content);
            if page.next_cursor == page.end_cursor {
                break;
            }
            assert!(page.next_cursor > cursor);
            cursor = page.next_cursor;
        }
        assert_eq!(full, expected);
    }
    let metadata = serde_json::to_value(&registry.snapshot()[0]).unwrap();
    assert!(metadata.get("arguments").is_none());
    assert!(metadata.get("invocation_result").is_none());
    assert!(metadata.get("result").is_none());
    assert_eq!(metadata["arguments_available"], true);
    assert_eq!(metadata["result_available"], true);
    let recovered = ExecutionRegistry::new(Limits::default());
    recovered.restore(&registry.recovery_snapshot()).unwrap();
    assert_eq!(
        recovered
            .read_content(id, ExecutionContentField::Arguments, 0, 512)
            .unwrap()
            .source_hash,
        registry
            .read_content(id, ExecutionContentField::Arguments, 0, 512)
            .unwrap()
            .source_hash
    );
    registry.close().await.unwrap();
    recovered.close().await.unwrap();
}

#[tokio::test]
async fn content_inspection_does_not_advance_drain_or_authorize_release() {
    let registry = ExecutionRegistry::new(Limits::default());
    let owner = registry.clone();
    let mut descriptor = row("stream");
    descriptor.arguments = Some("{\"path\":\"file\"}".into());
    let mut handle = registry
        .start(
            descriptor,
            CancellationToken::new(),
            move |progress| async move {
                let (writer, output) = owner.open_output(
                    progress.execution_id(),
                    OutputOptions::default(),
                    Arc::new(CancellationController::new(CancellationToken::new())),
                )?;
                writer.append(Some("stdout"), "retained output\n")?;
                writer.finish(Ok(()))?;
                Ok(ToolOutput::Buffered(output))
            },
        )
        .unwrap();
    let id = handle.id;
    handle.activate().unwrap();
    let receipt = handle.wait().await.unwrap();
    handle.detach();
    registry.retain_invocation_result(id, &receipt).unwrap();
    let before = registry.recovery_snapshot();
    for _ in 0..3 {
        assert!(
            registry
                .read_content(id, ExecutionContentField::Arguments, 0, 1024)
                .unwrap()
                .available
        );
        assert!(
            registry
                .read_content(id, ExecutionContentField::Result, 0, 1024)
                .unwrap()
                .available
        );
        assert_eq!(
            registry.read(id, 0, 1024).unwrap().chunks[0].content,
            "retained output\n"
        );
    }
    let after = registry.recovery_snapshot();
    assert_eq!(
        before[0].output.as_ref().unwrap().start_cursor,
        after[0].output.as_ref().unwrap().start_cursor
    );
    assert_eq!(
        before[0].delivery.unwrap().exposed,
        after[0].delivery.unwrap().exposed
    );
    assert_eq!(
        before[0].delivery.unwrap().released,
        after[0].delivery.unwrap().released
    );
    assert_eq!(
        registry.snapshot()[0].output_availability,
        Some(OutputAvailability::Retained)
    );
    registry.close().await.unwrap();
}

#[tokio::test]
async fn full_snapshot_metadata_matches_live_and_restored_availability() {
    let registry = ExecutionRegistry::new(Limits::default());
    let mut descriptor = row("inline");
    descriptor.arguments = Some("{}".into());
    let mut handle = registry
        .start(descriptor, CancellationToken::new(), |_| async {
            Ok(ToolOutput::text("retained final"))
        })
        .unwrap();
    handle.activate().unwrap();
    handle.wait().await.unwrap();
    handle.detach();
    let full = registry.recovery_snapshot();
    assert_eq!(full[0].output_availability, Some(OutputAvailability::None));
    let metadata = full
        .iter()
        .map(ExecutionSnapshot::metadata)
        .collect::<Vec<_>>();
    assert_eq!(
        serde_json::to_value(&metadata).unwrap(),
        serde_json::to_value(registry.snapshot()).unwrap()
    );
    let restored = ExecutionRegistry::new(Limits::default());
    restored.restore(&full).unwrap();
    assert_eq!(
        serde_json::to_value(restored.snapshot()).unwrap(),
        serde_json::to_value(metadata).unwrap()
    );
    // Old serialized nodes still decode with unknown optional availability;
    // a direct metadata projection does not rewrite their historical bytes.
    let mut old = full[0].clone();
    old.output_availability = None;
    let old = old.metadata();
    assert!(
        serde_json::to_value(old)
            .unwrap()
            .get("output_availability")
            .is_none()
    );
    registry.close().await.unwrap();
    restored.close().await.unwrap();
}

#[tokio::test]
async fn unavailable_old_arguments_are_not_fabricated_and_bad_pages_fail_closed() {
    let registry = ExecutionRegistry::new(Limits::default());
    let id = registry
        .admit_detached(row("old"), CancellationToken::new())
        .unwrap();
    let page = registry
        .read_content(id, ExecutionContentField::Arguments, 0, 512)
        .unwrap();
    assert!(!page.available);
    assert!(page.source_hash.is_none());
    assert_eq!(page.end_cursor, 0);
    assert!(
        registry
            .read_content(id, ExecutionContentField::Arguments, 1, 512)
            .is_err()
    );
    assert!(
        registry
            .read_content(id, ExecutionContentField::Arguments, 0, 0)
            .is_err()
    );
    assert!(
        registry
            .read_content(id, ExecutionContentField::Arguments, 0, 65537)
            .is_err()
    );
    assert!(
        registry
            .read_content(ExecutionId(999), ExecutionContentField::Arguments, 0, 512)
            .is_err()
    );
    registry.close().await.unwrap();
}
