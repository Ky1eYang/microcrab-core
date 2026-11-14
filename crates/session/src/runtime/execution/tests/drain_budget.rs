//! Response budgets cover the returned revision and every JSON-escaped output byte.
use super::*;
use crate::runtime::execution::{CancellationController, ToolOutput};

async fn buffered(content: &str) -> (Arc<ExecutionRegistry>, ExecutionId) {
    let registry = ExecutionRegistry::new(Limits::default());
    let owner = registry.clone();
    let content = content.to_owned();
    let mut handle = registry
        .start(
            ExecutionSnapshot::new(
                "owner".into(),
                "call".into(),
                "stream".into(),
                "agent".into(),
                None,
            ),
            CancellationToken::new(),
            move |progress| async move {
                let (writer, output) = owner.open_output(
                    progress.execution_id(),
                    OutputOptions::default(),
                    Arc::new(CancellationController::new(CancellationToken::new())),
                )?;
                writer.append(None, &content)?;
                writer.finish(Ok(()))?;
                Ok(ToolOutput::Buffered(output))
            },
        )
        .unwrap();
    let id = handle.id;
    handle.activate().unwrap();
    handle.wait().await.unwrap();
    handle.detach();
    (registry, id)
}
fn revision(registry: &ExecutionRegistry, id: ExecutionId, value: u64) {
    let mut state = registry.state.lock().unwrap();
    let entry = state.entries.get_mut(&id).unwrap();
    ExecutionRegistry::refresh_revision(entry);
    entry.revision = value;
}

#[tokio::test]
async fn read_preserves_revision_digits_and_output() {
    for before in [9, 99, 999, 9_007_199_254_740_991] {
        for content in ["x".repeat(100), "中文🙂\n\"\\\t".repeat(20)] {
            let (registry, id) = buffered(&content).await;
            revision(&registry, id, before);
            let full = registry.read(id, 0, 4096).unwrap();
            let maximum = serde_json::to_vec(&full).unwrap().len();
            let first = registry.read(id, 0, maximum).unwrap();
            assert!(
                serde_json::to_vec(&first).unwrap().len() <= maximum,
                "revision {before}"
            );
            assert!(first.next_cursor > 0);
            assert_eq!(first.revision, before);
            assert_eq!(first.revision, registry.execution_revision(id).unwrap());
            let mut output: String = first
                .chunks
                .into_iter()
                .map(|chunk| chunk.content)
                .collect();
            let mut cursor = first.next_cursor;
            while cursor < content.len() as u64 {
                let page = registry.read(id, cursor, maximum).unwrap();
                assert!(serde_json::to_vec(&page).unwrap().len() <= maximum);
                assert_eq!(page.cursor, cursor);
                assert!(page.next_cursor > cursor);
                cursor = page.next_cursor;
                output.extend(page.chunks.into_iter().map(|chunk| chunk.content));
            }
            assert_eq!(
                output, content,
                "revision {before} must not lose or repeat output"
            );
            registry.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn read_that_cannot_fit_metadata_does_not_mutate_or_wrap() {
    let (registry, id) = buffered("abc").await;
    let before = 9_007_199_254_740_991;
    revision(&registry, id, before);
    let empty = registry.read(id, 3, 4096).unwrap();
    let maximum = serde_json::to_vec(&empty).unwrap().len() - 2;
    assert!(matches!(
        registry.read(id, 0, maximum),
        Err(Error::Limit(_))
    ));
    assert_eq!(registry.execution_revision(id).unwrap(), before);
    let retained = registry.read(id, 0, 4096).unwrap();
    assert_eq!(retained.cursor, 0);
    assert_eq!(retained.chunks[0].content, "abc");
    registry.close().await.unwrap();
}

#[test]
fn output_pages_keep_utf8_channels_and_exact_serialized_bounds_across_many_chunks() {
    let parts = ["a", "中", "🙂", "\n\"\\\t\r\0"];
    for maximum in [360, 513, 1024, 4096, 131_072] {
        let mut buffer = Buffer::new(OutputOptions::default(), 32_768).unwrap();
        let mut expected = String::new();
        for index in 0..1024 {
            let text = parts[index % parts.len()];
            buffer.append(Some("channel\n\"中"), text).unwrap();
            expected.push_str(text);
        }
        buffer.closed = true;
        let mut output = String::new();
        let mut cursor = buffer.start;
        while cursor < buffer.end {
            let page = buffer.page(ExecutionId(1), cursor, maximum, 99).unwrap();
            assert!(
                serde_json::to_vec(&page).unwrap().len() <= maximum,
                "budget {maximum}"
            );
            assert!(
                page.next_cursor > cursor,
                "budget {maximum} must fit a complete character"
            );
            assert_eq!(page.cursor, cursor);
            let mut next = page.cursor;
            for chunk in page.chunks {
                assert_eq!(chunk.channel.as_deref(), Some("channel\n\"中"));
                assert_eq!(chunk.cursor, next);
                assert_eq!(chunk.next_cursor - chunk.cursor, chunk.content.len() as u64);
                next = chunk.next_cursor;
                output.push_str(&chunk.content);
            }
            assert_eq!(next, page.next_cursor);
            cursor = next;
        }
        assert_eq!(output, expected, "budget {maximum}");
    }
}
