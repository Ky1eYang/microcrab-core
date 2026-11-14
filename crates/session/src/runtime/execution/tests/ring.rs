//! Storage bounds, loss evidence and strict current-format recovery.
use super::*;

fn text(page: &OutputPage) -> String {
    page.chunks
        .iter()
        .map(|chunk| chunk.content.as_str())
        .collect()
}
#[test]
fn default_ring_retains_four_mib_and_oversized_append_keeps_utf8_suffix() {
    let limit = crate::Limits::default().output_buffer_bytes;
    assert_eq!(limit, 4 * 1024 * 1024);
    let mut ring = Buffer::new(OutputOptions::default(), limit).unwrap();
    ring.append(Some("out"), &"a".repeat(limit)).unwrap();
    assert_eq!((ring.start, ring.end), (0, limit as u64));
    ring.append(Some("err"), "中🙂").unwrap();
    assert_eq!(ring.end - ring.start, limit as u64);
    let first = ring.page(ExecutionId(1), 0, 1024, 0).unwrap();
    assert_eq!(first.cursor, 7);
    assert_eq!(first.start_cursor, 7);
    let oversized = "中🙂".repeat(limit / 7 + 10);
    ring.append(Some("out"), &oversized).unwrap();
    assert!(ring.end - ring.start <= limit as u64);
    let page = ring.page(ExecutionId(1), 0, 1024, 0).unwrap();
    assert!(
        oversized.ends_with(&String::from_utf8(ring.content.iter().copied().collect()).unwrap())
    );
    assert_eq!(page.cursor, ring.start);
    assert!(ring.content.capacity() <= limit.next_power_of_two());
    assert!(ring.tags.capacity() <= limit.next_power_of_two());
}
#[test]
fn alternating_channels_preserve_every_byte_and_bounded_checkpoint() {
    let limit = 128 * 1024;
    let mut ring = Buffer::new(OutputOptions::default(), limit).unwrap();
    for index in 0..limit {
        ring.append(Some(if index % 2 == 0 { "z" } else { "a" }), "x")
            .unwrap();
    }
    assert_eq!(ring.start, 0, "run count must never evict content early");
    assert_eq!(ring.content.len(), limit);
    assert_eq!(ring.tags.len(), limit);
    let encoded = ring.persisted();
    assert!(encoded.len() <= 6 * limit + 528 * 1024);
    let restored = Buffer::restore(&ring.metadata(), Some(&encoded), limit).unwrap();
    assert_eq!(
        restored.channels,
        ["z", "a"],
        "restore preserves dynamic tag catalog indices"
    );
    let mut cursor = 0;
    let mut count = 0;
    while cursor < ring.end {
        let page = restored.page(ExecutionId(1), cursor, 4096, 0).unwrap();
        assert!(serde_json::to_vec(&page).unwrap().len() <= 4096);
        assert!(page.next_cursor > cursor);
        for chunk in &page.chunks {
            assert_eq!(
                chunk.channel.as_deref(),
                Some(if count % 2 == 0 { "z" } else { "a" })
            );
            assert_eq!(chunk.content, "x");
            count += 1;
        }
        cursor = page.next_cursor;
    }
    assert_eq!(count, limit);
}
#[test]
fn same_channel_coalesces_lazily_with_split_ceiling_and_exact_order() {
    let mut ring = Buffer::new(OutputOptions::default(), 256 * 1024).unwrap();
    for _ in 0..70_000 {
        ring.append(Some("out"), "a").unwrap();
    }
    ring.append(Some("err"), "中").unwrap();
    ring.append(Some("out"), "last").unwrap();
    let page = ring.page(ExecutionId(1), 0, 256 * 1024, 0).unwrap();
    assert_eq!(page.chunks.len(), 4);
    assert_eq!(page.chunks[0].content.len(), CHUNK_BYTES);
    assert_eq!(page.chunks[1].content.len(), 70_000 - CHUNK_BYTES);
    assert_eq!(page.chunks[2].channel.as_deref(), Some("err"));
    assert_eq!(page.chunks[3].content, "last");
    for pair in page.chunks.windows(2) {
        assert_eq!(pair[0].next_cursor, pair[1].cursor);
    }
}
#[test]
fn marker_has_its_own_channel_boundary_and_empty_appends_do_not_reset_dedup() {
    let mut ring = Buffer::new(OutputOptions::default(), 1024).unwrap();
    ring.append(Some("stdout"), "before").unwrap();
    ring.mark_truncated().unwrap();
    ring.append(None, "").unwrap();
    ring.mark_truncated().unwrap();
    ring.append(Some("stderr"), "after").unwrap();
    let page = ring.page(ExecutionId(1), 0, 4096, 0).unwrap();
    assert_eq!(page.chunks.len(), 3);
    assert_eq!(page.chunks[0].channel.as_deref(), Some("stdout"));
    assert_eq!(page.chunks[1].channel, None);
    assert_eq!(page.chunks[1].content, "\n[upstream lost]\n");
    assert_eq!(page.chunks[1].cursor, 6);
    assert_eq!(page.chunks[2].channel.as_deref(), Some("stderr"));
    assert_eq!(page.end_cursor as usize, text(&page).len());
    ring.mark_truncated().unwrap();
    assert_eq!(
        text(&ring.page(ExecutionId(1), 0, 4096, 0).unwrap())
            .matches("[upstream lost]")
            .count(),
        2
    );
}
#[test]
fn cursors_clamp_only_evicted_prefix_and_validate_live_utf8() {
    let mut ring = Buffer::new(OutputOptions::default(), 7).unwrap();
    ring.append(None, "中🙂").unwrap();
    assert!(ring.page(ExecutionId(1), 1, 4096, 0).is_err());
    ring.append(None, "ab").unwrap();
    assert_eq!(ring.start, 3);
    assert_eq!(ring.page(ExecutionId(1), 1, 4096, 0).unwrap().cursor, 3);
    assert!(ring.page(ExecutionId(1), 4, 4096, 0).is_err());
    assert!(ring.page(ExecutionId(1), ring.end + 1, 4096, 0).is_err());
    let before = ring.persisted();
    let page = ring
        .page_capped(ExecutionId(1), 0, 4096, 0, Some(1))
        .unwrap();
    assert!(page.chunks.is_empty());
    assert_eq!(page.cursor, page.next_cursor);
    assert_eq!(ring.persisted(), before);
}
#[test]
fn strict_recovery_rejects_bad_tags_lengths_utf8_and_legacy_chunks() {
    let mut ring = Buffer::new(
        OutputOptions {
            channels: vec!["out".into()],
        },
        128,
    )
    .unwrap();
    ring.append(Some("out"), "中").unwrap();
    let metadata = ring.metadata();
    let original = serde_json::from_str::<serde_json::Value>(&ring.persisted()).unwrap();
    for tags in [vec![1], vec![1, 0, 1], vec![18, 18, 18], vec![2, 2, 2]] {
        let mut value = original.clone();
        value["tags_base64"] = serde_json::json!(STANDARD.encode(tags));
        assert!(Buffer::restore(&metadata, Some(&value.to_string()), 128).is_err());
    }
    let mut value = original.clone();
    value["content_base64"] = serde_json::json!(STANDARD.encode([0xff, 0xff, 0xff]));
    assert!(Buffer::restore(&metadata, Some(&value.to_string()), 128).is_err());
    let mut value = original;
    value["extra"] = serde_json::json!(true);
    assert!(Buffer::restore(&metadata, Some(&value.to_string()), 128).is_err());
    assert!(Buffer::restore(&metadata, Some("[]"), 128).is_err());
    assert!(Buffer::restore(&metadata, None, 128).is_err());
    let mut invalid = metadata.clone();
    invalid.bytes += 1;
    assert!(Buffer::restore(&invalid, Some(&ring.persisted()), 128).is_err());
    invalid = metadata;
    invalid.start_cursor = invalid.end_cursor + 1;
    assert!(Buffer::restore(&invalid, Some(&ring.persisted()), 128).is_err());
}
#[test]
fn cursor_overflow_is_checked_before_retention_mutation() {
    let mut ring = Buffer::new(OutputOptions::default(), 4).unwrap();
    ring.end = u64::MAX;
    ring.start = ring.end;
    let before = ring.persisted();
    assert!(ring.append(None, "x").is_err());
    assert_eq!(ring.end, u64::MAX);
    assert_eq!(ring.persisted(), before);
}

#[test]
fn public_output_schema_rejects_obsolete_fields_without_mutating_source_json() {
    let legacy = serde_json::json!({"channels":[],"bytes":0,"retained_bytes":0,
        "cursor":0,"end_cursor":0,"closed":true,"truncated":false});
    let frozen = legacy.to_string();
    assert!(serde_json::from_value::<OutputMetadata>(legacy.clone()).is_err());
    assert_eq!(legacy.to_string(), frozen);
    let current =
        serde_json::json!({"channels":[],"bytes":0,"start_cursor":0,"end_cursor":0,"closed":true});
    assert!(serde_json::from_value::<OutputMetadata>(current).is_ok());
}
