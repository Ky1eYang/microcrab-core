//! Raw UTF-8 caps are independent of response envelopes and never hide drain loss.
use super::*;
use crate::runtime::execution::{
    CancellationController, ExecutionSnapshot, OutputOptions, OutputWriter, buffer::Buffer,
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

fn contents(page: &Value) -> String {
    page["chunks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|chunk| chunk["content"].as_str().unwrap())
        .collect()
}
async fn output(context: &ToolContext, text: &str, terminal: bool) -> (ExecutionId, OutputWriter) {
    let registry = context.state.executions();
    let owner = registry.clone();
    let text = text.to_owned();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let mut execution = registry
        .start(
            ExecutionSnapshot::new(
                "owner".into(),
                "producer".into(),
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
                writer.append(None, &text)?;
                if terminal {
                    writer.finish(Ok(()))?;
                }
                assert!(sender.send(writer).is_ok());
                Ok(ToolOutput::Buffered(output))
            },
        )
        .unwrap();
    let id = execution.id;
    execution.activate().unwrap();
    execution.wait().await.unwrap();
    execution.detach();
    (id, receiver.await.unwrap())
}
async fn invoke(mut context: ToolContext, id: Value, action: &str, args: Value) -> Value {
    let arguments = json!({"id":id,"action":action,"args":args});
    crate::runtime::wasm::test_admission::admit_arguments(&mut context, None, arguments.clone());
    match Job.execute(arguments, context).await.unwrap() {
        ToolOutput::Final(output) => serde_json::from_str(&output.content).unwrap(),
        ToolOutput::Buffered(_) => panic!("job returns an observation"),
    }
}

#[tokio::test]
async fn scalar_read_caps_content_from_cursor_without_consuming() {
    let context = super::batch_tests::context();
    let (id, _writer) = output(&context, "a中🙂z", false).await;
    let handle = json!(format!("owner/{}", id.0));
    let page = invoke(
        context.clone(),
        handle.clone(),
        "read",
        json!({"cursor":1,"max_bytes":4}),
    )
    .await;
    assert_eq!(contents(&page), "中");
    assert_eq!(page["cursor"], 1);
    assert_eq!(page["next_cursor"], 4);
    assert_eq!(page["end_cursor"], 9);
    let tiny = invoke(
        context.clone(),
        handle.clone(),
        "read",
        json!({"cursor":4,"max_bytes":1}),
    )
    .await;
    assert_eq!(contents(&tiny), "");
    assert_eq!(tiny["next_cursor"], 4);
    assert!(tiny.get("truncated").is_none());
    let rest = invoke(
        context.clone(),
        handle,
        "read",
        json!({"cursor":4,"max_bytes":5}),
    )
    .await;
    assert_eq!(contents(&rest), "🙂z");
    assert_eq!(
        context
            .state
            .executions()
            .get(id)
            .unwrap()
            .output
            .unwrap()
            .start_cursor,
        0
    );
    context.state.executions().close().await.unwrap();
}

#[tokio::test]
async fn capped_reads_keep_prefix_and_running_append_remains_readable() {
    let context = super::batch_tests::context();
    let (id, writer) = output(&context, "older中🙂z", false).await;
    let handle = json!(format!("owner/{}", id.0));
    let page = invoke(
        context.clone(),
        handle.clone(),
        "read",
        json!({"max_bytes":6}),
    )
    .await;
    assert_eq!(contents(&page), "older");
    assert_eq!(page["cursor"], 0);
    assert_eq!(page["start_cursor"], 0);
    assert_eq!(page["next_cursor"], 5);
    writer.append(Some("stdout"), "new中").unwrap();
    let next = invoke(context.clone(), handle, "read", json!({"cursor":5})).await;
    assert_eq!(contents(&next), "中🙂znew中");
    assert_eq!(next["cursor"], 5);
    assert_eq!(
        context
            .state
            .executions()
            .get(id)
            .unwrap()
            .output
            .unwrap()
            .start_cursor,
        0
    );
    context.state.executions().close().await.unwrap();
}
#[tokio::test]
async fn tiny_read_cap_returns_no_character_without_mutation_or_release() {
    let context = super::batch_tests::context();
    let (id, _writer) = output(&context, "🙂old", true).await;
    let registry = context.state.executions();
    let before = registry.read_capped(id, 0, 4096, None).unwrap();
    let availability_before = registry.get(id).unwrap().output_availability;
    let page = invoke(
        context.clone(),
        json!(format!("owner/{}", id.0)),
        "read",
        json!({"max_bytes":3}),
    )
    .await;
    assert_eq!(contents(&page), "");
    assert_eq!(page["next_cursor"], 0);
    assert_eq!(
        serde_json::to_value(registry.read_capped(id, 0, 4096, None).unwrap()).unwrap(),
        serde_json::to_value(before).unwrap()
    );
    assert_eq!(
        registry.get(id).unwrap().output_availability,
        availability_before
    );
    let page = registry.read_capped(id, 0, 4096, Some(4)).unwrap();
    assert_eq!(page.chunks[0].content, "🙂");
    registry
        .release(id, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        registry.get(id).unwrap().output_availability,
        Some(crate::runtime::execution::OutputAvailability::Released)
    );
}

#[tokio::test]
async fn scalar_and_batch_caps_share_policy_and_one_serialized_response_budget() {
    let mut context = super::batch_tests::context();
    let limits = crate::Limits {
        max_tool_output_bytes: 2048,
        max_execution_buffers: 4,
        ..crate::Limits::default()
    };
    context.state = crate::State::new(limits);
    crate::runtime::wasm::test_admission::admit(&mut context, None);
    let text = "\"中🙂\\\n".repeat(500);
    let (one, _one) = output(&context, &text, false).await;
    let (two, _two) = output(&context, &text, false).await;
    for action in ["read"] {
        let value = invoke(
            context.clone(),
            json!([format!("owner/{}", one.0), format!("owner/{}", two.0)]),
            action,
            json!({"max_bytes":16777216}),
        )
        .await;
        assert!(serde_json::to_vec(&value).unwrap().len() <= 2048);
        for row in value["results"].as_array().unwrap() {
            assert_eq!(row["ok"], true);
            let page = &row["result"];
            let selected = contents(page);
            assert!(!selected.is_empty());
            assert!(text.starts_with(&selected));
        }
    }
    context.state.executions().close().await.unwrap();
}

#[test]
fn raw_caps_reject_invalid_bounds_and_action_fields_before_effects() {
    for id in [json!("owner/1"), json!(["owner/1", "owner/2"])] {
        for action in ["read"] {
            for cap in [
                json!(null),
                json!(0),
                json!(-1),
                json!(1.5),
                json!("4"),
                json!(16777217),
                json!(u64::MAX),
            ] {
                assert!(
                    validate(&json!({"id":id,"action":action,"args":{"max_bytes":cap}})).is_err()
                );
            }
            for cap in [1, 16777216] {
                assert!(
                    validate(&json!({"id":id,"action":action,"args":{"max_bytes":cap}})).is_ok()
                );
            }
        }
        for action in ["status", "wait", "cancel", "release", "write"] {
            assert!(validate(&json!({"id":id,"action":action,"args":{"max_bytes":4}})).is_err());
        }
        assert!(
            validate(&json!({"id":id,"action":"drain","args":{"cursor":0,"max_bytes":4}})).is_err()
        );
    }
}

#[test]
fn exact_read_budget_search_handles_decimal_cursors_utf8_and_escaping() {
    for (base, text) in [
        (0, "a".repeat(140)),
        (90, "a".repeat(40)),
        (990, "a".repeat(40)),
        (9990, "中🙂a\"\\\n".repeat(10)),
        (u64::MAX - 150, "a".repeat(140)),
    ] {
        let mut buffer = Buffer::new(OutputOptions::default(), 4096).unwrap();
        buffer.start = base;
        buffer.end = base;
        buffer.append(Some("out"), &text).unwrap();
        for cap in [1, 4, 9, 10, 39, 40, text.len()] {
            for budget in [160, 180, 200, 220, 240, 256, 300, 360, 512] {
                match buffer.page_capped(ExecutionId(1), base, budget, 10, Some(cap)) {
                    Ok(page) => {
                        let selected: String =
                            page.chunks.iter().map(|c| c.content.as_str()).collect();
                        assert!(text.starts_with(&selected));
                        assert!(selected.len() <= cap);
                        assert!(serde_json::to_vec(&page).unwrap().len() <= budget);
                        // Exhaust all legal longer prefixes to prove maximal selection.
                        for end in selected.len() + 1..=text.len().min(cap) {
                            if !text.is_char_boundary(end) {
                                continue;
                            }
                            let mut candidate = page.clone();
                            candidate.chunks = vec![crate::runtime::execution::OutputChunk {
                                channel: Some("out".into()),
                                content: text[..end].into(),
                                cursor: base,
                                next_cursor: base + end as u64,
                            }];
                            candidate.next_cursor = base + end as u64;
                            assert!(serde_json::to_vec(&candidate).unwrap().len() > budget);
                        }
                    }
                    Err(Error::Limit(_)) => {}
                    Err(error) => panic!("unexpected read failure: {error}"),
                }
            }
        }
    }
}
#[test]
fn read_pages_keep_channel_order_and_exact_bounds_after_retention_expiry() {
    for maximum in [256, 360, 512, 1024, 4096] {
        for cap in [1, 4, 7, 16, 128, 4096] {
            let mut buffer = Buffer::new(OutputOptions::default(), 256).unwrap();
            for _ in 0..40 {
                buffer.append(Some("out"), "ab中").unwrap();
                buffer.append(Some("err"), "🙂\"\\\nZ").unwrap();
            }
            let before = buffer.persisted();
            let all = buffer.page(ExecutionId(1), 0, 16384, 10).unwrap();
            let text: String = all.chunks.iter().map(|c| c.content.as_str()).collect();
            if let Ok(page) = buffer.page_capped(ExecutionId(1), 0, maximum, 10, Some(cap)) {
                let selected: String = page.chunks.iter().map(|c| c.content.as_str()).collect();
                assert!(text.starts_with(&selected));
                assert!(selected.len() <= cap);
                assert!(serde_json::to_vec(&page).unwrap().len() <= maximum);
                assert_eq!(page.cursor, buffer.start);
                for pair in page.chunks.windows(2) {
                    assert_eq!(pair[0].next_cursor, pair[1].cursor);
                }
            }
            assert_eq!(buffer.persisted(), before, "read must not consume");
        }
    }
}
