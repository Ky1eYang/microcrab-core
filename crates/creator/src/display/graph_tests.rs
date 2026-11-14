use super::*;
async fn open_recorder(source: DisplaySource) -> Arc<DisplayRecorder> {
    DisplayRecorder::open(
        source,
        Arc::new(|_| json!({})),
        Arc::new(|_| {}),
        Arc::new(|error| panic!("{error}")),
    )
    .await
    .unwrap()
}
async fn recorder_graph_contract(source: DisplaySource) {
    let recorder = open_recorder(source.clone()).await;
    recorder
        .append_block(
            "native-user".into(),
            "user".into(),
            json!({"canonical_ref":"native-user","text":"question"}),
        )
        .await
        .unwrap();
    recorder.checkpoint("before-attempt".into()).await.unwrap();
    recorder
        .record(observation(ObservationState::Started))
        .await
        .unwrap();
    recorder
        .record(observation(ObservationState::ReasoningDelta {
            text: "中文".into(),
        }))
        .await
        .unwrap();
    recorder
        .record(observation(ObservationState::ReasoningDelta {
            text: "🙂".into(),
        }))
        .await
        .unwrap();
    recorder.close().await.unwrap();
    drop(recorder);
    let recorder = open_recorder(source.clone()).await;
    recorder
        .record(observation(ObservationState::ReasoningDelta {
            text: "tail".into(),
        }))
        .await
        .unwrap();
    recorder
        .record(observation(ObservationState::TextDelta {
            text: "first text".into(),
        }))
        .await
        .unwrap();
    recorder
        .record(observation(ObservationState::ReasoningDelta {
            text: "later reasoning".into(),
        }))
        .await
        .unwrap();
    recorder
        .record(observation(ObservationState::ToolCallDelta {
            index: 0,
            id: Some("call".into()),
            name: Some("tool".into()),
            arguments: "{".into(),
        }))
        .await
        .unwrap();
    recorder
        .record(observation(ObservationState::TextDelta {
            text: "last text".into(),
        }))
        .await
        .unwrap();
    recorder
        .record(observation(ObservationState::Invalidated {
            reason: "failed".into(),
        }))
        .await
        .unwrap();
    recorder.close().await.unwrap();
    drop(recorder);
    let graph = source.graph().unwrap().unwrap();
    let mut nodes: Vec<_> = graph.blocks.values().cloned().collect();
    nodes.sort_by_key(|block| block.sequence);
    assert_eq!(
        nodes
            .iter()
            .map(|block| block.block_type.as_str())
            .collect::<Vec<_>>(),
        vec![
            "user",
            "reasoning",
            "text",
            "reasoning",
            "tool_call",
            "text"
        ]
    );
    assert_eq!(
        graph.content_bytes(&nodes[1].block_id),
        Some(("中文🙂tail").len() as u64)
    );
    for pair in nodes.windows(2) {
        assert_eq!(
            pair[1].parent_block_id.as_deref(),
            Some(pair[0].block_id.as_str())
        );
    }
    let old = source.query(0, None, 128).unwrap().unwrap().records;
    let chunks: Vec<_> = old
        .iter()
        .filter(|record| record.event["display_block"]["block_id"] == nodes[1].block_id)
        .collect();
    assert_eq!(chunks.len(), 3);
    assert_eq!(
        chunks[1].event["display_block"]["content_offset"],
        "中文".len()
    );
    assert_eq!(
        chunks[2].event["display_block"]["content_offset"],
        "中文🙂".len()
    );
    assert_eq!(chunks[2].event["stream_offset"], "中文🙂".len());
    assert_eq!(
        graph.attempt_parents["exact-attempt"].as_deref(),
        Some(nodes[0].block_id.as_str())
    );
    assert_eq!(
        graph.checkpoints["before-attempt"].as_deref(),
        Some(nodes[0].block_id.as_str())
    );
    let recorder = open_recorder(source.clone()).await;
    assert!(
        recorder
            .select_parent(Some("missing".into()))
            .await
            .is_err()
    );
    recorder
        .select_attempt_parent("exact-attempt".into())
        .await
        .unwrap();
    let mut retry = observation(ObservationState::Started);
    retry.call_id = "retry-attempt".into();
    recorder.record(retry.clone()).await.unwrap();
    retry.state = ObservationState::TextDelta {
        text: "replacement".into(),
    };
    recorder.record(retry).await.unwrap();
    recorder.close().await.unwrap();
    let selected = source.graph().unwrap().unwrap();
    let head = selected.selected_head.as_ref().unwrap();
    assert_eq!(
        selected.blocks[head].parent_block_id.as_deref(),
        Some(nodes[0].block_id.as_str())
    );
    for node in &nodes {
        assert_eq!(selected.blocks[&node.block_id], *node);
    }
    assert_eq!(
        source
            .query(0, Some(old.len() as u64), 128)
            .unwrap()
            .unwrap()
            .records,
        old
    );
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_recorder_segments_restart_checkpoint_and_exact_retry_graph() {
    let directory = tempfile::tempdir().unwrap();
    recorder_graph_contract(DisplaySource::File {
        directory: directory.path().join("display"),
        scope: DisplayScope::new("owner", "root", "session"),
    })
    .await;
}
#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_recorder_segments_restart_checkpoint_and_exact_retry_graph() {
    let directory = tempfile::tempdir().unwrap();
    recorder_graph_contract(DisplaySource::Sqlite {
        database: directory.path().join("display.db"),
        scope: DisplayScope::new("owner", "root", "session"),
    })
    .await;
}
