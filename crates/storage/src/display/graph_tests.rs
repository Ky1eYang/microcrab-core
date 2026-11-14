use super::*;

fn legacy_selection_reopen(source: DisplaySource) {
    let mut writer = source.open().unwrap();
    let legacy = ArchiveRecord {
        sequence: 1,
        attempt_id: "old".into(),
        event: json!({"phase":"text_delta","text":"old bytes"}),
    };
    writer.append(0, &legacy).unwrap();
    drop(writer);
    let graph = source.graph().unwrap().unwrap();
    let mut current = record(2, "current", "new graph turn");
    graph.decorate(&mut current, 1).unwrap();
    let exact_head = current.event["display_block"]["block_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut writer = source.open().unwrap();
    writer.append(1, &current).unwrap();
    let unknown = ArchiveRecord {
        sequence: 3,
        attempt_id: "undo".into(),
        event: json!({
            "phase":"display_head_selected","selected_head_known":false,
            "display_head":{"previous_block_id":exact_head,"selected_block_id":null}
        }),
    };
    writer.append(2, &unknown).unwrap();
    drop(writer);
    let unknown_graph = source.graph().unwrap().unwrap();
    assert!(!unknown_graph.selection_known);
    assert_eq!(unknown_graph.selected_head, None);
    assert!(
        unknown_graph
            .import_event()
            .unwrap_err()
            .to_string()
            .contains("ordering is unavailable")
    );
    assert_eq!(
        source.query(0, Some(1), 1).unwrap().unwrap().records,
        vec![legacy]
    );
    let redo = ArchiveRecord {
        sequence: 4,
        attempt_id: "redo".into(),
        event: json!({
            "phase":"display_head_selected","selected_head_known":true,
            "display_head":{"previous_block_id":null,"selected_block_id":exact_head}
        }),
    };
    let mut writer = source.open().unwrap();
    writer.append(3, &redo).unwrap();
    drop(writer);
    let restored = source.graph().unwrap().unwrap();
    assert!(restored.selection_known);
    assert_eq!(restored.selected_head.as_deref(), Some(exact_head.as_str()));
    assert!(restored.import_event().is_ok());
}
#[cfg(feature = "storage-file")]
#[test]
fn file_legacy_selection_reopens_unknown_and_redo_restores_exact_head() {
    let directory = Directory::new();
    legacy_selection_reopen(DisplaySource::File {
        directory: directory.0.join("legacy"),
        scope: DisplayScope::new("owner", "root", "session"),
    });
}
#[cfg(feature = "storage-sqlite")]
#[test]
fn sqlite_legacy_selection_reopens_unknown_and_redo_restores_exact_head() {
    let directory = Directory::new();
    legacy_selection_reopen(DisplaySource::Sqlite {
        database: directory.0.join("legacy.db"),
        scope: DisplayScope::new("owner", "root", "session"),
    });
}
fn graph_contract(source: DisplaySource) {
    let scope = match &source {
        DisplaySource::File { scope, .. } | DisplaySource::Sqlite { scope, .. } => scope.clone(),
    };
    let mut graph = DisplayGraph::empty(scope);
    let mut writer = source.open().unwrap();
    let mut commit = |graph: &mut DisplayGraph, mut row: ArchiveRecord| {
        graph.decorate(&mut row, 123).unwrap();
        writer.append(graph.high_water, &row).unwrap();
        graph.apply(&row).unwrap();
        row
    };
    let a = commit(&mut graph, record(1, "a", "中文"));
    let a_id = a.event["display_block"]["block_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let b = commit(&mut graph, record(2, "b", "b"));
    let b_id = b.event["display_block"]["block_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let c = commit(&mut graph, record(3, "c", "c"));
    let c_id = c.event["display_block"]["block_id"]
        .as_str()
        .unwrap()
        .to_owned();
    drop(commit);
    let selection = ArchiveRecord {
        sequence: 4,
        attempt_id: "select".into(),
        event: json!({"phase":"display_head_selected",
        "display_head":{"previous_block_id":c_id,"selected_block_id":b_id}}),
    };
    writer.append(3, &selection).unwrap();
    graph.apply(&selection).unwrap();
    let mut d = record(5, "d", "d");
    graph.decorate(&mut d, 1).unwrap(); // Timestamp never determines graph order.
    let d_id = d.event["display_block"]["block_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut corrupt = d.clone();
    corrupt.event["display_block"]["content_offset"] = json!(1);
    assert!(writer.append(4, &corrupt).is_err());
    assert_eq!(writer.revision().unwrap(), 4);
    writer.append(4, &d).unwrap();
    graph.apply(&d).unwrap();
    drop(writer);
    let reopened = source.graph().unwrap().unwrap();
    assert_eq!(reopened.selected_head.as_deref(), Some(d_id.as_str()));
    assert_eq!(
        reopened.blocks[&c_id].parent_block_id.as_deref(),
        Some(b_id.as_str())
    );
    assert_eq!(
        reopened.blocks[&d_id].parent_block_id.as_deref(),
        Some(b_id.as_str())
    );
    assert_eq!(
        reopened.blocks[&b_id].parent_block_id.as_deref(),
        Some(a_id.as_str())
    );
    assert_eq!(reopened.content_bytes(&a_id), Some("中文".len() as u64));
    assert_eq!(
        source
            .graph_at(Some(3))
            .unwrap()
            .unwrap()
            .selected_head
            .as_deref(),
        Some(c_id.as_str())
    );
    assert_eq!(
        source.query(0, Some(3), 3).unwrap().unwrap().records,
        vec![a, b, c]
    );
    let mut writer = source.open().unwrap();
    let mut resumed = record(6, "d", "尾部");
    reopened.decorate(&mut resumed, 999).unwrap();
    assert_eq!(resumed.event["display_block"]["block_id"], d_id);
    assert_eq!(resumed.event["display_block"]["content_offset"], 1);
    writer.append(5, &resumed).unwrap();
    drop(writer);
    let before_gap = source.graph().unwrap().unwrap();
    assert!(!before_gap.projection_incomplete);
    let selected = before_gap.selected_head.clone();
    let blocks = before_gap.blocks.clone();
    let marker = ArchiveRecord {
        sequence: 7,
        attempt_id: "projection".into(),
        event: json!({"phase":"display_projection_unavailable","display_graph_version":1,"reason":"canonical_source_read_failed"}),
    };
    let mut writer = source.open().unwrap();
    writer.append(6, &marker).unwrap();
    drop(writer);
    let gap = source.graph().unwrap().unwrap();
    assert!(gap.projection_incomplete);
    assert_eq!(gap.selected_head, selected);
    assert_eq!(gap.blocks, blocks);
    assert!(
        !source
            .graph_at(Some(6))
            .unwrap()
            .unwrap()
            .projection_incomplete
    );
    let mut later = record(8, "d", "successful later content");
    gap.decorate(&mut later, 1000).unwrap();
    let mut writer = source.open().unwrap();
    writer.append(7, &later).unwrap();
    drop(writer);
    assert!(source.graph().unwrap().unwrap().projection_incomplete);
}
#[cfg(feature = "storage-file")]
#[test]
fn file_graph_restart_integrity_and_immutable_sibling_branch() {
    let directory = Directory::new();
    graph_contract(DisplaySource::File {
        directory: directory.0.join("graph"),
        scope: DisplayScope::new("owner", "root", "session"),
    });
}
#[cfg(feature = "storage-sqlite")]
#[test]
fn sqlite_graph_restart_integrity_and_immutable_sibling_branch() {
    let directory = Directory::new();
    graph_contract(DisplaySource::Sqlite {
        database: directory.0.join("graph.db"),
        scope: DisplayScope::new("owner", "root", "session"),
    });
}
#[test]
fn graph_fork_keeps_original_ids_and_rejects_tampered_certificates() {
    let mut original = DisplayGraph::empty(DisplayScope::new("owner", "root", "source"));
    let mut row = record(1, "a", "thought");
    original.decorate(&mut row, 1).unwrap();
    original.apply(&row).unwrap();
    original.apply(&ArchiveRecord {
        sequence: 2,
        attempt_id: "projection".into(),
        event: json!({"phase":"display_projection_unavailable","reason":"canonical_source_read_failed"}),
    }).unwrap();
    let id = original.selected_head.clone().unwrap();
    let mut imported = DisplayGraph::empty(DisplayScope::new("owner", "fork", "fork"));
    let event = original.import_event().unwrap();
    let certificate = ArchiveRecord {
        sequence: 1,
        attempt_id: "fork".into(),
        event: event.clone(),
    };
    let mut corrupt = certificate.clone();
    corrupt.event["import"]["blocks"][0]["identity_scope"]["session"] = json!("other");
    assert!(imported.apply(&corrupt).is_err());
    imported.apply(&certificate).unwrap();
    assert_eq!(imported.selected_head.as_deref(), Some(id.as_str()));
    assert_eq!(imported.blocks[&id], original.blocks[&id]);
    assert!(imported.projection_incomplete);
    let mut next = record(2, "b", "new fork content");
    imported.decorate(&mut next, 2).unwrap();
    imported.apply(&next).unwrap();
    let head = imported.selected_head.clone().unwrap();
    assert_eq!(
        imported.blocks[&head].parent_block_id.as_deref(),
        Some(id.as_str())
    );
    let mut nested = DisplayGraph::empty(DisplayScope::new("owner", "nested", "nested"));
    nested
        .apply(&ArchiveRecord {
            sequence: 1,
            attempt_id: "nested".into(),
            event: imported.import_event().unwrap(),
        })
        .unwrap();
    assert_eq!(nested.blocks[&id], original.blocks[&id]);
    assert_eq!(nested.selected_head.as_deref(), Some(head.as_str()));
    assert!(nested.projection_incomplete);
}
#[test]
fn graph_navigation_fences_and_immutable_node_metadata() {
    let mut graph = DisplayGraph::empty(DisplayScope::new("owner", "root", "session"));
    let mut first = record(1, "attempt", "中文");
    graph.decorate(&mut first, 1).unwrap();
    graph.apply(&first).unwrap();
    let mut next = record(2, "attempt", "tail");
    graph.decorate(&mut next, 2).unwrap();
    for field in [
        "block_id",
        "parent_block_id",
        "block_type",
        "content_ref",
        "timestamp_ms",
        "sequence",
    ] {
        let mut corrupt = next.clone();
        corrupt.event["display_block"][field] = json!("changed");
        assert!(
            graph.validate(&corrupt).is_err(),
            "accepted mutation of {field}"
        );
    }
    let mut intent = ArchiveRecord {
        sequence: 2,
        attempt_id: "travel-a".into(),
        event: json!({"phase":"selection_intent","selected_head":graph.selected_head}),
    };
    graph.apply(&intent).unwrap();
    assert!(graph.pending_selection.is_some());
    intent.sequence = 3;
    intent.attempt_id = "travel-b".into();
    graph.apply(&intent).unwrap();
    let mut finish = ArchiveRecord {
        sequence: 4,
        attempt_id: "travel-b".into(),
        event: json!({"phase":"selection_abort"}),
    };
    graph.apply(&finish).unwrap();
    assert!(
        graph.pending_selection.is_some(),
        "a sibling certificate cleared an outstanding intent"
    );
    finish.sequence = 5;
    finish.attempt_id = "travel-a".into();
    graph.apply(&finish).unwrap();
    assert!(graph.pending_selection.is_none());
}
#[test]
fn graph_legacy_content_is_explicit_and_does_not_invent_nodes() {
    let mut graph = DisplayGraph::empty(DisplayScope::new("owner", "root", "session"));
    graph
        .apply(&record(1, "legacy", "historical body"))
        .unwrap();
    assert_eq!(graph.legacy_content_records, 1);
    assert!(graph.blocks.is_empty());
    assert!(graph.selected_head.is_none());
}
