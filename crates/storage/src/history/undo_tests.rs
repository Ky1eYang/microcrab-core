//! User input boundaries differ from arbitrary checkpoint parents.
use super::*;
use microcrab_session::{
    Limits, SessionState,
    context::{ContentItem, Context},
};
use serde_json::json;

fn recover(context: &Context) -> Recover {
    let mut state = SessionState::new(Limits::default());
    state.epoch_id = context.epoch();
    Recover {
        state,
        context: context.snapshot().unwrap(),
        continuation: None,
        workspace: None,
        cache_report: None,
        agent: None,
        prompt_config: None,
    }
}

async fn exact_boundary(journal: CreatorJournal) {
    let mut context =
        Context::new("system".into(), vec![], Limits::default().max_context_bytes).unwrap();
    journal
        .commit_boundary(
            recover(&context),
            microcrab_session::NavigationBoundary::InterTurn,
        )
        .await
        .unwrap();
    let root = journal.cursor().await.unwrap();
    journal.commit(recover(&context)).await.unwrap();
    assert_eq!(
        journal.cursor().await.as_ref(),
        Some(&root),
        "an unchanged internal flush must retain certified boundary identity"
    );
    context.admit_user("same input", &json!({})).unwrap();
    let first_index = context
        .items()
        .iter()
        .position(|item| matches!(item, ContentItem::User { .. }))
        .unwrap();
    let first_input = context.items()[first_index].clone();
    context
        .append_response(&microcrab_session::protocol::Response {
            content: "round one".into(),
            tool_calls: vec![],
            usage: Default::default(),
            provider_metadata: None,
        })
        .unwrap();
    journal
        .commit_boundary(
            recover(&context),
            microcrab_session::NavigationBoundary::InterTurn,
        )
        .await
        .unwrap();
    // A round or settings checkpoint inside the input's turn is not its undo boundary.
    context
        .append_response(&microcrab_session::protocol::Response {
            content: "round two".into(),
            tool_calls: vec![],
            usage: Default::default(),
            provider_metadata: None,
        })
        .unwrap();
    journal
        .commit_boundary(
            recover(&context),
            microcrab_session::NavigationBoundary::InterTurn,
        )
        .await
        .unwrap();
    let before_second = journal.cursor().await.unwrap();
    let first_turn_items = context.items().to_vec();
    context.admit_user("same input", &json!({})).unwrap();
    let second_index = context.items().len() - 1;
    let second_input = context.items()[second_index].clone();
    journal.commit(recover(&context)).await.unwrap();
    context
        .append_response(&microcrab_session::protocol::Response {
            content: "second answer".into(),
            tool_calls: vec![],
            usage: Default::default(),
            provider_metadata: None,
        })
        .unwrap();
    journal
        .commit_boundary(
            recover(&context),
            microcrab_session::NavigationBoundary::InterTurn,
        )
        .await
        .unwrap();
    let head = journal.cursor().await.unwrap();
    let checkpoint_undo = journal.prepare_travel(Travel::Undo).await.unwrap();
    assert_eq!(
        checkpoint_undo.context.items, first_turn_items,
        "checkpoint mode must skip the internal admitted-input node"
    );
    assert_eq!(journal.cursor().await.as_ref(), Some(&head));
    journal.cancel_pending().await;
    let selected = journal
        .prepare_undo_input(head.clone(), first_index, &first_input)
        .await
        .unwrap();
    assert_eq!(journal.cursor().await.as_ref(), Some(&head));
    assert!(selected.context.items.is_empty());
    journal.cancel_pending().await;
    let selected = journal
        .prepare_undo_input(head.clone(), second_index, &second_input)
        .await
        .unwrap();
    assert_eq!(selected.context.items, first_turn_items);
    journal.recovered(selected).await.unwrap();
    assert_eq!(journal.cursor().await.as_ref(), Some(&before_second));
    let redone = journal.prepare_travel(Travel::Redo).await.unwrap();
    journal.recovered(redone).await.unwrap();
    assert_eq!(journal.cursor().await.as_ref(), Some(&head));
    let selected = journal
        .prepare_undo_input(head.clone(), first_index, &first_input)
        .await
        .unwrap();
    journal.recovered(selected).await.unwrap();
    assert_eq!(journal.cursor().await.as_ref(), Some(&root));
    let redone = journal.prepare_travel(Travel::Redo).await.unwrap();
    journal.recovered(redone).await.unwrap();
    assert_eq!(journal.cursor().await.as_ref(), Some(&head));
    assert!(
        journal
            .prepare_undo_input(root, first_index, &first_input)
            .await
            .is_err()
    );
    // Repeated input text is disambiguated by its exact canonical item index.
    assert!(
        journal
            .prepare_undo_input(head.clone(), second_index, &first_input)
            .await
            .is_ok()
    );
    journal.cancel_pending().await;
    let wrong = ContentItem::User {
        parts: vec![microcrab_session::InputPart::Text {
            text: "different input".into(),
        }],
        source: None,
    };
    assert!(
        journal
            .prepare_undo_input(head.clone(), first_index, &wrong)
            .await
            .is_err()
    );
    assert!(journal.selection.lock().await.pending.is_none());
    assert_eq!(journal.cursor().await.as_ref(), Some(&head));
    journal.close().await.unwrap();
}

#[tokio::test]
async fn targeted_undo_memory_skips_round_checkpoints_and_redoes_one_step() {
    let (journal, _) = CreatorJournal::open("unused-memory", "input-undo".into(), false, false)
        .await
        .unwrap();
    exact_boundary(journal).await;
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn targeted_undo_file_skips_round_checkpoints_and_redoes_one_step() {
    let root = std::env::temp_dir().join(format!("microcrab-input-undo-{}", uuid::Uuid::new_v4()));
    let (journal, _) =
        CreatorJournal::open(root.to_str().unwrap(), "input-undo".into(), true, false)
            .await
            .unwrap();
    exact_boundary(journal).await;
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn targeted_undo_sqlite_skips_round_checkpoints_and_redoes_one_step() {
    let database = std::env::temp_dir().join(format!(
        "microcrab-input-undo-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let (journal, _) = CreatorJournal::open_sqlite(database, "input-undo".into(), false)
        .await
        .unwrap();
    exact_boundary(journal).await;
}

#[tokio::test]
async fn missing_exact_boundary_does_not_remove_earlier_input_or_stage_travel() {
    let (journal, _) = CreatorJournal::open("unused-memory", "input-undo".into(), false, false)
        .await
        .unwrap();
    let mut context =
        Context::new("system".into(), vec![], Limits::default().max_context_bytes).unwrap();
    journal
        .commit_boundary(
            recover(&context),
            microcrab_session::NavigationBoundary::InterTurn,
        )
        .await
        .unwrap();
    context.admit_user("earlier input", &json!({})).unwrap();
    context
        .admit_user("target with no intervening checkpoint", &json!({}))
        .unwrap();
    journal
        .commit_boundary(
            recover(&context),
            microcrab_session::NavigationBoundary::InterTurn,
        )
        .await
        .unwrap();
    let head = journal.cursor().await.unwrap();
    let index = context.items().len() - 1;
    assert!(
        journal
            .prepare_undo_input(head.clone(), index, &context.items()[index])
            .await
            .is_err()
    );
    assert!(journal.selection.lock().await.pending.is_none());
    assert_eq!(journal.cursor().await.as_ref(), Some(&head));
    journal
        .commit_boundary(
            recover(&context),
            microcrab_session::NavigationBoundary::InterTurn,
        )
        .await
        .unwrap();
}

async fn legacy_commit(journal: &CreatorJournal, recover: Recover) {
    let cursor = journal
        .backend
        .commit(
            journal.cursor().await,
            CreatorEnvelope {
                session_id: journal.session_id.clone(),
                recover,
                request_head: None,
                model_selection: None,
                route_witness: None,
                navigation_boundary: None,
            },
        )
        .await
        .unwrap();
    journal.selection.lock().await.head = Some(cursor.clone());
    journal.acknowledge(&cursor);
}

async fn semantic_boundary(journal: CreatorJournal) {
    use microcrab_session::protocol::{
        AssistantProviderMetadata, Response, ToolCall, ToolCallProviderMetadata, ToolDefinition,
    };
    let tool = ToolDefinition {
        name: "lookup".into(),
        description: "lookup".into(),
        parameters: json!({"type":"object"}),
    };
    let mut context = Context::new(
        "system".into(),
        vec![tool],
        Limits::default().max_context_bytes,
    )
    .unwrap();
    context
        .admit_user("same input", &json!({"session":{"epoch":0},"semantic":1}))
        .unwrap();
    context.append_response(&Response {
        content: "answer".into(), usage: Default::default(),
        tool_calls: vec![ToolCall {
            id: "canonical-call".into(), name: "lookup".into(), arguments: json!({"path":"one"}),
            provider_metadata: Some(ToolCallProviderMetadata::Google { thought_signature: "signature".into() }),
        }],
        provider_metadata: Some(AssistantProviderMetadata::Google { parts: vec![
            json!({"text":"retained reasoning","thought":true,"thoughtSignature":"private-hint"}),
            json!({"text":"answer"}),
            json!({"functionCall":{"id":"canonical-call","name":"lookup","args":{"path":"one"}},"thoughtSignature":"signature"}),
        ] }),
    }).unwrap();
    context
        .append_tool_result("canonical-call".into(), "exact result".into())
        .unwrap();
    context
        .synchronize_state(&json!({"session":{"epoch":1},"semantic":1}))
        .unwrap();
    let mut historical = context.snapshot().unwrap();
    historical.epoch = 1;
    *historical.items.last_mut().unwrap() = serde_json::from_value(json!({"State": {
        "type":"state_snapshot", "state":{"session":{"epoch":1},"semantic":1}
    }}))
    .unwrap();
    let mut context = Context::from_snapshot(historical).unwrap();
    legacy_commit(&journal, recover(&context)).await;
    let boundary = journal.cursor().await.unwrap();
    let boundary_bytes = serde_json::to_vec(
        &journal
            .backend
            .restore(RestoreTarget::Head)
            .await
            .unwrap()
            .checkpoint,
    )
    .unwrap();
    context
        .admit_user("same input", &json!({"session":{"epoch":1},"semantic":1}))
        .unwrap();
    // A later persisted recovery carries neutral Assistant metadata and patches
    // in place of old advancing-epoch snapshots. Historical boundary stays frozen.
    context.refresh_recovery_epoch().unwrap();
    let mut persisted = context.snapshot().unwrap();
    let index = persisted
        .items
        .iter()
        .rposition(|item| matches!(item, ContentItem::User { .. }))
        .unwrap();
    // Historical no-op frames are valid but a live rebuild removes them, changing
    // raw indices without changing a single semantic anchor or invocation.
    persisted.items.insert(
        index,
        serde_json::from_value(json!({"State": {
            "type":"state_patch", "set":{}, "remove":[]
        }}))
        .unwrap(),
    );
    let persisted = Context::from_snapshot(persisted).unwrap();
    legacy_commit(&journal, recover(&persisted)).await;
    let head = journal.cursor().await.unwrap();
    let mut live = persisted.clone();
    live.refresh_recovery_epoch().unwrap();
    let live = live.snapshot().unwrap();
    let live_index = live
        .items
        .iter()
        .rposition(|item| matches!(item, ContentItem::User { .. }))
        .unwrap();
    let input = &live.items[live_index];
    assert!(
        journal
            .prepare_undo_input(head.clone(), live_index, input)
            .await
            .is_err()
    );
    assert!(journal.selection.lock().await.pending.is_none());
    let target = journal
        .prepare_undo_input_from_context(head.clone(), &live, live_index, input)
        .await
        .unwrap();
    assert_eq!(target.context.epoch, 1);
    assert!(
        serde_json::to_string(&target)
            .unwrap()
            .contains("private-hint")
    );
    assert_eq!(journal.cursor().await.as_ref(), Some(&head));
    journal.cancel_pending().await;

    let mut wrong_state = live.clone();
    let mut state_frame = serde_json::to_value(&wrong_state.items[0]).unwrap();
    state_frame["State"]["state"]["semantic"] = json!(2);
    wrong_state.items[0] = serde_json::from_value(state_frame).unwrap();
    assert!(
        journal
            .prepare_undo_input_from_context(head.clone(), &wrong_state, live_index, input)
            .await
            .is_err()
    );
    let mut wrong_prior_user = live.clone();
    if let ContentItem::User { parts, .. } = &mut wrong_prior_user.items[1] {
        parts.push(microcrab_session::InputPart::Text {
            text: "different prior part".into(),
        });
    }
    assert!(
        journal
            .prepare_undo_input_from_context(head.clone(), &wrong_prior_user, live_index, input)
            .await
            .is_err()
    );
    let mut wrong_tool = live.clone();
    let tool = wrong_tool
        .items
        .iter_mut()
        .find(|item| matches!(item, ContentItem::Tool { .. }))
        .unwrap();
    if let ContentItem::Tool { call_id, .. } = tool {
        *call_id = "other-invocation".into();
    }
    assert!(
        journal
            .prepare_undo_input_from_context(head.clone(), &wrong_tool, live_index, input)
            .await
            .is_err()
    );
    let mut malformed = live.clone();
    for item in &mut malformed.items {
        if let ContentItem::Assistant {
            provider_metadata: Some(AssistantProviderMetadata::Neutral { replay_hints, .. }),
            ..
        } = item
        {
            replay_hints.push(microcrab_session::protocol::AssistantReplayHint {
                part_index: usize::MAX,
                scope: "bad".into(),
                kind: "signature".into(),
                data: json!("opaque"),
            });
        }
    }
    assert!(
        journal
            .prepare_undo_input_from_context(head.clone(), &malformed, live_index, input)
            .await
            .is_err()
    );
    assert!(journal.selection.lock().await.pending.is_none());
    assert_eq!(journal.cursor().await.as_ref(), Some(&head));
    let target = journal
        .prepare_undo_input_from_context(head.clone(), &live, live_index, input)
        .await
        .unwrap();
    journal.recovered(target).await.unwrap();
    assert_eq!(journal.cursor().await.as_ref(), Some(&boundary));
    let frozen = journal.backend.restore(RestoreTarget::Head).await.unwrap();
    assert_eq!(
        serde_json::to_vec(&frozen.checkpoint).unwrap(),
        boundary_bytes
    );
    let departure = journal.prepare_travel(Travel::Redo).await.unwrap();
    journal.recovered(departure).await.unwrap();
    assert_eq!(journal.cursor().await.as_ref(), Some(&head));

    let mut epoch_initial = persisted.clone();
    epoch_initial.refresh_recovery_epoch().unwrap();
    let epoch = epoch_initial.epoch();
    let mut first_turn = epoch_initial.clone();
    first_turn
        .admit_user(
            "first input of new epoch",
            &json!({"session":{"epoch":epoch},"semantic":1}),
        )
        .unwrap();
    legacy_commit(&journal, recover(&first_turn)).await;
    let unavailable_head = journal.cursor().await.unwrap();
    let target_index = first_turn.items().len() - 1;
    let error = journal
        .prepare_undo_input(
            unavailable_head.clone(),
            target_index,
            &first_turn.items()[target_index],
        )
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("no certified same-epoch ancestor")
    );
    assert_eq!(journal.cursor().await.as_ref(), Some(&unavailable_head));
    assert!(journal.selection.lock().await.pending.is_none());
    assert!(
        journal
            .backend
            .travel(unavailable_head, Travel::Redo)
            .await
            .is_err()
    );

    journal
        .commit_boundary(
            recover(&epoch_initial),
            microcrab_session::NavigationBoundary::EpochInitial,
        )
        .await
        .unwrap();
    let initial_cursor = journal.cursor().await.unwrap();
    let inherited = epoch_initial.items().to_vec();
    // Input preparation may persist runtime variables while Context is still the
    // exact initial prefix. That internal durability node is never a nav target.
    let mut internal = recover(&epoch_initial);
    internal.state.selected_mode = Some("input-specific-mode".into());
    journal.commit(internal).await.unwrap();
    journal.commit(recover(&first_turn)).await.unwrap();
    let departure = journal.cursor().await.unwrap();
    let initial = journal
        .prepare_undo_input(
            departure.clone(),
            target_index,
            &first_turn.items()[target_index],
        )
        .await
        .unwrap();
    assert_eq!(initial.context.epoch, epoch);
    assert_eq!(initial.context.items, inherited);
    assert_eq!(initial.state.selected_mode, None);
    journal.recovered(initial).await.unwrap();
    assert_eq!(journal.cursor().await.as_ref(), Some(&initial_cursor));
    let redone = journal.prepare_travel(Travel::Redo).await.unwrap();
    journal.recovered(redone).await.unwrap();
    assert_eq!(journal.cursor().await.as_ref(), Some(&departure));
    journal.close().await.unwrap();
}

#[tokio::test]
async fn semantic_undo_memory_preserves_anchors_and_maps_live_indices() {
    let (journal, _) = CreatorJournal::open("unused-memory", "semantic-undo".into(), false, false)
        .await
        .unwrap();
    semantic_boundary(journal).await;
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn semantic_undo_file_preserves_anchors_and_maps_live_indices() {
    let root =
        std::env::temp_dir().join(format!("microcrab-semantic-undo-{}", uuid::Uuid::new_v4()));
    let (journal, _) =
        CreatorJournal::open(root.to_str().unwrap(), "semantic-undo".into(), true, false)
            .await
            .unwrap();
    semantic_boundary(journal).await;
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn semantic_undo_sqlite_preserves_anchors_and_maps_live_indices() {
    let database = std::env::temp_dir().join(format!(
        "microcrab-semantic-undo-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let (journal, _) = CreatorJournal::open_sqlite(database, "semantic-undo".into(), false)
        .await
        .unwrap();
    semantic_boundary(journal).await;
}

#[tokio::test]
async fn same_summary_different_compaction_generation_is_a_barrier() {
    let (journal, _) =
        CreatorJournal::open("unused-memory", "generation-undo".into(), false, false)
            .await
            .unwrap();
    let mut context =
        Context::new("system".into(), vec![], Limits::default().max_context_bytes).unwrap();
    let state = json!({"session":{"epoch":0}});
    context
        .compact_baseline("same summary".into(), &state)
        .unwrap();
    let marker = context.compaction().cloned();
    journal
        .commit_boundary(
            recover(&context),
            microcrab_session::NavigationBoundary::InterTurn,
        )
        .await
        .unwrap();
    context
        .compact_baseline("same summary".into(), &state)
        .unwrap();
    assert_eq!(context.compaction(), marker.as_ref());
    context.admit_user("target", &state).unwrap();
    journal
        .commit_boundary(
            recover(&context),
            microcrab_session::NavigationBoundary::InterTurn,
        )
        .await
        .unwrap();
    let head = journal.cursor().await.unwrap();
    let index = context.items().len() - 1;
    let error = journal
        .prepare_undo_input(head.clone(), index, &context.items()[index])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("compaction baseline"));
    assert_eq!(journal.cursor().await.as_ref(), Some(&head));
    assert!(journal.selection.lock().await.pending.is_none());
}
