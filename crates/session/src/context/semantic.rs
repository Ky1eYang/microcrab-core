//! Narrow, temporary Undo proofs over immutable Context snapshots. Transport hints
//! may vary; semantic content and projected State at conversation anchors may not.
use super::*;
use std::sync::Arc;

#[derive(PartialEq)]
struct Anchor {
    index: usize,
    item: ContentItem,
    state: Option<Arc<Value>>,
}

struct Projection {
    anchors: Vec<Anchor>,
    // Includes the projection immediately after the last non-State anchor. A
    // boundary can stop anywhere in the subsequent State-only admission suffix.
    trailing: Vec<Option<Arc<Value>>>,
}

fn neutral_item(item: &ContentItem) -> Result<ContentItem, Error> {
    let mut item = item.clone();
    clear_transport_item(&mut item)?;
    Ok(item)
}

pub(super) fn clear_transport_item(item: &mut ContentItem) -> Result<(), Error> {
    if let ContentItem::Assistant {
        content,
        provider_metadata,
        tool_calls,
    } = item
    {
        // Identical to Context's recovery normalization: validate before removing
        // replay hints, retain ordered reasoning/text/call parts and canonical calls.
        if let Some(metadata) = provider_metadata {
            let mut neutral = metadata
                .normalized(content, tool_calls)
                .map_err(|reason| Error::Protocol(reason.into()))?;
            neutral.clear_replay_hints();
            *metadata = neutral;
        }
        for call in tool_calls {
            call.provider_metadata = None;
        }
    }
    Ok(())
}

fn project(items: &[ContentItem]) -> Result<Projection, Error> {
    let mut state = None;
    let mut anchors = Vec::new();
    let mut trailing = vec![None];
    let mut projected_bytes = 0usize;
    for (index, item) in items.iter().enumerate() {
        if let ContentItem::State(frame) = item {
            match frame {
                StateFrame::StateSnapshot { state: snapshot } => {
                    if !snapshot.is_object() {
                        return Err(Error::protocol("undo State snapshot must be an object"));
                    }
                    state = Some(Arc::new(snapshot.clone()));
                }
                StateFrame::StatePatch { .. } => {
                    let state = state
                        .as_mut()
                        .ok_or_else(|| Error::protocol("undo State patch precedes snapshot"))?;
                    microcrab_transport::apply_state_patch(Arc::make_mut(state), frame)
                        .map_err(Error::protocol)?;
                }
            }
            if let Some(state) = &state {
                projected_bytes =
                    projected_bytes.saturating_add(serde_json::to_vec(state.as_ref())?.len());
                if projected_bytes > 64 * 1024 * 1024 {
                    return Err(crate::budget::reject(
                        crate::LimitKind::ContextBytes,
                        "undo State projection byte budget",
                    ));
                }
            }
            trailing.push(state.clone());
        } else {
            anchors.push(Anchor {
                index,
                item: neutral_item(item)?,
                state: state.clone(),
            });
            trailing.clear();
            trailing.push(state.clone());
        }
    }
    Ok(Projection { anchors, trailing })
}

fn equal_anchors(left: &[Anchor], right: &[Anchor]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.item == right.item && left.state == right.state)
}

fn recovery_terminal_equal(
    source: &Option<Arc<Value>>,
    live: &Option<Arc<Value>>,
    source_epoch: u64,
    live_epoch: u64,
) -> bool {
    if source == live {
        return true;
    }
    // Recovery only appends a live epoch patch. Permit exactly that difference,
    // only at the end, and only when both values certify their enclosing epoch.
    if live_epoch <= source_epoch {
        return false;
    }
    let (Some(source), Some(live)) = (source, live) else {
        return false;
    };
    if source.pointer("/session/epoch").and_then(Value::as_u64) != Some(source_epoch)
        || live.pointer("/session/epoch").and_then(Value::as_u64) != Some(live_epoch)
    {
        return false;
    }
    let mut source = source.as_ref().clone();
    let Some(epoch) = source.pointer_mut("/session/epoch") else {
        return false;
    };
    *epoch = Value::from(live_epoch);
    source == **live
}

impl ContextSnapshot {
    /// Compaction markers alone are insufficient: repeated same-summary compaction
    /// advances generation and retires invocation provenance independently.
    pub fn same_undo_baseline(&self, other: &Self) -> bool {
        self.compaction == other.compaction
            && self.baseline_generation == other.baseline_generation
            && self.retired_call_ids == other.retired_call_ids
    }

    /// Correlate a live raw index with this immutable source using the complete
    /// ordered semantic conversation, never equal input text or a reset Turn ID.
    pub fn undo_input_index(
        &self,
        live: &Self,
        live_index: usize,
        input: &ContentItem,
    ) -> Result<usize, Error> {
        if !self.same_undo_baseline(live) {
            return Err(Error::config(
                "undo live Context has a different compaction baseline",
            ));
        }
        if !matches!(input, ContentItem::User { source: None, .. })
            || live.items.get(live_index) != Some(input)
        {
            return Err(Error::config("undo input is not in live Context"));
        }
        let source = project(&self.items)?;
        let current = project(&live.items)?;
        if !equal_anchors(&source.anchors, &current.anchors)
            || !recovery_terminal_equal(
                source
                    .trailing
                    .last()
                    .expect("projection retains terminal State"),
                current
                    .trailing
                    .last()
                    .expect("projection retains terminal State"),
                self.epoch,
                live.epoch,
            )
        {
            return Err(Error::config(
                "undo live Context does not match selected history",
            ));
        }
        let ordinal = current
            .anchors
            .iter()
            .position(|anchor| anchor.index == live_index)
            .ok_or_else(|| Error::config("undo input has no conversation anchor"))?;
        if self.compaction.is_some()
            && !source.anchors[..ordinal]
                .iter()
                .any(|anchor| matches!(anchor.item, ContentItem::User { .. }))
        {
            return Err(Error::config("compaction summary is not an undo input"));
        }
        Ok(source.anchors[ordinal].index)
    }

    /// Prove this settled candidate preserves every semantic item before the input.
    /// State representation may differ, but every non-State anchor must agree.
    /// Candidate terminal State must occur in the source's trailing State run;
    /// subsequent admission-only State frames retain the original prefix allowance.
    pub fn undo_boundary_matches(&self, source: &Self, index: usize) -> Result<bool, Error> {
        if !self.same_undo_baseline(source) {
            return Ok(false);
        }
        let input = source
            .items
            .get(index)
            .ok_or_else(|| Error::config("undo input index is outside selected history"))?;
        if !matches!(input, ContentItem::User { source: None, .. }) {
            return Err(Error::config("undo target is not a real user input"));
        }
        let before = project(&source.items[..index])?;
        let target_epoch = before
            .trailing
            .last()
            .and_then(Option::as_ref)
            .and_then(|state| state.pointer("/session/epoch"))
            .and_then(Value::as_u64)
            .unwrap_or(source.epoch);
        // A new epoch's first input must restore that epoch's certified initial
        // Recover, including inherited history and route authority.
        if self.epoch != target_epoch {
            return Ok(false);
        }
        if source.compaction.is_some()
            && !before
                .anchors
                .iter()
                .any(|anchor| matches!(anchor.item, ContentItem::User { .. }))
        {
            return Err(Error::config("compaction summary is not an undo input"));
        }
        // Keep the original exact-prefix path. Projection of `before` above
        // still validates replay metadata before an exact match is accepted.
        if source.items[..index].starts_with(&self.items)
            && source.items[self.items.len()..index]
                .iter()
                .all(|item| matches!(item, ContentItem::State(_)))
        {
            return Ok(true);
        }
        let candidate = project(&self.items)?;
        if !equal_anchors(&candidate.anchors, &before.anchors) {
            return Ok(false);
        }
        let terminal = candidate
            .trailing
            .last()
            .expect("projection retains terminal State");
        Ok(before.trailing.contains(terminal))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn user(text: &str) -> ContentItem {
        ContentItem::User {
            parts: vec![InputPart::Text { text: text.into() }],
            source: None,
        }
    }

    fn snapshot(items: Vec<ContentItem>) -> ContextSnapshot {
        let mut snapshot = Context::new("system".into(), vec![], 65536)
            .unwrap()
            .snapshot()
            .unwrap();
        snapshot.items = items;
        snapshot
    }

    fn state(value: u64) -> ContentItem {
        ContentItem::State(StateFrame::StateSnapshot {
            state: json!({"session":{"epoch":0},"value":value}),
        })
    }

    #[test]
    fn projected_state_matches_representation_but_retains_semantic_anchors() {
        let candidate = snapshot(vec![
            state(1),
            user("prior"),
            ContentItem::State(StateFrame::patch(
                serde_json::from_value(json!({"value":2})).unwrap(),
                vec![],
                None,
            )),
        ]);
        let source = snapshot(vec![state(1), user("prior"), state(2), user("target")]);
        assert!(candidate.undo_boundary_matches(&source, 3).unwrap());
        let mut wrong = source.clone();
        wrong.items[0] = state(99);
        assert!(!candidate.undo_boundary_matches(&wrong, 3).unwrap());
        let mut wrong_terminal = candidate.clone();
        wrong_terminal.items[2] = state(99);
        assert!(!wrong_terminal.undo_boundary_matches(&source, 3).unwrap());
        // Existing trailing State-only allowance: admission may add State after
        // the exact settled boundary without weakening any preceding anchor.
        let shorter = snapshot(vec![state(1), user("prior")]);
        assert!(shorter.undo_boundary_matches(&source, 3).unwrap());
    }

    #[test]
    fn live_mapping_preserves_image_parts_and_framework_origin() {
        let mut image_user = user("same");
        if let ContentItem::User { parts, .. } = &mut image_user {
            parts.push(InputPart::Image {
                source: microcrab_protocol::ImageSource::Data {
                    media_type: "image/png".into(),
                    data: vec![1, 2, 3],
                },
            });
        }
        let source = snapshot(vec![state(1), image_user, user("same")]);
        let mut live = source.clone();
        assert_eq!(
            source.undo_input_index(&live, 2, &live.items[2]).unwrap(),
            2
        );
        if let ContentItem::User { parts, .. } = &mut live.items[1] {
            if let InputPart::Image {
                source: microcrab_protocol::ImageSource::Data { data, .. },
            } = &mut parts[1]
            {
                *data = vec![4, 5, 6];
            }
        }
        assert!(source.undo_input_index(&live, 2, &live.items[2]).is_err());
        live = source.clone();
        if let ContentItem::User { source, .. } = &mut live.items[1] {
            *source = Some(microcrab_transport::UserSource::Framework);
        }
        assert!(source.undo_input_index(&live, 2, &live.items[2]).is_err());
    }

    #[test]
    fn only_terminal_recovery_epoch_difference_is_allowed() {
        let mut source = snapshot(vec![state(1), user("target")]);
        source.committed_state = Some(json!({"session":{"epoch":0},"value":1}));
        let mut live = source.clone();
        live.epoch = 1;
        live.items.push(ContentItem::State(StateFrame::patch(
            serde_json::from_value(json!({"session":{"epoch":1}})).unwrap(),
            vec![],
            None,
        )));
        assert_eq!(
            source.undo_input_index(&live, 1, &live.items[1]).unwrap(),
            1
        );
        live.items.push(ContentItem::State(StateFrame::patch(
            serde_json::from_value(json!({"value":2})).unwrap(),
            vec![],
            None,
        )));
        assert!(source.undo_input_index(&live, 1, &live.items[1]).is_err());
        let mut initial = snapshot(vec![]);
        initial.epoch = 1;
        assert!(!initial.undo_boundary_matches(&source, 1).unwrap());
    }

    #[test]
    fn first_input_new_epoch_cannot_select_previous_epoch_or_compaction_summary() {
        let prior = snapshot(vec![state(1), user("inherited")]);
        let mut source = prior.clone();
        source.epoch = 1;
        source.items.push(ContentItem::State(StateFrame::patch(
            serde_json::from_value(json!({"session":{"epoch":1}})).unwrap(),
            vec![],
            None,
        )));
        source.items.push(user("target"));
        assert!(!prior.undo_boundary_matches(&source, 3).unwrap());
        let mut initial = prior.clone();
        initial.epoch = 1;
        assert!(initial.undo_boundary_matches(&source, 3).unwrap());
        let mut compacted = Context::new("system".into(), vec![], 65536).unwrap();
        compacted
            .compact_baseline("summary".into(), &json!({}))
            .unwrap();
        let compacted = compacted.snapshot().unwrap();
        assert!(
            compacted
                .undo_input_index(&compacted, 1, &compacted.items[1])
                .unwrap_err()
                .to_string()
                .contains("summary")
        );
    }
}
