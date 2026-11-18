//! Versioned storage projection; live handles and pending tool calls are never serialized.
use super::*;
use serde::{Deserialize, Serialize};

/// Explicit provenance for the retained summary baseline; never a model message.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactionMarker {
    pub id: String,
    pub epoch: u64,
    pub summary: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextSnapshot {
    pub epoch: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub baseline_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_evidence: Option<ContextUsageEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_compaction_attempt: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub auto_compaction_retry: bool,
    #[serde(default)]
    pub pending_epoch: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction: Option<CompactionMarker>,
    pub system: String,
    pub items: Vec<ContentItem>,
    pub tools: Vec<ToolDefinition>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub historical_tools: std::collections::BTreeMap<u64, Vec<ToolDefinition>>,
    pub committed_state: Option<Value>,
    pub used_call_ids: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub retired_call_ids: BTreeSet<String>,
    pub max_bytes: usize,
}
fn is_false(value: &bool) -> bool {
    !*value
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

impl ContextSnapshot {
    pub fn validate(&self) -> Result<(), Error> {
        Context::from_snapshot(self.clone()).map(|_| ())
    }
}
impl Context {
    pub fn snapshot(&self) -> Result<ContextSnapshot, Error> {
        self.require_settled()?;
        Ok(ContextSnapshot {
            epoch: self.epoch,
            baseline_generation: self.baseline_generation,
            usage_evidence: self.usage_evidence.clone(),
            auto_compaction_attempt: self.auto_compaction_attempt.clone(),
            auto_compaction_retry: self.auto_compaction_retry,
            pending_epoch: self.pending_epoch,
            compaction: self.compaction.clone(),
            system: self.system.clone(),
            items: self.items.clone(),
            tools: self.tools.clone(),
            historical_tools: self.historical_tools.clone(),
            committed_state: self.committed_state.clone(),
            used_call_ids: self.used_call_ids.iter().cloned().collect(),
            retired_call_ids: self.retired_call_ids.clone(),
            max_bytes: self.max_bytes,
        })
    }
    pub fn from_snapshot(snapshot: ContextSnapshot) -> Result<Self, Error> {
        let mut context =
            Self::with_frozen_tools(snapshot.system, snapshot.tools, snapshot.max_bytes)?;
        context.epoch = snapshot.epoch;
        context.baseline_generation = snapshot.baseline_generation;
        if snapshot
            .auto_compaction_attempt
            .as_ref()
            .is_some_and(|value| value.len() > 4096 || value.contains('\0'))
        {
            return Err(Error::protocol(
                "invalid automatic compaction attempt witness",
            ));
        }
        if snapshot.auto_compaction_retry && snapshot.auto_compaction_attempt.is_none() {
            return Err(Error::protocol(
                "compaction retry has no durable attempt witness",
            ));
        }
        context.auto_compaction_retry = snapshot.auto_compaction_retry;
        context.auto_compaction_attempt = snapshot.auto_compaction_attempt;
        context.pending_epoch = snapshot.pending_epoch;
        for (epoch, tools) in &snapshot.historical_tools {
            if *epoch > snapshot.epoch {
                return Err(Error::protocol(
                    "historical tool epoch exceeds Context epoch",
                ));
            }
            Self::with_frozen_tools(context.system.clone(), tools.clone(), context.max_bytes)?;
        }
        context.historical_tools = snapshot.historical_tools;
        crate::budget::check(
            crate::LimitKind::ContextBytes,
            serde_json::to_vec(&context.historical_tools)?.len(),
            context.max_bytes,
        )
        .map_err(|error| match error {
            Error::Limit(limit) => Error::Limit(limit.with_label("historical tool catalog budget")),
            other => other,
        })?;
        if !snapshot.retired_call_ids.is_empty() && snapshot.baseline_generation == 0 {
            return Err(Error::protocol(
                "retired call identities require a Context baseline generation",
            ));
        }
        context.retired_call_ids = snapshot.retired_call_ids.clone();
        context
            .used_call_ids
            .extend(snapshot.retired_call_ids.iter().cloned());
        let mut projected: Option<Value> = None;
        for item in &snapshot.items {
            match item {
                ContentItem::State(frame) => {
                    context.require_settled()?;
                    match frame {
                        StateFrame::StateSnapshot { state } => {
                            let advances_epoch =
                                projected.as_ref().is_some_and(|previous| {
                                    match (
                                        previous.pointer("/session/epoch").and_then(Value::as_u64),
                                        state.pointer("/session/epoch").and_then(Value::as_u64),
                                    ) {
                                        (Some(before), Some(after)) => {
                                            after > before && after <= snapshot.epoch
                                        }
                                        _ => false,
                                    }
                                });
                            if (projected.is_some() && !advances_epoch) || !state.is_object() {
                                return Err(Error::protocol("invalid restored state snapshot"));
                            }
                            projected = Some(state.clone());
                        }
                        StateFrame::StatePatch { .. } => {
                            let state = projected
                                .as_mut()
                                .ok_or_else(|| Error::protocol("state patch precedes snapshot"))?;
                            microcrab_transport::apply_state_patch(state, frame)
                                .map_err(Error::protocol)?;
                        }
                    }
                }
                ContentItem::User { parts, .. } => {
                    context.require_settled()?;
                    crate::admission::input::validate_historical(
                        &SessionInput {
                            parts: parts.clone(),
                            variables: Default::default(),
                        },
                        context.max_bytes,
                    )?;
                }
                ContentItem::Assistant {
                    content,
                    tool_calls,
                    provider_metadata,
                } => {
                    context.require_settled()?;
                    let response = Response {
                        provider_metadata: provider_metadata.clone(),
                        content: content.clone(),
                        tool_calls: tool_calls.clone(),
                        usage: Default::default(),
                    };
                    let item_epoch = projected
                        .as_ref()
                        .and_then(|state| state.pointer("/session/epoch"))
                        .and_then(Value::as_u64)
                        .unwrap_or(snapshot.epoch);
                    let tools = context
                        .historical_tools
                        .get(&item_epoch)
                        .unwrap_or(&context.tools);
                    validate_response(&response, tools)?;
                    for call in tool_calls {
                        if !context.used_call_ids.insert(call.id.clone()) {
                            return Err(Error::protocol("reused restored tool call id"));
                        }
                        context.pending_calls.insert(call.id.clone());
                    }
                }
                ContentItem::Tool { call_id, parts, .. } => {
                    crate::admission::image::validate_tool_parts(parts)?;
                    if !context.pending_calls.remove(call_id) {
                        return Err(Error::protocol("orphan restored tool result"));
                    }
                }
            }
        }
        context.require_settled()?;
        if context
            .used_call_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
            != snapshot.used_call_ids
            || projected != snapshot.committed_state
        {
            return Err(Error::protocol("restored Context baseline mismatch"));
        }
        context.check_budget(&snapshot.items)?;
        if let Some(marker) = &snapshot.compaction {
            use sha2::{Digest, Sha256};
            let summary_id = format!("{:x}", Sha256::digest(marker.summary.as_bytes()));
            let baseline = snapshot.items.iter().find_map(|item| match item {
                ContentItem::User { parts, .. } => Some(parts),
                _ => None,
            });
            if marker.epoch > snapshot.epoch
                || marker.id != format!("compact-{}-{summary_id}", marker.epoch)
                || baseline
                    != Some(&vec![InputPart::Text {
                        text: marker.summary.clone(),
                    }])
            {
                return Err(Error::protocol("restored compaction provenance mismatch"));
            }
        }
        context.compaction = snapshot.compaction;
        context.items = snapshot.items;
        context.committed_state = snapshot.committed_state;
        context.usage_evidence = snapshot.usage_evidence;
        context.restore_usage_evidence()?;
        Ok(context)
    }
}
