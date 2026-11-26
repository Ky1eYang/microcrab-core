//! Session-owned universal content. Snapshot and patch are judged here, not rendered.
mod measurement;
pub use measurement::{ContextBytes, ContextCounts, ContextMeasurement};
mod pressure;
pub use pressure::{AutoCompactionPolicy, ContextUsageEvidence, TokenEstimate, TokenEstimator};
mod semantic;
mod snapshot;
pub use snapshot::{CompactionMarker, ContextSnapshot};
pub mod system;

use crate::error::Error;

use microcrab_protocol::{InputPart, Response, SessionInput, ToolDefinition};
use microcrab_transport::{CarrierEnvelopeVersion, ImportantInput, StateFrame, UserSource};
use serde_json::{Map, Value};
use std::collections::{BTreeSet, HashSet};

pub use microcrab_transport::{Content, ContentItem};

pub(crate) fn latest_direct_user(items: &[ContentItem]) -> Option<&ContentItem> {
    items
        .iter()
        .rev()
        .find(|item| matches!(item, ContentItem::User { source: None, .. }))
}

/// Validate and canonicalize Context's state snapshot; text rendering belongs to
/// transport. Reject nonobjects before constructing an owned frame.
fn state_snapshot(state: &Value) -> Result<StateFrame, Error> {
    if !state.is_object() {
        return Err(Error::config("state must be an object"));
    }
    let mut state = state.clone();
    state.sort_all_objects();
    Ok(StateFrame::StateSnapshot { state })
}

#[derive(Clone, Debug)]
pub struct Context {
    epoch: u64,
    baseline_generation: u64,
    usage_evidence: Option<ContextUsageEvidence>,
    auto_compaction_attempt: Option<String>,
    auto_compaction_retry: bool,
    pending_epoch: bool,
    compaction: Option<CompactionMarker>,
    system: String,
    items: Vec<ContentItem>,
    tools: Vec<ToolDefinition>,
    historical_tools: std::collections::BTreeMap<u64, Vec<ToolDefinition>>,
    committed_state: Option<Value>,
    pending_calls: BTreeSet<String>,
    used_call_ids: HashSet<String>,
    retired_call_ids: BTreeSet<String>,
    max_bytes: usize,
}

impl Context {
    /// Adopt an already-admitted functional request without reinterpreting its
    /// inherited historical schemas. One-shot Context never becomes a recoverable
    /// conversation or dispatches a copied tool invocation.
    pub(crate) fn from_functional_content(request: &Content) -> Result<Self, Error> {
        microcrab_transport::render_ast(request)?;
        let mut context = Self::with_frozen_tools(
            request.system.clone(),
            request.tools.clone(),
            request.max_bytes,
        )?;
        context.epoch = request.epoch;
        context.tools = request.tools.clone();
        for item in &request.items {
            match item {
                ContentItem::State(StateFrame::StateSnapshot { state }) => {
                    context.committed_state = Some(state.clone())
                }
                ContentItem::State(frame @ StateFrame::StatePatch { .. }) => {
                    let state = context.committed_state.as_mut().ok_or_else(|| {
                        Error::protocol("functional state patch precedes snapshot")
                    })?;
                    microcrab_transport::apply_state_patch(state, frame)
                        .map_err(Error::protocol)?;
                }
                ContentItem::Assistant { tool_calls, .. } => {
                    for call in tool_calls {
                        if !context.used_call_ids.insert(call.id.clone()) {
                            return Err(Error::protocol("reused functional tool call identity"));
                        }
                        context.pending_calls.insert(call.id.clone());
                    }
                }
                ContentItem::Tool { call_id, .. } if !context.pending_calls.remove(call_id) => {
                    return Err(Error::protocol("orphan functional tool result"));
                }
                _ => {}
            }
        }
        context.require_settled()?;
        context.items = request.items.clone();
        context.check_budget(&context.items)?;
        Ok(context)
    }
    /// Observation may inspect an admitted assistant while its tool results are
    /// pending. This does not authorize sending an unsettled model request.
    pub(crate) fn admitted_items(&self) -> (u64, &[ContentItem]) {
        (self.epoch, &self.items)
    }

    /// Project only the latest genuine direct input for a clean permission review.
    /// This also works while an admitted assistant still has pending tool calls.
    pub(crate) fn review_issuer(&self, max_output_tokens: u32) -> Content {
        // A retained compaction summary renders as a user message, but is not intent.
        let items = if self.compaction.is_some() {
            self.items
                .iter()
                .position(|item| matches!(item, ContentItem::User { .. }))
                .map_or(&[][..], |index| &self.items[index + 1..])
        } else {
            &self.items[..]
        };
        Content {
            epoch: self.epoch,
            subagent: None,
            system: "Permission review".into(),
            items: latest_direct_user(items).cloned().into_iter().collect(),
            tools: vec![],
            max_output_tokens,
            max_bytes: self.max_bytes,
            turn_id: None,
            round: None,
        }
    }

    pub(crate) fn initialization_review_issuer(
        &self,
        max_output_tokens: u32,
    ) -> Result<Content, Error> {
        self.validate_content(max_output_tokens)?;
        Ok(self.review_issuer(max_output_tokens))
    }

    pub fn new(
        system: String,
        mut tools: Vec<ToolDefinition>,
        max_bytes: usize,
    ) -> Result<Self, Error> {
        // Storage canonicalizes object keys. Freeze the same order before the
        // first model request so checkpoint restore cannot change cached bytes.
        for tool in &mut tools {
            tool.parameters.sort_all_objects();
        }
        Self::with_frozen_tools(system, tools, max_bytes)
    }

    // Restored epochs retain their exact historical schema representation.
    fn with_frozen_tools(
        system: String,
        mut tools: Vec<ToolDefinition>,
        max_bytes: usize,
    ) -> Result<Self, Error> {
        if system.trim().is_empty() || max_bytes == 0 {
            return Err(Error::config("system and context budget must be nonempty"));
        }
        tools.sort_by(|a, b| a.name.cmp(&b.name));
        let mut names = HashSet::new();
        for tool in &tools {
            if tool.name.trim().is_empty()
                || !names.insert(tool.name.clone())
                || !tool.parameters.is_object()
            {
                return Err(Error::config("invalid or duplicate tool definition"));
            }
        }
        let context = Self {
            epoch: 0,
            baseline_generation: 0,
            usage_evidence: None,
            auto_compaction_attempt: None,
            auto_compaction_retry: false,
            pending_epoch: false,
            compaction: None,
            system,
            items: Vec::new(),
            tools,
            historical_tools: Default::default(),
            committed_state: None,
            pending_calls: BTreeSet::new(),
            used_call_ids: HashSet::new(),
            retired_call_ids: BTreeSet::new(),
            max_bytes,
        };
        context.check_budget(&context.items)?;
        Ok(context)
    }

    pub(crate) fn frozen_tools(&self) -> &[ToolDefinition] {
        &self.tools
    }

    /// Current model-facing byte budget. Callers compare measurements against it
    /// before deciding whether the embedding owner must confirm another block.
    pub(crate) fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    /// Explicitly raise the model-facing byte budget after an embedding grant.
    /// The caller updates the State witness in the same transition, so a reload
    /// sees one agreed budget and no frozen Context bytes are rewritten.
    pub(crate) fn grant_bytes(&mut self, max_bytes: usize) -> Result<(), Error> {
        if max_bytes < self.max_bytes {
            return Err(Error::config(
                "context budget cannot shrink without explicit compaction",
            ));
        }
        self.max_bytes = max_bytes;
        Ok(())
    }

    /// Build a new protocol epoch from settled neutral history. Only the live
    /// candidate changes; immutable Journal nodes retain their original evidence.
    pub(crate) fn prepare_protocol_epoch(&mut self) -> Result<(), Error> {
        self.require_settled()?;
        let mut next = self.clone();
        next.pending_epoch = true;
        next.next_model_epoch()?;
        next.usage_evidence = None;
        next.auto_compaction_attempt = None;
        next.auto_compaction_retry = false;
        next.clear_transport_replay_hints()?;
        next.check_budget(&next.items)?;
        *self = next;
        Ok(())
    }

    /// Refresh only a live recovery candidate after an unwitnessed or changed
    /// route. This preserves settled semantic history, not its prior cache prefix.
    /// The creator must acknowledge the original Journal selection separately.
    /// No callbacks, model requests, or admitted tool effects run here.
    pub fn refresh_recovery_epoch(&mut self) -> Result<(), Error> {
        self.require_settled()?;
        let mut next = self.clone();
        next.prepare_protocol_epoch()?;
        next.materialize_epoch()?;
        let original = std::mem::take(&mut next.items);
        next.committed_state = None;
        let mut projected = None;
        for item in original {
            if let ContentItem::State(frame) = item {
                match &frame {
                    StateFrame::StateSnapshot { state } => projected = Some(state.clone()),
                    StateFrame::StatePatch { .. } => {
                        let state = projected.as_mut().ok_or_else(|| {
                            Error::protocol("state patch precedes recovery snapshot")
                        })?;
                        microcrab_transport::apply_state_patch(state, &frame)
                            .map_err(Error::protocol)?;
                    }
                }
                let state = projected
                    .as_ref()
                    .expect("State frame establishes projection");
                if let Some(frame) = next.state_frame(state)? {
                    next.items.push(ContentItem::State(frame));
                }
                next.committed_state = Some(state.clone());
            } else {
                if matches!(
                    item,
                    ContentItem::User {
                        envelope: Some(CarrierEnvelopeVersion::Important { .. }),
                        ..
                    }
                ) && !matches!(next.items.last(), Some(ContentItem::State(_)))
                {
                    // Recovery may have coalesced an unchanged admission frame.
                    // This envelope still owns a frame before its notice.
                    next.items.push(ContentItem::State(StateFrame::patch(
                        Map::new(),
                        Vec::new(),
                        None,
                    )));
                }
                next.items.push(item);
            }
        }
        // A prepared retry retains its Turn/round and semantic State. Only its
        // live epoch witness advances; the next ordinary State remains a patch.
        if let Some(mut state) = next.committed_state.clone()
            && let Some(session) = state.get_mut("session").and_then(Value::as_object_mut)
        {
            session.insert("epoch".into(), Value::from(next.epoch));
            next.synchronize_state(&state)?;
        }
        next.check_budget(&next.items)?;
        *self = next;
        Ok(())
    }

    fn clear_transport_replay_hints(&mut self) -> Result<(), Error> {
        for item in &mut self.items {
            semantic::clear_transport_item(item)?;
        }
        Ok(())
    }

    /// An explicit user environment update freezes a new catalog for the next
    /// model epoch. Prior definitions remain available to validate old calls on
    /// restart; the existing conversation and its State positions stay intact.
    pub(crate) fn replace_tools_for_environment(
        &mut self,
        tools: Vec<ToolDefinition>,
    ) -> Result<(), Error> {
        self.require_settled()?;
        let candidate = Self::new(self.system.clone(), tools, self.max_bytes)?;
        self.replace_frozen_catalog(candidate.tools)
    }

    /// Recovery has already checked installed implementations. Preserve external
    /// schema bytes while archiving the previous epoch's catalog unchanged.
    pub(crate) fn replace_tools_for_recovery(
        &mut self,
        tools: Vec<ToolDefinition>,
    ) -> Result<(), Error> {
        self.require_settled()?;
        let candidate = Self::with_frozen_tools(self.system.clone(), tools, self.max_bytes)?;
        self.replace_frozen_catalog(candidate.tools)
    }

    fn replace_frozen_catalog(&mut self, tools: Vec<ToolDefinition>) -> Result<(), Error> {
        if tools == self.tools {
            return Ok(());
        }
        let mut next = self.clone();
        // Multiple edits before the next model call retain the original epoch
        // catalog, not the last unconsumed candidate.
        next.historical_tools
            .entry(self.epoch)
            .or_insert_with(|| self.tools.clone());
        next.tools = tools;
        next.usage_evidence = None;
        crate::budget::check(
            crate::LimitKind::ContextBytes,
            serde_json::to_vec(&next.historical_tools)?.len(),
            self.max_bytes,
        )
        .map_err(|error| match error {
            Error::Limit(limit) => Error::Limit(
                limit.with_label("historical tool catalog budget; compact before changing tools"),
            ),
            other => other,
        })?;
        next.pending_epoch = true;
        next.next_model_epoch()?;
        next.check_budget(&next.items)?;
        *self = next;
        Ok(())
    }

    pub fn admit_user(
        &mut self,
        input: impl Into<SessionInput>,
        state: &Value,
    ) -> Result<(), Error> {
        self.admit_user_with_important(input.into(), state, None)
    }

    pub(crate) fn admit_user_with_important(
        &mut self,
        input: SessionInput,
        state: &Value,
        important: Option<ImportantInput>,
    ) -> Result<(), Error> {
        let envelope = important.map_or(CarrierEnvelopeVersion::V1, |notice| {
            CarrierEnvelopeVersion::Important { notice }
        });
        self.admit_input_run([(input, None, envelope)], state)
    }

    pub(crate) fn admit_framework(
        &mut self,
        input: SessionInput,
        state: &Value,
    ) -> Result<(), Error> {
        self.admit_input_run(
            [(
                input,
                Some(UserSource::Framework),
                CarrierEnvelopeVersion::V1,
            )],
            state,
        )
    }

    /// Admit an authenticated Session-owned carrier run atomically. The caller
    /// supplies trusted origin metadata after authentication, never from body text.
    /// Direct user input uses admit_user and cannot absorb this non-user run.
    pub(crate) fn admit_carriers(
        &mut self,
        carriers: Vec<(SessionInput, UserSource, CarrierEnvelopeVersion)>,
        state: &Value,
    ) -> Result<(), Error> {
        self.admit_input_run(
            carriers
                .into_iter()
                .map(|(input, source, envelope)| (input, Some(source), envelope)),
            state,
        )
    }

    fn admit_input_run(
        &mut self,
        inputs: impl IntoIterator<Item = (SessionInput, Option<UserSource>, CarrierEnvelopeVersion)>,
        state: &Value,
    ) -> Result<(), Error> {
        self.require_settled()?;
        let mut inputs = inputs.into_iter().peekable();
        if inputs.peek().is_none() {
            return Err(Error::config("carrier admission must not be empty"));
        }
        let mut items = self.items.clone();
        // A real admission always owns one frame. Equal State produces an empty
        // patch without inventing a clock update or rewriting retained history.
        let frame = self
            .state_frame(state)?
            .unwrap_or_else(|| StateFrame::patch(Map::new(), Vec::new(), None));
        items.push(ContentItem::State(frame));
        for (input, source, envelope) in inputs {
            crate::admission::input::validate_resolved(&input, self.max_bytes)?;
            items.push(ContentItem::User {
                parts: input.parts,
                source,
                envelope: Some(envelope),
            });
        }
        let mut candidate = self.clone();
        candidate.items = items;
        candidate.committed_state = Some(state.clone());
        // Validate disclosed addresses and escaped wire size before publication.
        microcrab_transport::render_ast(&candidate.content(1)?)?;
        *self = candidate;
        Ok(())
    }
    /// Commit only changed state keys before a model round, retaining the epoch baseline.
    pub fn synchronize_state(&mut self, state: &Value) -> Result<(), Error> {
        self.require_settled()?;
        if let Some(frame) = self.state_frame(state)? {
            let mut items = self.items.clone();
            items.push(ContentItem::State(frame));
            self.check_budget(&items)?;
            self.items = items;
        }
        self.committed_state = Some(state.clone());
        Ok(())
    }

    /// The admission reason must be independent of time/round refreshes.
    pub(crate) fn state_key_changed(&self, key: &str, value: &Value) -> bool {
        self.committed_state
            .as_ref()
            .and_then(|state| state.get(key))
            != Some(value)
    }

    /// Compare only model-relevant execution facts. Clocks, counters, delivery
    /// witnesses and retained bodies cannot create an unattended request.
    pub(crate) fn execution_metadata_changed(
        &self,
        ids: &[crate::runtime::execution::ExecutionId],
        state: &Value,
    ) -> bool {
        let projection = |value: Option<&Value>, id: u64| {
            value
                .and_then(|value| value.get("executions"))
                .and_then(Value::as_array)
                .and_then(|rows| {
                    rows.iter()
                        .find(|row| row.get("execution_id").and_then(Value::as_u64) == Some(id))
                })
                .map(|row| {
                    [
                        "status",
                        "failure",
                        "diagnostic",
                        "output",
                        "output_availability",
                    ]
                    .iter()
                    .map(|key| {
                        (
                            (*key).to_owned(),
                            row.get(*key).cloned().unwrap_or(Value::Null),
                        )
                    })
                    .collect::<Map<_, _>>()
                })
        };
        ids.iter().any(|id| {
            projection(self.committed_state.as_ref(), id.0) != projection(Some(state), id.0)
        })
    }

    pub fn append_response(&mut self, response: &Response) -> Result<(), Error> {
        self.require_settled()?;
        validate_response(response, &self.tools)?;
        if response
            .tool_calls
            .iter()
            .any(|call| self.used_call_ids.contains(&call.id))
        {
            return Err(Error::protocol("tool call id was already used"));
        }
        let mut tool_calls = response.tool_calls.clone();
        for call in &mut tool_calls {
            call.arguments.sort_all_objects();
        }
        let mut items = self.items.clone();
        items.push(ContentItem::Assistant {
            content: response.content.clone(),
            provider_metadata: response.provider_metadata.clone(),
            tool_calls,
        });
        self.check_budget(&items)?;
        self.items = items;
        // The latest accepted response replaces prior measurement authority.
        self.usage_evidence = None;
        for call in &response.tool_calls {
            self.pending_calls.insert(call.id.clone());
            self.used_call_ids.insert(call.id.clone());
        }
        Ok(())
    }

    pub fn append_tool_result(&mut self, call_id: String, content: String) -> Result<(), Error> {
        self.append_tool_result_with_parts(call_id, content, Vec::new())
    }

    /// Preserve typed self-contained tool media in the frozen model-facing history.
    pub fn append_tool_result_with_parts(
        &mut self,
        call_id: String,
        content: String,
        parts: Vec<InputPart>,
    ) -> Result<(), Error> {
        crate::admission::image::validate_tool_parts(&parts)?;
        if !self.pending_calls.contains(&call_id) {
            return Err(Error::protocol("tool result has no pending call"));
        }
        let mut items = self.items.clone();
        items.push(ContentItem::Tool {
            parts,
            call_id: call_id.clone(),
            content,
        });
        self.check_budget(&items)?;
        self.items = items;
        self.pending_calls.remove(&call_id);
        Ok(())
    }

    fn validate_content(&self, max_output_tokens: u32) -> Result<(), Error> {
        self.require_settled()?;
        if max_output_tokens == 0 {
            return Err(Error::config("max output tokens must be positive"));
        }
        self.check_budget(&self.items)
    }

    /// Assemble system, judged state, user text, and tools for the LLM client.
    pub fn content(&self, max_output_tokens: u32) -> Result<Content, Error> {
        self.validate_content(max_output_tokens)?;
        Ok(Content {
            epoch: self.epoch,
            subagent: None,
            system: self.system.clone(),
            items: self.items.clone(),
            tools: self.tools.clone(),
            max_output_tokens,
            max_bytes: self.max_bytes,
            turn_id: None,
            round: None,
        })
    }

    pub fn compact(&mut self, summary: String, state: &Value) -> Result<(), Error> {
        self.require_settled()?;
        if summary.trim().is_empty() || !state.is_object() {
            return Err(Error::config("compaction needs a summary and object state"));
        }
        use sha2::{Digest, Sha256};
        let summary_id = format!("{:x}", Sha256::digest(summary.as_bytes()));
        let items = vec![
            ContentItem::State(state_snapshot(state)?),
            ContentItem::User {
                envelope: None,
                source: None,
                parts: vec![InputPart::Text {
                    text: summary.clone(),
                }],
            },
        ];
        self.check_budget(&items)?;
        if self
            .compaction
            .as_ref()
            .is_none_or(|marker| marker.summary != summary)
        {
            self.compaction = Some(CompactionMarker {
                id: format!("compact-{}-{summary_id}", self.epoch),
                epoch: self.epoch,
                summary,
            });
        }
        self.pending_epoch |= self.items != items || self.committed_state.as_ref() != Some(state);
        self.usage_evidence = None;
        self.items = items;
        self.committed_state = Some(state.clone());
        self.auto_compaction_attempt = None;
        self.auto_compaction_retry = false;
        self.used_call_ids.clear();
        self.retired_call_ids.clear();
        self.historical_tools.clear();
        Ok(())
    }

    /// Replace settled model history without changing the execution epoch or Turn.
    /// The caller acknowledges Journal durability before publishing this candidate.
    pub fn compact_baseline(&mut self, summary: String, state: &Value) -> Result<(), Error> {
        if self.pending_epoch {
            return Err(Error::config("cannot replace a pending epoch baseline"));
        }
        let generation = self.baseline_generation.checked_add(1).ok_or_else(|| {
            crate::budget::reject(
                crate::LimitKind::Arithmetic,
                "context baseline generation overflow",
            )
        })?;
        let mut candidate = self.clone();
        candidate.compact(summary, state)?;
        candidate.retired_call_ids = self.used_call_ids.iter().cloned().collect();
        candidate.used_call_ids = self.used_call_ids.clone();
        candidate.pending_epoch = false;
        candidate.baseline_generation = generation;
        *self = candidate;
        Ok(())
    }

    pub(crate) fn clear_auto_compaction_attempt(&mut self) {
        self.auto_compaction_retry = false;
        self.auto_compaction_attempt = None;
    }

    pub(crate) fn auto_compaction_attempt(&self) -> Option<&str> {
        self.auto_compaction_attempt.as_deref()
    }

    pub(crate) fn mark_auto_compaction_attempt(&mut self, witness: String) {
        self.auto_compaction_retry = true;
        self.auto_compaction_attempt = Some(witness);
    }

    pub(crate) fn auto_compaction_retry_available(&self) -> bool {
        self.auto_compaction_retry
    }

    pub(crate) fn acknowledge_auto_compaction(&mut self) {
        self.auto_compaction_retry = false;
    }

    pub fn baseline_generation(&self) -> u64 {
        self.baseline_generation
    }

    pub fn compaction(&self) -> Option<&CompactionMarker> {
        self.compaction.as_ref()
    }

    pub fn items(&self) -> &[ContentItem] {
        &self.items
    }

    pub fn system(&self) -> &str {
        &self.system
    }

    pub(crate) fn pending_call_ids(&self) -> Vec<String> {
        self.pending_calls.iter().cloned().collect()
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Whether an explicit baseline change awaits its first model admission.
    pub fn pending_epoch(&self) -> bool {
        self.pending_epoch
    }

    pub(crate) fn next_model_epoch(&self) -> Result<u64, Error> {
        if self.pending_epoch {
            self.epoch.checked_add(1).ok_or_else(|| {
                crate::budget::reject(crate::LimitKind::Arithmetic, "context epoch overflow")
            })
        } else {
            Ok(self.epoch)
        }
    }

    /// Called after input preflight, before allocating the new epoch's first turn.
    pub(crate) fn materialize_epoch(&mut self) -> Result<(), Error> {
        self.epoch = self.next_model_epoch()?;
        self.pending_epoch = false;
        Ok(())
    }

    fn require_settled(&self) -> Result<(), Error> {
        if self.pending_calls.is_empty() {
            Ok(())
        } else {
            Err(Error::protocol("tool calls are awaiting results"))
        }
    }

    fn state_frame(&self, state: &Value) -> Result<Option<StateFrame>, Error> {
        let current = state
            .as_object()
            .ok_or_else(|| Error::config("state must be an object"))?;
        let Some(previous) = &self.committed_state else {
            return state_snapshot(state).map(Some);
        };
        if previous == state {
            return Ok(None);
        }
        // The retained State prefix owns the baseline. An epoch change updates
        // its session witness through a patch; only a replacement/empty prefix
        // without committed State needs a new snapshot.
        let previous = previous
            .as_object()
            .ok_or_else(|| Error::protocol("invalid committed state"))?;
        let mut set = current
            .iter()
            .filter(|(key, value)| previous.get(*key) != Some(*value))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Map<_, _>>();
        set.sort_keys();
        for value in set.values_mut() {
            value.sort_all_objects();
        }
        let mut remove = previous
            .keys()
            .filter(|key| !current.contains_key(*key))
            .cloned()
            .collect::<Vec<_>>();
        remove.sort();
        let mut collection_delta = |field: &str, id_field: &str| -> Result<Option<Value>, Error> {
            let Some(value) = set.get(field) else {
                return Ok(None);
            };
            let delta =
                microcrab_transport::keyed_state_delta(previous.get(field), value, id_field)
                    .map_err(Error::protocol)?;
            if delta.is_some() {
                set.remove(field);
            }
            Ok(delta)
        };
        let execution_delta = collection_delta("executions", "execution_id")?;
        Ok(Some(StateFrame::patch(set, remove, execution_delta)))
    }

    /// Measure the exact current admission envelope, including pending tool calls.
    pub fn measure(&self) -> Result<ContextMeasurement, Error> {
        let mut measurement = ContextMeasurement::measure(
            &self.system,
            &self.items,
            &self.tools,
            self.epoch,
            self.pending_epoch,
            self.max_bytes,
            self.pending_calls.len(),
        )?;
        measurement.baseline_generation = self.baseline_generation;
        Ok(measurement)
    }

    fn check_budget(&self, items: &[ContentItem]) -> Result<(), Error> {
        let bytes = measurement::budget_bytes(&self.system, items, &self.tools)?.len();
        crate::budget::check(crate::LimitKind::ContextBytes, bytes, self.max_bytes)
    }
}

pub(crate) fn validate_response(
    response: &Response,
    tools: &[ToolDefinition],
) -> Result<(), Error> {
    if response.content.trim().is_empty() && response.tool_calls.is_empty() {
        return Err(Error::protocol("empty assistant response"));
    }
    if let Some(metadata) = &response.provider_metadata {
        metadata
            .validate_projection(&response.content, &response.tool_calls)
            .map_err(|reason| Error::Protocol(reason.into()))?;
    }
    let mut ids = HashSet::new();
    for call in &response.tool_calls {
        if call.id.trim().is_empty() {
            return Err(Error::protocol("tool call ID is empty"));
        }
        if !ids.insert(call.id.as_str()) {
            return Err(Error::protocol("duplicate tool call ID"));
        }
        if !call.arguments.is_object() {
            return Err(Error::protocol("tool call arguments must be an object"));
        }
        if !tools.iter().any(|tool| tool.name == call.name) {
            return Err(Error::protocol("tool call names an unknown tool"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod protocol_epoch_tests {
    use super::*;
    use microcrab_protocol::{
        AssistantProviderMetadata, ToolCall, ToolCallProviderMetadata, Usage,
    };
    use serde_json::json;

    #[test]
    fn neutral_assistant_projection_is_checked_before_admission_and_on_restore() {
        use microcrab_protocol::{AssistantPart, AssistantReplayHint};
        let mut context = Context::new("system".into(), vec![], 65536).unwrap();
        context.admit_user("question", &json!({})).unwrap();
        let before = serde_json::to_vec(&context.snapshot().unwrap()).unwrap();
        let mut response = Response {
            content: "answer".into(),
            tool_calls: vec![],
            usage: Usage::default(),
            provider_metadata: Some(AssistantProviderMetadata::Neutral {
                parts: vec![
                    AssistantPart::Reasoning {
                        text: "reasoning".into(),
                    },
                    AssistantPart::Text {
                        text: "wrong answer".into(),
                    },
                ],
                replay_hints: vec![AssistantReplayHint {
                    part_index: 0,
                    scope: "test".into(),
                    kind: "signature".into(),
                    data: json!("opaque"),
                }],
            }),
        };
        assert!(context.append_response(&response).is_err());
        assert_eq!(
            serde_json::to_vec(&context.snapshot().unwrap()).unwrap(),
            before
        );
        if let Some(AssistantProviderMetadata::Neutral { parts, .. }) =
            &mut response.provider_metadata
        {
            parts[1] = AssistantPart::Text {
                text: "answer".into(),
            };
        }
        context.append_response(&response).unwrap();
        let snapshot = context.snapshot().unwrap();
        let frozen = serde_json::to_vec(&snapshot).unwrap();
        let restored = Context::from_snapshot(serde_json::from_slice(&frozen).unwrap()).unwrap();
        assert_eq!(
            serde_json::to_vec(&restored.snapshot().unwrap()).unwrap(),
            frozen
        );
        let mut invalid = snapshot;
        if let Some(ContentItem::Assistant { content, .. }) = invalid.items.last_mut() {
            *content = "different projection".into();
        }
        assert!(invalid.validate().is_err());
        context.refresh_recovery_epoch().unwrap();
        let encoded = serde_json::to_string(&context.items).unwrap();
        assert!(encoded.contains("reasoning"));
        assert!(!encoded.contains("opaque"));
    }

    #[test]
    fn protocol_epoch_keeps_one_snapshot_then_patch_before_each_new_user() {
        let mut context = Context::new("system".into(), vec![], 65536).unwrap();
        let initial =
            json!({"session":{"epoch":0,"turn":1,"round":1},"retained":"value","removed":true});
        context.admit_user("first", &initial).unwrap();
        let prefix = context.items.clone();
        context.prepare_protocol_epoch().unwrap();
        context.materialize_epoch().unwrap();
        let next = json!({"session":{"epoch":1,"turn":1,"round":1},"retained":"value"});
        context.admit_user("second", &next).unwrap();
        assert_eq!(&context.items[..prefix.len()], prefix.as_slice());
        assert!(matches!(
            context.items.as_slice(),
            [
                ContentItem::State(StateFrame::StateSnapshot { .. }),
                ContentItem::User { .. },
                ContentItem::State(StateFrame::StatePatch { .. }),
                ContentItem::User { .. },
            ]
        ));
        let ContentItem::State(StateFrame::StatePatch { set, remove, .. }) = &context.items[2]
        else {
            panic!("new epoch still patches retained State")
        };
        assert_eq!(
            serde_json::to_value(set).unwrap(),
            json!({"session":{"epoch":1,"turn":1,"round":1}})
        );
        assert_eq!(remove, &vec!["removed".to_owned()]);
        let before = context.items.clone();
        context.synchronize_state(&next).unwrap();
        assert_eq!(context.items, before);
        let restored = Context::from_snapshot(context.snapshot().unwrap()).unwrap();
        assert_eq!(restored.committed_state, Some(next));
        assert_eq!(restored.items, context.items);
    }

    #[test]
    fn compaction_replaces_prefix_with_one_snapshot_then_epoch_changes_are_patches() {
        let mut context = Context::new("system".into(), vec![], 65536).unwrap();
        context
            .admit_user(
                "discarded history",
                &json!({"session":{"epoch":0},"old":true}),
            )
            .unwrap();
        let baseline = json!({"session":{"epoch":0},"retained":"summary state"});
        context.compact("summary".into(), &baseline).unwrap();
        assert!(matches!(context.items.as_slice(), [
            ContentItem::State(StateFrame::StateSnapshot { state }),
            ContentItem::User { .. },
        ] if state == &baseline));
        let prefix = context.items.clone();
        context.materialize_epoch().unwrap();
        let next = json!({"session":{"epoch":1,"turn":1},"retained":"summary state"});
        context.admit_user("continue", &next).unwrap();
        assert_eq!(&context.items[..prefix.len()], prefix.as_slice());
        assert!(matches!(
            context.items.as_slice(),
            [
                ContentItem::State(StateFrame::StateSnapshot { .. }),
                ContentItem::User { .. },
                ContentItem::State(StateFrame::StatePatch { .. }),
                ContentItem::User { .. },
            ]
        ));
        let restored = Context::from_snapshot(context.snapshot().unwrap()).unwrap();
        assert_eq!(restored.committed_state, Some(next));
        assert_eq!(restored.items, context.items);
    }

    #[test]
    fn recovery_rebuilds_historical_epoch_snapshots_as_live_patches() {
        let mut context = Context::new("system".into(), vec![], 65536).unwrap();
        context
            .admit_user("first", &json!({"session":{"epoch":0},"value":1}))
            .unwrap();
        context.prepare_protocol_epoch().unwrap();
        context.materialize_epoch().unwrap();
        let later = json!({"session":{"epoch":1},"value":2});
        context.admit_user("second", &later).unwrap();
        let mut historical = context.snapshot().unwrap();
        historical.items[2] = ContentItem::State(StateFrame::StateSnapshot { state: later });
        let frozen = serde_json::to_vec(&historical).unwrap();
        let mut live = Context::from_snapshot(historical.clone()).unwrap();
        live.refresh_recovery_epoch().unwrap();
        assert_eq!(live.epoch(), 2);
        assert_eq!(
            live.items
                .iter()
                .filter(|item| matches!(item, ContentItem::State(StateFrame::StateSnapshot { .. })))
                .count(),
            1
        );
        assert!(
            matches!(&live.items[2], ContentItem::State(StateFrame::StatePatch { set, .. }) if set["session"]["epoch"] == 1 && set["value"] == 2)
        );
        assert!(
            matches!(live.items.last(), Some(ContentItem::State(StateFrame::StatePatch { set, .. })) if set["session"]["epoch"] == 2)
        );
        Context::from_snapshot(live.snapshot().unwrap()).unwrap();
        assert_eq!(serde_json::to_vec(&historical).unwrap(), frozen);
    }

    #[test]
    fn recovery_refresh_rebuilds_state_and_retains_settled_semantics() {
        let mut context = Context::new("frozen system".into(), vec![], 65536).unwrap();
        context
            .admit_user(
                "question",
                &json!({"session":{"epoch":0,"turn":1,"round":1},"value":1}),
            )
            .unwrap();
        context
            .synchronize_state(&json!({"session":{"epoch":0,"turn":1,"round":1},"value":2}))
            .unwrap();
        context
            .append_response(&Response {
                provider_metadata: None,
                content: "answer".into(),
                tool_calls: vec![],
                usage: Usage::default(),
            })
            .unwrap();
        let frozen = context.snapshot().unwrap();
        let frozen_bytes = serde_json::to_vec(&frozen).unwrap();
        context.refresh_recovery_epoch().unwrap();
        assert_eq!(context.epoch(), 1);
        assert!(!context.pending_epoch());
        assert_eq!(context.system(), frozen.system);
        assert_eq!(context.tools, frozen.tools);
        assert_eq!(
            context.committed_state.as_ref().unwrap()["session"],
            json!({"epoch":1,"turn":1,"round":1})
        );
        assert_eq!(context.committed_state.as_ref().unwrap()["value"], 2);
        let Some(ContentItem::State(StateFrame::StatePatch { set, remove, .. })) =
            context.items.last()
        else {
            panic!("retained State prefix requires an epoch patch")
        };
        assert_eq!(
            serde_json::to_value(set).unwrap(),
            json!({"session":{"epoch":1,"turn":1,"round":1}})
        );
        assert!(remove.is_empty());
        assert_eq!(
            context
                .items
                .iter()
                .filter(|item| matches!(item, ContentItem::State(StateFrame::StateSnapshot { .. })))
                .count(),
            1
        );
        assert_eq!(serde_json::to_vec(&frozen).unwrap(), frozen_bytes);
        let restored = Context::from_snapshot(context.snapshot().unwrap()).unwrap();
        assert_eq!(restored.items, context.items);
        let prefix = context.items.clone();
        context
            .synchronize_state(&json!({"session":{"epoch":1,"turn":1,"round":2},"value":2}))
            .unwrap();
        assert_eq!(&context.items[..prefix.len()], prefix.as_slice());
        assert!(matches!(
            context.items.last(),
            Some(ContentItem::State(StateFrame::StatePatch { .. }))
        ));
    }

    #[test]
    fn recovery_refresh_preserves_frozen_reference_schema_and_checks_budget_atomically() {
        let mut tool = crate::runtime::tool::reference::Reference::definition();
        tool.parameters["oneOf"] = json!([{"required":["action"]}]);
        let mut context = Context::new("system".into(), vec![tool], 65536).unwrap();
        context
            .admit_user(
                "question",
                &json!({"session":{"epoch":0,"turn":1,"round":1}}),
            )
            .unwrap();
        let frozen = context.snapshot().unwrap();
        let mut candidate = Context::from_snapshot(frozen.clone()).unwrap();
        candidate.refresh_recovery_epoch().unwrap();
        assert_eq!(candidate.tools, frozen.tools);
        assert!(candidate.tools[0].parameters.get("oneOf").is_some());
        context.max_bytes =
            measurement::budget_bytes(&context.system, &context.items, &context.tools)
                .unwrap()
                .len();
        let before = serde_json::to_vec(&context.snapshot().unwrap()).unwrap();
        assert!(matches!(
            context.refresh_recovery_epoch(),
            Err(Error::Limit(_))
        ));
        assert_eq!(
            serde_json::to_vec(&context.snapshot().unwrap()).unwrap(),
            before
        );
    }

    #[test]
    fn recovery_refresh_rejects_unsettled_and_overflow_without_changes() {
        let tool = ToolDefinition {
            name: "lookup".into(),
            description: "lookup".into(),
            parameters: json!({"type":"object"}),
        };
        let mut context = Context::new("system".into(), vec![tool], 65536).unwrap();
        context.admit_user("question", &json!({})).unwrap();
        context
            .append_response(&Response {
                provider_metadata: None,
                content: String::new(),
                tool_calls: vec![ToolCall {
                    id: "call".into(),
                    name: "lookup".into(),
                    arguments: json!({}),
                    provider_metadata: None,
                }],
                usage: Usage::default(),
            })
            .unwrap();
        let before = context.items.clone();
        assert!(
            context
                .refresh_recovery_epoch()
                .unwrap_err()
                .to_string()
                .contains("awaiting results")
        );
        assert_eq!(context.items, before);
        context
            .append_tool_result("call".into(), "result".into())
            .unwrap();
        context.epoch = u64::MAX;
        let before = serde_json::to_vec(&context.snapshot().unwrap()).unwrap();
        assert!(
            context
                .refresh_recovery_epoch()
                .unwrap_err()
                .to_string()
                .contains("overflow")
        );
        assert_eq!(
            serde_json::to_vec(&context.snapshot().unwrap()).unwrap(),
            before
        );
    }

    #[test]
    fn protocol_candidate_clears_hints_but_preserves_reasoning_and_frozen_history() {
        let tool: ToolDefinition = serde_json::from_value(
            json!({"name":"lookup","description":"lookup","parameters":{"type":"object"}}),
        )
        .unwrap();
        let mut context = Context::new("frozen system".into(), vec![tool], 65536).unwrap();
        context.admit_user("question", &json!({})).unwrap();
        context.append_response(&Response {
            content: "visible answer".into(), usage: Usage::default(),
            tool_calls: vec![ToolCall { id:"call".into(), name:"lookup".into(), arguments:json!({}), provider_metadata:Some(ToolCallProviderMetadata::Google { thought_signature:"signed".into() }) }],
            provider_metadata: Some(AssistantProviderMetadata::Google { parts: vec![
                json!({"text":"private thought","thought":true,"thoughtSignature":"hidden"}),
                json!({"text":"visible answer"}),
                json!({"functionCall":{"id":"call","name":"lookup","args":{}},"thoughtSignature":"signed"}),
            ] }),
        }).unwrap();
        context
            .append_tool_result("call".into(), "result".into())
            .unwrap();
        let historical = context.snapshot().unwrap();
        let bytes = serde_json::to_value(&historical).unwrap();
        let mut candidate = context.clone();
        candidate.prepare_protocol_epoch().unwrap();
        assert_eq!(
            serde_json::to_value(context.snapshot().unwrap()).unwrap(),
            bytes
        );
        assert_eq!(serde_json::to_value(&historical).unwrap(), bytes);
        assert!(candidate.pending_epoch());
        assert_eq!(candidate.system(), context.system());
        match &candidate.items[2] {
            ContentItem::Assistant {
                content,
                tool_calls,
                provider_metadata,
            } => {
                assert_eq!(content, "visible answer");
                assert!(matches!(provider_metadata,
                    Some(AssistantProviderMetadata::Neutral { parts, replay_hints })
                        if replay_hints.is_empty() && parts.iter().any(|part| matches!(part,
                            microcrab_protocol::AssistantPart::Reasoning { text } if text == "private thought"
                        ))
                ));
                assert!(tool_calls[0].provider_metadata.is_none());
                assert_eq!(tool_calls[0].id, "call");
            }
            _ => panic!("preserved assistant position"),
        }
        let live_bytes = serde_json::to_string(&candidate.items).unwrap();
        assert!(live_bytes.contains("private thought"));
        assert!(!live_bytes.contains("thoughtSignature"));
        assert!(!live_bytes.contains("hidden"));
        assert_eq!(candidate.next_model_epoch().unwrap(), context.epoch() + 1);
    }
}

#[cfg(test)]
mod tool_media_tests;

#[cfg(test)]
mod carrier_tests;
