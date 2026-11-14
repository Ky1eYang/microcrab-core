//! Reconstructed display identities. The append log remains the only authority.
use super::*;
use std::collections::BTreeMap;
mod import;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DisplayBlock {
    pub block_id: String,
    pub parent_block_id: Option<String>,
    pub block_type: String,
    pub content_ref: String,
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub attempt_id: String,
    pub stream_key: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DisplayChunk {
    #[serde(flatten)]
    pub block: DisplayBlock,
    pub content_offset: u64,
    pub content_bytes: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DisplayHeadTransition {
    pub previous_block_id: Option<String>,
    pub selected_block_id: Option<String>,
}
#[derive(Clone, Debug, Serialize)]
pub struct DisplayGraph {
    pub blocks: BTreeMap<String, DisplayBlock>,
    pub selected_head: Option<String>,
    pub selection_known: bool,
    pub legacy_content_records: u64,
    pub projection_incomplete: bool,
    pub high_water: u64,
    pub checkpoints: BTreeMap<String, Option<String>>,
    pub attempt_parents: BTreeMap<String, Option<String>>,
    pub pending_selection: Option<serde_json::Value>,
    #[serde(skip)]
    origins: BTreeMap<String, DisplayScope>,
    #[serde(skip)]
    selection_intents: BTreeMap<String, serde_json::Value>,
    #[serde(skip)]
    unresolved_selection: bool,
    #[serde(skip)]
    scope: DisplayScope,
    #[serde(skip)]
    lengths: BTreeMap<String, u64>,
    #[serde(skip)]
    active: Option<String>,
}
impl DisplayGraph {
    pub fn empty(scope: DisplayScope) -> Self {
        Self {
            blocks: BTreeMap::new(),
            selected_head: None,
            selection_known: true,
            legacy_content_records: 0,
            projection_incomplete: false,
            high_water: 0,
            checkpoints: BTreeMap::new(),
            attempt_parents: BTreeMap::new(),
            pending_selection: None,
            origins: BTreeMap::new(),
            selection_intents: BTreeMap::new(),
            unresolved_selection: false,
            scope,
            lengths: BTreeMap::new(),
            active: None,
        }
    }
    pub fn content_bytes(&self, block_id: &str) -> Option<u64> {
        self.lengths.get(block_id).copied()
    }
    fn identity(&self, block: &DisplayBlock) -> Result<String, Error> {
        let bytes = serde_json::to_vec(&(
            "microcrab.display.block.v1",
            &self.scope,
            &block.parent_block_id,
            &block.block_type,
            &block.attempt_id,
            &block.stream_key,
            block.sequence,
        ))?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
    /// Only content producer phases allocate nodes; admission and model Context
    /// certificates never acquire implicit display placement authority.
    pub fn decorate(&self, record: &mut ArchiveRecord, timestamp_ms: u64) -> Result<(), Error> {
        if record.event.get("display_block").is_some() || record.event.get("display_head").is_some()
        {
            return Err(failure("display graph fields are recorder-owned"));
        }
        let Some((kind, stream, bytes)) = content(&record.event)? else {
            return Ok(());
        };
        let existing = self
            .active
            .as_ref()
            .and_then(|id| self.blocks.get(id))
            .filter(|b| {
                record.event["phase"] != "display_canonical"
                    && b.attempt_id == record.attempt_id
                    && b.stream_key == stream
                    && b.block_type == kind
                    && self.selected_head.as_ref() == Some(&b.block_id)
            });
        let block = match existing {
            Some(block) => block.clone(),
            None => {
                let mut block = DisplayBlock {
                    block_id: String::new(),
                    parent_block_id: self.selected_head.clone(),
                    block_type: kind,
                    content_ref: String::new(),
                    sequence: record.sequence,
                    timestamp_ms,
                    attempt_id: record.attempt_id.clone(),
                    stream_key: stream,
                };
                block.block_id = self.identity(&block)?;
                block.content_ref = format!("display:{}", block.block_id);
                record.event["display_head"] = serde_json::to_value(DisplayHeadTransition {
                    previous_block_id: self.selected_head.clone(),
                    selected_block_id: Some(block.block_id.clone()),
                })?;
                block
            }
        };
        let offset = self.lengths.get(&block.block_id).copied().unwrap_or(0);
        record.event["display_block"] = serde_json::to_value(DisplayChunk {
            block,
            content_offset: offset,
            content_bytes: bytes,
        })?;
        Ok(())
    }
    /// Validate without mutation before persistence; replay applies the same checks.
    pub fn validate(&self, record: &ArchiveRecord) -> Result<(), Error> {
        if self.high_water.checked_add(1) != Some(record.sequence) {
            return Err(failure("display graph sequence mismatch"));
        }
        if let Some(version) = record.event.get("display_graph_version") {
            if version.as_u64() != Some(1) {
                return Err(failure("unsupported display graph version"));
            }
            if content(&record.event)?.is_some() && record.event.get("display_block").is_none() {
                return Err(failure(
                    "graph-aware display content is missing its block certificate",
                ));
            }
        }
        let chunk: Option<DisplayChunk> = record
            .event
            .get("display_block")
            .cloned()
            .map(serde_json::from_value)
            .transpose()?;
        let transition: Option<DisplayHeadTransition> = record
            .event
            .get("display_head")
            .cloned()
            .map(serde_json::from_value)
            .transpose()?;
        if let Some(chunk) = &chunk {
            let block = &chunk.block;
            let Some((kind, stream, bytes)) = content(&record.event)? else {
                return Err(failure("display block has no content phase"));
            };
            if block.attempt_id != record.attempt_id
                || block.block_type != kind
                || block.stream_key != stream
                || chunk.content_bytes != bytes
                || block.block_id != self.identity(block)?
                || block.content_ref != format!("display:{}", block.block_id)
            {
                return Err(failure("display block identity/content mismatch"));
            }
            if let Some(existing) = self.blocks.get(&block.block_id) {
                if existing != block
                    || self.active.as_ref() != Some(&block.block_id)
                    || self.selected_head.as_ref() != Some(&block.block_id)
                    || transition.is_some()
                {
                    return Err(failure("display immutable block/segment mismatch"));
                }
            } else if block.sequence != record.sequence
                || block.parent_block_id != self.selected_head
                || block
                    .parent_block_id
                    .as_ref()
                    .is_some_and(|id| !self.blocks.contains_key(id))
                || transition
                    .as_ref()
                    .and_then(|t| t.selected_block_id.as_ref())
                    != Some(&block.block_id)
            {
                return Err(failure("display new block parent/head mismatch"));
            }
            let offset = self.lengths.get(&block.block_id).copied().unwrap_or(0);
            if chunk.content_offset != offset || offset.checked_add(bytes).is_none() {
                return Err(failure("display block content offset mismatch"));
            }
        }
        if let Some(transition) = &transition {
            if transition.previous_block_id != self.selected_head {
                return Err(failure("display selected head conflict"));
            }
            let new_node = chunk.as_ref().map(|c| &c.block.block_id);
            if transition
                .selected_block_id
                .as_ref()
                .is_some_and(|id| !self.blocks.contains_key(id) && Some(id) != new_node)
            {
                return Err(failure("display selected head references missing block"));
            }
            if chunk.is_none() && record.event["phase"] != "display_head_selected" {
                return Err(failure(
                    "display head transition has no selection certificate",
                ));
            }
        } else if record.event["phase"] == "display_head_selected" {
            return Err(failure("display selection certificate missing transition"));
        }
        if record.event["phase"] == "display_head_selected"
            && record.event["select_attempt_parent"] == true
        {
            let baseline = self
                .attempt_parents
                .get(&record.attempt_id)
                .ok_or_else(|| failure("display retry attempt has no recorded parent"))?;
            if transition.as_ref().map(|head| &head.selected_block_id) != Some(baseline) {
                return Err(failure("display retry parent certificate mismatch"));
            }
        }
        if matches!(
            record.event["phase"].as_str(),
            Some("selection_intent" | "selection_commit")
        ) && record.event.get("selected_head").is_some()
        {
            let selected: Option<String> =
                serde_json::from_value(record.event["selected_head"].clone())?;
            if selected != self.selected_head {
                return Err(failure("display navigation head certificate mismatch"));
            }
        }
        if record.event["phase"] == "display_head_selected" {
            if let Some(known) = record.event.get("selected_head_known") {
                let known = known
                    .as_bool()
                    .ok_or_else(|| failure("display selection knowledge is not boolean"))?;
                if !known
                    && transition
                        .as_ref()
                        .is_some_and(|head| head.selected_block_id.is_some())
                {
                    return Err(failure("unknown display selection cannot certify a head"));
                }
            }
        }
        if record.event["phase"] == "display_graph_import" {
            if chunk.is_some() || transition.is_some() {
                return Err(failure(
                    "display fork import contains conflicting graph fields",
                ));
            }
            self.validate_import(&record.event)?;
        }
        if record.event["phase"] == "started"
            && self.attempt_parents.contains_key(&record.attempt_id)
        {
            return Err(failure("display attempt start identity reused"));
        }
        if record.event["phase"] == "display_checkpoint" {
            let checkpoint = record.event["checkpoint_id"]
                .as_str()
                .filter(|id| !id.is_empty())
                .ok_or_else(|| failure("display checkpoint identity missing"))?;
            let selected: Option<String> =
                serde_json::from_value(record.event["selected_head"].clone())?;
            if selected != self.selected_head
                || self
                    .checkpoints
                    .get(checkpoint)
                    .is_some_and(|head| head != &selected)
            {
                return Err(failure("display checkpoint head conflict"));
            }
        }
        Ok(())
    }
    pub fn apply(&mut self, record: &ArchiveRecord) -> Result<(), Error> {
        self.validate(record)?;
        if record.event["phase"] == "display_projection_unavailable" {
            self.projection_incomplete = true;
        }
        if record.event["phase"] == "display_graph_import" {
            self.apply_import(&record.event)?;
        }
        match record.event["phase"].as_str() {
            Some("selection_intent") => {
                if self
                    .selection_intents
                    .insert(record.attempt_id.clone(), record.event.clone())
                    .is_some()
                {
                    self.unresolved_selection = true;
                }
            }
            Some("selection_commit" | "selection_abort") => {
                if self.selection_intents.remove(&record.attempt_id).is_none() {
                    self.unresolved_selection = true;
                }
            }
            _ => {}
        }
        self.pending_selection = if self.unresolved_selection || !self.selection_intents.is_empty()
        {
            Some(
                serde_json::json!({"intents":self.selection_intents,"unresolved":self.unresolved_selection}),
            )
        } else {
            None
        };
        if record.event["phase"] == "started" {
            self.attempt_parents
                .insert(record.attempt_id.clone(), self.selected_head.clone());
            self.active = None;
        }
        if let Some(value) = record.event.get("display_block") {
            let chunk: DisplayChunk = serde_json::from_value(value.clone())?;
            self.lengths.insert(
                chunk.block.block_id.clone(),
                chunk.content_offset + chunk.content_bytes,
            );
            self.active = Some(chunk.block.block_id.clone());
            self.blocks
                .entry(chunk.block.block_id.clone())
                .or_insert(chunk.block);
        } else if content(&record.event)?.is_some() {
            self.legacy_content_records += 1;
            self.active = None;
        }
        if let Some(value) = record.event.get("display_head") {
            let transition: DisplayHeadTransition = serde_json::from_value(value.clone())?;
            self.selected_head = transition.selected_block_id;
            if record.event["phase"] == "display_head_selected" {
                self.selection_known = record.event["selected_head_known"]
                    .as_bool()
                    .unwrap_or(true);
            }
            if record.event.get("display_block").is_none() {
                self.active = None;
            }
        }
        if matches!(
            record.event["phase"].as_str(),
            Some("validated" | "invalidated")
        ) {
            self.active = None;
        }
        if record.event["phase"] == "display_checkpoint" {
            let checkpoint = record.event["checkpoint_id"]
                .as_str()
                .ok_or_else(|| failure("display checkpoint identity missing"))?;
            let selected = serde_json::from_value(record.event["selected_head"].clone())?;
            self.checkpoints.insert(checkpoint.into(), selected);
        }
        self.high_water = record.sequence;
        Ok(())
    }
}
fn content(event: &serde_json::Value) -> Result<Option<(String, String, u64)>, Error> {
    if event["phase"] == "display_canonical" {
        let kind = event["block_type"]
            .as_str()
            .filter(|kind| !kind.is_empty())
            .ok_or_else(|| failure("display canonical block type missing"))?;
        if event.get("content").is_none() {
            return Err(failure("display canonical content missing"));
        }
        return Ok(Some((
            kind.into(),
            format!("canonical:{kind}"),
            serde_json::to_vec(&event["content"])?.len() as u64,
        )));
    }
    let (kind, key, field) = match event["phase"].as_str() {
        Some("reasoning_delta") => ("reasoning", "reasoning".into(), "text"),
        Some("text_delta") => ("text", "text".into(), "text"),
        Some("tool_call_delta") => ("tool_call", format!("tool:{}", event["index"]), "arguments"),
        _ => return Ok(None),
    };
    let text = event[field]
        .as_str()
        .ok_or_else(|| failure("display content field is not text"))?;
    Ok(Some((kind.into(), key, text.len() as u64)))
}
