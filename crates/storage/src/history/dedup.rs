//! Stage B split-header checkpoint nodes (v5) and content-addressed Recover blobs.
use super::ancestry::VerifiedNode;
use super::{
    CreatorEnvelope, HISTORY_VERSION, HistoryNode, JournalCursor, RestorePoint, canonical_bytes,
    content_hash_value, valid_hash,
};
use microcrab_protocol::ToolDefinition;
use microcrab_session::context::{
    CompactionMarker, ContentItem, ContextSnapshot, ContextUsageEvidence, system::PromptConfig,
};
use microcrab_session::runtime::journal::{
    HistoricalAgent, Recover, RoundCacheReport, RoundContinuation,
};
use microcrab_session::{Error, NavigationBoundary, SessionState};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub const HISTORY_NODE_V5: u32 = 5;
pub const ITEM_CHUNK_SIZE: usize = 16;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoverRef {
    pub context_static_hash: String,
    pub item_chunk_hashes: Vec<String>,
    pub tail_hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreatorEnvelopeV5 {
    pub session_id: String,
    pub epoch: u64,
    pub recover_ref: RecoverRef,
    pub request_head: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_selection: Option<crate::directory::ModelSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_witness: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub navigation_boundary: Option<NavigationBoundary>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryNodeV5 {
    pub version: u32,
    pub kind: String,
    pub parent: Option<String>,
    pub checkpoint: CreatorEnvelopeV5,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextStaticBlob {
    pub system: String,
    pub tools: Vec<ToolDefinition>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub historical_tools: BTreeMap<u64, Vec<ToolDefinition>>,
    pub max_bytes: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ItemChunkBlob {
    pub items: Vec<ContentItem>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoverTailBlob {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_config: Option<PromptConfig>,
    pub state: SessionState,
    pub continuation: Option<RoundContinuation>,
    pub workspace: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_report: Option<RoundCacheReport>,
    pub agent: Option<HistoricalAgent>,
    pub context_epoch: u64,
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
    pub committed_state: Option<serde_json::Value>,
    pub used_call_ids: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub retired_call_ids: BTreeSet<String>,
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn encode_blob<T: Serialize>(value: &T) -> Result<(String, Vec<u8>), Error> {
    let bytes = canonical_bytes(value)?;
    let hash = sha256_hex(&bytes);
    Ok((hash, bytes))
}

fn decode_blob<T: for<'de> Deserialize<'de>>(
    expected_hash: &str,
    bytes: &[u8],
) -> Result<T, Error> {
    valid_hash(expected_hash)?;
    if sha256_hex(bytes) != expected_hash {
        return Err(Error::Protocol(
            "checkpoint blob content hash mismatch".into(),
        ));
    }
    Ok(serde_json::from_slice(bytes)?)
}

impl HistoryNodeV5 {
    pub fn id(&self) -> Result<String, Error> {
        content_hash_value(serde_json::to_value(self)?)
    }

    pub fn validate_header(&self, id: &str) -> Result<(), Error> {
        valid_hash(id)?;
        if let Some(parent) = &self.parent {
            valid_hash(parent)?;
        }
        if self.version != HISTORY_NODE_V5 || self.kind != "checkpoint" {
            return Err(Error::Protocol(
                "checkpoint content hash or schema mismatch".into(),
            ));
        }
        if self.checkpoint.session_id.trim().is_empty() || self.checkpoint.session_id.len() > 1024 {
            return Err(Error::config("invalid creator session id"));
        }
        if let Some(req) = &self.checkpoint.request_head {
            valid_hash(req)?;
        }
        valid_hash(&self.checkpoint.recover_ref.context_static_hash)?;
        valid_hash(&self.checkpoint.recover_ref.tail_hash)?;
        for chunk_hash in &self.checkpoint.recover_ref.item_chunk_hashes {
            valid_hash(chunk_hash)?;
        }
        if self.id()? != id {
            return Err(Error::Protocol(
                "checkpoint content hash or schema mismatch".into(),
            ));
        }
        Ok(())
    }

    pub fn cursor(&self, id: String) -> JournalCursor {
        JournalCursor {
            session_id: self.checkpoint.session_id.clone(),
            checkpoint_id: id,
            epoch: self.checkpoint.epoch,
        }
    }
}

/// Split a validated `CreatorEnvelope` into a lightweight `HistoryNodeV5` header
/// and deduplicated `(hash, canonical_bytes)` blobs.
pub fn split_v5(
    parent: Option<String>,
    checkpoint: &CreatorEnvelope,
) -> Result<(HistoryNodeV5, Vec<(String, Vec<u8>)>), Error> {
    let mut blobs = Vec::new();
    let ctx = &checkpoint.recover.context;
    let static_blob = ContextStaticBlob {
        system: ctx.system.clone(),
        tools: ctx.tools.clone(),
        historical_tools: ctx.historical_tools.clone(),
        max_bytes: ctx.max_bytes,
    };
    let (context_static_hash, static_bytes) = encode_blob(&static_blob)?;
    blobs.push((context_static_hash.clone(), static_bytes));

    let mut item_chunk_hashes = Vec::with_capacity(ctx.items.len().div_ceil(ITEM_CHUNK_SIZE));
    for chunk in ctx.items.chunks(ITEM_CHUNK_SIZE) {
        let chunk_blob = ItemChunkBlob {
            items: chunk.to_vec(),
        };
        let (chunk_hash, chunk_bytes) = encode_blob(&chunk_blob)?;
        item_chunk_hashes.push(chunk_hash);
        blobs.push((
            item_chunk_hashes.last().expect("just pushed").clone(),
            chunk_bytes,
        ));
    }

    let tail_blob = RecoverTailBlob {
        prompt_config: checkpoint.recover.prompt_config.clone(),
        state: checkpoint.recover.state.clone(),
        continuation: checkpoint.recover.continuation.clone(),
        workspace: checkpoint
            .recover
            .workspace
            .as_ref()
            .map(serde_json::to_value)
            .transpose()?,
        cache_report: checkpoint.recover.cache_report.clone(),
        agent: checkpoint.recover.agent.clone(),
        context_epoch: ctx.epoch,
        baseline_generation: ctx.baseline_generation,
        usage_evidence: ctx.usage_evidence.clone(),
        auto_compaction_attempt: ctx.auto_compaction_attempt.clone(),
        auto_compaction_retry: ctx.auto_compaction_retry,
        pending_epoch: ctx.pending_epoch,
        compaction: ctx.compaction.clone(),
        committed_state: ctx.committed_state.clone(),
        used_call_ids: ctx.used_call_ids.clone(),
        retired_call_ids: ctx.retired_call_ids.clone(),
    };
    let (tail_hash, tail_bytes) = encode_blob(&tail_blob)?;
    blobs.push((tail_hash.clone(), tail_bytes));

    let header = HistoryNodeV5 {
        version: HISTORY_NODE_V5,
        kind: "checkpoint".into(),
        parent,
        checkpoint: CreatorEnvelopeV5 {
            session_id: checkpoint.session_id.clone(),
            epoch: checkpoint.recover.state.epoch_id,
            recover_ref: RecoverRef {
                context_static_hash,
                item_chunk_hashes,
                tail_hash,
            },
            request_head: checkpoint.request_head.clone(),
            model_selection: checkpoint.model_selection.clone(),
            route_witness: checkpoint.route_witness.clone(),
            navigation_boundary: checkpoint.navigation_boundary.clone(),
        },
    };
    Ok((header, blobs))
}

/// Reconstruct and validate a full `RestorePoint` from a `HistoryNodeV5` header
/// and content-addressed blobs.
pub fn reconstruct_v5(
    id: &str,
    header: HistoryNodeV5,
    mut load_blob: impl FnMut(&str) -> Result<Vec<u8>, Error>,
) -> Result<RestorePoint, Error> {
    header.validate_header(id)?;
    let rref = &header.checkpoint.recover_ref;
    let static_bytes = load_blob(&rref.context_static_hash)?;
    let static_blob: ContextStaticBlob = decode_blob(&rref.context_static_hash, &static_bytes)?;

    let mut items = Vec::with_capacity(rref.item_chunk_hashes.len() * ITEM_CHUNK_SIZE);
    for chunk_hash in &rref.item_chunk_hashes {
        let chunk_bytes = load_blob(chunk_hash)?;
        let chunk_blob: ItemChunkBlob = decode_blob(chunk_hash, &chunk_bytes)?;
        items.extend(chunk_blob.items);
    }

    let tail_bytes = load_blob(&rref.tail_hash)?;
    let tail_blob: RecoverTailBlob = decode_blob(&rref.tail_hash, &tail_bytes)?;

    let envelope = CreatorEnvelope {
        session_id: header.checkpoint.session_id.clone(),
        recover: Recover {
            prompt_config: tail_blob.prompt_config,
            state: tail_blob.state,
            context: ContextSnapshot {
                epoch: tail_blob.context_epoch,
                baseline_generation: tail_blob.baseline_generation,
                usage_evidence: tail_blob.usage_evidence,
                auto_compaction_attempt: tail_blob.auto_compaction_attempt,
                auto_compaction_retry: tail_blob.auto_compaction_retry,
                pending_epoch: tail_blob.pending_epoch,
                compaction: tail_blob.compaction,
                system: static_blob.system,
                items,
                tools: static_blob.tools,
                historical_tools: static_blob.historical_tools,
                committed_state: tail_blob.committed_state,
                used_call_ids: tail_blob.used_call_ids,
                retired_call_ids: tail_blob.retired_call_ids,
                max_bytes: static_blob.max_bytes,
            },
            continuation: tail_blob.continuation,
            workspace: tail_blob
                .workspace
                .map(serde_json::from_value)
                .transpose()?,
            cache_report: tail_blob.cache_report,
            agent: tail_blob.agent,
        },
        request_head: header.checkpoint.request_head.clone(),
        model_selection: header.checkpoint.model_selection.clone(),
        route_witness: header.checkpoint.route_witness.clone(),
        navigation_boundary: header.checkpoint.navigation_boundary.clone(),
    };
    envelope.validate()?;
    let cursor = JournalCursor {
        session_id: envelope.session_id.clone(),
        checkpoint_id: id.to_owned(),
        epoch: envelope.recover.state.epoch_id,
    };
    if cursor.epoch != header.checkpoint.epoch {
        return Err(Error::Protocol(
            "checkpoint header epoch does not match recover state".into(),
        ));
    }
    Ok(RestorePoint {
        parent: header.parent,
        cursor,
        checkpoint: envelope,
    })
}

#[derive(Deserialize)]
struct VersionPeek {
    version: u32,
}

/// Decode and validate either a v4 (`HistoryNode`) or v5 (`HistoryNodeV5`) node
/// into a lightweight `VerifiedNode` without hydrating v5 blobs.
pub(super) fn verify_node_metadata(
    bytes: &[u8],
    id: &str,
    expected_session: &str,
) -> Result<VerifiedNode, Error> {
    let peek: VersionPeek = serde_json::from_slice(bytes)?;
    match peek.version {
        HISTORY_VERSION => {
            let node: HistoryNode = serde_json::from_slice(bytes)?;
            node.validate(id)?;
            if node.checkpoint.session_id != expected_session {
                return Err(Error::Protocol("journal cursor conflict".into()));
            }
            Ok(VerifiedNode::from_point(RestorePoint {
                cursor: node.cursor(id.into()),
                parent: node.parent,
                checkpoint: node.checkpoint,
            }))
        }
        HISTORY_NODE_V5 => {
            let node: HistoryNodeV5 = serde_json::from_slice(bytes)?;
            node.validate_header(id)?;
            if node.checkpoint.session_id != expected_session {
                return Err(Error::Protocol("journal cursor conflict".into()));
            }
            Ok(VerifiedNode {
                id: id.to_owned(),
                parent: node.parent,
                epoch: node.checkpoint.epoch,
                request_head: node.checkpoint.request_head,
            })
        }
        _ => Err(Error::Protocol(
            "checkpoint content hash or schema mismatch".into(),
        )),
    }
}

/// Decode and reconstruct a full `RestorePoint` for either v4 or v5 nodes.
pub(super) fn decode_restore_point(
    bytes: &[u8],
    id: &str,
    expected_session: &str,
    load_blob: impl FnMut(&str) -> Result<Vec<u8>, Error>,
) -> Result<RestorePoint, Error> {
    let peek: VersionPeek = serde_json::from_slice(bytes)?;
    match peek.version {
        HISTORY_VERSION => {
            let node: HistoryNode = serde_json::from_slice(bytes)?;
            node.validate(id)?;
            if node.checkpoint.session_id != expected_session {
                return Err(Error::Protocol("journal cursor conflict".into()));
            }
            Ok(RestorePoint {
                cursor: node.cursor(id.into()),
                parent: node.parent,
                checkpoint: node.checkpoint,
            })
        }
        HISTORY_NODE_V5 => {
            let node: HistoryNodeV5 = serde_json::from_slice(bytes)?;
            if node.checkpoint.session_id != expected_session {
                return Err(Error::Protocol("journal cursor conflict".into()));
            }
            reconstruct_v5(id, node, load_blob)
        }
        _ => Err(Error::Protocol(
            "checkpoint content hash or schema mismatch".into(),
        )),
    }
}
