//! Creator-owned identity and content-addressed history; core Recover is opaque.
use microcrab_session::{Error, Recover};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const HISTORY_VERSION: u32 = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct HistoryAvailable {
    pub undo: bool,
    pub redo: bool,
}

/// Nonblocking projection for audit attribution; selection remains authoritative.
#[derive(Clone, Debug, Default)]
pub struct AuditAnchor {
    pub display_lineage: String,
    pub history_head: Option<String>,
    pub request_head: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreatorEnvelope {
    pub session_id: String,
    pub recover: Recover,
    pub request_head: Option<String>,
    /// Creator routing is immutable with this new node; legacy None serializes identically.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_selection: Option<crate::directory::ModelSelection>,
    /// Hash of nonsecret effective wire routing; absent history is not route evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_witness: Option<String>,
    /// Session-certified navigation provenance; absent internal/legacy nodes keep
    /// their original canonical encoding and remain available for recovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub navigation_boundary: Option<microcrab_session::NavigationBoundary>,
}
impl CreatorEnvelope {
    pub fn validate(&self) -> Result<(), Error> {
        if self.session_id.trim().is_empty() || self.session_id.len() > 1024 {
            return Err(Error::config("invalid creator session id"));
        }
        if let Some(id) = &self.request_head {
            valid_hash(id)?;
        }
        if let Some(model) = &self.model_selection {
            if model.provider_id.trim().is_empty()
                || model.provider_id.len() > 256
                || model.model.trim().is_empty()
                || model.model.len() > 1024
                || model
                    .variant
                    .as_ref()
                    .is_some_and(|v| v.trim().is_empty() || v.len() > 64)
            {
                return Err(Error::config("invalid creator model route selection"));
            }
        }
        if let Some(witness) = &self.route_witness {
            valid_hash(witness)?;
            if self.model_selection.is_none() {
                return Err(Error::config("route witness requires model selection"));
            }
        }
        if matches!(
            self.navigation_boundary,
            Some(
                microcrab_session::NavigationBoundary::EpochInitial
                    | microcrab_session::NavigationBoundary::InterTurn
            )
        ) && (self.recover.continuation.is_some() || self.recover.context.pending_epoch)
        {
            return Err(Error::config(
                "navigation boundary requires a settled recovery and materialized epoch",
            ));
        }
        self.recover.validate()
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalCursor {
    pub session_id: String,
    pub checkpoint_id: String,
    pub epoch: u64,
}
/// Runtime-only append continuity; never serialized into durable checkpoints.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DisplayWitness {
    pub lineage: String,
    pub cursor: Option<JournalCursor>,
}

#[derive(Clone, Debug)]
pub struct RestorePoint {
    pub parent: Option<String>,
    pub cursor: JournalCursor,
    pub checkpoint: CreatorEnvelope,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Travel {
    Undo,
    /// An ancestor selected by CreatorJournal's input-boundary resolver. Retain
    /// the old head as one redo step, including every skipped round checkpoint.
    UndoTo {
        checkpoint_id: String,
    },
    Redo,
    To {
        checkpoint_id: String,
    },
}
#[derive(Clone, Debug)]
pub enum RestoreTarget {
    Head,
    Checkpoint { checkpoint_id: String },
}
#[derive(Clone, Copy, Debug)]
pub struct JournalOptions {
    pub max_checkpoint_bytes: usize,
}
impl Default for JournalOptions {
    fn default() -> Self {
        Self {
            max_checkpoint_bytes: 64 * 1024 * 1024,
        }
    }
}

/// The sole checkpoint authority. Its identifier hashes every field, including
/// the causal parent; navigation metadata never changes this immutable payload.
#[derive(Clone)]
pub struct HistoryNode {
    pub version: u32,
    pub kind: String,
    pub parent: Option<String>,
    pub checkpoint: CreatorEnvelope,
    /// Original and decoded projections are kept separately: historical absent,
    /// null and default fields belong to the content hash. Public-field edits
    /// produce a new encoding rather than retaining an obsolete hash witness.
    frozen: Option<(serde_json::Value, serde_json::Value)>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeFields {
    version: u32,
    kind: String,
    parent: Option<String>,
    checkpoint: CreatorEnvelope,
}
#[derive(Serialize)]
struct NodeView<'a> {
    version: u32,
    kind: &'a str,
    parent: &'a Option<String>,
    checkpoint: &'a CreatorEnvelope,
}
impl Serialize for HistoryNode {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let current = serde_json::to_value(NodeView {
            version: self.version,
            kind: &self.kind,
            parent: &self.parent,
            checkpoint: &self.checkpoint,
        })
        .map_err(serde::ser::Error::custom)?;
        let value = match &self.frozen {
            Some((original, decoded)) if decoded == &current => original,
            _ => &current,
        };
        value.serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for HistoryNode {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let original = serde_json::Value::deserialize(deserializer)?;
        let fields: NodeFields =
            serde_json::from_value(original.clone()).map_err(serde::de::Error::custom)?;
        let decoded = serde_json::to_value(&fields).map_err(serde::de::Error::custom)?;
        Ok(Self {
            version: fields.version,
            kind: fields.kind,
            parent: fields.parent,
            checkpoint: fields.checkpoint,
            frozen: Some((original, decoded)),
        })
    }
}
impl HistoryNode {
    pub fn new(parent: Option<String>, checkpoint: CreatorEnvelope) -> Self {
        Self {
            version: HISTORY_VERSION,
            kind: "checkpoint".into(),
            parent,
            checkpoint,
            frozen: None,
        }
    }
    pub fn id(&self) -> Result<String, Error> {
        content_hash(self)
    }
    pub fn validate(&self, id: &str) -> Result<(), Error> {
        valid_hash(id)?;
        if let Some(parent) = &self.parent {
            valid_hash(parent)?;
        }
        if self.version != HISTORY_VERSION || self.kind != "checkpoint" {
            return Err(Error::Protocol(
                "checkpoint content hash or schema mismatch".into(),
            ));
        }
        // The content hash and the Recover validation each walk the whole
        // checkpoint; run them side by side. The hash verdict still wins.
        let (hash, checkpoint) = std::thread::scope(|scope| {
            let hash = scope.spawn(|| self.id());
            let checkpoint = self.checkpoint.validate();
            (hash.join(), checkpoint)
        });
        let hash = hash.map_err(|_| Error::Protocol("checkpoint hash worker failed".into()))??;
        if hash != id {
            return Err(Error::Protocol(
                "checkpoint content hash or schema mismatch".into(),
            ));
        }
        checkpoint
    }
    pub fn cursor(&self, id: String) -> JournalCursor {
        JournalCursor {
            session_id: self.checkpoint.session_id.clone(),
            checkpoint_id: id,
            epoch: self.checkpoint.recover.state.epoch_id,
        }
    }
}

pub fn valid_hash(id: &str) -> Result<(), Error> {
    if id.len() != 64
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Error::config("invalid SHA256 history identifier"));
    }
    Ok(())
}

/// Canonical JSON v4: recursively sorted object keys, compact serde JSON scalar
/// encoding, and original array order. Version and kind domain-separate nodes.
fn sort_canonical(value: &mut serde_json::Value) {
    sort(value);
    fn sort(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Object(map) => {
                map.sort_keys();
                for child in map.values_mut() {
                    sort(child);
                }
            }
            serde_json::Value::Array(items) => {
                for child in items {
                    if matches!(
                        child,
                        serde_json::Value::Object(_) | serde_json::Value::Array(_)
                    ) {
                        sort(child);
                    }
                }
            }
            _ => {}
        }
    }
}
pub fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, Error> {
    let mut value = serde_json::to_value(value)?;
    sort_canonical(&mut value);
    Ok(serde_json::to_vec(&value)?)
}
pub fn content_hash<T: Serialize>(value: &T) -> Result<String, Error> {
    content_hash_value(serde_json::to_value(value)?)
}
/// `content_hash` of an owned JSON value without re-encoding it first; the
/// canonical bytes, and therefore the hash, are identical.
pub fn content_hash_value(mut value: serde_json::Value) -> Result<String, Error> {
    sort_canonical(&mut value);
    let mut hasher = Sha256::new();
    serde_json::to_writer(&mut HashWriter(&mut hasher), &value)?;
    Ok(format!("{:x}", hasher.finalize()))
}
struct HashWriter<'a>(&'a mut Sha256);
impl std::io::Write for HashWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod failure_cause_tests {
    use super::*;

    fn frozen_fixture() -> serde_json::Value {
        let limits = microcrab_session::Limits::default();
        let context = microcrab_session::context::Context::new(
            "frozen system".into(),
            vec![],
            limits.max_context_bytes,
        )
        .unwrap();
        let checkpoint = CreatorEnvelope {
            session_id: "historical-policy".into(),
            recover: Recover {
                state: microcrab_session::SessionState::new(limits),
                context: context.snapshot().unwrap(),
                continuation: None,
                workspace: None,
                cache_report: None,
                agent: None,
                prompt_config: None,
            },
            request_head: None,
            model_selection: None,
            route_witness: None,
            navigation_boundary: None,
        };
        let mut fixture = serde_json::to_value(HistoryNode::new(None, checkpoint)).unwrap();
        // Explicit null and absent defaulted fields must not normalize into one
        // another during old-node validation.
        fixture["checkpoint"]["model_selection"] = serde_json::Value::Null;
        fixture["checkpoint"]["route_witness"] = serde_json::Value::Null;
        fixture["checkpoint"]["recover"]["cache_report"] = serde_json::Value::Null;
        fixture["checkpoint"]["recover"]["agent"] = serde_json::json!({
            "name": "coding", "tools": [], "variables": {},
            "modes": [{ "name": "plan", "role": "both", "permissions": [{
                "agent": null, "mode": null, "tool": null,
                "capability": "tool_invoke", "resource": null,
                "arguments": [], "operation_arguments": [], "conditions": [],
                "action": {"conditional": {"then": "allow", "else": "deny"}}
            }]}]
        });
        fixture
    }

    #[test]
    fn frozen_legacy_agent_round_trips_without_becoming_live_authority() {
        let fixture = frozen_fixture();
        let id = content_hash(&fixture).unwrap();
        let original = canonical_bytes(&fixture).unwrap();
        let decoded: HistoryNode = serde_json::from_slice(&original).unwrap();
        decoded.validate(&id).unwrap();
        assert_eq!(decoded.id().unwrap(), id);
        assert_eq!(canonical_bytes(&decoded).unwrap(), original);
        assert!(
            decoded
                .checkpoint
                .recover
                .agent
                .as_ref()
                .unwrap()
                .current()
                .is_err()
        );
        let copied: HistoryNode =
            serde_json::from_slice(&canonical_bytes(&decoded.clone()).unwrap()).unwrap();
        assert_eq!(copied.id().unwrap(), id);
    }

    #[test]
    fn every_legacy_rule_field_remains_evidence_and_blocks_activation_even_when_null() {
        for field in [
            "capability",
            "resource",
            "arguments",
            "operation_arguments",
            "conditions",
        ] {
            let mut fixture = frozen_fixture();
            fixture["checkpoint"]["recover"]["agent"]["modes"][0]["permissions"][0] =
                serde_json::json!({"tool": null, "action": "allow"});
            fixture["checkpoint"]["recover"]["agent"]["modes"][0]["permissions"][0][field] =
                serde_json::Value::Null;
            let id = content_hash(&fixture).unwrap();
            let decoded: HistoryNode = serde_json::from_value(fixture.clone()).unwrap();
            decoded.validate(&id).unwrap();
            assert_eq!(serde_json::to_value(&decoded).unwrap(), fixture);
            let agent = decoded.checkpoint.recover.agent.as_ref().unwrap();
            assert_eq!(agent.name(), Some("coding"));
            let error = agent.current().unwrap_err().to_string();
            assert!(error.contains(field));
            assert!(error.contains("agent.modes[0].permissions[0]"));
        }
        let mut fixture = frozen_fixture();
        fixture["checkpoint"]["recover"]["agent"]["modes"][0]["permissions"][0] =
            serde_json::json!({"action": {"conditional": {"then": "allow", "else": "deny"}}});
        let id = content_hash(&fixture).unwrap();
        let decoded: HistoryNode = serde_json::from_value(fixture).unwrap();
        decoded.validate(&id).unwrap();
        assert!(
            decoded
                .checkpoint
                .recover
                .agent
                .as_ref()
                .unwrap()
                .current()
                .is_err()
        );
    }

    #[test]
    fn frozen_node_mutation_never_retains_original_hash_witness() {
        let fixture = frozen_fixture();
        let id = content_hash(&fixture).unwrap();
        let mut decoded: HistoryNode = serde_json::from_value(fixture).unwrap();
        decoded.checkpoint.request_head = Some("a".repeat(64));
        assert_ne!(decoded.id().unwrap(), id);
        assert!(decoded.validate(&id).is_err());
    }
    #[test]
    fn rejected_history_identifier_names_rule_without_echoing_identifier() {
        let error = valid_hash("private-storage-secret").unwrap_err();
        assert_eq!(
            error.public_reason(),
            Some("invalid SHA256 history identifier")
        );
        assert!(!format!("{error:?}").contains("private-storage-secret"));
    }
}
