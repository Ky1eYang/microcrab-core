//! Runtime persistence; creators own storage, identity, and history selection.
mod memory;
mod types;

use crate::Error;
use async_trait::async_trait;

pub use memory::MemoryJournal;
pub use types::{
    AgentInitRecord, AgentInitStatus, HistoricalAgent, RecordedCachePoint, Recover,
    RoundCacheReport, RoundContinuation,
};

/// Session-certified navigation provenance, separate from internal recovery commits.
/// Creator journals persist this beside the immutable recovery value; runtime-only
/// journals may retain their existing latest-value behavior.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NavigationBoundary {
    Internal,
    EpochInitial,
    InterTurn,
}

/// Runtime-only proof that an admission recovery adds synthetic receipts after
/// an exact live Context prefix. It is never serialized into historical nodes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionCheckpoint {
    pub admitted_prefix_len: usize,
    pub admitted_prefix_hash: String,
    pub pending: Vec<AdmissionInvocation>,
}

/// A pending invocation is bound to its actual Assistant/call slot and the hash
/// of that entire admitted prefix, rather than inferred from receipt content.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionInvocation {
    pub assistant_index: usize,
    pub call_index: usize,
    pub call_id: String,
}

impl AdmissionCheckpoint {
    pub fn from_context(context: &crate::context::Context) -> Result<Self, Error> {
        let items = context.items();
        let pending = Self::pending(items)?;
        if pending
            .iter()
            .map(|call| call.call_id.clone())
            .collect::<Vec<_>>()
            != context.pending_call_ids()
        {
            return Err(Error::protocol(
                "admission prefix pending invocation mismatch",
            ));
        }
        Ok(Self {
            admitted_prefix_len: items.len(),
            admitted_prefix_hash: Self::hash(items)?,
            pending,
        })
    }

    /// Validate the complete receipt suffix, never merely a caller-supplied
    /// prefix length. The journal separately checks its previously certified
    /// immutable prefix before accepting this proof as display continuity.
    pub fn validate(&self, recover: &Recover) -> Result<usize, Error> {
        use crate::context::ContentItem;
        let items = &recover.context.items;
        let Some(prefix) = items.get(..self.admitted_prefix_len) else {
            return Err(Error::protocol("admission prefix exceeds recovery items"));
        };
        if Self::hash(prefix)? != self.admitted_prefix_hash {
            return Err(Error::protocol("admission prefix digest mismatch"));
        }
        if Self::pending(prefix)? != self.pending || self.pending.is_empty() {
            return Err(Error::protocol(
                "admission prefix pending invocation mismatch",
            ));
        }
        let suffix = &items[self.admitted_prefix_len..];
        if suffix.len() != self.pending.len()
            || !suffix.iter().zip(&self.pending).all(|(item, invocation)| {
                matches!(item, ContentItem::Tool { call_id, .. } if call_id == &invocation.call_id)
            })
        {
            return Err(Error::protocol("admission recovery suffix is not the exact pending invocation set"));
        }
        Ok(self.admitted_prefix_len)
    }

    fn hash(items: &[crate::context::ContentItem]) -> Result<String, Error> {
        use sha2::{Digest, Sha256};
        let mut value = serde_json::to_value(items)?;
        value.sort_all_objects();
        Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(&value)?)))
    }

    fn pending(items: &[crate::context::ContentItem]) -> Result<Vec<AdmissionInvocation>, Error> {
        use crate::context::ContentItem;
        let mut pending = std::collections::BTreeMap::new();
        for (assistant_index, item) in items.iter().enumerate() {
            match item {
                ContentItem::Assistant { tool_calls, .. } => {
                    if !pending.is_empty() {
                        return Err(Error::protocol(
                            "admission prefix crosses unsettled Assistant calls",
                        ));
                    }
                    for (call_index, call) in tool_calls.iter().enumerate() {
                        if pending
                            .insert(
                                call.id.clone(),
                                AdmissionInvocation {
                                    assistant_index,
                                    call_index,
                                    call_id: call.id.clone(),
                                },
                            )
                            .is_some()
                        {
                            return Err(Error::protocol(
                                "admission prefix has duplicate pending calls",
                            ));
                        }
                    }
                }
                ContentItem::Tool { call_id, .. } => {
                    if pending.remove(call_id).is_none() {
                        return Err(Error::protocol(
                            "admission prefix has an unmatched tool result",
                        ));
                    }
                }
                _ if !pending.is_empty() => {
                    return Err(Error::protocol(
                        "admission prefix interrupts pending invocations",
                    ));
                }
                _ => {}
            }
        }
        Ok(pending.into_values().collect())
    }
}

/// Object-safe persistence for a settled runtime recovery value.
/// `recovered` publishes a creator's staged conversation selection; cache points
/// and legacy workspace bytes never authorize filesystem restoration.
#[async_trait]
pub trait Journal: Send + Sync + 'static {
    fn supports_communication(&self) -> bool {
        false
    }
    /// Commit a bounded inbox sidecar for this exact communication identity.
    /// Identical retries may observe the same revision; changed writes require
    /// the next revision. Unsupported implementations never acknowledge success.
    async fn commit_communication(
        &self,
        _agent_id: &crate::state::communication::AgentId,
        _inbox: crate::state::communication::inbox::DurableInbox,
    ) -> Result<(), Error> {
        Err(Error::config(
            "Journal does not support durable communication inboxes",
        ))
    }
    async fn load_communication(
        &self,
        _agent_id: &crate::state::communication::AgentId,
    ) -> Result<Option<crate::state::communication::inbox::DurableInbox>, Error> {
        Err(Error::config(
            "Journal does not support durable communication inboxes",
        ))
    }
    async fn commit(&self, recover: Recover) -> Result<(), Error>;

    /// Persist admission recovery without promoting its synthetic receipt suffix
    /// to immutable display evidence. Unaware journals keep their raw behavior.
    async fn commit_admission(
        &self,
        recover: Recover,
        _admission: AdmissionCheckpoint,
    ) -> Result<(), Error> {
        self.commit(recover).await
    }

    /// Publish a Session-certified conversation boundary. Internal admission,
    /// round and execution recovery continue to use `commit`.
    async fn commit_boundary(
        &self,
        recover: Recover,
        _boundary: NavigationBoundary,
    ) -> Result<(), Error> {
        self.commit(recover).await
    }

    /// Publish a recovery selection before Session installs its runtime state.
    /// Creator journals verify a prepared target and atomically move their head;
    /// the default accepts recovery without maintaining a history selection.
    async fn recovered(&self, _recover: Recover) -> Result<(), Error> {
        Ok(())
    }

    /// Whether attachment reuses an existing selected history rather than a
    /// fresh destination (for example a fork). No checkpoint identity is exposed.
    async fn has_selected_history(&self) -> bool {
        false
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}
