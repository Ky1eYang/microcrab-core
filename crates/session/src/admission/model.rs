//! Trusted embedding decision at a root model-admission boundary.
use crate::runtime::DialogueReplyRegistration;
use crate::{Error, Recover};
use async_trait::async_trait;

/// Session-scoped catalog owner. Installed only on persistent Interactive roots.
///
/// `prepare` runs with Session admission reserved. It may briefly acquire an
/// embedding fence to read authoritative state and publish its in-flight marker.
/// A settings writer holding that fence must never wait for Session operations.
/// The returned witness owns the marker, **not** the short-lived fence.
#[async_trait]
pub trait ModelAdmission: Send + Sync {
    /// Stable registration for this Session binding, even while not published.
    /// Return a clone of this exact instance from `prepare` when publishing it.
    fn dialogue_reply(&self) -> &DialogueReplyRegistration;

    /// Names excluded from this embedding's live catalogs; never legacy handlers.
    fn retired_tool_names(&self) -> &[&str] {
        &[]
    }

    async fn prepare(&self) -> Result<PreparedModelAdmission, Error>;
}

/// Opaque, non-cloneable admission witness and exact desired native effect.
/// Core retains it across journal acknowledgement, provider work and settlement.
pub struct PreparedModelAdmission {
    _witness: Box<dyn Send + Sync>,
    pub(crate) dialogue_reply: Option<DialogueReplyRegistration>,
}

impl PreparedModelAdmission {
    pub fn new(
        witness: Box<dyn Send + Sync>,
        dialogue_reply: Option<DialogueReplyRegistration>,
    ) -> Self {
        Self {
            _witness: witness,
            dialogue_reply,
        }
    }
}

pub(crate) fn reject_retired(owner: &dyn ModelAdmission, recover: &Recover) -> Result<(), Error> {
    if recover
        .context
        .tools
        .iter()
        .any(|tool| owner.retired_tool_names().contains(&tool.name.as_str()))
    {
        return Err(Error::IncompatibleModelCatalog);
    }
    Ok(())
}
