//! Root model ownership, separated from browser catalog generations.
use super::*;
use crate::runtime::{
    journal::Recover,
    tool::{Tool, ToolRegistry},
};

impl SessionInner {
    pub(super) async fn prepare_model_admission(
        &self,
    ) -> Result<Option<crate::PreparedModelAdmission>, Error> {
        let Some(owner) = &self.model_admission else {
            return Ok(None);
        };
        let prepared = owner.prepare().await?;
        if prepared.dialogue_reply.as_ref().is_some_and(|effect| {
            effect.admission_identity() != owner.dialogue_reply().admission_identity()
        }) {
            return Err(Error::IncompatibleModelCatalog);
        }
        Ok(Some(prepared))
    }

    pub(super) fn model_registry(
        &self,
        prepared: &crate::PreparedModelAdmission,
        external: Option<Vec<Arc<dyn Tool>>>,
    ) -> Result<Arc<ToolRegistry>, Error> {
        let owner = self
            .model_admission
            .as_ref()
            .expect("prepared embedding admission");
        self.runtime.prepare_model_catalog(
            owner.as_ref(),
            prepared.dialogue_reply.as_ref(),
            external,
        )
    }

    /// Prepare only the managed effect delta; unrelated exact frozen definitions
    /// (including compatible legacy builtin schemas) remain untouched.
    pub(super) fn model_catalog_candidate(
        &self,
        context: &mut Context,
        prepared: &crate::PreparedModelAdmission,
        has_continuation: bool,
    ) -> Result<bool, Error> {
        let owner = self
            .model_admission
            .as_ref()
            .expect("prepared embedding admission");
        let canonical = owner.dialogue_reply().definition();
        let current = context.frozen_tools();
        if current.iter().any(|tool| {
            owner.retired_tool_names().contains(&tool.name.as_str())
                || (tool.name == canonical.name && tool != canonical)
        }) {
            return Err(Error::IncompatibleModelCatalog);
        }
        let enabled = prepared.dialogue_reply.is_some();
        if current.iter().any(|t| t.name == canonical.name) == enabled {
            return Ok(false);
        }
        if has_continuation {
            return Err(Error::ModelCatalogContinuationMismatch);
        }
        let mut tools = current
            .iter()
            .filter(|tool| tool.name != canonical.name)
            .cloned()
            .collect::<Vec<_>>();
        if enabled {
            let mut canonical = canonical.clone();
            canonical.parameters.sort_all_objects();
            tools.push(canonical);
        }
        context.replace_tools_for_recovery(tools)?;
        Ok(true)
    }

    /// An inherited one-shot request keeps historical call evidence but never
    /// carries a root-only effect registration into its independent catalog.
    pub(in crate::inner) fn functional_issuer(
        &self,
        mut issuer: crate::context::Content,
    ) -> crate::context::Content {
        issuer.tools.retain(|tool| {
            self.runtime.origin(&tool.name) != Some(crate::runtime::ToolOrigin::DialogueReply)
                && self.model_admission.as_ref().is_none_or(|owner| {
                    tool.name != owner.dialogue_reply().definition().name
                        && !owner.retired_tool_names().contains(&tool.name.as_str())
                })
        });
        issuer
    }

    pub(super) async fn prepare_restore_model(
        &self,
        recover: &Recover,
    ) -> Result<Option<crate::PreparedModelAdmission>, Error> {
        if let Some(owner) = &self.model_admission {
            crate::admission::model::reject_retired(owner.as_ref(), recover)?;
        }
        self.prepare_model_admission().await
    }

    pub(super) async fn publish_model_catalog(
        &self,
        prepared: &crate::PreparedModelAdmission,
    ) -> Result<(), Error> {
        let mut context = self
            .context
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if self.model_catalog_candidate(&mut context, prepared, self.has_continuation())? {
            let registry = self.model_registry(prepared, None)?;
            if let Err(error) = self
                .settle_live_external_dialogues(
                    crate::state::communication::reply::DialogueTerminalReason::Interrupted,
                )
                .await
            {
                self.faulted.store(true, Ordering::Release);
                return Err(error);
            }
            self.apply_catalog(registry, context).await?;
        }
        Ok(())
    }

    /// This uses the same serialized reservation as model work, but never claims
    /// inbox entries or runs initialization. A dropped waiter owns no cancellation
    /// capability: the detached task retains the checkpoint and witness.
    pub(crate) async fn reconcile_model_catalog(self: &Arc<Self>) -> Result<(), Error> {
        let reservation = self.reserve_model().await?;
        if reservation.model_admission.is_none() {
            return Ok(());
        }
        let inner = self.clone();
        tokio::spawn(async move {
            if let Some(prepared) = &reservation.model_admission {
                inner.publish_model_catalog(prepared).await?;
            }
            inner.resume_model_admission_inbox();
            // Capture the full reservation (Rust otherwise captures just the
            // witness field); do not release Session guards before the ACK.
            drop(reservation);
            Ok(())
        })
        .await
        .map_err(|_| Error::lifecycle("model catalog publication worker stopped unexpectedly"))?
    }
}
