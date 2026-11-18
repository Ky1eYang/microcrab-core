//! Resolve one user-input boundary on the selected immutable ancestry. Preparation
//! holds the selection lock but never publishes; Session recovery owns publication.
use super::*;
use microcrab_session::context::{ContentItem, ContextSnapshot};

const MAX_UNDO_BYTES: usize = 64 * 1024 * 1024;

impl CreatorJournal {
    /// Resolve an exact persisted input index. Existing callers retain this port;
    /// callers holding live Context should use the fully anchored mapping variant.
    pub async fn prepare_undo_input(
        &self,
        expected: JournalCursor,
        index: usize,
        input: &ContentItem,
    ) -> Result<Recover, Error> {
        self.prepare_undo_input_checked(expected, None, index, input)
            .await
    }

    /// Stage a whole-input Undo after proving the live conversation corresponds to
    /// the expected immutable source. Raw State indices can change during recovery;
    /// exact ordered semantic anchors supply the input ordinal and identity.
    pub async fn prepare_undo_input_from_context(
        &self,
        expected: JournalCursor,
        live: &ContextSnapshot,
        index: usize,
        input: &ContentItem,
    ) -> Result<Recover, Error> {
        self.prepare_undo_input_checked(expected, Some(live), index, input)
            .await
    }

    async fn prepare_undo_input_checked(
        &self,
        expected: JournalCursor,
        live: Option<&ContextSnapshot>,
        index: usize,
        input: &ContentItem,
    ) -> Result<Recover, Error> {
        let mut selection = self.selection.lock().await;
        if selection.pending.is_some() {
            return Err(Error::Busy);
        }
        if selection.head.as_ref() != Some(&expected) {
            return Err(Error::Protocol("journal cursor conflict".into()));
        }
        let source = self
            .backend
            .restore(RestoreTarget::Checkpoint {
                checkpoint_id: expected.checkpoint_id.clone(),
            })
            .await?;
        let source_context = &source.checkpoint.recover.context;
        let mut bytes = serde_json::to_vec(&source.checkpoint.recover)?.len();
        if bytes > MAX_UNDO_BYTES {
            return Err(Error::Limit("undo input ancestry byte budget".into()));
        }
        if let Some(live) = live
            && serde_json::to_vec(live)?.len() > MAX_UNDO_BYTES
        {
            return Err(Error::Limit("undo live Context byte budget".into()));
        }
        let index =
            source_context.undo_input_index(live.unwrap_or(source_context), index, input)?;
        let mut next = source.parent;
        for _ in 0..1024 {
            let Some(id) = next else {
                return Err(Error::config(
                    "undo input boundary is unavailable: no certified same-epoch ancestor",
                ));
            };
            let point = self
                .backend
                .restore(RestoreTarget::Checkpoint { checkpoint_id: id })
                .await?;
            let recover = point.checkpoint.recover;
            bytes = bytes.saturating_add(serde_json::to_vec(&recover)?.len());
            if bytes > MAX_UNDO_BYTES {
                return Err(Error::Limit("undo input ancestry byte budget".into()));
            }
            // A repeated summary may reuse its marker. Generation and retired-call
            // provenance form an independent barrier and must stop traversal.
            if !recover.context.same_undo_baseline(source_context) {
                return Err(Error::config("undo input crosses a compaction baseline"));
            }
            if !matches!(
                point.checkpoint.navigation_boundary,
                Some(microcrab_session::NavigationBoundary::Internal)
            ) && recover.continuation.is_none()
                && recover
                    .context
                    .undo_boundary_matches(source_context, index)?
            {
                let recover_hash = content_hash(&recover)?;
                selection.pending = Some(Pending {
                    expected,
                    via: Travel::UndoTo {
                        checkpoint_id: point.cursor.checkpoint_id.clone(),
                    },
                    target: point.cursor,
                    request_head: point.checkpoint.request_head,
                    model_selection: point.checkpoint.model_selection,
                    route_witness: point.checkpoint.route_witness,
                    recover_hash,
                });
                return Ok(recover);
            }
            next = point.parent;
        }
        Err(Error::Limit("undo input ancestry budget".into()))
    }
}
