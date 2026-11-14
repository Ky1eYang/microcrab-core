//! Creator history serializes publication and stages conversation head selection until recovery commits it.
#[cfg(test)]
mod commit_observer_tests;
#[cfg(feature = "storage-file")]
mod disk;
#[cfg(all(test, any(feature = "storage-file", feature = "storage-sqlite")))]
#[path = "history/gateway_tests.rs"]
mod gateway_tests;
#[cfg(all(test, any(feature = "storage-file", feature = "storage-sqlite")))]
#[path = "history/google_replay_tests.rs"]
mod google_replay_tests;
mod memory;
#[cfg(feature = "storage-sqlite")]
mod sqlite;
#[cfg(all(test, feature = "storage-sqlite"))]
#[path = "history/sqlite_tests.rs"]
mod sqlite_tests;
#[cfg(all(test, feature = "storage-file"))]
#[path = "history/history_tests.rs"]
mod tests;
mod types;
mod undo;
#[cfg(test)]
mod undo_tests;

#[cfg(feature = "storage-file")]
pub use disk::FileJournal;
pub use memory::MemoryJournal;
use microcrab_session::runtime::journal::AdmissionCheckpoint;
use microcrab_session::{Error, Journal, Recover};
#[cfg(feature = "storage-sqlite")]
pub use sqlite::SqliteJournal;
use std::{future::Future, sync::Arc};
use tokio::sync::Mutex;
pub use types::*;

pub trait History: Send + Sync {
    fn close(&self) -> impl Future<Output = Result<(), Error>> + Send {
        async { Ok(()) }
    }
    fn commit(
        &self,
        expected: Option<JournalCursor>,
        checkpoint: CreatorEnvelope,
    ) -> impl Future<Output = Result<JournalCursor, Error>> + Send;
    fn restore(
        &self,
        target: RestoreTarget,
    ) -> impl Future<Output = Result<RestorePoint, Error>> + Send;
    fn travel(
        &self,
        expected: JournalCursor,
        direction: Travel,
    ) -> impl Future<Output = Result<RestorePoint, Error>> + Send;
    fn move_head(
        &self,
        expected: JournalCursor,
        target: JournalCursor,
        via: Travel,
    ) -> impl Future<Output = Result<JournalCursor, Error>> + Send;
}

enum Backend {
    Memory(MemoryJournal),
    #[cfg(feature = "storage-file")]
    Disk(FileJournal),
    #[cfg(feature = "storage-sqlite")]
    Sqlite(SqliteJournal),
}
impl History for Backend {
    async fn close(&self) -> Result<(), Error> {
        match self {
            Self::Memory(x) => x.close().await,
            #[cfg(feature = "storage-file")]
            Self::Disk(x) => x.close().await,
            #[cfg(feature = "storage-sqlite")]
            Self::Sqlite(x) => x.close().await,
        }
    }
    async fn commit(
        &self,
        expected: Option<JournalCursor>,
        checkpoint: CreatorEnvelope,
    ) -> Result<JournalCursor, Error> {
        match self {
            Self::Memory(x) => x.commit(expected, checkpoint).await,
            #[cfg(feature = "storage-file")]
            Self::Disk(x) => x.commit(expected, checkpoint).await,
            #[cfg(feature = "storage-sqlite")]
            Self::Sqlite(x) => x.commit(expected, checkpoint).await,
        }
    }
    async fn restore(&self, target: RestoreTarget) -> Result<RestorePoint, Error> {
        match self {
            Self::Memory(x) => x.restore(target).await,
            #[cfg(feature = "storage-file")]
            Self::Disk(x) => x.restore(target).await,
            #[cfg(feature = "storage-sqlite")]
            Self::Sqlite(x) => x.restore(target).await,
        }
    }
    async fn travel(
        &self,
        expected: JournalCursor,
        direction: Travel,
    ) -> Result<RestorePoint, Error> {
        match self {
            Self::Memory(x) => x.travel(expected, direction).await,
            #[cfg(feature = "storage-file")]
            Self::Disk(x) => x.travel(expected, direction).await,
            #[cfg(feature = "storage-sqlite")]
            Self::Sqlite(x) => x.travel(expected, direction).await,
        }
    }
    async fn move_head(
        &self,
        expected: JournalCursor,
        target: JournalCursor,
        via: Travel,
    ) -> Result<JournalCursor, Error> {
        match self {
            Self::Memory(x) => x.move_head(expected, target, via).await,
            #[cfg(feature = "storage-file")]
            Self::Disk(x) => x.move_head(expected, target, via).await,
            #[cfg(feature = "storage-sqlite")]
            Self::Sqlite(x) => x.move_head(expected, target, via).await,
        }
    }
}
struct Pending {
    expected: JournalCursor,
    target: JournalCursor,
    via: Travel,
    request_head: Option<String>,
    recover_hash: String,
    model_selection: Option<crate::directory::ModelSelection>,
    route_witness: Option<String>,
}
struct Selection {
    model_selection: Option<crate::directory::ModelSelection>,
    route_witness: Option<String>,
    prepared_model_selection: Option<crate::directory::ModelSelection>,
    prepared_route_witness: Option<Option<String>>,
    head: Option<JournalCursor>,
    pending: Option<Pending>,
    // Changes only with acknowledged history publication, never preview events.
    availability: Option<HistoryAvailable>,
    // Derived prefix, never a history authority. Normal commits only extend the
    // selected immutable parent chain; recovered() invalidates on every travel,
    // including To(current), so equal marker IDs on siblings cannot alias.
    compacted: Option<(String, Vec<CreatorEnvelope>)>,
    // Runtime-only continuity certificate. Commits retain it; selection changes
    // invalidate it even when travel selects the same content-addressed head.
    lineage: String,
    display_epoch: Option<u64>,
    display_baseline_generation: Option<u64>,
    display_marker: Option<String>,
    display_prefix: Option<DisplayPrefix>,
    display_admission: Option<AdmissionCheckpoint>,
}
/// Display continuity witness of the selected items. A restored head hashes its
/// items off the open path; the next commit resolves the result.
enum DisplayPrefix {
    Ready(usize, String),
    Pending(usize, tokio::task::JoinHandle<Result<String, Error>>),
}
/// Secondary projections observe an acknowledged new checkpoint before the next
/// commit or producer proceeds. Implementations must not reenter this Journal;
/// projection failure is reported by the observer and cannot undo durable history.
#[async_trait::async_trait]
pub trait CommitObserver: Send + Sync {
    async fn committed(&self, cursor: &JournalCursor);
}

#[derive(Clone)]
pub struct CreatorJournal {
    backend: Arc<Backend>,
    session_id: String,
    selection: Arc<Mutex<Selection>>,
    anchor: Arc<std::sync::Mutex<AuditAnchor>>,
    commit_observer: Arc<std::sync::Mutex<Option<Arc<dyn CommitObserver>>>>,
}
impl CreatorJournal {
    pub fn install_commit_observer(&self, observer: Arc<dyn CommitObserver>) -> Result<(), Error> {
        let mut installed = self
            .commit_observer
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if installed.is_some() {
            return Err(Error::config("journal commit observer already installed"));
        }
        *installed = Some(observer);
        Ok(())
    }
    pub async fn open(
        directory: &str,
        session_id: String,
        disk: bool,
        restore: bool,
    ) -> Result<(Self, Option<Recover>), Error> {
        let backend = if disk {
            #[cfg(feature = "storage-file")]
            {
                Backend::Disk(FileJournal::open(directory).await?)
            }
            #[cfg(not(feature = "storage-file"))]
            {
                let _ = directory;
                return Err(Error::config("file storage is not compiled"));
            }
        } else {
            Backend::Memory(MemoryJournal::default())
        };
        Self::from_backend(backend, session_id, restore).await
    }
    #[cfg(feature = "storage-sqlite")]
    pub async fn open_sqlite(
        database: impl AsRef<std::path::Path>,
        session_id: String,
        restore: bool,
    ) -> Result<(Self, Option<Recover>), Error> {
        let backend = Backend::Sqlite(SqliteJournal::open(database, session_id.clone()).await?);
        Self::from_backend(backend, session_id, restore).await
    }
    async fn from_backend(
        backend: Backend,
        session_id: String,
        restore: bool,
    ) -> Result<(Self, Option<Recover>), Error> {
        let point = if restore {
            Some(backend.restore(RestoreTarget::Head).await?)
        } else {
            None
        };
        if point
            .as_ref()
            .is_some_and(|point| point.cursor.session_id != session_id)
        {
            return Err(Error::config(
                "session directory belongs to another creator id",
            ));
        }
        let model_selection = point
            .as_ref()
            .and_then(|point| point.checkpoint.model_selection.clone());
        let route_witness = point
            .as_ref()
            .and_then(|point| point.checkpoint.route_witness.clone());
        let (point, recover) = match point {
            Some(point) => {
                let cursor = point.cursor;
                let request_head = point.checkpoint.request_head;
                (Some((cursor, request_head)), Some(point.checkpoint.recover))
            }
            None => (None, None),
        };
        let lineage = uuid::Uuid::new_v4().to_string();
        let anchor = Arc::new(std::sync::Mutex::new(AuditAnchor {
            display_lineage: lineage.clone(),
            history_head: point
                .as_ref()
                .map(|(cursor, _)| cursor.checkpoint_id.clone()),
            request_head: point
                .as_ref()
                .and_then(|(_, request_head)| request_head.clone()),
        }));
        Ok((
            Self {
                backend: Arc::new(backend),
                session_id,
                anchor,
                commit_observer: Arc::new(std::sync::Mutex::new(None)),
                selection: Arc::new(Mutex::new(Selection {
                    model_selection,
                    route_witness,
                    prepared_model_selection: None,
                    prepared_route_witness: None,
                    availability: if point.is_none() {
                        Some(HistoryAvailable {
                            undo: false,
                            redo: false,
                        })
                    } else {
                        None
                    },
                    lineage,
                    display_epoch: recover.as_ref().map(|recover| recover.state.epoch_id),
                    display_baseline_generation: recover
                        .as_ref()
                        .map(|recover| recover.context.baseline_generation),
                    display_marker: recover.as_ref().and_then(|recover| {
                        recover
                            .context
                            .compaction
                            .as_ref()
                            .map(|marker| marker.id.clone())
                    }),
                    display_admission: None,
                    display_prefix: recover.as_ref().map(|recover| {
                        let items = recover.context.items.clone();
                        DisplayPrefix::Pending(
                            items.len(),
                            tokio::task::spawn_blocking(move || content_hash(&items)),
                        )
                    }),
                    head: point.map(|(cursor, _)| cursor),
                    pending: None,
                    compacted: None,
                })),
            },
            recover,
        ))
    }
    /// Probe selected ancestry and retained redo without staging or moving travel.
    /// Successful results are cached until acknowledged history publication changes
    /// their meaning. First restored/traveled selection probes remain bounded by the
    /// existing checkpoint size limit; corrupt/inaccessible history remains an error.
    pub async fn model_selection(&self) -> Option<crate::directory::ModelSelection> {
        self.selection.lock().await.model_selection.clone()
    }
    /// The caller owns Session admission. This stages no durable mutation; the
    /// next checkpoint includes the route and Context in one immutable envelope.
    pub async fn prepare_model_route(
        &self,
        model: crate::directory::ModelSelection,
        witness: Option<String>,
    ) {
        let mut selection = self.selection.lock().await;
        selection.prepared_model_selection = Some(model);
        selection.prepared_route_witness = Some(witness);
    }
    /// Pending travel wins over the currently selected head. Neither path invents
    /// a witness for old history; callers explicitly rebuild an unwitnessed route.
    pub async fn restore_model_route(
        &self,
    ) -> Option<(
        JournalCursor,
        Option<crate::directory::ModelSelection>,
        Option<String>,
    )> {
        let selection = self.selection.lock().await;
        if let Some(pending) = &selection.pending {
            Some((
                pending.target.clone(),
                pending.model_selection.clone(),
                pending.route_witness.clone(),
            ))
        } else {
            selection.head.clone().map(|head| {
                (
                    head,
                    selection.model_selection.clone(),
                    selection.route_witness.clone(),
                )
            })
        }
    }
    pub async fn history_available(&self) -> Result<HistoryAvailable, Error> {
        let mut selection = self.selection.lock().await;
        if let Some(availability) = selection.availability {
            return Ok(availability);
        }
        let Some(head) = selection.head.as_ref() else {
            return Ok(HistoryAvailable {
                undo: false,
                redo: false,
            });
        };
        async fn available(
            backend: &Backend,
            head: &JournalCursor,
            via: Travel,
        ) -> Result<bool, Error> {
            match backend.travel(head.clone(), via).await {
                Ok(_) => Ok(true),
                Err(Error::Config(message))
                    if message.as_str() == "journal target does not exist" =>
                {
                    Ok(false)
                }
                Err(error) => Err(error),
            }
        }
        let undo = match self.checkpoint_undo_point(head).await {
            Ok(_) => true,
            Err(Error::Config(message)) if message.as_str() == "journal target does not exist" => {
                false
            }
            Err(error) => return Err(error),
        };
        let availability = HistoryAvailable {
            undo,
            redo: available(self.backend.as_ref(), head, Travel::Redo).await?,
        };
        selection.availability = Some(availability);
        Ok(availability)
    }
    pub fn history_anchor(&self) -> Arc<std::sync::Mutex<AuditAnchor>> {
        self.anchor.clone()
    }
    fn acknowledge(&self, cursor: &JournalCursor) {
        // This projection is updated only after durable selection publication.
        self.anchor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .history_head = Some(cursor.checkpoint_id.clone());
    }
    /// Read the selected immutable checkpoint without restoring the workspace.
    /// Session activation remains a separate caller-owned operation.
    pub async fn head_recover(&self) -> Result<Option<Recover>, Error> {
        let selection = self.selection.lock().await;
        if selection.head.is_none() {
            return Ok(None);
        }
        Ok(Some(
            self.backend
                .restore(RestoreTarget::Head)
                .await?
                .checkpoint
                .recover,
        ))
    }
    /// Read the latest Context preceding each selected compaction. This is display
    /// history only: no restore/travel publication, Host IO or model Context edits.
    /// Work and retained bytes are bounded; unreadable ancestry fails explicitly.
    pub async fn compacted_history(
        &self,
        marker: Option<&microcrab_session::context::CompactionMarker>,
    ) -> Result<Vec<Recover>, Error> {
        Ok(self
            .compacted_history_with_evidence(marker)
            .await?
            .into_iter()
            .map(|checkpoint| checkpoint.recover)
            .collect())
    }
    /// The same selected compaction contexts with their immutable audit anchors.
    /// This shares traversal/cache/bounds with display history; no sibling lookup
    /// or new persistent representation is introduced.
    pub async fn compacted_history_with_evidence(
        &self,
        marker: Option<&microcrab_session::context::CompactionMarker>,
    ) -> Result<Vec<CreatorEnvelope>, Error> {
        let Some(marker) = marker else {
            return Ok(vec![]);
        };
        let mut selection = self.selection.lock().await;
        if let Some((id, history)) = &selection.compacted
            && id == &marker.id
        {
            return Ok(history.clone());
        }
        let Some(head) = selection.head.as_ref() else {
            return Ok(vec![]);
        };
        fn finish(
            selection: &mut Selection,
            marker: &str,
            mut history: Vec<CreatorEnvelope>,
        ) -> Vec<CreatorEnvelope> {
            history.reverse();
            selection.compacted = Some((marker.to_owned(), history.clone()));
            history
        }
        let mut next = Some(head.checkpoint_id.clone());
        let mut marker_id = marker.id.clone();
        let mut history = vec![];
        let mut bytes = 0usize;
        for _ in 0..1024 {
            let Some(id) = next else {
                return Ok(finish(&mut selection, &marker.id, history));
            };
            let point = self
                .backend
                .restore(RestoreTarget::Checkpoint { checkpoint_id: id })
                .await?;
            bytes = bytes.saturating_add(serde_json::to_vec(&point.checkpoint)?.len());
            if bytes > 64 * 1024 * 1024 {
                return Err(Error::Limit("compacted display history byte budget".into()));
            }
            next = point.parent;
            let previous = point.checkpoint.recover.context.compaction.as_ref();
            if previous.is_some_and(|previous| previous.id == marker_id) {
                continue;
            }
            let previous_id = previous.map(|previous| previous.id.clone());
            history.push(point.checkpoint);
            if let Some((cached_marker, cached)) = &selection.compacted
                && previous_id.as_ref() == Some(cached_marker)
            {
                bytes = bytes.saturating_add(serde_json::to_vec(cached)?.len());
                if bytes > 64 * 1024 * 1024 {
                    return Err(Error::Limit("compacted display history byte budget".into()));
                }
                let mut combined = cached.clone();
                history.reverse();
                combined.extend(history);
                selection.compacted = Some((marker.id.clone(), combined.clone()));
                return Ok(combined);
            }
            let Some(previous_id) = previous_id else {
                return Ok(finish(&mut selection, &marker.id, history));
            };
            marker_id = previous_id;
        }
        Err(Error::Limit(
            "compacted display history ancestry budget".into(),
        ))
    }
    /// Read audit anchors at the newest selected checkpoint for requested epochs.
    /// Parent traversal uses the existing verified backend and never moves HEAD.
    /// This optional usage evidence has independent ancestry/byte bounds; callers
    /// retain readable history and unknown usage when it cannot be resolved.
    pub async fn display_request_heads(
        &self,
        epochs: &[u64],
    ) -> Result<Vec<(u64, Option<String>)>, Error> {
        if epochs.len() > 1024 {
            return Err(Error::Limit("display audit epoch budget".into()));
        }
        let selection = self.selection.lock().await;
        let mut wanted: std::collections::BTreeSet<_> = epochs.iter().copied().collect();
        let mut next = selection
            .head
            .as_ref()
            .map(|head| head.checkpoint_id.clone());
        let mut seen = std::collections::HashSet::new();
        let mut heads = vec![];
        let mut bytes = 0usize;
        while !wanted.is_empty() {
            let Some(id) = next else { break };
            if !seen.insert(id.clone()) || seen.len() > 1024 {
                return Err(Error::Limit("display audit ancestry budget".into()));
            }
            let point = self
                .backend
                .restore(RestoreTarget::Checkpoint { checkpoint_id: id })
                .await?;
            bytes = bytes.saturating_add(serde_json::to_vec(&point.checkpoint)?.len());
            if bytes > 64 * 1024 * 1024 {
                return Err(Error::Limit("display audit ancestry byte budget".into()));
            }
            let epoch = point.checkpoint.recover.state.epoch_id;
            if wanted.remove(&epoch) {
                heads.push((epoch, point.checkpoint.request_head));
            }
            next = point.parent;
        }
        heads.extend(wanted.into_iter().map(|epoch| (epoch, None)));
        Ok(heads)
    }
    pub async fn display_witness(&self) -> DisplayWitness {
        let selection = self.selection.lock().await;
        DisplayWitness {
            lineage: selection.lineage.clone(),
            cursor: selection.head.clone(),
        }
    }
    /// A projection may omit synthetic receipts only with this exact selected
    /// cursor's runtime proof. Open/recovery never invents legacy provenance.
    pub async fn display_projection(
        &self,
        expected: &JournalCursor,
    ) -> Option<(DisplayWitness, Option<AdmissionCheckpoint>)> {
        let selection = self.selection.lock().await;
        if selection.head.as_ref() != Some(expected) {
            return None;
        }
        Some((
            DisplayWitness {
                lineage: selection.lineage.clone(),
                cursor: selection.head.clone(),
            },
            selection.display_admission.clone(),
        ))
    }
    pub async fn cursor(&self) -> Option<JournalCursor> {
        self.selection.lock().await.head.clone()
    }
    async fn checkpoint_undo_point(&self, expected: &JournalCursor) -> Result<RestorePoint, Error> {
        let mut point = self.backend.travel(expected.clone(), Travel::Undo).await?;
        let source = self
            .backend
            .restore(RestoreTarget::Checkpoint {
                checkpoint_id: expected.checkpoint_id.clone(),
            })
            .await?;
        if !matches!(
            source.checkpoint.navigation_boundary,
            Some(
                microcrab_session::NavigationBoundary::EpochInitial
                    | microcrab_session::NavigationBoundary::InterTurn
            )
        ) {
            return Ok(point);
        }
        let mut bytes = serde_json::to_vec(&source.checkpoint)?.len();
        for _ in 0..1024 {
            bytes = bytes.saturating_add(serde_json::to_vec(&point.checkpoint)?.len());
            if bytes > 64 * 1024 * 1024 {
                return Err(Error::Limit(
                    "checkpoint navigation ancestry byte budget".into(),
                ));
            }
            if point.checkpoint.navigation_boundary
                != Some(microcrab_session::NavigationBoundary::Internal)
            {
                return Ok(point);
            }
            let id = point
                .parent
                .clone()
                .ok_or_else(|| Error::config("journal target does not exist"))?;
            point = self
                .backend
                .restore(RestoreTarget::Checkpoint { checkpoint_id: id })
                .await?;
        }
        Err(Error::Limit("checkpoint navigation ancestry budget".into()))
    }

    /// Read and retain a proposed conversation selection without moving the head.
    /// Pass the unchanged value to Session recovery, whose Journal::recovered call
    /// verifies and commits it; cancel_pending abandons the proposal.
    pub async fn prepare_travel(&self, via: Travel) -> Result<Recover, Error> {
        let mut selection = self.selection.lock().await;
        if selection.pending.is_some() {
            return Err(Error::Busy);
        }
        let expected = selection
            .head
            .clone()
            .ok_or_else(|| Error::config("journal has no head"))?;
        let point = if via == Travel::Undo {
            self.checkpoint_undo_point(&expected).await?
        } else {
            self.backend.travel(expected.clone(), via.clone()).await?
        };
        let via = if via == Travel::Undo {
            Travel::UndoTo {
                checkpoint_id: point.cursor.checkpoint_id.clone(),
            }
        } else {
            via
        };
        let recover = point.checkpoint.recover;
        let recover_hash = content_hash(&recover)?;
        selection.pending = Some(Pending {
            expected,
            target: point.cursor,
            request_head: point.checkpoint.request_head,
            model_selection: point.checkpoint.model_selection,
            route_witness: point.checkpoint.route_witness,
            recover_hash,
            via,
        });
        Ok(recover)
    }
    pub async fn cancel_pending(&self) {
        self.selection.lock().await.pending = None;
    }
}
impl CreatorJournal {
    async fn commit_navigation(
        &self,
        recover: Recover,
        navigation_boundary: Option<microcrab_session::NavigationBoundary>,
        admission: Option<AdmissionCheckpoint>,
    ) -> Result<(), Error> {
        recover.validate()?;
        let admitted_len = match &admission {
            Some(proof) => proof.validate(&recover)?,
            None => recover.context.items.len(),
        };
        let journal = self.clone();
        tokio::spawn(async move {
            let mut selection = journal.selection.lock().await;
            if selection.pending.is_some() {
                return Err(Error::Busy);
            }
            let request_head = journal
                .anchor
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .request_head
                .clone();
            let display_epoch = recover.state.epoch_id;
            let display_baseline_generation = recover.context.baseline_generation;
            let display_prefix = (
                admitted_len,
                content_hash(&&recover.context.items[..admitted_len])?,
            );
            let previous = match selection.display_prefix.take() {
                Some(DisplayPrefix::Ready(length, hash)) => Some((length, hash)),
                Some(DisplayPrefix::Pending(length, hashing)) => Some((
                    length,
                    hashing
                        .await
                        .map_err(|_| Error::protocol("journal display prefix worker failed"))??,
                )),
                None => None,
            };
            selection.display_prefix = previous
                .clone()
                .map(|(length, hash)| DisplayPrefix::Ready(length, hash));
            let extends = match &previous {
                Some((length, hash)) if *length <= admitted_len => {
                    content_hash(&&recover.context.items[..*length])? == *hash
                }
                Some(_) => false,
                None => true,
            };
            let marker = recover
                .context
                .compaction
                .as_ref()
                .map(|marker| marker.id.clone());
            if admission.is_some()
                && selection.display_epoch == Some(display_epoch)
                && selection.display_baseline_generation == Some(display_baseline_generation)
                && selection.display_marker == marker
                && previous
                    .as_ref()
                    .is_some_and(|(length, _)| *length > admitted_len)
            {
                return Err(Error::protocol(
                    "admission prefix retracts previously certified display items",
                ));
            }
            let model_selection = selection
                .prepared_model_selection
                .clone()
                .or_else(|| selection.model_selection.clone());
            let route_witness = selection
                .prepared_route_witness
                .clone()
                .unwrap_or_else(|| selection.route_witness.clone());
            let mut checkpoint = CreatorEnvelope {
                session_id: journal.session_id.clone(),
                recover,
                request_head,
                model_selection: model_selection.clone(),
                route_witness: route_witness.clone(),
                navigation_boundary,
            };
            if navigation_boundary == Some(microcrab_session::NavigationBoundary::Internal)
                && let Some(head) = &selection.head
            {
                let previous = journal
                    .backend
                    .restore(RestoreTarget::Checkpoint {
                        checkpoint_id: head.checkpoint_id.clone(),
                    })
                    .await?
                    .checkpoint;
                // An observation/close flush cannot demote a certified boundary or
                // manufacture a branch solely by changing its navigation label.
                // Every recovery byte, request head and route witness still match.
                checkpoint.navigation_boundary = previous.navigation_boundary;
                if canonical_bytes(&checkpoint)? != canonical_bytes(&previous)? {
                    checkpoint.navigation_boundary = navigation_boundary;
                }
            }
            let cursor = journal
                .backend
                .commit(selection.head.clone(), checkpoint)
                .await?;
            journal.acknowledge(&cursor);
            selection.model_selection = model_selection;
            selection.route_witness = route_witness;
            selection.prepared_model_selection = None;
            selection.prepared_route_witness = None;
            let new_checkpoint = selection.head.as_ref() != Some(&cursor);
            if new_checkpoint {
                // A new node points at the previous head and clears redo. The
                // backend preserves redo for identical commits returning old head.
                selection.availability = if navigation_boundary
                    == Some(microcrab_session::NavigationBoundary::EpochInitial)
                {
                    None
                } else {
                    Some(HistoryAvailable {
                        undo: selection.head.is_some(),
                        redo: false,
                    })
                };
            }
            // Ordinary commits extend this exact selected ancestry. A changed
            // marker may reuse the prior prefix after verifying its transition;
            // recovered() always invalidates on every branch/head selection.
            if marker.is_none() {
                selection.compacted = None;
            }
            if !extends
                || selection
                    .display_epoch
                    .is_some_and(|epoch| epoch != display_epoch)
                || selection
                    .display_baseline_generation
                    .is_some_and(|generation| generation != display_baseline_generation)
                || selection.display_marker != marker
            {
                selection.lineage = uuid::Uuid::new_v4().to_string();
            }
            journal
                .anchor
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .display_lineage = selection.lineage.clone();
            selection.display_admission = admission;
            selection.display_prefix =
                Some(DisplayPrefix::Ready(display_prefix.0, display_prefix.1));
            selection.display_epoch = Some(display_epoch);
            selection.display_baseline_generation = Some(display_baseline_generation);
            selection.display_marker = marker;
            selection.head = Some(cursor.clone());
            if new_checkpoint {
                let observer = journal
                    .commit_observer
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone();
                if let Some(observer) = observer {
                    observer.committed(&cursor).await;
                }
            }
            Ok(())
        })
        .await
        .map_err(|error| {
            Error::protocol_with_private_reason(
                "journal commit worker failed",
                format!("journal commit worker: {error}"),
            )
        })?
    }
}
#[async_trait::async_trait]
impl Journal for CreatorJournal {
    async fn commit(&self, recover: Recover) -> Result<(), Error> {
        self.commit_navigation(
            recover,
            Some(microcrab_session::NavigationBoundary::Internal),
            None,
        )
        .await
    }
    async fn commit_admission(
        &self,
        recover: Recover,
        admission: AdmissionCheckpoint,
    ) -> Result<(), Error> {
        self.commit_navigation(
            recover,
            Some(microcrab_session::NavigationBoundary::Internal),
            Some(admission),
        )
        .await
    }
    async fn commit_boundary(
        &self,
        recover: Recover,
        boundary: microcrab_session::NavigationBoundary,
    ) -> Result<(), Error> {
        self.commit_navigation(recover, Some(boundary), None).await
    }
    async fn has_selected_history(&self) -> bool {
        self.selection.lock().await.head.is_some()
    }
    async fn recovered(&self, recover: Recover) -> Result<(), Error> {
        recover.validate()?;
        let mut selection = self.selection.lock().await;
        let pending = selection
            .pending
            .take()
            .ok_or_else(|| Error::Protocol("recovery has no prepared creator head".into()))?;
        if content_hash(&recover)? != pending.recover_hash {
            return Err(Error::Protocol(
                "recovery differs from prepared creator target".into(),
            ));
        }
        let cursor = self
            .backend
            .move_head(pending.expected, pending.target, pending.via)
            .await?;
        let lineage = uuid::Uuid::new_v4().to_string();
        *self
            .anchor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = AuditAnchor {
            display_lineage: lineage.clone(),
            history_head: Some(cursor.checkpoint_id.clone()),
            request_head: pending.request_head,
        };
        selection.lineage = lineage;
        selection.display_admission = None;
        selection.display_prefix = Some(DisplayPrefix::Ready(
            recover.context.items.len(),
            content_hash(&recover.context.items)?,
        ));
        selection.display_epoch = Some(recover.state.epoch_id);
        selection.display_baseline_generation = Some(recover.context.baseline_generation);
        selection.display_marker = recover
            .context
            .compaction
            .as_ref()
            .map(|marker| marker.id.clone());
        selection.head = Some(cursor);
        selection.model_selection = pending.model_selection;
        selection.route_witness = pending.route_witness;
        selection.prepared_model_selection = None;
        selection.prepared_route_witness = None;
        selection.compacted = None;
        // Even To(current) changes redo metadata while preserving cursor equality.
        selection.availability = None;
        Ok(())
    }
    async fn close(&self) -> Result<(), Error> {
        let _selection = self.selection.lock().await;
        self.backend.close().await
    }
}
