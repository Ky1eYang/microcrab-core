//! Session execution owner. Host/controller callbacks run outside short locks.
//! The trusted notice sink is the exception: registry -> inbox serialization
//! publishes semantic facts before quiescence, and never invokes user callbacks.
use super::{
    BufferedOutput, ExecutionController, ExecutionId, ExecutionSnapshot, ExecutionStatus,
    OutputOptions, OutputPage, OutputWriter, ToolFailure, ToolFailureKind, ToolOutput,
    buffer::Buffer,
};
use crate::{Error, Limits};
const MAX_EXECUTION_INSPECTION_BYTES: usize = 2 * 1024 * 1024;
use futures_util::FutureExt;
use std::{
    collections::BTreeMap,
    future::Future,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex, Weak},
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{oneshot, watch};
use tokio_util::sync::CancellationToken;
struct Entry {
    redaction: super::InvocationRedaction,
    // One admission per reserved identity, independent of mutable tool contexts.
    admission_claimed: bool,
    controlled_receipt: bool,
    media_output_bytes: usize,
    row: ExecutionSnapshot,
    started: Option<Instant>,
    cancellation: CancellationToken,
    buffer: Option<Buffer>,
    controller: Option<Arc<dyn ExecutionController>>,
    subordinate_cleanups: Vec<Arc<dyn ExecutionController>>,
    nonce: u64,
    adopted: bool,
    exposed: bool,
    released: bool,
    worker: Option<tokio::task::JoinHandle<()>>,
    cleanup: Option<
        watch::Receiver<Option<Result<(), crate::observation::diagnostics::ToolDiagnostic>>>,
    >,
    revision: u64,
    observation: Option<serde_json::Value>,
    result_delivered: bool,
    notice_observation: Option<ExecutionSnapshot>,
    terminal_observer: Option<watch::Sender<Option<ExecutionStatus>>>,
}
struct RegistryState {
    next_id: u64,
    closed: bool,
    entries: BTreeMap<ExecutionId, Entry>,
}
impl RegistryState {
    fn buffers_used(&self) -> usize {
        self.entries
            .values()
            .filter(|entry| {
                (entry.buffer.is_some() || entry.controller.is_some()) && !entry.released
            })
            .count()
    }

    fn pending_used(&self) -> usize {
        self.entries
            .values()
            .filter(|entry| entry.row.status_projection().is_pending() && !entry.adopted)
            .count()
    }
}

/// Advisory occupancy of the live registry, not a reservation or a history count.
#[derive(serde::Serialize)]
pub(crate) struct ExecutionCapacity {
    buffers: CapacitySlots,
    pending: CapacitySlots,
}

#[derive(serde::Serialize)]
struct CapacitySlots {
    limit: usize,
    used: usize,
    remaining: usize,
}

impl CapacitySlots {
    fn new(limit: usize, used: usize) -> Self {
        Self {
            limit,
            used,
            remaining: limit.saturating_sub(used),
        }
    }
}

struct CapturedRow {
    row: ExecutionSnapshot,
    controller: Option<Arc<dyn ExecutionController>>,
}

pub struct ExecutionRegistry {
    state: Mutex<RegistryState>,
    limits: Limits,
    private_allocations: Arc<tokio::sync::Semaphore>,
    changed: watch::Sender<u64>,
    self_ref: Weak<ExecutionRegistry>,
    quiesce_gate: tokio::sync::Mutex<()>,
    notice_sink: Mutex<Option<Weak<dyn super::ExecutionNoticeSink>>>,
}
/// Revisions stay exactly representable by browser clients and wrap to a live value.
fn next_revision(revision: u64) -> u64 {
    if revision >= 9_007_199_254_740_991 {
        1
    } else {
        revision + 1
    }
}

#[cfg(test)]
#[path = "tests/drain_budget.rs"]
mod drain_budget_tests;
#[cfg(test)]
#[path = "tests/drain_release.rs"]
mod drain_release_tests;

#[cfg(test)]
#[path = "tests/reservation_resources.rs"]
mod reservation_resource_tests;

struct Completion {
    content: Result<String, Arc<Error>>,
    media: Mutex<Option<Vec<super::MediaAttachment>>>,
    had_media: bool,
}
impl Completion {
    fn new(result: Result<super::ResolvedOutput, Arc<Error>>) -> Self {
        match result {
            Ok(output) => Self {
                had_media: !output.attachments.is_empty(),
                content: Ok(output.content),
                media: Mutex::new(Some(output.attachments)),
            },
            Err(error) => Self {
                had_media: false,
                content: Err(error),
                media: Mutex::new(None),
            },
        }
    }
    fn result(&self) -> Result<super::ResolvedOutput, Error> {
        let content = self
            .content
            .as_ref()
            .cloned()
            .map_err(|error| match error.as_ref() {
                Error::Cancelled => Error::Cancelled,
                Error::ExecutionInterrupted => Error::ExecutionInterrupted,
                Error::ToolFailure(failure) => Error::ToolFailure(failure.clone()),
                Error::Tool(message) => Error::Tool(message.clone()),
                Error::Limit(message) => Error::Limit(message.clone()),
                other => Error::Diagnostic(other.tool_diagnostic()),
            })?;
        let attachments = if self.had_media {
            self.media
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
                .ok_or_else(|| Error::tool("typed media completion was already consumed"))?
        } else {
            Vec::new()
        };
        Ok(super::ResolvedOutput {
            content,
            attachments,
        })
    }
    fn text_result(&self) -> Result<String, Error> {
        if self.had_media {
            return Err(Error::tool("image output requires typed Session admission"));
        }
        self.result()?.into_text()
    }
}
#[derive(Clone)]
pub struct ExecutionProgress {
    registry: Weak<ExecutionRegistry>,
    id: ExecutionId,
}
impl ExecutionProgress {
    /// A progress port is valid only for the authoritative invocation registry/id.
    pub(crate) fn belongs_to(&self, registry: &Arc<ExecutionRegistry>, id: ExecutionId) -> bool {
        self.id == id && Weak::ptr_eq(&self.registry, &Arc::downgrade(registry))
    }

    /// Trusted Host adapters reserve and retain bounded effect receipts before
    /// returning to guest code. Ordinary tool names or model text grant no access.
    pub(crate) fn reserve_mutation_receipt(&self, bytes: usize) -> Result<(), Error> {
        let registry = self.registry.upgrade().ok_or(Error::Closed)?;
        crate::budget::check(
            crate::LimitKind::ExecutionInspection,
            bytes,
            MAX_EXECUTION_INSPECTION_BYTES.min(registry.limits.max_context_bytes),
        )
    }
    pub(crate) fn retain_mutation_receipt(&self, content: &str) -> Result<(), Error> {
        self.reserve_mutation_receipt(content.len())?;
        let registry = self.registry.upgrade().ok_or(Error::Closed)?;
        let mut state = registry
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let entry = state.entries.get_mut(&self.id).ok_or(Error::Closed)?;
        let content = entry.redaction.redact_content(content);
        self.reserve_mutation_receipt(content.len())?;
        entry.row.invocation_result = Some(content);
        entry.controlled_receipt = true;
        drop(state);
        registry.notify();
        Ok(())
    }
    /// Retain one actual cleanup owner before its first resource effect. Owners
    /// survive a dropped/poisoned guest Store; registration grants no Host access.
    pub fn retain_cleanup(&self, owner: Arc<dyn ExecutionController>) -> Result<(), Error> {
        let registry = self.registry.upgrade().ok_or(Error::Closed)?;
        registry.retain_cleanup(self.id, owner)
    }

    pub fn execution_id(&self) -> ExecutionId {
        self.id
    }
    /// Bind the child returned by Creator assembly, once, without another job.
    pub fn bind_delegate_child(&self, child: super::DelegateChild) -> Result<(), Error> {
        if child.session_id.trim().is_empty()
            || child.target.trim().is_empty()
            || child.session_id.len() > 256
            || child.target.len() > 256
        {
            return Err(Error::tool("invalid delegate child binding"));
        }
        let registry = self.registry.upgrade().ok_or(Error::Closed)?;
        {
            let mut state = registry
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let entry = state.entries.get_mut(&self.id).ok_or(Error::Closed)?;
            if entry.row.tool != "delegate" {
                return Err(Error::tool("child binding requires a delegate execution"));
            }
            match &entry.row.delegate_child {
                Some(existing) if existing != &child => {
                    return Err(Error::tool("delegate child is already bound"));
                }
                Some(_) => return Ok(()),
                None => entry.row.delegate_child = Some(child),
            }
        }
        registry.notify();
        Ok(())
    }
    pub fn mark_running(&self) {
        if let Some(registry) = self.registry.upgrade() {
            registry.mark(self.id, ExecutionStatus::Running);
        }
    }
    pub fn mark_waiting(&self) {
        if let Some(registry) = self.registry.upgrade() {
            registry.mark(self.id, ExecutionStatus::Waiting);
        }
    }
    pub fn mark_paused_waiting(&self) {
        self.mark_waiting()
    }
}
pub struct ExecutionHandle {
    pub id: ExecutionId,
    completion: watch::Receiver<Option<Arc<Completion>>>,
    activation: Option<oneshot::Sender<()>>,
    cancellation: CancellationToken,
    detached: bool,
}
impl ExecutionHandle {
    pub fn activate(&mut self) -> Result<(), Error> {
        self.activation
            .take()
            .ok_or_else(|| Error::config("execution already activated"))?
            .send(())
            .map_err(|_| Error::Cancelled)
    }
    pub fn detach(&mut self) {
        self.detached = true;
    }
    pub async fn wait(&mut self) -> Result<String, Error> {
        loop {
            if let Some(result) = self.completion.borrow().clone() {
                return result.text_result();
            }
            self.completion.changed().await.map_err(|_| Error::Closed)?;
        }
    }
    pub(crate) async fn wait_output(&mut self) -> Result<super::ResolvedOutput, Error> {
        loop {
            if let Some(result) = self.completion.borrow().clone() {
                return result.result();
            }
            self.completion.changed().await.map_err(|_| Error::Closed)?;
        }
    }
}
impl Drop for ExecutionHandle {
    fn drop(&mut self) {
        if !self.detached {
            self.cancellation.cancel();
        }
    }
}
include!("cleanup.rs");
impl ExecutionRegistry {
    pub(crate) fn retain_cleanup(
        &self,
        id: ExecutionId,
        owner: Arc<dyn ExecutionController>,
    ) -> Result<(), Error> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.closed {
            return Err(Error::Closed);
        }
        let entry = state.entries.get_mut(&id).ok_or(Error::Closed)?;
        if entry
            .subordinate_cleanups
            .iter()
            .any(|current| Arc::ptr_eq(current, &owner))
        {
            return Ok(());
        }
        if entry.cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if entry.cleanup.is_some() || !entry.row.status_projection().is_pending() {
            return Err(Error::tool(
                "execution cleanup registration is already closed",
            ));
        }
        crate::budget::check(
            crate::LimitKind::ExecutionBuffers,
            entry.subordinate_cleanups.len().saturating_add(1),
            8,
        )?;
        entry.subordinate_cleanups.push(owner);
        Ok(())
    }
    /// Claim proof issuance under the execution owner's existing identity lock.
    /// Context clones or cleared proof fields cannot reset this private fact.
    pub(crate) fn claim_admission(&self, id: ExecutionId) -> Result<bool, Error> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let entry = state.entries.get_mut(&id).ok_or(Error::Closed)?;
        if entry.admission_claimed {
            return Ok(false);
        }
        entry.admission_claimed = true;
        Ok(true)
    }
    pub fn new(limits: Limits) -> Arc<Self> {
        let (changed, _) = watch::channel(0);
        Arc::new_cyclic(|weak| Self {
            state: Mutex::new(RegistryState {
                next_id: 1,
                closed: false,
                entries: BTreeMap::new(),
            }),
            private_allocations: Arc::new(tokio::sync::Semaphore::new(
                limits
                    .max_execution_buffers
                    .min(tokio::sync::Semaphore::MAX_PERMITS),
            )),
            limits,
            changed,
            self_ref: weak.clone(),
            quiesce_gate: tokio::sync::Mutex::new(()),
            notice_sink: Mutex::new(None),
        })
    }
    pub fn generation(&self) -> u64 {
        *self.changed.borrow()
    }
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }
    pub(crate) fn bind_notice_sink(&self, sink: Weak<dyn super::ExecutionNoticeSink>) {
        *self
            .notice_sink
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(sink);
    }

    /// Context owns this acknowledgement, after its tool-result append succeeds.
    pub(crate) fn acknowledge_result_delivered(&self, id: ExecutionId) {
        {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if let Some(entry) = state.entries.get_mut(&id) {
                entry.result_delivered = true;
            }
        }
        self.notify();
    }

    /// Registry -> inbox is the sole lock order. Flush semantic observations in
    /// this same fence so a producer's mutation cannot look quiescent before its
    /// notice is accepted. The callback must not reenter this registry.
    pub(crate) fn with_quiescence<R>(&self, project: impl FnOnce(bool) -> R) -> R {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        self.publish_notices(&mut state);
        project(
            !state
                .entries
                .values()
                .any(|entry| entry.row.status_projection().is_pending()),
        )
    }

    fn notice_row(entry: &Entry) -> ExecutionSnapshot {
        let mut row = entry.row.metadata();
        row.output = entry.buffer.as_ref().map(Buffer::metadata);
        row.elapsed_ms = None;
        row.started_at_unix_ms = None;
        row.arguments_available = None;
        row.result_available = None;
        row
    }

    fn publish_notices(&self, state: &mut RegistryState) {
        let sink = self
            .notice_sink
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .and_then(Weak::upgrade);
        for entry in state.entries.values_mut() {
            if !entry.result_delivered || !entry.exposed {
                continue;
            }
            let row = Self::notice_row(entry);
            let changed = entry.notice_observation.as_ref().is_none_or(|previous| {
                serde_json::to_value(previous).ok() != serde_json::to_value(&row).ok()
            });
            if changed {
                let previous = entry.notice_observation.replace(row.clone());
                if let Some(sink) = &sink {
                    sink.changed(super::ExecutionNotice {
                        row,
                        previous,
                        result_delivered: true,
                    });
                }
            }
        }
    }

    fn notify(&self) {
        {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            for entry in state.entries.values_mut() {
                Self::refresh_revision(entry);
            }
            self.publish_notices(&mut state);
        }
        self.changed.send_modify(|n| *n = n.wrapping_add(1));
    }
    // Revisions describe bounded target facts, never elapsed time or unrelated
    // history. Refresh under the same lock as page/snapshot observation so a
    // producer mutation is visible even before its shared wake hint is sent.
    fn refresh_revision(entry: &mut Entry) -> u64 {
        if !entry.row.status.is_pending()
            && let Some(observer) = &entry.terminal_observer
        {
            observer.send_if_modified(|outcome| {
                if outcome.is_none() {
                    *outcome = Some(entry.row.status);
                    true
                } else {
                    false
                }
            });
        }
        let observation = serde_json::json!([
            entry.row.status,
            entry.row.delegate_child,
            entry.row.failure,
            entry.row.diagnostic,
            entry.row.capabilities,
            entry.row.elapsed_ms,
            entry.row.arguments.is_some(),
            entry.row.invocation_result.is_some(),
            entry.row.result.is_some(),
            entry.buffer.as_ref().map(Buffer::metadata),
            entry.adopted,
            entry.exposed,
            entry.released
        ]);
        if entry.observation.as_ref() != Some(&observation) {
            entry.observation = Some(observation);
            entry.revision = next_revision(entry.revision);
        }
        entry.revision
    }
    pub(crate) fn observe_terminal(
        &self,
        id: ExecutionId,
    ) -> Result<watch::Receiver<Option<ExecutionStatus>>, Error> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let entry = state
            .entries
            .get_mut(&id)
            .ok_or_else(|| Error::tool("unknown execution handle for terminal observation"))?;
        if entry.terminal_observer.is_none() {
            entry.terminal_observer = Some(watch::channel(None).0);
        }
        Self::refresh_revision(entry);
        Ok(entry
            .terminal_observer
            .as_ref()
            .expect("terminal observer was installed above")
            .subscribe())
    }
    pub fn execution_revision(&self, id: ExecutionId) -> Result<u64, Error> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let entry = state
            .entries
            .get_mut(&id)
            .ok_or_else(|| Error::tool("unknown execution handle"))?;
        Ok(Self::refresh_revision(entry))
    }
    pub async fn wait_for_execution_change(
        &self,
        id: ExecutionId,
        revision: Option<u64>,
        cancellation: CancellationToken,
    ) -> Result<(u64, ExecutionSnapshot), Error> {
        let mut changed = self.subscribe();
        let revision = match revision {
            Some(revision) => revision,
            None => self.execution_revision(id)?,
        };
        loop {
            changed.borrow_and_update();
            let observation = {
                let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                let entry = state
                    .entries
                    .get_mut(&id)
                    .ok_or_else(|| Error::tool("unknown execution handle"))?;
                let current = Self::refresh_revision(entry);
                (
                    current,
                    {
                        let mut row = entry.row.metadata();
                        row.output_availability = Some(if entry.buffer.is_none() {
                            super::OutputAvailability::None
                        } else if entry.released {
                            super::OutputAvailability::Released
                        } else {
                            super::OutputAvailability::Retained
                        });
                        row
                    },
                    entry.buffer.as_ref().map(Buffer::metadata),
                    entry.controller.clone(),
                )
            };
            if observation.0 != revision || !observation.1.status.is_pending() {
                let mut row = observation.1;
                row.output = observation.2;
                row.capabilities = observation
                    .3
                    .as_ref()
                    .map(|controller| controller.capabilities())
                    .unwrap_or_default();
                return Ok((observation.0, row));
            }
            tokio::select! {biased;_=cancellation.cancelled()=>return Err(Error::Cancelled),result=changed.changed()=>{result.map_err(|_|Error::Closed)?;}}
        }
    }
    pub fn has_pending(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .values()
            .any(|e| e.row.status_projection().is_pending())
    }
    pub fn snapshot(&self) -> Vec<ExecutionSnapshot> {
        self.rows(false, false)
    }
    pub fn recovery_snapshot(&self) -> Vec<ExecutionSnapshot> {
        self.rows(true, false)
    }
    pub fn model_projection(&self) -> Vec<ExecutionSnapshot> {
        self.rows(false, true)
    }
    /// Capture row visibility and occupancy at the same registry instant. As
    /// with other row readers, capability callbacks run outside the owner lock.
    pub(crate) fn model_observation(&self) -> (Vec<ExecutionSnapshot>, ExecutionCapacity) {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let rows = Self::capture_rows(&state, false, true);
        let capacity = ExecutionCapacity {
            buffers: CapacitySlots::new(self.limits.max_execution_buffers, state.buffers_used()),
            pending: CapacitySlots::new(self.limits.max_pending_executions, state.pending_used()),
        };
        drop(state);
        (Self::finish_rows(rows), capacity)
    }
    fn rows(&self, bodies: bool, model: bool) -> Vec<ExecutionSnapshot> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let rows = Self::capture_rows(&state, bodies, model);
        drop(state);
        Self::finish_rows(rows)
    }
    fn capture_rows(state: &RegistryState, bodies: bool, model: bool) -> Vec<CapturedRow> {
        state
            .entries
            .values()
            .filter(|e| !model || (e.exposed && !e.released))
            .map(|e| {
                let mut row = if bodies {
                    e.row.clone()
                } else {
                    e.row.metadata()
                };
                row.output = e.buffer.as_ref().map(Buffer::metadata);
                // Persist the same authoritative availability emitted by live
                // metadata, so full -> metadata and restore -> metadata agree.
                row.output_availability = Some(if e.buffer.is_none() {
                    super::OutputAvailability::None
                } else if e.released {
                    super::OutputAvailability::Released
                } else {
                    super::OutputAvailability::Retained
                });
                if bodies {
                    row.delivery = Some(super::ExecutionDelivery {
                        exposed: e.exposed,
                        released: e.released,
                    });
                    if let Some(buffer) = &e.buffer {
                        if buffer.is_private() {
                            row.output = Some(buffer.recovery_metadata());
                            row.result = None;
                            if let Some(delivery) = &mut row.delivery {
                                delivery.released = true;
                            }
                            row.output_availability = Some(super::OutputAvailability::Released);
                        } else {
                            row.result = Some(buffer.persisted());
                        }
                    }
                }
                CapturedRow {
                    row,
                    controller: e.controller.clone(),
                }
            })
            .collect()
    }
    fn finish_rows(rows: Vec<CapturedRow>) -> Vec<ExecutionSnapshot> {
        rows.into_iter()
            .map(|mut captured| {
                captured.row.capabilities = captured
                    .controller
                    .as_ref()
                    .map(|controller| controller.capabilities())
                    .unwrap_or_default();
                captured.row
            })
            .collect()
    }
    pub fn get(&self, id: ExecutionId) -> Result<ExecutionSnapshot, Error> {
        let (mut row, controller) = {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let entry = state
                .entries
                .get(&id)
                .ok_or_else(|| Error::tool("unknown execution handle"))?;
            let mut row = entry.row.metadata();
            row.output = entry.buffer.as_ref().map(Buffer::metadata);
            row.output_availability = Some(if entry.buffer.is_none() {
                super::OutputAvailability::None
            } else if entry.released {
                super::OutputAvailability::Released
            } else {
                super::OutputAvailability::Retained
            });
            (row, entry.controller.clone())
        };
        row.capabilities = controller
            .as_ref()
            .map(|controller| controller.capabilities())
            .unwrap_or_default();
        Ok(row)
    }
    /// Capture all target revisions under one owner lock before bounded waits
    /// start. Later targets must not silently capture a newer baseline.
    pub(crate) fn execution_revisions(&self, ids: &[ExecutionId]) -> Result<Vec<u64>, Error> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        ids.iter()
            .map(|id| {
                let entry = state
                    .entries
                    .get_mut(id)
                    .ok_or_else(|| Error::tool("unknown execution handle"))?;
                Ok(Self::refresh_revision(entry))
            })
            .collect()
    }
    /// Inspect retained invocation content without touching buffer cursors or cleanup.
    pub fn read_content(
        &self,
        id: ExecutionId,
        field: super::ExecutionContentField,
        cursor: u64,
        max_bytes: usize,
    ) -> Result<super::ExecutionContentPage, Error> {
        if max_bytes == 0 || max_bytes > 65536 {
            return Err(Error::tool("invalid execution content budget"));
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let entry = state
            .entries
            .get_mut(&id)
            .ok_or_else(|| Error::tool("unknown execution handle"))?;
        let revision = Self::refresh_revision(entry);
        let source = match field {
            super::ExecutionContentField::Arguments => entry.row.arguments.as_deref(),
            super::ExecutionContentField::Result => {
                entry.row.invocation_result.as_deref().or_else(|| {
                    entry
                        .buffer
                        .is_none()
                        .then_some(entry.row.result.as_deref())
                        .flatten()
                })
            }
        };
        let text = source.unwrap_or_default();
        use sha2::Digest;
        let source_hash = source.map(|text| format!("{:x}", sha2::Sha256::digest(text.as_bytes())));
        let start = usize::try_from(cursor).map_err(|_| Error::tool("invalid content cursor"))?;
        if start > text.len() || !text.is_char_boundary(start) {
            return Err(Error::tool("invalid content cursor"));
        }
        let mut end = text.len().min(start.saturating_add(max_bytes));
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        loop {
            let page = super::ExecutionContentPage {
                session_id: entry.row.session_id.clone(),
                execution_id: id,
                call_id: entry.row.call_id.clone(),
                field,
                content: text[start..end].to_owned(),
                cursor,
                next_cursor: end as u64,
                end_cursor: text.len() as u64,
                revision,
                available: source.is_some(),
                source_hash: source_hash.clone(),
            };
            let size = serde_json::to_vec(&page)?.len();
            if size <= max_bytes && (end > start || start == text.len()) {
                return Ok(page);
            }
            if end == start {
                return Err(crate::budget::reject(
                    crate::LimitKind::ExecutionInspection,
                    "execution content page budget",
                ));
            }
            end = end
                .saturating_sub(size.saturating_sub(max_bytes).max(1))
                .max(start);
            while !text.is_char_boundary(end) {
                end -= 1;
            }
        }
    }
    /// Retains the observed foreground result, including a buffered receipt, for
    /// authorized display after the corresponding transcript preview was truncated.
    pub(crate) fn error_result_with_effects(&self, id: ExecutionId, error: &Error) -> String {
        let diagnostic = self
            .redaction(id)
            .unwrap_or_default()
            .redact_diagnostic(error.tool_diagnostic());
        let mut value: serde_json::Value = serde_json::from_str(&diagnostic.result_json())
            .unwrap_or_else(|_| serde_json::json!({"error":"execution failed"}));
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(entry) = state
            .entries
            .get(&id)
            .filter(|entry| entry.controlled_receipt)
            && let Some(content) = &entry.row.invocation_result
            && let Ok(receipt) = serde_json::from_str::<serde_json::Value>(content)
        {
            let rows = receipt["receipts"].as_array();
            value["mutation_effects"] = serde_json::json!({"execution_id":id,"committed":rows.map_or(0,|rows|rows.iter().filter(|row|row["status"]=="committed").count()),"uncertain":rows.is_some_and(|rows|rows.iter().any(|row|row["status"]=="uncertain")),"receipts":"Inspect this execution's invocation_result for ordered Host outcomes; do not replay."});
        }
        value.to_string()
    }
    pub(crate) fn retain_invocation_result(
        &self,
        id: ExecutionId,
        content: &str,
    ) -> Result<(), Error> {
        let budget = MAX_EXECUTION_INSPECTION_BYTES.min(self.limits.max_context_bytes);
        crate::budget::check(crate::LimitKind::ExecutionInspection, content.len(), budget)?;
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let entry = state
            .entries
            .get_mut(&id)
            .ok_or_else(|| Error::tool("unknown execution handle"))?;
        if !entry.controlled_receipt {
            let content = entry.redaction.redact_content(content);
            crate::budget::check(crate::LimitKind::ExecutionInspection, content.len(), budget)?;
            entry.row.invocation_result = Some(content);
        }
        drop(state);
        self.notify();
        Ok(())
    }
    pub fn validate_recovery_limits(&self, limits: &Limits) -> Result<(), Error> {
        if limits.max_tool_output_bytes > self.limits.max_tool_output_bytes
            || limits.max_tool_image_bytes > self.limits.max_tool_image_bytes
            || limits.output_buffer_bytes > self.limits.output_buffer_bytes
            || limits.max_pending_executions > self.limits.max_pending_executions
            || limits.max_retained_executions > self.limits.max_retained_executions
            || limits.max_execution_buffers > self.limits.max_execution_buffers
        {
            return Err(Error::config(
                "recovery execution limits exceed configured owner",
            ));
        }
        Self::validate_rows(&self.recovery_snapshot(), limits)
    }
    pub fn validate_rows(rows: &[ExecutionSnapshot], limits: &Limits) -> Result<(), Error> {
        crate::budget::check(
            crate::LimitKind::ExecutionHistory,
            rows.len(),
            super::retention::RetentionPolicy::executions(limits)
                .execution_history_capacity(limits),
        )?;
        let mut seen = std::collections::BTreeSet::new();
        for row in rows {
            if row.execution_id.0 == 0
                || row.execution_id.0 >= 9_007_199_254_740_991
                || !seen.insert(row.execution_id)
                || row.invocation_result.as_ref().is_some_and(|s| {
                    s.len() > MAX_EXECUTION_INSPECTION_BYTES.min(limits.max_context_bytes)
                })
                || row.result.as_ref().is_some_and(|s| {
                    s.len()
                        > limits
                            .output_buffer_bytes
                            .saturating_mul(6)
                            .saturating_add(1024 * 512 + 16 * 1024)
                })
            {
                return Err(Error::tool("invalid execution history"));
            }
            if row.output.is_none()
                && (row.status == ExecutionStatus::Succeeded || row.failure.is_some())
                && row.result.is_none()
            {
                return Err(Error::tool(
                    "final execution history is missing result bytes",
                ));
            }
            if let Some(failure) = &row.failure {
                failure.validate()?;
            }
            if let Some(output) = &row.output {
                Buffer::new(
                    OutputOptions {
                        channels: output.channels.clone(),
                    },
                    limits.output_buffer_bytes,
                )?;
                Buffer::restore(output, row.result.as_deref(), limits.output_buffer_bytes)?;
                if row.delivery.is_some_and(|delivery| delivery.released)
                    && output.start_cursor != output.end_cursor
                {
                    return Err(Error::tool("released output retains bytes"));
                }
            }
        }
        Ok(())
    }
    /// Session's replacement fence prevents admission while this reusable cleanup
    /// settles. The worker retains cleanup ownership if the awaiting caller drops.
    pub async fn quiesce_for_restore(&self) -> Result<(), Error> {
        let owner = self.self_ref.upgrade().ok_or(Error::Closed)?;
        let (sender, receiver) = oneshot::channel();
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            Error::lifecycle("execution replacement cleanup needs the embedding async runtime")
        })?;
        runtime.spawn(async move {
            let result = owner.quiesce_owned().await;
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| Error::lifecycle("execution replacement cleanup worker stopped"))?
    }
    async fn quiesce_owned(&self) -> Result<(), Error> {
        let _replacement = self.quiesce_gate.lock().await;
        let ids = {
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            for entry in state.entries.values() {
                entry.cancellation.cancel();
            }
            state.entries.keys().copied().collect::<Vec<_>>()
        };
        let mut first = None;
        for id in &ids {
            if self.get(*id)?.status.is_pending()
                && let Err(error) = self.cancel(*id).await
            {
                // A producer may settle between the snapshot and cancel's
                // locked admission; that exact terminal race is success.
                let settled = matches!(&error, Error::Tool(cause) if cause.as_str() == "execution is already terminal")
                    && !self.get(*id)?.status.is_pending();
                if !settled {
                    first.get_or_insert(error);
                }
            }
        }
        for id in &ids {
            if let Err(error) = self.cleanup_controller(*id).await {
                first.get_or_insert(error);
            }
        }
        let workers = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state
                .entries
                .iter_mut()
                .filter_map(|(id, entry)| entry.worker.take().map(|worker| (*id, worker)))
                .collect::<Vec<_>>()
        };
        for (id, mut worker) in workers {
            match tokio::time::timeout(
                std::time::Duration::from_millis(self.limits.lifecycle_timeout_ms),
                &mut worker,
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    first.get_or_insert_with(|| {
                        Error::Lifecycle(
                            format!(
                                "execution {} replacement producer task {}",
                                id.0,
                                if error.is_cancelled() {
                                    "was cancelled"
                                } else {
                                    "panicked"
                                }
                            )
                            .into(),
                        )
                    });
                }
                Err(_) => {
                    let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(entry) = state.entries.get_mut(&id) {
                        entry.worker = Some(worker);
                    }
                    first.get_or_insert_with(|| {
                        Error::lifecycle("execution replacement producer did not settle")
                    });
                }
            }
        }
        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            for entry in state.entries.values_mut() {
                if entry.controller.is_none()
                    && entry.subordinate_cleanups.is_empty()
                    && entry.worker.is_none()
                {
                    if entry.row.status.is_pending() {
                        entry.row.status = ExecutionStatus::Interrupted;
                    }
                    if let Some(buffer) = &mut entry.buffer {
                        buffer.closed = true;
                    }
                }
            }
        }
        self.notify();
        first.map_or(Ok(()), Err)
    }
    pub async fn release_for_restore(&self) -> Result<(), Error> {
        let ids = {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state
                .entries
                .values()
                .any(|entry| entry.row.status_projection().is_pending())
            {
                return Err(Error::Busy);
            }
            state
                .entries
                .iter()
                .filter(|(_, entry)| {
                    entry.controller.is_some() || !entry.subordinate_cleanups.is_empty()
                })
                .map(|(id, _)| *id)
                .collect::<Vec<_>>()
        };
        for id in ids {
            self.cleanup_controller(id).await?;
        }
        Ok(())
    }
    pub fn restore(&self, rows: &[ExecutionSnapshot]) -> Result<(), Error> {
        Self::validate_rows(rows, &self.limits)?;
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.entries.values().any(|e| {
            e.row.status_projection().is_pending()
                || e.controller.is_some()
                || !e.subordinate_cleanups.is_empty()
        }) {
            return Err(Error::Busy);
        }
        for entry in state.entries.values_mut() {
            Self::refresh_revision(entry);
        }
        state.entries.clear();
        state.closed = false;
        for row in rows {
            let mut row = row.clone();
            let was_pending = row.status.is_pending();
            let delivery =
                row.delivery.unwrap_or(super::ExecutionDelivery {
                    exposed: row.output.as_ref().is_some_and(|output| {
                        output.end_cursor > output.start_cursor || was_pending
                    }),
                    released: row.output.as_ref().is_none_or(|output| {
                        output.end_cursor == output.start_cursor && !was_pending
                    }),
                });
            if row.status.is_pending() {
                row.status = ExecutionStatus::Interrupted;
                row.failure = None;
                row.diagnostic = Some(Error::lifecycle("execution interrupted").tool_diagnostic());
            }
            let mut buffer = None;
            if let Some(output) = &row.output {
                let mut restored = Buffer::restore(
                    output,
                    row.result.as_deref(),
                    self.limits.output_buffer_bytes,
                )?;
                restored.closed = true;
                buffer = Some(restored);
            }
            state.next_id = state
                .next_id
                .max(row.execution_id.0.checked_add(1).ok_or_else(|| {
                    crate::budget::reject(crate::LimitKind::Arithmetic, "execution id overflow")
                })?);
            let private = buffer.as_ref().is_some_and(Buffer::is_private);
            state.entries.insert(
                row.execution_id,
                Entry {
                    redaction: super::InvocationRedaction::default(),
                    admission_claimed: false,
                    controlled_receipt: false,
                    media_output_bytes: 0,
                    row,
                    started: None,
                    cancellation: CancellationToken::new(),
                    buffer,
                    controller: None,
                    subordinate_cleanups: Vec::new(),
                    nonce: 0,
                    adopted: true,
                    exposed: delivery.exposed || was_pending,
                    released: private || (delivery.released && !was_pending),
                    worker: None,
                    cleanup: None,
                    revision: 0,
                    observation: None,
                    result_delivered: false,
                    notice_observation: None,
                    terminal_observer: None,
                },
            );
        }
        drop(state);
        self.notify();
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn start<F, Make>(
        self: &Arc<Self>,
        row: ExecutionSnapshot,
        cancellation: CancellationToken,
        make: Make,
    ) -> Result<ExecutionHandle, Error>
    where
        F: Future<Output = Result<ToolOutput, Error>> + Send + 'static,
        Make: FnOnce(ExecutionProgress) -> F,
    {
        self.start_with_media_output(row, cancellation, 0, make)
    }

    /// Runtime pins this trusted registration declaration before activation/effects.
    pub(crate) fn start_with_media_output<F, Make>(
        self: &Arc<Self>,
        mut row: ExecutionSnapshot,
        cancellation: CancellationToken,
        declared_media_bytes: usize,
        make: Make,
    ) -> Result<ExecutionHandle, Error>
    where
        F: Future<Output = Result<ToolOutput, Error>> + Send + 'static,
        Make: FnOnce(ExecutionProgress) -> F,
    {
        crate::budget::check(
            crate::LimitKind::AttachmentBytes,
            declared_media_bytes,
            crate::admission::image::MAX_IMAGE_BYTES,
        )?;
        let media_output_bytes = declared_media_bytes.min(self.limits.max_tool_image_bytes);
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed {
            return Err(Error::Closed);
        }
        if row.execution_id.0 == 0 {
            self.check_pending_admission(&state)?;
        }
        let terminal: Vec<_> = state
            .entries
            .iter()
            .filter(|(_, e)| {
                !e.row.status_projection().is_pending()
                    && e.controller.is_none()
                    && e.subordinate_cleanups.is_empty()
                    && ((!e.exposed && e.buffer.is_none()) || e.released)
            })
            .map(|(id, _)| *id)
            .collect();
        let remove =
            super::retention::RetentionPolicy::executions(&self.limits).evict_count(terminal.len());
        if remove > 0 {
            // Retained capacity is a discard boundary, not a failure: record it so
            // dropped history stays auditable after the fact.
            tracing::warn!(
                evicted = remove,
                retained_capacity = self.limits.max_retained_executions,
                "retained execution history exceeded its capacity; oldest terminal entries were evicted"
            );
        }
        for id in terminal.into_iter().take(remove) {
            if let Some(entry) = state.entries.get_mut(&id) {
                Self::refresh_revision(entry);
            }
            state.entries.remove(&id);
        }
        if row.execution_id.0 == 0 {
            crate::budget::check(
                crate::LimitKind::ExecutionHistory,
                state.entries.len().saturating_add(1),
                super::retention::RetentionPolicy::executions(&self.limits)
                    .execution_history_capacity(&self.limits),
            )?;
        }
        let id = if row.execution_id.0 == 0 {
            let id = ExecutionId(state.next_id);
            state.next_id = state
                .next_id
                .checked_add(1)
                .filter(|n| *n <= 9_007_199_254_740_991)
                .ok_or_else(|| {
                    crate::budget::reject(crate::LimitKind::Arithmetic, "execution id overflow")
                })?;
            id
        } else {
            let entry = state
                .entries
                .get(&row.execution_id)
                .filter(|e| {
                    e.worker.is_none()
                        && e.controller.is_none()
                        && e.buffer.is_none()
                        && e.cleanup.is_none()
                        && e.row.status_projection().is_pending()
                        && e.row.call_id == row.call_id
                        && e.row.session_id == row.session_id
                })
                .ok_or_else(|| Error::tool("invalid execution reservation"))?;
            row.started_at_unix_ms = entry.row.started_at_unix_ms;
            row.arguments = entry.row.arguments.clone();
            row.execution_id
        };
        row.execution_id = id;
        if row.started_at_unix_ms.is_none() {
            row.started_at_unix_ms = Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
                    .try_into()
                    .unwrap_or(u64::MAX),
            );
        }
        let started = state
            .entries
            .get(&id)
            .and_then(|entry| entry.started)
            .unwrap_or_else(Instant::now);
        let progress = ExecutionProgress {
            registry: Arc::downgrade(self),
            id,
        };
        let future = make(progress);
        let (activation, activated) = oneshot::channel();
        let (sender, completion) = watch::channel(None);
        let weak = Arc::downgrade(self);
        let token = cancellation.clone();
        let cleanup = self.limits.lifecycle_timeout_ms;
        let worker = tokio::spawn(async move {
            let result = async {
                activated.await.map_err(|_| Error::Cancelled)?;
                if token.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                future.await
            };
            let execution = AssertUnwindSafe(result).catch_unwind();
            tokio::pin!(execution);
            let result = tokio::select! {biased;_=token.cancelled()=>match tokio::time::timeout(std::time::Duration::from_millis(cleanup),&mut execution).await{Ok(result)=>result.unwrap_or_else(|_|Err(Error::tool("execution panicked during cancellation"))),Err(_)=>Err(Error::ExecutionInterrupted)},result=&mut execution=>result.unwrap_or_else(|_|Err(Error::tool("execution panicked; effects may be unknown")))};
            if let Some(registry) = weak.upgrade() {
                let resolved = registry.resolve(id, result).await;
                if let Err(error) = &resolved {
                    registry.finish_error(id, error).await;
                }
                let _ = sender.send(Some(Arc::new(Completion::new(resolved.map_err(Arc::new)))));
            }
        });
        let admission_claimed = state
            .entries
            .get(&id)
            .is_some_and(|entry| entry.admission_claimed);
        let terminal_observer = state
            .entries
            .get_mut(&id)
            .and_then(|entry| entry.terminal_observer.take());
        let subordinate_cleanups = state
            .entries
            .get_mut(&id)
            .map(|entry| std::mem::take(&mut entry.subordinate_cleanups))
            .unwrap_or_default();
        let redaction = state
            .entries
            .get(&id)
            .map(|entry| entry.redaction.clone())
            .unwrap_or_default();
        state.entries.insert(
            id,
            Entry {
                redaction,
                admission_claimed,
                controlled_receipt: false,
                media_output_bytes,
                row,
                started: Some(started),
                cancellation: cancellation.clone(),
                buffer: None,
                controller: None,
                subordinate_cleanups,
                nonce: id.0,
                adopted: false,
                exposed: false,
                released: false,
                worker: Some(worker),
                cleanup: None,
                revision: 0,
                observation: None,
                result_delivered: false,
                notice_observation: None,
                terminal_observer,
            },
        );
        drop(state);
        self.notify();
        Ok(ExecutionHandle {
            id,
            completion,
            activation: Some(activation),
            cancellation,
            detached: false,
        })
    }
    fn check_pending_admission(&self, state: &RegistryState) -> Result<(), Error> {
        crate::budget::check(
            crate::LimitKind::PendingExecutions,
            state.pending_used().saturating_add(1),
            self.limits.max_pending_executions,
        )
    }

    pub(crate) fn admit_detached(
        self: &Arc<Self>,
        mut row: ExecutionSnapshot,
        cancellation: CancellationToken,
    ) -> Result<ExecutionId, Error> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed {
            return Err(Error::Closed);
        }
        self.check_pending_admission(&state)?;
        let terminal: Vec<_> = state
            .entries
            .iter()
            .filter(|(_, e)| {
                !e.row.status_projection().is_pending()
                    && e.controller.is_none()
                    && e.subordinate_cleanups.is_empty()
                    && ((!e.exposed && e.buffer.is_none()) || e.released)
            })
            .map(|(id, _)| *id)
            .collect();
        let remove =
            super::retention::RetentionPolicy::executions(&self.limits).evict_count(terminal.len());
        if remove > 0 {
            // Retained capacity is a discard boundary, not a failure: record it so
            // dropped history stays auditable after the fact.
            tracing::warn!(
                evicted = remove,
                retained_capacity = self.limits.max_retained_executions,
                "retained execution history exceeded its capacity; oldest terminal entries were evicted"
            );
        }
        for id in terminal.into_iter().take(remove) {
            if let Some(entry) = state.entries.get_mut(&id) {
                Self::refresh_revision(entry);
            }
            state.entries.remove(&id);
        }
        crate::budget::check(
            crate::LimitKind::ExecutionHistory,
            state.entries.len().saturating_add(1),
            super::retention::RetentionPolicy::executions(&self.limits)
                .execution_history_capacity(&self.limits),
        )?;
        let id = ExecutionId(state.next_id);
        state.next_id = state
            .next_id
            .checked_add(1)
            .filter(|n| *n <= 9_007_199_254_740_991)
            .ok_or_else(|| {
                crate::budget::reject(crate::LimitKind::Arithmetic, "execution id overflow")
            })?;
        row.execution_id = id;
        row.started_at_unix_ms = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
        );
        state.entries.insert(
            id,
            Entry {
                redaction: super::InvocationRedaction::default(),
                admission_claimed: false,
                controlled_receipt: false,
                media_output_bytes: 0,
                row,
                started: Some(Instant::now()),
                cancellation,
                buffer: None,
                controller: None,
                subordinate_cleanups: Vec::new(),
                nonce: id.0,
                adopted: false,
                exposed: false,
                released: false,
                worker: None,
                cleanup: None,
                revision: 0,
                observation: None,
                result_delivered: false,
                notice_observation: None,
                terminal_observer: None,
            },
        );
        drop(state);
        self.notify();
        Ok(id)
    }
    pub fn open_output(
        self: &Arc<Self>,
        id: ExecutionId,
        options: OutputOptions,
        controller: Arc<dyn ExecutionController>,
    ) -> Result<(OutputWriter, BufferedOutput), Error> {
        self.open_output_with_kind(id, options, controller, None)
    }
    pub(crate) fn open_private_output(
        self: &Arc<Self>,
        id: ExecutionId,
        media_type: String,
        controller: Arc<dyn ExecutionController>,
    ) -> Result<(OutputWriter, BufferedOutput), Error> {
        self.open_output_with_kind(id, OutputOptions::default(), controller, Some(media_type))
    }
    fn open_output_with_kind(
        self: &Arc<Self>,
        id: ExecutionId,
        options: OutputOptions,
        controller: Arc<dyn ExecutionController>,
        private_mime: Option<String>,
    ) -> Result<(OutputWriter, BufferedOutput), Error> {
        let redaction = self.redaction(id)?;
        if options
            .channels
            .iter()
            .any(|channel| redaction.redact_text(channel) != *channel)
        {
            return Err(Error::tool("output channel contains a secret"));
        }
        let mut buffer = Buffer::new(options, self.limits.output_buffer_bytes)?;
        buffer.install_redaction(redaction);
        if let Some(media_type) = private_mime {
            let permit = self
                .private_allocations
                .clone()
                .try_acquire_owned()
                .map_err(|_| {
                    crate::budget::reject(
                        crate::LimitKind::ToolOutput,
                        "private capture allocation limit reached",
                    )
                })?;
            buffer.install_private(media_type, permit)?;
        }
        let capabilities = controller.capabilities();
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed {
            return Err(Error::Closed);
        }
        if state.buffers_used() >= self.limits.max_execution_buffers {
            let mut diagnostic = crate::observation::diagnostics::ToolDiagnostic::new(
                crate::observation::diagnostics::DiagnosticCode::OutputLimit,
                crate::observation::diagnostics::DiagnosticLayer::Runtime,
                crate::observation::diagnostics::DiagnosticPhase::OutputValidation,
            );
            diagnostic.limit_owner =
                Some(crate::observation::diagnostics::LimitOwner::ExecutionBuffers);
            return Err(Error::Diagnostic(diagnostic));
        }
        let entry = state
            .entries
            .get_mut(&id)
            .ok_or_else(|| Error::tool("output requires an admitted execution"))?;
        if entry.buffer.is_some()
            || entry.controller.is_some()
            || entry.cleanup.is_some()
            || !entry.row.status_projection().is_pending()
        {
            return Err(Error::tool("execution already has output or is terminal"));
        }
        entry.buffer = Some(buffer);
        entry.row.capabilities = capabilities;
        entry.controller = Some(controller);
        let nonce = entry.nonce;
        drop(state);
        self.notify();
        Ok((
            OutputWriter::new(self, id, nonce),
            BufferedOutput {
                registry: self.clone(),
                id,
                nonce,
                adopted: false,
                include_output: false,
                on_adopt: None,
            },
        ))
    }
    pub(crate) fn install_redaction(
        &self,
        id: ExecutionId,
        redaction: super::InvocationRedaction,
    ) -> Result<(), Error> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let entry = state.entries.get_mut(&id).ok_or(Error::Closed)?;
        if entry.buffer.is_some() || entry.worker.is_some() {
            return Err(Error::tool(
                "execution redaction must be installed before dispatch",
            ));
        }
        entry.redaction = redaction;
        Ok(())
    }
    pub(crate) fn merge_redaction(
        &self,
        id: ExecutionId,
        source: &super::InvocationRedaction,
    ) -> Result<(), Error> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let entry = state.entries.get(&id).ok_or(Error::Closed)?;
        entry.redaction.merge(source)
    }
    fn redaction(&self, id: ExecutionId) -> Result<super::InvocationRedaction, Error> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .entries
            .get(&id)
            .ok_or(Error::Closed)?
            .redaction
            .clone())
    }
    pub(super) fn append(
        &self,
        id: ExecutionId,
        nonce: u64,
        channel: Option<&str>,
        content: &str,
    ) -> Result<(), Error> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let entry = state
            .entries
            .get_mut(&id)
            .filter(|e| e.nonce == nonce)
            .ok_or_else(|| Error::tool("stale output writer"))?;
        entry
            .buffer
            .as_mut()
            .ok_or_else(|| Error::tool("execution has no output"))?
            .append(channel, content)?;
        drop(state);
        self.notify();
        Ok(())
    }
    pub(super) fn append_private(
        &self,
        id: ExecutionId,
        nonce: u64,
        bytes: &[u8],
    ) -> Result<(), Error> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state
            .entries
            .get_mut(&id)
            .filter(|entry| entry.nonce == nonce)
            .and_then(|entry| entry.buffer.as_mut())
            .ok_or_else(|| Error::tool("stale private output writer"))?
            .append_private(bytes)?;
        drop(state);
        self.notify();
        Ok(())
    }
    /// Take a shared immutable backing while proving the exact execution and
    /// channel are still owned. The caller receives no borrow of the registry.
    pub(crate) fn private_output(
        &self,
        id: ExecutionId,
        session_id: &str,
        channel: &str,
        expected_mime: &str,
    ) -> Result<super::PrivateOutputLease, Error> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.closed {
            return Err(Error::Closed);
        }
        let entry = state
            .entries
            .get(&id)
            .filter(|entry| entry.row.session_id == session_id && entry.exposed)
            .ok_or_else(|| Error::tool("private output execution is not owned or exposed"))?;
        if entry.released || entry.row.status_projection().status != ExecutionStatus::Succeeded {
            return Err(Error::tool(
                "job redirect requires a successful terminal, unreleased execution",
            ));
        }
        entry
            .buffer
            .as_ref()
            .ok_or_else(|| Error::tool("execution has no retained private output"))?
            .private_output(channel, expected_mime)
    }
    pub(super) fn mark_truncated(&self, id: ExecutionId, nonce: u64) -> Result<(), Error> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let entry = state
            .entries
            .get_mut(&id)
            .filter(|e| e.nonce == nonce)
            .ok_or_else(|| Error::tool("stale output writer"))?;
        entry
            .buffer
            .as_mut()
            .ok_or_else(|| Error::tool("execution has no output"))?
            .mark_truncated()?;
        drop(state);
        self.notify();
        Ok(())
    }
    pub(super) fn confirm_cancelled(&self, id: ExecutionId, nonce: u64) -> Result<(), Error> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let entry = state
            .entries
            .get_mut(&id)
            .filter(|e| e.nonce == nonce)
            .ok_or_else(|| Error::tool("stale output writer"))?;
        if !entry.row.status_projection().is_pending() {
            return Ok(());
        }
        if let Some(buffer) = &mut entry.buffer {
            buffer.flush_redaction()?;
            buffer.closed = true;
        }
        entry.row.status = ExecutionStatus::Cancelled;
        entry.row.failure = None;
        entry.row.elapsed_ms = entry
            .started
            .map(|t| t.elapsed().as_millis().try_into().unwrap_or(u64::MAX));
        drop(state);
        self.notify();
        Ok(())
    }
    pub(super) fn interrupt_producer(
        &self,
        id: ExecutionId,
        nonce: u64,
        _message: String,
        diagnostic: Option<crate::observation::diagnostics::ToolDiagnostic>,
    ) -> Result<(), Error> {
        use crate::observation::diagnostics::{
            DiagnosticCode, DiagnosticLayer, DiagnosticPhase, ToolDiagnostic,
        };
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let entry = state
            .entries
            .get_mut(&id)
            .filter(|e| e.nonce == nonce)
            .ok_or_else(|| Error::tool("stale output writer"))?;
        let buffer = entry
            .buffer
            .as_mut()
            .ok_or_else(|| Error::tool("execution has no output"))?;
        if buffer.closed {
            return Ok(());
        }
        buffer.flush_redaction()?;
        buffer.closed = true;
        entry.row.status = ExecutionStatus::Interrupted;
        entry.row.failure = None;
        entry.row.diagnostic = Some(diagnostic.map_or_else(
            || ToolDiagnostic::new(
                DiagnosticCode::ToolFailed,
                DiagnosticLayer::Runtime,
                DiagnosticPhase::Unknown,
            ),
            |diagnostic| entry.redaction.redact_diagnostic(diagnostic),
        ));
        entry.row.elapsed_ms = entry
            .started
            .map(|t| t.elapsed().as_millis().try_into().unwrap_or(u64::MAX));
        drop(state);
        tracing::debug!(execution_id = id.0, "execution producer interrupted");
        self.notify();
        Ok(())
    }
    pub(super) fn finish_producer(
        &self,
        id: ExecutionId,
        nonce: u64,
        outcome: Result<(), ToolFailure>,
    ) -> Result<(), Error> {
        if let Err(failure) = &outcome {
            failure.validate()?;
        }
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let entry = state
            .entries
            .get_mut(&id)
            .filter(|e| e.nonce == nonce)
            .ok_or_else(|| Error::tool("stale output writer"))?;
        let outcome = match outcome {
            Ok(()) => Ok(()),
            Err(failure) => Err(failure.redacted(&entry.redaction)?),
        };
        let buffer = entry
            .buffer
            .as_mut()
            .ok_or_else(|| Error::tool("execution has no output"))?;
        if buffer.closed {
            return Ok(());
        }
        buffer.flush_redaction()?;
        buffer.closed = true;
        entry.row.status = match &outcome {
            Ok(()) => ExecutionStatus::Succeeded,
            Err(failure) if failure.kind == ToolFailureKind::Cancelled => {
                ExecutionStatus::Cancelled
            }
            Err(_) => ExecutionStatus::Failed,
        };
        entry.row.failure = outcome.err();
        entry.row.elapsed_ms = entry
            .started
            .map(|t| t.elapsed().as_millis().try_into().unwrap_or(u64::MAX));
        drop(state);
        self.notify();
        Ok(())
    }
    pub(crate) async fn complete_detached(
        self: &Arc<Self>,
        id: ExecutionId,
        output: super::FinalOutput,
    ) -> Result<(), Error> {
        let result = self.resolve(id, Ok(ToolOutput::Final(output))).await;
        if let Err(error) = &result {
            self.finish_error(id, error).await;
        }
        result.map(|_| ())
    }
    async fn resolve(
        self: &Arc<Self>,
        id: ExecutionId,
        result: Result<ToolOutput, Error>,
    ) -> Result<super::ResolvedOutput, Error> {
        match result {
            Ok(ToolOutput::Buffered(mut output)) => {
                if !Arc::ptr_eq(self, &output.registry) || output.id != id {
                    return Err(Error::tool("foreign buffered output"));
                }
                // Controller callbacks remain outside the registry lock. The
                // model-visible status/output and notice baseline below share
                // one locked observation, including the optional initial page.
                let capabilities = self.get(id)?.capabilities;
                let receipt = {
                    let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                    let entry = state
                        .entries
                        .get_mut(&id)
                        .filter(|e| e.nonce == output.nonce && e.buffer.is_some() && !e.adopted)
                        .ok_or_else(|| Error::tool("stale or already adopted output"))?;
                    entry.adopted = true;
                    entry.exposed = true;
                    let receipt =
                        self.receipt_from_entry(id, entry, output.include_output, capabilities);
                    if receipt.is_err() {
                        entry.adopted = false;
                        entry.exposed = false;
                    } else {
                        entry.notice_observation = Some(Self::notice_row(entry));
                    }
                    receipt?
                };
                output.adopted = true;
                if let Some(start) = output.on_adopt.take() {
                    start();
                }
                self.notify();
                Ok(super::ResolvedOutput::text(receipt))
            }
            Ok(ToolOutput::Final(mut final_output)) => {
                crate::budget::check(
                    crate::LimitKind::ToolOutput,
                    final_output.content.len(),
                    self.limits.max_tool_output_bytes,
                )?;
                let redaction = self.redaction(id)?;
                final_output.content = redaction.redact_content(&final_output.content);
                final_output.failure = final_output
                    .failure
                    .map(|failure| failure.redacted(&redaction))
                    .transpose()?;
                if let Some(failure) = &final_output.failure {
                    failure.validate()?;
                }
                crate::budget::check(
                    crate::LimitKind::AttachmentCount,
                    final_output.attachments.len(),
                    crate::admission::image::MAX_IMAGES_PER_TOOL_OUTPUT,
                )?;
                let media_limit = {
                    let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                    state
                        .entries
                        .get(&id)
                        .ok_or(Error::Closed)?
                        .media_output_bytes
                };
                for attachment in &final_output.attachments {
                    crate::budget::check(
                        crate::LimitKind::AttachmentBytes,
                        attachment.data.len(),
                        media_limit,
                    )?;
                    crate::admission::image::validate_image(
                        &attachment.media_type,
                        &attachment.data,
                    )?;
                }
                let body = if final_output.failure.is_some() {
                    serde_json::to_string(&serde_json::json!({
                        "content": &final_output.content,
                        "failure": &final_output.failure,
                    }))?
                } else {
                    final_output.content.clone()
                };
                let observation =
                    super::ResolvedOutput::observation_text(&body, &final_output.attachments)?;
                let display = if final_output.attachments.is_empty() {
                    final_output.content.clone()
                } else {
                    observation.clone()
                };
                if let Err(error) = crate::budget::check(
                    crate::LimitKind::ToolOutput,
                    body.len(),
                    self.limits.max_tool_output_bytes,
                ) {
                    self.finish_error(id, &error).await;
                    return Err(error);
                }
                {
                    let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                    let entry = state.entries.get_mut(&id).ok_or(Error::Closed)?;
                    if let Some(buffer) = &mut entry.buffer {
                        buffer.closed = true;
                    }
                    // A private producer may have launched before a guest returned
                    // an incompatible Final result. Keep its backing until cleanup ACK.
                    if !entry.buffer.as_ref().is_some_and(Buffer::is_private) {
                        entry.buffer = None;
                    }
                    entry.row.result = Some(display.clone());
                    if !entry.controlled_receipt {
                        entry.row.invocation_result = Some(observation.clone());
                    }
                    entry.row.elapsed_ms = entry
                        .started
                        .map(|t| t.elapsed().as_millis().try_into().unwrap_or(u64::MAX));
                }
                // Producer completion is measurable before potentially slow Host
                // cleanup. Keep pending status and owner until actual ACK; display
                // reads never release, acknowledge or resume a model continuation.
                self.notify();
                self.cleanup_controller(id).await?;
                {
                    let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                    let entry = state.entries.get_mut(&id).ok_or(Error::Closed)?;
                    entry.row.status = match final_output.failure.as_ref().map(|f| f.kind) {
                        None => ExecutionStatus::Succeeded,
                        Some(ToolFailureKind::Cancelled) => ExecutionStatus::Cancelled,
                        _ => ExecutionStatus::Failed,
                    };
                    entry.row.failure = final_output.failure;
                    entry.row.result = Some(display);
                    entry.buffer = None;
                    entry.released = true;
                    entry.redaction = super::InvocationRedaction::default();
                }
                self.notify();
                Ok(super::ResolvedOutput {
                    content: body,
                    attachments: final_output.attachments,
                })
            }
            Err(error) => {
                let error = self.redaction(id)?.redact_error(error);
                self.finish_error(id, &error).await;
                Err(error)
            }
        }
    }
    pub(crate) async fn finish_error(&self, id: ExecutionId, error: &Error) {
        let mut cleanup_failed = false;
        {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if let Some(entry) = state.entries.get_mut(&id) {
                cleanup_failed = entry.row.status == ExecutionStatus::Interrupted
                    && (entry.controller.is_some() || !entry.subordinate_cleanups.is_empty())
                    && matches!(error, Error::Diagnostic(diagnostic) if entry.row.diagnostic.as_ref() == Some(diagnostic));
                entry.row.status = if cleanup_failed {
                    ExecutionStatus::Interrupted
                } else {
                    match error {
                        Error::Cancelled => ExecutionStatus::Cancelled,
                        Error::ToolFailure(failure)
                            if failure.kind == ToolFailureKind::Cancelled =>
                        {
                            ExecutionStatus::Cancelled
                        }
                        Error::ExecutionInterrupted => ExecutionStatus::Interrupted,
                        _ => ExecutionStatus::Failed,
                    }
                };
                let mut diagnostic = entry.redaction.redact_diagnostic(error.tool_diagnostic());
                entry.row.failure = if let Error::ToolFailure(failure) = error {
                    if entry.buffer.is_none() {
                        entry.row.result = Some(String::new());
                    }
                    match failure.clone().redacted(&entry.redaction) {
                        Ok(failure) => Some(failure),
                        Err(error) => {
                            diagnostic = entry.redaction.redact_diagnostic(error.tool_diagnostic());
                            None
                        }
                    }
                } else {
                    None
                };
                entry.row.diagnostic = Some(diagnostic);
                if entry.row.elapsed_ms.is_none() {
                    entry.row.elapsed_ms = entry
                        .started
                        .map(|start| start.elapsed().as_millis().try_into().unwrap_or(u64::MAX));
                }
                if let Some(buffer) = &mut entry.buffer {
                    if let Err(error) = buffer.flush_redaction() {
                        entry.row.diagnostic = Some(error.tool_diagnostic());
                    }
                    buffer.closed = true;
                }
            }
        }
        self.notify();
        if !cleanup_failed
            && !matches!(error, Error::ExecutionInterrupted)
            && self.cleanup_controller(id).await.is_err()
        {
            tracing::warn!(execution_id = id.0, "execution cleanup remains unresolved");
        }
        self.notify();
    }
    pub fn receipt(&self, id: ExecutionId, include_output: bool) -> Result<String, Error> {
        let capabilities = self.get(id)?.capabilities;
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let entry = state
            .entries
            .get_mut(&id)
            .ok_or_else(|| Error::tool("unknown execution handle"))?;
        self.receipt_from_entry(id, entry, include_output, capabilities)
    }

    fn receipt_from_entry(
        &self,
        id: ExecutionId,
        entry: &mut Entry,
        include_output: bool,
        capabilities: super::ExecutionCapabilities,
    ) -> Result<String, Error> {
        let mut row = entry.row.metadata();
        row.output = entry.buffer.as_ref().map(Buffer::metadata);
        let mut receipt = serde_json::json!({"id":format!("{}/{}",row.session_id,id.0),"execution_id":id,"session_id":row.session_id,"tool":row.tool,"status":row.status,"capabilities":capabilities,"output":row.output,"failure":row.failure});
        if let Some(child) = &entry.row.delegate_child {
            receipt["delegate_child"] = serde_json::to_value(child)?;
        }
        if include_output && !entry.buffer.as_ref().is_some_and(Buffer::is_private) {
            let overhead = serde_json::to_vec(&receipt)?.len();
            let budget = self
                .limits
                .max_tool_output_bytes
                .saturating_sub(overhead + 32);
            let revision = Self::refresh_revision(entry);
            let page = if let Some(buffer) = &entry.buffer {
                buffer.page(id, 0, budget, revision)?
            } else {
                let mut buffer =
                    Buffer::new(OutputOptions::default(), self.limits.max_tool_output_bytes)?;
                if let Some(result) = &entry.row.result {
                    buffer.append(None, result)?;
                }
                buffer.closed = !entry.row.status_projection().is_pending();
                buffer.page(id, 0, budget, revision)?
            };
            receipt["initial"] = serde_json::to_value(page)?;
        }
        let text = serde_json::to_string(&receipt)?;
        crate::budget::check(
            crate::LimitKind::ToolOutput,
            text.len(),
            self.limits.max_tool_output_bytes,
        )?;
        Ok(text)
    }
    pub fn read(&self, id: ExecutionId, cursor: u64, maximum: usize) -> Result<OutputPage, Error> {
        self.read_capped(id, cursor, maximum, None)
    }
    pub(crate) fn read_capped(
        &self,
        id: ExecutionId,
        cursor: u64,
        maximum: usize,
        content_limit: Option<usize>,
    ) -> Result<OutputPage, Error> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let entry = state
            .entries
            .get_mut(&id)
            .ok_or_else(|| Error::tool("unknown execution handle"))?;
        let revision = Self::refresh_revision(entry);
        if let Some(buffer) = &entry.buffer {
            buffer.page_capped(id, cursor, maximum, revision, content_limit)
        } else {
            let mut buffer =
                Buffer::new(OutputOptions::default(), self.limits.max_tool_output_bytes)?;
            if let Some(result) = &entry.row.result {
                buffer.append(None, result)?;
            }
            buffer.closed = !entry.row.status_projection().is_pending();
            buffer.page_capped(id, cursor, maximum, revision, content_limit)
        }
    }
    /// Invocation admission/progress owns these lifecycle transitions; output reads
    /// do not. Completed executions remain terminal and notifications refresh the
    /// registry's revision/notice projection after releasing the owner lock.
    pub fn mark(&self, id: ExecutionId, status: ExecutionStatus) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = state.entries.get_mut(&id)
            && entry.row.status_projection().is_pending()
        {
            entry.row.status = status;
        }
        drop(state);
        self.notify();
    }
    pub async fn write(
        &self,
        id: ExecutionId,
        data: String,
        eof: bool,
        cancellation: CancellationToken,
    ) -> Result<(), Error> {
        let controller = {
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let entry = state
                .entries
                .get(&id)
                .filter(|e| e.adopted && !e.released)
                .ok_or_else(|| Error::tool("unknown or unavailable execution"))?;
            entry
                .controller
                .clone()
                .ok_or_else(|| Error::tool("execution has no input controller"))?
        };
        if !controller.capabilities().input {
            return Err(Error::tool("execution does not support input"));
        }
        controller.write(data, eof, cancellation).await
    }
    pub async fn cancel(&self, id: ExecutionId) -> Result<(), Error> {
        let controller = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let entry = state
                .entries
                .get_mut(&id)
                .ok_or_else(|| Error::tool("unknown execution handle"))?;
            if !entry.row.status_projection().is_pending() {
                return Err(Error::tool("execution is already terminal"));
            }
            entry.row.status = ExecutionStatus::Cancelling;
            if !entry.adopted {
                entry.cancellation.cancel();
            }
            entry.controller.clone()
        };
        self.notify();
        if let Some(controller) = controller {
            let timeout = std::time::Duration::from_millis(self.limits.lifecycle_timeout_ms);
            match tokio::time::timeout(
                timeout,
                AssertUnwindSafe(controller.cancel(CancellationToken::new())).catch_unwind(),
            )
            .await
            {
                Ok(Ok(result)) => result?,
                _ => {
                    let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                    if let Some(entry) = state.entries.get_mut(&id) {
                        entry.row.status = ExecutionStatus::Interrupted;
                        entry.row.diagnostic = Some(Error::ExecutionInterrupted.tool_diagnostic());
                        if let Some(buffer) = &mut entry.buffer {
                            buffer.closed = true;
                        }
                    }
                    drop(state);
                    self.notify();
                    return Err(Error::ExecutionInterrupted);
                }
            }
        }
        Ok(())
    }
    pub async fn release(
        &self,
        id: ExecutionId,
        cancellation: CancellationToken,
    ) -> Result<(), Error> {
        {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let entry = state
                .entries
                .get(&id)
                .ok_or_else(|| Error::tool("unknown execution handle"))?;
            if entry.row.status_projection().is_pending() {
                return Err(Error::tool("cancel running execution before release"));
            }
        }
        tokio::select! {biased;_=cancellation.cancelled()=>return Err(Error::Cancelled),result=self.cleanup_controller(id)=>{result?;}}
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(entry) = state.entries.get_mut(&id) {
            entry.released = true;
            entry.redaction = super::InvocationRedaction::default();
            if let Some(buffer) = &mut entry.buffer {
                buffer.closed = true;
                buffer.clear();
                buffer.install_redaction(super::InvocationRedaction::default());
            }
        }
        drop(state);
        self.notify();
        Ok(())
    }
    pub(super) fn abandon(self: &Arc<Self>, id: ExecutionId, nonce: u64) {
        let active = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let Some(entry) = state
                .entries
                .get_mut(&id)
                .filter(|entry| entry.nonce == nonce && !entry.adopted)
            else {
                return;
            };
            if let Some(buffer) = &mut entry.buffer {
                buffer.closed = true;
            }
            let active = entry
                .worker
                .as_ref()
                .is_some_and(|worker| !worker.is_finished());
            if !active {
                // A direct caller can still return Final after discarding its
                // prepared buffer. Its cancellation token belongs to the caller;
                // resource cleanup must not cancel subsequent direct calls.
                if entry.row.status_projection().is_pending() {
                    entry.row.status = ExecutionStatus::Interrupted;
                }
            }
            active
        };
        if !active && self.request_cleanup(id).is_err() {
            tracing::warn!(
                execution_id = id.0,
                "unadopted output cleanup remains owned"
            );
        }
        self.notify();
    }
    pub async fn close(&self) -> Result<(), Error> {
        let (ids, workers) = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.closed = true;
            let mut workers = Vec::new();
            let ids = state
                .entries
                .iter_mut()
                .map(|(id, e)| {
                    e.cancellation.cancel();
                    if let Some(worker) = e.worker.take() {
                        workers.push(worker);
                    }
                    *id
                })
                .collect::<Vec<_>>();
            (ids, workers)
        };
        for id in &ids {
            let pending = self.get(*id)?.status.is_pending();
            if pending {
                let _ = self.cancel(*id).await;
            }
        }
        for worker in workers {
            let _ = worker.await;
        }
        let mut first = None;
        for id in ids {
            let cleaned = match self.cleanup_controller(id).await {
                Ok(()) => true,
                Err(error) => {
                    first.get_or_insert(error);
                    false
                }
            };
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(entry) = state.entries.get_mut(&id) {
                if entry.row.status_projection().is_pending() {
                    entry.row.status = ExecutionStatus::Interrupted;
                }
                if let Some(buffer) = &mut entry.buffer {
                    buffer.closed = true;
                    if cleaned && buffer.is_private() {
                        buffer.clear_private();
                        entry.released = true;
                    }
                } else {
                    entry.released = entry.controller.is_none()
                        && entry.subordinate_cleanups.is_empty()
                        && !entry.exposed;
                }
            }
        }
        self.notify();
        if let Some(error) = first {
            Err(error)
        } else {
            Ok(())
        }
    }
    pub fn abort(&self) {
        let ids = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.closed = true;
            state
                .entries
                .iter_mut()
                .map(|(id, entry)| {
                    entry.cancellation.cancel();
                    if let Some(worker) = &entry.worker {
                        worker.abort();
                    }
                    if entry.row.status_projection().is_pending() {
                        entry.row.status = ExecutionStatus::Interrupted;
                    }
                    if let Some(buffer) = &mut entry.buffer {
                        buffer.closed = true;
                    }
                    *id
                })
                .collect::<Vec<_>>()
        };
        for id in ids {
            if self.request_cleanup(id).is_err() {
                tracing::warn!(
                    execution_id = id.0,
                    "aborted execution cleanup remains owned"
                );
            }
        }
        self.notify();
    }
}

#[cfg(test)]
mod admission_claim_tests {
    use super::*;
    #[tokio::test]
    async fn host_receipt_survives_cancellation_guest_trap_and_recovery() {
        for error in [
            Error::Cancelled,
            Error::wasm("guest exhausted fuel after Host publication"),
        ] {
            let registry = ExecutionRegistry::new(Limits::default());
            let token = CancellationToken::new();
            let id = registry
                .admit_detached(
                    ExecutionSnapshot::new(
                        "owner".into(),
                        "exact-call".into(),
                        "arbitrary-name".into(),
                        "agent".into(),
                        None,
                    ),
                    token.clone(),
                )
                .unwrap();
            let progress = ExecutionProgress {
                registry: Arc::downgrade(&registry),
                id,
            };
            progress.reserve_mutation_receipt(4096).unwrap();
            let receipt=serde_json::json!({"source":"host_file_mutation","receipts":[{"operation":1,"status":"committed","paths":["a"],"outcome":{"version":"a".repeat(64),"destination_written":true}},{"operation":2,"status":"uncertain","paths":["b"]},{"operation":3,"status":"not_started","paths":["c"]}],"replay_safe":false}).to_string();
            progress.retain_mutation_receipt(&receipt).unwrap();
            token.cancel();
            registry.finish_error(id, &error).await;
            registry
                .retain_invocation_result(id, &error.tool_diagnostic().result_json())
                .unwrap();
            assert_eq!(
                registry
                    .recovery_snapshot()
                    .into_iter()
                    .find(|row| row.execution_id == id)
                    .unwrap()
                    .invocation_result
                    .as_deref(),
                Some(receipt.as_str())
            );
            let summary: serde_json::Value =
                serde_json::from_str(&registry.error_result_with_effects(id, &error)).unwrap();
            assert_eq!(summary["mutation_effects"]["committed"], 1);
            assert_eq!(summary["mutation_effects"]["uncertain"], true);
            let recovered = ExecutionRegistry::new(Limits::default());
            recovered.restore(&registry.recovery_snapshot()).unwrap();
            assert_eq!(
                recovered
                    .recovery_snapshot()
                    .into_iter()
                    .find(|row| row.execution_id == id)
                    .unwrap()
                    .invocation_result
                    .as_deref(),
                Some(receipt.as_str())
            );
        }
    }
    #[tokio::test]
    async fn guest_final_cannot_replace_host_owned_mutation_receipt() {
        let registry = ExecutionRegistry::new(Limits::default());
        let id = registry
            .admit_detached(
                ExecutionSnapshot::new(
                    "owner".into(),
                    "exact-call".into(),
                    "arbitrary-name".into(),
                    "agent".into(),
                    None,
                ),
                CancellationToken::new(),
            )
            .unwrap();
        let progress = ExecutionProgress {
            registry: Arc::downgrade(&registry),
            id,
        };
        progress.reserve_mutation_receipt(4096).unwrap();
        let receipt=serde_json::json!({"source":"host_file_mutation","receipts":[{"operation":1,"status":"committed","paths":["a"]}],"replay_safe":false}).to_string();
        progress.retain_mutation_receipt(&receipt).unwrap();
        assert_eq!(
            registry
                .resolve(id, Ok(ToolOutput::text("guest summary")))
                .await
                .unwrap()
                .content,
            "guest summary"
        );
        registry
            .retain_invocation_result(id, "guest summary")
            .unwrap();
        let metadata = registry.get(id).unwrap();
        assert_eq!(metadata.status, ExecutionStatus::Succeeded);
        assert!(metadata.result.is_none());
        assert!(metadata.invocation_result.is_none());
        let row = registry
            .recovery_snapshot()
            .into_iter()
            .find(|row| row.execution_id == id)
            .unwrap();
        assert_eq!(row.result.as_deref(), Some("guest summary"));
        assert_eq!(row.invocation_result.as_deref(), Some(receipt.as_str()));
        let recovered = ExecutionRegistry::new(Limits::default());
        recovered.restore(&registry.recovery_snapshot()).unwrap();
        let restored = recovered
            .recovery_snapshot()
            .into_iter()
            .find(|row| row.execution_id == id)
            .unwrap();
        assert_eq!(restored.invocation_result, row.invocation_result);
    }
    #[test]
    fn mutation_receipt_reservation_rejects_over_capacity_before_effects() {
        let registry = ExecutionRegistry::new(Limits::default());
        let progress = ExecutionProgress {
            registry: Arc::downgrade(&registry),
            id: ExecutionId(1),
        };
        assert!(
            progress
                .reserve_mutation_receipt(MAX_EXECUTION_INSPECTION_BYTES + 1)
                .is_err()
        );
    }
    #[test]
    fn progress_port_requires_exact_registry_and_execution_even_when_ids_overlap() {
        let owner = ExecutionRegistry::new(Limits::default());
        let foreign = ExecutionRegistry::new(Limits::default());
        let progress = ExecutionProgress {
            registry: Arc::downgrade(&owner),
            id: ExecutionId(1),
        };
        assert!(progress.belongs_to(&owner, ExecutionId(1)));
        assert!(!progress.belongs_to(&owner, ExecutionId(2)));
        assert!(!progress.belongs_to(&foreign, ExecutionId(1)));
    }
    #[test]
    fn concurrent_proof_issuance_claim_has_one_winner_and_missing_record_fails() {
        let registry = ExecutionRegistry::new(Limits::default());
        assert!(registry.claim_admission(ExecutionId(999)).is_err());
        let id = registry
            .admit_detached(
                ExecutionSnapshot::new(
                    "owner".into(),
                    "call".into(),
                    "tool".into(),
                    "agent".into(),
                    None,
                ),
                CancellationToken::new(),
            )
            .unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let wins = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let registry = registry.clone();
                    let barrier = barrier.clone();
                    scope.spawn(move || {
                        barrier.wait();
                        usize::from(registry.claim_admission(id).unwrap())
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .sum::<usize>()
        });
        assert_eq!(wins, 1);
        registry.mark(id, ExecutionStatus::Waiting);
        assert!(!registry.claim_admission(id).unwrap());
    }
    #[tokio::test]
    async fn installing_worker_preserves_reserved_execution_admission_claim() {
        let registry = ExecutionRegistry::new(Limits::default());
        let id = registry
            .admit_detached(
                ExecutionSnapshot::new(
                    "owner".into(),
                    "call".into(),
                    "tool".into(),
                    "agent".into(),
                    None,
                ),
                CancellationToken::new(),
            )
            .unwrap();
        assert!(registry.claim_admission(id).unwrap());
        let mut worker = registry
            .start(
                registry.get(id).unwrap(),
                CancellationToken::new(),
                |_| async { Ok(ToolOutput::text("done")) },
            )
            .unwrap();
        assert!(!registry.claim_admission(id).unwrap());
        worker.activate().unwrap();
        worker.wait().await.unwrap();
        worker.detach();
        registry.close().await.unwrap();
    }
}
