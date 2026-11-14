//! Owned notification senders keep Runtime waiters live without polling. Route
//! loss wakes them but never invents completion or replays an effect.
use super::*;
use microcrab_host_api::process::{ProcessHandle, ProcessState};
use microcrab_host_protocol::{HostRequest, ProcessStarted};
use tokio::sync::watch;

pub(crate) struct Entry {
    pub pid: Option<u32>,
    pub changes: watch::Sender<u64>,
    revision: u64,
    pub terminal: bool,
    pub release_pending: bool,
    pub streams_taken: [bool; 2],
}
pub(crate) struct StartOwnership {
    pub cancelled: bool,
    pub receipt: Option<ProcessStarted>,
}
#[derive(Default)]
pub(crate) struct Processes {
    pub starts: Mutex<HashMap<String, StartOwnership>>,
    cleanup_gate: tokio::sync::Mutex<()>,
    pub entries: Mutex<HashMap<ProcessHandle, Entry>>,
    early: Mutex<VecDeque<(ProcessHandle, u64)>>,
}
impl Processes {
    pub fn notify_route_change(&self) {
        for entry in self
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
        {
            entry
                .changes
                .send_modify(|value| *value = value.wrapping_add(1));
        }
    }
    pub fn notify(&self, id: ProcessHandle, revision: u64) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = entries.get_mut(&id) {
            if revision > entry.revision {
                entry.revision = revision;
                entry
                    .changes
                    .send_modify(|value| *value = value.wrapping_add(1));
            }
        } else {
            let mut early = self.early.lock().unwrap_or_else(|e| e.into_inner());
            if let Some((_, previous)) = early.iter_mut().find(|(handle, _)| *handle == id) {
                *previous = (*previous).max(revision);
            } else {
                early.push_back((id, revision));
                while early.len() > 1024 {
                    early.pop_front();
                }
            }
        }
    }
    pub fn insert(&self, receipt: ProcessStarted) -> Result<ProcessHandle, ToolError> {
        if receipt.pid == Some(0)
            || receipt.handle.0.0 == 0
            || receipt.handle.0.0 > (1_u64 << 53) - 1
        {
            return Err(unavailable("invalid Host process receipt"));
        }
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if entries.contains_key(&receipt.handle) {
            return Err(unavailable("Host reused a live job identity"));
        }
        let mut early = self.early.lock().unwrap_or_else(|e| e.into_inner());
        let revision = early
            .iter()
            .find(|(id, _)| *id == receipt.handle)
            .map_or(receipt.revision, |(_, revision)| {
                (*revision).max(receipt.revision)
            });
        early.retain(|(id, _)| *id != receipt.handle);
        let (changes, _receiver) = watch::channel(0);
        // Completion can precede admission delivery; leave an observable change
        // for a waiter created from this receipt.
        if revision != 0 {
            changes.send_replace(1);
        }
        entries.insert(
            receipt.handle,
            Entry {
                pid: receipt.pid,
                changes,
                revision,
                terminal: false,
                release_pending: false,
                streams_taken: [false; 2],
            },
        );
        Ok(receipt.handle)
    }
}
impl RemoteHost {
    pub(crate) fn cancel_start(&self, operation_id: &str) {
        let receipt = {
            let mut starts = self
                .processes
                .starts
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let Some(start) = starts.get_mut(operation_id) else {
                return;
            };
            start.cancelled = true;
            start.receipt.clone()
        };
        if let Some(receipt) = receipt {
            self.own_cancelled_start(receipt);
        }
    }
    pub(crate) fn observe_start_reply(
        &self,
        operation_id: &str,
        result: &microcrab_host_protocol::HostReply,
    ) {
        let known = !matches!(result, Err(error) if matches!(error.code, microcrab_host_protocol::ErrorCode::OutcomeUnknown | microcrab_host_protocol::ErrorCode::Disconnected | microcrab_host_protocol::ErrorCode::LeaseExpired));
        if !known {
            return;
        }
        let cleanup = {
            let mut starts = self
                .processes
                .starts
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let Some(start) = starts.get_mut(operation_id) else {
                return;
            };
            match result {
                Ok(value) => {
                    let Ok(receipt) = serde_json::from_value::<ProcessStarted>(value.clone())
                    else {
                        return;
                    };
                    if receipt.pid == Some(0)
                        || receipt.handle.0.0 == 0
                        || receipt.handle.0.0 > (1_u64 << 53) - 1
                    {
                        return;
                    }
                    start.receipt = Some(receipt.clone());
                    if start.cancelled { Some(receipt) } else { None }
                }
                Err(_) => {
                    starts.remove(operation_id);
                    None
                }
            }
        };
        let digest = self
            .unresolved
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(operation_id);
        let scope = digest
            .as_ref()
            .and_then(|_| self.scope.lock().unwrap_or_else(|e| e.into_inner()).clone());
        if let (Some(payload_digest), Some(scope)) = (digest, scope) {
            self.remember_receipt(&OperationReceipt {
                operation_id: operation_id.into(),
                payload_digest,
                scope,
                state: microcrab_host_protocol::ReceiptState::Completed(result.clone()),
            });
        }
        if let Some(receipt) = cleanup {
            self.own_cancelled_start(receipt);
        }
    }
    fn own_cancelled_start(&self, receipt: ProcessStarted) {
        let id = receipt.handle;
        let owned = self
            .processes
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(&id);
        if !owned && self.processes.insert(receipt).is_err() {
            return;
        }
        if let Some(entry) = self
            .processes
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(&id)
        {
            entry.release_pending = true;
        }
        if !self.is_reconciling() {
            self.spawn_process_cleanup();
        }
    }
    fn spawn_process_cleanup(&self) {
        let Some(host) = self.weak_self.upgrade() else {
            return;
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let generation = self.generation.load(Ordering::SeqCst);
            runtime.spawn(async move {
                // The call owner records exact wire failures. Do not publish a
                // converted error after replacement, or overwrite that evidence.
                let result = host.cleanup_releases_for_generation(Some(generation)).await;
                if let Err(error) = result {
                    host.record_cleanup_failure(generation, &error);
                }
            });
        }
    }
    pub(crate) async fn reconcile_process_inventory(
        &self,
        generation: u64,
    ) -> Result<(), ToolError> {
        if !self
            .descriptor
            .supported_primitives
            .iter()
            .any(|name| name == "process.spawn" || name == "shell.launch")
        {
            return Ok(());
        }
        // A delivered start receipt still belongs to its caller until the same
        // admission transaction installs its handle (or records cancellation).
        // Inventory joins that transaction before taking its snapshot.
        let _admission = self.process_start_admission.lock().await;
        let inventory: Vec<ProcessStarted> = serde_json::from_value(
            self.call_for_generation(
                HostRequest::ProcessInventory,
                CancellationToken::new(),
                Some(generation),
            )
            .await?,
        )
        .map_err(|_| {
            self.reconciliation_validation_error(
                generation,
                microcrab_host_protocol::ErrorCode::InvalidRequest,
                "invalid Host process inventory",
                None,
            )
        })?;
        if inventory.len() > 1024 {
            return Err(self.reconciliation_validation_error(
                generation,
                microcrab_host_protocol::ErrorCode::Limit,
                "Host process inventory exceeds handle bound",
                None,
            ));
        }
        let mut seen = std::collections::HashSet::new();
        for receipt in inventory {
            if receipt.handle.0.0 == 0
                || receipt.handle.0.0 > (1_u64 << 53) - 1
                || receipt.pid == Some(0)
                || !seen.insert(receipt.handle)
            {
                return Err(self.reconciliation_validation_error(
                    generation,
                    microcrab_host_protocol::ErrorCode::InvalidRequest,
                    "invalid Host process inventory identity",
                    None,
                ));
            }
            let known_pid = self
                .processes
                .entries
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&receipt.handle)
                .map(|entry| entry.pid);
            if let Some(pid) = known_pid {
                if pid != receipt.pid {
                    return Err(self.reconciliation_validation_error(
                        generation,
                        microcrab_host_protocol::ErrorCode::OperationConflict,
                        "Host process inventory PID changed",
                        None,
                    ));
                }
                self.processes.notify(receipt.handle, receipt.revision);
                continue;
            }
            // A reconstructed backend has no caller owner for this handle.
            // Positively identify it, retain cleanup ownership, then release it.
            let id = receipt.handle;
            self.processes.insert(receipt)?;
            if let Some(entry) = self
                .processes
                .entries
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get_mut(&id)
            {
                entry.release_pending = true;
            }
        }
        Ok(())
    }
    pub(crate) async fn cleanup_releases(&self) -> Result<(), ToolError> {
        self.cleanup_releases_for_generation(None).await
    }
    pub(crate) async fn cleanup_releases_for_generation(
        &self,
        generation: Option<u64>,
    ) -> Result<(), ToolError> {
        let _gate = self.processes.cleanup_gate.lock().await;
        let pending: Vec<_> = self
            .processes
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|(_, entry)| entry.release_pending)
            .map(|(id, _)| *id)
            .collect();
        for id in pending {
            // This is a new, idempotent cleanup of a positively identified handle.
            // The original start payload is never replayed.
            self.call_for_generation(
                HostRequest::ProcessRelease { handle: id },
                CancellationToken::new(),
                generation,
            )
            .await?;
            self.processes
                .entries
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&id);
            self.processes
                .starts
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .retain(|_, start| {
                    start
                        .receipt
                        .as_ref()
                        .is_none_or(|receipt| receipt.handle != id)
                });
        }
        if self
            .processes
            .starts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .any(|start| start.cancelled && start.receipt.is_none())
        {
            return Err(unavailable(
                "cancelled Host start cleanup unresolved; original process identity is unknown",
            ));
        }
        Ok(())
    }
    pub(crate) fn check_process(&self, handle: ProcessHandle) -> Result<(), ToolError> {
        self.processes
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&handle)
            .filter(|entry| !entry.release_pending)
            .map(|_| ())
            .ok_or_else(|| {
                ToolError::Host(microcrab_host_api::HostError::known(
                    microcrab_host_api::ErrorCategory::NotFound,
                    "unknown Host process",
                ))
            })
    }
    pub(crate) async fn start_remote_process(
        &self,
        request: HostRequest,
        cancel: CancellationToken,
    ) -> Result<ProcessHandle, ToolError> {
        let operation = match &request {
            HostRequest::ProcessSpawn(_) => "process.spawn",
            HostRequest::ShellLaunch(_) => "shell.launch",
            _ => {
                return Err(ToolError::invalid_request("expected process admission"));
            }
        };
        if !self.capabilities.supports(operation) {
            return Err(ToolError::unsupported(operation));
        }
        // Inventory reconciliation needs this same admission mutex. Waiting for
        // its pass while holding the mutex would prevent that pass completing.
        let generation = self.await_ready(&cancel).await?;
        let _admission = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(ToolError::Cancelled),
            result = tokio::time::timeout(OPERATION_TIMEOUT, self.process_start_admission.lock()) => result.map_err(|_| unavailable("Host process admission deadline exceeded"))?,
        };
        // A pass can start after the preflight while the mutex is contended.
        // Never wait under the mutex: reject at the exact-generation send gate.
        let receipt: ProcessStarted = payload::decode(
            self.call_for_generation(request, cancel, Some(generation))
                .await?,
        )?;
        let handle = self.processes.insert(receipt)?;
        self.processes
            .starts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|_, start| {
                start
                    .receipt
                    .as_ref()
                    .is_none_or(|receipt| receipt.handle != handle)
            });
        Ok(handle)
    }
    pub(crate) fn release_remote_process(&self, handle: ProcessHandle) {
        if let Some(entry) = self
            .processes
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(&handle)
        {
            entry.release_pending = true;
            entry
                .changes
                .send_modify(|value| *value = value.wrapping_add(1));
        } else {
            return;
        }
        self.spawn_process_cleanup();
    }
    pub(crate) fn note_process_state(&self, id: ProcessHandle, state: &ProcessState) {
        if let Some(entry) = self
            .processes
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(&id)
        {
            entry.terminal = !matches!(state, ProcessState::Running);
        }
    }
}
