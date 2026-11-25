//! Session-owned stores. The catalog and Host never retain Session state.
//! The map lock only selects/drains slots; each worker serializes its own store.

use super::{Compiled, Instance};
use crate::Error;
use crate::observation::wasm::FailureEvidence;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub(crate) struct EpochInstances {
    state: Mutex<EpochState>,
}

#[derive(Default)]
struct EpochState {
    next_generation: u64,
    initialization_complete: bool,
    fresh_initialization_required: bool,
    closed: bool,
    slots: HashMap<usize, Arc<Slot>>,
}

pub(super) struct Slot {
    // Retaining the compiled allocation makes its pointer a stable cache key.
    _compiled: Arc<Compiled>,
    pub(super) generation: u64,
    pub(super) cancellation: CancellationToken,
    epoch_ended: CancellationToken,
    predecessor: Option<u64>,
    failed: AtomicBool,
    drivers: Mutex<Vec<Weak<super::driver::Cleanup>>>,
    pub(super) web: Arc<super::web_transport::SlotOwner>,
    worker: Arc<tokio::sync::Mutex<Worker>>,
}

#[derive(Default)]
pub(super) struct Worker {
    initialization_permitted: bool,
    instance: Option<Instance>,
    poisoned: bool,
    failure_evidence: Option<FailureEvidence>,
    replaceable: bool,
    retirement: Option<Retirement>,
}

struct Retirement {
    cleanup: super::RecoveryCleanup,
    drivers: Vec<Arc<super::driver::Cleanup>>,
}

impl Retirement {
    async fn acknowledge(&self, slot: &Slot) -> bool {
        self.cleanup
            .acknowledge(&slot.web, &self.drivers, &slot.epoch_ended)
            .await
    }
}

impl Worker {
    pub(super) fn poisoned(&self) -> Result<(), Error> {
        if !self.poisoned {
            return Ok(());
        }
        use crate::observation::diagnostics::{
            DiagnosticCode, DiagnosticLayer, DiagnosticPhase, EffectKnowledge, ToolDiagnostic,
        };
        let mut diagnostic = ToolDiagnostic::new(
            DiagnosticCode::WasmPoisoned,
            DiagnosticLayer::Runtime,
            DiagnosticPhase::Validation,
        );
        diagnostic.effects = EffectKnowledge::NotStarted;
        if let Some(evidence) = self.failure_evidence {
            diagnostic.evidence_id = Some(evidence.id);
            diagnostic.wasm_failure = evidence.cause;
        }
        Err(Error::Diagnostic(diagnostic))
    }

    pub(super) async fn acknowledge_retirement(&mut self, slot: &Slot) -> bool {
        if let Some(retirement) = &self.retirement
            && retirement.acknowledge(slot).await
        {
            self.replaceable = true;
            self.retirement = None;
        }
        self.replaceable
    }

    pub(super) fn ready(&self) -> bool {
        self.initialization_permitted
            || self
                .instance
                .as_ref()
                .is_some_and(|instance| instance.init_ready)
    }
}

impl EpochInstances {
    pub(crate) fn require_fresh_initialization(&self) {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .fresh_initialization_required = true;
    }
    pub(crate) fn fresh_initialization_required(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .fresh_initialization_required
    }
    pub(crate) fn initialization_complete(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .initialization_complete
    }
    pub(crate) fn permit_initialized_session(&self) {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .initialization_complete = true;
    }
    pub(super) fn slot(&self, compiled: &Arc<Compiled>) -> Result<Arc<Slot>, Error> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.closed {
            return Err(Error::Cancelled);
        }
        let key = Arc::as_ptr(compiled) as usize;
        if let Some(slot) = state.slots.get(&key) {
            return Ok(slot.clone());
        }
        Self::insert(&mut state, compiled, None)
    }

    fn insert(
        state: &mut EpochState,
        compiled: &Arc<Compiled>,
        predecessor: Option<u64>,
    ) -> Result<Arc<Slot>, Error> {
        state.next_generation = state
            .next_generation
            .checked_add(1)
            .ok_or_else(|| Error::lifecycle("WASM instance generation exhausted"))?;
        let slot = Arc::new(Slot {
            _compiled: compiled.clone(),
            generation: state.next_generation,
            cancellation: CancellationToken::new(),
            epoch_ended: CancellationToken::new(),
            predecessor,
            failed: AtomicBool::new(false),
            drivers: Mutex::new(Vec::new()),
            web: Default::default(),
            worker: Arc::new(tokio::sync::Mutex::new(Worker {
                initialization_permitted: state.initialization_complete || predecessor.is_some(),
                ..Worker::default()
            })),
        });
        state
            .slots
            .insert(Arc::as_ptr(compiled) as usize, slot.clone());
        Ok(slot)
    }

    /// Publish only the exact failed slot's successor, never a replacement for a
    /// later trap or a new Context epoch on behalf of an old invocation.
    pub(super) fn replace(
        &self,
        compiled: &Arc<Compiled>,
        failed: &Arc<Slot>,
    ) -> Result<Option<Arc<Slot>>, Error> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.closed || failed.epoch_ended.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let Some(current) = state.slots.get(&(Arc::as_ptr(compiled) as usize)) else {
            return Err(Error::Cancelled);
        };
        if Arc::ptr_eq(current, failed) {
            return Self::insert(&mut state, compiled, Some(failed.generation)).map(Some);
        }
        Ok((current.predecessor == Some(failed.generation)).then(|| current.clone()))
    }

    pub(super) fn reset(&self, compiled: &Arc<Compiled>) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(slot) = state.slots.remove(&(Arc::as_ptr(compiled) as usize)) {
            slot.retire();
        }
    }

    /// Invalidate in-flight workers without waiting for their store locks.
    /// Removed slots are never reinserted, even if a stale worker completes.
    pub(crate) fn end(&self) {
        self.clear(false);
    }

    /// Permanently reject acquisition, including after subsequent epoch ends.
    pub(crate) fn close(&self) {
        self.clear(true);
    }

    fn clear(&self, close: bool) {
        let slots = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.closed |= close;
            let slots = std::mem::take(&mut state.slots);
            // Cancellation belongs inside the map boundary: a new generation
            // cannot be acquired until every old slot has been invalidated.
            for slot in slots.values() {
                slot.retire();
            }
            slots
        };
        drop(slots);
    }
}

impl Drop for EpochInstances {
    fn drop(&mut self) {
        self.close();
    }
}

impl Slot {
    fn retire(&self) {
        self.epoch_ended.cancel();
        self.cancellation.cancel();
        self.web.retire();
    }

    pub(super) fn try_lease(&self) -> Option<tokio::sync::OwnedMutexGuard<Worker>> {
        if self.cancellation.is_cancelled() {
            return None;
        }
        self.worker.clone().try_lock_owned().ok()
    }
    pub(super) async fn lease(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<tokio::sync::OwnedMutexGuard<Worker>, Error> {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(Error::Cancelled),
            _ = self.cancellation.cancelled() => Err(Error::Cancelled),
            result = self.worker.clone().lock_owned() => Ok(result),
        }
    }

    pub(super) fn already_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    pub(super) async fn tool_lease(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<tokio::sync::OwnedMutexGuard<Worker>, Error> {
        // Even on a poisoned generation, join the worker to read its settled
        // cleanup and evidence. A lifecycle transition always wins.
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(Error::Cancelled),
            _ = self.epoch_ended.cancelled() => Err(Error::Cancelled),
            result = self.worker.clone().lock_owned() => Ok(result),
        }
    }

    pub(super) fn track_driver(&self, driver: &Arc<super::driver::Cleanup>) {
        let mut drivers = self
            .drivers
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        drivers.retain(|driver| driver.strong_count() != 0);
        drivers.push(Arc::downgrade(driver));
    }

    pub(super) fn run<T>(
        &self,
        mut worker: tokio::sync::OwnedMutexGuard<Worker>,
        call: impl FnOnce(&mut Option<Instance>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        if self.cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        worker.poisoned()?;
        let established = worker.ready();
        // Remain poisoned on unwinding; a guest-export Err is a normal result.
        worker.poisoned = true;
        let result = call(&mut worker.instance);
        let initialized = worker
            .instance
            .as_ref()
            .is_some_and(|instance| instance.init_ready);
        worker.initialization_permitted |= initialized;
        worker.failure_evidence = worker
            .instance
            .as_ref()
            .and_then(|instance| instance.failure_evidence)
            .or_else(|| result.as_ref().err().and_then(FailureEvidence::from_error));
        worker.poisoned = worker
            .instance
            .as_ref()
            .is_some_and(|instance| instance.poisoned)
            || worker.failure_evidence.is_some()
            || matches!(&result, Err(Error::Wasm(_)));
        if worker.poisoned {
            self.failed.store(true, Ordering::Release);
            self.cancellation.cancel();
            self.web.retire();
            let cleanup = worker
                .instance
                .take()
                .and_then(|mut instance| instance.recovery_cleanup.take());
            if let Some(cleanup) = cleanup {
                let drivers = {
                    let mut drivers = self
                        .drivers
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    std::mem::take(&mut *drivers)
                        .into_iter()
                        .filter_map(|driver| driver.upgrade())
                        .collect::<Vec<_>>()
                };
                // The old Store has been discarded. Join only exact owners,
                // with a single aggregate deadline; uncertainty fails closed.
                let retirement = Retirement { cleanup, drivers };
                let acknowledged = retirement
                    .cleanup
                    .runtime
                    .clone()
                    .block_on(retirement.acknowledge(self));
                worker.replaceable = established && acknowledged;
                if established
                    && !acknowledged
                    && !retirement.cleanup.process_launch_uncertain
                    && !self.epoch_ended.is_cancelled()
                {
                    worker.retirement = Some(retirement);
                }
            }
        }
        if self.epoch_ended.is_cancelled() {
            worker.instance = None;
            return Err(Error::Cancelled);
        }
        result
    }
}
