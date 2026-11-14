//! Session-owned stores. The catalog and Host never retain Session state.
//! The map lock only selects/drains slots; each worker serializes its own store.

use super::{Compiled, Instance};
use crate::Error;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
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
    worker: Arc<tokio::sync::Mutex<Worker>>,
}

#[derive(Default)]
pub(super) struct Worker {
    initialization_permitted: bool,
    instance: Option<Instance>,
    poisoned: bool,
    failure_evidence: Option<u64>,
}

impl Worker {
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
        state.next_generation = state
            .next_generation
            .checked_add(1)
            .ok_or_else(|| Error::lifecycle("WASM instance generation exhausted"))?;
        let slot = Arc::new(Slot {
            _compiled: compiled.clone(),
            generation: state.next_generation,
            cancellation: CancellationToken::new(),
            worker: Arc::new(tokio::sync::Mutex::new(Worker {
                initialization_permitted: state.initialization_complete,
                ..Worker::default()
            })),
        });
        state.slots.insert(key, slot.clone());
        Ok(slot)
    }

    pub(super) fn reset(&self, compiled: &Arc<Compiled>) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(slot) = state.slots.remove(&(Arc::as_ptr(compiled) as usize)) {
            slot.cancellation.cancel();
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
                slot.cancellation.cancel();
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

    pub(super) fn run<T>(
        &self,
        mut worker: tokio::sync::OwnedMutexGuard<Worker>,
        call: impl FnOnce(&mut Option<Instance>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        if self.cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if worker.poisoned {
            use crate::observation::diagnostics::{
                DiagnosticCode, DiagnosticLayer, DiagnosticPhase, EffectKnowledge, ToolDiagnostic,
            };
            let mut diagnostic = ToolDiagnostic::new(
                DiagnosticCode::WasmPoisoned,
                DiagnosticLayer::Runtime,
                DiagnosticPhase::Validation,
            );
            // Only this rejected call is effect-free. The earlier trap can have
            // admitted Host effects; do not recreate the instance or replay it.
            diagnostic.effects = EffectKnowledge::NotStarted;
            diagnostic.evidence_id = worker.failure_evidence;
            return Err(Error::Diagnostic(diagnostic));
        }
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
            .or_else(|| {
                result
                    .as_ref()
                    .err()
                    .and_then(crate::observation::wasm::failure_id)
            });
        worker.poisoned = worker
            .instance
            .as_ref()
            .is_some_and(|instance| instance.poisoned)
            || worker.failure_evidence.is_some()
            || matches!(&result, Err(Error::Wasm(_)));
        if worker.poisoned {
            worker.instance = None;
        }
        if self.cancellation.is_cancelled() {
            worker.instance = None;
            return Err(Error::Cancelled);
        }
        result
    }
}
