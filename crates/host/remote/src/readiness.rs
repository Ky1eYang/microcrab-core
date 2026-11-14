//! Readiness belongs to the replaceable route. Route -> readiness is the lock order.
//! A pass may clear only the cause revision it observed; late work cannot publish
//! facts about a newer connection or hide uncertainty admitted during that pass.
use super::*;
use microcrab_host_protocol::{ErrorCode, WireError};
use serde_json::{Value, json};

#[derive(Clone, serde::Serialize)]
pub(super) struct Cause {
    code: CauseCode,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    diagnostic_cause: Option<microcrab_host_protocol::HostCause>,
    #[serde(skip_serializing_if = "Option::is_none")]
    operation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
}
impl Cause {
    pub(super) fn new(
        error: WireError,
        operation_id: Option<&str>,
        request_id: Option<&str>,
    ) -> Self {
        Self {
            code: CauseCode::Wire(error.code),
            message: error.message,
            diagnostic_cause: error.cause,
            operation_id: operation_id.map(str::to_owned),
            request_id: request_id.map(str::to_owned),
        }
    }
    pub(super) fn local(code: ErrorCode, message: impl Into<String>) -> Self {
        Self::new(WireError::new(code, message), None, None)
    }
}
#[derive(Clone, serde::Serialize)]
#[serde(untagged)]
enum CauseCode {
    Wire(ErrorCode),
    Local(LocalCode),
}
#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum LocalCode {
    ReconciliationTimeout,
    ReconciliationFailed,
    ProcessCleanupFailed,
    ReconciliationCancelled,
}
impl Cause {
    pub(super) fn phase(code: LocalCode, message: impl Into<String>) -> Self {
        Self {
            code: CauseCode::Local(code),
            message: message.into(),
            diagnostic_cause: None,
            operation_id: None,
            request_id: None,
        }
    }
}
#[derive(Clone, Copy, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Disconnected,
    AwaitingReconciliation,
    Reconciling,
    Ready,
    Unready,
}
#[derive(Clone, serde::Serialize)]
struct TransportWarning {
    code: &'static str,
    message: &'static str,
}
pub(super) struct Readiness {
    generation: u64,
    ready: bool,
    reconciling: bool,
    phase: Phase,
    cause: Option<Cause>,
    transport_warning: Option<TransportWarning>,
    revision: u64,
    pass: u64,
    recovery_requested: bool,
}
impl Default for Readiness {
    fn default() -> Self {
        Self {
            generation: 0,
            ready: false,
            reconciling: false,
            phase: Phase::Disconnected,
            cause: Some(Cause::local(
                ErrorCode::Disconnected,
                "Host is disconnected",
            )),
            transport_warning: None,
            revision: 0,
            pass: 0,
            recovery_requested: false,
        }
    }
}
impl Readiness {
    fn admission_error(&self) -> ToolError {
        match self.cause.as_ref() {
            Some(Cause {
                code: CauseCode::Wire(code),
                message,
                diagnostic_cause,
                ..
            }) => calls::wire_error(
                WireError::new(code.clone(), message.clone()).with_cause(*diagnostic_cause),
            ),
            Some(Cause {
                code: CauseCode::Local(code),
                message,
                ..
            }) => {
                let code = match code {
                    LocalCode::ReconciliationTimeout => "reconciliation_timeout",
                    LocalCode::ReconciliationFailed => "reconciliation_failed",
                    LocalCode::ProcessCleanupFailed => "process_cleanup_failed",
                    LocalCode::ReconciliationCancelled => "reconciliation_cancelled",
                };
                ToolError::Worker(microcrab_host_api::FailureReason::from(format!("Host {code}: {message}")).with_public_reason(match code {
                    "reconciliation_timeout" => "Host reconciliation deadline exceeded; original ownership remains fenced",
                    "reconciliation_failed" => "Host reconciliation failed; request was not admitted",
                    "process_cleanup_failed" => "Host process cleanup failed; original ownership remains fenced",
                    _ => "Host reconciliation was cancelled locally",
                }))
            }
            None if self.reconciling => {
                unavailable("Host reconciliation is in progress; request was not admitted")
            }
            None => unavailable(
                "Host route is awaiting reconciliation; no reconciliation pass is active",
            ),
        }
    }
}

impl RemoteHost {
    /// Wait only for work that is already reconciling on this exact route/pass.
    /// No request identity, mutation ownership or outbound frame exists yet.
    pub(super) async fn await_ready(&self, cancel: &CancellationToken) -> Result<u64, ToolError> {
        let (generation, pass) = {
            let route = self.route.lock().unwrap_or_else(|e| e.into_inner());
            let state = self.readiness.lock().unwrap_or_else(|e| e.into_inner());
            let route = route.as_ref().ok_or_else(|| state.admission_error())?;
            if state.ready {
                return Ok(route.generation);
            }
            if !state.reconciling {
                return Err(state.admission_error());
            }
            (route.generation, state.pass)
        };
        // Reconciliation itself has a 15-second bound. This independent bound
        // also protects a dropped/stalled owner without adding a Turn deadline.
        let deadline = tokio::time::sleep(std::time::Duration::from_secs(15));
        tokio::pin!(deadline);
        loop {
            let changed = self.readiness_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let route = self.route.lock().unwrap_or_else(|e| e.into_inner());
                if !route
                    .as_ref()
                    .is_some_and(|route| route.generation == generation)
                {
                    let state = self.readiness.lock().unwrap_or_else(|e| e.into_inner());
                    if route.is_none() && state.generation == generation {
                        return Err(state.admission_error());
                    }
                    return Err(unavailable(
                        "Host route changed while awaiting reconciliation; request was not admitted",
                    ));
                }
                let state = self.readiness.lock().unwrap_or_else(|e| e.into_inner());
                if state.pass != pass {
                    return Err(unavailable(
                        "Host reconciliation pass changed before request admission",
                    ));
                }
                if state.ready {
                    return Ok(generation);
                }
                if !state.reconciling {
                    return Err(state.admission_error());
                }
            }
            tokio::select! { biased;
                _ = cancel.cancelled() => return Err(ToolError::Cancelled),
                _ = &mut deadline => return Err(unavailable("Host reconciliation wait exceeded 15 seconds; request was not admitted")),
                _ = &mut changed => {}
            }
        }
    }
    /// The caller holds the route lock through pending registration and send.
    /// A prior ready observation cannot authorize a replacement or a new pass.
    pub(super) fn check_ready_admission(&self, generation: u64) -> Result<(), ToolError> {
        let state = self.readiness.lock().unwrap_or_else(|e| e.into_inner());
        if state.generation != generation {
            return Err(unavailable("Host route changed before request admission"));
        }
        if !state.ready {
            return Err(state.admission_error());
        }
        Ok(())
    }
    /// One coherent owner snapshot; callers need not combine independently read scalar ports.
    pub fn readiness(&self) -> Value {
        let route = self.route.lock().unwrap_or_else(|e| e.into_inner());
        let state = self.readiness.lock().unwrap_or_else(|e| e.into_inner());
        json!({"connection_generation":state.generation.to_string(), "connected":route.is_some(),
            "ready":route.is_some() && state.ready, "reconciling":state.reconciling,
            "phase":state.phase, "cause":state.cause, "transport_warning":state.transport_warning})
    }
    pub fn is_ready(&self) -> bool {
        let route = self.route.lock().unwrap_or_else(|e| e.into_inner());
        route.is_some()
            && self
                .readiness
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .ready
    }
    pub fn is_reconciling(&self) -> bool {
        self.readiness
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .reconciling
    }
    pub fn reconciliation_error(&self) -> Option<Value> {
        self.readiness
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .cause
            .as_ref()
            .map(|cause| json!(cause))
    }
    // Caller holds route, so replacement and these transitions are atomic to readers.
    pub(super) fn attached_readiness(&self, generation: u64) {
        let mut state = self.readiness.lock().unwrap_or_else(|e| e.into_inner());
        *state = Readiness {
            generation,
            phase: Phase::AwaitingReconciliation,
            cause: None,
            revision: state.revision.wrapping_add(1),
            pass: state.pass,
            ..Readiness::default()
        };
        self.readiness_changed.notify_waiters();
    }
    pub(super) fn detached_readiness(&self, cause: Cause) {
        let mut state = self.readiness.lock().unwrap_or_else(|e| e.into_inner());
        state.ready = false;
        state.reconciling = false;
        state.recovery_requested = false;
        state.phase = Phase::Disconnected;
        state.transport_warning = None;
        state.cause = Some(cause);
        state.revision = state.revision.wrapping_add(1);
        self.readiness_changed.notify_waiters();
    }
    pub(super) fn heartbeat_warning(&self, generation: u64, backpressure: bool) {
        let route = self.route.lock().unwrap_or_else(|e| e.into_inner());
        if !route
            .as_ref()
            .is_some_and(|route| route.generation == generation)
        {
            return;
        }
        let mut state = self.readiness.lock().unwrap_or_else(|e| e.into_inner());
        let changed = backpressure && state.transport_warning.is_none();
        state.transport_warning = backpressure.then_some(TransportWarning {
            code: "heartbeat_backpressure",
            message: "Host heartbeat send exceeded 2 second deadline; recent inbound traffic keeps the route connected",
        });
        // Transport pressure is observational: it cannot gate execution or
        // advance a cause revision. Repeated blocked ticks add no log flood.
        if changed {
            self.readiness_diagnostic(
                generation,
                None,
                None,
                diagnostics::Reason::HeartbeatBackpressure,
            );
        }
    }
    pub(super) fn mark_unready(&self, generation: u64, cause: Cause) {
        self.update_unready(generation, cause, false);
    }
    pub(super) fn mark_unresolved_call(&self, generation: u64, cause: Cause) {
        self.update_unready(generation, cause, true);
    }
    fn update_unready(&self, generation: u64, cause: Cause, fallback: bool) {
        let route = self.route.lock().unwrap_or_else(|e| e.into_inner());
        if !route
            .as_ref()
            .is_some_and(|route| route.generation == generation)
        {
            return;
        }
        let mut state = self.readiness.lock().unwrap_or_else(|e| e.into_inner());
        if fallback
            && state.cause.as_ref().is_some_and(|old| {
                old.operation_id.is_some() && old.operation_id == cause.operation_id
            })
        {
            return;
        }
        state.ready = false;
        if !state.reconciling {
            state.phase = Phase::Unready;
        }
        state.cause = Some(cause);
        state.revision = state.revision.wrapping_add(1);
        self.readiness_changed.notify_waiters();
    }
    /// Reserve an owned pass synchronously, before the cancelled caller returns.
    /// Another abandonment during a pass requests one follow-up, not another worker.
    pub(super) fn request_call_recovery(&self, generation: u64) -> Option<(u64, u64)> {
        let route = self.route.lock().unwrap_or_else(|e| e.into_inner());
        if !route
            .as_ref()
            .is_some_and(|route| route.generation == generation)
        {
            return None;
        }
        let mut state = self.readiness.lock().unwrap_or_else(|e| e.into_inner());
        if state.ready {
            return None;
        }
        state.recovery_requested = true;
        if state.reconciling {
            return None;
        }
        state.recovery_requested = false;
        state.reconciling = true;
        state.phase = Phase::Reconciling;
        state.pass = state.pass.wrapping_add(1);
        self.readiness_changed.notify_waiters();
        Some((state.pass, state.revision))
    }
    pub(super) fn owns_reconciliation(&self, generation: u64, pass: u64) -> bool {
        let state = self.readiness.lock().unwrap_or_else(|e| e.into_inner());
        state.generation == generation && state.pass == pass && state.reconciling
    }
    pub(super) fn call_recovery_requested(&self, generation: u64) -> bool {
        let state = self.readiness.lock().unwrap_or_else(|e| e.into_inner());
        state.generation == generation
            && state.recovery_requested
            && !state.ready
            && !state.reconciling
    }
    pub(super) fn begin_reconciliation(&self, generation: u64) -> Result<(u64, u64), ToolError> {
        let route = self.route.lock().unwrap_or_else(|e| e.into_inner());
        if !route
            .as_ref()
            .is_some_and(|route| route.generation == generation)
        {
            return Err(unavailable("Host route changed before reconciliation"));
        }
        let mut state = self.readiness.lock().unwrap_or_else(|e| e.into_inner());
        state.ready = false;
        state.recovery_requested = false;
        state.reconciling = true;
        state.phase = Phase::Reconciling;
        state.pass = state.pass.wrapping_add(1);
        self.readiness_changed.notify_waiters();
        Ok((state.pass, state.revision))
    }
    pub(super) fn finish_reconciliation(
        &self,
        generation: u64,
        pass: u64,
        revision: u64,
        result: &Result<(), ToolError>,
    ) -> bool {
        self.finish_reconciliation_with_policy(generation, pass, revision, result, false)
    }
    pub(super) fn finish_call_recovery(
        &self,
        generation: u64,
        pass: u64,
        revision: u64,
        result: &Result<(), ToolError>,
    ) -> bool {
        self.finish_reconciliation_with_policy(generation, pass, revision, result, true)
    }
    fn finish_reconciliation_with_policy(
        &self,
        generation: u64,
        pass: u64,
        revision: u64,
        result: &Result<(), ToolError>,
        preserve_cause: bool,
    ) -> bool {
        let route = self.route.lock().unwrap_or_else(|e| e.into_inner());
        if !route
            .as_ref()
            .is_some_and(|route| route.generation == generation)
        {
            return false;
        }
        let mut state = self.readiness.lock().unwrap_or_else(|e| e.into_inner());
        if state.pass != pass || !state.reconciling {
            return false;
        }
        state.reconciling = false;
        self.readiness_changed.notify_waiters();
        if result.is_ok() && state.revision == revision {
            state.ready = true;
            state.phase = Phase::Ready;
            state.cause = None;
            return true;
        }
        state.phase = Phase::Unready;
        // Wire failures were captured before ToolError conversion. Never replace
        // their code/message/identity with a generic reconciliation wrapper.
        if state.revision == revision
            && (!preserve_cause || state.cause.is_none())
            && let Err(error) = result
        {
            state.cause = Some(Cause::phase(
                LocalCode::ReconciliationFailed,
                original_message(error),
            ));
        }
        false
    }
    pub(super) fn record_cleanup_failure(&self, generation: u64, error: &ToolError) {
        let route = self.route.lock().unwrap_or_else(|e| e.into_inner());
        if !route
            .as_ref()
            .is_some_and(|route| route.generation == generation)
        {
            return;
        }
        let mut state = self.readiness.lock().unwrap_or_else(|e| e.into_inner());
        if state.cause.is_none() {
            state.ready = false;
            if !state.reconciling {
                state.phase = Phase::Unready;
            }
            state.cause = Some(Cause::phase(
                LocalCode::ProcessCleanupFailed,
                original_message(error),
            ));
            state.revision = state.revision.wrapping_add(1);
            self.readiness_changed.notify_waiters();
        }
    }
    pub(super) fn abandon_reconciliation(&self, generation: u64, pass: u64) {
        let route = self.route.lock().unwrap_or_else(|e| e.into_inner());
        if !route
            .as_ref()
            .is_some_and(|route| route.generation == generation)
        {
            return;
        }
        let mut state = self.readiness.lock().unwrap_or_else(|e| e.into_inner());
        if state.pass == pass && state.reconciling {
            state.reconciling = false;
            self.readiness_changed.notify_waiters();
            state.phase = Phase::Unready;
            if state.cause.is_none() {
                state.cause = Some(Cause::phase(
                    LocalCode::ReconciliationCancelled,
                    "Host reconciliation was cancelled locally",
                ));
            }
        }
    }
}

fn original_message(error: &ToolError) -> String {
    match error {
        ToolError::Worker(message)
        | ToolError::InvalidRequest(message)
        | ToolError::OperationLimit(message) => message.to_string(),
        _ => error.to_string(),
    }
}
