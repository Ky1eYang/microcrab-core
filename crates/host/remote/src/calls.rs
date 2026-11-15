//! Correlation and cancellation retain an effect's unknown status across route loss.
use super::*;
use microcrab_host_api::{
    ErrorCategory, GlobOutput, GlobRequest, GrepOutput, GrepRequest, HostError, OpContext,
    ReadOutput, ReadRequest, WriteOutput, WriteRequest,
    filesystem::{FileKind, FileMeta, FsApi},
};
use microcrab_host_protocol::{ErrorCode, HostRequest, WireError};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    time::{Duration, UNIX_EPOCH},
};

struct CallGuard<'a> {
    host: &'a RemoteHost,
    request_id: String,
    operation_id: String,
    generation: u64,
    mutation: bool,
    admitted: bool,
    start: bool,
    delivered: bool,
    started: Instant,
    trace_span: tracing::Span,
    failure: Option<readiness::Cause>,
}
impl Drop for CallGuard<'_> {
    fn drop(&mut self) {
        tracing::info!(parent: &self.trace_span,
            bridge_request_id=%self.request_id,
            bridge_operation_id=%uuid::Uuid::parse_str(&self.operation_id).unwrap_or(uuid::Uuid::nil()),
            generation=self.generation, admitted=self.admitted, delivered=self.delivered,
            elapsed_ms=self.started.elapsed().as_millis() as u64,
            "Remote Host call completed");
        // Publish retirement before releasing pending: replies must see one or the other.
        let mut pending = self.host.pending.lock().unwrap_or_else(|e| e.into_inner());
        let removed = pending.remove(&self.request_id);
        let abandoned = removed.is_some() || self.failure.is_some();
        if removed.is_some() && self.admitted {
            let mut retired = self.host.retired.lock().unwrap_or_else(|e| e.into_inner());
            retired.push_back((
                self.request_id.clone(),
                self.operation_id.clone(),
                self.generation,
            ));
            while retired.len() > 256 {
                retired.pop_front();
            }
        }
        drop(pending);
        {
            let mut transfers = self
                .host
                .transfers
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            // Match accept_chunk's transfers -> retired_transfers lock order.
            transfers.retain(|id, transfer| {
                if transfer.request_id == self.request_id {
                    self.host.retire_transfer(id.clone(), self.generation);
                    false
                } else {
                    true
                }
            });
        }
        if self.start && !self.delivered {
            if self.admitted {
                self.host.cancel_start(&self.operation_id);
            } else {
                self.host
                    .processes
                    .starts
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&self.operation_id);
            }
        }
        if self.mutation
            && self.admitted
            && self
                .host
                .unresolved
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key(&self.operation_id)
        {
            if self.host.is_consumption(&self.operation_id) {
                // A cancelled cursor owns its exact receipt, never Host readiness.
                self.host
                    .recover_consumption(self.operation_id.clone(), self.generation);
            } else {
                self.host.mark_unresolved_call(
                    self.generation,
                    self.failure.clone().unwrap_or_else(|| {
                        readiness::Cause::new(
                            WireError::new(
                                ErrorCode::OutcomeUnknown,
                                "Host mutation call abandoned locally; outcome is unknown",
                            ),
                            Some(&self.operation_id),
                            Some(&self.request_id),
                        )
                    }),
                );
                self.host.readiness_diagnostic(
                    self.generation,
                    Some(&self.request_id),
                    Some(&self.operation_id),
                    diagnostics::Reason::UnresolvedCall,
                );
                if abandoned {
                    self.host.recover_abandoned_call(self.generation);
                }
            }
        }
        if self.mutation && !self.admitted {
            self.host
                .unresolved
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&self.operation_id);
            self.host.forget_consumption(&self.operation_id);
        }
        if removed.is_some() && self.admitted {
            let route = self.host.route.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(route) = route
                .as_ref()
                .filter(|route| route.generation == self.generation)
            {
                let _ = route.sender.try_send(HostFrame::Cancel {
                    request_id: self.request_id.clone(),
                    lease_generation: route.lease.clone(),
                });
            }
        }
    }
}
impl RemoteHost {
    pub(crate) fn route_parts(&self) -> Result<(mpsc::Sender<HostFrame>, u64, String), ToolError> {
        self.route
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|route| (route.sender.clone(), route.generation, route.lease.clone()))
            .ok_or_else(|| unavailable("Host is disconnected"))
    }
    pub(super) fn register_pending(
        &self,
        request_id: String,
        pending: Pending,
    ) -> Result<(), ToolError> {
        // Route replacement/detach takes these locks in this order. Admission
        // must be atomic with its drain, otherwise a caller can register after
        // fail_pending and wait an entire response timeout on an abandoned route.
        let route = self.route.lock().unwrap_or_else(|e| e.into_inner());
        if !route
            .as_ref()
            .is_some_and(|route| route.generation == pending.generation)
        {
            return Err(unavailable("Host route changed before request admission"));
        }
        let mut requests = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        if requests.len() >= microcrab_host_protocol::MAX_PENDING_REQUESTS + 8 {
            return Err(unavailable("Host pending request limit reached"));
        }
        requests.insert(request_id, pending);
        Ok(())
    }
    fn admit_request(
        &self,
        permit: mpsc::Permit<'_, HostFrame>,
        request_id: String,
        pending: Pending,
        frame: HostFrame,
        control: bool,
    ) -> Result<(), ToolError> {
        let route = self.route.lock().unwrap_or_else(|e| e.into_inner());
        if !route
            .as_ref()
            .is_some_and(|route| route.generation == pending.generation)
        {
            return Err(unavailable("Host route changed before request admission"));
        }
        if !pending.readiness_operation {
            self.check_ready_admission(pending.generation)?;
        }
        let mut requests = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        if requests.len()
            >= microcrab_host_protocol::MAX_PENDING_REQUESTS + if control { 8 } else { 0 }
        {
            return Err(unavailable("Host pending request limit reached"));
        }
        requests.insert(request_id, pending);
        // Capacity was reserved without locks. Replacement/reconciliation takes
        // route first, so it cannot interleave this final gate and the send.
        permit.send(frame);
        Ok(())
    }
    pub(crate) async fn call(
        &self,
        method: HostRequest,
        cancel: CancellationToken,
    ) -> Result<Value, ToolError> {
        self.call_for_generation(method, cancel, None).await
    }
    /// A pinned ordinary call never waits for reconciliation: its caller has
    /// already waited before taking any process-admission lock. The atomic send
    /// gate still requires this exact generation to remain ready.
    pub(crate) async fn call_for_generation(
        &self,
        method: HostRequest,
        cancel: CancellationToken,
        expected_generation: Option<u64>,
    ) -> Result<Value, ToolError> {
        if cancel.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        let owned_cleanup = match &method {
            HostRequest::ProcessRelease { handle } => self
                .processes
                .entries
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(handle)
                .is_some_and(|entry| entry.release_pending),
            _ => false,
        };
        let readiness_operation = owned_cleanup
            || matches!(
                &method,
                HostRequest::Recovery
                    | HostRequest::SettleUnknown { .. }
                    | HostRequest::ProcessInventory
            );
        let admission_generation = if readiness_operation {
            None
        } else if let Some(generation) = expected_generation {
            Some(generation)
        } else {
            match self.await_ready(&cancel).await {
                Ok(generation) => Some(generation),
                Err(error) => {
                    self.readiness_diagnostic(
                        self.generation.load(Ordering::SeqCst),
                        None,
                        None,
                        diagnostics::Reason::RejectedNotReady,
                    );
                    return Err(error);
                }
            }
        };
        if cancel.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        let (outbound, generation, lease) = self.route_parts()?;
        if admission_generation.is_some_and(|expected| expected != generation)
            || expected_generation.is_some_and(|expected| expected != generation)
        {
            return Err(unavailable("Host route changed before request admission"));
        }
        let permit = tokio::select! { biased;
            _ = cancel.cancelled() => return Err(ToolError::Cancelled),
            reserved = tokio::time::timeout(Duration::from_secs(2), outbound.reserve()) => {
                match reserved {
                    Ok(Ok(permit)) => permit,
                    _ => return Err(unavailable("Host route could not admit request")),
                }
            }
        };
        if cancel.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        let operation_id = uuid::Uuid::new_v4().to_string();
        let request_id = uuid::Uuid::new_v4().to_string();
        let control = method.is_control();
        let mutation = method.is_mutation();
        let consumption_key = method.consumption_key();
        if matches!(
            &method,
            HostRequest::PreflightMutations { .. } | HostRequest::Mutate(_)
        ) {
            // Native requests are inline protobuf frames. Check the exact frame
            // before immutable mutation intent or send admission: size rejection
            // must not create false uncertainty about filesystem effects.
            let frame = HostFrame::Request {
                request_id: request_id.clone(),
                operation_id: operation_id.clone(),
                lease_generation: lease.clone(),
                remaining_budget_ms: OPERATION_TIMEOUT.as_millis() as u32,
                method: method.clone(),
            };
            microcrab_host_protocol::encode_frame(&frame).map_err(|error| {
                ToolError::OperationLimit(microcrab_host_api::FailureReason::from(error.message)
                    .with_public_reason("native mutation request exceeds the 128 KiB protobuf frame budget; reduce the whole patch before retrying"))
            })?;
        }
        if mutation {
            let digest = microcrab_host_protocol::payload_digest(&method).map_err(wire_error)?;
            let mut unresolved = self.unresolved.lock().unwrap_or_else(|e| e.into_inner());
            let independent = method.is_process_operation() || method.is_start();
            let pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
            if unresolved.keys().any(|id| {
                if let Some(key) = self
                    .consumption
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(id)
                {
                    return consumption_key.as_ref() == Some(key);
                }
                !pending
                    .values()
                    .any(|request| &request.operation_id == id && (independent || request.raw_read))
            }) {
                return Err(unavailable(
                    "an earlier Host mutation is pending or unknown",
                ));
            }
            drop(pending);
            unresolved.insert(operation_id.clone(), digest);
            let mut consumption = self.consumption.lock().unwrap_or_else(|e| e.into_inner());
            consumption.retain(|id, _| unresolved.contains_key(id));
            if let Some(key) = consumption_key.clone() {
                consumption.insert(operation_id.clone(), key);
            }
        }
        let stream_read = matches!(&method, HostRequest::ProcessStreamRead { .. });
        let start = method.is_start();
        if start {
            self.processes
                .starts
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(
                    operation_id.clone(),
                    processes::StartOwnership {
                        cancelled: false,
                        receipt: None,
                    },
                );
        }
        let (sender, receiver) = oneshot::channel();
        let mut guard = CallGuard {
            host: self,
            request_id: request_id.clone(),
            operation_id: operation_id.clone(),
            generation,
            mutation,
            admitted: false,
            start,
            delivered: false,
            started: Instant::now(),
            trace_span: tracing::Span::current(),
            failure: None,
        };
        tracing::info!(bridge_request_id=%guard.request_id, bridge_operation_id=%guard.operation_id, generation, "Remote Host call started");
        self.admit_request(
            permit,
            request_id.clone(),
            Pending {
                raw_read: stream_read,
                readiness_operation,
                operation_id: operation_id.clone(),
                generation,
                sender,
            },
            HostFrame::Request {
                request_id,
                operation_id: operation_id.clone(),
                lease_generation: lease,
                remaining_budget_ms: OPERATION_TIMEOUT.as_millis() as u32,
                method,
            },
            control,
        )?;
        guard.admitted = true;
        tracing::info!(bridge_request_id=%guard.request_id, bridge_operation_id=%guard.operation_id, generation, "Remote Host request admitted");
        let response_deadline = async {
            if stream_read {
                std::future::pending::<()>().await;
            } else {
                tokio::time::sleep(OPERATION_TIMEOUT).await;
            }
        };
        let reply = tokio::select! {biased;
            _=cancel.cancelled()=> {
                guard.failure = Some(readiness::Cause::new(WireError::new(ErrorCode::Cancelled,
                    "Host mutation cancelled with unknown outcome"), Some(&guard.operation_id), Some(&guard.request_id)));
                return Err(if mutation && consumption_key.is_none() {unavailable("Host mutation cancelled with unknown outcome")}else{ToolError::Cancelled});
            },
            _=response_deadline=> {
                let message = if mutation {"Host mutation outcome unknown after deadline"}else{"Host response deadline exceeded"};
                guard.failure = Some(readiness::Cause::new(WireError::new(ErrorCode::Timeout, message),
                    Some(&guard.operation_id), Some(&guard.request_id)));
                return Err(unavailable(message));
            },
            reply=receiver=>match reply {
                Ok(result)=>result?,
                Err(_)=>return Err(unavailable("Host response owner stopped")),
            }
        };
        match reply {
            Reply::Value(value) => {
                guard.delivered = true;
                Ok(value)
            }
            Reply::TransportFailure(message) => {
                guard.failure = Some(readiness::Cause::new(
                    WireError::new(ErrorCode::Disconnected, message.clone()),
                    Some(&guard.operation_id),
                    Some(&guard.request_id),
                ));
                Err(ToolError::Worker(message.into()))
            }
            _ => Err(unavailable("unexpected Host reply kind")),
        }
    }
    pub(crate) async fn status_for_generation(
        &self,
        operation_id: &str,
        expected_generation: Option<u64>,
    ) -> Result<Option<OperationReceipt>, ToolError> {
        let (outbound, generation, lease) = self.route_parts()?;
        if expected_generation.is_some_and(|expected| expected != generation) {
            return Err(unavailable("Host route changed during reconciliation"));
        }
        let request_id = uuid::Uuid::new_v4().to_string();
        let (sender, receiver) = oneshot::channel();
        let mut status_guard = CallGuard {
            host: self,
            request_id: request_id.clone(),
            operation_id: operation_id.to_owned(),
            generation,
            mutation: false,
            admitted: false,
            start: false,
            delivered: false,
            started: Instant::now(),
            trace_span: tracing::Span::current(),
            failure: None,
        };
        tracing::info!(bridge_request_id=%status_guard.request_id,
            bridge_operation_id=%uuid::Uuid::parse_str(operation_id).unwrap_or(uuid::Uuid::nil()), generation,
            "Remote Host status query started");
        self.register_pending(
            request_id.clone(),
            Pending {
                raw_read: false,
                readiness_operation: true,
                operation_id: operation_id.to_owned(),
                generation,
                sender,
            },
        )?;
        if !matches!(
            tokio::time::timeout(
                Duration::from_secs(2),
                outbound.send(HostFrame::Status {
                    request_id,
                    operation_id: operation_id.to_owned(),
                    lease_generation: lease
                })
            )
            .await,
            Ok(Ok(()))
        ) {
            return Err(unavailable("Host status route unavailable"));
        }
        status_guard.admitted = true;
        match tokio::time::timeout(OPERATION_TIMEOUT, receiver).await {
            Ok(Ok(Ok(Reply::Receipt(receipt)))) => {
                status_guard.delivered = true;
                Ok(receipt)
            }
            Ok(Ok(Err(error))) => Err(error),
            _ => Err(unavailable("Host status query unavailable")),
        }
    }
    pub fn receive(&self, generation: u64, frame: HostFrame) -> Result<(), ToolError> {
        if let HostFrame::Diagnostic { event } = frame {
            return self.receive_diagnostic(generation, event);
        }
        {
            let mut route = self.route.lock().unwrap_or_else(|e| e.into_inner());
            let route = route
                .as_mut()
                .filter(|route| route.generation == generation)
                .ok_or_else(|| unavailable("stale Host response route"))?;
            route.last_received = Instant::now();
        }
        match frame {
            HostFrame::Response {
                request_id,
                operation_id,
                result,
            } => {
                let valid = self
                    .pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(&request_id)
                    .is_some_and(|pending| {
                        pending.generation == generation && pending.operation_id == operation_id
                    });
                if !valid {
                    let retired = self
                        .retired
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .iter()
                        .any(|(request, operation, old_generation)| {
                            request == &request_id
                                && operation == &operation_id
                                && *old_generation == generation
                        });
                    if retired {
                        self.readiness_diagnostic(
                            generation,
                            Some(&request_id),
                            Some(&operation_id),
                            diagnostics::Reason::LateResponse,
                        );
                        let unresolved = self
                            .unresolved
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .contains_key(&operation_id);
                        if unresolved {
                            // A late start reply must not remove its uncertainty
                            // before exact scope/digest receipt validation. The
                            // recovery owner installs cancelled-start cleanup.
                            if self.is_consumption(&operation_id) {
                                self.recover_consumption(operation_id.clone(), generation);
                            } else {
                                self.recover_abandoned_call(generation);
                            }
                        } else {
                            self.observe_start_reply(&operation_id, &result);
                        }
                        if let Ok(value) = result
                            && let Ok(reference) = serde_json::from_value::<
                                microcrab_host_protocol::TransferRef,
                            >(value)
                        {
                            self.retire_transfer(reference.transfer_id, generation);
                        }
                        return Ok(());
                    }
                    return Err(unavailable("uncorrelated Host response"));
                }
                let readiness_operation = self
                    .pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(&request_id)
                    .is_some_and(|pending| pending.readiness_operation);
                if let Err(error) = &result
                    && !self.is_consumption(&operation_id)
                    && (readiness_operation
                        || matches!(
                            error.code,
                            ErrorCode::OutcomeUnknown
                                | ErrorCode::Disconnected
                                | ErrorCode::LeaseExpired
                        ))
                {
                    self.mark_unready(
                        generation,
                        readiness::Cause::new(
                            error.clone(),
                            Some(&operation_id),
                            Some(&request_id),
                        ),
                    );
                }
                self.observe_start_reply(&operation_id, &result);
                if let Ok(value) = &result
                    && let Ok(reference) = serde_json::from_value::<
                        microcrab_host_protocol::TransferRef,
                    >(value.clone())
                {
                    return self.begin_transfer(request_id, operation_id, generation, reference);
                }
                let known = !matches!(&result,Err(error) if matches!(error.code,ErrorCode::OutcomeUnknown|ErrorCode::Disconnected|ErrorCode::LeaseExpired));
                if known {
                    self.forget_consumption(&operation_id);
                    let digest = self
                        .unresolved
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .remove(&operation_id);
                    let scope = digest
                        .as_ref()
                        .and_then(|_| self.scope.lock().unwrap_or_else(|e| e.into_inner()).clone());
                    if let (Some(payload_digest), Some(scope)) = (digest, scope) {
                        self.remember_receipt(&OperationReceipt {
                            operation_id: operation_id.clone(),
                            payload_digest,
                            scope,
                            state: microcrab_host_protocol::ReceiptState::Completed(result.clone()),
                        });
                    }
                } else {
                    self.readiness_diagnostic(
                        generation,
                        Some(&request_id),
                        Some(&operation_id),
                        diagnostics::Reason::UnknownResponse,
                    );
                }
                self.complete(
                    &request_id,
                    generation,
                    result.map(Reply::Value).map_err(wire_error),
                );
            }
            HostFrame::Receipt {
                request_id,
                receipt,
            } => {
                let pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
                let valid = pending.get(&request_id).is_some_and(|pending| {
                    pending.generation == generation
                        && receipt
                            .as_ref()
                            .is_none_or(|receipt| receipt.operation_id == pending.operation_id)
                });
                drop(pending);
                if !valid {
                    if self
                        .retired
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .iter()
                        .any(|(request, _, old_generation)| {
                            request == &request_id && *old_generation == generation
                        })
                    {
                        return Ok(());
                    }
                    return Err(unavailable("uncorrelated Host receipt"));
                }
                self.complete(&request_id, generation, Ok(Reply::Receipt(receipt)));
            }
            HostFrame::TransferChunk {
                transfer_id,
                sequence,
                data,
                eof,
            } => self.accept_chunk(generation, transfer_id, sequence, data, eof)?,
            HostFrame::ProcessChanged {
                lease_generation,
                handle,
                revision,
            } => {
                let valid = self
                    .route
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
                    .is_some_and(|route| {
                        route.generation == generation && route.lease == lease_generation
                    });
                if !valid {
                    return Err(unavailable("invalid Host process notification lease"));
                }
                self.processes.notify(handle, revision);
            }
            HostFrame::Pong { lease_generation } => {
                let valid = self
                    .route
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
                    .is_some_and(|route| {
                        route.generation == generation && route.lease == lease_generation
                    });
                if !valid {
                    return Err(unavailable("invalid Host heartbeat lease"));
                }
            }
            _ => return Err(unavailable("unexpected backend Host frame")),
        }
        Ok(())
    }
    /// A carrier stream failed before delivering its exact invocation. This is
    /// transport evidence, never a terminal effect receipt: retain uncertainty and
    /// let the call guard query the original operation without replaying it.
    pub fn fail_request(
        &self,
        generation: u64,
        request_id: &str,
        operation_id: &str,
        message: String,
    ) {
        let route = self.route.lock().unwrap_or_else(|e| e.into_inner());
        let Some(route) = route
            .as_ref()
            .filter(|route| route.generation == generation)
        else {
            return;
        };
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        if !pending.get(request_id).is_some_and(|pending| {
            pending.generation == generation && pending.operation_id == operation_id
        }) {
            return;
        }
        let Some(failed) = pending.remove(request_id) else {
            return;
        };
        let mut retired = self.retired.lock().unwrap_or_else(|e| e.into_inner());
        retired.push_back((request_id.to_owned(), operation_id.to_owned(), generation));
        while retired.len() > 256 {
            retired.pop_front();
        }
        drop(retired);
        drop(pending);
        let _ = route.sender.try_send(HostFrame::Cancel {
            request_id: request_id.to_owned(),
            lease_generation: route.lease.clone(),
        });
        let _ = failed.sender.send(Ok(Reply::TransportFailure(message)));
    }

    pub(crate) fn complete(
        &self,
        request_id: &str,
        generation: u64,
        result: Result<Reply, ToolError>,
    ) {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        if pending
            .get(request_id)
            .is_some_and(|pending| pending.generation == generation)
            && let Some(pending) = pending.remove(request_id)
        {
            let _ = pending.sender.send(result);
        }
    }
    pub(crate) async fn typed<T: DeserializeOwned>(
        &self,
        method: HostRequest,
        cancel: CancellationToken,
    ) -> Result<T, ToolError> {
        payload::decode(self.call(method, cancel).await?)
    }
}
fn path_string(path: &Path) -> Result<String, ToolError> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| ToolError::invalid_request("Host path is not UTF-8"))
}
pub(crate) fn wire_error(error: WireError) -> ToolError {
    match error.cause {
        Some(microcrab_host_protocol::HostCause::FilePathIsOutsideTheConfiguredRoot) => {
            return ToolError::PathOutsideRoot;
        }
        Some(
            microcrab_host_protocol::HostCause::ShellWorkingDirectoryIsOutsideTheConfiguredRoot,
        ) => return ToolError::CwdOutsideRoot,
        _ => {}
    }
    let reason = error.cause.map(|cause| cause.public_reason()).unwrap_or(match error.code {
        ErrorCode::Unsupported => "Host operation is unsupported",
        ErrorCode::Denied => "Host operation denied",
        ErrorCode::WorkspaceDenied => "Host workspace access denied",
        ErrorCode::PairingDenied => "Host pairing authorization denied",
        ErrorCode::Timeout => "Host RPC deadline exceeded",
        ErrorCode::Cancelled => "Host RPC cancelled",
        ErrorCode::Conflict => "Host operation conflicts with current state",
        ErrorCode::OperationConflict => "Host operation identity conflicts with its original payload or scope",
        ErrorCode::InvalidRequest => "Host RPC request does not match its operation contract",
        ErrorCode::NotFound => "Host resource not found",
        ErrorCode::Limit => "Host RPC resource limit exceeded",
        ErrorCode::Io => "Host RPC I/O failed",
        ErrorCode::Disconnected => "Host is disconnected",
        ErrorCode::LeaseExpired => "Host lease expired",
        ErrorCode::OutcomeUnknown => "Host mutation outcome is unknown; reconcile before further effects; do not replay automatically",
        ErrorCode::BindingMismatch => "Host binding does not match the pinned identity or scope",
    });
    let message = microcrab_host_api::FailureReason::from(error.message.as_str());
    let message = message.public_reason().unwrap_or_default();
    let reason = if message.is_empty() {
        reason.to_owned()
    } else if error.cause.is_some() && !message.contains(reason) {
        // A label may end in a sensitive field word such as "state". A
        // semicolon preserves the boundary when the next layer sanitizes again.
        format!("{reason}; {message}")
    } else {
        message.to_owned()
    };
    const UNKNOWN_GUIDANCE: &str = "mutation outcome is unknown; reconcile before further effects; do not replay automatically";
    let reason = if error.code == ErrorCode::OutcomeUnknown && !reason.contains(UNKNOWN_GUIDANCE) {
        format!("{reason}; {UNKNOWN_GUIDANCE}")
    } else {
        reason
    };
    let category = match error.code {
        ErrorCode::Unsupported => ErrorCategory::Unsupported,
        ErrorCode::Denied | ErrorCode::WorkspaceDenied | ErrorCode::PairingDenied => {
            ErrorCategory::Permission
        }
        ErrorCode::Timeout => ErrorCategory::Timeout,
        ErrorCode::Cancelled => ErrorCategory::Cancelled,
        ErrorCode::Conflict | ErrorCode::OperationConflict => ErrorCategory::Conflict,
        ErrorCode::NotFound => ErrorCategory::NotFound,
        ErrorCode::InvalidRequest => {
            return ToolError::InvalidRequest(
                microcrab_host_api::FailureReason::from(error.message).with_public_reason(&reason),
            );
        }
        ErrorCode::Limit => {
            return ToolError::OperationLimit(
                microcrab_host_api::FailureReason::from(error.message).with_public_reason(&reason),
            );
        }
        ErrorCode::Io
        | ErrorCode::Disconnected
        | ErrorCode::LeaseExpired
        | ErrorCode::OutcomeUnknown
        | ErrorCode::BindingMismatch => ErrorCategory::Io,
    };
    ToolError::Host(
        HostError::new(
            category,
            format!("Host {:?}: {}", error.code, error.message),
        )
        .with_public_reason(&reason),
    )
}

#[async_trait::async_trait]
impl Host for RemoteHost {
    fn info(&self) -> Result<HostInfo, ToolError> {
        Ok(self.descriptor.info.clone())
    }
    fn tool_capabilities(&self) -> Vec<String> {
        let capabilities: Vec<String> = self
            .descriptor
            .tool_capabilities
            .iter()
            .filter(|name| {
                matches!(
                    name.as_str(),
                    "read"
                        | "write"
                        | "glob"
                        | "grep"
                        | "shell"
                        | "process"
                        | "read_file"
                        | "normalize_resource"
                        | "file_mutation"
                        | "cache_point"
                )
            })
            .cloned()
            .collect();
        capabilities
    }
    async fn normalize_resource(
        &self,
        path: PathBuf,
        cancel: CancellationToken,
    ) -> Result<String, ToolError> {
        self.typed(
            HostRequest::Normalize {
                path: path_string(&path)?,
            },
            cancel,
        )
        .await
    }
    async fn read_text(
        &self,
        request: ReadRequest,
        cancel: CancellationToken,
    ) -> Result<ReadOutput, ToolError> {
        self.typed(HostRequest::Read(request), cancel).await
    }
    async fn write(
        &self,
        request: WriteRequest,
        cancel: CancellationToken,
    ) -> Result<WriteOutput, ToolError> {
        self.typed(HostRequest::Write(request), cancel).await
    }
    async fn preflight_mutations(
        &self,
        requests: Vec<microcrab_host_api::FileMutation>,
        cancel: CancellationToken,
    ) -> Result<Vec<microcrab_host_api::FileMutation>, ToolError> {
        if !self
            .tool_capabilities()
            .iter()
            .any(|cap| cap == "file_mutation")
        {
            return Err(ToolError::unsupported("file_mutation"));
        }
        self.typed(HostRequest::PreflightMutations { requests }, cancel)
            .await
    }
    async fn mutate(
        &self,
        request: microcrab_host_api::FileMutation,
        cancel: CancellationToken,
    ) -> Result<microcrab_host_api::MutationOutput, ToolError> {
        if !self
            .tool_capabilities()
            .iter()
            .any(|cap| cap == "file_mutation")
        {
            return Err(ToolError::unsupported("file_mutation"));
        }
        // Mutate is an effect in HostRequest: existing immutable intents,
        // receipt reconciliation and route-loss fencing prohibit replay.
        self.typed(HostRequest::Mutate(request), cancel).await
    }
    async fn glob(
        &self,
        request: GlobRequest,
        cancel: CancellationToken,
    ) -> Result<GlobOutput, ToolError> {
        self.typed(HostRequest::Glob(request), cancel).await
    }
    async fn grep(
        &self,
        request: GrepRequest,
        cancel: CancellationToken,
    ) -> Result<GrepOutput, ToolError> {
        self.typed(HostRequest::Grep(request), cancel).await
    }
    async fn read_file(
        &self,
        path: PathBuf,
        max_bytes: usize,
        cancel: CancellationToken,
    ) -> Result<Vec<u8>, ToolError> {
        self.typed(
            HostRequest::ReadFile {
                path: path_string(&path)?,
                max_bytes: u32::try_from(max_bytes).map_err(|_| ToolError::FileLimit)?,
            },
            cancel,
        )
        .await
    }
    async fn cache_point(
        &self,
        request: microcrab_host_api::CachePointRequest,
        cancel: CancellationToken,
    ) -> Result<microcrab_host_api::FileCachePoint, ToolError> {
        if request.max_bytes == 0 || request.max_bytes > microcrab_host_api::MAX_CACHE_POINT_BYTES {
            return Err(ToolError::invalid_request(
                "cache point byte budget is out of bounds",
            ));
        }
        let max_bytes = request.max_bytes;
        let point: microcrab_host_api::FileCachePoint =
            self.typed(HostRequest::CachePoint(request), cancel).await?;
        match (&point.data, &point.version) {
            (None, None) => {}
            (Some(data), Some(version))
                if data.len() <= max_bytes && microcrab_host_protocol::sha256(data) == *version => {
            }
            _ => return Err(unavailable("invalid Host cache point content or digest")),
        }
        let unrestricted = self.descriptor.info.description.as_deref()
            == Some(microcrab_host_api::UNRESTRICTED_FILESYSTEM_DESCRIPTION);
        // Preserve the existing carrier-relative syntax, including names from
        // non-Local Hosts. This is evidence validation, not LocalHost admission.
        let legacy_relative = !point.path.is_empty()
            && point.path.len() <= 32768
            && !point.path.contains(['\\', ':'])
            && Path::new(&point.path)
                .components()
                .all(|component| matches!(component, std::path::Component::Normal(_)));
        let absolute = unrestricted
            && (point.path.starts_with('/') || point.path.as_bytes().get(1) == Some(&b':'))
            && microcrab_host_api::is_normalized_file_path(&point.path, true);
        if !legacy_relative && !absolute {
            return Err(unavailable("invalid Host cache point path"));
        }
        Ok(point)
    }
}
#[async_trait::async_trait]
impl FsApi for RemoteHost {
    async fn stat(
        &self,
        ctx: &OpContext,
        path: &Path,
    ) -> Result<FileMeta, microcrab_host_api::HostError> {
        ctx.check()?;
        if !self.capabilities.supports("filesystem.stat") {
            return Err(HostError::unsupported_known("filesystem.stat"));
        }
        let request = HostRequest::Stat {
            path: path_string(path).map_err(ToolError::into_host_error)?,
        };
        let value = ctx
            .run(async {
                self.call(request, ctx.cancel.clone())
                    .await
                    .map_err(ToolError::into_host_error)
            })
            .await?;
        let kind = match value.get("kind").and_then(Value::as_str) {
            Some("file") => FileKind::File,
            Some("directory") => FileKind::Directory,
            Some("symlink") => FileKind::Symlink,
            Some("other") => FileKind::Other,
            _ => {
                return Err(HostError::known(
                    ErrorCategory::Io,
                    "invalid Host stat kind",
                ));
            }
        };
        let size = value
            .get("size")
            .and_then(Value::as_str)
            .and_then(|size| size.parse::<u64>().ok())
            .ok_or_else(|| HostError::known(ErrorCategory::Io, "invalid Host stat size"))?;
        let modified = value
            .get("modified_ms")
            .and_then(Value::as_u64)
            .and_then(|ms| UNIX_EPOCH.checked_add(Duration::from_millis(ms)));
        Ok(FileMeta {
            kind,
            size,
            modified,
        })
    }
}
