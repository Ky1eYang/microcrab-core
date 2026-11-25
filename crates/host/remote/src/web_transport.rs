//! Lease-local text transport. Response bodies never enter the legacy call,
//! readiness, receipt, or replay paths. Lock order: route -> web -> legacy transfers.
use super::*;
use microcrab_host_api::{
    ErrorCategory, OpContext,
    web_transport::{
        ConnectionOwner, ConnectionScope, HttpResponse, ProjectScope, WEB_MAX_BUDGET_MS,
        WebTransportError as Error, WebTransportResult as Result, WsPull, WsReceive,
    },
};
use microcrab_host_protocol::{
    BindingScope, HostRequest, MAX_CHUNK_BYTES, MAX_PENDING_REQUESTS, TransferRef,
    WebTransportRequest as Request, WireError,
};
use serde_json::Value;

const REQUEST_PREFIX: &str = "wt-";
const MAX_SCOPES: usize = 256;
const RETIRED_TRANSFERS: usize = 256;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Discovery,
    Discovered,
    Opening,
    Open,
    Failed,
    Closing,
}

struct Pin {
    generation: u64,
    phase: Phase,
}

#[derive(Clone, Copy)]
enum ReplyKind {
    Unit,
    Http(u32),
    Receive(u32),
    Pull(u32),
}

pub(super) enum WebReply {
    Unit,
    Http(HttpResponse),
    Receive(WsReceive),
    Pull(WsPull),
}

impl WebReply {
    pub(super) fn unit(self) -> Result<()> {
        match self {
            Self::Unit => Ok(()),
            _ => Err(Error::Io),
        }
    }

    pub(super) fn http(self) -> Result<HttpResponse> {
        match self {
            Self::Http(reply) => Ok(reply),
            _ => Err(Error::Io),
        }
    }

    pub(super) fn receive(self) -> Result<WsReceive> {
        match self {
            Self::Receive(reply) => Ok(reply),
            _ => Err(Error::Io),
        }
    }

    pub(super) fn pull(self) -> Result<WsPull> {
        match self {
            Self::Pull(reply) => Ok(reply),
            _ => Err(Error::Io),
        }
    }
}

impl ReplyKind {
    fn decode(self, value: Value) -> Result<WebReply> {
        match self {
            Self::Unit if value.is_null() => Ok(WebReply::Unit),
            Self::Http(max) => {
                let reply: HttpResponse = serde_json::from_value(value).map_err(|_| Error::Io)?;
                reply.validate(max)?;
                Ok(WebReply::Http(reply))
            }
            Self::Receive(max) => {
                let reply: WsReceive = serde_json::from_value(value).map_err(|_| Error::Io)?;
                reply.validate(max)?;
                Ok(WebReply::Receive(reply))
            }
            Self::Pull(max) => {
                let reply: WsPull = serde_json::from_value(value).map_err(|_| Error::Io)?;
                reply.validate(max)?;
                Ok(WebReply::Pull(reply))
            }
            _ => Err(Error::Io),
        }
    }
}

struct WebPending {
    operation_id: String,
    sender: oneshot::Sender<Result<WebReply>>,
    reply: ReplyKind,
    json_cap: Option<usize>,
    transfer_id: Option<String>,
}

struct Incoming {
    request_id: String,
    reference: TransferRef,
    next: u32,
    bytes: Vec<u8>,
}

#[derive(Default)]
pub(super) struct WebLane {
    pins: HashMap<ConnectionScope, Pin>,
    retired_scopes: VecDeque<ConnectionScope>,
    retired_owners: VecDeque<ConnectionOwner>,
    pending: HashMap<String, WebPending>,
    transfers: HashMap<String, Incoming>,
    retired_transfers: VecDeque<String>,
}

impl WebLane {
    pub(super) fn route_lost(&mut self) {
        // Keep only bounded scope metadata across replacement: a discovery on
        // the old route must not become a fresh open on a newly proven route.
        self.pending.clear();
        self.transfers.clear();
        self.retired_transfers.clear();
    }

    pub(super) fn owns_transfer_id(&self, id: &str) -> bool {
        self.transfers.contains_key(id) || self.retired_transfers.iter().any(|old| old == id)
    }

    fn retire_transfer(&mut self, id: String) {
        self.transfers.remove(&id);
        if id.is_empty() || id.len() > 256 || self.retired_transfers.contains(&id) {
            return;
        }
        self.retired_transfers.push_back(id);
        if self.retired_transfers.len() > RETIRED_TRANSFERS {
            self.retired_transfers.pop_front();
        }
    }

    fn remove_request(&mut self, request_id: &str) -> Option<WebPending> {
        let pending = self.pending.remove(request_id)?;
        if let Some(id) = &pending.transfer_id {
            self.retire_transfer(id.clone());
        }
        Some(pending)
    }

    fn complete(&mut self, request_id: &str, result: Result<WebReply>) {
        if let Some(pending) = self.remove_request(request_id) {
            let _ = pending.sender.send(result);
        }
    }

    fn retire_scope(&mut self, scope: &ConnectionScope) {
        self.pins.remove(scope);
        // Runtime never reuses scope IDs; the finite fence catches late calls
        // racing with close without retaining unbounded lifetime metadata.
        if !self.retired_scopes.contains(scope) {
            self.retired_scopes.push_back(scope.clone());
            if self.retired_scopes.len() > MAX_SCOPES {
                self.retired_scopes.pop_front();
            }
        }
    }

    fn reserve_scope(
        &mut self,
        scope: &ConnectionScope,
        generation: u64,
        phase: Phase,
    ) -> Result<()> {
        if self.retired_scopes.contains(scope) || self.retired_owners.contains(&scope.owner()) {
            return Err(Error::Stale);
        }
        if self.pins.len() >= MAX_SCOPES {
            return Err(Error::Limit);
        }
        self.pins.insert(scope.clone(), Pin { generation, phase });
        Ok(())
    }
}

impl RequestScope<'_> {
    fn phase(&self) -> Option<Phase> {
        match self {
            Self::Http(_) => Some(Phase::Discovery),
            Self::Open(_) => Some(Phase::Opening),
            Self::Existing(_) | Self::Pull(_) => Some(Phase::Open),
            Self::Close(_) => Some(Phase::Closing),
            Self::Release(_) => None,
        }
    }
    fn connection(&self) -> Option<&ConnectionScope> {
        match self {
            Self::Http(scope)
            | Self::Open(scope)
            | Self::Existing(scope)
            | Self::Pull(scope)
            | Self::Close(scope) => Some(scope),
            Self::Release(_) => None,
        }
    }
}

enum RequestScope<'a> {
    Http(&'a ConnectionScope),
    Open(&'a ConnectionScope),
    Existing(&'a ConnectionScope),
    Pull(&'a ConnectionScope),
    Close(&'a ConnectionScope),
    Release(&'a microcrab_host_api::web_transport::ConnectionOwner),
}

impl<'a> From<&'a Request> for RequestScope<'a> {
    fn from(request: &'a Request) -> Self {
        match request {
            Request::HttpGet { scope, .. } => Self::Http(scope),
            Request::WsOpen { scope, .. } => Self::Open(scope),
            Request::WsStatus { scope }
            | Request::WsSend { scope, .. }
            | Request::WsReceive { scope, .. } => Self::Existing(scope),
            Request::WsPull { scope, .. } => Self::Pull(scope),
            Request::WsClose { scope } => Self::Close(scope),
            Request::ReleaseScope { owner } => Self::Release(owner),
        }
    }
}

struct Call<'a> {
    host: &'a RemoteHost,
    scope: RequestScope<'a>,
    generation: u64,
    request_id: String,
    sent: bool,
    finished: bool,
}

impl Call<'_> {
    fn scope_is_current(&self, lane: &WebLane) -> bool {
        match (&self.scope, self.scope.phase()) {
            (RequestScope::Release(owner), _) => lane.pins.iter().any(|(scope, pin)| {
                scope.owner() == **owner
                    && pin.generation == self.generation
                    && pin.phase == Phase::Closing
            }),
            (scope, Some(phase)) => scope
                .connection()
                .and_then(|scope| lane.pins.get(scope))
                .is_some_and(|pin| pin.generation == self.generation && pin.phase == phase),
            _ => false,
        }
    }

    fn settle(&mut self, reply: Result<WebReply>) -> Result<WebReply> {
        let route = self.host.route.lock().unwrap_or_else(|e| e.into_inner());
        let mut lane = self.host.web.lock().unwrap_or_else(|e| e.into_inner());
        if !route
            .as_ref()
            .is_some_and(|route| route.generation == self.generation)
            || !self.scope_is_current(&lane)
        {
            return Err(Error::Stale);
        }
        self.finished = true;
        match self.scope {
            RequestScope::Http(scope) => {
                lane.pins.get_mut(scope).expect("checked pin").phase = if reply.is_ok() {
                    Phase::Discovered
                } else {
                    Phase::Failed
                };
            }
            RequestScope::Open(scope) => {
                lane.pins.get_mut(scope).expect("checked pin").phase = if reply.is_ok() {
                    Phase::Open
                } else {
                    Phase::Failed
                };
            }
            RequestScope::Existing(scope) => {
                if matches!(
                    &reply,
                    Err(Error::Stale
                        | Error::Closed
                        | Error::OutcomeUnknown
                        | Error::Io
                        | Error::Timeout
                        | Error::Cancelled
                        | Error::Limit)
                        | Ok(WebReply::Receive(WsReceive::Closed | WsReceive::Timeout))
                ) {
                    lane.pins.get_mut(scope).expect("checked pin").phase = Phase::Failed;
                }
            }
            RequestScope::Pull(scope) => {
                // The authenticated carrier returns these hard failures only
                // before dequeue. A lost/unusable reply cannot certify that.
                if !matches!(
                    &reply,
                    Ok(WebReply::Pull(WsPull::Message(_) | WsPull::Empty))
                        | Err(Error::Unsupported | Error::Denied | Error::InvalidRequest
                            | Error::Timeout | Error::Cancelled)
                ) {
                    lane.pins.get_mut(scope).expect("checked pin").phase = Phase::Failed;
                }
            }
            RequestScope::Close(scope) => {
                if matches!(reply, Ok(_) | Err(Error::Stale | Error::Closed)) {
                    lane.retire_scope(scope);
                }
            }
            RequestScope::Release(owner) => {
                if matches!(reply, Ok(_) | Err(Error::Stale | Error::Closed)) {
                    lane.pins.retain(|scope, pin| {
                        scope.owner() != *owner || pin.generation != self.generation
                    });
                }
            }
        }
        reply
    }
}

impl Drop for Call<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let route = self.host.route.lock().unwrap_or_else(|e| e.into_inner());
        let Some(route) = route.as_ref().filter(|r| r.generation == self.generation) else {
            return;
        };
        let mut lane = self.host.web.lock().unwrap_or_else(|e| e.into_inner());
        if self.scope_is_current(&lane) {
            match self.scope {
                RequestScope::Http(scope) | RequestScope::Open(scope) => {
                    lane.pins.get_mut(scope).expect("checked pin").phase = Phase::Failed;
                }
                RequestScope::Existing(scope) | RequestScope::Pull(scope) if self.sent => {
                    lane.pins.get_mut(scope).expect("checked pin").phase = Phase::Failed;
                }
                _ => {}
            }
        }
        if self.sent && lane.remove_request(&self.request_id).is_some() {
            // Advisory only; Runtime's exact close and native lease cleanup own
            // resource teardown even if this frame cannot be queued.
            let _ = route.sender.try_send(HostFrame::Cancel {
                request_id: self.request_id.clone(),
                lease_generation: route.lease.clone(),
            });
        }
    }
}

fn check(ctx: &OpContext) -> Result<()> {
    ctx.check().map_err(|err| match err.category {
        ErrorCategory::Cancelled => Error::Cancelled,
        ErrorCategory::Timeout => Error::Timeout,
        _ => Error::Io,
    })
}

impl RemoteHost {
    pub(super) fn web_binding(&self) -> Option<ProjectScope> {
        let route = self.route.lock().unwrap_or_else(|e| e.into_inner());
        let route = route.as_ref().filter(|route| route.web_transport_v1)?;
        self.check_ready_admission(route.generation).ok()?;
        match self
            .scope
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()?
        {
            BindingScope::Project(scope) => Some(scope.clone()),
            BindingScope::Conversation(_) => None,
        }
    }

    pub(super) async fn web_call(&self, ctx: &OpContext, request: Request) -> Result<WebReply> {
        use std::time::Duration;
        check(ctx)?;
        request.validate(WEB_MAX_BUDGET_MS)?;
        let cap = Duration::from_millis(WEB_MAX_BUDGET_MS.into());
        let deadline = ctx
            .deadline
            .map_or_else(|| Instant::now() + cap, |at| at.min(Instant::now() + cap));
        let request_scope = RequestScope::from(&request);
        let pull = matches!(request_scope, RequestScope::Pull(_));
        let (mut call, outbound, lease, stop) = {
            let route = self.route.lock().unwrap_or_else(|e| e.into_inner());
            let scope = self.scope.lock().unwrap_or_else(|e| e.into_inner());
            match scope.as_ref() {
                Some(BindingScope::Project(scope)) if scope == request.binding() => {}
                Some(BindingScope::Project(_)) => return Err(Error::Denied),
                _ => return Err(Error::Unsupported),
            }
            let mut lane = self.web.lock().unwrap_or_else(|e| e.into_inner());
            let generation = route.as_ref().map(|route| route.generation);
            match request_scope {
                RequestScope::Close(scope) => {
                    let Some(pin) = lane.pins.get_mut(scope) else {
                        return Ok(WebReply::Unit);
                    };
                    if Some(pin.generation) != generation {
                        lane.retire_scope(scope);
                        return Ok(WebReply::Unit);
                    }
                    pin.phase = Phase::Closing;
                }
                RequestScope::Release(owner) => {
                    if !lane.retired_owners.contains(owner) {
                        lane.retired_owners.push_back(owner.clone());
                        if lane.retired_owners.len() > MAX_SCOPES {
                            lane.retired_owners.pop_front();
                        }
                    }
                    let mut active = false;
                    lane.pins.retain(|scope, pin| {
                        if scope.owner() != *owner {
                            return true;
                        }
                        if Some(pin.generation) == generation {
                            active = true;
                            pin.phase = Phase::Closing;
                            true
                        } else {
                            false
                        }
                    });
                    if !active {
                        return Ok(WebReply::Unit);
                    }
                }
                _ => {}
            }
            if let Some(scope) = request_scope.connection()
                && let Some(pin) = lane.pins.get(scope)
                && Some(pin.generation) != generation
            {
                return Err(Error::Stale);
            }
            let route = route.as_ref().ok_or(Error::Stale)?;
            if !route.web_transport_v1 || (pull && !route.web_transport_ws_pull) {
                return Err(Error::Unsupported);
            }
            match request_scope {
                RequestScope::Http(scope) => {
                    if lane.pins.contains_key(scope) {
                        return Err(Error::Stale);
                    }
                    lane.reserve_scope(scope, route.generation, Phase::Discovery)?;
                }
                RequestScope::Open(scope) => match lane.pins.get_mut(scope) {
                    Some(pin) if pin.phase == Phase::Discovered => pin.phase = Phase::Opening,
                    Some(_) => return Err(Error::Stale),
                    None => lane.reserve_scope(scope, route.generation, Phase::Opening)?,
                },
                RequestScope::Existing(scope) | RequestScope::Pull(scope) => {
                    if !lane
                        .pins
                        .get(scope)
                        .is_some_and(|pin| pin.phase == Phase::Open)
                    {
                        return Err(Error::Stale);
                    }
                }
                RequestScope::Close(_) | RequestScope::Release(_) => {}
            }
            (
                Call {
                    host: self,
                    scope: request_scope,
                    generation: route.generation,
                    request_id: format!("{REQUEST_PREFIX}{}", uuid::Uuid::new_v4()),
                    sent: false,
                    finished: false,
                },
                route.sender.clone(),
                route.lease.clone(),
                route.stop.clone(),
            )
        };
        let cleanup = matches!(
            call.scope,
            RequestScope::Close(_) | RequestScope::Release(_)
        );
        let reply_kind = match request {
            Request::HttpGet { max_bytes, .. } => ReplyKind::Http(max_bytes),
            Request::WsReceive { max_bytes, .. } => ReplyKind::Receive(max_bytes),
            Request::WsPull { max_bytes, .. } => ReplyKind::Pull(max_bytes),
            _ => ReplyKind::Unit,
        };
        let json_cap = request.max_reply_json_bytes();
        let operation_id = uuid::Uuid::new_v4().to_string();
        let (sender, receiver) = oneshot::channel();
        let exchange = async {
            // Scoped teardown must remain possible while ordinary requests are gated.
            if !cleanup
                && self.await_ready(&ctx.cancel).await.map_err(|_| Error::Io)? != call.generation
            {
                return Err(Error::Stale);
            }
            let reserve_deadline = deadline.min(Instant::now() + Duration::from_secs(2));
            let permit = tokio::time::timeout_at(reserve_deadline.into(), outbound.reserve())
                .await
                .map_err(|_| Error::Timeout)?
                .map_err(|_| Error::Stale)?;
            {
                let route = self.route.lock().unwrap_or_else(|e| e.into_inner());
                if !route.as_ref().is_some_and(|route| {
                    route.generation == call.generation
                        && route.web_transport_v1
                        && (!pull || route.web_transport_ws_pull)
                }) {
                    return Err(Error::Stale);
                }
                if !cleanup {
                    self.check_ready_admission(call.generation)
                        .map_err(|_| Error::Io)?;
                }
                let mut lane = self.web.lock().unwrap_or_else(|e| e.into_inner());
                if !call.scope_is_current(&lane) {
                    return Err(Error::Stale);
                }
                if lane.pending.len() >= MAX_PENDING_REQUESTS + if cleanup { 8 } else { 0 } {
                    return Err(Error::Limit);
                }
                check(ctx)?;
                let millis = deadline
                    .saturating_duration_since(Instant::now())
                    .as_millis() as u32;
                if millis == 0 {
                    return Err(Error::Timeout);
                }
                // Endpoint/text sizes are bounded by validation, but JSON escaping can
                // expand them. Validate the exact protobuf envelope before admission.
                let frame = HostFrame::Request {
                    request_id: call.request_id.clone(),
                    operation_id: operation_id.clone(),
                    lease_generation: lease,
                    remaining_budget_ms: millis,
                    method: HostRequest::WebTransport(request.clone()),
                };
                microcrab_host_protocol::encode_frame(&frame).map_err(|_| Error::Limit)?;
                lane.pending.insert(
                    call.request_id.clone(),
                    WebPending {
                        operation_id: operation_id.clone(),
                        sender,
                        reply: reply_kind,
                        json_cap,
                        transfer_id: None,
                    },
                );
                permit.send(frame);
                call.sent = true;
            }
            tracing::debug!(
                observation = "web_transport_operation_admitted",
                source = "remote_host",
                operation_sha256 = %microcrab_host_protocol::sha256(ctx.trace_id.as_bytes()),
                principal_sha256 = %microcrab_host_protocol::sha256(ctx.principal.as_bytes()),
                bridge_operation_id = %operation_id,
                "remote web transport operation admitted"
            );
            call.settle(receiver.await.map_err(|_| Error::Stale)?)
        };
        let result = tokio::select! { biased;
            _ = stop.cancelled() => Err(Error::Stale),
            _ = ctx.cancel.cancelled() => Err(Error::Cancelled),
            _ = tokio::time::sleep_until(deadline.into()) => Err(Error::Timeout),
            result = exchange => result,
        };
        // After send/consume admission, an unobserved result is not proof that
        // the effect did not happen. Caller must retire, never resend/receive.
        let result = if call.sent
            && !call.finished
            && matches!(
                request,
                Request::WsSend { .. } | Request::WsReceive { .. } | Request::WsPull { .. }
            )
        {
            Err(Error::OutcomeUnknown)
        } else {
            result
        };
        if call.sent && let Err(category) = &result {
            tracing::warn!(
                observation = "web_transport_operation_failed",
                source = "remote_host",
                operation_sha256 = %microcrab_host_protocol::sha256(ctx.trace_id.as_bytes()),
                principal_sha256 = %microcrab_host_protocol::sha256(ctx.principal.as_bytes()),
                bridge_operation_id = %operation_id,
                ?category,
                "admitted remote web transport operation failed"
            );
        }
        result
    }

    pub(super) fn receive_web_response(
        &self,
        generation: u64,
        request_id: &str,
        operation_id: &str,
        result: std::result::Result<Value, WireError>,
    ) -> std::result::Result<(), ToolError> {
        let route = self.route.lock().unwrap_or_else(|e| e.into_inner());
        if !route
            .as_ref()
            .is_some_and(|route| route.generation == generation && route.web_transport_v1)
        {
            return Err(unavailable("stale web transport response route"));
        }
        let mut lane = self.web.lock().unwrap_or_else(|e| e.into_inner());
        let Some(pending) = lane.pending.get(request_id) else {
            // Ephemeral IDs have a separate namespace; late replies cannot
            // trigger any legacy reconciliation or change a new connection.
            if let Ok(value) = result
                && let Ok(reference) = serde_json::from_value::<TransferRef>(value)
                && !self.legacy_owns_transfer(&reference.transfer_id, generation)
                && !lane.transfers.contains_key(&reference.transfer_id)
            {
                lane.retire_transfer(reference.transfer_id);
            }
            return Ok(());
        };
        if pending.operation_id != operation_id || pending.transfer_id.is_some() {
            return Err(unavailable("uncorrelated web transport response"));
        }
        let (reply_kind, json_cap) = (pending.reply, pending.json_cap);
        let result = match result {
            Err(error) => Err(Error::from(error)),
            Ok(value) => {
                if let Ok(reference) = serde::Deserialize::deserialize(&value) {
                    let reference: TransferRef = reference;
                    let id = &reference.transfer_id;
                    if reference.kind == "json"
                        && !id.is_empty()
                        && id.len() <= 256
                        && reference.sha256.len() == 64
                        && reference
                            .sha256
                            .bytes()
                            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
                        && !lane.owns_transfer_id(id)
                        && !self.legacy_owns_transfer(id, generation)
                        && json_cap.is_some_and(|cap| {
                            reference.byte_length > 0 && reference.byte_length <= cap as u64
                        })
                    {
                        lane.pending
                            .get_mut(request_id)
                            .expect("known request")
                            .transfer_id = Some(id.clone());
                        lane.transfers.insert(
                            id.clone(),
                            Incoming {
                                request_id: request_id.to_owned(),
                                // The declaration passed the operation-specific
                                // cap; preallocate exactly once, not geometrically.
                                bytes: Vec::with_capacity(reference.byte_length as usize),
                                reference,
                                next: 0,
                            },
                        );
                        return Ok(());
                    }
                    if !lane.transfers.contains_key(id)
                        && !self.legacy_owns_transfer(id, generation)
                    {
                        lane.retire_transfer(id.clone());
                    }
                    Err(
                        if json_cap.is_some_and(|cap| reference.byte_length > cap as u64) {
                            Error::Limit
                        } else {
                            Error::Io
                        },
                    )
                } else {
                    reply_kind.decode(value)
                }
            }
        };
        lane.complete(request_id, result);
        Ok(())
    }

    /// Returns unconsumed bytes for the legacy transfer lane.
    pub(super) fn receive_web_chunk(
        &self,
        generation: u64,
        id: &str,
        sequence: u32,
        data: Vec<u8>,
        eof: bool,
    ) -> std::result::Result<Option<Vec<u8>>, ToolError> {
        let route = self.route.lock().unwrap_or_else(|e| e.into_inner());
        if !route
            .as_ref()
            .is_some_and(|route| route.generation == generation)
        {
            return Err(unavailable("stale Host transfer route"));
        }
        let mut lane = self.web.lock().unwrap_or_else(|e| e.into_inner());
        let Some(transfer) = lane.transfers.get_mut(id) else {
            return Ok((!lane.owns_transfer_id(id)).then_some(data));
        };
        let end = transfer.bytes.len() + data.len();
        let exact_end = end == transfer.reference.byte_length as usize;
        if transfer.next != sequence
            || data.len() > MAX_CHUNK_BYTES
            || data.is_empty()
            || end > transfer.reference.byte_length as usize
            || exact_end != eof
        {
            let request_id = transfer.request_id.clone();
            lane.complete(&request_id, Err(Error::Io));
        } else {
            transfer.next += 1;
            transfer.bytes.extend(data);
            if eof {
                let Incoming {
                    request_id,
                    reference,
                    bytes,
                    ..
                } = lane.transfers.remove(id).expect("known transfer");
                let reply = lane
                    .pending
                    .get(&request_id)
                    .expect("known transfer request")
                    .reply;
                let result = if microcrab_host_protocol::sha256(&bytes) == reference.sha256 {
                    serde_json::from_slice(&bytes)
                        .map_err(|_| Error::Io)
                        .and_then(|value| reply.decode(value))
                } else {
                    Err(Error::Io)
                };
                lane.complete(&request_id, result);
            }
        }
        Ok(None)
    }

    pub(super) fn fail_web_request(&self, route: &Route, request_id: &str, operation_id: &str) {
        let mut lane = self.web.lock().unwrap_or_else(|e| e.into_inner());
        if lane
            .pending
            .get(request_id)
            .is_some_and(|pending| pending.operation_id == operation_id)
        {
            lane.complete(request_id, Err(Error::OutcomeUnknown));
            let _ = route.sender.try_send(HostFrame::Cancel {
                request_id: request_id.to_owned(),
                lease_generation: route.lease.clone(),
            });
        }
    }
}

pub(super) fn is_web_request(id: &str) -> bool {
    id.starts_with(REQUEST_PREFIX)
}
