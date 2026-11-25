//! Bounded transport for components importing the web transport interface. The slot
//! owns live and proposed scopes; the guest only sees an opaque reference. In
//! particular, neither guest memory nor an invocation can restore authority.
use super::{
    RuntimeState, ToolContext, authority::WebBinding,
    microcrab::{host0_1_0::web_transport as wit, host0_1_1::web_transport_scoped as scoped_wit},
};
use microcrab_host_api::{Host, OpContext, web_transport as host};
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::runtime::Handle;
use wasmtime::{
    Engine, StoreContextMut,
    component::{Linker, types::ComponentItem},
};

pub(super) use host::ProjectScope;
use host::{ConnectionScope, EndpointKind, WebTransportError as E, WebTransportResult as Result};

pub(super) const INTERFACE: &str = "microcrab:host/web-transport@0.1.0";
pub(super) const SCOPED_INTERFACE: &str = "microcrab:host/web-transport-scoped@0.1.1";
pub(super) const CAPABILITY: &str = "web_transport";
const FUNCTIONS: [&str; 6] = [
    "http-get",
    "ws-open",
    "ws-status",
    "ws-send",
    "ws-receive",
    "ws-close",
];
#[cfg(test)]
mod tests;

pub(super) fn host_binding(host: &dyn Host) -> Option<ProjectScope> {
    host.supports_web_transport()
        .then(|| host.web_transport_binding())
        .flatten()
        .filter(ProjectScope::valid)
}

pub(super) fn allows_ordinary_import(name: &str, item: &ComponentItem, engine: &Engine) -> bool {
    match name {
        super::HOST_INTERFACE => true,
        microcrab_agent_wit::EXECUTION_INTERFACE => {
            // Execution primitives own their own budget and output checks.
            // Process/shell effects are not granted by Web Transport.
            matches!(item, ComponentItem::ComponentInstance(interface)
                if interface.exports(engine).all(|(name, export)| matches!(export.ty, ComponentItem::Type(_) | ComponentItem::Resource(_))
                    || (matches!(name, "create" | "append" | "mark-truncated" | "finish" | "attach-guest")
                        && matches!(export.ty, ComponentItem::ComponentFunc(_)))))
        }
        super::private_bytes::INTERFACE => super::private_bytes::valid_import(item, engine),
        super::guest_observation::INTERFACE | super::guest_observation::BROWSER_INTERFACE => {
            super::guest_observation::valid_import(name, item, engine)
        }
        super::communication::TYPED_VARIABLES_INTERFACE => {
            // Permit only admission facts, not access to Session variables.
            matches!(item, ComponentItem::ComponentInstance(interface)
                if interface.exports(engine).all(|(name, export)| matches!(export.ty, ComponentItem::Type(_))
                    || (name == "tool-call-facts" && matches!(export.ty, ComponentItem::ComponentFunc(_)))))
        }
        "tool-output" | "tool-failure" | "driver-step" | "init-result" | "init-error" => {
            matches!(item, ComponentItem::Type(_))
        }
        "buffer" => matches!(item, ComponentItem::Resource(_)),
        "microcrab:host/types@0.1.0"
        | "microcrab:host/tool-types@0.1.0"
        | "microcrab:host/process@0.1.0"
        | "microcrab:host/shell@0.1.0" => {
            // Canonical execution refers to Host types; that does not allow importing effects.
            matches!(item, ComponentItem::ComponentInstance(interface)
                if interface.exports(engine).all(|(_, export)| matches!(export.ty, ComponentItem::Type(_))))
        }
        _ => false,
    }
}

pub(super) fn validate_interface(
    item: &ComponentItem,
    engine: &Engine,
) -> std::result::Result<(), crate::Error> {
    validate_functions(item, engine, FUNCTIONS, "component must import the complete web transport interface")
}

pub(super) fn validate_scoped_interface(
    item: &ComponentItem,
    engine: &Engine,
) -> std::result::Result<(), crate::Error> {
    validate_functions(item, engine, ["ws-pull", "ws-budget", "ws-settle"], "component must import the complete scoped web transport interface")
}

fn validate_functions<const N: usize>(
    item: &ComponentItem,
    engine: &Engine,
    expected: [&str; N],
    error: &'static str,
) -> std::result::Result<(), crate::Error> {
    let ComponentItem::ComponentInstance(interface) = item else {
        return Err(crate::Error::wasm("invalid web transport interface"));
    };
    let functions: std::collections::HashSet<_> = interface
        .exports(engine)
        .filter_map(|(name, export)| {
            matches!(export.ty, ComponentItem::ComponentFunc(_)).then_some(name)
        })
        .collect();
    if functions != std::collections::HashSet::from(expected) {
        return Err(crate::Error::wasm(error));
    }
    // The Runtime linker then checks the signatures and type identities.
    Ok(())
}

struct Endpoint {
    exact: String,
    url: reqwest::Url,
    kind: EndpointKind,
}

impl Endpoint {
    fn parse(input: &str) -> Result<Self> {
        if input.len() > host::WEB_MAX_ENDPOINT_BYTES {
            return Err(E::Limit);
        }
        if input.contains("{{secret:") || input.contains("{{var:") {
            return Err(E::InvalidRequest);
        }
        let (exact, kind) = if let Some((scheme, rest)) = input.split_once("://") {
            let kind = if ["ws", "wss"].iter().any(|s| scheme.eq_ignore_ascii_case(s)) {
                EndpointKind::WebSocket
            } else {
                EndpointKind::Http
            };
            // Derive only the admitted HTTP(S) authority's standard path;
            // preserve the spelling of explicit paths, queries and WS URLs.
            let suffix = rest.find(['/', '?']).map_or("", |index| &rest[index..]);
            let exact = if kind == EndpointKind::Http && matches!(suffix, "" | "/") {
                parse_url(input, kind)?;
                format!("{}/json/version", input.trim_end_matches('/'))
            } else {
                input.to_owned()
            };
            (exact, kind)
        } else {
            // Bare authority must have an explicit port, including when it's the
            // scheme default. Parse its spelling separately from URL normalization.
            let (name, port) = input.rsplit_once(':').ok_or(E::InvalidRequest)?;
            if name.is_empty()
                || name.contains(['/', '?'])
                || (name.contains(':') && !(name.starts_with('[') && name.ends_with(']')))
                || port.is_empty()
                || !port.bytes().all(|b| b.is_ascii_digit())
                || port.parse::<u16>().ok().is_none_or(|port| port == 0)
            {
                return Err(E::InvalidRequest);
            }
            (format!("http://{input}/json/version"), EndpointKind::Http)
        };
        let url = parse_url(&exact, kind)?;
        Ok(Self { exact, url, kind })
    }

    fn permits_ws(&self, candidate: &str, discovered: bool) -> Result<()> {
        let candidate_url = parse_url(candidate, EndpointKind::WebSocket)?;
        let permitted = match self.kind {
            EndpointKind::WebSocket => self.exact == candidate,
            EndpointKind::Http => {
                discovered
                    && self.url.host() == candidate_url.host()
                    && self.url.port_or_known_default() == candidate_url.port_or_known_default()
                    && (self.url.scheme() != "https" || candidate_url.scheme() == "wss")
            }
        };
        if permitted { Ok(()) } else { Err(E::Denied) }
    }
}

fn parse_url(exact: &str, kind: EndpointKind) -> Result<reqwest::Url> {
    host::validate_endpoint_shape(exact, kind)?;
    if exact.contains("{{secret:") || exact.contains("{{var:") {
        return Err(E::InvalidRequest);
    }
    let url = reqwest::Url::parse(exact).map_err(|_| E::InvalidRequest)?;
    let authority = exact
        .split_once("://")
        .ok_or(E::InvalidRequest)?
        .1
        .split(['/', '?'])
        .next()
        .ok_or(E::InvalidRequest)?;
    if !url.has_host()
        || url.port_or_known_default().is_none_or(|port| port == 0)
        || authority.ends_with(':')
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(E::InvalidRequest);
    }
    Ok(url)
}

struct OwnedScope {
    scope: ConnectionScope,
    close: Arc<ScopeClose>,
    closed: bool,
}

struct ScopeClose {
    scope: ConnectionScope,
    backend: Arc<dyn Host>,
    runtime: Handle,
    gate: tokio::sync::Mutex<()>,
    acknowledged: std::sync::atomic::AtomicBool,
}

impl ScopeClose {
    async fn acknowledge(&self) -> Result<()> {
        let _gate = self.gate.lock().await;
        if self.is_acknowledged() {
            return Ok(());
        }
        let deadline = Instant::now() + Duration::from_millis(u64::from(host::WEB_MAX_BUDGET_MS));
        let context = OpContext {
            deadline: Some(deadline),
            principal: self.scope.session_id.clone(),
            ..Default::default()
        };
        let _cancel = context.cancel.clone().drop_guard();
        // Never release an entire epoch: a newer slot may already exist.
        match tokio::time::timeout_at(deadline.into(), self.backend.ws_close(&context, &self.scope)).await {
            Ok(Ok(()) | Err(E::Closed | E::Stale)) => {
                self.mark_acknowledged();
                Ok(())
            }
            Ok(Err(error)) => Err(error),
            Err(_) => Err(E::Timeout),
        }
    }

    fn is_acknowledged(&self) -> bool {
        self.acknowledged.load(std::sync::atomic::Ordering::Acquire)
    }

    fn mark_acknowledged(&self) {
        self.acknowledged.store(true, std::sync::atomic::Ordering::Release);
    }

    fn schedule(self: &Arc<Self>) {
        let close = self.clone();
        self.runtime.spawn(async move { let _ = close.acknowledge().await; });
    }
}

impl Drop for OwnedScope {
    fn drop(&mut self) {
        if !self.closed {
            self.close.schedule();
        }
    }
}

/// Receive-only authority for one connection and one accepted continuation.
pub(super) struct Continuation {
    slot: Arc<SlotOwner>,
    close: Arc<ScopeClose>,
    identity: String,
}

struct ConnectionController {
    slot: Arc<SlotOwner>,
    close: Arc<ScopeClose>,
}

#[async_trait::async_trait]
impl crate::runtime::execution::ExecutionController for ConnectionController {
    async fn cancel(&self, cancellation: tokio_util::sync::CancellationToken) -> std::result::Result<(), crate::Error> {
        self.release(cancellation).await
    }

    async fn release(&self, _: tokio_util::sync::CancellationToken) -> std::result::Result<(), crate::Error> {
        self.slot.forget(&self.close.scope, false);
        self.close.acknowledge().await.map_err(|error| crate::Error::Tool(error.to_string().into()))
    }
}

impl Continuation {
    pub(super) fn controller(&self) -> Arc<dyn crate::runtime::execution::ExecutionController> {
        Arc::new(ConnectionController { slot: self.slot.clone(), close: self.close.clone() })
    }
}

impl Continuation {
    pub(super) async fn cleanup(&self) -> Result<()> {
        self.slot.forget(&self.close.scope, false);
        self.close.acknowledge().await
    }
}

impl Continuation {
    pub(super) fn step(&self) -> Invocation {
        Invocation {
            slot: self.slot.clone(),
            binding: Some(self.close.scope.binding.clone()),
            identity: self.identity.clone(),
            endpoint: None,
            connection: Some(self.close.scope.connection_id.clone()),
            send: false,
            handoff: false,
            driven: true,
            discovery_attempted: true,
            discovered: false,
            open_attempted: true,
            proposal: None,
            opened: None,
            settled: false,
            guest_success: true,
            entered: true,
            command_started: false,
            transport_deadline: None,
            scoped: Some(ScopedInvocation::default()),
        }
    }
}

impl Drop for Continuation {
    fn drop(&mut self) {
        self.slot.forget(&self.close.scope, false);
        self.close.schedule();
    }
}

#[derive(Default)]
struct SlotState {
    retired: bool,
    pending: Option<OwnedScope>,
    active: Option<OwnedScope>,
    // A forgotten connection is fenced immediately, but its cleanup evidence
    // must not disappear before the worker is allowed to recover.
    closing: Vec<Arc<ScopeClose>>,
}

impl SlotState {
    const MAX_UNACKNOWLEDGED_SCOPES: usize = 8;

    fn detach(&mut self, mut owner: OwnedScope, acknowledged: bool) {
        if acknowledged {
            owner.close.mark_acknowledged();
        } else if !owner.close.is_acknowledged() {
            self.closing.push(owner.close.clone());
        }
        owner.closed = acknowledged;
    }
}

#[derive(Default)]
pub(super) struct SlotOwner(Mutex<SlotState>);

impl SlotOwner {
    fn lock(&self) -> std::sync::MutexGuard<'_, SlotState> {
        self.0.lock().unwrap_or_else(|error| error.into_inner())
    }

    pub(super) fn retire(&self) {
        let mut slot = self.lock();
        slot.retired = true;
        if let Some(owner) = slot.pending.take() {
            slot.detach(owner, false);
        }
        if let Some(owner) = slot.active.take() {
            slot.detach(owner, false);
        }
    }

    pub(super) async fn acknowledge_retirement(&self) -> Result<()> {
        let closing = self.lock().closing.clone();
        let results = futures_util::future::join_all(closing.iter().map(|close| close.acknowledge())).await;
        results.into_iter().collect::<Result<Vec<_>>>().map(|_| ())
    }

    fn prepare(&self, owner: OwnedScope) -> Result<ConnectionScope> {
        let mut slot = self.lock();
        if slot.retired {
            return Err(E::Cancelled);
        }
        // Reserve receipt capacity before any Host effect. Existing active
        // authority stays usable; uncertain closes cannot accumulate unboundedly.
        slot.closing.retain(|close| !close.is_acknowledged());
        if slot.pending.is_some() {
            return Err(E::Denied);
        }
        if slot.closing.len() + usize::from(slot.active.is_some()) >= SlotState::MAX_UNACKNOWLEDGED_SCOPES {
            return Err(E::Limit);
        }
        let scope = owner.scope.clone();
        slot.pending = Some(owner);
        Ok(scope)
    }

    fn ready_to_open(&self) -> Result<()> {
        let slot = self.lock();
        if slot.retired {
            return Err(E::Cancelled);
        }
        if slot.active.is_some() {
            return Err(E::Denied);
        }
        Ok(())
    }

    fn promote(&self, scope: &ConnectionScope) -> Result<()> {
        let mut slot = self.lock();
        if slot.retired {
            return Err(E::Cancelled);
        }
        if slot.active.is_some() || !slot.pending.as_ref().is_some_and(|p| p.scope == *scope) {
            return Err(E::Stale);
        }
        slot.active = slot.pending.take();
        Ok(())
    }

    fn active(&self, id: &str) -> Result<ConnectionScope> {
        let slot = self.lock();
        if slot.retired {
            return Err(E::Cancelled);
        }
        slot.active
            .as_ref()
            .filter(|owner| owner.scope.connection_id == id)
            .map(|owner| owner.scope.clone())
            .ok_or(E::Stale)
    }

    fn forget(&self, scope: &ConnectionScope, acknowledged: bool) {
        let mut slot = self.lock();
        if slot
            .active
            .as_ref()
            .is_some_and(|owner| owner.scope == *scope)
            && let Some(owner) = slot.active.take()
        {
            slot.detach(owner, acknowledged);
        }
    }

    fn settle(&self, discard_active: bool, failed_open: Option<&str>) {
        let mut slot = self.lock();
        if let Some(owner) = slot.pending.take() {
            slot.detach(owner, false);
        }
        if (discard_active
            || failed_open.is_some_and(|id| {
                slot.active
                    .as_ref()
                    .is_some_and(|owner| owner.scope.connection_id == id)
            }))
            && let Some(owner) = slot.active.take()
        {
            slot.detach(owner, false);
        }
    }
}

pub(super) struct Invocation {
    slot: Arc<SlotOwner>,
    binding: Option<ProjectScope>,
    identity: String,
    endpoint: Option<Endpoint>,
    connection: Option<String>,
    send: bool,
    handoff: bool,
    driven: bool,
    discovery_attempted: bool,
    discovered: bool,
    open_attempted: bool,
    proposal: Option<ConnectionScope>,
    opened: Option<String>,
    settled: bool,
    guest_success: bool,
    entered: bool,
    command_started: bool,
    transport_deadline: Option<Instant>,
    scoped: Option<ScopedInvocation>,
}

#[derive(Default)]
struct ScopedInvocation {
    touched: Option<ConnectionScope>,
    empty: Option<ConnectionScope>,
    acknowledged: Option<ConnectionScope>,
    violated: bool,
    handoff_ready: bool,
}

impl Invocation {
    pub(super) fn new(
        slot: Arc<SlotOwner>,
        binding: Option<ProjectScope>,
        identity: String,
        context: &ToolContext,
        dispatched: &serde_json::Value,
        authority: &WebBinding,
    ) -> std::result::Result<Self, crate::Error> {
        let (endpoint, connection, send, handoff) = match authority {
            WebBinding::Open { endpoint, handoff } => {
                let exact = endpoint.bind(context, dispatched)?;
                let endpoint = Endpoint::parse(exact)
                    .map_err(|error| crate::Error::Tool(error.to_string().into()))?;
                (Some(endpoint), None, true, *handoff)
            }
            WebBinding::Existing { connection, send } => {
                let connection = connection.bind(context, dispatched)?.to_owned();
                (None, Some(connection), *send, false)
            }
        };
        Ok(Self {
            slot,
            binding,
            identity,
            endpoint,
            connection,
            send,
            handoff,
            driven: false,
            discovery_attempted: false,
            discovered: false,
            open_attempted: false,
            proposal: None,
            opened: None,
            settled: false,
            guest_success: false,
            entered: false,
            command_started: false,
            transport_deadline: None,
            scoped: None,
        })
    }

    pub(super) fn take_handoff(&mut self) -> std::result::Result<Continuation, crate::Error> {
        if !self.requires_handoff() {
            return Err(crate::Error::wasm("missing declared connection handoff"));
        }
        let id = self.opened.as_deref().ok_or_else(|| crate::Error::wasm("missing connection for continuation"))?;
        let slot = self.slot.lock();
        let owner = slot.active.as_ref().filter(|owner| owner.scope.connection_id == id);
        let close = owner.map(|owner| owner.close.clone())
            .ok_or_else(|| crate::Error::wasm("connection is no longer available for handoff"))?;
        if let Some(scoped) = &self.scoped
            && scoped.handoff_ready && !scoped.violated && scoped.acknowledged.as_ref() == Some(&close.scope)
        {
            return Ok(Continuation { slot: self.slot.clone(), close, identity: self.identity.clone() });
        }
        Err(crate::Error::wasm("connection handoff requires a proven scoped receive and settlement"))
    }

    pub(super) fn requires_handoff(&self) -> bool {
        self.handoff && self.opened.is_some()
    }

    pub(super) fn with_scoped_transport(mut self, enabled: bool) -> Self {
        self.scoped = enabled.then(ScopedInvocation::default);
        self
    }

    fn touched(&mut self, scope: &ConnectionScope) {
        if let Some(scoped) = &mut self.scoped {
            scoped.touched = Some(scope.clone());
        }
    }

    pub(super) fn enter(&mut self) {
        self.entered = true;
    }

    pub(super) fn guest_returned(&mut self, success: bool) {
        self.guest_success = success;
    }

    fn settle(&mut self, scoped_ack_valid: bool) {
        if self.driven {
            self.settled = true;
            return;
        }
        let scoped_unproved = self.scoped.as_ref().is_some_and(|scoped| {
            (scoped.touched.is_some() || self.opened.is_some()) && !scoped_ack_valid
        });
        if scoped_unproved && let Some(scope) = self.scoped.as_ref().and_then(|scoped| scoped.touched.as_ref()) {
            self.slot.forget(scope, false);
        }
        self.slot.settle(
            self.scoped.is_none() && !self.guest_success && self.command_started,
            if self.guest_success && !scoped_unproved {
                None
            } else {
                self.opened.as_deref()
            },
        );
        self.settled = true;
    }
}

impl Drop for Invocation {
    fn drop(&mut self) {
        // On cancellation, deadline, trap or panic forget even an older active
        // connection: an admitted command may have been sent/consumed.
        if self.entered && !self.settled && !self.driven {
            self.slot.settle(true, None);
        }
    }
}

fn authorize(state: &RuntimeState) -> Result<Arc<dyn Host>> {
    let invocation = state.web.as_ref().filter(|call| call.entered).ok_or(E::Denied)?;
    let context = state
        .tool_context
        .as_ref()
        .filter(|context| context.subject.is_tool())
        .ok_or(E::Denied)?;
    context
        .ensure_admitted(Some(&invocation.identity))
        .map_err(|error| match error {
            crate::policy::PermissionError::Cancelled => E::Cancelled,
            _ => E::Denied,
        })?;
    if state.binding.is_none() {
        return Err(E::Denied);
    }
    if state.cancellation.is_cancelled() || state.epoch_cancellation.is_cancelled() {
        return Err(E::Cancelled);
    }
    let start = Instant::now();
    if start >= state.deadline {
        return Err(E::Timeout);
    }
    invocation.binding.as_ref().ok_or(E::Unsupported)?;
    state.host.clone().ok_or(E::Unsupported)
}

fn check_route(state: &mut RuntimeState, backend: &dyn Host) -> Result<()> {
    let invocation = state.web.as_mut().ok_or(E::Denied)?;
    if host_binding(backend).as_ref() != invocation.binding.as_ref() {
        // Never resume an active socket if its route disappears then returns.
        invocation.slot.settle(true, None);
        invocation.open_attempted = true;
        invocation.discovery_attempted = true;
        return Err(E::Unsupported);
    }
    Ok(())
}

fn charge(state: &mut RuntimeState, input: usize) -> Result<()> {
    if state.operation_calls_remaining == 0 || input > state.input_bytes_remaining {
        return Err(E::Limit);
    }
    state.operation_calls_remaining -= 1;
    state.input_bytes_remaining -= input;
    Ok(())
}

fn begin(
    state: &mut RuntimeState,
    input: usize,
) -> Result<(Arc<dyn Host>, OpContext)> {
    let backend = authorize(state)?;
    let start = Instant::now();
    check_route(state, backend.as_ref())?;
    let invocation = state.web.as_mut().ok_or(E::Denied)?;
    if let Some(scoped) = &mut invocation.scoped {
        // Once acknowledged, no more protocol I/O belongs to this invocation.
        if scoped.acknowledged.is_some() {
            scoped.violated = true;
            return Err(E::Denied);
        }
        scoped.empty = None;
    }
    charge(state, input)?;
    let context = state.tool_context.as_ref().ok_or(E::Denied)?;
    let invocation = state.web.as_mut().ok_or(E::Denied)?;
    // One wall window from the first I/O attempt, never renewed by another
    // import. Bound total invocation time by guest allowance plus this window.
    let deadline = *invocation
        .transport_deadline
        .get_or_insert_with(|| start + Duration::from_millis(u64::from(host::WEB_MAX_BUDGET_MS)));
    if start >= deadline {
        invocation.slot.settle(true, None);
        return Err(E::Timeout);
    }
    Ok((
        backend,
        OpContext {
            deadline: Some(deadline),
            cancel: state.cancellation.child_token(),
            principal: context.session_id.clone(),
            trace_id: context.call_id.clone(),
        },
    ))
}

fn run<T>(
    state: &mut RuntimeState,
    context: &OpContext,
    phase: &'static str,
    operation: impl Future<Output = Result<T>>,
) -> Result<T> {
    let runtime = state.runtime.as_ref().ok_or(E::Unsupported)?;
    let deadline = context.deadline.ok_or(E::Timeout)?;
    let _cancel = context.cancel.clone().drop_guard();
    let start = Instant::now();
    let (source, result) = runtime.block_on(async {
        tokio::select! { biased;
            _ = state.epoch_cancellation.cancelled() => ("epoch_cancelled", Err(E::Cancelled)),
            _ = context.cancel.cancelled() => ("call_cancelled", Err(E::Cancelled)),
            _ = tokio::time::sleep_until(deadline.into()) => ("runtime_deadline", Err(E::OutcomeUnknown)),
            result = operation => ("host_reply", result),
        }
    });
    if let Err(error) = &result {
        let execution_id = state.tool_context.as_ref().and_then(|context| context.execution_id).map(|id| id.0);
        tracing::warn!(target: "microcrab_session::runtime::wasm", observation = "web_transport_failure", phase, source, ?execution_id,
            operation_sha256 = %crate::observation::wasm::digest(context.trace_id.as_bytes()),
            session_sha256 = %crate::observation::wasm::digest(context.principal.as_bytes()),
            category = ?error, elapsed_ms = start.elapsed().as_millis() as u64,
            budget_ms = deadline.saturating_duration_since(start).as_millis() as u64,
            "scoped WASM web transport failed");
    }
    // Network waiting is outside the guest CPU wall allowance, but can only
    // add time within the one transport window. Fuel/cancellation still apply.
    state.deadline += start
        .elapsed()
        .min(deadline.saturating_duration_since(start));
    result
}

fn proposal(state: &mut RuntimeState, backend: Arc<dyn Host>) -> Result<ConnectionScope> {
    let invocation = state.web.as_mut().ok_or(E::Denied)?;
    if let Some(scope) = &invocation.proposal {
        return Ok(scope.clone());
    }
    let context = state.tool_context.as_ref().ok_or(E::Denied)?;
    let scope = ConnectionScope {
        binding: invocation.binding.clone().ok_or(E::Unsupported)?,
        session_id: context.session_id.clone(),
        epoch: context.state.snapshot().epoch_id,
        connection_id: uuid::Uuid::new_v4().simple().to_string(),
    };
    if !scope.valid() {
        return Err(E::Denied);
    }
    let close = Arc::new(ScopeClose {
        scope: scope.clone(),
        backend,
        runtime: state.runtime.clone().ok_or(E::Unsupported)?,
        gate: Default::default(),
        acknowledged: Default::default(),
    });
    let scope = invocation.slot.prepare(OwnedScope { scope, close, closed: false })?;
    invocation.proposal = Some(scope.clone());
    Ok(scope)
}

fn http_get(state: &mut RuntimeState, url: String, max: u32) -> Result<wit::HttpResponse> {
    let (backend, context) = begin(state, url.len().saturating_add(4))?;
    host::validate_limit(max, host::WEB_MAX_HTTP_BODY_BYTES)?;
    if max as usize > state.output_bytes_remaining {
        return Err(E::Limit);
    }
    let invocation = state.web.as_mut().ok_or(E::Denied)?;
    let endpoint = invocation.endpoint.as_ref().ok_or(E::Denied)?;
    if invocation.discovery_attempted
        || endpoint.kind != EndpointKind::Http
        || endpoint.exact != url
    {
        return Err(E::Denied);
    }
    invocation.discovery_attempted = true;
    let scope = proposal(state, backend.clone())?;
    let response = run(
        state,
        &context,
        "http_get",
        backend.http_get(&context, &scope, &url, max),
    )?;
    response.validate(max)?;
    state.output_bytes_remaining -= response.body.len();
    state.web.as_mut().ok_or(E::Denied)?.discovered = (200..300).contains(&response.status);
    Ok(wit::HttpResponse {
        status: response.status,
        body: response.body,
    })
}

fn ws_open(state: &mut RuntimeState, url: String) -> Result<String> {
    let (backend, context) = begin(state, url.len())?;
    let invocation = state.web.as_mut().ok_or(E::Denied)?;
    let endpoint = invocation.endpoint.as_ref().ok_or(E::Denied)?;
    if invocation.open_attempted {
        return Err(E::Denied);
    }
    invocation.open_attempted = true;
    endpoint.permits_ws(&url, invocation.discovered)?;
    invocation.slot.ready_to_open()?;
    let scope = proposal(state, backend.clone())?;
    run(state, &context, "ws_open", backend.ws_open(&context, &scope, &url))?;
    let invocation = state.web.as_mut().ok_or(E::Denied)?;
    invocation.slot.promote(&scope)?;
    invocation.opened = Some(scope.connection_id.clone());
    Ok(scope.connection_id)
}

fn active(state: &RuntimeState, id: &str, command: bool) -> Result<ConnectionScope> {
    let invocation = state.web.as_ref().ok_or(E::Denied)?;
    if invocation.connection.as_deref().or(invocation.opened.as_deref()) != Some(id)
        || command && !invocation.send
    {
        return Err(E::Denied);
    }
    let scope = invocation.slot.active(id)?;
    let context = state.tool_context.as_ref().ok_or(E::Denied)?;
    if scope.session_id != context.session_id
        || scope.epoch != context.state.snapshot().epoch_id
        || invocation.binding.as_ref() != Some(&scope.binding)
    {
        return Err(E::Stale);
    }
    Ok(scope)
}

fn resolve_active<T>(
    state: &RuntimeState,
    scope: &ConnectionScope,
    result: Result<T>,
) -> Result<T> {
    if result.is_err() {
        // No later invocation may implicitly resume a command whose send/read
        // outcome is not known. Close the exact scope on the original route.
        if let Some(invocation) = &state.web {
            invocation.slot.forget(scope, false);
        }
    }
    result
}

fn ws_status(state: &mut RuntimeState, id: String) -> Result<()> {
    let (backend, context) = begin(state, id.len())?;
    let scope = active(state, &id, false)?;
    let result = run(state, &context, "ws_status", backend.ws_status(&context, &scope));
    resolve_active(state, &scope, result)
}

fn ws_send(state: &mut RuntimeState, id: String, text: String) -> Result<()> {
    let (backend, context) = begin(state, id.len().saturating_add(text.len()))?;
    if text.len() > host::WEB_MAX_WS_SEND_BYTES {
        return Err(E::Limit);
    }
    let scope = active(state, &id, true)?;
    let invocation = state.web.as_mut().ok_or(E::Denied)?;
    invocation.command_started = true;
    invocation.touched(&scope);
    let result = run(state, &context, "ws_send", backend.ws_send(&context, &scope, &text));
    resolve_active(state, &scope, result)
}

fn ws_receive(state: &mut RuntimeState, id: String, max: u32) -> Result<wit::WsReceiveResult> {
    let (backend, context) = begin(state, id.len().saturating_add(4))?;
    host::validate_limit(max, host::WEB_MAX_WS_RECEIVE_BYTES)?;
    if max as usize > state.output_bytes_remaining {
        return Err(E::Limit);
    }
    let scope = active(state, &id, false)?;
    let invocation = state.web.as_mut().ok_or(E::Denied)?;
    if invocation.driven {
        return Err(E::Denied);
    }
    invocation.command_started = true;
    invocation.touched(&scope);
    let result =
        run(state, &context, "ws_receive", backend.ws_receive(&context, &scope, max)).and_then(|result| {
            result.validate(max)?;
            Ok(result)
        });
    Ok(match resolve_active(state, &scope, result)? {
        host::WsReceive::Message(text) => {
            state.output_bytes_remaining -= text.len();
            wit::WsReceiveResult::Message(text)
        }
        host::WsReceive::Timeout => {
            state
                .web
                .as_ref()
                .ok_or(E::Denied)?
                .slot
                .forget(&scope, false);
            wit::WsReceiveResult::Timeout
        }
        host::WsReceive::Closed => {
            state
                .web
                .as_ref()
                .ok_or(E::Denied)?
                .slot
                .forget(&scope, false);
            wit::WsReceiveResult::Closed
        }
    })
}

fn scoped_admission(state: &mut RuntimeState, id: &str) -> Result<ConnectionScope> {
    let backend = authorize(state)?;
    check_route(state, backend.as_ref())?;
    let invocation = state.web.as_ref().ok_or(E::Denied)?;
    if invocation.scoped.is_none() {
        return Err(E::Denied);
    }
    if invocation.transport_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        invocation.slot.settle(true, None);
        return Err(E::Timeout);
    }
    active(state, id, false)
}

fn ws_budget(state: &mut RuntimeState, id: String) -> Result<scoped_wit::WsBudgetResult> {
    scoped_admission(state, &id)?;
    charge(state, id.len())?;
    let now = Instant::now();
    let remaining_ms = |deadline: Instant| {
        u32::try_from(deadline.saturating_duration_since(now).as_millis()).unwrap_or(u32::MAX)
    };
    let invocation = state.web.as_ref().ok_or(E::Denied)?;
    Ok(scoped_wit::WsBudgetResult {
        transport_ms: invocation.transport_deadline.map_or(host::WEB_MAX_BUDGET_MS, remaining_ms),
        guest_ms: remaining_ms(state.deadline),
    })
}

fn ws_pull(state: &mut RuntimeState, id: String, max: u32, soft_wait: u32) -> Result<scoped_wit::WsPullResult> {
    let (backend, context) = begin(state, id.len().saturating_add(8))?;
    host::validate_limit(max, host::WEB_MAX_WS_RECEIVE_BYTES)?;
    if max as usize > state.output_bytes_remaining || soft_wait > host::WEB_MAX_WS_SOFT_WAIT_MS {
        return Err(E::Limit);
    }
    let scope = active(state, &id, false)?;
    let invocation = state.web.as_mut().ok_or(E::Denied)?;
    if invocation.scoped.is_none() {
        return Err(E::Denied);
    }
    invocation.touched(&scope);
    let result = run(state, &context, "ws_pull", backend.ws_pull(&context, &scope, max, soft_wait))
        .and_then(|value| match &value {
            host::WsPull::Message(text) if text.len() > max as usize => Err(E::Limit),
            _ => Ok(value),
        });
    let result = resolve_active(state, &scope, result)?;
    if let Some(scoped) = state.web.as_mut().and_then(|call| call.scoped.as_mut()) {
        scoped.handoff_ready |= !matches!(result, host::WsPull::Closed);
    }
    Ok(match result {
        host::WsPull::Message(text) => {
            state.output_bytes_remaining -= text.len();
            scoped_wit::WsPullResult::Message(text)
        }
        host::WsPull::Empty => {
            if soft_wait == 0 {
                state.web.as_mut().and_then(|call| call.scoped.as_mut()).ok_or(E::Denied)?.empty = Some(scope);
            }
            scoped_wit::WsPullResult::Empty
        }
        host::WsPull::Closed => {
            state.web.as_ref().ok_or(E::Denied)?.slot.forget(&scope, false);
            scoped_wit::WsPullResult::Closed
        }
    })
}

fn ws_settle(state: &mut RuntimeState, id: String) -> Result<()> {
    let scope = scoped_admission(state, &id)?;
    charge(state, id.len())?;
    let scoped = state.web.as_mut().and_then(|call| call.scoped.as_mut()).ok_or(E::Denied)?;
    if scoped.violated || scoped.acknowledged.is_some() || scoped.empty.take().as_ref() != Some(&scope) {
        return Err(E::Denied);
    }
    scoped.acknowledged = Some(scope);
    Ok(())
}

/// Called only after an accepted guest return and Runtime's normal gates.
/// The local mark cannot restore an absent/replaced route or bless later I/O.
pub(super) fn finish_invocation(state: &mut RuntimeState) {
    let acknowledged = state.web.as_ref()
        .and_then(|call| call.scoped.as_ref())
        .filter(|scoped| !scoped.violated)
        .and_then(|scoped| scoped.acknowledged.clone());
    let valid = acknowledged.is_some_and(|scope| {
        scoped_admission(state, &scope.connection_id).is_ok_and(|current| current == scope)
    });
    if let Some(invocation) = state.web.as_mut() {
        invocation.settle(valid);
    }
}

fn ws_close(state: &mut RuntimeState, id: String) -> Result<()> {
    let (backend, context) = begin(state, id.len())?;
    let scope = active(state, &id, false)?;
    let result = run(state, &context, "ws_close", backend.ws_close(&context, &scope));
    let invocation = state.web.as_mut().ok_or(E::Denied)?;
    let acknowledged = matches!(result, Ok(()) | Err(E::Closed | E::Stale));
    invocation.slot.forget(&scope, acknowledged);
    if !acknowledged {
        invocation.open_attempted = true;
    }
    result
}

fn wire(error: E) -> wit::TransportError {
    match error {
        E::Unsupported => wit::TransportError::Unsupported,
        E::Denied => wit::TransportError::Denied,
        E::InvalidRequest => wit::TransportError::InvalidRequest,
        E::Limit => wit::TransportError::Limit,
        E::Cancelled => wit::TransportError::Cancelled,
        E::Timeout => wit::TransportError::Timeout,
        E::Stale => wit::TransportError::Stale,
        E::Closed => wit::TransportError::Closed,
        E::OutcomeUnknown => wit::TransportError::OutcomeUnknown,
        E::Io => wit::TransportError::Io,
    }
}

pub(super) fn link(linker: &mut Linker<RuntimeState>, scoped: bool) -> wasmtime::Result<()> {
    let mut interface = linker.instance(INTERFACE)?;
    macro_rules! register {
        ($name:literal, $function:ident, ($($arg:ident : $ty:ty),+)) => {
            interface.func_wrap($name, |mut caller: StoreContextMut<'_, RuntimeState>, ($($arg,)+): ($($ty,)+)| {
                Ok(($function(caller.data_mut(), $($arg),+).map_err(wire),))
            })?;
        };
    }
    register!("http-get", http_get, (url: String, max: u32));
    register!("ws-open", ws_open, (url: String));
    register!("ws-status", ws_status, (id: String));
    register!("ws-send", ws_send, (id: String, text: String));
    register!("ws-receive", ws_receive, (id: String, max: u32));
    register!("ws-close", ws_close, (id: String));
    if scoped {
        let mut companion = linker.instance(SCOPED_INTERFACE)?;
        companion.func_wrap("ws-pull", |mut caller: StoreContextMut<'_, RuntimeState>, (id, max, soft_wait): (String, u32, u32)| {
            Ok((ws_pull(caller.data_mut(), id, max, soft_wait).map_err(wire),))
        })?;
        companion.func_wrap("ws-budget", |mut caller: StoreContextMut<'_, RuntimeState>, (id,): (String,)| {
            Ok((ws_budget(caller.data_mut(), id).map_err(wire),))
        })?;
        companion.func_wrap("ws-settle", |mut caller: StoreContextMut<'_, RuntimeState>, (id,): (String,)| {
            Ok((ws_settle(caller.data_mut(), id).map_err(wire),))
        })?;
    }
    Ok(())
}
