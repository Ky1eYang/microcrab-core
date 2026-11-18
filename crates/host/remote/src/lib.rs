//! Backend execution adapter. A route is replaceable; metadata and workspace are not.
//! Disconnects never replay effects. Unknown receipts gate reattachment explicitly.
mod calls;
mod consumption;
mod diagnostics;
mod files;
mod payload;
mod process;
mod processes;
mod readiness;
mod reconciliation;
mod transfer;
use microcrab_host_api::{
    Capabilities, Host, HostAPI, HostInfo, NetApi, Platform, ProcessApi, ShellApi, ToolError,
};
use microcrab_host_protocol::{HostDescriptor, HostFrame, OperationReceipt};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

const OPERATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
pub struct RemoteHost {
    process_start_admission: tokio::sync::Mutex<()>,
    processes: processes::Processes,
    weak_self: std::sync::Weak<RemoteHost>,
    descriptor: HostDescriptor,
    capabilities: Capabilities,
    scope: Mutex<Option<microcrab_host_protocol::BindingScope>>,
    route: Mutex<Option<Route>>,
    generation: AtomicU64,
    readiness: Mutex<readiness::Readiness>,
    readiness_changed: tokio::sync::Notify,
    pending: Mutex<HashMap<String, Pending>>,
    retired: Mutex<VecDeque<(String, String, u64)>>,
    retired_transfers: Mutex<VecDeque<(String, u64)>>,
    unresolved: Mutex<BTreeMap<String, String>>,
    consumption: Mutex<BTreeMap<String, String>>,
    consumption_recovery: Mutex<std::collections::HashSet<String>>,
    transfers: Mutex<HashMap<String, transfer::Incoming>>,
    reconciliation_gate: tokio::sync::Mutex<()>,
    diagnostics: Mutex<VecDeque<serde_json::Value>>,
    operation_history: Mutex<VecDeque<serde_json::Value>>,
    isolated_receipts: Mutex<BTreeMap<String, String>>,
    files: files::Files,
}
struct Route {
    generation: u64,
    lease: String,
    bridge_instance_id: String,
    sender: mpsc::Sender<HostFrame>,
    stop: CancellationToken,
    last_received: Instant,
}
struct Pending {
    raw_read: bool,
    readiness_operation: bool,
    operation_id: String,
    generation: u64,
    sender: oneshot::Sender<Result<Reply, ToolError>>,
}
enum Reply {
    Value(serde_json::Value),
    Receipt(Option<OperationReceipt>),
    /// Carrier-local failure is not a bridge outcome or a terminal receipt.
    TransportFailure(String),
}
fn unavailable(message: &'static str) -> ToolError {
    ToolError::worker(message)
}

impl RemoteHost {
    pub fn new(descriptor: HostDescriptor) -> Result<Arc<Self>, ToolError> {
        if descriptor.protocol_version != microcrab_host_protocol::PROTOCOL_VERSION
            || descriptor.host_id.is_empty()
            || descriptor.workspace_id.is_empty()
            || descriptor.bridge_instance_id.is_empty()
            || microcrab_host_protocol::descriptor_fingerprint(&descriptor)
                .map_err(|_| unavailable("invalid Host descriptor"))?
                != descriptor.descriptor_digest
        {
            return Err(unavailable("invalid or incompatible Host descriptor"));
        }
        // Only advertised primitives implemented by this adapter are effective.
        let mut capabilities = Capabilities::default();
        for operation in &descriptor.supported_primitives {
            if matches!(
                operation.as_str(),
                "file_mutation"
                    | "filesystem.stat"
                    | "filesystem.open"
                    | "filesystem.read"
                    | "filesystem.close"
                    | "process.spawn"
                    | "process.status"
                    | "process.pid"
                    | "process.wait"
                    | "process.watch"
                    | "process.signal"
                    | "process.kill-tree"
                    | "process.stdout"
                    | "process.stderr"
                    | "process.write-stdin"
                    | "process.close-stdin"
                    | "process.resize-pty"
                    | "process.release"
                    | "shell.launch"
                    | "shell.capture-status"
                    | "shell.read"
                    | "shell.drain"
                    | "shell.write"
            ) {
                capabilities.supported_operations.insert(operation.clone());
            }
        }
        capabilities.process_kill_tree = capabilities.supports("process.kill-tree");
        capabilities.process_pty = capabilities.supports("process.resize-pty");
        Ok(Arc::new_cyclic(|weak_self| Self {
            process_start_admission: tokio::sync::Mutex::new(()),
            processes: processes::Processes::default(),
            weak_self: weak_self.clone(),
            descriptor,
            capabilities,
            scope: Mutex::new(None),
            route: Mutex::new(None),
            generation: AtomicU64::new(0),
            readiness: Mutex::new(readiness::Readiness::default()),
            readiness_changed: tokio::sync::Notify::new(),
            pending: Mutex::new(HashMap::new()),
            retired: Mutex::new(VecDeque::new()),
            retired_transfers: Mutex::new(VecDeque::new()),
            unresolved: Mutex::new(BTreeMap::new()),
            consumption: Mutex::new(BTreeMap::new()),
            consumption_recovery: Mutex::new(std::collections::HashSet::new()),
            transfers: Mutex::new(HashMap::new()),
            reconciliation_gate: tokio::sync::Mutex::new(()),
            diagnostics: Mutex::new(VecDeque::new()),
            operation_history: Mutex::new(VecDeque::new()),
            isolated_receipts: Mutex::new(BTreeMap::new()),
            files: files::Files::default(),
        }))
    }
    pub fn set_scope(&self, scope: microcrab_host_protocol::BindingScope) -> Result<(), ToolError> {
        if !scope.valid() {
            return Err(unavailable("invalid Host binding scope"));
        }
        let mut current = self.scope.lock().unwrap_or_else(|e| e.into_inner());
        if current.as_ref().is_some_and(|previous| previous != &scope) {
            return Err(unavailable("Host binding scope cannot change"));
        }
        *current = Some(scope);
        Ok(())
    }
    fn valid_receipt_scope(&self, receipt: &OperationReceipt) -> bool {
        self.scope
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_some_and(|scope| scope == &receipt.scope)
    }
    pub fn descriptor(&self) -> &HostDescriptor {
        &self.descriptor
    }
    pub fn is_connected(&self) -> bool {
        self.route
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
    }
    pub fn attach(
        self: &Arc<Self>,
        outbound: mpsc::Sender<HostFrame>,
        generation: u64,
    ) -> Result<(), ToolError> {
        self.attach_with_lease(outbound, generation, generation.to_string())
    }
    pub fn attach_with_lease(
        self: &Arc<Self>,
        outbound: mpsc::Sender<HostFrame>,
        generation: u64,
        lease_generation: String,
    ) -> Result<(), ToolError> {
        self.attach_with_bridge_instance(
            outbound,
            generation,
            lease_generation,
            self.descriptor.bridge_instance_id.clone(),
        )
    }
    /// The authenticated Hello supplies this process identity on every attachment.
    pub fn attach_with_bridge_instance(
        self: &Arc<Self>,
        outbound: mpsc::Sender<HostFrame>,
        generation: u64,
        lease_generation: String,
        bridge_instance_id: String,
    ) -> Result<(), ToolError> {
        if generation == 0 || lease_generation.is_empty() || bridge_instance_id.is_empty() {
            return Err(unavailable("invalid Host route generation"));
        }
        let mut route = self.route.lock().unwrap_or_else(|e| e.into_inner());
        let previous = self.generation.load(Ordering::SeqCst);
        if generation <= previous {
            return Err(unavailable("stale Host route generation"));
        }
        self.attached_readiness(generation);
        if let Some(old) = route.take() {
            old.stop.cancel();
        }
        self.fail_pending("Host route replaced; mutation outcome may be unknown");
        let stop = CancellationToken::new();
        *route = Some(Route {
            generation,
            lease: lease_generation.clone(),
            bridge_instance_id,
            sender: outbound.clone(),
            stop: stop.clone(),
            last_received: Instant::now(),
        });
        self.generation.store(generation, Ordering::SeqCst);
        drop(route);
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut ticks = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                tokio::select! { _=stop.cancelled()=>break, _=ticks.tick()=>{} }
                let Some(host) = weak.upgrade() else { break };
                if !host
                    .heartbeat_once(generation, &outbound, &lease_generation)
                    .await
                {
                    break;
                }
            }
        });
        Ok(())
    }
    // One bounded send per tick. A full outbound queue is not evidence that a
    // peer with recent authenticated inbound traffic has disconnected.
    async fn heartbeat_once(
        &self,
        generation: u64,
        outbound: &mpsc::Sender<HostFrame>,
        lease: &str,
    ) -> bool {
        if !self.heartbeat_has_recent_inbound(generation) {
            return false;
        }
        match tokio::time::timeout(
            std::time::Duration::from_secs(2),
            outbound.send(HostFrame::Ping {
                lease_generation: lease.to_owned(),
            }),
        )
        .await
        {
            Ok(Ok(())) => {
                self.heartbeat_warning(generation, false);
                true
            }
            Ok(Err(_)) => {
                self.detach_with_cause(
                    generation,
                    microcrab_host_protocol::ErrorCode::Disconnected,
                    "Host heartbeat outbound route closed",
                );
                false
            }
            Err(_) => {
                // Recheck after the send deadline: inbound traffic may have
                // arrived while blocked, or crossed the silence deadline.
                if !self.heartbeat_has_recent_inbound(generation) {
                    return false;
                }
                self.heartbeat_warning(generation, true);
                true
            }
        }
    }
    fn heartbeat_has_recent_inbound(&self, generation: u64) -> bool {
        let recent = self
            .route
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .filter(|route| route.generation == generation)
            .map(|route| route.last_received.elapsed() < std::time::Duration::from_secs(45));
        match recent {
            Some(true) => true,
            Some(false) => {
                self.detach_with_cause(
                    generation,
                    microcrab_host_protocol::ErrorCode::Timeout,
                    "Host heartbeat received no frame for 45 seconds",
                );
                false
            }
            None => false,
        }
    }
    pub fn detach(&self, generation: u64) {
        self.detach_with_cause(
            generation,
            microcrab_host_protocol::ErrorCode::Disconnected,
            "Host disconnected; mutation outcome may be unknown",
        );
    }
    pub fn detach_with_cause(
        &self,
        generation: u64,
        code: microcrab_host_protocol::ErrorCode,
        message: &'static str,
    ) {
        let mut route = self.route.lock().unwrap_or_else(|e| e.into_inner());
        let current_gen = self.generation.load(Ordering::SeqCst);
        let matches = route
            .as_ref()
            .is_some_and(|route| route.generation == generation);
        if matches || generation >= current_gen {
            self.detached_readiness(readiness::Cause::local(code, message));
            if let Some(old) = route.take() {
                old.stop.cancel();
            }
            self.fail_pending(message);
        }
    }
    fn fail_pending(&self, message: &'static str) {
        self.processes.notify_route_change();
        let pending = std::mem::take(&mut *self.pending.lock().unwrap_or_else(|e| e.into_inner()));
        self.transfers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        for (_, pending) in pending {
            let _ = pending.sender.send(Err(unavailable(message)));
        }
    }
    pub fn unresolved_operations(&self) -> Vec<String> {
        self.unresolved
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect()
    }
}
impl Drop for RemoteHost {
    fn drop(&mut self) {
        if let Some(route) = self
            .route
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            route.stop.cancel();
        }
    }
}
impl HostAPI for RemoteHost {
    fn platform(&self) -> Platform {
        match self.descriptor.platform.as_str() {
            "windows" => Platform::Windows,
            "linux" => Platform::Linux,
            "macos" => Platform::MacOS,
            _ => Platform::Other,
        }
    }
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }
}
#[async_trait::async_trait]
impl NetApi for RemoteHost {}
#[cfg(test)]
mod tests;
