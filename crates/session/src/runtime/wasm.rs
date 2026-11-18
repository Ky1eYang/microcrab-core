//! Agent components with epoch-owned stores, per-call budgets, and narrow imports.
//! Discovery can read frozen HostInfo but cannot execute Host capabilities.
//! Tool execution follows the Session permission gate; no WASI linker is installed.

#[cfg(test)]
mod cleanup_tests;
mod communication;
mod driver;
mod epoch;
mod lifecycle;
#[cfg(test)]
mod media_tests;
pub use lifecycle::AgentLifecycle;
mod operations;
mod output;
mod resources;
#[cfg(test)]
pub(crate) mod test_admission;
pub(crate) use epoch::EpochInstances;

use super::execution::ToolOutput as NativeToolOutput;
use super::tool::{Tool, ToolContext};
use crate::Error;
use crate::state::wasm::SessionBinding;
use crate::utils::cancel::CancelOnDrop;
use async_trait::async_trait;
use communication::{
    host_info_import, scoped_variable_change_import, variable_change_import,
    variable_get_all_import, variable_get_import, variable_metadata_import,
};
use microcrab_agent_types::AgentInfo;
use microcrab_host_api::Host;
use microcrab_protocol::ToolDefinition;
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use wasmtime::component::{Component, Linker, types::ComponentItem};
use wasmtime::{Config, Engine, Store, StoreLimits, StoreLimitsBuilder, UpdateDeadline};

include!(concat!(env!("OUT_DIR"), "/runtime-bindings.rs"));
use microcrab_agent_wit::HOST_INTERFACE;

/// All budgets are host-owned. Memory is capped across at most one linear memory.
#[derive(Debug, Clone)]
pub struct WasmLimits {
    pub fuel: u64,
    pub memory_bytes: usize,
    pub timeout_ms: u64,
    pub max_input_bytes: usize,
    pub max_output_bytes: usize,
    pub max_component_bytes: usize,
}
impl Default for WasmLimits {
    fn default() -> Self {
        Self {
            fuel: 10_000_000,
            memory_bytes: 64 * 1024 * 1024,
            timeout_ms: 2_000,
            max_input_bytes: 1024 * 1024,
            max_output_bytes: 1024 * 1024,
            max_component_bytes: 16 * 1024 * 1024,
        }
    }
}
impl WasmLimits {
    fn validate(&self) -> Result<(), Error> {
        if self.fuel == 0
            || self.memory_bytes == 0
            || self.timeout_ms == 0
            || self.timeout_ms > 60_000
            || self.max_input_bytes == 0
            || self.max_output_bytes == 0
            || self.max_component_bytes == 0
        {
            return Err(Error::Config(
                "WASM budgets must be positive and timeout must be <= 60000 ms".into(),
            ));
        }
        Ok(())
    }
}

// Internal producer envelope: a 2048-byte safe reason can expand sixfold in
// JSON. This bound covers that expansion and the closed diagnostic metadata;
// the frozen guest ABI and its public reason bound remain unchanged.
const MAX_DIAGNOSTIC_CARRIER_BYTES: usize = 16 * 1024;
fn encode_diagnostic(diagnostic: &crate::observation::diagnostics::ToolDiagnostic) -> String {
    let encoded = format!("tool_diagnostic:{}", serde_json::json!(diagnostic));
    assert!(
        encoded.len() <= MAX_DIAGNOSTIC_CARRIER_BYTES,
        "closed diagnostic exceeds internal carrier bound"
    );
    encoded
}
fn admission_error(error: crate::policy::PermissionError) -> Error {
    match error {
        crate::policy::PermissionError::Cancelled => Error::Cancelled,
        error => Error::denied_with_private(error.public_reason(), error.to_string()),
    }
}
fn decode_diagnostic(message: &str) -> Option<crate::observation::diagnostics::ToolDiagnostic> {
    if message.len() > MAX_DIAGNOSTIC_CARRIER_BYTES {
        return None;
    }
    serde_json::from_str(message.strip_prefix("tool_diagnostic:")?).ok()
}

struct Compiled {
    engine: Engine,
    component: Component,
    limits: WasmLimits,
    host: Option<Arc<dyn Host>>,
    host_info_json: Result<String, String>,
    evidence_component: String,
    // Unique registered component/Host binding; code hashes are descriptive only.
    admission_identity: String,
}

/// An immutable agent catalog. Cloned tool handles share compiled code and Host;
/// Session managers own epoch instances and direct invocations use fresh stores.
pub struct WasmAgent {
    agents: Vec<AgentInfo>,
    modes: Vec<String>,
    tools: Vec<Arc<dyn Tool>>,
}
impl WasmAgent {
    pub fn load(bytes: &[u8], limits: WasmLimits, host: Arc<dyn Host>) -> Result<Self, Error> {
        load_agent(bytes, limits, host)
    }
    pub fn agents(&self) -> &[AgentInfo] {
        &self.agents
    }
    pub fn modes(&self) -> &[String] {
        &self.modes
    }
    pub fn tools(&self) -> Vec<Arc<dyn Tool>> {
        self.tools.clone()
    }
}

pub struct WasmTool {
    compiled: Arc<Compiled>,
    definition: ToolDefinition,
    media_output_bytes: usize,
}
impl WasmTool {
    pub fn load(
        bytes: &[u8],
        limits: WasmLimits,
        host: Arc<dyn Host>,
    ) -> Result<Vec<Arc<dyn Tool>>, Error> {
        load_component(bytes, limits, host)
    }
}

/// Load the current agent contract with explicitly injected execution capabilities.
pub fn load_component(
    bytes: &[u8],
    limits: WasmLimits,
    host: Arc<dyn Host>,
) -> Result<Vec<Arc<dyn Tool>>, Error> {
    Ok(WasmAgent::load(bytes, limits, host)?.tools)
}
pub fn load_component_with_host(
    bytes: &[u8],
    limits: WasmLimits,
    host: Arc<dyn Host>,
) -> Result<Vec<Arc<dyn Tool>>, Error> {
    Ok(WasmAgent::load(bytes, limits, host)?.tools)
}

struct CachedComponent {
    bytes: Vec<u8>,
    component: Component,
}

fn get_or_compile_component(bytes: &[u8]) -> Result<(Engine, Component), Error> {
    static SHARED_ENGINE: OnceLock<Engine> = OnceLock::new();
    static COMPONENT_CACHE: OnceLock<Mutex<HashMap<u64, CachedComponent>>> = OnceLock::new();

    let engine = SHARED_ENGINE.get_or_init(|| {
        let mut config = Config::new();
        config
            .wasm_component_model(true)
            .consume_fuel(true)
            .epoch_interruption(true);
        Engine::new(&config).expect("valid wasmtime config")
    });

    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    let hash = hasher.finish();

    let cache = COMPONENT_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    // Loading is synchronous worker work. Serialize misses so concurrent catalog
    // and create requests cannot compile the same large component repeatedly.
    let mut guard = cache.lock().unwrap_or_else(|error| error.into_inner());
    if let Some(cached) = guard.get(&hash)
        && cached.bytes.as_slice() == bytes
    {
        return Ok((engine.clone(), cached.component.clone()));
    }
    let component = Component::new(engine, bytes).map_err(|error| {
        crate::observation::wasm::EvidenceBoundary::new(
            &crate::observation::wasm::digest(bytes),
            None,
        )
        .failure(error, crate::observation::wasm::Phase::Compile)
    })?;
    // Active sessions retain their own component Arc after cache eviction.
    if guard.len() >= 8
        && let Some(oldest_key) = guard.keys().next().copied()
    {
        guard.remove(&oldest_key);
    }
    guard.insert(
        hash,
        CachedComponent {
            bytes: bytes.to_vec(),
            component: component.clone(),
        },
    );
    Ok((engine.clone(), component))
}

/// Compilation and frozen discovery are synchronous and capability-free. Callers
/// may place large artifact loading on a worker; no Tokio runtime is needed here.
fn load_agent(bytes: &[u8], limits: WasmLimits, host: Arc<dyn Host>) -> Result<WasmAgent, Error> {
    limits.validate()?;
    crate::budget::check(
        crate::LimitKind::WasmBytes,
        bytes.len(),
        limits.max_component_bytes,
    )
    .map_err(|error| match error {
        Error::Limit(limit) => Error::Limit(limit.with_label("WASM artifact bytes")),
        error => error,
    })?;
    let (engine, component) = get_or_compile_component(bytes)?;
    if !component
        .component_type()
        .exports(&engine)
        .any(|(name, _)| name == "describe")
    {
        return Err(Error::wasm("the current agent describe export is required"));
    }
    if !component
        .component_type()
        .exports(&engine)
        .any(|(name, _)| name == "init")
    {
        return Err(Error::wasm(
            "unsupported Agent ABI: required init export is absent (expected 0.8.0)",
        ));
    }
    let mut capabilities = Vec::new();
    let mut linked_operations = HashSet::new();
    let tool_capabilities = host.tool_capabilities();
    for (name, import) in component.component_type().imports(&engine) {
        let owner = match name {
            "tool-output" | "tool-failure" | "driver-step" | "init-result" | "init-error"
                if matches!(&import.ty, ComponentItem::Type(_)) =>
            {
                None
            }
            "buffer" if matches!(&import.ty, ComponentItem::Resource(_)) => None,
            HOST_INTERFACE
            | "microcrab:session/variables@0.1.0"
            | "microcrab:session/variables@0.1.1"
            | "microcrab:host/types@0.1.0"
            | "microcrab:host/tool-types@0.1.0"
            | "microcrab:host/cache-points@0.1.0"
            | "microcrab:agent/execution@0.8.0" => None,
            "microcrab:host/text-read@0.1.0" => Some("read"),
            "microcrab:host/file-mutation@0.1.0" => Some("file_mutation"),
            "microcrab:host/text-write@0.1.0" => Some("write"),
            "microcrab:host/glob@0.1.0" => Some("glob"),
            "microcrab:host/grep@0.1.0" => Some("grep"),
            "microcrab:host/shell@0.1.0" => Some("shell"),
            "microcrab:host/process@0.1.0" => Some("process"),
            "microcrab:host/filesystem@0.1.0" => Some("filesystem"),
            "microcrab:host/net@0.1.0" => Some("net"),
            _ => {
                return Err(Error::wasm(
                    "unsupported component import; only current explicitly supported agent/Host/Session interfaces are allowed; WASI is disabled",
                ));
            }
        };
        if let (Some(owner), ComponentItem::ComponentInstance(interface)) = (owner, import.ty) {
            for (function, export) in interface.exports(&engine) {
                if !matches!(export.ty, ComponentItem::ComponentFunc(_)) {
                    continue;
                }
                if owner == "file_mutation" {
                    if matches!(function, "preflight" | "mutate")
                        && tool_capabilities.iter().any(|name| name == owner)
                    {
                        linked_operations.insert(format!("{owner}.{function}"));
                    }
                    continue;
                }
                if matches!(owner, "read" | "write" | "glob" | "grep") {
                    if function == owner && tool_capabilities.iter().any(|name| name == owner) {
                        capabilities.push(owner.to_owned());
                    }
                    continue;
                }
                // WIT exposes bounded byte reads while native Process owns streams.
                // Bounded wait-for-state is a Runtime observation over native status.
                let native_function = match (owner, function) {
                    ("process", "read-stdout") => "stdout".to_owned(),
                    ("process", "read-stderr") => "stderr".to_owned(),
                    ("process", "wait-for-state") => "status".to_owned(),
                    _ => function.to_owned(),
                };
                let operation = format!("{owner}.{native_function}");
                if host.supports(&operation)
                    && resources::supported_operation(&operation)
                    && (function != "wait-for-state" || host.supports("process.watch"))
                {
                    linked_operations.insert(format!("{owner}.{function}"));
                    capabilities.push(operation);
                }
            }
        }
    }
    if linked_operations.contains("shell.launch")
        && (linked_operations.contains("shell.read") || linked_operations.contains("shell.drain"))
    {
        capabilities.push("shell".into());
    }
    if linked_operations.contains("process.wait-for-state") {
        capabilities.push("process".into());
    }
    if linked_operations.contains("file_mutation.preflight")
        && linked_operations.contains("file_mutation.mutate")
    {
        capabilities.push("file_mutation".into());
    }
    capabilities.sort_unstable();
    capabilities.dedup();
    let host_info_json = host
        .info()
        .map(|mut info| {
            // Actual import linkage and supported Runtime mappings own availability.
            info.capabilities = capabilities;
            use microcrab_agent_types::{HostFeatures, HostResources};
            let features = host.capabilities();
            info.resources = Some(HostResources {
                platform: match host.platform() {
                    microcrab_host_api::Platform::Linux => "linux",
                    microcrab_host_api::Platform::MacOS => "macos",
                    microcrab_host_api::Platform::Windows => "windows",
                    microcrab_host_api::Platform::Other => "other",
                }
                .into(),
                features: HostFeatures {
                    process_pty: features.process_pty,
                    process_kill_tree: features.process_kill_tree,
                    fs_watch: features.fs_watch,
                    fs_symlink: features.fs_symlink,
                    net_listen: features.net_listen,
                    net_udp: features.net_udp,
                },
            });
            info
        })
        .map_err(|error| format!("Host info: {error}"))
        .and_then(|info| serde_json::to_string(&info).map_err(|error| error.to_string()));
    if let Ok(json) = &host_info_json {
        crate::budget::check(
            crate::LimitKind::WasmBytes,
            json.len(),
            limits.max_input_bytes.min(limits.max_output_bytes),
        )
        .map_err(|error| match error {
            Error::Limit(limit) => Error::Limit(limit.with_label("WASM Host info bytes")),
            error => error,
        })?;
    }
    static NEXT_ADMISSION_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let admission_id = NEXT_ADMISSION_ID
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .map_err(|_| Error::wasm("component admission identities exhausted"))?;
    let compiled = Arc::new(Compiled {
        engine,
        component,
        limits,
        host: Some(host),
        host_info_json,
        evidence_component: crate::observation::wasm::digest(bytes),
        admission_identity: format!("wasm-instance:{admission_id}"),
    });
    let catalog = compiled.discover()?;
    compiled
        .host_info_json
        .as_ref()
        .map_err(|error| Error::Wasm(error.clone().into()))?;
    let agents: Vec<AgentInfo> = serde_json::from_str(&catalog)?;
    validate_agents(&agents)?;
    let definitions: Vec<ToolDefinition> = agents
        .iter()
        .flat_map(|agent| agent.tools.iter().cloned())
        .collect();
    let mut modes = Vec::new();
    for agent in &agents {
        for mode in &agent.modes {
            if !modes.contains(&mode.name) {
                modes.push(mode.name.clone());
            }
        }
    }
    // A valid agent can expose no Host tools (for example Chat Only). External
    // tools retain their own registration and admission; catalog limits still apply.
    if definitions.len() > 128 {
        return Err(Error::wasm("catalog must contain at most 128 tools"));
    }
    let mut names = HashSet::new();
    for definition in &definitions {
        if definition.name.is_empty()
            || definition.name.len() > 64
            || !definition
                .name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            || !names.insert(definition.name.clone())
        {
            return Err(Error::wasm("invalid or duplicate catalog tool name"));
        }
        if definition.parameters.get("type").and_then(Value::as_str) != Some("object") {
            return Err(Error::wasm("tool parameters must declare type object"));
        }
        jsonschema::validator_for(&definition.parameters).map_err(|e| {
            Error::Wasm(
                format!(
                    "invalid tool schema: {}",
                    microcrab_protocol::diagnostic::describe(&e.masked(), 1800)
                )
                .into(),
            )
        })?;
    }
    let tools = definitions
        .into_iter()
        .map(|definition| {
            Arc::new(WasmTool {
                compiled: compiled.clone(),
                media_output_bytes: agents
                    .iter()
                    .find_map(|agent| agent.tool_media_output_bytes.get(&definition.name))
                    .copied()
                    .unwrap_or(0),
                definition,
            }) as Arc<dyn Tool>
        })
        .collect();
    Ok(WasmAgent {
        agents,
        modes,
        tools,
    })
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

fn validate_agents(agents: &[AgentInfo]) -> Result<(), Error> {
    if agents.is_empty() || agents.len() > 32 {
        return Err(Error::wasm("describe must contain 1..=32 agents"));
    }
    let mut names = HashSet::new();
    for agent in agents {
        if !valid_name(&agent.name) || !names.insert(&agent.name) {
            return Err(Error::wasm("invalid or duplicate agent name"));
        }
        if agent.tools.len() > 128 || agent.tool_media_output_bytes.len() > 128 {
            return Err(Error::wasm(
                "agent tool/output declaration count exceeds128",
            ));
        }
        for (tool, bytes) in &agent.tool_media_output_bytes {
            if *bytes == 0
                || *bytes > crate::admission::image::MAX_IMAGE_BYTES
                || !agent
                    .tools
                    .iter()
                    .any(|definition| &definition.name == tool)
            {
                return Err(Error::wasm(
                    "invalid tool media output declaration: expected a known tool and1..=20MiB",
                ));
            }
        }
        let mut modes = HashSet::new();
        if agent.modes.is_empty()
            || agent.modes.len() > 32
            || agent
                .modes
                .iter()
                .any(|mode| !valid_name(&mode.name) || !modes.insert(&mode.name))
        {
            return Err(Error::wasm("invalid or duplicate agent mode"));
        }
    }
    Ok(())
}

/// Import adapters preserve source authority until the frozen ABI boundary.
#[derive(Clone, Debug)]
struct ImportFailure<E> {
    wire: E,
    authoritative: bool,
}
impl<E> ImportFailure<E> {
    fn local(wire: E) -> Self {
        Self {
            wire,
            authoritative: true,
        }
    }
    fn backend(wire: E) -> Self {
        Self {
            wire,
            authoritative: false,
        }
    }
}
// Raw native receipts never confer Runtime admission authority. Runtime-created
// failures must use local() explicitly before reaching the frozen ABI boundary.
impl<E> From<E> for ImportFailure<E> {
    fn from(wire: E) -> Self {
        Self::backend(wire)
    }
}
impl<E> std::ops::Deref for ImportFailure<E> {
    type Target = E;
    fn deref(&self) -> &E {
        &self.wire
    }
}
impl<E> std::ops::DerefMut for ImportFailure<E> {
    fn deref_mut(&mut self) -> &mut E {
        &mut self.wire
    }
}

struct RuntimeState {
    limits: StoreLimits,
    host: Option<Arc<dyn Host>>,
    // Binding exists only during an authorized call and is cleared before idle.
    binding: Option<SessionBinding>,
    tool_context: Option<ToolContext>,
    operation_scope: Option<(String, Value)>,
    // Handles minted by the current authorized process execution only.
    borrowed_processes: HashSet<u64>,
    // Normalized creation metadata follows handles for cleanup/output transfer.
    // It grants no authority; every import validates the invocation proof.
    admitted_process_scope: Option<(Vec<String>, Value)>,
    process_grants: HashMap<u64, (Vec<String>, Value)>,
    // Root confinement is authoritative even when guest code catches its ABI error.
    confinement_diagnostic: Option<crate::observation::diagnostics::ToolDiagnostic>,
    operation_cancelled: bool,
    operation_diagnostic_authoritative: bool,
    operation_diagnostic: Option<(crate::observation::diagnostics::ToolDiagnostic, String)>,
    known_write_committed: bool,
    host_info_json: Result<String, String>,
    runtime: Option<tokio::runtime::Handle>,
    cancellation: CancellationToken,
    // Only drive may finish typed cancellation and canonical post-return. Host
    // imports retain the original token; epoch invalidation and CPU bounds stay hard.
    cooperative_driver_cancellation: bool,
    epoch_cancellation: CancellationToken,
    deadline: Instant,
    max_input_bytes: usize,
    max_output_bytes: usize,
    max_media_output_bytes: usize,
    // Shared invocation bounds permit pagination and conditional-write edits.
    // Discovery and instantiation have no operation context or allowance.
    outputs: HashMap<u32, output::Entry>,
    file_reads: resources::files::Reads,
    next_output: u32,
    operation_calls_remaining: u8,
    input_bytes_remaining: usize,
    output_bytes_remaining: usize,
    // Ordinary controlled writes receive one before/after pair; whole-call
    // mutation preflight reserves up to the collector's 128 bounded points.
    cache_points_remaining: u8,
    patch_plan: Option<Vec<Value>>,
    patch_receipts: Vec<Value>,
}

impl RuntimeState {
    fn has_mutation_outcome(&self) -> bool {
        self.patch_receipts.iter().any(|receipt| {
            matches!(
                receipt["status"].as_str(),
                Some("committed" | "uncertain" | "failed")
            )
        })
    }
    fn confinement_failure(&self) -> Option<Error> {
        let mut diagnostic = self.confinement_diagnostic.clone()?;
        if self.known_write_committed {
            diagnostic.effects = crate::observation::diagnostics::EffectKnowledge::Committed;
        }
        Some(Error::Diagnostic(diagnostic))
    }
}

struct Instance {
    init_ready: bool,
    store: Store<RuntimeState>,
    bindings: Runtime,
    poisoned: bool,
    failure_evidence: Option<u64>,
}

fn report_untransferred_process_cleanup_failure(
    error: &Error,
    redaction: &super::execution::InvocationRedaction,
) {
    let reason = redaction.redact_text(&error.diagnostic_reason());
    tracing::warn!(reason = %reason, "Untransferred WASM process cleanup failed");
}

/// A cached store must never retain the Session (or its command endpoint) while
/// idle. This guard also runs if a Host implementation unwinds on the worker.
struct InvocationScope<'a>(&'a mut Instance);
impl Drop for InvocationScope<'_> {
    fn drop(&mut self) {
        let state = self.0.store.data_mut();
        state.outputs.clear();
        state.file_reads.detach();
        state.max_media_output_bytes = 0;
        if let (Some(runtime), Some(context)) = (&state.runtime, &state.tool_context) {
            let processes = context.processes.clone();
            let handles: Vec<_> = state
                .process_grants
                .keys()
                .filter(|id| !state.borrowed_processes.contains(id))
                .copied()
                .collect();
            if !handles.is_empty() {
                let redaction = context.redaction_handle();
                runtime.spawn(async move {
                    for id in handles {
                        let handle = microcrab_host_api::process::ProcessHandle(
                            microcrab_host_api::HandleId(id),
                        );
                        if processes.owns_process(handle)
                            && let Err(error) = processes
                                .release_process(handle, CancellationToken::new())
                                .await
                        {
                            report_untransferred_process_cleanup_failure(&error, &redaction);
                        }
                    }
                });
            }
        }
        state.binding = None;
        if let Some(context) = &state.tool_context {
            context.release_cache_reservation();
        }
        state.tool_context = None;
        state.operation_scope = None;
        state.process_grants.clear();
        state.admitted_process_scope = None;
        state.borrowed_processes.clear();
        state.confinement_diagnostic = None;
        state.operation_cancelled = false;
        state.operation_diagnostic = None;
        state.operation_diagnostic_authoritative = false;
        state.known_write_committed = false;
        state.runtime = None;
        state.operation_calls_remaining = 0;
        state.input_bytes_remaining = 0;
        state.output_bytes_remaining = 0;
        state.cache_points_remaining = 0;
        state.patch_plan = None;
        state.patch_receipts.clear();
        state.cancellation = CancellationToken::new();
        state.cooperative_driver_cancellation = false;
        state.epoch_cancellation = CancellationToken::new();
    }
}

impl Compiled {
    fn discover(&self) -> Result<String, Error> {
        self.run(
            &mut None,
            CancellationToken::new(),
            CancellationToken::new(),
            None,
            None,
            None,
            None,
            |bindings, store, boundary| {
                let result = bindings
                    .call_describe(store, self.host_info_json.as_deref().unwrap_or_default())
                    .map_err(|error| {
                        boundary.failure(error, crate::observation::wasm::Phase::Describe)
                    })?;
                let bytes = match &result {
                    Ok(value) | Err(value) => value.len(),
                };
                crate::budget::check(
                    crate::LimitKind::ToolOutput,
                    bytes,
                    self.limits.max_output_bytes,
                )
                .map_err(|error| match error {
                    Error::Limit(limit) => Error::Limit(limit.with_label("WASM response bytes")),
                    error => error,
                })?;
                result.map_err(|error| Error::Wasm(error.into()))
            },
        )
    }

    fn instantiate(
        &self,
        cancellation: CancellationToken,
        epoch_cancellation: CancellationToken,
        deadline: Instant,
        boundary: &crate::observation::wasm::EvidenceBoundary,
    ) -> Result<Instance, Error> {
        let mut store = Store::new(
            &self.engine,
            RuntimeState {
                limits: StoreLimitsBuilder::new()
                    .memory_size(self.limits.memory_bytes)
                    .memories(1)
                    .table_elements(4096)
                    .tables(4)
                    .instances(16)
                    .build(),
                host: self.host.clone(),
                outputs: HashMap::new(),
                file_reads: Default::default(),
                next_output: 1,
                binding: None,
                tool_context: None,
                operation_scope: None,
                borrowed_processes: HashSet::new(),
                admitted_process_scope: None,
                process_grants: HashMap::new(),
                confinement_diagnostic: None,
                operation_cancelled: false,
                operation_diagnostic: None,
                operation_diagnostic_authoritative: false,
                known_write_committed: false,
                host_info_json: self.host_info_json.clone(),
                runtime: None,
                cancellation,
                cooperative_driver_cancellation: false,
                epoch_cancellation,
                deadline,
                max_input_bytes: self.limits.max_input_bytes,
                max_output_bytes: self.limits.max_output_bytes,
                max_media_output_bytes: 0,
                operation_calls_remaining: 0,
                input_bytes_remaining: 0,
                output_bytes_remaining: 0,
                cache_points_remaining: 0,
                patch_plan: None,
                patch_receipts: Vec::new(),
            },
        );
        store.limiter(|state| &mut state.limits);
        self.reset_budgets(&mut store, boundary)?;
        // The callback outlives each invocation. Read the current call's state,
        // never capture the first call's deadline or cancellation token.
        store.epoch_deadline_callback(|context| {
            let state = context.data();
            if state.epoch_cancellation.is_cancelled()
                || (state.cancellation.is_cancelled() && !state.cooperative_driver_cancellation)
            {
                Err(crate::observation::wasm::Stop::Cancelled.into())
            } else if Instant::now() >= state.deadline {
                Err(crate::observation::wasm::Stop::Deadline.into())
            } else {
                Ok(UpdateDeadline::Continue(1))
            }
        });
        let mut linker = Linker::new(&self.engine);
        if self.host.is_some() {
            linker
                .instance("microcrab:host/types@0.1.0")
                .map_err(|error| {
                    boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                })?;
            linker
                .instance("microcrab:host/tool-types@0.1.0")
                .map_err(|error| {
                    boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                })?;
            linker
                .instance(HOST_INTERFACE)
                .map_err(|error| {
                    boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                })?
                .func_wrap("host-info", host_info_import)
                .map_err(|error| {
                    boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                })?;
            let mut variables = linker
                .instance("microcrab:session/variables@0.1.0")
                .map_err(|error| {
                    boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                })?;
            variables
                .func_wrap("variable-get", variable_get_import)
                .map_err(|error| {
                    boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                })?;
            variables
                .func_wrap("variable-change", variable_change_import)
                .map_err(|error| {
                    boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                })?;
            {
                let mut scoped = linker
                    .instance("microcrab:session/variables@0.1.1")
                    .map_err(|error| {
                        boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                    })?;
                scoped
                    .func_wrap("variable-get", variable_get_import)
                    .map_err(|error| {
                        boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                    })?;
                scoped
                    .func_wrap("variable-get-all", variable_get_all_import)
                    .map_err(|error| {
                        boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                    })?;
                scoped
                    .func_wrap("variable-metadata", variable_metadata_import)
                    .map_err(|error| {
                        boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                    })?;
                scoped
                    .func_wrap("variable-change", scoped_variable_change_import)
                    .map_err(|error| {
                        boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                    })?;
            }
            linker
                .instance("microcrab:host/text-read@0.1.0")
                .map_err(|error| {
                    boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                })?
                .func_wrap("read", operations::read)
                .map_err(|error| {
                    boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                })?;
            linker
                .instance("microcrab:host/cache-points@0.1.0")
                .map_err(|error| {
                    boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                })?
                .func_wrap("capture", operations::capture)
                .map_err(|error| {
                    boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                })?;
            linker
                .instance("microcrab:host/text-write@0.1.0")
                .map_err(|error| {
                    boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                })?
                .func_wrap("write", operations::write)
                .map_err(|error| {
                    boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                })?;
            linker
                .instance("microcrab:host/glob@0.1.0")
                .map_err(|error| {
                    boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                })?
                .func_wrap("glob", operations::glob)
                .map_err(|error| {
                    boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                })?;
            linker
                .instance("microcrab:host/grep@0.1.0")
                .map_err(|error| {
                    boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                })?
                .func_wrap("grep", operations::grep)
                .map_err(|error| {
                    boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                })?;
            {
                let mut interface = linker
                    .instance("microcrab:host/file-mutation@0.1.0")
                    .map_err(|error| {
                        boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                    })?;
                interface
                    .func_wrap("preflight", operations::mutations::preflight)
                    .map_err(|error| {
                        boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                    })?;
                interface
                    .func_wrap("mutate", operations::mutations::mutate)
                    .map_err(|error| {
                        boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                    })?;
            }
            // Generated linker helpers install common type identities as well as
            // resource functions; valid unsupported operations return typed errors.
            resources::add_to_linker(&mut linker).map_err(|error| {
                boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
            })?;
            output::add_to_linker(&mut linker).map_err(|error| {
                boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
            })?;
        }
        let bindings =
            Runtime::instantiate(&mut store, &self.component, &linker).map_err(|error| {
                boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
            })?;
        Ok(Instance {
            init_ready: false,
            store,
            bindings,
            poisoned: false,
            failure_evidence: None,
        })
    }

    fn reset_budgets(
        &self,
        store: &mut Store<RuntimeState>,
        boundary: &crate::observation::wasm::EvidenceBoundary,
    ) -> Result<(), Error> {
        store
            .set_fuel(self.limits.fuel)
            .map_err(|error| boundary.failure(error, crate::observation::wasm::Phase::Budget))?;
        store.set_hostcall_fuel(self.limits.max_output_bytes);
        store.set_epoch_deadline(1);
        Ok(())
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Guest calls carry distinct execution and epoch cancellation plus independently optional Session capabilities"
    )]
    fn run<T>(
        &self,
        cached: &mut Option<Instance>,
        cancellation: CancellationToken,
        epoch_cancellation: CancellationToken,
        runtime: Option<tokio::runtime::Handle>,
        binding: Option<SessionBinding>,
        tool_context: Option<ToolContext>,
        operation_scope: Option<(String, Value)>,
        call: impl FnOnce(
            &Runtime,
            &mut Store<RuntimeState>,
            &crate::observation::wasm::EvidenceBoundary,
        ) -> Result<T, Error>,
    ) -> Result<T, Error> {
        if cancellation.is_cancelled() || epoch_cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if let Some(context) = &tool_context {
            context
                .ensure_admitted(Some(&self.admission_identity))
                .map_err(admission_error)?;
        }
        let boundary = crate::observation::wasm::EvidenceBoundary::new(
            &self.evidence_component,
            tool_context.as_ref(),
        );
        let deadline = Instant::now() + Duration::from_millis(self.limits.timeout_ms);
        // No ticker is retained while a store is idle.
        let _ticker = EpochTicker::start(self.engine.clone())?;
        let fresh = cached.is_none();
        if fresh {
            *cached = Some(self.instantiate(
                cancellation.clone(),
                epoch_cancellation.clone(),
                deadline,
                &boundary,
            )?);
        }
        let instance = cached
            .as_mut()
            .expect("successful initialization installs an instance");
        let scope = InvocationScope(instance);
        let state = scope.0.store.data_mut();
        // Failed read-handle cleanup remains owned by the retained instance. No
        // new guest work may replace that owner until cleanup is acknowledged.
        resources::files::cleanup(state)?;
        state.cancellation = cancellation.clone();
        state.cooperative_driver_cancellation = false;
        state.epoch_cancellation = epoch_cancellation.clone();
        state.deadline = deadline;
        // A fresh store was budgeted before initialization. Do not grant its
        // first invocation a second allowance; later calls receive a fresh one.
        if !fresh && let Err(error) = self.reset_budgets(&mut scope.0.store, &boundary) {
            scope.0.poisoned = true;
            scope.0.failure_evidence = crate::observation::wasm::failure_id(&error);
            return Err(error);
        }
        // Component instantiation stays capability-free; explicit init receives its own admitted scope.
        let state = scope.0.store.data_mut();
        state.runtime = runtime;
        state.binding = binding;
        state.tool_context = tool_context;
        state.operation_scope = operation_scope;
        state.process_grants.clear();
        state.admitted_process_scope = None;
        state.borrowed_processes.clear();
        state.confinement_diagnostic = None;
        state.operation_cancelled = false;
        state.operation_diagnostic = None;
        state.operation_diagnostic_authoritative = false;
        state.known_write_committed = false;
        state.file_reads.reset();
        state.operation_calls_remaining = if state.tool_context.is_some() { 240 } else { 0 };
        state.input_bytes_remaining = self.limits.max_input_bytes;
        state.output_bytes_remaining = self.limits.max_output_bytes;
        state.cache_points_remaining = if state.tool_context.is_some() { 2 } else { 0 };
        state.patch_plan = None;
        state.patch_receipts.clear();
        scope
            .0
            .store
            .set_hostcall_fuel(self.limits.max_output_bytes);
        // Wasmtime 47 TypedFunc::call (used by bindgen) automatically runs
        // canonical post-return before returning the owned result to the host.
        let result = call(&scope.0.bindings, &mut scope.0.store, &boundary);
        // Cleanup is independent of caller cancellation and must settle before ready.
        let files_cleanup = resources::files::cleanup(scope.0.store.data_mut());
        let processes_cleanup = lifecycle::cleanup_init_processes(scope.0.store.data_mut());
        let cleanup = files_cleanup.and(processes_cleanup);
        let cleanup_succeeded = cleanup.is_ok();
        let result = match (result, cleanup) {
            (_, Err(error)) | (Err(error), Ok(())) => Err(error),
            (Ok(value), Ok(())) => Ok(value),
        };
        scope.0.failure_evidence = result
            .as_ref()
            .err()
            .and_then(crate::observation::wasm::failure_id);
        scope.0.poisoned =
            scope.0.failure_evidence.is_some() || matches!(&result, Err(Error::Wasm(_)));
        if epoch_cancellation.is_cancelled()
            || (cancellation.is_cancelled() && !scope.0.store.data().has_mutation_outcome())
        {
            return Err(Error::Cancelled);
        }
        let state = scope.0.store.data();
        if state.operation_scope.is_some()
            && state.operation_cancelled
            && !state.has_mutation_outcome()
        {
            return Err(Error::Cancelled);
        }

        if Instant::now() >= state.deadline {
            return Err(crate::budget::reject(
                crate::LimitKind::OperationDeadline,
                "WASM wall deadline",
            ));
        }
        if cleanup_succeeded && let Some(error) = state.confinement_failure() {
            return Err(error);
        }
        // Cleanup failures remain primary even when a preceding guest import
        // supplied a precise path rejection; they describe unsettled resources.
        if cleanup_succeeded
            && !(state.has_mutation_outcome() && result.is_ok())
            && !scope.0.poisoned
            && let Some((diagnostic, _)) = &state.operation_diagnostic
            // A concrete cause is diagnostic evidence, not authority to replace
            // a guest-handled Final result. Path confinement remains authoritative.
            && (result.is_err() || state.operation_diagnostic_authoritative
                || diagnostic.code
                    == crate::observation::diagnostics::DiagnosticCode::PathOutsideRoot)
            && (diagnostic.reason.is_some()
                || diagnostic.code
                    == crate::observation::diagnostics::DiagnosticCode::PathOutsideRoot)
        {
            let mut diagnostic = diagnostic.clone();
            // A rejected import proves only that operation did not start. Earlier
            // guest imports may have committed writes or other external effects.
            if state.known_write_committed {
                diagnostic.effects = crate::observation::diagnostics::EffectKnowledge::Committed;
            }
            return Err(Error::Diagnostic(diagnostic));
        }
        result
    }
}

/// The ticker stops and joins on every return, including traps and unwinding.
struct EpochTicker {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl EpochTicker {
    fn start(engine: Engine) -> Result<Self, Error> {
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let thread = std::thread::Builder::new()
            .name("microcrab-wasm-epoch".into())
            .spawn(move || {
                while !stopped.load(Ordering::Acquire) {
                    std::thread::park_timeout(Duration::from_millis(5));
                    engine.increment_epoch();
                }
            })?;
        Ok(Self {
            stop,
            thread: Some(thread),
        })
    }
}
impl Drop for EpochTicker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread.thread().unpark();
            let _ = thread.join();
        }
    }
}

#[async_trait]
impl Tool for WasmTool {
    fn media_output_bytes(&self) -> usize {
        self.media_output_bytes
    }
    fn definition(&self) -> ToolDefinition {
        self.definition.clone()
    }
    fn component_identity(&self) -> Option<&str> {
        Some(&self.compiled.admission_identity)
    }
    fn wasm_lifecycle(&self) -> Option<AgentLifecycle> {
        Some(AgentLifecycle {
            compiled: self.compiled.clone(),
        })
    }
    async fn execute(
        &self,
        arguments: Value,
        invocation: ToolContext,
    ) -> Result<NativeToolOutput, Error> {
        if invocation.cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if !invocation.subject.is_tool() || invocation.tool != self.definition.name {
            return Err(Error::denied(
                "WASM export differs from the admitted invocation",
            ));
        }
        invocation
            .ensure_admitted(Some(&self.compiled.admission_identity))
            .map_err(admission_error)?;
        invocation
            .ensure_admitted_arguments(&arguments)
            .map_err(admission_error)?;
        let state = invocation.state.clone();
        if !state.is_attached() {
            return self
                .execute_bound(arguments, invocation, None, state.wasm_instances())
                .await;
        }
        let instances = state.wasm_instances();
        self.execute_bound(
            arguments,
            invocation,
            Some(SessionBinding::from_state(state)),
            instances,
        )
        .await
    }
}

impl WasmTool {
    async fn execute_bound(
        &self,
        arguments: Value,
        invocation: ToolContext,
        binding: Option<SessionBinding>,
        instances: Arc<EpochInstances>,
    ) -> Result<NativeToolOutput, Error> {
        if invocation.cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if !arguments.is_object() {
            return Err(Error::Tool("WASM arguments must be an object".into()));
        }
        let effective_arguments = arguments.clone();
        let arguments = serde_json::to_string(&arguments)?;
        crate::budget::check(
            crate::LimitKind::ToolArguments,
            arguments.len(),
            self.compiled.limits.max_input_bytes,
        )
        .map_err(|error| match error {
            Error::Limit(limit) => Error::Limit(limit.with_label("WASM argument bytes")),
            error => error,
        })?;
        let compiled = self.compiled.clone();
        let name = self.definition.name.clone();
        let media_output_bytes = self.media_output_bytes;
        let cancellation = invocation.cancellation.child_token();
        let guard = CancelOnDrop(cancellation.clone());
        let runtime = tokio::runtime::Handle::current();
        let slot = instances.slot(&compiled)?;
        let lease = slot.lease(&cancellation).await?;
        if !lease.ready() {
            return Err(Error::Lifecycle("Agent initialization has not settled for this retained instance; initialize it with an explicit AgentInit scope or use Session".into()));
        }
        invocation
            .begin_dispatch(Some(&self.compiled.admission_identity))
            .map_err(admission_error)?;
        let driver_scope = driver::Scope::capture(&invocation);
        let driver_slot = slot.clone();
        let driver_compiled = compiled.clone();
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        let result = tokio::task::spawn_blocking(move || {
            tracing::dispatcher::with_default(&dispatch, || {
                let epoch_cancellation = slot.cancellation.clone();
                let run = |cached: &mut Option<Instance>| {
                    compiled.run(
                        cached,
                        cancellation,
                        epoch_cancellation,
                        Some(runtime),
                        binding,
                        Some(invocation),
                        Some((name.clone(), effective_arguments)),
                        |bindings, store, boundary| {
                            store.data_mut().max_media_output_bytes = media_output_bytes;
                            let media_limit = store
                                .data()
                                .tool_context
                                .as_ref()
                                .map_or(0, |context| {
                                    context.state.snapshot().limits.max_tool_image_bytes
                                })
                                .min(media_output_bytes);
                            store.set_hostcall_fuel(
                                compiled.limits.max_output_bytes.saturating_add(media_limit),
                            );
                            let value = bindings
                                .call_invoke(
                                    &mut *store,
                                    compiled.host_info_json.as_deref().unwrap_or_default(),
                                    &name,
                                    &arguments,
                                )
                                .map_err(|error| {
                                    boundary.failure(error, crate::observation::wasm::Phase::Invoke)
                                })?;
                            output::lift(store.data_mut(), value)
                        },
                    )
                };
                slot.run(lease, run)
            })
        })
        .await
        .map_err(|_| Error::wasm("WASM worker failed"))?;
        drop(guard);
        let prepared = result?;
        if let Some(source) = prepared.driver {
            driver::start(driver_compiled, driver_slot, driver_scope, source);
        }
        Ok(prepared.outcome)
    }
}
