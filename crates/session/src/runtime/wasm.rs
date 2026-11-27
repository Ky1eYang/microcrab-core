//! Agent components with epoch-owned stores, per-call budgets, and narrow imports.
//! Discovery can read frozen HostInfo but cannot execute Host capabilities.
//! Tool execution follows the Session permission gate; no WASI linker is installed.
//!
//! The Runtime and Host provide general interfaces and enforce the authority,
//! resource and lifecycle boundaries they own. Never hard-code behavior for a
//! particular Agent component, its tool or mode names, its argument vocabulary,
//! or its choice of an otherwise valid output. Agent behavior belongs in the
//! Guest. If an existing interface cannot express a use case, extend a reusable
//! capability contract instead of adding an Agent-specific exception.

mod authority;
mod capability;
mod references;
pub use capability::ComponentCapability;
#[cfg(test)]
mod cleanup_tests;
mod communication;
mod driver;
mod epoch;
mod file_versions;
mod guest_observation;
pub(crate) use file_versions::{FileVersionOwner, bind_arguments as bind_file_version_arguments};
mod lifecycle;
#[cfg(test)]
mod media_tests;
pub use lifecycle::AgentLifecycle;
mod operations;
mod output;
mod resources;
mod script;
#[cfg(test)]
pub(crate) mod test_admission;
mod web_transport;
pub(crate) use epoch::EpochInstances;

use super::execution::ToolOutput as NativeToolOutput;
use super::tool::{Tool, ToolContext};
use crate::Error;
use crate::observation::wasm::{EvidenceBoundary, FailureEvidence, Phase};
use crate::state::wasm::SessionBinding;
use crate::utils::cancel::CancelOnDrop;
use async_trait::async_trait;
use communication::host_info_import;
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
    file_versions: bool,
    session_script: bool,
    script_profile: script::ScriptProfile,
    observes: bool,
    evidence_component: String,
    // Unique registered component/Host binding; code hashes are descriptive only.
    admission_identity: String,
    glob_paths_linked: bool,
    required_capabilities: Arc<[ComponentCapability]>,
    web_transport_binding: Option<web_transport::ProjectScope>,
    scoped_web_transport: bool,
}

/// An immutable agent catalog. Cloned tool handles share compiled code and Host;
/// Session managers own epoch instances and direct invocations use fresh stores.
pub struct WasmAgent {
    agents: Vec<AgentInfo>,
    modes: Vec<String>,
    tools: Vec<Arc<dyn Tool>>,
    required_capabilities: Arc<[ComponentCapability]>,
}
impl WasmAgent {
    /// Load administrator-provisioned bytes and discover the catalog without
    /// executing Host capabilities. Callers may run this synchronous work on a
    /// worker; no Tokio runtime is needed.
    pub fn load(bytes: &[u8], limits: WasmLimits, host: Arc<dyn Host>) -> Result<Self, Error> {
        load_agent(bytes, limits, host)
    }
    /// Requirements come from exact versioned WIT imports, not descriptor
    /// metadata or the bound Host's ability to satisfy them. Check each
    /// requirement against the authorized Project/Host before offering or
    /// selecting this component.
    pub fn required_capabilities(&self) -> &[ComponentCapability] {
        &self.required_capabilities
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

struct WasmTool {
    compiled: Arc<Compiled>,
    definition: ToolDefinition,
    media_output_bytes: usize,
    bindings: authority::ToolBindings,
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
    let evidence_component = crate::observation::wasm::digest(bytes);
    let (engine, component) = get_or_compile_component(bytes)?;
    let component_type = component.component_type();
    if !component_type
        .exports(&engine)
        .any(|(name, _)| name == "describe")
    {
        return Err(Error::wasm("the current agent describe export is required"));
    }
    if !component_type
        .exports(&engine)
        .any(|(name, _)| name == "init")
    {
        return Err(Error::wasm(
            "unsupported Agent ABI: required init export is absent (expected 0.9.0)",
        ));
    }
    let required_capabilities: Arc<[ComponentCapability]> =
        ComponentCapability::discover(&component_type, &engine)?.into();
    let scoped_web_transport = component_type
        .imports(&engine)
        .any(|(name, _)| name == web_transport::SCOPED_INTERFACE);
    let mut capabilities = Vec::new();
    let mut linked_operations = HashSet::new();
    let mut current_execution = false;
    let mut file_versions = false;
    let mut private_shell_linked = false;
    let mut glob_paths_linked = false;
    let tool_capabilities = host.tool_capabilities();
    for (name, import) in component_type.imports(&engine) {
        if name == web_transport::SCOPED_INTERFACE || ComponentCapability::for_import(name).is_some() {
            continue;
        }
        if name == microcrab_agent_wit::EXECUTION_INTERFACE {
            current_execution = true;
            if let ComponentItem::ComponentInstance(interface) = &import.ty {
                private_shell_linked = interface.exports(&engine).any(|(name, export)| {
                    name == "launch-private-shell"
                        && matches!(export.ty, ComponentItem::ComponentFunc(_))
                });
            }
        } else if name.starts_with("microcrab:agent/") && name != HOST_INTERFACE {
            return Err(Error::wasm(
                "unsupported Agent ABI: new execution requires 0.9.0; historical Component pins must be migrated explicitly",
            ));
        }
        let owner = match name {
            file_versions::INTERFACE => {
                file_versions = true;
                if let ComponentItem::ComponentInstance(interface) = &import.ty {
                    for (function, export) in interface.exports(&engine) {
                        if matches!(export.ty, ComponentItem::ComponentFunc(_)) {
                            linked_operations.insert(format!("file_versions.{function}"));
                        }
                    }
                }
                None
            }
            "tool-output" | "tool-failure" | "driver-step" | "init-result" | "init-error"
                if matches!(&import.ty, ComponentItem::Type(_)) =>
            {
                None
            }
            "buffer" if matches!(&import.ty, ComponentItem::Resource(_)) => None,
            script::INTERFACE if script::valid_import(&import.ty, &engine) => None,
            guest_observation::INTERFACE | guest_observation::BROWSER_INTERFACE
                if guest_observation::valid_import(name, &import.ty, &engine) => None,
            HOST_INTERFACE
            | "microcrab:host/types@0.1.0"
            | "microcrab:host/tool-types@0.1.0"
            | "microcrab:host/cache-points@0.1.0"
            | microcrab_agent_wit::EXECUTION_INTERFACE => None,
            "microcrab:host/text-read@0.1.0" => Some("read"),
            "microcrab:host/file-mutation@0.1.0" => Some("file_mutation"),
            "microcrab:host/text-write@0.1.0" => Some("write"),
            "microcrab:host/glob@0.1.0" | "microcrab:host/glob-ignore@0.1.0" => Some("glob"),
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
                if name == "microcrab:host/glob-ignore@0.1.0"
                    && function == "glob-with-ignored-paths"
                {
                    glob_paths_linked = true;
                }
                if matches!(owner, "read" | "write" | "glob" | "grep") {
                    if (function == owner
                        || (owner == "glob"
                            && matches!(function, "glob-from-state" | "glob-with-ignored-paths")))
                        && tool_capabilities.iter().any(|name| name == owner)
                    {
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
    if !current_execution {
        return Err(Error::wasm(
            "unsupported Agent ABI: missing 0.9.0 execution interface; historical Component pins must be migrated explicitly",
        ));
    }
    if private_shell_linked && resources::private_shell_supported(host.as_ref()) {
        capabilities.push("shell.launch-private".into());
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
    if file_versions && host.file_version_authority().is_some() {
        capabilities.push(file_versions::CAPABILITY.into());
        for (function, capability) in [
            ("read", "read"),
            ("write", "write"),
            ("mutate", "file_mutation"),
        ] {
            if linked_operations.contains(&format!("file_versions.{function}"))
                && tool_capabilities.iter().any(|name| name == capability)
                && (function != "mutate" || linked_operations.contains("file_versions.preflight"))
            {
                capabilities.push(capability.into());
            }
        }
    }
    let web_transport_binding = required_capabilities
        .contains(&ComponentCapability::WebTransport)
        .then(|| web_transport::host_binding(host.as_ref()))
        .flatten();
    if web_transport_binding.is_some() {
        capabilities.push(ComponentCapability::WebTransport.name().into());
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
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .map_err(|_| Error::wasm("component admission identities exhausted"))?;
    let session_script = component_type
        .imports(&engine)
        .any(|(name, _)| name == script::INTERFACE);
    let script_profile = if required_capabilities.contains(&ComponentCapability::WebTransport) {
        script::ScriptProfile::WebTransport
    } else {
        script::ScriptProfile::Standard
    };
    let observes = component_type.imports(&engine).any(|(name, _)| {
        matches!(name, guest_observation::INTERFACE | guest_observation::BROWSER_INTERFACE)
    });
    if observes {
        guest_observation::prepare_sink();
    }
    let compiled = Arc::new(Compiled {
        engine,
        component,
        limits,
        host: Some(host),
        host_info_json,
        file_versions,
        session_script,
        script_profile,
        observes,
        evidence_component,
        admission_identity: format!("wasm-instance:{admission_id}"),
        glob_paths_linked,
        required_capabilities: required_capabilities.clone(),
        web_transport_binding,
        scoped_web_transport,
    });
    let catalog = compiled.discover()?;
    compiled
        .host_info_json
        .as_ref()
        .map_err(|error| Error::Wasm(error.clone().into()))?;
    let raw_agents: Vec<Value> = serde_json::from_str(&catalog)?;
    let mut agents = Vec::with_capacity(raw_agents.len());
    let mut tool_bindings = HashMap::new();
    for mut raw in raw_agents {
        let declarations = raw.as_object_mut().and_then(|object| object.remove("tool_bindings"));
        let agent: AgentInfo = serde_json::from_value(raw)?;
        let declarations: HashMap<String, authority::ToolBindings> = declarations.map(serde_json::from_value).transpose()?.unwrap_or_default();
        for (name, bindings) in declarations {
            if !agent.tools.iter().any(|tool| tool.name == name) {
                return Err(Error::wasm("capability bindings require a tool declared by the same agent"));
            }
            if matches!(bindings.web, Some(authority::WebBinding::Open { handoff: true, .. }))
                && !compiled.scoped_web_transport
            {
                return Err(Error::wasm("connection handoff requires scoped Web Transport"));
            }
            if bindings.web.is_some() && !compiled.has_capability(ComponentCapability::WebTransport) {
                return Err(Error::wasm("tool capability bindings require the matching import"));
            }
            bindings.validate()?;
            if tool_bindings.insert(name, bindings).is_some() {
                return Err(Error::wasm("duplicate tool capability bindings"));
            }
        }
        agents.push(agent);
    }
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
                bindings: tool_bindings.remove(&definition.name).unwrap_or_default(),
                definition,
            }) as Arc<dyn Tool>
        })
        .collect();
    Ok(WasmAgent {
        agents,
        modes,
        tools,
        required_capabilities,
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
    // Present only during the admitted invoke export, never init, describe or drive.
    web: Option<web_transport::Invocation>,
    references: Option<references::Ingress>,
    script_profile: script::ScriptProfile,
    guest_observation: Option<(Arc<guest_observation::Scope>, Phase)>,
    // Handles minted by the current authorized process execution only.
    borrowed_processes: HashSet<u64>,
    // Normalized creation metadata follows handles for cleanup/output transfer.
    // It grants no authority; every import validates the invocation proof.
    admitted_process_scope: Option<(Vec<String>, Value)>,
    process_launch_uncertain: bool,
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
    patch_version_refs: Vec<Option<(String, String, String)>>,
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
    failure_evidence: Option<FailureEvidence>,
    recovery_cleanup: Option<RecoveryCleanup>,
}

/// Exact scope abandoned by an established trap. The Execution Registry and
/// Process Registry retain cleanup authority if this bounded join fails.
struct RecoveryCleanup {
    runtime: tokio::runtime::Handle,
    execution: driver::Cleanup,
    processes: Arc<dyn super::execution::process::ProcessControl>,
    handles: Vec<u64>,
    process_launch_uncertain: bool,
    redaction: super::execution::InvocationRedaction,
    timeout: Duration,
}

impl RecoveryCleanup {
    fn capture(state: &RuntimeState) -> Option<Self> {
        let context = state.tool_context.as_ref()?;
        Some(Self {
            runtime: state.runtime.clone()?,
            execution: driver::Cleanup::capture(context)?,
            processes: context.processes.clone(),
            handles: state.process_grants.keys().filter(|id| !state.borrowed_processes.contains(id)).copied().collect(),
            process_launch_uncertain: state.process_launch_uncertain,
            redaction: context.redaction_handle(),
            timeout: Duration::from_millis(context.state.snapshot().limits.lifecycle_timeout_ms),
        })
    }

    async fn acknowledge(&self, web: &web_transport::SlotOwner, drivers: &[Arc<driver::Cleanup>], epoch_ended: &CancellationToken) -> bool {
        tokio::select! {
            biased;
            _ = epoch_ended.cancelled() => false,
            result = tokio::time::timeout(self.timeout, async {
                let processes = async {
                    let mut acknowledged = true;
                    for &id in &self.handles {
                        let handle = microcrab_host_api::process::ProcessHandle(microcrab_host_api::HandleId(id));
                        if self.processes.owns_process(handle)
                            && let Err(error) = self.processes.release_process(handle, CancellationToken::new()).await
                            && self.processes.owns_process(handle)
                        {
                            report_untransferred_process_cleanup_failure(&error, &self.redaction);
                            acknowledged = false;
                        }
                    }
                    acknowledged
                };
                let executions = async {
                    futures_util::future::join_all(std::iter::once(&self.execution).chain(drivers.iter().map(Arc::as_ref)).map(driver::Cleanup::acknowledge))
                        .await.into_iter().all(|result| result.is_ok())
                };
                let (web, processes, executions) = tokio::join!(web.acknowledge_retirement(), processes, executions);
                web.is_ok() && processes && executions && !self.process_launch_uncertain
            }) => result.unwrap_or(false),
        }
    }
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
        if self.0.recovery_cleanup.is_none()
            && let (Some(runtime), Some(context)) = (&state.runtime, &state.tool_context)
        {
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
        state.web = None;
        state.references = None;
        state.guest_observation = None;
        state.process_grants.clear();
        state.admitted_process_scope = None;
        state.process_launch_uncertain = false;
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
        state.patch_version_refs.clear();
        state.cancellation = CancellationToken::new();
        state.cooperative_driver_cancellation = false;
        state.epoch_cancellation = CancellationToken::new();
    }
}

impl Compiled {
    fn execution_failure(
        &self,
        boundary: &EvidenceBoundary,
        error: wasmtime::Error,
        phase: Phase,
    ) -> Error {
        boundary.execution_failure(error, phase, self.limits.fuel, self.limits.timeout_ms)
    }

    fn has_capability(&self, capability: ComponentCapability) -> bool {
        self.required_capabilities.contains(&capability)
    }

    fn observation_scope(&self, phase: Phase, context: Option<&ToolContext>) -> Option<Arc<guest_observation::Scope>> {
        self.observes.then(|| guest_observation::Scope::new(&self.evidence_component, phase, context)).flatten()
    }

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
                store.data_mut().guest_observation = self.observation_scope(Phase::Describe, None).map(|scope| (scope, Phase::Describe));
                let result = bindings
                    .call_describe(store, self.host_info_json.as_deref().unwrap_or_default())
                    .map_err(|error| self.execution_failure(boundary, error, Phase::Describe))?;
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
                web: None,
                references: None,
                script_profile: self.script_profile,
                guest_observation: self.observation_scope(Phase::Instantiate, None).map(|scope| (scope, Phase::Instantiate)),
                borrowed_processes: HashSet::new(),
                admitted_process_scope: None,
                process_launch_uncertain: false,
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
                patch_version_refs: Vec::new(),
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
        if self.observes {
            guest_observation::link(&mut linker).map_err(|error| boundary.failure(error, Phase::Instantiate))?;
        }
        if self.session_script {
            script::link(&mut linker).map_err(|error| boundary.failure(error, Phase::Instantiate))?;
        }
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
            {
                let mut interface = linker
                    .instance("microcrab:host/glob-ignore@0.1.0")
                    .map_err(|error| {
                        boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                    })?;
                interface
                    .func_wrap("snapshot", operations::glob_ignore_snapshot)
                    .map_err(|error| {
                        boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                    })?;
                interface
                    .func_wrap("glob-from-state", operations::glob_from_state)
                    .map_err(|error| {
                        boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                    })?;
                interface
                    .func_wrap("glob-ignored-paths", operations::glob_ignored_paths)
                    .map_err(|error| {
                        boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                    })?;
                interface
                    .func_wrap(
                        "glob-with-ignored-paths",
                        operations::glob_with_ignored_paths,
                    )
                    .map_err(|error| {
                        boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                    })?;
            }
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
            if self.has_capability(ComponentCapability::WebTransport) {
                web_transport::link(&mut linker, self.scoped_web_transport).map_err(|error| {
                    boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
                })?;
            }
            file_versions::link(&mut linker).map_err(|error| {
                boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
            })?;
            // Generated linker helpers install common type identities as well as
            // resource functions; valid unsupported operations return typed errors.
            resources::add_to_linker(&mut linker).map_err(|error| {
                boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
            })?;
            output::add_to_linker(&mut linker).map_err(|error| {
                boundary.failure(error, crate::observation::wasm::Phase::Instantiate)
            })?;
        }
        let bindings = Runtime::instantiate(&mut store, &self.component, &linker)
            .map_err(|error| self.execution_failure(boundary, error, Phase::Instantiate))?;
        store.data_mut().guest_observation = None;
        Ok(Instance {
            init_ready: false,
            store,
            bindings,
            poisoned: false,
            failure_evidence: None,
            recovery_cleanup: None,
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
            scope.0.failure_evidence = FailureEvidence::from_error(&error);
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
        state.process_launch_uncertain = false;
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
        state.patch_version_refs.clear();
        scope
            .0
            .store
            .set_hostcall_fuel(self.limits.max_output_bytes);
        // Wasmtime 47 TypedFunc::call (used by bindgen) automatically runs
        // canonical post-return before returning the owned result to the host.
        let result = call(&scope.0.bindings, &mut scope.0.store, &boundary);
        scope.0.failure_evidence = result.as_ref().err().and_then(FailureEvidence::from_error);
        if scope.0.failure_evidence.is_some() {
            scope.0.poisoned = true;
            // File and prepared output owners are registered independently of
            // the Store. Let the retiring slot join those owners after fencing
            // driver reentry and dropping InvocationScope, never under a guest wait.
            if scope.0.store.data().tool_context.as_ref().is_some_and(|context| context.subject.is_tool()) {
                scope.0.recovery_cleanup = RecoveryCleanup::capture(scope.0.store.data());
                if scope.0.recovery_cleanup.is_some() {
                    if epoch_cancellation.is_cancelled()
                        || (cancellation.is_cancelled() && !scope.0.store.data().has_mutation_outcome())
                    {
                        return Err(Error::Cancelled);
                    }
                    return result;
                }
            }
        }
        // Cleanup is independent of caller cancellation and must settle before ready.
        let files_cleanup = resources::files::cleanup(scope.0.store.data_mut());
        let processes_cleanup = lifecycle::cleanup_init_processes(scope.0.store.data_mut());
        let cleanup = files_cleanup.and(processes_cleanup);
        let cleanup_succeeded = cleanup.is_ok();
        let result = match (result, cleanup) {
            (_, Err(error)) | (Err(error), Ok(())) => Err(error),
            (Ok(value), Ok(())) => Ok(value),
        };
        scope.0.failure_evidence = result.as_ref().err().and_then(FailureEvidence::from_error);
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
            use crate::observation::diagnostics::{
                DiagnosticCode, DiagnosticLayer, DiagnosticPhase, ToolDiagnostic, WasmFailure,
            };
            // Keep evidence from a real engine failure, including OutOfFuel if
            // it won the race. If the post-call wall check alone expired, say
            // so without inventing a trap or poisoning the retained instance.
            if scope.0.poisoned && let Err(error) = result {
                return Err(error);
            }
            let mut diagnostic = ToolDiagnostic::new(
                DiagnosticCode::Timeout,
                DiagnosticLayer::Runtime,
                DiagnosticPhase::GuestExecution,
            );
            diagnostic.wasm_failure = Some(WasmFailure::WallDeadline {
                wall_timeout_ms: Some(self.limits.timeout_ms),
            });
            diagnostic.effective_timeout_ms = Some(self.limits.timeout_ms);
            return Err(Error::Diagnostic(diagnostic));
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
        if result.is_ok() {
            web_transport::finish_invocation(scope.0.store.data_mut());
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
    fn uses_literal_sources(&self) -> bool {
        matches!(self.bindings.web, Some(authority::WebBinding::Open { .. }))
    }
    fn secret_delivery(&self) -> super::tool::SecretDelivery {
        if self.uses_literal_sources() {
            // The exact endpoint is the authority and audit record; never
            // interpolate a secret into it.
            super::tool::SecretDelivery::ForwardPlaceholders
        } else {
            super::tool::SecretDelivery::Resolve
        }
    }
    fn permission_defaults(&self) -> Vec<crate::policy::Permission> {
        if self.uses_literal_sources() {
            vec![crate::policy::Permission {
                agent: None,
                mode: None,
                tool: Some(crate::policy::Matcher::Exact(self.definition.name.clone())),
                args: vec![],
                action: crate::policy::Action::Ask,
            }]
        } else {
            vec![]
        }
    }
    fn definition(&self) -> ToolDefinition {
        self.definition.clone()
    }
    fn file_version_host(&self) -> Option<&dyn Host> {
        self.compiled
            .file_versions
            .then(|| self.compiled.host.as_deref())
            .flatten()
    }
    fn component_identity(&self) -> Option<&str> {
        Some(&self.compiled.admission_identity)
    }
    fn uses_glob_path_default(&self) -> bool {
        self.compiled.glob_paths_linked
            && self
                .definition
                .parameters
                .pointer("/properties/ignored_list/default")
                .and_then(Value::as_str)
                == Some(super::tool::GLOB_PATH_DEFAULT)
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
        let arguments = if let Some(expected) = invocation.file_version_policy_arguments.as_deref()
        {
            // Revalidate scope and canonical versions on the actual dispatch copy.
            // Secret expansion may affect content, but may not substitute path/version.
            let host = self
                .compiled
                .host
                .as_deref()
                .ok_or_else(|| Error::tool("Host unavailable"))?;
            file_versions::dispatch_arguments(host, &invocation, &arguments, expected).await?
        } else {
            arguments
        };
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
        // In-flight waiters on the old generation keep that identity. Only a
        // separately dispatched call that sees it already fenced may select a
        // successor, and then it cannot retry after another trap.
        let observed_failed = slot.already_failed();
        let mut lease = slot.tool_lease(&cancellation).await?;
        if !lease.ready() && lease.poisoned().is_ok() {
            return Err(Error::Lifecycle("Agent initialization has not settled for this retained instance; initialize it with an explicit AgentInit scope or use Session".into()));
        }
        invocation
            .begin_dispatch(Some(&self.compiled.admission_identity))
            .map_err(admission_error)?;
        let (slot, lease) = if let Err(error) = lease.poisoned() {
            if !observed_failed {
                return Err(error);
            }
            let acknowledged = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(Error::Cancelled),
                result = lease.acknowledge_retirement(&slot) => result,
            };
            if !acknowledged {
                return Err(error);
            }
            let successor = instances.replace(&compiled, &slot)?.ok_or(error)?;
            drop(lease);
            let successor_lease = successor.tool_lease(&cancellation).await?;
            successor_lease.poisoned()?;
            (successor, successor_lease)
        } else {
            (slot, lease)
        };
        if !lease.ready() {
            return Err(Error::Lifecycle("Agent initialization has not settled for this retained instance; initialize it with an explicit AgentInit scope or use Session".into()));
        }
        let web = if let Some(web_authority) = &self.bindings.web {
            if binding.is_none() {
                return Err(Error::denied("web transport requires an attached Session"));
            }
            Some(web_transport::Invocation::new(
                slot.web.clone(),
                compiled.web_transport_binding.clone(),
                compiled.admission_identity.clone(),
                &invocation,
                &effective_arguments,
                web_authority,
            )?.with_scoped_transport(compiled.scoped_web_transport))
        } else {
            None
        };
        self.bindings.bind_exact(&invocation, &effective_arguments)?;
        let ingress = if compiled.session_script {
            Some(references::Ingress::new(&invocation, &cancellation).await?)
        } else {
            None
        };
        let observation = compiled.observation_scope(Phase::Invoke, Some(&invocation));
        let start_after_adoption = invocation.state.is_attached();
        let driver_scope = driver::Scope::capture(&invocation, observation.clone());
        let registration = driver_scope.registration();
        let driver_slot = slot.clone();
        let driver_compiled = compiled.clone();
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        let result = tokio::task::spawn_blocking(move || {
            tracing::dispatcher::with_default(&dispatch, || {
                let epoch_cancellation = slot.cancellation.clone();
                let run = |cached: &mut Option<Instance>| {
                    let commit_context = ingress.as_ref().map(|_| invocation.clone());
                    let mut staged_bytes = None;
                    let prepared = compiled.run(
                        cached,
                        cancellation.clone(),
                        epoch_cancellation.clone(),
                        Some(runtime.clone()),
                        binding,
                        Some(invocation),
                        Some((name.clone(), effective_arguments)),
                        |bindings, store, boundary| {
                            store.data_mut().guest_observation = observation.map(|scope| (scope, Phase::Invoke));
                            store.data_mut().references = ingress.clone();
                            store.data_mut().web = web.map(|mut invocation| {
                                invocation.enter();
                                invocation
                            });
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
                                    compiled.execution_failure(boundary, error, Phase::Invoke)
                                })?;
                            let mut prepared = output::lift(store.data_mut(), value)?;
                            let success = match &prepared.outcome {
                                NativeToolOutput::Final(output) => output.failure.is_none(),
                                NativeToolOutput::Buffered(_) => true,
                            };
                            if let Some(web_transport) = &mut store.data_mut().web {
                                if success && web_transport.requires_handoff() {
                                    let source = prepared.driver.as_mut().ok_or_else(|| Error::wasm("connection handoff requires an owned guest continuation"))?;
                                    source.retain_web(web_transport.take_handoff()?);
                                }
                                web_transport.guest_returned(success);
                            }
                            if success {
                                staged_bytes = store.data_mut().references.as_mut().and_then(references::Ingress::take);
                            }
                            Ok(prepared)
                        },
                    )?;
                    if let Some(staged) = staged_bytes {
                        let context = commit_context.as_ref().ok_or_else(|| Error::denied("references admission missing"))?;
                        let commit_cancel = cancellation.child_token();
                        runtime.block_on(async {
                            let commit = staged.commit(context, &commit_cancel);
                            tokio::pin!(commit);
                            tokio::select! { biased;
                                _ = epoch_cancellation.cancelled() => {
                                    commit_cancel.cancel();
                                    commit.await
                                }
                                result = &mut commit => result,
                            }
                        })?;
                    }
                    if prepared.driver.is_some() && let Some(registration) = registration.as_ref() {
                        slot.track_driver(registration);
                    }
                    Ok(prepared)
                };
                slot.run(lease, run)
            })
        })
        .await
        .map_err(|_| Error::wasm("WASM worker failed"))?;
        drop(guard);
        let mut prepared = result?;
        if let Some(source) = prepared.driver {
            let NativeToolOutput::Buffered(output) = &mut prepared.outcome else {
                return Err(Error::wasm("guest continuation requires an owned buffered output"));
            };
            let start = move || driver::start(driver_compiled, driver_slot, driver_scope, source);
            if start_after_adoption {
                output.start_on_adoption(start);
            } else {
                // Detached embedders own the returned buffer directly.
                start();
            }
        }
        Ok(prepared.outcome)
    }
}
