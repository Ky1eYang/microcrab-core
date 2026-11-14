//! Browser Host source delivery. Execution runs in an isolated browser realm;
//! this crate does not implement native OS resources or an Agent component.

/// Authority scope version for authenticated Conversation bindings.
pub const CONVERSATION_SCOPE_VERSION: u32 = 3;
/// Upstream shell/Pyodide artifact version required by the maintained installer.
pub const WASMSH_VERSION: &str = "0.5.1";
/// Browser entry source; generated assets are installed separately by xtask.
pub const SUPERVISOR_SOURCE: &str = include_str!("../browser/supervisor.mjs");
