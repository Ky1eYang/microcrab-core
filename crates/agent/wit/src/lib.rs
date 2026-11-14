//! Standard agent Component Model contract: Runtime-frozen host-info and exports.
//! Resource operations and optional Session services belong to their own packages.

pub const ABI_VERSION: &str = "0.7.0";
pub const HOST_INTERFACE: &str = "microcrab:agent/host-info@0.7.0";
pub const EXECUTE_MODE: &str = "execute";
pub const WIT_SOURCE: &str = include_str!("../wit/agent.wit");
