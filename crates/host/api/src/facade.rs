//! Object-safe metadata facade over independent resource owners.
use crate::{
    filesystem::FsApi,
    net::NetApi,
    process::ProcessApi,
    shell::ShellApi,
    types::{Capabilities, Platform},
};
use std::sync::LazyLock;

static EMPTY_CAPABILITIES: LazyLock<Capabilities> = LazyLock::new(Capabilities::default);

/// Metadata describes the executing host, including remote hosts; it never
/// authorizes operations. Implementations freeze capabilities for their lifetime.
/// Invoke colliding resource methods with qualified trait calls.
pub trait HostAPI: ProcessApi + ShellApi + FsApi + NetApi + crate::FileMutationApi {
    fn platform(&self) -> Platform {
        Platform::Other
    }
    fn capabilities(&self) -> &Capabilities {
        &EMPTY_CAPABILITIES
    }
    fn supports(&self, operation: &str) -> bool {
        self.capabilities().supports(operation)
    }
}
