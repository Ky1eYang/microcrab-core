//! Session-owned process policy and UI projection; Host owns execution and capture.
mod control;
mod registry;
mod source;
pub use control::ProcessControl;
use microcrab_host_api::ShellPosition;
pub use registry::{OwnedProcess, ProcessId, ProcessRegistry, ScopedProcessControl};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "lowercase")]
pub enum SessionProcessState {
    Running,
    Exited { code: i32 },
    Killed,
    Failed { reason: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionProcessStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(flatten)]
    pub state: SessionProcessState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    pub closed: bool,
    pub cursor: ShellPosition,
    pub stdout_bytes: usize,
    pub stderr_bytes: usize,
    pub truncated: bool,
    pub elapsed_s: u64,
}
