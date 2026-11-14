//! Primitive availability describes implemented backend behavior, not policy.
//! Process and shell operations delegate to the single native process owner.
use crate::LocalHost;
use async_trait::async_trait;
use microcrab_host_api::{Capabilities, HostAPI, NetApi, Platform};

#[async_trait]
impl NetApi for LocalHost {}

impl HostAPI for LocalHost {
    fn platform(&self) -> Platform {
        if cfg!(target_os = "windows") {
            Platform::Windows
        } else if cfg!(target_os = "macos") {
            Platform::MacOS
        } else if cfg!(target_os = "linux") {
            Platform::Linux
        } else {
            Platform::Other
        }
    }
    fn capabilities(&self) -> &Capabilities {
        static FILES: std::sync::LazyLock<Capabilities> =
            std::sync::LazyLock::new(|| Capabilities {
                supported_operations: [
                    "file_mutation",
                    "filesystem.stat",
                    "filesystem.list",
                    "filesystem.open",
                    "filesystem.read",
                    "filesystem.close",
                    "filesystem.atomic-write",
                ]
                .into_iter()
                .map(str::to_owned)
                .collect(),
                ..Capabilities::default()
            });
        static PROCESSES: std::sync::LazyLock<Capabilities> = std::sync::LazyLock::new(|| {
            let mut capabilities = FILES.clone();
            capabilities.process_kill_tree = true;
            capabilities.supported_operations.extend(
                [
                    "process.spawn",
                    "process.status",
                    "process.pid",
                    "process.watch",
                    "process.wait",
                    "process.kill-tree",
                    "process.stdout",
                    "process.stderr",
                    "process.write-stdin",
                    "process.close-stdin",
                    "process.release",
                    "process.shell-spec",
                    "process.exec",
                    "shell.launch",
                    "shell.capture-status",
                    "shell.read",
                    "shell.drain",
                    "shell.write",
                ]
                .into_iter()
                .map(str::to_owned),
            );
            capabilities
        });
        if self.require_process_execution("process.spawn").is_ok() {
            &PROCESSES
        } else {
            &FILES
        }
    }
}
