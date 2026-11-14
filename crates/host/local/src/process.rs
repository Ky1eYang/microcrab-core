//! Own the entire shell process tree. Windows enrolls a suspended child before
//! user code runs; POSIX uses a fresh process group. This is lifecycle control,
//! not containment against commands that deliberately escape OS process groups.

/// Record only native error facts: Display/source text can contain command paths.
/// Call at the failing operation, before cleanup can replace its public outcome.
pub(crate) fn native_failure(
    operation: &'static str,
    phase: &'static str,
    error: &std::io::Error,
    pid: Option<u32>,
) {
    tracing::error!(
        target: "microcrab_host_local::process",
        operation,
        phase,
        pid,
        error_kind = ?error.kind(),
        raw_os_error = error.raw_os_error(),
        "local native process operation failed"
    );
}

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub(crate) use windows::{ProcessTree, spawn};

#[cfg(unix)]
pub(crate) struct ProcessTree(u32);

#[cfg(unix)]
pub(crate) fn spawn(
    mut command: tokio::process::Command,
) -> std::io::Result<(tokio::process::Child, ProcessTree)> {
    command.process_group(0);
    let child = command.spawn().inspect_err(|error| {
        native_failure("process.spawn", "spawn", error, None);
    })?;
    let id = child
        .id()
        .ok_or_else(|| std::io::Error::other("spawned shell has no process id"))?;
    Ok((child, ProcessTree(id)))
}

#[cfg(unix)]
impl ProcessTree {
    pub(crate) fn terminate(&self) -> std::io::Result<()> {
        // SAFETY: the positive pid identifies the process group created by spawn;
        // negation addresses only that group and no Rust memory is accessed.
        if unsafe { libc::kill(-(self.0 as i32), libc::SIGKILL) } == -1 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                native_failure(
                    "process.terminate",
                    "kill_process_group",
                    &error,
                    Some(self.0),
                );
                return Err(error);
            }
        }
        Ok(())
    }
}

#[cfg(unix)]
impl Drop for ProcessTree {
    fn drop(&mut self) {
        let _ = self.terminate();
    }
}
