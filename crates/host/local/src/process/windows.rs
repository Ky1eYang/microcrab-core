//! Kill-on-close Windows Job owned by one supervisor; handles never inherit.

use super::native_failure;
use std::{io, mem::size_of, ptr::null, time::Instant};
use tokio::process::{Child, Command};
use windows_sys::Win32::{
    Foundation::{CloseHandle, ERROR_NO_MORE_FILES, HANDLE, INVALID_HANDLE_VALUE},
    System::{
        Diagnostics::ToolHelp::{
            CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
        },
        JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
            JobObjectBasicAccountingInformation, JobObjectExtendedLimitInformation,
            QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
        },
        Threading::{
            CREATE_NO_WINDOW, CREATE_SUSPENDED, OpenThread, ResumeThread, THREAD_SUSPEND_RESUME,
        },
    },
};

// An owned kernel handle is a value. Integer storage permits moving ownership
// between executor threads without claiming a borrowed pointer is thread-safe.
struct OwnedHandle(usize);
impl OwnedHandle {
    fn new(raw: HANDLE) -> io::Result<Self> {
        if raw.is_null() || raw == INVALID_HANDLE_VALUE {
            Err(io::Error::last_os_error())
        } else {
            Ok(Self(raw as usize))
        }
    }
    fn raw(&self) -> HANDLE {
        self.0 as HANDLE
    }
}
impl Drop for OwnedHandle {
    fn drop(&mut self) {
        // SAFETY: this non-inherited handle has exactly one owner and closes once.
        if unsafe { CloseHandle(self.raw()) } == 0 {
            native_failure(
                "process.cleanup",
                "close_handle",
                &io::Error::last_os_error(),
                None,
            );
        }
    }
}

pub(crate) struct ProcessTree(OwnedHandle, Option<u32>);
impl ProcessTree {
    pub(crate) fn terminate(&self) -> io::Result<()> {
        // SAFETY: the Job handle is valid for the entire call and remains owned.
        if unsafe { TerminateJobObject(self.0.raw(), 1) } == 0 {
            let error = io::Error::last_os_error();
            native_failure("process.terminate", "terminate_job", &error, self.1);
            return Err(error);
        }
        Ok(())
    }

    /// Termination is asynchronous in Windows. Do not release admission until
    /// accounting confirms the enrolled tree is empty, or report cleanup failure.
    pub(crate) async fn wait_empty(&self) -> io::Result<()> {
        let started = Instant::now();
        let mut last_active_processes = None;
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let mut accounting = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
                // SAFETY: this valid Job owns the queried processes; the writable
                // initialized buffer exactly matches the selected information class.
                if unsafe {
                    QueryInformationJobObject(
                        self.0.raw(),
                        JobObjectBasicAccountingInformation,
                        (&mut accounting as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
                        size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
                        std::ptr::null_mut(),
                    )
                } == 0
                {
                    let error = io::Error::last_os_error();
                    native_failure("process.cleanup", "query_job_accounting", &error, self.1);
                    return Err(error);
                }
                last_active_processes = Some(accounting.ActiveProcesses);
                if accounting.ActiveProcesses == 0 {
                    return Ok(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .map_err(|_| {
            tracing::error!(
                operation = "process.cleanup", phase = "wait_job_empty", pid = self.1,
                error_kind = ?io::ErrorKind::TimedOut,
                timeout_ms = 2000_u64, elapsed_ms = started.elapsed().as_millis() as u64,
                last_active_processes,
                "local Windows job cleanup timed out"
            );
            io::Error::new(
                io::ErrorKind::TimedOut,
                "Windows shell Job did not become empty after termination",
            )
        })?
    }
}

pub(crate) fn spawn(mut command: Command) -> io::Result<(Child, ProcessTree)> {
    // SAFETY: null security attributes and name create a private non-inherited Job.
    let job =
        OwnedHandle::new(unsafe { CreateJobObjectW(null(), null()) }).inspect_err(|error| {
            native_failure("process.spawn", "create_job", error, None);
        })?;
    let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
    limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
    // SAFETY: the initialized buffer matches the requested information class.
    if unsafe {
        SetInformationJobObject(
            job.raw(),
            JobObjectExtendedLimitInformation,
            (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
            size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
        )
    } == 0
    {
        let error = io::Error::last_os_error();
        native_failure("process.spawn", "set_job_limits", &error, None);
        return Err(error);
    }
    command.creation_flags(CREATE_SUSPENDED | CREATE_NO_WINDOW);
    let mut child = command.spawn().inspect_err(|error| {
        native_failure("process.spawn", "spawn_suspended", error, None);
    })?;
    let setup = (|| {
        let process = child
            .raw_handle()
            .ok_or_else(|| io::Error::other("spawned shell has no process handle"))?;
        // SAFETY: the child still owns the valid process handle and has not run.
        if unsafe { AssignProcessToJobObject(job.raw(), process as HANDLE) } == 0 {
            let error = io::Error::last_os_error();
            native_failure("process.spawn", "assign_job", &error, child.id());
            return Err(error);
        }
        let thread = primary_thread(
            child
                .id()
                .ok_or_else(|| io::Error::other("spawned shell has no pid"))?,
        )?;
        // SAFETY: this is the suspended child's sole initial thread; the owned
        // handle has resume permission and cannot close during this call.
        let previous = unsafe { ResumeThread(thread.raw()) };
        if previous == u32::MAX {
            let error = io::Error::last_os_error();
            native_failure("process.spawn", "resume_thread", &error, child.id());
            return Err(error);
        }
        if previous != 1 {
            tracing::error!(
                operation = "process.spawn",
                phase = "resume_thread_count",
                pid = child.id(),
                previous_suspend_count = previous,
                "local suspended child thread count was unexpected"
            );
            return Err(io::Error::other(
                "unexpected initial thread suspension count",
            ));
        }
        Ok(())
    })();
    if let Err(error) = setup {
        if let Err(kill_error) = child.start_kill() {
            native_failure(
                "process.cleanup",
                "setup_start_kill",
                &kill_error,
                child.id(),
            );
        }
        // kill_on_drop registers asynchronous reaping; job closure kills any
        // enrolled descendants even if setup failed after assignment.
        return Err(error);
    }
    let pid = child.id();
    Ok((child, ProcessTree(job, pid)))
}

fn primary_thread(pid: u32) -> io::Result<OwnedHandle> {
    // SAFETY: the snapshot owns system thread metadata; no pointers are borrowed.
    let snapshot = OwnedHandle::new(unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) })
        .inspect_err(|error| {
            native_failure("process.spawn", "snapshot_threads", error, Some(pid));
        })?;
    let mut entry = THREADENTRY32 {
        dwSize: size_of::<THREADENTRY32>() as u32,
        ..Default::default()
    };
    let mut found = None;
    let mut phase = "thread_first";
    // SAFETY: entry is initialized with the required layout and size.
    let mut available = unsafe { Thread32First(snapshot.raw(), &mut entry) };
    loop {
        if available == 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(ERROR_NO_MORE_FILES as i32) {
                native_failure("process.spawn", phase, &error, Some(pid));
                return Err(error);
            }
            break;
        }
        if entry.th32OwnerProcessID == pid {
            if found.is_some() {
                let error = io::Error::other("suspended child unexpectedly has multiple threads");
                native_failure(
                    "process.spawn",
                    "multiple_primary_threads",
                    &error,
                    Some(pid),
                );
                return Err(error);
            }
            found = Some(entry.th32ThreadID);
        }
        entry.dwSize = size_of::<THREADENTRY32>() as u32;
        phase = "thread_next";
        // SAFETY: snapshot and correctly sized writable entry remain valid.
        available = unsafe { Thread32Next(snapshot.raw(), &mut entry) };
    }
    let id = found.ok_or_else(|| {
        let error = io::Error::other("suspended child thread not found");
        native_failure("process.spawn", "primary_thread_missing", &error, Some(pid));
        error
    })?;
    // SAFETY: the thread id belongs to the suspended child; the returned handle
    // is private and requests only the permission needed to resume that thread.
    OwnedHandle::new(unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, id) }).inspect_err(|error| {
        native_failure("process.spawn", "open_primary_thread", error, Some(pid));
    })
}
