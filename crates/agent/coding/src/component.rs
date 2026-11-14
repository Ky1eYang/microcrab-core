//! The production adapter imports only the Host interfaces selected by this guest.
use crate::host::*;
include!(concat!(env!("OUT_DIR"), "/guest-bindings.rs"));
use microcrab::host::{
    cache_points, file_mutation, glob, grep, process, shell, text_read, text_write, tool_types,
    types,
};

struct Coding;
struct ImportedHost {
    output: Option<microcrab::agent::execution::Buffer>,
    process: Option<ProcessHandle>,
    state: Option<ProcessState>,
    truncated: bool,
}
fn error(e: tool_types::OperationError) -> OperationError {
    let code = match e.code {
        tool_types::OperationErrorCode::Denied => OperationErrorCode::Denied,
        tool_types::OperationErrorCode::InvalidRequest => OperationErrorCode::InvalidRequest,
        tool_types::OperationErrorCode::Unsupported => OperationErrorCode::Unsupported,
        tool_types::OperationErrorCode::Conflict => OperationErrorCode::Conflict,
        tool_types::OperationErrorCode::Limit => OperationErrorCode::Limit,
        tool_types::OperationErrorCode::Cancelled => OperationErrorCode::Cancelled,
        tool_types::OperationErrorCode::Timeout => OperationErrorCode::Timeout,
        tool_types::OperationErrorCode::Io => OperationErrorCode::Io,
    };
    OperationError {
        code,
        message: e.message,
    }
}
fn host_error(e: types::HostError) -> OperationError {
    let code = match e.category {
        types::ErrorCategory::NotFound => OperationErrorCode::Io,
        types::ErrorCategory::Permission => OperationErrorCode::Denied,
        types::ErrorCategory::Timeout => OperationErrorCode::Timeout,
        types::ErrorCategory::Cancelled => OperationErrorCode::Cancelled,
        types::ErrorCategory::Conflict => OperationErrorCode::Conflict,
        types::ErrorCategory::Unsupported => OperationErrorCode::Unsupported,
        types::ErrorCategory::Io => OperationErrorCode::Io,
    };
    OperationError {
        code,
        message: e.message,
    }
}
fn process_handle(h: ProcessHandle) -> process::ProcessHandle {
    process::ProcessHandle {
        id: h.id,
        pid: h.pid,
    }
}
fn mutation_to_abi(request: FileMutation) -> file_mutation::FileMutation {
    match request {
        FileMutation::Write(r) => file_mutation::FileMutation::Write(text_write::WriteRequest {
            path: r.path,
            content: r.content,
            precondition: match r.precondition {
                WritePrecondition::Missing => text_write::WritePrecondition::Missing,
                WritePrecondition::Version(v) => text_write::WritePrecondition::Version(v),
            },
        }),
        FileMutation::Delete(r) => {
            file_mutation::FileMutation::Delete(file_mutation::DeleteRequest {
                path: r.path,
                expected_version: r.expected_version,
            })
        }
        FileMutation::Move(r) => file_mutation::FileMutation::Move(file_mutation::MoveRequest {
            path: r.path,
            destination: r.destination,
            expected_version: r.expected_version,
            content: r.content,
        }),
    }
}
fn mutation_from_abi(request: file_mutation::FileMutation) -> FileMutation {
    match request {
        file_mutation::FileMutation::Write(r) => FileMutation::Write(WriteRequest {
            path: r.path,
            content: r.content,
            precondition: match r.precondition {
                text_write::WritePrecondition::Missing => WritePrecondition::Missing,
                text_write::WritePrecondition::Version(v) => WritePrecondition::Version(v),
            },
        }),
        file_mutation::FileMutation::Delete(r) => FileMutation::Delete(DeleteRequest {
            path: r.path,
            expected_version: r.expected_version,
        }),
        file_mutation::FileMutation::Move(r) => FileMutation::Move(MoveRequest {
            path: r.path,
            destination: r.destination,
            expected_version: r.expected_version,
            content: r.content,
        }),
    }
}
impl Host for ImportedHost {
    fn preflight_mutations(
        &mut self,
        requests: Vec<FileMutation>,
    ) -> Result<Vec<FileMutation>, OperationError> {
        let requests: Vec<_> = requests.into_iter().map(mutation_to_abi).collect();
        file_mutation::preflight(&requests)
            .map(|requests| requests.into_iter().map(mutation_from_abi).collect())
            .map_err(error)
    }
    fn mutate(&mut self, request: FileMutation) -> Result<MutationOutput, OperationError> {
        let out = file_mutation::mutate(&mutation_to_abi(request)).map_err(error)?;
        Ok(MutationOutput {
            version: out.version,
            bytes: out.bytes,
            source_removed: out.source_removed,
            destination_written: out.destination_written,
            diagnostic: out.diagnostic,
        })
    }
    fn capture(&mut self, path: &str, max_bytes: u64) -> Result<(), OperationError> {
        cache_points::capture(path, max_bytes).map_err(error)
    }
    fn read(&mut self, r: ReadRequest) -> Result<ReadOutput, OperationError> {
        let o = text_read::read(&text_read::ReadRequest {
            path: r.path,
            offset_line: r.offset_line,
            max_lines: r.max_lines,
            max_bytes: r.max_bytes,
        })
        .map_err(error)?;
        Ok(ReadOutput {
            content: o.content,
            start_line: o.start_line,
            next_line: o.next_line,
            truncated: o.truncated,
            version: o.version,
        })
    }
    fn write(&mut self, r: WriteRequest) -> Result<WriteOutput, OperationError> {
        let precondition = match r.precondition {
            WritePrecondition::Missing => text_write::WritePrecondition::Missing,
            WritePrecondition::Version(v) => text_write::WritePrecondition::Version(v),
        };
        let o = text_write::write(&text_write::WriteRequest {
            path: r.path,
            content: r.content,
            precondition,
        })
        .map_err(error)?;
        Ok(WriteOutput {
            version: o.version,
            bytes: o.bytes,
        })
    }
    fn glob(&mut self, r: GlobRequest) -> Result<GlobOutput, OperationError> {
        let o = glob::glob(&glob::GlobRequest {
            root: r.root,
            pattern: r.pattern,
            include_hidden: r.include_hidden,
            include_ignored: r.include_ignored,
            include_directories: r.include_directories,
            max_results: r.max_results,
        })
        .map_err(error)?;
        Ok(GlobOutput {
            entries: o
                .entries
                .into_iter()
                .map(|entry| GlobEntry {
                    path: entry.path,
                    kind: match entry.kind {
                        glob::GlobEntryKind::File => GlobEntryKind::File,
                        glob::GlobEntryKind::Directory => GlobEntryKind::Directory,
                        glob::GlobEntryKind::Symlink => GlobEntryKind::Symlink,
                    },
                })
                .collect(),
            truncated: o.truncated,
        })
    }
    fn grep(&mut self, r: GrepRequest) -> Result<GrepOutput, OperationError> {
        let o = grep::grep(&grep::GrepRequest {
            root: r.root,
            pattern: r.pattern,
            include_glob: r.include_glob,
            case_sensitive: r.case_sensitive,
            literal: r.literal,
            max_matches: r.max_matches,
            max_bytes: r.max_bytes,
        })
        .map_err(error)?;
        Ok(GrepOutput {
            matches: o
                .matches
                .into_iter()
                .map(|m| GrepMatch {
                    path: m.path,
                    line: m.line,
                    text: m.text,
                })
                .collect(),
            truncated: o.truncated,
        })
    }
    fn shell_launch(&mut self, r: ShellRequest) -> Result<ProcessHandle, OperationError> {
        if self.output.is_none() || self.process.is_some() {
            return Err(OperationError {
                code: OperationErrorCode::Conflict,
                message: "Shell requires one reserved invocation output".into(),
            });
        }
        let h = shell::launch(&shell::ShellRequest {
            command: r.command,
            cwd: r.cwd,
        })
        .map_err(host_error)?;
        let handle = ProcessHandle {
            id: h.id,
            pid: h.pid,
        };
        self.process = Some(handle);
        Ok(handle)
    }
    fn process_wait_for_state(
        &mut self,
        handle: ProcessHandle,
        yield_ms: u64,
    ) -> Result<ProcessState, OperationError> {
        let status =
            process::wait_for_state(process_handle(handle), yield_ms).map_err(host_error)?;
        let state = match status.state {
            process::ProcessState::Running => ProcessState::Running,
            process::ProcessState::Exited(status) => ProcessState::Exited { code: status.code },
            process::ProcessState::Failed(reason) => ProcessState::Failed(reason),
        };
        self.state = Some(state.clone());
        Ok(state)
    }
    fn shell_read(
        &mut self,
        handle: ProcessHandle,
        max_bytes: u64,
    ) -> Result<ShellOutput, OperationError> {
        let output = shell::read(process_handle(handle), max_bytes).map_err(host_error)?;
        self.truncated = output.truncated;
        Ok(ShellOutput {
            stdout: output.stdout,
            stderr: output.stderr,
            truncated: output.truncated,
        })
    }
}
fn checked_host(supplied: &str) -> Result<microcrab_agent_types::HostInfo, String> {
    let imported = microcrab::agent::host_info::host_info().map_err(|e| e.message)?;
    let supplied: microcrab_agent_types::HostInfo =
        serde_json::from_str(supplied).map_err(|e| e.to_string())?;
    let imported: microcrab_agent_types::HostInfo =
        serde_json::from_str(&imported).map_err(|e| e.to_string())?;
    if supplied != imported {
        return Err("host-info import does not match the supplied HostInfo".into());
    }
    Ok(supplied)
}
fn reported_operation_failure(error: OperationError) -> microcrab::agent::execution::ToolFailure {
    use microcrab::agent::execution::{FailureKind, ToolFailure};
    ToolFailure {
        kind: match error.code {
            OperationErrorCode::Timeout => FailureKind::Timeout,
            OperationErrorCode::Cancelled => FailureKind::Cancelled,
            _ => FailureKind::Failed,
        },
        code: format!("{:?}", error.code).to_ascii_lowercase(),
        message: error.message,
        retryable: false,
    }
}
fn reported_failure(message: String) -> microcrab::agent::execution::ToolFailure {
    if let Ok(error) = serde_json::from_str::<serde_json::Value>(&message) {
        let kind = match error.get("code").and_then(serde_json::Value::as_str) {
            Some("timeout") => microcrab::agent::execution::FailureKind::Timeout,
            Some("cancelled") => microcrab::agent::execution::FailureKind::Cancelled,
            _ => microcrab::agent::execution::FailureKind::Failed,
        };
        return microcrab::agent::execution::ToolFailure {
            kind,
            code: error
                .get("code")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("failed")
                .into(),
            message: error
                .get("message")
                .and_then(serde_json::Value::as_str)
                .unwrap_or(&message)
                .into(),
            retryable: false,
        };
    }
    microcrab::agent::execution::ToolFailure {
        kind: microcrab::agent::execution::FailureKind::Failed,
        code: "failed".into(),
        message,
        retryable: false,
    }
}
impl Guest for Coding {
    fn init() -> InitResult {
        InitResult::Ready
    }

    fn describe(host_info: String) -> Result<String, String> {
        serde_json::to_string(&crate::describe(&checked_host(&host_info)?)?)
            .map_err(|e| e.to_string())
    }
    fn invoke(
        host_info: String,
        name: String,
        arguments: String,
    ) -> Result<microcrab::agent::execution::ToolOutput, microcrab::agent::execution::ToolFailure>
    {
        use microcrab::agent::execution::{self, BufferedOutput, FinalOutput, ToolOutput};
        let checked = checked_host(&host_info).map_err(reported_failure)?;
        let output = execution::create(&["result".into(), "stdout".into(), "stderr".into()])?;
        let mut host = ImportedHost {
            output: Some(output),
            process: None,
            state: None,
            truncated: false,
        };
        let content =
            crate::invoke(&checked, &name, &arguments, &mut host).map_err(reported_failure)?;
        let reserved = host
            .output
            .take()
            .ok_or_else(|| reported_failure("Invocation output reservation absent".into()))?;
        let Some(handle) = host.process else {
            if content.len() > 512 && name != "apply_patch" {
                execution::append(&reserved, Some("result"), &content)?;
                execution::finish(&reserved, None)?;
                return Ok(ToolOutput::Buffered(BufferedOutput {
                    output: reserved,
                    include_initial: true,
                }));
            }
            return Ok(ToolOutput::Final(FinalOutput {
                content,
                failure: None,
            }));
        };
        if matches!(host.state, Some(ProcessState::Running)) || host.truncated {
            let output = reserved;
            let initial = shell::drain(process_handle(handle), u64::MAX)
                .map_err(|e| reported_operation_failure(host_error(e)))?;
            if initial.truncated {
                execution::mark_truncated(&output)?;
            }
            execution::append(&output, Some("stdout"), &initial.stdout)?;
            execution::append(&output, Some("stderr"), &initial.stderr)?;
            let state = serde_json::to_vec(
                &serde_json::json!({"id":handle.id,"pid":handle.pid,"loss_seen":initial.truncated}),
            )
            .map_err(|e| reported_failure(e.to_string()))?;
            execution::retain_process(&output, process_handle(handle), true)?;
            execution::attach_guest(&output, &state)?;
            return Ok(ToolOutput::Buffered(BufferedOutput {
                output,
                include_initial: true,
            }));
        }
        let output = shell::drain(process_handle(handle), u64::MAX)
            .map_err(|e| reported_operation_failure(host_error(e)))?;
        process::release(process_handle(handle))
            .map_err(|e| reported_operation_failure(host_error(e)))?;
        let failure = match &host.state {
            Some(ProcessState::Failed(message)) => Some(reported_failure(message.clone())),
            Some(ProcessState::Exited { code: Some(code) }) if *code != 0 => {
                Some(reported_failure(format!("Process exited with code {code}")))
            }
            _ => None,
        };
        if output.truncated || output.stdout.len() + output.stderr.len() > 64 {
            let buffered = reserved;
            if output.truncated {
                execution::mark_truncated(&buffered)?;
            }
            execution::append(&buffered, Some("stdout"), &output.stdout)?;
            execution::append(&buffered, Some("stderr"), &output.stderr)?;
            execution::finish(&buffered, failure.as_ref())?;
            return Ok(ToolOutput::Buffered(BufferedOutput {
                output: buffered,
                include_initial: true,
            }));
        }
        let content = serde_json::to_string(&serde_json::json!({"stdout":output.stdout,"stderr":output.stderr,"exit_code":match host.state { Some(ProcessState::Exited {code}) => code, _ => None }})).map_err(|e| reported_failure(e.to_string()))?;
        Ok(ToolOutput::Final(FinalOutput { content, failure }))
    }
    fn drive(
        output: &microcrab::agent::execution::Buffer,
        state: Vec<u8>,
        cancelled: bool,
    ) -> Result<microcrab::agent::execution::DriverStep, microcrab::agent::execution::ToolFailure>
    {
        use microcrab::agent::execution::{self, DriverStep};
        let value: serde_json::Value =
            serde_json::from_slice(&state).map_err(|e| reported_failure(e.to_string()))?;
        let id = value["id"]
            .as_u64()
            .ok_or_else(|| reported_failure("Missing process identity".into()))?;
        let pid = serde_json::from_value::<Option<u32>>(
            value
                .get("pid")
                .cloned()
                .ok_or_else(|| reported_failure("Missing process identity".into()))?,
        )
        .map_err(|e| reported_failure(e.to_string()))?;
        let handle = process::ProcessHandle { id, pid };
        let mut loss_seen = value["loss_seen"].as_bool().unwrap_or(false);
        let failure = |e| reported_operation_failure(host_error(e));
        if cancelled {
            process::kill_tree(handle, true).map_err(failure)?;
            let tail = shell::drain(handle, u64::MAX).map_err(failure)?;
            if tail.truncated && !loss_seen {
                execution::mark_truncated(output)?;
            }
            execution::append(output, Some("stdout"), &tail.stdout)?;
            execution::append(output, Some("stderr"), &tail.stderr)?;
            process::release(handle).map_err(failure)?;
            return Ok(DriverStep {
                state: Vec::new(),
                wake_after_ms: 0,
                finished: true,
            });
        }
        let status = process::status(handle).map_err(failure)?;
        let tail = shell::drain(handle, u64::MAX).map_err(failure)?;
        if tail.truncated && !loss_seen {
            execution::mark_truncated(output)?;
            loss_seen = true;
        }
        execution::append(output, Some("stdout"), &tail.stdout)?;
        execution::append(output, Some("stderr"), &tail.stderr)?;
        if matches!(status.state, process::ProcessState::Running) {
            return Ok(DriverStep {
                state: serde_json::to_vec(
                    &serde_json::json!({"id":id,"pid":pid,"loss_seen":loss_seen}),
                )
                .map_err(|e| reported_failure(e.to_string()))?,
                wake_after_ms: 20,
                finished: false,
            });
        }
        process::release(handle).map_err(failure)?;
        match status.state {
            process::ProcessState::Failed(message) => Err(reported_failure(message)),
            process::ProcessState::Exited(exit) if exit.code.is_some_and(|code| code != 0) => {
                Err(reported_failure(format!(
                    "Process exited with code {}",
                    exit.code.expect("checked exit code")
                )))
            }
            _ => Ok(DriverStep {
                state: Vec::new(),
                wake_after_ms: 0,
                finished: true,
            }),
        }
    }
}
export!(Coding);
