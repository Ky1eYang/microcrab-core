//! Host-aware universal execute agent. The guest has no direct OS access.

use microcrab_agent_types::{
    AgentInfo, AgentMode, AgentRole, Hint, HostInfo, OsFamily, Rule, Variable,
};
use microcrab_protocol::ToolDefinition;
use serde_json::{Value, json};

fn shell_tool(host: &HostInfo) -> Result<Option<(&str, &'static str)>, String> {
    if !["shell", "process"]
        .iter()
        .all(|capability| host.capabilities.iter().any(|linked| linked == capability))
    {
        return Ok(None);
    }
    let name = host
        .shell_tool_name()
        .filter(|name| {
            !matches!(
                *name,
                "job" | "delegate" | "sleep" | "expand" | "switch_mode" | "view_image"
            )
        })
        .ok_or("Host shell must provide a portable, nonreserved tool name")?;
    let description = match host.os_family {
        OsFamily::Windows => "Execute a PowerShell command using pwsh on the Windows Host.",
        OsFamily::Unix => "Execute a Bash command on the Unix Host.",
        OsFamily::Other => {
            "Execute a command using the configured Host shell. Consult Host environment information for its syntax and limitations."
        }
    };
    Ok(Some((name, description)))
}

const SHELL_UNAVAILABLE: &str = "Shell execution is unavailable on this Host; linked shell and process capabilities are required.";

fn private_shell(host: &HostInfo) -> bool {
    host.capabilities
        .iter()
        .any(|capability| capability == "shell.launch-private")
}

pub fn describe(host: &HostInfo) -> Result<Vec<AgentInfo>, String> {
    let shell = shell_tool(host)?;
    let execution = "Returns a complete result or a buffered receipt with id=session_id/execution_id and optional nonconsuming initial stdout/stderr.";
    let yield_description = "Initial wait, 0..=60000 ms; zero requests an immediate receipt. Omission defaults to 1000; null is invalid. Ending the wait does not terminate or restart the process.";
    let mut tools: Vec<_> = shell.into_iter().map(|(name, description)| ToolDefinition {
        name: name.into(),
        description: format!("{description} {execution}"),
        parameters: json!({
            "type":"object",
            "properties":{
                "command":{"type":"string","minLength":1,"description":"Host shell command, nonempty and without NUL; null is invalid."},
                "cwd":{"type":"string","minLength":1,"description":"Host cwd, relative to its workspace root or absolute within it; nonempty, no NUL. Omission uses the workspace root; null is invalid."},
                "yield_ms":{"type":"integer","minimum":0,"maximum":60000,"default":1000,"description":yield_description}
            },
            "required":["command"],
            "additionalProperties":false
        }),
    }).collect();
    if private_shell(host)
        && let Some(tool) = tools.first_mut()
    {
        tool.parameters["properties"]["capture"] = json!({"type":"string","enum":["text","private"],"description":"Omit for ordinary text. Private capture retains raw stdout without an initial page, discards stderr and closes stdin; it always returns an execution handle."});
        tool.parameters["properties"]["media_type"] = json!({"type":"string","maxLength":127,"pattern":"^[a-z0-9][a-z0-9!#$&^_.+-]*/[a-z0-9][a-z0-9!#$&^_.+-]*$","description":"Required for private capture only. Declared MIME (use application/octet-stream if unknown); this does not verify the contents."});
        tool.parameters["allOf"] = json!([
            {"if":{"properties":{"capture":{"const":"private"}},"required":["capture"]},"then":{"required":["media_type"]},"else":{"not":{"required":["media_type"]}}}
        ]);
    }
    let (description, mode_description, prompt, rules, mut hints, permissions) = match shell {
        Some((name, description)) => (
            "Universal agent for executing commands on the configured Host.",
            "Execute commands on the configured Host.",
            "Use the advertised Host shell, respect its workspace and capabilities, and inspect execution output before claiming completion.",
            vec![Rule { name: "host_scope".into(), description: "Use the advertised Host shell and keep the working directory within the configured root.".into() }],
            vec![Hint { name: "shell_syntax".into(), description: description.into() }],
            {
                let mut rules = vec![json!({"tool":{"kind":"exact","value":name}, "args":[], "action":"allow"})];
                if private_shell(host) {
                    rules.push(json!({"tool":{"kind":"exact","value":name}, "args":[{"pointer":"/capture","matcher":{"kind":"exact","value":"private"}}], "action":"ask"}));
                }
                serde_json::from_value(Value::Array(rules)).map_err(|e| e.to_string())?
            },
        ),
        None => (
            "Universal conversational agent. Shell execution is unavailable on this Host.",
            "Assist without Host shell execution.",
            "Assist using the available conversation context. Shell execution is unavailable on this Host; do not claim to run commands or inspect command output.",
            vec![],
            vec![Hint { name: "shell_unavailable".into(), description: SHELL_UNAVAILABLE.into() }],
            vec![],
        ),
    };
    let mut variables = Variable::default();
    variables
        .inject("description", description)
        .map_err(|e| e.to_string())?;
    variables
        .inject("rules", rules)
        .map_err(|e| e.to_string())?;
    if let Some(description) = &host.description {
        hints.push(Hint {
            name: "host_environment".into(),
            description: description.clone(),
        });
    }
    variables
        .inject("hints", hints)
        .map_err(|e| e.to_string())?;
    Ok(vec![AgentInfo {
        tool_media_output_bytes: Default::default(),
        name: "universal".into(),
        modes: vec![AgentMode {
            description: mode_description.into(),
            prompt: prompt.into(),
            name: "execute".into(),
            role: AgentRole::Both,
            permissions,
        }],
        tools,
        variables,
    }])
}

/// Independently validate every call; no discovery state survives a fresh store.
pub fn invoke_with_shell(
    host: &HostInfo,
    name: &str,
    arguments: &str,
    shell: impl FnOnce(&str) -> Result<String, String>,
) -> Result<String, String> {
    let (expected, _) = shell_tool(host)?.ok_or(SHELL_UNAVAILABLE)?;
    if name != expected {
        return Err(format!("unknown tool for this Host: {name}"));
    }
    let args: Value = serde_json::from_str(arguments).map_err(|e| e.to_string())?;
    let object = args.as_object().ok_or("arguments must be a JSON object")?;
    if object.keys().any(|key| {
        !matches!(
            key.as_str(),
            "command" | "cwd" | "yield_ms" | "capture" | "media_type"
        )
    }) {
        return Err("unknown shell argument".into());
    }
    let command = object
        .get("command")
        .and_then(Value::as_str)
        .ok_or("command must be a string")?;
    if command.is_empty() || command.contains('\0') {
        return Err("command must be nonempty and contain no NUL".into());
    }
    if let Some(cwd) = object.get("cwd")
        && !cwd
            .as_str()
            .is_some_and(|value| !value.is_empty() && !value.contains('\0'))
    {
        return Err("cwd must be a nonempty string without NUL".into());
    }
    if let Some(value) = object.get("yield_ms")
        && !value.as_u64().is_some_and(|value| value <= 60_000)
    {
        return Err("yield_ms must be 0..=60000 milliseconds".into());
    }
    match (object.get("capture"), object.get("media_type")) {
        (None, None) => {}
        (Some(mode), None) if mode.as_str() == Some("text") && private_shell(host) => {}
        (Some(mode), Some(mime))
            if mode.as_str() == Some("private")
                && private_shell(host)
                && mime
                    .as_str()
                    .is_some_and(microcrab_agent_types::is_capture_mime) => {}
        _ => return Err("private capture requires Host support and a canonical media_type".into()),
    }
    shell(arguments)
}

/// Both ABI paths must describe the same facts; exports keep using their supplied value.
#[cfg(any(target_arch = "wasm32", test))]
fn checked_host_info(supplied: &str, imported: &str) -> Result<HostInfo, String> {
    let supplied: HostInfo = serde_json::from_str(supplied).map_err(|e| e.to_string())?;
    let imported: HostInfo = serde_json::from_str(imported).map_err(|e| e.to_string())?;
    if supplied != imported {
        return Err("host-info import does not match the supplied HostInfo".into());
    }
    Ok(supplied)
}

#[cfg(target_arch = "wasm32")]
mod component {
    include!(concat!(env!("OUT_DIR"), "/guest-bindings.rs"));
    struct Universal;

    /// Preserve the typed Host category without interpreting implementation messages.
    /// Runtime supplies sanitized messages; this guest retains category and retryability.
    fn host_failure(
        error: microcrab::host::types::HostError,
    ) -> microcrab::agent::execution::ToolFailure {
        use microcrab::agent::execution::{FailureKind, ToolFailure};
        use microcrab::host::types::ErrorCategory;
        let (kind, code) = match error.category {
            ErrorCategory::NotFound => (FailureKind::Failed, "host_not_found"),
            ErrorCategory::Permission => (FailureKind::Failed, "host_permission"),
            ErrorCategory::Timeout => (FailureKind::Timeout, "host_timeout"),
            ErrorCategory::Cancelled => (FailureKind::Cancelled, "host_cancelled"),
            ErrorCategory::Conflict => (FailureKind::Failed, "host_conflict"),
            ErrorCategory::Unsupported => (FailureKind::Failed, "host_unsupported"),
            ErrorCategory::Io => (FailureKind::Failed, "host_io"),
        };
        ToolFailure {
            kind,
            code: code.into(),
            message: error.message,
            retryable: error.retryable,
        }
    }

    impl Guest for Universal {
        fn init() -> InitResult {
            InitResult::Ready
        }

        fn describe(host_info: String) -> Result<String, String> {
            let imported =
                microcrab::agent::host_info::host_info().map_err(|error| error.message)?;
            let host = super::checked_host_info(&host_info, &imported)?;
            serde_json::to_string(&super::describe(&host)?).map_err(|e| e.to_string())
        }
        fn invoke(
            host_info: String,
            name: String,
            arguments: String,
        ) -> Result<microcrab::agent::execution::ToolOutput, microcrab::agent::execution::ToolFailure>
        {
            use microcrab::agent::execution::{
                self, BufferedOutput, ContentKind, FailureKind, FinalOutput, OutputChannel,
                ProcessSource, ToolFailure, ToolOutput,
            };
            use microcrab::host::{process, shell};
            let failure = |code: &str, message: String| ToolFailure {
                kind: FailureKind::Failed,
                code: code.into(),
                message,
                retryable: false,
            };
            let imported = microcrab::agent::host_info::host_info()
                .map_err(|error| failure("host_info", error.message))?;
            let host = super::checked_host_info(&host_info, &imported)
                .map_err(|e| failure("host_info", e))?;
            if super::shell_tool(&host)
                .map_err(|e| failure("invalid_argument", e))?
                .is_none()
            {
                return Err(failure(
                    "shell_unavailable",
                    super::SHELL_UNAVAILABLE.into(),
                ));
            }
            super::invoke_with_shell(&host, &name, &arguments, |_| Ok(String::new()))
                .map_err(|e| failure("invalid_argument", e))?;
            let args: serde_json::Value = serde_json::from_str(&arguments)
                .map_err(|e| failure("invalid_argument", e.to_string()))?;
            let request = shell::ShellRequest {
                command: args["command"].as_str().expect("validated command").into(),
                cwd: args
                    .get("cwd")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
            };
            // Select the sink and reserve ownership before any external effect.
            if args.get("capture").and_then(serde_json::Value::as_str) == Some("private") {
                let media_type = args["media_type"].as_str().expect("validated MIME");
                let output = execution::create(&[OutputChannel {
                    name: "stdout".into(),
                    kind: ContentKind::Bytes(media_type.into()),
                }])?;
                execution::launch_private_shell(&output, &request)?;
                return Ok(ToolOutput::Buffered(BufferedOutput {
                    output,
                    include_initial: false,
                }));
            }
            let output = execution::create(&["stdout", "stderr"].map(|name| OutputChannel {
                name: name.into(),
                kind: ContentKind::Text,
            }))?;
            let handle = shell::launch(&request).map_err(|error| {
                let mut reported = host_failure(error);
                if matches!(reported.kind, FailureKind::Timeout) {
                    reported.code = "startup_timeout".into();
                }
                reported
            })?;
            // This observation policy belongs entirely to this guest tool.
            let yield_ms = args
                .get("yield_ms")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(1000);
            let status = process::wait_for_state(handle, yield_ms).map_err(host_failure)?;
            let initial = shell::drain(handle, u64::MAX).map_err(host_failure)?;
            if !matches!(status.state, process::ProcessState::Running) {
                process::release(handle).map_err(host_failure)?;
                let reported = match &status.state {
                    process::ProcessState::Failed(message) => {
                        Some(failure("process_failed", message.clone()))
                    }
                    process::ProcessState::Exited(exit)
                        if exit.code.is_some_and(|code| code != 0) =>
                    {
                        Some(failure(
                            "process_exit",
                            format!(
                                "Process exited with code {}",
                                exit.code.expect("checked exit code")
                            ),
                        ))
                    }
                    _ => None,
                };
                if initial.truncated || initial.stdout.len() + initial.stderr.len() > 64 {
                    if initial.truncated {
                        execution::mark_truncated(&output)?;
                    }
                    execution::append(&output, Some("stdout"), &initial.stdout)?;
                    execution::append(&output, Some("stderr"), &initial.stderr)?;
                    execution::finish(&output, reported.as_ref())?;
                    return Ok(ToolOutput::Buffered(BufferedOutput {
                        output,
                        include_initial: true,
                    }));
                }
                let content = serde_json::to_string(&serde_json::json!({"stdout":initial.stdout,"stderr":initial.stderr,"exit_code":match status.state { process::ProcessState::Exited(exit) => exit.code, _ => None }})).map_err(|e| failure("serialization", e.to_string()))?;
                return Ok(ToolOutput::Final(FinalOutput {
                    content,
                    failure: reported,
                    attachments: vec![],
                }));
            }
            if initial.truncated {
                execution::mark_truncated(&output)?;
            }
            execution::append(&output, Some("stdout"), &initial.stdout)?;
            execution::append(&output, Some("stderr"), &initial.stderr)?;
            let state = serde_json::to_vec(
                &serde_json::json!({"id":handle.id,"pid":handle.pid,"loss_seen":initial.truncated}),
            )
            .map_err(|e| failure("serialization", e.to_string()))?;
            execution::retain_process(&output, handle, ProcessSource::ShellText)?;
            execution::attach_guest(&output, &state)?;
            Ok(ToolOutput::Buffered(BufferedOutput {
                output,
                include_initial: true,
            }))
        }
        fn drive(
            output: &microcrab::agent::execution::Buffer,
            state: Vec<u8>,
            cancelled: bool,
        ) -> Result<microcrab::agent::execution::DriverStep, microcrab::agent::execution::ToolFailure>
        {
            use microcrab::agent::execution::{self, DriverStep, FailureKind, ToolFailure};
            use microcrab::host::{process, shell};
            let fail = |code: &str, message: String| ToolFailure {
                kind: FailureKind::Failed,
                code: code.into(),
                message,
                retryable: false,
            };
            let value: serde_json::Value =
                serde_json::from_slice(&state).map_err(|e| fail("driver_state", e.to_string()))?;
            let id = value["id"]
                .as_u64()
                .ok_or_else(|| fail("driver_state", "Missing process identity".into()))?;
            let pid = serde_json::from_value::<Option<u32>>(
                value
                    .get("pid")
                    .cloned()
                    .ok_or_else(|| fail("driver_state", "Missing process identity".into()))?,
            )
            .map_err(|e| fail("driver_state", e.to_string()))?;
            let handle = process::ProcessHandle { id, pid };
            let mut loss_seen = value["loss_seen"].as_bool().unwrap_or(false);
            if cancelled {
                process::kill_tree(handle, true).map_err(host_failure)?;
                let tail = shell::drain(handle, u64::MAX).map_err(host_failure)?;
                if tail.truncated && !loss_seen {
                    execution::mark_truncated(output)?;
                }
                execution::append(output, Some("stdout"), &tail.stdout)?;
                execution::append(output, Some("stderr"), &tail.stderr)?;
                process::release(handle).map_err(host_failure)?;
                return Ok(DriverStep {
                    state: Vec::new(),
                    wake_after_ms: 0,
                    finished: true,
                });
            }
            let status = process::status(handle).map_err(host_failure)?;
            let tail = shell::drain(handle, u64::MAX).map_err(host_failure)?;
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
                    .map_err(|e| fail("driver_state", e.to_string()))?,
                    wake_after_ms: 20,
                    finished: false,
                });
            }
            process::release(handle).map_err(host_failure)?;
            match status.state {
                process::ProcessState::Failed(message) => Err(fail("process_failed", message)),
                process::ProcessState::Exited(exit) if exit.code.is_some_and(|code| code != 0) => {
                    Err(fail(
                        "process_exit",
                        format!(
                            "Process exited with code {}",
                            exit.code.expect("checked exit code")
                        ),
                    ))
                }
                _ => Ok(DriverStep {
                    state: Vec::new(),
                    wake_after_ms: 0,
                    finished: true,
                }),
            }
        }
    }
    export!(Universal);
}

#[cfg(test)]
mod tests {
    use super::*;
    fn host(os_family: OsFamily) -> HostInfo {
        let (path_separator, newline) = match os_family {
            OsFamily::Windows => ("\\", "\r\n"),
            OsFamily::Unix => ("/", "\n"),
            OsFamily::Other => ("", ""),
        };
        HostInfo {
            os_family,
            os: "fixture".into(),
            arch: "fixture".into(),
            shell: "fixture".into(),
            path_separator: path_separator.into(),
            newline: newline.into(),
            capabilities: vec!["shell".into(), "process".into()],
            resources: None,
            virtual_shell: None,
            description: None,
        }
    }
    #[test]
    fn host_shell_and_environment_are_generic_and_do_not_grant_linkage() {
        let mut host = host(OsFamily::Other);
        host.shell = "custom-shell".into();
        host.description = Some("Host supplied grammar and interpreter limitations: λ.".into());
        let agent = describe(&host).unwrap().remove(0);
        assert_eq!(agent.tools[0].name, "custom-shell");
        let hints = agent
            .variables
            .extract::<Vec<Hint>>("hints")
            .unwrap()
            .unwrap();
        assert!(hints.iter().any(|hint| hint.name == "host_environment"
            && Some(&hint.description) == host.description.as_ref()));
        let arguments = r#"{"command":"custom-command 'λ'"}"#;
        assert_eq!(
            invoke_with_shell(&host, "custom-shell", arguments, |args| Ok(args.into())).unwrap(),
            arguments
        );
        assert!(
            invoke_with_shell(&host, "bash", arguments, |_| panic!(
                "wrong tool reached Host"
            ))
            .is_err()
        );
        host.description = None;
        let hints = describe(&host)
            .unwrap()
            .remove(0)
            .variables
            .extract::<Vec<Hint>>("hints")
            .unwrap()
            .unwrap();
        assert!(!hints.iter().any(|hint| hint.name == "host_environment"));
        for capabilities in [vec!["shell".into()], vec!["process".into()], vec![]] {
            host.capabilities = capabilities;
            host.description = Some("Descriptive text claims all operations are available.".into());
            assert!(describe(&host).unwrap()[0].tools.is_empty());
            assert!(
                invoke_with_shell(&host, "custom-shell", arguments, |_| panic!(
                    "unlinked Host reached"
                ))
                .is_err()
            );
        }
    }

    #[test]
    fn invalid_or_reserved_host_shell_identity_is_rejected() {
        for shell in [
            "",
            "/bin/custom",
            "two words",
            "job",
            "delegate",
            "sleep",
            "expand",
        ] {
            let mut host = host(OsFamily::Other);
            host.shell = shell.into();
            assert!(describe(&host).is_err());
        }
    }

    #[test]
    fn host_catalog_and_values_are_accurate() {
        for (family, name, syntax) in [
            (OsFamily::Windows, "pwsh", "PowerShell"),
            (OsFamily::Unix, "bash", "Bash"),
        ] {
            let agents = describe(&host(family)).unwrap();
            let agent = &agents[0];
            assert_eq!(agent.name, "universal");
            assert_eq!(
                agent.modes,
                [AgentMode {
                    description: "Execute commands on the configured Host.".into(),
                    prompt: "Use the advertised Host shell, respect its workspace and capabilities, and inspect execution output before claiming completion.".into(),
                    name: "execute".into(),
                    role: AgentRole::Both,
                    permissions: serde_json::from_value(json!([
                        {"tool":{"kind":"exact","value":name},"args":[],"action":"allow"}
                    ]))
                    .unwrap(),
                }]
            );
            assert_eq!(agent.tools.len(), 1);
            assert_eq!(agent.tools[0].name, name);
            assert!(agent.tools[0].description.contains(syntax));
            assert!(agent.variables.get("description").unwrap().is_string());
            assert!(agent.variables.get("rules").unwrap().is_array());
            assert!(agent.variables.get("hints").unwrap().is_array());
            assert_eq!(
                invoke_with_shell(&host(family), name, r#"{"command":"echo ok"}"#, |request| {
                    Ok(request.into())
                })
                .unwrap(),
                r#"{"command":"echo ok"}"#
            );
        }
        assert_eq!(
            describe(&host(OsFamily::Other)).unwrap()[0].tools[0].name,
            "fixture"
        );
    }
    #[test]
    fn shell_schema_describes_yield_defaults_and_process_lifecycle() {
        for family in [OsFamily::Windows, OsFamily::Unix] {
            let agents = describe(&host(family)).unwrap();
            let tool = &agents[0].tools[0];
            assert!(tool.description.contains("id=session_id/execution_id"));
            assert!(!tool.description.contains("job"));
            assert!(!tool.description.contains("drain"));
            assert!(!tool.description.contains("job_id"));
            assert!(!tool.description.contains("actual PID"));
            let properties = tool.parameters["properties"].as_object().unwrap();
            assert!(!properties.contains_key("timeout_ms"));
            for (name, property) in properties {
                let description = property["description"].as_str().unwrap();
                assert!(
                    description.len() >= 20,
                    "missing useful description: {name}"
                );
                if name != "command" {
                    assert!(description.contains("Omission"), "missing default: {name}");
                    assert!(
                        description.contains("null"),
                        "missing null semantics: {name}"
                    );
                }
            }
            let yield_ms = &properties["yield_ms"];
            assert_eq!(yield_ms["default"], json!(1000));
            assert_eq!(yield_ms["minimum"], json!(0));
            assert_eq!(yield_ms["maximum"], json!(60000));
            assert!(
                yield_ms["description"]
                    .as_str()
                    .unwrap()
                    .contains("does not terminate or restart")
            );
        }
    }
    #[test]
    fn private_capture_is_host_gated_and_asks_without_changing_text_default() {
        let mut host = host(OsFamily::Unix);
        let private =
            r#"{"command":"produce","capture":"private","media_type":"application/octet-stream"}"#;
        assert!(invoke_with_shell(&host, "bash", private, |_| panic!("must not launch")).is_err());
        host.capabilities.push("shell.launch-private".into());
        let agents = describe(&host).unwrap();
        let tool = &agents[0].tools[0];
        assert!(tool.parameters["properties"]["capture"].is_object());
        let rules = serde_json::to_value(&agents[0].modes[0].permissions).unwrap();
        assert_eq!(rules[0]["action"], "allow");
        assert_eq!(rules[1]["action"], "ask");
        assert_eq!(rules[1]["args"][0]["matcher"]["value"], "private");
        assert!(invoke_with_shell(&host, "bash", private, |_| Ok("launch".into())).is_ok());
        for args in [
            r#"{"command":"produce","capture":"private"}"#,
            r#"{"command":"produce","media_type":"image/png"}"#,
            r#"{"command":"produce","capture":"private","media_type":"image/PNG"}"#,
            r#"{"command":"produce","capture":"private","media_type":"image/png;foo=bar"}"#,
        ] {
            assert!(invoke_with_shell(&host, "bash", args, |_| panic!("must not launch")).is_err());
        }
    }

    #[test]
    fn missing_shell_ports_degrade_catalog_and_never_invoke_host() {
        for (family, name) in [
            (OsFamily::Windows, "pwsh"),
            (OsFamily::Unix, "bash"),
            (OsFamily::Other, "fixture"),
        ] {
            for capabilities in [
                vec![],
                vec!["host-info".into(), "variable-get".into()],
                vec!["shell".into()],
                vec!["process".into()],
            ] {
                let mut host = host(family);
                host.capabilities = capabilities;
                host.description = Some("Opaque Host environment facts: λ.".into());
                let agent = describe(&host).unwrap().remove(0);
                assert_eq!(agent.name, "universal");
                assert!(agent.tools.is_empty());
                assert_eq!(agent.modes[0].name, "execute");
                assert_eq!(agent.modes[0].role, AgentRole::Both);
                assert!(agent.modes[0].permissions.is_empty());
                assert!(
                    agent.modes[0]
                        .prompt
                        .contains("Shell execution is unavailable")
                );
                assert!(
                    !agent.modes[0]
                        .prompt
                        .contains("Use the advertised Host shell")
                );
                let rules: Vec<Rule> = agent.variables.extract("rules").unwrap().unwrap();
                assert!(rules.is_empty());
                let hints: Vec<Hint> = agent.variables.extract("hints").unwrap().unwrap();
                assert!(hints.iter().any(|hint| hint.name == "shell_unavailable"));
                assert!(!hints.iter().any(|hint| hint.name == "shell_syntax"));
                assert!(hints.iter().any(|hint| hint.name == "host_environment"
                    && Some(&hint.description) == host.description.as_ref()));
                assert!(
                    agent
                        .variables
                        .get("description")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .contains("Shell execution is unavailable")
                );
                for arguments in [r#"{"command":"ok"}"#, "not JSON"] {
                    assert_eq!(
                        invoke_with_shell(&host, name, arguments, |_| {
                            panic!("unlinked shell reached Host")
                        })
                        .unwrap_err(),
                        SHELL_UNAVAILABLE,
                    );
                }
            }
        }
    }

    #[test]
    fn unavailable_shell_does_not_require_a_shell_identity() {
        let mut host = host(OsFamily::Other);
        host.shell.clear();
        host.capabilities.clear();
        assert!(describe(&host).unwrap()[0].tools.is_empty());
        assert_eq!(
            invoke_with_shell(&host, "bash", "{}", |_| {
                panic!("unlinked shell reached Host")
            })
            .unwrap_err(),
            SHELL_UNAVAILABLE
        );
    }

    #[test]
    fn extended_host_info_decodes_and_checks_both_abi_paths() {
        for family in [OsFamily::Windows, OsFamily::Unix] {
            let expected = host(family);
            let supplied = serde_json::to_string(&expected).unwrap();
            let imported = serde_json::to_string_pretty(&expected).unwrap();
            assert_eq!(checked_host_info(&supplied, &imported).unwrap(), expected);
            assert_eq!(describe(&expected).unwrap()[0].tools.len(), 1);

            for field in [
                "path_separator",
                "newline",
                "capabilities",
                "shell",
                "description",
            ] {
                let mut changed = serde_json::to_value(&expected).unwrap();
                changed[field] = if field == "capabilities" {
                    json!([])
                } else {
                    json!("different")
                };
                assert!(checked_host_info(&supplied, &changed.to_string()).is_err());
            }
            assert!(checked_host_info(&supplied, "not JSON").is_err());
            assert!(checked_host_info("not JSON", &imported).is_err());
        }
    }

    #[test]
    fn session_variables_are_not_shell_parameters() {
        let host = host(OsFamily::Unix);
        let agent = describe(&host).unwrap().remove(0);
        let parameters = &agent.tools[0].parameters;
        let properties = parameters["properties"].as_object().unwrap();
        assert_eq!(properties.len(), 3);
        for key in ["command", "cwd", "yield_ms"] {
            assert!(
                properties.contains_key(key),
                "missing shell parameter: {key}"
            );
        }
        assert_eq!(parameters["additionalProperties"], false);
        for arguments in [
            r#"{"command":"ok","variables":["example"]}"#,
            r#"{"command":"ok","env":{"example":"value"}}"#,
        ] {
            assert_eq!(
                invoke_with_shell(&host, "bash", arguments, |_| {
                    panic!("unexpected shell parameter reached Host")
                })
                .unwrap_err(),
                "unknown shell argument"
            );
        }
    }

    #[test]
    fn wrong_host_name_and_exact_arguments_never_reach_capability() {
        for (family, name, args) in [
            (OsFamily::Windows, "bash", r#"{"command":"ok"}"#),
            (OsFamily::Unix, "pwsh", r#"{"command":"ok"}"#),
            (OsFamily::Other, "bash", r#"{"command":"ok"}"#),
            (OsFamily::Unix, "shell", r#"{"command":"ok"}"#),
            (OsFamily::Unix, "bash", "{}"),
            (OsFamily::Unix, "bash", r#"{"command":""}"#),
            (OsFamily::Unix, "bash", r#"{"command":"ok","timeout_ms":0}"#),
            (
                OsFamily::Unix,
                "bash",
                r#"{"command":"ok","timeout_ms":1000}"#,
            ),
            (
                OsFamily::Unix,
                "bash",
                r#"{"command":"ok","timeout_ms":1.5}"#,
            ),
            (OsFamily::Unix, "bash", r#"{"command":"ok","extra":1}"#),
            (OsFamily::Unix, "bash", r#"{"command":"ok","cwd":null}"#),
        ] {
            assert!(
                invoke_with_shell(&host(family), name, args, |_| panic!(
                    "invalid request reached Host"
                ))
                .is_err()
            );
        }
    }
}
