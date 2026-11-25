use super::*;
use microcrab_agent_types::{AgentRole, HostInfo, OsFamily};
use microcrab_permission::{Action, PermissionConfig, PermissionRequest, PolicySnapshot};
use serde_json::{Value, json};

#[test]
fn host_shell_catalog_routes_opaque_implementation_and_preserves_mode_policy() {
    let mut host = info(OsFamily::Other);
    host.shell = "custom-shell".into();
    host.description = Some("Host supplied grammar and interpreter limitations: λ.".into());
    let agent = describe(&host).unwrap().remove(0);
    let shell = agent
        .tools
        .iter()
        .find(|tool| tool.name == "custom-shell")
        .unwrap();
    assert!(shell.description.contains("configured Host shell"));
    let hints = agent
        .variables
        .extract::<Vec<microcrab_agent_types::Hint>>("hints")
        .unwrap()
        .unwrap();
    assert!(hints.iter().any(|hint| hint.name == "host_environment"
        && Some(&hint.description) == host.description.as_ref()));
    for mode in &agent.modes {
        let rule = mode
            .permissions
            .iter()
            .find(|rule| {
                rule.tool == Some(microcrab_permission::Matcher::Exact("custom-shell".into()))
            })
            .unwrap();
        assert_eq!(
            rule.action,
            if mode.name == "plan" {
                Action::Deny
            } else {
                Action::Ask
            }
        );
        assert!(rule.args.is_empty());
        assert_eq!(mode.permissions.len(), agent.tools.len());
    }
    let mut memory = MemoryHost::default();
    let command = "custom-command 'λ'";
    invoke(
        &host,
        "custom-shell",
        &json!({"command":command,"yield_ms":0}).to_string(),
        &mut memory,
    )
    .unwrap();
    assert_eq!(memory.shells[0].command, command);
    assert!(invoke(&host, "bash", r#"{"command":"echo ok"}"#, &mut memory).is_err());
    host.description = None;
    let agent = describe(&host).unwrap().remove(0);
    assert_eq!(
        agent
            .tools
            .iter()
            .filter(|tool| tool.name == "custom-shell")
            .count(),
        1
    );
    assert!(
        !agent
            .variables
            .extract::<Vec<microcrab_agent_types::Hint>>("hints")
            .unwrap()
            .unwrap()
            .iter()
            .any(|hint| hint.name == "host_environment")
    );
    host.capabilities
        .retain(|capability| capability != "process");
    assert!(
        !describe(&host).unwrap()[0]
            .tools
            .iter()
            .any(|tool| tool.name == "custom-shell")
    );
    assert!(invoke(&host, "custom-shell", r#"{"command":"ok"}"#, &mut memory).is_err());
}

#[test]
fn host_shell_cannot_shadow_tools_or_advertise_invalid_identity() {
    for name in [
        "read",
        "write",
        "apply_patch",
        "switch_mode",
        "job",
        "delegate",
        "sleep",
        "expand",
        "",
        "/bin/custom",
        "two words",
    ] {
        let mut host = info(OsFamily::Other);
        host.shell = name.into();
        let agent = describe(&host).unwrap().remove(0);
        assert_eq!(
            agent
                .tools
                .iter()
                .filter(|tool| tool.name == "read")
                .count(),
            1
        );
        assert!(
            agent
                .tools
                .iter()
                .all(|tool| tool.parameters["properties"].get("command").is_none())
        );
    }
}

#[test]
fn catalog_integer_limits_are_portable_without_losing_business_bounds() {
    let agent = describe(&info(OsFamily::Windows)).unwrap().remove(0);
    let read = agent.tools.iter().find(|tool| tool.name == "read").unwrap();
    let properties = &read.parameters["properties"];
    assert_eq!(properties["offset_line"]["minimum"], json!(1));
    assert!(properties["offset_line"].get("maximum").is_none());
    assert_eq!(properties["max_lines"]["maximum"], json!(10000));
    assert_eq!(properties["max_bytes"]["maximum"], json!(1048576));
    assert!(
        !serde_json::to_string(&agent.tools)
            .unwrap()
            .contains(&u64::MAX.to_string())
    );
}

#[test]
fn catalog_describes_nested_parameters_and_optional_defaults() {
    fn check(schema: &Value, location: &str) {
        if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
            for (name, property) in properties {
                let location = format!("{location}.{name}");
                let description = property["description"].as_str().unwrap_or("");
                assert!(
                    description.len() >= 20,
                    "missing useful description: {location}"
                );
                let required = schema["required"].as_array().is_some_and(|required| {
                    required
                        .iter()
                        .any(|item| item.as_str() == Some(name.as_str()))
                });
                if !required {
                    assert!(
                        description.contains("Omission"),
                        "missing default: {location}"
                    );
                    assert!(
                        description.contains("null"),
                        "missing null semantics: {location}"
                    );
                }
                check(property, &location);
            }
        }
        if let Some(items) = schema.get("items") {
            assert!(
                items["description"]
                    .as_str()
                    .is_some_and(|text| text.len() >= 20),
                "missing item description: {location}"
            );
            check(items, &format!("{location}[]"));
        }
    }
    for family in [OsFamily::Windows, OsFamily::Unix] {
        let agent = describe(&info(family)).unwrap().remove(0);
        for tool in &agent.tools {
            check(&tool.parameters, &tool.name);
        }
        let name = if family == OsFamily::Windows {
            "pwsh"
        } else {
            "bash"
        };
        let shell = agent.tools.iter().find(|tool| tool.name == name).unwrap();
        assert!(shell.parameters["properties"].get("timeout_ms").is_none());
        assert_eq!(
            shell.parameters["properties"]["yield_ms"]["default"],
            json!(1000)
        );
        assert_eq!(
            shell.parameters["properties"]["yield_ms"]["maximum"],
            json!(60000)
        );
        assert!(shell.description.contains("id=session_id/execution_id"));
        assert!(!shell.description.contains("job"));
        assert!(!shell.description.contains("drain"));
        assert!(!shell.description.contains("job_id"));
    }
}

fn version() -> String {
    "a".repeat(64)
}
fn info(family: OsFamily) -> HostInfo {
    HostInfo {
        os_family: family,
        os: "fixture".into(),
        arch: "fixture".into(),
        shell: "fixture".into(),
        path_separator: "/".into(),
        newline: "\n".into(),
        capabilities: vec![
            "read".into(),
            "file_mutation".into(),
            "write".into(),
            "glob".into(),
            "grep".into(),
            "shell".into(),
            "process".into(),
        ],
        resources: None,
        virtual_shell: None,
        description: None,
    }
}
#[derive(Default)]
struct MemoryHost {
    preflights: Vec<Vec<FileMutation>>,
    mutations: Vec<FileMutation>,
    fail_mutation: Option<(usize, OperationErrorCode)>,
    partial_move: bool,
    captures: Vec<(String, u64, String)>,
    fail_capture: Option<usize>,
    content: String,
    reads: Vec<ReadRequest>,
    writes: Vec<WriteRequest>,
    searches: Vec<GrepRequest>,
    globs: Vec<GlobRequest>,
    glob_entries: Option<Vec<GlobEntry>>,
    shells: Vec<ShellRequest>,
    waits: Vec<(ProcessHandle, u64)>,
    output_reads: Vec<(ProcessHandle, u64)>,
    failed: bool,
    truncated: bool,
    race: bool,
    started: bool,
}
impl Host for MemoryHost {
    fn preflight_mutations(
        &mut self,
        requests: Vec<FileMutation>,
    ) -> Result<Vec<FileMutation>, OperationError> {
        self.preflights.push(requests.clone());
        let mut paths = std::collections::BTreeSet::new();
        for request in &requests {
            for path in request.paths() {
                let path = path.trim_start_matches("./").to_ascii_lowercase();
                if path.contains("..") || path.starts_with('/') || !paths.insert(path) {
                    return Err(OperationError {
                        code: OperationErrorCode::InvalidRequest,
                        message: "alias/collision/path escape".into(),
                    });
                }
            }
        }
        Ok(requests)
    }
    fn mutate(&mut self, request: FileMutation) -> Result<MutationOutput, OperationError> {
        self.mutations.push(request.clone());
        if let Some((index, code)) = &self.fail_mutation
            && *index == self.mutations.len()
        {
            return Err(OperationError {
                code: code.clone(),
                message: "injected later operation failure".into(),
            });
        }
        let (content, source_removed, destination_written) = match request {
            FileMutation::Write(r) => (Some(r.content), false, true),
            FileMutation::Delete(_) => (None, true, false),
            FileMutation::Move(r) => (Some(r.content), !self.partial_move, true),
        };
        Ok(MutationOutput {
            bytes: content.as_ref().map_or(0, |s| s.len() as u64),
            version: content.map(|_| "b".repeat(64)),
            source_removed,
            destination_written,
            diagnostic: self
                .partial_move
                .then(|| "destination committed; source retained".into()),
        })
    }
    fn capture(&mut self, path: &str, max_bytes: u64) -> Result<(), OperationError> {
        self.captures
            .push((path.into(), max_bytes, self.content.clone()));
        if self.fail_capture == Some(self.captures.len()) {
            return Err(OperationError {
                code: OperationErrorCode::Limit,
                message: "cache-point capacity exceeded".into(),
            });
        }
        Ok(())
    }
    fn read(&mut self, r: ReadRequest) -> Result<ReadOutput, OperationError> {
        let start = r.offset_line;
        self.reads.push(r);
        Ok(ReadOutput {
            content: self.content.clone(),
            start_line: start,
            next_line: self.truncated.then_some(start + 1),
            truncated: self.truncated,
            version: version(),
        })
    }
    fn write(&mut self, r: WriteRequest) -> Result<WriteOutput, OperationError> {
        self.writes.push(r.clone());
        if self.race {
            return Err(OperationError {
                code: OperationErrorCode::Conflict,
                message: "concurrent edit".into(),
            });
        }
        self.content = r.content;
        Ok(WriteOutput {
            version: "b".repeat(64),
            bytes: self.content.len() as u64,
        })
    }
    fn glob(&mut self, r: GlobRequest) -> Result<GlobOutput, OperationError> {
        let include_directories = r.include_directories;
        self.globs.push(r);
        let mut entries = vec![GlobEntry {
            path: "src/main.rs".into(),
            kind: GlobEntryKind::File,
        }];
        if include_directories {
            entries.push(GlobEntry {
                path: "src".into(),
                kind: GlobEntryKind::Directory,
            });
            entries.push(GlobEntry {
                path: "linked".into(),
                kind: GlobEntryKind::Symlink,
            });
        }
        Ok(GlobOutput {
            entries: self.glob_entries.clone().unwrap_or(entries),
            truncated: true,
        })
    }
    fn grep(&mut self, r: GrepRequest) -> Result<GrepOutput, OperationError> {
        self.searches.push(r);
        Ok(GrepOutput {
            matches: vec![GrepMatch {
                path: "src/main.rs".into(),
                line: 7,
                text: "needle".into(),
            }],
            truncated: true,
        })
    }
    fn shell_launch(&mut self, r: ShellRequest) -> Result<ProcessHandle, OperationError> {
        self.shells.push(r);
        Ok(ProcessHandle {
            id: 17,
            pid: Some(if self.started { 55 } else { 54 }),
        })
    }
    fn process_wait_for_state(
        &mut self,
        handle: ProcessHandle,
        yield_ms: u64,
    ) -> Result<ProcessState, OperationError> {
        self.waits.push((handle, yield_ms));
        Ok(if self.failed {
            ProcessState::Failed("backend process failure".into())
        } else if self.started {
            ProcessState::Running
        } else {
            ProcessState::Exited { code: Some(0) }
        })
    }
    fn shell_read(
        &mut self,
        handle: ProcessHandle,
        max_bytes: u64,
    ) -> Result<ShellOutput, OperationError> {
        self.output_reads.push((handle, max_bytes));
        Ok(ShellOutput {
            stdout: if self.started { "partial stdout" } else { "ok" }.into(),
            stderr: if self.started { "partial stderr" } else { "" }.into(),
            truncated: self.truncated,
        })
    }
}
fn call(host: &mut MemoryHost, name: &str, args: Value) -> Result<Value, String> {
    let output = invoke(&info(OsFamily::Unix), name, &args.to_string(), host)?;
    serde_json::from_str(&output.content).map_err(|e| e.to_string())
}
#[test]
fn read_renders_lines_and_preserves_version_and_truncation() {
    let mut host = MemoryHost {
        content: "α\r\nβ\nlast".into(),
        truncated: true,
        ..MemoryHost::default()
    };
    let output = call(
        &mut host,
        "read",
        json!({"path":"a","offset_line":4,"max_lines":3,"max_bytes":64}),
    )
    .unwrap();
    assert_eq!(output["content"], "4\tα\r\n5\tβ\n6\tlast");
    assert_eq!(output["version"], version());
    assert_eq!(output["next_line"], 5);
    assert_eq!(output["truncated"], true);
    assert_eq!(host.reads[0].max_bytes, 64);
    host.content.clear();
    assert_eq!(
        call(&mut host, "read", json!({"path":"a"})).unwrap()["content"],
        ""
    );
}
#[test]
fn invalid_args_never_reach_any_host_operation() {
    let mut host = MemoryHost::default();
    for (name, args) in [
        ("read", json!({"path":"a","offset_line":0})),
        ("read", json!({"path":"a","max_bytes":1048577})),
        ("read", json!({"path":"a","extra":true})),
        ("read", json!({"path":"a","max_lines":1.5})),
        ("read", json!({"path":"a\0b"})),
        (
            "write",
            json!({"path":"a","content":"x","expected_version":null}),
        ),
        (
            "edit",
            json!({"path":"a","expected_version":version(),"old_string":"","new_string":"b"}),
        ),
        (
            "patch",
            json!({"path":"a","expected_version":version(),"replacements":[]}),
        ),
        ("grep", json!({"root":".","pattern":"x","literal":"true"})),
        (
            "grep",
            json!({"root":".","pattern":"x","include_glob":null}),
        ),
        ("glob", json!({"root":".","pattern":"*","include_hidden":1})),
        ("bash", json!({"command":"ok","yield_ms":60001})),
        ("bash", json!({"command":"ok","timeout_ms":0})),
        ("bash", json!({"command":"ok","timeout_ms":1000})),
        ("bash", json!({"command":"ok","cwd":null})),
        ("pwsh", json!({"command":"ok"})),
    ] {
        assert!(call(&mut host, name, args).is_err(), "accepted {name}");
    }
    for args in ["null", "[]", "{bad", "{\"path\":\"a\",\"path\":\"b\"}"] {
        assert!(invoke(&info(OsFamily::Unix), "read", args, &mut host).is_err());
    }
    assert!(
        host.captures.is_empty()
            && host.reads.is_empty()
            && host.writes.is_empty()
            && host.searches.is_empty()
            && host.globs.is_empty()
            && host.shells.is_empty()
    );
}
#[test]
fn edit_and_patch_publish_once_only_after_all_replacements_validate() {
    let mut host = MemoryHost {
        content: "one two three".into(),
        ..MemoryHost::default()
    };
    call(&mut host,"patch",json!({"path":"a","expected_version":version(),"replacements":[{"old_string":"one","new_string":"ONE"},{"old_string":"two","new_string":"TWO"}]})).unwrap();
    assert_eq!(host.content, "ONE TWO three");
    assert_eq!(host.writes.len(), 1);
    assert_eq!(
        host.writes[0].precondition,
        WritePrecondition::Version(version())
    );
    assert_eq!(host.reads[0].offset_line, 1);
    assert_eq!(host.reads[0].max_bytes, 1048576);
    let mut host = MemoryHost {
        content: "one two".into(),
        ..MemoryHost::default()
    };
    assert!(call(&mut host,"patch",json!({"path":"a","expected_version":version(),"replacements":[{"old_string":"one","new_string":"ONE"},{"old_string":"missing","new_string":"oops"}]})).is_err());
    assert_eq!(host.content, "one two");
    assert!(host.writes.is_empty());
    for content in ["x x", "aaa", "no match"] {
        host.content = content.into();
        assert!(call(&mut host,"edit",json!({"path":"a","expected_version":version(),"old_string":if content=="aaa" {"aa"} else {"x"},"new_string":"new"})).is_err());
    }
    assert!(host.writes.is_empty());
}
#[test]
fn stale_truncated_and_racing_edits_preserve_the_file() {
    let mut host = MemoryHost {
        content: "one".into(),
        ..MemoryHost::default()
    };
    let args =
        json!({"path":"a","expected_version":version(),"old_string":"one","new_string":"two"});
    let mut stale = args.clone();
    stale["expected_version"] = json!("c".repeat(64));
    let error: Value = serde_json::from_str(&call(&mut host, "edit", stale).unwrap_err()).unwrap();
    assert_eq!(error["code"], "conflict");
    host.truncated = true;
    assert!(call(&mut host, "edit", args.clone()).is_err());
    assert!(host.writes.is_empty());
    host.truncated = false;
    host.race = true;
    let error: Value = serde_json::from_str(&call(&mut host, "edit", args).unwrap_err()).unwrap();
    assert_eq!(error["code"], "conflict");
    assert_eq!(host.content, "one");
    assert_eq!(host.writes.len(), 1);
}
#[test]
fn controlled_writes_capture_before_and_after_without_changing_tool_output() {
    let mut host = MemoryHost {
        content: "before".into(),
        ..MemoryHost::default()
    };
    let output = call(&mut host, "write", json!({"path":"a","content":"after"})).unwrap();
    assert_eq!(output, json!({"version":"b".repeat(64),"bytes":5}));
    assert_eq!(
        host.captures,
        vec![
            ("a".into(), 1048576, "before".into()),
            ("a".into(), 1048576, "after".into())
        ]
    );
    host.captures.clear();
    call(&mut host, "patch", json!({"path":"a","expected_version":version(),"replacements":[{"old_string":"after","new_string":"patched"}]})).unwrap();
    assert_eq!(host.captures[0].2, "after");
    assert_eq!(host.captures[1].2, "patched");
}

#[test]
fn capture_and_mutation_failures_retain_only_completed_points() {
    let args = json!({"path":"a","content":"after"});
    let mut before_failure = MemoryHost {
        content: "before".into(),
        fail_capture: Some(1),
        ..MemoryHost::default()
    };
    assert!(call(&mut before_failure, "write", args.clone()).is_err());
    assert!(before_failure.writes.is_empty());
    assert_eq!(before_failure.content, "before");

    let mut write_failure = MemoryHost {
        content: "before".into(),
        race: true,
        ..MemoryHost::default()
    };
    assert!(call(&mut write_failure, "write", args.clone()).is_err());
    assert_eq!(write_failure.captures.len(), 1);
    assert_eq!(write_failure.captures[0].2, "before");
    assert_eq!(write_failure.content, "before");

    let mut after_failure = MemoryHost {
        content: "before".into(),
        fail_capture: Some(2),
        ..MemoryHost::default()
    };
    let error: Value =
        serde_json::from_str(&call(&mut after_failure, "write", args).unwrap_err()).unwrap();
    assert_eq!(error["code"], "limit");
    assert!(
        error["message"]
            .as_str()
            .unwrap()
            .contains("write succeeded")
    );
    assert_eq!(after_failure.writes.len(), 1);
    assert_eq!(after_failure.content, "after");
}

#[test]
fn create_and_replace_use_distinct_preconditions() {
    let mut host = MemoryHost::default();
    call(&mut host, "write", json!({"path":"a","content":"new"})).unwrap();
    assert_eq!(host.writes[0].precondition, WritePrecondition::Missing);
    call(
        &mut host,
        "write",
        json!({"path":"a","content":"","expected_version":version()}),
    )
    .unwrap();
    assert_eq!(
        host.writes[1].precondition,
        WritePrecondition::Version(version())
    );
}
#[test]
fn search_and_shell_render_bounded_host_results_and_existing_receipts() {
    let mut host = MemoryHost::default();
    let output = call(
        &mut host,
        "grep",
        json!({"root":".","pattern":"needle","literal":true}),
    )
    .unwrap();
    assert_eq!(output["content"], "src/main.rs:7:needle");
    assert_eq!(output["truncated"], true);
    assert!(host.searches[0].case_sensitive && host.searches[0].literal);
    assert_eq!(
        call(&mut host, "glob", json!({"root":".","pattern":"*.rs"})).unwrap()["truncated"],
        true
    );
    assert!(!host.globs[0].include_hidden && !host.globs[0].include_ignored);
    assert_eq!(
        call(&mut host, "bash", json!({"command":"echo ok"})).unwrap()["exit_code"],
        0
    );
    assert_eq!(host.waits[0].1, 1000);
    let completed = call(&mut host, "bash", json!({"command":"echo ok"})).unwrap();
    assert_eq!(completed["run_in_background"], false);
    assert_eq!(completed["pid"], 54);
    assert_eq!(completed["job_id"], 17);
    assert_eq!(completed["state"], "exited");
    assert!(completed.get("session_id").is_none());
    assert!(completed.get("id").is_none());
    assert_eq!(host.output_reads[0].0, host.waits[0].0);
    assert_eq!(host.output_reads[0].1, 65536);
    host.truncated = true;
    let oversized = call(&mut host, "bash", json!({"command":"large"})).unwrap();
    assert_eq!(oversized["job_id"], 17);
    assert_eq!(oversized["truncated"], true);
    assert_eq!(oversized["run_in_background"], false);
    host.truncated = false;
    host.failed = true;
    let failed = call(&mut host, "bash", json!({"command":"failed"})).unwrap();
    assert_eq!(failed["state"], "failed");
    assert_eq!(failed["run_in_background"], false);
    assert_eq!(failed["failure"], "backend process failure");
    assert!(failed.get("exit_code").is_none());
    host.failed = false;
    host.started = true;
    let output = call(&mut host, "bash", json!({"command":"long","yield_ms":0})).unwrap();
    assert_eq!(host.waits.last().unwrap().1, 0);
    assert!(output.get("exit_code").is_none());
    assert_eq!(
        output,
        json!({"run_in_background":true,"job_id":17,"pid":55,"state":"running","stdout":"partial stdout","stderr":"partial stderr","truncated":false})
    );
}
#[test]
fn glob_directory_opt_in_is_typed_strict_and_available_in_plan_mode() {
    let agent = describe(&info(OsFamily::Windows)).unwrap().remove(0);
    let tool = agent.tools.iter().find(|tool| tool.name == "glob").unwrap();
    assert_eq!(
        tool.parameters["properties"]["include_directories"]["type"],
        "boolean"
    );
    assert_eq!(
        tool.parameters["properties"]["include_directories"]["default"],
        false
    );
    assert!(
        !tool.parameters["required"]
            .as_array()
            .unwrap()
            .contains(&json!("include_directories"))
    );
    let plan = agent.modes.iter().find(|mode| mode.name == "plan").unwrap();
    assert!(plan.permissions.iter().any(|permission| permission.tool
        == Some(microcrab_permission::Matcher::Exact("glob".into()))
        && permission.action == Action::Allow));
    let mut host = MemoryHost::default();
    let output = call(&mut host, "glob", json!({"root":".","pattern":"*"})).unwrap();
    assert!(!host.globs[0].include_directories);
    assert_eq!(
        output["groups"],
        json!([{"kind":"file","path":["src/main.rs"]}])
    );
    assert!(output.get("paths").is_none());
    let output = call(
        &mut host,
        "glob",
        json!({"root":".","pattern":"*","include_directories":true}),
    )
    .unwrap();
    assert!(host.globs[1].include_directories);
    assert_eq!(
        output["groups"],
        json!([
            {"kind":"file","path":["src/main.rs"]},
            {"kind":"directory","path":["src"]},
            {"kind":"symlink","path":["linked"]},
        ])
    );
    for value in [json!(null), json!(1), json!("true"), json!([])] {
        assert!(
            call(
                &mut host,
                "glob",
                json!({"root":".","pattern":"*","include_directories":value})
            )
            .is_err()
        );
    }
    assert_eq!(
        host.globs.len(),
        2,
        "invalid flags must fail before Host IO"
    );
}

#[test]
fn glob_groups_preserve_paths_order_and_truncation_without_per_path_kind_repetition() {
    let entries = vec![
        GlobEntry {
            path: "目录".into(),
            kind: GlobEntryKind::Directory,
        },
        GlobEntry {
            path: "src/β.rs".into(),
            kind: GlobEntryKind::File,
        },
        GlobEntry {
            path: "link".into(),
            kind: GlobEntryKind::Symlink,
        },
        GlobEntry {
            path: "src/a.rs".into(),
            kind: GlobEntryKind::File,
        },
        GlobEntry {
            path: "目录/子目录".into(),
            kind: GlobEntryKind::Directory,
        },
        GlobEntry {
            path: "src/back\\slash\"quote.rs".into(),
            kind: GlobEntryKind::File,
        },
    ];
    let host_shape = serde_json::to_value(GlobOutput {
        entries: entries.clone(),
        truncated: true,
    })
    .unwrap();
    let mut host = MemoryHost {
        glob_entries: Some(entries.clone()),
        ..Default::default()
    };
    let output = call(
        &mut host,
        "glob",
        json!({"root":".","pattern":"**","include_directories":true}),
    )
    .unwrap();
    assert_eq!(
        output,
        json!({"groups":[
        {"kind":"file","path":["src/β.rs","src/a.rs","src/back\\slash\"quote.rs"]},
        {"kind":"directory","path":["目录","目录/子目录"]},
        {"kind":"symlink","path":["link"]}
    ],"truncated":true})
    );
    assert_eq!(host.glob_entries.as_ref(), Some(&entries));
    assert_eq!(host_shape["entries"].as_array().unwrap().len(), 6);
    assert!(output.get("entries").is_none());
    assert!(
        serde_json::to_string(&output).unwrap().len()
            < serde_json::to_string(&host_shape).unwrap().len()
    );
    host.glob_entries = Some(vec![]);
    let empty = call(&mut host, "glob", json!({"root":".","pattern":"nothing"})).unwrap();
    assert_eq!(empty, json!({"groups":[],"truncated":true}));
}

#[test]
fn shell_requires_both_execution_and_job_supervision_capabilities() {
    for (family, name) in [(OsFamily::Windows, "pwsh"), (OsFamily::Unix, "bash")] {
        for capabilities in [vec![], vec!["shell".into()], vec!["process".into()]] {
            let mut host_info = info(family);
            host_info.capabilities = capabilities;
            let agent = describe(&host_info).unwrap().remove(0);
            assert!(
                !agent
                    .tools
                    .iter()
                    .any(|tool| matches!(tool.name.as_str(), "pwsh" | "bash"))
            );
            let mut host = MemoryHost::default();
            assert!(invoke(&host_info, name, r#"{"command":"echo ok"}"#, &mut host).is_err());
            assert!(host.shells.is_empty());
        }
    }
}

#[test]
fn catalog_modes_are_recommendations_for_runtime_not_guest_decisions() {
    for (family, shell) in [(OsFamily::Unix, "bash"), (OsFamily::Windows, "pwsh")] {
        let agent = describe(&info(family)).unwrap().remove(0);
        assert_eq!(agent.name, "coding");
        assert_eq!(
            agent
                .modes
                .iter()
                .map(|m| m.name.as_str())
                .collect::<Vec<_>>(),
            ["plan", "build"]
        );
        assert_eq!(agent.modes[0].role, AgentRole::MainAgent);
        assert_eq!(agent.modes[1].role, AgentRole::Both);
        assert!(agent.tools.iter().any(|t| t.name == shell));
        assert_eq!(agent.tools.len(), 8);
        for mode in agent.modes {
            let config = PermissionConfig {
                agent: agent.name.clone(),
                mode: mode.name.clone(),
                defaults: mode.permissions,
                ..PermissionConfig::default()
            };
            for tool in agent.tools.iter().map(|t| t.name.as_str()).chain([
                "dangerous_plugin",
                if shell == "bash" { "pwsh" } else { "bash" },
            ]) {
                let request = PermissionRequest {
                    subject: Default::default(),
                    session_id: "s".into(),
                    call_id: "c".into(),
                    agent: "coding".into(),
                    mode: mode.name.clone(),
                    tool: tool.into(),
                    arguments: json!({}),
                    revision: 1,
                };
                let expected = if matches!(tool, "read" | "glob" | "grep") {
                    Action::Allow
                } else if agent.tools.iter().any(|t| t.name == tool) && mode.name == "build" {
                    Action::Ask
                } else {
                    Action::Deny
                };
                assert_eq!(
                    microcrab_permission::evaluate(
                        &PolicySnapshot {
                            revision: 1,
                            config: config.clone()
                        },
                        &request
                    )
                    .unwrap()
                    .action,
                    expected
                );
            }
            // Exact user overrides supersede mode defaults without changing the catalog.
            let mut override_config = config.clone();
            override_config
                .overrides
                .push(microcrab_permission::Permission {
                    agent: None,
                    mode: None,
                    tool: Some(microcrab_permission::Matcher::Exact(shell.into())),
                    args: vec![],
                    action: Action::Allow,
                });
            let policy = microcrab_permission::compile(
                &PolicySnapshot {
                    revision: 1,
                    config: override_config,
                },
                shell,
            )
            .unwrap();
            assert_eq!(
                microcrab_permission::evaluate_compiled(&policy, &json!({"command":"echo ok"}))
                    .unwrap(),
                Action::Allow
            );
        }
    }
    let mut host = info(OsFamily::Other);
    host.capabilities.clear();
    let agent = describe(&host).unwrap().remove(0);
    assert!(agent.tools.is_empty());
    assert!(agent.modes.iter().all(|m| m.permissions.is_empty()));
    host.capabilities = vec!["write".into()];
    let agent = describe(&host).unwrap().remove(0);
    for mode in &agent.modes {
        let dispatch = mode.permissions.iter().collect::<Vec<_>>();
        assert_eq!(dispatch.len(), 1);
        assert_eq!(
            dispatch[0].tool,
            Some(microcrab_permission::Matcher::Exact("write".into()))
        );
    }
    let names = agent
        .tools
        .iter()
        .map(|t| t.name.clone())
        .collect::<Vec<_>>();
    assert_eq!(names, ["write"]);
    assert!(invoke(&host, "edit", "{}", &mut MemoryHost::default()).is_err());
}

#[test]
fn edit_distinguishes_missing_ambiguous_overlap_noop_and_replace_all() {
    let mut host = MemoryHost {
        content: "one one".into(),
        ..Default::default()
    };
    let args = |old: &str, new: &str| json!({"path":"a","expected_version":version(),"old_string":old,"new_string":new});
    assert!(
        call(&mut host, "edit", args("absent", "x"))
            .unwrap_err()
            .contains("zero literal matches")
    );
    assert!(
        call(&mut host, "edit", args("one", "x"))
            .unwrap_err()
            .contains("2 matches")
    );
    let mut all = args("one", "ONE");
    all["replace_all"] = json!(true);
    let out = call(&mut host, "edit", all).unwrap();
    assert_eq!(out["replacement_counts"], json!([2]));
    assert_eq!(host.content, "ONE ONE");
    host.content = "aaa".into();
    let mut overlapping = args("aa", "x");
    overlapping["replace_all"] = json!(true);
    assert!(
        call(&mut host, "edit", overlapping)
            .unwrap_err()
            .contains("overlapping")
    );
    host.content = "unchanged".into();
    let writes = host.writes.len();
    let captures = host.captures.len();
    let out = call(&mut host, "edit", args("unchanged", "unchanged")).unwrap();
    assert_eq!(out["changed"], false);
    assert_eq!(host.writes.len(), writes);
    assert_eq!(host.captures.len(), captures);
}
fn patch_args(patch: &str, paths: &[(&str, bool)]) -> Value {
    json!({"patch":patch,"preconditions":paths.iter().map(|(path,missing)|json!({"path":path,"expected":if *missing {json!("missing")}else{json!({"version":version()})}})).collect::<Vec<_>>()})
}
#[test]
fn whole_patch_preflight_failure_has_no_mutations_or_capture_effects() {
    let patch = "*** Begin Patch\n*** Add File: first\n+created\n*** Update File: second\n@@\n-not present\n+new\n*** End Patch\n";
    let mut host = MemoryHost {
        content: "original\n".into(),
        ..Default::default()
    };
    assert!(
        call(
            &mut host,
            "apply_patch",
            patch_args(patch, &[("first", true), ("second", false)])
        )
        .is_err()
    );
    assert!(host.mutations.is_empty());
    assert!(host.captures.is_empty());
    let collision = "*** Begin Patch\n*** Add File: a\n+x\n*** Add File: ./a\n+y\n*** End Patch\n";
    assert!(
        call(
            &mut host,
            "apply_patch",
            patch_args(collision, &[("a", true), ("./a", true)])
        )
        .is_err()
    );
    assert!(host.mutations.is_empty());
    assert!(host.captures.is_empty());
}
#[test]
fn whole_patch_supports_add_update_delete_move_and_preserves_ordered_prefix() {
    let patch = "*** Begin Patch\n*** Add File: added\n+new\n*** Update File: updated\n@@\n-old\n+updated\n*** Delete File: deleted\n*** Update File: moved\n*** Move to: destination\n@@\n-old\n+moved\n*** End Patch\n";
    let args = patch_args(
        patch,
        &[
            ("added", true),
            ("updated", false),
            ("deleted", false),
            ("moved", false),
            ("destination", true),
        ],
    );
    let mut host = MemoryHost {
        content: "old\n".into(),
        ..Default::default()
    };
    let out = call(&mut host, "apply_patch", args.clone()).unwrap();
    assert_eq!(out["complete"], true);
    assert_eq!(host.mutations.len(), 4);
    assert!(matches!(&host.mutations[3],FileMutation::Move(r) if r.content=="moved\n"));
    host.mutations.clear();
    host.captures.clear();
    host.fail_mutation = Some((2, OperationErrorCode::Conflict));
    let out = call(&mut host, "apply_patch", args).unwrap();
    assert_eq!(out["receipts"][0]["status"], "committed");
    assert_eq!(out["receipts"][1]["status"], "failed");
    assert_eq!(out["receipts"][2]["status"], "not_started");
    assert_eq!(host.mutations.len(), 2);
}
#[test]
fn patch_uncertainty_cancel_and_after_capture_failure_never_hide_prefix() {
    let patch = "*** Begin Patch\n*** Add File: a\n+x\n*** Add File: b\n+y\n*** Add File: c\n+z\n*** End Patch\n";
    let args = patch_args(patch, &[("a", true), ("b", true), ("c", true)]);
    for code in [
        OperationErrorCode::Io,
        OperationErrorCode::Timeout,
        OperationErrorCode::Cancelled,
    ] {
        let mut host = MemoryHost {
            fail_mutation: Some((2, code)),
            ..Default::default()
        };
        let out = call(&mut host, "apply_patch", args.clone()).unwrap();
        assert_eq!(out["receipts"][0]["status"], "committed");
        assert_eq!(out["receipts"][1]["status"], "uncertain");
        assert_eq!(out["receipts"][2]["status"], "not_started");
        assert_eq!(host.mutations.len(), 2);
    }
    let mut host = MemoryHost {
        fail_capture: Some(4),
        ..Default::default()
    };
    let out = call(&mut host, "apply_patch", args).unwrap();
    assert_eq!(out["receipts"][0]["status"], "committed");
    assert_eq!(out["receipts"][0]["evidence_errors"][0]["code"], "limit");
    assert_eq!(host.mutations.len(), 1);
}
#[test]
fn patch_partial_move_reports_both_phases_and_stops_suffix() {
    let patch = "*** Begin Patch\n*** Update File: source\n*** Move to: target\n*** Delete File: later\n*** End Patch\n";
    let mut host = MemoryHost {
        content: "old\n".into(),
        partial_move: true,
        ..Default::default()
    };
    let out = call(
        &mut host,
        "apply_patch",
        patch_args(
            patch,
            &[("source", false), ("target", true), ("later", false)],
        ),
    )
    .unwrap();
    assert_eq!(out["receipts"][0]["status"], "committed");
    assert_eq!(out["receipts"][0]["outcome"]["destination_written"], true);
    assert_eq!(out["receipts"][0]["outcome"]["source_removed"], false);
    assert_eq!(out["receipts"][1]["status"], "not_started");
}

#[test]
fn image_tool_is_not_part_of_coding_even_with_full_filesystem() {
    let mut info = info(OsFamily::Unix);
    for capability in [
        "filesystem.stat",
        "filesystem.open",
        "filesystem.read",
        "filesystem.close",
    ] {
        info.capabilities.push(capability.into());
    }
    let agents = describe(&info).unwrap();
    assert!(!agents[0].tools.iter().any(|tool| tool.name == "read_image"));
    assert!(agents[0].tool_media_output_bytes.is_empty());
    assert!(
        invoke(
            &info,
            "read_image",
            r#"{"path":"picture.png"}"#,
            &mut MemoryHost::default()
        )
        .is_err()
    );
}
