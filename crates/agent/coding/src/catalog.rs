use microcrab_agent_types::{AgentInfo, AgentMode, AgentRole, Hint, HostInfo, Rule, Variable};
use microcrab_permission::{Action, Matcher, Permission};
use microcrab_protocol::ToolDefinition;
use serde_json::{Value, json};

pub(crate) fn linked(host: &HostInfo, capability: &str) -> bool {
    host.capabilities.iter().any(|value| value == capability)
}
pub(crate) fn shell_name(host: &HostInfo) -> Option<&str> {
    if !linked(host, "shell") || !linked(host, "process") {
        return None;
    }
    // A Host-provided shell identity cannot shadow a coding or Session tool.
    host.shell_tool_name().filter(|name| {
        !matches!(
            *name,
            "read"
                | "view_image"
                | "write"
                | "edit"
                | "patch"
                | "apply_patch"
                | "switch_mode"
                | "glob"
                | "grep"
                | "job"
                | "delegate"
                | "sleep"
                | "expand"
        )
    })
}
fn permission(mode: &str, tool: &str, action: Action) -> Permission {
    Permission {
        agent: Some("coding".into()),
        mode: Some(mode.into()),
        tool: Some(Matcher::Exact(tool.into())),
        args: vec![],
        action,
    }
}
fn mode(
    name: &str,
    role: AgentRole,
    mutation: Action,
    tools: &[ToolDefinition],
    shell: Option<&str>,
) -> AgentMode {
    let permissions = tools
        .iter()
        .map(|tool| {
            let action = if matches!(tool.name.as_str(), "read" | "glob" | "grep") {
                Action::Allow
            } else if matches!(
                tool.name.as_str(),
                "write" | "edit" | "patch" | "apply_patch"
            ) || Some(tool.name.as_str()) == shell
            {
                mutation
            } else {
                Action::Deny
            };
            permission(name, &tool.name, action)
        })
        .collect();
    AgentMode {
        name: name.into(),
        description: if name == "plan" { "Inspect code and prepare an implementation plan." } else { "Implement and verify requested code changes." }.into(),
        prompt: if name == "plan" {
            "Inspect relevant files, identify constraints and explain a concrete plan. Use read and search tools. Request a mode switch before implementing changes; Runtime remains the permission authority."
        } else {
            "Implement the requested changes, preserve unrelated work and verify the result within the user's constraints. Read before editing, use exact file versions, and report partial or uncertain effects without replaying mutations."
        }.into(),
        role,
        permissions,
    }
}
fn tool(name: &str, description: &str, properties: Value, required: &[&str]) -> ToolDefinition {
    ToolDefinition {
        name: name.into(),
        description: description.into(),
        parameters: json!({"type":"object","properties":properties,"required":required,"additionalProperties":false}),
    }
}
fn text(description: &str) -> Value {
    json!({"type":"string","minLength":1,"maxLength":4096,"description":description})
}
fn search_pattern(description: &str) -> Value {
    json!({"type":"string","minLength":1,"maxLength":1024,"description":description})
}
fn path() -> Value {
    text(
        "Host file path, relative to its workspace root or absolute within it. Nonempty, <=4096 UTF-8 bytes, no NUL; null is invalid.",
    )
}
fn root() -> Value {
    text(
        "Host search root: relative to its workspace or absolute within it; '.' selects the workspace root. Nonempty, <=4096 UTF-8 bytes, no NUL; null is invalid.",
    )
}
fn version(description: &str) -> Value {
    json!({"type":"string","pattern":"^[a-fA-F0-9]{64}$","description":description})
}
fn limit(default: u64, maximum: u64, description: &str) -> Value {
    // Unbounded Rust counters must not advertise u64::MAX as a provider integer:
    // compatible gateways commonly decode JSON schema numbers into signed i64.
    // Rust argument deserialization still enforces its numeric representation.
    let mut schema =
        json!({"type":"integer","minimum":1,"default":default,"description":description});
    if maximum != u64::MAX {
        schema["maximum"] = maximum.into();
    }
    schema
}
fn replacement_text(nonempty: bool, description: &str) -> Value {
    let mut schema = json!({"type":"string","maxLength":1048576,"description":description});
    if nonempty {
        schema["minLength"] = 1.into();
    }
    schema
}

pub fn describe(host: &HostInfo) -> Result<Vec<AgentInfo>, String> {
    let mut tools = Vec::new();
    if linked(host, "read") {
        tools.push(tool("read", "Read bounded, line-numbered UTF-8 text and its full-file SHA-256 version. Truncated output is incomplete; continue with next_line.", json!({
            "path":path(),
            "offset_line":limit(1,u64::MAX,"First line, one-based positive u64. Omission defaults to 1; null is invalid."),
            "max_lines":limit(200,10000,"Line cap, 1..=10000, also bounded by max_bytes. Omission defaults to 200; null is invalid."),
            "max_bytes":limit(65536,1048576,"UTF-8 content byte cap, 1..=1048576. Omission defaults to 65536; null is invalid.")
        }), &["path"]));
    }
    if linked(host, "write") {
        tools.push(tool("write", "Atomically create or replace a UTF-8 file under expected_version. Returns its new SHA-256 version and byte count; conflicts do not overwrite.", json!({
            "path":path(),
            "content":replacement_text(false,"Complete UTF-8 content, <=1048576 bytes, no NUL. Empty creates or truncates to an empty file; null is invalid."),
            "expected_version":version("Full-file SHA-256 from read: 64 hex characters, case-insensitive. Omission requires a missing file; a supplied version must match to replace; null is invalid.")
        }), &["path","content"]));
        if linked(host, "read") {
            tools.push(tool("edit", "Atomically replace text in a versioned UTF-8 file. Returns the new version and byte count; incomplete reads, version conflicts or invalid matches leave it unchanged.", json!({
                "path":path(),
                "expected_version":version("Full-file SHA-256 from read: 64 hex characters, case-insensitive; null is invalid. The file must fit 10000 lines and 1048576 bytes."),
                "old_string":replacement_text(true,"Literal UTF-8 match occurring exactly once: nonempty, <=1048576 bytes, no NUL; null is invalid."),
                "new_string":replacement_text(false,"UTF-8 replacement, <=1048576 bytes, no NUL. Empty deletes the match; null is invalid."),
                "replace_all":{"type":"boolean","default":false,"description":"Omission defaults to false and requires one match. True replaces all nonoverlapping matches and reports their count; overlapping matches fail. null is invalid."}
            }), &["path","expected_version","old_string","new_string"]));
            tools.push(tool("patch", "Atomically apply replacement objects to a versioned UTF-8 file, not a unified diff. All steps must validate before writing; returns the new version and byte count.", json!({
                "path":path(),
                "expected_version":version("Full-file SHA-256 from read: 64 hex characters, case-insensitive; mismatch fails without writing. The original file must fit 10000 lines and 1048576 bytes; null is invalid."),
                "replacements":{"type":"array","minItems":1,"maxItems":64,"description":"Apply 1..=64 literal replacements in order, each against the preceding result. Null is invalid.","items":{"type":"object","description":"One replacement with both text fields required.","properties":{
                    "old_string":replacement_text(true,"UTF-8 match occurring exactly once at this step: nonempty, <=1048576 bytes, no NUL; null is invalid."),
                    "new_string":replacement_text(false,"UTF-8 replacement, <=1048576 bytes, no NUL. Empty deletes the match; null is invalid."),
                "replace_all":{"type":"boolean","default":false,"description":"Omission defaults to false and requires one match. True replaces all nonoverlapping matches and reports their count; overlapping matches fail. null is invalid."}
                },"required":["old_string","new_string"],"additionalProperties":false}}
            }), &["path","expected_version","replacements"]));
        }
    }
    if linked(host, "file_mutation") && linked(host, "read") {
        tools.push(tool("apply_patch", "Apply a bounded multi-file patch after whole-call preflight. Syntax: *** Begin Patch; *** Add File: path with +payload lines; *** Delete File: path; *** Update File: path optionally followed by *** Move to: destination, then @@ hunks with space=context, -=old, +=new lines; *** End Patch. Every header occupies its own LF-delimited line. Use \\ No newline at end of file after a payload line to omit its final LF. Hunks require exact unique nonempty source context; no fuzzy/numbered hunks. Pure move needs no hunks. At most 4300000 serialized argument bytes, 32 operations, 64 hunks/file, 1 MiB/file, 10000 source lines, 4 MiB patch text, 4 MiB total source and 4 MiB total result bytes. Every source requires a SHA256 version; every new destination requires missing and an existing parent directory (create parents explicitly before patching). Host/carrier refinements, including native remote 128 KiB request frames, may reject smaller patches during whole-call preflight. Unused/duplicate preconditions, path aliases/collisions and unsupported capabilities fail before mutations. Commits are sequential: receipts retain the committed prefix, failed/uncertain operation and not-started suffix. Never replay after uncertainty; no multi-file rollback guarantee.",json!({
            "patch":{"type":"string","minLength":1,"maxLength":4194304,"description":"Deterministic LF-delimited patch text using the grammar above, <=4194304 UTF-8 bytes, no NUL; null is invalid."},
            "preconditions":{"type":"array","minItems":1,"maxItems":64,"description":"Exactly one explicit precondition per affected source or destination path; duplicate/unused entries fail before effects. null is invalid.","items":{"type":"object","description":"An exact patch path and its expected existing SHA256 version or missing destination state.","properties":{
                "path":path(),
                "expected":{"description":"Existing paths use {version:full-file SHA256}; new destinations use the string missing. null is invalid.","oneOf":[{"type":"string","enum":["missing"]},{"type":"object","properties":{"version":version("Expected full-file SHA256 digest, 64 hex characters; null is invalid.")},"required":["version"],"additionalProperties":false}]}
            },"required":["path","expected"],"additionalProperties":false}}
        }),&["patch","preconditions"]));
    }
    if linked(host, "glob") {
        tools.push(tool("glob", "Find Host entries with a glob. Returns groups of kind (file, directory, symlink), each with a path array. Paths are workspace-relative inside the workspace and absolute outside it when explicit Host filesystem policy permits external access. Empty kinds are omitted. Links are listed without reading or traversing targets. Results include a truncation flag; truncated output is incomplete.", json!({
            "root":root(),
            "pattern":search_pattern("Glob under root using Host syntax: nonempty, <=1024 UTF-8 bytes, no NUL; null is invalid."),
            "include_hidden":{"type":"boolean","default":false,"description":"Include hidden entries. Omission defaults to false; null is invalid."},
            "include_ignored":{"type":"boolean","default":false,"description":"Include entries excluded by Host ignore rules. Omission defaults to false; null is invalid."},
            "include_directories":{"type":"boolean","default":false,"description":"Include matching directories and symlink/junction nodes alongside files, without following links. Omission defaults to false (ordinary files only); null is invalid."},
            "max_results":limit(100,4096,"Combined entry cap, 1..=4096. Omission defaults to 100; null is invalid.")
        }), &["root","pattern"]));
    }
    if linked(host, "grep") {
        tools.push(tool("grep", "Search Host files for text. Returns structured matches, path:line:text and a truncation flag; truncated output is incomplete.", json!({
            "root":root(),
            "pattern":search_pattern("Host regex, or plain text when literal=true: nonempty, <=1024 UTF-8 bytes, no NUL; null is invalid."),
            "include_glob":search_pattern("Host glob restricting paths under root: nonempty, <=1024 UTF-8 bytes, no NUL. Omission adds no restriction; null is invalid."),
            "case_sensitive":{"type":"boolean","default":true,"description":"Case-sensitive matching. Omission defaults to true; null is invalid."},
            "literal":{"type":"boolean","default":false,"description":"Treat pattern as plain text. Omission defaults to false (Host regex); null is invalid."},
            "max_matches":limit(100,4096,"Match cap, 1..=4096, also bounded by max_bytes. Omission defaults to 100; null is invalid."),
            "max_bytes":limit(65536,1048576,"Host-counted matched-text and path byte cap, 1..=1048576. Omission defaults to 65536; null is invalid.")
        }), &["root","pattern"]));
    }
    if let Some(name) = shell_name(host) {
        let description = match host.os_family {
            microcrab_agent_types::OsFamily::Windows => {
                "Execute a PowerShell command on the Windows Host."
            }
            microcrab_agent_types::OsFamily::Unix => "Execute a Bash command on the Unix Host.",
            microcrab_agent_types::OsFamily::Other => {
                "Execute a command using the configured Host shell. Consult Host environment information for its syntax and limitations."
            }
        };
        let execution = "Returns a complete result or a buffered receipt with id=session_id/execution_id and optional nonconsuming initial stdout/stderr.";
        let yield_description = "Initial wait, 0..=60000 ms; zero requests an immediate receipt. Omission defaults to 1000; null is invalid. Ending the wait does not terminate or restart the process.";
        tools.push(tool(name, &format!("{description} {execution}"), json!({
            "command":{"type":"string","minLength":1,"maxLength":65536,"description":"Host shell command: nonempty, <=65536 UTF-8 bytes, no NUL; null is invalid."},
            "cwd":text("Host cwd, relative to its workspace root or absolute within it; nonempty, <=4096 UTF-8 bytes, no NUL. Omission uses the workspace root; null is invalid."),
            "yield_ms":{"type":"integer","minimum":0,"maximum":60000,"default":1000,"description":yield_description}
        }), &["command"]));
    }
    let mut variables = Variable::default();
    variables.inject("description", "Coding agent. Start in plan mode to inspect and plan; switch to build mode when implementation is requested.").map_err(|e|e.to_string())?;
    variables.inject("rules", vec![
        Rule { name: "plan_first".into(), description: "Inspect relevant code and form a concrete plan before implementation. Plan mode recommends read/search only. Runtime is the sole permission authority; never treat a tool schema or mode recommendation as approval.".into() },
        Rule { name: "safe_edits".into(), description: "Read before editing, preserve unrelated changes, and use the returned full-file version for conditional writes. On conflict reread and reconsider the edit. Never claim a truncated read covers the full file.".into() },
    ]).map_err(|e|e.to_string())?;
    variables.inject("hints", vec![Hint { name: "bounded_tools".into(), description: "Prefer read/glob/grep for inspection and edit/patch for literal changes. Continue truncated output explicitly. All paths and process execution are interpreted by the configured Host. Runtime can deny any operation.".into() }]).map_err(|e|e.to_string())?;
    if let Some(description) = &host.description {
        let mut hints: Vec<Hint> = variables
            .extract("hints")
            .map_err(|e| e.to_string())?
            .unwrap_or_default();
        hints.push(Hint {
            name: "host_environment".into(),
            description: description.clone(),
        });
        variables
            .inject("hints", hints)
            .map_err(|e| e.to_string())?;
    }
    Ok(vec![AgentInfo {
        tool_media_output_bytes: Default::default(),
        name: "coding".into(),
        modes: vec![
            mode(
                "plan",
                AgentRole::MainAgent,
                Action::Deny,
                &tools,
                shell_name(host),
            ),
            mode(
                "build",
                AgentRole::Both,
                Action::Ask,
                &tools,
                shell_name(host),
            ),
        ],
        tools,
        variables,
    }])
}
